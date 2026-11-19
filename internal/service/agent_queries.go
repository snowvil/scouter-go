package service

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// AgentBlobBackend owns the Agent connection and passes each Blob chunk to
// emit in upstream order as soon as it is read (Java AgentCall.call with a
// handler writing to the Client). emit must finish with the chunk before it
// returns; an emit error ends the relay. Blob commands must not be decoded
// through AgentBackend.
type AgentBlobBackend interface {
	AgentBlobRequest(ctx context.Context, hash int32, command string, p *protocol.MapPack, emit func([]byte) error) error
}

// AgentReadCommands returns the 43 additional read relays in query-scope.json.
// OBJECT_ENV is handled here too, but was supported before that inventory.
func AgentReadCommands() []string {
	return []string{
		"ACTIVE_QUERY_LIST", "BATCH_ACTIVE_STACK", "CUBRID_GET_ALERT_CONFIGURE", "DB_EXPLAIN_PLAN", "DB_PROCESS_DETAIL", "DB_PROCESS_LIST", "DB_VARIABLES", "DEBUG_AGENT", "DUMP_APACHE_STATUS", "EXPLAIN_PLAN_FOR_THREAD", "GET_CONFIGURE_WAS", "GET_QUERY_INTERVAL", "HOST_DISK_USAGE", "HOST_MEMINFO", "HOST_NET_STAT", "HOST_PROCESS_DETAIL", "HOST_TOP", "HOST_WHO", "INNODB_STATUS", "LIST_CONFIGURE_WAS", "LOCK_LIST", "OBJECT_ACTIVE_SERVICE_LIST", "OBJECT_ACTIVE_SERVICE_LIST_GROUP", "OBJECT_BATCH_ACTIVE_LIST", "OBJECT_CHECK_RESOURCE_FILE", "OBJECT_CLASS_DESC", "OBJECT_CLASS_LIST", "OBJECT_DOWNLOAD_HEAP_DUMP", "OBJECT_DOWNLOAD_JAR", "OBJECT_DUMP_FILE_DETAIL", "OBJECT_DUMP_FILE_LIST", "OBJECT_FILE_SOCKET", "OBJECT_HEAPHISTO", "OBJECT_LIST_HEAP_DUMP", "OBJECT_LOAD_CLASS_BY_STREAM", "SOCKET", "OBJECT_THREAD_DETAIL", "OBJECT_THREAD_DUMP", "OBJECT_THREAD_LIST", "REDIS_INFO", "SCHEMA_SIZE_STATUS", "SLAVE_STATUS", "TABLE_SIZE_STATUS",
	}
}

func isAgentBlobCommand(command string) bool {
	return command == "OBJECT_DUMP_FILE_DETAIL" || command == "OBJECT_DOWNLOAD_HEAP_DUMP"
}

// HandleAgentReadCommand recognizes only read relays, before reading their body.
// The dispatcher calls this before PacketCommands because the Batch relays share
// that older inventory. Unknown and management commands leave the stream intact.
func (s *Service) HandleAgentReadCommand(ctx context.Context, r transport.ClientRequest, stream *transport.Stream) (bool, error) {
	recognized := r.Command == "OBJECT_ENV"
	for _, command := range AgentReadCommands() {
		if r.Command == command {
			recognized = true
			break
		}
	}
	if !recognized {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if !r.Authenticated {
		return true, transport.ErrUnauthorized
	}
	p, err := readMap(stream)
	if err != nil {
		return true, err
	}
	return true, s.agentReadQuery(ctx, r.Command, p, stream)
}

func (s *Service) agentReadQuery(ctx context.Context, command string, p *protocol.MapPack, stream *transport.Stream) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if isAgentBlobCommand(command) {
		b, ok := s.backend.(AgentBlobBackend)
		if !ok {
			return transport.ErrUnsupported
		}
		hash, err := getInt(p, "objHash")
		if err != nil {
			return err
		}
		// HeapDumpService.downloadHepDump / DumpService.getDumpFileDetail:
		// every chunk is written to the Client as it arrives (no count or
		// total size; a slow Client blocks the relay by write backpressure).
		// Chunks already written stay written when the agent fails later.
		// The whole stream is an upstream wait: Java's handler waits for
		// each chunk as long as the Agent so_timeout allows, so the Client
		// progress limit is held; the chunk writes keep their own write
		// deadline (transport.Stream.AwaitUpstream).
		err = stream.AwaitUpstream(func() error {
			return b.AgentBlobRequest(ctx, hash, command, p, func(blob []byte) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				// The relay's own chunk permit counts this chunk (internal/server).
				return stream.ResponseRelayBlob(blob)
			})
		})
		if err != nil {
			return err
		}
		return ctx.Err()
	}
	b, ok := s.backend.(AgentBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	var hashes []int32
	decorate := command == "OBJECT_ACTIVE_SERVICE_LIST" || command == "OBJECT_ACTIVE_SERVICE_LIST_GROUP" || command == "OBJECT_BATCH_ACTIVE_LIST"
	if command == "OBJECT_ACTIVE_SERVICE_LIST_GROUP" {
		if lookup(p, "objHash").Type != protocol.ValueList {
			return badParam("objHash")
		}
		var err error
		hashes, err = getHashes(p, "objHash")
		if err != nil {
			return err
		}
		command = "OBJECT_ACTIVE_SERVICE_LIST"
	} else {
		hash, err := getInt(p, "objHash")
		if err != nil {
			return err
		}
		if command == "OBJECT_THREAD_DETAIL" {
			present := false
			for _, e := range p.Entries {
				if e.Key == "id" {
					present = true
					break
				}
			}
			if !present {
				return nil
			}
		}
		if decorate && hash == 0 {
			if lookup(p, "objType").Type == protocol.ValueNull {
				return nil
			}
			typ, err := getText(p, "objType")
			if err != nil {
				return err
			}
			objects, err := s.backend.Objects(ctx, "")
			if err != nil {
				return err
			}
			for _, o := range objects {
				if o != nil && o.Alive && o.ObjType == typ {
					if len(hashes) == 4096 {
						return transport.ErrLimit
					}
					hashes = append(hashes, o.ObjHash)
				}
			}
		} else {
			hashes = []int32{hash}
		}
	}
	if len(hashes) > 4096 {
		return transport.ErrLimit
	}
	for _, hash := range hashes {
		if err := ctx.Err(); err != nil {
			return err
		}
		results, err := awaitAgent(ctx, stream, b, hash, command, p)
		if err != nil {
			if !skippableAgentError(ctx, err) {
				return err
			}
			// AgentCall.scala:46-71 with TcpAgentWorker.scala:21-68: an agent
			// I/O failure closes that session and AgentCall returns null, the
			// same as an offline agent. Every Pack handler then goes on as for
			// null: the single-target ones write nothing (`if (p != null)`,
			// e.g. HostService.scala:35-39, AgentInfo.scala:113-117,
			// ThreadList.scala:67-71/155-159), the active-list ones write the
			// {objHash} placeholder (ThreadList.scala:113-124/131-146,
			// BatchService.scala:110-136) and the fan-outs go on with the next
			// agent.
			noteAgentRelaySkip(command, hash, decorate, err)
			results = nil
		}
		if len(results) > 4096 {
			return transport.ErrLimit
		}
		// AgentCall.scala:53-63 discards every Pack except the final MapPack.
		var result *protocol.MapPack
		if len(results) != 0 {
			result, _ = results[len(results)-1].(*protocol.MapPack)
		}
		if decorate {
			if result == nil {
				result = newMap(entry("objHash", decimal(int64(hash))))
			} else {
				// Clone before the source's LinkedHashMap put operation: a backend
				// may return the same snapshot for multiple target hashes.
				data, err := protocol.EncodePack(result, protocol.Limits{})
				if err != nil {
					return err
				}
				owned, err := protocol.DecodePack(data, protocol.Limits{})
				if err != nil {
					return err
				}
				result = owned.(*protocol.MapPack)
				replaced := false
				for i := range result.Entries {
					if result.Entries[i].Key == "objHash" {
						result.Entries[i].Value = decimal(int64(hash))
						replaced = true
						break
					}
				}
				if !replaced {
					result.Entries = append(result.Entries, entry("objHash", decimal(int64(hash))))
				}
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if result != nil {
			if err := stream.ResponsePack(result); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}

// awaitAgent waits for one Agent answer like Java's service thread: as long
// as the relay's own deadline (Agent so_timeout) allows, without the Client
// command progress limit, and only until the Client leaves
// (transport.Stream.AwaitUpstream). A fan-out waits per target, its writes in
// between count as progress.
func awaitAgent(ctx context.Context, stream *transport.Stream, b AgentBackend, hash int32, command string, p *protocol.MapPack) (results []protocol.Pack, err error) {
	err = stream.AwaitUpstream(func() error {
		results, err = b.AgentRequest(ctx, hash, command, p)
		return err
	})
	return results, err
}

// skippableAgentError reports whether a Pack relay treats err from one agent
// as Java's null answer. Java AgentCall swallows every agent I/O failure, a
// read timeout included (the session is closed and the call returns null), so
// those are skipped; an agent's own deadline is one of them while the
// request context is alive. A cancelled or expired request, a Go relay size
// limit (the agent session is closed and the Client request fails,
// logRelayLimit) and a backend without agent calls end the whole request.
func skippableAgentError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	return !errors.Is(err, transport.ErrLimit) && !errors.Is(err, transport.ErrUnsupported)
}

// agentRelaySkips counts agent failures answered as Java's null for the
// process lifetime; agentRelaySkipLog bounds the log to one line per minute.
var (
	agentRelaySkips   atomic.Uint64
	agentRelaySkipLog struct {
		sync.Mutex
		last       time.Time
		suppressed uint64
	}
	agentRelaySkipLogf = log.Printf
)

const agentRelaySkipLogEvery = time.Minute

func noteAgentRelaySkip(command string, hash int32, placeholder bool, err error) {
	total := agentRelaySkips.Add(1)
	l := &agentRelaySkipLog
	l.Lock()
	now := time.Now()
	if !l.last.IsZero() && now.Sub(l.last) < agentRelaySkipLogEvery {
		l.suppressed++
		l.Unlock()
		return
	}
	suppressed := l.suppressed
	l.last, l.suppressed = now, 0
	l.Unlock()
	answer := "no item"
	if placeholder {
		answer = "the {objHash} placeholder"
	}
	agentRelaySkipLogf("service: %s skipped objHash=%d after agent failure, answered %s like Java's null AgentCall result: %v (%d more skipped since the last line, %d in total)", command, hash, answer, err, suppressed, total)
}
