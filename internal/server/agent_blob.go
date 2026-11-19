package server

import (
	"context"
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// Agent relay response limits. Java AgentCall/TcpAgentWorker has no size or
// total-time limit; Go keeps the Java per-read net_tcp_agent_so_timeout_ms and
// bounds memory per command.
const (
	// agentPackItemBytes/agentPackTotalBytes: ordinary relays (Pack responses).
	agentPackItemBytes  = 8 << 20
	agentPackTotalBytes = 8 << 20
	// agentLargePackBytes: commands whose single MapPack can carry a whole
	// file or a large dump (OBJECT_DOWNLOAD_JAR's jar Blob,
	// AgentClassHandle.downloadJar). It matches the Client response value
	// limit (transport.MaxResponseValueBytes, 256MiB) so a Pack the agent may
	// send is not refused only by the relay.
	agentLargePackBytes = transport.MaxResponseValueBytes
	// agentLargeTransfer is the total deadline on top of the so-timeout for
	// those commands (transfer time of up to agentLargePackBytes).
	agentLargeTransfer = 10 * time.Minute
	// agentBlobChunkBytes bounds one relayed Blob chunk (encoded). Java agents
	// send 2MiB (AgentHeapDump.downloadHeapDump) or 4KiB (AgentEnv
	// getDumpFileDetail) chunks; the stream itself has no total size or total
	// deadline, as in Java.
	agentBlobChunkBytes = 16 << 20
	// agentPackCount bounds the Packs kept for one relay.
	agentPackCount = 4096
)

// largeAgentPackCommands answer with one MapPack that can be far larger than
// an ordinary relay: a jar file, a full heap histogram or thread dump.
var largeAgentPackCommands = map[string]bool{
	"OBJECT_DOWNLOAD_JAR": true, // AgentClassHandle.downloadJar: BlobValue(FileUtil.readAll(jar))
	"OBJECT_HEAPHISTO":    true, // jmap -histo text of every class
	"OBJECT_THREAD_DUMP":  true, // full thread dump text
}

// agentPackLimits returns the response limits for a Pack relay of cmd.
func agentPackLimits(cmd string) transport.AgentLimits {
	if largeAgentPackCommands[cmd] {
		return transport.AgentLimits{ItemBytes: agentLargePackBytes, TotalBytes: agentLargePackBytes, Transfer: agentLargeTransfer}
	}
	return transport.AgentLimits{ItemBytes: agentPackItemBytes, TotalBytes: agentPackTotalBytes}
}

// agentBlobLimits: per-chunk size only; reads keep the so-timeout and the
// Client side's per-chunk write deadline bounds the rest (no total deadline).
// The Client command progress limit is held for the whole stream
// (internal/service, transport.Stream.AwaitUpstream), as Java's handler waits
// for each chunk as long as the so_timeout allows.
var agentBlobLimits = transport.AgentLimits{ItemBytes: agentBlobChunkBytes, TotalBytes: -1, Transfer: -1}

// logRelayLimit names the command and limit when a relay is refused for size;
// the Client sees the failure as the closed request (no NoNEXT).
func logRelayLimit(hash int32, cmd string, limits transport.AgentLimits, err error) {
	if !errors.Is(err, transport.ErrLimit) && !errors.Is(err, protocol.ErrLimit) {
		return
	}
	total := "none"
	if limits.TotalBytes > 0 {
		total = formatBytes(limits.TotalBytes)
	}
	log.Printf("agent relay %s objHash=%d refused: response over the Go relay limit (item=%s total=%s): %v; the agent session is closed and the Client request fails",
		cmd, hash, formatBytes(int64(limits.ItemBytes)), total, err)
}

func formatBytes(n int64) string {
	switch {
	case n >= 1<<20 && n%(1<<20) == 0:
		return strconv.FormatInt(n>>20, 10) + "MiB"
	case n >= 1<<10 && n%(1<<10) == 0:
		return strconv.FormatInt(n>>10, 10) + "KiB"
	}
	return strconv.FormatInt(n, 10) + "B"
}

// requestBlob relays a raw Blob stream (HeapDumpService.downloadHepDump,
// DumpService.getDumpFileDetail): each chunk goes to emit as soon as it is
// read, so only one chunk is held. emit owns nothing after it returns: the
// slice is not reused, but emit must finish with it before returning. An emit
// error (Client gone or too slow) ends the request and closes the agent
// session, like Java TcpAgentWorker.read closing the socket when the handler
// throws.
func (p *agentPool) requestBlob(ctx context.Context, hash int32, cmd string, param *protocol.MapPack, emit func([]byte) error) error {
	// One chunk is held at a time: the stream holds one chunk of the relay
	// budget however long it runs.
	start := time.Now()
	permit, err := p.acquireRelay(ctx, hash, cmd, agentBlobChunkBytes, false)
	if err != nil || permit == nil {
		return err
	}
	defer permit.release()
	slot, err := p.checkoutAfterBudget(ctx, hash, start)
	if err != nil || slot == nil {
		return err
	}
	defer p.release(slot)
	if p.register != nil {
		p.register(cmd) // AgentCall: RequestLogger.registerCmd before the write
	}
	var emitErr error
	err = slot.conn.RequestLimited(ctx, cmd, param, agentBlobLimits, func(ctx context.Context, s *transport.Stream) error {
		b, err := s.ReadBlob()
		if err != nil {
			return err
		}
		if emitErr = emit(b); emitErr != nil {
			return emitErr
		}
		return ctx.Err()
	})
	if err != nil {
		if emitErr == nil {
			logRelayLimit(hash, cmd, agentBlobLimits, err)
		}
		if slot.conn.Closed() {
			slot.once.Do(func() { close(slot.done) })
		}
	}
	return err
}
