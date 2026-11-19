package transport

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

const (
	TCPClient       uint32 = 0xcafe2001
	TCPAgentV1      uint32 = 0xcafe1001
	TCPAgentV2      uint32 = 0xcafe1002
	TCPAgentRequest uint32 = 0xcafe1011
	TCPSendStack    uint32 = 0xeded0001
	OK              byte   = 0x01
	HasNext         byte   = 0x03
	NoNext          byte   = 0x04
	InvalidSession  byte   = 0x44
)

// Java Configure defaults for the live TCP keys. They apply until a setter runs.
const (
	DefaultClientSoTimeout = 8000 * time.Millisecond  // net_tcp_client_so_timeout_ms
	DefaultAgentSoTimeout  = 60000 * time.Millisecond // net_tcp_agent_so_timeout_ms
	DefaultServicePoolSize = 100                      // net_tcp_service_pool_size
)

// Connection budget defaults (Go only: Java TcpServer accepts without a limit
// and queues every socket on the service pool). They are generous on purpose:
// a limit should only stop a runaway, never a real Agent fleet.
//
// Measured on loopback (go1.27 darwin/arm64, 3000 parked TCP_AGENT_V2
// sockets, GC'd HeapInuse+StackInuse): about 14KiB of Go memory per idle
// Agent socket (4KiB bufio reader + 4KiB writer, Stream/AgentConn/idleReader,
// netFD, and a 4KiB goroutine stack; more while a request runs). A socket in
// handshake costs the same (its buffers exist before the magic is read).
//
//   - DefaultMaxAgentConnections 16384: ~224MiB at worst when every slot is
//     an idle Agent; 2048 objects at the server's 8 sockets per objHash.
//   - DefaultMaxHandshakes 2048: ~28MiB at worst. Agents send the magic and
//     objHash in their first write, so a slot is held for one round trip;
//     thousands of Agents reconnecting at once after a restart pass through it.
//     Only sockets that send nothing hold it up to HandshakeTimeout.
//   - DefaultMaxConnections 128 is this package's Client default; the server
//     wiring passes its own (--max-connections).
//
// Each socket holds one file descriptor, so the process fd limit (ulimit -n)
// must exceed MaxHandshakes+MaxAgentConnections+MaxConnections plus storage
// files; otherwise accept fails before these limits are reached.
const (
	DefaultMaxHandshakes       = 2048
	DefaultMaxAgentConnections = 16384
	DefaultMaxConnections      = 128
)

// TCPConfig bounds active sockets, buffered command bodies and raw stack uploads.
// Sockets are budgeted in three groups, each closed and counted on overflow:
// MaxHandshakes bounds accepted sockets whose 4-byte magic is still unread
// (checked at accept), MaxAgentConnections bounds sockets with an Agent magic
// (TCP_AGENT, TCP_AGENT_V2, TCP_AGENT_REQ) and MaxConnections bounds Client
// sockets (TCP_CLIENT), waiting or in session. A socket gives its handshake
// slot back when its magic is read and then takes one of its group, so it
// never holds two. A socket with an unknown magic takes none and is closed.
// RequestTimeout is a total deadline for Agent requests and stack uploads, not
// refreshed by partial reads. For Client commands it is a progress limit
// instead (no total deadline, see client_progress.go). MaxFrameBytes is the
// cumulative per-command budget of request bodies and of Agent/stack writes;
// Client responses are bounded per item (MaxResponseValueBytes), not in total.
// HandshakeTimeout bounds the 4-byte magic (and object hash) read of every
// connection. IdleTimeout bounds TCP_AGENT_REQ command waits and the Agent
// post-handshake deadline. Client command waits use the live
// net_tcp_client_so_timeout_ms (SetClientSoTimeout), not IdleTimeout.
type TCPConfig struct {
	MaxConnections, MaxFrameBytes, MaxTextBytes   int
	MaxHandshakes, MaxAgentConnections            int
	MaxUploadBytes                                int64
	HandshakeTimeout, IdleTimeout, RequestTimeout time.Duration
	CodecLimits                                   protocol.Limits
	// MaxClientResponseBytes bounds the encoded Client response items of all
	// connections held in memory at once (client_budget.go). 0 selects
	// DefaultClientResponseBytes(); negative is rejected.
	MaxClientResponseBytes int64
}

func (c TCPConfig) normalized() (TCPConfig, error) {
	for _, field := range []struct {
		p        *int
		fallback int
	}{{&c.MaxConnections, DefaultMaxConnections}, {&c.MaxHandshakes, DefaultMaxHandshakes}, {&c.MaxAgentConnections, DefaultMaxAgentConnections},
		{&c.MaxFrameBytes, 8 << 20}, {&c.MaxTextBytes, 64 << 10}} {
		v, err := positive(*field.p, field.fallback)
		if err != nil {
			return c, err
		}
		*field.p = v
	}
	if c.MaxUploadBytes < 0 {
		return c, ErrMalformed
	}
	if c.MaxUploadBytes == 0 {
		c.MaxUploadBytes = 64 << 20
	}
	if c.MaxClientResponseBytes < 0 {
		return c, fmt.Errorf("%w: negative Client response budget", ErrMalformed)
	}
	if c.MaxClientResponseBytes == 0 {
		c.MaxClientResponseBytes = DefaultClientResponseBytes()
	}
	for _, field := range []struct {
		p        *time.Duration
		fallback time.Duration
	}{{&c.HandshakeTimeout, 5 * time.Second}, {&c.IdleTimeout, 60 * time.Second}, {&c.RequestTimeout, 30 * time.Second}} {
		v, err := duration(*field.p, field.fallback)
		if err != nil {
			return c, err
		}
		*field.p = v
	}
	return c, validCodecLimits(c.CodecLimits)
}

type ClientRequest struct {
	Command       string
	Session       int64
	Authenticated bool
	Peer          net.Addr
	// Conn is the Client connection of the request (session tracking).
	Conn ClientConn
}
type StackUpload struct {
	ObjectHash           int32
	StartTime            int64
	ObjectName, FileName string
	Size                 int64
	Peer                 net.Addr
}

// All callbacks are synchronous and must honor ctx. Client owns its body parser
// (Pack, Value, Blob or no body), not the connection or the final NoNEXT flag.
// Agent owns the connection's service lifetime and returns only when done; it is
// a server->agent request channel, not unsolicited telemetry ingest.
// Stack consumes exactly Size raw bytes and commits persistence before returning
// nil. Filename is untrusted metadata; storage must not use it as an unchecked path.
type TCPHandlers struct {
	Client    func(context.Context, ClientRequest, *Stream) error
	Authorize func(context.Context, ClientRequest) (bool, error)
	Agent     func(context.Context, *AgentConn) error
	Stack     func(context.Context, StackUpload, io.Reader) error
	// Sessions, when set, is told which session ids each admitted Client
	// connection carries and when it stops carrying them (client_sessions.go).
	Sessions ClientSessionObserver
}

// TCPStats: Active counts all sockets that passed accept and are not closed
// yet, including ones about to close after a group limit. Handshaking,
// AgentConnections and ClientConnections are the slots held now in each
// budget, and MaxHandshakes/MaxAgentConnections/MaxConnections their limits
// (the normalized TCPConfig values). Rejected is the total of
// HandshakeRejected (MaxHandshakes full at accept), AgentRejected
// (MaxAgentConnections full) and ClientRejected (MaxConnections full);
// rejected sockets are reset without a response and are not Errors.
// ClientSessions/ClientWaiting split Client connections that hold or wait
// for a net_tcp_service_pool_size permit. Waiters that end without a session are
// closed and counted once: ClientAbandoned (peer EOF or reset while waiting),
// ClientWaitTimeouts (wait limit reached) or ClientWaitRejected (waiting set
// already at MaxClientWaiters).
type TCPStats struct {
	Accepted, Rejected, Errors, ClientRequests, StackUploads uint64
	Active                                                   int64
	ClientSessions, ClientWaiting, ServicePoolSize           int
	ClientAbandoned, ClientWaitTimeouts, ClientWaitRejected  uint64
	HandshakeRejected, AgentRejected, ClientRejected         uint64
	Handshaking, AgentConnections, ClientConnections         int
	MaxHandshakes, MaxAgentConnections, MaxConnections       int
	// AcceptRetries counts temporary Accept failures (e.g. EMFILE) the
	// server waited out instead of stopping the listener.
	AcceptRetries uint64
	// Client response memory budget (client_budget.go): bytes of encoded
	// items held now, the most ever held, the limit, items waiting now,
	// items that had to wait, and items refused (the command failed).
	ClientResponseBytes, ClientResponsePeakBytes, MaxClientResponseBytes int64
	ClientResponseWaiting                                                int
	ClientResponseWaits, ClientResponseRejected                          uint64
	// Errors by class (conn_errors.go); they add up to Errors.
	TimeoutErrors, ClosedErrors, LimitErrors, UnsupportedErrors, UnauthorizedErrors, OtherErrors uint64
}

// connBudget is one connection group: a slot channel and its rejections.
type connBudget struct {
	name, actor, flag string // stats name, Java-log actor ("Agent", ...), server flag that raises it
	slots             chan struct{}
	rejected          atomic.Uint64
	log               rateLimit
	unlogged          atomic.Uint64 // rejections not yet named in a log line
}

func newConnBudget(name, actor, flag string, limit int) connBudget {
	return connBudget{name: name, actor: actor, flag: flag, slots: make(chan struct{}, limit)}
}
func (b *connBudget) tryAcquire() bool {
	select {
	case b.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// connLease is the budget slot one connection holds. Only the connection's
// goroutine (or the accept loop before it starts) touches it.
type connLease struct{ held *connBudget }

func (l *connLease) release() {
	if l.held != nil {
		<-l.held.slots
		l.held = nil
	}
}

type TCPServer struct {
	listener                              net.Listener
	config                                TCPConfig
	handlers                              TCPHandlers
	errors                                ErrorHandler
	serving, closed                       atomic.Bool
	closeOnce                             sync.Once
	mu                                    sync.Mutex
	connections                           map[net.Conn]struct{}
	agents                                map[int32]int
	accepted, failures, requests, uploads atomic.Uint64
	handshake, agent, client              connBudget
	active                                atomic.Int64
	clientSoTimeout, agentSoTimeout       atomic.Int64
	clientIDs                             atomic.Uint64
	sessions                              *sessionGate
	abandoned, waitTimeouts, waitRejected atomic.Uint64
	waitLog                               rateLimit
	acceptRetries                         atomic.Uint64
	acceptLog                             rateLimit
	actionLog                             atomic.Bool
	log                                   logSink
	responses                             *responseBudget
	connErrors                            [connErrorClasses]connErrorLog
}

// MaxClientWaiters is the waiting-Client bound for a MaxConnections value: half
// of it, at least 1, so waiting Clients cannot take every Client slot. Agent
// sockets have their own budget (MaxAgentConnections).
func MaxClientWaiters(maxConnections int) int { return max(1, maxConnections/2) }

func NewTCPServer(listener net.Listener, config TCPConfig, handlers TCPHandlers, onError ErrorHandler) (*TCPServer, error) {
	if listener == nil {
		return nil, fmt.Errorf("%w: TCP listener required", ErrMalformed)
	}
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	s := &TCPServer{listener: listener, config: config, handlers: handlers, errors: onError, connections: make(map[net.Conn]struct{}), agents: make(map[int32]int), sessions: newSessionGate(DefaultServicePoolSize, MaxClientWaiters(config.MaxConnections)),
		handshake: newConnBudget("handshake", "TCP", "--max-handshakes", config.MaxHandshakes),
		agent:     newConnBudget("agent", "Agent", "--max-agent-connections", config.MaxAgentConnections),
		client:    newConnBudget("client", "Client", "--max-connections", config.MaxConnections),
		responses: newResponseBudget(config.MaxClientResponseBytes)}
	s.clientSoTimeout.Store(int64(DefaultClientSoTimeout))
	s.agentSoTimeout.Store(int64(DefaultAgentSoTimeout))
	return s, nil
}
func (s *TCPServer) Active() int64 { return s.active.Load() }
func (s *TCPServer) Stats() TCPStats {
	limit, sessions, waiting := s.sessions.snapshot()
	handshakeRejected, agentRejected, clientRejected := s.handshake.rejected.Load(), s.agent.rejected.Load(), s.client.rejected.Load()
	st := TCPStats{
		Accepted: s.accepted.Load(), Rejected: handshakeRejected + agentRejected + clientRejected, Errors: s.failures.Load(),
		ClientRequests: s.requests.Load(), StackUploads: s.uploads.Load(), Active: s.Active(),
		ClientSessions: sessions, ClientWaiting: waiting, ServicePoolSize: limit,
		ClientAbandoned: s.abandoned.Load(), ClientWaitTimeouts: s.waitTimeouts.Load(), ClientWaitRejected: s.waitRejected.Load(),
		HandshakeRejected: handshakeRejected, AgentRejected: agentRejected, ClientRejected: clientRejected,
		Handshaking: len(s.handshake.slots), AgentConnections: len(s.agent.slots), ClientConnections: len(s.client.slots),
		MaxHandshakes: cap(s.handshake.slots), MaxAgentConnections: cap(s.agent.slots), MaxConnections: cap(s.client.slots),
		AcceptRetries: s.acceptRetries.Load(),
	}
	st.ClientResponseBytes, st.ClientResponsePeakBytes, st.MaxClientResponseBytes, st.ClientResponseWaiting, st.ClientResponseWaits, st.ClientResponseRejected = s.responses.stats()
	s.connErrorStats(&st)
	return st
}

// rejectLogEvery is the per-budget interval between rejection log lines.
const rejectLogEvery = 10 * time.Second

// reject counts a socket refused by budget b and makes it look like a server
// that is down: the caller closes it at once, with SO_LINGER 0 so the peer
// gets a reset and the server keeps no TIME_WAIT for a peer retrying every
// second. Nothing is read, written or waited for. object is the Agent objHash
// when its bytes were already buffered.
//
// One Go-only line (Java has no connection limit, so it has none) is written
// per budget every 10 seconds, whatever log_tcp_action_enabled says, to the
// Java log and to the ErrorHandler. Rejections in between are not lost: the
// next line carries "<n> more rejected", and TCPStats has exact totals.
func (s *TCPServer) reject(conn net.Conn, b *connBudget, object *int32) {
	b.rejected.Add(1)
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	if !b.log.allow(time.Now(), rejectLogEvery) {
		b.unlogged.Add(1)
		return
	}
	line := b.actor + " : " + javaSocketAddress(conn.RemoteAddr()) + " rejected"
	if object != nil {
		line += " [" + hexa32(int64(*object)) + "]"
	}
	line += ", " + b.name + " connections=" + strconv.Itoa(len(b.slots)) + "/" + strconv.Itoa(cap(b.slots)) +
		" (Go connection limit, raise " + b.flag + ")"
	if more := b.unlogged.Swap(0); more > 0 {
		line += ", " + strconv.FormatUint(more, 10) + " more rejected since the last line"
	}
	if s.log.enabled() && !s.closed.Load() {
		s.log.emit("", line)
	}
	report(s.errors, "tcp.connections."+b.name, conn.RemoteAddr().String(), fmt.Errorf("%w: %s", ErrLimit, line))
}

// admit moves a classified connection from its handshake slot to budget b.
// The handshake slot goes back first, so no connection holds two slots. On
// false the connection was counted as rejected and must close at once. For an
// Agent magic, reader (if not nil) supplies the objHash for the log line only
// when it is already buffered; admit never waits for it.
func (s *TCPServer) admit(conn net.Conn, lease *connLease, b *connBudget, reader *bufio.Reader) bool {
	lease.release()
	if !b.tryAcquire() {
		var object *int32
		if reader != nil && reader.Buffered() >= 4 {
			if raw, err := reader.Peek(4); err == nil {
				v := int32(binary.BigEndian.Uint32(raw))
				object = &v
			}
		}
		s.reject(conn, b, object)
		return false
	}
	lease.held = b
	return true
}

// SetClientSoTimeout is net_tcp_client_so_timeout_ms. Like Java's accept-time
// Socket.setSoTimeout it applies to Client connections accepted after the call;
// open connections keep their value. Each socket read must complete within d
// (between commands and inside a command body); 0 means no per-read limit
// between commands as in Java, and RequestTimeout per read inside a command.
// No total command deadline applies. Negative is rejected.
func (s *TCPServer) SetClientSoTimeout(d time.Duration) error {
	if d < 0 {
		return fmt.Errorf("%w: negative client so-timeout", ErrMalformed)
	}
	s.clientSoTimeout.Store(int64(d))
	return nil
}

// SetAgentSoTimeout is net_tcp_agent_so_timeout_ms: the per-read limit while an
// Agent response is read. It applies to Agent connections registered after the
// call. 0 means no per-read limit; the request total deadline (min of ctx and
// RequestTimeout) still applies. Negative is rejected.
func (s *TCPServer) SetAgentSoTimeout(d time.Duration) error {
	if d < 0 {
		return fmt.Errorf("%w: negative agent so-timeout", ErrMalformed)
	}
	s.agentSoTimeout.Store(int64(d))
	return nil
}

// SetServicePoolSize is net_tcp_service_pool_size: concurrent Client sessions.
// Increases admit waiting Client connections immediately; decreases stop new
// admissions until enough sessions end. Values below 1 are rejected and the
// previous value stays (Java cannot start with them either).
func (s *TCPServer) SetServicePoolSize(n int) error {
	if n < 1 {
		return fmt.Errorf("%w: service pool size %d", ErrMalformed, n)
	}
	s.sessions.setLimit(n)
	return nil
}

// SetActionLog is log_tcp_action_enabled, read at every log site like Java.
func (s *TCPServer) SetActionLog(enabled bool) { s.actionLog.Store(enabled) }

// SetLogFunc installs the Java Logger bridge; nil disables transport log lines.
func (s *TCPServer) SetLogFunc(fn LogFunc) { s.log.set(fn) }

func (s *TCPServer) logAction(message func() string) {
	if s.actionLog.Load() && s.log.enabled() && !s.closed.Load() {
		s.log.emit("", message())
	}
}
func (s *TCPServer) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		err = s.listener.Close()
		s.mu.Lock()
		conns := make([]net.Conn, 0, len(s.connections))
		for conn := range s.connections {
			conns = append(conns, conn)
		}
		s.mu.Unlock()
		for _, conn := range conns {
			conn.Close()
		}
	})
	return err
}
func (s *TCPServer) Serve(parent context.Context) error {
	if s.closed.Load() || !s.serving.CompareAndSwap(false, true) {
		return ErrClosed
	}
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(ctx, func() { s.Close() })
	var workers sync.WaitGroup
	var result error
	var backoff time.Duration
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if parent.Err() == nil && !s.closed.Load() && temporaryAccept(err) {
				// Out of descriptors or buffers: keep serving the
				// connected Agents and accept again after a pause, as
				// net/http does, instead of stopping the listener.
				backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
				s.acceptRetries.Add(1)
				if s.acceptLog.allow(time.Now(), rejectLogEvery) {
					report(s.errors, "tcp.accept", "", fmt.Errorf("%w (retrying in %v; check the open file limit, ulimit -n)", err, backoff))
				}
				select {
				case <-time.After(backoff):
					continue
				case <-ctx.Done():
				}
			}
			if parent.Err() != nil {
				result = parent.Err()
			} else if !s.closed.Load() {
				result = err
				report(s.errors, "tcp.accept", "", err)
			}
			break
		}
		backoff = 0
		if s.closed.Load() {
			conn.Close()
			break
		}
		// Java sets SO_TIMEOUT on accept; later changes reach new sockets only.
		clientSoTimeout := time.Duration(s.clientSoTimeout.Load())
		// Until its magic is read a socket holds a handshake slot (bounding
		// sockets that send nothing); serveConn moves it to its group budget.
		if !s.handshake.tryAcquire() {
			s.reject(conn, &s.handshake, nil)
			conn.Close()
			continue
		}
		lease := &connLease{held: &s.handshake}
		s.mu.Lock()
		// Close may have raced with Accept before registration.
		if s.closed.Load() {
			s.mu.Unlock()
			conn.Close()
			lease.release()
			break
		}
		s.connections[conn] = struct{}{}
		s.mu.Unlock()
		s.accepted.Add(1)
		s.active.Add(1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() {
				conn.Close()
				s.mu.Lock()
				delete(s.connections, conn)
				s.mu.Unlock()
				s.active.Add(-1)
				lease.release()
			}()
			if err := s.serveConn(ctx, conn, clientSoTimeout, lease); err != nil && ctx.Err() == nil {
				s.failures.Add(1)
				s.reportConnError(conn.RemoteAddr().String(), err, time.Now())
			}
		}()
	}
	cancel()
	stop()
	s.Close()
	workers.Wait()
	return result
}

// arm sets the read policy below the bufio.Reader and the write deadline.
func arm(conn net.Conn, input *idleReader, idle time.Duration, limit time.Time) error {
	input.set(idle, limit)
	return conn.SetWriteDeadline(limit)
}

// serveConn returns nil for a socket refused by its group budget: the refusal
// is counted in TCPStats, not as a connection error.
func (s *TCPServer) serveConn(ctx context.Context, conn net.Conn, clientSoTimeout time.Duration, lease *connLease) error {
	input := &idleReader{conn: conn}
	if err := arm(conn, input, 0, time.Now().Add(s.config.HandshakeTimeout)); err != nil {
		return err
	}
	reader, writer := bufio.NewReaderSize(input, 4096), bufio.NewWriterSize(conn, 4096)
	stream := newStream(reader, writer, s.config)
	magic, err := stream.ReadInt32()
	if err != nil {
		return err
	}
	switch uint32(magic) {
	case TCPClient:
		if !s.admit(conn, lease, &s.client, nil) {
			return nil
		}
		return s.serveClient(ctx, conn, input, reader, stream, writer, clientSoTimeout)
	case TCPAgentV1, TCPAgentV2:
		// The object hash read keeps the HandshakeTimeout deadline.
		if !s.admit(conn, lease, &s.agent, reader) {
			return nil
		}
		object, err := stream.ReadInt32()
		if err != nil {
			return err
		}
		if s.handlers.Agent == nil {
			return fmt.Errorf("%w: agent dispatcher missing", ErrUnsupported)
		}
		version := 1
		if uint32(magic) == TCPAgentV2 {
			version = 2
		}
		if err := arm(conn, input, 0, time.Now().Add(s.config.IdleTimeout)); err != nil {
			return err
		}
		// Java TcpAgentWorker reads net_tcp_agent_so_timeout_ms once at registration.
		agent := &AgentConn{ObjectHash: object, Version: version, Peer: conn.RemoteAddr(), conn: conn, input: input, stream: stream, writer: writer, config: s.config, gate: make(chan struct{}, 1), soTimeout: time.Duration(s.agentSoTimeout.Load()), server: s}
		count := s.agentOpened(object)
		defer s.agentClosed(object)
		s.logAction(func() string {
			return "Agent : " + javaSocketAddress(agent.Peer) + " open [" + hexa32(int64(object)) + "] #" + strconv.Itoa(count)
		})
		defer agent.logClose()
		return s.handlers.Agent(ctx, agent)
	case TCPAgentRequest:
		if !s.admit(conn, lease, &s.agent, reader) {
			return nil
		}
		object, err := stream.ReadInt32()
		if err != nil {
			return err
		}
		return s.serveUploads(ctx, conn, input, object, stream, writer)
	default:
		lease.release()
		s.logAction(func() string { return "Unknown : " + javaSocketAddress(conn.RemoteAddr()) + " drop" })
		return fmt.Errorf("%w: TCP magic %08x", ErrUnsupported, uint32(magic))
	}
}

// agentOpened returns Java's "#n": open Agent connections for the object hash,
// this one included (Java prints its TcpAgentManager queue size after add).
func (s *TCPServer) agentOpened(object int32) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.agents[object]++
	return s.agents[object]
}
func (s *TCPServer) agentClosed(object int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.agents[object]--; s.agents[object] <= 0 {
		delete(s.agents, object)
	}
}
func freeCommand(command string) bool {
	return command == "LOGIN" || command == "SERVER_VERSION" || command == "SERVER_TIME"
}

// invalidSessionError keeps the rejected command for the Java action log line;
// its text and errors.Is identity stay ErrUnauthorized.
type invalidSessionError struct{ command string }

func (e *invalidSessionError) Error() string        { return ErrUnauthorized.Error() }
func (e *invalidSessionError) Is(target error) bool { return target == ErrUnauthorized }

// clientWaitLimit bounds a pool wait: the connection's client so-timeout, or
// RequestTimeout when the so-timeout is 0. A Scouter Client gives up after its
// own read timeout (Client 8s, webapp 30s), so a longer wait only keeps a dead
// request queued.
func (s *TCPServer) clientWaitLimit(soTimeout time.Duration) time.Duration {
	if soTimeout > 0 {
		return soTimeout
	}
	return s.config.RequestTimeout
}

// awaitSession takes a net_tcp_service_pool_size permit. While it waits, a
// probe goroutine peeks (never consumes) the buffered reader. Any peer EOF or
// read error ends the wait even when requests are buffered: a Scouter Client
// never half-closes, so EOF means it gave up and its queued commands must not
// run later. ok=false without an error means the waiter was closed and counted
// (abandoned, timed out or rejected).
func (s *TCPServer) awaitSession(ctx context.Context, conn net.Conn, input *idleReader, reader *bufio.Reader, soTimeout time.Duration) (bool, error) {
	if s.sessions.tryAcquire() {
		return true, nil
	}
	limit := s.clientWaitLimit(soTimeout)
	deadline := time.Now().Add(limit)
	input.set(0, deadline)
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	lost := make(chan error, 1)
	stopProbe := probePeer(input, reader, func(err error) { lost <- err })
	err := s.sessions.acquire(waitCtx, lost)
	stopProbe()
	if err == nil {
		select {
		case <-lost: // the peer left before the grant was observed: never run its commands
			s.sessions.release()
		default:
			return true, nil
		}
		s.abandoned.Add(1)
		return false, nil
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	peer := javaSocketAddress(conn.RemoteAddr())
	switch {
	case errors.Is(err, errWaitFull):
		s.waitRejected.Add(1)
		s.logWait(func() string {
			return "Client : " + peer + " rejected, waiting clients=" + strconv.Itoa(MaxClientWaiters(s.config.MaxConnections)) + " (Go service pool wait limit)"
		})
	case errors.Is(err, context.DeadlineExceeded):
		s.waitTimeouts.Add(1)
		s.logWait(func() string {
			return "Client : " + peer + " closed after waiting " + strconv.FormatInt(limit.Milliseconds(), 10) + "ms for a service pool slot (Go service pool wait limit)"
		})
	default:
		s.abandoned.Add(1)
	}
	return false, nil
}

// probePeer starts a goroutine that peeks (never consumes) the buffered reader
// so a peer EOF or read error is seen while the owner does not read; lost gets
// that error once. Buffered bytes stay for the owner's next read, and a full
// buffer ends the probe silently. The returned stop interrupts the probe read
// and waits for the goroutine, after which the owner reads again. The owner
// must not use reader between probePeer and stop.
func probePeer(input *idleReader, reader *bufio.Reader, lost func(error)) (stop func()) {
	probed := make(chan struct{})
	go func() {
		defer close(probed)
		for {
			n := reader.Buffered() + 1
			if n > reader.Size() {
				return
			}
			if _, err := reader.Peek(n); err != nil {
				if errors.Is(err, errReadInterrupted) || errors.Is(err, os.ErrDeadlineExceeded) {
					return
				}
				lost(err)
				return
			}
		}
	}()
	return func() {
		input.interrupt()
		<-probed
		input.resume()
	}
}

// awaitUpstream runs wait, a handler's wait for an Agent answer
// (Stream.AwaitUpstream), with the command's progress limit held: Java's
// service thread waits for the Agent however long its own read timeout lets
// it, whatever the Client does. Meanwhile the Client socket is probed without
// a read deadline, and a Client EOF or reset ends the command at once with
// ErrClientGone, so the relay becomes an orphan instead of waiting for a
// Client that is gone. A half-open Client (no FIN or RST) is noticed only
// through TCP keepalive (the listener's: net.Listen sets 15s idle, 15s
// interval, 9 probes) or when the wait ends. wait may write responses (a Blob
// relay); the probe only reads. Bytes the Client sends meanwhile stay
// buffered for the next command.
func awaitUpstream(c *progressContext, stream *Stream, input *idleReader, reader *bufio.Reader, readIdle time.Duration, wait func() error) error {
	release := c.hold(stream)
	defer release()
	input.set(0, time.Time{})
	stopProbe := probePeer(input, reader, func(err error) { c.finish(&clientGoneError{cause: err}) })
	// Deferred so a panicking wait still stops the probe and restores the
	// command's read so-timeout before the reader is used again.
	defer func() {
		stopProbe()
		input.set(readIdle, time.Time{})
	}()
	return wait()
}

// logGo writes a Go-only line (no Java counterpart) whatever
// log_tcp_action_enabled says; callers apply their own rate limit.
func (s *TCPServer) logGo(line string) {
	if s.log.enabled() && !s.closed.Load() {
		s.log.emit("", line)
	}
}

// logWait writes a Go-only line (Java waits without a limit, so it has none)
// at most once every 10 seconds, whatever log_tcp_action_enabled says.
func (s *TCPServer) logWait(message func() string) {
	if s.log.enabled() && !s.closed.Load() && s.waitLog.allow(time.Now(), 10*time.Second) {
		s.log.emit("", message())
	}
}
func (s *TCPServer) serveClient(ctx context.Context, conn net.Conn, input *idleReader, reader *bufio.Reader, stream *Stream, writer *bufio.Writer, soTimeout time.Duration) error {
	ok, err := s.awaitSession(ctx, conn, input, reader, soTimeout)
	if !ok {
		return err
	}
	defer s.sessions.release()
	tracker := &clientSessions{id: s.clientIDs.Add(1), observer: s.handlers.Sessions}
	defer tracker.release()
	// Nothing was written yet, so swapping the socket writer loses no bytes.
	writer.Reset(&progressWriter{conn: conn, timeout: s.config.RequestTimeout})
	stream.asClient(writer.Flush)
	peer := javaSocketAddress(conn.RemoteAddr())
	stream.client.budget = &clientBudget{shared: s.responses, wait: min(s.clientWaitLimit(soTimeout), s.config.RequestTimeout), peer: peer, log: s.logGo}
	s.logAction(func() string {
		_, active, _ := s.sessions.snapshot()
		return "Client : " + peer + " open #" + strconv.Itoa(active)
	})
	closedByCommand, err := s.clientSession(ctx, conn, input, reader, stream, writer, soTimeout, tracker)
	if !closedByCommand && ctx.Err() == nil {
		s.logAction(func() string { return clientClosedLine(peer, err, s.sessions) })
	}
	return err
}

// clientClosedLine follows ServiceWorker's catch blocks: EOF and socket timeout
// print "closed" (Java adds a stack trace on stderr for timeouts), any other
// failure prints the exception and the active worker count. A Client that left
// during an upstream wait (ErrClientGone, EOF or reset) prints "closed" as
// well; Java notices it only at its next write or read, whose exception
// decides between "closed" and "closed <exception> workers=N".
func clientClosedLine(peer string, err error, sessions *sessionGate) string {
	var netErr net.Error
	var invalid *invalidSessionError
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrClientGone) ||
		errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) ||
		(errors.As(err, &netErr) && netErr.Timeout()) {
		return "Client : " + peer + " closed"
	}
	_, active, _ := sessions.snapshot()
	cause := sanitizeLogText(err.Error())
	if errors.As(err, &invalid) {
		cause = "java.lang.RuntimeException: Invalid session key : " + sanitizeLogText(invalid.command)
	}
	return "Client : " + peer + " closed " + cause + " workers=" + strconv.Itoa(active)
}
func (s *TCPServer) clientSession(ctx context.Context, conn net.Conn, input *idleReader, reader *bufio.Reader, stream *Stream, writer *bufio.Writer, soTimeout time.Duration, tracker *clientSessions) (bool, error) {
	authenticated := false
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		stream.reset()
		if err := arm(conn, input, soTimeout, time.Time{}); err != nil {
			return false, err
		}
		command, err := stream.ReadText()
		if err != nil {
			if errors.Is(err, io.EOF) && stream.readBytes == 0 {
				return false, nil
			}
			return false, err
		}
		if command == "CLOSE" {
			return true, nil
		} // CLOSE does not carry a session.
		// No total command deadline (Java has none): reads inside the command
		// keep the so-timeout (RequestTimeout when it is 0), each write chunk
		// gets RequestTimeout (progressWriter) and the handler ends after
		// RequestTimeout without any stream progress outside an upstream wait
		// (client_progress.go).
		readIdle := soTimeout
		if readIdle <= 0 {
			readIdle = s.config.RequestTimeout
		}
		input.set(readIdle, time.Time{})
		requestCtx, cancel := withProgress(ctx, stream, s.config.RequestTimeout)
		stream.client.done = requestCtx.Done()
		stream.client.await = func(wait func() error) error {
			return awaitUpstream(requestCtx, stream, input, reader, readIdle, wait)
		}
		err = s.clientCommand(requestCtx, conn, command, stream, writer, &authenticated, tracker)
		stream.client.done, stream.client.await = nil, nil
		cancel()
		if err != nil {
			return false, err
		}
	}
}
func (s *TCPServer) clientCommand(ctx context.Context, conn net.Conn, command string, stream *Stream, writer *bufio.Writer, authenticated *bool, tracker *clientSessions) error {
	session, err := stream.ReadInt64()
	if err != nil {
		return err
	}
	// Track before Authorize: the observer decides on its own whether an
	// expired id may be held (it must not revive it).
	tracker.use(session)
	request := ClientRequest{Command: command, Session: session, Authenticated: *authenticated, Peer: conn.RemoteAddr(), Conn: ClientConn{ID: tracker.id, tracker: tracker}}
	// Original sessionOk is a connection latch. Free commands do not set it.
	if !*authenticated && !freeCommand(command) {
		if s.handlers.Authorize != nil {
			*authenticated, err = s.handlers.Authorize(ctx, request)
			if err != nil {
				return err
			}
		}
		if !*authenticated {
			if err := stream.WriteByte(InvalidSession); err != nil {
				return err
			}
			if err := writer.Flush(); err != nil {
				return err
			}
			return &invalidSessionError{command: command}
		}
	}
	request.Authenticated = *authenticated
	if s.handlers.Client == nil {
		return fmt.Errorf("%w: client dispatcher missing", ErrUnsupported)
	}
	if err := s.handlers.Client(ctx, request, stream); err != nil {
		// TcpWorker finally closes BufferedDataOutput, flushing emitted items
		// without a success terminator when the handler fails.
		return errors.Join(err, writer.Flush())
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := stream.WriteByte(NoNext); err != nil {
		return errors.Join(err, writer.Flush())
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	s.requests.Add(1)
	return nil
}
func (s *TCPServer) serveUploads(ctx context.Context, conn net.Conn, input *idleReader, object int32, stream *Stream, writer *bufio.Writer) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		stream.reset()
		if err := arm(conn, input, 0, time.Now().Add(s.config.IdleTimeout)); err != nil {
			return err
		}
		command, err := stream.ReadInt32()
		if err != nil {
			if errors.Is(err, io.EOF) && stream.readBytes == 0 {
				return nil
			}
			return err
		}
		if uint32(command) != TCPSendStack {
			return fmt.Errorf("%w: agent request %08x", ErrUnsupported, uint32(command))
		}
		deadline := time.Now().Add(s.config.RequestTimeout)
		if err := arm(conn, input, 0, deadline); err != nil {
			return err
		}
		requestCtx, cancel := context.WithDeadline(ctx, deadline)
		err = s.stackCommand(requestCtx, conn, object, stream, writer)
		cancel()
		if err != nil {
			return err
		}
	}
}
func (s *TCPServer) stackCommand(ctx context.Context, conn net.Conn, object int32, stream *Stream, writer *bufio.Writer) error {
	meta := StackUpload{ObjectHash: object, Peer: conn.RemoteAddr()}
	var err error
	meta.StartTime, err = stream.ReadInt64()
	if err != nil {
		return err
	}
	meta.ObjectName, err = stream.ReadText()
	if err != nil {
		return err
	}
	meta.FileName, err = stream.ReadText()
	if err != nil {
		return err
	}
	meta.Size, err = stream.ReadInt64()
	if err != nil {
		return err
	}
	if meta.Size < 0 {
		return ErrMalformed
	}
	if meta.Size > s.config.MaxUploadBytes {
		return ErrLimit
	}
	if s.handlers.Stack == nil {
		return fmt.Errorf("%w: stack handler missing", ErrUnsupported)
	}
	body := &io.LimitedReader{R: stream.reader, N: meta.Size}
	if err := s.handlers.Stack(ctx, meta, body); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if body.N != 0 {
		return ErrUnreadBody
	}
	if err := stream.WriteByte(OK); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	s.uploads.Add(1)
	return nil
}

// AgentConn serializes server requests. NoNEXT is one byte with no V2 length.
// Request gives custom command bodies to a callback; RequestPack handles ordinary
// Pack responses. Commands with raw Blob/stream responses must use Request.
type AgentConn struct {
	ObjectHash int32
	Version    int
	Peer       net.Addr
	conn       net.Conn
	input      *idleReader
	stream     *Stream
	writer     *bufio.Writer
	config     TCPConfig
	gate       chan struct{}
	closed     atomic.Bool
	soTimeout  time.Duration // net_tcp_agent_so_timeout_ms at registration
	server     *TCPServer
	closeLog   sync.Once
}

func (a *AgentConn) Close() error { a.closed.Store(true); a.logClose(); return a.conn.Close() }

// logClose prints Java TcpAgentWorker.close()'s line once per connection
// (Java may repeat it when close() runs more than once).
func (a *AgentConn) logClose() {
	if a.server == nil {
		return
	}
	a.closeLog.Do(func() {
		a.server.logAction(func() string { return "Agent : " + javaSocketAddress(a.Peer) + " close" })
	})
}

// Closed reports a locally observed close; it does not probe an idle remote peer.
func (a *AgentConn) Closed() bool { return a.closed.Load() }

// agentResponseValues bounds the entries of one collection and the values of
// one Agent response item. Java readPack has no such cap, and the request
// decoder's 4096/16384 are below real answers: OBJECT_ACTIVE_SERVICE_LIST is
// 12 parallel lists (AgentThread.java), so 1,366 active services already
// passed 16384 and closed the Agent session. A decoded Value is about 250
// bytes, so one item at this cap holds at most about 64MiB besides its bytes.
const agentResponseValues = 1 << 18

// AgentLimits selects one Agent request's response budget and total deadline.
// Java TcpAgentWorker has no size or total-time limit, only SO_TIMEOUT per
// read. Go keeps that per-read so-timeout and adds these bounds per command.
type AgentLimits struct {
	// ItemBytes bounds one HasNEXT item as read from the socket (a V1 Pack, a
	// V2 IntBytes body or a raw Blob chunk) and every Blob/Text Value decoded
	// inside it. 0 selects TCPConfig.MaxFrameBytes.
	ItemBytes int
	// TotalBytes bounds the whole response, flags included. 0 selects
	// ItemBytes; negative means no total, for chunk streams that the caller
	// relays one item at a time (only one item is held in memory).
	TotalBytes int64
	// Transfer is the total deadline on top of the so-timeout: the request ends
	// at now+so_timeout+Transfer or at the ctx deadline, whichever is first.
	// 0 selects TCPConfig.RequestTimeout; negative means no total deadline, so
	// only the ctx and the per-read so-timeout end the request.
	Transfer time.Duration
}

// Request uses the default AgentLimits: TCPConfig.MaxFrameBytes for the whole
// response and a total deadline of so_timeout+RequestTimeout.
func (a *AgentConn) Request(ctx context.Context, command string, parameter protocol.Pack, receive func(context.Context, *Stream) error) error {
	return a.RequestLimited(ctx, command, parameter, AgentLimits{}, receive)
}

// RequestLimited writes one command and passes each HasNEXT item to receive.
// The Stream given to receive is valid only during that call; its read budget
// is the item's (AgentLimits.ItemBytes, less when TotalBytes leaves less).
// Every response read must see input within the registration so-timeout (Java
// SO_TIMEOUT). A 0 so-timeout (Java: no per-read limit) keeps no per-read limit
// when the request has a total deadline, and falls back to RequestTimeout per
// read when it has none. Any error closes the connection, as Java
// TcpAgentWorker.read/readByte/readPack close the socket when a read or the
// response handler throws. The end of ctx is such an error too, so a caller
// that must keep the session when its own caller leaves passes the
// connection's lifetime context instead (internal/server agentPool.relay).
func (a *AgentConn) RequestLimited(ctx context.Context, command string, parameter protocol.Pack, limits AgentLimits, receive func(context.Context, *Stream) error) (err error) {
	if receive == nil {
		return fmt.Errorf("%w: agent response callback missing", ErrMalformed)
	}
	if a.gate == nil || a.input == nil {
		return fmt.Errorf("%w: uninitialized AgentConn", ErrMalformed)
	}
	if limits.ItemBytes < 0 {
		return fmt.Errorf("%w: negative agent item limit", ErrMalformed)
	}
	item := limits.ItemBytes
	if item == 0 {
		item = a.config.MaxFrameBytes
	}
	total := limits.TotalBytes
	if total == 0 {
		total = int64(item)
	}
	select {
	case a.gate <- struct{}{}:
		defer func() { <-a.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	now := time.Now()
	idle := a.soTimeout
	var deadline time.Time
	if limits.Transfer >= 0 {
		transfer := limits.Transfer
		if transfer == 0 {
			transfer = a.config.RequestTimeout
		}
		deadline = now.Add(idle + transfer)
	} else if idle == 0 {
		idle = a.config.RequestTimeout
	}
	if contextDeadline, ok := ctx.Deadline(); ok && (deadline.IsZero() || contextDeadline.Before(deadline)) {
		deadline = contextDeadline
	}
	// The command write is small; without a total deadline it still must not
	// block longer than one read may wait.
	writeLimit := deadline
	if writeLimit.IsZero() {
		writeLimit = now.Add(idle)
	}
	a.input.set(idle, deadline)
	if err = a.conn.SetWriteDeadline(writeLimit); err != nil {
		a.Close()
		return err
	}
	var requestCtx context.Context
	var cancel context.CancelFunc
	if deadline.IsZero() {
		requestCtx, cancel = context.WithCancel(ctx)
	} else {
		requestCtx, cancel = context.WithDeadline(ctx, deadline)
	}
	defer cancel()
	stop := context.AfterFunc(requestCtx, func() { a.Close() })
	defer stop()
	defer func() {
		if err != nil {
			a.Close()
		}
	}()
	// One Stream per request over the connection's buffered reader and writer,
	// with this command's item budget and codec limits.
	codec := a.config.CodecLimits
	codec.MaxBytes, codec.MaxBlobBytes = item, item
	if codec.MaxCollectionEntries < agentResponseValues {
		codec.MaxCollectionEntries = agentResponseValues
	}
	if codec.MaxTotalValues < agentResponseValues {
		codec.MaxTotalValues = agentResponseValues
	}
	config := a.config
	config.MaxFrameBytes, config.CodecLimits = item, codec
	stream := newStream(a.stream.reader, a.stream.writer, config)
	if a.Version != 1 && a.Version != 2 {
		return ErrUnsupported
	}
	if len(command) > a.config.MaxTextBytes {
		return ErrLimit
	}
	// Encode the whole command before writing, so a refused command (limit or
	// invalid data) puts no byte on the wire: V2 sends it as one IntBytes body,
	// V1 inline (the Agent reads readText then readPack).
	encoder := protocol.NewEncoder(codec)
	encoder.Text(command)
	encoder.Pack(parameter)
	body, e := encoder.Bytes()
	if e != nil {
		return e
	}
	if a.Version == 1 {
		_, err = stream.Write(body)
	} else {
		err = stream.WriteIntBytes(body)
	}
	if err != nil {
		return err
	}
	if err = a.writer.Flush(); err != nil {
		return err
	}
	if !deadline.IsZero() {
		// Response reads have no write; keep the write deadline only for the command.
		_ = a.conn.SetWriteDeadline(deadline)
	}
	var read int64
	for {
		if total > 0 && read >= total {
			return fmt.Errorf("%w: agent response reached %d bytes", ErrLimit, total)
		}
		stream.reset()
		stream.config.MaxFrameBytes = 1
		flag, e := stream.ReadByte()
		if e != nil {
			if requestCtx.Err() != nil {
				return requestCtx.Err()
			}
			return e
		}
		read++
		switch flag {
		case NoNext:
			return requestCtx.Err()
		case HasNext:
			budget := item
			if total > 0 && total-read < int64(budget) {
				budget = int(total - read)
			}
			stream.reset()
			stream.config.MaxFrameBytes = budget
			stream.config.CodecLimits.MaxBytes, stream.config.CodecLimits.MaxBlobBytes = budget, budget
			err = receive(requestCtx, stream)
			read += int64(stream.readBytes)
			if err != nil {
				return err
			}
			if err = requestCtx.Err(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: agent response flag %02x", ErrMalformed, flag)
		}
	}
}

// RequestPack uses the default AgentLimits; see RequestPackLimited.
func (a *AgentConn) RequestPack(ctx context.Context, command string, parameter protocol.Pack, receive func(context.Context, protocol.Pack) error) error {
	return a.RequestPackLimited(ctx, command, parameter, AgentLimits{}, receive)
}

// RequestPackLimited decodes each HasNEXT item as a Pack (V1 inline, V2 one
// IntBytes body) within the item budget.
func (a *AgentConn) RequestPackLimited(ctx context.Context, command string, parameter protocol.Pack, limits AgentLimits, receive func(context.Context, protocol.Pack) error) error {
	if receive == nil {
		return fmt.Errorf("%w: Pack response callback missing", ErrMalformed)
	}
	return a.RequestLimited(ctx, command, parameter, limits, func(requestCtx context.Context, stream *Stream) error {
		var pack protocol.Pack
		var err error
		if a.Version == 1 {
			pack, err = stream.ReadPack()
		} else {
			var body []byte
			body, err = stream.ReadIntBytes()
			if err == nil {
				pack, err = protocol.DecodePack(body, stream.config.CodecLimits)
			}
		}
		if err != nil {
			return err
		}
		return receive(requestCtx, pack)
	})
}

// temporaryAccept reports an Accept error that clears by itself: descriptor
// or buffer exhaustion and a connection aborted before it was accepted.
func temporaryAccept(err error) bool {
	for _, e := range []error{syscall.EMFILE, syscall.ENFILE, syscall.ENOBUFS, syscall.ENOMEM, syscall.ECONNABORTED} {
		if errors.Is(err, e) {
			return true
		}
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
