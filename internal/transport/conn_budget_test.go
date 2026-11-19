package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// budgetHandlers serves SERVER_TIME and parks every Agent until shutdown.
func budgetHandlers(agents chan<- *AgentConn) TCPHandlers {
	handlers := serverTimeHandlers()
	handlers.Agent = func(ctx context.Context, a *AgentConn) error { agents <- a; <-ctx.Done(); return ctx.Err() }
	return handlers
}

// startTCPEvents is startTCP with an ErrorHandler that collects events.
func startTCPEvents(t *testing.T, config TCPConfig, handlers TCPHandlers) (*TCPServer, net.Listener, context.CancelFunc, <-chan error, func() []ErrorEvent) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var events []ErrorEvent
	server, err := NewTCPServer(listener, config, handlers, func(e ErrorEvent) { mu.Lock(); events = append(events, e); mu.Unlock() })
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() { cancel(); server.Close() })
	return server, listener, cancel, done, func() []ErrorEvent {
		mu.Lock()
		defer mu.Unlock()
		return append([]ErrorEvent(nil), events...)
	}
}

// expectRejected waits for a budget rejection: the server closes with a reset
// (SO_LINGER 0), so the peer sees EOF or ECONNRESET without any byte.
func expectRejected(t *testing.T, conn net.Conn, within time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	conn.SetReadDeadline(start.Add(within))
	raw, err := io.ReadAll(conn)
	if len(raw) != 0 || (err != nil && !errors.Is(err, syscall.ECONNRESET)) {
		t.Fatalf("rejected connection read %x err=%v", raw, err)
	}
	return time.Since(start)
}

// dialRejected dials, sends payload and expects a budget rejection. The reset
// may already fail the dial (macOS reports it from connect), as a down
// server would.
func dialRejected(t *testing.T, listener net.Listener, payload []byte) time.Duration {
	t.Helper()
	start := time.Now()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		if !errors.Is(err, syscall.ECONNRESET) {
			t.Fatal(err)
		}
		return time.Since(start)
	}
	defer conn.Close()
	conn.Write(payload)
	return expectRejected(t, conn, time.Second)
}

func agentHeader(magic uint32, object int32) []byte {
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], magic)
	binary.BigEndian.PutUint32(header[4:], uint32(object))
	return header[:]
}

func TestTCPDefaultConnectionBudgets(t *testing.T) {
	config, err := TCPConfig{}.normalized()
	if err != nil {
		t.Fatal(err)
	}
	if config.MaxHandshakes != DefaultMaxHandshakes || config.MaxAgentConnections != DefaultMaxAgentConnections || config.MaxConnections != DefaultMaxConnections ||
		DefaultMaxHandshakes != 2048 || DefaultMaxAgentConnections != 16384 || DefaultMaxConnections != 128 {
		t.Fatalf("%+v", config)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server, err := NewTCPServer(listener, TCPConfig{MaxConnections: 1024}, TCPHandlers{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st := server.Stats(); st.MaxHandshakes != 2048 || st.MaxAgentConnections != 16384 || st.MaxConnections != 1024 {
		t.Fatalf("%+v", st)
	}
}

// A full Client budget (sessions plus waiters) does not keep Agents out.
func TestTCPClientBudgetFullStillAdmitsAgents(t *testing.T) {
	agents := make(chan *AgentConn, 4)
	server, listener, cancel, done, events := startTCPEvents(t, TCPConfig{MaxConnections: 2}, budgetHandlers(agents))
	logs := &logCollector{}
	server.SetLogFunc(logs.add)
	server.SetServicePoolSize(1)
	occupy(t, listener)
	waiter := dialRaw(t, listener)
	waiter.Write(clientBytes(t, true, "SERVER_TIME")) // MaxClientWaiters(2) == 1
	waitFor(t, "waiter", func() bool { return server.Stats().ClientWaiting == 1 })

	dialRejected(t, listener, clientBytes(t, true, "SERVER_TIME"))
	waitFor(t, "client rejection", func() bool { return server.Stats().ClientRejected == 1 })

	registerAgent(t, listener, agents)
	registerAgentV2(t, listener, agents)
	st := server.Stats()
	if st.ClientConnections != 2 || st.AgentConnections != 2 || st.AgentRejected != 0 || st.HandshakeRejected != 0 || st.Rejected != 1 || st.ClientWaitRejected != 0 {
		t.Fatalf("%+v", st)
	}
	lines := logs.snapshot()
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "Client : /127.0.0.1:") || !strings.HasSuffix(lines[0], " rejected, client connections=2/2 (Go connection limit, raise --max-connections)") {
		t.Fatalf("lines %q", lines)
	}
	got := events()
	if len(got) != 1 || got[0].Operation != "tcp.connections.client" || !errors.Is(got[0].Err, ErrLimit) || !strings.Contains(got[0].Err.Error(), "--max-connections") {
		t.Fatalf("events %+v", got)
	}
	stopTCP(t, cancel, done)
}

// Only Agent sockets (V1, V2, TCP_AGENT_REQ) are refused at the Agent limit; a
// freed Agent slot is reusable.
func TestTCPAgentBudgetRejectsOnlyAgents(t *testing.T) {
	agents := make(chan *AgentConn, 4)
	server, listener, cancel, done, events := startTCPEvents(t, TCPConfig{MaxAgentConnections: 2}, budgetHandlers(agents))
	logs := &logCollector{}
	server.SetLogFunc(logs.add)
	registerAgent(t, listener, agents)
	registerAgentV2(t, listener, agents)

	for i, header := range [][]byte{agentHeader(TCPAgentV2, 1234), agentHeader(TCPAgentV1, 9), agentHeader(TCPAgentRequest, 9)} {
		if elapsed := dialRejected(t, listener, header); elapsed > 500*time.Millisecond {
			t.Fatalf("rejected agent closed after %v", elapsed)
		}
		waitFor(t, "agent rejection", func() bool { return server.Stats().AgentRejected == uint64(i+1) })
	}
	select {
	case a := <-agents:
		t.Fatalf("agent over the limit registered: %+v", a)
	default:
	}
	client := dialRaw(t, listener)
	client.Write(clientBytes(t, true, "SERVER_TIME"))
	expectBytes(t, client, serverTimeResponse, time.Second)

	st := server.Stats()
	if st.AgentConnections != 2 || st.MaxAgentConnections != 2 || st.ClientConnections != 1 || st.Rejected != 3 || st.ClientRejected != 0 || st.HandshakeRejected != 0 || st.Errors != 0 {
		t.Fatalf("%+v", st)
	}
	// Three rejections, one line per 10 seconds; the first names the objHash.
	lines := logs.snapshot()
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "Agent : /127.0.0.1:") ||
		!strings.HasSuffix(lines[0], " rejected ["+hexa32(1234)+"], agent connections=2/2 (Go connection limit, raise --max-agent-connections)") {
		t.Fatalf("lines %q", lines)
	}
	// The next line, once the interval passed, carries the rejections in between.
	server.agent.log.last.Store(0)
	dialRejected(t, listener, agentHeader(TCPAgentV2, 5))
	waitFor(t, "fourth rejection", func() bool { return server.Stats().AgentRejected == 4 })
	lines = logs.snapshot()
	if len(lines) != 2 || !strings.HasSuffix(lines[1], " rejected [5], agent connections=2/2 (Go connection limit, raise --max-agent-connections), 2 more rejected since the last line") {
		t.Fatalf("lines %q", lines)
	}
	got := events()
	if len(got) != 2 {
		t.Fatalf("events %+v", got)
	}
	for _, e := range got {
		if e.Operation != "tcp.connections.agent" || !errors.Is(e.Err, ErrLimit) {
			t.Fatalf("event %+v", e)
		}
	}
	stopTCP(t, cancel, done)
	if st := server.Stats(); st.AgentConnections != 0 || st.ClientConnections != 0 || st.Handshaking != 0 {
		t.Fatalf("slots after shutdown %+v", st)
	}
}

// A freed Agent slot admits the next Agent without a restart.
func TestTCPAgentSlotReusedAfterClose(t *testing.T) {
	agents := make(chan *AgentConn, 4)
	release := make(chan struct{}, 4)
	handlers := TCPHandlers{Agent: func(ctx context.Context, a *AgentConn) error {
		agents <- a
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	server, listener, cancel, done := startTCP(t, TCPConfig{MaxAgentConnections: 1}, handlers)
	registerAgentV2(t, listener, agents)
	dialRejected(t, listener, agentHeader(TCPAgentV2, 8))
	release <- struct{}{}
	waitFor(t, "agent slot free", func() bool { return server.Stats().AgentConnections == 0 })
	registerAgentV2(t, listener, agents)
	if st := server.Stats(); st.AgentConnections != 1 || st.AgentRejected != 1 {
		t.Fatalf("%+v", st)
	}
	stopTCP(t, cancel, done)
}

// Sockets that never send a magic fill only the handshake budget: classified
// Agents and Clients keep working and new ones are admitted once it drains.
func TestTCPSlowHandshakesDoNotAffectClassified(t *testing.T) {
	agents := make(chan *AgentConn, 4)
	server, listener, cancel, done, events := startTCPEvents(t, TCPConfig{MaxHandshakes: 2, HandshakeTimeout: 2 * time.Second}, budgetHandlers(agents))
	agentConn, agent := registerAgent(t, listener, agents)
	logs := &logCollector{}
	server.SetLogFunc(logs.add)
	session := occupy(t, listener)

	silent := []net.Conn{dialRaw(t, listener), dialRaw(t, listener)}
	waitFor(t, "handshakes held", func() bool { return server.Stats().Handshaking == 2 })
	if elapsed := dialRejected(t, listener, nil); elapsed > 500*time.Millisecond {
		t.Fatalf("over-limit handshake closed after %v", elapsed)
	}
	waitFor(t, "handshake rejection", func() bool { return server.Stats().HandshakeRejected == 1 })

	// The registered Agent still answers a server request.
	agentConn.Write([]byte{NoNext})
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	err := agent.RequestPack(ctx, "KEEP_ALIVE", &protocol.MapPack{}, func(context.Context, protocol.Pack) error { return nil })
	stop()
	if err != nil || agent.Closed() {
		t.Fatalf("agent request %v closed=%v", err, agent.Closed())
	}
	// The Client session still runs commands.
	session.Write(clientBytes(t, false, "SERVER_TIME"))
	expectBytes(t, session, serverTimeResponse, time.Second)

	for _, conn := range silent {
		conn.Close()
	}
	waitFor(t, "handshakes released", func() bool { return server.Stats().Handshaking == 0 })
	registerAgentV2(t, listener, agents)
	fresh := dialRaw(t, listener)
	fresh.Write(clientBytes(t, true, "SERVER_TIME"))
	waitFor(t, "fresh client classified", func() bool { return server.Stats().ClientConnections == 2 })

	st := server.Stats()
	if st.AgentConnections != 2 || st.Rejected != 1 || st.AgentRejected != 0 || st.ClientRejected != 0 {
		t.Fatalf("%+v", st)
	}
	if lines := logs.snapshot(); len(lines) != 1 || !strings.HasPrefix(lines[0], "TCP : /127.0.0.1:") ||
		!strings.HasSuffix(lines[0], " rejected, handshake connections=2/2 (Go connection limit, raise --max-handshakes)") {
		t.Fatalf("lines %q", lines)
	}
	got := events()
	if len(got) == 0 || got[0].Operation != "tcp.connections.handshake" {
		t.Fatalf("events %+v", got)
	}
	for _, e := range got[1:] { // peers that closed before their magic are connection errors
		if strings.HasPrefix(e.Operation, "tcp.connections.") {
			t.Fatalf("extra rejection %+v", e)
		}
	}
	stopTCP(t, cancel, done)
}

// Every dial ends in exactly one budget outcome and no slot leaks, under a
// concurrent mix of Agents, Clients, unknown magics and silent sockets.
func TestTCPConnectionBudgetAccountingRace(t *testing.T) {
	handlers := serverTimeHandlers()
	handlers.Agent = func(ctx context.Context, a *AgentConn) error { return nil }
	handlers.Stack = nil
	server, listener, cancel, done := startTCP(t, TCPConfig{MaxHandshakes: 3, MaxAgentConnections: 2, MaxConnections: 2, HandshakeTimeout: 100 * time.Millisecond}, handlers)
	server.SetClientSoTimeout(100 * time.Millisecond)
	const dials = 120
	var wg sync.WaitGroup
	for i := 0; i < dials; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", listener.Addr().String())
			if err != nil {
				if !errors.Is(err, syscall.ECONNRESET) { // a rejection reset seen by connect
					t.Error(err)
				}
				return
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(3 * time.Second))
			switch i % 4 {
			case 0:
				conn.Write(agentHeader(TCPAgentV2, int32(i)))
			case 1:
				conn.Write(clientBytes(t, true, "SERVER_TIME", "CLOSE"))
			case 2:
				conn.Write([]byte{0, 0, 0, 0}) // unknown magic
			case 3: // silent: the handshake deadline closes it
			}
			buf := make([]byte, 64)
			for {
				if _, err := conn.Read(buf); err != nil {
					return
				}
			}
		}(i)
	}
	wg.Wait()
	waitFor(t, "all slots free", func() bool {
		st := server.Stats()
		return st.Active == 0 && st.Handshaking == 0 && st.AgentConnections == 0 && st.ClientConnections == 0 && st.ClientWaiting == 0 && st.ClientSessions == 0
	})
	st := server.Stats()
	if st.Accepted+st.HandshakeRejected != dials || st.Rejected != st.HandshakeRejected+st.AgentRejected+st.ClientRejected {
		t.Fatalf("%+v", st)
	}
	t.Logf("%+v", st)
	// Slots are reusable after the burst.
	conn := dialRaw(t, listener)
	conn.Write(clientBytes(t, true, "SERVER_TIME"))
	expectBytes(t, conn, serverTimeResponse, time.Second)
	stopTCP(t, cancel, done)
}
