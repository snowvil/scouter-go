package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// Orphan relay tests (C4, agent_pool_relay.go): a Pack relay whose caller
// leaves first keeps reading under its own relay deadline and returns the
// Agent session to the pool, like Java TcpAgentWorker.

func e2eObjHashParam(t *testing.T, hash int32) []byte {
	t.Helper()
	body, err := protocol.EncodePack(&protocol.MapPack{Entries: []protocol.Entry{
		{Key: "objHash", Value: protocol.Value{Type: protocol.ValueDecimal, Int: int64(hash)}}}}, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// orphanHarness is newPoolTCPHarness with the server context and shutdown in
// the test's hands. soTimeout 0 keeps the server default
// net_tcp_agent_so_timeout_ms (60s); the relay total deadline is so_timeout +
// RequestTimeout 3s.
type orphanHarness struct {
	*poolTCPHarness
	stop func() error
	logs *orphanLogs
}

type orphanLogs struct {
	mu    sync.Mutex
	lines []string
}

func (l *orphanLogs) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

// wait returns the lines once there are n of them (a rejected relay's line
// is written after its caller returned).
func (l *orphanLogs) wait(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		l.mu.Lock()
		lines := append([]string(nil), l.lines...)
		l.mu.Unlock()
		if len(lines) >= n || time.Now().After(deadline) {
			if len(lines) != n {
				t.Fatalf("orphan log lines %q, want %d", lines, n)
			}
			return lines
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newOrphanHarness(t *testing.T, soTimeout time.Duration) *orphanHarness {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	logs := &orphanLogs{}
	p := &agentPool{agents: make(map[int32][]*agentSlot)}
	p.orphans.logf = logs.logf
	server, err := transport.NewTCPServer(listener, transport.TCPConfig{RequestTimeout: 3 * time.Second}, transport.TCPHandlers{Agent: p.handle}, nil)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	if soTimeout > 0 {
		if err := server.SetAgentSoTimeout(soTimeout); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	var once sync.Once
	var stopErr error
	stop := func() error {
		once.Do(func() {
			cancel()
			_ = server.Close()
			select {
			case stopErr = <-done:
			case <-time.After(2 * time.Second):
				stopErr = errors.New("pool handler leaked during shutdown")
			}
		})
		return stopErr
	}
	t.Cleanup(func() {
		if err := stop(); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	})
	return &orphanHarness{poolTCPHarness: &poolTCPHarness{pool: p, address: listener.Addr().String()}, stop: stop, logs: logs}
}

func (h *orphanHarness) slot(t *testing.T, hash int32) *agentSlot {
	t.Helper()
	h.pool.mu.Lock()
	defer h.pool.mu.Unlock()
	if len(h.pool.agents[hash]) != 1 {
		t.Fatalf("slots for %d: %d", hash, len(h.pool.agents[hash]))
	}
	return h.pool.agents[hash][0]
}

// orphan starts cmd, lets the fake Agent read it and ends the caller's
// context; it returns once the caller got its error.
func (h *orphanHarness) orphan(t *testing.T, peer *poolFakeAgent, hash int32, cmd string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startPoolRequest(h.pool, ctx, hash, cmd)
	if got, _ := peer.readRequest(t); got != cmd {
		t.Fatal(got)
	}
	cancel()
	select {
	case r := <-done:
		if !errors.Is(r.err, context.Canceled) || r.packs != nil {
			t.Fatalf("caller result %+v %v", r.packs, r.err)
		}
	case <-time.After(time.Second):
		t.Fatal("caller did not return when its context ended")
	}
}

func TestAgentPoolOrphanReadsToEndAndKeepsSession(t *testing.T) {
	h := newOrphanHarness(t, 0)
	peer := h.dial(t, 42)
	h.orphan(t, peer, 42, "OBJECT_HEAPHISTO")
	slot := h.slot(t, 42)
	if slot.conn.Closed() || !slot.busy {
		t.Fatalf("orphan closed=%v busy=%v", slot.conn.Closed(), slot.busy)
	}
	if s := h.pool.orphans.snapshot(); s.Active != 1 || s.Orphaned != 1 || s.Bytes != int64(agentLargePackBytes) {
		t.Fatalf("orphan stats %+v", s)
	}
	// A retry while the orphan runs finds no idle session (Java
	// RequestQueue.get returns null): no second command reaches the Agent.
	if packs, err := h.pool.request(context.Background(), 42, "OBJECT_HEAPHISTO", &protocol.MapPack{}); packs != nil || err != nil {
		t.Fatalf("retry during orphan %+v %v", packs, err)
	}
	peer.packResponse(t, 1)
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 1 && list[0] == slot && !list[0].busy }, 42)
	if slot.conn.Closed() {
		t.Fatal("orphan closed the session after a complete answer")
	}
	if s := h.pool.orphans.snapshot(); s.Active != 0 || s.Bytes != 0 || s.Completed != 1 || s.Failed != 0 || s.Rejected != 0 {
		t.Fatalf("orphan stats %+v", s)
	}
	if lines := h.logs.wait(t, 1); !strings.Contains(lines[0], "agent relay OBJECT_HEAPHISTO objHash=42 answered") || !strings.Contains(lines[0], "the agent session kept") {
		t.Fatal(lines)
	}
	// The same socket serves the next command.
	next := startPoolRequest(h.pool, context.Background(), 42, "HOST_TOP")
	if cmd, _ := peer.readRequest(t); cmd != "HOST_TOP" {
		t.Fatal(cmd)
	}
	peer.packResponse(t, 2)
	awaitPoolRequest(t, next, 2)
}

func TestAgentPoolOrphanReadTimeoutClosesSession(t *testing.T) {
	h := newOrphanHarness(t, time.Second)
	peer := h.dial(t, 42)
	started := time.Now()
	h.orphan(t, peer, 42, "HOST_TOP")
	// No answer: the per-read so_timeout (1s here) ends the orphan and closes
	// the session, like a Java read timeout (TcpAgentWorker.readByte).
	deadline := time.After(5 * time.Second)
	for {
		if s := h.pool.orphans.snapshot(); s.Failed == 1 {
			if s.Active != 0 || s.Bytes != 0 {
				t.Fatalf("orphan stats %+v", s)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatalf("orphan did not end at its read timeout: %+v", h.pool.orphans.snapshot())
		case <-time.After(20 * time.Millisecond):
		}
	}
	if elapsed := time.Since(started); elapsed < 900*time.Millisecond {
		t.Fatalf("orphan ended before its read timeout: %v", elapsed)
	}
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 0 }, 42)
}

func TestAgentPoolOrphanLimitsCloseAtOnce(t *testing.T) {
	for _, tc := range []struct {
		name   string
		active int
		bytes  int64
		cmd    string
	}{
		{"count", agentOrphanMax, 0, "HOST_TOP"},
		{"bytes", 1, agentOrphanMaxBytes - agentLargePackBytes + 1, "OBJECT_THREAD_DUMP"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newOrphanHarness(t, 0)
			peer := h.dial(t, 42)
			h.pool.orphans.mu.Lock()
			h.pool.orphans.active, h.pool.orphans.bytes = tc.active, tc.bytes
			h.pool.orphans.mu.Unlock()
			h.orphan(t, peer, 42, tc.cmd)
			// Rejected: the session is closed before the caller returns.
			h.wait(t, func(list []*agentSlot) bool { return len(list) == 0 }, 42)
			s := h.pool.orphans.snapshot()
			if s.Rejected != 1 || s.Orphaned != 0 || s.Active != tc.active || s.Bytes != tc.bytes {
				t.Fatalf("orphan stats %+v", s)
			}
			if lines := h.logs.wait(t, 1); !strings.Contains(lines[0], "agent relay "+tc.cmd+" objHash=42 closed: its caller left and the orphan relay limits (32 relays, 512MiB) are full") {
				t.Fatal("rejection not logged", lines)
			}
		})
	}
	// Two large orphans fit the byte budget, a third does not.
	var o agentOrphans
	for i := 0; i < 2; i++ {
		if !o.reserve(agentLargePackBytes) {
			t.Fatal("large orphan", i)
		}
	}
	if o.reserve(agentLargePackBytes) || o.reserve(agentPackItemBytes) {
		t.Fatal("byte budget exceeded")
	}
	o.unreserve(agentLargePackBytes)
	if !o.reserve(agentPackItemBytes) {
		t.Fatal("released budget not reusable")
	}
	if s := o.snapshot(); s.Active != 2 || s.Bytes != agentLargePackBytes+agentPackItemBytes {
		t.Fatalf("budget %+v", s)
	}
}

// A caller deadline on requestDiscard (text cache reset's 5s) leaves the
// session open too.
func TestAgentPoolDiscardOrphanKeepsSession(t *testing.T) {
	h := newOrphanHarness(t, 0)
	peer := h.dial(t, 42)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	type result struct {
		reached bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		reached, err := h.pool.requestDiscard(ctx, 42, "OBJECT_RESET_CACHE", &protocol.MapPack{})
		done <- result{reached, err}
	}()
	if cmd, _ := peer.readRequest(t); cmd != "OBJECT_RESET_CACHE" {
		t.Fatal(cmd)
	}
	select {
	case r := <-done:
		if !r.reached || !errors.Is(r.err, context.DeadlineExceeded) {
			t.Fatal(r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("caller deadline ignored")
	}
	slot := h.slot(t, 42)
	peer.packResponse(t, 1)
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 1 && list[0] == slot && !list[0].busy }, 42)
	if slot.conn.Closed() || h.pool.orphans.snapshot().Completed != 1 {
		t.Fatal("discard orphan", slot.conn.Closed(), h.pool.orphans.snapshot())
	}
}

// Server shutdown ends an orphan at once: the connection handler closes the
// busy session and waits for its release, so no relay goroutine outlives it.
func TestAgentPoolOrphanEndsOnShutdown(t *testing.T) {
	h := newOrphanHarness(t, 0)
	peer := h.dial(t, 42)
	h.orphan(t, peer, 42, "OBJECT_HEAPHISTO")
	started := time.Now()
	if err := h.stop(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("shutdown waited %v for the orphan", elapsed)
	}
	if s := h.pool.orphans.snapshot(); s.Active != 0 || s.Failed != 1 {
		t.Fatalf("orphan stats %+v", s)
	}
}

// C4 with the production defaults: the Agent answers a heap histogram
// after 40s, past the Client progress limit (RequestTimeout 30s) and inside
// the Agent so_timeout (60s). Like Java's service thread, the command waits
// for the Agent (Stream.AwaitUpstream), so the Client gets the answer, no
// relay is orphaned, and both the Client connection and the Agent session
// serve the next command.
func TestE2EAgentRelaySlowerThanClientProgressKeepsSession(t *testing.T) {
	if testing.Short() {
		t.Skip("40s production-timing reproduction")
	}
	t.Parallel()
	const hash = 4250
	r, addr := e2eRuntime(t)
	agent := e2eAgent(t, r, addr, hash)
	client := moLogin(t, addr)
	moSend(t, client.conn, "OBJECT_HEAPHISTO", client.session, e2eObjHashParam(t, hash))
	client.conn.SetDeadline(time.Now().Add(2 * time.Minute))
	started := time.Now()
	if cmd, _ := agent.readRequest(t); cmd != "OBJECT_HEAPHISTO" {
		t.Fatal(cmd)
	}
	type clientEnd struct {
		after time.Duration
		err   error
	}
	// The first response byte (or the connection end) is awaited here; the
	// response is read on the test goroutine.
	ended := make(chan clientEnd, 1)
	go func() {
		_, err := client.r.Peek(1)
		ended <- clientEnd{time.Since(started), err}
	}()
	time.Sleep(40*time.Second - time.Since(started))
	select {
	case end := <-ended:
		t.Fatalf("Client command ended before the Agent answered: %+v", end)
	default:
	}
	agent.packResponse(t, 40)
	select {
	case end := <-ended:
		if end.err != nil || end.after < 40*time.Second {
			t.Fatalf("Client command end %+v", end)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Client did not get the Agent answer")
	}
	if resp := moRead(t, client.r, "pack"); !resp.complete || len(resp.items) != 1 || moMapGet(resp.items[0].pack.(*protocol.MapPack), "peer").Int != 40 {
		t.Fatalf("Client answer %+v", resp)
	}
	h := &poolTCPHarness{pool: &r.pool}
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 1 && !list[0].busy && !list[0].conn.Closed() }, hash)
	if s := r.pool.orphans.snapshot(); s.Orphaned != 0 || s.Completed != 0 || s.Failed != 0 {
		t.Fatalf("orphan stats %+v", s)
	}
	client.conn.SetDeadline(time.Now().Add(10 * time.Second))
	moSend(t, client.conn, "HOST_TOP", client.session, e2eObjHashParam(t, hash))
	// The pool's idle KEEP_ALIVE may reach the session first; answer it like
	// the Java Agent and wait for the Client's command.
	cmd, _ := agent.readRequest(t)
	for ; cmd == "KEEP_ALIVE"; cmd, _ = agent.readRequest(t) {
		agent.packResponse(t, 0)
	}
	if cmd != "HOST_TOP" {
		t.Fatal("the same Agent socket did not take the next command:", cmd)
	}
	agent.packResponse(t, 41)
	resp := moRead(t, client.r, "pack")
	if !resp.complete || len(resp.items) != 1 || moMapGet(resp.items[0].pack.(*protocol.MapPack), "peer").Int != 41 {
		t.Fatalf("next command on the same connections %+v", resp)
	}
}

// An Agent slower than its so_timeout (35s here, past the 30s Client
// progress limit) fails the relay like Java's AgentCall read timeout: the
// session is closed and the Client gets the null answer (no item, NoNEXT)
// instead of a connection closed at the progress limit.
func TestE2EAgentRelayPastAgentSoTimeoutAnswersEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("35s production-timing reproduction")
	}
	t.Parallel()
	const hash = 4251
	r, addr := e2eRuntime(t)
	if err := r.tcp.SetAgentSoTimeout(35 * time.Second); err != nil {
		t.Fatal(err)
	}
	agent := e2eAgent(t, r, addr, hash)
	client := moLogin(t, addr)
	moSend(t, client.conn, "OBJECT_HEAPHISTO", client.session, e2eObjHashParam(t, hash))
	client.conn.SetDeadline(time.Now().Add(2 * time.Minute))
	started := time.Now()
	if cmd, _ := agent.readRequest(t); cmd != "OBJECT_HEAPHISTO" {
		t.Fatal(cmd)
	}
	resp := moRead(t, client.r, "pack")
	after := time.Since(started)
	if !resp.complete || len(resp.items) != 0 || after < 34*time.Second || after > 45*time.Second {
		t.Fatalf("Client answer after %v: %+v", after, resp)
	}
	h := &poolTCPHarness{pool: &r.pool}
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 0 }, hash)
	if s := r.pool.orphans.snapshot(); s.Orphaned != 0 {
		t.Fatalf("orphan stats %+v", s)
	}
	// The Client connection stays usable.
	client.conn.SetDeadline(time.Now().Add(10 * time.Second))
	moSend(t, client.conn, "SERVER_TIME", client.session, nil)
	if resp := moRead(t, client.r, "pack"); !resp.complete || len(resp.items) != 1 {
		t.Fatalf("SERVER_TIME after the empty answer %+v", resp)
	}
}

// A Client that closes its connection during the Agent wait ends the
// command at once; the relay becomes an orphan, reads the answer and keeps
// the Agent session.
func TestE2EAgentRelayClientLeavesFirstOrphans(t *testing.T) {
	const hash = 4252
	r, addr := e2eRuntime(t)
	agent := e2eAgent(t, r, addr, hash)
	client := moLogin(t, addr)
	moSend(t, client.conn, "OBJECT_HEAPHISTO", client.session, e2eObjHashParam(t, hash))
	if cmd, _ := agent.readRequest(t); cmd != "OBJECT_HEAPHISTO" {
		t.Fatal(cmd)
	}
	left := time.Now()
	client.conn.Close()
	for r.pool.orphans.snapshot().Orphaned != 1 {
		if time.Since(left) > time.Second {
			t.Fatalf("Client exit not noticed: orphan stats %+v", r.pool.orphans.snapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}
	agent.packResponse(t, 1)
	h := &poolTCPHarness{pool: &r.pool}
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 1 && !list[0].busy && !list[0].conn.Closed() }, hash)
	if s := r.pool.orphans.snapshot(); s.Orphaned != 1 || s.Completed != 1 || s.Failed != 0 || s.Active != 0 {
		t.Fatalf("orphan stats %+v", s)
	}
}

// The orphan goroutine writes its outcome line before it returns the slot
// and the permit: once the session is idle again no relay goroutine is
// left (the Agent handler waits only for the slot, runtime.go handle).
func TestAgentPoolOrphanLogsBeforeRelease(t *testing.T) {
	h := newOrphanHarness(t, 0)
	busyAtLog := make(chan bool, 1)
	peer := h.dial(t, 42)
	h.orphan(t, peer, 42, "OBJECT_HEAPHISTO")
	slot := h.slot(t, 42)
	h.pool.orphans.mu.Lock()
	h.pool.orphans.logf = func(string, ...any) {
		h.pool.mu.Lock()
		busy := slot.busy
		h.pool.mu.Unlock()
		busyAtLog <- busy && h.pool.budget.snapshot().Bytes > 0
	}
	h.pool.orphans.mu.Unlock()
	peer.packResponse(t, 1)
	select {
	case held := <-busyAtLog:
		if !held {
			t.Fatal("orphan outcome logged after the slot and permit were released")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no orphan outcome line")
	}
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 1 && !list[0].busy }, 42)
}

// ShutdownReport (main's exit 3 line) names the Client commands in their
// handler, the orphan relays and the relay budget bytes: a relay waiting
// for its Agent, then the same relay as an orphan after its Client left.
func TestShutdownReportRelayState(t *testing.T) {
	const hash = 4253
	r, addr := e2eRuntime(t)
	agent := e2eAgent(t, r, addr, hash)
	client := moLogin(t, addr)
	moSend(t, client.conn, "OBJECT_HEAPHISTO", client.session, e2eObjHashParam(t, hash))
	if cmd, _ := agent.readRequest(t); cmd != "OBJECT_HEAPHISTO" {
		t.Fatal(cmd)
	}
	waitReport := func(want string) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			report := r.ShutdownReport()
			if strings.Contains(report, want) {
				if !strings.HasPrefix(report, `stage="running"`) {
					t.Fatal(report)
				}
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("report %q, want %q", report, want)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitReport(fmt.Sprintf(" clientCommands=1 agentOrphans=0 agentRelayBytes=%d ", agentLargePackBytes))
	client.conn.Close()
	waitReport(fmt.Sprintf(" clientCommands=0 agentOrphans=1 agentRelayBytes=%d ", agentLargePackBytes))
	agent.packResponse(t, 1)
	waitReport(" clientCommands=0 agentOrphans=0 agentRelayBytes=0 ")
}
