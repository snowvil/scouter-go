package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

type poolTCPHarness struct {
	pool    *agentPool
	address string
}
type poolFakeAgent struct {
	conn   net.Conn
	reader *bufio.Reader
}
type poolRequestResult struct {
	packs []protocol.Pack
	err   error
}

func newPoolTCPHarness(t *testing.T) *poolTCPHarness {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &agentPool{agents: make(map[int32][]*agentSlot)}
	server, err := transport.NewTCPServer(listener, transport.TCPConfig{RequestTimeout: 3 * time.Second}, transport.TCPHandlers{Agent: p.handle}, nil)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("pool TCP shutdown %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("pool handler leaked during shutdown")
		}
	})
	return &poolTCPHarness{pool: p, address: listener.Addr().String()}
}
func (h *poolTCPHarness) dial(t *testing.T, hash int32) *poolFakeAgent {
	t.Helper()
	conn, err := net.DialTimeout("tcp", h.address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], transport.TCPAgentV1)
	binary.BigEndian.PutUint32(header[4:], uint32(hash))
	if _, err = conn.Write(header[:]); err != nil {
		t.Fatal(err)
	}
	peer := &poolFakeAgent{conn: conn, reader: bufio.NewReader(conn)}
	h.wait(t, func(list []*agentSlot) bool {
		for _, s := range list {
			if s.conn.Peer.String() == conn.LocalAddr().String() {
				return true
			}
		}
		return false
	}, hash)
	return peer
}
func (h *poolTCPHarness) wait(t *testing.T, predicate func([]*agentSlot) bool, hash int32) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		h.pool.mu.Lock()
		ok := predicate(h.pool.agents[hash])
		if h.pool.changed == nil {
			h.pool.changed = make(chan struct{})
		}
		changed := h.pool.changed
		h.pool.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-changed:
		case <-timer.C:
			t.Fatal("pool state notification timed out")
		}
	}
}
func (a *poolFakeAgent) readRequest(t *testing.T) (string, *protocol.MapPack) {
	t.Helper()
	if err := a.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	// AgentV1 Text has no Value tag. Add only that tag to use the public codec,
	// which consumes the command without reading ahead into the following Pack.
	v, err := protocol.ReadValue(io.MultiReader(bytes.NewReader([]byte{protocol.ValueText}), a.reader), protocol.Limits{})
	if err != nil {
		t.Fatal("fake Agent command", err)
	}
	p, err := protocol.ReadPack(a.reader, protocol.Limits{})
	if err != nil {
		t.Fatal("fake Agent Pack", err)
	}
	m, ok := p.(*protocol.MapPack)
	if !ok {
		t.Fatalf("fake Agent parameter type %T", p)
	}
	return v.Text, m
}
func (a *poolFakeAgent) packResponse(t *testing.T, id int64) {
	t.Helper()
	e := protocol.NewEncoder(protocol.Limits{})
	e.Byte(transport.HasNext)
	e.Pack(&protocol.MapPack{Entries: []protocol.Entry{{Key: "peer", Value: protocol.Value{Type: protocol.ValueDecimal, Int: id}}}})
	e.Byte(transport.NoNext)
	b, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.conn.Write(b); err != nil {
		t.Fatal(err)
	}
}
func startPoolRequest(p *agentPool, ctx context.Context, hash int32, cmd string) <-chan poolRequestResult {
	done := make(chan poolRequestResult, 1)
	go func() {
		packs, err := p.request(ctx, hash, cmd, &protocol.MapPack{})
		done <- poolRequestResult{packs, err}
	}()
	return done
}
func awaitPoolRequest(t *testing.T, done <-chan poolRequestResult, id int64) {
	t.Helper()
	select {
	case r := <-done:
		if r.err != nil || len(r.packs) != 1 {
			t.Fatalf("pool response %+v %v", r.packs, r.err)
		}
		m, ok := r.packs[0].(*protocol.MapPack)
		if !ok || len(m.Entries) != 1 || m.Entries[0].Value.Int != id {
			t.Fatalf("wrong Agent response %+v", r.packs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pool request did not complete")
	}
}

func TestAgentPoolBlockedOwnerUsesOtherIdleConnection(t *testing.T) {
	h := newPoolTCPHarness(t)
	first := h.dial(t, 42)
	second := h.dial(t, 42)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	one := startPoolRequest(h.pool, ctx, 42, "FIRST")
	cmd, _ := first.readRequest(t)
	if cmd != "FIRST" {
		t.Fatal(cmd)
	}
	two := startPoolRequest(h.pool, ctx, 42, "SECOND")
	cmd, _ = second.readRequest(t)
	if cmd != "SECOND" {
		t.Fatal(cmd)
	}
	second.packResponse(t, 2)
	awaitPoolRequest(t, two, 2)
	select {
	case r := <-one:
		t.Fatalf("blocked owner was disturbed: %+v", r)
	default:
	}
	first.packResponse(t, 1)
	awaitPoolRequest(t, one, 1)
}

func TestAgentPoolCancelledWaiterDoesNotCloseOwner(t *testing.T) {
	h := newPoolTCPHarness(t)
	peer := h.dial(t, 42)
	ownerCtx, ownerCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer ownerCancel()
	owner := startPoolRequest(h.pool, ownerCtx, 42, "OWNER")
	cmd, _ := peer.readRequest(t)
	if cmd != "OWNER" {
		t.Fatal(cmd)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := h.pool.request(waitCtx, 42, "WAITER", &protocol.MapPack{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter cancellation %v", err)
	}
	h.pool.mu.Lock()
	slot := h.pool.agents[42][0]
	closed := slot.conn.Closed()
	busy := slot.busy
	h.pool.mu.Unlock()
	if closed || !busy {
		t.Fatalf("waiter changed owner closed=%v busy=%v", closed, busy)
	}
	peer.packResponse(t, 7)
	awaitPoolRequest(t, owner, 7)
	// The same live peer remains usable after cancellation and owner release.
	next := startPoolRequest(h.pool, ownerCtx, 42, "NEXT")
	cmd, _ = peer.readRequest(t)
	if cmd != "NEXT" {
		t.Fatal(cmd)
	}
	peer.packResponse(t, 8)
	awaitPoolRequest(t, next, 8)
}

func TestAgentPoolFIFOReleaseRecheckoutAndOneSecondWait(t *testing.T) {
	h := newPoolTCPHarness(t)
	first := h.dial(t, 42)
	second := h.dial(t, 42)
	ctx := context.Background()
	a, err := h.pool.checkout(ctx, 42)
	if err != nil || a == nil || a.conn.Peer.String() != first.conn.LocalAddr().String() {
		t.Fatal("first FIFO", err)
	}
	b, err := h.pool.checkout(ctx, 42)
	if err != nil || b == nil || b.conn.Peer.String() != second.conn.LocalAddr().String() {
		t.Fatal("second FIFO", err)
	}
	started := time.Now()
	none, err := h.pool.checkout(ctx, 42)
	elapsed := time.Since(started)
	if err != nil || none != nil || elapsed < 900*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("occupied pool wait slot=%v err=%v duration=%v", none, err, elapsed)
	}
	if a.conn.Closed() || b.conn.Closed() {
		t.Fatal("checkout timeout closed an owner")
	}
	h.pool.release(a)
	again, err := h.pool.checkout(ctx, 42)
	if err != nil || again != a {
		t.Fatal("released connection not available", err)
	}
	h.pool.release(b)
	h.pool.release(again)
	again, err = h.pool.checkout(ctx, 42)
	if err != nil || again != b {
		t.Fatal("released FIFO tail order", err)
	}
	h.pool.release(again)
	started = time.Now()
	none, err = h.pool.checkout(ctx, 999)
	if err != nil || none != nil || time.Since(started) > 200*time.Millisecond {
		t.Fatal("unregistered object should return offline immediately", err)
	}
}

func TestAgentPoolClosedSlotRetiresAndReconnects(t *testing.T) {
	h := newPoolTCPHarness(t)
	h.dial(t, 42)
	slot, err := h.pool.checkout(context.Background(), 42)
	if err != nil || slot == nil {
		t.Fatal(err)
	}
	if err = slot.conn.Close(); err != nil {
		t.Fatal(err)
	}
	h.pool.release(slot)
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 0 }, 42)
	peer := h.dial(t, 42)
	replacement, err := h.pool.checkout(context.Background(), 42)
	if err != nil || replacement == nil || replacement == slot || replacement.conn.Peer.String() != peer.conn.LocalAddr().String() {
		t.Fatal("reconnect selected stale slot", err)
	}
	h.pool.release(replacement)
}

func TestAgentPoolIdleKeepAliveClaimAndRemoteClose(t *testing.T) {
	h := newPoolTCPHarness(t)
	peer := h.dial(t, 42)
	h.pool.mu.Lock()
	slot := h.pool.agents[42][0]
	slot.lastWrite = time.Now().Add(-4 * time.Second)
	h.pool.mu.Unlock()
	if h.pool.claimIdle(slot, time.Now()) {
		t.Fatal("keepalive ran before the original five-second idle threshold")
	}
	h.pool.mu.Lock()
	slot.lastWrite = time.Now().Add(-6 * time.Second)
	slot.busy = true
	h.pool.mu.Unlock()
	if h.pool.claimIdle(slot, time.Now()) {
		t.Fatal("keepalive claimed a request owner")
	}
	h.pool.mu.Lock()
	slot.busy = false
	h.pool.mu.Unlock()
	if !h.pool.claimIdle(slot, time.Now()) {
		t.Fatal("idle keepalive not claimed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- slot.conn.RequestPack(ctx, "KEEP_ALIVE", &protocol.MapPack{}, func(c context.Context, _ protocol.Pack) error { return c.Err() })
	}()
	cmd, _ := peer.readRequest(t)
	if cmd != "KEEP_ALIVE" {
		t.Fatal(cmd)
	}
	peer.packResponse(t, 1)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("live keepalive stuck")
	}
	h.pool.release(slot)
	if h.pool.claimIdle(slot, time.Now()) {
		t.Fatal("the KEEP_ALIVE claim did not refresh lastWrite")
	}
	if err := peer.conn.Close(); err != nil {
		t.Fatal(err)
	}
	if slot.conn.Closed() {
		t.Fatal("remote close was incorrectly assumed locally observed")
	}
	h.pool.mu.Lock()
	slot.lastWrite = time.Now().Add(-6 * time.Second)
	h.pool.mu.Unlock()
	// Exercise one original tick directly, without changing the production
	// five-second scan interval. It owns the idle socket until release.
	if !h.pool.claimIdle(slot, time.Now()) {
		t.Fatal("closed remote peer not eligible for keepalive probe")
	}
	err := slot.conn.RequestPack(ctx, "KEEP_ALIVE", &protocol.MapPack{}, func(c context.Context, _ protocol.Pack) error { return c.Err() })
	if err == nil || !slot.conn.Closed() {
		t.Fatalf("remote close not detected %v", err)
	}
	h.pool.release(slot)
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 0 }, 42)
}

func TestAgentPoolPartialBlobBeforeEOFIsPreserved(t *testing.T) {
	h := newPoolTCPHarness(t)
	peer := h.dial(t, 42)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type result struct {
		blobs [][]byte
		err   error
	}
	done := make(chan result, 1)
	go func() {
		var b [][]byte
		e := h.pool.requestBlob(ctx, 42, "BLOB", &protocol.MapPack{}, func(chunk []byte) error { b = append(b, chunk); return nil })
		done <- result{b, e}
	}()
	cmd, _ := peer.readRequest(t)
	if cmd != "BLOB" {
		t.Fatal(cmd)
	}
	e := protocol.NewEncoder(protocol.Limits{})
	e.Byte(transport.HasNext)
	e.Blob([]byte("synthetic partial chunk"))
	b, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = peer.conn.Write(b); err != nil {
		t.Fatal(err)
	}
	_ = peer.conn.Close()
	select {
	case r := <-done:
		if r.err == nil || !errors.Is(r.err, io.EOF) || len(r.blobs) != 1 || string(r.blobs[0]) != "synthetic partial chunk" {
			t.Fatalf("partial blobs=%q error=%v", r.blobs, r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("partial EOF request stuck")
	}
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 0 }, 42)
}

func TestAgentPoolIdleKeepAliveThreeSecondDeadline(t *testing.T) {
	h := newPoolTCPHarness(t)
	peer := h.dial(t, 42)
	h.pool.mu.Lock()
	slot := h.pool.agents[42][0]
	slot.lastWrite = time.Now().Add(-6 * time.Second)
	h.pool.mu.Unlock()
	if !h.pool.claimIdle(slot, time.Now()) {
		t.Fatal("idle probe not claimed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- slot.conn.RequestPack(ctx, "KEEP_ALIVE", &protocol.MapPack{}, func(c context.Context, _ protocol.Pack) error { return c.Err() })
	}()
	cmd, _ := peer.readRequest(t)
	if cmd != "KEEP_ALIVE" {
		t.Fatal(cmd)
	}
	// The peer remains open but deliberately sends no response. Do not shorten
	// either original keepalive timeout or the production scan configuration.
	select {
	case err := <-done:
		var networkError net.Error
		if !errors.Is(err, context.DeadlineExceeded) && (!errors.As(err, &networkError) || !networkError.Timeout()) {
			t.Fatalf("keepalive timeout %v", err)
		}
		elapsed := time.Since(started)
		if elapsed < 2800*time.Millisecond || elapsed > 4500*time.Millisecond {
			t.Fatalf("original three-second timeout duration=%v", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("keepalive timeout did not retire socket")
	}
	if !slot.conn.Closed() {
		t.Fatal("timed-out keepalive retained connection")
	}
	h.pool.release(slot)
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 0 }, 42)
}

// An answer with an unknown Pack type is an Agent failure (Java readPack
// throws, the session closes and AgentCall returns null), not ErrUnsupported,
// which the service relays read as a backend without agent calls.
func TestAgentPoolUndecodableAnswerIsAgentFailure(t *testing.T) {
	h := newPoolTCPHarness(t)
	agent := h.dial(t, 42)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := startPoolRequest(h.pool, ctx, 42, "OBJECT_THREAD_LIST")
	agent.readRequest(t)
	if _, err := agent.conn.Write([]byte{transport.HasNext, 0x7f}); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r.err == nil || errors.Is(r.err, transport.ErrUnsupported) {
			t.Fatalf("undecodable answer: %v", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request did not end")
	}
}
