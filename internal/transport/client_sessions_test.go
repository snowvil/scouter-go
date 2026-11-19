package transport

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

type sessionEvent struct {
	bound   bool
	conn    uint64
	session int64
}

type recordingObserver struct {
	mu     sync.Mutex
	events []sessionEvent
	held   map[uint64]map[int64]bool
}

func (o *recordingObserver) ClientSessionBound(conn uint64, session int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.held == nil {
		o.held = map[uint64]map[int64]bool{}
	}
	if o.held[conn] == nil {
		o.held[conn] = map[int64]bool{}
	}
	if o.held[conn][session] {
		panic(fmt.Sprintf("double bind %d/%d", conn, session))
	}
	o.held[conn][session] = true
	o.events = append(o.events, sessionEvent{true, conn, session})
}
func (o *recordingObserver) ClientSessionReleased(conn uint64, session int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.held[conn][session] {
		panic(fmt.Sprintf("release without bind %d/%d", conn, session))
	}
	delete(o.held[conn], session)
	o.events = append(o.events, sessionEvent{false, conn, session})
}
func (o *recordingObserver) snapshot() []sessionEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]sessionEvent(nil), o.events...)
}
func (o *recordingObserver) heldCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, m := range o.held {
		n += len(m)
	}
	return n
}

func TestClientSessionsHoldOnlyTheLastSession(t *testing.T) {
	o := &recordingObserver{}
	c := &clientSessions{id: 9, observer: o}
	c.use(0) // free commands carry 0: never tracked
	c.use(1)
	c.use(1) // same id again: no event
	c.use(2) // re-login on the same connection: 1 is no longer held
	ClientConn{ID: 9, tracker: c}.BindSession(77)
	if o.heldCount() != 1 {
		t.Fatalf("held %d ids, want only the last one", o.heldCount())
	}
	c.release()
	c.release() // idempotent
	c.use(78)   // after release: ignored
	ClientConn{}.BindSession(5)
	if o.heldCount() != 0 {
		t.Fatal("ids left held after release")
	}
	want := []sessionEvent{{true, 9, 1}, {false, 9, 1}, {true, 9, 2}, {false, 9, 2}, {true, 9, 77}, {false, 9, 77}}
	if got := o.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("events %v, want %v", got, want)
	}
	(&clientSessions{}).use(1) // no observer: no-op
	(&clientSessions{}).release()
}

func clientCommandBytes(t *testing.T, command string, session int64) []byte {
	t.Helper()
	e := protocol.NewEncoder(protocol.Limits{})
	e.Text(command)
	e.Int64(session)
	raw, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func openClient(t *testing.T, listener net.Listener) net.Conn {
	t.Helper()
	conn := dialTCP(t, listener)
	var magic [4]byte
	binary.BigEndian.PutUint32(magic[:], TCPClient)
	if _, err := conn.Write(magic[:]); err != nil {
		t.Fatal(err)
	}
	return conn
}

func expectFlag(t *testing.T, conn net.Conn, want byte) {
	t.Helper()
	var b [1]byte
	if _, err := io.ReadFull(conn, b[:]); err != nil || b[0] != want {
		t.Fatalf("flag %x %v, want %x", b[0], err, want)
	}
}

func waitEvents(t *testing.T, o *recordingObserver, n int) []sessionEvent {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if events := o.snapshot(); len(events) >= n {
			return events
		}
		if time.Now().After(deadline) {
			t.Fatalf("events %v, want %d", o.snapshot(), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Every admitted Client connection reports the sessions its commands carried
// and the LOGIN binding, and releases them when it ends by EOF, CLOSE or an
// invalid session. Agent connections report nothing.
func TestClientConnectionSessionTracking(t *testing.T) {
	o := &recordingObserver{}
	var idsMu sync.Mutex
	ids := map[uint64]bool{}
	handlers := TCPHandlers{
		Sessions:  o,
		Authorize: func(_ context.Context, r ClientRequest) (bool, error) { return r.Session != 666, nil },
		Client: func(_ context.Context, r ClientRequest, _ *Stream) error {
			idsMu.Lock()
			ids[r.Conn.ID] = true
			idsMu.Unlock()
			if r.Command == "LOGIN" {
				r.Conn.BindSession(77)
			}
			return nil
		},
		Agent: func(context.Context, *AgentConn) error { return nil },
	}
	_, listener, cancel, done := startTCP(t, TCPConfig{}, handlers)

	first := openClient(t, listener)
	writeSlow(t, first, clientCommandBytes(t, "LOGIN", 0))
	expectFlag(t, first, NoNext)
	writeSlow(t, first, clientCommandBytes(t, "GET_TEXT", 77))
	expectFlag(t, first, NoNext)
	writeSlow(t, first, clientCommandBytes(t, "GET_TEXT", 5)) // latched connection: the new id replaces 77
	expectFlag(t, first, NoNext)
	second := openClient(t, listener)
	writeSlow(t, second, clientCommandBytes(t, "GET_TEXT", 5))
	expectFlag(t, second, NoNext)
	events := waitEvents(t, o, 4)
	conn1, conn2 := events[0].conn, events[3].conn
	if conn1 == 0 || conn2 == 0 || conn1 == conn2 {
		t.Fatalf("connection ids %d %d", conn1, conn2)
	}
	if want := []sessionEvent{{true, conn1, 77}, {false, conn1, 77}, {true, conn1, 5}, {true, conn2, 5}}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events %v, want %v", events, want)
	}
	idsMu.Lock()
	if !ids[conn1] || !ids[conn2] || len(ids) != 2 {
		t.Fatalf("ClientRequest.Conn.ID %v", ids)
	}
	idsMu.Unlock()

	first.Close() // EOF
	events = waitEvents(t, o, 5)
	if want := []sessionEvent{{false, conn1, 5}}; !reflect.DeepEqual(events[4:], want) {
		t.Fatalf("EOF release %v", events[4:])
	}
	closeCmd := protocol.NewEncoder(protocol.Limits{})
	closeCmd.Text("CLOSE")
	raw, err := closeCmd.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	writeSlow(t, second, raw)
	events = waitEvents(t, o, 6)
	if events[5] != (sessionEvent{false, conn2, 5}) {
		t.Fatalf("CLOSE release %v", events[5])
	}
	invalid := openClient(t, listener)
	writeSlow(t, invalid, clientCommandBytes(t, "GET_TEXT", 666))
	expectFlag(t, invalid, InvalidSession)
	events = waitEvents(t, o, 8)
	if events[6].session != 666 || !events[6].bound || events[7].session != 666 || events[7].bound {
		t.Fatalf("invalid session %v", events[6:])
	}
	agent := dialTCP(t, listener)
	var hello [8]byte
	binary.BigEndian.PutUint32(hello[:4], TCPAgentV2)
	binary.BigEndian.PutUint32(hello[4:], 42)
	if _, err := agent.Write(hello[:]); err != nil {
		t.Fatal(err)
	}
	io.ReadAll(agent) // the Agent handler returned and the server closed it
	agent.Close()
	stopTCP(t, cancel, done)
	if events := o.snapshot(); len(events) != 8 || o.heldCount() != 0 {
		t.Fatalf("after shutdown %v held=%d", events, o.heldCount())
	}
}

// Concurrent Client connections, closed by the peer or by server shutdown,
// release exactly what they bound.
func TestClientSessionTrackingConcurrentShutdown(t *testing.T) {
	o := &recordingObserver{}
	handlers := TCPHandlers{
		Sessions:  o,
		Authorize: func(context.Context, ClientRequest) (bool, error) { return true, nil },
		Client: func(_ context.Context, r ClientRequest, _ *Stream) error {
			if r.Command == "LOGIN" {
				r.Conn.BindSession(r.Session + 1000)
			}
			return nil
		},
	}
	_, listener, cancel, done := startTCP(t, TCPConfig{}, handlers)
	var wg sync.WaitGroup
	var open []net.Conn
	var openMu sync.Mutex
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", listener.Addr().String())
			if err != nil {
				t.Error(err)
				return
			}
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			var raw []byte
			raw = binary.BigEndian.AppendUint32(raw, TCPClient)
			for j := range 20 {
				raw = append(raw, clientCommandBytes(t, "LOGIN", int64(i*100+j))...)
				raw = append(raw, clientCommandBytes(t, "GET_TEXT", int64(i*100+j%3+1))...)
			}
			conn.Write(raw)
			io.ReadFull(conn, make([]byte, 40))
			if i%2 == 0 {
				conn.Close()
				return
			}
			openMu.Lock()
			open = append(open, conn)
			openMu.Unlock()
		}()
	}
	wg.Wait()
	stopTCP(t, cancel, done)
	for _, conn := range open {
		conn.Close()
	}
	if o.heldCount() != 0 {
		t.Fatalf("held after shutdown: %d", o.heldCount())
	}
}
