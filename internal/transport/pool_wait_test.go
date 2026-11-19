package transport

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingHandlers is serverTimeHandlers plus a call counter.
func countingHandlers(calls *atomic.Int64) TCPHandlers {
	inner := serverTimeHandlers().Client
	return TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		calls.Add(1)
		return inner(ctx, r, s)
	}}
}

// occupy opens a Client session that holds the only pool permit.
func occupy(t *testing.T, listener net.Listener) net.Conn {
	t.Helper()
	conn := dialRaw(t, listener)
	conn.Write(clientBytes(t, true, "SERVER_TIME"))
	expectBytes(t, conn, serverTimeResponse, time.Second)
	return conn
}

func TestServicePoolWaitLimitFollowsClientSoTimeout(t *testing.T) {
	var calls atomic.Int64
	server, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: 300 * time.Millisecond}, countingHandlers(&calls))
	logs := &logCollector{}
	server.SetLogFunc(logs.add)
	server.SetServicePoolSize(1)
	a := occupy(t, listener)

	server.SetClientSoTimeout(100 * time.Millisecond)
	start := time.Now()
	waiter := dialRaw(t, listener)
	waiter.Write(clientBytes(t, true, "SERVER_TIME"))
	waitFor(t, "waiter", func() bool { return server.Stats().ClientWaiting == 1 })
	expectClosed(t, waiter, 2*time.Second)
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("waiter closed after %v, before the 100ms limit", elapsed)
	}
	// The peer sees EOF before the server goroutine finishes its cleanup.
	waitFor(t, "waiter released", func() bool { return server.Stats().Active == 1 })
	if st := server.Stats(); st.ClientWaitTimeouts != 1 || st.ClientWaiting != 0 || st.ClientAbandoned != 0 || st.Active != 1 || st.ClientConnections != 1 || st.Errors != 0 {
		t.Fatalf("%+v", st)
	}
	lines := logs.snapshot()
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "Client : /127.0.0.1:") ||
		!strings.HasSuffix(lines[0], " closed after waiting 100ms for a service pool slot (Go service pool wait limit)") {
		t.Fatalf("wait timeout line %q", lines)
	}

	// so-timeout 0 (no per-read limit) falls back to RequestTimeout; the
	// second line falls inside the 10s rate limit.
	server.SetClientSoTimeout(0)
	start = time.Now()
	unlimited := dialRaw(t, listener)
	unlimited.Write(clientBytes(t, true, "SERVER_TIME"))
	waitFor(t, "unlimited waiter", func() bool { return server.Stats().ClientWaiting == 1 })
	expectSilence(t, unlimited, 150*time.Millisecond)
	expectClosed(t, unlimited, 2*time.Second)
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Fatalf("RequestTimeout wait ended after %v", elapsed)
	}
	if st := server.Stats(); st.ClientWaitTimeouts != 2 {
		t.Fatalf("%+v", st)
	}
	if got := logs.snapshot(); len(got) != 1 {
		t.Fatalf("rate limit let through %q", got)
	}
	// The timed-out commands never ran, also after the permit frees.
	a.Write(clientBytes(t, false, "CLOSE"))
	expectClosed(t, a, time.Second)
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("handler calls %d", calls.Load())
	}
	stopTCP(t, cancel, done)
}

func TestServicePoolStaleCommandsNeverRun(t *testing.T) {
	var calls atomic.Int64
	server, listener, cancel, done := startTCP(t, TCPConfig{}, countingHandlers(&calls))
	server.SetServicePoolSize(1)
	a := occupy(t, listener)

	gone := dialRaw(t, listener)
	gone.Write(clientBytes(t, true, "SERVER_TIME", "SERVER_TIME"))
	waitFor(t, "waiter", func() bool { return server.Stats().ClientWaiting == 1 })
	gone.Close() // a Client that timed out sends CLOSE-less FIN after its commands
	half := dialRaw(t, listener)
	half.Write(clientBytes(t, true, "SERVER_TIME"))
	half.(*net.TCPConn).CloseWrite()
	waitFor(t, "both abandoned", func() bool {
		st := server.Stats()
		return st.ClientAbandoned == 2 && st.ClientWaiting == 0 && st.Active == 1
	})

	a.Write(clientBytes(t, false, "CLOSE"))
	expectClosed(t, a, time.Second)
	waitFor(t, "permit free", func() bool { return server.Stats().ClientSessions == 0 })
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("stale commands ran: handler calls %d", calls.Load())
	}
	fresh := dialRaw(t, listener)
	fresh.Write(clientBytes(t, true, "SERVER_TIME"))
	expectBytes(t, fresh, serverTimeResponse, time.Second)
	if st := server.Stats(); st.Errors != 0 || st.ClientWaitTimeouts != 0 || st.ClientWaitRejected != 0 {
		t.Fatalf("%+v", st)
	}
	stopTCP(t, cancel, done)
}

func TestServicePoolWaiterCapKeepsClientSlots(t *testing.T) {
	if MaxClientWaiters(0) != 1 || MaxClientWaiters(1) != 1 || MaxClientWaiters(5) != 2 || MaxClientWaiters(1024) != 512 {
		t.Fatal("MaxClientWaiters")
	}
	agents := make(chan *AgentConn, 1)
	handlers := serverTimeHandlers()
	handlers.Agent = func(ctx context.Context, a *AgentConn) error { agents <- a; <-ctx.Done(); return ctx.Err() }
	server, listener, cancel, done := startTCP(t, TCPConfig{MaxConnections: 4}, handlers)
	logs := &logCollector{}
	server.SetLogFunc(logs.add)
	server.SetServicePoolSize(1)
	occupy(t, listener)
	for i := 0; i < 2; i++ { // MaxClientWaiters(4) == 2
		w := dialRaw(t, listener)
		w.Write(clientBytes(t, true, "SERVER_TIME"))
		waitFor(t, "waiter", func() bool { return server.Stats().ClientWaiting == i+1 })
	}
	over := dialRaw(t, listener)
	over.Write(clientBytes(t, true, "SERVER_TIME"))
	if elapsed := expectClosed(t, over, time.Second); elapsed > 500*time.Millisecond {
		t.Fatalf("waiter over the cap closed after %v", elapsed)
	}
	waitFor(t, "rejected waiter released", func() bool {
		st := server.Stats()
		return st.ClientWaitRejected == 1 && st.ClientWaiting == 2 && st.Active == 3
	})
	lines := logs.snapshot()
	if len(lines) != 1 || !strings.HasSuffix(lines[0], " rejected, waiting clients=2 (Go service pool wait limit)") {
		t.Fatalf("rejected line %q", lines)
	}
	// The fourth Client slot is still free, and an Agent uses its own budget.
	registerAgent(t, listener, agents)
	if st := server.Stats(); st.Active != 4 || st.Rejected != 0 || st.ClientConnections != 3 || st.AgentConnections != 1 {
		t.Fatalf("%+v", st)
	}
	stopTCP(t, cancel, done)
}

// Every Client ends exactly once as served, wait-timed-out, wait-rejected or
// accept-rejected while the pool size changes under it.
func TestServicePoolResizeRaceAccounting(t *testing.T) {
	server, listener, cancel, done := startTCP(t, TCPConfig{MaxConnections: 16}, serverTimeHandlers())
	server.SetClientSoTimeout(150 * time.Millisecond)
	server.SetServicePoolSize(1)
	stop := make(chan struct{})
	var resizer sync.WaitGroup
	resizer.Add(1)
	go func() {
		defer resizer.Done()
		r := rand.New(rand.NewSource(7))
		for {
			select {
			case <-stop:
				return
			default:
				server.SetServicePoolSize(1 + r.Intn(3))
				if st := server.Stats(); st.ClientWaiting > MaxClientWaiters(16) {
					t.Errorf("waiting %d above cap", st.ClientWaiting)
				}
				time.Sleep(time.Duration(r.Intn(3)) * time.Millisecond)
			}
		}
	}()
	const clients = 40
	var served atomic.Int64
	var clientsDone sync.WaitGroup
	for i := 0; i < clients; i++ {
		clientsDone.Add(1)
		go func(seed int64) {
			defer clientsDone.Done()
			time.Sleep(time.Duration(seed) * 4 * time.Millisecond) // stagger arrivals
			conn, err := net.Dial("tcp", listener.Addr().String())
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			conn.Write(clientBytes(t, true, "SERVER_TIME"))
			got := make([]byte, len(serverTimeResponse))
			if _, err := io.ReadFull(conn, got); err == nil {
				served.Add(1)
				time.Sleep(time.Duration(seed%5) * 20 * time.Millisecond) // hold the session
				conn.Write(clientBytes(t, false, "CLOSE"))
			}
			io.ReadAll(conn)
		}(int64(i))
	}
	clientsDone.Wait()
	close(stop)
	resizer.Wait()
	waitFor(t, "all connections released", func() bool {
		st := server.Stats()
		return st.Active == 0 && st.ClientSessions == 0 && st.ClientWaiting == 0
	})
	st := server.Stats()
	ended := st.ClientRequests + st.ClientWaitTimeouts + st.ClientWaitRejected + st.Rejected
	if int64(st.ClientRequests) != served.Load() || ended != clients || st.ClientAbandoned != 0 || st.Errors != 0 {
		t.Fatalf("served=%d %+v", served.Load(), st)
	}
	t.Logf("served=%d waitTimeouts=%d waitRejected=%d acceptRejected=%d", st.ClientRequests, st.ClientWaitTimeouts, st.ClientWaitRejected, st.Rejected)
	stopTCP(t, cancel, done)
}

func TestSessionGateWaiterCapUnderResize(t *testing.T) {
	const maxWaiters = 3
	gate := newSessionGate(1, maxWaiters)
	stop := make(chan struct{})
	var resizer sync.WaitGroup
	resizer.Add(1)
	go func() {
		defer resizer.Done()
		r := rand.New(rand.NewSource(3))
		for {
			select {
			case <-stop:
				return
			default:
				gate.setLimit(1 + r.Intn(2))
				if _, _, waiting := gate.snapshot(); waiting > maxWaiters {
					t.Errorf("waiting %d above cap", waiting)
				}
			}
		}
	}()
	var full atomic.Int64
	var workers sync.WaitGroup
	for i := 0; i < 12; i++ {
		workers.Add(1)
		go func(seed int64) {
			defer workers.Done()
			r := rand.New(rand.NewSource(seed))
			for j := 0; j < 200; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.Intn(200))*time.Microsecond)
				err := gate.acquire(ctx, make(chan error))
				cancel()
				switch {
				case err == nil:
					gate.release()
				case errors.Is(err, errWaitFull):
					full.Add(1)
				}
			}
		}(int64(i))
	}
	workers.Wait()
	close(stop)
	resizer.Wait()
	if _, active, waiting := gate.snapshot(); active != 0 || waiting != 0 {
		t.Fatalf("leaked permits active=%d waiting=%d", active, waiting)
	}
	if full.Load() == 0 {
		t.Log("waiter cap never reached in this run")
	}
}
