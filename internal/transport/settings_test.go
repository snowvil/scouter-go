package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"math/rand"
	"net"
	"net/netip"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// serverTimeHandler answers SERVER_TIME with one Blob item: HasNext,1,1,NoNext.
var serverTimeResponse = []byte{HasNext, 1, 1, NoNext}

func serverTimeHandlers() TCPHandlers {
	return TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if r.Command != "SERVER_TIME" {
			return ErrUnsupported
		}
		return s.ResponseBlob([]byte{1})
	}}
}

func clientBytes(t *testing.T, handshake bool, commands ...string) []byte {
	t.Helper()
	e := protocol.NewEncoder(protocol.Limits{})
	if handshake {
		e.Int32(-889315327) // 0xcafe2001
	}
	for _, command := range commands {
		e.Text(command)
		if command != "CLOSE" {
			e.Int64(0)
		}
	}
	raw, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func dialRaw(t *testing.T, listener net.Listener) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func expectBytes(t *testing.T, conn net.Conn, want []byte, within time.Duration) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(within))
	got := make([]byte, len(want))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read %x: %v", want, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %x want %x", got, want)
	}
}

func expectSilence(t *testing.T, conn net.Conn, wait time.Duration) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(wait))
	one := make([]byte, 1)
	if n, err := conn.Read(one); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected no response, got %d bytes err=%v", n, err)
	}
}

// expectClosed returns how long the server took to close the connection.
func expectClosed(t *testing.T, conn net.Conn, within time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	conn.SetReadDeadline(start.Add(within))
	raw, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("connection not closed by server: %v (read %x)", err, raw)
	}
	return time.Since(start)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestClientSoTimeoutAppliesToNewConnections(t *testing.T) {
	server, listener, cancel, done := startTCP(t, TCPConfig{}, serverTimeHandlers())
	old := dialRaw(t, listener)
	old.Write(clientBytes(t, true, "SERVER_TIME"))
	expectBytes(t, old, serverTimeResponse, time.Second)

	if err := server.SetClientSoTimeout(80 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	fresh := dialRaw(t, listener)
	fresh.Write(clientBytes(t, true, "SERVER_TIME"))
	expectBytes(t, fresh, serverTimeResponse, time.Second)
	if elapsed := expectClosed(t, fresh, 2*time.Second); elapsed < 50*time.Millisecond {
		t.Fatalf("idle close after %v, before the 80ms so-timeout", elapsed)
	}

	// The connection accepted before the change keeps Java's 8s default.
	time.Sleep(200 * time.Millisecond)
	old.Write(clientBytes(t, false, "SERVER_TIME"))
	expectBytes(t, old, serverTimeResponse, time.Second)

	if err := server.SetClientSoTimeout(0); err != nil {
		t.Fatal(err)
	}
	unlimited := dialRaw(t, listener)
	unlimited.Write(clientBytes(t, true, "SERVER_TIME"))
	expectBytes(t, unlimited, serverTimeResponse, time.Second)
	time.Sleep(200 * time.Millisecond)
	unlimited.Write(clientBytes(t, false, "SERVER_TIME"))
	expectBytes(t, unlimited, serverTimeResponse, time.Second)
	if err := server.SetClientSoTimeout(-time.Millisecond); !errors.Is(err, ErrMalformed) {
		t.Fatal("negative so-timeout accepted", err)
	}
	stopTCP(t, cancel, done)
}

func TestClientSoTimeoutIsPerRead(t *testing.T) {
	server, listener, cancel, done := startTCP(t, TCPConfig{}, serverTimeHandlers())
	server.SetClientSoTimeout(150 * time.Millisecond)
	// Each gap is below the so-timeout while the whole command takes far longer.
	slow := dialRaw(t, listener)
	start := time.Now()
	for _, b := range clientBytes(t, true, "SERVER_TIME") {
		if _, err := slow.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	expectBytes(t, slow, serverTimeResponse, time.Second)
	if time.Since(start) < 300*time.Millisecond {
		t.Fatal("test did not exceed the so-timeout in total")
	}
	// A stall inside a command body closes the connection like SocketTimeoutException.
	stalled := dialRaw(t, listener)
	stalled.Write(clientBytes(t, true, "SERVER_TIME")[:7])
	if elapsed := expectClosed(t, stalled, 2*time.Second); elapsed < 100*time.Millisecond {
		t.Fatalf("stalled command closed after %v", elapsed)
	}
	stopTCP(t, cancel, done)
}

func agentServer(t *testing.T, config TCPConfig) (*TCPServer, net.Listener, context.CancelFunc, <-chan error, <-chan *AgentConn) {
	agents := make(chan *AgentConn, 4)
	server, listener, cancel, done := startTCP(t, config, TCPHandlers{Agent: func(ctx context.Context, a *AgentConn) error {
		agents <- a
		<-ctx.Done()
		return ctx.Err()
	}})
	return server, listener, cancel, done, agents
}

func registerAgent(t *testing.T, listener net.Listener, agents <-chan *AgentConn) (net.Conn, *AgentConn) {
	t.Helper()
	conn := dialRaw(t, listener)
	conn.Write(fixture(t, "tcp-agent-v1"))
	select {
	case a := <-agents:
		return conn, a
	case <-time.After(time.Second):
		t.Fatal("agent not registered")
	}
	return nil, nil
}

func keepAlive(a *AgentConn) <-chan error {
	result := make(chan error, 1)
	go func() {
		result <- a.RequestPack(context.Background(), "KEEP_ALIVE", &protocol.MapPack{}, func(context.Context, protocol.Pack) error { return nil })
	}()
	return result
}

func TestAgentSoTimeoutPerReadForNewAgents(t *testing.T) {
	server, listener, cancel, done, agents := agentServer(t, TCPConfig{})
	oldConn, oldAgent := registerAgent(t, listener, agents)
	if err := server.SetAgentSoTimeout(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	newConn, newAgent := registerAgent(t, listener, agents)
	command := make([]byte, 13) // Text(KEEP_ALIVE) + empty MapPack

	// Silent agent registered after the change: the response read times out.
	start := time.Now()
	silent := keepAlive(newAgent)
	io.ReadFull(newConn, command)
	select {
	case err := <-silent:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed < 80*time.Millisecond || elapsed > 2*time.Second {
			t.Fatalf("agent so-timeout after %v", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("agent so-timeout not applied")
	}
	if !newAgent.Closed() {
		t.Fatal("timed out agent connection kept")
	}

	// Agent registered before the change keeps the 60s default.
	slow := keepAlive(oldAgent)
	io.ReadFull(oldConn, command)
	time.Sleep(250 * time.Millisecond)
	oldConn.Write([]byte{NoNext})
	if err := <-slow; err != nil {
		t.Fatal("old agent used the new so-timeout", err)
	}

	// The limit is per read: a trickling response longer than the limit succeeds.
	trickleConn, trickleAgent := registerAgent(t, listener, agents)
	trickle := keepAlive(trickleAgent)
	io.ReadFull(trickleConn, command)
	start = time.Now()
	for _, b := range []byte{HasNext, protocol.PackMap, 0, NoNext} {
		time.Sleep(50 * time.Millisecond)
		trickleConn.Write([]byte{b})
	}
	if err := <-trickle; err != nil {
		t.Fatal("per-read limit applied as a total deadline", err)
	}
	if time.Since(start) < 150*time.Millisecond {
		t.Fatal("trickle shorter than the so-timeout")
	}
	if err := server.SetAgentSoTimeout(-1); !errors.Is(err, ErrMalformed) {
		t.Fatal("negative agent so-timeout accepted", err)
	}
	stopTCP(t, cancel, done)
}

func TestServicePoolWaitsAndFollowsResize(t *testing.T) {
	server, listener, cancel, done := startTCP(t, TCPConfig{}, serverTimeHandlers())
	if err := server.SetServicePoolSize(1); err != nil {
		t.Fatal(err)
	}
	a := dialRaw(t, listener)
	a.Write(clientBytes(t, true, "SERVER_TIME"))
	expectBytes(t, a, serverTimeResponse, time.Second)

	b := dialRaw(t, listener)
	b.Write(clientBytes(t, true, "SERVER_TIME"))
	expectSilence(t, b, 150*time.Millisecond)
	if st := server.Stats(); st.ClientSessions != 1 || st.ClientWaiting != 1 || st.ServicePoolSize != 1 || st.Active != 2 {
		t.Fatalf("%+v", st)
	}

	server.SetServicePoolSize(2) // increase admits the waiter now
	expectBytes(t, b, serverTimeResponse, time.Second)

	c := dialRaw(t, listener)
	c.Write(clientBytes(t, true, "SERVER_TIME"))
	expectSilence(t, c, 100*time.Millisecond)

	server.SetServicePoolSize(1) // decrease: running sessions stay, no new admission
	a.Write(clientBytes(t, false, "CLOSE"))
	expectClosed(t, a, time.Second)
	expectSilence(t, c, 150*time.Millisecond)
	b.Write(clientBytes(t, false, "CLOSE"))
	expectBytes(t, c, serverTimeResponse, time.Second)
	if err := server.SetServicePoolSize(0); !errors.Is(err, ErrMalformed) {
		t.Fatal("zero pool size accepted", err)
	}
	if server.Stats().ServicePoolSize != 1 {
		t.Fatal("rejected pool size changed the limit")
	}
	stopTCP(t, cancel, done)
}

func TestServicePoolWaiterDisconnectAndShutdown(t *testing.T) {
	server, listener, cancel, done := startTCP(t, TCPConfig{}, serverTimeHandlers())
	server.SetServicePoolSize(1)
	a := dialRaw(t, listener)
	a.Write(clientBytes(t, true, "SERVER_TIME"))
	expectBytes(t, a, serverTimeResponse, time.Second)

	gone := dialRaw(t, listener)
	gone.Write(clientBytes(t, true))
	waitFor(t, "waiter", func() bool { return server.Stats().ClientWaiting == 1 })
	gone.Close()
	waitFor(t, "abandoned waiter released", func() bool {
		st := server.Stats()
		return st.ClientWaiting == 0 && st.ClientAbandoned == 1 && st.Active == 1
	})

	// EOF after buffered requests is also a peer that gave up (Scouter Clients
	// never half-close): it is closed without running them.
	half := dialRaw(t, listener)
	half.Write(clientBytes(t, true, "SERVER_TIME"))
	half.(*net.TCPConn).CloseWrite()
	waitFor(t, "half-closed waiter abandoned", func() bool {
		st := server.Stats()
		return st.ClientWaiting == 0 && st.ClientAbandoned == 2 && st.Active == 1
	})
	expectClosed(t, half, time.Second)
	a.Write(clientBytes(t, false, "CLOSE"))
	expectClosed(t, a, time.Second)

	// Shutdown releases sessions and waiters.
	waiting := dialRaw(t, listener)
	waiting.Write(clientBytes(t, true, "SERVER_TIME"))
	blocker := dialRaw(t, listener)
	blocker.Write(clientBytes(t, true, "SERVER_TIME"))
	waitFor(t, "one session one waiter", func() bool { st := server.Stats(); return st.ClientSessions == 1 && st.ClientWaiting == 1 })

	stopTCP(t, cancel, done)
	if st := server.Stats(); st.Active != 0 || st.ClientWaiting != 0 || st.ClientSessions != 0 {
		t.Fatalf("after shutdown %+v", st)
	}
}

func TestServicePoolDoesNotBlockAgents(t *testing.T) {
	agents := make(chan *AgentConn, 1)
	handlers := serverTimeHandlers()
	handlers.Agent = func(ctx context.Context, a *AgentConn) error { agents <- a; <-ctx.Done(); return ctx.Err() }
	server, listener, cancel, done := startTCP(t, TCPConfig{}, handlers)
	server.SetServicePoolSize(1)
	a := dialRaw(t, listener)
	a.Write(clientBytes(t, true, "SERVER_TIME"))
	expectBytes(t, a, serverTimeResponse, time.Second)
	registerAgent(t, listener, agents)
	stopTCP(t, cancel, done)
}

func TestSessionGateConcurrentResize(t *testing.T) {
	gate := newSessionGate(2, 1<<20)
	const maxLimit = 4
	var inside, peak atomic.Int32
	stop := make(chan struct{})
	var resizer sync.WaitGroup
	resizer.Add(1)
	go func() {
		defer resizer.Done()
		r := rand.New(rand.NewSource(1))
		for {
			select {
			case <-stop:
				gate.setLimit(maxLimit)
				return
			default:
				gate.setLimit(1 + r.Intn(maxLimit))
				time.Sleep(time.Duration(r.Intn(200)) * time.Microsecond)
			}
		}
	}()
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func(seed int64) {
			defer workers.Done()
			r := rand.New(rand.NewSource(seed))
			for j := 0; j < 200; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.Intn(300))*time.Microsecond)
				lost := make(chan error, 1)
				if r.Intn(8) == 0 {
					lost <- io.EOF
				}
				if err := gate.acquire(ctx, lost); err == nil {
					n := inside.Add(1)
					for {
						p := peak.Load()
						if n <= p || peak.CompareAndSwap(p, n) {
							break
						}
					}
					time.Sleep(time.Duration(r.Intn(50)) * time.Microsecond)
					inside.Add(-1)
					gate.release()
				}
				cancel()
			}
		}(int64(i))
	}
	workers.Wait()
	close(stop)
	resizer.Wait()
	if peak.Load() > maxLimit {
		t.Fatalf("peak %d above every limit", peak.Load())
	}
	if _, active, waiting := gate.snapshot(); active != 0 || waiting != 0 {
		t.Fatalf("leaked permits active=%d waiting=%d", active, waiting)
	}
}

type logCollector struct {
	mu    sync.Mutex
	lines []string
}

func (c *logCollector) add(id, message string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if id != "" {
		message = "[" + id + "] " + message
	}
	c.lines = append(c.lines, message)
}
func (c *logCollector) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}
func (c *logCollector) waitLines(t *testing.T, want ...string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := c.snapshot()
		if len(got) >= len(want) {
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("line %d:\n got %q\nwant %q\n all %q", i, got[i], want[i], got)
				}
			}
			if len(got) > len(want) {
				t.Fatalf("extra lines %q", got[len(want):])
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lines %q, want %q", got, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
func (c *logCollector) reset() { c.mu.Lock(); c.lines = nil; c.mu.Unlock() }

func javaPeer(conn net.Conn) string {
	return "/127.0.0.1:" + strconv.Itoa(conn.LocalAddr().(*net.TCPAddr).Port)
}

func TestTCPActionLogLines(t *testing.T) {
	logs := &logCollector{}
	agents := make(chan *AgentConn, 1)
	handlers := serverTimeHandlers()
	handlers.Authorize = func(context.Context, ClientRequest) (bool, error) { return false, nil }
	handlers.Agent = func(ctx context.Context, a *AgentConn) error { agents <- a; return nil }
	server, listener, cancel, done := startTCP(t, TCPConfig{}, handlers)
	server.SetLogFunc(logs.add)

	// Disabled (Java default false): nothing is printed.
	quiet := dialRaw(t, listener)
	quiet.Write(clientBytes(t, true, "SERVER_TIME"))
	expectBytes(t, quiet, serverTimeResponse, time.Second)
	quiet.Close()
	time.Sleep(50 * time.Millisecond)
	if lines := logs.snapshot(); len(lines) != 0 {
		t.Fatal(lines)
	}

	server.SetActionLog(true)
	closing := dialRaw(t, listener)
	closing.Write(clientBytes(t, true, "SERVER_TIME", "CLOSE"))
	expectBytes(t, closing, serverTimeResponse, time.Second)
	expectClosed(t, closing, time.Second)
	logs.waitLines(t, "Client : "+javaPeer(closing)+" open #1") // CLOSE prints no closed line

	logs.reset()
	eof := dialRaw(t, listener)
	eof.Write(clientBytes(t, true, "SERVER_TIME"))
	expectBytes(t, eof, serverTimeResponse, time.Second)
	eof.Close()
	logs.waitLines(t, "Client : "+javaPeer(eof)+" open #1", "Client : "+javaPeer(eof)+" closed")

	logs.reset()
	invalid := dialRaw(t, listener)
	invalid.Write(clientBytes(t, true, "OBJECT_LIST_REAL_TIME"))
	expectBytes(t, invalid, []byte{InvalidSession}, time.Second)
	logs.waitLines(t, "Client : "+javaPeer(invalid)+" open #1",
		"Client : "+javaPeer(invalid)+" closed java.lang.RuntimeException: Invalid session key : OBJECT_LIST_REAL_TIME workers=1")

	logs.reset()
	agent := dialRaw(t, listener)
	agent.Write(fixture(t, "tcp-agent-v1")) // objHash -7
	<-agents
	logs.waitLines(t, "Agent : "+javaPeer(agent)+" open [z7] #1", "Agent : "+javaPeer(agent)+" close")

	logs.reset()
	unknown := dialRaw(t, listener)
	unknown.Write([]byte{0, 0, 0, 0})
	logs.waitLines(t, "Unknown : "+javaPeer(unknown)+" drop")

	logs.reset()
	server.SetClientSoTimeout(30 * time.Millisecond)
	timeout2 := dialRaw(t, listener)
	timeout2.Write(clientBytes(t, true))
	logs.waitLines(t, "Client : "+javaPeer(timeout2)+" open #1", "Client : "+javaPeer(timeout2)+" closed")

	logs.reset()
	stopTCP(t, cancel, done)
	if lines := logs.snapshot(); len(lines) != 0 {
		t.Fatal("shutdown printed Java action lines", lines)
	}
}

func TestUDPConfigHelpers(t *testing.T) {
	for _, tc := range []struct {
		in, want int
		adjusted bool
	}{{65535, 65507, true}, {70000, 65507, true}, {0, 65507, true}, {-1, 65507, true}, {1500, 1500, false}, {65507, 65507, false}} {
		if got, adjusted := UDPPacketBufferSize(tc.in); got != tc.want || adjusted != tc.adjusted {
			t.Fatalf("buffer %d -> %d %v", tc.in, got, adjusted)
		}
	}
	for _, tc := range []struct {
		in, want int
		adjusted bool
	}{{3, 3, false}, {0, 3, true}, {-5, 3, true}, {1000, MaxUDPWorkers, true}, {MaxUDPWorkers, MaxUDPWorkers, false}} {
		if got, adjusted := UDPWorkerCount(tc.in); got != tc.want || adjusted != tc.adjusted {
			t.Fatalf("workers %d -> %d %v", tc.in, got, adjusted)
		}
	}
	if _, err := NewReassembler(UDPConfig{Workers: MaxUDPWorkers + 1}); err == nil {
		t.Fatal("unbounded workers accepted")
	}
	size, _ := UDPPacketBufferSize(1500)
	if _, err := NewReassembler(UDPConfig{MaxDatagramBytes: size, Workers: 3}); err != nil {
		t.Fatal(err)
	}
}

func startUDP(t *testing.T, config UDPConfig, setup func(*UDPReceiver)) (*UDPReceiver, *net.UDPConn, context.CancelFunc, <-chan error) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := NewUDPReceiver(conn, config, func(context.Context, Packet) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	setup(receiver)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- receiver.Serve(ctx) }()
	client, err := net.DialUDP("udp", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); client.Close() })
	return receiver, client, cancel, done
}

func fragmentObject(t *testing.T) int32 {
	raw := fixture(t, "udp-cafm-0")
	return int32(uint32(raw[4])<<24 | uint32(raw[5])<<16 | uint32(raw[6])<<8 | uint32(raw[7]))
}

func TestUDPMultipacketLogLines(t *testing.T) {
	object := fragmentObject(t)
	logs := &logCollector{}
	receiver, client, cancel, done := startUDP(t, UDPConfig{Workers: 1}, func(r *UDPReceiver) {
		r.SetLogFunc(logs.add)
		r.SetLogUDPMultipacket(true)
		r.SetObjectNameFunc(func(h int32) (string, bool) { return "/host/app", h == object })
	})
	client.Write(fixture(t, "udp-cafe")) // single packets never print S160
	client.Write(fixture(t, "udp-cafm-1"))
	client.Write(fixture(t, "udp-cafm-0"))
	logs.waitLines(t, "[S160] recv scouter.lang.pack.TextPack total=2 object=("+strconv.Itoa(int(object))+")/host/app /127.0.0.1")
	waitFor(t, "packets", func() bool { return receiver.Stats().Packets == 2 })
	cancel()
	<-done
}

func TestUDPExpiredMultipacketLogAndRateLimit(t *testing.T) {
	object := fragmentObject(t)
	logs := &logCollector{}
	receiver, client, cancel, done := startUDP(t, UDPConfig{Workers: 1, FragmentTTL: 10 * time.Millisecond}, func(r *UDPReceiver) {
		r.SetLogFunc(logs.add) // log_expired_multipacket defaults to true
	})
	client.Write(fixture(t, "udp-cafm-0"))
	logs.waitLines(t, "[S150] MultiPacket total=2 recv=1 object=("+strconv.Itoa(int(object))+")null /127.0.0.1")
	client.Write(fixture(t, "udp-cafm-0"))
	waitFor(t, "second expiry", func() bool { return receiver.Stats().Expired == 2 })
	time.Sleep(20 * time.Millisecond)
	if lines := logs.snapshot(); len(lines) != 1 {
		t.Fatal("S150 not limited to one line per 10 seconds", lines)
	}
	cancel()
	<-done

	logs.reset()
	receiver, client, cancel, done = startUDP(t, UDPConfig{Workers: 1, FragmentTTL: 10 * time.Millisecond}, func(r *UDPReceiver) {
		r.SetLogFunc(logs.add)
		r.SetLogExpiredMultipacket(false)
	})
	client.Write(fixture(t, "udp-cafm-0"))
	waitFor(t, "expiry", func() bool { return receiver.Stats().Expired == 1 })
	if lines := logs.snapshot(); len(lines) != 0 {
		t.Fatal(lines)
	}
	cancel()
	<-done

	var limit rateLimit
	t0 := time.Unix(1700000000, 0)
	if !limit.allow(t0, 10*time.Second) || limit.allow(t0.Add(10*time.Second-time.Millisecond), 10*time.Second) || !limit.allow(t0.Add(10*time.Second), 10*time.Second) {
		t.Fatal("Logger.checkOk window")
	}
}

func TestDecodeExpiredCarriesAssembly(t *testing.T) {
	a, _ := NewReassembler(UDPConfig{FragmentTTL: time.Millisecond})
	sender := netip.MustParseAddrPort("127.0.0.1:1")
	now := time.Now()
	a.Decode(sender, fixture(t, "udp-cafm-0"), now)
	_, _, err := a.decode(sender, fixture(t, "udp-cafm-1"), now.Add(time.Millisecond))
	var expired *expiredError
	if !errors.Is(err, ErrExpired) || !errors.As(err, &expired) {
		t.Fatal(err)
	}
	if got := expired.assembly; got.Total != 2 || got.Received != 1 || got.ObjectHash != fragmentObject(t) || got.Sender != sender {
		t.Fatalf("%+v", got)
	}
}

// Expected strings were printed by Zulu JDK 8 from scouter.util.Hexa32 at the
// baseline commit and java.net.InetAddress/InetSocketAddress.toString().
func TestJavaLogFormatting(t *testing.T) {
	for _, tc := range []struct {
		in   int64
		want string
	}{{0, "0"}, {9, "9"}, {10, "xa"}, {31, "xv"}, {32, "x10"}, {-1, "z1"}, {-7, "z7"}, {2147483647, "x1vvvvvv"}, {-2147483648, "z2000000"}, {1234567, "x15lk7"}, {math.MinInt64, "z8000000000000"}} {
		if got := hexa32(tc.in); got != tc.want {
			t.Fatalf("hexa32(%d)=%q want %q", tc.in, got, tc.want)
		}
	}
	for _, tc := range []struct{ in, want string }{
		{"127.0.0.1", "/127.0.0.1"}, {"::1", "/0:0:0:0:0:0:0:1"}, {"2001:db8::a", "/2001:db8:0:0:0:0:0:a"}, {"::ffff:10.1.2.3", "/10.1.2.3"},
	} {
		if got := javaInetAddress(netip.MustParseAddr(tc.in)); got != tc.want {
			t.Fatalf("%s -> %q want %q", tc.in, got, tc.want)
		}
	}
	if got := javaSocketAddress(&net.TCPAddr{IP: net.ParseIP("::1"), Port: 54321}); got != "/0:0:0:0:0:0:0:1:54321" {
		t.Fatal(got)
	}
	if got := javaSocketAddress(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 54321}); got != "/127.0.0.1:54321" {
		t.Fatal(got)
	}
	if got := packClassName(&protocol.ProfilePack{Version2: true}); got != "scouter.lang.pack.XLogProfilePack2" {
		t.Fatal(got)
	}
	if got := sanitizeLogText("A\nB\x00C"); got != "A?B?C" {
		t.Fatal(got)
	}
	long := sanitizeLogText(string(bytes.Repeat([]byte("한"), 200)))
	if len(long) > maxLogText+3 || !utf8.ValidString(long) {
		t.Fatal(len(long))
	}
}
