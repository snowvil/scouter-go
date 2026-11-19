package main

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

func startAgentServer(t *testing.T, config transport.TCPConfig, agent func(context.Context, *transport.AgentConn) error) (*transport.TCPServer, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := transport.NewTCPServer(listener, config, transport.TCPHandlers{Agent: agent}, nil)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { server.Serve(ctx); close(done) }()
	t.Cleanup(func() { cancel(); server.Close(); <-done })
	return server, listener.Addr().String()
}

func testFleet(addr string, agents int, v1Ratio float64) *fleet {
	return &fleet{o: agentsOptions{TCP: addr, Agents: agents, SessionCount: 1, V1Ratio: v1Ratio, NamePrefix: "/t", Seed: 1,
		ConnectTimeout: time.Second, SoTimeout: 5 * time.Second, RetryInterval: 20 * time.Millisecond, Settle: 50 * time.Millisecond}, start: time.Now()}
}

func runFleetAgents(t *testing.T, f *fleet) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, a := range f.specs() {
		wg.Add(1)
		go func() { defer wg.Done(); f.runAgent(ctx, a) }()
	}
	stop := func() { cancel(); wg.Wait() }
	t.Cleanup(stop)
	return stop
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The emulated agents speak both Agent framings to the Go transport: a
// KEEP_ALIVE gets a bare NoNEXT and a relayed command one Pack.
func TestFleetAnswersGoServerAgentRequests(t *testing.T) {
	type result struct {
		version, keepPacks int
		answer             string
		err                error
	}
	results := make(chan result, 4)
	_, addr := startAgentServer(t, transport.TCPConfig{}, func(ctx context.Context, a *transport.AgentConn) error {
		r := result{version: a.Version}
		r.err = a.RequestPack(ctx, "KEEP_ALIVE", &protocol.MapPack{}, func(context.Context, protocol.Pack) error { r.keepPacks++; return nil })
		if r.err == nil {
			r.err = a.RequestPack(ctx, "OBJECT_ENV", &protocol.MapPack{}, func(_ context.Context, p protocol.Pack) error {
				if m, ok := p.(*protocol.MapPack); ok {
					r.answer = mapGet(m, "agent").Text
				}
				return nil
			})
		}
		results <- r
		<-ctx.Done()
		return nil
	})
	f := testFleet(addr, 4, 0.5)
	stop := runFleetAgents(t, f)
	versions := map[int]int{}
	for i := 0; i < 4; i++ {
		select {
		case r := <-results:
			if r.err != nil || r.keepPacks != 0 || r.answer == "" {
				t.Fatalf("agent V%d: err=%v keepalive packs=%d answer=%q", r.version, r.err, r.keepPacks, r.answer)
			}
			versions[r.version]++
		case <-time.After(5 * time.Second):
			t.Fatal("agent requests timed out")
		}
	}
	if versions[1] != 2 || versions[2] != 2 {
		t.Fatalf("versions %v, want 2 V1 and 2 V2", versions)
	}
	waitFor(t, "four settled sessions", func() bool { return f.settled.Load() == 4 && f.everConnected.Load() == 4 })
	stop()
	if f.keepalives.Load() != 4 || f.responses.Load() != 4 || f.writeErrors.Load() != 0 {
		t.Fatalf("keepalives=%d responses=%d write errors=%d", f.keepalives.Load(), f.responses.Load(), f.writeErrors.Load())
	}
	if f.connected.Load() != 0 || f.settled.Load() != 0 {
		t.Fatalf("open %d settled %d after stop", f.connected.Load(), f.settled.Load())
	}
}

// Agents over the Agent budget are reset before any command, counted as such,
// and keep retrying on their tick; a freed slot is taken again.
func TestFleetRetriesAfterBudgetReject(t *testing.T) {
	release := make(chan struct{}, 1)
	server, addr := startAgentServer(t, transport.TCPConfig{MaxAgentConnections: 2}, func(ctx context.Context, a *transport.AgentConn) error {
		select {
		case <-ctx.Done():
		case <-release:
		}
		return nil
	})
	f := testFleet(addr, 3, 0)
	runFleetAgents(t, f)
	waitFor(t, "two settled sessions and rejections", func() bool {
		return f.settled.Load() == 2 && server.Stats().AgentRejected >= 2 && f.endedEarly.total() >= 2
	})
	for class := range f.endedEarly.snapshot() {
		if class != "reset" && class != "eof" {
			t.Fatalf("rejected sessions ended with %v", f.endedEarly.snapshot())
		}
	}
	if f.everConnected.Load() != 2 {
		t.Fatalf("agents settled once: %d, want 2 (rejects never settle)", f.everConnected.Load())
	}
	// End one session on the server: the slot is taken again (by whichever
	// agent's tick comes first) and the rejections go on.
	rejected := server.Stats().AgentRejected
	release <- struct{}{}
	waitFor(t, "slot taken again and rejections continue", func() bool {
		return f.settled.Load() == 2 && server.Stats().AgentConnections == 2 && server.Stats().AgentRejected > rejected+1
	})
}

func TestNextTickFollowsAgentGrid(t *testing.T) {
	start := time.Unix(100, 0)
	phase, retry := 300*time.Millisecond, time.Second
	for _, c := range []struct{ at, want time.Duration }{
		{0, 300 * time.Millisecond},
		{300 * time.Millisecond, 300 * time.Millisecond},
		{301 * time.Millisecond, 1300 * time.Millisecond},
		{4200 * time.Millisecond, 4300 * time.Millisecond},
	} {
		if got := nextTick(start, phase, retry, start.Add(c.at)).Sub(start); got != c.want {
			t.Fatalf("nextTick(+%v) = +%v, want +%v", c.at, got, c.want)
		}
	}
}

func TestAgentsOptionsRejectNonLoopback(t *testing.T) {
	if _, err := parseAgentsOptions([]string{"--tcp=10.0.0.1:6100"}); err == nil {
		t.Fatal("non-loopback TCP accepted")
	}
	o, err := parseAgentsOptions([]string{"--tcp=127.0.0.1:6100", "--udp=127.0.0.1:6100", "--agents=10"})
	if err != nil || o.Agents != 10 {
		t.Fatalf("options %+v err=%v", o, err)
	}
}
