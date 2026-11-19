package server

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/config"
	"github.com/snowvil/scouter-go/internal/pulse"
	"github.com/snowvil/scouter-go/internal/pulse/kafka"
	"github.com/snowvil/scouter-go/internal/transport"
	"github.com/snowvil/scouter-go/internal/webapp"
)

// The go_agent_relay_* keys change the relay budget live: a larger budget
// admits a waiting relay at once, a smaller one refuses new relays until the
// held permits fit (none is revoked), and zero values return to the defaults.
func TestRelayBudgetConfigureLive(t *testing.T) {
	b := &relayBudget{logf: func(string, ...any) {}}
	b.configure(100, 1, 30*time.Millisecond)
	if st := b.snapshot(); st.Limit != 100 || st.MaxLarge != 1 || b.serverCallWait() != 30*time.Millisecond {
		t.Fatalf("configured %+v wait %s", st, b.serverCallWait())
	}
	ctx := context.Background()
	first, _ := b.acquire(ctx, 1, "A", 60, false, 0, false)
	if first == nil {
		t.Fatal("first relay refused")
	}
	got := make(chan *relayPermit, 1)
	go func() {
		p, _ := b.acquire(ctx, 2, "B", 60, false, 5*time.Second, false)
		got <- p
	}()
	waitFor(t, "a waiting relay", func() bool { return b.snapshot().Waiting == 1 })
	start := time.Now()
	b.configure(200, 1, 0)
	second := <-got
	if second == nil || time.Since(start) > 2*time.Second {
		t.Fatal("a larger budget did not admit the waiter")
	}
	b.configure(50, 1, 0)
	if p, _ := b.acquire(ctx, 3, "C", 10, false, 0, false); p != nil {
		t.Fatal("a smaller budget admitted a relay past it")
	}
	if st := b.snapshot(); st.Bytes != 120 || st.Limit != 50 {
		t.Fatalf("held permits must survive a smaller budget: %+v", st)
	}
	first.release()
	second.release()
	// A relay above the budget is clamped to it and runs alone.
	big, _ := b.acquire(ctx, 4, "D", 80, true, 0, false)
	if big == nil || b.snapshot().Bytes != 50 {
		t.Fatalf("clamped relay %+v", b.snapshot())
	}
	big.release()
	l1, _ := b.acquire(ctx, 5, "E", 1, true, 0, false)
	if p, _ := b.acquire(ctx, 6, "F", 1, true, 0, false); p != nil || l1 == nil {
		t.Fatal("go_agent_relay_max_large=1 admitted a second large relay")
	}
	b.configure(50, 2, 0)
	l2, _ := b.acquire(ctx, 6, "F", 1, true, 0, false)
	if l2 == nil {
		t.Fatal("go_agent_relay_max_large=2 refused the second large relay")
	}
	l1.release()
	l2.release()
	b.configure(0, 0, 0)
	if st := b.snapshot(); st.Limit != defaultRelayBudgetBytes() || st.MaxLarge != agentRelayMaxLarge || b.serverCallWait() != agentRelayServerWait {
		t.Fatalf("defaults %+v wait %s", st, b.serverCallWait())
	}
}

// go_text_cache_reset_parallel bounds the concurrent reset calls of the next
// pass; zero keeps textCacheResetParallel.
func TestTextCacheResetLiveParallel(t *testing.T) {
	var parallel atomic.Int64
	var mu sync.Mutex
	running, peak := 0, 0
	tr := &textCacheResetter{liveParallel: func() int { return int(parallel.Load()) }, call: func(context.Context, int32) (bool, error) {
		mu.Lock()
		running++
		peak = max(peak, running)
		mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		mu.Lock()
		running--
		mu.Unlock()
		return true, nil
	}}
	hashes := make([]int32, 64)
	for i := range hashes {
		hashes[i] = int32(i + 1)
	}
	for _, c := range []struct{ live, want int }{{2, 2}, {0, textCacheResetParallel}} {
		parallel.Store(int64(c.live))
		peak = 0
		for _, out := range tr.callAll(context.Background(), hashes) {
			if !out.made || !out.called {
				t.Fatal("call not made")
			}
		}
		if peak > c.want || (c.live > 0 && peak != c.want) {
			t.Fatalf("live %d: peak %d calls at once, want at most %d", c.live, peak, c.want)
		}
	}
}

// Every go_* limit reaches its consumer at startup; on reload the live keys
// change and the restart keys keep the startup value; removing the keys
// returns the live ones to their defaults.
func TestGoLimitsFromConfig(t *testing.T) {
	original := newPulseSink
	newPulseSink = func(pulse.Config, int16, func(kafka.ErrorEvent)) (pulse.Sink, *kafka.Producer, error) {
		return &memorySink{}, nil, nil
	}
	t.Cleanup(func() { newPulseSink = original })
	root := t.TempDir()
	confPath := filepath.Join(root, "scouter.conf")
	write := func(text string, at time.Time) {
		t.Helper()
		if err := os.WriteFile(confPath, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(confPath, at, at); err != nil {
			t.Fatal(err)
		}
	}
	at := time.Now().Add(-time.Hour)
	write("go_http_max_connections=100\ngo_http_ops_max_inflight=3\ngo_http_servlet_max_inflight=5\ngo_http_servlet_queue_wait_ms=1500\n"+
		"go_http_webapp_max_inflight=7\ngo_http_webapp_queue_wait_ms=250\ngo_client_response_budget_mb=300\n"+
		"go_agent_relay_budget_mb=1024\ngo_agent_relay_max_large=3\ngo_agent_relay_server_wait_ms=2000\ngo_text_cache_reset_parallel=4\n"+
		"go_text_max_unknown_divs=5\ngo_pulse_xlog_queue_mb=2\ngo_pulse_profile_queue_mb=3\ngo_visitor_resident_mb=32\ngo_visitor_delta_mb=16\n", at)
	var rt atomic.Pointer[Runtime]
	var store *config.Store
	store, err := config.Open(confPath, config.Options{Lookup: func(string) (string, bool) { return "", false }, HostName: "h",
		OnChange: func(config.Change) {
			if r := rt.Load(); r != nil {
				r.ApplySettings(store.Current().Live())
			}
		}})
	if err != nil {
		t.Fatal(err)
	}
	w := webapp.DefaultConfig()
	r, err := Open(Config{Directory: filepath.Join(root, "db"), LogDirectory: filepath.Join(root, "logs"), TelegrafConfigurationPath: filepath.Join(root, "missing.xml"),
		Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", HTTP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>"), ConfigFile: store,
		ScouterHTTP: &ScouterHTTPConfig{Listen: "127.0.0.1:0", Webapp: &w},
		Pulse:       &PulseConfig{Values: map[string]string{pulse.KeyKafkaServers: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rt.Store(r)
	check := func(phase string, relayLimit int64, maxLarge int, serverWait time.Duration, parallel int) {
		t.Helper()
		for name, l := range map[string]httpLimitsStats{"http": r.http.limits.stats(), "scouterHttp": r.scouterHTTP.server.limits.stats()} {
			if l.MaxConnections != 100 || l.Operational.Limit != 3 {
				t.Fatalf("%s: %s listener %+v", phase, name, l)
			}
		}
		if g := r.scouterHTTP.webGate; g.stats().Limit != 7 || g.wait != 250*time.Millisecond {
			t.Fatalf("%s: webapp gate %+v wait %s", phase, g.stats(), g.wait)
		}
		if n := r.scouterHTTP.api.Stats().MaxInFlight; n != 5 {
			t.Fatalf("%s: servlet in flight %d", phase, n)
		}
		if n := r.tcp.Stats().MaxClientResponseBytes; n != 300<<20 {
			t.Fatalf("%s: Client response budget %d", phase, n)
		}
		if st := r.pulse.dispatcher.Stats(); st.XLog.MaxPendingBytes != 2<<20 || st.Profile.MaxPendingBytes != 3<<20 {
			t.Fatalf("%s: pulse %d %d", phase, st.XLog.MaxPendingBytes, st.Profile.MaxPendingBytes)
		}
		if st := r.pool.budget.snapshot(); st.Limit != relayLimit || st.MaxLarge != maxLarge || r.pool.budget.serverCallWait() != serverWait {
			t.Fatalf("%s: relay budget %+v wait %s", phase, st, r.pool.budget.serverCallWait())
		}
		if n := r.liveNow().Limits.TextCacheResetParallel; n != parallel {
			t.Fatalf("%s: text cache reset parallel %d", phase, n)
		}
	}
	unknownDivs := func(phase string, want int) {
		t.Helper()
		if n := r.store.MaxUnknownTextDivs(); n != want {
			t.Fatalf("%s: go_text_max_unknown_divs %d, want %d", phase, n, want)
		}
	}
	visitorBudgets := func(phase string, resident, delta int64) {
		t.Helper()
		if st := r.store.VisitorStats(); st.ResidentMax != resident || st.DeltaMax != delta {
			t.Fatalf("%s: visitor budgets %d %d, want %d %d", phase, st.ResidentMax, st.DeltaMax, resident, delta)
		}
	}
	check("startup", 1024<<20, 3, 2*time.Second, 4)
	unknownDivs("startup", 5)
	visitorBudgets("startup", 32<<20, 16<<20)
	// Live keys change, restart keys (here every go_http_*, the Client and
	// Pulse budgets) keep the startup value.
	write("go_http_max_connections=200\ngo_http_ops_max_inflight=4\ngo_http_servlet_max_inflight=6\ngo_http_webapp_max_inflight=8\n"+
		"go_client_response_budget_mb=512\ngo_pulse_xlog_queue_mb=4\n"+
		"go_agent_relay_budget_mb=2048\ngo_agent_relay_max_large=1\ngo_agent_relay_server_wait_ms=500\ngo_text_cache_reset_parallel=9\ngo_text_max_unknown_divs=11\n"+
		"go_visitor_resident_mb=1\ngo_visitor_delta_mb=128\n", at.Add(time.Minute))
	if _, err := store.Reload(true); err != nil {
		t.Fatal(err)
	}
	check("reload", 2048<<20, 1, 500*time.Millisecond, 9)
	unknownDivs("reload", 11)
	visitorBudgets("reload", 1<<20, 128<<20)
	write("a=1\n", at.Add(2*time.Minute))
	if _, err := store.Reload(true); err != nil {
		t.Fatal(err)
	}
	check("removed", defaultRelayBudgetBytes(), agentRelayMaxLarge, agentRelayServerWait, 0)
	unknownDivs("removed", 0)
	visitorBudgets("removed", 256<<20, 64<<20)
}

// Without the keys every bound keeps its built-in default.
func TestGoLimitsAbsentKeepDefaults(t *testing.T) {
	root := t.TempDir()
	w := webapp.DefaultConfig()
	r, err := Open(Config{Directory: filepath.Join(root, "db"), LogDirectory: filepath.Join(root, "logs"), TelegrafConfigurationPath: filepath.Join(root, "missing.xml"),
		Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", HTTP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>"),
		ScouterHTTP: &ScouterHTTPConfig{Listen: "127.0.0.1:0", Webapp: &w}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	conns := httpConnectionLimit(httpMaxConnections)
	if l := r.http.limits.stats(); l.MaxConnections != conns || l.Operational.Limit != httpOpsInFlight {
		t.Fatalf("http %+v", l)
	}
	if g := r.scouterHTTP.webGate; g.stats().Limit != httpWebappInFlight || g.wait != httpQueueWait {
		t.Fatalf("webapp gate %+v %s", g.stats(), g.wait)
	}
	if n := r.scouterHTTP.api.Stats().MaxInFlight; n != 64 {
		t.Fatalf("servlet %d", n)
	}
	if n := r.tcp.Stats().MaxClientResponseBytes; n != transport.DefaultClientResponseBytes() {
		t.Fatalf("Client response budget %d", n)
	}
	if st := r.pool.budget.snapshot(); st.Limit != defaultRelayBudgetBytes() || st.MaxLarge != agentRelayMaxLarge || r.pool.budget.serverCallWait() != agentRelayServerWait {
		t.Fatalf("relay budget %+v", st)
	}
	if st := r.store.VisitorStats(); st.ResidentMax != 256<<20 || st.DeltaMax != 64<<20 {
		t.Fatalf("visitor budgets %+v", st)
	}
	if (r.liveNow().Limits != config.GoLimits{}) {
		t.Fatalf("limits %+v", r.liveNow().Limits)
	}
}

// configure racing acquire/release, waits that expire and callers that leave:
// once every relay has ended no bytes, large slots or waiters remain.
func TestRelayBudgetConfigureStress(t *testing.T) {
	b := &relayBudget{logf: func(string, ...any) {}}
	b.configure(64, 1, 0)
	stop := make(chan struct{})
	var cfg sync.WaitGroup
	cfg.Add(1)
	go func() {
		defer cfg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			// Shrink and grow around the relays' sizes.
			b.configure(int64(16+(i%5)*24), 1+i%3, 0)
			time.Sleep(100 * time.Microsecond)
		}
	}()
	var relays sync.WaitGroup
	var granted, refused, left atomic.Int64
	for w := range 16 {
		relays.Add(1)
		go func() {
			defer relays.Done()
			for i := range 200 {
				ctx, cancel := context.WithCancel(context.Background())
				if (w+i)%7 == 0 {
					// The caller leaves while waiting.
					time.AfterFunc(200*time.Microsecond, cancel)
				}
				p, err := b.acquire(ctx, int32(w), "X", int64(8+(w+i)%40), (w+i)%5 == 0, time.Duration((w+i)%3)*time.Millisecond, w%2 == 0)
				switch {
				case err != nil:
					left.Add(1)
				case p == nil:
					refused.Add(1)
				default:
					granted.Add(1)
					time.Sleep(50 * time.Microsecond)
					p.release()
					p.release() // a second release is a no-op
				}
				cancel()
			}
		}()
	}
	relays.Wait()
	close(stop)
	cfg.Wait()
	st := b.snapshot()
	if st.Bytes != 0 || st.Large != 0 || st.Waiting != 0 {
		t.Fatalf("budget not empty after the relays ended: %+v", st)
	}
	if granted.Load() == 0 || refused.Load() == 0 || uint64(granted.Load()) != st.Granted {
		t.Fatalf("granted %d refused %d left %d, stats %+v", granted.Load(), refused.Load(), left.Load(), st)
	}
}
