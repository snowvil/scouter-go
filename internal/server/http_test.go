package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/httpapi"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/pulse"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/webapp"
)

type recordingAcceptor struct {
	mu    sync.Mutex
	packs []protocol.Pack
}

func (a *recordingAcceptor) AcceptFrom(_ context.Context, p protocol.Pack, _ string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.packs = append(a.packs, p)
	return nil
}

func (a *recordingAcceptor) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.packs)
}

func TestScouterHTTPSeparateListener(t *testing.T) {
	dir := t.TempDir()
	xml := filepath.Join(dir, "scouter-telegraf.xml")
	if err := os.WriteFile(xml, []byte(telegrafSample), 0o600); err != nil {
		t.Fatal(err)
	}
	acc := &recordingAcceptor{}
	s, err := openScouterHTTP(ScouterHTTPConfig{Listen: "127.0.0.1:0", API: httpapi.Config{LogDir: filepath.Join(dir, "logs"), LogKeepDays: 31}}, acc, xml)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.serve(ctx)
	if s.addr() == "" {
		t.Fatal("no listener")
	}
	body := "mem,host=h1 used_percent=1 1700000000000000000\n"
	resp, err := http.Post("http://"+s.addr()+"/telegraf/metric", "text/plain; charset=utf-8", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 || acc.count() != 2 {
		t.Fatalf("%d %d", resp.StatusCode, acc.count())
	}
	resp, err = http.Get("http://" + s.addr() + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatal("the servlet listener does not serve /health", resp.StatusCode)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	// /health never reaches the servlet API on the separate listener (the
	// mux answers 404), so only the Telegraf request is counted and logged.
	if st := s.api.Stats(); st.Requests != 1 || st.LogLines != 1 {
		t.Fatalf("%+v", st)
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "logs", "http-request-*.log"))
	if len(matches) != 1 {
		t.Fatal(matches)
	}
}

func TestScouterHTTPMountedOnOperationalMux(t *testing.T) {
	acc := &recordingAcceptor{}
	s, err := openScouterHTTP(ScouterHTTPConfig{}, acc, filepath.Join(t.TempDir(), "missing.xml"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if s.addr() != "" {
		t.Fatal("mounted API must not listen")
	}
	mux := (&Runtime{}).operationalMux(s.api)
	for path, pattern := range map[string]string{"/counter": "/counter", "/counter/x": "/counter/", "/register": "/register", "/telegraf/metric": "/telegraf/", "/health": "GET /health", "/debug/pprof/": "/debug/pprof/"} {
		method := "POST"
		if path == "/health" {
			method = "GET"
		}
		_, got := mux.Handler(httptest.NewRequest(method, path, nil))
		if got != pattern {
			t.Errorf("%s routed to %q want %q", path, got, pattern)
		}
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/counter", strings.NewReader(`{"object":{"host":"h"}}`))
	req.RemoteAddr = "127.0.0.1:1"
	mux.ServeHTTP(rec, req)
	if rec.Code != 200 || acc.count() != 1 {
		t.Fatal(rec.Code, acc.count())
	}
	if (&Runtime{}).operationalMux(nil) == nil {
		t.Fatal("nil api")
	}
}

func TestHTTPServerCloseWithoutServe(t *testing.T) {
	s, err := listenHTTP("127.0.0.1:0", http.NotFoundHandler(), httpAdmission{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close without serve blocked")
	}
}

// The runtime opens the Java servlet listener from Config.ScouterHTTP, feeds
// the real ingest engine, reports it in /health and reloads the Telegraf
// configuration on demand.
func TestRuntimeScouterHTTPEndToEnd(t *testing.T) {
	root := t.TempDir()
	xml := filepath.Join(root, "scouter-telegraf.xml")
	if err := os.WriteFile(xml, []byte(telegrafSample), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Directory: filepath.Join(root, "db"), LogDirectory: filepath.Join(root, "logs"), TelegrafConfigurationPath: xml, Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", HTTP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>"),
		ScouterHTTP: &ScouterHTTPConfig{Listen: "127.0.0.1:0", API: httpapi.Config{AllowIPs: []string{"*"}}}}
	r, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	addr := r.scouterHTTP.addr()
	if addr == "" {
		t.Fatal("servlet listener missing")
	}
	resp, err := http.Post("http://"+addr+"/telegraf/metric", "text/plain; charset=utf-8", strings.NewReader("mem,host=h1 used_percent=1 1700000000000000000\n"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	if n := r.engine.ObjectCount(); n != 1 {
		t.Fatalf("telegraf object not ingested: %d", n)
	}
	r.ReloadTelegraf(nil)
	resp, err = http.Get("http://" + r.http.addr + "/health")
	if err != nil {
		t.Fatal(err)
	}
	var health struct {
		Endpoints   Endpoints `json:"endpoints"`
		ScouterHTTP struct {
			Requests uint64 `json:"Requests"`
		} `json:"scouterHttp"`
		AgentOrphans     *agentOrphanStats                 `json:"agentOrphans"`
		AgentRelayBudget *relayBudgetStats                 `json:"agentRelayBudget"`
		HTTPAdmission    map[string]httpLimitsStats        `json:"httpAdmission"`
		IndexOpens       map[string]storage.IndexOpenStats `json:"indexOpens"`
	}
	err = json.NewDecoder(resp.Body).Decode(&health)
	resp.Body.Close()
	if err != nil || health.Endpoints.ScouterHTTP != addr || health.ScouterHTTP.Requests != 1 || health.AgentOrphans == nil || len(health.IndexOpens) != 2 {
		t.Fatalf("health %+v %v", health, err)
	}
	if b := health.AgentRelayBudget; b == nil || b.Limit != defaultRelayBudgetBytes() || b.MaxLarge != agentRelayMaxLarge {
		t.Fatalf("relay budget %+v", b)
	}
	if a := health.HTTPAdmission; a["http"].MaxConnections == 0 || a["http"].Operational.Served != 1 || a["scouterHttp"].MaxConnections == 0 {
		t.Fatalf("http admission %+v", a)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// The webapp mounts under /scouter/ on the servlet listener, logs in to the
// runtime's own TCP endpoint as __local and answers /v1/info/server.
func TestRuntimeWebappMounted(t *testing.T) {
	root := t.TempDir()
	w := webapp.DefaultConfig()
	cfg := Config{Directory: filepath.Join(root, "db"), LogDirectory: filepath.Join(root, "logs"), AccountDir: filepath.Join(root, "conf"), TelegrafConfigurationPath: filepath.Join(root, "missing.xml"), Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", HTTP: "127.0.0.1:0", CounterXML: []byte("<Counters/>"),
		ScouterHTTP: &ScouterHTTPConfig{Listen: "127.0.0.1:0", Webapp: &w}}
	r, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	resp, err := http.Get("http://" + r.scouterHTTP.addr() + "/scouter/v1/info/server")
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Status string `json:"status"`
		Result []struct {
			Connected bool `json:"connected"`
		} `json:"result"`
	}
	err = json.NewDecoder(resp.Body).Decode(&body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != 200 || body.Status != "200" || len(body.Result) != 1 || !body.Result[0].Connected {
		t.Fatalf("status=%d body=%+v err=%v", resp.StatusCode, body, err)
	}
	// The webapp gate is in /health.
	resp, err = http.Get("http://" + r.http.addr + "/health")
	if err != nil {
		t.Fatal(err)
	}
	var health struct {
		HTTPAdmission struct {
			Webapp httpGateStats `json:"webapp"`
		} `json:"httpAdmission"`
	}
	err = json.NewDecoder(resp.Body).Decode(&health)
	resp.Body.Close()
	if g := health.HTTPAdmission.Webapp; err != nil || g.Served != 1 || g.Limit != httpWebappInFlight {
		t.Fatalf("webapp gate %+v %v", g, err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// Java EmbeddedConfigureAdaptor dials 127.0.0.1:<port>; unspecified and
// loopback listeners therefore map to the same collector address and serverId.
func TestWebappCollectorAddr(t *testing.T) {
	cases := map[string]string{"0.0.0.0:6100": "127.0.0.1:6100", "[::]:6100": "127.0.0.1:6100", "127.0.0.1:6100": "127.0.0.1:6100", "[::1]:6100": "127.0.0.1:6100", "10.0.0.5:6100": "10.0.0.5:6100"}
	for in, want := range cases {
		addr, err := net.ResolveTCPAddr("tcp", in)
		if err != nil {
			t.Fatal(err)
		}
		if got := webappCollectorAddr(addr); got != want {
			t.Errorf("%s: got %s want %s", in, got, want)
		}
	}
}

// --lock-stats serves the engine lock, the gates and the writers' shares of
// the Store gate in /health; without it there is no "locks" field.
func TestHealthLockStats(t *testing.T) {
	for _, on := range []bool{false, true} {
		root := t.TempDir()
		r, err := Open(Config{Directory: filepath.Join(root, "db"), LogDirectory: filepath.Join(root, "logs"), TelegrafConfigurationPath: filepath.Join(root, "missing.xml"), Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", HTTP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>"), LockStats: on})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()
		end := time.Now().UnixMilli()
		if err := r.engine.Accept(ctx, &protocol.XLogPack{EndTime: end, ObjHash: 1, Service: 5, TxID: 1, Elapsed: 1, TailVersion: protocol.XLogCurrentTail}); err != nil {
			t.Fatal(err)
		}
		if err := r.store.Sync(); err != nil {
			t.Fatal(err)
		}
		resp, err := http.Get("http://" + r.http.addr + "/health")
		if err != nil {
			t.Fatal(err)
		}
		var health struct {
			Locks map[string]struct {
				Acquired uint64  `json:"acquired"`
				HeldMs   float64 `json:"heldMs"`
			} `json:"locks"`
		}
		err = json.NewDecoder(resp.Body).Decode(&health)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !on {
			if health.Locks != nil {
				t.Fatal("locks without --lock-stats", health.Locks)
			}
		} else {
			for _, name := range []string{"engine", "store", "tags", "store.xlogWriter", "store.profileWriter", "store.textWriter"} {
				if _, ok := health.Locks[name]; !ok {
					t.Fatal("missing", name, health.Locks)
				}
			}
			if health.Locks["engine"].Acquired == 0 || health.Locks["store"].Acquired == 0 || health.Locks["store.xlogWriter"].Acquired == 0 {
				t.Fatalf("not counted: %+v", health.Locks)
			}
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

// close gives in-flight handlers 2s (Shutdown) and then closes their
// connections without waiting for them (http.Server.Close): a handler can
// still be running when Close moves on to the later stages.
func TestHTTPServerCloseLeavesStuckHandler(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	finished := make(chan struct{})
	s, err := listenHTTP("127.0.0.1:0", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
		close(finished)
	}), httpAdmission{})
	if err != nil {
		t.Fatal(err)
	}
	s.serve(context.Background())
	go func() {
		if resp, err := http.Get("http://" + s.addr + "/"); err == nil {
			resp.Body.Close()
		}
	}()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- s.close() }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("close waited for the handler")
	}
	select {
	case <-finished:
		t.Fatal("handler finished before release")
	default:
	}
	close(release)
	<-finished
}

// A /health request still running after the HTTP stage (see
// TestHTTPServerCloseLeavesStuckHandler) reads the listener, servlet and
// Pulse fields while Close tears them down: no data race (-race).
func TestHealthDuringClose(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Directory: filepath.Join(root, "db"), LogDirectory: filepath.Join(root, "logs"), Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", HTTP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>"),
		ScouterHTTP: &ScouterHTTPConfig{Listen: "127.0.0.1:0", API: httpapi.Config{AllowIPs: []string{"*"}}},
		Pulse:       &PulseConfig{Values: map[string]string{pulse.KeyKafkaServers: "127.0.0.1:1"}}}
	r, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.pulse == nil || r.scouterHTTP == nil || r.http == nil {
		t.Fatal("pulse, servlets or --http not opened", r.pulseError)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	stop := make(chan struct{})
	polled := make(chan int)
	go func() {
		n := 0
		defer func() { polled <- n }()
		for {
			select {
			case <-stop:
				return
			default:
			}
			rec := httptest.NewRecorder()
			r.healthHandler(rec, httptest.NewRequest("GET", "/health", nil))
			if rec.Code != 200 {
				t.Error("health", rec.Code)
				return
			}
			n++
		}
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	err = <-done
	close(stop)
	if n := <-polled; n == 0 {
		t.Fatal("no /health during the run")
	}
	if err != nil {
		t.Fatal(err)
	}
}

// Run's last counter line has its own stage: VisitorStats waits for a
// visitor write in progress, and a stall there is not reported as the
// background stage.
func TestShutdownStagesIncludeFinalStats(t *testing.T) {
	var mu sync.Mutex
	var stages []string
	ShutdownStageHook = func(s string) {
		mu.Lock()
		defer mu.Unlock()
		stages = append(stages, s)
	}
	defer func() { ShutdownStageHook = nil }()
	root := t.TempDir()
	r, err := Open(Config{Directory: filepath.Join(root, "db"), LogDirectory: filepath.Join(root, "logs"), Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	got := strings.Join(stages, ",")
	if !strings.Contains(got, stageBackground+","+stageFinalStats+","+stageHTTP) {
		t.Fatal(got)
	}
}
