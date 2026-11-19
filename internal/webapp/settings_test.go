package webapp

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDefaultPoolIdleTimeoutIsJavaDerived(t *testing.T) {
	// applyConfig: net_webapp_tcp_client_pool_timeout defaults to the current
	// net_tcp_client_so_timeout_ms, whose default is 8000.
	if got := DefaultConfig().PoolIdleTimeout; got != 8*time.Second {
		t.Fatal(got)
	}
}

func TestSettingsApplyToTheNextRequest(t *testing.T) {
	f := newFakeCollector(t)
	setupAuth(f)
	s, hs := newTestServer(t, testConfig(f))
	url := hs.URL + "/scouter/v1/object"
	origin := map[string]string{"Origin": "http://ui.example"}
	if res := doHTTP(t, nil, "GET", url, "", origin); res.status != 200 || res.header.Get("Access-Control-Allow-Origin") != "http://ui.example" {
		t.Fatalf("default CORS %+v", res.header)
	}

	// CORS: a fixed origin, then credentials "false" keeps "*" as is, then off.
	live := s.Settings()
	live.CORSAllowOrigin = "http://fixed.example"
	s.SetSettings(live)
	if res := doHTTP(t, nil, "GET", url, "", origin); res.header.Get("Access-Control-Allow-Origin") != "http://fixed.example" {
		t.Fatalf("fixed origin %+v", res.header)
	}
	live.CORSAllowOrigin, live.CORSAllowCredentials = "*", "false"
	s.SetSettings(live)
	if res := doHTTP(t, nil, "GET", url, "", origin); res.header.Get("Access-Control-Allow-Origin") != "*" || res.header.Get("Access-Control-Allow-Credentials") != "false" {
		t.Fatalf("credentials false %+v", res.header)
	}
	live.CORSAllowOrigin = "  "
	s.SetSettings(live)
	if res := doHTTP(t, nil, "GET", url, "", origin); res.header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("blank origin %+v", res.header)
	}

	// IP authentication with a live allow list and header key.
	live.AuthIPEnabled, live.AllowIPs = true, "10.1.*"
	s.SetSettings(live)
	if res := doHTTP(t, nil, "GET", url, "", nil); res.status != 401 {
		t.Fatalf("loopback not allowed: %+v", res)
	}
	live.AuthIPHeaderKey = "X-Forwarded-For"
	s.SetSettings(live)
	if res := doHTTP(t, nil, "GET", url, "", map[string]string{"X-Forwarded-For": "10.1.2.3"}); res.status != 200 {
		t.Fatalf("header ip allowed: %+v", res)
	}
	live.AllowIPs = "127.0.0.1"
	s.SetSettings(live)
	if res := doHTTP(t, nil, "GET", url, "", map[string]string{"X-Forwarded-For": "10.1.2.3"}); res.status != 401 {
		t.Fatalf("new allow list: %+v", res)
	}
	live.AuthIPHeaderKey = ""
	s.SetSettings(live)
	if res := doHTTP(t, nil, "GET", url, "", map[string]string{"X-Forwarded-For": "10.1.2.3"}); res.status != 200 {
		t.Fatalf("remote address allowed: %+v", res)
	}

	// Session and bearer authentication switched on and off.
	live.AuthIPEnabled, live.AuthSessionEnabled = false, true
	s.SetSettings(live)
	if res := doHTTP(t, nil, "GET", url, "", nil); res.status != 401 || !strings.Contains(res.body, "login") {
		t.Fatalf("session required: %+v", res)
	}
	live.AuthSessionEnabled, live.AuthBearerEnabled = false, true
	s.SetSettings(live)
	if res := doHTTP(t, nil, "GET", url, "", nil); res.status != 401 {
		t.Fatalf("bearer required: %+v", res)
	}
	live.AuthBearerEnabled = false
	s.SetSettings(live)
	if res := doHTTP(t, nil, "GET", url, "", nil); res.status != 200 {
		t.Fatalf("no auth: %+v", res)
	}
}

func TestPoolIdleTimeoutIsLive(t *testing.T) {
	f := newFakeCollector(t)
	setupAuth(f)
	c := &clock{t: time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)}
	cfg := testConfig(f)
	cfg.Clock = c.now
	cfg.PoolIdleTimeout = time.Hour
	s, hs := newTestServer(t, cfg)
	url := hs.URL + "/scouter/v1/object"
	get := func() {
		t.Helper()
		if res := doHTTP(t, nil, "GET", url, "", nil); res.status != 200 {
			t.Fatalf("%+v", res)
		}
	}
	get()
	dials := f.dials.Load()
	c.add(10 * time.Second)
	get()
	if f.dials.Load() != dials {
		t.Fatal("idle connection within 1h was not reused")
	}
	live := s.Settings()
	live.PoolIdleTimeout = 5 * time.Second
	s.SetSettings(live)
	c.add(10 * time.Second)
	get()
	if f.dials.Load() != dials+1 {
		t.Fatalf("expired idle connection reused: dials %d -> %d", dials, f.dials.Load())
	}
	// Zero closes every idle connection (Java lastUsed < now - (0 - 1)).
	live.PoolIdleTimeout = 0
	s.SetSettings(live)
	get()
	if f.dials.Load() != dials+2 {
		t.Fatalf("zero timeout reused: dials %d -> %d", dials, f.dials.Load())
	}
}

func TestSetSettingsWarnsWhenAuthTurnsOff(t *testing.T) {
	f := newFakeCollector(t)
	var mu sync.Mutex
	var logs []string
	cfg := testConfig(f)
	cfg.ListenAddr = "0.0.0.0:6180"
	cfg.AuthSessionEnabled = true
	cfg.Logf = func(format string, args ...any) { mu.Lock(); logs = append(logs, format); mu.Unlock() }
	s, _ := newTestServer(t, cfg)
	if len(logs) != 0 {
		t.Fatal(logs)
	}
	live := s.Settings()
	live.AuthSessionEnabled = false
	s.SetSettings(live)
	s.SetSettings(live) // already off: no repeated warning
	mu.Lock()
	defer mu.Unlock()
	if len(logs) != 1 || !strings.Contains(logs[0], "no authentication") {
		t.Fatal(logs)
	}
}

// TestSettingsConcurrentWithRequests runs under -race.
func TestSettingsConcurrentWithRequests(t *testing.T) {
	f := newFakeCollector(t)
	setupAuth(f)
	s, hs := newTestServer(t, testConfig(f))
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			live := s.Settings()
			live.CORSAllowOrigin = []string{"*", "http://a.example"}[i%2]
			live.AllowIPs = []string{"127.0.0.1", "10.*"}[i%2]
			live.PoolIdleTimeout = time.Duration(i) * time.Millisecond
			s.SetSettings(live)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if res := doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/object", "", nil); res.status != 200 {
				t.Errorf("%+v", res)
				return
			}
		}
	}()
	wg.Wait()
}
