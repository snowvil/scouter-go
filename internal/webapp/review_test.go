package webapp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// M1: a stored footprint far in the future never expires; reject it.
func TestBearerFutureFootprintRejected(t *testing.T) {
	f := newFakeCollector(t)
	setupAuth(f)
	cfg := testConfig(f)
	cfg.AuthBearerEnabled = true
	now := time.Unix(1_800_000_000, 0)
	cfg.Clock = func() time.Time { return now }
	s, hs := newTestServer(t, cfg)
	sid := hexa32(int64(s.serverID()))
	token := strings.Repeat("A", 32)
	set := func(footprint int64) {
		f.mu.Lock()
		f.kv[webSessionSpace+"/alice"] = "V1." + strconv.FormatInt(footprint, 10) + "." + token + ".alice"
		f.mu.Unlock()
	}
	auth := map[string]string{"Authorization": "Bearer V1." + token + "." + sid + ".alice"}
	set(now.Unix() + tokenFutureSkew + 1)
	if res := doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/object", "", auth); res.status != 401 {
		t.Fatalf("future footprint accepted: %+v", res)
	}
	// Within the skew it is accepted (fresh webapp: empty token cache).
	set(now.Unix() + tokenFutureSkew)
	_, hs2 := newTestServer(t, cfg)
	if res := doHTTP(t, nil, "GET", hs2.URL+"/scouter/v1/object", "", auth); res.status != 200 {
		t.Fatalf("skewed footprint rejected: %+v", res)
	}
	if bReservedKeySpace("__web_session__") == nil || bReservedKeySpace("___Web_Session_Private___") == nil || bReservedKeySpace("shortner") != nil {
		t.Fatal("bReservedKeySpace")
	}
}

// M5: login always issues a new session id; the cookie is SameSite=Lax.
func TestSessionLoginRotatesID(t *testing.T) {
	f := newFakeCollector(t)
	setupAuth(f)
	cfg := testConfig(f)
	cfg.AuthSessionEnabled = true
	_, hs := newTestServer(t, cfg)
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	u, _ := url.Parse(hs.URL)
	cookie := func() string {
		for _, c := range jar.Cookies(u) {
			if c.Name == sessionCookie {
				return c.Value
			}
		}
		return ""
	}
	res := doHTTP(t, client, "POST", hs.URL+"/scouter/v1/user/login", loginBody, nil)
	if res.status != 200 || !strings.Contains(res.header.Get("Set-Cookie"), "SameSite=Lax") || !strings.Contains(res.header.Get("Set-Cookie"), "HttpOnly") {
		t.Fatalf("cookie: %+v", res.header)
	}
	first := cookie()
	if res = doHTTP(t, client, "POST", hs.URL+"/scouter/v1/user/login", loginBody, nil); res.status != 200 {
		t.Fatal(res.body)
	}
	second := cookie()
	if first == "" || second == "" || first == second {
		t.Fatalf("session id not rotated: %q %q", first, second)
	}
	// The old id is gone.
	if res = doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/object", "", map[string]string{"Cookie": sessionCookie + "=" + first}); res.status != 401 {
		t.Fatalf("old session still valid: %+v", res)
	}
	if res = doHTTP(t, client, "GET", hs.URL+"/scouter/v1/object", "", nil); res.status != 200 {
		t.Fatalf("new session: %+v", res)
	}
	// A planted (unknown) id is never adopted.
	res = doHTTP(t, nil, "POST", hs.URL+"/scouter/v1/user/login", loginBody, map[string]string{"Cookie": sessionCookie + "=planted"})
	if strings.Contains(res.header.Get("Set-Cookie"), "planted") || !strings.Contains(res.header.Get("Set-Cookie"), sessionCookie+"=") {
		t.Fatalf("fixation: %+v", res.header)
	}
}

// M4: GET_TEXT_ANY_TYPE batches stay under the codec collection limit.
func TestLoadTextsBatches(t *testing.T) {
	f := newFakeCollector(t)
	var mu sync.Mutex
	var sizes []int
	f.handle("GET_TEXT_ANY_TYPE", bodyPack, func(r fakeReq) ([]any, error) {
		types, _ := mapList(r.Pack, "type")
		hashes, _ := mapList(r.Pack, "hash")
		mu.Lock()
		sizes = append(sizes, len(hashes))
		mu.Unlock()
		out := make([]any, 0, len(hashes))
		for i, h := range hashes {
			out = append(out, &protocol.TextPack{XType: types[i].Text, Hash: int32(h.Int), Text: fmt.Sprintf("t%d", h.Int)})
		}
		return out, nil
	})
	s, err := New(testConfig(f))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	keys := make([]textKey, 0, 4097)
	for i := 1; i <= 4097; i++ {
		keys = append(keys, textKey{Type: "service", Hash: int32(i)})
	}
	if err := s.loadTexts(context.Background(), keys); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := append([]int(nil), sizes...)
	mu.Unlock()
	if len(got) != 3 || got[0] != 2000 || got[1] != 2000 || got[2] != 97 {
		t.Fatalf("batches %v", got)
	}
	if v := s.text(context.Background(), "20261015", "service", 4097); v != "t4097" {
		t.Fatal(v)
	}
	if v := s.text(context.Background(), "20261015", "service", 1); v != "t1" {
		t.Fatal(v)
	}
}

// L7: a panicking each callback still returns the pool slot.
func TestEachPanicReleasesSlot(t *testing.T) {
	f := newFakeCollector(t)
	f.static("PING", newMap(), newMap())
	cfg := testConfig(f)
	cfg.PoolSize = 1
	s, _ := New(cfg)
	defer s.Close()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic not propagated")
			}
		}()
		s.call(context.Background(), "PING", newMap(), func(Item) error { panic("boom") })
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := s.single(ctx, "PING", newMap()); err != nil {
		t.Fatal("slot leaked after panic:", err)
	}
}

// L8: callers waiting for a running LOGIN honor their own ctx.
func TestLoginWaitHonorsContext(t *testing.T) {
	f := newFakeCollector(t)
	f.static("PING", newMap())
	gate := make(chan struct{})
	f.loginGate = gate
	cfg := testConfig(f)
	s, _ := New(cfg)
	defer s.Close()
	first := make(chan error, 1)
	go func() {
		_, err := s.single(context.Background(), "PING", newMap())
		first <- err
	}()
	time.Sleep(100 * time.Millisecond) // first caller is inside LOGIN
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := s.single(ctx, "PING", newMap())
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("waiter did not honor ctx: %v after %v", err, time.Since(start))
	}
	close(gate)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if f.logins.Load() != 1 {
		t.Fatalf("logins %d", f.logins.Load())
	}
}

// L9: every NoAuth pattern is a registered mux pattern.
func TestNoAuthPatternsRegistered(t *testing.T) {
	f := newFakeCollector(t)
	s, err := New(testConfig(f))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for pattern := range noAuthPatterns {
		method, path, _ := strings.Cut(pattern, " ")
		path = strings.Replace(path, "{key}", "abc", 1)
		req, _ := http.NewRequest(method, path, nil)
		if _, got := s.mux.Handler(req); got != pattern {
			t.Errorf("%s resolves to %q", pattern, got)
		}
	}
	// A NoAuth path with another method is not NoAuth.
	req, _ := http.NewRequest("GET", "/scouter/v1/user/login", nil)
	if _, got := s.mux.Handler(req); noAuthPatterns[got] {
		t.Error("GET login treated as NoAuth")
	}
}

// L10: allow-list comparison normalizes IP text.
func TestIPAllowNormalized(t *testing.T) {
	a := newIPAllow("localhost,127.0.0.1,0:0:0:0:0:0:0:1,10.1.*,2001:db8::1")
	for ip, want := range map[string]bool{
		"::1": true, "0:0:0:0:0:0:0:1": true, "::ffff:127.0.0.1": true, "2001:0db8:0000:0000:0000:0000:0000:0001": true,
		"::ffff:10.1.2.3": true, "10.1.9.9": true, "10.2.0.1": false, "2001:db8::2": false, "localhost": true,
	} {
		if got := a.allowed(ip); got != want {
			t.Errorf("allowed(%q)=%v want %v", ip, got, want)
		}
	}
}

// L12: no authentication on a non-loopback listener logs a warning.
func TestNoAuthNonLoopbackWarning(t *testing.T) {
	f := newFakeCollector(t)
	for addr, want := range map[string]bool{"0.0.0.0:6180": true, ":6180": true, "10.0.0.5:6180": true, "127.0.0.1:6180": false, "localhost:6180": false, "[::1]:6180": false, "": false} {
		var logs []string
		cfg := testConfig(f)
		cfg.ListenAddr = addr
		cfg.Logf = func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) }
		s, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
		warned := len(logs) == 1 && strings.Contains(logs[0], "no authentication")
		if warned != want {
			t.Errorf("%q: logs %v", addr, logs)
		}
	}
	cfg := testConfig(f)
	cfg.ListenAddr = "0.0.0.0:6180"
	cfg.AuthSessionEnabled = true
	var logs []string
	cfg.Logf = func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) }
	s, _ := New(cfg)
	s.Close()
	if len(logs) != 0 {
		t.Errorf("warned with auth enabled: %v", logs)
	}
}
