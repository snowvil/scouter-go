package webapp

import (
	"net/http"
	"net/http/cookiejar"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func setupAuth(f *fakeCollector) {
	f.handle("OBJECT_LIST_REAL_TIME", bodyNone, func(fakeReq) ([]any, error) { return nil, nil })
	f.handle("GET_XML_COUNTER", bodyNone, func(fakeReq) ([]any, error) { return []any{newMap()}, nil })
	f.handle("CHECK_LOGIN", bodyPack, func(r fakeReq) ([]any, error) {
		ok := mapText(r.Pack, "id") == "alice" && mapText(r.Pack, "pass") == scouterPassword("pw")
		return []any{protocol.Value{Type: protocol.ValueBoolean, Bool: ok}}, nil
	})
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

const loginBody = `{"user":{"id":"alice","password":"pw"}}`

func TestNoAuthByDefault(t *testing.T) {
	f := newFakeCollector(t)
	setupAuth(f)
	_, hs := newTestServer(t, testConfig(f))
	if res := doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/object", "", nil); res.status != 200 {
		t.Fatal(res.body)
	}
}

func TestSessionAuth(t *testing.T) {
	f := newFakeCollector(t)
	setupAuth(f)
	cfg := testConfig(f)
	cfg.AuthSessionEnabled = true
	ck := &clock{t: time.Now()}
	cfg.Clock = ck.now
	cfg.SessionTimeout = time.Hour
	_, hs := newTestServer(t, cfg)
	res := doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/object", "", nil)
	if res.status != 401 || !strings.HasSuffix(res.body, `"resultCode":"401","message":"login required.","result":null}`) || res.header.Get("Set-Cookie") != "" {
		t.Fatalf("unauthenticated: %+v", res)
	}
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	res = doHTTP(t, client, "POST", hs.URL+"/scouter/v1/user/login", `{"user":{"id":"alice","password":"bad"}}`, nil)
	if res.status != 404 || !strings.Contains(res.body, `"message":"id or password is incorrect."`) {
		t.Fatalf("bad login: %+v", res)
	}
	res = doHTTP(t, client, "POST", hs.URL+"/scouter/v1/user/login", `{"user":{}}`, nil)
	if res.status != 400 || !strings.Contains(res.body, "login.arg0.user.id may not be null") {
		t.Fatalf("validation: %+v", res)
	}
	res = doHTTP(t, client, "POST", hs.URL+"/scouter/v1/user/login", loginBody, nil)
	if res.status != 200 || resultOf(t, res.body) != "true" || !strings.Contains(res.header.Get("Set-Cookie"), "JSESSIONID=") {
		t.Fatalf("login: %+v", res)
	}
	if res = doHTTP(t, client, "GET", hs.URL+"/scouter/v1/object", "", nil); res.status != 200 {
		t.Fatalf("session access: %+v", res)
	}
	if r := f.requests("CHECK_LOGIN"); len(r) == 0 || mapText(r[len(r)-1].Pack, "pass") != scouterPassword("pw") {
		t.Fatal("CHECK_LOGIN pass must be the salted sha256")
	}
	ck.add(2 * time.Hour)
	if res = doHTTP(t, client, "GET", hs.URL+"/scouter/v1/object", "", nil); res.status != 401 {
		t.Fatalf("expired session: %+v", res)
	}
	// @NoAuth stays open.
	if res = doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/info/server", "", nil); res.status != 200 {
		t.Fatalf("NoAuth: %+v", res)
	}
}

var bearerRe = regexp.MustCompile(`"bearerToken":"(V1\.([0-9A-F]{32})\.([^.]+)\.alice)"`)

func TestBearerAuth(t *testing.T) {
	f := newFakeCollector(t)
	setupAuth(f)
	cfg := testConfig(f)
	cfg.AuthBearerEnabled = true
	cfg.SessionTimeout = 1000 * time.Second
	ck := &clock{t: time.Unix(1_800_000_000, 0)}
	cfg.Clock = ck.now
	s, hs := newTestServer(t, cfg)
	res := doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/object", "", nil)
	if res.status != 401 || !strings.Contains(res.body, `"message":"authorization token or session is expired."`) {
		t.Fatalf("no header: %+v", res)
	}
	res = doHTTP(t, nil, "POST", hs.URL+"/scouter/v1/user/loginGetToken", loginBody, nil)
	m := bearerRe.FindStringSubmatch(res.body)
	if res.status != 200 || m == nil || !strings.Contains(res.body, `"result":{"success":true,"bearerToken":`) || m[3] != hexa32(int64(s.serverID())) {
		t.Fatalf("token: %+v", res)
	}
	token := m[1]
	stored := f.kvGet(webSessionSpace + "/alice")
	if stored != "V1.1800000000."+m[2]+".alice" {
		t.Fatalf("stored %q", stored)
	}
	if r := f.requests("SET_CUSTOM_KV"); mapInt(r[0].Pack, "ttl") != 1100 {
		t.Fatalf("ttl %+v", r[0].Pack)
	}
	auth := map[string]string{"Authorization": "Bearer " + token}
	if res = doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/object", "", auth); res.status != 200 {
		t.Fatalf("bearer: %+v", res)
	}
	// Malformed tokens are 401 (Java W1: 500).
	for _, bad := range []string{"Bearer x", "Bearer V1.tok", "Bearer V1." + m[2] + ".!!.alice", "Bearer V1." + m[2] + "." + m[3] + ".mallory"} {
		if res = doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/object", "", map[string]string{"Authorization": bad}); res.status != 401 {
			t.Fatalf("%q: %+v", bad, res)
		}
	}
	// Touch after 10%: footprint renewed in KV.
	ck.add(101 * time.Second)
	if res = doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/object", "", auth); res.status != 200 {
		t.Fatal(res.body)
	}
	if stored := f.kvGet(webSessionSpace + "/alice"); stored != "V1.1800000101."+m[2]+".alice" {
		t.Fatalf("touched %q", stored)
	}
	// A new webapp instance (empty cache) validates from the KV store.
	cfg2 := cfg
	_, hs2 := newTestServer(t, cfg2)
	if res = doHTTP(t, nil, "GET", hs2.URL+"/scouter/v1/object", "", auth); res.status != 200 {
		t.Fatalf("from KV: %+v", res)
	}
	// Unknown user in KV: 401 (Java W11: 500).
	other := "Bearer V1." + m[2] + "." + m[3] + ".bob"
	if res = doHTTP(t, nil, "GET", hs2.URL+"/scouter/v1/object", "", map[string]string{"Authorization": other}); res.status != 401 {
		t.Fatalf("bob: %+v", res)
	}
	// Expired after timeout*1.1 without touch.
	ck.add(1101 * time.Second)
	if res = doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/object", "", auth); res.status != 401 {
		t.Fatalf("expired: %+v", res)
	}
	// A second token merges with the stored list, dropping expired entries.
	res = doHTTP(t, nil, "POST", hs.URL+"/scouter/v1/user/loginGetToken", loginBody, nil)
	m2 := bearerRe.FindStringSubmatch(res.body)
	if m2 == nil {
		t.Fatal(res.body)
	}
	want := "V1." + strconv.FormatInt(ck.now().Unix(), 10) + "." + m2[2] + ".alice"
	if stored := f.kvGet(webSessionSpace + "/alice"); stored != want {
		t.Fatalf("merged %q want %q", stored, want)
	}
}

func TestIPAuthAndAsymmetry(t *testing.T) {
	f := newFakeCollector(t)
	setupAuth(f)
	cfg := testConfig(f)
	cfg.AuthIPEnabled = true
	cfg.AllowIPs = "10.1.*,127.0.0.1"
	cfg.AuthIPHeaderKey = "X-Forwarded-For"
	_, hs := newTestServer(t, cfg)
	if res := doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/object", "", nil); res.status != 200 {
		t.Fatalf("loopback allowed: %+v", res)
	}
	if res := doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/object", "", map[string]string{"X-Forwarded-For": "10.1.2.3, 9.9.9.9"}); res.status != 200 {
		t.Fatalf("glob allowed: %+v", res)
	}
	if res := doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/object", "", map[string]string{"X-Forwarded-For": "9.9.9.9"}); res.status != 401 ||
		!strings.Contains(res.body, "authorization token or session is expired.") {
		t.Fatalf("violation: %+v", res)
	}

	// IP violation + valid bearer passes; IP violation + valid session is 401.
	cfg.AuthBearerEnabled, cfg.AuthSessionEnabled = true, true
	_, hs2 := newTestServer(t, cfg)
	res := doHTTP(t, nil, "POST", hs2.URL+"/scouter/v1/user/loginGetToken", loginBody, nil)
	m := bearerRe.FindStringSubmatch(res.body)
	if m == nil {
		t.Fatal(res.body)
	}
	bad := map[string]string{"X-Forwarded-For": "9.9.9.9", "Authorization": "bearer " + m[1]}
	if res = doHTTP(t, nil, "GET", hs2.URL+"/scouter/v1/object", "", bad); res.status != 200 {
		t.Fatalf("ip violation + bearer: %+v", res)
	}
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	if res = doHTTP(t, client, "POST", hs2.URL+"/scouter/v1/user/login", loginBody, nil); res.status != 200 {
		t.Fatal(res.body)
	}
	if res = doHTTP(t, client, "GET", hs2.URL+"/scouter/v1/object", "", map[string]string{"X-Forwarded-For": "9.9.9.9"}); res.status != 401 {
		t.Fatalf("ip violation + session: %+v", res)
	}
	if res = doHTTP(t, client, "GET", hs2.URL+"/scouter/v1/object", "", nil); res.status != 200 {
		t.Fatalf("allowed ip short-circuits: %+v", res)
	}
}

func TestSessionStoreBound(t *testing.T) {
	st := newSessionStore(2, time.Hour)
	now := time.Now()
	for i := 0; i < 5; i++ {
		rec := &statusRecorder{header: http.Header{}}
		req, _ := http.NewRequest("POST", "/", nil)
		if err := st.login(httpWriter{rec}, req, "u", now); err != nil {
			t.Fatal(err)
		}
	}
	if st.order.Len() != 2 || len(st.byID) != 2 {
		t.Fatalf("sessions %d", st.order.Len())
	}
	tc := newTokenCache(2)
	for i := 0; i < 5; i++ {
		tc.put(userToken{token: strconv.Itoa(i)}, false)
	}
	if _, ok := tc.get("0"); ok || tc.order.Len() != 2 {
		t.Fatal("token cache bound")
	}
}

type httpWriter struct{ *statusRecorder }
