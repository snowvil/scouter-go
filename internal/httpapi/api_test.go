package httpapi

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/telegraf"
)

type accepted struct {
	pack protocol.Pack
	peer string
}

type fakeEngine struct {
	mu    sync.Mutex
	packs []accepted
	err   error
}

func (f *fakeEngine) AcceptFrom(_ context.Context, p protocol.Pack, peer string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	// Every pack must be encodable like the real engine requires.
	if _, err := protocol.EncodePack(p, protocol.Limits{}); err != nil {
		panic(err)
	}
	f.packs = append(f.packs, accepted{p, peer})
	return nil
}

func (f *fakeEngine) take() []accepted {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.packs
	f.packs = nil
	return out
}

type registrar struct {
	mu       sync.Mutex
	types    map[string]telegraf.ObjectTypeDef
	families map[string]telegraf.FamilyDef
	added    []telegraf.FamilyDef
	edited   []telegraf.ObjectTypeDef
	notified int
	fail     error
}

func newRegistrar() *registrar {
	return &registrar{types: map[string]telegraf.ObjectTypeDef{}, families: map[string]telegraf.FamilyDef{}}
}

func (r *registrar) ObjectType(name string) (telegraf.ObjectTypeDef, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.types[name]
	return t, ok
}
func (r *registrar) HasFamily(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.families[name]
	return ok
}
func (r *registrar) AddFamilyAndObjectType(f telegraf.FamilyDef, t telegraf.ObjectTypeDef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail != nil {
		return r.fail
	}
	r.added = append(r.added, f)
	r.families[f.Name] = f
	t.Counters = f.Counters
	r.types[t.Name] = t
	return nil
}
func (r *registrar) EditObjectType(t telegraf.ObjectTypeDef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.edited = append(r.edited, t)
	return r.fail
}
func (r *registrar) SafelyAddFamily(telegraf.FamilyDef) error           { return nil }
func (r *registrar) SafelyAddObjectType(telegraf.ObjectTypeDef) error   { return nil }
func (r *registrar) AddFamilyCounter(string, telegraf.CounterDef) error { return nil }
func (r *registrar) NotifyClients()                                     { r.mu.Lock(); r.notified++; r.mu.Unlock() }

var testNow = time.Date(2026, 10, 15, 23, 59, 59, 0, time.UTC)

func newAPI(t *testing.T, cfg Config) (*API, *fakeEngine) {
	t.Helper()
	if cfg.Clock == nil {
		cfg.Clock = func() time.Time { return testNow }
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	if cfg.Location == nil {
		cfg.Location = time.UTC
	}
	engine := &fakeEngine{}
	api, err := New(engine, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { api.Close() })
	return api, engine
}

func do(api http.Handler, method, target, body string, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.RemoteAddr = "127.0.0.1:50000"
	for i := 0; i+1 < len(headers); i += 2 {
		if headers[i] == "RemoteAddr" {
			req.RemoteAddr = headers[i+1]
			continue
		}
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	return rec
}

const counterExample = `[
 {"object": {"host": "VM123", "name": "my_server1", "type": "redis", "address": "10.10.10.10"},
  "counters": [{"name": "aof_rewrite_scheduled", "value": 55}, {"name": "client_longest_output_list", "value": 245}, {"name": "used_cpu_user", "value": 4245}]},
 {"object": {"host": "VM123", "name": "my_server2", "type": "redis", "address": "10.10.10.10"},
  "counters": [{"name": "aof_rewrite_scheduled", "value": 35}, {"name": "client_longest_output_list", "value": 65.5}, {"name": "used_cpu_user", "value": 8888}]}
]`

func TestCounterServlet(t *testing.T) {
	api, engine := newAPI(t, Config{})
	rec := do(api, "POST", "/counter", counterExample, "Content-Type", "application/json")
	if rec.Code != 200 || rec.Body.Len() != 0 {
		t.Fatal(rec.Code, rec.Body.String())
	}
	got := engine.take()
	if len(got) != 4 {
		t.Fatalf("%+v", got)
	}
	obj := got[0].pack.(*protocol.ObjectPack)
	perf := got[1].pack.(*protocol.PerfCounterPack)
	if obj.ObjName != "/VM123/my_server1" || obj.ObjHash != telegraf.Hash("/VM123/my_server1") || obj.ObjType != "redis" || obj.Address != "10.10.10.10" || got[0].peer != "10.10.10.10" || obj.Tags.Type != protocol.ValueMap || len(obj.Tags.Entries) != 0 || !obj.Alive || obj.Wakeup != 0 {
		t.Fatalf("%+v", obj)
	}
	if perf.ObjName != "/VM123/my_server1" || perf.TimeType != 1 || perf.Time != testNow.UnixMilli() || len(perf.Data.Entries) != 3 || perf.Data.Entries[0].Value.Type != protocol.ValueFloat || perf.Data.Entries[2].Value.Float != 4245 {
		t.Fatalf("%+v", perf)
	}
	if got[3].pack.(*protocol.PerfCounterPack).Data.Entries[1].Value.Float != 65.5 {
		t.Fatal("double value")
	}

	// A single object; name omitted; host null; no address.
	do(api, "POST", "/counter/anything", `{"object": {"host": null, "type": "t"}, "counters": []}`)
	got = engine.take()
	if len(got) != 2 || got[0].pack.(*protocol.ObjectPack).ObjName != "/null" || got[0].peer != "0.0.0.0" || got[0].pack.(*protocol.ObjectPack).Address != "" {
		t.Fatalf("%+v", got)
	}
	// The first failing element stops the rest; earlier packs stay.
	rec = do(api, "POST", "/counter", `[{"object":{"host":"a"}}, 5, {"object":{"host":"b"},"counters":[{"name":"x","value":"bad"}]}, {"object":{"host":"c"}}]`)
	got = engine.take()
	if rec.Code != 200 || len(got) != 2 || got[1].pack.(*protocol.ObjectPack).ObjName != "/b" {
		t.Fatalf("%d %+v", rec.Code, got)
	}
	do(api, "POST", "/counter", `[{"object":{"host":"a","address":"300.1.1.1"}},{"object":{"host":"b"}}]`)
	if got = engine.take(); len(got) != 0 {
		t.Fatalf("octet out of range must stop processing: %+v", got)
	}
	for _, bad := range []string{`{`, `[1] x`, `{"object":{"host":"a"},"counters":[{"name":"x","value":99999999999999999999}]}`, `"str"`} {
		if rec := do(api, "POST", "/counter", bad); rec.Code != 200 || len(engine.take()) != 0 {
			t.Fatalf("%q: %d", bad, rec.Code)
		}
	}
	s := api.Stats()
	if s.CounterParseErrors != 4 || s.CounterElementErrors != 2 || s.CounterPacks != 8 {
		t.Fatalf("%+v", s)
	}
	engine.err = errors.New("full")
	do(api, "POST", "/counter", `{"object":{"host":"a"}}`)
	if api.Stats().CounterAcceptErrors != 1 {
		t.Fatal(api.Stats())
	}
}

func TestRegisterServlet(t *testing.T) {
	reg := newRegistrar()
	api, _ := newAPI(t, Config{Registrar: reg})
	body := `{"object":{"type":"redis","display":"Redis"},"counters":[{"name":"a","unit":"cnt","display":"A","total":true,"all":true},{"name":"b","unit":"cnt"},{"name":"c","unit":"cnt","total":false}]}`
	if rec := do(api, "POST", "/register", body); rec.Code != http.StatusCreated || rec.Body.Len() != 0 {
		t.Fatal(rec.Code)
	}
	f := reg.added[0]
	typ := reg.types["redis"]
	if f.Name != "redis" || f.Master != "a" || len(f.Counters) != 3 || f.Counters[1].DisplayName != "b" || !f.Counters[1].Total || f.Counters[2].Total || typ.DisplayName != "Redis" || typ.Family != "redis" || reg.notified != 1 {
		t.Fatalf("%+v %+v", f, typ)
	}
	// Existing type: known counters are skipped, new ones edited in.
	if rec := do(api, "POST", "/register", `{"object":{"type":"redis"},"counters":[{"name":"a"},{"name":"d","unit":"ms"}]}`); rec.Code != 201 {
		t.Fatal(rec.Code)
	}
	e := reg.edited[0]
	if e.Name != "redis" || e.DisplayName != "Redis" || e.Family != "redis" || len(e.Counters) != 1 || e.Counters[0].Name != "d" || e.Counters[0].Unit != "ms" {
		t.Fatalf("%+v", e)
	}
	// A family of the same name already exists: random suffix.
	reg.families["x"] = telegraf.FamilyDef{Name: "x"}
	do(api, "POST", "/register", `{"object":{"type":"x"},"counters":[{"name":"a"}]}`)
	if name := reg.added[1].Name; !strings.HasPrefix(name, "x.") || reg.types["x"].Family != name {
		t.Fatal(name)
	}
	for _, bad := range []string{`[]`, `{"object":{"type":"y"}}`, `{"counters":[]}`, `{"object":{"type":"y"},"counters":[{"name":"a","total":"yes"}]}`, `{"object":{"type":"y"},"counters":[1]}`, `{"object":{},"counters":[]}`, `{`} {
		if rec := do(api, "POST", "/register", bad); rec.Code != 500 {
			t.Errorf("%s: %d", bad, rec.Code)
		}
	}
	reg.fail = errors.New("write failed")
	if rec := do(api, "POST", "/register", `{"object":{"type":"z"},"counters":[]}`); rec.Code != 500 {
		t.Fatal(rec.Code)
	}
	noReg, _ := newAPI(t, Config{})
	if rec := do(noReg, "POST", "/register", body); rec.Code != 500 {
		t.Fatal("missing registrar must not be an empty success", rec.Code)
	}
}

func TestTelegrafServlet(t *testing.T) {
	f, _, err := telegraf.ParseFile([]byte(`<TelegrafConfig><measurements><measurement><measurementName>m</measurementName><objFamilyBase>F</objFamilyBase><objTypeBase>T</objTypeBase>
<counterMappings><counterMapping><tgFieldName>v</tgFieldName><counterName>v</counterName><normalizeSeconds>0</normalizeSeconds></counterMapping></counterMappings></measurement></measurements></TelegrafConfig>`))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := telegraf.DefaultConfig().ApplyFile(f, testNow)
	if err != nil {
		t.Fatal(err)
	}
	api, engine := newAPI(t, Config{Telegraf: cfg})
	rec := do(api, "POST", "/telegraf/metric", "m,host=h v=1.5 1\nm,host=h2 v=2i 1\n", "Content-Type", "text/plain; charset=utf-8", "X-Forwarded-For", "10.9.8.7, 1.1.1.1", "RemoteAddr", "192.0.2.1:1")
	if rec.Code != 200 || rec.Body.Len() != 0 {
		t.Fatal(rec.Code)
	}
	got := engine.take()
	if len(got) != 4 || got[0].pack.(*protocol.ObjectPack).Address != "10.9.8.7" || got[0].peer != "10.9.8.7" || got[0].pack.(*protocol.ObjectPack).ObjType != "T" {
		t.Fatalf("%+v", got)
	}
	if v := got[1].pack.(*protocol.PerfCounterPack).Data.Entries[0].Value; v.Type != protocol.ValueFloat || v.Float != 1.5 {
		t.Fatalf("%+v", v)
	}
	disabled := *cfg
	disabled.Enabled = false
	api.SetTelegrafConfig(&disabled)
	if rec := do(api, "POST", "/telegraf", strings.Repeat("x", DefaultMaxBodyBytes+1)); rec.Code != 200 || len(engine.take()) != 0 {
		t.Fatal("a disabled input answers 200 without reading the body", rec.Code)
	}
	if api.Stats().Telegraf.DisabledRequests != 1 {
		t.Fatal(api.Stats().Telegraf)
	}
}

func TestLimitsAndRouting(t *testing.T) {
	api, _ := newAPI(t, Config{MaxBodyBytes: 16, CheckAllowIPs: true})
	cases := []struct {
		method, path, body string
		headers            []string
		code               int
	}{
		{"GET", "/counter", "", nil, 405},
		{"PUT", "/register/x", "", nil, 405},
		{"OPTIONS", "/telegraf", "", nil, 200},
		{"POST", "/", "", nil, 404},
		{"POST", "/counters", "", nil, 404},
		{"POST", "/scouter/v1/info", "", nil, 404},
		{"POST", "/counter", strings.Repeat("x", 17), nil, 413},
		{"POST", "/counter", "{}", []string{"Content-Encoding", "gzip"}, 415},
		{"POST", "/counter", "{}", []string{"Content-Type", "application/json; charset=euc-kr"}, 415},
		{"POST", "/counter", "{}", []string{"Content-Encoding", "identity"}, 200},
		{"POST", "/counter", "{}", []string{"RemoteAddr", "10.0.0.1:1"}, 403},
		{"POST", "/counter", "{}", []string{"RemoteAddr", "10.0.0.1:1", "X-Forwarded-For", "127.0.0.1"}, 403},
		{"POST", "/counter", "{}", []string{"RemoteAddr", "[::1]:1"}, 200},
	}
	for _, c := range cases {
		if rec := do(api, c.method, c.path, c.body, c.headers...); rec.Code != c.code {
			t.Errorf("%s %s %v: %d want %d", c.method, c.path, c.headers, rec.Code, c.code)
		} else if c.code == 405 && rec.Header().Get("Allow") != "POST, OPTIONS" {
			t.Errorf("Allow header %q", rec.Header().Get("Allow"))
		}
	}
	s := api.Stats()
	if s.Denied != 2 || s.TooLarge != 1 || s.UnsupportedMedia != 2 || s.MethodNotAllowed != 2 || s.NotFound != 3 {
		t.Fatalf("%+v", s)
	}
	api.Close()
	if rec := do(api, "POST", "/counter", "{}"); rec.Code != 503 {
		t.Fatal(rec.Code)
	}
}

func TestAllowIPs(t *testing.T) {
	open, _ := newAPI(t, Config{AllowIPs: []string{"10.1.1.1"}})
	if rec := do(open, "POST", "/counter", "{}", "RemoteAddr", "203.0.113.9:1"); rec.Code != 200 {
		t.Fatal("default must keep the original servlets open", rec.Code)
	}
	glob, _ := newAPI(t, Config{CheckAllowIPs: true, AllowIPs: ParseAllowIPs("10.0.*,192.168.1.5")})
	header, _ := newAPI(t, Config{CheckAllowIPs: true, AllowIPs: []string{"10.1.1.1"}, AuthIPHeader: "X-Real-IP"})
	cases := []struct {
		api     *API
		remote  string
		headers []string
		code    int
	}{
		{glob, "10.0.3.4:1", nil, 200},
		{glob, "192.168.1.5:1", nil, 200},
		{glob, "192.168.1.50:1", nil, 403},
		{glob, "127.0.0.1:1", nil, 403},
		{header, "127.0.0.1:1", []string{"X-Real-IP", "10.1.1.1"}, 200},
		{header, "127.0.0.1:1", []string{"X-Real-IP", " 10.1.1.1 , 2.2.2.2"}, 200},
		{header, "10.1.1.1:1", []string{"X-Real-IP", "10.1.1.2"}, 403},
		{header, "10.1.1.1:1", nil, 200},
	}
	for _, c := range cases {
		h := append([]string{"RemoteAddr", c.remote}, c.headers...)
		if rec := do(c.api, "POST", "/counter", "{}", h...); rec.Code != c.code {
			t.Errorf("%s %v: %d want %d", c.remote, c.headers, rec.Code, c.code)
		}
	}
	if got := ParseAllowIPs("a,,b,,"); len(got) != 3 || got[1] != "" {
		t.Fatal(got)
	}
}

func TestStrMatch(t *testing.T) {
	cases := []struct {
		pattern, target string
		want            bool
	}{
		{"*", "x", true}, {"**", "x", true}, {"*", "", false},
		{"a", "a", true}, {"a", "ab", false},
		{"10.0.*", "10.0.1.1", true}, {"10.0.*", "10.1.0.1", false},
		{"*.1", "10.0.0.1", true}, {"*0.0*", "10.0.1", true},
		{"10.*.1", "10.9.9.1", true}, {"10.*.1", "10.9.9.2", false},
		{"10.*.1*", "10.9.1.5", true}, {"*.9.*", "10.9.9.1", true},
		{"*a*b*", "xaxbx", true}, {"*a*b*", "xbxax", false},
		{"*a*b", "xaxb", true}, {"a*b*c", "abxc", false},
	}
	for _, c := range cases {
		if got := newStrMatch(c.pattern).include(c.target); got != c.want {
			t.Errorf("%q include %q = %v", c.pattern, c.target, got)
		}
	}
}

func TestDecodeBody(t *testing.T) {
	latin := []byte{'a', 0xe9}
	cases := []struct {
		ct   string
		body []byte
		want string
		err  bool
	}{
		{"", latin, "aé", false},
		{"text/plain", latin, "aé", false},
		{"application/json", []byte("é"), "é", false},
		{"application/json; charset=ISO-8859-1", latin, "aé", false},
		{"application/json;x=1", []byte("é"), "Ã©", false},
		{"text/plain; charset=UTF-8", []byte{'a', 0xff}, "a�", false},
		{"text/plain; charset=us-ascii", latin, "a�", false},
		{"text/plain; charset=shift_jis", latin, "", true},
	}
	for _, c := range cases {
		got, err := decodeBody(c.ct, c.body)
		if got != c.want || (err != nil) != c.err {
			t.Errorf("%q: %q %v", c.ct, got, err)
		}
	}
}

func TestRemoteAddr(t *testing.T) {
	cases := map[string]string{
		"127.0.0.1:1":         "127.0.0.1",
		"[::1]:1":             "0:0:0:0:0:0:0:1",
		"[::ffff:10.0.0.1]:1": "10.0.0.1",
		"[fe80::1%en0]:1":     "fe80:0:0:0:0:0:0:1%en0",
		"[2001:db8::ab:c]:1":  "2001:db8:0:0:0:0:ab:c",
		"pipe":                "pipe",
	}
	for in, want := range cases {
		if got := remoteAddr(in); got != want {
			t.Errorf("%s: %s want %s", in, got, want)
		}
	}
}

func TestRequestLogFormatAndRotation(t *testing.T) {
	dir := t.TempDir()
	now := testNow
	api, _ := newAPI(t, Config{LogDir: dir, LogKeepDays: 3, Clock: func() time.Time { return now }})
	old := filepath.Join(dir, "http-request-2026_10_01.log")
	keep := filepath.Join(dir, "http-request-2026_10_14.log")
	other := filepath.Join(dir, "other.log")
	for _, p := range []string{old, keep, other} {
		if err := os.WriteFile(p, []byte("x\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	os.Chtimes(old, now.AddDate(0, 0, -10), now.AddDate(0, 0, -10))
	os.Chtimes(keep, now.AddDate(0, 0, -1), now.AddDate(0, 0, -1))
	os.Chtimes(other, now.AddDate(0, 0, -10), now.AddDate(0, 0, -10))
	do(api, "POST", "/counter?x=1", "{}", "User-Agent", "Telegraf/1.30", "Referer", "")
	now = now.Add(2 * time.Second) // next day
	do(api, "GET", "/register", "")
	first, err := os.ReadFile(filepath.Join(dir, "http-request-2026_10_15.log"))
	if err != nil {
		t.Fatal(err)
	}
	if want := `127.0.0.1 - - [15/Oct/2026:23:59:59 +0000] "POST /counter?x=1 HTTP/1.1" 200 0 "" "Telegraf/1.30"` + "\n"; string(first) != want {
		t.Fatalf("got  %q\nwant %q", first, want)
	}
	second, err := os.ReadFile(filepath.Join(dir, "http-request-2026_10_16.log"))
	if err != nil || !strings.HasPrefix(string(second), `127.0.0.1 - - [16/Oct/2026:00:00:01 +0000] "GET /register HTTP/1.1" 405 0 "-" "-"`) {
		t.Fatalf("%q %v", second, err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("expired log not removed")
	}
	for _, p := range []string{keep, other} {
		if _, err := os.Stat(p); err != nil {
			t.Fatal(p, err)
		}
	}
	if s := api.Stats(); s.LogLines != 2 || s.LogFailures != 0 || s.LogRemoved != 1 {
		t.Fatalf("%+v", s)
	}
	// A log directory that cannot be created is counted, not fatal.
	blocker := filepath.Join(dir, "file")
	os.WriteFile(blocker, nil, 0o600)
	broken, _ := newAPI(t, Config{LogDir: filepath.Join(blocker, "sub")})
	if rec := do(broken, "POST", "/counter", "{}"); rec.Code != 200 || broken.Stats().LogFailures != 1 {
		t.Fatal(rec.Code, broken.Stats())
	}
}

func TestGzipHandler(t *testing.T) {
	big := strings.Repeat("a", gzipMinSize)
	h := gzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		switch r.URL.Path {
		case "/big":
			io.WriteString(w, big[:10])
			io.WriteString(w, big[10:])
		case "/small":
			io.WriteString(w, "small")
		case "/encoded":
			w.Header().Set("Content-Encoding", "br")
			io.WriteString(w, big)
		}
	}))
	serve := func(method, path, accept string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		if accept != "" {
			req.Header.Set("Accept-Encoding", accept)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	rec := serve("POST", "/big", "deflate, gzip")
	if rec.Code != 202 || rec.Header().Get("Content-Encoding") != "gzip" {
		t.Fatal(rec.Code, rec.Header())
	}
	zr, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := io.ReadAll(zr); string(b) != big {
		t.Fatal("gzip body")
	}
	for _, c := range []struct{ method, path, accept string }{
		{"POST", "/small", "gzip"}, {"POST", "/big", ""}, {"POST", "/big", "gzip;q=0"}, {"OPTIONS", "/big", "gzip"}, {"POST", "/encoded", "gzip"},
	} {
		rec := serve(c.method, c.path, c.accept)
		if rec.Header().Get("Content-Encoding") == "gzip" || rec.Code != 202 {
			t.Errorf("%+v compressed: %v", c, rec.Header())
		}
	}
	if rec := serve("POST", "/small", "gzip"); !bytes.Equal(rec.Body.Bytes(), []byte("small")) {
		t.Fatal(rec.Body.String())
	}
	api, _ := newAPI(t, Config{Gzip: true})
	if rec := do(api, "POST", "/counter", "{}", "Accept-Encoding", "gzip"); rec.Code != 200 || rec.Header().Get("Content-Encoding") != "" {
		t.Fatal("empty servlet answers are not compressed")
	}
}

func TestJavaDouble(t *testing.T) {
	for in, want := range map[float64]string{37.5: "37.5", 1: "1.0", 0.0001: "1.0E-4", 0: "0.0", 12345678: "1.2345678E7"} {
		if got := javaDouble(in); got != want {
			t.Errorf("%v: %s want %s", in, got, want)
		}
	}
}

func TestNewRejectsNilEngine(t *testing.T) {
	if _, err := New(nil, Config{}); err == nil {
		t.Fatal("nil engine")
	}
}
