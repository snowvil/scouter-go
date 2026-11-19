package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/storage"
)

type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (l *logSink) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logSink) contains(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, s) {
			return true
		}
	}
	return false
}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func noEnv(string) (string, bool) { return "", false }

func openTest(t *testing.T, text string, opts Options) (*Store, string, *logSink) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "conf", "scouter.conf")
	if text != "" {
		os.MkdirAll(filepath.Dir(path), 0o755)
		if err := os.WriteFile(path, []byte(text), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	sink := &logSink{}
	if opts.Logf == nil {
		opts.Logf = sink.logf
	}
	if opts.Lookup == nil {
		opts.Lookup = noEnv
	}
	if opts.HostName == "" {
		opts.HostName = "test-host"
	}
	s, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	return s, path, sink
}

// rewrite changes the file so its stat differs (size and mtime).
func rewrite(t *testing.T, path, text string, mtime time.Time) {
	t.Helper()
	if err := os.WriteFile(path, []byte(text), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

func TestKeysTable(t *testing.T) {
	seen := map[string]bool{}
	for _, k := range Keys {
		if k.Name == "" || seen[k.Name] {
			t.Fatalf("duplicate or empty key %q", k.Name)
		}
		seen[k.Name] = true
		if k.Type < ValueText || k.Type > ValueCSVPairs {
			t.Fatalf("%s: type %d", k.Name, k.Type)
		}
		if (k.Type == ValueCSVPairs) != (k.TypeDesc != nil) {
			t.Fatalf("%s: TypeDesc only for COMMA_COLON_SEPARATED_VALUE", k.Name)
		}
		if k.TypeDesc != nil && len(k.TypeDesc.Strings) != len(k.TypeDesc.Booleans) {
			t.Fatalf("%s: strings/booleans length", k.Name)
		}
		wantType := map[Kind]ValueType{KindInt: ValueNum, KindLong: ValueNum, KindBool: ValueBool}[k.Kind]
		if wantType != 0 && k.Type != wantType {
			t.Fatalf("%s: reflection type %d, got %d", k.Name, wantType, k.Type)
		}
	}
	// 136 settings: the 142 public non-static Configure.java fields minus
	// property, telegrafInputConfigMap[Deprecated], telegrafOriginalConfig,
	// allowIpExact and allowIpMatch (CONTRACT.md).
	if len(Keys) != 136 {
		t.Fatalf("Keys has %d entries", len(Keys))
	}
	for _, k := range []string{"server_id", "db_dir", "mgr_purge_enabled", "net_http_port", "input_telegraf_$measurement$_host_mappings", "visitor_hourly_count_enabled"} {
		if !seen[k] {
			t.Fatalf("missing %s", k)
		}
	}
	if Keys[0].Name != "server_id" || Keys[len(Keys)-1].Name != "visitor_hourly_count_enabled" {
		t.Fatal("declaration order")
	}
	if k, _ := Lookup("mgr_purge_daily_text_days"); k.Default != "140" {
		t.Fatalf("field default %q", k.Default)
	}
}

func TestDefaultsWithoutFile(t *testing.T) {
	s, _, _ := openTest(t, "", Options{})
	snap := s.Current()
	if snap.Exists {
		t.Fatal("missing file reported as existing")
	}
	rows := snap.Properties()
	if len(rows) != len(Keys) {
		t.Fatalf("%d rows", len(rows))
	}
	got := map[string]Property{}
	for _, p := range rows {
		got[p.Key] = p
	}
	for key, want := range map[string][2]string{
		// value (applyConfig default), default (field initializer)
		"server_id":                          {"test-host", "test-host"},
		"net_udp_listen_port":                {"6100", "6100"},
		"object_zipkin_deadtime_ms":          {"300000", "180000"},
		"net_webapp_tcp_client_pool_size":    {"12", "30"},
		"net_webapp_tcp_client_pool_timeout": {"8000", "60000"},
		"net_webapp_tcp_client_so_timeout":   {"30000", "30000"},
		"net_http_api_auth_ip_header_key":    {"", ""},
		"mgr_purge_counter_keep_days":        {"70", "70"},
		"mgr_purge_daily_text_days":          {"140", "140"},
		"mgr_log_ignore_ids":                 {"{}", "{}"},
		"_compress_read_cache_expired_ms":    {"60000", "60000"},
		"geoip_data_city_file":               {"./conf/GeoLiteCity.dat", "./conf/GeoLiteCity.dat"},
	} {
		if got[key].Value != want[0] || got[key].Default != want[1] {
			t.Fatalf("%s: value %q default %q, want %q", key, got[key].Value, got[key].Default, want)
		}
	}
	srv := snap.Server()
	if srv.DBDir != "./database" || srv.UDPListenIP != "0.0.0.0" || srv.TCPListenPort != 6100 || srv.HTTPPort != 6180 || !srv.PurgeEnabled || len(srv.Purge) != 0 || srv.ObjectDeadTime != 8*time.Second || srv.LDAP.Auth != "simple" || srv.UDPReceiveBuffer != 4<<20 {
		t.Fatalf("defaults %+v", srv)
	}
	text, exists, err := s.LoadText()
	if err != nil || exists || text != "" {
		t.Fatalf("LoadText of missing file: %q %v %v", text, exists, err)
	}
}

func TestTypedValuesFollowApplyConfig(t *testing.T) {
	env := map[string]string{"SCOUTER_DB": "/data/scouter", "server_id": "  from-env  "}
	s, _, sink := openTest(t, strings.Join([]string{
		"db_dir=${SCOUTER_DB}/database",
		"net_udp_listen_port=not-a-number",
		"net_tcp_listen_port=+6201",
		"compress_xlog_enabled=TRUE",
		"compress_profile_enabled=yes",
		"mgr_purge_keep_days=5",
		"mgr_purge_xlog_keep_days=0",
		"mgr_log_ignore_ids= b , a ,,b",
		"net_tcp_client_so_timeout_ms=9000",
		"account_use_ldap=true",
		"account_ldap_auth\\==strong",
		"ext_plugin_pulse_kafka_servers=k1:9092",
		"some_typo_key=1",
		"input_telegraf_$cpu$_enabled=false",
	}, "\n"), Options{Lookup: func(k string) (string, bool) { v, ok := env[k]; return v, ok }, ExtraKeys: []string{"ext_plugin_pulse_kafka_servers"}})
	snap := s.Current()
	srv := snap.Server()
	if srv.DBDir != "/data/scouter/database" || srv.UDPListenPort != 6100 || srv.TCPListenPort != 6201 || !srv.CompressXLog || srv.CompressProfile {
		t.Fatalf("typed %+v", srv)
	}
	if srv.ServerID != "from-env" {
		t.Fatalf("environment fallback for a missing key: %q", srv.ServerID)
	}
	if !srv.LDAP.Enabled || srv.LDAP.Auth != "strong" {
		t.Fatalf("ldap %+v", srv.LDAP)
	}
	// mgr_purge_* derivation equals storage.JavaRetentionConfig on the set keys.
	r := storage.JavaRetentionConfig(srv.Purge)
	if !reflect.DeepEqual(srv.Purge, map[string]int{"mgr_purge_keep_days": 5, "mgr_purge_xlog_keep_days": 0}) {
		t.Fatalf("purge set %v", srv.Purge)
	}
	for key, want := range map[string]int{"mgr_purge_counter_keep_days": r.CounterDays, "mgr_purge_xlog_keep_days": r.XLogDays, "mgr_purge_daily_text_days": r.TextDays, "mgr_purge_profile_keep_days": r.ProfileDays} {
		if v, _ := snap.Value(key); v != fmt.Sprint(want) {
			t.Fatalf("%s: %s, storage derivation %d", key, v, want)
		}
	}
	if v, _ := snap.Value("mgr_log_ignore_ids"); v != "{b, a}" {
		t.Fatalf("string set %q", v)
	}
	if v, _ := snap.Value("net_webapp_tcp_client_pool_timeout"); v != "9000" {
		t.Fatalf("derived default %q", v)
	}
	if got := snap.Unknown(); !reflect.DeepEqual(got, []string{"some_typo_key"}) {
		t.Fatalf("unknown %v", got)
	}
	if !sink.contains("unknown key some_typo_key") {
		t.Fatal("unknown key not logged")
	}
}

func TestReloadGateMtimeAndRestartKeys(t *testing.T) {
	clk := &clock{now: time.Unix(1_700_000_000, 0)}
	var changes []Change
	s, path, sink := openTest(t, "db_dir=/a\nlog_keep_days=31\next_plugin_pulse_enable=true\n", Options{
		Now: clk.Now, RestartKeys: []string{"ext_plugin_pulse_enable"},
		OnChange: func(c Change) { changes = append(changes, c) },
	})
	base := time.Now().Add(-time.Hour)
	rewrite(t, path, "db_dir=/b\nlog_keep_days=7\next_plugin_pulse_enable=false\nnew_unknown=1\n", base)

	// Within the 3 s gate a non-forced reload does nothing.
	clk.add(time.Second)
	if c, err := s.Reload(false); err != nil || !c.Empty() {
		t.Fatalf("gated reload %+v %v", c, err)
	}
	if v, _ := s.Current().Value("log_keep_days"); v != "31" {
		t.Fatal("gate let the reload through")
	}
	clk.add(3 * time.Second)
	c, err := s.Reload(false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Applied, []string{"log_keep_days", "new_unknown"}) || !reflect.DeepEqual(c.Ignored, []string{"db_dir", "ext_plugin_pulse_enable"}) {
		t.Fatalf("change %+v", c)
	}
	snap := s.Current()
	if v, _ := snap.Value("log_keep_days"); v != "7" {
		t.Fatalf("reloadable key %s", v)
	}
	if v, _ := snap.Value("db_dir"); v != "/a" {
		t.Fatalf("restart key must keep the running value, got %s", v)
	}
	if v, _ := snap.Get("ext_plugin_pulse_enable"); v != "true" {
		t.Fatalf("restart extra key %s", v)
	}
	if !sink.contains("db_dir changed") || !sink.contains("restart required") || !sink.contains("unknown key new_unknown") {
		t.Fatalf("logs %v", sink.lines)
	}
	if len(changes) != 1 {
		t.Fatalf("OnChange calls %d", len(changes))
	}
	// Unchanged stat: no re-read even when forced.
	if c, _ := s.Reload(true); !c.Empty() {
		t.Fatalf("unchanged file reloaded %+v", c)
	}
	// Same size, different mtime, same content: re-read but no change.
	if err := os.Chtimes(path, base.Add(time.Minute), base.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if c, _ := s.Reload(true); !c.Empty() {
		t.Fatalf("identical content reported %+v", c)
	}
	// A deleted file keeps the loaded values (Java canRead false).
	os.Remove(path)
	if c, err := s.Reload(true); err != nil || !c.Empty() {
		t.Fatalf("missing file %+v %v", c, err)
	}
	if v, _ := s.Current().Value("log_keep_days"); v != "7" {
		t.Fatal("deleted file reset the values")
	}
	// A key removed from the file returns to its default on reload.
	rewrite(t, path, "db_dir=/a\n", base.Add(2*time.Minute))
	c, _ = s.Reload(true)
	if !reflect.DeepEqual(c.Applied, []string{"log_keep_days", "new_unknown"}) || !reflect.DeepEqual(c.Ignored, []string{"ext_plugin_pulse_enable"}) {
		t.Fatalf("removal %+v", c)
	}
	if v, _ := s.Current().Value("log_keep_days"); v != "31" {
		t.Fatalf("removed key value %s", v)
	}
}

func TestReloadMalformedAndOversize(t *testing.T) {
	s, path, sink := openTest(t, "a=1\n", Options{})
	rewrite(t, path, "log_keep_days=9\nbad=\\u00zz\nafter=1\n", time.Now().Add(-time.Hour))
	c, err := s.Reload(true)
	if !errors.Is(err, ErrMalformedUnicode) || !reflect.DeepEqual(c.Applied, []string{"a", "log_keep_days"}) {
		t.Fatalf("partial apply %+v %v", c, err)
	}
	if !sink.contains("malformed") {
		t.Fatal("parse error not logged")
	}
	rewrite(t, path, strings.Repeat("#", MaxFileBytes+1), time.Now().Add(-2*time.Hour))
	if _, err := s.Reload(true); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversize reload %v", err)
	}
	if v, _ := s.Current().Value("log_keep_days"); v != "9" {
		t.Fatal("oversize file replaced the values")
	}
	if _, err := Open(path, Options{Lookup: noEnv, Logf: sink.logf}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Open of an oversize file: %v", err)
	}
}

func TestWatchDetectsChangesAndStops(t *testing.T) {
	changed := make(chan Change, 4)
	s, path, _ := openTest(t, "log_keep_days=1\n", Options{Interval: 5 * time.Millisecond, OnChange: func(c Change) { changed <- c }})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Watch(ctx) }()
	rewrite(t, path, "log_keep_days=22\n", time.Now().Add(-time.Hour))
	select {
	case c := <-changed:
		if !reflect.DeepEqual(c.Applied, []string{"log_keep_days"}) {
			t.Fatalf("change %+v", c)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not see the change")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Watch did not stop")
	}
}

func TestSaveTextAtomicAndBounded(t *testing.T) {
	s, path, _ := openTest(t, "a=1\n", Options{})
	if err := s.SaveText("# saved by client\nlog_keep_days=3\n"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("mode %v %v", info.Mode(), err)
	}
	text, exists, err := s.LoadText()
	if err != nil || !exists || text != "# saved by client\nlog_keep_days=3\n" {
		t.Fatalf("LoadText %q %v %v", text, exists, err)
	}
	c, _ := s.Reload(true)
	if !reflect.DeepEqual(c.Applied, []string{"a", "log_keep_days"}) {
		t.Fatalf("reload after save %+v", c)
	}
	if err := s.SaveText(strings.Repeat("x", MaxFileBytes+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversize save %v", err)
	}
	if text, _, _ := s.LoadText(); !strings.HasPrefix(text, "# saved") {
		t.Fatal("rejected save modified the file")
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatalf("temporary files left: %v", entries)
	}
	// A missing parent directory is created (Java mkdirs).
	fresh, err := Open(filepath.Join(t.TempDir(), "new", "dir", "scouter.conf"), Options{Lookup: noEnv, Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.SaveText("x=1\n"); err != nil {
		t.Fatal(err)
	}
	// A symlinked file is replaced at its target and stays a symlink.
	target := filepath.Join(t.TempDir(), "real.conf")
	os.WriteFile(target, []byte("old=1\n"), 0o600)
	link := filepath.Join(t.TempDir(), "scouter.conf")
	if err := os.Symlink(target, link); err != nil {
		t.Skip("symlink unsupported:", err)
	}
	ls, err := Open(link, Options{Lookup: noEnv, Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ls.SaveText("new=1\n"); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Lstat(link); fi.Mode()&os.ModeSymlink == 0 {
		t.Fatal("symlink replaced by a regular file")
	}
	if b, _ := os.ReadFile(target); string(b) != "new=1\n" {
		t.Fatalf("target %q", b)
	}
}

// Readers never observe a partial file while SaveText and Reload run.
func TestSaveTextConcurrentReaders(t *testing.T) {
	s, _, _ := openTest(t, "a=0\n", Options{})
	texts := []string{"a=" + strings.Repeat("1", 4096) + "\n", "a=" + strings.Repeat("2", 8192) + "\n"}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				text, _, err := s.LoadText()
				if err != nil || (text != texts[0] && text != texts[1] && text != "a=0\n") {
					errs <- fmt.Errorf("partial read %d bytes: %v", len(text), err)
					return
				}
				s.Current().Server()
			}
		}()
	}
	for i := 0; i < 50; i++ {
		if err := s.SaveText(texts[i%2]); err != nil {
			t.Fatal(err)
		}
		s.Reload(true)
	}
	cancel()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestKnownKeys(t *testing.T) {
	s, _, _ := openTest(t, "", Options{ExtraKeys: []string{"extra_key"}})
	for key, want := range map[string]bool{
		"db_dir": true, "account_ldap_basedn": true, "account_ldap_auth=": true, "extra_key": true,
		"input_telegraf_$redis_keyspace$_counter_mappings": true, "input_telegraf_$nodollar": false, "nope": false,
	} {
		if s.Known(key) != want {
			t.Fatalf("Known(%q) != %v", key, want)
		}
	}
}

// scouter.conf and scouter-telegraf.xml are re-read together: every snapshot
// holds the current XML text with the current deprecated conf keys
// (decision against the Java D13 ordering defect, CONTRACT.md §5.1).
func TestTelegrafRecomposedWithConf(t *testing.T) {
	dir := t.TempDir()
	tg := filepath.Join(dir, "scouter-telegraf.xml")
	var changes []Change
	s, path, _ := openTest(t, "input_telegraf_enabled=false\ninput_telegraf_$cpu$_enabled=true\ntelegraf_object_deadtime_ms=1000\nlog_keep_days=1\n", Options{TelegrafPath: tg, OnChange: func(c Change) { changes = append(changes, c) }})
	snap := s.Current()
	if f := snap.Telegraf(); f.Exists || f.Path != tg || f.Text != "" {
		t.Fatalf("missing xml %+v", f)
	}
	want := map[string]string{"input_telegraf_enabled": "false", "input_telegraf_$cpu$_enabled": "true", "telegraf_object_deadtime_ms": "1000"}
	if got := snap.TelegrafDeprecated(); !reflect.DeepEqual(got, want) {
		t.Fatalf("deprecated %v", got)
	}
	// XML appears: conf values unchanged, Telegraf reported.
	mtime := time.Now().Add(-time.Hour).Truncate(time.Second)
	rewrite(t, tg, "<Telegraf><enabled>true</enabled></Telegraf>", mtime)
	c, err := s.Reload(true)
	if err != nil || !c.Telegraf || len(c.Applied) != 0 {
		t.Fatalf("xml change %+v %v", c, err)
	}
	f := s.Current().Telegraf()
	if !f.Exists || f.Text != "<Telegraf><enabled>true</enabled></Telegraf>" || !f.ModTime.Equal(mtime) {
		t.Fatalf("xml %+v", f)
	}
	if v, _ := s.Current().Value("log_keep_days"); v != "1" {
		t.Fatal("xml reload lost the conf values")
	}
	// Only the conf changes: the XML stays in the new snapshot (Java D13
	// would reset the XML-overridden globals until the XML mtime changes).
	rewrite(t, path, "input_telegraf_enabled=true\nlog_keep_days=2\n", time.Now().Add(-2*time.Hour))
	c, _ = s.Reload(true)
	if c.Telegraf || !reflect.DeepEqual(c.Applied, []string{"input_telegraf_$cpu$_enabled", "input_telegraf_enabled", "log_keep_days", "telegraf_object_deadtime_ms"}) {
		t.Fatalf("conf change %+v", c)
	}
	snap = s.Current()
	if snap.Telegraf().Text != f.Text || !reflect.DeepEqual(snap.TelegrafDeprecated(), map[string]string{"input_telegraf_enabled": "true"}) {
		t.Fatalf("recomposed snapshot %+v %v", snap.Telegraf(), snap.TelegrafDeprecated())
	}
	// SaveTelegrafText + Reload(true), then removal.
	if err := s.SaveTelegrafText("<Telegraf/>"); err != nil {
		t.Fatal(err)
	}
	if c, _ := s.Reload(true); !c.Telegraf || s.Current().Telegraf().Text != "<Telegraf/>" {
		t.Fatalf("saved xml %+v", c)
	}
	os.Remove(tg)
	if c, _ := s.Reload(true); !c.Telegraf || s.Current().Telegraf().Exists {
		t.Fatalf("removed xml %+v", c)
	}
	if len(changes) != 4 {
		t.Fatalf("OnChange %d", len(changes))
	}
	if err := (&Store{}).SaveTelegrafText("x"); err == nil {
		t.Fatal("no telegraf path must fail")
	}
}
