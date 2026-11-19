package server

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/config"
	"github.com/snowvil/scouter-go/internal/logger"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/sqltable"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/webapp"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// loginAs sends LOGIN and returns the response and the connection.
func loginAs(t *testing.T, addr, id, pass string) (*protocol.MapPack, *moClient) {
	t.Helper()
	conn, r := moDial(t, addr)
	moSend(t, conn, "LOGIN", 0, encodePackT(t, &protocol.MapPack{Entries: []protocol.Entry{
		{Key: "id", Value: protocol.Value{Type: protocol.ValueText, Text: id}},
		{Key: "pass", Value: protocol.Value{Type: protocol.ValueText, Text: pass}},
		{Key: "ip", Value: protocol.Value{Type: protocol.ValueText, Text: "10.1.2.3"}}}}))
	resp := moRead(t, r, "pack")
	if len(resp.items) != 1 {
		t.Fatal("LOGIN response")
	}
	m := resp.items[0].pack.(*protocol.MapPack)
	return m, &moClient{owner: t, addr: addr, session: moMapGet(m, "session").Int, conn: conn, r: r}
}

func encodePackT(t *testing.T, p protocol.Pack) []byte {
	t.Helper()
	b, err := protocol.EncodePack(p, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// End-to-end wiring of the live settings: a scouter.conf change reaches the
// service (LOGIN), transport (pool size), storage (retention off -> on),
// the logger (log_dir reopen), the webapp, account LDAP, SqlTables and the
// log_udp_* output through Store.OnChange -> ApplySettings; a flag-pinned
// key keeps its value; the request log gets one line for a Client command.
func TestLiveWiringEndToEnd(t *testing.T) {
	root := t.TempDir()
	confDir := filepath.Join(root, "conf")
	confPath := filepath.Join(confDir, "scouter.conf")
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		t.Fatal(err)
	}
	logs1, logs2 := filepath.Join(root, "logs1"), filepath.Join(root, "logs2")
	start := time.Now().Add(-time.Hour)
	write := func(text string, at time.Time) {
		t.Helper()
		if err := os.WriteFile(confPath, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(confPath, at, at); err != nil {
			t.Fatal(err)
		}
	}
	write(strings.Join([]string{
		"server_id=srv-a", "ext_link_name=paper-a", "net_tcp_client_so_timeout_ms=5000",
		"net_tcp_service_pool_size=50", "log_dir=" + logs1, "compress_xlog_enabled=false",
		"net_http_api_cors_allow_origin=a.example", "geoip_data_city_file=" + filepath.Join(root, "missing.dat"), "",
	}, "\n"), start)
	var rt *Runtime
	var store *config.Store
	var err error
	store, err = config.Open(confPath, config.Options{Lookup: func(string) (string, bool) { return "", false }, HostName: "h",
		Pinned: map[string]string{"compress_xlog_enabled": "true"},
		OnChange: func(config.Change) {
			if rt != nil {
				rt.ApplySettings(store.Current().Live())
			}
		}})
	if err != nil {
		t.Fatal(err)
	}
	stdout := &syncBuffer{}
	lg, err := logger.New(LoggerConfig(store.Current().Live()), logger.Options{Location: time.UTC, Stdout: stdout, Stderr: stdout})
	if err != nil {
		t.Fatal(err)
	}
	defer lg.Close()
	w := webapp.DefaultConfig()
	rt, addr := moStart(t, Config{Directory: filepath.Join(root, "db"), ConfigFile: store, Logger: lg, AccountDir: confDir,
		LogDirectory: logs1, CounterXML: []byte("<Counters/>"),
		ScouterHTTP: &ScouterHTTPConfig{Listen: "127.0.0.1:0", Webapp: &w}})
	ctx := context.Background()
	today := storage.JavaDate(time.Now().UnixMilli(), time.UTC)

	// Startup values.
	if rt.tcp.Stats().ServicePoolSize != 50 {
		t.Fatalf("pool size %d", rt.tcp.Stats().ServicePoolSize)
	}
	if on, _ := rt.retention.Settings(); on {
		t.Fatal("retention must be off without mgr_purge_enabled")
	}
	if s := rt.webapp.Settings(); s.CORSAllowOrigin != "a.example" || s.PoolIdleTimeout != 5*time.Second {
		t.Fatalf("webapp startup %+v", s)
	}
	if !rt.Settings().CompressXLog {
		t.Fatal("pinned compress_xlog_enabled")
	}
	admin, _ := rt.accounts.Account("admin")
	if _, ok, _ := rt.accounts.Authorize(ctx, "admin", admin.Password); !ok {
		t.Fatal("file login")
	}
	loginPack := func() *protocol.MapPack { m, _ := loginAs(t, addr, "admin", admin.Password); return m }
	login := loginPack()
	if moMapGet(login, "server_id").Text != "srv-a" || moMapGet(login, "ext_link_name").Text != "paper-a" || moMapGet(login, "so_time_out").Int != 5000 {
		t.Fatalf("LOGIN startup %+v", login.Entries)
	}
	// Request log: one line for a RequestLogger command after the session
	// check, "<id>(<LOGIN ip>) <cmd>" in log_dir/request-yyyyMMdd.log.
	_, client := loginAs(t, addr, "admin", admin.Password)
	moSend(t, client.conn, "TRANX_LOAD_TIME_GROUP", client.session, encodePackT(t, &protocol.MapPack{}))
	waitFor(t, "request log line", func() bool {
		lg.Sync()
		b, _ := os.ReadFile(filepath.Join(logs1, "request-"+today+".log"))
		return strings.Count(string(b), " admin(10.1.2.3) TRANX_LOAD_TIME_GROUP\n") == 1
	})
	lg.Println("before reload")
	lg.Sync()
	if b, err := os.ReadFile(filepath.Join(logs1, "server-"+today+".log")); err != nil || !strings.Contains(string(b), "before reload") {
		t.Fatalf("server log in log_dir: %v %q", err, b)
	}
	// A text dictionary entry before the reload: log_udp_text is still off.
	sql := "SELECT a FROM t1 WHERE secret = 'x'"
	if err := rt.engine.Accept(ctx, &protocol.TextPack{XType: "sql", Hash: 101, Text: sql}); err != nil {
		t.Fatal(err)
	}
	want := sqltable.CrudInfo(sql)
	waitFor(t, "SqlTables table text", func() bool {
		v, ok, _ := rt.store.Text(today, "table", 101)
		return ok && v == want
	})

	write(strings.Join([]string{
		"server_id=srv-b", "ext_link_name=paper-b", "net_tcp_client_so_timeout_ms=6000",
		"net_tcp_service_pool_size=20", "log_dir=" + logs2, "compress_xlog_enabled=false",
		"mgr_purge_enabled=true", "mgr_purge_keep_days=3",
		"net_http_api_cors_allow_origin=b.example", "net_webapp_tcp_client_pool_timeout=1500",
		"log_udp_xlog=true", "log_udp_text=true", "sql_table_parsing_enabled=false",
		"geoip_data_city_file=" + filepath.Join(root, "missing.dat"), "",
	}, "\n"), start.Add(time.Minute))
	if _, err := store.Reload(true); err != nil {
		t.Fatal(err)
	}
	login = loginPack()
	if moMapGet(login, "server_id").Text != "srv-b" || moMapGet(login, "ext_link_name").Text != "paper-b" || moMapGet(login, "so_time_out").Int != 6000 {
		t.Fatalf("LOGIN after reload %+v", login.Entries)
	}
	if rt.tcp.Stats().ServicePoolSize != 20 {
		t.Fatalf("pool size after reload %d", rt.tcp.Stats().ServicePoolSize)
	}
	if on, c := rt.retention.Settings(); !on || c.ProfileDays != 10 || c.CounterDays != 21 {
		t.Fatalf("retention after reload %v %+v", on, c)
	}
	if s := rt.webapp.Settings(); s.CORSAllowOrigin != "b.example" || s.PoolIdleTimeout != 1500*time.Millisecond {
		t.Fatalf("webapp after reload %+v", s)
	}
	if !rt.Settings().CompressXLog || store.Current().Live().Effective()["compress_xlog_enabled"] != "true" {
		t.Fatal("pinned compress_xlog_enabled changed by the file")
	}
	if !rt.liveNow().LogUDP["log_udp_xlog"] || rt.liveNow().SQLTableParsing {
		t.Fatal("live log_udp_xlog / sql_table_parsing_enabled")
	}

	// log_dir change: the next line goes to the new directory.
	lg.Println("after reload")
	lg.Sync()
	if b, err := os.ReadFile(filepath.Join(logs2, "server-"+today+".log")); err != nil || !strings.Contains(string(b), "after reload") {
		t.Fatalf("server log after log_dir change: %v %q", err, b)
	}

	// log_udp_xlog / log_udp_text on stdout; the SQL text is masked.
	end := time.Now().UnixMilli()
	if err := rt.engine.Accept(ctx, &protocol.XLogPack{EndTime: end, ObjHash: 7, Service: 9, TxID: end, Elapsed: 3, TailVersion: protocol.XLogCurrentTail}); err != nil {
		t.Fatal(err)
	}
	if err := rt.engine.Accept(ctx, &protocol.TextPack{XType: "sql", Hash: 102, Text: sql}); err != nil {
		t.Fatal(err)
	}
	lg.Sync()
	out := stdout.String()
	if strings.Count(out, "DEBUG UDP XLOG: XLOG ") != 1 || !strings.Contains(out, "DEBUG UDP TEXT: sql ") || !strings.Contains(out, "<sql masked len=") || strings.Contains(out, "secret") {
		t.Fatalf("log_udp output %q", out)
	}
	// sql_table_parsing_enabled=false: the second SQL is not parsed.
	time.Sleep(50 * time.Millisecond)
	if _, ok, _ := rt.store.Text(today, "table", 102); ok {
		t.Fatal("SqlTables ran while disabled")
	}

	// account LDAP (A1): the next LOGIN binds to the directory; the invalid
	// URL makes every LDAP login fail, so the file password is refused.
	write(strings.Join([]string{"log_dir=" + logs2, "geoip_data_city_file=" + filepath.Join(root, "missing.dat"), "account_use_ldap=true", "account_ldap_provider_url=not a url", ""}, "\n"), start.Add(2*time.Minute))
	if _, err := store.Reload(true); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := rt.accounts.Authorize(ctx, "admin", admin.Password); ok {
		t.Fatal("LDAP (invalid URL) must reject the file password after the reload")
	}
	if m, _ := loginAs(t, addr, "admin", admin.Password); moMapGet(m, "session").Int != 0 {
		t.Fatal("LOGIN succeeded with LDAP on")
	}
}

// NetDataProcessor / CounterManager / ProfilePreCore debug output (I8):
// log_udp_packet before the dispatch, log_udp_object after it with the
// filled address, log_udp_some_object lines, and a log_test_rate=100 line.
func TestDebugOutputHooks(t *testing.T) {
	root := t.TempDir()
	stdout := &syncBuffer{}
	logs := filepath.Join(root, "logs")
	lg, err := logger.New(logger.Config{Dir: logs, Rotation: true, KeepDays: 31}, logger.Options{Location: time.UTC, Stdout: stdout, Stderr: stdout})
	if err != nil {
		t.Fatal(err)
	}
	defer lg.Close()
	r, err := Open(Config{Directory: filepath.Join(root, "db"), Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin",
		CounterXML: []byte("<Counters/>"), Logger: lg, GeoIPFile: filepath.Join(root, "missing.dat")})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx := context.Background()
	if err := r.engine.Accept(ctx, &protocol.ObjectPack{ObjName: "/quiet", ObjType: "mytype", Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
		t.Fatal(err)
	}
	lg.Sync()
	if out := stdout.String(); out != "" {
		t.Fatalf("debug output while off: %q", out)
	}
	l := config.DefaultLive()
	l.LogUDP = map[string]bool{"log_udp_packet": true, "log_udp_object": true}
	l.LogUDPSomeObject = "mytype"
	l.LogTestRate = 100
	r.ApplySettings(l)
	if err := r.engine.AcceptFrom(ctx, &protocol.ObjectPack{ObjName: "/obj", ObjType: "mytype", Tags: protocol.Value{Type: protocol.ValueMap}}, "10.0.0.9"); err != nil {
		t.Fatal(err)
	}
	if err := r.engine.Accept(ctx, &protocol.ProfilePack{ObjHash: 1, Service: 2, TxID: 3, Profile: []byte{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	lg.Sync()
	out := stdout.String()
	for _, want := range []string{"DEBUG objType: mytype\n", "DEBUG objType: mytype, detected: null\n", "OBJECT  objType=mytype", "DEBUG UDP OBJECT: OBJECT  objType=mytype", "addr=10.0.0.9"} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout lacks %q:\n%s", want, out)
		}
	}
	// log_udp_packet prints before the dispatch fills the address.
	if i, j := strings.Index(out, "OBJECT  objType=mytype"), strings.Index(out, "DEBUG UDP OBJECT: "); i < 0 || j < i || strings.Contains(out[:j], "addr=") {
		t.Fatalf("order/address:\n%s", out)
	}
	day := storage.JavaDate(time.Now().UnixMilli(), time.UTC)
	b, err := os.ReadFile(filepath.Join(logs, "server-"+day+".log"))
	if err != nil || !strings.Contains(string(b), "3\t[ProfileCoreCase3(Old)] Profile  svcName=null objHash=1 svc=2 txid=3 profile=3") {
		t.Fatalf("log_test_rate line: %v %q", err, b)
	}
}

// TestStorageLogRateLimit: the storage sink passes the Java interval to
// Logger.println(id, sec, msg), so the per-id limit is process wide (the
// Store and the Counters warners share it).
func TestStorageLogRateLimit(t *testing.T) {
	logs := t.TempDir()
	now := time.Date(2026, 10, 15, 1, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	lg, err := logger.New(logger.Config{Dir: logs, Rotation: true, KeepDays: 31}, logger.Options{Location: time.UTC, Now: clock, Stdout: &syncBuffer{}, Stderr: &syncBuffer{}})
	if err != nil {
		t.Fatal(err)
	}
	defer lg.Close()
	sink := (&Runtime{config: Config{Logger: lg}}).storageLog()
	sink("S152", 10, "first")
	sink("S152", 10, "suppressed")
	sink("S161", 10, "other id")
	mu.Lock()
	now = now.Add(11 * time.Second)
	mu.Unlock()
	sink("S152", 10, "after interval")
	lg.Sync()
	b, err := os.ReadFile(filepath.Join(logs, "server-20261015.log"))
	if err != nil {
		t.Fatal(err)
	}
	out := string(b)
	for _, want := range []string{"[S152] first", "[S161] other id", "[S152] after interval"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "suppressed") {
		t.Fatalf("rate limit not applied:\n%s", out)
	}
}

// TestSQLTextStoreAddTextHonorsContext: once Tables.Close's ctx has ended
// the sink writes nothing more.
func TestSQLTextStoreAddTextHonorsContext(t *testing.T) {
	store, err := storage.Open(storage.Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	sink := sqlTextStore{store}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sink.AddText(ctx, "20261015", sqltable.TextTypeTables, 7, "T1"); err != context.Canceled {
		t.Fatalf("cancelled AddText: %v", err)
	}
	if _, ok, err := sink.Text(context.Background(), "20261015", sqltable.TextTypeTables, 7); ok || err != nil {
		t.Fatalf("written after cancel: %v %v", ok, err)
	}
	if err := sink.AddText(context.Background(), "20261015", sqltable.TextTypeTables, 7, "T1"); err != nil {
		t.Fatal(err)
	}
	if text, ok, err := sink.Text(context.Background(), "20261015", sqltable.TextTypeTables, 7); !ok || err != nil || text != "T1" {
		t.Fatalf("text %q %v %v", text, ok, err)
	}
}
