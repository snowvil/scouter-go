package main

import (
	"errors"
	"flag"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/config"
	"github.com/snowvil/scouter-go/internal/server"
	"github.com/snowvil/scouter-go/internal/webapp"
)

func confSnapshot(t *testing.T, text string) *config.Snapshot {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scouter.conf")
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := config.Open(path, config.Options{Lookup: func(string) (string, bool) { return "", false }, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	return s.Current()
}

// flagDefaults mirrors the main() flag defaults the mapping starts from.
func flagDefaults() server.Config {
	return server.Config{Directory: "./database", UDP: "127.0.0.1:6100", TCP: "127.0.0.1:6100", LogDirectory: "./logs", PluginDirectory: "./plugin"}
}

func TestApplyConfPriority(t *testing.T) {
	snap := confSnapshot(t, strings.Join([]string{
		"server_id=java-collector",
		"db_dir=/data/db",
		"log_dir=/data/logs",
		"plugin_dir=/data/plugin",
		"net_udp_listen_port=6200",
		"net_tcp_listen_ip=0.0.0.0",
		"net_tcp_listen_port=6201",
		"net_udp_so_rcvbuf_size=2097152",
		"compress_xlog_enabled=true",
		"compress_profile_enabled=true",
		"net_http_server_enabled=true",
		"net_http_port=6280",
		"mgr_purge_enabled=true",
		"mgr_purge_keep_days=4",
		"ext_plugin_pulse_kafka_servers=k1:9092",
		"ext_plugin_pulse_enable=true",
		"account_use_ldap=true",
		"mgr_text_db_daily_api_enabled=true",
		"object_deadtime_ms=20000",
	}, "\n"))

	// conf > default
	cfg := flagDefaults()
	r := applyConf(&cfg, snap, map[string]bool{})
	want := server.Config{ServerID: "java-collector", Directory: "/data/db", LogDirectory: "/data/logs", PluginDirectory: "/data/plugin",
		UDP: "127.0.0.1:6200", TCP: "0.0.0.0:6201", UDPReadBuffer: 2097152, CompressXLog: true, CompressProfile: true, ScouterHTTP: &server.ScouterHTTPConfig{Listen: "0.0.0.0:6280"}, ObjectDeadTime: 20 * time.Second}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("conf mapping\n got %+v\nwant %+v", cfg, want)
	}
	if !r.purgeEnabled || !reflect.DeepEqual(r.purge, map[string]int{"mgr_purge_keep_days": 4}) {
		t.Fatalf("purge %+v", r)
	}
	if !reflect.DeepEqual(r.pulse, map[string]string{"ext_plugin_pulse_kafka_servers": "k1:9092", "ext_plugin_pulse_enable": "true"}) {
		t.Fatalf("pulse %v", r.pulse)
	}
	notes := strings.Join(r.notes, "\n")
	for _, s := range []string{"0.0.0.0:6280"} {
		if !strings.Contains(notes, s) {
			t.Fatalf("missing note %q in %s", s, notes)
		}
	}

	// flag > conf: explicitly given flags keep their values.
	cfg = server.Config{ServerID: "flag-id", Directory: "/flag/db", UDP: "10.0.0.1:7000", TCP: "127.0.0.1:6100", LogDirectory: "./logs", PluginDirectory: "./plugin", HTTP: ""}
	explicit := map[string]bool{"server-id": true, "db": true, "udp": true, "compress-xlog": true, "http": true, "pulse-conf": true, "udp-read-buffer": true}
	r = applyConf(&cfg, snap, explicit)
	if cfg.ServerID != "flag-id" || cfg.Directory != "/flag/db" || cfg.UDP != "10.0.0.1:7000" || cfg.CompressXLog || cfg.HTTP != "" || cfg.UDPReadBuffer != 0 {
		t.Fatalf("flags overridden by conf: %+v", cfg)
	}
	if cfg.TCP != "0.0.0.0:6201" || !cfg.CompressProfile {
		t.Fatalf("unflagged keys not from conf: %+v", cfg)
	}
	if r.pulse != nil {
		t.Fatal("--pulse-conf must win over scouter.conf pulse keys")
	}
}

func TestApplyConfEmptyKeepsFlagDefaults(t *testing.T) {
	snap := confSnapshot(t, "# only comments\nlog_keep_days=3\nnet_http_server_enabled=false\n")
	cfg := flagDefaults()
	r := applyConf(&cfg, snap, map[string]bool{})
	if !reflect.DeepEqual(cfg, flagDefaults()) {
		t.Fatalf("defaults changed: %+v", cfg)
	}
	if r.purgeEnabled || r.purge != nil || r.pulse != nil || len(r.notes) != 0 {
		t.Fatalf("unexpected %+v", r)
	}
	// Only the port set: the Go default host is kept.
	snap = confSnapshot(t, "net_udp_listen_port=6300\nnet_tcp_listen_port=bad\n")
	cfg = flagDefaults()
	applyConf(&cfg, snap, map[string]bool{})
	if cfg.UDP != "127.0.0.1:6300" || cfg.TCP != "127.0.0.1:6100" {
		t.Fatalf("listen merge %s %s", cfg.UDP, cfg.TCP)
	}
	// mgr_purge_enabled=false explicitly disables.
	snap = confSnapshot(t, "mgr_purge_enabled=false\nmgr_purge_keep_days=2\n")
	cfg = flagDefaults()
	if r := applyConf(&cfg, snap, map[string]bool{}); r.purgeEnabled || r.purge["mgr_purge_keep_days"] != 2 {
		t.Fatalf("purge %+v", r)
	}
}

// Startup-only keys: UDP workers (flag wins) and datagram bound only when
// set in the file, with a note when adjusted; *_mb index sizes in range.
func TestApplyConfStartupKeys(t *testing.T) {
	snap := confSnapshot(t, "_net_udp_worker_thread_count=0\nnet_udp_packet_buffer_size=65535\n_mgr_kv_store_index_default_mb=4\n_mgr_text_db_index_service_mb=99\n_mgr_text_db_daily_index_mb=2\n")
	cfg := flagDefaults()
	r := applyConf(&cfg, snap, map[string]bool{})
	if cfg.UDPWorkers != 3 || cfg.UDPMaxDatagram != 65507 || cfg.IndexSizes.KV != 4 || cfg.IndexSizes.TextDaily != 2 || cfg.IndexSizes.TextService != 0 {
		t.Fatalf("startup keys %+v", cfg)
	}
	notes := strings.Join(r.notes, "\n")
	for _, s := range []string{"_net_udp_worker_thread_count=0 adjusted to 3", "net_udp_packet_buffer_size=65535 adjusted to 65507", "_mgr_text_db_index_service_mb=99 is outside"} {
		if !strings.Contains(notes, s) {
			t.Fatalf("missing note %q in %s", s, notes)
		}
	}
	cfg = flagDefaults()
	cfg.UDPWorkers = 7
	applyConf(&cfg, confSnapshot(t, "_net_udp_worker_thread_count=5\n"), map[string]bool{"udp-workers": true})
	if cfg.UDPWorkers != 7 || cfg.UDPMaxDatagram != 0 {
		t.Fatalf("--udp-workers must win: %+v", cfg)
	}
}

// net_webapp_tcp_client_pool_timeout defaults to net_tcp_client_so_timeout_ms.
func TestWebappPoolTimeoutDerived(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scouter.conf")
	if err := os.WriteFile(path, []byte("net_http_api_enabled=true\nnet_tcp_client_so_timeout_ms=4500\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := config.Open(path, config.Options{Lookup: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatal(err)
	}
	cfg := server.Config{ScouterHTTP: &server.ScouterHTTPConfig{Listen: "127.0.0.1:0"}}
	webappConfig(&cfg, store, map[string]bool{}, false)
	if cfg.ScouterHTTP.Webapp == nil || cfg.ScouterHTTP.Webapp.PoolIdleTimeout != 4500*time.Millisecond {
		t.Fatalf("pool timeout %+v", cfg.ScouterHTTP.Webapp)
	}
}

func TestOpenConf(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "conf", "scouter.conf")
	if _, err := openConf(missing, true, "", nil, nil); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("explicit missing --conf: %v", err)
	}
	s, err := openConf(missing, false, "", nil, nil)
	if err != nil || s == nil || s.Current().Exists {
		t.Fatalf("default missing conf: %v", err)
	}
	if !s.Known("ext_plugin_pulse_kafka_servers") {
		t.Fatal("pulse keys must be known")
	}
}

func TestListenerConflict(t *testing.T) {
	servlet := func(listen string) *server.ScouterHTTPConfig { return &server.ScouterHTTPConfig{Listen: listen} }
	for _, c := range []struct {
		http    string
		servlet *server.ScouterHTTPConfig
		want    string
	}{
		{"127.0.0.1:6180", servlet("0.0.0.0:6180"), "--http 127.0.0.1:6181"},
		{"0.0.0.0:6180", servlet("127.0.0.1:6180"), "--http 0.0.0.0:6181"},
		{"127.0.0.1:6180", servlet(":6180"), "share port 6180"},
		{"[::1]:6180", servlet("[::]:6180"), "--http [::1]:6181"},
		{"127.0.0.1:6180", servlet("127.0.0.1:6180"), "change net_http_port"},
		{"127.0.0.1:65535", servlet("0.0.0.0:65535"), "--http another port"},
		{"127.0.0.1:06180", servlet("0.0.0.0:6180"), "share port 6180"},
		{"[::ffff:127.0.0.1]:6180", servlet("127.0.0.1:6180"), "share port 6180"},
		{"127.0.0.1:6180", servlet("[0:0:0:0:0:0:0:0]:6180"), "share port 6180"},
		{"localhost:6180", servlet(":6180"), "--http localhost:6181"},
		{":6180", servlet(":6180"), "share port 6180"},
		{"127.0.0.1:6181", servlet("0.0.0.0:6180"), ""},
		{"localhost:6180", servlet("127.0.0.1:6180"), ""},
		{"127.0.0.1:00", servlet("0.0.0.0:0"), ""},
		{"127.0.0.1:6180", servlet("10.0.0.1:6180"), ""},
		{"127.0.0.1:0", servlet("0.0.0.0:0"), ""},
		{"", servlet("0.0.0.0:6180"), ""},
		{"127.0.0.1:6180", nil, ""},
	} {
		err := listenerConflict(c.http, c.servlet)
		if c.want == "" {
			if err != nil {
				t.Errorf("%s vs %+v: %v", c.http, c.servlet, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s vs %+v: %v, want %q", c.http, c.servlet, err, c.want)
		}
	}
}

// TestWebappPoolTimeoutNonPositive: like Java ConnectionPool, a timeout <= 0
// closes every idle connection; it must neither fail startup (webapp.New)
// nor turn into the 8s default.
func TestWebappPoolTimeoutNonPositive(t *testing.T) {
	for _, v := range []string{"0", "-5"} {
		path := filepath.Join(t.TempDir(), "scouter.conf")
		if err := os.WriteFile(path, []byte("net_http_api_enabled=true\nnet_webapp_tcp_client_pool_timeout="+v+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		store, err := config.Open(path, config.Options{Lookup: func(string) (string, bool) { return "", false }})
		if err != nil {
			t.Fatal(err)
		}
		cfg := server.Config{ScouterHTTP: &server.ScouterHTTPConfig{Listen: "127.0.0.1:0"}}
		webappConfig(&cfg, store, map[string]bool{}, false)
		if cfg.ScouterHTTP.Webapp == nil || cfg.ScouterHTTP.Webapp.PoolIdleTimeout >= 0 {
			t.Fatalf("%s: pool timeout %+v", v, cfg.ScouterHTTP.Webapp)
		}
		w := *cfg.ScouterHTTP.Webapp
		w.CollectorAddr, w.Username = "127.0.0.1:6100", "admin"
		srv, err := webapp.New(w)
		if err != nil {
			t.Fatalf("%s: webapp.New: %v", v, err)
		}
		if got := srv.Settings().PoolIdleTimeout; got >= 0 {
			t.Fatalf("%s: live pool timeout %v", v, got)
		}
		srv.Close()
	}
}

// TestServletFlagsDoNotPinWebappKeys: --scouter-http-allow-ips and
// --scouter-http-auth-ip-header are servlet-only; the webapp keeps the
// net_http_api_allow_ips / net_http_api_auth_ip_header_key of scouter.conf.
func TestServletFlagsDoNotPinWebappKeys(t *testing.T) {
	set := flag.NewFlagSet("t", flag.ContinueOnError)
	set.String("scouter-http-allow-ips", "", "")
	set.String("scouter-http-auth-ip-header", "", "")
	set.Bool("scouter-http-gzip", true, "")
	set.String("log-dir", "", "")
	if err := set.Parse([]string{"-scouter-http-allow-ips", "10.0.0.1", "-scouter-http-auth-ip-header", "X-Servlet", "-scouter-http-gzip=false", "-log-dir", "/x"}); err != nil {
		t.Fatal(err)
	}
	explicit := map[string]bool{}
	set.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	pins := flagPinsOf(set, explicit)
	if _, ok := pins["net_http_api_allow_ips"]; ok {
		t.Fatalf("allow list pinned: %v", pins)
	}
	if _, ok := pins["net_http_api_auth_ip_header_key"]; ok {
		t.Fatalf("ip header pinned: %v", pins)
	}
	if pins["log_dir"] != "/x" || pins["net_http_api_gzip_enabled"] != "false" {
		t.Fatalf("other pins lost: %v", pins)
	}

	path := filepath.Join(t.TempDir(), "scouter.conf")
	if err := os.WriteFile(path, []byte("net_http_api_allow_ips=192.168.0.7\nnet_http_api_auth_ip_header_key=X-Webapp\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := config.Open(path, config.Options{Lookup: func(string) (string, bool) { return "", false }, Pinned: pins})
	if err != nil {
		t.Fatal(err)
	}
	if w := store.Current().Live().Webapp; w.AllowIPs != "192.168.0.7" || w.AuthIPHeaderKey != "X-Webapp" {
		t.Fatalf("webapp live %+v", w)
	}
	// The servlet listener still applies the flags over the file.
	cfg := server.Config{}
	servletConfig(&cfg, store, explicit, "127.0.0.1:0", "10.0.0.1", "X-Servlet", true, true, 31)
	if api := cfg.ScouterHTTP.API; !reflect.DeepEqual(api.AllowIPs, []string{"10.0.0.1"}) || api.AuthIPHeader != "X-Servlet" {
		t.Fatalf("servlet api %+v", api)
	}
}
