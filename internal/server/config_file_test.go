package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/config"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/transport"
)

func testConfStore(t *testing.T, text string) (*config.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scouter.conf")
	if text != "" {
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	s, err := config.Open(path, config.Options{Lookup: func(string) (string, bool) { return "", false }, HostName: "conf-host", Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	return s, path
}

func TestServerConfigFileSave(t *testing.T) {
	store, path := testConfStore(t, "log_keep_days=1\n")
	f := ServerConfigFile{Store: store}
	ctx := context.Background()
	if err := f.SaveConfigurationText(ctx, "telegraf", "x"); !errors.Is(err, transport.ErrUnsupported) {
		t.Fatalf("other kinds belong elsewhere: %v", err)
	}
	if err := (ServerConfigFile{}).SaveConfigurationText(ctx, "server", "x"); !errors.Is(err, transport.ErrUnsupported) {
		t.Fatalf("nil store: %v", err)
	}
	if err := f.SaveConfigurationText(ctx, "server", "# from client\nlog_keep_days=5\ndb_dir=/elsewhere\n"); err != nil {
		t.Fatal(err)
	}
	if v, _ := store.Current().Value("log_keep_days"); v != "5" {
		t.Fatalf("saved text not applied at once: %s", v)
	}
	if v, _ := store.Current().Value("db_dir"); v != "./database" {
		t.Fatalf("restart-only key applied: %s", v)
	}
	if b, _ := os.ReadFile(path); string(b) != "# from client\nlog_keep_days=5\ndb_dir=/elsewhere\n" {
		t.Fatalf("file %q", b)
	}
	if err := f.SaveConfigurationText(ctx, "server", string(make([]byte, config.MaxFileBytes+1))); !errors.Is(err, config.ErrTooLarge) {
		t.Fatalf("oversize: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := f.SaveConfigurationText(canceled, "server", "x=1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}

func TestRuntimeMetadataFromConf(t *testing.T) {
	text := "# comment kept verbatim\nlog_keep_days=4\ndb_dir=/from/conf\next_link_name=paper \\\n  two\n"
	store, _ := testConfStore(t, text)
	retention := storage.JavaRetentionConfig(map[string]int{"mgr_purge_keep_days": 3})
	cfg := Config{Directory: "/flag/db", LogDirectory: "./logs", PluginDirectory: "./plugin", UDP: "0.0.0.0:6200", TCP: "127.0.0.1:6201", HTTP: "127.0.0.1:6180", ScouterHTTP: &ScouterHTTPConfig{Listen: "0.0.0.0:6280"}, ServerID: "go-1", Location: time.UTC, Retention: &retention}
	backend, err := runtimeMetadataWith(cfg, store, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	got, err := backend.ConfigurationText(ctx, "server")
	if err != nil || got != text {
		t.Fatalf("server text %q %v", got, err)
	}
	if _, err := backend.ConfigurationText(ctx, "counters-site"); err != nil {
		t.Fatalf("other kinds still served by FileMetadata: %v", err)
	}
	rows, err := backend.Configuration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(config.Keys) || rows[0].Key != "server_id" {
		t.Fatalf("%d rows, first %q", len(rows), rows[0].Key)
	}
	byKey := map[string]service.MetadataProperty{}
	for _, r := range rows {
		byKey[r.Key] = r
	}
	for key, want := range map[string]string{
		"server_id": "go-1", "db_dir": "/flag/db", "log_keep_days": "4", "ext_link_name": "paper two",
		"net_udp_listen_ip": "0.0.0.0", "net_udp_listen_port": "6200", "net_tcp_listen_port": "6201",
		"net_http_server_enabled": "true", "net_http_port": "6280", "mgr_purge_enabled": "true",
		"mgr_purge_counter_keep_days": "21",
	} {
		if byKey[key].Value != want {
			t.Fatalf("%s = %q, want %q", key, byKey[key].Value, want)
		}
	}
	if byKey["server_id"].Default != "conf-host" || byKey["net_udp_listen_port"].Type != 2 || byKey["compress_xlog_enabled"].Type != 3 || byKey["net_http_api_allow_ips"].Type != 4 {
		t.Fatalf("descriptor %+v", byKey["server_id"])
	}
	desc := byKey["input_telegraf_$measurement$_host_mappings"]
	if desc.Type != 5 || len(desc.TypeDescription) != 8 || desc.TypeDescription[0].Key != "strings" || desc.TypeDescription[3].Key != "booleans" {
		t.Fatalf("type desc %+v", desc.TypeDescription)
	}
	if v := desc.TypeDescription[0].Value; v.Type != protocol.ValueList || len(v.Values) != 2 || v.Values[0].Text != "telegraf host name(reqiured)" {
		t.Fatalf("strings %+v", v)
	}
	if v := desc.TypeDescription[3].Value; len(v.Values) != 2 || v.Values[1].Type != protocol.ValueBoolean || !v.Values[1].Bool {
		t.Fatalf("booleans %+v", v)
	}
	// Reload is visible without rebuilding the backend; the metadata backend
	// is also the configuration writer.
	writer, ok := backend.(interface {
		SaveConfigurationText(context.Context, string, string) error
	})
	if !ok {
		t.Fatal("metadata backend does not save the server configuration")
	}
	if err := writer.SaveConfigurationText(ctx, "server", "log_keep_days=9\n"); err != nil {
		t.Fatal(err)
	}
	rows, _ = backend.Configuration(ctx)
	for _, r := range rows {
		if r.Key == "log_keep_days" && r.Value != "9" {
			t.Fatalf("reloaded value %q", r.Value)
		}
	}
	if _, ok := backend.(service.MetadataAlertScriptBackend); !ok {
		t.Fatal("alert script reads lost")
	}
}

func TestRuntimeMetadataMissingConf(t *testing.T) {
	store, _ := testConfStore(t, "")
	backend, err := runtimeMetadataWith(Config{Directory: t.TempDir(), Location: time.UTC}, store, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Java loadText returns null for a missing file; GET_CONFIGURE_SERVER sends "".
	if text, err := backend.ConfigurationText(context.Background(), "server"); err != nil || text != "" {
		t.Fatalf("missing file %q %v", text, err)
	}
	// Without a store the flag rows remain.
	flagOnly, err := runtimeMetadataWith(Config{Directory: t.TempDir(), Location: time.UTC}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := flagOnly.Configuration(context.Background())
	if len(rows) != 9 || rows[0].Key != "log_dir" {
		t.Fatalf("flag rows %+v", rows)
	}
}
