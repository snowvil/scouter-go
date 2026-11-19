package server

import (
	"bytes"
	"context"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/config"
	"github.com/snowvil/scouter-go/internal/ingest"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

// F0 end to end: a scouter.conf change reaches ingest, the agent pool and
// the GeoIP owner through Store.OnChange -> ApplySettings (main's wiring);
// a flag-pinned key keeps its value.
func TestApplySettingsFromConfigReload(t *testing.T) {
	root := t.TempDir()
	confPath := filepath.Join(root, "conf", "scouter.conf")
	if err := os.MkdirAll(filepath.Dir(confPath), 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(text string, at time.Time) {
		t.Helper()
		if err := os.WriteFile(confPath, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(confPath, at, at); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now().Add(-time.Hour)
	write("object_deadtime_ms=9000\nnet_tcp_agent_keepalive_interval_ms=7000\ngeoip_data_city_file="+filepath.Join(root, "missing.dat")+"\n", start)
	var rt atomic.Pointer[Runtime]
	var store *config.Store
	var err error
	store, err = config.Open(confPath, config.Options{Lookup: func(string) (string, bool) { return "", false }, HostName: "h",
		Pinned: map[string]string{"tagcnt_enabled": "false"},
		OnChange: func(config.Change) {
			if r := rt.Load(); r != nil {
				r.ApplySettings(store.Current().Live())
			}
		}})
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)
	r, err := Open(Config{Directory: filepath.Join(root, "db"), Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin",
		CounterXML: []byte("<Counters/>"), ConfigFile: store})
	if err != nil {
		t.Fatal(err)
	}
	rt.Store(r)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	s := r.engine.Settings()
	if s.ObjectDeadTimeMs != 9000 || s.TagCount || r.pool.timing.keepAlive() != 7*time.Second || r.pool.timing.wait() != time.Second {
		t.Fatalf("startup settings %+v keepalive %v", s, r.pool.timing.keepAlive())
	}
	write("object_deadtime_ms=12000\ntagcnt_enabled=true\nobject_inactive_alert_level=3\nnet_tcp_get_agent_connection_wait_ms=0\n"+
		"geoip_data_city_file="+filepath.Join("..", "geoip", "testdata", "oracle", "city_rev1.dat")+"\n", start.Add(time.Minute))
	if _, err := store.Reload(true); err != nil {
		t.Fatal(err)
	}
	s = r.engine.Settings()
	if s.ObjectDeadTimeMs != 12000 || s.TagCount || s.InactiveAlertLevel != 3 || r.pool.timing.wait() != 0 {
		t.Fatalf("reloaded settings %+v wait %v", s, r.pool.timing.wait())
	}
	// The GeoIP reload runs on the owner goroutine.
	deadline := time.Now().Add(5 * time.Second)
	for {
		end := time.Now().UnixMilli()
		txid := end
		if err := r.engine.Accept(ctx, &protocol.XLogPack{EndTime: end, ObjHash: 1, Service: 5, TxID: txid, Elapsed: 1, IPAddress: []byte{192, 168, 0, 1}, TailVersion: protocol.XLogCurrentTail}); err != nil {
			t.Fatal(err)
		}
		// The XLog writer stores it asynchronously: wait for this one.
		x, err := r.engine.XLog(ctx, storage.JavaDate(end, time.UTC), txid)
		for err == nil && x == nil && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
			x, err = r.engine.XLog(ctx, storage.JavaDate(end, time.UTC), txid)
		}
		if err == nil && x != nil && x.CountryCode == "KR" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("geoip_data_city_file change not applied:\n%s", logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	// LIST_CONFIGURE_SERVER shows the applied (pinned) value of a live key.
	if v := store.Current().Live().Effective()["tagcnt_enabled"]; v != "false" {
		t.Fatalf("effective pinned value %q", v)
	}
}

func TestApplySettingsHooksAndCoalescing(t *testing.T) {
	r := &Runtime{live: newLiveSettings()}
	var calls []int
	r.OnSettings(func(prev, next config.Live) { calls = append(calls, next.ObjectDeadTimeMs) })
	var owner atomic.Int32
	r.OnSettingsOwner(func(context.Context, config.Live) { owner.Add(1) })
	for i := 1; i <= 3; i++ {
		l := config.DefaultLive()
		l.ObjectDeadTimeMs = i
		r.ApplySettings(l)
	}
	if len(calls) != 3 || r.Settings().ObjectDeadTimeMs != 3 {
		t.Fatalf("setters %v", calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.runSettingsOwner(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for owner.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
	if owner.Load() != 1 {
		t.Fatalf("three changes coalesce into one owner pass, got %d", owner.Load())
	}
}

func TestIngestSettingsConversion(t *testing.T) {
	l := config.DefaultLive()
	l.ObjectInactiveAlertLevel = 2
	l.GeoIPEnabled = false
	got := ingestSettings(l)
	want := ingest.DefaultSettings()
	want.InactiveAlertLevel, want.GeoIP = 2, false
	if got != want {
		t.Fatalf("%+v != %+v", got, want)
	}
	if !strings.Contains(strings.Join(config.LiveKeys, ","), "geoip_data_city_file") {
		t.Fatal("LiveKeys")
	}
}

// CounterManager.addObjectTypeIfNotExist through the runtime counters store:
// the unknown type is appended to counters.site.xml with the detected
// type's family, icon and sub-object flag, and the family check sees it.
func TestAddObjectTypeIfNotExistWritesCountersSite(t *testing.T) {
	counterXML, err := os.ReadFile(filepath.Join("..", "..", "cmd", "scouter-go", "counters.xml"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	confDir := filepath.Join(root, "conf")
	r, err := Open(Config{Directory: filepath.Join(root, "db"), Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin",
		CounterXML: counterXML, ConfDir: confDir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	p := &protocol.ObjectPack{ObjName: "/my", ObjType: "mytomcat", Tags: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{
		{Key: "detected", Value: protocol.Value{Type: protocol.ValueText, Text: "tomcat"}}}}}
	if err := r.engine.Accept(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	site, err := os.ReadFile(filepath.Join(confDir, "counters.site.xml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(site)
	for _, want := range []string{`name="mytomcat"`, `disp="mytomcat"`, `family="javaee"`, `icon="tomcat"`, `sub-object="false"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("counters.site.xml lacks %s:\n%s", want, text)
		}
	}
	info, ok := r.objectType(nil)("mytomcat")
	if !ok || info.Family != "javaee" {
		t.Fatalf("family %+v %v", info, ok)
	}
	// A second heartbeat finds the type and writes nothing.
	before, _ := os.Stat(filepath.Join(confDir, "counters.site.xml"))
	if err := r.engine.Accept(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(filepath.Join(confDir, "counters.site.xml"))
	if !after.ModTime().Equal(before.ModTime()) || r.addTypeFailures.Load() != 0 {
		t.Fatal("known type rewritten")
	}
}

// go_maxprocs (Go-only) sets GOMAXPROCS at startup and on reload; removing
// it returns to the startup value.
func TestGoMaxProcsFromConfig(t *testing.T) {
	start := runtime.GOMAXPROCS(0)
	t.Cleanup(func() {
		if os.Getenv("GOMAXPROCS") == "" {
			runtime.SetDefaultGOMAXPROCS()
		} else {
			runtime.GOMAXPROCS(start)
		}
	})
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
	write("go_maxprocs=1\n", at)
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
	r, err := Open(Config{Directory: filepath.Join(root, "db"), LogDirectory: filepath.Join(root, "logs"), TelegrafConfigurationPath: filepath.Join(root, "missing.xml"), Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>"), ConfigFile: store})
	if err != nil {
		t.Fatal(err)
	}
	rt.Store(r)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	if n := runtime.GOMAXPROCS(0); n != 1 {
		t.Fatal("startup go_maxprocs=1 not applied:", n)
	}
	reload := func(text string) {
		t.Helper()
		at = at.Add(time.Minute)
		write(text, at)
		if _, err := store.Reload(true); err != nil {
			t.Fatal(err)
		}
	}
	reload("go_maxprocs=2\n")
	if n := runtime.GOMAXPROCS(0); n != 2 {
		t.Fatal("reloaded go_maxprocs=2 not applied:", n)
	}
	reload("object_deadtime_ms=9000\n")
	if n := runtime.GOMAXPROCS(0); n != start {
		t.Fatal("removing go_maxprocs did not restore", start, "got", n)
	}
	reload("go_maxprocs=-3\n")
	if n := runtime.GOMAXPROCS(0); n != start {
		t.Fatal("a negative go_maxprocs must keep the startup value, got", n)
	}
}
