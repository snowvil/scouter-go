package server

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/transport"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriterLockAndFreshJavaLock(t *testing.T) {
	root := t.TempDir()
	lock, err := acquireLock(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = acquireLock(root); err == nil {
		t.Fatal("duplicate writer accepted")
	}
	if err = lock.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "lock.dat"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = acquireLock(root); err == nil {
		t.Fatal("fresh Java writer accepted")
	}
	old := time.Now().Add(-10 * time.Second)
	if err = os.Chtimes(filepath.Join(root, "lock.dat"), old, old); err != nil {
		t.Fatal(err)
	}
	lock, err = acquireLock(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = lock.Close(); err != nil {
		t.Fatal(err)
	}
}

// A Go writer killed with SIGKILL leaves a fresh lock.dat; the next Go
// writer holds .go-writer.lock, so it takes the heartbeat over at once
// instead of failing for 5s. A fresh lock.dat without the Go tag (Java
// creates it empty) still refuses.
func TestCrashedGoWriterLockTakenOverAtOnce(t *testing.T) {
	root := t.TempDir()
	lock, err := acquireLock(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = lock.touch(); err != nil {
		t.Fatal(err)
	}
	lock.file.Close() // the process died: flock released, lock.dat left behind
	lock, err = acquireLock(root)
	if err != nil {
		t.Fatal("restart after a crashed Go writer:", err)
	}
	if err = lock.Close(); err != nil {
		t.Fatal(err)
	}
	for _, content := range []string{"", "scouter-go"} {
		if err = os.WriteFile(filepath.Join(root, "lock.dat"), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = acquireLock(root); err == nil {
			t.Fatalf("fresh lock.dat %q accepted", content)
		}
	}
}
func TestRuntimeStartShutdownRestart(t *testing.T) {
	root := t.TempDir()
	ready := filepath.Join(t.TempDir(), "ready.json")
	cfg := Config{Directory: root, Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", ReadyFile: ready, Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>")}
	for i := 0; i < 2; i++ {
		r, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(ready)
		if err != nil {
			t.Fatal(err)
		}
		var endpoints Endpoints
		if err = json.Unmarshal(b, &endpoints); err != nil || endpoints.TCP == "" || endpoints.UDP == "" {
			t.Fatal(string(b), err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()
		cancel()
		select {
		case err = <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("shutdown blocked")
		}
		if _, err = os.Stat(filepath.Join(root, "lock.dat")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("heartbeat not released", err)
		}
	}
	cfg.TCP = "bad address"
	if _, err := Open(cfg); err == nil {
		t.Fatal("bad listen address accepted")
	}
	lock, err := acquireLock(root)
	if err != nil {
		t.Fatal("failed startup leaked DB lock", err)
	}
	if err = lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeatOwnershipAndIndependentRefresh(t *testing.T) {
	root := t.TempDir()
	lock, err := acquireLock(root)
	if err != nil {
		t.Fatal(err)
	}
	r := &Runtime{lock: lock}
	r.startHeartbeat(10 * time.Millisecond)
	old := time.Now().Add(-10 * time.Second)
	if err = os.Chtimes(lock.heartbeat, old, old); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		info, err := os.Stat(lock.heartbeat)
		if err != nil {
			t.Fatal(err)
		}
		if time.Since(info.ModTime()) < time.Second {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("heartbeat waited for foreground work")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err = os.OpenFile(lock.heartbeat, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600); !errors.Is(err, os.ErrExist) {
		t.Fatal("Java exclusive claim could overlap Go writer", err)
	}
	r.heartbeatCancel()
	<-r.heartbeatDone
	replacement := filepath.Join(root, "replacement")
	if err = os.WriteFile(replacement, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(replacement, lock.heartbeat); err != nil {
		t.Fatal(err)
	}
	if err = lock.touch(); err == nil {
		t.Fatal("foreign heartbeat treated as owned")
	}
	if err = lock.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(root, "lock.dat")); err != nil {
		t.Fatal("foreign heartbeat removed", err)
	}
}
func TestErrorReportingDoesNotBlockReceiver(t *testing.T) {
	r := &Runtime{errorEvents: make(chan transport.ErrorEvent, 64)}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 65; i++ {
			r.reportError(transport.ErrorEvent{Err: errors.New("synthetic")})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("error callback blocked on logging")
	}
	if r.droppedErrorEvents.Load() != 1 {
		t.Fatal("missing overflow metric")
	}
}

// Startup runs one retention cycle (Java AutoDeleteScheduler order) and the
// periodic sync persists index headers before shutdown.
func TestRuntimeRetentionAndPeriodicSync(t *testing.T) {
	root := t.TempDir()
	old := filepath.Join(root, "20010101", "xlog", "xlog.service")
	if err := os.MkdirAll(filepath.Dir(old), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(old, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(root, "00000000", "text", "text_service.data")
	if err := os.MkdirAll(filepath.Dir(keep), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keep, nil, 0600); err != nil {
		t.Fatal(err)
	}
	retention := storage.JavaRetentionDefaults()
	retention.DiskUsagePct = 0 // the shared test host disk must not drive deletion.
	cfg := Config{Directory: root, Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>"), SyncInterval: 20 * time.Millisecond, Retention: &retention, RetentionInterval: time.Hour}
	r, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	if err = r.store.PutXLog(&protocol.XLogPack{EndTime: now, TxID: 7}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	header := filepath.Join(root, storage.JavaDate(now, time.UTC), "xlog", "xlog_tim.hfile")
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, oldErr := os.Stat(filepath.Join(root, "20010101"))
		b, readErr := os.ReadFile(header)
		synced := readErr == nil && len(b) >= 8 && binary.BigEndian.Uint32(b[4:8]) > 0
		if errors.Is(oldErr, os.ErrNotExist) && synced {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retention or periodic sync not observed", oldErr, readErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err = os.Stat(keep); err != nil {
		t.Fatal("permanent dictionary removed", err)
	}
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if r.maintenanceErrors.Load() != 0 || r.purged.Load() == 0 {
		t.Fatal(r.maintenanceErrors.Load(), r.purged.Load())
	}
	if _, err = Open(Config{Directory: root, Location: time.UTC, SyncInterval: -1}); err == nil {
		t.Fatal("negative sync interval accepted")
	}
}

// --http serves /health with the live counters and pprof; UDP tuning flags
// reach the receiver and the socket.
func TestRuntimeHTTPHealthAndUDPTuning(t *testing.T) {
	root := t.TempDir()
	ready := filepath.Join(t.TempDir(), "ready.json")
	cfg := Config{Directory: root, Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", HTTP: "127.0.0.1:0", ReadyFile: ready, Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>"), UDPWorkers: 3, UDPQueue: 1024, UDPReadBuffer: 1 << 20}
	r, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	b, err := os.ReadFile(ready)
	if err != nil {
		t.Fatal(err)
	}
	var endpoints Endpoints
	if err = json.Unmarshal(b, &endpoints); err != nil || endpoints.HTTP == "" {
		t.Fatal(string(b), err)
	}
	resp, err := http.Get("http://" + endpoints.HTTP + "/health")
	if err != nil {
		t.Fatal(err)
	}
	var health Health
	err = json.NewDecoder(resp.Body).Decode(&health)
	resp.Body.Close()
	if err != nil || resp.StatusCode != 200 || health.Status != "ok" || health.Maintenance["errors"] != 0 {
		t.Fatal(resp.StatusCode, health, err)
	}
	if v, ok := health.Maintenance["counterNamesLimited"]; !ok || v != 0 {
		t.Fatal("counterNamesLimited", health.Maintenance)
	}
	if resp, err = http.Get("http://" + endpoints.HTTP + "/debug/pprof/heap?debug=1"); err != nil || resp.StatusCode != 200 {
		t.Fatal(resp, err)
	}
	resp.Body.Close()
	if resp, err = http.Post("http://"+endpoints.HTTP+"/health", "text/plain", nil); err != nil || resp.StatusCode != 405 {
		t.Fatal("POST /health", resp.StatusCode, err)
	}
	resp.Body.Close()
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if _, err = http.Get("http://" + endpoints.HTTP + "/health"); err == nil {
		t.Fatal("HTTP listener survived shutdown")
	}
}

// Config.AccountDir turns on the Java AccountManager files: missing files are
// created from the Scouter defaults and the reload daemon stops with Run.
func TestRuntimeAccountFiles(t *testing.T) {
	root := t.TempDir()
	conf := filepath.Join(root, "conf")
	cfg := Config{Directory: filepath.Join(root, "db"), Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", AccountDir: conf, CounterXML: []byte("<Counters/>")}
	r, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"account.xml", "account_group.xml"} {
		if _, err := os.Stat(filepath.Join(conf, name)); err != nil {
			t.Fatalf("%s not created: %v", name, err)
		}
	}
	if len(r.accounts.Accounts()) != 2 || len(r.accounts.Groups()) != 2 {
		t.Fatalf("default accounts/groups: %d/%d", len(r.accounts.Accounts()), len(r.accounts.Groups()))
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown blocked by the account reload loop")
	}
}
