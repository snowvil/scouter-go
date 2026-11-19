package webapp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func TestCallReusesConnectionAndSession(t *testing.T) {
	f := newFakeCollector(t)
	f.static("PING", newMap(entry("ok", decimalValue(1))))
	s, err := New(testConfig(f))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 5; i++ {
		m, err := s.single(context.Background(), "PING", newMap())
		if err != nil || mapInt(m, "ok") != 1 {
			t.Fatal(m, err)
		}
	}
	if f.dials.Load() != 1 || f.logins.Load() != 1 {
		t.Fatalf("dials=%d logins=%d", f.dials.Load(), f.logins.Load())
	}
	if s.serverID() != javaHash("127.0.0.1"+f.addr()[len("127.0.0.1:"):]) {
		t.Fatal("serverID is not hash(ip+port)")
	}
}

func TestCallReloginsOnInvalidSession(t *testing.T) {
	f := newFakeCollector(t)
	f.static("PING", newMap())
	s, _ := New(testConfig(f))
	defer s.Close()
	if _, err := s.single(context.Background(), "PING", newMap()); err != nil {
		t.Fatal(err)
	}
	f.invalidateSessions()
	// Pooled connection is already authorized (connection latch) so force a
	// fresh one: close idle connections.
	s.pool.mu.Lock()
	for _, c := range s.pool.idle {
		c.closeConn()
	}
	s.pool.idle = nil
	s.pool.mu.Unlock()
	if _, err := s.single(context.Background(), "PING", newMap()); err != nil {
		t.Fatal("relogin retry failed:", err)
	}
	if f.logins.Load() != 2 {
		t.Fatalf("logins=%d", f.logins.Load())
	}
}

func TestCallBoundsConcurrencyByPoolSize(t *testing.T) {
	f := newFakeCollector(t)
	release := make(chan struct{})
	f.handle("SLOW", bodyPack, func(fakeReq) ([]any, error) {
		<-release
		return []any{newMap()}, nil
	})
	cfg := testConfig(f)
	cfg.PoolSize = 2
	s, _ := New(cfg)
	defer s.Close()
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.single(context.Background(), "SLOW", newMap())
			errs <- err
		}()
	}
	time.Sleep(200 * time.Millisecond)
	if n := f.active.Load(); n > 2 {
		t.Fatalf("active=%d over pool size", n)
	}
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if f.maxAct.Load() > 2 || f.dials.Load() > 2 {
		t.Fatalf("max active %d dials %d", f.maxAct.Load(), f.dials.Load())
	}
}

func TestCallWaitHonorsContext(t *testing.T) {
	f := newFakeCollector(t)
	block := make(chan struct{})
	defer close(block)
	f.handle("SLOW", bodyPack, func(fakeReq) ([]any, error) { <-block; return nil, nil })
	cfg := testConfig(f)
	cfg.PoolSize = 1
	s, _ := New(cfg)
	defer s.Close()
	go s.single(context.Background(), "SLOW", newMap())
	time.Sleep(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := s.single(ctx, "SLOW", newMap()); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expected pool wait timeout, got", err)
	}
}

func TestCallUnsupportedAndClosed(t *testing.T) {
	f := newFakeCollector(t)
	cfg := testConfig(f)
	cfg.Supported = func(cmd string) bool { return cmd != "NOPE" }
	s, _ := New(cfg)
	defer s.Close()
	err := s.callNoBody(context.Background(), "NOPE", func(Item) error { return nil })
	var api *apiError
	if !errors.As(err, &api) || api.status != http.StatusNotImplemented || len(f.requests("NOPE")) != 0 {
		t.Fatal("unsupported command not 501 before TCP:", err)
	}
	// Collector closes without terminator: error, connection not pooled.
	err = s.callNoBody(context.Background(), "UNKNOWN", func(Item) error { return nil })
	if !errors.Is(err, errClosedByServer) {
		t.Fatal(err)
	}
	if len(s.pool.idle) != 0 {
		t.Fatal("broken connection pooled")
	}
}

func TestCallValueResponseAndLimits(t *testing.T) {
	f := newFakeCollector(t)
	f.handle("GET_GLOBAL_KV", bodyValue, func(r fakeReq) ([]any, error) {
		return []any{textValue("v:" + r.Value.Text)}, nil
	})
	f.static("MANY", newMap(), newMap(), newMap())
	cfg := testConfig(f)
	cfg.MaxResponseItems = 2
	s, _ := New(cfg)
	defer s.Close()
	var got string
	err := s.callValue(context.Background(), "GET_GLOBAL_KV", textValue("k"), func(it Item) error {
		if it.Value == nil || it.Pack != nil {
			t.Fatal("value response not decoded as Value")
		}
		got = it.Value.Text
		return nil
	})
	if err != nil || got != "v:k" {
		t.Fatal(got, err)
	}
	if err := s.call(context.Background(), "MANY", newMap(), func(Item) error { return nil }); !errors.Is(err, protocol.ErrLimit) {
		t.Fatal("item limit not enforced:", err)
	}
}

// TestStaleIdleConnectionRetriedOnce: a pooled connection the collector
// closed after the alive check (no response byte) is replaced by a new
// connection once for a command without side effects.
func TestStaleIdleConnectionRetriedOnce(t *testing.T) {
	f := newFakeCollector(t)
	f.static("PING", newMap(entry("ok", decimalValue(1))))
	s, _ := New(testConfig(f))
	defer s.Close()
	ctx := context.Background()
	if _, err := s.single(ctx, "PING", newMap()); err != nil {
		t.Fatal(err)
	}
	f.dropNext("PING", 1)
	if m, err := s.single(ctx, "PING", newMap()); err != nil || mapInt(m, "ok") != 1 {
		t.Fatal(m, err)
	}
	if d, n := f.dials.Load(), len(f.requests("PING")); d != 2 || n != 2 {
		t.Fatalf("dials=%d PING handled=%d", d, n)
	}
	// One retry only: the new connection failing too is returned.
	f.dropNext("PING", 2)
	if _, err := s.single(ctx, "PING", newMap()); !errors.Is(err, errClosedByServer) {
		t.Fatal(err)
	}
	if d := f.dials.Load(); d != 3 {
		t.Fatalf("dials=%d", d)
	}
	// A read command the collector executed and then closed without a byte
	// is also repeated (no side effect).
	var gets atomic.Int32
	f.handle("GET_X", bodyPack, func(fakeReq) ([]any, error) {
		if gets.Add(1) == 1 {
			return nil, errors.New("close")
		}
		return []any{newMap()}, nil
	})
	if _, err := s.single(ctx, "PING", newMap()); err != nil { // leave an idle connection
		t.Fatal(err)
	}
	if _, err := s.single(ctx, "GET_X", newMap()); err != nil || gets.Load() != 2 {
		t.Fatal(gets.Load(), err)
	}
}

// TestStaleConnectionNotRetried: a dialled (not pooled) connection and a
// command with side effects are never repeated after the request was sent.
func TestStaleConnectionNotRetried(t *testing.T) {
	f := newFakeCollector(t)
	f.static("PING", newMap())
	cfg := testConfig(f)
	cfg.PoolIdleTimeout = -time.Millisecond // Java <= 0: never reuse
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := s.single(ctx, "PING", newMap()); err != nil {
		t.Fatal(err)
	}
	dials := f.dials.Load()
	f.dropNext("PING", 1)
	if _, err := s.single(ctx, "PING", newMap()); !errors.Is(err, errClosedByServer) {
		t.Fatal(err)
	}
	if d := f.dials.Load(); d != dials+1 {
		t.Fatalf("fresh connection retried: dials %d -> %d", dials, d)
	}
	s.Close()

	s, _ = New(testConfig(f))
	defer s.Close()
	if _, err := s.single(ctx, "PING", newMap()); err != nil {
		t.Fatal(err)
	}
	dials = f.dials.Load()
	f.dropNext("SET_CUSTOM_KV", 1)
	set := newMap(entry("keySpace", textValue("ks")), entry("key", textValue("k")), entry("value", textValue("v")))
	if err := s.call(ctx, "SET_CUSTOM_KV", set, func(Item) error { return nil }); !errors.Is(err, errClosedByServer) {
		t.Fatal(err)
	}
	if d := f.dials.Load(); d != dials || f.kvGet("ks/k") != "" {
		t.Fatalf("side-effect command retried: dials %d -> %d kv=%q", dials, d, f.kvGet("ks/k"))
	}
	var sets atomic.Int32
	f.handle("SET_X", bodyPack, func(fakeReq) ([]any, error) { sets.Add(1); return nil, errors.New("close") })
	if _, err := s.single(ctx, "PING", newMap()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.single(ctx, "SET_X", newMap()); err == nil || sets.Load() != 1 {
		t.Fatal(sets.Load(), err)
	}
}

// TestStaleIdleConnectionLoginRetried: LOGIN on a stale pooled connection
// is repeated on a new connection instead of entering the login backoff.
func TestStaleIdleConnectionLoginRetried(t *testing.T) {
	f := newFakeCollector(t)
	f.static("PING", newMap())
	s, _ := New(testConfig(f))
	defer s.Close()
	ctx := context.Background()
	if _, err := s.single(ctx, "PING", newMap()); err != nil {
		t.Fatal(err)
	}
	session, _ := s.session(ctx)
	s.invalidate(session)
	f.dropNext("LOGIN", 1)
	if _, err := s.single(ctx, "PING", newMap()); err != nil {
		t.Fatal(err)
	}
	if l, d := f.logins.Load(), f.dials.Load(); l != 2 || d != 2 {
		t.Fatalf("logins=%d dials=%d", l, d)
	}
}

func TestStaleRetryable(t *testing.T) {
	stale := &staleConnError{err: errClosedByServer}
	unsent := &staleConnError{err: errClosedByServer, unsent: true}
	for _, c := range []struct {
		cmd  string
		err  error
		want bool
	}{
		{"GET_TEXT_100", stale, true}, {"TRANX_REAL_TIME_GROUP", fmt.Errorf("x: %w", stale), true}, {"LOGIN", stale, true},
		{"SET_CUSTOM_KV", stale, false}, {"SAVE_ALERT_SCRIPTING_CONTETNS", stale, false}, {"OBJECT_REMOVE_INACTIVE", stale, false},
		{"OBJECT_THREAD_CONTROL", stale, false}, {"OBJECT_HEAPHISTO", stale, false},
		{"SET_CUSTOM_KV", unsent, true}, {"GET_TEXT_100", errClosedByServer, false},
	} {
		if got := staleRetryable(c.cmd, c.err); got != c.want {
			t.Errorf("%s %v: %v", c.cmd, c.err, got)
		}
	}
}

func TestLoginFailureIsCollectorNotConnected(t *testing.T) {
	f := newFakeCollector(t)
	cfg := testConfig(f)
	cfg.Password = "wrong"
	s, _ := New(cfg)
	defer s.Close()
	_, err := s.single(context.Background(), "PING", newMap())
	var api *apiError
	if !errors.As(err, &api) || api.message != msgNotConnected {
		t.Fatal(err)
	}
}
