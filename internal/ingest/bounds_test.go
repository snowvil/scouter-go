package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/storage"
)

// Startup must not fail when today's object file holds more objects than
// MaxObjects (C2): the most recently active ones are loaded, the rest are
// counted and register on their next heartbeat.
func TestStartupLoadsMostRecentObjectsUpToMax(t *testing.T) {
	now := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	root := t.TempDir()
	s, err := storage.Open(storage.Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	c, err := storage.OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(); s.Close() })
	const max = 4
	for i := 0; i <= max; i++ { // MaxObjects+1 records; /o0 is the oldest
		p := object(fmt.Sprintf("/o%d", i), "java")
		p.ObjHash = storage.CounterHash(p.ObjName)
		p.Wakeup = now.Add(time.Duration(i-10) * time.Second).UnixMilli()
		if err := s.PutObject("20261021", p); err != nil {
			t.Fatal(err)
		}
	}
	e, err := New(Config{Location: time.UTC, Clock: func() time.Time { return now }, XLogCapacity: 4, MaxObjects: max}, s, c)
	if err != nil {
		t.Fatal("startup over MaxObjects must not fail:", err)
	}
	defer e.Close()
	if e.ObjectCount() != max || e.Stats().ObjectsNotLoaded != 1 {
		t.Fatalf("loaded %d, not loaded %d", e.ObjectCount(), e.Stats().ObjectsNotLoaded)
	}
	if _, ok := e.ObjectNameCached(storage.CounterHash("/o0")); ok {
		t.Fatal("the least recently active object must be the one left out")
	}
	// The left-out object registers on its next heartbeat: every loaded one
	// is past its dead time, so the longest-inactive one makes room.
	if err := e.Accept(context.Background(), object("/o0", "java")); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.ObjectNameCached(storage.CounterHash("/o0")); !ok || e.ObjectCount() != max || e.Stats().ObjectsEvicted != 1 {
		t.Fatalf("heartbeat after startup: count %d evicted %d", e.ObjectCount(), e.Stats().ObjectsEvicted)
	}
	if _, ok := e.ObjectNameCached(storage.CounterHash("/o1")); ok {
		t.Fatal("eviction must take the longest-inactive object (/o1)")
	}

	// A cache budget too small for every record also loads what fits.
	small, err := New(Config{Location: time.UTC, Clock: func() time.Time { return now }, XLogCapacity: 4, MaxObjects: 100, MaxCacheBytes: 1}, s, c)
	if err != nil {
		t.Fatal("startup over MaxCacheBytes must not fail:", err)
	}
	defer small.Close()
	if small.ObjectCount() != 0 || small.Stats().ObjectsNotLoaded != max+1 {
		t.Fatalf("budget: loaded %d, not loaded %d", small.ObjectCount(), small.Stats().ObjectsNotLoaded)
	}
}

// At MaxObjects a new object takes the place of the longest-inactive one
// (memory only, like RemoveInactiveObjects); with none inactive it is
// rejected and counted.
func TestObjectLimitEvictsLongestInactive(t *testing.T) {
	now := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	e, s := testEngineWith(t, &now, func(c *Config) { c.MaxObjects = 3; c.ObjectType = alertTypes })
	ctx := context.Background()
	accept := func(name, typ string, tags ...protocol.Entry) error {
		t.Helper()
		return e.Accept(ctx, object(name, typ, tags...))
	}
	for _, name := range []string{"/a", "/b", "/c"} {
		if err := accept(name, "tomcat"); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
	}
	// Every object alive: rejected, counted, nothing evicted.
	if err := accept("/d", "tomcat"); !errors.Is(err, protocol.ErrLimit) {
		t.Fatal("want ErrLimit with no inactive object, got", err)
	}
	if st := e.Stats(); st.ObjectsLimited != 1 || st.ObjectsEvicted != 0 || st.Rejected != 1 || e.ObjectCount() != 3 {
		t.Fatalf("rejection: %+v count=%d", st, e.ObjectCount())
	}
	if err := accept("/d", "tomcat"); !errors.Is(err, protocol.ErrLimit) || e.Stats().ObjectsLimited != 2 {
		t.Fatal("second rejection", err)
	}
	// /a (wakeup t0) and /b (t0+1s) are dead at t0+9.5s; /c (t0+2s) alive.
	now = time.Date(2026, 10, 21, 9, 0, 9, 500e6, time.UTC)
	if err := e.Tick(ctx); err != nil { // INACTIVE_OBJECT for /a and /b
		t.Fatal(err)
	}
	if err := accept("/d", "tomcat"); err != nil {
		t.Fatal(err)
	}
	hashA, hashB := storage.CounterHash("/a"), storage.CounterHash("/b")
	e.mu.Lock()
	_, aLive := e.objects[hashA]
	_, aState := e.objState[hashA]
	_, aName := e.objectNames["/a"]
	_, bLive := e.objects[hashB]
	e.mu.Unlock()
	if aLive || aState || aName || !bLive || e.Stats().ObjectsEvicted != 1 {
		t.Fatalf("evicted /a only: a=%v/%v/%v b=%v evicted=%d", aLive, aState, aName, bLive, e.Stats().ObjectsEvicted)
	}
	flush(t, e)
	stored, err := s.Objects("20261021")
	if err != nil || len(stored) != 4 {
		t.Fatalf("eviction must not touch the stored records: %d %v", len(stored), err)
	}
	// The evicted object's next heartbeat registers it again (evicting /b),
	// as a new registration: no ACTIVATED_OBJECT.
	alerts := len(realtimeAlerts(t, e))
	if err := accept("/a", "tomcat"); err != nil {
		t.Fatal(err)
	}
	if e.Stats().ObjectsEvicted != 2 || len(realtimeAlerts(t, e)) != alerts {
		t.Fatalf("re-registration: evicted=%d alerts %d -> %d", e.Stats().ObjectsEvicted, alerts, len(realtimeAlerts(t, e)))
	}
}

// Batch objects never expire and are never evicted; the eviction hint is
// lowered by a heartbeat that shortens an object's dead time and dropped by
// a settings change.
func TestObjectLimitBatchAndHint(t *testing.T) {
	now := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.MaxObjects = 2 })
	ctx := context.Background()
	if err := e.Accept(ctx, object("/batch", "batch")); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, object("/a", "java")); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	tick := func() {
		t.Helper()
		if err := e.Tick(ctx); err != nil { // inactivates the dead objects first
			t.Fatal(err)
		}
	}
	tick()
	// /a is dead, /batch is not a candidate.
	if err := e.Accept(ctx, object("/x", "java")); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.ObjectNameCached(storage.CounterHash("/batch")); !ok {
		t.Fatal("batch object evicted")
	}
	// A batch heartbeat: /batch silent for an hour would be evicted next
	// (TestObjectLimitSilentExemptObjects).
	if err := e.Accept(ctx, object("/batch", "batch")); err != nil {
		t.Fatal(err)
	}
	// /x alive: rejected, the hint is set to /x's dead time (now+8s).
	if err := e.Accept(ctx, object("/y", "java")); !errors.Is(err, protocol.ErrLimit) {
		t.Fatal(err)
	}
	// A heartbeat of /x with deadTime=1000 lowers its dead time to now+2s.
	now = now.Add(time.Second)
	if err := e.Accept(ctx, object("/x", "java", protocol.Entry{Key: tagDeadTime, Value: protocol.Value{Type: protocol.ValueDecimal, Int: 1000}})); err != nil {
		t.Fatal(err)
	}
	now = now.Add(1500 * time.Millisecond)
	tick()
	if err := e.Accept(ctx, object("/y", "java")); err != nil {
		t.Fatal("hint not lowered by the heartbeat:", err)
	}
	// /y alive: rejected (hint = /y's dead time, now+8s). A shorter
	// object_deadtime_ms applies at once.
	if err := e.Accept(ctx, object("/z", "java")); !errors.Is(err, protocol.ErrLimit) {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	live := e.Settings()
	live.ObjectDeadTimeMs = 1000
	e.SetSettings(live)
	tick()
	if err := e.Accept(ctx, object("/z", "java")); err != nil {
		t.Fatal("hint not dropped by SetSettings:", err)
	}
	if st := e.Stats(); st.ObjectsEvicted != 3 || st.ObjectsLimited != 2 {
		t.Fatalf("%+v", st)
	}
}

// S2: past the cache budget the XLog is still stored; only the realtime
// ring (and the processed gxid) is skipped.
func TestXLogStoredWhenCacheBudgetExceeded(t *testing.T) {
	now := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	e, s := testEngineWith(t, &now, nil)
	ctx := context.Background()
	if err := e.Accept(ctx, &protocol.XLogPack{TxID: 1, Service: 1, ObjHash: 1, Elapsed: 5}); err != nil {
		t.Fatal(err)
	}
	before, err := e.XLogsRealtime(ctx, service.RealtimeQuery{LatestCount: 16})
	if err != nil || len(before.Records) != 1 {
		t.Fatal(before, err)
	}
	e.mu.Lock()
	e.config.MaxCacheBytes = e.cacheBytes + e.ringBytes // no room left
	e.mu.Unlock()
	if err := e.Accept(ctx, &protocol.XLogPack{TxID: 2, Service: 1, ObjHash: 1, Elapsed: 7}); err != nil {
		t.Fatal("cache budget rejected an XLog:", err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	stored, err := e.XLog(ctx, "20261021", 2)
	if err != nil || stored == nil || stored.Elapsed != 7 {
		t.Fatal("XLog not stored past the cache budget", stored, err)
	}
	after, err := e.XLogsRealtime(ctx, service.RealtimeQuery{LatestCount: 16})
	if err != nil || len(after.Records) != 1 || after.Index != before.Index || after.Loop != before.Loop {
		t.Fatalf("ring must not take the XLog: %d records, cursor %d/%d -> %d/%d", len(after.Records), before.Loop, before.Index, after.Loop, after.Index)
	}
	st := e.Stats()
	// The ring slot and the processed gxid of the driving XLog.
	if st.XLogCacheSkipped != 2 || st.XLogs != 2 || st.Completed != 2 || st.Rejected != 0 {
		t.Fatalf("%+v", st)
	}
}

// S2 for counters: past MaxCounters or the byte budget the realtime counter
// is still written (RealtimeCounterWR before CounterCache, PerfCountCore).
func TestCounterStoredWhenCacheBoundExceeded(t *testing.T) {
	now := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.MaxCounters = 3 })
	ctx := context.Background()
	counter := func(name string, v int64) *protocol.PerfCounterPack {
		return &protocol.PerfCounterPack{ObjName: name, Time: now.UnixMilli(), TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: v}}}}}
	}
	if err := e.Accept(ctx, counter("/one", 1)); err != nil { // TPS, objHash, time: 3 keys
		t.Fatal(err)
	}
	if err := e.Accept(ctx, counter("/two", 2)); err != nil {
		t.Fatal("MaxCounters rejected a stored counter:", err)
	}
	two := storage.CounterHash("/two")
	if _, ok, err := e.Counter(ctx, two, "TPS", 1); ok || err != nil {
		t.Fatal("over-bound counter cached", ok, err)
	}
	flush(t, e)
	points, err := e.CounterHistory(ctx, two, "TPS", now.UnixMilli()-1000, now.UnixMilli()+1000)
	if err != nil || len(points) != 1 || points[0].Value.Int != 2 {
		t.Fatal("counter not stored past MaxCounters", points, err)
	}
	// Byte budget: an existing key's update that does not fit keeps the old
	// cached value and is still stored.
	e.mu.Lock()
	e.config.MaxCacheBytes = e.cacheBytes + e.ringBytes
	e.mu.Unlock()
	now = now.Add(time.Second)
	if err := e.Accept(ctx, counter("/one", 1<<40)); err != nil {
		t.Fatal(err)
	}
	one := storage.CounterHash("/one")
	if v, ok, err := e.Counter(ctx, one, "TPS", 1); !ok || err != nil || v.Int != 1 {
		t.Fatal("skipped update changed the cache", v, ok, err)
	}
	flush(t, e)
	points, err = e.CounterHistory(ctx, one, "TPS", now.UnixMilli()-500, now.UnixMilli()+1000)
	if err != nil || len(points) != 1 || points[0].Value.Int != 1<<40 {
		t.Fatal("counter not stored past the byte budget", points, err)
	}
	if st := e.Stats(); st.CounterCacheSkipped != 2 || st.Rejected != 0 {
		t.Fatalf("%+v", st)
	}
}

// Concurrent new-object registration at MaxObjects with the 1s tick,
// clearInactive, OBJECT_LIST and the eviction path (run with -race).
func TestObjectLimitConcurrent(t *testing.T) {
	var clock atomic.Int64
	clock.Store(time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC).UnixMilli())
	root := t.TempDir()
	s, err := storage.Open(storage.Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	c, err := storage.OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{Location: time.UTC, Clock: func() time.Time { return time.UnixMilli(clock.Load()) }, XLogCapacity: 16, MaxObjects: 8}, s, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close(); c.Close(); s.Close() })
	ctx := context.Background()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				err := e.Accept(ctx, object(fmt.Sprintf("/w%d-%d", w, i%12), "java"))
				if err != nil && !errors.Is(err, protocol.ErrLimit) {
					t.Error(err)
					return
				}
				clock.Add(700)
			}
		}(w)
	}
	// Ticks run until the workers are done: eviction needs objects the Tick
	// already inactivated, which depends on the workers advancing the clock.
	workersDone := make(chan struct{})
	go func() { wg.Wait(); close(workersDone) }()
	var ticker sync.WaitGroup
	ticker.Add(1)
	go func() {
		defer ticker.Done()
		for i := 0; ; i++ {
			select {
			case <-workersDone:
				return
			default:
			}
			if err := e.Tick(ctx); err != nil {
				t.Error(err)
				return
			}
			if i%10 == 0 {
				if err := e.RemoveInactiveObjects(ctx); err != nil {
					t.Error(err)
					return
				}
			}
			if _, err := e.Objects(ctx, ""); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
	ticker.Wait()
	if n := e.ObjectCount(); n > 8 {
		t.Fatalf("live objects %d over MaxObjects", n)
	}
	st := e.Stats()
	// Whether a Tick inactivated objects before a full map met a new one
	// depends on scheduling; TestObjectLimitEvictsLongestInactive and
	// TestObjectLimitWaitsForInactivation pin eviction itself.
	t.Logf("evicted %d, limited %d", st.ObjectsEvicted, st.ObjectsLimited)
	if st.Completed+st.Rejected != 200 || st.Rejected != st.ObjectsLimited {
		t.Fatalf("accounting: %+v", st)
	}
}

// A dead object the 1s Tick has not inactivated yet is not evicted (Java
// clearInactive removes only alive == false): its INACTIVE_OBJECT alert
// comes first, then it can make room.
func TestObjectLimitWaitsForInactivation(t *testing.T) {
	now := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.MaxObjects = 2; c.ObjectType = alertTypes })
	ctx := context.Background()
	for _, name := range []string{"/a", "/b"} {
		if err := e.Accept(ctx, object(name, "tomcat")); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(30 * time.Second) // both dead, no Tick yet
	if err := e.Accept(ctx, object("/c", "tomcat")); !errors.Is(err, protocol.ErrLimit) {
		t.Fatal("dead but still alive objects must not be evicted, got", err)
	}
	if st := e.Stats(); st.ObjectsEvicted != 0 || st.ObjectsLimited != 1 {
		t.Fatalf("%+v", st)
	}
	alerts := len(realtimeAlerts(t, e))
	if err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(realtimeAlerts(t, e)); got != alerts+2 {
		t.Fatal("INACTIVE_OBJECT alerts before eviction:", got-alerts)
	}
	if err := e.Accept(ctx, object("/c", "tomcat")); err != nil {
		t.Fatal(err)
	}
	if st := e.Stats(); st.ObjectsEvicted != 1 {
		t.Fatalf("%+v", st)
	}
}
