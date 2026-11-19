package ingest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/storage"
)

// tagInboxCap is storage tagInboxMax: nothing drains the inbox in these
// tests (the runtime tag goroutine does), so it fills after this many XLogs.
const tagInboxCap = 1 << 14

// fillTagInbox accepts XLogs of a live object until the tag count inbox is
// full; the next tag contribution has to wait for room. Filling may find the
// XLog queue full for a moment on a loaded host (those XLogs are deferred
// too), so it returns once the XLog writer has drained the queue: a test's
// next XLog is offered to a queue with room.
func fillTagInbox(t *testing.T, e *Engine, s *storage.Store, objHash int32) {
	t.Helper()
	ctx := context.Background()
	for i := int64(1); i <= tagInboxCap; i++ {
		if err := e.Accept(ctx, &protocol.XLogPack{TxID: 100000 + i, XType: 2, Service: 1, ObjHash: objHash, TailVersion: protocol.XLogCurrentTail}); err != nil {
			t.Fatal(err)
		}
	}
	for deadline := time.Now().Add(30 * time.Second); s.XLogQueueStats().Len > 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the XLog queue did not drain")
		}
	}
}

// A full tag count inbox makes the XLog's collector wait for room (at most
// tagInboxWait, then the tags are dropped, S188) without the engine lock:
// another collector's pack is not held up meanwhile.
func TestFullTagInboxWaitsOutsideEngineLock(t *testing.T) {
	now := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	e, s := testEngineWith(t, &now, nil)
	ctx := context.Background()
	obj := &protocol.ObjectPack{ObjName: "/agent", ObjType: "java", Tags: protocol.Value{Type: protocol.ValueMap}}
	if err := e.Accept(ctx, obj); err != nil {
		t.Fatal(err)
	}
	hash := storage.CounterHash("/agent")
	fillTagInbox(t, e, s, hash)
	if st := s.TagCountStats(); st.Backpressure != 0 || st.InboxDropped != 0 {
		t.Fatalf("inbox not filled exactly: %+v", st)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var acceptErr error
	go func() {
		defer wg.Done()
		acceptErr = e.Accept(ctx, &protocol.XLogPack{TxID: 1, XType: 2, Service: 1, ObjHash: hash, TailVersion: protocol.XLogCurrentTail})
	}()
	for deadline := time.Now().Add(5 * time.Second); s.TagCountStats().Backpressure == 0; time.Sleep(100 * time.Microsecond) {
		if time.Now().After(deadline) {
			t.Fatal("the XLog did not wait for the tag inbox")
		}
	}
	start := time.Now()
	counter := &protocol.PerfCounterPack{ObjName: "/agent", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueFloat, Float: 3}}}}}
	if err := acceptWithin(e, counter); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Errorf("another collector took %v while the tag inbox was waited for", d)
	}
	wg.Wait()
	if acceptErr != nil {
		t.Fatal(acceptErr)
	}
	if st := s.TagCountStats(); st.InboxDropped != 1 {
		t.Errorf("tags dropped %d, want 1 after the bounded wait", st.InboxDropped)
	}
	if v, ok, err := e.Counter(ctx, hash, "TPS", 1); err != nil || !ok || v.Float != 3 {
		t.Errorf("counter %v %v %v", v, ok, err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if x, err := s.XLog("20261022", 1); err != nil || x == nil {
		t.Fatalf("the XLog whose tags were dropped is stored: %v %v", x, err)
	}
	if st := e.Stats(); st.Completed != tagInboxCap+3 || st.Rejected != 0 || st.XLogs != tagInboxCap+1 {
		t.Errorf("stats %+v", st)
	}
}

// Tick's INACTIVE_OBJECT alerts count their tags like any alert. With the
// tag inbox full, the tag writes of the hold are submitted after the engine
// lock (both deferred, in order, then dropped after the bounded wait): the
// tick waits, another collector does not, and the drops are not alert
// errors.
func TestTickObjectAlertTagsWaitOutsideEngineLock(t *testing.T) {
	now := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	e, s := testEngineWith(t, &now, func(c *Config) {
		c.ObjectType = func(string) (ObjectTypeInfo, bool) { return ObjectTypeInfo{Family: "javaee"}, true }
	})
	ctx := context.Background()
	for _, name := range []string{"/agent", "/other"} {
		if err := e.Accept(ctx, &protocol.ObjectPack{ObjName: name, ObjType: "java", Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
			t.Fatal(err)
		}
	}
	fillTagInbox(t, e, s, storage.CounterHash("/agent"))
	now = now.Add(10 * time.Second) // past object_deadtime_ms: both become inactive
	before := e.Stats().StoreWritesDeferred
	done := make(chan error, 1)
	go func() { done <- e.Tick(ctx) }()
	for deadline := time.Now().Add(5 * time.Second); s.TagCountStats().Backpressure == 0; time.Sleep(100 * time.Microsecond) {
		if time.Now().After(deadline) {
			t.Fatal("the alert tags did not wait for the tag inbox")
		}
	}
	start := time.Now()
	if err := acceptWithin(e, &protocol.TextPack{XType: "sql", Hash: 5, Text: "select 1"}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Errorf("another collector took %v while the tick waited for the tag inbox", d)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	st := e.Stats()
	if st.StoreWritesDeferred-before != 2 || st.ObjectAlertErrors != 0 {
		t.Errorf("stats %+v", st)
	}
	if ts := s.TagCountStats(); ts.InboxDropped != 2 || ts.Backpressure != 1 {
		t.Errorf("tag stats %+v, want both dropped after one wait", ts)
	}
	alerts, err := e.AlertsRealtime(ctx, service.AlertQuery{})
	if err != nil || len(alerts.Records) != 2 {
		t.Fatalf("alerts %+v %v", alerts, err)
	}
	e.mu.Lock()
	held := len(e.held)
	e.mu.Unlock()
	if held != 0 {
		t.Fatal("held writes left behind", held)
	}
}

// One hold deferring many writes of a full queue (a sampling bucket roll or
// a driving XLog releasing many children while the queue is full) stays
// linear under the engine lock and keeps at most heldMaxWrites: at the bound
// the held writes are submitted under the lock (the previous in-lock wait:
// here the inbox stays full, so the first waits and saturates it), and the
// later offers find the queue saturated and drop at once, as before.
func TestHeldWritesBoundedAndLinear(t *testing.T) {
	now := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	e, s := testEngineWith(t, &now, nil)
	ctx := context.Background()
	if err := e.Accept(ctx, &protocol.ObjectPack{ObjName: "/agent", ObjType: "java", Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
		t.Fatal(err)
	}
	hash := storage.CounterHash("/agent")
	fillTagInbox(t, e, s, hash)
	// Each XLog of the hold: its tags (inbox full: deferred) and a text of a
	// queue with room (the same key: a duplicate, nothing queued), which
	// must not look through the writes held so far.
	const n = 30000
	x := &protocol.XLogPack{EndTime: now.UnixMilli(), TxID: 1, Service: 1, ObjHash: hash}
	writes := make([]storage.Write, 0, 2*n)
	for i := 0; i < n; i++ {
		w, err := s.PrepareXLogTags("java", x)
		if err != nil {
			t.Fatal(err)
		}
		text, err := s.PrepareText("20261022", &protocol.TextPack{XType: "group", Hash: 5, Text: "/g"})
		if err != nil {
			t.Fatal(err)
		}
		writes = append(writes, w, text)
	}
	before := e.Stats()
	e.mu.Lock()
	start := time.Now()
	for _, w := range writes {
		if err := e.offerLocked(w, true, nil); err != nil {
			e.mu.Unlock()
			t.Fatal(err)
		}
	}
	held := time.Since(start)
	kept, bytes := len(e.held), e.heldBytes
	b := e.takeHeldLocked()
	e.mu.Unlock()
	if err := e.submitHeld(ctx, b); err != nil {
		t.Fatal(err)
	}
	if held > 500*time.Millisecond { // with one 100ms wait
		t.Errorf("%d offers took %v under the engine lock", 2*n, held)
	}
	if kept > heldMaxWrites || bytes > heldMaxBytes {
		t.Errorf("held %d writes, %d bytes", kept, bytes)
	}
	st := e.Stats()
	if d, in := st.StoreWritesDeferred-before.StoreWritesDeferred, st.StoreWritesHeldInline-before.StoreWritesHeldInline; d != heldMaxWrites || in != heldMaxWrites || st.StoreWritesHeldDropped != 0 {
		t.Errorf("deferred %d, submitted under the lock %d, stats %+v", d, in, st)
	}
	if ts := s.TagCountStats(); ts.InboxDropped != n || ts.Backpressure != 1 {
		t.Errorf("tag stats %+v: want every write dropped after one wait", ts)
	}
}

// A hold that panics after deferring writes leaves no held writes for the
// next holder: they are dropped as their full queue would, the lock is
// released and nothing is left in flight.
func TestPanicDiscardsHeldWrites(t *testing.T) {
	now := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	var boom atomic.Bool
	e, s := testEngineWith(t, &now, func(c *Config) {
		c.PackLog = func(_ protocol.Pack, processed bool) {
			if processed && boom.Load() {
				panic("pack log")
			}
		}
	})
	ctx := context.Background()
	if err := e.Accept(ctx, &protocol.ObjectPack{ObjName: "/agent", ObjType: "java", Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
		t.Fatal(err)
	}
	hash := storage.CounterHash("/agent")
	fillTagInbox(t, e, s, hash)
	before := e.Stats()
	boom.Store(true)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("no panic")
			}
		}()
		_ = e.Accept(ctx, &protocol.XLogPack{TxID: 1, XType: 2, Service: 1, ObjHash: hash, TailVersion: protocol.XLogCurrentTail})
	}()
	boom.Store(false)
	e.mu.Lock()
	held := len(e.held)
	e.mu.Unlock()
	if held != 0 {
		t.Fatal("held writes left for the next holder", held)
	}
	if st := e.Stats(); st.StoreWritesDeferred-before.StoreWritesDeferred != 1 || st.StoreWritesHeldDropped != 1 {
		t.Fatalf("stats %+v", st)
	}
	if ts := s.TagCountStats(); ts.InboxDropped != 1 || ts.Backpressure != 0 {
		t.Fatalf("tag stats %+v", ts)
	}
	fctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := e.Flush(fctx); err != nil {
		t.Fatal("a batch left in flight:", err)
	}
	if err := acceptWithin(e, &protocol.XLogPack{TxID: 2, XType: 2, Service: 1, TailVersion: protocol.XLogCurrentTail}); err != nil {
		t.Fatal(err)
	}
}

// A service text queued while lockForAccept reads the files without the
// engine lock is the answer (the caches are asked again under the lock), as
// if the text had arrived before the XLog took the lock.
func TestServiceTextArrivingDuringReadAhead(t *testing.T) {
	now := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	e, s := testEngineWith(t, &now, nil)
	e.serviceTextReadHook = func() {
		if err := s.PutText("20261022", &protocol.TextPack{XType: "service", Hash: 7, Text: "/order/list"}); err != nil {
			t.Error(err)
		}
	}
	if err := e.Accept(context.Background(), &protocol.XLogPack{TxID: 1, Service: 7}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	x, err := s.XLog("20261022", 1)
	if err != nil || x == nil || x.Group != storage.CounterHash("/order") {
		t.Fatalf("stored %+v %v", x, err)
	}
	if st := e.Stats(); st.ServiceTextsAhead != 1 || st.ServiceTextSkipped != 0 {
		t.Fatalf("stats %+v", st)
	}
}

// A delayed XLog whose service text left the storage caches before its
// driving XLog released it is not read from the files under the engine lock:
// it keeps no group (counted) and the read is started for later XLogs.
func TestEvictedServiceTextNotReadUnderLock(t *testing.T) {
	now := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	e, s := testEngineWith(t, &now, nil)
	ctx := context.Background()
	if err := s.PutText("20261022", &protocol.TextPack{XType: "service", Hash: 9, Text: "/pay/do"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil { // written and in the read cache
		t.Fatal(err)
	}
	if err := e.Accept(ctx, &protocol.XLogPack{TxID: 21, GxID: 20, Service: 9, DiscardType: 2, TailVersion: protocol.XLogCurrentTail}); err != nil {
		t.Fatal(err) // delayed; its text answered from the cache
	}
	// Deleting the permanent texts drops their cache entries (a purge would
	// too); the files no longer have the text either.
	if n, err := s.DeleteDatabasePaths(ctx, []string{"/00000000/text"}); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if err := e.Accept(ctx, &protocol.XLogPack{TxID: 20, GxID: 20, XType: 2, Service: 1, DiscardType: 1, TailVersion: protocol.XLogCurrentTail}); err != nil {
		t.Fatal(err)
	}
	if st := e.Stats(); st.ServiceTextsAhead != 0 || st.ServiceTextSkipped != 1 {
		t.Fatalf("stats %+v", st)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if x, err := s.XLog("20261022", 21); err != nil || x == nil || x.Group != 0 {
		t.Fatalf("released child %+v %v", x, err)
	}
}

// A Tick that panics after deferring writes (object alert tags) leaves no
// batch in flight and no held writes: Flush does not wait for them.
func TestTickPanicDiscardsHeldWrites(t *testing.T) {
	now := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	var boom atomic.Bool
	e, s := testEngineWith(t, &now, func(c *Config) {
		c.ObjectType = func(string) (ObjectTypeInfo, bool) {
			if boom.Load() && calls.Add(1) == 2 {
				panic("object type")
			}
			return ObjectTypeInfo{Family: "javaee"}, true
		}
	})
	ctx := context.Background()
	for _, name := range []string{"/agent", "/other"} {
		if err := e.Accept(ctx, &protocol.ObjectPack{ObjName: name, ObjType: "java", Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
			t.Fatal(err)
		}
	}
	fillTagInbox(t, e, s, storage.CounterHash("/agent"))
	now = now.Add(10 * time.Second) // both inactive: the first alert's tags are deferred, then a panic
	before := e.Stats()
	boom.Store(true)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("no panic")
			}
		}()
		_ = e.Tick(ctx)
	}()
	boom.Store(false)
	e.mu.Lock()
	held := len(e.held)
	e.mu.Unlock()
	if st := e.Stats(); held != 0 || st.StoreWritesDeferred-before.StoreWritesDeferred != 1 || st.StoreWritesHeldDropped != 1 {
		t.Fatalf("held %d, stats %+v", held, st)
	}
	if ts := s.TagCountStats(); ts.InboxDropped != 1 {
		t.Fatalf("tag stats %+v", ts)
	}
	fctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := e.Flush(fctx); err != nil {
		t.Fatal("a batch left in flight:", err)
	}
}

// Flush returns only after the writes a hold before it deferred are
// submitted (queued or dropped), so a sync or purge after it finds them in
// their queues; Close waits for them too.
func TestFlushAndCloseWaitForDeferredWrites(t *testing.T) {
	for _, closing := range []bool{false, true} {
		now := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
		e, s := testEngineWith(t, &now, nil)
		ctx := context.Background()
		if err := e.Accept(ctx, &protocol.ObjectPack{ObjName: "/agent", ObjType: "java", Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
			t.Fatal(err)
		}
		hash := storage.CounterHash("/agent")
		fillTagInbox(t, e, s, hash)
		accepted := make(chan struct{})
		go func() {
			defer close(accepted)
			_ = e.Accept(ctx, &protocol.XLogPack{TxID: 1, XType: 2, Service: 1, ObjHash: hash, TailVersion: protocol.XLogCurrentTail})
		}()
		for deadline := time.Now().Add(5 * time.Second); s.TagCountStats().Backpressure == 0; time.Sleep(100 * time.Microsecond) {
			if time.Now().After(deadline) {
				t.Fatal("the tag write did not wait")
			}
		}
		if closing {
			e.Close()
		} else if err := e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case <-accepted:
		default:
			t.Fatalf("returned before the deferred tag write was submitted (close=%v)", closing)
		}
		if ts := s.TagCountStats(); ts.InboxDropped != 1 {
			t.Fatalf("tag stats %+v", ts)
		}
	}
}

// The service text answer lockForAccept found is used only for the XLog it
// was found for: an XLog stamped with now (EndTime 0) on the other side of
// midnight asks again for its own date.
func TestServiceTextHintMatches(t *testing.T) {
	date := func(ms int64) string {
		return time.UnixMilli(ms).UTC().Format("20060102")
	}
	before := time.Date(2026, 10, 22, 23, 59, 59, 990e6, time.UTC).UnixMilli()
	after := before + 20
	h := serviceTextHint{ref: serviceTextRef{date(before), 7}, ok: true}
	if !h.matches(7, before+1, date) || h.matches(7, after, date) || h.matches(8, before, date) {
		t.Fatal("EndTime 0 hint compares dates")
	}
	h.end = before
	if !h.matches(7, before, date) || h.matches(7, before+1, date) {
		t.Fatal("hint compares EndTime")
	}
	if (&serviceTextHint{}).matches(0, 0, date) {
		t.Fatal("empty hint")
	}
}
