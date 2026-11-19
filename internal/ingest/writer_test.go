package ingest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/storage"
)

// flush waits for the record writers (Engine.Flush): a test reads the files
// only after the records queued before are written, as a Client sees them
// after the original's writer threads.
func flush(t testing.TB, e *Engine) {
	t.Helper()
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// blockWriters makes the writers of kinds wait before each write until
// release is called or the writer context ends (Close giving up): a stalled
// disk or a long-held storage gate. The other kinds write as usual.
func blockWriters(t testing.TB, e *Engine, kinds ...writeKind) (release func()) {
	ch := make(chan struct{})
	blocked := map[writeKind]bool{} // read-only once the hook is set
	for _, k := range kinds {
		blocked[k] = true
	}
	hook := func(ctx context.Context, k writeKind) {
		if blocked[k] {
			select {
			case <-ch:
			case <-ctx.Done():
			}
		}
	}
	e.w.hook.Store(&hook)
	var once sync.Once
	release = func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)
	return release
}

// blockWritersSignalled is blockWriters that also reports each blocked
// write as it starts waiting (entered), so a test can wait for that state
// instead of sleeping.
func blockWritersSignalled(t testing.TB, e *Engine, kinds ...writeKind) (release func(), entered <-chan writeKind) {
	ch := make(chan struct{})
	in := make(chan writeKind, 64)
	blocked := map[writeKind]bool{}
	for _, k := range kinds {
		blocked[k] = true
	}
	hook := func(ctx context.Context, k writeKind) {
		if blocked[k] {
			select {
			case in <- k:
			default:
			}
			select {
			case <-ch:
			case <-ctx.Done():
			}
		}
	}
	e.w.hook.Store(&hook)
	var once sync.Once
	release = func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)
	return release, in
}

func waitEntered(t testing.TB, entered <-chan writeKind, k writeKind) {
	t.Helper()
	select {
	case got := <-entered:
		if got != k {
			t.Fatalf("writer %d blocked, want %d", got, k)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the writer did not start the write")
	}
}

func writerStats(e *Engine, k writeKind) WriterStats { return e.WriterStats()[k] }

func counterPack(name string, now time.Time, timeType byte, key string, v int64) *protocol.PerfCounterPack {
	return &protocol.PerfCounterPack{ObjName: name, Time: now.UnixMilli(), TimeType: timeType, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: key, Value: protocol.Value{Type: protocol.ValueDecimal, Int: v}}}}}
}

func objectPack(name, version string) *protocol.ObjectPack {
	return &protocol.ObjectPack{ObjName: name, ObjType: "java", Version: version, Tags: protocol.Value{Type: protocol.ValueMap}}
}

// S1/S3: with the counter, alert, status and object writers stalled, XLogs
// are still accepted at once and the realtime state (counter cache, alert
// ring, status cache, live objects) is updated at once; the files get the
// records once the writers resume.
func TestStalledWritersDoNotStallIngestOrMemory(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 3, 0, 0, time.UTC)
	e, s, _ := testEngine(t, &now, 64)
	release := blockWriters(t, e, writeCounter, writeAlert, writeStatus, writeObject, writeVisitor)
	ctx := context.Background()
	hash := storage.CounterHash("/a")
	for _, p := range []protocol.Pack{
		objectPack("/a", "1"),
		counterPack("/a", now, 1, "TPS", 41),
		&protocol.AlertPack{ObjHash: hash, ObjType: "java", Title: "slow", Level: 1, Tags: protocol.Value{Type: protocol.ValueMap}},
		&protocol.StatusPack{ObjType: "java", ObjHash: hash, Key: "threads", Data: protocol.Value{Type: protocol.ValueMap}},
	} {
		if err := acceptWithin(e, p); err != nil {
			t.Fatal(p, err)
		}
	}
	for i := int64(1); i <= 200; i++ {
		if err := acceptWithin(e, &protocol.XLogPack{TxID: i, Service: 1, ObjHash: hash, Elapsed: 10, UserID: i, TailVersion: 12}); err != nil {
			t.Fatal("XLog stalled behind a blocked writer:", err)
		}
	}
	if st := e.Stats(); st.XLogs != 200 || st.Rejected != 0 {
		t.Fatalf("%+v", st)
	}
	if v, ok, err := e.Counter(ctx, hash, "TPS", 1); err != nil || !ok || v.Int != 41 {
		t.Fatal("realtime counter not cached at once", v, ok, err)
	}
	if out, err := e.AlertsRealtime(ctx, service.AlertQuery{}); err != nil || len(out.Records) != 1 || out.Records[0].Title != "slow" {
		t.Fatal("alert ring not updated at once", out, err)
	}
	if st, err := e.Status(ctx, hash, "threads"); err != nil || st == nil {
		t.Fatal("status cache not updated at once", st, err)
	}
	if obj, err := e.Object(ctx, "", hash); err != nil || obj == nil || !obj.Alive {
		t.Fatal("object not live at once", obj, err)
	}
	// Nothing reached the files yet.
	if pts, err := e.CounterHistory(ctx, hash, "TPS", now.UnixMilli()-1000, now.UnixMilli()+1000); err != nil || len(pts) != 0 {
		t.Fatal("counter written while its writer is blocked", pts, err)
	}
	if w := writerStats(e, writeCounter); w.Pending != 1 || w.Written != 0 {
		t.Fatalf("%+v", w)
	}
	if n, err := e.VisitorRealtime(ctx, []int32{hash}, ""); err != nil || n != 0 {
		t.Fatal("visits counted while the visitor writer is blocked", n, err)
	}
	release()
	flush(t, e)
	if pts, err := e.CounterHistory(ctx, hash, "TPS", now.UnixMilli()-1000, now.UnixMilli()+1000); err != nil || len(pts) != 1 || pts[0].Value.Int != 41 {
		t.Fatal(pts, err)
	}
	day := now.Truncate(24 * time.Hour).UnixMilli()
	if out, err := e.AlertHistory(ctx, "20261021", day, now.UnixMilli()+1); err != nil || len(out) != 1 {
		t.Fatal(out, err)
	}
	if out, err := e.StatusHistory(ctx, "20261021", day, now.UnixMilli()+1); err != nil || len(out) != 1 {
		t.Fatal(out, err)
	}
	if out, err := s.Objects("20261021"); err != nil || len(out) != 1 || out[0].ObjName != "/a" {
		t.Fatal(out, err)
	}
	if n, err := e.VisitorRealtime(ctx, []int32{hash}, ""); err != nil || n != 200 {
		t.Fatal("visits", n, err)
	}
	st := e.Stats()
	if st.WritePending != 0 || st.WriteErrors != 0 || st.WriteDropped != 0 || st.WriteEnqueued != st.WriteWritten {
		t.Fatalf("%+v", st)
	}
}

// S3: a failing counter or alert write leaves the realtime state updated and
// is counted per kind; ingest is not rejected.
func TestFailedWritesKeepMemoryAndAreCounted(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 3, 0, 0, time.UTC)
	e, s, c := testEngine(t, &now, 8)
	ctx := context.Background()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	hash := storage.CounterHash("/a")
	if err := e.Accept(ctx, counterPack("/a", now, 1, "TPS", 7)); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, &protocol.AlertPack{ObjHash: hash, ObjType: "java", Title: "t", Level: 2, Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
		t.Fatal(err)
	}
	flush(t, e)
	if v, ok, err := e.Counter(ctx, hash, "TPS", 1); err != nil || !ok || v.Int != 7 {
		t.Fatal(v, ok, err)
	}
	if out, err := e.AlertsRealtime(ctx, service.AlertQuery{}); err != nil || len(out.Records) != 1 {
		t.Fatal(out, err)
	}
	if w := writerStats(e, writeCounter); w.WriteErrors != 1 || w.Written != 0 || w.Pending != 0 {
		t.Fatalf("counter %+v", w)
	}
	if w := writerStats(e, writeAlert); w.WriteErrors != 1 || w.Written != 0 {
		t.Fatalf("alert %+v", w)
	}
	if st := e.Stats(); st.Rejected != 0 || st.StorageErrors != 0 || st.Completed != 2 || st.WriteErrors != 2 {
		t.Fatalf("%+v", st)
	}
}

// A full queue drops the new record at once (RequestQueue.put), counted per
// kind; XLog ingest is not affected. The byte bound applies too.
func TestFullWriterQueueDropsAndCounts(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 3, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 64)
	release := blockWriters(t, e, writeCounter, writeStack)
	ctx := context.Background()
	const n = 10100
	for i := 0; i < n; i++ {
		if err := e.Accept(ctx, counterPack("/a", now, 1, "TPS", int64(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := acceptWithin(e, &protocol.XLogPack{TxID: 1, Service: 1, ObjHash: 1, Elapsed: 10, TailVersion: 12}); err != nil {
		t.Fatal(err)
	}
	w := writerStats(e, writeCounter)
	// The writer holds one record in its (blocked) write; the queue the rest.
	if w.Enqueued+w.Dropped != n || w.Dropped < uint64(n-writerSpecs[writeCounter].items-1) || w.Enqueued > uint64(writerSpecs[writeCounter].items)+1 {
		t.Fatalf("%+v", w)
	}
	if v, ok, err := e.Counter(ctx, storage.CounterHash("/a"), "TPS", 1); err != nil || !ok || v.Int != n-1 {
		t.Fatal("cache must follow every accepted pack", v, ok, err)
	}
	if st := e.Stats(); st.Rejected != 0 || st.XLogs != 1 || st.WriteDropped != w.Dropped {
		t.Fatalf("%+v", st)
	}
	// Byte bound: 1 MiB stacks into a 16 MiB queue.
	if err := e.Accept(ctx, objectPack("/s", "1")); err != nil {
		t.Fatal(err)
	}
	const stacks = 20
	for i := 0; i < stacks; i++ {
		if err := e.Accept(ctx, &protocol.StackPack{Time: now.UnixMilli(), ObjHash: storage.CounterHash("/s"), Data: make([]byte, 1<<20)}); err != nil {
			t.Fatal(err)
		}
	}
	sw := writerStats(e, writeStack)
	if sw.Enqueued+sw.Dropped != stacks || sw.Dropped == 0 || sw.Enqueued > uint64(writerSpecs[writeStack].bytes>>20)+1 {
		t.Fatalf("stack %+v", sw)
	}
	release()
	flush(t, e)
	w = writerStats(e, writeCounter)
	if w.Written != w.Enqueued || w.Pending != 0 || w.WriteErrors != 0 {
		t.Fatalf("%+v", w)
	}
}

// Records of one kind are written in accept order (one consumer): for a daily
// counter bucket the last queued value stays, whichever producer queued it
// (an Agent's non-realtime pack, the 5m sample pass, the group save).
func TestDailyCounterFIFOLastQueuedValueWins(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 3, 0, 0, time.UTC)
	e, _, c := testEngine(t, &now, 8)
	release := blockWriters(t, e, writeDaily)
	ctx := context.Background()
	for i := int64(1); i <= 200; i++ {
		if err := e.Accept(ctx, counterPack("/fifo", now, 3, "TPS", i)); err != nil {
			t.Fatal(err)
		}
	}
	// Agent 5m value, then the sample pass for the same key and bucket.
	if err := e.Accept(ctx, counterPack("/a", now, 1, "TPS", 41)); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, counterPack("/a", now, 3, "TPS", 7)); err != nil {
		t.Fatal(err)
	}
	if err := e.Sample5M(ctx); err != nil {
		t.Fatal(err)
	}
	release()
	flush(t, e) // one sample pass is queued at a time (passKind)
	// The sample pass, then an Agent 5m value for the same key and bucket.
	if err := e.Accept(ctx, counterPack("/b", now, 1, "TPS", 50)); err != nil {
		t.Fatal(err)
	}
	if err := e.Sample5M(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, counterPack("/b", now, 3, "TPS", 9)); err != nil {
		t.Fatal(err)
	}
	release()
	flush(t, e)
	for name, want := range map[string]int64{"/fifo": 200, "/a": 41, "/b": 9} {
		values, err := c.Daily("20261021", storage.CounterHash(name), "TPS", 3)
		if err != nil || len(values) <= 144 || values[144].Int != want {
			t.Fatal(name, values, err)
		}
	}
	if w := writerStats(e, writeDaily); w.WriteErrors != 0 || w.Dropped != 0 || w.Pending != 0 {
		t.Fatalf("%+v", w)
	}
}

// Stack and visitor records need a live object when they are processed
// (StackAnalyzerDB, VisitorCore); without one they are counted, not queued.
func TestStackAndVisitWithoutObjectAreCounted(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 3, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 8)
	ctx := context.Background()
	if err := e.Accept(ctx, &protocol.StackPack{Time: now.UnixMilli(), ObjHash: 99, Data: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, &protocol.XLogPack{TxID: 1, Service: 1, ObjHash: 99, UserID: 5, Elapsed: 1, TailVersion: 12}); err != nil {
		t.Fatal(err)
	}
	st := e.Stats()
	if st.SkippedStacks != 1 || st.VisitorsSkipped != 1 {
		t.Fatalf("%+v", st)
	}
	if writerStats(e, writeStack).Enqueued != 0 || writerStats(e, writeVisitor).Enqueued != 0 {
		t.Fatal(e.WriterStats())
	}
}

// Times and dates are fixed at accept: records queued on both sides of
// midnight and written after it go to the file of their own day.
func TestQueuedRecordsKeepTheirDayAcrossMidnight(t *testing.T) {
	now := time.Date(2026, 10, 21, 23, 59, 59, 500e6, time.UTC)
	e, s, c := testEngine(t, &now, 8)
	release := blockWriters(t, e, writeCounter, writeAlert, writeStatus, writeObject)
	ctx := context.Background()
	hash := storage.CounterHash("/a")
	accept := func() {
		t.Helper()
		for _, p := range []protocol.Pack{
			objectPack("/a", "1"),
			&protocol.PerfCounterPack{ObjName: "/a", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: now.UnixMilli()}}}}}, // time 0: now
			&protocol.AlertPack{ObjHash: hash, ObjType: "java", Title: "t", Tags: protocol.Value{Type: protocol.ValueMap}},
			&protocol.StatusPack{ObjType: "java", ObjHash: hash, Key: "k", Data: protocol.Value{Type: protocol.ValueMap}},
		} {
			if err := e.Accept(ctx, p); err != nil {
				t.Fatal(err)
			}
		}
	}
	accept()
	first := now
	now = now.Add(time.Second) // 2026-10-22 00:00:00.5
	accept()
	release()
	flush(t, e)
	for _, day := range []struct {
		date string
		at   time.Time
	}{{"20261021", first}, {"20261022", now}} {
		from, to := day.at.Truncate(24*time.Hour).UnixMilli(), day.at.UnixMilli()+1 // within the day
		if out, err := e.StatusHistory(ctx, day.date, from, to); err != nil || len(out) != 1 || out[0].Time != day.at.UnixMilli() {
			t.Fatal(day.date, "status", out, err)
		}
		if out, err := e.AlertHistory(ctx, day.date, from, to); err != nil || len(out) != 1 || out[0].Time != day.at.UnixMilli() {
			t.Fatal(day.date, "alert", out, err)
		}
		if pts, err := c.Realtime(day.date, hash, "TPS", from, to); err != nil || len(pts) != 1 || pts[0].Value.Int != day.at.UnixMilli() {
			t.Fatal(day.date, "counter", pts, err)
		}
		if out, err := s.Objects(day.date); err != nil || len(out) != 1 || out[0].Wakeup != day.at.UnixMilli() {
			t.Fatal(day.date, "object", out, err)
		}
	}
}

// Close writes what was queued before it returns (the storage is closed
// after it), within the deadline.
func TestCloseDrainsQueuedRecords(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 3, 0, 0, time.UTC)
	e, _, c := testEngine(t, &now, 8)
	release := blockWriters(t, e, writeCounter)
	ctx := context.Background()
	for i := int64(1); i <= 3; i++ {
		if err := e.Accept(ctx, counterPack("/a", now.Add(time.Duration(i)*time.Millisecond), 1, "TPS", i)); err != nil {
			t.Fatal(err)
		}
	}
	closed := make(chan struct{})
	go func() { e.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned before the queue was written")
	case <-time.After(50 * time.Millisecond):
	}
	release()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
	if w := writerStats(e, writeCounter); w.Written != 3 || w.Abandoned != 0 || w.Pending != 0 {
		t.Fatalf("%+v", w)
	}
	pts, err := c.Realtime("20261021", storage.CounterHash("/a"), "TPS", now.UnixMilli(), now.UnixMilli()+10)
	if err != nil || len(pts) != 3 {
		t.Fatal(pts, err)
	}
	if err := e.Flush(ctx); err != nil {
		t.Fatal("Flush after Close", err)
	}
	if err := e.Accept(ctx, counterPack("/a", now, 1, "TPS", 1)); err == nil {
		t.Fatal("accepted after Close")
	}
}

// Past the deadline Close stops writing: the record in progress sees its
// context cancelled (a write error) and the rest is counted as abandoned.
func TestCloseDeadlineAbandonsTheRest(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 3, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 8)
	blockWriters(t, e, writeCounter)
	ctx := context.Background()
	for i := int64(1); i <= 5; i++ {
		if err := e.Accept(ctx, counterPack("/a", now, 1, "TPS", i)); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(e.w.q[writeCounter].ch) != 4 { // the writer holds the first one
		if time.Now().After(deadline) {
			t.Fatal("the writer did not take the first record")
		}
		time.Sleep(time.Millisecond)
	}
	if !e.w.close(50 * time.Millisecond) {
		t.Fatal("deadline not reported")
	}
	w := writerStats(e, writeCounter)
	if w.Written != 0 || w.WriteErrors != 1 || w.Abandoned != 4 || w.Pending != 0 || w.Enqueued != 5 {
		t.Fatalf("%+v", w)
	}
	if st := e.Stats(); st.WriteAbandoned != 4 {
		t.Fatalf("%+v", st)
	}
}

// A writer stuck in a call that ignores its context (a hung disk) does not
// hold Close past the deadline and writerStuckGrace: its records stay
// counted as Pending, and once the call returns the rest is abandoned.
func TestCloseDeadlineLeavesAStuckWriter(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 3, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 8)
	ch := make(chan struct{})
	hook := func(_ context.Context, k writeKind) {
		if k == writeCounter {
			<-ch // no ctx: the stuck syscall
		}
	}
	e.w.hook.Store(&hook)
	var once sync.Once
	release := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)
	ctx := context.Background()
	for i := int64(1); i <= 3; i++ {
		if err := e.Accept(ctx, counterPack("/a", now, 1, "TPS", i)); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(e.w.q[writeCounter].ch) != 2 { // the writer holds the first one
		if time.Now().After(deadline) {
			t.Fatal("the writer did not take the first record")
		}
		time.Sleep(time.Millisecond)
	}
	closed := make(chan bool, 1)
	start := time.Now()
	go func() { closed <- e.w.close(50 * time.Millisecond) }()
	select {
	case expired := <-closed:
		if !expired {
			t.Fatal("deadline not reported")
		}
	case <-time.After(writerStuckGrace + 3*time.Second):
		t.Fatal("close waits for a writer stuck past its deadline")
	}
	if d := time.Since(start); d > writerStuckGrace+time.Second {
		t.Fatalf("close took %v", d)
	}
	if w := writerStats(e, writeCounter); w.Pending != 3 || w.Abandoned != 0 || w.Enqueued != 3 {
		t.Fatalf("stuck writer %+v", w)
	}
	release()
	select {
	case <-e.w.q[writeCounter].done:
	case <-time.After(5 * time.Second):
		t.Fatal("the writer did not return once its call did")
	}
	// The record in the stuck call is not written: its write sees the
	// cancelled context once the call returns.
	if w := writerStats(e, writeCounter); w.Pending != 0 || w.Abandoned != 2 || w.Written != 0 || w.WriteErrors != 1 {
		t.Fatalf("after the stuck call %+v", w)
	}
}

// Visits and the hourly roll of the tick go through one queue in accept
// order; the visitor query reads the in-memory state concurrently.
func TestVisitorQueueOrderAndConcurrentQueries(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 59, 59, 0, time.UTC)
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	e, _ := testEngineWith(t, &now, func(c *Config) { c.Clock = clock })
	ctx := context.Background()
	hash := storage.CounterHash("/a")
	if err := e.Accept(ctx, objectPack("/a", "1")); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	queries := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				queries <- nil
				return
			default:
			}
			if _, err := e.VisitorRealtime(ctx, []int32{hash}, ""); err != nil {
				queries <- err
				return
			}
			_ = e.WriterStats()
		}
	}()
	for i := int64(1); i <= 300; i++ {
		if i == 150 {
			mu.Lock()
			now = now.Add(2 * time.Second) // 13:00:01: the tick rolls the hour
			mu.Unlock()
			if err := e.Tick(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if err := e.Accept(ctx, &protocol.XLogPack{TxID: i, Service: 1, ObjHash: hash, UserID: i, Elapsed: 1, TailVersion: 12}); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	if err := <-queries; err != nil {
		t.Fatal(err)
	}
	flush(t, e)
	if n, err := e.VisitorRealtime(ctx, []int32{hash}, ""); err != nil || n < 290 || n > 310 { // HLL estimate of 300
		t.Fatal(n, err)
	}
	if w := writerStats(e, writeVisitor); w.Written != w.Enqueued || w.Enqueued != 301 || w.WriteErrors != 0 {
		t.Fatalf("%+v", w)
	}
}

// Ingest, periodic passes, flushes, stats and queries at once (-race).
func TestWritersConcurrentWithIngest(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 5, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 64)
	ctx := context.Background()
	if err := e.Accept(ctx, objectPack("/a", "1")); err != nil {
		t.Fatal(err)
	}
	hash := storage.CounterHash("/a")
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := int64(1); i <= 300; i++ {
				var p protocol.Pack
				switch i % 6 {
				case 0:
					p = &protocol.XLogPack{TxID: i + int64(g)<<20, Service: 1, ObjHash: hash, UserID: i, Elapsed: 1, TailVersion: 12}
				case 1:
					p = counterPack("/a", now, 1, "TPS", i)
				case 2:
					p = &protocol.AlertPack{ObjHash: hash, ObjType: "java", Title: "t", Tags: protocol.Value{Type: protocol.ValueMap}}
				case 3:
					p = &protocol.StatusPack{ObjType: "java", ObjHash: hash, Key: "k", Data: protocol.Value{Type: protocol.ValueMap}}
				case 4:
					p = objectPack("/a", string(rune('a'+i%3)))
				case 5:
					p = &protocol.StackPack{Time: now.UnixMilli(), ObjHash: hash, Data: []byte{1, 2}}
				}
				if err := e.Accept(ctx, p); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			if err := errors.Join(e.Tick(ctx), e.Sample5M(ctx), e.Flush(ctx)); err != nil {
				errs <- err
				return
			}
			_ = e.Stats()
			for _, w := range e.WriterStats() {
				if w.Written+w.WriteErrors+w.WriteInvalid+w.Abandoned > w.Enqueued {
					errs <- fmt.Errorf("outcomes ahead of Enqueued: %+v", w)
					return
				}
			}
			if _, err := e.AlertsRealtime(ctx, service.AlertQuery{}); err != nil {
				errs <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	flush(t, e)
	st := e.Stats()
	if st.WriteEnqueued != st.WriteWritten+st.WriteErrors+st.WriteInvalid || st.WritePending != 0 || st.WriteErrors != 0 {
		t.Fatalf("%+v %+v", st, e.WriterStats())
	}
}

// A permanent removal is queued behind the object writes queued before it
// (one ObjectWR queue in the original): a queued write does not add the
// removed record back, and the command does not wait for the disk under the
// engine lock.
//
// Store.RemoveObject removes from the object file of the real today
// (ObjectWR.remove, DateUtil.yyyymmdd() in the Store's location), so the
// engine clock is today at 12:03 in that location (testEngine: UTC). A run
// that straddles midnight is repeated once with the new date.
func TestPermanentRemovalFollowsQueuedObjectWrite(t *testing.T) {
	for attempt := 0; ; attempt++ {
		day := time.Now().UTC()
		err := permanentRemovalAfterQueuedWrite(t, day)
		if attempt == 0 && storage.JavaDate(time.Now().UnixMilli(), time.UTC) != storage.JavaDate(day.UnixMilli(), time.UTC) {
			continue // midnight passed: the removal may have used the next day
		}
		if err != nil {
			t.Fatal(err)
		}
		return
	}
}

func permanentRemovalAfterQueuedWrite(t *testing.T, day time.Time) error {
	now := time.Date(day.Year(), day.Month(), day.Day(), 12, 3, 0, 0, time.UTC)
	date := storage.JavaDate(now.UnixMilli(), time.UTC)
	e, s, _ := testEngine(t, &now, 8)
	release, entered := blockWritersSignalled(t, e, writeObject)
	ctx := context.Background()
	if err := e.Accept(ctx, objectPack("/a", "1")); err != nil {
		return err
	}
	waitEntered(t, entered, writeObject) // the object write is in progress (blocked)
	if err := e.RemoveObjects(ctx, []int32{storage.CounterHash("/a")}, true); err != nil {
		return fmt.Errorf("removal must not wait for the blocked writer: %w", err)
	}
	if e.ObjectCount() != 0 {
		return errors.New("not removed from the live map")
	}
	if err := acceptWithin(e, &protocol.XLogPack{TxID: 1, Service: 1, ObjHash: 1, Elapsed: 1, TailVersion: 12}); err != nil {
		return fmt.Errorf("ingest stalled: %w", err)
	}
	if w := writerStats(e, writeObject); w.Pending != 2 {
		return fmt.Errorf("write and removal queued: %+v", w)
	}
	release()
	flush(t, e)
	if out, err := s.Objects(date); err != nil || len(out) != 0 {
		return fmt.Errorf("removed object written back %v %v", out, err)
	}
	if w := writerStats(e, writeObject); w.Written != 2 || w.WriteErrors != 0 {
		return fmt.Errorf("%+v", w)
	}
	return nil
}

// Periodic passes meeting in one queue in the same tick (group save and 5m
// samples in the daily queue) are not dropped by the byte bound, even with
// the queue full of Agent records; a second pass of one producer while the
// first is still queued is dropped and counted (a stalled writer does not
// pile passes up). Records the daily format cannot hold are counted apart
// from I/O errors, and an unknown time type is refused at accept.
func TestPassesShareTickWithoutByteDrop(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 59, 0, time.UTC)
	e, _, c := testEngine(t, &now, 8)
	release, entered := blockWritersSignalled(t, e, writeDaily)
	ctx := context.Background()
	big := strings.Repeat("x", 3<<20)
	filler := func() *protocol.PerfCounterPack { // a Text daily value: refused by the writer (WriteInvalid)
		return &protocol.PerfCounterPack{ObjName: "/f", Time: now.UnixMilli(), TimeType: 3, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "big", Value: protocol.Value{Type: protocol.ValueText, Text: big}}}}}
	}
	if err := e.Accept(ctx, filler()); err != nil {
		t.Fatal(err)
	}
	waitEntered(t, entered, writeDaily)
	for writerStats(e, writeDaily).Dropped == 0 {
		if err := e.Accept(ctx, filler()); err != nil {
			t.Fatal(err)
		}
	}
	// Then small records until the slack is below one of them (< 1 KiB).
	small := &protocol.PerfCounterPack{ObjName: "/f", Time: now.UnixMilli(), TimeType: 3, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "s", Value: protocol.Value{Type: protocol.ValueText}}}}}
	for dropped := writerStats(e, writeDaily).Dropped; writerStats(e, writeDaily).Dropped == dropped; {
		if err := e.Accept(ctx, small); err != nil {
			t.Fatal(err)
		}
	}
	queued := writerStats(e, writeDaily)
	// Passes of a few KiB each: 20 group meters (60 rows), 52 samples.
	for g := int32(1); g <= 20; g++ {
		if err := e.processGroup(ctx, &protocol.XLogPack{Group: 7 * g, ObjHash: 1, Elapsed: 5}, now.UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	wide := counterPack("/a", now, 1, "TPS", 41)
	for i := 0; i < 49; i++ {
		wide.Data.Entries = append(wide.Data.Entries, protocol.Entry{Key: "k" + strings.Repeat("x", i), Value: protocol.Value{Type: protocol.ValueDecimal, Int: int64(i)}})
	}
	if err := e.Accept(ctx, wide); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second) // 12:01:00: group save due; the runtime runs Sample5M in the same tick
	if err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.Sample5M(ctx); err != nil {
		t.Fatal(err)
	}
	if w := writerStats(e, writeDaily); w.Dropped != queued.Dropped || e.Stats().Sample5MDropped != 0 {
		t.Fatalf("a pass was dropped by the byte bound: %+v", w)
	}
	// A second sample pass while the first is still queued is dropped.
	if err := e.Accept(ctx, counterPack("/b", now, 1, "TPS", 5)); err != nil {
		t.Fatal(err)
	}
	if err := e.Sample5M(ctx); err != nil {
		t.Fatal(err)
	}
	if st := e.Stats(); st.Sample5MDropped != 3 { // TPS, objHash, time of /b
		t.Fatalf("second pass: %+v", st)
	}
	if err := e.Accept(ctx, counterPack("/a", now, 9, "TPS", 1)); !errors.Is(err, protocol.ErrUnsupported) {
		t.Fatal("unknown time type accepted:", err)
	}
	release()
	flush(t, e)
	values, err := c.Daily("20261021", 1, "cnt:7", 3)
	if err != nil || values[144].Int != 1 {
		t.Fatal("group save", values, err)
	}
	values, err = c.Daily("20261021", storage.CounterHash("/a"), "TPS", 3)
	if err != nil || values[144].Int != 41 {
		t.Fatal("5m sample", values, err)
	}
	w := writerStats(e, writeDaily)
	if w.WriteInvalid != queued.Enqueued || w.WriteErrors != 0 || w.Enqueued != w.Written+w.WriteInvalid || w.Pending != 0 {
		t.Fatalf("%+v (fillers %+v)", w, queued)
	}
	if st := e.Stats(); st.WriteInvalid != w.WriteInvalid {
		t.Fatalf("%+v", st)
	}
}

// An object record that was not written (here: refused by a full object
// queue) is written by the next unchanged heartbeat instead of waiting for a
// change or the next day, without counting a change (no MAYBE_OBJ_DUP).
func TestUnsavedObjectRewrittenByNextHeartbeat(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 3, 0, 0, time.UTC)
	e, s, _ := testEngine(t, &now, 8)
	release, entered := blockWritersSignalled(t, e, writeObject)
	ctx := context.Background()
	half := strings.Repeat("y", (4<<20)-300)
	filler := &protocol.ObjectPack{ObjName: "/filler", ObjType: "java", Tags: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{
		{Key: "a", Value: protocol.Value{Type: protocol.ValueText, Text: half}}, {Key: "b", Value: protocol.Value{Type: protocol.ValueText, Text: half}}}}}
	if err := e.Accept(ctx, filler); err != nil { // fills the 8 MiB object queue alone
		t.Fatal(err)
	}
	waitEntered(t, entered, writeObject)
	if err := e.Accept(ctx, objectPack("/a", "1")); err != nil {
		t.Fatal(err)
	}
	if w := writerStats(e, writeObject); w.Dropped != 1 {
		t.Fatalf("/a not refused: %+v", w)
	}
	release()
	flush(t, e)
	if out, err := s.Objects("20261021"); err != nil || len(out) != 0 { // the filler exceeds the 4 KiB slot
		t.Fatal(out, err)
	}
	now = now.Add(2 * time.Second)
	if err := e.Accept(ctx, objectPack("/a", "1")); err != nil { // unchanged heartbeat
		t.Fatal(err)
	}
	flush(t, e)
	out, err := s.Objects("20261021")
	if err != nil || len(out) != 1 || out[0].ObjName != "/a" || out[0].Wakeup != now.UnixMilli() {
		t.Fatal("unsaved object not rewritten", out, err)
	}
	if st := e.objState[storage.CounterHash("/a")]; st.updated != 0 || st.unsaved.Load() {
		t.Fatal("rewrite counted as a change", st.updated, st.unsaved.Load())
	}
	// Written once: the next unchanged heartbeat takes the fast path again.
	now = now.Add(2 * time.Second)
	if err := e.Accept(ctx, objectPack("/a", "1")); err != nil {
		t.Fatal(err)
	}
	flush(t, e)
	if w := writerStats(e, writeObject); w.Written != 1 || w.WriteInvalid != 1 || w.Enqueued != 2 {
		t.Fatalf("%+v", w)
	}
}
