package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var visitorTestNow = time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()

// openVisitorStore opens a deferred-flush Store with the visitor state set up
// by tune (budgets, caps).
func openVisitorStore(t *testing.T, dir string, tune func(v *visitorState)) *Store {
	t.Helper()
	s, err := Open(Config{Directory: dir, Location: time.UTC, DeferVisitorFlush: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	s.visitorMu.Lock()
	s.visitorInit(visitorTestNow)
	if tune != nil {
		tune(s.visitor)
	}
	s.visitorMu.Unlock()
	return s
}

func visitorHLLOf(users ...int64) *visitorHLL {
	h := newVisitorHLL()
	for _, u := range users {
		h.offer(u)
	}
	return h
}

func visitorFileEquals(t *testing.T, path string, want *visitorHLL) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want.bytes()) {
		t.Fatal(path, "differs from the expected HLL", err)
	}
}

// Resident HLLs that grow (sparse to dense) after promotion stay within the
// budget while visits keep coming, without waiting for a flush; the files
// still equal an unbounded HLL.
func TestVisitorResidentBudgetHeldWhileHLLsGrow(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openVisitorStore(t, dir, func(v *visitorState) { v.residentMax = 2 * visitorDenseBytes })
	const objects = 6
	want := map[int32]*visitorHLL{}
	for o := int32(1); o <= objects; o++ {
		if err := s.PutVisitorContext(ctx, visitorTestNow, "java", int64(o), o); err != nil {
			t.Fatal(err)
		}
		want[o] = visitorHLLOf(int64(o))
	}
	if err := s.flushVisitors(ctx); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.visitorModes(); r == 0 {
		t.Fatal("nothing promoted")
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // a pass running alongside, as the maintenance loop does
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := s.flushVisitors(ctx); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	worst := int64(0)
	// Each object gets more users than a sparse HLL holds (it densifies).
	for u := int64(100); u < 100+objects*(visitorSparseMax+1000); u++ {
		o := int32(u%objects) + 1
		if err := s.PutVisitorContext(ctx, visitorTestNow, "java", u*7919, o); err != nil {
			t.Fatal(err)
		}
		want[o].offer(u * 7919)
		s.visitorMu.Lock()
		worst = max(worst, s.visitor.residentBytes-s.visitor.residentMax)
		s.visitorMu.Unlock()
	}
	close(stop)
	wg.Wait()
	if visitorHLLOf().words != nil || want[1].words == nil {
		t.Fatal("fixture did not densify")
	}
	if worst > 0 {
		t.Fatalf("resident HLLs exceeded the budget by %d bytes between flushes", worst)
	}
	if err := s.flushVisitors(ctx); err != nil {
		t.Fatal(err)
	}
	for o, h := range want {
		visitorFileEquals(t, filepath.Join(dir, "20261013", "visit", aggregationHash(o)+".usr"), h)
	}
}

// Past the object cap the oldest entry is written and dropped; an entry a
// pass is writing is skipped; a victim that cannot be written stays (its
// registers kept) and the next one goes; an hourly key back in the same hour
// merges into its file instead of replacing it.
func TestVisitorObjectCapEviction(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openVisitorStore(t, dir, func(v *visitorState) { v.objectsMax = 2; v.residentMax = 0 })
	visit := func(u int64, o int32) {
		t.Helper()
		if err := s.PutVisitorContext(ctx, visitorTestNow, "java", u, o); err != nil {
			t.Fatal(err)
		}
	}
	daily := func(o int32) string { return filepath.Join(dir, "20261013", "visit", aggregationHash(o)+".usr") }
	hour := strconv.FormatInt(JavaMillis(visitorTestNow, time.UTC)/3600000, 10)
	hourly := func(o int32) string {
		return filepath.Join(dir, "20261013", "visit_hourly", aggregationHash(o)+"_"+hour+".usr")
	}
	inSet := func(o int32) bool {
		s.visitorMu.Lock()
		defer s.visitorMu.Unlock()
		return s.visitor.daily["h:"+aggregationHash(o)] != nil
	}

	visit(1, 1)
	visit(2, 2)
	visit(3, 3) // evicts object 1 (dirty): written first
	if inSet(1) {
		t.Fatal("object 1 not evicted")
	}
	visitorFileEquals(t, daily(1), visitorHLLOf(1))
	visitorFileEquals(t, hourly(1), visitorHLLOf(1))

	// Object 2 is the oldest but a pass is writing it: object 3 goes.
	s.visitorMu.Lock()
	s.visitor.daily["h:"+aggregationHash(2)].writes++
	s.visitorMu.Unlock()
	visit(4, 4)
	s.visitorMu.Lock()
	s.visitor.daily["h:"+aggregationHash(2)].writes--
	s.visitorMu.Unlock()
	if !inSet(2) || inSet(3) {
		t.Fatal("eviction did not skip the entry being written")
	}

	// Object 2's file cannot be written (a directory): it stays, object 4 goes.
	if err := os.MkdirAll(daily(2), 0700); err != nil {
		t.Fatal(err)
	}
	visit(5, 5)
	if !inSet(2) || inSet(4) {
		t.Fatal("failed victim dropped or next victim kept")
	}
	if err := os.Remove(daily(2)); err != nil {
		t.Fatal(err)
	}
	if err := s.flushVisitors(ctx); err != nil {
		t.Fatal(err)
	}
	visitorFileEquals(t, daily(2), visitorHLLOf(2))

	// Object 1 comes back this hour: its hourly file keeps user 1.
	visit(6, 1)
	if err := s.flushVisitors(ctx); err != nil {
		t.Fatal(err)
	}
	visitorFileEquals(t, hourly(1), visitorHLLOf(1, 6))
	visitorFileEquals(t, daily(1), visitorHLLOf(1, 6))
}

// A write that fails (file or directory not writable) keeps the registers;
// the retry gives the same bytes as an unbounded HLL, for partial and
// resident entries, and an hourly first write retried still replaces the
// file from the load path (the source restart behaviour).
func TestVisitorFailedWritesRetryToSameBytes(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission bits do not stop root")
	}
	ctx := context.Background()
	for _, budget := range []int64{0, visitorResidentMax} {
		t.Run(strconv.FormatInt(budget, 10), func(t *testing.T) {
			dir := t.TempDir()
			s := openVisitorStore(t, dir, func(v *visitorState) { v.residentMax = budget })
			path := filepath.Join(dir, "20261013", "visit", "java.usr")
			want := newVisitorHLL()
			put := func(from, to int64) {
				for u := from; u < to; u++ {
					if err := s.PutVisitorContext(ctx, visitorTestNow, "java", u, 42); err != nil {
						t.Fatal(err)
					}
					want.offer(u)
				}
			}
			put(1, 50)
			if err := s.flushVisitors(ctx); err != nil {
				t.Fatal(err)
			}
			put(50, 80)
			if err := os.Chmod(path, 0400); err != nil {
				t.Fatal(err)
			}
			if err := s.flushVisitors(ctx); err == nil {
				t.Fatal("write to a read-only snapshot succeeded")
			}
			if err := os.Chmod(path, 0600); err != nil {
				t.Fatal(err)
			}
			put(80, 90)
			if err := s.flushVisitors(ctx); err != nil {
				t.Fatal(err)
			}
			visitorFileEquals(t, path, want)
		})
	}
	t.Run("hourly first write", func(t *testing.T) {
		dir := t.TempDir()
		hour := strconv.FormatInt(JavaMillis(visitorTestNow, time.UTC)/3600000, 10)
		hdir := filepath.Join(dir, "20261013", "visit_hourly")
		path := filepath.Join(hdir, aggregationHash(42)+"_"+hour+".usr")
		if err := os.MkdirAll(hdir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, visitorHLLOf(100).bytes(), 0600); err != nil { // before a restart
			t.Fatal(err)
		}
		s := openVisitorStore(t, dir, nil)
		if err := s.PutVisitorContext(ctx, visitorTestNow, "java", 1, 42); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(hdir, 0500); err != nil {
			t.Fatal(err)
		}
		err := s.flushVisitors(ctx)
		if cerr := os.Chmod(hdir, 0700); cerr != nil {
			t.Fatal(cerr)
		}
		if err == nil {
			t.Fatal("write into a read-only directory succeeded")
		}
		if err = s.flushVisitors(ctx); err != nil {
			t.Fatal(err)
		}
		visitorFileEquals(t, path, visitorHLLOf(1))
	})
}

// visitorCountdownCtx is cancelled from its n-th Err call on.
type visitorCountdownCtx struct {
	context.Context
	n atomic.Int64
}

func (c *visitorCountdownCtx) Err() error {
	if c.n.Add(-1) < 0 {
		return context.Canceled
	}
	return nil
}

// A pass cancelled at any point (including during a promotion load) keeps
// the registers it had not written; the next pass writes them.
func TestVisitorCancelledPassKeepsRegisters(t *testing.T) {
	for n := int64(0); n < 40; n++ {
		dir := t.TempDir()
		path := filepath.Join(dir, "20261013", "visit", "java.usr")
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, visitorHLLOf(100).bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		s := openVisitorStore(t, dir, nil)
		for u := int64(1); u <= 3; u++ {
			if err := s.PutVisitorContext(context.Background(), visitorTestNow, "java", u, 42); err != nil {
				t.Fatal(err)
			}
		}
		ctx := &visitorCountdownCtx{Context: context.Background()}
		ctx.n.Store(n)
		if err := s.flushVisitors(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(n, err)
		}
		if err := s.flushVisitors(context.Background()); err != nil {
			t.Fatal(n, err)
		}
		visitorFileEquals(t, path, visitorHLLOf(100, 1, 2, 3))
		s.Close()
	}
}

// Passes, evictions, realtime queries and deletion of the day's visitor
// directories run together without a race or an unexpected error.
func TestVisitorPassEvictionDeleteConcurrent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openVisitorStore(t, dir, func(v *visitorState) { v.objectsMax = 8; v.residentMax = 3 * visitorDenseBytes })
	var wg sync.WaitGroup
	stop := make(chan struct{})
	run := func(f func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := f(); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	run(func() error { return s.flushVisitors(ctx) })
	run(func() error {
		_, err := s.VisitorRealtimeContext(ctx, visitorTestNow, []int32{1, 2, 3}, "")
		return err
	})
	run(func() error {
		_, err := s.VisitorRealtimeContext(ctx, visitorTestNow, nil, "java")
		return err
	})
	run(func() error {
		_, err := s.DeleteDatabasePaths(ctx, []string{"/20261013/visit_hourly"})
		return err
	})
	for u := int64(1); u <= 4000; u++ {
		if err := s.PutVisitorContext(ctx, visitorTestNow, "java", u, int32(u%20)+1); err != nil {
			t.Fatal(err)
		}
		if u%500 == 0 {
			if _, err := s.DeleteDatabasePaths(ctx, []string{"/20261013/visit"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	close(stop)
	wg.Wait()
}

// A resident entry whose write failed (unsynced) and is then demoted keeps
// every register: the retry, a whole rewrite from the load path base plus the
// delta, gives the bytes of an unbounded HLL (daily and hourly).
func TestVisitorFailedWriteThenDemotedRetries(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission bits do not stop root")
	}
	ctx := context.Background()
	dir := t.TempDir()
	s := openVisitorStore(t, dir, nil)
	hour := strconv.FormatInt(JavaMillis(visitorTestNow, time.UTC)/3600000, 10)
	daily := filepath.Join(dir, "20261013", "visit", aggregationHash(42)+".usr")
	hourly := filepath.Join(dir, "20261013", "visit_hourly", aggregationHash(42)+"_"+hour+".usr")
	want := newVisitorHLL()
	put := func(from, to int64) {
		for u := from; u < to; u++ {
			if err := s.PutVisitorContext(ctx, visitorTestNow, "java", u, 42); err != nil {
				t.Fatal(err)
			}
			want.offer(u)
		}
	}
	put(1, 40)
	if err := s.flushVisitors(ctx); err != nil { // promoted: resident, synced
		t.Fatal(err)
	}
	put(40, 70)
	for _, p := range []string{daily, hourly} {
		if err := os.Chmod(p, 0400); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.flushVisitors(ctx); err == nil {
		t.Fatal("write to read-only snapshots succeeded")
	}
	s.visitorMu.Lock()
	for _, e := range []*visitorEntry{s.visitor.daily["h:"+aggregationHash(42)], s.visitor.hourly[aggregationHash(42)+"_"+hour]} {
		if e.partial || e.synced || !e.dirty {
			t.Fatal("want a dirty unsynced resident entry", e.partial, e.synced, e.dirty)
		}
	}
	s.visitor.residentMax = 0
	s.demoteVisitorsLocked(true)
	s.visitorMu.Unlock()
	if r, _ := s.visitorModes(); r != 0 {
		t.Fatal("not demoted", r)
	}
	for _, p := range []string{daily, hourly} {
		if err := os.Chmod(p, 0600); err != nil {
			t.Fatal(err)
		}
	}
	put(70, 80)
	if err := s.flushVisitors(ctx); err != nil {
		t.Fatal(err)
	}
	visitorFileEquals(t, daily, want)
	visitorFileEquals(t, hourly, want)
}

// A dense resident HLL demoted with unwritten registers answers realtime
// queries exactly (file plus delta), before and after the next flush.
func TestVisitorDemotedDenseRealtime(t *testing.T) {
	ctx := context.Background()
	s := openVisitorStore(t, t.TempDir(), nil)
	want := newVisitorHLL()
	put := func(from, to int64) {
		for u := from; u < to; u++ {
			if err := s.PutVisitorContext(ctx, visitorTestNow, "java", u*31, 42); err != nil {
				t.Fatal(err)
			}
			want.offer(u * 31)
		}
	}
	check := func(step string) {
		t.Helper()
		n, err := s.VisitorRealtimeContext(ctx, visitorTestNow, []int32{42}, "")
		if err != nil || n != want.cardinality() {
			t.Fatal(step, n, want.cardinality(), err)
		}
	}
	put(1, visitorSparseMax+2000)
	if err := s.flushVisitors(ctx); err != nil {
		t.Fatal(err)
	}
	put(visitorSparseMax+2000, visitorSparseMax+2500) // unwritten registers
	s.visitorMu.Lock()
	e := s.visitor.daily["h:"+aggregationHash(42)]
	if e.partial || e.hll.words == nil || !e.dirty {
		s.visitorMu.Unlock()
		t.Fatal("fixture: want a dirty dense resident entry")
	}
	s.visitor.residentMax = 0
	s.demoteVisitorsLocked(true)
	partial := e.partial
	s.visitorMu.Unlock()
	if !partial {
		t.Fatal("not demoted")
	}
	check("demoted")
	if err := s.flushVisitors(ctx); err != nil {
		t.Fatal(err)
	}
	check("flushed")
}

// A write-through past deltaMax that fails is not an error of the visit: the
// registers stay, VisitorStats counts it, and the next pass returns the error;
// once writable the file gets every visit.
func TestVisitorWriteThroughFailureDeferred(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openVisitorStore(t, dir, func(v *visitorState) { v.residentMax, v.deltaMax = 0, 1 })
	path := filepath.Join(dir, "20261013", "visit", aggregationHash(42)+".usr")
	if err := os.MkdirAll(path, 0700); err != nil { // a directory: every write fails
		t.Fatal(err)
	}
	want := newVisitorHLL()
	for u := int64(1); u <= 20; u++ {
		if err := s.PutVisitorContext(ctx, visitorTestNow, "java", u, 42); err != nil {
			t.Fatal("visit failed on a write-through error", err)
		}
		want.offer(u)
	}
	if st := s.VisitorStats(); st.WriteThroughFailures == 0 || st.Entries == 0 {
		t.Fatalf("%+v", st)
	}
	if err := s.flushVisitors(ctx); err == nil {
		t.Fatal("pass did not report the failed writes")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := s.flushVisitors(ctx); err != nil {
		t.Fatal(err)
	}
	visitorFileEquals(t, path, want)
}

// A pass write that failed while Close ran is retried once after Close:
// writing a snapshot after Close is allowed and its error goes to the pass.
func TestVisitorWriteFailedDuringCloseRetried(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openVisitorStore(t, dir, func(v *visitorState) { v.residentMax = 0 })
	for u := int64(1); u <= 3; u++ {
		if err := s.PutVisitorContext(ctx, visitorTestNow, "java", u, 42); err != nil {
			t.Fatal(err)
		}
	}
	// A pass takes the object's write (as flushVisitors does) and is out
	// writing when Close runs.
	s.visitorMu.Lock()
	key := "h:" + aggregationHash(42)
	w := s.visitorWorkLocked(false, key, s.visitor.daily[key])
	s.visitorIO.Lock()
	snap := s.takeVisitorLocked(w.entry, w.path, w.loadPath, false)
	w.entry.writes++
	s.visitorIO.Unlock()
	s.visitorMu.Unlock()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(w.path); !os.IsNotExist(err) {
		t.Fatal("Close wrote the delta the pass held", err)
	}
	s.visitorMu.Lock()
	err := s.finishVisitorWriteLocked(ctx, w, &snap, nil, errors.New("disk"))
	s.visitorMu.Unlock()
	if err != nil {
		t.Fatal("retry after Close", err)
	}
	visitorFileEquals(t, w.path, visitorHLLOf(1, 2, 3))
}
