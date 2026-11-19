package storage

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// visitorBudgetFixture offers visits to a Store and keeps, per snapshot file,
// the HLL of every visit (the unbounded HLL the files must equal).
type visitorBudgetFixture struct {
	t    *testing.T
	s    *Store
	dir  string
	want map[string]*visitorHLL
	hour string
}

func newVisitorBudgetFixture(t *testing.T, tune func(v *visitorState)) *visitorBudgetFixture {
	dir := t.TempDir()
	return &visitorBudgetFixture{t: t, s: openVisitorStore(t, dir, tune), dir: dir, want: map[string]*visitorHLL{},
		hour: strconv.FormatInt(JavaMillis(visitorTestNow, time.UTC)/3600000, 10)}
}

// newVisitorBudgetFixtureFlushing is newVisitorBudgetFixture with the flush
// run by the visit that finds it due (DeferVisitorFlush false).
func newVisitorBudgetFixtureFlushing(t *testing.T) *visitorBudgetFixture {
	dir := t.TempDir()
	s, err := Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return &visitorBudgetFixture{t: t, s: s, dir: dir, want: map[string]*visitorHLL{},
		hour: strconv.FormatInt(JavaMillis(visitorTestNow, time.UTC)/3600000, 10)}
}

// set calls SetVisitorBudgets and the roll tick that applies it.
func (f *visitorBudgetFixture) set(residentMB, deltaMB int) {
	f.t.Helper()
	f.s.SetVisitorBudgets(residentMB, deltaMB)
	if err := f.s.VisitorRollContext(context.Background(), visitorTestNow); err != nil {
		f.t.Fatal(err)
	}
}

func (f *visitorBudgetFixture) visit(u int64, o int32) {
	f.t.Helper()
	if err := f.s.PutVisitorContext(context.Background(), visitorTestNow, "java", u, o); err != nil {
		f.t.Fatal(err)
	}
	for _, name := range []string{filepath.Join("visit", "java"), filepath.Join("visit", aggregationHash(o)),
		filepath.Join("visit_hourly", aggregationHash(o)+"_"+f.hour)} {
		if f.want[name] == nil {
			f.want[name] = newVisitorHLL()
		}
		f.want[name].offer(u)
	}
}

// visits offers n new users round robin over objects 1..objects, from user
// *next on.
func (f *visitorBudgetFixture) visits(next *int64, n int, objects int32) {
	f.t.Helper()
	for end := *next + int64(n); *next < end; *next++ {
		f.visit(*next*7919, int32(*next%int64(objects))+1)
	}
}

func (f *visitorBudgetFixture) flush() {
	f.t.Helper()
	if err := f.s.flushVisitors(context.Background()); err != nil {
		f.t.Fatal(err)
	}
}

// check compares every file with its unbounded HLL and the realtime objType
// count with the visits so far.
func (f *visitorBudgetFixture) check() {
	f.t.Helper()
	f.checkFiles()
	n, err := f.s.VisitorRealtimeContext(context.Background(), visitorTestNow, nil, "java")
	if want := f.want[filepath.Join("visit", "java")].cardinality(); err != nil || n != want {
		f.fatalf("realtime %d, want %d (%v)", n, want, err)
	}
}

func (f *visitorBudgetFixture) checkFiles() {
	f.t.Helper()
	for name, h := range f.want {
		visitorFileEquals(f.t, filepath.Join(f.dir, "20261013", name+".usr"), h)
	}
}

func (f *visitorBudgetFixture) fatalf(format string, args ...any) {
	f.t.Helper()
	f.t.Fatalf(format, args...)
}

// state returns the visitor accounting under visitorMu.
func (f *visitorBudgetFixture) state() (resident, residentMax, delta, deltaMax int64, wait, due bool) {
	f.s.visitorMu.Lock()
	defer f.s.visitorMu.Unlock()
	v := f.s.visitor
	v.checkSets()
	return v.residentBytes, v.residentMax, v.deltaBytes, v.deltaMax, v.demoteWait, v.flushDue
}

// denseResident makes every HLL of objects dense and resident (one flush).
func (f *visitorBudgetFixture) denseResident(next *int64, objects int32) {
	f.t.Helper()
	f.visits(next, int(objects)*(visitorSparseMax+2000), objects)
	f.flush()
	if r, p := f.s.visitorModes(); r != 2*int(objects)+1 || p != 0 {
		f.fatalf("resident %d partial %d, want %d resident", r, p, 2*objects+1)
	}
	if f.want[filepath.Join("visit", aggregationHash(1))].words == nil {
		f.fatalf("fixture did not densify")
	}
}

// Budgets set before the visitor state exists apply when it is created; 0
// restores the defaults. Applied (at the next visit, roll tick, stats read
// or pass), a lower resident budget demotes clean entries at once; a higher one promotes again from the next pass; a delta budget below
// the pending deltas marks the flush due. The files stay those of an
// unbounded HLL throughout.
func TestVisitorSetBudgets(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, DeferVisitorFlush: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetVisitorBudgets(3, 20)
	if st := s.VisitorStats(); st.ResidentMax != 3<<20 || st.DeltaMax != 20<<20 {
		t.Fatalf("before the state %+v", st)
	}
	if err := s.PutVisitorContext(ctx, visitorTestNow, "java", 1, 1); err != nil {
		t.Fatal(err)
	}
	if st := s.VisitorStats(); st.ResidentMax != 3<<20 || st.DeltaMax != 20<<20 || st.Entries != 3 {
		t.Fatalf("created state %+v", st)
	}
	s.SetVisitorBudgets(0, 0)
	if st := s.VisitorStats(); st.ResidentMax != visitorResidentMax || st.DeltaMax != visitorDeltaMax {
		t.Fatalf("defaults %+v", st)
	}

	f := newVisitorBudgetFixture(t, nil)
	next := int64(1)
	const objects = 2
	f.denseResident(&next, objects)
	f.set(1, 0)
	resident, residentMax, delta, _, wait, _ := f.state()
	if resident > residentMax-residentMax/8 || wait || delta > 2*objects*visitorHLLBytes+visitorHLLBytes {
		f.fatalf("clean entries not demoted at once: resident %d of %d, delta %d, wait %v", resident, residentMax, delta, wait)
	}
	f.check()

	// Visits to the demoted entries are (dense) deltas again; a delta budget
	// below them marks the flush due.
	f.visits(&next, objects*(visitorSparseMax+2000), objects)
	f.s.visitorMu.Lock()
	f.s.visitor.flushDue = false
	f.s.visitorMu.Unlock()
	f.set(1, 1)
	if _, _, delta, deltaMax, _, due := f.state(); delta <= deltaMax || !due {
		f.fatalf("delta %d of %d, due %v", delta, deltaMax, due)
	}
	f.flush()
	f.check()

	f.set(0, 0)
	if r, _ := f.s.visitorModes(); r == 2*objects+1 {
		f.fatalf("promoted before a pass")
	}
	f.visits(&next, objects*20, objects) // every daily and hourly entry dirty
	f.flush()
	if r, p := f.s.visitorModes(); r != 2*objects+1 || p != 0 {
		f.fatalf("raised budget: resident %d partial %d", r, p)
	}
	f.check()
}

// Cutting the resident budget far below dirty dense resident HLLs must not
// move them all to the deltas: demoted, each delta is a dense HLL, so the
// deltas would pass the delta budget and every following visit to a partial
// entry would be written through on the ingest path. Demotion stops at the
// delta budget, the rest wait (over the resident budget) for the pass marked
// due, which writes and demotes them; no visit is lost or written through.
// Before demoteWait the cut left about 8MiB of deltas over a 4MiB budget.
// Close while waiting writes every entry.
func TestVisitorBudgetCutWaitsForThePass(t *testing.T) {
	f := newVisitorBudgetFixture(t, nil)
	next := int64(1)
	const objects = 6
	f.denseResident(&next, objects)
	// New users mark (nearly) every page: a demoted entry's delta is dense.
	f.visits(&next, objects*3000, objects)
	if _, _, delta, _, _, _ := f.state(); delta != 0 {
		f.fatalf("deltas %d before the cut", delta)
	}
	f.set(1, 4)
	resident, residentMax, delta, deltaMax, wait, due := f.state()
	if delta > deltaMax {
		f.fatalf("demotion took the deltas to %d past the budget %d", delta, deltaMax)
	}
	if resident <= residentMax || !wait || !due {
		f.fatalf("want dirty entries waiting for the pass: resident %d of %d, wait %v, due %v", resident, residentMax, wait, due)
	}
	// Visits meanwhile neither demote nor write through.
	f.visits(&next, objects*200, objects)
	if _, _, delta, deltaMax, _, _ := f.state(); delta > deltaMax {
		f.fatalf("deltas %d past %d while waiting", delta, deltaMax)
	}
	if st := f.s.VisitorStats(); st.WriteThroughs != 0 {
		f.fatalf("%d visits written through on the ingest path", st.WriteThroughs)
	}
	n, err := f.s.VisitorRealtimeContext(context.Background(), visitorTestNow, nil, "java")
	if want := f.want[filepath.Join("visit", "java")].cardinality(); err != nil || n != want {
		f.fatalf("realtime while waiting %d, want %d (%v)", n, want, err)
	}
	f.flush()
	resident, residentMax, delta, deltaMax, wait, _ = f.state()
	if resident > residentMax || wait || delta > deltaMax {
		f.fatalf("after the pass: resident %d of %d, deltas %d of %d, wait %v", resident, residentMax, delta, deltaMax, wait)
	}
	if st := f.s.VisitorStats(); st.WriteThroughs != 0 {
		f.fatalf("%d visits written through", st.WriteThroughs)
	}
	f.check()

	f.set(0, 0)
	f.visits(&next, objects, objects) // every entry dirty: the pass promotes it
	f.flush()
	f.visits(&next, objects*3000, objects)
	f.set(1, 4)
	if _, _, _, _, wait, _ := f.state(); !wait {
		f.fatalf("second cut did not wait")
	}
	if err := f.s.Close(); err != nil {
		f.fatalf("close while waiting: %v", err)
	}
	f.checkFiles()
}

// SetVisitorBudgets racing visits, passes and realtime reads (run with
// -race): the files and counts stay exact.
func TestVisitorSetBudgetsConcurrent(t *testing.T) {
	f := newVisitorBudgetFixture(t, nil)
	ctx := context.Background()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if i%2 == 0 {
				f.s.SetVisitorBudgets(1, 1)
			} else {
				f.s.SetVisitorBudgets(0, 0)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := f.s.flushVisitors(ctx); err != nil {
				t.Error(err)
				return
			}
			if _, err := f.s.VisitorRealtimeContext(ctx, visitorTestNow, []int32{1, 2}, ""); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	next := int64(1)
	f.visits(&next, 4*(visitorSparseMax+2000), 4)
	close(stop)
	wg.Wait()
	f.flush()
	f.check()
}

// cutToWait makes 6 objects' HLLs dense, resident and dirty and cuts the
// budgets to 1MiB/4MiB, so dirty entries wait for a pass.
func (f *visitorBudgetFixture) cutToWait(next *int64) {
	f.t.Helper()
	const objects = 6
	f.denseResident(next, objects)
	f.visits(next, objects*3000, objects)
	f.set(1, 4)
	if resident, residentMax, _, _, wait, due := f.state(); resident <= residentMax || !wait || !due {
		f.fatalf("not waiting: resident %d of %d, wait %v, due %v", resident, residentMax, wait, due)
	}
}

// SetVisitorBudgets runs on the config reload path, which must not wait for
// a visitor lock holder doing I/O: it only stores the values, and the next
// visit applies them.
func TestVisitorSetBudgetsDoesNotTakeTheVisitorLock(t *testing.T) {
	f := newVisitorBudgetFixture(t, nil)
	f.visit(1, 1)
	f.s.visitorMu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.s.SetVisitorBudgets(2, 32)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		f.s.visitorMu.Unlock()
		t.Fatal("SetVisitorBudgets waited for visitorMu")
	}
	applied := f.s.visitor.residentMax != visitorResidentMax
	f.s.visitorMu.Unlock()
	if applied {
		t.Fatal("applied without visitorMu")
	}
	f.visit(2, 1)
	f.s.visitorMu.Lock()
	resident, delta := f.s.visitor.residentMax, f.s.visitor.deltaMax
	f.s.visitorMu.Unlock()
	if resident != 2<<20 || delta != 32<<20 {
		t.Fatalf("not applied by the next visit: %d %d", resident, delta)
	}
}

// A pass whose writes fail while entries wait does not mark the flush due
// again (that would be a failing pass every second); the 10s tick does, and
// once the writes succeed the wait ends with the files of an unbounded HLL.
// FlushVisitorsIfDue runs only the due pass.
func TestVisitorBudgetWaitAfterFailedPass(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("runs as root: read-only permission bits cannot make the snapshot writes fail")
	}
	f := newVisitorBudgetFixture(t, nil)
	next := int64(1)
	f.cutToWait(&next)
	files := visitorFiles(t, f.dir)
	dirs := []string{filepath.Join(f.dir, "20261013", "visit"), filepath.Join(f.dir, "20261013", "visit_hourly")}
	chmod := func(file, dir os.FileMode) {
		t.Helper()
		for _, p := range files {
			if err := os.Chmod(p, file); err != nil {
				t.Fatal(err)
			}
		}
		for _, d := range dirs {
			if err := os.Chmod(d, dir); err != nil {
				t.Fatal(err)
			}
		}
	}
	chmod(0400, 0500)
	t.Cleanup(func() { chmod(0600, 0700) })
	ctx := context.Background()
	if err := f.s.FlushVisitorsIfDue(ctx); err == nil {
		f.fatalf("writes to read-only snapshots succeeded")
	}
	if _, _, delta, deltaMax, wait, due := f.state(); !wait || due || delta > deltaMax {
		f.fatalf("after a failed pass: wait %v, due %v (want only wait), deltas %d of %d", wait, due, delta, deltaMax)
	}
	chmod(0600, 0700)
	if err := f.s.FlushVisitorsIfDue(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, wait, _ := f.state(); !wait {
		f.fatalf("a pass ran before it was due")
	}
	if err := f.s.VisitorRollContext(ctx, visitorTestNow+10000); err != nil {
		t.Fatal(err)
	}
	if err := f.s.FlushVisitorsIfDue(ctx); err != nil {
		t.Fatal(err)
	}
	if resident, residentMax, _, _, wait, _ := f.state(); resident > residentMax || wait {
		f.fatalf("after the due pass: resident %d of %d, wait %v", resident, residentMax, wait)
	}
	f.check()
}

// While entries wait, a visit past the resident budget does not sort every
// entry again: it demotes only the entry it grew, if its delta fits. Without
// room a resident HLL grows up to dense (the bound: resident entries x dense
// size); with room the visited entries go partial. A visit can also be the
// one that starts the wait (its demotion skips dirty entries).
func TestVisitorBudgetWaitVisitsDemoteTheirEntry(t *testing.T) {
	f := newVisitorBudgetFixture(t, nil)
	next := int64(1)
	const objects = 4
	f.visits(&next, objects*3000, objects) // objects sparse, objType dense
	f.flush()
	if r, p := f.s.visitorModes(); r != 2*objects+1 || p != 0 {
		f.fatalf("resident %d partial %d", r, p)
	}
	f.visits(&next, objects*500, objects)
	f.s.visitorMu.Lock()
	f.s.visitor.residentMax, f.s.visitor.deltaMax, f.s.visitor.flushDue = f.s.visitor.residentBytes, 1, false
	f.s.visitorMu.Unlock()
	f.visits(&next, objects*5, objects) // sparse entries grow past the budget
	if _, _, _, _, wait, due := f.state(); !wait || !due {
		f.fatalf("visit did not start the wait: wait %v due %v", wait, due)
	}
	if r, _ := f.s.visitorModes(); r != 2*objects+1 {
		f.fatalf("dirty entries demoted past the delta budget: resident %d", r)
	}
	before, _, _, _, _, _ := f.state()
	f.visits(&next, objects*6000, objects) // the object HLLs densify
	resident, _, delta, _, wait, _ := f.state()
	if r, _ := f.s.visitorModes(); r != 2*objects+1 || !wait || delta != 0 {
		f.fatalf("without delta room: resident entries %d, wait %v, deltas %d", r, wait, delta)
	}
	if resident <= before || resident > (2*objects+1)*visitorDenseBytes {
		f.fatalf("resident %d (before %d) outside the bound %d", resident, before, (2*objects+1)*visitorDenseBytes)
	}
	f.s.visitorMu.Lock()
	f.s.visitor.deltaMax = 64 << 20
	f.s.visitorMu.Unlock()
	f.visits(&next, objects, objects)
	after, _, _, _, wait, _ := f.state()
	if r, _ := f.s.visitorModes(); r >= 2*objects+1 || after >= resident || !wait {
		f.fatalf("with delta room: resident entries %d, bytes %d (was %d), wait %v", r, after, resident, wait)
	}
	if st := f.s.VisitorStats(); st.WriteThroughs != 0 {
		f.fatalf("%d write-throughs", st.WriteThroughs)
	}
	f.flush()
	if resident, residentMax, _, _, wait, _ := f.state(); resident > residentMax || wait {
		f.fatalf("after the pass: resident %d of %d, wait %v", resident, residentMax, wait)
	}
	f.check()
}

// Without DeferVisitorFlush the visit that finds the flush due runs the pass
// that ends the wait.
func TestVisitorBudgetWaitEndsByVisitFlush(t *testing.T) {
	f := newVisitorBudgetFixtureFlushing(t)
	next := int64(1)
	f.cutToWait(&next)
	f.visit(next*7919, 1)
	if resident, residentMax, _, _, wait, due := f.state(); resident > residentMax || wait || due {
		f.fatalf("after the visit: resident %d of %d, wait %v, due %v", resident, residentMax, wait, due)
	}
	if st := f.s.VisitorStats(); st.WriteThroughs != 0 {
		f.fatalf("%d write-throughs", st.WriteThroughs)
	}
	f.check()
}

// A demotion that skips dirty entries and still ends under the budget (the
// clean ones were enough to pass 7/8 of it, not the margin) waits too: else
// every later visit past the budget would sort and skip the same entries.
// It does not mark the flush due (nothing is over the budget).
func TestVisitorBudgetSkipUnderBudgetWaits(t *testing.T) {
	f := newVisitorBudgetFixture(t, nil)
	next := int64(1)
	const objects = 2
	f.denseResident(&next, objects)
	for i := 0; i < 20; i++ { // java and object 2 dirty, object 1 clean
		f.visit((next+int64(i))*7919, 2)
	}
	f.s.visitorMu.Lock()
	v := f.s.visitor
	v.residentMax, v.deltaMax, v.flushDue = 3*visitorDenseBytes+100000, 1, false
	f.s.demoteVisitorsLocked(true)
	f.s.visitorMu.Unlock()
	resident, residentMax, _, _, wait, due := f.state()
	if resident != 3*visitorDenseBytes || !wait || due {
		f.fatalf("resident %d of %d (want the 3 dirty HLLs), wait %v, due %v", resident, residentMax, wait, due)
	}
	f.flush()
	if _, _, _, _, wait, _ := f.state(); wait {
		f.fatalf("wait after the pass")
	}
	f.check()
}

// The runtime with DeferVisitorFlush calls FlushVisitorsIfDue every second:
// with no visit or roll tick, that one call applies a cut, demotes the clean
// entries, keeps the dirty ones that do not fit the deltas, marks the flush
// due and runs the pass that writes and demotes them.
func TestVisitorBudgetAppliedByFlushVisitorsIfDue(t *testing.T) {
	f := newVisitorBudgetFixture(t, nil)
	next := int64(1)
	const objects = 6
	f.denseResident(&next, objects)
	for i := int64(0); i < 3000; i++ { // java and object 1 dirty, the rest clean
		f.visit((next+i)*7919, 1)
	}
	next += 3000
	f.s.visitorMu.Lock()
	f.s.visitor.flushDue = false
	f.s.visitorMu.Unlock()
	f.s.SetVisitorBudgets(1, 1)
	if _, residentMax, _, _, _, _ := f.state(); residentMax != visitorResidentMax {
		f.fatalf("applied before the visitor state ran: %d", residentMax)
	}
	if err := f.s.FlushVisitorsIfDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	resident, residentMax, delta, deltaMax, wait, due := f.state()
	if residentMax != 1<<20 || deltaMax != 1<<20 || resident > residentMax || wait || due || delta > deltaMax {
		f.fatalf("after one FlushVisitorsIfDue: resident %d of %d, deltas %d of %d, wait %v, due %v", resident, residentMax, delta, deltaMax, wait, due)
	}
	if r, _ := f.s.visitorModes(); r > 1 {
		f.fatalf("%d entries still resident", r)
	}
	f.check()
}
