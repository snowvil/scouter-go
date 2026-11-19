package storage

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// visitorFiles lists the .usr snapshots below dir.
func visitorFiles(t testing.TB, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == ".usr" {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

type visitorScale struct {
	objects, rounds       int
	residentMax, deltaMax int64
	shrink                int64 // residentMax from the last two rounds on, if set
	residentBeforeShrink  *int
}

// run offers rounds of one new user per object, flushing after every round
// past the first two, and checks that every file equals an HLL offered every
// visit directly (as if the cache were unbounded and never written).
func (c visitorScale) run(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	s, err := Open(Config{Directory: dir, Location: time.UTC, DeferVisitorFlush: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	s.visitorMu.Lock()
	s.visitorInit(now)
	if c.residentMax >= 0 {
		s.visitor.residentMax = c.residentMax
	}
	if c.deltaMax > 0 {
		s.visitor.deltaMax = c.deltaMax
	}
	s.visitorMu.Unlock()
	want := map[string]*visitorHLL{}
	expect := func(name string, u int64) {
		h := want[name]
		if h == nil {
			h = newVisitorHLL()
			want[name] = h
		}
		h.offer(u)
	}
	hour := strconv.FormatInt(JavaMillis(now, time.UTC)/3600000, 10)
	for r := 0; r < c.rounds; r++ {
		if c.shrink > 0 && r == c.rounds-2 {
			*c.residentBeforeShrink, _ = s.visitorModes()
			s.visitorMu.Lock()
			s.visitor.residentMax = c.shrink
			s.visitorMu.Unlock()
		}
		for o := int32(1); o <= int32(c.objects); o++ {
			// Some users repeat, so some visits change no register.
			u := int64(r%3)*int64(c.objects) + int64(o)*int64(r+1)
			if err := s.PutVisitorContext(ctx, now, "java", u, o); err != nil {
				t.Fatal(err)
			}
			expect(filepath.Join("visit", "java"), u)
			expect(filepath.Join("visit", aggregationHash(o)), u)
			expect(filepath.Join("visit_hourly", aggregationHash(o)+"_"+hour), u)
		}
		if r == 1 && c.deltaMax == 0 {
			if files := visitorFiles(t, dir); len(files) != 0 {
				t.Fatalf("ingest path wrote %d snapshots before the flush (e.g. %s)", len(files), files[0])
			}
		}
		if r >= 1 {
			if err = s.flushVisitors(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	files := visitorFiles(t, dir)
	if len(files) != len(want) {
		t.Fatalf("files %d, want %d", len(files), len(want))
	}
	for name, h := range want {
		got, err := os.ReadFile(filepath.Join(dir, "20261013", name+".usr"))
		if err != nil || !bytes.Equal(got, h.bytes()) {
			t.Fatal(name, "file differs from the unbounded HLL", err)
		}
	}
	hashes := make([]int32, 0, 64)
	for o := int32(1); o <= 64; o++ {
		hashes = append(hashes, o)
	}
	total := newVisitorHLL()
	for _, o := range hashes {
		total.merge(want[filepath.Join("visit", aggregationHash(o))])
	}
	n, err := s.VisitorRealtimeContext(ctx, now, hashes, "")
	if err != nil || n != total.cardinality() {
		t.Fatal("realtime", n, total.cardinality(), err)
	}
	n, err = s.VisitorRealtimeContext(ctx, now, nil, "java")
	if err != nil || n != want[filepath.Join("visit", "java")].cardinality() {
		t.Fatal("realtime objType", n, err)
	}
	return s
}

func (s *Store) visitorModes() (resident, partial int) {
	s.visitorMu.Lock()
	defer s.visitorMu.Unlock()
	for _, set := range [...]map[string]*visitorEntry{s.visitor.daily, s.visitor.hourly} {
		for _, e := range set {
			if e.partial {
				partial++
			} else {
				resident++
			}
		}
	}
	return resident, partial
}

// Thousands of objects with user ids (past the Java 500 object entries) keep
// every visit in memory: the ingest path writes and reads no snapshot, and
// the flush writes files equal to an unbounded HLL. Within the resident
// budget every entry ends resident.
func TestVisitorManyObjectsNoIngestIOAndExactFiles(t *testing.T) {
	s := visitorScale{objects: 4000, rounds: 3, residentMax: -1}.run(t)
	if r, p := s.visitorModes(); p != 0 || r != 2*4000+1 {
		t.Fatal("resident", r, "partial", p)
	}
}

// Past the resident budget the same files come from partial entries merged
// into the files (budget 0) or from a mix where the flush demotes the least
// recently used resident entries; past deltaMax visits are written through.
func TestVisitorBudgetsGiveSameFiles(t *testing.T) {
	t.Run("partial", func(t *testing.T) {
		s := visitorScale{objects: 700, rounds: 4, residentMax: 0}.run(t)
		if r, _ := s.visitorModes(); r != 0 {
			t.Fatal("resident past a zero budget", r)
		}
	})
	t.Run("demote", func(t *testing.T) {
		// Promotion stops at the budget (it reserves a dense HLL); the
		// shrunk budget makes the flush demote.
		var before int
		s := visitorScale{objects: 700, rounds: 5, residentMax: visitorDenseBytes + 60000, shrink: 30000, residentBeforeShrink: &before}.run(t)
		r, p := s.visitorModes()
		if before == 0 || r >= before || p == 0 {
			t.Fatal("want demotion: resident before", before, "after", r, "partial", p)
		}
		s.visitorMu.Lock()
		over := s.visitor.residentBytes > s.visitor.residentMax
		s.visitorMu.Unlock()
		if over {
			t.Fatal("resident HLLs over the budget after a flush")
		}
	})
	t.Run("write-through", func(t *testing.T) {
		visitorScale{objects: 700, rounds: 4, residentMax: 0, deltaMax: 4096}.run(t)
	})
}

// A partial entry over a file that is not a full snapshot keeps its registers
// (ErrCorrupt, never overwritten) and the pass still writes the others.
func TestVisitorPartialCorruptFileKeepsDelta(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	s, err := Open(Config{Directory: dir, Location: time.UTC, DeferVisitorFlush: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.visitorMu.Lock()
	s.visitorInit(now)
	s.visitor.residentMax = 0
	s.visitorMu.Unlock()
	bad := filepath.Join(dir, "20261013", "visit", aggregationHash(7)+".usr")
	if err = os.MkdirAll(filepath.Dir(bad), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(bad, []byte("short"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, o := range []int32{7, 8} {
		if err = s.PutVisitorContext(ctx, now, "java", 1, o); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.flushVisitors(ctx); !errors.Is(err, ErrCorrupt) {
		t.Fatal("want ErrCorrupt", err)
	}
	if b, _ := os.ReadFile(bad); string(b) != "short" {
		t.Fatal("corrupt snapshot overwritten")
	}
	if _, err = os.Stat(filepath.Join(dir, "20261013", "visit", aggregationHash(8)+".usr")); err != nil {
		t.Fatal("other entry not written", err)
	}
	s.visitorMu.Lock()
	e := s.visitor.daily["h:"+aggregationHash(7)]
	kept := e.dirty && e.hll.word(0) == 0 && len(e.hll.buckets()) == 1
	s.visitorMu.Unlock()
	if !kept {
		t.Fatal("delta not kept for the retry")
	}
}

// Realtime counts stay exact while flushes write partial entries and visits
// keep arriving (run with -race).
func TestVisitorRealtimeDuringFlush(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, DeferVisitorFlush: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.visitorMu.Lock()
	s.visitorInit(now)
	s.visitor.residentMax = 2 * visitorDenseBytes
	s.visitorMu.Unlock()
	const objects = 50
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20; i++ {
			if err := s.flushVisitors(ctx); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	want := newVisitorHLL()
	hashes := []int32{1, 2, 3}
	for u := int64(1); u <= 3000; u++ {
		o := int32(u%objects) + 1
		if err := s.PutVisitorContext(ctx, now, "java", u, o); err != nil {
			t.Fatal(err)
		}
		if o <= 3 {
			want.offer(u)
		}
		if u%100 == 0 {
			n, err := s.VisitorRealtimeContext(ctx, now, hashes, "")
			if err != nil || n != want.cardinality() {
				t.Fatal("realtime during flush", u, n, want.cardinality(), err)
			}
		}
	}
	<-done
}

// benchVisitor offers b.N visits round robin over objects, each from a new
// user, with the deferred flush of the runtime run every 10s of visit time.
func benchVisitor(b *testing.B, objects int) {
	ctx := context.Background()
	dir := b.TempDir()
	s, err := Open(Config{Directory: dir, Location: time.UTC, DeferVisitorFlush: true})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	now := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	// Warm up: every object seen once and flushed, as in a running server.
	for o := 1; o <= objects; o++ {
		if err := s.PutVisitorContext(ctx, now, "java", int64(o), int32(o)); err != nil {
			b.Fatal(err)
		}
	}
	if err := s.flushVisitors(ctx); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	cpu0 := processCPU(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm := now + int64(i/5) // 5 visits per ms of visit time
		if err := s.PutVisitorContext(ctx, tm, "java", int64(objects+i), int32(i%objects+1)); err != nil {
			b.Fatal(err)
		}
		if err := s.FlushVisitorsIfDue(ctx); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer() // Close (deferred) writes every dirty entry
	// Wall time includes waiting in file syscalls; this is the process CPU.
	b.ReportMetric(float64(processCPU(b)-cpu0)/float64(b.N), "cpu-ns/op")
}

// processCPU is the user plus system CPU time of the process so far.
func processCPU(b *testing.B) time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		b.Fatal(err)
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func BenchmarkVisitorObjects200(b *testing.B)  { benchVisitor(b, 200) }
func BenchmarkVisitorObjects4000(b *testing.B) { benchVisitor(b, 4000) }

// raiseVisitorFile gives the bytes of the in-memory merge for sparse and dense
// deltas, and reports a missing file without creating it.
func TestRaiseVisitorFileMatchesMerge(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.usr")
	page := make([]byte, visitorPageBytes)
	if ok, err := raiseVisitorFile(path, newVisitorHLL(), page); ok || err != nil {
		t.Fatal("missing file", ok, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("missing file created", err)
	}
	for _, users := range []int64{50, 3 * visitorSparseMax} {
		base, delta := newVisitorHLL(), newVisitorHLL()
		for u := int64(1); u <= 2000; u++ {
			base.offer(u)
		}
		for u := int64(1000); u < 1000+users; u++ {
			delta.offer(u * 7)
		}
		if (delta.words != nil) != (users > visitorSparseMax) {
			t.Fatal("delta representation", users)
		}
		if err := os.WriteFile(path, base.bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		if ok, err := raiseVisitorFile(path, delta, page); !ok || err != nil {
			t.Fatal(ok, err)
		}
		base.merge(delta)
		if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, base.bytes()) {
			t.Fatal(users, "raised file differs from the merge", err)
		}
	}
}

func TestVisitorFIFOOrderAcrossCompaction(t *testing.T) {
	var q visitorFIFO
	next, want := uint64(0), uint64(0)
	for round := 0; round < 50; round++ {
		for i := 0; i < 7; i++ {
			next++
			q.push(visitorRef{strconv.FormatUint(next, 10), next})
		}
		for i := 0; i < 5; i++ {
			want++
			if r := q.pop(); r.sequence != want || r.key != strconv.FormatUint(want, 10) {
				t.Fatal("order", r, want)
			}
		}
	}
	if q.len() != int(next-want) || len(q.items) > 2*q.len()+8 {
		t.Fatal("len", q.len(), "items", len(q.items))
	}
}
