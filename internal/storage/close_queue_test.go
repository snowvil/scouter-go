package storage

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

// slowDisk models a slow fsync (macOS F_FULLFSYNC takes tens to hundreds of
// ms): every data file fsync sleeps for delay while enabled.
type slowDisk struct {
	enabled atomic.Bool
	calls   atomic.Int64
	delay   time.Duration
}

func (d *slowDisk) sync(f *os.File) error {
	if d.enabled.Load() {
		d.calls.Add(1)
		time.Sleep(d.delay)
	}
	return f.Sync()
}

func xlogDay(day int) int64 {
	return time.Date(2026, 9, 1, 1, 0, 0, 0, time.UTC).AddDate(0, 0, day).UnixMilli()
}

// fillIndexes opens xlog indexes for consecutive days until the index cache
// is full (32) and returns the number of days written.
func fillIndexes(t *testing.T, s *Store) int {
	t.Helper()
	days := 0
	for len(s.indexes) < 32 {
		if err := s.PutXLog(&protocol.XLogPack{EndTime: xlogDay(days), TxID: int64(days + 1)}); err != nil {
			t.Fatal(err)
		}
		days++
	}
	return days
}

// An eviction while SyncData fsyncs outside the gate must not hold the gate
// until that pass ends: the victim leaves the cache at once and SyncData
// closes it afterwards. The deferred close still writes the victim's header,
// so its records stay reachable through the saved header alone.
func TestEvictionDuringSlowSyncDoesNotStallGate(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	disk := &slowDisk{delay: 40 * time.Millisecond}
	s.closes.fsync = disk.sync
	days := fillIndexes(t, s)
	victimDate := JavaDate(xlogDay(0), time.UTC)
	victim := s.indexes[filepath.Join(dir, victimDate, "xlog", "xlog_tid")]
	if victim == nil {
		t.Fatal("victim index not cached")
	}
	files := len(s.files)
	if files < 8 {
		t.Fatal("too few data files for a measurable pass", files)
	}
	disk.enabled.Store(true)
	syncDone := make(chan error, 1)
	go func() { syncDone <- s.SyncData() }()
	// Wait until the pass is fsyncing outside the gate.
	for deadline := time.Now().Add(5 * time.Second); ; {
		s.mu.Lock()
		syncing := s.closes.syncing
		s.mu.Unlock()
		if syncing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("SyncData never began its pass")
		}
		time.Sleep(time.Millisecond)
	}
	// Open a new day: evicts the least recently used xlog indexes of day 0.
	start := time.Now()
	if err = s.PutXLog(&protocol.XLogPack{EndTime: xlogDay(days), TxID: int64(days + 1)}); err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	passAtLeast := time.Duration(files) * disk.delay
	t.Logf("eviction put took %v while the sync pass takes >= %v", took, passAtLeast)
	if took > passAtLeast/2 {
		t.Fatalf("eviction waited for the sync pass: put took %v, pass >= %v", took, passAtLeast)
	}
	s.mu.Lock()
	pending := len(s.closes.pending)
	_, cached := s.indexes[filepath.Join(dir, victimDate, "xlog", "xlog_tid")]
	s.mu.Unlock()
	if pending == 0 || cached {
		t.Fatalf("victim not deferred: pending=%d cached=%v", pending, cached)
	}
	if err = <-syncDone; err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	pending, closing := len(s.closes.pending), s.closes.closing
	s.mu.Unlock()
	if pending != 0 || closing != nil {
		t.Fatalf("deferred closes not drained: pending=%d closing=%v", pending, closing)
	}
	if _, err = victim.Get(longKey(1)); !errors.Is(err, legacyindex.ErrClosed) {
		t.Fatal("victim index still open after drain", err)
	}
	if disk.calls.Load() < int64(files) {
		t.Fatal("slow fsync seam not exercised", disk.calls.Load())
	}
	// The victim's header alone (no Close of the Store yet) reaches its record.
	ro, err := legacyindex.OpenReadOnly(filepath.Join(dir, victimDate, "xlog", "xlog_tid"), false, nil)
	if err != nil {
		t.Fatal(err)
	}
	v, err := ro.Get(longKey(1))
	ro.Close()
	if err != nil || v == nil {
		t.Fatal("deferred close lost the victim header", v, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}

// A path evicted during a pass and requested again before the pass ends
// re-adopts the open handle: no second handle, and the old handle's final
// header cannot overwrite records written through a newer one.
func TestEvictedHandleReadoptedDuringSync(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	disk := &slowDisk{delay: 20 * time.Millisecond}
	s.closes.fsync = disk.sync
	days := fillIndexes(t, s)
	victimPath := filepath.Join(dir, JavaDate(xlogDay(0), time.UTC), "xlog", "xlog_tid")
	victim := s.indexes[victimPath]
	disk.enabled.Store(true)
	syncDone := make(chan error, 1)
	go func() { syncDone <- s.SyncData() }()
	for {
		s.mu.Lock()
		syncing := s.closes.syncing
		s.mu.Unlock()
		if syncing {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err = s.PutXLog(&protocol.XLogPack{EndTime: xlogDay(days), TxID: int64(days + 1)}); err != nil {
		t.Fatal(err)
	}
	if _, cached := s.indexes[victimPath]; cached {
		t.Fatal("day 0 was not evicted")
	}
	// Day 0 again while the pass runs: the queued handle comes back.
	if err = s.PutXLog(&protocol.XLogPack{EndTime: xlogDay(0), TxID: 1000}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	readopted := s.indexes[victimPath] == victim
	over := len(s.indexes) > 32
	s.mu.Unlock()
	if !readopted || over {
		t.Fatalf("readopted=%v over=%v", readopted, over)
	}
	if err = <-syncDone; err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := legacyindex.OpenReadOnly(victimPath, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	for _, tx := range []int64{1, 1000} {
		if v, err := ro.Get(longKey(tx)); err != nil || v == nil {
			t.Fatal("record lost across readoption", tx, v, err)
		}
	}
}

// A path whose deferred close is running right now waits for exactly that
// close, then opens afresh (never two handles on one path, never a stale
// header after a newer one).
func TestOpenWaitsForRunningDeferredClose(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	disk := &slowDisk{delay: 150 * time.Millisecond}
	s.closes.fsync = disk.sync
	path := filepath.Join(dir, "a.data")
	s.mu.Lock()
	if _, err = s.appendData(path, []byte{1}, 4); err != nil {
		t.Fatal(err)
	}
	old := s.files[path]
	// Model an eviction during a pass: the close is queued, not run.
	s.closes.begin()
	for len(s.files) > 0 {
		if err = s.evictFileLocked(); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.closes.pending) != 1 {
		t.Fatal(len(s.closes.pending))
	}
	s.mu.Unlock()
	disk.enabled.Store(true)
	drained := make(chan error, 1)
	go func() { drained <- s.closes.drain(&s.mu) }()
	for {
		s.mu.Lock()
		running := s.closes.closing != nil
		s.mu.Unlock()
		if running {
			break
		}
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	s.mu.Lock()
	f, err := s.file(path, false)
	waited := time.Since(start)
	s.mu.Unlock()
	if err != nil || f == nil || f == old {
		t.Fatal("expected a fresh handle", f == old, err)
	}
	if waited < disk.delay/2 {
		t.Fatal("open did not wait for the running close", waited)
	}
	if err = <-drained; err != nil {
		t.Fatal(err)
	}
	if _, err = old.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("old handle still open", err)
	}
	if _, err = f.Stat(); err != nil {
		t.Fatal("fresh handle unusable", err)
	}
}

// Beyond the queue bound an eviction closes in place even during a pass; the
// pass tolerates the closed handle because that close fsynced it.
func TestCloseQueueBoundClosesInPlace(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	disk := &slowDisk{delay: 10 * time.Millisecond}
	s.closes.fsync = disk.sync
	fillIndexes(t, s)
	disk.enabled.Store(true)
	syncDone := make(chan error, 1)
	go func() { syncDone <- s.SyncData() }()
	for {
		s.mu.Lock()
		syncing := s.closes.syncing
		s.mu.Unlock()
		if syncing {
			break
		}
		time.Sleep(time.Millisecond)
	}
	// Many new days during one pass: 32 closes are queued, the rest run in place.
	for day := 100; day < 100+maxPendingCloses; day++ {
		if err = s.PutXLog(&protocol.XLogPack{EndTime: xlogDay(day), TxID: int64(day)}); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.Lock()
	pending := len(s.closes.pending)
	s.mu.Unlock()
	if pending > maxPendingCloses {
		t.Fatal("queue unbounded", pending)
	}
	if err = <-syncDone; err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for day := 100; day < 100+maxPendingCloses; day++ {
		if x, err := s.XLog(JavaDate(xlogDay(day), time.UTC), int64(day)); err != nil || x == nil {
			t.Fatal(day, x, err)
		}
	}
}

// Counters: a table evicted during a slow Sync is closed after the pass, not
// under the gate while the pass runs.
func TestCounterEvictionDuringSlowSyncDoesNotStallGate(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCounters(dir, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	disk := &slowDisk{delay: 60 * time.Millisecond}
	c.closes.fsync = disk.sync
	put := func(day int, v int64) {
		t.Helper()
		if err := c.Put(&protocol.PerfCounterPack{Time: xlogDay(day), ObjName: "/a", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: v}}}}}); err != nil {
			t.Fatal(err)
		}
	}
	for day := 0; day < 8; day++ {
		put(day, int64(day))
	}
	if len(c.tables) != 8 {
		t.Fatal(len(c.tables))
	}
	disk.enabled.Store(true)
	syncDone := make(chan error, 1)
	go func() { syncDone <- c.Sync() }()
	for {
		c.mu.Lock()
		syncing := c.closes.syncing
		c.mu.Unlock()
		if syncing {
			break
		}
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	put(8, 8) // evicts the oldest table while 8 data fsyncs (>= 480ms) run
	if took := time.Since(start); took > 4*disk.delay {
		t.Fatalf("counter eviction waited for the sync pass: %v", took)
	}
	if err = <-syncDone; err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	pending := len(c.closes.pending)
	c.mu.Unlock()
	if pending != 0 {
		t.Fatal(pending)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = OpenCounters(dir, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for day := 0; day < 9; day++ {
		pts, err := c.Realtime(JavaDate(xlogDay(day), time.UTC), CounterHash("/a"), "TPS", xlogDay(day)-1, xlogDay(day)+1)
		if err != nil || len(pts) != 1 || pts[0].Value.Int != int64(day) {
			t.Fatal(day, pts, err)
		}
	}
}
