package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

// dropRuntimeState removes the Go runtime files (clean marker, checkpoint
// logs) from a treeBytes map: they are not DB content.
func dropRuntimeState(tree map[string][]byte) {
	delete(tree, cleanMarkerName)
	delete(tree, checkpointName)
	delete(tree, counterCheckpointName)
}

// dropCheckpointLogs simulates a crash before any checkpoint was persisted.
func dropCheckpointLogs(t *testing.T, dir string) {
	t.Helper()
	for _, name := range []string{checkpointName, counterCheckpointName} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
}

func readCheckpointLog(t *testing.T, dir, name string) checkpointFile {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	var m checkpointFile
	if err = json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// (a) S4: a crash after SyncData (SIGKILL: no Close, the process state is
// dropped) restarts by validating only the records appended after the last
// checkpoint, with the outcome of the full walk.
func TestCrashRestartValidatesOnlyTail(t *testing.T) {
	dir := t.TempDir()
	crashed := cleanStore(t, dir, nil)
	putXLogs(t, crashed, 1, 100)
	if err := crashed.Sync(); err != nil {
		t.Fatal(err)
	}
	putXLogs(t, crashed, 101, 110) // never synced; the process dies here
	if err := crashed.flushPacks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if markerExists(t, dir) {
		t.Fatal("crash left a clean marker")
	}
	log := readCheckpointLog(t, dir, checkpointName)
	for _, name := range []string{"xlog_tim", "xlog_tid", "xlog_gid"} {
		if _, ok := log.Indexes["20261013/xlog/"+name]; !ok {
			t.Fatalf("no checkpoint for %s: %v", name, log.Indexes)
		}
	}
	var events []RecoveryEvent
	s := cleanStore(t, dir, &events)
	defer s.Close()
	putXLogs(t, s, 111, 111)
	st := s.IndexOpenStats()
	// Three XLog indexes, ten records each after the checkpoint.
	if st.Walked != 0 || st.Checkpointed != 3 || st.Sealed != 0 || st.WalkedRecords != 30 || st.CheckpointRejected != 0 {
		t.Fatalf("crash restart: %+v", st)
	}
	if len(events) != 3 || events[0].Kind != "index-relinked" {
		t.Fatal(events)
	}
	checkXLogs(t, s, "20261013", 1, 111)
	if logs, err := s.XLogs("20261013", cleanDay, cleanDay+1000, false); err != nil || len(logs) != 111 {
		t.Fatal(len(logs), err)
	}
}

// (a) The same for the counters (realtime and 5m) and a KV pair.
func TestCrashRestartCounterAndKVTail(t *testing.T) {
	dir := t.TempDir()
	s, c := runtimePair(t, dir)
	putCounters(t, c, 1, 200)
	putKV(t, s, "a")
	if err := errorsJoin(s.Sync(), c.Sync()); err != nil {
		t.Fatal(err)
	}
	putCounters(t, c, 201, 207)
	putKV(t, s, "b")
	// Crash: neither is closed.
	s2, c2 := runtimePair(t, dir)
	defer closeRuntimePair(t, s2, c2)
	putCounters(t, c2, 208, 208)
	putKV(t, s2, "c")
	// real: 7 records after the checkpoint; 5m: none (one record per key).
	if st := c2.IndexOpenStats(); st.Walked != 0 || st.Checkpointed != 2 || st.WalkedRecords != 7 {
		t.Fatalf("counters after crash: %+v", st)
	}
	if st := s2.IndexOpenStats(); st.Walked != 0 || st.Checkpointed != 1 || st.WalkedRecords != 1 {
		t.Fatalf("KV after crash: %+v", st)
	}
	points, err := c2.Realtime("20261013", CounterHash("/host/app"), "TPS", cleanDay, cleanDay+86399999)
	if err != nil || len(points) != 208 {
		t.Fatal(len(points), err)
	}
	for _, k := range []string{"a", "b", "c"} {
		if v, ok, err := s2.KeyValueContext(context.Background(), "space", k, 1); err != nil || !ok || v != "v-"+k {
			t.Fatal(k, v, ok, err)
		}
	}
}

// (b) A damaged or foreign log, and a pair changed after its checkpoint, are
// validated in full.
func TestCheckpointLogFallbacks(t *testing.T) {
	setup := func(t *testing.T) string {
		dir := t.TempDir()
		s := cleanStore(t, dir, nil)
		putXLogs(t, s, 1, 20)
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		putXLogs(t, s, 21, 25)
		if err := s.flushPacks(context.Background()); err != nil {
			t.Fatal(err)
		}
		return dir // crashed
	}
	reopen := func(t *testing.T, dir string, relinked int) IndexOpenStats {
		var events []RecoveryEvent
		s := cleanStore(t, dir, &events)
		defer s.Close()
		putXLogs(t, s, 26, 26)
		checkXLogs(t, s, "20261013", 1, 26)
		if len(events) != relinked {
			t.Fatal(events)
		}
		return s.IndexOpenStats()
	}
	for name, damage := range map[string]func(t *testing.T, path string){
		"not json":       func(t *testing.T, path string) { os.WriteFile(path, []byte("{not json"), 0600) },
		"other format":   func(t *testing.T, path string) { rewriteLog(t, path, func(m *checkpointFile) { m.Format = 99 }) },
		"other writer":   func(t *testing.T, path string) { rewriteLog(t, path, func(m *checkpointFile) { m.Writer = "java" }) },
		"directory":      func(t *testing.T, path string) { os.Remove(path); os.Mkdir(path, 0700) },
		"escaping paths": func(t *testing.T, path string) { rewriteLog(t, path, escapeEntries) },
		"wrong sums": func(t *testing.T, path string) {
			rewriteLog(t, path, func(m *checkpointFile) {
				for k, e := range m.Indexes {
					e.HeaderSum++
					m.Indexes[k] = e
				}
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := setup(t)
			damage(t, filepath.Join(dir, checkpointName))
			st := reopen(t, dir, 3)
			if st.Walked != 3 || st.Checkpointed != 0 {
				t.Fatalf("%+v", st)
			}
		})
	}
	t.Run("pair changed by another writer", func(t *testing.T) {
		dir := setup(t)
		// Another writer opens the crashed pair (relinking it) and flushes
		// its header.
		other, err := legacyindex.Open(filepath.Join(dir, "20261013", "xlog", "xlog_tid"), 0, false, time.UTC)
		if err != nil {
			t.Fatal(err)
		}
		if err = other.Close(); err != nil {
			t.Fatal(err)
		}
		st := reopen(t, dir, 2)
		if st.Walked != 1 || st.Checkpointed != 2 || st.CheckpointRejected != 1 {
			t.Fatalf("%+v", st)
		}
	})
}

func rewriteLog(t *testing.T, path string, change func(*checkpointFile)) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var m checkpointFile
	if err = json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	change(&m)
	if b, err = json.Marshal(m); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
}

func escapeEntries(m *checkpointFile) {
	out := map[string]checkpointEntry{}
	for k, e := range m.Indexes {
		out["../"+k] = e
		out["/abs/"+k] = e
		out["20261013/./xlog/../xlog/"+filepath.Base(k)] = e
	}
	m.Indexes = out
}

// (d) An index evicted from the bounded cache is reopened in the same run
// through the checkpoint its close established, without walking.
func TestEvictionReopenUsesCheckpoint(t *testing.T) {
	dir := t.TempDir()
	s := cleanStore(t, dir, nil)
	defer s.Close()
	day := int64(86_400_000)
	for d := int64(0); d < 12; d++ { // 3 XLog indexes per day: more than 32
		putXLogsAt(t, s, cleanDay+d*day, 1, 20)
	}
	if err := s.flushPacks(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := s.IndexOpenStats()
	if before.Created != 36 {
		t.Fatalf("%+v", before)
	}
	putXLogsAt(t, s, cleanDay, 21, 30) // day 0 was evicted
	if err := s.flushPacks(context.Background()); err != nil {
		t.Fatal(err)
	}
	after := s.IndexOpenStats()
	if after.Checkpointed-before.Checkpointed != 3 || after.Walked != before.Walked || after.WalkedRecords != before.WalkedRecords {
		t.Fatalf("reopen after eviction: %+v -> %+v", before, after)
	}
	checkXLogs(t, s, "20261013", 1, 30)
}

// Same for the counter table cache (8 days).
func TestCounterEvictionReopenUsesCheckpoint(t *testing.T) {
	c, err := OpenCounters(t.TempDir(), time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	put := func(day int64) {
		p := &protocol.PerfCounterPack{Time: cleanDay + day*86_400_000, ObjName: "/o", TimeType: 1,
			Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: day}}}}}
		if err := c.Put(p); err != nil {
			t.Fatal(err)
		}
	}
	for d := int64(0); d < 10; d++ {
		put(d)
	}
	before := c.IndexOpenStats()
	put(0)
	after := c.IndexOpenStats()
	if after.Checkpointed-before.Checkpointed != 1 || after.Walked != 0 || after.WalkedRecords != 0 {
		t.Fatalf("%+v -> %+v", before, after)
	}
}

// Order: the log is written only after the headers it describes reached
// disk; every entry it holds at that moment opens through the checkpoint.
func TestCheckpointLogWrittenAfterHeaders(t *testing.T) {
	dir := t.TempDir()
	s := cleanStore(t, dir, nil)
	defer s.Close()
	checked := 0
	s.checkpoints.writeHook = func() error {
		s.checkpoints.mu.Lock()
		entries := make(map[string]legacyindex.Checkpoint, len(s.checkpoints.entries))
		for path, e := range s.checkpoints.entries {
			entries[path] = e.Checkpoint
		}
		s.checkpoints.mu.Unlock()
		for path, cp := range entries {
			copyDir := t.TempDir()
			dst := filepath.Join(copyDir, "x")
			for _, ext := range []string{".kfile", ".hfile"} {
				b, err := os.ReadFile(path + ext)
				if err != nil {
					return err
				}
				if err = os.WriteFile(dst+ext, b, 0600); err != nil {
					return err
				}
			}
			idx, err := legacyindex.OpenCheckpointed(dst, 0, filepath.Base(path) == "xlog_tim", time.UTC, nil, nil, &cp)
			if err != nil {
				return err
			}
			info := idx.OpenInfo()
			idx.Close()
			if info.Check != legacyindex.CheckTail {
				return fmt.Errorf("%s: entry does not describe the disk: %+v", path, info)
			}
			checked++
		}
		return nil
	}
	for round := int64(0); round < 4; round++ {
		putXLogs(t, s, round*10+1, round*10+10)
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if checked < 3 {
		t.Fatal("hook saw no entries", checked)
	}
}

// A pair created anew (the date was deleted) forgets the old checkpoint, and
// entries of pairs that no longer exist are dropped when the log is loaded.
func TestCheckpointOfRecreatedPairIsDropped(t *testing.T) {
	dir := t.TempDir()
	s := cleanStore(t, dir, nil)
	putXLogs(t, s, 1, 50)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(dir, "20261013")); err != nil {
		t.Fatal(err)
	}
	s = cleanStore(t, dir, nil)
	if n := len(s.checkpoints.entries); n != 0 {
		t.Fatal("entries of removed pairs kept", n)
	}
	putXLogs(t, s, 1, 2)
	if st := s.IndexOpenStats(); st.Created != 3 || st.Checkpointed != 0 {
		t.Fatalf("%+v", st)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	log := readCheckpointLog(t, dir, checkpointName)
	info, err := os.Stat(filepath.Join(dir, "20261013", "xlog", "xlog_tid.kfile"))
	if err != nil {
		t.Fatal(err)
	}
	if e := log.Indexes["20261013/xlog/xlog_tid"]; e.KeyBytes != info.Size() {
		t.Fatalf("stale entry: %+v, file %d", e, info.Size())
	}
}

func failDataFsync(mu syncxLocker, q *closeQueue, fail bool) {
	mu.Lock()
	defer mu.Unlock()
	if fail {
		q.fsync = func(*os.File) error { return errors.New("injected data fsync failure") }
	} else {
		q.fsync = nil
	}
}

type syncxLocker interface {
	Lock()
	Unlock()
}

func tidPath(dir, date string) string { return filepath.Join(dir, date, "xlog", "xlog_tid") }

// A data fsync failure leaves the headers of that pass without a crash
// checkpoint (they may reference records that are not durable): SyncData,
// Store.Close and Counters.Sync.
func TestNoCheckpointAfterDataFsyncFailure(t *testing.T) {
	t.Run("SyncData", func(t *testing.T) {
		dir := t.TempDir()
		s := cleanStore(t, dir, nil)
		defer s.Close()
		putXLogs(t, s, 1, 10)
		if err := s.flushPacks(context.Background()); err != nil {
			t.Fatal(err)
		}
		failDataFsync(&s.mu, &s.closes, true)
		if err := s.SyncData(); err == nil {
			t.Fatal("fsync failure not reported")
		}
		if cp := s.checkpoints.get(tidPath(dir, "20261013")); cp != nil {
			t.Fatalf("checkpoint recorded over a failed data fsync: %+v", cp)
		}
		failDataFsync(&s.mu, &s.closes, false)
		if err := s.SyncData(); err != nil {
			t.Fatal(err)
		}
		if s.checkpoints.get(tidPath(dir, "20261013")) == nil {
			t.Fatal("no checkpoint after a clean pass")
		}
	})
	t.Run("Close", func(t *testing.T) {
		dir := t.TempDir()
		s := cleanStore(t, dir, nil)
		putXLogs(t, s, 1, 10)
		if err := s.flushPacks(context.Background()); err != nil {
			t.Fatal(err)
		}
		failDataFsync(&s.mu, &s.closes, true)
		if err := s.Close(); err == nil {
			t.Fatal("fsync failure not reported")
		}
		if b, err := os.ReadFile(filepath.Join(dir, checkpointName)); err == nil {
			var m checkpointFile
			if json.Unmarshal(b, &m) == nil {
				for rel := range m.Indexes {
					if filepath.Dir(rel) == "20261013/xlog" {
						t.Fatalf("checkpoint for %s recorded over a failed data fsync", rel)
					}
				}
			}
		}
	})
	t.Run("Counters.Sync", func(t *testing.T) {
		dir := t.TempDir()
		c, err := OpenCounters(dir, time.UTC)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		putCounters(t, c, 1, 3)
		failDataFsync(&c.mu, &c.closes, true)
		if err = c.Sync(); err == nil {
			t.Fatal("fsync failure not reported")
		}
		real := filepath.Join(dir, "20261013", "counter", "real")
		if cp := c.checkpoints.get(real); cp != nil {
			t.Fatalf("checkpoint recorded over a failed data fsync: %+v", cp)
		}
		failDataFsync(&c.mu, &c.closes, false)
		if err = c.Sync(); err != nil || c.checkpoints.get(real) == nil {
			t.Fatal("no checkpoint after a clean pass", err)
		}
	})
}

// forgetCheckpointsLocked drops the entries at or below a purged or deleted
// root (directory or file, absolute or relative to the DB root); persist
// then writes once and skips the write while nothing changes.
func TestForgetCheckpointsLocked(t *testing.T) {
	dir := t.TempDir()
	s, c := runtimePair(t, dir)
	defer closeRuntimePair(t, s, c)
	putXLogsAt(t, s, cleanDay, 1, 5)
	putXLogsAt(t, s, cleanDay+86_400_000, 1, 5)
	putKV(t, s, "a")
	putCounters(t, c, 1, 3)
	if err := errorsJoin(s.Sync(), c.Sync()); err != nil {
		t.Fatal(err)
	}
	writes := 0
	s.checkpoints.writeHook = func() error { writes++; return nil }
	if err := s.SyncData(); err != nil || writes != 0 {
		t.Fatal("unchanged log rewritten", writes, err)
	}
	has := func(path string) bool { return s.checkpoints.get(path) != nil }
	day13, day14 := filepath.Join(dir, "20261013"), filepath.Join(dir, "20261014")
	if !has(tidPath(dir, "20261013")) || !has(tidPath(dir, "20261014")) {
		t.Fatal("setup: no checkpoints")
	}
	s.mu.Lock()
	s.forgetCheckpointsLocked("20261014/xlog/xlog_tid.kfile") // one file, relative
	s.mu.Unlock()
	if has(tidPath(dir, "20261014")) || !has(filepath.Join(day14, "xlog", "xlog_tim")) {
		t.Fatal("file root: wrong entries dropped")
	}
	s.mu.Lock()
	s.forgetCheckpointsLocked(day13) // a whole date, absolute
	s.mu.Unlock()
	for path := range s.checkpoints.entries {
		if strings.HasPrefix(path, day13+string(filepath.Separator)) {
			t.Fatal("entry kept below the forgotten root", path)
		}
	}
	if !has(filepath.Join(day14, "xlog", "xlog_tim")) {
		t.Fatal("other date dropped")
	}
	if err := s.checkpoints.persist(); err != nil || writes != 1 {
		t.Fatal("forget not persisted once", writes, err)
	}
	if err := s.checkpoints.persist(); err != nil || writes != 1 {
		t.Fatal("unchanged log rewritten", writes, err)
	}
	log := readCheckpointLog(t, dir, checkpointName)
	if _, ok := log.Indexes["20261013/xlog/xlog_tid"]; ok {
		t.Fatal("forgotten entry persisted")
	}

	real := filepath.Join(day13, "counter", "real")
	c.mu.Lock()
	c.forgetCheckpointsLocked(filepath.Join(day13, "counter", "real.data")) // not an index file
	c.mu.Unlock()
	if c.checkpoints.get(real) == nil {
		t.Fatal("data file root dropped the index entry")
	}
	c.mu.Lock()
	c.forgetCheckpointsLocked(filepath.Join(day13, "counter", "real.hfile"))
	c.mu.Unlock()
	if c.checkpoints.get(real) != nil || c.checkpoints.get(filepath.Join(day13, "counter", "5m")) == nil {
		t.Fatal("counter forget")
	}
}

// A handle released (purged or deleted) while SyncData fsyncs outside the
// gate does not get its entry back from that pass.
func TestSyncDataSkipsReleasedHandles(t *testing.T) {
	dir := t.TempDir()
	s := cleanStore(t, dir, nil)
	defer s.Close()
	putXLogs(t, s, 1, 10)
	if err := s.flushPacks(context.Background()); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dir, "20261013", "xlog")
	var released []*legacyindex.Index
	s.mu.Lock()
	s.closes.fsync = func(f *os.File) error {
		// Runs during the pass, outside the gate: a purge detaches root.
		s.closes.fsync = nil
		s.mu.Lock()
		for path, idx := range s.indexes {
			if strings.HasPrefix(path, root+string(filepath.Separator)) {
				released = append(released, idx)
				delete(s.indexes, path)
			}
		}
		s.forgetCheckpointsLocked(root)
		s.mu.Unlock()
		return f.Sync()
	}
	s.mu.Unlock()
	if err := s.SyncData(); err != nil {
		t.Fatal(err)
	}
	if len(released) != 3 {
		t.Fatal("setup", len(released))
	}
	for _, idx := range released {
		idx.Close()
	}
	for path := range s.checkpoints.entries {
		if strings.HasPrefix(path, root+string(filepath.Separator)) {
			t.Fatal("released handle recorded again", path)
		}
	}
}

// A handle evicted during a sync pass is closed by that pass. Its checkpoint
// is recorded then, unless a purge or DB delete forgot its queue entry
// (key cleared: the files moved away) meanwhile.
func TestForgottenPendingEvictionRecordsNothing(t *testing.T) {
	for _, forget := range []bool{false, true} {
		t.Run(fmt.Sprint("forgotten ", forget), func(t *testing.T) {
			dir := t.TempDir()
			s := cleanStore(t, dir, nil)
			defer s.Close()
			putXLogs(t, s, 1, 10)
			if err := s.flushPacks(context.Background()); err != nil {
				t.Fatal(err)
			}
			block, entered := make(chan struct{}), make(chan struct{})
			s.mu.Lock()
			s.closes.fsync = func(f *os.File) error {
				if entered != nil {
					close(entered)
					entered = nil
					<-block
				}
				return f.Sync()
			}
			wait := entered
			s.mu.Unlock()
			done := make(chan error, 1)
			go func() { done <- s.SyncData() }()
			<-wait
			tid := tidPath(dir, "20261013")
			s.mu.Lock()
			for len(s.indexes) > 0 {
				if err := s.evictIndexLocked(); err != nil {
					t.Error(err)
				}
			}
			s.checkpoints.drop(tid) // only the deferred close may record it
			if forget {
				for _, p := range s.closes.pending {
					if p.key == tid {
						p.key, p.adopt = "", nil
					}
				}
			}
			s.mu.Unlock()
			close(block)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			s.mu.Lock()
			s.closes.fsync = nil
			s.mu.Unlock()
			if got := s.checkpoints.get(tid) != nil; got == forget {
				t.Fatalf("forgotten=%v: checkpoint recorded=%v", forget, got)
			}
		})
	}
}
