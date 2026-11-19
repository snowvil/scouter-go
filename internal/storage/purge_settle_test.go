package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

func indexClosed(idx *legacyindex.Index) bool {
	_, err := idx.Get(longKey(0))
	return errors.Is(err, legacyindex.ErrClosed)
}

// A purge whose target is several files, some of which cannot be moved
// (here xlog.profile and xlog_profile.kfile stay, xlog_profile.hfile moves):
// the profile index is closed in place, and the data file that stays is
// fsynced before that close writes the index header (data before header).
func TestPurgeMixedTargetSyncsDataBeforeIndexClose(t *testing.T) {
	s, c, root := retentionFixture(t)
	old := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	date := JavaDate(old, time.UTC)
	if err := s.PutProfile(&protocol.ProfilePack{Time: old, TxID: 1, Profile: []byte{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(root, date, "xlog", "xlog.profile")
	idxPath := filepath.Join(root, date, "xlog", "xlog_profile")
	s.mu.Lock()
	idx, f := s.indexes[idxPath], s.files[dataPath]
	s.mu.Unlock()
	if idx == nil || f == nil {
		t.Fatal("fixture not cached")
	}
	purgeRename = func(from, to string) error {
		if from == dataPath || strings.HasSuffix(from, ".kfile") {
			return &os.LinkError{Op: "rename", Old: from, New: to, Err: syscall.EBUSY}
		}
		return os.Rename(from, to)
	}
	defer func() { purgeRename = os.Rename }()
	var synced []string
	s.closes.fsync = func(file *os.File) error {
		if !indexClosed(idx) {
			synced = append(synced, file.Name())
		}
		return file.Sync()
	}
	defer func() { s.closes.fsync = nil }()
	r := &Retention{Store: s, Counters: c}
	if _, err := r.purge(context.Background(), date, "20261013", PurgeProfile); err == nil || !strings.Contains(err.Error(), "left in place") {
		t.Fatal(err)
	}
	if len(synced) != 1 || synced[0] != dataPath {
		t.Fatal("data not fsynced before the index close", synced)
	}
	if !indexClosed(idx) {
		t.Fatal("index of a partly moved target left open")
	}
	s.mu.Lock()
	kept := s.files[dataPath] == f
	s.mu.Unlock()
	if !kept {
		t.Fatal("staying data handle not restored")
	}
}

// Handles evicted during a sync pass wait in the close queue under their
// path (re-adopted when that path is opened again before the pass closes
// them). After a purge moved their files away, a write or query of that path
// must not re-adopt them: the write would land in the renamed files and the
// query would return purged records.
func TestPurgeForgetsPendingEvictions(t *testing.T) {
	s, c, _ := retentionFixture(t)
	old := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	date := JavaDate(old, time.UTC)
	ctx := context.Background()
	if err := s.PutXLog(&protocol.XLogPack{EndTime: old, TxID: 1, GxID: 7}); err != nil {
		t.Fatal(err)
	}
	// Hold a SyncData pass in flight (its first fsync outside the gate).
	block, entered := make(chan struct{}), make(chan struct{}, 1)
	var once sync.Once
	s.closes.fsync = func(f *os.File) error {
		once.Do(func() { entered <- struct{}{}; <-block })
		return f.Sync()
	}
	defer func() { s.closes.fsync = nil }()
	var unblock sync.Once
	release := func() { unblock.Do(func() { close(block) }) }
	defer release() // a failed check must not leave the pass (and Close) stuck
	synced := make(chan error, 1)
	go func() { synced <- s.SyncData() }()
	<-entered
	// Evict every cached index and data file of the date into the queue.
	s.mu.Lock()
	for len(s.indexes) > 0 {
		if err := s.evictIndexLocked(); err != nil {
			t.Error(err)
		}
	}
	for len(s.files) > 0 {
		if err := s.evictFileLocked(); err != nil {
			t.Error(err)
		}
	}
	pending := len(s.closes.pending)
	s.mu.Unlock()
	if pending == 0 {
		t.Fatal("nothing queued for the pass")
	}
	r := &Retention{Store: s, Counters: c}
	if _, err := r.purge(ctx, date, "20261013", PurgeXLog); err != nil {
		t.Fatal(err)
	}
	check := func(when string) {
		t.Helper()
		if x, err := s.XLog(date, 1); err != nil || x != nil {
			t.Fatalf("%s: purged XLog returned: %v %v", when, x, err)
		}
		if x, err := s.XLog(date, 2); err != nil || x == nil {
			t.Fatalf("%s: XLog written after the purge lost: %v", when, err)
		}
	}
	if err := s.PutXLog(&protocol.XLogPack{EndTime: old + 1, TxID: 2, GxID: 7}); err != nil {
		t.Fatal(err)
	}
	check("pass in flight")
	release()
	if err := <-synced; err != nil {
		t.Fatal(err)
	}
	check("after the pass")
}

// cleanTrash removes quarantine names left in a date directory and one
// level below it (a target whose unlink failed), not only at the root.
func TestCleanTrashRemovesQuarantineInDateDirectories(t *testing.T) {
	s, c, root := retentionFixture(t)
	left := []string{
		filepath.Join(root, quarantinePrefix+"20261001-ALL-1-1-0"),
		filepath.Join(root, "20261001", quarantinePrefix+"20261001-XLOG-1-1-0"),
		filepath.Join(root, "20261001", "xlog", quarantinePrefix+"20261001-PROFILE-1-1-0"),
		filepath.Join(root, "00000000", "text", quarantinePrefix+"delete-1-1-0"),
	}
	for _, p := range left {
		touch(t, filepath.Join(p, "x"))
	}
	keep := filepath.Join(root, "20261001", "xlog", "xlog.service")
	touch(t, keep)
	if _, err := (&Retention{Store: s, Counters: c}).RunOnce(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, p := range left {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Error("quarantine left behind", p, err)
		}
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatal("cleanTrash removed DB data", err)
	}
}

// A trash or quarantine path a purge or DB delete is unlinking right now
// (moveAway registered it) is not cleanTrash's: it skips it instead of
// racing the owner's RemoveAll and reporting a false cleanup error.
func TestCleanTrashSkipsRemovalsInFlight(t *testing.T) {
	s, c, root := retentionFixture(t)
	owned := filepath.Join(root, purgeTrash, "delete-1-1-0")
	stale := filepath.Join(root, purgeTrash, "delete-0-0-0")
	touch(t, filepath.Join(owned, "x"))
	touch(t, filepath.Join(stale, "x"))
	removing.Store(owned, struct{}{})
	defer removing.Delete(owned)
	var removed []string
	purgeRemoveAll = func(p string) error { removed = append(removed, p); return os.RemoveAll(p) }
	defer func() { purgeRemoveAll = os.RemoveAll }()
	if _, err := (&Retention{Store: s, Counters: c}).RunOnce(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0] != stale {
		t.Fatal("cleanTrash touched an owned removal", removed)
	}
	if _, err := os.Stat(owned); err != nil {
		t.Fatal(err)
	}
	// The hook (and its slice) is single-goroutine: restore it before the
	// concurrent part.
	purgeRemoveAll = os.RemoveAll
	// A concurrent DB delete and retention cycle, under -race.
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	wg.Add(2)
	go func() {
		defer wg.Done()
		d := &DatabaseDeleter{Store: s, Counters: c}
		for i := 0; i < 30; i++ {
			touch(t, filepath.Join(root, "20261002", "xlog", "f"))
			if _, err := d.Delete(context.Background(), []string{"/20261002/xlog"}); err != nil {
				errs <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		r := &Retention{Store: s, Counters: c}
		for i := 0; i < 30; i++ {
			if _, err := r.RunOnce(context.Background(), time.Now()); err != nil {
				errs <- err
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// checkpointsUnder lists the crash checkpoint log entries of the Store and
// Counters below dir.
func checkpointsUnder(s *Store, c *Counters, dir string) []string {
	var out []string
	for _, l := range []*checkpointLog{s.checkpoints, c.checkpoints} {
		if l == nil {
			continue
		}
		l.mu.Lock()
		for path := range l.entries {
			if under(path, []string{dir}) {
				out = append(out, path)
			}
		}
		l.mu.Unlock()
	}
	sort.Strings(out)
	return out
}

// A purge or DB delete drops the crash checkpoints of the pairs it moved
// away (Store and Counters logs); a target that stayed keeps them.
func TestPurgeAndDeleteForgetCheckpointsOfMovedTargets(t *testing.T) {
	s, c, root := retentionFixture(t)
	ctx := context.Background()
	old := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	other := time.Date(2026, 10, 2, 1, 0, 0, 0, time.UTC).UnixMilli()
	write := func(tm int64) {
		t.Helper()
		if err := s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: tm, GxID: 7}); err != nil {
			t.Fatal(err)
		}
		if err := c.Put(counterPack(tm)); err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(s.SyncData(), c.Sync()); err != nil {
			t.Fatal(err)
		}
	}
	write(old)
	write(other)
	date, otherDate := JavaDate(old, time.UTC), JavaDate(other, time.UTC)
	day, otherDay := filepath.Join(root, date), filepath.Join(root, otherDate)
	hasCounter := func(dir string) bool {
		for _, p := range checkpointsUnder(s, c, dir) {
			if strings.Contains(p, string(filepath.Separator)+"counter"+string(filepath.Separator)) {
				return true
			}
		}
		return false
	}
	if len(checkpointsUnder(s, c, day)) == 0 || !hasCounter(day) || len(checkpointsUnder(s, c, otherDay)) == 0 {
		t.Fatal("fixture recorded no checkpoints", checkpointsUnder(s, c, root))
	}
	r := &Retention{Store: s, Counters: c}
	// Nothing moves: the checkpoints stay with the files.
	before := checkpointsUnder(s, c, day)
	failRename(t, "")
	if _, err := r.purge(ctx, date, "20261013", PurgeAll); err == nil {
		t.Fatal("rename failure hidden")
	}
	if got := checkpointsUnder(s, c, day); !reflect.DeepEqual(got, before) {
		t.Fatal("checkpoints of a target that stayed were dropped", before, got)
	}
	purgeRename = os.Rename
	// Purge of one kind directory: only its pairs go.
	if _, err := r.purge(ctx, date, "20261013", PurgeXLog); err != nil {
		t.Fatal(err)
	}
	if got := checkpointsUnder(s, c, filepath.Join(day, "xlog")); len(got) != 0 {
		t.Fatal("xlog checkpoints kept after the purge", got)
	}
	if !hasCounter(day) {
		t.Fatal("counter checkpoints dropped by an XLOG purge")
	}
	if _, err := r.purge(ctx, date, "20261013", PurgeAll); err != nil {
		t.Fatal(err)
	}
	if got := checkpointsUnder(s, c, day); len(got) != 0 {
		t.Fatal("checkpoints kept after PurgeAll", got)
	}
	// DB delete of the other date.
	if n, err := (&DatabaseDeleter{Store: s, Counters: c}).Delete(ctx, []string{"/" + otherDate}); n != 1 || err != nil {
		t.Fatal(n, err)
	}
	if got := checkpointsUnder(s, c, otherDay); len(got) != 0 {
		t.Fatal("checkpoints kept after the DB delete", got)
	}
}
