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
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func counterPack(tm int64) *protocol.PerfCounterPack {
	return &protocol.PerfCounterPack{Time: tm, ObjName: "/a", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 1}}}}}
}

func cachedUnder(s *Store, c *Counters, date string) []string {
	var hits []string
	for p := range s.indexes {
		if strings.Contains(p, date) {
			hits = append(hits, "index:"+p)
		}
	}
	for p := range s.files {
		if strings.Contains(p, date) {
			hits = append(hits, "file:"+p)
		}
	}
	for p := range s.ttlIndexes {
		if strings.Contains(p, date) {
			hits = append(hits, "ttl:"+p)
		}
	}
	if c != nil {
		for k := range c.tables {
			if strings.HasPrefix(k, date) {
				hits = append(hits, "counter:"+k)
			}
		}
	}
	return hits
}

func TestDeleteDatabasePathsClosesHandlesAndRecreatesOnWrite(t *testing.T) {
	s, c, root := retentionFixture(t)
	ctx := context.Background()
	a := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	b := time.Date(2026, 10, 2, 1, 0, 0, 0, time.UTC).UnixMilli()
	dateA, dateB := JavaDate(a, time.UTC), JavaDate(b, time.UTC)
	for _, tm := range []int64{a, b} {
		if err := s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 1, ObjHash: 1}); err != nil {
			t.Fatal(err)
		}
		if err := c.Put(counterPack(tm)); err != nil {
			t.Fatal(err)
		}
		if err := s.PutObject(JavaDate(tm, time.UTC), objectWithHash(1, "obj")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.PutKeyValueContext(ctx, "space", "k", "v", 10, a/1000); err != nil {
		t.Fatal(err)
	}
	if len(cachedUnder(s, c, dateA)) == 0 {
		t.Fatal("fixture opened nothing")
	}
	d := &DatabaseDeleter{Store: s, Counters: c}
	// Java name form from SERVER_DB_LIST ("/yyyymmdd/..."), plus a missing name.
	n, err := d.Delete(ctx, []string{"/" + dateA, "/" + dateA + "/xlog/", "//" + dateA + "/./object", "/19990101"})
	if err != nil || n != 1 {
		t.Fatalf("deleted %d %v", n, err)
	}
	if _, err := os.Stat(filepath.Join(root, dateA)); !os.IsNotExist(err) {
		t.Fatal("date remains", err)
	}
	if hits := cachedUnder(s, c, dateA); len(hits) != 0 {
		t.Fatal("handles survived", hits)
	}
	if entries, err := os.ReadDir(filepath.Join(root, purgeTrash)); err != nil || len(entries) != 0 {
		t.Fatal("trash not cleaned", entries, err)
	}
	// Explicit, error-free empty reads afterwards; the other date is intact.
	if logs, err := s.XLogs(dateA, a, a, false); err != nil || len(logs) != 0 {
		t.Fatal(logs, err)
	}
	if objects, err := s.Objects(dateA); err != nil || len(objects) != 0 {
		t.Fatal(objects, err)
	}
	if logs, err := s.XLogs(dateB, b, b, false); err != nil || len(logs) != 1 {
		t.Fatal(logs, err)
	}
	if objects, err := s.Objects(dateB); err != nil || len(objects) != 1 {
		t.Fatal(objects, err)
	}
	if points, err := c.Realtime(dateB, CounterHash("/a"), "TPS", b, b); err != nil || len(points) != 1 {
		t.Fatal(points, err)
	}
	if points, err := c.Realtime(dateA, CounterHash("/a"), "TPS", a, a); err != nil || len(points) != 0 {
		t.Fatal(points, err)
	}
	// Late data recreates fresh files instead of writing into unlinked inodes.
	if err := s.PutXLog(&protocol.XLogPack{EndTime: a, TxID: 2, ObjHash: 1}); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(counterPack(a)); err != nil {
		t.Fatal(err)
	}
	if logs, err := s.XLogs(dateA, a, a, false); err != nil || len(logs) != 1 || logs[0].TxID != 2 {
		t.Fatal(logs, err)
	}
	if points, err := c.Realtime(dateA, CounterHash("/a"), "TPS", a, a); err != nil || len(points) != 1 {
		t.Fatal(points, err)
	}
	// A sub-path below the permanent directory is allowed (Java deletes it too).
	if v, ok, err := s.KeyValueContext(ctx, "space", "k", a/1000); err != nil || !ok || v != "v" {
		t.Fatal(v, ok, err)
	}
	if n, err := s.DeleteDatabasePaths(ctx, []string{"/00000000/kv2"}); err != nil || n != 1 {
		t.Fatalf("kv2: %d %v", n, err)
	}
	if hits := cachedUnder(s, nil, "kv2"); len(hits) != 0 {
		t.Fatal("kv handles survived", hits)
	}
	if _, ok, err := s.KeyValueContext(ctx, "space", "k", a/1000); err != nil || ok {
		t.Fatal("kv survived", ok, err)
	}
	// Partial name: only xlog of date B goes, the object DB stays.
	if n, err := d.Delete(ctx, []string{"/" + dateB + "/xlog"}); err != nil || n != 1 {
		t.Fatalf("xlog only: %d %v", n, err)
	}
	if logs, err := s.XLogs(dateB, b, b, false); err != nil || len(logs) != 0 {
		t.Fatal(logs, err)
	}
	if objects, err := s.Objects(dateB); err != nil || len(objects) != 1 {
		t.Fatal(objects, err)
	}
}

func TestDeleteDatabasePathsRefusalMatrix(t *testing.T) {
	s, _, root := retentionFixture(t)
	ctx := context.Background()
	const date = "20261001"
	if err := s.PutObject(date, objectWithHash(1, "obj")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"lock.dat", ".go-writer.lock"} {
		if err := os.WriteFile(filepath.Join(root, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "victim"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	const linkedDate = "20261002"
	if err := os.Symlink(outside, filepath.Join(root, linkedDate)); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, purgeTrash, "x"), 0700); err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("a", maxDeletePathBytes+1)
	many := make([]string, maxDeletePaths+1)
	for i := range many {
		many[i] = "/19990101"
	}
	cases := []struct {
		name  string
		paths []string
		want  error
	}{
		{"empty name", []string{""}, ErrInvalid},
		{"root slash", []string{"/"}, ErrInvalid},
		{"root dot", []string{"/."}, ErrInvalid},
		{"no leading slash", []string{date}, ErrInvalid},
		{"no leading slash dot", []string{"."}, ErrInvalid},
		{"dotdot", []string{"/.."}, ErrInvalid},
		{"escape", []string{"/../x"}, ErrInvalid},
		{"escape folded back", []string{"/../" + date}, ErrInvalid},
		{"escape via date", []string{"/" + date + "/../.."}, ErrInvalid},
		{"escape via date deeper", []string{"/" + date + "/../../" + date}, ErrInvalid},
		{"host absolute path", []string{filepath.ToSlash(filepath.Join(root, date))}, ErrInvalid},
		{"unknown top level", []string{"/conf"}, ErrInvalid},
		{"invalid date", []string{"/20261301"}, ErrInvalid},
		{"java lock", []string{"/lock.dat"}, ErrInvalid},
		{"go lock", []string{"/.go-writer.lock"}, ErrInvalid},
		{"trash", []string{"/.purge-trash/x"}, ErrInvalid},
		{"permanent", []string{"/00000000"}, ErrInvalid},
		{"permanent trailing slash", []string{"/00000000/"}, ErrInvalid},
		{"permanent dot", []string{"/00000000/."}, ErrInvalid},
		{"backslash", []string{`/\` + date}, ErrInvalid},
		{"nul", []string{"/" + date + "\x00"}, ErrInvalid},
		{"invalid utf8", []string{"/\xff"}, ErrInvalid},
		{"symlink", []string{"/" + linkedDate}, ErrInvalid},
		{"symlink child", []string{"/" + linkedDate + "/victim"}, ErrInvalid},
		{"too long", []string{long}, ErrLimit},
		{"too many", many, ErrLimit},
	}
	for _, tc := range cases {
		// A valid existing name in the same request must survive a refusal.
		paths := append([]string{"/" + date}, tc.paths...)
		if len(tc.paths) > maxDeletePaths {
			paths = tc.paths
		}
		n, err := s.DeleteDatabasePaths(ctx, paths)
		if !errors.Is(err, tc.want) || n != 0 {
			t.Fatalf("%s: %d %v", tc.name, n, err)
		}
		if _, err := os.Stat(filepath.Join(root, date, "object", "obj.data")); err != nil {
			t.Fatalf("%s: valid sibling deleted: %v", tc.name, err)
		}
	}
	for _, name := range []string{"lock.dat", ".go-writer.lock", linkedDate, filepath.Join(purgeTrash, "x")} {
		if _, err := os.Lstat(filepath.Join(root, name)); err != nil {
			t.Fatal(name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(outside, "victim")); err != nil {
		t.Fatal("symlink target touched", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.DeleteDatabasePaths(cancelled, []string{"/" + date}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if n, err := s.DeleteDatabasePaths(ctx, []string{"/" + date}); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if objects, err := s.Objects(date); err != nil || len(objects) != 0 {
		t.Fatal(objects, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteDatabasePaths(ctx, []string{"/" + date}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := (&DatabaseDeleter{}).Delete(ctx, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestDeleteDatabasePathsConcurrentWithIngestAndQuery(t *testing.T) {
	s, c, _ := retentionFixture(t)
	tm := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC).UnixMilli()
	date := JavaDate(tm, time.UTC)
	d := &DatabaseDeleter{Store: s, Counters: c}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	wg.Add(4)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if err := s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: int64(i + 1)}); err != nil {
				errs <- err
				return
			}
			if err := s.PutObject(date, objectWithHash(int32(i%5), "obj")); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if err := c.Put(counterPack(tm)); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if _, err := s.XLogs(date, tm, tm, false); err != nil {
				errs <- err
				return
			}
			if _, err := s.Objects(date); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if _, err := d.Delete(context.Background(), []string{"/" + date, "/" + date + "/xlog"}); err != nil {
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
	if _, err := s.XLogs(date, tm, tm, false); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteDatabasePathsNeedsCountersForCounterFiles(t *testing.T) {
	s, c, root := retentionFixture(t)
	ctx := context.Background()
	tm := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	date := JavaDate(tm, time.UTC)
	if err := s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 1, ObjHash: 1}); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(counterPack(tm)); err != nil {
		t.Fatal(err)
	}
	// Without Counters a counter table would stay open on unlinked files:
	// the whole request is refused explicitly and nothing is removed.
	for _, name := range []string{"/" + date, "/" + date + "/counter", "/" + date + "/counter/real.data"} {
		if n, err := s.DeleteDatabasePaths(ctx, []string{"/" + date + "/xlog", name}); !errors.Is(err, ErrInvalid) || n != 0 {
			t.Fatalf("%s: %d %v", name, n, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, date, "xlog")); err != nil {
		t.Fatal("refused request removed a sibling", err)
	}
	// Paths that do not hold counter files need no Counters.
	if n, err := s.DeleteDatabasePaths(ctx, []string{"/" + date + "/xlog"}); err != nil || n != 1 {
		t.Fatalf("xlog: %d %v", n, err)
	}
	other, err := OpenCounters(t.TempDir(), time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := s.AttachCounters(other); !errors.Is(err, ErrInvalid) {
		t.Fatal("foreign root attached", err)
	}
	if err := s.AttachCounters(nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.AttachCounters(c); err != nil {
		t.Fatal(err)
	}
	if n, err := s.DeleteDatabasePaths(ctx, []string{"/" + date}); err != nil || n != 1 {
		t.Fatalf("attached: %d %v", n, err)
	}
	if hits := cachedUnder(s, c, date); len(hits) != 0 {
		t.Fatal("handles survived", hits)
	}
	// The deleted open date is recreated by the next write, never written
	// through a stale handle.
	if err := c.Put(counterPack(tm)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, date, "counter", "real.data")); err != nil {
		t.Fatal("counter not recreated", err)
	}
	if points, err := c.Realtime(date, CounterHash("/a"), "TPS", tm, tm); err != nil || len(points) != 1 {
		t.Fatal(points, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteDatabasePaths(ctx, []string{"/" + date}); !errors.Is(err, ErrClosed) {
		t.Fatal("closed counters", err)
	}
}

func TestDeleteDatabasePathsDiscardsVisitorSetOfDeletedDirectory(t *testing.T) {
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC, DeferVisitorFlush: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	now := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	date := JavaDate(now, time.UTC)
	if err := s.PutVisitorContext(ctx, now, "java", 7, 1); err != nil {
		t.Fatal(err)
	}
	if len(s.visitor.daily) == 0 || len(s.visitor.hourly) == 0 {
		t.Fatal("fixture has no pending visitor sets")
	}
	for _, dir := range []string{"visit", "visit_hourly"} {
		if err := os.MkdirAll(filepath.Join(root, date, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := s.DeleteDatabasePaths(ctx, []string{"/" + date + "/visit_hourly"}); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if len(s.visitor.hourly) != 0 || len(s.visitor.daily) == 0 {
		t.Fatal("visit_hourly delete: hourly", len(s.visitor.hourly), "daily", len(s.visitor.daily))
	}
	if n, err := s.DeleteDatabasePaths(ctx, []string{"/" + date + "/visit"}); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if len(s.visitor.daily) != 0 {
		t.Fatal("visit delete kept daily", len(s.visitor.daily))
	}
}

// Fault injection for SERVER_DB_DELETE: a target no rename can move is an
// error and stays whole (nothing is unlinked, its handles are closed before
// the gates are released); with only the trash failing it goes through a
// quarantine name and is unlinked after the gates.
func TestDeleteDatabasePathsRenameFailure(t *testing.T) {
	s, c, root := retentionFixture(t)
	ctx := context.Background()
	old := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	date := JavaDate(old, time.UTC)
	removed := watchRemoveAll(t, s, c)
	if err := s.PutXLog(&protocol.XLogPack{EndTime: old, TxID: 1, GxID: 7}); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(counterPack(old)); err != nil {
		t.Fatal(err)
	}
	before := cachedUnder(s, c, date)
	sort.Strings(before)
	d := &DatabaseDeleter{Store: s, Counters: c}
	failRename(t, "")
	n, err := d.Delete(ctx, []string{"/" + date + "/xlog", "/" + date + "/counter"})
	if n != 0 || err == nil || !strings.Contains(err.Error(), "left in place") {
		t.Fatal(n, err)
	}
	if len(*removed) != 0 {
		t.Fatal("unlinked although nothing moved", *removed)
	}
	// Nothing moved: the same handles are back in their caches.
	after := cachedUnder(s, c, date)
	sort.Strings(after)
	if len(before) == 0 || !reflect.DeepEqual(after, before) {
		t.Fatal("handles not restored", before, after)
	}
	if err := s.PutXLog(&protocol.XLogPack{EndTime: old + 1, TxID: 2, GxID: 7}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.XLogsByGxIDContext(ctx, date, 7); err != nil || len(got) != 2 {
		t.Fatal("files not usable after the failed delete", len(got), err)
	}
	failRename(t, filepath.Join(root, purgeTrash))
	if n, err = d.Delete(ctx, []string{"/" + date + "/xlog"}); n != 1 || err != nil {
		t.Fatal(n, err)
	}
	if _, err := os.Stat(filepath.Join(root, date, "xlog")); !os.IsNotExist(err) {
		t.Fatal("target not removed", err)
	}
	if len(*removed) != 1 || filepath.Dir((*removed)[0]) != filepath.Join(root, date) || !strings.HasPrefix(filepath.Base((*removed)[0]), quarantinePrefix) {
		t.Fatal("unlinks", *removed)
	}
	if got, err := s.XLogsByGxIDContext(ctx, date, 7); err != nil || got != nil {
		t.Fatal("deleted date still answers", len(got), err)
	}
}
