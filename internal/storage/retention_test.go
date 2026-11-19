package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func TestJavaRetentionDefaults(t *testing.T) {
	c := JavaRetentionDefaults()
	want := RetentionConfig{ProfileDays: 10, XLogDays: 30, CounterDays: 70, RealtimeCounterDays: 70, TagDays: 70, VisitorDays: 70, TextDays: 140, SumDays: 60, DiskUsagePct: 80}
	if c != want {
		t.Fatalf("%+v", c)
	}
	// Counter derives from the deprecated keep_days, not profile; zero profile/xlog fall back.
	c = JavaRetentionConfig(map[string]int{"mgr_purge_keep_days": 2, "mgr_purge_profile_keep_days": 5})
	want = RetentionConfig{ProfileDays: 5, XLogDays: 15, CounterDays: 14, RealtimeCounterDays: 14, TagDays: 14, VisitorDays: 14, TextDays: 30, SumDays: 60, DiskUsagePct: 80}
	if c != want {
		t.Fatalf("%+v", c)
	}
	c = JavaRetentionConfig(map[string]int{"mgr_purge_keep_days": 3, "mgr_purge_profile_keep_days": 0, "mgr_purge_xlog_keep_days": 0, "mgr_purge_xlog_without_profile_keep_days": 4, "mgr_purge_disk_usage_pct": 0, "mgr_purge_daily_text_days": 1})
	want = RetentionConfig{ProfileDays: 3, XLogDays: 4, CounterDays: 21, RealtimeCounterDays: 21, TagDays: 21, VisitorDays: 21, TextDays: 1, SumDays: 60}
	if c != want {
		t.Fatalf("%+v", c)
	}
}

func retentionFixture(t *testing.T) (*Store, *Counters, string) {
	t.Helper()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	c, err := OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close(); c.Close() })
	return s, c, root
}

func touch(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
}

func listFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			rel, _ := filepath.Rel(root, p)
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

// Day-limit modes follow AutoDeleteScheduler.deleteData targets and the
// int(dir) <= yyyymmdd(now-N days) boundary; today and reserved names survive.
func TestRetentionDayLimitTargetsAndBoundaries(t *testing.T) {
	s, c, root := retentionFixture(t)
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	full := []string{"xlog/xlog.profile", "xlog/xlog_profile.hfile", "xlog/xlog_profile.kfile", "xlog/xlog.profile.conf", "xlog/xlog.service", "span/span.data", "tagcnt/java/x", "text/text.kfile", "sum/sum.sum", "visit/a.usr", "visit_hourly/a.usr", "counter/real.data", "counter/real.hfile", "counter/real.kfile", "counter/real.head", "counter/5m.data"}
	// 20261012 is one day old, 20261003 is exactly the 10-day profile limit.
	for _, d := range []string{"20261013", "20261012", "20261003", "20261002", "20260913", "20260912", "20260801", "20261014"} {
		for _, f := range full {
			touch(t, filepath.Join(root, d, f))
		}
	}
	for _, f := range []string{"00000000/text/text_service.data", "00000000/kv2/kv_x.kvdata", "abc/keep", "123/keep", "lock.dat", "20000101/keep"} {
		touch(t, filepath.Join(root, f))
	}
	r := &Retention{Store: s, Counters: c, Config: RetentionConfig{ProfileDays: 10, XLogDays: 30, CounterDays: 70, RealtimeCounterDays: 40, TagDays: 70, VisitorDays: 70, TextDays: 0, SumDays: 60}}
	actions, err := r.RunOnce(context.Background(), now)
	if err != nil {
		t.Fatal(err, actions)
	}
	got := listFiles(t, root)
	has := func(f string) bool {
		for _, g := range got {
			if g == f {
				return true
			}
		}
		return false
	}
	for _, d := range []string{"20261013", "20261012", "20261014"} {
		for _, f := range full {
			if !has(d + "/" + f) {
				t.Fatalf("%s/%s removed: %v", d, f, actions)
			}
		}
	}
	// Profile-only removal leaves the mode file like Java.
	for _, d := range []string{"20261003", "20261002"} {
		for _, f := range full {
			profile := strings.HasPrefix(f, "xlog/xlog_profile") || f == "xlog/xlog.profile"
			if has(d+"/"+f) == profile {
				t.Fatalf("%s/%s profile boundary: %v", d, f, got)
			}
		}
	}
	// 20260913 = now-30d: XLog/span removed; text (max(text,tag,xlog)=70) stays.
	for _, f := range []string{"xlog/xlog.service", "xlog/xlog.profile.conf", "span/span.data"} {
		if has("20260913/" + f) {
			t.Fatal("xlog limit", got)
		}
	}
	for _, f := range []string{"text/text.kfile", "sum/sum.sum", "counter/real.data"} {
		if !has("20260913/" + f) {
			t.Fatal("retained", f, got)
		}
	}
	// 20260912 is 31 days old, inside the 40-day realtime limit.
	if !has("20260912/counter/real.head") || !has("20260912/counter/real.data") {
		t.Fatal(got)
	}
	// 20260801 is 73 days old: ALL mode removes the directory.
	for _, g := range got {
		if strings.HasPrefix(g, "20260801/") {
			t.Fatal("ALL mode left", g)
		}
	}
	for _, f := range []string{"00000000/text/text_service.data", "00000000/kv2/kv_x.kvdata", "abc/keep", "123/keep", "lock.dat", "20000101/keep"} {
		if !has(f) {
			t.Fatal("reserved removed", f)
		}
	}
	// The second cycle finds nothing left: no action, no failure, no change.
	if again, err := r.RunOnce(context.Background(), now); err != nil || len(again) != 0 {
		t.Fatal(again, err)
	}
	if !reflect.DeepEqual(got, listFiles(t, root)) {
		t.Fatal("second cycle changed files")
	}
}

func TestRetentionRealtimeAndSumModes(t *testing.T) {
	s, c, root := retentionFixture(t)
	now := time.Date(2026, 10, 13, 0, 0, 1, 0, time.UTC)
	for _, f := range []string{"counter/real.data", "counter/real.hfile", "counter/real.kfile", "counter/real.head", "counter/5m.data", "sum/sum.sum", "visit/a.usr", "visit_hourly/a.usr", "tagcnt/a", "text/t"} {
		touch(t, filepath.Join(root, "20261010", f))
	}
	r := &Retention{Store: s, Counters: c, Config: RetentionConfig{RealtimeCounterDays: 1, SumDays: 1, VisitorDays: 1, TagDays: 1, TextDays: 1}}
	if _, err := r.RunOnce(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	want := []string{"20261010/counter/5m.data", "20261010/counter/real.head", "20261010/visit_hourly/a.usr"}
	if got := listFiles(t, root); !reflect.DeepEqual(got, want) {
		t.Fatal(got)
	}
}

// Open handles of the purged date are synced and closed before removal; later
// late data recreates the date, and the next cycle (no persistent history) purges it again.
func TestRetentionClosesOpenHandlesAndRepurgesResurrectedDate(t *testing.T) {
	s, c, root := retentionFixture(t)
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	old := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	date := JavaDate(old, time.UTC)
	put := func(tx int64) {
		t.Helper()
		if err := s.PutXLog(&protocol.XLogPack{EndTime: old, TxID: tx, ObjHash: 1}); err != nil {
			t.Fatal(err)
		}
		if err := c.Put(&protocol.PerfCounterPack{Time: old, ObjName: "/a", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 1}}}}}); err != nil {
			t.Fatal(err)
		}
	}
	put(1)
	if logs, err := s.XLogs(date, old, old, false); err != nil || len(logs) != 1 {
		t.Fatal(logs, err)
	}
	r := &Retention{Store: s, Counters: c, Config: RetentionConfig{CounterDays: 3}}
	actions, err := r.RunOnce(context.Background(), now)
	if err != nil || len(actions) != 1 || actions[0].Date != date || actions[0].Mode != PurgeAll {
		t.Fatal(actions, err)
	}
	if _, err = os.Stat(filepath.Join(root, date)); !os.IsNotExist(err) {
		t.Fatal("date remains", err)
	}
	for p := range s.indexes {
		if strings.Contains(p, date) {
			t.Fatal("cached index survived", p)
		}
	}
	for p := range s.files {
		if strings.Contains(p, date) {
			t.Fatal("cached file survived", p)
		}
	}
	for k := range c.tables {
		if strings.HasPrefix(k, date) {
			t.Fatal("counter table survived", k)
		}
	}
	// Query of the purged date is an empty, error-free read.
	if logs, err := s.XLogs(date, old, old, false); err != nil || len(logs) != 0 {
		t.Fatal(logs, err)
	}
	// Late data recreates the date in fresh files rather than unlinked inodes.
	put(2)
	if logs, err := s.XLogs(date, old, old, false); err != nil || len(logs) != 1 || logs[0].TxID != 2 {
		t.Fatal(logs, err)
	}
	if actions, err = r.RunOnce(context.Background(), now); err != nil || len(actions) != 1 {
		t.Fatal(actions, err)
	}
	if _, err = os.Stat(filepath.Join(root, date)); !os.IsNotExist(err) {
		t.Fatal("resurrected date not purged again", err)
	}
}

func TestRetentionDiskUsageThresholds(t *testing.T) {
	s, c, root := retentionFixture(t)
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	dates := []string{"20261001", "20261002", "20261003", "20261013"}
	for _, d := range dates {
		touch(t, filepath.Join(root, d, "xlog/xlog.profile"))
		touch(t, filepath.Join(root, d, "xlog/xlog.service"))
		touch(t, filepath.Join(root, d, "counter/5m.data"))
	}
	// Usage stays constant, so each pass tries every old date once and stops.
	var used float64
	var calls int
	r := &Retention{Store: s, Counters: c, Config: RetentionConfig{DiskUsagePct: 80}}
	run := func() []PurgeAction {
		actions, err := r.RunOnce(context.Background(), now)
		if err != nil {
			t.Fatal(err)
		}
		return actions
	}
	r.DiskUsage = func(string) (float64, error) { calls++; return used, nil }
	used = 73 // > 72 only: PROFILE for every old date, nothing else.
	actions := run()
	if len(actions) != 3 {
		t.Fatal(actions)
	}
	for _, a := range actions {
		if a.Mode != PurgeProfile {
			t.Fatal(actions)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "20261013/xlog/xlog.profile")); err != nil {
		t.Fatal("today profile removed")
	}
	used = 77 // > 72 and > 76: profiles are already gone, so only XLOG acts.
	actions = run()
	if len(actions) != 3 || actions[0].Mode != PurgeXLog {
		t.Fatal(actions)
	}
	used = 81 // all three passes; ALL removes old directories.
	run()
	got := listFiles(t, root)
	for _, g := range got {
		if !strings.HasPrefix(g, "20261013/") {
			t.Fatal(got)
		}
	}
	if calls == 0 {
		t.Fatal("usage not measured")
	}
	r.DiskUsage = func(string) (float64, error) { return 0, errors.New("statfs failed") }
	if _, err := r.RunOnce(context.Background(), now); err == nil {
		t.Fatal("usage error hidden")
	}
}

// A failed removal is reported (not logged as success) and retried next cycle.
func TestRetentionFailureReportedAndRetried(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	s, c, root := retentionFixture(t)
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	touch(t, filepath.Join(root, "20261001", "xlog", "xlog.service"))
	xlogDir := filepath.Join(root, "20261001", "xlog")
	// A read-only parent makes the detach rename fail on every Unix.
	dayDir := filepath.Join(root, "20261001")
	if err := os.Chmod(dayDir, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dayDir, 0700) })
	// TEXT also runs (max(text, tag, xlog)) but has no target, so only XLOG acts.
	r := &Retention{Store: s, Counters: c, Config: RetentionConfig{XLogDays: 1}}
	actions, err := r.RunOnce(context.Background(), now)
	if err == nil || len(actions) != 1 || actions[0].Mode != PurgeXLog || actions[0].Err == "" {
		t.Fatal(actions, err)
	}
	// Neither rename worked: nothing is removed in place (under the gates),
	// the target stays whole for the next cycle.
	if _, err = os.Stat(filepath.Join(xlogDir, "xlog.service")); err != nil {
		t.Fatal("unmovable target was removed", err)
	}
	if q, stuck := r.DetachStats(); q != 0 || stuck != 1 {
		t.Fatal("detach stats", q, stuck)
	}
	if err = os.Chmod(dayDir, 0700); err != nil {
		t.Fatal(err)
	}
	if actions, err = r.RunOnce(context.Background(), now); err != nil || len(actions) != 1 || actions[0].Err != "" {
		t.Fatal(actions, err)
	}
	if _, err = os.Stat(xlogDir); !os.IsNotExist(err) {
		t.Fatal("retry did not remove", err)
	}
}

func TestRetentionRejectsCurrentDateAndClosedStore(t *testing.T) {
	s, c, _ := retentionFixture(t)
	r := &Retention{Store: s, Counters: c}
	if _, err := r.purge(context.Background(), "20261013", "20261013", PurgeAll); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := r.purge(context.Background(), "2026101x", "20261013", PurgeAll); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := r.purge(context.Background(), "20261001", "20261013", PurgeMode("BOGUS")); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.RunOnce(ctx, time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if removed, err := r.purge(context.Background(), "20261001", "20261013", PurgeAll); removed || err != nil {
		t.Fatal("missing target reported as removed", removed, err)
	}
	touch(t, filepath.Join(s.config.Directory, "20261001", "x"))
	s.Close()
	if _, err := r.purge(context.Background(), "20261001", "20261013", PurgeAll); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

// Purge holds the Store/Counters gates; concurrent today writes and old-date
// queries must not race with handle release (run under -race).
func TestRetentionConcurrentWithIngestAndQuery(t *testing.T) {
	s, c, _ := retentionFixture(t)
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	old := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	today := now.UnixMilli()
	r := &Retention{Store: s, Counters: c, Config: RetentionConfig{XLogDays: 3, CounterDays: 5}}
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if err := s.PutXLog(&protocol.XLogPack{EndTime: today, TxID: int64(i + 1)}); err != nil {
				errs <- err
				return
			}
			if err := s.PutXLog(&protocol.XLogPack{EndTime: old, TxID: int64(i + 1)}); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if _, err := s.XLogs(JavaDate(old, time.UTC), old, old, false); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if _, err := r.RunOnce(context.Background(), now); err != nil {
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
	if logs, err := s.XLogs(JavaDate(today, time.UTC), today, today, false); err != nil || len(logs) != 200 {
		t.Fatal(len(logs), err)
	}
}

// The bounded handle cache evicts the least recently used entry, so a hot
// handle survives a burst of cold opens (previously the sorted-first key).
func TestStoreHandleCacheLRU(t *testing.T) {
	s, _, root := retentionFixture(t)
	s.mu.Lock()
	defer s.mu.Unlock()
	hot := filepath.Join(root, "00", "a.data")
	if _, err := s.file(hot, true); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		if _, err := s.file(filepath.Join(root, "zz", strconv.Itoa(i)+".data"), true); err != nil {
			t.Fatal(err)
		}
		if _, err := s.file(hot, false); err != nil {
			t.Fatal(err)
		}
		if s.files[hot] == nil || len(s.files) > 16 || len(s.lastUse) > 16 {
			t.Fatal(i, len(s.files), len(s.lastUse))
		}
	}
}

// The compression mode is read once per conf path while the Store is open (Java
// reads it at daily writer open) and dropped when retention removes the date.
func TestCompressionModeCachedAndPurged(t *testing.T) {
	s, c, root := retentionFixture(t)
	tm := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	date := JavaDate(tm, time.UTC)
	if err := s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 1}); err != nil {
		t.Fatal(err)
	}
	conf := filepath.Join(root, date, "xlog", "xlog.service.conf")
	if err := os.WriteFile(conf, []byte("compress_xlog_enabled=true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, date, "xlog", "data")); !os.IsNotExist(err) {
		t.Fatal("cached plain mode was re-read mid-day", err)
	}
	if mode, present, err := ReadCompressionMode(conf, "compress_xlog_enabled", false); err != nil || !present || !mode {
		t.Fatal(mode, present, err)
	}
	r := &Retention{Store: s, Counters: c, Config: RetentionConfig{XLogDays: 1}}
	if _, err := r.RunOnce(context.Background(), time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if len(s.modes) != 0 {
		t.Fatal(s.modes)
	}
}

// Purge discards (does not flush) pending tag aggregation of the purged date,
// returns its byte accounting, leaves other dates' pending intact and never
// recreates the date on the next Sync.
func TestRetentionDiscardsPurgedAggregation(t *testing.T) {
	s, c, root := retentionFixture(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	old := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	today := now.UnixMilli()
	if err := s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: today, ObjHash: 1, Service: 2}); err != nil {
		t.Fatal(err)
	}
	lockTags(s)
	keepMeta, keepPending := s.tagCounts.metadataBytes, s.tagCounts.pendingBytes
	s.tags.mu.Unlock()
	if keepPending == 0 {
		t.Fatal("no pending tag delta")
	}
	if err := s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: old, ObjHash: 1, Service: 2}); err != nil {
		t.Fatal(err)
	}
	touch(t, filepath.Join(root, "20261001", "xlog", "xlog.service"))
	r := &Retention{Store: s, Counters: c, Config: RetentionConfig{CounterDays: 5}}
	if actions, err := r.RunOnce(ctx, now); err != nil || len(actions) != 1 {
		t.Fatal(actions, err)
	}
	lockTags(s)
	meta, pending, tables := s.tagCounts.metadataBytes, s.tagCounts.pendingBytes, len(s.tagCounts.tables)
	s.tags.mu.Unlock()
	if meta != keepMeta || pending != keepPending || tables != 1 {
		t.Fatal(meta, keepMeta, pending, keepPending, tables)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "20261001")); !os.IsNotExist(err) {
		t.Fatal("discarded aggregation recreated the purged date", err)
	}
	if entries, _ := os.ReadDir(filepath.Join(root, purgeTrash)); len(entries) != 0 {
		t.Fatal("trash not emptied", entries)
	}
}

// A trash entry that could not be removed is retried by the next cycle.
func TestRetentionTrashCleanupRetried(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	s, c, root := retentionFixture(t)
	stuck := filepath.Join(root, purgeTrash, "20261001-ALL-1-1-0")
	touch(t, filepath.Join(stuck, "xlog", "x"))
	if err := os.Chmod(filepath.Join(stuck, "xlog"), 0500); err != nil {
		t.Fatal(err)
	}
	r := &Retention{Store: s, Counters: c}
	if _, err := r.RunOnce(context.Background(), time.Now()); err == nil {
		t.Fatal("trash cleanup failure hidden")
	}
	if err := os.Chmod(filepath.Join(stuck, "xlog"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RunOnce(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stuck); !os.IsNotExist(err) {
		t.Fatal("trash not retried", err)
	}
}

// When the trash cannot be created (ENOSPC for its directory, or here a file
// in the way) targets are moved to a quarantine name next to themselves and
// removed after the gates instead of blocking the purge.
func TestRetentionQuarantinesWhenTrashUnusable(t *testing.T) {
	s, c, root := retentionFixture(t)
	touch(t, filepath.Join(root, purgeTrash)) // a regular file blocks MkdirAll
	touch(t, filepath.Join(root, "20261001", "xlog", "xlog.service"))
	r := &Retention{Store: s, Counters: c, Config: RetentionConfig{XLogDays: 1}}
	actions, err := r.RunOnce(context.Background(), time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC))
	if len(actions) != 1 || actions[0].Mode != PurgeXLog || actions[0].Err != "" {
		t.Fatal(actions, err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "20261001", "xlog")); !os.IsNotExist(statErr) {
		t.Fatal("target not removed", statErr)
	}
	// The blocked trash is still reported by cleanTrash, not hidden.
	if err == nil {
		t.Fatal("trash problem hidden")
	}
	if q, stuck := r.DetachStats(); q != 1 || stuck != 0 {
		t.Fatal("detach stats", q, stuck)
	}
	if left, _ := filepath.Glob(filepath.Join(root, "20261001", quarantinePrefix+"*")); len(left) != 0 {
		t.Fatal("quarantine left behind", left)
	}
}

// gatesFree fails unless the Store and Counters gates can be taken now (the
// caller holds neither).
func gatesFree(t *testing.T, s *Store, c *Counters) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.mu.LockContext(ctx); err != nil {
		t.Error("Store gate held during the unlink", err)
		return
	}
	s.mu.Unlock()
	if c != nil {
		if err := c.mu.LockContext(ctx); err != nil {
			t.Error("Counters gate held during the unlink", err)
			return
		}
		c.mu.Unlock()
	}
}

// failRename makes purge renames fail: every rename when trash is "", else
// only those into the trash (EXDEV: the trash on another filesystem).
func failRename(t *testing.T, trash string) {
	t.Helper()
	purgeRename = func(from, to string) error {
		if trash == "" || strings.HasPrefix(to, trash+string(filepath.Separator)) {
			return &os.LinkError{Op: "rename", Old: from, New: to, Err: syscall.EXDEV}
		}
		return os.Rename(from, to)
	}
	t.Cleanup(func() { purgeRename = os.Rename })
}

// watchRemoveAll records the unlinks of purges and checks each runs with the
// gates free.
func watchRemoveAll(t *testing.T, s *Store, c *Counters) *[]string {
	t.Helper()
	var removed []string
	purgeRemoveAll = func(p string) error {
		gatesFree(t, s, c)
		removed = append(removed, p)
		return os.RemoveAll(p)
	}
	t.Cleanup(func() { purgeRemoveAll = os.RemoveAll })
	return &removed
}

// Fault injection: when no rename works the purge fails without unlinking
// anything (in or out of the gates), the doomed handles are closed before
// the gates are released, and the files stay usable; when only the trash
// fails, the target goes through a quarantine name and is unlinked outside
// the gates.
func TestPurgeRenameFailureNeverUnlinksUnderGates(t *testing.T) {
	old := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	date := JavaDate(old, time.UTC)
	ctx := context.Background()
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	for _, zip := range []bool{false, true} {
		s, c, root := retentionFixture(t)
		s.SetCompression(zip, zip)
		removed := watchRemoveAll(t, s, c)
		if err := s.PutXLog(&protocol.XLogPack{EndTime: old, TxID: 1, GxID: 7}); err != nil {
			t.Fatal(err)
		}
		if err := c.Put(counterPack(old)); err != nil {
			t.Fatal(err)
		}
		before := cachedUnder(s, c, date)
		sort.Strings(before)
		if len(before) == 0 {
			t.Fatal("fixture opened nothing")
		}
		r := &Retention{Store: s, Counters: c, Config: RetentionConfig{CounterDays: 5}}
		failRename(t, "")
		actions, err := r.RunOnce(ctx, now)
		if err == nil || !strings.Contains(err.Error(), "left in place") || len(actions) != 1 || actions[0].Err == "" {
			t.Fatal(actions, err)
		}
		if len(*removed) != 0 {
			t.Fatal("unlinked although nothing moved", *removed)
		}
		if _, err := os.Stat(filepath.Join(root, date, "xlog")); err != nil {
			t.Fatal("target touched", err)
		}
		// Nothing moved: the same handles are back in their caches (none
		// closed, so no header was written over unsynced data), and writes
		// continue through them.
		after := cachedUnder(s, c, date)
		sort.Strings(after)
		if !reflect.DeepEqual(after, before) {
			t.Fatal("handles not restored", before, after)
		}
		if q, stuck := r.DetachStats(); q != 0 || stuck != 1 {
			t.Fatal("detach stats", q, stuck)
		}
		if err := s.PutXLog(&protocol.XLogPack{EndTime: old + 1, TxID: 2, GxID: 7}); err != nil {
			t.Fatal(err)
		}
		if got, err := s.XLogsByGxIDContext(ctx, date, 7); err != nil || len(got) != 2 {
			t.Fatal("files not usable after the failed purge", len(got), err)
		}
		// Only the trash fails: quarantine next to the target, unlink after.
		failRename(t, filepath.Join(root, purgeTrash))
		actions, err = r.RunOnce(ctx, now)
		if err != nil || len(actions) != 1 || actions[0].Err != "" {
			t.Fatal(actions, err)
		}
		if _, err := os.Stat(filepath.Join(root, date)); !os.IsNotExist(err) {
			t.Fatal("target not removed", err)
		}
		if len(*removed) != 1 || !strings.HasPrefix(filepath.Base((*removed)[0]), quarantinePrefix) || filepath.Dir((*removed)[0]) != root {
			t.Fatal("unlinks", *removed)
		}
		if q, stuck := r.DetachStats(); q != 1 || stuck != 1 {
			t.Fatal("detach stats", q, stuck)
		}
		// The normal path unlinks the trash entry, also after the gates.
		purgeRename = os.Rename
		if err := s.PutXLog(&protocol.XLogPack{EndTime: old, TxID: 3}); err != nil {
			t.Fatal(err)
		}
		if actions, err = r.RunOnce(ctx, now); err != nil || len(actions) != 1 {
			t.Fatal(actions, err)
		}
		if len(*removed) != 2 || filepath.Dir((*removed)[1]) != filepath.Join(root, purgeTrash) {
			t.Fatal("unlinks", *removed)
		}
	}
}

// A root-level quarantine name that could not be unlinked is retried by the
// next cycle's cleanTrash.
func TestRetentionQuarantineLeftoverRetried(t *testing.T) {
	s, c, root := retentionFixture(t)
	stuck := filepath.Join(root, quarantinePrefix+"20261001-ALL-1-1-0")
	touch(t, filepath.Join(stuck, "xlog", "x"))
	r := &Retention{Store: s, Counters: c}
	if _, err := r.RunOnce(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stuck); !os.IsNotExist(err) {
		t.Fatal("quarantine leftover not removed", err)
	}
}

// Tag flush looks up pending keys through the hash chain, so a tag file larger
// than the query byte budget keeps flushing (the full scan hit ErrLimit forever
// in a 10-minute soak) and repeated flushes accumulate into the same record.
func TestTagFlushScalesBeyondQueryBudget(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, MaxQueryBytes: 64 << 10, HashSizeBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	tm := time.Date(2026, 10, 13, 1, 2, 0, 0, time.UTC).UnixMilli()
	for round := 0; round < 3; round++ {
		for svc := int32(1); svc <= 40; svc++ { // 40 first records ≈ 230KiB > 64KiB
			if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc}); err != nil {
				t.Fatal(round, svc, err)
			}
		}
		// Sync, not SyncData: the periodic SyncData no longer writes tag records.
		if err = s.Sync(); err != nil {
			t.Fatal(round, err)
		}
	}
	lockTags(s)
	defer s.tags.mu.Unlock()
	path := filepath.Join(s.config.Directory, "20261013", "tagcnt", "java", "first")
	for _, svc := range []int32{1, 17, 40} {
		key, err := tagRecordKey(tagKey("service", "service"), textHash(svc), s.config.CodecLimits)
		if err != nil {
			t.Fatal(err)
		}
		r, ok, err := s.tagFindRecordLocked(ctx, path, true, key)
		if err != nil || !ok || r.counts[62] != 3 {
			t.Fatal(svc, ok, err, r.counts[62])
		}
	}
	if s.tagCounts.pendingBytes != 0 {
		t.Fatal("pending not flushed", s.tagCounts.pendingBytes)
	}
}

// A top100 cache build that fails is reported once per 5-minute cycle instead
// of being retried on every tick.
func TestTagTop100FailureBacksOff(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, MaxQueryBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	tm := time.Date(2026, 10, 13, 1, 2, 0, 0, time.UTC).UnixMilli()
	for svc := int32(1); svc <= 40; svc++ {
		if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc}); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	// The streaming build no longer fails on a large file; a torn trailing
	// record makes it fail instead.
	f, err := os.OpenFile(filepath.Join(s.config.Directory, "20261013", "tagcnt", "java", "first.kfile"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write([]byte{0})
	if err = errors.Join(err, f.Close()); err != nil {
		t.Fatal(err)
	}
	// The Store caches tag file sizes and does not support files changed
	// behind it; drop the cached size so the torn tail is seen.
	s.tags.mu.Lock()
	delete(s.tags.ends, filepath.Join(s.config.Directory, "20261013", "tagcnt", "java", "first.kfile"))
	s.tags.mu.Unlock()
	if len(s.tagCounts.cachePaths) == 0 {
		t.Fatal("no cache path registered")
	}
	// The engine tick (TagTickContext) only marks the rebuild due; the
	// maintenance loop's FlushTagCountsIfDue runs it and reports the failure.
	start := tm
	if err = s.VisitorSyncContext(ctx, start); err != nil { // first tick only initializes the clocks
		t.Fatal(err)
	}
	if err = s.FlushTagCountsIfDue(ctx); err != nil {
		t.Fatal("nothing due yet", err)
	}
	if err = s.VisitorSyncContext(ctx, start+300000); err != nil {
		t.Fatal("tick must not do the rebuild", err)
	}
	if err = s.FlushTagCountsIfDue(ctx); !errors.Is(err, ErrCorrupt) {
		t.Fatal("first cycle", err)
	}
	if !s.tagCounts.cachePaths[filepath.Join(s.config.Directory, "20261013", "tagcnt", "java")] {
		t.Fatal("failed path not kept for the next cycle")
	}
	if err = s.VisitorSyncContext(ctx, start+301000); err != nil {
		t.Fatal(err)
	}
	if err = s.FlushTagCountsIfDue(ctx); err != nil {
		t.Fatal("retried before the next cycle", err)
	}
	if err = s.VisitorSyncContext(ctx, start+600000); err != nil {
		t.Fatal(err)
	}
	if err = s.FlushTagCountsIfDue(ctx); !errors.Is(err, ErrCorrupt) {
		t.Fatal("retried at the next cycle", err)
	}
}

// releaseLocked releases the handles below roots as a purge does, closing
// them at once or by the sync pass in flight.
func (s *Store) releaseLocked(roots []string) error { return s.releaseDoomedLocked(roots, nil) }

func (c *Counters) releaseDateLocked(date string, realtimeOnly bool) error {
	return c.releaseDateDoomedLocked(date, realtimeOnly, nil)
}
