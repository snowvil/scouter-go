package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

var tagFlushTestDay = time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)

// tagEquivMix adds First and Next contributions to two object types over
// several hours; later rounds update existing records in place and append new
// ones (services beyond 100 per tag go to Next).
func tagEquivMix(t *testing.T, s *Store, round int) {
	t.Helper()
	ctx := context.Background()
	for _, objType := range []string{"java", "node"} {
		for i := 1; i <= 230; i++ {
			svc := int32(i + round*20)
			tm := tagFlushTestDay.Add(time.Duration((i+round)%24)*time.Hour + time.Duration(i%60)*time.Minute).UnixMilli()
			p := &protocol.XLogPack{EndTime: tm, ObjHash: int32(i % 7), Service: svc, Elapsed: int32(i%5 + 1), SQLCount: int32(i % 4)}
			if i%9 == 0 {
				p.Error = int32(i)
			}
			p.CountryCode = fmt.Sprintf("C%d", i%130)
			if err := s.PutXLogTagsContext(ctx, objType, p); err != nil {
				t.Fatal(round, i, err)
			}
			if i%2 == 0 {
				a := &protocol.AlertPack{Time: tm, ObjHash: int32(i % 7), Level: byte(i % 4), Title: fmt.Sprintf("title-%d", (i+round)%150)}
				if err := s.PutAlertTagsContext(ctx, objType, a); err != nil {
					t.Fatal(round, i, err)
				}
			}
		}
	}
}

// readTagTree returns every file below {date}/tagcnt by relative path.
func readTagTree(t *testing.T, root string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	base := filepath.Join(root, "20261013", "tagcnt")
	err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(base, path)
		out[rel] = b
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// The incremental flush and the held top100 rebuild, forced into one record
// per gate hold, write byte-identical first/next/hfile/data/top100 files to
// the former one-hold full flush and rebuild.
func TestTagIncrementalFlushMatchesFullFlush(t *testing.T) {
	ctx := context.Background()
	open := func() (*Store, string) {
		root := t.TempDir()
		// A tiny hash area makes flushes walk long chains.
		s, err := Open(Config{Directory: root, Location: time.UTC, HashSizeBytes: 64})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		return s, root
	}
	full, fullRoot := open()
	inc, incRoot := open()
	inc.tagHoldSteps = 1
	holds := 0
	inc.tagBetweenHolds = func() { holds++ }
	for round := 0; round < 3; round++ {
		tagEquivMix(t, full, round)
		tagEquivMix(t, inc, round)
		lockTags(full)
		err := full.tagFlushLocked(ctx)
		full.tags.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		before := holds
		if err = inc.flushTagCounts(ctx); err != nil {
			t.Fatal(err)
		}
		if holds-before < 1000 {
			t.Fatal("expected one record per hold", holds-before)
		}
		if inc.tagCounts.pendingBytes != 0 || full.tagCounts.pendingBytes != 0 {
			t.Fatal("pending left", inc.tagCounts.pendingBytes, full.tagCounts.pendingBytes)
		}
	}
	paths := []string{filepath.Join(fullRoot, "20261013", "tagcnt", "java"), filepath.Join(fullRoot, "20261013", "tagcnt", "node")}
	lockTags(full)
	for _, path := range paths {
		if err := full.tagMakeCacheLocked(ctx, path); err != nil {
			full.tags.mu.Unlock()
			t.Fatal(err)
		}
	}
	full.tags.mu.Unlock()
	tick := tagFlushTestDay.UnixMilli()
	if err := inc.VisitorSyncContext(ctx, tick); err != nil {
		t.Fatal(err)
	}
	if err := inc.VisitorSyncContext(ctx, tick+300000); err != nil {
		t.Fatal(err)
	}
	before := holds
	if err := inc.FlushTagCountsIfDue(ctx); err != nil {
		t.Fatal(err)
	}
	if holds-before < 1000 {
		t.Fatal("expected one record per rebuild hold", holds-before)
	}
	want, got := readTagTree(t, fullRoot), readTagTree(t, incRoot)
	names := make([]string, 0, len(want))
	tops := 0
	for name := range want {
		names = append(names, name)
		if strings.HasPrefix(filepath.Base(name), "top100.") {
			tops++
		}
	}
	sort.Strings(names)
	if len(want) != len(got) || tops < 10 {
		t.Fatal("file sets", len(want), len(got), tops)
	}
	for _, name := range names {
		if !bytes.Equal(want[name], got[name]) {
			t.Fatal("differs", name, len(want[name]), len(got[name]))
		}
	}
	for _, kind := range []string{"first.kfile", "first.hfile", "next.kfile", "next.hfile", "next.data"} {
		if _, ok := want[filepath.Join("java", kind)]; !ok {
			t.Fatal("missing", kind)
		}
	}
}

// tagServiceTotal is the count of one service at minute: its First or Next
// record plus its pending delta. The caller holds the tag gate.
func tagServiceTotal(s *Store, path string, svc int32, minute int) (float32, error) {
	key, err := tagRecordKey(tagKey("service", "service"), textHash(svc), s.config.CodecLimits)
	if err != nil {
		return 0, err
	}
	var total float32
	for _, first := range []bool{true, false} {
		base := "next"
		if first {
			base = "first"
		}
		r, ok, err := s.tagFindRecordLocked(context.Background(), filepath.Join(path, base), first, key)
		if err != nil {
			return 0, err
		}
		if ok {
			total += r.counts[minute]
		}
	}
	if table := s.tagCounts.tables[path]; table != nil {
		if d := table.pending[tagValueKey{tag: tagKey("service", "service"), kind: protocol.ValueTextHash, n: int64(svc)}]; d != nil {
			total += d.minute(minute)
		}
	}
	return total, nil
}

// Contributions that arrive between the holds of a pass (to keys already
// written, keys still ahead and new keys, with and without the inline
// pending-bytes flush) are neither lost nor double counted, and between holds
// every record plus its pending delta equals what was contributed.
func TestTagIncrementalFlushInterleavesIngest(t *testing.T) {
	for _, tc := range []struct {
		name     string
		maxBytes int
	}{{"periodic", 0}} { // the pending bound drops entries now (TestTagPendingBoundDropsWholeEntries)
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			s, err := Open(Config{Directory: root, Location: time.UTC, HashSizeBytes: 64, MaxQueryBytes: tc.maxBytes})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
			const minute = 430
			path := filepath.Join(root, "20261013", "tagcnt", "java")
			want := map[int32]float32{}
			put := func(svc int32) {
				if err := s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc}); err != nil {
					t.Error(err)
				}
				want[svc]++
			}
			for svc := int32(1); svc <= 150; svc++ {
				put(svc)
			}
			if err = s.flushTagCounts(ctx); err != nil {
				t.Fatal(err)
			}
			for svc := int32(1); svc <= 150; svc += 2 {
				put(svc)
			}
			s.tagHoldSteps = 1
			calls := 0
			s.tagBetweenHolds = func() {
				calls++
				put(int32(calls*7%180 + 1)) // existing First/Next keys and new keys
				if _, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service", textHash(int32(calls%180+1))); err != nil {
					t.Error("query between holds", err)
				}
				lockTags(s)
				defer s.tags.mu.Unlock()
				for svc := int32(1); svc <= 180; svc += 11 {
					got, err := tagServiceTotal(s, path, svc, minute)
					if err != nil || got != want[svc] {
						t.Errorf("between holds service %d: record+pending %v, contributed %v, %v", svc, got, want[svc], err)
					}
				}
			}
			if err = s.flushTagCounts(ctx); err != nil {
				t.Fatal(err)
			}
			if calls == 0 {
				t.Fatal("pass ran in one hold")
			}
			s.tagBetweenHolds = nil
			if err = s.flushTagCounts(ctx); err != nil { // what arrived during the pass
				t.Fatal(err)
			}
			if s.tagCounts.pendingBytes != 0 {
				t.Fatal("pending left", s.tagCounts.pendingBytes)
			}
			lockTags(s)
			defer s.tags.mu.Unlock()
			for svc := int32(1); svc <= 180; svc++ {
				got, err := tagServiceTotal(s, path, svc, minute)
				if err != nil || got != want[svc] {
					t.Fatalf("service %d: file %v, contributed %v, %v", svc, got, want[svc], err)
				}
			}
		})
	}
}

// Another goroutine takes the Store gate while a flush pass and a top100
// rebuild over many records are in progress, with the default hold budget.
func TestTagPassReleasesGateBetweenHolds(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	for svc := int32(1); svc <= 700; svc++ {
		if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc}); err != nil {
			t.Fatal(err)
		}
	}
	other := make(chan int32)
	done := make(chan error)
	go func() {
		for svc := range other {
			// PutXLogTagsContext needs the gate the pass holds during a hold.
			done <- s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc})
		}
	}()
	defer close(other)
	holds, next := 0, int32(1000)
	s.tagBetweenHolds = func() {
		holds++
		next++
		other <- next
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("gate not acquired between holds")
		}
	}
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	// 702 records at most 256 per hold.
	if holds < 2 {
		t.Fatal("flush holds", holds)
	}
	tick := tm
	if err = s.VisitorSyncContext(ctx, tick); err != nil {
		t.Fatal(err)
	}
	if err = s.VisitorSyncContext(ctx, tick+300000); err != nil {
		t.Fatal(err)
	}
	holds = 0
	if err = s.FlushTagCountsIfDue(ctx); err != nil {
		t.Fatal(err)
	}
	if holds < 3 {
		t.Fatal("flush and rebuild holds", holds)
	}
	if _, err = os.Stat(tagCachePath(filepath.Join(root, "20261013", "tagcnt", "java"), tagKey("service", "service"))); err != nil {
		t.Fatal("top100 not written", err)
	}
}

// The engine tick (TagTickContext) only marks the flush and the top100
// rebuild due; FlushTagCountsIfDue writes them.
func TestTagTickOnlyMarksDue(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	for svc := int32(1); svc <= 150; svc++ {
		if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc}); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(root, "20261013", "tagcnt", "java")
	for _, now := range []int64{tm, tm + 5000, tm + 10000, tm + 300000} {
		if err = s.VisitorSyncContext(ctx, now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("tick wrote tag files", err)
	}
	if !s.tags.flushDue || !s.tags.cacheDue || len(s.tags.inbox) == 0 {
		t.Fatal("not marked due", s.tags.flushDue, s.tags.cacheDue, len(s.tags.inbox))
	}
	if err = s.FlushTagCountsIfDue(ctx); err != nil {
		t.Fatal(err)
	}
	if s.tags.flushDue || s.tags.cacheDue || s.tagCounts.pendingBytes != 0 || len(s.tags.inbox) != 0 {
		t.Fatal("due work left", s.tags.flushDue, s.tags.cacheDue, s.tagCounts.pendingBytes, len(s.tags.inbox))
	}
	for _, name := range []string{"first.kfile", "next.kfile", "next.data", filepath.Base(tagCachePath(path, tagKey("service", "service")))} {
		if _, err = os.Stat(filepath.Join(path, name)); err != nil {
			t.Fatal(name, err)
		}
	}
	series, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service", textHash(150))
	if err != nil || len(series) != 1440 || series[430] != 1 {
		t.Fatal(series, err)
	}
	// Nothing due: no pass.
	if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: 1}); err != nil {
		t.Fatal(err)
	}
	if err = s.FlushTagCountsIfDue(ctx); err != nil || s.tagCounts.pendingBytes == 0 {
		t.Fatal("flushed before due", err, s.tagCounts.pendingBytes)
	}
}

// Close between the holds of a pass flushes everything itself; the pass then
// stops without error and nothing is lost.
func TestTagPassStopsAfterClose(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	for svc := int32(1); svc <= 150; svc++ {
		if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc}); err != nil {
			t.Fatal(err)
		}
	}
	s.tagHoldSteps = 1
	s.tagBetweenHolds = func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.FlushTagCountsIfDue(ctx); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for svc := int32(1); svc <= 150; svc++ {
		series, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service", textHash(svc))
		if err != nil || len(series) != 1440 || series[430] != 1 {
			t.Fatal(svc, series, err)
		}
	}
	// A rebuild interrupted by Close stops quietly too.
	s.tagHoldSteps = 1
	s.tagBetweenHolds = func() { s.Close() }
	if err = s.MakeTagTop100Context(ctx, "20261013", "java"); err != nil {
		t.Fatal(err)
	}
	s.tags.mu.Lock()
	s.tagCounts = &tagCountState{tables: map[string]*tagTable{}, cachePaths: map[string]bool{filepath.Join(root, "20261013", "tagcnt", "java"): true}}
	s.tags.mu.Unlock()
	s.tags.inboxMu.Lock()
	s.tags.cacheDue = true
	s.tags.inboxMu.Unlock()
	if err = s.FlushTagCountsIfDue(ctx); err != nil {
		t.Fatal(err)
	}
}

// A database delete between the holds of a top100 rebuild stops it quietly:
// no cache file recreates the deleted directory and the path is not retried.
func TestTagRebuildStopsAfterPurgeBetweenHolds(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	for svc := int32(1); svc <= 150; svc++ { // services beyond 100 go to Next
		if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc}); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "20261013", "tagcnt", "java")
	s.tags.inboxMu.Lock()
	s.tags.cacheDue = true
	s.tags.inboxMu.Unlock()
	lockTags(s)
	if !s.tagCounts.cachePaths[path] {
		s.tags.mu.Unlock()
		t.Fatal("path not marked for a rebuild")
	}
	s.tags.mu.Unlock()
	s.tagHoldSteps = 1
	deleted := false
	s.tagBetweenHolds = func() {
		if deleted {
			return
		}
		deleted = true
		if _, err := s.DeleteDatabasePaths(ctx, []string{"/20261013"}); err != nil {
			t.Error(err)
		}
	}
	if err = s.FlushTagCountsIfDue(ctx); err != nil {
		t.Fatal("a purge between holds is not an error", err)
	}
	if !deleted {
		t.Fatal("the rebuild never paused between holds")
	}
	if _, err = os.Stat(filepath.Join(root, "20261013")); !os.IsNotExist(err) {
		t.Fatal("deleted date recreated", err)
	}
	lockTags(s)
	defer s.tags.mu.Unlock()
	if s.tagCounts.cachePaths[path] {
		t.Fatal("purged path re-registered")
	}
}

// Pending deltas are charged per touched hour, so tens of thousands of new
// values in one hour stay pending for the periodic flush instead of hitting
// the pending-bytes bound and flushing inline in the ingest path.
func TestTagPendingIsChargedPerHour(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	startTagDrainer(t, s)
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	for svc := int32(1); svc <= 20000; svc++ {
		if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm + int64(svc%600)*100, ObjHash: 1, Service: svc, Elapsed: 5}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = os.Stat(filepath.Join(root, "20261013", "tagcnt")); !os.IsNotExist(err) {
		t.Fatal("an inline flush wrote tag files before the periodic flush", err)
	}
	lockTags(s)
	pending := s.tagCounts.pendingBytes
	s.tags.mu.Unlock()
	if pending > 32<<20 {
		t.Fatalf("pending bytes %d for one hour of 20000 values", pending)
	}
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	for _, svc := range []int32{1, 9999, 20000} {
		series, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service-time-sum", textHash(svc))
		if err != nil || len(series) != 1440 || tagSum(series) != 5 {
			t.Fatal(svc, err, tagSum(series))
		}
	}
}

// Next records spanning several hours keep the full-day sums and totalCount
// (new hours on existing deltas and records).
func TestTagNextTotalsAcrossHours(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	day := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	put := func(svc int32, hour, minute int, elapsed int32) {
		tm := day.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute).UnixMilli()
		if err := s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc, Elapsed: elapsed}); err != nil {
			t.Fatal(err)
		}
	}
	// 100 services fill First; 101..103 go to Next.
	for svc := int32(1); svc <= 103; svc++ {
		put(svc, 1, 5, 2)
	}
	if err = s.Sync(); err != nil { // the records exist from here on
		t.Fatal(err)
	}
	for hour := 2; hour <= 6; hour++ { // new hours on existing records
		for svc := int32(101); svc <= 103; svc++ {
			put(svc, hour, hour, int32(hour))
		}
	}
	if err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	lockTags(s)
	defer s.tags.mu.Unlock()
	for svc := int32(101); svc <= 103; svc++ {
		key, err := tagRecordKey(tagKey("service", "service-time-sum"), textHash(svc), s.config.CodecLimits)
		if err != nil {
			t.Fatal(err)
		}
		r, ok, err := s.tagFindRecordLocked(ctx, filepath.Join(root, "20261013", "tagcnt", "java", "next"), false, key)
		if err != nil || !ok {
			t.Fatal(svc, ok, err)
		}
		want := float32(2)
		if r.counts[65] != 2 {
			t.Fatal(svc, "hour 1", r.counts[65])
		}
		for hour := 2; hour <= 6; hour++ {
			if r.counts[hour*60+hour] != float32(hour) {
				t.Fatal(svc, hour, r.counts[hour*60+hour])
			}
			want += float32(hour)
		}
		if r.totalCount != want || tagSum(r.counts) != want {
			t.Fatal(svc, "totalCount", r.totalCount, tagSum(r.counts), want)
		}
	}
}

// The cached tag file sizes (s.ends) match the files after appends, in-place
// updates, hash table creation and top100 rewrites.
func TestTagSizeCacheMatchesFiles(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for round := 0; round < 3; round++ {
		tagEquivMix(t, s, round)
		if err = s.flushTagCounts(ctx); err != nil {
			t.Fatal(err)
		}
		if err = s.MakeTagTop100Context(ctx, "20261013", "java"); err != nil {
			t.Fatal(err)
		}
	}
	lockTags(s)
	defer s.tags.mu.Unlock()
	checked := 0
	for path, size := range s.tags.ends {
		if !strings.Contains(path, string(filepath.Separator)+"tagcnt"+string(filepath.Separator)) {
			continue
		}
		st, err := os.Stat(path)
		if err != nil || st.Size() != size {
			t.Fatal(path, size, st, err)
		}
		checked++
	}
	if checked < 5 {
		t.Fatal("too few cached tag sizes", checked)
	}
}

// lockTags applies the inbox and takes the tag gate, for tests that inspect
// tag state.
func lockTags(s *Store) {
	s.tags.mu.Lock()
	if err := s.tagDrainAllLocked(context.Background()); err != nil {
		panic(err)
	}
}

// Ingest hands tag contributions to the inbox and does not wait for the tag
// gate; with the runtime drainer a full inbox costs a short wait, not a loss.
func TestTagInboxDecouplesIngestAndBackpressure(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	put := func(svc int32) error {
		return s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc})
	}
	s.tags.mu.Lock() // a tag pass holds the tag gate
	done := make(chan error, 1)
	go func() { done <- put(1) }()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ingest waited for the tag gate")
	}
	s.tags.mu.Unlock()
	startTagDrainer(t, s)
	for i := 0; i < 3*tagInboxMax; i++ {
		if err = put(int32(i%300 + 2)); err != nil {
			t.Fatal(err)
		}
	}
	if st := s.TagCountStats(); st.InboxDropped != 0 {
		t.Fatal("dropped with a drainer", st)
	}
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	total, err := s.TagValuesContext(ctx, "20261013", "java", "service", "@total")
	if err != nil {
		t.Fatal(err)
	}
	if want := float32(3*tagInboxMax + 1); total.TotalCount != want {
		t.Fatal("total", total.TotalCount, want)
	}
}

// A stalled tag count (nothing drains) costs ingest one short wait per
// saturation; then contributions are dropped at once and counted, and the
// next drain ends the saturation.
func TestTagInboxStalledDropsWithoutSlowingIngest(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	put := func(svc int32) {
		if err := s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < tagInboxMax; i++ {
		put(int32(i%300 + 1))
	}
	start := time.Now()
	put(1) // waits for room, then drops
	if d := time.Since(start); d < tagInboxWait/2 {
		t.Fatal("no wait before the first drop", d)
	}
	start = time.Now()
	for i := 0; i < 1000; i++ {
		put(2) // saturated: dropped without waiting
	}
	if d := time.Since(start); d > tagInboxWait {
		t.Fatal("ingest slowed while saturated", d)
	}
	if st := s.TagCountStats(); st.InboxDropped != 1001 || st.Backpressure != 1 {
		t.Fatal("drops", st)
	}
	if err = s.FlushTagCountsIfDue(ctx); err != nil {
		t.Fatal(err)
	}
	put(3) // room again: queued
	if st := s.TagCountStats(); st.InboxDropped != 1001 {
		t.Fatal("dropped after the drain", st)
	}
}

// A database delete drops contributions still in the inbox for the deleted
// date, so no flush recreates it; Close applies what is left.
func TestTagInboxPurgeAndClose(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC)
	for _, tm := range []time.Time{day, day.Add(24 * time.Hour)} {
		if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm.UnixMilli(), ObjHash: 1, Service: 5}); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.MkdirAll(filepath.Join(root, "20261013"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DeleteDatabasePaths(ctx, []string{"/20261013"}); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(root, "20261013")); !os.IsNotExist(err) {
		t.Fatal("deleted date recreated from the inbox", err)
	}
	if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: day.UnixMilli(), ObjHash: 1, Service: 5}); !errors.Is(err, ErrClosed) {
		t.Fatal("put after Close", err)
	}
	s, err = Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	series, err := s.TagValueSeriesContext(ctx, "20261014", "java", "service", "service", textHash(5))
	if err != nil || len(series) != 1440 || series[430] != 1 {
		t.Fatal("Close did not apply the inbox", series, err)
	}
}

// A drain stopped by ctx, between holds or before it starts, neither loses
// nor repeats contributions: the rest stays queued in arrival order.
func TestTagDrainCancelKeepsEveryContributionOnce(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	const n = 600
	for i := 0; i < n; i++ {
		if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: int32(i%150 + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	s.tagHoldSteps = 7
	cancelled, cancel := context.WithCancel(ctx)
	holds := 0
	s.tagBetweenHolds = func() {
		if holds++; holds == 3 {
			cancel()
		}
	}
	s.tagRun.Lock()
	err = s.tagDrain(cancelled)
	s.tagRun.Unlock()
	if !errors.Is(err, context.Canceled) {
		t.Fatal("drain not stopped", err)
	}
	s.tagBetweenHolds = nil
	if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: 1}); err != nil {
		t.Fatal(err)
	}
	s.tags.mu.Lock()
	if err = s.tagDrainAllLocked(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled drain-all", err)
	}
	s.tags.mu.Unlock()
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	total, err := s.TagValuesContext(ctx, "20261013", "java", "service", "@total")
	if err != nil || total.TotalCount != n+1 {
		t.Fatal("total", total.TotalCount, err)
	}
	series, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service", textHash(1))
	if err != nil || series[430] != n/150+1 {
		t.Fatal("service 1", series[430], err)
	}
}

// countdownCtx reports cancellation from its n-th Err call on, to stop a
// flush at every possible check.
type countdownCtx struct {
	context.Context
	n *int
}

func (c countdownCtx) Err() error {
	if *c.n--; *c.n < 0 {
		return context.Canceled
	}
	return nil
}

// A flush cancelled at any check (the shutdown of the maintenance loop) leaves
// whole records: no hash bucket points past its key file, the next flush and
// queries succeed, and every contribution is counted once.
func TestTagFlushCancelledAnywhereLeavesWholeRecords(t *testing.T) {
	ctx := context.Background()
	for stop := 0; stop < 400; stop += 7 {
		root := t.TempDir()
		s, err := Open(Config{Directory: root, Location: time.UTC, HashSizeBytes: 64})
		if err != nil {
			t.Fatal(err)
		}
		tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
		for svc := int32(1); svc <= 130; svc++ { // First and Next records, shared buckets
			if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc, Elapsed: 3}); err != nil {
				t.Fatal(err)
			}
		}
		n := stop
		if err = s.flushTagCounts(countdownCtx{ctx, &n}); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(stop, err)
		}
		if err = s.Close(); err != nil {
			t.Fatal(stop, "close after a cancelled flush", err)
		}
		for _, first := range []bool{true, false} {
			rep, err := VerifyTagCountFiles(filepath.Join(root, "20261013", "tagcnt", "java", map[bool]string{true: "first", false: "next"}[first]), first)
			if err != nil || len(rep.Problems) != 0 {
				t.Fatal(stop, "chain after a cancelled flush", first, err, rep)
			}
		}
		if s, err = Open(Config{Directory: root, Location: time.UTC, HashSizeBytes: 64}); err != nil {
			t.Fatal(err)
		}
		for _, svc := range []int32{1, 64, 130} {
			series, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service-time-sum", textHash(svc))
			if err != nil || len(series) != 1440 || series[430] != 3 {
				t.Fatal(stop, svc, series[430], err)
			}
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// The chain check accepts what the Store writes and reports the shapes a torn
// tag write leaves: a bucket head past its records, a cut key file.
func TestVerifyTagCountFiles(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC, HashSizeBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 2; round++ {
		tagEquivMix(t, s, round)
		if err = s.flushTagCounts(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "20261013", "tagcnt", "java")
	for _, first := range []bool{true, false} {
		base := filepath.Join(dir, map[bool]string{true: "first", false: "next"}[first])
		rep, err := VerifyTagCountFiles(base, first)
		if err != nil || rep.Records == 0 || len(rep.Problems) != 0 {
			t.Fatal("Store-written pair", first, err, rep)
		}
	}
	// A head written before its record (the fixed shutdown tear).
	base := filepath.Join(dir, "next")
	st, err := os.Stat(base + ".kfile")
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(base+".hfile", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var b [5]byte
	for off := int64(1024); ; off += 5 {
		if _, err = f.ReadAt(b[:], off); err != nil {
			t.Fatal(err)
		}
		if readLong5(b[:]) != 0 {
			pos := st.Size()
			if _, err = f.WriteAt([]byte{byte(pos >> 32), byte(pos >> 24), byte(pos >> 16), byte(pos >> 8), byte(pos)}, off); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	f.Close()
	rep, err := VerifyTagCountFiles(base, false)
	if err != nil || len(rep.Problems) != 1 || rep.Problems[0].Kind != "head_past_end" {
		t.Fatal("dangling head", err, rep)
	}
	if err = os.Truncate(base+".kfile", st.Size()-3); err != nil {
		t.Fatal(err)
	}
	rep, err = VerifyTagCountFiles(base, false)
	if err != nil || len(rep.Problems) == 0 || rep.Problems[0].Kind != "torn_tail" {
		t.Fatal("cut key file", err, rep)
	}
}

// A Client tag query scans in holds: the tag gate is free between them.
func TestTagQueryScanReleasesTagGate(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	for svc := int32(1); svc <= 150; svc++ {
		if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc}); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	s.tagHoldSteps = 3
	free := 0
	s.tagQueryBetween = func() {
		s.tags.mu.Lock() // deadlocks if the query kept the gate
		s.tags.mu.Unlock()
		free++
	}
	series, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service", textHash(150))
	if err != nil || len(series) != 1440 || series[430] != 1 {
		t.Fatal(series, err)
	}
	total, err := s.TagValuesContext(ctx, "20261013", "java", "service", "service")
	if err != nil || total.HowManyValues != 100 {
		t.Fatal(total, err)
	}
	if free < 10 {
		t.Fatal("scan did not release the gate between holds", free)
	}
}

// Updating existing tag records in place, hour by hour, writes the same bytes
// as rewriting whole records (First minute slices, Next hour blocks, totals,
// new hours appended), over rounds that update and append.
func TestTagInPlaceUpdateMatchesFullRewrite(t *testing.T) {
	ctx := context.Background()
	open := func(full bool) (*Store, string) {
		root := t.TempDir()
		s, err := Open(Config{Directory: root, Location: time.UTC, HashSizeBytes: 64})
		if err != nil {
			t.Fatal(err)
		}
		s.tagFullRewrite = full
		return s, root
	}
	inPlace, a := open(false)
	whole, b := open(true)
	for round := 0; round < 4; round++ {
		for _, s := range []*Store{inPlace, whole} {
			tagEquivMix(t, s, round)
			if err := s.flushTagCounts(ctx); err != nil {
				t.Fatal(round, err)
			}
		}
	}
	for _, s := range []*Store{inPlace, whole} {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	got, want := readTagTree(t, a), readTagTree(t, b)
	if len(got) == 0 || len(got) != len(want) {
		t.Fatal("tag files", len(got), len(want))
	}
	for name, w := range want {
		if !bytes.Equal(got[name], w) {
			t.Fatal(name, "differs from the whole-record rewrite")
		}
	}
}

// The inbox keeps draining during a long flush pass: contributions queued
// between its holds reach pending before the pass ends.
func TestTagFlushHoldsDrainTheInbox(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	put := func(svc int32) {
		if err := s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc}); err != nil {
			t.Fatal(err)
		}
	}
	for svc := int32(1); svc <= 120; svc++ {
		put(svc)
	}
	lockTags(s) // pending now; the hook below counts flush holds only
	s.tags.mu.Unlock()
	s.tagHoldSteps = 2
	holds := 0
	s.tagBetweenHolds = func() {
		if holds++; holds == 2 {
			put(500)
		}
		if holds == 5 {
			s.tags.inboxMu.Lock()
			left := len(s.tags.inbox)
			s.tags.inboxMu.Unlock()
			if left != 0 {
				t.Error("inbox not drained by the flush holds", left)
			}
		}
	}
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	if holds < 5 {
		t.Fatal("too few holds", holds)
	}
}

// A tag write that fails before any count changed keeps the delta for a
// retry (and cuts a failed append off); one that fails after counts reached
// the file drops the delta instead of adding it twice.
func TestTagWriteFailureRetryOrDrop(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	day := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	put := func(svc int32, hour int) {
		tm := day.Add(time.Duration(hour)*time.Hour + 5*time.Minute).UnixMilli()
		if err := s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc, Elapsed: 2}); err != nil {
			t.Fatal(err)
		}
	}
	series := func(svc int32) []float32 {
		got, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service-time-sum", textHash(svc))
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	for svc := int32(1); svc <= 101; svc++ { // 101 goes to Next
		put(svc, 1)
	}
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "20261013", "tagcnt", "java")
	enospc := errors.New("no space")
	// 1) A new Next hour cannot be appended: nothing counted, retried.
	put(101, 3)
	s.tagWriteHook = func(path string, pos int64, n int) error {
		if strings.HasSuffix(path, "next.data") && n == 240 {
			return enospc
		}
		return nil
	}
	if err = s.flushTagCounts(ctx); !errors.Is(err, enospc) {
		t.Fatal("append failure not reported", err)
	}
	s.tagWriteHook = nil
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	if got := series(101); got[65] != 2 || got[185] != 2 || s.TagCountStats().DroppedDeltas != 0 {
		t.Fatal("retried append", got[65], got[185], s.TagCountStats())
	}
	// 2) The head write fails after the in-place add: dropped, not doubled.
	put(101, 1)
	s.tagWriteHook = func(path string, pos int64, n int) error {
		if strings.HasSuffix(path, "next.kfile") && n == 129 {
			return enospc
		}
		return nil
	}
	if err = s.flushTagCounts(ctx); !errors.Is(err, enospc) {
		t.Fatal("head failure not reported", err)
	}
	s.tagWriteHook = nil
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	if got := series(101); got[65] != 4 || s.TagCountStats().DroppedDeltas != 1 {
		t.Fatal("dropped after a partial write", got[65], s.TagCountStats())
	}
	dataSize := func() int64 {
		st, err := os.Stat(filepath.Join(dir, "next.data"))
		if err != nil {
			t.Fatal(err)
		}
		return st.Size()
	}
	// 3) A new record's key append fails: the record and its data blocks are
	// cut off, retried, chain intact.
	before := dataSize()
	put(500, 2)
	s.tagWriteHook = func(path string, pos int64, n int) error {
		if strings.HasSuffix(path, "next.kfile") && n > 129 {
			return enospc
		}
		return nil
	}
	if err = s.flushTagCounts(ctx); !errors.Is(err, enospc) {
		t.Fatal("record append failure not reported", err)
	}
	if got := dataSize(); got != before {
		t.Fatal("orphan data blocks left", got, before)
	}
	s.tagWriteHook = nil
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	if got := series(500); got[125] != 2 {
		t.Fatal("retried record", got[125])
	}
	s.tags.mu.Lock()
	for _, first := range []bool{true, false} {
		rep, err := VerifyTagCountFiles(filepath.Join(dir, map[bool]string{true: "first", false: "next"}[first]), first)
		if err != nil || len(rep.Problems) != 0 {
			s.tags.mu.Unlock()
			t.Fatal("chain after failures", first, err, rep)
		}
	}
	s.tags.mu.Unlock()
}

// A purge of another date between the holds of a query does not cut it
// short; a purge of the queried files does (empty result).
func TestTagQueryHoldsSurviveUnrelatedPurge(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, day := range []time.Time{time.Date(2026, 10, 12, 7, 10, 0, 0, time.UTC), time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC)} {
		for svc := int32(1); svc <= 150; svc++ {
			if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: day.UnixMilli(), ObjHash: 1, Service: svc}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	s.tagHoldSteps = 3
	target := "/20261012"
	s.tagQueryBetween = func() {
		if target != "" {
			if _, err := s.DeleteDatabasePaths(ctx, []string{target}); err != nil {
				t.Error(err)
			}
			target = ""
		}
	}
	series, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service", textHash(150))
	if err != nil || len(series) != 1440 || series[430] != 1 {
		t.Fatal("unrelated purge cut the query", series, err)
	}
	target = "/20261013"
	series, err = s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service", textHash(150))
	if err != nil || series != nil {
		t.Fatal("purged files still read", series, err)
	}
}

// FlushTagCountsIfDue applies the inbox on every call (the runtime's 100ms
// tick); records are written only once the engine tick marks a flush due.
func TestFlushTagCountsIfDueDrainsInbox(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: 1}); err != nil {
		t.Fatal(err)
	}
	queued := func() int {
		s.tags.inboxMu.Lock()
		defer s.tags.inboxMu.Unlock()
		return len(s.tags.inbox)
	}
	if err = s.FlushTagCountsIfDue(ctx); err != nil || queued() != 0 {
		t.Fatal("not drained", err, queued())
	}
	s.tags.mu.Lock()
	pending := s.tagCounts.pendingBytes
	s.tags.mu.Unlock()
	if pending == 0 {
		t.Fatal("drained contribution written before a flush was due")
	}
}

// startTagDrainer runs the runtime's tag goroutine (FlushTagCountsIfDue on
// TagDrainSignal and a short tick) for the test.
func startTagDrainer(t *testing.T, s *Store) {
	t.Helper()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
			case <-s.TagDrainSignal():
			}
			s.FlushTagCountsIfDue(context.Background())
		}
	}()
	t.Cleanup(func() { close(stop); <-done })
}

// At the pending bound an inbox entry is dropped whole and counted, not
// flushed inline: an XLog's tags are either all counted or none.
func TestTagPendingBoundDropsWholeEntries(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, MaxQueryBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	const n = 300
	for svc := int32(1); svc <= n; svc++ {
		if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc, Elapsed: 3}); err != nil {
			t.Fatal(err)
		}
	}
	lockTags(s)
	s.tags.mu.Unlock()
	st := s.TagCountStats()
	if st.PendingDropped == 0 || st.InlineFlushes != 0 {
		t.Fatal("no whole-entry drop at the bound", st)
	}
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	total, err := s.TagValuesContext(ctx, "20261013", "java", "service", "@total")
	if err != nil {
		t.Fatal(err)
	}
	counted := 0
	for svc := int32(1); svc <= n; svc++ {
		a, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service", textHash(svc))
		if err != nil {
			t.Fatal(err)
		}
		b, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service-time-sum", textHash(svc))
		if err != nil {
			t.Fatal(err)
		}
		if (a == nil) != (b == nil) {
			t.Fatal(svc, "an XLog's tags were counted in part")
		}
		if a != nil {
			counted++
		}
	}
	if counted == 0 || uint64(n-counted) != st.PendingDropped || total.TotalCount != float32(counted) {
		t.Fatal("counted", counted, "dropped", st.PendingDropped, "total", total.TotalCount)
	}
}

// A pass hold applies the inbox within half of its budget and takes a new
// inbox only once the batch is applied, so the batch stays within one inbox
// and the rest of the hold is left to the pass.
func TestTagDrainSomeBounded(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	put := func(n int) {
		for i := 0; i < n; i++ {
			if err := s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: int32(i + 1)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	put(300)
	s.tags.mu.Lock()
	defer s.tags.mu.Unlock()
	hold := tagHold{max: 100, deadline: time.Now().Add(time.Hour)}
	s.tagDrainSomeLocked(&hold)
	if hold.steps != 50 || len(s.tags.draining) != 300 || s.tags.drainPos != 50 {
		t.Fatal("first hold", hold.steps, len(s.tags.draining), s.tags.drainPos)
	}
	put(10) // arrives while the batch is applied
	for i := 0; i < 5; i++ {
		hold = tagHold{max: 100, deadline: time.Now().Add(time.Hour)}
		s.tagDrainSomeLocked(&hold)
		if hold.steps > 50 || len(s.tags.draining) > tagInboxMax {
			t.Fatal("hold past half", hold.steps, len(s.tags.draining))
		}
	}
	// 300 applied after 6 holds; the next takes the 10 that came meanwhile.
	if s.tags.draining != nil {
		t.Fatal("batch left", len(s.tags.draining), s.tags.drainPos)
	}
	hold = tagHold{max: 100, deadline: time.Now().Add(time.Hour)}
	s.tagDrainSomeLocked(&hold)
	if hold.steps != 10 {
		t.Fatal("new inbox", hold.steps)
	}
}

// The whole-entry pre-check never underestimates: a contribution adds at
// most tagNeedMax pending bytes at the length prefix boundaries of its value.
func TestTagNeedMaxCoversKey(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tags := []tagContribution{
		{name: "@total", kind: protocol.ValueNull},
		{name: "service", kind: protocol.ValueTextHash, n: -1},
		{name: "level", kind: protocol.ValueDecimal, n: math.MinInt64},
	}
	for _, n := range []int{0, 1, 252, 253, 254, 65535, 65536, 32700} {
		tags = append(tags, tagContribution{name: "title", kind: protocol.ValueText, text: strings.Repeat("x", n)})
	}
	s.tags.mu.Lock()
	defer s.tags.mu.Unlock()
	for _, tag := range tags {
		e := tagInboxEntry{group: "service", tags: []tagContribution{tag}}
		id := e.items(nil)[0]
		key, err := s.tagScratchKey(id.id.tag, id.value())
		if errors.Is(err, ErrLimit) {
			continue // not counted at all
		}
		if err != nil {
			t.Fatal(err)
		}
		if got := tagDeltaOverhead + len(key) + tagHourBytes; got > tagNeedMax(id) {
			t.Fatal(tag.kind, len(tag.text), got, tagNeedMax(id))
		}
	}
}

// A failing tag flush (ENOSPC) is tried once per 5s tick, not on every 100ms
// call of the tag goroutine, and its records stay pending for the next try.
// The failed header writes of the new files leave empty files that the next
// try writes again (not ErrCorrupt).
func TestTagFlushFailureRetriedAtNextTick(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: 1}); err != nil {
		t.Fatal(err)
	}
	enospc := errors.New("no space")
	writes := 0
	s.tagWriteHook = func(string, int64, int) error { writes++; return enospc }
	if err = s.VisitorSyncContext(ctx, tm); err != nil { // initializes the clocks
		t.Fatal(err)
	}
	if err = s.VisitorSyncContext(ctx, tm+5000); err != nil {
		t.Fatal(err)
	}
	if err = s.FlushTagCountsIfDue(ctx); !errors.Is(err, enospc) {
		t.Fatal("flush failure not reported", err)
	}
	tried := writes
	for i := 0; i < 10; i++ { // the 100ms calls before the next tick
		if err = s.FlushTagCountsIfDue(ctx); err != nil || writes != tried {
			t.Fatal("retried before the next tick", err, writes, tried)
		}
	}
	s.tagWriteHook = nil
	if err = s.VisitorSyncContext(ctx, tm+10000); err != nil {
		t.Fatal(err)
	}
	if err = s.FlushTagCountsIfDue(ctx); err != nil {
		t.Fatal(err)
	}
	if s.tagCounts.pendingBytes != 0 {
		t.Fatal("not written at the next tick", s.tagCounts.pendingBytes)
	}
	if got, err := s.TagValuesContext(ctx, "20261013", "java", "service", "@total"); err != nil || got.TotalCount != 1 {
		t.Fatal("record lost", got, err)
	}
}

// A long flush pass keeps applying the inbox while it rests between holds:
// with tiny holds the in-hold drain alone (half a hold) could not empty a
// large inbox before the pass ends; the rests do.
func TestTagPassRestAppliesInbox(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.tagHoldSteps = 4
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	put := func(from, to int32) {
		for svc := from; svc < to; svc++ {
			if err := s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc}); err != nil {
				t.Fatal(err)
			}
		}
	}
	put(1, 2001) // pending for a pass of about 1000 holds (2 records each)
	if err := s.VisitorSyncContext(ctx, tm); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushTagCountsIfDue(ctx); err != nil { // applies the inbox, nothing due yet
		t.Fatal(err)
	}
	if err := s.VisitorSyncContext(ctx, tm+5000); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.FlushTagCountsIfDue(ctx) }()
	for deadline := time.Now().Add(5 * time.Second); s.tagRun.TryLock(); time.Sleep(time.Millisecond) {
		s.tagRun.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("pass did not start")
		}
	}
	put(5001, 11001)       // 6000 entries while the pass runs: more than its holds apply
	queued := func() int { // not yet applied: the rest of the batch and the inbox
		s.tags.mu.Lock()
		n := len(s.tags.draining) - s.tags.drainPos
		s.tags.mu.Unlock()
		s.tags.inboxMu.Lock()
		defer s.tags.inboxMu.Unlock()
		return n + len(s.tags.inbox)
	}
	drainedDuringPass := false
	for queued() > 0 {
		if s.tagRun.TryLock() { // the pass ended first
			s.tagRun.Unlock()
			break
		}
		time.Sleep(100 * time.Microsecond)
	}
	if !s.tagRun.TryLock() {
		drainedDuringPass = true
	} else {
		s.tagRun.Unlock()
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !drainedDuringPass {
		t.Fatal("inbox not applied while the pass rested", queued())
	}
	if st := s.TagCountStats(); st.InboxDropped != 0 || st.PendingDropped != 0 {
		t.Fatal(st)
	}
}

func (d *tagDelta) minute(m int) float32 {
	if h := d.hours[m/60]; h != nil {
		return h[m%60]
	}
	return 0
}

func textHash(n int32) protocol.Value {
	return protocol.Value{Type: protocol.ValueTextHash, Int: int64(n)}
}
