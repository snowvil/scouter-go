package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// The read cache is bounded by bytes and keeps the entries that are read:
// an entry read since it reached the front survives the eviction that takes
// the entries never read again.
func TestTextReadCacheKeepsEntriesInUse(t *testing.T) {
	const max = 64 << 10
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, TextCacheBytes: max})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ref := func(i int) textRef {
		return textRef{s.dailyTextPath("20261013"), string(textKey("service", int32(i), true))}
	}
	hot := []int{1, 2, 3}
	for _, i := range hot {
		s.rememberRead(ref(i), fmt.Sprintf("/hot/%d", i))
	}
	for i := 100; i < 5000; i++ { // several times the bound of one-off texts
		s.rememberRead(ref(i), fmt.Sprintf("/once/%d", i))
		if i%50 == 0 {
			for _, h := range hot {
				if _, ok := s.pendingText(ref(h)); !ok {
					t.Fatalf("hot text %d evicted after %d one-off texts", h, i-100)
				}
			}
		}
	}
	st := s.TextCacheStats()
	if st.Bytes > max || st.MaxBytes != max || st.Evicted == 0 || st.Entries >= 4900 {
		t.Fatalf("cache not bounded: %+v", st)
	}
	if _, ok := s.pendingText(ref(100)); ok {
		t.Fatal("the oldest one-off text is still cached")
	}
	// Unread for a full round, the hot texts go like any other.
	for i := 5000; i < 12000; i++ {
		s.rememberRead(ref(i), fmt.Sprintf("/once/%d", i))
	}
	s.texts.mu.Lock()
	_, kept := s.texts.read[ref(1)]
	bytes, order := s.texts.readBytes, len(s.texts.readOrder)
	sum := 0
	for r, e := range s.texts.read {
		sum += textReadCost(r, e.text)
	}
	entries := len(s.texts.read)
	s.texts.mu.Unlock()
	if kept {
		t.Fatal("an entry no longer read was kept")
	}
	if sum != bytes || order != entries {
		t.Fatalf("accounting: bytes %d (entries sum %d), order slots %d for %d entries", bytes, sum, order, entries)
	}
}

// A purge drops the cached texts of its dates and their order slots.
func TestTextReadCachePurgeReleasesSlots(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, date := range []string{"20261013", "20261014"} {
		for i := int32(0); i < 100; i++ {
			s.rememberRead(textRef{s.dailyTextPath(date), string(textKey("service", i, true))}, "/a")
		}
	}
	s.mu.Lock()
	s.discardTextsLocked([]string{filepath.Join(s.config.Directory, "20261013")})
	s.mu.Unlock()
	s.texts.mu.Lock()
	defer s.texts.mu.Unlock()
	if len(s.texts.read) != 100 || len(s.texts.readOrder) != 100 {
		t.Fatalf("entries %d, order slots %d", len(s.texts.read), len(s.texts.readOrder))
	}
}

func forgetReadTexts(s *Store) {
	q := &s.texts
	q.mu.Lock()
	q.read, q.readOrder, q.readBytes = nil, nil, 0
	q.missing, q.missingOrder = nil, nil
	q.mu.Unlock()
}

// Lookups that miss the caches read the files one at a time: the readers
// behind the first find its answer in the cache, so one text costs one file
// read (and one wait at the Store gate) however many ask for it.
func TestTextConcurrentMissesReadTheFileOnce(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.PutText("20261013", &protocol.TextPack{XType: "service", Hash: 7, Text: "/order/list"}); err != nil {
		t.Fatal(err)
	}
	if err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	forgetReadTexts(s)
	before := s.TextCacheStats().FileReads
	s.mu.Lock()
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			text, ok, err := s.TextContext(context.Background(), "20261013", "service", 7)
			if err != nil || !ok || text != "/order/list" {
				errs <- fmt.Errorf("%q %v %v", text, ok, err)
			}
		}()
	}
	time.Sleep(20 * time.Millisecond) // one at the gate, the others behind it
	s.mu.Unlock()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if n := s.TextCacheStats().FileReads - before; n != 1 {
		t.Fatalf("%d file reads for one text", n)
	}
	// A reader that gives up while it waits its turn leaves the turn free.
	forgetReadTexts(s)
	s.mu.Lock()
	first := make(chan error, 1)
	go func() {
		_, _, err := s.TextContext(context.Background(), "20261013", "service", 7)
		first <- err
	}()
	time.Sleep(10 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	_, _, err = s.TextContext(ctx, "20261013", "service", 7)
	cancel()
	s.mu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("waiting reader:", err)
	}
	if err = <-first; err != nil {
		t.Fatal(err)
	}
	if text, ok, err := s.Text("20261013", "service", 7); err != nil || !ok || text != "/order/list" {
		t.Fatal(text, ok, err)
	}
}

// TextWithin answers from the files when the Store gate is free, gives up
// after its wait when it is not, does not wait again while the store is
// slow, and leaves the answer in the caches once the read gets through.
func TestTextWithin(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	for hash, text := range map[int32]string{7: "/order/list", 8: "/pay/do"} {
		if err = s.PutText("20261013", &protocol.TextPack{XType: "service", Hash: hash, Text: text}); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	forgetReadTexts(s)
	if text, found, known, err := s.TextWithin(ctx, "20261013", "service", 7, time.Second); err != nil || !known || !found || text != "/order/list" {
		t.Fatal("free gate:", text, found, known, err)
	}
	if _, found, known, err := s.TextWithin(ctx, "20261013", "service", 99, time.Second); err != nil || !known || found {
		t.Fatal("a text the files do not have:", found, known, err)
	}
	if _, _, _, err := s.TextWithin(ctx, "2026", "service", 7, time.Second); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid date:", err)
	}
	forgetReadTexts(s)
	s.mu.Lock()
	start := time.Now()
	_, _, known, err := s.TextWithin(ctx, "20261013", "service", 7, 20*time.Millisecond)
	if d := time.Since(start); err != nil || known || d < 20*time.Millisecond || d > time.Second {
		s.mu.Unlock()
		t.Fatal("held gate:", known, err, d)
	}
	start = time.Now()
	for i := 0; i < 100; i++ { // slow: no wait, and one read in flight per text
		if _, _, known, err = s.TextWithin(ctx, "20261013", "service", int32(7+i%2), time.Second); err != nil || known {
			s.mu.Unlock()
			t.Fatal("slow store:", known, err)
		}
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		s.mu.Unlock()
		t.Fatal("lookups waited while the store was slow:", d)
	}
	if st := s.TextCacheStats(); !st.Slow || st.Slowdowns != 1 || st.Unanswered != 101 || st.FetchPending != 2 {
		s.mu.Unlock()
		t.Fatalf("%+v", st)
	}
	s.mu.Unlock()
	for deadline := time.Now().Add(5 * time.Second); s.TextCacheStats().FetchPending != 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the read ahead did not finish")
		}
	}
	for hash, want := range map[int32]string{7: "/order/list", 8: "/pay/do"} {
		if text, found, known := s.TextCached("20261013", "service", hash); !known || !found || text != want {
			t.Fatal("not cached after the read ahead:", hash, text, found, known)
		}
	}
}

// A lookup no file can answer (a date that is not a date, as the end time of
// an XLog past year 9999 gives) is refused without the Store gate, so it
// cannot hold a collector behind a slow gate.
func TestTextInvalidLookupDoesNotWaitForGate(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	done := make(chan error, 3)
	go func() {
		_, _, _, err := s.TextWithin(context.Background(), "337600101", "service", 7, time.Minute)
		done <- err
		_, _, err = s.TextContext(context.Background(), "337600101", "service", 7)
		done <- err
		_, _, err = s.TextContext(context.Background(), "20261013", "no/div", 7)
		done <- err
	}()
	for i := 0; i < 3; i++ {
		select {
		case err := <-done:
			if !errors.Is(err, ErrInvalid) {
				t.Fatal(i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("an invalid lookup waits for the held gate")
		}
	}
	if st := s.TextCacheStats(); st.FileReads != 0 || st.FetchPending != 0 {
		t.Fatalf("%+v", st)
	}
}

// Close does not wait for a read ahead stuck behind the gate it needs.
func TestTextWithinDuringClose(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	if _, _, known, err := s.TextWithin(context.Background(), "20261013", "service", 7, 0); err != nil || known {
		s.mu.Unlock()
		t.Fatal(known, err)
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	time.Sleep(20 * time.Millisecond)
	s.mu.Unlock()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close waits for the read ahead")
	}
	if _, _, _, err := s.TextWithin(context.Background(), "20261013", "service", 7, time.Second); !errors.Is(err, ErrClosed) {
		t.Fatal("after Close:", err)
	}
}

// The gzip members a pack writer builds before it takes the gate are the
// bytes a write under the gate builds: a compressed day written through the
// queue is byte for byte the day written by PutXLog.
func TestPackWriterMembersMatchInGateCompression(t *testing.T) {
	tm := time.Date(2026, 10, 13, 7, 0, 0, 0, time.UTC).UnixMilli()
	syncDir, asyncDir := t.TempDir(), t.TempDir()
	a, err := Open(Config{Directory: syncDir, Location: time.UTC, CompressXLog: true})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Open(Config{Directory: asyncDir, Location: time.UTC, CompressXLog: true})
	if err != nil {
		t.Fatal(err)
	}
	release := HoldGate(b) // the writer takes whole holds, not single records
	for i := int64(0); i < 2000; i++ {
		x, _ := packFixture(i, tm)
		if err := a.PutXLog(x); err != nil {
			t.Fatal(err)
		}
		if err := b.EnqueueXLogContext(context.Background(), x); err != nil {
			t.Fatal(err)
		}
	}
	release()
	if err := b.Sync(); err != nil {
		t.Fatal(err)
	}
	if st := b.XLogQueueStats(); st.Written != 2000 || st.Dropped != 0 || st.WriteErrors != 0 {
		t.Fatal(st)
	}
	if err := errorsJoin(a.Close(), b.Close()); err != nil {
		t.Fatal(err)
	}
	sa, sb := treeBytes(t, syncDir), treeBytes(t, asyncDir)
	dropRuntimeState(sa)
	dropRuntimeState(sb)
	if len(sa) != len(sb) {
		t.Fatal(len(sa), len(sb))
	}
	for name, want := range sa {
		if !bytes.Equal(sb[name], want) {
			t.Fatalf("%s differs", name)
		}
	}
}

// A top100 rebuild that ends at a resource limit is reported once and not
// run again (the files only grow): later contributions do not put the path
// back, its counts are still written, and a purge forgets it.
func TestTagTop100LimitIsNotRetried(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	tm := time.Date(2026, 10, 13, 1, 2, 0, 0, time.UTC).UnixMilli()
	add := func(from, to int32) {
		t.Helper()
		for svc := from; svc <= to; svc++ {
			if err := s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc}); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	add(1, 40)
	path := filepath.Join(s.config.Directory, "20261013", "tagcnt", "java")
	s.tagScanMax = 10
	if err = s.VisitorSyncContext(ctx, tm); err != nil {
		t.Fatal(err)
	}
	if err = s.FlushTagCountsIfDue(ctx); err != nil {
		t.Fatal(err)
	}
	cycle := func(n int64) error {
		t.Helper()
		if err := s.VisitorSyncContext(ctx, tm+n*300000); err != nil {
			t.Fatal(err)
		}
		return s.FlushTagCountsIfDue(ctx)
	}
	if err = cycle(1); !errors.Is(err, ErrLimit) {
		t.Fatal("first cycle:", err)
	}
	marked := func() (due, limited bool) {
		lockTags(s)
		defer s.tags.mu.Unlock()
		return s.tagCounts.cachePaths[path], s.tagCounts.cacheLimited[path]
	}
	if due, limited := marked(); due || !limited {
		t.Fatal("after the limit:", due, limited)
	}
	add(41, 60)
	if due, _ := marked(); due {
		t.Fatal("a contribution put the limited path back")
	}
	if err = cycle(2); err != nil {
		t.Fatal("rebuilt again:", err)
	}
	if st := s.TagCountStats(); st.CacheLimited != 1 {
		t.Fatalf("%+v", st)
	}
	if _, err = s.DeleteDatabasePaths(ctx, []string{"/20261013"}); err != nil {
		t.Fatal(err)
	}
	if _, limited := marked(); limited {
		t.Fatal("the purge kept the limited path")
	}
}
