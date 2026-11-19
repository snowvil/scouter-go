package storage_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/ingest"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

// These tests drive the ingest engine against a Store whose gate is held (a
// stalled disk or a long hold): a collector that has to wait for a storage
// queue or a text file read must wait without the engine lock, so another
// collector's pack goes through meanwhile (ingest CONTRACT §11).

const lockTestDate = "20261022"

var lockTestNow = time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)

func lockTestEngine(t *testing.T, dir string, cfg storage.Config) (*ingest.Engine, *storage.Store) {
	t.Helper()
	cfg.Directory, cfg.Location = dir, time.UTC
	s, err := storage.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, err := storage.OpenCounters(dir, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	e, err := ingest.New(ingest.Config{Location: time.UTC, Clock: func() time.Time { return lockTestNow }, XLogCapacity: 64}, s, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close(); c.Close(); s.Close() })
	return e, s
}

// otherCollector accepts a counter pack, as another UDP worker would, and
// returns how long it took; it fails the test when the engine lock does not
// come within limit.
func otherCollector(t *testing.T, e *ingest.Engine, limit time.Duration) time.Duration {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	p := &protocol.PerfCounterPack{ObjName: "/other", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueFloat, Float: 1}}}}}
	start := time.Now()
	if err := e.Accept(ctx, p); err != nil {
		t.Errorf("another collector waited for the engine lock: %v after %v", err, time.Since(start))
	}
	return time.Since(start)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for", what)
		}
	}
}

// A full text, XLog or profile queue makes the collecting goroutine wait
// (at most the queue's 100ms, then the record is dropped and counted), but
// not under the engine lock: another collector is not held up.
func TestIngestFullStorageQueueWaitsOutsideEngineLock(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fill    func(t *testing.T, s *storage.Store)
		pack    protocol.Pack
		dropped func(s *storage.Store) uint64
	}{{
		name: "text",
		fill: func(t *testing.T, s *storage.Store) {
			put := func(h int32) {
				if err := s.PutText(lockTestDate, &protocol.TextPack{XType: "sql", Hash: h, Text: "q"}); err != nil {
					t.Fatal(err)
				}
			}
			put(1)
			waitFor(t, "the text writer to take a text", func() bool { n, _, _ := storage.WriterQueueLens(s); return n == 0 })
			for h := int32(2); h <= storage.TextQueueMax+1; h++ {
				put(h)
			}
		},
		pack:    &protocol.TextPack{XType: "sql", Hash: -1, Text: "full"},
		dropped: func(s *storage.Store) uint64 { return s.TextDropped() },
	}, {
		name: "xlog",
		fill: func(t *testing.T, s *storage.Store) {
			put := func(i int64) {
				if err := s.EnqueueXLogContext(context.Background(), &protocol.XLogPack{EndTime: lockTestNow.UnixMilli(), TxID: 1000 + i, Service: 1}); err != nil {
					t.Fatal(err)
				}
			}
			put(0)
			waitFor(t, "the XLog writer to take a record", func() bool { at, _ := storage.PackWritersAtGate(s); return at })
			put(1)
			put(2)
		},
		// XType 2 (no group step) so only the XLog queue is involved.
		pack:    &protocol.XLogPack{TxID: 1, XType: 2, Service: 1, TailVersion: protocol.XLogCurrentTail},
		dropped: func(s *storage.Store) uint64 { return s.XLogQueueStats().Dropped },
	}, {
		name: "profile",
		fill: func(t *testing.T, s *storage.Store) {
			put := func(i int64) {
				if err := s.EnqueueProfileContext(context.Background(), &protocol.ProfilePack{Time: lockTestNow.UnixMilli(), TxID: 1000 + i, Profile: []byte{1}}); err != nil {
					t.Fatal(err)
				}
			}
			put(0)
			waitFor(t, "the profile writer to take a record", func() bool { _, at := storage.PackWritersAtGate(s); return at })
			put(1)
			put(2)
		},
		pack:    &protocol.ProfilePack{TxID: 1, Profile: []byte{1, 2, 3}},
		dropped: func(s *storage.Store) uint64 { return s.ProfileQueueStats().Dropped },
	}} {
		t.Run(tc.name, func(t *testing.T) {
			e, s := lockTestEngine(t, t.TempDir(), storage.Config{XLogQueueSize: 2, ProfileQueueSize: 2})
			release := storage.HoldGate(s)
			released := false
			defer func() {
				if !released {
					release()
				}
			}()
			tc.fill(t, s)
			if n := tc.dropped(s); n != 0 {
				t.Fatal("dropped while filling", n)
			}
			var wg sync.WaitGroup
			wg.Add(1)
			var acceptErr error
			var waited time.Duration
			go func() {
				defer wg.Done()
				start := time.Now()
				acceptErr = e.Accept(context.Background(), tc.pack)
				waited = time.Since(start)
			}()
			time.Sleep(20 * time.Millisecond) // the collector is waiting for room
			if d := otherCollector(t, e, 2*time.Second); d > 50*time.Millisecond {
				t.Errorf("another collector took %v while a full %s queue was waited for", d, tc.name)
			}
			wg.Wait()
			if acceptErr != nil {
				t.Fatal(acceptErr)
			}
			if waited < 50*time.Millisecond {
				t.Errorf("no bounded wait for room: %v", waited)
			}
			if n := tc.dropped(s); n != 1 {
				t.Errorf("dropped %d, want 1 (after the bounded wait)", n)
			}
			release()
			released = true
			if st := e.Stats(); st.Completed != 2 || st.Rejected != 0 {
				t.Errorf("stats %+v", st)
			}
		})
	}
}

// writeServiceTexts stores service texts in a Store that is then closed, so
// the next Store finds them only in the files (no read cache).
func writeServiceTexts(t *testing.T, dir string, texts map[int32]string) {
	t.Helper()
	s, err := storage.Open(storage.Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	for hash, text := range texts {
		if err := s.PutText(lockTestDate, &protocol.TextPack{XType: "service", Hash: hash, Text: text}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// The group step of an XLog whose service text is in no cache needs the text
// file, which is read under the Store gate. The read is done for the
// collector without the engine lock, so a held gate does not hold up another
// collector; when the gate is free the stored XLog carries the group of its
// service text (XLogGroupUtil).
func TestIngestColdServiceTextReadOutsideEngineLock(t *testing.T) {
	dir := t.TempDir()
	writeServiceTexts(t, dir, map[int32]string{7: "/order/list"})
	e, s := lockTestEngine(t, dir, storage.Config{})
	if err := e.Accept(context.Background(), &protocol.XLogPack{TxID: 1, Service: 7, ObjHash: 3, Elapsed: 5}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	x, err := s.XLog(lockTestDate, 1)
	if err != nil || x == nil || x.Group != storage.CounterHash("/order") {
		t.Fatalf("stored XLog %+v %v, want group %d", x, err, storage.CounterHash("/order"))
	}
	if st := e.Stats(); st.ServiceTextsAhead != 1 || st.ServiceTextSkipped != 0 {
		t.Fatalf("stats %+v", st)
	}
	if st := s.TextCacheStats(); st.FileReads != 1 || st.Waited != 1 || st.Unanswered != 0 {
		t.Fatalf("text cache %+v", st)
	}
}

// A Store gate that stays held (a writer waiting for a cold disk) does not
// hold a collector: an XLog whose service text is in no cache waits for the
// read only for a bounded time, is then stored without its group (counted),
// and the XLogs after it do not wait at all while the store is slow. The
// read goes on behind the gate, so once the gate is free the service's
// XLogs get their group again.
func TestIngestColdServiceTextDoesNotWaitForHeldGate(t *testing.T) {
	dir := t.TempDir()
	writeServiceTexts(t, dir, map[int32]string{7: "/order/list", 8: "/pay/do"})
	e, s := lockTestEngine(t, dir, storage.Config{})
	ctx := context.Background()
	release := storage.HoldGate(s)
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	start := time.Now()
	if err := e.Accept(ctx, &protocol.XLogPack{TxID: 1, Service: 7, ObjHash: 3, Elapsed: 5}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("the collector waited %v for the held gate", d)
	}
	// Slow from now on: these do not wait at all (the same text, another
	// text, and the same again).
	start = time.Now()
	for txid, service := range map[int64]int32{2: 7, 3: 8, 4: 7} {
		if err := e.Accept(ctx, &protocol.XLogPack{TxID: txid, Service: service, ObjHash: 3, Elapsed: 5}); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d > 40*time.Millisecond {
		t.Errorf("3 XLogs took %v while the store was slow", d)
	}
	if st := e.Stats(); st.ServiceTextSkipped != 4 || st.Completed != 4 || st.Rejected != 0 {
		t.Fatalf("stats %+v", st)
	}
	if st := s.TextCacheStats(); st.Slowdowns != 1 || st.Unanswered != 4 || !st.Slow || st.FetchPending != 2 {
		t.Fatalf("text cache %+v", st)
	}
	if d := otherCollector(t, e, time.Second); d > 50*time.Millisecond {
		t.Errorf("another collector took %v", d)
	}
	release()
	released = true
	waitFor(t, "the texts to be read ahead", func() bool { return s.TextCacheStats().FetchPending == 0 })
	if err := e.Accept(ctx, &protocol.XLogPack{TxID: 5, Service: 7, ObjHash: 3, Elapsed: 5}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	for txid, group := range map[int64]int32{1: 0, 2: 0, 3: 0, 4: 0, 5: storage.CounterHash("/order")} {
		x, err := s.XLog(lockTestDate, txid)
		if err != nil || x == nil || x.Group != group {
			t.Fatalf("stored XLog %d %+v %v, want group %d", txid, x, err, group)
		}
	}
	if st := s.TextCacheStats(); st.FileReads != 2 {
		t.Fatalf("each text is read once: %+v", st)
	}
}

// An XLog whose end time gives no valid date is rejected as before, without
// waiting for a held Store gate.
func TestIngestInvalidDateServiceTextDoesNotWaitForHeldGate(t *testing.T) {
	e, s := lockTestEngine(t, t.TempDir(), storage.Config{})
	release := storage.HoldGate(s)
	defer release()
	done := make(chan error, 1)
	go func() {
		done <- e.Accept(context.Background(), &protocol.XLogPack{TxID: 1, Service: 7, EndTime: 1 << 50})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, storage.ErrInvalid) {
			t.Fatalf("accept %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the collector waits for the held gate")
	}
	if d := otherCollector(t, e, time.Second); d > 50*time.Millisecond {
		t.Errorf("another collector took %v", d)
	}
}

// An XLog delayed by sampling has its service text read ahead like any XLog
// (before its own hold), so when its driving XLog releases it with the Store
// gate held its group step needs no file read under the engine lock; a
// service without a text is read once ahead and then answered "not found"
// from the cache (group 0, as before).
func TestIngestDelayedServiceTextReadAhead(t *testing.T) {
	dir := t.TempDir()
	writeServiceTexts(t, dir, map[int32]string{7: "/order/list", 9: "/pay/do"})
	e, s := lockTestEngine(t, dir, storage.Config{})
	ctx := context.Background()
	accept := func(x *protocol.XLogPack) {
		t.Helper()
		x.TailVersion = protocol.XLogCurrentTail
		if err := e.Accept(ctx, x); err != nil {
			t.Fatal(err)
		}
	}
	accept(&protocol.XLogPack{TxID: 1, Service: 7})                               // memo of service 7
	accept(&protocol.XLogPack{TxID: 2, Service: 8})                               // no text: read ahead, "not found"
	accept(&protocol.XLogPack{TxID: 21, GxID: 20, Service: 9, DiscardType: 2})    // delayed child
	accept(&protocol.XLogPack{TxID: 3, Service: 8})                               // answered from the cache
	if st := e.Stats(); st.ServiceTextsAhead != 3 || st.ServiceTextSkipped != 0 { // services 7, 8, 9
		t.Fatalf("stats %+v", st)
	}
	release := storage.HoldGate(s)
	done := make(chan error, 1)
	go func() {
		done <- e.Accept(ctx, &protocol.XLogPack{TxID: 20, GxID: 20, Service: 7, DiscardType: 1, TailVersion: protocol.XLogCurrentTail})
	}()
	select {
	case err := <-done:
		if err != nil {
			release()
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		release()
		<-done
		t.Fatal("the released child read its service text behind the held gate")
	}
	release()
	if st := e.Stats(); st.ServiceTextSkipped != 0 || st.Completed != 5 || st.Rejected != 0 {
		t.Fatalf("stats %+v", st)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	for txid, group := range map[int64]int32{1: storage.CounterHash("/order"), 2: 0, 3: 0, 20: storage.CounterHash("/order"), 21: storage.CounterHash("/pay")} {
		x, err := s.XLog(lockTestDate, txid)
		if err != nil || x == nil || x.Group != group {
			t.Errorf("txid %d: %+v %v, want group %d", txid, x, err, group)
		}
	}
}

// The XLogs one hold stores (released children, then the driving XLog) reach
// the XLog file in the same order whether the queue had room or was full and
// they were submitted after the engine lock.
func TestIngestDeferredXLogsKeepTheirOrder(t *testing.T) {
	order := func(full bool) []int64 {
		t.Helper()
		e, s := lockTestEngine(t, t.TempDir(), storage.Config{XLogQueueSize: 8})
		ctx := context.Background()
		for _, txid := range []int64{11, 12, 13} {
			// XType 2: no group step (no service text read behind the gate).
			if err := e.Accept(ctx, &protocol.XLogPack{TxID: txid, GxID: 10, XType: 2, Service: 1, DiscardType: 2, TailVersion: protocol.XLogCurrentTail}); err != nil {
				t.Fatal(err)
			}
		}
		var release func()
		if full {
			release = storage.HoldGate(s)
			for i := int64(0); i <= 8; i++ { // one taken by the blocked writer, 8 queued
				if err := s.EnqueueXLogContext(ctx, &protocol.XLogPack{EndTime: lockTestNow.UnixMilli(), TxID: 1000 + i, GxID: 999, Service: 1}); err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					waitFor(t, "the XLog writer to take a record", func() bool { at, _ := storage.PackWritersAtGate(s); return at })
				}
			}
			time.AfterFunc(30*time.Millisecond, release) // room before the 100ms bound
		}
		if err := e.Accept(ctx, &protocol.XLogPack{TxID: 10, GxID: 10, XType: 2, Service: 1, DiscardType: 1, TailVersion: protocol.XLogCurrentTail}); err != nil {
			t.Fatal(err)
		}
		st := e.Stats()
		if want := map[bool]uint64{false: 0, true: 4}[full]; st.StoreWritesDeferred != want {
			t.Fatalf("full=%v: deferred %d, want %d", full, st.StoreWritesDeferred, want)
		}
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if q := s.XLogQueueStats(); q.Dropped != 0 {
			t.Fatalf("full=%v: dropped %+v", full, q)
		}
		xs, err := s.XLogsByGxIDContext(ctx, lockTestDate, 10)
		if err != nil {
			t.Fatal(err)
		}
		var txids []int64
		for _, x := range xs {
			txids = append(txids, x.TxID)
		}
		return txids
	}
	free, full := order(false), order(true)
	if len(free) != 4 || len(full) != 4 {
		t.Fatalf("stored %v and %v", free, full)
	}
	for i := range free {
		if free[i] != full[i] {
			t.Fatalf("order with room %v, after a full queue %v", free, full)
		}
	}
}

// A service text whose text queue was full stays readable while its collector
// waits for room after the engine lock: another collector's XLog of that
// service, accepted meanwhile, gets its group from it (as when the waiting
// PutText held the engine lock), not the "not found" cached before the text
// arrived.
func TestIngestDeferredTextVisibleToOtherCollectors(t *testing.T) {
	e, s := lockTestEngine(t, t.TempDir(), storage.Config{})
	ctx := context.Background()
	// Service 7 has no text yet: the files are read once, "not found" is
	// cached and the XLog stored without a group (this also queues the
	// group seed texts).
	if err := e.Accept(ctx, &protocol.XLogPack{TxID: 100, Service: 7}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil { // the text queue is empty
		t.Fatal(err)
	}
	release := storage.HoldGate(s)
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	// Full by bytes: a waiting text is then not pending until it has room
	// (full by count it would be, while it waits for the channel).
	fillTextQueueBytes(t, s)
	done := make(chan error, 1)
	go func() {
		done <- e.Accept(ctx, &protocol.TextPack{XType: "service", Hash: 7, Text: "/order/list"})
	}()
	waitFor(t, "the service text to be deferred", func() bool { return e.Stats().StoreWritesDeferred > 0 })
	if err := e.Accept(ctx, &protocol.XLogPack{TxID: 1, Service: 7}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	release()
	released = true
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	for txid, group := range map[int64]int32{100: 0, 1: storage.CounterHash("/order")} {
		x, err := s.XLog(lockTestDate, txid)
		if err != nil || x == nil || x.Group != group {
			t.Errorf("txid %d: %+v %v, want group %d", txid, x, err, group)
		}
	}
}

// fillTextQueueBytes, with the Store gate held by the caller, fills the text
// queue by bytes to 10 bytes short of its bound, so a
// waiting text is not pending until it has room.
func fillTextQueueBytes(t *testing.T, s *storage.Store) {
	t.Helper()
	put := func(h int32, n int) {
		if err := s.PutText(lockTestDate, &protocol.TextPack{XType: "sql", Hash: h, Text: strings.Repeat("q", n)}); err != nil {
			t.Fatal(err)
		}
	}
	big := 4<<20 - 4096 // under MaxRecordBytes; 8 of them leave about 32KiB
	put(1, big)
	waitFor(t, "the text writer to take a text", func() bool { n, _, _ := storage.WriterQueueLens(s); return n == 0 })
	for h := int32(2); h <= 8; h++ {
		put(h, big)
	}
	put(9, storage.TextQueueMaxBytes-10-storage.TextQueueBytes(s)-storage.TextQueueOverhead(s, "sql"))
	if n := storage.TextQueueBytes(s); n != storage.TextQueueMaxBytes-10 || s.TextDropped() != 0 {
		t.Fatalf("text queue bytes %d, dropped %d", n, s.TextDropped())
	}
}

// A Store closed while a collector waits for room for a deferred write
// (after the engine lock) fails that pack with ErrClosed, counted as before
// (Rejected, StorageErrors), and the engine Close waits for it.
func TestIngestCloseDuringDeferredSubmit(t *testing.T) {
	e, s := lockTestEngine(t, t.TempDir(), storage.Config{})
	release := storage.HoldGate(s)
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	fillTextQueueBytes(t, s)
	done := make(chan error, 1)
	go func() {
		done <- e.Accept(context.Background(), &protocol.TextPack{XType: "service", Hash: 7, Text: "/order/list"})
	}()
	waitFor(t, "the text to be deferred", func() bool { return e.Stats().StoreWritesDeferred > 0 })
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	time.Sleep(20 * time.Millisecond) // Close stops taking texts, then waits for the writer
	release()
	released = true
	if err := <-done; !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("accept during close: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if st := e.Stats(); st.Rejected != 1 || st.StorageErrors != 1 || st.Completed != 0 {
		t.Fatalf("stats %+v", st)
	}
}

// The service text read ahead of the engine lock follows the collector's
// ctx: a canceled collector stops waiting for a held Store gate and its pack
// is rejected (the in-lock read used to wait for the gate whatever the ctx).
func TestIngestServiceTextReadAheadCanceled(t *testing.T) {
	e, s := lockTestEngine(t, t.TempDir(), storage.Config{})
	release := storage.HoldGate(s)
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond) // well under the bounded wait
	defer cancel()
	if err := e.Accept(ctx, &protocol.XLogPack{TxID: 1, Service: 7}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("accept %v", err)
	}
	if st := e.Stats(); st.Rejected != 1 || st.Completed != 0 {
		t.Fatalf("stats %+v", st)
	}
	if d := otherCollector(t, e, time.Second); d > 50*time.Millisecond {
		t.Errorf("engine lock held after the canceled read: %v", d)
	}
}

// A service text read that fails rejects the XLog with the error the read
// returns, as the read under the engine lock did; the files are read once
// (ahead of the lock).
func TestIngestServiceTextReadErrorRejectsAsBefore(t *testing.T) {
	dir := t.TempDir()
	writeServiceTexts(t, dir, map[int32]string{7: "/order/list"})
	data := filepath.Join(dir, "00000000", "text", "text_service.data")
	if err := os.Truncate(data, 0); err != nil {
		t.Fatal(err)
	}
	direct, err := storage.Open(storage.Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	_, _, want := direct.Text(lockTestDate, "service", 7)
	if err := direct.Close(); err != nil {
		t.Fatal(err)
	}
	if want == nil {
		t.Fatal("the truncated text file reads without an error")
	}
	e, _ := lockTestEngine(t, dir, storage.Config{})
	got := e.Accept(context.Background(), &protocol.XLogPack{TxID: 1, Service: 7})
	if got == nil || got.Error() != want.Error() {
		t.Fatalf("accept %v, want %v", got, want)
	}
	if st := e.Stats(); st.Rejected != 1 || st.ServiceTextsAhead != 1 || st.ServiceTextSkipped != 0 {
		t.Fatalf("stats %+v", st)
	}
}

// A hold that stores more XLogs than the held bound (a driving XLog
// releasing many children) while the XLog queue is full only for a moment
// loses none of them: past the bound the held writes are submitted in order
// under the lock, as every write waited before, and the writer catches up.
func TestIngestManyReleasedXLogsWithBriefFullQueue(t *testing.T) {
	e, s := lockTestEngine(t, t.TempDir(), storage.Config{XLogQueueSize: 64})
	ctx := context.Background()
	const children = 1300
	for i := int64(0); i < children; i++ {
		if err := e.Accept(ctx, &protocol.XLogPack{TxID: 100 + i, GxID: 10, XType: 2, Service: 1, DiscardType: 2, TailVersion: protocol.XLogCurrentTail}); err != nil {
			t.Fatal(err)
		}
	}
	release := storage.HoldGate(s)
	time.AfterFunc(20*time.Millisecond, release) // the writer resumes shortly
	if err := e.Accept(ctx, &protocol.XLogPack{TxID: 10, GxID: 10, XType: 2, Service: 1, DiscardType: 1, TailVersion: protocol.XLogCurrentTail}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if q := s.XLogQueueStats(); q.Dropped != 0 || q.Written != children+1 {
		t.Fatalf("XLog queue %+v, want all %d written", q, children+1)
	}
	if e.Stats().StoreWritesDeferred == 0 {
		t.Fatal("the queue was never full")
	}
}

// A profile near MaxRecordBytes whose queue is full waits for room like any
// write (the held byte bound does not apply to the first held write).
func TestIngestLargeProfileWithFullQueueWaits(t *testing.T) {
	e, s := lockTestEngine(t, t.TempDir(), storage.Config{ProfileQueueSize: 2})
	ctx := context.Background()
	release := storage.HoldGate(s)
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	put := func(i int64) {
		if err := s.EnqueueProfileContext(ctx, &protocol.ProfilePack{Time: lockTestNow.UnixMilli(), TxID: 1000 + i, Profile: []byte{1}}); err != nil {
			t.Fatal(err)
		}
	}
	put(0)
	waitFor(t, "the profile writer to take a record", func() bool { _, at := storage.PackWritersAtGate(s); return at })
	put(1)
	put(2)
	time.AfterFunc(20*time.Millisecond, func() { release() })
	released = true
	big := make([]byte, 4<<20-10)
	if err := e.Accept(ctx, &protocol.ProfilePack{TxID: 1, Profile: big}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if q := s.ProfileQueueStats(); q.Dropped != 0 || q.Written != 4 {
		t.Fatalf("profile queue %+v", q)
	}
}
