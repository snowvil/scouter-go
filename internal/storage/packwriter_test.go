package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func packFixture(i int64, tm int64) (*protocol.XLogPack, *protocol.ProfilePack) {
	x := &protocol.XLogPack{EndTime: tm + i*7, ObjHash: 42, TxID: i + 1, Service: int32(i % 5), Elapsed: int32(i)}
	if i%3 == 0 {
		x.GxID = i + 1000
	}
	return x, &protocol.ProfilePack{Time: tm + i*7, TxID: i + 1, Profile: bytes.Repeat([]byte{byte(i)}, int(1+i%300))}
}

// The writers store what PutXLog/PutProfile store, file for file, and in the
// same order of each kind (XLogWR: data, then the time, txid, gxid index);
// compressed, every record reads back.
func TestPackWritersMatchSyncWrites(t *testing.T) {
	tm := time.Date(2026, 10, 13, 7, 0, 0, 0, time.UTC).UnixMilli()
	for _, compress := range []bool{false, true} {
		syncDir, asyncDir := t.TempDir(), t.TempDir()
		a, err := Open(Config{Directory: syncDir, Location: time.UTC, CompressXLog: compress, CompressProfile: compress})
		if err != nil {
			t.Fatal(err)
		}
		b, err := Open(Config{Directory: asyncDir, Location: time.UTC, CompressXLog: compress, CompressProfile: compress})
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		for i := int64(0); i < 3000; i++ {
			x, p := packFixture(i, tm)
			if err := a.PutXLog(x); err != nil {
				t.Fatal(err)
			}
			if err := a.PutProfile(p); err != nil {
				t.Fatal(err)
			}
			if err := b.EnqueueXLogContext(ctx, x); err != nil {
				t.Fatal(err)
			}
			if err := b.EnqueueProfileContext(ctx, p); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Sync(); err != nil {
			t.Fatal(err)
		}
		x, err := b.XLog("20261013", 2999)
		if err != nil || x == nil || x.TxID != 2999 {
			t.Fatal("not readable after Sync", x, err)
		}
		if st := b.XLogQueueStats(); st.Enqueued != 3000 || st.Written != 3000 || st.Dropped != 0 || st.WriteErrors != 0 {
			t.Fatal(st)
		}
		if st := b.ProfileQueueStats(); st.Enqueued != 3000 || st.Written != 3000 {
			t.Fatal(st)
		}
		if compress {
			// XLogs and profiles share the day's compressed store
			// (appendZip), so their positions follow how the two writers
			// interleave, as with the original's two writer threads: check
			// every record instead of the bytes.
			for i := int64(0); i < 3000; i++ {
				want, wp := packFixture(i, tm)
				got, err := b.XLog("20261013", want.TxID)
				if err != nil || got == nil || got.EndTime != want.EndTime || got.Elapsed != want.Elapsed {
					t.Fatal("compressed xlog", i, got, err)
				}
				prof, err := b.Profile("20261013", want.TxID)
				if err != nil || !bytes.Equal(prof, wp.Profile) {
					t.Fatal("compressed profile", i, err)
				}
			}
			if err := errorsJoin(a.Close(), b.Close()); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := errorsJoin(a.Close(), b.Close()); err != nil {
			t.Fatal(err)
		}
		sa, sb := treeBytes(t, syncDir), treeBytes(t, asyncDir)
		dropRuntimeState(sa) // close time and checkpoint log, not DB content
		dropRuntimeState(sb)
		if len(sa) != len(sb) {
			t.Fatal(compress, len(sa), len(sb))
		}
		for name, want := range sa {
			if !bytes.Equal(sb[name], want) {
				t.Fatalf("compress=%v %s differs", compress, name)
			}
		}
	}
}

func errorsJoin(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// A stalled writer (the Store gate held) costs a sender one bounded wait per
// saturation; then records are dropped at once, counted and logged S144 /
// S142 as XLogWR / XLogProfileWR do, and the writer's progress ends the
// saturation. The engine lock is never held longer than that wait.
func TestPackQueueStalledDropsWithoutSlowingIngest(t *testing.T) {
	var mu sync.Mutex
	logged := map[string]int{}
	s, _ := liveStore(t, Config{XLogQueueSize: 64, ProfileQueueSize: 8, Log: func(id string, _ int, _ string) {
		mu.Lock()
		logged[id]++
		mu.Unlock()
	}})
	ctx := context.Background()
	tm := time.Date(2026, 10, 13, 7, 0, 0, 0, time.UTC).UnixMilli()
	s.mu.Lock() // a stalled disk or a long hold
	putX := func(i int64) {
		x, _ := packFixture(i, tm)
		if err := s.EnqueueXLogContext(ctx, x); err != nil {
			t.Error(err)
		}
	}
	putP := func(i int64) {
		_, p := packFixture(i, tm)
		if err := s.EnqueueProfileContext(ctx, p); err != nil {
			t.Error(err)
		}
	}
	put := func(i int64) { putX(i); putP(i) }
	// The writers take one record each and wait for the gate; then fill the
	// queues to capacity.
	put(0)
	for deadline := time.Now().Add(5 * time.Second); !s.xlogQ.atGate.Load() || !s.profQ.atGate.Load(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			s.mu.Unlock()
			t.Fatal("writers did not take the first records")
		}
	}
	for i := int64(1); i <= 64; i++ {
		putX(i)
	}
	for i := int64(1); i <= 8; i++ {
		putP(i)
	}
	if s.XLogQueueStats().Dropped+s.ProfileQueueStats().Dropped != 0 {
		s.mu.Unlock()
		t.Fatal("dropped below capacity")
	}
	start := time.Now()
	put(1000) // each queue waits for room once, then drops
	if d := time.Since(start); d < 2*packQueueWait {
		s.mu.Unlock()
		t.Fatal("no wait before the first drop", d)
	}
	start = time.Now()
	for i := int64(2000); i < 3000; i++ {
		put(i)
	}
	if d := time.Since(start); d > packQueueWait {
		s.mu.Unlock()
		t.Fatal("senders slowed while saturated", d)
	}
	xs, ps := s.XLogQueueStats(), s.ProfileQueueStats()
	s.mu.Unlock()
	if xs.Dropped < 1000 || ps.Dropped < 1000 {
		t.Fatal("drops", xs, ps)
	}
	mu.Lock()
	if logged["S144"] == 0 || logged["S142"] == 0 {
		t.Fatal("drops not logged", logged)
	}
	mu.Unlock()
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	x, _ := packFixture(5000, tm)
	if err := s.EnqueueXLogContext(ctx, x); err != nil { // room again
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if got, err := s.XLog("20261013", x.TxID); err != nil || got == nil {
		t.Fatal("not stored after the stall", got, err)
	}
	if st := s.XLogQueueStats(); st.Written != st.Enqueued || st.Len != 0 || st.WriteErrors != 0 {
		t.Fatal(st)
	}
}

// Close writes every accepted record before it closes the files; a sender
// after Close gets ErrClosed.
func TestPackQueueCloseWritesAccepted(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tm := time.Date(2026, 10, 13, 7, 0, 0, 0, time.UTC).UnixMilli()
	for i := int64(0); i < 2000; i++ {
		x, p := packFixture(i, tm)
		if err := s.EnqueueXLogContext(ctx, x); err != nil {
			t.Fatal(err)
		}
		if err := s.EnqueueProfileContext(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	x, p := packFixture(1, tm)
	if err := s.EnqueueXLogContext(ctx, x); err != ErrClosed {
		t.Fatal(err)
	}
	if err := s.EnqueueProfileContext(ctx, p); err != ErrClosed {
		t.Fatal(err)
	}
	s, err = Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := int64(0); i < 2000; i += 199 {
		want, wp := packFixture(i, tm)
		got, err := s.XLog("20261013", want.TxID)
		if err != nil || got == nil || got.Elapsed != want.Elapsed {
			t.Fatal(i, got, err)
		}
		b, err := s.Profile("20261013", want.TxID)
		if err != nil || !bytes.Equal(b, wp.Profile) {
			t.Fatal("profile", i, len(b), err)
		}
	}
}

// A database delete of a day removes the records queued before it with the
// directory, as when writes were synchronous: the writer does not recreate
// the removed directory. A record accepted after the delete is written.
func TestPackQueueDeleteDiscardsQueuedRecords(t *testing.T) {
	s, root := liveStore(t, Config{})
	ctx := context.Background()
	tm := time.Date(2026, 10, 13, 7, 0, 0, 0, time.UTC).UnixMilli()
	x0, p0 := packFixture(0, tm)
	if err := s.PutXLog(x0); err != nil { // the day exists on disk
		t.Fatal(err)
	}
	s.mu.Lock() // the writers wait for the gate
	for i := int64(1); i <= 50; i++ {
		x, p := packFixture(i, tm)
		if err := s.EnqueueXLogContext(ctx, x); err != nil {
			t.Fatal(err)
		}
		if err := s.EnqueueProfileContext(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.Unlock()
	// The delete may land after some records were written; none may be
	// written after it.
	if _, err := s.DeleteDatabasePaths(ctx, []string{"/20261013"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "20261013")); !os.IsNotExist(err) {
		t.Fatal("deleted day recreated by queued records", err)
	}
	xs, ps := s.XLogQueueStats(), s.ProfileQueueStats()
	if xs.Written+xs.Discarded != 50 || ps.Written+ps.Discarded != 50 || xs.WriteErrors+ps.WriteErrors != 0 {
		t.Fatal(xs, ps)
	}
	_ = p0
	x, _ := packFixture(99, tm)
	if err := s.EnqueueXLogContext(ctx, x); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if got, err := s.XLog("20261013", x.TxID); err != nil || got == nil {
		t.Fatal("record after the delete not written", got, err)
	}
}

// Sync completes while a queue is saturated; the byte bound holds with large
// records (one record alone may exceed it); a cancelled wait for room
// returns the context error and gives the room back.
func TestPackQueueByteBoundSyncAndCancel(t *testing.T) {
	s, _ := liveStore(t, Config{MaxRecordBytes: 32 << 20})
	ctx := context.Background()
	tm := time.Date(2026, 10, 13, 7, 0, 0, 0, time.UTC).UnixMilli()
	big := bytes.Repeat([]byte{7}, 20<<20)
	s.mu.Lock()
	put := func(c context.Context, txid int64) error {
		return s.EnqueueProfileContext(c, &protocol.ProfilePack{Time: tm, TxID: txid, Profile: big})
	}
	if err := put(ctx, 1); err != nil { // the writer takes it
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); len(s.profQ.queue) != 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			s.mu.Unlock()
			t.Fatal("writer did not take the first record")
		}
	}
	queued := 0
	for txid := int64(2); txid < 20; txid++ {
		c, cancel := context.WithTimeout(ctx, packQueueWait/4)
		err := put(c, txid)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			break // the byte bound
		}
		if err != nil {
			s.mu.Unlock()
			t.Fatal(err)
		}
		queued++
	}
	s.profQ.mu.Lock()
	bytesQueued := s.profQ.bytes
	s.profQ.mu.Unlock()
	if queued < 1 || bytesQueued > packQueueMaxBytes+len(big)+64 || s.ProfileQueueStats().Dropped != 0 {
		s.mu.Unlock()
		t.Fatal("byte bound", queued, bytesQueued, s.ProfileQueueStats())
	}
	synced := make(chan error, 1)
	go func() { synced <- s.Sync() }()
	time.Sleep(10 * time.Millisecond)
	s.mu.Unlock()
	select {
	case err := <-synced:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Sync did not complete")
	}
	if st := s.ProfileQueueStats(); st.Written != uint64(queued)+1 || st.Len != 0 {
		t.Fatal(st)
	}
	s.profQ.mu.Lock()
	left := s.profQ.bytes
	s.profQ.mu.Unlock()
	if left != 0 {
		t.Fatal("bytes not returned", left)
	}
}

// Concurrent senders and Close: every accepted record is written, every
// other call returns ErrClosed or nil (-race).
func TestPackQueueConcurrentEnqueueAndClose(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tm := time.Date(2026, 10, 13, 7, 0, 0, 0, time.UTC).UnixMilli()
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := map[int64]bool{}
	for w := int64(0); w < 4; w++ {
		wg.Add(1)
		go func(w int64) {
			defer wg.Done()
			for i := int64(0); i < 500; i++ {
				x, _ := packFixture(w*1000+i, tm)
				switch err := s.EnqueueXLogContext(ctx, x); err {
				case nil:
					mu.Lock()
					accepted[x.TxID] = true
					mu.Unlock()
				case ErrClosed:
					return
				default:
					t.Error(err)
					return
				}
			}
		}(w)
	}
	time.Sleep(5 * time.Millisecond)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	st := s.XLogQueueStats()
	if st.Written != st.Enqueued || st.Dropped != 0 || uint64(len(accepted)) != st.Enqueued {
		t.Fatal(st, len(accepted))
	}
	s, err = Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for txid := range accepted {
		if got, err := s.XLog("20261013", txid); err != nil || got == nil {
			t.Fatal("accepted record missing", txid, err)
		}
	}
}

// Ordered for certain: records queued while a delete of their day runs under
// the gate (what DeleteDatabasePaths does there) are all discarded.
func TestPackQueueDiscardWhileQueued(t *testing.T) {
	s, root := liveStore(t, Config{})
	ctx := context.Background()
	tm := time.Date(2026, 10, 13, 7, 0, 0, 0, time.UTC).UnixMilli()
	x0, _ := packFixture(0, tm)
	if err := s.PutXLog(x0); err != nil {
		t.Fatal(err)
	}
	day := filepath.Join(root, "20261013")
	s.mu.Lock()
	for i := int64(1); i <= 50; i++ {
		x, p := packFixture(i, tm)
		if err := s.EnqueueXLogContext(ctx, x); err != nil {
			t.Fatal(err)
		}
		if err := s.EnqueueProfileContext(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	s.discardPacksLocked([]string{day})
	if err := s.releaseLocked([]string{day}); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(day); err != nil {
		t.Fatal(err)
	}
	s.mu.Unlock()
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(day); !os.IsNotExist(err) {
		t.Fatal("removed day recreated", err)
	}
	if xs, ps := s.XLogQueueStats(), s.ProfileQueueStats(); xs.Discarded != 50 || ps.Discarded != 50 || xs.Written+ps.Written != 0 {
		t.Fatal(xs, ps)
	}
}

// An XLog enqueued with its encoded bytes is stored exactly as one the store
// encodes itself.
func TestEnqueueXLogEncodedMatches(t *testing.T) {
	tm := time.Date(2026, 10, 13, 7, 0, 0, 0, time.UTC).UnixMilli()
	dirs := [2]string{t.TempDir(), t.TempDir()}
	for i, dir := range dirs {
		s, err := Open(Config{Directory: dir, Location: time.UTC})
		if err != nil {
			t.Fatal(err)
		}
		for n := int64(0); n < 500; n++ {
			x, _ := packFixture(n, tm)
			if i == 0 {
				err = s.EnqueueXLogContext(context.Background(), x)
			} else {
				b, encErr := protocol.EncodePack(x, protocol.Limits{})
				if encErr != nil {
					t.Fatal(encErr)
				}
				err = s.EnqueueXLogEncodedContext(context.Background(), x, b)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	a, b := treeBytes(t, dirs[0]), treeBytes(t, dirs[1])
	dropRuntimeState(a)
	dropRuntimeState(b)
	if len(a) != len(b) {
		t.Fatal(len(a), len(b))
	}
	for name, want := range a {
		if !bytes.Equal(b[name], want) {
			t.Fatal(name, "differs")
		}
	}
}

// In a batch, an XLog over the 2-byte length prefix fails alone (as a
// single write did); the others of the hold are written.
func TestPackBatchOversizeFailsAlone(t *testing.T) {
	s, _ := liveStore(t, Config{})
	ctx := context.Background()
	tm := time.Date(2026, 10, 13, 7, 0, 0, 0, time.UTC).UnixMilli()
	s.mu.Lock() // queue all three before the writer takes one
	for n := int64(1); n <= 3; n++ {
		x, _ := packFixture(n, tm)
		if n == 2 {
			x.Text1 = strings.Repeat("x", 40000)
			x.TailVersion = 7
		}
		if err := s.EnqueueXLogContext(ctx, x); err != nil {
			s.mu.Unlock()
			t.Fatal(err)
		}
	}
	s.mu.Unlock()
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if st := s.XLogQueueStats(); st.Written != 2 || st.WriteErrors != 1 {
		t.Fatal(st)
	}
	for _, txid := range []int64{2, 4} {
		if got, err := s.XLog("20261013", txid); err != nil || got == nil {
			t.Fatal(txid, got, err)
		}
	}
}
