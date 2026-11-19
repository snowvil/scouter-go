package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

// gateFree reports whether another goroutine can take the Store gate now.
func gateFree(s *Store) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.mu.LockContext(ctx); err != nil {
		return false
	}
	s.mu.Unlock()
	return true
}

// dropZipCache forgets the decoded blocks, so the next read decodes again.
func dropZipCache(s *Store) {
	s.mu.Lock()
	if s.zip != nil {
		clear(s.zip.blocks)
	}
	s.mu.Unlock()
}

// zipReads are the reads of compressed records that can drop the gate.
func zipReads(ctx context.Context, date string, from, to int64) []struct {
	name string
	run  func(*Store) (any, error)
} {
	scan := func(q XLogScan) func(*Store) (any, error) {
		return func(s *Store) (any, error) {
			var out []XLogRecord
			err := s.ScanXLogsContext(ctx, q, func(r XLogRecord) (bool, error) {
				out = append(out, r)
				return true, nil
			})
			return out, err
		}
	}
	return []struct {
		name string
		run  func(*Store) (any, error)
	}{
		{"profile", func(s *Store) (any, error) { return s.ProfileChunksContext(ctx, date, 1) }},
		{"gxid XLogs", func(s *Store) (any, error) { return s.XLogsByGxIDContext(ctx, date, 7) }},
		{"txid XLog", func(s *Store) (any, error) { return s.XLogContext(ctx, date, 1003) }},
		{"V1 scan", scan(XLogScan{Date: date, From: from, To: to})},
		{"V1 reverse scan", scan(XLogScan{Date: date, From: from, To: to, Reverse: true})},
		{"V2 scan", scan(XLogScan{Date: date, From: from, To: to, Paged: true})},
	}
}

// A read that misses the block cache decodes the block without the Store
// gate: a writer can take the gate while the block is decoded. Results are
// the ones of the in-gate decode.
func TestCompressedBlockDecodeLeavesGateFree(t *testing.T) {
	ctx := context.Background()
	s := scanTestStore(t, Config{CompressXLog: true, CompressProfile: true})
	putCollectFixture(t, s, collectBase, 60, 700)
	for _, q := range zipReads(ctx, collectDate, collectBase, collectBase+60) {
		dropZipCache(s)
		want, werr := q.run(s)
		if werr != nil {
			t.Fatal(q.name, werr)
		}
		var decodes, held int
		zipDecodeHook = func() {
			decodes++
			if !gateFree(s) {
				held++
			}
		}
		dropZipCache(s)
		got, err := q.run(s)
		zipDecodeHook = nil
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("%s: result differs: %v", q.name, err)
		}
		if decodes == 0 || held != 0 {
			t.Errorf("%s: %d block decodes, %d of them under the gate", q.name, decodes, held)
		}
	}
}

// putLargeBlock fills block 0 of the date of scanBase with about 30 MiB of
// profiles (moderately compressible, so decoding is not a copy), then XLogs
// of gxid 7 and txids 1000.. at scanBase+i.
func putLargeBlock(t *testing.T, s *Store) {
	t.Helper()
	rng := rand.New(rand.NewSource(1))
	block := make([]byte, 256<<10)
	for i := 0; i < 118; i++ {
		for n := range block {
			block[n] = byte(rng.Intn(16))
		}
		if err := s.PutProfile(&protocol.ProfilePack{Time: scanBase + int64(i), TxID: 1 + int64(i%2), Profile: block}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 50; i++ {
		if err := s.PutXLog(&protocol.XLogPack{EndTime: scanBase + int64(i), TxID: int64(1000 + i), GxID: 7, ObjHash: int32(i)}); err != nil {
			t.Fatal(err)
		}
	}
}

// warmIngest creates the files of the date measureIngestDuringScan writes,
// so the measured PutXLog latency is the wait for the gate, not file setup.
func warmIngest(t *testing.T, s *Store) {
	t.Helper()
	if err := s.PutXLog(&protocol.XLogPack{EndTime: scanBase + 2*86400000, TxID: 1<<40 - 1, ObjHash: 9}); err != nil {
		t.Fatal(err)
	}
}

// timeBlockDecode is how long one decode of block 0 of date takes.
func timeBlockDecode(t *testing.T, s *Store, date string) time.Duration {
	t.Helper()
	f, err := os.Open(zipBlockPath(s.path(date, "xlog", "data"), 0))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	b, err := DecodeZipBlock(context.Background(), f, st.Size())
	if err != nil || len(b.Data) < 28<<20 {
		t.Fatal(len(b.Data), err)
	}
	return time.Since(start)
}

// Ingest latency while compressed reads decode a cold 30 MiB block: the
// decode runs outside the gate, so PutXLog waits for a hold, not a decode.
func TestCompressedBlockDecodeIngestWait(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a 30 MiB compressed block")
	}
	ctx := context.Background()
	s := scanTestStore(t, Config{CompressXLog: true, CompressProfile: true, MaxRecordBytes: 1 << 20})
	putLargeBlock(t, s)
	warmIngest(t, s)
	date := JavaDate(scanBase, time.UTC)
	decode := timeBlockDecode(t, s, date)
	t.Logf("one block decode: %v", decode)
	for _, q := range zipReads(ctx, date, scanBase, scanBase+50) {
		var decodes int
		var mu sync.Mutex
		zipDecodeHook = func() { mu.Lock(); decodes++; mu.Unlock() }
		took, worst := measureIngestDuringScan(t, s, func() error {
			for i := 0; i < 3; i++ {
				dropZipCache(s)
				if _, err := q.run(s); err != nil {
					return err
				}
			}
			return nil
		})
		zipDecodeHook = nil
		t.Logf("%s: 3 cold reads (%d block decodes) in %v, worst concurrent PutXLog %v", q.name, decodes, took, worst)
		if decodes < 3 {
			t.Errorf("%s: %d block decodes, want 3", q.name, decodes)
		}
		// Decoding under the gate made PutXLog wait about one decode.
		if worst > gateHoldLimit() || worst > decode*3/4 {
			t.Errorf("%s: PutXLog waited %v behind a block decode of %v (limit %v)", q.name, worst, decode, gateHoldLimit())
		}
	}
}

// Ingest latency while collect queries walk a long hash chain: with one hash
// bucket, every key of an index shares one chain. The single-hold oracle
// shows what a walk under the gate costs; the queries must not make a
// writer wait for it.
func TestLongKeyChainIngestWait(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 500000 records")
	}
	ctx := context.Background()
	for _, zip := range []bool{false, true} {
		s := scanTestStore(t, Config{HashSizeBytes: 5, CompressXLog: zip, CompressProfile: zip})
		for i := 0; i < 300000; i++ {
			gxid := int64(100 + i)
			if i%50000 == 0 {
				gxid = 7
			}
			if err := s.PutXLog(&protocol.XLogPack{EndTime: scanBase + int64(i%1000), TxID: int64(1000 + i), GxID: gxid}); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 100000; i++ {
			txid, gxid := int64(100+i), int64(100+i)
			if i%25000 == 0 {
				txid, gxid = 1, 7
			}
			if err := s.PutProfile(&protocol.ProfilePack{Time: scanBase, TxID: txid, Profile: []byte{byte(i)}}); err != nil {
				t.Fatal(err)
			}
			sp := container(t, collectSpan(t, gxid, int64(i)))
			sp.GxID, sp.Timestamp = gxid, scanBase
			if err := s.PutSpanContainer(sp); err != nil {
				t.Fatal(err)
			}
		}
		warmIngest(t, s)
		date := JavaDate(scanBase, time.UTC)
		for _, q := range []struct {
			name      string
			run, once func() (any, error)
		}{
			{"gxid XLogs", func() (any, error) { return s.XLogsByGxIDContext(ctx, date, 7) },
				func() (any, error) { return oneHoldXLogsByGxID(s, ctx, date, 7) }},
			{"profile", func() (any, error) { return s.ProfileChunksContext(ctx, date, 1) },
				func() (any, error) { return oneHoldProfileChunks(s, ctx, date, 1) }},
			{"spans", func() (any, error) { return s.SpansByGxIDContext(ctx, date, 7) },
				func() (any, error) { return oneHoldSpansByGxID(s, ctx, date, 7) }},
		} {
			start := time.Now()
			want, err := q.once()
			oneHold := time.Since(start)
			if err != nil || reflect.ValueOf(want).Len() < 4 {
				t.Fatal(q.name, err)
			}
			var got any
			took, worst := measureIngestDuringScan(t, s, func() error {
				var err error
				got, err = q.run()
				return err
			})
			t.Logf("zip=%v %s: %d results in %v (single hold %v), worst concurrent PutXLog %v", zip, q.name, reflect.ValueOf(got).Len(), took, oneHold, worst)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("zip=%v %s: result differs", zip, q.name)
			}
			// Walking under the gate made PutXLog wait about one walk.
			if worst > gateHoldLimit() || worst > oneHold/2 {
				t.Errorf("zip=%v %s: PutXLog waited %v behind the chain walk (single hold %v, limit %v)", zip, q.name, worst, oneHold, gateHoldLimit())
			}
		}
	}
}

// Collect queries over one-bucket hash indexes (every key of an index in one
// chain, walked from the KeyChain snapshot) return the single-hold results.
func TestCollectQueriesMatchOneHoldSharedChain(t *testing.T) {
	for _, zip := range []bool{false, true} {
		for _, records := range []int{0, 40, 1} {
			s := scanTestStore(t, Config{HashSizeBytes: 5, CompressXLog: zip, CompressProfile: zip, MaxQueryRecords: records})
			putCollectFixture(t, s, collectBase, 300, 700)
			compareCollect(t, fmt.Sprintf("shared chain zip=%v records=%d", zip, records), s, collectDate)
		}
	}
}

// zipReadResults runs every zipReads query on a warm cache: the in-gate
// results (no block is decoded).
func zipReadResults(t *testing.T, s *Store, date string, from, to int64) []any {
	t.Helper()
	var out []any
	for _, q := range zipReads(context.Background(), date, from, to) {
		got, err := q.run(s)
		if err != nil {
			t.Fatal(q.name, err)
		}
		out = append(out, got)
	}
	return out
}

// noDecodeSlotsHeld checks that every decode slot was released.
func noDecodeSlotsHeld(t *testing.T, s *Store, name string) {
	t.Helper()
	if n := len(s.zipDecodes); n != 0 {
		t.Fatalf("%s: %d decode slot(s) still held", name, n)
	}
}

// A block decoded outside the gate that is no longer the file's leading
// bytes (here: every decode is followed by a block truncation count) is
// decoded again, at most zipFetchAttempts times; then the read decodes under
// the gate as before. The results are the in-gate ones.
func TestCompressedReadFallsBackToInGateDecode(t *testing.T) {
	s := scanTestStore(t, Config{CompressXLog: true, CompressProfile: true})
	putCollectFixture(t, s, collectBase, 60, 700)
	want := zipReadResults(t, s, collectDate, collectBase, collectBase+60)
	for n, q := range zipReads(context.Background(), collectDate, collectBase, collectBase+60) {
		var outside, inside int
		zipDecodeHook = func() {
			if !gateFree(s) {
				inside++
				return
			}
			outside++
			s.mu.Lock()
			s.zip.truncs++ // as a rollover or failed write would
			s.mu.Unlock()
		}
		dropZipCache(s)
		got, err := q.run(s)
		zipDecodeHook = nil
		if err != nil || !reflect.DeepEqual(got, want[n]) {
			t.Errorf("%s: result differs: %v", q.name, err)
		}
		if outside != zipFetchAttempts || inside != 1 {
			t.Errorf("%s: %d decodes outside the gate, %d inside; want %d and 1", q.name, outside, inside, zipFetchAttempts)
		}
		noDecodeSlotsHeld(t, s, q.name)
	}
}

// Appends to the block while it is decoded outside the gate: the decoded
// bytes are still the file's leading bytes; the appended members are joined
// and the block is cached as the current file.
func TestCompressedReadWhileBlockGrows(t *testing.T) {
	ctx := context.Background()
	s := scanTestStore(t, Config{CompressXLog: true, CompressProfile: true})
	putCollectFixture(t, s, collectBase, 60, 700)
	want := zipReadResults(t, s, collectDate, collectBase, collectBase+60)
	tx := int64(1 << 30)
	for n, q := range zipReads(ctx, collectDate, collectBase, collectBase+60) {
		zipDecodeHook = func() {
			if !gateFree(s) {
				return
			}
			tx++ // same date and block, outside the queried keys and range
			if err := s.PutXLog(&protocol.XLogPack{EndTime: collectBase + 3600000, TxID: tx, GxID: 99}); err != nil {
				t.Error(err)
			}
		}
		dropZipCache(s)
		got, err := q.run(s)
		zipDecodeHook = nil
		if err != nil || !reflect.DeepEqual(got, want[n]) {
			t.Errorf("%s: result differs: %v", q.name, err)
		}
		noDecodeSlotsHeld(t, s, q.name)
		s.mu.Lock()
		path := zipBlockPath(s.path(collectDate, "xlog", "data"), 0)
		if c := s.zip.blocks[path]; c == nil {
			t.Errorf("%s: the grown block was not cached", q.name)
		} else if st, err := os.Stat(path); err != nil || !sameZipFile(c.info, st) || !bytes.Equal(c.data, decodeZipFile(t, path).Data) {
			t.Errorf("%s: cached block does not match its file", q.name)
		}
		s.mu.Unlock()
	}

	// A record appended after the miss lies past the decoded bytes: the
	// appended members are decoded under the gate and joined, without a
	// second block decode.
	dropZipCache(s)
	s.mu.Lock()
	pos := s.zip.writers[collectDate].count
	s.mu.Unlock()
	decodes := 0
	zipDecodeHook = func() {
		if decodes++; decodes == 1 {
			s.mu.Lock()
			_, err := s.appendZip(collectDate, []byte("late"))
			s.mu.Unlock()
			if err != nil {
				t.Error(err)
			}
		}
	}
	defer func() { zipDecodeHook = nil }()
	var fetch zipFetch
	for {
		s.mu.Lock()
		b, err := s.readZipFetch(ctx, collectDate, pos, &fetch)
		s.mu.Unlock()
		var miss *zipMiss
		if errors.As(err, &miss) {
			if err := s.fetchZip(ctx, &fetch, miss); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err != nil || string(b) != "late" || decodes != 1 {
			t.Fatalf("late record: %q %v after %d decodes", b, err, decodes)
		}
		break
	}
	noDecodeSlotsHeld(t, s, "late record")
}

// A purge, DB delete or Close while a block is decoded outside the gate: the
// reads end as they do when it happens between holds (collect queries:
// nothing; scans and the txid lookup: the index is gone; Close: ErrClosed),
// never with records of other files, and the decode slot is released.
func TestCompressedReadDuringPurgeAndClose(t *testing.T) {
	ctx := context.Background()
	old := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	date := JavaDate(old, time.UTC)
	for _, step := range []string{"purge", "delete", "close"} {
		for n, q := range zipReads(ctx, date, old, old+60) {
			s, c, _ := retentionFixture(t)
			s.SetCompression(true, true)
			putCollectFixture(t, s, old, 60, 700)
			dropZipCache(s)
			fired := false
			zipDecodeHook = func() {
				if fired || !gateFree(s) {
					return
				}
				fired = true
				switch step {
				case "purge":
					if _, err := (&Retention{Store: s, Counters: c}).purge(ctx, date, "20261013", PurgeAll); err != nil {
						t.Error(err)
					}
				case "delete":
					if k, err := (&DatabaseDeleter{Store: s, Counters: c}).Delete(ctx, []string{"/" + date}); k != 1 || err != nil {
						t.Error(k, err)
					}
				case "close":
					if err := s.Close(); err != nil {
						t.Error(err)
					}
				}
			}
			got, err := q.run(s)
			zipDecodeHook = nil
			name := fmt.Sprintf("%s during %s", step, q.name)
			if !fired {
				t.Fatalf("%s: no decode outside the gate", name)
			}
			if step == "close" {
				if !errors.Is(err, ErrClosed) {
					t.Errorf("%s: %v, want ErrClosed", name, err)
				}
			} else if v := reflect.ValueOf(got); err != nil || (v.Kind() == reflect.Slice && v.Len() != 0) || (v.Kind() == reflect.Pointer && !v.IsNil()) {
				t.Errorf("%s: %v %v, want nothing", name, got, err)
			}
			noDecodeSlotsHeld(t, s, name)
			_ = n
		}
	}
}

// At most zipDecodeConcurrent blocks are decoded outside the gate at once.
func TestCompressedDecodesAreBounded(t *testing.T) {
	s := scanTestStore(t, Config{CompressXLog: true, CompressProfile: true})
	putCollectFixture(t, s, collectBase, 60, 700)
	dropZipCache(s)
	var mu sync.Mutex
	running, most := 0, 0
	zipDecodeHook = func() {
		mu.Lock()
		running++
		most = max(most, running)
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		running--
		mu.Unlock()
	}
	defer func() { zipDecodeHook = nil }()
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.ProfileChunksContext(context.Background(), collectDate, 1); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if most > zipDecodeConcurrent || most == 0 {
		t.Fatalf("%d decodes at once, bound %d", most, zipDecodeConcurrent)
	}
	noDecodeSlotsHeld(t, s, "bounded")
}

// Compressed reads that decode outside the gate, writers appending to the
// same block, cache evictions and purges of another date (run under -race).
func TestCompressedReadsConcurrentWithWriters(t *testing.T) {
	ctx := context.Background()
	s, c, _ := retentionFixture(t)
	s.SetCompression(true, true)
	old := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	putCollectFixture(t, s, collectBase, 60, 700)
	putCollectFixture(t, s, old, 20, 100)
	want := zipReadResults(t, s, collectDate, collectBase, collectBase+60)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for n, q := range zipReads(ctx, collectDate, collectBase, collectBase+60) {
					got, err := q.run(s)
					if err != nil || !reflect.DeepEqual(got, want[n]) {
						errs <- fmt.Errorf("%s: result differs: %v", q.name, err)
						return
					}
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := s.PutXLog(&protocol.XLogPack{EndTime: collectBase + 3600000, TxID: int64(1<<30 + i), GxID: 99}); err != nil {
				errs <- err
				return
			}
			if i%20 == 0 {
				dropZipCache(s)
				s.SetCompressReadCacheBlocks(1 + i%3)
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := &Retention{Store: s, Counters: c}
		for i := 0; i < 5; i++ {
			if _, err := r.purge(ctx, JavaDate(old, time.UTC), "20261013", PurgeAll); err != nil {
				errs <- err
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	noDecodeSlotsHeld(t, s, "concurrent")
}

// A block appended to all the time (today's) that is not cached is
// decoded once, not per record: the grown block is joined and cached, and
// the writer keeps the cached block current.
func TestHotBlockDecodedOncePerScan(t *testing.T) {
	ctx := context.Background()
	s := scanTestStore(t, Config{CompressXLog: true})
	putScanXLogs(t, s, 2000, func(i int) int64 { return scanBase + int64(i) }, func(int) int32 { return 1 })
	date := JavaDate(scanBase, time.UTC)
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			// Same date and block, after the scanned range.
			if err := s.PutXLog(&protocol.XLogPack{EndTime: scanBase + 3600000, TxID: int64(1<<30 + i)}); err != nil {
				done <- err
				return
			}
		}
	}()
	for _, q := range []XLogScan{{Date: date, From: scanBase, To: scanBase + 1999}, {Date: date, From: scanBase, To: scanBase + 1999, Paged: true}} {
		var mu sync.Mutex
		decodes := 0
		zipDecodeHook = func() {
			mu.Lock()
			decodes++
			mu.Unlock()
			if gateFree(s) { // the block grows while it is decoded
				if err := s.PutXLog(&protocol.XLogPack{EndTime: scanBase + 3600000, TxID: int64(1 << 29)}); err != nil {
					t.Error(err)
				}
			}
		}
		dropZipCache(s)
		ids, _ := scanTxIDs(t, s, q)
		zipDecodeHook = nil
		if len(ids) != 2000 {
			t.Fatalf("paged=%v: %d records", q.Paged, len(ids))
		}
		if decodes > zipFetchAttempts {
			t.Errorf("paged=%v: %d block decodes for 2000 records of one block", q.Paged, decodes)
		}
		t.Logf("paged=%v: %d block decodes", q.Paged, decodes)
	}
	close(stop)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_ = ctx
	noDecodeSlotsHeld(t, s, "hot block")
}

// A read whose block another reader cached meanwhile releases its own
// decoded block and slot at once, not at its next miss or the query's end.
func TestCacheHitReleasesFetchedBlock(t *testing.T) {
	ctx := context.Background()
	s := scanTestStore(t, Config{CompressXLog: true})
	putScanXLogs(t, s, 10, func(i int) int64 { return scanBase + int64(i) }, func(int) int32 { return 1 })
	date := JavaDate(scanBase, time.UTC)
	dropZipCache(s)
	var fetches [2]zipFetch
	var misses [2]*zipMiss
	for n := range fetches {
		s.mu.Lock()
		_, err := s.readZipFetch(ctx, date, 0, &fetches[n])
		s.mu.Unlock()
		if !errors.As(err, &misses[n]) {
			t.Fatalf("read %d: %v, want a miss", n, err)
		}
	}
	for n := range fetches {
		if err := s.fetchZip(ctx, &fetches[n], misses[n]); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.zipDecodes) != 2 {
		t.Fatalf("%d slots held by two decoded blocks", len(s.zipDecodes))
	}
	for n := range fetches {
		s.mu.Lock()
		_, err := s.readZipFetch(ctx, date, 0, &fetches[n])
		s.mu.Unlock()
		if err != nil {
			t.Fatal(n, err)
		}
		if fetches[n].block != nil || len(s.zipDecodes) != 1-n {
			t.Fatalf("read %d: block kept %v, %d slots held", n, fetches[n].block != nil, len(s.zipDecodes))
		}
	}
}

// A query waiting for a decode slot ends with its ctx and holds no slot or
// block file afterwards (the file is opened only once a slot is held).
func TestDecodeSlotWaitCancelled(t *testing.T) {
	s := scanTestStore(t, Config{CompressXLog: true, CompressProfile: true})
	putCollectFixture(t, s, collectBase, 60, 700)
	fds := func() int {
		entries, err := os.ReadDir("/dev/fd")
		if err != nil {
			t.Skip("no /dev/fd")
		}
		return len(entries)
	}
	for n := range zipReads(context.Background(), collectDate, collectBase, collectBase+60) {
		dropZipCache(s)
		for i := 0; i < zipDecodeConcurrent; i++ {
			s.zipDecodes <- struct{}{} // every slot taken
		}
		before := fds()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		q := zipReads(ctx, collectDate, collectBase, collectBase+60)[n]
		_, err := q.run(s)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%s: %v, want the ctx error", q.name, err)
		}
		if after := fds(); after > before {
			t.Errorf("%s: %d descriptors left open", q.name, after-before)
		}
		for i := 0; i < zipDecodeConcurrent; i++ {
			<-s.zipDecodes
		}
		noDecodeSlotsHeld(t, s, q.name)
	}
}

// TagActualContext reads compressed XLogs like the scans: a cold block is
// decoded outside the gate and the results are the single-hold ones.
func TestTagActualDecodesOutsideGate(t *testing.T) {
	ctx := context.Background()
	s := scanTestStore(t, Config{CompressXLog: true})
	tm := time.Date(2026, 10, 13, 7, 0, 0, 0, time.UTC).UnixMilli()
	if err := s.PutObject("20261013", sampleObject("/synthetic")); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 1500; i++ {
		x := &protocol.XLogPack{EndTime: tm + (i*37)%60000, ObjHash: 42, TxID: i + 1, Service: int32(7 + i%3)}
		if err := s.PutXLog(x); err != nil {
			t.Fatal(err)
		}
	}
	svc := protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "service", Value: protocol.Value{Type: protocol.ValueList, Values: []protocol.Value{textHash(7), textHash(8)}}}}}
	for _, q := range []TagActualQuery{
		{Group: "service", From: tm, To: tm + 60000, Max: 100000, Filter: svc},
		{Group: "service", From: tm, To: tm + 60000, Max: 100000, Reverse: true, Filter: svc},
		{Group: "service", From: tm + 777, To: tm + 41234, Max: 300, TxID: 401, Filter: svc},
	} {
		q.Date, q.ObjType = "20261013", "java"
		want, werr := tagActualOneHold(s, q)
		if werr != nil || len(want) == 0 {
			t.Fatal(len(want), werr)
		}
		var decodes, held int
		zipDecodeHook = func() {
			decodes++
			if !gateFree(s) {
				held++
			}
		}
		dropZipCache(s)
		got, err := s.TagActualContext(ctx, q)
		zipDecodeHook = nil
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("%+v: %d records %v, want %d", q, len(got), err, len(want))
		}
		if decodes == 0 || held != 0 {
			t.Errorf("%+v: %d decodes, %d under the gate", q, decodes, held)
		}
		noDecodeSlotsHeld(t, s, "tag actual")
	}
}

// A miss inside a V1 bucket larger than scanSortInline (ordered between
// holds), forward and reverse: the record is read again at its place.
func TestV1LargeBucketMiss(t *testing.T) {
	s := scanTestStore(t, Config{CompressXLog: true})
	putScanXLogs(t, s, 1500, func(i int) int64 { return scanBase + 100 + int64(i%7) }, func(int) int32 { return 1 })
	date := JavaDate(scanBase, time.UTC)
	for _, reverse := range []bool{false, true} {
		q := XLogScan{Date: date, From: scanBase, To: scanBase + 499, Reverse: reverse}
		want, _ := scanTxIDs(t, s, q)
		var decodes, held int
		zipDecodeHook = func() {
			decodes++
			if !gateFree(s) {
				held++
			}
		}
		dropZipCache(s)
		got, _ := scanTxIDs(t, s, q)
		zipDecodeHook = nil
		if len(want) != 1500 || !reflect.DeepEqual(got, want) {
			t.Errorf("reverse=%v: %d records, want %d in the same order", reverse, len(got), len(want))
		}
		if decodes == 0 || held != 0 {
			t.Errorf("reverse=%v: %d decodes, %d under the gate", reverse, decodes, held)
		}
	}
}

// V2 resumes at the missed record itself: records appended to the bucket
// head during the decode are not seen (the scan had reached the head), and a
// stateful Match (the V2 resume txid) runs exactly once per record.
func TestV2HeadMissWithAppends(t *testing.T) {
	s := scanTestStore(t, Config{CompressXLog: true})
	putScanXLogs(t, s, 600, func(i int) int64 { return scanBase + 100 + int64(i%7) }, func(int) int32 { return 1 })
	date := JavaDate(scanBase, time.UTC)
	query := func(calls map[int64]int) XLogScan {
		started := false
		return XLogScan{Date: date, From: scanBase, To: scanBase + 999, Paged: true, Match: func(p *protocol.XLogPack) bool {
			calls[p.TxID]++
			if !started {
				started = p.TxID == 450
				return false
			}
			return true
		}}
	}
	want, _ := scanTxIDs(t, s, query(map[int64]int{}))
	tx := int64(1 << 30)
	zipDecodeHook = func() {
		if !gateFree(s) {
			return
		}
		for i := 0; i < 20; i++ { // the same bucket, inside the range
			tx++
			if err := s.PutXLog(&protocol.XLogPack{EndTime: scanBase + 101, TxID: tx, ObjHash: 1}); err != nil {
				t.Error(err)
			}
		}
	}
	defer func() { zipDecodeHook = nil }()
	dropZipCache(s)
	calls := map[int64]int{}
	got, _ := scanTxIDs(t, s, query(calls))
	if len(want) == 0 || !reflect.DeepEqual(got, want) {
		t.Fatalf("%d records, want the %d before the appends", len(got), len(want))
	}
	for id, n := range calls {
		if n != 1 || id > 600 {
			t.Fatalf("Match ran %d times for txid %d", n, id)
		}
	}
	if len(calls) != 600 {
		t.Fatalf("Match ran for %d records, want 600", len(calls))
	}
}

// A block with a torn tail is decoded outside the gate while the writer
// reloads, truncates the tail (a block truncation) and appends: the decoded
// block is not used, and the read ends with the in-gate result.
func TestMissWithTornTailAndWriterTruncate(t *testing.T) {
	ctx := context.Background()
	s := scanTestStore(t, Config{CompressXLog: true, CompressProfile: true})
	putCollectFixture(t, s, collectBase, 60, 700)
	want := zipReadResults(t, s, collectDate, collectBase, collectBase+60)
	path := zipBlockPath(s.path(collectDate, "xlog", "data"), 0)
	tx := int64(1 << 30)
	for n, q := range zipReads(ctx, collectDate, collectBase, collectBase+60) {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(gzipMagic); err != nil { // a torn member header
			t.Fatal(err)
		}
		f.Close()
		s.mu.Lock()
		truncs := s.zip.truncs
		s.mu.Unlock()
		outside := 0
		zipDecodeHook = func() {
			if !gateFree(s) {
				return
			}
			if outside++; outside == 1 {
				// The writer reloads (as after an eviction), truncates
				// the torn tail and appends.
				s.mu.Lock()
				delete(s.zip.writers, collectDate)
				s.mu.Unlock()
				tx++
				if err := s.PutXLog(&protocol.XLogPack{EndTime: collectBase + 3600000, TxID: tx, GxID: 99}); err != nil {
					t.Error(err)
				}
			}
		}
		dropZipCache(s)
		got, err := q.run(s)
		zipDecodeHook = nil
		if err != nil || !reflect.DeepEqual(got, want[n]) {
			t.Errorf("%s: result differs: %v", q.name, err)
		}
		s.mu.Lock()
		truncated := s.zip.truncs > truncs
		s.mu.Unlock()
		if outside == 0 || !truncated {
			t.Errorf("%s: %d decodes outside the gate, truncated %v", q.name, outside, truncated)
		}
		noDecodeSlotsHeld(t, s, q.name)
	}
}

// The indexes walked from KeyChain snapshots (collect queries, txid
// lookup) refuse in-place rewrites; others are not marked.
func TestKeyChainIndexesAreAppendOnly(t *testing.T) {
	s := scanTestStore(t, Config{})
	putCollectFixture(t, s, collectBase, 5, 100)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, path := range []string{
		s.path(collectDate, "xlog", "xlog") + "_profile",
		s.path(collectDate, "xlog", "xlog_gid"),
		s.path(collectDate, "span", "span") + "_gid",
		s.path(collectDate, "xlog", "xlog_tid"),
	} {
		idx, err := s.index(path, false, false)
		if err != nil || idx == nil {
			t.Fatal(path, err)
		}
		if !idx.AppendOnly() {
			t.Errorf("%s: not append-only", path)
		}
		if _, err := idx.Update(longKey(7), offsetBytes(1)); !errors.Is(err, legacyindex.ErrAppendOnly) {
			t.Errorf("%s: Update %v", path, err)
		}
		if _, err := idx.Delete(longKey(7)); !errors.Is(err, legacyindex.ErrAppendOnly) {
			t.Errorf("%s: Delete %v", path, err)
		}
	}
	idx, err := s.index(s.path(collectDate, "xlog", "xlog_tim"), true, false)
	if err != nil || idx == nil || idx.AppendOnly() {
		t.Fatal("xlog_tim is not walked from snapshots", err)
	}
}

// A scan that waits for a decode slot while the block it missed is appended
// to all the time: the block is decoded up to the file size once a slot is
// held, so the members joined under the gate are only the ones
// written during the decode and the wait for the gate, and a longer tail is
// not decoded under the gate at all. Every gate hold stays short.
func TestHotBlockTailUnderGateBounded(t *testing.T) {
	s := scanTestStore(t, Config{CompressXLog: true})
	putScanXLogs(t, s, 2000, func(i int) int64 { return scanBase + int64(i) }, func(int) int32 { return 1 })
	date := JavaDate(scanBase, time.UTC)
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			if err := s.PutXLog(&protocol.XLogPack{EndTime: scanBase + 3600000, TxID: int64(1<<30 + i), Service: int32(i)}); err != nil {
				done <- err
				return
			}
		}
	}()
	var mu sync.Mutex
	var longest time.Duration
	var tails []int64
	var grown int64
	scanHoldObserver = func(d time.Duration) { mu.Lock(); longest = max(longest, d); mu.Unlock() }
	zipTailHook = func(n int64) { mu.Lock(); tails = append(tails, n); mu.Unlock() }
	defer func() { scanHoldObserver, zipTailHook = nil, nil }()
	var statErr error
	size := func() int64 {
		st, err := os.Stat(zipBlockPath(s.path(date, "xlog", "data"), 0))
		if err != nil {
			mu.Lock()
			statErr = err
			mu.Unlock()
			return 0
		}
		return st.Size()
	}
	// The slot holders must end before the writer stops (they wait for it
	// to append) and before the test returns (the Store closes then).
	var holders sync.WaitGroup
	n := 0
	dropZipCache(s)
	err := s.ScanXLogsContext(context.Background(), XLogScan{Date: date, From: scanBase, To: scanBase + 1999}, func(r XLogRecord) (bool, error) {
		if n++; n%300 == 1 {
			// Evicted, and the decode slots are busy for a while: the next
			// miss waits for a slot while the writer keeps appending.
			dropZipCache(s)
			for i := 0; i < zipDecodeConcurrent; i++ {
				s.zipDecodes <- struct{}{}
			}
			holders.Add(1)
			go func() {
				defer holders.Done()
				// Hold the slots until more than zipTailMaxBytes were
				// appended (bounded wait; -race writes slower).
				before := size()
				for limit := time.Now().Add(10 * time.Second); size()-before <= zipTailMaxBytes && time.Now().Before(limit); {
					time.Sleep(10 * time.Millisecond)
				}
				mu.Lock()
				grown = max(grown, size()-before)
				mu.Unlock()
				for i := 0; i < zipDecodeConcurrent; i++ {
					<-s.zipDecodes
				}
			}()
		}
		return true, nil
	})
	holders.Wait()
	close(stop)
	if werr := <-done; werr != nil {
		t.Fatal(werr)
	}
	if err != nil || n != 2000 {
		t.Fatal(n, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if statErr != nil {
		t.Fatal(statErr)
	}
	t.Logf("longest scan gate hold %v; block grew %d bytes during a slot wait; tails decoded under the gate %v", longest, grown, tails)
	if grown <= zipTailMaxBytes {
		t.Fatalf("the block grew only %d bytes during a slot wait", grown)
	}
	for _, n := range tails {
		if n > zipTailMaxBytes {
			t.Errorf("a %d-byte tail was decoded under the gate", n)
		}
	}
	// The tail bound above is the property; the hold time also measures
	// scheduling, so it is only a coarse check (wider under -race).
	holdLimit := 50 * time.Millisecond
	if raceEnabled {
		holdLimit = 250 * time.Millisecond
	}
	if longest > holdLimit {
		t.Errorf("a scan gate hold took %v", longest)
	}
}

// Members appended during the decode beyond zipTailMaxBytes are not decoded
// under the gate: the block is decoded again outside it.
func TestLongTailDecodedOutsideGate(t *testing.T) {
	s := scanTestStore(t, Config{CompressXLog: true})
	putScanXLogs(t, s, 100, func(i int) int64 { return scanBase + int64(i) }, func(int) int32 { return 1 })
	date := JavaDate(scanBase, time.UTC)
	want, _ := scanTxIDs(t, s, XLogScan{Date: date, From: scanBase, To: scanBase + 99})
	decodes := 0
	zipDecodeHook = func() {
		if decodes++; decodes > 1 || !gateFree(s) {
			return
		}
		for i := 0; i < 5000; i++ { // ~450KiB of members
			if err := s.PutXLog(&protocol.XLogPack{EndTime: scanBase + 3600000, TxID: int64(1<<30 + i), Service: int32(i)}); err != nil {
				t.Error(err)
			}
		}
	}
	defer func() { zipDecodeHook = nil }()
	dropZipCache(s)
	got, _ := scanTxIDs(t, s, XLogScan{Date: date, From: scanBase, To: scanBase + 99})
	if !reflect.DeepEqual(got, want) || decodes != 2 {
		t.Fatalf("%d records after %d decodes, want %d after 2", len(got), decodes, len(want))
	}
}

// The members appended during the decode end with a torn member (joined as
// a whole-file decode would: without it) or with garbage (the block is not
// used; the read fails as an in-gate decode does).
func TestGrownTailTornOrDamaged(t *testing.T) {
	for _, damaged := range []bool{false, true} {
		s := scanTestStore(t, Config{CompressXLog: true})
		putScanXLogs(t, s, 100, func(i int) int64 { return scanBase + int64(i) }, func(int) int32 { return 1 })
		date := JavaDate(scanBase, time.UTC)
		q := XLogScan{Date: date, From: scanBase, To: scanBase + 99}
		want, _ := scanTxIDs(t, s, q)
		path := zipBlockPath(s.path(date, "xlog", "data"), 0)
		decodes := 0
		zipDecodeHook = func() {
			if decodes++; decodes > 1 || !gateFree(s) {
				return
			}
			if err := s.PutXLog(&protocol.XLogPack{EndTime: scanBase + 3600000, TxID: 1 << 30}); err != nil {
				t.Error(err)
			}
			tail := gzipMagic // torn
			if damaged {
				tail = []byte("not a gzip member")
			}
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err == nil {
				_, err = f.Write(tail)
				f.Close()
			}
			if err != nil {
				t.Error(err)
			}
		}
		dropZipCache(s)
		var got []int64
		err := s.ScanXLogsContext(context.Background(), q, func(r XLogRecord) (bool, error) {
			got = append(got, r.Pack.TxID)
			return true, nil
		})
		zipDecodeHook = nil
		if damaged {
			if !errors.Is(err, ErrCorrupt) {
				t.Errorf("damaged tail: %v, want ErrCorrupt", err)
			}
		} else {
			if err != nil || !reflect.DeepEqual(got, want) || decodes != 1 {
				t.Errorf("torn tail: %d records %v after %d decodes", len(got), err, decodes)
			}
			s.mu.Lock()
			c := s.zip.blocks[path]
			s.mu.Unlock()
			if c == nil || !bytes.Equal(c.data, decodeZipFile(t, path).Data) {
				t.Errorf("torn tail: cached block is not the whole-file decode")
			}
		}
		noDecodeSlotsHeld(t, s, "grown tail")
	}
}

// The writer rolls over to the next block while a full block is decoded:
// the decoded block is unchanged and used; the records of both blocks read.
func TestRolloverDuringDecode(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a 30 MiB compressed block")
	}
	ctx := context.Background()
	s := scanTestStore(t, Config{CompressXLog: true, CompressProfile: true, MaxRecordBytes: 1 << 20})
	putLargeBlock(t, s)
	date := JavaDate(scanBase, time.UTC)
	want := zipReadResults(t, s, date, scanBase, scanBase+50)
	big := make([]byte, 1<<20)
	for n, q := range zipReads(ctx, date, scanBase, scanBase+50) {
		var positions []int64
		fired := false
		zipDecodeHook = func() {
			if fired || !gateFree(s) {
				return
			}
			fired = true
			for i := 0; i < 4; i++ {
				big[0] = byte(i)
				s.mu.Lock()
				pos, err := s.appendZip(date, big)
				s.mu.Unlock()
				if err != nil {
					t.Error(err)
				}
				positions = append(positions, pos)
			}
		}
		dropZipCache(s)
		got, err := q.run(s)
		zipDecodeHook = nil
		if err != nil || !reflect.DeepEqual(got, want[n]) {
			t.Errorf("%s: result differs: %v", q.name, err)
		}
		if !fired || positions[len(positions)-1]/zipBlockBytes == 0 {
			t.Fatalf("%s: no rollover during the decode", q.name)
		}
		for i, pos := range positions {
			if b, err := s.readZip(date, pos); err != nil || len(b) != len(big) || b[0] != byte(i) {
				t.Errorf("%s: record at %d: %v", q.name, pos, err)
			}
		}
		noDecodeSlotsHeld(t, s, q.name)
	}
}

// TagActualContext with a TxID cursor whose own record misses: the cursor
// record is read again and starts the result once.
func TestTagActualCursorRecordMiss(t *testing.T) {
	ctx := context.Background()
	s := scanTestStore(t, Config{CompressXLog: true})
	tm := time.Date(2026, 10, 13, 7, 0, 0, 0, time.UTC).UnixMilli()
	if err := s.PutObject("20261013", sampleObject("/synthetic")); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 400; i++ {
		if err := s.PutXLog(&protocol.XLogPack{EndTime: tm + i*10, ObjHash: 42, TxID: i + 1, Service: 7}); err != nil {
			t.Fatal(err)
		}
	}
	svc := protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "service", Value: protocol.Value{Type: protocol.ValueList, Values: []protocol.Value{textHash(7)}}}}}
	// The first record of the range is the cursor record (txid 101).
	q := TagActualQuery{Date: "20261013", ObjType: "java", Group: "service", From: tm + 1000, To: tm + 3990, Max: 100000, TxID: 101, Filter: svc}
	want, err := tagActualOneHold(s, q)
	if err != nil || len(want) != 299 {
		t.Fatal(len(want), err)
	}
	decodes := 0
	zipDecodeHook = func() { decodes++ }
	defer func() { zipDecodeHook = nil }()
	dropZipCache(s)
	got, err := s.TagActualContext(ctx, q)
	if err != nil || !reflect.DeepEqual(got, want) || decodes != 1 {
		t.Fatalf("%d records %v after %d decodes, want %d", len(got), err, decodes, len(want))
	}
}

// A scan that took a decoded block and then reads a date that is no longer
// compressed releases the block and its slot at once.
func TestModeChangeReleasesFetchedBlock(t *testing.T) {
	ctx := context.Background()
	s := scanTestStore(t, Config{CompressXLog: true})
	putScanXLogs(t, s, 10, func(i int) int64 { return scanBase + int64(i) }, func(int) int32 { return 1 })
	date := JavaDate(scanBase, time.UTC)
	dropZipCache(s)
	var fetch zipFetch
	value := offsetBytes(0)
	s.mu.Lock()
	_, _, err := s.decodeXLogFetch(ctx, date, value, &fetch)
	s.mu.Unlock()
	var miss *zipMiss
	if !errors.As(err, &miss) {
		t.Fatal(err)
	}
	if err := s.fetchZip(ctx, &fetch, miss); err != nil || fetch.block == nil {
		t.Fatal(err)
	}
	// The date's mode reads as uncompressed now (as after a mode file change).
	s.mu.Lock()
	s.modes[s.path(date, "xlog", "xlog")+".service.conf"] = false
	_, _, _ = s.decodeXLogFetch(ctx, date, value, &fetch)
	s.mu.Unlock()
	if fetch.block != nil {
		t.Fatal("block kept")
	}
	noDecodeSlotsHeld(t, s, "mode change")
}
