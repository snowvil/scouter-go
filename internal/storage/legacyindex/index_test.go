package legacyindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func openTest(t *testing.T, size int, timed bool, loc *time.Location) (*Index, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "index")
	index, err := Open(path, size, timed, loc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	return index, path
}
func timeKey(ms int64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, uint64(ms))
	return out
}
func mustPut(t *testing.T, idx *Index, key, value []byte) int64 {
	t.Helper()
	pos, err := idx.Put(key, value)
	if err != nil {
		t.Fatal(err)
	}
	return pos
}
func TestHashIndex(t *testing.T) {
	idx, path := openTest(t, 6, false, nil) // capacity1 with Java's unused trailing padding
	key := []byte{0, 0xff, 2}
	p1 := mustPut(t, idx, key, []byte{0x80})
	p2 := mustPut(t, idx, []byte("collision"), []byte{2})
	p3 := mustPut(t, idx, key, []byte{3})
	if p1 != 2 || p2 != 15 || p3 != 34 {
		t.Fatalf("positions %d %d %d", p1, p2, p3)
	}
	value, err := idx.Get(key)
	if err != nil || !bytes.Equal(value, []byte{3}) {
		t.Fatalf("get %x %v", value, err)
	}
	value[0] = 99
	all, err := idx.GetAll(key)
	if err != nil || len(all) != 2 || all[0][0] != 3 || all[1][0] != 0x80 {
		t.Fatalf("getAll %x %v", all, err)
	}
	records, err := idx.Records()
	if err != nil || len(records) != 3 || records[2].PreviousPosition != p2 {
		t.Fatalf("records %+v %v", records, err)
	}
	count, _ := idx.Count()
	if count != 1 {
		t.Fatalf("occupied buckets %d", count)
	}
	ok, err := idx.Update(key, []byte{4})
	if !ok || err != nil {
		t.Fatalf("update %v %v", ok, err)
	}
	ok, err = idx.Update([]byte("collision"), []byte{5})
	if !ok || err != nil {
		t.Fatalf("collision update %v %v", ok, err)
	}
	ok, err = idx.Update(key, []byte{1, 2})
	if ok || err != nil {
		t.Fatalf("resize update %v %v", ok, err)
	}
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}
	idx, err = Open(path, 100, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	value, err = idx.Get([]byte("collision"))
	if err != nil || !bytes.Equal(value, []byte{5}) {
		t.Fatalf("reopen %x %v", value, err)
	}
	h, err := os.ReadFile(path + ".hfile")
	if err != nil {
		t.Fatal(err)
	}
	if len(h) != 1030 || !bytes.Equal(h[:2], []byte{0xca, 0xfe}) || binary.BigEndian.Uint32(h[4:8]) != 1 {
		t.Fatalf("header %x", h[:8])
	}
	missing, err := idx.Get([]byte("absent"))
	if missing != nil || err != nil {
		t.Fatalf("missing %x %v", missing, err)
	}
}
func TestTimeIndex(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	idx, path := openTest(t, 0, true, loc)
	base := time.Date(2000, 1, 1, 0, 0, 0, 0, loc).UnixMilli()
	start := base + 183*86400000 + 125
	mustPut(t, idx, timeKey(start), []byte{0x7f})
	mustPut(t, idx, timeKey(start), []byte{0x80})
	mustPut(t, idx, timeKey(start), []byte{0x80})
	mustPut(t, idx, timeKey(start+400), []byte{0})
	count, _ := idx.Count()
	if count != 6 {
		t.Fatalf("time count records4+occupied2=%d", count)
	}
	records, err := idx.Range(start, start+400, false)
	if err != nil || len(records) != 3 || records[0].Value[0] != 0x80 || records[1].Value[0] != 0x7f {
		t.Fatalf("range %v %v", records, err)
	}
	var values []byte
	err = idx.ScanBucket(start, false, func(r Record) error { values = append(values, r.Value[0]); return nil })
	if err != nil || !bytes.Equal(values, []byte{0x80, 0x80, 0x7f}) {
		t.Fatalf("v2 %x %v", values, err)
	}
	records, err = idx.Range(start, start+400, true)
	if err != nil || len(records) != 3 || records[0].Value[0] != 0 {
		t.Fatalf("reverse %v %v", records, err)
	}
	if err = idx.Sync(); err != nil {
		t.Fatal(err)
	}
	h, _ := os.ReadFile(path + ".hfile")
	if !bytes.Equal(h[1024:1029], []byte{0, 0, 0, 0, 38}) {
		t.Fatalf("DST fixed base bucket %x", h[1024:1029])
	}
	if _, err = idx.Put(timeKey(0), []byte{}); err == nil {
		t.Fatal("nonpositive time accepted")
	}
	if _, err = idx.Range(start, start+86400001, false); err == nil {
		t.Fatal("unbounded range accepted")
	}
}
func TestBoundsOwnershipAndClose(t *testing.T) {
	idx, _ := openTest(t, 5, false, nil)
	for _, pair := range [][2][]byte{{nil, []byte{}}, {[]byte{}, nil}, {make([]byte, 65536), []byte{}}, {[]byte{}, make([]byte, MaxValueBytes+1)}} {
		if _, err := idx.Put(pair[0], pair[1]); err == nil {
			t.Fatal("invalid key/value accepted")
		}
	}
	key, value := []byte{1}, []byte{2}
	mustPut(t, idx, key, value)
	key[0] = 9
	value[0] = 9
	got, err := idx.Get([]byte{1})
	if err != nil || got[0] != 2 {
		t.Fatalf("ownership %x %v", got, err)
	}
	mustPut(t, idx, []byte{}, []byte{})
	got, err = idx.Get([]byte{})
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty versus absent %v %v", got, err)
	}
	sentinel := errors.New("stop")
	visited := 0
	err = idx.Scan(func(Record) error { visited++; return sentinel })
	if !errors.Is(err, sentinel) || visited != 1 {
		t.Fatalf("callback stop %d %v", visited, err)
	}
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = idx.Get([]byte{1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed get %v", err)
	}
	if err = idx.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed sync %v", err)
	}
}
func TestBlobWidthsAndLongKeys(t *testing.T) {
	idx, path := openTest(t, 15, false, nil)
	for _, n := range []int{0, 253, 254, 65535, 65536} {
		key := []byte{byte(n >> 8), byte(n)}
		value := bytes.Repeat([]byte{0x81}, n)
		mustPut(t, idx, key, value)
		got, err := idx.Get(key)
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("length%d %v", n, err)
		}
	}
	long := bytes.Repeat([]byte{7}, 65535)
	mustPut(t, idx, long, []byte{9})
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	idx, err := Open(path, 0, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	got, err := idx.Get(long)
	if err != nil || !bytes.Equal(got, []byte{9}) {
		t.Fatalf("unsigned short key %x %v", got, err)
	}
}
func TestCorruption(t *testing.T) {
	cases := map[string]func([]byte, []byte) ([]byte, []byte){
		"keymagic": func(k, h []byte) ([]byte, []byte) { k[0] = 0; return k, h }, "hashmagic": func(k, h []byte) ([]byte, []byte) { h[0] = 0; return k, h },
		"truncated": func(k, h []byte) ([]byte, []byte) { return k[:len(k)-1], h }, "cycle": func(k, h []byte) ([]byte, []byte) { k[7] = 2; return k, h },
		"negativeprev": func(k, h []byte) ([]byte, []byte) { k[3] = 0x80; return k, h }, "badhead": func(k, h []byte) ([]byte, []byte) { h[1028] = 3; return k, h },
		"negativecount": func(k, h []byte) ([]byte, []byte) { h[4] = 0x80; return k, h }, "partialhash": func(k, h []byte) ([]byte, []byte) { return k, h[:1028] },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			idx, path := openTest(t, 5, false, nil)
			mustPut(t, idx, []byte{1}, []byte{2})
			if err := idx.Close(); err != nil {
				t.Fatal(err)
			}
			k, _ := os.ReadFile(path + ".kfile")
			h, _ := os.ReadFile(path + ".hfile")
			k, h = mutate(k, h)
			_ = os.WriteFile(path+".kfile", k, 0600)
			_ = os.WriteFile(path+".hfile", h, 0600)
			bad, err := Open(path, 5, false, nil)
			if err == nil {
				bad.Close()
				t.Fatal("corruption accepted")
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("wrong error %v", err)
			}
		})
	}
}
func TestConcurrentReadWriteClose(t *testing.T) {
	idx, _ := openTest(t, 100, false, nil)
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, err := idx.Put([]byte{byte(n)}, []byte{byte(j)})
				if err != nil && !errors.Is(err, ErrClosed) {
					t.Error(err)
				}
				_, err = idx.Get([]byte{byte(n)})
				if err != nil && !errors.Is(err, ErrClosed) {
					t.Error(err)
				}
			}
		}(n)
	}
	wg.Add(1)
	go func() { defer wg.Done(); _ = idx.Close() }()
	wg.Wait()
}

func TestResultLimitAndStreaming(t *testing.T) {
	idx, _ := openTest(t, 5, false, nil)
	for n := 0; n <= MaxResultRecords; n++ {
		mustPut(t, idx, []byte{1}, []byte{})
	}
	if _, err := idx.GetAll([]byte{1}); !errors.Is(err, ErrLimit) {
		t.Fatalf("unbounded GetAll: %v", err)
	}
	if _, err := idx.Records(); !errors.Is(err, ErrLimit) {
		t.Fatalf("unbounded Records: %v", err)
	}
	stop := errors.New("bounded caller stop")
	count := 0
	err := idx.ScanKey([]byte{1}, func(r Record) error {
		count++
		if count == 10 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || count != 10 {
		t.Fatalf("streaming stop %d %v", count, err)
	}
}

func FuzzIndexFiles(f *testing.F) {
	f.Add([]byte{0xca, 0xfe}, []byte{0, 0, 0, 0, 0})
	f.Add([]byte{0xca, 0xfe, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2}, []byte{0, 0, 0, 0, 2})
	f.Fuzz(func(t *testing.T, k, bucket []byte) {
		if len(k) > 4096 || len(bucket) != 5 {
			return
		}
		path := filepath.Join(t.TempDir(), "fuzz")
		h := make([]byte, 1029)
		h[0], h[1] = 0xca, 0xfe
		copy(h[1024:], bucket)
		if err := os.WriteFile(path+".kfile", k, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path+".hfile", h, 0600); err != nil {
			t.Fatal(err)
		}
		planned, planErr := AnalyzeRecovery(path, false, nil)
		idx, err := Open(path, 5, false, nil)
		if (err == nil) != (planErr == nil) {
			t.Fatalf("analyze %v disagrees with writable open %v", planErr, err)
		}
		if err != nil {
			return
		}
		if got := idx.Recovered(); got.RelinkedRecords != planned.RelinkedRecords || got.TruncatedTailBytes != planned.TruncatedTailBytes {
			t.Fatalf("analyze %+v, applied %+v", planned, got)
		}
		_, _ = idx.Get([]byte{1})
		_, _ = idx.GetAll([]byte{1})
		records, _ := idx.Records()
		// A KeyChain snapshot walks what ScanKey walks, errors included.
		keyChainMatches(t, idx, []byte{1})
		for n, r := range records {
			if n < 8 {
				keyChainMatches(t, idx, r.Key)
			}
		}
		if err = idx.Close(); err != nil {
			return
		}
		// Recovery is persisted by Close and is idempotent.
		again, err := Open(path, 5, false, nil)
		if err != nil {
			t.Fatalf("reopen after recovery: %v", err)
		}
		if again.Recovered().Recovered() {
			t.Fatalf("second open recovered again: %+v", again.Recovered())
		}
		again.Close()
	})
}

func TestRangeVisitorOwnsBytes(t *testing.T) {
	idx, _ := openTest(t, 0, true, time.UTC)
	ms := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	for _, value := range []byte{1, 1, 2} {
		mustPut(t, idx, timeKey(ms), []byte{value})
	}
	visited := 0
	err := idx.ScanRange(ms, ms, false, func(r Record) error { visited++; r.Key[0] = 0; r.Value[0] = 99; return nil })
	if err != nil || visited != 2 {
		t.Fatalf("owned callback bytes altered duplicate semantics: visits=%d err=%v", visited, err)
	}
}

// cancelAfterChecks makes cancellation deterministic inside a collision chain.
type cancelAfterChecks struct {
	context.Context
	checks int
}

func (c *cancelAfterChecks) Err() error {
	c.checks++
	if c.checks >= 4 {
		return context.Canceled
	}
	return nil
}
func TestScanKeyContextCancelsNonmatchingCollisionChain(t *testing.T) {
	idx, _ := openTest(t, 5, false, nil)
	for n := byte(0); n < 10; n++ {
		mustPut(t, idx, []byte{n}, []byte{n})
	}
	ctx := &cancelAfterChecks{Context: context.Background()}
	calls := 0
	err := idx.ScanKeyContext(ctx, []byte{99}, func(Record) error { calls++; return nil })
	if !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("err=%v callbacks=%d", err, calls)
	}
}
func TestScanKeyContextCancelsLockWait(t *testing.T) {
	idx, _ := openTest(t, 5, false, nil)
	idx.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- idx.ScanKeyContext(ctx, []byte{1}, func(Record) error { return nil }) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		idx.mu.Unlock()
		t.Fatal("blocked on index lock")
	}
	idx.mu.Unlock()
}

func TestBucketEndpointsIncludeDeletedPhysicalRecords(t *testing.T) {
	idx, _ := openTest(t, 0, true, time.UTC)
	tm := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	first := mustPut(t, idx, timeKey(tm), []byte{1})
	last := mustPut(t, idx, timeKey(tm+1), []byte{2})
	a, b, err := idx.BucketEndpointsContext(context.Background(), tm)
	if err != nil || a.Position != first || b.Position != last {
		t.Fatal(a, b, err)
	}
	if _, err = idx.keys.WriteAt([]byte{1}, first); err != nil {
		t.Fatal(err)
	}
	a, b, err = idx.BucketEndpointsContext(context.Background(), tm)
	if err != nil || !a.Deleted || b.Position != last {
		t.Fatal(a, b, err)
	}
	a.Value[0] = 9
	c, _, err := idx.BucketEndpointsContext(context.Background(), tm)
	if err != nil || c.Value[0] != 1 {
		t.Fatal("borrowed endpoint", c, err)
	}
}

// meta reads a record head in one ReadAt when it fits metaReadAhead and
// falls back to metaFields otherwise; both give the same metadata and value
// for every record, including keys and payloads at the read-ahead edge, long
// blob prefixes, deleted records and a torn tail.
func TestMetaReadAheadMatchesFields(t *testing.T) {
	idx, path := openTest(t, 1024, false, time.UTC)
	var positions []int64
	for _, size := range [][2]int{{8, 5}, {8, 0}, {40, 10}, {50, 5}, {51, 4}, {52, 4}, {60, 0}, {8, 253}, {8, 300}, {200, 70000}} {
		key := bytes.Repeat([]byte{byte(size[0])}, size[0])
		positions = append(positions, mustPut(t, idx, key, bytes.Repeat([]byte{7}, size[1])))
	}
	if _, err := idx.Delete(bytes.Repeat([]byte{8}, 8)); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		idx.mu.RLock()
		defer idx.mu.RUnlock()
		for _, pos := range append(positions, idx.length-12, 1, idx.length) {
			fast, ferr := idx.meta(pos)
			slow, serr := idx.metaFields(pos)
			if (ferr == nil) != (serr == nil) || (ferr != nil && ferr.Error() != serr.Error()) {
				t.Fatalf("pos %d: errors %v / %v", pos, ferr, serr)
			}
			if ferr != nil {
				continue
			}
			fr, err1 := idx.record(fast)
			sr, err2 := idx.record(slow)
			if err1 != nil || err2 != nil || fr.Position != sr.Position || fr.PreviousPosition != sr.PreviousPosition || fr.NextPosition != sr.NextPosition ||
				fr.Deleted != sr.Deleted || fr.Tombstone != sr.Tombstone || !bytes.Equal(fr.Key, sr.Key) || !bytes.Equal(fr.Value, sr.Value) || fast.valueAt != slow.valueAt || fast.valueBytes != slow.valueBytes {
				t.Fatalf("pos %d: %+v / %+v (%v %v)", pos, fr, sr, err1, err2)
			}
		}
	}
	check()
	// A torn record at the tail: its head fits the read-ahead, its payload is
	// cut off.
	f, err := os.OpenFile(path+".kfile", os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	torn := []byte{0, 0, 0, 0, 0, 0, 0, 8, 1, 2, 3, 4, 5, 6, 7, 8, 5, 1}
	if _, err = f.Write(torn); err != nil {
		t.Fatal(err)
	}
	f.Close()
	idx.mu.Lock()
	tornAt := idx.length
	idx.length += int64(len(torn))
	idx.mu.Unlock()
	positions = append(positions, tornAt)
	check()
}

// PutBatch leaves the files, header and positions consecutive Puts leave,
// for hash and time indexes, with repeated keys and shared buckets.
func TestPutBatchMatchesPuts(t *testing.T) {
	for _, timed := range []bool{false, true} {
		a, pa := openTest(t, 64, timed, time.UTC)
		b, pb := openTest(t, 64, timed, time.UTC)
		var entries []Entry
		for n := 0; n < 300; n++ {
			key := []byte{byte(n % 7), byte(n % 13)}
			if timed {
				key = timeKey(1700000000000 + int64(n%40)*250)
			}
			entries = append(entries, Entry{Key: key, Value: bytes.Repeat([]byte{byte(n)}, n%9)})
		}
		var want []int64
		for _, e := range entries {
			want = append(want, mustPut(t, a, e.Key, e.Value))
		}
		got, err := b.PutBatch(entries[:150])
		if err != nil {
			t.Fatal(err)
		}
		more, err := b.PutBatch(entries[150:])
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, more...)
		for n := range want {
			if got[n] != want[n] {
				t.Fatalf("timed=%v position %d: %d want %d", timed, n, got[n], want[n])
			}
		}
		if err := errorsJoin(a.Close(), b.Close()); err != nil {
			t.Fatal(err)
		}
		for _, ext := range []string{".kfile", ".hfile"} {
			x, _ := os.ReadFile(pa + ext)
			y, _ := os.ReadFile(pb + ext)
			if !bytes.Equal(x, y) {
				t.Fatalf("timed=%v %s differs", timed, ext)
			}
		}
	}
}

func errorsJoin(errs ...error) error { return errors.Join(errs...) }
