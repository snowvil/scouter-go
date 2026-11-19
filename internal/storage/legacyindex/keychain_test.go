package legacyindex

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// scanKeyRecords is ScanKeyContext (or a KeyChain snapshot) collected.
func scanKeyRecords(scan func(context.Context, func(Record) error) error) ([]Record, error) {
	var out []Record
	err := scan(context.Background(), func(r Record) error {
		out = append(out, r)
		return nil
	})
	return out, err
}

// keyChainMatches checks that a KeyChain taken now walks what
// ScanKeyContext walks now: records, order, error.
func keyChainMatches(t *testing.T, idx *Index, key []byte) {
	t.Helper()
	want, werr := scanKeyRecords(func(ctx context.Context, visit func(Record) error) error {
		return idx.ScanKeyContext(ctx, key, visit)
	})
	c, err := idx.KeyChain(key)
	if err != nil {
		if errString(err) != errString(werr) {
			t.Fatalf("key %x: KeyChain %v, ScanKey %v", key, err, werr)
		}
		return
	}
	defer c.Close()
	got, gerr := scanKeyRecords(c.ScanContext)
	if errString(gerr) != errString(werr) || !reflect.DeepEqual(got, want) {
		t.Fatalf("key %x: chain %d records %v, ScanKey %d records %v", key, len(got), gerr, len(want), werr)
	}
	if c.Empty() != (c.head == 0) || (c.Empty() && len(got) != 0) {
		t.Fatalf("key %x: Empty %v with %d records", key, c.Empty(), len(got))
	}
	// And its lookup is LookupContext's (FuzzIndexFiles: damaged files).
	var calls []deepCall
	prev := idx.deepSearch
	idx.SetDeepSearch(func(op DeepSearchOp, k []byte, looped int) {
		calls = append(calls, deepCall{op, string(k), looped})
	})
	defer idx.SetDeepSearch(prev)
	keyChainLookupMatches(t, idx, &calls, key)
}

// A KeyChain walks exactly ScanKeyContext: shared buckets, other keys in the
// chain, deleted records, large values (read field by field), long keys.
func TestKeyChainMatchesScanKey(t *testing.T) {
	for _, size := range []int{5, 50, 5000} {
		idx, _ := openTest(t, size, false, nil)
		for i := 0; i < 600; i++ {
			key := []byte(fmt.Sprint("k", i%37))
			value := []byte{byte(i), byte(i >> 8)}
			switch i % 50 {
			case 7:
				value = bytes.Repeat([]byte{byte(i)}, 300) // 3-byte blob prefix
			case 9:
				value = bytes.Repeat([]byte{byte(i)}, 70000) // 5-byte blob prefix
			case 11:
				key = bytes.Repeat([]byte{byte(i)}, 4000)
			}
			mustPut(t, idx, key, value)
		}
		if _, err := idx.Delete([]byte("k3")); err != nil {
			t.Fatal(err)
		}
		for k := 0; k < 40; k++ {
			keyChainMatches(t, idx, []byte(fmt.Sprint("k", k)))
		}
		keyChainMatches(t, idx, bytes.Repeat([]byte{11}, 4000))
		keyChainMatches(t, idx, nil) // invalid key: the same error
	}
	timed, _ := openTest(t, 0, true, time.UTC)
	if _, err := timed.KeyChain(timeKey(1)); err == nil {
		t.Fatal("time index key chain")
	}
}

// The snapshot is the chain when KeyChain returned: later appends are not
// seen, and closing the index or renaming and removing its files does not
// change the walk.
func TestKeyChainIsASnapshot(t *testing.T) {
	idx, path := openTest(t, 5, false, nil)
	for i := 0; i < 300; i++ {
		mustPut(t, idx, []byte{byte(i % 3)}, []byte{byte(i)})
	}
	want, err := idx.GetAll([]byte{1})
	if err != nil || len(want) != 100 {
		t.Fatal(len(want), err)
	}
	c, err := idx.KeyChain([]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < 50; i++ {
		mustPut(t, idx, []byte{1}, []byte{0xee})
	}
	if _, err := idx.PutBatch([]Entry{{Key: []byte{1}, Value: []byte{0xef}}}); err != nil {
		t.Fatal(err)
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(filepath.Dir(path), "moved")
	if err := os.Rename(path+".kfile", moved+".kfile"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(moved + ".kfile"); err != nil {
		t.Fatal(err)
	}
	var got [][]byte
	err = c.ScanContext(context.Background(), func(r Record) error {
		got = append(got, r.Value)
		return nil
	})
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot walk: %d values %v, want %d", len(got), err, len(want))
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.ScanContext(context.Background(), func(Record) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatal("scan after Close", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal("second Close", err)
	}
}

// An empty chain opens nothing; a closed index refuses; cancellation and
// visitor errors end the walk.
func TestKeyChainEdges(t *testing.T) {
	idx, _ := openTest(t, 5000, false, nil)
	c, err := idx.KeyChain([]byte("absent"))
	if err != nil || !c.Empty() || c.view != nil {
		t.Fatal(err)
	}
	if err := c.ScanContext(context.Background(), nil); err == nil {
		t.Fatal("nil visitor")
	}
	for i := 0; i < 20; i++ {
		mustPut(t, idx, []byte("k"), []byte{byte(i)})
	}
	c, err = idx.KeyChain([]byte("k"))
	if err != nil || c.Empty() {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	n := 0
	err = c.ScanContext(ctx, func(Record) error {
		if n++; n == 5 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) || n != 5 {
		t.Fatal(n, err)
	}
	stop := errors.New("stop")
	if err := c.ScanContext(context.Background(), func(Record) error { return stop }); err != stop {
		t.Fatal(err)
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.KeyChain([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

// Walks of snapshots race with appends, syncs and Close (-race).
func TestKeyChainConcurrentWithWriter(t *testing.T) {
	idx, _ := openTest(t, 5, false, nil)
	for i := 0; i < 200; i++ {
		mustPut(t, idx, []byte{byte(i % 4)}, []byte{byte(i)})
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 3000; i++ {
			if _, err := idx.Put([]byte{byte(i % 4)}, []byte{byte(i)}); err != nil {
				done <- err
				return
			}
			if i%100 == 0 {
				if err := idx.Sync(); err != nil {
					done <- err
					return
				}
			}
		}
		done <- idx.Close()
	}()
	values := func(scan func(context.Context, func(Record) error) error) ([][]byte, error) {
		var out [][]byte
		err := scan(context.Background(), func(r Record) error {
			out = append(out, r.Value)
			return nil
		})
		return out, err
	}
	for n := 0; ; n++ {
		key := []byte{byte(n % 4)}
		want, err := values(func(ctx context.Context, visit func(Record) error) error { return idx.ScanKeyContext(ctx, key, visit) })
		if errors.Is(err, ErrClosed) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		c, err := idx.KeyChain(key)
		if errors.Is(err, ErrClosed) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got, err := values(c.ScanContext)
		c.Close()
		// The snapshot was taken after ScanKey: it ends with what ScanKey saw.
		if err != nil || len(got) < len(want) || !reflect.DeepEqual(got[len(got)-len(want):], want) {
			t.Fatalf("walk %d: %d values %v, ScanKey %d", n, len(got), err, len(want))
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// A key file replaced at the index path (not the index's own any more) is
// refused instead of walked.
func TestKeyChainRefusesReplacedKeyFile(t *testing.T) {
	idx, path := openTest(t, 5, false, nil)
	mustPut(t, idx, []byte("k"), []byte{1})
	b, err := os.ReadFile(path + ".kfile")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".kfile", path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".kfile", b, 0600); err != nil {
		t.Fatal(err)
	}
	if c, err := idx.KeyChain([]byte("k")); err == nil {
		c.Close()
		t.Fatal("walked a replaced key file")
	}
}

// SetAppendOnly refuses Update and Delete; Put and KeyChain still work.
func TestAppendOnly(t *testing.T) {
	idx, _ := openTest(t, 50, false, nil)
	mustPut(t, idx, []byte("k"), []byte{1})
	idx.SetAppendOnly()
	if _, err := idx.Update([]byte("k"), []byte{2}); !errors.Is(err, ErrAppendOnly) {
		t.Fatal(err)
	}
	if _, err := idx.Delete([]byte("k")); !errors.Is(err, ErrAppendOnly) {
		t.Fatal(err)
	}
	mustPut(t, idx, []byte("k"), []byte{3})
	keyChainMatches(t, idx, []byte("k"))
}

// keyChainLookupMatches checks that a KeyChain taken now looks up what
// LookupContext looks up now: value, error and deep search report.
func keyChainLookupMatches(t *testing.T, idx *Index, calls *[]deepCall, key []byte) error {
	t.Helper()
	ctx := context.Background()
	*calls = nil
	want, werr := idx.LookupContext(ctx, key)
	wantCalls := *calls
	*calls = nil
	// LookupWithin ends as LookupContext or (a chain longer than the limit)
	// with nothing; a larger limit never ends with nothing again.
	ended := false
	for _, limit := range []int{-1, 0, 1, 2, 3, 8, 1 << 30} {
		got, done, gerr := idx.LookupWithin(ctx, key, limit, time.Time{})
		if !done {
			if ended || got != nil || gerr != nil || *calls != nil || limit == 1<<30 {
				t.Fatalf("key %x: limit %d: not done after done %v, %x %v %v", key, limit, ended, got, gerr, *calls)
			}
			continue
		}
		ended = true
		if !bytes.Equal(got, want) || (got == nil) != (want == nil) || errString(gerr) != errString(werr) || !reflect.DeepEqual(*calls, wantCalls) {
			t.Fatalf("key %x: limit %d: %x %v %v, LookupContext %x %v %v", key, limit, got, gerr, *calls, want, werr, wantCalls)
		}
		*calls = nil
	}
	c, err := idx.KeyChain(key)
	if err != nil {
		if errString(err) != errString(werr) || wantCalls != nil {
			t.Fatalf("key %x: KeyChain %v, LookupContext %v %v", key, err, werr, wantCalls)
		}
		return werr
	}
	defer c.Close()
	got, gerr := c.LookupContext(ctx)
	if !bytes.Equal(got, want) || (got == nil) != (want == nil) || errString(gerr) != errString(werr) || !reflect.DeepEqual(*calls, wantCalls) {
		t.Fatalf("key %x: chain %x %v %v, LookupContext %x %v %v", key, got, gerr, *calls, want, werr, wantCalls)
	}
	return werr
}

// KeyChain.LookupContext is LookupContext on the snapshot: the newest live
// value, misses reported with the walked length (deleted records counted,
// an empty chain as 0), the same errors on damaged chains.
func TestKeyChainLookupMatchesLookupContext(t *testing.T) {
	for _, size := range []int{5, 50, 5000} {
		idx, path := openTest(t, size, false, nil)
		var calls []deepCall
		idx.SetDeepSearch(func(op DeepSearchOp, key []byte, looped int) {
			calls = append(calls, deepCall{op, string(key), looped})
		})
		keyChainLookupMatches(t, idx, &calls, []byte("k1")) // empty index
		var positions []int64
		for i := 0; i < 600; i++ {
			key := []byte(fmt.Sprint("k", i%37))
			value := []byte{byte(i), byte(i >> 8)}
			switch i % 50 {
			case 7:
				value = bytes.Repeat([]byte{byte(i)}, 300)
			case 9:
				value = bytes.Repeat([]byte{byte(i)}, 70000)
			case 11:
				key = bytes.Repeat([]byte{byte(i)}, 4000)
			}
			positions = append(positions, mustPut(t, idx, key, value))
		}
		if _, err := idx.Delete([]byte("k3")); err != nil {
			t.Fatal(err)
		}
		keys := [][]byte{nil, bytes.Repeat([]byte{11}, 4000)}
		for k := 0; k < 40; k++ {
			keys = append(keys, []byte(fmt.Sprint("k", k)))
		}
		for _, key := range keys {
			keyChainLookupMatches(t, idx, &calls, key)
		}
		// Damaged record headers (flag, link, key): both walk the same bytes.
		f, err := os.OpenFile(path+".kfile", os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(int64(size)))
		failed := 0
		for n := 0; n < 40; n++ {
			at := positions[rng.Intn(len(positions))] + rng.Int63n(9)
			if _, err := f.WriteAt([]byte{byte(rng.Intn(256))}, at); err != nil {
				t.Fatal(err)
			}
			for _, key := range keys[1:] {
				if keyChainLookupMatches(t, idx, &calls, key) != nil {
					failed++
				}
			}
		}
		f.Close()
		t.Logf("size %d: %d failed lookups on the damaged file", size, failed)
		if failed == 0 {
			t.Fatal("damage made no lookup fail")
		}
	}
}

// A KeyChain lookup is cancelled per chain record and refused after Close;
// a nil DeepSearchFunc reports nothing.
func TestKeyChainLookupEdges(t *testing.T) {
	idx, _ := openTest(t, 5, false, nil)
	for i := 0; i < 20; i++ {
		mustPut(t, idx, []byte{byte(i)}, []byte{byte(i)})
	}
	c, err := idx.KeyChain([]byte{0})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.LookupContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if v, err := c.LookupContext(context.Background()); err != nil || !bytes.Equal(v, []byte{0}) {
		t.Fatal(v, err)
	}
	c.Close()
	if _, err := c.LookupContext(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	c, err = idx.KeyChain([]byte{99})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if v, err := c.LookupContext(context.Background()); err != nil || v != nil {
		t.Fatal(v, err)
	}
}

// LookupWithin at its limits: a chain of exactly limit records ends (hit at
// its last record or miss), one more record does not; a passed deadline
// ends a walk of more than 16 records with nothing (checked every 16), a
// walk of 16 still ends; no deadline never stops.
func TestLookupWithinLimits(t *testing.T) {
	ctx := context.Background()
	past := time.Unix(1, 0)
	for _, n := range []int{16, 17, 40} {
		idx, _ := openTest(t, 5, false, nil)
		var calls []deepCall
		idx.SetDeepSearch(func(op DeepSearchOp, key []byte, looped int) {
			calls = append(calls, deepCall{op, string(key), looped})
		})
		for i := 0; i < n; i++ {
			mustPut(t, idx, []byte(fmt.Sprint("k", i)), []byte{byte(i)})
		}
		for _, c := range []struct {
			key      string
			limit    int
			deadline time.Time
			done     bool
		}{
			{"k0", n, time.Time{}, true},
			{"k0", n - 1, time.Time{}, false},
			{"absent", n, time.Time{}, true},
			{"absent", n - 1, time.Time{}, false},
			{"k0", 1 << 30, past, n <= 16},
			{"absent", 1 << 30, past, n <= 16},
			{"absent", 1 << 30, time.Now().Add(time.Hour), true},
			{fmt.Sprint("k", n-1), 1, past, true}, // the head: found first
		} {
			calls = nil
			v, done, err := idx.LookupWithin(ctx, []byte(c.key), c.limit, c.deadline)
			if err != nil || done != c.done {
				t.Fatalf("n=%d %+v: done %v %v", n, c, done, err)
			}
			switch {
			case !done:
				if v != nil || calls != nil {
					t.Fatalf("n=%d %+v: not done with %x %v", n, c, v, calls)
				}
			case c.key == "absent":
				if v != nil || len(calls) != 1 || calls[0] != (deepCall{DeepGet, "absent", n}) {
					t.Fatalf("n=%d %+v: miss %x %v", n, c, v, calls)
				}
			default:
				if v == nil || calls != nil {
					t.Fatalf("n=%d %+v: hit %x %v", n, c, v, calls)
				}
			}
		}
	}
}
