package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// oneHoldXLog is the former single-hold XLogContext (the index lookup and
// the record read in one Store gate hold, a compressed block decoded under
// the gate), kept verbatim as the oracle of the snapshot lookup.
func oneHoldXLog(s *Store, ctx context.Context, date string, txid int64) (*protocol.XLogPack, error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	if err := s.check(date); err != nil {
		return nil, err
	}
	idx, err := s.index(s.path(date, "xlog", "xlog_tid"), false, false)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if err != nil || idx == nil {
		return nil, err
	}
	value, err := queryGet(ctx, idx, longKey(txid))
	if err != nil || value == nil {
		return nil, err
	}
	p, _, err := s.decodeXLogContext(ctx, date, value)
	if err == nil && p.TxID != txid {
		return nil, ErrCorrupt
	}
	return p, err
}

// txidFixtureKeys are the txids compareTxid looks up in a putTxidFixture
// date: records (1005 twice: the newest wins), absent keys, and index
// entries of no XLog of theirs (5555: another txid's record, 6666: a bad
// offset value, 7777: an offset past the data).
var txidFixtureKeys = []int64{1000, 1005, 1150, 1299, 1<<40 + 3, 5555, 6666, 7777, 1, 0, 999999, 1<<40 + 4}

func putTxidFixture(t *testing.T, s *Store, base int64) {
	t.Helper()
	putCollectKinds(t, s, base, 300, 700, "x")
	for _, p := range []*protocol.XLogPack{{EndTime: base + 7, TxID: 1005, Elapsed: 99999}, {EndTime: base + 8, TxID: 1<<40 + 3}} {
		if err := s.PutXLog(p); err != nil {
			t.Fatal(err)
		}
	}
	date := JavaDate(base, time.UTC)
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, err := s.index(s.path(date, "xlog", "xlog_tid"), false, false)
	if err != nil || idx == nil {
		t.Fatal(err)
	}
	other, err := idx.Get(longKey(1000))
	if err != nil || other == nil {
		t.Fatal(err)
	}
	for key, value := range map[int64][]byte{5555: other, 6666: {1, 2}, 7777: offsetBytes(1 << 38)} {
		if _, err := idx.Put(longKey(key), value); err != nil {
			t.Fatal(err)
		}
	}
}

// compareTxid looks up every txid with XLogContext and the oracle: the same
// record, error and S152 lines (each call in a fresh 10-second window).
func compareTxid(t *testing.T, name string, s *Store, l *logged, date string, txids []int64) {
	t.Helper()
	ctx := context.Background()
	clock := time.Unix(1000, 0)
	s.live.warn.now = func() time.Time { return clock }
	for _, txid := range txids {
		clock = clock.Add(11 * time.Second)
		l.take()
		got, gerr := s.XLogContext(ctx, date, txid)
		gotLog := l.take()
		clock = clock.Add(11 * time.Second)
		want, werr := oneHoldXLog(s, ctx, date, txid)
		wantLog := l.take()
		if errText(gerr) != errText(werr) || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(gotLog, wantLog) {
			t.Errorf("%s: txid %d: got %v %v %q, want %v %v %q", name, txid, got, gerr, gotLog, want, werr, wantLog)
		}
	}
}

// XLogContext (index hold, chain walk without the gate) returns what the
// single-hold lookup returned: the newest record, nothing for an absent key
// or date, the same first error for damaged index values or data, and the
// same S152 deep search warnings (Java IndexKeyFile.get: a miss whose chain,
// deleted records counted, is longer than log_index_traversal_warning_count).
func TestXLogLookupMatchesOneHold(t *testing.T) {
	for _, zip := range []bool{false, true} {
		for _, hash := range []int{0, 5} { // 5: one bucket, every txid in one chain
			for _, warn := range []int{-1, 0, 100, 1000} {
				var l logged
				name := fmt.Sprintf("zip=%v hash=%d warn=%d", zip, hash, warn)
				s := scanTestStore(t, Config{CompressXLog: zip, HashSizeBytes: hash, Log: l.log})
				s.SetIndexTraversalWarningCount(warn)
				putTxidFixture(t, s, collectBase)
				dropZipCache(s)
				compareTxid(t, name, s, &l, collectDate, txidFixtureKeys)
				if hash == 5 && warn == 0 {
					// The fixture covers what it names.
					s.live.warn.now = func() time.Time { return time.Now().Add(time.Hour) }
					ctx := context.Background()
					if p, err := s.XLogContext(ctx, collectDate, 1005); err != nil || p.Elapsed != 99999 {
						t.Fatalf("%s: newest 1005: %v %v", name, p, err)
					}
					if _, err := s.XLogContext(ctx, collectDate, 5555); !errors.Is(err, ErrCorrupt) {
						t.Fatalf("%s: 5555: %v", name, err)
					}
					if p, err := s.XLogContext(ctx, collectDate, 1); err != nil || p != nil {
						t.Fatalf("%s: absent: %v %v", name, p, err)
					}
					if got := l.take(); len(got) != 1 || got[0] != "S152|[warn] Too many index deep searching. 0" {
						t.Fatalf("%s: S152 %q", name, got)
					}
				}
				compareTxid(t, name+" no index", s, &l, "20261001", []int64{1000, 1})
				compareTxid(t, name+" bad date", s, &l, "2026101x", []int64{1000})
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err := s.XLogContext(ctx, collectDate, 1000); !errors.Is(err, context.Canceled) {
					t.Errorf("%s: cancelled %v", name, err)
				}
			}
		}
	}
	// The XLog compression mode file is unreadable (over 64 KiB): reported
	// by the record read, after the lookup (a miss reports nothing).
	for _, zip := range []bool{false, true} {
		dir := t.TempDir()
		c := Config{Directory: dir, Location: time.UTC, CompressXLog: zip}
		s, err := Open(c)
		if err != nil {
			t.Fatal(err)
		}
		putTxidFixture(t, s, collectBase)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		conf := filepath.Join(dir, collectDate, "xlog", "xlog.service.conf")
		if err := os.WriteFile(conf, bytes.Repeat([]byte("#"), 70<<10), 0600); err != nil {
			t.Fatal(err)
		}
		var l logged
		c.Log = l.log
		if s, err = Open(c); err != nil {
			t.Fatal(err)
		}
		s.SetIndexTraversalWarningCount(-1)
		compareTxid(t, fmt.Sprintf("zip=%v bad mode file", zip), s, &l, collectDate, txidFixtureKeys)
		if _, err := s.XLogContext(context.Background(), collectDate, 1000); !errors.Is(err, ErrLimit) {
			t.Errorf("zip=%v bad mode file: %v", zip, err)
		}
		s.Close()
	}
	// Damaged data: truncated in the middle, and missing.
	damage := map[string]func(dir string) error{
		"truncated": func(dir string) error {
			for _, f := range []string{"xlog/xlog.service", "xlog/data/xlog.0"} {
				p := filepath.Join(dir, collectDate, filepath.FromSlash(f))
				st, err := os.Stat(p)
				if os.IsNotExist(err) {
					continue
				}
				if err != nil {
					return err
				}
				if err := os.Truncate(p, st.Size()/2); err != nil {
					return err
				}
			}
			return nil
		},
		"missing": func(dir string) error {
			for _, f := range []string{"xlog/xlog.service", "xlog/data"} {
				if err := os.RemoveAll(filepath.Join(dir, collectDate, filepath.FromSlash(f))); err != nil {
					return err
				}
			}
			return nil
		},
	}
	for _, zip := range []bool{false, true} {
		for what, apply := range damage {
			dir := t.TempDir()
			c := Config{Directory: dir, Location: time.UTC, CompressXLog: zip}
			s, err := Open(c)
			if err != nil {
				t.Fatal(err)
			}
			putTxidFixture(t, s, collectBase)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if err := apply(dir); err != nil {
				t.Fatal(err)
			}
			var l logged
			c.Log = l.log
			if s, err = Open(c); err != nil {
				t.Fatal(err)
			}
			compareTxid(t, fmt.Sprintf("zip=%v %s", zip, what), s, &l, collectDate, txidFixtureKeys)
			s.Close()
		}
	}
}

// Ingest latency while a txid lookup walks a long hash chain (one bucket:
// every txid of the date in one chain; the oldest txid and an absent one
// walk all of it). The single-hold oracle shows what a walk under the gate
// costs; the lookup must not make a writer wait for it.
func TestLongTxidChainIngestWait(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 300000 records")
	}
	ctx := context.Background()
	for _, zip := range []bool{false, true} {
		var l logged
		s := scanTestStore(t, Config{HashSizeBytes: 5, CompressXLog: zip, Log: l.log})
		s.SetIndexTraversalWarningCount(100)
		for i := 0; i < 300000; i++ {
			if err := s.PutXLog(&protocol.XLogPack{EndTime: scanBase + int64(i%1000), TxID: int64(1000 + i)}); err != nil {
				t.Fatal(err)
			}
		}
		warmIngest(t, s)
		date := JavaDate(scanBase, time.UTC)
		clock := time.Unix(1000, 0)
		s.live.warn.now = func() time.Time { return clock }
		for _, txid := range []int64{1000, 1} {
			clock = clock.Add(11 * time.Second)
			start := time.Now()
			want, werr := oneHoldXLog(s, ctx, date, txid)
			oneHold := time.Since(start)
			wantLog := l.take()
			if werr != nil || (txid == 1000) != (want != nil) {
				t.Fatal(txid, want, werr)
			}
			// The least of a few runs' worst waits: one stall of a loaded
			// host does not decide, a wait of one walk every run does.
			least := time.Duration(1<<63 - 1)
			for run := 0; run < 3; run++ {
				clock = clock.Add(11 * time.Second)
				var got *protocol.XLogPack
				took, worst := measureIngestDuringScan(t, s, func() error {
					var err error
					got, err = s.XLogContext(ctx, date, txid)
					return err
				})
				gotLog := l.take()
				t.Logf("zip=%v txid %d: lookup %v (single hold %v), worst concurrent PutXLog %v, S152 %q", zip, txid, took, oneHold, worst, gotLog)
				if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(gotLog, wantLog) {
					t.Errorf("zip=%v txid %d: got %v %q, want %v %q", zip, txid, got, gotLog, want, wantLog)
				}
				if txid == 1 && len(gotLog) != 1 {
					t.Errorf("zip=%v: a miss over 300000 records logged %q", zip, gotLog)
				}
				least = min(least, worst)
			}
			// Walking under the gate made PutXLog wait about one walk.
			if least > gateHoldLimit() || least > oneHold/2 {
				t.Errorf("zip=%v txid %d: PutXLog waited at least %v behind the chain walk (single hold %v, limit %v)", zip, txid, least, oneHold, gateHoldLimit())
			}
		}
	}
}

// A purge, DB delete or Close between the index hold and the record read
// of a long chain lookup (one bucket, more than scanHoldSteps records):
// an uncompressed record is read through the handle of the index hold (the
// lookup's result had it finished first); a compressed one, read in a gate
// hold, ends as a lookup after the purge (nothing) or ErrClosed.
func TestTxidLookupBetweenHolds(t *testing.T) {
	ctx := context.Background()
	old := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	date := JavaDate(old, time.UTC)
	for _, zip := range []bool{false, true} {
		for _, step := range []string{"purge", "delete", "close"} {
			name := fmt.Sprintf("zip=%v %s", zip, step)
			s, c := longChainFixture(t, zip)
			putCollectKinds(t, s, old, 2*scanHoldSteps, 700, "x")
			want, err := s.XLogContext(ctx, date, 1010)
			if err != nil || want == nil {
				t.Fatal(name, err)
			}
			fired := false
			collectBetweenHolds = func(int) {
				if fired {
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
			got, err := s.XLogContext(ctx, date, 1010)
			collectBetweenHolds = nil
			if !fired {
				t.Fatalf("%s: no read after the index hold", name)
			}
			switch {
			case !zip:
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Errorf("%s: %v %v, want the record", name, got, err)
				}
			case step == "close":
				if !errors.Is(err, ErrClosed) {
					t.Errorf("%s: %v, want ErrClosed", name, err)
				}
			default:
				if err != nil || got != nil {
					t.Errorf("%s: %v %v, want nothing", name, got, err)
				}
			}
			if step != "close" {
				if got, err := s.XLogContext(ctx, date, 1010); err != nil || got != nil {
					t.Errorf("%s: after: %v %v", name, got, err)
				}
			}
			noDecodeSlotsHeld(t, s, name)
		}
	}
}

// Txid lookups (short and long chains) race with writers of the same date
// and chain, a purge and a DB delete of it and Close (-race): every result
// is nothing, an error of the purge or Close, or the record of that txid.
func TestTxidLookupsConcurrentWithPurgeAndClose(t *testing.T) {
	ctx := context.Background()
	old := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	date := JavaDate(old, time.UTC)
	for _, tc := range []struct {
		zip  bool
		hash int
	}{{false, 0}, {true, 0}, {false, 5}, {true, 5}} {
		zip := tc.zip
		s, c := retentionFixtureHash(t, tc.hash)
		s.SetCompression(zip, zip)
		putCollectKinds(t, s, old, 400, 300, "x")
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(w)))
				for {
					select {
					case <-stop:
						return
					default:
					}
					txid := int64(1000 + rng.Intn(500))
					p, err := s.XLogContext(ctx, date, txid)
					if err != nil && !errors.Is(err, ErrClosed) && !errors.Is(err, ErrPurgeOverlap) {
						t.Errorf("zip=%v txid %d: %v", zip, txid, err)
						return
					}
					if p != nil && p.TxID != txid {
						t.Errorf("zip=%v txid %d: record of %d", zip, txid, p.TxID)
						return
					}
				}
			}(w)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				err := s.PutXLog(&protocol.XLogPack{EndTime: old + int64(i), TxID: int64(1400 + i%100)})
				if err != nil && !errors.Is(err, ErrClosed) {
					t.Error(err)
					return
				}
			}
		}()
		time.Sleep(20 * time.Millisecond)
		if _, err := (&Retention{Store: s, Counters: c}).purge(ctx, date, "20261013", PurgeAll); err != nil {
			t.Error(err)
		}
		putCollectKinds(t, s, old, 100, 300, "x")
		time.Sleep(10 * time.Millisecond)
		if _, err := (&DatabaseDeleter{Store: s, Counters: c}).Delete(ctx, []string{"/" + date}); err != nil {
			t.Error(err)
		}
		putCollectKinds(t, s, old, 100, 300, "x")
		time.Sleep(10 * time.Millisecond)
		if err := s.Close(); err != nil {
			t.Error(err)
		}
		time.Sleep(5 * time.Millisecond)
		close(stop)
		wg.Wait()
		noDecodeSlotsHeld(t, s, fmt.Sprint("zip=", zip))
	}
}

// Single txid lookups with short chains (the usual case: default hash
// size, every txid hit): the former single hold vs XLogContext.
func BenchmarkTxidLookup(b *testing.B) {
	ctx := context.Background()
	for _, zip := range []bool{false, true} {
		dir := b.TempDir()
		s, err := Open(Config{Directory: dir, Location: time.UTC, CompressXLog: zip})
		if err != nil {
			b.Fatal(err)
		}
		const n = 20000
		for i := 0; i < n; i++ {
			if err := s.PutXLog(&protocol.XLogPack{EndTime: collectBase + int64(i), TxID: int64(1000 + i), Elapsed: int32(i)}); err != nil {
				b.Fatal(err)
			}
		}
		for _, way := range []struct {
			name string
			run  func(context.Context, string, int64) (*protocol.XLogPack, error)
		}{
			{"one hold", func(ctx context.Context, date string, txid int64) (*protocol.XLogPack, error) {
				return oneHoldXLog(s, ctx, date, txid)
			}},
			{"XLogContext", s.XLogContext},
		} {
			b.Run(fmt.Sprintf("zip=%v/%s", zip, way.name), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					txid := int64(1000 + (i*7919)%n)
					p, err := way.run(ctx, collectDate, txid)
					if err != nil || p == nil || p.TxID != txid {
						b.Fatal(txid, p, err)
					}
				}
			})
		}
		s.Close()
	}
}

// retentionFixtureHash is retentionFixture with Config.HashSizeBytes.
func retentionFixtureHash(t *testing.T, hash int) (*Store, *Counters) {
	t.Helper()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC, HashSizeBytes: hash})
	if err != nil {
		t.Fatal(err)
	}
	c, err := OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close(); c.Close() })
	return s, c
}

// longChainFixture is a Store whose hash indexes have one bucket (every
// txid of a date in one chain), compressed or not.
func longChainFixture(t *testing.T, zip bool) (*Store, *Counters) {
	t.Helper()
	s, c := retentionFixtureHash(t, 5)
	s.SetCompression(zip, zip)
	return s, c
}

// Chains up to scanHoldSteps records are looked up in one gate hold (no
// chain snapshot, no hold between the index and the record); longer ones
// from a snapshot. Both return the single-hold result, at the boundary too.
func TestTxidLookupShortAndLongChains(t *testing.T) {
	for _, zip := range []bool{false, true} {
		for _, n := range []int{1, scanHoldSteps - 1, scanHoldSteps, scanHoldSteps + 1} {
			var l logged
			s := scanTestStore(t, Config{HashSizeBytes: 5, CompressXLog: zip, Log: l.log})
			s.SetIndexTraversalWarningCount(-1)
			for i := 0; i < n; i++ {
				if err := s.PutXLog(&protocol.XLogPack{EndTime: collectBase + int64(i), TxID: int64(1000 + i)}); err != nil {
					t.Fatal(err)
				}
			}
			name := fmt.Sprintf("zip=%v chain %d", zip, n)
			// The oldest record (found at the chain's end), the newest, a miss.
			snapshots, longs := 0, 0
			collectBetweenHolds = func(int) { snapshots++ }
			xlogLongChainObserver = func() { longs++ }
			compareTxid(t, name, s, &l, collectDate, []int64{1000, int64(1000 + n - 1), 1})
			collectBetweenHolds, xlogLongChainObserver = nil, nil
			// Past scanHoldSteps records the oldest txid and the miss take
			// the long path (the oracle never does); only the hit reads
			// through eachRecord. A shorter chain takes it only when its
			// walk outlasted scanHoldTime (a loaded host).
			if n > scanHoldSteps && (longs != 2 || snapshots == 0) {
				t.Errorf("%s: %d long lookups, %d snapshot reads", name, longs, snapshots)
			}
			if n <= scanHoldSteps && longs != 0 {
				t.Logf("%s: %d lookups past scanHoldTime", name, longs)
			}
		}
	}
}

// When every run of a long chain lookup on a compressed date is overlapped
// by pruned purge records, it fails with the retryable ErrPurgeOverlap after
// collectAttempts runs; overlapped once, its rerun returns the single-hold
// result.
func TestTxidLookupPurgeOverlap(t *testing.T) {
	ctx := context.Background()
	s, c := longChainFixture(t, true)
	putCollectKinds(t, s, collectBase, 2*scanHoldSteps, 300, "x")
	want, err := oneHoldXLog(s, ctx, collectDate, 1010)
	if err != nil || want == nil {
		t.Fatal(err)
	}
	runs := 0
	collectBetweenHolds = func(next int) {
		if next == 0 {
			runs++
			manyDeletes(t, s, c, packDiscardMax+2)
		}
	}
	defer func() { collectBetweenHolds = nil }()
	if got, err := s.XLogContext(ctx, collectDate, 1010); !errors.Is(err, ErrPurgeOverlap) || got != nil {
		t.Fatalf("every run overlapped: %v %v", got, err)
	}
	if runs != collectAttempts {
		t.Fatalf("%d runs, want %d", runs, collectAttempts)
	}
	runs = 0
	collectBetweenHolds = func(next int) {
		if next == 0 {
			if runs++; runs == 1 {
				manyDeletes(t, s, c, packDiscardMax+2)
			}
		}
	}
	got, err := s.XLogContext(ctx, collectDate, 1010)
	if err != nil || !reflect.DeepEqual(got, want) || runs != 2 {
		t.Fatalf("overlapped once: %v %v after %d runs, want %v", got, err, runs, want)
	}
}

// S152 reports of txid misses walked outside the gate (long chains) race
// with the in-gate ones (short txid chains, text and object lookups) on
// one sink and rate limiter (-race).
func TestTxidDeepSearchConcurrentWithInGateReports(t *testing.T) {
	ctx := context.Background()
	var l logged
	s := scanTestStore(t, Config{HashSizeBytes: 5, Log: l.log})
	s.SetIndexTraversalWarningCount(-1)
	const longDate, shortDate = collectDate, "20261014"
	putCollectKinds(t, s, collectBase, 2*scanHoldSteps, 100, "x")
	if err := s.PutXLog(&protocol.XLogPack{EndTime: collectBase + 86400000, TxID: 1}); err != nil {
		t.Fatal(err)
	}
	putText(t, s, longDate, "sql", 1, "a")
	var wg sync.WaitGroup
	for w, run := range []func(i int) error{
		func(i int) error { _, err := s.XLogContext(ctx, longDate, int64(1<<20+i)); return err },
		func(i int) error { _, err := s.XLogContext(ctx, longDate, int64(1<<21+i)); return err },
		func(i int) error { _, err := s.XLogContext(ctx, shortDate, int64(1<<20+i)); return err },
		func(i int) error { _, _, err := s.TextContext(ctx, longDate, "sql", int32(100+i)); return err },
		func(i int) error { return s.PutObject(longDate, objectWithHash(int32(100+i), "/o")) },
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if err := run(i); err != nil {
					t.Error(w, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	lines := l.take()
	if len(lines) == 0 {
		t.Fatal("no S152 report")
	}
	for _, line := range lines {
		if !strings.HasPrefix(line, "S152|[warn] Too many index deep searching. ") {
			t.Fatalf("report %q", line)
		}
	}
}

// A txid miss on one-bucket chains at the in-gate limit and just past it:
// the longest gate hold of the short path and the cost of the long one.
func BenchmarkTxidLookupChainLimit(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{scanHoldSteps, scanHoldSteps + 1} {
		s, err := Open(Config{Directory: b.TempDir(), Location: time.UTC, HashSizeBytes: 5})
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < n; i++ {
			if err := s.PutXLog(&protocol.XLogPack{EndTime: collectBase + int64(i), TxID: int64(1000 + i)}); err != nil {
				b.Fatal(err)
			}
		}
		b.Run(fmt.Sprintf("chain=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if p, err := s.XLogContext(ctx, collectDate, 1); err != nil || p != nil {
					b.Fatal(p, err)
				}
			}
		})
		s.Close()
	}
}

// A compressed lookup's block miss is decoded outside the gate while
// writers append scanHoldSteps+ records to the same chain: the retry finds
// the chain too long for one hold and takes the long path, with the
// single-hold result, no S152 for the hit and every decode slot released.
func TestTxidMissRetryTurnsLong(t *testing.T) {
	ctx := context.Background()
	var l logged
	s := scanTestStore(t, Config{HashSizeBytes: 5, CompressXLog: true, Log: l.log})
	s.SetIndexTraversalWarningCount(-1)
	for i := 0; i < 100; i++ {
		if err := s.PutXLog(&protocol.XLogPack{EndTime: collectBase + int64(i), TxID: int64(1000 + i)}); err != nil {
			t.Fatal(err)
		}
	}
	dropZipCache(s)
	decodes, longs := 0, 0
	zipDecodeHook = func() {
		if decodes++; decodes > 1 || !gateFree(s) {
			return
		}
		for i := 0; i < scanHoldSteps+50; i++ {
			if err := s.PutXLog(&protocol.XLogPack{EndTime: collectBase + 200 + int64(i), TxID: int64(5000 + i)}); err != nil {
				t.Error(err)
			}
		}
	}
	xlogLongChainObserver = func() { longs++ }
	got, err := s.XLogContext(ctx, collectDate, 1000)
	zipDecodeHook, xlogLongChainObserver = nil, nil
	gotLog := l.take()
	want, werr := oneHoldXLog(s, ctx, collectDate, 1000)
	wantLog := l.take()
	if decodes == 0 || longs != 1 {
		t.Fatalf("%d decodes, %d long lookups", decodes, longs)
	}
	if err != nil || werr != nil || want == nil || !reflect.DeepEqual(got, want) || len(gotLog) != 0 || len(wantLog) != 0 {
		t.Fatalf("got %v %v %q, want %v %v %q", got, err, gotLog, want, werr, wantLog)
	}
	noDecodeSlotsHeld(t, s, "retry turned long")
}
