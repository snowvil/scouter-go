package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

// The former single-hold collect queries, kept verbatim as the oracle of the
// hold-split ones (profile.go): same bytes, order, limits and first error.

func oneHoldProfileChunks(s *Store, ctx context.Context, date string, txid int64) ([][]byte, error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	if err := s.check(date); err != nil {
		return nil, err
	}
	path := s.path(date, "xlog", "xlog")
	idx, err := s.index(path+"_profile", false, false)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if err != nil || idx == nil {
		return nil, err
	}
	offsets := make([]int64, 0)
	err = idx.ScanKey(longKey(txid), func(r legacyindex.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(offsets) >= s.config.MaxQueryRecords {
			return ErrLimit
		}
		pos, err := dataOffset(r.Value)
		if err != nil {
			return err
		}
		offsets = append(offsets, pos)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	zip, err := s.compressionMode(path+".profile.conf", "compress_profile_enabled", s.live.compressProfile.Load(), false)
	if err != nil {
		return nil, err
	}
	chunks := make([][]byte, 0, len(offsets))
	used := 0
	for _, pos := range offsets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var b []byte
		if zip {
			b, err = s.readZipContext(ctx, date, pos)
		} else {
			b, err = s.readDataContext(ctx, path+".profile", pos, 4)
		}
		if err != nil {
			return nil, err
		}
		used += len(b)
		if used > s.config.MaxQueryBytes {
			return nil, ErrLimit
		}
		chunks = append(chunks, b)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return chunks, nil
}

func oneHoldXLogsByGxID(s *Store, ctx context.Context, date string, gxid int64) ([]*protocol.XLogPack, error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	if err := s.check(date); err != nil {
		return nil, err
	}
	idx, err := s.index(s.path(date, "xlog", "xlog_gid"), false, false)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if err != nil || idx == nil {
		return nil, err
	}
	var out []*protocol.XLogPack
	used := 0
	err = idx.ScanKeyContext(ctx, longKey(gxid), func(r legacyindex.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(out) >= s.config.MaxQueryRecords {
			return ErrLimit
		}
		pack, n, err := s.decodeXLogContext(ctx, date, r.Value)
		if err != nil {
			return err
		}
		if pack.GxID != gxid {
			return ErrCorrupt
		}
		if used > s.config.MaxQueryBytes-n {
			return ErrLimit
		}
		used += n
		out = append(out, pack)
		return nil
	})
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

func oneHoldSpansByGxID(s *Store, ctx context.Context, date string, gxid int64) ([]*protocol.SpanPack, error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	if err := s.check(date); err != nil {
		return nil, err
	}
	path := s.path(date, "span", "span")
	idx, err := s.index(path+"_gid", false, false)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if err != nil || idx == nil {
		return nil, err
	}
	var out []*protocol.SpanPack
	used, containers := 0, 0
	err = idx.ScanKeyContext(ctx, longKey(gxid), func(r legacyindex.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if containers >= s.config.MaxQueryRecords {
			return ErrLimit
		}
		containers++
		pos, err := dataOffset(r.Value)
		if err != nil {
			return err
		}
		raw, err := s.readDataContext(ctx, path+".data", pos, 2)
		if err != nil {
			return err
		}
		if used > s.config.MaxQueryBytes-len(raw) {
			return ErrLimit
		}
		used += len(raw)
		pack, err := protocol.DecodePack(raw, s.config.CodecLimits)
		if err != nil {
			return err
		}
		container, ok := pack.(*protocol.SpanContainerPack)
		if !ok || container.GxID != gxid {
			return ErrCorrupt
		}
		if out == nil {
			out = make([]*protocol.SpanPack, 0)
		}
		reader := bytes.NewReader(container.Spans)
		for reader.Len() > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(out) >= s.config.MaxQueryRecords {
				return ErrLimit
			}
			nested, err := protocol.ReadPack(reader, s.config.CodecLimits)
			if err != nil {
				return err
			}
			span, ok := nested.(*protocol.SpanPack)
			if !ok {
				return fmt.Errorf("%w: span Blob contains another Pack", ErrCorrupt)
			}
			out = append(out, span)
		}
		return nil
	})
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

const collectDate = "20261013"

var collectBase = time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()

// putCollectFixture writes, interleaved: txid 1 with chunks profile blocks
// and txid 2 with a few, chunks XLogs of gxid 7 and some of gxid 8, and
// chunks span containers of gxid 7 (two spans each) and some of gxid 8.
func putCollectFixture(t *testing.T, s *Store, base int64, chunks, chunkBytes int) {
	t.Helper()
	putCollectKinds(t, s, base, chunks, chunkBytes, "pxs")
}

// putCollectKinds writes the fixture kinds named in kinds: p(rofile),
// x(log), s(pan).
func putCollectKinds(t *testing.T, s *Store, base int64, chunks, chunkBytes int, kinds string) {
	t.Helper()
	has := func(k byte) bool { return bytes.IndexByte([]byte(kinds), k) >= 0 }
	for i := 0; i < chunks; i++ {
		size := 1 + (i*37)%chunkBytes
		if has('p') {
			if err := s.PutProfile(&protocol.ProfilePack{Time: base + int64(i), TxID: 1, Profile: bytes.Repeat([]byte{byte(i), byte(i >> 8)}, size/2+1)}); err != nil {
				t.Fatal(err)
			}
			if i%5 == 0 {
				if err := s.PutProfile(&protocol.ProfilePack{Time: base + int64(i), TxID: 2, Profile: []byte{2, byte(i)}}); err != nil {
					t.Fatal(err)
				}
			}
		}
		gxid := int64(7)
		if i%4 == 3 {
			gxid = 8
		}
		if has('x') {
			if err := s.PutXLog(&protocol.XLogPack{EndTime: base + int64(i), TxID: int64(1000 + i), GxID: gxid, ObjHash: int32(i), Elapsed: int32(i)}); err != nil {
				t.Fatal(err)
			}
		}
		if has('s') {
			sp := container(t, collectSpan(t, gxid, int64(2*i)), collectSpan(t, gxid, int64(2*i+1)))
			sp.GxID, sp.Timestamp = gxid, base+int64(i)
			if err := s.PutSpanContainer(sp); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func collectSpan(t *testing.T, gxid, id int64) *protocol.SpanPack {
	p := testSpan(t, id)
	p.GxID = gxid
	return p
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// compareCollect runs every collect query both ways on s.
func compareCollect(t *testing.T, name string, s *Store, date string) {
	t.Helper()
	ctx := context.Background()
	for _, txid := range []int64{1, 2, 3} {
		got, gerr := s.ProfileChunksContext(ctx, date, txid)
		want, werr := oneHoldProfileChunks(s, ctx, date, txid)
		if errText(gerr) != errText(werr) || !reflect.DeepEqual(got, want) {
			t.Errorf("%s: profile %d: got %d chunks %v, want %d chunks %v", name, txid, len(got), gerr, len(want), werr)
		}
	}
	for _, gxid := range []int64{7, 8, 9} {
		got, gerr := s.XLogsByGxIDContext(ctx, date, gxid)
		want, werr := oneHoldXLogsByGxID(s, ctx, date, gxid)
		if errText(gerr) != errText(werr) || !reflect.DeepEqual(got, want) {
			t.Errorf("%s: gxid XLogs %d: got %d %v, want %d %v", name, gxid, len(got), gerr, len(want), werr)
		}
		spans, serr := s.SpansByGxIDContext(ctx, date, gxid)
		wantSpans, wserr := oneHoldSpansByGxID(s, ctx, date, gxid)
		if errText(serr) != errText(wserr) || !reflect.DeepEqual(spans, wantSpans) {
			t.Errorf("%s: spans %d: got %d %v, want %d %v", name, gxid, len(spans), serr, len(wantSpans), wserr)
		}
	}
}

// The hold-split collect queries return what the single-hold ones returned:
// results, order, record/byte limits, damaged files and missing data.
func TestCollectQueriesMatchOneHold(t *testing.T) {
	for _, zip := range []bool{false, true} {
		for _, lim := range []struct {
			name            string
			records, nbytes int
		}{{"no limit", 0, 0}, {"record limit", 40, 0}, {"byte limit", 0, 6000}, {"one record", 1, 0}} {
			name := fmt.Sprintf("zip=%v %s", zip, lim.name)
			s := scanTestStore(t, Config{CompressXLog: zip, CompressProfile: zip, MaxQueryRecords: lim.records, MaxQueryBytes: lim.nbytes})
			putCollectFixture(t, s, collectBase, 300, 700)
			compareCollect(t, name, s, collectDate)
			// No index at all, and an invalid date.
			compareCollect(t, name+" empty date", s, "20261001")
			if _, err := s.ProfileChunksContext(context.Background(), "2026101x", 1); !errors.Is(err, ErrInvalid) {
				t.Errorf("%s: invalid date %v", name, err)
			}
		}
	}
	// Damaged data files: truncated (a read past the end in the middle of
	// the result) and missing (the index names records of no file).
	damage := map[string]func(dir string) error{
		"truncated": func(dir string) error {
			for _, f := range []string{"xlog/xlog.profile", "xlog/xlog.service", "span/span.data", "xlog/data/xlog.0"} {
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
			for _, f := range []string{"xlog/xlog.profile", "xlog/xlog.service", "span/span.data", "xlog/data"} {
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
			c := Config{Directory: dir, Location: time.UTC, CompressXLog: zip, CompressProfile: zip}
			s, err := Open(c)
			if err != nil {
				t.Fatal(err)
			}
			putCollectFixture(t, s, collectBase, 200, 700)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if err := apply(dir); err != nil {
				t.Fatal(err)
			}
			if s, err = Open(c); err != nil {
				t.Fatal(err)
			}
			compareCollect(t, fmt.Sprintf("zip=%v %s", zip, what), s, collectDate)
			s.Close()
		}
	}
}

// collectResult holds the results of the three collect queries of the
// fixture keys.
type collectResult struct {
	profile [][]byte
	xlogs   []*protocol.XLogPack
	spans   []*protocol.SpanPack
	errs    [3]error
}

type collectQuerier interface {
	ProfileChunksContext(context.Context, string, int64) ([][]byte, error)
	XLogsByGxIDContext(context.Context, string, int64) ([]*protocol.XLogPack, error)
	SpansByGxIDContext(context.Context, string, int64) ([]*protocol.SpanPack, error)
}

func runCollect(s collectQuerier, ctx context.Context, date string) collectResult {
	var r collectResult
	r.profile, r.errs[0] = s.ProfileChunksContext(ctx, date, 1)
	r.xlogs, r.errs[1] = s.XLogsByGxIDContext(ctx, date, 7)
	r.spans, r.errs[2] = s.SpansByGxIDContext(ctx, date, 7)
	return r
}

// A Store gate user (here PutXLog of another date) gets the gate between the
// holds of a large profile query: the uncompressed blocks are read without
// the gate (one hold: the index walk), the compressed ones in holds of at
// most scanHoldSteps blocks. A writer that needs the gate completes at every
// point where the query is between holds.
func TestCollectQueriesLetGateUsersInBetween(t *testing.T) {
	for _, zip := range []bool{false, true} {
		s := scanTestStore(t, Config{CompressXLog: zip, CompressProfile: zip})
		const chunks = 1500
		putCollectFixture(t, s, collectBase, chunks, 4000)
		want := runCollect(oneHoldStore{s}, context.Background(), collectDate)
		for _, err := range want.errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		var holds int
		var mu sync.Mutex
		scanHoldObserver = func(time.Duration) { mu.Lock(); holds++; mu.Unlock() }
		var points, last []int
		tx := int64(1 << 40)
		collectBetweenHolds = func(next int) {
			points = append(points, next)
			if len(points)%50 != 1 {
				return
			}
			// The query holds no gate here: a writer must finish.
			tx++
			done := make(chan error, 1)
			go func() {
				done <- s.PutXLog(&protocol.XLogPack{EndTime: collectBase + 3*86400000, TxID: tx, ObjHash: 9})
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("writer blocked while the query was between holds")
			}
		}
		got := runCollect(s, context.Background(), collectDate)
		collectBetweenHolds, scanHoldObserver = nil, nil
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("zip=%v: result differs", zip)
		}
		last = points
		if zip {
			// Profile and gxid XLogs read in holds: consecutive hold starts
			// are at most scanHoldSteps records apart.
			if holds <= 3 {
				t.Fatalf("zip: %d holds", holds)
			}
		} else if holds != 3 {
			// One index hold per query; every record outside the gate.
			t.Fatalf("uncompressed: %d holds, want the 3 index holds", holds)
		}
		for i := 1; i < len(last); i++ {
			if d := last[i] - last[i-1]; d > scanHoldSteps {
				t.Fatalf("zip=%v: %d records in one hold", zip, d)
			}
		}
		t.Logf("zip=%v: %d gate holds, %d between-hold points", zip, holds, len(points))
	}
}

// oneHoldStore runs the oracle queries through runCollect.
type oneHoldStore struct{ *Store }

func (o oneHoldStore) ProfileChunksContext(ctx context.Context, date string, txid int64) ([][]byte, error) {
	return oneHoldProfileChunks(o.Store, ctx, date, txid)
}
func (o oneHoldStore) XLogsByGxIDContext(ctx context.Context, date string, gxid int64) ([]*protocol.XLogPack, error) {
	return oneHoldXLogsByGxID(o.Store, ctx, date, gxid)
}
func (o oneHoldStore) SpansByGxIDContext(ctx context.Context, date string, gxid int64) ([]*protocol.SpanPack, error) {
	return oneHoldSpansByGxID(o.Store, ctx, date, gxid)
}

// Ingest latency while a large profile (about 24 MiB) is read: the gate is
// held for the index walk only (uncompressed) or for bounded holds.
func TestProfileQueryIngestWaitIsBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a 24 MiB profile")
	}
	for _, zip := range []bool{false, true} {
		s := scanTestStore(t, Config{CompressProfile: zip, MaxRecordBytes: 1 << 20})
		block := make([]byte, 256<<10)
		for i := 0; i < 96; i++ {
			for n := range block {
				block[n] = byte(n*7 + i)
			}
			if err := s.PutProfile(&protocol.ProfilePack{Time: scanBase + int64(i), TxID: 1, Profile: block}); err != nil {
				t.Fatal(err)
			}
		}
		date := JavaDate(scanBase, time.UTC)
		var size int
		took, worst := measureIngestDuringScan(t, s, func() error {
			chunks, err := s.ProfileChunksContext(context.Background(), date, 1)
			for _, c := range chunks {
				size += len(c)
			}
			return err
		})
		t.Logf("zip=%v: %d bytes in %v, worst concurrent PutXLog %v", zip, size, took, worst)
		if size != 96*len(block) {
			t.Fatalf("zip=%v: %d bytes", zip, size)
		}
		if worst > gateHoldLimit() {
			t.Errorf("zip=%v: PutXLog waited %v behind the profile query (limit %v)", zip, worst, gateHoldLimit())
		}
	}
}

// A purge, DB delete or Close between the holds of a collect query: an
// uncompressed query finishes on the inode it opened (the result of a query
// that completed first), a compressed one returns the after-purge result
// (nothing, no error) or ErrClosed, and never mixes in records written after
// the purge at the same offsets.
func TestCollectQueriesDuringPurgeDeleteAndClose(t *testing.T) {
	old := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	date := JavaDate(old, time.UTC)
	ctx := context.Background()
	recreateKinds := ""
	type step func(t *testing.T, s *Store, c *Counters)
	purge := func(modes ...PurgeMode) step {
		return func(t *testing.T, s *Store, c *Counters) {
			r := &Retention{Store: s, Counters: c}
			for _, mode := range modes {
				if _, err := r.purge(ctx, date, "20261013", mode); err != nil {
					t.Error(err)
				}
			}
		}
	}
	steps := []struct {
		name   string
		do     step
		zipErr error // compressed outcome: no result and this error
	}{
		{"purge ALL", purge(PurgeAll), nil},
		{"purge PROFILE, XLOG", purge(PurgeProfile, PurgeXLog), nil},
		{"db delete", func(t *testing.T, s *Store, c *Counters) {
			if n, err := (&DatabaseDeleter{Store: s, Counters: c}).Delete(ctx, []string{"/" + date}); n != 1 || err != nil {
				t.Error(n, err)
			}
		}, nil},
		{"purge and recreate", func(t *testing.T, s *Store, c *Counters) {
			purge(PurgeAll)(t, s, c)
			putCollectKinds(t, s, old+5000, 600, 300, recreateKinds) // other records, same offsets
		}, nil},
		{"close", func(t *testing.T, s *Store, c *Counters) {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		}, ErrClosed},
	}
	queries := []struct {
		name, kind string
		run, which func(*Store) (any, error)
		compressed bool // reads GZipStore records when the date is compressed
	}{
		{"profile", "p", func(s *Store) (any, error) { return s.ProfileChunksContext(ctx, date, 1) },
			func(s *Store) (any, error) { return oneHoldProfileChunks(s, ctx, date, 1) }, true},
		{"gxid XLogs", "x", func(s *Store) (any, error) { return s.XLogsByGxIDContext(ctx, date, 7) },
			func(s *Store) (any, error) { return oneHoldXLogsByGxID(s, ctx, date, 7) }, true},
		{"spans", "s", func(s *Store) (any, error) { return s.SpansByGxIDContext(ctx, date, 7) },
			func(s *Store) (any, error) { return oneHoldSpansByGxID(s, ctx, date, 7) }, false},
	}
	for _, zip := range []bool{false, true} {
		for _, st := range steps {
			for _, q := range queries {
				for _, at := range []int{0, 1} {
					name := fmt.Sprintf("zip=%v %s during %s (at %d)", zip, st.name, q.name, at)
					s, c, _ := retentionFixture(t)
					s.SetCompression(zip, zip)
					recreateKinds = q.kind
					putCollectKinds(t, s, old, 600, 300, q.kind)
					want, err := q.which(s)
					if err != nil || reflect.ValueOf(want).Len() < 300 {
						t.Fatal(name, err)
					}
					fired := false
					collectBetweenHolds = func(next int) {
						if !fired && next >= at {
							fired = true
							st.do(t, s, c)
						}
					}
					got, err := q.run(s)
					collectBetweenHolds = nil
					if !fired {
						t.Fatalf("%s: the step never ran", name)
					}
					if zip && q.compressed {
						if err != st.zipErr || !reflect.ValueOf(got).IsNil() {
							t.Errorf("%s: %d records %v, want none and %v", name, reflect.ValueOf(got).Len(), err, st.zipErr)
						}
						continue
					}
					if err != nil || !reflect.DeepEqual(got, want) {
						t.Errorf("%s: %d records %v, want the %d before the step", name, reflect.ValueOf(got).Len(), err, reflect.ValueOf(want).Len())
					}
				}
			}
		}
	}
}

// Concurrent collect queries, writers and purges (run under -race).
func TestCollectQueriesConcurrentWithIngestAndPurge(t *testing.T) {
	s, c, _ := retentionFixture(t)
	old := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	date := JavaDate(old, time.UTC)
	ctx := context.Background()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	errs := make(chan error, 8)
	for _, zip := range []bool{false, true} {
		s.SetCompression(zip, zip)
		putCollectFixture(t, s, old, 100, 200)
	}
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			r := runCollect(s, ctx, date)
			for _, err := range r.errs {
				// A purge may land between a query's holds; only its own
				// errors (none here) or nothing are acceptable.
				if err != nil {
					errs <- err
					return
				}
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := s.PutProfile(&protocol.ProfilePack{Time: old + int64(i), TxID: 1, Profile: []byte{byte(i)}}); err != nil {
				errs <- err
				return
			}
			if err := s.PutXLog(&protocol.XLogPack{EndTime: old + int64(i), TxID: int64(5000 + i), GxID: 7}); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		r := &Retention{Store: s, Counters: c}
		for i := 0; i < 30; i++ {
			s.SetCompression(i%2 == 0, i%2 == 0)
			if _, err := r.purge(ctx, date, "20261013", PurgeAll); err != nil {
				errs <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// manyDeletes records more than packDiscardMax purge/delete records (one per
// DB delete target) for dates other than the queried one.
var manyDeleteSeq int

func manyDeletes(t *testing.T, s *Store, c *Counters, n int) {
	t.Helper()
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		manyDeleteSeq++
		rel := fmt.Sprintf("20260801/d%05d", manyDeleteSeq)
		touch(t, filepath.Join(s.config.Directory, filepath.FromSlash(rel), "x"))
		names = append(names, "/"+rel)
	}
	if got, err := (&DatabaseDeleter{Store: s, Counters: c}).Delete(context.Background(), names); got != n || err != nil {
		t.Fatal(got, err)
	}
}

// More purge/delete records than packDiscardMax between the holds of a
// compressed query, none of them for the queried date: the query must not
// answer "purged" (a silent empty result for a live date, today included).
func TestCompressedCollectSurvivesPrunedPurgeRecords(t *testing.T) {
	ctx := context.Background()
	for _, q := range []struct {
		name, kind string
		run, which func(*Store) (any, error)
	}{
		{"profile", "p", func(s *Store) (any, error) { return s.ProfileChunksContext(ctx, collectDate, 1) },
			func(s *Store) (any, error) { return oneHoldProfileChunks(s, ctx, collectDate, 1) }},
		{"gxid XLogs", "x", func(s *Store) (any, error) { return s.XLogsByGxIDContext(ctx, collectDate, 7) },
			func(s *Store) (any, error) { return oneHoldXLogsByGxID(s, ctx, collectDate, 7) }},
	} {
		s, c, _ := retentionFixture(t)
		s.SetCompression(true, true)
		putCollectKinds(t, s, collectBase, 600, 300, q.kind)
		want, err := q.which(s)
		if err != nil || reflect.ValueOf(want).Len() < 300 {
			t.Fatal(q.name, err)
		}
		fired := false
		collectBetweenHolds = func(next int) {
			if !fired && next >= 1 {
				fired = true
				manyDeletes(t, s, c, packDiscardMax+6)
			}
		}
		got, err := q.run(s)
		collectBetweenHolds = nil
		if !fired {
			t.Fatalf("%s: deletes never ran", q.name)
		}
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("%s: %d records %v after pruned purge records, want %d", q.name, reflect.ValueOf(got).Len(), err, reflect.ValueOf(want).Len())
		}
	}
}

// When every run of the query is overlapped by pruned purge records, it fails
// with the retryable ErrPurgeOverlap instead of guessing.
func TestCompressedCollectPurgeOverlapIsAnError(t *testing.T) {
	s, c, _ := retentionFixture(t)
	s.SetCompression(true, true)
	putCollectKinds(t, s, collectBase, 600, 300, "px")
	runs := 0
	collectBetweenHolds = func(next int) {
		if next == 0 {
			runs++
			manyDeletes(t, s, c, packDiscardMax+2)
		}
	}
	defer func() { collectBetweenHolds = nil }()
	if got, err := s.ProfileChunksContext(context.Background(), collectDate, 1); !errors.Is(err, ErrPurgeOverlap) || got != nil {
		t.Fatalf("profile: %d %v", len(got), err)
	}
	if runs != collectAttempts {
		t.Fatalf("profile: %d runs, want %d", runs, collectAttempts)
	}
	runs = 0
	if got, err := s.XLogsByGxIDContext(context.Background(), collectDate, 7); !errors.Is(err, ErrPurgeOverlap) || got != nil {
		t.Fatalf("gxid: %d %v", len(got), err)
	}
	if runs != collectAttempts {
		t.Fatalf("gxid: %d runs", runs)
	}
}
