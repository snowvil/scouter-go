package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

// The one-hold scans below are the implementations before the realtime
// counter scans moved to bounded gate holds (scanRealtimeHolds). They are the
// result oracle: the same records, order, limits and errors.

func oneHoldRealtime(c *Counters, date string, hash int32, name string, start, end int64) ([]CounterPoint, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, err := c.table(date, true, false)
	if err != nil || t == nil {
		return nil, err
	}
	out := []CounterPoint{}
	err = t.index.ScanRange(start, end, false, func(r legacyindex.Record) error {
		if len(r.Key) != 8 || len(r.Value) != 9 {
			return protocol.ErrInvalid
		}
		tm := int64(binary.BigEndian.Uint64(r.Key))
		if int32(binary.BigEndian.Uint32(r.Value[5:])) != hash {
			return nil
		}
		pos, err := readFive(r.Value)
		if err != nil {
			return err
		}
		head := make([]byte, 4)
		if _, err = t.data.ReadAt(head, pos); err != nil {
			return err
		}
		n := int(binary.BigEndian.Uint32(head))
		if n > 8<<20 {
			return protocol.ErrLimit
		}
		b := make([]byte, n)
		if _, err = t.data.ReadAt(b, pos+4); err != nil {
			return err
		}
		d := protocol.NewDecoder(b, protocol.Limits{})
		count := d.Decimal()
		if count < 0 || count > 4096 {
			return protocol.ErrLimit
		}
		for i := int64(0); i < count; i++ {
			id := d.Decimal()
			v := d.Value()
			if d.Err() != nil {
				return d.Err()
			}
			if id < 1 || id > int64(len(t.names)) {
				return protocol.ErrInvalid
			}
			if t.names[id-1] == name {
				if len(out) >= 100000 {
					return protocol.ErrLimit
				}
				out = append(out, CounterPoint{tm, v})
			}
		}
		if d.Err() != nil {
			return d.Err()
		}
		if d.Remaining() != 0 {
			return protocol.ErrTrailing
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func oneHoldCounterRows(c *Counters, date string, hash int32, start, end int64) ([]CounterRow, error) {
	ctx := context.Background()
	c.mu.Lock()
	defer c.mu.Unlock()
	t, err := c.table(date, true, false)
	if err != nil || t == nil {
		return nil, err
	}
	var rows []CounterRow
	scanned, used := 0, 0
	err = t.index.ScanRange(start, end, false, func(r legacyindex.Record) error {
		if scanned >= counterCatalogRecords || used > counterCatalogBytes-len(r.Key)-len(r.Value) {
			return ErrLimit
		}
		scanned++
		used += len(r.Key) + len(r.Value)
		if len(r.Key) != 8 || len(r.Value) != 9 {
			return ErrCorrupt
		}
		if int32(binary.BigEndian.Uint32(r.Value[5:])) != hash {
			return nil
		}
		pos, err := readFive(r.Value)
		if err != nil {
			return errors.Join(ErrCorrupt, err)
		}
		body, err := counterCatalogBody(ctx, t.data, pos, counterCatalogBytes-used)
		if err != nil {
			return err
		}
		used += len(body) + 4
		values, err := counterCatalogMap(body, t.names)
		if err != nil {
			return err
		}
		rows = append(rows, CounterRow{Time: int64(binary.BigEndian.Uint64(r.Key)), Values: values})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func oneHoldBulkRows(c *Counters, date string, start, end int64) ([]CounterRow, error) {
	ctx := context.Background()
	c.mu.Lock()
	defer c.mu.Unlock()
	t, err := c.table(date, true, false)
	if err != nil || t == nil {
		return nil, err
	}
	var rows []CounterRow
	used := 0
	for minute := start; minute <= end; minute += 60000 {
		chunkEnd := min(minute+59999, end)
		oldest, err := counterBulkEndpoint(ctx, t.index, minute, chunkEnd, false)
		if err != nil {
			return nil, err
		}
		newest, err := counterBulkEndpoint(ctx, t.index, minute, chunkEnd, true)
		if err != nil {
			return nil, err
		}
		if oldest == nil || newest == nil {
			continue
		}
		first, err := readFive(oldest.Value)
		if err != nil {
			return nil, err
		}
		last, err := readFive(newest.Value)
		if err != nil {
			return nil, err
		}
		if first == 0 || last == 0 {
			continue
		}
		for pos := first; ; {
			if len(rows) >= counterCatalogRecords {
				return nil, ErrLimit
			}
			body, err := counterCatalogBody(ctx, t.data, pos, counterCatalogBytes-used)
			if err != nil {
				return nil, err
			}
			used += len(body) + 4
			values, err := counterCatalogMap(body, t.names)
			if err != nil {
				return nil, err
			}
			rows = append(rows, CounterRow{Values: values})
			if pos == last {
				break
			}
			pos += int64(len(body)) + 4
		}
	}
	return rows, nil
}

func scanPut(t *testing.T, c *Counters, tm int64, obj string, entries ...protocol.Entry) {
	t.Helper()
	if err := c.PutContext(context.Background(), &protocol.PerfCounterPack{Time: tm, ObjName: obj, TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: entries}}); err != nil {
		t.Fatal(err)
	}
}
func counterDecimal(key string, n int64) protocol.Entry {
	return protocol.Entry{Key: key, Value: protocol.Value{Type: protocol.ValueDecimal, Int: n}}
}

// scanFixture writes realtime counters spread over many 500 ms buckets
// (several objects and records per bucket, names added over time) plus one
// dense bucket larger than scanSortInline, so a scan takes many gate holds
// and orders a bucket between holds.
func scanFixture(t *testing.T) (*Counters, int64) {
	t.Helper()
	c, start := catalogCounters(t)
	objs := []string{"/a", "/b", "/c"}
	for i := int64(0); i < 4000; i++ {
		entries := []protocol.Entry{counterDecimal("cpu", i), counterDecimal("mem", -i)}
		if i%3 == 0 {
			entries = append(entries, counterDecimal(fmt.Sprintf("disk%d", i%7), i))
		}
		scanPut(t, c, start+i*137, objs[i%3], entries...)
	}
	dense := start + 900000
	for i := int64(0); i < 1500; i++ {
		// Out-of-order times and repeated times inside one bucket.
		scanPut(t, c, dense+(i*37)%499, "/a", counterDecimal("cpu", 100000+i))
	}
	return c, start
}

type scanQuery struct{ from, to int64 }

func scanQueries(start int64) []scanQuery {
	return []scanQuery{
		{start, start + 86399999},
		{start + 333, start + 700777},
		{start + 899999, start + 901000},
		{start + 900100, start + 900300},
		{start + 548000, start + 548000},
	}
}

func TestRealtimeCounterScansMatchOneHold(t *testing.T) {
	c, start := scanFixture(t)
	ctx := context.Background()
	holds := 0
	counterScanBetweenHolds = func() { holds++ }
	defer func() { counterScanBetweenHolds = nil }()
	for _, q := range scanQueries(start) {
		for _, obj := range []string{"/a", "/b", "/missing"} {
			for _, name := range []string{"cpu", "mem", "disk3", "none"} {
				want, wantErr := oneHoldRealtime(c, "20260101", CounterHash(obj), name, q.from, q.to)
				got, err := c.RealtimeContext(ctx, "20260101", CounterHash(obj), name, q.from, q.to)
				if !errors.Is(err, wantErr) || !reflect.DeepEqual(got, want) {
					t.Fatalf("realtime %s %s %+v: got %d %v want %d %v", obj, name, q, len(got), err, len(want), wantErr)
				}
			}
			want, wantErr := oneHoldCounterRows(c, "20260101", CounterHash(obj), q.from, q.to)
			got, err := c.CounterRowsContext(ctx, "20260101", CounterHash(obj), q.from, q.to)
			if !errors.Is(err, wantErr) || !reflect.DeepEqual(got, want) {
				t.Fatalf("rows %s %+v: got %d %v want %d %v", obj, q, len(got), err, len(want), wantErr)
			}
		}
		if q.to-q.from < 86400000-1 { // bulk scans whole minutes; keep the day query for the others
			want, wantErr := oneHoldBulkRows(c, "20260101", q.from, q.to)
			got, err := c.CounterBulkRowsContext(ctx, "20260101", q.from, q.to)
			if !errors.Is(err, wantErr) || !reflect.DeepEqual(got, want) {
				t.Fatalf("bulk %+v: got %d %v want %d %v", q, len(got), err, len(want), wantErr)
			}
		}
	}
	want, _ := oneHoldBulkRows(c, "20260101", start, start+86399999)
	got, err := c.CounterBulkRowsContext(ctx, "20260101", start, start+86399999)
	if err != nil || len(want) < 5000 || !reflect.DeepEqual(got, want) {
		t.Fatalf("bulk day: got %d %v want %d", len(got), err, len(want))
	}
	if holds < 20 {
		t.Fatalf("scans took %d extra gate holds; want many", holds)
	}
	// Missing date: nil, nil as before.
	if p, err := c.RealtimeContext(ctx, "20260102", 0, "cpu", start+86400000, start+86400001); p != nil || err != nil {
		t.Fatal(p, err)
	}
}

// A day-long realtime counter query must not keep PerfCounterPack ingest
// (Counters.PutContext, called under the ingest engine lock) waiting: the
// Counters gate is free between holds.
func TestRealtimeCounterScanFreesGateForIngest(t *testing.T) {
	c, start := scanFixture(t)
	ctx := context.Background()
	want, err := oneHoldRealtime(c, "20260101", CounterHash("/a"), "cpu", start, start+86399999)
	if err != nil {
		t.Fatal(err)
	}
	for _, scan := range []struct {
		name string
		run  func() error
	}{
		{"realtime", func() error {
			got, err := c.RealtimeContext(ctx, "20260101", CounterHash("/a"), "cpu", start, start+86399999)
			if err == nil && !reflect.DeepEqual(got, want) {
				err = fmt.Errorf("realtime result changed: %d, want %d", len(got), len(want))
			}
			return err
		}},
		{"rows", func() error {
			_, err := c.CounterRowsContext(ctx, "20260101", CounterHash("/a"), start, start+86399999)
			return err
		}},
		{"bulk", func() error {
			_, err := c.CounterBulkRowsContext(ctx, "20260101", start, start+86399999)
			return err
		}},
	} {
		ingested, n := 0, 0
		counterScanBetweenHolds = func() {
			n++
			if n%5 != 1 {
				return
			}
			// Ingest in another goroutine, as the engine does, while the scan
			// is between holds; a new counter name grows the dictionary the
			// scan's batches still read (race detector).
			done := make(chan error, 1)
			go func() {
				ictx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				done <- c.PutContext(ictx, &protocol.PerfCounterPack{Time: start + 86399000, ObjName: "/ingest", TimeType: 1,
					Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{counterDecimal(fmt.Sprintf("%s%d", scan.name, n), 1)}}})
			}()
			if err := <-done; err != nil {
				t.Errorf("%s: ingest during scan: %v", scan.name, err)
			}
			ingested++
		}
		if err := scan.run(); err != nil {
			t.Fatalf("%s: %v", scan.name, err)
		}
		counterScanBetweenHolds = nil
		if ingested < 2 {
			t.Fatalf("%s: ingest ran between %d holds; want several", scan.name, ingested)
		}
	}
	// Concurrent ingest from other goroutines while scans run (no hook).
	stop := make(chan struct{})
	ingestDone := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				ingestDone <- nil
				return
			default:
			}
			if err := c.PutContext(ctx, &protocol.PerfCounterPack{Time: start + 86000000 + int64(i), ObjName: "/a", TimeType: 1,
				Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{counterDecimal(fmt.Sprintf("live%d", i%50), 1), counterDecimal("cpu", 7)}}}); err != nil {
				ingestDone <- err
				return
			}
		}
	}()
	for range 3 {
		got, err := c.RealtimeContext(ctx, "20260101", CounterHash("/a"), "cpu", start, start+85999999)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("concurrent realtime: %d %v", len(got), err)
		}
	}
	close(stop)
	if err := <-ingestDone; err != nil {
		t.Fatal(err)
	}
}

// Between holds the table may be evicted and reopened (same file: the scan
// goes on), purged (the scan ends quietly with what it read), recreated after
// a purge (another file: ends) or the Counters closed (ErrClosed).
func TestRealtimeCounterScanAcrossEvictionPurgeAndClose(t *testing.T) {
	c, start := scanFixture(t)
	ctx := context.Background()
	hash := CounterHash("/a")
	want, err := oneHoldRealtime(c, "20260101", hash, "cpu", start, start+86399999)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { counterScanBetweenHolds = nil }()
	n := 0
	counterScanBetweenHolds = func() {
		n++
		if n%15 != 0 {
			return
		}
		// Opening 8 other days evicts the scanned table.
		for d := 2; d <= 9; d++ {
			scanPut(t, c, start+int64(d)*86400000, "/other", counterDecimal("cpu", 1))
		}
	}
	got, err := c.RealtimeContext(ctx, "20260101", hash, "cpu", start, start+86399999)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("eviction: %d %v want %d", len(got), err, len(want))
	}
	if st := c.IndexOpenStats(); st.Walked+st.Checkpointed+st.Created < 12 {
		t.Fatalf("table was not reopened: %+v", st)
	}

	purge := func(recreate bool) func() {
		n := 0
		return func() {
			if n++; n != 2 {
				return
			}
			c.mu.Lock()
			if err := c.releaseDateLocked("20260101", true); err != nil {
				t.Error(err)
			}
			for _, suffix := range []string{".data", ".kfile", ".hfile", ".head"} {
				if err := os.Remove(filepath.Join(c.root, "20260101/counter/real"+suffix)); err != nil && !os.IsNotExist(err) {
					t.Error(err)
				}
			}
			c.mu.Unlock()
			if recreate {
				scanPut(t, c, start+86399000, "/a", counterDecimal("cpu", 1))
			}
		}
	}
	counterScanBetweenHolds = purge(true)
	got, err = c.RealtimeContext(ctx, "20260101", hash, "cpu", start, start+86399999)
	if err != nil || len(got) == 0 || len(got) >= len(want) || !reflect.DeepEqual(got, want[:len(got)]) {
		t.Fatalf("recreated after purge: %d %v", len(got), err)
	}
	scanFixtureAgain := func() {
		counterScanBetweenHolds = nil
		for i := int64(0); i < 3000; i++ {
			scanPut(t, c, start+i*137, "/a", counterDecimal("cpu", i))
		}
	}
	scanFixtureAgain()
	counterScanBetweenHolds = purge(false)
	got, err = c.RealtimeContext(ctx, "20260101", hash, "cpu", start, start+86399999)
	if err != nil || len(got) == 0 || len(got) >= 3000 {
		t.Fatalf("purged: %d %v", len(got), err)
	}
	// A purge whose recreated file looks the same (inode reuse, no shrink):
	// the per-date purge count alone ends the scan.
	scanFixtureAgain()
	marked := 0
	counterScanBetweenHolds = func() {
		if marked++; marked == 2 {
			c.mu.Lock()
			c.markPurgedLocked("20260101")
			c.mu.Unlock()
		}
	}
	full, err := oneHoldRealtime(c, "20260101", hash, "cpu", start, start+86399999)
	if err != nil {
		t.Fatal(err)
	}
	got, err = c.RealtimeContext(ctx, "20260101", hash, "cpu", start, start+86399999)
	if err != nil || len(got) == 0 || len(got) >= len(full) {
		t.Fatalf("purge with the same file: %d %v of %d", len(got), err, len(full))
	}
	// A purge of another date does not end it.
	marked = 0
	counterScanBetweenHolds = func() {
		if marked++; marked == 2 {
			c.mu.Lock()
			c.markPurgedLocked("20251231")
			c.mu.Unlock()
		}
	}
	got, err = c.RealtimeContext(ctx, "20260101", hash, "cpu", start, start+86399999)
	if err != nil || !reflect.DeepEqual(got, full) {
		t.Fatalf("other date purge: %d %v of %d", len(got), err, len(full))
	}
	counterScanBetweenHolds = func() { _ = c.Close() }
	if _, err = c.CounterRowsContext(ctx, "20260101", hash, start, start+86399999); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}

func TestRealtimeCounterScanCancelBetweenHolds(t *testing.T) {
	c, start := scanFixture(t)
	defer func() { counterScanBetweenHolds = nil }()
	ctx, cancel := context.WithCancel(context.Background())
	counterScanBetweenHolds = cancel
	if _, err := c.RealtimeContext(ctx, "20260101", CounterHash("/a"), "cpu", start, start+86399999); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	counterScanBetweenHolds = cancel
	if _, err := c.CounterBulkRowsContext(ctx, "20260101", start, start+86399999); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// RealtimeMultiContext gives each object exactly what the one-hold
// per-object scan gives it (order, repeated and out-of-order times, mixed
// value types), in one index walk; objects without points have no entry.
func TestRealtimeMultiMatchesPerObject(t *testing.T) {
	c, start := scanFixture(t)
	scanPut(t, c, start+5000, "/b", protocol.Entry{Key: "cpu", Value: protocol.Value{Type: protocol.ValueFloat, Float: 1.5}})
	ctx := context.Background()
	hashes := []int32{CounterHash("/a"), CounterHash("/b"), CounterHash("/c"), CounterHash("/none"), CounterHash("/a")}
	for _, q := range scanQueries(start) {
		for _, name := range []string{"cpu", "disk3", "missing"} {
			series, err := c.RealtimeMultiContext(ctx, "20260101", hashes, name, q.from, q.to)
			if err != nil {
				t.Fatal(q, name, err)
			}
			for _, h := range hashes {
				want, err := oneHoldRealtime(c, "20260101", h, name, q.from, q.to)
				if err != nil {
					t.Fatal(err)
				}
				got, err := series[h].Points()
				if err != nil {
					t.Fatal(err)
				}
				if len(want) == 0 && series[h] != nil {
					t.Fatal("entry without points", h)
				}
				if len(want) == 0 {
					want = []CounterPoint{}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%v %s %d: %d points, want %d", q, name, h, len(got), len(want))
				}
			}
		}
	}
	if series, err := c.RealtimeMultiContext(ctx, "20250101", hashes, "cpu", start-86400000*365, start-86400000*365+1000); err != nil || series != nil {
		t.Fatal("no table", series, err)
	}
}
