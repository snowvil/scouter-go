package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

func scanTestStore(t *testing.T, config Config) *Store {
	t.Helper()
	config.Directory, config.Location = t.TempDir(), time.UTC
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

var scanBase = time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC).UnixMilli()

func putScanXLogs(t *testing.T, s *Store, n int, end func(i int) int64, obj func(i int) int32) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := s.PutXLog(&protocol.XLogPack{EndTime: end(i), TxID: int64(i + 1), ObjHash: obj(i), Elapsed: int32(i)}); err != nil {
			t.Fatal(err)
		}
	}
}
func scanTxIDs(t *testing.T, s *Store, q XLogScan) ([]int64, []int64) {
	t.Helper()
	var ids, buckets []int64
	err := s.ScanXLogsContext(context.Background(), q, func(r XLogRecord) (bool, error) {
		ids = append(ids, r.Pack.TxID)
		buckets = append(buckets, r.BucketTime)
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return ids, buckets
}

// Batches resume inside a bucket (V2 chain position, V1 bucket snapshot) and
// still produce exactly the single-pass legacyindex order.
func TestScanXLogsBatchesKeepIndexOrder(t *testing.T) {
	s := scanTestStore(t, Config{})
	// 1300 records in one 500 ms bucket (> scanBatchRecords), the rest spread.
	putScanXLogs(t, s, 2000, func(i int) int64 {
		if i < 1300 {
			return scanBase + 100 + int64(i%7)
		}
		return scanBase + 600 + int64(i)*3
	}, func(int) int32 { return 1 })
	date := JavaDate(scanBase, time.UTC)
	from, to := scanBase, scanBase+600+2000*3
	want := func(paged, reverse bool) []int64 {
		s.mu.Lock()
		defer s.mu.Unlock()
		idx, err := s.index(s.path(date, "xlog", "xlog_tim"), true, false)
		if err != nil {
			t.Fatal(err)
		}
		var ids []int64
		visit := func(r legacyindex.Record) error {
			p, _, err := s.decodeXLogContext(context.Background(), date, r.Value)
			if err != nil {
				return err
			}
			if tm := int64(binary.BigEndian.Uint64(r.Key)); tm >= from && tm <= to {
				ids = append(ids, p.TxID)
			}
			return nil
		}
		if !paged {
			err = idx.ScanRange(from, to, reverse, visit)
		} else {
			for b := from; b <= to && err == nil; b += 500 {
				err = idx.ScanBucket(b, false, visit)
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		return ids
	}
	for _, tc := range []struct{ paged, reverse bool }{{false, false}, {false, true}, {true, false}} {
		got, _ := scanTxIDs(t, s, XLogScan{Date: date, From: from, To: to, Paged: tc.paged, Reverse: tc.reverse})
		exp := want(tc.paged, tc.reverse)
		if len(got) != 2000 || len(got) != len(exp) {
			t.Fatalf("%+v: got %d want %d", tc, len(got), len(exp))
		}
		for i := range got {
			if got[i] != exp[i] {
				t.Fatalf("%+v: record %d txid %d want %d", tc, i, got[i], exp[i])
			}
		}
	}
}

// Regression: the object filter and max run in the scan. A range far
// above MaxQueryRecords succeeds when the selected result is small, and the
// scan ends at max instead of reading the rest.
func TestScanXLogsFilterAndMaxInsideScan(t *testing.T) {
	s := scanTestStore(t, Config{MaxQueryRecords: 50})
	putScanXLogs(t, s, 400, func(i int) int64 { return scanBase + int64(i)*10 }, func(i int) int32 {
		if i%100 == 7 {
			return 7
		}
		return 8
	})
	date := JavaDate(scanBase, time.UTC)
	q := XLogScan{Date: date, From: scanBase, To: scanBase + 4000, Match: func(p *protocol.XLogPack) bool { return p.ObjHash == 7 }}
	if ids, _ := scanTxIDs(t, s, q); len(ids) != 4 || ids[0] != 8 || ids[3] != 308 {
		t.Fatal(ids)
	}
	if _, err := s.XLogsContext(context.Background(), date, scanBase, scanBase+4000, false); !errors.Is(err, ErrLimit) {
		t.Fatal("collected API keeps its result cap", err)
	}
	calls := 0
	q.Match = func(*protocol.XLogPack) bool { calls++; return true }
	q.Max = 3
	if ids, _ := scanTxIDs(t, s, q); len(ids) != 3 || calls != 3 {
		t.Fatalf("max did not end the scan: ids=%v match calls=%d", ids, calls)
	}
	// V2 pages: Max counts matches after the resume txid.
	started := false
	q = XLogScan{Date: date, From: scanBase, To: scanBase + 4000, Paged: true, Max: 2, Match: func(p *protocol.XLogPack) bool {
		if !started {
			started = p.TxID == 108
			return false
		}
		return p.ObjHash == 7
	}}
	ids, buckets := scanTxIDs(t, s, q)
	if len(ids) != 2 || ids[0] != 208 || ids[1] != 308 || buckets[0] != scanBase+2000 {
		t.Fatal(ids, buckets)
	}
}

// visit runs outside the store gate: ingest proceeds while a Client is slow.
func TestScanXLogsVisitDoesNotHoldGate(t *testing.T) {
	s := scanTestStore(t, Config{})
	putScanXLogs(t, s, 10, func(i int) int64 { return scanBase + int64(i) }, func(int) int32 { return 1 })
	date := JavaDate(scanBase, time.UTC)
	first := true
	err := s.ScanXLogsContext(context.Background(), XLogScan{Date: date, From: scanBase, To: scanBase + 100}, func(r XLogRecord) (bool, error) {
		if !first {
			return true, nil
		}
		first = false
		done := make(chan error, 1)
		go func() { done <- s.PutXLog(&protocol.XLogPack{EndTime: scanBase + 50, TxID: 99, ObjHash: 1}) }()
		select {
		case err := <-done:
			return true, err
		case <-time.After(5 * time.Second):
			return false, errors.New("PutXLog blocked by a scan visit")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestScanXLogsContextCancel(t *testing.T) {
	s := scanTestStore(t, Config{})
	putScanXLogs(t, s, 1200, func(i int) int64 { return scanBase + int64(i) }, func(int) int32 { return 1 })
	ctx, cancel := context.WithCancel(context.Background())
	n := 0
	err := s.ScanXLogsContext(ctx, XLogScan{Date: JavaDate(scanBase, time.UTC), From: scanBase, To: scanBase + 2000}, func(XLogRecord) (bool, error) {
		n++
		if n == 600 {
			cancel()
		}
		return true, nil
	})
	if !errors.Is(err, context.Canceled) || n != 600 {
		t.Fatal(err, n)
	}
}

// Regression: Status/Summary/Alert time scans bound only the collected
// result. A query whose filter keeps few records reads past MaxQueryRecords,
// and the Scan* variants have no cap at all.
func TestTimedPackScansBoundOnlyTheCollectedResult(t *testing.T) {
	s := scanTestStore(t, Config{MaxQueryRecords: 50})
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		obj := int32(8)
		if i%50 == 3 {
			obj = 7
		}
		if err := s.PutStatusContext(ctx, &protocol.StatusPack{Time: scanBase + int64(i), ObjHash: obj, Key: "k", Data: mapValue()}); err != nil {
			t.Fatal(err)
		}
		if err := s.PutSummaryContext(ctx, &protocol.SummaryPack{Time: scanBase + int64(i), SType: 1, Table: mapValue()}); err != nil {
			t.Fatal(err)
		}
		if err := s.PutAlertContext(ctx, &protocol.AlertPack{Time: scanBase + int64(i), Tags: mapValue()}); err != nil {
			t.Fatal(err)
		}
	}
	date := JavaDate(scanBase, time.UTC)
	if got, err := s.QueryListContext(ctx, date, scanBase, scanBase+1000, 7); err != nil || len(got) != 4 {
		t.Fatal(len(got), err)
	}
	if _, err := s.SummariesContext(ctx, date, scanBase, scanBase+1000, 1); !errors.Is(err, ErrLimit) {
		t.Fatal("collected summaries keep their cap", err)
	}
	n := 0
	if err := s.ScanSummariesContext(ctx, date, scanBase, scanBase+1000, 1, func(*protocol.SummaryPack) (bool, error) { n++; return true, nil }); err != nil || n != 200 {
		t.Fatal(n, err)
	}
	n = 0
	if err := s.ScanAlertsContext(ctx, date, scanBase, scanBase+1000, func(*protocol.AlertPack) (bool, error) { n++; return n < 10, nil }); err != nil || n != 10 {
		t.Fatal(n, err)
	}
}

func (s *Store) xlogBytesContext(ctx context.Context, date string, pos int64) ([]byte, error) {
	return s.xlogBytesFetch(ctx, date, pos, nil)
}

func (s *Store) decodeXLogContext(ctx context.Context, date string, value []byte) (*protocol.XLogPack, int, error) {
	return s.decodeXLogFetch(ctx, date, value, nil)
}
