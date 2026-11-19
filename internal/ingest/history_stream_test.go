package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/storage"
)

// Regression: TRANX_LOAD_TIME_GROUP(_V2) over a range holding far more
// XLogs than MaxQueryRecords succeeds when the object filter selects few, and
// max/pageCount end the scan (Java XLogService handlers).
func TestXLogsHistoryFiltersInsideTheScan(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	root := t.TempDir()
	s, err := storage.Open(storage.Config{Directory: root, Location: time.UTC, MaxQueryRecords: 50})
	if err != nil {
		t.Fatal(err)
	}
	c, err := storage.OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{Location: time.UTC, Clock: func() time.Time { return now }, XLogCapacity: 8}, s, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close(); c.Close(); s.Close() })
	base := now.UnixMilli()
	for i := 0; i < 400; i++ {
		obj := int32(8)
		if i%100 == 7 {
			obj = 7
		}
		if err := s.PutXLog(&protocol.XLogPack{EndTime: base + int64(i)*10, TxID: int64(i + 1), ObjHash: obj, Elapsed: int32(i)}); err != nil {
			t.Fatal(err)
		}
	}
	date := storage.JavaDate(base, time.UTC)
	run := func(q service.HistoryQuery) ([]int64, service.HistoryResult) {
		t.Helper()
		var ids []int64
		result, err := e.XLogsHistory(context.Background(), q, func(x *protocol.XLogPack) error {
			ids = append(ids, x.TxID)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return ids, result
	}
	q := service.HistoryQuery{Date: date, From: base, To: base + 4000, ObjectHashes: []int32{7}, MinElapsed: -1}
	if ids, _ := run(q); len(ids) != 4 || ids[0] != 8 || ids[3] != 308 {
		t.Fatal("V1 filter", ids)
	}
	q.Max, q.Reverse = 2, true
	if ids, _ := run(q); len(ids) != 2 || ids[0] != 308 || ids[1] != 208 {
		t.Fatal("V1 reverse max", ids)
	}
	q = service.HistoryQuery{Date: date, From: base, To: base + 4000, ObjectHashes: []int32{7}, PageCount: 3}
	ids, page := run(q)
	if len(ids) != 3 || !page.HasMore || page.LastTxID != 208 || page.LastBucketTime != base+2000 {
		t.Fatal("V2 page 1", ids, page)
	}
	q.LastBucketTime, q.LastTxID = page.LastBucketTime, page.LastTxID
	ids, page = run(q)
	if len(ids) != 1 || ids[0] != 308 || page.HasMore {
		t.Fatal("V2 page 2", ids, page)
	}
}
