package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTagFirst100NextAndTypedValues(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	for i := 1; i <= 101; i++ {
		p := &protocol.XLogPack{EndTime: tm, ObjHash: 42, Service: int32(i), Elapsed: 1}
		if err = s.PutXLogTagsContext(ctx, "java", p); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	total, err := s.TagValuesContext(ctx, "20261013", "java", "service", "service")
	if err != nil || total.HowManyValues != 100 || total.TotalCount != 100 {
		t.Fatal(total, err)
	}
	series, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service", protocol.Value{Type: protocol.ValueTextHash, Int: 101})
	if err != nil || len(series) != 1440 || series[430] != 1 {
		t.Fatal(series, err)
	}
	missing, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service", protocol.Value{Type: protocol.ValueDecimal, Int: 101})
	if err != nil || missing != nil {
		t.Fatal("typed Value", missing, err)
	}
	if err = s.MakeTagTop100Context(ctx, "20261013", "java"); err != nil {
		t.Fatal(err)
	}
	total, err = s.TagValuesContext(ctx, "20261013", "java", "service", "service")
	if err != nil || total.HowManyValues != 101 || total.TotalCount != 101 || len(total.Values) != 100 {
		t.Fatal(total, err)
	}
}

func TestTagActualORCursorReverseAndRaw(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 7, 0, 0, 0, time.UTC).UnixMilli()
	if err = s.PutObject("20261013", sampleObject("/synthetic")); err != nil {
		t.Fatal(err)
	}
	var expected [][]byte
	for i := int64(1); i <= 3; i++ {
		p := &protocol.XLogPack{EndTime: tm + i*500, ObjHash: 42, TxID: i, Service: 7, Error: 8}
		b, err := protocol.EncodePack(p, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		expected = append(expected, b)
		if err = s.PutXLog(p); err != nil {
			t.Fatal(err)
		}
	}
	q := TagActualQuery{Date: "20261013", ObjType: "java", Group: "service", From: tm, To: tm + 2000, Max: 20, TxID: 1, Filter: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "error", Value: protocol.Value{Type: protocol.ValueList, Values: []protocol.Value{textHash(999)}}}, {Key: "service", Value: protocol.Value{Type: protocol.ValueList, Values: []protocol.Value{textHash(7)}}}}}}
	got, err := s.TagActualContext(ctx, q)
	if err != nil || len(got) != 2 || !bytes.Equal(got[0], expected[1]) || !bytes.Equal(got[1], expected[2]) {
		t.Fatal("OR/cursor/raw", len(got), err)
	}
	q.TxID = 0
	q.Reverse = true
	q.Max = 1
	got, err = s.TagActualContext(ctx, q)
	if err != nil || len(got) != 1 || !bytes.Equal(got[0], expected[2]) {
		t.Fatal("reverse max", len(got), err)
	}
	q.Filter = protocol.Value{Type: protocol.ValueMap}
	got, err = s.TagActualContext(ctx, q)
	if err != nil || len(got) != 0 {
		t.Fatal("empty filter", got, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = s.TagActualContext(cancelled, q)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestAggregationCorruptAndSymlinkRejected(t *testing.T) {
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	path := filepath.Join(root, "20261013/tagcnt/java")
	if err = os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(path, "first.kfile"), []byte{0xca, 0xfe, 0}, 0600); err != nil {
		t.Fatal(err)
	}
	_, err = s.TagValuesContext(ctx, "20261013", "java", "service", "service")
	if !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err = os.Symlink(outside, filepath.Join(root, "20261014")); err != nil {
		t.Fatal(err)
	}
	tm := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC).UnixMilli()
	if err = s.PutVisitorContext(ctx, tm, "java", 1, 42); !errors.Is(err, ErrInvalid) {
		t.Fatal("symlink", err)
	}
}
func TestTagCacheEvictionSyncCloseAndOwnedSeries(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	for i := 0; i < 31; i++ {
		if err = s.PutXLogTagsContext(ctx, fmt.Sprintf("java%d", i), &protocol.XLogPack{EndTime: tm, ObjHash: 42, Service: 7}); err != nil {
			t.Fatal(err)
		}
	}
	lockTags(s)
	exceeded := len(s.tagCounts.tables) > 30 || len(s.files) > 16 || len(s.tags.files) > tagFileMax || s.tagCounts.pendingBytes > 64<<20
	s.tags.mu.Unlock()
	if exceeded {
		t.Fatal("cache exceeded")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 31; i++ {
		series, err := s.TagValueSeriesContext(ctx, "20261013", fmt.Sprintf("java%d", i), "service", "service", textHash(7))
		if err != nil || len(series) != 1440 || series[430] != 1 {
			t.Fatal(i, series, err)
		}
		series[430] = 99
		again, err := s.TagValueSeriesContext(ctx, "20261013", fmt.Sprintf("java%d", i), "service", "service", textHash(7))
		if err != nil || again[430] != 1 {
			t.Fatal("borrowed result", err)
		}
	}
}

// tagActualOneHold is the former single-hold TagActualContext (service and
// alert), kept as the oracle of the hold-split scan.
func tagActualOneHold(s *Store, q TagActualQuery) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx := context.Background()
	objects, err := s.objectsLockedForTags(ctx, q.Date)
	if err != nil {
		return nil, err
	}
	hashes := map[int32]bool{}
	for _, o := range objects {
		if o.ObjType == q.ObjType {
			hashes[o.ObjHash] = true
		}
	}
	spec := alertRecordSpec
	if q.Group == "service" {
		spec = timedPackSpec{"xlog", "xlog", ".xlog", "_tim", 4, protocol.PackXLog}
	}
	path := s.path(q.Date, spec.dir, spec.base)
	idx, err := s.index(path+spec.index, true, false)
	if err != nil || idx == nil {
		return nil, err
	}
	maxRecords, maxBytes := s.extraQueryBudget()
	used, scanned := 0, 0
	ready := q.TxID == 0
	var out [][]byte
	err = idx.ScanRange(q.From, q.To, q.Reverse, func(r legacyindex.Record) error {
		if scanned >= maxRecords {
			return ErrLimit
		}
		scanned++
		pos, err := dataOffset(r.Value)
		if err != nil {
			return err
		}
		var b []byte
		if q.Group == "service" {
			b, err = s.xlogBytesContext(ctx, q.Date, pos)
		} else {
			b, err = s.extraReadData(ctx, path+spec.extension, pos, spec.prefix, maxBytes-used)
		}
		if err != nil {
			return err
		}
		used += len(b)
		p, err := protocol.DecodePack(b, s.config.CodecLimits)
		if err != nil {
			return err
		}
		if used > maxBytes {
			return ErrLimit
		}
		if x, ok := p.(*protocol.XLogPack); ok {
			if hashes[x.ObjHash] {
				if !ready {
					ready = x.TxID == q.TxID
				} else if TagServiceFilterOK(q.Filter, x) {
					out = append(out, append([]byte(nil), b...))
				}
			}
		} else if x, ok := p.(*protocol.AlertPack); ok && hashes[x.ObjHash] && TagAlertFilterOK(q.Filter, x) {
			out = append(out, b)
		}
		if len(out) >= q.Max {
			return errLookupDone
		}
		return nil
	})
	if errors.Is(err, errLookupDone) {
		err = nil
	}
	return out, err
}

// The hold-split TagActualContext returns what one hold returned (order,
// cursor, filter, max, reverse, scan budget) across many holds, each within
// the hold bound, for XLogs and alerts.
func TestTagActualHoldsMatchOneHold(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, MaxQueryRecords: 2500})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 7, 0, 0, 0, time.UTC).UnixMilli()
	other := sampleObject("/other")
	other.ObjHash, other.ObjType = 43, "tomcat"
	for _, o := range []*protocol.ObjectPack{sampleObject("/synthetic"), other} {
		if err = s.PutObject("20261013", o); err != nil {
			t.Fatal(err)
		}
	}
	for i := int64(0); i < 2000; i++ {
		obj := int32(42)
		if i%4 == 3 {
			obj = 43
		}
		// Repeated and out-of-order end times inside 500ms buckets.
		x := &protocol.XLogPack{EndTime: tm + (i*37)%60000, ObjHash: obj, TxID: i + 1, Service: int32(7 + i%3), Error: int32(i % 2)}
		if err = s.PutXLog(x); err != nil {
			t.Fatal(err)
		}
		if i%5 == 0 {
			a := &protocol.AlertPack{Time: tm + (i*53)%60000, Level: byte(i % 3), ObjType: "java", ObjHash: obj, Title: "t", Message: "m", Tags: protocol.Value{Type: protocol.ValueMap}}
			if err = s.PutAlertContext(ctx, a); err != nil {
				t.Fatal(err)
			}
		}
	}
	svc := protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "service", Value: protocol.Value{Type: protocol.ValueList, Values: []protocol.Value{textHash(7), textHash(8)}}}}}
	level := protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "level", Value: protocol.Value{Type: protocol.ValueList, Values: []protocol.Value{{Type: protocol.ValueDecimal, Int: 1}}}}}}
	holds := 0
	scanHoldObserver = func(d time.Duration) { holds++ }
	defer func() { scanHoldObserver = nil }()
	for _, q := range []TagActualQuery{
		{Group: "service", From: tm, To: tm + 60000, Max: 100000, Filter: svc},
		{Group: "service", From: tm + 777, To: tm + 41234, Max: 300, TxID: 401, Filter: svc},
		{Group: "service", From: tm, To: tm + 60000, Max: 100000, Reverse: true, Filter: svc},
		{Group: "service", From: tm, To: tm + 3000, Max: 0, Filter: svc},
		{Group: "alert", From: tm, To: tm + 60000, Max: 100000, Filter: level},
		{Group: "alert", From: tm, To: tm + 60000, Max: 5, Reverse: true, Filter: level},
	} {
		q.Date, q.ObjType = "20261013", "java"
		want, werr := tagActualOneHold(s, q)
		if q.Max > 0 && len(want) == 0 {
			t.Fatalf("%+v: empty oracle", q)
		}
		holds = 0
		got, gerr := s.TagActualContext(ctx, q)
		if (werr == nil) != (gerr == nil) || (werr != nil && !errors.Is(gerr, ErrLimit)) || len(got) != len(want) {
			t.Fatalf("%+v: %d %v, want %d %v", q, len(got), gerr, len(want), werr)
		}
		for i := range want {
			if !bytes.Equal(got[i], want[i]) {
				t.Fatalf("%+v: record %d differs", q, i)
			}
		}
		if q.Max > 1000 && holds < 3 {
			t.Fatalf("%+v: %d holds", q, holds)
		}
	}
	// A scan past the record budget fails as it did in one hold.
	q := TagActualQuery{Date: "20261013", ObjType: "java", Group: "service", From: tm, To: tm + 60000, Max: 100000, Filter: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "service", Value: protocol.Value{Type: protocol.ValueList, Values: []protocol.Value{textHash(999)}}}}}}
	s.config.MaxQueryRecords = 1500
	if _, err := tagActualOneHold(s, q); !errors.Is(err, ErrLimit) {
		t.Fatal("oracle budget", err)
	}
	if _, err := s.TagActualContext(ctx, q); !errors.Is(err, ErrLimit) {
		t.Fatal("budget", err)
	}
}
