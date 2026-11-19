package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func TestCounterRestartAndSparseBuckets(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Seoul")
	root := t.TempDir()
	c, err := OpenCounters(root, loc)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 13, 12, 3, 0, 0, loc).UnixMilli()
	p := &protocol.PerfCounterPack{Time: now, ObjName: "/한글/app", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 37}}}}}
	if err = c.Put(p); err != nil {
		t.Fatal(err)
	}
	p.TimeType = 3
	if err = c.Put(p); err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = OpenCounters(root, loc)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	points, err := c.Realtime("20261013", CounterHash(p.ObjName), "TPS", now-1, now+1)
	if err != nil || len(points) != 1 || points[0].Value.Int != 37 {
		t.Fatalf("points=%v err=%v", points, err)
	}
	vals, err := c.Daily("20261013", CounterHash(p.ObjName), "TPS", 3)
	if err != nil || len(vals) != 288 || vals[144].Int != 37 || vals[143].Type != protocol.ValueNull {
		t.Fatalf("values=%v err=%v", vals, err)
	}
	b, err := os.ReadFile(filepath.Join(root, "20261013", "counter", "5m.data"))
	if err != nil || len(b) != 2882 || b[0] != 20 || b[1] != 3 || b[2+144*10] != 20 {
		t.Fatalf("daily bytes %d err %v", len(b), err)
	}
	p.Data.Entries[0].Value = protocol.Value{Type: protocol.ValueDouble, Double: 37}
	if err = c.Put(p); err == nil {
		t.Fatal("type change should be explicit error")
	}
}
func TestCounterLimitsAndTimezone(t *testing.T) {
	loc, _ := time.LoadLocation("America/New_York")
	tm := time.Date(2026, 7, 1, 0, 0, 0, 0, loc).UnixMilli()
	if JavaDate(tm, loc) != "20260630" || JavaMillis(tm, loc) != 23*3600000 {
		t.Fatalf("fixed Java DST boundary %s %d", JavaDate(tm, loc), JavaMillis(tm, loc))
	}
	c, err := OpenCounters(t.TempDir(), loc)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = c.Daily("../escape", 0, "x", 3); err == nil {
		t.Fatal("unsafe date accepted")
	}
	if err = c.Put(&protocol.PerfCounterPack{}); err == nil {
		t.Fatal("empty counter accepted")
	}
}

func TestCounterNarrowQueryBeyondWholeDayRecordLimit(t *testing.T) {
	c, err := OpenCounters(t.TempDir(), time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	start := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC).UnixMilli()
	p := &protocol.PerfCounterPack{ObjName: "/dense", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 37}}}}}
	for i := int64(0); i < 100001; i++ {
		p.Time = start + i
		if err = c.Put(p); err != nil {
			t.Fatal(err)
		}
	}
	last := start + 100000
	points, err := c.Realtime("20261013", CounterHash(p.ObjName), "TPS", last, last)
	if err != nil || len(points) != 1 || points[0].Time != last {
		t.Fatalf("narrow query lost the last record: %v %v", points, err)
	}
	// The whole range exceeds the 100000 point cap, across many gate holds,
	// exactly as the former one-hold scan did.
	if _, err = oneHoldRealtime(c, "20261013", CounterHash(p.ObjName), "TPS", start, last); !errors.Is(err, protocol.ErrLimit) {
		t.Fatalf("one-hold oracle: %v", err)
	}
	if points, err = c.Realtime("20261013", CounterHash(p.ObjName), "TPS", start, last); !errors.Is(err, protocol.ErrLimit) || points != nil {
		t.Fatalf("point cap: %d %v", len(points), err)
	}
	if points, err = c.Realtime("20261013", CounterHash(p.ObjName), "TPS", start+1, last); err != nil || len(points) != 100000 {
		t.Fatalf("at the cap: %d %v", len(points), err)
	}
}
