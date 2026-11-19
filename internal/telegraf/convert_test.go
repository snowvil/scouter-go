package telegraf

import (
	"math"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func entry(t *testing.T, p *protocol.PerfCounterPack, key string) (protocol.Value, bool) {
	t.Helper()
	for _, e := range p.Data.Entries {
		if e.Key == key {
			return e.Value, true
		}
	}
	return protocol.Value{}, false
}

func point(t *testing.T, cfg *Config, s string, received int64) *Point {
	t.Helper()
	line := ParseLine(s)
	p, reason := Resolve(&line, cfg, received)
	if p == nil {
		t.Fatalf("%s: %v", s, reason)
	}
	return p
}

func deltaConfig(t *testing.T, normalizeDefault bool, mappings string) *Config {
	t.Helper()
	cfg, _ := FromProperties(map[string]string{
		"input_telegraf_delta_counter_normalize_default": map[bool]string{true: "true", false: "false"}[normalizeDefault],
		"input_telegraf_$n$_objFamily_base":              "n",
		"input_telegraf_$n$_counter_mappings":            mappings,
	}, time.Unix(0, 0))
	return cfg
}

func TestDeltaWithoutNormalization(t *testing.T) {
	clock := &fakeClock{time.Unix(1700000000, 0)}
	cfg := deltaConfig(t, false, "&req:req,&f:f")
	conv := NewConverter(Limits{}, clock.now)
	first := conv.PerfCounterPack(point(t, cfg, "n,host=h req=100i,f=1.5 1700000000000000000", 1), cfg)
	if len(first.Data.Entries) != 0 || first.Time != 1 || first.TimeType != 1 || first.ObjName != "/h/X$" {
		t.Fatalf("first sample has no delta: %+v", first)
	}
	clock.t = clock.t.Add(10 * time.Second)
	second := conv.PerfCounterPack(point(t, cfg, "n,host=h req=160i,f=4 1700000010000000000", 2), cfg)
	if v, ok := entry(t, second, "req_$delta"); !ok || v.Type != protocol.ValueFloat || v.Float != 6 {
		t.Fatalf("req %+v", v)
	}
	if v, ok := entry(t, second, "f_$delta"); !ok || v.Float != 0.25 {
		t.Fatalf("f %+v", v)
	}
	if _, ok := entry(t, second, "req"); ok {
		t.Fatal("DELTA has no normal counter")
	}
	// Same timestamp again: Java float division by zero (D5).
	third := conv.PerfCounterPack(point(t, cfg, "n,host=h req=170i,f=4 1700000010000000000", 3), cfg)
	if v, _ := entry(t, third, "req_$delta"); !math.IsInf(float64(v.Float), 1) {
		t.Fatalf("expected +Inf, got %v", v.Float)
	}
	if v, _ := entry(t, third, "f_$delta"); !math.IsNaN(float64(v.Float)) {
		t.Fatalf("expected NaN, got %v", v.Float)
	}
	// The previous value expires after 120s.
	clock.t = clock.t.Add(121 * time.Second)
	if p := conv.PerfCounterPack(point(t, cfg, "n,host=h req=200i 1700000200000000000", 4), cfg); len(p.Data.Entries) != 0 {
		t.Fatalf("expired previous value used: %+v", p)
	}
}

func TestDeltaDefaultNormalization(t *testing.T) {
	clock := &fakeClock{time.Unix(1700000000, 0)}
	cfg := deltaConfig(t, true, "&&req:req")
	conv := NewConverter(Limits{}, clock.now)
	conv.PerfCounterPack(point(t, cfg, "n,host=h req=100i 1700000000000000000", 1), cfg)
	clock.t = clock.t.Add(10 * time.Second)
	p := conv.PerfCounterPack(point(t, cfg, "n,host=h req=160i 1700000010000000000", 2), cfg)
	if v, _ := entry(t, p, "req_$delta"); v.Float != 6 {
		t.Fatal(v)
	}
	if v, _ := entry(t, p, "req"); v.Type != protocol.ValueDecimal || v.Int != 160 {
		t.Fatalf("BOTH keeps the raw normal value: %+v", v)
	}
	clock.t = clock.t.Add(10 * time.Second)
	p = conv.PerfCounterPack(point(t, cfg, "n,host=h req=260i 1700000020000000000", 3), cfg)
	if v, _ := entry(t, p, "req_$delta"); v.Float != 8 { // (6 + 10) / 2 within the 60s window
		t.Fatal(v)
	}
	// Past the 2s x 30 bucket window only the newest rate remains.
	clock.t = clock.t.Add(70 * time.Second)
	p = conv.PerfCounterPack(point(t, cfg, "n,host=h req=330i 1700000090000000000", 4), cfg)
	if v, _ := entry(t, p, "req_$delta"); v.Float != 1 {
		t.Fatal(v)
	}
}

func TestNormalCounterNormalization(t *testing.T) {
	clock := &fakeClock{time.Unix(1700000000, 0)}
	cfg := deltaConfig(t, false, "v:v::::4,raw:raw")
	conv := NewConverter(Limits{}, clock.now)
	p := conv.PerfCounterPack(point(t, cfg, "n,host=h v=10,raw=3i 1", 1), cfg)
	if v, _ := entry(t, p, "v"); v.Type != protocol.ValueDouble || v.Double != 10 {
		t.Fatalf("%+v", v)
	}
	if v, _ := entry(t, p, "raw"); v.Type != protocol.ValueDecimal || v.Int != 3 {
		t.Fatalf("%+v", v)
	}
	clock.t = clock.t.Add(2 * time.Second)
	p = conv.PerfCounterPack(point(t, cfg, "n,host=h v=20 2", 2), cfg)
	if v, _ := entry(t, p, "v"); v.Double != 15 {
		t.Fatal(v)
	}
	// normalizeSeconds=4 averages 4 two-second buckets (8s, Q1).
	clock.t = clock.t.Add(8 * time.Second)
	p = conv.PerfCounterPack(point(t, cfg, "n,host=h v=40 3", 3), cfg)
	if v, _ := entry(t, p, "v"); v.Double != 40 {
		t.Fatal(v)
	}
}

func TestDeltaArithmetic(t *testing.T) {
	dec := func(n int64) protocol.Value { return protocol.Value{Type: protocol.ValueDecimal, Int: n} }
	flt := func(f float32) protocol.Value { return protocol.Value{Type: protocol.ValueFloat, Float: f} }
	if got := deltaPerSec(dec(1<<40+1), dec(1), 3); got != float32(float32(1<<40)*1000)/3 {
		t.Fatal(got)
	}
	if got := deltaPerSec(dec(10), flt(2.9), 1000); got != 8 { // (long)2.9f = 2
		t.Fatal(got)
	}
	if got := deltaPerSec(flt(1), dec(3), 1000); got != -2 {
		t.Fatal(got)
	}
	if javaF2L(float32(math.NaN())) != 0 || javaF2L(float32(math.Inf(1))) != math.MaxInt64 || javaF2L(-1e30) != math.MinInt64 {
		t.Fatal("f2l")
	}
}

func TestConverterBounds(t *testing.T) {
	clock := &fakeClock{time.Unix(1700000000, 0)}
	cfg := deltaConfig(t, true, "&&req:req-$id$")
	conv := NewConverter(Limits{PrevValues: 2, Meters: 3}, clock.now)
	for i := 0; i < 5; i++ {
		conv.PerfCounterPack(point(t, cfg, "n,host=h,id="+string(rune('a'+i))+" req=1i 1", 1), cfg)
	}
	s := conv.Stats()
	if s.PrevSize != 2 || s.PrevEvicted != 3 || s.MeterSize != 0 {
		t.Fatalf("%+v", s)
	}
	// The oldest key was evicted: its next sample has no delta.
	if p := conv.PerfCounterPack(point(t, cfg, "n,host=h,id=a req=2i 2", 2), cfg); len(p.Data.Entries) != 1 {
		t.Fatalf("%+v", p.Data.Entries)
	}
	var m fifo[string, int] = newFIFO[string, int](2)
	m.put("a", 1)
	m.put("b", 2)
	m.put("a", 3) // update keeps the insertion position
	if !m.put("c", 4) {
		t.Fatal("no eviction")
	}
	if _, ok := m.get("a"); ok {
		t.Fatal("a must be evicted first")
	}
}

func TestObjectPack(t *testing.T) {
	cfg := sampleConfig(t, time.Unix(0, 0))
	p := point(t, cfg, "cpu,cpu=cpu-total,host=h1 usage_user=1 1", 1)
	o := ObjectPack(p, "10.0.0.9", cfg.ObjectDeadtimeMs)
	if !o.Alive || o.Wakeup != 0 || o.ObjHash != Hash("/h1/X$LINUX_NEW") || o.ObjType != "HOST_NEW" || o.Address != "10.0.0.9" || o.Version != "" || len(o.Tags.Entries) != 1 || o.Tags.Entries[0].Key != "deadTime" || o.Tags.Entries[0].Value.Type != protocol.ValueDecimal || o.Tags.Entries[0].Value.Int != 35000 {
		t.Fatalf("%+v", o)
	}
	if _, err := protocol.EncodePack(o, protocol.Limits{}); err != nil {
		t.Fatal(err)
	}
}
