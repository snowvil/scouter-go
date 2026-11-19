package ingest

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

func benchEngine(b *testing.B, configure func(*Config)) *Engine {
	b.Helper()
	root := b.TempDir()
	s, err := storage.Open(storage.Config{Directory: root, Location: time.UTC})
	if err != nil {
		b.Fatal(err)
	}
	c, err := storage.OpenCounters(root, time.UTC)
	if err != nil {
		b.Fatal(err)
	}
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	cfg := Config{Location: time.UTC, Clock: func() time.Time { return now }, XLogCapacity: 16, MaxCacheBytes: 1 << 30}
	if configure != nil {
		configure(&cfg)
	}
	e, err := New(cfg, s, c)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { e.Close(); c.Close(); s.Close() })
	return e
}

// liveHeap is the live heap object count and bytes after a full GC.
func liveHeap() (objects, bytes uint64) {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapObjects, m.HeapAlloc
}

// BenchmarkAddProcessed is the steady state of the processed gxid set at its
// default bound: every add evicts the oldest. heap-objects and heap-B are
// the live heap the engine with the full set adds (what the GC marks).
func BenchmarkAddProcessed(b *testing.B) {
	objects, bytes := liveHeap()
	e := benchEngine(b, nil)
	for i := range int64(defaultGxidKeepCount) {
		e.addProcessed(i+1, discardNone)
	}
	afterObjects, afterBytes := liveHeap()
	objects, bytes = afterObjects-objects, afterBytes-bytes
	b.ReportAllocs()
	txid := int64(defaultGxidKeepCount + 1)
	for b.Loop() {
		e.addProcessed(txid, discardNone)
		txid++
	}
	b.ReportMetric(float64(objects), "heap-objects")
	b.ReportMetric(float64(bytes), "heap-B")
}

// BenchmarkCacheCounter is the realtime counter cache update of the replay
// generator's counter pack (5 values and the objHash/time metadata) over
// 4000 objects whose keys are already cached.
func BenchmarkCacheCounter(b *testing.B) {
	e := benchEngine(b, nil)
	const objects = 4000
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC).UnixMilli()
	packs := make([]*protocol.PerfCounterPack, objects)
	hashes := make([]int32, objects)
	for i := range packs {
		f := func(v float64) protocol.Value { return protocol.Value{Type: protocol.ValueFloat, Float: float32(v)} }
		d := func(v int64) protocol.Value { return protocol.Value{Type: protocol.ValueDecimal, Int: v} }
		name := "/host" + strconv.Itoa(i) + "/tomcat"
		p := &protocol.PerfCounterPack{ObjName: name, Time: now, TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{
			{Key: "TPS", Value: f(12.5)}, {Key: "ElapsedTime", Value: d(int64(i % 500))}, {Key: "ActiveService", Value: d(3)},
			{Key: "ErrorRate", Value: f(0.25)}, {Key: "HeapUsed", Value: f(300)},
		}}}
		hashes[i] = storage.CounterHash(name)
		setCounterMetadata(&p.Data, "objHash", int64(hashes[i]))
		setCounterMetadata(&p.Data, "time", now)
		packs[i] = p
		if err := e.cacheCounterLocked(p, hashes[i], now); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if err := e.cacheCounterLocked(packs[i], hashes[i], now); err != nil {
			b.Fatal(err)
		}
		if i++; i == objects {
			i = 0
		}
	}
}
