package telegraf

import (
	"container/list"
	"math"
	"sync"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Limits bound the converter state. Zero values select the defaults.
type Limits struct {
	// PrevValues bounds the delta "previous value" map (Java
	// CounterTimeCache: 1,000,000 rows, 120s keep). Default 100,000.
	PrevValues int
	// PrevKeep is the previous value lifetime; default 120s.
	PrevKeep time.Duration
	// Meters bounds the normalization meters (Java MeterCounterManager
	// LinkedMap max 10,000). Default 10,000.
	Meters int
}

func (l Limits) withDefaults() Limits {
	if l.PrevValues <= 0 {
		l.PrevValues = 100000
	}
	if l.PrevKeep <= 0 {
		l.PrevKeep = 120 * time.Second
	}
	if l.Meters <= 0 {
		l.Meters = 10000
	}
	return l
}

// ConverterStats are cumulative state counters.
type ConverterStats struct {
	PrevEvicted, MeterEvicted uint64
	PrevSize, MeterSize       int
}

// Converter turns Points into packs and owns the delta/normalize state shared
// by all requests (CounterTimeCache and MeterCounterManager). Safe for
// concurrent use.
type Converter struct {
	mu     sync.Mutex
	clock  func() time.Time
	limits Limits
	prev   fifo[stateKey, prevValue]
	meters fifo[stateKey, *meter]
	stats  ConverterStats
}

type stateKey struct {
	objHash int32
	name    string
}

type prevValue struct {
	value   protocol.Value
	time    int64 // line timestamp ms
	expires time.Time
}

// NewConverter builds an empty converter. clock nil means time.Now.
func NewConverter(limits Limits, clock func() time.Time) *Converter {
	if clock == nil {
		clock = time.Now
	}
	l := limits.withDefaults()
	return &Converter{clock: clock, limits: l, prev: newFIFO[stateKey, prevValue](l.PrevValues), meters: newFIFO[stateKey, *meter](l.Meters)}
}

// Stats returns a snapshot.
func (c *Converter) Stats() ConverterStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats
	s.PrevSize, s.MeterSize = c.prev.len(), c.meters.len()
	return s
}

// DeadTimeTag is ObjectPack.TAG_KEY_DEAD_TIME.
const DeadTimeTag = "deadTime"

// ObjectPack is InfluxSingleLine.toObjectPack: objHash/objName/objType, the
// request remote address and tags {deadTime: DecimalValue(deadTimeMs)}. No
// version is set.
func ObjectPack(p *Point, address string, deadTimeMs int) *protocol.ObjectPack {
	// alive keeps the Java field default true (ObjectPack.java:58);
	// toObjectPack does not change it.
	return &protocol.ObjectPack{ObjHash: p.ObjHash, ObjName: p.ObjName, ObjType: p.ObjType, Address: address, Alive: true,
		Tags: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: DeadTimeTag, Value: protocol.Value{Type: protocol.ValueDecimal, Int: int64(deadTimeMs)}}}}}
}

// PerfCounterPack is InfluxSingleLine.toPerfCounterPack. time is the receive
// time, timetype REALTIME(1). For each mapped field, in line order:
//   - normal counter (deltaType != DELTA): NONE with normalizeSec > 0 stores
//     the meter average as DoubleValue; otherwise the parsed value as is.
//   - delta counter (DELTA/BOTH): with a previous value for (objHash, delta
//     name) the per-second rate on line timestamps, normalized by the mapping
//     seconds or, when 0 and DeltaNormalizeDefault, the default seconds, as
//     FloatValue; the current value always becomes the new previous value.
//
// The pack's objHash is not set; ingest derives it from objName.
func (c *Converter) PerfCounterPack(p *Point, cfg *Config) *protocol.PerfCounterPack {
	pack := &protocol.PerfCounterPack{Time: p.ReceivedTime, TimeType: 1, ObjName: p.ObjName, Data: protocol.Value{Type: protocol.ValueMap}}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock()
	for _, f := range p.Fields {
		cp := f.Counter
		if cp.HasNormal() {
			name := cp.TaggedName(p.Tags)
			if cp.NormalizeSec > 0 && !cp.HasDelta() {
				m := c.meter(stateKey{p.ObjHash, name}, now)
				m.add(doubleValue(f.Value), now)
				putEntry(&pack.Data, name, protocol.Value{Type: protocol.ValueDouble, Double: m.avg(cp.NormalizeSec, now)})
			} else {
				putEntry(&pack.Data, name, f.Value)
			}
		}
		if !cp.HasDelta() {
			continue
		}
		name := cp.TaggedDeltaName(p.Tags)
		key := stateKey{p.ObjHash, name}
		if prev, ok := c.prev.get(key); ok && now.Before(prev.expires) {
			rate := deltaPerSec(f.Value, prev.value, p.TimestampMs-prev.time)
			seconds := cp.NormalizeSec
			if seconds <= 0 && cfg.DeltaNormalizeDefault {
				seconds = cfg.DeltaNormalizeDefaultSeconds
			}
			if cp.NormalizeSec > 0 || cfg.DeltaNormalizeDefault {
				m := c.meter(key, now)
				m.add(float64(rate), now)
				rate = float32(m.avg(seconds, now))
			}
			putEntry(&pack.Data, name, protocol.Value{Type: protocol.ValueFloat, Float: rate})
		}
		if c.prev.put(key, prevValue{value: f.Value, time: p.TimestampMs, expires: now.Add(c.limits.PrevKeep)}) {
			c.stats.PrevEvicted++
		}
	}
	return pack
}

func (c *Converter) meter(key stateKey, now time.Time) *meter {
	if m, ok := c.meters.get(key); ok {
		return m
	}
	m := newMeter(now)
	if c.meters.put(key, m) {
		c.stats.MeterEvicted++
	}
	return m
}

// deltaPerSec reproduces the Java float arithmetic: DECIMAL values subtract as
// long, others as float; (delta * 1000f) / (float)elapsedMs. A zero elapsed
// time yields ±Inf or NaN like Java (D5).
func deltaPerSec(cur, prev protocol.Value, elapsedMs int64) float32 {
	var delta float32
	if cur.Type == protocol.ValueDecimal {
		delta = float32(longValue(cur) - longValue(prev))
	} else {
		delta = floatValue(cur) - floatValue(prev)
	}
	return delta * 1000 / float32(elapsedMs)
}

func longValue(v protocol.Value) int64 {
	if v.Type == protocol.ValueDecimal {
		return v.Int
	}
	return javaF2L(v.Float)
}

// javaF2L is Java's (long) float narrowing: NaN is 0, out of range saturates.
func javaF2L(f float32) int64 {
	switch {
	case f != f:
		return 0
	case f >= math.MaxInt64:
		return math.MaxInt64
	case f <= math.MinInt64:
		return math.MinInt64
	}
	return int64(f)
}

func floatValue(v protocol.Value) float32 {
	if v.Type == protocol.ValueDecimal {
		return float32(v.Int)
	}
	return v.Float
}

func doubleValue(v protocol.Value) float64 {
	if v.Type == protocol.ValueDecimal {
		return float64(v.Int)
	}
	return float64(v.Float)
}

// putEntry is MapValue.put: replace in place or append.
func putEntry(m *protocol.Value, key string, v protocol.Value) {
	for i := range m.Entries {
		if m.Entries[i].Key == key {
			m.Entries[i].Value = v
			return
		}
	}
	m.Entries = append(m.Entries, protocol.Entry{Key: key, Value: v})
}

// meter is MeterCounter: MeteringUtil(2000ms, 31 buckets). avg(period)
// averages the last min(period, 30) two-second buckets including the current
// one, so normalizeSeconds N spans 2N seconds like Java (Q1).
type meter struct {
	buckets [meterBuckets]struct {
		sum   float64
		count int
	}
	time int64
	pos  int
}

const (
	meterBuckets = 31
	meterUnitMs  = 2000
)

func newMeter(now time.Time) *meter {
	t := now.UnixMilli() / meterUnitMs
	return &meter{time: t, pos: int(t % meterBuckets)}
}

func (m *meter) position(now time.Time) int {
	cur := now.UnixMilli() / meterUnitMs
	if cur != m.time {
		for i := int64(0); i < cur-m.time && i < meterBuckets; i++ {
			m.pos = (m.pos + 1) % meterBuckets
			m.buckets[m.pos].sum, m.buckets[m.pos].count = 0, 0
		}
		m.time = cur
		m.pos = int(((cur % meterBuckets) + meterBuckets) % meterBuckets)
	}
	return m.pos
}

func (m *meter) add(v float64, now time.Time) {
	b := &m.buckets[m.position(now)]
	b.sum += v
	b.count++
}

func (m *meter) avg(period int, now time.Time) float64 {
	if period >= meterBuckets {
		period = meterBuckets - 1
	}
	pos := m.position(now)
	var sum float64
	count := 0
	for i := 0; i < period; i++ {
		sum += m.buckets[pos].sum
		count += m.buckets[pos].count
		if pos == 0 {
			pos = meterBuckets - 1
		} else {
			pos--
		}
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

// fifo is a bounded map that evicts in insertion order; updating an existing
// key keeps its position (Java LinkedMap/CacheTable put in LAST mode).
type fifo[K comparable, V any] struct {
	max   int
	items map[K]*list.Element
	order *list.List
}

type fifoEntry[K comparable, V any] struct {
	key   K
	value V
}

func newFIFO[K comparable, V any](max int) fifo[K, V] {
	return fifo[K, V]{max: max, items: map[K]*list.Element{}, order: list.New()}
}

func (f *fifo[K, V]) len() int { return len(f.items) }

func (f *fifo[K, V]) get(k K) (V, bool) {
	if e, ok := f.items[k]; ok {
		return e.Value.(*fifoEntry[K, V]).value, true
	}
	var zero V
	return zero, false
}

// put stores the value and reports whether an entry was evicted.
func (f *fifo[K, V]) put(k K, v V) bool {
	if e, ok := f.items[k]; ok {
		e.Value.(*fifoEntry[K, V]).value = v
		return false
	}
	evicted := false
	for len(f.items) >= f.max {
		first := f.order.Front()
		delete(f.items, first.Value.(*fifoEntry[K, V]).key)
		f.order.Remove(first)
		evicted = true
	}
	f.items[k] = f.order.PushBack(&fifoEntry[K, V]{key: k, value: v})
	return evicted
}
