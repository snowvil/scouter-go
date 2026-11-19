package ingest

import (
	"bytes"
	"context"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

// mapCounterCache is the counter cache before cachedCounter: five maps with
// the sizes and expiry kept apart and EncodeValue per entry, kept as the
// oracle of cacheCounterLocked, expireCounter(s) and the takeSamples clear.
type mapCounterCache struct {
	maxCounters, maxObjects, maxBytes int
	current, samples                  map[counterKey][]byte
	counterNames                      map[int32]string
	counterSizes, sampleSizes         map[counterKey]int
	counterExpiry                     map[counterKey]int64
	counterOrder                      []counterKey
	cacheBytes                        int
	skipped                           int
}

func newMapCounterCache(maxCounters, maxObjects, maxBytes int) *mapCounterCache {
	return &mapCounterCache{maxCounters: maxCounters, maxObjects: maxObjects, maxBytes: maxBytes,
		current: map[counterKey][]byte{}, samples: map[counterKey][]byte{}, counterNames: map[int32]string{},
		counterSizes: map[counterKey]int{}, sampleSizes: map[counterKey]int{}, counterExpiry: map[counterKey]int64{}}
}

func (m *mapCounterCache) cache(v *protocol.PerfCounterPack, hash int32, now int64) error {
	newKeys, newSamples := 0, 0
	for _, x := range v.Data.Entries {
		key := counterKey{hash, x.Key, 1}
		if _, ok := m.current[key]; !ok {
			newKeys++
		}
		if numericSample(x.Value) {
			if _, ok := m.samples[key]; !ok {
				newSamples++
			}
		}
	}
	limited := len(m.samples)+newSamples > m.maxCounters || len(m.current)+newKeys > m.maxCounters
	if len(v.Data.Entries) > 0 {
		if _, exists := m.counterNames[hash]; !exists && len(m.counterNames) >= m.maxObjects {
			limited = true
		}
	}
	if limited {
		m.skipped++
		return nil
	}
	delta := 0
	encoded := make([][]byte, len(v.Data.Entries))
	for i, x := range v.Data.Entries {
		b, err := protocol.EncodeValue(x.Value, protocol.Limits{})
		if err != nil {
			return err
		}
		encoded[i] = append(make([]byte, 0, len(b)), b...)
		key := counterKey{hash, x.Key, 1}
		size := len(b) + len(x.Key)
		delta += size - m.counterSizes[key]
		if numericSample(x.Value) {
			delta += size + 16 - m.sampleSizes[key]
		}
	}
	if len(v.Data.Entries) > 0 {
		delta += len(v.ObjName) - len(m.counterNames[hash])
	}
	if delta > m.maxBytes-m.cacheBytes {
		m.skipped++
		return nil
	}
	if len(v.Data.Entries) > 0 {
		m.counterNames[hash] = v.ObjName
	}
	for i, x := range v.Data.Entries {
		key := counterKey{hash, x.Key, 1}
		if _, exists := m.current[key]; !exists {
			m.counterOrder = append(m.counterOrder, key)
		}
		b := encoded[i]
		m.counterExpiry[key] = now + 10000
		m.current[key] = b
		m.counterSizes[key] = len(b) + len(x.Key)
		if numericSample(x.Value) {
			m.samples[key] = b
			m.sampleSizes[key] = len(b) + len(x.Key) + 16
		}
	}
	m.cacheBytes += delta
	return nil
}

func (m *mapCounterCache) expireCounter(key counterKey) {
	if _, exists := m.current[key]; !exists {
		return
	}
	delete(m.current, key)
	delete(m.counterExpiry, key)
	m.cacheBytes -= m.counterSizes[key]
	delete(m.counterSizes, key)
	for i, k := range m.counterOrder {
		if k == key {
			m.counterOrder = append(m.counterOrder[:i], m.counterOrder[i+1:]...)
			break
		}
	}
}

func (m *mapCounterCache) expireCounters(now int64) {
	kept := m.counterOrder[:0]
	for _, key := range m.counterOrder {
		if m.counterExpiry[key] < now {
			delete(m.current, key)
			delete(m.counterExpiry, key)
			m.cacheBytes -= m.counterSizes[key]
			delete(m.counterSizes, key)
		} else {
			kept = append(kept, key)
		}
	}
	m.counterOrder = kept
}

func (m *mapCounterCache) takeSamples() map[counterKey][]byte {
	taken := map[counterKey][]byte{}
	for key, b := range m.samples {
		if _, exists := m.counterNames[key.hash]; exists {
			taken[key] = b
		}
	}
	for _, size := range m.sampleSizes {
		m.cacheBytes -= size
	}
	clear(m.sampleSizes)
	clear(m.samples)
	live := map[int32]bool{}
	for key := range m.current {
		live[key.hash] = true
	}
	for key := range m.samples {
		live[key.hash] = true
	}
	for hash, name := range m.counterNames {
		if !live[hash] {
			m.cacheBytes -= len(name)
			delete(m.counterNames, hash)
		}
	}
	return taken
}

func randomCounterValue(r *rand.Rand) protocol.Value {
	switch r.IntN(11) {
	case 7:
		// Values larger than the encoder's last buffer: it regrows (and
		// past counterEncKeep is replaced) while earlier copies stay put.
		return protocol.Value{Type: protocol.ValueText, Text: string(bytes.Repeat([]byte{'a' + byte(r.IntN(26))}, r.IntN(3000)))}
	case 8:
		l := protocol.Value{Type: protocol.ValueList}
		for range r.IntN(300) {
			l.Values = append(l.Values, protocol.Value{Type: protocol.ValueDecimal, Int: r.Int64N(1 << 50)})
		}
		return l
	case 9:
		m := protocol.Value{Type: protocol.ValueMap}
		for i := range r.IntN(20) {
			m.Entries = append(m.Entries, protocol.Entry{Key: "k" + strconv.Itoa(i), Value: protocol.Value{Type: protocol.ValueText, Text: strconv.Itoa(r.IntN(1 << 30))}})
		}
		return m
	case 10:
		if r.IntN(20) == 0 {
			return protocol.Value{Type: protocol.ValueText, Text: string(bytes.Repeat([]byte{'z'}, counterEncKeep+r.IntN(1000)))}
		}
		return protocol.Value{Type: protocol.ValueBlob, Bytes: bytes.Repeat([]byte{byte(r.IntN(256))}, r.IntN(500))}
	case 0:
		return protocol.Value{Type: protocol.ValueDecimal, Int: r.Int64N(1 << 40)}
	case 1:
		return protocol.Value{Type: protocol.ValueFloat, Float: r.Float32()}
	case 2:
		return protocol.Value{Type: protocol.ValueDouble, Double: r.Float64()}
	case 3:
		return protocol.Value{Type: protocol.ValueBoolean, Bool: r.IntN(2) == 0}
	case 4:
		return protocol.Value{Type: protocol.ValueText, Text: strconv.Itoa(r.IntN(1 << 20))}
	case 5:
		return protocol.Value{Type: protocol.ValueNull}
	}
	return protocol.Value{Type: protocol.ValueDecimal, Int: int64(r.IntN(3))}
}

// cacheCounterLocked, the expiry paths and the 5m sample clear leave the
// state, the accounted bytes and the skip count of the five-map cache: same
// keys, bytes, expiry and order, with the count, object and byte bounds hit,
// values of every sample/non-sample kind, keys dropped from a pack (so they
// expire) and lookups (Counter, Objects) expiring single keys.
func TestCounterCacheMatchesMapOracle(t *testing.T) {
	for _, bound := range []struct{ counters, objects, bytes int }{{1 << 20, 1 << 20, 1 << 30}, {40, 1 << 20, 1 << 30}, {1 << 20, 3, 1 << 30}, {1 << 20, 1 << 20, 900}, {25, 4, 1500}, {1 << 20, 1 << 20, 200000}} {
		now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
		e, _ := testEngineWith(t, &now, func(c *Config) { c.MaxCounters = bound.counters; c.MaxObjects = bound.objects })
		e.mu.Lock()
		base := e.cacheBytes + e.ringBytes
		e.config.MaxCacheBytes = base + bound.bytes
		e.mu.Unlock()
		oracle := newMapCounterCache(bound.counters, bound.objects, bound.bytes)
		r := rand.New(rand.NewPCG(uint64(bound.counters), uint64(bound.bytes)))
		names := []string{"TPS", "ElapsedTime", "ActiveService", "ErrorRate", "HeapUsed", "GcCount", "Cpu", "objHash", "time"}
		ctx := context.Background()
		for step := 0; step < 4000; step++ {
			now = now.Add(time.Duration(r.IntN(900)) * time.Millisecond)
			ms := now.UnixMilli()
			obj := r.IntN(8)
			objName := "/host" + strconv.Itoa(obj) + "/tomcat"
			hash := storage.CounterHash(objName)
			p := &protocol.PerfCounterPack{ObjName: objName, Time: ms, TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap}}
			for _, i := range r.Perm(len(names))[:r.IntN(len(names)+1)] {
				p.Data.Entries = append(p.Data.Entries, protocol.Entry{Key: names[i], Value: randomCounterValue(r)})
			}
			// Under the engine lock with a deferred unlock: a t.Fatal must not
			// leave it held for the cleanup's Close.
			func() {
				e.mu.Lock()
				defer e.mu.Unlock()
				counterCacheStep(t, e, oracle, r.IntN(20), p, hash, ms, counterKey{hash, names[r.IntN(len(names))], 1})
			}()
			if step%500 == 499 {
				_, taken, err := e.takeSamples(ctx)
				if err != nil {
					t.Fatal(err)
				}
				want := oracle.takeSamples()
				got := map[counterKey][]byte{}
				for _, s := range taken {
					got[counterKey{storage.CounterHash(s.objName), s.name, 1}] = s.value
				}
				if !maps.EqualFunc(got, want, bytes.Equal) {
					t.Fatalf("bound %+v step %d: samples taken %d want %d", bound, step, len(got), len(want))
				}
			}
			func() {
				e.mu.Lock()
				defer e.mu.Unlock()
				compareCounterCache(t, e, oracle, base, bound, step)
			}()
		}
		if oracle.skipped == 0 && bound.counters < 1<<20 {
			t.Fatalf("bound %+v never reached", bound)
		}
	}
}

func counterCacheStep(t *testing.T, e *Engine, oracle *mapCounterCache, op int, p *protocol.PerfCounterPack, hash int32, ms int64, key counterKey) {
	t.Helper()
	switch {
	case op < 15:
		if err := e.cacheCounterLocked(p, hash, ms); err != nil {
			t.Fatal(err)
		}
		if err := oracle.cache(p, hash, ms); err != nil {
			t.Fatal(err)
		}
	case op < 18:
		if err := e.expireCounters(context.Background(), ms); err != nil {
			t.Fatal(err)
		}
		oracle.expireCounters(ms)
	default:
		// Counter/Objects: one key expired when its time passed.
		if e.current[key].expiry < ms {
			e.expireCounter(key)
		}
		if oracle.counterExpiry[key] < ms {
			oracle.expireCounter(key)
		}
	}
}

func compareCounterCache(t *testing.T, e *Engine, m *mapCounterCache, base int, bound any, step int) {
	t.Helper()
	if e.cacheBytes+e.ringBytes-base != m.cacheBytes || int(e.counterCacheSkipped.Load()) != m.skipped {
		t.Fatalf("bound %+v step %d: bytes %d want %d, skipped %d want %d", bound, step, e.cacheBytes+e.ringBytes-base, m.cacheBytes, e.counterCacheSkipped.Load(), m.skipped)
	}
	if !slices.Equal(e.counterOrder, m.counterOrder) || !maps.Equal(e.counterNames, m.counterNames) || !maps.EqualFunc(e.samples, m.samples, bytes.Equal) {
		t.Fatalf("bound %+v step %d: order %d/%d names %d/%d samples %d/%d", bound, step, len(e.counterOrder), len(m.counterOrder), len(e.counterNames), len(m.counterNames), len(e.samples), len(m.samples))
	}
	if len(e.current) != len(m.current) {
		t.Fatalf("bound %+v step %d: current %d want %d", bound, step, len(e.current), len(m.current))
	}
	for key, c := range e.current {
		if !bytes.Equal(c.data, m.current[key]) || c.expiry != m.counterExpiry[key] || cap(c.data) != len(c.data) {
			t.Fatalf("bound %+v step %d: %v = %x/%d want %x/%d", bound, step, key, c.data, c.expiry, m.current[key], m.counterExpiry[key])
		}
	}
}

// The cached values are copies, not views of the reused encoder buffer:
// samples taken and decoded outside the engine lock (as the Sample5M writer
// pass does), Counter reads and the Objects counter tag keep their bytes
// while counter packs are accepted concurrently. Each key has one fixed value, so a view of the
// encoder buffer would decode to another key's value; under -race the
// overwrite is also reported.
func TestCounterCacheValuesOwned(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.MasterCounters = map[string]string{"tomcat": "K3"} })
	ctx := context.Background()
	const objects, keys = 4, 12
	for obj := range objects {
		name := "/host" + strconv.Itoa(obj) + "/tomcat"
		if err := e.Accept(ctx, &protocol.ObjectPack{ObjType: "tomcat", ObjHash: storage.CounterHash(name), ObjName: name, Alive: true, Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
			t.Fatal(err)
		}
	}
	pack := func(obj int) *protocol.PerfCounterPack {
		p := &protocol.PerfCounterPack{ObjName: "/host" + strconv.Itoa(obj) + "/tomcat", Time: now.UnixMilli(), TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap}}
		for k := range keys {
			p.Data.Entries = append(p.Data.Entries, protocol.Entry{Key: "K" + strconv.Itoa(k), Value: protocol.Value{Type: protocol.ValueDecimal, Int: int64(k*1000 + obj)}})
		}
		return p
	}
	want := map[counterKey]int64{}
	for obj := range objects {
		for k := range keys {
			want[counterKey{storage.CounterHash("/host" + strconv.Itoa(obj) + "/tomcat"), "K" + strconv.Itoa(k), 1}] = int64(k*1000 + obj)
		}
	}
	done := make(chan error, 1)
	go func() {
		for i := range 3000 {
			if err := e.Accept(ctx, pack(i%objects)); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	check := func(key counterKey, b []byte) {
		t.Helper()
		if _, fixed := want[key]; !fixed {
			return // objHash/time metadata set by Accept
		}
		v, err := protocol.DecodeValue(b, protocol.Limits{})
		if err != nil || v.Type != protocol.ValueDecimal || v.Int != want[key] {
			t.Fatalf("%v decoded %+v %v, want %d", key, v, err, want[key])
		}
	}
	var held [][]sample5M
	tags := 0
	for running := true; running; {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			running = false
		default:
		}
		_, taken, err := e.takeSamples(ctx)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, taken)
		for _, s := range taken {
			check(counterKey{storage.CounterHash(s.objName), s.name, 1}, s.value)
		}
		v, ok, err := e.Counter(ctx, storage.CounterHash("/host1/tomcat"), "K3", 1)
		if err != nil || (ok && v.Int != 3001) {
			t.Fatal("Counter", v, ok, err)
		}
		list, err := e.Objects(ctx, "")
		if err != nil || len(list) != objects {
			t.Fatal("Objects", len(list), err)
		}
		for _, o := range list {
			for _, tag := range o.Tags.Entries {
				if tag.Key == "counter" {
					tags++
					check(counterKey{o.ObjHash, "K3", 1}, mustEncode(t, tag.Value))
				}
			}
		}
	}
	if tags == 0 {
		t.Fatal("no Objects counter tag seen")
	}
	// Taken samples stay intact after all the later encodes.
	for _, taken := range held {
		for _, s := range taken {
			check(counterKey{storage.CounterHash(s.objName), s.name, 1}, s.value)
		}
	}
	if err := e.Sample5M(ctx); err != nil {
		t.Fatal(err)
	}
	flush(t, e)
	if st := e.Stats(); st.Sample5MDropped != 0 || st.WriteErrors != 0 {
		t.Fatal("5m pass", st.Sample5MDropped, st.WriteErrors)
	}
}

// An entry that does not encode fails the pack before any state changes:
// cache, sample, order, names, bytes and skip count stay, the scratch holds
// nothing, and the next pack is cached.
func TestCounterCacheEncodeErrorLeavesState(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, nil)
	ms := now.UnixMilli()
	hash := storage.CounterHash("/a")
	good := func(v int64) *protocol.PerfCounterPack {
		return &protocol.PerfCounterPack{ObjName: "/a", Time: ms, TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{
			{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: v}}, {Key: "Name", Value: protocol.Value{Type: protocol.ValueText, Text: "x"}}}}}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.cacheCounterLocked(good(1), hash, ms); err != nil {
		t.Fatal(err)
	}
	current, samples, order, names, cached := maps.Clone(e.current), maps.Clone(e.samples), slices.Clone(e.counterOrder), maps.Clone(e.counterNames), e.cacheBytes
	bad := good(2)
	bad.Data.Entries = slices.Insert(bad.Data.Entries, 1, protocol.Entry{Key: "New", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 3}},
		protocol.Entry{Key: "Bad", Value: protocol.Value{Type: protocol.ValueIP4, Bytes: []byte{1, 2, 3}}})
	if err := e.cacheCounterLocked(bad, hash, ms+1); err == nil {
		t.Fatal("bad value cached")
	}
	same := func(a, b cachedCounter) bool { return bytes.Equal(a.data, b.data) && a.expiry == b.expiry }
	if !maps.EqualFunc(e.current, current, same) || !maps.EqualFunc(e.samples, samples, bytes.Equal) || !slices.Equal(e.counterOrder, order) ||
		!maps.Equal(e.counterNames, names) || e.cacheBytes != cached || e.counterCacheSkipped.Load() != 0 {
		t.Fatal("state changed by a failed pack")
	}
	if len(e.counterUpdates) != 0 || slices.ContainsFunc(e.counterUpdates[:cap(e.counterUpdates)], func(u counterUpdate) bool { return u.data != nil }) {
		t.Fatal("scratch keeps a value")
	}
	if err := e.cacheCounterLocked(good(5), hash, ms+2); err != nil {
		t.Fatal(err)
	}
	if c := e.current[counterKey{hash, "TPS", 1}]; c.expiry != ms+2+10000 || !bytes.Equal(c.data, mustEncode(t, protocol.Value{Type: protocol.ValueDecimal, Int: 5})) {
		t.Fatalf("next pack %x %d", c.data, c.expiry)
	}
}

func mustEncode(t *testing.T, v protocol.Value) []byte {
	t.Helper()
	b, err := protocol.EncodeValue(v, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
