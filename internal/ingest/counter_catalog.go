package ingest

import (
	"context"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"sort"
	"unicode/utf16"
)

func (e *Engine) expireCounter(key counterKey) {
	c, exists := e.current[key]
	if !exists {
		return
	}
	delete(e.current, key)
	e.cacheBytes -= len(c.data) + len(key.name)
	for i, k := range e.counterOrder {
		if k == key {
			e.counterOrder = append(e.counterOrder[:i], e.counterOrder[i+1:]...)
			break
		}
	}
}
func javaStringHash(s string) uint32 {
	var h uint32
	for _, u := range utf16.Encode([]rune(s)) {
		h = h*31 + uint32(u)
	}
	return h
}
func (e *Engine) CounterNames(ctx context.Context, hash int32) ([]string, error) {
	if err := e.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer e.mu.Unlock()
	now := e.config.Clock().UnixMilli()
	if err := e.expireCounters(ctx, now); err != nil {
		return nil, err
	}
	names := []string{}
	for _, key := range e.counterOrder {
		if key.hash == hash && key.kind == 1 {
			names = append(names, key.name)
		}
	}
	// java.util.HashMap traverses ascending buckets and retains collision-chain
	// insertion order. Capacity doubles after the 0.75 threshold.
	capacity := 16
	for len(names) > capacity*3/4 {
		capacity *= 2
	}
	sort.SliceStable(names, func(i, j int) bool {
		a, b := javaStringHash(names[i]), javaStringHash(names[j])
		a ^= a >> 16
		b ^= b >> 16
		return a&uint32(capacity-1) < b&uint32(capacity-1)
	})
	return names, nil
}
func (e *Engine) CounterNamesForDate(ctx context.Context, date string, hash int32) ([]string, error) {
	return e.counters.DailyCounterNamesContext(ctx, date, hash)
}
func (e *Engine) CounterType(ctx context.Context, typ string) (service.CounterTypeMetadata, error) {
	if err := ctx.Err(); err != nil {
		return service.CounterTypeMetadata{}, err
	}
	v, exists := e.config.CounterTypes[typ]
	if !exists {
		return service.CounterTypeMetadata{DisplayName: "<unknown>"}, nil
	}
	v.Counters = append([]string(nil), v.Counters...)
	return v, nil
}
func (e *Engine) CounterRows(ctx context.Context, date string, hash int32, from, to int64) ([]service.CounterRow, error) {
	v, err := e.counters.CounterRowsContext(ctx, date, hash, from, to)
	out := make([]service.CounterRow, len(v))
	for i, row := range v {
		out[i] = service.CounterRow{Time: row.Time, Values: row.Values}
	}
	return out, err
}
func (e *Engine) CounterBulkRows(ctx context.Context, date string, from, to int64) ([]service.CounterRow, error) {
	v, err := e.counters.CounterBulkRowsContext(ctx, date, from, to)
	out := make([]service.CounterRow, len(v))
	for i, row := range v {
		out[i] = service.CounterRow{Time: row.Time, Values: row.Values}
	}
	return out, err
}

// CounterBulkRowsFor is CounterBulkRows limited to the rows of hashes.
func (e *Engine) CounterBulkRowsFor(ctx context.Context, date string, from, to int64, hashes map[int32]bool) ([]service.CounterRow, error) {
	v, err := e.counters.CounterBulkRowsForContext(ctx, date, from, to, hashes)
	out := make([]service.CounterRow, len(v))
	for i, row := range v {
		out[i] = service.CounterRow{Time: row.Time, Values: row.Values}
	}
	return out, err
}

func numericSample(v protocol.Value) bool {
	switch v.Type {
	case protocol.ValueDecimal, protocol.ValueBoolean, protocol.ValueFloat, protocol.ValueDouble:
		return true
	}
	return false
}
func (e *Engine) pruneCounterNames() {
	live := map[int32]bool{}
	for key := range e.current {
		live[key.hash] = true
	}
	for key := range e.samples {
		live[key.hash] = true
	}
	for hash, name := range e.counterNames {
		if !live[hash] {
			e.cacheBytes -= len(name)
			delete(e.counterNames, hash)
		}
	}
}

// NetDataProcessor sets these before PerfCountCore dispatches every time type.
func setCounterMetadata(v *protocol.Value, key string, n int64) {
	item := protocol.Value{Type: protocol.ValueDecimal, Int: n}
	for i := range v.Entries {
		if v.Entries[i].Key == key {
			v.Entries[i].Value = item
			return
		}
	}
	v.Entries = append(v.Entries, protocol.Entry{Key: key, Value: item})
}

func (e *Engine) expireCounters(ctx context.Context, now int64) error {
	kept := e.counterOrder[:0]
	var canceled error
	for _, key := range e.counterOrder {
		if canceled == nil {
			canceled = ctx.Err()
		}
		if c, exists := e.current[key]; canceled == nil && c.expiry < now {
			if exists {
				delete(e.current, key)
				e.cacheBytes -= len(c.data) + len(key.name)
			}
		} else {
			kept = append(kept, key)
		}
	}
	e.counterOrder = kept
	return canceled
}
