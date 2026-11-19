// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
package legacyindex

import (
	"encoding/binary"
	"sort"
)

// TimeEntries collects one time bucket in chunks (WalkRangeStep) and yields
// it in the RangeBucket order: ascending (time, value) with Java TreeSet
// deduplication, reversed for reverse scans, filtered to [start, end].
//
// It keeps the bucket compactly — time, value offset and length (16 bytes)
// plus the value bytes — instead of Records with their slices, and applies
// the bucketRecords limits (MaxResultRecords, MaxResultBytes) so a bucket
// that RangeBucket refuses is refused here too. The zero value is ready.
type TimeEntries struct {
	entries []timeEntry
	arena   []byte
	size    int
}
type timeEntry struct {
	ms     int64
	off, n uint32
}

// Reset empties t and keeps its capacity.
func (t *TimeEntries) Reset() {
	t.entries, t.arena, t.size = t.entries[:0], t.arena[:0], 0
}

// Add appends one chain record. Records must come from a time index.
func (t *TimeEntries) Add(r Record) error {
	if len(r.Key) != 8 {
		return corrupt(CodeChain, r.Position, "time index key width")
	}
	if len(t.entries) >= MaxResultRecords || t.size > MaxResultBytes-len(r.Key)-len(r.Value) {
		return ErrLimit
	}
	t.size += len(r.Key) + len(r.Value)
	t.entries = append(t.entries, timeEntry{ms: int64(binary.BigEndian.Uint64(r.Key)), off: uint32(len(t.arena)), n: uint32(len(r.Value))})
	t.arena = append(t.arena, r.Value...)
	return nil
}

func (t *TimeEntries) value(e timeEntry) []byte { return t.arena[e.off : e.off+e.n : e.off+e.n] }
func (t *TimeEntries) compare(a, b timeEntry) int {
	if a.ms != b.ms {
		if a.ms < b.ms {
			return -1
		}
		return 1
	}
	av, bv := t.value(a), t.value(b)
	for n := 0; n < len(av) && n < len(bv); n++ {
		if x, y := int8(av[n]), int8(bv[n]); x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return len(av) - len(bv)
}

// Finish orders the collected bucket for reading with Len/At.
func (t *TimeEntries) Finish(start, end int64, reverse bool) {
	sort.SliceStable(t.entries, func(a, b int) bool { return t.compare(t.entries[a], t.entries[b]) < 0 })
	out := t.entries[:0]
	for _, e := range t.entries {
		if len(out) > 0 && t.compare(out[len(out)-1], e) == 0 {
			continue
		}
		out = append(out, e)
	}
	kept := out[:0]
	for _, e := range out {
		if e.ms >= start && e.ms <= end {
			kept = append(kept, e)
		}
	}
	if reverse {
		for a, b := 0, len(kept)-1; a < b; a, b = a+1, b-1 {
			kept[a], kept[b] = kept[b], kept[a]
		}
	}
	t.entries = kept
}

// Len is the number of ordered entries after Finish.
func (t *TimeEntries) Len() int { return len(t.entries) }

// At returns entry n after Finish. value aliases t until the next Reset.
func (t *TimeEntries) At(n int) (ms int64, value []byte) {
	e := t.entries[n]
	return e.ms, t.value(e)
}
