// Copyright 2015 the original author or authors (Scouter).
// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
package legacyindex

import (
	"encoding/binary"
	"errors"
	"sort"
)

func compare(a, b Record) int {
	at, bt := int64(binary.BigEndian.Uint64(a.Key)), int64(binary.BigEndian.Uint64(b.Key))
	if at < bt {
		return -1
	}
	if at > bt {
		return 1
	}
	for n := 0; n < len(a.Value) && n < len(b.Value); n++ {
		av, bv := int8(a.Value[n]), int8(b.Value[n])
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
	}
	return len(a.Value) - len(b.Value)
}
func (i *Index) bucketRecords(bucket int) ([]Record, error) {
	var out []Record
	size := 0
	err := i.walkChain(bucket, func(m metadata) error {
		if len(out) >= MaxResultRecords || size > MaxResultBytes-len(m.Key)-m.valueBytes {
			return ErrLimit
		}
		r, err := i.record(m)
		if err != nil {
			return err
		}
		return collect(&out, &size, r)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ScanBucket is the Java V2 order: reverse=false visits newest appended first
// without deduplication or a bucket-sized buffer. reverse=true visits oldest first.
func (i *Index) ScanBucket(ms int64, reverse bool, visit func(Record) error) error {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if err := i.ready(); err != nil {
		return err
	}
	if !i.timeIndex || ms <= 0 || visit == nil {
		return errors.New("invalid time bucket/visitor")
	}
	if !reverse {
		return i.walkChain(i.timeBucket(ms), func(m metadata) error {
			r, err := i.record(m)
			if err != nil {
				return err
			}
			return visit(r)
		})
	}
	records, err := i.bucketRecords(i.timeBucket(ms))
	if err != nil {
		return err
	}
	for n := len(records) - 1; n >= 0; n-- {
		if err = visit(records[n]); err != nil {
			return err
		}
	}
	return nil
}

// WalkBucketFrom continues ScanBucket(ms, false) (newest appended first) after
// a pause: resume is the Record.PreviousPosition of the last record visited
// and after its Record.Position. at == 0 starts at the bucket head. Records
// appended after the walk began are not visited (they precede at in the
// chain), like a Java reader that already passed the head. To walk again
// from a visited record r itself, pass at = r.NextPosition (its end bounds
// the link to it as the newer record's position does) and resume =
// r.Position.
func (i *Index) WalkBucketFrom(ms, at, resume int64, visit func(Record) error) error {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if err := i.ready(); err != nil {
		return err
	}
	if !i.timeIndex || ms <= 0 || visit == nil {
		return errors.New("invalid time bucket/visitor")
	}
	return i.walkFrom(i.timeBucket(ms), at, resume, visit)
}

// WalkRangeStep walks the chain of the bucket ScanRange visits at step (see
// RangeSteps), newest appended first, resuming like WalkBucketFrom. It lets a
// caller collect one bucket in chunks (TimeEntries) and release its locks in
// between, instead of reading the bucket whole as RangeBucket does.
func (i *Index) WalkRangeStep(start, end int64, reverse bool, step int, at, resume int64, visit func(Record) error) error {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if err := i.ready(); err != nil {
		return err
	}
	count, err := i.rangeSteps(start, end)
	if err != nil {
		return err
	}
	if visit == nil || step < 0 || step >= count {
		return errors.New("invalid time range step/visitor")
	}
	return i.walkFrom(i.rangeStepBucket(start, end, reverse, step), at, resume, visit)
}
func (i *Index) walkFrom(bucket int, at, resume int64, visit func(Record) error) error {
	if at < 0 || resume < 0 {
		return errors.New("invalid resume position")
	}
	pos, next := i.head(bucket), i.length
	if at != 0 {
		if at > i.length {
			return corrupt(CodeChain, at, "resume position beyond index")
		}
		pos, next = resume, at
	}
	_, err := i.walkChainFrom(bucket, pos, next, func(m metadata) error {
		r, err := i.record(m)
		if err != nil {
			return err
		}
		return visit(r)
	})
	return err
}

// RangeSteps is the number of buckets ScanRange visits for [start, end]; the
// steps are passed to RangeBucket.
func (i *Index) RangeSteps(start, end int64) (int, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if err := i.ready(); err != nil {
		return 0, err
	}
	return i.rangeSteps(start, end)
}
func (i *Index) rangeSteps(start, end int64) (int, error) {
	if !i.timeIndex || start <= 0 || end < start {
		return 0, errors.New("invalid time range/visitor")
	}
	if end-start > dayMillis {
		return 0, ErrLimit
	}
	// Times preceding Java's base all hash to bucket zero; visit it only once.
	count := 1
	if end >= i.base {
		alignedStart := start
		if alignedStart < i.base {
			alignedStart = i.base
		}
		count = int((end-i.base)/500-(alignedStart-i.base)/500) + 1
		if count > 172800 {
			count = 172800
		}
	}
	return count, nil
}

// RangeBucket returns the records ScanRange visits at step (0 <= step <
// RangeSteps), in visit order. The slice and its records are owned by the
// caller, so a scan can release its locks between steps.
func (i *Index) RangeBucket(start, end int64, reverse bool, step int) ([]Record, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if err := i.ready(); err != nil {
		return nil, err
	}
	count, err := i.rangeSteps(start, end)
	if err != nil {
		return nil, err
	}
	if step < 0 || step >= count {
		return nil, errors.New("invalid time range step")
	}
	return i.rangeBucket(start, end, reverse, step)
}
func (i *Index) rangeStepBucket(start, end int64, reverse bool, step int) int {
	if reverse {
		return (i.timeBucket(end) - step + 172800) % 172800
	}
	return (i.timeBucket(start) + step) % 172800
}
func (i *Index) rangeBucket(start, end int64, reverse bool, step int) ([]Record, error) {
	records, err := i.bucketRecords(i.rangeStepBucket(start, end, reverse, step))
	if err != nil {
		return nil, err
	}
	sort.SliceStable(records, func(a, b int) bool { return compare(records[a], records[b]) < 0 })
	// Finish deduplication before handing owned slices to a callback.
	unique := records[:0]
	for _, r := range records {
		if len(unique) == 0 || compare(unique[len(unique)-1], r) != 0 {
			unique = append(unique, r)
		}
	}
	// Forward filters in place (writes never pass reads); reverse needs its
	// own slice.
	out := unique[:0]
	if reverse {
		out = make([]Record, 0, len(unique))
	}
	for step := 0; step < len(unique); step++ {
		j := step
		if reverse {
			j = len(unique) - 1 - step
		}
		ms := int64(binary.BigEndian.Uint64(unique[j].Key))
		if ms >= start && ms <= end {
			out = append(out, unique[j])
		}
	}
	return out, nil
}

// ScanRange implements filtered Java V1 order and TreeSet duplicate semantics.
// It visits every bucket intersecting the inclusive endpoints, correcting the
// original unaligned-endpoint traversal gap. Maximum interval is one fixed day.
func (i *Index) ScanRange(start, end int64, reverse bool, visit func(Record) error) error {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if err := i.ready(); err != nil {
		return err
	}
	if visit == nil {
		return errors.New("invalid time range/visitor")
	}
	count, err := i.rangeSteps(start, end)
	if err != nil {
		return err
	}
	for n := 0; n < count; n++ {
		records, err := i.rangeBucket(start, end, reverse, n)
		if err != nil {
			return err
		}
		for _, r := range records {
			if err = visit(r); err != nil {
				return err
			}
		}
	}
	return nil
}
func (i *Index) Range(start, end int64, reverse bool) ([]Record, error) {
	var out []Record
	size := 0
	err := i.ScanRange(start, end, reverse, func(r Record) error { return collect(&out, &size, r) })
	if err != nil {
		return nil, err
	}
	return out, nil
}
