// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
package legacyindex

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

// openMode selects how an existing pair is validated.
type openMode int

const (
	modeWrite    openMode = iota // validate, then apply a proven recovery
	modeReadOnly                 // validate only; never repairs (pre-recovery semantics)
	modeAnalyze                  // read-only dry run of modeWrite's recovery
)

// RecoveryInfo reports what a writable Open repaired, or what it would repair
// (AnalyzeRecovery). Zero means the files were already consistent.
//
// Java writers append a key record immediately but persist bucket heads only
// on the 4 s FlushCtr/close (Go: Sync/Close/eviction). After a crash the key
// file holds records newer than the saved head of their bucket. Java never
// rebuilds them (they stay unreachable); Go relinks them when every such
// record continues its bucket chain exactly, and truncates an unreferenced
// torn final record. Both are intentional, reported differences.
type RecoveryInfo struct {
	RelinkedRecords    int64 `json:"relinked_records"`     // records newer than their saved bucket head
	RelinkedBuckets    int64 `json:"relinked_buckets"`     // bucket heads moved forward
	NewBuckets         int64 `json:"new_buckets"`          // relinked buckets whose saved head was 0
	TruncatedTailBytes int64 `json:"truncated_tail_bytes"` // torn final record removed (or to remove)
	TruncatedAt        int64 `json:"truncated_at,omitempty"`
	// RelinkRefused is set when records newer than their saved head do not
	// form one exact continuation per bucket (e.g. Java restarted from a
	// stale header and crashed again before its flush). Nothing is relinked;
	// the index opens with the original Java semantics and Unreachable
	// records stay unreachable.
	RelinkRefused string `json:"relink_refused,omitempty"`
	Unreachable   int64  `json:"unreachable_records,omitempty"`
	// Tombstoned counts refused orphan records marked deleted (or to be marked
	// by a writable open) so that a later open never relinks them after the
	// data file has grown over the offsets they reference.
	Tombstoned  int64 `json:"tombstoned_records,omitempty"`
	CountBefore int32 `json:"count_before"`
	CountAfter  int32 `json:"count_after"`
	Applied     bool  `json:"applied"` // false for AnalyzeRecovery and read-only handles
}

// Recovered reports whether anything was (or would be) repaired. A refused
// relink alone is not a repair; see RelinkRefused.
func (r RecoveryInfo) Recovered() bool { return r.RelinkedRecords > 0 || r.TruncatedTailBytes > 0 }

// Recovered returns the repair performed by a writable Open. The header change
// is persisted by the next Sync/Close; a truncation is already on disk.
func (i *Index) Recovered() RecoveryInfo {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.recovery
}
func (t *TTLIndex) Recovered() RecoveryInfo { return t.index.Recovered() }

// AnalyzeRecovery opens an existing pair with O_RDONLY descriptors and returns
// the repair a writable Open would perform, or the CorruptError it would return.
// Nothing is written.
func AnalyzeRecovery(path string, timeIndex bool, location *time.Location) (RecoveryInfo, error) {
	idx, err := open(path, 0, timeIndex, location, modeAnalyze)
	if err != nil {
		return RecoveryInfo{}, err
	}
	info := idx.recovery
	return info, idx.Close()
}

// AnalyzeTTLRecovery is AnalyzeRecovery for a .k2file/.hfile pair.
func AnalyzeTTLRecovery(ctx context.Context, path string) (RecoveryInfo, error) {
	idx, err := openTTL(ctx, path, 0, modeAnalyze)
	if err != nil {
		return RecoveryInfo{}, err
	}
	info := idx.index.recovery
	return info, idx.Close()
}

// maxRecordBytes bounds one record: fixed fields + key + widest Blob prefix + value.
func maxRecordBytes(fixed int64) int64 { return fixed + 2 + math.MaxUint16 + 5 + MaxValueBytes }

// validateRecover walks every record once. It keeps the pre-existing strict
// checks (record boundaries, head positions and buckets) and, outside
// modeReadOnly, collects records newer than their bucket's saved head.
// prevAt/fixed describe the record layout (RealKeyFile: 1/8, RealKeyFile2: 6/13).
//
// start > 2 is a checkpointed (tail) validation (checkpoint.go): the header
// is the checkpoint's, so every head lies below start on a record boundary
// the checkpoint validated, and no record below start is newer than its
// head. Only the records from start on are walked. Whatever the tail alone
// cannot decide as the full walk would - a head at or past start, a refused
// relink (its message and tombstones depend on earlier records), any
// corruption - returns errTailFallback or the error before anything is
// written, and the caller walks everything.
func (i *Index) validateRecover(ctx context.Context, meta func(int64) (metadata, error), prevAt, fixed int64, mode openMode, start int64) error {
	tail := start > 2
	heads := make(map[int64]int)
	buckets := (len(i.header) - headerBytes) / 5
	for bucket := 0; bucket < buckets; bucket++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		pos := i.head(bucket)
		if pos == 0 {
			continue
		}
		if pos < 2 || pos >= i.length {
			return corrupt(CodeHead, pos, "head position %d", pos)
		}
		if tail {
			if pos >= start {
				return errTailFallback
			}
			continue
		}
		if _, exists := heads[pos]; exists {
			return corrupt(CodeHead, pos, "shared bucket head")
		}
		heads[pos] = bucket
	}
	recover := mode != modeReadOnly
	tails := map[int]int64{}   // bucket -> newest chained orphan (deleted ones included)
	live := map[int]bool{}     // bucket -> a non-tombstone orphan is chained
	segment := map[int]int64{} // bucket -> records in the chain segment to adopt
	var liveOrphans []int64    // non-tombstone orphans (tombstoned on refusal)
	var info RecoveryInfo
	end := i.length
	i.openInfo.WalkedBytes += i.length - start
	for pos := start; pos < i.length; {
		if err := ctx.Err(); err != nil {
			return err
		}
		i.openInfo.WalkedRecords++
		m, err := meta(pos)
		if err != nil {
			var ce *CorruptError
			if recover && errors.As(err, &ce) && ce.Code == CodeTruncated {
				if tornErr := i.tornTail(heads, pos, prevAt, fixed); tornErr != nil {
					return errors.Join(err, tornErr)
				}
				info.TruncatedAt, info.TruncatedTailBytes = pos, i.length-pos
				end = pos
				break
			}
			return err
		}
		bucket, _ := i.bucket(m.Key)
		if b, exists := heads[pos]; exists {
			if b != bucket {
				return corrupt(CodeHead, pos, "head bucket mismatch")
			}
			delete(heads, pos)
		}
		if recover {
			saved := i.head(bucket)
			if pos > saved {
				// Every orphan takes part in the chain (Java may delete a
				// record that is still a chain member), but tombstones left by
				// an earlier refusal never trigger a relink: they must stay
				// unreachable. A live record that links back to the
				// saved head while every earlier orphan of its bucket is a
				// tombstone restarts the chain instead of forking it.
				expected, seen := tails[bucket]
				if !seen {
					expected = saved
				}
				switch {
				case m.PreviousPosition == expected:
				case m.PreviousPosition == saved && seen && !live[bucket]:
					segment[bucket] = 0 // tombstones before this record are abandoned
				default:
					if info.RelinkRefused == "" {
						info.RelinkRefused = fmt.Sprintf("record %d newer than bucket %d head %d links to %d, expected %d", pos, bucket, saved, m.PreviousPosition, expected)
					}
				}
				if !m.Tombstone {
					info.Unreachable++
					liveOrphans = append(liveOrphans, pos)
					live[bucket] = true
					if info.RelinkRefused == "" && i.relinkCheck != nil && !m.Deleted {
						r, err := i.record(m)
						if err != nil {
							return err
						}
						if err = i.relinkCheck(r.Key, r.Value); err != nil {
							info.RelinkRefused = fmt.Sprintf("record %d: %v", pos, err)
						}
					}
				}
				tails[bucket] = pos
				segment[bucket]++
			}
		}
		pos = m.NextPosition
	}
	if len(heads) != 0 {
		return corrupt(CodeHead, -1, "head is not a record boundary")
	}
	// Only buckets with a live orphan are adopted; a bucket holding nothing
	// but tombstones keeps its saved head.
	for bucket := range tails {
		if !live[bucket] {
			delete(tails, bucket)
			continue
		}
		info.RelinkedRecords += segment[bucket]
		if i.head(bucket) == 0 {
			info.NewBuckets++
		}
	}
	if info.RelinkRefused != "" && tail {
		return errTailFallback
	}
	if info.RelinkRefused != "" {
		// All-or-nothing: a partial relink would hide the conflicting fork.
		tails = map[int]int64{}
		info.RelinkedRecords, info.NewBuckets = 0, 0
		// The refusal must survive reopen: once the data file grows past the
		// offsets these records reference, a structural check alone would
		// relink them onto unrelated data. They are unreachable in Java
		// anyway, so marking them deleted changes no Java query result.
		info.Tombstoned = int64(len(liveOrphans))
		if mode == modeWrite {
			for _, pos := range liveOrphans {
				if _, err := i.keys.WriteAt([]byte{tombstoneFlag}, pos); err != nil {
					return err
				}
			}
			info.Applied = true
		}
	} else {
		info.Unreachable = 0
	}
	if !info.Recovered() {
		i.recovery = info
		return nil
	}
	info.RelinkedBuckets = int64(len(tails))
	info.CountBefore = int32(binary.BigEndian.Uint32(i.header[4:8]))
	// Java MemHashBlock adds one per newly occupied bucket; MemTimeBlock also
	// adds one per record. Apply the increments the lost header flush missed.
	add := info.NewBuckets
	if i.timeIndex {
		add += info.RelinkedRecords
	}
	if int64(info.CountBefore)+add > math.MaxInt32 {
		return corrupt(CodeHeader, -1, "recovered header count overflows")
	}
	info.CountAfter = info.CountBefore + int32(add)
	if mode == modeWrite {
		if info.TruncatedTailBytes > 0 {
			if err := i.keys.Truncate(end); err != nil {
				return err
			}
		}
		info.Applied = true
		i.length = end
		for bucket, pos := range tails {
			writeLong5(i.header[headerBytes+bucket*5:], pos)
			i.markHeader(headerBytes+bucket*5, 5)
		}
		binary.BigEndian.PutUint32(i.header[4:8], uint32(info.CountAfter))
		if info.RelinkedRecords > 0 {
			i.markHeader(4, 4)
		}
	}
	i.recovery = info
	return nil
}

// tornTail accepts an incomplete final record only when no saved head points
// at or beyond it and its readable fixed fields are well formed, so a damaged
// record in the middle of the file is not mistaken for an interrupted append.
func (i *Index) tornTail(heads map[int64]int, pos, prevAt, fixed int64) error {
	tail := i.length - pos
	if tail > maxRecordBytes(fixed) {
		return corrupt(CodeTruncated, pos, "unparsable region of %d bytes exceeds one record", tail)
	}
	for head, bucket := range heads {
		if head >= pos {
			return corrupt(CodeTruncated, pos, "incomplete record at %d is referenced by bucket %d head %d", pos, bucket, head)
		}
	}
	b := make([]byte, min(tail, fixed))
	if _, err := i.keys.ReadAt(b, pos); err != nil {
		return err
	}
	if b[0] > 1 {
		return corrupt(CodeTruncated, pos, "incomplete record has deleted flag %d", b[0])
	}
	if int64(len(b)) >= prevAt+5 {
		if prev := readLong5(b[prevAt:]); prev != 0 && (prev < 2 || prev >= pos) {
			return corrupt(CodeTruncated, pos, "incomplete record has previous link %d", prev)
		}
	}
	return nil
}
