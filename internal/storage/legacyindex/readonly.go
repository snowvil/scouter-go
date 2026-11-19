// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
package legacyindex

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

// ErrReadOnly is returned by every mutation and Sync on a read-only handle.
var ErrReadOnly = errors.New("legacy index opened read-only")

// Structural corruption codes reported by CorruptError.
const (
	CodeMissingPair = "missing_pair"
	CodeNonregular  = "nonregular"
	CodeSize        = "size"
	CodeHeader      = "header"
	CodeMagic       = "magic"
	CodeRecord      = "record"
	CodeTruncated   = "truncated"
	CodeCycle       = "cycle"
	CodeKey         = "key"
	CodeHead        = "head"
	CodeChain       = "chain"
	CodeLink        = "link"
)

// CorruptError classifies index corruption. errors.Is(err, ErrCorrupt) holds,
// and the message text is the same as the previous untyped errors.
type CorruptError struct {
	Code   string
	Offset int64 // key-file offset, or -1 when the problem is not positional
	msg    string
}

func (e *CorruptError) Error() string { return e.msg }
func (e *CorruptError) Unwrap() error { return ErrCorrupt }

func corrupt(code string, offset int64, format string, args ...any) error {
	return &CorruptError{Code: code, Offset: offset, msg: ErrCorrupt.Error() + ": " + fmt.Sprintf(format, args...)}
}
func recordCode(why string) string {
	switch why {
	case "previous link/cycle":
		return CodeCycle
	case "invalid key":
		return CodeKey
	case "blob length limit":
		return CodeRecord
	}
	return CodeTruncated
}

// OpenReadOnly validates an existing .kfile/.hfile pair with O_RDONLY
// descriptors. It never creates, writes, syncs or flushes either file; Put,
// Update and Sync return ErrReadOnly and Close only releases descriptors.
func OpenReadOnly(path string, timeIndex bool, location *time.Location) (*Index, error) {
	return open(path, 0, timeIndex, location, modeReadOnly)
}

// OpenTTLReadOnly is the TTL (.k2file) equivalent of OpenReadOnly.
func OpenTTLReadOnly(ctx context.Context, path string) (*TTLIndex, error) {
	return openTTL(ctx, path, 0, modeReadOnly)
}

// Stats describes a structurally valid index. ExpectedCount follows the Java
// MemHashBlock/MemTimeBlock count rule for the index kind.
type Stats struct {
	KeyFileBytes     int64
	Buckets          int
	HeaderCount      int32
	Records          int64 // physical records, including deletion markers
	Deleted          int64
	OccupiedBuckets  int64
	ReachableRecords int64 // records reachable from bucket heads
	ExpectedCount    int64
	// TTL only: per-record state at the inspection time. Live+Expired+Deleted=Records.
	Live, Expired int64
}

// bitset bounds boundary tracking to KeyFileBytes/8 bytes of memory.
type bitset []uint64

func (b bitset) set(n int64)      { b[n>>6] |= 1 << (uint(n) & 63) }
func (b bitset) has(n int64) bool { return b[n>>6]&(1<<(uint(n)&63)) != 0 }

// inspect walks every record then every bucket chain. Each chain link must
// land on a physical record boundary of the same bucket, in decreasing order.
func (i *Index) inspect(ctx context.Context, meta func(int64) (metadata, int64, error), record func(metadata, int64)) (Stats, error) {
	st := Stats{KeyFileBytes: i.length, Buckets: (len(i.header) - headerBytes) / 5, HeaderCount: int32(binary.BigEndian.Uint32(i.header[4:8]))}
	bounds := make(bitset, i.length/64+1)
	for pos := int64(2); pos < i.length; {
		if err := ctx.Err(); err != nil {
			return st, err
		}
		m, extra, err := meta(pos)
		if err != nil {
			return st, err
		}
		bounds.set(pos)
		st.Records++
		if m.Deleted {
			st.Deleted++
		}
		record(m, extra)
		pos = m.NextPosition
	}
	for bucket := 0; bucket < st.Buckets; bucket++ {
		pos := i.head(bucket)
		if pos == 0 {
			continue
		}
		st.OccupiedBuckets++
		next := i.length
		for pos != 0 {
			if err := ctx.Err(); err != nil {
				return st, err
			}
			if pos < 2 || pos >= next || !bounds.has(pos) {
				return st, corrupt(CodeLink, pos, "bucket %d link %d is not an earlier record boundary", bucket, pos)
			}
			m, _, err := meta(pos)
			if err != nil {
				return st, err
			}
			if actual, _ := i.bucket(m.Key); actual != bucket {
				return st, corrupt(CodeChain, pos, "record in bucket %d chain hashes to %d", bucket, actual)
			}
			st.ReachableRecords++
			next, pos = pos, m.PreviousPosition
		}
	}
	st.ExpectedCount = st.OccupiedBuckets
	if i.timeIndex {
		st.ExpectedCount += st.Records
	}
	return st, nil
}

// Inspect re-validates every record and chain without writing. Memory use is
// the key file size divided by eight plus the already loaded hash header.
func (i *Index) Inspect(ctx context.Context) (Stats, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if err := i.ready(); err != nil {
		return Stats{}, err
	}
	return i.inspect(ctx, func(pos int64) (metadata, int64, error) {
		m, err := i.meta(pos)
		return m, 0, err
	}, func(metadata, int64) {})
}

// Inspect counts TTL record states with Java's expire < now rule (seconds).
func (t *TTLIndex) Inspect(ctx context.Context, nowSeconds int64) (Stats, error) {
	if err := t.lock(ctx, false); err != nil {
		return Stats{}, err
	}
	defer t.index.mu.RUnlock()
	if err := t.index.ready(); err != nil {
		return Stats{}, err
	}
	var live, expired int64
	st, err := t.index.inspect(ctx, func(pos int64) (metadata, int64, error) {
		m, err := t.meta(pos)
		return m.metadata, m.expires, err
	}, func(m metadata, expires int64) {
		if m.Deleted {
			return
		}
		if expires < nowSeconds {
			expired++
		} else {
			live++
		}
	})
	st.Live, st.Expired = live, expired
	// MemHashBlock counts occupied buckets only for TTL files.
	st.ExpectedCount = st.OccupiedBuckets
	return st, err
}

// TTLRecord is an owned physical TTL record; Expires is Unix seconds.
type TTLRecord struct {
	Record
	Expires int64
}

// ScanContext visits every physical TTL record in append order, including
// deleted and expired records.
func (t *TTLIndex) ScanContext(ctx context.Context, visit func(TTLRecord) error) error {
	if err := t.lock(ctx, false); err != nil {
		return err
	}
	defer t.index.mu.RUnlock()
	if err := t.index.ready(); err != nil {
		return err
	}
	if visit == nil {
		return errors.New("nil visitor")
	}
	for pos := int64(2); pos < t.index.length; {
		if err := ctx.Err(); err != nil {
			return err
		}
		m, err := t.meta(pos)
		if err != nil {
			return err
		}
		r, err := t.index.record(m.metadata)
		if err != nil {
			return err
		}
		if err = visit(TTLRecord{Record: r, Expires: m.expires}); err != nil {
			return err
		}
		pos = m.NextPosition
	}
	return nil
}

// ScanAllContext visits every physical record in append order, including
// deletion markers (Record.Deleted), with cancellation between records.
func (i *Index) ScanAllContext(ctx context.Context, visit func(Record) error) error {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if err := i.ready(); err != nil {
		return err
	}
	if visit == nil {
		return errors.New("nil visitor")
	}
	for pos := int64(2); pos < i.length; {
		if err := ctx.Err(); err != nil {
			return err
		}
		m, err := i.meta(pos)
		if err != nil {
			return err
		}
		r, err := i.record(m)
		if err != nil {
			return err
		}
		if err = visit(r); err != nil {
			return err
		}
		pos = m.NextPosition
	}
	return nil
}

// IsTimeIndex reports whether the handle uses the Java time bucket layout.
func (i *Index) IsTimeIndex() bool { return i.timeIndex }
