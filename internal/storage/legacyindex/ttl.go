// Copyright 2015 Scouter authors; Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
// Adapted from RealKeyFile2, IndexKeyFile2 and MemHashBlock at
// dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5; see compatibility/ttl-index.md.
package legacyindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"
)

// TTLIndex owns one .k2file/.hfile pair. Times are absolute Unix seconds, not
// milliseconds. A negative expiry means Java LONG5_MAX_VALUE (no practical expiry).
// Returned slices are owned. One process owner per path is required.
type TTLIndex struct{ index Index }

// SetDeepSearch installs the IndexKeyFile2 traversal observer (see
// DeepSearchFunc): DeepGet for a GetContext miss, DeepUpdate for every
// UpdateOrPutContext that walked the whole chain and a SetTTLContext miss.
// Call it before the index is shared.
func (t *TTLIndex) SetDeepSearch(f DeepSearchFunc) { t.index.SetDeepSearch(f) }

func (t *TTLIndex) deep(op DeepSearchOp, key []byte, looped int, err error) {
	if err == nil && t.index.deepSearch != nil {
		t.index.deepSearch(op, key, looped)
	}
}

// TTLNoExpiry is DataInputX.LONG5_MAX_VALUE, intentionally 255 below the true
// signed 40-bit maximum. Using (1<<39)-1 would change Java-created bytes.
const TTLNoExpiry int64 = math.MaxInt32 * 256

type ttlMetadata struct {
	metadata
	expires int64
}

// OpenTTL uses hashSizeBytes only on creation, including trailing padding.
func OpenTTL(path string, hashSizeBytes int) (*TTLIndex, error) {
	return OpenTTLContext(context.Background(), path, hashSizeBytes)
}
func OpenTTLContext(ctx context.Context, path string, hashSizeBytes int) (*TTLIndex, error) {
	return openTTL(ctx, path, hashSizeBytes, modeWrite)
}

// OpenTTLSealed is OpenTTLContext that skips the record walk when seal
// matches the pair exactly (see OpenSealed); otherwise it validates fully.
func OpenTTLSealed(ctx context.Context, path string, hashSizeBytes int, seal *Seal) (*TTLIndex, error) {
	return openTTLIndex(ctx, path, hashSizeBytes, modeWrite, seal, nil)
}
func openTTL(ctx context.Context, path string, hashSizeBytes int, mode openMode) (*TTLIndex, error) {
	return openTTLIndex(ctx, path, hashSizeBytes, mode, nil, nil)
}
func openTTLIndex(ctx context.Context, path string, hashSizeBytes int, mode openMode, seal *Seal, cp *Checkpoint) (_ *TTLIndex, err error) {
	readOnly := mode != modeWrite
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if path == "" {
		return nil, errors.New("invalid TTL index path")
	}
	ks, ke := os.Stat(path + ".k2file")
	hs, he := os.Stat(path + ".hfile")
	fresh := errors.Is(ke, os.ErrNotExist) && errors.Is(he, os.ErrNotExist)
	if fresh && readOnly {
		return nil, fmt.Errorf("read-only TTL index %s: %w", path, os.ErrNotExist)
	}
	if !fresh {
		if ke != nil || he != nil {
			return nil, corrupt(CodeMissingPair, -1, "missing TTL file pair")
		}
		if !ks.Mode().IsRegular() || !hs.Mode().IsRegular() {
			return nil, corrupt(CodeNonregular, -1, "nonregular TTL files")
		}
		size := hs.Size() - headerBytes
		if ks.Size() < 2 || ks.Size() > maxPosition || size < 5 || size > MaxHashBytes {
			return nil, corrupt(CodeSize, -1, "TTL file size")
		}
		hashSizeBytes = int(size)
	} else if hashSizeBytes < 5 || hashSizeBytes > MaxHashBytes {
		return nil, fmt.Errorf("%w: TTL hash bytes", ErrLimit)
	}
	out := &TTLIndex{}
	idx := &out.index
	idx.readOnly = readOnly
	flags := os.O_RDWR
	if readOnly {
		flags = os.O_RDONLY
	} else if fresh {
		flags |= os.O_CREATE | os.O_EXCL
	}
	idx.keys, err = os.OpenFile(path+".k2file", flags, 0600)
	if err != nil {
		return nil, err
	}
	createdHead := false
	defer func() {
		if err != nil {
			idx.keys.Close()
			if idx.heads != nil {
				idx.heads.Close()
			}
			if fresh {
				os.Remove(path + ".k2file")
			}
			if createdHead {
				os.Remove(path + ".hfile")
			}
		}
	}()
	idx.heads, err = os.OpenFile(path+".hfile", flags, 0600)
	if err != nil {
		return nil, err
	}
	createdHead = fresh
	idx.header = make([]byte, headerBytes+hashSizeBytes)
	if fresh {
		idx.header[0], idx.header[1] = 0xca, 0xfe
		if _, err = idx.keys.WriteAt([]byte{0xca, 0xfe}, 0); err != nil {
			return nil, err
		}
		if _, err = idx.heads.WriteAt(idx.header, 0); err != nil {
			return nil, err
		}
		idx.length = 2
		idx.initPages()
		idx.markHeader(0, 2) // the next sync fsyncs the new header
	} else {
		if _, err = idx.heads.ReadAt(idx.header, 0); err != nil {
			return nil, corrupt(CodeHeader, -1, "TTL hash header: %v", err)
		}
		var magic [2]byte
		if _, err = idx.keys.ReadAt(magic[:], 0); err != nil {
			return nil, corrupt(CodeHeader, 0, "TTL key header: %v", err)
		}
		if magic != [2]byte{0xca, 0xfe} || idx.header[0] != 0xca || idx.header[1] != 0xfe || int32(binary.BigEndian.Uint32(idx.header[4:8])) < 0 {
			return nil, corrupt(CodeMagic, 0, "TTL magic/count")
		}
		if mode == modeWrite {
			idx.initPages()
		}
		if mode == modeWrite && idx.sealedOpen(seal) {
			idx.openCheck = CheckSealed
			return out, nil
		}
		idx.length = ks.Size()
		if mode != modeWrite {
			cp = nil
		}
		if err = idx.validateFrom(cp, func(start int64) error { return out.validate(ctx, mode, start) }); err != nil {
			return nil, err
		}
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (t *TTLIndex) lock(ctx context.Context, write bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		locked := false
		if write {
			locked = t.index.mu.TryLock()
		} else {
			locked = t.index.mu.TryRLock()
		}
		if locked {
			if err := ctx.Err(); err != nil {
				if write {
					t.index.mu.Unlock()
				} else {
					t.index.mu.RUnlock()
				}
				return err
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}
func (t *TTLIndex) meta(pos int64) (ttlMetadata, error) {
	i := &t.index
	bad := func(why string) (ttlMetadata, error) {
		return ttlMetadata{}, corrupt(recordCode(why), pos, "TTL record at %d: %s", pos, why)
	}
	// Only a short read is a truncated record; other I/O errors are not
	// corruption and must never be mistaken for a torn tail.
	badRead := func(err error, why string) (ttlMetadata, error) {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return bad(why)
		}
		return ttlMetadata{}, fmt.Errorf("legacy index read at %d (%s): %w", pos, why, err)
	}
	if pos < 2 || pos > i.length-14 {
		return bad("position")
	}
	var fixed [13]byte
	if _, err := i.keys.ReadAt(fixed[:], pos); err != nil {
		return badRead(err, "truncated fixed fields")
	}
	prev := readLong5(fixed[6:11])
	if prev != 0 && (prev < 2 || prev >= pos) {
		return bad("previous link/cycle")
	}
	keylen := int(binary.BigEndian.Uint16(fixed[11:13]))
	at := pos + 13 + int64(keylen)
	if at >= i.length {
		return bad("truncated key")
	}
	key := make([]byte, keylen)
	if _, err := i.keys.ReadAt(key, pos+13); err != nil {
		return badRead(err, "truncated key")
	}
	var prefix [5]byte
	if _, err := i.keys.ReadAt(prefix[:1], at); err != nil {
		return badRead(err, "blob prefix")
	}
	width := int64(1)
	n := int64(prefix[0])
	if prefix[0] == 255 {
		width = 3
	} else if prefix[0] == 254 {
		width = 5
	}
	if width > 1 {
		if _, err := i.keys.ReadAt(prefix[1:width], at+1); err != nil {
			return badRead(err, "blob length")
		}
		if width == 3 {
			n = int64(binary.BigEndian.Uint16(prefix[1:3]))
		} else {
			n = int64(int32(binary.BigEndian.Uint32(prefix[1:5])))
		}
	}
	if n < 0 || n > MaxValueBytes {
		return bad("blob length limit")
	}
	end := at + width + n
	if end > i.length {
		return bad("truncated payload")
	}
	return ttlMetadata{metadata: metadata{Record: Record{Position: pos, PreviousPosition: prev, NextPosition: end, Key: key, Deleted: fixed[0] != 0, Tombstone: fixed[0] == tombstoneFlag}, valueAt: at + width, valueBytes: int(n)}, expires: readLong5(fixed[1:6])}, nil
}
func (t *TTLIndex) validate(ctx context.Context, mode openMode, start int64) error {
	meta := func(pos int64) (metadata, error) {
		m, err := t.meta(pos)
		return m.metadata, err
	}
	return t.index.validateRecover(ctx, meta, 6, 13, mode, start)
}

// Unlike Index.walkChain, matching deleted records must be visited: the newest
// match hides all older values even when deleted or expired.
// It returns the number of chain records read (Java IndexKeyFile2 looping).
func (t *TTLIndex) walk(ctx context.Context, bucket int, visit func(ttlMetadata) error) (int, error) {
	i := &t.index
	next := i.length
	count := 0
	for pos := i.head(bucket); pos != 0; {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		if count >= MaxResultRecords {
			return count, ErrLimit
		}
		count++
		m, err := t.meta(pos)
		if err != nil {
			return count, err
		}
		actual, _ := i.bucket(m.Key)
		if m.NextPosition > next || actual != bucket {
			return count, corrupt(CodeChain, pos, "TTL bucket chain")
		}
		if err := visit(m); err != nil {
			return count, err
		}
		next, pos = pos, m.PreviousPosition
	}
	return count, nil
}
func (t *TTLIndex) GetContext(ctx context.Context, key []byte, nowSeconds int64) ([]byte, error) {
	if err := t.lock(ctx, false); err != nil {
		return nil, err
	}
	defer t.index.mu.RUnlock()
	if err := t.index.ready(); err != nil {
		return nil, err
	}
	bucket, err := t.index.bucket(key)
	if err != nil {
		return nil, err
	}
	var value []byte
	looped, err := t.walk(ctx, bucket, func(m ttlMetadata) error {
		if !bytes.Equal(m.Key, key) {
			return nil
		}
		if !m.Deleted && m.expires >= nowSeconds {
			r, err := t.index.record(m.metadata)
			if err != nil {
				return err
			}
			value = r.Value
		}
		return errFound
	})
	if errors.Is(err, errFound) {
		return value, nil
	}
	t.deep(DeepGet, key, looped, err)
	return value, err
}
func ttlExpiry(expires int64) (int64, error) {
	if expires < 0 {
		return TTLNoExpiry, nil
	}
	if expires > maxPosition {
		return 0, ErrLimit
	}
	return expires, nil
}
func ttlRecord(key, value []byte, prev, expires int64) []byte {
	prefix := blobPrefix(len(value))
	out := make([]byte, 13+len(key)+len(prefix)+len(value))
	writeLong5(out[1:6], expires)
	writeLong5(out[6:11], prev)
	binary.BigEndian.PutUint16(out[11:13], uint16(len(key)))
	copy(out[13:], key)
	copy(out[13+len(key):], prefix)
	copy(out[13+len(key)+len(prefix):], value)
	return out
}
func (t *TTLIndex) put(bucket int, key, value []byte, expires int64) (int64, error) {
	i := &t.index
	prev := i.head(bucket)
	count := int32(binary.BigEndian.Uint32(i.header[4:8]))
	if prev == 0 && count == math.MaxInt32 {
		return 0, ErrLimit
	}
	record := ttlRecord(key, value, prev, expires)
	pos := i.length
	if pos > maxPosition-int64(len(record)) {
		return 0, ErrLimit
	}
	if _, err := i.keys.WriteAt(record, pos); err != nil {
		if rollback := i.keys.Truncate(pos); rollback != nil {
			i.fault = errors.Join(err, rollback)
		}
		return 0, err
	}
	i.length += int64(len(record))
	writeLong5(i.header[headerBytes+bucket*5:], pos)
	i.markHeader(headerBytes+bucket*5, 5)
	if prev == 0 {
		binary.BigEndian.PutUint32(i.header[4:8], uint32(count+1))
		i.markHeader(4, 4)
	}
	return pos, nil
}
func (t *TTLIndex) checkPut(key, value []byte, expires int64) (int, int64, error) {
	if err := t.index.writable(); err != nil {
		return 0, 0, err
	}
	bucket, err := t.index.bucket(key)
	if err != nil {
		return 0, 0, err
	}
	if value == nil {
		return 0, 0, errors.New("nil TTL value")
	}
	if len(value) > MaxValueBytes {
		return 0, 0, ErrLimit
	}
	exp, err := ttlExpiry(expires)
	return bucket, exp, err
}
func (t *TTLIndex) PutContext(ctx context.Context, key, value []byte, expiresSeconds int64) (int64, error) {
	if err := t.lock(ctx, true); err != nil {
		return 0, err
	}
	defer t.index.mu.Unlock()
	bucket, expires, err := t.checkPut(key, value, expiresSeconds)
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return t.put(bucket, key, value, expires)
}

// UpdateOrPutContext preserves IndexKeyFile2's matching-record rewrites followed
// by unconditional append. Different Blob widths append immediately, avoiding
// RealKeyFile2's shrinking-record corruption; normal KV offsets are always 5B.
func (t *TTLIndex) UpdateOrPutContext(ctx context.Context, key, value []byte, expiresSeconds int64) (int64, error) {
	if err := t.lock(ctx, true); err != nil {
		return 0, err
	}
	defer t.index.mu.Unlock()
	bucket, expires, err := t.checkPut(key, value, expiresSeconds)
	if err != nil {
		return 0, err
	}
	looped, err := t.walk(ctx, bucket, func(m ttlMetadata) error {
		if !bytes.Equal(m.Key, key) {
			return nil
		}
		if m.valueBytes != len(value) {
			return errFound
		}
		original := make([]byte, m.NextPosition-m.Position)
		if _, err := t.index.keys.ReadAt(original, m.Position); err != nil {
			return err
		}
		replacement := ttlRecord(key, value, m.PreviousPosition, expires)
		if len(replacement) != len(original) {
			return errFound
		} // noncanonical legacy Blob prefix
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := t.index.keys.WriteAt(replacement, m.Position); err != nil {
			if _, rollback := t.index.keys.WriteAt(original, m.Position); rollback != nil {
				t.index.fault = errors.Join(err, rollback)
			}
			return err
		}
		return nil
	})
	if err != nil && !errors.Is(err, errFound) {
		return 0, err
	}
	t.deep(DeepUpdate, key, looped, err) // whole chain walked (no early put)
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return t.put(bucket, key, value, expires)
}
func (t *TTLIndex) DeleteContext(ctx context.Context, key []byte) (bool, error) {
	if err := t.lock(ctx, true); err != nil {
		return false, err
	}
	defer t.index.mu.Unlock()
	if err := t.index.writable(); err != nil {
		return false, err
	}
	bucket, err := t.index.bucket(key)
	if err != nil {
		return false, err
	}
	deleted := false
	_, err = t.walk(ctx, bucket, func(m ttlMetadata) error {
		if !bytes.Equal(m.Key, key) {
			return nil
		}
		if !m.Deleted {
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, err := t.index.keys.WriteAt([]byte{1}, m.Position); err != nil {
				return err
			}
			deleted = true
		}
		return errFound
	})
	if errors.Is(err, errFound) {
		err = nil
	}
	return deleted, err
}

// SetTTLContext accepts an absolute expiry. The caller converts relative TTL
// using its own clock. Only the newest live matching record can be changed.
func (t *TTLIndex) SetTTLContext(ctx context.Context, key []byte, nowSeconds, expiresSeconds int64) (bool, error) {
	if err := t.lock(ctx, true); err != nil {
		return false, err
	}
	defer t.index.mu.Unlock()
	if err := t.index.writable(); err != nil {
		return false, err
	}
	bucket, err := t.index.bucket(key)
	if err != nil {
		return false, err
	}
	expires, err := ttlExpiry(expiresSeconds)
	if err != nil {
		return false, err
	}
	changed := false
	looped, err := t.walk(ctx, bucket, func(m ttlMetadata) error {
		if !bytes.Equal(m.Key, key) {
			return nil
		}
		if !m.Deleted && m.expires >= nowSeconds {
			if err := ctx.Err(); err != nil {
				return err
			}
			var b [5]byte
			writeLong5(b[:], expires)
			if _, err := t.index.keys.WriteAt(b[:], m.Position+1); err != nil {
				writeLong5(b[:], m.expires)
				if _, rollback := t.index.keys.WriteAt(b[:], m.Position+1); rollback != nil {
					t.index.fault = errors.Join(err, rollback)
				}
				return err
			}
			changed = true
		}
		return errFound
	})
	if errors.Is(err, errFound) {
		return changed, nil
	}
	t.deep(DeepUpdate, key, looped, err)
	return changed, err
}
func (t *TTLIndex) Sync() error { return t.index.Sync() }

// CloseSealed closes the handle and returns its Seal (see Index.CloseSealed).
func (t *TTLIndex) CloseSealed() (Seal, error) { return t.index.CloseSealed() }

// OpenCheck reports how Open accepted the pair (see Index.OpenCheck).
func (t *TTLIndex) OpenCheck() OpenCheck { return t.index.OpenCheck() }
func (t *TTLIndex) Close() error         { return t.index.Close() }
