// Copyright 2015 the original author or authors (Scouter).
// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
// Format and algorithms adapted from Scouter's RealKeyFile, IndexKeyFile,
// IndexTimeFile, MemHashBlock, MemTimeBlock, and HashUtil at
// dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5. See CONTRACT.md.
package legacyindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var ErrClosed = errors.New("legacy index closed")
var ErrCorrupt = errors.New("corrupt legacy index")
var ErrLimit = errors.New("legacy index limit exceeded")

const (
	MaxValueBytes    = 16 << 20
	MaxResultBytes   = 64 << 20
	MaxResultRecords = 100000
	MaxHashBytes     = 128 << 20
	TimeHashBytes    = 172800 * 5
	headerBytes      = 1024
	maxPosition      = (1 << 39) - 1
	dayMillis        = int64(86400000)
)

// Record owns its byte slices; callers may retain and mutate them.
type Record struct {
	Position, PreviousPosition, NextPosition int64
	Key, Value                               []byte
	Deleted                                  bool
	// Tombstone marks a record a refused relink retired (deleted flag 2).
	// Java reads any nonzero flag as deleted; Go never adopts it as a head.
	Tombstone bool
}

// tombstoneFlag is the deleted-flag value written by a refused relink. Java
// DataInputStream.readBoolean treats every nonzero byte as true.
const tombstoneFlag = 2

// Index has one owner per path. Callbacks run under the read lock and must not
// call Index methods. No cross-process writer coordination is provided.
type Index struct {
	mu          sync.RWMutex
	headerIO    sync.Mutex // orders header writes of sync and SyncPoint.Commit
	keys, heads *os.File
	header      []byte
	length      int64
	timeIndex   bool
	base        int64
	// dirty: header bytes changed in memory since they were last snapshot or
	// written (dirtyBits lists the 4 KiB pages; checkpoint.go). Guarded by mu.
	closed, dirty bool
	readOnly      bool
	fault         error
	recovery      RecoveryInfo
	openCheck     OpenCheck
	openInfo      OpenInfo
	relinkCheck   RelinkCheck // only consulted while opening
	deepSearch    DeepSearchFunc
	// Partial header writes (writable handles; checkpoint.go). dirtyBits,
	// pendingRefs (per page, how many uncommitted SyncPoints hold it in their
	// snapshot) and outstanding are guarded by mu. sums (CRC32 of each header
	// page as on disk), sumsOK, lost and cpN are guarded by headerIO;
	// headerSeq counts header writes (written under headerIO). cp/cpOK/cpGen
	// are guarded by mu.
	dirtyBits    []uint64
	pendingRefs  []uint32
	outstanding  int // SyncPoints begun and not yet committed
	sums         []uint32
	sumsOK, lost bool
	headerSeq    atomic.Uint64
	cpN          uint64
	cp           Checkpoint
	cpOK         bool
	cpGen        uint64
	// viewBuckets is the hash capacity of a KeyChain view (header == nil).
	viewBuckets int
	// appendOnly refuses in-place rewrites (Update, Delete): KeyChain
	// snapshots of the index are then exact (SetAppendOnly).
	appendOnly bool
	// hook observes or fails I/O steps in tests ("keys-sync",
	// "header-write", "header-sync"); nil in production.
	hook func(op string) error
}

// DeepSearchOp names the Java IndexKeyFile/IndexKeyFile2 method whose
// "Too many index deep searching" warning a lookup corresponds to.
type DeepSearchOp uint8

const (
	// DeepGet is IndexKeyFile.get / IndexKeyFile2.get (Java id S152).
	DeepGet DeepSearchOp = iota
	// DeepUpdate is IndexKeyFile2.updateOrPut / setTTL (Java id S161).
	DeepUpdate
)

// DeepSearchFunc observes the hash-chain length a Java-warning lookup walked
// (looped, Java's loop counter, deleted records included) when Java would
// compare it with log_index_traversal_warning_count. Index lookups call it
// under the index lock; KeyChain.LookupContext calls it without any lock,
// possibly concurrently with other calls. Either way it must be fast, safe
// for concurrent use and must not call the index. key is the caller's slice
// and is only valid during the call.
type DeepSearchFunc func(op DeepSearchOp, key []byte, looped int)

// SetDeepSearch installs the observer; call it before the index is shared.
func (i *Index) SetDeepSearch(f DeepSearchFunc) {
	i.mu.Lock()
	i.deepSearch = f
	i.mu.Unlock()
}

// Open accepts a base path without suffixes. Existing hash capacity comes from
// the .hfile; hashSizeBytes is used only for creation. Time mode requires an
// explicit timezone and uses Java's fixed 2000-01-01 midnight base.
func Open(path string, hashSizeBytes int, timeIndex bool, location *time.Location) (*Index, error) {
	return open(path, hashSizeBytes, timeIndex, location, modeWrite)
}

// RelinkCheck validates the key/value of a record that a writable Open would
// relink (see RecoveryInfo). Any error refuses the whole relink, leaving the
// original Java semantics (records stay unreachable), and is reported in
// RecoveryInfo.RelinkRefused. Callers use it to require that the data a value
// points at survived the crash.
type RelinkCheck func(key, value []byte) error

// AnalyzeRecoveryChecked is AnalyzeRecovery with the same RelinkCheck a
// writable OpenChecked would apply, so previews match the runtime decision.
func AnalyzeRecoveryChecked(path string, timeIndex bool, location *time.Location, check RelinkCheck) (RecoveryInfo, error) {
	idx, err := openWith(path, 0, timeIndex, location, modeAnalyze, check)
	if err != nil {
		return RecoveryInfo{}, err
	}
	info := idx.recovery
	return info, idx.Close()
}

// OpenChecked is Open with a RelinkCheck applied before any relink.
func OpenChecked(path string, hashSizeBytes int, timeIndex bool, location *time.Location, check RelinkCheck) (*Index, error) {
	return openWith(path, hashSizeBytes, timeIndex, location, modeWrite, check)
}
func open(path string, hashSizeBytes int, timeIndex bool, location *time.Location, mode openMode) (*Index, error) {
	return openWith(path, hashSizeBytes, timeIndex, location, mode, nil)
}
func openWith(path string, hashSizeBytes int, timeIndex bool, location *time.Location, mode openMode, check RelinkCheck) (*Index, error) {
	return openIndex(path, hashSizeBytes, timeIndex, location, mode, check, nil, nil)
}
func openIndex(path string, hashSizeBytes int, timeIndex bool, location *time.Location, mode openMode, check RelinkCheck, seal *Seal, cp *Checkpoint) (_ *Index, err error) {
	readOnly := mode != modeWrite
	if path == "" || (timeIndex && location == nil) {
		return nil, errors.New("invalid index path/timezone")
	}
	idx := &Index{timeIndex: timeIndex, readOnly: readOnly, relinkCheck: check}
	if timeIndex {
		hashSizeBytes = TimeHashBytes
		idx.base = time.Date(2000, 1, 1, 0, 0, 0, 0, location).UnixMilli()
	}
	ks, ke := os.Stat(path + ".kfile")
	hs, he := os.Stat(path + ".hfile")
	newFiles := errors.Is(ke, os.ErrNotExist) && errors.Is(he, os.ErrNotExist)
	if newFiles && readOnly {
		return nil, fmt.Errorf("read-only index %s: %w", path, os.ErrNotExist)
	}
	if !newFiles {
		if ke != nil || he != nil {
			return nil, corrupt(CodeMissingPair, -1, "missing file pair: %v / %v", ke, he)
		}
		if !ks.Mode().IsRegular() || !hs.Mode().IsRegular() {
			return nil, corrupt(CodeNonregular, -1, "nonregular index files")
		}
		size := hs.Size() - headerBytes
		if ks.Size() < 2 || ks.Size() > maxPosition || size < 5 || size > MaxHashBytes || (timeIndex && size != TimeHashBytes) {
			return nil, corrupt(CodeSize, -1, "invalid file size")
		}
		hashSizeBytes = int(size)
	} else if hashSizeBytes < 5 || hashSizeBytes > MaxHashBytes {
		return nil, fmt.Errorf("%w: hash bytes", ErrLimit)
	}
	flags := os.O_RDWR
	if readOnly {
		flags = os.O_RDONLY
	} else if newFiles {
		flags |= os.O_CREATE | os.O_EXCL
	}
	idx.keys, err = os.OpenFile(path+".kfile", flags, 0600)
	if err != nil {
		return nil, err
	}
	createdKey := newFiles
	createdHead := false
	defer func() {
		if err != nil {
			_ = idx.keys.Close()
			if idx.heads != nil {
				_ = idx.heads.Close()
			}
			if createdKey {
				_ = os.Remove(path + ".kfile")
			}
			if createdHead {
				_ = os.Remove(path + ".hfile")
			}
		}
	}()
	idx.heads, err = os.OpenFile(path+".hfile", flags, 0600)
	if err != nil {
		return nil, err
	}
	createdHead = newFiles
	idx.header = make([]byte, headerBytes+hashSizeBytes)
	if newFiles {
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
			return nil, corrupt(CodeHeader, -1, "hash header: %v", err)
		}
		magic := make([]byte, 2)
		if _, err = idx.keys.ReadAt(magic, 0); err != nil {
			return nil, corrupt(CodeHeader, 0, "key header: %v", err)
		}
		if !bytes.Equal(magic, []byte{0xca, 0xfe}) || idx.header[0] != 0xca || idx.header[1] != 0xfe || int32(binary.BigEndian.Uint32(idx.header[4:8])) < 0 {
			return nil, corrupt(CodeMagic, 0, "magic/count")
		}
		if mode == modeWrite {
			idx.initPages()
		}
		if mode == modeWrite && idx.sealedOpen(seal) {
			idx.openCheck = CheckSealed
			return idx, nil
		}
		idx.length = ks.Size()
		if mode != modeWrite {
			cp = nil
		}
		if err = idx.validateFrom(cp, func(start int64) error { return idx.validateRecover(context.Background(), idx.meta, 1, 8, mode, start) }); err != nil {
			return nil, err
		}
	}
	return idx, nil
}

// OpenCheck reports how Open accepted the pair.
type OpenCheck uint8

const (
	// CheckCreated: a new pair was created; there was nothing to check.
	CheckCreated OpenCheck = iota
	// CheckWalked: every record was validated (validateRecover), O(records).
	CheckWalked
	// CheckSealed: a matching Seal replaced the walk (OpenSealed); header,
	// magic, sizes and bucket-head ranges were checked, not the records.
	CheckSealed
	// CheckTail: a matching Checkpoint limited the walk to the records
	// appended after it (OpenCheckpointed), O(tail).
	CheckTail
)

// OpenCheck reports how this handle's Open accepted the pair.
func (i *Index) OpenCheck() OpenCheck { return i.openCheck }

func (i *Index) ready() error {
	if i.closed {
		return ErrClosed
	}
	return i.fault
}

// ErrAppendOnly reports Update or Delete on an index marked SetAppendOnly.
var ErrAppendOnly = errors.New("legacy index is append-only")

// SetAppendOnly refuses Update and Delete from now on (ErrAppendOnly), so
// records once written never change and every KeyChain snapshot is exact.
// Open-time recovery is not affected. Call it before the index is shared.
func (i *Index) SetAppendOnly() {
	i.mu.Lock()
	i.appendOnly = true
	i.mu.Unlock()
}

// AppendOnly reports SetAppendOnly.
func (i *Index) AppendOnly() bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.appendOnly
}

// rewritable is writable for in-place rewrites (Update, Delete).
func (i *Index) rewritable() error {
	if err := i.writable(); err != nil {
		return err
	}
	if i.appendOnly {
		return ErrAppendOnly
	}
	return nil
}
func (i *Index) writable() error {
	if err := i.ready(); err != nil {
		return err
	}
	if i.readOnly {
		return ErrReadOnly
	}
	return nil
}
func readLong5(b []byte) int64 {
	v := int64(0)
	for _, n := range b[:5] {
		v = v<<8 | int64(n)
	}
	if b[0]&0x80 != 0 {
		v -= 1 << 40
	}
	return v
}
func writeLong5(b []byte, v int64) {
	for n := 4; n >= 0; n-- {
		b[n] = byte(v)
		v >>= 8
	}
}
func (i *Index) bucket(key []byte) (int, error) {
	if key == nil {
		return 0, errors.New("nil key")
	}
	if len(key) > 65535 {
		return 0, fmt.Errorf("%w: key bytes", ErrLimit)
	}
	if i.timeIndex {
		if len(key) != 8 {
			return 0, errors.New("time key must be 8 bytes")
		}
		ms := int64(binary.BigEndian.Uint64(key))
		if ms <= 0 {
			return 0, errors.New("time must be positive")
		}
		return i.timeBucket(ms), nil
	}
	return int((crc32.ChecksumIEEE(key) & math.MaxInt32) % uint32(i.buckets())), nil
}

// buckets is the hash capacity. A KeyChain view has no header and carries
// the capacity instead.
func (i *Index) buckets() int {
	if i.header == nil {
		return i.viewBuckets
	}
	return (len(i.header) - headerBytes) / 5
}
func (i *Index) timeBucket(ms int64) int {
	if ms < i.base {
		return 0
	}
	return int(((ms - i.base) % dayMillis) / 500)
}
func (i *Index) head(bucket int) int64 { return readLong5(i.header[headerBytes+bucket*5:]) }

func (i *Index) Put(key, value []byte) (int64, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.writable(); err != nil {
		return 0, err
	}
	bucket, err := i.bucket(key)
	if err != nil {
		return 0, err
	}
	if value == nil {
		return 0, errors.New("nil value")
	}
	if len(value) > MaxValueBytes {
		return 0, fmt.Errorf("%w: value bytes", ErrLimit)
	}
	add := int32(0)
	previous := i.head(bucket)
	if previous == 0 {
		add++
	}
	if i.timeIndex {
		add++
	}
	count := int32(binary.BigEndian.Uint32(i.header[4:8]))
	if count > math.MaxInt32-add {
		return 0, fmt.Errorf("%w: header count overflow", ErrLimit)
	}
	prefix := blobPrefix(len(value))
	record := make([]byte, 8+len(key)+len(prefix)+len(value))
	writeLong5(record[1:6], previous)
	binary.BigEndian.PutUint16(record[6:8], uint16(len(key)))
	copy(record[8:], key)
	copy(record[8+len(key):], prefix)
	copy(record[8+len(key)+len(prefix):], value)
	pos := i.length
	if pos > maxPosition-int64(len(record)) {
		return 0, fmt.Errorf("%w: file position", ErrLimit)
	}
	if _, err = i.keys.WriteAt(record, pos); err != nil {
		if rollback := i.keys.Truncate(pos); rollback != nil {
			i.fault = errors.Join(err, rollback)
		}
		return 0, err
	}
	i.length += int64(len(record))
	writeLong5(i.header[headerBytes+bucket*5:], pos)
	binary.BigEndian.PutUint32(i.header[4:8], uint32(count+add))
	i.markHeader(headerBytes+bucket*5, 5)
	i.markHeader(4, 4)
	return pos, nil
}

// Entry is one key/value of PutBatch.
type Entry struct{ Key, Value []byte }

// PutBatch appends entries as consecutive Puts would, in order (each record
// links to its bucket's head, which may be an earlier record of the batch),
// with one write for all records instead of one per record. It returns the
// record positions. On a write error the key file is truncated back and
// nothing of the batch is kept, as Put does for its one record.
func (i *Index) PutBatch(entries []Entry) ([]int64, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.writable(); err != nil {
		return nil, err
	}
	type planned struct {
		bucket int
		pos    int64
	}
	plan := make([]planned, len(entries))
	size := 0
	for n, e := range entries {
		bucket, err := i.bucket(e.Key)
		if err != nil {
			return nil, err
		}
		if e.Value == nil {
			return nil, errors.New("nil value")
		}
		if len(e.Value) > MaxValueBytes {
			return nil, fmt.Errorf("%w: value bytes", ErrLimit)
		}
		plan[n].bucket = bucket
		size += 8 + len(e.Key) + len(blobPrefix(len(e.Value))) + len(e.Value)
	}
	start := i.length
	if start > maxPosition-int64(size) {
		return nil, fmt.Errorf("%w: file position", ErrLimit)
	}
	// Heads and the count as consecutive Puts leave them; applied only after
	// the write succeeds.
	heads := map[int]int64{}
	head := func(b int) int64 {
		if h, ok := heads[b]; ok {
			return h
		}
		return i.head(b)
	}
	count := int32(binary.BigEndian.Uint32(i.header[4:8]))
	buf := make([]byte, 0, size)
	pos := start
	for n, e := range entries {
		b := plan[n].bucket
		previous := head(b)
		add := int32(0)
		if previous == 0 {
			add++
		}
		if i.timeIndex {
			add++
		}
		if count > math.MaxInt32-add {
			return nil, fmt.Errorf("%w: header count overflow", ErrLimit)
		}
		count += add
		prefix := blobPrefix(len(e.Value))
		var fixed [8]byte
		writeLong5(fixed[1:6], previous)
		binary.BigEndian.PutUint16(fixed[6:8], uint16(len(e.Key)))
		buf = append(buf, fixed[:]...)
		buf = append(buf, e.Key...)
		buf = append(buf, prefix...)
		buf = append(buf, e.Value...)
		plan[n].pos = pos
		heads[b] = pos
		pos = start + int64(len(buf))
	}
	if _, err := i.keys.WriteAt(buf, start); err != nil {
		if rollback := i.keys.Truncate(start); rollback != nil {
			i.fault = errors.Join(err, rollback)
		}
		return nil, err
	}
	i.length = start + int64(len(buf))
	for b, h := range heads {
		writeLong5(i.header[headerBytes+b*5:], h)
		i.markHeader(headerBytes+b*5, 5)
	}
	binary.BigEndian.PutUint32(i.header[4:8], uint32(count))
	if len(entries) > 0 {
		i.markHeader(4, 4)
	}
	out := make([]int64, len(plan))
	for n := range plan {
		out[n] = plan[n].pos
	}
	return out, nil
}

func blobPrefix(n int) []byte {
	if n <= 253 {
		return []byte{byte(n)}
	}
	if n <= 65535 {
		return []byte{255, byte(n >> 8), byte(n)}
	}
	out := make([]byte, 5)
	out[0] = 254
	binary.BigEndian.PutUint32(out[1:], uint32(n))
	return out
}

type metadata struct {
	Record
	valueAt    int64
	valueBytes int
	// value is the payload when meta read it with the record head (owned by
	// this metadata), nil otherwise.
	value []byte
}

// metaReadAhead is how much meta reads at a record in one ReadAt: the fixed
// fields, key, blob prefix and payload of a typical record (time and counter
// keys are 8 bytes, payloads a 5-byte data position). A range scan reads
// tens of thousands of chain records, and one syscall per field made it
// syscall-bound. A record that does not fit, or fails a check, is read again
// field by field (metaFields), so errors and results do not change.
const metaReadAhead = 64

func (i *Index) meta(pos int64) (metadata, error) {
	if pos < 2 || pos > i.length-9 {
		return i.metaFields(pos)
	}
	buf := make([]byte, min(int64(metaReadAhead), i.length-pos))
	if _, err := i.keys.ReadAt(buf, pos); err != nil {
		return i.metaFields(pos)
	}
	previous := readLong5(buf[1:6])
	if previous != 0 && (previous < 2 || previous >= pos) {
		return i.metaFields(pos)
	}
	keylen := int(binary.BigEndian.Uint16(buf[6:8]))
	prefixAt := 8 + keylen
	if prefixAt >= len(buf) || buf[prefixAt] >= 254 {
		return i.metaFields(pos)
	}
	n := int(buf[prefixAt])
	end := prefixAt + 1 + n
	if end > len(buf) {
		return i.metaFields(pos)
	}
	key := buf[8:prefixAt:prefixAt]
	if _, err := i.bucket(key); err != nil {
		return i.metaFields(pos)
	}
	return metadata{Record: Record{Position: pos, PreviousPosition: previous, NextPosition: pos + int64(end), Key: key, Deleted: buf[0] != 0, Tombstone: buf[0] == tombstoneFlag},
		valueAt: pos + int64(prefixAt) + 1, valueBytes: n, value: buf[prefixAt+1 : end : end]}, nil
}

// metaFields reads a record head field by field (see meta).
func (i *Index) metaFields(pos int64) (metadata, error) {
	bad := func(why string) (metadata, error) {
		return metadata{}, corrupt(recordCode(why), pos, "record at %d: %s", pos, why)
	}
	// Only a short read is a truncated record; other I/O errors are not
	// corruption and must never be mistaken for a torn tail.
	badRead := func(err error, why string) (metadata, error) {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return bad(why)
		}
		return metadata{}, fmt.Errorf("legacy index read at %d (%s): %w", pos, why, err)
	}
	if pos < 2 || pos > i.length-9 {
		return bad("position")
	}
	var fixed [8]byte
	if _, err := i.keys.ReadAt(fixed[:], pos); err != nil {
		return badRead(err, "truncated fixed fields")
	}
	previous := readLong5(fixed[1:6])
	if previous != 0 && (previous < 2 || previous >= pos) {
		return bad("previous link/cycle")
	}
	keylen := int(binary.BigEndian.Uint16(fixed[6:8]))
	prefixAt := pos + 8 + int64(keylen)
	if prefixAt >= i.length {
		return bad("truncated key")
	}
	key := make([]byte, keylen)
	if _, err := i.keys.ReadAt(key, pos+8); err != nil {
		return badRead(err, "truncated key")
	}
	var prefix [5]byte
	if _, err := i.keys.ReadAt(prefix[:1], prefixAt); err != nil {
		return badRead(err, "truncated blob prefix")
	}
	width := int64(1)
	n := int64(prefix[0])
	switch prefix[0] {
	case 255:
		width = 3
	case 254:
		width = 5
	}
	if width > 1 {
		if _, err := i.keys.ReadAt(prefix[1:width], prefixAt+1); err != nil {
			return badRead(err, "truncated blob length")
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
	end := prefixAt + width + n
	if end > i.length {
		return bad("truncated blob payload")
	}
	if _, err := i.bucket(key); err != nil {
		return bad("invalid key")
	}
	return metadata{Record: Record{Position: pos, PreviousPosition: previous, NextPosition: end, Key: key, Deleted: fixed[0] != 0, Tombstone: fixed[0] == tombstoneFlag}, valueAt: prefixAt + width, valueBytes: int(n)}, nil
}
func (i *Index) record(m metadata) (Record, error) {
	if m.value != nil {
		m.Value = m.value
		return m.Record, nil
	}
	m.Value = make([]byte, m.valueBytes)
	if _, err := i.keys.ReadAt(m.Value, m.valueAt); err != nil {
		return Record{}, fmt.Errorf("%w: payload: %v", ErrCorrupt, err)
	}
	return m.Record, nil
}

// walkChain visits matching bucket records, latest first, and validates links as read.
func (i *Index) walkChain(bucket int, visit func(metadata) error) error {
	_, err := i.walkChainCounted(bucket, visit)
	return err
}

// walkChainCounted also returns the number of chain records read, deleted
// ones included (Java IndexKeyFile's looping counter).
func (i *Index) walkChainCounted(bucket int, visit func(metadata) error) (int, error) {
	return i.walkChainFrom(bucket, i.head(bucket), i.length, visit)
}

// walkChainFrom walks bucket's chain from pos; next is the position of the
// record linking to pos (i.length at the head) and bounds the first link.
func (i *Index) walkChainFrom(bucket int, pos, next int64, visit func(metadata) error) (int, error) {
	return i.walkChainLimit(bucket, pos, next, -1, time.Time{}, visit)
}

// errChainLimit ends a walkChainLimit walk at its record or time limit.
var errChainLimit = errors.New("chain longer than the walk limit")

// walkChainLimit is walkChainFrom reading at most limit chain records
// (deleted ones counted; a negative limit is no limit) and, with a non-zero
// deadline, stopping once the deadline has passed (checked every 16
// records): a longer walk ends with errChainLimit before the next record is
// read.
func (i *Index) walkChainLimit(bucket int, pos, next int64, limit int, deadline time.Time, visit func(metadata) error) (int, error) {
	looped := 0
	for pos != 0 {
		if looped == limit || (looped > 0 && looped%16 == 0 && !deadline.IsZero() && time.Now().After(deadline)) {
			return looped, errChainLimit
		}
		m, err := i.meta(pos)
		if err != nil {
			return looped, err
		}
		actual, _ := i.bucket(m.Key)
		if m.NextPosition > next || actual != bucket {
			return looped, corrupt(CodeChain, pos, "invalid bucket chain")
		}
		if !m.Deleted {
			if err = visit(m); err != nil {
				return looped, err
			}
		}
		looped++
		next, pos = pos, m.PreviousPosition
	}
	return looped, nil
}

var errFound = errors.New("found")

func (i *Index) Get(key []byte) ([]byte, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if err := i.ready(); err != nil {
		return nil, err
	}
	bucket, err := i.bucket(key)
	if err != nil {
		return nil, err
	}
	var out []byte
	err = i.walkChain(bucket, func(m metadata) error {
		if !bytes.Equal(m.Key, key) {
			return nil
		}
		r, e := i.record(m)
		if e != nil {
			return e
		}
		out = r.Value
		return errFound
	})
	if errors.Is(err, errFound) {
		err = nil
	}
	return out, err
}

// LookupContext is Java IndexKeyFile.get: the newest live record of key, or
// nil. Unlike Get (used where Java calls hasKey) a miss reports the walked
// chain length to the DeepSearchFunc as DeepGet. Cancellation is checked per
// chain record.
func (i *Index) LookupContext(ctx context.Context, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for !i.mu.TryRLock() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	defer i.mu.RUnlock()
	if err := i.ready(); err != nil {
		return nil, err
	}
	bucket, err := i.bucket(key)
	if err != nil {
		return nil, err
	}
	var out []byte
	looped, err := i.walkChainCounted(bucket, func(m metadata) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !bytes.Equal(m.Key, key) {
			return nil
		}
		r, e := i.record(m)
		if e != nil {
			return e
		}
		out = r.Value
		return errFound
	})
	if errors.Is(err, errFound) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	if i.deepSearch != nil {
		i.deepSearch(DeepGet, key, looped)
	}
	return nil, nil
}
func (i *Index) ScanKey(key []byte, visit func(Record) error) error {
	return i.ScanKeyContext(context.Background(), key, visit)
}

// ScanKeyContext checks cancellation even in nonmatching collision records.
func (i *Index) ScanKeyContext(ctx context.Context, key []byte, visit func(Record) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for !i.mu.TryRLock() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	defer i.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := i.ready(); err != nil {
		return err
	}
	if visit == nil {
		return errors.New("nil visitor")
	}
	bucket, err := i.bucket(key)
	if err != nil {
		return err
	}
	return i.walkChain(bucket, func(m metadata) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !bytes.Equal(m.Key, key) {
			return nil
		}
		r, err := i.record(m)
		if err != nil {
			return err
		}
		return visit(r)
	})
}
func (i *Index) GetAll(key []byte) ([][]byte, error) {
	var out [][]byte
	size := 0
	err := i.ScanKey(key, func(r Record) error {
		if len(out) >= MaxResultRecords || size > MaxResultBytes-len(r.Value) {
			return ErrLimit
		}
		size += len(r.Value)
		out = append(out, r.Value)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
func (i *Index) Scan(visit func(Record) error) error {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if err := i.ready(); err != nil {
		return err
	}
	if visit == nil {
		return errors.New("nil visitor")
	}
	for pos := int64(2); pos < i.length; {
		m, err := i.meta(pos)
		if err != nil {
			return err
		}
		if !m.Deleted {
			r, err := i.record(m)
			if err != nil {
				return err
			}
			if err = visit(r); err != nil {
				return err
			}
		}
		pos = m.NextPosition
	}
	return nil
}
func collect(appendTo *[]Record, size *int, r Record) error {
	n := len(r.Key) + len(r.Value)
	if len(*appendTo) >= MaxResultRecords || *size > MaxResultBytes-n {
		return ErrLimit
	}
	*size += n
	*appendTo = append(*appendTo, r)
	return nil
}
func (i *Index) Records() ([]Record, error) {
	var out []Record
	size := 0
	err := i.Scan(func(r Record) error { return collect(&out, &size, r) })
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Update replaces the newest exact key, only when Blob length remains unchanged.
// Java's unchecked bucket-head update and shrinking-record corruption are excluded.
func (i *Index) Update(key, value []byte) (bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.rewritable(); err != nil {
		return false, err
	}
	bucket, err := i.bucket(key)
	if err != nil {
		return false, err
	}
	if value == nil {
		return false, errors.New("nil value")
	}
	if len(value) > MaxValueBytes {
		return false, ErrLimit
	}
	updated := false
	err = i.walkChain(bucket, func(m metadata) error {
		if !bytes.Equal(m.Key, key) {
			return nil
		}
		if m.valueBytes == len(value) {
			if _, err := i.keys.WriteAt(value, m.valueAt); err != nil {
				return err
			}
			updated = true
		}
		return errFound
	})
	if errors.Is(err, errFound) {
		err = nil
	}
	return updated, err
}
func (i *Index) Count() (int32, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if err := i.ready(); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(i.header[4:8])), nil
}

// sync fsyncs the key file, then writes the header pages changed since they
// were last written (dirty, and those an uncommitted SyncPoint snapshot: the
// current bytes supersede the snapshot, see SyncPoint.Commit) and fsyncs the
// header, then records the checkpoint. Caller holds mu.
func (i *Index) sync() error {
	i.headerIO.Lock()
	defer i.headerIO.Unlock()
	if err := i.syncKeys(); err != nil {
		i.cpOK = false
		return err
	}
	if pages := i.unsyncedPages(); len(pages) > 0 {
		if err := i.writePages(pages, i.copyPages(pages)); err != nil {
			i.sumsOK, i.cpOK = false, false // pages stay dirty; the disk state is unknown
			return err
		}
		for _, p := range pages {
			i.sums[p] = crc32.ChecksumIEEE(i.page(p))
		}
		// pendingRefs stay: the outstanding points find headerSeq changed
		// and only mark their pages dirty again (SyncPoint.Commit).
		clear(i.dirtyBits)
		i.headerSeq.Add(1)
	}
	i.dirty = false
	if cp, ok := i.checkpointLocked(i.length); ok {
		i.cpN++
		i.publishCheckpoint(cp, i.cpN)
	}
	return nil
}
func (i *Index) Sync() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.writable(); err != nil {
		return err
	}
	return i.sync()
}

// Close of a read-only handle releases descriptors without fsync or header writes.
func (i *Index) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return nil
	}
	if i.readOnly {
		i.closed = true
		return errors.Join(i.keys.Close(), i.heads.Close())
	}
	err := errors.Join(i.fault, i.sync(), i.keys.Close(), i.heads.Close())
	i.closed = true
	return err
}

// BucketEndpointsContext returns physical oldest/newest time-bucket records,
// including deletion markers, as used by Java IndexTimeFile.getStartEndDataPos.
func (i *Index) BucketEndpointsContext(ctx context.Context, ms int64) (*Record, *Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	for !i.mu.TryRLock() {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	defer i.mu.RUnlock()
	if err := i.ready(); err != nil {
		return nil, nil, err
	}
	if !i.timeIndex || ms <= 0 {
		return nil, nil, errors.New("positive time index key required")
	}
	bucket := i.timeBucket(ms)
	var newest, oldest *Record
	next := i.length
	for pos := i.head(bucket); pos != 0; {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		m, err := i.meta(pos)
		if err != nil {
			return nil, nil, err
		}
		actual, _ := i.bucket(m.Key)
		if m.NextPosition > next || actual != bucket {
			return nil, nil, ErrCorrupt
		}
		r, err := i.record(m)
		if err != nil {
			return nil, nil, err
		}
		if newest == nil {
			newest = &r
		}
		oldest = &r
		next, pos = pos, m.PreviousPosition
	}
	return oldest, newest, nil
}
