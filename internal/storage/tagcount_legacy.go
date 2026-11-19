// Copyright Scouter authors, Apache-2.0. First/Next/Top100 persisted formats.
package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/snowvil/scouter-go/internal/protocol"
)

type tagRecord struct {
	key        []byte
	tag        int64
	value      protocol.Value
	counts     []float32
	pos, link  int64
	hours      [24]int64
	next       bool
	totalCount float32
}

func tagKey(group, name string) int64 {
	return int64(uint64(crc32String(group))<<32 | uint64(crc32String(name)))
}

var crc32Table = crc32.MakeTable(crc32.IEEE)

// crc32String is crc32.ChecksumIEEE([]byte(s)) without the copy, which the
// tag apply paid for every tag of every XLog.
func crc32String(s string) uint32 {
	crc := ^uint32(0)
	for i := 0; i < len(s); i++ {
		crc = crc32Table[byte(crc)^s[i]] ^ crc>>8
	}
	return ^crc
}
func tagRecordKey(tag int64, value protocol.Value, limits protocol.Limits) ([]byte, error) {
	e := protocol.NewEncoder(limits)
	e.Int64(tag)
	e.Value(value)
	b, err := e.Bytes()
	if err != nil {
		return nil, err
	}
	if len(b) > 32767 {
		return nil, ErrLimit
	}
	return b, nil
}
func (s *Store) tagReadAt(ctx context.Context, path string, b []byte, pos int64) error {
	if err := s.tagSafePath(path); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := s.tagFile(path, false)
	if err != nil {
		return err
	}
	if f == nil {
		return ErrCorrupt
	}
	_, err = f.ReadAt(b, pos)
	if err != nil {
		return errors.Join(ErrCorrupt, err)
	}
	return ctx.Err()
}
func (s *Store) tagWriteAt(ctx context.Context, path string, b []byte, pos int64) error {
	if err := s.tagSafePath(path); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := s.tagFile(path, true)
	if err != nil {
		return err
	}
	var n int
	if hook := s.tagWriteHook; hook != nil {
		err = hook(path, pos, len(b))
	}
	if err == nil {
		n, err = f.WriteAt(b, pos)
	}
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err != nil {
		delete(s.tags.ends, path)
		return err
	}
	if end, ok := s.tags.ends[path]; ok && pos+int64(n) > end {
		s.tags.ends[path] = pos + int64(n)
	}
	s.tags.mark(&s.tags.dirty, path)
	return nil
}

// tagSizeLocked returns the size of the open tag file f at path. The Store is
// the only writer, so the size is kept in s.tags.ends (dropped with the handle and
// on a write error, maintained by tagWriteAt) instead of an fstat per record
// (a measured flush hot spot). fresh reports a size just read from the file;
// headers are validated once per handle (tagState.valid).
func (s *Store) tagSizeLocked(path string, f *os.File) (size int64, fresh bool, err error) {
	if n, ok := s.tags.ends[path]; ok {
		return n, false, nil
	}
	st, err := f.Stat()
	if err != nil {
		return 0, false, err
	}
	if s.tags.ends == nil {
		s.tags.ends = make(map[string]int64)
	}
	s.tags.ends[path] = st.Size()
	return st.Size(), true, nil
}

// tagKeyFileLocked validates a tag key file header and returns its size;
// size 0 means the file does not exist or is empty (its header write failed,
// for example on ENOSPC; tagEnsureFile writes it again, as the original
// writes the header of a zero-length key file on open).
func (s *Store) tagKeyFileLocked(ctx context.Context, path string) (int64, error) {
	if err := s.tagSafePath(path + ".kfile"); err != nil {
		return 0, err
	}
	keyfile := path + ".kfile"
	f, err := s.tagFile(keyfile, false)
	if err != nil || f == nil {
		return 0, err
	}
	size, _, err := s.tagSizeLocked(keyfile, f)
	if err != nil || size == 0 {
		return 0, err
	}
	if size < 2 {
		return 0, ErrCorrupt
	}
	if !s.tags.valid[keyfile] { // header checked once per handle
		var magic [2]byte
		if err = s.tagReadAt(ctx, keyfile, magic[:], 0); err != nil {
			return 0, err
		}
		if magic != [2]byte{0xca, 0xfe} {
			delete(s.tags.ends, keyfile)
			return 0, ErrCorrupt
		}
		s.tags.mark(&s.tags.valid, keyfile)
	}
	return size, nil
}

func tagFixedBytes(first bool) int {
	if first {
		return 1 + 5 + 1440*4
	}
	return 5 + 4 + 24*5
}

// tagKeyReadAhead is the key bytes read together with a record head.
const tagKeyReadAhead = 64

// tagReadRecordLocked parses the record at pos of a key file of size bytes and
// returns its on-disk length.
func (s *Store) tagReadRecordLocked(ctx context.Context, path string, first bool, pos, size int64) (tagRecord, bool, int, error) {
	return s.tagReadRecordKeyLocked(ctx, path, first, pos, size, nil, true)
}

// tagReadRecordKeyLocked is tagReadRecordLocked for a chain walk looking for
// want: a record with another key comes back with its key, link and deleted
// flag only (no counts are decoded or allocated). A record of want comes back
// whole when full, else with its head only (link, deleted, and for Next the
// total and hour positions; no counts). want nil reads everything.
func (s *Store) tagReadRecordKeyLocked(ctx context.Context, path string, first bool, pos, size int64, want []byte, full bool) (tagRecord, bool, int, error) {
	keyfile := path + ".kfile"
	fixed := tagFixedBytes(first)
	if pos < 2 || pos+int64(fixed+2) > size {
		return tagRecord{}, false, 0, ErrCorrupt
	}
	// One read covers the head and a typical key (tagKeyReadAhead); a longer
	// key takes a second read.
	headLen := int(min(int64(fixed+2+tagKeyReadAhead), size-pos))
	if cap(s.tags.readBuf) < headLen {
		s.tags.readBuf = make([]byte, fixed+2+tagKeyReadAhead)
	}
	head := s.tags.readBuf[:headLen] // decoded below; never returned
	if err := s.tagReadAt(ctx, keyfile, head, pos); err != nil {
		return tagRecord{}, false, 0, err
	}
	n := int(int16(binary.BigEndian.Uint16(head[fixed:])))
	if n < 9 || pos+int64(fixed+2+n) > size {
		return tagRecord{}, false, 0, ErrCorrupt
	}
	if fixed+2+n > s.config.MaxQueryBytes {
		return tagRecord{}, false, 0, ErrLimit
	}
	key := make([]byte, n)
	if got := copy(key, head[fixed+2:]); got < n {
		if err := s.tagReadAt(ctx, keyfile, key[got:], pos+int64(fixed+2+got)); err != nil {
			return tagRecord{}, false, 0, err
		}
	}
	head = head[:fixed+2]
	if want != nil && (!full || !bytes.Equal(key, want)) {
		other := tagRecord{key: key, pos: pos, next: !first}
		deleted := first && head[0] != 0
		linkAt := 0
		if first {
			linkAt = 1 // after the deleted flag
		}
		other.link = readLong5(head[linkAt:])
		if other.link < 0 || other.link >= size || other.link == pos {
			return tagRecord{}, false, 0, ErrCorrupt
		}
		if !first && bytes.Equal(key, want) {
			other.totalCount = math.Float32frombits(binary.BigEndian.Uint32(head[5:]))
			for i := range other.hours {
				other.hours[i] = readLong5(head[9+i*5:])
			}
		}
		return other, deleted, fixed + 2 + n, nil
	}
	d := protocol.NewDecoder(key, s.config.CodecLimits)
	r := tagRecord{key: key, tag: d.Int64(), pos: pos, counts: make([]float32, 1440)}
	r.value = d.Value()
	if d.Err() != nil || d.Remaining() != 0 {
		return tagRecord{}, false, 0, ErrCorrupt
	}
	hd := protocol.NewDecoder(head[:fixed], s.config.CodecLimits)
	deleted := false
	if first {
		deleted = hd.Bool()
	}
	r.link = hd.Long5()
	if r.link < 0 || r.link >= size || r.link == pos {
		return tagRecord{}, false, 0, ErrCorrupt
	}
	if first {
		for i := range r.counts {
			r.counts[i] = hd.Float32()
		}
	} else {
		r.next = true
		r.totalCount = hd.Float32()
		for i := range r.hours {
			r.hours[i] = hd.Long5()
		}
		dataf, err := s.tagFile(path+".data", false)
		if err != nil {
			return tagRecord{}, false, 0, err
		}
		if dataf == nil {
			return tagRecord{}, false, 0, ErrCorrupt
		}
		dsize, _, err := s.tagSizeLocked(path+".data", dataf)
		if err != nil {
			return tagRecord{}, false, 0, err
		}
		if dsize < 2 {
			return tagRecord{}, false, 0, ErrCorrupt
		}
		if !s.tags.valid[path+".data"] {
			var magic [2]byte
			if err = s.tagReadAt(ctx, path+".data", magic[:], 0); err != nil {
				return tagRecord{}, false, 0, err
			}
			if magic != [2]byte{0xca, 0xfe} {
				delete(s.tags.ends, path+".data")
				return tagRecord{}, false, 0, ErrCorrupt
			}
			s.tags.mark(&s.tags.valid, path+".data")
		}
		for hour, vpos := range r.hours {
			if vpos == 0 {
				continue
			}
			if vpos < 2 || vpos > dsize-240 {
				return tagRecord{}, false, 0, ErrCorrupt
			}
			b := make([]byte, 240)
			if err = s.tagReadAt(ctx, path+".data", b, vpos); err != nil {
				return tagRecord{}, false, 0, err
			}
			for min := 0; min < 60; min++ {
				r.counts[hour*60+min] = math.Float32frombits(binary.BigEndian.Uint32(b[min*4:]))
			}
		}
	}
	for _, v := range r.counts {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return tagRecord{}, false, 0, ErrCorrupt
		}
	}
	return r, deleted, fixed + 2 + n, nil
}

// tagScanMaxRecords bounds one streaming key-file scan (deleted records
// included). Memory does not grow with it: a scan holds one record at a time.
// It bounds the time spent under Store.mu on a huge or looping file.
const tagScanMaxRecords = 1 << 24

// tagScanLocked streams the records of a key file in file order, the order of
// the original IndexFile.read (next/IndexFile.scala:148-168), skipping deleted
// records. fn owns each record; every record is freshly allocated. A non-nil
// fn error stops the scan and is returned. More than maxRecords records, a
// corrupt record or context cancellation return an explicit error.
func (s *Store) tagScanLocked(ctx context.Context, path string, first bool, maxRecords int, fn func(tagRecord) error) error {
	c := tagScanCursor{path: path, first: first}
	return s.tagScanStepLocked(ctx, &c, maxRecords, nil, fn)
}

// tagScanCursor resumes a tagScanLocked file-order scan across Store gate
// holds. The key file size is read once, by the first step: records appended
// later are not seen. Records never move (an update rewrites a record in place
// with the same length and link), so pos stays a record boundary between holds;
// each record, with its Next hour blocks, is read within one hold.
type tagScanCursor struct {
	path      string
	first     bool
	size, pos int64
	scanned   int
	started   bool
	done      bool
}

// tagScanStepLocked reads records of c in file order until the file ends
// (c.done) or, when hold is not nil, the hold is spent (at least one record per
// call). Limits and errors are those of tagScanLocked.
func (s *Store) tagScanStepLocked(ctx context.Context, c *tagScanCursor, maxRecords int, hold *tagHold, fn func(tagRecord) error) error {
	if !c.started {
		size, err := s.tagKeyFileLocked(ctx, c.path)
		if err != nil {
			return err
		}
		c.started, c.size, c.pos = true, size, 2
	}
	for c.pos < c.size {
		if c.scanned >= maxRecords {
			return ErrLimit
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		r, deleted, length, err := s.tagReadRecordLocked(ctx, c.path, c.first, c.pos, c.size)
		if err != nil {
			return err
		}
		c.scanned++
		c.pos += int64(length)
		if !deleted {
			if err = fn(r); err != nil {
				return err
			}
		}
		if hold != nil && hold.spend() {
			c.done = c.pos >= c.size
			return nil
		}
	}
	c.done = true
	return nil
}

// tagMaxChain bounds one hash-chain walk, as legacyindex bounds its chains.
const tagMaxChain = 100000

// tagFindRecordLocked returns the newest non-deleted record of key through the
// .hfile bucket chain, the same record a full scan keeps (last in file order).
// Flush uses it so cost follows pending keys, not file size.
func (s *Store) tagFindRecordLocked(ctx context.Context, path string, first bool, key []byte) (tagRecord, bool, error) {
	return s.tagFindRecordModeLocked(ctx, path, first, key, true)
}

// tagLocateRecordLocked is tagFindRecordLocked returning the record head only
// (tagReadRecordKeyLocked with full false), for an in-place update.
func (s *Store) tagLocateRecordLocked(ctx context.Context, path string, first bool, key []byte) (tagRecord, bool, error) {
	return s.tagFindRecordModeLocked(ctx, path, first, key, false)
}
func (s *Store) tagFindRecordModeLocked(ctx context.Context, path string, first bool, key []byte, full bool) (tagRecord, bool, error) {
	size, err := s.tagKeyFileLocked(ctx, path)
	if err != nil || size == 0 {
		return tagRecord{}, false, err
	}
	hashfile := path + ".hfile"
	if err = s.tagSafePath(hashfile); err != nil {
		return tagRecord{}, false, err
	}
	hf, err := s.tagFile(hashfile, false)
	if err != nil {
		return tagRecord{}, false, err
	}
	if hf == nil {
		return tagRecord{}, false, ErrCorrupt
	}
	hsize, _, err := s.tagSizeLocked(hashfile, hf)
	if err != nil {
		return tagRecord{}, false, err
	}
	if hsize < 1029 || hsize > int64(1024+(64<<20)) {
		return tagRecord{}, false, ErrCorrupt
	}
	capacity := (hsize - 1024) / 5
	var b [5]byte
	if err = s.tagReadAt(ctx, hashfile, b[:], int64(crc32.ChecksumIEEE(key)&0x7fffffff)%capacity*5+1024); err != nil {
		return tagRecord{}, false, err
	}
	pos, err := dataOffset(b[:])
	if err != nil {
		return tagRecord{}, false, err
	}
	for steps := 0; pos != 0; steps++ {
		if steps >= tagMaxChain {
			return tagRecord{}, false, ErrLimit
		}
		if err = ctx.Err(); err != nil {
			return tagRecord{}, false, err
		}
		r, deleted, _, err := s.tagReadRecordKeyLocked(ctx, path, first, pos, size, key, full)
		if err != nil {
			return tagRecord{}, false, err
		}
		if !deleted && bytes.Equal(r.key, key) {
			return r, true, nil
		}
		if r.link >= pos {
			return tagRecord{}, false, ErrCorrupt // chains only point backwards
		}
		pos = r.link
	}
	return tagRecord{}, false, nil
}

func (s *Store) tagEnsureFile(ctx context.Context, path string) error {
	if err := s.tagSafePath(path); err != nil {
		return err
	}
	f, err := s.tagFile(path, true)
	if err != nil {
		return err
	}
	size, _, err := s.tagSizeLocked(path, f)
	if err != nil {
		return err
	}
	if size == 0 {
		if err := s.tagWriteAt(ctx, path, []byte{0xca, 0xfe}, 0); err != nil {
			return err
		}
		s.tags.mark(&s.tags.valid, path)
	}
	return nil
}

// tagHashPrev returns the bucket head of key (the newest record of the
// chain, 0 for none) and the bucket's offset in the .hfile, creating the hash
// file on first use. Nothing else is written: the new head goes in only after
// the record it points to (tagHashLink), as IndexFile.put appends the key
// record before hashFile.put.
func (s *Store) tagHashPrev(ctx context.Context, path string, key []byte) (prev, offset int64, err error) {
	hashfile := path + ".hfile"
	if err := s.tagSafePath(hashfile); err != nil {
		return 0, 0, err
	}
	f, err := s.tagFile(hashfile, true)
	if err != nil {
		return 0, 0, err
	}
	size, _, err := s.tagSizeLocked(hashfile, f)
	if err != nil {
		return 0, 0, err
	}
	if size == 0 {
		size = int64(1024 + s.config.HashSizeBytes)
		if err = f.Truncate(size); err != nil {
			delete(s.tags.ends, hashfile)
			return 0, 0, err
		}
		s.tags.ends[hashfile] = size
		s.tags.mark(&s.tags.dirty, hashfile)
		if err = s.tagWriteAt(ctx, hashfile, []byte{0xca, 0xfe}, 0); err != nil {
			// Back to empty, so the next use creates it again instead of
			// using a hash file without its header.
			delete(s.tags.ends, hashfile)
			return 0, 0, errors.Join(err, f.Truncate(0))
		}
	}
	if size < 1029 || size > int64(1024+(64<<20)) {
		return 0, 0, ErrCorrupt
	}
	capacity := (size - 1024) / 5
	offset = int64(crc32.ChecksumIEEE(key)&0x7fffffff)%capacity*5 + 1024
	var b [5]byte
	if err = s.tagReadAt(ctx, hashfile, b[:], offset); err != nil {
		return 0, 0, err
	}
	prev, err = dataOffset(b[:])
	return prev, offset, err
}

// tagHashLink makes newPos the head of the bucket at offset, counting a
// bucket that was empty (prev 0) in the header.
func (s *Store) tagHashLink(ctx context.Context, path string, offset, prev, newPos int64) error {
	hashfile := path + ".hfile"
	if prev == 0 {
		var count [4]byte
		if err := s.tagReadAt(ctx, hashfile, count[:], 4); err != nil {
			return err
		}
		binary.BigEndian.PutUint32(count[:], binary.BigEndian.Uint32(count[:])+1)
		if err := s.tagWriteAt(ctx, hashfile, count[:], 4); err != nil {
			return err
		}
	}
	return s.tagWriteAt(ctx, hashfile, offsetBytes(newPos), offset)
}

// tagSaveRecord writes r: a new record (pos 0) is appended to the key file
// and only then linked from its hash bucket; an existing one is rewritten in
// place. Once started it is not cancelled (ctx is not consulted), so a
// shutdown never leaves a bucket pointing past the key file or a record half
// written. wrote reports whether counts reached the files before an error: a
// new record's data blocks and an appended key record that fails are cut off
// again (nothing counted, the delta can be retried); after the key record or
// any rewrite of an existing record the counts are visible.
func (s *Store) tagSaveRecord(_ context.Context, path string, first bool, r tagRecord) (wrote bool, err error) {
	ctx := context.Background()
	if err := s.tagEnsureFile(ctx, path+".kfile"); err != nil {
		return false, err
	}
	var bucket, prev int64
	dataEnd := int64(-1) // data size before this record's first appended block
	added := r.pos == 0
	if added {
		f, err := s.tagFile(path+".kfile", false)
		if err != nil {
			return false, err
		}
		if r.pos, _, err = s.tagSizeLocked(path+".kfile", f); err != nil {
			return false, err
		}
		if prev, bucket, err = s.tagHashPrev(ctx, path, r.key); err != nil {
			return false, err
		}
		r.link = prev
	}
	e := protocol.NewEncoder(s.config.CodecLimits)
	if first {
		e.Bool(false)
	}
	e.Long5(r.link)
	if first {
		for _, n := range r.counts {
			e.Float32(n)
		}
	} else {
		data := path + ".data"
		if err := s.tagEnsureFile(ctx, data); err != nil {
			return false, err
		}
		for h := 0; h < 24; h++ {
			nonzero := false
			for _, v := range r.counts[h*60 : h*60+60] {
				if v != 0 {
					nonzero = true
				}
			}
			if !nonzero && r.hours[h] == 0 {
				continue
			}
			appended := r.hours[h] == 0
			if appended {
				f, err := s.tagFile(data, false)
				if err != nil {
					return wrote, err
				}
				if r.hours[h], _, err = s.tagSizeLocked(data, f); err != nil {
					return wrote, err
				}
				if dataEnd < 0 {
					dataEnd = r.hours[h]
				}
			}
			b := make([]byte, 240)
			for i, v := range r.counts[h*60 : h*60+60] {
				binary.BigEndian.PutUint32(b[i*4:], math.Float32bits(v))
			}
			if err := s.tagWriteAt(ctx, data, b, r.hours[h]); err != nil {
				if dataEnd >= 0 && !wrote {
					err = errors.Join(err, s.tagCutLocked(data, dataEnd))
				}
				return wrote, err
			}
			if !appended {
				wrote = true // an existing block now holds the new counts
			}
		}
		e.Float32(r.totalCount)
		for _, pos := range r.hours {
			e.Long5(pos)
		}
	}
	e.Int16(int16(len(r.key)))
	for _, b := range r.key {
		e.Byte(b)
	}
	b, err := e.Bytes()
	if err != nil {
		return wrote, err
	}
	if err = s.tagWriteAt(ctx, path+".kfile", b, r.pos); err != nil {
		if added {
			err = errors.Join(err, s.tagCutLocked(path+".kfile", r.pos)) // no half record at the end
			if dataEnd >= 0 {
				err = errors.Join(err, s.tagCutLocked(path+".data", dataEnd)) // nor orphan blocks
			}
		}
		return wrote, err
	}
	if !added {
		return true, nil
	}
	// The record is in the key file (a scan sees it); a failed link leaves
	// it unreachable through the hash but counted.
	return true, s.tagHashLink(ctx, path, bucket, prev, r.pos)
}

// tagCutLocked truncates a tag file back to size after a failed append, so
// the next append does not land behind a half record or after an orphan
// block. The size cache is dropped and re-read either way; a failed truncate
// is returned with the write error.
func (s *Store) tagCutLocked(path string, size int64) error {
	delete(s.tags.ends, path)
	if f := s.tags.files[path]; f != nil {
		return f.Truncate(size)
	}
	return nil
}

func (s *Store) tagReadCacheLocked(ctx context.Context, path string) (TagValueTotal, bool, error) {
	if err := s.tagSafePath(path); err != nil {
		return TagValueTotal{}, false, err
	}
	f, err := s.tagFile(path, false)
	if err != nil || f == nil {
		return TagValueTotal{}, false, err
	}
	size, _, err := s.tagSizeLocked(path, f)
	if err != nil {
		return TagValueTotal{}, false, err
	}
	if size < 12 {
		return TagValueTotal{}, false, ErrCorrupt
	}
	if size > int64(s.config.MaxQueryBytes) {
		return TagValueTotal{}, false, ErrLimit
	}
	b := make([]byte, size)
	if err = s.tagReadAt(ctx, path, b, 0); err != nil {
		return TagValueTotal{}, false, err
	}
	d := protocol.NewDecoder(b, s.config.CodecLimits)
	out := TagValueTotal{HowManyValues: int(d.Int32()), TotalCount: d.Float32()}
	if math.IsNaN(float64(out.TotalCount)) || math.IsInf(float64(out.TotalCount), 0) {
		return TagValueTotal{}, false, ErrCorrupt
	}
	n := int(d.Int32())
	if n < 0 || n > 100 || out.HowManyValues < n {
		return TagValueTotal{}, false, ErrCorrupt
	}
	for i := 0; i < n; i++ {
		v := TagValueCount{Value: d.Value(), Count: d.Float64()}
		if math.IsNaN(v.Count) || math.IsInf(v.Count, 0) {
			return TagValueTotal{}, false, ErrCorrupt
		}
		out.Values = append(out.Values, v)
	}
	if d.Err() != nil || d.Remaining() != 0 {
		return TagValueTotal{}, false, ErrCorrupt
	}
	return out, true, ctx.Err()
}
func tagSum(values []float32) float32 {
	var total float32
	for _, v := range values {
		total += v
	}
	return total
}
func tagCachePath(path string, tag int64) string {
	return filepath.Join(path, "top100."+formatTagKey(tag)+".dat")
}
func tagFileExists(path string) bool { _, err := os.Stat(path); return err == nil }

// tagUpdateRecordLocked adds delta to the existing record r (located head
// only) in place, touching only the hours the delta holds: a First record's
// 240-byte hour slices in the key file, a Next record's hour blocks in the
// data file and then its head (total and hour positions). Minutes and the
// total are summed in the order tagSaveRecord sums them, so the files are
// byte-identical to rewriting the whole record. Like tagSaveRecord it is not
// cancelled once started and reports whether counts reached the files: a
// Next record's new hours are appended first (in ascending hour order, the
// order tagSaveRecord appends them; an in-place add does not move the data
// file end), so running out of space fails before any count changes.
func (s *Store) tagUpdateRecordLocked(path string, first bool, r tagRecord, delta *tagDelta) (wrote bool, err error) {
	ctx := context.Background()
	var block [240]byte
	add := func(file string, at int64, counts *[60]float32) error {
		if err := s.tagReadAt(ctx, file, block[:], at); err != nil {
			return err
		}
		for m, n := range counts {
			v := math.Float32frombits(binary.BigEndian.Uint32(block[m*4:]))
			if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
				return ErrCorrupt
			}
			binary.BigEndian.PutUint32(block[m*4:], math.Float32bits(v+n))
		}
		return s.tagWriteAt(ctx, file, block[:], at)
	}
	if first {
		for h, counts := range delta.hours {
			if counts == nil {
				continue
			}
			if err := add(path+".kfile", r.pos+1+5+int64(h*240), counts); err != nil {
				return wrote, err
			}
			wrote = true
		}
		return wrote, nil
	}
	data := path + ".data"
	dsize, err := s.tagDataSizeLocked(ctx, data, r.hours)
	if err != nil {
		return false, err
	}
	for h, counts := range delta.hours { // validate before writing anything
		if counts != nil && r.hours[h] != 0 && (r.hours[h] < 2 || r.hours[h] > dsize-240) {
			return false, ErrCorrupt
		}
	}
	dataEnd := dsize
	for h, counts := range delta.hours { // 1: new hours, appended
		if counts == nil || r.hours[h] != 0 {
			continue
		}
		nonzero := false
		for _, v := range counts {
			if v != 0 {
				nonzero = true
			}
		}
		if !nonzero {
			continue // no block for an hour without counts, as tagSaveRecord
		}
		r.hours[h] = dataEnd
		for m, v := range counts {
			binary.BigEndian.PutUint32(block[m*4:], math.Float32bits(v))
		}
		if err = s.tagWriteAt(ctx, data, block[:], dataEnd); err != nil {
			return false, errors.Join(err, s.tagCutLocked(data, dsize))
		}
		dataEnd += 240
	}
	// A failure before any count changed cuts the blocks appended above, so
	// the retry does not leave them behind.
	failed := func(err error) (bool, error) {
		if !wrote && dataEnd > dsize {
			err = errors.Join(err, s.tagCutLocked(data, dsize))
		}
		return wrote, err
	}
	for h, counts := range delta.hours { // 2: existing hours, added in place
		if counts == nil || r.hours[h] == 0 || r.hours[h] >= dsize {
			continue
		}
		if err = add(data, r.hours[h], counts); err != nil {
			return failed(err)
		}
		wrote = true
	}
	for _, counts := range delta.hours { // the total in hour order
		if counts != nil {
			r.totalCount += tagSum(counts[:])
		}
	}
	e := protocol.NewEncoder(s.config.CodecLimits)
	e.Long5(r.link)
	e.Float32(r.totalCount)
	for _, pos := range r.hours {
		e.Long5(pos)
	}
	head, err := e.Bytes()
	if err != nil {
		return failed(err)
	}
	if err = s.tagWriteAt(ctx, path+".kfile", head, r.pos); err != nil {
		return failed(err)
	}
	return true, nil
}

// tagDataSizeLocked returns the size of a Next data file with its header
// checked once per handle. A missing file is created only for a record
// without hour blocks; one with blocks makes it ErrCorrupt.
func (s *Store) tagDataSizeLocked(ctx context.Context, data string, hours [24]int64) (int64, error) {
	if err := s.tagSafePath(data); err != nil {
		return 0, err
	}
	f, err := s.tagFile(data, false)
	if err != nil {
		return 0, err
	}
	if f != nil { // an empty data file is one whose header write failed
		if size, _, err := s.tagSizeLocked(data, f); err == nil && size == 0 {
			f = nil
		}
	}
	if f == nil {
		for _, h := range hours {
			if h != 0 {
				return 0, ErrCorrupt
			}
		}
		if err = s.tagEnsureFile(ctx, data); err != nil {
			return 0, err
		}
		if f, err = s.tagFile(data, false); err != nil {
			return 0, err
		}
	}
	size, _, err := s.tagSizeLocked(data, f)
	if err != nil {
		return 0, err
	}
	if size < 2 {
		return 0, ErrCorrupt
	}
	if !s.tags.valid[data] {
		var magic [2]byte
		if err = s.tagReadAt(ctx, data, magic[:], 0); err != nil {
			return 0, err
		}
		if magic != [2]byte{0xca, 0xfe} {
			delete(s.tags.ends, data)
			return 0, ErrCorrupt
		}
		s.tags.mark(&s.tags.valid, data)
	}
	return size, nil
}
