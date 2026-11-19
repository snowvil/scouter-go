// Copyright Scouter authors, Apache-2.0. Read-only tag count chain check.
package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// TagChainProblem is one inconsistency of a tag count key/hash file pair.
type TagChainProblem struct {
	// torn_tail, link_mismatch, head_past_end, head_mismatch, dangling_head,
	// count_mismatch, missing_hash, bad_hash, bad_header. head_past_end makes
	// lookups of that bucket fail (ErrCorrupt); the others make them miss
	// records. A copy of a running server or one taken after a crash also
	// shows them: the original keeps the hash table in memory and saves it
	// every 4s (MemHashBlock).
	Kind   string
	Pos    int64 // key file position (hash file offset for heads), -1 for the file
	Detail string
}

// TagChainReport is the result of VerifyTagCountFiles for one pair.
type TagChainReport struct {
	Records, Buckets int64
	Problems         []TagChainProblem
	MoreProblems     int64 // problems past the first tagChainProblemMax
}

const tagChainProblemMax = 20

func (r *TagChainReport) add(kind string, pos int64, format string, args ...any) {
	if len(r.Problems) >= tagChainProblemMax {
		r.MoreProblems++
		return
	}
	r.Problems = append(r.Problems, TagChainProblem{kind, pos, fmt.Sprintf(format, args...)})
}

// VerifyTagCountFiles checks the First (first) or Next key file base+".kfile"
// against base+".hfile" without writing. Records are only appended and each
// new record links to its bucket's head, as IndexFile.put does in the
// original and tagSaveRecord does here, so in a consistent pair every
// record's link is the previous record of its bucket in file order, every
// bucket head is the last record of its bucket and the header counts the
// non-empty buckets. A torn write (a head written before its record, a record
// cut short) breaks one of these. A missing pair is not a problem.
func VerifyTagCountFiles(base string, first bool) (*TagChainReport, error) {
	rep := &TagChainReport{}
	kf, err := os.Open(base + ".kfile")
	if os.IsNotExist(err) {
		return rep, nil
	}
	if err != nil {
		return nil, err
	}
	defer kf.Close()
	hf, err := os.Open(base + ".hfile")
	if err != nil {
		if os.IsNotExist(err) {
			rep.add("missing_hash", -1, "key file without hash file")
			return rep, nil
		}
		return nil, err
	}
	defer hf.Close()
	hst, err := hf.Stat()
	if err != nil {
		return nil, err
	}
	hsize := hst.Size()
	if hsize < 1029 || hsize > int64(1024+(64<<20)) {
		rep.add("bad_hash", -1, "hash file size %d", hsize)
		return rep, nil
	}
	capacity := (hsize - 1024) / 5
	kst, err := kf.Stat()
	if err != nil {
		return nil, err
	}
	ksize := kst.Size()
	r := bufio.NewReaderSize(kf, 1<<20)
	var magic [2]byte
	if _, err = io.ReadFull(r, magic[:]); err != nil || magic != [2]byte{0xca, 0xfe} {
		rep.add("bad_header", 0, "key file magic")
		return rep, nil
	}
	fixed := tagFixedBytes(first)
	linkAt := 0
	if first {
		linkAt = 1
	}
	last := map[int64]int64{} // bucket -> last record position
	head := make([]byte, fixed+2)
	for pos := int64(2); pos < ksize; {
		if pos+int64(fixed+2) > ksize {
			rep.add("torn_tail", pos, "record head past the key file end (%d bytes left)", ksize-pos)
			break
		}
		if _, err = io.ReadFull(r, head); err != nil {
			return nil, err
		}
		n := int(int16(binary.BigEndian.Uint16(head[fixed:])))
		if n < 9 || pos+int64(fixed+2+n) > ksize {
			rep.add("torn_tail", pos, "key of %d bytes past the key file end", n)
			break
		}
		key := make([]byte, n)
		if _, err = io.ReadFull(r, key); err != nil {
			return nil, err
		}
		link := readLong5(head[linkAt:])
		bucket := int64(crc32.ChecksumIEEE(key)&0x7fffffff) % capacity
		if want := last[bucket]; link != want {
			rep.add("link_mismatch", pos, "link %d, previous record of the bucket %d", link, want)
		}
		last[bucket] = pos
		rep.Records++
		pos += int64(fixed + 2 + n)
	}
	buckets := make([]byte, hsize-1024)
	if _, err = hf.ReadAt(buckets, 1024); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	var hdr [8]byte
	if _, err = hf.ReadAt(hdr[:], 0); err != nil {
		return nil, err
	}
	for b := int64(0); b < capacity; b++ {
		h := readLong5(buckets[b*5:])
		want, used := last[b]
		switch {
		case h == 0 && !used:
		case h >= ksize:
			rep.add("head_past_end", 1024+b*5, "bucket head %d past the key file end %d", h, ksize)
		case h != 0 && !used:
			rep.add("dangling_head", 1024+b*5, "bucket head %d with no record of the bucket", h)
		case h != want:
			rep.add("head_mismatch", 1024+b*5, "bucket head %d, last record of the bucket %d", h, want)
		}
		if h != 0 || used {
			rep.Buckets++
		}
	}
	if count := int64(binary.BigEndian.Uint32(hdr[4:])); count != int64(len(last)) {
		rep.add("count_mismatch", 4, "header counts %d buckets, %d hold records", count, len(last))
	}
	return rep, nil
}

func readLong5(b []byte) int64 {
	return int64(int8(b[0]))<<32 | int64(b[1])<<24 | int64(b[2])<<16 | int64(b[3])<<8 | int64(b[4])
}
