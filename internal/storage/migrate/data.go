// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
package migrate

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/snowvil/scouter-go/internal/storage"
)

const (
	maxRecordBytes = 32 << 20 // storage.Config.MaxRecordBytes upper bound
	zipBlockBytes  = 32 << 20 // GZipCtr.BLOCK_MAX_SIZE
	maxZipChain    = 100000
	maxLong5       = 1<<39 - 1
	zipCacheBlocks = 2
)

// dataError is a classified payload problem at a data offset.
type dataError struct {
	code   string
	offset int64
	detail string
}

func (e *dataError) Error() string { return e.code + ": " + e.detail }
func dataErr(code string, offset int64, format string, args ...any) error {
	return &dataError{code: code, offset: offset, detail: fmt.Sprintf(format, args...)}
}

func readLong5(b []byte) int64 {
	v := int64(int8(b[0]))
	for _, n := range b[1:5] {
		v = v<<8 | int64(n)
	}
	return v
}

// dataFile is one length-prefixed record file opened O_RDONLY.
type dataFile struct {
	rel     string
	f       *os.File // nil while evicted from the bounded descriptor set
	abs     string
	size    int64
	prefix  int
	refs    int64 // references from the primary index
	maxEnd  int64 // end of the furthest referenced record
	missing bool
}

func openDataFile(root, rel string, prefix int) (*dataFile, error) {
	d := &dataFile{rel: rel, prefix: prefix, abs: filepath.Join(root, filepath.FromSlash(rel))}
	f, err := os.OpenFile(d.abs, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		d.missing = true
		return d, nil
	}
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	d.f, d.size = f, st.Size()
	return d, nil
}
func (d *dataFile) close() error {
	if d.f == nil {
		return nil
	}
	err := d.f.Close()
	d.f = nil
	return err
}
func (d *dataFile) length(head []byte) int64 {
	if d.prefix == 2 {
		return int64(int16(binary.BigEndian.Uint16(head)))
	}
	return int64(int32(binary.BigEndian.Uint32(head)))
}

// read returns the payload at pos, checking every extent against EOF before allocation.
func (d *dataFile) read(pos int64) ([]byte, error) {
	if d.missing {
		return nil, dataErr("data.missing", pos, "%s does not exist", d.rel)
	}
	if pos < 0 || pos > d.size-int64(d.prefix) {
		return nil, dataErr("data.offset_eof", pos, "length prefix at %d exceeds file size %d", pos, d.size)
	}
	head := make([]byte, d.prefix)
	if _, err := d.f.ReadAt(head, pos); err != nil {
		return nil, err
	}
	n := d.length(head)
	if n < 0 || n > maxRecordBytes {
		return nil, dataErr("data.length", pos, "record length %d", n)
	}
	end := pos + int64(d.prefix) + n
	if end > d.size {
		return nil, dataErr("data.offset_eof", pos, "record end %d exceeds file size %d", end, d.size)
	}
	b := make([]byte, n)
	if _, err := d.f.ReadAt(b, pos+int64(d.prefix)); err != nil {
		return nil, err
	}
	if end > d.maxEnd {
		d.maxEnd = end
	}
	return b, nil
}

// dataWalk is a streaming sequential parse; memory is independent of file size.
type dataWalk struct {
	records     int64
	afterMaxEnd int64 // complete records starting at or after maxEnd
	stop        int64 // first byte not covered by a complete record
	reason      string
}

func (d *dataFile) walk(ctx context.Context) (dataWalk, error) {
	var w dataWalk
	if d.missing {
		return w, nil
	}
	r := bufio.NewReaderSize(io.NewSectionReader(d.f, 0, d.size), 1<<20)
	head := make([]byte, d.prefix)
	for pos := int64(0); ; {
		if w.records%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return w, err
			}
		}
		w.stop = pos
		if pos == d.size {
			return w, nil
		}
		if d.size-pos < int64(d.prefix) {
			w.reason = fmt.Sprintf("%d trailing bytes shorter than the length prefix", d.size-pos)
			return w, nil
		}
		if _, err := io.ReadFull(r, head); err != nil {
			return w, err
		}
		n := d.length(head)
		if n < 0 || n > maxRecordBytes {
			w.reason = fmt.Sprintf("invalid record length %d", n)
			return w, nil
		}
		if n > d.size-pos-int64(d.prefix) {
			w.reason = fmt.Sprintf("record length %d exceeds remaining %d bytes", n, d.size-pos-int64(d.prefix))
			return w, nil
		}
		if _, err := r.Discard(int(n)); err != nil {
			return w, err
		}
		if pos >= d.maxEnd {
			w.afterMaxEnd++
		}
		w.records++
		pos += int64(d.prefix) + n
	}
}

// zipStore reads Java GZipStore blocks ({date}/xlog/data) without writing.
// Blocks are decoded member by member (storage.DecodeZipBlock): a torn final
// member is excluded from the logical bytes and recorded in torn.
type zipStore struct {
	rel    string // slash path of the data directory
	dir    string
	count  int64 // count.dat logical next offset; -1 when unusable
	blocks map[int64]bool
	cache  map[int64][]byte
	order  []int64
	refs   int64
	torn   map[int64]zipTorn // decoded blocks with an incomplete final member
}

// zipTorn describes an incomplete final gzip member: bytes after end, the
// file offset following the last of members complete members.
type zipTorn struct {
	members    int
	end, bytes int64
}

func (z *zipStore) block(n int64) ([]byte, error) {
	if b, ok := z.cache[n]; ok {
		return b, nil
	}
	if !z.blocks[n] {
		return nil, dataErr("zip.missing_block", n*zipBlockBytes, "block xlog.%d does not exist", n)
	}
	f, err := os.OpenFile(filepath.Join(z.dir, "xlog."+strconv.FormatInt(n, 10)), os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	blk, err := storage.DecodeZipBlock(context.Background(), f, st.Size())
	switch {
	case errors.Is(err, storage.ErrCorrupt):
		return nil, dataErr("zip.block_gzip", n*zipBlockBytes, "xlog.%d: %v", n, err)
	case errors.Is(err, storage.ErrLimit):
		return nil, dataErr("zip.block_size", n*zipBlockBytes, "xlog.%d exceeds %d uncompressed (or %d compressed) bytes", n, zipBlockBytes, 5*zipBlockBytes)
	case err != nil:
		return nil, err
	}
	b := blk.Data
	if blk.Torn > 0 {
		if z.torn == nil {
			z.torn = map[int64]zipTorn{}
		}
		z.torn[n] = zipTorn{members: blk.Members, end: blk.End, bytes: blk.Torn}
	}
	if len(z.order) >= zipCacheBlocks {
		delete(z.cache, z.order[0])
		z.order = z.order[1:]
	}
	z.cache[n] = b
	z.order = append(z.order, n)
	return b, nil
}

// read follows the linked record chain (next Long5 + Int3 length + bytes).
func (z *zipStore) read(pos int64) ([]byte, error) {
	var out []byte
	seen := map[int64]struct{}{}
	start := pos
	for {
		if pos < 0 || pos > maxLong5 || (z.count >= 0 && pos >= z.count) {
			return nil, dataErr("zip.offset", start, "logical offset %d outside count.dat %d", pos, z.count)
		}
		if _, ok := seen[pos]; ok {
			return nil, dataErr("zip.cycle", start, "linked record cycle at %d", pos)
		}
		if len(seen) >= maxZipChain {
			return nil, dataErr("zip.cycle", start, "linked record chain exceeds %d records", maxZipChain)
		}
		seen[pos] = struct{}{}
		b, err := z.block(pos / zipBlockBytes)
		if err != nil {
			return nil, err
		}
		at := int(pos % zipBlockBytes)
		if at > len(b)-8 {
			return nil, dataErr("zip.record_bounds", pos, "record header beyond block length %d", len(b))
		}
		next := readLong5(b[at : at+5])
		n := int(int8(b[at+5]))<<16 | int(b[at+6])<<8 | int(b[at+7])
		if n < 0 || n > len(b)-at-8 {
			return nil, dataErr("zip.record_bounds", pos, "record length %d beyond block length %d", n, len(b))
		}
		if len(out)+n > maxRecordBytes {
			return nil, dataErr("data.length", start, "linked record exceeds %d bytes", maxRecordBytes)
		}
		out = append(out, b[at+8:at+8+n]...)
		if next == 0 {
			return out, nil
		}
		if next < 0 {
			return nil, dataErr("zip.link", pos, "negative next offset %d", next)
		}
		pos = next
	}
}

// parseBlockName accepts the Java "xlog.N" names only.
func parseBlockName(name string) (int64, bool) {
	s, ok := strings.CutPrefix(name, "xlog.")
	if !ok || s == "" || (len(s) > 1 && s[0] == '0') {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil && n >= 0 && n <= 16383
}
