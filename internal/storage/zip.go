// Scouter DB formats: Copyright 2015 Scouter authors, Apache-2.0.
// Independent implementation of pinned source; see compatibility/storage-contract.md.
package storage

import (
	"bufio"
	"compress/flate"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// GZipStore ({date}/xlog/data, shared by compressed XLog and Profile).
//
// xlog.N holds complete gzip members whose concatenated decompression is
// logical block N. Java CompressUtil.unZip (GZIPInputStream) reads concatenated
// members, so the Go writer appends one independent member per record instead
// of recompressing the whole block. A Java block (one member) is continued the
// same way. count.dat (int64 logical next offset) is overwritten in place
// after the member write: a process crash can leave count behind its block,
// never ahead of it. Both handles live in the Store file cache, so Sync and
// Close fsync them; nothing is fsynced per record.
const (
	// zipCacheBlocks is the default bound of decoded blocks kept for reads
	// (<= 4 x 32MiB) until _compress_read_cache_block_count is applied
	// (Store.SetCompressReadCacheBlocks). Java keeps 3 read-cache plus 3
	// writing blocks by default.
	zipCacheBlocks = 4
	// zipWriters is the default bound of per-date writer positions (no memory
	// beyond a few words); _compress_dailycount_header_cache_size adjusts it.
	zipWriters = 8
	// maxZipFileBytes bounds the compressed size of one xlog.N. One member per
	// minimal record costs at most ~4x its logical size.
	maxZipFileBytes = 5 * zipBlockBytes
	// maxZipTornBytes bounds an incomplete final member treated as torn: one
	// member of the largest record (Int3 0x7fffff) plus gzip framing.
	maxZipTornBytes = 9 << 20
	// zipLevel: measured on replay records (Go 1.27, arm64), BestSpeed costs
	// ~45us vs ~67us per 1-128KiB profile member for a ratio of 0.149 vs
	// 0.143; ~110B XLog members cost ~5us at either level and grow ~1.2x
	// (gzip framing), so the level only matters for large records.
	zipLevel = gzip.BestSpeed
)

var gzipMagic = []byte{0x1f, 0x8b, 8}

// ZipBlock is the decoded content of one xlog.N file. Data covers the complete
// members only; End is the file offset after the last complete member.
type ZipBlock struct {
	Data    []byte
	Members int
	End     int64
	// Torn is the length of an incomplete final member after End: the input
	// ends inside its header, deflate stream or trailer. The member bytes are
	// excluded from Data.
	Torn int64
}

// DecodeZipBlock decodes r[0:size] member by member. An incomplete final
// member of at most maxZipTornBytes (a torn append) is reported in Torn
// instead of failing. Any other damage (bad header, checksum, deflate data,
// or a longer incomplete tail) is ErrCorrupt. Logical bytes above 32MiB are
// ErrLimit. Context and I/O errors are returned unchanged.
func DecodeZipBlock(ctx context.Context, r io.ReaderAt, size int64) (ZipBlock, error) {
	return decodeZipLimit(ctx, r, size, zipBlockBytes)
}

// decodeZipLimit is DecodeZipBlock with logical bytes above limit ErrLimit.
func decodeZipLimit(ctx context.Context, r io.ReaderAt, size int64, limit int) (ZipBlock, error) {
	var b ZipBlock
	if size < 0 {
		return b, ErrInvalid
	}
	if size > maxZipFileBytes {
		return b, ErrLimit
	}
	cr := &countingReader{r: bufio.NewReaderSize(queryReader{ctx: ctx, reader: io.NewSectionReader(r, 0, size)}, 64<<10)}
	var z gzip.Reader
	var out []byte
	for {
		start, logical := cr.n, len(out)
		err := z.Reset(cr)
		if err == io.EOF && cr.n == start {
			break
		}
		if err == nil {
			z.Multistream(false)
			out, err = readMember(&z, out, limit)
		}
		if err != nil {
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				if errors.Is(err, gzip.ErrHeader) || errors.Is(err, gzip.ErrChecksum) || isFlateCorrupt(err) {
					return ZipBlock{}, fmt.Errorf("gzip member at byte %d: %v: %w", start, err, ErrCorrupt)
				}
				return ZipBlock{}, err
			}
			tail := size - start
			if tail > maxZipTornBytes {
				return ZipBlock{}, fmt.Errorf("incomplete gzip member of %d bytes at byte %d: %w", tail, start, ErrCorrupt)
			}
			if tail < 10 {
				head := make([]byte, tail)
				if _, rerr := r.ReadAt(head, start); rerr != nil {
					return ZipBlock{}, rerr
				}
				if !slices.Equal(head, gzipMagic[:min(len(head), len(gzipMagic))]) {
					return ZipBlock{}, fmt.Errorf("%d trailing bytes at byte %d are not a gzip header: %w", tail, start, ErrCorrupt)
				}
			}
			b.Torn = tail
			out = out[:logical]
			break
		}
		b.Members++
		b.End = cr.n
	}
	if err := ctx.Err(); err != nil {
		return ZipBlock{}, err
	}
	b.Data = out
	return b, nil
}
func isFlateCorrupt(err error) bool {
	var c flate.CorruptInputError
	return errors.As(err, &c)
}

// readMember appends one member's decompressed bytes to out.
func readMember(z *gzip.Reader, out []byte, limit int) ([]byte, error) {
	for {
		if len(out) == cap(out) {
			out = slices.Grow(out, 64<<10)
		}
		n, err := z.Read(out[len(out):cap(out)])
		out = out[:len(out)+n]
		if len(out) > limit {
			return out, ErrLimit
		}
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
	}
}

// countingReader exposes io.ByteReader so flate reads exactly up to each
// member's end and the member boundaries are known.
type countingReader struct {
	r *bufio.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
func (c *countingReader) ReadByte() (byte, error) {
	b, err := c.r.ReadByte()
	if err == nil {
		c.n++
	}
	return b, err
}

// zipState is the per-Store GZipStore state. It is used only under the Store
// gate. Cached block data is owned by the cache: readers copy record bytes out
// and never return or retain a view.
type zipState struct {
	blocks  map[string]*zipCached
	writers map[string]*zipWriter
	// maxBlocks/maxWriters are the live bounds, refreshed by Store.zipState.
	maxBlocks, maxWriters int
	clock                 uint64
	gzipMember
	// truncs counts block file truncations (truncateCached). Appends only
	// add bytes, so a block decoded outside the gate (zipFetch) still holds
	// the file's leading bytes while truncs is unchanged.
	truncs uint64
	// failed remembers refused writer loads (zipLoadFailure), at most
	// zipLoadFailures dates.
	failed map[string]*zipLoadFailure
}
type zipCached struct {
	data []byte
	info os.FileInfo // file identity, size and mtime the data matches
	// end is the file offset after the last complete member: data is the
	// decode of [0, end). end < info.Size() when a torn member follows.
	end  int64
	used uint64
}

// zipLoadFailure is a writer load refused by validation (ErrCorrupt,
// ErrLimit). While every file the load read is unchanged (key), the next load
// of the date returns err without decoding the block again. I/O errors are
// not remembered: they may clear without any file change.
type zipLoadFailure struct {
	key  zipLoadKey
	err  error
	used uint64
}

// zipLoadKey is what a writer load reads: the count.dat value, the stat of
// the count.dat and count block handles it read (the Store file cache may
// keep a handle to a file since replaced by path), and the stat of the data
// directory (its listing: later blocks) and of the next block path (rollover
// orphan, opened by path); nil when missing.
type zipLoadKey struct {
	count int64
	files [4]os.FileInfo
}

func zipLoadKeyOf(dir string, countInfo, blockInfo os.FileInfo, count, number int64) (zipLoadKey, error) {
	k := zipLoadKey{count: count, files: [4]os.FileInfo{1: countInfo, 2: blockInfo}}
	for _, f := range []struct {
		i    int
		path string
	}{{0, dir}, {3, zipBlockPath(dir, number+1)}} {
		st, err := os.Stat(f.path)
		if err != nil && !os.IsNotExist(err) {
			return k, err
		}
		if err == nil {
			k.files[f.i] = st
		}
	}
	return k, nil
}
func (k zipLoadKey) same(o zipLoadKey) bool {
	if k.count != o.count {
		return false
	}
	for i, st := range k.files {
		if (st == nil) != (o.files[i] == nil) || st != nil && !sameZipFile(st, o.files[i]) {
			return false
		}
	}
	return true
}

// zipWriter is the append position of one date. The handles come from the
// Store file cache; a different handle (eviction, purge) forces a reload.
type zipWriter struct {
	countFile, blockFile *os.File
	countInfo, blockInfo os.FileInfo // identity when the handles were taken
	count                int64       // count.dat value
	number               int64       // block of blockFile
	end                  int64       // file length of blockFile (complete members)
	used                 uint64
}

// rebind accepts reopened handles (file cache eviction) that still name the
// same files in the expected state. Anything else (purge, external change)
// returns false and the writer is reloaded from disk.
func (w *zipWriter) rebind(cf, bf *os.File) bool {
	if (cf == nil) != (w.countFile == nil) || (bf == nil) != (w.blockFile == nil) {
		return false
	}
	if cf != nil && cf != w.countFile {
		st, err := cf.Stat()
		var b [8]byte
		if err != nil || !os.SameFile(st, w.countInfo) || st.Size() != 8 {
			return false
		}
		if _, err = cf.ReadAt(b[:], 0); err != nil || int64(binary.BigEndian.Uint64(b[:])) != w.count {
			return false
		}
		w.countFile, w.countInfo = cf, st
	}
	if bf != nil && bf != w.blockFile {
		st, err := bf.Stat()
		if err != nil || !os.SameFile(st, w.blockInfo) || st.Size() != w.end {
			return false
		}
		w.blockFile, w.blockInfo = bf, st
	}
	return true
}

type writeBuffer struct{ b []byte }

func (w *writeBuffer) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}

// zipState returns the Store's compressed block cache and writer positions,
// created on first use; callers hold the Store gate.
func (s *Store) zipState() *zipState {
	if s.zip == nil {
		s.zip = &zipState{blocks: map[string]*zipCached{}, writers: map[string]*zipWriter{}, failed: map[string]*zipLoadFailure{}}
	}
	s.zip.maxBlocks, s.zip.maxWriters = s.zipCacheLimit(), s.zipWriterLimit()
	s.zip.trimBlocks(s.zip.maxBlocks) // a lowered live bound applies at once
	return s.zip
}

// zipReleaseLocked forgets cached blocks, writer positions and refused loads
// under dir (a purged date). Identity checks already reject a replaced file;
// this only returns the memory early.
func (s *Store) zipReleaseLocked(dir string) {
	z := s.zipState()
	prefix := dir + string(filepath.Separator)
	for p := range z.blocks {
		if strings.HasPrefix(p, prefix) {
			delete(z.blocks, p)
		}
	}
	for d := range z.writers {
		if strings.HasPrefix(s.path(d, "xlog", "data")+string(filepath.Separator), prefix) {
			delete(z.writers, d)
		}
	}
	for d := range z.failed {
		if strings.HasPrefix(s.path(d, "xlog", "data")+string(filepath.Separator), prefix) {
			delete(z.failed, d)
		}
	}
}

// truncateCached truncates a Store-cached handle. Block files never go
// through appendData, but its cached end offset is dropped anyway so a
// truncated path can never be appended at a stale offset. Handles are never
// closed here (closing is the file cache's job, via evictFileLocked).
func (s *Store) truncateCached(f *os.File, path string, size int64) error {
	delete(s.ends, path)
	if s.zip != nil {
		s.zip.truncs++
	}
	return f.Truncate(size)
}
func zipBlockPath(dir string, number int64) string {
	return filepath.Join(dir, "xlog."+strconv.FormatInt(number, 10))
}
func sameZipFile(a, b os.FileInfo) bool {
	return a != nil && b != nil && os.SameFile(a, b) && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}
func (z *zipState) tick() uint64 {
	z.clock++
	return z.clock
}
func (z *zipState) cache(path string, data []byte, info os.FileInfo, end int64) {
	delete(z.blocks, path)
	z.trimBlocks(z.maxBlocks - 1)
	z.blocks[path] = &zipCached{data: data, info: info, end: end, used: z.tick()}
}

// zipLoadFailures bounds the remembered refused loads (a few hundred bytes
// each). It is fixed, not the writer bound: refused dates that take turns
// beyond the writer positions must still not decode again.
const zipLoadFailures = MaxZipWriters

// rememberFailure records a refused load of date (zipLoadFailure), evicting
// the least recently used one beyond zipLoadFailures.
func (z *zipState) rememberFailure(date string, key zipLoadKey, err error) {
	delete(z.failed, date)
	for len(z.failed) > 0 && len(z.failed) >= zipLoadFailures {
		victim, oldest := "", uint64(0)
		for d, f := range z.failed {
			if victim == "" || f.used < oldest || (f.used == oldest && d < victim) {
				victim, oldest = d, f.used
			}
		}
		delete(z.failed, victim)
	}
	z.failed[date] = &zipLoadFailure{key: key, err: err, used: z.tick()}
}

// trimBlocks evicts least recently used blocks until at most n remain.
func (z *zipState) trimBlocks(n int) {
	for len(z.blocks) > 0 && len(z.blocks) > n {
		victim, oldest := "", uint64(0)
		for p, c := range z.blocks {
			if victim == "" || c.used < oldest || (c.used == oldest && p < victim) {
				victim, oldest = p, c.used
			}
		}
		delete(z.blocks, victim)
	}
}

// zipDecodeHook, when set by a test, runs before a read decodes a block
// (outside the gate in fetchZip, under it otherwise). zipTailHook receives
// the bytes of every grown-block tail decoded under the gate.
// zipLoadDecodeHook runs before a writer load decodes its block; an error it
// returns is the decode's error. zipMemberWriteHook replaces the member write
// of appendZip.
var (
	zipDecodeHook      func()
	zipTailHook        func(int64)
	zipLoadDecodeHook  func() error
	zipMemberWriteHook func(f *os.File, member []byte, off int64) (int, error)
)

// Block decodes outside the Store gate (zipFetch).
//
// A cold block costs a decode of up to 32MiB logical bytes (tens of
// milliseconds), which under the gate stalls every writer. A read that can
// end its gate hold passes a zipFetch: on a cache miss it records the block
// file (path and stat) and returns a *zipMiss instead of decoding. The reader
// ends its hold, decodes the file without the gate (fetchZip: up to its size
// once a slot is held and the file is open, so appends while the reader
// waited are decoded there too) and repeats the read in its next hold, which
// takes the block from the fetch:
//
//   - The block is used while it is still the file's leading bytes: the
//     same file (os.SameFile), no block truncation since the miss
//     (zipState.truncs) and no purge or DB delete of the date
//     (xlogPurgedSinceLocked).
//   - Unchanged file (sameZipFile): the block is cached as an in-gate decode
//     would have cached it (a torn final member excluded alike).
//   - Grown file (appends to the open block during the decode and the wait
//     for the gate): the members after the decoded ones (from ZipBlock.End,
//     a checked member boundary, so a member that was being written at the
//     decode is read again whole) are decoded under the gate and joined, and
//     the block is cached with the current stat; the writer then keeps it
//     current (appendZip) as after an in-gate decode. That tail is bounded
//     (zipTailMaxBytes, zipTailMaxLogical) so it costs about one hold;
//     a longer tail, a tail error or more than 32MiB leaves the block unused.
//   - A block that is not used costs a miss; a read misses at most
//     zipFetchAttempts times, then decodes under the gate as before. So a read
//     always ends with the in-gate result of its last hold.
//
// Memory and slots: zipDecodeConcurrent bounds the blocks being decoded or
// waiting for their read outside the gate (a slot from before the file is
// opened until the read takes the block or the fetch is dropped). A taken
// block goes into the cache (bounded by _compress_read_cache_block_count) or
// is dropped. A query holds at most one block and waits for a slot only while
// it holds none, so slots cannot deadlock; the wait is outside the gate and
// ends with ctx. A query has the block file open only while it decodes.
const (
	zipFetchAttempts    = 4
	zipDecodeConcurrent = 2
	// zipTailMaxBytes and zipTailMaxLogical bound the members decoded under
	// the gate to join a grown block. Measured (Go 1.27, arm64): 256KiB of
	// ~90 B XLog members (about 2,800) decode in ~0.8ms, dominated by the
	// per-member cost; highly compressible profile members decode ~1GB/s,
	// so 1MiB logical is ~1ms. Either bound keeps the tail near scanHoldTime.
	zipTailMaxBytes   = 256 << 10
	zipTailMaxLogical = 1 << 20
)

// zipFetch is one query's block decoded outside the gate. It is owned by the
// query's goroutine.
type zipFetch struct {
	misses int // misses of the record being read (reset when a read ends)
	block  *zipFetched
}

// zipFetched is a block decoded by fetchZip. info is the file as stat'ed
// after it was opened: its size is the number of bytes decoded. end and torn
// are the decode's ZipBlock.End and Torn.
type zipFetched struct {
	path, date  string
	data        []byte
	err         error
	info        os.FileInfo
	end, torn   int64
	truncs, seq uint64
	release     func()
}

// zipMiss ends a gate hold to decode a block (fetchZip).
type zipMiss struct {
	path, date  string
	info        os.FileInfo
	truncs, seq uint64
}

func (m *zipMiss) Error() string { return "compressed block " + m.path + " not cached" }

// take returns the fetched block of path, if any, and leaves the fetch empty
// when it does; the block's slot is released (the block then goes into the
// cache or is dropped).
func (f *zipFetch) take(path string) *zipFetched {
	if f == nil || f.block == nil || f.block.path != path {
		return nil
	}
	b := f.block
	f.block = nil
	b.release()
	return b
}

// drop releases a fetched block that no read used.
func (f *zipFetch) drop() {
	if f != nil && f.block != nil {
		f.block.release()
		f.block = nil
	}
}

// missLocked records the block file for a decode outside the gate. It
// returns nil when the read must decode under the gate instead (no fetch, or
// the record missed zipFetchAttempts times).
func (s *Store) missLocked(fetch *zipFetch, date, path string, st os.FileInfo) *zipMiss {
	if fetch == nil || fetch.misses >= zipFetchAttempts {
		return nil
	}
	fetch.misses++
	return &zipMiss{path: path, date: date, info: st, truncs: s.zip.truncs, seq: s.packSeq.Load()}
}

// fetchZip decodes the block of m without the gate into fetch (see above).
// A query calls it between its holds after a read returned m. A file that is
// gone or no longer the stat'ed one leaves the fetch empty: the next read
// misses again or, after zipFetchAttempts, decodes under the gate.
func (s *Store) fetchZip(ctx context.Context, fetch *zipFetch, m *zipMiss) error {
	fetch.drop()
	release, err := acquireArchiveSlot(ctx, s.zipDecodes)
	if err != nil {
		return err
	}
	f, err := os.Open(m.path)
	if err != nil {
		release()
		return nil
	}
	defer f.Close() // read-only
	st, err := f.Stat()
	if err != nil || !os.SameFile(st, m.info) || st.Size() < m.info.Size() {
		release()
		return nil
	}
	if hook := zipDecodeHook; hook != nil {
		hook()
	}
	b, err := DecodeZipBlock(ctx, f, st.Size())
	if cancelled := ctx.Err(); cancelled != nil {
		release()
		return cancelled
	}
	if err != nil {
		err = fmt.Errorf("%s: %w", m.path, err)
	}
	fetch.block = &zipFetched{path: m.path, date: m.date, data: b.Data, err: err, info: st, end: b.End, torn: b.Torn,
		truncs: m.truncs, seq: m.seq, release: release}
	return nil
}

// useFetchedLocked returns the logical bytes of the block file st describes
// from b (see above), caching them; ok=false when b cannot be used.
func (s *Store) useFetchedLocked(ctx context.Context, b *zipFetched, st os.FileInfo) (data []byte, ok bool, err error) {
	if b.truncs != s.zip.truncs || !os.SameFile(b.info, st) || st.Size() < b.info.Size() {
		return nil, false, nil
	}
	if purged, known := s.xlogPurgedSinceLocked(b.date, b.seq); purged || !known {
		return nil, false, nil
	}
	if sameZipFile(b.info, st) {
		if b.err != nil {
			return nil, true, b.err
		}
		s.zip.cache(b.path, b.data, st, b.end)
		return b.data, true, nil
	}
	if b.err != nil || st.Size()-b.end > zipTailMaxBytes {
		return nil, false, nil
	}
	f, err := os.Open(b.path)
	if err != nil {
		return nil, false, nil
	}
	defer f.Close()
	now, err := f.Stat()
	if err != nil || !sameZipFile(now, st) || now.Size() > maxZipFileBytes {
		return nil, false, nil
	}
	off := b.end
	if hook := zipTailHook; hook != nil {
		hook(now.Size() - off)
	}
	tail, err := decodeZipLimit(ctx, io.NewSectionReader(f, off, now.Size()-off), now.Size()-off, zipTailMaxLogical)
	if err != nil || len(b.data)+len(tail.Data) > zipBlockBytes {
		return nil, false, nil
	}
	data = append(b.data, tail.Data...)
	s.zip.cache(b.path, data, now, off+tail.End)
	return data, true, nil
}

// zipDataFetch returns the cached logical bytes of block number (read-only
// view). A torn final member is excluded; reads into it fail instead of
// returning partial bytes. A file changed since caching (size, mtime,
// identity) is decoded again. Given a fetch, it returns a *zipMiss instead of
// decoding a missing block under the gate, and uses the block a fetch
// decoded.
func (s *Store) zipDataFetch(ctx context.Context, date string, number int64, fetch *zipFetch) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if number < 0 || number > 16383 {
		return nil, ErrLimit
	}
	path := zipBlockPath(s.path(date, "xlog", "data"), number)
	z := s.zipState()
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if c := z.blocks[path]; c != nil && sameZipFile(c.info, st) {
		c.used = z.tick()
		return c.data, nil
	}
	if b := fetch.take(path); b != nil {
		if data, ok, err := s.useFetchedLocked(ctx, b, st); ok {
			return data, err
		}
	}
	if m := s.missLocked(fetch, date, path, st); m != nil {
		return nil, m
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if st, err = f.Stat(); err != nil {
		return nil, err
	}
	if hook := zipDecodeHook; hook != nil {
		hook()
	}
	b, err := DecodeZipBlock(ctx, f, st.Size())
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	z.cache(path, b.Data, st, b.End)
	return b.Data, nil
}

// readZipFetch reads the compressed record at logical pos of date, joining
// its linked parts across blocks. With a fetch, a missing block is a
// *zipMiss (zipDataFetch) and the read is repeated after fetchZip. Any other
// end of the read releases what the fetch still holds (a block the read did
// not need, e.g. one the cache already had).
func (s *Store) readZipFetch(ctx context.Context, date string, pos int64, fetch *zipFetch) (out []byte, err error) {
	if fetch != nil {
		defer func() {
			var miss *zipMiss
			if !errors.As(err, &miss) {
				fetch.drop()
				fetch.misses = 0
			}
		}()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pos < 0 || pos > 1<<39-1 {
		return nil, ErrCorrupt
	}
	out = make([]byte, 0)
	seen := make(map[int64]struct{})
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(seen) >= s.config.MaxQueryRecords {
			return nil, ErrLimit
		}
		if _, ok := seen[pos]; ok {
			return nil, ErrCorrupt
		}
		seen[pos] = struct{}{}
		b, err := s.zipDataFetch(ctx, date, pos/zipBlockBytes, fetch)
		if err != nil {
			return nil, err
		}
		start := int(pos % zipBlockBytes)
		if start > len(b)-8 {
			return nil, io.ErrUnexpectedEOF
		}
		next, err := dataOffset(b[start : start+5])
		if err != nil {
			return nil, err
		}
		n := int(int8(b[start+5]))<<16 | int(b[start+6])<<8 | int(b[start+7])
		if n < 0 {
			return nil, ErrCorrupt
		}
		if n > s.config.MaxRecordBytes-len(out) {
			return nil, ErrLimit
		}
		if n > len(b)-start-8 {
			return nil, io.ErrUnexpectedEOF
		}
		out = append(out, b[start+8:start+8+n]...)
		if next == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return out, nil
		}
		pos = next
	}
}

func zipMismatch(why string) error {
	return fmt.Errorf("compressed count/block mismatch (%s): %w", why, ErrCorrupt)
}

// zipWriterFor returns the date's append position, loading (and repairing) it
// when it is new or one of its handles is no longer the cached one.
func (s *Store) zipWriterFor(z *zipState, date, dir string) (*zipWriter, error) {
	w := z.writers[date]
	if w != nil {
		cf, err := s.file(filepath.Join(dir, "count.dat"), false)
		if err != nil {
			return nil, err
		}
		bf, err := s.file(zipBlockPath(dir, w.number), false)
		if err != nil {
			return nil, err
		}
		if w.rebind(cf, bf) {
			w.used = z.tick()
			return w, nil
		}
		delete(z.writers, date)
	}
	w, err := s.loadZipWriter(z, date, dir)
	if err != nil {
		return nil, err
	}
	for len(z.writers) > 0 && len(z.writers) >= z.maxWriters {
		victim, oldest := "", uint64(0)
		for d, x := range z.writers {
			if victim == "" || x.used < oldest || (x.used == oldest && d < victim) {
				victim, oldest = d, x.used
			}
		}
		delete(z.writers, victim)
	}
	w.used = z.tick()
	z.writers[date] = w
	return w, nil
}

// loadZipWriter validates count.dat against its block before the first append.
// Every check runs before any repair, so a refused state is left untouched.
// Repairs are limited to this writer's own crash shapes:
//   - a torn final member of the count block is truncated (zip-torn-member);
//   - complete next=0 records after count are kept by advancing count
//     (zip-count-advanced), see planZipCountRecovery.
func (s *Store) loadZipWriter(z *zipState, date, dir string) (*zipWriter, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	countPath := filepath.Join(dir, "count.dat")
	cf, err := s.file(countPath, false)
	if err != nil {
		return nil, err
	}
	var count int64
	var countInfo os.FileInfo
	if cf != nil {
		if countInfo, err = cf.Stat(); err != nil {
			return nil, err
		}
		var b [9]byte
		n, err := cf.ReadAt(b[:], 0)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n != 8 {
			return nil, fmt.Errorf("count.dat has %d bytes: %w", n, ErrCorrupt)
		}
		count = int64(binary.BigEndian.Uint64(b[:8]))
		if count < 0 || count > 1<<39-1 {
			return nil, ErrCorrupt
		}
	}
	number, start := count/zipBlockBytes, int(count%zipBlockBytes)
	blockPath := zipBlockPath(dir, number)
	bf, err := s.file(blockPath, false)
	if err != nil {
		return nil, err
	}
	var blockInfo os.FileInfo
	if bf != nil {
		if blockInfo, err = bf.Stat(); err != nil {
			return nil, err
		}
	}
	key, err := zipLoadKeyOf(dir, countInfo, blockInfo, count, number)
	if err != nil {
		return nil, err
	}
	if f := z.failed[date]; f != nil && f.key.same(key) {
		f.used = z.tick()
		return nil, f.err
	}
	delete(z.failed, date)
	refuse := func(err error) (*zipWriter, error) {
		if errors.Is(err, ErrCorrupt) || errors.Is(err, ErrLimit) {
			z.rememberFailure(date, key, err)
		}
		return nil, err
	}
	var blk ZipBlock
	if bf != nil {
		st := blockInfo
		// A cached block of these exact complete members is the decode
		// (CONTRACT §5): no torn tail (end == size) and the same file.
		if c := z.blocks[blockPath]; c != nil && c.end == st.Size() && sameZipFile(c.info, st) {
			c.used = z.tick()
			blk = ZipBlock{Data: c.data, End: c.end}
		} else {
			if hook := zipLoadDecodeHook; hook != nil {
				if err := hook(); err != nil {
					return refuse(fmt.Errorf("%s: %w", blockPath, err))
				}
			}
			if blk, err = DecodeZipBlock(context.Background(), bf, st.Size()); err != nil {
				return refuse(fmt.Errorf("%s: %w", blockPath, err))
			}
		}
	}
	if len(blk.Data) < start {
		return refuse(zipMismatch("count beyond block"))
	}
	var advance string
	if len(blk.Data) > start {
		if advance, err = planZipCountRecovery(dir, number, start, blk.Data); err != nil {
			return refuse(err)
		}
	}
	if blk.Torn > 0 {
		if err = s.truncateCached(bf, blockPath, blk.End); err != nil {
			return nil, err
		}
		s.recovered(RecoveryEvent{Path: blockPath, Kind: "zip-torn-member", Detail: fmt.Sprintf("truncated an incomplete final gzip member (%d byte(s)) after %d complete member(s) at byte %d, logical length %d", blk.Torn, blk.Members, blk.End, len(blk.Data))})
	}
	w := &zipWriter{countFile: cf, blockFile: bf, countInfo: countInfo, count: count, number: number, end: blk.End}
	if bf != nil {
		if w.blockInfo, err = bf.Stat(); err != nil {
			return nil, err
		}
		z.cache(blockPath, blk.Data, w.blockInfo, blk.End)
	}
	if advance != "" {
		next := number*zipBlockBytes + int64(len(blk.Data))
		if err = s.writeZipCount(w, countPath, next); err != nil {
			return nil, err
		}
		w.count = next
		s.recovered(RecoveryEvent{Path: countPath, Kind: "zip-count-advanced", Detail: fmt.Sprintf("count %d -> %d, %s", count, next, advance)})
	}
	return w, nil
}

// planZipCountRecovery accepts count behind block number only when the tail
// after count is complete next=0 records (a crash after the member write but
// before count.dat) and no block beyond number+1 exists. A rollover crash may
// leave one next block: it is accepted when it is empty or its complete
// members hold only complete next=0 records whose first record could not have
// fit after count (the rollover condition); a torn final member there is
// allowed. The next rollover truncates it. It returns the event detail.
func planZipCountRecovery(dir string, number int64, start int, block []byte) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if n, convErr := strconv.ParseInt(strings.TrimPrefix(e.Name(), "xlog."), 10, 64); convErr == nil && strings.HasPrefix(e.Name(), "xlog.") && n > number+1 {
			return "", zipMismatch("later blocks exist")
		}
	}
	orphan := ""
	if f, err := os.Open(zipBlockPath(dir, number+1)); err == nil {
		defer f.Close()
		st, err := f.Stat()
		if err != nil {
			return "", err
		}
		next, err := DecodeZipBlock(context.Background(), f, st.Size())
		if errors.Is(err, ErrCorrupt) {
			return "", zipMismatch("later block is not a readable rollover")
		}
		if err != nil {
			return "", err
		}
		if len(next.Data) > 0 {
			if _, ok := completeZipRecords(next.Data, 0); !ok || len(next.Data) < 8 {
				return "", zipMismatch("later block exists")
			}
			first := int(next.Data[5])<<16 | int(next.Data[6])<<8 | int(next.Data[7])
			if start+8+first <= zipBlockBytes {
				return "", zipMismatch("later block is not a rollover")
			}
		}
		orphan = fmt.Sprintf(", orphan block %d kept until the next rollover", number+1)
	} else if !os.IsNotExist(err) {
		return "", err
	}
	records, ok := completeZipRecords(block, start)
	if !ok {
		return "", zipMismatch("tail is not complete unlinked records")
	}
	return fmt.Sprintf("%d unindexed record(s) left unreferenced%s", records, orphan), nil
}

// completeZipRecords reports whether block[start:] is only complete next=0 records.
func completeZipRecords(block []byte, start int) (int, bool) {
	records := 0
	for p := start; p < len(block); records++ {
		if len(block)-p < 8 || readSignedLong5(block[p:p+5]) != 0 || block[p+5]&0x80 != 0 {
			return records, false
		}
		n := int(block[p+5])<<16 | int(block[p+6])<<8 | int(block[p+7])
		if p+8+n > len(block) {
			return records, false
		}
		p += 8 + n
	}
	return records, true
}
func readSignedLong5(b []byte) int64 {
	return int64(int8(b[0]))<<32 | int64(b[1])<<24 | int64(b[2])<<16 | int64(b[3])<<8 | int64(b[4])
}

// writeZipCount stores count.dat: created by an atomic rename the first time
// (never an empty or short file), then overwritten in place (8 bytes).
func (s *Store) writeZipCount(w *zipWriter, countPath string, count int64) error {
	if w.countFile == nil {
		if err := atomicWrite(countPath, longKey(count)); err != nil {
			return err
		}
		f, err := s.file(countPath, false)
		if err != nil {
			return err
		}
		if f == nil {
			return fmt.Errorf("count.dat vanished after creation: %w", ErrCorrupt)
		}
		st, err := f.Stat()
		if err != nil {
			return err
		}
		w.countFile, w.countInfo = f, st
		return nil
	}
	n, err := w.countFile.WriteAt(longKey(count), 0)
	if err == nil && n != 8 {
		err = io.ErrShortWrite
	}
	return err
}

// gzipMember builds the gzip members of zip records. The Store's (zipState)
// is used under the Store gate; a pack writer has its own, so it compresses
// the records it took from its queue before it takes the gate
// (packQueue.prepare).
type gzipMember struct {
	gz     *gzip.Writer
	member writeBuffer
}

// compress returns one gzip member holding the record (Long5 next=0, Int3
// length, payload). The buffer is reused by the next call and is only
// passed to a synchronous write or copied.
func (z *gzipMember) compress(b []byte) ([]byte, error) {
	z.member.b = z.member.b[:0]
	if z.gz == nil {
		gz, err := gzip.NewWriterLevel(&z.member, zipLevel)
		if err != nil {
			return nil, err
		}
		z.gz = gz
	} else {
		z.gz.Reset(&z.member)
	}
	head := [8]byte{5: byte(len(b) >> 16), 6: byte(len(b) >> 8), 7: byte(len(b))}
	if _, err := z.gz.Write(head[:]); err != nil {
		return nil, err
	}
	if _, err := z.gz.Write(b); err != nil {
		return nil, err
	}
	if err := z.gz.Close(); err != nil {
		return nil, err
	}
	return z.member.b, nil
}

// appendZip appends one next=0 record and returns its logical offset. The
// member is written before count.dat. A failed member write is truncated
// away; a failed count write leaves count behind the block, which the next
// load recovers. Either failure drops the writer so the next append reloads;
// the reload validates the cached block instead of decoding it when the
// block is the cached one again (a member write truncated away, or a count
// write after the cache took the member).
func (s *Store) appendZip(date string, b []byte) (int64, error) {
	return s.appendZipMember(date, b, nil)
}

// appendZipMember is appendZip with the record's gzip member built before
// the gate was taken (gzipMember.compress of b); nil compresses here.
func (s *Store) appendZipMember(date string, b, member []byte) (int64, error) {
	if len(b) > s.config.MaxRecordBytes || len(b) > 0x7fffff {
		return 0, ErrLimit
	}
	z := s.zipState()
	dir := s.path(date, "xlog", "data")
	w, err := s.zipWriterFor(z, date, dir)
	if err != nil {
		return 0, err
	}
	pos := w.count
	number, start := pos/zipBlockBytes, pos%zipBlockBytes
	if start+8+int64(len(b)) > zipBlockBytes {
		number++
		pos = number * zipBlockBytes
	}
	if pos+int64(8+len(b)) > 1<<39-1 {
		return 0, ErrLimit
	}
	if member == nil {
		if member, err = z.compress(b); err != nil {
			return 0, err
		}
	}
	defer func() {
		if cap(z.member.b) > 1<<20 {
			z.member.b = nil
		}
	}()
	blockPath := zipBlockPath(dir, number)
	if w.blockFile == nil || number != w.number {
		// New block (rollover or first record). A leftover rollover orphan
		// is truncated first, so a crash leaves it empty, never mixed.
		f, err := s.file(blockPath, true)
		if err != nil {
			return 0, err
		}
		st, err := f.Stat()
		if err != nil {
			return 0, err
		}
		delete(z.blocks, blockPath)
		if st.Size() > 0 {
			if err = s.truncateCached(f, blockPath, 0); err != nil {
				delete(z.writers, date)
				return 0, err
			}
		}
		w.blockFile, w.blockInfo, w.number, w.end = f, st, number, 0
	}
	cached := z.blocks[blockPath]
	if cached != nil && (cached.info.Size() != w.end || cached.end != w.end) {
		delete(z.blocks, blockPath)
		cached = nil
	}
	var n int
	if hook := zipMemberWriteHook; hook != nil {
		n, err = hook(w.blockFile, member, w.end)
	} else {
		n, err = w.blockFile.WriteAt(member, w.end)
	}
	if err == nil && n != len(member) {
		err = io.ErrShortWrite
	}
	if err != nil {
		delete(z.writers, date)
		terr := s.truncateCached(w.blockFile, blockPath, w.end)
		// Bytes before w.end are untouched, so the cached data still matches
		// the truncated file of the same identity; only its mtime changed.
		if st, serr := w.blockFile.Stat(); terr == nil && serr == nil && cached != nil && st.Size() == w.end && os.SameFile(st, cached.info) {
			cached.info = st
		} else {
			delete(z.blocks, blockPath)
		}
		return 0, errors.Join(err, terr)
	}
	w.end += int64(len(member))
	if cached != nil || w.end == int64(len(member)) {
		if st, err := w.blockFile.Stat(); err == nil {
			var data []byte
			if cached != nil {
				data = cached.data
			}
			head := [8]byte{5: byte(len(b) >> 16), 6: byte(len(b) >> 8), 7: byte(len(b))}
			data = append(append(data, head[:]...), b...)
			if cached != nil {
				cached.data, cached.info, cached.end, cached.used = data, st, w.end, z.tick()
			} else {
				z.cache(blockPath, data, st, w.end)
			}
		} else {
			delete(z.blocks, blockPath)
		}
	}
	next := pos + int64(8+len(b))
	if err = s.writeZipCount(w, filepath.Join(dir, "count.dat"), next); err != nil {
		delete(z.writers, date)
		return 0, err
	}
	w.count = next
	return pos, nil
}
