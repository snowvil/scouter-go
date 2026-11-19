package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

func (s *Store) PutProfile(p *protocol.ProfilePack) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if p == nil || p.Time <= 0 {
		return ErrInvalid
	}
	if len(p.Profile) == 0 {
		return nil
	}
	if len(p.Profile) > s.config.MaxRecordBytes {
		return ErrLimit
	}
	date := JavaDate(p.Time, s.config.Location)
	if err := validDate(date); err != nil {
		return err
	}
	return s.putProfileLocked(date, p.Profile, nil, p.TxID)
}

// putProfileLocked writes one profile record and its txid index entry
// (XLogProfileWR). The caller holds Store.mu. member, when not nil, is the
// profile's gzip member (appendZipMember).
func (s *Store) putProfileLocked(date string, profile, member []byte, txid int64) error {
	path := s.path(date, "xlog", "xlog")
	zip, err := s.compressionMode(path+".profile.conf", "compress_profile_enabled", s.live.compressProfile.Load(), true)
	if err != nil {
		return err
	}
	var pos int64
	if zip {
		pos, err = s.appendZipMember(date, profile, member)
	} else {
		pos, err = s.appendData(path+".profile", profile, 4)
	}
	if err != nil {
		return err
	}
	idx, err := s.index(path+"_profile", false, true)
	if err != nil {
		return err
	}
	_, err = idx.Put(longKey(txid), offsetBytes(pos))
	return err
}

// putProfileBatchLocked is putProfileLocked for a run of queued profiles of
// one date: the data records in one write, then the txid index records in
// one write, the bytes consecutive putProfileLocked calls would leave.
func (s *Store) putProfileBatchLocked(date string, items []*packItem) []error {
	errs := make([]error, len(items))
	path := s.path(date, "xlog", "xlog")
	zip, err := s.compressionMode(path+".profile.conf", "compress_profile_enabled", s.live.compressProfile.Load(), true)
	if err != nil || zip || len(items) == 1 {
		for n, it := range items {
			if errs[n] = err; err == nil {
				errs[n] = s.putProfileLocked(date, it.b, it.member, it.txid)
			}
		}
		return errs
	}
	run := make([]*packItem, 0, len(items))
	data := make([][]byte, 0, len(items))
	for n, it := range items {
		if len(it.b) > s.config.MaxRecordBytes {
			errs[n] = ErrLimit
			continue
		}
		run = append(run, it)
		data = append(data, it.b)
	}
	if len(run) == 0 {
		return errs
	}
	fail := func(err error) []error {
		for n := range errs {
			if errs[n] == nil {
				errs[n] = err
			}
		}
		return errs
	}
	pos, err := s.appendDataBatch(path+".profile", data, 4)
	if err != nil {
		return fail(err)
	}
	entries := make([]legacyindex.Entry, len(run))
	for n, it := range run {
		entries[n] = legacyindex.Entry{Key: longKey(it.txid), Value: offsetBytes(pos[n])}
	}
	idx, err := s.index(path+"_profile", false, true)
	if err != nil {
		return fail(err)
	}
	if _, err = idx.PutBatch(entries); err != nil {
		return fail(err)
	}
	return errs
}

// Collect queries of one key (TRANX_PROFILE txid, gxid XLogs, gxid spans)
// split their Store gate use like the range scans (CONTRACT.md §6):
//
//   - The index hold takes a snapshot of the key's hash chain
//     (legacyindex.KeyChain: the bucket head, the key file length and a
//     read-only key file handle) and opens what the reads need while the
//     index and the data still belong to the same files.
//   - The chain is then walked without the gate, however long it is, and
//     the index values (5-byte data positions) are kept, at most the query's
//     record limit. The walk sees the chain as it was at the index hold: the
//     Store only appends to these indexes, and the handle keeps its inode
//     through a purge, DB delete or Close.
//   - Uncompressed records (xlog.profile, xlog.service, span.data) are then
//     read without the gate through a read-only handle the query opened in
//     its index hold (as batch archive reads do). The records the index named
//     are complete and never rewritten, and the handle keeps its inode, so a
//     purge, DB delete or Store.Close meanwhile cannot mix files: the result
//     is the one the query would have returned had it finished first.
//   - Compressed records (GZipStore, whose block cache the gate guards) are
//     read in gate holds of at most scanHoldSteps records or scanHoldTime
//     (checked after every record; at least one record per hold). A record
//     whose block is not cached ends the hold; its block is decoded outside
//     the gate and the record is read again in the next hold (zip.go,
//     zipFetch). Each hold
//     first checks the Store (closed: ErrClosed) and whether a purge or DB
//     delete touched the date's xlog directory since the index hold; then
//     the query returns what it returns after the purge: nothing, no error.
//     When that cannot be told (purge records pruned) the query starts over
//     (collectAttempts), then fails with ErrPurgeOverlap.
//
// Results, order, limits and errors (which record's error comes first) are
// the ones of the former single-hold queries.

// recordSource is how a collect query reads the data records its index hold
// found. It is owned by that query's goroutine.
type recordSource struct {
	date string
	// seq is Store.packSeq at the index hold (xlogPurgedSinceLocked).
	seq uint64
	// zip: read GZipStore records under the gate (eachRecord holds).
	zip bool
	// modeErr is the deferred compressionMode error of a query that read the
	// mode per record (gxid XLogs): reported by the first read, as before.
	modeErr error
	// f is the read-only handle of the uncompressed data file; nil with
	// openErr == nil when the file does not exist.
	f       *os.File
	openErr error
	prefix  int
	max     int // MaxRecordBytes
	// fetch is a compressed block decoded between holds (zip.go).
	fetch zipFetch
}

// openRecordSourceLocked runs in the index hold (Store gate held). path is
// the uncompressed data file; it is not opened when the records are
// compressed or there is nothing to read (an empty key chain).
func (s *Store) openRecordSourceLocked(date, path string, prefix int, zip, open bool) *recordSource {
	src := &recordSource{date: date, seq: s.packSeq.Load(), zip: zip, prefix: prefix, max: s.config.MaxRecordBytes}
	if zip || !open {
		return src
	}
	f, err := os.Open(path)
	if err != nil && !os.IsNotExist(err) {
		src.openErr = err
	}
	if err == nil {
		src.f = f
	}
	return src
}

func (r *recordSource) close() {
	if r == nil {
		return
	}
	r.fetch.drop()
	if r.f != nil {
		r.f.Close() // read-only: nothing to flush
		r.f = nil
	}
}

// read returns the record at pos. Compressed sources need the gate (the
// caller is inside an eachRecord hold); uncompressed ones read without it.
// The checks run in readDataContext's order.
func (s *Store) readRecord(ctx context.Context, src *recordSource, pos int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if src.modeErr != nil {
		return nil, src.modeErr
	}
	if src.zip {
		return s.readZipFetch(ctx, src.date, pos, &src.fetch)
	}
	b, err := src.readAt(pos)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	return b, err
}

// readAt is readData on the source's own handle.
func (r *recordSource) readAt(pos int64) ([]byte, error) {
	if pos < 0 {
		return nil, ErrCorrupt
	}
	if r.openErr != nil {
		return nil, r.openErr
	}
	if r.f == nil {
		return nil, fmt.Errorf("missing data file: %w", ErrCorrupt)
	}
	var head [4]byte
	if _, err := r.f.ReadAt(head[:r.prefix], pos); err != nil {
		return nil, err
	}
	n := 0
	if r.prefix == 2 {
		n = int(int16(binary.BigEndian.Uint16(head[:2])))
	} else {
		n = int(int32(binary.BigEndian.Uint32(head[:])))
	}
	if n < 0 {
		return nil, ErrCorrupt
	}
	if n > r.max {
		return nil, ErrLimit
	}
	data := make([]byte, n)
	_, err := r.f.ReadAt(data, pos+int64(r.prefix))
	return data, err
}

// collectBetweenHolds, when set by a test, runs outside the gate in
// eachRecord with the next record's index: before every gate hold of a
// compressed source (the first one included) and before every record of an
// uncompressed one.
var collectBetweenHolds func(next int)

// eachRecord calls visit(0..n-1) in order, each reading one record of src.
// Uncompressed sources run without the gate. Compressed ones run in gate
// holds (see above); purged reports a purge or DB delete of the date's xlog
// directory since the index hold, after which nothing more is read. A visit
// that returns a *zipMiss (its read found the block not cached) ends the
// hold; the block is decoded outside the gate and visit runs again for the
// same record in the next hold, so visit must not change state before its
// read succeeds.
func (s *Store) eachRecord(ctx context.Context, src *recordSource, n int, visit func(int) error) (purged bool, err error) {
	if !src.zip {
		for i := 0; i < n; i++ {
			if observe := collectBetweenHolds; observe != nil {
				observe(i)
			}
			if err := visit(i); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	for i := 0; i < n; {
		if observe := collectBetweenHolds; observe != nil {
			observe(i)
		}
		unlock, err := s.lockScan(ctx)
		if err != nil {
			return false, err
		}
		if s.closed {
			unlock()
			return false, ErrClosed
		}
		purged, known := s.xlogPurgedSinceLocked(src.date, src.seq)
		if !known {
			unlock()
			return false, errPurgeUnknown
		}
		if purged {
			unlock()
			return true, nil
		}
		deadline := time.Now().Add(scanHoldTime)
		var miss *zipMiss
		for steps := 1; i < n; steps++ {
			err := visit(i)
			if errors.As(err, &miss) {
				break
			}
			i++
			if err != nil {
				unlock()
				return false, err
			}
			if steps >= scanHoldSteps || time.Now().After(deadline) {
				break
			}
		}
		unlock()
		if miss != nil {
			if err := s.fetchZip(ctx, &src.fetch, miss); err != nil {
				return false, err
			}
		}
	}
	return false, nil
}

// xlogPurgedSinceLocked reports whether a purge or DB delete after seq (a
// Store.packSeq value) touched date's xlog directory (the directory or
// anything in it). known is false when purge records since seq were pruned
// (packDiscardMax/packDiscardAge): the answer is then unknown, and the
// caller restarts its query (collectAttempts) instead of guessing; an
// "after-purge" guess would be a silent empty result for a live date.
func (s *Store) xlogPurgedSinceLocked(date string, seq uint64) (purged, known bool) {
	if s.packSeq.Load() == seq {
		return false, true
	}
	if len(s.packDiscards) == 0 || s.packDiscards[0].seq > seq+1 {
		return false, false
	}
	return s.packDiscardedLocked(&packItem{date: date, seq: seq}), true
}

// ErrPurgeOverlap reports a compressed collect query that could not tell
// whether its date was purged meanwhile: more purge and DB delete records
// than the Store keeps (packDiscardMax) came between its holds, on every one
// of collectAttempts runs. Nothing was returned; the query can be retried.
var ErrPurgeOverlap = errors.New("storage: too many purges or DB deletes during the query; retry")

// errPurgeUnknown ends one run of a compressed collect query (eachRecord)
// whose purge check is unknown; the query starts over from its index hold.
var errPurgeUnknown = errors.New("purge records pruned during the query")

// collectAttempts bounds the runs of a compressed collect query restarted
// by errPurgeUnknown.
const collectAttempts = 3

// restartOnPurgeUnknown runs a collect query again from its index hold
// while its purge check was unknown, at most collectAttempts times.
func restartOnPurgeUnknown[T any](run func() (T, error)) (T, error) {
	for attempt := 1; ; attempt++ {
		out, err := run()
		if !errors.Is(err, errPurgeUnknown) {
			return out, err
		}
		if attempt >= collectAttempts {
			var none T
			return none, ErrPurgeOverlap
		}
	}
}

// keyValues is the index hold of a gxid collect query: the key's index
// values in chain order (newest first), at most limit. over reports a
// further matching record and walkErr a chain error after the values; both
// are reported once the values before them were processed, as the former
// single walk did.
type keyValues struct {
	values  [][]byte
	over    bool
	walkErr error
}

var errKeyLimit = errors.New("key scan limit")

// keyChainIndexes are the indexes the collect queries and the txid lookup
// (XLogContext) walk from KeyChain snapshots. The Store only appends to
// them; Store.index marks them SetAppendOnly so an in-place rewrite (which
// a snapshot walk could see half-way) fails instead.
var keyChainIndexes = []string{"xlog_profile", "xlog_gid", "span_gid", "xlog_tid"}

func keyChainIndex(path string) bool {
	base := filepath.Base(path)
	return slices.Contains(keyChainIndexes, base)
}

// scanKeyValues walks the chain snapshot of an index hold, without the gate.
func scanKeyValues(ctx context.Context, chain *legacyindex.KeyChain, limit int) keyValues {
	var k keyValues
	err := chain.ScanContext(ctx, func(r legacyindex.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(k.values) >= limit {
			k.over = true
			return errKeyLimit
		}
		k.values = append(k.values, r.Value)
		return nil
	})
	if !errors.Is(err, errKeyLimit) {
		k.walkErr = err
	}
	return k
}

// profileIndexLocked is the index hold of a profile query: txid's chain
// snapshot and the source of its records. src == nil (with err == nil) means
// the date has no profile index. A compression mode error is reported after
// the walk (src.modeErr), as the single walk did.
func (s *Store) profileIndexLocked(ctx context.Context, date string, txid int64) (*legacyindex.KeyChain, *recordSource, error) {
	if err := s.check(date); err != nil {
		return nil, nil, err
	}
	path := s.path(date, "xlog", "xlog")
	idx, err := s.index(path+"_profile", false, false)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, nil, cancelled
	}
	if err != nil || idx == nil {
		return nil, nil, err
	}
	chain, err := idx.KeyChain(longKey(txid))
	if err != nil {
		return nil, nil, err
	}
	zip, modeErr := s.compressionMode(path+".profile.conf", "compress_profile_enabled", s.live.compressProfile.Load(), false)
	if modeErr != nil {
		src := s.openRecordSourceLocked(date, path+".profile", 4, false, false)
		src.modeErr = modeErr
		return chain, src, nil
	}
	return chain, s.openRecordSourceLocked(date, path+".profile", 4, zip, !chain.Empty()), nil
}

// profileOffsets walks the chain snapshot of a profile query (no gate): the
// data offsets of txid, newest first, at most MaxQueryRecords.
func (s *Store) profileOffsets(ctx context.Context, chain *legacyindex.KeyChain) ([]int64, error) {
	offsets := make([]int64, 0)
	err := chain.ScanContext(ctx, func(r legacyindex.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(offsets) >= s.config.MaxQueryRecords {
			return ErrLimit
		}
		pos, err := dataOffset(r.Value)
		if err != nil {
			return err
		}
		offsets = append(offsets, pos)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return offsets, ctx.Err()
}

func (s *Store) ProfileChunks(date string, txid int64) ([][]byte, error) {
	return s.ProfileChunksContext(context.Background(), date, txid)
}

// ProfileChunksContext returns txid's profile blocks in data file order
// (XLogProfileRD.getFullProfile), at most MaxQueryRecords blocks and
// MaxQueryBytes bytes. One Store gate hold takes the index snapshot; the
// walk and the blocks are read as described above (collect queries).
func (s *Store) ProfileChunksContext(ctx context.Context, date string, txid int64) ([][]byte, error) {
	return restartOnPurgeUnknown(func() ([][]byte, error) { return s.profileChunksOnce(ctx, date, txid) })
}

func (s *Store) profileChunksOnce(ctx context.Context, date string, txid int64) ([][]byte, error) {
	unlock, err := s.lockScan(ctx)
	if err != nil {
		return nil, err
	}
	chain, src, err := s.profileIndexLocked(ctx, date, txid)
	unlock()
	if err != nil || src == nil {
		return nil, err
	}
	defer chain.Close()
	defer src.close()
	offsets, err := s.profileOffsets(ctx, chain)
	if err != nil {
		return nil, err
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if src.modeErr != nil {
		return nil, src.modeErr
	}
	chunks := make([][]byte, 0, len(offsets))
	used := 0
	purged, err := s.eachRecord(ctx, src, len(offsets), func(i int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := s.readRecord(ctx, src, offsets[i])
		if err != nil {
			return err
		}
		used += len(b)
		if used > s.config.MaxQueryBytes {
			return ErrLimit
		}
		chunks = append(chunks, b)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if purged {
		return nil, nil // as a query after the purge: no profile index
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return chunks, nil
}

func (s *Store) Profile(date string, txid int64) ([]byte, error) {
	chunks, err := s.ProfileChunksContext(context.Background(), date, txid)
	if err != nil {
		return nil, err
	}
	size := 0
	for _, b := range chunks {
		size += len(b)
	}
	out := make([]byte, 0, size)
	for _, b := range chunks {
		out = append(out, b...)
	}
	return out, nil
}
