package storage

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

// zipBlockBytes is GZipCtr.BLOCK_MAX_SIZE, the logical size of one xlog.N block.
const zipBlockBytes = 32 << 20

// compressionMode preserves Java per-file Properties, including mode chosen by a
// previous writer. Java reads the file once per daily writer/reader open; the
// Store caches an existing file's mode under its gate instead of reopening it
// for every Pack (measured replay bottleneck). Purge drops cached entries.
func (s *Store) compressionMode(path, key string, fallback, create bool) (bool, error) {
	if s != nil {
		if mode, ok := s.modes[path]; ok {
			return mode, nil
		}
	}
	mode, present, err := loadCompressionMode(path, key, fallback, create)
	if err != nil || !present || s == nil {
		return mode, err
	}
	if s.modes == nil || len(s.modes) >= 256 {
		s.modes = make(map[string]bool)
	}
	s.modes[path] = mode
	return mode, nil
}
func loadCompressionMode(path, key string, fallback, create bool) (mode, present bool, err error) {
	b, err := limitedFile(path, 64<<10)
	if os.IsNotExist(err) {
		if !create {
			return fallback, false, nil
		}
		if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return false, false, err
		}
		err = atomicWrite(path, []byte(key+"="+strconv.FormatBool(fallback)+"\n"))
		return fallback, err == nil, err
	}
	if err != nil {
		return false, false, err
	}
	if len(b) > 64<<10 {
		return false, false, ErrLimit
	}
	value := strconv.FormatBool(fallback)
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}
		i := strings.IndexAny(line, "=: \t")
		if i < 0 {
			if line == key {
				value = ""
			}
			continue
		}
		if strings.TrimSpace(line[:i]) == key {
			value = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line[i:]), "=:"))
		}
	}
	return strings.EqualFold(strings.TrimSpace(value), "true"), true, nil
}

// ReadCompressionMode reads an existing Java .service.conf/.profile.conf with the
// same Properties subset as the Store. It never creates the file; a missing file
// returns fallback and present=false.
func ReadCompressionMode(path, key string, fallback bool) (mode, present bool, err error) {
	if _, err = os.Lstat(path); os.IsNotExist(err) {
		return fallback, false, nil
	} else if err != nil {
		return false, false, err
	}
	return loadCompressionMode(path, key, fallback, false)
}
func limitedFile(path string, max int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, int64(max)+1))
	if err != nil {
		return nil, err
	}
	if len(b) > max {
		return nil, ErrLimit
	}
	return b, nil
}
func atomicWrite(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".scouter-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	n, err := f.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(name, path)
}

// xlogBytesFetch reads the stored XLog bytes at pos of date, from the
// compressed blocks or the xlog.service data file as the date's mode says.
// With a fetch, a compressed record whose block is not cached is a *zipMiss
// (zip.go). A read that does not go
// through readZipFetch (an error first, or a date no longer compressed)
// releases what the fetch holds, as readZipFetch does when it ends.
func (s *Store) xlogBytesFetch(ctx context.Context, date string, pos int64, fetch *zipFetch) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		fetch.drop()
		return nil, err
	}
	path := s.path(date, "xlog", "xlog")
	zip, err := s.compressionMode(path+".service.conf", "compress_xlog_enabled", s.live.compressXLog.Load(), false)
	if err != nil {
		fetch.drop()
		return nil, err
	}
	if zip {
		return s.readZipFetch(ctx, date, pos, fetch)
	}
	fetch.drop()
	return s.readDataContext(ctx, path+".service", pos, 2)
}
func (s *Store) PutXLog(p *protocol.XLogPack) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if p == nil || p.EndTime <= 0 {
		return ErrInvalid
	}
	date := JavaDate(p.EndTime, s.config.Location)
	if err := validDate(date); err != nil {
		return err
	}
	b, err := protocol.EncodePack(p, s.config.CodecLimits)
	if err != nil {
		return err
	}
	if len(b) > s.config.MaxRecordBytes {
		return ErrLimit
	}
	return s.putXLogLocked(date, b, nil, p.EndTime, p.TxID, p.GxID)
}

// putXLogLocked writes one encoded XLog as XLogWR does: the data record, then
// the time, txid and (non-zero) gxid index entries. The caller holds Store.mu.
// member, when not nil, is b's gzip member (appendZipMember).
func (s *Store) putXLogLocked(date string, b, member []byte, endTime, txid, gxid int64) error {
	path := s.path(date, "xlog", "xlog")
	zip, err := s.compressionMode(path+".service.conf", "compress_xlog_enabled", s.live.compressXLog.Load(), true)
	if err != nil {
		return err
	}
	var pos int64
	if zip {
		pos, err = s.appendZipMember(date, b, member)
	} else {
		pos, err = s.appendData(path+".service", b, 2)
	}
	if err != nil {
		return err
	}
	for _, entry := range []struct {
		name  string
		key   []byte
		timed bool
	}{{"_tim", longKey(endTime), true}, {"_tid", longKey(txid), false}, {"_gid", longKey(gxid), false}} {
		if entry.name == "_gid" && gxid == 0 {
			continue
		}
		idx, err := s.index(path+entry.name, entry.timed, true)
		if err != nil {
			return err
		}
		if _, err = idx.Put(entry.key, offsetBytes(pos)); err != nil {
			return err
		}
	}
	return nil
}

// putXLogBatchLocked is putXLogLocked for a run of queued XLogs of one date:
// the data records in one write, then each index's records in one write
// (PutBatch). The files get the bytes consecutive putXLogLocked calls would
// give them, and each index record still follows its data. A record over a
// size limit fails alone, as it did; a write error fails the run.
func (s *Store) putXLogBatchLocked(date string, items []*packItem) []error {
	errs := make([]error, len(items))
	path := s.path(date, "xlog", "xlog")
	zip, err := s.compressionMode(path+".service.conf", "compress_xlog_enabled", s.live.compressXLog.Load(), true)
	if err != nil || zip || len(items) == 1 {
		for n, it := range items {
			if errs[n] = err; err == nil {
				errs[n] = s.putXLogLocked(date, it.b, it.member, it.time, it.txid, it.gxid)
			}
		}
		return errs
	}
	run := make([]*packItem, 0, len(items))
	data := make([][]byte, 0, len(items))
	for n, it := range items {
		if len(it.b) > s.config.MaxRecordBytes || len(it.b) > 32767 {
			errs[n] = ErrLimit
			continue
		}
		run = append(run, it)
		data = append(data, it.b)
	}
	fail := func(err error) []error {
		for n := range errs {
			if errs[n] == nil {
				errs[n] = err
			}
		}
		return errs
	}
	if len(run) == 0 {
		return errs
	}
	pos, err := s.appendDataBatch(path+".service", data, 2)
	if err != nil {
		return fail(err)
	}
	entries := make([]legacyindex.Entry, 0, len(run))
	for _, index := range []struct {
		name  string
		timed bool
		key   func(*packItem) int64
	}{{"_tim", true, func(it *packItem) int64 { return it.time }}, {"_tid", false, func(it *packItem) int64 { return it.txid }}, {"_gid", false, func(it *packItem) int64 { return it.gxid }}} {
		entries = entries[:0]
		for n, it := range run {
			if index.name == "_gid" && it.gxid == 0 {
				continue
			}
			entries = append(entries, legacyindex.Entry{Key: longKey(index.key(it)), Value: offsetBytes(pos[n])})
		}
		if len(entries) == 0 {
			continue
		}
		idx, err := s.index(path+index.name, index.timed, true)
		if err != nil {
			return fail(err)
		}
		if _, err = idx.PutBatch(entries); err != nil {
			return fail(err)
		}
	}
	return errs
}

// decodeXLogFetch decodes the XLog an index value points to, reading it
// through xlogBytesFetch.
func (s *Store) decodeXLogFetch(ctx context.Context, date string, value []byte, fetch *zipFetch) (*protocol.XLogPack, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	pos, err := dataOffset(value)
	if err != nil {
		return nil, 0, err
	}
	b, err := s.xlogBytesFetch(ctx, date, pos, fetch)
	if err != nil {
		return nil, 0, err
	}
	return s.decodeXLogBytes(ctx, b)
}

// decodeXLogFrom is decodeXLogFetch reading through a collect query's
// source (profile.go): same checks, same order.
func (s *Store) decodeXLogFrom(ctx context.Context, src *recordSource, value []byte) (*protocol.XLogPack, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	pos, err := dataOffset(value)
	if err != nil {
		return nil, 0, err
	}
	b, err := s.readRecord(ctx, src, pos)
	if err != nil {
		return nil, 0, err
	}
	return s.decodeXLogBytes(ctx, b)
}
func (s *Store) decodeXLogBytes(ctx context.Context, b []byte) (*protocol.XLogPack, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	p, err := protocol.DecodePack(b, s.config.CodecLimits)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, 0, cancelled
	}
	if err != nil {
		return nil, 0, err
	}
	x, ok := p.(*protocol.XLogPack)
	if !ok {
		return nil, 0, ErrCorrupt
	}
	return x, len(b), nil
}
func (s *Store) XLog(date string, txid int64) (*protocol.XLogPack, error) {
	return s.XLogContext(context.Background(), date, txid)
}

// XLogContext is XLogRD.getByTxid: the newest record of txid (Java
// IndexKeyFile.get, with its S152 deep search report on a miss).
//
// A short chain, the usual case (txids are nearly unique keys), is looked
// up in one Store gate hold that walks at most scanHoldSteps chain records
// or until scanHoldTime (legacyindex.Index.LookupWithin) and reads the record through the Store's
// handles, as the former lookup did. When the record's compressed block is
// not cached, the hold ends, the block is decoded outside the gate (zip.go,
// zipFetch) and the lookup runs again in a new hold.
//
// A longer chain (hash collisions, a small hash area) or a walk slower than
// scanHoldTime (cold page cache, slow disk) runs as the collect
// queries do (profile.go, xlogLongChain): one gate hold takes the chain
// snapshot (legacyindex.KeyChain) and the source of the record, the chain
// is walked without the gate however long it is, and the record is read
// without the gate (uncompressed, through the handle of the index hold) or
// in a gate hold (compressed). Store.index marks xlog_tid append-only, so
// the walk returns what one walk at the index hold would have.
//
// Either way, results and errors are those of the former single-hold
// lookup at the (last) index hold; on the long path a purge or DB delete of
// the date before a compressed read returns nothing, as a lookup after it
// would.
func (s *Store) XLogContext(ctx context.Context, date string, txid int64) (*protocol.XLogPack, error) {
	var fetch zipFetch
	defer fetch.drop()
	for {
		p, err := s.xlogShortChain(ctx, date, txid, &fetch)
		var miss *zipMiss
		switch {
		case errors.Is(err, errLongChain):
			fetch.drop()
			return restartOnPurgeUnknown(func() (*protocol.XLogPack, error) { return s.xlogLongChain(ctx, date, txid) })
		case errors.As(err, &miss):
			if err := s.fetchZip(ctx, &fetch, miss); err != nil {
				return nil, err
			}
		default:
			return p, err
		}
	}
}

// errLongChain ends xlogShortChain for a txid chain longer than its walk
// limit; nothing was read or reported.
var errLongChain = errors.New("txid chain longer than one gate hold")

// xlogLongChainObserver, when set by a test, runs when a txid lookup leaves
// the short path for xlogLongChain.
var xlogLongChainObserver func()

// xlogShortChain is the former single-hold lookup with the chain walk
// bounded like the other gate holds: scanHoldSteps records or scanHoldTime
// from the start of the hold, whichever comes first.
func (s *Store) xlogShortChain(ctx context.Context, date string, txid int64, fetch *zipFetch) (*protocol.XLogPack, error) {
	unlock, err := s.lockScan(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	deadline := time.Now().Add(scanHoldTime)
	if err := s.check(date); err != nil {
		return nil, err
	}
	idx, err := s.index(s.path(date, "xlog", "xlog_tid"), false, false)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if err != nil || idx == nil {
		return nil, err
	}
	value, done, err := idx.LookupWithin(ctx, longKey(txid), scanHoldSteps, deadline)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if !done {
		if observe := xlogLongChainObserver; observe != nil {
			observe()
		}
		return nil, errLongChain
	}
	if err != nil || value == nil {
		return nil, err
	}
	p, _, err := s.decodeXLogFetch(ctx, date, value, fetch)
	if err == nil && p.TxID != txid {
		return nil, ErrCorrupt
	}
	return p, err
}

// xlogLongChain is the lookup of a txid chain longer than one gate hold.
func (s *Store) xlogLongChain(ctx context.Context, date string, txid int64) (*protocol.XLogPack, error) {
	unlock, err := s.lockScan(ctx)
	if err != nil {
		return nil, err
	}
	chain, src, err := s.txidIndexLocked(ctx, date, txid)
	unlock()
	if err != nil || src == nil {
		return nil, err
	}
	defer chain.Close()
	defer src.close()
	value, err := chain.LookupContext(ctx)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if err != nil || value == nil {
		return nil, err
	}
	var p *protocol.XLogPack
	purged, err := s.eachRecord(ctx, src, 1, func(int) error {
		pack, _, err := s.decodeXLogFrom(ctx, src, value)
		if err != nil {
			return err
		}
		if pack.TxID != txid {
			return ErrCorrupt
		}
		p = pack
		return nil
	})
	if err != nil || purged {
		return nil, err // purged: as a lookup after the purge, no index
	}
	return p, nil
}

// txidIndexLocked is the index hold of a txid lookup (gxidIndexLocked for
// xlog_tid): src == nil (with err == nil) means the date has no txid index.
func (s *Store) txidIndexLocked(ctx context.Context, date string, txid int64) (*legacyindex.KeyChain, *recordSource, error) {
	if err := s.check(date); err != nil {
		return nil, nil, err
	}
	idx, err := s.index(s.path(date, "xlog", "xlog_tid"), false, false)
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
	path := s.path(date, "xlog", "xlog")
	zip, modeErr := s.compressionMode(path+".service.conf", "compress_xlog_enabled", s.live.compressXLog.Load(), false)
	if modeErr != nil {
		src := s.openRecordSourceLocked(date, path+".service", 2, false, false)
		src.modeErr = modeErr
		return chain, src, nil
	}
	return chain, s.openRecordSourceLocked(date, path+".service", 2, zip, !chain.Empty()), nil
}
func (s *Store) XLogs(date string, start, end int64, reverse bool) ([]*protocol.XLogPack, error) {
	return s.XLogsContext(context.Background(), date, start, end, reverse)
}

// XLogsContext collects ScanXLogsContext (V1 order) into a slice for callers
// that need one (console, migration checks). The collected result keeps the
// MaxQueryRecords/MaxQueryBytes caps; Client queries stream instead.
func (s *Store) XLogsContext(ctx context.Context, date string, start, end int64, reverse bool) ([]*protocol.XLogPack, error) {
	var out []*protocol.XLogPack
	used := 0
	err := s.ScanXLogsContext(ctx, XLogScan{Date: date, From: start, To: end, Reverse: reverse}, func(r XLogRecord) (bool, error) {
		if len(out) >= s.config.MaxQueryRecords || used > s.config.MaxQueryBytes-r.Bytes {
			return false, ErrLimit
		}
		used += r.Bytes
		out = append(out, r.Pack)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

type XLogRecord struct {
	BucketTime int64
	Pack       *protocol.XLogPack
	// Bytes is the stored (encoded) size of Pack.
	Bytes int
}

func (s *Store) XLogsV2(date string, start, end, lastBucket int64) ([]XLogRecord, error) {
	return s.XLogsV2Context(context.Background(), date, start, end, lastBucket)
}

// XLogsV2Context collects a Paged ScanXLogsContext with the collected-result
// caps of XLogsContext.
func (s *Store) XLogsV2Context(ctx context.Context, date string, start, end, lastBucket int64) ([]XLogRecord, error) {
	var out []XLogRecord
	used := 0
	err := s.ScanXLogsContext(ctx, XLogScan{Date: date, From: start, To: end, Paged: true, LastBucket: lastBucket}, func(r XLogRecord) (bool, error) {
		if len(out) >= s.config.MaxQueryRecords || used > s.config.MaxQueryBytes-r.Bytes {
			return false, ErrLimit
		}
		used += r.Bytes
		out = append(out, r)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
