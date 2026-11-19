// Scouter DB formats: Copyright 2015 Scouter authors, Apache-2.0.
// Independent implementation of pinned source; see compatibility/storage-contract.md.
package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
	"github.com/snowvil/scouter-go/internal/syncx"
)

var (
	ErrInvalid = protocol.ErrInvalid
	ErrLimit   = protocol.ErrLimit
	ErrClosed  = os.ErrClosed
	ErrCorrupt = errors.New("corrupt Scouter DB")
)

type Config struct {
	Directory   string
	Location    *time.Location
	CodecLimits protocol.Limits
	// CompressXLog/CompressProfile and TextDaily* are the initial values of
	// the live settings (SetCompression, SetTextDaily).
	CompressXLog, CompressProfile               bool
	TextDailyService, TextDailyAPI, TextDailyUA bool
	// HashSizeBytes sizes new hash indexes that have no scouter.conf size
	// key, and those whose key is unset (see IndexSizes).
	HashSizeBytes, MaxRecordBytes, MaxQueryRecords, MaxQueryBytes int
	// IndexSizes are the server-start *_mb index creation sizes.
	IndexSizes IndexSizesMB
	// Log receives Java-id warnings (S152/S161 index traversal); nil drops them.
	Log          LogFunc
	MaxQuerySpan time.Duration
	// OnRecover observes every automatic crash recovery; it runs under the
	// Store gate and must not call back into the Store.
	OnRecover func(RecoveryEvent)
	// XLogQueueSize and ProfileQueueSize bound the XLog and profile writer
	// queues (xlog_queue_size, profile_queue_size; read at start like the
	// original's RequestQueue). 0 means 10000 and 1000.
	XLogQueueSize, ProfileQueueSize int
	// TextCacheBytes bounds the text read cache (textQueue.read); 0 means
	// TextCacheDefaultBytes.
	TextCacheBytes int
	// DeferVisitorFlush leaves the 10s visitor snapshot writes to an owner that
	// calls FlushVisitorsIfDue (the runtime maintenance loop) instead of the
	// ingest goroutine that crossed the 10s tick.
	DeferVisitorFlush bool
}

// RecoveryEvent reports an automatic repair of a crash-consistent state.
// Anything outside the documented recoverable shapes stays ErrCorrupt.
type RecoveryEvent struct {
	Path, Kind, Detail string
}

func (s *Store) recovered(e RecoveryEvent) {
	if s.config.OnRecover != nil {
		s.config.OnRecover(e)
	}
}
func (s *Store) reportIndexRecovery(path string, info legacyindex.RecoveryInfo) {
	// A refused relink leaves the file unchanged, so every reopen sees it
	// again; report it once per path and process.
	if info.RelinkRefused != "" {
		if s.refusedReported[path] {
			info.RelinkRefused = ""
		} else {
			if s.refusedReported == nil || len(s.refusedReported) >= 1024 {
				s.refusedReported = make(map[string]bool)
			}
			s.refusedReported[path] = true
		}
	}
	reportIndexRecovery(s.config.OnRecover, path, info)
}
func reportIndexRecovery(hook func(RecoveryEvent), path string, info legacyindex.RecoveryInfo) {
	if hook == nil {
		return
	}
	if info.RelinkRefused != "" {
		// Not a repair: Java semantics keep these records unreachable.
		hook(RecoveryEvent{Path: path, Kind: "index-relink-refused", Detail: fmt.Sprintf("%d unreachable record(s) left as Java would: %s", info.Unreachable, info.RelinkRefused)})
	}
	if !info.Recovered() {
		return
	}
	hook(RecoveryEvent{Path: path, Kind: "index-relinked", Detail: fmt.Sprintf("relinked %d record(s) in %d bucket(s), count %d -> %d, truncated torn tail %d byte(s)", info.RelinkedRecords, info.RelinkedBuckets, info.CountBefore, info.CountAfter, info.TruncatedTailBytes)})
}

type Store struct {
	mu     syncx.Mutex
	config Config
	// archiveUploads and archiveReads admit batchArchiveConcurrent batch
	// archive uploads and reads each (their I/O runs outside the gate).
	archiveUploads, archiveReads chan struct{}
	closed                       bool
	indexes                      map[string]*legacyindex.Index
	files                        map[string]*os.File
	visitor                      *visitorState
	tagCounts                    *tagCountState
	ttlIndexes                   map[string]*legacyindex.TTLIndex
	// lastUse orders bounded file/index cache eviction least-recently-used first.
	lastUse map[string]uint64
	clock   uint64
	// modes caches existing .service.conf/.profile.conf compression modes.
	modes           map[string]bool
	refusedReported map[string]bool
	// ends caches the append offset of cached data files (see appendData).
	ends map[string]int64
	// appendBuf is appendData's reused record buffer (guarded by mu).
	appendBuf []byte
	zip       *zipState // GZipStore block cache and writer positions (zip.go)
	// zipDecodes admits zipDecodeConcurrent compressed block decodes
	// outside the gate (zip.go, fetchZip).
	zipDecodes chan struct{}
	// visitorMu guards visitor and visitorClosed: ingest updates visitors
	// without the Store gate. Order: mu, visitorMu, visitorIO; purge, delete
	// and Close take it under mu.
	visitorMu     sync.Mutex
	visitorClosed bool
	// visitorResidentMB and visitorDeltaMB are the SetVisitorBudgets values
	// (0: the defaults); visitorBudgetGen counts the calls, so the visitor
	// state applies a change once, under visitorMu (applyVisitorBudgetsLocked).
	visitorResidentMB, visitorDeltaMB atomic.Int32
	visitorBudgetGen                  atomic.Uint64
	// visitorIO orders visitor snapshot writes done outside visitorMu (taken
	// under it).
	visitorIO sync.Mutex
	// visitorBuf is the snapshot image buffer of flushVisitors, owned by the
	// holder of visitorIO.
	visitorBuf []byte
	// closes defers the close of handles evicted while SyncData fsyncs outside
	// mu, so eviction never waits for that pass. syncRun admits one SyncData
	// (or Close) at a time and is held until the deferred closes ran.
	closes  closeQueue
	syncRun sync.Mutex
	// tagRun admits one tag pass (drain, flush or top100 rebuild,
	// tagcount_flush.go) at a time. Order: syncRun, tagRun, mu, tags.mu.
	tagRun sync.Mutex
	// tags is the tag count state with its own gate and file handles
	// (tagstore.go); tagCounts is guarded by tags.mu, not mu.
	tags tagState
	// texts queues text dictionary writes for the text writer (textqueue.go).
	texts textQueue
	// xlogQ and profQ queue XLog and profile records for their writers
	// (packwriter.go). packSeq numbers purges and deletes; packDiscards
	// (guarded by mu) lists the recent ones the writers check.
	xlogQ, profQ packQueue
	packSeq      atomic.Uint64
	packDiscards []packDiscard
	// Test hooks, set before use: tagHoldSteps overrides the records per tag
	// pass hold (0: scanHoldSteps) and tagBetweenHolds runs outside the tag
	// gate between the holds of a tag pass.
	tagHoldSteps    int
	tagScanMax      int // records a top100 rebuild scans (0: tagScanMaxRecords; tests)
	tagBetweenHolds func()
	tagQueryBetween func() // between the holds of a query scan (tests)
	tagNoRest       bool   // tag passes skip their rest between holds (tests)
	tagFullRewrite  bool   // flush existing tag records by whole rewrite (equivalence tests)
	// lockStats turns on the measurement-only holder sums (TrackLocks).
	lockStats atomic.Bool
	// tagWriteHook, when set, can fail a tag file write before it happens (tests).
	tagWriteHook func(path string, pos int64, n int) error
	// Tag counters (TagCountStats).
	tagInline, tagBackpressure, tagApplyErrors, tagPasses, tagDropped, tagInboxDropped, tagPendingDropped atomic.Uint64
	tagCacheLimited                                                                                       atomic.Uint64
	tagPassMax, tagPassLast                                                                               atomic.Int64
	// counters shares the root and is released by DeleteDatabasePaths
	// (AttachCounters in delete.go); guarded by mu.
	counters *Counters
	// live holds scouter.conf values re-read on use (live.go); never needs mu.
	live live
	// closeStage is the step Close is in (CloseStage); never needs mu.
	closeStage atomic.Pointer[string]
	// noClean (SkipCleanMarker) keeps Close from writing .go-clean;
	// markerWritten reports that it did (CleanMarkerWritten).
	noClean, markerWritten atomic.Bool
	// opens counts how index() accepted existing pairs; it has its own leaf
	// lock so /health never waits for a gate held by a long index walk.
	opens openCounts
	// clean holds the clean-shutdown marker read by Open (clean.go); guarded by mu.
	clean cleanState
	// checkpoints is the crash checkpoint log of the Store's index and KV
	// pairs (clean.go); it has its own lock.
	checkpoints *checkpointLog
}

// IndexOpenStats counts how the Store opened legacy index pairs: Walked
// pairs were validated record by record (O(file size)), Sealed pairs matched
// the clean-shutdown marker and skipped that walk, Checkpointed pairs matched
// a crash checkpoint and only the records appended after it were validated,
// Created pairs were new, Failed opens returned an error (a pair failing
// validation is counted here). CheckpointRejected counts opens that had a
// checkpoint but did not use it (changed header, shorter file, or a tail the
// full walk had to decide). WalkedRecords/WalkedBytes sum the records and key
// bytes validated one by one. CleanMarker/CleanClosedAt describe the marker
// Open found (clean.go).
type IndexOpenStats struct {
	Walked, Sealed, Checkpointed, Created, Failed uint64
	CheckpointRejected                            uint64
	WalkedRecords, WalkedBytes                    uint64
	CleanMarker                                   bool
	CleanClosedAt                                 time.Time
}

func (st *IndexOpenStats) count(info legacyindex.OpenInfo) {
	switch info.Check {
	case legacyindex.CheckWalked:
		st.Walked++
	case legacyindex.CheckSealed:
		st.Sealed++
	case legacyindex.CheckTail:
		st.Checkpointed++
	case legacyindex.CheckCreated:
		st.Created++
	}
	if info.CheckpointRejected != "" {
		st.CheckpointRejected++
	}
	st.WalkedRecords += uint64(info.WalkedRecords)
	st.WalkedBytes += uint64(info.WalkedBytes)
}

// countOpen counts an accepted open; a new pair forgets its old checkpoint
// (the path was deleted and created again). Caller holds mu.
func (s *Store) countOpen(path string, info legacyindex.OpenInfo) {
	s.opens.count(info)
	if info.Check == legacyindex.CheckCreated {
		s.checkpoints.drop(path)
	}
}

// IndexOpenStats returns the counters since Open (Store index and KV pairs).
// It does not take the Store gate; the marker fields are set by Open.
func (s *Store) IndexOpenStats() IndexOpenStats {
	st := s.opens.get()
	st.CleanMarker, st.CleanClosedAt = s.clean.found, s.clean.closedAt
	return st
}

// openCounts is IndexOpenStats behind a leaf lock (taken under the gates,
// never the other way round).
type openCounts struct {
	mu sync.Mutex
	st IndexOpenStats
}

func (o *openCounts) count(info legacyindex.OpenInfo) {
	o.mu.Lock()
	o.st.count(info)
	o.mu.Unlock()
}
func (o *openCounts) failed() {
	o.mu.Lock()
	o.st.Failed++
	o.mu.Unlock()
}
func (o *openCounts) get() IndexOpenStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.st
}

// closeQueue owns the lifecycle of cached handles that leave a cache while a
// sync pass (SyncData / Counters.Sync) fsyncs them outside the gate.
//
// Rules, all under the owner's gate unless noted:
//   - begin marks a pass in flight; the pass then fsyncs outside the gate.
//   - release closes a handle that just left its cache. With no pass in
//     flight it closes in place (the victim's own fsync only). During a pass
//     the close is queued instead, so the gate is never held across the
//     pass's fsyncs and no handle is closed under a Commit in flight.
//   - drain (called by the pass after its fsyncs, outside the gate, with the
//     pass's syncRun still held) closes the queued handles in order.
//   - adopt is consulted before a path is (re)opened: a queued handle for the
//     same key goes back into its cache instead of a second handle being
//     opened, and a key whose close is running right now waits for that one
//     close. Either way the old handle's final header can never overwrite
//     records written through a newer handle at the same path.
//   - The queue is bounded (maxPendingCloses): beyond it a handle is closed in
//     place even during a pass. The pass tolerates os.ErrClosed from such a
//     handle because every path that closes a cached handle fsyncs it first
//     (eviction, archive replacement, Close) or is purging it (retention).
type closeQueue struct {
	syncing bool
	pending []*pendingClose
	closing *pendingClose
	// fsync replaces (*os.File).Sync in tests that model a slow disk.
	fsync func(*os.File) error
}

const maxPendingCloses = 32

type pendingClose struct {
	key   string       // cache key; "" when the handle must never return to a cache
	adopt func()       // puts the handle back into its cache (key != "")
	close func() error // closes (and, for data files, fsyncs) the handle
	// after, when set, runs under the owner's gate once close returned;
	// forgotten is true when the entry's key was cleared meanwhile (a purge
	// or DB delete moved its files away, retention forgetPendingLocked). It
	// records the crash checkpoint of an evicted index.
	after func(forgotten bool)
	done  chan struct{}
}

func (p *pendingClose) finish() {
	if p.after != nil {
		p.after(p.key == "")
	}
}

func (q *closeQueue) syncFile(f *os.File) error {
	if q.fsync != nil {
		return q.fsync(f)
	}
	return f.Sync()
}

// syncClosedOK fsyncs a data file that may already have been closed. A cached
// data file is only ever closed after its fsync or while its date is purged,
// so a closed handle needs nothing more.
func (q *closeQueue) syncClosedOK(f *os.File) error {
	if err := q.syncFile(f); err != nil && !errors.Is(err, os.ErrClosed) {
		return err
	}
	return nil
}

func (q *closeQueue) begin() { q.syncing = true }

func (q *closeQueue) release(p *pendingClose) error {
	if !q.syncing || len(q.pending) >= maxPendingCloses {
		err := p.close()
		p.finish()
		return err
	}
	p.done = make(chan struct{})
	q.pending = append(q.pending, p)
	return nil
}

// adopt re-adopts a queued handle for key (true) or waits for its running
// close (false, open afresh). The owner's gate is held throughout; the wait is
// one handle's close, never a sync pass.
func (q *closeQueue) adopt(key string) bool {
	for i, p := range q.pending {
		if p.key == key {
			q.pending = append(q.pending[:i], q.pending[i+1:]...)
			p.adopt()
			return true
		}
	}
	if c := q.closing; c != nil && c.key == key {
		<-c.done
	}
	return false
}

// drain ends the pass and closes the queued handles one at a time outside mu.
// Callers hold their syncRun, so no other pass begins before drain returns.
func (q *closeQueue) drain(mu *syncx.Mutex) error {
	var errs []error
	mu.Lock()
	q.syncing = false
	for len(q.pending) > 0 {
		p := q.pending[0]
		q.pending = q.pending[1:]
		q.closing = p
		mu.Unlock()
		err := p.close()
		close(p.done) // before retaking mu: an adopt may wait on it under mu
		mu.Lock()
		p.finish()
		q.closing = nil
		errs = append(errs, err)
	}
	mu.Unlock()
	return errors.Join(errs...)
}

// closeAll closes queued handles in place; only for Close, which holds syncRun
// so that no pass is in flight and the queue is normally empty already.
func (q *closeQueue) closeAll() error {
	var errs []error
	for _, p := range q.pending {
		errs = append(errs, p.close())
		p.finish()
		close(p.done)
	}
	q.pending, q.syncing = nil, false
	return errors.Join(errs...)
}

// filesUnderLocked lists cached data files below dir. Every index references
// only data files in its own directory subtree ({date}/{kind}/..., including
// the compressed {date}/xlog/data/ blocks), so these are the files that must
// reach disk before that index's header.
func (s *Store) filesUnderLocked(dir string) []*os.File {
	prefix := dir + string(filepath.Separator)
	var files []*os.File
	for p, f := range s.files {
		if strings.HasPrefix(p, prefix) {
			files = append(files, f)
		}
	}
	return files
}

// evictIndexLocked removes the least recently used index from the cache and
// closes it (or queues the close during a sync pass). Data reaches disk before
// the header that references it: the index's data files are fsynced first.
// When that fsync fails in place the victim stays cached, as before, so no
// header is written over unsynced data.
func (s *Store) evictIndexLocked() error {
	victim := leastRecent(s.indexes, s.lastUse)
	idx := s.indexes[victim]
	files := s.filesUnderLocked(filepath.Dir(victim))
	if !s.closes.syncing {
		for _, f := range files {
			if err := s.closes.syncFile(f); err != nil {
				return err
			}
		}
		files = nil
	}
	// A failed Close still releases the handle; keeping a closed index cached
	// would turn every later lookup of this path into ErrClosed.
	delete(s.indexes, victim)
	delete(s.lastUse, victim)
	dataOK := false
	return s.closes.release(&pendingClose{
		key:   victim,
		adopt: func() { s.indexes[victim] = idx; s.touch(victim) },
		close: func() error {
			var errs []error
			for _, f := range files { // deferred: fsync may have been done by a later close
				errs = append(errs, s.closes.syncClosedOK(f))
			}
			dataErr := errors.Join(errs...)
			dataOK = dataErr == nil
			return errors.Join(dataErr, idx.Close())
		},
		after: func(forgotten bool) {
			if dataOK && !forgotten {
				s.recordEvictedLocked(victim, idx.Checkpoint)
			}
		},
	})
}

// recordEvictedLocked records the checkpoint an evicted handle's close
// established (closeQueue runs it under the gate, pendingClose.after), unless
// the path is cached again meanwhile (a newer handle: its own checkpoints
// count) or its key file is gone (purged or deleted while the close ran).
func (s *Store) recordEvictedLocked(path string, checkpoint func() (legacyindex.Checkpoint, bool)) {
	if s.indexes[path] != nil || s.ttlIndexes[path] != nil {
		return
	}
	if _, err := os.Stat(keyFileOf(path)); err != nil {
		return
	}
	cp, ok := checkpoint()
	s.checkpoints.record(path, cp, ok)
}

// evictFileLocked removes the least recently used data file from the cache
// and fsyncs+closes it (or queues that during a sync pass).
func (s *Store) evictFileLocked() error {
	victim := leastRecent(s.files, s.lastUse)
	f := s.files[victim]
	delete(s.files, victim)
	delete(s.ends, victim)
	delete(s.lastUse, victim)
	return s.closes.release(&pendingClose{
		key:   victim,
		adopt: func() { s.files[victim] = f; s.touch(victim) },
		close: func() error { return errors.Join(s.closes.syncFile(f), f.Close()) },
	})
}

func (s *Store) touch(key string) {
	if s.lastUse == nil {
		s.lastUse = make(map[string]uint64)
	}
	s.clock++
	s.lastUse[key] = s.clock
}

// leastRecent picks the eviction victim; keys are sorted first so ties are deterministic.
func leastRecent[V any](m map[string]V, lastUse map[string]uint64) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	victim := keys[0]
	for _, k := range keys[1:] {
		if lastUse[k] < lastUse[victim] {
			victim = k
		}
	}
	return victim
}

type queryReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r queryReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(b)
	if cancelled := r.ctx.Err(); cancelled != nil {
		return n, cancelled
	}
	return n, err
}

var errLookupDone = errors.New("storage index lookup complete")

// queryGet is Java IndexKeyFile.get (newest live match; a miss may log S152).
func queryGet(ctx context.Context, idx *legacyindex.Index, key []byte) ([]byte, error) {
	value, err := idx.LookupContext(ctx, key)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	return value, err
}
func (s *Store) readDataContext(ctx context.Context, path string, pos int64, prefix int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, err := s.readData(path, pos, prefix)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	return b, err
}

func Open(c Config) (*Store, error) {
	if c.Directory == "" || c.Location == nil {
		return nil, fmt.Errorf("directory and timezone required: %w", ErrInvalid)
	}
	if c.TextCacheBytes < 0 {
		return nil, ErrInvalid
	}
	if c.HashSizeBytes < 0 || c.MaxRecordBytes < 0 || c.MaxQueryRecords < 0 || c.MaxQueryBytes < 0 || c.MaxQuerySpan < 0 {
		return nil, ErrInvalid
	}
	if c.HashSizeBytes == 0 {
		c.HashSizeBytes = 1 << 20
	}
	if c.XLogQueueSize <= 0 {
		c.XLogQueueSize = xlogQueueDefault
	}
	if c.ProfileQueueSize <= 0 {
		c.ProfileQueueSize = profQueueDefault
	}
	if c.MaxRecordBytes == 0 {
		c.MaxRecordBytes = 4 << 20
	}
	if c.MaxQueryRecords == 0 {
		c.MaxQueryRecords = 100000
	}
	if c.MaxQueryBytes == 0 {
		c.MaxQueryBytes = 64 << 20
	}
	if c.MaxQuerySpan == 0 {
		c.MaxQuerySpan = 24 * time.Hour
	}
	if c.HashSizeBytes < 5 || c.HashSizeBytes > 64<<20 || c.MaxRecordBytes > 32<<20 {
		return nil, ErrLimit
	}
	if err := c.IndexSizes.validate(); err != nil {
		return nil, err
	}
	// Validate codec settings without allocating a user payload.
	if _, err := protocol.EncodePack(&protocol.MapPack{}, c.CodecLimits); err != nil {
		return nil, err
	}
	root, err := filepath.Abs(c.Directory)
	if err != nil {
		return nil, err
	}
	c.Directory = root
	if err = os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	clean, err := consumeCleanMarker(root)
	if err != nil {
		return nil, err
	}
	s := &Store{config: c, indexes: make(map[string]*legacyindex.Index), files: make(map[string]*os.File), clean: clean, checkpoints: loadCheckpoints(root, checkpointName)}
	s.SetTextDaily(c.TextDailyService, c.TextDailyAPI, c.TextDailyUA)
	s.SetCompression(c.CompressXLog, c.CompressProfile)
	s.live.warn.setLog(c.Log)
	s.tags.space, s.tags.drainNow = make(chan struct{}, 1), make(chan struct{}, 1)
	s.archiveUploads = make(chan struct{}, batchArchiveConcurrent)
	s.archiveReads = make(chan struct{}, batchArchiveConcurrent)
	s.zipDecodes = make(chan struct{}, zipDecodeConcurrent)
	s.startTextWriter()
	s.startPackWriters()
	return s, nil
}
func validDate(date string) error {
	if len(date) != 8 {
		return ErrInvalid
	}
	if _, err := time.Parse("20060102", date); err != nil {
		return fmt.Errorf("date %q: %w", date, ErrInvalid)
	}
	return nil
}
func (s *Store) check(date string) error {
	if s.closed {
		return ErrClosed
	}
	return validDate(date)
}
func (s *Store) path(date, dir, name string) string {
	return filepath.Join(s.config.Directory, date, dir, name)
}
func (s *Store) index(path string, timed, create bool) (*legacyindex.Index, error) {
	if idx := s.indexes[path]; idx != nil {
		s.touch(path)
		return idx, nil
	}
	if s.closes.adopt(path) {
		idx := s.indexes[path]
		for len(s.indexes) > 32 {
			if err := s.evictIndexLocked(); err != nil {
				return nil, err
			}
		}
		return idx, nil
	}
	if !create {
		if _, err := os.Stat(path + ".kfile"); err != nil {
			if os.IsNotExist(err) {
				return nil, nil
			}
			return nil, err
		}
	}
	if len(s.indexes) >= 32 {
		if err := s.evictIndexLocked(); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	check, done := s.relinkCheckFor(path)
	idx, err := legacyindex.OpenCheckpointed(path, s.hashSizeFor(path), timed, s.config.Location, check, s.clean.takeSeal(path), s.checkpoints.get(path))
	done()
	if err != nil {
		s.opens.failed()
		return nil, err
	}
	if !timed {
		idx.SetDeepSearch(s.live.warn.hook(javaIndexKeyText))
	}
	if keyChainIndex(path) {
		idx.SetAppendOnly()
	}
	s.countOpen(path, idx.OpenInfo())
	s.reportIndexRecovery(path, idx.Recovered())
	s.indexes[path] = idx
	s.touch(path)
	return idx, nil
}
func (s *Store) file(path string, create bool) (*os.File, error) {
	if f := s.files[path]; f != nil {
		s.touch(path)
		return f, nil
	}
	if s.closes.adopt(path) {
		f := s.files[path]
		for len(s.files) > 16 {
			if err := s.evictFileLocked(); err != nil {
				return nil, err
			}
		}
		return f, nil
	}
	if len(s.files) >= 16 {
		if err := s.evictFileLocked(); err != nil {
			return nil, err
		}
	}
	flag := os.O_RDWR
	if create {
		flag |= os.O_CREATE
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, flag, 0600)
	if os.IsNotExist(err) && !create {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s.files[path] = f
	s.touch(path)
	return f, nil
}

// appendBufMax bounds the record buffer appendData keeps (Store.appendBuf):
// profiles of up to 1 MiB reuse it, a larger record gets its own.
const appendBufMax = 1 << 20

func (s *Store) appendData(path string, data []byte, prefix int) (int64, error) {
	one := [1][]byte{data}
	pos, err := s.appendDataBatch(path, one[:], prefix)
	if err != nil {
		return 0, err
	}
	return pos[0], nil
}

// appendDataBatch appends records, each with its length prefix, in one
// write (the XLog and profile writers append a hold of records at once). The
// file gets the bytes consecutive appendData calls would give it. It returns
// each record's position.
func (s *Store) appendDataBatch(path string, data [][]byte, prefix int) ([]int64, error) {
	size := 0
	for _, d := range data {
		if len(d) > s.config.MaxRecordBytes {
			return nil, ErrLimit
		}
		if prefix == 2 && len(d) > 32767 {
			return nil, ErrLimit
		}
		size += prefix + len(d)
	}
	f, err := s.file(path, true)
	if err != nil {
		return nil, err
	}
	// The Store is the only writer, so the append offset is kept in memory
	// instead of an lseek per record (a measured hot syscall). Any write error
	// drops it and the next append re-reads the real end.
	pos, known := s.ends[path]
	if !known {
		if pos, err = f.Seek(0, io.SeekEnd); err != nil {
			return nil, err
		}
	}
	if pos > 1<<39-1 {
		return nil, ErrLimit
	}
	// One record buffer is reused under the gate (every caller holds it);
	// a larger write gets its own and is not kept.
	var buf []byte
	if size <= appendBufMax {
		if cap(s.appendBuf) < size {
			s.appendBuf = make([]byte, appendBufMax)
		}
		buf = s.appendBuf[:size]
	} else {
		buf = make([]byte, size)
	}
	positions := make([]int64, len(data))
	at := 0
	for n, d := range data {
		positions[n] = pos + int64(at)
		if prefix == 2 {
			binary.BigEndian.PutUint16(buf[at:], uint16(len(d)))
		} else {
			binary.BigEndian.PutUint32(buf[at:], uint32(len(d)))
		}
		copy(buf[at+prefix:], d)
		at += prefix + len(d)
	}
	n, err := f.WriteAt(buf, pos)
	if err == nil && n != len(buf) {
		err = io.ErrShortWrite
	}
	if err != nil {
		delete(s.ends, path)
		return nil, err
	}
	if s.ends == nil {
		s.ends = make(map[string]int64)
	}
	s.ends[path] = pos + int64(n)
	return positions, nil
}
func (s *Store) readData(path string, pos int64, prefix int) ([]byte, error) {
	if pos < 0 {
		return nil, ErrCorrupt
	}
	f, err := s.file(path, false)
	if err != nil {
		return nil, err
	}
	if f == nil {
		return nil, fmt.Errorf("missing data file: %w", ErrCorrupt)
	}
	var head [4]byte
	if _, err = f.ReadAt(head[:prefix], pos); err != nil {
		return nil, err
	}
	n := 0
	if prefix == 2 {
		n = int(int16(binary.BigEndian.Uint16(head[:2])))
	} else {
		n = int(int32(binary.BigEndian.Uint32(head[:])))
	}
	if n < 0 {
		return nil, ErrCorrupt
	}
	if n > s.config.MaxRecordBytes {
		return nil, ErrLimit
	}
	data := make([]byte, n)
	_, err = f.ReadAt(data, pos+int64(prefix))
	return data, err
}
func offsetBytes(pos int64) []byte {
	return []byte{byte(pos >> 32), byte(pos >> 24), byte(pos >> 16), byte(pos >> 8), byte(pos)}
}
func dataOffset(b []byte) (int64, error) {
	if len(b) != 5 {
		return 0, ErrCorrupt
	}
	pos := int64(int8(b[0]))<<32 | int64(b[1])<<24 | int64(b[2])<<16 | int64(b[3])<<8 | int64(b[4])
	if pos < 0 {
		return pos, ErrCorrupt
	}
	return pos, nil
}
func intKey(v int32) []byte { b := make([]byte, 4); binary.BigEndian.PutUint32(b, uint32(v)); return b }
func longKey(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// syncLocked (Close) applies and flushes tag aggregation and closes the tag
// files (tagClose), then fsyncs data before index headers.
// Visitor snapshots are never fsynced (Java VisitorDB parity) and keep their
// own 10s cadence outside the gate; Close flushes them. failed lists the data
// files whose fsync failed (their indexes get no crash checkpoint).
func (s *Store) syncLocked() (failed []string, err error) {
	var errs []error
	errs = append(errs, s.tagClose())
	for path, f := range s.files {
		if ferr := s.closes.syncFile(f); ferr != nil {
			failed = append(failed, path)
			errs = append(errs, ferr)
		}
	}
	for _, idx := range s.ttlIndexes {
		errs = append(errs, idx.Sync())
	}
	for _, idx := range s.indexes {
		errs = append(errs, idx.Sync())
	}
	return failed, errors.Join(errs...)
}

// dataFailedFor reports whether a data file an index may reference (one in
// its directory subtree, see filesUnderLocked) failed its fsync: that index's
// header then may point at records that are not durable, so no crash
// checkpoint is recorded for it.
func dataFailedFor(failed []string, index string) bool {
	prefix := filepath.Dir(index) + string(filepath.Separator)
	for _, f := range failed {
		if strings.HasPrefix(f, prefix) {
			return true
		}
	}
	return false
}

// Sync writes dirty visitor snapshots and pending tag records (outside the
// gate or in bounded holds) and then fsyncs data files before index headers.
func (s *Store) Sync() error {
	ctx := context.Background()
	return errors.Join(s.flushPacks(ctx), s.flushTexts(ctx), s.flushVisitors(ctx), s.flushTagCounts(ctx), s.SyncData())
}

// SyncData is the periodic durability point: data fsync, then index headers.
// Visitor snapshots and tag records are skipped; they keep their own 10s and
// 5s cadence (FlushVisitorsIfDue, FlushTagCountsIfDue) and Close
// flushes both. Neither is fsynced by Java.
//
// Only the header snapshots run under one Store gate hold and the fsyncs run
// outside the gate, so ingest is not stalled (measured, local replay: periodic fsync and a
// full tag flush under the gate caused UDP queue rejects). Data files
// are fsynced before the headers are written, and a header snapshot only
// references records appended before it, so a saved header never points at
// unsynced data.
//
// Handles evicted while the pass runs are closed by this call afterwards
// (closeQueue), so an eviction never waits for the pass either. A handle that
// was nevertheless closed meanwhile (queue bound reached) was fsynced by that
// close, so its os.ErrClosed is not an error here.
func (s *Store) SyncData() error {
	s.syncRun.Lock()
	defer s.syncRun.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	var errs []error
	files := make([]*os.File, 0, len(s.files))
	filePaths := make([]string, 0, len(s.files))
	for path, f := range s.files {
		files = append(files, f)
		filePaths = append(filePaths, path)
	}
	var points []*legacyindex.SyncPoint
	var paths []string
	var owners []any // the handle each point belongs to (*Index or *TTLIndex)
	for path, idx := range s.indexes {
		p, err := idx.BeginSync()
		errs = append(errs, err)
		if p != nil {
			points, paths, owners = append(points, p), append(paths, path), append(owners, idx)
		}
	}
	for path, idx := range s.ttlIndexes {
		p, err := idx.BeginSync()
		errs = append(errs, err)
		if p != nil {
			points, paths, owners = append(points, p), append(paths, path), append(owners, idx)
		}
	}
	s.closes.begin()
	s.mu.Unlock()
	var failed []string
	for n, f := range files {
		if err := s.closes.syncClosedOK(f); err != nil {
			failed = append(failed, filePaths[n])
			errs = append(errs, err)
		}
	}
	errs = append(errs, s.tagSyncHandles()) // tag files have their own handles (tagstore.go)
	for _, p := range points {
		if err := p.Commit(); err != nil && !errors.Is(err, os.ErrClosed) {
			errs = append(errs, err)
		}
	}
	// Record under the gate, and only for handles still cached: a handle
	// that left the cache meanwhile was evicted (its close records a newer
	// checkpoint itself) or purged/deleted (its entry must stay forgotten,
	// forgetCheckpointsLocked).
	s.mu.Lock()
	for n, p := range points {
		cp, ok := p.Checkpoint()
		if !ok || dataFailedFor(failed, paths[n]) {
			continue
		}
		cached := false
		switch h := owners[n].(type) {
		case *legacyindex.Index:
			cached = s.indexes[paths[n]] == h
		case *legacyindex.TTLIndex:
			cached = s.ttlIndexes[paths[n]] == h
		}
		if cached {
			s.checkpoints.set(paths[n], cp)
		}
	}
	s.mu.Unlock()
	errs = append(errs, s.closes.drain(&s.mu))
	// Every header above (and of the handles drained) is fsynced: only now
	// may the log describe them (clean.go, crash checkpoints).
	return errors.Join(append(errs, s.checkpoints.persist())...)
}

// CloseStage names the step Close is in ("" before Close), for the
// shutdown report of a Close that does not return (a stalled disk). It
// takes no lock.
func (s *Store) CloseStage() string {
	if p := s.closeStage.Load(); p != nil {
		return *p
	}
	return ""
}

// CloseStageHook, when set before Close (tests only), runs at the start of
// every Close step. The cmd/scouter-go tests stall a step with it the way a
// stalled disk stalls a call that never sees a context.
var CloseStageHook func(stage string)

func (s *Store) enterCloseStage(stage string) {
	s.closeStage.Store(&stage)
	if h := CloseStageHook; h != nil {
		h(stage)
	}
}

// SkipCleanMarker makes Close leave the clean-shutdown marker unwritten,
// for a shutdown that may not have finished every write meant for the
// files (an ingest writer still in a write after its close deadline). The
// next Open then validates the indexes as after a crash.
func (s *Store) SkipCleanMarker() { s.noClean.Store(true) }

// CleanMarkerWritten reports whether Close wrote the clean-shutdown marker.
// It takes no lock.
func (s *Store) CleanMarkerWritten() bool { return s.markerWritten.Load() }

func (s *Store) Close() error {
	// The writers need the gate: they finish their queues first.
	s.enterCloseStage("xlog writer")
	s.closePackQueue(&s.xlogQ)
	s.enterCloseStage("profile writer")
	s.closePackQueue(&s.profQ)
	s.enterCloseStage("text writers")
	s.closeTexts()
	// syncRun before mu (same order as SyncData): an in-flight SyncData must
	// finish committing its snapshot and closing its deferred handles before
	// Close writes the final headers, or the older snapshot could overwrite
	// them.
	s.enterCloseStage("gates")
	s.syncRun.Lock()
	defer s.syncRun.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.visitorMu.Lock()
	defer s.visitorMu.Unlock()
	s.visitorIO.Lock() // wait for an in-flight snapshot write
	defer s.visitorIO.Unlock()
	s.visitorClosed = true
	s.enterCloseStage("sync")
	failed, syncErr := s.syncLocked()
	s.enterCloseStage("visitor flush")
	err := errors.Join(s.visitorFlushLocked(context.Background()), syncErr)
	s.closed = true
	s.enterCloseStage("index close")
	var errs []error
	sealed := make(map[string]legacyindex.Seal, len(s.indexes)+len(s.ttlIndexes))
	for path, idx := range s.ttlIndexes {
		seal, cerr := idx.CloseSealed()
		if errors.Is(cerr, legacyindex.ErrClosed) {
			continue
		}
		errs = append(errs, cerr)
		if cerr == nil {
			sealed[path] = seal
		}
		if !dataFailedFor(failed, path) {
			cp, ok := idx.Checkpoint()
			s.checkpoints.record(path, cp, ok)
		}
	}
	for path, idx := range s.indexes {
		seal, cerr := idx.CloseSealed()
		if errors.Is(cerr, legacyindex.ErrClosed) {
			continue // Index.Close is idempotent; an already closed handle is not sealed
		}
		errs = append(errs, cerr)
		if cerr == nil {
			sealed[path] = seal
		}
		if !dataFailedFor(failed, path) {
			cp, ok := idx.Checkpoint()
			s.checkpoints.record(path, cp, ok)
		}
	}
	for _, f := range s.files {
		errs = append(errs, f.Close())
	}
	err = errors.Join(err, errors.Join(errs...), s.closes.closeAll())
	if err == nil && !s.noClean.Load() {
		s.enterCloseStage("clean marker")
		// Only a fully successful Close is clean (clean.go). Without the
		// marker the next Open validates every index, as after a crash.
		carried, counterSealed := s.counterSealsLocked()
		for path, seal := range s.clean.seals {
			carried[path] = seal
		}
		for path, seal := range counterSealed {
			sealed[path] = seal
		}
		if merr := writeCleanMarker(s.config.Directory, carried, sealed, time.Now()); merr != nil {
			err = fmt.Errorf("clean shutdown marker (data is closed; next start validates all indexes): %w", merr)
		} else {
			s.markerWritten.Store(true)
		}
	}
	// The checkpoints of the closed pairs, written after their final fsyncs,
	// serve the restart after a later crash of the next run.
	s.enterCloseStage("checkpoints")
	err = errors.Join(err, s.checkpoints.persist())
	s.clean.seals = nil
	s.ttlIndexes = nil
	s.indexes = nil
	s.files = nil
	s.ends = nil
	s.zip = nil
	return err
}
func (s *Store) queryRange(date string, start, end int64) error {
	if err := s.check(date); err != nil {
		return err
	}
	if start <= 0 || end < start {
		return ErrInvalid
	}
	if end-start > int64(s.config.MaxQuerySpan/time.Millisecond) {
		return ErrLimit
	}
	if JavaDate(start, s.config.Location) != date || JavaDate(end, s.config.Location) != date {
		return fmt.Errorf("query must remain in requested Java date: %w", ErrInvalid)
	}
	return nil
}
