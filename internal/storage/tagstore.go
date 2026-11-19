// Copyright Scouter authors, Apache-2.0. Tag count state apart from the Store
// gate: its own gate, file handles and a contribution inbox.
package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/syncx"
)

// The original server counts tags on its own threads (XLogTagCount queue of
// 10000 XLogs and daemon, MoveToNextCollector, Top100FileCache); collection
// only enqueues. Here ingest appends its contributions to the inbox (inboxMu,
// a leaf lock held only to append or swap) and never waits for tag file I/O or
// for the Store gate. The tag gate (tagState.mu) guards the pending tables,
// the tag file handles and their sizes; the runtime tag goroutine drains the
// inbox and flushes in bounded holds of it. Tag files are only read and
// written under the tag gate, so a tag pass does not delay XLog, text or
// profile writes (a full inbox costs ingest one bounded wait, then drops; a
// purge waits for the tag gate; see internal/storage/CONTRACT.md §8).
//
// Lock order: syncRun, tagRun, Store.mu, tagState.mu, inboxMu. The Store gate
// is never taken under the tag gate.

// tagInboxMax bounds the inbox (XLog/alert contributions); a runtime
// goroutine drains it every 100ms and at once when it fills (TagDrainSignal).
const tagInboxMax = 1 << 14

// tagInboxWait is how long ingest waits for room in a full inbox before it
// drops the contribution (counted, logged S188 as the original
// XLogTagCount does). Ingest waits without its engine lock (Offer, then
// Submit after the lock), and the wait is short and taken at most once per
// saturation: while the inbox stays full after a timeout, contributions are
// dropped without waiting, so a stalled tag count (a hung disk, a long
// query) never slows XLog ingest further.
const tagInboxWait = 100 * time.Millisecond

// tagFileMax bounds the cached tag file handles.
const tagFileMax = 64

// tagPathMax bounds the tag table path cache (date and object type pairs).
const tagPathMax = 1024

// tagInboxEntry is the contributions of one XLog (x) or alert (tags) to one
// table.
type tagInboxEntry struct {
	path, group string
	minute      int
	tags        []tagContribution
	xlog        bool
	x           tagXLogFields
	// gen is tagState.purges when the entry was made (prepareTags): a
	// purge or delete of its path since then discards it at the inbox
	// (tagEnqueue), as it discards the entries already in the inbox.
	gen uint64
}

type tagState struct {
	mu      syncx.Mutex
	files   map[string]*os.File
	lastUse map[string]uint64
	clock   uint64
	ends    map[string]int64 // cached file sizes, see tagSizeLocked
	// paths caches tagTablePath (its own leaf lock: ingest calls it outside
	// the tag gate).
	pathMu sync.Mutex
	paths  map[[2]string]string
	// valid marks key/data files whose header was checked for this handle;
	// dirty the handles written since their last fsync.
	valid, dirty map[string]bool
	closed       bool
	// draining is the inbox batch being applied in holds; drainPos is its
	// next entry. Purges filter it like the inbox.
	draining []tagInboxEntry
	drainPos int
	// newCounts is the counts buffer of tagNewRecord, reused under the tag
	// gate: the record is saved and dropped before the next one is made.
	newCounts []float32
	// keyEnc encodes tag record keys for tagAddLocked (tagScratchKey);
	// readBuf holds a record head being decoded (tagReadRecordKeyLocked).
	keyEnc  *protocol.Encoder
	readBuf []byte
	// purges counts discardAggregationLocked calls and purgeLog keeps the
	// roots of the last tagPurgeLogMax, so a multi-hold scan or rebuild
	// notices when its own files were deleted between its holds
	// (tagPurgedSince), and so does an inbox entry made before a purge
	// (tagEnqueue). Both are written under the tag gate and inboxMu;
	// purgeLog is read under either, purges also without a lock (the
	// generation a new inbox entry is stamped with).
	purges   atomic.Uint64
	purgeLog []tagPurge

	inboxMu     sync.Mutex
	inbox       []tagInboxEntry
	inboxClosed bool
	// spare is the array of the last applied drain batch, cleared, for the
	// inbox to fill again (tagDrainedLocked): entries are 160 bytes and an
	// inbox grown from nil each drain allocated twice its size. The inbox
	// grows to at most tagInboxMax entries (tagEnqueue), and only an array of
	// at most that capacity is kept (about 2.6MB).
	spare []tagInboxEntry
	// space is signalled when a drain takes the inbox, drainNow when the
	// inbox fills; saturated is set after a wait timed out and cleared by
	// the next drain.
	space, drainNow chan struct{}
	saturated       atomic.Bool
	// Engine tick state (tagTickLocked), also under inboxMu so the tick never
	// waits for a tag pass.
	lastFlush, lastCache int64
	flushDue, cacheDue   bool
}

// tagEnqueue appends e to the inbox; false when it is full.
func (s *Store) tagEnqueue(e tagInboxEntry) (bool, error) {
	t := &s.tags
	t.inboxMu.Lock()
	defer t.inboxMu.Unlock()
	if t.inboxClosed {
		return false, ErrClosed
	}
	if e.gen != t.purges.Load() && s.tagPurgedSinceKnown(e.gen, e.path) {
		return true, nil // its directory was purged after it was made
	}
	if len(t.inbox) >= tagInboxMax {
		return false, nil
	}
	if t.inbox == nil {
		t.inbox, t.spare = t.spare, nil
	}
	if len(t.inbox) == cap(t.inbox) { // append would grow past tagInboxMax
		grown := make([]tagInboxEntry, len(t.inbox), min(max(2*cap(t.inbox), 64), tagInboxMax))
		copy(grown, t.inbox)
		t.inbox = grown
	}
	t.inbox = append(t.inbox, e)
	return true, nil
}

// tagPurgeGen is the purge generation a new inbox entry is stamped with. A
// purge counts it up before it logs its roots under inboxMu, where
// tagEnqueue reads both, so an entry stamped before the count sees the log.
func (s *Store) tagPurgeGen() uint64 { return s.tags.purges.Load() }

func (s *Store) tagTakeInbox() []tagInboxEntry {
	t := &s.tags
	t.inboxMu.Lock()
	batch := t.inbox
	t.inbox = nil
	t.inboxMu.Unlock()
	if len(batch) > 0 {
		t.saturated.Store(false)
		signal(t.space)
	}
	return batch
}

// signal makes a non-blocking send on a 1-slot channel (nil: none).
func signal(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

// TagDrainSignal fires when the tag inbox fills, for the runtime goroutine
// that drains it (FlushTagCountsIfDue) between its 100ms ticks.
func (s *Store) TagDrainSignal() <-chan struct{} { return s.tags.drainNow }

// tagOffer queues e, waiting at most tagInboxWait for room in a full inbox
// (once per saturation); otherwise e is dropped and counted. With wait false
// it never waits: a full inbox that is not saturated returns done=false with
// nothing changed (the caller submits e later, Submit).
func (s *Store) tagOffer(ctx context.Context, e tagInboxEntry, wait bool) (done bool, err error) {
	ok, err := s.tagEnqueue(e)
	if ok || err != nil {
		return true, err
	}
	signal(s.tags.drainNow)
	if !s.tags.saturated.Load() {
		if !wait {
			return false, nil
		}
		s.tagBackpressure.Add(1)
		if ok, err = s.tagWaitEnqueue(ctx, e); ok || err != nil {
			return true, err
		}
		s.tags.saturated.Store(true)
	}
	s.tagInboxDropped.Add(1)
	s.logJava("S188", "XLogTagCount queue exceeded!!")
	return true, nil
}

// tagWaitEnqueue retries e each time a drain makes room, for tagInboxWait.
func (s *Store) tagWaitEnqueue(ctx context.Context, e tagInboxEntry) (bool, error) {
	timer := time.NewTimer(tagInboxWait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-timer.C:
			return false, nil
		case <-s.tags.space:
			if ok, err := s.tagEnqueue(e); ok || err != nil {
				return ok, err
			}
			signal(s.tags.drainNow)
		}
	}
}

// tagNeedMax is at least the pending bytes one contribution can add (a new
// delta and hour): its record key is the 8-byte tag, a type byte and the
// value (text with a length prefix of at most 5 bytes, a decimal of at most
// 9 bytes).
func tagNeedMax(it tagItem) int {
	return tagDeltaOverhead + tagHourBytes + 2*len(it.text) + 32
}

// items appends the contributions of e in counting order.
func (e *tagInboxEntry) items(out []tagItem) []tagItem {
	if e.xlog {
		return e.x.items(out)
	}
	for _, tag := range e.tags {
		it := tagItem{id: tagValueKey{tag: tagKey(e.group, tag.name), kind: tag.kind}, count: tag.count}
		if tag.kind == protocol.ValueText {
			it.text = tag.text
		} else {
			it.id.n = tag.n
		}
		out = append(out, it)
	}
	return out
}

// tagApplyLocked adds one inbox entry to the pending tables.
//
// An entry that could take pending past its bound is dropped whole (counted)
// rather than flushed inline: the periodic pass writes at its own pace, and
// dropping all of one XLog's tags keeps them consistent with each other. The
// remaining partial case is an error inside tagAddLocked (the 64MiB First
// metadata bound, a key past 32767 bytes, a table load failure): the tags
// before it stay applied and the entry counts as ApplyErrors, as the original
// counts tag by tag and logs a failing one.
//
// The table is looked up for the entry's first tag (where each tag looked it
// up before) and marked for the top100 rebuild after it is counted: the
// entry's tags share it, and nothing within an entry removes a table or the
// mark.
func (s *Store) tagApplyLocked(ctx context.Context, e *tagInboxEntry) error {
	var buf [tagXMax]tagItem
	items := e.items(buf[:0])
	if s.tagCounts != nil {
		need := 0
		for _, it := range items {
			need += tagNeedMax(it)
		}
		if s.tagCounts.pendingBytes+need > s.tagPendingMax() {
			s.tagPendingDropped.Add(1)
			s.logJava("S182", "queue is full")
			return nil
		}
	}
	var table *tagTable
	for i, it := range items {
		if table == nil {
			var err error
			if table, err = s.tagTableLocked(ctx, e.path); err != nil {
				return err
			}
		}
		if err := s.tagAddLocked(ctx, table, it, e.minute); err != nil {
			return err
		}
		if i == 0 && !s.tagCounts.cacheLimited[e.path] {
			s.tagCounts.cachePaths[e.path] = true
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}

// tagTakeLocked appends the inbox behind the rest of the drain batch, so
// arrival order holds across drains that stopped early. Without a rest the
// inbox becomes the batch as it is.
func (s *Store) tagTakeLocked() {
	t := &s.tags
	rest := t.draining[t.drainPos:]
	if inbox := s.tagTakeInbox(); len(rest) == 0 {
		t.draining = inbox
	} else {
		t.draining = append(rest[:len(rest):len(rest)], inbox...)
	}
	t.drainPos = 0
}

// tagDrainedLocked ends a drain batch that is applied to the end and keeps
// its array, cleared (no paths or contributions stay reachable), for the
// inbox to fill again. Entries past the length of a batch are always zero:
// the inbox only appends, and a rest or a purge makes a new array.
func (s *Store) tagDrainedLocked() {
	t := &s.tags
	batch := t.draining
	t.draining, t.drainPos = nil, 0
	if cap(batch) == 0 || cap(batch) > tagInboxMax { // a rest and an inbox
		return
	}
	clear(batch)
	t.inboxMu.Lock()
	if t.spare == nil {
		t.spare = batch[:0]
	}
	t.inboxMu.Unlock()
}

// tagStepLocked applies the next batch entry. The position moves first and
// the entry is applied whole (no cancellation inside it), so an entry is
// applied exactly once whatever stops the caller afterwards. A failing entry
// is counted (ApplyErrors) and skipped, as XLogTagCount logs and goes on.
func (s *Store) tagStepLocked() {
	t := &s.tags
	e := &t.draining[t.drainPos]
	t.drainPos++
	if err := s.tagApplyLocked(context.Background(), e); err != nil {
		s.tagApplyErrors.Add(1)
	}
}

// tagDrainSomeLocked applies queued entries within at most half of hold (the
// caller holds the tag gate). Flush and rebuild holds call it first, so the
// inbox keeps draining during a long pass while the pass itself keeps making
// progress. The inbox is taken only once the previous batch is applied (the
// batch stays within one inbox here, within two after a tagDrain or a query
// appended an inbox behind a rest): when draining falls behind, the inbox
// fills and new contributions are dropped (S188) instead of the batch
// growing without bound.
func (s *Store) tagDrainSomeLocked(hold *tagHold) {
	t := &s.tags
	if t.drainPos >= len(t.draining) {
		s.tagDrainedLocked()
		s.tagTakeLocked()
	}
	for n := 0; n < hold.max/2 && t.drainPos < len(t.draining) && !hold.over; n++ {
		s.tagStepLocked()
		hold.spend()
	}
	if t.drainPos >= len(t.draining) {
		s.tagDrainedLocked()
	}
}

// tagDrainAllLocked applies, in arrival order, the rest of the drain batch
// and then the whole inbox. ctx is checked between entries; what is left
// stays queued for the next drain.
func (s *Store) tagDrainAllLocked(ctx context.Context) error {
	t := &s.tags
	s.tagTakeLocked()
	for t.drainPos < len(t.draining) {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.tagStepLocked()
	}
	s.tagDrainedLocked()
	return nil
}

// tagDrain applies the inbox in bounded holds of the tag gate; the caller
// holds tagRun.
func (s *Store) tagDrain(ctx context.Context) error {
	t := &s.tags
	if err := t.mu.LockContext(ctx); err != nil {
		return err
	}
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	s.tagTakeLocked()
	t.mu.Unlock()
	for holds := 0; ; holds++ {
		if holds > 0 {
			s.betweenTagHolds(ctx, 0) // in memory: no rest
		}
		if err := t.mu.LockContext(ctx); err != nil {
			return err
		}
		if t.closed || t.drainPos >= len(t.draining) {
			if !t.closed {
				s.tagDrainedLocked()
			}
			t.mu.Unlock()
			return nil
		}
		hold := s.newTagHold()
		for t.drainPos < len(t.draining) && ctx.Err() == nil {
			s.tagStepLocked()
			if hold.spend() {
				break
			}
		}
		t.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err // the rest stays in draining for the next drain
		}
	}
}

// lockTagsForDetach takes the tag gate for a purge or database delete (the
// caller holds the Store gate) and returns its release.
func (s *Store) lockTagsForDetach() func() {
	s.tags.mu.Lock()
	return s.tags.mu.Unlock
}

// tagSafePath rejects a tag file path as walkSafePath does, except that a
// cached tag handle was validated when it was opened (re-walking every
// component per tag record I/O was a measured replay hot spot).
func (s *Store) tagSafePath(path string) error {
	if _, open := s.tags.files[path]; open {
		return nil
	}
	return s.walkSafePath(path)
}

// tagFile is Store.file for tag files, with its own bounded handle cache.
func (s *Store) tagFile(path string, create bool) (*os.File, error) {
	t := &s.tags
	if f := t.files[path]; f != nil {
		t.touch(path)
		return f, nil
	}
	if len(t.files) >= tagFileMax {
		if err := t.evict(); err != nil {
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
	if t.files == nil {
		t.files = map[string]*os.File{}
	}
	t.files[path] = f
	t.touch(path)
	return f, nil
}

func (t *tagState) touch(path string) {
	if t.lastUse == nil {
		t.lastUse = map[string]uint64{}
	}
	t.clock++
	t.lastUse[path] = t.clock
}

// evict closes the least recently used tag handle, after an fsync when it
// was written, as a data handle leaves the Store cache.
func (t *tagState) evict() error {
	victim := leastRecent(t.files, t.lastUse)
	f, written := t.files[victim], t.dirty[victim]
	t.drop(victim)
	if written {
		return errors.Join(f.Sync(), f.Close())
	}
	return f.Close()
}
func (t *tagState) drop(path string) {
	delete(t.files, path)
	delete(t.ends, path)
	delete(t.lastUse, path)
	delete(t.valid, path)
	delete(t.dirty, path)
}
func (t *tagState) mark(m *map[string]bool, path string) {
	if *m == nil {
		*m = map[string]bool{}
	}
	(*m)[path] = true
}

// releaseLocked closes, without fsync, the tag handles below roots, which are
// about to be deleted.
func (t *tagState) releaseLocked(roots []string) error {
	var errs []error
	for p, f := range t.files {
		if under(p, roots) {
			errs = append(errs, f.Close())
			t.drop(p)
		}
	}
	return errors.Join(errs...)
}

// tagDiscardLocked drops the pending tables, top100 marks, inbox and drain batch
// entries and open handles below paths (the caller holds the tag gate).
func (s *Store) tagDiscardLocked(paths []string) {
	t := &s.tags
	t.inboxMu.Lock() // tagEnqueue reads the purge log under inboxMu
	gen := t.purges.Add(1)
	t.purgeLog = append(t.purgeLog, tagPurge{gen, append([]string(nil), paths...)})
	if len(t.purgeLog) > tagPurgeLogMax {
		t.purgeLog = t.purgeLog[len(t.purgeLog)-tagPurgeLogMax:]
	}
	t.inboxMu.Unlock()
	keep := func(entries []tagInboxEntry) []tagInboxEntry {
		var kept []tagInboxEntry
		for _, e := range entries {
			if !under(e.path, paths) {
				kept = append(kept, e)
			}
		}
		return kept
	}
	t.draining, t.drainPos = keep(t.draining[t.drainPos:]), 0
	t.inboxMu.Lock()
	before := len(t.inbox)
	t.inbox = keep(t.inbox)
	freed := len(t.inbox) < before
	t.inboxMu.Unlock()
	if freed {
		t.saturated.Store(false)
		signal(t.space)
	}
	t.releaseLocked(paths)
	if s.tagCounts == nil {
		return
	}
	state := s.tagCounts
	for p, table := range state.tables {
		if !under(p, paths) {
			continue
		}
		state.metadataBytes -= table.metadataBytes
		for _, d := range table.pending {
			state.pendingBytes -= d.bytes
		}
		delete(state.tables, p)
	}
	for p := range state.cachePaths {
		if under(p, paths) {
			delete(state.cachePaths, p)
		}
	}
	for p := range state.cacheLimited {
		if under(p, paths) {
			delete(state.cacheLimited, p)
		}
	}
}

// tagSyncHandles fsyncs the open tag files (SyncData). A handle closed
// meanwhile was fsynced by its eviction or is being deleted.
func (s *Store) tagSyncHandles() error {
	t := &s.tags
	t.mu.Lock()
	files := make([]*os.File, 0, len(t.dirty))
	for p := range t.dirty {
		if f := t.files[p]; f != nil {
			files = append(files, f)
		}
	}
	t.dirty = nil // written again meanwhile: marked again
	t.mu.Unlock()
	var errs []error
	for _, f := range files {
		if err := f.Sync(); err != nil && !errors.Is(err, os.ErrClosed) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// tagClose stops the inbox, applies and flushes everything pending, and
// fsyncs and closes the tag handles (Store.Close).
func (s *Store) tagClose() error {
	t := &s.tags
	t.inboxMu.Lock()
	t.inboxClosed = true
	t.inboxMu.Unlock()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	ctx := context.Background()
	errs := []error{s.tagDrainAllLocked(ctx), s.tagFlushLocked(ctx)}
	paths := make([]string, 0, len(t.files))
	for p := range t.files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		f := t.files[p]
		errs = append(errs, f.Sync(), f.Close())
		t.drop(p)
	}
	t.closed = true
	return errors.Join(errs...)
}

type tagPurge struct {
	gen   uint64
	roots []string
}

const tagPurgeLogMax = 64

// tagPurgedSince reports whether a purge or delete after gen covered path
// (a tag file base or table directory): the root contains it or lies below
// it. Purges older than the log count as covering it.
func (s *Store) tagPurgedSince(gen uint64, path string) bool {
	return s.tagPurgedSinceOr(gen, path, true)
}

// tagPurgedSinceKnown is tagPurgedSince for an inbox entry (tagEnqueue):
// when the log no longer reaches back to gen (more than tagPurgeLogMax
// purges or deletes between its Prepare and its Submit) it keeps the entry
// rather than drop contributions of every path. The directory could then be
// recreated, as by any contribution queued before this check existed.
func (s *Store) tagPurgedSinceKnown(gen uint64, path string) bool {
	return s.tagPurgedSinceOr(gen, path, false)
}

func (s *Store) tagPurgedSinceOr(gen uint64, path string, unknown bool) bool {
	t := &s.tags
	if t.purges.Load() == gen {
		return false
	}
	if len(t.purgeLog) == 0 || t.purgeLog[0].gen > gen+1 {
		return unknown // the log no longer reaches back to gen
	}
	for _, p := range t.purgeLog {
		if p.gen <= gen {
			continue
		}
		if under(path, p.roots) {
			return true
		}
		for _, root := range p.roots {
			if under(root, []string{path}) || strings.HasPrefix(root, path+".") {
				return true // a file of path (base.kfile) or below the table
			}
		}
	}
	return false
}
