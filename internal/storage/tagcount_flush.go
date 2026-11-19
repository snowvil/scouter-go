// Copyright Scouter authors, Apache-2.0. Tag count flush and top100 rebuild
// in bounded holds of the tag gate (tagstore.go).
package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"time"
)

// The engine tick only marks the work due (tagTickLocked) and the runtime
// tag goroutine calls FlushTagCountsIfDue every 100ms. A pass holds the tag gate
// for at most scanHoldSteps records or scanHoldTime, whichever comes first (at
// least one record), and releases it between holds so tag queries, purges and
// an ingest at inbox backpressure interleave. Ingest itself does not wait for
// the tag gate (tagstore.go), so a pass does not yield further.
//
// tagRun admits one pass (drain, flush or rebuild) at a time; it is never
// taken under a gate.

// tagHold meters one tag gate hold of a tag pass. A step is one flushed
// record, one skipped work item or one scanned record. Steps cost several
// syscalls each, so the clock is read after every step.
type tagHold struct {
	steps, max      int
	start, deadline time.Time
	over            bool
}

func (s *Store) newTagHold() tagHold {
	max := s.tagHoldSteps
	if max <= 0 {
		max = scanHoldSteps
	}
	now := time.Now()
	return tagHold{max: max, start: now, deadline: now.Add(scanHoldTime)}
}

// spend counts one step and reports whether the hold should end.
func (h *tagHold) spend() bool {
	h.steps++
	h.over = h.over || h.steps >= h.max || time.Now().After(h.deadline)
	return h.over
}

// betweenTagHolds runs between two holds of a flush or rebuild pass: the
// test hook if set, otherwise a rest as long as the hold (tagPassYieldFactor),
// ended by ctx. A pass reads and writes tag files as fast as the disk lets
// it; the rest keeps it to about half of the disk and CPU it could take, so a
// long pass (a storm of new tag values) does not slow XLog writes. With
// little to flush a pass is a few holds and the rest is negligible.
func (s *Store) betweenTagHolds(ctx context.Context, held time.Duration) {
	if hook := s.tagBetweenHolds; hook != nil {
		hook()
		return
	}
	if s.tagNoRest || held <= 0 {
		return
	}
	deadline := time.Now().Add(tagPassYieldFactor * held)
	// The rest leaves disk and CPU to others; applying the inbox is in
	// memory and keeps going meanwhile, in short holds, so a long pass does
	// not let the inbox fill and make ingest wait (measured, local replay at 38,000 XLog/s:
	// 3.4s of 30s under the engine lock waiting for room).
	for time.Now().Before(deadline) && s.tagApplyDuringRest(ctx) {
	}
	if rest := time.Until(deadline); rest > 0 {
		t := time.NewTimer(rest)
		defer t.Stop()
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}
}

// tagApplyDuringRest applies queued contributions in one bounded hold of the
// tag gate (the caller runs a pass: tagRun is held). It takes the inbox only
// once the batch is applied, as tagDrainSomeLocked does, and reports false
// when nothing was queued.
func (s *Store) tagApplyDuringRest(ctx context.Context) bool {
	t := &s.tags
	if err := t.mu.LockContext(ctx); err != nil {
		return false
	}
	defer t.mu.Unlock()
	if t.closed {
		return false
	}
	if t.drainPos >= len(t.draining) {
		s.tagDrainedLocked()
		s.tagTakeLocked()
	}
	if t.drainPos >= len(t.draining) {
		s.tagDrainedLocked()
		return false
	}
	hold := s.newTagHold()
	for t.drainPos < len(t.draining) {
		s.tagStepLocked()
		if hold.spend() {
			break
		}
	}
	if t.drainPos >= len(t.draining) {
		s.tagDrainedLocked()
	}
	return true
}

const tagPassYieldFactor = 1

// tagPendingMax is the pending-bytes bound of tagAddLocked.
func (s *Store) tagPendingMax() int {
	return min(s.config.MaxQueryBytes, 64<<20)
}

// TagCountStats are the tag count counters. InlineFlushes counts the full
// flushes tagAddLocked ran at the pending bound, Backpressure the
// contributions ingest waited for room in a full inbox (at most
// tagInboxWait, once per saturation), InboxDropped those dropped because the
// inbox stayed full (S188), PendingDropped the inbox entries dropped whole
// because pending was at its bound (S182), ApplyErrors the inbox entries
// that failed to apply,
// DroppedDeltas the pending deltas dropped after a write failed once some
// of their counts were written (a retry would add them twice), Passes the
// incremental flush passes and MaxPassMillis/LastPassMillis their wall time,
// CacheLimited the paths whose top100 rebuild was given up at a resource
// limit (tagRebuildCaches).
type TagCountStats struct {
	InlineFlushes, Backpressure, InboxDropped, PendingDropped, ApplyErrors, DroppedDeltas, Passes uint64
	MaxPassMillis, LastPassMillis                                                                 int64
	CacheLimited                                                                                  uint64
}

func (s *Store) TagCountStats() TagCountStats {
	return TagCountStats{CacheLimited: s.tagCacheLimited.Load(), InlineFlushes: s.tagInline.Load(), Backpressure: s.tagBackpressure.Load(), InboxDropped: s.tagInboxDropped.Load(), PendingDropped: s.tagPendingDropped.Load(), ApplyErrors: s.tagApplyErrors.Load(), DroppedDeltas: s.tagDropped.Load(), Passes: s.tagPasses.Load(), MaxPassMillis: s.tagPassMax.Load(), LastPassMillis: s.tagPassLast.Load()}
}

func (s *Store) recordTagPass(start time.Time) {
	ms := time.Since(start).Milliseconds()
	s.tagPasses.Add(1)
	s.tagPassLast.Store(ms)
	for {
		old := s.tagPassMax.Load()
		if ms <= old || s.tagPassMax.CompareAndSwap(old, ms) {
			return
		}
	}
}

// errTagPassClosed ends a pass whose Store was closed between holds; Close
// has already flushed everything pending.
var errTagPassClosed = errors.New("tag pass: store closed")

// errTagPassPurged ends a rebuild whose files a purge or database delete
// removed between its holds: the path is gone, so nothing is written and the
// path is not retried.
var errTagPassPurged = errors.New("tag pass: aggregation purged")

// FlushTagCountsIfDue applies the inbox, writes the pending tag records once
// the 5s tick has passed and rebuilds the top100 caches once the 5-minute
// tick has passed (both marked by the engine tick). It is the runtime tag
// goroutine's entry point, called every 100ms; the tag gate is held
// only in bounded holds. A failed flush is retried at the next 5s tick (not
// at the next call, so a persistent failure such as ENOSPC costs one attempt
// and one log line per 5s, as the original MoveToNextCollector retries on its
// next save), a failed rebuild at the next 5-minute cycle; a rebuild due
// when the flush failed waits for the next successful flush.
func (s *Store) FlushTagCountsIfDue(ctx context.Context) error {
	s.tagRun.Lock()
	defer s.tagRun.Unlock()
	drainErr := s.tagDrain(ctx)
	t := &s.tags
	t.inboxMu.Lock()
	flush, cache := t.flushDue, t.cacheDue
	t.flushDue, t.cacheDue = false, false
	t.inboxMu.Unlock()
	if !flush && !cache {
		return drainErr
	}
	// The rebuild reads the files, so a due rebuild flushes first, as the
	// former tick did (its 5-minute cycle always crossed the 5s flush too).
	if err := s.tagFlushPass(ctx); err != nil {
		t.inboxMu.Lock()
		t.cacheDue = t.cacheDue || cache
		t.inboxMu.Unlock()
		return errors.Join(drainErr, err)
	}
	if drainErr != nil {
		return drainErr
	}
	if !cache {
		return nil
	}
	return s.tagRebuildCaches(ctx)
}

// flushTagCounts applies the inbox and writes every record pending then, in
// bounded holds. Contributions that arrive meanwhile are written by the next
// pass (or by this one when their key is still ahead of it).
func (s *Store) flushTagCounts(ctx context.Context) error {
	s.tagRun.Lock()
	defer s.tagRun.Unlock()
	return errors.Join(s.tagDrain(ctx), s.tagFlushPass(ctx))
}

// tagFlushItem names one pending delta: table path and record key.
type tagFlushItem struct {
	path  string
	key   []byte
	id    tagValueKey
	first bool
}

// tagFlushPass is the incremental tagFlushLocked; the caller holds tagRun.
//
// Order: the work list is the pending keys at the start, sorted as
// tagFlushLocked writes them (table path, First before Next, key), so each
// key file receives the same appends in the same order and, without
// concurrent contributions, the files are byte-identical to a full flush.
//
// Concurrency: each item is re-resolved under the tag gate. A delta that is gone
// (written by the inline pending-bytes flush in tagAddLocked, by a table
// eviction in tagTableLocked, by Close, or dropped by a purge) or that changed
// First/Next is skipped. Every write of one record (tagFlushRecordLocked: find,
// save, delete from pending, pendingBytes) is inside one hold, and all of those
// writers run under the tag gate, so a delta is applied exactly once.
func (s *Store) tagFlushPass(ctx context.Context) error {
	start := time.Now()
	if err := s.tags.mu.LockContext(ctx); err != nil {
		return err
	}
	if s.tags.closed || s.tagCounts == nil {
		s.tags.mu.Unlock()
		return nil
	}
	var items []tagFlushItem
	for path, table := range s.tagCounts.tables {
		for id, delta := range table.pending {
			items = append(items, tagFlushItem{path, delta.key, id, delta.first})
		}
	}
	s.tags.mu.Unlock()
	if len(items) == 0 {
		return ctx.Err()
	}
	defer s.recordTagPass(start)
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.path != b.path {
			return a.path < b.path
		}
		if a.first != b.first {
			return a.first
		}
		return bytes.Compare(a.key, b.key) < 0
	})
	var held time.Duration
	for i := 0; i < len(items); {
		if i > 0 {
			s.betweenTagHolds(ctx, held)
		}
		if err := s.tags.mu.LockContext(ctx); err != nil {
			return err
		}
		if s.tags.closed {
			s.tags.mu.Unlock()
			return nil
		}
		hold := s.newTagHold()
		s.tagDrainSomeLocked(&hold)
		hold.over = false // the flush still makes progress in this hold
		var err error
		for i < len(items) {
			it := items[i]
			i++
			if table := s.tagCounts.tables[it.path]; table != nil {
				if delta := table.pending[it.id]; delta != nil && delta.first == it.first {
					if err = s.tagFlushRecordLocked(ctx, it.path, table, delta); err != nil {
						break
					}
				}
			}
			if hold.spend() {
				break
			}
		}
		held = time.Since(hold.start)
		s.tags.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

// tagRebuildCaches rebuilds the top100 cache of every path that received
// contributions since its last rebuild. The caller holds tagRun. A path is
// claimed (removed from cachePaths) before its build, so contributions during
// the build mark it again for the next cycle; a failed build puts it back to
// be retried at the next 5-minute cycle without starving the other paths.
//
// A build that ends at a resource limit (ErrLimit: more records than
// tagScanMaxRecords, or more retained than tagCacheMaxRetained) is not
// retried: the files only grow, so every retry would read them whole again
// (gigabytes every 5 minutes, from the disk when the page cache is cold) and
// fail the same way. The path is given up until a purge removes it or the
// server restarts (cacheLimited, TagCountStats.CacheLimited); its error is
// returned once, its counts keep being written and its top100 files stay as
// the last successful build left them.
func (s *Store) tagRebuildCaches(ctx context.Context) error {
	if err := s.tags.mu.LockContext(ctx); err != nil {
		return err
	}
	if s.tags.closed || s.tagCounts == nil {
		s.tags.mu.Unlock()
		return nil
	}
	paths := make([]string, 0, len(s.tagCounts.cachePaths))
	for path := range s.tagCounts.cachePaths {
		paths = append(paths, path)
	}
	s.tags.mu.Unlock()
	sort.Strings(paths)
	var errs []error
	for _, path := range paths {
		if err := s.tags.mu.LockContext(ctx); err != nil {
			return err
		}
		if s.tags.closed {
			s.tags.mu.Unlock()
			return nil
		}
		claimed := s.tagCounts.cachePaths[path] // false: purged meanwhile
		delete(s.tagCounts.cachePaths, path)
		s.tags.mu.Unlock()
		if !claimed {
			continue
		}
		err := s.tagMakeCacheHolds(ctx, path)
		if err == nil {
			continue
		}
		if errors.Is(err, errTagPassClosed) {
			return nil
		}
		if errors.Is(err, errTagPassPurged) {
			// The counter is store-wide: a purge of another date also stops
			// this build, so a path whose files remain is retried next cycle.
			s.tags.mu.Lock()
			if !s.tags.closed && tagFileExists(filepath.Join(path, "next.kfile")) {
				s.tagCounts.cachePaths[path] = true
			}
			s.tags.mu.Unlock()
			continue
		}
		limited := errors.Is(err, ErrLimit)
		s.tags.mu.Lock()
		if !s.tags.closed {
			if limited {
				delete(s.tagCounts.cachePaths, path) // marked during the build
				if s.tagCounts.cacheLimited == nil {
					s.tagCounts.cacheLimited = map[string]bool{}
				}
				s.tagCounts.cacheLimited[path] = true
				s.tagCacheLimited.Add(1)
			} else {
				s.tagCounts.cachePaths[path] = true
			}
		}
		s.tags.mu.Unlock()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if limited {
			errs = append(errs, fmt.Errorf("tag top100 cache %s: %w (too large to rebuild: not rebuilt again)", path, err))
			continue
		}
		errs = append(errs, fmt.Errorf("tag top100 cache %s: %w", path, err))
	}
	return errors.Join(errs...)
}

// tagMakeCacheHolds rebuilds the top100 cache files of path in bounded holds:
// the First then the Next key file are streamed in file order with
// tagScanCursor into one tagTop100Builder, the builder state stays outside
// the gate between holds, and the cache files are written in a last hold.
//
// The result is the one-hold build's (tagMakeCacheLocked in the tests: same
// file order, same TopN insertion and HashSet order) unless records are
// written meanwhile. Those can only come
// from the inline pending-bytes flush or a table eviction (periodic flushes
// and rebuilds are serialized by tagRun): a record appended after its file's
// scan started is not seen, a record updated in place is read either before or
// after the update, never half written (one record per hold). The original
// Top100FileCache also reads the files while MoveToNextCollector writes them.
func (s *Store) tagMakeCacheHolds(ctx context.Context, path string) error {
	b := newTagTop100Builder(tagCacheMaxRetained)
	cursors := []*tagScanCursor{{path: filepath.Join(path, "first"), first: true}, {path: filepath.Join(path, "next")}}
	adds := []func(tagRecord) error{b.addFirst, b.addNext}
	scanMax := tagScanMaxRecords
	if s.tagScanMax > 0 {
		scanMax = s.tagScanMax
	}
	var purges uint64
	var held time.Duration
	for c, holds := 0, 0; c < len(cursors); holds++ {
		if holds > 0 {
			s.betweenTagHolds(ctx, held)
		}
		if err := s.tags.mu.LockContext(ctx); err != nil {
			return err
		}
		if s.tags.closed {
			s.tags.mu.Unlock()
			return errTagPassClosed
		}
		if holds == 0 {
			purges = s.tags.purges.Load()
		} else if s.tagPurgedSince(purges, path) {
			s.tags.mu.Unlock()
			return errTagPassPurged
		}
		hold := s.newTagHold()
		// Mostly pending only; an inline cap flush or a table eviction it
		// causes may write the scanned files, as described above.
		s.tagDrainSomeLocked(&hold)
		hold.over = false
		var err error
		for c < len(cursors) && !hold.over {
			if err = s.tagScanStepLocked(ctx, cursors[c], scanMax, &hold, adds[c]); err != nil {
				break
			}
			if cursors[c].done {
				c++
			}
		}
		held = time.Since(hold.start)
		s.tags.mu.Unlock()
		if err != nil {
			return err
		}
	}
	items := b.finish()
	if len(items) == 0 {
		return ctx.Err()
	}
	s.betweenTagHolds(ctx, held)
	if err := s.tags.mu.LockContext(ctx); err != nil {
		return err
	}
	defer s.tags.mu.Unlock()
	if s.tags.closed {
		return errTagPassClosed
	}
	// A purge between holds removed the directory; writing would recreate it.
	if s.tagPurgedSince(purges, path) || !tagFileExists(filepath.Join(path, "next.kfile")) {
		return errTagPassPurged
	}
	return s.tagWriteTop100Locked(ctx, path, items)
}

// tagScanInHolds is tagScanLocked in bounded holds of the tag gate, for
// Client queries: a long scan does not hold the gate the maintenance drain,
// backpressure ingest and purges wait for. Each record is read within one
// hold; records appended after the scan started are not seen and a record
// updated in place is read before or after the update, as the original
// readers do while the writer thread runs. A purge between holds ends the
// scan with errTagPassPurged (the data is gone).
func (s *Store) tagScanInHolds(ctx context.Context, path string, first bool, fn func(tagRecord) error) error {
	c := tagScanCursor{path: path, first: first}
	var purges uint64
	for holds := 0; ; holds++ {
		if hook := s.tagQueryBetween; holds > 0 && hook != nil {
			hook() // test hook; tagBetweenHolds belongs to passes
		}
		if err := s.tags.mu.LockContext(ctx); err != nil {
			return err
		}
		if s.tags.closed {
			s.tags.mu.Unlock()
			return ErrClosed
		}
		if holds == 0 {
			purges = s.tags.purges.Load()
		} else if s.tagPurgedSince(purges, path) {
			s.tags.mu.Unlock()
			return errTagPassPurged
		}
		hold := s.newTagHold()
		err := s.tagScanStepLocked(ctx, &c, tagScanMaxRecords, &hold, fn)
		s.tags.mu.Unlock()
		if err != nil || c.done {
			return err
		}
	}
}
