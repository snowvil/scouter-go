package legacyindex

import (
	"errors"
	"hash/crc32"
	"sync/atomic"
)

// SyncPoint is a snapshot of the dirty header pages taken under the index
// lock. Commit persists it without holding that lock, so Put/Get are never
// blocked behind an fsync.
//
// A header written by Commit only references records whose key bytes were
// appended before BeginSync. The caller must fsync any data file those records
// point at before Commit, and must not Close the index until Commit returns.
//
// Ownership: buf is allocated by BeginSync for this point only and is read by
// Commit alone; it is never reused or shared with the index.
type SyncPoint struct {
	index  *Index
	length int64  // key bytes the snapshot references (i.length at BeginSync)
	seq    uint64 // header writes before the snapshot (Index.headerSeq)
	pages  []int  // dirty header pages, ascending; nil when the header was clean
	buf    []byte // those pages back to back
	cp     Checkpoint
	cpOK   bool
	done   atomic.Bool // Commit ran
}

// BeginSync snapshots the header pages that may differ on disk: the dirty
// ones and those still held by an uncommitted earlier SyncPoint (whose Commit
// may write nothing, see Commit), so a Commit of this point leaves the whole
// disk header equal to the memory header at BeginSync. It performs no I/O.
// Only those pages are copied (4 KiB each), not the whole header (1 MiB by
// default, up to the configured hash size).
func (i *Index) BeginSync() (*SyncPoint, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.writable(); err != nil {
		return nil, err
	}
	p := &SyncPoint{index: i, length: i.length, seq: i.headerSeq.Load()}
	if i.dirty || i.outstanding > 0 {
		p.pages = i.unsyncedPages()
		p.buf = i.copyPages(p.pages)
		for _, pg := range p.pages {
			i.pendingRefs[pg]++
		}
		clear(i.dirtyBits)
		i.dirty = false
	}
	i.outstanding++
	return p, nil
}

// BeginSync snapshots a TTL index header; see Index.BeginSync.
func (t *TTLIndex) BeginSync() (*SyncPoint, error) { return t.index.BeginSync() }

// Commit fsyncs the key file, then writes and fsyncs the snapshot pages, and
// then establishes the checkpoint of that state (Checkpoint). Pages changed
// after BeginSync stay dirty for the next sync; on failure the snapshot pages
// are marked dirty again.
//
// Commit writes only while no header write happened since its BeginSync
// (headerSeq). Otherwise - a Sync or Close in between, or another SyncPoint
// begun later and committed first (callers do neither: one pass at a time) -
// it writes no header and establishes no checkpoint, so an older snapshot
// never overwrites newer header bytes, and it marks its pages dirty again so
// the next sync writes their current bytes. Commit must be called exactly
// once per SyncPoint.
func (p *SyncPoint) Commit() error {
	if p.done.Swap(true) {
		return errors.New("legacy index sync point committed twice")
	}
	i := p.index
	crcs := make([]uint32, len(p.pages))
	for n, pg := range p.pages {
		crcs[n] = crc32.ChecksumIEEE(p.page(n, pg))
	}
	i.headerIO.Lock()
	err := i.syncKeys()
	current := i.headerSeq.Load() == p.seq
	if err == nil && current && len(p.pages) > 0 {
		if err = i.writePages(p.pages, p.buf); err != nil {
			i.sumsOK = false // the disk state of these pages is unknown now
		} else {
			for n, pg := range p.pages {
				i.sums[pg] = crcs[n]
			}
			i.headerSeq.Add(1)
		}
	}
	var gen uint64
	if err == nil && current {
		if p.cp, p.cpOK = i.checkpointLocked(p.length); p.cpOK {
			i.cpN++
			gen = i.cpN
		}
	}
	i.headerIO.Unlock()
	i.mu.Lock()
	for _, pg := range p.pages {
		i.pendingRefs[pg]--
	}
	i.outstanding--
	if (err != nil || !current) && len(p.pages) > 0 {
		// Not (or not surely) written by this point: the next sync writes
		// the current bytes.
		setPages(i.dirtyBits, p.pages, true)
		i.dirty = true
	}
	if err != nil {
		i.cpOK = false // the disk may no longer match the last checkpoint
	}
	if p.cpOK {
		i.publishCheckpoint(p.cp, gen)
	}
	i.mu.Unlock()
	return err
}

// page returns the n-th snapshot page (header page pg).
func (p *SyncPoint) page(n, pg int) []byte {
	at := n * headerPage
	return p.buf[at : at+len(p.index.page(pg))]
}

// Checkpoint returns the checkpoint a successful Commit established: the key
// bytes below the BeginSync length and the header as written.
func (p *SyncPoint) Checkpoint() (Checkpoint, bool) { return p.cp, p.cpOK }
