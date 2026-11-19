// Copyright Scouter authors, Apache-2.0. Asynchronous text dictionary writes.
package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// The original server writes texts on its own thread (TextWR queue and
// daemon) and puts each text in TextCache at once, so readers see it before
// it reaches the file. PutText here validates the pack, records it in pending
// (the read-through cache; the first value of a key wins, as on disk) and
// hands it to the Store's text writer goroutine through a bounded queue. A
// full queue makes PutText wait at most textQueueWait, then the text is
// dropped and counted (TextWR drops at once) and kept readable in a small
// bounded cache of dropped texts (rememberDropped). The writer stores queued texts in short Store gate holds and yields the gate
// between them, so the ingest engine lock is never held for a text write.
//
// Lock order: Store.mu before textQueue.mu (a leaf lock, never held while
// waiting for the queue or the gate).

// textQueueMax bounds the queued texts and textQueueMaxBytes their bytes (a
// text alone may exceed it when nothing else is queued); textQueueWait is
// the longest PutText waits for room (enqueueText).
const (
	textQueueMax      = 8192
	textQueueMaxBytes = 32 << 20
	textQueueWait     = 100 * time.Millisecond
)

// textRef names one text record: the index path it goes to and its key.
type textRef struct{ path, key string }

type textItem struct {
	ref   textRef
	date  string
	daily bool
	pack  protocol.TextPack
	// barrier, when set, is closed by the writer once every earlier item is
	// written (Sync); the item carries no text.
	barrier chan struct{}
	size    int // charged to textQueue.bytes
	// held marks a text Offer found no room for: it stays pending (readable,
	// no queue bytes) until Submit queues or drops it (offerText). A purge
	// that removes it from pending meanwhile discards it. Under textQueue.mu.
	held bool
}

type textQueue struct {
	held    holdSum // Store gate holds (LockStats)
	mu      sync.Mutex
	pending map[textRef]*textItem
	closed  bool
	senders sync.WaitGroup
	bytes   int           // queued text bytes, see textQueueMaxBytes
	space   chan struct{} // the writer released bytes
	// saturated is set after a wait for room timed out and cleared when the
	// writer makes room; dropped counts the texts dropped meanwhile.
	saturated atomic.Bool
	dropped   atomic.Uint64
	// recent holds dropped texts (rememberDropped), in recentOrder.
	recent      map[textRef]string
	recentOrder []textRef
	recentBytes int
	// read holds texts read from the files (rememberRead), in readOrder: the
	// TextCache of the original, so repeated lookups (an ingest group lookup
	// per XLog) skip the Store gate. A stored text never changes (the first
	// value of a key wins), so an entry stays valid until a purge or delete.
	//
	// The cache is bounded by bytes (readMax, Config.TextCacheBytes) and
	// evicts by second chance: an entry read since it was last at the front
	// of readOrder is kept for another round, so the texts every XLog needs
	// stay in memory however many one-off texts pass through. The ingest and
	// Pulse lookups depend on it, not on the page cache: a file read takes
	// the Store gate, and with the page cache cold it takes it for a disk
	// seek (CONTRACT.md §8.2).
	read      map[textRef]textRead
	readOrder []textRef
	readBytes int
	readMax   int
	// paths interns the index paths of read and missing refs (a few per
	// date), which would otherwise be held once per entry.
	paths map[string]string
	// slot admits one file read of a text at a time (TextContext): readers
	// that missed the caches queue here instead of at the Store gate, so the
	// writers wait behind one reader, not behind all of them.
	slot chan struct{}
	// fetching and fetchQ are the read-ahead of TextWithin (textfetch.go).
	fetching  map[textFetchKey]*textFetch
	fetchQ    chan *textFetch
	fetchStop context.CancelFunc
	fetchDone chan struct{}
	slowUntil atomic.Int64 // UnixNano until which TextWithin does not wait
	stats     textCacheCounters
	// missing holds refs a file read did not find (rememberMissing), in
	// missingOrder: the ingest group lookup of a service whose text has not
	// arrived would read the files under the Store gate once per XLog. An
	// entry is dropped when a text of the ref is queued, so it never hides a
	// text that arrived later.
	missing      map[textRef]struct{}
	missingOrder []textRef
	queue        chan *textItem
	done         chan struct{}
	errors       atomic.Uint64
	// divs bounds the unknown divs of permanent texts (text.go).
	divs textDivs
}

// startTextWriter starts the writer goroutine; Close stops it (closeTexts).
func (s *Store) startTextWriter() {
	q := &s.texts
	q.pending = map[textRef]*textItem{}
	q.queue = make(chan *textItem, textQueueMax)
	q.space = make(chan struct{}, 1)
	q.done = make(chan struct{})
	q.slot = make(chan struct{}, 1)
	q.readMax = s.config.TextCacheBytes
	go s.textWriter()
	s.startTextFetcher()
}

// textWriter owns the queue's receive side until closeTexts closes it. It
// writes the queued texts in Store gate holds of at most scanHoldSteps texts
// or scanHoldTime, so a burst of new texts does not take the gate once per
// text and a hold stays short.
func (s *Store) textWriter() {
	q := &s.texts
	defer close(q.done)
	var done, batch []*textItem
	for it := range q.queue {
		s.mu.Lock()
		start := time.Now()
		hold := newHoldBudget()
		bytes := 0
		for {
			if it.barrier != nil {
				s.writeTextsLocked(batch) // every earlier item, then the barrier
				batch = batch[:0]
				close(it.barrier)
			} else {
				batch = append(batch, it)
				bytes += len(it.pack.Text)
			}
			done = append(done, it)
			hold.spend()
			// The writes come after the batch is taken, so the bytes bound
			// the hold too (large texts are written a few per hold).
			if hold.exhausted() || bytes >= holdBatchBytes {
				break
			}
			var ok bool
			select {
			case it, ok = <-q.queue:
			default:
			}
			if !ok || it == nil {
				break
			}
		}
		s.writeTextsLocked(batch)
		clear(batch)
		batch = batch[:0]
		s.mu.Unlock()
		held := time.Since(start)
		if s.lockStats.Load() {
			q.held.add(held)
		}
		// After the gate: a reader under the gate saw the text pending or
		// on file.
		for _, it := range done {
			q.mu.Lock()
			if it.barrier == nil && q.pending[it.ref] == it {
				delete(q.pending, it.ref)
			}
			q.mu.Unlock()
			s.releaseText(it)
		}
		done = done[:0]
		s.textYield(held)
	}
}

// After a hold the writer stays off the gate for textYieldFactor times its
// length (half of the gate) and does not rest while its queue is more than
// half full (textBacklogYieldFactor). The rest was larger (3, and 1 under a
// backlog) while XLog and profile writes ran under the engine lock and a
// text hold delayed them; they now run on their own writers, ingest group
// lookups use the read cache, and the larger rest only let the text queue
// fill and make ingest wait (measured, local replay: at 22,000 mixed XLog/s the UDP queue
// grew to 7,000-8,000 and rejected; with these values it stayed near 300,
// and a 100% new-text storm at 20,000 XLog/s still lost no XLog, 3 of 3).
const (
	textYieldFactor        = 1
	textBacklogYieldFactor = 0
)

func (s *Store) textYield(held time.Duration) {
	q := &s.texts
	factor := time.Duration(textYieldFactor)
	q.mu.Lock()
	backlog := q.bytes > textQueueMaxBytes/2
	q.mu.Unlock()
	if backlog || len(q.queue) > textQueueMax/2 {
		factor = textBacklogYieldFactor
	}
	time.Sleep(factor * held)
}

// writeTextsLocked stores a hold of queued texts (the caller holds Store.mu),
// those of one dictionary in one batch (putTextBatchLocked). The pending
// check is made under the gate, where purge and delete discard, so a
// discarded text never recreates a removed directory.
func (s *Store) writeTextsLocked(items []*textItem) {
	q := &s.texts
	var paths []string
	byPath := map[string][]*textItem{}
	for _, it := range items {
		q.mu.Lock()
		live := q.pending[it.ref] == it
		q.mu.Unlock()
		if !live || s.closed {
			continue
		}
		if _, ok := byPath[it.ref.path]; !ok {
			paths = append(paths, it.ref.path)
		}
		byPath[it.ref.path] = append(byPath[it.ref.path], it)
	}
	for _, path := range paths {
		group := byPath[path]
		wrote, errs := s.putTextBatchLocked(path, group)
		for n, it := range group {
			if errs[n] != nil {
				q.errors.Add(1)
			} else if wrote[n] {
				// Stored now: reads find it and a resubmission is skipped
				// (enqueueText), as TextDupCheck skips it in the original.
				s.rememberRead(it.ref, it.pack.Text)
			}
		}
	}
}

// releaseText returns the bytes of a written or skipped item and wakes a
// PutText waiting for room.
func (s *Store) releaseText(it *textItem) {
	q := &s.texts
	q.mu.Lock()
	q.bytes -= it.size
	// Saturation ends only below half (hysteresis): a writer that keeps up
	// with a trickle must not make every PutText wait again.
	low := q.bytes <= textQueueMaxBytes/2 && len(q.queue) <= textQueueMax/2
	q.mu.Unlock()
	if low {
		q.saturated.Store(false)
	}
	select {
	case q.space <- struct{}{}:
	default:
	}
}

// enqueueText records it as pending unless a text of the same key is pending
// already, and queues it. A full queue is waited for at most textQueueWait,
// once per saturation; then the text is dropped, counted and logged (S140),
// as the original TextWR drops at once: PutText runs in the ingest path, so a
// stalled text store must not hold up XLog ingest. A Sync barrier waits as
// long as it takes.
func (s *Store) enqueueText(ctx context.Context, it *textItem) error {
	_, err := s.offerText(ctx, it, true)
	return err
}

// offerText is enqueueText; with wait false it never waits: a full queue
// that is not saturated returns done=false and keeps the text pending
// without queue bytes (held), so readers see it as they would have while
// PutText waited for room; the caller submits it later (Submit), which
// queues or drops it like PutText, or discards it when a purge removed it
// from pending meanwhile.
func (s *Store) offerText(ctx context.Context, it *textItem, wait bool) (done bool, err error) {
	q := &s.texts
	if it.barrier == nil {
		it.size = len(it.pack.Text) + len(it.ref.path) + len(it.ref.key) + 64
	}
	var timer *time.Timer
	deadline := func() <-chan time.Time {
		if it.barrier != nil {
			return nil
		}
		if timer == nil {
			timer = time.NewTimer(textQueueWait)
		}
		return timer.C
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		q.mu.Lock()
		if q.closed || q.queue == nil {
			q.releaseHeldLocked(it)
			q.mu.Unlock()
			return true, ErrClosed
		}
		if it.held && q.pending[it.ref] != it {
			q.mu.Unlock()
			return true, nil // purged (or deleted) while held: not written
		}
		if p := q.pending[it.ref]; it.barrier == nil && p != nil && p != it {
			q.mu.Unlock()
			return true, nil // the first value of a key wins
		}
		if _, stored := q.read[it.ref]; it.barrier == nil && stored {
			// Known stored (read or written before; the original skips it
			// with TextDupCheck): the first value of a key wins on the file.
			q.releaseHeldLocked(it)
			q.mu.Unlock()
			return true, nil
		}
		if q.bytes == 0 || q.bytes+it.size <= textQueueMaxBytes {
			q.bytes += it.size
			if it.barrier == nil {
				q.pending[it.ref] = it
				delete(q.missing, it.ref) // found from now on
			}
			q.senders.Add(1)
			q.mu.Unlock()
			break
		}
		q.mu.Unlock()
		if it.barrier == nil && q.saturated.Load() {
			return true, s.dropText(it)
		}
		if !wait {
			s.holdText(it)
			return false, nil
		}
		select {
		case <-ctx.Done():
			s.releaseHeld(it)
			return true, ctx.Err()
		case <-q.space:
		case <-deadline():
			q.saturated.Store(true)
			return true, s.dropText(it)
		}
	}
	defer q.senders.Done()
	select {
	case q.queue <- it:
		return true, nil
	default:
	}
	if it.barrier == nil && q.saturated.Load() {
		s.unqueueText(it)
		return true, s.dropText(it)
	}
	if !wait {
		q.mu.Lock()
		q.bytes -= it.size // keep it pending, without queue bytes
		it.held = true
		q.mu.Unlock()
		signal(q.space)
		return false, nil
	}
	select {
	case q.queue <- it:
		return true, nil
	case <-ctx.Done():
		s.unqueueText(it)
		return true, ctx.Err()
	case <-deadline():
		q.saturated.Store(true)
		s.unqueueText(it)
		return true, s.dropText(it)
	}
}

// unqueueText takes back a reservation that never reached the queue.
func (s *Store) unqueueText(it *textItem) {
	q := &s.texts
	q.mu.Lock()
	if q.pending[it.ref] == it {
		delete(q.pending, it.ref)
	}
	q.bytes -= it.size
	q.mu.Unlock()
	signal(q.space) // room for another sender; not progress of the writer
}

// holdText keeps it pending without queue bytes (offerText with wait false).
func (s *Store) holdText(it *textItem) {
	q := &s.texts
	q.mu.Lock()
	if q.pending[it.ref] == nil {
		q.pending[it.ref] = it
		delete(q.missing, it.ref) // found from now on
		it.held = true
	}
	q.mu.Unlock()
}

// releaseHeld takes a held text out of pending when it is not queued.
func (s *Store) releaseHeld(it *textItem) {
	q := &s.texts
	q.mu.Lock()
	q.releaseHeldLocked(it)
	q.mu.Unlock()
}

func (q *textQueue) releaseHeldLocked(it *textItem) {
	if it.held && q.pending[it.ref] == it {
		delete(q.pending, it.ref)
	}
}

func (s *Store) dropText(it *textItem) error {
	s.releaseHeld(it) // a dropped held text moves to the dropped cache
	s.texts.dropped.Add(1)
	s.rememberDropped(it)
	s.logJava("S140", "queue exceeded!!")
	return nil
}

// Dropped texts stay readable in a small bounded cache, as the original puts
// a text in TextCache before offering it to the TextWR queue: a dropped
// service or object name is still shown (from memory, not from the file),
// and ingest group lookups of it are answered without the Store gate.
const (
	textRecentMax      = 8192
	textRecentMaxBytes = 4 << 20
)

func (s *Store) rememberDropped(it *textItem) {
	q := &s.texts
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.recent == nil {
		q.recent = map[textRef]string{}
	}
	if _, ok := q.recent[it.ref]; ok {
		return
	}
	q.recent[it.ref] = it.pack.Text
	q.recentOrder = append(q.recentOrder, it.ref)
	q.recentBytes += len(it.pack.Text) + len(it.ref.path) + len(it.ref.key)
	for len(q.recentOrder) > textRecentMax || q.recentBytes > textRecentMaxBytes {
		old := q.recentOrder[0]
		q.recentOrder = q.recentOrder[1:]
		if text, ok := q.recent[old]; ok {
			q.recentBytes -= len(text) + len(old.path) + len(old.key)
			delete(q.recent, old)
		}
	}
}

// TextDropped counts texts dropped because the text queue stayed full.
func (s *Store) TextDropped() uint64 { return s.texts.dropped.Load() }

// pendingText returns a queued text of ref (Store.mu may or may not be held).
func (s *Store) pendingText(ref textRef) (string, bool) {
	q := &s.texts
	q.mu.Lock()
	defer q.mu.Unlock()
	if it := q.pending[ref]; it != nil {
		return it.pack.Text, true
	}
	if text, ok := q.recent[ref]; ok {
		return text, true // dropped, kept in memory
	}
	if e, ok := q.read[ref]; ok {
		if !e.used {
			e.used = true
			q.read[ref] = e
		}
		return e.text, true // read from the file before
	}
	return "", false
}

// knownMissing reports whether a file read found no text of ref since the
// last text of ref was queued.
func (s *Store) knownMissing(ref textRef) bool {
	q := &s.texts
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.missing[ref]
	return ok
}

// textMissingMax bounds the not-found cache (rememberMissing).
const textMissingMax = 1 << 18

// rememberMissing records that the files have no text of ref, unless a text
// of ref was queued meanwhile (the caller read under the Store gate; a text
// queued after that read is pending, so it is not hidden).
func (s *Store) rememberMissing(ref textRef) {
	q := &s.texts
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.pending[ref] != nil {
		return
	}
	if q.missing == nil {
		q.missing = map[textRef]struct{}{}
	}
	if _, ok := q.missing[ref]; ok {
		return
	}
	ref.path = q.internPathLocked(ref.path)
	q.missing[ref] = struct{}{}
	q.missingOrder = append(q.missingOrder, ref)
	for len(q.missingOrder) > textMissingMax {
		delete(q.missing, q.missingOrder[0])
		q.missingOrder = q.missingOrder[1:]
	}
}

// textRead is one read cache entry; used is the second chance bit.
type textRead struct {
	text string
	used bool
}

// TextCacheDefaultBytes is the read cache bound when Config.TextCacheBytes
// is 0; textReadOverhead is charged per entry for its map and order slots.
const (
	TextCacheDefaultBytes = 128 << 20
	textReadOverhead      = 96
	textReadSpare         = 64
	textPathsMax          = 1024
)

func textReadCost(ref textRef, text string) int {
	return len(text) + len(ref.key) + textReadOverhead
}

// internPathLocked returns the one copy of path the cache entries share.
func (q *textQueue) internPathLocked(path string) string {
	if p, ok := q.paths[path]; ok {
		return p
	}
	if q.paths == nil || len(q.paths) >= textPathsMax {
		q.paths = map[string]string{} // entries keep their copies
	}
	q.paths[path] = path
	return path
}

// rememberRead caches a text read from (or written to) the files under ref,
// evicting past the byte bound: the entry at the front of readOrder goes,
// unless it was read since it got there (second chance).
func (s *Store) rememberRead(ref textRef, text string) {
	q := &s.texts
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.read == nil {
		q.read = map[textRef]textRead{}
	}
	if _, ok := q.read[ref]; ok {
		return
	}
	max := q.readMax
	if max <= 0 {
		max = TextCacheDefaultBytes
	}
	if textReadCost(ref, text) > max/8 {
		return // one text must not empty the cache
	}
	ref.path = q.internPathLocked(ref.path)
	q.read[ref] = textRead{text: text}
	q.readOrder = append(q.readOrder, ref)
	q.readBytes += textReadCost(ref, text)
	// Each pass over an entry clears its bit, so the loop ends. At most
	// textReadSpare entries get their second chance per call: with every
	// entry in use, one insert would otherwise walk the whole cache under
	// the lock every lookup takes (and, for a file read, under the Store
	// gate).
	for spared := 0; q.readBytes > max && len(q.readOrder) > 0; {
		old := q.readOrder[0]
		q.readOrder = q.readOrder[1:]
		e, ok := q.read[old]
		if !ok {
			continue // purged; its slot is skipped
		}
		if e.used && spared < textReadSpare {
			spared++
			e.used = false
			q.read[old] = e
			q.readOrder = append(q.readOrder, old)
			continue
		}
		q.readBytes -= textReadCost(old, e.text)
		delete(q.read, old)
		q.stats.evicted.Add(1)
	}
}

// flushTexts waits until every text queued before it is written.
func (s *Store) flushTexts(ctx context.Context) error {
	it := &textItem{barrier: make(chan struct{})}
	if err := s.enqueueText(ctx, it); err != nil {
		if err == ErrClosed {
			return nil // Close wrote everything
		}
		return err
	}
	select {
	case <-it.barrier:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// discardTextsLocked drops queued texts whose index lies below roots; the
// writer skips them (purge and database delete, under Store.mu).
func (s *Store) discardTextsLocked(roots []string) {
	q := &s.texts
	q.mu.Lock()
	defer q.mu.Unlock()
	gone := func(ref textRef) bool {
		return under(ref.path, roots) || under(ref.path+".kfile", roots) || under(ref.path+".data", roots)
	}
	for ref := range q.pending {
		if gone(ref) {
			delete(q.pending, ref)
		}
	}
	for ref, text := range q.recent {
		if gone(ref) {
			q.recentBytes -= len(text) + len(ref.path) + len(ref.key)
			delete(q.recent, ref) // its recentOrder slot is skipped when evicted
		}
	}
	removed := false
	for ref, e := range q.read {
		if gone(ref) {
			q.readBytes -= textReadCost(ref, e.text)
			delete(q.read, ref)
			removed = true
		}
	}
	if removed {
		// The cache may never fill again, so the slots do not wait for an
		// eviction to skip them.
		keep := q.readOrder[:0]
		for _, ref := range q.readOrder {
			if _, ok := q.read[ref]; ok {
				keep = append(keep, ref)
			}
		}
		clear(q.readOrder[len(keep):])
		q.readOrder = keep
	}
	for ref := range q.missing {
		if gone(ref) {
			delete(q.missing, ref) // its missingOrder slot is skipped when evicted
		}
	}
}

// closeTexts stops intake, lets every accepted text reach the writer and
// waits until it has written them and exited. It runs before Close takes the
// Store gate, which the writer needs.
func (s *Store) closeTexts() {
	q := &s.texts
	q.mu.Lock()
	if q.queue == nil { // never opened
		q.closed = true
		q.mu.Unlock()
		return
	}
	if q.closed {
		q.mu.Unlock()
		<-q.done
		return
	}
	q.closed = true
	q.mu.Unlock()
	s.stopTextFetcher()
	q.senders.Wait()
	close(q.queue)
	<-q.done
	close(q.space) // wakes PutText calls waiting for room; they see closed
}

// TextWriteErrors counts queued texts the writer failed to store.
func (s *Store) TextWriteErrors() uint64 { return s.texts.errors.Load() }
