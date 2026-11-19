// Copyright Scouter authors, Apache-2.0. Asynchronous XLog and profile writes.
package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// The original server writes XLogs and profiles on their own threads:
// XLogCore hands the encoded XLog to XLogWR (queue of xlog_queue_size, one
// writer thread: data, then the time/txid/gxid index) and ProfileCore to
// XLogProfileWR (profile_queue_size); a full queue drops with S144/S142.
// Collection never waits for the disk. Here EnqueueXLogContext and
// EnqueueProfileContext validate and encode the pack, then hand it to the
// Store's writer goroutine of its kind through a bounded queue. The writer
// stores queued records in short Store gate holds (scanHoldSteps records or
// scanHoldTime), so the ingest engine lock is no longer held for these disk
// writes (measured, local replay: at 9,000 mixed XLog/s nearly all CPU under the engine
// lock was pwrite of XLog and profile records).
//
// A full queue (records or bytes) is waited for at most packQueueWait, once
// per saturation; then the record is dropped, counted and logged as the
// original does at once. Saturation ends when the writer has brought the
// queue to half. A queued record is not readable until it is written, as with
// the original (realtime XLogs come from the engine's ring, not the files).
// Sync waits for everything queued before it; Close writes every accepted
// record before it takes the gate. A crash loses the queue, like the
// original's.
//
// Lock order: Store.mu before packQueue.mu (a leaf, never held while waiting
// for the queue or the gate).

// holdBatchBytes bounds the record bytes a writer takes into one hold's
// batch (pack and text writers), so a hold of large records stays short.
const holdBatchBytes = 1 << 20

const (
	packQueueWait     = 100 * time.Millisecond
	xlogQueueDefault  = 10000 // xlog_queue_size
	profQueueDefault  = 1000  // profile_queue_size
	packQueueMaxBytes = 64 << 20
)

// packItem is one encoded record, or a Sync barrier (closed by the writer
// once every earlier item is written).
type packItem struct {
	date             string
	b                []byte
	time, txid, gxid int64
	barrier          chan struct{}
	size             int    // charged to packQueue.bytes
	seq              uint64 // Store.packSeq when it was enqueued (packDiscardedLocked)
	// member is b's gzip member, built by the writer before it takes the
	// gate when the queue's data is compressed (packQueue.prepare); it
	// points into the writer's arena and is valid for that hold only.
	member []byte
}

// packDiscard is one purge or database delete (discardPacksLocked): a queued
// record enqueued before it (seq) whose day's xlog directory it touched is
// not written, as it would have been written and then removed with the
// directory when writes were synchronous.
type packDiscard struct {
	seq   uint64
	roots []string
	at    time.Time
}

// packDiscardMax and packDiscardAge bound the purge/delete records the
// writers check: at most packDiscardMax, none older than packDiscardAge (a
// record waits in a queue for milliseconds; a stall longer than that is not
// covered).
const (
	packDiscardMax = 64
	packDiscardAge = time.Hour
)

type packQueue struct {
	code, what    string // drop log, e.g. S144 "queue exceeded!!"
	errCode, kind string // write failure log, e.g. SZ143 "XLogWR"
	max           int
	// writeBatch stores records of one date in order (under Store.mu) and
	// returns each one's error.
	writeBatch func(date string, items []*packItem) []error
	// zipLive reports the live compression setting of the queue's data
	// (compress_xlog_enabled, compress_profile_enabled): when set, the
	// writer builds the gzip members of a hold's records before the gate
	// (prepare). A date pinned the other way by its conf file is decided
	// under the gate as before: an unprepared record is compressed there, a
	// prepared one of an uncompressed date ignores its member.
	zipLive func() bool
	gz      gzipMember // the writer goroutine's
	arena   []byte     // the members of one hold
	offs    []int
	// atGate is set while the writer waits for or holds the Store gate with
	// the records it took (tests wait for it: the records are off the queue
	// before the gate is taken).
	atGate atomic.Bool

	mu      sync.Mutex
	closed  bool
	senders sync.WaitGroup
	bytes   int
	space   chan struct{}
	queue   chan *packItem
	done    chan struct{}
	held    holdSum // Store gate holds (LockStats)

	saturated                                     atomic.Bool
	enqueued, written, dropped, errors, discarded atomic.Uint64
}

// PackQueueStats are the counters of one writer queue (/health).
type PackQueueStats struct {
	Enqueued, Written, Dropped, WriteErrors uint64
	// Discarded counts records a purge or database delete removed before
	// they were written.
	Discarded uint64
	Len       int
}

func (q *packQueue) stats() PackQueueStats {
	n := 0
	if q.queue != nil {
		n = len(q.queue)
	}
	return PackQueueStats{q.enqueued.Load(), q.written.Load(), q.dropped.Load(), q.errors.Load(), q.discarded.Load(), n}
}

// XLogQueueStats and ProfileQueueStats report the writer queues.
func (s *Store) XLogQueueStats() PackQueueStats    { return s.xlogQ.stats() }
func (s *Store) ProfileQueueStats() PackQueueStats { return s.profQ.stats() }

func (s *Store) startPackWriters() {
	s.xlogQ.code, s.xlogQ.what, s.xlogQ.errCode, s.xlogQ.kind = "S144", "queue exceeded!!", "SZ143", "XLogWR"
	s.xlogQ.max = s.config.XLogQueueSize
	s.xlogQ.writeBatch = s.putXLogBatchLocked
	s.xlogQ.zipLive, s.profQ.zipLive = s.live.compressXLog.Load, s.live.compressProfile.Load
	s.profQ.code, s.profQ.what, s.profQ.errCode, s.profQ.kind = "S142", "queue exceeded!!", "S141", "XLogProfileWR"
	s.profQ.max = s.config.ProfileQueueSize
	s.profQ.writeBatch = s.putProfileBatchLocked
	for _, q := range []*packQueue{&s.xlogQ, &s.profQ} {
		q.queue = make(chan *packItem, q.max)
		q.space = make(chan struct{}, 1)
		q.done = make(chan struct{})
		go s.packWriter(q)
	}
}

// EnqueueXLogContext is PutXLog for the ingest path: the XLog is validated
// and encoded now and written by the XLog writer (XLogWR). A full queue drops
// it after a short wait (counted, S144) and returns nil.
func (s *Store) EnqueueXLogContext(ctx context.Context, p *protocol.XLogPack) error {
	w, err := s.prepareXLog(p, nil)
	if err != nil {
		return err
	}
	return s.Submit(ctx, w)
}

// EnqueueXLogEncodedContext is EnqueueXLogContext for an XLog the caller has
// already encoded (b = EncodePack(p)): ingest encodes it once for its
// realtime ring and the writer stores the same bytes, as XLogCore hands
// XLogWR the bytes it put in XLogCache. b is shared read-only. Encoding is
// deterministic, so b is what EnqueueXLogContext would store; a Store with
// its own codec limits encodes again to apply them.
func (s *Store) EnqueueXLogEncodedContext(ctx context.Context, p *protocol.XLogPack, b []byte) error {
	w, err := s.PrepareXLogEncoded(p, b)
	if err != nil {
		return err
	}
	return s.Submit(ctx, w)
}

// PrepareXLogEncoded validates the XLog of EnqueueXLogEncodedContext and
// returns it as a Write for Offer/Submit (ingestwrite.go).
func (s *Store) PrepareXLogEncoded(p *protocol.XLogPack, b []byte) (Write, error) {
	if s.config.CodecLimits != (protocol.Limits{}) {
		b = nil
	}
	return s.prepareXLog(p, b)
}

// prepareXLog validates p and encodes it unless b is its encoding.
func (s *Store) prepareXLog(p *protocol.XLogPack, b []byte) (Write, error) {
	if p == nil || p.EndTime <= 0 {
		return Write{}, ErrInvalid
	}
	date := JavaDate(p.EndTime, s.config.Location)
	if err := validDate(date); err != nil {
		return Write{}, err
	}
	if b == nil {
		var err error
		if b, err = protocol.EncodePack(p, s.config.CodecLimits); err != nil {
			return Write{}, err
		}
	}
	if len(b) > s.config.MaxRecordBytes {
		return Write{}, ErrLimit
	}
	return Write{queue: XLogQueue, pack: s.newPackItem(&packItem{date: date, b: b, time: p.EndTime, txid: p.TxID, gxid: p.GxID}), packQ: &s.xlogQ}, nil
}

// EnqueueProfileContext is PutProfile for the ingest path (XLogProfileWR;
// S142 on a full queue). It takes ownership of p.Profile: the caller must not
// modify those bytes afterwards (ingest hands over a pack it owns).
func (s *Store) EnqueueProfileContext(ctx context.Context, p *protocol.ProfilePack) error {
	w, err := s.PrepareProfile(p)
	if err != nil {
		return err
	}
	return s.Submit(ctx, w)
}

// PrepareProfile validates the profile of EnqueueProfileContext and returns
// it as a Write (empty for a profile without bytes). It takes ownership of
// p.Profile as EnqueueProfileContext does.
func (s *Store) PrepareProfile(p *protocol.ProfilePack) (Write, error) {
	if p == nil || p.Time <= 0 {
		return Write{}, ErrInvalid
	}
	if len(p.Profile) == 0 {
		return Write{}, nil
	}
	if len(p.Profile) > s.config.MaxRecordBytes {
		return Write{}, ErrLimit
	}
	date := JavaDate(p.Time, s.config.Location)
	if err := validDate(date); err != nil {
		return Write{}, err
	}
	return Write{queue: ProfileQueue, pack: s.newPackItem(&packItem{date: date, b: p.Profile, time: p.Time, txid: p.TxID}), packQ: &s.profQ}, nil
}

// newPackItem charges a record its queue bytes and stamps the purge sequence
// at acceptance: a purge or delete after this point discards the record even
// when it reaches the queue later (a Write submitted after a full queue).
func (s *Store) newPackItem(it *packItem) *packItem {
	it.size = len(it.b) + 64
	it.seq = s.packSeq.Load()
	return it
}

func (s *Store) enqueuePack(ctx context.Context, q *packQueue, it *packItem) error {
	_, err := s.offerPack(ctx, q, it, true)
	return err
}

// offerPack queues it. A full queue is waited for at most packQueueWait,
// once per saturation; then the record is dropped (counted, logged). With
// wait false it never waits: a full queue that is not saturated returns
// done=false with nothing changed (the caller submits it later).
func (s *Store) offerPack(ctx context.Context, q *packQueue, it *packItem, wait bool) (done bool, err error) {
	var timer *time.Timer
	deadline := func() <-chan time.Time {
		if it.barrier != nil {
			return nil
		}
		if timer == nil {
			timer = time.NewTimer(packQueueWait)
		}
		return timer.C
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	drop := func() (bool, error) {
		q.dropped.Add(1)
		s.logJava(q.code, q.what)
		return true, nil
	}
	for {
		q.mu.Lock()
		if q.closed || q.queue == nil {
			q.mu.Unlock()
			return true, ErrClosed
		}
		if q.bytes == 0 || q.bytes+it.size <= packQueueMaxBytes {
			q.bytes += it.size
			q.senders.Add(1)
			q.mu.Unlock()
			break
		}
		q.mu.Unlock()
		if it.barrier == nil && q.saturated.Load() {
			return drop()
		}
		if !wait {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return true, ctx.Err()
		case <-q.space:
		case <-deadline():
			q.saturated.Store(true)
			return drop()
		}
	}
	defer q.senders.Done()
	unreserve := func() {
		q.mu.Lock()
		q.bytes -= it.size
		q.mu.Unlock()
		signal(q.space)
	}
	select {
	case q.queue <- it:
		q.countEnqueued(it)
		return true, nil
	default:
	}
	if it.barrier == nil && q.saturated.Load() {
		unreserve()
		return drop()
	}
	if !wait {
		unreserve()
		return false, nil
	}
	select {
	case q.queue <- it:
		q.countEnqueued(it)
		return true, nil
	case <-ctx.Done():
		unreserve()
		return true, ctx.Err()
	case <-deadline():
		q.saturated.Store(true)
		unreserve()
		return drop()
	}
}

func (q *packQueue) countEnqueued(it *packItem) {
	if it.barrier == nil {
		q.enqueued.Add(1)
	}
}

// prepare builds the gzip members of the records the writer took for its
// next hold, without the Store gate: compression is most of the CPU of a
// compressed write, and under the gate it made every other gate user wait
// for it. The members are the bytes appendZip would build (same level, one
// member per record).
func (q *packQueue) prepare(items []*packItem) {
	if q.zipLive == nil || !q.zipLive() {
		return
	}
	q.arena, q.offs = q.arena[:0], q.offs[:0]
	for _, it := range items {
		q.offs = append(q.offs, len(q.arena))
		if it.barrier != nil || len(it.b) == 0 || len(it.b) > 0x7fffff {
			continue // appendZip refuses it (or it is a barrier)
		}
		if m, err := q.gz.compress(it.b); err == nil {
			q.arena = append(q.arena, m...)
		}
	}
	q.offs = append(q.offs, len(q.arena))
	for n, it := range items {
		if a, b := q.offs[n], q.offs[n+1]; b > a {
			it.member = q.arena[a:b:b]
		}
	}
	if cap(q.gz.member.b) > 1<<20 {
		q.gz.member.b = nil
	}
	if cap(q.arena) > 4<<20 {
		q.arena = nil // the items keep this hold's
	}
}

// packWriter owns q's receive side until closePackQueue closes it. A hold
// writes the queued records the writer took before it (at most
// scanHoldSteps records or holdBatchBytes, their gzip members built by
// prepare) in batches (one write per file for a run of records of one date,
// as consecutive single writes would leave the files), then releases the
// gate, which hands over in FIFO order, so queries, the text writer and the
// other pack writer get their turn between holds.
func (s *Store) packWriter(q *packQueue) {
	defer close(q.done)
	var done, batch []*packItem
	flush := func() {
		for start := 0; start < len(batch); {
			end := start + 1
			for end < len(batch) && batch[end].date == batch[start].date {
				end++
			}
			for _, err := range q.writeBatch(batch[start].date, batch[start:end]) {
				if err != nil {
					q.errors.Add(1)
					// The original prints the stack trace of every failure;
					// here once per 10s per writer (logJava), with the cause.
					s.logJava(q.errCode, q.kind+" write failed: "+err.Error())
				} else {
					q.written.Add(1)
				}
			}
			start = end
		}
		clear(batch)
		batch = batch[:0]
	}
	for it := range q.queue {
		// The hold's records are taken before the gate, so the bytes bound
		// the hold (large profiles are written a few per hold).
		done = append(done, it)
		for bytes := len(it.b); len(done) < scanHoldSteps && bytes < holdBatchBytes; {
			var next *packItem
			select {
			case next = <-q.queue:
			default:
			}
			if next == nil { // empty, or closed and drained
				break
			}
			done = append(done, next)
			bytes += len(next.b)
		}
		q.prepare(done)
		q.atGate.Store(true)
		s.mu.Lock()
		var start time.Time
		if s.lockStats.Load() {
			start = time.Now()
		}
		for _, it := range done {
			if it.barrier != nil {
				flush()
				close(it.barrier) // every earlier item is written
			} else if s.packDiscardedLocked(it) {
				q.discarded.Add(1)
			} else if !s.closed {
				batch = append(batch, it)
			}
		}
		flush()
		q.atGate.Store(false)
		s.mu.Unlock()
		if !start.IsZero() {
			q.held.add(time.Since(start))
		}
		freed := 0
		for _, it := range done {
			freed += it.size
		}
		clear(done) // the records may be large; do not keep them until the next hold
		done = done[:0]
		q.mu.Lock()
		q.bytes -= freed
		low := q.bytes <= packQueueMaxBytes/2 && len(q.queue) <= q.max/2
		q.mu.Unlock()
		if low {
			q.saturated.Store(false)
		}
		signal(q.space)
	}
}

// discardPacksLocked records a purge or database delete of roots (the caller
// holds Store.mu, as the writers do when they check it).
func (s *Store) discardPacksLocked(roots []string) {
	now := time.Now()
	keep := s.packDiscards[:0]
	for _, d := range s.packDiscards {
		if now.Sub(d.at) < packDiscardAge {
			keep = append(keep, d)
		}
	}
	if len(keep) >= packDiscardMax {
		keep = append(keep[:0], keep[len(keep)-packDiscardMax+1:]...)
	}
	s.packDiscards = append(keep, packDiscard{seq: s.packSeq.Add(1), roots: append([]string(nil), roots...), at: now})
}

// packDiscardedLocked reports whether a purge or delete after it was enqueued
// touched the record's xlog directory (the directory or anything in it: all
// XLog and profile files of a day, compressed data included, are there).
func (s *Store) packDiscardedLocked(it *packItem) bool {
	if len(s.packDiscards) == 0 {
		return false
	}
	dir := s.path(it.date, "xlog", "")
	for _, d := range s.packDiscards {
		if d.seq <= it.seq {
			continue
		}
		if under(dir, d.roots) {
			return true
		}
		for _, root := range d.roots {
			if under(root, []string{dir}) {
				return true
			}
		}
	}
	return false
}

// flushPackQueue waits until every record queued in q before it is written.
func (s *Store) flushPackQueue(ctx context.Context, q *packQueue) error {
	it := &packItem{barrier: make(chan struct{})}
	if err := s.enqueuePack(ctx, q, it); err != nil {
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

// flushPacks is flushPackQueue for both writers.
func (s *Store) flushPacks(ctx context.Context) error {
	if err := s.flushPackQueue(ctx, &s.xlogQ); err != nil {
		return err
	}
	return s.flushPackQueue(ctx, &s.profQ)
}

// closePackQueue stops intake, lets every accepted record reach the writer
// and waits until it has written them and exited. It runs before Close takes
// the Store gate, which the writer needs.
func (s *Store) closePackQueue(q *packQueue) {
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
	q.senders.Wait()
	close(q.queue)
	<-q.done
	close(q.space) // wakes senders waiting for room; they see closed
}
