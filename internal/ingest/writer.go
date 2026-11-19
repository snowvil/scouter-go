package ingest

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Asynchronous record writers (CONTRACT.md §13).
//
// The original server never writes to disk on the UDP thread: every kind
// goes through a *Core queue and a *WR/*DB writer queue, each consumed by one
// thread, and a full RequestQueue drops the new record at once with a
// rate-limited log line. Here the engine lock updates the in-memory state and
// only enqueues an owned snapshot of the record; one goroutine per kind
// writes them in FIFO order. XLogs, profiles and texts have their own writers
// in storage (storage CONTRACT §8) and are not repeated here.
//
// Ownership: an item's write closure captures values the engine no longer
// touches (a pack handed over by the accept path, a copy of an object pack,
// scalars). The engine never waits for a writer while it holds Engine.mu. The
// daily writer takes Engine.mu to undo a failed group save
// (restoreGroupSave), so Flush and Close must not be called under Engine.mu.

type writeKind uint8

const (
	writeCounter writeKind = iota // RealtimeCounterWR
	writeDaily                    // DailyCounterWR (agent non-realtime packs, Auto5MSampling, XLogGroupPerf)
	writeObject                   // ObjectWR
	writeStatus                   // StatusCore -> StatusWR
	writeAlert                    // AlertCore -> AlertWR
	writeSummary                  // SummaryCore -> SummaryWR (agent packs and the alert summary)
	writeStack                    // StackAnalyzerCore -> StackAnalyzerDB
	writeBatch                    // BatchCore -> BatchDB
	writeSpan                     // SpanCore -> ZipkinSpanWR
	writeVisitor                  // VisitorCore (VisitorDB/VisitorHourlyDB) and the visitor/tag roll
	writeKinds
)

// writerSpec is one kind's queue. items is the original queue size
// (CoreRun/DBCtr.MAX_QUE_SIZE 10000, span_queue_size 1000). bytes bounds the
// estimated heap of the queued records (packHeapBytes): the original has no
// byte bound (Java heap), and a queued Go pack weighs up to ~20x its encoded
// size (a counter entry is a 272-byte Entry). The values let a disk stall of
// a few seconds pass at ordinary rates without drops while bounding the sum
// of all kinds to 194 MiB of estimated heap (summary: the Agents send their
// 5-minute SummaryPacks together, and one of 5000 rows weighs about 8 MiB
// queued). A record bigger than the bound
// is accepted when its queue is empty. A periodic pass (passKind) is exempt
// from the byte bound: its size is bounded by the engine state it was taken
// from, and at most one pass of each producer is queued (CONTRACT.md §13).
// dropIDs and errID are the original log ids of a full queue and of a
// write failure ("-": the original prints a stack trace without an id).
type writerSpec struct {
	name           string
	items, bytes   int
	dropIDs, errID string
}

var writerSpecs = [writeKinds]writerSpec{
	writeCounter: {"counter", 10000, 32 << 20, "S109/S134", "S133"},
	writeDaily:   {"daily", 10000, 32 << 20, "S123", "S122"},
	writeObject:  {"object", 10000, 8 << 20, "S131", "-"},
	writeStatus:  {"status", 10000, 8 << 20, "S113/S136", "S135"},
	writeAlert:   {"alert", 10000, 8 << 20, "S106/S121", "S120"},
	// Every Agent sends its summaries at the same 5-minute boundary, each up
	// to protocol.AgentPackLimits rows; the budget holds that burst (64MiB
	// did not with many Agents).
	writeSummary: {"summary", 10000, 256 << 20, "SummaryCore/S211", "S210"},
	writeStack:   {"stack", 10000, 16 << 20, "S200/S000", "-"},
	writeBatch:   {"batch", 10000, 8 << 20, "S200/S000", "-"},
	writeSpan:    {"span", 1000, 16 << 20, "SZ116/SZ144", "SZ143"},
	writeVisitor: {"visitor", 10000, 2 << 20, "S208", "S199"},
}

// writerCloseDeadline bounds how long Engine.Close writes the queued records
// before it gives up on the rest (counted as Abandoned). The original drops
// every queue on shutdown; the bound keeps a stalled disk from stopping the
// shutdown, which still has the storage close (fsync, headers) to do. 5s is
// the bound the runtime already gives the SQL table worker.
const writerCloseDeadline = 5 * time.Second

// writerStuckGrace is how long close waits, once it has given up on the
// queues, for the writer goroutines to return. A write that sees its
// cancelled context returns at once; one stuck in a call that ignores it (a
// hung disk) is left running, its records still counted as Pending, so the
// shutdown goes on to the storage close and the process budget (main
// --shutdown-timeout) decides when to give up.
const writerStuckGrace = time.Second

// StallWriterHook, when set before any engine starts (tests only), runs on
// a writer goroutine before each write with the kind's name. The
// cmd/scouter-go tests hold a writer in it the way a stalled disk holds a
// call that never sees its context.
var StallWriterHook func(kind string)

// writerLogInterval is the original Logger.println(id, 10, ...) interval.
const writerLogInterval = 10 * time.Second

// passKind names the producer of a periodic pass item. The original queues
// such a pass record by record and drops what does not fit; here a pass is one
// item, exempt from the byte bound, and refused while the previous pass of
// the same producer is still queued (a stalled writer then drops later
// passes, as a full RequestQueue would, instead of piling them up).
type passKind uint8

const (
	passNone         passKind = iota
	passSample5M              // Auto5MSampling (daily)
	passGroupSave             // XLogGroupPerf minute save (daily)
	passAlertSummary          // AlertSummary.doFlush (summary)
	passKinds
)

// errWriterFull and errWriterClosed are why enqueue refused an item.
var (
	errWriterFull   = errors.New("ingest writer queue full")
	errWriterClosed = errors.New("ingest writer closed")
)

// writeItem is one queued record (records == 1) or periodic pass (a 5m sample
// pass, a group save, the alert summary), or a Flush barrier. write returns
// the number of records written before an error.
type writeItem struct {
	records, size int
	pass          passKind
	write         func(context.Context) (int, error)
	barrier       chan struct{}
}

// invalidRecord reports whether a write failed on the record itself (Agent
// input the storage format cannot hold: an unsupported value type or time
// type, a name or record over a format limit) rather than on I/O. Such
// failures are counted apart (WriteInvalid).
func invalidRecord(err error) bool {
	return errors.Is(err, protocol.ErrInvalid) || errors.Is(err, protocol.ErrLimit) || errors.Is(err, protocol.ErrUnsupported)
}

func one(f func(context.Context) error) func(context.Context) (int, error) {
	return func(ctx context.Context) (int, error) {
		if err := f(ctx); err != nil {
			return 0, err
		}
		return 1, nil
	}
}

type writerQueue struct {
	kind writeKind
	spec writerSpec
	ch   chan *writeItem // never closed: senders cannot panic after Close
	done chan struct{}   // closed when the writer goroutine has returned

	mu             sync.Mutex
	closed         bool
	bytes, pending int // queued bytes; queued or in-progress records

	enqueued, written, writeErrors, writeInvalid, dropped, abandoned atomic.Uint64
	dropLog, errLog, invalidLog                                      rateLog
}

// WriterStats are one writer queue's counters. Enqueued = Written +
// WriteErrors + WriteInvalid + Abandoned + Pending. WriteErrors are failed
// writes (I/O, closed storage), WriteInvalid records the storage format
// refused (invalidRecord). Dropped records were refused at a full (or
// closed) queue and never enqueued.
type WriterStats struct {
	Kind                                                             string
	Enqueued, Written, WriteErrors, WriteInvalid, Dropped, Abandoned uint64
	Pending                                                          int
}

type writers struct {
	q       [writeKinds]*writerQueue
	ctx     context.Context // cancelled when Close gives up on the queues
	cancel  context.CancelFunc
	stop    chan struct{}
	abandon atomic.Bool
	once    sync.Once
	// passQueued marks a periodic pass of each producer that is queued or
	// being written (passKind).
	passQueued [passKinds]atomic.Bool
	// hook, when set (tests), runs on the writer goroutine before each write.
	hook atomic.Pointer[func(context.Context, writeKind)]
}

func newWriters() *writers {
	ctx, cancel := context.WithCancel(context.Background())
	w := &writers{ctx: ctx, cancel: cancel, stop: make(chan struct{})}
	for k := range w.q {
		spec := writerSpecs[k]
		w.q[k] = &writerQueue{kind: writeKind(k), spec: spec, ch: make(chan *writeItem, spec.items), done: make(chan struct{})}
	}
	return w
}

// start launches one goroutine per kind; each ends in close.
func (w *writers) start() {
	for _, q := range w.q {
		go w.run(q)
	}
}

func (w *writers) run(q *writerQueue) {
	defer close(q.done)
	for {
		select {
		case it := <-q.ch:
			w.process(q, it)
		case <-w.stop:
			for {
				select {
				case it := <-q.ch:
					w.process(q, it)
				default:
					return
				}
			}
		}
	}
}

func (w *writers) process(q *writerQueue, it *writeItem) {
	if it.barrier != nil {
		close(it.barrier) // every earlier item of this queue is done
		return
	}
	if w.abandon.Load() {
		q.abandoned.Add(uint64(it.records))
		w.release(q, it)
		return
	}
	if h := w.hook.Load(); h != nil {
		(*h)(w.ctx, q.kind)
	}
	if h := StallWriterHook; h != nil {
		h(q.spec.name)
	}
	n, err := it.write(w.ctx)
	q.written.Add(uint64(n))
	if err != nil {
		failed := uint64(it.records - n)
		var batch *visitBatchError
		if errors.As(err, &batch) {
			// The batch tried every record and says which were invalid.
			invalid := uint64(batch.invalid)
			if invalid > 0 {
				total := q.writeInvalid.Add(invalid)
				if q.invalidLog.allow() {
					log.Printf("ingest: %s writer refused a record (%s): %v (%d invalid records so far)", q.spec.name, q.spec.errID, batch.invalidErr, total)
				}
			}
			if failed > invalid {
				total := q.writeErrors.Add(failed - invalid)
				if q.errLog.allow() {
					log.Printf("ingest: %s writer failed (%s): %v (%d write errors so far)", q.spec.name, q.spec.errID, batch.failedErr, total)
				}
			}
		} else if invalidRecord(err) {
			// Invalid input: the first record failed on its own content;
			// the rest of a pass was not attempted and counts as errors.
			total := q.writeInvalid.Add(1)
			if failed > 1 {
				q.writeErrors.Add(failed - 1)
			}
			if q.invalidLog.allow() {
				log.Printf("ingest: %s writer refused a record (%s): %v (%d invalid records so far)", q.spec.name, q.spec.errID, err, total)
			}
		} else {
			total := q.writeErrors.Add(failed)
			if q.errLog.allow() {
				log.Printf("ingest: %s writer failed (%s): %v (%d write errors so far)", q.spec.name, q.spec.errID, err, total)
			}
		}
	}
	w.release(q, it)
}

func (w *writers) release(q *writerQueue, it *writeItem) {
	q.mu.Lock()
	q.bytes -= it.size
	q.pending -= it.records
	q.mu.Unlock()
	if it.pass != passNone {
		w.passQueued[it.pass].Store(false)
	}
}

// enqueue hands a record to its writer without waiting. A full queue (items
// or bytes) drops it: counted and logged once per 10s with the original ids,
// as RequestQueue.put does. After Close the record is counted as dropped
// without that log (the closing log reports the writers). The error says why
// it was refused.
func (w *writers) enqueue(k writeKind, records, size int, write func(context.Context) (int, error)) error {
	return w.put(k, &writeItem{records: records, size: size + 128, write: write}) // 128: the item and its closure
}

// enqueuePass is enqueue for a periodic pass: no byte bound, at most one
// queued pass per producer (passKind).
func (w *writers) enqueuePass(k writeKind, pass passKind, records, size int, write func(context.Context) (int, error)) error {
	return w.put(k, &writeItem{records: records, size: size + 128, pass: pass, write: write})
}

func (w *writers) put(k writeKind, it *writeItem) error {
	q := w.q[k]
	var err error
	q.mu.Lock()
	switch {
	case q.closed:
		err = errWriterClosed
	case it.pass != passNone && !w.passQueued[it.pass].CompareAndSwap(false, true):
		err = errWriterFull // the previous pass of this producer is still queued
	case it.pass == passNone && q.bytes > 0 && q.bytes+it.size > q.spec.bytes:
		err = errWriterFull
	default:
		// Counted before the send, so a reader never sees the writer's
		// Written ahead of Enqueued (bytes and pending are released under
		// q.mu, which is held here).
		q.enqueued.Add(uint64(it.records))
		select {
		case q.ch <- it:
			q.bytes += it.size
			q.pending += it.records
		default:
			q.enqueued.Add(^uint64(it.records - 1)) // undo
			if it.pass != passNone {
				w.passQueued[it.pass].Store(false)
			}
			err = errWriterFull
		}
	}
	q.mu.Unlock()
	if err == nil {
		return nil
	}
	total := q.dropped.Add(uint64(it.records))
	if err == errWriterFull && q.dropLog.allow() {
		log.Printf("ingest: %s writer queue exceeded!! (%s; %d records dropped so far)", q.spec.name, q.spec.dropIDs, total)
	}
	return err
}

// flush waits until every item enqueued before it in the queues of kinds
// (all when none is given) is done (written, failed or abandoned). A closed
// queue was drained by Close.
func (w *writers) flush(ctx context.Context, kinds ...writeKind) error {
	var barriers [writeKinds]chan struct{}
	selected := func(k writeKind) bool {
		if len(kinds) == 0 {
			return true
		}
		for _, x := range kinds {
			if x == k {
				return true
			}
		}
		return false
	}
	for k, q := range w.q {
		if !selected(writeKind(k)) {
			continue
		}
		q.mu.Lock()
		closed := q.closed
		q.mu.Unlock()
		if closed {
			continue
		}
		b := make(chan struct{})
		select {
		case q.ch <- &writeItem{barrier: b}:
			barriers[k] = b
		case <-q.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for k, b := range barriers {
		if b == nil {
			continue
		}
		select {
		case <-b:
		case <-w.q[k].done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for k, q := range w.q {
		if !selected(writeKind(k)) {
			continue
		}
		q.mu.Lock()
		closed := q.closed
		q.mu.Unlock()
		if closed {
			select {
			case <-q.done:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

// close stops intake, lets the writers finish their queues within deadline,
// then counts what is left as abandoned (the in-progress write sees its ctx
// cancelled) and waits writerStuckGrace for every writer goroutine to
// return. A writer still in a write after that is logged with its pending
// records and left running (writerStuckGrace). It reports whether the
// deadline passed.
func (w *writers) close(deadline time.Duration) (expired bool) {
	w.once.Do(func() {
		for _, q := range w.q {
			q.mu.Lock()
			q.closed = true
			q.mu.Unlock()
		}
		close(w.stop)
		timer := time.NewTimer(deadline)
		defer timer.Stop()
		expired = !w.returned(timer.C)
		if expired {
			w.abandon.Store(true)
			w.cancel()
			grace := time.NewTimer(writerStuckGrace)
			defer grace.Stop()
			if !w.returned(grace.C) {
				for _, q := range w.q {
					select {
					case <-q.done:
						continue
					default:
					}
					q.mu.Lock()
					pending := q.pending
					q.mu.Unlock()
					log.Printf("ingest: %s writer still in a write %v after the close deadline (a call that ignores cancellation, e.g. a stalled disk): %d records left pending", q.spec.name, writerStuckGrace, pending)
				}
			}
		}
		w.cancel()
	})
	return expired
}

// returned waits until every writer goroutine has returned (true) or until
// fires (false).
func (w *writers) returned(until <-chan time.Time) bool {
	for _, q := range w.q {
		select {
		case <-q.done:
		case <-until:
			return false
		}
	}
	return true
}

func (w *writers) stats() []WriterStats {
	out := make([]WriterStats, len(w.q))
	for i, q := range w.q {
		q.mu.Lock()
		pending := q.pending
		q.mu.Unlock()
		// The outcomes are read before Enqueued: a record is counted
		// enqueued before its outcome, so the snapshot never shows more
		// outcomes than enqueued records.
		st := WriterStats{Kind: q.spec.name, Written: q.written.Load(), WriteErrors: q.writeErrors.Load(), WriteInvalid: q.writeInvalid.Load(), Abandoned: q.abandoned.Load(), Dropped: q.dropped.Load(), Pending: pending}
		st.Enqueued = q.enqueued.Load()
		out[i] = st
	}
	return out
}

// rateLog lets one line through per writerLogInterval (wall clock).
type rateLog struct{ next atomic.Int64 }

func (r *rateLog) allow() bool {
	now := time.Now().UnixNano()
	next := r.next.Load()
	return now >= next && r.next.CompareAndSwap(next, now+int64(writerLogInterval))
}

// Estimated heap bytes of a queued record (writerSpec.bytes). Strings and
// byte slices count their length, each Value/Entry its struct size; slice
// headers and allocator rounding are not counted.
const (
	valueStructBytes = int(unsafe.Sizeof(protocol.Value{}))
	entryStructBytes = int(unsafe.Sizeof(protocol.Entry{}))
)

func valueHeapBytes(v *protocol.Value) int {
	n := len(v.Text) + len(v.Bytes) + 4*len(v.Ints) + 4*len(v.Floats) + 8*len(v.Longs)
	for _, s := range v.Texts {
		n += 16 + len(s)
	}
	for i := range v.Entries {
		n += entryStructBytes + len(v.Entries[i].Key) + valueHeapBytes(&v.Entries[i].Value)
	}
	for i := range v.Values {
		n += valueStructBytes + valueHeapBytes(&v.Values[i])
	}
	return n
}

func packHeapBytes(p protocol.Pack) int {
	const base = 64 // the pack struct's scalars
	switch v := p.(type) {
	case *protocol.PerfCounterPack:
		return base + valueStructBytes + len(v.ObjName) + valueHeapBytes(&v.Data)
	case *protocol.ObjectPack:
		return base + valueStructBytes + len(v.ObjType) + len(v.ObjName) + len(v.Address) + len(v.Version) + valueHeapBytes(&v.Tags)
	case *protocol.StatusPack:
		return base + valueStructBytes + len(v.ObjType) + len(v.Key) + valueHeapBytes(&v.Data)
	case *protocol.AlertPack:
		return base + valueStructBytes + len(v.ObjType) + len(v.Title) + len(v.Message) + valueHeapBytes(&v.Tags)
	case *protocol.SummaryPack:
		return base + valueStructBytes + len(v.ObjType) + valueHeapBytes(&v.Table)
	case *protocol.StackPack:
		return base + len(v.Data)
	case *protocol.BatchPack:
		n := base + 128 + len(v.BatchJobID) + len(v.Args) + len(v.ObjName) + len(v.ObjType) + len(v.Extra) + 64*len(v.SQLStats)
		for _, s := range v.UniqueSQLs {
			n += 24 + len(s.Text)
		}
		return n
	case *protocol.SpanContainerPack:
		return base + len(v.Spans)
	}
	return base
}
