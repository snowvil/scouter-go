package ingest

import (
	"context"
	"sync"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/storage"
)

// Storage queue writes of the collection path (texts, XLogs, profiles, tag
// counts) are offered under the engine lock without waiting
// (storage.Store.Offer). A write whose queue is full is kept and submitted by
// the lock holder after it releases the lock, with the queue's bounded wait
// (storage.Store.Submit): one collector waits for room, the others go on
// (CONTRACT.md §11). Validation errors are still returned under the lock, as
// the queuing calls returned them.

// deferredWrite is a write offered under the engine lock whose queue was
// full.
type deferredWrite struct {
	w storage.Write
	// acceptCtx submits with the caller's ctx, else context.Background(),
	// as the in-lock queuing call did.
	acceptCtx bool
	// failed takes a submit error that is not the caller's result (a GeoIP
	// city text, the tag count of an object alert); nil returns it.
	failed func(error)
}

// heldBatch is the deferred writes of one hold, taken by the lock holder;
// id tracks it until submitted (heldFlights).
type heldBatch struct {
	writes []deferredWrite
	id     uint64
	err    error // of the writes submitted under the lock (heldErr)
}

// heldMaxWrites and heldMaxBytes (storage.Write.Size; the first held write
// is kept whatever its size) bound the writes one hold keeps for after the
// lock. A hold that would pass them (a driving XLog or a sampling bucket
// roll releasing many delayed XLogs while the XLog queue is full) submits
// the held writes in order under the lock instead, each with its queue's
// bounded wait as every write waited before, then goes on offering
// (StoreWritesHeldInline): the order and the drops are the previous ones,
// and no more than this stays outside the cache budget (the released records
// were taken out of it).
const (
	heldMaxWrites = 1024
	heldMaxBytes  = 4 << 20
)

// offerLocked offers w to its queue. A write of a queue that already has a
// deferred write in this hold is deferred too, so the writes of one queue
// keep their order. The caller holds e.mu and submits the held writes after
// releasing it (unlockHeld, or takeHeldLocked and submitHeld).
func (e *Engine) offerLocked(w storage.Write, acceptCtx bool, failed func(error)) error {
	q := w.Queue()
	if q == storage.NoQueue {
		return nil
	}
	bit := uint8(1) << q
	if e.heldQueues&bit == 0 {
		done, err := e.store.Offer(w)
		if err != nil || done {
			return err
		}
	}
	size := w.Size()
	if len(e.held) >= heldMaxWrites || (len(e.held) > 0 && e.heldBytes+size > heldMaxBytes) {
		e.submitHeldInlineLocked()
		done, err := e.store.Offer(w)
		if err != nil || done {
			return err
		}
	}
	e.held = append(e.held, deferredWrite{w, acceptCtx, failed})
	e.heldQueues |= bit
	e.heldBytes += size
	e.writesDeferred.Add(1)
	return nil
}

// putTextLocked is TextWR.add (Store.PutText) of the collection path.
func (e *Engine) putTextLocked(date string, p *protocol.TextPack, failed func(error)) error {
	w, err := e.store.PrepareText(date, p)
	if err != nil {
		return err
	}
	return e.offerLocked(w, false, failed)
}

// submitHeldInlineLocked submits the held writes now, under the engine lock
// (the hold reached heldMaxWrites/heldMaxBytes). Their first error without a
// failed hook becomes the hold's result (heldErr), as a submit after the
// lock would.
func (e *Engine) submitHeldInlineLocked() {
	ctx := e.holdCtx
	if ctx == nil {
		ctx = context.Background()
	}
	writes := e.held
	e.writesHeldInline.Add(uint64(len(writes)))
	e.dropHeldLocked()
	if err := e.submitWrites(ctx, writes); err != nil && e.heldErr == nil {
		e.heldErr = err
	}
}

// takeHeldLocked takes the deferred writes of this hold and registers them
// in flight (Flush and Close wait for them), with the error of writes
// submitted under the lock; the caller releases e.mu and then calls
// submitHeld.
func (e *Engine) takeHeldLocked() heldBatch {
	b := heldBatch{err: e.heldErr}
	e.heldErr, e.holdCtx = nil, nil
	if len(e.held) == 0 {
		return b
	}
	b.writes, b.id = e.held, e.flights.begin()
	e.dropHeldLocked()
	return b
}

// dropHeldLocked forgets the deferred writes of this hold (taken).
func (e *Engine) dropHeldLocked() {
	e.held, e.heldQueues, e.heldBytes = nil, 0, 0
}

// discardHeldLocked drops the deferred writes of a hold that panicked, as
// their full queues would (a held text leaves pending for the dropped-text
// cache), so the next holder does not submit them.
func (e *Engine) discardHeldLocked() {
	for _, d := range e.held {
		e.store.Drop(d.w)
		e.writesHeldDropped.Add(1)
	}
	e.dropHeldLocked()
	e.heldErr, e.holdCtx = nil, nil
}

// submitHeld submits the deferred writes in offer order, each with its
// queue's bounded wait. It runs without the engine lock, on the goroutine
// that held it. It returns the first submit error of a write without a
// failed hook (only ErrClosed or ctx's error: a full queue drops after its
// wait without an error).
func (e *Engine) submitHeld(ctx context.Context, b heldBatch) error {
	if b.id == 0 {
		return b.err
	}
	defer e.flights.end(b.id)
	if err := e.submitWrites(ctx, b.writes); b.err == nil {
		return err
	}
	return b.err
}

// submitWrites submits writes in order; it returns the first error of a
// write without a failed hook.
func (e *Engine) submitWrites(ctx context.Context, writes []deferredWrite) (err error) {
	for _, d := range writes {
		c := context.Background()
		if d.acceptCtx {
			c = ctx
		}
		if serr := e.store.Submit(c, d.w); serr != nil {
			if d.failed != nil {
				d.failed(serr)
			} else if err == nil {
				err = serr
			}
		}
	}
	return err
}

// unlockHeld releases e.mu and submits the writes the hold deferred.
func (e *Engine) unlockHeld(ctx context.Context) error {
	b := e.takeHeldLocked()
	e.mu.Unlock()
	return e.submitHeld(ctx, b)
}

// heldFlights tracks the held batches being submitted after their hold, so
// Flush and Close can wait for the ones taken before them. Ids increase in
// hold order (begin runs under the engine lock).
type heldFlights struct {
	mu      sync.Mutex
	last    uint64
	active  map[uint64]struct{}
	changed chan struct{} // closed and replaced by end
}

func (f *heldFlights) begin() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.active == nil {
		f.active = map[uint64]struct{}{}
		f.changed = make(chan struct{})
	}
	f.last++
	f.active[f.last] = struct{}{}
	return f.last
}

func (f *heldFlights) end(id uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.active, id)
	close(f.changed)
	f.changed = make(chan struct{})
}

// lastID is the id of the latest batch taken (the caller holds the engine
// lock, so no batch of an earlier hold is taken after it).
func (f *heldFlights) lastID() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last
}

// wait returns once no batch with an id up to upTo is still being submitted.
func (f *heldFlights) wait(ctx context.Context, upTo uint64) error {
	for {
		f.mu.Lock()
		busy := false
		for id := range f.active {
			if id <= upTo {
				busy = true
				break
			}
		}
		changed := f.changed
		f.mu.Unlock()
		if !busy {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// serviceTextRef is a service text the group step looks up.
type serviceTextRef struct {
	date    string
	service int32
}

// serviceTextHint is the service text answer lockForAccept found for the
// XLog it locks for, kept for that hold's group step (processGroup) so the
// lookup is not repeated under the lock. end is the XLog's EndTime (0 when
// acceptLocked stamps it: then the date is compared).
type serviceTextHint struct {
	ref   serviceTextRef
	end   int64
	text  string
	found bool
	err   error
	ok    bool
	// unanswered: the store did not answer within serviceTextWait; the
	// group step leaves the XLog without a group (ServiceTextSkipped).
	unanswered bool
}

// serviceTextWait is the longest an XLog waits for its service text to be
// read from the files (Store.TextWithin). A read normally takes
// microseconds; when the Store gate is slow (a cold page cache, a stalled
// disk) the XLog is accepted without its group instead of holding the UDP
// worker, and the text is read ahead for the next XLog of the service.
const serviceTextWait = 50 * time.Millisecond

// matches reports whether h answers the group step of an XLog of service
// that ends at end.
func (h *serviceTextHint) matches(service int32, end int64, loc func(int64) string) bool {
	if !h.ok || h.ref.service != service {
		return false
	}
	if h.end != 0 {
		return h.end == end
	}
	return h.ref.date == loc(end)
}

// serviceLookupLocked reports the service text the group step of x will look
// up: an XLog of type 0/1 without a group whose service has no
// service->group memo. It changes nothing; the caller holds e.mu.
func (e *Engine) serviceLookupLocked(x *protocol.XLogPack) (serviceTextRef, bool) {
	if x.Service == 0 || x.Group != 0 || (x.XType != 0 && x.XType != 1) {
		return serviceTextRef{}, false
	}
	if _, ok := e.extra.group.services[x.Service]; ok {
		return serviceTextRef{}, false
	}
	end := x.EndTime
	if end == 0 {
		end = e.config.Clock().UnixMilli() // acceptLocked stamps it
	}
	return serviceTextRef{scoutertime.Date(end, e.config.Location), x.Service}, true
}

// lockForAccept takes the engine lock for p. When p is an XLog whose group
// step will look up its service text (serviceLookupLocked), the answer is
// found here and kept for the group step (groupHint): from the storage
// caches (Store.TextCached), or, when they cannot answer, by having the
// files read with the engine lock released (before anything changed) and
// taken again, waiting at most serviceTextWait for the read
// (Store.TextWithin). After the read the caches are asked again under the
// lock (a text queued meanwhile wins); else the read's answer, or its error,
// is used, and when there is none in time the XLog gets no group
// (ServiceTextSkipped). The check does not ask whether the XLog is stored
// now: one delayed by sampling has its text read too, so its group step when
// its driving XLog releases it is answered from the cache (unless evicted by
// then: it gets no group either).
func (e *Engine) lockForAccept(ctx context.Context, p protocol.Pack) error {
	if err := e.mu.LockContext(ctx); err != nil {
		return err
	}
	x, _ := p.(*protocol.XLogPack)
	if x == nil || e.closed {
		return nil
	}
	r, lookup := e.serviceLookupLocked(x)
	if !lookup {
		return nil
	}
	hint := serviceTextHint{ref: r, end: x.EndTime, ok: true}
	var known bool
	if hint.text, hint.found, known = e.store.TextCached(r.date, "service", r.service); !known {
		e.mu.Unlock()
		e.serviceTextsAhead.Add(1)
		hint.text, hint.found, known, hint.err = e.store.TextWithin(ctx, r.date, "service", r.service, serviceTextWait)
		hint.unanswered = !known && hint.err == nil
		if e.serviceTextReadHook != nil {
			e.serviceTextReadHook()
		}
		if err := e.mu.LockContext(ctx); err != nil {
			return err
		}
		if text, found, known := e.store.TextCached(r.date, "service", r.service); known {
			hint.text, hint.found, hint.err, hint.unanswered = text, found, nil, false
		}
		if e.closed {
			return nil
		}
	}
	e.groupHint = hint
	return nil
}
