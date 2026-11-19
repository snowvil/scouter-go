package ingest

import (
	"bytes"
	"context"
	"math"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/storage"
)

type statusKey struct {
	hash int32
	key  string
}
type cachedPacket struct {
	data    []byte
	expires int64
}
type interactionKey struct {
	kind     string
	from, to int32
}
type interactionTable struct {
	entries map[interactionKey]cachedPacket
	order   []interactionKey
}
type alertCount struct {
	level byte
	count int32
}
type alertSummary struct {
	objType string
	counts  map[string]alertCount
	titles  []string
}
type packetState struct {
	statuses        map[statusKey]cachedPacket
	interactions    map[int32]*interactionTable
	interactionRows int
	nextCleanup     int64
	nextSummary     int64
	alerts          [][]byte
	alertIndex      int32
	alertLoop       int64
	alertSummaries  map[int32]*alertSummary
	sampling        *samplingState
	group           *groupState
}

func newPacketState(now int64) *packetState {
	return &packetState{statuses: map[statusKey]cachedPacket{}, interactions: map[int32]*interactionTable{},
		nextCleanup: now + 5000, nextSummary: (now/300000 + 1) * 300000, alerts: make([][]byte, 1024), alertSummaries: map[int32]*alertSummary{}, sampling: newSamplingState(now, &defaultSettings, defaultGxidKeepCount), group: newGroupState(now)}
}

// cacheDelta refuses a cache update that grows the accounted bytes past
// MaxCacheBytes. An update that does not grow them (delta <= 0) always
// passes, even when the object map and kube state (coreDelta) leave the
// cache no room.
func (e *Engine) cacheDelta(delta int) error {
	if delta > 0 && delta > e.cacheRoom() {
		return protocol.ErrLimit
	}
	return nil
}

// cacheRoom is what is left of the cache budget (negative when the object
// map and kube state alone hold more, see coreDelta).
func (e *Engine) cacheRoom() int { return e.config.MaxCacheBytes - e.cacheBytes - e.ringBytes }

// coreDelta is cacheDelta for the object map and the kube pod seq state.
// They are AgentManager state, not a cache: a refusal loses the heartbeat
// (object record, liveness), so only their own bytes count against
// MaxCacheBytes and a full cache never refuses them. They stay in cacheBytes,
// so the caches skip their updates instead; the accounted total is at most
// twice MaxCacheBytes (CONTRACT.md §12).
func (e *Engine) coreDelta(delta int) error {
	if delta > e.config.MaxCacheBytes-e.coreBytes {
		return protocol.ErrLimit
	}
	return nil
}

// addCoreBytes accounts n bytes of the object map or the kube state.
func (e *Engine) addCoreBytes(n int) {
	e.coreBytes += n
	e.cacheBytes += n
}

func (e *Engine) acceptExtra(ctx context.Context, p protocol.Pack, now int64) error {
	switch v := p.(type) {
	case *protocol.DroppedXLogPack:
		e.dropDelaying(v.TxID, v.GxID)
		e.droppedXlogs.Add(1)
		return nil
	case *protocol.StatusPack:
		v.Time = now
		key := statusKey{v.ObjHash, v.Key}
		b, err := protocol.EncodePack(v, protocol.Limits{})
		if err != nil {
			return err
		}
		previous, exists := e.extra.statuses[key]
		delta := len(b) - len(previous.data)
		if !exists {
			delta += len(key.key) + 16
		}
		// StatusCore: time = now, StatusCache.put, then StatusWR.add (queued).
		// Past MaxCounters or the cache budget only the cache is skipped
		// (Stats.StatusCacheSkipped): a new key is not cached and an existing
		// one is dropped, so the cache never shows a value older than the
		// one received (CONTRACT.md §12).
		if (exists || len(e.extra.statuses)+e.extra.interactionRows < e.config.MaxCounters) && e.cacheDelta(delta) == nil {
			e.extra.statuses[key] = cachedPacket{b, now + 15000}
			e.cacheBytes += delta
		} else {
			if exists {
				delete(e.extra.statuses, key)
				e.cacheBytes -= len(previous.data) + len(key.key) + 16
			}
			e.statusCacheSkipped.Add(1)
		}
		e.w.enqueue(writeStatus, 1, packHeapBytes(v), one(func(ctx context.Context) error { return e.store.PutStatusContext(ctx, v) }))
		return nil
	case *protocol.InteractionPerfCounterPack:
		hash := storage.CounterHash(v.ObjName)
		key := interactionKey{v.InteractionType, v.FromHash, v.ToHash}
		b, err := protocol.EncodePack(v, protocol.Limits{})
		if err != nil {
			return err
		}
		table := e.extra.interactions[hash]
		var previous cachedPacket
		var exists bool
		if table != nil {
			previous, exists = table.entries[key]
		}
		// The original Core calls CacheTable.get before put. An expired key is
		// removed here and is reinserted at the end; a live update stays in place.
		if exists && previous.expires < now {
			e.removeInteraction(hash, key)
			previous, exists = cachedPacket{}, false
			table = e.extra.interactions[hash]
		}
		delta := len(b) - len(previous.data)
		if !exists {
			if len(e.extra.statuses)+e.extra.interactionRows >= e.config.MaxCounters {
				return protocol.ErrLimit
			}
			delta += len(key.kind) + 24
		}
		if table == nil {
			delta += 16
		}
		if err = e.cacheDelta(delta); err != nil {
			return err
		}
		if table == nil {
			table = &interactionTable{entries: map[interactionKey]cachedPacket{}}
			e.extra.interactions[hash] = table
		}
		if !exists {
			table.order = append(table.order, key)
			e.extra.interactionRows++
		}
		table.entries[key] = cachedPacket{b, now + 10000}
		e.cacheBytes += delta
		return nil
	case *protocol.AlertPack:
		return e.acceptAlertLocked(ctx, v, now, nil)
	case *protocol.SummaryPack:
		e.w.enqueue(writeSummary, 1, packHeapBytes(v), one(func(ctx context.Context) error { return e.store.PutSummaryContext(ctx, v) }))
		return nil
	case *protocol.StackPack:
		if obj := e.objects[v.ObjHash]; obj != nil {
			name := obj.ObjName
			v.Data = bytes.Clone(v.Data) // detachBytes
			e.w.enqueue(writeStack, 1, packHeapBytes(v), one(func(ctx context.Context) error { return e.store.PutStackContext(ctx, name, v) }))
			return nil
		}
		// StackAnalyzerDB.open does not invent an object for an unknown Agent
		// (the original drops it silently; here it is counted).
		e.skippedStacks.Add(1)
		return nil
	case *protocol.BatchPack:
		obj := e.objects[v.ObjHash]
		if obj == nil {
			obj = &protocol.ObjectPack{ObjHash: v.ObjHash, ObjName: v.ObjName, ObjType: v.ObjType, Tags: protocol.Value{Type: protocol.ValueMap}}
			if err := e.acceptLocked(ctx, obj, now); err != nil {
				return err
			}
			obj = e.objects[obj.ObjHash]
		}
		if v.StartTime > math.MaxInt64-v.ElapsedTime || v.ElapsedTime < 0 {
			return protocol.ErrInvalid
		}
		name := obj.ObjName
		v.Extra = bytes.Clone(v.Extra) // detachBytes
		e.w.enqueue(writeBatch, 1, packHeapBytes(v), one(func(ctx context.Context) error { return e.store.PutBatchContext(ctx, name, v) }))
		return nil
	case *protocol.SpanContainerPack:
		if v.Timestamp == 0 {
			v.Timestamp = now
		}
		v.Spans = bytes.Clone(v.Spans) // detachBytes
		e.w.enqueue(writeSpan, 1, packHeapBytes(v), one(func(ctx context.Context) error { return e.store.PutSpanContainerContext(ctx, v) }))
		return nil
	default:
		// Map and standalone Span belong to the original extension chain, not
		// the built-in ingest pipeline. Unsupported extensions remain observable.
		return protocol.ErrUnsupported
	}
}

// acceptAlertLocked is AlertCore.add. tagFailed takes the error of a tag
// count write submitted after the engine lock (nil: the caller's result).
func (e *Engine) acceptAlertLocked(ctx context.Context, v *protocol.AlertPack, now int64, tagFailed func(error)) error {
	v.Time = now
	b, err := protocol.EncodePack(v, protocol.Limits{})
	if err != nil {
		return err
	}
	// AlertCore: time = now, AlertSummary.add, AlertCache.put, then
	// AlertWR.add (queued) and the tag count, whatever the write does. Past
	// MaxObjects/MaxCounters or the cache budget the summary and the ring
	// are skipped each on its own (Stats.AlertCacheSkipped); the alert is
	// still stored and tag counted (CONTRACT.md §12).
	e.addAlertSummaryLocked(v)
	e.cacheAlertRingLocked(ctx, b, now)
	e.w.enqueue(writeAlert, 1, packHeapBytes(v), one(func(ctx context.Context) error { return e.store.PutAlertContext(ctx, v) }))
	if !e.settings().TagCount {
		return nil // AlertCore: AlertTagCount.add only when tagcnt_enabled
	}
	if obj := e.objects[v.ObjHash]; obj != nil {
		w, err := e.store.PrepareAlertTags(obj.ObjType, v)
		if err != nil {
			return err
		}
		return e.offerLocked(w, true, tagFailed)
	}
	return nil
}

// detachBytes: a pack the UDP receiver hands over may alias its whole
// datagram (protocol AdoptInput clips the capacity, so the size of the
// underlying buffer cannot be seen). A queued Stack, Span or Batch would keep
// the datagram (up to the reassembly bound) alive while it waits, beyond what
// the byte bound counts. Its payload is copied before it is queued instead:
// one allocation and copy of a payload that is written to disk anyway, on
// packs that are rare next to XLogs and counters. Smaller fields (strings)
// are already copies.

// addAlertSummaryLocked is AlertSummary.add: the five-minute count per
// object and title. A new object past MaxObjects, a new title past
// MaxCounters or a size past the cache budget is not counted
// (Stats.AlertCacheSkipped): the next summary pack misses that alert.
func (e *Engine) addAlertSummaryLocked(v *protocol.AlertPack) {
	summary := e.extra.alertSummaries[v.ObjHash]
	delta := 0
	if summary == nil {
		delta += len(v.ObjType) + 16
	}
	var prior alertCount
	exists := false
	if summary != nil {
		prior, exists = summary.counts[v.Title]
	}
	if !exists {
		delta += len(v.Title) + 16
	}
	if (summary == nil && len(e.extra.alertSummaries) >= e.config.MaxObjects) ||
		(summary != nil && !exists && len(summary.counts) >= e.config.MaxCounters) || e.cacheDelta(delta) != nil {
		e.alertCacheSkipped.Add(1)
		return
	}
	if summary == nil {
		summary = &alertSummary{objType: v.ObjType, counts: map[string]alertCount{}}
		e.extra.alertSummaries[v.ObjHash] = summary
	}
	if !exists {
		summary.titles = append(summary.titles, v.Title)
	}
	summary.counts[v.Title] = alertCount{v.Level, prior.count + 1}
	e.cacheBytes += delta
}

// cacheAlertRingLocked is AlertCache.put of the encoded alert b. Past the
// cache budget the oldest delayed profile groups take the recover rule early
// to make room (Java never refuses the ring: the realtime alert popup comes
// before profiles waiting for a driving XLog). When recovering every delayed
// profile could not make the room (the budget is held by other state), none
// is recovered; then, or on a recovery error, the ring is left as it is
// (cursor unchanged, Stats.AlertCacheSkipped): Client realtime alerts miss
// that alert.
func (e *Engine) cacheAlertRingLocked(ctx context.Context, b []byte, now int64) {
	delta := len(b) - len(e.extra.alerts[e.extra.alertIndex])
	if delta > e.cacheRoom()+e.extra.sampling.profileBytes {
		e.alertCacheSkipped.Add(1)
		return
	}
	for e.cacheDelta(delta) != nil {
		if recovered, err := e.recoverOldestDelayed(ctx, true, now); err != nil || !recovered {
			e.alertCacheSkipped.Add(1)
			return
		}
	}
	e.cacheBytes += delta
	e.putAlertRingLocked(b)
}

// putAlertRingLocked is AlertCache.put: the realtime alert ring. The caller
// accounted the size change (len(b) - len(the replaced slot)).
func (e *Engine) putAlertRingLocked(b []byte) {
	e.extra.alerts[e.extra.alertIndex] = b
	e.extra.alertIndex++
	if int(e.extra.alertIndex) == len(e.extra.alerts) {
		e.extra.alertIndex = 0
		e.extra.alertLoop++
	}
}

// cacheAlertLocked is a bare AlertCache.put(p) with p.time = now (Java
// AgentManager.alertTooManyChange): realtime ring only, no store, summary,
// plugin or tag count. A full cache budget skips it (Stats.AlertCacheSkipped).
func (e *Engine) cacheAlertLocked(ctx context.Context, p *protocol.AlertPack, now int64) {
	p.Time = now
	b, err := protocol.EncodePack(p, protocol.Limits{})
	if err != nil {
		e.objectAlertErrors.Add(1)
		return
	}
	e.cacheAlertRingLocked(ctx, b, now)
}

func (e *Engine) removeInteraction(hash int32, key interactionKey) {
	table := e.extra.interactions[hash]
	if table == nil {
		return
	}
	entry, exists := table.entries[key]
	if !exists {
		return
	}
	delete(table.entries, key)
	for i, candidate := range table.order {
		if candidate == key {
			table.order = append(table.order[:i], table.order[i+1:]...)
			break
		}
	}
	e.extra.interactionRows--
	e.cacheBytes -= len(entry.data) + len(key.kind) + 24
	if len(table.entries) == 0 {
		delete(e.extra.interactions, hash)
		e.cacheBytes -= 16
	}
}

// Tick performs the original periodic cache cleanup and aggregation flushes.
// Interaction enumeration itself deliberately does not apply a strict TTL.
//
// The state work runs under the engine lock; the five-minute alert summary
// and the minute group counter rows are built there and handed to their
// writers (summary, daily counter) after the lock is released, so a slow disk
// does not stall ingest. Write failures are counted and logged by the
// writers (CONTRACT.md §13); Tick returns only errors of the state work.
func (e *Engine) Tick(ctx context.Context) error {
	now, summaries, group, groupDue, held, err := e.tickLocked(ctx)
	// The tag counts of the object alerts whose inbox was full wait for room
	// here, after the engine lock (their failures count as
	// ObjectAlertErrors).
	heldErr := e.submitHeld(ctx, held)
	if err != nil {
		return err
	}
	if heldErr != nil {
		return heldErr
	}
	if e.periodicWriteHook != nil && (len(summaries) > 0 || groupDue) {
		e.periodicWriteHook()
	}
	e.queueAlertSummaries(summaries)
	if groupDue {
		e.queueGroupSave(group)
	}
	// The tag count marks need the Store gate; they run here, outside the
	// engine lock and off the visitor writer, and order with nothing.
	return e.store.TagTickContext(ctx, now)
}

// tickLocked is the engine-locked part of Tick. err ends the tick before any
// write.
func (e *Engine) tickLocked(ctx context.Context) (now int64, summaries []*protocol.SummaryPack, group groupSave, groupDue bool, held heldBatch, err error) {
	if err := e.mu.LockContext(ctx); err != nil {
		return 0, nil, group, false, held, err
	}
	defer e.mu.Unlock()
	defer func() { // before the unlock
		if r := recover(); r != nil {
			e.discardHeldLocked() // nothing of this hold is submitted
			panic(r)
		}
		held = e.takeHeldLocked()
	}()
	if e.closed {
		return 0, nil, group, false, held, storage.ErrClosed
	}
	e.holdCtx = ctx
	now = e.config.Clock().UnixMilli()
	// AgentManager's 1s daemon: inactive transition, alert, pod seq release.
	e.inactivateObjectsLocked(ctx, now)
	if now >= e.extra.nextCleanup {
		for hash, table := range e.extra.interactions {
			kept := table.order[:0]
			var canceled error
			for _, key := range table.order {
				if canceled == nil {
					canceled = ctx.Err()
				}
				entry := table.entries[key]
				if canceled == nil && entry.expires < now {
					delete(table.entries, key)
					e.extra.interactionRows--
					e.cacheBytes -= len(entry.data) + len(key.kind) + 24
				} else {
					kept = append(kept, key)
				}
			}
			table.order = kept
			if len(table.entries) == 0 {
				delete(e.extra.interactions, hash)
				e.cacheBytes -= 16
			}
			if canceled != nil {
				return 0, nil, group, false, held, canceled
			}
		}
		if err := e.expireCounters(ctx, now); err != nil {
			return 0, nil, group, false, held, err
		}
		e.pruneCounterNames()
		for key, entry := range e.extra.statuses {
			if entry.expires < now {
				delete(e.extra.statuses, key)
				e.cacheBytes -= len(entry.data) + len(key.key) + 16
			}
		}
		e.extra.nextCleanup = now + 5000
	}
	if now >= e.extra.nextSummary {
		summaries = e.takeAlertSummariesLocked(now)
		e.extra.nextSummary = (now/300000 + 1) * 300000
	}
	group, groupDue = e.takeGroupSaveLocked(now)
	// The visitor roll is queued to the visitor writer under the engine lock:
	// the visits of the XLogs accepted before this tick are queued before it
	// and the later ones after it, so the roll runs with this tick's now in
	// the same order as when it ran here (an older now must not roll an hour
	// back over a newer pack's hour). It takes the visitor lock only. A full
	// visitor queue drops it like a visit (counted); the next tick rolls.
	e.flushVisitsLocked() // the visits accepted before this tick go first
	e.w.enqueue(writeVisitor, 1, 0, one(func(ctx context.Context) error { return e.store.VisitorRollContext(ctx, now) }))
	return now, summaries, group, groupDue, held, nil
}

func (e *Engine) Status(ctx context.Context, hash int32, key string) (*protocol.StatusPack, error) {
	if err := e.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer e.mu.Unlock()
	entry, exists := e.extra.statuses[statusKey{hash, key}]
	if !exists {
		return nil, nil
	}
	if entry.expires < e.config.Clock().UnixMilli() {
		delete(e.extra.statuses, statusKey{hash, key})
		e.cacheBytes -= len(entry.data) + len(key) + 16
		return nil, nil
	}
	p, err := protocol.DecodePack(entry.data, protocol.Limits{})
	if err != nil {
		return nil, err
	}
	return p.(*protocol.StatusPack), nil
}

func (e *Engine) InteractionCounters(ctx context.Context, hash int32) ([]*protocol.InteractionPerfCounterPack, error) {
	if err := e.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer e.mu.Unlock()
	table := e.extra.interactions[hash]
	if table == nil {
		return nil, nil
	}
	var out []*protocol.InteractionPerfCounterPack
	for _, key := range table.order {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p, err := protocol.DecodePack(table.entries[key].data, protocol.Limits{})
		if err != nil {
			return nil, err
		}
		out = append(out, p.(*protocol.InteractionPerfCounterPack))
	}
	return out, nil
}

func (e *Engine) AlertsRealtime(ctx context.Context, q service.AlertQuery) (service.AlertResult, error) {
	if err := e.mu.LockContext(ctx); err != nil {
		return service.AlertResult{}, err
	}
	defer e.mu.Unlock()
	state := e.extra
	out := service.AlertResult{Loop: state.alertLoop, Index: state.alertIndex}
	capacity := int32(len(state.alerts))
	if q.Index < 0 || q.Index > capacity {
		return out, protocol.ErrInvalid
	}
	add := func(from, to int32) error {
		for i := from; i < to; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if state.alerts[i] == nil {
				continue
			}
			p, err := protocol.DecodePack(state.alerts[i], protocol.Limits{})
			if err != nil {
				return err
			}
			v := p.(*protocol.AlertPack)
			if q.ObjType == "" || q.ObjType == v.ObjType {
				out.Records = append(out.Records, v)
			}
		}
		return nil
	}
	var err error
	switch out.Loop - q.Loop {
	case 0:
		if q.Index < out.Index {
			err = add(q.Index, out.Index)
		}
	case 1:
		start := q.Index
		if start <= out.Index {
			start = out.Index
		}
		err = add(start, capacity)
		if err == nil {
			err = add(0, out.Index)
		}
	default:
		err = add(out.Index, capacity)
		if err == nil {
			err = add(0, out.Index)
		}
	}
	return out, err
}

func (e *Engine) StatusHistory(ctx context.Context, date string, from, to int64) ([]*protocol.StatusPack, error) {
	return e.store.StatusesContext(ctx, date, from, to)
}
func (e *Engine) AlertHistory(ctx context.Context, date string, from, to int64) ([]*protocol.AlertPack, error) {
	return e.store.AlertsContext(ctx, date, from, to)
}
func (e *Engine) Summaries(ctx context.Context, date string, from, to int64, stype byte) ([]*protocol.SummaryPack, error) {
	return e.store.SummariesContext(ctx, date, from, to, stype)
}

// ScanSummaries and ScanAlertHistory stream the time range to visit
// (service.SummaryBackend, service.AlertBackend) without a total cap.
func (e *Engine) ScanSummaries(ctx context.Context, date string, from, to int64, stype byte, visit func(*protocol.SummaryPack) (bool, error)) error {
	return e.store.ScanSummariesContext(ctx, date, from, to, stype, visit)
}
func (e *Engine) ScanAlertHistory(ctx context.Context, date string, from, to int64, visit func(*protocol.AlertPack) (bool, error)) error {
	return e.store.ScanAlertsContext(ctx, date, from, to, visit)
}
func (e *Engine) Stacks(ctx context.Context, objName string, from, to int64) ([]*protocol.StackPack, error) {
	return e.store.StacksContext(ctx, objName, from, to)
}
func (e *Engine) Batches(ctx context.Context, objName string, from, to int64) ([]*protocol.BatchPack, error) {
	return e.store.BatchesContext(ctx, objName, from, to)
}
func (e *Engine) BatchAt(ctx context.Context, objName string, time, position int64) (*protocol.BatchPack, error) {
	return e.store.BatchAtContext(ctx, objName, time, position)
}

func (e *Engine) XLogsByGxID(ctx context.Context, date string, gxid int64) ([]*protocol.XLogPack, error) {
	return e.store.XLogsByGxIDContext(ctx, date, gxid)
}
func (e *Engine) XLogsByTime(ctx context.Context, date string, from, to int64) ([]*protocol.XLogPack, error) {
	return e.store.XLogsByTimeContext(ctx, date, from, to)
}
func (e *Engine) SpansByGxID(ctx context.Context, date string, gxid int64) ([]*protocol.SpanPack, error) {
	return e.store.SpansByGxIDContext(ctx, date, gxid)
}

func (e *Engine) BatchStack(ctx context.Context, objName string, tm int64, filename string) ([][]byte, error) {
	return e.store.BatchArchiveContext(ctx, tm, objName, filename)
}
