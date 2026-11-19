package ingest

import (
	"container/list"
	"context"
	"errors"

	"github.com/snowvil/scouter-go/internal/protocol"
)

const (
	discardNone byte = 1
	discardAll  byte = 2
)

// processedSet is XLogDelayingCache.processedGxidMap, a LongIntLinkedMap
// bounded by setMax (xlog_sampling_matcher_gxid_keep_memory_count, fixed at
// startup) and filled with add in LAST mode: a txid added again keeps its
// place and accumulates the discard flag (int, wrapping); a new txid at the
// bound first evicts the oldest. Nothing else removes an entry, so the
// insertion order is a ring. Neither the map nor the ring holds a pointer,
// so the GC does not scan the entries (a container/list element and its
// value per entry were 96% of the live heap objects at 30,000 XLog/s, local replay).
type processedSet struct {
	discard map[int64]int32
	order   []int64 // insertion order from order[head] once the ring is full
	head    int
	maximum int
}

// newProcessedSet takes the positive xlog_sampling_matcher_gxid_keep_memory_count
// (config and New pass only positive values). LongIntLinkedMap treats
// max <= 0 as unbounded; a bound below 1 is taken as 1 here, as every engine
// cache has a bound.
func newProcessedSet(maximum int) processedSet {
	return processedSet{discard: map[int64]int32{}, maximum: max(maximum, 1)}
}
func (s *processedSet) len() int   { return len(s.order) }
func (s *processedSet) full() bool { return len(s.order) >= s.maximum }
func (s *processedSet) get(txid int64) (int32, bool) {
	d, ok := s.discard[txid]
	return d, ok
}

// accumulate adds discard to the flag of a present txid and reports whether
// it was present.
func (s *processedSet) accumulate(txid int64, discard int32) bool {
	d, ok := s.discard[txid]
	if ok {
		s.discard[txid] = d + discard
	}
	return ok
}

// push adds a txid that is not present, evicting the oldest when full. The
// ring grows up to maximum and is reused from then on.
func (s *processedSet) push(txid int64, discard int32) {
	if s.full() {
		delete(s.discard, s.order[s.head])
		s.order[s.head] = txid
		if s.head++; s.head == len(s.order) {
			s.head = 0
		}
	} else {
		if len(s.order) == cap(s.order) {
			s.order = append(make([]int64, 0, min(s.maximum, max(2*cap(s.order), 1024))), s.order...)
		}
		s.order = append(s.order, txid)
	}
	s.discard[txid] = discard
}

type delayedTrace struct {
	gxid int64
	data [][]byte
}
type delayingBucket struct {
	entries map[int64]*list.Element
	order   *list.List
	// maximum is LongKeyLinkedMap.setMax, fixed when the bucket is created
	// (XLogDelayingCache/ProfileDelayingCache newMap).
	maximum int
	profile bool // a ProfileDelayingCache bucket
}
type samplingState struct {
	processed      processedSet
	xlogUnit       int64
	xlogCurrent    *delayingBucket
	xlogOld        *delayingBucket
	profileUnit    int64
	profileCurrent *delayingBucket
	profileOld     []*delayingBucket
	// xlogBytes and profileBytes are the accounted bytes (part of
	// cacheBytes) of the delayed groups of each kind: what recovering every
	// group of the kind early can free.
	xlogBytes, profileBytes int
}

func newDelayingBucket(maximum int, profile bool) *delayingBucket {
	return &delayingBucket{entries: map[int64]*list.Element{}, order: list.New(), maximum: maximum, profile: profile}
}

// delayedBytes is xlogBytes or profileBytes.
func (s *samplingState) delayedBytes(profile bool) *int {
	if profile {
		return &s.profileBytes
	}
	return &s.xlogBytes
}

// newSamplingState is the XLogDelayingCache/ProfileDelayingCache
// construction with the settings at that time.
func newSamplingState(now int64, s *Settings, gxidKeepCount int) *samplingState {
	return &samplingState{processed: newProcessedSet(gxidKeepCount), xlogUnit: now / int64(s.XLogKeepMillis),
		xlogCurrent: newDelayingBucket(s.XLogKeepCount, false), xlogOld: newDelayingBucket(s.XLogKeepCount, false), profileUnit: now / profileBucketMillis, profileCurrent: newDelayingBucket(s.ProfileKeepCount, true)}
}
func driving(txid, gxid int64) bool { return gxid == txid || gxid == 0 }
func serviceType(kind byte) bool    { return kind == 0 || kind == 1 || kind == 3 }
func aliveXLog(discard byte) bool   { return discard == 1 || discard == 3 || discard == 5 }

func (e *Engine) acceptXLog(ctx context.Context, p *protocol.XLogPack, now int64) error {
	if p.Service == 0 {
		e.dropDelaying(p.TxID, p.GxID)
		e.droppedXlogs.Add(1)
		return nil
	}
	state := e.extra.sampling
	_, processed := state.processed.get(p.GxID)
	canProcess := driving(p.TxID, p.GxID) || p.IgnoreGlobalConsequentSampling || p.DiscardType == 0 ||
		(serviceType(p.XType) && aliveXLog(p.DiscardType)) || p.XType == 5 || processed
	if !canProcess {
		// xlog_sampling_matcher_xlog_keep_memory_millis is read on every
		// delayed pack; the bucket bound when the next bucket is created.
		s := e.settings()
		if unit := now / int64(s.XLogKeepMillis); unit != state.xlogUnit {
			if err := e.recoverBucket(ctx, state.xlogOld, now); err != nil {
				return err
			}
			state.xlogOld, state.xlogCurrent, state.xlogUnit = state.xlogCurrent, newDelayingBucket(s.XLogKeepCount, false), unit
		}
		return e.delayPack(ctx, state.xlogCurrent, p.GxID, p, now)
	}
	if p.IgnoreGlobalConsequentSampling && !aliveXLog(p.DiscardType) {
		return nil
	}
	if driving(p.TxID, p.GxID) {
		e.addProcessed(p.TxID, p.DiscardType)
		if err := e.releaseProfiles(ctx, p.TxID, p.DiscardType, now); err != nil {
			return err
		}
		children := append(e.takeDelayed(state.xlogCurrent, p.TxID), e.takeDelayed(state.xlogOld, p.TxID)...)
		if p.DiscardType == discardNone {
			for _, b := range children {
				if err := ctx.Err(); err != nil {
					return err
				}
				child, err := protocol.DecodePack(b, protocol.Limits{})
				if err != nil {
					return err
				}
				if err = e.storeXLog(ctx, child.(*protocol.XLogPack), now); err != nil {
					return err
				}
			}
		}
	}
	return e.storeXLog(ctx, p, now)
}

func (e *Engine) acceptProfile(ctx context.Context, p *protocol.ProfilePack, now int64) error {
	if len(p.Profile) == 0 {
		return nil
	}
	if !p.Version2 {
		e.profileTest(ProfileTestOld, 0, 0, p)
		return e.putProfile(p)
	}
	state := e.extra.sampling
	processedWithProfile := false
	if discard, ok := state.processed.get(p.GxID); ok {
		// LongIntLinkedMap.add accumulates duplicate driving discard flags.
		processedWithProfile = byte(discard) == discardNone
	}
	canProcess := driving(p.TxID, p.GxID) || p.IgnoreGlobalConsequentSampling ||
		(serviceType(p.XType) && p.DiscardType == discardNone) || p.XType == 5 || processedWithProfile || p.DiscardType == 0
	if canProcess {
		if p.IgnoreGlobalConsequentSampling && p.DiscardType != discardNone {
			return nil
		}
		if p.IgnoreGlobalConsequentSampling {
			e.profileTest(ProfileTestIgnoreCons, 0, 0, p)
		} else {
			e.profileTest(ProfileTestNormal, 0, 0, p)
		}
		return e.putProfile(p)
	}
	if now/profileBucketMillis != state.profileUnit {
		// The original rolls only when a new delayed Profile arrives. Recovery
		// is not a strict wall-clock expiration of each buffered packet.
		// maxBucketCount = xlog_sampling_matcher_profile_keep_memory_secs*1000/500
		// is read at each roll.
		s := e.settings()
		for len(state.profileOld) >= s.profileBuckets() {
			bucket := state.profileOld[0]
			if err := e.recoverBucket(ctx, bucket, now); err != nil {
				return err
			}
			state.profileOld = state.profileOld[1:]
		}
		state.profileOld = append(state.profileOld, state.profileCurrent)
		state.profileCurrent, state.profileUnit = newDelayingBucket(s.ProfileKeepCount, true), now/profileBucketMillis
	}
	return e.delayPack(ctx, state.profileCurrent, p.GxID, p, now)
}

// profileTest calls Config.ProfileTestLog (log_test_rate) when it is set.
func (e *Engine) profileTest(c ProfileTestCase, cnt, total int, p *protocol.ProfilePack) bool {
	if e.config.ProfileTestLog == nil {
		return false
	}
	return e.config.ProfileTestLog(c, cnt, total, p)
}

// putProfile is the single ProfileCore write path: every accepted profile with
// bytes (immediate, Version1, released child and recovered) reaches the
// OnProfile callback before the store write, as PlugInManager.profile precedes
// XLogProfileWR.add in the Java ProfileCore loop.
func (e *Engine) putProfile(p *protocol.ProfilePack) error {
	if len(p.Profile) == 0 {
		return nil
	}
	if e.config.OnProfile != nil {
		e.config.OnProfile(p)
	}
	// The profile writer stores it (XLogProfileWR); a full queue drops it
	// after a bounded wait, counted, taken after the engine lock.
	w, err := e.store.PrepareProfile(p)
	if err != nil {
		return err
	}
	return e.offerLocked(w, false, nil)
}

// addProcessed is XLogDelayingCache.addProcessed. Past the cache budget the
// gxid is not remembered (Stats.XLogCacheSkipped) and the driving XLog is
// still stored: a child arriving later is then delayed and recovered instead
// of being matched (CONTRACT.md §12).
func (e *Engine) addProcessed(txid int64, discard byte) {
	set := &e.extra.sampling.processed
	if set.accumulate(txid, int32(discard)) {
		return
	}
	delta := 40
	if set.full() {
		delta = 0 // the oldest gives its place
	}
	if e.cacheDelta(delta) != nil {
		e.xlogCacheSkipped.Add(1)
		return
	}
	set.push(txid, int32(discard))
	e.cacheBytes += delta
}
func delayedSize(group *delayedTrace) int {
	size := 32
	for _, b := range group.data {
		size += len(b) + 8
	}
	return size
}

// delayPack is XLogDelayingCache/ProfileDelayingCache.addDelaying into
// bucket (the current XLog or profile bucket). The count bound of the bucket
// drops its oldest group as LongKeyLinkedMap.setMax does. The cache budget,
// which Java does not have, never drops a pack: while the pack does not fit,
// the oldest delayed group of its kind takes the recover rule early, as it
// would when its bucket rolls; when recovering every group of its kind could
// not make the room (the budget is held by other state), or nothing of its
// kind is left, the pack itself takes it now and the groups keep waiting
// (Stats.DelayedRecoveredEarly/DelayedDiscardedEarly, CONTRACT.md §12).
func (e *Engine) delayPack(ctx context.Context, bucket *delayingBucket, gxid int64, p protocol.Pack, now int64) error {
	maximum := bucket.maximum
	b, err := protocol.EncodePack(p, protocol.Limits{})
	if err != nil {
		return err
	}
	_, profile := p.(*protocol.ProfilePack)
	kindBytes := e.extra.sampling.delayedBytes(profile)
	var entry *list.Element
	var delta int
	for {
		entry = bucket.entries[gxid]
		delta = len(b) + 8
		if entry == nil {
			delta += 32
			if len(bucket.entries) >= maximum {
				delta -= delayedSize(bucket.order.Front().Value.(*delayedTrace))
			}
		}
		if e.cacheDelta(delta) == nil {
			break
		}
		if delta > e.cacheRoom()+*kindBytes {
			return e.recoverPack(ctx, p, now, true)
		}
		recovered, err := e.recoverOldestDelayed(ctx, profile, now)
		if err != nil {
			return err
		}
		if !recovered {
			return e.recoverPack(ctx, p, now, true)
		}
	}
	if entry == nil {
		if len(bucket.entries) >= maximum {
			first := bucket.order.Front()
			delete(bucket.entries, first.Value.(*delayedTrace).gxid)
			bucket.order.Remove(first)
		}
		entry = bucket.order.PushBack(&delayedTrace{gxid: gxid})
		bucket.entries[gxid] = entry
	}
	group := entry.Value.(*delayedTrace)
	group.data = append(group.data, b)
	e.cacheBytes += delta
	*kindBytes += delta
	return nil
}

// oldestDelayed is the oldest non-empty delayed bucket of profiles or XLogs:
// the one whose front group would be recovered first.
func (e *Engine) oldestDelayed(profile bool) *delayingBucket {
	state := e.extra.sampling
	buckets := []*delayingBucket{state.xlogOld, state.xlogCurrent}
	if profile {
		buckets = append(state.profileOld[:len(state.profileOld):len(state.profileOld)], state.profileCurrent)
	}
	for _, bucket := range buckets {
		if bucket.order.Len() > 0 {
			return bucket
		}
	}
	return nil
}

// recoverOldestDelayed takes the oldest delayed group of profiles or XLogs
// out of its bucket and applies the recover rule to it early. It reports
// false when there is none.
func (e *Engine) recoverOldestDelayed(ctx context.Context, profile bool, now int64) (bool, error) {
	oldest := e.oldestDelayed(profile)
	if oldest == nil {
		return false, nil
	}
	group := e.takeDelayed(oldest, oldest.order.Front().Value.(*delayedTrace).gxid)
	return true, e.recoverGroup(ctx, group, now, true)
}
func (e *Engine) takeDelayed(bucket *delayingBucket, gxid int64) [][]byte {
	entry := bucket.entries[gxid]
	if entry == nil {
		return nil
	}
	group := entry.Value.(*delayedTrace)
	delete(bucket.entries, gxid)
	bucket.order.Remove(entry)
	e.cacheBytes -= delayedSize(group)
	*e.extra.sampling.delayedBytes(bucket.profile) -= delayedSize(group)
	return group.data
}
func (e *Engine) dropDelaying(txid, gxid int64) {
	if !driving(txid, gxid) {
		return
	}
	state := e.extra.sampling
	e.takeDelayed(state.xlogCurrent, txid)
	e.takeDelayed(state.xlogOld, txid)
	e.takeDelayed(state.profileCurrent, txid)
	for _, bucket := range state.profileOld {
		e.takeDelayed(bucket, txid)
	}
}

// releaseProfiles stores the delayed children of txid. Java re-enqueues them
// through ProfileCore.add, which stamps the release time (ProfilePreCore.scala:113,
// ProfileCore.scala:48), so the stored/Pulse time is the release time, not the
// receive time.
func (e *Engine) releaseProfiles(ctx context.Context, txid int64, discard byte, now int64) error {
	state := e.extra.sampling
	children := e.takeDelayed(state.profileCurrent, txid)
	for _, bucket := range state.profileOld {
		children = append(children, e.takeDelayed(bucket, txid)...)
	}
	if discard == discardAll {
		return nil
	}
	logged := 0
	for _, b := range children {
		if err := ctx.Err(); err != nil {
			return err
		}
		p, err := protocol.DecodePack(b, protocol.Limits{})
		if err != nil {
			return err
		}
		x := p.(*protocol.ProfilePack)
		// processDelayingProfiles logs with the receive time, before
		// ProfileCore.add stamps the release time.
		if e.profileTest(ProfileTestDelaying, logged, len(children), x) {
			logged++
		}
		x.Time = now
		if err = e.putProfile(x); err != nil {
			return err
		}
	}
	return nil
}

// recoverBucket is XLogDelayingsRecoverCore/ProfileDelayingsRecoverCore for
// a rolled bucket: every group in insertion order takes the recover rule.
func (e *Engine) recoverBucket(ctx context.Context, bucket *delayingBucket, now int64) error {
	for bucket.order.Len() > 0 {
		group := bucket.order.Front().Value.(*delayedTrace)
		if err := e.recoverGroup(ctx, e.takeDelayed(bucket, group.gxid), now, false); err != nil {
			return err
		}
	}
	return nil
}

// recoverGroup applies the recover rule to the children of one group already
// taken out of its bucket (early: before the bucket rolled). An error (ctx
// canceled, decode, store) ends it: the child that failed and the ones after
// it are lost and counted in Stats.DelayedAbandoned.
func (e *Engine) recoverGroup(ctx context.Context, children [][]byte, now int64, early bool) error {
	for i, b := range children {
		err := ctx.Err()
		var p protocol.Pack
		if err == nil {
			p, err = protocol.DecodePack(b, protocol.Limits{})
		}
		if err == nil {
			err = e.recoverPack(ctx, p, now, early)
		}
		if err != nil {
			e.delayedAbandoned.Add(uint64(len(children) - i))
			return err
		}
	}
	return nil
}

// recoverPack is the recover rule for one delayed pack whose driving XLog
// did not decide: an XLog is stored unless discardAll, a profile only when
// discardNone, stamped with the release time (ProfileCore.add). early counts
// the outcome in Stats.DelayedRecoveredEarly (stored) or
// DelayedDiscardedEarly (dropped by the rule).
func (e *Engine) recoverPack(ctx context.Context, p protocol.Pack, now int64, early bool) error {
	store := false
	var err error
	switch x := p.(type) {
	case *protocol.XLogPack:
		if store = x.DiscardType != discardAll; store {
			err = e.storeXLog(ctx, x, now)
		}
	case *protocol.ProfilePack:
		x.Time = now
		if store = x.DiscardType == discardNone; store {
			err = e.putProfile(x)
		}
	}
	switch {
	case err != nil || !early:
	case store:
		e.delayedRecoveredEarly.Add(1)
	default:
		e.delayedDiscardedEarly.Add(1)
	}
	return err
}

func (e *Engine) storeXLog(ctx context.Context, p *protocol.XLogPack, now int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s := e.settings()
	if p.XType == 0 || p.XType == 1 {
		if err := e.processGroup(ctx, p, now); err != nil {
			return err
		}
	}
	b, err := protocol.EncodePack(p, protocol.Limits{})
	if err != nil {
		return err
	}
	if e.config.OnXLog != nil {
		e.config.OnXLog(p)
	}
	// The XLog writer stores it (XLogWR); a full queue drops it after a
	// bounded wait, counted, taken after the engine lock.
	w, err := e.store.PrepareXLogEncoded(p, b) // the ring's bytes, encoded once
	if err != nil {
		return err
	}
	if err = e.offerLocked(w, true, nil); err != nil {
		return err
	}
	// XLogCache.put: the realtime ring is a cache. Past the budget the stored
	// XLog is left out of the ring (cursor unchanged, Stats.XLogCacheSkipped):
	// Client realtime views never show it, history queries do.
	if e.cacheDelta(len(b)-len(e.ring[e.index])) == nil {
		e.ringBytes += len(b) - len(e.ring[e.index])
		e.ring[e.index] = b
		e.index++
		if int(e.index) >= len(e.ring) {
			e.index = 0
			e.loop++
		}
	} else {
		e.xlogCacheSkipped.Add(1)
	}
	e.xlogs.Add(1)
	obj := e.objects[p.ObjHash]
	if p.XType == 0 && p.UserID != 0 {
		// VisitorCore: the visit is counted for a live object only (Java
		// drops it silently; counted here). The offer, and the HLL load/save
		// it may need, run on the visitor writer in accept order, with the
		// receive time and object type fixed now.
		if obj == nil {
			e.visitorsSkipped.Add(1)
		} else {
			e.visits = append(e.visits, visit{now: now, objType: obj.ObjType, userID: p.UserID, objHash: p.ObjHash})
			if len(e.visits) >= visitBatch {
				e.flushVisitsLocked()
			}
		}
	}
	if obj == nil {
		return nil
	}
	if !s.TagCount {
		return nil // XLogCore: XLogTagCount.add only when tagcnt_enabled
	}
	tags, err := e.store.PrepareXLogTags(obj.ObjType, p)
	if err != nil {
		return err
	}
	return e.offerLocked(tags, true, nil)
}

// visitBatch is how many visits one visitor writer item carries. Queuing
// each visit cost a channel send and a writer wakeup per XLog (+4.8% server
// CPU at 30,000 XLog-only/s, VM pairs); a batch is queued when full, before
// each Tick's roll (so the roll keeps its place in the visit order), and by
// Flush and Close. Realtime visitor counts lag by at most one tick.
const visitBatch = 256

// visit is one VisitorCore offer, its time and object type fixed at accept.
type visit struct {
	now     int64
	objType string
	userID  int64
	objHash int32
}

// visitBatchError is the error of a visitor batch item that offered every
// visit (until ctx ended): invalid visits were refused on their content (a
// name or path the format cannot hold), the rest of the unwritten ones on
// I/O, the closed Store or ctx. A visit counts as written only if all its
// offers (objType, object, hourly) were; one refused offer makes it invalid
// if any of its errors is an input error, else failed, although its other
// offers were counted.
type visitBatchError struct {
	invalid               int
	invalidErr, failedErr error
}

func (e *visitBatchError) Error() string {
	return errors.Join(e.invalidErr, e.failedErr).Error()
}
func (e *visitBatchError) Unwrap() []error { return []error{e.invalidErr, e.failedErr} }

// flushVisitsLocked queues the pending visits as one visitor writer item.
// The caller holds e.mu.
func (e *Engine) flushVisitsLocked() {
	if len(e.visits) == 0 {
		return
	}
	batch := e.visits
	e.visits = make([]visit, 0, visitBatch)
	size := 0
	for _, v := range batch {
		size += 48 + len(v.objType)
	}
	e.w.enqueue(writeVisitor, len(batch), size, func(ctx context.Context) (int, error) {
		// A refused visit does not drop the rest of the batch; the writer
		// counts the invalid ones and the other unwritten ones apart.
		written, b := 0, &visitBatchError{}
		for _, v := range batch {
			err := e.store.PutVisitorContext(ctx, v.now, v.objType, v.userID, v.objHash)
			switch {
			case err == nil:
				written++
				continue
			case invalidRecord(err):
				b.invalid++
				if b.invalidErr == nil {
					b.invalidErr = err
				}
			case b.failedErr == nil:
				b.failedErr = err
			}
			if ctx.Err() != nil {
				break // the rest is not tried: counted as failed
			}
		}
		if written == len(batch) {
			return written, nil
		}
		if b.failedErr == nil && written+b.invalid < len(batch) {
			b.failedErr = ctx.Err()
		}
		return written, b
	})
}
