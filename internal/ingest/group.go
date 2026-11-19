// Scouter XLogGroupUtil/XLogGroupPerf/MeterService: Copyright 2015 Scouter
// authors, Apache-2.0. Independently implemented; see service-group.md.
package ingest

import (
	"container/list"
	"context"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/storage"
)

const (
	groupMeterMax   = 2000
	groupServiceMax = 50000
	// Conservative accounting for bucket arrays, maps, FIFO elements and snapshots.
	groupMeterBytes    = 301*16 + 256
	groupServiceBytes  = 96
	groupSnapshotBytes = 64
)

type groupKey struct{ objHash, group int32 }
type groupStat struct {
	count, error int32
	elapsed      int64
}
type groupMeter struct {
	buckets  [301]groupStat
	second   int64
	position int
	order    *list.Element
}
type groupState struct {
	meters              map[groupKey]*groupMeter
	order               list.List
	services            map[int32]int32
	serviceOrder        list.List
	cached              map[groupKey]groupStat
	empty               map[groupKey]struct{}
	lastBuild, nextSave int64
	seeded              bool
}

func newGroupState(now int64) *groupState {
	return &groupState{meters: make(map[groupKey]*groupMeter), services: make(map[int32]int32), cached: make(map[groupKey]groupStat), empty: make(map[groupKey]struct{}), lastBuild: now, nextSave: (now/60000 + 1) * 60000}
}

// Engine's mutex owns all group state; processGroup and the tick's group save are locked callers.
func (e *Engine) processGroup(ctx context.Context, p *protocol.XLogPack, now int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state := e.extra.group
	for key := range state.empty {
		e.removeGroupMeter(key)
	}
	clear(state.empty)
	if !state.seeded {
		for _, name := range []string{"*.jsp", "**", "images", "statics", "/**"} {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := e.putTextLocked(scoutertime.Date(now, e.config.Location), &protocol.TextPack{XType: "group", Hash: storage.CounterHash(name), Text: name}, nil); err != nil {
				return err
			}
		}
		state.seeded = true
	}
	if p.Group == 0 {
		if cached, ok := state.services[p.Service]; ok {
			p.Group = cached
		} else {
			var text, date string
			var found, known, asked bool
			if h := &e.groupHint; h.matches(p.Service, p.EndTime, e.javaDate) {
				// Looked up by lockForAccept for this XLog (a read error
				// fails it as the read here did).
				if h.err != nil {
					return h.err
				}
				date, text, found, known, asked = h.ref.date, h.text, h.found, !h.unanswered, true
			} else {
				date = scoutertime.Date(p.EndTime, e.config.Location)
				text, found, known = e.store.TextCached(date, "service", p.Service)
			}
			if !known {
				// The store did not answer in time (lockForAccept), or this
				// is an XLog delayed by sampling, released now, whose text
				// was read at its own accept but evicted from the storage
				// caches since. The files are not read under the engine
				// lock: the read is started for the next XLog of the service
				// and this one keeps no group.
				if !asked {
					var err error
					if text, found, known, err = e.store.TextWithin(ctx, date, "service", p.Service, 0); err != nil {
						return err
					}
				}
				if !known {
					e.serviceTextSkipped.Add(1)
				}
			}
			if found {
				name := groupName(text)
				hash := storage.CounterHash(name)
				// The original saved set is never populated: dynamic names are submitted
				// repeatedly; permanent dictionary storage preserves the first value.
				if name != "*.jsp" && name != "images" && name != "statics" && name != "/**" {
					if err := e.putTextLocked(date, &protocol.TextPack{XType: "group", Hash: hash, Text: name}, nil); err != nil {
						return err
					}
				}
				delta := groupServiceBytes
				if len(state.services) == groupServiceMax {
					delta = 0
				}
				if hash != 0 && e.cacheDelta(delta) != nil {
					// The service->group memo is a cache: the group is
					// still resolved for this XLog, the next one of the
					// service reads the text again.
					e.xlogCacheSkipped.Add(1)
				} else if hash != 0 {
					if len(state.services) == groupServiceMax {
						first := state.serviceOrder.Front()
						delete(state.services, first.Value.(int32))
						state.serviceOrder.Remove(first)
					}
					state.serviceOrder.PushBack(p.Service)
					state.services[p.Service] = hash
					e.cacheBytes += delta
				}
				p.Group = hash
			}
		}
	}
	// XLogCore.calc: GeoIpUtil.setNationAndCity between XLogGroupUtil.process
	// and XLogGroupPerf.add, whether or not a group was found.
	e.applyGeoIPLocked(p, scoutertime.Date(now, e.config.Location))
	if p.Group == 0 {
		return nil
	}
	key := groupKey{p.ObjHash, p.Group}
	meter := state.meters[key]
	if meter == nil {
		delta := groupMeterBytes
		if len(state.meters) == groupMeterMax {
			delta = 0
		}
		if e.cacheDelta(delta) != nil {
			// XLogGroupPerf is realtime/minute statistics: the XLog is still
			// stored (with its group), this meter misses it.
			e.xlogCacheSkipped.Add(1)
			return nil
		}
		if len(state.meters) == groupMeterMax {
			e.removeGroupMeter(state.order.Front().Value.(groupKey))
		}
		meter = &groupMeter{second: now / 1000, position: int(now / 1000 % 301)}
		meter.order = state.order.PushBack(key)
		state.meters[key] = meter
		e.cacheBytes += groupMeterBytes
	}
	meter.advance(now)
	bucket := &meter.buckets[meter.position]
	bucket.count++
	bucket.elapsed += int64(p.Elapsed)
	if p.Error != 0 {
		bucket.error++
	}
	return nil
}

// javaDate is the yyyymmdd of the group step (scoutertime.Date).
func (e *Engine) javaDate(ms int64) string { return scoutertime.Date(ms, e.config.Location) }

func groupName(url string) string {
	if dot := strings.LastIndexByte(url, '.'); dot > 0 {
		switch strings.ToLower(url[dot+1:]) {
		case "jsp":
			return "*.jsp"
		case "gif", "jpg", "png", "bmp", "ico":
			return "images"
		case "html", "htm", "css", "xml", "js":
			return "statics"
		}
	}
	if len(url) > 1 {
		if slash := strings.IndexByte(url[1:], '/'); slash >= 0 {
			return url[:slash+1]
		}
	}
	return "/**"
}
func (e *Engine) removeGroupMeter(key groupKey) {
	state := e.extra.group
	if meter := state.meters[key]; meter != nil {
		state.order.Remove(meter.order)
		delete(state.meters, key)
		e.cacheBytes -= groupMeterBytes
	}
}
func (m *groupMeter) advance(now int64) {
	second := now / 1000
	steps := second - m.second
	if steps > 301 {
		steps = 301
	}
	for i := int64(0); i < steps; i++ {
		m.position = (m.position + 1) % 301
		m.buckets[m.position] = groupStat{}
	}
	m.second = second
	m.position = int(second % 301)
}
func (m *groupMeter) stat(now int64, period int) groupStat {
	m.advance(now)
	var out groupStat
	pos := m.position
	for i := 0; i < period; i++ {
		b := m.buckets[pos]
		out.count += b.count
		out.error += b.error
		out.elapsed += b.elapsed
		pos = (pos + 300) % 301
	}
	return out
}

// groupSave is one minute's XLogGroupPerf save: the counter rows built under
// the engine lock and handed to queueGroupSave after it is released. At most
// groupMeterMax*3 rows.
type groupSave struct {
	date                        string
	minute                      int
	rows                        []groupRow
	prevNextSave, nextSaveAfter int64
}
type groupRow struct {
	key    groupKey
	marked bool // this save marked the meter empty (cnt row only)
	name   string
	value  int64
}

// takeGroupSaveLocked is the engine-locked half of the minute save: it reads
// the live rolling 300s sums (marking zero meters empty as before), builds the
// rows in meter order and advances nextSave. ok is false when no save is due.
func (e *Engine) takeGroupSaveLocked(now int64) (save groupSave, ok bool) {
	state := e.extra.group
	if now < state.nextSave {
		return save, false
	}
	// Save the live rolling 300s sum, with the original ten-second offset only
	// on the destination day/minute. Repeated writes overwrite the 5m slot.
	stamp := now - 10000
	save.date = scoutertime.Date(stamp, e.config.Location)
	save.minute = int(scoutertime.Millis(stamp, e.config.Location) / 60000)
	save.rows = make([]groupRow, 0, 3*len(state.meters))
	for item := state.order.Front(); item != nil; item = item.Next() {
		key := item.Value.(groupKey)
		stat := state.meters[key].stat(now, 300)
		marked := false
		if stat.count == 0 {
			if _, already := state.empty[key]; !already {
				state.empty[key] = struct{}{}
				marked = true
			}
		}
		suffix := strconv.FormatInt(int64(key.group), 10)
		save.rows = append(save.rows,
			groupRow{key, marked, "cnt:" + suffix, int64(stat.count)},
			groupRow{key, false, "tm:" + suffix, stat.elapsed},
			groupRow{key, false, "err:" + suffix, int64(stat.error)})
	}
	save.prevNextSave = state.nextSave
	save.nextSaveAfter = (now/60000 + 1) * 60000
	state.nextSave = save.nextSaveAfter
	return save, true
}

// queueGroupSave hands the rows to the daily counter writer as one item
// (DailyCounterWR, after any daily record queued before it), written in meter
// order without the engine lock. A write error ends the pass (counted and
// logged by the writer) and the save is retried on a later tick as before:
// nextSave goes back to its previous value (unless a later save already
// moved it) and the meters from the failed one on are no longer marked empty
// by this save (a marked meter that processGroup already removed in the
// meantime had a zero sum and is skipped by the retry). A full queue drops
// the whole save the same way (counted), so it is retried too.
func (e *Engine) queueGroupSave(save groupSave) {
	if len(save.rows) == 0 {
		return // no meter: nothing to write
	}
	size := 0
	for _, r := range save.rows {
		size += int(unsafe.Sizeof(r)) + len(r.name)
	}
	if e.w.enqueuePass(writeDaily, passGroupSave, len(save.rows), size, func(ctx context.Context) (int, error) {
		for i, r := range save.rows {
			if err := e.counters.PutDailyContext(ctx, save.date, r.key.objHash, r.name, 3, save.minute, protocol.Value{Type: protocol.ValueDecimal, Int: r.value}); err != nil {
				e.restoreGroupSave(save, save.rows[i/3*3:])
				return i, err
			}
		}
		return len(save.rows), nil
	}) != nil {
		e.restoreGroupSave(save, save.rows)
	}
}

func (e *Engine) restoreGroupSave(save groupSave, unwritten []groupRow) {
	e.mu.Lock()
	defer e.mu.Unlock()
	state := e.extra.group
	if state.nextSave == save.nextSaveAfter {
		state.nextSave = save.prevNextSave
	}
	for _, r := range unwritten {
		if r.marked {
			delete(state.empty, r.key)
		}
	}
}

// RealtimeServiceGroup returns an owned response ready for the Client handler.
// An empty requested object set has no response, as in GroupService.scala.
func (e *Engine) RealtimeServiceGroup(ctx context.Context, hashes []int32) (*protocol.MapPack, error) {
	if len(hashes) > 100000 {
		return nil, protocol.ErrLimit
	}
	if len(hashes) == 0 {
		return nil, nil
	}
	totals, groups, now, err := e.realtimeGroupTotals(ctx, hashes)
	if err != nil {
		return nil, err
	}
	// Group names are read without the engine lock: a Client query must not
	// hold ingest while it waits for the text store.
	names, counts, elapsed, errs := []protocol.Value{}, []protocol.Value{}, []protocol.Value{}, []protocol.Value{}
	for _, hash := range groups {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name, found, err := e.store.TextContext(ctx, scoutertime.Date(now, e.config.Location), "group", hash)
		if err != nil {
			return nil, err
		}
		if !found {
			name = "unknown"
		}
		stat := totals[hash]
		var avg float32
		if stat.count != 0 {
			avg = float32(stat.elapsed) / float32(stat.count)
		}
		names = append(names, protocol.Value{Type: protocol.ValueText, Text: name})
		counts = append(counts, protocol.Value{Type: protocol.ValueDecimal, Int: int64(stat.count)})
		elapsed = append(elapsed, protocol.Value{Type: protocol.ValueFloat, Float: avg})
		errs = append(errs, protocol.Value{Type: protocol.ValueDecimal, Int: int64(stat.error)})
	}
	return &protocol.MapPack{Entries: []protocol.Entry{{Key: "name", Value: protocol.Value{Type: protocol.ValueList, Values: names}}, {Key: "count", Value: protocol.Value{Type: protocol.ValueList, Values: counts}}, {Key: "elapsed", Value: protocol.Value{Type: protocol.ValueList, Values: elapsed}}, {Key: "error", Value: protocol.Value{Type: protocol.ValueList, Values: errs}}}}, nil
}

// realtimeGroupTotals sums the 30s group meters of the selected objects
// under the engine lock and returns the group hashes in order.
func (e *Engine) realtimeGroupTotals(ctx context.Context, hashes []int32) (map[int32]groupStat, []int32, int64, error) {
	if err := e.mu.LockContext(ctx); err != nil {
		return nil, nil, 0, err
	}
	defer e.mu.Unlock()
	if e.closed {
		return nil, nil, 0, storage.ErrClosed
	}
	state := e.extra.group
	now := e.config.Clock().UnixMilli()
	if now >= state.lastBuild+1000 {
		delta := (len(state.meters) - len(state.cached)) * groupSnapshotBytes
		if err := e.cacheDelta(delta); err != nil {
			return nil, nil, 0, err
		}
		snapshot := make(map[groupKey]groupStat, len(state.meters))
		for key, meter := range state.meters {
			if err := ctx.Err(); err != nil {
				return nil, nil, 0, err
			}
			snapshot[key] = meter.stat(now, 30)
		}
		state.cached = snapshot
		state.lastBuild = now
		e.cacheBytes += delta
	}
	selected := make(map[int32]struct{}, len(hashes))
	for _, hash := range hashes {
		selected[hash] = struct{}{}
	}
	totals := make(map[int32]groupStat)
	for key, stat := range state.cached {
		if err := ctx.Err(); err != nil {
			return nil, nil, 0, err
		}
		if _, ok := selected[key.objHash]; !ok {
			continue
		}
		total := totals[key.group]
		total.count += stat.count
		total.error += stat.error
		total.elapsed += stat.elapsed
		totals[key.group] = total
	}
	groups := make([]int32, 0, len(totals))
	for hash := range totals {
		groups = append(groups, hash)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i] < groups[j] })
	return totals, groups, now, nil
}
