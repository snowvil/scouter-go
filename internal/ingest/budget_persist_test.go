package ingest

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

// openPersistEngine opens an engine on root; the caller closes it with the
// returned function before reading root back with reopenStore.
func openPersistEngine(t *testing.T, root string, now *time.Time) (*Engine, func()) {
	t.Helper()
	s, err := storage.Open(storage.Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	c, err := storage.OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{Location: time.UTC, Clock: func() time.Time { return *now }, XLogCapacity: 16}, s, c)
	if err != nil {
		t.Fatal(err)
	}
	var closed bool
	closeAll := func() {
		if !closed {
			closed = true
			e.Close()
			c.Close()
			s.Close()
		}
	}
	t.Cleanup(closeAll)
	return e, closeAll
}

func reopenStore(t *testing.T, root string) *storage.Store {
	t.Helper()
	s, err := storage.Open(storage.Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// childXLog is a delayed child (BACK_THREAD, not driving) that the recover
// rule stores: discardNone.
func childXLog(txid, gxid int64, elapsed int32) *protocol.XLogPack {
	return &protocol.XLogPack{TxID: txid, GxID: gxid, Service: 1, ObjHash: 1, Elapsed: elapsed, XType: 2, DiscardType: discardNone, TailVersion: protocol.XLogCurrentTail}
}

// S2: with the cache budget filled by delayed sampling children, an
// AlertPack, a StatusPack and further delayed children are still persisted
// (found on disk after the store is closed and reopened). Only cache/ring
// updates are skipped, and the delay buffer recovers its oldest group by the
// original recover rule to make room.
func TestPersistedPastCacheBudget(t *testing.T) {
	now := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	root := t.TempDir()
	e, closeAll := openPersistEngine(t, root, &now)
	ctx := context.Background()
	const date = "20261024"
	// Fill the budget with delayed children of gxids 100..103.
	for g := int64(100); g < 104; g++ {
		if err := e.Accept(ctx, childXLog(g+1000, g, int32(g))); err != nil {
			t.Fatal(err)
		}
	}
	e.mu.Lock()
	e.config.MaxCacheBytes = e.cacheBytes + e.ringBytes // no room left
	e.mu.Unlock()

	alert := &protocol.AlertPack{Level: 2, ObjType: "java", ObjHash: 1, Title: "budget", Message: "past the cache budget", Tags: protocol.Value{Type: protocol.ValueMap}}
	if err := e.Accept(ctx, alert); err != nil {
		t.Error("alert rejected past the cache budget:", err)
	}
	status := &protocol.StatusPack{ObjType: "java", ObjHash: 1, Key: "budget", Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "v", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 7}}}}}
	if err := e.Accept(ctx, status); err != nil {
		t.Error("status rejected past the cache budget:", err)
	}
	// A new delayed child (gxid 200) and its delayed profile, then the
	// driving XLog that releases them (discardNone: stored).
	if err := e.Accept(ctx, childXLog(201, 200, 201)); err != nil {
		t.Error("delayed child XLog rejected past the cache budget:", err)
	}
	profile := &protocol.ProfilePack{TxID: 201, GxID: 200, ObjHash: 1, Profile: []byte{9, 8, 7}, Version2: true, XType: 2, DiscardType: discardNone}
	if err := e.Accept(ctx, profile); err != nil {
		t.Error("delayed child profile rejected past the cache budget:", err)
	}
	driving := &protocol.XLogPack{TxID: 200, GxID: 200, Service: 1, ObjHash: 1, Elapsed: 200, DiscardType: discardNone, TailVersion: protocol.XLogCurrentTail}
	if err := e.Accept(ctx, driving); err != nil {
		t.Fatal(err)
	}
	// Alert: summary and ring each skipped. Delayed: at least the oldest
	// XLog group and the profile (no profile group to recover).
	if st := e.Stats(); st.Rejected != 0 || st.AlertCacheSkipped != 2 || st.StatusCacheSkipped != 1 || st.DelayedRecoveredEarly < 2 {
		t.Errorf("past the cache budget: %+v", st)
	}
	if got := realtimeAlerts(t, e); len(got) != 0 {
		t.Errorf("alert ring took an alert past the budget: %d", len(got))
	}
	closeAll()

	s := reopenStore(t, root)
	for _, txid := range []int64{200, 201, 1100} { // 1100: the oldest group, recovered for room
		x, err := s.XLog(date, txid)
		if err != nil || x == nil {
			t.Error("XLog not on disk", txid, x, err)
		}
	}
	if b, err := s.Profile(date, 201); err != nil || len(b) == 0 {
		t.Error("delayed profile not on disk", b, err)
	}
	from, to := now.UnixMilli()-1000, now.UnixMilli()+1000
	alerts, err := s.AlertsContext(ctx, date, from, to)
	if err != nil || len(alerts) != 1 || alerts[0].Title != "budget" {
		t.Error("alert not on disk", alerts, err)
	}
	statuses, err := s.StatusesContext(ctx, date, from, to)
	if err != nil || len(statuses) != 1 || statuses[0].Key != "budget" {
		t.Error("status not on disk", statuses, err)
	}
}

// delayPack past the cache budget: the oldest delayed groups of the pack's
// kind take the recover rule early (XLog stored unless discardAll, profile
// stored when discardNone) until the pack fits; when recovering them all
// could not make the room the pack itself takes the rule and the groups
// keep waiting. The other kind is not touched.
func TestDelayPackRecoversOldestPastBudget(t *testing.T) {
	now := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	e, s := testEngineWith(t, &now, nil)
	ctx := context.Background()
	const date = "20261024"
	// Fill the realtime ring (16 slots) with larger XLogs so that storing a
	// recovered XLog takes no budget (its ring slot shrinks).
	for i := int64(1); i <= 16; i++ {
		x := &protocol.XLogPack{TxID: 100 + i, Service: 1, ObjHash: 1, Elapsed: 1 << 30, TailVersion: protocol.XLogCurrentTail}
		if err := e.Accept(ctx, x); err != nil {
			t.Fatal(err)
		}
	}
	for g := int64(1); g <= 3; g++ {
		if err := e.Accept(ctx, childXLog(g+1000, g, 1)); err != nil {
			t.Fatal(err)
		}
	}
	keep := &protocol.ProfilePack{TxID: 1009, GxID: 9, ObjHash: 1, Profile: []byte{1}, Version2: true, XType: 2, DiscardType: discardNone}
	if err := e.Accept(ctx, keep); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.config.MaxCacheBytes = e.cacheBytes + e.ringBytes
	e.mu.Unlock()
	// Same size as group 1: recovering group 1 (stored) makes exactly the room.
	if err := e.Accept(ctx, childXLog(1004, 4, 1)); err != nil {
		t.Fatal(err)
	}
	state := e.extra.sampling
	delayed := func() (xlogs []int64, bytes int) {
		for _, b := range []*delayingBucket{state.xlogOld, state.xlogCurrent} {
			for el := b.order.Front(); el != nil; el = el.Next() {
				xlogs = append(xlogs, el.Value.(*delayedTrace).gxid)
				bytes += delayedSize(el.Value.(*delayedTrace))
			}
		}
		return xlogs, bytes
	}
	if got, _ := delayed(); len(got) != 3 || got[0] != 2 || got[2] != 4 || e.Stats().DelayedRecoveredEarly != 1 {
		t.Fatalf("delayed %v, stats %+v", got, e.Stats())
	}
	// Room for one group after recovering all three (groups 2..4, all the
	// size of the new pack): they are recovered (stored) one by one and the
	// discardAll pack waits in their place.
	_, xlogBytes := delayed()
	e.mu.Lock()
	e.config.MaxCacheBytes = e.cacheBytes + e.ringBytes - xlogBytes*2/3
	e.mu.Unlock()
	gone := childXLog(1005, 5, 1)
	gone.DiscardType = discardAll
	if err := e.Accept(ctx, gone); err != nil {
		t.Fatal(err)
	}
	if got, _ := delayed(); len(got) != 1 || got[0] != 5 || e.Stats().DelayedRecoveredEarly != 4 || e.Stats().DelayedDiscardedEarly != 0 {
		t.Fatalf("delayed %v, stats %+v", got, e.Stats())
	}
	// No room even with every XLog group recovered: the pack takes the rule
	// at once (stored) and group 5 keeps waiting.
	e.mu.Lock()
	e.config.MaxCacheBytes = e.cacheBytes + e.ringBytes - 1
	e.mu.Unlock()
	if err := e.Accept(ctx, childXLog(1006, 6, 1)); err != nil {
		t.Fatal(err)
	}
	if got, _ := delayed(); len(got) != 1 || got[0] != 5 || e.Stats().DelayedRecoveredEarly != 5 {
		t.Fatalf("delayed %v, stats %+v", got, e.Stats())
	}
	if state.profileCurrent.order.Len() != 1 {
		t.Fatal("an XLog recovered a delayed profile")
	}
	// A profile of group 9's size with no room: group 9 is recovered
	// (discardNone: stored) and the discardAll profile waits.
	e.mu.Lock()
	e.config.MaxCacheBytes = e.cacheBytes + e.ringBytes
	e.mu.Unlock()
	drop := &protocol.ProfilePack{TxID: 1010, GxID: 10, ObjHash: 1, Profile: []byte{2}, Version2: true, XType: 2, DiscardType: discardAll}
	if err := e.Accept(ctx, drop); err != nil {
		t.Fatal(err)
	}
	front := state.profileCurrent.order.Front()
	if st := e.Stats(); state.profileCurrent.order.Len() != 1 || front.Value.(*delayedTrace).gxid != 10 || st.DelayedRecoveredEarly != 6 || st.DelayedDiscardedEarly != 0 || st.DelayedAbandoned != 0 || st.Rejected != 0 {
		t.Fatalf("profile groups %d, stats %+v", state.profileCurrent.order.Len(), st)
	}
	flush(t, e)
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	for txid := int64(1001); txid <= 1006; txid++ {
		x, err := s.XLog(date, txid)
		if stored := x != nil; err != nil || stored != (txid != 1005) {
			t.Error("XLog", txid, "stored", stored, err)
		}
	}
	for txid, want := range map[int64]bool{1009: true, 1010: false} {
		b, err := s.Profile(date, txid)
		if err != nil || (len(b) > 0) != want {
			t.Error("profile", txid, len(b), err)
		}
	}
}

// Status past MaxCounters or the byte budget: stored, the cache skipped (a
// new key is not cached, an existing key is dropped). Alert
// summary past MaxCounters: the alert is stored and in the ring, the summary
// does not count it.
func TestStatusAndAlertCacheSkippedStillStored(t *testing.T) {
	now := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	e, s := testEngineWith(t, &now, func(c *Config) { c.MaxCounters = 1 })
	ctx := context.Background()
	status := func(key string, v int64) *protocol.StatusPack {
		return &protocol.StatusPack{ObjType: "java", ObjHash: 1, Key: key, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "v", Value: protocol.Value{Type: protocol.ValueDecimal, Int: v}}}}}
	}
	for _, p := range []*protocol.StatusPack{status("a", 1), status("b", 2)} {
		if err := e.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	if p, err := e.Status(ctx, 1, "b"); p != nil || err != nil {
		t.Fatal("status cached past MaxCounters", p, err)
	}
	e.mu.Lock()
	e.config.MaxCacheBytes = e.cacheBytes + e.ringBytes
	e.mu.Unlock()
	if err := e.Accept(ctx, status("a", 1<<40)); err != nil { // a larger value
		t.Fatal(err)
	}
	// The skipped update of an existing key drops it: no stale value.
	if p, err := e.Status(ctx, 1, "a"); err != nil || p != nil {
		t.Fatal("skipped status update left the previous value", p, err)
	}
	e.mu.Lock()
	e.config.MaxCacheBytes = DefaultMaxCacheBytes
	e.mu.Unlock()
	// With room again the dropped key is inserted anew.
	if err := e.Accept(ctx, status("a", 3)); err != nil {
		t.Fatal(err)
	}
	if p, err := e.Status(ctx, 1, "a"); err != nil || p == nil || p.Data.Entries[0].Value.Int != 3 {
		t.Fatal("status not reinserted", p, err)
	}
	for _, title := range []string{"one", "two"} {
		a := &protocol.AlertPack{Level: 1, ObjType: "java", ObjHash: 1, Title: title, Message: "m", Tags: protocol.Value{Type: protocol.ValueMap}}
		if err := e.Accept(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	if got := realtimeAlerts(t, e); len(got) != 2 {
		t.Fatal("alert ring", len(got))
	}
	e.mu.Lock()
	summary := e.extra.alertSummaries[1]
	titles := len(summary.counts)
	e.mu.Unlock()
	if titles != 1 {
		t.Fatal("summary titles past MaxCounters", titles)
	}
	if st := e.Stats(); st.StatusCacheSkipped != 2 || st.AlertCacheSkipped != 1 || st.Rejected != 0 {
		t.Fatalf("%+v", st)
	}
	flush(t, e)
	from, to := now.UnixMilli()-1000, now.UnixMilli()+1000
	statuses, err := s.StatusesContext(ctx, "20261024", from, to)
	if err != nil || len(statuses) != 4 {
		t.Fatal("statuses stored", len(statuses), err)
	}
	alerts, err := s.AlertsContext(ctx, "20261024", from, to)
	if err != nil || len(alerts) != 2 {
		t.Fatal("alerts stored", len(alerts), err)
	}
}

// The object map and the kube pod seq state count only their own bytes
// against MaxCacheBytes (coreDelta): a full cache does not refuse a new
// object, a changed heartbeat or a new pod seq, and the object is stored.
// Their own bytes past the budget still refuse them.
func TestObjectsAndKubeIgnoreFullCache(t *testing.T) {
	now := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	e, s := testEngineWith(t, &now, nil)
	ctx := context.Background()
	if err := e.Accept(ctx, object("/a", "tomcat")); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, &protocol.StatusPack{ObjType: "tomcat", ObjHash: 1, Key: "filler", Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "f", Value: protocol.Value{Type: protocol.ValueText, Text: strings.Repeat("f", 4096)}}}}}); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.config.MaxCacheBytes = e.cacheBytes + e.ringBytes // the cache is full
	core := e.coreBytes
	e.mu.Unlock()
	tag := protocol.Entry{Key: "k", Value: protocol.Value{Type: protocol.ValueText, Text: "a longer tag value"}}
	for _, p := range []*protocol.ObjectPack{
		object("/a", "tomcat", tag), // unchanged heartbeat, larger tags
		object("/b", "tomcat"),      // new object
		kubePack("/k", kubeTags{pod: "pod", host: "h", useKube: true, seq: 3}), // new pod seq
	} {
		if err := e.Accept(ctx, p); err != nil {
			t.Fatalf("%s refused by a full cache: %v", p.ObjName, err)
		}
	}
	e.mu.Lock()
	grown := e.coreBytes - core
	e.mu.Unlock()
	if grown <= 0 || e.kube.managers["pod"] == nil || e.Stats().KubeLimited != 0 {
		t.Fatalf("core bytes %d, stats %+v", grown, e.Stats())
	}
	flush(t, e)
	stored, err := s.Objects("20261024")
	found := map[string]bool{}
	for _, p := range stored {
		found[p.ObjName] = true
	}
	if err != nil || !found["/b"] || !found["/k"] {
		t.Fatal("objects not stored past a full cache", found, err)
	}
	// Their own bytes past the budget refuse them.
	e.mu.Lock()
	e.config.MaxCacheBytes = e.coreBytes
	e.mu.Unlock()
	if err := e.Accept(ctx, object("/c", "tomcat")); !errors.Is(err, protocol.ErrLimit) {
		t.Fatal("object past its own bytes:", err)
	}
	if err := e.Accept(ctx, kubePack("/k2", kubeTags{pod: "pod2", host: "h2", useKube: true, seq: 1})); !errors.Is(err, protocol.ErrLimit) || e.Stats().KubeLimited != 1 {
		t.Fatal("kube past its own bytes:", err, e.Stats().KubeLimited)
	}
}

// coreBytes follows the object map and the kube state: registering and then
// removing them returns it to where it was.
func TestCoreBytesReturnAfterRemoval(t *testing.T) {
	now := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, nil)
	ctx := context.Background()
	e.mu.Lock()
	base, cache := e.coreBytes, e.cacheBytes
	e.mu.Unlock()
	for _, p := range []*protocol.ObjectPack{object("/a", "tomcat"), kubePack("/k", kubeTags{pod: "pod", host: "h", useKube: true, seq: 3})} {
		if err := e.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(time.Minute)
	if err := e.Tick(ctx); err != nil { // both inactive: the kube seq is released
		t.Fatal(err)
	}
	if err := e.RemoveInactiveObjects(ctx); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	core, total := e.coreBytes, e.cacheBytes
	e.mu.Unlock()
	if core != base || e.ObjectCount() != 0 {
		t.Fatalf("core bytes %d -> %d (cache %d -> %d), objects %d", base, core, cache, total, e.ObjectCount())
	}
}

// kubeBytes recomputes the accounted bytes of the kube state.
func kubeBytes(k *kubeState) int {
	n := 0
	for pod, m := range k.managers {
		n += len(pod) + kubeManagerBytes
		for host := range m.hostSeq {
			n += len(host) + kubeEntryBytes
		}
		for _, host := range m.seqHost {
			n += len(host) + kubeEntryBytes
		}
	}
	for el := k.initialOrder.Front(); el != nil; el = el.Next() {
		v := el.Value.(*kubeInitial)
		n += len(v.host) + len(v.podName) + kubeInitialBytes
	}
	return n
}

// coreBytes stays the sum of the object sizes and the kube state over a
// random mix of registrations, changed and saved heartbeats, kube pod seqs,
// inactivity, evictions at MaxObjects, removals and a date rollover.
func TestCoreBytesSymmetric(t *testing.T) {
	now := time.Date(2026, 10, 24, 23, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.MaxObjects = 6; c.MaxCounters = 12 })
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(1, 2))
	check := func(step int, op string) {
		t.Helper()
		e.mu.Lock()
		defer e.mu.Unlock()
		want := kubeBytes(e.kube)
		for hash, n := range e.objectSizes {
			if e.objects[hash] == nil {
				t.Fatalf("step %d %s: size of a removed object %d", step, op, hash)
			}
			want += n
		}
		if len(e.objectSizes) != len(e.objects) || e.coreBytes != want || e.cacheBytes < e.coreBytes {
			t.Fatalf("step %d %s: core %d want %d (cache %d, %d objects, %d sizes)", step, op, e.coreBytes, want, e.cacheBytes, len(e.objects), len(e.objectSizes))
		}
	}
	for step := 0; step < 3000; step++ {
		var op string
		switch r := rng.IntN(100); {
		case r < 40:
			op = "object"
			tags := []protocol.Entry{{Key: "k", Value: protocol.Value{Type: protocol.ValueText, Text: strings.Repeat("t", rng.IntN(40))}}}
			p := object(fmt.Sprint("/o", rng.IntN(10)), []string{"tomcat", "batch", "java"}[rng.IntN(3)], tags...)
			p.Version = fmt.Sprint(rng.IntN(2))
			_ = e.Accept(ctx, p)
		case r < 65:
			op = "kube"
			_ = e.Accept(ctx, kubePack(fmt.Sprint("/k", rng.IntN(8)), kubeTags{pod: fmt.Sprint("pod", rng.IntN(3)), host: fmt.Sprint("host", rng.IntN(6)), useKube: true, seq: int64(rng.IntN(5)) - 1}))
		case r < 85:
			op = "tick"
			now = now.Add(time.Duration(rng.IntN(20)) * time.Second)
			if err := e.Tick(ctx); err != nil {
				t.Fatal(err)
			}
		case r < 92:
			op = "remove"
			if err := e.RemoveObjects(ctx, []int32{storage.CounterHash(fmt.Sprint("/o", rng.IntN(10)))}, false); err != nil {
				t.Fatal(err)
			}
		default:
			op = "removeInactive"
			if err := e.RemoveInactiveObjects(ctx); err != nil {
				t.Fatal(err)
			}
		}
		check(step, op)
	}
	st := e.Stats()
	if st.ObjectsEvicted+st.ObjectsEvictedSilent == 0 || st.KubeLimited == 0 {
		t.Fatalf("the mix did not reach the bounds: %+v", st)
	}
	if storage.JavaDate(now.UnixMilli(), time.UTC) == "20261024" {
		t.Fatal("no date rollover")
	}
}

// cacheMismatch recomputes cacheBytes, ringBytes and the delayed bytes of
// each kind from the engine state (caller holds e.mu) and describes the
// first difference ("" when they match). Counters and interactions are not
// part of the callers' mixes.
func cacheMismatch(e *Engine) string {
	if len(e.current) != 0 || len(e.samples) != 0 || len(e.counterNames) != 0 || e.extra.interactionRows != 0 {
		return "counters or interactions in the mix"
	}
	cache := kubeBytes(e.kube)
	for _, n := range e.objectSizes {
		cache += n
	}
	cache += 40 * e.extra.sampling.processed.len()
	state := e.extra.sampling
	kinds := map[bool]int{}
	for _, b := range append([]*delayingBucket{state.xlogOld, state.xlogCurrent, state.profileCurrent}, state.profileOld...) {
		for el := b.order.Front(); el != nil; el = el.Next() {
			kinds[b.profile] += delayedSize(el.Value.(*delayedTrace))
		}
	}
	cache += kinds[false] + kinds[true]
	for key, entry := range e.extra.statuses {
		cache += len(entry.data) + len(key.key) + 16
	}
	for _, summary := range e.extra.alertSummaries {
		cache += len(summary.objType) + 16
		for title := range summary.counts {
			cache += len(title) + 16
		}
	}
	for _, b := range e.extra.alerts {
		cache += len(b)
	}
	g := e.extra.group
	cache += len(g.services)*groupServiceBytes + len(g.meters)*groupMeterBytes + len(g.cached)*groupSnapshotBytes
	ring := 0
	for _, b := range e.ring {
		ring += len(b)
	}
	if cache != e.cacheBytes || ring != e.ringBytes || kinds[false] != state.xlogBytes || kinds[true] != state.profileBytes {
		return fmt.Sprintf("cache %d want %d, ring %d want %d, delayed xlog %d want %d, profile %d want %d", e.cacheBytes, cache, e.ringBytes, ring, state.xlogBytes, kinds[false], state.profileBytes, kinds[true])
	}
	return ""
}

// cacheBytes and ringBytes stay the recomputed sums over a random mix of
// delayed children (count-bound eviction, rolls, early recovery under a
// moving budget), driving XLogs that release them, dropped drivings,
// statuses, alerts (summary, ring, profile recovery for the ring) and ticks.
func TestCacheBytesSymmetric(t *testing.T) {
	now := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.MaxCounters = 8; c.SamplingGxidKeepCount = 20 })
	s := DefaultSettings()
	s.XLogKeepCount, s.XLogKeepMillis, s.ProfileKeepCount, s.ProfileKeepSecs = 3, 1000, 3, 1
	e.SetSettings(s)
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(3, 4))
	discards := []byte{discardNone, discardAll, 3}
	for step := 0; step < 4000; step++ {
		gxid := int64(1 + rng.IntN(12))
		var p protocol.Pack
		var op string
		switch r := rng.IntN(100); {
		case r < 25:
			op = "child"
			p = &protocol.XLogPack{TxID: 1000 + int64(step), GxID: gxid, Service: 1, ObjHash: 1, Elapsed: int32(rng.IntN(1 << 20)), XType: 2, DiscardType: discards[rng.IntN(3)], TailVersion: protocol.XLogCurrentTail}
		case r < 45:
			op = "profile"
			p = &protocol.ProfilePack{TxID: 1000 + int64(step), GxID: gxid, ObjHash: 1, Profile: make([]byte, 1+rng.IntN(200)), Version2: true, XType: 2, DiscardType: discards[rng.IntN(3)]}
		case r < 55:
			op = "driving"
			p = &protocol.XLogPack{TxID: gxid, GxID: gxid, Service: 1, ObjHash: 1, Elapsed: 1, XType: 2, DiscardType: discards[rng.IntN(3)], TailVersion: protocol.XLogCurrentTail}
		case r < 60:
			op = "dropped"
			p = &protocol.DroppedXLogPack{TxID: gxid, GxID: gxid}
		case r < 72:
			op = "status"
			p = &protocol.StatusPack{ObjType: "java", ObjHash: int32(rng.IntN(3)), Key: fmt.Sprint("k", rng.IntN(12)), Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "v", Value: protocol.Value{Type: protocol.ValueText, Text: strings.Repeat("v", rng.IntN(100))}}}}}
		case r < 84:
			op = "alert"
			p = &protocol.AlertPack{Level: 1, ObjType: "java", ObjHash: int32(rng.IntN(12)), Title: fmt.Sprint("t", rng.IntN(12)), Message: strings.Repeat("m", rng.IntN(100)), Tags: protocol.Value{Type: protocol.ValueMap}}
		case r < 92:
			op = "time"
			now = now.Add(time.Duration(rng.IntN(1500)) * time.Millisecond)
			if rng.IntN(10) == 0 {
				now = now.Add(5 * time.Minute) // alert summary flush, status expiry
			}
			if err := e.Tick(ctx); err != nil {
				t.Fatal(err)
			}
		default:
			op = "budget"
			e.mu.Lock()
			e.config.MaxCacheBytes = max(1, e.cacheBytes+e.ringBytes+rng.IntN(4000)-3000)
			e.mu.Unlock()
		}
		if p != nil {
			if err := e.Accept(ctx, p); err != nil {
				t.Fatalf("step %d %s: %v", step, op, err)
			}
		}
		e.mu.Lock()
		if m := cacheMismatch(e); m != "" {
			e.mu.Unlock()
			t.Fatalf("step %d %s: %s", step, op, m)
		}
		e.mu.Unlock()
	}
	st := e.Stats()
	if st.DelayedRecoveredEarly == 0 || st.DelayedDiscardedEarly == 0 || st.AlertCacheSkipped == 0 || st.StatusCacheSkipped == 0 || st.DelayedAbandoned != 0 || st.Rejected != 0 {
		t.Fatalf("the mix did not reach the paths: %+v", st)
	}
}

// A child recovered early (stored) is not stored again when its driving XLog
// arrives afterwards, whatever the driving's discard: one record per txid.
func TestEarlyRecoveredChildStoredOnce(t *testing.T) {
	for _, discard := range []byte{discardNone, discardAll} {
		now := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
		e, s := testEngineWith(t, &now, nil)
		ctx := context.Background()
		for g := int64(1); g <= 2; g++ {
			if err := e.Accept(ctx, childXLog(100+g, g, 1)); err != nil {
				t.Fatal(err)
			}
		}
		e.mu.Lock()
		e.config.MaxCacheBytes = e.cacheBytes + e.ringBytes
		e.mu.Unlock()
		if err := e.Accept(ctx, childXLog(103, 3, 1)); err != nil { // recovers group 1 (txid 101)
			t.Fatal(err)
		}
		e.mu.Lock()
		e.config.MaxCacheBytes = DefaultMaxCacheBytes
		e.mu.Unlock()
		before := e.Stats().XLogs
		driving := &protocol.XLogPack{TxID: 1, GxID: 1, Service: 1, ObjHash: 1, Elapsed: 1, XType: 2, DiscardType: discard, TailVersion: protocol.XLogCurrentTail}
		if err := e.Accept(ctx, driving); err != nil {
			t.Fatal(err)
		}
		if got := e.Stats().XLogs - before; got != 1 { // the driving only
			t.Fatalf("discard %d: XLogs +%d", discard, got)
		}
		flush(t, e)
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		all, err := s.XLogsByTimeContext(ctx, "20261024", now.UnixMilli()-1000, now.UnixMilli()+1000)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, x := range all {
			if x.TxID == 101 {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("discard %d: txid 101 stored %d times", discard, count)
		}
	}
}

// A recovery that stops on a canceled ctx after its first child counts that
// child (stored) and the rest as abandoned; the group is out of its bucket
// and the accounting stays consistent.
func TestRecoverGroupCanceledMidway(t *testing.T) {
	now := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e, _ := testEngineWith(t, &now, func(c *Config) {
		c.OnXLog = func(x *protocol.XLogPack) {
			if x.TxID == 101 {
				cancel()
			}
		}
	})
	for txid := int64(101); txid <= 103; txid++ {
		if err := e.Accept(context.Background(), childXLog(txid, 1, 1)); err != nil {
			t.Fatal(err)
		}
	}
	e.mu.Lock()
	recovered, err := e.recoverOldestDelayed(ctx, false, now.UnixMilli())
	left := e.extra.sampling.xlogCurrent.order.Len()
	consistent := cacheMismatch(e) == ""
	e.mu.Unlock()
	if !recovered || !errors.Is(err, context.Canceled) || left != 0 || !consistent {
		t.Fatal(recovered, err, left, consistent)
	}
	if st := e.Stats(); st.DelayedRecoveredEarly != 1 || st.DelayedAbandoned != 2 || st.DelayedDiscardedEarly != 0 {
		t.Fatalf("%+v", st)
	}
}

// With no room at all (the budget below what is already accounted, e.g. the
// object map over it), a delayed pack takes the rule at once: recovering
// every group of its kind could not make the room, so they keep waiting. An
// alert skips the ring (and a new summary entry) without recovering delayed
// profiles; an update that does not grow the cache still passes.
func TestDelayPackNegativeRoom(t *testing.T) {
	now := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	e, s := testEngineWith(t, &now, nil)
	ctx := context.Background()
	for g := int64(1); g <= 3; g++ {
		if err := e.Accept(ctx, childXLog(100+g, g, 1)); err != nil {
			t.Fatal(err)
		}
	}
	keep := &protocol.ProfilePack{TxID: 109, GxID: 9, ObjHash: 1, Profile: []byte{1}, Version2: true, XType: 2, DiscardType: discardNone}
	if err := e.Accept(ctx, keep); err != nil {
		t.Fatal(err)
	}
	status := &protocol.StatusPack{ObjType: "java", ObjHash: 1, Key: "k", Data: protocol.Value{Type: protocol.ValueMap}}
	if err := e.Accept(ctx, status); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.config.MaxCacheBytes = 1 // far below cacheBytes
	e.mu.Unlock()
	if err := e.Accept(ctx, childXLog(104, 4, 1)); err != nil {
		t.Fatal(err)
	}
	a := &protocol.AlertPack{Level: 1, ObjType: "java", ObjHash: 1, Title: "t", Message: "m", Tags: protocol.Value{Type: protocol.ValueMap}}
	if err := e.Accept(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, status); err != nil { // same size: still cached
		t.Fatal(err)
	}
	e.mu.Lock()
	xlogs, profiles := e.extra.sampling.xlogCurrent.order.Len()+e.extra.sampling.xlogOld.order.Len(), e.extra.sampling.profileCurrent.order.Len()
	consistent := cacheMismatch(e) == ""
	e.mu.Unlock()
	if xlogs != 3 || profiles != 1 || !consistent {
		t.Fatal(xlogs, profiles, consistent)
	}
	if got := realtimeAlerts(t, e); len(got) != 0 {
		t.Fatal("alert ring past no room", len(got))
	}
	if p, err := e.Status(ctx, 1, "k"); p == nil || err != nil {
		t.Fatal("a same-size status update was skipped", err)
	}
	if st := e.Stats(); st.DelayedRecoveredEarly != 1 || st.DelayedDiscardedEarly != 0 || st.AlertCacheSkipped != 2 || st.StatusCacheSkipped != 0 || st.Rejected != 0 {
		t.Fatalf("%+v", st)
	}
	flush(t, e)
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	for txid := int64(101); txid <= 104; txid++ {
		if x, err := s.XLog("20261024", txid); err != nil || (x != nil) != (txid == 104) {
			t.Fatal("XLog", txid, x != nil, err)
		}
	}
	if b, err := s.Profile("20261024", 109); err != nil || len(b) != 0 {
		t.Fatal("a waiting profile was recovered", len(b), err)
	}
}

// The realtime alert ring makes room by recovering delayed profile groups
// early: a budget filled by delayed profiles does not hide alerts.
func TestAlertRingRecoversDelayedProfiles(t *testing.T) {
	now := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	e, s := testEngineWith(t, &now, nil)
	ctx := context.Background()
	for g := int64(1); g <= 3; g++ {
		p := &protocol.ProfilePack{TxID: 100 + g, GxID: g, ObjHash: 1, Profile: make([]byte, 512), Version2: true, XType: 2, DiscardType: discardNone}
		if err := e.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	e.mu.Lock()
	e.config.MaxCacheBytes = e.cacheBytes + e.ringBytes
	e.mu.Unlock()
	a := &protocol.AlertPack{Level: 2, ObjType: "java", ObjHash: 1, Title: "popup", Message: "m", Tags: protocol.Value{Type: protocol.ValueMap}}
	if err := e.Accept(ctx, a); err != nil {
		t.Fatal(err)
	}
	if got := realtimeAlerts(t, e); len(got) != 1 || got[0].Title != "popup" {
		t.Fatalf("alert ring: %d", len(got))
	}
	// One profile group (512 B) makes room for the ring; the summary, which
	// does not recover, is skipped.
	if st := e.Stats(); st.DelayedRecoveredEarly != 1 || st.AlertCacheSkipped != 1 || e.extra.sampling.profileCurrent.order.Len() != 2 {
		t.Fatalf("%+v", st)
	}
	flush(t, e)
	if b, err := s.Profile("20261024", 101); err != nil || len(b) == 0 {
		t.Fatal("recovered profile not stored", err)
	}
}

// When recovering every delayed profile could not make room for the alert
// ring, none is recovered: the ring is skipped at once (one count) and the
// profiles keep waiting. The summary of a known title does not grow and
// still counts the alert.
func TestAlertRingNoRoomRecoversNothing(t *testing.T) {
	now := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, nil)
	ctx := context.Background()
	alert := func(message string) *protocol.AlertPack {
		return &protocol.AlertPack{Level: 1, ObjType: "java", ObjHash: 1, Title: "t", Message: message, Tags: protocol.Value{Type: protocol.ValueMap}}
	}
	if err := e.Accept(ctx, alert("m")); err != nil {
		t.Fatal(err)
	}
	keep := &protocol.ProfilePack{TxID: 101, GxID: 1, ObjHash: 1, Profile: []byte{1}, Version2: true, XType: 2, DiscardType: discardNone}
	if err := e.Accept(ctx, keep); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.config.MaxCacheBytes = e.cacheBytes + e.ringBytes - 1
	profileBytes := e.extra.sampling.profileBytes
	e.mu.Unlock()
	if err := e.Accept(ctx, alert(strings.Repeat("m", 2*profileBytes))); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	count := e.extra.alertSummaries[1].counts["t"].count
	groups := e.extra.sampling.profileCurrent.order.Len()
	mismatch := cacheMismatch(e)
	e.mu.Unlock()
	if st := e.Stats(); st.AlertCacheSkipped != 1 || st.DelayedRecoveredEarly != 0 || st.DelayedDiscardedEarly != 0 || groups != 1 || count != 2 || mismatch != "" {
		t.Fatalf("groups %d, summary %d, %s, stats %+v", groups, count, mismatch, st)
	}
	if got := realtimeAlerts(t, e); len(got) != 1 {
		t.Fatal("ring", len(got))
	}
}

// Object alerts raised by Tick (INACTIVE_OBJECT), by a heartbeat
// (ACTIVATED_OBJECT) and on the 20th save (MAYBE_OBJ_DUP, ring only) make
// room in the alert ring by recovering delayed profiles: Tick returns, the
// ring has the alerts, the profiles are stored, the accounting holds.
func TestObjectAlertsRecoverDelayedProfiles(t *testing.T) {
	for _, queue := range []int{0, 1} { // 1: a profile queue that saturates
		now := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
		root := t.TempDir()
		s, err := storage.Open(storage.Config{Directory: root, Location: time.UTC, ProfileQueueSize: queue})
		if err != nil {
			t.Fatal(err)
		}
		c, err := storage.OpenCounters(root, time.UTC)
		if err != nil {
			t.Fatal(err)
		}
		e, err := New(Config{Location: time.UTC, Clock: func() time.Time { return now }, XLogCapacity: 16, ObjectType: alertTypes}, s, c)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		within := func(what string, f func() error) {
			t.Helper()
			done := make(chan error, 1)
			go func() { done <- f() }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(what, err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal(what, "did not return")
			}
		}
		noRoom := func() {
			e.mu.Lock()
			e.config.MaxCacheBytes = e.cacheBytes + e.ringBytes
			e.mu.Unlock()
		}
		within("register", func() error { return e.Accept(ctx, object("/a", "tomcat")) })
		for g := int64(1); g <= 6; g++ {
			p := &protocol.ProfilePack{TxID: 100 + g, GxID: g, ObjHash: 1, Profile: make([]byte, 512), Version2: true, XType: 2, DiscardType: discardNone}
			within("profile", func() error { return e.Accept(ctx, p) })
		}
		noRoom()
		now = now.Add(9 * time.Second)
		within("tick", func() error { return e.Tick(ctx) })
		noRoom()
		within("heartbeat", func() error { return e.Accept(ctx, object("/a", "tomcat")) })
		for i := 0; i < 20; i++ {
			p := object("/a", "tomcat")
			p.Version = fmt.Sprint(i % 2)
			if i == 19 {
				noRoom()
			}
			within("save", func() error { return e.Accept(ctx, p) })
		}
		var titles []string
		for _, a := range realtimeAlerts(t, e) {
			titles = append(titles, a.Title)
		}
		e.mu.Lock()
		mismatch := cacheMismatch(e)
		left := e.extra.sampling.profileCurrent.order.Len()
		for _, b := range e.extra.sampling.profileOld {
			left += b.order.Len()
		}
		e.mu.Unlock()
		if strings.Join(titles, ",") != alertInactive+","+alertActivated+","+alertDup || mismatch != "" || left >= 6 || e.Stats().DelayedRecoveredEarly != uint64(6-left) {
			t.Fatalf("queue %d: alerts %v, %s, %d profile groups left, stats %+v", queue, titles, mismatch, left, e.Stats())
		}
		flush(t, e)
		if queue == 0 {
			if b, err := s.Profile("20261024", 101); err != nil || len(b) == 0 {
				t.Fatal("recovered profile not stored", err)
			}
		}
		e.Close()
		c.Close()
		s.Close()
	}
}

// A roll that stops on a canceled ctx counts the rest of the group being
// recovered as abandoned and keeps the groups not reached in the old bucket;
// the next delayed pack rolls again and recovers them.
func TestRollCanceledKeepsRemainingGroups(t *testing.T) {
	now := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e, s := testEngineWith(t, &now, func(c *Config) {
		c.OnXLog = func(x *protocol.XLogPack) {
			if x.TxID == 101 {
				cancel()
			}
		}
	})
	set := DefaultSettings()
	set.XLogKeepMillis = 1000
	e.SetSettings(set)
	bg := context.Background()
	// First pack rolls (unit from startup 5000 ms); the rest join one bucket.
	for _, p := range []*protocol.XLogPack{childXLog(101, 1, 1), childXLog(102, 1, 1), childXLog(103, 1, 1), childXLog(104, 4, 1)} {
		if err := e.Accept(bg, p); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(time.Second)
	if err := e.Accept(bg, childXLog(105, 5, 1)); err != nil { // roll: groups 1, 4 become old
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := e.Accept(ctx, childXLog(106, 6, 1)); !errors.Is(err, context.Canceled) {
		t.Fatal("roll with a canceled ctx:", err)
	}
	e.mu.Lock()
	old := e.extra.sampling.xlogOld.order.Len()
	mismatch := cacheMismatch(e)
	e.mu.Unlock()
	if st := e.Stats(); st.DelayedAbandoned != 2 || st.DelayedRecoveredEarly != 0 || old != 1 || mismatch != "" {
		t.Fatalf("old groups %d, %s, stats %+v", old, mismatch, st)
	}
	if err := e.Accept(bg, childXLog(107, 7, 1)); err != nil { // rolls again: group 4
		t.Fatal(err)
	}
	flush(t, e)
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	for txid, want := range map[int64]bool{101: true, 102: false, 103: false, 104: true} {
		if x, err := s.XLog("20261024", txid); err != nil || (x != nil) != want {
			t.Error("XLog", txid, x != nil, err)
		}
	}
}
