package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

func TestGroupURLRules(t *testing.T) {
	for _, tc := range []struct{ url, name string }{
		{"/a/b.JSP", "*.jsp"}, {"/a/x.PNG", "images"}, {"/a/x.js", "statics"},
		{"", "/**"}, {"/", "/**"}, {"/one", "/**"}, {"/api/order", "/api"},
		{"http://example.invalid/x", "http:"}, {"/api/x.png?q=1", "/api"}, {".jsp", "/**"},
	} {
		if got := groupName(tc.url); got != tc.name {
			t.Errorf("%q: %q != %q", tc.url, got, tc.name)
		}
	}
	if got := storage.CounterHash("/api"); got != -1957600919 {
		t.Fatalf("CRC32=%d", got)
	}
}

func TestGroupMeterWindowAndCache(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 3)
	ctx := context.Background()
	add := func(obj, grp, elapsed, err int32) {
		t.Helper()
		if x := e.processGroup(ctx, &protocol.XLogPack{ObjHash: obj, Group: grp, Elapsed: elapsed, Error: err}, now.UnixMilli()); x != nil {
			t.Fatal(x)
		}
	}
	add(1, 7, 10, 0)
	add(2, 7, 20, 3)
	p, err := e.RealtimeServiceGroup(ctx, []int32{1, 2})
	if err != nil || len(groupList(p, "count")) != 0 {
		t.Fatal("startup empty cached snapshot", p, err)
	}
	now = now.Add(time.Second)
	p, err = e.RealtimeServiceGroup(ctx, []int32{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if groupList(p, "count")[0].Int != 2 || groupList(p, "elapsed")[0].Float != 15 || groupList(p, "error")[0].Int != 1 || groupList(p, "name")[0].Text != "unknown" {
		t.Fatalf("aggregate=%+v", p)
	}
	add(1, 7, 100, 0)
	same, _ := e.RealtimeServiceGroup(ctx, []int32{1})
	if groupList(same, "count")[0].Int != 1 {
		t.Fatal("cache rebuilt too early")
	}
	groupList(p, "count")[0].Int = 999
	owned, err := e.RealtimeServiceGroup(ctx, []int32{1, 1, 2})
	if err != nil || groupList(owned, "count")[0].Int != 2 {
		t.Fatal("response must own values and object set must deduplicate", owned, err)
	}
	now = now.Add(29 * time.Second)
	at30, _ := e.RealtimeServiceGroup(ctx, []int32{1, 2})
	if groupList(at30, "count")[0].Int != 1 {
		t.Fatalf("current plus previous29 seconds: %+v", at30)
	}
	now = now.Add(time.Second)
	expired, _ := e.RealtimeServiceGroup(ctx, []int32{1, 2})
	if groupList(expired, "count")[0].Int != 0 {
		t.Fatal("30s expiration", expired)
	}
	empty, err := e.RealtimeServiceGroup(ctx, nil)
	if empty != nil || err != nil {
		t.Fatal(empty, err)
	}
	missing, err := e.RealtimeServiceGroup(ctx, []int32{99})
	if err != nil || len(groupList(missing, "name")) != 0 {
		t.Fatal(missing, err)
	}
}

func TestGroupDictionaryFirstValueAndServiceCache(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, s, _ := testEngine(t, &now, 3)
	ctx := context.Background()
	date := "20261013"
	if err := s.PutText(date, &protocol.TextPack{XType: "service", Hash: 11, Text: "/api/order"}); err != nil {
		t.Fatal(err)
	}
	p := &protocol.XLogPack{ObjHash: 1, Service: 11, EndTime: now.UnixMilli(), Elapsed: 3}
	if err := e.processGroup(ctx, p, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if p.Group != storage.CounterHash("/api") {
		t.Fatal(p.Group)
	}
	if err := s.PutText(date, &protocol.TextPack{XType: "group", Hash: p.Group, Text: "replacement"}); err != nil {
		t.Fatal(err)
	}
	txt, found, err := s.Text(date, "group", p.Group)
	if err != nil || !found || txt != "/api" {
		t.Fatal(txt, found, err)
	}
	p2 := &protocol.XLogPack{ObjHash: 1, Service: 11, EndTime: now.UnixMilli() + 86400000}
	if err := e.processGroup(ctx, p2, now.UnixMilli()); err != nil || p2.Group != p.Group {
		t.Fatal(p2, err)
	}
	missing := &protocol.XLogPack{ObjHash: 1, Service: 22, EndTime: now.UnixMilli()}
	if err := e.processGroup(ctx, missing, now.UnixMilli()); err != nil || missing.Group != 0 {
		t.Fatal(missing, err)
	}
	if err := s.PutText(date, &protocol.TextPack{XType: "service", Hash: 22, Text: "/image/a.JPG"}); err != nil {
		t.Fatal(err)
	}
	if err := e.processGroup(ctx, missing, now.UnixMilli()); err != nil || missing.Group != storage.CounterHash("images") {
		t.Fatal(missing, err)
	}
}

func TestGroupFiveMinutePersistenceAndDeferredCleanup(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, _, c := testEngine(t, &now, 3)
	ctx := context.Background()
	key := groupKey{1, 7}
	if err := e.processGroup(ctx, &protocol.XLogPack{ObjHash: 1, Group: 7, Elapsed: 23, Error: 9}, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	flush(t, e)
	for name, want := range map[string]int64{"cnt:7": 1, "tm:7": 23, "err:7": 1} {
		values, err := c.Daily("20261013", 1, name, 3)
		if err != nil || values[144].Int != want {
			t.Fatal(name, values, err)
		}
	}
	now = now.Add(time.Minute)
	if err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	flush(t, e)
	values, err := c.Daily("20261013", 1, "cnt:7", 3)
	if err != nil || values[144].Int != 1 {
		t.Fatal(values, err)
	}
	now = now.Add(3 * time.Minute)
	if err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if e.extra.group.meters[key] == nil {
		t.Fatal("empty is removed on processing, not save")
	}
	if err := e.processGroup(ctx, &protocol.XLogPack{ObjHash: 1, Group: 7, Elapsed: 5}, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if stat := e.extra.group.meters[key].stat(now.UnixMilli(), 300); stat.count != 1 || stat.elapsed != 5 {
		t.Fatal(stat)
	}
}

func TestGroupBoundsCancelAndClosed(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 3)
	ctx := context.Background()
	for i := int32(1); i <= 2001; i++ {
		if err := e.processGroup(ctx, &protocol.XLogPack{ObjHash: i, Group: 7}, now.UnixMilli()); err != nil {
			t.Fatal(i, err)
		}
	}
	if len(e.extra.group.meters) != 2000 || e.extra.group.meters[groupKey{1, 7}] != nil {
		t.Fatal("meter FIFO bound")
	}
	small, _, _ := testEngine(t, &now, 3)
	small.config.MaxCacheBytes = 1
	// A meter past the cache budget is skipped (counted); the XLog goes on.
	if err := small.processGroup(ctx, &protocol.XLogPack{Group: 7}, now.UnixMilli()); err != nil || len(small.extra.group.meters) != 0 || small.Stats().XLogCacheSkipped != 1 {
		t.Fatal("cache budget", err, len(small.extra.group.meters), small.Stats().XLogCacheSkipped)
	}
	cancel, stop := context.WithCancel(ctx)
	stop()
	if _, err := e.RealtimeServiceGroup(cancel, []int32{1}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := e.groupTick(cancel, now.Add(time.Minute).UnixMilli()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	e.Close()
	if _, err := e.RealtimeServiceGroup(ctx, []int32{1}); !errors.Is(err, storage.ErrClosed) {
		t.Fatal(fmt.Sprint(err))
	}
}

func TestGroupServiceFIFOAndBudgetAccounting(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, s, _ := testEngine(t, &now, 3)
	ctx := context.Background()
	state := e.extra.group
	// Fill only the mapping boundary: this test does not create 50,000 disk
	// dictionaries or claim end-to-end performance from map insertion cost.
	for i := int32(1); i <= groupServiceMax; i++ {
		state.services[i] = 7
		state.serviceOrder.PushBack(i)
	}
	e.cacheBytes += groupServiceMax * groupServiceBytes
	if err := e.processGroup(ctx, &protocol.XLogPack{ObjHash: 1, Service: 1, EndTime: now.UnixMilli()}, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := s.PutText("20261013", &protocol.TextPack{XType: "service", Hash: groupServiceMax + 1, Text: "/api/order"}); err != nil {
		t.Fatal(err)
	}
	if err := e.processGroup(ctx, &protocol.XLogPack{ObjHash: 1, Service: groupServiceMax + 1, EndTime: now.UnixMilli()}, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, exists := state.services[1]; exists || len(state.services) != groupServiceMax {
		t.Fatal("reads must not refresh FIFO")
	}
	if e.cacheBytes != groupServiceMax*groupServiceBytes+2*groupMeterBytes {
		t.Fatal("retained budget", e.cacheBytes)
	}
	now = now.Add(time.Second)
	if _, err := e.RealtimeServiceGroup(ctx, []int32{1}); err != nil {
		t.Fatal(err)
	}
	if e.cacheBytes != groupServiceMax*groupServiceBytes+2*groupMeterBytes+2*groupSnapshotBytes {
		t.Fatal("snapshot budget", e.cacheBytes)
	}
	if _, err := e.RealtimeServiceGroup(ctx, make([]int32, 100001)); !errors.Is(err, protocol.ErrLimit) {
		t.Fatal(err)
	}
}

func TestGroupConcurrentAcceptQueryAndTick(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 4)
	now = now.Add(time.Minute) // fixed clock during concurrent work
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= 30; i++ {
			if err := e.Accept(ctx, &protocol.XLogPack{XType: 1, Service: 1, Group: 7, ObjHash: 1, TxID: i, Elapsed: 3, TailVersion: 12}); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			if _, err := e.RealtimeServiceGroup(ctx, []int32{1}); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			if err := e.Tick(ctx); err != nil {
				errs <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	meter := e.extra.group.meters[groupKey{1, 7}]
	if meter == nil {
		t.Fatal("normal service fixture did not reach group meter")
	}
	if stat := meter.stat(now.UnixMilli(), 30); stat.count != 30 || stat.elapsed != 90 {
		t.Fatal(stat)
	}
}

func TestGroupSaveErrorKeepsRetryAndMeterLongJump(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, _, c := testEngine(t, &now, 4)
	ctx := context.Background()
	if err := e.processGroup(ctx, &protocol.XLogPack{Group: 7, ObjHash: 1, Elapsed: 5}, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	deadline := e.extra.group.nextSave
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.groupTick(ctx, deadline); err != nil {
		t.Fatal(err)
	}
	flush(t, e) // the daily writer fails the save (counted) and restores it
	if e.extra.group.nextSave != deadline {
		t.Fatal("failure disabled retry")
	}
	if w := e.WriterStats()[writeDaily]; w.WriteErrors != 3 || w.Written != 0 {
		t.Fatalf("failed save not counted: %+v", w)
	}
	m := e.extra.group.meters[groupKey{1, 7}]
	if stat := m.stat(now.Add(301*time.Second).UnixMilli(), 300); stat.count != 0 || stat.elapsed != 0 {
		t.Fatal(stat)
	}
}

func groupList(p *protocol.MapPack, key string) []protocol.Value {
	if p == nil {
		return nil
	}
	for _, v := range p.Entries {
		if v.Key == key {
			return v.Value.Values
		}
	}
	return nil
}

// groupTick is takeGroupSaveLocked + queueGroupSave for a caller that does
// not hold the engine lock; Tick runs the two halves itself.
func (e *Engine) groupTick(ctx context.Context, now int64) error {
	if err := e.mu.LockContext(ctx); err != nil {
		return err
	}
	save, ok := e.takeGroupSaveLocked(now)
	e.mu.Unlock()
	if ok {
		e.queueGroupSave(save)
	}
	return nil
}
