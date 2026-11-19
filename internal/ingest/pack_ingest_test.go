package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

func TestAdditionalBuiltinPacketsAccepted(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 3)
	ctx := context.Background()
	hash := storage.CounterHash("/test")
	if err := e.Accept(ctx, &protocol.ObjectPack{ObjHash: hash, ObjName: "/test", ObjType: "java", Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
		t.Fatal(err)
	}
	packs := []protocol.Pack{
		&protocol.InteractionPerfCounterPack{Time: 1, ObjName: "/test", InteractionType: "http", FromHash: hash, ToHash: 2, Count: 3, CustomData: protocol.Value{Type: protocol.ValueMap}},
		&protocol.StatusPack{Time: 1, ObjType: "java", ObjHash: hash, Key: "threads", Data: protocol.Value{Type: protocol.ValueMap}},
		&protocol.StackPack{Time: now.UnixMilli(), ObjHash: hash, Data: []byte{1, 2, 3}},
		&protocol.SummaryPack{Time: now.UnixMilli(), ObjHash: hash, ObjType: "java", SType: 1, Table: protocol.Value{Type: protocol.ValueMap}},
		&protocol.BatchPack{StartTime: now.UnixMilli(), ObjHash: hash, ObjName: "/test", ObjType: "java", BatchJobID: "job", ElapsedTime: 5},
		&protocol.SpanContainerPack{Timestamp: now.UnixMilli(), GxID: 11},
		&protocol.AlertPack{Time: 1, Level: 1, ObjType: "java", ObjHash: hash, Title: "synthetic", Message: "only", Tags: protocol.Value{Type: protocol.ValueMap}},
		&protocol.DroppedXLogPack{GxID: 11, TxID: 11},
	}
	for _, p := range packs {
		if err := e.Accept(ctx, p); err != nil {
			t.Errorf("builtin type %d rejected: %v", p.Type(), err)
		}
	}
	if packs[1].(*protocol.StatusPack).Time != 1 || packs[6].(*protocol.AlertPack).Time != 1 {
		t.Fatal("receiver modified caller-owned packet")
	}
	if e.Stats().XLogs != 0 {
		t.Fatal("SpanContainer or Dropped packet became a completed XLog")
	}
}

func TestDroppedDrivingRemovesPendingChildrenOnly(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, s, _ := testEngine(t, &now, 10)
	ctx := context.Background()
	child := &protocol.XLogPack{TxID: 11, GxID: 10, Service: 1, XType: 4, DiscardType: 1, TailVersion: 12, EndTime: now.UnixMilli(), Elapsed: 1}
	profile := &protocol.ProfilePack{Version2: true, Time: now.UnixMilli(), TxID: 11, GxID: 10, XType: 4, DiscardType: 1, Profile: []byte{1}}
	for _, p := range []protocol.Pack{child, profile, &protocol.DroppedXLogPack{TxID: 10, GxID: 10}, &protocol.XLogPack{TxID: 10, GxID: 10, Service: 1, TailVersion: 12, EndTime: now.UnixMilli(), Elapsed: 1, DiscardType: 1}} {
		if err := e.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Sync(); err != nil { // the pack writers store queued records
		t.Fatal(err)
	}
	if p, err := s.XLog("20261013", 11); err != nil || p != nil {
		t.Fatal("dropped child stored", p, err)
	}
	if err := s.Sync(); err != nil { // the pack writers store queued records
		t.Fatal(err)
	}
	if p, err := s.Profile("20261013", 11); err != nil || len(p) != 0 {
		t.Fatal("dropped profile stored", p, err)
	}
	if err := e.Accept(ctx, &protocol.DroppedXLogPack{TxID: 10, GxID: 10}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil { // the pack writers store queued records
		t.Fatal(err)
	}
	if p, err := s.XLog("20261013", 10); err != nil || p == nil {
		t.Fatal("drop removed completed history", p, err)
	}
}
func TestDrivingReleaseAndExtensionRejection(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, s, _ := testEngine(t, &now, 10)
	ctx := context.Background()
	for _, p := range []protocol.Pack{&protocol.ProfilePack{Version2: true, Time: now.UnixMilli(), TxID: 11, GxID: 10, XType: 4, DiscardType: 1, Profile: []byte{1}}, &protocol.XLogPack{TxID: 11, GxID: 10, Service: 1, XType: 4, DiscardType: 1, TailVersion: 12, EndTime: now.UnixMilli(), Elapsed: 1}, &protocol.XLogPack{TxID: 10, GxID: 10, Service: 1, TailVersion: 12, DiscardType: 1, EndTime: now.UnixMilli(), Elapsed: 1}} {
		if err := e.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Sync(); err != nil { // the pack writers store queued records
		t.Fatal(err)
	}
	if p, err := s.XLog("20261013", 11); err != nil || p == nil {
		t.Fatal("child not released", p, err)
	}
	if err := s.Sync(); err != nil { // the pack writers store queued records
		t.Fatal(err)
	}
	if p, err := s.Profile("20261013", 11); err != nil || len(p) != 1 {
		t.Fatal("profile not released", p, err)
	}
	for _, p := range []protocol.Pack{&protocol.MapPack{}, &protocol.SpanPack{AnnotationTimestamps: protocol.Value{Type: protocol.ValueList}, AnnotationValues: protocol.Value{Type: protocol.ValueList}, Tags: protocol.Value{Type: protocol.ValueMap}}} {
		if err := e.Accept(ctx, p); err != protocol.ErrUnsupported {
			t.Fatalf("extension %T err=%v", p, err)
		}
	}
}
func TestCounterExpiryPreservesReceiveTTLAndMetadata(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 10)
	ctx := context.Background()
	if err := e.Accept(ctx, &protocol.PerfCounterPack{ObjName: "/test", Time: 1, TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "cpu", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 2}}}}}); err != nil {
		t.Fatal(err)
	}
	hash := storage.CounterHash("/test")
	names, err := e.CounterNames(ctx, hash)
	if err != nil || len(names) != 3 {
		t.Fatal(names, err)
	}
	now = now.Add(10 * time.Second)
	if _, found, err := e.Counter(ctx, hash, "cpu", 1); err != nil || !found {
		t.Fatal("TTL equality", found, err)
	}
	now = now.Add(time.Millisecond)
	if _, found, err := e.Counter(ctx, hash, "cpu", 1); err != nil || found {
		t.Fatal("TTL expiration", found, err)
	}
}

func TestAlertSummaryBoundaryAndCounts(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 4, 59, 0, time.UTC)
	e, s, _ := testEngine(t, &now, 10)
	ctx := context.Background()
	for _, level := range []byte{1, 2} {
		if err := e.Accept(ctx, &protocol.AlertPack{ObjHash: 42, ObjType: "java", Title: "same", Level: level, Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(time.Second)
	if err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	flush(t, e)
	out, err := s.SummariesContext(ctx, "20261013", now.Add(-5*time.Minute).UnixMilli(), now.UnixMilli(), 3)
	if err != nil || len(out) != 1 {
		t.Fatal(out, err)
	}
	p := out[0]
	if p.Time != now.Add(-5*time.Minute).UnixMilli() || len(p.Table.Entries) != 3 || p.Table.Entries[0].Key != "count" || p.Table.Entries[0].Value.Values[0].Int != 2 || p.Table.Entries[2].Value.Values[0].Int != 2 {
		t.Fatal(p)
	}
	if len(e.extra.alertSummaries) != 0 {
		t.Fatal("flushed summary still buffered")
	}
}

func TestCounterExpiryCannotBypassPendingSampleBound(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 10)
	e.config.MaxCounters = 3
	e.config.MaxObjects = 1
	ctx := context.Background()
	counter := func(name string) *protocol.PerfCounterPack {
		return &protocol.PerfCounterPack{ObjName: name, Time: now.UnixMilli(), TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "cpu", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 1}}}}}
	}
	if err := e.Accept(ctx, counter("/one")); err != nil {
		t.Fatal(err)
	}
	now = now.Add(10001 * time.Millisecond)
	if err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	// The pending sample of /one still holds the object-name slot: /two is
	// stored but not cached.
	if err := e.Accept(ctx, counter("/two")); err != nil || len(e.current) != 0 || e.Stats().CounterCacheSkipped != 1 {
		t.Fatal("expiry bypassed sample bound", err, len(e.current))
	}
	if err := e.Sample5M(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, counter("/two")); err != nil || len(e.current) != 3 || e.Stats().CounterCacheSkipped != 1 {
		t.Fatal("sample budget not released", err)
	}
	if e.cacheBytes <= 0 || len(e.samples) != 3 || len(e.counterNames) != 1 {
		t.Fatal("sample accounting", e.cacheBytes, len(e.samples), len(e.counterNames))
	}
}

func TestTickCompactsExpiredCachesAndKeepsLiveOrder(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 10)
	table := &interactionTable{entries: map[interactionKey]cachedPacket{}}
	e.extra.interactions[42] = table
	e.extra.nextCleanup = now.UnixMilli()
	e.cacheBytes = 16
	for i := int32(0); i < 3000; i++ {
		expiry := now.UnixMilli() - 1
		if i%1000 == 0 {
			expiry = now.UnixMilli()
		}
		ik := interactionKey{kind: "http", from: i, to: 42}
		table.entries[ik] = cachedPacket{data: []byte{1}, expires: expiry}
		table.order = append(table.order, ik)
		e.extra.interactionRows++
		e.cacheBytes += 29
		ck := counterKey{hash: 42, name: string(rune(i + 1)), kind: 1}
		e.current[ck] = cachedCounter{data: []byte{protocol.ValueDecimal, 1}, expiry: expiry}
		e.counterOrder = append(e.counterOrder, ck)
		e.cacheBytes += 2 + len(ck.name)
	}
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 16 + 3 interactions of 29 + counters "\x01", "\u03e9", "\u07d1" of 2+1, 2+2, 2+2.
	if len(table.order) != 3 || len(e.counterOrder) != 3 || e.extra.interactionRows != 3 || e.cacheBytes != 114 {
		t.Fatalf("cleanup accounting: interactions=%d counters=%d bytes=%d", e.extra.interactionRows, len(e.counterOrder), e.cacheBytes)
	}
	for i, key := range table.order {
		if key.from != int32(i*1000) {
			t.Fatal("live order changed", table.order)
		}
	}
	now = now.Add(5 * time.Second)
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(e.extra.interactions) != 0 || len(e.current) != 0 || len(e.counterOrder) != 0 || e.cacheBytes != 0 {
		t.Fatalf("expired residual: interactions=%d counters=%d bytes=%d", len(e.extra.interactions), len(e.current), e.cacheBytes)
	}
}
