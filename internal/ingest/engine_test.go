package ingest

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/storage"
)

func testEngine(t *testing.T, now *time.Time, capacity int) (*Engine, *storage.Store, *storage.Counters) {
	t.Helper()
	root := t.TempDir()
	s, err := storage.Open(storage.Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	c, err := storage.OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{Location: time.UTC, Clock: func() time.Time { return *now }, XLogCapacity: capacity}, s, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close(); c.Close(); s.Close() })
	return e, s, c
}
func TestIngestCursorOwnershipAndProfileTime(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, s, _ := testEngine(t, &now, 3)
	ctx := context.Background()
	p := &protocol.ObjectPack{ObjName: "/test", ObjType: "java", Tags: protocol.Value{Type: protocol.ValueMap}}
	if err := e.Accept(ctx, p); err != nil {
		t.Fatal(err)
	}
	if p.ObjHash != 0 || p.Wakeup != 0 {
		t.Fatal("borrowed packet mutated")
	}
	hash := storage.CounterHash("/test")
	for i := int64(1); i <= 5; i++ {
		x := &protocol.XLogPack{TxID: i, Service: 1, ObjHash: hash, Elapsed: int32(i), Error: int32(i % 2), TailVersion: 12}
		if err := e.Accept(ctx, x); err != nil {
			t.Fatal(err)
		}
	}
	out, err := e.XLogsRealtime(ctx, service.RealtimeQuery{Loop: 0, Index: 0, MinElapsed: 5})
	if err != nil {
		t.Fatal(err)
	}
	if out.Loop != 1 || out.Index != 2 || len(out.Records) != 2 || out.Records[0].TxID != 3 || out.Records[1].TxID != 5 {
		t.Fatalf("cursor=%+v", out)
	}
	out.Records[0].TxID = 99
	again, err := e.XLogsRealtime(ctx, service.RealtimeQuery{Loop: 0, Index: 0, MinElapsed: 5})
	if err != nil || again.Records[0].TxID != 3 {
		t.Fatal("cache ownership", err)
	}
	profile := &protocol.ProfilePack{Time: 1, TxID: 5, ObjHash: hash, Profile: []byte{1, 2, 3}}
	if err := e.Accept(ctx, profile); err != nil {
		t.Fatal(err)
	}
	profile.Profile[0] = 99
	if err := s.Sync(); err != nil { // the profile writer stores it (XLogProfileWR)
		t.Fatal(err)
	}
	b, err := s.Profile("20261013", 5)
	if err != nil || len(b) != 3 || b[0] != 1 {
		t.Fatal(b, err)
	}
	if err := e.Accept(ctx, &protocol.MapPack{}); err == nil {
		t.Fatal("unsupported packet silently accepted")
	}
	stats := e.Stats()
	if stats.Completed != 7 || stats.XLogs != 5 || stats.Rejected != 1 {
		t.Fatal(stats)
	}
}
func TestObjectHeartbeatAndSampling(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 3, 0, 0, time.UTC)
	e, s, c := testEngine(t, &now, 3)
	ctx := context.Background()
	p := &protocol.ObjectPack{ObjName: "/test", ObjType: "java", Tags: protocol.Value{Type: protocol.ValueMap}}
	if err := e.Accept(ctx, p); err != nil {
		t.Fatal(err)
	}
	initial := now.UnixMilli()
	now = now.Add(time.Second)
	if err := e.Accept(ctx, p); err != nil {
		t.Fatal(err)
	}
	flush(t, e)
	stored, err := s.Objects("20261013")
	if err != nil || len(stored) != 1 || stored[0].Wakeup != initial {
		t.Fatalf("heartbeat rewrote history %v %v", stored, err)
	}
	hash := storage.CounterHash(p.ObjName)
	counter := &protocol.PerfCounterPack{ObjName: p.ObjName, Time: now.UnixMilli(), TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 37}}}}}
	if err := e.Accept(ctx, counter); err != nil {
		t.Fatal(err)
	}
	counter.Data.Entries[0].Value.Int = 41
	counter.Time++
	if err := e.Accept(ctx, counter); err != nil {
		t.Fatal(err)
	}
	now = now.Add(5 * time.Minute)
	if err := e.Sample5M(ctx); err != nil {
		t.Fatal(err)
	}
	flush(t, e)
	values, err := c.Daily("20261013", hash, "TPS", 3)
	if err != nil || values[145].Int != 41 {
		t.Fatal(values, err)
	}
	objects, err := e.Objects(ctx, "")
	if err != nil || objects[0].Alive {
		t.Fatal("deadtime", objects, err)
	}
}

func TestEmptyCountersAndMetadataBound(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 3)
	ctx := context.Background()
	e.config.MaxCounters = 6
	for _, name := range []string{"/a", "/b", "/c"} {
		if err := e.Accept(ctx, &protocol.PerfCounterPack{Time: now.UnixMilli(), ObjName: name, TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(e.counterNames) != 3 || len(e.current) != 6 || len(e.samples) != 6 {
		t.Fatal("empty telemetry must retain source metadata", e.cacheBytes)
	}
	e.config.MaxCacheBytes = 16
	p := &protocol.PerfCounterPack{Time: now.UnixMilli(), ObjName: "/long-object-name", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "long-counter-name", Value: protocol.Value{Type: protocol.ValueNull}}}}}
	// Past the budget the pack is stored but not cached (S2: the cache never
	// keeps a counter from the disk).
	if err := e.Accept(ctx, p); err != nil {
		t.Fatal("cache budget rejected a stored counter", err)
	}
	if len(e.counterNames) != 3 || len(e.current) != 6 || e.Stats().CounterCacheSkipped != 1 {
		t.Fatal("skipped cache update mutated cache", e.Stats().CounterCacheSkipped)
	}
}
func TestObjectDefaultDeadlineAndCacheQueryCancellation(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 3)
	ctx := context.Background()
	if err := e.Accept(ctx, &protocol.ObjectPack{ObjType: "java", ObjName: "/test", Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(9 * time.Second)
	objects, err := e.Objects(ctx, "")
	if err != nil || objects[0].Alive {
		t.Fatal("Java 8s inactivity deadline", objects, err)
	}
	e.mu.Lock()
	waiting, cancel := context.WithCancel(ctx)
	cancel()
	done := make(chan error, 1)
	go func() { _, _, err := e.Counter(waiting, 0, "x", 1); done <- err }()
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("cancelled query accepted")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cancelled query waited for disk owner")
	}
	e.mu.Unlock()
}

func TestSample5MAfterClose(t *testing.T) {
	now := time.Now()
	e, _, _ := testEngine(t, &now, 2)
	e.Close()
	if err := e.Sample5M(context.Background()); err != storage.ErrClosed {
		t.Fatalf("closed sampling=%v", err)
	}
}

// AcceptOwnedFrom (the UDP path, no clone) leaves the same state as
// AcceptFrom (cloned): stored XLog and profile, realtime ring, object address
// from the peer.
func TestAcceptOwnedMatchesAccept(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()
	run := func(owned bool) (string, string, string) {
		e, s, _ := testEngine(t, &now, 8)
		accept := e.AcceptFrom
		if owned {
			accept = e.AcceptOwnedFrom
		}
		obj := &protocol.ObjectPack{ObjType: "java", ObjHash: 7, ObjName: "/h/app", Alive: true, Tags: protocol.Value{Type: protocol.ValueMap}}
		x := &protocol.XLogPack{EndTime: now.UnixMilli(), ObjHash: 7, TxID: 11, Service: 3, Elapsed: 5, TailVersion: protocol.XLogCurrentTail}
		prof := &protocol.ProfilePack{Time: now.UnixMilli(), TxID: 11, ObjHash: 7, Profile: []byte{1, 2, 3}}
		for _, p := range []protocol.Pack{obj, x, prof} {
			if err := accept(ctx, p, "10.0.0.9"); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		stored, err := s.XLog("20261013", 11)
		if err != nil || stored == nil {
			t.Fatal(stored, err)
		}
		b, err := s.Profile("20261013", 11)
		if err != nil {
			t.Fatal(err)
		}
		rt, err := e.XLogsRealtime(ctx, service.RealtimeQuery{})
		if err != nil {
			t.Fatal(err)
		}
		objs, err := e.Objects(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		var objText string
		for _, o := range objs {
			objText += fmt.Sprintf("%#v;", *o)
		}
		return fmt.Sprintf("%#v %v", *stored, b), fmt.Sprintf("%d", len(rt.Records)), objText
	}
	a1, a2, a3 := run(false)
	b1, b2, b3 := run(true)
	if a1 != b1 || a2 != b2 || a3 != b3 {
		t.Fatalf("owned differs:\n%s\n%s\n%s\n%s\n%s\n%s", a1, b1, a2, b2, a3, b3)
	}
}

// Object must answer exactly what the live list holds for that hash, without
// depending on the other objects.
func TestObjectMatchesLiveListEntry(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 3)
	ctx := context.Background()
	for _, name := range []string{"/a", "/b", "/c"} {
		if err := e.Accept(ctx, &protocol.ObjectPack{ObjType: "java", ObjName: name, Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(9 * time.Second) // /a and /b pass the 8s inactivity deadline
	if err := e.Accept(ctx, &protocol.ObjectPack{ObjType: "java", ObjName: "/c", Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, &protocol.PerfCounterPack{ObjName: "/c", Time: now.UnixMilli(), TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 37}}}}}); err != nil {
		t.Fatal(err)
	}
	list, err := e.Objects(ctx, "")
	if err != nil || len(list) != 3 {
		t.Fatal(list, err)
	}
	alive := 0
	for _, want := range list {
		got, err := e.Object(ctx, "", want.ObjHash)
		if err != nil || got == nil {
			t.Fatal(want.ObjName, got, err)
		}
		if got == want || !reflect.DeepEqual(got, want) {
			t.Fatalf("Object(%s) = %+v, live list has %+v", want.ObjName, got, want)
		}
		if got.Alive {
			alive++
		}
	}
	if alive != 1 {
		t.Fatal("only /c is alive", alive)
	}
	if got, err := e.Object(ctx, "", 12345); err != nil || got != nil {
		t.Fatal("unknown hash", got, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := e.Object(cancelled, "", list[0].ObjHash); err == nil {
		t.Fatal("cancelled lookup accepted")
	}
}
