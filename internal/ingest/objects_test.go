package ingest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/storage"
)

func alertTypes(objType string) (ObjectTypeInfo, bool) {
	switch objType {
	case "tomcat", "zipkin", "z$svc":
		return ObjectTypeInfo{Family: familyJavaEE}, true
	case "sub":
		return ObjectTypeInfo{Family: "host", SubObject: true}, true
	}
	return ObjectTypeInfo{}, false
}

func realtimeAlerts(t *testing.T, e *Engine) []*protocol.AlertPack {
	t.Helper()
	r, err := e.AlertsRealtime(context.Background(), service.AlertQuery{Loop: -1})
	if err != nil {
		t.Fatal(err)
	}
	return r.Records
}

func object(name, typ string, tags ...protocol.Entry) *protocol.ObjectPack {
	return &protocol.ObjectPack{ObjName: name, ObjType: typ, Tags: protocol.Value{Type: protocol.ValueMap, Entries: tags}}
}

func TestObjectInactiveAndActivatedAlerts(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.ObjectType = alertTypes })
	ctx := context.Background()
	t0 := now.UnixMilli()
	for _, p := range []*protocol.ObjectPack{object("/a", "tomcat"), object("/sub", "sub"), object("/x", "unknown")} {
		if err := e.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	tick := func(d time.Duration) {
		t.Helper()
		now = now.Add(d)
		if err := e.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	tick(8 * time.Second) // now == wakeup + 8000: still alive
	if n := len(realtimeAlerts(t, e)); n != 0 {
		t.Fatalf("alive at the dead-time boundary: %d alerts", n)
	}
	tick(time.Millisecond)
	alerts := realtimeAlerts(t, e)
	if len(alerts) != 1 {
		t.Fatalf("INACTIVE only for a known non-sub type: %+v", alerts)
	}
	a := alerts[0]
	hash := storage.CounterHash("/a")
	wantMsg := "/a is not running. OBJECT  objType=tomcat objHash=" + javaHexa32(int64(hash)) + " objName=/a " + javaTimestamp(t0, time.UTC)
	if a.Title != "INACTIVE_OBJECT" || a.Level != 0 || a.ObjType != "scouter" || a.ObjHash != hash || a.Message != wantMsg || a.Time != now.UnixMilli() {
		t.Fatalf("inactive alert %+v\nwant message %q", a, wantMsg)
	}
	flush(t, e)
	if stored, err := e.AlertHistory(ctx, "20261015", time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC).UnixMilli(), now.UnixMilli()+1); err != nil || len(stored) != 1 {
		t.Fatalf("INACTIVE goes through AlertCore (stored): %v %v", stored, err)
	}
	tick(time.Second)
	if len(realtimeAlerts(t, e)) != 1 {
		t.Fatal("inactive once per alive period")
	}
	// The heartbeat of an inactive primary object raises ACTIVATED with the
	// stored pack before wakeup (no "alive", old wakeup), level from settings.
	set := DefaultSettings()
	set.InactiveAlertLevel = 258 // asInstanceOf[Byte]: 2
	e.SetSettings(set)
	if err := e.Accept(ctx, object("/a", "tomcat", protocol.Entry{Key: "k", Value: protocol.Value{Type: protocol.ValueText, Text: "v"}})); err != nil {
		t.Fatal(err)
	}
	alerts = realtimeAlerts(t, e)
	if len(alerts) != 2 || alerts[1].Title != "ACTIVATED_OBJECT" || alerts[1].Level != 2 ||
		alerts[1].Message != "/a is running now. OBJECT  objType=tomcat objHash="+javaHexa32(int64(hash))+" objName=/a "+javaTimestamp(t0, time.UTC) {
		t.Fatalf("activated %+v", alerts)
	}
	if err := e.Accept(ctx, object("/a", "tomcat")); err != nil {
		t.Fatal(err)
	}
	if len(realtimeAlerts(t, e)) != 2 {
		t.Fatal("ACTIVATED only after an inactive period")
	}
}

func TestObjectDeadTimeRules(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.ObjectType = alertTypes })
	ctx := context.Background()
	dead := func(v protocol.Value) protocol.Entry { return protocol.Entry{Key: "deadTime", Value: v} }
	for _, p := range []*protocol.ObjectPack{
		object("/zipkin", "zipkin"), object("/zsvc", "z$svc"), object("/tomcat", "tomcat"),
		object("/neg", "tomcat", dead(protocol.Value{Type: protocol.ValueDecimal, Int: -1})),
		object("/float", "tomcat", dead(protocol.Value{Type: protocol.ValueFloat, Float: 2500.9})),
		object("/zero", "zipkin", dead(protocol.Value{Type: protocol.ValueDecimal, Int: 0})),
		object("/batch", "batch", dead(protocol.Value{Type: protocol.ValueDecimal, Int: -1})),
	} {
		if err := e.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	alive := func() map[string]bool {
		list, err := e.Objects(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]bool{}
		for _, p := range list {
			m[p.ObjName] = p.Alive
		}
		return m
	}
	if m := alive(); m["/neg"] || !m["/float"] || !m["/batch"] {
		t.Fatalf("negative deadTime is dead at once, batch never: %v", m)
	}
	now = now.Add(2501 * time.Millisecond)
	if m := alive(); m["/float"] || !m["/tomcat"] {
		t.Fatalf("Float deadTime narrows to 2500: %v", m)
	}
	now = now.Add(290 * time.Second)
	if m := alive(); m["/tomcat"] || !m["/zipkin"] || !m["/zsvc"] || !m["/zero"] {
		t.Fatalf("zipkin types use object_zipkin_deadtime_ms (300000): %v", m)
	}
	set := DefaultSettings()
	set.ZipkinDeadTimeMs = 1000
	e.SetSettings(set)
	if m := alive(); m["/zipkin"] || m["/zsvc"] {
		t.Fatalf("live zipkin dead time: %v", m)
	}
}

// MAYBE_OBJ_DUP: every 20th saved update of a registered object goes to the
// realtime alert cache only (AlertCache.put), with objHash 0 and level 0.
func TestObjectTooManyChangeAlert(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.ObjectType = alertTypes })
	ctx := context.Background()
	for i := 0; i <= 20; i++ {
		p := object("/dup", "tomcat")
		p.Version = fmt.Sprint("v", i%2) // every heartbeat differs: save
		now = now.Add(time.Second)
		if err := e.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	alerts := realtimeAlerts(t, e)
	if len(alerts) != 1 {
		t.Fatalf("one MAYBE_OBJ_DUP after 20 updates: %+v", alerts)
	}
	a := alerts[0]
	want := "Maybe duplicated objNames!! Please check OBJECT  objType=tomcat objHash=" + javaHexa32(int64(storage.CounterHash("/dup"))) + " objName=/dup v1 alive " + javaTimestamp(now.UnixMilli(), time.UTC)
	if a.Title != "MAYBE_OBJ_DUP" || a.ObjHash != 0 || a.Level != 0 || a.ObjType != "scouter" || a.Message != want {
		t.Fatalf("dup alert %+v\nwant %q", a, want)
	}
	flush(t, e)
	if stored, err := e.AlertHistory(ctx, "20261015", time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC).UnixMilli(), now.UnixMilli()+1); err != nil || len(stored) != 0 {
		t.Fatalf("MAYBE_OBJ_DUP is not stored: %v %v", stored, err)
	}
}

// Objects read from today's file at startup are not in Java's objMap: no
// INACTIVE, and their first heartbeat is a new registration (no ACTIVATED).
func TestObjectsLoadedAtStartupRaiseNoAlert(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	root := t.TempDir()
	open := func() (*Engine, func()) {
		s, err := storage.Open(storage.Config{Directory: root, Location: time.UTC})
		if err != nil {
			t.Fatal(err)
		}
		c, err := storage.OpenCounters(root, time.UTC)
		if err != nil {
			t.Fatal(err)
		}
		e, err := New(Config{Location: time.UTC, Clock: func() time.Time { return now }, XLogCapacity: 8, ObjectType: alertTypes}, s, c)
		if err != nil {
			t.Fatal(err)
		}
		return e, func() { e.Close(); c.Close(); s.Close() }
	}
	e, closeAll := open()
	if err := e.Accept(context.Background(), object("/a", "tomcat")); err != nil {
		t.Fatal(err)
	}
	closeAll()
	now = now.Add(time.Minute)
	e, closeAll = open()
	defer closeAll()
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(context.Background(), object("/a", "tomcat")); err != nil {
		t.Fatal(err)
	}
	if n := len(realtimeAlerts(t, e)); n != 0 {
		t.Fatalf("startup-loaded object raised %d alerts", n)
	}
}

// The first heartbeat of an object loaded at startup takes the Java
// "objPack == null" branch even when address/type/version/date are unchanged:
// ObjectWR.add (object file and object text) with the received tags, and the
// object is in objMap from then on.
func TestObjectLoadedAtStartupFirstHeartbeatSaves(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	root := t.TempDir()
	s, err := storage.Open(storage.Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c, err := storage.OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	hash := storage.CounterHash("/a")
	tag := func(v string) protocol.Entry {
		return protocol.Entry{Key: "k", Value: protocol.Value{Type: protocol.ValueText, Text: v}}
	}
	// Only the object file: no object text, as after a lost text write.
	loaded := object("/a", "tomcat", tag("old"))
	loaded.ObjHash, loaded.Wakeup, loaded.Alive = hash, now.UnixMilli(), true
	if err := s.PutObject("20261015", loaded); err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{Location: time.UTC, Clock: func() time.Time { return now }, XLogCapacity: 8, ObjectType: alertTypes}, s, c)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	now = now.Add(time.Minute)
	if err := e.Accept(ctx, object("/a", "tomcat", tag("new"))); err != nil {
		t.Fatal(err)
	}
	if text, ok, err := s.Text("20261015", "object", hash); err != nil || !ok || text != "/a" {
		t.Fatalf("object text not written: %q %v %v", text, ok, err)
	}
	flush(t, e)
	stored, err := s.Objects("20261015")
	if err != nil || len(stored) != 1 {
		t.Fatalf("stored objects %+v %v", stored, err)
	}
	if got := stored[0]; got.Wakeup != now.UnixMilli() || len(got.Tags.Entries) != 1 || got.Tags.Entries[0].Value.Text != "new" {
		t.Fatalf("stored object not updated: %+v", got)
	}
	if st := e.objState[hash]; st == nil || !st.alive || st.updated != 0 {
		t.Fatalf("object must now be in objMap: %+v", st)
	}
	now = now.Add(9 * time.Second)
	if err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if a := realtimeAlerts(t, e); len(a) != 1 || a[0].Title != alertInactive {
		t.Fatalf("registered object must go inactive: %+v", a)
	}
}

// ACTIVATED_OBJECT is raised only once the heartbeat is accepted: a heartbeat
// rejected at the object byte bound (coreDelta) keeps the object inactive and
// raises nothing, so a saturated engine does not repeat the alert on every
// heartbeat.
func TestObjectActivatedOnlyAfterAcceptedHeartbeat(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.ObjectType = alertTypes })
	ctx := context.Background()
	if err := e.Accept(ctx, object("/a", "tomcat")); err != nil {
		t.Fatal(err)
	}
	now = now.Add(9 * time.Second)
	if err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(realtimeAlerts(t, e)); n != 1 {
		t.Fatalf("inactive alert: %d", n)
	}
	hash := storage.CounterHash("/a")
	big := protocol.Entry{Key: "k", Value: protocol.Value{Type: protocol.ValueText, Text: "a longer tag value"}}
	e.mu.Lock()
	budget := e.config.MaxCacheBytes
	e.config.MaxCacheBytes = e.coreBytes
	e.mu.Unlock()
	for i, p := range []*protocol.ObjectPack{
		object("/a", "tomcat", big), // unchanged heartbeat, larger tags
		object("/a", "tomcat", big),
		{ObjName: "/a", ObjType: "tomcat", Version: "v2", Tags: protocol.Value{Type: protocol.ValueMap}}, // save branch
	} {
		if err := e.Accept(ctx, p); !errors.Is(err, protocol.ErrLimit) {
			t.Fatalf("heartbeat %d: want ErrLimit, got %v", i, err)
		}
		// An attempted alert would also fail at the bound: count both.
		if n, failed := len(realtimeAlerts(t, e)), e.Stats().ObjectAlertErrors; n != 1 || failed != 0 {
			t.Fatalf("heartbeat %d: rejected heartbeat raised an alert (%d, %d failed)", i, n, failed)
		}
		if e.objState[hash].alive {
			t.Fatalf("heartbeat %d: rejected heartbeat revived the object", i)
		}
	}
	e.mu.Lock()
	e.config.MaxCacheBytes = budget
	e.mu.Unlock()
	if err := e.Accept(ctx, object("/a", "tomcat", big)); err != nil {
		t.Fatal(err)
	}
	if a := realtimeAlerts(t, e); len(a) != 2 || a[1].Title != alertActivated || !e.objState[hash].alive {
		t.Fatalf("accepted heartbeat: %+v", a)
	}
	if err := e.Accept(ctx, object("/a", "tomcat", big)); err != nil || len(realtimeAlerts(t, e)) != 2 {
		t.Fatalf("ACTIVATED once per inactive period: %v", err)
	}
}

func TestTagCountGate(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	root := t.TempDir()
	s, err := storage.Open(storage.Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	c, err := storage.OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	off := DefaultSettings()
	off.TagCount = false
	e, err := New(Config{Location: time.UTC, Clock: func() time.Time { return now }, XLogCapacity: 8, Settings: &off}, s, c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { e.Close(); c.Close(); s.Close() }()
	ctx := context.Background()
	if err := e.Accept(ctx, object("/a", "tomcat")); err != nil {
		t.Fatal(err)
	}
	hash := storage.CounterHash("/a")
	if err := e.Accept(ctx, &protocol.XLogPack{EndTime: now.UnixMilli(), ObjHash: hash, Service: 5, TxID: 1, Elapsed: 1, TailVersion: protocol.XLogCurrentTail}); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, &protocol.AlertPack{ObjHash: hash, Title: "t", ObjType: "tomcat", Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "20261015", "tagcnt")); !os.IsNotExist(err) {
		t.Fatalf("tagcnt_enabled=false must not count tags: %v", err)
	}
	e.SetSettings(DefaultSettings())
	if err := e.Accept(ctx, &protocol.XLogPack{EndTime: now.UnixMilli(), ObjHash: hash, Service: 5, TxID: 2, Elapsed: 1, TailVersion: protocol.XLogCurrentTail}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "20261015", "tagcnt")); err != nil {
		t.Fatalf("tagcnt_enabled=true counts again: %v", err)
	}
}

func TestAuto5MSamplingGate(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, nil)
	ctx := context.Background()
	v := protocol.Value{Type: protocol.ValueDecimal, Int: 7}
	if err := e.Accept(ctx, &protocol.PerfCounterPack{ObjName: "/a", Time: now.UnixMilli(), TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: v}}}}); err != nil {
		t.Fatal(err)
	}
	off := DefaultSettings()
	off.Auto5MSampling = false
	e.SetSettings(off)
	before := len(e.samples)
	if err := e.Sample5M(ctx); err != nil {
		t.Fatal(err)
	}
	if before == 0 || len(e.samples) != before {
		t.Fatal("disabled tick keeps the accumulated samples")
	}
	e.SetSettings(DefaultSettings())
	if err := e.Sample5M(ctx); err != nil {
		t.Fatal(err)
	}
	if len(e.samples) != 0 {
		t.Fatal("enabled tick writes and clears")
	}
	flush(t, e)
	got, err := e.CounterDaily(ctx, "20261015", storage.CounterHash("/a"), "TPS", 3)
	if err != nil || len(got) == 0 {
		t.Fatalf("5m sample written: %v %v", got, err)
	}
}

func TestAddObjectTypeIfNotExistHook(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	var calls [][2]string
	e, _ := testEngineWith(t, &now, func(c *Config) {
		c.ObjectType = alertTypes
		c.AddObjectTypeIfNotExist = func(objType, detected string) { calls = append(calls, [2]string{objType, detected}) }
	})
	ctx := context.Background()
	text := func(s string) protocol.Value { return protocol.Value{Type: protocol.ValueText, Text: s} }
	for _, p := range []*protocol.ObjectPack{
		object("/k", "tomcat", protocol.Entry{Key: "detected", Value: text("tomcat")}), // known type
		object("/n", "mytype"), // no detected tag
		object("/h", "mytype", protocol.Entry{Key: "detected", Value: protocol.Value{Type: protocol.ValueTextHash, Int: 1}}), // not a TextValue
		object("/d", "mytype", protocol.Entry{Key: "detected", Value: text("tomcat")}),
	} {
		if err := e.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	if len(calls) != 1 || calls[0] != [2]string{"mytype", "tomcat"} {
		t.Fatalf("calls %v", calls)
	}
}
