package ingest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

func deadTimeTag(ms int64) protocol.Entry {
	return protocol.Entry{Key: tagDeadTime, Value: protocol.Value{Type: protocol.ValueDecimal, Int: ms}}
}

// objectBound is an engine at MaxObjects=max with the alerting test types
// and a heartbeat helper that runs the 1s Tick after an accepted pack.
func objectBound(t *testing.T, now *time.Time, max int) (*Engine, *storage.Store, func(*protocol.ObjectPack) error) {
	t.Helper()
	e, s := testEngineWith(t, now, func(c *Config) { c.MaxObjects, c.ObjectType = max, alertTypes })
	accept := func(p *protocol.ObjectPack) error {
		t.Helper()
		err := e.Accept(context.Background(), p)
		if err == nil {
			if err := e.Tick(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		return err
	}
	return e, s, accept
}

func mustAccept(t *testing.T, accept func(*protocol.ObjectPack) error, ps ...*protocol.ObjectPack) {
	t.Helper()
	for _, p := range ps {
		if err := accept(p); err != nil {
			t.Fatal(p.ObjName, err)
		}
	}
}

func refused(t *testing.T, accept func(*protocol.ObjectPack) error, p *protocol.ObjectPack) {
	t.Helper()
	if err := accept(p); !errors.Is(err, protocol.ErrLimit) {
		t.Fatal(p.ObjName, "not refused:", err)
	}
}

func live(e *Engine, name string) bool {
	_, ok := e.ObjectNameCached(storage.CounterHash(name))
	return ok
}

// R7: objects that do not die by the Java rule (batch, a deadTime of an hour
// or more) filled MaxObjects for good after one burst, so every new Agent
// was refused. At the bound, with no inactive object, the exempt object
// silent the longest is evicted: batch after a minute, a long deadTime after
// an hour. No alert is raised and the stored record is kept.
func TestObjectLimitSilentExemptObjects(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	e, s, accept := objectBound(t, &now, 3)
	mustAccept(t, accept, object("/b1", "batch"))
	now = now.Add(time.Second)
	mustAccept(t, accept, object("/d1", "tomcat", deadTimeTag(1<<31-1)))
	now = now.Add(time.Second)
	mustAccept(t, accept, object("/x", "tomcat"))
	// 30s later: the exempt objects were heard from within a minute.
	now = now.Add(28 * time.Second)
	mustAccept(t, accept, object("/x", "tomcat"))
	refused(t, accept, object("/real1", "tomcat"))
	// Past a minute /b1 makes room; /d1 (deadTime) waits for an hour.
	now = now.Add(35 * time.Second)
	mustAccept(t, accept, object("/x", "tomcat"), object("/real1", "tomcat"))
	if live(e, "/b1") || !live(e, "/d1") {
		t.Fatal("batch evicted first")
	}
	refused(t, accept, object("/real2", "tomcat"))
	for i := 0; i < 60; i++ { // an hour of heartbeats of /x and /real1
		now = now.Add(time.Minute)
		mustAccept(t, accept, object("/x", "tomcat"), object("/real1", "tomcat"))
	}
	mustAccept(t, accept, object("/real2", "tomcat"))
	if live(e, "/d1") {
		t.Fatal("/d1 kept after an hour of silence")
	}
	refused(t, accept, object("/real3", "tomcat"))
	if st := e.Stats(); st.ObjectsEvictedSilent != 2 || st.ObjectsEvicted != 0 || st.ObjectsLimited != 3 {
		t.Fatalf("%+v", st)
	}
	for _, a := range realtimeAlerts(t, e) { // /x and /real1 go inactive between minutes
		if a.ObjHash == storage.CounterHash("/b1") || a.ObjHash == storage.CounterHash("/d1") {
			t.Fatalf("silent eviction raised %s", a.Title)
		}
	}
	flush(t, e)
	stored, err := s.Objects("20261023")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, p := range stored {
		names = append(names, p.ObjName)
	}
	if got := strings.Join(names, ","); !strings.Contains(got, "/b1") || !strings.Contains(got, "/d1") {
		t.Fatal("stored records", got)
	}
}

// A finite deadTime under an hour (here 120s, as a third-party Agent or a
// Telegraf input might set) is not exempt: the object dies by the Java
// rule (INACTIVE_OBJECT) and is then evicted as a dead object.
func TestObjectLimitLongDeadTimeFollowsJava(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	e, _, accept := objectBound(t, &now, 2)
	mustAccept(t, accept, object("/t", "tomcat", deadTimeTag(120000)), object("/x", "tomcat"))
	for i := 0; i < 14; i++ { // 70s of /x heartbeats
		now = now.Add(5 * time.Second)
		mustAccept(t, accept, object("/x", "tomcat"))
	}
	refused(t, accept, object("/new", "tomcat"))
	for i := 0; i < 11; i++ { // past 120s
		now = now.Add(5 * time.Second)
		mustAccept(t, accept, object("/x", "tomcat"))
	}
	alerts := realtimeAlerts(t, e)
	if len(alerts) != 1 || alerts[0].Title != alertInactive || alerts[0].ObjHash != storage.CounterHash("/t") {
		t.Fatalf("alerts %+v", alerts)
	}
	mustAccept(t, accept, object("/new", "tomcat"))
	if st := e.Stats(); st.ObjectsEvicted != 1 || st.ObjectsEvictedSilent != 0 {
		t.Fatalf("%+v", st)
	}
}

// A dead object waiting for the 1s Tick is preferred: no exempt object is
// evicted while one exists; after the Tick the dead object goes.
func TestObjectLimitDeadBeforeTickPreferred(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	e, _, accept := objectBound(t, &now, 2)
	mustAccept(t, accept, object("/b", "batch"), object("/x", "tomcat"))
	now = now.Add(2 * time.Minute) // /x dead, no Tick yet; /b silent 2 min
	if err := e.Accept(context.Background(), object("/new", "tomcat")); !errors.Is(err, protocol.ErrLimit) {
		t.Fatal(err)
	}
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustAccept(t, accept, object("/new", "tomcat"))
	if live(e, "/x") || !live(e, "/b") {
		t.Fatal("dead object not chosen")
	}
	if st := e.Stats(); st.ObjectsEvicted != 1 || st.ObjectsEvictedSilent != 0 {
		t.Fatalf("%+v", st)
	}
}

// The batch silence is at least object_deadtime_ms (here 120s > 1 minute).
func TestObjectLimitSilenceFollowsObjectDeadTime(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	e, _, accept := objectBound(t, &now, 2)
	set := DefaultSettings()
	set.ObjectDeadTimeMs = 120000
	e.SetSettings(set)
	mustAccept(t, accept, object("/b", "batch"), object("/x", "tomcat"))
	now = now.Add(90 * time.Second)
	mustAccept(t, accept, object("/x", "tomcat"))
	refused(t, accept, object("/new", "tomcat"))
	now = now.Add(30 * time.Second) // 120s: not past it
	mustAccept(t, accept, object("/x", "tomcat"))
	refused(t, accept, object("/new", "tomcat"))
	now = now.Add(time.Millisecond)
	mustAccept(t, accept, object("/new", "tomcat"))
	if live(e, "/b") || e.Stats().ObjectsEvictedSilent != 1 {
		t.Fatalf("%+v", e.Stats())
	}
}

// The eviction is made only once the new object is accepted: a new object
// refused by the cache budget leaves the exempt object registered.
func TestObjectLimitRefusedRegistrationEvictsNothing(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.MaxObjects, c.MaxCacheBytes = 2, 4096 })
	ctx := context.Background()
	for _, p := range []*protocol.ObjectPack{object("/b", "batch"), object("/x", "tomcat")} {
		if err := e.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(2 * time.Minute)
	if err := e.Accept(ctx, object("/x", "tomcat")); err != nil {
		t.Fatal(err)
	}
	big := object("/big", "tomcat", protocol.Entry{Key: "k", Value: protocol.Value{Type: protocol.ValueText, Text: strings.Repeat("v", 8192)}})
	if err := e.Accept(ctx, big); !errors.Is(err, protocol.ErrLimit) {
		t.Fatal(err)
	}
	if !live(e, "/b") || e.Stats().ObjectsEvictedSilent != 0 {
		t.Fatalf("evicted for a refused object: %+v", e.Stats())
	}
	if err := e.Accept(ctx, object("/small", "tomcat")); err != nil {
		t.Fatal(err)
	}
	if live(e, "/b") || e.Stats().ObjectsEvictedSilent != 1 {
		t.Fatalf("%+v", e.Stats())
	}
}

// A silently evicted kube object keeps its pod seq, as Java objMap.remove
// (no inactive(), so no release); its next heartbeat registers it again.
func TestObjectLimitSilentEvictionKeepsKubeSeq(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.MaxObjects = 2 })
	ctx := context.Background()
	k := kubeTags{pod: "app", host: "h1", useKube: true, seq: -1, extraTagEntries: []protocol.Entry{deadTimeTag(2 * 3600000)}}
	if err := e.Accept(ctx, kubePack("/k", k)); err != nil { // held, seq 0 pushed
		t.Fatal(err)
	}
	takeCalls(e.kubeCalls)
	k.seq = 0
	if err := e.Accept(ctx, kubePack("/k", k)); err != nil {
		t.Fatal(err)
	}
	if !e.registered(storage.CounterHash("/k")) {
		t.Fatal("kube object not registered")
	}
	if err := e.Accept(ctx, object("/x", "tomcat")); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour + time.Second)
	if err := e.Accept(ctx, object("/x", "tomcat")); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, object("/new", "tomcat")); err != nil {
		t.Fatal(err)
	}
	if e.registered(storage.CounterHash("/k")) || e.Stats().ObjectsEvictedSilent != 1 {
		t.Fatalf("%+v", e.Stats())
	}
	e.mu.Lock()
	m := e.kube.managers["app"]
	seq, ok := int32(-1), false
	if m != nil {
		seq, ok = m.hostSeq["h1"]
	}
	e.mu.Unlock()
	if !ok || seq != 0 {
		t.Fatal("pod seq released", seq, ok)
	}
}
