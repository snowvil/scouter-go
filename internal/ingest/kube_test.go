package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

func testEngineWith(t *testing.T, now *time.Time, configure func(*Config)) (*Engine, *storage.Store) {
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
	cfg := Config{Location: time.UTC, Clock: func() time.Time { return *now }, XLogCapacity: 16}
	if configure != nil {
		configure(&cfg)
	}
	e, err := New(cfg, s, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close(); c.Close(); s.Close() })
	return e, s
}

type kubeTags struct {
	pod, host       string
	noHost, noSeq   bool
	useKube         bool
	seq             int64
	extraTagEntries []protocol.Entry
}

func kubePack(name string, k kubeTags) *protocol.ObjectPack {
	tags := protocol.Value{Type: protocol.ValueMap}
	add := func(key string, v protocol.Value) {
		tags.Entries = append(tags.Entries, protocol.Entry{Key: key, Value: v})
	}
	if k.pod != "" {
		add(tagPodName, protocol.Value{Type: protocol.ValueText, Text: k.pod})
	}
	if !k.noHost {
		add(tagHostName, protocol.Value{Type: protocol.ValueText, Text: k.host})
	}
	add(tagUseKubeSeq, protocol.Value{Type: protocol.ValueBoolean, Bool: k.useKube})
	if !k.noSeq {
		add(tagKubeSeq, protocol.Value{Type: protocol.ValueDecimal, Int: k.seq})
	}
	tags.Entries = append(tags.Entries, k.extraTagEntries...)
	return &protocol.ObjectPack{ObjName: name, ObjType: "tomcat", Tags: tags}
}

// takeCalls drains a queue without running the worker.
func takeCalls(q *agentCallQueue) []agentCall {
	var out []agentCall
	for {
		select {
		case c := <-q.ch:
			out = append(out, c)
		default:
			return out
		}
	}
}

func seqOf(t *testing.T, c agentCall) int64 {
	t.Helper()
	if c.cmd != cmdSetKubeSeq || len(c.param.Entries) != 1 || c.param.Entries[0].Key != "seq" || c.param.Entries[0].Value.Type != protocol.ValueDecimal {
		t.Fatalf("kube push %+v", c)
	}
	return c.param.Entries[0].Value.Int
}

func (e *Engine) registered(hash int32) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.objects[hash] != nil
}

func TestKubePodSeqLifecycle(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, s := testEngineWith(t, &now, nil)
	ctx := context.Background()
	accept := func(name string, k kubeTags) {
		t.Helper()
		if err := e.Accept(ctx, kubePack(name, k)); err != nil {
			t.Fatal(err)
		}
	}
	h1, h2, h3 := storage.CounterHash("/h1"), storage.CounterHash("/h2"), storage.CounterHash("/h3")

	// Init: no seq yet -> held, seq 0 pushed to the Agent.
	accept("/h1", kubeTags{pod: "app", host: "h1", useKube: true, seq: -1})
	if e.registered(h1) || e.Stats().KubeHeld != 1 {
		t.Fatal("init must hold the registration")
	}
	calls := takeCalls(e.kubeCalls)
	if len(calls) != 1 || calls[0].hash != h1 || seqOf(t, calls[0]) != 0 {
		t.Fatalf("init push %+v", calls)
	}
	// Within 15s the same host is held again and gets the same seq.
	now = now.Add(5 * time.Second)
	accept("/h1", kubeTags{pod: "app", host: "h1", useKube: true, seq: -1})
	if calls = takeCalls(e.kubeCalls); len(calls) != 1 || seqOf(t, calls[0]) != 0 || e.registered(h1) {
		t.Fatalf("repeat init %+v", calls)
	}
	accept("/h2", kubeTags{pod: "app", host: "h2", useKube: true, seq: -1})
	if calls = takeCalls(e.kubeCalls); len(calls) != 1 || seqOf(t, calls[0]) != 1 {
		t.Fatalf("second host %+v", calls)
	}
	// The Agent comes back with its seq: confirmed, registered, no push.
	accept("/h1", kubeTags{pod: "app", host: "h1", useKube: true, seq: 0})
	if !e.registered(h1) || len(takeCalls(e.kubeCalls)) != 0 || e.kube.initial["h1"] != nil {
		t.Fatal("confirmed seq must register and drop the initial record")
	}
	// h3 claims seq 1 owned by h2: initialized again with a fresh seq 2.
	accept("/h3", kubeTags{pod: "app", host: "h3", useKube: true, seq: 1})
	if calls = takeCalls(e.kubeCalls); len(calls) != 1 || seqOf(t, calls[0]) != 2 || e.registered(h3) {
		t.Fatalf("taken seq %+v", calls)
	}
	// 15s after h2's initial record it proceeds without a push and its seq is released.
	now = now.Add(15 * time.Second)
	accept("/h2", kubeTags{pod: "app", host: "h2", useKube: true, seq: -1})
	if !e.registered(h2) || len(takeCalls(e.kubeCalls)) != 0 {
		t.Fatal("after 15s the object proceeds without a push")
	}
	if _, ok := e.kube.managers["app"].seqHost[1]; ok || e.kube.initial["h2"] == nil {
		t.Fatal("15s path releases the seq and keeps the initial record")
	}
	// Stored bytes do not carry kube fields: they are transient in Java.
	flush(t, e)
	stored, err := s.Objects("20261015")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range stored {
		if p.ObjHash == h1 {
			want := kubePack("/h1", kubeTags{pod: "app", host: "h1", useKube: true, seq: 0})
			if !bytes.Equal(encodeTags(t, p.Tags), encodeTags(t, want.Tags)) {
				t.Fatal("stored tags changed")
			}
		}
	}
	// Inactivation releases h1's seq 0 and the next init reuses it.
	now = now.Add(9 * time.Second)
	if err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.kube.managers["app"].seqHost[0]; ok {
		t.Fatal("inactive object must release its seq")
	}
	if e.objState[h1].alive || e.objState[h2].alive {
		t.Fatal("inactive flag")
	}
	// h3 (seq 2) still holds; h4 takes the lowest free seq (0).
	accept("/h4", kubeTags{pod: "app", host: "h4", useKube: true, seq: -1})
	if calls = takeCalls(e.kubeCalls); len(calls) != 1 || seqOf(t, calls[0]) != 0 {
		t.Fatalf("released seq reuse %+v", calls)
	}
	// h1 returns with its old seq 0, now owned by h4: initialized again with 1.
	accept("/h1", kubeTags{pod: "app", host: "h1", useKube: true, seq: 0})
	if calls = takeCalls(e.kubeCalls); len(calls) != 1 || seqOf(t, calls[0]) != 1 {
		t.Fatalf("re-init %+v", calls)
	}
	if e.objState[h1].alive {
		t.Fatal("a held heartbeat does not wake the stored object")
	}
	// The pushed seq confirmed: the unchanged stored object wakes up (no save).
	accept("/h1", kubeTags{pod: "app", host: "h1", useKube: true, seq: 1})
	if !e.objState[h1].alive || !e.registered(h1) {
		t.Fatal("wakeup must make the kube object alive")
	}
	// A second Tick inside the dead time changes nothing.
	if err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if e.kube.managers["app"].seqHost[1] != "h1" {
		t.Fatal("alive object keeps its seq")
	}
}

func encodeTags(t *testing.T, v protocol.Value) []byte {
	t.Helper()
	b, err := protocol.EncodeValue(v, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestKubeBranchConditions(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, nil)
	ctx := context.Background()
	cases := []struct {
		name     string
		tags     kubeTags
		register bool
	}{
		{"/off", kubeTags{pod: "app", host: "x", useKube: false, seq: -1}, true},
		{"/nopod", kubeTags{host: "x", useKube: true, seq: -1}, true},
		{"/nohost", kubeTags{pod: "app", noHost: true, useKube: true, seq: -1}, true},
		{"/emptyhost", kubeTags{pod: "app", host: "", useKube: true, seq: -1}, true},
		// MapValue.getInt of an absent kubeSeq is 0: a recovery of seq 0, not an init.
		{"/noseq", kubeTags{pod: "app", host: "y", useKube: true, noSeq: true}, true},
		// A Text kubeSeq is not a Number: getInt is 0 as well.
		{"/textseq", kubeTags{pod: "app2", host: "z", useKube: true, noSeq: true,
			extraTagEntries: []protocol.Entry{{Key: tagKubeSeq, Value: protocol.Value{Type: protocol.ValueText, Text: "-1"}}}}, true},
		// A Double -1.5 narrows to -1: init.
		{"/doubleseq", kubeTags{pod: "app3", host: "w", useKube: true, noSeq: true,
			extraTagEntries: []protocol.Entry{{Key: tagKubeSeq, Value: protocol.Value{Type: protocol.ValueDouble, Double: -1.5}}}}, false},
	}
	for _, c := range cases {
		if err := e.Accept(ctx, kubePack(c.name, c.tags)); err != nil {
			t.Fatal(c.name, err)
		}
		if e.registered(storage.CounterHash(c.name)) != c.register {
			t.Fatalf("%s registered=%v", c.name, !c.register)
		}
	}
	if m := e.kube.managers["app"]; m == nil || m.seqHost[0] != "y" || m.hostSeq["y"] != 0 {
		t.Fatalf("absent kubeSeq must recover seq 0: %+v", m)
	}
	// Object-level kube fields: /nohost has podName but a null hostName.
	if o := e.kube.objects[storage.CounterHash("/nohost")]; o == nil || o.hostOK || o.podName != "app" {
		t.Fatalf("null host object %+v", o)
	}
	if o := e.kube.objects[storage.CounterHash("/off")]; o != nil {
		t.Fatal("no kube branch, no kube fields")
	}
	if n := len(takeCalls(e.kubeCalls)); n != 1 {
		t.Fatalf("only the double init pushes: %d", n)
	}
}

// copyKubePropFrom takes the podName of the initial pack, which may belong to
// another pod manager, and inactivation then releases through that manager.
func TestKubeInitialFromAnotherPod(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, nil)
	ctx := context.Background()
	if err := e.Accept(ctx, kubePack("/a", kubeTags{pod: "p1", host: "h", useKube: true, seq: -1})); err != nil {
		t.Fatal(err)
	}
	takeCalls(e.kubeCalls)
	now = now.Add(15 * time.Second) // exactly 15s: initialTime > now-15000 is false
	if err := e.Accept(ctx, kubePack("/a", kubeTags{pod: "p2", host: "h", useKube: true, seq: -1})); err != nil {
		t.Fatal(err)
	}
	hash := storage.CounterHash("/a")
	if !e.registered(hash) || len(takeCalls(e.kubeCalls)) != 0 {
		t.Fatal("15s boundary must proceed without a push")
	}
	if o := e.kube.objects[hash]; o == nil || o.podName != "p1" || o.hostName != "h" {
		t.Fatalf("copied kube props %+v", o)
	}
	// p1 still holds seq 0 for h (p2's removePodSeq had nothing to remove).
	if e.kube.managers["p1"].hostSeq["h"] != 0 || e.kube.managers["p2"] != nil {
		t.Fatal("manager state")
	}
	now = now.Add(9 * time.Second)
	if err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if e.kube.managers["p1"] != nil || e.kube.initial["h"] != nil {
		t.Fatal("inactivation must release through the copied podName")
	}
}

func TestKubeRemovedObjectKeepsSeq(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, nil)
	ctx := context.Background()
	if err := e.Accept(ctx, kubePack("/a", kubeTags{pod: "p", host: "h", useKube: true, seq: 3})); err != nil {
		t.Fatal(err)
	}
	if err := e.RemoveObjects(ctx, []int32{storage.CounterHash("/a")}, false); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	// ObjectMap.remove does not call inactive(): the seq stays taken.
	if e.kube.managers["p"].seqHost[3] != "h" || len(e.kube.objects) != 0 {
		t.Fatal("removed object must not release its seq")
	}
}

func TestKubeBounds(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.MaxObjects = 2; c.MaxCounters = 6 })
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := e.Accept(ctx, kubePack(fmt.Sprint("/p", i), kubeTags{pod: fmt.Sprint("pod", i), host: "h", useKube: true, seq: int64(i)})); err != nil {
			t.Fatal(err)
		}
	}
	before := e.cacheBytes
	err := e.Accept(ctx, kubePack("/p2", kubeTags{pod: "pod2", host: "h", useKube: true, seq: 0}))
	if !errors.Is(err, protocol.ErrLimit) || e.Stats().KubeLimited != 1 || e.cacheBytes != before || e.kube.managers["pod2"] != nil {
		t.Fatalf("manager bound: %v %+v", err, e.Stats())
	}
	// entries: 4 used of 6; a recovery needs 2 -> ok; the next needs 2 more -> limit.
	if err := e.Accept(ctx, kubePack("/q", kubeTags{pod: "pod0", host: "h9", useKube: true, seq: 9})); err != nil && !errors.Is(err, protocol.ErrLimit) {
		t.Fatal(err)
	}
	if e.kube.entries != 6 {
		t.Fatalf("entries %d", e.kube.entries)
	}
	if err := e.Accept(ctx, kubePack("/r", kubeTags{pod: "pod0", host: "h8", useKube: true, seq: 8})); !errors.Is(err, protocol.ErrLimit) {
		t.Fatalf("entry bound: %v", err)
	}
	// Saturated managers, entries and cache budget: the heartbeat of an
	// object whose seq is confirmed (seqHost[kubeSeq] == host) adds nothing
	// and must be accepted, or the object would cycle INACTIVE/ACTIVATED.
	limited := e.Stats().KubeLimited
	e.mu.Lock()
	e.config.MaxCacheBytes = e.cacheBytes + e.ringBytes
	e.mu.Unlock()
	for i := 0; i < 2; i++ {
		now = now.Add(time.Second)
		for j := 0; j < 2; j++ {
			if err := e.Accept(ctx, kubePack(fmt.Sprint("/p", j), kubeTags{pod: fmt.Sprint("pod", j), host: "h", useKube: true, seq: int64(j)})); err != nil {
				t.Fatalf("confirmed heartbeat /p%d: %v", j, err)
			}
		}
	}
	if st := e.Stats(); st.KubeLimited != limited || e.kube.entries != 6 || len(e.kube.managers) != 2 {
		t.Fatalf("confirmed heartbeat changed kube state: %+v entries=%d", st, e.kube.entries)
	}
	if st := e.objState[storage.CounterHash("/p0")]; st == nil || !st.alive {
		t.Fatalf("object state %+v", st)
	}
}

// A held host (initial record younger than 15s) adds nothing either: its
// repeated init heartbeat is held again, not rejected, at a full entry bound.
func TestKubeHeldHeartbeatAtEntryBound(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.MaxCounters = 2 })
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := e.Accept(ctx, kubePack("/a", kubeTags{pod: "p", host: "a", useKube: true, seq: -1})); err != nil {
			t.Fatalf("init %d: %v", i, err)
		}
		now = now.Add(time.Second)
	}
	if st := e.Stats(); st.KubeHeld != 3 || st.KubeLimited != 0 || e.kube.entries != 2 {
		t.Fatalf("held heartbeats %+v entries=%d", st, e.kube.entries)
	}
	// A second host needs two more keys: rejected without any change.
	if err := e.Accept(ctx, kubePack("/b", kubeTags{pod: "p", host: "b", useKube: true, seq: -1})); !errors.Is(err, protocol.ErrLimit) || e.kube.initial["b"] != nil {
		t.Fatalf("new host at the bound: %v", err)
	}
}

func TestKubeInitialFIFOBoundAndAccounting(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.MaxCounters = 1 << 20 })
	ctx := context.Background()
	base := e.cacheBytes
	pods := 200
	for i := 0; i <= kubeInitialMax; i++ {
		k := kubeTags{pod: fmt.Sprint("pod", i%pods), host: fmt.Sprint("host", i), useKube: true, seq: -1}
		if err := e.Accept(ctx, kubePack(fmt.Sprint("/o", i), k)); err != nil {
			t.Fatal(err)
		}
	}
	if len(e.kube.initial) != kubeInitialMax || e.kube.initial["host0"] != nil || e.Stats().KubeInitialEvicted != 1 {
		t.Fatalf("initial FIFO: %d %+v", len(e.kube.initial), e.Stats())
	}
	if st := e.Stats().KubeSeqCalls(); st.Enqueued != kubeSeqQueueSize || st.Dropped != uint64(kubeInitialMax+1-kubeSeqQueueSize) {
		t.Fatalf("push queue saturation %+v", st)
	}
	// Releasing every host returns the accounted bytes exactly.
	e.mu.Lock()
	for i := 0; i <= kubeInitialMax; i++ {
		e.objectInactivated(fmt.Sprint("pod", i%pods), fmt.Sprint("host", i), true)
	}
	e.mu.Unlock()
	if e.cacheBytes != base || len(e.kube.managers) != 0 || e.kube.entries != 0 || len(e.kube.initial) != 0 {
		t.Fatalf("accounting %d != %d managers=%d entries=%d", e.cacheBytes, base, len(e.kube.managers), e.kube.entries)
	}
}

// getNewPodSeq gives -1 once 0..9999 are taken and still pushes it.
func TestKubeSeqExhausted(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.MaxCounters = 1 << 20 })
	e.mu.Lock()
	m := &podSeqManager{hostSeq: map[string]int32{}, seqHost: map[int32]string{}}
	e.kube.managers["p"] = m
	for i := int32(0); i < kubeSeqSearch; i++ {
		e.setSeqHost(m, i, fmt.Sprint("x", i))
	}
	e.mu.Unlock()
	if err := e.Accept(context.Background(), kubePack("/n", kubeTags{pod: "p", host: "new", useKube: true, seq: -1})); err != nil {
		t.Fatal(err)
	}
	if calls := takeCalls(e.kubeCalls); len(calls) != 1 || seqOf(t, calls[0]) != -1 {
		t.Fatalf("exhausted %+v", calls)
	}
}
