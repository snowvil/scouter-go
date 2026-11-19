package telegraf

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

type accepted struct {
	pack protocol.Pack
	peer string
}

type fakeAcceptor struct {
	mu    sync.Mutex
	packs []accepted
	err   error
}

func (f *fakeAcceptor) AcceptFrom(_ context.Context, p protocol.Pack, peer string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if _, err := protocol.EncodePack(p, protocol.Limits{}); err != nil {
		panic(err)
	}
	f.packs = append(f.packs, accepted{p, peer})
	return nil
}

func (f *fakeAcceptor) take() []accepted {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.packs
	f.packs = nil
	return out
}

// fakeRegistrar is an in-memory counter catalog.
type fakeRegistrar struct {
	mu       sync.Mutex
	families map[string]FamilyDef
	types    map[string]ObjectTypeDef
	notified int
	failAdd  bool
}

func newFakeRegistrar() *fakeRegistrar {
	return &fakeRegistrar{families: map[string]FamilyDef{}, types: map[string]ObjectTypeDef{}}
}

func (r *fakeRegistrar) ObjectType(name string) (ObjectTypeDef, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.types[name]
	if !ok {
		return ObjectTypeDef{}, false
	}
	t.Counters = append(append([]CounterDef(nil), t.Counters...), r.families[t.Family].Counters...)
	return t, true
}
func (r *fakeRegistrar) HasFamily(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.families[name]
	return ok
}
func (r *fakeRegistrar) AddFamilyAndObjectType(f FamilyDef, t ObjectTypeDef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.families[f.Name], r.types[t.Name] = f, t
	return nil
}
func (r *fakeRegistrar) EditObjectType(t ObjectTypeDef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.types[t.Name]
	t.Counters = append(old.Counters, t.Counters...)
	r.types[t.Name] = t
	return nil
}
func (r *fakeRegistrar) SafelyAddFamily(f FamilyDef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.families[f.Name] = f
	return nil
}
func (r *fakeRegistrar) SafelyAddObjectType(t ObjectTypeDef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.types[t.Name] = t
	return nil
}
func (r *fakeRegistrar) AddFamilyCounter(family string, c CounterDef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failAdd {
		return errors.New("disk full")
	}
	f := r.families[family]
	f.Name = family
	f.Counters = upsertCounter(f.Counters, c)
	r.families[family] = f
	return nil
}
func (r *fakeRegistrar) NotifyClients() {
	r.mu.Lock()
	r.notified++
	r.mu.Unlock()
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func quietLogf(string, ...any) {}

func TestInputRegistrationFlow(t *testing.T) {
	now := time.Unix(1700000000, 0)
	cfg := sampleConfig(t, now)
	acc, reg := &fakeAcceptor{}, newFakeRegistrar()
	in := NewInput(cfg, reg, acc, Options{Clock: func() time.Time { return now }, Logf: quietLogf})
	defer in.Close()
	body := "cpu,cpu=cpu-total,host=h1 usage_user=1.5,usage_system=2 1700000000000000000\n"
	if err := in.Handle(context.Background(), body, "10.1.2.3"); err != nil {
		t.Fatal(err)
	}
	if len(acc.take()) != 0 || in.Stats().PendingObjType != 1 {
		t.Fatal("a new object type is registered, not stored (Q3)")
	}
	waitFor(t, func() bool { _, ok := reg.ObjectType("HOST_NEW"); return ok })
	reg.mu.Lock()
	fam := reg.families["X$HOST_METRIC"]
	typ := reg.types["HOST_NEW"]
	notified := reg.notified
	reg.mu.Unlock()
	if fam.Master != "cpu_user_cpu-total" || len(fam.Counters) != 2 || fam.Counters[0].Unit != "%" || fam.Counters[0].Total || typ.Icon != "linux" || typ.DisplayName != "HOST_NEW" || typ.Family != "X$HOST_METRIC" || notified != 1 {
		t.Fatalf("%+v %+v %d", fam, typ, notified)
	}
	if err := in.Handle(context.Background(), body, "10.1.2.3"); err != nil {
		t.Fatal(err)
	}
	got := acc.take()
	if len(got) != 2 {
		t.Fatalf("%+v", got)
	}
	obj := got[0].pack.(*protocol.ObjectPack)
	perf := got[1].pack.(*protocol.PerfCounterPack)
	if obj.Address != "10.1.2.3" || got[0].peer != "10.1.2.3" || obj.ObjName != "/h1/X$LINUX_NEW" || perf.Time != now.UnixMilli() || len(perf.Data.Entries) != 2 {
		t.Fatalf("%+v %+v", obj, perf)
	}
	// A new counter for the existing type is registered first (line dropped).
	if err := in.Handle(context.Background(), "cpu,cpu=cpu-total,host=h1 usage_iowait=3 1700000002000000000", "a.b"); err != nil {
		t.Fatal(err)
	}
	if len(acc.take()) != 0 || in.Stats().PendingCounter != 1 {
		t.Fatal("new counter line must be dropped")
	}
	waitFor(t, func() bool {
		t2, _ := reg.ObjectType("HOST_NEW")
		_, has := t2.Counter("cpu_iowait_cpu-total")
		return has
	})
	if err := in.Handle(context.Background(), "cpu,cpu=cpu-total,host=h1 usage_iowait=3 1700000004000000000", "a.b"); err != nil {
		t.Fatal(err)
	}
	if got := acc.take(); len(got) != 2 || got[0].peer != "0.0.0.0" || got[0].pack.(*protocol.ObjectPack).Address != "a.b" {
		t.Fatalf("%+v", got)
	}
	waitFor(t, func() bool { return in.Stats().Notifies >= 2 })
}

func TestInputPayloadRules(t *testing.T) {
	now := time.Unix(1700000000, 0)
	cfg := sampleConfig(t, now)
	acc := &fakeAcceptor{}
	var logs []string
	var logMu sync.Mutex
	in := NewInput(cfg, nil, acc, Options{Clock: func() time.Time { return now }, Logf: func(f string, a ...any) {
		logMu.Lock()
		logs = append(logs, fmt.Sprintf(f, a...))
		logMu.Unlock()
	}})
	defer in.Close()
	ctx := context.Background()
	// Exactly 1000 lines are processed; 1001 drop the payload (D7).
	lines := strings.Repeat("mem,host=h used_percent=1 1\n", MaxLines)
	in.Handle(ctx, lines, "127.0.0.1")
	if got := acc.take(); len(got) != 2 || in.Stats().DuplicateLines != MaxLines-1 {
		t.Fatalf("dedupe: %d %+v", len(got), in.Stats())
	}
	in.Handle(ctx, lines+"mem,host=h2 used_percent=1 1", "127.0.0.1")
	if got := acc.take(); len(got) != 0 || in.Stats().OversizedPayloads != 1 {
		t.Fatal("oversized payload must be dropped as a whole")
	}
	// The last line of a series wins; other series are kept in order.
	in.Handle(ctx, "mem,host=a used_percent=1 1\r\nmem,host=b used_percent=2 1\rmem,host=a used_percent=3 1", "127.0.0.1")
	got := acc.take()
	if len(got) != 4 || got[0].pack.(*protocol.ObjectPack).ObjName != "/a/X$LINUX_NEW" || got[1].pack.(*protocol.PerfCounterPack).Data.Entries[0].Value.Double != 3 {
		t.Fatalf("%+v", got)
	}
	in.Handle(ctx, "unknown,host=a v=1 1\ncpu,host=a,cpu=cpu0 usage_user=1 1\ncpu,host=a,cpu=cpu-total usage_user=x1 1\n"+strings.Repeat("x", MaxLineBytes+1), "127.0.0.1")
	s := in.Stats()
	if s.UnknownMeasurement != 1 || s.Filtered != 1 || s.ParseErrors != 1 || s.LongLines != 1 || s.UnregisteredTypes != 1 {
		t.Fatalf("%+v", s)
	}
	acc.err = errors.New("queue full")
	in.Handle(ctx, "mem,host=z used_percent=1 1", "127.0.0.1")
	if in.Stats().AcceptErrors != 2 {
		t.Fatal(in.Stats())
	}
	disabled := *cfg
	disabled.Enabled = false
	in.SetConfig(&disabled)
	in.Handle(ctx, "mem,host=z used_percent=1 1", "127.0.0.1")
	if in.Stats().DisabledRequests != 1 || in.Enabled() {
		t.Fatal("disabled input")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	in.SetConfig(cfg)
	if err := in.Handle(cancelled, "mem,host=z used_percent=1 1", "127.0.0.1"); err == nil || in.Stats().CancelledLines != 1 {
		t.Fatal("cancelled context")
	}
	logMu.Lock()
	defer logMu.Unlock()
	if len(logs) == 0 {
		t.Fatal("expected TG010 and debug logs")
	}
}

// blockingRegistrar holds SafelyAddFamily until released.
type blockingRegistrar struct {
	*fakeRegistrar
	entered, release chan struct{}
}

func (r *blockingRegistrar) SafelyAddFamily(f FamilyDef) error {
	r.entered <- struct{}{}
	<-r.release
	return r.fakeRegistrar.SafelyAddFamily(f)
}

func TestInputQueueFullAndClose(t *testing.T) {
	now := time.Unix(1700000000, 0)
	cfg := sampleConfig(t, now)
	reg := &blockingRegistrar{newFakeRegistrar(), make(chan struct{}), make(chan struct{})}
	in := NewInput(cfg, reg, &fakeAcceptor{}, Options{QueueSize: 1, Clock: func() time.Time { return now }, Logf: quietLogf})
	ctx := context.Background()
	in.Handle(ctx, "mem,host=h0 used_percent=1 1", "127.0.0.1")
	<-reg.entered // the worker holds the first registration
	in.Handle(ctx, "mem,host=h1 used_percent=1 1", "127.0.0.1")
	in.Handle(ctx, "mem,host=h2 used_percent=1 1", "127.0.0.1")
	if s := in.Stats(); s.PendingObjType != 3 || s.RegisterQueueFull != 1 {
		t.Fatalf("%+v", s)
	}
	close(reg.release)
	go func() {
		for range reg.entered {
		}
	}()
	residual := in.Close()
	if residual > 1 || in.Close() != residual {
		t.Fatal(residual)
	}
	close(reg.entered)
	if in.Stats().Registered < 1 {
		t.Fatal(in.Stats())
	}
}

func TestInputCounterAddErrorAndEqualIgnore(t *testing.T) {
	now := time.Unix(1700000000, 0)
	cfg := sampleConfig(t, now)
	reg := newFakeRegistrar()
	reg.types["HOST_NEW"] = ObjectTypeDef{Name: "HOST_NEW", Family: "X$HOST_METRIC"}
	reg.families["X$HOST_METRIC"] = FamilyDef{Name: "X$HOST_METRIC"}
	param := addCounterParam{objectType: reg.types["HOST_NEW"], counter: CounterDef{Name: "x", DisplayName: "x"}}
	in := NewInput(cfg, reg, &fakeAcceptor{}, Options{Clock: func() time.Time { return now }, Logf: quietLogf})
	defer in.Close()
	reg.mu.Lock()
	reg.failAdd = true
	reg.mu.Unlock()
	in.Handle(context.Background(), "mem,host=h used_percent=1 1", "127.0.0.1")
	waitFor(t, func() bool { return in.Stats().CounterAddErrors == 1 })
	reg.mu.Lock()
	reg.failAdd = false
	reg.mu.Unlock()
	// Two different objects ask for the same counter: the second is ignored
	// by prevAddedCounter when it is semantically equal within 5s.
	prev := newFIFO[string, prevAdded](10)
	if !in.addCounter(param, &prev) || in.addCounter(param, &prev) {
		t.Fatal("equal counter within 5s must be ignored")
	}
}
