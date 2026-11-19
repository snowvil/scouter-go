package counters

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/telegraf"
)

type discardAcceptor struct {
	mu    sync.Mutex
	packs int
}

func (a *discardAcceptor) AcceptFrom(context.Context, protocol.Pack, string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.packs++
	return nil
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func telegrafSampleConfig(t *testing.T, now time.Time) *telegraf.Config {
	t.Helper()
	data, err := os.ReadFile("../server/telegraf-sample.xml")
	if err != nil {
		t.Fatal(err)
	}
	f, _, err := telegraf.ParseFile(data)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := telegraf.DefaultConfig().ApplyFile(f, now)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// TestRegistrarTelegrafInput drives the real telegraf.Input against a Store
// of a new installation (no counters.site.xml: Java NullPointerException).
func TestRegistrarTelegrafInput(t *testing.T) {
	s, dir := openStore(t)
	var notified atomic.Int64
	reg := NewRegistrar(s, func() { notified.Add(1) })
	now := time.Unix(1700000000, 0)
	in := telegraf.NewInput(telegrafSampleConfig(t, now), reg, &discardAcceptor{}, telegraf.Options{Clock: func() time.Time { return now }, Logf: func(string, ...any) {}})
	defer in.Close()
	body := "cpu,cpu=cpu-total,host=h1 usage_user=1.5,usage_system=2 1700000000000000000\n"
	if err := in.Handle(context.Background(), body, "10.1.2.3"); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { _, ok := reg.ObjectType("HOST_NEW"); return ok })
	typ, _ := reg.ObjectType("HOST_NEW")
	if typ.Family != "X$HOST_METRIC" || typ.Icon != "linux" || len(typ.Counters) != 2 {
		t.Fatalf("%+v", typ)
	}
	eventually(t, func() bool { return notified.Load() >= 1 })
	// A new counter for the registered type goes through AddFamilyCounter.
	if err := in.Handle(context.Background(), "cpu,cpu=cpu-total,host=h1 usage_iowait=3 1700000002000000000", "10.1.2.3"); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		t2, _ := reg.ObjectType("HOST_NEW")
		_, ok := t2.Counter("cpu_iowait_cpu-total")
		return ok
	})
	disk, err := os.ReadFile(filepath.Join(dir, CustomFileName))
	if err != nil || !strings.Contains(string(disk), `name="cpu_iowait_cpu-total"`) || !strings.Contains(string(disk), `<ObjectType name="HOST_NEW"`) {
		t.Fatalf("%v\n%s", err, disk)
	}
	if st := in.Stats(); st.RegisterErrors != 0 || st.CounterAddErrors != 0 {
		t.Fatalf("%+v", st)
	}
}

// TestRegistrarRegisterAndMerge covers the /register calls and the
// SafelyAddFamily merge (Java replaces the family: Q2).
func TestRegistrarRegisterAndMerge(t *testing.T) {
	s, _ := openStore(t)
	reg := NewRegistrar(s, nil)
	reg.NotifyClients() // nil notify: no-op
	if reg.HasFamily("redis") {
		t.Fatal("family before register")
	}
	fam := telegraf.FamilyDef{Name: "redis", Master: "ops", Counters: []telegraf.CounterDef{{Name: "ops", DisplayName: "Ops", Unit: "cnt", All: true, Total: true}}}
	if err := reg.AddFamilyAndObjectType(fam, telegraf.ObjectTypeDef{Name: "redis", DisplayName: "Redis", Family: "redis"}); err != nil {
		t.Fatal(err)
	}
	typ, ok := reg.ObjectType("redis")
	if !ok || typ.DisplayName != "Redis" || len(typ.Counters) != 1 || typ.Counters[0] != fam.Counters[0] || !reg.HasFamily("redis") {
		t.Fatalf("%+v", typ)
	}
	// Existing type: only new counters, as own counters (shadowing order).
	edit := telegraf.ObjectTypeDef{Name: "redis", DisplayName: "Redis2", Family: "redis", Counters: []telegraf.CounterDef{{Name: "mem", DisplayName: "Mem", Unit: "MB", All: true, Total: true}, {Name: "ops", DisplayName: "Own ops", All: true}}}
	if err := reg.EditObjectType(edit); err != nil {
		t.Fatal(err)
	}
	typ, _ = reg.ObjectType("redis")
	if typ.DisplayName != "Redis2" || len(typ.Counters) != 2 || typ.Counters[0].Name != "mem" || typ.Counters[1].DisplayName != "Own ops" {
		t.Fatalf("%+v", typ)
	}
	if c, _ := typ.Counter("ops"); c.DisplayName != "Own ops" {
		t.Fatal("own counter must shadow the family counter", c)
	}
	// Two measurements share a family: the second keeps the first's counters.
	if err := reg.SafelyAddFamily(telegraf.FamilyDef{Name: "X$SHARED", Master: "cpu_user", Counters: []telegraf.CounterDef{{Name: "cpu_user", All: true}}}); err != nil {
		t.Fatal(err)
	}
	if err := reg.SafelyAddObjectType(telegraf.ObjectTypeDef{Name: "T1", DisplayName: "T1", Family: "X$SHARED", Icon: "linux"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.SafelyAddFamily(telegraf.FamilyDef{Name: "X$SHARED", Master: "mem_used", Counters: []telegraf.CounterDef{{Name: "mem_used", All: true}}}); err != nil {
		t.Fatal(err)
	}
	if err := reg.SafelyAddObjectType(telegraf.ObjectTypeDef{Name: "T2", DisplayName: "T2", Family: "X$SHARED"}); err != nil {
		t.Fatal(err)
	}
	f := s.Engine().Family("X$SHARED")
	if f.Master != "cpu_user" || len(f.Counters()) != 2 {
		t.Fatalf("merge lost counters: %s %d", f.Master, len(f.Counters()))
	}
	t1, _ := reg.ObjectType("T1")
	if _, ok := t1.Counter("cpu_user"); !ok {
		t.Fatal("T1 lost its family counter")
	}
	if err := reg.AddFamilyCounter("X$SHARED", telegraf.CounterDef{Name: "cpu_user", DisplayName: "CPU user", Unit: "%", All: true}); err != nil {
		t.Fatal(err)
	}
	t2, _ := reg.ObjectType("T2")
	if c, _ := t2.Counter("cpu_user"); c.DisplayName != "CPU user" || c.Unit != "%" {
		t.Fatal(c)
	}
	if err := reg.AddFamilyCounter("missing", telegraf.CounterDef{Name: "x"}); err == nil {
		t.Fatal("unknown family accepted")
	}
	if err := reg.EditObjectType(telegraf.ObjectTypeDef{Name: "redis", Family: "missing"}); err == nil {
		t.Fatal("unknown family accepted")
	}
}

// TestRegistrarNotifyNeverBlocks: a blocked notify must not block callers,
// requests are coalesced and at most one notifier runs.
func TestRegistrarNotifyNeverBlocks(t *testing.T) {
	s, _ := openStore(t)
	release := make(chan struct{})
	var calls, active, maxActive atomic.Int64
	reg := NewRegistrar(s, func() {
		n := active.Add(1)
		for {
			m := maxActive.Load()
			if n <= m || maxActive.CompareAndSwap(m, n) {
				break
			}
		}
		calls.Add(1)
		<-release
		active.Add(-1)
	})
	// Block the notifier first, so every request below arrives while a
	// notification runs. Without this, a late-scheduled notifier could take
	// all 50 requests in its first call and never make the second one.
	reg.NotifyClients()
	eventually(t, func() bool { return calls.Load() == 1 })
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); reg.NotifyClients() }()
		}
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("NotifyClients blocked")
	}
	eventually(t, func() bool { return calls.Load() == 1 })
	close(release)
	// The requests made during the blocked call are delivered by one more call.
	eventually(t, func() bool { return calls.Load() == 2 && !reg.running.Load() })
	if maxActive.Load() != 1 {
		t.Fatal("concurrent notifiers", maxActive.Load())
	}
	reg.NotifyClients()
	eventually(t, func() bool { return calls.Load() == 3 && !reg.running.Load() })
}
