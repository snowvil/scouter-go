package service

import (
	"reflect"
	"strings"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func TestParseGoroutineDump(t *testing.T) {
	dump := "goroutine 1 [running]:\n" +
		"main.main()\n\t/src/main.go:10 +0x1d\n\n" +
		"goroutine 7 gp=0xc000 m=nil [chan receive, 3 minutes, locked to thread]:\n" +
		"example.com/p.(*T).wait(0xc0001, {0x1, 0x2})\n\t/src/p/t.go:42 +0x55\n" +
		"example.com/p.worker(...)\n\t/src/p/w.go:9\n" +
		"created by example.com/p.Start in goroutine 1\n\t/src/p/start.go:5 +0x9\n\n" +
		"goroutine 9 [select]:\n" +
		"deep.f()\n\t/src/d.go:1 +0x1\n...additional frames elided...\n" +
		"created by deep.g in goroutine 1\n\t/src/d.go:2 +0x2\n\n" +
		"not a goroutine block\n"
	gs := parseGoroutineDump([]byte(dump))
	if len(gs) != 3 {
		t.Fatal(len(gs))
	}
	if gs[0].id != 1 || gs[0].state() != "running" || gs[0].name() != "goroutine 1 main.main" || !reflect.DeepEqual(gs[0].frames, []string{"main.main(/src/main.go:10)"}) {
		t.Fatalf("%#v", gs[0])
	}
	g := gs[1]
	if g.id != 7 || g.status != "chan receive, 3 minutes, locked to thread" || g.state() != "chan receive" || g.name() != "goroutine 7 example.com/p.worker" {
		t.Fatalf("%#v", g)
	}
	if !reflect.DeepEqual(g.frames, []string{"example.com/p.(*T).wait(/src/p/t.go:42)", "example.com/p.worker(/src/p/w.go:9)", "created by example.com/p.Start in goroutine 1 (/src/p/start.go:5)"}) {
		t.Fatalf("%#v", g.frames)
	}
	if gs[2].entry != "" || gs[2].name() != "goroutine 9" {
		t.Fatalf("elided entry %#v", gs[2])
	}
}

func parkedForThreadTest(ready chan<- struct{}, release <-chan struct{}) {
	close(ready)
	<-release
}

func TestServerThreadListAndDetailTCP(t *testing.T) {
	ready, release := make(chan struct{}), make(chan struct{})
	go parkedForThreadTest(ready, release)
	defer close(release)
	<-ready
	f := newConfigureFixture(t)
	conn, r, session := f.start(t)
	send(t, conn, "SERVER_THREAD_LIST", session, nil)
	m := singleMap(t, r)
	if !reflect.DeepEqual(keys(m), []string{"id", "name", "stat", "cpu"}) {
		t.Fatal(keys(m))
	}
	ids, names, stats, cpus := lookup(m, "id").Values, lookup(m, "name").Values, lookup(m, "stat").Values, lookup(m, "cpu").Values
	if len(ids) == 0 || len(ids) != len(names) || len(ids) != len(stats) || len(ids) != len(cpus) {
		t.Fatal(len(ids), len(names), len(stats), len(cpus))
	}
	parked := int64(0)
	for i := range ids {
		if ids[i].Type != protocol.ValueDecimal || names[i].Type != protocol.ValueText || stats[i].Type != protocol.ValueText || cpus[i].Type != protocol.ValueDecimal || cpus[i].Int != 0 {
			t.Fatal("types", i, ids[i], names[i], stats[i], cpus[i])
		}
		if !strings.HasPrefix(names[i].Text, "goroutine ") {
			t.Fatal(names[i].Text)
		}
		if strings.HasSuffix(names[i].Text, ".parkedForThreadTest") {
			parked = ids[i].Int
			if stats[i].Text != "chan receive" {
				t.Fatal("state", stats[i].Text)
			}
		}
	}
	if parked == 0 {
		t.Fatal("parked goroutine not listed")
	}
	send(t, conn, "SERVER_THREAD_DETAIL", session, newMap(entry("id", decimal(parked))))
	d := singleMap(t, r)
	wantKeys := []string{"Thread Id", "Thread Cpu Time", "Thread User Time", "Blocked Count", "Blocked Time", "Waited Count", "Waited Time", "Lock Owner Id", "Lock Name", "Lock Owner Name", "Thread Name", "Stack Trace", "State"}
	if !reflect.DeepEqual(keys(d), wantKeys) {
		t.Fatal(keys(d))
	}
	for i, k := range wantKeys {
		wantType := protocol.ValueDecimal
		if i >= 8 {
			wantType = protocol.ValueText
		}
		if lookup(d, k).Type != wantType {
			t.Fatal(k, lookup(d, k))
		}
	}
	if lookup(d, "Thread Id").Int != parked || lookup(d, "Lock Owner Id").Int != -1 || !strings.Contains(lookup(d, "Stack Trace").Text, "parkedForThreadTest(") || !strings.HasPrefix(lookup(d, "State").Text, "chan receive") {
		t.Fatal(d)
	}
	// id 0 / absent / unknown: ThreadUtil returns an empty MapPack item.
	for _, p := range []*protocol.MapPack{newMap(entry("id", decimal(0))), newMap(), newMap(entry("id", stringValue("1"))), newMap(entry("id", decimal(1<<62)))} {
		send(t, conn, "SERVER_THREAD_DETAIL", session, p)
		if m := singleMap(t, r); len(m.Entries) != 0 {
			t.Fatal(p, m)
		}
	}
}
