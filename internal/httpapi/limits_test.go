package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// counterBody builds a /counter body of about maxBytes; counter names cycle
// over distinct names (one object).
func counterBody(maxBytes, distinct int) (string, int) {
	var b strings.Builder
	b.WriteString(`{"object":{"host":"h"},"counters":[`)
	n := 0
	for b.Len() < maxBytes-64 {
		if n > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"name":"c%d","value":%d}`, n%distinct, n)
		n++
	}
	b.WriteString(`]}`)
	return b.String(), n
}

// A 1 MiB body with ~40k counters used to take seconds (MapValue.put scanned
// all entries for every name). Names are now indexed, and more distinct names
// than one PerfCounterPack can carry stop the element early.
func TestCounterManyNamesIsLinear(t *testing.T) {
	api, engine := newAPI(t, Config{})
	body, n := counterBody(DefaultMaxBodyBytes, 4000)
	if n < 30000 {
		t.Fatal(n)
	}
	start := time.Now()
	rec := do(api, "POST", "/counter", body, "Content-Type", "application/json")
	elapsed := time.Since(start)
	got := engine.take()
	if rec.Code != 200 || len(got) != 2 {
		t.Fatalf("%d %d", rec.Code, len(got))
	}
	entries := got[1].pack.(*protocol.PerfCounterPack).Data.Entries
	if len(entries) != 4000 || entries[0].Key != "c0" || entries[0].Value.Float != float32((n-1)/4000*4000) { // last value wins
		t.Fatalf("%d %+v", len(entries), entries[0])
	}
	if elapsed > 3*time.Second {
		t.Fatalf("%d counters took %v", n, elapsed)
	}
	t.Logf("%d counters over 4000 names in %v", n, elapsed)

	body, n = counterBody(DefaultMaxBodyBytes, 1<<30) // every name distinct
	start = time.Now()
	rec = do(api, "POST", "/counter", body, "Content-Type", "application/json")
	elapsed = time.Since(start)
	got = engine.take()
	if rec.Code != 200 || len(got) != 1 || api.Stats().CounterTooMany != 1 || elapsed > 3*time.Second {
		t.Fatalf("%d packs=%d %+v %v", rec.Code, len(got), api.Stats(), elapsed)
	}
	t.Logf("%d distinct counters rejected in %v", n, elapsed)

	body, _ = counterBody(64*MaxCounterNames, MaxCounterNames)
	do(api, "POST", "/counter", body)
	if got = engine.take(); len(got) != 2 || len(got[1].pack.(*protocol.PerfCounterPack).Data.Entries) != MaxCounterNames {
		t.Fatalf("exactly MaxCounterNames must be sent: %d", len(got))
	}

	// Duplicate names keep the first position and the last value.
	do(api, "POST", "/counter", `{"object":{"host":"h"},"counters":[{"name":"a","value":1},{"name":"b","value":2},{"name":"a","value":3}]}`)
	e := engine.take()[1].pack.(*protocol.PerfCounterPack).Data.Entries
	if len(e) != 2 || e[0].Key != "a" || e[0].Value.Float != 3 || e[1].Key != "b" {
		t.Fatalf("%+v", e)
	}
}

type gateEngine struct {
	entered chan struct{}
	release chan struct{}
}

func (g *gateEngine) AcceptFrom(ctx context.Context, p protocol.Pack, _ string) error {
	g.entered <- struct{}{}
	<-g.release
	return nil
}

func TestMaxInFlight(t *testing.T) {
	g := &gateEngine{entered: make(chan struct{}), release: make(chan struct{})}
	api, err := New(g, Config{MaxInFlight: 1, QueueWait: -1, Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if rec := do(api, "POST", "/counter", `{"object":{"host":"h"}}`); rec.Code != 200 {
			t.Error(rec.Code)
		}
	}()
	<-g.entered // the first request holds the only slot
	if rec := do(api, "POST", "/telegraf", "m,h=1 v=1 1"); rec.Code != http.StatusServiceUnavailable {
		t.Fatal(rec.Code)
	}
	close(g.release)
	wg.Wait()
	if s := api.Stats(); s.Busy != 1 || s.Requests != 2 {
		t.Fatalf("%+v", s)
	}
	// The slot is released: a new request is served again.
	go func() { <-g.entered }()
	if rec := do(api, "POST", "/counter", `{"object":{"host":"h"}}`); rec.Code != 200 {
		t.Fatal(rec.Code)
	}
}

func TestDecodeBodySizes(t *testing.T) {
	latin := make([]byte, 1000)
	for i := range latin {
		latin[i] = byte(0x80 + i%0x80)
	}
	s, err := decodeBody("", latin)
	if err != nil || len(s) != 2000 || []rune(s)[0] != 0x80 {
		t.Fatal(len(s), err)
	}
	allocs := testing.AllocsPerRun(10, func() { decodeBody("", latin) })
	if allocs > 2 {
		t.Fatalf("latin decode allocs %v", allocs)
	}
}

// A request waits up to QueueWait for a slot: it is served when one frees in
// time, answered 503 with Retry-After after the wait, and not counted as busy
// when its client leaves first. The 503 line is rate-limited.
func TestQueueWait(t *testing.T) {
	g := &gateEngine{entered: make(chan struct{}), release: make(chan struct{})}
	var lines []string
	var mu sync.Mutex
	api, err := New(g, Config{MaxInFlight: 1, QueueWait: 100 * time.Millisecond, Logf: func(f string, a ...any) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, fmt.Sprintf(f, a...))
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	hold := func() *sync.WaitGroup {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rec := do(api, "POST", "/counter", `{"object":{"host":"h"}}`); rec.Code != 200 {
				t.Error(rec.Code)
			}
		}()
		<-g.entered
		return &wg
	}
	wg := hold()
	for range 2 {
		rec := do(api, "POST", "/telegraf", "m,h=1 v=1 1")
		if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
			t.Fatal(rec.Code, rec.Header())
		}
	}
	mu.Lock()
	if len(lines) != 1 || !strings.Contains(lines[0], "503") {
		t.Fatalf("busy lines %q", lines)
	}
	mu.Unlock()
	// The holder ends within the wait: the waiting request is served.
	served := make(chan int, 1)
	go func() { served <- do(api, "POST", "/counter", `{"object":{"host":"h"}}`).Code }()
	for api.Stats().Waited != 3 {
		time.Sleep(time.Millisecond)
	}
	g.release <- struct{}{}
	<-g.entered
	g.release <- struct{}{}
	if code := <-served; code != 200 {
		t.Fatal(code)
	}
	wg.Wait()
	// A client that leaves while waiting is not busy.
	wg = hold()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest("POST", "/counter", strings.NewReader("{}")).WithContext(ctx)
	req.RemoteAddr = "127.0.0.1:1"
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	g.release <- struct{}{}
	wg.Wait()
	if s := api.Stats(); s.Busy != 2 || s.Waited != 4 || s.Abandoned != 1 || s.InFlight != 0 {
		t.Fatalf("%+v", s)
	}
}

type panicEngine struct{}

func (panicEngine) AcceptFrom(context.Context, protocol.Pack, string) error {
	panic(http.ErrAbortHandler)
}

// A handler panic (net/http recovers it) gives the MaxInFlight slot back.
func TestInFlightSlotReleasedOnPanic(t *testing.T) {
	api, err := New(panicEngine{}, Config{MaxInFlight: 1, QueueWait: -1, Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	for range 2 {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("no panic")
				}
			}()
			do(api, "POST", "/counter", `{"object":{"host":"h"}}`)
		}()
	}
	if s := api.Stats(); s.InFlight != 0 || s.Busy != 0 {
		t.Fatalf("slot leaked: %+v", s)
	}
}
