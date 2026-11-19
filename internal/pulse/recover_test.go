package pulse

import (
	"context"
	"strings"
	"sync"
	"testing"
)

// A panic while converting one task must not kill the server: the task counts
// as an error, its bytes are released and the worker takes the next task
// (the Java plugin dispatcher catches Exception per task).
func TestDispatcherWorkerRecoversPanic(t *testing.T) {
	opts, dict, sink, _ := testOptions(t, map[string]string{KeyKafkaDispatchWorkerCount: "1"})
	lookup := dict.lookup
	opts.Lookup = func(ctx context.Context, date, div string, hash int32) (string, bool, error) {
		if hash == 999 {
			panic("converter bug")
		}
		return lookup(ctx, date, div, hash)
	}
	var events []Event
	var mu sync.Mutex
	opts.OnEvent = func(e Event) { mu.Lock(); events = append(events, e); mu.Unlock() }
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 3; i++ {
		d.OfferXLog(xlogPack(999, 202, i, 200))
	}
	d.OfferXLog(xlogPack(100, 202, 4, 200))
	if err := d.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := d.Stats().XLog
	if s.Errors != 3 || s.Sent != 1 || s.PendingBytes != 0 || len(sink.all()) != 1 {
		t.Fatalf("%+v", s)
	}
	mu.Lock()
	defer mu.Unlock()
	panics := 0
	for _, e := range events {
		if e.Kind == "error" && e.Err != nil && strings.Contains(e.Err.Error(), "converter bug") {
			panics++
		}
	}
	if panics != 1 {
		t.Fatalf("want one rate-limited panic event, got %d: %+v", panics, events)
	}
}
