package pulse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// blockingLookup holds every worker inside the dictionary lookup until release
// is closed (or the dispatcher context ends), so offered tasks stay queued.
func blockingLookup(release <-chan struct{}, entered chan<- struct{}) TextLookup {
	return func(ctx context.Context, date, div string, hash int32) (string, bool, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
			return "", false, ctx.Err()
		}
		return "/host/app@1", true, nil
	}
}

// A stalled worker with large profiles queued must not hold more than the
// default profile queue byte budget, whatever the task count bound allows.
func TestDispatcherDefaultProfileBytesBound(t *testing.T) {
	opts, _, _, _ := testOptions(t, map[string]string{KeyKafkaDispatchWorkerCount: "1"})
	release, entered := make(chan struct{}), make(chan struct{}, 1)
	opts.Lookup = blockingLookup(release, entered)
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		d.Close(context.Background())
	}()
	const size = 1 << 20
	body := make([]byte, size)
	first := profilePack(100, 202, 1)
	first.Profile = body
	if !d.OfferProfile(first) {
		t.Fatal("first offer rejected")
	}
	<-entered
	accepted := 0
	for i := int64(2); i <= 96; i++ {
		p := profilePack(100, 202, i)
		p.Profile = body
		if d.OfferProfile(p) {
			accepted++
		}
	}
	// 1 in the worker + accepted queued, each holding its own 1MiB clone.
	if held := int64(accepted+1) * size; held > 64<<20 {
		t.Fatalf("profile queue holds %d MiB (accepted %d of 95 behind a stalled worker)", held>>20, accepted)
	}
	// 64MiB / (1MiB + struct) = 63 tasks, one of them in the worker.
	s := d.Stats().Profile
	if accepted < 60 || s.DroppedByBytes != uint64(95-accepted) || s.Dropped != s.DroppedByBytes || s.PendingBytes > s.MaxPendingBytes ||
		s.PendingBytes != int64(accepted+1)*(profilePackBytes+size) {
		t.Fatalf("accepted=%d stats=%+v", accepted, s)
	}
}

func TestDispatcherQueueBytesAccounting(t *testing.T) {
	opts, _, sink, clock := testOptions(t, map[string]string{KeyKafkaDispatchQueueCapacity: "64", KeyKafkaDispatchWorkerCount: "1"})
	release, entered := make(chan struct{}), make(chan struct{}, 1)
	opts.Lookup = blockingLookup(release, entered)
	const body = 1000
	cost := profilePackBytes + body
	opts.MaxProfileQueueBytes = 4*cost + cost/2 // room for 4 tasks
	var events []Event
	var mu sync.Mutex
	opts.OnEvent = func(e Event) { mu.Lock(); events = append(events, e); mu.Unlock() }
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	pack := func(txid int64) *protocol.ProfilePack {
		p := profilePack(100, 202, txid)
		p.Profile = make([]byte, body)
		return p
	}
	if !d.OfferProfile(pack(1)) {
		t.Fatal("first offer rejected")
	}
	<-entered
	accepted := 1
	for i := int64(2); i <= 10; i++ {
		if d.OfferProfile(pack(i)) {
			accepted++
		}
	}
	// A task held by the worker still counts against the budget.
	s := d.Stats().Profile
	if accepted != 4 || s.Enqueued != 4 || s.Pending != 3 || s.PendingBytes != 4*cost || s.MaxPendingBytes != opts.MaxProfileQueueBytes ||
		s.Dropped != 6 || s.DroppedByBytes != 6 {
		t.Fatalf("accepted=%d stats=%+v", accepted, s)
	}
	// A task larger than the whole budget can never fit.
	big := profilePack(100, 202, 11)
	big.Profile = make([]byte, opts.MaxProfileQueueBytes)
	if d.OfferProfile(big) || d.Stats().Profile.DroppedByBytes != 7 {
		t.Fatal("oversized profile accepted")
	}
	// The XLog queue has its own budget.
	if !d.OfferXLog(xlogPack(100, 202, 12, 200)) {
		t.Fatal("xlog offer rejected by the profile budget")
	}
	// One rate-limited "dropped" Event per queue per interval, with totals.
	mu.Lock()
	if len(events) != 1 || events[0].Kind != "dropped" || events[0].Queue != "xlog-profile" ||
		!strings.Contains(events[0].Detail, "queue bytes full") || !strings.Contains(events[0].Detail, fmt.Sprintf("bytes %d/%d", 4*cost, opts.MaxProfileQueueBytes)) {
		t.Fatalf("events %+v", events)
	}
	mu.Unlock()
	clock.Advance(eventInterval)
	d.OfferProfile(pack(13))
	mu.Lock()
	if len(events) != 2 || !strings.Contains(events[1].Detail, "dropped 8 (by bytes 8)") {
		t.Fatalf("events %+v", events)
	}
	mu.Unlock()
	// Finished tasks release their bytes.
	close(release)
	waitFor(t, func() bool { s := d.Stats(); return s.Profile.PendingBytes == 0 && s.XLog.PendingBytes == 0 })
	if err := d.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The zero-filled profiles fail step decoding; only completion matters.
	if s := d.Stats(); s.Profile.Sent+s.Profile.Errors+s.Profile.Filtered != 4 || s.XLog.Sent+s.XLog.Filtered != 1 ||
		len(sink.all()) != int(s.Profile.Sent+s.XLog.Sent) {
		t.Fatalf("after drain %+v", s)
	}
}

// Count-bound drops are not byte drops, and a forced Close releases the bytes
// of the discarded tasks.
func TestDispatcherQueueBytesCountDropAndForcedClose(t *testing.T) {
	opts, _, _, _ := testOptions(t, map[string]string{KeyKafkaDispatchQueueCapacity: "2", KeyKafkaDispatchWorkerCount: "1"})
	release, entered := make(chan struct{}), make(chan struct{}, 1)
	defer close(release)
	opts.Lookup = blockingLookup(release, entered)
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	x := xlogPack(100, 202, 1, 200)
	x.Text1, x.Extra = "abcdefgh", make([]byte, 100)
	if got, want := xlogTaskBytes(x), xlogPackBytes+4+8+100; got != want {
		t.Fatalf("xlog task bytes %d, want %d", got, want)
	}
	d.OfferXLog(x)
	<-entered
	for i := int64(2); i <= 5; i++ {
		d.OfferXLog(xlogPack(100, 202, i, 200))
	}
	s := d.Stats().XLog
	if s.Enqueued != 3 || s.Dropped != 2 || s.DroppedByBytes != 0 || s.PendingBytes != xlogTaskBytes(x)+2*xlogTaskBytes(xlogPack(0, 0, 0, 0)) {
		t.Fatalf("%+v", s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s := d.Stats().XLog; s.ShutdownDiscarded != 3 || s.PendingBytes != 0 {
		t.Fatalf("after forced close %+v", s)
	}
	if _, err := New(Options{Config: DefaultConfig(), Sink: &memSink{}, Lookup: opts.Lookup, Location: time.UTC, MaxProfileQueueBytes: -1}); !errors.Is(err, ErrDispatcherOptions) {
		t.Fatal("negative byte bound accepted")
	}
}

// A drop must not copy the pack: the bounds are checked before the clone.
func TestDispatcherDropDoesNotClone(t *testing.T) {
	opts, _, _, _ := testOptions(t, map[string]string{KeyKafkaDispatchWorkerCount: "1"})
	release, entered := make(chan struct{}), make(chan struct{}, 1)
	defer close(release)
	opts.Lookup = blockingLookup(release, entered)
	opts.MaxProfileQueueBytes = 1 << 20
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(context.Background())
	p := profilePack(100, 202, 1)
	p.Profile = make([]byte, 2<<20)
	if allocs := testing.AllocsPerRun(100, func() { d.OfferProfile(p) }); allocs != 0 {
		t.Fatalf("dropped offer allocated %v times", allocs)
	}
}

// Concurrent offers against running workers: the byte charge always returns
// to zero (run with -race).
func TestDispatcherQueueBytesConcurrent(t *testing.T) {
	opts, _, _, _ := testOptions(t, map[string]string{KeyKafkaDispatchQueueCapacity: "8", KeyKafkaDispatchWorkerCount: "2"})
	opts.MaxProfileQueueBytes = 6 * (profilePackBytes + 512)
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				p := profilePack(100, 202, int64(i+1))
				p.Profile = append(p.Profile, make([]byte, 512)...)
				d.OfferProfile(p)
				if s := d.Stats().Profile; s.PendingBytes > s.MaxPendingBytes || s.PendingBytes < 0 {
					t.Errorf("pending bytes %d of %d", s.PendingBytes, s.MaxPendingBytes)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := d.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := d.Stats().Profile
	if s.PendingBytes != 0 || s.Enqueued+s.Dropped != 2000 || s.Enqueued != s.Errors+s.Sent+s.Filtered {
		t.Fatalf("%+v", s)
	}
}

// /health marshals Stats as is (internal/server/http.go); the byte fields
// must stay exported.
func TestStatsJSONHasByteFields(t *testing.T) {
	b, err := json.Marshal(map[string]any{"dispatcher": Stats{Profile: QueueStats{PendingBytes: 1, MaxPendingBytes: 2, DroppedByBytes: 3}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"PendingBytes":1`, `"MaxPendingBytes":2`, `"DroppedByBytes":3`} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("%s missing in %s", want, b)
		}
	}
}

// The channel can fill between the count check and the send (a concurrent
// offer); the reserved bytes must then be returned.
func TestDispatcherSendFailureReleasesBytes(t *testing.T) {
	opts, _, _, _ := testOptions(t, map[string]string{KeyKafkaDispatchQueueCapacity: "1", KeyKafkaDispatchWorkerCount: "1"})
	release, entered := make(chan struct{}), make(chan struct{}, 1)
	opts.Lookup = blockingLookup(release, entered)
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	d.OfferXLog(xlogPack(100, 202, 1, 200))
	<-entered
	q := d.xlog
	if d.offer(q, 500, 2, func() task {
		q.items <- task{xlog: xlogPack(100, 202, 3, 200)} // the racing offer wins the slot
		return task{xlog: xlogPack(100, 202, 2, 200)}
	}) {
		t.Fatal("offer into a full channel accepted")
	}
	s := d.Stats().XLog
	if s.PendingBytes != xlogTaskBytes(xlogPack(0, 0, 0, 0)) || s.Dropped != 1 || s.DroppedByBytes != 0 {
		t.Fatalf("%+v", s)
	}
	close(release)
	if err := d.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s := d.Stats().XLog; s.PendingBytes != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestDispatcherSinkErrorReleasesBytes(t *testing.T) {
	opts, _, sink, _ := testOptions(t, nil)
	sink.err = errors.New("buffer full")
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(context.Background())
	d.OfferXLog(xlogPack(100, 202, 1, 200))
	waitFor(t, func() bool { s := d.Stats().XLog; return s.Failed == 1 && s.PendingBytes == 0 })
}

// Close racing with offers from several goroutines leaves no charge behind,
// whether Close drains or gives up (run with -race).
func TestDispatcherQueueBytesCloseDuringOffers(t *testing.T) {
	for _, forced := range []bool{false, true} {
		opts, _, _, _ := testOptions(t, map[string]string{KeyKafkaDispatchQueueCapacity: "16", KeyKafkaDispatchWorkerCount: "2"})
		d, err := New(opts)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 300; i++ {
					d.OfferXLog(xlogPack(100, 202, int64(i+1), 200))
					d.OfferProfile(profilePack(100, 202, int64(i+1)))
				}
			}()
		}
		ctx, cancel := context.WithCancel(context.Background())
		if forced {
			cancel()
		}
		d.Close(ctx)
		cancel()
		wg.Wait()
		if s := d.Stats(); s.XLog.PendingBytes != 0 || s.Profile.PendingBytes != 0 {
			t.Fatalf("forced=%v %+v", forced, s)
		}
	}
}

func TestDispatcherNegativeXLogBytesRejected(t *testing.T) {
	opts, _, _, _ := testOptions(t, nil)
	opts.MaxXLogQueueBytes = -1
	if _, err := New(opts); !errors.Is(err, ErrDispatcherOptions) {
		t.Fatal("negative xlog byte bound accepted")
	}
}

// The drop Event window survives a clock that goes backwards, and a drop
// Event the owner could not queue is retried on the next drop.
func TestDispatcherDropEventClockAndRetry(t *testing.T) {
	opts, _, _, clock := testOptions(t, map[string]string{KeyKafkaDispatchQueueCapacity: "1", KeyKafkaDispatchWorkerCount: "1"})
	release, entered := make(chan struct{}), make(chan struct{}, 1)
	opts.Lookup = blockingLookup(release, entered)
	var mu sync.Mutex
	var events []Event
	accept := false
	opts.TryEvent = func(e Event) bool {
		mu.Lock()
		defer mu.Unlock()
		if accept {
			events = append(events, e)
		}
		return accept
	}
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		d.Close(context.Background())
	}()
	d.OfferXLog(xlogPack(100, 202, 1, 200))
	<-entered
	d.OfferXLog(xlogPack(100, 202, 2, 200)) // fills the channel
	count := func() int { mu.Lock(); defer mu.Unlock(); return len(events) }
	d.OfferXLog(xlogPack(100, 202, 3, 200)) // dropped, Event refused
	mu.Lock()
	accept = true
	mu.Unlock()
	d.OfferXLog(xlogPack(100, 202, 4, 200)) // retried at once
	d.OfferXLog(xlogPack(100, 202, 5, 200)) // inside the window
	if count() != 1 {
		t.Fatalf("events %d", count())
	}
	clock.Advance(-time.Hour)
	d.OfferXLog(xlogPack(100, 202, 6, 200)) // clock went back: window reset
	if count() != 2 {
		t.Fatalf("events after clock step back %d", count())
	}
	d.OfferXLog(xlogPack(100, 202, 7, 200))
	clock.Advance(eventInterval)
	d.OfferXLog(xlogPack(100, 202, 8, 200))
	if count() != 3 {
		t.Fatalf("events after interval %d", count())
	}
	if s := d.Stats().XLog; s.Dropped != 6 {
		t.Fatalf("%+v", s)
	}
}
