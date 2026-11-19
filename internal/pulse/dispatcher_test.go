package pulse

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

type memSink struct {
	mu      sync.Mutex
	records []Record
	err     error
	block   chan struct{} // when non-nil, Enqueue waits until it is closed
	entered atomic.Int64  // Enqueue calls started
}

func (s *memSink) Enqueue(ctx context.Context, rec Record) error {
	s.entered.Add(1)
	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.records = append(s.records, rec)
	return nil
}
func (s *memSink) all() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Record(nil), s.records...)
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// testAppFilters names the applications that the dispatcher tests exclude,
// rebase and treat as non-Spring. The Go defaults are empty, so testOptions
// sets them unless a test overrides the key.
var testAppFilters = map[string]string{
	KeyExcludeApplications:        "excluded-app",
	KeyRebaseTransactionApps:      "gateway-app|/|3",
	KeyNonSpringBasedApplications: "gateway-app,plain-app-a,plain-app-b,plain-app-c",
}

func testOptions(t *testing.T, values map[string]string) (Options, *memDict, *memSink, *fakeClock) {
	t.Helper()
	merged := make(map[string]string, len(testAppFilters)+len(values))
	for k, v := range testAppFilters {
		merged[k] = v
	}
	for k, v := range values {
		merged[k] = v
	}
	cfg, err := ParseJavaConfig(merged)
	if err != nil {
		t.Fatal(err)
	}
	d := newDict()
	d.put("", TextObject, 100, "/host/app@1")
	d.put("", TextObject, 101, "/host/excluded-app@1")
	d.put("", TextObject, 102, "/host/gateway-app@1")
	d.put("", TextObject, 103, "short")
	d.put("", TextService, 200, "/api/v1/users")
	d.put("", TextService, 201, "/static/font.woff")
	d.put("", TextService, 202, "UserService.find<T>")
	d.put("", TextService, 203, "/a/b/c/d$$x")
	d.put("", TextHashMessage, 300, "thread-1")
	sink := &memSink{}
	clock := &fakeClock{now: time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)}
	return Options{Config: cfg, Sink: sink, Lookup: d.lookup, Location: time.UTC, Clock: clock.Now}, d, sink, clock
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}

func xlogPack(objHash, service int32, txid int64, status int32) *protocol.XLogPack {
	return &protocol.XLogPack{EndTime: time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC).UnixMilli(), ObjHash: objHash, Service: service, TxID: txid, ThreadNameHash: 300, Status: status, IPAddress: []byte{127, 0, 0, 1}, TailVersion: 12}
}

func profilePack(objHash, service int32, txid int64) *protocol.ProfilePack {
	steps, _ := protocol.EncodeSteps([]protocol.Step{&protocol.MessageStep{Message: "m"}}, StepLimits)
	return &protocol.ProfilePack{Time: time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC).UnixMilli(), ObjHash: objHash, Service: service, TxID: txid, Profile: steps}
}

func TestDispatcherRecordsAndFilters(t *testing.T) {
	opts, _, sink, _ := testOptions(t, map[string]string{KeyDropHTTPStatusRules: "*|404", KeyKafkaTopicPartitionCount: "7"})
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(context.Background())
	offers := []struct {
		xlog    *protocol.XLogPack
		profile *protocol.ProfilePack
	}{
		{xlog: xlogPack(100, 202, -1, 200)},  // sent: transaction contains "<" and ">"
		{xlog: xlogPack(101, 200, 2, 200)},   // excluded application
		{xlog: xlogPack(103, 200, 3, 200)},   // extraction failure (split length 1)
		{xlog: xlogPack(999, 200, 4, 200)},   // dictionary miss -> extraction failure
		{xlog: xlogPack(100, 201, 5, 200)},   // extension
		{xlog: xlogPack(100, 999, 6, 200)},   // null transaction -> extension stage
		{xlog: xlogPack(100, 200, 7, 200)},   // Spring URI without "<>" -> filtered
		{xlog: xlogPack(100, 202, 8, 200)},   // sent
		{xlog: xlogPack(100, 202, 9, 404)},   // http status -> remembered
		{profile: profilePack(100, 202, 9)},  // skipped by filtered txid
		{profile: profilePack(100, 202, 10)}, // sent
		{xlog: xlogPack(102, 203, 11, 200)},  // gateway-app: non-Spring, rebase to /a/b
		{xlog: xlogPack(102, 200, 0, 404)},   // txid 0 is dropped but not remembered
		{profile: profilePack(102, 200, 0)},  // gateway-app non-Spring -> sent
		{profile: &protocol.ProfilePack{Time: 1, ObjHash: 100, Service: 202, TxID: 12, Profile: []byte{200}}}, // step decode error
	}
	// XLogs first, then profiles, so the status-filtered txid is remembered
	// before its profile arrives (the Java pools race here).
	for _, o := range offers {
		if o.xlog != nil && !d.OfferXLog(o.xlog) {
			t.Fatal("offer rejected")
		}
	}
	waitFor(t, func() bool { s := d.Stats(); return s.XLog.Sent+s.XLog.Filtered+s.XLog.Errors == 11 })
	for _, o := range offers {
		if o.profile != nil && !d.OfferProfile(o.profile) {
			t.Fatal("offer rejected")
		}
	}
	waitFor(t, func() bool {
		s := d.Stats()
		return s.XLog.Enqueued == 11 && s.Profile.Enqueued == 4 && s.XLog.Sent+s.XLog.Filtered+s.XLog.Errors == 11 && s.Profile.Sent+s.Profile.Filtered+s.Profile.Errors == 4
	})
	s := d.Stats()
	if s.XLog.Sent != 3 || s.XLog.Filtered != 8 || s.XLog.Errors != 0 || s.XLog.Failed != 0 || s.XLog.Dropped != 0 {
		t.Fatalf("xlog stats %+v", s.XLog)
	}
	wantStages := [filterStageCount]uint64{FilterExtractFail: 2, FilterApplication: 1, FilterHTTPStatus: 2, FilterTransactionExtension: 2, FilterSpringURI: 1}
	if s.XLog.FilteredByStage != wantStages {
		t.Fatalf("xlog stages %v want %v", s.XLog.FilteredByStage, wantStages)
	}
	if s.Profile.Sent != 2 || s.Profile.Filtered != 1 || s.Profile.FilteredByStage[FilterFilteredTrace] != 1 || s.Profile.Errors != 1 || s.FilteredTraces != 1 {
		t.Fatalf("profile stats %+v traces=%d", s.Profile, s.FilteredTraces)
	}
	byKey := map[string]Record{}
	for _, r := range sink.all() {
		byKey[r.Topic+"/"+hex.EncodeToString(r.Key)] = r
	}
	if len(byKey) != 5 {
		t.Fatal(len(byKey))
	}
	r, ok := byKey["dev-pulse-scouter-xlog-meta/ffffffffffffffff"]
	if !ok {
		t.Fatalf("missing txid -1 record: %v", byKey)
	}
	if r.Partition != Partition("app", 7) || len(r.Headers) != 3 || r.Headers[0].Key != "apm_type" || string(r.Headers[0].Value) != "SCOUTER" ||
		r.Headers[1].Key != "xlog_serializer_version" || string(r.Headers[1].Value) != "V1" || r.Headers[2].Key != "xlog_application" || string(r.Headers[2].Value) != "app" {
		t.Fatalf("record %+v", r)
	}
	if !containsBytes(r.Value, `"transactionDictContent":"UserService.find<T>"`) {
		t.Fatal(string(r.Value))
	}
	r = byKey["dev-pulse-scouter-xlog-meta/000000000000000b"]
	if !containsBytes(r.Value, `"transactionDictId":`+fmt.Sprint(TransactionHash("/a/b"))+`,`) || !containsBytes(r.Value, `"transactionDictContent":"/a/b"`) || string(r.Headers[2].Value) != "gateway-app" {
		t.Fatal(string(r.Value))
	}
	r = byKey["dev-pulse-scouter-xlog-profile/000000000000000a"]
	if r.Partition != Partition("app", 7) || !containsBytes(r.Value, `"traceId":10,`) || !containsBytes(r.Value, `"mainValue":"m"`) {
		t.Fatalf("profile record %+v", r)
	}
	if _, ok := byKey["dev-pulse-scouter-xlog-profile/0000000000000000"]; !ok {
		t.Fatal("txid 0 profile must not be filtered by the status map")
	}
}

func TestDispatcherFilteredTraceTTLAndBound(t *testing.T) {
	// One worker keeps the remember() order deterministic.
	opts, _, _, clock := testOptions(t, map[string]string{KeyDropHTTPStatusRules: "*|5xx", KeyKafkaDispatchWorkerCount: "1"})
	opts.MaxFilteredTraces = 3
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(context.Background())
	for i := int64(1); i <= 4; i++ {
		d.OfferXLog(xlogPack(100, 200, i, 503))
	}
	waitFor(t, func() bool { return d.Stats().XLog.Filtered == 4 })
	if d.Stats().FilteredTraces != 3 || d.filtered.contains(1, clock.Now()) || !d.filtered.contains(4, clock.Now()) {
		t.Fatal("oldest entry must be evicted beyond the bound", d.Stats().FilteredTraces)
	}
	d.OfferProfile(profilePack(100, 202, 4))
	waitFor(t, func() bool { return d.Stats().Profile.Filtered == 1 })
	clock.Advance(FilteredTraceTTL + time.Millisecond)
	d.OfferProfile(profilePack(100, 202, 4))
	waitFor(t, func() bool { return d.Stats().Profile.Sent == 1 })
	if d.Stats().FilteredTraces != 2 {
		t.Fatal("expired entry must be removed on lookup", d.Stats().FilteredTraces)
	}
	// Java sheds expired entries when the map reaches 10,000 entries.
	f := &filteredTraces{}
	f.init(1 << 20)
	base := clock.Now()
	for i := int64(1); i <= 10000; i++ {
		f.remember(i, base)
	}
	f.remember(10001, base.Add(FilteredTraceTTL+time.Second))
	if f.size() != 1 {
		t.Fatal("expired entries must be shed at 10000", f.size())
	}
}

func TestDispatcherQueueSaturationAndClose(t *testing.T) {
	opts, _, sink, _ := testOptions(t, map[string]string{KeyKafkaDispatchQueueCapacity: "2", KeyKafkaDispatchWorkerCount: "1"})
	sink.block = make(chan struct{})
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	// The first task is held by the worker blocked inside the sink; then two
	// fit in the queue.
	if !d.OfferXLog(xlogPack(100, 202, 1, 200)) {
		t.Fatal("first offer rejected")
	}
	waitFor(t, func() bool { return sink.entered.Load() == 1 })
	accepted := 1
	for i := int64(2); i <= 10; i++ {
		if d.OfferXLog(xlogPack(100, 202, i, 200)) {
			accepted++
		}
	}
	s := d.Stats()
	if accepted != 3 || s.XLog.Enqueued != 3 || s.XLog.Dropped != 7 {
		t.Fatalf("accepted=%d stats=%+v", accepted, s.XLog)
	}
	// Close with an expired deadline: the held task is interrupted (ctx cancel
	// unblocks the sink) and, like the queued tasks, counted as shutdown
	// residue rather than a sink failure.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	s = d.Stats()
	if s.XLog.ShutdownDiscarded != 3 || s.XLog.Failed != 0 || s.XLog.Sent != 0 || s.XLog.Pending != 0 {
		t.Fatalf("after forced close %+v", s.XLog)
	}
	if d.OfferXLog(xlogPack(100, 202, 11, 200)) || d.Stats().XLog.Dropped != 7 {
		t.Fatal("offer after close must be rejected without counting a drop")
	}
	if err := d.Close(context.Background()); err != nil {
		t.Fatal("second close", err)
	}
	// Graceful close drains everything that was queued.
	opts, _, sink, _ = testOptions(t, map[string]string{KeyKafkaDispatchQueueCapacity: "16", KeyKafkaDispatchWorkerCount: "2"})
	sink.block = make(chan struct{})
	d, _ = New(opts)
	for i := int64(1); i <= 8; i++ {
		d.OfferXLog(xlogPack(100, 202, i, 200))
		d.OfferProfile(profilePack(100, 202, i))
	}
	closed := make(chan error, 1)
	go func() { closed <- d.Close(context.Background()) }()
	time.Sleep(10 * time.Millisecond)
	close(sink.block)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	s = d.Stats()
	if s.XLog.Sent != 8 || s.Profile.Sent != 8 || s.XLog.ShutdownDiscarded != 0 || len(sink.all()) != 16 {
		t.Fatalf("drain %+v %+v", s.XLog, s.Profile)
	}
}

func TestDispatcherSinkAndLookupErrors(t *testing.T) {
	opts, dict, sink, _ := testOptions(t, nil)
	sink.err = errors.New("buffer full")
	var events []Event
	var mu sync.Mutex
	opts.OnEvent = func(e Event) { mu.Lock(); events = append(events, e); mu.Unlock() }
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(context.Background())
	d.OfferXLog(xlogPack(100, 202, 1, 200))
	waitFor(t, func() bool { return d.Stats().XLog.Failed == 1 })
	dict.mu.Lock()
	dict.err = errors.New("disk")
	dict.mu.Unlock()
	d.OfferXLog(xlogPack(100, 202, 2, 200))
	d.OfferProfile(profilePack(100, 202, 2))
	waitFor(t, func() bool { return d.Stats().XLog.Errors == 1 && d.Stats().Profile.Errors == 1 })
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 3 || events[0].Kind != "sink" || events[1].Kind != "error" || events[2].Kind != "error" {
		t.Fatalf("events %+v", events)
	}
	if s := d.Stats(); s.XLog.Sent != 0 || s.XLog.Filtered != 0 {
		t.Fatalf("%+v", s.XLog)
	}
}

func TestDispatcherOwnsClonedPacks(t *testing.T) {
	opts, _, sink, _ := testOptions(t, nil)
	sink.block = make(chan struct{})
	d, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(context.Background())
	x := xlogPack(100, 202, 1, 200)
	p := profilePack(100, 202, 2)
	d.OfferXLog(x)
	d.OfferProfile(p)
	x.IPAddress[0] = 1
	x.TxID = 99
	p.Profile[0] = 0
	p.TxID = 99
	close(sink.block)
	waitFor(t, func() bool { return len(sink.all()) == 2 })
	for _, r := range sink.all() {
		if hex.EncodeToString(r.Key) == "0000000000000063" {
			t.Fatal("dispatcher processed a caller-mutated pack")
		}
		if r.Topic == "dev-pulse-scouter-xlog-meta" && !containsBytes(r.Value, `"ipaddr":"fwAAAQ=="`) {
			t.Fatal(string(r.Value))
		}
	}
	if _, err := New(Options{}); !errors.Is(err, ErrDispatcherOptions) {
		t.Fatal("missing options accepted")
	}
}

// Offer and Close may race from different goroutines; a send on the closed
// queue channel must never happen (run with -race).
func TestOfferCloseRace(t *testing.T) {
	for round := 0; round < 50; round++ {
		d, err := New(Options{Config: DefaultConfig(), Sink: &memSink{}, Lookup: func(context.Context, string, string, int32) (string, bool, error) { return "", false, nil }, Location: time.UTC})
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				d.OfferXLog(&protocol.XLogPack{TxID: int64(i), EndTime: 1})
				d.OfferProfile(&protocol.ProfilePack{TxID: int64(i), Time: 1, Profile: []byte{1}})
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err = d.Close(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
		wg.Wait()
		if d.OfferXLog(&protocol.XLogPack{TxID: 1, EndTime: 1}) {
			t.Fatal("offer accepted after close")
		}
	}
}
