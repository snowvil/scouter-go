package kafka

// Producer behaviour against the in-process fake broker. These tests prove
// the client side of the wire subset and the bounded lifecycle; they do not
// prove interoperability with a real Apache Kafka broker (unverified).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"testing"
	"time"
)

func testConfig(view *clusterView, log *errorLog) Config {
	var bootstrap []string
	for _, b := range view.brokers {
		bootstrap = append(bootstrap, b.addr)
	}
	cfg := Config{
		Bootstrap:      bootstrap,
		Linger:         5 * time.Millisecond,
		RequestTimeout: 2 * time.Second,
		DialTimeout:    2 * time.Second,
		RetryBackoff:   10 * time.Millisecond,
	}
	if log != nil {
		cfg.OnError = log.handler
	}
	return cfg
}

func mustProducer(t testing.TB, cfg Config) *Producer {
	t.Helper()
	p, err := NewProducer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		p.Close(ctx)
	})
	return p
}

func enqueue(t testing.TB, p *Producer, rec Record) {
	t.Helper()
	if err := p.Enqueue(context.Background(), rec); err != nil {
		t.Fatalf("enqueue %s/%d: %v", rec.Topic, rec.Partition, err)
	}
}

func waitRecords(t testing.TB, b *fakeBroker, n int) {
	t.Helper()
	waitUntil(t, 5*time.Second, fmt.Sprintf("%d records on broker %d", n, b.node), func() bool { return len(b.records()) >= n })
	if got := len(b.records()); got != n {
		t.Fatalf("broker %d got %d records, want %d", b.node, got, n)
	}
}

func flush(t testing.TB, p *Producer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Flush(ctx); err != nil {
		t.Fatalf("flush: %v (stats %+v)", err, p.Stats())
	}
}

func TestProducerRoutesToPartitionLeaders(t *testing.T) {
	view := newView()
	a := startFakeBroker(t, view, 1)
	b := startFakeBroker(t, view, 2)
	view.setLeader("meta", 0, 1)
	view.setLeader("meta", 1, 2)
	view.setLeader("profile", 0, 2)
	var log errorLog
	cfg := testConfig(view, &log)
	cfg.Bootstrap = []string{a.addr} // b must be discovered through metadata
	p := mustProducer(t, cfg)

	key := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe} // txid -2 as Java LongSerializer
	headers := []Header{{"apm_type", []byte("SCOUTER")}, {"xlog_serializer_version", []byte("V1")}, {"xlog_application", []byte("gateway-app")}}
	before := time.Now().UnixMilli()
	enqueue(t, p, Record{Topic: "meta", Partition: 0, Key: key, Value: []byte(`{"a":1}`), Headers: headers})
	enqueue(t, p, Record{Topic: "meta", Partition: 1, Key: key, Value: []byte(`{"b":2}`), Headers: headers})
	enqueue(t, p, Record{Topic: "profile", Partition: 0, Key: nil, Value: []byte(`[]`)})
	flush(t, p)
	after := time.Now().UnixMilli()
	// acks=0: Flush returns once the bytes are written; the broker appends
	// asynchronously, so wait for it to observe the records.
	waitRecords(t, a, 1)
	waitRecords(t, b, 2)
	ra, rb := a.records(), b.records()
	if ra[0].topic != "meta" || ra[0].partition != 0 || string(ra[0].value) != `{"a":1}` || string(ra[0].key) != string(key) {
		t.Fatalf("broker a record %+v", ra[0])
	}
	if len(ra[0].headers) != 3 {
		t.Fatalf("headers %+v", ra[0].headers)
	}
	for i, h := range headers {
		if ra[0].headers[i].Key != h.Key || string(ra[0].headers[i].Value) != string(h.Value) {
			t.Fatalf("header %d = %+v, want %+v", i, ra[0].headers[i], h)
		}
	}
	if ra[0].timestamp < before || ra[0].timestamp > after {
		t.Fatalf("timestamp %d outside [%d,%d]", ra[0].timestamp, before, after)
	}
	var sawMeta1, sawProfile bool
	for _, r := range rb {
		switch {
		case r.topic == "meta" && r.partition == 1 && string(r.value) == `{"b":2}`:
			sawMeta1 = true
		case r.topic == "profile" && r.partition == 0 && r.keyNull && string(r.value) == "[]":
			sawProfile = true
		}
	}
	if !sawMeta1 || !sawProfile {
		t.Fatalf("broker b records %+v", rb)
	}
	s := p.Stats()
	if s.Enqueued != 3 || s.SentRecords != 3 || s.SentBatches != 3 || s.Failed != 0 || s.Dropped != 0 || s.Pending != 0 || s.PendingBytes != 0 {
		t.Fatalf("stats %+v", s)
	}
	// One refresh covers both topics when the coordinator first wakes after
	// all three Enqueues; if it wakes between them it fetches "meta" alone and
	// refreshes again for the missing "profile" (the intended missing-topic
	// path), so 2 is legitimate. Never more, and never a failure.
	if s.MetadataRefreshes < 1 || s.MetadataRefreshes > 2 || s.MetadataFailures != 0 || s.Reconnects != 2 {
		t.Fatalf("stats %+v", s)
	}
	if len(log.events) != 0 {
		t.Fatalf("unexpected errors %+v", log.events)
	}
}

func TestProducerBatchesPerPartitionInOrder(t *testing.T) {
	view := newView()
	a := startFakeBroker(t, view, 1)
	view.setLeader("t", 0, 1)
	view.setLeader("t", 1, 1)
	cfg := testConfig(view, nil)
	cfg.BatchBytes = 512
	cfg.Linger = time.Second
	p := mustProducer(t, cfg)
	const n = 200
	for i := range n {
		enqueue(t, p, Record{Topic: "t", Partition: int32(i % 2), Value: []byte(fmt.Sprintf("record-%03d-%s", i, strings.Repeat("x", 20)))})
	}
	flush(t, p)
	waitRecords(t, a, n)
	recs := a.records()
	last := map[int32]int{0: -1, 1: -1}
	for _, r := range recs {
		var i int
		if _, err := fmt.Sscanf(string(r.value), "record-%03d-", &i); err != nil {
			t.Fatal(err)
		}
		if int32(i%2) != r.partition || i <= last[r.partition] {
			t.Fatalf("partition %d order broken at %d after %d", r.partition, i, last[r.partition])
		}
		last[r.partition] = i
	}
	s := p.Stats()
	if s.SentBatches < 10 || s.SentRecords != n {
		t.Fatalf("stats %+v", s)
	}
	if got := a.requestCount(apiProduce); got >= int(s.SentBatches) {
		t.Fatalf("expected batches to share requests: %d requests for %d batches", got, s.SentBatches)
	}
}

func TestProducerNotLeaderRefreshesMetadata(t *testing.T) {
	view := newView()
	a := startFakeBroker(t, view, 1)
	b := startFakeBroker(t, view, 2)
	view.setLeader("t", 0, 1)
	a.produceError = func(string, int32, int32) int16 { return 6 }
	var log errorLog
	cfg := testConfig(view, &log)
	cfg.Acks = 1
	p := mustProducer(t, cfg)
	enqueue(t, p, Record{Topic: "t", Partition: 0, Value: []byte("one")})
	waitUntil(t, 5*time.Second, "first record failure", func() bool { return p.Stats().Failed == 1 })
	ev, ok := log.find(func(ev ErrorEvent) bool { return ev.Operation == "produce.partition" })
	var kerr Error
	if !ok || !errors.As(ev.Err, &kerr) || kerr.Code != 6 || !strings.Contains(ev.Err.Error(), "NOT_LEADER_OR_FOLLOWER") {
		t.Fatalf("event %+v", ev)
	}
	view.setLeader("t", 0, 2)
	enqueue(t, p, Record{Topic: "t", Partition: 0, Value: []byte("two")})
	waitUntil(t, 5*time.Second, "record on new leader", func() bool { return len(b.records()) == 1 })
	if r := b.records()[0]; string(r.value) != "two" {
		t.Fatalf("record %+v", r)
	}
	s := p.Stats()
	if s.MetadataRefreshes < 2 || s.SentRecords != 1 || s.Failed != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestProducerReconnectsAfterConnectionLoss(t *testing.T) {
	view := newView()
	a := startFakeBroker(t, view, 1)
	view.setLeader("t", 0, 1)
	var log errorLog
	cfg := testConfig(view, &log)
	cfg.Acks = 1
	p := mustProducer(t, cfg)
	enqueue(t, p, Record{Topic: "t", Partition: 0, Value: []byte("one")})
	flush(t, p)
	if p.Stats().Reconnects != 1 {
		t.Fatalf("stats %+v", p.Stats())
	}
	a.closeConns()
	waitUntil(t, 2*time.Second, "server side close", func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return len(a.conns) == 0
	})
	enqueue(t, p, Record{Topic: "t", Partition: 0, Value: []byte("two")})
	waitUntil(t, 5*time.Second, "second record outcome", func() bool {
		s := p.Stats()
		return s.SentRecords+s.Failed == 2
	})
	enqueue(t, p, Record{Topic: "t", Partition: 0, Value: []byte("three")})
	waitUntil(t, 5*time.Second, "record after reconnect", func() bool {
		for _, r := range a.records() {
			if string(r.value) == "three" {
				return true
			}
		}
		return false
	})
	s := p.Stats()
	if s.Reconnects != 2 || s.SentRecords+s.Failed != 3 || s.Pending != 0 {
		t.Fatalf("stats %+v", s)
	}
	if s.Failed == 1 {
		if _, ok := log.find(func(ev ErrorEvent) bool { return ev.Operation == "produce.request" }); !ok {
			t.Fatalf("failed request without report: %+v", log.events)
		}
	}
}

func TestProducerAcksErrorCodes(t *testing.T) {
	view := newView()
	a := startFakeBroker(t, view, 1)
	view.setLeader("t", 0, 1)
	view.setLeader("t", 1, 1)
	a.produceError = func(_ string, partition int32, _ int32) int16 {
		if partition == 0 {
			return 19 // NOT_ENOUGH_REPLICAS
		}
		return 200 // unknown code
	}
	var log errorLog
	cfg := testConfig(view, &log)
	cfg.Acks = -1
	p := mustProducer(t, cfg)
	enqueue(t, p, Record{Topic: "t", Partition: 0, Value: []byte("a")})
	enqueue(t, p, Record{Topic: "t", Partition: 1, Value: []byte("b")})
	flush(t, p)
	s := p.Stats()
	if s.Failed != 2 || s.SentRecords != 0 || len(a.records()) != 0 {
		t.Fatalf("stats %+v records %d", s, len(a.records()))
	}
	if _, ok := log.find(func(ev ErrorEvent) bool {
		var kerr Error
		return errors.As(ev.Err, &kerr) && kerr.Code == 19 && strings.Contains(ev.Err.Error(), "NOT_ENOUGH_REPLICAS")
	}); !ok {
		t.Fatalf("no NOT_ENOUGH_REPLICAS report: %+v", log.events)
	}
	if _, ok := log.find(func(ev ErrorEvent) bool {
		var kerr Error
		return errors.As(ev.Err, &kerr) && kerr.Code == 200 && strings.Contains(ev.Err.Error(), "code 200")
	}); !ok {
		t.Fatalf("no unknown-code report: %+v", log.events)
	}
}

func TestProducerAcksZeroSendsNoResponseRead(t *testing.T) {
	view := newView()
	a := startFakeBroker(t, view, 1)
	view.setLeader("t", 0, 1)
	// produceError still records nothing for code != 0 but with acks=0 the
	// broker never answers, so the producer counts the batch as sent.
	a.produceError = func(string, int32, int32) int16 { return 6 }
	p := mustProducer(t, testConfig(view, nil))
	enqueue(t, p, Record{Topic: "t", Partition: 0, Value: []byte("a")})
	flush(t, p)
	s := p.Stats()
	if s.SentRecords != 1 || s.Failed != 0 || len(a.records()) != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func unreachableAddr(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func TestProducerBufferFullDropsNewRecords(t *testing.T) {
	var log errorLog
	cfg := Config{
		Bootstrap:    []string{unreachableAddr(t)},
		BufferBytes:  1024,
		Linger:       5 * time.Millisecond,
		DialTimeout:  200 * time.Millisecond,
		RetryBackoff: 10 * time.Millisecond,
		OnError:      log.handler,
	}
	p := mustProducer(t, cfg)
	value := make([]byte, 200)
	var accepted int
	var err error
	for i := range 20 {
		err = p.Enqueue(context.Background(), Record{Topic: "t", Partition: int32(i % 3), Value: value})
		if err != nil {
			break
		}
		accepted++
	}
	if !errors.Is(err, ErrBufferFull) {
		t.Fatalf("expected ErrBufferFull after %d records, got %v", accepted, err)
	}
	if accepted < 2 || accepted > 5 {
		t.Fatalf("accepted %d records into 1024 bytes", accepted)
	}
	for range 3 {
		if err := p.Enqueue(context.Background(), Record{Topic: "t", Partition: 0, Value: value}); !errors.Is(err, ErrBufferFull) {
			t.Fatalf("expected ErrBufferFull, got %v", err)
		}
	}
	s := p.Stats()
	if s.Dropped != 4 || s.DroppedBufferFull != 4 || int(s.Pending) != accepted || s.PendingBytes > 1024 || s.PendingBytes <= 0 {
		t.Fatalf("stats %+v", s)
	}
	if err := p.Enqueue(context.Background(), Record{Topic: "t", Partition: 0, Value: make([]byte, 2<<20)}); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("oversized record: %v", err)
	}
	if s := p.Stats(); s.Dropped != 5 || s.DroppedBufferFull != 4 {
		t.Fatalf("a too-large record is not a buffer-full drop: %+v", s)
	}
	waitUntil(t, 3*time.Second, "metadata failure report", func() bool { return p.Stats().MetadataFailures >= 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	abandoned, err := p.Close(ctx)
	if int(abandoned) != accepted || !errors.Is(err, ErrAbandoned) {
		t.Fatalf("close abandoned=%d err=%v stats=%+v", abandoned, err, p.Stats())
	}
	if err := p.Enqueue(context.Background(), Record{Topic: "t", Partition: 0, Value: value}); !errors.Is(err, ErrClosed) {
		t.Fatalf("enqueue after close: %v", err)
	}
	s = p.Stats()
	if s.Pending != 0 || s.PendingBytes != 0 {
		t.Fatalf("stats after close %+v", s)
	}
	if again, _ := p.Close(context.Background()); again != abandoned {
		t.Fatalf("second close %d != %d", again, abandoned)
	}
}

func TestProducerDeliveryTimeoutFailsStuckBatches(t *testing.T) {
	var log errorLog
	cfg := Config{
		Bootstrap:       []string{unreachableAddr(t)},
		Linger:          5 * time.Millisecond,
		DialTimeout:     100 * time.Millisecond,
		RequestTimeout:  100 * time.Millisecond,
		DeliveryTimeout: 300 * time.Millisecond,
		RetryBackoff:    10 * time.Millisecond,
		OnError:         log.handler,
	}
	p := mustProducer(t, cfg)
	enqueue(t, p, Record{Topic: "t", Partition: 0, Value: []byte("x")})
	waitUntil(t, 3*time.Second, "delivery timeout", func() bool { return p.Stats().Failed == 1 })
	if !log.has(ErrDeliveryTimeout) {
		t.Fatalf("events %+v", log.events)
	}
	if s := p.Stats(); s.Pending != 0 || s.PendingBytes != 0 {
		t.Fatalf("stats %+v", s)
	}
	// Close with an unbounded context must still return: nothing is queued.
	if abandoned, err := p.Close(context.Background()); abandoned != 0 || err != nil {
		t.Fatalf("close %d %v", abandoned, err)
	}
}

func TestProducerCloseDrainsWithoutWaitingForLinger(t *testing.T) {
	view := newView()
	a := startFakeBroker(t, view, 1)
	view.setLeader("t", 0, 1)
	cfg := testConfig(view, nil)
	cfg.Linger = 10 * time.Second
	p := mustProducer(t, cfg)
	for i := range 3 {
		enqueue(t, p, Record{Topic: "t", Partition: 0, Value: []byte{byte(i)}})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	abandoned, err := p.Close(ctx)
	if abandoned != 0 || err != nil {
		t.Fatalf("close %d %v", abandoned, err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("close waited for linger: %s", time.Since(start))
	}
	waitRecords(t, a, 3)
	if s := p.Stats(); s.SentRecords != 3 || s.SentBatches != 1 || s.Pending != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestProducerContextCancellation(t *testing.T) {
	cfg := Config{
		Bootstrap:    []string{unreachableAddr(t)},
		Linger:       time.Second,
		DialTimeout:  100 * time.Millisecond,
		RetryBackoff: 10 * time.Millisecond,
	}
	p := mustProducer(t, cfg)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Enqueue(cancelled, Record{Topic: "t", Partition: 0, Value: []byte("x")}); !errors.Is(err, context.Canceled) {
		t.Fatalf("enqueue: %v", err)
	}
	if err := p.Flush(cancelled); err != nil {
		t.Fatalf("flush with nothing pending must succeed: %v", err)
	}
	enqueue(t, p, Record{Topic: "t", Partition: 0, Value: []byte("x")})
	ctx, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()
	if err := p.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("flush: %v", err)
	}
	closeCtx, cancel3 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel3()
	start := time.Now()
	abandoned, err := p.Close(closeCtx)
	if abandoned != 1 || !errors.Is(err, ErrAbandoned) {
		t.Fatalf("close %d %v", abandoned, err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("close not bounded: %s", time.Since(start))
	}
	if s := p.Stats(); s.Dropped != 1 || s.Pending != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestProducerRejectsOversizedAndBrokenMetadataThenRecovers(t *testing.T) {
	for mode, want := range map[int]error{1: ErrLimit, 2: ErrMalformed, 3: ErrMalformed} {
		t.Run(fmt.Sprint("mode", mode), func(t *testing.T) {
			view := newView()
			a := startFakeBroker(t, view, 1)
			view.setLeader("t", 0, 1)
			a.mu.Lock()
			a.metadataMode = mode
			a.mu.Unlock()
			var log errorLog
			cfg := testConfig(view, &log)
			cfg.Limits.MaxResponseBytes = 4096
			p := mustProducer(t, cfg)
			enqueue(t, p, Record{Topic: "t", Partition: 0, Value: []byte("x")})
			waitUntil(t, 5*time.Second, "metadata rejection", func() bool { return log.has(want) })
			if s := p.Stats(); s.MetadataFailures == 0 || s.MetadataRefreshes != 0 || s.Pending != 1 {
				t.Fatalf("stats %+v", s)
			}
			a.mu.Lock()
			a.metadataMode = 0
			a.mu.Unlock()
			flush(t, p)
			waitRecords(t, a, 1)
		})
	}
}

// A broker whose Produce range starts above ours (a future broker that
// dropped v12) is rejected rather than spoken to in a guessed layout.
func TestProducerRejectsBrokerWithoutCommonVersion(t *testing.T) {
	view := newView()
	a := startFakeBroker(t, view, 1)
	view.setLeader("t", 0, 1)
	a.versions[apiProduce] = versionRange{13, 14}
	var log errorLog
	cfg := testConfig(view, &log)
	cfg.DeliveryTimeout = 300 * time.Millisecond
	p := mustProducer(t, cfg)
	enqueue(t, p, Record{Topic: "t", Partition: 0, Value: []byte("x")})
	waitUntil(t, 5*time.Second, "unsupported version report", func() bool { return log.has(ErrUnsupportedVersion) })
	waitUntil(t, 5*time.Second, "batch failure", func() bool { return p.Stats().Failed == 1 })
	if len(a.records()) != 0 || p.Stats().SentRecords != 0 {
		t.Fatalf("records sent to an unsupported broker")
	}
}

func TestProducerMetadataAndProduceVersionMatrix(t *testing.T) {
	for mv := int16(1); mv <= 13; mv++ {
		for _, pv := range []int16{3, 5, 7, 8, 9, 10, 12} {
			t.Run(fmt.Sprintf("metadata%d_produce%d", mv, pv), func(t *testing.T) {
				view := newView()
				a := startFakeBroker(t, view, 1)
				a.versions[apiMetadata] = versionRange{0, mv}
				a.versions[apiProduce] = versionRange{3, pv}
				a.unknownTags = mv%2 == 0
				view.setLeader("t", 3, 1)
				var log errorLog
				cfg := testConfig(view, &log)
				cfg.Acks = 1
				p := mustProducer(t, cfg)
				enqueue(t, p, Record{Topic: "t", Partition: 3, Key: []byte("k"), Value: []byte("v"), Headers: []Header{{"h", []byte("x")}}})
				flush(t, p)
				recs := a.records()
				if len(recs) != 1 || recs[0].partition != 3 || string(recs[0].key) != "k" || len(recs[0].headers) != 1 {
					t.Fatalf("records %+v errors %+v", recs, log.events)
				}
				if s := p.Stats(); s.SentRecords != 1 || s.Failed != 0 {
					t.Fatalf("stats %+v", s)
				}
				if got := a.seen(apiProduce); len(got) != 1 || got[0] != pv {
					t.Fatalf("produce versions %v", got)
				}
				if got := a.seen(apiMetadata); len(got) == 0 || got[0] != mv {
					t.Fatalf("metadata versions %v", got)
				}
			})
		}
	}
}

func TestProducerUnknownTopicStaysPendingUntilTimeout(t *testing.T) {
	view := newView()
	a := startFakeBroker(t, view, 1)
	view.setLeader("known", 0, 1)
	var log errorLog
	cfg := testConfig(view, &log)
	cfg.DeliveryTimeout = 300 * time.Millisecond
	p := mustProducer(t, cfg)
	enqueue(t, p, Record{Topic: "missing", Partition: 0, Value: []byte("x")})
	enqueue(t, p, Record{Topic: "known", Partition: 0, Value: []byte("y")})
	waitUntil(t, 5*time.Second, "known topic delivery", func() bool { return len(a.records()) == 1 })
	waitUntil(t, 5*time.Second, "missing topic expiry", func() bool { return p.Stats().Failed == 1 })
	if !log.has(ErrDeliveryTimeout) {
		t.Fatalf("events %+v", log.events)
	}
	if _, ok := log.find(func(ev ErrorEvent) bool {
		var kerr Error
		return ev.Operation == "metadata.topic" && errors.As(ev.Err, &kerr) && kerr.Code == 3
	}); !ok {
		t.Fatalf("no UNKNOWN_TOPIC report: %+v", log.events)
	}
}

func TestProducerTooManyTopics(t *testing.T) {
	cfg := Config{Bootstrap: []string{unreachableAddr(t)}, MaxTopics: 2, DialTimeout: 50 * time.Millisecond}
	p := mustProducer(t, cfg)
	enqueue(t, p, Record{Topic: "a", Partition: 0, Value: []byte("x")})
	enqueue(t, p, Record{Topic: "b", Partition: 0, Value: []byte("x")})
	if err := p.Enqueue(context.Background(), Record{Topic: "c", Partition: 0, Value: []byte("x")}); !errors.Is(err, ErrTooManyTopics) {
		t.Fatalf("third topic: %v", err)
	}
	if err := p.Enqueue(context.Background(), Record{Topic: "t", Partition: -1}); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("invalid record: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	bad := []Config{
		{},
		{Bootstrap: []string{""}},
		{Bootstrap: []string{"x"}, Acks: 2},
		{Bootstrap: []string{"x"}, Linger: -1},
		{Bootstrap: []string{"x"}, BufferBytes: -1},
		{Bootstrap: []string{"x"}, MaxRecordBytes: 2 << 20, MaxRequestBytes: 1 << 20},
		{Bootstrap: []string{"x"}, Limits: Limits{MaxBrokers: -1}},
		{Bootstrap: []string{"x"}, Limits: Limits{MaxTaggedFields: -1}},
		{Bootstrap: []string{"x"}, Compression: "gzip"},
		{Bootstrap: []string{"x"}, Compression: "zstd"},
		{Bootstrap: []string{"x"}, Compression: "snappy"},
		{Bootstrap: []string{"x"}, Compression: "LZ4"},
		{Bootstrap: []string{"x"}, Compression: "brotli"},
	}
	for i, cfg := range bad {
		if _, err := NewProducer(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("config %d: %v", i, err)
		}
	}
	cfg, err := Config{Bootstrap: []string{"x"}}.normalized()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Acks != 0 || cfg.Linger != 20*time.Millisecond || cfg.BatchBytes != 128<<10 || cfg.BufferBytes != 64<<20 ||
		cfg.RequestTimeout != 5*time.Second || cfg.DeliveryTimeout != 5100*time.Millisecond || cfg.MetadataRefresh != 5*time.Minute ||
		cfg.Limits.MaxResponseBytes != 1<<20 || cfg.Limits.MaxTaggedFields != 64 || cfg.ClientID != "scouter-go" || cfg.Compression != CompressionLZ4 {
		t.Fatalf("defaults %+v", cfg)
	}
	for _, s := range []string{"none", "lz4"} {
		if c, err := ParseCompression(s); err != nil || string(c) != s {
			t.Fatalf("ParseCompression(%q) = %q %v", s, c, err)
		}
	}
}

func TestProducerCompression(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	noise := func() []byte {
		b := make([]byte, 2000)
		r.Read(b)
		return b
	}
	repeat := func(s string, n int) [][]byte {
		v := []byte(strings.Repeat(s, n))
		return [][]byte{v, v, v}
	}
	for _, tc := range []struct {
		compression Compression
		values      [][]byte
		want        int16
	}{
		{"", repeat(`{"traceId":1,"text":"repeat"}`, 20), batchCodecLZ4},
		{CompressionLZ4, repeat("abc", 1000), batchCodecLZ4},
		{CompressionLZ4, [][]byte{noise(), noise(), noise()}, 0}, // incompressible: sent uncompressed
		{CompressionNone, repeat("abc", 1000), 0},
	} {
		t.Run(fmt.Sprintf("%s_%d", tc.compression, tc.want), func(t *testing.T) {
			view := newView()
			a := startFakeBroker(t, view, 1)
			view.setLeader("t", 0, 1)
			var log errorLog
			cfg := testConfig(view, &log)
			cfg.Acks = 1
			cfg.Compression = tc.compression
			p := mustProducer(t, cfg)
			for i, v := range tc.values {
				enqueue(t, p, Record{Topic: "t", Partition: 0, Key: []byte{byte(i)}, Value: v, Headers: []Header{{"h", []byte("x")}}})
			}
			flush(t, p)
			recs := a.records()
			if len(recs) != 3 {
				t.Fatalf("records %d errors %+v", len(recs), log.events)
			}
			for i, rec := range recs {
				if !bytes.Equal(rec.value, tc.values[i]) || rec.key[0] != byte(i) {
					t.Fatalf("record %d differs", i)
				}
			}
			for _, c := range a.receivedCodecs() {
				if c != tc.want {
					t.Fatalf("codec %d want %d", c, tc.want)
				}
			}
		})
	}
}

// A broker that predates ApiVersions v3 answers UNSUPPORTED_VERSION in the v0
// layout; the producer retries v0 on the same connection and then uses the
// highest non-flexible versions that broker offers.
func TestProducerApiVersionsFallbackForOldBroker(t *testing.T) {
	view := newView()
	a := startFakeBroker(t, view, 1)
	a.rejectFlexibleApiVersions = true
	a.versions = map[int16]versionRange{apiProduce: {0, 7}, apiMetadata: {0, 8}, apiApiVersions: {0, 2}}
	view.setLeader("t", 0, 1)
	var log errorLog
	cfg := testConfig(view, &log)
	cfg.Acks = 1
	p := mustProducer(t, cfg)
	enqueue(t, p, Record{Topic: "t", Partition: 0, Value: []byte("v")})
	flush(t, p)
	if len(a.records()) != 1 || len(log.events) != 0 {
		t.Fatalf("records %d errors %+v", len(a.records()), log.events)
	}
	if got := fmt.Sprint(a.seen(apiApiVersions)); !strings.HasPrefix(got, "[3 0") {
		t.Fatalf("api versions sequence %s", got)
	}
	if got := a.seen(apiProduce); len(got) != 1 || got[0] != 7 {
		t.Fatalf("produce versions %v", got)
	}
	if got := a.seen(apiMetadata); len(got) == 0 || got[0] != 8 {
		t.Fatalf("metadata versions %v", got)
	}
}

// A Kafka 4.0-like broker gets the highest flexible versions; unknown tagged
// fields in every response structure are skipped.
func TestProducerFlexibleVersionsWithUnknownTags(t *testing.T) {
	for _, acks := range []int16{0, 1, -1} {
		t.Run(fmt.Sprint("acks", acks), func(t *testing.T) {
			view := newView()
			a := startFakeBroker(t, view, 1)
			b := startFakeBroker(t, view, 2)
			a.unknownTags, b.unknownTags = true, true
			view.setLeader("t", 0, 1)
			view.setLeader("t", 1, 2)
			var log errorLog
			cfg := testConfig(view, &log)
			cfg.Acks = acks
			p := mustProducer(t, cfg)
			enqueue(t, p, Record{Topic: "t", Partition: 0, Value: []byte("a")})
			enqueue(t, p, Record{Topic: "t", Partition: 1, Value: []byte("b")})
			flush(t, p)
			waitRecords(t, a, 1)
			waitRecords(t, b, 1)
			if len(log.events) != 0 {
				t.Fatalf("errors %+v", log.events)
			}
			for _, br := range []*fakeBroker{a, b} {
				if got := br.seen(apiApiVersions); len(got) == 0 || got[0] != 3 {
					t.Fatalf("api versions %v", got)
				}
				if got := br.seen(apiProduce); len(got) > 0 && got[0] != 12 {
					t.Fatalf("produce versions %v", got)
				}
			}
			if got := a.seen(apiMetadata); len(got) > 0 && got[0] != 13 {
				t.Fatalf("metadata versions %v", got)
			}
		})
	}
}

func TestProducerReportsProduceErrorMessage(t *testing.T) {
	view := newView()
	a := startFakeBroker(t, view, 1)
	view.setLeader("t", 0, 1)
	a.produceError = func(string, int32, int32) int16 { return 87 }
	var log errorLog
	cfg := testConfig(view, &log)
	cfg.Acks = 1
	p := mustProducer(t, cfg)
	enqueue(t, p, Record{Topic: "t", Partition: 0, Value: []byte("v")})
	flush(t, p)
	ev, ok := log.find(func(ev ErrorEvent) bool { return ev.Operation == "produce.partition" })
	var kerr Error
	if !ok || !errors.As(ev.Err, &kerr) || kerr.Code != 87 || !strings.Contains(ev.Err.Error(), "fake error") {
		t.Fatalf("event %+v %v", ev, ok)
	}
}

func TestProducerConcurrentEnqueueRace(t *testing.T) {
	view := newView()
	a := startFakeBroker(t, view, 1)
	b := startFakeBroker(t, view, 2)
	for i := range int32(4) {
		view.setLeader("t", i, 1+i%2)
	}
	cfg := testConfig(view, nil)
	cfg.BatchBytes = 2048
	p := mustProducer(t, cfg)
	const workers, per = 8, 100
	done := make(chan struct{})
	for w := range workers {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := range per {
				_ = p.Enqueue(context.Background(), Record{Topic: "t", Partition: int32((w + i) % 4), Value: []byte(fmt.Sprintf("%d-%d", w, i))})
				if i%10 == 0 {
					_ = p.Stats()
				}
			}
		}()
	}
	for range workers {
		<-done
	}
	flush(t, p)
	waitUntil(t, 5*time.Second, "all records", func() bool { return len(a.records())+len(b.records()) == workers*per })
	if s := p.Stats(); s.SentRecords != workers*per || s.Failed != 0 || s.Dropped != 0 {
		t.Fatalf("stats %+v", s)
	}
}

// An idle connection (older than IdleReconnect) is redialled before the next
// Produce so a broker-side idle close cannot swallow an acks=0 batch.
func TestIdleConnectionIsRedialled(t *testing.T) {
	view := newView()
	a := startFakeBroker(t, view, 1)
	view.setLeader("meta", 0, 1)
	var log errorLog
	cfg := testConfig(view, &log)
	cfg.Bootstrap = []string{a.addr}
	cfg.IdleReconnect = 50 * time.Millisecond
	p := mustProducer(t, cfg)
	ctx := context.Background()
	send := func() {
		if err := p.Enqueue(ctx, Record{Topic: "meta", Partition: 0, Value: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		if err := p.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	send()
	before := p.Stats().Reconnects
	send() // within the idle window: same connection
	if p.Stats().Reconnects != before {
		t.Fatal("reconnected while warm")
	}
	time.Sleep(120 * time.Millisecond)
	send()
	if p.Stats().Reconnects != before+1 {
		t.Fatalf("idle connection not redialled: %d -> %d", before, p.Stats().Reconnects)
	}
	if _, err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
