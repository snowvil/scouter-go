package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/pulse"
	"github.com/snowvil/scouter-go/internal/pulse/kafka"
)

type memorySink struct {
	mu      sync.Mutex
	records []pulse.Record
}

func (m *memorySink) Enqueue(_ context.Context, rec pulse.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, rec)
	return nil
}

// The runtime wires the ingest hooks to the Pulse dispatcher and the sink,
// reads the Java keys from the properties file and reports stats at shutdown.
func TestRuntimePulseStream(t *testing.T) {
	sink := &memorySink{}
	original := newPulseSink
	newPulseSink = func(cfg pulse.Config, acks int16, _ func(kafka.ErrorEvent)) (pulse.Sink, *kafka.Producer, error) {
		if cfg.KafkaServers != "127.0.0.1:1" || cfg.TopicPartitionCount != 7 || acks != 1 {
			t.Errorf("config not applied: %+v acks=%d", cfg, acks)
		}
		return sink, nil, nil
	}
	defer func() { newPulseSink = original }()
	values, unknown, err := ReadPulseProperties(strings.NewReader("# scouter.conf\next_plugin_pulse_kafka_servers=127.0.0.1:1\nplugin_pulse_kafka_topic_partition_count : 7\next_plugin_pulse_typo=1\nnet_tcp_client_port=6100\n"))
	if err != nil || len(unknown) != 1 || unknown[0] != "ext_plugin_pulse_typo" || len(values) != 2 {
		t.Fatal(values, unknown, err)
	}
	var logs bytes.Buffer
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)
	root := t.TempDir()
	cfg := Config{Directory: root, Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>"), Pulse: &PulseConfig{Values: values, Acks: 1}}
	r, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	now := time.Now().UnixMilli()
	date := now - now%86400000 + 3600000
	const obj, svc = int32(4242), int32(777)
	for _, p := range []protocol.Pack{
		&protocol.TextPack{XType: "object", Hash: obj, Text: "host/instance/pulse-app@node-1"},
		&protocol.TextPack{XType: "service", Hash: svc, Text: "GET /pulse/<id>"},
		&protocol.XLogPack{EndTime: date, ObjHash: obj, Service: svc, TxID: -99, Elapsed: 12},
		&protocol.ProfilePack{Time: date, ObjHash: obj, Service: svc, TxID: -99, Profile: []byte{}},
	} {
		if err = r.engine.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		sink.mu.Lock()
		n := len(sink.records)
		sink.mu.Unlock()
		if n >= 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.records) != 1 {
		t.Fatalf("records %d; log:\n%s", len(sink.records), logs.String())
	}
	rec := sink.records[0]
	var key [8]byte
	txid := int64(-99)
	binary.BigEndian.PutUint64(key[:], uint64(txid))
	if rec.Topic != pulse.DefaultXLogMetaTopic || rec.Partition != pulse.Partition("pulse-app", 7) || !bytes.Equal(rec.Key, key[:]) || len(rec.Headers) != 3 || string(rec.Headers[2].Value) != "pulse-app" {
		t.Fatalf("%+v", rec)
	}
	if !bytes.Contains(rec.Value, []byte(`"transactionDictContent":"GET /pulse/<id>"`)) || !bytes.HasSuffix(bytes.TrimSpace(rec.Value), []byte(`"xtype":0}`)) {
		t.Fatalf("%s", rec.Value)
	}
	out := logs.String()
	if !strings.Contains(out, "pulse enabled") || !strings.Contains(out, "pulse={XLog:{Enqueued:1 Dropped:0 Sent:1") {
		t.Fatalf("stats line missing:\n%s", out)
	}
	for _, key := range []string{pulse.KeyExcludeApplications, pulse.KeyRebaseTransactionApps, pulse.KeyNonSpringBasedApplications} {
		if !strings.Contains(out, "WARNING: pulse: "+key+" is not set") {
			t.Fatalf("missing-key warning for %s absent:\n%s", key, out)
		}
	}
	if _, err = os.Stat(filepath.Join(root, "lock.dat")); !os.IsNotExist(err) {
		t.Fatal("lock not released", err)
	}
}

// ext_plugin_pulse_enable=false keeps the stream off even with a config file.
func TestRuntimePulseDisabledByKey(t *testing.T) {
	root := t.TempDir()
	r, err := Open(Config{Directory: root, Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>"), Pulse: &PulseConfig{Values: map[string]string{pulse.KeyEnable: "false", pulse.KeyKafkaServers: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatal(err)
	}
	if r.pulse != nil {
		t.Fatal("stream created while disabled")
	}
	r.Close()
}

// A bad Pulse configuration is isolated like the Java plugin: collection
// starts, the error is logged and visible in /health, the stream stays off.
func TestRuntimePulseConfigErrorIsIsolated(t *testing.T) {
	root := t.TempDir()
	r, err := Open(Config{Directory: root, Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>"), Pulse: &PulseConfig{Values: map[string]string{pulse.KeyKafkaTopicPartitionCount: "-5", pulse.KeyKafkaServers: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.pulse != nil || r.pulseError == "" {
		t.Fatal(r.pulse, r.pulseError)
	}
}

func TestPulseAppFilterWarnings(t *testing.T) {
	values, _, err := ReadPulseProperties(strings.NewReader("ext_plugin_pulse_exclude_applications=\next_plugin_pulse_rebase_transaction_applications=app-a|/|3\n"))
	if err != nil {
		t.Fatal(err)
	}
	// "key=" is a deliberate empty value and a set key is fine; only the
	// missing non-Spring key is reported.
	got := pulseAppFilterWarnings(values, true)
	if len(got) != 1 || !strings.HasPrefix(got[0], "WARNING: pulse: "+pulse.KeyNonSpringBasedApplications+" is not set: the Spring URI filter applies to every application") {
		t.Fatal(got)
	}
	if got := pulseAppFilterWarnings(nil, true); len(got) != 3 {
		t.Fatal(got)
	}
	// With the Spring URI filter off the non-Spring list has no effect.
	if got := pulseAppFilterWarnings(values, false); len(got) != 0 {
		t.Fatal(got)
	}
	if got := pulseAppFilterWarnings(nil, false); len(got) != 2 {
		t.Fatal(got)
	}
	values[pulse.KeyNonSpringBasedApplications] = "app-a"
	if got := pulseAppFilterWarnings(values, true); len(got) != 0 {
		t.Fatal(got)
	}
}

// java.util.Properties semantics: a bare key line is an empty value.
func TestReadPulsePropertiesBareKey(t *testing.T) {
	values, unknown, err := ReadPulseProperties(strings.NewReader("ext_plugin_pulse_enable\n"))
	if err != nil || len(unknown) != 0 || values[pulse.KeyEnable] != "" {
		t.Fatal(values, unknown, err)
	}
	if _, ok := values[pulse.KeyEnable]; !ok {
		t.Fatal("bare key dropped")
	}
	conf, err := pulse.ParseJavaConfig(values)
	if err != nil || conf.Enable {
		t.Fatal("bare ext_plugin_pulse_enable must disable like Java parseBoolean(\"\")", conf.Enable, err)
	}
}

type fakeTextEngine struct{ byDate map[string]string }

func (f fakeTextEngine) Text(_ context.Context, date, _ string, _ int32) (string, bool, error) {
	v, ok := f.byDate[date]
	return v, ok, nil
}

// TextCache approximation: an event-date miss falls back to the receive date.
func TestPulseLookupFallsBackToToday(t *testing.T) {
	today := storageDate(time.Now().UnixMilli(), time.UTC)
	lookup := pulseLookup(fakeTextEngine{map[string]string{today: "host/i/app@n"}}, time.UTC)
	if v, ok, err := lookup(context.Background(), "20200101", "object", 1); err != nil || !ok || v != "host/i/app@n" {
		t.Fatal(v, ok, err)
	}
	lookup = pulseLookup(fakeTextEngine{map[string]string{}}, time.UTC)
	if _, ok, err := lookup(context.Background(), "20200101", "object", 1); err != nil || ok {
		t.Fatal(ok, err)
	}
}

// /health reports the Kafka producer counters next to the dispatcher's.
func TestHealthPulseKafkaStats(t *testing.T) {
	root := t.TempDir()
	r, err := Open(Config{Directory: root, Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>"), Pulse: &PulseConfig{Values: map[string]string{pulse.KeyKafkaServers: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.pulse == nil || r.pulse.producer == nil {
		t.Fatal("pulse producer not opened", r.pulseError)
	}
	rec := httptest.NewRecorder()
	r.healthHandler(rec, httptest.NewRequest("GET", "/health", nil))
	var health struct {
		Pulse map[string]json.RawMessage `json:"pulse"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &health); err != nil || health.Pulse["kafka"] == nil || health.Pulse["dispatcher"] == nil {
		t.Fatalf("pulse health %s %v", rec.Body.String(), err)
	}
}

// blockingSink holds every record until release is closed.
type blockingSink struct{ release chan struct{} }

func (b *blockingSink) Enqueue(ctx context.Context, _ pulse.Record) error {
	select {
	case <-b.release:
	case <-ctx.Done():
	}
	return nil
}

// A queue-drop event that finds the runtime's event queue full is counted as
// dropped and not charged to the dispatcher's rate limit: the next drop is
// delivered at once instead of 10s later.
func TestPulseDropEventRetriedWhenEventQueueFull(t *testing.T) {
	sink := &blockingSink{release: make(chan struct{})}
	original := newPulseSink
	newPulseSink = func(pulse.Config, int16, func(kafka.ErrorEvent)) (pulse.Sink, *kafka.Producer, error) {
		return sink, nil, nil
	}
	defer func() { newPulseSink = original }()
	root := t.TempDir()
	r, err := Open(Config{Directory: root, Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>"),
		Pulse: &PulseConfig{Values: map[string]string{pulse.KeyKafkaServers: "127.0.0.1:1", pulse.KeyKafkaDispatchQueueCapacity: "1", pulse.KeyKafkaDispatchWorkerCount: "1"}}})
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			close(sink.release)
		}
		r.Close()
	}()
	ctx := context.Background()
	now := time.Now().UnixMilli()
	const obj, svc = int32(4242), int32(777)
	for _, p := range []protocol.Pack{
		&protocol.TextPack{XType: "object", Hash: obj, Text: "host/instance/pulse-app@node-1"},
		&protocol.TextPack{XType: "service", Hash: svc, Text: "GET /pulse/<id>"},
	} {
		if err := r.engine.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	s := r.pulse
	for len(s.events) < cap(s.events) {
		s.events <- pulse.Event{Queue: "test", Kind: "filler"}
	}
	offer := func() bool {
		return s.dispatcher.OfferXLog(&protocol.XLogPack{EndTime: now, ObjHash: obj, Service: svc, TxID: -99, Elapsed: 12})
	}
	deadline := time.Now().Add(5 * time.Second)
	for offer() {
		if time.Now().After(deadline) {
			t.Fatal("the dispatch queue never filled")
		}
		time.Sleep(time.Millisecond)
	}
	if s.dropped.Load() == 0 {
		t.Fatal("drop event into a full event queue not counted")
	}
	for len(s.events) > 0 {
		<-s.events
	}
	if offer() {
		t.Fatal("offer accepted while the sink is blocked")
	}
	select {
	case e := <-s.events:
		if e.Kind != "dropped" {
			t.Fatalf("event %+v", e)
		}
	default:
		t.Fatal("the next drop event was suppressed by the rate limit")
	}
	close(sink.release)
	released = true
}
