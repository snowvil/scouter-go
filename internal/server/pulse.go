package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/pulse"
	"github.com/snowvil/scouter-go/internal/pulse/kafka"
)

// PulseConfig enables the built-in Pulse stream (the Go port of the
// Pulse Scouter plugin). Values are the Java scouter.conf keys
// (ext_plugin_pulse_*, plugin_pulse_kafka_topic_partition_count); unset keys
// take the plugin defaults, except that the application-name filters are
// empty (pulseAppFilters). Acks is a Go-only producer option (contract: 0).
// MaxXLogQueueBytes and MaxProfileQueueBytes are the Go-only queue byte
// bounds (pulse.Options); zero takes go_pulse_xlog_queue_mb /
// go_pulse_profile_queue_mb, else the pulse defaults.
type PulseConfig struct {
	Values                                  map[string]string
	Acks                                    int16
	MaxXLogQueueBytes, MaxProfileQueueBytes int64
}

// ReadPulseProperties parses a Java properties subset (key=value or key:value,
// # and ! comments) and returns the pulse_* keys; other keys are ignored like
// the Java Configure does. Unknown pulse keys are reported so typos surface.
func ReadPulseProperties(r io.Reader) (map[string]string, []string, error) {
	values := map[string]string{}
	var unknown []string
	known := map[string]bool{}
	for _, k := range pulse.JavaKeys() {
		known[k] = true
	}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}
		// java.util.Properties: a line without a separator is a key with an
		// empty value (so a bare ext_plugin_pulse_enable disables the stream).
		key, value := line, ""
		if i := strings.IndexAny(line, "=:"); i >= 0 {
			key, value = strings.TrimSpace(line[:i]), strings.TrimSpace(line[i+1:])
		}
		if !strings.Contains(key, "plugin_pulse") {
			continue
		}
		if !known[key] {
			unknown = append(unknown, key)
			continue
		}
		values[key] = value
	}
	return values, unknown, scanner.Err()
}

// pulseStream owns the dispatcher and the Kafka producer. The producer is the
// single sink; the dispatcher workers call Enqueue, which never waits on the
// network, so ingest is never blocked by Kafka (contract).
type pulseStream struct {
	dispatcher *pulse.Dispatcher
	producer   *kafka.Producer
	sink       pulse.Sink
	events     chan pulse.Event
	dropped    atomic.Uint64
	sinkErrors atomic.Uint64
	debug      bool
}

// newPulseSink is replaced by tests to observe records without a broker.
var newPulseSink = func(cfg pulse.Config, acks int16, report func(kafka.ErrorEvent)) (pulse.Sink, *kafka.Producer, error) {
	servers := strings.Split(cfg.KafkaServers, ",")
	bootstrap := servers[:0]
	for _, s := range servers {
		if s = strings.TrimSpace(s); s != "" {
			bootstrap = append(bootstrap, s)
		}
	}
	if len(bootstrap) == 0 {
		return nil, nil, fmt.Errorf("%s is empty", pulse.KeyKafkaServers)
	}
	timeout := time.Duration(cfg.ProduceTimeoutMillis) * time.Millisecond
	p, err := kafka.NewProducer(kafka.Config{Bootstrap: bootstrap, ClientID: "scouter-go-pulse", Acks: acks, RequestTimeout: timeout, DeliveryTimeout: timeout + 100*time.Millisecond, OnError: report})
	if err != nil {
		return nil, nil, err
	}
	return kafkaSink{p}, p, nil
}

type kafkaSink struct{ p *kafka.Producer }

func (s kafkaSink) Enqueue(ctx context.Context, rec pulse.Record) error {
	headers := make([]kafka.Header, len(rec.Headers))
	for i, h := range rec.Headers {
		headers[i] = kafka.Header{Key: h.Key, Value: h.Value}
	}
	return s.p.Enqueue(ctx, kafka.Record{Topic: rec.Topic, Partition: rec.Partition, Key: rec.Key, Value: rec.Value, Headers: headers})
}

// openPulse builds the stream. Like the Java plugin, which catches its own
// initialisation failure and leaves itself disabled, a bad Pulse
// configuration must not stop Scouter collection: the error is logged, kept
// for /health and the stream stays off.
func (r *Runtime) openPulse(cfg *PulseConfig, lookup pulse.TextLookup, location *time.Location) error {
	err := r.openPulseStrict(cfg, lookup, location)
	if err != nil {
		log.Printf("pulse disabled: %v", err)
		r.pulseError = err.Error()
	}
	return nil
}

func (r *Runtime) openPulseStrict(cfg *PulseConfig, lookup pulse.TextLookup, location *time.Location) error {
	conf, err := pulse.ParseJavaConfig(cfg.Values)
	if err != nil {
		return fmt.Errorf("pulse configuration: %w", err)
	}
	if !conf.Enable {
		log.Printf("pulse: %s=false, stream disabled", pulse.KeyEnable)
		return nil
	}
	s := &pulseStream{events: make(chan pulse.Event, 64), debug: conf.Debug}
	report := func(e kafka.ErrorEvent) {
		s.sinkErrors.Add(1)
		select {
		case s.events <- pulse.Event{Queue: "kafka", Kind: e.Operation, Detail: e.Broker, Err: e.Err}:
		default:
			s.dropped.Add(1)
		}
	}
	s.sink, s.producer, err = newPulseSink(conf, cfg.Acks, report)
	if err != nil {
		return err
	}
	s.dispatcher, err = pulse.New(pulse.Options{Config: conf, Sink: s.sink, Lookup: lookup, Location: location, TryEvent: s.tryEvent,
		MaxXLogQueueBytes: cfg.MaxXLogQueueBytes, MaxProfileQueueBytes: cfg.MaxProfileQueueBytes})
	if err != nil {
		if s.producer != nil {
			s.producer.Close(context.Background())
		}
		return err
	}
	r.pulse = s
	log.Printf("pulse enabled: topics %s/%s partitions %d workers %d queue %d acks %d", conf.XLogMetaTopic, conf.XLogProfileTopic, conf.TopicPartitionCount, conf.DispatchWorkerCount, conf.DispatchQueueCapacity, cfg.Acks)
	for _, w := range pulseAppFilterWarnings(cfg.Values, conf.EnableSpringURIFilter) {
		log.Print(w)
	}
	return nil
}

// pulseAppFilters are the application-name filters whose plugin defaults
// (in its settings loader) name the services of one deployment. Go ships
// them empty (internal/pulse CONTRACT.md "의도된 차이"), so a key missing from
// the configuration is reported at startup. A key set to an empty value
// ("key=") is a deliberate choice and is not reported. The non-Spring list
// only matters while the Spring URI filter is on.
var pulseAppFilters = []struct {
	key, effect string
	springOnly  bool
}{
	{pulse.KeyExcludeApplications, "no application is excluded", false},
	{pulse.KeyRebaseTransactionApps, "no application's transactions are rebased", false},
	{pulse.KeyNonSpringBasedApplications, "the Spring URI filter applies to every application", true},
}

func pulseAppFilterWarnings(values map[string]string, springFilter bool) []string {
	var out []string
	for _, f := range pulseAppFilters {
		if f.springOnly && !springFilter {
			continue
		}
		if _, ok := values[f.key]; !ok {
			out = append(out, fmt.Sprintf("WARNING: pulse: %s is not set: %s (the original plugin default listed deployment-specific names; set %s= to leave it empty)", f.key, f.effect, f.key))
		}
	}
	return out
}

// tryEvent queues a dispatcher event for logEvent without blocking. A full
// queue counts the event as dropped and returns false, so the dispatcher
// sends its next rate-limited event (queue drop, worker panic) at the next
// occurrence instead of after its interval.
func (s *pulseStream) tryEvent(e pulse.Event) bool {
	select {
	case s.events <- e:
		return true
	default:
		s.dropped.Add(1)
		return false
	}
}

// logEvent drains one queued event per tick with a bounded message, like the
// transport error log. Filter events are logged only in debug mode (Java
// filter log is gated by ext_plugin_pulse_debug).
func (s *pulseStream) logEvent() {
	select {
	case e := <-s.events:
		if e.Kind != "sink" && e.Kind != "error" && e.Kind != "dropped" && e.Kind != "shutdown" && e.Queue != "kafka" && !s.debug {
			return
		}
		msg := e.Detail
		if e.Err != nil {
			msg = e.Err.Error()
		}
		if len(msg) > 256 {
			msg = msg[:256]
		}
		log.Printf("pulse %s %s txid=%d: %q", e.Queue, e.Kind, e.TxID, msg)
	default:
	}
}

// close drains the dispatcher queues, then the producer buffer, each within a
// bound; leftovers are counted, never silently lost.
func (s *pulseStream) close() (pulse.Stats, kafka.Stats, int64, error) {
	// Separate budgets: a slow dispatcher drain must not eat the producer's.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.dispatcher.Close(ctx)
	var abandoned int64
	var kstats kafka.Stats
	if s.producer != nil {
		pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer pcancel()
		var perr error
		abandoned, perr = s.producer.Close(pctx)
		err = errors.Join(err, perr)
		kstats = s.producer.Stats()
	}
	return s.dispatcher.Stats(), kstats, abandoned, err
}

// pulseLookup approximates Java TextRD.getString: the plugin reads the
// date-independent TextCache (every recently received TextPack) before the
// daily file. Go has no such cache, so a miss on the event date falls back to
// the receive date (today), which covers texts received after midnight for an
// XLog that ended before it. Texts evicted from the Java LRU but still in
// yesterday's file are an intended superset.
func pulseLookup(engine interface {
	Text(ctx context.Context, date, div string, hash int32) (string, bool, error)
}, location *time.Location) pulse.TextLookup {
	return func(ctx context.Context, date, div string, hash int32) (string, bool, error) {
		text, ok, err := engine.Text(ctx, date, div, hash)
		if err != nil || ok {
			return text, ok, err
		}
		if today := storageDate(time.Now().UnixMilli(), location); today != date {
			return engine.Text(ctx, today, div, hash)
		}
		return text, ok, err
	}
}
