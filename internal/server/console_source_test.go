package server

import (
	"bytes"
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/console"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/pulse"
	"github.com/snowvil/scouter-go/internal/pulse/kafka"
	"github.com/snowvil/scouter-go/internal/storage"
)

// TestConsoleSourceOverRuntime runs console commands against a real runtime
// fed with packs: the Source adapter, the counters.xml engine and the
// debug queue key set.
func TestConsoleSourceOverRuntime(t *testing.T) {
	xml, err := os.ReadFile("../../cmd/scouter-go/counters.xml")
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: xml})
	if err != nil {
		t.Fatal(err)
	}
	src := r.ConsoleSource()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	// Run starts the UDP receiver asynchronously; debug queue reports its
	// queue capacity only once Serve owns the queue.
	for deadline := time.Now().Add(5 * time.Second); r.udp.Stats().QueueCap == 0; {
		if time.Now().After(deadline) {
			t.Fatal("UDP receiver did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	name := "/host1/tomcat1"
	hash := storage.CounterHash(name)
	now := time.Now().UnixMilli()
	for _, p := range []protocol.Pack{
		&protocol.ObjectPack{ObjType: "tomcat", ObjHash: hash, ObjName: name, Address: "10.0.0.1", Tags: protocol.Value{Type: protocol.ValueMap}},
		&protocol.PerfCounterPack{ObjName: name, Time: now, TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{
			{Key: "TPS", Value: protocol.Value{Type: protocol.ValueFloat, Float: 12.5}},
			{Key: "ActiveService", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 3}},
		}}},
		&protocol.TextPack{XType: "service", Hash: 77, Text: "/console"},
		&protocol.XLogPack{EndTime: now, ObjHash: hash, Service: 77, TxID: 1, Elapsed: 2500, Error: 1},
	} {
		if err := r.engine.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	tomcat, ok := src.ObjectType("tomcat")
	if !ok || !tomcat.HasFamily || tomcat.Family != "javaee" || len(tomcat.Counters) == 0 {
		t.Fatalf("tomcat type %+v", tomcat)
	}
	if _, ok = src.ObjectType("no-such-type"); ok {
		t.Fatal("unknown type found")
	}
	var out, errs bytes.Buffer
	c, err := console.New(console.Config{Source: src, Out: &out, Err: &errs, Location: time.UTC, Interval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, line := range []string{"objtype", "object", "counter tomcat", "realtime tomcat TPS", "dashboard tomcat", "xlog tomcat", "debug queue", ""} {
		c.Execute(ctx, line)
	}
	got := out.String()
	for _, want := range []string{
		"\ttomcat\n",
		"\ttomcat              \t/host1/tomcat1                \talive\n",
		"\tTPS\n", "\tActiveService\n",
		" /host1/tomcat1 12\n",
		"     1     3    12     0     0\n",
		strings.Repeat("_", 15) + "E" + strings.Repeat("_", 44) + " 10sec 1\n",
		"ingest.completed=", "udp.datagrams=0\n", "tcp.active=0\n", "server.recoveries=0\n",
		"agentcall.kube-seq.queue=0\n", "udp.queue=0\n", "udp.queue_capacity=8192\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
	if errs.Len() != 0 {
		t.Fatalf("errors: %s", errs.String())
	}
	var keys []string
	for _, s := range src.Stats() {
		if s.Key != "udp.socket_drops" { // Linux only (KernelCounters)
			keys = append(keys, s.Key)
		}
	}
	sort.Strings(keys)
	want := "agentcall.cpu-dump.abandoned agentcall.cpu-dump.done agentcall.cpu-dump.dropped agentcall.cpu-dump.enqueued agentcall.cpu-dump.failed agentcall.cpu-dump.queue " +
		"agentcall.kube-seq.abandoned agentcall.kube-seq.done agentcall.kube-seq.dropped agentcall.kube-seq.enqueued agentcall.kube-seq.failed agentcall.kube-seq.queue " +
		"ingest.completed ingest.dropped_xlogs ingest.geoip_errors ingest.geoip_text_errors ingest.objects ingest.rejected ingest.skipped_stacks ingest.storage_errors " +
		ingestWriterKeys() + "ingest.xlogs " +
		"server.dropped_error_logs server.error_log_queue server.maintenance_errors server.purged server.recoveries " +
		"tcp.accepted tcp.active tcp.client_requests tcp.errors tcp.rejected tcp.stack_uploads " +
		"udp.aborted_assemblies udp.aborted_packets udp.assembly_rejected udp.datagrams udp.expired udp.handler_errors udp.malformed udp.packets udp.queue udp.queue_capacity udp.rejected udp.shutdown_discarded"
	if strings.Join(keys, " ") != want {
		t.Fatalf("debug queue keys\n got %s\nwant %s", strings.Join(keys, " "), want)
	}
	// visitor/tagcnt/xlist reach storage without errors.
	out.Reset()
	for _, line := range []string{"visitor tomcat", "visitor tomcat1", "tagcnt top100 tomcat service error", "xlist tomcat 00:00:00.000"} {
		c.Execute(ctx, line)
	}
	if errs.Len() != 0 {
		t.Fatalf("errors: %s", errs.String())
	}
	if !strings.HasPrefix(out.String(), "\t0\n") {
		t.Fatalf("visitor: %q", out.String())
	}
}

// TestConsoleSourceAfterClose: stats stay readable and queries fail with
// an error (never a panic) once the runtime is closed.
func TestConsoleSourceAfterClose(t *testing.T) {
	r, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>")})
	if err != nil {
		t.Fatal(err)
	}
	src := r.ConsoleSource()
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if len(src.Stats()) == 0 {
		t.Fatal("no stats")
	}
	if _, ok := src.ObjectType("tomcat"); ok {
		t.Fatal("empty counters.xml has no types")
	}
	_, _ = src.Objects(context.Background())
	_, _ = src.TagValues(context.Background(), "20260101", "t", "service", "error")
}

// TestConsoleStatsIncludeServerStat: every Runtime.ServerStat entry (Pulse
// queues included) appears in `debug queue` with the same name and value.
func TestConsoleStatsIncludeServerStat(t *testing.T) {
	original := newPulseSink
	newPulseSink = func(pulse.Config, int16, func(kafka.ErrorEvent)) (pulse.Sink, *kafka.Producer, error) {
		return &memorySink{}, nil, nil
	}
	defer func() { newPulseSink = original }()
	values := map[string]string{"ext_plugin_pulse_kafka_servers": "127.0.0.1:1"}
	r, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>"), Pulse: &PulseConfig{Values: values, Acks: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	stats := map[string]int64{}
	for _, s := range r.ConsoleSource().Stats() {
		if _, dup := stats[s.Key]; dup {
			t.Fatalf("duplicate key %s", s.Key)
		}
		stats[s.Key] = s.Value
	}
	entries := r.ServerStat()
	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name] = true
		if v, ok := stats[e.Name]; !ok || v != e.Value {
			t.Errorf("ServerStat %s=%d, debug queue %d (present %v)", e.Name, e.Value, v, ok)
		}
	}
	for _, n := range []string{statKubeSeqQueue, statCPUDumpQueue, statPulseXLogQueue, statPulseProfileQueue} {
		if !names[n] {
			t.Errorf("ServerStat lacks %s; the comparison would not cover it", n)
		}
	}
	for k := range stats {
		if strings.HasSuffix(k, "core.queue") || strings.HasSuffix(k, "db.queue") {
			t.Errorf("Java stage queue name %s must not be faked", k)
		}
	}
}

// ingestWriterKeys are the sorted `debug queue` keys of the ingest record
// writers, each followed by a space.
func ingestWriterKeys() string {
	var keys []string
	for _, kind := range []string{"alert", "batch", "counter", "daily", "object", "span", "stack", "status", "summary", "visitor"} {
		for _, field := range []string{"abandoned", "dropped", "enqueued", "queue", "write_errors", "written"} {
			keys = append(keys, "ingest.writer."+kind+"."+field)
		}
	}
	return strings.Join(keys, " ") + " "
}
