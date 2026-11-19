package server

import (
	"context"
	"sync"

	"github.com/snowvil/scouter-go/internal/console"
	"github.com/snowvil/scouter-go/internal/counters"
	"github.com/snowvil/scouter-go/internal/ingest"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/pulse"
	"github.com/snowvil/scouter-go/internal/pulse/kafka"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/transport"
)

// ConsoleSource returns the console's view of the running server. Call it
// after Open and before Run: it captures the components once, so it never
// reads Runtime fields that Close resets. After Close the queries fail with
// the components' closed errors and the statistics stay readable.
func (r *Runtime) ConsoleSource() console.Source {
	s := &consoleSource{engine: r.engine, udp: r.udp, tcp: r.tcp, runtime: r, site: r.countersSite, xml: r.config.CounterXML}
	if p := r.pulse; p != nil {
		s.pulse = p.dispatcher
		if p.producer != nil {
			s.kafka = p.producer.Stats
		}
	}
	return s
}

type consoleSource struct {
	engine  *ingest.Engine
	udp     *transport.UDPReceiver
	tcp     *transport.TCPServer
	runtime *Runtime
	pulse   *pulse.Dispatcher
	kafka   func() kafka.Stats
	site    *counters.Store
	xml     []byte

	once     sync.Once
	fallback *counters.Engine // counters.xml when there is no counters.site.xml store
}

// counterEngine is CounterManager.getCounterEngine(): the site store when
// ConfDir is set (it re-reads counters.site.xml), else the embedded default.
func (s *consoleSource) counterEngine() *counters.Engine {
	if s.site != nil {
		return s.site.Engine()
	}
	s.once.Do(func() {
		e := counters.NewEngine(0)
		if err := e.Parse(s.xml); err == nil {
			s.fallback = e
		}
	})
	return s.fallback
}

func (s *consoleSource) Objects(ctx context.Context) ([]console.Object, error) {
	packs, err := s.engine.Objects(ctx, "")
	if err != nil {
		return nil, err
	}
	out := make([]console.Object, len(packs))
	for i, p := range packs {
		out[i] = console.Object{Hash: p.ObjHash, Type: p.ObjType, Name: p.ObjName, Alive: p.Alive}
	}
	return out, nil
}

func (s *consoleSource) ObjectType(name string) (console.ObjectType, bool) {
	e := s.counterEngine()
	if e == nil {
		return console.ObjectType{}, false
	}
	t := e.ObjectType(name)
	if t == nil {
		return console.ObjectType{}, false
	}
	out := console.ObjectType{HasFamily: t.Family != nil}
	if t.Family == nil {
		return out, true
	}
	out.Family = t.Family.Name
	for _, c := range t.Family.Counters() {
		if c.All {
			out.Counters = append(out.Counters, c.Name)
		}
	}
	for _, c := range t.Counters() {
		if c.All {
			out.Counters = append(out.Counters, c.Name)
		}
	}
	return out, true
}

// realtimeKind is TimeTypeEnum.REALTIME.
const realtimeKind byte = 1

func (s *consoleSource) RealtimeCounter(ctx context.Context, hash int32, name string) (protocol.Value, bool, error) {
	return s.engine.Counter(ctx, hash, name, realtimeKind)
}

func (s *consoleSource) XLogsSince(ctx context.Context, loop int64, index int32, hashes []int32) (int64, int32, []*protocol.XLogPack, error) {
	res, err := s.engine.XLogsRealtime(ctx, service.RealtimeQuery{Loop: loop, Index: index, ObjectHashes: hashes})
	return res.Loop, res.Index, res.Records, err
}

func (s *consoleSource) XLogsByTime(ctx context.Context, date string, from, to int64) ([]*protocol.XLogPack, error) {
	return s.engine.XLogsByTime(ctx, date, from, to)
}

func (s *consoleSource) Text(ctx context.Context, date, div string, hash int32) (string, bool, error) {
	return s.engine.Text(ctx, date, div, hash)
}

func (s *consoleSource) TagValues(ctx context.Context, date, objType, group, tag string) ([]console.TagValue, error) {
	total, err := s.engine.TagValues(ctx, date, objType, group, tag)
	if err != nil {
		return nil, err
	}
	out := make([]console.TagValue, len(total.Values))
	for i, v := range total.Values {
		out[i] = console.TagValue{Value: v.Value, Count: v.Count}
	}
	return out, nil
}

func (s *consoleSource) TagValueSeries(ctx context.Context, date, objType, group, tag string, value protocol.Value) ([]float32, error) {
	return s.engine.TagValueSeries(ctx, date, objType, group, tag, value)
}

func (s *consoleSource) VisitorObjType(ctx context.Context, objType string) (int64, error) {
	return s.engine.VisitorRealtime(ctx, nil, objType)
}

func (s *consoleSource) VisitorObject(ctx context.Context, hash int32) (int64, error) {
	return s.engine.VisitorRealtime(ctx, []int32{hash}, "")
}

// serverStat is Runtime.ServerStat over the components captured by
// ConsoleSource (same names, same values). It does not read Runtime.pulse,
// which Close resets while a console cycle may still run;
// TestConsoleStatsIncludeServerStat keeps both in step.
func (s *consoleSource) serverStat() []ServerStatEntry {
	st := s.engine.Stats()
	out := []ServerStatEntry{
		{statKubeSeqQueue, int64(st.KubeSeqCalls().Pending)},
		{statCPUDumpQueue, int64(st.CPUDumpCalls().Pending)},
	}
	out = append(out, ingestWriterQueues(s.engine)...)
	if s.pulse != nil {
		d := s.pulse.Stats()
		out = append(out, ServerStatEntry{statPulseXLogQueue, int64(d.XLog.Pending)}, ServerStatEntry{statPulseProfileQueue, int64(d.Profile.Pending)})
		if s.kafka != nil {
			out = append(out, ServerStatEntry{statPulseKafkaQueue, s.kafka().Pending})
		}
	}
	return out
}

// Stats is the `debug queue` key set. It starts with the ServerStat queue
// lengths (Runtime.ServerStat names; the Java stage queues have no Go
// counterpart and are not faked) and the UDP worker queue, then adds the Go
// pipeline counters. Every value is an atomic snapshot, safe while the
// runtime runs or after Close.
func (s *consoleSource) Stats() []console.Stat {
	in, udp, tcp := s.engine.Stats(), s.udp.Stats(), s.tcp.Stats()
	r := s.runtime
	var stats []console.Stat
	for _, e := range s.serverStat() {
		stats = append(stats, console.Stat{Key: e.Name, Value: e.Value})
	}
	agentCalls := func(name string, a ingest.AgentCallStats) {
		stats = append(stats,
			console.Stat{Key: "agentcall." + name + ".enqueued", Value: int64(a.Enqueued)},
			console.Stat{Key: "agentcall." + name + ".done", Value: int64(a.Done)},
			console.Stat{Key: "agentcall." + name + ".failed", Value: int64(a.Failed)},
			console.Stat{Key: "agentcall." + name + ".dropped", Value: int64(a.Dropped)},
			console.Stat{Key: "agentcall." + name + ".abandoned", Value: int64(a.Abandoned)},
		)
	}
	agentCalls("kube-seq", in.KubeSeqCalls())
	agentCalls("cpu-dump", in.CPUDumpCalls())
	for _, w := range s.engine.WriterStats() {
		prefix := "ingest.writer." + w.Kind
		stats = append(stats,
			console.Stat{Key: prefix + ".enqueued", Value: int64(w.Enqueued)},
			console.Stat{Key: prefix + ".written", Value: int64(w.Written)},
			console.Stat{Key: prefix + ".write_errors", Value: int64(w.WriteErrors)},
			console.Stat{Key: prefix + ".dropped", Value: int64(w.Dropped)},
			console.Stat{Key: prefix + ".abandoned", Value: int64(w.Abandoned)},
		)
	}
	stats = append(stats, []console.Stat{
		{Key: "udp.queue", Value: int64(udp.QueueLen)},
		{Key: "udp.queue_capacity", Value: int64(udp.QueueCap)},
		{Key: "ingest.completed", Value: int64(in.Completed)},
		{Key: "ingest.rejected", Value: int64(in.Rejected)},
		{Key: "ingest.storage_errors", Value: int64(in.StorageErrors)},
		{Key: "ingest.xlogs", Value: int64(in.XLogs)},
		{Key: "ingest.dropped_xlogs", Value: int64(in.DroppedXLogs)},
		{Key: "ingest.skipped_stacks", Value: int64(in.SkippedStacks)},
		{Key: "ingest.geoip_errors", Value: int64(in.GeoIPErrors)},
		{Key: "ingest.geoip_text_errors", Value: int64(in.GeoIPTextErrors)},
		{Key: "ingest.objects", Value: int64(s.engine.ObjectCount())},
		{Key: "udp.datagrams", Value: int64(udp.Datagrams)},
		{Key: "udp.packets", Value: int64(udp.Packets)},
		{Key: "udp.rejected", Value: int64(udp.Rejected)},
		{Key: "udp.malformed", Value: int64(udp.Malformed)},
		{Key: "udp.expired", Value: int64(udp.Expired)},
		{Key: "udp.handler_errors", Value: int64(udp.HandlerErrors)},
		{Key: "udp.aborted_assemblies", Value: int64(udp.AbortedAssemblies)},
		{Key: "udp.aborted_packets", Value: int64(udp.AbortedPackets)},
		{Key: "udp.shutdown_discarded", Value: int64(udp.ShutdownDiscarded)},
		{Key: "udp.assembly_rejected", Value: int64(udp.AssemblyRejected)},
		{Key: "tcp.accepted", Value: int64(tcp.Accepted)},
		{Key: "tcp.rejected", Value: int64(tcp.Rejected)},
		{Key: "tcp.errors", Value: int64(tcp.Errors)},
		{Key: "tcp.client_requests", Value: int64(tcp.ClientRequests)},
		{Key: "tcp.stack_uploads", Value: int64(tcp.StackUploads)},
		{Key: "tcp.active", Value: tcp.Active},
		{Key: "server.error_log_queue", Value: int64(len(r.errorEvents))},
		{Key: "server.dropped_error_logs", Value: int64(r.droppedErrorEvents.Load())},
		{Key: "server.maintenance_errors", Value: int64(r.maintenanceErrors.Load())},
		{Key: "server.purged", Value: int64(r.purged.Load())},
		{Key: "server.recoveries", Value: int64(r.recoveries.Load())},
	}...)
	if udp.KernelCounters {
		// Linux /proc: datagrams the kernel dropped on this socket.
		stats = append(stats, console.Stat{Key: "udp.socket_drops", Value: int64(udp.SocketDrops)})
	}
	if s.pulse != nil {
		p := s.pulse.Stats()
		stats = append(stats,
			console.Stat{Key: "pulse.xlog.enqueued", Value: int64(p.XLog.Enqueued)},
			console.Stat{Key: "pulse.xlog.sent", Value: int64(p.XLog.Sent)},
			console.Stat{Key: "pulse.xlog.dropped", Value: int64(p.XLog.Dropped)},
			console.Stat{Key: "pulse.xlog.failed", Value: int64(p.XLog.Failed)},
			console.Stat{Key: "pulse.profile.enqueued", Value: int64(p.Profile.Enqueued)},
			console.Stat{Key: "pulse.profile.sent", Value: int64(p.Profile.Sent)},
			console.Stat{Key: "pulse.profile.dropped", Value: int64(p.Profile.Dropped)},
			console.Stat{Key: "pulse.profile.failed", Value: int64(p.Profile.Failed)},
		)
	}
	if s.kafka != nil {
		k := s.kafka()
		stats = append(stats,
			console.Stat{Key: "kafka.failed", Value: int64(k.Failed)},
			console.Stat{Key: "kafka.dropped", Value: int64(k.Dropped)},
		)
	}
	return stats
}
