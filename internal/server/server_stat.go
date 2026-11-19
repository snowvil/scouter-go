package server

import (
	"sort"
	"strconv"
	"strings"

	"github.com/snowvil/scouter-go/internal/ingest"
)

// ServerStatEntry is one row of the Java core.ServerStat table (queue name ->
// size) as shown by the console "DEBUG queue" command.
type ServerStatEntry struct {
	Name  string
	Value int64
}

// Go queue names reported by ServerStat. The Java names
// (xlog.core0.queue, xlog.core.queue, xlog.core-r.queue, profile.core0.queue,
// profile.core.queue, profile.core-r.queue, text.core.queue, alert.core.queue,
// visitor.core.queue, span.core.queue, xlog.db.queue, text.db.queue,
// profile.db.queue, summary.db.queue, zipkin-span.db.queue) have no Go
// counterpart of the same shape: the engine processes every pack on the UDP
// worker under its lock and hands the records to one writer queue per kind
// (ingest.writer.<kind>.queue, internal/ingest CONTRACT.md §5, §13), the XLog,
// profile and text writers are in storage. The Java names are not emitted as
// zero, which would read as an empty healthy queue.
const (
	statKubeSeqQueue      = "agentcall.kube-seq.queue"
	statCPUDumpQueue      = "agentcall.cpu-dump.queue"
	statPulseXLogQueue    = "pulse.xlog.queue"
	statPulseProfileQueue = "pulse.profile.queue"
	statPulseKafkaQueue   = "pulse.kafka.queue"
)

// ingestWriterQueues are the ingest record writer queue lengths (pending
// records), "ingest.writer.<kind>.queue".
func ingestWriterQueues(e *ingest.Engine) []ServerStatEntry {
	ws := e.WriterStats()
	out := make([]ServerStatEntry, len(ws))
	for i, w := range ws {
		out[i] = ServerStatEntry{"ingest.writer." + w.Kind + ".queue", int64(w.Pending)}
	}
	return out
}

// ServerStat returns the current length of every bounded Go queue, sorted by
// name like ServerStat.toString (SortUtil.sort over String keys; the names
// are ASCII, so byte order equals String.compareTo). Java stores the size seen
// when a core thread last dequeued; Go reports the length at the call.
func (r *Runtime) ServerStat() []ServerStatEntry {
	var out []ServerStatEntry
	if r.engine != nil {
		st := r.engine.Stats()
		out = append(out, ServerStatEntry{statKubeSeqQueue, int64(st.KubeSeqCalls().Pending)},
			ServerStatEntry{statCPUDumpQueue, int64(st.CPUDumpCalls().Pending)})
		out = append(out, ingestWriterQueues(r.engine)...)
	}
	if p := r.pulse; p != nil {
		d := p.dispatcher.Stats()
		out = append(out, ServerStatEntry{statPulseXLogQueue, int64(d.XLog.Pending)},
			ServerStatEntry{statPulseProfileQueue, int64(d.Profile.Pending)})
		if p.producer != nil {
			out = append(out, ServerStatEntry{statPulseKafkaQueue, p.producer.Stats().Pending})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// FormatServerStat is ServerStat.toString(key, prefix): every entry whose name
// contains key (all when key is empty) as "prefix name=value\n".
func FormatServerStat(entries []ServerStatEntry, key, prefix string) string {
	var b strings.Builder
	for _, e := range entries {
		if key == "" || strings.Contains(e.Name, key) {
			b.WriteString(prefix)
			b.WriteString(e.Name)
			b.WriteByte('=')
			b.WriteString(strconv.FormatInt(e.Value, 10))
			b.WriteByte('\n')
		}
	}
	return b.String()
}
