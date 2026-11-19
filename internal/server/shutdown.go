package server

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/ingest"
)

// Shutdown stages of Run and Close, in order (CONTRACT §shutdown). main
// bounds the whole shutdown (--shutdown-timeout) and, when the budget runs
// out, logs ShutdownReport: the stage that did not finish and the queues
// still holding records.
const (
	stageRunning    = "running"
	stageListeners  = "listeners"      // UDP/TCP Serve returning
	stageBackground = "background"     // maintain (periodic sync, purge, visitor flush), tag flush, ticks, agent calls
	stageFinalStats = "final stats"    // Run's last counter line (VisitorStats waits for a visitor write in progress)
	stageHTTP       = "http"           // --http and servlet listeners
	stageCloseNet   = "listener close" // UDP/TCP sockets and connections
	stageIngest     = "ingest writers" // Engine.Close: deferred writes and the record writers (bounded)
	stageSQLTable   = "sqltable"
	stagePulse      = "pulse"
	stageCounters   = "counters" // Counters.Close: fsync and sealed headers
	stageStore      = "store"    // Store.Close (sub-stage: Store.CloseStage)
	stageWriterLock = "writer lock"
	stageClosed     = "closed"
)

// ShutdownStageHook, when set before Run, runs at the start of every
// shutdown stage on the goroutine that enters it. Tests only: the
// cmd/scouter-go tests stall a stage with it the way a hung disk stalls a
// call that never sees a context.
var ShutdownStageHook func(stage string)

// shutdownState tracks Run's teardown for ShutdownStarted and
// ShutdownReport, which other goroutines read.
type shutdownState struct {
	once    sync.Once
	started chan struct{} // closed when the teardown begins
	at      atomic.Int64  // UnixNano of the beginning
	stage   atomic.Pointer[string]
	pulse   atomic.Pointer[pulseStream] // r.pulse as Open set it (Close clears the field)
}

func newShutdownState() *shutdownState {
	s := &shutdownState{started: make(chan struct{})}
	running := stageRunning
	s.stage.Store(&running)
	return s
}

// beginShutdown marks the start of the teardown once (Run's context ended,
// or a listener or the heartbeat failed). A Runtime not made by Open (tests)
// has no state to track.
func (r *Runtime) beginShutdown() {
	if r.shutdown == nil {
		return
	}
	r.shutdown.once.Do(func() {
		r.shutdown.at.Store(time.Now().UnixNano())
		close(r.shutdown.started)
	})
}

// enterStage records the stage Run or Close is about to wait on.
func (r *Runtime) enterStage(stage string) {
	if r.shutdown == nil {
		return
	}
	r.shutdown.stage.Store(&stage)
	if h := ShutdownStageHook; h != nil {
		h(stage)
	}
}

// ShutdownStarted is closed when Run starts tearing down: its context ended
// or a listener or the writer heartbeat failed.
// A Runtime not made by Open returns nil (never ready).
func (r *Runtime) ShutdownStarted() <-chan struct{} {
	if r.shutdown == nil {
		return nil
	}
	return r.shutdown.started
}

// ShutdownReport describes the shutdown in progress in one line: the stage,
// the time since the teardown began, the Client commands and Agent relays
// still running, what is still queued (ingest record writers with pending
// records, the XLog/profile writer queues, Pulse dispatcher and Kafka
// producer) and whether Store.Close wrote the clean-shutdown marker. It
// reads only atomics and leaf locks that are never held across I/O, never a
// storage gate, so it answers while a stage is stuck in a write.
func (r *Runtime) ShutdownReport() string {
	if r.shutdown == nil {
		return `stage="unknown"`
	}
	var b strings.Builder
	stage := *r.shutdown.stage.Load()
	if stage == stageStore && r.store != nil {
		if sub := r.store.CloseStage(); sub != "" {
			stage += "/" + sub
		}
	}
	fmt.Fprintf(&b, "stage=%q", stage)
	if at := r.shutdown.at.Load(); at != 0 {
		fmt.Fprintf(&b, " elapsed=%v", time.Since(time.Unix(0, at)).Round(time.Millisecond))
	}
	// Client commands in their handler (a relay waiting for an Agent
	// included), Agent relays still reading after their Client left, and
	// the relay budget bytes all relays hold (orphans included).
	fmt.Fprintf(&b, " clientCommands=%d agentOrphans=%d agentRelayBytes=%d", r.clientCommands.Load(), r.pool.orphans.snapshot().Active, r.pool.budget.snapshot().Bytes)
	if r.engine != nil {
		var total int
		var kinds []string
		for _, w := range r.engine.WriterStats() {
			if w.Pending > 0 {
				total += w.Pending
				kinds = append(kinds, fmt.Sprintf("%s:%d", w.Kind, w.Pending))
			}
		}
		fmt.Fprintf(&b, " ingestPending=%d", total)
		if len(kinds) > 0 {
			fmt.Fprintf(&b, "(%s)", strings.Join(kinds, " "))
		}
	}
	if r.store != nil {
		fmt.Fprintf(&b, " xlogQueue=%d profileQueue=%d", r.store.XLogQueueStats().Len, r.store.ProfileQueueStats().Len)
		marker := "absent"
		if r.store.CleanMarkerWritten() {
			marker = "written"
		}
		fmt.Fprintf(&b, " marker=%s", marker)
	}
	if p := r.shutdown.pulse.Load(); p != nil {
		st := p.dispatcher.Stats()
		fmt.Fprintf(&b, " pulseQueue=%d/%d", st.XLog.Pending, st.Profile.Pending)
		if p.producer != nil {
			fmt.Fprintf(&b, " kafkaPending=%d", p.producer.Stats().Pending)
		}
	}
	return b.String()
}

// writersLeft is the number of records ingest writers still hold after
// Engine.Close: non-zero only when a writer is still in a write that
// ignored its cancelled context (ingest CONTRACT §13).
func writersLeft(stats []ingest.WriterStats) int {
	n := 0
	for _, w := range stats {
		n += w.Pending
	}
	return n
}
