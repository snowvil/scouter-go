package ingest

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Server-initiated Agent calls raised by the ingest path (Java
// PodSeqManager.pushSeqToAgent and ObjectCpuChecker's dump thread). The Java
// kube push calls AgentCall inline in the AgentManager thread; the Go ingest
// path holds the engine lock, so every call is queued and a RunAgentCalls
// worker performs it. Offers never block: a full queue drops the call and
// counts it, like RequestQueue.put returning false.
const (
	cmdSetKubeSeq        = "OBJECT_SET_KUBE_SEQ"
	cmdTriggerDumps      = "TRIGGER_THREAD_DUMPS_FROM_CONDITIONS"
	keyTriggerDumpReason = "TRIGGER_DUMP_REASON"
	dumpReasonCPU        = "TRIGGER_DUMP_REASON_TYPE_CPU_EXCEEDED"
	// cpuDumpQueueSize is ObjectCpuChecker's RequestQueue(100).
	cpuDumpQueueSize = 100
	// kubeSeqQueueSize bounds the pushes that Java performs synchronously.
	kubeSeqQueueSize = 1024
	// agentCallTimeout bounds one call; Java relies on the agent socket
	// timeout (net_tcp_agent_so_timeout_ms default 60000).
	agentCallTimeout = 60 * time.Second
)

type agentCall struct {
	hash  int32
	cmd   string
	param *protocol.MapPack
}

// AgentCallStats separates every outcome of one queue. Pending is the queue
// length at the time of the snapshot. Abandoned counts calls still queued when
// the worker stopped plus offers made after it stopped.
type AgentCallStats struct {
	Enqueued, Done, Failed, Dropped, Abandoned uint64
	Pending                                    int
}

type agentCallQueue struct {
	name                                       string
	mu                                         sync.Mutex // orders offer against stop
	stopped                                    bool
	ch                                         chan agentCall
	enqueued, done, failed, dropped, abandoned atomic.Uint64
}

func newAgentCallQueue(name string, size int) *agentCallQueue {
	return &agentCallQueue{name: name, ch: make(chan agentCall, size)}
}

func (q *agentCallQueue) offer(c agentCall) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		q.abandoned.Add(1)
		return false
	}
	select {
	case q.ch <- c:
		q.enqueued.Add(1)
		return true
	default:
		q.dropped.Add(1)
		return false
	}
}

// stop rejects further offers and counts what is still queued.
func (q *agentCallQueue) stop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return
	}
	q.stopped = true
	for {
		select {
		case <-q.ch:
			q.abandoned.Add(1)
		default:
			return
		}
	}
}

func (q *agentCallQueue) stats() AgentCallStats {
	return AgentCallStats{Enqueued: q.enqueued.Load(), Done: q.done.Load(), Failed: q.failed.Load(),
		Dropped: q.dropped.Load(), Abandoned: q.abandoned.Load(), Pending: len(q.ch)}
}

type agentCaller func(context.Context, int32, string, *protocol.MapPack) ([]protocol.Pack, error)

func (q *agentCallQueue) run(ctx context.Context, call agentCaller) {
	defer q.stop()
	for {
		select {
		case <-ctx.Done():
			return
		case c := <-q.ch:
			if ctx.Err() != nil {
				// select picked the call although ctx was already done: it
				// was never performed, like the rest of the queue.
				q.abandoned.Add(1)
				return
			}
			if call == nil {
				q.failed.Add(1)
				continue
			}
			callCtx, cancel := context.WithTimeout(ctx, agentCallTimeout)
			// The reply (a MapPack or nothing) is discarded, as in Java.
			_, err := call(callCtx, c.hash, c.cmd, c.param)
			cancel()
			if err != nil {
				q.failed.Add(1)
			} else {
				q.done.Add(1)
			}
		}
	}
}

// RunAgentCalls performs the queued kube-seq pushes and CPU thread-dump
// triggers with Config.AgentCall until ctx is done or the engine is closed;
// either one also cancels the call in flight. It is meant to run once in its
// own goroutine owned by the caller. It starts one worker per queue and
// returns after both have stopped; their remainder is counted as abandoned.
func (e *Engine) RunAgentCalls(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { // ends with RunAgentCalls at the latest (deferred cancel)
		select {
		case <-e.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	var wg sync.WaitGroup
	for _, q := range []*agentCallQueue{e.kubeCalls, e.cpuCalls} {
		wg.Add(1)
		go func(q *agentCallQueue) {
			defer wg.Done()
			q.run(ctx, e.config.AgentCall)
		}(q)
	}
	wg.Wait()
}
