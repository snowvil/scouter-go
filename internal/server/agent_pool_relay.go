package server

import (
	"context"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Orphaned Agent relays (C4). Java TcpAgentWorker reads an Agent answer to
// the end on the service thread whatever happens to the Client: a Client that
// gave up (its own read timeout) only fails the later write, and the Agent
// session stays open (AgentCall.scala:46-71, TcpAgentWorker.scala:21-68).
// Go ends a Client command earlier (the caller's context: Client progress
// limit, server shutdown), so a Pack relay runs detached from that context:
// when the caller leaves first, the request becomes an orphan that keeps
// reading under its own relay deadline (so_timeout+RequestTimeout, or +10min
// for the large commands), discards the answer and returns the session to the
// pool. The slot stays busy meanwhile, so a retry finds no idle session (like
// Java's RequestQueue.get returning null) instead of reconnecting the Agent
// and repeating the work.
//
// Orphans are bounded: at most agentOrphanMax at a time and at most
// agentOrphanMaxBytes of relay budget permits (agent_relay_budget.go): an
// orphan keeps the permit its relay took before the command was written, so
// agentOrphanMaxBytes is the share of that budget orphans may hold, not a
// second reservation (kept Packs are dropped once orphaned). A relay that
// would exceed either closes its Agent session at once, the former
// behaviour, and is counted as rejected.
const (
	agentOrphanMax      = 32
	agentOrphanMaxBytes = 512 << 20
	agentOrphanLogEvery = time.Minute
)

const (
	relayRunning int32 = iota
	relayFinished
	relayOrphaned
)

// agentOrphanStats is a snapshot of the orphan relay counters.
type agentOrphanStats struct {
	// Active and Bytes are the current orphans and the relay budget permits
	// they hold (part of agentRelayBudget.Bytes, not in addition to it).
	Active int
	Bytes  int64
	// Orphaned counts relays whose caller left first; Completed those that
	// then read the whole answer (session kept), Failed those that ended with
	// an Agent error (session closed), Rejected the relays closed at once
	// because the orphan limits were full.
	Orphaned, Completed, Failed, Rejected uint64
}

type agentOrphans struct {
	mu                                    sync.Mutex
	active                                int
	bytes                                 int64
	orphaned, completed, failed, rejected atomic.Uint64
	lastLog                               time.Time
	suppressed                            uint64
	logf                                  func(format string, args ...any)
}

func (o *agentOrphans) reserve(n int64) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.active >= agentOrphanMax || o.bytes+n > agentOrphanMaxBytes {
		return false
	}
	o.active++
	o.bytes += n
	return true
}

func (o *agentOrphans) unreserve(n int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.active--
	o.bytes -= n
}

func (o *agentOrphans) snapshot() agentOrphanStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	return agentOrphanStats{Active: o.active, Bytes: o.bytes, Orphaned: o.orphaned.Load(), Completed: o.completed.Load(), Failed: o.failed.Load(), Rejected: o.rejected.Load()}
}

// note writes one line per agentOrphanLogEvery; lines in between are counted
// and reported with the next one.
func (o *agentOrphans) note(message func() string) {
	o.mu.Lock()
	now := time.Now()
	if !o.lastLog.IsZero() && now.Sub(o.lastLog) < agentOrphanLogEvery {
		o.suppressed++
		o.mu.Unlock()
		return
	}
	suppressed := o.suppressed
	o.lastLog, o.suppressed = now, 0
	logf := o.logf
	o.mu.Unlock()
	if logf == nil {
		logf = log.Printf
	}
	s := o.snapshot()
	logf("%s (%d more orphan relay lines suppressed; orphans active=%d reserved=%s orphaned=%d completed=%d failed=%d rejected=%d)",
		message(), suppressed, s.Active, formatBytes(s.Bytes), s.Orphaned, s.Completed, s.Failed, s.Rejected)
}

// lifetime is the context of the Agent connection (agentPool.handle): it ends
// on server shutdown, never because a Client left.
func (s *agentSlot) lifetime() context.Context {
	if s.life != nil {
		return s.life
	}
	return context.Background()
}

// relay runs do on a checked-out slot, detached from ctx, and always releases
// the slot and permit, the relay budget the request holds until its read
// ends (an orphan included). do gets the connection-lifetime context and
// orphaned, which reports (from inside do) that the caller left and any
// answer is discarded. When ctx ends first relay returns ctx.Err() at once;
// see agentOrphans.
func (p *agentPool) relay(ctx context.Context, slot *agentSlot, hash int32, cmd string, permit *relayPermit, do func(ctx context.Context, orphaned func() bool) error) error {
	reserve := permit.n
	if err := ctx.Err(); err != nil {
		// The caller left before the command was written: send nothing.
		p.release(slot)
		permit.release()
		return err
	}
	var state atomic.Int32
	orphaned := func() bool { return state.Load() == relayOrphaned }
	done := make(chan error, 1)
	started := time.Now()
	go func() {
		err := do(slot.lifetime(), orphaned)
		if state.CompareAndSwap(relayRunning, relayFinished) {
			done <- err
			return
		}
		// Orphan: the caller is gone; this goroutine owns the slot. The
		// counters change and the outcome line is written before the slot
		// is visible as idle again: the Agent handler waits only for the
		// slot, so nothing of the relay runs after it (no line after the
		// shutdown closed the log). The trade-off: a blocked log writer
		// (one line per agentOrphanLogEvery, the rest only counted) delays
		// the slot and permit release with it.
		p.orphans.unreserve(reserve)
		if err != nil {
			p.orphans.failed.Add(1)
			p.orphans.note(func() string {
				return "agent relay " + cmd + " objHash=" + strconv.Itoa(int(hash)) + " orphan failed after " + time.Since(started).Round(time.Millisecond).String() + ": " + err.Error() + "; the agent session is closed"
			})
		} else {
			p.orphans.completed.Add(1)
			p.orphans.note(func() string {
				return "agent relay " + cmd + " objHash=" + strconv.Itoa(int(hash)) + " answered " + time.Since(started).Round(time.Millisecond).String() + " after the request, after its caller had left; the answer is discarded and the agent session kept"
			})
		}
		p.release(slot)
		permit.release()
	}()
	select {
	case err := <-done:
		p.release(slot)
		permit.release()
		return err
	case <-ctx.Done():
	}
	select {
	case err := <-done:
		// The answer finished as the caller left: keep the session.
		p.release(slot)
		permit.release()
		if err != nil {
			return err
		}
		return ctx.Err()
	default:
	}
	if !p.orphans.reserve(reserve) {
		// Orphan limits full: end the Agent read now (the pre-C4 behaviour).
		_ = slot.conn.Close()
		<-done
		p.orphans.rejected.Add(1)
		p.release(slot)
		permit.release()
		p.orphans.note(func() string {
			return "agent relay " + cmd + " objHash=" + strconv.Itoa(int(hash)) + " closed: its caller left and the orphan relay limits (" + strconv.Itoa(agentOrphanMax) + " relays, " + formatBytes(agentOrphanMaxBytes) + ") are full"
		})
		return ctx.Err()
	}
	if !state.CompareAndSwap(relayRunning, relayOrphaned) {
		// Finished at the same time: no orphan after all.
		p.orphans.unreserve(reserve)
		err := <-done
		p.release(slot)
		permit.release()
		if err != nil {
			return err
		}
		return ctx.Err()
	}
	p.orphans.orphaned.Add(1)
	return ctx.Err()
}
