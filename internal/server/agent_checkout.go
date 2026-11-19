package server

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// agentTiming holds the live net_tcp_agent_keepalive_interval_ms and
// net_tcp_get_agent_connection_wait_ms (T3). Unset values are the Java
// defaults 5000/1000.
type agentTiming struct {
	set                 atomic.Bool
	keepAliveMs, waitMs atomic.Int64
}

func (t *agentTiming) store(keepAliveMs, waitMs int) {
	t.keepAliveMs.Store(int64(keepAliveMs))
	t.waitMs.Store(int64(waitMs))
	t.set.Store(true)
}

// keepAlive is the idle-since-last-write threshold (TcpAgentWorker.isExpired).
func (t *agentTiming) keepAlive() time.Duration {
	if !t.set.Load() {
		return 5 * time.Second
	}
	return time.Duration(t.keepAliveMs.Load()) * time.Millisecond
}

// wait is RequestQueue.get(net_tcp_get_agent_connection_wait_ms).
func (t *agentTiming) wait() time.Duration {
	if !t.set.Load() {
		return time.Second
	}
	return time.Duration(t.waitMs.Load()) * time.Millisecond
}

// signalLocked wakes pool waiters without blocking the connection owner.
func (p *agentPool) signalLocked() {
	if p.changed != nil {
		close(p.changed)
	}
	p.changed = make(chan struct{})
}

// checkout takes an idle session of hash, waiting up to the live
// net_tcp_get_agent_connection_wait_ms (<= 0: no wait, like
// RequestQueue.get(timeout)). The caller writes a command at once, so the
// session's lastWrite is set here.
func (p *agentPool) checkout(ctx context.Context, hash int32) (*agentSlot, error) {
	return p.checkoutWithin(ctx, hash, p.timing.wait())
}

// checkoutAfterBudget is checkout for a relay that started waiting for its
// relay budget permit at start. A Client command's relay waits the live
// net_tcp_get_agent_connection_wait_ms in total for both, the one wait Java
// has (RequestQueue.get; the budget is Go's), so the command's progress
// limit, held while it waits upstream, is not held twice as long. A queued
// server call keeps the whole session wait after its own budget wait.
func (p *agentPool) checkoutAfterBudget(ctx context.Context, hash int32, start time.Time) (*agentSlot, error) {
	wait := p.timing.wait()
	if server, _ := ctx.Value(serverRelayKey{}).(bool); !server {
		wait -= time.Since(start)
	}
	return p.checkoutWithin(ctx, hash, wait)
}

// checkoutWithin is checkout waiting up to wait (<= 0: an idle session or
// none at once).
func (p *agentPool) checkoutWithin(ctx context.Context, hash int32, wait time.Duration) (*agentSlot, error) {
	timer := time.NewTimer(max(wait, 0))
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p.mu.Lock()
		list := p.agents[hash]
		if len(list) == 0 {
			p.mu.Unlock()
			return nil, nil
		}
		for _, slot := range list {
			if !slot.busy && !slot.conn.Closed() {
				slot.busy = true
				slot.lastWrite = time.Now()
				p.mu.Unlock()
				return slot, nil
			}
		}
		if p.changed == nil {
			p.changed = make(chan struct{})
		}
		changed := p.changed
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, nil
		case <-changed:
		}
	}
}

// claimIdle is the TcpAgentManager 5s scan for one session: an idle session
// whose last write is at least net_tcp_agent_keepalive_interval_ms old is
// claimed for KEEP_ALIVE, which counts as a write.
func (p *agentPool) claimIdle(slot *agentSlot, now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if slot.busy || slot.conn.Closed() || now.Sub(slot.lastWrite) < p.timing.keepAlive() {
		return false
	}
	slot.busy = true
	slot.lastWrite = now
	return true
}
func (p *agentPool) release(slot *agentSlot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	slot.busy = false
	if slot.conn.Closed() {
		slot.once.Do(func() { close(slot.done) })
	}
	list := p.agents[slot.conn.ObjectHash]
	for i, s := range list {
		if s == slot {
			p.agents[slot.conn.ObjectHash] = append(append(list[:i:i], list[i+1:]...), slot)
			break
		}
	}
	p.signalLocked()
}

// connected reports whether hash has an open agent session, idle or busy
// (TcpAgentManager holds a worker for it). It neither waits nor claims one.
func (p *agentPool) connected(hash int32) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, slot := range p.agents[hash] {
		if !slot.conn.Closed() {
			return true
		}
	}
	return false
}

// requestDiscard is request for a command whose answer is discarded
// (OBJECT_RESET_CACHE). reached reports whether a session took the command:
// request returns nil Packs both when no session was free and when the agent
// answered NoNEXT only (a host agent has no OBJECT_RESET_CACHE handler).
// The answer bytes stay bounded by agentPackLimits and no Pack is kept.
func (p *agentPool) requestDiscard(ctx context.Context, hash int32, cmd string, param *protocol.MapPack) (reached bool, err error) {
	limits := agentPackLimits(cmd)
	if !p.connected(hash) {
		return false, nil
	}
	// No wait for the budget (agent_relay_budget.go): a full budget is
	// errRelayBudget and the caller retries later.
	permit, err := p.budget.acquire(ctx, hash, cmd, limits.TotalBytes, largeAgentPackCommands[cmd], 0, true)
	if err != nil {
		return false, err
	}
	if permit == nil {
		return false, errRelayBudget
	}
	slot, err := p.checkout(ctx, hash)
	if err != nil || slot == nil {
		permit.release()
		return false, err
	}
	if p.register != nil {
		p.register(cmd)
	}
	// Detached like request: a caller that leaves first (shutdown; the text
	// cache reset sets no deadline of its own) leaves an orphan that reads
	// the answer under the relay deadline.
	err = p.relay(ctx, slot, hash, cmd, permit, func(agentCtx context.Context, _ func() bool) error {
		err := slot.conn.RequestPackLimited(agentCtx, cmd, param, limits, func(ctx context.Context, _ protocol.Pack) error { return ctx.Err() })
		if err != nil {
			logRelayLimit(hash, cmd, limits, err)
		}
		return err
	})
	return true, err
}
