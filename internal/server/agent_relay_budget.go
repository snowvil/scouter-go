package server

import (
	"context"
	"errors"
	"log"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Agent relay memory budget (R4). Java AgentCall has no total memory bound;
// Go bounds each relay (agentPackLimits, agentBlobLimits) and, here, the sum:
// every relay to an Agent holds a permit for the bytes it may still hold
// (TotalBytes of its limits; one chunk for a Blob stream) from before its
// command is written until its read ends. Large relays (256MiB answers) are
// also bounded in number.
//
// Size: the same rule as the transport Client response budget
// (transport.DefaultClientResponseBytes): a quarter of the Go memory limit
// when GOMEMLIMIT set one, else 1GiB, but never below agentRelayBudgetMin, so
// the two large relays (512MiB) always leave 256MiB (32 ordinary relays) to
// the others. Without GOMEMLIMIT: the Client service pool is 100
// (transport.DefaultServicePoolSize), the text cache reset runs
// textCacheResetParallel calls and ingest triggers a few more, so ordinary
// relays need about 120 * 8MiB; 1GiB holds 128 of them with no large relay
// running. The size is read once, at the first relay. Permits count wire
// bytes: a V2 answer holds its body and the decoded Packs at once while it is
// decoded, so the real peak can be about twice the permits. Orphan relays keep the
// permit of their relay: their agentOrphanMaxBytes is a share of this budget,
// not a second reservation.
//
// The wait for a permit depends on who sends the relay (acquireRelay):
//   - a Client command (the runtime marks its context, withRelayCommand) waits
//     the live net_tcp_get_agent_connection_wait_ms (Java default 1s), the
//     wait Java RequestQueue.get gives a busy Agent session; the session
//     checkout after it gets only what is left (checkoutAfterBudget), so the
//     command waits that value once in total, as in Java. The wait runs
//     inside Stream.AwaitUpstream: a Client that closes or resets meanwhile
//     ends the command's context (the transport probe), so a relay is never
//     sent after its Client left; a half-open Client is noticed only through
//     TCP keepalive or when the wait ends. When the budget stays full the
//     relay is not sent and the caller gets what Java AgentCall returns
//     then: null (the Client handler writes nothing for that agent, a
//     fan-out goes on with the next one). After one
//     refusal the rest of the same command does not wait again, so a fan-out
//     over N agents waits at most once, not N times.
//   - a queued server call (kube seq push, CPU thread-dump trigger; the
//     runtime marks RunAgentCalls' context, withServerRelay) waits
//     agentRelayServerWait and a refusal is errRelayBudget (counted failed).
//   - the text cache reset does not wait (requestDiscard): a refusal is
//     errRelayBudget and the agent is retried later without using an attempt,
//     so the reset never takes the Client relays' share for long.
//
// The Agent sees nothing of a refused relay and its session stays open.
//
// go_agent_relay_budget_mb, go_agent_relay_max_large and
// go_agent_relay_server_wait_ms replace the size, agentRelayMaxLarge and
// agentRelayServerWait live (relayBudget.configure); a smaller budget refuses
// new relays until the held permits fit, it never revokes one.
const (
	agentRelayBudgetBytes    = 1 << 30
	agentRelayBudgetMin      = 768 << 20
	agentRelayMaxLarge       = 2
	agentRelayServerWait     = 10 * time.Second
	agentRelayBudgetLogEvery = time.Minute
)

// relayBudget is a byte semaphore with a count of large holders. The zero
// value uses the defaults; limit and maxLarge (guarded by mu once relays
// run; configure sets them) override them, and wait > 0 replaces every
// caller's wait (tests).
type relayBudget struct {
	limit    int64
	maxLarge int
	wait     time.Duration
	// serverWait overrides agentRelayServerWait when > 0 (nanoseconds).
	serverWait atomic.Int64
	// sized holds defaultRelayBudgetBytes once read (limit <= 0).
	sizeOnce sync.Once
	sized    int64

	mu      sync.Mutex
	bytes   int64
	large   int
	waiting int
	changed chan struct{}
	lastLog time.Time
	// suppressed counts refusals not logged since the last line.
	suppressed uint64
	logf       func(format string, args ...any)

	granted, waited, refused, largeRefused, serverRefused atomic.Uint64
}

// errRelayBudget is the refusal a server-originated relay sees.
var errRelayBudget = errors.New("agent relay not sent: the Go relay memory budget is full")

type relayCommandKey struct{}
type serverRelayKey struct{}

// relayCommand is the relay state of one Client command.
type relayCommand struct{ refused atomic.Bool }

// withRelayCommand marks ctx as one Client command's (runtime Client handler).
func withRelayCommand(ctx context.Context) context.Context {
	return context.WithValue(ctx, relayCommandKey{}, &relayCommand{})
}

// withServerRelay marks ctx as the queued server calls' (RunAgentCalls).
func withServerRelay(ctx context.Context) context.Context {
	return context.WithValue(ctx, serverRelayKey{}, true)
}

// relayBudgetStats is the /health view of the budget.
type relayBudgetStats struct {
	// Bytes and Large are the permits held now (orphan relays included);
	// Limit and MaxLarge their bounds; Waiting the relays waiting for one.
	Bytes, Limit    int64
	Large, MaxLarge int
	Waiting         int
	// Granted counts permits given, Waited those that had to wait first,
	// Refused the relays not sent because the budget stayed full;
	// LargeRefused and ServerRefused are the large relays and the server
	// calls (text cache reset, kube seq, CPU dump) among them.
	Granted, Waited, Refused, LargeRefused, ServerRefused uint64
}

// relayPermit is held by one relay; release returns it once.
type relayPermit struct {
	budget   *relayBudget
	n        int64
	large    bool
	released atomic.Bool
}

// defaultRelayBudgetBytes applies the size rule above to the current Go
// memory limit.
func defaultRelayBudgetBytes() int64 {
	limit := debug.SetMemoryLimit(-1)
	if limit <= 0 || limit == math.MaxInt64 {
		return agentRelayBudgetBytes
	}
	return max(limit/4, agentRelayBudgetMin)
}

// configure applies the live go_agent_relay_* keys; zero values select the
// defaults. Waiters re-check the new bounds at once.
func (b *relayBudget) configure(limit int64, maxLarge int, serverWait time.Duration) {
	b.serverWait.Store(int64(serverWait))
	b.mu.Lock()
	b.limit, b.maxLarge = limit, maxLarge
	b.wakeLocked()
	b.mu.Unlock()
}

// serverCallWait is the wait of a queued server call.
func (b *relayBudget) serverCallWait() time.Duration {
	if d := time.Duration(b.serverWait.Load()); d > 0 {
		return d
	}
	return agentRelayServerWait
}

func (b *relayBudget) wakeLocked() {
	if b.changed != nil {
		close(b.changed)
		b.changed = nil
	}
}

// bounds returns the limits in force; b.mu must be held.
func (b *relayBudget) bounds() (int64, int) {
	limit, maxLarge := b.limit, b.maxLarge
	if limit <= 0 {
		b.sizeOnce.Do(func() { b.sized = defaultRelayBudgetBytes() })
		limit = b.sized
	}
	if maxLarge <= 0 {
		maxLarge = agentRelayMaxLarge
	}
	return limit, maxLarge
}

// acquire waits up to wait (<= 0: not at all) for n bytes (and a large slot
// when large). It returns nil, nil when the budget stayed full, and ctx.Err()
// when the caller left first. server only selects the counter and log text. n above the limit is clamped to the limit so such a
// relay can still run alone. Waiters are not queued in order: a large relay
// waiting for its bytes can lose them to smaller relays and is then refused
// after the wait like any other.
func (b *relayBudget) acquire(ctx context.Context, hash int32, cmd string, n int64, large bool, wait time.Duration, server bool) (*relayPermit, error) {
	if b.wait > 0 {
		wait = b.wait
	}
	n = max(n, 0)
	var timer *time.Timer
	counted := false
	b.mu.Lock()
	for {
		// The bounds are read on every pass: configure may change them.
		limit, maxLarge := b.bounds()
		want := min(n, limit)
		if b.bytes+want <= limit && (!large || b.large < maxLarge) {
			b.bytes += want
			if large {
				b.large++
			}
			if counted {
				b.waiting--
			}
			b.mu.Unlock()
			if timer != nil {
				timer.Stop()
			}
			b.granted.Add(1)
			return &relayPermit{budget: b, n: want, large: large}, nil
		}
		if wait <= 0 {
			b.mu.Unlock()
			b.refuse(hash, cmd, wait, limit, maxLarge, large, server)
			return nil, nil
		}
		if !counted {
			counted = true
			b.waiting++
			b.waited.Add(1)
			timer = time.NewTimer(wait)
		}
		if b.changed == nil {
			b.changed = make(chan struct{})
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-changed:
			b.mu.Lock()
			continue
		case <-ctx.Done():
			b.leave(timer)
			return nil, ctx.Err()
		case <-timer.C:
			b.mu.Lock()
			b.waiting--
			limit, maxLarge := b.bounds()
			b.mu.Unlock()
			b.refuse(hash, cmd, wait, limit, maxLarge, large, server)
			return nil, nil
		}
	}
}

func (b *relayBudget) refuse(hash int32, cmd string, wait time.Duration, limit int64, maxLarge int, large, server bool) {
	b.refused.Add(1)
	if large {
		b.largeRefused.Add(1)
	}
	if server {
		b.serverRefused.Add(1)
	}
	b.note(hash, cmd, wait, limit, maxLarge, server)
}

func (b *relayBudget) leave(timer *time.Timer) {
	if timer != nil {
		timer.Stop()
	}
	b.mu.Lock()
	b.waiting--
	b.mu.Unlock()
}

func (p *relayPermit) release() {
	if p == nil || !p.released.CompareAndSwap(false, true) {
		return
	}
	b := p.budget
	b.mu.Lock()
	b.bytes -= p.n
	if p.large {
		b.large--
	}
	b.wakeLocked()
	b.mu.Unlock()
}

func (b *relayBudget) snapshot() relayBudgetStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	limit, maxLarge := b.bounds()
	return relayBudgetStats{Bytes: b.bytes, Limit: limit, Large: b.large, MaxLarge: maxLarge, Waiting: b.waiting,
		Granted: b.granted.Load(), Waited: b.waited.Load(), Refused: b.refused.Load(), LargeRefused: b.largeRefused.Load(), ServerRefused: b.serverRefused.Load()}
}

// note writes one refusal line per agentRelayBudgetLogEvery.
func (b *relayBudget) note(hash int32, cmd string, wait time.Duration, limit int64, maxLarge int, server bool) {
	b.mu.Lock()
	now := time.Now()
	if !b.lastLog.IsZero() && now.Sub(b.lastLog) < agentRelayBudgetLogEvery {
		b.suppressed++
		b.mu.Unlock()
		return
	}
	suppressed := b.suppressed
	b.lastLog, b.suppressed = now, 0
	used, large := b.bytes, b.large
	logf := b.logf
	b.mu.Unlock()
	if logf == nil {
		logf = log.Printf
	}
	outcome := "the Client gets the answer of an agent without a free session"
	if server {
		outcome = "the server call is not sent (text cache reset retries it later)"
	}
	logf("agent relay %s objHash=%d not sent: the Go relay memory budget stayed full for %s (in use %s of %s, large relays %d/%d); %s (%d more refusals since the last line, %d in total)",
		cmd, hash, max(wait, 0), formatBytes(used), formatBytes(limit), large, maxLarge, outcome, suppressed, b.refused.Load())
}

// acquireRelay is the budget step of a Client or queued server relay; an
// agent without a session is answered at once like before (no wait for a
// budget it would not use). A refused server relay returns errRelayBudget.
func (p *agentPool) acquireRelay(ctx context.Context, hash int32, cmd string, n int64, large bool) (*relayPermit, error) {
	if !p.connected(hash) {
		return nil, nil
	}
	if server, _ := ctx.Value(serverRelayKey{}).(bool); server {
		permit, err := p.budget.acquire(ctx, hash, cmd, n, large, p.budget.serverCallWait(), true)
		if err == nil && permit == nil {
			err = errRelayBudget
		}
		return permit, err
	}
	state, _ := ctx.Value(relayCommandKey{}).(*relayCommand)
	wait := p.timing.wait()
	if state != nil && state.refused.Load() {
		wait = 0
	}
	permit, err := p.budget.acquire(ctx, hash, cmd, n, large, wait, false)
	if err == nil && permit == nil && state != nil {
		state.refused.Store(true)
	}
	return permit, err
}
