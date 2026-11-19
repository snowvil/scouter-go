package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// sessionGate is the Go counterpart of the Java ServiceServer ThreadPoolExecutor
// (net_tcp_service_pool_size): at most limit Client sessions run, the rest wait
// in FIFO order. At most maxWaiters connections wait (Java's queue is
// unbounded); a further one gets errWaitFull at once. setLimit applies
// immediately in both directions (JDK 8 setCorePoolSize meaning): a larger limit
// admits waiters now, a smaller one only stops new admissions; running sessions
// are never interrupted.
type sessionGate struct {
	mu         sync.Mutex
	limit      int
	maxWaiters int
	active     int
	waiters    []chan struct{}
}

var errWaitFull = errors.New("transport client wait queue full")

func newSessionGate(limit, maxWaiters int) *sessionGate {
	return &sessionGate{limit: limit, maxWaiters: maxWaiters}
}

func (g *sessionGate) tryAcquire() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active < g.limit && len(g.waiters) == 0 {
		g.active++
		return true
	}
	return false
}

// acquire waits for a permit until ctx ends or lost delivers. A permit granted
// concurrently with cancellation is returned to the gate, never leaked. A full
// waiting set returns errWaitFull without waiting.
func (g *sessionGate) acquire(ctx context.Context, lost <-chan error) error {
	g.mu.Lock()
	if g.active < g.limit && len(g.waiters) == 0 {
		g.active++
		g.mu.Unlock()
		return nil
	}
	if len(g.waiters) >= g.maxWaiters {
		g.mu.Unlock()
		return errWaitFull
	}
	ready := make(chan struct{})
	g.waiters = append(g.waiters, ready)
	g.mu.Unlock()
	var cause error
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		cause = ctx.Err()
	case cause = <-lost:
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for i, w := range g.waiters {
		if w == ready {
			g.waiters = append(g.waiters[:i], g.waiters[i+1:]...)
			return cause
		}
	}
	g.active-- // granted after we stopped waiting
	g.grantLocked()
	return cause
}

func (g *sessionGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active--
	g.grantLocked()
}

func (g *sessionGate) setLimit(limit int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.limit = limit
	g.grantLocked()
}

func (g *sessionGate) grantLocked() {
	for g.active < g.limit && len(g.waiters) > 0 {
		next := g.waiters[0]
		g.waiters[0] = nil
		g.waiters = g.waiters[1:]
		g.active++
		close(next)
	}
}

func (g *sessionGate) snapshot() (limit, active, waiting int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.limit, g.active, len(g.waiters)
}

var errReadInterrupted = errors.New("transport read interrupted")

// idleReader sits under the connection's bufio.Reader and arms the read
// deadline before every socket read: min(now+idle, limit). idle reproduces Java
// Socket.setSoTimeout (refreshed per read, 0 = no per-read limit); limit is the
// Go total deadline (zero = none). Writes keep their own deadline.
// interrupt/resume let the owner stop a probe read running on another goroutine.
type idleReader struct {
	conn        net.Conn
	mu          sync.Mutex
	idle        time.Duration
	limit       time.Time
	interrupted bool
}

func (r *idleReader) set(idle time.Duration, limit time.Time) {
	r.mu.Lock()
	r.idle, r.limit = idle, limit
	r.mu.Unlock()
}

func (r *idleReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	if r.interrupted {
		r.mu.Unlock()
		return 0, errReadInterrupted
	}
	deadline := r.limit
	if r.idle > 0 {
		if next := time.Now().Add(r.idle); deadline.IsZero() || next.Before(deadline) {
			deadline = next
		}
	}
	err := r.conn.SetReadDeadline(deadline)
	r.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return r.conn.Read(p)
}

func (r *idleReader) interrupt() {
	r.mu.Lock()
	r.interrupted = true
	r.conn.SetReadDeadline(time.Unix(1, 0))
	r.mu.Unlock()
}

func (r *idleReader) resume() {
	r.mu.Lock()
	r.interrupted = false
	r.mu.Unlock()
}
