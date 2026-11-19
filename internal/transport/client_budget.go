package transport

import (
	"fmt"
	"math"
	"runtime/debug"
	"strconv"
	"sync"
	"time"
)

// Client response memory budget (Go only, R2). A Client response item is
// encoded in full before it is written, and the write blocks while the Client
// reads slowly, so each session may hold up to clientPendingLimit (257MiB)
// until its Client has read it. clientPendingLimit bounds one item, the budget
// bounds all of them: an item takes len(item) bytes before its HasNEXT flag is
// written and gives them back when the socket writer returned. Java has no
// such bound (its heap -Xmx is the only one).
//
//   - Items below clientBudgetMinItem never take the budget nor wait: most
//     answers are small, and at most one is held per Client session, so they
//     hold at most net_tcp_service_pool_size x 64KiB.
//   - Blob chunks an Agent relay already counts in its own budget
//     (Stream.ResponseRelayBlob) skip it: waiting here would stop the Agent
//     read and a refusal would close the Agent session.
//   - An item that does not fit waits for the bytes, at most the connection's
//     pool wait limit (client so-timeout, RequestTimeout when it is 0) and
//     never more than RequestTimeout, and no longer than the command's
//     context: a Scouter Client stops reading after its own read timeout
//     anyway. Items waiting together hold at most the limit (they are already
//     encoded); beyond it an item is refused at once.
//   - A refused item fails its command with ErrResponseBudget (an ErrLimit):
//     nothing of the item is written, the items before it are flushed and the
//     connection closes without NoNEXT, like for an item above
//     clientPendingLimit. The Java Client's TcpProxy.process catches the read
//     error, closes its socket and returns normally: the caller gets the items
//     read so far as if the answer were complete (no error shown), and the
//     next command reconnects. A Java server instead logs a handler exception
//     (ServiceHandlingProxy), still writes NoNEXT and keeps the connection.
//   - An item larger than the whole limit is admitted when nothing else holds
//     the budget, so every response the per-item cap allows still goes out.
//     Encoded bytes held: max(limit, one item) being sent plus as much
//     waiting, so about twice the limit. The encode itself (the encoder's
//     growth room, the item before it takes the budget) is outside it.
//
// The waiting order is first fit: a returned budget admits every waiter that
// fits, oldest first, so a waiting large item does not hold back small ones.
// The budget counts encoded items only, not what a handler builds before it
// encodes them (Pack values, query buffers).

// clientBudgetMinItem is the smallest item that takes the budget.
const clientBudgetMinItem = clientWriteChunk

// DefaultClientResponseBudget is the budget without GOMEMLIMIT: four items
// of the per-item maximum, or thousands of ordinary multi-MiB answers.
const DefaultClientResponseBudget = 1 << 30

// ErrResponseBudget fails a Client command whose response item found the
// Client response memory budget full for the wait limit.
var ErrResponseBudget = fmt.Errorf("%w: Client response memory budget full", ErrLimit)

// DefaultClientResponseBytes is the budget TCPConfig.MaxClientResponseBytes 0
// selects: a quarter of the Go memory limit when GOMEMLIMIT (or
// debug.SetMemoryLimit) set one, else DefaultClientResponseBudget; never
// less than one largest item. The rest of the limit stays for ingest, storage
// caches and Agent sockets.
func DefaultClientResponseBytes() int64 {
	limit := debug.SetMemoryLimit(-1)
	if limit <= 0 || limit == math.MaxInt64 {
		return DefaultClientResponseBudget
	}
	return max(limit/4, int64(clientPendingLimit))
}

type budgetWaiter struct {
	n       int64
	ready   chan struct{}
	granted bool
}

// responseBudget is the byte semaphore shared by every Client connection of a
// TCPServer. mu guards all fields except log/unlogged.
type responseBudget struct {
	mu                         sync.Mutex
	limit, used, peak, waiting int64
	waiters                    []*budgetWaiter // oldest first
	waits, rejected            uint64
	log                        rateLimit
	unlogged                   uint64
}

func newResponseBudget(limit int64) *responseBudget { return &responseBudget{limit: limit} }

func (b *responseBudget) fits(n int64) bool { return b.used+n <= b.limit || b.used == 0 }
func (b *responseBudget) take(n int64) {
	b.used += n
	b.peak = max(b.peak, b.used)
}

// acquire takes n bytes, waiting at most wait or until done closes.
func (b *responseBudget) acquire(n int64, wait time.Duration, done <-chan struct{}) error {
	b.mu.Lock()
	// Every release grants the waiters that fit, so a waiter never fits now
	// and an item that fits may go ahead of them (first fit).
	if b.fits(n) {
		b.take(n)
		b.mu.Unlock()
		return nil
	}
	if b.waiting > 0 && b.waiting+n > b.limit {
		b.rejected++
		b.mu.Unlock()
		return ErrResponseBudget
	}
	w := &budgetWaiter{n: n, ready: make(chan struct{})}
	b.waiters = append(b.waiters, w)
	b.waiting += n
	b.waits++
	b.mu.Unlock()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-w.ready:
		return nil
	case <-timer.C:
	case <-done:
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if w.granted {
		return nil
	}
	for i, other := range b.waiters {
		if other == w {
			b.waiters = append(b.waiters[:i], b.waiters[i+1:]...)
			break
		}
	}
	b.waiting -= n
	b.rejected++
	return ErrResponseBudget
}

func (b *responseBudget) release(n int64) {
	b.mu.Lock()
	b.used -= n
	b.grant()
	b.mu.Unlock()
}

// grant admits every waiter that fits now, oldest first. mu is held.
func (b *responseBudget) grant() {
	kept := b.waiters[:0]
	for _, w := range b.waiters {
		if b.fits(w.n) {
			b.take(w.n)
			b.waiting -= w.n
			w.granted = true
			close(w.ready)
			continue
		}
		kept = append(kept, w)
	}
	clear(b.waiters[len(kept):])
	b.waiters = kept
}

// stats returns used, peak, limit, waiting items, waits and rejections.
func (b *responseBudget) stats() (used, peak, limit int64, waiting int, waits, rejected uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used, b.peak, b.limit, len(b.waiters), b.waits, b.rejected
}

// refusedLine is the Go-only log line for a refused item, at most one every
// rejectLogEvery; refusals in between are carried as "<n> more".
func (b *responseBudget) refusedLine(peer string, n int64, now time.Time) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.log.allow(now, rejectLogEvery) {
		b.unlogged++
		return "", false
	}
	line := "Client : " + peer + " response of " + strconv.FormatInt(n, 10) + " bytes refused, response memory=" +
		strconv.FormatInt(b.used, 10) + "/" + strconv.FormatInt(b.limit, 10) + " (Go Client response memory budget)"
	if b.unlogged > 0 {
		line += ", " + strconv.FormatUint(b.unlogged, 10) + " more refused since the last line"
		b.unlogged = 0
	}
	return line, true
}

// clientBudget is one Client connection's view of the shared budget.
type clientBudget struct {
	shared *responseBudget
	wait   time.Duration
	peer   string
	log    func(string)
}

// hold takes the budget for an n-byte item, waiting until c.wait or done
// (the command's context: progress limit, server end); release must be called
// after the item was handed to the socket writer (or failed).
func (c *clientBudget) hold(n int, done <-chan struct{}) (release func(), err error) {
	if c == nil || n < clientBudgetMinItem {
		return func() {}, nil
	}
	size := int64(n)
	if err := c.shared.acquire(size, c.wait, done); err != nil {
		if line, ok := c.shared.refusedLine(c.peer, size, time.Now()); ok && c.log != nil {
			c.log(line)
		}
		return nil, err
	}
	return func() { c.shared.release(size) }, nil
}
