package server

import (
	"bytes"
	"context"
	"errors"
	"log"
	"math"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// The date-change pass calls independent agents in parallel (bounded), and a
// call has no caller deadline of its own: a slow agent is waited for under
// the relay deadline instead of becoming an orphan relay.
func TestTextCacheResetPassCallsAgentsInParallel(t *testing.T) {
	seoul, _ := time.LoadLocation("Asia/Seoul")
	hashes := make([]int32, 8)
	for i := range hashes {
		hashes[i] = int32(i + 1)
	}
	var running, peak atomic.Int32
	var deadlines atomic.Int32
	r := &textCacheResetter{
		location: seoul,
		now:      time.Now,
		live:     func(context.Context) ([]int32, error) { return hashes, nil },
		call: func(ctx context.Context, _ int32) (bool, error) {
			if _, ok := ctx.Deadline(); ok {
				deadlines.Add(1)
			}
			n := running.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(200 * time.Millisecond)
			running.Add(-1)
			return true, nil
		},
	}
	start := time.Now()
	result, err := r.tick(context.Background())
	elapsed := time.Since(start)
	if err != nil || result == nil || result.Called != len(hashes) {
		t.Fatalf("pass %+v %v", result, err)
	}
	if peak.Load() < 2 || elapsed > time.Second {
		t.Fatalf("pass ran %d calls at most at once and took %v; want parallel calls", peak.Load(), elapsed)
	}
	if deadlines.Load() != 0 {
		t.Fatalf("%d calls had a caller deadline (a slow agent becomes an orphan relay)", deadlines.Load())
	}
}

// At most agentRelayMaxLarge large relays (256MiB answers) run at once; the
// next one waits for a permit before anything is sent to its agent.
func TestAgentRelayLargeConcurrencyBounded(t *testing.T) {
	h := newPoolTCPHarness(t)
	peers := []*poolFakeAgent{h.dial(t, 1), h.dial(t, 2), h.dial(t, 3)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := make([]<-chan poolRequestResult, len(peers))
	for i := range peers {
		results[i] = startPoolRequest(h.pool, ctx, int32(i+1), "OBJECT_THREAD_DUMP")
	}
	// Collect which agents got the command within a short window.
	got := make(chan int, len(peers))
	var wg sync.WaitGroup
	for i, p := range peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.conn.SetReadDeadline(time.Now().Add(400 * time.Millisecond)); err != nil {
				return
			}
			var b [1]byte
			if _, err := p.reader.Peek(len(b)); err == nil {
				got <- i
			}
		}()
	}
	wg.Wait()
	close(got)
	var reached []int
	for i := range got {
		reached = append(reached, i)
	}
	if len(reached) != 2 {
		t.Fatalf("%d large relays reached their agents at once (%v), want 2", len(reached), reached)
	}
	// Answer one: the waiting relay gets the permit and reaches its agent.
	first := reached[0]
	peers[first].readRequest(t)
	peers[first].packResponse(t, int64(first+1))
	awaitPoolRequest(t, results[first], int64(first+1))
	waiting := 3 - reached[0] - reached[1]
	cmd, _ := peers[waiting].readRequest(t)
	if cmd != "OBJECT_THREAD_DUMP" {
		t.Fatal(cmd)
	}
	peers[waiting].packResponse(t, int64(waiting+1))
	awaitPoolRequest(t, results[waiting], int64(waiting+1))
	second := reached[1]
	peers[second].readRequest(t)
	peers[second].packResponse(t, int64(second+1))
	awaitPoolRequest(t, results[second], int64(second+1))
}

// A relay whose budget stays full is not sent: the caller gets what an agent
// without a free session gives (nil, nil), the agent receives nothing and its
// session stays open.
func TestAgentRelayBudgetFullSendsNothing(t *testing.T) {
	h := newPoolTCPHarness(t)
	var lines []string
	var mu sync.Mutex
	h.pool.budget.limit = agentPackTotalBytes
	h.pool.budget.wait = 100 * time.Millisecond
	h.pool.budget.logf = func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, format)
	}
	holder := h.dial(t, 1)
	idle := h.dial(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	held := startPoolRequest(h.pool, ctx, 1, "OBJECT_ENV")
	holder.readRequest(t)
	packs, err := h.pool.request(ctx, 2, "OBJECT_ENV", &protocol.MapPack{})
	if err != nil || packs != nil {
		t.Fatalf("refused relay %v %v", packs, err)
	}
	if err := idle.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := idle.reader.Peek(1); err == nil {
		t.Fatal("the agent received a refused relay")
	}
	if !h.pool.connected(2) {
		t.Fatal("refusal closed the agent session")
	}
	s := h.pool.budget.snapshot()
	if s.Refused != 1 || s.Bytes != agentPackTotalBytes || s.Waiting != 0 {
		t.Fatalf("budget %+v", s)
	}
	mu.Lock()
	if len(lines) != 1 {
		t.Fatalf("log lines %q", lines)
	}
	mu.Unlock()
	holder.packResponse(t, 1)
	awaitPoolRequest(t, held, 1)
	// The permit is back: the same request now reaches the agent.
	second := startPoolRequest(h.pool, ctx, 2, "OBJECT_ENV")
	idle.readRequest(t)
	idle.packResponse(t, 2)
	awaitPoolRequest(t, second, 2)
	if s := h.pool.budget.snapshot(); s.Bytes != 0 || s.Granted != 2 {
		t.Fatalf("budget after %+v", s)
	}
}

// An orphan keeps its relay permit; the orphan byte figure is the same bytes,
// not a second reservation, and both return to zero when it ends.
func TestAgentRelayOrphanKeepsPermit(t *testing.T) {
	h := newOrphanHarness(t, 0)
	peer := h.dial(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := startPoolRequest(h.pool, ctx, 1, "OBJECT_ENV")
	peer.readRequest(t)
	cancel()
	if r := <-done; !errors.Is(r.err, context.Canceled) {
		t.Fatalf("caller result %+v", r)
	}
	if b, o := h.pool.budget.snapshot(), h.pool.orphans.snapshot(); b.Bytes != agentPackTotalBytes || o.Active != 1 || o.Bytes != b.Bytes {
		t.Fatalf("budget %+v orphans %+v", b, o)
	}
	peer.packResponse(t, 1)
	h.logs.wait(t, 1)
	if b, o := h.pool.budget.snapshot(), h.pool.orphans.snapshot(); b.Bytes != 0 || o.Active != 0 || o.Bytes != 0 {
		t.Fatalf("after orphan budget %+v orphans %+v", b, o)
	}
}

// A caller that leaves while waiting for the budget sends nothing.
func TestAgentRelayBudgetWaitEndsWithCaller(t *testing.T) {
	b := &relayBudget{limit: 10, wait: time.Hour}
	held, err := b.acquire(context.Background(), 1, "X", 10, false, 0, false)
	if err != nil || held == nil {
		t.Fatal(held, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	go func() {
		p, err := b.acquire(ctx, 1, "X", 1, false, 0, false)
		if p != nil {
			err = errors.New("permit granted")
		}
		got <- err
	}()
	for b.snapshot().Waiting != 1 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-got; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	held.release()
	held.release() // idempotent
	if s := b.snapshot(); s.Bytes != 0 || s.Waiting != 0 || s.Refused != 0 {
		t.Fatalf("%+v", s)
	}
	// Large relays are counted separately from the bytes.
	b = &relayBudget{limit: 100, maxLarge: 1, wait: 20 * time.Millisecond, logf: func(string, ...any) {}}
	large, _ := b.acquire(context.Background(), 1, "L", 1, true, 0, false)
	if p, err := b.acquire(context.Background(), 1, "L", 1, true, 0, false); p != nil || err != nil {
		t.Fatal("second large relay granted", p, err)
	}
	if s := b.snapshot(); s.Refused != 1 || s.LargeRefused != 1 || s.ServerRefused != 0 {
		t.Fatalf("%+v", s)
	}
	if p, _ := b.acquire(context.Background(), 1, "S", 1, false, 0, false); p == nil {
		t.Fatal("ordinary relay refused next to a large one")
	}
	large.release()
}

// The relay budget follows the Client response budget rule: a quarter of
// GOMEMLIMIT, 1GiB without one, never below agentRelayBudgetMin.
func TestDefaultRelayBudgetBytes(t *testing.T) {
	previous := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(previous)
	debug.SetMemoryLimit(math.MaxInt64)
	if got := defaultRelayBudgetBytes(); got != agentRelayBudgetBytes {
		t.Fatalf("no limit: %d", got)
	}
	debug.SetMemoryLimit(8 << 30)
	if got := defaultRelayBudgetBytes(); got != 2<<30 {
		t.Fatalf("8GiB limit: %d", got)
	}
	debug.SetMemoryLimit(2 << 30)
	if got := defaultRelayBudgetBytes(); got != agentRelayBudgetMin {
		t.Fatalf("2GiB limit: %d", got)
	}
}

// Per-agent errors of a pass are logged at most once per
// textCacheErrorLogEvery; the pass result still counts each one.
func TestTextCacheResetAgentErrorLogIsRateLimited(t *testing.T) {
	var logs bytes.Buffer
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)
	seoul, _ := time.LoadLocation("Asia/Seoul")
	clock := &retryClock{now: time.Date(2026, 10, 15, 9, 40, 0, 0, seoul)}
	r, _ := newRetryResetter(clock, []int32{1, 2, 3, 4, 5}, map[int32]bool{}, func(int32) (bool, error) {
		return false, errors.New("synthetic agent failure")
	})
	result, err := r.tick(context.Background())
	if err != nil || result.Errors != 5 {
		t.Fatalf("pass %+v %v", result, err)
	}
	if n := strings.Count(logs.String(), "text cache reset: agent"); n != 1 {
		t.Fatalf("%d agent error lines:\n%s", n, logs.String())
	}
	clock.add(textCacheErrorLogEvery)
	r.logAgentError(9, errors.New("later"))
	if !strings.Contains(logs.String(), "agent 9: later (4 more agent errors since the last line)") {
		t.Fatal(logs.String())
	}
}

// A Client command's relay waits the live net_tcp_get_agent_connection_wait_ms
// for the budget (Java RequestQueue.get); later relays of the same command
// do not wait again. The text cache reset does not wait and sees
// errRelayBudget. Refused relays hold neither a permit nor a session.
func TestAgentRelayBudgetWaitByOrigin(t *testing.T) {
	h := newPoolTCPHarness(t)
	h.pool.budget.limit = agentPackTotalBytes
	h.pool.budget.logf = func(string, ...any) {}
	h.pool.timing.store(5000, 200)
	holder := h.dial(t, 1)
	h.dial(t, 2)
	h.dial(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	held := startPoolRequest(h.pool, ctx, 1, "OBJECT_ENV")
	holder.readRequest(t)
	cmd := withRelayCommand(ctx)
	start := time.Now()
	if packs, err := h.pool.request(cmd, 2, "OBJECT_ENV", &protocol.MapPack{}); packs != nil || err != nil {
		t.Fatal(packs, err)
	}
	if d := time.Since(start); d < 150*time.Millisecond || d > 2*time.Second {
		t.Fatalf("Client relay waited %v for the budget, want the 200ms connection wait", d)
	}
	start = time.Now()
	if packs, err := h.pool.request(cmd, 3, "OBJECT_ENV", &protocol.MapPack{}); packs != nil || err != nil {
		t.Fatal(packs, err)
	}
	emitted := false
	if err := h.pool.requestBlob(cmd, 3, "OBJECT_DOWNLOAD_HEAP_DUMP", &protocol.MapPack{}, func([]byte) error { emitted = true; return nil }); err != nil || emitted {
		t.Fatal(err, emitted)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("the same command waited again: %v", d)
	}
	if reached, err := h.pool.requestDiscard(context.Background(), 2, textCacheResetCommand, &protocol.MapPack{}); reached || !errors.Is(err, errRelayBudget) {
		t.Fatal(reached, err)
	}
	s := h.pool.budget.snapshot()
	if s.Refused != 4 || s.ServerRefused != 1 || s.Bytes != agentPackTotalBytes {
		t.Fatalf("budget %+v", s)
	}
	h.pool.mu.Lock()
	for _, hash := range []int32{2, 3} {
		if h.pool.agents[hash][0].busy {
			t.Errorf("agent %d session held by a refused relay", hash)
		}
	}
	h.pool.mu.Unlock()
	holder.packResponse(t, 1)
	awaitPoolRequest(t, held, 1)
}

// relayWaitHarness takes agent 2's only session (relay "session") and holds
// the rest of a budget of limit bytes with a relay to agent 1 ("budget"),
// whose answer frees its permit.
type relayWaitHarness struct {
	*poolTCPHarness
	busy, other     *poolFakeAgent
	session, budget <-chan poolRequestResult
}

func newRelayWaitHarness(t *testing.T, ctx context.Context, limit int64, wait time.Duration) *relayWaitHarness {
	t.Helper()
	h := newPoolTCPHarness(t)
	h.pool.budget.limit = limit
	h.pool.budget.logf = func(string, ...any) {}
	h.pool.timing.store(5000, int(wait.Milliseconds()))
	w := &relayWaitHarness{poolTCPHarness: h, busy: h.dial(t, 2), other: h.dial(t, 1)}
	w.session = startPoolRequest(h.pool, ctx, 2, "OBJECT_ENV")
	w.busy.readRequest(t)
	w.budget = startPoolRequest(h.pool, ctx, 1, "OBJECT_ENV")
	w.other.readRequest(t)
	return w
}

// finish answers both holders.
func (w *relayWaitHarness) finish(t *testing.T) {
	t.Helper()
	w.busy.packResponse(t, 2)
	awaitPoolRequest(t, w.session, 2)
}

// A Client command's relay waits net_tcp_get_agent_connection_wait_ms in
// total for the budget and then a free session (Java RequestQueue.get is its
// only wait), not that wait for each; a queued server call keeps the whole
// session wait after its own budget wait. Before the change the Client
// relay took about budget wait + session wait (3.5s here).
func TestAgentRelayClientWaitsOnceForBudgetAndSession(t *testing.T) {
	for _, tc := range []struct {
		name     string
		server   bool
		min, max time.Duration
	}{
		{"client", false, 1800 * time.Millisecond, 2900 * time.Millisecond},
		{"server call", true, 3300 * time.Millisecond, 6 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const wait, freed = 2 * time.Second, 1500 * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			w := newRelayWaitHarness(t, ctx, 2*agentPackTotalBytes, wait)
			cmd := withRelayCommand(ctx)
			if tc.server {
				cmd = withServerRelay(ctx)
			}
			start := time.Now()
			result := startPoolRequest(w.pool, cmd, 2, "OBJECT_ENV")
			time.Sleep(freed)
			w.other.packResponse(t, 1)
			awaitPoolRequest(t, w.budget, 1)
			select {
			case r := <-result:
				if r.packs != nil || r.err != nil {
					t.Fatal(r.packs, r.err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("relay did not end")
			}
			if d := time.Since(start); d < tc.min || d > tc.max {
				t.Fatalf("relay waited %v for the budget and the session, want %v..%v", d, tc.min, tc.max)
			}
			w.finish(t)
		})
	}
}

// A Client relay whose budget wait used up the connection wait still takes
// an idle session at once; with none idle it returns at once.
func TestAgentCheckoutAfterBudgetNoTimeLeft(t *testing.T) {
	h := newPoolTCPHarness(t)
	h.pool.timing.store(5000, 2000)
	h.dial(t, 42)
	ctx := withRelayCommand(context.Background())
	long := time.Now().Add(-time.Minute)
	slot, err := h.pool.checkoutAfterBudget(ctx, 42, long)
	if err != nil || slot == nil {
		t.Fatal("idle session not taken", slot, err)
	}
	start := time.Now()
	if none, err := h.pool.checkoutAfterBudget(ctx, 42, long); none != nil || err != nil {
		t.Fatal(none, err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("waited %v with no time left", d)
	}
	h.pool.release(slot)
}

// A Blob relay that waits for the budget and then for the session within
// the connection wait in total is sent and streamed.
func TestAgentBlobRelayWaitsForBudgetThenSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// After agent 1 answers, the budget holds exactly one Blob chunk permit.
	w := newRelayWaitHarness(t, ctx, agentPackTotalBytes+agentBlobChunkBytes, 2*time.Second)
	var got bytes.Buffer
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		done <- w.pool.requestBlob(withRelayCommand(ctx), 2, "OBJECT_DOWNLOAD_HEAP_DUMP", &protocol.MapPack{}, func(chunk []byte) error {
			got.Write(chunk)
			return nil
		})
	}()
	time.Sleep(600 * time.Millisecond)
	w.other.packResponse(t, 1)
	awaitPoolRequest(t, w.budget, 1)
	time.Sleep(600 * time.Millisecond)
	w.busy.packResponse(t, 2)
	awaitPoolRequest(t, w.session, 2)
	if cmd, _ := w.busy.readRequest(t); cmd != "OBJECT_DOWNLOAD_HEAP_DUMP" {
		t.Fatal(cmd)
	}
	if _, err := w.busy.conn.Write(append(encodeBlobItem(t, []byte("dump")), transport.NoNext)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil || got.String() != "dump" {
			t.Fatalf("blob %q %v", got.String(), err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blob relay did not end")
	}
	if d := time.Since(start); d < time.Second {
		t.Fatalf("blob relay sent after %v, before the session was free", d)
	}
}

// A text cache reset call refused by the relay budget is retried later and
// never uses one of the agent's attempts.
func TestTextCacheResetBudgetRefusalKeepsAttempts(t *testing.T) {
	seoul, _ := time.LoadLocation("Asia/Seoul")
	clock := &retryClock{now: time.Date(2026, 10, 15, 9, 40, 0, 0, seoul)}
	refusals := textCacheRetryAttempts + 3
	r, called := newRetryResetter(clock, []int32{7}, map[int32]bool{7: true}, func(int32) (bool, error) {
		if refusals > 0 {
			refusals--
			return false, errRelayBudget
		}
		return true, nil
	})
	ctx := context.Background()
	result, err := r.tick(ctx)
	if err != nil || result.Refused != 1 || result.Errors != 0 || result.Pending != 1 {
		t.Fatalf("pass %+v %v", result, err)
	}
	for range textCacheRetryAttempts + 3 {
		clock.add(textCacheRetryGap)
		if _, err := r.tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if r.stats.Called != 1 || r.stats.GaveUp != 0 || len(r.pending) != 0 || len(*called) != textCacheRetryAttempts+4 {
		t.Fatalf("stats %+v pending %d calls %d", r.stats, len(r.pending), len(*called))
	}
}

// Concurrent acquires and releases keep the budget within its bounds and
// return it to zero.
func TestRelayBudgetStress(t *testing.T) {
	b := &relayBudget{limit: 64, maxLarge: 2, logf: func(string, ...any) {}}
	var wg sync.WaitGroup
	var over atomic.Int32
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				large := (g+i)%7 == 0
				n := int64(1 + (g*31+i)%16)
				if large {
					n = 32
				}
				p, err := b.acquire(context.Background(), 1, "X", n, large, time.Millisecond, i%3 == 0)
				if err != nil {
					t.Error(err)
					return
				}
				if s := b.snapshot(); s.Bytes > 64 || s.Large > 2 {
					over.Add(1)
				}
				p.release()
			}
		}()
	}
	wg.Wait()
	if s := b.snapshot(); over.Load() != 0 || s.Bytes != 0 || s.Large != 0 || s.Waiting != 0 || s.Granted+s.Refused != 16*500 {
		t.Fatalf("over %d %+v", over.Load(), s)
	}
}
