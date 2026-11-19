package server

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

func TestTextCacheResetterCallsOnStartAndDateChange(t *testing.T) {
	seoul, _ := time.LoadLocation("Asia/Seoul")
	now := time.Date(2026, 10, 15, 9, 40, 0, 0, seoul)
	var called []int32
	r := &textCacheResetter{
		location: seoul,
		now:      func() time.Time { return now },
		live:     func(context.Context) ([]int32, error) { return []int32{1, 2, 3}, nil },
		// Missed agents never connect here: no retry call in this test.
		connected: func(int32) bool { return false },
		// One call at a time: called records the order.
		parallel: 1,
		call: func(_ context.Context, hash int32) (bool, error) {
			called = append(called, hash)
			switch hash {
			case 2:
				return false, nil // no TCP connection: AgentCall S501
			case 3:
				return false, errors.New("boom") // S114: logged, loop continues
			}
			return true, nil
		},
	}
	ctx := context.Background()
	// oldunit starts at 0 in Java: the first pass after start always resets.
	result, err := r.tick(ctx)
	if err != nil || result == nil {
		t.Fatalf("first tick %+v %v", result, err)
	}
	if result.Date != "20261015" || result.Live != 3 || result.Called != 1 || result.Missing != 1 || result.Errors != 1 || result.Pending != 2 {
		t.Fatalf("first result %+v", *result)
	}
	if len(called) != 3 {
		t.Fatalf("called %v", called)
	}
	// Same date unit: nothing happens.
	if result, err = r.tick(ctx); err != nil || result != nil {
		t.Fatalf("same date tick %+v %v", result, err)
	}
	// Java fixed 24h date unit from the 2000-01-01 base: 23:59:59 and
	// 00:00:00 are different units.
	now = time.Date(2026, 10, 15, 23, 59, 59, 0, seoul)
	if result, err = r.tick(ctx); err != nil || result != nil {
		t.Fatalf("late evening tick %+v %v", result, err)
	}
	now = time.Date(2026, 10, 16, 0, 0, 0, 0, seoul)
	result, err = r.tick(ctx)
	if err != nil || result == nil || result.Date != "20261016" {
		t.Fatalf("midnight tick %+v %v", result, err)
	}
	if len(called) != 6 {
		t.Fatalf("called after midnight %v", called)
	}
}

func TestTextCacheResetterRetriesAfterLiveListError(t *testing.T) {
	seoul, _ := time.LoadLocation("Asia/Seoul")
	fail := true
	calls := 0
	r := &textCacheResetter{
		location: seoul,
		now:      func() time.Time { return time.Date(2026, 10, 15, 9, 40, 0, 0, seoul) },
		live: func(context.Context) ([]int32, error) {
			if fail {
				return nil, errors.New("engine busy")
			}
			return []int32{7}, nil
		},
		call: func(context.Context, int32) (bool, error) { calls++; return true, nil },
	}
	if _, err := r.tick(context.Background()); err == nil {
		t.Fatal("expected live list error")
	}
	fail = false
	result, err := r.tick(context.Background())
	if err != nil || result == nil || result.Called != 1 || calls != 1 {
		t.Fatalf("retry %+v %v calls %d", result, err, calls)
	}
}

func TestTextCacheResetterStopsOnCancel(t *testing.T) {
	seoul, _ := time.LoadLocation("Asia/Seoul")
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	r := &textCacheResetter{
		location: seoul,
		now:      time.Now,
		live: func(context.Context) ([]int32, error) {
			return []int32{1, 2, 3, 4}, nil
		},
		call: func(ctx context.Context, hash int32) (bool, error) {
			if hash == 1 {
				close(started)
				cancel()
			}
			return false, ctx.Err()
		},
	}
	done := make(chan struct{})
	go func() { r.run(ctx, 0, time.Hour); close(done) }()
	<-started
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not stop after cancel")
	}
	// Cancelled mid-pass: the pass is abandoned, not reported as complete.
	result, err := (&textCacheResetter{location: seoul, now: time.Now, live: r.live, call: r.call}).tick(ctx)
	if !errors.Is(err, context.Canceled) || result == nil || result.Called != 0 {
		t.Fatalf("cancelled tick %+v %v", result, err)
	}
}

// The wire form is the Java AgentCall contract: the command text, an empty
// MapPack parameter, and the agent's echoed MapPack read to NoNEXT.
func TestTextCacheResetSendsCommandOverAgentConnection(t *testing.T) {
	h := newPoolTCPHarness(t)
	peer := h.dial(t, 42)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	call := func(ctx context.Context, hash int32) (bool, error) {
		return h.pool.requestDiscard(ctx, hash, textCacheResetCommand, &protocol.MapPack{})
	}
	type outcome struct {
		ok  bool
		err error
	}
	done := make(chan outcome, 1)
	go func() { ok, err := call(ctx, 42); done <- outcome{ok, err} }()
	cmd, param := peer.readRequest(t)
	if cmd != textCacheResetCommand || len(param.Entries) != 0 {
		t.Fatalf("agent received %q %+v", cmd, param)
	}
	peer.packResponse(t, 42)
	select {
	case o := <-done:
		if o.err != nil || !o.ok {
			t.Fatalf("call %+v", o)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("call did not complete")
	}
	// A host agent has no OBJECT_RESET_CACHE handler and answers NoNEXT
	// only (TcpWorker.java:77-82): the command reached it, so it is called.
	go func() { ok, err := call(ctx, 42); done <- outcome{ok, err} }()
	if cmd, _ := peer.readRequest(t); cmd != textCacheResetCommand {
		t.Fatalf("agent received %q", cmd)
	}
	if _, err := peer.conn.Write([]byte{transport.NoNext}); err != nil {
		t.Fatal(err)
	}
	select {
	case o := <-done:
		if o.err != nil || !o.ok {
			t.Fatalf("NoNEXT-only answer %+v", o)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("call did not complete")
	}
	if !h.pool.connected(42) {
		t.Fatal("connected agent reported missing")
	}
	// No connection for this hash: skipped without error (S501).
	if h.pool.connected(43) {
		t.Fatal("unknown agent reported connected")
	}
	ok, err := call(ctx, 43)
	if err != nil || ok {
		t.Fatalf("missing agent %v %v", ok, err)
	}
}

// retryClock is a manual clock for the retry tests.
type retryClock struct{ now time.Time }

func (c *retryClock) add(d time.Duration) { c.now = c.now.Add(d) }

func newRetryResetter(clock *retryClock, live []int32, connected map[int32]bool, call func(int32) (bool, error)) (*textCacheResetter, *[]int32) {
	seoul, _ := time.LoadLocation("Asia/Seoul")
	var called []int32
	return &textCacheResetter{
		location:  seoul,
		now:       func() time.Time { return clock.now },
		live:      func(context.Context) ([]int32, error) { return live, nil },
		connected: func(hash int32) bool { return connected[hash] },
		parallel:  1, // called records the order
		call: func(_ context.Context, hash int32) (bool, error) {
			called = append(called, hash)
			return call(hash)
		},
	}, &called
}

// Intended difference: an agent without a TCP session at the pass gets the
// reset on the same date unit once it connects (Java waits for midnight).
func TestTextCacheResetterRetriesWhenAgentConnects(t *testing.T) {
	seoul, _ := time.LoadLocation("Asia/Seoul")
	clock := &retryClock{now: time.Date(2026, 10, 15, 9, 40, 0, 0, seoul)}
	connected := map[int32]bool{1: true}
	r, called := newRetryResetter(clock, []int32{1, 2}, connected, func(hash int32) (bool, error) { return connected[hash], nil })
	ctx := context.Background()
	result, err := r.tick(ctx)
	if err != nil || result == nil || result.Called != 1 || result.Missing != 1 || result.Pending != 1 {
		t.Fatalf("pass %+v %v", result, err)
	}
	// Still unconnected: the pending hash is not called.
	for range 3 {
		clock.add(2 * time.Second)
		if result, err = r.tick(ctx); err != nil || result != nil {
			t.Fatalf("retry tick %+v %v", result, err)
		}
	}
	if len(*called) != 2 {
		t.Fatalf("called before connect %v", *called)
	}
	connected[2] = true
	clock.add(2 * time.Second)
	if _, err = r.tick(ctx); err != nil {
		t.Fatal(err)
	}
	if got := *called; len(got) != 3 || got[2] != 2 || len(r.pending) != 0 || r.stats.Called != 1 {
		t.Fatalf("retry after connect %v pending %d stats %+v", got, len(r.pending), r.stats)
	}
	line, ok := r.retryLine(false)
	if !ok || line != "text cache reset: date 20261015 retry called 1 failed 0 gave up 0 discarded 0 pending 0" {
		t.Fatalf("retry line %q %v", line, ok)
	}
	// Done for the day: no further call.
	clock.add(time.Hour)
	if _, err = r.tick(ctx); err != nil || len(*called) != 3 {
		t.Fatalf("after success %v %v", *called, err)
	}
}

func TestTextCacheResetterRetriesBusyAndErrorWithGap(t *testing.T) {
	seoul, _ := time.LoadLocation("Asia/Seoul")
	clock := &retryClock{now: time.Date(2026, 10, 15, 9, 40, 0, 0, seoul)}
	connected := map[int32]bool{5: true}
	n := 0
	r, called := newRetryResetter(clock, []int32{5}, connected, func(int32) (bool, error) {
		n++
		switch n {
		case 1:
			return false, errors.New("synthetic agent failure")
		case 2:
			return false, nil // busy: no free session within the wait
		}
		return true, nil
	})
	ctx := context.Background()
	if result, err := r.tick(ctx); err != nil || result.Errors != 1 || result.Pending != 1 {
		t.Fatalf("pass %+v %v", result, err)
	}
	step := func(d time.Duration, wantCalls int) {
		t.Helper()
		clock.add(d)
		if _, err := r.tick(ctx); err != nil {
			t.Fatal(err)
		}
		if len(*called) != wantCalls {
			t.Fatalf("after %v: calls %v want %d", d, *called, wantCalls)
		}
	}
	step(5*time.Second, 1) // error gap 10s not over
	step(5*time.Second, 2) // retry 1: busy
	step(9*time.Second, 2) // gap 10s after the first failed retry
	step(1*time.Second, 3) // retry 2: called
	step(time.Minute, 3)   // removed
	if r.stats.Failed != 1 || r.stats.Called != 1 || r.stats.GaveUp != 0 || len(r.pending) != 0 {
		t.Fatalf("stats %+v pending %d", r.stats, len(r.pending))
	}
}

func TestTextCacheResetterGivesUpAfterMaxAttempts(t *testing.T) {
	seoul, _ := time.LoadLocation("Asia/Seoul")
	clock := &retryClock{now: time.Date(2026, 10, 15, 9, 40, 0, 0, seoul)}
	connected := map[int32]bool{7: true}
	r, called := newRetryResetter(clock, []int32{7}, connected, func(int32) (bool, error) {
		return false, errors.New("synthetic agent failure")
	})
	ctx := context.Background()
	if _, err := r.tick(ctx); err != nil {
		t.Fatal(err)
	}
	for range 100 {
		clock.add(time.Minute)
		if _, err := r.tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(*called) - 1; got != textCacheRetryAttempts {
		t.Fatalf("retry calls %d want %d", got, textCacheRetryAttempts)
	}
	if r.stats.GaveUp != 1 || r.stats.Failed != textCacheRetryAttempts-1 || len(r.pending) != 0 {
		t.Fatalf("stats %+v pending %d", r.stats, len(r.pending))
	}
}

func TestTextCacheResetterDateChangeDiscardsPending(t *testing.T) {
	seoul, _ := time.LoadLocation("Asia/Seoul")
	clock := &retryClock{now: time.Date(2026, 10, 15, 23, 59, 50, 0, seoul)}
	connected := map[int32]bool{}
	r, called := newRetryResetter(clock, []int32{2, 3}, connected, func(hash int32) (bool, error) { return connected[hash], nil })
	ctx := context.Background()
	if result, err := r.tick(ctx); err != nil || result.Pending != 2 {
		t.Fatalf("pass %+v %v", result, err)
	}
	connected[3] = true
	clock.add(20 * time.Second)
	result, err := r.tick(ctx)
	if err != nil || result == nil || result.Date != "20261016" || result.Called != 1 || result.Missing != 1 || result.Pending != 1 {
		t.Fatalf("midnight pass %+v %v", result, err)
	}
	if r.stats.Discarded != 2 || len(r.pending) != 1 || r.pending[0].hash != 2 || len(*called) != 4 {
		t.Fatalf("stats %+v pending %+v calls %v", r.stats, r.pending, *called)
	}
	// The forced line after the pass names the previous date unit.
	line, ok := r.retryLine(true)
	if !ok || line != "text cache reset: date 20261015 retry called 0 failed 0 gave up 0 discarded 2 pending 0" {
		t.Fatalf("retry line %q %v", line, ok)
	}
}

func TestTextCacheResetterBoundsPendingAndRound(t *testing.T) {
	seoul, _ := time.LoadLocation("Asia/Seoul")
	clock := &retryClock{now: time.Date(2026, 10, 15, 9, 40, 0, 0, seoul)}
	live := make([]int32, textCacheRetryMax+10)
	for i := range live {
		live[i] = int32(i + 1)
	}
	connected := map[int32]bool{}
	r, called := newRetryResetter(clock, live, connected, func(hash int32) (bool, error) { return connected[hash], nil })
	ctx := context.Background()
	result, err := r.tick(ctx)
	if err != nil || result.Missing != len(live) || result.Pending != textCacheRetryMax || result.Overflow != 10 {
		t.Fatalf("pass %+v %v", result, err)
	}
	for _, hash := range live {
		connected[hash] = true
	}
	start := len(*called)
	clock.add(2 * time.Second)
	if _, err = r.tick(ctx); err != nil {
		t.Fatal(err)
	}
	if got := (*called)[start:]; len(got) != textCacheRetryPerRound || got[0] != 1 || got[textCacheRetryPerRound-1] != textCacheRetryPerRound {
		t.Fatalf("round calls %v", got)
	}
	if len(r.pending) != textCacheRetryMax-textCacheRetryPerRound || r.pending[0].hash != textCacheRetryPerRound+1 {
		t.Fatalf("pending %d first %+v", len(r.pending), r.pending[0])
	}
	// Rate-limited summary: one line per textCacheRetryLogEvery.
	if _, ok := r.retryLine(false); !ok {
		t.Fatal("first retry line suppressed")
	}
	clock.add(2 * time.Second)
	if _, err = r.tick(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.retryLine(false); ok {
		t.Fatal("retry line not rate-limited")
	}
	clock.add(textCacheRetryLogEvery)
	if line, ok := r.retryLine(false); !ok || r.stats.Called != 0 {
		t.Fatalf("retry line after interval %q %v", line, ok)
	}
}

// End to end over the agent pool: the pass misses agent 42, the agent
// connects later, and the running daemon sends OBJECT_RESET_CACHE to it.
func TestTextCacheResetRetryReachesLateAgent(t *testing.T) {
	h := newPoolTCPHarness(t)
	seoul, _ := time.LoadLocation("Asia/Seoul")
	var passes atomic.Int32
	r := &textCacheResetter{
		location: seoul,
		now:      time.Now,
		live: func(context.Context) ([]int32, error) {
			passes.Add(1)
			return []int32{42}, nil
		},
		call: func(ctx context.Context, hash int32) (bool, error) {
			return h.pool.requestDiscard(ctx, hash, textCacheResetCommand, &protocol.MapPack{})
		},
		connected: h.pool.connected,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.run(ctx, 0, 10*time.Millisecond); close(done) }()
	defer func() {
		cancel()
		<-done
	}()
	deadline := time.Now().Add(2 * time.Second)
	for passes.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("pass did not run")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Give the pass time to find no session, then connect.
	time.Sleep(50 * time.Millisecond)
	peer := h.dial(t, 42)
	cmd, param := peer.readRequest(t)
	if cmd != textCacheResetCommand || len(param.Entries) != 0 {
		t.Fatalf("agent received %q %+v", cmd, param)
	}
	peer.packResponse(t, 42)
}
