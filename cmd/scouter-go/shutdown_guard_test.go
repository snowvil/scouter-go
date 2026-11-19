package main

import (
	"context"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// guardHarness drives a shutdownGuard with injected exit and log.
type guardHarness struct {
	t       *testing.T
	g       *shutdownGuard
	sigs    chan os.Signal
	ctx     context.Context
	cancel  context.CancelFunc
	started chan struct{}
	exits   chan int
	mu      sync.Mutex
	lines   []string
}

func newGuardHarness(t *testing.T, budget time.Duration, logf func(string)) *guardHarness {
	h := &guardHarness{t: t, sigs: make(chan os.Signal, 4), started: make(chan struct{}), exits: make(chan int, 4)}
	h.ctx, h.cancel = context.WithCancel(context.Background())
	t.Cleanup(h.cancel)
	if logf == nil {
		logf = func(line string) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.lines = append(h.lines, line)
		}
	}
	h.g = newShutdownGuard(budget, func() string { return `stage="test"` }, logf, func() {}, func(code int) { h.exits <- code })
	h.g.watch(h.sigs, h.ctx, h.cancel, h.started)
	return h
}

func (h *guardHarness) exitCode(d time.Duration) (int, bool) {
	select {
	case code := <-h.exits:
		return code, true
	case <-time.After(d):
		return 0, false
	}
}

func (h *guardHarness) cancelled(d time.Duration) bool {
	select {
	case <-h.ctx.Done():
		return true
	case <-time.After(d):
		return false
	}
}

func (h *guardHarness) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.lines...)
}

func (h *guardHarness) logged(want string) bool {
	for _, l := range h.snapshot() {
		if strings.Contains(l, want) {
			return true
		}
	}
	return false
}

// waitLogged waits up to d for a line: the signal lines are written by the
// guard's log goroutine.
func (h *guardHarness) waitLogged(want string, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for !h.logged(want) {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
	return true
}

// A first-signal log line that never completes (stderr on a stalled disk or
// a tty stopped with XOFF) must not keep the signal from cancelling the
// run, arming the budget or exiting on a later signal.
func TestGuardFirstSignalNotHeldByLog(t *testing.T) {
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	h := newGuardHarness(t, 300*time.Millisecond, func(string) { <-block })
	h.sigs <- syscall.SIGTERM
	if !h.cancelled(time.Second) {
		t.Fatal("first signal did not cancel while its log line blocks")
	}
	// The budget still ends the process (the report line blocks too; the
	// forced exit waits at most forcedExitWait for it).
	if code, ok := h.exitCode(300*time.Millisecond + forcedExitWait + time.Second); !ok || code != exitShutdownTimeout {
		t.Fatalf("budget exit %d %v", code, ok)
	}
}

// The first signal after a shutdown another trigger began (exit file,
// console quit, Run's own teardown: ctx or started) is a first signal:
// graceful, no exit. A later signal then exits at once.
func TestGuardFirstSignalAfterOtherTrigger(t *testing.T) {
	for _, trigger := range []string{"ctx", "started"} {
		t.Run(trigger, func(t *testing.T) {
			h := newGuardHarness(t, 0, nil)
			if trigger == "ctx" {
				h.cancel()
			} else {
				close(h.started)
			}
			deadline := time.Now().Add(time.Second)
			for !h.g.isArmed() {
				if time.Now().After(deadline) {
					t.Fatal("not armed")
				}
				time.Sleep(time.Millisecond)
			}
			h.sigs <- syscall.SIGTERM
			if code, ok := h.exitCode(signalDupWindow + 200*time.Millisecond); ok {
				t.Fatalf("first SIGTERM after %s exited %d", trigger, code)
			}
			h.sigs <- syscall.SIGTERM
			if code, ok := h.exitCode(time.Second); !ok || code != 128+int(syscall.SIGTERM) {
				t.Fatalf("second SIGTERM: %d %v", code, ok)
			}
		})
	}
}

// One signal often arrives twice (Ctrl-C to a wrapper that forwards it, a
// terminal hangup): a repeat within signalDupWindow is ignored.
func TestGuardDuplicateSignalIgnored(t *testing.T) {
	h := newGuardHarness(t, 0, nil)
	h.sigs <- syscall.SIGINT
	h.sigs <- syscall.SIGINT
	if code, ok := h.exitCode(signalDupWindow + 200*time.Millisecond); ok {
		t.Fatalf("duplicate SIGINT exited %d", code)
	}
	if h.ctx.Err() == nil || !h.waitLogged("shutdown: SIGINT received", time.Second) {
		t.Fatalf("first SIGINT not handled: %v", h.snapshot())
	}
	h.sigs <- syscall.SIGINT
	if code, ok := h.exitCode(time.Second); !ok || code != 128+int(syscall.SIGINT) {
		t.Fatalf("second SIGINT: %d %v", code, ok)
	}
}

// SIGHUP never forces the exit: a hangup is delivered again by the
// terminal and by shells; only SIGINT/SIGTERM after an earlier signal do.
func TestGuardSIGHUPNeverForces(t *testing.T) {
	h := newGuardHarness(t, 0, nil)
	h.sigs <- syscall.SIGHUP
	time.Sleep(signalDupWindow + 100*time.Millisecond)
	h.sigs <- syscall.SIGHUP
	if code, ok := h.exitCode(300 * time.Millisecond); ok {
		t.Fatalf("SIGHUP after SIGHUP exited %d", code)
	}
	h.sigs <- syscall.SIGTERM
	if code, ok := h.exitCode(time.Second); !ok || code != 128+int(syscall.SIGTERM) {
		t.Fatalf("SIGTERM after SIGHUP: %d %v", code, ok)
	}
}

// With budget 0 nothing times out; the second signal still exits.
func TestGuardZeroBudget(t *testing.T) {
	h := newGuardHarness(t, 0, nil)
	h.sigs <- syscall.SIGTERM
	if code, ok := h.exitCode(500 * time.Millisecond); ok {
		t.Fatalf("budget 0 exited %d", code)
	}
	if !h.waitLogged("no shutdown budget", time.Second) {
		t.Fatalf("lines %v", h.snapshot())
	}
}

// A cleanup that never returns (a terminal write) does not hold back the
// report line, and the exit still comes within forcedExitWait.
func TestGuardStuckAbortStillReports(t *testing.T) {
	h := newGuardHarness(t, 100*time.Millisecond, nil)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	h.g.setAbort(func() { <-block })
	h.sigs <- syscall.SIGTERM
	if code, ok := h.exitCode(100*time.Millisecond + forcedExitWait + time.Second); !ok || code != exitShutdownTimeout {
		t.Fatalf("exit %d %v", code, ok)
	}
	if !h.logged(`shutdown: budget 100ms exceeded; exiting 3 stage="test"`) {
		t.Fatalf("no report line: %v", h.snapshot())
	}
}

// While logf is stuck, signal lines queue up to guardLogLines and the rest
// are dropped and counted (no goroutine per signal); the count is shown on
// the next line written.
func TestGuardLogLinesBounded(t *testing.T) {
	block := make(chan struct{})
	var mu sync.Mutex
	var lines []string
	h := newGuardHarness(t, 0, func(line string) {
		<-block
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, line)
	})
	before := runtime.NumGoroutine()
	h.sigs <- syscall.SIGTERM
	for i := 0; i < 50; i++ {
		h.sigs <- syscall.SIGHUP
	}
	deadline := time.Now().Add(2 * time.Second)
	for h.g.dropped.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no line dropped while logf is stuck")
		}
		time.Sleep(time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before+3 {
		t.Fatalf("goroutines %d -> %d", before, n)
	}
	close(block)
	h.sigs <- syscall.SIGHUP // written after the drops
	deadline = time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		got := strings.Join(lines, "\n")
		mu.Unlock()
		if strings.Contains(got, "earlier shutdown log lines dropped") && strings.Contains(got, "shutdown: SIGTERM received") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lines %q", got)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
