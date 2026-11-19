package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// defaultShutdownTimeout bounds the whole graceful shutdown. It stays below
// the 30s after which scripts/scouter-go.sh stop sends SIGKILL
// (SCOUTER_STOP_TIMEOUT; the script passes STOP_TIMEOUT-10s when that is
// set), so a stalled stage is logged before the kill. A shutdown takes
// about a second, also under load (docs/compatibility/runtime.md "종료
// 상한"). The bounded waits inside it add up past 20s only when several
// stall at once: Engine.Close 5s for deferred writes + 5s for the writers +
// 1s writerStuckGrace, the SQL table worker 5s, the Pulse dispatcher and
// producer 5s each (26s in all); Counters.Close and Store.Close have no
// bound of their own.
const defaultShutdownTimeout = 20 * time.Second

// exitShutdownTimeout is the exit code when --shutdown-timeout expires. A
// forced second signal exits 128+signal number (SIGINT 130, SIGTERM 143),
// the shell convention for a signal death. Neither writes the .go-clean
// marker unless Store.Close already had (the report says marker=written);
// without it the next start validates the indexes from the crash
// checkpoints, as after SIGKILL.
const exitShutdownTimeout = 3

// forcedExitWait bounds the cleanup, the report and the log flush before a
// forced exit: on a stalled disk the log file write may never return.
const forcedExitWait = 2 * time.Second

// signalDupWindow: a signal repeated this soon after the first one is the
// same request delivered twice (Ctrl-C reaching both the server and a
// wrapper that forwards it, a shell resending a hangup) and is ignored.
const signalDupWindow = time.Second

// shutdownGuard enforces the shutdown budget and the forced exit on a later
// signal. It is armed when the shutdown begins (a signal, the signal
// context ended, or the runtime began tearing down on its own) and lives
// until the process exits.
type shutdownGuard struct {
	budget time.Duration
	report func() string  // the stalled stage and queues; may block
	logf   func(string)   // writes one line to stderr and the server log; may block
	flush  func()         // flushes the server log; may block
	exit   func(code int) // os.Exit
	abort  atomic.Pointer[func()]
	once   sync.Once
	armed  chan struct{}
	// lines feeds the one goroutine that writes the signal goroutine's log
	// lines; a full channel (logf stuck) drops the line and counts it.
	lines   chan string
	dropped atomic.Uint64
}

// guardLogLines bounds the signal log lines waiting for a stuck logf.
const guardLogLines = 8

// newShutdownGuard starts the guard's log goroutine; it lives until the
// process exits.
func newShutdownGuard(budget time.Duration, report func() string, logf func(string), flush func(), exit func(int)) *shutdownGuard {
	g := &shutdownGuard{budget: budget, report: report, logf: logf, flush: flush, exit: exit, armed: make(chan struct{}), lines: make(chan string, guardLogLines)}
	go func() {
		for line := range g.lines {
			if n := g.dropped.Swap(0); n > 0 {
				line += fmt.Sprintf(" (%d earlier shutdown log lines dropped)", n)
			}
			g.logf(line)
		}
	}()
	return g
}

// logAsync hands line to the log goroutine without waiting.
func (g *shutdownGuard) logAsync(line string) {
	select {
	case g.lines <- line:
	default:
		g.dropped.Add(1)
	}
}

// setAbort registers the best-effort cleanup a forced exit runs first
// (exit file, terminal mode), since it skips the run mode's stop.
func (g *shutdownGuard) setAbort(f func()) { g.abort.Store(&f) }

// arm starts the budget once. With a zero budget only the forced exit on a
// later signal applies.
func (g *shutdownGuard) arm() {
	g.once.Do(func() {
		close(g.armed)
		if g.budget <= 0 {
			return
		}
		time.AfterFunc(g.budget, func() {
			g.forceExit(exitShutdownTimeout, fmt.Sprintf("shutdown: budget %v exceeded; exiting %d", g.budget, exitShutdownTimeout))
		})
	})
}

func (g *shutdownGuard) isArmed() bool {
	select {
	case <-g.armed:
		return true
	default:
		return false
	}
}

// watch handles the shutdown signals (signal.NotifyContext would swallow
// all but the first). The first signal arms the budget and cancels, even
// when another trigger began the shutdown already. A later SIGINT or
// SIGTERM, more than signalDupWindow after the first signal, exits at once;
// repeats within the window and every later SIGHUP are logged and ignored.
// The signal goroutine never writes a log line itself (stderr may block on
// a stalled disk or a stopped tty): it hands lines to the log goroutine
// (logAsync). watch also arms the budget when started closes (the runtime
// tears down after a listener or heartbeat failure) or ctx ends (exit file,
// console quit). The goroutines live until the process exits.
func (g *shutdownGuard) watch(sigs <-chan os.Signal, ctx context.Context, cancel context.CancelFunc, started <-chan struct{}) {
	go func() {
		var first time.Time
		for sig := range sigs {
			now := time.Now()
			switch {
			case first.IsZero():
				first = now
				g.arm()
				cancel()
				g.logAsync(fmt.Sprintf("shutdown: %s received; %s", signalName(sig), g.plan()))
			case sig == syscall.SIGHUP:
				g.logAsync("shutdown: SIGHUP during shutdown ignored (only SIGINT/SIGTERM force the exit)")
			case now.Sub(first) < signalDupWindow:
				g.logAsync(fmt.Sprintf("shutdown: %s within %v of the first signal ignored as a repeat", signalName(sig), signalDupWindow))
			default:
				code := 128
				if s, ok := sig.(syscall.Signal); ok {
					code += int(s)
				}
				g.forceExit(code, fmt.Sprintf("shutdown: second signal %s; exiting now", signalName(sig)))
				return
			}
		}
	}()
	go func() {
		select {
		case <-ctx.Done():
		case <-started:
		}
		g.arm()
	}()
}

func (g *shutdownGuard) plan() string {
	if g.budget <= 0 {
		return fmt.Sprintf("graceful shutdown, no shutdown budget (SIGINT/SIGTERM again after %v exits at once)", signalDupWindow)
	}
	return fmt.Sprintf("graceful shutdown within %v (SIGINT/SIGTERM again after %v exits at once)", g.budget, signalDupWindow)
}

func signalName(sig os.Signal) string {
	switch sig {
	case syscall.SIGINT:
		return "SIGINT"
	case syscall.SIGTERM:
		return "SIGTERM"
	case syscall.SIGHUP:
		return "SIGHUP"
	}
	return sig.String()
}

// forceExit logs line with the report and flushes the server log while the
// cleanup runs beside it (a stuck cleanup does not hold the line back),
// waits for both within forcedExitWait, then exits with code whatever they
// did. The line goes to logf directly, not through the log goroutine,
// which may be stuck or full.
func (g *shutdownGuard) forceExit(code int, line string) {
	logged, aborted := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(logged)
		g.logf(line + " " + g.report())
		g.flush()
	}()
	go func() {
		defer close(aborted)
		if f := g.abort.Load(); f != nil {
			(*f)()
		}
	}()
	timer := time.NewTimer(forcedExitWait)
	defer timer.Stop()
	for _, done := range []chan struct{}{logged, aborted} {
		select {
		case <-done:
		case <-timer.C:
			g.exit(code)
			return
		}
	}
	g.exit(code)
}
