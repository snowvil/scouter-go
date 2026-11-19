// Package console is the Go port of the Scouter server `-console` mode
// (scouter.server.term.TermMain and term/handler/*). It reads commands from a
// LineReader, prints the Java output formats and reads server state through
// the Source interface. See CONTRACT.md.
package console

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

const (
	ansiReset = "\u001B[0m"
	ansiGreen = "\u001B[32m"
	ansiBlue  = "\u001B[34m"

	// DefaultInterval is the Java handler thread sleep (Thread.sleep(2000)).
	DefaultInterval = 2 * time.Second
	// Prompt is the TermMain prompt text (green when ANSI is on).
	Prompt = "scouter> "
)

// Config wires a Console. Source, Out and Location are required.
type Config struct {
	Source Source
	// Out receives the command output (Java System.out).
	Out io.Writer
	// Err receives one-line error messages that replace the Java stack
	// traces; nil writes them to Out.
	Err      io.Writer
	Location *time.Location
	// Now is the clock; nil uses time.Now.
	Now func() time.Time
	// ANSI is AnsiPrint.enable.
	ANSI bool
	// Interval is the repeat period of dashboard/realtime/xlog; 0 is 2s.
	Interval time.Duration
	// Quit runs after `quit`/`q` printed "bye bye!!" (Java System.exit(0)):
	// it must start the server's graceful shutdown. Nil does nothing.
	Quit func()
	// Logf records console lifecycle events (EOF); nil discards them.
	Logf func(format string, args ...any)
}

// Console executes console commands. Run (or Execute) and Close are called
// from one goroutine; the only other goroutine a Console owns is the current
// repeat job, which Execute, Run's cancellation and Close stop and await.
type Console struct {
	cfg  Config
	out  *lockedWriter
	errw io.Writer
	now  func() time.Time

	base       context.Context
	cancelBase context.CancelFunc

	mu  sync.Mutex // guards job
	job *job

	// Java handler object state, owned by the running job (a job starts only
	// after the previous one has ended, so ownership passes through done).
	lines       int   // Dashboard.lines
	hostColumns int   // Dashboard.oldTitle.size()
	xlogLoop    int64 // XLOG.loop
	xlogIndex   int32 // XLOG.index

	closed    bool // guarded by mu
	closeOnce sync.Once
}

type job struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// New validates cfg and returns an idle Console.
func New(cfg Config) (*Console, error) {
	if cfg.Source == nil || cfg.Out == nil || cfg.Location == nil {
		return nil, errors.New("console: Source, Out and Location are required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	c := &Console{cfg: cfg, now: cfg.Now}
	if c.now == nil {
		c.now = time.Now
	}
	mu := &sync.Mutex{}
	c.out = &lockedWriter{mu: mu, w: cfg.Out}
	c.errw = c.out
	if cfg.Err != nil {
		c.errw = &lockedWriter{mu: mu, w: cfg.Err}
	}
	c.base, c.cancelBase = context.WithCancel(context.Background())
	return c, nil
}

// Writer is the serialized console output; a LineReader draws its prompt
// through it so prompts and repeat output never interleave mid-write.
func (c *Console) Writer() io.Writer { return c.out }

// PromptText is the prompt with the Java color applied.
func (c *Console) PromptText() string { return c.green(Prompt) }

// Run reads and executes lines until quit, EOF or ctx ends. EOF stops only
// the input loop (a running repeat job keeps printing until Close); the
// server is not shut down. Cancellation stops the repeat job before Run
// returns. Run returns nil for quit and EOF.
func (c *Console) Run(ctx context.Context, in LineReader) error {
	for {
		line, err := in.ReadLine(ctx, c.PromptText())
		switch {
		case err == nil:
		case ctx.Err() != nil:
			c.stopJob()
			return ctx.Err()
		case errors.Is(err, io.EOF):
			c.logf("console: input reached EOF; the console stops reading and the server keeps running")
			return nil
		case errors.Is(err, ErrLineTooLong):
			c.errorf("console: input line longer than %d bytes ignored", MaxLineBytes)
			continue
		default:
			c.stopJob()
			return err
		}
		if c.Execute(ctx, line) {
			return nil
		}
	}
}

// Execute runs one input line (ProcessMain.process) and reports whether it
// was quit. Every line, the empty one included, first stops the running
// repeat job (Java loopProcess += 1).
func (c *Console) Execute(ctx context.Context, line string) (quit bool) {
	cmd := javaTrim(line)
	c.stopJob()
	word := asciiLower(firstWord(cmd, " "))
	arg := func() string { return javaTrim(cmd[len(word):]) }
	switch word {
	case "help":
		c.help()
	case "q", "quit":
		c.println("bye bye!!")
		if c.cfg.Quit != nil {
			c.cfg.Quit()
		}
		return true
	case "objtype":
		c.objType(ctx)
	case "object":
		c.objectList(ctx, cmd)
	case "counter":
		c.counterList(cmd)
	case "dashboard":
		c.dashboard(ctx, arg())
	case "realtime":
		c.realtime(ctx, arg())
	case "xlog":
		c.xlog(ctx, arg())
	case "xlist":
		c.xlist(ctx, arg())
	case "tagcnt":
		c.tagcnt(ctx, arg())
	case "visitor":
		c.visitor(ctx, arg())
	case "debug":
		c.debug(arg())
	case "":
	default:
		c.help()
	}
	return false
}

// Close stops the repeat job and waits for it. It is idempotent.
func (c *Console) Close() error {
	c.closeOnce.Do(func() {
		c.stopJob()
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.cancelBase()
	})
	return nil
}

// repeat is the Java handler daemon loop. The first cycle runs here, before
// the next prompt (Java starts it on the new thread at once); later cycles
// run every Interval on the job goroutine until step returns false or the
// job is stopped.
func (c *Console) repeat(ctx context.Context, step func(ctx context.Context) bool) {
	if !step(ctx) || ctx.Err() != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	jobCtx, cancel := context.WithCancel(c.base)
	j := &job{cancel: cancel, done: make(chan struct{})}
	c.job = j
	go func() {
		defer close(j.done)
		timer := time.NewTimer(c.cfg.Interval)
		defer timer.Stop()
		for {
			select {
			case <-jobCtx.Done():
				return
			case <-timer.C:
			}
			if !step(jobCtx) || jobCtx.Err() != nil {
				return
			}
			timer.Reset(c.cfg.Interval)
		}
	}()
}

// stopJob cancels the running repeat job and waits until it has returned.
func (c *Console) stopJob() {
	c.mu.Lock()
	j := c.job
	c.job = nil
	c.mu.Unlock()
	if j != nil {
		j.cancel()
		<-j.done
	}
}

func (c *Console) println(s string) { _, _ = io.WriteString(c.out, s+"\n") }

func (c *Console) errorf(format string, args ...any) {
	_, _ = io.WriteString(c.errw, fmt.Sprintf(format, args...)+"\n")
}

// queryFailed reports a Source error in one line, except the cancellation
// of a stopped job or a shutting down console.
func (c *Console) queryFailed(ctx context.Context, cmd string, err error) {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return
	}
	c.errorf("%s: %v", cmd, err)
}

func (c *Console) logf(format string, args ...any) {
	if c.cfg.Logf != nil {
		c.cfg.Logf(format, args...)
	}
}

func (c *Console) green(s string) string {
	if !c.cfg.ANSI {
		return s
	}
	return ansiGreen + s + ansiReset
}

func (c *Console) blue(s string) string {
	if !c.cfg.ANSI {
		return s
	}
	return ansiBlue + s + ansiReset
}

func (c *Console) nowMillis() int64 { return c.now().UnixMilli() }

// lockedWriter serializes whole Write calls of the input loop, the repeat
// job and the line editor (Out and Err share one mutex).
type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// asciiLower lowers ASCII letters only: every command word is ASCII, and
// Java toLowerCase never maps a non-ASCII word onto one of them.
func asciiLower(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return s
		}
	}
	return strings.ToLower(s)
}
