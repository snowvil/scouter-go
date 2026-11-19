package console

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a goroutine-safe output sink.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// checkGoroutines fails when goroutines started by the test outlive it.
func checkGoroutines(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			t.Fatalf("goroutines %d > %d\n%s", runtime.NumGoroutine(), before, buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newRunConsole(t *testing.T, out io.Writer, quit func(), logs *[]string, interval time.Duration) *Console {
	t.Helper()
	var mu sync.Mutex
	c, err := New(Config{Source: seedSource(), Out: out, Location: seoul, Interval: interval, Quit: quit,
		Logf: func(f string, a ...any) { mu.Lock(); *logs = append(*logs, f); mu.Unlock() }})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestRepeatStopsOnNextInput: a repeat command prints every interval and
// the next line (even an empty one) stops it within the 2s Java bound; the
// job goroutine is gone afterwards.
func TestRepeatStopsOnNextInput(t *testing.T) {
	before := runtime.NumGoroutine()
	out := &syncBuffer{}
	var logs []string
	c := newRunConsole(t, out, nil, &logs, 20*time.Millisecond)
	pr, pw := io.Pipe()
	in := NewPlainReader(pr, c.Writer(), pw.Close)
	done := make(chan error, 1)
	go func() { done <- c.Run(context.Background(), in) }()
	io.WriteString(pw, "realtime tomcat TPS\n")
	waitFor(t, "three cycles", func() bool { return strings.Count(out.String(), "/host1/tomcat1 12") >= 3 })
	start := time.Now()
	io.WriteString(pw, "\n")
	waitFor(t, "second prompt", func() bool { return strings.Count(out.String(), Prompt) >= 3 })
	stopped := out.String()
	time.Sleep(100 * time.Millisecond)
	if out.String() != stopped {
		t.Fatalf("output after the stop line:\n%s", strings.TrimPrefix(out.String(), stopped))
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("stop took %v", d)
	}
	pw.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 || !strings.Contains(logs[0], "EOF") {
		t.Fatalf("EOF log %q", logs)
	}
	c.Close()
	in.Close()
	checkGoroutines(t, before)
}

// TestEOFKeepsRepeatUntilClose: EOF ends only the input loop (no busy loop,
// one log line); a running repeat command continues until Close.
func TestEOFKeepsRepeatUntilClose(t *testing.T) {
	before := runtime.NumGoroutine()
	out := &syncBuffer{}
	var logs []string
	c := newRunConsole(t, out, func() { t.Error("EOF must not quit") }, &logs, 10*time.Millisecond)
	in := NewPlainReader(strings.NewReader("xlog tomcat 5"), c.Writer(), nil)
	if err := c.Run(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	n := strings.Count(out.String(), "5sec")
	waitFor(t, "repeat after EOF", func() bool { return strings.Count(out.String(), "5sec") > n+2 })
	if _, err := in.ReadLine(context.Background(), ""); !errors.Is(err, io.EOF) {
		t.Fatalf("after EOF: %v", err)
	}
	c.Close()
	settled := out.String()
	time.Sleep(50 * time.Millisecond)
	if out.String() != settled {
		t.Fatal("repeat survived Close")
	}
	c.Execute(context.Background(), "dashboard tomcat") // after Close: no new job
	in.Close()
	checkGoroutines(t, before)
	if len(logs) != 1 {
		t.Fatalf("logs %q", logs)
	}
}

func TestQuitAndCancel(t *testing.T) {
	before := runtime.NumGoroutine()
	out := &syncBuffer{}
	var logs []string
	quits := 0
	c := newRunConsole(t, out, func() { quits++ }, &logs, time.Hour)
	in := NewPlainReader(strings.NewReader("dashboard tomcat\nquit\nhelp\n"), c.Writer(), nil)
	if err := c.Run(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if quits != 1 || !strings.HasSuffix(out.String(), "bye bye!!\n") || strings.Contains(out.String(), "help = Help") {
		t.Fatalf("quit %d\n%s", quits, out.String())
	}
	in.Close()
	c.Close()

	// Cancellation: Run returns ctx.Err() and stops the repeat job first.
	c = newRunConsole(t, out, nil, &logs, time.Millisecond)
	pr, pw := io.Pipe()
	in = NewPlainReader(pr, c.Writer(), pw.Close)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, in) }()
	io.WriteString(pw, "realtime * TPS\n")
	waitFor(t, "repeat", func() bool { return strings.Contains(out.String(), "/host2/tomcat2") })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := in.Close(); err != nil {
		t.Fatal(err)
	}
	c.Close()
	checkGoroutines(t, before)
}

// TestStopWaitsForBlockedCycle: stopping a job whose query blocks cancels the
// query context instead of waiting for it.
func TestStopWaitsForBlockedCycle(t *testing.T) {
	src := seedSource()
	errs := &syncBuffer{}
	c, err := New(Config{Source: src, Out: io.Discard, Err: errs, Location: seoul, Interval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	c.Execute(context.Background(), "realtime tomcat TPS")
	src.mu.Lock()
	src.gate = make(chan struct{}) // the next cycle blocks in its query
	src.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	c.Execute(context.Background(), "")
	if d := time.Since(start); d > time.Second {
		t.Fatalf("stop took %v", d)
	}
	if errs.String() != "" {
		t.Fatalf("a cancelled query must not report an error: %q", errs.String())
	}
	c.Close()
}

func TestPlainReaderLines(t *testing.T) {
	long := strings.Repeat("x", MaxLineBytes+10)
	out := &syncBuffer{}
	r := NewPlainReader(strings.NewReader("a\r\n"+long+"\nb\nlast"), out, nil)
	ctx := context.Background()
	for _, want := range []struct {
		line string
		err  error
	}{{"a", nil}, {"", ErrLineTooLong}, {"b", nil}, {"last", nil}, {"", io.EOF}, {"", io.EOF}} {
		line, err := r.ReadLine(ctx, "> ")
		if line != want.line || !errors.Is(err, want.err) && !(err == nil && want.err == nil) {
			t.Fatalf("got %q %v want %q %v", line, err, want.line, want.err)
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "> > ") {
		t.Fatal(out.String())
	}
	// Close without any read and a blocked source without unblock.
	if err := NewPlainReader(strings.NewReader(""), out, nil).Close(); err != nil {
		t.Fatal(err)
	}
	pr, pw := io.Pipe()
	defer pw.Close()
	blocked := NewPlainReader(pr, out, nil)
	cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := blocked.ReadLine(cctx, ""); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := blocked.Close(); err == nil {
		t.Fatal("a blocked source without unblock must be reported")
	}
	pw.Close() // releases the reader goroutine
}

func TestLongLineReportedByRun(t *testing.T) {
	out := &syncBuffer{}
	var logs []string
	c := newRunConsole(t, out, nil, &logs, time.Hour)
	in := NewPlainReader(strings.NewReader(strings.Repeat("y", MaxLineBytes*2)+"\nobjtype\n"), c.Writer(), nil)
	if err := c.Run(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "longer than 4096 bytes") || !strings.Contains(out.String(), "\ttomcat") {
		t.Fatal(out.String())
	}
	c.Close()
	in.Close()
}
