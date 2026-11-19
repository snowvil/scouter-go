package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Connection-scale test of 2026-10-22: Client commands that end at the progress
// limit used to write one "tcp.connection: context deadline exceeded" event
// each. They are now one event per class every 10 seconds; TCPStats.Errors
// keeps the exact count.
func TestTCPConnectionErrorsRateLimited(t *testing.T) {
	const clients = 5
	server, listener, _, _, events := startTCPEvents(t, TCPConfig{RequestTimeout: 100 * time.Millisecond}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if _, err := s.ReadPack(); err != nil {
			return err
		}
		<-ctx.Done() // no progress: the command ends at the progress limit
		return ctx.Err()
	}})
	for i := 0; i < clients; i++ {
		conn := dialTCP(t, listener)
		clientLogin(t, conn)
	}
	waitStats(t, server, "progress timeouts", func(st TCPStats) bool { return st.Errors == clients })
	var got []ErrorEvent
	for _, e := range events() {
		if e.Operation == "tcp.connection" {
			got = append(got, e)
		}
	}
	if len(got) != 1 || !errors.Is(got[0].Err, context.DeadlineExceeded) {
		t.Fatalf("events %+v", got)
	}
	if st := server.Stats(); st.TimeoutErrors != clients || st.OtherErrors+st.ClosedErrors+st.LimitErrors+st.UnsupportedErrors+st.UnauthorizedErrors != 0 {
		t.Fatalf("stats %+v", st)
	}
}

// Scanner noise (unknown magic) has its own class: it does not use up the
// "other" line a handler or storage error needs.
func TestTCPConnectionErrorsUnknownMagicApart(t *testing.T) {
	server, listener, _, _, events := startTCPEvents(t, TCPConfig{}, TCPHandlers{})
	for i := 0; i < 3; i++ {
		conn := dialTCP(t, listener)
		conn.Write([]byte("GET / HTTP/1.0\r\n\r\n"))
	}
	waitStats(t, server, "unknown magic errors", func(st TCPStats) bool { return st.UnsupportedErrors == 3 })
	n := 0
	for _, e := range events() {
		if e.Operation == "tcp.connection" {
			n++
		}
	}
	if st := server.Stats(); n != 1 || st.OtherErrors != 0 || st.Errors != 3 {
		t.Fatalf("events=%d stats %+v", n, st)
	}
}

func TestTCPConnectionErrorsConcurrent(t *testing.T) {
	var mu sync.Mutex
	got := 0
	var more uint64
	s := &TCPServer{errors: func(e ErrorEvent) {
		mu.Lock()
		defer mu.Unlock()
		got++
		// Errors counted before the logged one's Swap ride on its line.
		fmt.Sscanf(strings.TrimPrefix(e.Err.Error(), io.EOF.Error()+" ("), "%d more", &more)
	}}
	var wg sync.WaitGroup
	now := time.Now()
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				s.reportConnError("p", io.EOF, now)
			}
		}()
	}
	wg.Wait()
	var st TCPStats
	s.connErrorStats(&st)
	if unlogged := s.connErrors[connErrorClosed].unlogged.Load(); got != 1 || st.ClosedErrors != 1600 || more+unlogged != 1599 {
		t.Fatal(got, st.ClosedErrors, more, unlogged)
	}
}

func TestTCPConnectionErrorClassesAndCarry(t *testing.T) {
	var got []ErrorEvent
	s := &TCPServer{errors: func(e ErrorEvent) { got = append(got, e) }}
	now := time.Now()
	for i := 0; i < 3; i++ {
		s.reportConnError("p", context.DeadlineExceeded, now)
	}
	s.reportConnError("p", io.EOF, now) // another class has its own interval
	s.reportConnError("p", os.ErrDeadlineExceeded, now.Add(rejectLogEvery))
	if len(got) != 3 || got[0].Err != context.DeadlineExceeded || got[1].Err != io.EOF {
		t.Fatalf("events %+v", got)
	}
	if last := got[2].Err; !errors.Is(last, os.ErrDeadlineExceeded) || !strings.Contains(last.Error(), "(2 more timeout errors since the last line)") {
		t.Fatal(last)
	}
	for _, c := range []struct {
		err   error
		class int
	}{
		{fmt.Errorf("x: %w", ErrLimit), connErrorLimit},
		{ErrResponseBudget, connErrorLimit},
		{&net.OpError{Op: "read", Err: os.ErrDeadlineExceeded}, connErrorTimeout},
		{io.ErrUnexpectedEOF, connErrorClosed},
		{&net.OpError{Op: "write", Err: syscall.EPIPE}, connErrorClosed},
		{ErrMalformed, connErrorOther},
		{fmt.Errorf("%w: TCP magic 47455420", ErrUnsupported), connErrorUnsupported},
		{&invalidSessionError{command: "X"}, connErrorUnauthorized},
		{errors.Join(&invalidSessionError{command: "X"}, io.EOF), connErrorUnauthorized},
	} {
		if got := connErrorClass(c.err); got != c.class {
			t.Errorf("%v: class %d want %d", c.err, got, c.class)
		}
	}
}
