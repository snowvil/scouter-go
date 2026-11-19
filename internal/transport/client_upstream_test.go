package transport

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"testing"
	"time"
)

// Stream.AwaitUpstream: a Client command waiting for an Agent answer
// is not ended by the progress limit (Java waits for the Agent whatever the
// Client does), but a Client that leaves meanwhile ends it at once.

// upstreamCommand is the fixture's first command frame (no magic, no CLOSE),
// which a Client may send again on the same connection.
func upstreamCommand(t *testing.T) []byte {
	t.Helper()
	request := fixture(t, "tcp-client")
	return request[4 : len(request)-6]
}

// A wait several times the progress limit ends with the answer delivered.
func TestClientUpstreamWaitOutlivesProgressLimit(t *testing.T) {
	_, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: 100 * time.Millisecond}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if _, err := s.ReadPack(); err != nil {
			return err
		}
		if err := s.AwaitUpstream(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(450 * time.Millisecond):
				return nil
			}
		}); err != nil {
			return err
		}
		return s.ResponseBlob([]byte{7})
	}})
	conn := dialTCP(t, listener)
	clientLogin(t, conn)
	if items, total := readBlobResponse(t, conn); items != 1 || total != 1 {
		t.Fatalf("items=%d bytes=%d", items, total)
	}
	stopTCP(t, cancel, done)
}

// A Client that closes or resets the connection during the wait ends the
// command context with ErrClientGone within milliseconds, not at the
// progress limit.
func TestClientUpstreamWaitEndsWhenClientLeaves(t *testing.T) {
	for _, reset := range []bool{false, true} {
		name := "close"
		if reset {
			name = "reset"
		}
		t.Run(name, func(t *testing.T) {
			waiting := make(chan struct{})
			ended := make(chan error, 1)
			_, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: 10 * time.Second}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
				if _, err := s.ReadPack(); err != nil {
					return err
				}
				err := s.AwaitUpstream(func() error {
					close(waiting)
					<-ctx.Done()
					return ctx.Err()
				})
				ended <- err
				return err
			}})
			conn := dialTCP(t, listener)
			clientLogin(t, conn)
			<-waiting
			if reset {
				conn.(*net.TCPConn).SetLinger(0)
			}
			left := time.Now()
			conn.Close()
			select {
			case err := <-ended:
				if !errors.Is(err, ErrClientGone) || !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				if elapsed := time.Since(left); elapsed > time.Second {
					t.Fatalf("Client exit noticed after %v", elapsed)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Client exit not noticed during the upstream wait")
			}
			stopTCP(t, cancel, done)
		})
	}
}

// Bytes the Client sends during the wait are not consumed by the probe: the
// next command parses normally after the answer.
func TestClientUpstreamWaitKeepsBytesSentMeanwhile(t *testing.T) {
	waiting := make(chan struct{}, 2)
	sent := make(chan struct{})
	commands := 0
	_, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: 100 * time.Millisecond}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if _, err := s.ReadPack(); err != nil {
			return err
		}
		commands++
		first := commands == 1
		if err := s.AwaitUpstream(func() error {
			waiting <- struct{}{}
			if first {
				<-sent
				time.Sleep(300 * time.Millisecond) // past the progress limit
			}
			return ctx.Err()
		}); err != nil {
			return err
		}
		return s.ResponseBlob([]byte{byte(commands)})
	}})
	conn := dialTCP(t, listener)
	clientLogin(t, conn)
	<-waiting
	if _, err := conn.Write(upstreamCommand(t)); err != nil {
		t.Fatal(err)
	}
	close(sent)
	for i := 1; i <= 2; i++ {
		if items, total := readBlobResponse(t, conn); items != 1 || total != 1 {
			t.Fatalf("command %d: items=%d bytes=%d", i, items, total)
		}
	}
	stopTCP(t, cancel, done)
}

// Server shutdown ends a command in an upstream wait at once.
func TestClientUpstreamWaitEndsOnShutdown(t *testing.T) {
	waiting := make(chan struct{})
	ended := make(chan error, 1)
	_, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: 10 * time.Second}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if _, err := s.ReadPack(); err != nil {
			return err
		}
		err := s.AwaitUpstream(func() error {
			close(waiting)
			<-ctx.Done()
			return ctx.Err()
		})
		ended <- err
		return err
	}})
	conn := dialTCP(t, listener)
	clientLogin(t, conn)
	<-waiting
	started := time.Now()
	stopTCP(t, cancel, done)
	select {
	case err := <-ended:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("upstream wait outlived the server")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("shutdown took %v", elapsed)
	}
}

// Outside an upstream wait the progress limit applies as before, restarting
// in full when the wait ends.
func TestClientProgressLimitAfterUpstreamWait(t *testing.T) {
	const limit = 150 * time.Millisecond
	type end struct {
		err   error
		after time.Duration
	}
	ended := make(chan end, 1)
	_, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: limit}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if _, err := s.ReadPack(); err != nil {
			return err
		}
		if err := s.AwaitUpstream(func() error { time.Sleep(2 * limit); return nil }); err != nil {
			return err
		}
		released := time.Now()
		<-ctx.Done() // no progress after the wait
		ended <- end{ctx.Err(), time.Since(released)}
		return ctx.Err()
	}})
	conn := dialTCP(t, listener)
	clientLogin(t, conn)
	select {
	case e := <-ended:
		if !errors.Is(e.err, context.DeadlineExceeded) || e.after < limit-20*time.Millisecond || e.after > 2*time.Second {
			t.Fatalf("progress end %v after %v", e.err, e.after)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("progress limit not applied after the upstream wait")
	}
	if _, err := io.ReadAll(conn); err != nil {
		t.Fatal("connection not closed after the progress limit:", err)
	}
	stopTCP(t, cancel, done)
}

// A panicking wait still stops the probe and restores the command's read
// so-timeout, so the reader is the handler's alone again (review finding).
func TestAwaitUpstreamPanicRestoresReader(t *testing.T) {
	serverSide, clientSide := net.Pipe()
	defer clientSide.Close()
	defer serverSide.Close()
	input := &idleReader{conn: serverSide}
	reader := bufio.NewReaderSize(input, 4096)
	stream := newStream(reader, io.Discard, TCPConfig{})
	stream.asClient(nil)
	c, cancel := withProgress(context.Background(), stream, time.Minute)
	defer cancel()
	const readIdle = 7 * time.Second
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("wait did not panic")
			}
		}()
		awaitUpstream(c, stream, input, reader, readIdle, func() error { panic("synthetic") })
	}()
	input.mu.Lock()
	idle, interrupted := input.idle, input.interrupted
	input.mu.Unlock()
	c.mu.Lock()
	holds := c.holds
	c.mu.Unlock()
	if idle != readIdle || interrupted || holds != 0 {
		t.Fatalf("after a panicking wait: idle=%v interrupted=%v holds=%d", idle, interrupted, holds)
	}
	go clientSide.Write([]byte{9})
	if b, err := reader.ReadByte(); err != nil || b != 9 {
		t.Fatal("reader after a panicking wait:", b, err)
	}
}

// A nested AwaitUpstream runs under the outer wait: it must not stop the
// outer probe, so a Client exit after it is still noticed (review finding).
func TestClientUpstreamNestedWaitKeepsOuterProbe(t *testing.T) {
	inner := make(chan struct{})
	ended := make(chan error, 1)
	_, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: 10 * time.Second}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if _, err := s.ReadPack(); err != nil {
			return err
		}
		err := s.AwaitUpstream(func() error {
			if err := s.AwaitUpstream(func() error { return nil }); err != nil {
				return err
			}
			close(inner)
			<-ctx.Done()
			return ctx.Err()
		})
		ended <- err
		return err
	}})
	conn := dialTCP(t, listener)
	clientLogin(t, conn)
	<-inner
	conn.Close()
	select {
	case err := <-ended:
		if !errors.Is(err, ErrClientGone) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Client exit not noticed after a nested wait")
	}
	stopTCP(t, cancel, done)
}

// After the wait, a read in the same command gets the Client so-timeout
// again (the probe ran without a read deadline).
func TestClientUpstreamWaitRestoresReadTimeout(t *testing.T) {
	const so = 200 * time.Millisecond
	type end struct {
		err   error
		after time.Duration
	}
	ended := make(chan end, 1)
	server, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: 10 * time.Second}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if _, err := s.ReadPack(); err != nil {
			return err
		}
		if err := s.AwaitUpstream(func() error { return nil }); err != nil {
			return err
		}
		started := time.Now()
		_, err := s.ReadPack() // the Client sends nothing more
		ended <- end{err, time.Since(started)}
		return err
	}})
	if err := server.SetClientSoTimeout(so); err != nil {
		t.Fatal(err)
	}
	conn := dialTCP(t, listener)
	clientLogin(t, conn)
	select {
	case e := <-ended:
		if !errors.Is(e.err, os.ErrDeadlineExceeded) || e.after < so-20*time.Millisecond || e.after > 2*time.Second {
			t.Fatalf("read after the wait ended with %v after %v", e.err, e.after)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("read after the wait has no so-timeout")
	}
	stopTCP(t, cancel, done)
}

// A Client that sends its next command during the wait and then closes or
// resets still ends the command with ErrClientGone.
func TestClientUpstreamWaitBytesThenLeave(t *testing.T) {
	for _, reset := range []bool{false, true} {
		name := "close"
		if reset {
			name = "reset"
		}
		t.Run(name, func(t *testing.T) {
			waiting := make(chan struct{})
			ended := make(chan error, 1)
			_, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: 10 * time.Second}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
				if _, err := s.ReadPack(); err != nil {
					return err
				}
				err := s.AwaitUpstream(func() error {
					close(waiting)
					<-ctx.Done()
					return ctx.Err()
				})
				ended <- err
				return err
			}})
			conn := dialTCP(t, listener)
			clientLogin(t, conn)
			<-waiting
			if _, err := conn.Write(upstreamCommand(t)); err != nil {
				t.Fatal(err)
			}
			time.Sleep(50 * time.Millisecond) // the probe peeks the bytes first
			if reset {
				conn.(*net.TCPConn).SetLinger(0)
			}
			conn.Close()
			select {
			case err := <-ended:
				if !errors.Is(err, ErrClientGone) {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Client exit after buffered bytes not noticed")
			}
			stopTCP(t, cancel, done)
		})
	}
}

// Thousands of short waits start and stop the probe without leaking a
// goroutine or ending the command.
func TestClientUpstreamWaitStress(t *testing.T) {
	const waits = 3000
	commands := 0
	_, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: 10 * time.Second}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if _, err := s.ReadPack(); err != nil {
			return err
		}
		commands++
		n := 1
		if commands == 2 {
			n = waits
		}
		for i := 0; i < n; i++ {
			if err := s.AwaitUpstream(func() error { return ctx.Err() }); err != nil {
				return err
			}
		}
		return s.ResponseBlob([]byte{byte(commands)})
	}})
	conn := dialTCP(t, listener)
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	clientLogin(t, conn)
	if items, _ := readBlobResponse(t, conn); items != 1 {
		t.Fatal("first command", items)
	}
	baseline := runtime.NumGoroutine()
	if _, err := conn.Write(upstreamCommand(t)); err != nil {
		t.Fatal(err)
	}
	if items, _ := readBlobResponse(t, conn); items != 1 {
		t.Fatal("stress command", items)
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(deadline) {
			t.Fatalf("goroutines %d after %d waits, %d before", runtime.NumGoroutine(), waits, baseline)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopTCP(t, cancel, done)
}

// A Client that resets during the wait counts as a closed connection in
// TCPStats and prints Java's short "closed" action line (review finding).
func TestClientUpstreamWaitClientGoneStatsAndLog(t *testing.T) {
	logs := &logCollector{}
	waiting := make(chan struct{})
	server, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: 10 * time.Second}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if _, err := s.ReadPack(); err != nil {
			return err
		}
		return s.AwaitUpstream(func() error {
			close(waiting)
			<-ctx.Done()
			return ctx.Err()
		})
	}})
	server.SetLogFunc(logs.add)
	server.SetActionLog(true)
	conn := dialTCP(t, listener)
	peer := javaPeer(conn)
	clientLogin(t, conn)
	<-waiting
	conn.(*net.TCPConn).SetLinger(0)
	conn.Close()
	logs.waitLines(t, "Client : "+peer+" open #1", "Client : "+peer+" closed")
	waitStats(t, server, "closed error", func(st TCPStats) bool { return st.Errors == 1 })
	if st := server.Stats(); st.ClosedErrors != 1 || st.TimeoutErrors+st.OtherErrors+st.LimitErrors+st.UnsupportedErrors+st.UnauthorizedErrors != 0 {
		t.Fatalf("stats %+v", st)
	}
	stopTCP(t, cancel, done)
}
