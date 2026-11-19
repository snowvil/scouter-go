package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func startTCP(t *testing.T, config TCPConfig, handlers TCPHandlers) (*TCPServer, net.Listener, context.CancelFunc, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewTCPServer(listener, config, handlers, nil)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() { cancel(); server.Close() })
	return server, listener, cancel, done
}
func dialTCP(t *testing.T, listener net.Listener) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	t.Cleanup(func() { conn.Close() })
	return conn
}
func stopTCP(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("TCP shutdown blocked")
	}
}
func writeSlow(t *testing.T, conn net.Conn, data []byte) {
	t.Helper()
	for _, b := range data {
		if _, err := conn.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
}
func mapInt(t *testing.T, p protocol.Pack) int64 {
	t.Helper()
	m, ok := p.(*protocol.MapPack)
	if !ok || len(m.Entries) != 1 {
		t.Fatalf("unexpected pack %#v", p)
	}
	return m.Entries[0].Value.Int
}

func TestJavaTCPClientAndClose(t *testing.T) {
	requests := make(chan ClientRequest, 1)
	_, listener, cancel, done := startTCP(t, TCPConfig{}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		requests <- r
		p, err := s.ReadPack()
		if err != nil {
			return err
		}
		if mapInt(t, p) != -7 {
			return errors.New("body")
		}
		return s.ResponsePack(p)
	}})
	conn := dialTCP(t, listener)
	writeSlow(t, conn, fixture(t, "tcp-client"))
	response, err := io.ReadAll(conn)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte{HasNext}, fixture(t, "tcp-agent-response-v1")[1:]...)
	if !bytes.Equal(response, want) {
		t.Fatalf("response %x want %x", response, want)
	}
	r := <-requests
	if r.Command != "LOGIN" || r.Session != 0 || r.Authenticated || r.Peer == nil {
		t.Fatal(r)
	}
	stopTCP(t, cancel, done)
}

func TestClientSessionLatchAndNoFakeSuccess(t *testing.T) {
	var checks atomic.Int32
	var requests atomic.Int32
	_, listener, cancel, done := startTCP(t, TCPConfig{}, TCPHandlers{
		Authorize: func(ctx context.Context, r ClientRequest) (bool, error) { checks.Add(1); return r.Session == 42, nil },
		Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
			if !r.Authenticated {
				return ErrUnauthorized
			}
			requests.Add(1)
			_, err := s.ReadPack()
			return err
		},
	})
	conn := dialTCP(t, listener)
	conn.Write(fixture(t, "tcp-client-latch"))
	response, err := io.ReadAll(conn)
	if err != nil || !bytes.Equal(response, []byte{NoNext, NoNext}) || checks.Load() != 1 || requests.Load() != 2 {
		t.Fatalf("latch response=%x err=%v checks=%d requests=%d", response, err, checks.Load(), requests.Load())
	}
	stopTCP(t, cancel, done)
	_, listener, cancel, done = startTCP(t, TCPConfig{}, TCPHandlers{})
	conn = dialTCP(t, listener)
	conn.Write(fixture(t, "tcp-client"))
	raw, _ := io.ReadAll(conn)
	if len(raw) != 0 {
		t.Fatal("missing handler returned success", raw)
	}
	stopTCP(t, cancel, done)
}

func TestTCPBodylessFreeCommandAndInvalidSession(t *testing.T) {
	enc := protocol.NewEncoder(protocol.Limits{})
	enc.Int32(int32(-889315327))
	enc.Text("SERVER_TIME")
	enc.Int64(0)
	enc.Text("CLOSE")
	raw, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	_, listener, cancel, done := startTCP(t, TCPConfig{}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if r.Command != "SERVER_TIME" {
			return ErrUnsupported
		}
		return s.ResponseBlob([]byte{1, 2})
	}})
	conn := dialTCP(t, listener)
	conn.Write(raw)
	got, err := io.ReadAll(conn)
	if err != nil || !bytes.Equal(got, []byte{HasNext, 2, 1, 2, NoNext}) {
		t.Fatalf("bodyless %x %v", got, err)
	}
	stopTCP(t, cancel, done)
	_, listener, cancel, done = startTCP(t, TCPConfig{}, TCPHandlers{Client: func(context.Context, ClientRequest, *Stream) error {
		t.Error("unauthorized handler invoked")
		return nil
	}})
	conn = dialTCP(t, listener)
	conn.Write(fixture(t, "tcp-client-latch"))
	got, _ = io.ReadAll(conn)
	if !bytes.Equal(got, []byte{InvalidSession}) {
		t.Fatalf("invalid session %x", got)
	}
	stopTCP(t, cancel, done)
}

func TestJavaTCPAgentV1V2(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			received := make(chan error, 1)
			_, listener, cancel, done := startTCP(t, TCPConfig{}, TCPHandlers{Agent: func(ctx context.Context, a *AgentConn) error {
				if a.ObjectHash != -7 || a.Version != version {
					return ErrMalformed
				}
				parameter := &protocol.MapPack{Entries: []protocol.Entry{{Key: "x", Value: protocol.Value{Type: protocol.ValueDecimal, Int: -7}}}}
				err := a.RequestPack(ctx, "KEEP_ALIVE", parameter, func(_ context.Context, p protocol.Pack) error {
					if mapInt(t, p) != -7 {
						return ErrMalformed
					}
					return nil
				})
				received <- err
				return err
			}})
			conn := dialTCP(t, listener)
			writeSlow(t, conn, fixture(t, "tcp-agent-v"+string(rune('0'+version))))
			command := fixture(t, "tcp-agent-command-v"+string(rune('0'+version)))
			got := make([]byte, len(command))
			if _, err := io.ReadFull(conn, got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, command) {
				t.Fatalf("server->agent command %x != %x", got, command)
			}
			writeSlow(t, conn, fixture(t, "tcp-agent-response-v"+string(rune('0'+version))))
			select {
			case err := <-received:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("agent response")
			}
			stopTCP(t, cancel, done)
		})
	}
}

func TestJavaTCPStackUploadAndNoEarlyOK(t *testing.T) {
	upload := make(chan StackUpload, 1)
	_, listener, cancel, done := startTCP(t, TCPConfig{}, TCPHandlers{Stack: func(ctx context.Context, meta StackUpload, body io.Reader) error {
		upload <- meta
		data, err := io.ReadAll(body)
		if err != nil {
			return err
		}
		if !bytes.Equal(data, []byte{'P', 'K', 3, 4, 0, 1, 2, 3}) {
			return ErrMalformed
		}
		return nil
	}})
	conn := dialTCP(t, listener)
	writeSlow(t, conn, fixture(t, "tcp-stack"))
	ack := make([]byte, 1)
	if _, err := io.ReadFull(conn, ack); err != nil || !bytes.Equal(ack, fixture(t, "tcp-stack-ok")) {
		t.Fatalf("ack %x %v", ack, err)
	}
	meta := <-upload
	if meta.ObjectHash != -7 || meta.StartTime != 1700000000123 || meta.ObjectName != "/synthetic/app" || meta.FileName != "stack.zip" || meta.Size != 8 {
		t.Fatal(meta)
	}
	stopTCP(t, cancel, done)
	_, listener, cancel, done = startTCP(t, TCPConfig{}, TCPHandlers{Stack: func(context.Context, StackUpload, io.Reader) error { return nil }})
	conn = dialTCP(t, listener)
	conn.Write(fixture(t, "tcp-stack"))
	got, _ := io.ReadAll(conn)
	if len(got) != 0 {
		t.Fatal("unconsumed upload acknowledged", got)
	}
	stopTCP(t, cancel, done)
}

func TestTCPBoundsDeadlineSaturationAndShutdown(t *testing.T) {
	server, listener, cancel, done := startTCP(t, TCPConfig{MaxHandshakes: 1, HandshakeTimeout: 40 * time.Millisecond, IdleTimeout: 40 * time.Millisecond}, TCPHandlers{})
	first := dialTCP(t, listener)
	deadline := time.Now().Add(time.Second)
	for server.Active() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	dialRejected(t, listener, fixture(t, "tcp-agent-v1")) // no response, reset
	raw, _ := io.ReadAll(first)
	if len(raw) != 0 {
		t.Fatal("handshake timeout response")
	}
	deadline = time.Now().Add(time.Second)
	for server.Active() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if st := server.Stats(); server.Active() != 0 || st.Rejected == 0 || st.HandshakeRejected != st.Rejected || st.Handshaking != 0 {
		t.Fatal(st)
	}
	// Same buffering must preserve data already received after the handshake.
	bad := dialTCP(t, listener)
	bad.Write([]byte{0, 0, 0, 0})
	io.ReadAll(bad)
	stopTCP(t, cancel, done)
	if server.Active() != 0 {
		t.Fatal("active connections after shutdown")
	}
	if _, err := listener.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatal("listener owned", err)
	}
	for _, config := range []TCPConfig{{MaxConnections: -1}, {MaxHandshakes: -1}, {MaxAgentConnections: -1}} {
		if _, err := NewTCPServer(listener, config, TCPHandlers{}, nil); err == nil {
			t.Fatalf("negative limit %+v", config)
		}
	}
}

func TestTCPAgentV2MalformedLengthAndStackBound(t *testing.T) {
	_, listener, cancel, done := startTCP(t, TCPConfig{MaxFrameBytes: 128}, TCPHandlers{Agent: func(ctx context.Context, a *AgentConn) error {
		return a.RequestPack(ctx, "KEEP_ALIVE", &protocol.MapPack{}, func(context.Context, protocol.Pack) error { return nil })
	}})
	conn := dialTCP(t, listener)
	conn.Write(fixture(t, "tcp-agent-v2"))
	prefix := make([]byte, 4)
	if _, err := io.ReadFull(conn, prefix); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, int(binary.BigEndian.Uint32(prefix)))
	io.ReadFull(conn, body)
	conn.Write([]byte{HasNext, 255, 255, 255, 255})
	raw, _ := io.ReadAll(conn)
	if len(raw) != 0 {
		t.Fatal("negative length accepted")
	}
	stopTCP(t, cancel, done)
	called := false
	_, listener, cancel, done = startTCP(t, TCPConfig{MaxUploadBytes: 4}, TCPHandlers{Stack: func(context.Context, StackUpload, io.Reader) error { called = true; return nil }})
	conn = dialTCP(t, listener)
	conn.Write(fixture(t, "tcp-stack"))
	io.ReadAll(conn)
	stopTCP(t, cancel, done)
	if called {
		t.Fatal("oversized stack reached handler")
	}
}

func FuzzTCPHeader(f *testing.F) {
	f.Add([]byte{0xca, 0xfe, 0x20, 0x01})
	f.Add(fixture(f, "tcp-client"))
	f.Add(fixture(f, "tcp-stack"))
	f.Fuzz(func(t *testing.T, data []byte) {
		s := newStream(bytes.NewReader(data), io.Discard, TCPConfig{MaxFrameBytes: 1024, MaxTextBytes: 128})
		magic, err := s.ReadInt32()
		if err == nil {
			switch uint32(magic) {
			case TCPClient:
				s.ReadText()
				s.ReadInt64()
			case TCPAgentV1, TCPAgentV2, TCPAgentRequest:
				s.ReadInt32()
			}
		}
	})
}

func TestTCPRequestDeadlineCancelsCallbacks(t *testing.T) {
	for _, kind := range []string{"authorize", "client", "stack"} {
		t.Run(kind, func(t *testing.T) {
			callback := make(chan error, 1)
			wait := func(ctx context.Context) error { <-ctx.Done(); callback <- ctx.Err(); return ctx.Err() }
			handlers := TCPHandlers{}
			input := fixture(t, "tcp-client")
			switch kind {
			case "authorize":
				handlers.Authorize = func(ctx context.Context, _ ClientRequest) (bool, error) { return false, wait(ctx) }
				handlers.Client = func(context.Context, ClientRequest, *Stream) error { return nil }
				input = fixture(t, "tcp-client-latch")
			case "client":
				handlers.Client = func(ctx context.Context, _ ClientRequest, _ *Stream) error { return wait(ctx) }
			case "stack":
				handlers.Stack = func(ctx context.Context, _ StackUpload, _ io.Reader) error { return wait(ctx) }
				input = fixture(t, "tcp-stack")
			}
			_, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: 20 * time.Millisecond}, handlers)
			conn := dialTCP(t, listener)
			conn.Write(input)
			select {
			case err := <-callback:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
			case <-time.After(150 * time.Millisecond):
				t.Error("request deadline did not cancel callback")
			}
			stopTCP(t, cancel, done)
		})
	}
}

func TestTCPTruncatedCommandIsReported(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan ErrorEvent, 1)
	server, err := NewTCPServer(listener, TCPConfig{}, TCPHandlers{}, func(e ErrorEvent) { events <- e })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	conn := dialTCP(t, listener)
	conn.Write([]byte{0xca, 0xfe, 0x20, 0x01, 10}) // Text prefix says 10; body has no bytes.
	conn.(*net.TCPConn).CloseWrite()
	io.ReadAll(conn)
	select {
	case e := <-events:
		if !errors.Is(e.Err, io.EOF) && !errors.Is(e.Err, io.ErrUnexpectedEOF) {
			t.Fatal(e)
		}
	case <-time.After(150 * time.Millisecond):
		t.Error("truncated command silently accepted as clean EOF")
	}
	stopTCP(t, cancel, done)
}

func TestAgentWaitingRequestCancellationDoesNotCloseActiveRequest(t *testing.T) {
	agent := make(chan *AgentConn, 1)
	_, listener, cancel, done := startTCP(t, TCPConfig{}, TCPHandlers{Agent: func(ctx context.Context, a *AgentConn) error { agent <- a; <-ctx.Done(); return ctx.Err() }})
	conn := dialTCP(t, listener)
	conn.Write(fixture(t, "tcp-agent-v1"))
	a := <-agent
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- a.RequestPack(context.Background(), "KEEP_ALIVE", &protocol.MapPack{}, func(context.Context, protocol.Pack) error { return nil })
	}()
	command := make([]byte, 13)
	if _, err := io.ReadFull(conn, command); err != nil {
		t.Fatal(err)
	} // Text(KEEP_ALIVE)+Map(0)
	waitingCtx, waitingCancel := context.WithCancel(context.Background())
	waitingDone := make(chan error, 1)
	go func() {
		waitingDone <- a.RequestPack(waitingCtx, "KEEP_ALIVE", &protocol.MapPack{}, func(context.Context, protocol.Pack) error { return nil })
	}()
	waitingCancel()
	select {
	case err := <-waitingDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("cancelled request blocked on agent serialization")
	}
	if a.Closed() {
		t.Fatal("queued cancellation retired the active connection")
	}
	conn.Write([]byte{NoNext})
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal("waiting cancellation closed active request", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first request")
	}
	stopTCP(t, cancel, done)
}

func TestAgentRequestDeadlineCancelsResponseCallback(t *testing.T) {
	for _, kind := range []string{"custom", "pack"} {
		t.Run(kind, func(t *testing.T) {
			completed := make(chan error, 1)
			server, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: 20 * time.Millisecond}, TCPHandlers{Agent: func(ctx context.Context, a *AgentConn) error {
				var err error
				if kind == "custom" {
					err = a.Request(ctx, "KEEP_ALIVE", &protocol.MapPack{}, func(requestCtx context.Context, s *Stream) error {
						if _, err := s.ReadPack(); err != nil {
							return err
						}
						<-requestCtx.Done()
						return requestCtx.Err()
					})
				} else {
					err = a.RequestPack(ctx, "KEEP_ALIVE", &protocol.MapPack{}, func(requestCtx context.Context, _ protocol.Pack) error { <-requestCtx.Done(); return requestCtx.Err() })
				}
				completed <- err
				return err
			}})
			// The total deadline is so_timeout+RequestTimeout (40ms here).
			if err := server.SetAgentSoTimeout(20 * time.Millisecond); err != nil {
				t.Fatal(err)
			}
			conn := dialTCP(t, listener)
			conn.Write(fixture(t, "tcp-agent-v1"))
			command := make([]byte, 13)
			if _, err := io.ReadFull(conn, command); err != nil {
				t.Fatal(err)
			}
			conn.Write([]byte{HasNext, protocol.PackMap, 0})
			select {
			case err := <-completed:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
			case <-time.After(150 * time.Millisecond):
				t.Error("agent callback deadline")
			}
			stopTCP(t, cancel, done)
		})
	}
}

func TestAgentV2NoNextHasNoLengthAndCustomBlobResponse(t *testing.T) {
	completed := make(chan error, 1)
	_, listener, cancel, done := startTCP(t, TCPConfig{}, TCPHandlers{Agent: func(ctx context.Context, a *AgentConn) error {
		err := a.Request(ctx, "CUSTOM_BLOB", &protocol.MapPack{}, func(_ context.Context, s *Stream) error {
			data, err := s.ReadBlob()
			if err == nil && !bytes.Equal(data, []byte{1, 2, 3}) {
				return ErrMalformed
			}
			return err
		})
		completed <- err
		return err
	}})
	conn := dialTCP(t, listener)
	conn.Write(fixture(t, "tcp-agent-v2"))
	size := make([]byte, 4)
	if _, err := io.ReadFull(conn, size); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, int(binary.BigEndian.Uint32(size)))
	io.ReadFull(conn, body)
	conn.Write([]byte{HasNext, 3, 1, 2, 3, NoNext}) // special command raw Blob has no V2 intBytes wrapper.
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("custom response")
	}
	stopTCP(t, cancel, done)
}

func TestClientHandlerFailureFlushesPartialResponse(t *testing.T) {
	_, listener, cancel, done := startTCP(t, TCPConfig{}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		p, err := s.ReadPack()
		if err != nil {
			return err
		}
		if err = s.ResponsePack(p); err != nil {
			return err
		}
		return errors.New("original handler fails after first response")
	}})
	conn := dialTCP(t, listener)
	request := fixture(t, "tcp-client")
	writeSlow(t, conn, request[:len(request)-6])
	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatal(err)
	}
	full := fixture(t, "tcp-agent-response-v1")
	want := append([]byte{HasNext}, full[1:len(full)-1]...)
	if !bytes.Equal(got, want) {
		t.Fatalf("partial=%x want=%x", got, want)
	}
	stopTCP(t, cancel, done)
}

// The pending-send cap bounds one encoded item; a failing item still flushes
// the items before it without a success terminator.
func TestClientPendingItemLimitFlushesPartialResponse(t *testing.T) {
	old := clientPendingLimit
	clientPendingLimit = 64
	t.Cleanup(func() { clientPendingLimit = old })
	small, large := bytes.Repeat([]byte{42}, 63), bytes.Repeat([]byte{43}, 64)
	_, listener, cancel, done := startTCP(t, TCPConfig{}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if _, err := s.ReadPack(); err != nil {
			return err
		}
		if err := s.ResponseRaw(small); err != nil {
			return err
		}
		return s.ResponseRaw(large) // HasNEXT + 64 bytes exceeds 64
	}})
	conn := dialTCP(t, listener)
	request := fixture(t, "tcp-client")
	writeSlow(t, conn, request[:len(request)-6])
	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte{HasNext}, small...)
	if !bytes.Equal(got, want) {
		t.Fatalf("partial bytes=%d want=%d", len(got), len(want))
	}
	stopTCP(t, cancel, done)
}
