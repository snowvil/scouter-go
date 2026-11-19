package service

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// Agent relays wait for the Agent past the Client command progress limit
// (transport.Stream.AwaitUpstream), like Java's service thread, and
// end at once when the Client leaves.

// slowAgentFake answers each AgentRequest after delay, or ends with the
// request context.
type slowAgentFake struct {
	*fakeBackend
	delay   time.Duration
	started chan struct{}
	mu      sync.Mutex
	ends    []error
}

func (b *slowAgentFake) AgentRequest(ctx context.Context, hash int32, command string, p *protocol.MapPack) ([]protocol.Pack, error) {
	select {
	case b.started <- struct{}{}:
	default:
	}
	var err error
	select {
	case <-time.After(b.delay):
	case <-ctx.Done():
		err = ctx.Err()
	}
	b.mu.Lock()
	b.ends = append(b.ends, err)
	b.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return []protocol.Pack{newMap(entry("answer", decimal(int64(hash))))}, nil
}

// startServiceProgress is startService with a short Client progress limit.
func startServiceProgress(t *testing.T, backend Backend, limit time.Duration) (net.Conn, *bufio.Reader) {
	t.Helper()
	service, err := New(queryConfig(), backend)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := transport.NewTCPServer(listener, transport.TCPConfig{RequestTimeout: limit}, service.Handlers(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	var magic [4]byte
	binary.BigEndian.PutUint32(magic[:], transport.TCPClient)
	if _, err := conn.Write(magic[:]); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		cancel()
		server.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("server shutdown timeout")
		}
	})
	return conn, bufio.NewReader(conn)
}

func TestAgentRelayWaitOutlivesProgressLimit(t *testing.T) {
	const limit = 100 * time.Millisecond
	for _, tc := range []struct {
		command string
		param   *protocol.MapPack
		want    []int64
	}{
		{"OBJECT_HEAPHISTO", newMap(entry("objHash", decimal(42))), []int64{42}},
		// A fan-out waits per target; each wait alone outlives the limit.
		{"OBJECT_ACTIVE_SERVICE_LIST_GROUP", newMap(entry("objHash", list([]protocol.Value{decimal(7), decimal(8)}))), []int64{7, 8}},
		{"OBJECT_CALL_HEAP_DUMP", newMap(entry("objHash", decimal(42))), []int64{42}},
		// The silent mutation answers no item, after the Agent did.
		{"OBJECT_SYSTEM_GC", newMap(entry("objHash", decimal(42))), nil},
		// ConfigureService relays (server_metadata.go).
		{"CONFIGURE_DESC", newMap(entry("objHash", decimal(42))), []int64{42}},
		{"CONFIGURE_VALUE_TYPE", newMap(entry("objHash", decimal(42))), []int64{42}},
	} {
		t.Run(tc.command, func(t *testing.T) {
			captureManageLog(t)
			b := &slowAgentFake{fakeBackend: &fakeBackend{}, delay: 4 * limit}
			conn, r := startServiceProgress(t, b, limit)
			session := loginSession(t, conn, r)
			send(t, conn, tc.command, session, tc.param)
			packs := readPacks(t, r)
			if len(packs) != len(tc.want) {
				t.Fatal(packs)
			}
			for i, want := range tc.want {
				if got := mapValue(packs[i].(*protocol.MapPack), "answer").Int; got != want {
					t.Fatal(i, got, want)
				}
			}
			b.mu.Lock()
			ends := append([]error(nil), b.ends...)
			b.mu.Unlock()
			if len(ends) != max(len(tc.want), 1) {
				t.Fatal("agent calls", ends)
			}
			for _, err := range ends {
				if err != nil {
					t.Fatal("agent wait cut:", err)
				}
			}
			send(t, conn, "SERVER_TIME", session, nil)
			if len(readPacks(t, r)) != 1 {
				t.Fatal("stream desynchronized")
			}
		})
	}
}

func TestAgentRelayWaitEndsWhenClientLeaves(t *testing.T) {
	b := &slowAgentFake{fakeBackend: &fakeBackend{}, delay: time.Minute, started: make(chan struct{}, 1)}
	conn, r := startServiceProgress(t, b, 10*time.Second)
	session := loginSession(t, conn, r)
	send(t, conn, "OBJECT_HEAPHISTO", session, newMap(entry("objHash", decimal(42))))
	<-b.started
	left := time.Now()
	conn.Close()
	deadline := time.Now().Add(3 * time.Second)
	for {
		b.mu.Lock()
		ends := append([]error(nil), b.ends...)
		b.mu.Unlock()
		if len(ends) == 1 {
			if !errors.Is(ends[0], transport.ErrClientGone) {
				t.Fatal(ends[0])
			}
			if elapsed := time.Since(left); elapsed > time.Second {
				t.Fatalf("Client exit noticed after %v", elapsed)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Client exit not noticed during the agent wait")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// fanoutFake answers each target after its own delay (none: until the
// request context ends) and records the calls in order.
type fanoutFake struct {
	*fakeBackend
	delays  map[int32]time.Duration
	started chan int32
	mu      sync.Mutex
	calls   []int32
	ends    []error
}

func (b *fanoutFake) AgentRequest(ctx context.Context, hash int32, command string, p *protocol.MapPack) ([]protocol.Pack, error) {
	b.mu.Lock()
	b.calls = append(b.calls, hash)
	b.mu.Unlock()
	b.started <- hash
	var err error
	if delay, ok := b.delays[hash]; ok {
		time.Sleep(delay)
	} else {
		<-ctx.Done()
		err = ctx.Err()
	}
	b.mu.Lock()
	b.ends = append(b.ends, err)
	b.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return []protocol.Pack{newMap(entry("answer", decimal(int64(hash))))}, nil
}

// A Client that leaves while a fan-out waits for its second target ends the
// request there: the first answer was written, the third target is never
// called.
func TestAgentRelayFanOutClientLeavesMidway(t *testing.T) {
	// 250ms passes the 200ms flush interval, so the first answer reaches the
	// Client before the second target is called.
	b := &fanoutFake{fakeBackend: &fakeBackend{}, delays: map[int32]time.Duration{7: 250 * time.Millisecond, 9: 0}, started: make(chan int32, 3)}
	conn, r := startServiceProgress(t, b, 10*time.Second)
	session := loginSession(t, conn, r)
	send(t, conn, "OBJECT_ACTIVE_SERVICE_LIST_GROUP", session, newMap(entry("objHash", list([]protocol.Value{decimal(7), decimal(8), decimal(9)}))))
	if flag, err := r.ReadByte(); err != nil || flag != transport.HasNext {
		t.Fatal("first answer flag", flag, err)
	}
	first, err := protocol.ReadPack(r, protocol.Limits{})
	if err != nil || mapValue(first.(*protocol.MapPack), "answer").Int != 7 {
		t.Fatal("first answer", first, err)
	}
	if <-b.started != 7 || <-b.started != 8 {
		t.Fatal("fan-out order")
	}
	conn.Close()
	deadline := time.Now().Add(3 * time.Second)
	for {
		b.mu.Lock()
		ends := append([]error(nil), b.ends...)
		b.mu.Unlock()
		if len(ends) == 2 {
			if ends[0] != nil || !errors.Is(ends[1], transport.ErrClientGone) {
				t.Fatal(ends)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Client exit not noticed during the second target", ends)
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	b.mu.Lock()
	calls := append([]int32(nil), b.calls...)
	b.mu.Unlock()
	if len(calls) != 2 || calls[0] != 7 || calls[1] != 8 {
		t.Fatal("targets called after the Client left:", calls)
	}
}
