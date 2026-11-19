package transport

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

func TestUDPStatsQueueLengthLifecycle(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	var once sync.Once
	r, err := NewUDPReceiver(conn, UDPConfig{Workers: 1, QueueCapacity: 3}, func(ctx context.Context, p Packet) error {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return ctx.Err()
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st := r.Stats(); st.QueueLen != 0 || st.QueueCap != 0 {
		t.Fatalf("before Serve %+v", st)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Serve(ctx) }()
	stopPoll := make(chan struct{})
	polled := make(chan struct{})
	go func() { // concurrent readers must be race-free during the whole lifecycle
		defer close(polled)
		for {
			select {
			case <-stopPoll:
				return
			default:
				if st := r.Stats(); st.QueueLen > st.QueueCap {
					t.Error("length above capacity", st)
				}
			}
		}
	}()
	client, err := net.DialUDP("udp", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	raw := fixture(t, "udp-cafe")
	client.Write(raw)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler not entered")
	}
	for i := 0; i < 2; i++ {
		client.Write(raw)
	}
	deadline := time.Now().Add(time.Second)
	for r.Stats().QueueLen != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if st := r.Stats(); st.QueueLen != 2 || st.QueueCap != 3 {
		t.Fatalf("while serving %+v", st)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown")
	}
	close(stopPoll)
	<-polled
	if st := r.Stats(); st.QueueLen != 0 || st.QueueCap != 0 || st.ShutdownDiscarded != 2 {
		t.Fatalf("after Serve %+v", st)
	}
	if n := r.queuedBytes.Load(); n != 0 {
		t.Fatal("queued bytes left after Serve", n)
	}
}

// Queued payload is bounded by MaxQueuedBytes as well as by the datagram
// count: past it a datagram is rejected and counted, and the bytes are
// released as workers take datagrams.
func TestUDPQueueByteBound(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	raw := fixture(t, "udp-cafe")
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	r, err := NewUDPReceiver(conn, UDPConfig{Workers: 1, QueueCapacity: 100, MaxQueuedBytes: 2 * len(raw)}, func(ctx context.Context, p Packet) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Serve(ctx) }()
	client, err := net.DialUDP("udp", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.Write(raw) // taken by the blocked worker
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler not entered")
	}
	for i := 0; i < 4; i++ { // two fit in the byte bound, two are rejected
		client.Write(raw)
	}
	deadline := time.Now().Add(time.Second)
	for r.Stats().Rejected != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if st := r.Stats(); st.QueueLen != 2 || st.Rejected != 2 {
		t.Fatalf("byte bound %+v", st)
	}
	close(release)
	deadline = time.Now().Add(time.Second)
	for r.queuedBytes.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n := r.queuedBytes.Load(); n != 0 {
		t.Fatal("queued bytes not released", n)
	}
	cancel()
	<-done
}
