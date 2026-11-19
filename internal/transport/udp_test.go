package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func fixture(t testing.TB, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "protocol", "testdata", "java", "transport-"+name+".bin"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestJavaUDPForms(t *testing.T) {
	sender := netip.MustParseAddrPort("127.0.0.1:12345")
	for _, magic := range []string{"cafe", "java", "cafn", "javn", "cafm", "jmtu"} {
		t.Run(magic, func(t *testing.T) {
			assembler, err := NewReassembler(UDPConfig{})
			if err != nil {
				t.Fatal(err)
			}
			var packets []protocol.Pack
			if magic == "cafm" || magic == "jmtu" {
				// Out-of-order and repeated identical fragments are valid.
				first := fixture(t, "udp-"+magic+"-1")
				for i := 0; i < 2; i++ {
					p, err := assembler.Decode(sender, first, time.Now())
					if err != nil || len(p) != 0 {
						t.Fatalf("fragment pending: %v %d", err, len(p))
					}
				}
				packets, err = assembler.Decode(sender, fixture(t, "udp-"+magic+"-0"), time.Now())
			} else {
				packets, err = assembler.Decode(sender, fixture(t, "udp-"+magic), time.Now())
			}
			want := 1
			if magic == "cafn" || magic == "javn" {
				want = 2
			}
			if err != nil || len(packets) != want {
				t.Fatalf("packets=%d err=%v", len(packets), err)
			}
			text, ok := packets[0].(*protocol.TextPack)
			if !ok || text.Hash != -123 || text.Text != "synthetic transport 한글" {
				t.Fatalf("TextPack: %#v", packets[0])
			}
			if assembler.Pending() != 0 || assembler.BufferedBytes() != 0 {
				t.Fatal("completed assembly retained")
			}
		})
	}
}

func TestUDPIsolationLimitsExpiry(t *testing.T) {
	a, err := NewReassembler(UDPConfig{MaxAssemblies: 2, MaxBufferedBytes: 100, MaxAssemblyBytes: 100, FragmentTTL: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	first, second := fixture(t, "udp-cafm-0"), fixture(t, "udp-cafm-1")
	p1, p2 := netip.MustParseAddrPort("127.0.0.1:1"), netip.MustParseAddrPort("127.0.0.1:2")
	now := time.Now()
	if _, err = a.Decode(p1, first, now); err != nil {
		t.Fatal(err)
	}
	if packets, err := a.Decode(p2, second, now); err != nil || len(packets) != 0 {
		t.Fatal("mixed sender", err)
	}
	if a.Pending() != 2 {
		t.Fatal("sender isolation")
	}
	third := netip.MustParseAddrPort("127.0.0.1:3")
	if _, err = a.Decode(third, first, now); !errors.Is(err, ErrLimit) {
		t.Fatal("assembly bound", err)
	}
	if expired := a.Expire(now.Add(10 * time.Millisecond)); len(expired) != 2 {
		t.Fatal("expiry", expired)
	}
	if a.Pending() != 0 || a.BufferedBytes() != 0 {
		t.Fatal("expiry retained bytes")
	}
	tiny, _ := NewReassembler(UDPConfig{MaxAssemblyBytes: 5})
	if _, err = tiny.Decode(p1, first, now); !errors.Is(err, ErrLimit) {
		t.Fatal("size bound", err)
	}
	bad := append([]byte(nil), first...)
	binary.BigEndian.PutUint16(bad[16:18], 0)
	if _, err = a.Decode(p1, bad, now); !errors.Is(err, ErrMalformed) {
		t.Fatal("zero total", err)
	}
	bad = append([]byte(nil), first...)
	binary.BigEndian.PutUint16(bad[18:20], 2)
	if _, err = a.Decode(p1, bad, now); !errors.Is(err, ErrMalformed) {
		t.Fatal("fragment index", err)
	}
	if _, err = a.Decode(p1, first, now); err != nil {
		t.Fatal(err)
	}
	conflict := append([]byte(nil), first...)
	conflict[len(conflict)-1] ^= 1
	if _, err = a.Decode(p1, conflict, now); !errors.Is(err, ErrMalformed) {
		t.Fatal("conflicting duplicate", err)
	}
	if a.Pending() != 0 {
		t.Fatal("conflicting assembly retained")
	}
	if _, err = a.Decode(p1, append(fixture(t, "udp-cafe"), 0), now); !errors.Is(err, protocol.ErrTrailing) {
		t.Fatal("trailing", err)
	}
}

func TestUDPReceiverLoopbackAndShutdown(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	packets := make(chan Packet, 4)
	events := make(chan ErrorEvent, 8)
	receiver, err := NewUDPReceiver(conn, UDPConfig{Workers: 2, QueueCapacity: 4}, func(ctx context.Context, p Packet) error { packets <- p; return nil }, func(e ErrorEvent) { events <- e })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- receiver.Serve(ctx) }()
	client, err := net.DialUDP("udp", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, data := range [][]byte{fixture(t, "udp-cafe"), fixture(t, "udp-cafm-1"), fixture(t, "udp-cafm-0"), []byte("bad!")} {
		if _, err = client.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case p := <-packets:
			if !p.Sender.IsValid() {
				t.Fatal("sender")
			}
		case <-time.After(time.Second):
			t.Fatal("packet timeout")
		}
	}
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("malformed not reported")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked")
	}
	if receiver.Stats().Packets != 2 || receiver.Stats().Malformed == 0 {
		t.Fatal(receiver.Stats())
	}
	if _, err = conn.WriteToUDP([]byte{1}, client.LocalAddr().(*net.UDPAddr)); !errors.Is(err, net.ErrClosed) {
		t.Fatal("socket ownership", err)
	}
}

func TestUDPQueueAndHandlerCancellation(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	var once sync.Once
	r, err := NewUDPReceiver(conn, UDPConfig{Workers: 1, QueueCapacity: 1}, func(ctx context.Context, p Packet) error {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return ctx.Err()
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Serve(ctx) }()
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
	for i := 0; i < 20; i++ {
		client.Write(raw)
	}
	deadline := time.Now().Add(time.Second)
	for r.Stats().Rejected == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if r.Stats().Rejected == 0 {
		t.Fatal("queue never rejected")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler shutdown")
	}
	// Every received single-Pack datagram is exactly one of: handled, failed in
	// the handler, rejected (queue full) or discarded from the queue at shutdown.
	st := r.Stats()
	if st.ShutdownDiscarded == 0 || st.Datagrams != st.Packets+st.HandlerErrors+st.Rejected+st.ShutdownDiscarded {
		t.Fatalf("%+v", st)
	}
}

func FuzzUDPDatagram(f *testing.F) {
	f.Add([]byte("CAFE"))
	f.Add([]byte("CAFM"))
	f.Add([]byte{0, 1, 2})
	for _, name := range []string{"udp-cafe", "udp-cafn", "udp-cafm-0", "udp-cafm-1", "udp-jmtu-0"} {
		f.Add(fixture(f, name))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		a, err := NewReassembler(UDPConfig{MaxDatagramBytes: 1024, MaxAssemblyBytes: 1024, MaxBufferedBytes: 2048, MaxAssemblies: 2, MaxFragments: 8, MaxBatchPackets: 8, CodecLimits: protocol.Limits{MaxBytes: 2048, MaxBlobBytes: 1024, MaxCollectionEntries: 16, MaxTotalValues: 64}})
		if err != nil {
			t.Fatal(err)
		}
		a.Decode(netip.MustParseAddrPort("127.0.0.1:1"), data, time.Now())
		if a.BufferedBytes() > 2048 || a.Pending() > 2 {
			t.Fatal("unbounded state")
		}
	})
}

func TestUDPConfigAndConcurrentReassembly(t *testing.T) {
	if _, err := NewReassembler(UDPConfig{Workers: -1}); err == nil {
		t.Fatal("invalid config")
	}
	a, _ := NewReassembler(UDPConfig{})
	var wg sync.WaitGroup
	for i := 1; i <= 20; i++ {
		wg.Add(1)
		go func(port uint16) {
			defer wg.Done()
			sender := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port)
			a.Decode(sender, fixture(t, "udp-cafm-0"), time.Now())
			a.Decode(sender, fixture(t, "udp-cafm-1"), time.Now())
		}(uint16(i))
	}
	wg.Wait()
	if a.Pending() != 0 {
		t.Fatal("assemblies retained")
	}
}

func TestUDPIdleExpiryAndOversizedSocketDatagram(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan ErrorEvent, 16)
	receiver, err := NewUDPReceiver(conn, UDPConfig{FragmentTTL: 10 * time.Millisecond, MaxDatagramBytes: 64}, func(context.Context, Packet) error { t.Error("unexpected complete packet"); return nil }, func(e ErrorEvent) { events <- e })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- receiver.Serve(ctx) }()
	client, err := net.DialUDP("udp", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.Write(fixture(t, "udp-cafm-0"))
	select {
	case e := <-events:
		if e.Operation != "udp.expired" {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("idle expiry not reported")
	}
	if receiver.Stats().Expired != 1 {
		t.Fatal(receiver.Stats())
	}
	client.Write(make([]byte, 1024))
	select {
	case e := <-events:
		if e.Operation != "udp.limit" || !errors.Is(e.Err, ErrLimit) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("oversize not reported")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop")
	}
	if receiver.Stats().Rejected != 1 {
		t.Fatal(receiver.Stats())
	}
}

func TestUDPExpiredSameKeyAndBatchBounds(t *testing.T) {
	a, _ := NewReassembler(UDPConfig{FragmentTTL: time.Millisecond, MaxBatchPackets: 1})
	sender := netip.MustParseAddrPort("127.0.0.1:1")
	now := time.Now()
	a.Decode(sender, fixture(t, "udp-cafm-0"), now)
	if _, err := a.Decode(sender, fixture(t, "udp-cafm-1"), now.Add(time.Millisecond)); !errors.Is(err, ErrExpired) {
		t.Fatal("expired key accepted", err)
	}
	if a.Pending() != 0 || a.BufferedBytes() != 0 {
		t.Fatal("expired key retained")
	}
	if _, err := a.Decode(sender, fixture(t, "udp-cafn"), now); !errors.Is(err, ErrLimit) {
		t.Fatal("batch bound", err)
	}
}
