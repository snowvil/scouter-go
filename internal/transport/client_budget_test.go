package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"
)

// budgetServer serves Client commands that each answer the Blob items of the
// next size list taken from sizes (a negative size is a relayed Blob of -size
// bytes, ResponseRelayBlob), and reports each handler result on results.
func budgetServer(t *testing.T, config TCPConfig, soTimeout time.Duration) (*TCPServer, net.Listener, chan<- []int, <-chan error, func() []ErrorEvent) {
	t.Helper()
	sizes := make(chan []int, 8)
	results := make(chan error, 8)
	server, listener, _, _, events := startTCPEvents(t, config, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if _, err := s.ReadPack(); err != nil {
			return err
		}
		var err error
		for _, n := range <-sizes {
			if n < 0 {
				err = s.ResponseRelayBlob(bytes.Repeat([]byte{7}, -n))
			} else {
				err = s.ResponseBlob(bytes.Repeat([]byte{7}, n))
			}
			if err != nil {
				break
			}
		}
		results <- err
		return err
	}})
	if err := server.SetClientSoTimeout(soTimeout); err != nil {
		t.Fatal(err)
	}
	return server, listener, sizes, results, events
}

// waitStats polls Stats until ok holds or fails the test after 3 seconds.
func waitStats(t *testing.T, server *TCPServer, what string, ok func(TCPStats) bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); !ok(server.Stats()); {
		if time.Now().After(deadline) {
			t.Fatalf("%s: %+v", what, server.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// R2: encoded Client response items of all connections share one memory
// budget. A Client that does not read holds its item; a second large item
// waits up to the wait limit and then fails its command (the connection
// closes without NoNEXT, as for any other response limit), while a small item
// is not affected. The held item still reaches its Client when it reads.
func TestClientResponseBudgetRefusesWhenFull(t *testing.T) {
	const held = 32 << 20 // more than loopback socket buffers absorb
	server, listener, sizes, results, events := budgetServer(t, TCPConfig{RequestTimeout: 5 * time.Second, MaxClientResponseBytes: held + 1<<20}, 300*time.Millisecond)
	lines := make(chan string, 8)
	server.SetLogFunc(func(id, message string) { lines <- message })
	holder := dialTCP(t, listener)
	holder.SetDeadline(time.Now().Add(20 * time.Second))
	sizes <- []int{held}
	clientLogin(t, holder) // does not read yet
	waitStats(t, server, "budget not held by the blocked item", func(st TCPStats) bool { return st.ClientResponseBytes >= held })

	refused := dialTCP(t, listener)
	sizes <- []int{2 << 20}
	start := time.Now()
	clientLogin(t, refused)
	refused.SetReadDeadline(time.Now().Add(3 * time.Second))
	raw, err := io.ReadAll(refused)
	if len(raw) != 0 || err != nil {
		t.Fatalf("refused command answered %d bytes, err=%v", len(raw), err)
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Fatalf("refused after %v, before the wait limit", elapsed)
	}
	if err := <-results; !errors.Is(err, ErrLimit) {
		t.Fatalf("refused item: %v", err)
	}
	if line := <-lines; !strings.HasPrefix(line, "Client : /127.0.0.1:") || !strings.HasSuffix(line, " response of 2097157 bytes refused, response memory=33554437/34603008 (Go Client response memory budget)") {
		t.Fatal(line)
	}

	small := dialTCP(t, listener)
	sizes <- []int{1 << 10}
	clientLogin(t, small)
	if items, total := readBlobResponse(t, small); items != 1 || total != 1<<10 {
		t.Fatalf("small item while full: %d/%d", items, total)
	}
	if err := <-results; err != nil {
		t.Fatal(err)
	}

	if items, total := readBlobResponse(t, holder); items != 1 || total != held {
		t.Fatalf("held item: %d/%d", items, total)
	}
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	waitStats(t, server, "budget not returned", func(st TCPStats) bool { return st.ClientResponseBytes == 0 })
	st := server.Stats()
	if st.ClientResponseWaits != 1 || st.ClientResponseRejected != 1 || st.ClientResponseWaiting != 0 ||
		st.ClientResponsePeakBytes < held || st.MaxClientResponseBytes != held+1<<20 {
		t.Fatalf("stats %+v", st)
	}
	found := false
	for _, e := range events() {
		found = found || (e.Operation == "tcp.connection" && errors.Is(e.Err, ErrLimit))
	}
	if !found {
		t.Fatalf("refusal not reported: %+v", events())
	}
}

// R2: a waiting item goes out as soon as the budget has room for it.
func TestClientResponseBudgetAdmitsWaiter(t *testing.T) {
	const held = 32 << 20
	server, listener, sizes, results, _ := budgetServer(t, TCPConfig{RequestTimeout: 5 * time.Second, MaxClientResponseBytes: held + 1<<20}, 3*time.Second)
	holder := dialTCP(t, listener)
	holder.SetDeadline(time.Now().Add(20 * time.Second))
	sizes <- []int{held}
	clientLogin(t, holder)
	waitStats(t, server, "budget not held by the blocked item", func(st TCPStats) bool { return st.ClientResponseBytes >= held })

	waiter := dialTCP(t, listener)
	waiter.SetDeadline(time.Now().Add(10 * time.Second))
	sizes <- []int{2 << 20}
	clientLogin(t, waiter)
	waitStats(t, server, "item not waiting", func(st TCPStats) bool { return st.ClientResponseWaiting == 1 })

	if items, total := readBlobResponse(t, holder); items != 1 || total != held {
		t.Fatalf("held item: %d/%d", items, total)
	}
	if items, total := readBlobResponse(t, waiter); items != 1 || total != 2<<20 {
		t.Fatalf("waiting item: %d/%d", items, total)
	}
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	waitStats(t, server, "budget not returned", func(st TCPStats) bool { return st.ClientResponseBytes == 0 })
	if st := server.Stats(); st.ClientResponseWaits != 1 || st.ClientResponseRejected != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestResponseBudgetRules(t *testing.T) {
	never := make(chan struct{})
	b := newResponseBudget(100)
	// An item above the limit goes out alone.
	if err := b.acquire(150, 0, never); err != nil {
		t.Fatal(err)
	}
	// Waiters hold at most the limit: the first may wait, a second that
	// would pass the limit together with it is refused at once.
	waited := make(chan error, 1)
	go func() { waited <- b.acquire(80, time.Minute, never) }()
	for _, _, _, waiting, _, _ := b.stats(); waiting != 1; _, _, _, waiting, _, _ = b.stats() {
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	if err := b.acquire(30, time.Minute, never); !errors.Is(err, ErrResponseBudget) || time.Since(start) > time.Second {
		t.Fatal("waiting set beyond the limit", err)
	}
	b.release(150)
	if err := <-waited; err != nil {
		t.Fatal(err)
	}
	// First fit: a small item passes a large waiter.
	large := make(chan error, 1)
	go func() { large <- b.acquire(90, time.Minute, never) }()
	for _, _, _, waiting, _, _ := b.stats(); waiting != 1; _, _, _, waiting, _, _ = b.stats() {
		time.Sleep(time.Millisecond)
	}
	if err := b.acquire(20, 0, never); err != nil {
		t.Fatal("small item behind a large waiter", err)
	}
	b.release(80)
	b.release(20)
	if err := <-large; err != nil {
		t.Fatal(err)
	}
	// The wait limit and the server's end both refuse a waiter.
	if err := b.acquire(50, 10*time.Millisecond, never); !errors.Is(err, ErrResponseBudget) {
		t.Fatal(err)
	}
	done := make(chan struct{})
	close(done)
	if err := b.acquire(50, time.Minute, done); !errors.Is(err, ErrResponseBudget) {
		t.Fatal(err)
	}
	b.release(90)
	used, peak, limit, waiting, waits, rejected := b.stats()
	if used != 0 || peak != 150 || limit != 100 || waiting != 0 || waits != 4 || rejected != 3 {
		t.Fatal(used, peak, limit, waiting, waits, rejected)
	}
}

// Items below clientBudgetMinItem never take the budget.
func TestClientBudgetSmallItemsBypass(t *testing.T) {
	shared := newResponseBudget(1)
	shared.take(1)
	c := &clientBudget{shared: shared, wait: time.Minute}
	release, err := c.hold(clientBudgetMinItem-1, nil)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if used, _, _, _, _, _ := shared.stats(); used != 1 {
		t.Fatal(used)
	}
	var none *clientBudget
	if _, err := none.hold(1<<20, nil); err != nil {
		t.Fatal(err)
	}
}

func TestClientResponseBudgetConfig(t *testing.T) {
	if _, err := (TCPConfig{MaxClientResponseBytes: -1}).normalized(); !errors.Is(err, ErrMalformed) {
		t.Fatal(err)
	}
	c, err := TCPConfig{}.normalized()
	if err != nil || c.MaxClientResponseBytes != DefaultClientResponseBytes() {
		t.Fatal(c.MaxClientResponseBytes, err)
	}
	old := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(old) })
	debug.SetMemoryLimit(math.MaxInt64)
	if got := DefaultClientResponseBytes(); got != DefaultClientResponseBudget {
		t.Fatal(got)
	}
	debug.SetMemoryLimit(8 << 30)
	if got := DefaultClientResponseBytes(); got != 2<<30 {
		t.Fatal(got)
	}
	debug.SetMemoryLimit(256 << 20) // a quarter would not hold one largest item
	if got := DefaultClientResponseBytes(); got != int64(clientPendingLimit) {
		t.Fatal(got)
	}
}

// holdBudget makes a Client that does not read hold a 32MiB item of the
// budget and returns its connection.
func holdBudget(t *testing.T, server *TCPServer, listener net.Listener, sizes chan<- []int) net.Conn {
	t.Helper()
	const held = 32 << 20
	holder := dialTCP(t, listener)
	holder.SetDeadline(time.Now().Add(20 * time.Second))
	sizes <- []int{held}
	clientLogin(t, holder)
	waitStats(t, server, "budget not held by the blocked item", func(st TCPStats) bool { return st.ClientResponseBytes >= held })
	return holder
}

// Review finding: Blob chunks of an Agent relay are counted by the relay's own
// permit, so a full Client response budget neither delays nor refuses them
// (a refusal would close the Agent session).
func TestClientResponseBudgetSkipsRelayBlob(t *testing.T) {
	server, listener, sizes, results, _ := budgetServer(t, TCPConfig{RequestTimeout: 5 * time.Second, MaxClientResponseBytes: 33 << 20}, 3*time.Second)
	holder := holdBudget(t, server, listener, sizes)
	relay := dialTCP(t, listener)
	sizes <- []int{-(2 << 20), -(2 << 20)}
	start := time.Now()
	clientLogin(t, relay)
	if items, total := readBlobResponse(t, relay); items != 2 || total != 4<<20 {
		t.Fatalf("relayed chunks: %d/%d", items, total)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("relayed chunks waited %v", elapsed)
	}
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	if st := server.Stats(); st.ClientResponseWaits != 0 || st.ClientResponseRejected != 0 {
		t.Fatalf("stats %+v", st)
	}
	holder.Close()
}

// A refused second item: the Client gets the first item, then EOF without
// NoNEXT (Java TcpProxy returns the items read so far).
func TestClientResponseBudgetRefusesLaterItem(t *testing.T) {
	server, listener, sizes, results, _ := budgetServer(t, TCPConfig{RequestTimeout: 5 * time.Second, MaxClientResponseBytes: 33 << 20}, 200*time.Millisecond)
	holdBudget(t, server, listener, sizes)
	conn := dialTCP(t, listener)
	sizes <- []int{1 << 10, 2 << 20}
	clientLogin(t, conn)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	raw, err := io.ReadAll(conn)
	want := append([]byte{HasNext, 255, 4, 0}, bytes.Repeat([]byte{7}, 1<<10)...)
	if err != nil || !bytes.Equal(raw, want) {
		t.Fatalf("read %d bytes (want %d, first item only), err=%v", len(raw), len(want), err)
	}
	if err := <-results; !errors.Is(err, ErrResponseBudget) {
		t.Fatal(err)
	}
}

// The budget comes back when the holder's write fails (the Client left), and
// a waiter ends at once when the server closes.
func TestClientResponseBudgetReleaseOnFailureAndClose(t *testing.T) {
	server, listener, sizes, results, _ := budgetServer(t, TCPConfig{RequestTimeout: 5 * time.Second, MaxClientResponseBytes: 33 << 20}, 5*time.Second)
	holder := holdBudget(t, server, listener, sizes)
	holder.Close()
	if err := <-results; err == nil {
		t.Fatal("write to a closed Client succeeded")
	}
	waitStats(t, server, "budget kept after a failed write", func(st TCPStats) bool { return st.ClientResponseBytes == 0 })

	holdBudget(t, server, listener, sizes)
	waiter := dialTCP(t, listener)
	sizes <- []int{2 << 20}
	clientLogin(t, waiter)
	waitStats(t, server, "item not waiting", func(st TCPStats) bool { return st.ClientResponseWaiting == 1 })
	start := time.Now()
	server.Close()
	for i := 0; i < 2; i++ {
		select {
		case <-results:
		case <-time.After(2 * time.Second):
			t.Fatal("handler still blocked after Close")
		}
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("waiter ended %v after Close", elapsed)
	}
	if st := server.Stats(); st.ClientResponseWaiting != 0 || st.ClientResponseBytes != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestResponseBudgetStress(t *testing.T) {
	b := newResponseBudget(1000)
	never := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				n := int64(50 + (g*37+i*11)%900)
				if err := b.acquire(n, time.Duration(i%3)*time.Millisecond, never); err == nil {
					b.release(n)
				}
			}
		}(g)
	}
	wg.Wait()
	used, peak, limit, waiting, waits, rejected := b.stats()
	if used != 0 || waiting != 0 || peak > limit || b.waiting != 0 || len(b.waiters) != 0 {
		t.Fatal(used, peak, waiting, waits, rejected)
	}
}
