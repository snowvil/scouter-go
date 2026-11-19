package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// Relay tests: Agent Blob streams are relayed chunk by chunk with a
// per-chunk limit only, large single-Pack answers (OBJECT_DOWNLOAD_JAR) have a
// command limit above the ordinary 8MiB, and a Client-side failure closes the
// agent session like Java TcpAgentWorker.read.

var relayCodec = protocol.Limits{MaxBytes: 64 << 20, MaxBlobBytes: 64 << 20}

func newRelayHarness(t *testing.T, soTimeout time.Duration) *poolTCPHarness {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &agentPool{agents: make(map[int32][]*agentSlot)}
	server, err := transport.NewTCPServer(listener, transport.TCPConfig{RequestTimeout: 3 * time.Second}, transport.TCPHandlers{Agent: p.handle}, nil)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	if err := server.SetAgentSoTimeout(soTimeout); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("relay handler leaked during shutdown")
		}
	})
	return &poolTCPHarness{pool: p, address: listener.Addr().String()}
}

// dialV2 registers a TCP_AGENT_V2 fake agent.
func (h *poolTCPHarness) dialV2(t *testing.T, hash int32) *poolFakeAgent {
	t.Helper()
	conn, err := net.DialTimeout("tcp", h.address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], transport.TCPAgentV2)
	binary.BigEndian.PutUint32(header[4:], uint32(hash))
	if _, err = conn.Write(header[:]); err != nil {
		t.Fatal(err)
	}
	h.wait(t, func(list []*agentSlot) bool {
		for _, s := range list {
			if s.conn.Peer.String() == conn.LocalAddr().String() {
				return true
			}
		}
		return false
	}, hash)
	return &poolFakeAgent{conn: conn, reader: bufio.NewReader(conn)}
}

// readRequestV2 reads one V2 command (IntBytes of Text + MapPack).
func (a *poolFakeAgent) readRequestV2(t *testing.T) string {
	t.Helper()
	a.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var n [4]byte
	if _, err := io.ReadFull(a.reader, n[:]); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, binary.BigEndian.Uint32(n[:]))
	if _, err := io.ReadFull(a.reader, body); err != nil {
		t.Fatal(err)
	}
	d := protocol.NewDecoder(body, protocol.Limits{})
	cmd := d.Text()
	d.Pack()
	if err := d.Err(); err != nil {
		t.Fatal(err)
	}
	return cmd
}

func encodeBlobItem(t *testing.T, data []byte) []byte {
	t.Helper()
	e := protocol.NewEncoder(relayCodec)
	e.Byte(transport.HasNext)
	e.Blob(data)
	b, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func chunkData(i, size int) []byte {
	b := make([]byte, size)
	for j := range b {
		b[j] = byte(i*31 + j)
	}
	return b
}

// A 48MiB heap dump in 2MiB chunks (AgentHeapDump.downloadHeapDump) is
// relayed byte for byte, each chunk reaching the Client side before the agent
// sends the next one (so the relay never holds the dump), and the heap stays
// at chunk scale. Before the fix requestBlob kept every chunk until the end
// and failed at 8MiB with ErrLimit.
func TestAgentBlobRelayStreamsChunks(t *testing.T) {
	h := newRelayHarness(t, 2*time.Second)
	peer := h.dial(t, 42)
	const chunks, size = 24, 2 << 20
	received := make(chan int, 1)
	// Hashes instead of copies: the test itself holds no dump either.
	got, want := sha256.New(), sha256.New()
	var peak atomic.Uint64
	var stats runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&stats)
	base := stats.HeapAlloc
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		done <- h.pool.requestBlob(ctx, 42, "OBJECT_DOWNLOAD_HEAP_DUMP", &protocol.MapPack{}, func(chunk []byte) error {
			got.Write(chunk)
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > peak.Load() {
				peak.Store(m.HeapAlloc)
			}
			received <- len(chunk)
			return nil
		})
	}()
	if cmd, _ := peer.readRequest(t); cmd != "OBJECT_DOWNLOAD_HEAP_DUMP" {
		t.Fatal(cmd)
	}
	for i := 0; i < chunks; i++ {
		data := chunkData(i, size)
		want.Write(data)
		if _, err := peer.conn.Write(encodeBlobItem(t, data)); err != nil {
			t.Fatal(err)
		}
		// Lockstep: the next chunk is sent only after this one was emitted.
		select {
		case n := <-received:
			if n != size {
				t.Fatalf("chunk %d relayed as %d bytes", i, n)
			}
		case err := <-done:
			t.Fatalf("relay ended at chunk %d: %v", i, err)
		case <-time.After(5 * time.Second):
			t.Fatalf("chunk %d not relayed before the next one (buffered relay)", i)
		}
		runtime.GC() // collect the test's own per-chunk garbage
	}
	peer.conn.Write([]byte{transport.NoNext})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Sum(nil), want.Sum(nil)) {
		t.Fatal("relayed bytes differ")
	}
	// 48MiB went through; the heap held only a few 2MiB chunks at a time.
	if limit := base + 24<<20; peak.Load() > limit {
		t.Fatalf("heap peak %dMiB over base %dMiB+24MiB: relay buffered chunks", peak.Load()>>20, base>>20)
	}
	t.Logf("48MiB relayed; heap peak %dMiB, base %dMiB", peak.Load()>>20, base>>20)
	// The session is reused afterwards (protocol state intact).
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 1 && !list[0].busy }, 42)
}

// A slow Client: each emit takes longer than the agent so-timeout. The agent
// is backpressured (its writes wait for the server to read); the per-read
// so-timeout only counts waiting for agent input, so the relay completes.
func TestAgentBlobRelaySlowClient(t *testing.T) {
	h := newRelayHarness(t, 100*time.Millisecond)
	peer := h.dial(t, 42)
	const chunks, size = 6, 2 << 20
	var got bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- h.pool.requestBlob(context.Background(), 42, "OBJECT_DOWNLOAD_HEAP_DUMP", &protocol.MapPack{}, func(chunk []byte) error {
			time.Sleep(150 * time.Millisecond)
			got.Write(chunk)
			return nil
		})
	}()
	peer.readRequest(t)
	var want bytes.Buffer
	writeErr := make(chan error, 1)
	go func() {
		for i := 0; i < chunks; i++ {
			data := chunkData(i, size)
			want.Write(data)
			if _, err := peer.conn.Write(encodeBlobItem(t, data)); err != nil {
				writeErr <- err
				return
			}
		}
		_, err := peer.conn.Write([]byte{transport.NoNext})
		writeErr <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("slow Client broke the relay", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("slow relay stuck")
	}
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want.Bytes()) {
		t.Fatalf("relayed %d bytes, want %d", got.Len(), want.Len())
	}
}

// The Client goes away mid-stream: the emit error ends the relay and closes
// the agent session (Java TcpAgentWorker.read closes the socket when the
// handler throws), so the unread rest of the stream never reaches a later
// command. The pool drops the session.
func TestAgentBlobRelayClientGoneClosesAgentSession(t *testing.T) {
	h := newRelayHarness(t, 2*time.Second)
	peer := h.dial(t, 42)
	gone := errors.New("synthetic client write failure")
	count := 0
	done := make(chan error, 1)
	go func() {
		done <- h.pool.requestBlob(context.Background(), 42, "OBJECT_DOWNLOAD_HEAP_DUMP", &protocol.MapPack{}, func([]byte) error {
			if count++; count == 2 {
				return gone
			}
			return nil
		})
	}()
	peer.readRequest(t)
	go func() {
		for i := 0; i < 5; i++ {
			if _, err := peer.conn.Write(encodeBlobItem(t, chunkData(i, 64<<10))); err != nil {
				return
			}
		}
	}()
	select {
	case err := <-done:
		if !errors.Is(err, gone) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client failure did not end the relay")
	}
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 0 }, 42)
}

// A chunk above the per-chunk limit is refused before it is read, and the
// agent session is closed (the stream cannot be resynchronized).
func TestAgentBlobRelayChunkLimit(t *testing.T) {
	h := newRelayHarness(t, 2*time.Second)
	peer := h.dial(t, 42)
	done := make(chan error, 1)
	go func() {
		done <- h.pool.requestBlob(context.Background(), 42, "OBJECT_DOWNLOAD_HEAP_DUMP", &protocol.MapPack{}, func([]byte) error { return nil })
	}()
	peer.readRequest(t)
	// Only the length prefix of a chunk over agentBlobChunkBytes.
	var header [5]byte
	header[0] = 254
	binary.BigEndian.PutUint32(header[1:], agentBlobChunkBytes+1)
	peer.conn.Write(append([]byte{transport.HasNext}, header[:]...))
	select {
	case err := <-done:
		if !errors.Is(err, transport.ErrLimit) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("oversized chunk not refused")
	}
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 0 }, 42)
}

// M1: AgentClassHandle.downloadJar answers one MapPack holding the whole jar
// as a BlobValue. A 20MiB jar over V1 and V2 is relayed intact; before the fix
// the V2 decode used the 4MiB codec Blob limit and the relay an 8MiB total.
// An ordinary relay keeps the 8MiB limit.
func TestAgentRelayLargeJarPack(t *testing.T) {
	h := newRelayHarness(t, 2*time.Second)
	jar := chunkData(7, 20<<20)
	pack := &protocol.MapPack{Entries: []protocol.Entry{{Key: "jar", Value: protocol.Value{Type: protocol.ValueBlob, Bytes: jar}}}}
	body, err := protocol.EncodePack(pack, relayCodec)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		version int
		cmd     string
		ok      bool
	}{{1, "OBJECT_DOWNLOAD_JAR", true}, {2, "OBJECT_DOWNLOAD_JAR", true}, {2, "OBJECT_ENV", false}, {1, "OBJECT_ENV", false}} {
		hash := int32(100 + c.version*10 + len(c.cmd))
		var peer *poolFakeAgent
		if c.version == 1 {
			peer = h.dial(t, hash)
		} else {
			peer = h.dialV2(t, hash)
		}
		done := make(chan poolRequestResult, 1)
		go func() {
			packs, err := h.pool.request(context.Background(), hash, c.cmd, &protocol.MapPack{})
			done <- poolRequestResult{packs, err}
		}()
		var cmd string
		response := []byte{transport.HasNext}
		if c.version == 1 {
			cmd, _ = peer.readRequest(t)
		} else {
			cmd = peer.readRequestV2(t)
			response = binary.BigEndian.AppendUint32(response, uint32(len(body)))
		}
		if cmd != c.cmd {
			t.Fatal(cmd)
		}
		response = append(append(response, body...), transport.NoNext)
		go peer.conn.Write(response)
		var r poolRequestResult
		select {
		case r = <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("jar relay stuck")
		}
		if !c.ok {
			if !errors.Is(r.err, transport.ErrLimit) && !errors.Is(r.err, protocol.ErrLimit) {
				t.Fatalf("V%d %s over 8MiB: %v", c.version, c.cmd, r.err)
			}
			h.wait(t, func(list []*agentSlot) bool { return len(list) == 0 }, hash)
			continue
		}
		if r.err != nil {
			t.Fatalf("V%d jar relay: %v", c.version, r.err)
		}
		m, _ := r.packs[0].(*protocol.MapPack)
		if len(r.packs) != 1 || m == nil || !bytes.Equal(m.Entries[0].Value.Bytes, jar) {
			t.Fatalf("V%d jar not intact", c.version)
		}
		// The session stays usable after a large answer.
		h.wait(t, func(list []*agentSlot) bool { return len(list) == 1 && !list[0].busy }, hash)
	}
}

// The relay limits per command stay bounded (AGENTS.md) while allowing the
// Java payloads named in the comments.
func TestAgentRelayLimitTable(t *testing.T) {
	for cmd := range largeAgentPackCommands {
		l := agentPackLimits(cmd)
		if l.ItemBytes != 256<<20 || l.TotalBytes != 256<<20 || l.Transfer <= 0 {
			t.Fatal(cmd, l)
		}
	}
	if l := agentPackLimits("OBJECT_ENV"); l.ItemBytes != 8<<20 || l.TotalBytes != 8<<20 || l.Transfer != 0 {
		t.Fatal(l)
	}
	if agentBlobLimits.ItemBytes != 16<<20 || agentBlobLimits.TotalBytes >= 0 || agentBlobLimits.Transfer >= 0 {
		t.Fatal(agentBlobLimits)
	}
}
