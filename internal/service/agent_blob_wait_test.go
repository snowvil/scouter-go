package service

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// A Blob stream relay (OBJECT_DOWNLOAD_HEAP_DUMP,
// OBJECT_DUMP_FILE_DETAIL) waits for the Agent's next chunk past the Client
// command progress limit, like Java's HeapDumpService/DumpService handler,
// which only has the Agent so_timeout per read. The Client writes in between
// keep their own per-chunk write deadline, and a Client that leaves ends the
// relay at once.

// delayedBlobFake emits each chunk after its delay, or ends with the request
// context; a chunk delay of -1 waits until the context ends.
type delayedBlobFake struct {
	*fakeBackend
	chunks  [][]byte
	delays  []time.Duration
	started chan int // chunk index whose wait began
	mu      sync.Mutex
	end     error
	ended   chan struct{}
}

func (b *delayedBlobFake) AgentBlobRequest(ctx context.Context, _ int32, _ string, _ *protocol.MapPack, emit func([]byte) error) (err error) {
	defer func() {
		b.mu.Lock()
		b.end = err
		b.mu.Unlock()
		close(b.ended)
	}()
	for i, chunk := range b.chunks {
		if b.started != nil {
			b.started <- i
		}
		wait := make(<-chan time.Time)
		if b.delays[i] >= 0 {
			wait = time.After(b.delays[i])
		}
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := emit(chunk); err != nil {
			return err
		}
	}
	return nil
}

func (b *delayedBlobFake) result() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.end
}

// The first chunk and the gap between chunks each take four times the Client
// progress limit; the Client still gets every chunk and NoNEXT, and the
// connection serves the next command.
func TestAgentBlobRelayWaitOutlivesProgressLimit(t *testing.T) {
	const limit = 100 * time.Millisecond
	for _, command := range []string{"OBJECT_DOWNLOAD_HEAP_DUMP", "OBJECT_DUMP_FILE_DETAIL"} {
		t.Run(command, func(t *testing.T) {
			b := &delayedBlobFake{fakeBackend: &fakeBackend{}, ended: make(chan struct{}),
				chunks: [][]byte{{1, 2, 3}, bytes.Repeat([]byte{4}, 4096), {5}},
				delays: []time.Duration{4 * limit, 4 * limit, 0}}
			conn, r := startServiceProgress(t, b, limit)
			session := loginSession(t, conn, r)
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			send(t, conn, command, session, newMap(entry("objHash", decimal(42))))
			for i, want := range b.chunks {
				aggregationFlag(t, r, transport.HasNext)
				if got := readBlob(t, r); !bytes.Equal(got, want) {
					t.Fatalf("chunk %d: %x", i, got)
				}
			}
			aggregationFlag(t, r, transport.NoNext)
			<-b.ended
			if err := b.result(); err != nil {
				t.Fatal("blob relay cut:", err)
			}
			send(t, conn, "SERVER_TIME", session, nil)
			if len(readPacks(t, r)) != 1 {
				t.Fatal("stream desynchronized")
			}
		})
	}
}

// A Client that closes its connection while the relay waits for the next
// chunk ends the relay context at once with ErrClientGone (the server then
// closes the Agent session; Blob streams are never orphaned).
func TestAgentBlobRelayWaitEndsWhenClientLeaves(t *testing.T) {
	b := &delayedBlobFake{fakeBackend: &fakeBackend{}, ended: make(chan struct{}), started: make(chan int, 2),
		chunks: [][]byte{{1}, {2}}, delays: []time.Duration{0, -1}}
	conn, r := startServiceProgress(t, b, 10*time.Second)
	session := loginSession(t, conn, r)
	send(t, conn, "OBJECT_DOWNLOAD_HEAP_DUMP", session, newMap(entry("objHash", decimal(42))))
	// The first chunk was written (it may still sit in the write buffer, as
	// in Java's BufferedOutputStream) before the wait for the second began.
	for i := range 2 {
		if got := <-b.started; got != i {
			t.Fatal("chunk wait", got)
		}
	}
	left := time.Now()
	conn.Close()
	select {
	case <-b.ended:
	case <-time.After(3 * time.Second):
		t.Fatal("Client exit not noticed while waiting for the next chunk")
	}
	if elapsed := time.Since(left); elapsed > time.Second {
		t.Fatalf("Client exit noticed after %v", elapsed)
	}
	if err := b.result(); !errors.Is(err, transport.ErrClientGone) {
		t.Fatal(err)
	}
}

// A Client that stops reading still ends a Blob relay: each socket write
// chunk keeps its RequestTimeout deadline while the progress limit is held.
func TestAgentBlobRelayStalledClientEndsRelay(t *testing.T) {
	const limit = 200 * time.Millisecond
	b := &delayedBlobFake{fakeBackend: &fakeBackend{}, ended: make(chan struct{})}
	// Far more than the loopback socket buffers can absorb.
	for range 64 {
		b.chunks = append(b.chunks, bytes.Repeat([]byte{7}, 2<<20))
		b.delays = append(b.delays, 0)
	}
	conn, r := startServiceProgress(t, b, limit)
	session := loginSession(t, conn, r)
	sent := time.Now()
	send(t, conn, "OBJECT_DOWNLOAD_HEAP_DUMP", session, newMap(entry("objHash", decimal(42))))
	select {
	case <-b.ended:
	case <-time.After(10 * time.Second):
		t.Fatal("a Client that reads nothing kept the Blob relay running")
	}
	// The write deadline, not the (held) progress limit or the Client's
	// connection end, ended it: soon after the socket buffers filled.
	if err := b.result(); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal("stalled Client relay end:", err)
	}
	if elapsed := time.Since(sent); elapsed > 3*time.Second {
		t.Fatalf("stalled Client relay ended after %v", elapsed)
	}
}

// throttledReader is a Client reading at most 64KiB every interval. A 64KiB
// socket write completes only once the receiver's window opens again, which
// on macOS loopback took a few hundred KiB of reads (observed: 64KiB every
// quarter of the write deadline missed it), so the test Client reads about
// 1.5MiB per write deadline.
type throttledReader struct {
	r        io.Reader
	interval time.Duration
}

func (r *throttledReader) Read(p []byte) (int, error) {
	time.Sleep(r.interval)
	if len(p) > 64<<10 {
		p = p[:64<<10]
	}
	return r.r.Read(p)
}

// A slow Client that keeps reading 64KiB pieces receives a chunk whose
// transfer takes many times the progress limit: progress is only recorded
// once a whole item is written, so the held limit, not the item, must bound
// the write; each 64KiB piece keeps its own write deadline.
func TestAgentBlobRelaySlowReadingClientGetsLargeChunk(t *testing.T) {
	const limit = 300 * time.Millisecond
	chunk := bytes.Repeat([]byte{9}, 8<<20)
	b := &delayedBlobFake{fakeBackend: &fakeBackend{}, ended: make(chan struct{}),
		chunks: [][]byte{chunk, {1}}, delays: []time.Duration{0, 0}}
	conn, r := startServiceProgress(t, b, limit)
	session := loginSession(t, conn, r)
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	send(t, conn, "OBJECT_DOWNLOAD_HEAP_DUMP", session, newMap(entry("objHash", decimal(42))))
	started := time.Now()
	slow := bufio.NewReaderSize(&throttledReader{r: r, interval: limit / 30}, 16)
	for i, want := range b.chunks {
		aggregationFlag(t, slow, transport.HasNext)
		if got := readBlob(t, slow); !bytes.Equal(got, want) {
			t.Fatalf("chunk %d: %d bytes", i, len(got))
		}
	}
	aggregationFlag(t, slow, transport.NoNext)
	if elapsed := time.Since(started); elapsed < 4*limit {
		t.Fatalf("transfer took only %v: the Client was not slow enough to test the limit", elapsed)
	}
	<-b.ended
	if err := b.result(); err != nil {
		t.Fatal(err)
	}
}
