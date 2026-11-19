package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// Large responses. The Client stream limits are set here to what the
// Client response path allows (transport.MaxResponseValueBytes, no 8MiB
// command total), so these tests check only the service side.
var largeClientCodec = protocol.Limits{MaxBytes: 64 << 20, MaxBlobBytes: 64 << 20}

func startLargeService(t *testing.T, backend Backend) (net.Conn, *bufio.Reader) {
	t.Helper()
	service, err := New(queryConfig(), backend)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := transport.NewTCPServer(listener, transport.TCPConfig{MaxFrameBytes: 64 << 20, CodecLimits: largeClientCodec}, service.Handlers(), nil)
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
	conn.SetDeadline(time.Now().Add(20 * time.Second))
	var magic [4]byte
	binary.BigEndian.PutUint32(magic[:], transport.TCPClient)
	conn.Write(magic[:])
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

// Java XLogProfileRD.getProfile concatenates every profile block with no size
// limit. A 9MiB profile (before: refused over 4MiB) is answered whole.
func TestTranxProfileOverFourMiB(t *testing.T) {
	var chunks [][]byte
	var want []byte
	for i := 0; i < 3; i++ {
		chunk := bytes.Repeat([]byte{byte('a' + i)}, 3<<20)
		chunks = append(chunks, chunk)
		want = append(want, chunk...)
	}
	backend := &fakeBackend{xlog: &protocol.XLogPack{TxID: 11}, chunks: chunks}
	conn, r := startLargeService(t, backend)
	session := loginSession(t, conn, r)
	send(t, conn, "TRANX_PROFILE", session, mp(entry("txid", number(11))))
	aggregationFlag(t, r, transport.HasNext)
	p, err := protocol.ReadPack(r, largeClientCodec)
	if err != nil {
		t.Fatal(err)
	}
	profile, ok := p.(*protocol.ProfilePack)
	if !ok || !bytes.Equal(profile.Profile, want) {
		t.Fatalf("profile %T not answered whole", p)
	}
	aggregationFlag(t, r, transport.NoNext)
}

// streamingBlobFake emits each chunk only after the Client has read the
// previous one, so it passes only when the service writes chunks as they come.
type streamingBlobFake struct {
	*fakeBackend
	chunks [][]byte
	read   chan struct{}
}

func (b *streamingBlobFake) AgentBlobRequest(ctx context.Context, _ int32, _ string, _ *protocol.MapPack, emit func([]byte) error) error {
	for i, chunk := range b.chunks {
		if err := emit(chunk); err != nil {
			return err
		}
		if i == len(b.chunks)-1 {
			break
		}
		select {
		case <-b.read:
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return io.ErrNoProgress // the service kept the chunk instead of writing it
		}
	}
	return nil
}

// HeapDumpService.downloadHepDump: 48MiB in 2MiB chunks reaches the Client
// chunk by chunk with no count limit (before: chunks collected first, 4096
// chunks and 8MiB at most).
func TestAgentBlobRelayWritesEachChunkBeforeTheNext(t *testing.T) {
	b := &streamingBlobFake{fakeBackend: &fakeBackend{}, read: make(chan struct{})}
	for i := 0; i < 24; i++ {
		b.chunks = append(b.chunks, bytes.Repeat([]byte{byte(i)}, 2<<20))
	}
	conn, r := startLargeService(t, b)
	session := loginSession(t, conn, r)
	send(t, conn, "OBJECT_DOWNLOAD_HEAP_DUMP", session, newMap(entry("objHash", decimal(42))))
	for i, want := range b.chunks {
		aggregationFlag(t, r, transport.HasNext)
		if got := readBlob(t, r); !bytes.Equal(got, want) {
			t.Fatalf("chunk %d differs", i)
		}
		if i < len(b.chunks)-1 {
			b.read <- struct{}{}
		}
	}
	aggregationFlag(t, r, transport.NoNext)
}
