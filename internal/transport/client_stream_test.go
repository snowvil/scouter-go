package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// readClientResponse reads HasNEXT items until NoNEXT and returns the item
// count and the total item bytes (flags excluded) — Blob items only.
func readBlobResponse(t *testing.T, conn net.Conn) (items int, total int64) {
	t.Helper()
	for {
		var flag [1]byte
		if _, err := io.ReadFull(conn, flag[:]); err != nil {
			t.Fatalf("after %d items: %v", items, err)
		}
		if flag[0] == NoNext {
			return items, total
		}
		if flag[0] != HasNext {
			t.Fatalf("flag %02x", flag[0])
		}
		var prefix [1]byte
		if _, err := io.ReadFull(conn, prefix[:]); err != nil {
			t.Fatal(err)
		}
		size := int64(prefix[0])
		switch prefix[0] {
		case 255:
			var b [2]byte
			io.ReadFull(conn, b[:])
			size = int64(b[0])<<8 | int64(b[1])
		case 254:
			var b [4]byte
			io.ReadFull(conn, b[:])
			size = int64(b[0])<<24 | int64(b[1])<<16 | int64(b[2])<<8 | int64(b[3])
		}
		if _, err := io.CopyN(io.Discard, conn, size); err != nil {
			t.Fatal(err)
		}
		items++
		total += size
	}
}

func clientLogin(t *testing.T, conn net.Conn) {
	t.Helper()
	request := fixture(t, "tcp-client")
	if _, err := conn.Write(request[:len(request)-6]); err != nil { // without CLOSE
		t.Fatal(err)
	}
}

// Regression (M2): a Client response is no longer capped at 8 MiB per command
// nor at 4 MiB per Blob value. Large chunked Blob streams (Agent blob relay,
// heap/thread dumps) must reach the Client whole.
func TestClientStreamLargerThanFrameBudget(t *testing.T) {
	chunks := []int{6 << 20, 6 << 20, 9 << 20, 1 << 20} // 22 MiB, one Blob > 8 MiB
	_, listener, cancel, done := startTCP(t, TCPConfig{}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if _, err := s.ReadPack(); err != nil {
			return err
		}
		for _, n := range chunks {
			if err := s.ResponseBlob(bytes.Repeat([]byte{7}, n)); err != nil {
				return err
			}
		}
		return nil
	}})
	conn := dialTCP(t, listener)
	conn.SetDeadline(time.Now().Add(20 * time.Second))
	clientLogin(t, conn)
	items, total := readBlobResponse(t, conn)
	want := int64(0)
	for _, n := range chunks {
		want += int64(n)
	}
	if items != len(chunks) || total != want {
		t.Fatalf("items=%d bytes=%d want %d/%d", items, total, len(chunks), want)
	}
	stopTCP(t, cancel, done)
}

// Regression (M2): a response that keeps sending runs past RequestTimeout (the
// former total deadline; 30 s by default, scaled down here).
func TestClientStreamProgressOutlivesRequestTimeout(t *testing.T) {
	const items = 12
	_, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: 100 * time.Millisecond}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if _, err := s.ReadPack(); err != nil {
			return err
		}
		for i := 0; i < items; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(40 * time.Millisecond):
			}
			if err := s.ResponseBlob([]byte{byte(i)}); err != nil {
				return err
			}
		}
		return nil
	}})
	conn := dialTCP(t, listener)
	start := time.Now()
	clientLogin(t, conn)
	n, total := readBlobResponse(t, conn)
	if n != items || total != items {
		t.Fatalf("items=%d bytes=%d", n, total)
	}
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Fatalf("stream finished in %v; the test needs to outlive RequestTimeout", elapsed)
	}
	stopTCP(t, cancel, done)
}

// A slow stream is flushed while it runs: the Client sees the first items
// before the handler ends (Java flushes each history XLog).
func TestClientStreamFlushesWhileRunning(t *testing.T) {
	release := make(chan struct{})
	_, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: 5 * time.Second}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if _, err := s.ReadPack(); err != nil {
			return err
		}
		time.Sleep(clientFlushInterval + 50*time.Millisecond)
		if err := s.ResponseBlob([]byte{1}); err != nil {
			return err
		}
		<-release
		return nil
	}})
	conn := dialTCP(t, listener)
	clientLogin(t, conn)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	got := make([]byte, 3)
	if _, err := io.ReadFull(conn, got); err != nil || !bytes.Equal(got, []byte{HasNext, 1, 1}) {
		t.Fatalf("first item not flushed: %x %v", got, err)
	}
	close(release)
	stopTCP(t, cancel, done)
}

// A Client that stops reading ends the command at the write progress limit.
func TestClientStreamWriteStallTimesOut(t *testing.T) {
	result := make(chan error, 1)
	_, listener, cancel, done := startTCP(t, TCPConfig{RequestTimeout: 100 * time.Millisecond}, TCPHandlers{Client: func(ctx context.Context, r ClientRequest, s *Stream) error {
		if _, err := s.ReadPack(); err != nil {
			return err
		}
		var err error
		for i := 0; i < 1024 && err == nil; i++ {
			err = s.ResponseBlob(make([]byte, 1<<20))
		}
		result <- err
		return err
	}})
	conn := dialTCP(t, listener)
	clientLogin(t, conn) // never reads the response
	select {
	case err := <-result:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stalled Client kept the command running")
	}
	stopTCP(t, cancel, done)
}

// The response encoder accepts values up to MaxResponseValueBytes while the
// request decoder keeps TCPConfig.CodecLimits.
func TestClientResponseLimitsAreResponseOnly(t *testing.T) {
	s := newStream(bytes.NewReader(nil), io.Discard, TCPConfig{CodecLimits: protocol.Limits{MaxBlobBytes: 16}})
	s.asClient(nil)
	if got := s.responseLimits().MaxBlobBytes; got != MaxResponseValueBytes {
		t.Fatal(got)
	}
	if err := s.ResponseBlob(make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	limits, err := s.readLimits()
	if err != nil || limits.MaxBlobBytes != 16 {
		t.Fatal(limits, err)
	}
}

// A Client response may hold more entries than the request decoder accepts:
// a day of 2s counter points (COUNTER_PAST_TIME_ALL) is 43,200 per list.
func TestClientResponseHoldsLargeCollections(t *testing.T) {
	s := newStream(bytes.NewReader(nil), io.Discard, TCPConfig{})
	pack := &protocol.MapPack{}
	for _, key := range []string{"time", "value"} {
		list := make([]protocol.Value, 43200)
		for i := range list {
			list[i] = protocol.Value{Type: protocol.ValueDecimal, Int: int64(i)}
		}
		pack.Entries = append(pack.Entries, protocol.Entry{Key: key, Value: protocol.Value{Type: protocol.ValueList, Values: list}})
	}
	if err := s.ResponsePack(pack); err == nil { // before asClient: request limits
		t.Fatal("request limits accepted the response")
	}
	s.asClient(nil)
	if err := s.ResponsePack(pack); err != nil {
		t.Fatal(err)
	}
}
