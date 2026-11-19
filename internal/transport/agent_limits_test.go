package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

var bigCodec = protocol.Limits{MaxBytes: 64 << 20, MaxBlobBytes: 64 << 20}

func encodeAgentItems(t *testing.T, encode func(*protocol.Encoder)) []byte {
	t.Helper()
	e := protocol.NewEncoder(bigCodec)
	encode(e)
	b, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func blobChunk(t *testing.T, data []byte) []byte {
	return encodeAgentItems(t, func(e *protocol.Encoder) { e.Byte(HasNext); e.Blob(data) })
}

// registerAgentV2 connects a TCP_AGENT_V2 peer.
func registerAgentV2(t *testing.T, listener net.Listener, agents <-chan *AgentConn) (net.Conn, *AgentConn) {
	t.Helper()
	conn := dialRaw(t, listener)
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], TCPAgentV2)
	binary.BigEndian.PutUint32(header[4:], 7)
	conn.Write(header[:])
	select {
	case a := <-agents:
		return conn, a
	case <-time.After(time.Second):
		t.Fatal("agent not registered")
	}
	return nil, nil
}

// readCommand consumes one KEEP_ALIVE command (V1 13 bytes, V2 IntBytes).
func readCommand(t *testing.T, conn net.Conn, version int) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if version == 1 {
		if _, err := io.ReadFull(conn, make([]byte, 13)); err != nil {
			t.Fatal(err)
		}
		return
	}
	var n [4]byte
	if _, err := io.ReadFull(conn, n[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(conn, make([]byte, binary.BigEndian.Uint32(n[:]))); err != nil {
		t.Fatal(err)
	}
}

// A response that keeps making progress may take longer than RequestTimeout:
// the total deadline is so_timeout+RequestTimeout, never below the so-timeout
// (before, RequestTimeout alone cut a 60s net_tcp_agent_so_timeout_ms at 30s).
func TestAgentResponseWithProgressOutlivesRequestTimeout(t *testing.T) {
	server, listener, cancel, done, agents := agentServer(t, TCPConfig{RequestTimeout: 100 * time.Millisecond})
	if err := server.SetAgentSoTimeout(400 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	conn, agent := registerAgent(t, listener, agents)
	result := keepAlive(agent)
	readCommand(t, conn, 1)
	start := time.Now()
	for _, b := range []byte{HasNext, protocol.PackMap, 0, NoNext} {
		time.Sleep(70 * time.Millisecond)
		conn.Write([]byte{b})
	}
	if err := <-result; err != nil {
		t.Fatal("progressing response cut by RequestTimeout", err)
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("response took %v, not longer than RequestTimeout", elapsed)
	}
	// A silent agent still ends within so_timeout+RequestTimeout.
	start = time.Now()
	silent := keepAlive(agent)
	readCommand(t, conn, 1)
	select {
	case err := <-silent:
		if !errors.Is(err, os.ErrDeadlineExceeded) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed < 350*time.Millisecond {
			t.Fatalf("silent agent ended after %v, before the so-timeout", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("silent agent not ended")
	}
	if !agent.Closed() {
		t.Fatal("timed out agent kept")
	}
	stopTCP(t, cancel, done)
}

// A chunk stream (Transfer<0, TotalBytes<0) has no total deadline and no total
// size: only each read's so-timeout and each item's size are bounded.
func TestAgentChunkStreamBoundsOnlyItemsAndReads(t *testing.T) {
	server, listener, cancel, done, agents := agentServer(t, TCPConfig{RequestTimeout: 50 * time.Millisecond, MaxFrameBytes: 1 << 20})
	if err := server.SetAgentSoTimeout(200 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	stream := AgentLimits{ItemBytes: 512 << 10, TotalBytes: -1, Transfer: -1}
	for _, version := range []int{1, 2} {
		var conn net.Conn
		var agent *AgentConn
		if version == 1 {
			conn, agent = registerAgent(t, listener, agents)
		} else {
			conn, agent = registerAgentV2(t, listener, agents)
		}
		// 12 chunks of 256KiB = 3MiB (3x MaxFrameBytes), 60ms apart: 720ms in
		// total, longer than so_timeout+RequestTimeout.
		var got bytes.Buffer
		result := make(chan error, 1)
		go func() {
			result <- agent.RequestLimited(context.Background(), "KEEP_ALIVE", &protocol.MapPack{}, stream, func(_ context.Context, s *Stream) error {
				b, err := s.ReadBlob()
				got.Write(b)
				return err
			})
		}()
		readCommand(t, conn, version)
		var want bytes.Buffer
		for i := 0; i < 12; i++ {
			chunk := bytes.Repeat([]byte{byte(version*16 + i)}, 256<<10)
			want.Write(chunk)
			time.Sleep(60 * time.Millisecond)
			if _, err := conn.Write(blobChunk(t, chunk)); err != nil {
				t.Fatal(err)
			}
		}
		conn.Write([]byte{NoNext})
		if err := <-result; err != nil {
			t.Fatalf("V%d chunk stream %v", version, err)
		}
		if !bytes.Equal(got.Bytes(), want.Bytes()) {
			t.Fatalf("V%d relayed %d bytes, want %d", version, got.Len(), want.Len())
		}

		// One chunk over ItemBytes is rejected before its body is read.
		result = make(chan error, 1)
		go func() {
			result <- agent.RequestLimited(context.Background(), "KEEP_ALIVE", &protocol.MapPack{}, stream, func(_ context.Context, s *Stream) error {
				_, err := s.ReadBlob()
				return err
			})
		}()
		readCommand(t, conn, version)
		conn.Write(blobChunk(t, make([]byte, 600<<10)))
		if err := <-result; !errors.Is(err, ErrLimit) {
			t.Fatalf("V%d oversized chunk %v", version, err)
		}
		if !agent.Closed() {
			t.Fatal("limit error kept the agent connection")
		}
	}

	// A silent agent in a stream ends at the so-timeout.
	conn, agent := registerAgent(t, listener, agents)
	start := time.Now()
	result := make(chan error, 1)
	go func() {
		result <- agent.RequestLimited(context.Background(), "KEEP_ALIVE", &protocol.MapPack{}, stream, func(_ context.Context, s *Stream) error {
			_, err := s.ReadBlob()
			return err
		})
	}()
	readCommand(t, conn, 1)
	conn.Write(blobChunk(t, []byte("first")))
	select {
	case err := <-result:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed < 180*time.Millisecond {
			t.Fatalf("stream so-timeout after %v", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("silent stream not ended")
	}
	stopTCP(t, cancel, done)
}

// The default budget is the whole response within MaxFrameBytes; a command's
// AgentLimits raise it for one large Pack (OBJECT_DOWNLOAD_JAR's jar Blob).
func TestAgentPackItemLimitPerCommand(t *testing.T) {
	_, listener, cancel, done, agents := agentServer(t, TCPConfig{})
	jar := bytes.Repeat([]byte{0x5a}, 10<<20) // over the 8MiB default frame and 4MiB codec Blob
	pack := &protocol.MapPack{Entries: []protocol.Entry{{Key: "jar", Value: protocol.Value{Type: protocol.ValueBlob, Bytes: jar}}}}
	for _, version := range []int{1, 2} {
		body, err := protocol.EncodePack(pack, bigCodec)
		if err != nil {
			t.Fatal(err)
		}
		response := []byte{HasNext}
		if version == 2 {
			response = binary.BigEndian.AppendUint32(response, uint32(len(body)))
		}
		response = append(append(response, body...), NoNext)
		for _, c := range []struct {
			limits AgentLimits
			ok     bool
		}{{AgentLimits{}, false}, {AgentLimits{ItemBytes: 32 << 20}, true}} {
			var conn net.Conn
			var agent *AgentConn
			if version == 1 {
				conn, agent = registerAgent(t, listener, agents)
			} else {
				conn, agent = registerAgentV2(t, listener, agents)
			}
			var got *protocol.MapPack
			result := make(chan error, 1)
			go func() {
				result <- agent.RequestPackLimited(context.Background(), "KEEP_ALIVE", &protocol.MapPack{}, c.limits, func(_ context.Context, p protocol.Pack) error {
					got, _ = p.(*protocol.MapPack)
					return nil
				})
			}()
			readCommand(t, conn, version)
			go conn.Write(response)
			err := <-result
			if !c.ok {
				if !errors.Is(err, ErrLimit) && !errors.Is(err, protocol.ErrLimit) {
					t.Fatalf("V%d default limit %v", version, err)
				}
				continue
			}
			if err != nil {
				t.Fatalf("V%d large Pack %v", version, err)
			}
			if got == nil || len(got.Entries) != 1 || !bytes.Equal(got.Entries[0].Value.Bytes, jar) {
				t.Fatalf("V%d jar not relayed intact", version)
			}
		}
	}
	stopTCP(t, cancel, done)
}

// Agent answers are not held to the request decoder's 4096 entries and 16384
// values: an OBJECT_ACTIVE_SERVICE_LIST with 2000 active services is 12 lists
// of 2000 (24000 values).
func TestAgentPackManyValues(t *testing.T) {
	_, listener, cancel, done, agents := agentServer(t, TCPConfig{})
	pack := &protocol.MapPack{}
	for i := 0; i < 12; i++ {
		list := protocol.Value{Type: protocol.ValueList, Values: make([]protocol.Value, 2000)}
		for j := range list.Values {
			list.Values[j] = protocol.Value{Type: protocol.ValueDecimal, Int: int64(j)}
		}
		pack.Entries = append(pack.Entries, protocol.Entry{Key: "k" + strconv.Itoa(i), Value: list})
	}
	for _, version := range []int{1, 2} {
		body, err := protocol.EncodePack(pack, protocol.Limits{MaxCollectionEntries: 1 << 16, MaxTotalValues: 1 << 16})
		if err != nil {
			t.Fatal(err)
		}
		response := []byte{HasNext}
		if version == 2 {
			response = binary.BigEndian.AppendUint32(response, uint32(len(body)))
		}
		response = append(append(response, body...), NoNext)
		var conn net.Conn
		var agent *AgentConn
		if version == 1 {
			conn, agent = registerAgent(t, listener, agents)
		} else {
			conn, agent = registerAgentV2(t, listener, agents)
		}
		var got *protocol.MapPack
		result := make(chan error, 1)
		go func() {
			result <- agent.RequestPack(context.Background(), "OBJECT_ACTIVE_SERVICE_LIST", &protocol.MapPack{}, func(_ context.Context, p protocol.Pack) error {
				got, _ = p.(*protocol.MapPack)
				return nil
			})
		}()
		readCommand(t, conn, version)
		go conn.Write(response)
		if err := <-result; err != nil {
			t.Fatalf("V%d %v", version, err)
		}
		if got == nil || len(got.Entries) != 12 || len(got.Entries[11].Value.Values) != 2000 {
			t.Fatalf("V%d answer not decoded whole", version)
		}
	}
	stopTCP(t, cancel, done)
}
