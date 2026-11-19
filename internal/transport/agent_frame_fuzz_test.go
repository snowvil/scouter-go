package transport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// The Java Agent allocates every length the server declares without a bound:
// V2 TcpWorker reads in.readIntBytes() and V1 reads Blob lengths inline, both
// into new byte[len] (DataInputX.java:71-80, 90-110). A server frame whose
// declared length is wrong or above the command's item budget therefore turns
// into an Agent heap allocation (agent-side-effects.md §2, §8 row 2). These
// tests capture what RequestLimited actually puts on the wire and decode it
// the way the Agent does, independently of the Go decoder.

// javaAgentInput reads like scouter.io.DataInputX over the peer side of the
// pipe, recording every byte. It refuses a length above budget before
// allocating, where the real Agent would allocate it.
type javaAgentInput struct {
	r      io.Reader
	wire   []byte
	budget int
}

func (in *javaAgentInput) read(n int) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("negative length %d (Agent NegativeArraySizeException)", n)
	}
	if len(in.wire)+n > in.budget {
		return nil, fmt.Errorf("declared length %d at wire offset %d exceeds the item budget %d", n, len(in.wire), in.budget)
	}
	p := make([]byte, n)
	if _, err := io.ReadFull(in.r, p); err != nil {
		return nil, err
	}
	in.wire = append(in.wire, p...)
	return p, nil
}
func (in *javaAgentInput) byte() (byte, error) {
	p, err := in.read(1)
	if err != nil {
		return 0, err
	}
	return p[0], nil
}
func (in *javaAgentInput) int(n int) (int64, error) {
	p, err := in.read(n)
	if err != nil {
		return 0, err
	}
	var v int64
	for _, b := range p {
		v = v<<8 | int64(b)
	}
	shift := uint(64 - 8*n)
	return v << shift >> shift, nil // sign-extend like Java readShort/readInt3/readInt/readLong5
}

// blob is DataInputX.readBlob.
func (in *javaAgentInput) blob() ([]byte, error) {
	n, err := in.byte()
	if err != nil {
		return nil, err
	}
	switch n {
	case 255:
		v, err := in.int(2)
		if err != nil {
			return nil, err
		}
		return in.read(int(uint16(v)))
	case 254:
		v, err := in.int(4)
		if err != nil {
			return nil, err
		}
		return in.read(int(v))
	case 0:
		return []byte{}, nil
	}
	return in.read(int(n))
}

// decimal is DataInputX.readDecimal.
func (in *javaAgentInput) decimal() (int64, error) {
	n, err := in.byte()
	if err != nil {
		return 0, err
	}
	switch n {
	case 0:
		return 0, nil
	case 1, 2, 3, 4, 5:
		return in.int(int(n))
	}
	return in.int(8)
}

// command reads readText() then readPack() as a MapPack with the Value types
// the fuzz parameters use (MapPack.read, ValueEnum).
func (in *javaAgentInput) command() (string, []protocol.Entry, error) {
	text, err := in.blob()
	if err != nil {
		return "", nil, err
	}
	kind, err := in.byte()
	if err != nil {
		return "", nil, err
	}
	if kind != protocol.PackMap {
		return "", nil, fmt.Errorf("pack type %d, want MapPack", kind)
	}
	count, err := in.decimal()
	if err != nil {
		return "", nil, err
	}
	var entries []protocol.Entry
	for i := int64(0); i < count; i++ {
		key, err := in.blob()
		if err != nil {
			return "", nil, err
		}
		t, err := in.byte()
		if err != nil {
			return "", nil, err
		}
		v := protocol.Value{Type: t}
		switch t {
		case protocol.ValueNull:
		case protocol.ValueBoolean:
			b, err := in.byte()
			if err != nil {
				return "", nil, err
			}
			v.Bool = b != 0
		case protocol.ValueDecimal:
			if v.Int, err = in.decimal(); err != nil {
				return "", nil, err
			}
		case protocol.ValueText:
			p, err := in.blob()
			if err != nil {
				return "", nil, err
			}
			v.Text = string(p)
		case protocol.ValueBlob:
			if v.Bytes, err = in.blob(); err != nil {
				return "", nil, err
			}
		default:
			return "", nil, fmt.Errorf("value type %d", t)
		}
		entries = append(entries, protocol.Entry{Key: string(key), Value: v})
	}
	return string(text), entries, nil
}

// agentPeer is what the fake Agent observed for one request.
type agentPeer struct {
	wire      []byte // every byte read before the server closed or answered
	declared  int64  // V2 IntBytes length, -1 when not read
	command   string
	entries   []protocol.Entry
	truncated bool  // the server closed before a whole command arrived
	err       error // framing/decode violation the Agent would hit
	// selfClosed: RequestLimited had closed the connection when it returned
	// (it must on every error, like Java TcpAgentWorker).
	selfClosed bool
}

// serveAgentPeer reads one command like the Java Agent of that version, answers
// NoNEXT once it is decoded, then counts what follows until the server closes.
func serveAgentPeer(conn net.Conn, version, item int) agentPeer {
	in := &javaAgentInput{r: conn, budget: item}
	result := agentPeer{declared: -1}
	var err error
	if version == 2 {
		var n int64
		if n, err = in.int(4); err == nil {
			result.declared = n
			// TcpWorker: in.readIntBytes() then new DataInputX(buff).
			var body []byte
			if body, err = in.read(int(n)); err == nil {
				frame := &javaAgentInput{r: bytes.NewReader(body), budget: len(body)}
				result.command, result.entries, err = frame.command()
				if err == nil && len(frame.wire) != len(body) {
					err = fmt.Errorf("frame declares %d bytes, command uses %d", len(body), len(frame.wire))
				}
			}
		}
	} else {
		result.command, result.entries, err = in.command()
	}
	if err == nil {
		_, err = conn.Write([]byte{NoNext})
		// The server waits for the next request; anything more is a framing bug.
		rest, _ := io.ReadAll(conn)
		if len(rest) > 0 {
			err = fmt.Errorf("%d bytes after the command", len(rest))
		}
	} else if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		result.truncated, err = true, nil
	} else {
		io.Copy(io.Discard, conn)
	}
	result.wire, result.err = in.wire, err
	return result
}

// newPipeAgent wraps one end of net.Pipe like TCPServer.serveConn does.
func newPipeAgent(t testing.TB, conn net.Conn, version int, config TCPConfig) *AgentConn {
	t.Helper()
	config, err := config.normalized()
	if err != nil {
		t.Fatal(err)
	}
	input := &idleReader{conn: conn}
	reader, writer := bufio.NewReaderSize(input, 4096), bufio.NewWriterSize(conn, 4096)
	return &AgentConn{ObjectHash: 7, Version: version, conn: conn, input: input, stream: newStream(reader, writer, config), writer: writer,
		config: config, gate: make(chan struct{}, 1), soTimeout: 2 * time.Second}
}

// requestOverPipe runs one RequestLimited against a fake Agent and returns the
// request error and what the Agent saw (with the connection's closed state
// as RequestLimited left it).
func requestOverPipe(t testing.TB, version int, config TCPConfig, item int, command string, param *protocol.MapPack) (error, agentPeer) {
	t.Helper()
	server, agent := net.Pipe()
	defer agent.Close()
	a := newPipeAgent(t, server, version, config)
	peer := make(chan agentPeer, 1)
	go func() { peer <- serveAgentPeer(agent, version, item) }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := a.RequestLimited(ctx, command, param, AgentLimits{ItemBytes: item, Transfer: 5 * time.Second},
		func(context.Context, *Stream) error { return errors.New("unexpected HasNEXT") })
	selfClosed := a.Closed()
	a.Close()
	result := <-peer
	result.selfClosed = selfClosed
	return err, result
}

// expectedCommand is the wire form of the command (text, then pack): V2
// inside one IntBytes body, V1 inline.
func expectedCommand(command string, param *protocol.MapPack) ([]byte, []byte, error) {
	limits := protocol.Limits{MaxBytes: math.MaxInt32, MaxBlobBytes: math.MaxInt32, MaxCollectionEntries: 1 << 20, MaxTotalValues: 1 << 20}
	text := protocol.NewEncoder(limits)
	text.Text(command)
	t, err := text.Bytes()
	if err != nil {
		return nil, nil, err
	}
	p, err := protocol.EncodePack(param, limits)
	return t, p, err
}

// checkAgentFrame asserts the framing invariants for one request.
func checkAgentFrame(t testing.TB, version int, config TCPConfig, item int, command string, param *protocol.MapPack) {
	t.Helper()
	err, peer := requestOverPipe(t, version, config, item, command, param)
	if peer.err != nil {
		t.Fatalf("V%d item=%d: Agent-side violation: %v (request err %v, wire %d bytes)", version, item, peer.err, err, len(peer.wire))
	}
	if len(peer.wire) > item {
		t.Fatalf("V%d: %d bytes on the wire, item budget %d", version, len(peer.wire), item)
	}
	text, pack, encodeErr := expectedCommand(command, param)
	maxText := config.MaxTextBytes
	if maxText == 0 {
		maxText = 64 << 10
	}
	fits := encodeErr == nil && len(command) <= maxText
	if version == 2 {
		fits = fits && 4+len(text)+len(pack) <= item
	} else {
		fits = fits && len(text)+len(pack) <= item
	}
	if err == nil && peer.truncated {
		t.Fatalf("V%d: request succeeded but the Agent saw a truncated command (%d bytes)", version, len(peer.wire))
	}
	if peer.selfClosed != (err != nil) {
		t.Fatalf("V%d: request err %v but connection closed=%v", version, err, peer.selfClosed)
	}
	if fits != (err == nil) {
		t.Fatalf("V%d item=%d text=%d pack=%d maxText=%d: request err %v, encode err %v", version, item, len(text), len(pack), maxText, err, encodeErr)
	}
	if err != nil {
		if !errors.Is(err, ErrLimit) && !errors.Is(err, protocol.ErrInvalid) {
			t.Fatalf("V%d: request err %v, want ErrLimit or ErrInvalid", version, err)
		}
		if len(peer.wire) != 0 {
			t.Fatalf("V%d %v: %d bytes reached the Agent", version, err, len(peer.wire))
		}
		return
	}
	if version == 2 && (peer.declared != int64(len(text)+len(pack)) || int64(len(peer.wire)) != 4+peer.declared) {
		t.Fatalf("V2: declared %d, body %d, wire %d", peer.declared, len(text)+len(pack), len(peer.wire))
	}
	if !bytes.Equal(peer.wire[len(peer.wire)-len(text)-len(pack):], append(text, pack...)) {
		t.Fatalf("V%d: wire differs from the encoded command", version)
	}
	if peer.command != command || len(peer.entries) != len(param.Entries) {
		t.Fatalf("V%d: Agent decoded %q with %d entries, sent %q with %d", version, peer.command, len(peer.entries), command, len(param.Entries))
	}
	for i, e := range param.Entries {
		got := peer.entries[i]
		if got.Key != e.Key || got.Value.Type != e.Value.Type || got.Value.Bool != e.Value.Bool || got.Value.Int != e.Value.Int ||
			got.Value.Text != e.Value.Text || !bytes.Equal(got.Value.Bytes, e.Value.Bytes) {
			t.Fatalf("V%d entry %d: Agent decoded %+v, sent %+v", version, i, got, e)
		}
	}
}

// fuzzParam builds a MapPack from fuzz bytes: each entry takes a type byte, a
// key length and a value; blob/text sizes come from size so values can reach
// the 4KiB writer buffer and the item budget.
func fuzzParam(data []byte, size uint16) *protocol.MapPack {
	param := &protocol.MapPack{}
	seen := map[string]bool{}
	for len(data) >= 2 && len(param.Entries) < 32 {
		kind, n := data[0], int(data[1]%16)
		data = data[2:]
		if n > len(data) {
			n = len(data)
		}
		key := string(data[:n])
		data = data[n:]
		if seen[key] {
			continue // Java MapPack.put keeps one value per key
		}
		seen[key] = true
		v := protocol.Value{}
		switch kind % 5 {
		case 0:
			v.Type = protocol.ValueNull
		case 1:
			v = protocol.Value{Type: protocol.ValueBoolean, Bool: kind&0x80 != 0}
		case 2:
			var raw [8]byte
			copy(raw[:], data)
			v = protocol.Value{Type: protocol.ValueDecimal, Int: int64(binary.BigEndian.Uint64(raw[:])) >> (kind % 64)}
		case 3:
			v = protocol.Value{Type: protocol.ValueText, Text: string(bytes.Repeat([]byte{'t'}, int(size)%300)) + key}
		case 4:
			v = protocol.Value{Type: protocol.ValueBlob, Bytes: bytes.Repeat([]byte{kind}, int(size))}
		}
		param.Entries = append(param.Entries, protocol.Entry{Key: key, Value: v})
	}
	return param
}

// FuzzAgentRequestFrame: for any command and MapPack parameter, the bytes the
// Agent reads either form exactly one command within the item budget (V2:
// declared IntBytes length == bytes that follow, 4+length <= item; V1: a
// Java-compatible reader consumes exactly the command) or, on a refused
// request, nothing; the connection is closed exactly when the request fails.
func FuzzAgentRequestFrame(f *testing.F) {
	f.Add(uint8(2), uint32(8<<10), uint16(0), "KEEP_ALIVE", []byte{})
	f.Add(uint8(1), uint32(8<<10), uint16(0), "OBJECT_RESET_CACHE", []byte{})
	f.Add(uint8(2), uint32(4096), uint16(4000), "OBJECT_SET_KUBE_SEQ", []byte{2, 3, 's', 'e', 'q', 1})
	f.Add(uint8(2), uint32(5000), uint16(4980), "TRIGGER_THREAD_DUMPS_FROM_CONDITIONS", []byte{4, 1, 'b', 3, 6, 'r', 'e', 'a', 's', 'o', 'n'})
	f.Add(uint8(1), uint32(9000), uint16(8500), "OBJECT_ENV", []byte{4, 1, 'x', 4, 1, 'y'})
	f.Add(uint8(1), uint32(6000), uint16(0), string(bytes.Repeat([]byte{'c'}, 5000)), []byte{4, 1, 'b'})
	// V1 command Text larger than the 4KiB writer buffer, Pack over the item budget.
	f.Add(uint8(1), uint32(6000), uint16(5000), string(bytes.Repeat([]byte{'c'}, 5000)), []byte{4, 1, 'b'})
	f.Add(uint8(2), uint32(64), uint16(100), "OBJECT_THREAD_LIST", []byte{1, 0, 4, 2, 'k', 'k'})
	f.Add(uint8(2), uint32(70000), uint16(65535), "LIST_CONFIGURE_WAS", []byte{4, 0, 3, 1, 'a'})
	f.Add(uint8(1), uint32(300), uint16(10), "\xff", []byte{3, 2, 0xff, 0xfe})
	f.Add(uint8(2), uint32(300), uint16(10), "\xff", []byte{3, 2, 0xff, 0xfe})
	f.Add(uint8(2), uint32(300), uint16(10), "OBJECT\xc3(ENV", []byte{})
	f.Fuzz(func(t *testing.T, version uint8, item uint32, size uint16, command string, data []byte) {
		v := 2 - int(version%2) // odd: V1, even: V2
		budget := 16 + int(item%(1<<17))
		config := TCPConfig{MaxTextBytes: 64 + int(size)%(16<<10)}
		checkAgentFrame(t, v, config, budget, command, fuzzParam(data, size))
	})
}

// TestAgentRequestFrameAtRelayLimit pins the invariant at the real ordinary
// relay item budget (8MiB, internal/server agent_blob.go): a command that
// fills the budget exactly is written whole, one byte more is refused with
// nothing on the wire.
func TestAgentRequestFrameAtRelayLimit(t *testing.T) {
	const item = 8 << 20
	for _, version := range []int{1, 2} {
		for _, over := range []int{0, 1} {
			t.Run("V"+strconv.Itoa(version)+"+"+strconv.Itoa(over), func(t *testing.T) {
				command := "OBJECT_ENV"
				// Size a Blob so the command is exactly item (+over) bytes on the wire.
				frame := func(n int) int {
					text, pack, err := expectedCommand(command, &protocol.MapPack{Entries: []protocol.Entry{{Key: "b", Value: protocol.Value{Type: protocol.ValueBlob, Bytes: make([]byte, n)}}}})
					if err != nil {
						t.Fatal(err)
					}
					if version == 2 {
						return 4 + len(text) + len(pack)
					}
					return len(text) + len(pack)
				}
				n := item - frame(0)
				for frame(n) > item {
					n--
				}
				for frame(n) < item+over {
					n++
				}
				if frame(n) != item+over {
					t.Fatalf("no Blob size gives %d bytes", item+over)
				}
				param := &protocol.MapPack{Entries: []protocol.Entry{{Key: "b", Value: protocol.Value{Type: protocol.ValueBlob, Bytes: bytes.Repeat([]byte{7}, n)}}}}
				err, peer := requestOverPipe(t, version, TCPConfig{}, item, command, param)
				if over == 0 {
					if err != nil || peer.err != nil || len(peer.wire) != item || peer.selfClosed {
						t.Fatalf("request %v, Agent %v, wire %d", err, peer.err, len(peer.wire))
					}
					if version == 2 && peer.declared != item-4 {
						t.Fatalf("declared %d, want %d", peer.declared, item-4)
					}
					return
				}
				if !errors.Is(err, ErrLimit) || peer.err != nil || len(peer.wire) != 0 || !peer.selfClosed {
					t.Fatalf("request %v, Agent %v, wire %d, closed %v; want ErrLimit, no bytes and a closed connection", err, peer.err, len(peer.wire), peer.selfClosed)
				}
			})
		}
	}
}

// TestAgentRequestFrameAtTextLimit pins the command Text bound
// (TCPConfig.MaxTextBytes) for V1 and V2: a command of exactly the bound is
// written whole, one byte more is refused with ErrLimit, nothing on the wire
// and the connection closed.
func TestAgentRequestFrameAtTextLimit(t *testing.T) {
	const maxText, item = 1000, 8 << 10
	for _, version := range []int{1, 2} {
		for _, over := range []int{0, 1} {
			t.Run("V"+strconv.Itoa(version)+"+"+strconv.Itoa(over), func(t *testing.T) {
				command := string(bytes.Repeat([]byte{'c'}, maxText+over))
				param := &protocol.MapPack{Entries: []protocol.Entry{{Key: "k", Value: protocol.Value{Type: protocol.ValueText, Text: "v"}}}}
				err, peer := requestOverPipe(t, version, TCPConfig{MaxTextBytes: maxText}, item, command, param)
				if over == 0 {
					text, pack, encodeErr := expectedCommand(command, param)
					want := len(text) + len(pack)
					if version == 2 {
						want += 4
					}
					if encodeErr != nil || err != nil || peer.err != nil || peer.selfClosed || len(peer.wire) != want || peer.command != command {
						t.Fatalf("request %v, Agent %v, closed %v, wire %d (want %d)", err, peer.err, peer.selfClosed, len(peer.wire), want)
					}
					return
				}
				if !errors.Is(err, ErrLimit) || peer.err != nil || len(peer.wire) != 0 || !peer.selfClosed {
					t.Fatalf("request %v, Agent %v, wire %d, closed %v; want ErrLimit, no bytes and a closed connection", err, peer.err, len(peer.wire), peer.selfClosed)
				}
			})
		}
	}
}
