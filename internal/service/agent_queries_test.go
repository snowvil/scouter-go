package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

type agentReadCall struct {
	hash    int32
	command string
	param   *protocol.MapPack
}
type agentReadFake struct {
	*fakeBackend
	mu      sync.Mutex
	calls   []agentReadCall
	replies []protocol.Pack
	blobs   [][]byte
	err     error
	cancel  context.CancelFunc
}

func (b *agentReadFake) AgentRequest(_ context.Context, hash int32, command string, p *protocol.MapPack) ([]protocol.Pack, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls = append(b.calls, agentReadCall{hash, command, p})
	if b.cancel != nil {
		b.cancel()
	}
	return b.replies, b.err
}
func (b *agentReadFake) AgentBlobRequest(_ context.Context, hash int32, command string, p *protocol.MapPack, emit func([]byte) error) error {
	b.mu.Lock()
	b.calls = append(b.calls, agentReadCall{hash, command, p})
	if b.cancel != nil {
		b.cancel()
	}
	blobs, err := b.blobs, b.err
	b.mu.Unlock()
	for _, blob := range blobs {
		if e := emit(blob); e != nil {
			return e
		}
	}
	return err
}
func (b *agentReadFake) snapshot() []agentReadCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]agentReadCall(nil), b.calls...)
}

func TestAgentReadScopeAndMutationExclusion(t *testing.T) {
	data, err := os.ReadFile("../../docs/compatibility/query-scope.json")
	if err != nil {
		t.Fatal(err)
	}
	var scope struct {
		Commands []struct {
			Literal  string `json:"command_literal"`
			Category string `json:"category"`
		}
	}
	if err = json.Unmarshal(data, &scope); err != nil {
		t.Fatal(err)
	}
	got := AgentReadCommands()
	want := []string{}
	for _, c := range scope.Commands {
		if c.Category == "agent_read_relay" {
			want = append(want, c.Literal)
		}
		if c.Category == "mutation" {
			s := &Service{}
			handled, err := s.HandleAgentReadCommand(context.Background(), transport.ClientRequest{Command: c.Literal}, nil)
			if handled || err != nil {
				t.Fatal("mutation recognized", c.Literal, err)
			}
		}
	}
	if !reflect.DeepEqual(got, want) || len(got) != 43 {
		t.Fatal(got, want)
	}
	got[0] = "changed"
	if AgentReadCommands()[0] == "changed" {
		t.Fatal("borrowed command list")
	}
}

func TestAgentReadAllCommandsBody(t *testing.T) {
	for _, cmd := range append(AgentReadCommands(), "OBJECT_ENV") {
		t.Run(cmd, func(t *testing.T) {
			b := &agentReadFake{fakeBackend: &fakeBackend{}, replies: []protocol.Pack{newMap(entry("answer", decimal(1)))}, blobs: [][]byte{{1, 2}, {}, {3}}}
			_, conn, r := startService(t, queryConfig(), b)
			session := loginSession(t, conn, r)
			p := newMap(entry("objHash", decimal(42)), entry("opaque", protocol.Value{Type: protocol.ValueBlob, Bytes: []byte{0, 255}}), entry("id", protocol.Value{Type: protocol.ValueNull}))
			if cmd == "OBJECT_ACTIVE_SERVICE_LIST_GROUP" {
				p.Entries[0].Value = list([]protocol.Value{decimal(42)})
			}
			send(t, conn, cmd, session, p)
			if isAgentBlobCommand(cmd) {
				for _, want := range b.blobs {
					aggregationFlag(t, r, transport.HasNext)
					if got := readBlob(t, r); !reflect.DeepEqual(got, want) {
						t.Fatal(got, want)
					}
				}
				aggregationFlag(t, r, transport.NoNext)
			} else {
				packs := readPacks(t, r)
				if len(packs) != 1 || mapValue(packs[0].(*protocol.MapPack), "answer").Int != 1 {
					t.Fatal(packs)
				}
			}
			calls := b.snapshot()
			upstream := cmd
			if cmd == "OBJECT_ACTIVE_SERVICE_LIST_GROUP" {
				upstream = "OBJECT_ACTIVE_SERVICE_LIST"
			}
			if len(calls) != 1 || calls[0].hash != 42 || calls[0].command != upstream || !reflect.DeepEqual(calls[0].param, p) {
				t.Fatal(calls, p)
			}
		})
	}
}

func TestAgentReadLastPackAndNull(t *testing.T) {
	for _, tc := range []struct {
		name    string
		replies []protocol.Pack
		want    int64
	}{
		{"last map", []protocol.Pack{newMap(entry("answer", decimal(1))), newMap(entry("answer", decimal(2)))}, 2},
		{"last nonmap", []protocol.Pack{newMap(entry("answer", decimal(1))), &protocol.TextPack{Text: "synthetic"}}, 0},
		{"offline", nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &agentReadFake{fakeBackend: &fakeBackend{}, replies: tc.replies}
			_, conn, r := startService(t, queryConfig(), b)
			session := loginSession(t, conn, r)
			send(t, conn, "HOST_TOP", session, newMap(entry("objHash", decimal(42))))
			packs := readPacks(t, r)
			if tc.want == 0 {
				if len(packs) != 0 {
					t.Fatal(packs)
				}
			} else if len(packs) != 1 || mapValue(packs[0].(*protocol.MapPack), "answer").Int != tc.want {
				t.Fatal(packs)
			}
		})
	}
}

func TestAgentReadActiveSelectionOrderAndOwnedDecoration(t *testing.T) {
	for _, cmd := range []string{"OBJECT_ACTIVE_SERVICE_LIST", "OBJECT_BATCH_ACTIVE_LIST", "OBJECT_ACTIVE_SERVICE_LIST_GROUP"} {
		t.Run(cmd, func(t *testing.T) {
			original := newMap(entry("first", decimal(1)), entry("objHash", decimal(999)), entry("last", decimal(2)))
			b := &agentReadFake{fakeBackend: &fakeBackend{objects: []*protocol.ObjectPack{{ObjHash: 9, ObjType: "java", Alive: true}, {ObjHash: 4, ObjType: "java", Alive: false}, {ObjHash: 3, ObjType: "host", Alive: true}, {ObjHash: 2, ObjType: "java", Alive: true}}}, replies: []protocol.Pack{original}}
			_, conn, r := startService(t, queryConfig(), b)
			session := loginSession(t, conn, r)
			p := newMap(entry("objType", stringValue("java")))
			want := []int32{9, 2}
			if cmd == "OBJECT_ACTIVE_SERVICE_LIST_GROUP" {
				want = []int32{2, 9, 2}
				p = newMap(entry("objHash", list([]protocol.Value{decimal(2), decimal(9), decimal(2)})))
			}
			send(t, conn, cmd, session, p)
			packs := readPacks(t, r)
			calls := b.snapshot()
			if len(packs) != len(want) || len(calls) != len(want) {
				t.Fatal(packs, calls)
			}
			for i, hash := range want {
				v := packs[i].(*protocol.MapPack)
				if calls[i].hash != hash || mapValue(v, "objHash").Int != int64(hash) || v.Entries[1].Key != "objHash" {
					t.Fatal(v, calls)
				}
			}
			if mapValue(original, "objHash").Int != 999 {
				t.Fatal("mutated borrowed result", original)
			}
		})
	}
}

func TestAgentReadFallbackAndThreadIDPresence(t *testing.T) {
	b := &agentReadFake{fakeBackend: &fakeBackend{}}
	_, conn, r := startService(t, queryConfig(), b)
	session := loginSession(t, conn, r)
	send(t, conn, "OBJECT_ACTIVE_SERVICE_LIST", session, newMap(entry("objHash", decimal(42))))
	packs := readPacks(t, r)
	if len(packs) != 1 || len(packs[0].(*protocol.MapPack).Entries) != 1 || mapValue(packs[0].(*protocol.MapPack), "objHash").Int != 42 {
		t.Fatal(packs)
	}
	send(t, conn, "OBJECT_ACTIVE_SERVICE_LIST", session, newMap())
	if len(readPacks(t, r)) != 0 || len(b.snapshot()) != 1 {
		t.Fatal("missing type sent upstream")
	}
	send(t, conn, "OBJECT_THREAD_DETAIL", session, newMap(entry("objHash", decimal(42))))
	if len(readPacks(t, r)) != 0 || len(b.snapshot()) != 1 {
		t.Fatal("missing id sent upstream")
	}
	send(t, conn, "OBJECT_THREAD_DETAIL", session, newMap(entry("objHash", decimal(42)), entry("id", protocol.Value{Type: protocol.ValueNull})))
	readPacks(t, r)
	if len(b.snapshot()) != 2 {
		t.Fatal("null id key ignored")
	}
}

func TestAgentReadFailureBoundaries(t *testing.T) {
	s := &Service{}
	if ok, err := s.HandleAgentReadCommand(context.Background(), transport.ClientRequest{Command: "HOST_TOP"}, nil); !ok || !errors.Is(err, transport.ErrUnauthorized) {
		t.Fatal(ok, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ok, err := s.HandleAgentReadCommand(ctx, transport.ClientRequest{Command: "HOST_TOP", Authenticated: true}, nil); !ok || !errors.Is(err, context.Canceled) {
		t.Fatal(ok, err)
	}
	for _, p := range []protocol.Pack{&protocol.TextPack{}, newMap(entry("objHash", decimal(1<<40)))} {
		b := &agentReadFake{fakeBackend: &fakeBackend{}}
		_, conn, r := startService(t, queryConfig(), b)
		session := loginSession(t, conn, r)
		send(t, conn, "HOST_TOP", session, p)
		if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
		if len(b.snapshot()) != 0 {
			t.Fatal("invalid request sent")
		}
	}
	// A Pack relay answers an agent failure like Java's null AgentCall
	// result (TestAgentReadSingleTargetFailureIsJavaNull); a Blob stream and
	// a Go relay limit still fail the request.
	for _, tc := range []struct {
		cmd string
		err error
	}{{"OBJECT_DOWNLOAD_HEAP_DUMP", errors.New("synthetic failure")}, {"HOST_TOP", transport.ErrLimit}} {
		b := &agentReadFake{fakeBackend: &fakeBackend{}, err: tc.err}
		_, conn, r := startService(t, queryConfig(), b)
		session := loginSession(t, conn, r)
		send(t, conn, tc.cmd, session, newMap(entry("objHash", decimal(42))))
		if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
			t.Fatal(tc.cmd, "error became empty success", err)
		}
	}
}

// Every single-target Pack relay handler in Java calls AgentCall.call, which
// returns null after an agent I/O failure (TcpAgentWorker.scala:21-68 closes
// the session and swallows the error, AgentCall.scala:46-71). The handlers
// then write nothing (`if (p != null)`) except the active-list ones, which
// write {objHash} (ThreadList.scala:113-124, BatchService.scala:125-136).
// The Client gets a complete empty answer, not a closed connection.
func TestAgentReadSingleTargetFailureIsJavaNull(t *testing.T) {
	restore := agentRelaySkipLogf
	agentRelaySkipLogf = func(string, ...any) {}
	defer func() { agentRelaySkipLogf = restore }()
	captureManageLog(t)
	commands := append([]string{"OBJECT_ENV", "CONFIGURE_DESC", "CONFIGURE_VALUE_TYPE"}, AgentMutationCommands()...)
	for _, cmd := range AgentReadCommands() {
		if !isAgentBlobCommand(cmd) && cmd != "OBJECT_ACTIVE_SERVICE_LIST_GROUP" {
			commands = append(commands, cmd)
		}
	}
	b := &agentReadFake{fakeBackend: &fakeBackend{}, err: io.ErrUnexpectedEOF}
	_, conn, r := startService(t, queryConfig(), b)
	session := loginSession(t, conn, r)
	before := agentRelaySkips.Load()
	for _, cmd := range commands {
		p := newMap(entry("objHash", decimal(42)))
		if cmd == "OBJECT_THREAD_DETAIL" {
			p.Entries = append(p.Entries, entry("id", decimal(7)))
		}
		send(t, conn, cmd, session, p)
		packs := readPacks(t, r)
		switch cmd {
		case "OBJECT_ACTIVE_SERVICE_LIST", "OBJECT_BATCH_ACTIVE_LIST":
			if len(packs) != 1 || len(packs[0].(*protocol.MapPack).Entries) != 1 || mapValue(packs[0].(*protocol.MapPack), "objHash").Int != 42 {
				t.Fatal(cmd, "placeholder", packs)
			}
		default:
			if len(packs) != 0 {
				t.Fatal(cmd, packs)
			}
		}
	}
	if got := agentRelaySkips.Load() - before; got != uint64(len(commands)) || len(b.snapshot()) != len(commands) {
		t.Fatal("skips", got, len(b.snapshot()), len(commands))
	}
}

func TestAgentReadOptionalBackendAndResponseLimits(t *testing.T) {
	s := &Service{backend: &fakeBackend{}}
	for _, command := range []string{"HOST_TOP", "OBJECT_DOWNLOAD_HEAP_DUMP"} {
		if err := s.agentReadQuery(context.Background(), command, newMap(), nil); !errors.Is(err, transport.ErrUnsupported) {
			t.Fatal(command, err)
		}
	}
	// Blob relays have no chunk count limit: chunks stream to the Client one at
	// a time like Java HeapDumpService (large_response_test.go).
	b := &agentReadFake{fakeBackend: &fakeBackend{}, replies: make([]protocol.Pack, 4097)}
	s.backend = b
	for _, command := range []string{"HOST_TOP"} {
		if err := s.agentReadQuery(context.Background(), command, newMap(entry("objHash", decimal(42))), nil); !errors.Is(err, transport.ErrLimit) {
			t.Fatal(command, err)
		}
	}
	b.mu.Lock()
	b.calls = nil
	b.mu.Unlock()
	p := newMap(entry("objHash", list(make([]protocol.Value, 4097))))
	for i := range p.Entries[0].Value.Values {
		p.Entries[0].Value.Values[i] = decimal(42)
	}
	if err := s.agentReadQuery(context.Background(), "OBJECT_ACTIVE_SERVICE_LIST_GROUP", p, nil); !errors.Is(err, transport.ErrLimit) || len(b.snapshot()) != 0 {
		t.Fatal(err, b.snapshot())
	}
	for _, v := range []protocol.Value{{Type: protocol.ValueNull}, decimal(42), list([]protocol.Value{stringValue("42")}), list([]protocol.Value{decimal(1 << 40)})} {
		if err := s.agentReadQuery(context.Background(), "OBJECT_ACTIVE_SERVICE_LIST_GROUP", newMap(entry("objHash", v)), nil); !errors.Is(err, transport.ErrMalformed) {
			t.Fatal(v, err)
		}
	}
}

func TestAgentReadBlobLengthBoundaries(t *testing.T) {
	b := &agentReadFake{fakeBackend: &fakeBackend{}}
	for _, n := range []int{0, 253, 254, 65535, 65536} {
		data := make([]byte, n)
		for i := range data {
			data[i] = byte(i)
		}
		b.blobs = append(b.blobs, data)
	}
	_, conn, r := startService(t, queryConfig(), b)
	session := loginSession(t, conn, r)
	send(t, conn, "OBJECT_DUMP_FILE_DETAIL", session, newMap(entry("objHash", decimal(42))))
	for _, want := range b.blobs {
		aggregationFlag(t, r, transport.HasNext)
		if got := readBlob(t, r); !reflect.DeepEqual(got, want) {
			t.Fatal("blob boundary", len(got), len(want))
		}
	}
	aggregationFlag(t, r, transport.NoNext)
}

func TestAgentReadCancellationAfterEmptyBackendResponse(t *testing.T) {
	for _, command := range []string{"HOST_TOP", "OBJECT_DOWNLOAD_HEAP_DUMP"} {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		b := &agentReadFake{fakeBackend: &fakeBackend{}, cancel: cancel}
		s := &Service{backend: b}
		if err := s.agentReadQuery(ctx, command, newMap(entry("objHash", decimal(42))), nil); !errors.Is(err, context.Canceled) {
			t.Fatal(command, err)
		}
	}
}

type agentBlobOnlyFake struct{ *fakeBackend }

func (*agentBlobOnlyFake) AgentBlobRequest(_ context.Context, _ int32, _ string, _ *protocol.MapPack, emit func([]byte) error) error {
	return emit([]byte{1, 2, 3})
}
func TestAgentReadBlobOnlyBackend(t *testing.T) {
	b := &agentBlobOnlyFake{&fakeBackend{}}
	_, conn, r := startService(t, queryConfig(), b)
	session := loginSession(t, conn, r)
	send(t, conn, "OBJECT_DOWNLOAD_HEAP_DUMP", session, newMap(entry("objHash", decimal(42))))
	aggregationFlag(t, r, transport.HasNext)
	if got := readBlob(t, r); !reflect.DeepEqual(got, []byte{1, 2, 3}) {
		t.Fatal(got)
	}
	aggregationFlag(t, r, transport.NoNext)
}

func TestAgentReadPartialBlobBeforeUpstreamFailure(t *testing.T) {
	b := &agentReadFake{fakeBackend: &fakeBackend{}, blobs: [][]byte{{1, 2}, {3}}, err: io.ErrUnexpectedEOF}
	_, conn, r := startService(t, queryConfig(), b)
	session := loginSession(t, conn, r)
	send(t, conn, "OBJECT_DOWNLOAD_HEAP_DUMP", session, newMap(entry("objHash", decimal(42))))
	for _, want := range b.blobs {
		aggregationFlag(t, r, transport.HasNext)
		if got := readBlob(t, r); !reflect.DeepEqual(got, want) {
			t.Fatalf("chunk=%x want=%x", got, want)
		}
	}
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("upstream failure must close without terminator: %v", err)
	}
}

// agentFanOutFake answers per objHash: errs[hash] fails that agent only.
type agentFanOutFake struct {
	*fakeBackend
	mu     sync.Mutex
	calls  []int32
	errs   map[int32]error
	cancel map[int32]context.CancelFunc
}

func (b *agentFanOutFake) AgentRequest(ctx context.Context, hash int32, _ string, _ *protocol.MapPack) ([]protocol.Pack, error) {
	b.mu.Lock()
	b.calls = append(b.calls, hash)
	cancel := b.cancel[hash]
	err := b.errs[hash]
	b.mu.Unlock()
	if cancel != nil {
		cancel()
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, err
	}
	return []protocol.Pack{newMap(entry("answer", decimal(int64(hash)*10)))}, nil
}
func (b *agentFanOutFake) snapshot() []int32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int32(nil), b.calls...)
}

// Java AgentCall returns null for a failed agent (TcpAgentWorker closes the
// session), so the group handlers write {objHash} and go on with the rest.
func TestAgentReadFanOutSkipsFailedAgent(t *testing.T) {
	var (
		linesMu sync.Mutex
		lines   []string
	)
	restore := agentRelaySkipLogf
	agentRelaySkipLogf = func(format string, args ...any) {
		linesMu.Lock()
		defer linesMu.Unlock()
		lines = append(lines, format)
	}
	defer func() { agentRelaySkipLogf = restore }()
	before := agentRelaySkips.Load()
	for _, cmd := range []string{"OBJECT_ACTIVE_SERVICE_LIST_GROUP", "OBJECT_ACTIVE_SERVICE_LIST", "OBJECT_BATCH_ACTIVE_LIST"} {
		t.Run(cmd, func(t *testing.T) {
			objects := []*protocol.ObjectPack{{ObjHash: 1, ObjType: "java", Alive: true}, {ObjHash: 2, ObjType: "java", Alive: true}, {ObjHash: 3, ObjType: "java", Alive: true}}
			b := &agentFanOutFake{fakeBackend: &fakeBackend{objects: objects}, errs: map[int32]error{2: io.ErrUnexpectedEOF}}
			_, conn, r := startService(t, queryConfig(), b)
			session := loginSession(t, conn, r)
			p := newMap(entry("objType", stringValue("java")))
			if cmd == "OBJECT_ACTIVE_SERVICE_LIST_GROUP" {
				p = newMap(entry("objHash", list([]protocol.Value{decimal(1), decimal(2), decimal(3)})))
			}
			send(t, conn, cmd, session, p)
			packs := readPacks(t, r)
			if got := b.snapshot(); !reflect.DeepEqual(got, []int32{1, 2, 3}) {
				t.Fatal("calls", got)
			}
			if len(packs) != 3 {
				t.Fatal("packs", packs)
			}
			for i, hash := range []int32{1, 2, 3} {
				m := packs[i].(*protocol.MapPack)
				if mapValue(m, "objHash").Int != int64(hash) {
					t.Fatal(i, m)
				}
				if hash == 2 {
					if len(m.Entries) != 1 {
						t.Fatal("failed agent left more than the placeholder", m)
					}
				} else if mapValue(m, "answer").Int != int64(hash)*10 {
					t.Fatal(i, m)
				}
			}
		})
	}
	if got := agentRelaySkips.Load() - before; got != 3 {
		t.Fatal("skips", got)
	}
	// One line per minute: the later two skips are only counted.
	linesMu.Lock()
	defer linesMu.Unlock()
	if len(lines) > 1 {
		t.Fatal("skip log not rate-limited", lines)
	}
}

// A request-wide error still ends the fan-out (and a single-target relay): a
// cancelled request, a Go relay size limit and a backend without agent calls.
func TestAgentReadFanOutStopsOnRequestErrors(t *testing.T) {
	group := func() *protocol.MapPack {
		return newMap(entry("objHash", list([]protocol.Value{decimal(1), decimal(2), decimal(3)})))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := &agentFanOutFake{fakeBackend: &fakeBackend{}, cancel: map[int32]context.CancelFunc{1: cancel}}
	s := &Service{backend: b}
	if err := s.agentReadQuery(ctx, "OBJECT_ACTIVE_SERVICE_LIST_GROUP", group(), nil); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel", err)
	}
	if got := b.snapshot(); !reflect.DeepEqual(got, []int32{1}) {
		t.Fatal("calls after cancel", got)
	}
	for _, failure := range []error{transport.ErrLimit, transport.ErrUnsupported} {
		b := &agentFanOutFake{fakeBackend: &fakeBackend{}, errs: map[int32]error{1: failure}}
		s := &Service{backend: b}
		if err := s.agentReadQuery(context.Background(), "OBJECT_ACTIVE_SERVICE_LIST_GROUP", group(), nil); !errors.Is(err, failure) {
			t.Fatal(failure, err)
		}
		if got := b.snapshot(); !reflect.DeepEqual(got, []int32{1}) {
			t.Fatal("calls after", failure, got)
		}
	}
	for _, failure := range []error{transport.ErrLimit, transport.ErrUnsupported} {
		b = &agentFanOutFake{fakeBackend: &fakeBackend{}, errs: map[int32]error{42: failure}}
		s = &Service{backend: b}
		if err := s.agentReadQuery(context.Background(), "OBJECT_ACTIVE_SERVICE_LIST", newMap(entry("objHash", decimal(42))), nil); !errors.Is(err, failure) {
			t.Fatal("single target", failure, err)
		}
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	b = &agentFanOutFake{fakeBackend: &fakeBackend{}, cancel: map[int32]context.CancelFunc{42: cancel}}
	s = &Service{backend: b}
	if err := s.agentReadQuery(ctx, "HOST_TOP", newMap(entry("objHash", decimal(42))), nil); !errors.Is(err, context.Canceled) {
		t.Fatal("single target cancel", err)
	}
}
