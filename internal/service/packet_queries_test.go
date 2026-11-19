package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
	"io"
	"net"
	"testing"
	"time"
)

type packetQueryBackend struct {
	*extendedCounterBackend
	statuses     []*protocol.StatusPack
	alerts       []*protocol.AlertPack
	realtime     AlertResult
	summaries    []*protocol.SummaryPack
	stacks       []*protocol.StackPack
	batches      []*protocol.BatchPack
	batch        *protocol.BatchPack
	interactions map[int32][]*protocol.InteractionPerfCounterPack
	reply        []protocol.Pack
	group        *protocol.MapPack
	hashes       []int32
	from, to     int64
}

func (b *packetQueryBackend) RealtimeServiceGroup(ctx context.Context, hashes []int32) (*protocol.MapPack, error) {
	b.hashes = append([]int32{}, hashes...)
	return b.group, b.err
}

func (b *packetQueryBackend) StatusHistory(ctx context.Context, date string, from, to int64) ([]*protocol.StatusPack, error) {
	b.from, b.to = from, to
	return b.statuses, b.err
}
func (b *packetQueryBackend) Status(ctx context.Context, hash int32, key string) (*protocol.StatusPack, error) {
	return nil, b.err
}
func (b *packetQueryBackend) AlertsRealtime(ctx context.Context, q AlertQuery) (AlertResult, error) {
	return b.realtime, b.err
}

// scanSlice feeds a fake result to a streaming visit like the storage scans.
func scanSlice[T any](records []T, err error, visit func(T) (bool, error)) error {
	if err != nil {
		return err
	}
	for _, r := range records {
		if more, err := visit(r); err != nil || !more {
			return err
		}
	}
	return nil
}
func (b *packetQueryBackend) ScanAlertHistory(ctx context.Context, date string, from, to int64, visit func(*protocol.AlertPack) (bool, error)) error {
	return scanSlice(b.alerts, b.err, visit)
}
func (b *packetQueryBackend) ScanSummaries(ctx context.Context, date string, from, to int64, kind byte, visit func(*protocol.SummaryPack) (bool, error)) error {
	return scanSlice(b.summaries, b.err, visit)
}
func (b *packetQueryBackend) Stacks(ctx context.Context, name string, from, to int64) ([]*protocol.StackPack, error) {
	b.from, b.to = from, to
	return b.stacks, b.err
}
func (b *packetQueryBackend) Batches(ctx context.Context, name string, from, to int64) ([]*protocol.BatchPack, error) {
	return b.batches, b.err
}
func (b *packetQueryBackend) BatchAt(ctx context.Context, name string, when, pos int64) (*protocol.BatchPack, error) {
	b.from, b.to = when, pos
	return b.batch, b.err
}
func (b *packetQueryBackend) InteractionCounters(ctx context.Context, hash int32) ([]*protocol.InteractionPerfCounterPack, error) {
	return b.interactions[hash], b.err
}
func (b *packetQueryBackend) AgentRequest(ctx context.Context, hash int32, cmd string, p *protocol.MapPack) ([]protocol.Pack, error) {
	return b.reply, b.err
}
func startPacketQuery(t *testing.T, b Backend) (*Service, net.Conn, *bufio.Reader, <-chan error) {
	t.Helper()
	s, err := New(queryConfig(), b)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 32)
	h := transport.TCPHandlers{Authorize: func(context.Context, transport.ClientRequest) (bool, error) { return true, nil }, Client: func(ctx context.Context, r transport.ClientRequest, st *transport.Stream) error {
		ok, err := s.HandlePacketCommand(ctx, r, st)
		if !ok && err == nil {
			err = transport.ErrUnsupported
		}
		results <- err
		return err
	}}
	server, err := transport.NewTCPServer(l, transport.TCPConfig{}, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(3 * time.Second))
	var magic [4]byte
	binary.BigEndian.PutUint32(magic[:], transport.TCPClient)
	c.Write(magic[:])
	t.Cleanup(func() {
		c.Close()
		cancel()
		server.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("shutdown timeout")
		}
	})
	return s, c, bufio.NewReader(c), results
}
func TestPacketStatusAroundReturnsFirstForwardMatch(t *testing.T) {
	b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{}, statuses: []*protocol.StatusPack{{Time: 10, ObjHash: 1, Key: "k", Data: protocol.Value{Type: protocol.ValueMap}}, {Time: 11, ObjHash: 42, Key: "k", Data: protocol.Value{Type: protocol.ValueMap}}, {Time: 12, ObjHash: 42, Key: "k", Data: protocol.Value{Type: protocol.ValueMap}}}}
	_, c, r, _ := startPacketQuery(t, b)
	send(t, c, "STATUS_AROUND_VALUE", 1, newMap(entry("objHash", decimal(42)), entry("key", text("k")), entry("time", decimal(10))))
	ps := readPacks(t, r)
	if len(ps) != 1 || ps[0].(*protocol.StatusPack).Time != 11 || b.from != 10 || b.to != 60010 {
		t.Fatalf("forward first %#v range %d..%d", ps, b.from, b.to)
	}
}
func TestPacketAlertCursorFirstAndOriginalCountPlusOne(t *testing.T) {
	alerts := []*protocol.AlertPack{{Title: "a", Tags: protocol.Value{Type: protocol.ValueMap}}, {Title: "b", Tags: protocol.Value{Type: protocol.ValueMap}}, {Title: "c", Tags: protocol.Value{Type: protocol.ValueMap}}}
	b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{}, alerts: alerts, realtime: AlertResult{Loop: 2, Index: 7, Records: alerts}}
	_, c, r, _ := startPacketQuery(t, b)
	send(t, c, "ALERT_REAL_TIME", 1, newMap(entry("first", boolean(true))))
	ps := readPacks(t, r)
	if len(ps) != 1 || lookup(ps[0].(*protocol.MapPack), "index").Int != 7 {
		t.Fatal("first emits cursor only")
	}
	send(t, c, "ALERT_LOAD_TIME", 1, newMap(entry("date", text("20260101")), entry("stime", decimal(1767225600000)), entry("etime", decimal(1767225660000)), entry("count", decimal(1))))
	if ps = readPacks(t, r); len(ps) != 2 {
		t.Fatalf("original count+1, got %d", len(ps))
	}
}
func TestPacketStackIndexIsRawInt64AndHourFallback(t *testing.T) {
	b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{}, stacks: []*protocol.StackPack{{Time: 123, ObjHash: 42, Data: []byte("opaque")}}}
	_, c, r, _ := startPacketQuery(t, b)
	send(t, c, "GET_STACK_INDEX", 1, newMap(entry("objName", text("/app")), entry("date", text("20260101")), entry("hour", decimal(3))))
	f, err := r.ReadByte()
	if err != nil || f != transport.HasNext {
		t.Fatalf("flag %x %v", f, err)
	}
	var raw [8]byte
	if _, err = io.ReadFull(r, raw[:]); err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint64(raw[:]) != 123 {
		t.Fatal("raw int64")
	}
	r.ReadByte()
	if b.to-b.from != 3600000 {
		t.Fatal("hour fallback")
	}
}
func TestPacketBatchListSimpleBlobAndArchiveUnsupported(t *testing.T) {
	batch := &protocol.BatchPack{StartTime: 100, ObjHash: 42, BatchJobID: "job", ElapsedTime: 1000, Position: 9, SQLTotalCount: 0}
	b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{objects: map[string][]*protocol.ObjectPack{"": {{ObjHash: 42, ObjName: "/batch"}}}}, batches: []*protocol.BatchPack{batch}}
	_, c, r, _ := startPacketQuery(t, b)
	send(t, c, "BATCH_HISTORY_LIST", 1, newMap(entry("objHash", decimal(42)), entry("from", decimal(100)), entry("to", decimal(1100)), entry("response", decimal(1000))))
	f, err := r.ReadByte()
	if err != nil || f != transport.HasNext {
		t.Fatalf("flag %x %v", f, err)
	}
	v, err := protocol.ReadValue(io.MultiReader(bytes.NewReader([]byte{protocol.ValueBlob}), r), protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	e := protocol.NewEncoder(protocol.Limits{})
	e.Blob(v.Bytes)
	simple, _ := e.Bytes()
	p, err := protocol.DecodeBatchSimple(simple, protocol.Limits{})
	if err != nil || p.Position != 9 || p.BatchJobID != "job" {
		t.Fatalf("simple %#v %v", p, err)
	}
	if f, _ := r.ReadByte(); f != transport.NoNext {
		t.Fatal("terminator")
	}
	_, c2, _, results := startPacketQuery(t, b)
	send(t, c2, "BATCH_HISTORY_STACK", 1, newMap(entry("objHash", decimal(42)), entry("time", decimal(100)), entry("filename", text("synthetic.zip"))))
	if err := <-results; !errors.Is(err, transport.ErrUnsupported) {
		t.Fatalf("archive unsupported %v", err)
	}
}
func TestPacketInteractionExplicitInactiveDuplicateSelector(t *testing.T) {
	b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{}, interactions: map[int32][]*protocol.InteractionPerfCounterPack{42: {{ObjName: "/app", FromHash: 42, ToHash: 9, Count: 3, CustomData: protocol.Value{Type: protocol.ValueMap}}}}}
	_, c, r, _ := startPacketQuery(t, b)
	send(t, c, "INTR_COUNTER_REAL_TIME_BY_OBJ", 1, newMap(entry("objHash", list([]protocol.Value{decimal(42), decimal(42), decimal(9)}))))
	ps := readPacks(t, r)
	if len(ps) != 2 || ps[0].(*protocol.InteractionPerfCounterPack).Count != 3 {
		t.Fatal("Pack response/duplicate order")
	}
}

func TestPacketRealtimeGroupAndEmptyResult(t *testing.T) {
	b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{}, group: newMap(entry("name", list([]protocol.Value{text("checkout")})), entry("count", list([]protocol.Value{decimal(3)})))}
	_, c, r, _ := startPacketQuery(t, b)
	send(t, c, "REALTIME_SERVICE_GROUP", 1, newMap(entry("objHash", list([]protocol.Value{decimal(42), decimal(42)}))))
	ps := readPacks(t, r)
	if len(ps) != 1 || lookup(ps[0].(*protocol.MapPack), "count").Values[0].Int != 3 || len(b.hashes) != 2 {
		t.Fatal("group backend request and response")
	}
	b.group = nil
	send(t, c, "REALTIME_SERVICE_GROUP", 1, newMap())
	if len(readPacks(t, r)) != 0 {
		t.Fatal("nil group is empty")
	}
}

func TestPacketBatchDetailAndRelayShapes(t *testing.T) {
	base := &extendedCounterBackend{objects: map[string][]*protocol.ObjectPack{"": {{ObjHash: 42, ObjName: "/batch"}}}}
	b := &packetQueryBackend{extendedCounterBackend: base, batch: &protocol.BatchPack{StartTime: 100, ObjHash: 42, BatchJobID: "job", ElapsedTime: 7}}
	_, c, r, _ := startPacketQuery(t, b)
	send(t, c, "BATCH_HISTORY_DETAIL", 1, newMap(entry("objHash", decimal(42)), entry("startTime", decimal(100)), entry("elapsedTime", decimal(7)), entry("position", decimal(9))))
	ps := readPacks(t, r)
	if len(ps) != 1 || ps[0].(*protocol.BatchPack).BatchJobID != "job" || b.from != 107 || b.to != 9 {
		t.Fatal("batch detail date and position")
	}
	send(t, c, "OBJECT_BATCH_ACTIVE_LIST", 1, newMap(entry("objHash", decimal(42))))
	ps = readPacks(t, r)
	if len(ps) != 1 || len(ps[0].(*protocol.MapPack).Entries) != 1 || lookup(ps[0].(*protocol.MapPack), "objHash").Int != 42 {
		t.Fatal("unavailable agent emits object placeholder")
	}
	send(t, c, "BATCH_ACTIVE_STACK", 1, newMap(entry("objHash", decimal(42))))
	if len(readPacks(t, r)) != 0 {
		t.Fatal("unavailable stack is empty")
	}
}

func TestPacketRelayDoesNotMutateAgentMapAndRejectsWrongPack(t *testing.T) {
	original := newMap(entry("objHash", decimal(9)), entry("job", text("synthetic")))
	b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{}, reply: []protocol.Pack{original}}
	_, c, r, _ := startPacketQuery(t, b)
	send(t, c, "OBJECT_BATCH_ACTIVE_LIST", 1, newMap(entry("objHash", decimal(42))))
	ps := readPacks(t, r)
	if lookup(ps[0].(*protocol.MapPack), "objHash").Int != 42 || lookup(original, "objHash").Int != 9 {
		t.Fatal("reply ownership")
	}
	bad := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{}, reply: []protocol.Pack{&protocol.ObjectPack{}}}
	_, c2, _, results := startPacketQuery(t, bad)
	send(t, c2, "BATCH_ACTIVE_STACK", 1, newMap(entry("objHash", decimal(42))))
	if err := <-results; !errors.Is(err, transport.ErrMalformed) {
		t.Fatalf("wrong relay body %v", err)
	}
}

func TestPacketBoundariesAndBackendFailure(t *testing.T) {
	s, err := New(queryConfig(), &extendedCounterBackend{})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.HandlePacketCommand(context.Background(), transport.ClientRequest{Command: "unimplemented"}, nil); ok || err != nil {
		t.Fatal("unknown dispatcher")
	}
	if ok, err := s.HandlePacketCommand(context.Background(), transport.ClientRequest{Command: "GET_STACK_INDEX"}, nil); !ok || !errors.Is(err, transport.ErrUnauthorized) {
		t.Fatal("unauthorized")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.HandlePacketCommand(ctx, transport.ClientRequest{Command: "GET_STACK_INDEX", Authenticated: true}, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel before body")
	}
	backendErr := errors.New("synthetic storage failure")
	b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{err: backendErr}}
	_, c, _, results := startPacketQuery(t, b)
	send(t, c, "GET_STACK_ANALYZER", 1, newMap(entry("objName", text("/app")), entry("from", decimal(1)), entry("to", decimal(2))))
	if err := <-results; !errors.Is(err, backendErr) {
		t.Fatalf("backend error %v", err)
	}
}
