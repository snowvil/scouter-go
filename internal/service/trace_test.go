package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type traceFake struct {
	*fakeBackend
	byID  map[int64]*protocol.XLogPack
	byGX  map[string][]*protocol.XLogPack
	spans []*protocol.SpanPack
	raw   map[string][]*protocol.XLogPack
	err   error
}

func (b *traceFake) XLog(_ context.Context, date string, id int64) (*protocol.XLogPack, error) {
	return b.byID[id], b.err
}
func (b *traceFake) XLogsByGxID(_ context.Context, date string, gxid int64) ([]*protocol.XLogPack, error) {
	return b.byGX[date], b.err
}
func (b *traceFake) SpansByGxID(_ context.Context, date string, gxid int64) ([]*protocol.SpanPack, error) {
	return b.spans, b.err
}
func (b *traceFake) ScanXLogsByTime(_ context.Context, date string, from, to int64, visit func(*protocol.XLogPack) (bool, error)) error {
	if b.err != nil {
		return b.err
	}
	for _, x := range b.raw[date] {
		if more, err := visit(x); err != nil || !more {
			return err
		}
	}
	return nil
}
func startTraceService(t *testing.T, b Backend, configs ...Config) (*Service, net.Conn, *bufio.Reader) {
	t.Helper()
	config := queryConfig()
	if len(configs) > 0 {
		config = configs[0]
	}
	s, err := New(config, b)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handlers()
	base := h.Client
	h.Client = func(ctx context.Context, r transport.ClientRequest, stream *transport.Stream) error {
		switch r.Command {
		case "XLOG_READ_BY_GXID", "XLOG_LOAD_BY_TXIDS", "XLOG_LOAD_BY_GXID", "QUICKSEARCH_XLOG_LIST", "SEARCH_XLOG_LIST":
			return s.HandleTraceCommand(ctx, r, stream)
		}
		return base(ctx, r, stream)
	}
	server, err := transport.NewTCPServer(l, transport.TCPConfig{}, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(2 * time.Second))
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
			t.Error("shutdown timeout")
		}
	})
	return s, conn, bufio.NewReader(conn)
}
func simpleSpan(id, caller, stamp int64, kind byte) *protocol.SpanPack {
	return &protocol.SpanPack{GxID: 7, TxID: id, Caller: caller, Timestamp: stamp, SpanType: kind, Name: 7, ObjHash: 42, Elapsed: 200, AnnotationTimestamps: list([]protocol.Value{number(stamp)}), AnnotationValues: list([]protocol.Value{text("synthetic")}), Tags: protocol.Value{Type: protocol.ValueMap}}
}
func TestSpanBuilderHierarchySharedIDsAndTies(t *testing.T) {
	root := simpleSpan(100, 0, 1000, 1)
	api := simpleSpan(200, 100, 1020, 0)
	api.Tags.Entries = []protocol.Entry{entry("http.url", text("https://example.invalid/synthetic"))}
	spans := []*protocol.SpanPack{root, simpleSpan(100, 0, 1000, 0), simpleSpan(10, 100, 1010, 99), simpleSpan(11, 100, 1010, 99), api, simpleSpan(200, 100, 1020, 1), simpleSpan(12, 200, 1030, 99)}
	steps, owner, err := BuildSpanProfile(context.Background(), 7, 100, spans)
	if err != nil || len(steps) != 3 || owner != root {
		t.Fatal(steps, owner, err)
	}
	if steps[0].(*protocol.SpanStep).Parent != -1 || steps[1].(*protocol.SpanStep).StartTime != 10 {
		t.Fatal(steps)
	}
	call := steps[2].(*protocol.SpanCallStep)
	if call.TxID != 200 || call.Parent != 0 || call.Index != 2 || call.StartTime != 20 || call.Opt != 1 || call.Address != "https://example.invalid/synthetic" || len(call.AnnotationValues.Values) != 1 {
		t.Fatal(call)
	}
}
func TestTraceSelectorsOrderAndDuplicates(t *testing.T) {
	b := &traceFake{fakeBackend: &fakeBackend{}, byID: map[int64]*protocol.XLogPack{1: {TxID: 1, Caller: 77}}, byGX: map[string][]*protocol.XLogPack{"20261013": {{TxID: 2, Caller: 88}, {TxID: 1, Caller: 77}}, "20261014": {{TxID: 3}}}}
	_, conn, r := startTraceService(t, b)
	session := loginSession(t, conn, r)
	for _, tc := range []struct {
		cmd string
		p   *protocol.MapPack
		ids []int64
	}{{"XLOG_READ_BY_GXID", mp(entry("date", text("20261013")), entry("gxid", number(7))), []int64{2, 1}}, {"XLOG_LOAD_BY_TXIDS", mp(entry("date", text("20261013")), entry("txid", list([]protocol.Value{number(1), number(99), number(1)}))), []int64{1, 1}}, {"QUICKSEARCH_XLOG_LIST", mp(entry("date", text("20261013")), entry("txid", number(1)), entry("gxid", number(7))), []int64{1, 2, 1}}, {"XLOG_LOAD_BY_GXID", mp(entry("stime", number(time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC).UnixMilli())), entry("etime", number(time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC).UnixMilli())), entry("gxid", number(7))), []int64{2, 1, 3}}} {
		send(t, conn, tc.cmd, session, tc.p)
		packs := readPacks(t, r)
		var ids []int64
		for _, p := range packs {
			ids = append(ids, p.(*protocol.XLogPack).TxID)
		}
		if !reflect.DeepEqual(ids, tc.ids) {
			t.Fatalf("%s %v", tc.cmd, ids)
		}
	}
}
func TestTraceTxIDParentRewriteAndFallback(t *testing.T) {
	b := &traceFake{fakeBackend: &fakeBackend{}, byID: map[int64]*protocol.XLogPack{1: {TxID: 1, Caller: 9, GxID: 7, B3Mode: true}, 2: {TxID: 2, GxID: 7}}, spans: []*protocol.SpanPack{simpleSpan(9, 2, 1000, 99), simpleSpan(2, 0, 1000, 1)}}
	_, conn, r := startTraceService(t, b)
	session := loginSession(t, conn, r)
	for _, id := range []int64{1, 9} {
		send(t, conn, "XLOG_READ_BY_TXID", session, mp(entry("date", text("20261013")), entry("txid", number(id)), entry("gxid", number(7))))
		x := readPacks(t, r)[0].(*protocol.XLogPack)
		if id == 1 && x.Caller != 2 || id == 9 && x.TxID != 2 {
			t.Fatal(x)
		}
	}
	if b.byID[1].Caller != 9 {
		t.Fatal("backend snapshot mutated")
	}
}
func TestTraceSearchRawZeroElapsedAndMissingVersusEmpty(t *testing.T) {
	stamp := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC).UnixMilli()
	b := &traceFake{fakeBackend: &fakeBackend{texts: map[int32]string{7: "syntheticService"}}, raw: map[string][]*protocol.XLogPack{"20261013": {{TxID: 1, EndTime: stamp, Elapsed: 0, Service: 7, Text1: "abc-mid-end"}, {TxID: 2, EndTime: stamp + 1, Service: 8}}}}
	_, conn, r := startTraceService(t, b)
	session := loginSession(t, conn, r)
	base := []protocol.Entry{entry("stime", number(stamp)), entry("etime", number(stamp+100))}
	send(t, conn, "SEARCH_XLOG_LIST", session, mp(append(base, entry("service", text("synthetic*")), entry("text1", text("abc*end")))...))
	packs := readPacks(t, r)
	if len(packs) != 1 || packs[0].(*protocol.XLogPack).TxID != 1 {
		t.Fatal(packs)
	}
	send(t, conn, "SEARCH_XLOG_LIST", session, mp(append(base, entry("service", text("")))...))
	if packs := readPacks(t, r); len(packs) != 0 {
		t.Fatal("empty pattern treated as absent")
	}
}
func TestSpanBuilderMissingCycleAndCancellation(t *testing.T) {
	if _, _, err := BuildSpanProfile(context.Background(), 7, 100, nil); !errors.Is(err, protocol.ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := BuildSpanProfile(ctx, 7, 100, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestTraceSearchConfiguredLimitAndTxIDAttemptLimit(t *testing.T) {
	stamp := queryConfig().Clock().UnixMilli()
	b := &traceFake{fakeBackend: &fakeBackend{}, byID: map[int64]*protocol.XLogPack{1: {TxID: 1}, 2: {TxID: 2}, 3: {TxID: 3}}, raw: map[string][]*protocol.XLogPack{"20261013": {{TxID: 1, EndTime: stamp}, {TxID: 2, EndTime: stamp + 1}, {TxID: 3, EndTime: stamp + 2}}}}
	config := queryConfig()
	config.SearchXLogMaxCount = 2
	_, conn, r := startTraceService(t, b, config)
	session := loginSession(t, conn, r)
	send(t, conn, "SEARCH_XLOG_LIST", session, mp(entry("stime", number(stamp)), entry("etime", number(stamp+100))))
	if got := readPacks(t, r); len(got) != 2 {
		t.Fatal(len(got))
	}
	send(t, conn, "XLOG_LOAD_BY_TXIDS", session, mp(entry("date", text("20261013")), entry("txid", list([]protocol.Value{number(1), number(2), number(3)}))))
	if got := readPacks(t, r); len(got) != 1 {
		t.Fatal(len(got))
	}
}
func TestTraceProfileBranchPrecedenceAndSpanWire(t *testing.T) {
	chunk, err := protocol.EncodeStep(&protocol.StepControl{Message: "normal"}, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	b := &traceFake{fakeBackend: &fakeBackend{chunks: [][]byte{chunk}}, byID: map[int64]*protocol.XLogPack{1: {TxID: 1, GxID: 7, B3Mode: true}, 100: {TxID: 100, GxID: 7, XType: 5}}, spans: []*protocol.SpanPack{simpleSpan(100, 0, 1000, 1), simpleSpan(200, 100, 1020, 0)}}
	_, conn, r := startTraceService(t, b)
	session := loginSession(t, conn, r)
	send(t, conn, "TRANX_PROFILE", session, mp(entry("txid", number(1)), entry("xlogType", number(5))))
	normal := readPacks(t, r)[0].(*protocol.ProfilePack)
	if normal.TxID != 0 || len(normal.Profile) != len(chunk) {
		t.Fatal(normal)
	}
	send(t, conn, "TRANX_PROFILE", session, mp(entry("txid", number(100)), entry("max", number(1))))
	span := readPacks(t, r)[0].(*protocol.ProfilePack)
	if span.TxID != 100 || span.Service != 7 || span.ObjHash != 42 || span.Time != 0 {
		t.Fatal(span)
	}
	steps, err := protocol.DecodeSteps(span.Profile, protocol.Limits{})
	if err != nil || len(steps) != 2 {
		t.Fatal(steps, err)
	}
	send(t, conn, "TRANX_PROFILE_FULL", session, mp(entry("txid", number(100)), entry("max", number(1))))
	flag, err := r.ReadByte()
	if err != nil || flag != transport.HasNext {
		t.Fatal(flag, err)
	}
	full := readBlob(t, r)
	if !reflect.DeepEqual(full, span.Profile) {
		t.Fatal("span full differs")
	}
	flag, err = r.ReadByte()
	if err != nil || flag != transport.NoNext {
		t.Fatal(flag, err)
	}
}
func TestTraceMatchPinnedStarAndEmptySemantics(t *testing.T) {
	for _, tc := range []struct {
		p, target string
		want      bool
	}{{"*", "", false}, {"*", "x", true}, {"", "x", false}, {"a*b*c", "axbyc", false}, {"a*b*c", "axxb*c", true}, {"*one*two*", "twoxone", false}, {"*one*two*", "onetwo", true}, {"시작*끝", "시작중간끝", true}} {
		if got := traceMatch(tc.p, tc.target); got != tc.want {
			t.Fatalf("%q %q %v", tc.p, tc.target, got)
		}
	}
}

func TestTraceScalaSpanOracle(t *testing.T) {
	if _, err := exec.LookPath("javac"); err != nil {
		t.Skip("JDK unavailable: actual Scala oracle not executed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	dir := t.TempDir()
	cmd := exec.CommandContext(ctx, "python3", "../../scripts/java-storage.py", "--generate-spans", dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("actual Scala oracle: %v\n%s", err, output)
	}
	for _, name := range []string{"span-simple", "span-shared"} {
		input, err := os.ReadFile(filepath.Join(dir, name+"-input.bin"))
		if err != nil {
			t.Fatal(err)
		}
		expected, err := os.ReadFile(filepath.Join(dir, name+"-steps.bin"))
		if err != nil {
			t.Fatal(err)
		}
		pack, err := protocol.DecodePack(input, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		container := pack.(*protocol.SpanContainerPack)
		reader := bytes.NewReader(container.Spans)
		var spans []*protocol.SpanPack
		for reader.Len() > 0 {
			pack, err := protocol.ReadPack(reader, protocol.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			spans = append(spans, pack.(*protocol.SpanPack))
		}
		steps, _, err := BuildSpanProfile(ctx, container.GxID, 100, spans)
		if err != nil {
			t.Fatal(err)
		}
		got, err := protocol.EncodeSteps(steps, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("%s original Scala bytes differ: got=%x want=%x", name, got, expected)
		}
	}
}

func TestTraceIPEmptyBlobUsesPinnedIPv4Fallback(t *testing.T) {
	stamp := queryConfig().Clock().UnixMilli()
	b := &traceFake{fakeBackend: &fakeBackend{}, raw: map[string][]*protocol.XLogPack{"20261013": {{TxID: 1, EndTime: stamp}}}}
	_, conn, r := startTraceService(t, b)
	session := loginSession(t, conn, r)
	send(t, conn, "SEARCH_XLOG_LIST", session, mp(entry("stime", number(stamp)), entry("etime", number(stamp+100)), entry("ip", text("0.0.0.0"))))
	if got := readPacks(t, r); len(got) != 1 {
		t.Fatal("empty encoded IP was not Java 0.0.0.0", len(got))
	}
}
func TestTraceNormalSpanProfileMissingOwnerIsEmpty(t *testing.T) {
	b := &traceFake{fakeBackend: &fakeBackend{}, byID: map[int64]*protocol.XLogPack{100: {TxID: 100, GxID: 7, XType: 5}}, spans: []*protocol.SpanPack{simpleSpan(200, 0, 1000, 1)}}
	_, conn, r := startTraceService(t, b)
	session := loginSession(t, conn, r)
	send(t, conn, "TRANX_PROFILE", session, mp(entry("txid", number(100))))
	if got := readPacks(t, r); len(got) != 0 {
		t.Fatal(got)
	}
}

func TestTraceCancelCycleAndBackendError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s, err := New(queryConfig(), &fakeBackend{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.HandleTraceCommand(ctx, transport.ClientRequest{}, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.HandleTraceCommand(context.Background(), transport.ClientRequest{}, nil); !errors.Is(err, transport.ErrUnauthorized) {
		t.Fatal(err)
	}
	cyclic := []*protocol.SpanPack{simpleSpan(100, 0, 1000, 1), simpleSpan(100, 100, 1001, 0)}
	if _, _, err := BuildSpanProfile(context.Background(), 7, 100, cyclic); !errors.Is(err, protocol.ErrInvalid) {
		t.Fatal("cycle accepted", err)
	}
	b := &traceFake{fakeBackend: &fakeBackend{}, err: errors.New("synthetic backend error")}
	_, conn, r := startTraceService(t, b)
	session := loginSession(t, conn, r)
	send(t, conn, "XLOG_READ_BY_GXID", session, mp(entry("date", text("20261013")), entry("gxid", number(7))))
	if _, err := r.ReadByte(); err == nil {
		t.Fatal("backend error became success")
	}
}
func TestTraceSpanParentMissingVersusEmptyIndex(t *testing.T) {
	b := &traceFake{fakeBackend: &fakeBackend{}, byID: map[int64]*protocol.XLogPack{1: {TxID: 1, GxID: 7, Caller: 99, B3Mode: true}}, spans: []*protocol.SpanPack{}}
	_, conn, r := startTraceService(t, b)
	session := loginSession(t, conn, r)
	send(t, conn, "XLOG_READ_BY_TXID", session, mp(entry("date", text("20261013")), entry("txid", number(1))))
	got := readPacks(t, r)
	if len(got) != 1 || got[0].(*protocol.XLogPack).Caller != 0 {
		t.Fatal(got)
	}
}

type dateBoundTrace struct{ *traceFake }

func (b *dateBoundTrace) ScanXLogsByTime(ctx context.Context, date string, from, to int64, visit func(*protocol.XLogPack) (bool, error)) error {
	if time.UnixMilli(from).UTC().Format("20060102") != date || time.UnixMilli(to).UTC().Format("20060102") != date {
		return errors.New("raw query crossed date boundary")
	}
	return b.traceFake.ScanXLogsByTime(ctx, date, from, to, visit)
}
func TestTraceSearchOnlyEndpointDatesAcrossLongRange(t *testing.T) {
	from := queryConfig().Clock().UnixMilli()
	to := from + 3*86400000 + 100
	b := &dateBoundTrace{&traceFake{fakeBackend: &fakeBackend{}, raw: map[string][]*protocol.XLogPack{"20261013": {{TxID: 1, EndTime: from}}, "20261016": {{TxID: 2, EndTime: to}}, "20261014": {{TxID: 99, EndTime: from + 86400000}}}}}
	_, conn, r := startTraceService(t, b)
	session := loginSession(t, conn, r)
	send(t, conn, "SEARCH_XLOG_LIST", session, mp(entry("stime", number(from)), entry("etime", number(to))))
	got := readPacks(t, r)
	if len(got) != 2 || got[0].(*protocol.XLogPack).TxID != 1 || got[1].(*protocol.XLogPack).TxID != 2 {
		t.Fatal(got)
	}
}
