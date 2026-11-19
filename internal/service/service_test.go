package service

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

type fakeBackend struct {
	Backend
	objects        []*protocol.ObjectPack
	object         *protocol.ObjectPack
	texts          map[int32]string
	value          protocol.Value
	found          bool
	points         []CounterPoint
	daily          []protocol.Value
	realtime       RealtimeResult
	history        HistoryResult
	historyRecords []*protocol.XLogPack
	xlog           *protocol.XLogPack
	chunks         [][]byte
	calls          []string
}

func (b *fakeBackend) Objects(_ context.Context, date string) ([]*protocol.ObjectPack, error) {
	b.calls = append(b.calls, "objects:"+date)
	return b.objects, nil
}
func (b *fakeBackend) Object(_ context.Context, date string, hash int32) (*protocol.ObjectPack, error) {
	b.calls = append(b.calls, "object:"+date)
	return b.object, nil
}
func (b *fakeBackend) Text(_ context.Context, date, typ string, hash int32) (string, bool, error) {
	v, ok := b.texts[hash]
	return v, ok, nil
}
func (b *fakeBackend) Counter(_ context.Context, hash int32, counter string, tt byte) (protocol.Value, bool, error) {
	b.calls = append(b.calls, "counter:"+string(rune(tt)))
	return b.value, b.found, nil
}
func (b *fakeBackend) CounterHistory(_ context.Context, hash int32, counter string, from, to int64) ([]CounterPoint, error) {
	return b.points, nil
}
func (b *fakeBackend) CounterDaily(_ context.Context, date string, hash int32, counter string, tt byte) ([]protocol.Value, error) {
	return b.daily, nil
}
func (b *fakeBackend) XLogsRealtime(_ context.Context, q RealtimeQuery) (RealtimeResult, error) {
	return b.realtime, nil
}
func (b *fakeBackend) XLogsHistory(_ context.Context, q HistoryQuery, visit func(*protocol.XLogPack) error) (HistoryResult, error) {
	for _, x := range b.historyRecords {
		if err := visit(x); err != nil {
			return HistoryResult{}, err
		}
	}
	return b.history, nil
}
func (b *fakeBackend) XLog(_ context.Context, date string, txid int64) (*protocol.XLogPack, error) {
	return b.xlog, nil
}
func (b *fakeBackend) Profile(_ context.Context, date string, txid int64) ([][]byte, error) {
	return b.chunks, nil
}

func startService(t *testing.T, config Config, backend Backend) (*Service, net.Conn, *bufio.Reader) {
	t.Helper()
	service, err := New(config, backend)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := transport.NewTCPServer(listener, transport.TCPConfig{}, service.Handlers(), nil)
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
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	var magic [4]byte
	binary.BigEndian.PutUint32(magic[:], transport.TCPClient)
	if _, err := conn.Write(magic[:]); err != nil {
		t.Fatal(err)
	}
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
	return service, conn, bufio.NewReader(conn)
}

func send(t *testing.T, conn net.Conn, command string, session int64, pack protocol.Pack) {
	t.Helper()
	e := protocol.NewEncoder(protocol.Limits{})
	e.Text(command)
	e.Int64(session)
	if pack != nil {
		e.Pack(pack)
	}
	raw, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Write(raw); err != nil {
		t.Fatal(err)
	}
}

func readPacks(t *testing.T, r *bufio.Reader) []protocol.Pack {
	t.Helper()
	var packs []protocol.Pack
	for {
		flag, err := r.ReadByte()
		if err != nil {
			t.Fatalf("response flag: %v", err)
		}
		if flag == transport.NoNext {
			return packs
		}
		if flag != transport.HasNext {
			t.Fatalf("response flag %x", flag)
		}
		p, err := protocol.ReadPack(r, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		packs = append(packs, p)
	}
}

func mapValue(p *protocol.MapPack, key string) protocol.Value {
	for _, e := range p.Entries {
		if e.Key == key {
			return e.Value
		}
	}
	return protocol.Value{}
}
func text(s string) protocol.Value                   { return protocol.Value{Type: protocol.ValueText, Text: s} }
func number(n int64) protocol.Value                  { return protocol.Value{Type: protocol.ValueDecimal, Int: n} }
func mp(entries ...protocol.Entry) *protocol.MapPack { return &protocol.MapPack{Entries: entries} }
func encrypted(password string) string {
	sum := sha256.Sum256([]byte("qwertyuiop!@#$%^&*()zxcvbnm,." + password))
	return hex.EncodeToString(sum[:])
}

func TestLoginAndFailureWire(t *testing.T) {
	_, conn, r := startService(t, Config{Username: "synthetic", Password: "password", ServerID: "7"}, &fakeBackend{})
	send(t, conn, "LOGIN", 0, mp(protocol.Entry{Key: "id", Value: text("synthetic")}, protocol.Entry{Key: "pass", Value: text("wrong")}))
	bad := readPacks(t, r)[0].(*protocol.MapPack)
	if mapValue(bad, "session").Int != 0 || mapValue(bad, "error").Text != "login fail" {
		t.Fatalf("failure: %#v", bad)
	}
	send(t, conn, "LOGIN", 0, mp(protocol.Entry{Key: "id", Value: text("synthetic")}, protocol.Entry{Key: "pass", Value: text(encrypted("password"))}))
	ok := readPacks(t, r)[0].(*protocol.MapPack)
	if mapValue(ok, "session").Int == 0 || mapValue(ok, "server_id").Text != "7" {
		t.Fatalf("login: %#v", ok)
	}
	if mapValue(ok, "pass").Type != protocol.ValueNull {
		t.Fatal("credential echoed")
	}
}

func loginSession(t *testing.T, conn net.Conn, r *bufio.Reader) int64 {
	t.Helper()
	send(t, conn, "LOGIN", 0, mp(entry("id", text("synthetic")), entry("pass", text(encrypted("password")))))
	return mapValue(readPacks(t, r)[0].(*protocol.MapPack), "session").Int
}
func queryConfig() Config {
	return Config{Username: "synthetic", Password: "password", Location: time.UTC, Clock: func() time.Time { return time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC) }}
}

func TestBootstrapAndNoBodyQueries(t *testing.T) {
	backend := &fakeBackend{objects: []*protocol.ObjectPack{{ObjHash: 42, ObjType: "host", Tags: protocol.Value{Type: protocol.ValueMap}}}}
	config := queryConfig()
	config.CounterXML = []byte("<Counters/>")
	_, conn, r := startService(t, config, backend)
	session := loginSession(t, conn, r)
	// Consecutive no-body commands must never consume the following command as a Pack.
	send(t, conn, "GET_XML_COUNTER", session, nil)
	send(t, conn, "SERVER_STATUS", session, nil)
	send(t, conn, "OBJECT_LIST_REAL_TIME", session, nil)
	xml := readPacks(t, r)[0].(*protocol.MapPack)
	if !bytes.Equal(mapValue(xml, "default").Bytes, config.CounterXML) {
		t.Fatal("XML")
	}
	status := readPacks(t, r)[0].(*protocol.MapPack)
	if mapValue(status, "time").Int != config.Clock().UnixMilli() || mapValue(status, "total").Int < mapValue(status, "used").Int {
		t.Fatal("status")
	}
	objects := readPacks(t, r)
	if len(objects) != 1 || objects[0].(*protocol.ObjectPack).ObjHash != 42 {
		t.Fatal("objects")
	}
}
func TestSessionBoundsExpiryAndConcurrentTouch(t *testing.T) {
	now := time.Now()
	var clockMu sync.Mutex
	config := queryConfig()
	config.MaxSessions = 1
	config.SessionTTL = time.Minute
	config.Clock = func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	service, conn, r := startService(t, config, &fakeBackend{})
	first := loginSession(t, conn, r)
	if first == 0 || loginSession(t, conn, r) != 0 {
		t.Fatal("session capacity")
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				if ok, err := service.Authorize(context.Background(), transport.ClientRequest{Session: first}); err != nil || !ok {
					t.Errorf("authorize %v %v", ok, err)
				}
			}
		}()
	}
	wg.Wait()
	// A session held by a live connection never expires; once its connection
	// is gone it expires and frees its slot.
	other, otherR := secondClient(t, conn)
	conn.Close()
	waitSessionConns(t, service, first, 0)
	clockMu.Lock()
	now = now.Add(time.Minute + time.Millisecond)
	clockMu.Unlock()
	if ok, _ := service.Authorize(context.Background(), transport.ClientRequest{Session: first}); ok {
		t.Fatal("expired session")
	}
	if loginSession(t, other, otherR) == 0 {
		t.Fatal("expired entry not reclaimed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Authorize(ctx, transport.ClientRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := service.Handle(context.Background(), transport.ClientRequest{Command: "OBJECT_LIST_REAL_TIME"}, nil); !errors.Is(err, transport.ErrUnauthorized) {
		t.Fatal(err)
	}
}
func TestInvalidSessionWire(t *testing.T) {
	_, conn, r := startService(t, queryConfig(), &fakeBackend{})
	send(t, conn, "GET_XML_COUNTER", 123, nil)
	flag, err := r.ReadByte()
	if err != nil || flag != transport.InvalidSession {
		t.Fatalf("flag %x %v", flag, err)
	}
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}
func TestTextHashKeysAndMissingHash(t *testing.T) {
	backend := &fakeBackend{texts: map[int32]string{9: "nine", 10: "ten", -32: "negative", -2147483648: "min", 42: ""}}
	_, conn, r := startService(t, queryConfig(), backend)
	session := loginSession(t, conn, r)
	send(t, conn, "GET_TEXT", session, mp(entry("type", text("service"))))
	if packs := readPacks(t, r); len(packs) != 0 {
		t.Fatalf("missing hash %v", packs)
	}
	send(t, conn, "GET_TEXT", session, mp(entry("type", text("service")), entry("hash", list([]protocol.Value{number(9), number(10), number(-32), number(-2147483648), number(42), number(999)}))))
	got := readPacks(t, r)[0].(*protocol.MapPack)
	keys := []string{}
	for _, entry := range got.Entries {
		keys = append(keys, entry.Key)
	}
	if !reflect.DeepEqual(keys, []string{"9", "xa", "z10", "z2000000", "x1a"}) {
		t.Fatal(keys)
	}
	if mapValue(got, "x1a").Type != protocol.ValueText {
		t.Fatal("empty existing text omitted")
	}
}
func TestCounterValueAndHistory(t *testing.T) {
	backend := &fakeBackend{value: number(17), found: true, points: []CounterPoint{{Time: 1000, Value: number(17)}, {Time: 2000, Value: protocol.Value{Type: protocol.ValueFloat, Float: 1.5}}}}
	_, conn, r := startService(t, queryConfig(), backend)
	session := loginSession(t, conn, r)
	send(t, conn, "COUNTER_REAL_TIME", session, mp(entry("objHash", number(42)), entry("counter", text("synthetic")), entry("timetype", number(1))))
	flag, err := r.ReadByte()
	if err != nil || flag != transport.HasNext {
		t.Fatal(flag, err)
	}
	value, err := protocol.ReadValue(r, protocol.Limits{})
	if err != nil || value.Int != 17 {
		t.Fatal(value, err)
	}
	flag, err = r.ReadByte()
	if err != nil || flag != transport.NoNext {
		t.Fatal(flag, err)
	}
	send(t, conn, "COUNTER_PAST_TIME", session, mp(entry("objHash", number(42)), entry("counter", text("synthetic")), entry("stime", number(1000)), entry("etime", number(2000))))
	history := readPacks(t, r)[0].(*protocol.MapPack)
	if len(mapValue(history, "time").Values) != 2 || mapValue(history, "value").Values[1].Float != 1.5 {
		t.Fatal(history)
	}
}
func TestXLogCursorPlacement(t *testing.T) {
	record := &protocol.XLogPack{TxID: 11, EndTime: 1001}
	backend := &fakeBackend{realtime: RealtimeResult{Loop: 3, Index: 4, Records: []*protocol.XLogPack{record}}, historyRecords: []*protocol.XLogPack{record}, history: HistoryResult{HasMore: true, LastBucketTime: 1000, LastTxID: 11}}
	_, conn, r := startService(t, queryConfig(), backend)
	session := loginSession(t, conn, r)
	send(t, conn, "TRANX_REAL_TIME_GROUP", session, mp(entry("loop", number(0)), entry("index", number(0))))
	realtime := readPacks(t, r)
	if len(realtime) != 2 || mapValue(realtime[0].(*protocol.MapPack), "index").Int != 4 || realtime[1].(*protocol.XLogPack).TxID != 11 {
		t.Fatal(realtime)
	}
	send(t, conn, "TRANX_LOAD_TIME_GROUP_V2", session, mp(entry("date", text("20261013")), entry("stime", number(0)), entry("etime", number(2000)), entry("pageCount", number(1)), entry("objHash", list([]protocol.Value{number(42)}))))
	history := readPacks(t, r)
	if len(history) != 2 || history[0].(*protocol.XLogPack).TxID != 11 || mapValue(history[1].(*protocol.MapPack), "lastXLogTime").Int != 1000 || !mapValue(history[1].(*protocol.MapPack), "hasMore").Bool {
		t.Fatal(history)
	}
	send(t, conn, "TRANX_LOAD_TIME_GROUP_V2", session, mp(entry("date", text("20261013")), entry("stime", number(0)), entry("etime", number(2000)), entry("pageCount", number(1)), entry("lastBucketTime", number(1000)), entry("objHash", list([]protocol.Value{number(42)}))))
	invalid := readPacks(t, r)
	if len(invalid) != 1 || mapValue(invalid[0].(*protocol.MapPack), "hasMore").Bool {
		t.Fatal(invalid)
	}
}
func readBlob(t *testing.T, r *bufio.Reader) []byte {
	t.Helper()
	prefix, err := r.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	size := int(prefix)
	switch prefix {
	case 255:
		var b [2]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			t.Fatal(err)
		}
		size = int(binary.BigEndian.Uint16(b[:]))
	case 254:
		var b [4]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			t.Fatal(err)
		}
		size = int(binary.BigEndian.Uint32(b[:]))
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(r, raw); err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestOrderedProfileChunksAndTruncation(t *testing.T) {
	first, err := protocol.EncodeStep(&protocol.StepControl{Message: "first"}, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := protocol.EncodeStep(&protocol.StepControl{Message: "second"}, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeBackend{xlog: &protocol.XLogPack{TxID: 11}, chunks: [][]byte{first, second}}
	_, conn, r := startService(t, queryConfig(), backend)
	session := loginSession(t, conn, r)
	send(t, conn, "TRANX_PROFILE", session, mp(entry("txid", number(11)), entry("max", number(1))))
	profile := readPacks(t, r)[0].(*protocol.ProfilePack)
	if profile.Version2 || profile.TxID != 0 {
		t.Fatal(profile)
	}
	steps, err := protocol.DecodeSteps(profile.Profile, protocol.Limits{})
	if err != nil || len(steps) != 2 || steps[0].(*protocol.StepControl).Message != "first" || steps[1].(*protocol.StepControl).Message != " ** Profile Truncated ** " {
		t.Fatal(steps, err)
	}
	send(t, conn, "TRANX_PROFILE_FULL", session, mp(entry("txid", number(11)), entry("max", number(1))))
	for _, want := range backend.chunks {
		flag, err := r.ReadByte()
		if err != nil || flag != transport.HasNext {
			t.Fatal(flag, err)
		}
		if got := readBlob(t, r); !bytes.Equal(got, want) {
			t.Fatal(got)
		}
	}
	flag, err := r.ReadByte()
	if err != nil || flag != transport.NoNext {
		t.Fatal(flag, err)
	}
}

type agentFake struct {
	*fakeBackend
	gotHash    int32
	gotCommand string
	gotPack    *protocol.MapPack
	err        error
}

func (b *agentFake) AgentRequest(_ context.Context, hash int32, command string, p *protocol.MapPack) ([]protocol.Pack, error) {
	b.gotHash = hash
	b.gotCommand = command
	b.gotPack = p
	return []protocol.Pack{mp(entry("synthetic", text("env")))}, b.err
}
func TestAgentRelayAndError(t *testing.T) {
	backend := &agentFake{fakeBackend: &fakeBackend{}}
	_, conn, r := startService(t, queryConfig(), backend)
	session := loginSession(t, conn, r)
	send(t, conn, "OBJECT_ENV", session, mp(entry("objHash", number(42)), entry("extra", text("preserved"))))
	result := readPacks(t, r)[0].(*protocol.MapPack)
	if mapValue(result, "synthetic").Text != "env" || backend.gotHash != 42 || backend.gotCommand != "OBJECT_ENV" || mapValue(backend.gotPack, "extra").Text != "preserved" {
		t.Fatal("relay changed request")
	}
	// An agent failure is Java's null AgentCall result: AgentInfo.scala:113-117
	// writes nothing and the command ends normally.
	backend.err = errors.New("synthetic backend failure")
	send(t, conn, "OBJECT_ENV", session, mp(entry("objHash", number(42))))
	if packs := readPacks(t, r); len(packs) != 0 {
		t.Fatal("agent failure answered", packs)
	}
	// A Go relay limit still fails the request.
	backend.err = transport.ErrLimit
	send(t, conn, "OBJECT_ENV", session, mp(entry("objHash", number(42))))
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatal("relay limit became success", err)
	}
}
func TestUnsupportedAndMalformedClose(t *testing.T) {
	for _, tc := range []struct {
		command string
		body    protocol.Pack
	}{{"NOT_IMPLEMENTED", nil}, {"OBJECT_INFO", &protocol.TextPack{}}, {"COUNTER_REAL_TIME", mp(entry("objHash", number(1<<40)))}, {"GET_TEXT_ANY_TYPE", mp(entry("hash", list([]protocol.Value{number(42)})), entry("type", list(nil)))}} {
		t.Run(tc.command, func(t *testing.T) {
			_, conn, r := startService(t, queryConfig(), &fakeBackend{})
			session := loginSession(t, conn, r)
			send(t, conn, tc.command, session, tc.body)
			if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
		})
	}
}
func TestDailyCounterTimezoneAndNull(t *testing.T) {
	location, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	config := queryConfig()
	config.Location = location
	_, conn, r := startService(t, config, &fakeBackend{daily: []protocol.Value{number(1), {Type: protocol.ValueNull}, number(2)}})
	session := loginSession(t, conn, r)
	send(t, conn, "COUNTER_PAST_DATE", session, mp(entry("date", text("20261013")), entry("objHash", number(42)), entry("counter", text("synthetic"))))
	result := readPacks(t, r)[0].(*protocol.MapPack)
	start := time.Date(2026, 10, 13, 0, 0, 0, 0, location).UnixMilli()
	if mapValue(result, "time").Values[0].Int != start || mapValue(result, "time").Values[2].Int != start+600000 || mapValue(result, "value").Values[1].Type != protocol.ValueNull {
		t.Fatal(result)
	}
}
func TestBackThread2ProfileSupported(t *testing.T) {
	backend := &fakeBackend{xlog: &protocol.XLogPack{TxID: 11, XType: 4}, chunks: [][]byte{{}}}
	_, conn, r := startService(t, queryConfig(), backend)
	session := loginSession(t, conn, r)
	send(t, conn, "TRANX_PROFILE", session, mp(entry("txid", number(11)), entry("xlogType", number(4))))
	if result := readPacks(t, r); len(result) != 1 {
		t.Fatal(result)
	}
}

func TestDailyCounterPreservesJavaFixedDayDuringDST(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	config := queryConfig()
	config.Location = location
	config.Clock = func() time.Time { return time.Date(2026, 7, 13, 4, 30, 0, 0, time.UTC) }
	_, conn, r := startService(t, config, &fakeBackend{daily: []protocol.Value{number(1)}})
	session := loginSession(t, conn, r)
	send(t, conn, "COUNTER_TODAY", session, mp(entry("objHash", number(42)), entry("counter", text("synthetic")), entry("timetype", number(3))))
	result := readPacks(t, r)[0].(*protocol.MapPack)
	// Java retains January 2000's -05:00 base, so 04:30Z is still July 12.
	want := time.Date(2026, 7, 12, 5, 0, 0, 0, time.UTC).UnixMilli()
	if got := mapValue(result, "time").Values[0].Int; got != want {
		t.Fatalf("fixed midnight got=%d want=%d", got, want)
	}
}

func TestSpanParentRewriteFailsExplicitly(t *testing.T) {
	backend := &fakeBackend{xlog: &protocol.XLogPack{TxID: 11, B3Mode: true, Caller: 12, GxID: 13}}
	_, conn, r := startService(t, queryConfig(), backend)
	session := loginSession(t, conn, r)
	send(t, conn, "XLOG_READ_BY_TXID", session, mp(entry("date", text("20261013")), entry("txid", number(11))))
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatal("unsupported parent rewrite returned raw record", err)
	}
}

func TestTxIDMissingDateDoesNotSelectToday(t *testing.T) {
	backend := &fakeBackend{xlog: &protocol.XLogPack{TxID: 11}}
	_, conn, r := startService(t, queryConfig(), backend)
	session := loginSession(t, conn, r)
	send(t, conn, "XLOG_READ_BY_TXID", session, mp(entry("txid", number(11))))
	if result := readPacks(t, r); len(result) != 0 {
		t.Fatal("missing required date became today's result")
	}
}

// multiHistoryBackend reads COUNTER_PAST_TIME_ALL histories in one pass.
type multiHistoryBackend struct {
	*fakeBackend
	perHash map[int32][]CounterPoint
	passes  int
}

func (b *multiHistoryBackend) CounterHistory(_ context.Context, hash int32, _ string, _, _ int64) ([]CounterPoint, error) {
	return b.perHash[hash], nil
}
func (b *multiHistoryBackend) CounterHistoryMulti(_ context.Context, hashes []int32, _ string, _, _ int64, each func(int32, []CounterPoint) error) error {
	b.passes++
	for _, h := range hashes {
		if err := each(h, b.perHash[h]); err != nil {
			return err
		}
	}
	return nil
}

// COUNTER_PAST_TIME_ALL answers one pack per requested objHash in request
// order (empty lists for an object without points), the same through the
// one-pass backend as through per-object reads.
func TestCounterPastTimeAllOnePass(t *testing.T) {
	perHash := map[int32][]CounterPoint{7: {{Time: 1000, Value: number(1)}, {Time: 3000, Value: number(2)}}, 9: {{Time: 2000, Value: protocol.Value{Type: protocol.ValueFloat, Float: 1.5}}}}
	req := mp(entry("objHash", protocol.Value{Type: protocol.ValueList, Values: []protocol.Value{number(9), number(8), number(7)}}),
		entry("counter", text("TPS")), entry("stime", number(1000)), entry("etime", number(5000)))
	answer := func(backend Backend) []protocol.Pack {
		_, conn, r := startService(t, queryConfig(), backend)
		session := loginSession(t, conn, r)
		send(t, conn, "COUNTER_PAST_TIME_ALL", session, req)
		return readPacks(t, r)
	}
	multi := &multiHistoryBackend{fakeBackend: &fakeBackend{}, perHash: perHash}
	got := answer(multi)
	want := answer(&perObjectBackend{multi})
	if multi.passes != 1 || !reflect.DeepEqual(got, want) || len(got) != 3 {
		t.Fatal(multi.passes, got, want)
	}
	if h := mapValue(got[1].(*protocol.MapPack), "objHash").Int; h != 8 || len(mapValue(got[1].(*protocol.MapPack), "time").Values) != 0 {
		t.Fatal("object without points", got[1])
	}
}

// perObjectBackend hides CounterHistoryMulti (the per-object path).
type perObjectBackend struct{ *multiHistoryBackend }

func (b *perObjectBackend) CounterHistoryMulti() {}
