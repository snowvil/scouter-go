package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

type databaseBackend struct {
	*extendedCounterBackend
	statuses     []*protocol.StatusPack
	latest       map[int32]*protocol.StatusPack
	dailyValues  map[string]map[int32]map[string][]protocol.Value
	historyCalls int
}

func (b *databaseBackend) StatusHistory(ctx context.Context, date string, from, to int64) ([]*protocol.StatusPack, error) {
	b.historyCalls++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.statuses, b.err
}
func (b *databaseBackend) Status(ctx context.Context, hash int32, key string) (*protocol.StatusPack, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v := b.latest[hash]
	if v != nil && v.Key != key {
		v = nil
	}
	return v, b.err
}
func (b *databaseBackend) CounterDaily(ctx context.Context, date string, hash int32, name string, tt byte) ([]protocol.Value, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tt != 3 {
		return nil, errors.New("expected FIVE_MIN")
	}
	return b.dailyValues[date][hash][name], b.err
}
func startDatabaseQuery(t *testing.T, b Backend, configs ...Config) (*Service, net.Conn, *bufio.Reader, <-chan error) {
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
	results := make(chan error, 32)
	h := transport.TCPHandlers{Authorize: func(context.Context, transport.ClientRequest) (bool, error) { return true, nil }, Client: func(ctx context.Context, r transport.ClientRequest, st *transport.Stream) error {
		ok, err := s.HandleDatabaseCounterCommand(ctx, r, st)
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
	if _, err = c.Write(magic[:]); err != nil {
		t.Fatal(err)
	}
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
func dbHashes(ns ...int64) protocol.Entry {
	v := []protocol.Value{}
	for _, n := range ns {
		v = append(v, decimal(n))
	}
	return entry("objHash", list(v))
}
func dbStatus(hash int32, when int64, key string, es ...protocol.Entry) *protocol.StatusPack {
	return &protocol.StatusPack{ObjHash: hash, Time: when, Key: key, Data: protocol.Value{Type: protocol.ValueMap, Entries: es}}
}
func TestDatabaseRealtimeValueAndMissingContracts(t *testing.T) {
	for _, tc := range []struct {
		cmd    string
		values map[int32]map[string]protocol.Value
		want   []protocol.Entry
	}{
		{"DB_REALTIME_CONNECTIONS", map[int32]map[string]protocol.Value{1: {"THREADS_TOTAL": decimal(5), "THREADS_RUNNING": {Type: protocol.ValueDouble, Double: 2.9}}, 2: {"THREADS_TOTAL": text("7")}}, []protocol.Entry{entry("total", decimal(12)), entry("active", decimal(2))}},
		{"DB_REALTIME_ACTIVITY", map[int32]map[string]protocol.Value{1: {"COM_SELECT": decimal(3)}}, []protocol.Entry{entry("call", decimal(0)), entry("select", decimal(3)), entry("update", decimal(0)), entry("insert", decimal(0)), entry("delete", decimal(0))}},
		{"CUBRID_DB_REALTIME_DML", map[int32]map[string]protocol.Value{1: {"db_num": decimal(12), "num_query_selects": decimal(8)}, 2: {"db_num": decimal(3)}}, []protocol.Entry{entry("dbname", text("123")), entry("select", decimal(8)), entry("update", decimal(0)), entry("insert", decimal(0)), entry("delete", decimal(0))}},
		{"CUBRID_DB_REALTIME_MULTI_DATA", map[int32]map[string]protocol.Value{1: {"custom": decimal(5)}, 2: {"custom": {Type: protocol.ValueFloat, Float: 2.5}}}, []protocol.Entry{entry("custom", decimal(7))}},
	} {
		t.Run(tc.cmd, func(t *testing.T) {
			b := &databaseBackend{extendedCounterBackend: &extendedCounterBackend{values: tc.values}}
			_, c, r, _ := startDatabaseQuery(t, b)
			send(t, c, tc.cmd, 1, newMap(dbHashes(1, 2), entry("counter", text("custom"))))
			v := readSingleValue(t, r)
			if v.Type != protocol.ValueMap {
				t.Fatalf("type %d", v.Type)
			}
			for _, e := range tc.want {
				if got := lookup(newMap(v.Entries...), e.Key); !reflect.DeepEqual(got, e.Value) {
					t.Fatalf("%s: %#v != %#v", e.Key, got, e.Value)
				}
			}
			send(t, c, tc.cmd, 1, newMap(dbHashes()))
			if got := readPacks(t, r); len(got) != 0 {
				t.Fatalf("empty hashes returned %v", got)
			}
		})
	}
}
func TestDatabaseHitRatioUsesPerCounterFloatDenominator(t *testing.T) {
	b := &databaseBackend{extendedCounterBackend: &extendedCounterBackend{values: map[int32]map[string]protocol.Value{1: {"INNO_BUF_HIT": decimal(8), "KEY_RATIO": decimal(4)}, 2: {"INNO_BUF_HIT": decimal(2)}}}}
	_, c, r, _ := startDatabaseQuery(t, b)
	send(t, c, "DB_REALTIME_HIT_RATIO", 1, newMap(dbHashes(1, 2, 3)))
	p := readPacks(t, r)[0].(*protocol.MapPack)
	if v := lookup(p, "innodb_buffer"); v.Type != protocol.ValueFloat || v.Float != 5 {
		t.Fatalf("float %#v", v)
	}
	if lookup(p, "key_cache").Float != 4 || lookup(p, "query_cache").Type != protocol.ValueNull {
		t.Fatalf("missing denominator %#v", p)
	}
}
func TestDatabaseDailyZeroFillDoubleAndActivityOrder(t *testing.T) {
	for _, cmd := range []string{"DB_DAILY_CONNECTIONS", "DB_DAILY_ACTIVITY"} {
		t.Run(cmd, func(t *testing.T) {
			b := &databaseBackend{extendedCounterBackend: &extendedCounterBackend{}, dailyValues: map[string]map[int32]map[string][]protocol.Value{"20260101": {1: {"THREADS_TOTAL": {decimal(2)}, "THREADS_RUNNING": {decimal(1)}, "COM_SELECT": {decimal(4)}, "COM_INSERT": {decimal(3)}}}}}
			_, c, r, _ := startDatabaseQuery(t, b)
			send(t, c, cmd, 1, newMap(dbHashes(1, 2), entry("sdate", text("20260101")), entry("edate", text("20260101"))))
			p := readPacks(t, r)[0].(*protocol.MapPack)
			if len(lookup(p, "time").Values) != 288 {
				t.Fatal("five minute series")
			}
			key := "total"
			want := 2.0
			if cmd == "DB_DAILY_ACTIVITY" {
				key = "select"
				want = 4
				keys := []string{}
				for _, e := range p.Entries {
					keys = append(keys, e.Key)
				}
				if !reflect.DeepEqual(keys, []string{"time", "call", "select", "insert", "update", "delete"}) {
					t.Fatalf("order %v", keys)
				}
			}
			v := lookup(p, key).Values
			if v[0].Type != protocol.ValueDouble || v[0].Double != want || v[287].Double != 0 {
				t.Fatalf("series %#v", v[:2])
			}
		})
	}
}
func TestDatabaseCubridHistoryAndPeriod(t *testing.T) {
	b := &databaseBackend{extendedCounterBackend: &extendedCounterBackend{bulk: []CounterRow{{Values: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("objHash", decimal(2)), entry("time", decimal(12)), entry("custom", decimal(7))}}}}}, statuses: []*protocol.StatusPack{dbStatus(1, 10, "k"), dbStatus(2, 12, "k")}}
	_, c, r, _ := startDatabaseQuery(t, b)
	for _, cmd := range []string{"CUBRID_DB_REALTIME_STATUS", "CUBRID_DB_SERVER_INFO", "CUBRID_DB_LONG_TRANSACTION_DATA"} {
		send(t, c, cmd, 1, newMap(dbHashes(1, 2), entry("key", text("k")), entry("date", text("20260101")), entry("stime", decimal(0)), entry("etime", decimal(30))))
		ps := readPacks(t, r)
		if len(ps) != 2 || ps[0].(*protocol.StatusPack).ObjHash != 2 || ps[1].(*protocol.StatusPack).ObjHash != 2 {
			t.Fatalf("repeated reverse first %s %#v", cmd, ps)
		}
	}
	send(t, c, "CUBRID_DB_PERIOD_MULTI_DATA", 1, newMap(dbHashes(2, 1, 2), entry("counter", text("custom")), entry("stime", decimal(0)), entry("etime", decimal(30))))
	ps := readPacks(t, r)
	if len(ps) != 3 || len(lookup(ps[0].(*protocol.MapPack), "value").Values) != 1 || len(lookup(ps[1].(*protocol.MapPack), "value").Values) != 0 {
		t.Fatalf("bulk duplicate/empty %#v", ps)
	}
	send(t, c, "CUBRID_DB_LONG_PERIOD_MULTI_DATA", 1, newMap(dbHashes(2, 1), entry("counter", text("custom")), entry("sDate", text("20260101")), entry("eDate", text("20260102"))))
	ps = readPacks(t, r)
	if len(ps) != 4 || lookup(ps[0].(*protocol.MapPack), "objHash").Int != 2 || len(lookup(ps[0].(*protocol.MapPack), "value").Values) != 0 {
		t.Fatalf("empty day*object %#v", ps)
	}
}
func TestDatabaseStatusCacheResponseAndActive(t *testing.T) {
	b := &databaseBackend{extendedCounterBackend: &extendedCounterBackend{}, latest: map[int32]*protocol.StatusPack{1: dbStatus(1, 0, databaseDigestKey, entry("AVG_TIMER_WAIT", list([]protocol.Value{{Type: protocol.ValueDouble, Double: 100}, {Type: protocol.ValueDouble, Double: 200}})), entry("LAST_SEEN", list([]protocol.Value{decimal(1700000000001), decimal(1700000000002)})), entry("COUNT_STAR", list([]protocol.Value{decimal(1), decimal(3)})))}}
	s, c, r, _ := startDatabaseQuery(t, b)
	s.config.Clock = func() time.Time { return time.UnixMilli(1700000300000) }
	send(t, c, "DB_REALTIME_RESPONSE_TIME", 1, newMap(dbHashes(1, 2)))
	ps := readPacks(t, r)
	if len(ps) != 1 || lookup(ps[0].(*protocol.MapPack), "value").Type != protocol.ValueDouble || lookup(ps[0].(*protocol.MapPack), "value").Double != 175 {
		t.Fatalf("weighted %#v", ps)
	}
	send(t, c, "CUBRID_ACTIVE_DB_LIST", 1, newMap(dbHashes(2, 1, 1), entry("key", text(databaseDigestKey))))
	ps = readPacks(t, r)
	if len(ps) != 2 {
		t.Fatalf("cache duplicates %#v", ps)
	}
}
func TestDatabaseDigestQueries(t *testing.T) {
	p1 := dbStatus(1, 10, databaseDigestKey, entry("DIGEST_TEXT", list([]protocol.Value{decimal(7)})), entry("LAST_SEEN", list([]protocol.Value{decimal(15)})), entry("MIN_TIMER_WAIT", list([]protocol.Value{decimal(4)})), entry("MAX_TIMER_WAIT", list([]protocol.Value{decimal(20)})), entry("AVG_TIMER_WAIT", list([]protocol.Value{decimal(10)})))
	p2 := dbStatus(1, 20, databaseDigestKey, entry("DIGEST_TEXT", list([]protocol.Value{decimal(7)})), entry("LAST_SEEN", list([]protocol.Value{decimal(25)})), entry("MIN_TIMER_WAIT", list([]protocol.Value{decimal(2)})), entry("MAX_TIMER_WAIT", list([]protocol.Value{decimal(30)})), entry("AVG_TIMER_WAIT", list([]protocol.Value{decimal(20)})))
	cur := dbStatus(1, 25, databaseCurrentKey, entry("DIGEST_TEXT", list([]protocol.Value{decimal(7), decimal(7)})), entry("THREAD_ID", list([]protocol.Value{decimal(9), decimal(8)})), entry("TIMER_WAIT", list([]protocol.Value{decimal(12), decimal(99)})), entry("LOCK_TIME", list([]protocol.Value{decimal(2), decimal(3)})), entry("SQL_TEXT", list([]protocol.Value{decimal(5), decimal(6)})))
	b := &databaseBackend{extendedCounterBackend: &extendedCounterBackend{}, statuses: []*protocol.StatusPack{p1, p2, cur}}
	_, c, r, _ := startDatabaseQuery(t, b)
	p := newMap(dbHashes(1, 2), entry("date", text("20260101")), entry("stime", decimal(0)), entry("etime", decimal(30)), entry("time", decimal(30)), entry("digest", decimal(7)), entry("column", text("AVG_TIMER_WAIT")))
	send(t, c, "DB_DIGEST_TABLE", 1, p)
	ps := readPacks(t, r)
	if len(ps) != 4 || lookup(ps[0].(*protocol.MapPack), "percent").Int != 33 || lookup(ps[1].(*protocol.MapPack), "percent").Int != 66 {
		t.Fatalf("progress %#v", ps)
	}
	sum := ps[2].(*protocol.MapPack)
	if lookup(sum, "MIN_TIMER_WAIT").Int != 2 || lookup(sum, "MAX_TIMER_WAIT").Int != 30 || lookup(sum, "AVG_TIMER_WAIT").Int != 30 || lookup(sum, "count").Int != 2 || ps[3].(*protocol.StatusPack).Time != 20 {
		t.Fatalf("summary/last %#v", ps)
	}
	send(t, c, "DB_LAST_DIGEST_TABLE", 1, p)
	ps = readPacks(t, r)
	if len(ps) != 1 || ps[0].(*protocol.StatusPack).Time != 20 {
		t.Fatalf("latest %#v", ps)
	}
	send(t, c, "DB_LOAD_DIGEST_COUNTER", 1, p)
	ps = readPacks(t, r)
	if len(ps) != 2 || len(lookup(ps[0].(*protocol.MapPack), "value").Values) != 2 || len(lookup(ps[1].(*protocol.MapPack), "value").Values) != 0 {
		t.Fatalf("digest series %#v", ps)
	}
	send(t, c, "DB_MAX_TIMER_WAIT_THREAD", 1, p)
	ps = readPacks(t, r)
	if len(ps) != 1 || lookup(ps[0].(*protocol.MapPack), "thread_id").Int != 9 || lookup(ps[0].(*protocol.MapPack), "timer").Int != 12 {
		t.Fatalf("first matching digest %#v", ps)
	}
}
func TestDatabaseBoundsCancellationAndRecognition(t *testing.T) {
	s, _ := New(queryConfig(), &fakeBackend{})
	for _, cmd := range []string{"DB_KILL_PROCESS", "CUBRID_SET_ALERT_CONFIGURE", "unknown"} {
		ok, err := s.HandleDatabaseCounterCommand(context.Background(), transport.ClientRequest{Command: cmd}, nil)
		if ok || err != nil {
			t.Fatalf("recognition %s %v %v", cmd, ok, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok, err := s.HandleDatabaseCounterCommand(ctx, transport.ClientRequest{Command: "DB_DIGEST_TABLE", Authenticated: true}, nil)
	if !ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel %v %v", ok, err)
	}
	ok, err = s.HandleDatabaseCounterCommand(context.Background(), transport.ClientRequest{Command: "DB_DIGEST_TABLE"}, nil)
	if !ok || !errors.Is(err, transport.ErrUnauthorized) {
		t.Fatalf("auth %v %v", ok, err)
	}
	if len(DatabaseCounterCommands()) != 18 {
		t.Fatalf("command count %d", len(DatabaseCounterCommands()))
	}
}

func readSingleValue(t *testing.T, r *bufio.Reader) protocol.Value {
	t.Helper()
	flag, err := r.ReadByte()
	if err != nil || flag != transport.HasNext {
		t.Fatalf("value flag %d %v", flag, err)
	}
	v, err := protocol.ReadValue(r, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	flag, err = r.ReadByte()
	if err != nil || flag != transport.NoNext {
		t.Fatalf("terminal flag %d %v", flag, err)
	}
	return v
}

func TestDatabaseNumberGetterAndCastUtilBoundaries(t *testing.T) {
	p := newMap(entry("objHash", list([]protocol.Value{decimal(4294967297), {Type: protocol.ValueDouble, Double: 2.9}, text("3"), {Type: protocol.ValueNull}})))
	hs, err := databaseHashes(p, false)
	if err != nil || !reflect.DeepEqual(hs, []int32{1, 2, 0, 0}) {
		t.Fatalf("Number.intValue %#v %v", hs, err)
	}
	if databaseLong(text("7")) != 0 || extendedLong(text("7")) != 7 || databaseLong(protocol.Value{Type: protocol.ValueDouble, Double: 1e100}) != 9223372036854775807 {
		t.Fatal("Number vs CastUtil contract")
	}
	_, err = databaseHashes(newMap(entry("objHash", list(make([]protocol.Value, extendedQueryCells+1)))), false)
	if !errors.Is(err, transport.ErrLimit) {
		t.Fatal("hash limit", err)
	}
}
func TestDatabaseBulkExplicitNullIsPresent(t *testing.T) {
	b := &databaseBackend{extendedCounterBackend: &extendedCounterBackend{bulk: []CounterRow{{Values: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("objHash", decimal(1)), entry("time", decimal(7)), entry("counter", protocol.Value{Type: protocol.ValueNull})}}}}}}
	_, c, r, _ := startDatabaseQuery(t, b)
	send(t, c, "CUBRID_DB_PERIOD_MULTI_DATA", 1, newMap(dbHashes(1), entry("counter", text("counter")), entry("stime", decimal(0)), entry("etime", decimal(10))))
	p := readPacks(t, r)[0].(*protocol.MapPack)
	if values := lookup(p, "value").Values; len(values) != 1 || values[0].Type != protocol.ValueNull || lookup(p, "time").Values[0].Int != 7 {
		t.Fatalf("present NullValue %#v", p)
	}
}
func TestDatabaseBackendAndMalformedFailures(t *testing.T) {
	backendErr := errors.New("synthetic backend failure")
	tests := []struct {
		name, cmd string
		b         Backend
		p         *protocol.MapPack
		want      error
	}{
		{"missing status backend", "DB_REALTIME_RESPONSE_TIME", &fakeBackend{}, newMap(dbHashes(1)), transport.ErrUnsupported},
		{"missing bulk backend", "CUBRID_DB_PERIOD_MULTI_DATA", &fakeBackend{}, newMap(dbHashes(1)), transport.ErrUnsupported},
		{"counter backend error", "DB_REALTIME_CONNECTIONS", &databaseBackend{extendedCounterBackend: &extendedCounterBackend{err: backendErr}}, newMap(dbHashes(1)), backendErr},
		{"status backend error", "DB_LAST_DIGEST_TABLE", &databaseBackend{extendedCounterBackend: &extendedCounterBackend{err: backendErr}}, newMap(dbHashes(1), entry("date", text("20260101")), entry("time", decimal(300001))), backendErr},
		{"missing hash list", "DB_REALTIME_CONNECTIONS", &fakeBackend{}, newMap(), transport.ErrMalformed},
		{"unbounded daily allocation", "DB_DAILY_ACTIVITY", &databaseBackend{extendedCounterBackend: &extendedCounterBackend{}}, newMap(dbHashes(), entry("sdate", text("20200101")), entry("edate", text("20260101"))), transport.ErrLimit},
		{"digest zero division", "DB_DIGEST_TABLE", &databaseBackend{extendedCounterBackend: &extendedCounterBackend{}}, newMap(dbHashes(1), entry("date", text("20260101"))), transport.ErrMalformed},
		{"misaligned status columns", "DB_REALTIME_RESPONSE_TIME", &databaseBackend{extendedCounterBackend: &extendedCounterBackend{}, latest: map[int32]*protocol.StatusPack{1: dbStatus(1, 1, databaseDigestKey, entry("AVG_TIMER_WAIT", list([]protocol.Value{decimal(1)})), entry("LAST_SEEN", list([]protocol.Value{})))}}, newMap(dbHashes(1)), transport.ErrMalformed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, c, _, results := startDatabaseQuery(t, tc.b)
			send(t, c, tc.cmd, 1, tc.p)
			if err := <-results; !errors.Is(err, tc.want) {
				t.Fatalf("got %v want %v", err, tc.want)
			}
		})
	}
}
func TestDatabaseEmptyTimerResultUsesOriginalEmptyMap(t *testing.T) {
	b := &databaseBackend{extendedCounterBackend: &extendedCounterBackend{}}
	_, c, r, _ := startDatabaseQuery(t, b)
	send(t, c, "DB_MAX_TIMER_WAIT_THREAD", 1, newMap(dbHashes(1), entry("date", text("20260101")), entry("stime", decimal(1)), entry("etime", decimal(2))))
	ps := readPacks(t, r)
	if len(ps) != 1 || len(ps[0].(*protocol.MapPack).Entries) != 0 {
		t.Fatalf("original empty max result %#v", ps)
	}
}

// This oracle compiles the pinned, unchanged MariaService/CubridService and
// actual StatusRD/index/reader/writer. CounterCache supply is an explicit seam;
// query calculations and response encoding execute the original handlers.
func TestDatabaseOriginalScalaHandlerWireOracle(t *testing.T) {
	if _, err := os.Stat(filepath.Join(scouterSource(), "scouter.server")); err != nil {
		t.Skip("pinned original source unavailable; DB Scala oracle not run")
	}
	if _, err := exec.LookPath("java"); err != nil {
		t.Skip("JDK unavailable; DB Scala oracle not run")
	}
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	output, err := exec.CommandContext(ctx, "python3", "../../scripts/java-storage.py", "--source", scouterSource(), "--generate-db-handlers", dir).CombinedOutput()
	if err != nil {
		t.Fatalf("actual DB Scala handler oracle %v\n%s", err, output)
	}
	read := func(name string) []byte {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	decode := func(name string) protocol.Pack {
		t.Helper()
		p, err := protocol.DecodePack(read(name), protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	cached := decode("db-response-time-input-status.bin").(*protocol.StatusPack)
	values := map[int32]map[string]protocol.Value{}
	for _, h := range []int32{42, 43} {
		p := decode("db-counter-" + strconv.FormatInt(int64(h), 10) + "-input.bin").(*protocol.PerfCounterPack)
		if p.TimeType != 1 {
			t.Fatal("oracle counter not realtime")
		}
		values[h] = map[string]protocol.Value{}
		for _, e := range p.Data.Entries {
			values[h][e.Key] = e.Value
		}
	}
	statuses := []*protocol.StatusPack{}
	for i := 0; i < 4; i++ {
		statuses = append(statuses, decode("db-status-"+strconv.Itoa(i)+"-input.bin").(*protocol.StatusPack))
	}
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	config := queryConfig()
	config.Location = loc
	config.Clock = func() time.Time { return time.UnixMilli(cached.Time) }
	for _, tc := range []struct {
		name, command string
		value         bool
	}{
		{"connections", "DB_REALTIME_CONNECTIONS", true}, {"activity", "DB_REALTIME_ACTIVITY", true}, {"cubrid-dml", "CUBRID_DB_REALTIME_DML", true},
		{"hit-ratio", "DB_REALTIME_HIT_RATIO", false}, {"response-time", "DB_REALTIME_RESPONSE_TIME", false}, {"digest-table", "DB_DIGEST_TABLE", false},
		{"cubrid-status", "CUBRID_DB_REALTIME_STATUS", false}, {"cubrid-server-info", "CUBRID_DB_SERVER_INFO", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &databaseBackend{extendedCounterBackend: &extendedCounterBackend{values: values}, latest: map[int32]*protocol.StatusPack{42: cached}, statuses: statuses}
			_, c, r, results := startDatabaseQuery(t, b, config)
			request := read("db-" + tc.name + "-request.bin")
			want := read("db-" + tc.name + "-response.bin")
			e := protocol.NewEncoder(protocol.Limits{})
			e.Text(tc.command)
			e.Int64(1)
			header, err := e.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.Write(append(header, request...)); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(want))
			if _, err := io.ReadFull(r, got); err != nil {
				t.Fatal(err)
			}
			if flag, err := r.ReadByte(); err != nil || flag != transport.NoNext {
				t.Fatalf("terminator %x %v", flag, err)
			}
			if err := <-results; err != nil {
				t.Fatalf("Go handler %v", err)
			}
			if tc.value {
				// Java Hashtable order is unspecified. Compare the emitted raw Value bytes
				// after sorting keys, preserving Value types and every scalar bit.
				if len(got) == 0 || got[0] != transport.HasNext || want[0] != transport.HasNext {
					t.Fatal("Value HasNEXT")
				}
				gv, err := protocol.DecodeValue(got[1:], protocol.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				wv, err := protocol.DecodeValue(want[1:], protocol.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				sort.Slice(gv.Entries, func(i, j int) bool { return gv.Entries[i].Key < gv.Entries[j].Key })
				sort.Slice(wv.Entries, func(i, j int) bool { return wv.Entries[i].Key < wv.Entries[j].Key })
				gb, err := protocol.EncodeValue(gv, protocol.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				wb, err := protocol.EncodeValue(wv, protocol.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(gb, wb) {
					t.Fatalf("original vs Go canonical Value bytes\ngot %x\nwant %x", gb, wb)
				}
			} else if tc.name == "digest-table" {
				// Original StatusRD decodes StatusPack.Data into a Hashtable.
				// Collision chains can change order on re-encoding. Only that
				// unordered nested MapValue is normalized; progress/summary
				// MapPack insertion order and all scalar bytes stay exact.
				canonical := func(raw []byte) []byte {
					reader := bytes.NewReader(raw)
					result := []byte{}
					for reader.Len() > 0 {
						flag, err := reader.ReadByte()
						if err != nil || flag != transport.HasNext {
							t.Fatalf("Pack HasNEXT %x %v", flag, err)
						}
						pack, err := protocol.ReadPack(reader, protocol.Limits{})
						if err != nil {
							t.Fatal(err)
						}
						if status, ok := pack.(*protocol.StatusPack); ok {
							status.Data = canonicalDatabaseValue(status.Data)
						}
						encoded, err := protocol.EncodePack(pack, protocol.Limits{})
						if err != nil {
							t.Fatal(err)
						}
						result = append(result, flag)
						result = append(result, encoded...)
					}
					return result
				}
				if gb, wb := canonical(got), canonical(want); !bytes.Equal(gb, wb) {
					t.Fatalf("original vs Go Pack bytes with nested Hashtable canonicalization\ngot %x\nwant %x", gb, wb)
				}
			} else if !bytes.Equal(got, want) {
				t.Fatalf("original vs Go raw Pack bytes\ngot %x\nwant %x", got, want)
			}
		})
	}
}

func canonicalDatabaseValue(v protocol.Value) protocol.Value {
	if v.Type == protocol.ValueMap {
		sort.Slice(v.Entries, func(i, j int) bool { return v.Entries[i].Key < v.Entries[j].Key })
		for i := range v.Entries {
			v.Entries[i].Value = canonicalDatabaseValue(v.Entries[i].Value)
		}
	}
	if v.Type == protocol.ValueList {
		for i := range v.Values {
			v.Values[i] = canonicalDatabaseValue(v.Values[i])
		}
	}
	return v
}
