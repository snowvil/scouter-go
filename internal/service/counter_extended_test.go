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
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/transport"
)

type extendedCounterBackend struct {
	Backend
	objects map[string][]*protocol.ObjectPack
	values  map[int32]map[string]protocol.Value
	daily   map[string]map[int32][]protocol.Value
	history map[int32][]CounterPoint
	bulk    []CounterRow
	names   []string
	err     error
}

func (b *extendedCounterBackend) CounterBulkRows(ctx context.Context, date string, from, to int64) ([]CounterRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.bulk, b.err
}

func (b *extendedCounterBackend) Objects(ctx context.Context, date string) ([]*protocol.ObjectPack, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.objects[date], b.err
}
func (b *extendedCounterBackend) Object(ctx context.Context, date string, hash int32) (*protocol.ObjectPack, error) {
	for _, o := range b.objects[date] {
		if o.ObjHash == hash {
			return o, b.err
		}
	}
	return nil, b.err
}
func (b *extendedCounterBackend) Counter(ctx context.Context, hash int32, name string, tt byte) (protocol.Value, bool, error) {
	if err := ctx.Err(); err != nil {
		return protocol.Value{}, false, err
	}
	v, ok := b.values[hash][name]
	return v, ok, b.err
}
func (b *extendedCounterBackend) CounterDaily(ctx context.Context, date string, hash int32, name string, tt byte) ([]protocol.Value, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.daily[date][hash], b.err
}
func (b *extendedCounterBackend) CounterHistory(ctx context.Context, hash int32, name string, from, to int64) ([]CounterPoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.history[hash], b.err
}
func (b *extendedCounterBackend) CounterNames(ctx context.Context, hash int32) ([]string, error) {
	return b.names, b.err
}
func (b *extendedCounterBackend) CounterNamesForDate(ctx context.Context, date string, hash int32) ([]string, error) {
	return b.names, b.err
}
func (b *extendedCounterBackend) CounterType(ctx context.Context, typ string) (CounterTypeMetadata, error) {
	return CounterTypeMetadata{DisplayName: "Synthetic", Counters: b.names}, b.err
}

func startExtendedCounter(t *testing.T, backend Backend, configs ...Config) (*Service, net.Conn, *bufio.Reader, <-chan error) {
	t.Helper()
	config := queryConfig()
	if len(configs) > 0 {
		config = configs[0]
	}
	s, err := New(config, backend)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 32)
	h := transport.TCPHandlers{Authorize: func(context.Context, transport.ClientRequest) (bool, error) { return true, nil }, Client: func(ctx context.Context, r transport.ClientRequest, stream *transport.Stream) error {
		ok, err := s.HandleExtendedCounterCommand(ctx, r, stream)
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
func TestExtendedCounterTodayTotalOriginalSingletonAndCaughtPartial(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []protocol.Value
		want float64
	}{{"empty", nil, 1}, {"zero", []protocol.Value{decimal(0), decimal(0)}, 1}, {"partial", []protocol.Value{decimal(12), decimal(24)}, 300.0 / 289.0}} {
		t.Run(tc.name, func(t *testing.T) {
			b := &extendedCounterBackend{objects: map[string][]*protocol.ObjectPack{"": {{ObjHash: 42, ObjType: "host", Alive: true}}}, daily: map[string]map[int32][]protocol.Value{"20261013": {42: tc.data}}}
			_, c, r, _ := startExtendedCounter(t, b)
			send(t, c, "COUNTER_TODAY_TOT", 1, newMap(entry("counter", text("cpu")), entry("objType", text("host")), entry("mode", text("avg"))))
			p := readPacks(t, r)[0].(*protocol.MapPack)
			values := lookup(p, "value").Values
			if len(values) != 1 || values[0].Double != tc.want {
				t.Fatalf("original singleton %#v", values)
			}
		})
	}

}
func TestExtendedCounterPeriodicSelectorUsesFirstDatedObjectType(t *testing.T) {
	b := &extendedCounterBackend{objects: map[string][]*protocol.ObjectPack{"20260101": {{ObjHash: 1, ObjType: "host"}}, "20260102": {{ObjHash: 1, ObjType: "java"}}}}
	_, c, r, _ := startExtendedCounter(t, b)
	send(t, c, "COUNTER_PAST_LONGDATE_ALL", 1, newMap(entry("counter", text("cpu")), entry("objType", text("java")), entry("sDate", text("20260101")), entry("eDate", text("20260102"))))
	if ps := readPacks(t, r); len(ps) != 0 {
		t.Fatalf("Java HashSet retains first ObjectPack for same hash, got %d", len(ps))
	}
}
func TestExtendedCounterPastTotalOriginalEmptyAndNonemptyFailure(t *testing.T) {
	b := &extendedCounterBackend{objects: map[string][]*protocol.ObjectPack{"20260101": {{ObjHash: 42, ObjType: "host"}}}}
	_, c, r, _ := startExtendedCounter(t, b)
	p := newMap(entry("counter", text("cpu")), entry("objType", text("host")), entry("stime", decimal(1767225600000)), entry("etime", decimal(1767225604000)))
	send(t, c, "COUNTER_PAST_TIME_TOT", 1, p)
	ps := readPacks(t, r)
	if len(ps) != 1 || len(lookup(ps[0].(*protocol.MapPack), "time").Values) != 0 {
		t.Fatal("empty original series")
	}
	b2 := &extendedCounterBackend{objects: b.objects, history: map[int32][]CounterPoint{42: {{Time: 1767225600000, Value: decimal(12)}, {Time: 1767225602000, Value: decimal(24)}}}}
	_, c2, r2, results := startExtendedCounter(t, b2)
	send(t, c2, "COUNTER_PAST_TIME_TOT", 1, p)
	if err := <-results; err == nil || errors.Is(err, transport.ErrUnsupported) {
		t.Fatalf("original nonempty cast failure: %v", err)
	}
	if _, err := r2.ReadByte(); err == nil {
		t.Fatal("nonempty original handler must not return bytes")
	}
}

// scouterSource is the pinned original source of the Java oracles:
// SCOUTER_SOURCE, else the sibling scouter checkout (the java-storage.py
// default).
func scouterSource() string {
	if source := os.Getenv("SCOUTER_SOURCE"); source != "" {
		return source
	}
	return filepath.Clean("../../../scouter")
}

func TestExtendedCounterOriginalScalaTodayWireOracle(t *testing.T) {
	if _, err := os.Stat(filepath.Join(scouterSource(), "scouter.server")); err != nil {
		t.Skip("pinned original source unavailable; Scala oracle not run")
	}
	if _, err := exec.LookPath("java"); err != nil {
		t.Skip("JDK unavailable; Scala oracle not run")
	}
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	output, err := exec.CommandContext(ctx, "python3", "../../scripts/java-storage.py", "--source", scouterSource(), "--generate-counter-handlers", dir).CombinedOutput()
	if err != nil {
		t.Fatalf("actual Scala handler oracle: %v\n%s", err, output)
	}
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		data    []protocol.Value
		objects bool
	}{{"empty", nil, false}, {"zero", []protocol.Value{{Type: protocol.ValueDouble, Double: 0}, {Type: protocol.ValueDouble, Double: 0}}, true}, {"one-positive", []protocol.Value{{Type: protocol.ValueDouble, Double: 12}}, true}, {"two-positive", []protocol.Value{{Type: protocol.ValueDouble, Double: 12}, {Type: protocol.ValueDouble, Double: 24}}, true}} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := os.ReadFile(filepath.Join(dir, "today-tot-"+tc.name+"-request.bin"))
			if err != nil {
				t.Fatal(err)
			}
			want, err := os.ReadFile(filepath.Join(dir, "today-tot-"+tc.name+"-response.bin"))
			if err != nil {
				t.Fatal(err)
			}
			d := protocol.NewDecoder(want[1:], protocol.Limits{})
			expected := d.Pack().(*protocol.MapPack)
			midnight := lookup(expected, "time").Values[0].Int
			config := queryConfig()
			config.Location = loc
			config.Clock = func() time.Time { return time.UnixMilli(midnight) }
			date := scoutertime.Date(midnight, loc)
			b := &extendedCounterBackend{objects: map[string][]*protocol.ObjectPack{}, daily: map[string]map[int32][]protocol.Value{date: {42: tc.data}}}
			if tc.objects {
				b.objects[""] = []*protocol.ObjectPack{{ObjHash: 42, ObjType: "host", Alive: true}}
			}
			_, c, r, _ := startExtendedCounter(t, b, config)
			e := protocol.NewEncoder(protocol.Limits{})
			e.Text("COUNTER_TODAY_TOT")
			e.Int64(1)
			header, _ := e.Bytes()
			c.Write(append(header, req...))
			got := make([]byte, len(want))
			if _, err := io.ReadFull(r, got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("Scala vs Go raw response\ngot %x\nwant %x", got, want)
			}
			if flag, err := r.ReadByte(); err != nil || flag != transport.NoNext {
				t.Fatalf("terminator %x %v", flag, err)
			}
		})
	}
	for _, tc := range []struct {
		name     string
		nonempty bool
	}{{"empty", false}, {"two-points", true}} {
		t.Run("past-"+tc.name, func(t *testing.T) {
			req, err := os.ReadFile(filepath.Join(dir, "past-tot-"+tc.name+"-request.bin"))
			if err != nil {
				t.Fatal(err)
			}
			want, err := os.ReadFile(filepath.Join(dir, "past-tot-"+tc.name+"-response.bin"))
			if err != nil {
				t.Fatal(err)
			}
			d := protocol.NewDecoder(req, protocol.Limits{})
			p := d.Pack().(*protocol.MapPack)
			from := lookup(p, "stime").Int
			date := scoutertime.Date(from, time.UTC)
			b := &extendedCounterBackend{objects: map[string][]*protocol.ObjectPack{date: {{ObjHash: 42, ObjType: "host"}}}, history: map[int32][]CounterPoint{}}
			if tc.nonempty {
				b.history[42] = []CounterPoint{{Time: from, Value: decimal(12)}, {Time: from + 2000, Value: decimal(24)}}
			}
			_, c, r, results := startExtendedCounter(t, b)
			e := protocol.NewEncoder(protocol.Limits{})
			e.Text("COUNTER_PAST_TIME_TOT")
			e.Int64(1)
			header, _ := e.Bytes()
			c.Write(append(header, req...))
			if tc.nonempty {
				if len(want) != 0 {
					t.Fatal("oracle unexpected partial response")
				}
				if err := <-results; err == nil {
					t.Fatal("expected original ClassCastException equivalent failure")
				}
				if _, err := r.ReadByte(); err == nil {
					t.Fatal("nonempty failure returned bytes")
				}
				return
			}
			got := make([]byte, len(want))
			if _, err := io.ReadFull(r, got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("past empty raw response %x != %x", got, want)
			}
			if f, err := r.ReadByte(); err != nil || f != transport.NoNext {
				t.Fatalf("terminator %x %v", f, err)
			}
		})
	}
	for _, name := range []string{"plain", "metadata"} {
		t.Run("past-group-"+name, func(t *testing.T) {
			prefix := filepath.Join(dir, "past-group-"+name)
			req, err := os.ReadFile(prefix + "-request.bin")
			if err != nil {
				t.Fatal(err)
			}
			want, err := os.ReadFile(prefix + "-response.bin")
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(prefix + "-row.bin")
			if err != nil {
				t.Fatal(err)
			}
			row, err := protocol.DecodeValue(raw, protocol.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			b := &extendedCounterBackend{bulk: []CounterRow{{Time: 1000, Values: row}}}
			_, c, r, _ := startExtendedCounter(t, b)
			e := protocol.NewEncoder(protocol.Limits{})
			e.Text("COUNTER_PAST_TIME_GROUP")
			e.Int64(1)
			header, _ := e.Bytes()
			if _, err := c.Write(append(header, req...)); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(want))
			if _, err := io.ReadFull(r, got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("original group raw response %x != %x", got, want)
			}
			if f, err := r.ReadByte(); err != nil || f != transport.NoNext {
				t.Fatalf("terminator %x %v", f, err)
			}
		})
	}
}
func TestExtendedCounterGroupPreservesOrderDuplicatesAndMissing(t *testing.T) {
	b := &extendedCounterBackend{values: map[int32]map[string]protocol.Value{1: {"cpu": decimal(7)}, 2: {"cpu": {Type: protocol.ValueFloat, Float: 2.5}}}}
	_, c, r, _ := startExtendedCounter(t, b)
	send(t, c, "COUNTER_REAL_TIME_GROUP", 1, newMap(entry("counter", text("cpu")), entry("objHash", list([]protocol.Value{decimal(2), decimal(9), decimal(1), decimal(2)}))))
	p := readPacks(t, r)[0].(*protocol.MapPack)
	if got := lookup(p, "objHash").Values; !reflect.DeepEqual(got, []protocol.Value{decimal(2), decimal(1), decimal(2)}) {
		t.Fatalf("hashes %#v", got)
	}
	if got := lookup(p, "value").Values; len(got) != 3 || got[0].Type != protocol.ValueFloat || got[1].Int != 7 {
		t.Fatalf("values %#v", got)
	}
}
func TestExtendedCounterTotalLiveAverageAndEmpty(t *testing.T) {
	b := &extendedCounterBackend{objects: map[string][]*protocol.ObjectPack{"": {{ObjHash: 1, ObjType: "java", Alive: true}, {ObjHash: 2, ObjType: "java", Alive: false}, {ObjHash: 3, ObjType: "java", Alive: true}, {ObjHash: 4, ObjType: "host", Alive: true}}}, values: map[int32]map[string]protocol.Value{1: {"cpu": decimal(0)}, 2: {"cpu": decimal(100)}, 3: {"cpu": {Type: protocol.ValueDouble, Double: 10}}, 4: {"cpu": decimal(80)}}}
	_, c, r, _ := startExtendedCounter(t, b)
	send(t, c, "COUNTER_REAL_TIME_TOT", 1, newMap(entry("counter", text("cpu")), entry("objType", text("java")), entry("mode", text("AvG"))))
	flag, err := r.ReadByte()
	if err != nil || flag != transport.HasNext {
		t.Fatalf("flag %x %v", flag, err)
	}
	v, err := protocol.ReadValue(r, protocol.Limits{})
	if err != nil || v.Type != protocol.ValueDouble || v.Double != 5 {
		t.Fatalf("value %#v %v", v, err)
	}
	if f, _ := r.ReadByte(); f != transport.NoNext {
		t.Fatal("terminator")
	}
	send(t, c, "COUNTER_REAL_TIME_TOT", 1, newMap(entry("counter", text("cpu")), entry("objType", text("missing"))))
	r.ReadByte()
	v, err = protocol.ReadValue(r, protocol.Limits{})
	if err != nil || v.Type != protocol.ValueDouble || v.Double != 0 {
		t.Fatalf("empty %#v %v", v, err)
	}
	r.ReadByte()
}
func TestExtendedCounterDailyGroupMissingAndFixedSlots(t *testing.T) {
	b := &extendedCounterBackend{daily: map[string]map[int32][]protocol.Value{"20260101": {1: {decimal(-2), {Type: protocol.ValueNull}, decimal(0)}}}}
	_, c, r, _ := startExtendedCounter(t, b)
	send(t, c, "COUNTER_PAST_DATE_GROUP", 1, newMap(entry("counter", text("cpu")), entry("date", text("20260101")), entry("objHash", list([]protocol.Value{decimal(9), decimal(1)}))))
	ps := readPacks(t, r)
	if len(ps) != 2 {
		t.Fatalf("items %d", len(ps))
	}
	if len(lookup(ps[0].(*protocol.MapPack), "time").Values) != 0 {
		t.Fatal("missing must emit empty series")
	}
	p := ps[1].(*protocol.MapPack)
	times := lookup(p, "time").Values
	if len(times) != 3 || times[1].Int-times[0].Int != 300000 {
		t.Fatalf("times %#v", times)
	}
}
func TestExtendedCounterCancellationAndBackendError(t *testing.T) {
	s, _ := New(queryConfig(), &extendedCounterBackend{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok, err := s.HandleExtendedCounterCommand(ctx, transport.ClientRequest{Command: "COUNTER_REAL_TIME_GROUP", Authenticated: true}, nil)
	if !ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel %v %v", ok, err)
	}
	b := &extendedCounterBackend{err: errors.New("synthetic storage error")}
	_, c, _, results := startExtendedCounter(t, b)
	send(t, c, "COUNTER_REAL_TIME_GROUP", 1, newMap(entry("counter", text("cpu")), entry("objHash", list([]protocol.Value{decimal(1)}))))
	if err := <-results; !errors.Is(err, b.err) {
		t.Fatalf("backend error %v", err)
	}
}

func TestExtendedCounterCatalogAndMultiSelectors(t *testing.T) {
	b := &extendedCounterBackend{names: []string{"cpu", "missing"}, objects: map[string][]*protocol.ObjectPack{"": {{ObjHash: 1, ObjType: "java", ObjName: "live", Alive: true}, {ObjHash: 2, ObjType: "java", ObjName: "dead", Alive: false}}}, values: map[int32]map[string]protocol.Value{1: {"cpu": decimal(7)}, 2: {"cpu": decimal(9)}}}
	_, c, r, _ := startExtendedCounter(t, b)
	send(t, c, "COUNTER_REAL_TIME_OBJECT_ALL", 1, newMap(entry("objHash", decimal(1))))
	p := readPacks(t, r)[0].(*protocol.MapPack)
	if vs := lookup(p, "counter").Values; len(vs) != 1 || vs[0].Text != "cpu" {
		t.Fatalf("catalog %#v", p)
	}
	send(t, c, "COUNTER_REAL_TIME_OBJECT_TYPE_ALL", 1, newMap(entry("objType", text("java"))))
	p = readPacks(t, r)[0].(*protocol.MapPack)
	if len(p.Entries) != 1 || p.Entries[0].Key != "live" || p.Entries[0].Value.Type != protocol.ValueMap {
		t.Fatalf("type/live %#v", p)
	}
	send(t, c, "COUNTER_REAL_TIME_ALL_MULTI", 1, newMap(entry("counter", list([]protocol.Value{text("cpu"), text("missing"), text("cpu")})), entry("objType", text("host")), entry("objHash", list([]protocol.Value{decimal(2), decimal(1)}))))
	p = readPacks(t, r)[0].(*protocol.MapPack)
	if vs := lookup(p, "objHash").Values; len(vs) != 4 || vs[0].Int != 2 || vs[1].Int != 2 || vs[2].Int != 1 {
		t.Fatalf("explicit inactive / duplicates %#v", p)
	}
}
func TestExtendedCounterPastGroupUsesEmbeddedMetadataNotIndex(t *testing.T) {
	b := &extendedCounterBackend{history: map[int32][]CounterPoint{42: {{Time: 999, Value: decimal(99)}}}, bulk: []CounterRow{{Time: 999, Values: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("cpu", decimal(12))}}}, {Time: 999, Values: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("objHash", decimal(42)), entry("time", decimal(7)), entry("cpu", decimal(5))}}}}}
	_, c, r, _ := startExtendedCounter(t, b)
	send(t, c, "COUNTER_PAST_TIME_GROUP", 1, newMap(entry("counter", text("cpu")), entry("objHash", list([]protocol.Value{decimal(42), decimal(0), decimal(42)})), entry("stime", decimal(1767225600000)), entry("etime", decimal(1767225602000))))
	ps := readPacks(t, r)
	if len(ps) != 3 {
		t.Fatal("request duplicate order")
	}
	p := ps[0].(*protocol.MapPack)
	if vs := lookup(p, "time").Values; len(vs) != 1 || vs[0].Int != 7 {
		t.Fatalf("embedded time %#v", p)
	}
	p = ps[1].(*protocol.MapPack)
	if lookup(p, "time").Values[0].Int != 0 || lookup(p, "value").Values[0].Int != 12 {
		t.Fatalf("missing metadata defaults %#v", p)
	}
}
func TestExtendedCounterDailyRemainingShapes(t *testing.T) {
	objects := []*protocol.ObjectPack{{ObjHash: 1, ObjType: "java", Alive: true}, {ObjHash: 2, ObjType: "java", Alive: false}}
	b := &extendedCounterBackend{objects: map[string][]*protocol.ObjectPack{"": objects, "20260101": objects, "20260102": objects, "20261013": objects}, daily: map[string]map[int32][]protocol.Value{"20260101": {1: {decimal(-2), {Type: protocol.ValueNull}}, 2: {decimal(0)}}, "20260102": {1: {decimal(6)}}, "20261013": {1: {decimal(4)}}}}
	_, c, r, _ := startExtendedCounter(t, b)
	for _, tc := range []struct {
		cmd   string
		p     *protocol.MapPack
		items int
	}{{"COUNTER_TODAY_ALL", newMap(entry("objType", text("java"))), 1}, {"COUNTER_TODAY_GROUP", newMap(entry("objHash", list([]protocol.Value{decimal(2), decimal(1)}))), 2}, {"COUNTER_PAST_DATE_ALL", newMap(entry("objType", text("java")), entry("date", text("20260101"))), 2}, {"COUNTER_PAST_TIME_GROUP", newMap(entry("objHash", list([]protocol.Value{decimal(2), decimal(1)})), entry("stime", decimal(1767225600000)), entry("etime", decimal(1767225600001))), 2}, {"COUNTER_PAST_LONGDATE_ALL", newMap(entry("objHash", list([]protocol.Value{decimal(2), decimal(1)})), entry("sDate", text("20260101")), entry("eDate", text("20260102"))), 4}} {
		tc.p.Entries = append(tc.p.Entries, entry("counter", text("cpu")))
		send(t, c, tc.cmd, 1, tc.p)
		ps := readPacks(t, r)
		if len(ps) != tc.items {
			t.Fatalf("%s items=%d", tc.cmd, len(ps))
		}
		if tc.cmd == "COUNTER_PAST_LONGDATE_ALL" {
			for i, hash := range []int64{2, 1, 2, 1} {
				if lookup(ps[i].(*protocol.MapPack), "objHash").Int != hash {
					t.Fatal("date-before-object order")
				}
			}
		}
	}
	send(t, c, "COUNTER_PAST_DATE_TOT", 1, newMap(entry("counter", text("cpu")), entry("objType", text("java")), entry("date", text("20260101")), entry("mode", text("avg"))))
	p := readPacks(t, r)[0].(*protocol.MapPack)
	vs := lookup(p, "value").Values
	if len(vs) != 288 || vs[0].Double != -1 || vs[1].Double != 0 {
		t.Fatalf("past nonnull zero/negative aggregate %#v", vs[:2])
	}
	send(t, c, "COUNTER_PAST_LONGDATE_TOT", 1, newMap(entry("counter", text("cpu")), entry("objType", text("java")), entry("sDate", text("20260101")), entry("eDate", text("20260102")), entry("mode", text("avg"))))
	p = readPacks(t, r)[0].(*protocol.MapPack)
	vs = lookup(p, "value").Values
	if len(vs) != 576 || vs[0].Double != 0 || vs[288].Double != 6 {
		t.Fatal("long total positive-only")
	}
	send(t, c, "GET_COUNTER_EXIST_DAYS", 1, newMap(entry("counter", text("cpu")), entry("objType", text("java")), entry("duration", decimal(1)), entry("etime", decimal(1767312000000))))
	p = readPacks(t, r)[0].(*protocol.MapPack)
	if len(lookup(p, "date").Values) != 2 || !lookup(p, "exist").Values[0].Bool {
		t.Fatal("duration+1/nonempty existence")
	}
}
func TestExtendedCounterLongGroupWholeDateNullFillAndLimit(t *testing.T) {
	b := &extendedCounterBackend{daily: map[string]map[int32][]protocol.Value{"20260101": {1: {decimal(7)}}}}
	_, c, r, _ := startExtendedCounter(t, b)
	send(t, c, "COUNTER_PAST_LONGDATE_GROUP", 1, newMap(entry("counter", text("cpu")), entry("objHash", list([]protocol.Value{decimal(1)})), entry("stime", decimal(1767225600000+3600000)), entry("etime", decimal(1767312000000+3600000))))
	p := readPacks(t, r)[0].(*protocol.MapPack)
	times, values := lookup(p, "time").Values, lookup(p, "value").Values
	if len(times) != 289 || times[0].Int != 1767225600000 || times[1].Int != 1767312000000 || values[1].Type != protocol.ValueNull {
		t.Fatalf("whole day missing fill %d %#v", len(times), times[:2])
	}
	_, c2, _, results := startExtendedCounter(t, b)
	send(t, c2, "GET_COUNTER_EXIST_DAYS", 1, newMap(entry("duration", decimal(3660))))
	if err := <-results; !errors.Is(err, transport.ErrLimit) {
		t.Fatalf("bound %v", err)
	}
}
