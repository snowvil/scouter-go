package console

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

var update = flag.Bool("update", false, "rewrite testdata/golden")

// fakeSource is fixed seed data. Every method is safe for the repeat job.
type fakeSource struct {
	mu       sync.Mutex
	objects  []Object
	types    map[string]ObjectType
	counters map[int32]map[string]protocol.Value
	ring     []*protocol.XLogPack // realtime records, cursor = index in ring
	byTime   []*protocol.XLogPack
	texts    map[string]string // div/hash
	tags     []TagValue
	series   []float32
	visitT   map[string]int64
	visitO   map[int32]int64
	stats    []Stat
	err      error // returned by Objects when set
	calls    int
	gate     chan struct{} // when set, RealtimeCounter blocks until closed or ctx ends
}

func (f *fakeSource) Objects(ctx context.Context) ([]Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return append([]Object(nil), f.objects...), nil
}

func (f *fakeSource) ObjectType(name string) (ObjectType, bool) {
	t, ok := f.types[name]
	return t, ok
}

func (f *fakeSource) RealtimeCounter(ctx context.Context, hash int32, name string) (protocol.Value, bool, error) {
	f.mu.Lock()
	gate := f.gate
	f.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return protocol.Value{}, false, ctx.Err()
		}
	}
	v, ok := f.counters[hash][name]
	return v, ok, nil
}

func (f *fakeSource) XLogsSince(ctx context.Context, loop int64, index int32, hashes []int32) (int64, int32, []*protocol.XLogPack, error) {
	var out []*protocol.XLogPack
	for _, p := range f.ring[index:] {
		for _, h := range hashes {
			if p.ObjHash == h {
				out = append(out, p)
			}
		}
	}
	return loop, int32(len(f.ring)), out, nil
}

func (f *fakeSource) XLogsByTime(ctx context.Context, date string, from, to int64) ([]*protocol.XLogPack, error) {
	var out []*protocol.XLogPack
	for _, p := range f.byTime {
		if p.EndTime >= from && p.EndTime <= to {
			out = append(out, p)
		}
	}
	return out, nil
}

func (f *fakeSource) Text(ctx context.Context, date, div string, hash int32) (string, bool, error) {
	s, ok := f.lookup(div, hash)
	return s, ok, nil
}

func (f *fakeSource) TagValues(ctx context.Context, date, objType, group, tag string) ([]TagValue, error) {
	if objType != "tomcat" || group != "service" {
		return nil, nil
	}
	return f.tags, nil
}

func (f *fakeSource) TagValueSeries(ctx context.Context, date, objType, group, tag string, value protocol.Value) ([]float32, error) {
	if value.Type == protocol.ValueDecimal {
		return nil, nil
	}
	return f.series, nil
}

func (f *fakeSource) VisitorObjType(ctx context.Context, objType string) (int64, error) {
	return f.visitT[objType], nil
}

func (f *fakeSource) VisitorObject(ctx context.Context, hash int32) (int64, error) {
	return f.visitO[hash], nil
}

func (f *fakeSource) Stats() []Stat { return append([]Stat(nil), f.stats...) }

func textKey(div string, hash int32) string {
	return div + "/" + valueString(protocol.Value{Type: protocol.ValueTextHash, Int: int64(hash)})
}

var seoul = func() *time.Location {
	l, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		panic(err)
	}
	return l
}()

// fixedNow is the injected console clock.
var fixedNow = time.Date(2026, 10, 15, 9, 5, 7, 42e6, seoul)

func at(h, m, s, ms int) int64 {
	return time.Date(2026, 10, 15, h, m, s, ms*1e6, seoul).UnixMilli()
}

func seedSource() *fakeSource {
	dec := func(v int64) protocol.Value { return protocol.Value{Type: protocol.ValueDecimal, Int: v} }
	flt := func(v float32) protocol.Value { return protocol.Value{Type: protocol.ValueFloat, Float: v} }
	series := make([]float32, 1440)
	for i := range series {
		series[i] = float32(i%7) * 0.25
	}
	series[61] = 1234.55
	f := &fakeSource{
		objects: []Object{
			{Hash: 100, Type: "tomcat", Name: "/host1/tomcat1", Alive: true},
			{Hash: 200, Type: "tomcat", Name: "/host2/tomcat2", Alive: true},
			{Hash: 300, Type: "tomcat", Name: "/host3/tomcat3"},
			{Hash: 400, Type: "linux", Name: "/host1", Alive: true},
			{Hash: 500, Type: "linux", Name: "/host-long-name2", Alive: true},
			{Hash: 700, Type: "redis", Name: "/r1", Alive: true},
			{Hash: 800, Type: "orphan", Name: "/o1", Alive: true},
		},
		types: map[string]ObjectType{
			"tomcat": {Family: "javaee", HasFamily: true, Counters: []string{"TPS", "ActiveService", "ElapsedTime", "GcCount", "ErrorRate", "TPS"}},
			"linux":  {Family: "host", HasFamily: true, Counters: []string{"Cpu", "Mem"}},
			"redis":  {Family: "redisfam", HasFamily: true},
			"orphan": {},
		},
		counters: map[int32]map[string]protocol.Value{
			100: {"ActiveService": dec(3), "TPS": flt(12.5), "ElapsedTime": dec(251), "GcCount": dec(1), "Flag": {Type: protocol.ValueBoolean, Bool: true}, "Name": {Type: protocol.ValueText, Text: "abc"}},
			200: {"ActiveService": dec(2), "TPS": flt(7.5), "ElapsedTime": dec(150), "GcCount": dec(0)},
			400: {"Cpu": flt(12.5)},
			500: {"Cpu": flt(87.5)},
		},
		ring: []*protocol.XLogPack{
			{ObjHash: 100, EndTime: at(9, 5, 1, 5), Elapsed: 120},
			{ObjHash: 200, EndTime: at(9, 5, 2, 0), Elapsed: 4999, Error: 1},
			{ObjHash: 100, EndTime: at(9, 5, 3, 0), Elapsed: 50000},
			{ObjHash: 100, EndTime: at(9, 5, 3, 1), Elapsed: 4990},
			{ObjHash: 400, EndTime: at(9, 5, 3, 1), Elapsed: 1},
		},
		byTime: []*protocol.XLogPack{
			{ObjHash: 100, EndTime: at(10, 12, 23, 123), TxID: 1234567890123, IPAddress: []byte{10, 1, 2, 3}, Service: 11, Elapsed: 1234},
			{ObjHash: 200, EndTime: at(10, 12, 24, 0), TxID: -5, IPAddress: nil, Service: 12, Elapsed: 5, SQLCount: 3, SQLTime: 1500, APICallCount: 1, APICallTime: 20, UserAgent: 21},
			{ObjHash: 999, EndTime: at(10, 12, 25, 0), TxID: 7, Service: 99, Elapsed: 0, UserAgent: 22},
			{ObjHash: 100, EndTime: at(10, 12, 26, 0), TxID: 8, Service: 11, Elapsed: 1},
		},
		texts: map[string]string{
			textKey("service", 11):   "/index.jsp",
			textKey("service", 12):   "/api/order",
			textKey("ua", 21):        "Mozilla/5.0",
			textKey("error", 0x1234): "java.lang.NullPointerException",
		},
		tags: []TagValue{
			{Value: protocol.Value{Type: protocol.ValueTextHash, Int: 0x1234}, Count: 10},
			{Value: protocol.Value{Type: protocol.ValueTextHash, Int: -2}, Count: 4},
			{Value: dec(5), Count: 3},
			{Value: protocol.Value{Type: protocol.ValueText, Text: "KR"}, Count: float64(float32(0.1))},
		},
		series: series,
		visitT: map[string]int64{"tomcat": 1234567},
		visitO: map[int32]int64{100: 5, 200: 0, 300: 12000},
		stats:  []Stat{{"udp.rejected", 2}, {"ingest.completed", 1500}, {"tcp.active", 1}, {"Zeta", 9}},
	}
	return f
}

func (f *fakeSource) lookup(div string, hash int32) (string, bool) {
	s, ok := f.texts[textKey(div, hash)]
	return s, ok
}

type transcript struct {
	t      *testing.T
	c      *Console
	out    *bytes.Buffer
	errBuf *bytes.Buffer
	text   strings.Builder
	quits  int
}

func newTranscript(t *testing.T, src Source, ansi bool) *transcript {
	tr := &transcript{t: t, out: &bytes.Buffer{}, errBuf: &bytes.Buffer{}}
	c, err := New(Config{Source: src, Out: tr.out, Err: tr.errBuf, Location: seoul, Now: func() time.Time { return fixedNow }, ANSI: ansi, Interval: time.Hour, Quit: func() { tr.quits++ }})
	if err != nil {
		t.Fatal(err)
	}
	tr.c = c
	t.Cleanup(func() { c.Close() })
	return tr
}

// run executes one line; a repeat command runs exactly one cycle (the first
// one starts at once and the next line stops the job before the hour-long
// interval).
func (tr *transcript) run(line string) {
	tr.c.Execute(context.Background(), line)
	tr.c.stopJob()
	tr.text.WriteString("scouter> " + line + "\n")
	tr.text.WriteString(tr.out.String())
	for _, l := range strings.SplitAfter(tr.errBuf.String(), "\n") {
		if l != "" {
			tr.text.WriteString("[stderr] " + l)
		}
	}
	tr.out.Reset()
	tr.errBuf.Reset()
}

func checkGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", "golden", name)
	if *update {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("%s differs (go test ./internal/console -run %s -update)\n--- got\n%s\n--- want\n%s", path, t.Name(), got, want)
	}
}

func TestCommandsGolden(t *testing.T) {
	tr := newTranscript(t, seedSource(), false)
	for _, line := range []string{
		"help", "HELP me", "unknown words", "", "   ",
		"objtype", "object", "object tomcat1", "OBJECT ,host1",
		"counter", "counter tomcat", "counter nothing", "counter orphan",
		"dashboard", "dashboard nothing", "dashboard tomcat",
		"dashboard linux Cpu", "dashboard linux", "dashboard redis", "dashboard orphan",
		"realtime", "realtime tomcat", "realtime tomcat TPS", "realtime * TPS sum", "realtime tomcat TPS AVG",
		"realtime tomcat TPS max", "realtime host1 Cpu", "realtime nomatch TPS", "realtime tomcat Flag", "realtime tomcat Name", "realtime tomcat Flag SUM",
		"xlog", "xlog tomcat", "xlog tomcat", "xlog nomatch", "xlog * 0",
		"xlist", "xlist tomcat", "xlist tomcat 10:12:23.000", "xlist tomcat 10:12:23.000 2", "xlist tomcat 10:12:23.000 x", "xlist tomcat 9:00", "xlist tomcat 10:11:83.1000",
		"tagcnt", "tagcnt group", "tagcnt tag service", "tagcnt tag alert", "tagcnt tag none", "tagcnt tag",
		"tagcnt top100 tomcat service error", "tagcnt top100 tomcat service",
		"tagcnt data tomcat service error 0", "tagcnt data tomcat service error 3", "tagcnt data tomcat service error x", "tagcnt data tomcat service error",
		"visitor tomcat", "visitor host", "visitor", "visitor nothing",
		"debug queue", "debug _", "debug", "debug QUEUE",
	} {
		tr.run(line)
	}
	checkGolden(t, "commands.txt", tr.text.String())
	if tr.quits != 0 {
		t.Fatal("quit called")
	}
}

func TestCommandsGoldenANSIAndQuit(t *testing.T) {
	tr := newTranscript(t, seedSource(), true)
	for _, line := range []string{"objtype", "counter linux", "realtime tomcat TPS", "xlog tomcat", "dashboard tomcat", "q"} {
		tr.run(line)
	}
	tr.text.WriteString("prompt=" + tr.c.PromptText() + "\n")
	checkGolden(t, "ansi.txt", tr.text.String())
	if tr.quits != 1 {
		t.Fatalf("quit callbacks %d", tr.quits)
	}
	if !tr.c.Execute(context.Background(), "QUIT now") || tr.quits != 2 {
		t.Fatal("QUIT must quit")
	}
}

// TestDashboardHeaderCycle checks the shared Dashboard.lines counter (header
// every 11 lines) and the host header on a column count change.
func TestDashboardHeaderCycle(t *testing.T) {
	src := seedSource()
	tr := newTranscript(t, src, false)
	for i := 0; i < 12; i++ {
		tr.run("dashboard tomcat")
	}
	tr.run("dashboard linux Cpu")
	delete(src.counters, 500)
	tr.run("dashboard linux Cpu")
	tr.run("dashboard linux Cpu")
	checkGolden(t, "dashboard-cycle.txt", tr.text.String())
}

func TestObjectsErrorIsOneLine(t *testing.T) {
	src := seedSource()
	src.err = errors.New("engine closed")
	tr := newTranscript(t, src, false)
	for _, line := range []string{"objtype", "object", "realtime * TPS", "xlog *", "visitor x", "xlist t 10:00:00.000"} {
		tr.run(line)
	}
	got := tr.text.String()
	if strings.Count(got, "[stderr] ") != 6 || strings.Contains(got, "goroutine") {
		t.Fatal(got)
	}
}

func TestParseLogTimeLenient(t *testing.T) {
	base := time.Date(2026, 10, 15, 0, 0, 0, 0, seoul).UnixMilli()
	for in, want := range map[string]int64{
		"10:12:23.123":     base + ((10*60+12)*60+23)*1000 + 123,
		"10:12:23.5":       base + ((10*60+12)*60+23)*1000 + 5,
		"10:61:00.000":     base + (11*60+1)*60*1000,
		"25:00:00.000xyz":  base + 25*3600*1000,
		"00:00:00.0000002": base + 2,
	} {
		got, ok := parseLogTime("20261015", in, seoul)
		if !ok || got != want {
			t.Errorf("%s: %d %v want %d", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "9:00:00.000", "10:12:23", "10:12", "ab:00:00.000", "10-12-23.123"} {
		if _, ok := parseLogTime("20261015", in, seoul); ok {
			t.Errorf("%q accepted", in)
		}
	}
}
