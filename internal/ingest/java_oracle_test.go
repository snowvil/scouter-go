package ingest

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/counters"
	"github.com/snowvil/scouter-go/internal/geoip"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

// Replays of the Java oracle golden (tools/java-ingest, original PodSeqManager,
// ObjectCpuChecker and GeoIpUtil run unmodified on JDK 8) against the engine.

type oracleDoc struct {
	Scenario string            `json:"scenario"`
	Steps    []json.RawMessage `json:"steps"`
}

func loadOracle(t *testing.T, name string) []json.RawMessage {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "java-oracle", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc oracleDoc
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Scenario != name || len(doc.Steps) == 0 {
		t.Fatalf("golden %s: %q %d steps", name, doc.Scenario, len(doc.Steps))
	}
	return doc.Steps
}

// typedValue is the oracle's {"t": ..., "v": ...}.
type typedValue struct {
	T string          `json:"t"`
	V json.RawMessage `json:"v"`
}

func (tv typedValue) value(t *testing.T) protocol.Value {
	t.Helper()
	num := func() float64 {
		var s string
		if json.Unmarshal(tv.V, &s) == nil && s == "NaN" {
			return math.NaN()
		}
		f, err := strconv.ParseFloat(string(tv.V), 64)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	switch tv.T {
	case "text":
		var s string
		if err := json.Unmarshal(tv.V, &s); err != nil {
			t.Fatal(err)
		}
		return protocol.Value{Type: protocol.ValueText, Text: s}
	case "bool":
		return protocol.Value{Type: protocol.ValueBoolean, Bool: string(tv.V) == "true"}
	case "decimal":
		n, err := strconv.ParseInt(string(tv.V), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return protocol.Value{Type: protocol.ValueDecimal, Int: n}
	case "float":
		return protocol.Value{Type: protocol.ValueFloat, Float: float32(num())}
	case "double":
		return protocol.Value{Type: protocol.ValueDouble, Double: num()}
	}
	t.Fatalf("type %q", tv.T)
	return protocol.Value{}
}

// oraclePairs is a list of [key, typedValue].
type oraclePairs [][2]json.RawMessage

func (p oraclePairs) mapValue(t *testing.T) protocol.Value {
	t.Helper()
	m := protocol.Value{Type: protocol.ValueMap}
	for _, kv := range p {
		var key string
		var tv typedValue
		if err := json.Unmarshal(kv[0], &key); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(kv[1], &tv); err != nil {
			t.Fatal(err)
		}
		// MapValue is a Hashtable: put replaces an existing key.
		replaced := false
		for i := range m.Entries {
			if m.Entries[i].Key == key {
				m.Entries[i].Value, replaced = tv.value(t), true
			}
		}
		if !replaced {
			m.Entries = append(m.Entries, protocol.Entry{Key: key, Value: tv.value(t)})
		}
	}
	return m
}

type oracleCall struct {
	ObjHash int32  `json:"objHash"`
	Cmd     string `json:"cmd"`
	Param   string `json:"param"`
}

func goCalls(t *testing.T, q *agentCallQueue) []oracleCall {
	t.Helper()
	out := []oracleCall{}
	for _, c := range takeCalls(q) {
		b, err := protocol.EncodePack(c.param, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, oracleCall{c.hash, c.cmd, hex.EncodeToString(b)})
	}
	return out
}

// ------------------------------------------------------------------ kube

type kubeManagerOut struct {
	Pod     string           `json:"pod"`
	HostSeq [][2]interface{} `json:"hostSeq"`
	SeqHost [][2]interface{} `json:"seqHost"`
}
type kubeInitialOut struct {
	Host    string `json:"host"`
	PodName string `json:"podName"`
	PodSeq  int32  `json:"podSeq"`
	T       int64  `json:"t"`
}
type kubeOut struct {
	AllowProceed *bool            `json:"allowProceed,omitempty"`
	PushSeq      *bool            `json:"pushSeq,omitempty"`
	PodSeq       *int32           `json:"podSeq,omitempty"`
	PodName      *string          `json:"podName"`
	HostName     *string          `json:"hostName"`
	Calls        []oracleCall     `json:"calls"`
	Managers     []kubeManagerOut `json:"managers"`
	Initial      []kubeInitialOut `json:"initial"`
}
type kubeStep struct {
	Op       string      `json:"op"`
	T        int64       `json:"t"`
	ObjName  string      `json:"objName"`
	Tags     oraclePairs `json:"tags"`
	PodName  string      `json:"podName"`
	HostName *string     `json:"hostName"`
	Out      kubeOut     `json:"out"`
}

func (e *Engine) kubeSnapshot(base int64) ([]kubeManagerOut, []kubeInitialOut) {
	managers := []kubeManagerOut{}
	pods := make([]string, 0, len(e.kube.managers))
	for pod := range e.kube.managers {
		pods = append(pods, pod)
	}
	sort.Strings(pods)
	for _, pod := range pods {
		m := e.kube.managers[pod]
		out := kubeManagerOut{Pod: pod, HostSeq: [][2]interface{}{}, SeqHost: [][2]interface{}{}}
		hosts := make([]string, 0, len(m.hostSeq))
		for h := range m.hostSeq {
			hosts = append(hosts, h)
		}
		sort.Strings(hosts)
		for _, h := range hosts {
			out.HostSeq = append(out.HostSeq, [2]interface{}{h, float64(m.hostSeq[h])})
		}
		seqs := make([]int32, 0, len(m.seqHost))
		for s := range m.seqHost {
			seqs = append(seqs, s)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		for _, s := range seqs {
			out.SeqHost = append(out.SeqHost, [2]interface{}{float64(s), m.seqHost[s]})
		}
		managers = append(managers, out)
	}
	initial := []kubeInitialOut{}
	for host, el := range e.kube.initial {
		v := el.Value.(*kubeInitial)
		initial = append(initial, kubeInitialOut{Host: host, PodName: v.podName, PodSeq: v.podSeq, T: v.initialTime - base})
	}
	sort.Slice(initial, func(i, j int) bool { return initial[i].Host < initial[j].Host })
	return managers, initial
}

func normalizeJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var x any
	if err := json.Unmarshal(b, &x); err != nil {
		t.Fatal(err)
	}
	b, _ = json.Marshal(x)
	return string(b)
}

func TestJavaOraclePodSeqManager(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	base := now.UnixMilli()
	e, _ := testEngineWith(t, &now, nil)
	for i, raw := range loadOracle(t, "kube") {
		var st kubeStep
		if err := json.Unmarshal(raw, &st); err != nil {
			t.Fatal(err)
		}
		now = time.UnixMilli(base + st.T).UTC()
		got := kubeOut{}
		e.mu.Lock()
		switch st.Op {
		case "active":
			p := &protocol.ObjectPack{ObjName: st.ObjName, ObjType: "tomcat", ObjHash: storage.CounterHash(st.ObjName), Tags: st.Tags.mapValue(t)}
			r, err := e.activeKubeLocked(p, now.UnixMilli())
			if err != nil {
				e.mu.Unlock()
				t.Fatalf("step %d: %v", i, err)
			}
			allow, push, seq := r.allowProceed, r.pushSeq, r.podSeq
			got.AllowProceed, got.PushSeq, got.PodSeq = &allow, &push, &seq
			if r.applied {
				pod := r.podName
				got.PodName = &pod
				if r.hostOK {
					host := r.hostName
					got.HostName = &host
				}
			}
		case "inactive":
			host := ""
			if st.HostName != nil {
				host = *st.HostName
			}
			e.objectInactivated(st.PodName, host, st.HostName != nil)
		default:
			e.mu.Unlock()
			t.Fatalf("op %q", st.Op)
		}
		got.Managers, got.Initial = e.kubeSnapshot(base)
		e.mu.Unlock()
		got.Calls = goCalls(t, e.kubeCalls)
		if a, b := normalizeJSON(t, got), normalizeJSON(t, st.Out); a != b {
			t.Fatalf("step %d %s %s at %d:\n go   %s\n java %s", i, st.Op, st.ObjName, st.T, a, b)
		}
	}
}

// ------------------------------------------------------------------- cpu

type cpuStep struct {
	Op       string      `json:"op"`
	ObjName  string      `json:"objName"`
	ObjType  string      `json:"objType"`
	Tags     oraclePairs `json:"tags"`
	Time     int64       `json:"time"`
	TimeType byte        `json:"timetype"`
	Data     oraclePairs `json:"data"`
	Triggers int         `json:"triggers"`
	Out      struct {
		First              *int64       `json:"first"`
		Calls              []oracleCall `json:"calls"`
		QueuedWhileBlocked int          `json:"queuedWhileBlocked"`
		Called             uint64       `json:"called"`
	} `json:"out"`
}

func bundledTypes(t *testing.T) func(string) (ObjectTypeInfo, bool) {
	t.Helper()
	xml, err := os.ReadFile(filepath.Join("..", "..", "cmd", "scouter-go", "counters.xml"))
	if err != nil {
		t.Fatal(err)
	}
	engine := counters.NewEngine(0)
	if err := engine.Parse(xml); err != nil {
		t.Fatal(err)
	}
	return func(objType string) (ObjectTypeInfo, bool) {
		ot := engine.ObjectType(objType)
		if ot == nil {
			return ObjectTypeInfo{}, false
		}
		info := ObjectTypeInfo{SubObject: ot.SubObject}
		if ot.Family != nil {
			info.Family = ot.Family.Name
		}
		return info, true
	}
}

func TestJavaOracleObjectCpuChecker(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	gate := make(chan struct{})
	var entered sync.WaitGroup
	var blocking bool
	var mu sync.Mutex
	call := func(ctx context.Context, hash int32, cmd string, p *protocol.MapPack) ([]protocol.Pack, error) {
		mu.Lock()
		b := blocking
		mu.Unlock()
		if b {
			entered.Done()
			select {
			case <-gate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return nil, nil
	}
	e, _ := testEngineWith(t, &now, func(c *Config) { c.ObjectType = bundledTypes(t); c.AgentCall = call })
	ctx := context.Background()
	for i, raw := range loadOracle(t, "cpu") {
		var st cpuStep
		if err := json.Unmarshal(raw, &st); err != nil {
			t.Fatal(err)
		}
		switch st.Op {
		case "agent":
			if err := e.Accept(ctx, &protocol.ObjectPack{ObjName: st.ObjName, ObjType: st.ObjType, Tags: st.Tags.mapValue(t)}); err != nil {
				t.Fatal(err)
			}
			continue
		case "counter":
			p := &protocol.PerfCounterPack{ObjName: st.ObjName, Time: st.Time, TimeType: st.TimeType, Data: st.Data.mapValue(t)}
			if err := e.Accept(ctx, p); err != nil {
				t.Fatalf("step %d: %v", i, err)
			}
			first, ok := e.cpuFirst[st.ObjName]
			javaStreak := st.Out.First != nil && *st.Out.First > 0
			if ok != javaStreak || (ok && first != *st.Out.First) {
				t.Fatalf("step %d %s@%d: go streak %v/%d, java first %v", i, st.ObjName, st.Time, ok, first, st.Out.First)
			}
			if got := goCalls(t, e.cpuCalls); !reflect.DeepEqual(got, st.Out.Calls) {
				t.Fatalf("step %d calls:\n go   %+v\n java %+v", i, got, st.Out.Calls)
			}
		case "saturate":
			// Java: the daemon holds one call while RequestQueue(100) fills.
			if err := e.Accept(ctx, &protocol.ObjectPack{ObjName: st.ObjName, ObjType: "tomcat", Tags: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{
				{Key: "ADC", Value: protocol.Value{Type: protocol.ValueBoolean, Bool: true}}, {Key: "ADC_TH", Value: decimal(0)}, {Key: "ADC_D", Value: decimal(-1)}}}}); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			blocking = true
			mu.Unlock()
			entered.Add(1)
			runCtx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			go func() { defer close(done); e.RunAgentCalls(runCtx) }()
			before := e.Stats().CPUDumpCalls()
			counter := func(i int) {
				high := decimal(1)
				if err := e.Accept(ctx, cpuCounter(st.ObjName, now.UnixMilli()+int64(i), &high)); err != nil {
					t.Fatal(err)
				}
			}
			counter(0)
			entered.Wait()
			mu.Lock()
			blocking = false
			mu.Unlock()
			for j := 1; j < st.Triggers; j++ {
				counter(j)
			}
			if p := e.Stats().CPUDumpCalls().Pending; p != st.Out.QueuedWhileBlocked {
				t.Fatalf("queued while blocked: go %d java %d", p, st.Out.QueuedWhileBlocked)
			}
			close(gate)
			waitFor(t, func() bool { return e.Stats().CPUDumpCalls().Done-before.Done == st.Out.Called })
			if d := e.Stats().CPUDumpCalls().Dropped - before.Dropped; d != uint64(st.Triggers)-st.Out.Called {
				t.Fatalf("dropped %d", d)
			}
			cancel()
			<-done
		default:
			t.Fatalf("op %q", st.Op)
		}
	}
}

// ----------------------------------------------------------------- geoip

type oracleGeo struct {
	calls    int
	table    map[string]*[2]string // nil value: not in the database
	fallback *[2]string
}

func (g *oracleGeo) Lookup(ip [4]byte) (geoip.Location, bool, error) {
	g.calls++
	v, ok := g.table[netip.AddrFrom4(ip).String()]
	if !ok {
		v = g.fallback
	}
	if v == nil {
		return geoip.Location{}, false, nil
	}
	return geoip.Location{CountryCode: v[0], City: v[1]}, true, nil
}

type geoEntry struct {
	IP      []int   `json:"ip"`
	Found   bool    `json:"found"`
	Country *string `json:"country"`
	City    *string `json:"city"`
}
type geoStep struct {
	Op       string     `json:"op"`
	OK       bool       `json:"ok"`
	Entries  []geoEntry `json:"entries"`
	Fallback *struct {
		Country *string `json:"country"`
		City    *string `json:"city"`
	} `json:"fallback"`
	IP             []int           `json:"ip"`
	CountryCode    *string         `json:"countryCode"`
	First          int             `json:"first"`
	Start          int             `json:"start"`
	Count          int             `json:"count"`
	DistinctCities bool            `json:"distinctCities"`
	Firsts         []int           `json:"firsts"`
	Out            json.RawMessage `json:"out"`
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func ipBytes(ip []int) []byte {
	if ip == nil {
		return nil
	}
	b := make([]byte, len(ip))
	for i, o := range ip {
		b[i] = byte(o)
	}
	return b
}

func TestJavaOracleGeoIpUtil(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, nil)
	var texts [][3]any
	e.geo.putText = func(_ string, p *protocol.TextPack) error {
		texts = append(texts, [3]any{p.XType, float64(p.Hash), p.Text})
		return nil
	}
	geo := &oracleGeo{table: map[string]*[2]string{}}
	apply := func(ip []int, country string) *protocol.XLogPack {
		p := &protocol.XLogPack{IPAddress: ipBytes(ip), CountryCode: country, TailVersion: protocol.XLogCurrentTail}
		e.mu.Lock()
		defer e.mu.Unlock()
		e.applyGeoIPLocked(p, "20261015")
		return p
	}
	lookups := 0
	delta := func() int { d := geo.calls - lookups; lookups = geo.calls; return d }
	for i, raw := range loadOracle(t, "geoip") {
		var st geoStep
		if err := json.Unmarshal(raw, &st); err != nil {
			t.Fatal(err)
		}
		switch st.Op {
		case "db":
			if st.OK {
				e.config.GeoIP = geo
			} else {
				e.config.GeoIP = nil
			}
		case "table":
			geo.table, geo.fallback = map[string]*[2]string{}, nil
			for _, en := range st.Entries {
				key := netip.AddrFrom4([4]byte(ipBytes(en.IP))).String()
				if en.Found {
					geo.table[key] = &[2]string{deref(en.Country), deref(en.City)}
				} else {
					geo.table[key] = nil
				}
			}
			if st.Fallback != nil {
				geo.fallback = &[2]string{deref(st.Fallback.Country), deref(st.Fallback.City)}
			}
		case "xlog":
			texts = nil
			p := apply(st.IP, deref(st.CountryCode))
			var want struct {
				CountryCode *string  `json:"countryCode"`
				City        int32    `json:"city"`
				Lookups     int      `json:"lookups"`
				Texts       [][3]any `json:"texts"`
			}
			if err := json.Unmarshal(st.Out, &want); err != nil {
				t.Fatal(err)
			}
			got := texts
			if got == nil {
				got = [][3]any{}
			}
			if p.CountryCode != deref(want.CountryCode) || p.City != want.City || delta() != want.Lookups || !reflect.DeepEqual(got, want.Texts) {
				t.Fatalf("step %d %v: go %q/%d texts %v, java %+v", i, st.IP, p.CountryCode, p.City, got, want)
			}
		case "bulk":
			if st.DistinctCities {
				geo.table, geo.fallback = map[string]*[2]string{}, nil
				for j := st.Start; j < st.Start+st.Count; j++ {
					geo.table[netip.AddrFrom4([4]byte(bulkIP(st.First, j))).String()] = &[2]string{"ZZ", fmt.Sprint("c", j)}
				}
			}
			texts = nil
			for j := st.Start; j < st.Start+st.Count; j++ {
				b := bulkIP(st.First, j)
				apply([]int{int(b[0]), int(b[1]), int(b[2]), int(b[3])}, "")
			}
			var want struct{ Lookups, Texts int }
			if err := json.Unmarshal(st.Out, &want); err != nil {
				t.Fatal(err)
			}
			if d := delta(); d != want.Lookups || len(texts) != want.Texts {
				t.Fatalf("step %d bulk: go lookups %d texts %d, java %+v", i, d, len(texts), want)
			}
		case "sweep":
			want := map[string][]int{}
			if err := json.Unmarshal(st.Out, &want); err != nil {
				t.Fatal(err)
			}
			for _, a := range st.Firsts {
				looked := []int{}
				for b := 0; b < 256; b++ {
					apply([]int{a, b, 7, 7}, "")
					if delta() > 0 {
						looked = append(looked, b)
					}
				}
				if !reflect.DeepEqual(looked, want[strconv.Itoa(a)]) {
					t.Fatalf("sweep %d: go %v java %v", a, looked, want[strconv.Itoa(a)])
				}
			}
		default:
			t.Fatalf("op %q", st.Op)
		}
	}
}

func bulkIP(first, i int) []byte {
	return []byte{byte(first), byte(i >> 16), byte(i >> 8), byte(i)}
}
