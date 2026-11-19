package telegraf

// Java oracle comparison. The goldens under
// ../service/testdata/management/{telegraf,telegraf-legacy,telegraf-golden}
// are produced by scripts/java-management.py, which runs the unchanged
// TelegrafInputHandler/InfluxSingleLine/CounterProtocol/ScouterTgMtConfig/
// Configure of Scouter 2.21.3 (tools/java-management/README.md).
//
// Java emits packs only after the objType and counters are registered in
// counters.site.xml; the oracle waits for that registration, so each line is
// converted exactly once, like the Go Input with a nil registrar used here.
// Registration itself (and the original empty counters.site.xml NPE of the
// telegraf-fresh scenario) is not compared here; the Go registrar is
// counters.Registrar (internal/counters TestRegistrarOperations).
//
// Intended differences (lead decision, CONTRACT.md "의도된 차이"): Go keeps
// line order after LineKey de-duplication and field order inside a line; Java
// iterates a HashMap of lines and writes MapValue (Hashtable) enumeration
// order, neither of which is a contract. Observed in the oracle: PerfCounterPack
// data order differs for lines with several mapped fields (telegraf
// first-registers-type, second-delta-baseline, third-delta-float, legacy-first,
// legacy-second) and the golden_packs steps "mem ..." (data order) and the two
// "redis BOTH ..." steps (pack order and data order). Packs are therefore
// compared per request as a multiset of packs with MapValue entries sorted:
// every field, value type and exact value bits must match; order is not compared.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

const javaOracleDir = "../service/testdata/management"

func readJavaOracle(t *testing.T, scenario, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(javaOracleDir, scenario, name))
	if err != nil {
		t.Fatalf("Java oracle golden missing (python3 scripts/java-management.py --generate): %v", err)
	}
	return b
}

// simpleProperties reads the oracle's synthetic scouter.conf inputs (plain
// key=value lines without escapes or continuations).
func simpleProperties(t *testing.T, text string) map[string]string {
	t.Helper()
	props := map[string]string{}
	for _, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok || strings.ContainsAny(line, "\\") {
			t.Fatalf("unexpected oracle property line %q", line)
		}
		props[k] = v
	}
	return props
}

// sortedEntries orders MapValue entries by key (Java Hashtable order is not an
// insertion-order contract; see the file comment).
func sortedEntries(v protocol.Value) protocol.Value {
	if v.Type != protocol.ValueMap {
		return v
	}
	v.Entries = append([]protocol.Entry(nil), v.Entries...)
	sort.SliceStable(v.Entries, func(i, j int) bool { return v.Entries[i].Key < v.Entries[j].Key })
	return v
}

func mapOrderNormalizedPack(t *testing.T, p protocol.Pack) []byte {
	t.Helper()
	switch x := p.(type) {
	case *protocol.ObjectPack:
		c := *x
		c.Tags = sortedEntries(x.Tags)
		p = &c
	case *protocol.PerfCounterPack:
		c := *x
		c.Data = sortedEntries(x.Data)
		p = &c
	}
	b, err := protocol.EncodePack(p, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type javaStep struct {
	Name  string `json:"name"`
	Line  string `json:"line"`
	Parse struct {
		Parsed          bool              `json:"parsed"`
		Host            string            `json:"host"`
		Family          string            `json:"family"`
		ObjType         string            `json:"objType"`
		ObjTypeIcon     string            `json:"objTypeIcon"`
		ObjName         string            `json:"objName"`
		ObjHash         int32             `json:"objHash"`
		ReceivedTime    int64             `json:"receivedTime"`
		TimestampOrigin int64             `json:"timestampOrigin"`
		Tags            map[string]string `json:"tags"`
	} `json:"parse"`
	Attempts []struct {
		Packs []struct {
			File string `json:"file"`
			Addr string `json:"addr"`
		} `json:"packs"`
	} `json:"attempts"`
}

// javaReceived and javaRemote are TelegrafOracle.RECEIVED / REMOTE.
const (
	javaReceived = 1767193200123
	javaRemote   = "10.0.0.7"
)

// TestJavaOracleLineConversion replays the telegraf (XML configuration) and
// telegraf-legacy (scouter.conf input_telegraf_$m$_*) scenarios line by line.
func TestJavaOracleLineConversion(t *testing.T) {
	for _, scenario := range []string{"telegraf", "telegraf-legacy"} {
		t.Run(scenario, func(t *testing.T) {
			var root struct {
				Facts struct {
					LineKeys map[string]string `json:"line_string_keys"`
					Steps    []javaStep        `json:"steps"`
				} `json:"facts"`
			}
			if err := json.Unmarshal(readJavaOracle(t, scenario, "cases.json"), &root); err != nil {
				t.Fatal(err)
			}
			if len(root.Facts.Steps) == 0 {
				t.Fatal("no oracle steps")
			}
			for line, key := range root.Facts.LineKeys {
				if got := LineKey(line); got != key {
					t.Errorf("LineKey(%q) = %q, Java %q", line, got, key)
				}
			}
			props := simpleProperties(t, string(readJavaOracle(t, scenario, "scouter.conf.input")))
			var xmlText []byte
			exists := scenario == "telegraf"
			if exists {
				xmlText = readJavaOracle(t, scenario, "scouter-telegraf.input.xml")
			}
			now := time.UnixMilli(javaReceived)
			cfg, warnings, err := Compose(props, xmlText, exists, now)
			if err != nil {
				t.Fatal(err, warnings)
			}
			acc := &fakeAcceptor{}
			in := NewInput(cfg, nil, acc, Options{Clock: func() time.Time { return now }, Logf: quietLogf})
			defer in.Close()
			for _, step := range root.Facts.Steps {
				id := scenario + "/" + step.Name
				line := ParseLine(step.Line)
				point, _ := Resolve(&line, cfg, javaReceived)
				if (point != nil) != step.Parse.Parsed {
					t.Fatalf("%s: Go resolved=%v, Java parsed=%v", id, point != nil, step.Parse.Parsed)
				}
				if point != nil {
					p, j := step.Parse, point
					if j.Host != p.Host || j.Family != p.Family || j.ObjType != p.ObjType || j.ObjTypeIcon != p.ObjTypeIcon || j.ObjName != p.ObjName ||
						j.ObjHash != p.ObjHash || j.ReceivedTime != p.ReceivedTime || j.TimestampMs != p.TimestampOrigin || !reflect.DeepEqual(j.Tags, p.Tags) {
						t.Fatalf("%s: point %+v, Java %+v", id, *j, p)
					}
				}
				if err := in.Handle(context.Background(), step.Line, javaRemote); err != nil {
					t.Fatal(id, err)
				}
				got := acc.take()
				var want []struct {
					File string `json:"file"`
					Addr string `json:"addr"`
				}
				if n := len(step.Attempts); n > 0 {
					want = step.Attempts[n-1].Packs
				}
				if len(got) != len(want) {
					t.Fatalf("%s: Go %d packs, Java %d", id, len(got), len(want))
				}
				// Multiset per line: (peer, pack with sorted MapValue entries).
				unmatched := map[string]int{}
				javaPacks := map[string]protocol.Pack{}
				for _, w := range want {
					jp, err := protocol.DecodePack(readJavaOracle(t, scenario, w.File), protocol.Limits{})
					if err != nil {
						t.Fatal(err)
					}
					key := w.Addr + "|" + fmt.Sprintf("%x", mapOrderNormalizedPack(t, jp))
					unmatched[key]++
					javaPacks[fmt.Sprintf("%T", jp)] = jp
				}
				for i, g := range got {
					key := g.peer + "|" + fmt.Sprintf("%x", mapOrderNormalizedPack(t, g.pack))
					if unmatched[key] == 0 {
						diff := "no Java pack of this type"
						if jp, ok := javaPacks[fmt.Sprintf("%T", g.pack)]; ok {
							diff = packFieldDiff(g.pack, jp)
						}
						t.Errorf("%s: Go pack %d (%T, peer %s) has no Java counterpart; differs in %s", id, i, g.pack, g.peer, diff)
						continue
					}
					unmatched[key]--
				}
			}
		})
	}
}

// packFieldDiff names the differing fields of two packs of the same type.
func packFieldDiff(a, b protocol.Pack) string {
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return fmt.Sprintf("pack type %T vs %T", a, b)
	}
	va, vb := reflect.ValueOf(a).Elem(), reflect.ValueOf(b).Elem()
	var fields []string
	for i := 0; i < va.NumField(); i++ {
		fa, fb := va.Field(i).Interface(), vb.Field(i).Interface()
		if x, ok := fa.(protocol.Value); ok {
			fa, fb = sortedEntries(x), sortedEntries(fb.(protocol.Value))
		}
		if !reflect.DeepEqual(fa, fb) {
			fields = append(fields, fmt.Sprintf("%s(go %v, java %v)", va.Type().Field(i).Name, va.Field(i).Interface(), vb.Field(i).Interface()))
		}
	}
	if len(fields) == 0 {
		return "MapValue entry order only"
	}
	return strings.Join(fields, ", ")
}

// javaGoldenPack is one entry of telegraf-golden/java_packs.json.
type javaGoldenPack struct {
	Type     string `json:"type"`
	Peer     string `json:"peer"`
	ObjHash  int32  `json:"objHash"`
	ObjName  string `json:"objName"`
	ObjType  string `json:"objType"`
	Address  string `json:"address"`
	Time     int64  `json:"time"`
	TimeType byte   `json:"timetype"`
	Values   []struct {
		Key   string `json:"key"`
		Type  string `json:"type"`
		Value string `json:"value"`
		Bits  string `json:"bits"`
	} `json:"values"`
}

// canonicalValue renders a value by type and exact bits (Java gives the IEEE
// bits; Go gives the shortest round-trip string of the float32/float64).
func canonicalValue(t *testing.T, typ, value, bits string) string {
	t.Helper()
	switch typ {
	case "decimal":
		return "decimal:" + value
	case "float":
		if bits != "" {
			return "float:" + bits
		}
		f, err := strconv.ParseFloat(value, 32)
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("float:0x%08x", math.Float32bits(float32(f)))
	case "double":
		if bits != "" {
			return "double:" + bits
		}
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("double:0x%016x", math.Float64bits(f))
	}
	t.Fatalf("unexpected value type %q", typ)
	return ""
}

// canonicalPack is order-insensitive in the values (MapValue entry order is
// an intended difference).
func canonicalPack(header string, values []string) string {
	s := append([]string(nil), values...)
	sort.Strings(s)
	return header + "|" + strings.Join(s, ",")
}

// TestJavaOracleGoldenPacks compares runGolden (testdata/golden_packs.json
// inputs, sample configuration, nil registrar) with the Java handlerRequest
// replay of the same bodies (telegraf-golden scenario).
func TestJavaOracleGoldenPacks(t *testing.T) {
	var inputs struct {
		GoldenStart int64        `json:"goldenStart"`
		Steps       []goldenStep `json:"steps"`
	}
	if err := json.Unmarshal(readJavaOracle(t, "telegraf-golden", "input-steps.json"), &inputs); err != nil {
		t.Fatal(err)
	}
	var local []goldenStep
	data, err := os.ReadFile(filepath.Join("testdata", "golden_packs.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &local); err != nil {
		t.Fatal(err)
	}
	if inputs.GoldenStart != goldenStart || len(inputs.Steps) != len(local) {
		t.Fatalf("Java replay inputs are stale: regenerate with python3 scripts/java-management.py --generate")
	}
	for i := range local {
		a, b := local[i], inputs.Steps[i]
		if a.Name != b.Name || a.OffsetMs != b.OffsetMs || a.Remote != b.Remote || a.Body != b.Body {
			t.Fatalf("Java replay input %d differs from golden_packs.json: regenerate the oracle", i)
		}
	}
	var java []struct {
		Name  string           `json:"name"`
		Packs []javaGoldenPack `json:"packs"`
	}
	if err := json.Unmarshal(readJavaOracle(t, "telegraf-golden", "java_packs.json"), &java); err != nil {
		t.Fatal(err)
	}
	got := runGolden(t, inputs.Steps)
	if len(java) != len(got) {
		t.Fatalf("Java %d steps, Go %d", len(java), len(got))
	}
	for i, step := range got {
		if java[i].Name != step.Name {
			t.Fatalf("step %d: Java %q, Go %q", i, java[i].Name, step.Name)
		}
		var goSorted, javaSorted []string
		for _, p := range step.Packs {
			var values []string
			for _, v := range p.Values {
				values = append(values, v.Key+"="+canonicalValue(t, v.Type, v.Value, ""))
			}
			goSorted = append(goSorted, canonicalPack(fmt.Sprintf("%s|%s|%d|%s|%s|%s|%d|%d", p.Type, p.Peer, p.ObjHash, p.ObjName, p.ObjType, p.Address, p.Time, p.TimeType), values))
		}
		for _, p := range java[i].Packs {
			var values []string
			for _, v := range p.Values {
				values = append(values, v.Key+"="+canonicalValue(t, v.Type, v.Value, v.Bits))
			}
			hash := p.ObjHash
			if p.Type == "perf" {
				hash = 0 // Go golden omits objHash on PerfCounterPack (ingest computes it)
			}
			javaSorted = append(javaSorted, canonicalPack(fmt.Sprintf("%s|%s|%d|%s|%s|%s|%d|%d", p.Type, p.Peer, hash, p.ObjName, p.ObjType, p.Address, p.Time, p.TimeType), values))
		}
		sort.Strings(goSorted)
		sort.Strings(javaSorted)
		if !reflect.DeepEqual(goSorted, javaSorted) {
			t.Errorf("%s: converted packs differ\n go   %q\n java %q", step.Name, goSorted, javaSorted)
		}
	}
}
