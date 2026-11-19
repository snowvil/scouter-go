package httpapi

// Java oracle comparison for /counter (CounterHandler) and /register
// (RegisterHandler). The goldens under ../service/testdata/management/
// {counterhttp,register} come from scripts/java-management.py, which calls the
// unchanged Scouter 2.21.3 handlers with the same bodies
// (tools/java-management/README.md). The oracle calls CounterHandler.process
// and RegisterHandler.process directly, so servlet-level answers (Go 400/413/
// 415 rejections, Content-Type handling) are not part of these goldens.

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
)

const javaOracleDir = "../service/testdata/management"

// netCafe is scouter.net.NetCafe.CAFE, the UDP single-packet prefix that
// CounterHandler.passToNetDataProcessor writes before each Pack.
const netCafe = 0x43414645

func readJavaOracle(t *testing.T, scenario, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(javaOracleDir, scenario, name))
	if err != nil {
		t.Fatalf("Java oracle golden missing (python3 scripts/java-management.py --generate): %v", err)
	}
	return b
}

func sortedMapValue(v protocol.Value) protocol.Value {
	if v.Type != protocol.ValueMap {
		return v
	}
	v.Entries = append([]protocol.Entry(nil), v.Entries...)
	sort.SliceStable(v.Entries, func(i, j int) bool { return v.Entries[i].Key < v.Entries[j].Key })
	return v
}

// packFieldDiff names the differing fields; MapValue order alone is reported
// as such (Java writes Hashtable enumeration order, Go insertion order).
func packFieldDiff(a, b protocol.Pack) string {
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return fmt.Sprintf("pack type %T vs %T", a, b)
	}
	va, vb := reflect.ValueOf(a).Elem(), reflect.ValueOf(b).Elem()
	var fields []string
	for i := 0; i < va.NumField(); i++ {
		fa, fb := va.Field(i).Interface(), vb.Field(i).Interface()
		if x, ok := fa.(protocol.Value); ok {
			fa, fb = sortedMapValue(x), sortedMapValue(fb.(protocol.Value))
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

func TestJavaOracleCounterServlet(t *testing.T) {
	var root struct {
		Facts struct {
			Steps []struct {
				Name    string `json:"name"`
				Body    string `json:"body"`
				Records []struct {
					File string `json:"file"`
					Addr string `json:"addr"`
				} `json:"records"`
			} `json:"steps"`
		} `json:"facts"`
	}
	if err := json.Unmarshal(readJavaOracle(t, "counterhttp", "cases.json"), &root); err != nil {
		t.Fatal(err)
	}
	if len(root.Facts.Steps) == 0 {
		t.Fatal("no oracle steps")
	}
	api, engine := newAPI(t, Config{})
	for _, step := range root.Facts.Steps {
		rec := do(api, "POST", "/counter", step.Body, "Content-Type", "application/json")
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d", step.Name, rec.Code)
		}
		got := engine.take()
		if len(got) != len(step.Records) {
			t.Errorf("%s: Go %d packs, Java %d", step.Name, len(got), len(step.Records))
			continue
		}
		for i, want := range step.Records {
			java := readJavaOracle(t, "counterhttp", want.File)
			p := got[i].pack
			if perf, ok := p.(*protocol.PerfCounterPack); ok {
				// The oracle normalized CounterHandler's System.currentTimeMillis() to 0.
				c := *perf
				c.Time = 0
				p = &c
			}
			raw, err := protocol.EncodePack(p, protocol.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			raw = append(binary.BigEndian.AppendUint32(nil, netCafe), raw...)
			if got[i].peer != want.Addr {
				t.Errorf("%s record %d: peer %q, Java %q", step.Name, i, got[i].peer, want.Addr)
			}
			if bytes.Equal(raw, java) {
				continue
			}
			if binary.BigEndian.Uint32(java) != netCafe {
				t.Fatalf("%s record %d: Java record without CAFE prefix", step.Name, i)
			}
			jp, err := protocol.DecodePack(java[4:], protocol.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			t.Errorf("%s record %d differs in %s\n go   %x\n java %x", step.Name, i, packFieldDiff(p, jp), raw, java)
		}
	}
}

// TestJavaOracleRegisterServlet: Java RegisterHandler.process returns the
// listed results but never changes counters.site.xml or the engine
// (objectType_after.exists=false, management-commands.md: XmlUtil only returns
// the string). Intended difference: Go applies the change through the
// registrar. Results map to the servlet status 201 (true) / 500 (false).
func TestJavaOracleRegisterServlet(t *testing.T) {
	var root struct {
		Facts struct {
			Steps []struct {
				Name            string `json:"name"`
				Body            string `json:"body"`
				Result          bool   `json:"result"`
				ObjectTypeAfter *struct {
					Exists bool `json:"exists"`
				} `json:"objectType_after"`
			} `json:"steps"`
		} `json:"facts"`
	}
	if err := json.Unmarshal(readJavaOracle(t, "register", "cases.json"), &root); err != nil {
		t.Fatal(err)
	}
	if len(root.Facts.Steps) != 5 {
		t.Fatalf("expected 5 oracle steps, got %d", len(root.Facts.Steps))
	}
	reg := newRegistrar()
	api, _ := newAPI(t, Config{Registrar: reg})
	for _, step := range root.Facts.Steps {
		want := http.StatusInternalServerError
		if step.Result {
			want = http.StatusCreated
		}
		if rec := do(api, "POST", "/register", step.Body, "Content-Type", "application/json"); rec.Code != want {
			t.Errorf("%s: status %d, Java result %v", step.Name, rec.Code, step.Result)
		}
		if step.ObjectTypeAfter != nil && step.ObjectTypeAfter.Exists {
			t.Fatalf("%s: the Java golden now shows a registered type; revisit the intended difference", step.Name)
		}
	}
	// Go side effects that the Java original drops.
	if len(reg.added) != 1 || reg.added[0].Name != "reg_redis" || reg.added[0].Master != "aof" || len(reg.added[0].Counters) != 3 {
		t.Fatalf("added %+v", reg.added)
	}
	if len(reg.edited) != 1 || reg.edited[0].Name != "reg_redis" || len(reg.edited[0].Counters) != 1 || reg.edited[0].Counters[0].Name != "mem" {
		t.Fatalf("edited %+v", reg.edited)
	}
	if reg.notified != 2 {
		t.Fatalf("notified %d", reg.notified)
	}
}
