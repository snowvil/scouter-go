package logger

import (
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func loadJSON(t *testing.T, name string, v any) {
	t.Helper()
	b, err := os.ReadFile("testdata/java/" + name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

func mustLoc(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

type packGolden struct {
	Timezone string `json:"timezone"`
	Packs    []struct {
		Name, Pack string
		Text       *string
	} `json:"packs"`
	Values []struct {
		Name, Value string
		Text        *string
	} `json:"values"`
	ProfileDebug []struct {
		Name, Pack string
		SvcName    *string `json:"svc_name"`
		Text       string
	} `json:"profile_debug"`
}

func decodePackHex(t *testing.T, s string) protocol.Pack {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	p, err := protocol.DecodePack(b, protocol.Limits{})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return p
}

// maskedPacks is the allowlist of Java golden cases whose text differs by
// design (D-S1, account text): the Go line keeps "<xtype> <hash> " and replaces
// the text with a length/hash mask.
var maskedPacks = map[string]bool{"text_sql": true, "text_sql_unicode": true, "text_login": true}

func TestFormatPackJavaGolden(t *testing.T) {
	var g packGolden
	loadJSON(t, "packstring.json", &g)
	loc := mustLoc(t, g.Timezone)
	if len(g.Packs) < 35 {
		t.Fatalf("golden has %d packs", len(g.Packs))
	}
	for _, c := range g.Packs {
		p := decodePackHex(t, c.Pack)
		got := FormatPack(p, loc)
		if maskedPacks[c.Name] {
			tp := p.(*protocol.TextPack)
			prefix := tp.XType + " " + javaHexa32(int64(tp.Hash)) + " "
			if !strings.HasPrefix(*c.Text, prefix) || *c.Text != prefix+tp.Text {
				t.Fatalf("%s: java golden shape changed: %q", c.Name, *c.Text)
			}
			if strings.Contains(got, tp.Text) || !strings.HasPrefix(got, prefix+"<"+tp.XType+" masked len="+strconv.Itoa(len(tp.Text))+" hash=") {
				t.Fatalf("%s: not masked: %q", c.Name, got)
			}
			continue
		}
		if got != *c.Text {
			t.Errorf("%s:\n go   %q\n java %q", c.Name, got, *c.Text)
		}
	}
}

func TestFormatValueJavaGolden(t *testing.T) {
	var g packGolden
	loadJSON(t, "packstring.json", &g)
	for _, c := range g.Values {
		b, _ := hex.DecodeString(c.Value)
		v, err := protocol.DecodeValue(b, protocol.Limits{})
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		want := "null" // Java toString() returned null; concatenation prints "null"
		if c.Text != nil {
			want = *c.Text
		}
		if got := FormatValue(v); got != want {
			t.Errorf("%s:\n go   %q\n java %q", c.Name, got, want)
		}
	}
}

func TestProfileDebugJavaGolden(t *testing.T) {
	var g packGolden
	loadJSON(t, "packstring.json", &g)
	for _, c := range g.ProfileDebug {
		p := decodePackHex(t, c.Pack).(*protocol.ProfilePack)
		svc := JavaNull
		if c.SvcName != nil {
			svc = *c.SvcName
		}
		if got := ProfileDebugString(p, svc); got != c.Text {
			t.Errorf("%s:\n go   %q\n java %q", c.Name, got, c.Text)
		}
		line := ProfileTestLine(ProfileCaseNormal, p, svc)
		if want := strconv.Itoa(len(p.Profile)) + "\t[ProfileCoreCase2] " + c.Text; line != want {
			t.Errorf("%s: test line %q", c.Name, line)
		}
	}
}

func TestNumbersJavaGolden(t *testing.T) {
	var g struct {
		Float  [][2]string `json:"float_to_string"`
		Double [][2]string `json:"double_decimal_format"`
	}
	loadJSON(t, "numbers.json", &g)
	for _, c := range g.Float {
		bits, _ := strconv.ParseUint(c[0], 16, 32)
		if got := javaFloatString(math.Float32frombits(uint32(bits))); got != c[1] {
			t.Errorf("Float.toString(%s): go %q java %q", c[0], got, c[1])
		}
	}
	for _, c := range g.Double {
		bits, _ := strconv.ParseUint(c[0], 16, 64)
		if got := javaDecimalFormat(math.Float64frombits(bits)); got != c[1] {
			t.Errorf("DecimalFormat(%s): go %q java %q", c[0], got, c[1])
		}
	}
	if len(g.Float) < 800 || len(g.Double) < 1000 {
		t.Fatalf("sweep too small: %d floats, %d doubles", len(g.Float), len(g.Double))
	}
}

func TestCalendarJavaGolden(t *testing.T) {
	for _, tz := range []string{"Asia/Seoul", "America/New_York"} {
		var g struct {
			Timezone string
			Cases    []struct {
				Ms           int64
				Datetime     string
				Timestamp    string
				Yyyymmdd     string
				DateUnit     int64  `json:"date_unit"`
				SQLTimestamp string `json:"sql_timestamp"`
			}
			Parse []struct {
				Date     string
				DateUnit any `json:"date_unit"`
			}
		}
		loadJSON(t, "time-"+strings.ReplaceAll(tz, "/", "_")+".json", &g)
		loc := mustLoc(t, g.Timezone)
		c := newCalendar(loc)
		for _, x := range g.Cases {
			if got := c.datetime(x.Ms); got != x.Datetime {
				t.Errorf("%s datetime(%d) = %q, java %q", tz, x.Ms, got, x.Datetime)
			}
			if got := c.timestamp(x.Ms); got != x.Timestamp {
				t.Errorf("%s timestamp(%d) = %q, java %q", tz, x.Ms, got, x.Timestamp)
			}
			if got := c.yyyymmdd(x.Ms); got != x.Yyyymmdd {
				t.Errorf("%s yyyymmdd(%d) = %q, java %q", tz, x.Ms, got, x.Yyyymmdd)
			}
			if got := c.dateUnit(x.Ms); got != x.DateUnit {
				t.Errorf("%s dateUnit(%d) = %d, java %d", tz, x.Ms, got, x.DateUnit)
			}
			if got := sqlTimestamp(x.Ms, loc); got != x.SQLTimestamp {
				t.Errorf("%s Timestamp(%d) = %q, java %q", tz, x.Ms, got, x.SQLTimestamp)
			}
		}
		for _, x := range g.Parse {
			unit, ok := parseDateUnit(x.Date)
			if s, isErr := x.DateUnit.(string); isErr {
				if ok || s != "error" {
					t.Errorf("parse %q: go %d ok=%v, java %v", x.Date, unit, ok, x.DateUnit)
				}
				continue
			}
			if !ok || float64(unit) != x.DateUnit.(float64) {
				t.Errorf("parse %q: go %d ok=%v, java %v", x.Date, unit, ok, x.DateUnit)
			}
		}
	}
}

func TestUDPLogKeys(t *testing.T) {
	cases := []struct {
		p           protocol.Pack
		key, prefix string
	}{
		{&protocol.PerfCounterPack{}, "log_udp_counter", "DEBUG UDP COUNTER: "},
		{&protocol.InteractionPerfCounterPack{}, "log_udp_interaction_counter", "DEBUG UDP INTERACTION COUNTER: "},
		{&protocol.XLogPack{}, "log_udp_xlog", "DEBUG UDP XLOG: "},
		{&protocol.DroppedXLogPack{}, "log_udp_xlog", "DEBUG UDP DROPPED XLOG: "},
		{&protocol.ProfilePack{}, "log_udp_profile", "DEBUG UDP PROFILE: "},
		{&protocol.ProfilePack{Version2: true}, "log_udp_profile", "DEBUG UDP PROFILE: "},
		{&protocol.TextPack{}, "log_udp_text", "DEBUG UDP TEXT: "},
		{&protocol.AlertPack{}, "log_udp_alert", "DEBUG UDP ALERT: "},
		{&protocol.ObjectPack{}, "log_udp_object", "DEBUG UDP OBJECT: "},
		{&protocol.StatusPack{}, "log_udp_status", "DEBUG UDP STATUS: "},
		{&protocol.StackPack{}, "log_udp_stack", "DEBUG UDP STACK: "},
		{&protocol.SummaryPack{}, "log_udp_summary", "DEBUG UDP SUMMARY: "},
		{&protocol.BatchPack{}, "log_udp_batch", "DEBUG UDP Batch: "},
		{&protocol.SpanContainerPack{}, "log_udp_span", "DEBUG UDP SPAN CONTAINER: "},
	}
	for _, c := range cases {
		key, prefix, ok := UDPLog(c.p)
		if !ok || key != c.key || prefix != c.prefix {
			t.Errorf("%T: %q %q %v", c.p, key, prefix, ok)
		}
	}
	for _, p := range []protocol.Pack{&protocol.MapPack{}, &protocol.SpanPack{}, nil} {
		if _, _, ok := UDPLog(p); ok {
			t.Errorf("%T must not have a per-type key", p)
		}
	}
	if FormatPack(nil, time.UTC) != "null" {
		t.Fatal("nil pack")
	}
}

func TestZeroMapFieldsPrintEmptyMap(t *testing.T) {
	loc := time.UTC
	if got := FormatPack(&protocol.AlertPack{}, loc); !strings.HasSuffix(got, " tags={}") {
		t.Fatal(got)
	}
	if got := FormatPack(&protocol.PerfCounterPack{}, loc); !strings.HasSuffix(got, "  {}") {
		t.Fatal(got)
	}
	if got := FormatPack(&protocol.ObjectPack{}, loc); got != "OBJECT  objType= objHash=0 objName=" {
		t.Fatal(got)
	}
}

func TestMaskSQL(t *testing.T) {
	got := MaskSQL("select 1")
	if strings.Contains(got, "select") || !strings.HasPrefix(got, "<sql masked len=8 hash=") {
		t.Fatal(got)
	}
	if MaskSQL("select 1") != got || MaskSQL("select 2") == got {
		t.Fatal("mask must be deterministic and content dependent")
	}
}

func TestSomeObjectLines(t *testing.T) {
	if SomeObjectLine("t") != "DEBUG objType: t" ||
		SomeObjectDetectedLine("t", JavaNull) != "DEBUG objType: t, detected: null" ||
		SomeObjectWillBeAddedLine("t") != "DEBUG objType: t, will be added " ||
		SomeObjectNewXMLLine("t", "<x/>") != "DEBUG objType: t, new XML = <x/>" {
		t.Fatal("some object line format")
	}
	if ProfileCaseDelaying(1, 3) != "[processDelaying][1/3]" {
		t.Fatal(ProfileCaseDelaying(1, 3))
	}
}
