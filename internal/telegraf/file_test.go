package telegraf

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sampleXML(t testing.TB) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "server", "telegraf-sample.xml"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func sampleConfig(t testing.TB, now time.Time) *Config {
	t.Helper()
	f, warnings, err := ParseFile(sampleXML(t))
	if err != nil || len(warnings) != 0 {
		t.Fatal(err, warnings)
	}
	cfg, err := DefaultConfig().ApplyFile(f, now)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestSampleRoundTrip(t *testing.T) {
	data := sampleXML(t)
	f, warnings, err := ParseFile(data)
	if err != nil || len(warnings) != 0 {
		t.Fatal(err, warnings)
	}
	if got := f.Marshal(); !bytes.Equal(got, data) {
		t.Fatalf("marshal differs from TgConfig.getSampleContents():\n%s", got)
	}
	if !f.Enabled || !f.DebugEnabled || f.DeltaCounterNormalizeDefaultSeconds != 30 || f.ObjectDeadtimeMs != 35000 || len(f.Measurements) != 7 {
		t.Fatalf("%+v", f)
	}
	cpu := f.Measurements[0]
	if *cpu.MeasurementName != "cpu" || cpu.ObjFamilyBase != "HOST_METRIC" || len(cpu.ObjFamilyAppendTags) != 0 || len(cpu.ObjTypePrependTags) != 1 || len(cpu.TagFilters) != 1 || cpu.TagFilters[0].Match[0] != "cpu-total" || len(cpu.CounterMappings) != 5 {
		t.Fatalf("%+v", cpu)
	}
	mysql := f.Measurements[5]
	if *mysql.MeasurementName != "mysql" || mysql.ObjNameBase != "" {
		t.Fatalf("%+v", mysql)
	}
}

func TestParseFileDefaultsAndWrappers(t *testing.T) {
	f, warnings, err := ParseFile([]byte(`<TelegrafConfig><enabled> false </enabled><objectDeadtimeMs>x</objectDeadtimeMs><measurements>
<measurement><measurementName>a</measurementName><counterMappings><counterMapping><tgFieldName>f</tgFieldName><counterName>c</counterName><deltaType>BAD</deltaType></counterMapping></counterMappings></measurement>
<measurement><measurementName>b</measurementName><objTypePrependTags/><unknown>ignored</unknown></measurement>
</measurements></TelegrafConfig>`))
	if err != nil {
		t.Fatal(err)
	}
	if f.Enabled || f.ObjectDeadtimeMs != 0 || !f.DeltaCounterNormalizeDefault || len(warnings) != 2 { // JAXB RI: invalid int is 0
		t.Fatalf("%+v %v", f, warnings)
	}
	a, b := f.Measurements[0], f.Measurements[1]
	if len(a.ObjTypePrependTags) != 1 || a.ObjTypePrependTags[0] != DefaultTypePrependTag || a.HostTag != "host" || !a.Enabled {
		t.Fatalf("absent wrapper must keep the default: %+v", a)
	}
	if b.ObjTypePrependTags == nil || len(b.ObjTypePrependTags) != 0 {
		t.Fatalf("empty wrapper must clear the default: %+v", b)
	}
	cm := a.CounterMappings[0]
	if cm.DeltaType != "" || cm.NormalizeSeconds != 30 || cm.Totalizable || cm.DisplayName != nil {
		t.Fatalf("%+v", cm)
	}
	for _, bad := range []string{`<Other/>`, `<TelegrafConfig>`, ``} {
		if _, _, err := ParseFile([]byte(bad)); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
	if _, _, err := ParseFile(make([]byte, MaxFileBytes+1)); err == nil {
		t.Fatal("oversized file accepted")
	}
}

func TestMarshalOmitsNullAndEscapes(t *testing.T) {
	m := NewMeasurementFile()
	name := "a&b"
	m.MeasurementName = &name
	m.TagFilters = []TagFilter{{Tag: &name, Match: []string{"<x>"}}}
	cm := NewCounterMapping()
	cm.DeltaType = ""
	m.CounterMappings = []CounterMapping{cm}
	out := string((&File{Measurements: []MeasurementFile{m}}).Marshal())
	for _, want := range []string{"<measurementName>a&amp;b</measurementName>", "<match>&lt;x&gt;</match>", "<counterMapping>\n                    <unit></unit>"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in\n%s", want, out)
		}
	}
	if strings.Contains(out, "deltaType") || strings.Contains(out, "tgFieldName") {
		t.Fatal(out)
	}
	if !strings.Contains(string((&File{}).Marshal()), "    <measurements/>\n") {
		t.Fatal("empty measurements wrapper")
	}
}
