package telegraf

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseLine(t *testing.T) {
	cases := []struct {
		in   string
		want Line
	}{
		{"cpu,cpu=cpu-total,host=h1 usage_user=1.5,usage_system=2i 1700000000000000000",
			Line{"cpu", []KV{{"cpu", "cpu-total"}, {"host", "h1"}}, []KV{{"usage_user", "1.5"}, {"usage_system", "2i"}}, "1700000000000000000"}},
		{`cpu,host=a\ b,path=/x\,y usage_user=1 1`,
			Line{"cpu", []KV{{"host", "a b"}, {"path", "/x,y"}}, []KV{{"usage_user", "1"}}, "1"}},
		{`m\,x,host=h s="a b,c=d",v=1 1`,
			Line{"m,x", []KV{{"host", "h"}}, []KV{{"s", ""}, {"v", "1"}}, "1"}},
		{`m,host=h s="a\"b",v=t 9`, // a quoted backslash is ignored: \" closes and the next " reopens
			Line{"m", []KV{{"host", "h"}}, nil, ""}},
		{`m,host=h b=true,v=t 9`, Line{"m", []KV{{"host", "h"}}, []KV{{"b", "true"}, {"v", "t"}}, "9"}},
		{`m,host=h s="a\"b c",v=1 1`, // D12: \" closes the quote, the space starts the timestamp
			Line{"m", []KV{{"host", "h"}}, []KV{{"s", "b"}}, `c",v=1 1`}},
		{"m,t=1,t=2 v=1,v=2 5 6", Line{"m", []KV{{"t", "2"}}, []KV{{"v", "2"}}, "5 6"}},
		{"m,a=x=y v=1 1", Line{"m", []KV{{"a", "xy"}}, []KV{{"v", "1"}}, "1"}},
		{"cpu value=1 1", Line{Measurement: "cpu value=1 1"}},     // D1: no tags
		{"m,host=h v=1", Line{"m", []KV{{"host", "h"}}, nil, ""}}, // no timestamp: last field not stored
		{"m,host=한글 v=1i 2", Line{"m", []KV{{"host", "한글"}}, []KV{{"v", "1i"}}, "2"}},
		{"", Line{}},
	}
	for _, c := range cases {
		if got := ParseLine(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("ParseLine(%q)\n got %#v\nwant %#v", c.in, got, c.want)
		}
	}
}

func TestLineKey(t *testing.T) {
	cases := map[string]string{
		"cpu,host=h1 usage_user=1 1":             "cpu,host=h1 usage_user",
		"cpu,host=h1 usage_user=2 2":             "cpu,host=h1 usage_user",
		`cpu,host=a\ b f\=x=1,g=2 3`:             "cpu,host=a b f=x",
		"m,a=b f,g=1":                            "m,a=b f",
		"cpu":                                    "cpu",
		"cpu,host=h1 usage_user=1,usage_sys=2 1": "cpu,host=h1 usage_user",
	}
	for in, want := range cases {
		if got := LineKey(in); got != want {
			t.Errorf("LineKey(%q)=%q want %q", in, got, want)
		}
	}
}

func TestSplitLines(t *testing.T) {
	got := splitLines("a\nb\r\nc\rd\n\ne\n", 100)
	if want := []string{"a", "b", "c", "d", "", "e"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("%q", got)
	}
	if got := splitLines("a\nb\nc", 2); len(got) != 2 {
		t.Fatalf("limit: %q", got)
	}
	if got := splitLines("", 5); len(got) != 0 {
		t.Fatalf("%q", got)
	}
}

func TestParseNumber(t *testing.T) {
	cases := []struct {
		in    string
		typ   byte
		i     int64
		f     float32
		ok    bool
		error bool
	}{
		{"-5i", 20, -5, 0, true, false},
		{"+7i", 20, 7, 0, true, false},
		{"9223372036854775808i", 0, 0, 0, false, true},
		{"i", 0, 0, 0, false, true},
		{"1.5", 30, 0, 1.5, true, false},
		{"1e3", 30, 0, 1000, true, false},
		{"0x1p3", 30, 0, 8, true, false},
		{"1e50", 30, 0, float32Inf(), true, false},
		{"1_0", 0, 0, 0, false, true},
		{"abc1", 0, 0, 0, false, true},
		{"true", 0, 0, 0, false, false},
		{"t", 0, 0, 0, false, false},
		{"5u", 0, 0, 0, false, false},
		{"", 0, 0, 0, false, true},
	}
	for _, c := range cases {
		v, ok, err := parseNumber(c.in)
		if ok != c.ok || (err != nil) != c.error || (ok && (v.Type != c.typ || v.Int != c.i || v.Float != c.f)) {
			t.Errorf("parseNumber(%q) = %+v %v %v", c.in, v, ok, err)
		}
	}
}

func float32Inf() float32 {
	var zero float32
	return 1 / zero
}

// FuzzLineProtocol checks that the scan, key, resolve and conversion never
// panic and that resolved points only carry mapped numeric fields.
func FuzzLineProtocol(f *testing.F) {
	for _, s := range []string{
		"cpu,cpu=cpu-total,host=h1 usage_user=1.5,usage_system=2i 1700000000000000000",
		`redis_keyspace,host=a\ b,port=6379,database=db0 keys=10i,expires=1i 1700000000000000000`,
		`nginx,host=h,port=80 accepts=1i,handled="x y",requests=t 1`,
		"mem,host=h used_percent=,free=1e400 9",
		"\\", "a,\\", "a,b=\"", "a,b=c d=\"\\\"\" 1",
	} {
		f.Add(s)
	}
	now := time.Unix(1700000000, 0)
	cfg := sampleConfig(f, now)
	conv := NewConverter(Limits{PrevValues: 16, Meters: 16}, func() time.Time { return now })
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > MaxLineBytes {
			return
		}
		line := ParseLine(s)
		key := LineKey(s)
		if len(key) > len(s) {
			t.Fatalf("key longer than line: %q", key)
		}
		if !strings.Contains(s, ",") && line.Tags != nil {
			t.Fatalf("tags without a comma: %#v", line)
		}
		p, reason := Resolve(&line, cfg, now.UnixMilli())
		if (p == nil) == (reason == Accepted) {
			t.Fatalf("reason %v with point %v", reason, p)
		}
		if p == nil {
			return
		}
		for _, field := range p.Fields {
			if field.Value.Type != 20 && field.Value.Type != 30 {
				t.Fatalf("field type %d", field.Value.Type)
			}
		}
		ObjectPack(p, "10.0.0.1", cfg.ObjectDeadtimeMs)
		conv.PerfCounterPack(p, cfg)
	})
}

// A maximal line with thousands of distinct keys stays linear, and repeated
// keys after the index switch keep the first position and the last value.
func TestParseLineManyKeys(t *testing.T) {
	var b strings.Builder
	b.WriteString("m")
	for i := 0; b.Len() < MaxLineBytes/2; i++ {
		fmt.Fprintf(&b, ",t%d=v", i)
	}
	b.WriteString(",t0=last f0=1")
	for i := 1; b.Len() < MaxLineBytes-32; i++ {
		fmt.Fprintf(&b, ",f%d=%d", i, i)
	}
	b.WriteString(",f0=9 1")
	start := time.Now()
	line := ParseLine(b.String())
	if time.Since(start) > time.Second {
		t.Fatal("quadratic key handling")
	}
	if line.Tags[0] != (KV{"t0", "last"}) || line.Fields[0] != (KV{"f0", "9"}) || len(line.Tags) < 3000 || len(line.Fields) < 3000 || line.Timestamp != "1" {
		t.Fatalf("%v %v %d %d", line.Tags[0], line.Fields[0], len(line.Tags), len(line.Fields))
	}
	if v, ok := line.Tag("t17"); !ok || v != "v" {
		t.Fatal(v, ok)
	}
}
