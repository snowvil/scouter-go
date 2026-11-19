package webapp

import (
	"hash/crc32"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func TestAppendJSONNumbersAsStrings(t *testing.T) {
	type inner struct {
		A int8    `json:"a"`
		B uint8   `json:"b"`
		C float32 `json:"c"`
	}
	type view struct {
		Status  int32     `json:"status"`
		Name    string    `json:"name"`
		Ok      bool      `json:"ok"`
		Skip    int       `json:"-"`
		Nil     []int64   `json:"nil"`
		Empty   []int64   `json:"empty"`
		Ptr     *inner    `json:"ptr"`
		NilPtr  *inner    `json:"nilPtr"`
		Any     any       `json:"any"`
		Doubles []float64 `json:"doubles"`
		Obj     jsonObject
		Map     map[string]any `json:"map"`
		Bytes   []byte         `json:"bytes"`
	}
	v := view{Status: 200, Name: "a\"b\\c\n\u0001</>한", Ok: true, Skip: 9, Empty: []int64{}, Ptr: &inner{-1, 255, 0.1},
		Doubles: []float64{1, 1e7, 0.001, 1e-4, 123456789, math.Copysign(0, -1), math.NaN()},
		Obj:     jsonObject{{"z", int64(-5)}, {"a", jsonObject{{"n", nil}}}}, Map: map[string]any{"b": 1, "a": "x"}, Bytes: []byte{1, 2}}
	got, err := appendJSON(nil, v)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"status":"200","name":"a\"b\\c\n\u0001</>한","ok":true,"nil":null,"empty":[],"ptr":{"a":"-1","b":"255","c":"0.1"},"nilPtr":null,"any":null,` +
		`"doubles":["1.0","1.0E7","0.001","1.0E-4","1.23456789E8","-0.0","NaN"],"Obj":{"z":"-5","a":{"n":null}},"map":{"a":"x","b":"1"},"bytes":["1","2"]}`
	if string(got) != want {
		t.Fatalf("\n got %s\nwant %s", got, want)
	}
}

func TestJavaFloatingStrings(t *testing.T) {
	for _, c := range []struct {
		in   float64
		want string
	}{{0, "0.0"}, {1.5, "1.5"}, {9999999, "9999999.0"}, {1e7, "1.0E7"}, {0.00099, "9.9E-4"}, {-12345678.9, "-1.23456789E7"}, {math.Inf(1), "Infinity"}, {100, "100.0"}} {
		if got := javaDoubleString(c.in); got != c.want {
			t.Errorf("double %v = %s want %s", c.in, got, c.want)
		}
	}
	if got := javaFloatString(0.1); got != "0.1" {
		t.Errorf("float 0.1 = %s", got)
	}
	if got := javaFloatString(1.0e10); got != "1.0E10" {
		t.Errorf("float 1e10 = %s", got)
	}
}

func TestDecimalFormatAndCString(t *testing.T) {
	for _, c := range []struct {
		d          float64
		minF, maxF int
		want       string
	}{{1.5, 0, 0, "2"}, {2.5, 0, 0, "2"}, {-0.4, 0, 0, "-0"}, {0.1, 1, 17, "0.1"}, {3, 1, 17, "3.0"}, {1.25, 1, 1, "1.2"}, {1.35, 1, 1, "1.4"}, {9.96, 1, 1, "10.0"}} {
		if got := decimalFormat(c.d, c.minF, c.maxF); got != c.want {
			t.Errorf("decimalFormat(%v,%d,%d)=%s want %s", c.d, c.minF, c.maxF, got, c.want)
		}
	}
	if got := cString(protocol.Value{Type: protocol.ValueDecimal, Int: 42}); got != "42" {
		t.Errorf("cString decimal %v", got)
	}
	if got := cString(protocol.Value{Type: protocol.ValueDouble, Double: 2.5}); got != "2" {
		t.Errorf("cString double %v", got)
	}
	if got := cString(protocol.Value{Type: protocol.ValueNull}); got != nil {
		t.Errorf("cString null %v", got)
	}
	if got := valueToString(protocol.Value{Type: protocol.ValueDouble, Double: 0.1}); got != "0.1" {
		t.Errorf("DoubleValue.toString %v", got)
	}
}

func TestHexa32AndHash(t *testing.T) {
	for _, n := range []int64{0, 5, 9, 10, 31, 32, -1, -12345, math.MaxInt64, math.MinInt64} {
		s := hexa32(n)
		back, err := parseHexa32(s)
		if err != nil || back != n {
			t.Fatalf("hexa32 %d -> %s -> %d %v", n, s, back, err)
		}
	}
	if hexa32(10) != "xa" || hexa32(-1) != "z1" || hexa32(9) != "9" {
		t.Fatal(hexa32(10), hexa32(-1))
	}
	if javaHash("127.0.0.16100") != int32(crc32.ChecksumIEEE([]byte("127.0.0.16100"))) {
		t.Fatal("javaHash")
	}
	if javaStringHash("Cpu") != 67976 || javaStringHash("") != 0 {
		t.Fatal(javaStringHash("Cpu"))
	}
}

func TestSplitAndOrder(t *testing.T) {
	if got := splitParam("[a,b]"); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatal(got)
	}
	if got := splitParam("a,,b,"); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatal(got)
	}
	if got := splitParam(" a, b"); !reflect.DeepEqual(got, []string{" a", " b"}) {
		t.Fatal(got)
	}
	if got := javaSplit("V1.tok.x1.user.name", '.', 4); !reflect.DeepEqual(got, []string{"V1", "tok", "x1", "user.name"}) {
		t.Fatal(got)
	}
	if got := javaSplit("..a..b", '.', 0); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatal(got)
	}
	if _, err := splitParamInt32("objHashes", "1,x"); err == nil {
		t.Fatal("bad int accepted")
	}
	// HashSet<Integer>{17,1,33}: all bucket 1 -> insertion order; {3,1,2} -> 1,2,3.
	if got := javaIntSetOrder([]int32{17, 1, 33}); !reflect.DeepEqual(got, []int32{17, 1, 33}) {
		t.Fatal(got)
	}
	if got := javaIntSetOrder([]int32{3, 1, 2, 3}); !reflect.DeepEqual(got, []int32{1, 2, 3}) {
		t.Fatal(got)
	}
	// Negative Integer hash spreads high bits: -1 -> bucket 0xffff^... & 15 = 0.
	if got := javaIntSetOrder([]int32{5, -1}); !reflect.DeepEqual(got, []int32{-1, 5}) {
		t.Fatal(got)
	}
}

func TestStrMatch(t *testing.T) {
	for _, c := range []struct {
		p, t string
		want bool
	}{{"*", "x", true}, {"10.*", "10.1.2.3", true}, {"10.*", "11.1", false}, {"*.5", "1.2.3.5", true}, {"*2.3*", "1.2.3.4", true},
		{"10.*.5", "10.0.0.5", true}, {"10.*.5", "10.0.0.6", false}, {"*1*3*", "0123", true}, {"10.*.*", "10.1.x", true}, {"a", "a", true}, {"x", "", false}} {
		if got := newStrMatch(c.p).include(c.t); got != c.want {
			t.Errorf("StrMatch(%q).include(%q)=%v", c.p, c.t, got)
		}
	}
}

func TestCanonicalNameAndHeapParsing(t *testing.T) {
	for in, want := range map[string]string{"[I": "int[]", "[[Ljava.lang.String;": "java.lang.String[][]", "java.util.HashMap": "java.util.HashMap",
		"Lfoo": "foo", "[B": "byte[]", "V": "void"} {
		if got := canonicalName(in); got != want {
			t.Errorf("canonicalName(%q)=%q want %q", in, got, want)
		}
	}
	if javaCint("12") != 12 || javaCint("x") != 0 || javaClong("-7") != -7 || javaCint("99999999999") != 0 {
		t.Fatal("cint/clong")
	}
}

func TestCounterEngineParse(t *testing.T) {
	e := newCounterEngine()
	xml := `<Counters><Familys>
<Family name="javaee" master="TPS"><Counter name="TPS" disp="TPS" unit="cnt" total="true"/><Counter name="Cpu" disp="CPU" unit="%"/></Family>
<Family name="host"><Counter name="Mem" disp="Memory"/></Family></Familys>
<Types><ObjectType name="tomcat" disp="Tomcat" family="javaee" icon="tc"><Counter name="Extra" disp="X" unit="u"/></ObjectType>
<ObjectType name="orphan" family="nofamily" sub-object="TRUE"/></Types></Counters>`
	if err := e.parse([]byte(xml)); err != nil {
		t.Fatal(err)
	}
	if e.familyName("tomcat") != "javaee" || e.familyName("nope") != "UNKNOWN" || e.familyName("orphan") != "UNKNOWN" {
		t.Fatal("familyName")
	}
	if e.displayName("tomcat", "Cpu") != "CPU" || e.displayName("tomcat", "Extra") != "X" || e.displayName("tomcat", "zz") != "" || e.displayName("nope", "Cpu") != "" {
		t.Fatal("displayName")
	}
	if e.unit("nope", "Cpu") != nil || e.unit("tomcat", "zz") != "" || e.unit("tomcat", "TPS") != "cnt" {
		t.Fatal("unit")
	}
	got, err := appendJSON(nil, buildCounterModel(e))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"families":[{"name":"javaee","counters":[{"name":"TPS","displayName":"TPS","unit":"cnt","totalizable":true,"icon":null,"master":true},` +
		`{"name":"Cpu","displayName":"CPU","unit":"%","totalizable":false,"icon":null,"master":false}]},{"name":"host","counters":[{"name":"Mem","displayName":"Memory","unit":null,"totalizable":false,"icon":null,"master":false}]}],` +
		`"objTypes":[{"name":"tomcat","familyName":"javaee","displayName":"Tomcat","icon":"tc","subObject":false},{"name":"orphan","familyName":null,"displayName":null,"icon":null,"subObject":true}]}`
	if string(got) != want {
		t.Fatalf("\n got %s\nwant %s", got, want)
	}
}

func TestDateUnits(t *testing.T) {
	loc := time.FixedZone("KST", 9*3600)
	ms := time.Date(2026, 10, 15, 0, 0, 1, 0, loc).UnixMilli()
	u := dateUnit(ms, loc)
	if dateUnitMillis(u, loc) != time.Date(2026, 10, 15, 0, 0, 0, 0, loc).UnixMilli() {
		t.Fatal("date unit boundary")
	}
}

func TestReplaceConfigKey(t *testing.T) {
	text := "a=1\nkey.x = old\nb=2"
	if got := replaceConfigKey(text, "key.x", "$1\\n"); got != "a=1\nkey.x=$1\\n\nb=2" {
		t.Fatalf("%q", got)
	}
	if got := replaceConfigKey("a=1", "k", "v"); got != "a=1\n\n#auto-added\nk=v\n" {
		t.Fatalf("%q", got)
	}
	// The key is literal: "key.x" must not match "keyAx".
	if got := replaceConfigKey("keyAx=1", "key.x", "2"); got != "keyAx=1\n\n#auto-added\nkey.x=2\n" {
		t.Fatalf("%q", got)
	}
	if removeVariableString("a$b$c") != "ac" {
		t.Fatal("removeVariableString")
	}
}
