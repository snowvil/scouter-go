package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func fixture(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata/java", name+".bin"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestJavaPrimitiveOracle(t *testing.T) {
	d := NewDecoder(fixture(t, "primitives"), Limits{})
	if !d.Bool() || d.Bool() || d.Byte() != 249 || d.Int16() != math.MinInt16 || d.Int32() != -1234567 || d.Int64() != math.MinInt64+7 {
		t.Fatal("big-endian signed primitive mismatch")
	}
	if math.Float32bits(d.Float32()) != 0x80000000 || !math.IsNaN(float64(d.Float32())) || !math.IsInf(d.Float64(), 1) {
		t.Fatal("IEEE754 mismatch")
	}
	if d.Int3() != -8388608 || d.Long5() != -549755813888 || d.Err() != nil || d.Remaining() != 0 {
		t.Fatalf("signed-width mismatch: %v", d.Err())
	}
	e := NewEncoder(Limits{})
	e.Bool(true)
	e.Bool(false)
	e.Byte(249)
	e.Int16(math.MinInt16)
	e.Int32(-1234567)
	e.Int64(math.MinInt64 + 7)
	e.Float32(math.Float32frombits(0x80000000))
	e.Float32(float32(math.NaN()))
	e.Float64(math.Inf(1))
	e.Int3(-8388608)
	e.Long5(-549755813888)
	got, err := e.Bytes()
	if err != nil || !bytes.Equal(got, fixture(t, "primitives")) {
		t.Fatalf("Java primitive encoding mismatch: %x %v", got, err)
	}
}

var decimalCases = []int64{math.MinInt64, -549755813889, -549755813888, -2147483649, -2147483648, -8388609, -8388608, -32769, -32768, -129, -128, -1, 0, 1, 127, 128, 32767, 32768, 8388607, 8388608, 2147483647, 2147483648, 549755813887, 549755813888, math.MaxInt64}

func TestJavaDecimalOracle(t *testing.T) {
	for i, want := range decimalCases {
		t.Run(fmt.Sprint(want), func(t *testing.T) {
			raw := fixture(t, fmt.Sprintf("decimal_%d", i))
			d := NewDecoder(raw, Limits{})
			if got := d.Decimal(); got != want || d.Err() != nil || d.Remaining() != 0 {
				t.Fatalf("got %d want %d: %v", got, want, d.Err())
			}
			e := NewEncoder(Limits{})
			e.Decimal(want)
			got, err := e.Bytes()
			if err != nil || !bytes.Equal(got, raw) {
				t.Fatalf("Java bytes mismatch: %x != %x: %v", got, raw, err)
			}
		})
	}
}

func TestJavaBlobAndUTF8Oracle(t *testing.T) {
	for _, size := range []int{0, 1, 253, 254, 65535, 65536} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			raw := fixture(t, fmt.Sprintf("blob_%d", size))
			d := NewDecoder(raw, Limits{})
			got := d.Blob()
			if len(got) != size || d.Err() != nil {
				t.Fatalf("blob length: %d %v", len(got), d.Err())
			}
			for i, v := range got {
				if v != byte(i*31) {
					t.Fatalf("payload at %d", i)
				}
			}
			e := NewEncoder(Limits{})
			e.Blob(got)
			encoded, err := e.Bytes()
			if err != nil || !bytes.Equal(encoded, raw) {
				t.Fatal("Java blob encoding mismatch", err)
			}
			if size > 0 {
				raw[len(raw)-1] ^= 255
				if got[len(got)-1] != byte((size-1)*31) {
					t.Fatal("decoded Blob aliases input")
				}
			}
		})
	}
	raw := fixture(t, "text")
	d := NewDecoder(raw, Limits{})
	if d.Text() != "" || d.Text() != "" || d.Text() != "합성 fixture 😀" || d.Err() != nil {
		t.Fatal("UTF8/null/empty mismatch", d.Err())
	}
	e := NewEncoder(Limits{})
	e.Text("")
	e.Text("")
	e.Text("합성 fixture 😀")
	got, err := e.Bytes()
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatal("text encoding mismatch", err)
	}
}

func TestJavaValueOracle(t *testing.T) {
	raw := fixture(t, "values")
	v, err := DecodeValue(raw, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if v.Type != ValueMap || len(v.Entries) != 17 {
		t.Fatalf("map shape: %+v", v)
	}
	wants := []string{"null", "bool", "decimal", "float", "double", "double_summary", "long_summary", "text", "hash", "blob", "ip", "list", "ints", "floats", "texts", "longs", "map"}
	// MapValue Java Hashtable wire order is not insertion order. Compare semantics by key.
	actual := make(map[string]Value)
	for _, entry := range v.Entries {
		actual[entry.Key] = entry.Value
	}
	original := v
	v.Entries = make([]Entry, len(wants))
	for i, key := range wants {
		value, ok := actual[key]
		if !ok {
			t.Fatalf("missing key %s", key)
		}
		v.Entries[i] = Entry{Key: key, Value: value}
	}
	if !v.Entries[1].Value.Bool || v.Entries[2].Value.Int != -549755813888 || math.Float32bits(v.Entries[3].Value.Float) != 0x80000000 || !math.IsInf(v.Entries[4].Value.Double, 1) {
		t.Fatal("numeric Value decode")
	}
	ds := v.Entries[5].Value
	ls := v.Entries[6].Value
	if ds.Double != 7.5 || ds.Count != 3 || ds.MinDouble != -1.25 || ds.MaxDouble != 8.75 || ls.Int != math.MinInt64 || ls.Count != 2 || ls.MinInt != -7 || ls.MaxInt != 9 {
		t.Fatal("Summary Value decode")
	}
	if v.Entries[7].Value.Text != "합성 fixture 😀" || v.Entries[8].Value.Int != -1234567 || !bytes.Equal(v.Entries[9].Value.Bytes, []byte{0, 1, 255}) || !bytes.Equal(v.Entries[10].Value.Bytes, []byte{192, 0, 2, 7}) {
		t.Fatal("text/hash/blob/ip decode")
	}
	if len(v.Entries[11].Value.Values) != 2 || v.Entries[11].Value.Values[0].Int != 128 || v.Entries[11].Value.Values[1].Text != "nested" {
		t.Fatal("nested list decode")
	}
	if !reflect.DeepEqual(v.Entries[12].Value.Ints, []int32{math.MinInt32, 0, math.MaxInt32}) || !reflect.DeepEqual(v.Entries[14].Value.Texts, []string{"", "한글", ""}) || !reflect.DeepEqual(v.Entries[15].Value.Longs, []int64{math.MinInt64, -1, math.MaxInt64}) {
		t.Fatal("array decode")
	}
	fs := v.Entries[13].Value.Floats
	if len(fs) != 3 || fs[0] != -1.25 || !math.IsNaN(float64(fs[1])) || !math.IsInf(float64(fs[2]), 1) {
		t.Fatal("float array decode")
	}
	nested := v.Entries[16].Value.Entries
	if len(nested) != 2 {
		t.Fatal("nested map count")
	}
	for _, entry := range nested {
		if (entry.Key == "z" && entry.Value.Int != 1) || (entry.Key == "a" && entry.Value.Int != 2) {
			t.Fatal("nested map decode")
		}
	}
	encoded, err := EncodeValue(original, Limits{})
	if err != nil || !bytes.Equal(encoded, raw) {
		t.Fatal("Java Value byte mismatch", err)
	}
}

func TestJavaPackOracle(t *testing.T) {
	names := []string{"map", "object", "counter", "textpack", "xlog", "profile", "profile2"}
	for version := 0; version <= 12; version++ {
		names = append(names, fmt.Sprintf("xlog_v%d", version))
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			raw := fixture(t, name)
			p, err := DecodePack(raw, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			switch v := p.(type) {
			case *MapPack:
				if len(v.Entries) != 2 || v.Entries[0].Key != "z" || v.Entries[1].Key != "a" || v.Entries[1].Value.Int != -7 {
					t.Fatal("MapPack decode")
				}
			case *ObjectPack:
				if v.ObjType != "javaee" || v.ObjHash != -1234567 || v.ObjName != "/synthetic/app" || v.Address != "192.0.2.7" || v.Version != "synthetic-2.21.3" || !v.Alive || v.Wakeup != 1700000000123 || len(v.Tags.Entries) != 17 {
					t.Fatal("ObjectPack decode")
				}
			case *PerfCounterPack:
				if v.Time != 1700000000123 || v.ObjName != "/synthetic/app" || v.TimeType != 1 || len(v.Data.Entries) != 17 {
					t.Fatal("PerfCounterPack decode")
				}
			case *TextPack:
				if v.XType != "service" || v.Hash != -1234567 || v.Text != "synthetic service 한글" {
					t.Fatal("TextPack decode")
				}
			case *XLogPack:
				assertXLog(t, v)
				if name != "xlog" {
					var version int
					fmt.Sscanf(name, "xlog_v%d", &version)
					if int(v.TailVersion) != version {
						t.Fatalf("tail version %d != %d", v.TailVersion, version)
					}
				}
			case *ProfilePack:
				if v.Time != 1700000000123 || v.ObjHash != -1234567 || v.Service != 7654321 || v.TxID != math.MinInt64+7 || !bytes.Equal(v.Profile, []byte{0, 1, 255, 2, 3}) {
					t.Fatal("Profile decode")
				}
				if name == "profile2" && (!v.Version2 || v.GxID != math.MaxInt64-9 || v.XType != 2 || v.DiscardType != 1 || !v.IgnoreGlobalConsequentSampling) {
					t.Fatal("Profile V2 decode")
				}
			default:
				t.Fatalf("unexpected Pack %T", p)
			}
			encoded, err := EncodePack(p, Limits{})
			if err != nil || !bytes.Equal(encoded, raw) {
				t.Fatalf("Java Pack byte mismatch: %v", err)
			}
		})
	}
}

func assertXLog(t *testing.T, p *XLogPack) {
	t.Helper()
	if p.EndTime != 1700000000123 || p.ObjHash != -1234567 || p.Service != 7654321 || p.TxID != math.MinInt64+7 || p.Caller != 0x1122334455667788 || p.GxID != math.MaxInt64-9 || p.Elapsed != 128 || p.Error != -32769 || p.CPU != 127 || p.SQLCount != 3 || p.SQLTime != 42 || !bytes.Equal(p.IPAddress, []byte{192, 0, 2, 7}) || p.KBytes != 32768 || p.Status != 200 || p.UserID != -549755813889 || p.UserAgent != 17 || p.Referer != 18 || p.Group != 19 || p.APICallCount != 2 || p.APICallTime != 6 {
		t.Fatal("XLog required fields")
	}
	if p.TailVersion >= 1 && (p.CountryCode != "ZZ" || p.City != 20) {
		t.Fatal("XLog country")
	}
	if p.TailVersion >= 2 && p.XType != 2 {
		t.Fatal("XLog type")
	}
	if p.TailVersion >= 3 && (p.Login != 21 || p.Desc != 22) {
		t.Fatal("XLog login")
	}
	if p.TailVersion >= 4 && (p.WebHash != 23 || p.WebTime != 24) {
		t.Fatal("XLog web")
	}
	if p.TailVersion >= 5 && p.HasDump != 1 {
		t.Fatal("XLog dump")
	}
	if p.TailVersion >= 6 && p.ThreadNameHash != -25 {
		t.Fatal("XLog thread")
	}
	if p.TailVersion >= 7 && (p.Text1 != "합성" || p.Text2 != "fixture") {
		t.Fatal("XLog texts 1/2")
	}
	if p.TailVersion >= 8 && (p.QueuingHostHash != 26 || p.QueuingTime != 27 || p.Queuing2ndHostHash != 28 || p.Queuing2ndTime != 29) {
		t.Fatal("XLog queue")
	}
	if p.TailVersion >= 9 && (p.Text3 != "third" || p.Text4 != "fourth" || p.Text5 != "fifth") {
		t.Fatal("XLog texts 3/4/5")
	}
	if p.TailVersion >= 10 && p.ProfileCount != 30 {
		t.Fatal("XLog profile count")
	}
	if p.TailVersion >= 11 && !p.B3Mode {
		t.Fatal("XLog B3")
	}
	if p.TailVersion >= 12 && (p.ProfileSize != 31 || p.DiscardType != 2 || !p.IgnoreGlobalConsequentSampling) {
		t.Fatal("XLog current tail")
	}
}

func TestJavaManifestBaseline(t *testing.T) {
	raw, err := os.ReadFile("testdata/java/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		SourceCommit string `json:"source_commit"`
		Synthetic    bool   `json:"synthetic_only"`
		Files        []struct {
			Name   string `json:"name"`
			Bytes  int    `json:"bytes"`
			SHA256 string `json:"sha256"`
		}
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.SourceCommit != "dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5" || !m.Synthetic || len(m.Files) < 50 {
		t.Fatal("unpinned/incomplete oracle manifest")
	}
	listed := make(map[string]bool, len(m.Files))
	for _, entry := range m.Files {
		if filepath.Base(entry.Name) != entry.Name || listed[entry.Name] {
			t.Fatal("invalid/duplicate fixture name", entry.Name)
		}
		listed[entry.Name] = true
		data, err := os.ReadFile(filepath.Join("testdata/java", entry.Name))
		if err != nil {
			t.Fatal(err)
		}
		if len(data) != entry.Bytes || fmt.Sprintf("%x", sha256.Sum256(data)) != entry.SHA256 {
			t.Fatal("fixture checksum/length mismatch", entry.Name)
		}
	}
	files, err := filepath.Glob("testdata/java/*.bin")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		if !listed[filepath.Base(name)] {
			t.Fatal("unlisted Java fixture", name)
		}
	}
}

func TestMalformedBounds(t *testing.T) {
	cases := []struct {
		name   string
		raw    []byte
		decode func(*Decoder)
	}{
		{"negative blob", []byte{254, 255, 255, 255, 255}, func(d *Decoder) { d.Blob() }},
		{"oversize blob", []byte{254, 127, 255, 255, 255}, func(d *Decoder) { d.Blob() }},
		{"truncated blob", []byte{255, 0, 254, 1}, func(d *Decoder) { d.Blob() }},
		{"negative map", []byte{ValueMap, 1, 255}, func(d *Decoder) { d.Value() }},
		{"overflow map", append([]byte{ValueMap, 8}, bytes.Repeat([]byte{127}, 8)...), func(d *Decoder) { d.Value() }},
		{"negative array", []byte{ValueIntArray, 255, 255}, func(d *Decoder) { d.Value() }},
		{"unsupported value", []byte{99}, func(d *Decoder) { d.Value() }},
		{"invalid decimal width", []byte{6, 0, 0, 0, 0, 0, 0, 0, 0}, func(d *Decoder) { d.Decimal() }},
		{"truncated fixed", []byte{0, 1}, func(d *Decoder) { d.Int64() }},
		{"invalid UTF8", []byte{1, 255}, func(d *Decoder) { d.Text() }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := NewDecoder(c.raw, Limits{})
			c.decode(d)
			if d.Err() == nil {
				t.Fatal("malformed input accepted")
			}
			old := d.Err()
			d.Text()
			if !errors.Is(d.Err(), old) {
				t.Fatal("sticky error changed")
			}
		})
	}
	if _, err := DecodePack([]byte{99}, Limits{}); !errors.Is(err, ErrUnsupported) {
		t.Fatal("unsupported Pack not explicit", err)
	}
	if _, err := DecodePack([]byte{10, 0, 1}, Limits{}); !errors.Is(err, ErrTrailing) {
		t.Fatal("trailing data accepted", err)
	}
	if _, err := DecodeValue([]byte{ValueNull}, Limits{MaxBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeValue([]byte{ValueNull, 0}, Limits{MaxBytes: 1}); !errors.Is(err, ErrLimit) {
		t.Fatal("frame bound not enforced", err)
	}
	d := NewDecoder([]byte{4, 1, 2, 3, 4}, Limits{MaxBlobBytes: 3})
	d.Blob()
	if !errors.Is(d.Err(), ErrLimit) {
		t.Fatal("blob bound not enforced", d.Err())
	}
	d = NewDecoder([]byte{ValueList, 1, 2, 0, 0}, Limits{MaxCollectionEntries: 1})
	d.Value()
	if !errors.Is(d.Err(), ErrLimit) {
		t.Fatal("count bound not enforced", d.Err())
	}
	raw := []byte{ValueList, 1, 1, ValueList, 1, 1, ValueNull}
	if _, err := DecodeValue(raw, Limits{MaxDepth: 2}); !errors.Is(err, ErrLimit) {
		t.Fatal("depth not enforced", err)
	}
	e := NewEncoder(Limits{MaxBytes: 2})
	e.Int64(7)
	if _, err := e.Bytes(); !errors.Is(err, ErrLimit) {
		t.Fatal("encode size bound not enforced", err)
	}
	e = NewEncoder(Limits{})
	e.Int3(8388608)
	if _, err := e.Bytes(); !errors.Is(err, ErrInvalid) {
		t.Fatal("int3 overflow accepted", err)
	}
	e = NewEncoder(Limits{})
	e.Long5(549755813888)
	if _, err := e.Bytes(); !errors.Is(err, ErrInvalid) {
		t.Fatal("long5 overflow accepted", err)
	}
	if _, err := EncodeValue(Value{Type: ValueIP4, Bytes: []byte{1}}, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("short IPv4 accepted", err)
	}
	if _, err := EncodeValue(Value{Type: 99}, Limits{}); !errors.Is(err, ErrUnsupported) {
		t.Fatal("unsupported encoding accepted", err)
	}
	if _, err := EncodePack(nil, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil Pack accepted", err)
	}
}

func TestTruncatedJavaFixtures(t *testing.T) {
	for _, name := range []string{"object", "counter", "textpack", "profile", "profile2"} {
		raw := fixture(t, name)
		for n := 0; n < len(raw); n++ {
			if _, err := DecodePack(raw[:n], Limits{}); err == nil {
				t.Fatalf("%s prefix %d accepted", name, n)
			}
		}
	}
	// XLog outer Blob must declare its full body even when legacy groups are optional.
	raw := fixture(t, "xlog")
	for n := 0; n < len(raw); n++ {
		if _, err := DecodePack(raw[:n], Limits{}); err == nil {
			t.Fatalf("XLog truncated outer Blob %d accepted", n)
		}
	}
	// Partial append groups inside a correctly sized Blob are still malformed.
	raw = fixture(t, "xlog_v1")
	body := raw[2:]
	for n := 1; n < 3; n++ {
		cut := append([]byte{PackXLog, byte(len(body) - n)}, body[:len(body)-n]...)
		if _, err := DecodePack(cut, Limits{}); err == nil {
			t.Fatal("partial XLog append group accepted")
		}
	}
}

func TestDecimalInt32Overflow(t *testing.T) {
	// ObjectPack objHash is Java int, but a hostile Decimal can carry int64.
	raw := append([]byte{PackObject, 0}, fixture(t, "decimal_24")...)
	if _, err := DecodePack(raw, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("int64 silently wrapped to int32", err)
	}
}

func TestMapDuplicateKeysUseJavaSemantics(t *testing.T) {
	raw := []byte{ValueMap, 1, 3, 1, 'z', ValueDecimal, 1, 1, 1, 'a', ValueDecimal, 1, 2, 1, 'z', ValueDecimal, 1, 3}
	v, err := DecodeValue(raw, Limits{})
	if err != nil || len(v.Entries) != 2 || v.Entries[0].Key != "z" || v.Entries[0].Value.Int != 3 || v.Entries[1].Key != "a" {
		t.Fatal("Java map key overwrite mismatch", v, err)
	}
}

func TestDecoderCollectionTotalBudget(t *testing.T) {
	raw := []byte{ValueList, 1, 2, ValueList, 1, 2, 0, 0, ValueList, 1, 2, 0, 0}
	if _, err := DecodeValue(raw, Limits{MaxTotalValues: 4}); !errors.Is(err, ErrLimit) {
		t.Fatal("aggregate allocation budget ignored", err)
	}
}

func TestXLogUnknownTailPreserved(t *testing.T) {
	raw := fixture(t, "xlog")
	d := NewDecoder(raw[1:], Limits{})
	body := d.Blob()
	body = append(body, 0xaa, 0xbb)
	// Fixture body is short; use independent standard binary header when crossing boundary.
	var extended []byte
	if len(body) <= 253 {
		extended = append([]byte{PackXLog, byte(len(body))}, body...)
	} else {
		extended = make([]byte, 4)
		extended[0] = PackXLog
		extended[1] = 255
		binary.BigEndian.PutUint16(extended[2:], uint16(len(body)))
		extended = append(extended, body...)
	}
	p, err := DecodePack(extended, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(p.(*XLogPack).Extra, []byte{0xaa, 0xbb}) {
		t.Fatal("unknown tail lost")
	}
	got, err := EncodePack(p, Limits{})
	if err != nil || !bytes.Equal(got, extended) {
		t.Fatal("unknown tail changed", err)
	}
}

func TestEncodeRejectsInvalidContracts(t *testing.T) {
	duplicates := []Entry{{Key: "same", Value: Value{Type: ValueNull}}, {Key: "same", Value: Value{Type: ValueNull}}}
	cases := []struct {
		name   string
		encode func() error
	}{
		{"duplicate map key", func() error { _, err := EncodeValue(Value{Type: ValueMap, Entries: duplicates}, Limits{}); return err }},
		{"TextHash overflow", func() error {
			_, err := EncodeValue(Value{Type: ValueTextHash, Int: math.MaxInt64}, Limits{})
			return err
		}},
		{"invalid UTF8", func() error { e := NewEncoder(Limits{}); e.Text("\xff"); _, err := e.Bytes(); return err }},
		{"nil typed Pack", func() error { var p *MapPack; _, err := EncodePack(p, Limits{}); return err }},
		{"Object non-map tags", func() error { _, err := EncodePack(&ObjectPack{}, Limits{}); return err }},
		{"counter non-map data", func() error { _, err := EncodePack(&PerfCounterPack{}, Limits{}); return err }},
		{"unknown XLog tail version", func() error { _, err := EncodePack(&XLogPack{TailVersion: 13}, Limits{}); return err }},
		{"Extra before current tail", func() error { _, err := EncodePack(&XLogPack{Extra: []byte{1}}, Limits{}); return err }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := c.encode(); !errors.Is(err, ErrInvalid) {
				t.Fatal("invalid input not rejected", err)
			}
		})
	}
	for _, limits := range []Limits{{MaxBytes: -1}, {MaxBlobBytes: -1}, {MaxCollectionEntries: -1}, {MaxTotalValues: -1}, {MaxDepth: -1}} {
		if _, err := DecodeValue([]byte{ValueNull}, limits); !errors.Is(err, ErrInvalid) {
			t.Fatal("negative decoder limit", err)
		}
		if _, err := EncodeValue(Value{Type: ValueNull}, limits); !errors.Is(err, ErrInvalid) {
			t.Fatal("negative encoder limit", err)
		}
	}
	if _, err := EncodeValue(Value{Type: ValueBlob, Bytes: []byte{1, 2}}, Limits{MaxBlobBytes: 1}); !errors.Is(err, ErrLimit) {
		t.Fatal("encode Blob bound", err)
	}
	nested := Value{Type: ValueList, Values: []Value{{Type: ValueList, Values: []Value{{Type: ValueNull}}}}}
	if _, err := EncodeValue(nested, Limits{MaxDepth: 2}); !errors.Is(err, ErrLimit) {
		t.Fatal("encode depth bound", err)
	}
	if _, err := EncodeValue(nested, Limits{MaxTotalValues: 2}); !errors.Is(err, ErrLimit) {
		t.Fatal("encode aggregate count bound", err)
	}
	if _, err := EncodeValue(Value{Type: ValueList, Values: []Value{{Type: ValueNull}, {Type: ValueNull}}}, Limits{MaxCollectionEntries: 1}); !errors.Is(err, ErrLimit) {
		t.Fatal("encode collection bound", err)
	}
}

func TestDecodedPackBytesSurviveBufferReuse(t *testing.T) {
	for _, name := range []string{"object", "counter", "xlog", "profile", "profile2"} {
		t.Run(name, func(t *testing.T) {
			raw := fixture(t, name)
			expected := bytes.Clone(raw)
			p, err := DecodePack(raw, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			for i := range raw {
				raw[i] = 0
			}
			got, err := EncodePack(p, Limits{})
			if err != nil || !bytes.Equal(got, expected) {
				t.Fatal("decoded Pack aliases leased input", err)
			}
		})
	}
}

func TestStreamingPackDecoderConsumesExactBodies(t *testing.T) {
	d := NewDecoder([]byte{PackMap, 0, PackText, 0, 0, 0, 0, 7, 0}, Limits{})
	first := d.Pack()
	second := d.Pack()
	if first.Type() != PackMap || second.(*TextPack).Hash != 7 || d.Remaining() != 0 || d.Offset() != 9 || d.Err() != nil {
		t.Fatal("stream boundary changed", first, second, d.Err())
	}
}

func FuzzDecodePack(f *testing.F) {
	for _, name := range []string{"map", "object", "counter", "textpack", "xlog", "xlog_v0", "profile", "profile2"} {
		f.Add(fixture(f, name))
	}
	f.Add([]byte{99})
	f.Add([]byte{PackMap, 8, 127, 255, 255, 255, 255, 255, 255, 255})
	f.Fuzz(func(t *testing.T, raw []byte) {
		limits := Limits{MaxBytes: 1 << 16, MaxBlobBytes: 1 << 15, MaxCollectionEntries: 128, MaxTotalValues: 512, MaxDepth: 8}
		p, err := DecodePack(raw, limits)
		if err != nil {
			return
		}
		encoded, err := EncodePack(p, limits)
		if err != nil {
			t.Fatal("decoded packet cannot encode", err)
		}
		again, err := DecodePack(encoded, limits)
		if err != nil {
			t.Fatal("encoded packet cannot decode", err)
		}
		// A decoded pack survives encode and decode unchanged (fields, nil
		// versus empty, NaN payloads compared as printed), so ingest may use
		// a pack the UDP receiver decoded without cloning it.
		if a, b := fmt.Sprintf("%#v", p), fmt.Sprintf("%#v", again); a != b {
			t.Fatalf("round trip changed the pack:\n%s\n%s", a, b)
		}
		// Decoding a handed-over input (the UDP path) gives the same pack.
		adopted, err := DecodePackAdopt(append([]byte(nil), raw...), limits)
		if err != nil {
			t.Fatal("adopted decode failed", err)
		}
		if a, b := fmt.Sprintf("%#v", p), fmt.Sprintf("%#v", adopted); a != b {
			t.Fatalf("adopted decode differs:\n%s\n%s", a, b)
		}
	})
}

func FuzzDecodeValue(f *testing.F) {
	f.Add(fixture(f, "values"))
	f.Add([]byte{ValueNull})
	f.Add([]byte{ValueList, 1, 255})
	f.Fuzz(func(t *testing.T, raw []byte) {
		limits := Limits{MaxBytes: 1 << 16, MaxBlobBytes: 1 << 15, MaxCollectionEntries: 128, MaxTotalValues: 512, MaxDepth: 8}
		v, err := DecodeValue(raw, limits)
		if err != nil {
			return
		}
		encoded, err := EncodeValue(v, limits)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeValue(encoded, limits); err != nil {
			t.Fatal(err)
		}
	})
}

func BenchmarkDecodeXLog(b *testing.B) {
	raw := fixture(b, "xlog")
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if _, err := DecodePack(raw, Limits{}); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkEncodeXLog(b *testing.B) {
	raw := fixture(b, "xlog")
	p, err := DecodePack(raw, Limits{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if _, err := EncodePack(p, Limits{}); err != nil {
			b.Fatal(err)
		}
	}
}

// With the input handed over, two packs decoded from one buffer alias it,
// but an append to one's Blob never writes into the other's bytes.
func TestAdoptInputClipsCapacity(t *testing.T) {
	var e Encoder
	e.init(Limits{}, 0)
	for _, b := range [][]byte{{1, 2, 3}, {9, 9}} {
		e.Pack(&ProfilePack{Time: 1, TxID: 2, Profile: b})
	}
	raw, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	d := NewDecoder(raw, Limits{})
	d.AdoptInput()
	first, second := d.Pack().(*ProfilePack), d.Pack().(*ProfilePack)
	if d.Err() != nil {
		t.Fatal(d.Err())
	}
	if cap(first.Profile) != len(first.Profile) {
		t.Fatal("capacity not clipped", cap(first.Profile), len(first.Profile))
	}
	_ = append(first.Profile, 7, 7, 7, 7, 7, 7, 7, 7)
	if !bytes.Equal(second.Profile, []byte{9, 9}) || !bytes.Equal(first.Profile, []byte{1, 2, 3}) {
		t.Fatal(first.Profile, second.Profile)
	}
}
