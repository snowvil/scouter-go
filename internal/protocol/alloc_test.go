package protocol

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// nestedBlobReference reproduces the former nested-Encoder framing so the
// in-place beginBlob/endBlob path can be checked byte for byte.
func nestedBlobReference(t *testing.T, kind byte, body func(*Encoder)) []byte {
	t.Helper()
	inner := NewEncoder(Limits{})
	body(inner)
	payload, err := inner.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	outer := NewEncoder(Limits{})
	outer.Byte(kind)
	outer.Blob(payload)
	raw, err := outer.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestEncoderBytesOwnership(t *testing.T) {
	e := NewEncoder(Limits{})
	e.Int32(1)
	first, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := bytes.Clone(first)
	e.Int32(2)
	second, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, snapshot) || cap(first) != len(first) {
		t.Fatal("Encoder wrote into a returned buffer", first, snapshot)
	}
	if !bytes.Equal(second, []byte{0, 0, 0, 1, 0, 0, 0, 2}) {
		t.Fatal("later Bytes lost earlier output", second)
	}
	// Writes after a failure stay failed and never hand out partial output.
	bad := NewEncoder(Limits{MaxBytes: 2})
	bad.Int32(1)
	if _, err := bad.Bytes(); !errors.Is(err, ErrLimit) {
		t.Fatal("limit not sticky", err)
	}
}

func TestInPlaceBlobFramingMatchesNestedEncoder(t *testing.T) {
	for _, size := range []int{0, 1, 200, 253, 254, 1000, 65535, 65536, 70000} {
		p := &XLogPack{TailVersion: XLogCurrentTail, EndTime: 1700000000000, ObjHash: -7, Service: 9, TxID: 1, IPAddress: []byte{10, 0, 0, 1}, Text5: strings.Repeat("x", size)}
		got, err := EncodePack(p, Limits{})
		if err != nil {
			t.Fatal(size, err)
		}
		// The field writers are unchanged; what endBlob adds is the prefix
		// widening, so compare against Blob() framing of the extracted body.
		d := NewDecoder(got, Limits{})
		kind, body := d.Byte(), d.Blob()
		if d.Err() != nil || d.Remaining() != 0 || len(body) < size {
			t.Fatal("XLog frame", size, d.Err(), d.Remaining(), len(body))
		}
		want := nestedBlobReference(t, kind, func(e *Encoder) { e.raw(body) })
		if !bytes.Equal(got, want) {
			t.Fatal("XLog in-place framing differs", size, len(got), len(want))
		}
		back, err := DecodePack(got, Limits{})
		if err != nil || back.(*XLogPack).Text5 != p.Text5 || back.(*XLogPack).TailVersion != XLogCurrentTail {
			t.Fatal("XLog round trip", size, err)
		}
	}
	batch := &BatchPack{StartTime: 5, BatchJobID: "job", Args: strings.Repeat("a", 300), SQLTotalCount: 1, SQLStats: []BatchSQLStat{{HashValue: 1, Runs: 2}}, UniqueSQLs: []BatchSQLText{{Hash: 1, Text: strings.Repeat("s", 70000)}}}
	got, err := EncodePack(batch, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if got[1] != 254 || !bytes.Equal(got[6:14], []byte{0, 0, 0, 0, 0, 0, 0, 5}) {
		t.Fatal("Batch Blob prefix", got[:14])
	}
	back, err := DecodePack(got, Limits{})
	if err != nil || !reflect.DeepEqual(back, batch) {
		t.Fatal("Batch round trip", err)
	}
	simple, err := EncodeBatchSimple(batch, Limits{})
	if err != nil || simple[0] != 255 || len(simple) != 3+int(uint16(simple[1])<<8|uint16(simple[2])) {
		t.Fatal("BatchSimple Blob prefix", err, simple[:3], len(simple))
	}
}

func TestInPlaceBlobFramingLimits(t *testing.T) {
	p := &XLogPack{TailVersion: XLogCurrentTail, Text5: strings.Repeat("x", 300)}
	if _, err := EncodePack(p, Limits{MaxBlobBytes: 200}); !errors.Is(err, ErrLimit) {
		t.Fatal("XLog body exceeding MaxBlobBytes", err)
	}
	if _, err := EncodePack(p, Limits{MaxBytes: 310}); !errors.Is(err, ErrLimit) {
		t.Fatal("XLog frame exceeding MaxBytes once the prefix widens", err)
	}
	if got, err := EncodePack(p, Limits{MaxBytes: 400}); err != nil || len(got) > 400 {
		t.Fatal("XLog within MaxBytes", err, len(got))
	}
	if _, err := EncodeSteps([]Step{&MethodStep{}}, Limits{MaxBytes: 4}); !errors.Is(err, ErrLimit) {
		t.Fatal("Step frame bound with size hint above MaxBytes", err)
	}
	if _, err := DecodeSteps([]byte{StepMethod, 0, 0, 0, 0, 0, 0, 0}, Limits{MaxCollectionEntries: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatal("negative Step limit", err)
	}
}

func mapWithDuplicates(n int) ([]byte, []Entry) {
	var entries []Entry
	for i := 0; i < n; i++ {
		entries = append(entries, Entry{Key: "k" + strings.Repeat("y", i%5) + string(rune('a'+i%26)), Value: Value{Type: ValueDecimal, Int: int64(i)}})
	}
	e := NewEncoder(Limits{})
	e.Byte(ValueMap)
	e.Decimal(int64(n + 2))
	for _, entry := range entries {
		e.Text(entry.Key)
		e.Value(entry.Value)
	}
	// Duplicate of the first and the last key, each overwriting in place.
	e.Text(entries[0].Key)
	e.Value(Value{Type: ValueText, Text: "first"})
	e.Text(entries[n-1].Key)
	e.Value(Value{Type: ValueText, Text: "last"})
	raw, _ := e.Bytes()
	entries[0].Value = Value{Type: ValueText, Text: "first"}
	entries[n-1].Value = Value{Type: ValueText, Text: "last"}
	return raw, entries
}

func TestMapDuplicateKeysAcrossDecodePaths(t *testing.T) {
	for _, n := range []int{1, 3, linearKeyScan, linearKeyScan + 1, 80} {
		raw, want := mapWithDuplicates(n)
		got, err := DecodeValue(raw, Limits{})
		if err != nil || !reflect.DeepEqual(got.Entries, want) {
			t.Fatal("in-memory duplicate handling", n, err)
		}
		streamed, err := ReadValue(bytes.NewReader(raw), Limits{})
		if err != nil || !reflect.DeepEqual(streamed.Entries, want) {
			t.Fatal("stream duplicate handling", n, err)
		}
		// Keys own their bytes: the input may be reused after decoding.
		for i := range raw {
			raw[i] = 0
		}
		if !reflect.DeepEqual(got.Entries, want) {
			t.Fatal("arena keys alias input", n)
		}
		encoded, err := EncodeValue(got, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		again, err := DecodeValue(encoded, Limits{})
		if err != nil || !reflect.DeepEqual(again.Entries, want) {
			t.Fatal("re-encoded map", n, err)
		}
		dupEncode := append([]Entry(nil), want...)
		dupEncode = append(dupEncode, Entry{Key: want[0].Key})
		if _, err := EncodeValue(Value{Type: ValueMap, Entries: dupEncode}, Limits{}); !errors.Is(err, ErrInvalid) {
			t.Fatal("encode duplicate detection", n, err)
		}
	}
}

func TestDecodeStepsMatchSingleDecodes(t *testing.T) {
	var steps []Step
	for i := int32(0); i < 70; i++ {
		single := SingleStep{Parent: i - 1, Index: i, StartTime: i, StartCPU: i}
		switch i % 7 {
		case 0:
			steps = append(steps, &MethodStep{SingleStep: single, Hash: i})
		case 1:
			steps = append(steps, &SQLStep3{SQLStep2: SQLStep2{SQLStep: SQLStep{SingleStep: single, Hash: i, Param: "p"}, XType: 2}, Updated: 3})
		case 2:
			steps = append(steps, &APICallStep2{APICallStep: APICallStep{CallStepFields: CallStepFields{SingleStep: single, TxID: int64(i)}, Opt: 1, Address: "h:1"}, Async: 1})
		case 3:
			steps = append(steps, &HashedMessageStep{SingleStep: single, Hash: i, Time: 1, Value: 2})
		case 4:
			steps = append(steps, &ParameterizedMessageStep{SingleStep: single, Hash: i, Elapsed: 1, Level: -1, ParamString: "a,b"})
		case 5:
			steps = append(steps, &MessageStep{SingleStep: single, Message: "m"})
		default:
			steps = append(steps, &MethodStep2{MethodStep: MethodStep{SingleStep: single, Hash: i}, Error: i})
		}
	}
	raw, err := EncodeSteps(steps, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeSteps(raw, Limits{})
	if err != nil || len(decoded) != len(steps) {
		t.Fatal("sequence decode", err, len(decoded))
	}
	for i, step := range decoded {
		if !reflect.DeepEqual(step, steps[i]) {
			t.Fatal("sequence-decoded Step differs", i, step, steps[i])
		}
		one, err := EncodeStep(steps[i], Limits{})
		if err != nil {
			t.Fatal(err)
		}
		alone, err := DecodeStep(one, Limits{})
		if err != nil || !reflect.DeepEqual(alone, step) {
			t.Fatal("single decode differs", i, err)
		}
	}
	// Records are independent values.
	decoded[6].(*MethodStep2).Hash = -1
	if decoded[13].(*MethodStep2).Hash != 13 || decoded[0].(*MethodStep).Hash != 0 {
		t.Fatal("Step records alias each other")
	}
	for i := range raw {
		raw[i] = 0
	}
	if !reflect.DeepEqual(decoded[1], steps[1]) || !reflect.DeepEqual(decoded[4], steps[4]) {
		t.Fatal("Step strings alias input")
	}
}

func TestHotPathAllocationBudget(t *testing.T) {
	xlog := fixture(t, "xlog")
	p, err := DecodePack(xlog, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	text := fixture(t, "textpack")
	counter := fixture(t, "counter")
	budget := []struct {
		name string
		max  float64
		run  func()
	}{
		{"EncodePack xlog", 1, func() { _, _ = EncodePack(p, Limits{}) }},
		{"DecodePack textpack", 3, func() { _, _ = DecodePack(text, Limits{}) }},
		{"DecodePack counter", 20, func() { _, _ = DecodePack(counter, Limits{}) }},
	}
	for _, b := range budget {
		if got := testing.AllocsPerRun(200, b.run); got > b.max {
			t.Fatalf("%s allocates %.0f per op, budget %.0f", b.name, got, b.max)
		}
	}
}
