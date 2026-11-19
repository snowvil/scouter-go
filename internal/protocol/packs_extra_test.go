package protocol

import (
	"bytes"
	"errors"
	"io"
	"math"
	"reflect"
	"testing"
)

var extraPackFixtures = []string{"dropped", "span", "span-container", "span-container-empty", "status", "stack", "stack-empty", "summary", "batch", "batch-zero", "interaction", "alert"}

func extraFixture(t testing.TB, name string) []byte {
	t.Helper()
	return fixture(t, "pack-extra-"+name)
}

func TestExtraJavaPackOracle(t *testing.T) {
	for _, name := range extraPackFixtures {
		t.Run(name, func(t *testing.T) {
			raw := extraFixture(t, name)
			p, err := DecodePack(raw, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			assertExtraPack(t, name, p)
			got, err := EncodePack(p, Limits{})
			if err != nil || !bytes.Equal(got, raw) {
				t.Fatalf("Java Pack byte mismatch: %v", err)
			}
		})
	}
}

func assertExtraMap(t *testing.T, v Value) {
	t.Helper()
	if v.Type != ValueMap || len(v.Entries) != 3 {
		t.Fatalf("map shape: %+v", v)
	}
	entries := make(map[string]Value, len(v.Entries))
	for _, e := range v.Entries {
		entries[e.Key] = e.Value
	}
	if entries["hash"].Type != ValueDecimal || entries["hash"].Int != -1234567 || entries["text"].Type != ValueText || entries["text"].Text != "합성" || entries["blob"].Type != ValueBlob || !bytes.Equal(entries["blob"].Bytes, []byte{0, 1, 255}) {
		t.Fatal("MapValue semantics", v)
	}
}
func assertExtraPack(t *testing.T, name string, p Pack) {
	t.Helper()
	const tm int64 = 1700000000123
	switch v := p.(type) {
	case *DroppedXLogPack:
		if v.GxID != math.MaxInt64-9 || v.TxID != math.MinInt64+7 {
			t.Fatal("DroppedXLog fields")
		}
	case *SpanPack:
		if v.GxID != math.MaxInt64-9 || v.TxID != math.MinInt64+7 || v.Caller != 0x1122334455667788 || v.Timestamp != tm || v.Elapsed != 128 || v.SpanType != 2 || v.Name != -32769 || v.ObjHash != -1234567 || v.Error != 32768 || v.LocalEndpointServiceName != -8388608 || !bytes.Equal(v.LocalEndpointIP, []byte{192, 0, 2, 7}) || v.LocalEndpointPort != -1 || v.RemoteEndpointServiceName != 8388607 || !bytes.Equal(v.RemoteEndpointIP, []byte{0x20, 1, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}) || v.RemoteEndpointPort != 443 || !v.Debug || v.Shared {
			t.Fatal("Span scalar/endpoint fields")
		}
		if v.AnnotationTimestamps.Type != ValueList || !reflect.DeepEqual(v.AnnotationTimestamps.Values, []Value{{Type: ValueDecimal, Int: tm}, {Type: ValueDecimal, Int: tm + 1}}) || v.AnnotationValues.Type != ValueList || !reflect.DeepEqual(v.AnnotationValues.Values, []Value{{Type: ValueText, Text: "start"}, {Type: ValueText, Text: "끝"}}) {
			t.Fatal("Span annotations")
		}
		assertExtraMap(t, v.Tags)
	case *SpanContainerPack:
		if name == "span-container-empty" {
			if v.GxID != 1 || v.SpanCount != 0 || v.Timestamp != 0 || len(v.Spans) != 0 {
				t.Fatal("empty span container")
			}
			return
		}
		span := extraFixture(t, "span")
		if v.GxID != math.MaxInt64-9 || v.SpanCount != 2 || v.Timestamp != tm || !bytes.Equal(v.Spans, append(append([]byte{}, span...), span...)) {
			t.Fatal("SpanContainer fields")
		}
	case *StatusPack:
		if v.Time != tm || v.ObjType != "javaee" || v.ObjHash != -1234567 || v.Key != "synthetic-status" {
			t.Fatal("Status fields")
		}
		assertExtraMap(t, v.Data)
	case *StackPack:
		if name == "stack-empty" {
			if v.Time != 0 || v.ObjHash != 0 || len(v.Data) != 0 {
				t.Fatal("empty stack")
			}
			return
		}
		if v.Time != tm || v.ObjHash != -1234567 || !bytes.Equal(v.Data, []byte{0, 1, 255, 2}) {
			t.Fatal("Stack fields")
		}
	case *SummaryPack:
		if v.Time != tm || v.ObjHash != -1234567 || v.ObjType != "javaee" || v.SType != 3 {
			t.Fatal("Summary fields")
		}
		assertExtraMap(t, v.Table)
	case *InteractionPerfCounterPack:
		if v.Time != tm || v.ObjName != "/synthetic/app" || v.InteractionType != "synthetic-api" || v.FromHash != -1234567 || v.ToHash != 7654321 || v.Period != 3000 || v.Count != 4 || v.ErrorCount != 1 || v.TotalElapsed != 2147483648 {
			t.Fatal("Interaction fields")
		}
		assertExtraMap(t, v.CustomData)
	case *AlertPack:
		if v.Time != tm || v.Level != 2 || v.ObjType != "javaee" || v.ObjHash != -1234567 || v.Title != "synthetic alert" || v.Message != "합성 message" {
			t.Fatal("Alert fields")
		}
		assertExtraMap(t, v.Tags)
	case *BatchPack:
		count := int32(2)
		if name == "batch-zero" {
			count = 0
		}
		if v.StartTime != tm || v.ObjHash != -1234567 || v.BatchJobID != "합성-job" || v.Args != "synthetic" || v.PID != -123 || v.ElapsedTime != 2147483648 || v.ThreadCount != 3 || v.CPUTime != 4 || v.GCTime != 5 || v.GCCount != 6 || v.SQLTotalCount != count || v.SQLTotalTime != 7 || v.SQLTotalRows != 8 || v.SQLTotalRuns != 9 || !v.IsStack || v.ObjName != "/synthetic/batch" || v.ObjType != "batch" || v.Position != math.MaxInt64-7 || len(v.Extra) != 0 {
			t.Fatal("Batch fixed fields")
		}
		if count == 0 {
			if len(v.SQLStats) != 0 || len(v.UniqueSQLs) != 0 {
				t.Fatal("zero count allocated rows")
			}
			return
		}
		want := []BatchSQLStat{{HashValue: -1234567, Runs: 2, StartTime: tm, EndTime: tm + 10, TotalTime: 10, MinTime: 1, MaxTime: 9, ProcessedRows: 20, Rowed: true}, {HashValue: -1234566, Runs: 3, StartTime: tm + 1, EndTime: tm + 11, TotalTime: 11, MinTime: 2, MaxTime: 10, ProcessedRows: 21, Rowed: false}}
		if !reflect.DeepEqual(v.SQLStats, want) || len(v.UniqueSQLs) != 2 {
			t.Fatal("Batch SQL stat fields", v.SQLStats)
		}
		sql := make(map[int32]string)
		for _, entry := range v.UniqueSQLs {
			sql[entry.Hash] = entry.Text
		}
		if !reflect.DeepEqual(sql, map[int32]string{-1234567: "synthetic SQL 0", -1234566: "synthetic SQL 1"}) {
			t.Fatal("Batch SQL dictionary", sql)
		}
	default:
		t.Fatalf("unexpected Pack %T", p)
	}
}

func TestExtraPackTruncationAndStreamBoundary(t *testing.T) {
	for _, name := range extraPackFixtures {
		t.Run(name, func(t *testing.T) {
			raw := extraFixture(t, name)
			for n := 0; n < len(raw); n++ {
				if _, err := DecodePack(raw[:n], Limits{}); err == nil {
					t.Fatalf("prefix %d accepted", n)
				}
			}
			r := bytes.NewReader(append(append([]byte{}, raw...), 0xab))
			p, err := ReadPack(r, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			assertExtraPack(t, name, p)
			got, err := EncodePack(p, Limits{})
			if err != nil || !bytes.Equal(got, raw) {
				t.Fatal("stream byte mismatch", err)
			}
			sentinel, err := r.ReadByte()
			if err != nil || sentinel != 0xab || r.Len() != 0 {
				t.Fatal("ReadPack consumed next frame", err)
			}
		})
	}
}

func TestExtraPackTypedNilAndValueTypes(t *testing.T) {
	nils := []Pack{(*DroppedXLogPack)(nil), (*SpanPack)(nil), (*SpanContainerPack)(nil), (*StatusPack)(nil), (*StackPack)(nil), (*SummaryPack)(nil), (*BatchPack)(nil), (*InteractionPerfCounterPack)(nil), (*AlertPack)(nil)}
	for _, p := range nils {
		if _, err := EncodePack(p, Limits{}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("typed nil %T: %v", p, err)
		}
	}
	invalid := []Pack{&StatusPack{}, &SummaryPack{}, &InteractionPerfCounterPack{}, &AlertPack{}, &SpanPack{AnnotationTimestamps: Value{Type: ValueList}, AnnotationValues: Value{Type: ValueList}}, &SpanPack{AnnotationValues: Value{Type: ValueList}, Tags: Value{Type: ValueMap}}, &SpanPack{AnnotationTimestamps: Value{Type: ValueList}, Tags: Value{Type: ValueMap}}}
	for _, p := range invalid {
		if _, err := EncodePack(p, Limits{}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid field %T: %v", p, err)
		}
	}
	for _, raw := range [][]byte{{PackStatus, 0, 0, 0, 0, ValueNull}, {PackSummary, 0, 0, 0, 0, 0, 0, 0, ValueNull}} {
		if _, err := DecodePack(raw, Limits{}); !errors.Is(err, ErrInvalid) {
			t.Fatal("non-Map accepted", err)
		}
	}
}

func TestExtraPackOwnedResults(t *testing.T) {
	for _, name := range extraPackFixtures {
		t.Run(name, func(t *testing.T) {
			raw := extraFixture(t, name)
			want := append([]byte{}, raw...)
			p, err := DecodePack(raw, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			for i := range raw {
				raw[i] ^= 255
			}
			got, err := EncodePack(p, Limits{})
			if err != nil || !bytes.Equal(got, want) {
				t.Fatal("decoded Pack borrowed caller buffer", err)
			}
		})
	}
}

func TestBatchCountsAndTail(t *testing.T) {
	raw := extraFixture(t, "batch")
	p, err := DecodePack(raw, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	b := p.(*BatchPack)
	for _, limits := range []Limits{{MaxCollectionEntries: 1}, {MaxTotalValues: 3}} {
		if _, err := DecodePack(raw, limits); !errors.Is(err, ErrLimit) {
			t.Fatal("Batch decode allocation budget", err)
		}
		if _, err := EncodePack(b, limits); !errors.Is(err, ErrLimit) {
			t.Fatal("Batch encode allocation budget", err)
		}
	}
	b.SQLTotalCount = -1
	if _, err := EncodePack(b, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("negative count encoded", err)
	}
	b.SQLTotalCount = 3
	if _, err := EncodePack(b, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("count mismatch encoded", err)
	}
	b.SQLTotalCount = 2
	b.UniqueSQLs = b.UniqueSQLs[:1]
	if _, err := EncodePack(b, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("dictionary count mismatch encoded", err)
	}
	// Independently locate the count in the Java fixture, then replace its fixed int32.
	d := NewDecoder(raw[1:], Limits{})
	body := d.Blob()
	h := NewDecoder(body, Limits{})
	h.Int64()
	h.Int32()
	h.Text()
	h.Text()
	h.Int32()
	h.Int64()
	h.Int32()
	h.Int64()
	h.Int64()
	h.Int64()
	at := h.Offset()
	for _, count := range []int32{-1, math.MaxInt32} {
		changed := append([]byte{}, body...)
		changed[at] = byte(count >> 24)
		changed[at+1] = byte(count >> 16)
		changed[at+2] = byte(count >> 8)
		changed[at+3] = byte(count)
		wire := NewEncoder(Limits{})
		wire.Byte(PackBatch)
		wire.Blob(changed)
		input, _ := wire.Bytes()
		if _, err := DecodePack(input, Limits{}); err == nil {
			t.Fatal("hostile SQL count accepted")
		}
	}
	zero := extraFixture(t, "batch-zero")
	d = NewDecoder(zero[1:], Limits{})
	body = d.Blob()
	wire := NewEncoder(Limits{})
	wire.Byte(PackBatch)
	wire.Blob(append(body, 0xaa, 0xbb))
	input, _ := wire.Bytes()
	decoded, err := DecodePack(input, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.(*BatchPack).Extra, []byte{0xaa, 0xbb}) {
		t.Fatal("Batch unknown tail lost")
	}
	encoded, err := EncodePack(decoded, Limits{})
	if err != nil || !bytes.Equal(encoded, input) {
		t.Fatal("Batch tail byte mismatch", err)
	}
}

func FuzzExtraPack(f *testing.F) {
	for _, name := range extraPackFixtures {
		f.Add(extraFixture(f, name))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		limits := Limits{MaxBytes: 1 << 16, MaxBlobBytes: 1 << 15, MaxCollectionEntries: 128, MaxTotalValues: 512, MaxDepth: 8}
		p, err := DecodePack(raw, limits)
		if err != nil {
			return
		}
		got, err := EncodePack(p, limits)
		if err != nil {
			t.Fatal("successful decode cannot encode", err)
		}
		if _, err := DecodePack(got, limits); err != nil {
			t.Fatal("encoded Pack cannot decode", err)
		}
		// Streams must agree with buffered decode and leave a following byte intact.
		reader := bytes.NewReader(append(append([]byte{}, got...), 0xab))
		stream, err := ReadPack(reader, limits)
		if err != nil {
			t.Fatal(err)
		}
		streamBytes, err := EncodePack(stream, limits)
		if err != nil || !bytes.Equal(got, streamBytes) {
			t.Fatal("stream mismatch", err)
		}
		tail, _ := io.ReadAll(reader)
		if !bytes.Equal(tail, []byte{0xab}) {
			t.Fatal("stream boundary", tail)
		}
	})
}
