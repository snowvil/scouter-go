package protocol

import (
	"errors"
	"testing"
)

// DecimalValue must leave the Decoder exactly where Value leaves it, with the
// same error class and Value budget, for every type and every truncation.
func TestDecimalValueMatchesValue(t *testing.T) {
	values := []Value{
		{Type: ValueNull},
		{Type: ValueBoolean, Bool: true},
		{Type: ValueDecimal},
		{Type: ValueDecimal, Int: -1},
		{Type: ValueDecimal, Int: 300},
		{Type: ValueDecimal, Int: 1 << 20},
		{Type: ValueDecimal, Int: -(1 << 31)},
		{Type: ValueDecimal, Int: 1 << 36},
		{Type: ValueDecimal, Int: 1 << 60},
		{Type: ValueFloat, Float: 1.5},
		{Type: ValueDouble, Double: 2.5},
		{Type: ValueTextHash, Int: 77},
		{Type: ValueText, Text: "서비스"},
		{Type: ValueBlob, Bytes: []byte{1, 2, 3}},
		{Type: ValueIP4, Bytes: []byte{10, 0, 0, 1}},
		{Type: ValueDoubleSummary, Double: 1, Count: 2, MinDouble: 3, MaxDouble: 4},
		{Type: ValueLongSummary, Int: 1, Count: 2, MinInt: 3, MaxInt: 4},
		{Type: ValueList, Values: []Value{{Type: ValueDecimal, Int: 9}, {Type: ValueText, Text: "x"}}},
		{Type: ValueMap, Entries: []Entry{{Key: "a", Value: Value{Type: ValueDecimal, Int: 5}}}},
		{Type: ValueIntArray, Ints: []int32{1, 2}},
		{Type: ValueFloatArray, Floats: []float32{1, 2}},
		{Type: ValueLongArray, Longs: []int64{1, 2}},
		{Type: ValueTextArray, Texts: []string{"a", "b"}},
	}
	check := func(data []byte) {
		t.Helper()
		full := NewDecoder(data, Limits{})
		want := full.Value()
		fast := NewDecoder(data, Limits{})
		n, decimal := fast.DecimalValue()
		if (full.Err() == nil) != (fast.Err() == nil) || full.Offset() != fast.Offset() || full.values != fast.values || fast.depth != 0 {
			t.Fatalf("% x: Value offset %d values %d err %v, DecimalValue offset %d values %d depth %d err %v",
				data, full.Offset(), full.values, full.Err(), fast.Offset(), fast.values, fast.depth, fast.Err())
		}
		for _, kind := range []error{ErrTruncated, ErrInvalid, ErrLimit, ErrUnsupported} {
			if errors.Is(full.Err(), kind) != errors.Is(fast.Err(), kind) {
				t.Fatalf("% x: error class %v, want %v", data, fast.Err(), full.Err())
			}
		}
		if wantDecimal := full.Err() == nil && want.Type == ValueDecimal; decimal != wantDecimal || (decimal && n != want.Int) {
			t.Fatalf("% x: got %d %v, want %+v", data, n, decimal, want)
		}
	}
	for _, v := range values {
		data, err := EncodeValue(v, Limits{})
		if err != nil {
			t.Fatal(v, err)
		}
		for cut := 0; cut <= len(data); cut++ {
			check(data[:cut])
		}
		check(append(append([]byte{}, data...), 0xff)) // trailing bytes stay unread
	}
	check([]byte{0xee})             // unknown type
	check([]byte{ValueDecimal, 7})  // invalid Decimal width
	check([]byte{ValueDecimal, 6})  // invalid Decimal width
	check([]byte{ValueBoolean, 2})  // any non-zero byte is true
	check([]byte{ValueList, 1, 99}) // list count beyond the payload

	// The Value budget is spent as by Value.
	d := NewDecoder([]byte{ValueNull, ValueNull}, Limits{MaxTotalValues: 1})
	d.DecimalValue()
	if d.DecimalValue(); !errors.Is(d.Err(), ErrLimit) {
		t.Fatal("Value budget not applied", d.Err())
	}
}
