// Scouter wire format: Copyright 2015 the original author or authors,
// https://github.com/scouter-project/scouter, Apache License 2.0.
// Independent Go implementation; see CONTRACT.md for pinned source provenance.
package protocol

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"
)

const (
	ValueNull          byte = 0
	ValueBoolean       byte = 10
	ValueDecimal       byte = 20
	ValueFloat         byte = 30
	ValueDouble        byte = 40
	ValueDoubleSummary byte = 45
	ValueLongSummary   byte = 46
	ValueText          byte = 50
	ValueTextHash      byte = 51
	ValueBlob          byte = 60
	ValueIP4           byte = 61
	ValueList          byte = 70
	ValueIntArray      byte = 71
	ValueFloatArray    byte = 72
	ValueTextArray     byte = 73
	ValueLongArray     byte = 74
	ValueMap           byte = 80
)

func validText(v []byte) bool   { return utf8.Valid(v) }
func validString(v string) bool { return utf8.ValidString(v) }

// Entry preserves wire order. MapPack emits Java insertion order; MapValue's
// Java Hashtable has no insertion-order contract. Decoded wire order is retained.
type Entry struct {
	Key   string
	Value Value
}

// Value is a tagged wire value. Only fields corresponding to Type are encoded.
// Int carries Decimal/TextHash/LongSummary.sum, Double carries DoubleSummary.sum.
// Bytes and recursively nested slices are owned by decoded results.
type Value struct {
	Type                 byte
	Bool                 bool
	Int                  int64
	Float                float32
	Double               float64
	Text                 string
	Bytes                []byte
	Entries              []Entry
	Values               []Value
	Ints                 []int32
	Floats               []float32
	Texts                []string
	Longs                []int64
	Count                int32
	MinInt, MaxInt       int64
	MinDouble, MaxDouble float64
}

// linearKeyScan bounds the O(n^2) duplicate-key scan used instead of a map
// allocation for the small maps that dominate counter/object/alert traffic.
const linearKeyScan = 32

func entryIndex(entries []Entry, key string) int {
	for i := range entries {
		if entries[i].Key == key {
			return i
		}
	}
	return -1
}

func (d *Decoder) entries() []Entry {
	n := d.count(d.Decimal(), 2)
	if d.err != nil {
		return nil
	}
	entries := make([]Entry, 0, n)
	if n <= linearKeyScan && d.reader == nil {
		return d.entriesArena(entries, n)
	}
	var positions map[string]int
	if n > linearKeyScan {
		positions = make(map[string]int, n)
	}
	for i := 0; i < n && d.err == nil; i++ {
		key := d.Text()
		v := d.Value()
		if positions == nil {
			if at := entryIndex(entries, key); at >= 0 {
				entries[at].Value = v
				continue
			}
		} else if at, ok := positions[key]; ok {
			entries[at].Value = v
			continue
		} else {
			positions[key] = len(entries)
		}
		entries = append(entries, Entry{key, v})
	}
	return entries
}

// entriesArena decodes a small in-memory map whose keys are materialized as
// substrings of one owned string instead of one allocation per key. The keys
// of one map therefore share a backing array that lives as long as any of them;
// its size is bounded by the map's own key bytes. Key views borrow the input
// only until this function returns, so it is not used in stream mode where the
// scratch buffer is reused between reads.
func (d *Decoder) entriesArena(entries []Entry, n int) []Entry {
	var keys [linearKeyScan][]byte
	total := 0
	for i := 0; i < n && d.err == nil; i++ {
		key := d.textView()
		v := d.Value()
		if d.err != nil {
			break
		}
		at := -1
		for j := range entries {
			if bytes.Equal(keys[j], key) {
				at = j
				break
			}
		}
		if at >= 0 {
			entries[at].Value = v
			continue
		}
		keys[len(entries)] = key
		total += len(key)
		entries = append(entries, Entry{Value: v})
	}
	if d.err != nil {
		return nil
	}
	var all strings.Builder
	all.Grow(total)
	for i := range entries {
		all.Write(keys[i])
	}
	owned, offset := all.String(), 0
	for i := range entries {
		end := offset + len(keys[i])
		entries[i].Key = owned[offset:end]
		offset = end
	}
	return entries
}
func (e *Encoder) entries(entries []Entry) {
	e.count(len(entries))
	if e.err != nil {
		return
	}
	e.Decimal(int64(len(entries)))
	var seen map[string]struct{}
	if len(entries) > linearKeyScan {
		seen = make(map[string]struct{}, len(entries))
	}
	for i, entry := range entries {
		duplicate := false
		if seen == nil {
			duplicate = entryIndex(entries[:i], entry.Key) >= 0
		} else if _, ok := seen[entry.Key]; ok {
			duplicate = true
		} else {
			seen[entry.Key] = struct{}{}
		}
		if duplicate {
			e.fail(ErrInvalid, "duplicate encoded map key")
			return
		}
		e.Text(entry.Key)
		e.Value(entry.Value)
		if e.err != nil {
			return
		}
	}
}

func (d *Decoder) Value() Value {
	if !d.enterValue() {
		return Value{}
	}
	v := d.value()
	d.depth--
	return v
}

// DecimalValue consumes one Value and returns its number when it is a
// Decimal. Scalar values are consumed without building a Value, for callers
// that walk many entries to reach one; any other type takes the full decode,
// so limits and errors are those of Value.
func (d *Decoder) DecimalValue() (int64, bool) {
	if !d.enterValue() {
		return 0, false
	}
	defer func() { d.depth-- }()
	if d.reader != nil {
		v := d.value()
		return v.Int, v.Type == ValueDecimal && d.err == nil
	}
	switch d.Byte() {
	case ValueDecimal:
		n := d.Decimal()
		return n, d.err == nil
	case ValueNull:
	case ValueBoolean:
		d.Byte()
	case ValueFloat, ValueTextHash:
		d.take(4)
	case ValueDouble:
		d.take(8)
	default:
		if d.err == nil {
			d.offset-- // the type byte: decode this Value in full
			d.value()
		}
	}
	return 0, false
}

func (d *Decoder) value() Value {
	v := Value{Type: d.Byte()}
	switch v.Type {
	case ValueNull:
	case ValueBoolean:
		v.Bool = d.Bool()
	case ValueDecimal:
		v.Int = d.Decimal()
	case ValueFloat:
		v.Float = d.Float32()
	case ValueDouble:
		v.Double = d.Float64()
	case ValueDoubleSummary:
		v.Double = d.Float64()
		v.Count = d.Int32()
		v.MinDouble = d.Float64()
		v.MaxDouble = d.Float64()
	case ValueLongSummary:
		v.Int = d.Int64()
		v.Count = d.Int32()
		v.MinInt = d.Int64()
		v.MaxInt = d.Int64()
	case ValueText:
		v.Text = d.Text()
	case ValueTextHash:
		v.Int = int64(d.Int32())
	case ValueBlob:
		v.Bytes = d.Blob()
	case ValueIP4:
		b := d.take(4)
		if d.err == nil {
			v.Bytes = d.owned(b)
		}
	case ValueMap:
		v.Entries = d.entries()
	case ValueList:
		n := d.count(d.Decimal(), 1)
		if d.err != nil {
			return Value{}
		}
		v.Values = make([]Value, 0, n)
		for i := 0; i < n && d.err == nil; i++ {
			v.Values = append(v.Values, d.Value())
		}
	case ValueIntArray, ValueFloatArray, ValueTextArray, ValueLongArray:
		width := 1
		if v.Type == ValueIntArray || v.Type == ValueFloatArray {
			width = 4
		} else if v.Type == ValueLongArray {
			width = 8
		}
		n := d.count(int64(d.Int16()), width)
		if d.err != nil {
			return Value{}
		}
		d.values += n
		switch v.Type {
		case ValueIntArray:
			v.Ints = make([]int32, n)
			for i := range v.Ints {
				v.Ints[i] = d.Int32()
			}
		case ValueFloatArray:
			v.Floats = make([]float32, n)
			for i := range v.Floats {
				v.Floats[i] = d.Float32()
			}
		case ValueTextArray:
			v.Texts = make([]string, n)
			for i := range v.Texts {
				v.Texts[i] = d.Text()
			}
		case ValueLongArray:
			v.Longs = make([]int64, n)
			for i := range v.Longs {
				v.Longs[i] = d.Int64()
			}
		}
	default:
		d.fail(ErrUnsupported, fmt.Sprintf("Value type %d", v.Type))
	}
	return v
}

func (e *Encoder) Value(v Value) {
	if !e.enterValue() {
		return
	}
	e.value(v)
	e.depth--
}
func (e *Encoder) value(v Value) {
	e.Byte(v.Type)
	switch v.Type {
	case ValueNull:
	case ValueBoolean:
		e.Bool(v.Bool)
	case ValueDecimal:
		e.Decimal(v.Int)
	case ValueFloat:
		e.Float32(v.Float)
	case ValueDouble:
		e.Float64(v.Double)
	case ValueDoubleSummary:
		e.Float64(v.Double)
		e.Int32(v.Count)
		e.Float64(v.MinDouble)
		e.Float64(v.MaxDouble)
	case ValueLongSummary:
		e.Int64(v.Int)
		e.Int32(v.Count)
		e.Int64(v.MinInt)
		e.Int64(v.MaxInt)
	case ValueText:
		e.Text(v.Text)
	case ValueTextHash:
		if v.Int < math.MinInt32 || v.Int > math.MaxInt32 {
			e.fail(ErrInvalid, "TextHash overflow")
			return
		}
		e.Int32(int32(v.Int))
	case ValueBlob:
		e.Blob(v.Bytes)
	case ValueIP4:
		if len(v.Bytes) != 4 {
			e.fail(ErrInvalid, "IPv4 length must be 4")
			return
		}
		e.raw(v.Bytes)
	case ValueMap:
		e.entries(v.Entries)
	case ValueList:
		e.count(len(v.Values))
		if e.err != nil {
			return
		}
		e.Decimal(int64(len(v.Values)))
		for _, item := range v.Values {
			e.Value(item)
			if e.err != nil {
				return
			}
		}
	case ValueIntArray, ValueFloatArray, ValueTextArray, ValueLongArray:
		var n int
		switch v.Type {
		case ValueIntArray:
			n = len(v.Ints)
		case ValueFloatArray:
			n = len(v.Floats)
		case ValueTextArray:
			n = len(v.Texts)
		case ValueLongArray:
			n = len(v.Longs)
		}
		e.count(n)
		if n > math.MaxInt16 {
			e.fail(ErrInvalid, "array count does not fit signed int16")
		}
		if e.err != nil {
			return
		}
		e.values += n
		e.Int16(int16(n))
		switch v.Type {
		case ValueIntArray:
			for _, item := range v.Ints {
				e.Int32(item)
			}
		case ValueFloatArray:
			for _, item := range v.Floats {
				e.Float32(item)
			}
		case ValueTextArray:
			for _, item := range v.Texts {
				e.Text(item)
			}
		case ValueLongArray:
			for _, item := range v.Longs {
				e.Int64(item)
			}
		}
	default:
		e.fail(ErrUnsupported, fmt.Sprintf("Value type %d", v.Type))
	}
}
