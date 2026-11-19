package ingest

import (
	"math"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// mapEntry is MapValue.get over a decoded Value. The decoder already keeps the
// last value of a repeated key (Hashtable.put); locally built maps are searched
// from the end so the same rule holds for them.
func mapEntry(m protocol.Value, key string) (protocol.Value, bool) {
	if m.Type != protocol.ValueMap {
		return protocol.Value{}, false
	}
	for i := len(m.Entries) - 1; i >= 0; i-- {
		if m.Entries[i].Key == key {
			return m.Entries[i].Value, true
		}
	}
	return protocol.Value{}, false
}

// javaGetBoolean is MapValue.getBoolean: only a BooleanValue counts.
func javaGetBoolean(m protocol.Value, key string) bool {
	v, ok := mapEntry(m, key)
	return ok && v.Type == protocol.ValueBoolean && v.Bool
}

// javaGetText is MapValue.getText: only a TextValue counts; ok=false is Java null.
func javaGetText(m protocol.Value, key string) (string, bool) {
	v, ok := mapEntry(m, key)
	if !ok || v.Type != protocol.ValueText {
		return "", false
	}
	return v.Text, true
}

// javaGetInt is MapValue.getInt: NumberValue.intValue() of Decimal/Float/Double
// ((int) long wraps, (int) double saturates and maps NaN to 0), otherwise 0.
func javaGetInt(m protocol.Value, key string) int32 {
	v, ok := mapEntry(m, key)
	if !ok {
		return 0
	}
	switch v.Type {
	case protocol.ValueDecimal:
		return int32(v.Int)
	case protocol.ValueFloat:
		return javaD2I(float64(v.Float))
	case protocol.ValueDouble:
		return javaD2I(v.Double)
	}
	return 0
}

// javaGetFloat is MapValue.getFloat: (float) Number.doubleValue(), otherwise 0.
func javaGetFloat(m protocol.Value, key string) float32 {
	v, ok := mapEntry(m, key)
	if !ok {
		return 0
	}
	switch v.Type {
	case protocol.ValueDecimal:
		return float32(float64(v.Int))
	case protocol.ValueFloat:
		return v.Float
	case protocol.ValueDouble:
		return float32(v.Double)
	}
	return 0
}

// javaD2I is the JLS 5.1.3 narrowing of a double to int.
func javaD2I(f float64) int32 {
	switch {
	case math.IsNaN(f):
		return 0
	case f >= math.MaxInt32:
		return math.MaxInt32
	case f <= math.MinInt32:
		return math.MinInt32
	}
	return int32(f)
}
