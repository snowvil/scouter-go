package ingest

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Java toString of the values that appear in the AgentManager alert
// messages ("<objName> is not running. " + ObjectPack.toString()).

// javaObjectString is ObjectPack.toString (scouter.common ObjectPack.java):
// "OBJECT " then " objType=..." (two spaces), Hexa32 objHash, the optional
// address/version, " alive" when the Java alive flag is set, the wakeup as
// java.sql.Timestamp in the server timezone and the tags MapValue.
func javaObjectString(p *protocol.ObjectPack, alive bool, loc *time.Location) string {
	var b strings.Builder
	b.WriteString("OBJECT ")
	b.WriteString(" objType=" + p.ObjType)
	b.WriteString(" objHash=" + javaHexa32(int64(p.ObjHash)))
	b.WriteString(" objName=" + p.ObjName)
	if p.Address != "" {
		b.WriteString(" addr=" + p.Address)
	}
	if p.Version != "" {
		b.WriteString(" " + p.Version)
	}
	if alive {
		b.WriteString(" alive")
	}
	if p.Wakeup > 0 {
		b.WriteString(" " + javaTimestamp(p.Wakeup, loc))
	}
	if p.Tags.Type == protocol.ValueMap && len(p.Tags.Entries) > 0 {
		b.WriteString(" " + javaValueString(p.Tags))
	}
	return b.String()
}

// javaHexa32 is scouter.util.Hexa32.toString32.
func javaHexa32(n int64) string {
	switch {
	case n == math.MinInt64:
		return "z8000000000000"
	case n < 0:
		return "z" + strconv.FormatInt(-n, 32)
	case n < 10:
		return strconv.FormatInt(n, 10)
	}
	return "x" + strconv.FormatInt(n, 32)
}

// javaTimestamp is new java.sql.Timestamp(ms).toString():
// "yyyy-mm-dd hh:mm:ss.f" with the nanos stripped of trailing zeros ("0"
// when zero).
func javaTimestamp(ms int64, loc *time.Location) string {
	t := time.UnixMilli(ms).In(loc)
	nanos := strconv.Itoa(t.Nanosecond())
	if t.Nanosecond() == 0 {
		nanos = "0"
	} else {
		nanos = strings.TrimRight(fmt.Sprintf("%09d", t.Nanosecond()), "0")
	}
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%s", t.Year(), int(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second(), nanos)
}

// javaValueString is Value.toString for the scouter.lang.value types.
// MapValue iterates its Hashtable, so the entries are emitted in Java
// Hashtable order rebuilt from the wire (insertion) order.
func javaValueString(v protocol.Value) string {
	switch v.Type {
	case protocol.ValueNull:
		return "null"
	case protocol.ValueBoolean:
		return strconv.FormatBool(v.Bool)
	case protocol.ValueDecimal:
		return strconv.FormatInt(v.Int, 10)
	case protocol.ValueFloat:
		return javaFloatString(float64(v.Float), 32)
	case protocol.ValueDouble:
		return javaDecimalFormat(v.Double)
	case protocol.ValueText:
		return v.Text
	case protocol.ValueTextHash:
		return strconv.FormatInt(int64(int32(v.Int)), 16)
	case protocol.ValueBlob:
		return "byte[" + strconv.Itoa(len(v.Bytes)) + "]"
	case protocol.ValueIP4:
		if len(v.Bytes) == 4 {
			return fmt.Sprintf("%d.%d.%d.%d", v.Bytes[0], v.Bytes[1], v.Bytes[2], v.Bytes[3])
		}
		return "0.0.0.0"
	case protocol.ValueList:
		parts := make([]string, len(v.Values))
		for i, x := range v.Values {
			parts[i] = javaValueString(x)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case protocol.ValueIntArray:
		parts := make([]string, len(v.Ints))
		for i, x := range v.Ints {
			parts[i] = strconv.Itoa(int(x))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case protocol.ValueLongArray:
		parts := make([]string, len(v.Longs))
		for i, x := range v.Longs {
			parts[i] = strconv.FormatInt(x, 10)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case protocol.ValueFloatArray:
		parts := make([]string, len(v.Floats))
		for i, x := range v.Floats {
			parts[i] = javaFloatString(float64(x), 32)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case protocol.ValueTextArray:
		return "[" + strings.Join(v.Texts, ", ") + "]"
	case protocol.ValueLongSummary:
		return fmt.Sprintf("[sum=%d,count=%d,min=%d,max=%d]", v.Int, v.Count, v.MinInt, v.MaxInt)
	case protocol.ValueDoubleSummary:
		return fmt.Sprintf("[sum=%s,count=%d,min=%s,max=%s]", javaDecimalFormat(v.Double), v.Count, javaDecimalFormat(v.MinDouble), javaDecimalFormat(v.MaxDouble))
	case protocol.ValueMap:
		byKey := make(map[string]protocol.Value, len(v.Entries))
		keys := make([]string, 0, len(v.Entries))
		for _, e := range v.Entries {
			if _, ok := byKey[e.Key]; !ok {
				keys = append(keys, e.Key)
			}
			byKey[e.Key] = e.Value
		}
		var b strings.Builder
		b.WriteByte('{')
		for i, k := range javaHashtableOrder(keys) {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(k + "=" + javaValueString(byKey[k]))
		}
		b.WriteByte('}')
		return b.String()
	}
	return ""
}

// javaFloatString is Float.toString/Double.toString: the shortest digits
// that identify the value, plain between 1e-3 and 1e7, otherwise
// "d.dddE<exp>". (JDK 8 occasionally prints one more digit than the
// shortest form; CONTRACT.md.)
func javaFloatString(f float64, bits int) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	case f == 0:
		if math.Signbit(f) {
			return "-0.0"
		}
		return "0.0"
	}
	abs := math.Abs(f)
	if abs >= 1e-3 && abs < 1e7 {
		s := strconv.FormatFloat(f, 'f', -1, bits)
		if !strings.Contains(s, ".") {
			s += ".0"
		}
		return s
	}
	s := strconv.FormatFloat(f, 'e', -1, bits) // d.ddde±xx
	mant, exp, _ := strings.Cut(s, "e")
	if !strings.Contains(mant, ".") {
		mant += ".0"
	}
	n, _ := strconv.Atoi(exp)
	return mant + "E" + strconv.Itoa(n)
}

// javaDecimalFormat is new DecimalFormat("#0.0#################").format(d)
// in the JDK 8 default (en) symbols: plain notation, 1 to 18 fraction
// digits rounded HALF_EVEN from the shortest digits, NaN as U+FFFD and
// infinity as U+221E.
func javaDecimalFormat(d float64) string {
	switch {
	case math.IsNaN(d):
		return "�"
	case math.IsInf(d, 1):
		return "∞"
	case math.IsInf(d, -1):
		return "-∞"
	}
	neg := math.Signbit(d)
	s := strconv.FormatFloat(math.Abs(d), 'e', -1, 64)
	mant, exp, _ := strings.Cut(s, "e")
	e, _ := strconv.Atoi(exp)
	digits := strings.Replace(mant, ".", "", 1)
	// value = 0.digits * 10^(e+1)
	point := e + 1
	var intPart, frac string
	switch {
	case point <= 0:
		intPart, frac = "0", strings.Repeat("0", -point)+digits
	case point >= len(digits):
		intPart, frac = digits+strings.Repeat("0", point-len(digits)), ""
	default:
		intPart, frac = digits[:point], digits[point:]
	}
	if len(frac) > 18 {
		intPart, frac = roundHalfEven(intPart, frac, 18)
	}
	frac = strings.TrimRight(frac, "0")
	if frac == "" {
		frac = "0"
	}
	intPart = strings.TrimLeft(intPart, "0")
	if intPart == "" {
		intPart = "0"
	}
	out := intPart + "." + frac
	if neg && out != "0.0" {
		out = "-" + out
	} else if neg {
		out = "-0.0"
	}
	return out
}

func roundHalfEven(intPart, frac string, keep int) (string, string) {
	digits := []byte(intPart + frac[:keep])
	rest := frac[keep:]
	up := false
	switch {
	case rest[0] > '5':
		up = true
	case rest[0] == '5':
		if strings.TrimRight(rest[1:], "0") != "" {
			up = true
		} else {
			up = (digits[len(digits)-1]-'0')%2 == 1
		}
	}
	if up {
		i := len(digits) - 1
		for ; i >= 0; i-- {
			if digits[i] == '9' {
				digits[i] = '0'
				continue
			}
			digits[i]++
			break
		}
		if i < 0 {
			digits = append([]byte{'1'}, digits...)
		}
	}
	n := len(digits) - keep
	return string(digits[:n]), string(digits[n:])
}

// javaHashtableOrder is the iteration order of a java.util.Hashtable filled
// with keys in the given order: capacity 11, load factor 0.75, head
// insertion, rehash to 2n+1 walking the old buckets from the top, and
// enumeration from the highest bucket down.
func javaHashtableOrder(keys []string) []string {
	type node struct {
		key  string
		hash int32
	}
	capacity := 11
	threshold := int(float64(capacity) * 0.75)
	table := make([][]node, capacity)
	count := 0
	index := func(h int32, c int) int { return int(uint32(h)&0x7fffffff) % c }
	for _, k := range keys {
		h := int32(javaStringHash(k))
		if count >= threshold {
			nc := capacity*2 + 1
			nt := make([][]node, nc)
			for i := capacity - 1; i >= 0; i-- {
				for _, n := range table[i] {
					at := index(n.hash, nc)
					nt[at] = append([]node{n}, nt[at]...)
				}
			}
			table, capacity, threshold = nt, nc, int(float64(nc)*0.75)
		}
		at := index(h, capacity)
		table[at] = append([]node{{k, h}}, table[at]...)
		count++
	}
	out := make([]string, 0, count)
	for i := capacity - 1; i >= 0; i-- {
		for _, n := range table[i] {
			out = append(out, n.key)
		}
	}
	return out
}
