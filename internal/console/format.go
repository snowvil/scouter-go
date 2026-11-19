package console

import (
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
)

// Java helpers used by the term handlers. Each mirrors the pinned Scouter or
// JDK behavior recorded in testdata/format-jdk{8,17}.golden.

// decimalFormat is FormatUtil.print(Number, "#,##0") (frac 0) or "#,##0.0"
// (frac 1) for a value DecimalFormat formats through doubleValue(): HALF_EVEN
// on the exact binary value, ',' grouping (locale fixed, CONTRACT.md), and
// "-0" for a negative value that rounds to zero. Like DecimalFormat, digits
// beyond the shortest round-trip representation print as zeros.
func decimalFormat(v float64, frac int) string {
	switch {
	case math.IsNaN(v):
		return "NaN" // JDK 17 (CLDR); JDK 8 prints U+FFFD
	case math.IsInf(v, 1):
		return "∞"
	case math.IsInf(v, -1):
		return "-∞"
	}
	s := strconv.FormatFloat(v, 'f', frac, 64)
	shortest := strconv.FormatFloat(math.Abs(v), 'e', -1, 64)
	mant, exp, _ := strings.Cut(shortest, "e")
	digits := strings.Replace(mant, ".", "", 1)
	if e, err := strconv.Atoi(exp); err == nil && e+1 > len(digits) {
		s = digits + strings.Repeat("0", e+1-len(digits))
		if frac > 0 {
			s += "." + strings.Repeat("0", frac)
		}
		if math.Signbit(v) {
			s = "-" + s
		}
	}
	return group(s)
}

// longFormat is FormatUtil.print(long/int, "#,##0").
func longFormat(v int64) string { return group(strconv.FormatInt(v, 10)) }

// group inserts ',' every three integer digits of a plain decimal string.
func group(s string) string {
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign, s = "-", s[1:]
	}
	intPart, fracPart, hasFrac := strings.Cut(s, ".")
	var b strings.Builder
	b.WriteString(sign)
	for i, c := range intPart {
		if i > 0 && (len(intPart)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	if hasFrac {
		b.WriteByte('.')
		b.WriteString(fracPart)
	}
	return b.String()
}

// valueFormat is FormatUtil.print(Value, "#,##0"): NumberValue types go
// through DecimalFormat.format(doubleValue()), every other Value prints its
// toString() (BooleanValue "true", TextValue the text, ...).
func valueFormat(v protocol.Value) string {
	switch v.Type {
	case protocol.ValueDecimal:
		return decimalFormat(float64(v.Int), 0)
	case protocol.ValueFloat:
		return decimalFormat(float64(v.Float), 0)
	case protocol.ValueDouble:
		return decimalFormat(v.Double, 0)
	}
	return valueString(v)
}

// valueNumber is Number.doubleValue() for NumberValue; ok=false is the
// ClassCastException the handlers raise for any other Value.
func valueNumber(v protocol.Value) (float64, bool) {
	switch v.Type {
	case protocol.ValueDecimal:
		return float64(v.Int), true
	case protocol.ValueFloat:
		return float64(v.Float), true
	case protocol.ValueDouble:
		return v.Double, true
	}
	return 0, false
}

// valueString is Value.toString() for the value types a console line can
// show (counter values and tag values).
func valueString(v protocol.Value) string {
	switch v.Type {
	case protocol.ValueNull:
		return "null"
	case protocol.ValueBoolean:
		return strconv.FormatBool(v.Bool)
	case protocol.ValueDecimal:
		return strconv.FormatInt(v.Int, 10)
	case protocol.ValueFloat:
		return javaFloatString(v.Float)
	case protocol.ValueDouble:
		// DoubleValue.toString is DecimalFormat("#0.0#################").
		return doubleValueString(v.Double)
	case protocol.ValueText:
		return v.Text
	case protocol.ValueTextHash:
		return strconv.FormatInt(int64(int32(v.Int)), 16)
	case protocol.ValueIP4:
		return ipString(v.Bytes)
	}
	return "<value type " + strconv.Itoa(int(v.Type)) + ">"
}

// doubleValueString is DecimalFormat("#0.0#################") HALF_EVEN.
func doubleValueString(d float64) string {
	switch {
	case math.IsNaN(d):
		return "NaN"
	case math.IsInf(d, 1):
		return "∞"
	case math.IsInf(d, -1):
		return "-∞"
	}
	s := strconv.FormatFloat(d, 'f', -1, 64)
	if _, f, _ := strings.Cut(s, "."); len(f) > 18 {
		s = strconv.FormatFloat(d, 'f', 18, 64)
	}
	whole, frac, _ := strings.Cut(s, ".")
	frac = strings.TrimRight(frac, "0")
	if frac == "" {
		frac = "0"
	}
	return whole + "." + frac
}

// javaDoubleString is Double.toString(d) (string concatenation of a double)
// using the shortest round-trip digits. JDK 8-18 FloatingDecimal is not
// shortest for a few values (1.0E23 prints 9.999999999999999E22); the
// golden test records those cases.
func javaDoubleString(d float64) string {
	return javaFloating(d, 64)
}

func sign(d float64) string {
	if d < 0 {
		return "-"
	}
	return ""
}

// javaFloatString is Float.toString(f).
func javaFloatString(f float32) string { return javaFloating(float64(f), 32) }

func javaFloating(d float64, bits int) string {
	switch {
	case math.IsNaN(d):
		return "NaN"
	case math.IsInf(d, 1):
		return "Infinity"
	case math.IsInf(d, -1):
		return "-Infinity"
	case d == 0:
		if math.Signbit(d) {
			return "-0.0"
		}
		return "0.0"
	}
	abs := math.Abs(d)
	if bits == 64 && abs == math.SmallestNonzeroFloat64 {
		// FloatingDecimal keeps two digits for Double.MIN_VALUE.
		return sign(d) + "4.9E-324"
	}
	if abs >= 1e-3 && abs < 1e7 {
		s := strconv.FormatFloat(d, 'f', -1, bits)
		if !strings.Contains(s, ".") {
			s += ".0"
		}
		return s
	}
	shortest := strconv.FormatFloat(d, 'e', -1, bits)
	mant, exp, _ := strings.Cut(shortest, "e")
	if !strings.Contains(mant, ".") {
		mant += ".0"
	}
	e, _ := strconv.Atoi(exp)
	return mant + "E" + strconv.Itoa(e)
}

// logTime is DateUtil.getLogTime: HH:mm:ss.SSS on the fixed 24h day of
// DateTimeHelper, "00:00:00.000" before 2000-01-01 in the location.
func logTime(ms int64, loc *time.Location) string {
	d := scoutertime.Millis(ms, loc)
	b := make([]byte, 0, 12)
	b = pad(b, d/3600000, 2)
	b = append(b, ':')
	b = pad(b, d/60000%60, 2)
	b = append(b, ':')
	b = pad(b, d/1000%60, 2)
	b = append(b, '.')
	b = pad(b, d%1000, 3)
	return string(b)
}

func pad(b []byte, v int64, width int) []byte {
	s := strconv.FormatInt(v, 10)
	for i := len(s); i < width; i++ {
		b = append(b, '0')
	}
	return append(b, s...)
}

// hexa32 is scouter.util.Hexa32.toString32.
func hexa32(n int64) string {
	switch {
	case n == math.MinInt64:
		return "z8000000000000"
	case n < 0:
		return "z" + strconv.FormatInt(-n, 32)
	case n < 10:
		return strconv.FormatInt(n, 10)
	default:
		return "x" + strconv.FormatInt(n, 32)
	}
}

// ipString is IPUtil.toString(byte[]): "0.0.0.0" for null or short input.
func ipString(ip []byte) string {
	if len(ip) < 4 {
		return "0.0.0.0"
	}
	return strconv.Itoa(int(ip[0])) + "." + strconv.Itoa(int(ip[1])) + "." + strconv.Itoa(int(ip[2])) + "." + strconv.Itoa(int(ip[3]))
}

// javaLen is String.length(): UTF-16 code units.
func javaLen(s string) int {
	n := 0
	for _, r := range s {
		if utf16.RuneLen(r) == 2 {
			n += 2
		} else {
			n++
		}
	}
	return n
}

// leftPad is StringUtil.leftPad: spaces in front, never truncates.
func leftPad(s string, size int) string {
	if n := javaLen(s); n < size {
		return strings.Repeat(" ", size-n) + s
	}
	return s
}

// rpad is StringUtil.rpad: spaces behind, never truncates.
func rpad(s string, size int) string {
	if n := javaLen(s); n < size {
		return s + strings.Repeat(" ", size-n)
	}
	return s
}

// javaTrim is String.trim(): strips every char <= ' ' at both ends.
func javaTrim(s string) string {
	return strings.TrimFunc(s, func(r rune) bool { return r <= ' ' })
}

// tokenizer is StringUtil.tokenizer: StringTokenizer over any delimiter
// char, each token trimmed and empty ones dropped. ok=false is the null
// Java returns for an empty input.
func tokenizer(s, delims string) (tokens []string, ok bool) {
	if s == "" {
		return nil, false
	}
	for _, t := range strings.FieldsFunc(s, func(r rune) bool { return strings.ContainsRune(delims, r) }) {
		if t = javaTrim(t); t != "" {
			tokens = append(tokens, t)
		}
	}
	return tokens, true
}

// firstWord is StringUtil.firstWord(s, delims).
func firstWord(s, delims string) string {
	for _, t := range strings.FieldsFunc(s, func(r rune) bool { return strings.ContainsRune(delims, r) }) {
		return javaTrim(t)
	}
	return ""
}

// lastWord is StringUtil.lastWord(s, delims).
func lastWord(s, delims string) string {
	last := ""
	for _, t := range strings.FieldsFunc(s, func(r rune) bool { return strings.ContainsRune(delims, r) }) {
		last = javaTrim(t)
	}
	return last
}

// javaLess is String.compareTo order (UTF-16 code units), which differs from
// Go byte order only for supplementary characters.
func javaLess(a, b string) bool {
	ua, ub := utf16.Encode([]rune(a)), utf16.Encode([]rune(b))
	for i := 0; i < len(ua) && i < len(ub); i++ {
		if ua[i] != ub[i] {
			return ua[i] < ub[i]
		}
	}
	return len(ua) < len(ub)
}

// javaInt is CastUtil.cint(String): Integer.parseInt, 0 when it fails.
func javaInt(s string) int32 {
	v, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}
	return int32(v)
}
