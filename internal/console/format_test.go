package console

import (
	"bufio"
	"encoding/hex"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// goFormat evaluates one oracle line (kind, input) with the Go helpers.
func goFormat(t *testing.T, kind, in string) string {
	t.Helper()
	bits := func() uint64 {
		v, err := strconv.ParseUint(in, 16, 64)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	long := func() int64 {
		v, err := strconv.ParseInt(in, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	text := func() string {
		if in == "-" {
			return ""
		}
		b, err := hex.DecodeString(in)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	switch kind {
	case "double#,##0":
		return decimalFormat(math.Float64frombits(bits()), 0)
	case "double#,##0.0":
		return decimalFormat(math.Float64frombits(bits()), 1)
	case "DoubleValue#,##0":
		return valueFormat(protocol.Value{Type: protocol.ValueDouble, Double: math.Float64frombits(bits())})
	case "float#,##0":
		return decimalFormat(float64(math.Float32frombits(uint32(bits()))), 0)
	case "float#,##0.0":
		return decimalFormat(float64(math.Float32frombits(uint32(bits()))), 1)
	case "FloatValue#,##0":
		return valueFormat(protocol.Value{Type: protocol.ValueFloat, Float: math.Float32frombits(uint32(bits()))})
	case "long#,##0":
		return longFormat(long())
	case "DecimalValue#,##0":
		return valueFormat(protocol.Value{Type: protocol.ValueDecimal, Int: long()})
	case "Double.toString":
		return javaDoubleString(math.Float64frombits(bits()))
	case "Integer.toString16":
		return valueString(protocol.Value{Type: protocol.ValueTextHash, Int: long()})
	case "Hexa32":
		return hexa32(long())
	case "leftPad5":
		return "[" + leftPad(text(), 5) + "]"
	case "rpad5":
		return "[" + rpad(text(), 5) + "]"
	}
	t.Fatalf("unknown oracle kind %q", kind)
	return ""
}

// knownJDKDifferences are oracle lines where the Go value deliberately
// differs: JDK 8 DecimalFormat prints U+FFFD for NaN (Go uses the JDK 17
// "NaN"), and JDK <= 18 Double.toString is not the shortest representation
// for 1.0E23 and 2.0E23 (Go prints the shortest digits).
var knownJDKDifferences = map[string]string{
	"double#,##0\t7ff8000000000000":      "NaN",
	"double#,##0.0\t7ff8000000000000":    "NaN",
	"DoubleValue#,##0\t7ff8000000000000": "NaN",
	"Double.toString\t44b52d02c7e14af6":  "1.0E23",
	"Double.toString\t44c52d02c7e14af6":  "2.0E23",
}

func TestJavaFormatOracle(t *testing.T) {
	for _, file := range []string{"testdata/format-jdk8.golden", "testdata/format-jdk17.golden"} {
		f, err := os.Open(file)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(f)
		lines, known := 0, 0
		for sc.Scan() {
			parts := strings.SplitN(sc.Text(), "\t", 3)
			if len(parts) != 3 {
				t.Fatalf("%s: bad line %q", file, sc.Text())
			}
			lines++
			got := goFormat(t, parts[0], parts[1])
			if want, ok := knownJDKDifferences[parts[0]+"\t"+parts[1]]; ok {
				known++
				if got != want {
					t.Errorf("%s %s %s: got %q, want the documented Go value %q", file, parts[0], parts[1], got, want)
				}
				continue
			}
			if got != parts[2] {
				t.Errorf("%s %s %s: got %q, Java %q", file, parts[0], parts[1], got, parts[2])
			}
		}
		f.Close()
		if err = sc.Err(); err != nil {
			t.Fatal(err)
		}
		if lines < 200 || known != len(knownJDKDifferences) {
			t.Fatalf("%s: %d lines, %d known differences", file, lines, known)
		}
	}
}

func TestLogTimeAndJavaHelpers(t *testing.T) {
	seoul, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	ms := time.Date(2026, 10, 15, 9, 5, 7, 42e6, seoul).UnixMilli()
	if got := logTime(ms, seoul); got != "09:05:07.042" {
		t.Fatal(got)
	}
	if got := logTime(time.Date(1999, 12, 31, 23, 0, 0, 0, seoul).UnixMilli(), seoul); got != "00:00:00.000" {
		t.Fatal(got)
	}
	if tokens, ok := tokenizer("", " "); ok || tokens != nil {
		t.Fatal("empty tokenizer must be Java null")
	}
	if tokens, ok := tokenizer("a ,b,, c", " ,"); !ok || strings.Join(tokens, "|") != "a|b|c" {
		t.Fatal(tokens)
	}
	if firstWord("  x y ", " ") != "x" || lastWord("x y z", " ") != "z" || lastWord("", " ") != "" {
		t.Fatal("first/last word")
	}
	if javaInt("12") != 12 || javaInt("x") != 0 || javaInt("99999999999") != 0 || javaInt("+3") != 3 {
		t.Fatal("cint")
	}
	if javaLess("\uffff", "\U0001F600") || !javaLess("\U0001F600", "\uffff") {
		// U+FFFF sorts after the surrogate pair in UTF-16 order.
		t.Fatal("javaLess must use UTF-16 order")
	}
	if ipString(nil) != "0.0.0.0" || ipString([]byte{10, 0, 0, 255}) != "10.0.0.255" {
		t.Fatal("ip")
	}
	if doubleValueString(0.1) != "0.1" || doubleValueString(12) != "12.0" {
		t.Fatal(doubleValueString(0.1))
	}
	if javaFloatString(1.5) != "1.5" || javaFloatString(1e10) != "1.0E10" {
		t.Fatal(javaFloatString(1e10))
	}
}
