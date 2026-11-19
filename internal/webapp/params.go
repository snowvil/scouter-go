package webapp

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
)

// ---- Java utility equivalents ----

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

// parseHexa32 is Hexa32.toLong32 ("" is 0).
func parseHexa32(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	switch s[0] {
	case 'z':
		if s == "z8000000000000" {
			return math.MinInt64, nil
		}
		n, err := strconv.ParseInt(s[1:], 32, 64)
		return -n, err
	case 'x':
		return strconv.ParseInt(s[1:], 32, 64)
	}
	return strconv.ParseInt(s, 10, 64)
}

// javaHash is scouter.util.HashUtil.hash(String): CRC32 of the UTF-8 bytes.
func javaHash(s string) int32 { return int32(crc32.ChecksumIEEE([]byte(s))) }

// javaStringHash is java.lang.String.hashCode over UTF-16 code units.
func javaStringHash(s string) int32 {
	var h int32
	for _, r := range s {
		if r >= 0x10000 {
			r -= 0x10000
			h = 31*h + int32(0xD800+(r>>10))
			h = 31*h + int32(0xDC00+(r&0x3ff))
			continue
		}
		h = 31*h + int32(r)
	}
	return h
}

// javaHashOrder returns indexes of keys in java.util.HashMap/HashSet
// iteration order for n entries inserted in index order with the given
// hashCodes (no removals; tree bins are not modeled).
func javaHashOrder(hashes []int32) []int {
	capacity := 16
	for float64(len(hashes)) > float64(capacity)*0.75 {
		capacity *= 2
	}
	order := make([]int, len(hashes))
	for i := range order {
		order[i] = i
	}
	bucket := func(h int32) int {
		u := uint32(h)
		return int((u ^ (u >> 16)) & uint32(capacity-1))
	}
	sort.SliceStable(order, func(a, b int) bool { return bucket(hashes[order[a]]) < bucket(hashes[order[b]]) })
	return order
}

// javaStringSetOrder dedupes values (first wins) in HashSet<String> order.
func javaStringSetOrder(values []string) []string {
	seen := map[string]bool{}
	var unique []string
	var hashes []int32
	for _, v := range values {
		if !seen[v] {
			seen[v] = true
			unique = append(unique, v)
			hashes = append(hashes, javaStringHash(v))
		}
	}
	out := make([]string, 0, len(unique))
	for _, i := range javaHashOrder(hashes) {
		out = append(out, unique[i])
	}
	return out
}

// javaIntSetOrder dedupes values in HashSet<Integer> order.
func javaIntSetOrder(values []int32) []int32 {
	seen := map[int32]bool{}
	var unique []int32
	for _, v := range values {
		if !seen[v] {
			seen[v] = true
			unique = append(unique, v)
		}
	}
	out := make([]int32, 0, len(unique))
	for _, i := range javaHashOrder(unique) {
		out = append(out, unique[i])
	}
	return out
}

// javaSplit is commons-lang StringUtils.split(str, sep, max): separator runs
// produce no empty tokens; with max > 0 the last token keeps the remainder.
func javaSplit(s string, sep byte, max int) []string {
	var out []string
	i, n := 0, len(s)
	for i < n {
		for i < n && s[i] == sep {
			i++
		}
		if i >= n {
			break
		}
		if max > 0 && len(out) == max-1 {
			out = append(out, s[i:])
			break
		}
		j := i
		for j < n && s[j] != sep {
			j++
		}
		out = append(out, s[i:j])
		i = j
	}
	return out
}

// splitParam is ZZ.splitParam: a leading '[' strips the first and last
// character, then commas split without trimming or empty tokens.
func splitParam(s string) []string {
	if s != "" && s[0] == '[' {
		if len(s) < 2 {
			s = ""
		} else {
			s = s[1 : len(s)-1]
		}
	}
	return javaSplit(s, ',', 0)
}

// splitParamInt32 is ZZ.splitParamAsInteger. Java's NumberFormatException
// (500) is reported as a 400 validation error.
func splitParamInt32(name, s string) ([]int32, error) {
	var out []int32
	for _, item := range splitParam(s) {
		n, err := strconv.ParseInt(item, 10, 32)
		if err != nil {
			return nil, validateError(fmt.Sprintf("%s has an invalid integer %q", name, item))
		}
		out = append(out, int32(n))
	}
	return out, nil
}

// ---- HTTP parameters ----

// queryInt32 reads an optional int query parameter (absent or empty is 0).
// Jersey answers a conversion failure with 404; Go answers 400.
func queryInt32(r *http.Request, name string) (int32, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return 0, validateError(fmt.Sprintf("%s is not an int: %q", name, v))
	}
	return int32(n), nil
}

func queryInt64(r *http.Request, name string) (int64, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, validateError(fmt.Sprintf("%s is not a long: %q", name, v))
	}
	return n, nil
}

func pathInt32(r *http.Request, name string) (int32, error) {
	v := r.PathValue(name)
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return 0, validateError(fmt.Sprintf("%s is not an int: %q", name, v))
	}
	return int32(n), nil
}

func pathInt64(r *http.Request, name string) (int64, error) {
	v := r.PathValue(name)
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, validateError(fmt.Sprintf("%s is not a long: %q", name, v))
	}
	return n, nil
}

// serverIDParam validates the optional serverId query. The Go webapp has one
// collector, which every serverId resolves to, as ServerManager does with a
// single server (getServer(id)==null falls back to the default server).
func serverIDParam(r *http.Request) error {
	_, err := queryInt32(r, "serverId")
	return err
}

// flexInt accepts a JSON number or numeric string (Jackson coercion).
type flexInt struct {
	Value int64
	Set   bool
}

func (f *flexInt) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" {
		return nil
	}
	s = strings.Trim(s, `"`)
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("not an integer: %s", b)
	}
	f.Value, f.Set = n, true
	return nil
}

// readBody decodes a JSON request body bounded by Config.MaxBodyBytes.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request, v any) error {
	body := http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
	dec := json.NewDecoder(body)
	if err := dec.Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			return validateError("request body is required")
		}
		return validateError("invalid JSON body: " + err.Error())
	}
	return nil
}

// ---- time ----

// location is the collector timezone (Config.Location).
func (s *Server) location() *time.Location { return s.cfg.Location }

// now is Config.Clock.
func (s *Server) now() time.Time { return s.cfg.Clock() }

// collectorTime is Server.getCurrentTime: local time + LOGIN clock delta.
func (s *Server) collectorTime() int64 {
	s.login.mu.Lock()
	delta := s.login.delta
	s.login.mu.Unlock()
	return s.now().UnixMilli() + delta
}

// today is DateUtil.yyyymmdd() in the collector timezone.
func (s *Server) today() string { return scoutertime.Date(s.now().UnixMilli(), s.cfg.Location) }

// dateBase is DateTimeHelper.BASE_TIME: 2000-01-01 00:00 in loc.
func dateBase(loc *time.Location) int64 {
	return time.Date(2000, 1, 1, 0, 0, 0, 0, loc).UnixMilli()
}

// dateUnit is DateTimeHelper.getDateUnit (Java long division truncates).
func dateUnit(ms int64, loc *time.Location) int64 {
	return (ms - dateBase(loc)) / scoutertime.DayMillis
}

// dateUnitMillis is DateTimeHelper.dateUnitToTimeMillis.
func dateUnitMillis(unit int64, loc *time.Location) int64 {
	return unit*scoutertime.DayMillis + dateBase(loc)
}

// ---- Value/MapPack readers with Scouter MapPack/ListValue semantics ----

func textValue(s string) protocol.Value { return protocol.Value{Type: protocol.ValueText, Text: s} }
func decimalValue(n int64) protocol.Value {
	return protocol.Value{Type: protocol.ValueDecimal, Int: n}
}
func listValue(values []protocol.Value) protocol.Value {
	if values == nil {
		values = []protocol.Value{}
	}
	return protocol.Value{Type: protocol.ValueList, Values: values}
}

func newMap(entries ...protocol.Entry) *protocol.MapPack {
	return &protocol.MapPack{Entries: entries}
}
func entry(key string, v protocol.Value) protocol.Entry { return protocol.Entry{Key: key, Value: v} }

// mapGet returns the value and presence (MapPack.get).
func mapGet(m *protocol.MapPack, key string) (protocol.Value, bool) {
	if m == nil {
		return protocol.Value{}, false
	}
	for i := len(m.Entries) - 1; i >= 0; i-- {
		if m.Entries[i].Key == key {
			return m.Entries[i].Value, true
		}
	}
	return protocol.Value{}, false
}

func isNumber(v protocol.Value) bool {
	return v.Type == protocol.ValueDecimal || v.Type == protocol.ValueFloat || v.Type == protocol.ValueDouble
}

// numInt64 is Number.longValue for Decimal/Float/Double; other types are 0.
func numInt64(v protocol.Value) int64 {
	switch v.Type {
	case protocol.ValueDecimal:
		return v.Int
	case protocol.ValueFloat:
		return javaD2L(float64(v.Float))
	case protocol.ValueDouble:
		return javaD2L(v.Double)
	}
	return 0
}

// javaD2L is the Java (long) cast of a double.
func javaD2L(d float64) int64 {
	switch {
	case math.IsNaN(d):
		return 0
	case d >= math.MaxInt64:
		return math.MaxInt64
	case d <= math.MinInt64:
		return math.MinInt64
	}
	return int64(d)
}

func numInt32(v protocol.Value) int32 {
	switch v.Type {
	case protocol.ValueDecimal:
		return int32(v.Int)
	case protocol.ValueFloat, protocol.ValueDouble:
		d := numFloat64(v)
		switch {
		case math.IsNaN(d):
			return 0
		case d >= math.MaxInt32:
			return math.MaxInt32
		case d <= math.MinInt32:
			return math.MinInt32
		}
		return int32(d)
	}
	return 0
}

func numFloat64(v protocol.Value) float64 {
	switch v.Type {
	case protocol.ValueDecimal:
		return float64(v.Int)
	case protocol.ValueFloat:
		return float64(v.Float)
	case protocol.ValueDouble:
		return v.Double
	}
	return 0
}

func mapInt(m *protocol.MapPack, key string) int64 { v, _ := mapGet(m, key); return numInt64(v) }
func mapInt32(m *protocol.MapPack, key string) int32 {
	v, _ := mapGet(m, key)
	return numInt32(v)
}
func mapBool(m *protocol.MapPack, key string) bool {
	v, _ := mapGet(m, key)
	return v.Type == protocol.ValueBoolean && v.Bool
}

// mapText is MapPack.getText: the text of a TextValue, otherwise "" (Java null).
func mapText(m *protocol.MapPack, key string) string {
	v, _ := mapGet(m, key)
	if v.Type == protocol.ValueText {
		return v.Text
	}
	return ""
}

// mapTextNull is MapPack.getText keeping Java null as nil.
func mapTextNull(m *protocol.MapPack, key string) any {
	v, _ := mapGet(m, key)
	if v.Type == protocol.ValueText {
		return v.Text
	}
	return nil
}

// mapList is MapPack.getList; ok=false where Java would get null.
func mapList(m *protocol.MapPack, key string) ([]protocol.Value, bool) {
	v, ok := mapGet(m, key)
	if !ok || v.Type != protocol.ValueList {
		return nil, false
	}
	return v.Values, true
}

// requireLists returns the named lists or an error where Java would throw a
// NullPointerException (500).
func requireLists(cmd string, m *protocol.MapPack, keys ...string) ([][]protocol.Value, error) {
	out := make([][]protocol.Value, len(keys))
	for i, k := range keys {
		l, ok := mapList(m, k)
		if !ok {
			return nil, fmt.Errorf("%s response has no %q list", cmd, k)
		}
		out[i] = l
	}
	return out, nil
}

// listAt bounds-checks like ArrayList.get (IndexOutOfBounds -> 500).
func listAt(cmd, key string, l []protocol.Value, i int) (protocol.Value, error) {
	if i >= len(l) {
		return protocol.Value{}, fmt.Errorf("%s response list %q is shorter than %d", cmd, key, i+1)
	}
	return l[i], nil
}

// valueString is ListValue.getString: TextValue text, Java null for a missing
// or NullValue, otherwise Value.toString.
func valueString(v protocol.Value) any {
	switch v.Type {
	case protocol.ValueText:
		return v.Text
	case protocol.ValueNull:
		return nil
	}
	return valueToString(v)
}

// valueToString is Value.toString for the scalar Value types.
func valueToString(v protocol.Value) any {
	switch v.Type {
	case protocol.ValueNull:
		return nil
	case protocol.ValueBoolean:
		return strconv.FormatBool(v.Bool)
	case protocol.ValueDecimal:
		return strconv.FormatInt(v.Int, 10)
	case protocol.ValueFloat:
		return javaFloatString(v.Float)
	case protocol.ValueDouble:
		return decimalFormat(v.Double, 1, 17)
	case protocol.ValueText:
		return v.Text
	case protocol.ValueTextHash:
		return strconv.FormatInt(int64(int32(v.Int)), 16)
	case protocol.ValueBlob:
		return "byte[" + strconv.Itoa(len(v.Bytes)) + "]"
	case protocol.ValueList:
		parts := make([]string, len(v.Values))
		for i, e := range v.Values {
			s, _ := valueToString(e).(string)
			if valueToString(e) == nil {
				s = "null"
			}
			parts[i] = s
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	return fmt.Sprintf("%v", javaObject(v))
}

// cString is CastUtil.cString(Value): Number values go through
// DecimalFormat("#0") (rounding HALF_EVEN to an integer), others toString.
func cString(v protocol.Value) any {
	if isNumber(v) {
		return decimalFormat(numFloat64(v), 0, 0)
	}
	return valueToString(v)
}

// decimalFormat mimics DecimalFormat("#0.0###") with minFrac..maxFrac
// fraction digits and HALF_EVEN rounding applied to the shortest decimal
// digits of d (DecimalFormat starts from Double.toString digits).
func decimalFormat(d float64, minFrac, maxFrac int) string {
	if math.IsNaN(d) {
		return "NaN"
	}
	if math.IsInf(d, 0) {
		if d < 0 {
			return "-\u221e"
		}
		return "\u221e"
	}
	s := strconv.FormatFloat(d, 'f', -1, 64)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	intPart, frac, _ := strings.Cut(s, ".")
	if len(frac) > maxFrac {
		digits := []byte(intPart + frac[:maxFrac])
		next, rest := frac[maxFrac], strings.TrimRight(frac[maxFrac+1:], "0")
		last := digits[len(digits)-1]
		up := next > '5' || next == '5' && (rest != "" || (last-'0')%2 == 1)
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
		intPart = string(digits[:len(digits)-maxFrac])
		frac = string(digits[len(digits)-maxFrac:])
	}
	frac = strings.TrimRight(frac, "0")
	for len(frac) < minFrac {
		frac += "0"
	}
	out := intPart
	if frac != "" {
		out += "." + frac
	}
	if neg {
		out = "-" + out
	}
	return out
}

// javaObject is Value.toJavaObject as Jackson would serialize it.
func javaObject(v protocol.Value) any {
	switch v.Type {
	case protocol.ValueNull:
		return nil
	case protocol.ValueBoolean:
		return v.Bool
	case protocol.ValueDecimal:
		return v.Int
	case protocol.ValueFloat:
		return v.Float
	case protocol.ValueDouble:
		return v.Double
	case protocol.ValueText:
		return v.Text
	case protocol.ValueTextHash:
		return int32(v.Int)
	case protocol.ValueBlob:
		if v.Bytes == nil {
			return nil
		}
		return base64.StdEncoding.EncodeToString(v.Bytes) // Jackson byte[]
	case protocol.ValueList:
		out := make([]any, len(v.Values))
		for i, e := range v.Values {
			out[i] = javaObject(e)
		}
		return out
	case protocol.ValueIntArray:
		return append([]int32{}, v.Ints...)
	case protocol.ValueFloatArray:
		return append([]float32{}, v.Floats...)
	case protocol.ValueTextArray:
		return append([]string{}, v.Texts...)
	case protocol.ValueLongArray:
		return append([]int64{}, v.Longs...)
	case protocol.ValueMap:
		return entriesObject(v.Entries)
	case protocol.ValueDoubleSummary:
		return jsonObject{{"sum", v.Double}, {"count", v.Count}, {"min", v.MinDouble}, {"max", v.MaxDouble}}
	case protocol.ValueLongSummary:
		return jsonObject{{"sum", v.Int}, {"count", v.Count}, {"min", v.MinInt}, {"max", v.MaxInt}}
	}
	return nil
}

// entriesObject keeps wire order (Java HashMap order is unspecified).
func entriesObject(entries []protocol.Entry) jsonObject {
	out := make(jsonObject, 0, len(entries))
	for _, e := range entries {
		out = append(out, jsonField{e.Key, javaObject(e.Value)})
	}
	return out
}
