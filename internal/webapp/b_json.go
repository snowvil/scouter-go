package webapp

import (
	"context"
	"encoding/base64"
	"hash/crc32"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// ---- Java number/text formatting -----------------------------------------

// bJavaDouble is Double.toString (shortest round-trip digits; JDK 19+ uses the
// same shortest algorithm, older JDKs may print extra digits in rare cases).
func bJavaDouble(d float64) string { return bJavaFloating(d, 64) }

// bJavaFloat is Float.toString.
func bJavaFloat(f float32) string { return bJavaFloating(float64(f), 32) }

func bJavaFloating(d float64, bits int) string {
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
	sign := ""
	if d < 0 {
		sign, d = "-", -d
	}
	// Shortest uniquely-distinguishing digits; when that is a single digit the
	// JDK selects the closest 2-digit decimal instead (Double.MIN_VALUE ->
	// 4.9E-324, Float.MIN_VALUE -> 1.4E-45).
	mant, exp, _ := strings.Cut(strconv.FormatFloat(d, 'e', -1, bits), "e")
	if !strings.Contains(mant, ".") {
		mant, exp, _ = strings.Cut(strconv.FormatFloat(d, 'e', 1, bits), "e")
	}
	digits := strings.TrimRight(strings.Replace(mant, ".", "", 1), "0")
	if digits == "" {
		digits = "0"
	}
	e, _ := strconv.Atoi(exp) // value = d1.d2d3... x 10^e
	if e >= -3 && e < 7 {
		if e < 0 {
			return sign + "0." + strings.Repeat("0", -e-1) + digits
		}
		if len(digits) <= e+1 {
			return sign + digits + strings.Repeat("0", e+1-len(digits)) + ".0"
		}
		return sign + digits[:e+1] + "." + digits[e+1:]
	}
	frac := digits[1:]
	if frac == "" {
		frac = "0"
	}
	return sign + digits[:1] + "." + frac + "E" + strconv.Itoa(e)
}

// bJavaDecimalFormat is DoubleValue.toString: DecimalFormat("#0.0#################").
func bJavaDecimalFormat(d float64) string {
	if math.IsNaN(d) {
		return "NaN" // DecimalFormat prints U+FFFD; not reproduced
	}
	if math.IsInf(d, 0) {
		if d > 0 {
			return "∞"
		}
		return "-∞"
	}
	s := strconv.FormatFloat(d, 'f', -1, 64)
	if i := strings.IndexByte(s, '.'); i >= 0 && len(s)-i-1 > 18 {
		s = strconv.FormatFloat(d, 'f', 18, 64) // HALF_EVEN vs Go rounding may differ in the last digit
		s = strings.TrimRight(s, "0")
	}
	if strings.HasPrefix(s, "-") && strings.Trim(s, "-0.") == "" {
		s = "-0"
	}
	if !strings.Contains(s, ".") {
		s += ".0"
	} else if strings.HasSuffix(s, ".") {
		s += "0"
	}
	return s
}

// bIPString is IPUtil.toString(byte[]) / ZZ.ipByteToString: first four bytes, "0.0.0.0" otherwise.
func bIPString(ip []byte) string {
	if len(ip) < 4 {
		return "0.0.0.0"
	}
	return strconv.Itoa(int(ip[0])) + "." + strconv.Itoa(int(ip[1])) + "." + strconv.Itoa(int(ip[2])) + "." + strconv.Itoa(int(ip[3]))
}

// bIPInt is IPUtil.toString(int): big-endian bytes of the int.
func bIPInt(n int32) string {
	u := uint32(n)
	return bIPString([]byte{byte(u >> 24), byte(u >> 16), byte(u >> 8), byte(u)})
}

// bJavaHash is HashUtil.hash(String): CRC32 of the (UTF-8) bytes.
func bJavaHash(s string) int32 { return int32(crc32.ChecksumIEEE([]byte(s))) }

// bBase64 is Jackson's byte[] serialization (MIME-less standard Base64); null stays null.
func bBase64(b []byte) any {
	if b == nil {
		return nil
	}
	return base64.StdEncoding.EncodeToString(b)
}

// ---- java.util.HashMap iteration order --------------------------------------

// bJavaHashOrder returns, for distinct keys inserted in the given order with the
// given Java hashCode values, the HashMap iteration order (indices). It models
// table growth from 16 with load factor 0.75 and order-preserving resize splits.
// Treeified bins (>= 8 collisions in one bucket at capacity >= 64) are not modeled.
func bJavaHashOrder(hashes []int32) []int {
	capacity := 16
	for float64(len(hashes)) > float64(capacity)*0.75 {
		capacity *= 2
	}
	idx := make([]int, len(hashes))
	for i := range idx {
		idx[i] = i
	}
	bucket := func(h int32) int {
		u := uint32(h)
		return int((u ^ (u >> 16)) & uint32(capacity-1))
	}
	sort.SliceStable(idx, func(a, b int) bool { return bucket(hashes[idx[a]]) < bucket(hashes[idx[b]]) })
	return idx
}

// bJavaIntSet returns the iteration order of a HashSet<Integer> built from list.
func bJavaIntSet(list []int32) []int32 {
	seen := make(map[int32]bool, len(list))
	uniq := make([]int32, 0, len(list))
	for _, v := range list {
		if !seen[v] {
			seen[v] = true
			uniq = append(uniq, v)
		}
	}
	out := make([]int32, len(uniq))
	for i, at := range bJavaHashOrder(uniq) {
		out[i] = uniq[at]
	}
	return out
}

// bJavaStringHash is String.hashCode over UTF-16 code units.
func bJavaStringHash(s string) int32 {
	var h int32
	for _, c := range utf16.Encode([]rune(s)) {
		h = 31*h + int32(c)
	}
	return h
}

// bJavaStringSet returns distinct strings in HashSet<String>/HashMap key order.
func bJavaStringSet(list []string) []string {
	seen := make(map[string]bool, len(list))
	uniq := make([]string, 0, len(list))
	for _, v := range list {
		if !seen[v] {
			seen[v] = true
			uniq = append(uniq, v)
		}
	}
	hashes := make([]int32, len(uniq))
	for i, s := range uniq {
		hashes[i] = bJavaStringHash(s)
	}
	out := make([]string, len(uniq))
	for i, at := range bJavaHashOrder(hashes) {
		out[i] = uniq[at]
	}
	return out
}

// ---- Scouter Value helpers ---------------------------------------------------

func bDec(n int64) protocol.Value    { return protocol.Value{Type: protocol.ValueDecimal, Int: n} }
func bTextV(s string) protocol.Value { return protocol.Value{Type: protocol.ValueText, Text: s} }
func bEntry(k string, v protocol.Value) protocol.Entry {
	return protocol.Entry{Key: k, Value: v}
}
func bMap(entries ...protocol.Entry) *protocol.MapPack { return &protocol.MapPack{Entries: entries} }

func bDecList(ns []int32) protocol.Value {
	vs := make([]protocol.Value, len(ns))
	for i, n := range ns {
		vs[i] = bDec(int64(n))
	}
	return protocol.Value{Type: protocol.ValueList, Values: vs}
}

func bTextList(ss []string) protocol.Value {
	vs := make([]protocol.Value, len(ss))
	for i, s := range ss {
		vs[i] = bTextV(s)
	}
	return protocol.Value{Type: protocol.ValueList, Values: vs}
}

// bOptText mirrors MapPack.put(key, String): a Java null becomes TextValue("").
func bOptText(s *string) protocol.Value {
	if s == nil {
		return bTextV("")
	}
	return bTextV(*s)
}

func bGet(p *protocol.MapPack, key string) (protocol.Value, bool) {
	if p == nil {
		return protocol.Value{}, false
	}
	for i := range p.Entries {
		if p.Entries[i].Key == key {
			return p.Entries[i].Value, true
		}
	}
	return protocol.Value{}, false
}

// bLong is Number.longValue() (0 for non-Number values, as MapPack/ListValue.getLong).
func bLong(v protocol.Value) int64 {
	switch v.Type {
	case protocol.ValueDecimal:
		return v.Int
	case protocol.ValueFloat:
		return bJavaF2L(float64(v.Float))
	case protocol.ValueDouble:
		return bJavaF2L(v.Double)
	}
	return 0
}

// bInt is Number.intValue().
func bInt(v protocol.Value) int32 {
	switch v.Type {
	case protocol.ValueDecimal:
		return int32(v.Int)
	case protocol.ValueFloat, protocol.ValueDouble:
		d := float64(v.Float)
		if v.Type == protocol.ValueDouble {
			d = v.Double
		}
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

func bJavaF2L(d float64) int64 {
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

func bMapLong(p *protocol.MapPack, key string) int64 {
	v, _ := bGet(p, key)
	return bLong(v)
}
func bMapInt(p *protocol.MapPack, key string) int32 {
	v, _ := bGet(p, key)
	return bInt(v)
}
func bMapBool(p *protocol.MapPack, key string) bool {
	v, _ := bGet(p, key)
	return v.Type == protocol.ValueBoolean && v.Bool
}

// bMapText is MapPack.getText: only a TextValue yields a string.
func bMapText(p *protocol.MapPack, key string) *string {
	v, ok := bGet(p, key)
	if !ok || v.Type != protocol.ValueText {
		return nil
	}
	s := v.Text
	return &s
}

func bMapList(p *protocol.MapPack, key string) []protocol.Value {
	v, ok := bGet(p, key)
	if !ok || v.Type != protocol.ValueList {
		return nil
	}
	return v.Values
}

func bAt(list []protocol.Value, i int) protocol.Value {
	if i < len(list) {
		return list[i]
	}
	return protocol.Value{}
}

// bListString is ListValue.getString(i).
func bListString(list []protocol.Value, i int) *string {
	if i >= len(list) {
		return nil
	}
	return bValueString(list[i])
}

// bValueString is Value.toString() (Java null -> nil).
func bValueString(v protocol.Value) *string {
	var s string
	switch v.Type {
	case protocol.ValueNull:
		return nil
	case protocol.ValueText:
		s = v.Text
	case protocol.ValueDecimal:
		s = strconv.FormatInt(v.Int, 10)
	case protocol.ValueBoolean:
		s = strconv.FormatBool(v.Bool)
	case protocol.ValueFloat:
		s = bJavaFloat(v.Float)
	case protocol.ValueDouble:
		s = bJavaDecimalFormat(v.Double)
	case protocol.ValueTextHash:
		s = strconv.FormatInt(int64(int32(v.Int)), 16)
	default:
		s = strings.TrimSpace(v.Text)
	}
	return &s
}

// bValueJava converts Value.toJavaObject() into a JSON-ready value with the
// Jackson WRITE_NUMBERS_AS_STRINGS representation.
func bValueJava(v protocol.Value) any {
	switch v.Type {
	case protocol.ValueNull:
		return nil
	case protocol.ValueBoolean:
		return v.Bool
	case protocol.ValueDecimal:
		return strconv.FormatInt(v.Int, 10)
	case protocol.ValueTextHash:
		return strconv.FormatInt(int64(int32(v.Int)), 10)
	case protocol.ValueFloat:
		return bJavaFloat(v.Float)
	case protocol.ValueDouble:
		return bJavaDouble(v.Double)
	case protocol.ValueText:
		return v.Text
	case protocol.ValueBlob, protocol.ValueIP4:
		return bBase64(v.Bytes)
	case protocol.ValueList:
		out := make([]any, len(v.Values))
		for i := range v.Values {
			out[i] = bValueJava(v.Values[i])
		}
		return out
	case protocol.ValueMap:
		obj := make(jsonObject, 0, len(v.Entries))
		for _, e := range v.Entries {
			obj = append(obj, jsonField{Key: e.Key, Value: bValueJava(e.Value)})
		}
		return obj
	case protocol.ValueIntArray:
		out := make([]string, len(v.Ints))
		for i, n := range v.Ints {
			out[i] = strconv.FormatInt(int64(n), 10)
		}
		return out
	case protocol.ValueLongArray:
		out := make([]string, len(v.Longs))
		for i, n := range v.Longs {
			out[i] = strconv.FormatInt(n, 10)
		}
		return out
	case protocol.ValueFloatArray:
		out := make([]string, len(v.Floats))
		for i, n := range v.Floats {
			out[i] = bJavaFloat(n)
		}
		return out
	case protocol.ValueTextArray:
		return v.Texts
	}
	return nil
}

// ---- collector calls ---------------------------------------------------------

// bCollect runs a call and returns every item.
func (s *Server) bCollect(ctx context.Context, cmd string, param protocol.Pack) ([]Item, error) {
	var items []Item
	err := s.call(ctx, cmd, param, func(it Item) error {
		items = append(items, it)
		return nil
	})
	return items, err
}

// bSingle is TcpProxy.getSingle: the first Pack or nil.
func (s *Server) bSingle(ctx context.Context, cmd string, param protocol.Pack) (protocol.Pack, error) {
	var first protocol.Pack
	err := s.call(ctx, cmd, param, func(it Item) error {
		if first == nil && it.Pack != nil {
			first = it.Pack
		}
		return nil
	})
	return first, err
}

// bSingleValue is TcpProxy.getSingleValue: the first Value or nil.
func (s *Server) bSingleValue(ctx context.Context, cmd string, param protocol.Pack) (*protocol.Value, error) {
	var first *protocol.Value
	err := s.call(ctx, cmd, param, func(it Item) error {
		if first == nil && it.Value != nil {
			first = it.Value
		}
		return nil
	})
	return first, err
}

// bInternal is a Java RuntimeException mapped by GenericExceptionMapper (500/500, message).
func bInternal(msg string) error {
	return newAPIError(http.StatusInternalServerError, http.StatusInternalServerError, msg)
}

// bWriteStream writes CommonResultView.jsonStream/jsonArrayStream:
// {"status":"200","resultCode":"0","message":"","result":...} without requestId.
// The Go version buffers the body so that a collector error before the first
// byte is reported as a normal error response instead of a truncated 200.
func (s *Server) bWriteStream(w http.ResponseWriter, r *http.Request, result any) {
	body, err := appendJSON(nil, jsonObject{
		{Key: "status", Value: 200},
		{Key: "resultCode", Value: 0},
		{Key: "message", Value: ""},
		{Key: "result", Value: result},
	})
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// bPreload mirrors TextLoader.loadAll: one GET_TEXT_ANY_TYPE batch for the
// distinct non-zero hashes before the per-hash lookups. As in Java, a failed
// preload is not an error (lookups fall back to "**unlabeled**").
func (s *Server) bPreload(ctx context.Context, keys []textKey) error {
	seen := make(map[textKey]bool, len(keys))
	uniq := make([]textKey, 0, len(keys))
	for _, k := range keys {
		if k.Hash != 0 && !seen[k] {
			seen[k] = true
			uniq = append(uniq, k)
		}
	}
	if len(uniq) > 0 {
		_ = s.loadTexts(ctx, uniq)
	}
	return ctx.Err()
}
