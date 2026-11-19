// KeyValueStoreService / DatabaseService: Copyright 2015 Scouter authors,
// Apache-2.0. Independent implementation of pinned source dee47fb5.
package service

import (
	"context"
	"fmt"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
	"math"
	"math/big"
	"math/bits"
	"strconv"
	"strings"
	"unicode/utf16"
)

const GlobalKeySpace = "__SCOUTER_KV_GLOBAL__"

// LocalStoreBackend is optional. Implementations return owned data and enforce
// context cancellation and bounded database reads. now is epoch seconds.
type LocalStoreBackend interface {
	KeyValue(context.Context, string, string, int64) (string, bool, error)
	QueryList(context.Context, string, int64, int64, int32) ([]*protocol.StatusPack, error)
}

func LocalStoreCommands() []string {
	return []string{"GET_GLOBAL_KV", "GET_GLOBAL_KV_BULK", "GET_CUSTOM_KV", "GET_CUSTOM_KV_BULK", "EXIST_QUERY_LIST", "LOAD_QUERY_LIST"}
}

func (s *Service) HandleLocalStoreCommand(ctx context.Context, r transport.ClientRequest, stream *transport.Stream) (bool, error) {
	recognized := localStoreMutation(r.Command)
	for _, cmd := range LocalStoreCommands() {
		if r.Command == cmd {
			recognized = true
			break
		}
	}
	if !recognized {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if !r.Authenticated {
		return true, transport.ErrUnauthorized
	}
	if localStoreMutation(r.Command) {
		return true, s.localStoreMutation(ctx, r.Command, stream)
	}
	b, ok := s.backend.(LocalStoreBackend)
	if !ok {
		return true, transport.ErrUnsupported
	}
	// This command alone reads a Value; treating it as MapPack loses pipelining.
	if r.Command == "GET_GLOBAL_KV" {
		v, err := stream.ReadValue()
		if err != nil {
			return true, err
		}
		key, err := localKVKey(v)
		if err != nil {
			return true, err
		}
		return true, s.localSingleKV(ctx, b, GlobalKeySpace, key, stream)
	}
	p, err := readMap(stream)
	if err != nil {
		return true, err
	}
	if r.Command == "EXIST_QUERY_LIST" || r.Command == "LOAD_QUERY_LIST" {
		return true, s.localQueryList(ctx, b, r.Command, p, stream)
	}
	space := GlobalKeySpace
	if r.Command == "GET_CUSTOM_KV" || r.Command == "GET_CUSTOM_KV_BULK" {
		space, err = getText(p, "keySpace")
		if err != nil {
			return true, err
		}
	}
	if r.Command == "GET_CUSTOM_KV" {
		if lookup(p, "key").Type == protocol.ValueNull {
			return true, nil
		}
		key, err := getText(p, "key")
		if err != nil {
			return true, err
		}
		return true, s.localSingleKV(ctx, b, space, key, stream)
	}
	keys, err := getList(p, "key")
	if err != nil || len(keys) == 0 {
		return true, err
	}
	if len(keys) > extendedQueryCells {
		return true, transport.ErrLimit
	}
	result := newMap()
	positions := make(map[string]int, len(keys))
	used := 0
	for _, v := range keys {
		if err := ctx.Err(); err != nil {
			return true, err
		}
		key, err := localKVKey(v)
		if err != nil {
			return true, err
		}
		text, found, err := b.KeyValue(ctx, space, key, s.config.Clock().Unix())
		if err != nil {
			return true, err
		}
		if !found {
			continue
		}
		if at, ok := positions[key]; ok {
			used -= len(result.Entries[at].Value.Text)
			if len(text) > (8<<20)-used {
				return true, transport.ErrLimit
			}
			used += len(text)
			result.Entries[at].Value = stringValue(text)
		} else {
			if len(key) > (8<<20)-used || len(text) > (8<<20)-used-len(key) {
				return true, transport.ErrLimit
			}
			used += len(key) + len(text)
			positions[key] = len(result.Entries)
			result.Entries = append(result.Entries, entry(key, stringValue(text)))
		}
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	return true, stream.ResponsePack(result)
}

// localKVKey mirrors the pinned Value.toString methods for stable types.
func localKVKey(v protocol.Value) (string, error) {
	switch v.Type {
	case protocol.ValueNull:
		return "", nil
	case protocol.ValueText:
		return v.Text, nil
	case protocol.ValueDecimal:
		return strconv.FormatInt(v.Int, 10), nil
	case protocol.ValueBoolean:
		return strconv.FormatBool(v.Bool), nil
	case protocol.ValueFloat:
		return localJavaFloat(v.Float), nil
	case protocol.ValueDouble:
		return localJavaDouble(v.Double), nil
	case protocol.ValueTextHash:
		return strconv.FormatInt(int64(int32(v.Int)), 16), nil
	case protocol.ValueBlob:
		return "byte[" + strconv.Itoa(len(v.Bytes)) + "]", nil
	case protocol.ValueIP4:
		if len(v.Bytes) != 4 {
			return "", badParam("IP4 key")
		}
		return fmt.Sprintf("%d.%d.%d.%d", v.Bytes[0], v.Bytes[1], v.Bytes[2], v.Bytes[3]), nil
	case protocol.ValueList:
		values := make([]string, 0, len(v.Values))
		for _, item := range v.Values {
			text, err := localKVKey(item)
			if err != nil {
				return "", err
			}
			if item.Type == protocol.ValueNull {
				text = "null"
			}
			values = append(values, text)
		}
		return "[" + strings.Join(values, ",") + "]", nil
	case protocol.ValueIntArray:
		values := make([]string, len(v.Ints))
		for i, x := range v.Ints {
			values[i] = strconv.FormatInt(int64(x), 10)
		}
		return "[" + strings.Join(values, ", ") + "]", nil
	case protocol.ValueLongArray:
		values := make([]string, len(v.Longs))
		for i, x := range v.Longs {
			values[i] = strconv.FormatInt(x, 10)
		}
		return "[" + strings.Join(values, ", ") + "]", nil
	case protocol.ValueFloatArray:
		values := make([]string, len(v.Floats))
		for i, x := range v.Floats {
			values[i] = localJavaFloat(x)
		}
		return "[" + strings.Join(values, ", ") + "]", nil
	case protocol.ValueMap:
		ordered := localJavaMapEntries(v.Entries)
		values := make([]string, 0, len(ordered))
		for _, item := range ordered {
			text, err := localKVKey(item.Value)
			if err != nil {
				return "", err
			}
			if item.Value.Type == protocol.ValueNull {
				text = "null"
			}
			values = append(values, item.Key+"="+text)
		}
		return "{" + strings.Join(values, ",") + "}", nil
	case protocol.ValueTextArray:
		return "[" + strings.Join(v.Texts, ", ") + "]", nil
	case protocol.ValueLongSummary:
		return fmt.Sprintf("[sum=%d,count=%d,min=%d,max=%d]", v.Int, v.Count, v.MinInt, v.MaxInt), nil
	case protocol.ValueDoubleSummary:
		return fmt.Sprintf("[sum=%s,count=%d,min=%s,max=%s]", localJavaDouble(v.Double), v.Count, localJavaDouble(v.MinDouble), localJavaDouble(v.MaxDouble)), nil
	}
	return "", badParam("key Value type")
}

// Java 8-18 floating formatting uses symmetric half-ULP bounds (a quarter
// ULP at exact powers of two) and a distinct integer rounding fast path.
// Generate decimal digits with exact integer ratios rather than Go's shortest
// formatter, which chooses different strings for several legal Float keys.
func localJavaDigits(v float64, precision int) (string, int) {
	_, binaryExponent := math.Frexp(v)
	e := binaryExponent - 1
	sig := uint64(math.Ldexp(v, precision-1-e))
	width := precision
	if precision == 24 && e < -126 {
		width = bits.Len64(uint64(math.Ldexp(v, 149)))
	}
	if precision == 53 && e < -1022 {
		width = bits.Len64(uint64(math.Ldexp(v, 1074)))
	}
	if e >= -21 && e <= 62 && precision-bits.TrailingZeros64(sig)-e-1 <= 0 {
		integer := uint64(v)
		discard := 0
		if e > width && e-width-1 > 1 {
			for x := uint64(1) << uint(e-width-1); x >= 10; x /= 10 {
				discard++
			}
		}
		scale := uint64(1)
		for i := 0; i < discard; i++ {
			scale *= 10
		}
		rounded := integer / scale
		if integer%scale >= scale/2 && discard > 0 {
			rounded++
		}
		text := strconv.FormatUint(rounded, 10)
		exponent := len(text) - 1 + discard
		return strings.TrimRight(text, "0"), exponent
	}

	// Decimal digit generation uses an integer value/scale ratio and its
	// rounding margin. Width-limited JVM arithmetic must remain width-limited:
	// its overflow changes a few otherwise valid Float key strings.
	trailing := bits.TrailingZeros64(sig)
	nfrac := precision - trailing
	tiny := max(0, nfrac-e-1)
	estimate := int(math.Floor((math.Ldexp(v, -e)-1.5)*0.289529654 + 0.176091259 + float64(e)*0.301029995663981))
	b5 := max(0, -estimate)
	s5 := max(0, estimate)
	b2 := b5 + tiny + e - (nfrac - 1)
	s2 := s5 + tiny
	m2 := b5 + tiny + e - width
	common := min(b2, s2)
	b2 -= common
	s2 -= common
	m2 -= common
	if nfrac == 1 {
		m2--
	}
	if m2 < 0 {
		b2 -= m2
		s2 -= m2
		m2 = 0
	}
	five := func(n int) *big.Int { return new(big.Int).Exp(big.NewInt(5), big.NewInt(int64(n)), nil) }
	scaled := func(value uint64, p5, p2 int) *big.Int {
		return new(big.Int).Lsh(new(big.Int).Mul(new(big.Int).SetUint64(value), five(p5)), uint(p2))
	}
	bnum := scaled(sig>>uint(trailing), b5, b2)
	den := scaled(1, s5, s2)
	margin := scaled(1, b5, m2)
	tens := new(big.Int).Mul(den, big.NewInt(10))
	powerBits := func(p int) int {
		if p == 0 {
			return 0
		}
		return five(p).BitLen()
	}
	bbits := nfrac + b2 + powerBits(b5)
	sbits := s2 + 1 + powerBits(s5+1)
	mode := 0
	if bbits < 64 && sbits < 64 {
		mode = 64
		if bbits < 32 && sbits < 32 {
			mode = 32
		}
	}
	wrap := func(n *big.Int) *big.Int {
		if mode == 32 {
			return big.NewInt(int64(int32(n.Int64())))
		}
		if mode == 64 {
			return big.NewInt(n.Int64())
		}
		return n
	}
	digits := make([]byte, 0, 19)
	low, high := false, false
	for count := 0; count < 20; count++ {
		q, r := new(big.Int), new(big.Int)
		q.QuoRem(bnum, den, r)
		bnum = wrap(new(big.Int).Mul(r, big.NewInt(10)))
		margin = wrap(new(big.Int).Mul(margin, big.NewInt(10)))
		low = bnum.Cmp(margin) < 0
		sum := wrap(new(big.Int).Add(bnum, margin))
		high = sum.Cmp(tens) > 0
		if mode == 0 {
			high = sum.Cmp(tens) >= 0
		}
		if count > 0 && mode != 0 && margin.Sign() <= 0 {
			low = true
			high = true
		}
		if count == 0 && q.Sign() == 0 && !high {
			estimate--
		} else {
			digits = append(digits, byte(q.Int64())+'0')
		}
		if count == 0 && (estimate < -3 || estimate >= 8) {
			low = false
			high = false
		}
		if low || high {
			break
		}
	}
	if high {
		difference := wrap(new(big.Int).Sub(wrap(new(big.Int).Lsh(new(big.Int).Set(bnum), 1)), tens))
		if !low || difference.Sign() > 0 || difference.Sign() == 0 && digits[len(digits)-1]&1 != 0 {
			i := len(digits) - 1
			for i > 0 && digits[i] == '9' {
				digits[i] = '0'
				i--
			}
			if digits[i] == '9' {
				digits[0] = '1'
				estimate++
			} else {
				digits[i]++
			}
		}
	}
	return string(digits), estimate
}

func localJavaFloat(v float32) string {
	x := float64(v)
	if math.IsNaN(x) {
		return "NaN"
	}
	if math.IsInf(x, 1) {
		return "Infinity"
	}
	if math.IsInf(x, -1) {
		return "-Infinity"
	}
	sign := ""
	if math.Signbit(x) {
		sign = "-"
		x = -x
	}
	if x == 0 {
		return sign + "0.0"
	}
	digits, exponent := localJavaDigits(x, 24)
	if exponent >= -3 && exponent < 7 {
		return sign + localPlainDigits(digits, exponent)
	}
	fraction := digits[1:]
	if fraction == "" {
		fraction = "0"
	}
	return sign + digits[:1] + "." + fraction + "E" + strconv.Itoa(exponent)
}
func localPlainDigits(digits string, exponent int) string {
	point := exponent + 1
	if point <= 0 {
		return "0." + strings.Repeat("0", -point) + digits
	}
	if point >= len(digits) {
		return digits + strings.Repeat("0", point-len(digits)) + ".0"
	}
	return digits[:point] + "." + digits[point:]
}

// DoubleValue uses DecimalFormat #0.0#################, not Double.toString.
// The baseline default Korean/English JVM locale uses '.' and the infinity
// symbol. Locale-specific decimal separators cannot be inferred from wire.
func localJavaDouble(v float64) string {
	if math.IsNaN(v) {
		return "NaN"
	}
	if math.IsInf(v, 1) {
		return "∞"
	}
	if math.IsInf(v, -1) {
		return "-∞"
	}
	sign := ""
	x := v
	if math.Signbit(v) {
		sign = "-"
		x = -v
	}
	if x == 0 {
		return sign + "0.0"
	}
	digits, exponent := localJavaDigits(x, 53)
	plain := localPlainDigits(digits, exponent)
	point := strings.IndexByte(plain, '.')
	if len(plain)-point-1 <= 18 {
		return sign + plain
	}
	scaled := new(big.Rat).SetFloat64(x)
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	scaled.Mul(scaled, new(big.Rat).SetInt(scale))
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(scaled.Num(), scaled.Denom(), r)
	cmp := new(big.Int).Lsh(r, 1).Cmp(scaled.Denom())
	if cmp > 0 || cmp == 0 && q.Bit(0) == 1 {
		q.Add(q, big.NewInt(1))
	}
	text := q.String()
	if len(text) <= 18 {
		text = strings.Repeat("0", 19-len(text)) + text
	}
	point = len(text) - 18
	fraction := strings.TrimRight(text[point:], "0")
	if fraction == "" {
		fraction = "0"
	}
	return sign + text[:point] + "." + fraction
}

// Reconstruct the Hashtable created by MapValue.read from its wire insertion
// order. Java's default capacity 11/load factor .75 and bucket-chain rehash
// affect both MapValue.toString and StatusPack.toMapPack for colliding keys.
func localJavaMapEntries(entries []protocol.Entry) []protocol.Entry {
	heads := make([]int, 11)
	for i := range heads {
		heads[i] = -1
	}
	next := make([]int, len(entries))
	hashes := make([]uint32, len(entries))
	hash := func(text string) uint32 {
		var h uint32
		for _, c := range text {
			if c <= 0xffff {
				h = 31*h + uint32(c)
			} else {
				high, low := utf16.EncodeRune(c)
				h = 31*h + uint32(high)
				h = 31*h + uint32(low)
			}
		}
		return h & 0x7fffffff
	}
	threshold := 8
	for i, entry := range entries {
		if i >= threshold {
			fresh := make([]int, 2*len(heads)+1)
			for n := range fresh {
				fresh[n] = -1
			}
			for bucket := len(heads) - 1; bucket >= 0; bucket-- {
				for current := heads[bucket]; current >= 0; {
					oldNext := next[current]
					at := int(hashes[current] % uint32(len(fresh)))
					next[current] = fresh[at]
					fresh[at] = current
					current = oldNext
				}
			}
			heads = fresh
			threshold = len(heads) * 3 / 4
		}
		hashes[i] = hash(entry.Key)
		bucket := int(hashes[i] % uint32(len(heads)))
		next[i] = heads[bucket]
		heads[bucket] = i
	}
	out := make([]protocol.Entry, 0, len(entries))
	for bucket := len(heads) - 1; bucket >= 0; bucket-- {
		for current := heads[bucket]; current >= 0; current = next[current] {
			out = append(out, entries[current])
		}
	}
	return out
}

func (s *Service) localSingleKV(ctx context.Context, b LocalStoreBackend, space, key string, stream *transport.Stream) error {
	text, _, err := b.KeyValue(ctx, space, key, s.config.Clock().Unix())
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// TextValue(null) writes an empty Text, not a NullValue.
	return stream.ResponseValue(stringValue(text))
}
func (s *Service) localQueryList(ctx context.Context, b LocalStoreBackend, cmd string, p *protocol.MapPack, stream *transport.Stream) error {
	hash, err := getInt(p, "objHash")
	if err != nil {
		return err
	}
	date, err := s.date(p, false)
	if err != nil {
		return err
	}
	if date == "" {
		return badParam("date")
	}
	from, err := getNumber(p, "stime")
	if err != nil {
		return err
	}
	to, err := getNumber(p, "etime")
	if err != nil {
		return err
	}
	records, err := b.QueryList(ctx, date, from, to, hash)
	if err != nil {
		return err
	}
	if len(records) > extendedQueryCells {
		return transport.ErrLimit
	}
	times := make([]protocol.Value, 0, len(records))
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if record == nil || record.ObjHash != hash {
			continue
		}
		if record.Data.Type != protocol.ValueMap {
			return badParam("Status data")
		}
		if cmd == "EXIST_QUERY_LIST" {
			times = append(times, decimal(record.Time))
			continue
		}
		// StatusPack.toMapPack copies only data's Hashtable enumeration; metadata
		// time/objHash/key are never synthesized into the response.
		if err := stream.ResponsePack(&protocol.MapPack{Entries: localJavaMapEntries(record.Data.Entries)}); err != nil {
			return err
		}
	}
	if cmd == "EXIST_QUERY_LIST" {
		return stream.ResponsePack(newMap(entry("time", list(times))))
	}
	return nil
}
