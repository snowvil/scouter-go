package kafka

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

var (
	ErrMalformed          = errors.New("kafka: malformed message")
	ErrLimit              = errors.New("kafka: limit exceeded")
	ErrUnsupportedVersion = errors.New("kafka: broker offers no supported api version")
	ErrClosed             = errors.New("kafka: producer closed")
	ErrBufferFull         = errors.New("kafka: producer buffer full")
	ErrRecordTooLarge     = errors.New("kafka: record exceeds MaxRecordBytes")
	ErrInvalidRecord      = errors.New("kafka: invalid record")
	ErrTooManyTopics      = errors.New("kafka: MaxTopics exceeded")
	ErrDeliveryTimeout    = errors.New("kafka: delivery timeout expired")
	ErrAbandoned          = errors.New("kafka: batch abandoned at close")
	ErrInvalidConfig      = errors.New("kafka: invalid configuration")
)

// Limits bounds every length/count read from a broker. They are validated
// before any allocation so a hostile or broken broker cannot make the
// producer allocate unbounded memory.
type Limits struct {
	MaxResponseBytes int // whole response frame excluding the 4-byte size prefix
	MaxBrokers       int // brokers per Metadata response
	MaxTopics        int // topics per Metadata/Produce response
	MaxPartitions    int // partitions per topic in a response
	MaxStringBytes   int // any string field (host, topic, cluster id, ...)
	MaxAPIKeys       int // entries in an ApiVersions response
	// MaxTaggedFields bounds the tagged fields of one structure in a flexible
	// response. Tagged fields are skipped without allocation; each one's size
	// is checked against the remaining bytes.
	MaxTaggedFields int
}

var defaultLimits = Limits{
	MaxResponseBytes: 1 << 20,
	MaxBrokers:       1024,
	MaxTopics:        1024,
	MaxPartitions:    4096,
	MaxStringBytes:   32 << 10,
	MaxAPIKeys:       256,
	MaxTaggedFields:  64,
}

func (l Limits) normalized() (Limits, error) {
	for _, f := range []struct {
		p        *int
		fallback int
	}{
		{&l.MaxResponseBytes, defaultLimits.MaxResponseBytes},
		{&l.MaxBrokers, defaultLimits.MaxBrokers},
		{&l.MaxTopics, defaultLimits.MaxTopics},
		{&l.MaxPartitions, defaultLimits.MaxPartitions},
		{&l.MaxStringBytes, defaultLimits.MaxStringBytes},
		{&l.MaxAPIKeys, defaultLimits.MaxAPIKeys},
		{&l.MaxTaggedFields, defaultLimits.MaxTaggedFields},
	} {
		if *f.p < 0 {
			return l, fmt.Errorf("%w: negative limit", ErrInvalidConfig)
		}
		if *f.p == 0 {
			*f.p = f.fallback
		}
	}
	if l.MaxResponseBytes > math.MaxInt32 {
		return l, fmt.Errorf("%w: MaxResponseBytes exceeds int32", ErrInvalidConfig)
	}
	return l, nil
}

// encoder appends Kafka wire primitives: big-endian fixed-width integers,
// int16-length strings, int32-length bytes and zigzag varints (record fields).
type encoder struct{ buf []byte }

func (e *encoder) int8(v int8)   { e.buf = append(e.buf, byte(v)) }
func (e *encoder) int16(v int16) { e.buf = binary.BigEndian.AppendUint16(e.buf, uint16(v)) }
func (e *encoder) int32(v int32) { e.buf = binary.BigEndian.AppendUint32(e.buf, uint32(v)) }
func (e *encoder) int64(v int64) { e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(v)) }
func (e *encoder) bool(v bool) {
	if v {
		e.buf = append(e.buf, 1)
	} else {
		e.buf = append(e.buf, 0)
	}
}

// str writes a non-nullable string. Callers validate length <= MaxInt16 first.
func (e *encoder) str(s string) {
	e.int16(int16(len(s)))
	e.buf = append(e.buf, s...)
}

// nullableStr writes -1 for null, matching Kafka NULLABLE_STRING.
func (e *encoder) nullableStr(s string, null bool) {
	if null {
		e.int16(-1)
		return
	}
	e.str(s)
}

// bytes writes an int32-length-prefixed byte block (BYTES); nil is encoded as
// null (-1) which is what NULLABLE_BYTES means for the Produce records field.
func (e *encoder) nullableBytes(b []byte) {
	if b == nil {
		e.int32(-1)
		return
	}
	e.int32(int32(len(b)))
	e.buf = append(e.buf, b...)
}

// Flexible versions (KIP-482): unsigned varint lengths with a +1 offset so 0
// means null, and a tagged field section after every structure.

func (e *encoder) uvarint(v uint32) { e.buf = binary.AppendUvarint(e.buf, uint64(v)) }

// compactStr writes COMPACT_STRING. Callers validate length <= MaxInt16 first.
func (e *encoder) compactStr(s string) {
	e.uvarint(uint32(len(s)) + 1)
	e.buf = append(e.buf, s...)
}

// compactNull writes a null COMPACT_NULLABLE_STRING/ARRAY/RECORDS.
func (e *encoder) compactNull() { e.uvarint(0) }

func (e *encoder) compactArrayLen(n int) { e.uvarint(uint32(n) + 1) }

// compactBytes writes non-null COMPACT_BYTES/COMPACT_RECORDS.
func (e *encoder) compactBytes(b []byte) {
	e.uvarint(uint32(len(b)) + 1)
	e.buf = append(e.buf, b...)
}

// emptyTags writes a tagged field section with no fields.
func (e *encoder) emptyTags() { e.buf = append(e.buf, 0) }

func zigzag32(v int32) uint32 { return uint32((v << 1) ^ (v >> 31)) }
func zigzag64(v int64) uint64 { return uint64((v << 1) ^ (v >> 63)) }

func appendVarint(b []byte, v int32) []byte  { return binary.AppendUvarint(b, uint64(zigzag32(v))) }
func appendVarlong(b []byte, v int64) []byte { return binary.AppendUvarint(b, zigzag64(v)) }

func varintSize(v int32) int {
	u := zigzag32(v)
	n := 1
	for u >= 0x80 {
		u >>= 7
		n++
	}
	return n
}

// decoder reads Kafka wire primitives from a response body with bounds and
// limit checks. Every count is validated against both the configured limit and
// the remaining bytes before anything is allocated.
type decoder struct {
	buf    []byte
	off    int
	limits Limits
}

func (d *decoder) remaining() int { return len(d.buf) - d.off }
func (d *decoder) need(n int) error {
	if n < 0 || d.remaining() < n {
		return fmt.Errorf("%w: truncated at offset %d (need %d, have %d)", ErrMalformed, d.off, n, d.remaining())
	}
	return nil
}
func (d *decoder) int8() (int8, error) {
	if err := d.need(1); err != nil {
		return 0, err
	}
	v := int8(d.buf[d.off])
	d.off++
	return v, nil
}
func (d *decoder) int16() (int16, error) {
	if err := d.need(2); err != nil {
		return 0, err
	}
	v := int16(binary.BigEndian.Uint16(d.buf[d.off:]))
	d.off += 2
	return v, nil
}
func (d *decoder) int32() (int32, error) {
	if err := d.need(4); err != nil {
		return 0, err
	}
	v := int32(binary.BigEndian.Uint32(d.buf[d.off:]))
	d.off += 4
	return v, nil
}
func (d *decoder) int64() (int64, error) {
	if err := d.need(8); err != nil {
		return 0, err
	}
	v := int64(binary.BigEndian.Uint64(d.buf[d.off:]))
	d.off += 8
	return v, nil
}
func (d *decoder) bool() (bool, error) {
	v, err := d.int8()
	return v != 0, err
}

// str reads a non-nullable STRING. A -1 length is malformed here.
func (d *decoder) str() (string, error) {
	s, null, err := d.nullableStr()
	if err == nil && null {
		return "", fmt.Errorf("%w: null in non-nullable string", ErrMalformed)
	}
	return s, err
}

func (d *decoder) nullableStr() (string, bool, error) {
	n, err := d.int16()
	if err != nil {
		return "", false, err
	}
	if n == -1 {
		return "", true, nil
	}
	if n < 0 {
		return "", false, fmt.Errorf("%w: negative string length %d", ErrMalformed, n)
	}
	if int(n) > d.limits.MaxStringBytes {
		return "", false, fmt.Errorf("%w: string length %d > %d", ErrLimit, n, d.limits.MaxStringBytes)
	}
	if err := d.need(int(n)); err != nil {
		return "", false, err
	}
	s := string(d.buf[d.off : d.off+int(n)])
	d.off += int(n)
	return s, false, nil
}

// count reads a non-nullable ARRAY length and validates it against max and
// against the minimum encoded size of one element.
func (d *decoder) count(minElem, max int) (int, error) {
	n, err := d.int32()
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf("%w: negative array length %d", ErrMalformed, n)
	}
	if int(n) > max {
		return 0, fmt.Errorf("%w: array length %d > %d", ErrLimit, n, max)
	}
	if minElem > 0 && int(n)*minElem > d.remaining() {
		return 0, fmt.Errorf("%w: array length %d exceeds remaining %d bytes", ErrMalformed, n, d.remaining())
	}
	return int(n), nil
}

func (d *decoder) int32Array(max int) ([]int32, error) {
	n, err := d.count(4, max)
	if err != nil {
		return nil, err
	}
	out := make([]int32, n)
	for i := range out {
		if out[i], err = d.int32(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// uvarint reads an UNSIGNED_VARINT that must fit 32 bits (at most 5 bytes),
// like Java ByteUtils.readUnsignedVarint.
func (d *decoder) uvarint() (uint32, error) {
	var v uint32
	for i := 0; i < 5; i++ {
		if err := d.need(1); err != nil {
			return 0, err
		}
		b := d.buf[d.off]
		d.off++
		if i == 4 && b > 0x0f {
			return 0, fmt.Errorf("%w: unsigned varint exceeds 32 bits", ErrMalformed)
		}
		v |= uint32(b&0x7f) << (7 * i)
		if b < 0x80 {
			return v, nil
		}
	}
	return 0, fmt.Errorf("%w: unsigned varint longer than 5 bytes", ErrMalformed)
}

// compactLen reads a +1-offset length; null reports the 0 encoding.
func (d *decoder) compactLen() (n int, null bool, err error) {
	u, err := d.uvarint()
	if err != nil {
		return 0, false, err
	}
	if u == 0 {
		return 0, true, nil
	}
	if u-1 > math.MaxInt32 {
		return 0, false, fmt.Errorf("%w: compact length %d", ErrMalformed, u-1)
	}
	return int(u - 1), false, nil
}

func (d *decoder) compactNullableStr() (string, bool, error) {
	n, null, err := d.compactLen()
	if err != nil || null {
		return "", null, err
	}
	if n > d.limits.MaxStringBytes {
		return "", false, fmt.Errorf("%w: string length %d > %d", ErrLimit, n, d.limits.MaxStringBytes)
	}
	if err := d.need(n); err != nil {
		return "", false, err
	}
	s := string(d.buf[d.off : d.off+n])
	d.off += n
	return s, false, nil
}

func (d *decoder) compactStr() (string, error) {
	s, null, err := d.compactNullableStr()
	if err == nil && null {
		return "", fmt.Errorf("%w: null in non-nullable compact string", ErrMalformed)
	}
	return s, err
}

// compactCount reads a non-nullable COMPACT_ARRAY length with the same checks
// as count.
func (d *decoder) compactCount(minElem, max int) (int, error) {
	n, null, err := d.compactLen()
	if err != nil {
		return 0, err
	}
	if null {
		return 0, fmt.Errorf("%w: null in non-nullable compact array", ErrMalformed)
	}
	if n > max {
		return 0, fmt.Errorf("%w: array length %d > %d", ErrLimit, n, max)
	}
	if minElem > 0 && n > d.remaining()/minElem {
		return 0, fmt.Errorf("%w: array length %d exceeds remaining %d bytes", ErrMalformed, n, d.remaining())
	}
	return n, nil
}

func (d *decoder) compactInt32Array(max int) ([]int32, error) {
	n, err := d.compactCount(4, max)
	if err != nil {
		return nil, err
	}
	out := make([]int32, n)
	for i := range out {
		if out[i], err = d.int32(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// skipTags skips a tagged field section. None of the tagged fields brokers
// send (ApiVersions features, Produce CurrentLeader/NodeEndpoints, ...) is
// needed by this producer, so known and unknown tags are skipped alike.
func (d *decoder) skipTags() error {
	n, err := d.uvarint()
	if err != nil {
		return err
	}
	if int64(n) > int64(d.limits.MaxTaggedFields) {
		return fmt.Errorf("%w: %d tagged fields > %d", ErrLimit, n, d.limits.MaxTaggedFields)
	}
	if int64(n)*2 > int64(d.remaining()) {
		return fmt.Errorf("%w: %d tagged fields exceed remaining %d bytes", ErrMalformed, n, d.remaining())
	}
	for range n {
		if _, err := d.uvarint(); err != nil { // tag
			return err
		}
		size, err := d.uvarint()
		if err != nil {
			return err
		}
		if int64(size) > int64(d.remaining()) {
			return fmt.Errorf("%w: tagged field size %d exceeds remaining %d bytes", ErrMalformed, size, d.remaining())
		}
		d.off += int(size)
	}
	return nil
}

func (d *decoder) skip(n int) error {
	if err := d.need(n); err != nil {
		return err
	}
	d.off += n
	return nil
}

// done rejects trailing garbage so a mis-negotiated version is loud, not silent.
func (d *decoder) done() error {
	if d.remaining() != 0 {
		return fmt.Errorf("%w: %d trailing bytes", ErrMalformed, d.remaining())
	}
	return nil
}
