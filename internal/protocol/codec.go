// Package protocol implements the bounded, pinned Scouter wire codec.
// It has no network, storage, or queue dependencies. See CONTRACT.md for coverage.
// Scouter wire format: Copyright 2015 the original author or authors,
// https://github.com/scouter-project/scouter, Apache License 2.0.
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

var (
	ErrTruncated   = errors.New("truncated Scouter data")
	ErrLimit       = errors.New("Scouter resource limit exceeded")
	ErrInvalid     = errors.New("invalid Scouter data")
	ErrUnsupported = errors.New("unsupported Scouter type")
	ErrTrailing    = errors.New("trailing Scouter data")
)

// Limits bound each frame, Blob, collection, aggregate Value/Step count, and nesting.
// Zero fields select defaults; negative fields are invalid. These are codec limits,
// not a server resource allocation or a promise about RSS.
type Limits struct {
	MaxBytes             int
	MaxBlobBytes         int
	MaxCollectionEntries int
	MaxTotalValues       int
	MaxDepth             int
}

// AgentPackLimits bounds one Pack as an Agent sends it. The collection
// defaults (4096 entries, 16384 Values) are below what an Agent sends by
// default: a 5-minute SummaryPack table has up to 5000 rows
// (_summary_*_max_count) in up to six ListValue columns, and Java reads any
// size. 16384 rows in eight columns fit.
var AgentPackLimits = Limits{MaxCollectionEntries: 1 << 14, MaxTotalValues: 1 << 17}

func (l Limits) normalized() (Limits, error) {
	if l.MaxBytes < 0 || l.MaxBlobBytes < 0 || l.MaxCollectionEntries < 0 || l.MaxTotalValues < 0 || l.MaxDepth < 0 {
		return l, ErrInvalid
	}
	if l.MaxBytes == 0 {
		l.MaxBytes = 8 << 20
	}
	if l.MaxBlobBytes == 0 {
		l.MaxBlobBytes = 4 << 20
	}
	if l.MaxCollectionEntries == 0 {
		l.MaxCollectionEntries = 4096
	}
	if l.MaxTotalValues == 0 {
		l.MaxTotalValues = 16384
	}
	if l.MaxDepth == 0 {
		l.MaxDepth = 32
	}
	return l, nil
}

// Decoder borrows its input only during decoding. The caller must not mutate that
// input while this Decoder is in use. Every exported Blob/Pack/Value result owns
// its byte slices and survives return of the input buffer to a pool.
// The first error is sticky; methods return zero values after it. Check Err before
// using results. DecodePack and DecodeValue do this and reject trailing bytes.
type Decoder struct {
	data   []byte
	reader io.Reader
	// scratch is allocated only for stream mode; passing a slice of the Decoder
	// itself to io.ReadFull would force every Decoder onto the heap.
	scratch []byte
	offset  int
	limits  Limits
	err     error
	depth   int
	values  int
	// adopt: the caller handed the input over (AdoptInput), so Blob and raw
	// byte results may alias it instead of being copied.
	adopt bool
}

// AdoptInput hands the input over to the decoded results: Blob and the other
// raw byte fields may alias it (each with its capacity clipped, so an append
// never reaches a neighbour) instead of being copied. The caller must not
// modify or reuse the input afterwards. It has no effect on a stream decoder.
// The UDP receiver uses it for a datagram it copied for this decode only.
func (d *Decoder) AdoptInput() { d.adopt = d.reader == nil }

// owned returns b for a result: aliased when the input was handed over,
// otherwise a detached copy.
func (d *Decoder) owned(b []byte) []byte {
	if d.adopt {
		return b[:len(b):len(b)]
	}
	return ownedCopy(b)
}

func NewDecoder(data []byte, limits Limits) *Decoder {
	d := new(Decoder)
	d.init(data, limits)
	return d
}

// init prepares a zero Decoder in place so one-shot helpers can keep it on the
// stack instead of heap-allocating through NewDecoder.
func (d *Decoder) init(data []byte, limits Limits) {
	normalized, err := limits.normalized()
	*d = Decoder{data: data, limits: normalized, err: err}
	if err == nil && len(data) > normalized.MaxBytes {
		d.fail(ErrLimit, "frame bytes")
	}
}

// nestedFrame saves the outer input while a Blob body is decoded in place.
// The body bytes were already consumed by take, so offset is restored as is.
type nestedFrame struct {
	data   []byte
	reader io.Reader
	offset int
}

// enterBody re-scopes the Decoder to an already consumed Blob body so that
// Remaining and trailing-byte checks apply to the body alone. Limits, the
// sticky error and the aggregate Value budget continue to apply unchanged.
func (d *Decoder) enterBody(body []byte) nestedFrame {
	saved := nestedFrame{data: d.data, reader: d.reader, offset: d.offset}
	d.data, d.reader, d.offset = body, nil, 0
	return saved
}
func (d *Decoder) leaveBody(saved nestedFrame) {
	d.data, d.reader, d.offset = saved.data, saved.reader, saved.offset
}
func (d *Decoder) Err() error { return d.err }
func (d *Decoder) Remaining() int {
	if d.reader != nil {
		return d.limits.MaxBytes - d.offset
	}
	return len(d.data) - d.offset
}
func (d *Decoder) Offset() int { return d.offset }
func (d *Decoder) fail(kind error, reason string) {
	if d.err == nil {
		d.err = fmt.Errorf("%w at byte %d: %s", kind, d.offset, reason)
	}
}
func (d *Decoder) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 {
		d.fail(ErrInvalid, "negative length")
		return nil
	}
	if n > d.Remaining() {
		if d.reader != nil {
			d.fail(ErrLimit, "stream frame bytes")
			return nil
		}
		d.fail(ErrTruncated, "payload")
		return nil
	}
	if d.reader != nil {
		var b []byte
		if n <= cap(d.scratch) {
			b = d.scratch[:n]
		} else {
			b = make([]byte, n)
		}
		read, err := io.ReadFull(d.reader, b)
		d.offset += read
		if err != nil {
			d.err = fmt.Errorf("%w at byte %d: %w", ErrTruncated, d.offset, err)
			return nil
		}
		return b
	}
	b := d.data[d.offset : d.offset+n]
	d.offset += n
	return b
}
func (d *Decoder) Byte() byte {
	b := d.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}
func (d *Decoder) Bool() bool { return d.Byte() != 0 }
func (d *Decoder) Int16() int16 {
	b := d.take(2)
	if b == nil {
		return 0
	}
	return int16(binary.BigEndian.Uint16(b))
}
func (d *Decoder) Int32() int32 {
	b := d.take(4)
	if b == nil {
		return 0
	}
	return int32(binary.BigEndian.Uint32(b))
}
func (d *Decoder) Int64() int64 {
	b := d.take(8)
	if b == nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}
func (d *Decoder) Int3() int32 {
	b := d.take(3)
	if b == nil {
		return 0
	}
	return int32(b[0])<<24>>8 | int32(b[1])<<8 | int32(b[2])
}
func (d *Decoder) Long5() int64 {
	b := d.take(5)
	if b == nil {
		return 0
	}
	return int64(int8(b[0]))<<32 | int64(b[1])<<24 | int64(b[2])<<16 | int64(b[3])<<8 | int64(b[4])
}
func (d *Decoder) Float32() float32 { return math.Float32frombits(uint32(d.Int32())) }
func (d *Decoder) Float64() float64 { return math.Float64frombits(uint64(d.Int64())) }
func (d *Decoder) Decimal() int64 {
	switch width := d.Byte(); width {
	case 0:
		return 0
	case 1:
		return int64(int8(d.Byte()))
	case 2:
		return int64(d.Int16())
	case 3:
		return int64(d.Int3())
	case 4:
		return int64(d.Int32())
	case 5:
		return d.Long5()
	case 8:
		return d.Int64()
	default:
		d.fail(ErrInvalid, fmt.Sprintf("Decimal width %d", width))
		return 0
	}
}
func (d *Decoder) decimalInt32() int32 {
	v := d.Decimal()
	if v < math.MinInt32 || v > math.MaxInt32 {
		d.fail(ErrInvalid, "Decimal does not fit int32")
		return 0
	}
	return int32(v)
}
func (d *Decoder) blobView() []byte {
	var n int64
	switch prefix := d.Byte(); prefix {
	case 255:
		n = int64(uint16(d.Int16()))
	case 254:
		n = int64(d.Int32())
	default:
		n = int64(prefix)
	}
	if d.err != nil {
		return nil
	}
	if n < 0 {
		d.fail(ErrInvalid, "negative Blob length")
		return nil
	}
	if n > int64(d.limits.MaxBlobBytes) {
		d.fail(ErrLimit, "Blob bytes")
		return nil
	}
	return d.take(int(n))
}
func (d *Decoder) Blob() []byte {
	b := d.blobView()
	if d.err != nil {
		return nil
	}
	return d.owned(b)
}

// ownedCopy returns a detached copy; the result never aliases decoder input.
func ownedCopy(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
func (d *Decoder) Text() string {
	return string(d.textView())
}

// textView returns validated UTF-8 bytes that borrow the input (or the stream
// scratch buffer) only until the next read; callers must copy before returning.
func (d *Decoder) textView() []byte {
	// Java new String(...,"UTF8") accepts malformed UTF-8 with replacement. Normal
	// synthetic/Agent strings are UTF-8; invalid input is deliberately rejected.
	b := d.blobView()
	if d.err != nil {
		return nil
	}
	if !validText(b) {
		d.fail(ErrInvalid, "invalid UTF-8 Text")
		return nil
	}
	return b
}
func (d *Decoder) count(n int64, minBytes int) int {
	if d.err != nil {
		return 0
	}
	if n < 0 {
		d.fail(ErrInvalid, "negative collection count")
		return 0
	}
	if n > int64(d.limits.MaxCollectionEntries) || n > int64(d.limits.MaxTotalValues-d.values) {
		d.fail(ErrLimit, "collection count")
		return 0
	}
	if n > int64(d.Remaining()/minBytes) {
		if d.reader != nil {
			d.fail(ErrLimit, "stream collection bytes")
			return 0
		}
		d.fail(ErrTruncated, "collection items")
		return 0
	}
	return int(n)
}
func (d *Decoder) enterValue() bool {
	if d.err != nil {
		return false
	}
	if d.depth >= d.limits.MaxDepth || d.values >= d.limits.MaxTotalValues {
		d.fail(ErrLimit, "Value depth/count")
		return false
	}
	d.depth++
	d.values++
	return true
}

// Encoder appends owned output to one growing buffer. Bytes hands that buffer
// to the caller without copying: the Encoder never writes to a slice it has
// returned, so a later write first moves the remaining state to a new buffer.
// The first error is sticky; no partially encoded packet is returned as success.
type Encoder struct {
	data   []byte
	limits Limits
	err    error
	depth  int
	values int
	shared bool // data was returned by Bytes; copy before the next write
}

// encoderSizeHint is the initial capacity for whole-Pack/Value/Step encodes.
// It covers typical Agent packets in one allocation and is clamped to MaxBytes.
const encoderSizeHint = 256

func NewEncoder(limits Limits) *Encoder {
	e := new(Encoder)
	e.init(limits, 0)
	return e
}
func (e *Encoder) init(limits Limits, sizeHint int) {
	l, err := limits.normalized()
	*e = Encoder{limits: l, err: err}
	if err == nil && sizeHint > 0 {
		e.data = make([]byte, 0, min(sizeHint, l.MaxBytes))
	}
}
func (e *Encoder) Err() error { return e.err }

// Reset empties e for the next encode with the same limits, keeping its
// buffer. Slices returned by Bytes before the Reset must not be used after it.
func (e *Encoder) Reset() {
	l, err := e.limits.normalized()
	*e = Encoder{data: e.data[:0], limits: l, err: err}
}
func (e *Encoder) fail(kind error, reason string) {
	if e.err == nil {
		e.err = fmt.Errorf("%w at byte %d: %s", kind, len(e.data), reason)
	}
}

// grow validates MaxBytes for n more bytes and guarantees that appending them
// never writes into a buffer previously returned by Bytes.
func (e *Encoder) grow(n int) bool {
	if e.err != nil {
		return false
	}
	if n > e.limits.MaxBytes-len(e.data) {
		e.fail(ErrLimit, "encoded frame bytes")
		return false
	}
	if e.shared {
		fresh := make([]byte, len(e.data), max(2*len(e.data), len(e.data)+n))
		copy(fresh, e.data)
		e.data = fresh
		e.shared = false
	}
	return true
}
func (e *Encoder) raw(b []byte) {
	if e.grow(len(b)) {
		e.data = append(e.data, b...)
	}
}
func (e *Encoder) rawString(s string) {
	if e.grow(len(s)) {
		e.data = append(e.data, s...)
	}
}
func (e *Encoder) Byte(v byte) {
	if e.grow(1) {
		e.data = append(e.data, v)
	}
}
func (e *Encoder) Bool(v bool) {
	if v {
		e.Byte(1)
	} else {
		e.Byte(0)
	}
}
func (e *Encoder) Int16(v int16) {
	if e.grow(2) {
		e.data = binary.BigEndian.AppendUint16(e.data, uint16(v))
	}
}
func (e *Encoder) Int32(v int32) {
	if e.grow(4) {
		e.data = binary.BigEndian.AppendUint32(e.data, uint32(v))
	}
}
func (e *Encoder) Int64(v int64) {
	if e.grow(8) {
		e.data = binary.BigEndian.AppendUint64(e.data, uint64(v))
	}
}
func (e *Encoder) Int3(v int32) {
	if v < -8388608 || v > 8388607 {
		e.fail(ErrInvalid, "int3 overflow")
		return
	}
	if e.grow(3) {
		e.data = append(e.data, byte(v>>16), byte(v>>8), byte(v))
	}
}
func (e *Encoder) Long5(v int64) {
	if v < -549755813888 || v > 549755813887 {
		e.fail(ErrInvalid, "long5 overflow")
		return
	}
	if e.grow(5) {
		e.data = append(e.data, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
}
func (e *Encoder) Float32(v float32) {
	bits := math.Float32bits(v)
	if math.IsNaN(float64(v)) {
		bits = 0x7fc00000
	}
	e.Int32(int32(bits))
}
func (e *Encoder) Float64(v float64) {
	bits := math.Float64bits(v)
	if math.IsNaN(v) {
		bits = 0x7ff8000000000000
	}
	e.Int64(int64(bits))
}
func (e *Encoder) Decimal(v int64) {
	switch {
	case v == 0:
		e.Byte(0)
	case v >= math.MinInt8 && v <= math.MaxInt8:
		if e.grow(2) {
			e.data = append(e.data, 1, byte(v))
		}
	case v >= math.MinInt16 && v <= math.MaxInt16:
		if e.grow(3) {
			e.data = append(e.data, 2, byte(v>>8), byte(v))
		}
	case v >= -8388608 && v <= 8388607:
		if e.grow(4) {
			e.data = append(e.data, 3, byte(v>>16), byte(v>>8), byte(v))
		}
	case v >= math.MinInt32 && v <= math.MaxInt32:
		if e.grow(5) {
			e.data = append(e.data, 4, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		}
	case v >= -549755813888 && v <= 549755813887:
		if e.grow(6) {
			e.data = append(e.data, 5, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		}
	default:
		if e.grow(9) {
			e.data = binary.BigEndian.AppendUint64(append(e.data, 8), uint64(v))
		}
	}
}
func (e *Encoder) blobLength(n int) {
	if n > e.limits.MaxBlobBytes || int64(n) > math.MaxInt32 {
		e.fail(ErrLimit, "encoded Blob bytes")
		return
	}
	switch {
	case n <= 253:
		e.Byte(byte(n))
	case n <= 65535:
		e.Byte(255)
		e.Int16(int16(n))
	default:
		e.Byte(254)
		e.Int32(int32(n))
	}
}
func (e *Encoder) Blob(v []byte) { e.blobLength(len(v)); e.raw(v) }
func (e *Encoder) Text(v string) {
	if !validString(v) {
		e.fail(ErrInvalid, "invalid UTF-8 Text")
		return
	}
	e.blobLength(len(v))
	e.rawString(v)
}

// beginBlob reserves a one-byte Blob length prefix for a body encoded in place
// (XLog/Batch) and returns the body start. endBlob fixes the prefix up to the
// Java 0xff+uint16 / 0xfe+int32 form once the body length is known.
func (e *Encoder) beginBlob() int {
	e.Byte(0)
	return len(e.data)
}
func (e *Encoder) endBlob(start int) {
	if e.err != nil {
		return
	}
	n := len(e.data) - start
	if n > e.limits.MaxBlobBytes || int64(n) > math.MaxInt32 {
		e.fail(ErrLimit, "encoded Blob bytes")
		return
	}
	var prefix [5]byte
	extra := 0
	switch {
	case n <= 253:
		prefix[0] = byte(n)
	case n <= 65535:
		prefix[0] = 255
		binary.BigEndian.PutUint16(prefix[1:], uint16(n))
		extra = 2
	default:
		prefix[0] = 254
		binary.BigEndian.PutUint32(prefix[1:], uint32(n))
		extra = 4
	}
	if extra > 0 {
		if !e.grow(extra) {
			return
		}
		e.data = append(e.data, prefix[1:1+extra]...)
		copy(e.data[start+extra:], e.data[start:start+n])
		copy(e.data[start:], prefix[1:1+extra])
	}
	e.data[start-1] = prefix[0]
}

// Bytes returns the encoded bytes on success. The caller owns the result: the
// Encoder never writes into it again. Writing more after Bytes continues from
// a private copy, so a later Bytes call still returns the full output.
func (e *Encoder) Bytes() ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	e.shared = true
	return e.data[:len(e.data):len(e.data)], nil
}
func (e *Encoder) count(n int) {
	if n > e.limits.MaxCollectionEntries || n > e.limits.MaxTotalValues-e.values {
		e.fail(ErrLimit, "encoded collection count")
	}
}
func (e *Encoder) enterValue() bool {
	if e.err != nil {
		return false
	}
	if e.depth >= e.limits.MaxDepth || e.values >= e.limits.MaxTotalValues {
		e.fail(ErrLimit, "encoded Value depth/count")
		return false
	}
	e.depth++
	e.values++
	return true
}

func DecodePack(data []byte, limits Limits) (Pack, error) {
	return decodePack(data, limits, false)
}

// DecodePackAdopt is DecodePack for an input the caller hands over (see
// AdoptInput): the result may alias data, which must not change afterwards.
func DecodePackAdopt(data []byte, limits Limits) (Pack, error) {
	return decodePack(data, limits, true)
}

func decodePack(data []byte, limits Limits, adopt bool) (Pack, error) {
	var d Decoder
	d.init(data, limits)
	if adopt {
		d.AdoptInput()
	}
	p := d.Pack()
	if d.err == nil && d.Remaining() != 0 {
		d.fail(ErrTrailing, "Pack frame")
	}
	if d.err != nil {
		return nil, d.err
	}
	return p, nil
}
func EncodePack(p Pack, limits Limits) ([]byte, error) {
	var e Encoder
	e.init(limits, encoderSizeHint)
	e.Pack(p)
	return e.Bytes()
}
func DecodeValue(data []byte, limits Limits) (Value, error) {
	var d Decoder
	d.init(data, limits)
	v := d.Value()
	if d.err == nil && d.Remaining() != 0 {
		d.fail(ErrTrailing, "Value frame")
	}
	if d.err != nil {
		return Value{}, d.err
	}
	return v, nil
}
func EncodeValue(v Value, limits Limits) ([]byte, error) {
	var e Encoder
	e.init(limits, encoderSizeHint)
	e.Value(v)
	return e.Bytes()
}
