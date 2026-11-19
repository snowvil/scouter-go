package kafka

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"time"
)

// Record is one message for a specific topic partition. The caller decides the
// partition (Pulse uses Java String.hashCode floorMod, not a Kafka
// partitioner). Key nil encodes as a null key; an empty non-nil key is a
// zero-length key. Header order is preserved exactly.
type Record struct {
	Topic     string
	Partition int32
	Key       []byte
	Value     []byte
	Headers   []Header
}

// Header is a Kafka record header. Value nil encodes as null.
type Header struct {
	Key   string
	Value []byte
}

// RecordBatch magic 2 layout (all big-endian):
//
//	baseOffset int64 | batchLength int32 | partitionLeaderEpoch int32 | magic int8
//	crc uint32 (CRC32C over attributes..end) | attributes int16 | lastOffsetDelta int32
//	baseTimestamp int64 | maxTimestamp int64 | producerId int64 | producerEpoch int16
//	baseSequence int32 | recordCount int32 | records...
//
// batchLength counts everything after itself; crc covers everything after crc.
const (
	batchHeaderLen = 61
	batchCRCOffset = 21
	batchMagic     = 2
	// compression none, timestamp type create, not transactional, not control
	batchAttributes int16 = 0
	// attributes bits 0-2: compression codec (3 = lz4)
	batchCodecLZ4 int16 = 3
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type topicPartition struct {
	topic     string
	partition int32
}

// batch accumulates encoded records for one topic partition. It is owned by
// the accumulator until sealed and then by exactly one worker job until
// completed; no field is touched concurrently.
type batch struct {
	tp                          topicPartition
	created                     time.Time
	baseTimestamp, maxTimestamp int64
	records                     []byte
	count                       int32
}

func newBatch(tp topicPartition, now time.Time) *batch {
	return &batch{tp: tp, created: now}
}

// encodedSize is the full RecordBatch size and the number accounted against
// BufferBytes.
func (b *batch) encodedSize() int { return batchHeaderLen + len(b.records) }

// appendRecord encodes one record (already validated and size-checked by the
// caller through encodeRecord) at the next offset delta.
func (b *batch) appendRecord(encoded []byte, timestamp int64) {
	if b.count == 0 {
		b.baseTimestamp = timestamp
		b.maxTimestamp = timestamp
	} else if timestamp > b.maxTimestamp {
		b.maxTimestamp = timestamp
	}
	b.records = append(b.records, encoded...)
	b.count++
}

// encodeRecord appends the record (length varint + body) to dst. timestampDelta
// and offsetDelta must be computed by the caller against the batch base.
//
//	length varint | attributes int8 | timestampDelta varlong | offsetDelta varint
//	keyLength varint (-1 null) | key | valueLength varint (-1 null) | value
//	headerCount varint | [keyLength varint | key | valueLength varint (-1 null) | value]*
func encodeRecord(dst []byte, rec Record, timestampDelta int64, offsetDelta int32) []byte {
	bodyLen := 1 + varlongSize(timestampDelta) + varintSize(offsetDelta)
	bodyLen += varintSize(bytesLen(rec.Key)) + len(rec.Key)
	bodyLen += varintSize(bytesLen(rec.Value)) + len(rec.Value)
	bodyLen += varintSize(int32(len(rec.Headers)))
	for _, h := range rec.Headers {
		bodyLen += varintSize(int32(len(h.Key))) + len(h.Key)
		bodyLen += varintSize(bytesLen(h.Value)) + len(h.Value)
	}
	dst = appendVarint(dst, int32(bodyLen))
	dst = append(dst, 0) // record attributes: unused in magic 2
	dst = appendVarlong(dst, timestampDelta)
	dst = appendVarint(dst, offsetDelta)
	dst = appendVarint(dst, bytesLen(rec.Key))
	dst = append(dst, rec.Key...)
	dst = appendVarint(dst, bytesLen(rec.Value))
	dst = append(dst, rec.Value...)
	dst = appendVarint(dst, int32(len(rec.Headers)))
	for _, h := range rec.Headers {
		dst = appendVarint(dst, int32(len(h.Key)))
		dst = append(dst, h.Key...)
		dst = appendVarint(dst, bytesLen(h.Value))
		dst = append(dst, h.Value...)
	}
	return dst
}

func bytesLen(b []byte) int32 {
	if b == nil {
		return -1
	}
	return int32(len(b))
}

func varlongSize(v int64) int {
	u := zigzag64(v)
	n := 1
	for u >= 0x80 {
		u >>= 7
		n++
	}
	return n
}

// validateRecord bounds every length that must fit the int32 varint fields.
func validateRecord(rec Record) error {
	if rec.Topic == "" || len(rec.Topic) > math.MaxInt16 {
		return ErrInvalidRecord
	}
	if rec.Partition < 0 {
		return ErrInvalidRecord
	}
	if len(rec.Key) > math.MaxInt32 || len(rec.Value) > math.MaxInt32 || len(rec.Headers) > math.MaxInt32 {
		return ErrInvalidRecord
	}
	for _, h := range rec.Headers {
		if len(h.Key) > math.MaxInt32 || len(h.Value) > math.MaxInt32 {
			return ErrInvalidRecord
		}
	}
	return nil
}

// appendEncoded writes the complete RecordBatch to dst. With lz non-nil the
// records section is one LZ4 frame (attributes codec 3, as the Java producer's
// compression.type=lz4) unless the frame would not be smaller than the plain
// records; such a batch is sent uncompressed (codec 0) instead. The result is
// therefore never larger than encodedSize(), which keeps the BufferBytes and
// MaxRequestBytes accounting an upper bound of the bytes on the wire.
func (b *batch) appendEncoded(dst []byte, lz *lz4Compressor) []byte {
	start := len(dst)
	dst = binary.BigEndian.AppendUint64(dst, 0)              // baseOffset
	dst = binary.BigEndian.AppendUint32(dst, 0)              // batchLength placeholder
	dst = binary.BigEndian.AppendUint32(dst, math.MaxUint32) // partitionLeaderEpoch -1
	dst = append(dst, batchMagic)
	dst = binary.BigEndian.AppendUint32(dst, 0)                 // crc placeholder
	dst = binary.BigEndian.AppendUint16(dst, 0)                 // attributes placeholder
	dst = binary.BigEndian.AppendUint32(dst, uint32(b.count-1)) // lastOffsetDelta
	dst = binary.BigEndian.AppendUint64(dst, uint64(b.baseTimestamp))
	dst = binary.BigEndian.AppendUint64(dst, uint64(b.maxTimestamp))
	dst = binary.BigEndian.AppendUint64(dst, math.MaxUint64) // producerId -1
	dst = binary.BigEndian.AppendUint16(dst, math.MaxUint16) // producerEpoch -1
	dst = binary.BigEndian.AppendUint32(dst, math.MaxUint32) // baseSequence -1
	dst = binary.BigEndian.AppendUint32(dst, uint32(b.count))
	attributes := batchAttributes
	recordsAt := len(dst)
	if lz != nil && len(b.records) > 0 {
		dst = lz.appendLZ4Frame(dst, b.records)
		if len(dst)-recordsAt < len(b.records) {
			attributes |= batchCodecLZ4
		} else {
			dst = append(dst[:recordsAt], b.records...)
		}
	} else {
		dst = append(dst, b.records...)
	}
	binary.BigEndian.PutUint32(dst[start+8:], uint32(len(dst)-start-12))
	binary.BigEndian.PutUint16(dst[start+batchCRCOffset:], uint16(attributes))
	crc := crc32.Checksum(dst[start+batchCRCOffset:], castagnoli)
	binary.BigEndian.PutUint32(dst[start+batchCRCOffset-4:], crc)
	return dst
}
