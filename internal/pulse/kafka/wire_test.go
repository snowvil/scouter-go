package kafka

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
	"time"
)

// Zigzag/varint vectors follow Kafka's ByteUtils (and Protocol Buffers):
// zigzag(n) = (n << 1) ^ (n >> 31), then little-endian base-128 groups.
func TestVarintZigzagVectors(t *testing.T) {
	cases := []struct {
		v    int32
		want []byte
	}{
		{0, []byte{0x00}},
		{-1, []byte{0x01}},
		{1, []byte{0x02}},
		{-2, []byte{0x03}},
		{2, []byte{0x04}},
		{63, []byte{0x7e}},
		{-64, []byte{0x7f}},
		{64, []byte{0x80, 0x01}},
		{-65, []byte{0x81, 0x01}},
		{150, []byte{0xac, 0x02}},  // zigzag 300 = 0b100101100
		{-150, []byte{0xab, 0x02}}, // zigzag 299
		{8191, []byte{0xfe, 0x7f}},
		{8192, []byte{0x80, 0x80, 0x01}},
		{2147483647, []byte{0xfe, 0xff, 0xff, 0xff, 0x0f}},
		{-2147483648, []byte{0xff, 0xff, 0xff, 0xff, 0x0f}},
	}
	for _, c := range cases {
		got := appendVarint(nil, c.v)
		if !bytes.Equal(got, c.want) {
			t.Errorf("varint(%d) = %x, want %x", c.v, got, c.want)
		}
		if n := varintSize(c.v); n != len(c.want) {
			t.Errorf("varintSize(%d) = %d, want %d", c.v, n, len(c.want))
		}
		if got64 := appendVarlong(nil, int64(c.v)); !bytes.Equal(got64, c.want) {
			t.Errorf("varlong(%d) = %x, want %x", c.v, got64, c.want)
		}
	}
	long := []struct {
		v    int64
		want []byte
	}{
		// zigzag(2^40) = 2^41 = 128^5 * 64: five zero groups then 0x40.
		{1 << 40, []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x40}},
		{-(1 << 40), []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x3f}},
	}
	for _, c := range long {
		if got := appendVarlong(nil, c.v); !bytes.Equal(got, c.want) {
			t.Errorf("varlong(%d) = %x, want %x", c.v, got, c.want)
		}
		if n := varlongSize(c.v); n != len(c.want) {
			t.Errorf("varlongSize(%d) = %d, want %d", c.v, n, len(c.want))
		}
	}
	if got := appendVarlong(nil, -9223372036854775808); !bytes.Equal(got, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}) {
		t.Errorf("varlong(min) = %x", got)
	}
}

// CRC32C check values: the standard "123456789" vector is 0xE3069283
// (RFC 3720 / Castagnoli); "a" is 0xC1D04330; empty input is 0.
func TestCRC32CVectors(t *testing.T) {
	cases := []struct {
		in   string
		want uint32
	}{
		{"", 0},
		{"a", 0xC1D04330},
		{"123456789", 0xE3069283},
	}
	for _, c := range cases {
		if got := crc32.Checksum([]byte(c.in), castagnoli); got != c.want {
			t.Errorf("crc32c(%q) = %08x, want %08x", c.in, got, c.want)
		}
	}
}

// Hand-laid-out RecordBatch for one record key "k", value "v", header h=x at
// timestamp 1000ms. Record body (12 bytes): attributes 00, timestampDelta 00,
// offsetDelta 00, keyLen 02 'k', valueLen 02 'v', headerCount 02,
// headerKeyLen 02 'h', headerValueLen 02 'x'; length varint(12) = 0x18.
func TestRecordBatchExactBytes(t *testing.T) {
	b := newBatch(topicPartition{"t", 0}, time.UnixMilli(1000))
	rec := Record{Topic: "t", Key: []byte("k"), Value: []byte("v"), Headers: []Header{{Key: "h", Value: []byte("x")}}}
	b.appendRecord(encodeRecord(nil, rec, 0, 0), 1000)
	got := b.appendEncoded(nil, nil)

	record := []byte{0x18, 0x00, 0x00, 0x00, 0x02, 'k', 0x02, 'v', 0x02, 0x02, 'h', 0x02, 'x'}
	want := []byte{
		0, 0, 0, 0, 0, 0, 0, 0, // baseOffset
		0, 0, 0, 0x3e, // batchLength = 49 + 13
		0xff, 0xff, 0xff, 0xff, // partitionLeaderEpoch -1
		0x02,       // magic
		0, 0, 0, 0, // crc placeholder
		0, 0, // attributes
		0, 0, 0, 0, // lastOffsetDelta
		0, 0, 0, 0, 0, 0, 0x03, 0xe8, // baseTimestamp 1000
		0, 0, 0, 0, 0, 0, 0x03, 0xe8, // maxTimestamp 1000
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // producerId -1
		0xff, 0xff, // producerEpoch -1
		0xff, 0xff, 0xff, 0xff, // baseSequence -1
		0, 0, 0, 1, // record count
	}
	want = append(want, record...)
	if len(want) != 61+13 {
		t.Fatalf("layout length %d", len(want))
	}
	binary.BigEndian.PutUint32(want[17:], crc32.Checksum(want[21:], castagnoli))
	if !bytes.Equal(got, want) {
		t.Fatalf("batch bytes\n got %x\nwant %x", got, want)
	}
	if b.encodedSize() != len(got) {
		t.Fatalf("encodedSize %d != %d", b.encodedSize(), len(got))
	}
	meta, records, err := decodeBatch(got)
	if err != nil {
		t.Fatal(err)
	}
	if meta.count != 1 || meta.baseTimestamp != 1000 || meta.maxTimestamp != 1000 || meta.lastOffsetDelta != 0 {
		t.Fatalf("meta %+v", meta)
	}
	r := records[0]
	if string(r.key) != "k" || r.keyNull || string(r.value) != "v" || len(r.headers) != 1 || r.headers[0].Key != "h" || string(r.headers[0].Value) != "x" || r.timestamp != 1000 || r.offsetDelta != 0 {
		t.Fatalf("record %+v", r)
	}
}

func TestRecordNullAndEmptyEncoding(t *testing.T) {
	// null key -> varint(-1) = 0x01; empty value -> varint(0) = 0x00; null header value 0x01
	rec := Record{Topic: "t", Key: nil, Value: []byte{}, Headers: []Header{{Key: "", Value: nil}}}
	got := encodeRecord(nil, rec, -1, 5)
	want := []byte{0x10, 0x00, 0x01, 0x0a, 0x01, 0x00, 0x02, 0x00, 0x01}
	if !bytes.Equal(got, want) {
		t.Fatalf("record = %x, want %x", got, want)
	}
	b := newBatch(topicPartition{"t", 1}, time.Now())
	b.appendRecord(encodeRecord(nil, rec, 0, 0), 7)
	b.appendRecord(encodeRecord(nil, Record{Topic: "t", Value: []byte("zz")}, 3, 1), 10)
	_, records, err := decodeBatch(b.appendEncoded(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || !records[0].keyNull || records[0].value == nil || len(records[0].value) != 0 {
		t.Fatalf("records %+v", records)
	}
	if !records[0].headers[0].null || records[1].timestamp != 10 || records[1].offsetDelta != 1 || !records[1].keyNull {
		t.Fatalf("records %+v", records)
	}
}

func TestDecodeBatchRejectsBadCRC(t *testing.T) {
	b := newBatch(topicPartition{"t", 0}, time.Now())
	b.appendRecord(encodeRecord(nil, Record{Topic: "t", Value: []byte("v")}, 0, 0), 1)
	buf := b.appendEncoded(nil, nil)
	buf[len(buf)-1] ^= 0xff
	if _, _, err := decodeBatch(buf); err == nil {
		t.Fatal("corrupted batch accepted")
	}
}

func TestValidateRecord(t *testing.T) {
	if err := validateRecord(Record{Topic: "", Partition: 0}); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("empty topic: %v", err)
	}
	if err := validateRecord(Record{Topic: "t", Partition: -1}); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("negative partition: %v", err)
	}
	if err := validateRecord(Record{Topic: "t"}); err != nil {
		t.Fatalf("valid: %v", err)
	}
}

func TestDecoderLimits(t *testing.T) {
	limits := defaultLimits
	limits.MaxStringBytes = 4
	d := &decoder{buf: []byte{0x00, 0x05, 'a', 'b', 'c', 'd', 'e'}, limits: limits}
	if _, err := d.str(); !errors.Is(err, ErrLimit) {
		t.Fatalf("string over limit: %v", err)
	}
	d = &decoder{buf: []byte{0x00, 0x03, 'a'}, limits: limits}
	if _, err := d.str(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("truncated string: %v", err)
	}
	d = &decoder{buf: []byte{0xff, 0xff}, limits: limits}
	if _, err := d.str(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("null non-nullable string: %v", err)
	}
	d = &decoder{buf: []byte{0x7f, 0xff, 0xff, 0xff}, limits: limits}
	if _, err := d.count(1, 10); !errors.Is(err, ErrLimit) {
		t.Fatalf("count over limit: %v", err)
	}
	d = &decoder{buf: []byte{0x00, 0x00, 0x00, 0x08, 0, 0}, limits: limits}
	if _, err := d.count(4, 1000); !errors.Is(err, ErrMalformed) {
		t.Fatalf("count beyond remaining: %v", err)
	}
	d = &decoder{buf: []byte{0xff, 0xff, 0xff, 0xff}, limits: limits}
	if _, err := d.count(1, 1000); !errors.Is(err, ErrMalformed) {
		t.Fatalf("negative count: %v", err)
	}
}
