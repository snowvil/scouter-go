package kafka

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// Test-only LZ4 frame reader, written from the LZ4 frame/block format
// specifications independently of the writer. It accepts any valid frame
// (block checksums, content size, content checksum, dependent blocks) so it
// also decodes frames written by lz4-java (see TestJavaLZ4Oracle).

const testLZ4MaxOutput = 64 << 20

func lz4DecodeFrame(frame []byte) ([]byte, error) {
	if len(frame) < 7 {
		return nil, errors.New("short frame")
	}
	if binary.LittleEndian.Uint32(frame) != lz4Magic {
		return nil, fmt.Errorf("magic %x", frame[:4])
	}
	flg, bd := frame[4], frame[5]
	if flg>>6 != 1 {
		return nil, fmt.Errorf("version %d", flg>>6)
	}
	if flg&0x02 != 0 || bd&0x8f != 0 {
		return nil, fmt.Errorf("reserved bits FLG %x BD %x", flg, bd)
	}
	independent := flg&0x20 != 0
	blockChecksum := flg&0x10 != 0
	contentSize := flg&0x08 != 0
	contentChecksum := flg&0x04 != 0
	dictID := flg&0x01 != 0
	maxBlock := map[byte]int{4: 64 << 10, 5: 256 << 10, 6: 1 << 20, 7: 4 << 20}[bd>>4]
	if maxBlock == 0 {
		return nil, fmt.Errorf("block max size code %d", bd>>4)
	}
	off := 6
	if contentSize {
		off += 8
	}
	if dictID {
		off += 4
	}
	if len(frame) < off+1 {
		return nil, errors.New("short descriptor")
	}
	if want := byte(xxhash32(frame[4:off], 0) >> 8); frame[off] != want {
		return nil, fmt.Errorf("header checksum %x want %x", frame[off], want)
	}
	off++
	var out []byte
	for {
		if len(frame) < off+4 {
			return nil, errors.New("missing block size / end mark")
		}
		size := binary.LittleEndian.Uint32(frame[off:])
		off += 4
		if size == 0 {
			break
		}
		raw := size&lz4BlockUncompressed != 0
		n := int(size &^ lz4BlockUncompressed)
		if n > maxBlock || len(frame) < off+n {
			return nil, fmt.Errorf("block size %d", n)
		}
		block := frame[off : off+n]
		off += n
		if blockChecksum {
			if len(frame) < off+4 || binary.LittleEndian.Uint32(frame[off:]) != xxhash32(block, 0) {
				return nil, errors.New("block checksum")
			}
			off += 4
		}
		if raw {
			out = append(out, block...)
			continue
		}
		start := len(out)
		var err error
		if independent {
			out, err = lz4DecodeBlock(out, block, start, maxBlock)
		} else {
			out, err = lz4DecodeBlock(out, block, 0, maxBlock)
		}
		if err != nil {
			return nil, err
		}
		if len(out) > testLZ4MaxOutput {
			return nil, errors.New("output too large")
		}
	}
	if contentChecksum {
		if len(frame) < off+4 || binary.LittleEndian.Uint32(frame[off:]) != xxhash32(out, 0) {
			return nil, errors.New("content checksum")
		}
		off += 4
	}
	if off != len(frame) {
		return nil, fmt.Errorf("%d trailing bytes", len(frame)-off)
	}
	return out, nil
}

// lz4DecodeBlock appends the decoded block to out. Matches may reference
// out[window:]; window is the block start for independent blocks.
func lz4DecodeBlock(out, src []byte, window, maxBlock int) ([]byte, error) {
	start := len(out)
	length := func(i int, base int) (int, int, error) {
		n := base
		if base != 15 {
			return n, i, nil
		}
		for {
			if i >= len(src) {
				return 0, 0, errors.New("truncated length")
			}
			b := src[i]
			i++
			n += int(b)
			if n > maxBlock {
				return 0, 0, errors.New("length overflow")
			}
			if b != 255 {
				return n, i, nil
			}
		}
	}
	i := 0
	for {
		if i >= len(src) {
			return nil, errors.New("missing token")
		}
		token := src[i]
		i++
		lit, ni, err := length(i, int(token>>4))
		if err != nil {
			return nil, err
		}
		i = ni
		if i+lit > len(src) {
			return nil, errors.New("truncated literals")
		}
		out = append(out, src[i:i+lit]...)
		i += lit
		if i == len(src) {
			break // last sequence has literals only
		}
		if i+2 > len(src) {
			return nil, errors.New("truncated offset")
		}
		offset := int(binary.LittleEndian.Uint16(src[i:]))
		i += 2
		ml, ni, err := length(i, int(token&15))
		if err != nil {
			return nil, err
		}
		i = ni
		ml += lz4MinMatch
		if offset == 0 || len(out)-offset < window {
			return nil, fmt.Errorf("offset %d outside window", offset)
		}
		if len(out)-start+ml > maxBlock {
			return nil, errors.New("block output too large")
		}
		from := len(out) - offset
		for k := 0; k < ml; k++ {
			out = append(out, out[from+k])
		}
	}
	return out, nil
}

// lz4CheckBlockRules verifies the end-of-block rules of the format on a
// block this package produced: the last sequence holds >= 5 literals and the
// last match starts >= 12 bytes before the end (unless the block is literals
// only and shorter than that).
func lz4CheckBlockRules(t *testing.T, src []byte, plainLen int) {
	t.Helper()
	i, pos, lastMatchStart := 0, 0, -1
	for {
		token := src[i]
		i++
		lit := int(token >> 4)
		if lit == 15 {
			for {
				b := src[i]
				i++
				lit += int(b)
				if b != 255 {
					break
				}
			}
		}
		i += lit
		pos += lit
		if i == len(src) {
			if plainLen >= lz4MFLimit+1 && lit < lz4LastLiterals {
				t.Fatalf("last literals %d < 5", lit)
			}
			break
		}
		i += 2
		ml := int(token & 15)
		if ml == 15 {
			for {
				b := src[i]
				i++
				ml += int(b)
				if b != 255 {
					break
				}
			}
		}
		lastMatchStart = pos
		pos += ml + lz4MinMatch
	}
	if pos != plainLen {
		t.Fatalf("decoded length %d != %d", pos, plainLen)
	}
	if lastMatchStart >= 0 && lastMatchStart > plainLen-lz4MFLimit {
		t.Fatalf("last match at %d of %d", lastMatchStart, plainLen)
	}
}

func TestXXHash32Vectors(t *testing.T) {
	// Reference values from the xxHash test suite / python-xxhash.
	for _, tc := range []struct {
		in   string
		seed uint32
		want uint32
	}{
		{"", 0, 0x02cc5d05},
		{"a", 0, 0x550d7456},
		{"abc", 0, 0x32d153ff},
		{"Nobody inspects the spammish repetition", 0, 0xe2293b2f},
	} {
		if got := xxhash32([]byte(tc.in), tc.seed); got != tc.want {
			t.Errorf("xxhash32(%q, %d) = %08x, want %08x", tc.in, tc.seed, got, tc.want)
		}
	}
}

func TestLZ4FrameHeaderBytes(t *testing.T) {
	var c lz4Compressor
	frame := c.appendLZ4Frame(nil, []byte("hello"))
	// magic, FLG 0x60, BD 0x40, HC = (xxh32(60 40) >> 8) & 0xff = 0x82
	want := []byte{0x04, 0x22, 0x4d, 0x18, 0x60, 0x40, 0x82}
	if !bytes.Equal(frame[:7], want) {
		t.Fatalf("header %x want %x", frame[:7], want)
	}
	// 5 literal bytes do not compress: stored raw, then the end mark
	tail := []byte{5, 0, 0, 0x80, 'h', 'e', 'l', 'l', 'o', 0, 0, 0, 0}
	if !bytes.Equal(frame[7:], tail) {
		t.Fatalf("body %x want %x", frame[7:], tail)
	}
	if empty := c.appendLZ4Frame(nil, nil); !bytes.Equal(empty, append(want, 0, 0, 0, 0)) {
		t.Fatalf("empty frame %x", empty)
	}
}

func lz4Inputs() map[string][]byte {
	r := rand.New(rand.NewSource(7))
	random := func(n int) []byte {
		b := make([]byte, n)
		r.Read(b)
		return b
	}
	json := bytes.Repeat([]byte(`{"traceId":-1234567890123,"service":"/api/v1/orders/{id}","text":"한글 \"quote\""},`), 2000)
	mixed := append(append(random(70000), bytes.Repeat([]byte("abcd"), 40000)...), random(1000)...)
	return map[string][]byte{
		"one":         {1},
		"twelve":      bytes.Repeat([]byte{7}, 12),
		"thirteen":    bytes.Repeat([]byte{7}, 13),
		"zeros64k":    make([]byte, 64<<10),
		"zeros64k+1":  make([]byte, 64<<10+1),
		"zeros300k":   make([]byte, 300<<10),
		"random100k":  random(100 << 10),
		"json":        json,
		"mixed":       mixed,
		"longliteral": append(random(5000), make([]byte, 5000)...),
		"period3":     bytes.Repeat([]byte("xyz"), 30000),
	}
}

func TestLZ4RoundTrip(t *testing.T) {
	var c lz4Compressor
	for name, in := range lz4Inputs() {
		frame := c.appendLZ4Frame([]byte("prefix"), in)
		if string(frame[:6]) != "prefix" {
			t.Fatalf("%s: prefix clobbered", name)
		}
		frame = frame[6:]
		if len(frame) > lz4FrameBound(len(in)) {
			t.Fatalf("%s: frame %d > bound %d", name, len(frame), lz4FrameBound(len(in)))
		}
		got, err := lz4DecodeFrame(frame)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !bytes.Equal(got, in) {
			t.Fatalf("%s: round trip differs (len %d vs %d)", name, len(got), len(in))
		}
		// every compressed block obeys the end-of-block rules
		off := 7
		remaining := len(in)
		for {
			size := binary.LittleEndian.Uint32(frame[off:])
			off += 4
			if size == 0 {
				break
			}
			n := int(size &^ lz4BlockUncompressed)
			plain := min(remaining, lz4BlockSize)
			if size&lz4BlockUncompressed == 0 {
				lz4CheckBlockRules(t, frame[off:off+n], plain)
			} else if n != plain {
				t.Fatalf("%s: raw block %d != %d", name, n, plain)
			}
			remaining -= plain
			off += n
		}
		t.Logf("%s: %d -> %d", name, len(in), len(frame))
	}
	// compressible data actually compresses
	if f := c.appendLZ4Frame(nil, lz4Inputs()["json"]); len(f)*4 > len(lz4Inputs()["json"]) {
		t.Fatalf("json compressed to %d", len(f))
	}
}

func FuzzLZ4RoundTrip(f *testing.F) {
	for _, in := range lz4Inputs() {
		if len(in) <= 70000 {
			f.Add(in)
		}
	}
	f.Add([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"))
	var c lz4Compressor
	f.Fuzz(func(t *testing.T, in []byte) {
		frame := c.appendLZ4Frame(nil, in)
		if len(frame) > lz4FrameBound(len(in)) {
			t.Fatalf("frame %d > bound %d", len(frame), lz4FrameBound(len(in)))
		}
		got, err := lz4DecodeFrame(frame)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, in) {
			t.Fatal("round trip differs")
		}
	})
}

// A compressed batch keeps the plain header fields, sets codec 3, carries the
// CRC over the compressed bytes and decodes to the same records; an
// incompressible batch falls back to codec 0 and is never larger than
// encodedSize().
func TestRecordBatchLZ4(t *testing.T) {
	var c lz4Compressor
	b := newBatch(topicPartition{"t", 0}, time.UnixMilli(1000))
	for i := 0; i < 50; i++ {
		rec := Record{Topic: "t", Key: []byte{0, 0, 0, byte(i)}, Value: []byte(fmt.Sprintf(`{"traceId":%d,"text":"repeated text repeated text"}`, i)), Headers: []Header{{Key: "apm_type", Value: []byte("SCOUTER")}}}
		b.appendRecord(encodeRecord(nil, rec, int64(i), int32(i)), 1000+int64(i))
	}
	plain := b.appendEncoded(nil, nil)
	packed := b.appendEncoded(nil, &c)
	if len(packed) >= len(plain) {
		t.Fatalf("lz4 batch %d not smaller than %d", len(packed), len(plain))
	}
	if !bytes.Equal(packed[:8], plain[:8]) || !bytes.Equal(packed[12:17], plain[12:17]) || !bytes.Equal(packed[23:batchHeaderLen], plain[23:batchHeaderLen]) {
		t.Fatal("header fields other than length/crc/attributes changed")
	}
	metaPlain, recsPlain, err := decodeBatch(plain)
	if err != nil {
		t.Fatal(err)
	}
	metaPacked, recsPacked, err := decodeBatch(packed)
	if err != nil {
		t.Fatal(err)
	}
	if metaPlain.attributes != 0 || metaPacked.attributes != batchCodecLZ4 {
		t.Fatalf("attributes %d/%d", metaPlain.attributes, metaPacked.attributes)
	}
	if fmt.Sprint(recsPlain) != fmt.Sprint(recsPacked) || len(recsPacked) != 50 {
		t.Fatal("records differ after decompression")
	}

	r := rand.New(rand.NewSource(3))
	noise := make([]byte, 4000)
	r.Read(noise)
	nb := newBatch(topicPartition{"t", 0}, time.UnixMilli(1000))
	nb.appendRecord(encodeRecord(nil, Record{Topic: "t", Value: noise}, 0, 0), 1000)
	got := nb.appendEncoded(nil, &c)
	if !bytes.Equal(got, nb.appendEncoded(nil, nil)) || len(got) != nb.encodedSize() {
		t.Fatal("incompressible batch must be sent uncompressed")
	}
}

// lz4FrameBound is the largest frame appendLZ4Frame can produce for n input
// bytes: header, one 4-byte size per block (a block is never larger than its
// input), the end mark.
func lz4FrameBound(n int) int {
	blocks := (n + lz4BlockSize - 1) / lz4BlockSize
	return lz4FrameHeaderLen + blocks*4 + n + 4
}
