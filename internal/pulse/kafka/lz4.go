package kafka

import (
	"encoding/binary"
	"math/bits"
)

// LZ4 frame writer matching the Java producer's compression.type=lz4
// (kafka-clients 4.0.0 org.apache.kafka.common.compress.Lz4BlockOutputStream
// with its defaults: 64KiB blocks, level 9 = lz4-java fastCompressor, no block
// checksum, no content size, no content checksum, independent blocks).
//
//	magic 04 22 4D 18 | FLG 0x60 | BD 0x40 | HC
//	{ blockSize uint32 LE (bit 31 = stored uncompressed) | block bytes }*
//	end mark 00 00 00 00
//
// FLG 0x60 is version 01 + block independence; BD 0x40 is max block size 64KiB.
// HC is (xxHash32(FLG BD, seed 0) >> 8) & 0xFF. Magic-2 record batches use the
// correct checksum; the "broken" variant that also hashes the magic bytes only
// applies to the legacy magic 0 message format, which this package never
// writes. Like the Java writer, a block whose compressed form is not smaller
// than its input is stored raw with bit 31 set.
//
// The compressed bytes are not byte-identical to lz4-java (a different match
// finder is allowed by the LZ4 format); what is fixed is that any LZ4 frame
// reader, including the Java consumer, restores the exact record bytes.

const (
	lz4Magic             = 0x184D2204
	lz4FLG               = 0x60
	lz4BD                = 0x40
	lz4BlockSize         = 64 << 10
	lz4FrameHeaderLen    = 7
	lz4BlockUncompressed = 1 << 31

	lz4MinMatch     = 4
	lz4LastLiterals = 5  // the last 5 bytes of a block are always literals
	lz4MFLimit      = 12 // the last match starts at least 12 bytes before the end
	lz4MaxOffset    = 65535
	lz4HashLog      = 12
)

// lz4HeaderChecksum is the frame descriptor checksum byte for FLG/BD above.
var lz4HeaderChecksum = byte(xxhash32([]byte{lz4FLG, lz4BD}, 0) >> 8)

// lz4Compressor holds the match-finder hash table (16KiB). It is not safe for
// concurrent use; each broker connection owns one.
type lz4Compressor struct {
	table [1 << lz4HashLog]uint32 // position+1 within the current block, 0 = empty
}

// appendLZ4Frame appends src as one LZ4 frame to dst.
func (c *lz4Compressor) appendLZ4Frame(dst, src []byte) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, lz4Magic)
	dst = append(dst, lz4FLG, lz4BD, lz4HeaderChecksum)
	for len(src) > 0 {
		n := min(len(src), lz4BlockSize)
		block := src[:n]
		src = src[n:]
		sizeAt := len(dst)
		dst = append(dst, 0, 0, 0, 0)
		dst = c.compressBlock(dst, block)
		if size := len(dst) - sizeAt - 4; size < n {
			binary.LittleEndian.PutUint32(dst[sizeAt:], uint32(size))
			continue
		}
		dst = append(dst[:sizeAt+4], block...)
		binary.LittleEndian.PutUint32(dst[sizeAt:], uint32(n)|lz4BlockUncompressed)
	}
	return binary.LittleEndian.AppendUint32(dst, 0)
}

func lz4Hash(v uint32) uint32 { return (v * 2654435761) >> (32 - lz4HashLog) }

// compressBlock appends one independent LZ4 block (sequences of token,
// literals, little-endian offset, match length) for src (len <= 64KiB). The
// encoder is greedy with a single-entry hash table, like LZ4's fast mode.
func (c *lz4Compressor) compressBlock(dst, src []byte) []byte {
	n := len(src)
	anchor := 0
	if n > lz4MFLimit {
		clear(c.table[:])
		matchLimit := n - lz4LastLiterals
		last := n - lz4MFLimit // a match may start at most here
		i := 0
		misses := 0
		for i <= last {
			seq := binary.LittleEndian.Uint32(src[i:])
			h := lz4Hash(seq)
			ref := int(c.table[h]) - 1
			c.table[h] = uint32(i + 1)
			if ref < 0 || i-ref > lz4MaxOffset || binary.LittleEndian.Uint32(src[ref:]) != seq {
				// LZ4-style acceleration: skip faster through incompressible data.
				i += 1 + misses>>6
				misses++
				continue
			}
			misses = 0
			// extend backwards over pending literals
			for i > anchor && ref > 0 && src[i-1] == src[ref-1] {
				i--
				ref--
			}
			end := i + lz4MinMatch
			for r := ref + lz4MinMatch; end < matchLimit && src[end] == src[r]; r++ {
				end++
			}
			dst = lz4AppendSequence(dst, src[anchor:i], i-ref, end-i)
			// index a position inside the match to help the next search
			if end-2 > i && end-2 <= last {
				c.table[lz4Hash(binary.LittleEndian.Uint32(src[end-2:]))] = uint32(end - 2 + 1)
			}
			i = end
			anchor = end
		}
	}
	return lz4AppendLastLiterals(dst, src[anchor:])
}

func lz4AppendLength(dst []byte, n int) []byte {
	for n >= 255 {
		dst = append(dst, 255)
		n -= 255
	}
	return append(dst, byte(n))
}

func lz4AppendSequence(dst, literals []byte, offset, matchLen int) []byte {
	lit := len(literals)
	ml := matchLen - lz4MinMatch
	token := byte(min(lit, 15))<<4 | byte(min(ml, 15))
	dst = append(dst, token)
	if lit >= 15 {
		dst = lz4AppendLength(dst, lit-15)
	}
	dst = append(dst, literals...)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(offset))
	if ml >= 15 {
		dst = lz4AppendLength(dst, ml-15)
	}
	return dst
}

func lz4AppendLastLiterals(dst, literals []byte) []byte {
	lit := len(literals)
	dst = append(dst, byte(min(lit, 15))<<4)
	if lit >= 15 {
		dst = lz4AppendLength(dst, lit-15)
	}
	return append(dst, literals...)
}

// xxHash32 (https://github.com/Cyan4973/xxHash, XXH32), used only for the
// frame descriptor checksum.
const (
	xxPrime1 uint32 = 2654435761
	xxPrime2 uint32 = 2246822519
	xxPrime3 uint32 = 3266489917
	xxPrime4 uint32 = 668265263
	xxPrime5 uint32 = 374761393
)

func xxRound(acc, input uint32) uint32 {
	acc += input * xxPrime2
	acc = bits.RotateLeft32(acc, 13)
	return acc * xxPrime1
}

func xxhash32(b []byte, seed uint32) uint32 {
	n := len(b)
	var h uint32
	if n >= 16 {
		v1 := seed + xxPrime1 + xxPrime2
		v2 := seed + xxPrime2
		v3 := seed
		v4 := seed - xxPrime1
		for len(b) >= 16 {
			v1 = xxRound(v1, binary.LittleEndian.Uint32(b[0:]))
			v2 = xxRound(v2, binary.LittleEndian.Uint32(b[4:]))
			v3 = xxRound(v3, binary.LittleEndian.Uint32(b[8:]))
			v4 = xxRound(v4, binary.LittleEndian.Uint32(b[12:]))
			b = b[16:]
		}
		h = bits.RotateLeft32(v1, 1) + bits.RotateLeft32(v2, 7) + bits.RotateLeft32(v3, 12) + bits.RotateLeft32(v4, 18)
	} else {
		h = seed + xxPrime5
	}
	h += uint32(n)
	for len(b) >= 4 {
		h += binary.LittleEndian.Uint32(b) * xxPrime3
		h = bits.RotateLeft32(h, 17) * xxPrime4
		b = b[4:]
	}
	for _, x := range b {
		h += uint32(x) * xxPrime5
		h = bits.RotateLeft32(h, 11) * xxPrime1
	}
	h ^= h >> 15
	h *= xxPrime2
	h ^= h >> 13
	h *= xxPrime3
	h ^= h >> 16
	return h
}
