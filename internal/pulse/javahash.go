// Package pulse embeds the Pulse Scouter plugin contract (source pinned by
// digest in docs/compatibility/baseline.json) as Go code: XLog/Profile conversion, filters, JSON/Step encoding and
// a bounded dispatcher. It never waits for a Kafka response on the ingest path.
package pulse

import (
	"hash/crc32"
	"unicode/utf16"
)

// JavaStringHashCode reproduces java.lang.String.hashCode: h = 31*h + c over
// UTF-16 code units with int32 wraparound. Runes above U+FFFF contribute both
// surrogates. The plugin Kafka dispatcher uses this for the partition.
func JavaStringHashCode(s string) int32 {
	var h int32
	for _, r := range s {
		if r >= 0x10000 {
			hi, lo := utf16.EncodeRune(r)
			h = 31*h + int32(hi)
			h = 31*h + int32(lo)
			continue
		}
		h = 31*h + int32(r)
	}
	return h
}

// FloorMod is java.lang.Math.floorMod(int, int); the result has the sign of n.
// n must be nonzero (Java throws ArithmeticException; the Config rejects 0).
func FloorMod(x int32, n int32) int32 {
	m := x % n
	if m != 0 && (m^n) < 0 {
		m += n
	}
	return m
}

// Partition is Math.floorMod(application.hashCode(), partitionCount).
func Partition(application string, partitionCount int32) int32 {
	return FloorMod(JavaStringHashCode(application), partitionCount)
}

// TransactionHash is scouter.util.HashUtil.hash(String): CRC-32 (IEEE table,
// init 0xffffffff, final xor) over str.getBytes() in the JVM default charset,
// which this port assumes to be UTF-8. HashUtil.java:56-66.
func TransactionHash(s string) int32 {
	return int32(crc32.ChecksumIEEE([]byte(s)))
}
