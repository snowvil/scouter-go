package logger

import (
	"math"
	"strconv"
	"unicode/utf16"
)

// javaHexa32 is scouter.util.Hexa32.toString32: 0..9 in decimal, other
// non-negative values as 'x' + base 32, negative values as 'z' + base 32 of
// the magnitude, Long.MIN_VALUE as the fixed "z8000000000000".
func javaHexa32(n int64) string {
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

// javaStringHash is java.lang.String.hashCode over UTF-16 code units.
func javaStringHash(s string) int32 {
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

// hashtableOrder returns the iteration order (indexes into keys) of a JDK 8
// java.util.Hashtable filled by put(keys[i]) in order: capacity 11, load
// factor 0.75, head insertion, rehash to 2n+1 when count >= threshold before
// an insert (walking old buckets from the top, each chain from its head), and
// entrySet iteration from the highest bucket down. A repeated key keeps its
// first position (put replaces the value in place).
func hashtableOrder(keys []string) []int {
	type node struct {
		idx  int
		hash int32
	}
	capacity := 11
	threshold := int(float32(capacity) * 0.75)
	table := make([][]node, capacity)
	count := 0
	bucket := func(h int32, c int) int { return int(uint32(h)&0x7fffffff) % c }
	var seen map[string]struct{}
	if len(keys) > 1 {
		seen = make(map[string]struct{}, len(keys))
	}
	for i, k := range keys {
		if seen != nil {
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
		}
		h := javaStringHash(k)
		if count >= threshold {
			nc := capacity*2 + 1
			nt := make([][]node, nc)
			for b := capacity - 1; b >= 0; b-- {
				for _, n := range table[b] {
					at := bucket(n.hash, nc)
					nt[at] = append([]node{n}, nt[at]...)
				}
			}
			table, capacity, threshold = nt, nc, int(float32(nc)*0.75)
		}
		at := bucket(h, capacity)
		table[at] = append([]node{{i, h}}, table[at]...)
		count++
	}
	out := make([]int, 0, count)
	for b := capacity - 1; b >= 0; b-- {
		for _, n := range table[b] {
			out = append(out, n.idx)
		}
	}
	return out
}

// javaIP is scouter.util.IPUtil.toString(byte[]): the first four bytes as a
// dotted quad, or "0.0.0.0" when fewer than four bytes are present.
func javaIP(ip []byte) string {
	if len(ip) < 4 {
		return "0.0.0.0"
	}
	return strconv.Itoa(int(ip[0])) + "." + strconv.Itoa(int(ip[1])) + "." + strconv.Itoa(int(ip[2])) + "." + strconv.Itoa(int(ip[3]))
}
