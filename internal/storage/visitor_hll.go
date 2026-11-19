// Scouter/stream-lib HLL contract: Copyright Clearspring/Scouter authors, Apache-2.0.
package storage

import (
	"encoding/binary"
	"math"
	"math/bits"
	"slices"
)

const visitorPrecision = 20
const visitorRegisters = 1 << visitorPrecision
const visitorWords = visitorRegisters/6 + 1
const visitorFileBytes = 8 + visitorWords*4

// visitorPageBytes is the unit of an in-place snapshot rewrite.
const visitorPageBytes = 4096
const visitorPages = (visitorFileBytes + visitorPageBytes - 1) / visitorPageBytes

// visitorSparseMax is the number of nonzero register words an HLL keeps in a
// map before it switches to the dense array (~700KiB). Most per-object and
// hourly HLLs see few visitors, so they stay a few KiB; the file image and the
// estimate do not depend on the representation.
const visitorSparseMax = 8192

// visitorHLL keeps, besides the registers, which file pages changed since
// takePages: an unchanged page need not be rewritten. Registers are packed six
// per 32-bit word as in the Java RegisterSet; words is the dense array, or nil
// while sparse holds the nonzero words.
type visitorHLL struct {
	words  []uint32
	sparse map[uint32]uint32
	pages  [(visitorPages + 63) / 64]uint64
}

func newVisitorHLL() *visitorHLL { return &visitorHLL{sparse: map[uint32]uint32{}} }

func (h *visitorHLL) word(bucket uint32) uint32 {
	if h.words != nil {
		return h.words[bucket]
	}
	return h.sparse[bucket]
}
func (h *visitorHLL) setWord(bucket, w uint32) {
	if h.words == nil {
		if _, ok := h.sparse[bucket]; !ok && len(h.sparse) >= visitorSparseMax {
			h.densify()
		} else if h.sparse == nil {
			h.sparse = map[uint32]uint32{}
		}
	}
	if h.words != nil {
		h.words[bucket] = w
	} else {
		h.sparse[bucket] = w
	}
	page := (8 + int(bucket)*4) / visitorPageBytes
	h.pages[page/64] |= 1 << (page % 64)
}
func (h *visitorHLL) densify() {
	h.words = make([]uint32, visitorWords)
	for b, w := range h.sparse {
		h.words[b] = w
	}
	h.sparse = nil
}

// dense returns the register words, building a temporary array for a sparse HLL.
func (h *visitorHLL) dense() []uint32 {
	if h.words != nil {
		return h.words
	}
	words := make([]uint32, visitorWords)
	for b, w := range h.sparse {
		words[b] = w
	}
	return words
}

func visitorHashLong(v int64) uint32 {
	const m uint32 = 0x5bd1e995
	k := uint32(v) * m
	k ^= k >> 24
	h := k * m
	k = uint32(uint64(v)>>32) * m
	k ^= k >> 24
	h = h*m ^ k*m
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

// offer reports whether a register changed (RegisterSet.updateIfGreater).
func (h *visitorHLL) offer(v int64) bool {
	x := visitorHashLong(v)
	j := x >> (32 - visitorPrecision)
	r := uint32(bits.LeadingZeros32(x<<visitorPrecision|(1<<(visitorPrecision-1)+1)) + 1)
	bucket, shift := j/6, (j%6)*5
	w := h.word(bucket)
	if old := w >> shift & 31; old < r {
		h.setWord(bucket, w & ^(uint32(31)<<shift) | r<<shift)
		return true
	}
	return false
}
func (h *visitorHLL) cardinality() int64 {
	// Summed in register order over the dense words, as Java does, so a
	// sparse HLL gives the same float result.
	words := h.dense()
	sum, zeros := 0.0, 0.0
	for j := 0; j < visitorRegisters; j++ {
		v := words[j/6] >> uint(j%6*5) & 31
		sum += 1 / float64(uint32(1)<<v)
		if v == 0 {
			zeros++
		}
	}
	m := float64(visitorRegisters)
	estimate := (0.7213 / (1 + 1.079/m)) * m * m / sum
	if estimate <= 2.5*m {
		estimate = m * math.Log(m/zeros)
	}
	return int64(math.Floor(estimate + 0.5))
}

// visitorMaxWord is the register-wise maximum of two packed words.
func visitorMaxWord(a, b uint32) uint32 {
	var word uint32
	for j := 0; j < 6; j++ {
		mask := uint32(31) << uint(j*5)
		x, y := a&mask, b&mask
		if y > x {
			x = y
		}
		word |= x
	}
	return word
}
func (h *visitorHLL) merge(other *visitorHLL) {
	h.raise(other)
	for i := range h.pages {
		h.pages[i] = ^uint64(0)
	}
}

// raise merges other's registers into h, marking only the pages of changed
// words, and reports whether any changed.
func (h *visitorHLL) raise(other *visitorHLL) bool {
	changed := false
	up := func(bucket, ow uint32) {
		hw := h.word(bucket)
		if word := visitorMaxWord(hw, ow); word != hw {
			h.setWord(bucket, word)
			changed = true
		}
	}
	if other.words == nil {
		for bucket, w := range other.sparse {
			up(bucket, w)
		}
	} else {
		for bucket, w := range other.words {
			if w != 0 {
				up(uint32(bucket), w)
			}
		}
	}
	return changed
}

// buckets returns the indexes of the nonzero words in ascending order.
func (h *visitorHLL) buckets() []uint32 {
	if h.words == nil {
		out := make([]uint32, 0, len(h.sparse))
		for b := range h.sparse {
			out = append(out, b)
		}
		slices.Sort(out)
		return out
	}
	var out []uint32
	for b, w := range h.words {
		if w != 0 {
			out = append(out, uint32(b))
		}
	}
	return out
}

// visitorSparseWordBytes is the heap a sparse word takes in the map (measured
// 13-19 bytes with Go 1.27 maps); visitorHLLBytes is the fixed part.
const (
	visitorSparseWordBytes = 20
	visitorHLLBytes        = 64
	visitorDenseBytes      = visitorHLLBytes + visitorWords*4
)

// memBytes estimates the heap the registers take, for the visitor budgets.
func (h *visitorHLL) memBytes() int64 {
	if h.words != nil {
		return visitorDenseBytes
	}
	return visitorHLLBytes + int64(len(h.sparse))*visitorSparseWordBytes
}

// markedWords returns the nonzero words of the pages changed since the last
// takePages, as a new HLL.
func (h *visitorHLL) markedWords() *visitorHLL {
	d := &visitorHLL{}
	marked := func(page int) bool { return h.pages[page/64]&(1<<(page%64)) != 0 }
	if h.words == nil {
		for b, w := range h.sparse {
			if marked((8 + int(b)*4) / visitorPageBytes) {
				d.setWord(b, w)
			}
		}
		return d
	}
	for page := 0; page < visitorPages; page++ {
		if !marked(page) {
			continue
		}
		// Words are 4-byte aligned after the 8-byte header, so none straddles a page.
		lo, hi := max(page*visitorPageBytes-8, 0)/4, min((page+1)*visitorPageBytes-8, visitorWords*4)/4
		for b := lo; b < hi; b++ {
			if w := h.words[b]; w != 0 {
				d.setWord(uint32(b), w)
			}
		}
	}
	return d
}

// takePages returns the pages changed since the last call and clears them.
func (h *visitorHLL) takePages() [(visitorPages + 63) / 64]uint64 {
	p := h.pages
	h.pages = [len(h.pages)]uint64{}
	return p
}

// encode writes the file image into dst's storage when it is large enough
// (the 10s flush reuses one buffer instead of allocating ~700KiB per entry).
func (h *visitorHLL) encode(dst []byte) []byte {
	b := dst[:0]
	if cap(b) < visitorFileBytes {
		b = make([]byte, visitorFileBytes)
	}
	b = b[:visitorFileBytes]
	binary.BigEndian.PutUint32(b, visitorPrecision)
	binary.BigEndian.PutUint32(b[4:], visitorWords)
	if h.words == nil {
		clear(b[8:])
		for i, w := range h.sparse {
			binary.BigEndian.PutUint32(b[8+i*4:], w)
		}
		return b
	}
	for i, w := range h.words {
		binary.BigEndian.PutUint32(b[8+i*4:], w)
	}
	return b
}
func decodeVisitorHLL(b []byte) (*visitorHLL, error) {
	if len(b) != visitorFileBytes || binary.BigEndian.Uint32(b) != visitorPrecision || binary.BigEndian.Uint32(b[4:]) != visitorWords {
		return nil, ErrCorrupt
	}
	h := newVisitorHLL()
	for i := 0; i < visitorWords; i++ {
		w := binary.BigEndian.Uint32(b[8+i*4:])
		if w>>30 != 0 {
			return nil, ErrCorrupt
		}
		if w != 0 {
			if h.words == nil && len(h.sparse) >= visitorSparseMax {
				h.densify()
			}
			if h.words != nil {
				h.words[i] = w
			} else {
				h.sparse[uint32(i)] = w
			}
		}
	}
	return h, nil
}
