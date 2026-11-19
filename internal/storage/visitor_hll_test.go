package storage

import (
	"bytes"
	"testing"
)

func TestVisitorHLLDuplicateMergeAndCorrupt(t *testing.T) {
	h := newVisitorHLL()
	for _, id := range []int64{1, 1, -1, 2, 2} {
		h.offer(id)
	}
	if got := h.cardinality(); got != 3 {
		t.Fatalf("unique users=%d, want 3", got)
	}
	b := h.bytes()
	if len(b) != 699060 {
		t.Fatalf("HLL file length=%d", len(b))
	}
	restored, err := decodeVisitorHLL(b)
	if err != nil || restored.cardinality() != 3 || !bytes.Equal(restored.bytes(), b) {
		t.Fatal("restore", err)
	}
	other := newVisitorHLL()
	other.offer(2)
	other.offer(3)
	restored.merge(other)
	if restored.cardinality() != 4 {
		t.Fatal("merge must deduplicate")
	}
	for _, bad := range [][]byte{nil, b[:7], append(append([]byte(nil), b...), 0), append([]byte{0, 0, 0, 30}, b[4:]...)} {
		if _, err := decodeVisitorHLL(bad); err == nil {
			t.Fatal("corrupt accepted")
		}
	}
}

func TestVisitorHLLOfferReportsChangeAndEncodeReuse(t *testing.T) {
	h := newVisitorHLL()
	if !h.offer(7) || h.offer(7) {
		t.Fatal("first offer must change a register, a repeat must not")
	}
	buf := make([]byte, 3)
	if got := h.encode(buf); !bytes.Equal(got, h.bytes()) {
		t.Fatal("encode into a short buffer")
	}
	big := make([]byte, visitorFileBytes)
	for i := range big {
		big[i] = 0xff
	}
	got := h.encode(big)
	if &got[0] != &big[0] || !bytes.Equal(got, h.bytes()) {
		t.Fatal("encode must reuse a large enough buffer and overwrite it fully")
	}
}

// A sparse HLL and a dense one fed the same visitors have the same file image,
// estimate and merge result, below and past the promotion threshold.
func TestVisitorHLLSparseMatchesDense(t *testing.T) {
	for _, n := range []int{0, 1, 500, 20000, 200000} {
		sparse, dense := newVisitorHLL(), newVisitorHLL()
		dense.densify()
		for i := 0; i < n; i++ {
			v := int64(i)*7919 - 3
			if sparse.offer(v) != dense.offer(v) {
				t.Fatal(n, i, "offer result differs")
			}
		}
		if n <= 500 && sparse.words != nil {
			t.Fatal(n, "promoted too early")
		}
		if n >= 200000 && sparse.words == nil {
			t.Fatal(n, "never promoted")
		}
		if sparse.takePages() != dense.takePages() {
			t.Fatal(n, "changed pages differ")
		}
		if !bytes.Equal(sparse.bytes(), dense.bytes()) || sparse.cardinality() != dense.cardinality() {
			t.Fatal(n, "image or estimate differs", sparse.cardinality(), dense.cardinality())
		}
		restored, err := decodeVisitorHLL(sparse.bytes())
		if err != nil || !bytes.Equal(restored.bytes(), dense.bytes()) || (n <= 500) != (restored.words == nil) {
			t.Fatal(n, "decode", err)
		}
		other := newVisitorHLL()
		for i := 0; i < 300; i++ {
			other.offer(int64(i) * 104729)
		}
		a, b := newVisitorHLL(), newVisitorHLL()
		b.densify()
		a.merge(sparse)
		a.merge(other)
		b.merge(dense)
		b.merge(other)
		if !bytes.Equal(a.bytes(), b.bytes()) || a.cardinality() != b.cardinality() {
			t.Fatal(n, "merge differs")
		}
	}
}

func (h *visitorHLL) bytes() []byte { return h.encode(nil) }
