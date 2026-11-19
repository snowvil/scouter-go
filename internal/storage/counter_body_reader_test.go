package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// counterBodyReader must answer what counterCatalogBody answers for the same
// file, position and byte budget: the same body or the same error class.
func TestCounterBodyReaderMatchesCatalogBody(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var file []byte
	var starts []int64
	put := func(n int) {
		starts = append(starts, int64(len(file)))
		var head [4]byte
		binary.BigEndian.PutUint32(head[:], uint32(n))
		file = append(file, head[:]...)
		body := make([]byte, n)
		rng.Read(body)
		file = append(file, body...)
	}
	for len(file) < 3*counterBodyBlock { // several block ends fall inside records
		put(rng.Intn(400))
	}
	put(0)
	put(counterBodyBlock - 4) // exactly one block
	put(counterBodyBlock - 3) // one byte more than a block: its own read
	put(2 * counterBodyBlock)
	put(7)
	// Corrupt prefixes: negative length, length past the file end.
	negative := int64(len(file))
	file = append(file, 0xff, 0xff, 0xff, 0xff, 1, 2, 3)
	beyond := int64(len(file))
	file = append(file, 0, 0, 0x10, 0, 9)
	path := filepath.Join(t.TempDir(), "real.data")
	if err := os.WriteFile(path, file, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ctx := context.Background()
	size := int64(len(file))
	positions := append([]int64{}, starts...)
	positions = append(positions, negative, beyond, -1, size-4, size-3, size, size+10, 1, 5, starts[3]+1)
	same := func(r *counterBodyReader, pos int64, remaining int) {
		t.Helper()
		want, wantErr := counterCatalogBody(ctx, f, pos, remaining)
		got, gotErr := r.body(ctx, pos, remaining)
		for _, kind := range []error{ErrCorrupt, ErrLimit} {
			if errors.Is(wantErr, kind) != errors.Is(gotErr, kind) {
				t.Fatalf("pos %d remaining %d: error %v, want %v", pos, remaining, gotErr, wantErr)
			}
		}
		if (wantErr == nil) != (gotErr == nil) || !bytes.Equal(got, want) {
			t.Fatalf("pos %d remaining %d: %d bytes %v, want %d bytes %v", pos, remaining, len(got), gotErr, len(want), wantErr)
		}
		if gotErr == nil && cap(got) != len(got) {
			t.Fatalf("pos %d: a body can be appended into its block (len %d cap %d)", pos, len(got), cap(got))
		}
	}
	// In scan order with one reader, as fill uses it.
	sequential := counterBodyReader{f: f}
	var kept [][]byte
	for _, pos := range starts {
		same(&sequential, pos, counterCatalogBytes)
		body, _ := sequential.body(ctx, pos, counterCatalogBytes)
		kept = append(kept, body)
	}
	// Earlier bodies are not overwritten by later block reads.
	for n, pos := range starts {
		want, _ := counterCatalogBody(ctx, f, pos, counterCatalogBytes)
		if !bytes.Equal(kept[n], want) {
			t.Fatalf("body at %d changed after later reads", pos)
		}
	}
	// Any order, any budget, bad positions.
	random := counterBodyReader{f: f}
	for n := 0; n < 4000; n++ {
		pos := positions[rng.Intn(len(positions))]
		remaining := []int{counterCatalogBytes, 0, 3, 4, 5, 100, 404, counterBodyBlock, counterBodyBlock + 4}[rng.Intn(9)]
		same(&random, pos, remaining)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := (&counterBodyReader{f: f}).body(cancelled, starts[0], counterCatalogBytes); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled read", err)
	}
}
