package protocol

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type shortReader struct{ io.Reader }

func (r shortReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.Reader.Read(p)
}

func TestReadPackStreamBoundaries(t *testing.T) {
	for _, name := range []string{"map", "object", "counter", "textpack", "xlog", "profile", "profile2"} {
		t.Run(name, func(t *testing.T) {
			raw := fixture(t, name)
			r := bytes.NewReader(append(append([]byte{}, raw...), 0xa5))
			p, err := ReadPack(shortReader{r}, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := EncodePack(p, Limits{})
			if err != nil || !bytes.Equal(got, raw) {
				t.Fatalf("round trip: %x %v", got, err)
			}
			if r.Len() != 1 {
				t.Fatalf("consumed next frame: %d", r.Len())
			}
			for i := 0; i < len(raw); i++ {
				p, err := ReadPack(bytes.NewReader(raw[:i]), Limits{})
				if p != nil || !errors.Is(err, ErrTruncated) {
					t.Fatalf("prefix %d: %v", i, err)
				}
			}
		})
	}
}

func TestReadValueStreamBoundaries(t *testing.T) {
	raw := fixture(t, "values")
	r := bytes.NewReader(append(append([]byte{}, raw...), 0xa5))
	v, err := ReadValue(shortReader{r}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := EncodeValue(v, Limits{})
	if err != nil || !bytes.Equal(got, raw) || r.Len() != 1 {
		t.Fatalf("boundary/round trip: %v remaining=%d", err, r.Len())
	}
}

func TestReadStreamLimitsAndErrors(t *testing.T) {
	raw := fixture(t, "xlog")
	if _, err := ReadPack(bytes.NewReader(raw), Limits{MaxBytes: len(raw) - 1}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := ReadPack(nil, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := ReadValue(nil, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := ReadPack(bytes.NewReader(nil), Limits{}); !errors.Is(err, io.EOF) || !errors.Is(err, ErrTruncated) {
		t.Fatal(err)
	}
	if _, err := ReadPack(bytes.NewReader([]byte{PackText, 3, 'x'}), Limits{}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	// A complete collection beyond the configured byte budget is a limit error,
	// even when the budget is exhausted before its items are read.
	if _, err := ReadPack(bytes.NewReader([]byte{PackMap, 1, 1, 0, ValueNull}), Limits{MaxBytes: 4}); !errors.Is(err, ErrLimit) || errors.Is(err, ErrTruncated) {
		t.Fatal("collection budget classified as truncated input", err)
	}
}

func FuzzReadPack(f *testing.F) {
	for _, name := range []string{"map", "object", "counter", "textpack", "xlog", "profile", "profile2"} {
		f.Add(fixture(f, name))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		limits := Limits{MaxBytes: 1 << 16, MaxBlobBytes: 1 << 15, MaxCollectionEntries: 128, MaxTotalValues: 512, MaxDepth: 8}
		if len(raw) > limits.MaxBytes {
			return
		}
		d := NewDecoder(raw, limits)
		want := d.Pack()
		r := bytes.NewReader(raw)
		got, err := ReadPack(r, limits)
		if (err == nil) != (d.Err() == nil) {
			t.Fatalf("stream/slice result differs: %v / %v", err, d.Err())
		}
		if err != nil {
			return
		}
		if len(raw)-r.Len() != d.Offset() {
			t.Fatal("stream consumed a different object boundary")
		}
		wantBytes, wantErr := EncodePack(want, limits)
		gotBytes, gotErr := EncodePack(got, limits)
		if wantErr != nil || gotErr != nil || !bytes.Equal(wantBytes, gotBytes) {
			t.Fatalf("stream/slice result mismatch: %v / %v", wantErr, gotErr)
		}
	})
}
