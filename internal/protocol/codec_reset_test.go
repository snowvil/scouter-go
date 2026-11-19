package protocol

import (
	"bytes"
	"testing"
)

// Reset reuses the buffer and yields the bytes a fresh encoder would.
func TestEncoderReset(t *testing.T) {
	e := NewEncoder(Limits{})
	e.Int64(7)
	e.Value(Value{Type: ValueText, Text: "long enough text to grow the buffer"})
	if _, err := e.Bytes(); err != nil {
		t.Fatal(err)
	}
	e.Reset()
	e.Int64(-1)
	e.Value(Value{Type: ValueTextHash, Int: 42})
	got, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	f := NewEncoder(Limits{})
	f.Int64(-1)
	f.Value(Value{Type: ValueTextHash, Int: 42})
	want, _ := f.Bytes()
	if !bytes.Equal(got, want) {
		t.Fatalf("after Reset %x, fresh %x", got, want)
	}
}
