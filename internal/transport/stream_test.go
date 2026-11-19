package transport

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func TestStreamPrimitivesBoundsAndPacketBoundary(t *testing.T) {
	data := fixture(t, "tcp-agent-response-v1")
	stream := newStream(bytes.NewReader(data), io.Discard, TCPConfig{MaxFrameBytes: 128})
	flag, err := stream.ReadByte()
	if err != nil || flag != HasNext {
		t.Fatal(flag, err)
	}
	pack, err := stream.ReadPack()
	if err != nil || mapInt(t, pack) != -7 {
		t.Fatal(pack, err)
	}
	flag, err = stream.ReadByte()
	if err != nil || flag != NoNext {
		t.Fatal("Pack overread", flag, err)
	}
	for _, data := range [][]byte{{254, 255, 255, 255, 255}, {255, 255, 255}, {254, 0, 0, 1, 0}} {
		stream = newStream(bytes.NewReader(data), io.Discard, TCPConfig{MaxFrameBytes: 16})
		if _, err := stream.ReadBlob(); !errors.Is(err, ErrMalformed) && !errors.Is(err, ErrLimit) {
			t.Fatal("invalid Blob allocation", err)
		}
	}
	stream = newStream(bytes.NewReader([]byte{255, 0}), io.Discard, TCPConfig{})
	if _, err := stream.ReadText(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("partial length", err)
	}
	stream = newStream(bytes.NewReader([]byte{1, 255}), io.Discard, TCPConfig{})
	if _, err := stream.ReadText(); !errors.Is(err, ErrMalformed) {
		t.Fatal("invalid UTF8", err)
	}
	stream = newStream(bytes.NewReader([]byte{protocol.PackMap, 0}), io.Discard, TCPConfig{MaxFrameBytes: 1})
	if _, err := stream.ReadPack(); !errors.Is(err, ErrLimit) {
		t.Fatal("frame bound", err)
	}
	stream = newStream(bytes.NewReader(nil), shortWriter{}, TCPConfig{})
	if _, err := stream.Write([]byte{1, 2}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
}

type shortWriter struct{}

func (shortWriter) Write(data []byte) (int, error) { return len(data) / 2, nil }

func TestStreamCodecLimitUsesRemainingBudget(t *testing.T) {
	s := newStream(bytes.NewReader(nil), io.Discard, TCPConfig{MaxFrameBytes: 128, CodecLimits: protocol.Limits{MaxBytes: 1024, MaxBlobBytes: 512}})
	s.readBytes = 127
	limits, err := s.readLimits()
	if err != nil || limits.MaxBytes != 1 || limits.MaxBlobBytes != 1 {
		t.Fatal(limits, err)
	}
	s.readBytes = 128
	if _, err = s.ReadPack(); !errors.Is(err, ErrLimit) {
		t.Fatal("zero remaining became default", err)
	}
	if _, err = s.ReadValue(); !errors.Is(err, ErrLimit) {
		t.Fatal("zero remaining Value", err)
	}
}
