package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/snowvil/scouter-go/internal/protocol"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testSpan(t *testing.T, id int64) *protocol.SpanPack {
	t.Helper()
	return &protocol.SpanPack{GxID: 7, TxID: id, SpanType: 1, Timestamp: time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC).UnixMilli(), AnnotationTimestamps: protocol.Value{Type: protocol.ValueList}, AnnotationValues: protocol.Value{Type: protocol.ValueList}, Tags: protocol.Value{Type: protocol.ValueMap}}
}
func container(t *testing.T, spans ...*protocol.SpanPack) *protocol.SpanContainerPack {
	t.Helper()
	e := protocol.NewEncoder(protocol.Limits{})
	for _, span := range spans {
		e.Pack(span)
	}
	raw, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return &protocol.SpanContainerPack{GxID: 7, SpanCount: int32(len(spans)), Timestamp: time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC).UnixMilli(), Spans: raw}
}
func TestSpanContainerFilesRestartAndOwnership(t *testing.T) {
	dir := t.TempDir()
	config := Config{Directory: dir, Location: time.UTC}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	first := container(t, testSpan(t, 1), testSpan(t, 2))
	encoded, err := protocol.EncodePack(first, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutSpanContainer(first); err != nil {
		t.Fatal(err)
	}
	if err := s.PutSpanContainer(container(t, testSpan(t, 3))); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "20261013/span/span.data"))
	if err != nil {
		t.Fatal(err)
	}
	if int(binary.BigEndian.Uint16(data)) != len(encoded) || !bytes.Equal(data[2:2+len(encoded)], encoded) {
		t.Fatal("signed-short record")
	}
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.SpansByGxIDContext(context.Background(), "20261013", 7)
	if err != nil || len(got) != 3 || got[0].TxID != 3 || got[1].TxID != 1 || got[2].TxID != 2 {
		t.Fatal(got, err)
	}
	got[0].TxID = 999
	again, err := s.SpansByGxIDContext(context.Background(), "20261013", 7)
	if err != nil || again[0].TxID != 3 {
		t.Fatal("borrowed result", err)
	}
	if x, err := s.XLog("20261013", 3); err != nil || x != nil {
		t.Fatal("span generated an XLog", x, err)
	}
}
func TestXLogsByGxIDOrderAndRawZeroElapsed(t *testing.T) {
	for _, zip := range []bool{false, true} {
		s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, CompressXLog: zip})
		if err != nil {
			t.Fatal(err)
		}
		start := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC).UnixMilli()
		for _, p := range []*protocol.XLogPack{{TxID: 1, GxID: 7, EndTime: start + 100, Elapsed: 0}, {TxID: 2, GxID: 7, EndTime: start + 50, Elapsed: 2}, {TxID: 3, GxID: 8, EndTime: start + 150}} {
			if err := s.PutXLog(p); err != nil {
				t.Fatal(err)
			}
		}
		got, err := s.XLogsByGxIDContext(context.Background(), "20261013", 7)
		if err != nil || len(got) != 2 || got[0].TxID != 2 || got[1].TxID != 1 {
			t.Fatal(got, err)
		}
		raw, err := s.XLogsByTimeContext(context.Background(), "20261013", start, start+200)
		if err != nil || len(raw) != 3 || raw[1].Elapsed != 0 {
			t.Fatal(raw, err)
		}
		s.Close()
	}
}
func TestSpanLimitsCancelAndMalformed(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, MaxQueryRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	big := container(t)
	big.Spans = make([]byte, 32768)
	if err := s.PutSpanContainer(big); !errors.Is(err, ErrLimit) {
		t.Fatal("oversize", err)
	}
	if err := s.PutSpanContainer(container(t, testSpan(t, 1), testSpan(t, 2))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SpansByGxIDContext(context.Background(), "20261013", 7); !errors.Is(err, ErrLimit) {
		t.Fatal("query limit", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.SpansByGxIDContext(ctx, "20261013", 7); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.PutSpanContainerContext(ctx, container(t)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.SpansByGxIDContext(context.Background(), "../escape", 7); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestSpanDeclaredCountMalformedBlobAndByteLimit(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p := container(t, testSpan(t, 1))
	p.SpanCount = 99
	if err := s.PutSpanContainer(p); err != nil {
		t.Fatal(err)
	}
	got, err := s.SpansByGxIDContext(context.Background(), "20261013", 7)
	if err != nil || len(got) != 1 {
		t.Fatal("declared count wrongly controls parse", got, err)
	}
	bad := container(t)
	bad.GxID = 8
	bad.Spans = []byte{255}
	if err := s.PutSpanContainer(bad); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SpansByGxIDContext(context.Background(), "20261013", 8); err == nil {
		t.Fatal("unknown nested Pack accepted")
	}
	s2, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, MaxQueryBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if err := s2.PutSpanContainer(container(t, testSpan(t, 1))); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.SpansByGxIDContext(context.Background(), "20261013", 7); !errors.Is(err, ErrLimit) {
		t.Fatal("byte limit", err)
	}
}

func TestSpanEmptyContainerDistinguishesMissingGroup(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.PutSpanContainer(container(t)); err != nil {
		t.Fatal(err)
	}
	got, err := s.SpansByGxIDContext(context.Background(), "20261013", 7)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("existing empty group %#v %v", got, err)
	}
	missing, err := s.SpansByGxIDContext(context.Background(), "20261013", 8)
	if err != nil || missing != nil {
		t.Fatalf("missing group %#v %v", missing, err)
	}
}
