package storage

import (
	"context"
	"errors"
	"github.com/snowvil/scouter-go/internal/protocol"
	"testing"
)

func TestQueryListIncludesEveryStatusKeyAndOwnsTypedData(t *testing.T) {
	s, start := extraStore(t, Config{})
	ctx := context.Background()
	data := protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "query", Value: protocol.Value{Type: protocol.ValueList, Values: []protocol.Value{{Type: protocol.ValueText, Text: "synthetic"}}}}}}
	for _, p := range []*protocol.StatusPack{{Time: start + 10, ObjHash: 42, Key: "query", Data: data}, {Time: start + 10, ObjHash: 7, Key: "query", Data: data}, {Time: start + 20, ObjHash: 42, Key: "other", Data: data}} {
		if err := s.PutStatusContext(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.QueryListContext(ctx, "20260101", start, start+20, 42)
	if err != nil || len(got) != 2 || got[0].Time != start+10 || got[1].Key != "other" {
		t.Fatalf("query list %+v %v", got, err)
	}
	got[0].Data.Entries[0].Value.Values[0].Text = "changed"
	again, err := s.QueryListContext(ctx, "20260101", start, start+20, 42)
	if err != nil || again[0].Data.Entries[0].Value.Values[0].Text != "synthetic" {
		t.Fatal("unowned results", err)
	}
	missing, err := s.QueryListContext(ctx, "20260102", start+86400000, start+86400001, 42)
	if err != nil || len(missing) != 0 {
		t.Fatal("missing", err)
	}
}
func TestQueryListBoundsAndCancellation(t *testing.T) {
	s, start := extraStore(t, Config{MaxQueryRecords: 1})
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := s.PutStatusContext(ctx, &protocol.StatusPack{Time: start + int64(i), ObjHash: 7, Data: mapValue()}); err != nil {
			t.Fatal(err)
		}
	}
	// Bounds apply to the collected result, not to records the object filter
	// skips (Java StatusRD.readByTime has no scan cap).
	if got, err := s.QueryListContext(ctx, "20260101", start, start+1, 42); err != nil || len(got) != 0 {
		t.Fatal("filtered scan", len(got), err)
	}
	if _, err := s.QueryListContext(ctx, "20260101", start, start+1, 7); !errors.Is(err, ErrLimit) {
		t.Fatal("unbounded collected result", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.QueryListContext(cancelled, "20260101", start, start+1, 42); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.QueryListContext(ctx, "../escape", start, start+1, 42); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
