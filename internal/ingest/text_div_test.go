package ingest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

// R7: TextPacks of divs the original never writes are rejected (counted in
// TextDivsLimited and Rejected, not StorageErrors) once
// storage.DefaultMaxUnknownTextDivs unknown divs have a permanent index; a
// normal Agent's texts and registration are not affected.
func TestUnknownTextDivsLimitedInIngest(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	e, s, _ := testEngine(t, &now, 16)
	ctx := context.Background()
	const n = 100
	for i := 0; i < n; i++ {
		err := e.Accept(ctx, &protocol.TextPack{XType: fmt.Sprintf("junk%d", i), Hash: int32(i), Text: "x"})
		if i < storage.DefaultMaxUnknownTextDivs && err != nil {
			t.Fatal(i, err)
		}
		if i >= storage.DefaultMaxUnknownTextDivs && !errors.Is(err, storage.ErrTextDivLimit) {
			t.Fatal(i, err)
		}
	}
	limited := uint64(n - storage.DefaultMaxUnknownTextDivs)
	if st := e.Stats(); st.TextDivsLimited != limited || st.Rejected != limited || st.StorageErrors != 0 || st.Completed != storage.DefaultMaxUnknownTextDivs {
		t.Fatalf("stats %+v", st)
	}
	// A normal Agent: registration (object text), its service/sql/error texts.
	if err := e.Accept(ctx, &protocol.ObjectPack{ObjName: "/host/app", ObjType: "java", Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
		t.Fatal(err)
	}
	texts := []protocol.TextPack{{XType: "service", Hash: 11, Text: "/svc"}, {XType: "sql", Hash: 12, Text: "select 1"}, {XType: "error", Hash: 13, Text: "boom"}, {XType: "hmsg", Hash: 14, Text: "m"}}
	for i := range texts {
		if err := e.Accept(ctx, &texts[i]); err != nil {
			t.Fatal(texts[i].XType, err)
		}
	}
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	const date = "20261023"
	for _, p := range texts {
		if got, ok, err := s.Text(date, p.XType, p.Hash); err != nil || !ok || got != p.Text {
			t.Fatalf("%s: %q %v %v", p.XType, got, ok, err)
		}
	}
	if got, ok, err := s.Text(date, "object", storage.CounterHash("/host/app")); err != nil || !ok || got != "/host/app" {
		t.Fatalf("object text %q %v %v", got, ok, err)
	}
	if st := e.Stats(); st.TextDivsLimited != limited || st.Rejected != limited {
		t.Fatalf("stats after normal traffic %+v", st)
	}
}
