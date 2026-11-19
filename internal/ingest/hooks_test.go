package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// TextCore: SqlTables.add sees an sql TextPack before TextWR.add stores it;
// other text types are not offered. PackLog runs before and after the
// dispatch with the dispatched fields filled.
func TestSQLTextAndPackLogHooks(t *testing.T) {
	now := time.Date(2026, 10, 15, 1, 2, 3, 0, time.UTC)
	var sqlSeen []string
	var events []string
	var e *Engine
	e, store := testEngineWith(t, &now, func(c *Config) {
		c.OnSQLText = func(date string, hash int32, text string) {
			_, stored, _ := e.store.Text(date, "sql", hash)
			sqlSeen = append(sqlSeen, date+"|"+text)
			if stored {
				t.Error("SqlTables.add must precede the text store write")
			}
		}
		c.PackLog = func(p protocol.Pack, processed bool) {
			if o, ok := p.(*protocol.ObjectPack); ok {
				events = append(events, map[bool]string{false: "before:", true: "after:"}[processed]+o.Address)
			}
		}
	})
	ctx := context.Background()
	if err := e.Accept(ctx, &protocol.TextPack{XType: "sql", Hash: 5, Text: "select 1"}); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, &protocol.TextPack{XType: "service", Hash: 6, Text: "/svc"}); err != nil {
		t.Fatal(err)
	}
	if len(sqlSeen) != 1 || sqlSeen[0] != "20261015|select 1" {
		t.Fatalf("OnSQLText %v", sqlSeen)
	}
	if v, ok, _ := store.Text("20261015", "sql", 5); !ok || v != "select 1" {
		t.Fatal("sql text not stored")
	}
	if err := e.AcceptFrom(ctx, &protocol.ObjectPack{ObjName: "/o", ObjHash: 9, ObjType: "t", Tags: protocol.Value{Type: protocol.ValueMap}}, "10.0.0.1"); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0] != "before:" || events[1] != "after:10.0.0.1" {
		t.Fatalf("PackLog %v", events)
	}
	if name, ok := e.ObjectNameCached(9); !ok || name != "/o" {
		t.Fatalf("ObjectNameCached %q %v", name, ok)
	}
	if err := e.RemoveObjects(ctx, []int32{9}, false); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.ObjectNameCached(9); ok {
		t.Fatal("removed object still cached")
	}
}
