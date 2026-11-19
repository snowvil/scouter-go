package service

import (
	"context"
	"github.com/snowvil/scouter-go/internal/protocol"
	"testing"
)

type exportCounterBackend struct {
	*extendedCounterBackend
	rows     []CounterRow
	from, to int64
}

func (b *exportCounterBackend) CounterRows(ctx context.Context, date string, hash int32, from, to int64) ([]CounterRow, error) {
	b.from, b.to = from, to
	return b.rows, b.err
}
func (b *exportCounterBackend) CounterType(ctx context.Context, typ string) (CounterTypeMetadata, error) {
	return CounterTypeMetadata{DisplayName: "Synthetic", Counters: []string{"z", "cpu"}}, nil
}
func (b *exportCounterBackend) CounterNamesForDate(ctx context.Context, date string, hash int32) ([]string, error) {
	return []string{"a", "cpu"}, nil
}
func TestExportTimeRowsSortedHeaderMissingAndRepeatedTimestamp(t *testing.T) {
	b := &exportCounterBackend{extendedCounterBackend: &extendedCounterBackend{objects: map[string][]*protocol.ObjectPack{"20260101": {{ObjHash: 1, ObjType: "java"}}}}, rows: []CounterRow{{Time: 1767225600000, Values: protocol.Value{Type: protocol.ValueMap}}, {Time: 1767225600000, Values: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("cpu", decimal(7))}}}}}
	_, c, r, _ := startExtendedCounter(t, b)
	send(t, c, "EXPORT_OBJECT_TIME_COUNTER", 1, newMap(entry("objHash", decimal(1)), entry("date", text("20260101"))))
	ps := readPacks(t, r)
	if len(ps) != 3 {
		t.Fatalf("rows %d", len(ps))
	}
	header := lookup(ps[0].(*protocol.MapPack), "values").Values
	if len(header) != 3 || header[0].Text != "time" || header[1].Text != "cpu" || header[2].Text != "z" {
		t.Fatalf("header %#v", header)
	}
	a := lookup(ps[1].(*protocol.MapPack), "values").Values
	brow := lookup(ps[2].(*protocol.MapPack), "values").Values
	if a[0].Text != "000000" || a[1].Type != protocol.ValueText || a[1].Text != "" || brow[1].Int != 7 {
		t.Fatalf("rows %#v %#v", a, brow)
	}
	if b.to-b.from != 86400000 {
		t.Fatal("default full date range")
	}
}
func TestExportRegularStoredNamesAndNullSlots(t *testing.T) {
	b := &exportCounterBackend{extendedCounterBackend: &extendedCounterBackend{objects: map[string][]*protocol.ObjectPack{"20260101": {{ObjHash: 1, ObjType: "java"}}}, daily: map[string]map[int32][]protocol.Value{"20260101": {1: {decimal(7)}}}}}
	_, c, r, _ := startExtendedCounter(t, b)
	send(t, c, "EXPORT_OBJECT_REGULAR_COUNTER", 1, newMap(entry("objHash", decimal(1)), entry("date", text("20260101")), entry("timeType", decimal(5))))
	ps := readPacks(t, r)
	if len(ps) != 25 {
		t.Fatalf("hour rows %d", len(ps))
	}
	header := lookup(ps[0].(*protocol.MapPack), "values").Values
	if len(header) != 4 || header[1].Text != "a" || header[2].Text != "cpu" || header[3].Text != "z" {
		t.Fatalf("header %#v", header)
	}
	row := lookup(ps[24].(*protocol.MapPack), "values").Values
	if row[0].Text != "23:00" || row[1].Type != protocol.ValueNull {
		t.Fatalf("tail %#v", row)
	}
}
