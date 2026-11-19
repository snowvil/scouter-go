package service

import (
	"context"
	"encoding/binary"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
	"io"
	"math"
	"testing"
	"time"
)

type aggregationTestBackend struct {
	*fakeBackend
	total  TagValueTotal
	series []float32
	raw    [][]byte
}

func (b *aggregationTestBackend) VisitorRealtime(context.Context, []int32, string) (int64, error) {
	return 7, nil
}
func (b *aggregationTestBackend) VisitorDaily(context.Context, string, []int32, string) (int64, error) {
	return 8, nil
}
func (b *aggregationTestBackend) VisitorHourly(context.Context, string, int64, []int32) (int64, error) {
	return 9, nil
}
func (b *aggregationTestBackend) TagValues(context.Context, string, string, string, string) (TagValueTotal, error) {
	return b.total, nil
}
func (b *aggregationTestBackend) TagValueSeries(context.Context, string, string, string, string, protocol.Value) ([]float32, error) {
	return b.series, nil
}
func (b *aggregationTestBackend) TagActual(context.Context, TagActualQuery) ([][]byte, error) {
	return b.raw, nil
}
func aggregationFlag(t *testing.T, r io.ByteReader, want byte) {
	t.Helper()
	b, err := r.ReadByte()
	if err != nil || b != want {
		t.Fatalf("flag=%x want=%x err=%v", b, want, err)
	}
}
func TestVisitorCommandsBodyAndDateBoundaries(t *testing.T) {
	b := &aggregationTestBackend{fakeBackend: &fakeBackend{}}
	_, conn, r := startService(t, queryConfig(), b)
	session := loginSession(t, conn, r)
	for _, cmd := range []string{"VISITOR_REALTIME", "VISITOR_REALTIME_TOTAL", "VISITOR_REALTIME_GROUP", "VISITOR_LOADDATE", "VISITOR_LOADDATE_TOTAL"} {
		p := newMap(entry("objHash", decimal(42)), entry("objType", stringValue("java")), entry("date", stringValue("20261013")))
		if cmd == "VISITOR_REALTIME_GROUP" {
			p = newMap(entry("objHash", list([]protocol.Value{decimal(42)})))
		}
		send(t, conn, cmd, session, p)
		aggregationFlag(t, r, transport.HasNext)
		v, err := protocol.ReadValue(r, protocol.Limits{})
		want := int64(7)
		if cmd == "VISITOR_LOADDATE" || cmd == "VISITOR_LOADDATE_TOTAL" {
			want = 8
		}
		if err != nil || v.Type != protocol.ValueDecimal || v.Int != want {
			t.Fatal(cmd, v, err)
		}
		aggregationFlag(t, r, transport.NoNext)
	}
	send(t, conn, "VISITOR_LOADDATE_GROUP", session, newMap(entry("objHash", list(nil)), entry("startDate", stringValue("20261013")), entry("endDate", stringValue("20261014"))))
	packs := readPacks(t, r)
	if len(packs) != 2 || mapValue(packs[0].(*protocol.MapPack), "date").Text != "20261013" || mapValue(packs[1].(*protocol.MapPack), "value").Int != 8 {
		t.Fatal(packs)
	}
	from := time.Date(2026, 10, 13, 23, 30, 0, 0, time.UTC).UnixMilli()
	send(t, conn, "VISITOR_LOADHOUR_GROUP", session, newMap(entry("objHash", list(nil)), entry("stime", decimal(from)), entry("etime", decimal(from+3600000))))
	packs = readPacks(t, r)
	if len(packs) != 2 || mapValue(packs[0].(*protocol.MapPack), "time").Values[0].Int != from {
		t.Fatal("no hour rounding", packs)
	}
	send(t, conn, "VISITOR_LOADHOUR_GROUP", session, newMap(entry("objHash", list(nil)), entry("stime", decimal(from)), entry("etime", decimal(from-1))))
	packs = readPacks(t, r)
	if len(packs) != 1 || len(mapValue(packs[0].(*protocol.MapPack), "time").Values) != 0 {
		t.Fatal("inverted hourly empty map", packs)
	}
}
func TestTagCommandsExactCustomBodies(t *testing.T) {
	raw, err := protocol.EncodePack(&protocol.AlertPack{Time: 123, Title: "synthetic", Tags: protocol.Value{Type: protocol.ValueMap}}, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	b := &aggregationTestBackend{fakeBackend: &fakeBackend{}, total: TagValueTotal{HowManyValues: 3, TotalCount: 1.5, Values: []TagValueCount{{Value: protocol.Value{Type: protocol.ValueTextHash, Int: 42}, Count: 1.5}}}, series: []float32{1.5, 0}, raw: [][]byte{raw}}
	_, conn, r := startService(t, queryConfig(), b)
	session := loginSession(t, conn, r)
	for _, c := range []struct {
		cmd, group string
		want       []string
	}{{"TAGCNT_DIV_NAMES", "", []string{"service", "alert"}}, {"TAGCNT_TAG_NAMES", "alert", []string{"@total", "title", "level", "object"}}} {
		send(t, conn, c.cmd, session, newMap(entry("tagGroup", stringValue(c.group))))
		for _, want := range c.want {
			aggregationFlag(t, r, transport.HasNext)
			v, err := protocol.ReadValue(r, protocol.Limits{})
			if err != nil || v.Text != want {
				t.Fatal(v, err)
			}
		}
		aggregationFlag(t, r, transport.NoNext)
	}
	p := newMap(entry("objType", stringValue("java")), entry("tagGroup", stringValue("service")), entry("tagName", list([]protocol.Value{stringValue("service")})))
	send(t, conn, "TAGCNT_TAG_VALUES", session, p)
	aggregationFlag(t, r, transport.HasNext)
	name := string(readBlob(t, r))
	if name != "service" {
		t.Fatal(name, err)
	}
	var head [12]byte
	if _, err = io.ReadFull(r, head[:]); err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(head[:]) != 3 || math.Float32frombits(binary.BigEndian.Uint32(head[4:])) != 1.5 || binary.BigEndian.Uint32(head[8:]) != 1 {
		t.Fatal(head)
	}
	v, err := protocol.ReadValue(r, protocol.Limits{})
	if err != nil || v.Type != protocol.ValueTextHash || v.Int != 42 {
		t.Fatal(v, err)
	}
	var n [4]byte
	if _, err = io.ReadFull(r, n[:]); err != nil || math.Float32frombits(binary.BigEndian.Uint32(n[:])) != 1.5 {
		t.Fatal(n, err)
	}
	aggregationFlag(t, r, transport.NoNext)
	p.Entries[2].Value = stringValue("service")
	send(t, conn, "TAGCNT_TAG_VALUE_DATA", session, p)
	aggregationFlag(t, r, transport.HasNext)
	var series [10]byte
	if _, err = io.ReadFull(r, series[:]); err != nil || binary.BigEndian.Uint16(series[:]) != 2 || math.Float32frombits(binary.BigEndian.Uint32(series[2:])) != 1.5 {
		t.Fatal(series, err)
	}
	aggregationFlag(t, r, transport.NoNext)
	send(t, conn, "TAGCNT_TAG_ACTUAL_DATA", session, p)
	aggregationFlag(t, r, transport.HasNext)
	got := make([]byte, len(raw))
	if _, err = io.ReadFull(r, got); err != nil || string(got) != string(raw) {
		t.Fatal("raw", err)
	}
	aggregationFlag(t, r, transport.NoNext)
}
