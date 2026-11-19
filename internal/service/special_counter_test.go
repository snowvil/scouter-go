package service

import (
	"errors"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
	"testing"
)

func TestSpecialCounterActiveSpeedSelectorsAndFloatTPS(t *testing.T) {
	b := &extendedCounterBackend{objects: map[string][]*protocol.ObjectPack{"": {{ObjHash: 1, ObjType: "java", Alive: true}, {ObjHash: 2, ObjType: "java", Alive: false}}}, values: map[int32]map[string]protocol.Value{1: {"ActiveSpeed": list([]protocol.Value{decimal(1), decimal(2), decimal(3)}), "TPS": {Type: protocol.ValueFloat, Float: 1.25}}, 2: {"ActiveSpeed": list([]protocol.Value{decimal(9), decimal(9), decimal(9)}), "TPS": decimal(100)}}}
	_, c, r, _ := startExtendedCounter(t, b)
	send(t, c, "ACTIVESPEED_REAL_TIME", 1, newMap(entry("objType", text("java"))))
	ps := readPacks(t, r)
	if len(ps) != 2 || lookup(ps[1].(*protocol.MapPack), "act1").Int != 9 {
		t.Fatal("per-object must include inactive")
	}
	send(t, c, "ACTIVESPEED_GROUP_REAL_TIME_GROUP", 1, newMap(entry("objHash", list([]protocol.Value{decimal(1), decimal(2), decimal(1), decimal(99)}))))
	p := readPacks(t, r)[0].(*protocol.MapPack)
	if lookup(p, "act1").Int != 2 || lookup(p, "tps").Type != protocol.ValueFloat || lookup(p, "tps").Float != 2.5 {
		t.Fatalf("aggregate %#v", p)
	}
}
func TestSpecialCounterMapRawValueAndPartialFailure(t *testing.T) {
	b := &extendedCounterBackend{values: map[int32]map[string]protocol.Value{1: {"m": {Type: protocol.ValueMap, Entries: []protocol.Entry{entry("a", decimal(8))}}}, 2: {"m": decimal(5)}}}
	_, c, r, results := startExtendedCounter(t, b)
	send(t, c, "COUNTER_MAP_REAL_TIME", 1, newMap(entry("counter", text("m")), entry("objHash", list([]protocol.Value{decimal(1), decimal(2), decimal(9)}))))
	// The transport closes on handler failure before flushing buffered items.
	// Never append NoNEXT to a partial failed response.
	if f, err := r.ReadByte(); err == nil && f == transport.NoNext {
		t.Fatal("failed query sent a success terminator")
	}
	if err := <-results; err == nil || errors.Is(err, transport.ErrUnsupported) {
		t.Fatalf("missing cache must fail after partial output: %v", err)
	}
}
func TestSpecialCounterNumberReceiverControlsTypeAndTruncation(t *testing.T) {
	b := &extendedCounterBackend{objects: map[string][]*protocol.ObjectPack{"": {{ObjHash: 1, ObjType: "java", Alive: true}, {ObjHash: 2, ObjType: "java", Alive: true}}}, values: map[int32]map[string]protocol.Value{1: {"cpu": decimal(7)}, 2: {"cpu": {Type: protocol.ValueFloat, Float: 2.9}}}}
	_, c, r, _ := startExtendedCounter(t, b)
	send(t, c, "SHOW_REAL_TIME_STRING", 1, newMap(entry("counter", text("cpu")), entry("objType", text("java"))))
	p := readPacks(t, r)[0].(*protocol.MapPack)
	v := lookup(p, "result")
	if v.Type != protocol.ValueDecimal || v.Int != 9 || lookup(p, "desc").Text != "cpu of Synthetic" {
		t.Fatalf("number result %#v", p)
	}
	if b.values[1]["cpu"].Int != 7 {
		t.Fatal("query changed backend cache")
	}
}
func TestSpecialCounterDecimalToFloatAvoidsDoubleRounding(t *testing.T) {
	n := int64(1<<60) + int64(1<<36) + 1
	b := &extendedCounterBackend{objects: map[string][]*protocol.ObjectPack{"": {{ObjHash: 1, ObjType: "java", Alive: true}}}, values: map[int32]map[string]protocol.Value{1: {"TPS": decimal(n)}}}
	_, c, r, _ := startExtendedCounter(t, b)
	send(t, c, "ACTIVESPEED_REAL_TIME_GROUP", 1, newMap(entry("objType", text("java"))))
	p := readPacks(t, r)[0].(*protocol.MapPack)
	if got := lookup(p, "tps").Float; got != float32(n) {
		t.Fatalf("Java long->float direct rounding got %g want %g", got, float32(n))
	}
}
