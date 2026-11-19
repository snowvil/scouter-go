package service

import (
	"github.com/snowvil/scouter-go/internal/protocol"
	"testing"
)

func TestObjectTodayFullListBodylessPipelineStatusesAndOwnership(t *testing.T) {
	current := &protocol.ObjectPack{ObjHash: 1, ObjType: "java", ObjName: "current", Alive: true, Tags: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("status", text("old"))}}}
	b := &extendedCounterBackend{objects: map[string][]*protocol.ObjectPack{"": {current, {ObjHash: 2, ObjType: "java", Alive: false, Tags: protocol.Value{Type: protocol.ValueMap}}}, "20261013": {{ObjHash: 3, ObjType: "java", Alive: true, Tags: protocol.Value{Type: protocol.ValueMap}}, {ObjHash: 1, ObjType: "java", ObjName: "stored", Tags: protocol.Value{Type: protocol.ValueMap}}, {ObjHash: 2, ObjType: "java", Tags: protocol.Value{Type: protocol.ValueMap}}}}}
	_, c, r, _ := startExtendedCounter(t, b)
	send(t, c, "OBJECT_TODAY_FULL_LIST", 1, nil)
	send(t, c, "COUNTER_REAL_TIME_GROUP", 1, newMap(entry("objHash", list(nil)), entry("counter", text("cpu"))))
	ps := readPacks(t, r)
	if len(ps) != 3 {
		t.Fatalf("items %d", len(ps))
	}
	want := []string{"dead", "active", "inactive"}
	for i, p := range ps {
		o := p.(*protocol.ObjectPack)
		status := ""
		for _, e := range o.Tags.Entries {
			if e.Key == "status" {
				status = e.Value.Text
			}
		}
		if status != want[i] {
			t.Fatalf("status %d %s", i, status)
		}
	}
	if ps[1].(*protocol.ObjectPack).ObjName != "current" || ps[0].(*protocol.ObjectPack).Alive {
		t.Fatal("current selection/dead alive flag")
	}
	if current.Tags.Entries[0].Value.Text != "old" {
		t.Fatal("shared tags mutated")
	}
	if len(readPacks(t, r)) != 1 {
		t.Fatal("bodyless consumed next command")
	}
}
