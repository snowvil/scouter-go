package ingest

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/storage"
)

type alertsStep struct {
	Op      string          `json:"op"`
	T       int64           `json:"t"`
	ObjName string          `json:"objName"`
	ObjType string          `json:"objType"`
	Address string          `json:"address"`
	Version string          `json:"version"`
	Tags    oraclePairs     `json:"tags"`
	Key     string          `json:"key"`
	Value   int64           `json:"value"`
	Ops     []any           `json:"ops"`
	Out     json.RawMessage `json:"out"`
}

type oracleAlert struct {
	Via     string `json:"via"`
	Title   string `json:"title"`
	Level   int    `json:"level"`
	ObjHash int32  `json:"objHash"`
	ObjType string `json:"objType"`
	Message string `json:"message"`
}

// The original AgentManager (active/inactive, its 1s daemon and ObjectMap)
// replayed: alert titles, levels, order and messages (ObjectPack.toString
// with Hexa32, Timestamp and Hashtable-ordered tags), the alive flags, the
// dead-time rules, and the IntKeyMap order of the daemon walk.
func TestJavaOracleAgentManagerAlerts(t *testing.T) {
	const base = int64(1760000000000) // IngestOracle.ALERT_BASE
	seoul, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(base)
	root := t.TempDir()
	s, err := storage.Open(storage.Config{Directory: root, Location: seoul})
	if err != nil {
		t.Fatal(err)
	}
	c, err := storage.OpenCounters(root, seoul)
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{Location: seoul, Clock: func() time.Time { return now }, XLogCapacity: 8, ObjectType: bundledTypes(t)}, s, c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { e.Close(); c.Close(); s.Close() }()
	ctx := context.Background()
	cursor := service.AlertQuery{}
	newAlerts := func() []oracleAlert {
		r, err := e.AlertsRealtime(ctx, cursor)
		if err != nil {
			t.Fatal(err)
		}
		cursor.Loop, cursor.Index = r.Loop, r.Index
		out := []oracleAlert{}
		for _, a := range r.Records {
			via := "core" // AlertCore.add (stored, TestObjectInactiveAndActivatedAlerts)
			if a.Title == alertDup {
				via = "cache" // AlertCache.put only (TestObjectTooManyChangeAlert)
			}
			out = append(out, oracleAlert{via, a.Title, int(a.Level), a.ObjHash, a.ObjType, a.Message})
		}
		return out
	}
	aliveMap := func() map[string]bool {
		m := map[string]bool{}
		for hash, st := range e.objState {
			m[e.objects[hash].ObjName] = st.alive
		}
		return m
	}
	set := DefaultSettings()
	steps := 0
	for i, raw := range loadOracle(t, "alerts") {
		var st alertsStep
		if err := json.Unmarshal(raw, &st); err != nil {
			t.Fatal(err)
		}
		switch st.Op {
		case "timestamps":
			var want map[string]string
			if err := json.Unmarshal(st.Out, &want); err != nil {
				t.Fatal(err)
			}
			for k, v := range want {
				ms, _ := strconv.ParseInt(k, 10, 64)
				if got := javaTimestamp(ms, seoul); got != v {
					t.Fatalf("Timestamp(%d) go %q java %q", ms, got, v)
				}
			}
			continue
		case "intkeymap":
			m := newIntKeyOrder()
			for _, op := range st.Ops {
				switch v := op.(type) {
				case float64:
					m.put(int32(v))
				case string:
					n, _ := strconv.ParseInt(v[1:], 10, 32)
					m.remove(int32(n))
				}
			}
			var want []int32
			if err := json.Unmarshal(st.Out, &want); err != nil {
				t.Fatal(err)
			}
			if got := m.keys(); !reflect.DeepEqual(got, want) {
				t.Fatalf("IntKeyMap order:\n go   %v\n java %v", got, want)
			}
			continue
		case "conf":
			switch st.Key {
			case "object_zipkin_deadtime_ms":
				set.ZipkinDeadTimeMs = st.Value
			case "object_inactive_alert_level":
				set.InactiveAlertLevel = int32(st.Value)
			default:
				t.Fatalf("conf %s", st.Key)
			}
			e.SetSettings(set)
			continue
		case "active":
			now = time.UnixMilli(base + st.T)
			p := &protocol.ObjectPack{ObjName: st.ObjName, ObjType: st.ObjType, Address: st.Address, Version: st.Version, Tags: st.Tags.mapValue(t)}
			if err := e.Accept(ctx, p); err != nil {
				t.Fatalf("step %d: %v", i, err)
			}
		case "tick":
			now = time.UnixMilli(base + st.T)
			if err := e.Tick(ctx); err != nil {
				t.Fatalf("step %d: %v", i, err)
			}
		default:
			t.Fatalf("op %q", st.Op)
		}
		steps++
		var want struct {
			Alerts []oracleAlert   `json:"alerts"`
			Alive  map[string]bool `json:"alive"`
		}
		if err := json.Unmarshal(st.Out, &want); err != nil {
			t.Fatal(err)
		}
		if got := newAlerts(); !reflect.DeepEqual(got, want.Alerts) {
			t.Fatalf("step %d %s %s@%d alerts:\n go   %+v\n java %+v", i, st.Op, st.ObjName, st.T, got, want.Alerts)
		}
		if got := aliveMap(); !reflect.DeepEqual(got, want.Alive) {
			t.Fatalf("step %d alive:\n go   %v\n java %v", i, got, want.Alive)
		}
	}
	if steps < 30 {
		t.Fatalf("only %d replayed steps", steps)
	}
}
