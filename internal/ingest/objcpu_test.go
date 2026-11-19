package ingest

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

func javaeeType(objType string) (ObjectTypeInfo, bool) {
	switch objType {
	case "tomcat":
		return ObjectTypeInfo{Family: familyJavaEE}, true
	case "linux":
		return ObjectTypeInfo{Family: "host"}, true
	}
	return ObjectTypeInfo{}, false
}

func cpuAgent(name, objType string, enabled bool, threshold, duration protocol.Value) *protocol.ObjectPack {
	tags := protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{
		{Key: tagAutodumpCPUEnabled, Value: protocol.Value{Type: protocol.ValueBoolean, Bool: enabled}},
		{Key: tagAutodumpCPUThreshold, Value: threshold},
		{Key: tagAutodumpCPUDuration, Value: duration},
	}}
	return &protocol.ObjectPack{ObjName: name, ObjType: objType, Tags: tags}
}

func decimal(n int64) protocol.Value { return protocol.Value{Type: protocol.ValueDecimal, Int: n} }

func cpuCounter(name string, ts int64, cpu *protocol.Value) *protocol.PerfCounterPack {
	data := protocol.Value{Type: protocol.ValueMap}
	if cpu != nil {
		data.Entries = append(data.Entries, protocol.Entry{Key: counterProcCPU, Value: *cpu})
	} else {
		data.Entries = append(data.Entries, protocol.Entry{Key: "Other", Value: decimal(1)})
	}
	return &protocol.PerfCounterPack{ObjName: name, Time: ts, TimeType: 1, Data: data}
}

func TestObjectCpuCheckerBranches(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.ObjectType = javaeeType })
	ctx := context.Background()
	if err := e.Accept(ctx, cpuAgent("/was", "tomcat", true, decimal(50), decimal(10000))); err != nil {
		t.Fatal(err)
	}
	hash := storage.CounterHash("/was")
	t0 := now.UnixMilli()
	high, low := protocol.Value{Type: protocol.ValueFloat, Float: 60}, protocol.Value{Type: protocol.ValueFloat, Float: 50}
	send := func(ts int64, cpu *protocol.Value) {
		t.Helper()
		if err := e.Accept(ctx, cpuCounter("/was", ts, cpu)); err != nil {
			t.Fatal(err)
		}
	}
	send(t0, &high)
	if e.cpuFirst["/was"] != t0 {
		t.Fatal("streak starts at the first pack over the threshold")
	}
	send(t0+5000, &high)
	send(t0+10000, &high) // first < time - duration is false at exactly duration
	if len(takeCalls(e.cpuCalls)) != 0 {
		t.Fatal("no dump within the duration")
	}
	send(t0+10001, &high)
	calls := takeCalls(e.cpuCalls)
	if len(calls) != 1 || calls[0].hash != hash || calls[0].cmd != cmdTriggerDumps {
		t.Fatalf("dump %+v", calls)
	}
	got, err := protocol.EncodePack(calls[0].param, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	want, _ := protocol.EncodePack(&protocol.MapPack{Entries: []protocol.Entry{{Key: "objHash", Value: decimal(int64(hash))},
		{Key: "TRIGGER_DUMP_REASON", Value: protocol.Value{Type: protocol.ValueText, Text: "TRIGGER_DUMP_REASON_TYPE_CPU_EXCEEDED"}}}}, protocol.Limits{})
	if !bytes.Equal(got, want) {
		t.Fatalf("dump param %x != %x", got, want)
	}
	if _, ok := e.cpuFirst["/was"]; ok {
		t.Fatal("dump clears the streak")
	}
	// At or below the threshold clears (threshold < cpu is strict).
	send(t0+20000, &high)
	send(t0+21000, &low)
	if _, ok := e.cpuFirst["/was"]; ok {
		t.Fatal("cpu == threshold clears")
	}
	// A pack without ProcCpu returns without clearing.
	send(t0+22000, &high)
	send(t0+23000, nil)
	if e.cpuFirst["/was"] != t0+22000 {
		t.Fatal("missing ProcCpu keeps the streak")
	}
	// A streak older than duration*2 is cleared and this pack is not counted.
	send(t0+22000+20001, &high)
	if _, ok := e.cpuFirst["/was"]; ok || len(takeCalls(e.cpuCalls)) != 0 {
		t.Fatal("stale streak must clear without a dump")
	}
	// Disabled autodump clears and returns.
	send(t0+50000, &high)
	if err := e.Accept(ctx, cpuAgent("/was", "tomcat", false, decimal(50), decimal(10000))); err != nil {
		t.Fatal(err)
	}
	send(t0+70000, &high)
	if _, ok := e.cpuFirst["/was"]; ok || len(takeCalls(e.cpuCalls)) != 0 {
		t.Fatal("disabled autodump")
	}
}

func TestObjectCpuCheckerScope(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.ObjectType = javaeeType })
	ctx := context.Background()
	for _, p := range []*protocol.ObjectPack{
		cpuAgent("/host", "linux", true, decimal(0), decimal(-1)),
		cpuAgent("/unknown", "nosuch", true, decimal(0), decimal(-1)),
		cpuAgent("/was", "tomcat", true, decimal(0), decimal(-1)),
	} {
		if err := e.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	t0 := now.UnixMilli()
	high := protocol.Value{Type: protocol.ValueDecimal, Int: 5}
	for _, name := range []string{"/host", "/unknown", "/absent"} {
		if err := e.Accept(ctx, cpuCounter(name, t0, &high)); err != nil {
			t.Fatal(err)
		}
	}
	// Not REALTIME: the 5-minute pack is not checked.
	p := cpuCounter("/was", t0, &high)
	p.TimeType = 3
	if err := e.Accept(ctx, p); err != nil {
		t.Fatal(err)
	}
	if len(takeCalls(e.cpuCalls)) != 0 || len(e.cpuFirst) != 0 {
		t.Fatal("only realtime packs of javaee agents are checked")
	}
	// A negative duration dumps on the first pack over the threshold.
	if err := e.Accept(ctx, cpuCounter("/was", t0, &high)); err != nil {
		t.Fatal(err)
	}
	if len(takeCalls(e.cpuCalls)) != 1 {
		t.Fatal("negative duration")
	}
	// No family resolver: disabled.
	e.config.ObjectType = nil
	if err := e.Accept(ctx, cpuCounter("/was", t0, &high)); err != nil {
		t.Fatal(err)
	}
	if len(takeCalls(e.cpuCalls)) != 0 {
		t.Fatal("nil ObjectType")
	}
}

func TestObjectCpuCheckerJavaArithmetic(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.ObjectType = javaeeType })
	ctx := context.Background()
	// duration 1<<30: Scala duration*2 wraps to math.MinInt32, so
	// "first < time - duration*2" is first < time + 2^31 and the streak clears.
	if err := e.Accept(ctx, cpuAgent("/was", "tomcat", true, protocol.Value{Type: protocol.ValueDouble, Double: 1e12}, decimal(1<<30))); err != nil {
		t.Fatal(err)
	}
	t0 := now.UnixMilli()
	// Threshold Double 1e12 narrows to MaxInt32; float32(MaxInt32) == 2^31.
	over := protocol.Value{Type: protocol.ValueDouble, Double: math.Pow(2, 31) * 1.01}
	if err := e.Accept(ctx, cpuCounter("/was", t0, &over)); err != nil {
		t.Fatal(err)
	}
	if e.cpuFirst["/was"] != t0 {
		t.Fatal("saturated threshold comparison")
	}
	if err := e.Accept(ctx, cpuCounter("/was", t0+1, &over)); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.cpuFirst["/was"]; ok {
		t.Fatal("wrapped duration*2 must clear the streak")
	}
	if javaD2I(math.NaN()) != 0 || javaD2I(-1e300) != math.MinInt32 || javaD2I(3.9) != 3 || javaD2I(-3.9) != -3 {
		t.Fatal("javaD2I")
	}
}

func TestObjectCpuCheckerQueueAndObjectRemoval(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.ObjectType = javaeeType })
	ctx := context.Background()
	if err := e.Accept(ctx, cpuAgent("/was", "tomcat", true, decimal(0), decimal(-1))); err != nil {
		t.Fatal(err)
	}
	high := decimal(5)
	for i := 0; i < cpuDumpQueueSize+5; i++ {
		if err := e.Accept(ctx, cpuCounter("/was", now.UnixMilli()+int64(i), &high)); err != nil {
			t.Fatal(err)
		}
	}
	if st := e.Stats().CPUDumpCalls(); st.Enqueued != cpuDumpQueueSize || st.Dropped != 5 || st.Pending != cpuDumpQueueSize {
		t.Fatalf("RequestQueue(100) put drops when full: %+v", st)
	}
	// A streak of a removed object is dropped with it.
	if err := e.Accept(ctx, cpuAgent("/was", "tomcat", true, decimal(0), decimal(100000))); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, cpuCounter("/was", now.UnixMilli(), &high)); err != nil {
		t.Fatal(err)
	}
	if len(e.cpuFirst) != 1 {
		t.Fatal("streak")
	}
	if err := e.RemoveObjects(ctx, []int32{storage.CounterHash("/was")}, false); err != nil {
		t.Fatal(err)
	}
	if len(e.cpuFirst) != 0 || len(e.objectNames) != 0 {
		t.Fatal("object removal must drop the name and the streak")
	}
	// Close counts what is still queued.
	e.Close()
	if st := e.Stats().CPUDumpCalls(); st.Abandoned != cpuDumpQueueSize || st.Pending != 0 {
		t.Fatalf("abandoned %+v", st)
	}
}
