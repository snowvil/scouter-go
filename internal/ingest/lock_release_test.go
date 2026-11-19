package ingest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/storage"
)

// acceptWithin fails instead of hanging when the engine lock is still held.
func acceptWithin(e *Engine, p protocol.Pack) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return e.Accept(ctx, p)
}

// legacyRealtime is the XLogsRealtime body before the decode moved out of the
// engine lock (whole query under the lock), kept as the equivalence oracle.
func legacyRealtime(e *Engine, q service.RealtimeQuery) (service.RealtimeResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	result := service.RealtimeResult{Loop: e.loop, Index: e.index}
	capacity := int32(len(e.ring))
	if q.Index < 0 || q.Index > capacity || q.LatestCount < 0 || q.LatestCount > capacity {
		return result, protocol.ErrInvalid
	}
	add := func(from, to int32) error {
		for i := from; i < to; i++ {
			if e.ring[i] == nil {
				continue
			}
			p, err := protocol.DecodePack(e.ring[i], protocol.Limits{})
			if err != nil {
				return err
			}
			v := p.(*protocol.XLogPack)
			if contains(q.ObjectHashes, v.ObjHash) && (q.LatestCount > 0 || v.Elapsed >= q.MinElapsed || v.Error != 0) {
				result.Records = append(result.Records, v)
			}
		}
		return nil
	}
	var err error
	if q.LatestCount > 0 {
		count := q.LatestCount
		switch e.loop - q.Loop {
		case 0:
			if gap := e.index - q.Index; gap > 0 && gap < count {
				count = gap
			}
		case 1:
			if gap := capacity - q.Index + e.index; gap < count {
				count = gap
			}
		}
		if count > e.index {
			err = add(capacity-(count-e.index), capacity)
			if err == nil {
				err = add(0, e.index)
			}
		} else {
			err = add(e.index-count, e.index)
		}
	} else {
		switch e.loop - q.Loop {
		case 0:
			if q.Index < e.index {
				err = add(q.Index, e.index)
			}
		case 1:
			start := q.Index
			if start <= e.index {
				start = e.index
			}
			err = add(start, capacity)
			if err == nil {
				err = add(0, e.index)
			}
		default:
			err = add(e.index, capacity)
			if err == nil {
				err = add(0, e.index)
			}
		}
	}
	return result, err
}

func TestXLogsRealtimeMatchesLockedQuery(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	const capacity = 16
	e, _, _ := testEngine(t, &now, capacity)
	ctx := context.Background()
	check := func(stage string) {
		t.Helper()
		for loop := int64(-1); loop <= e.loop+1; loop++ {
			for index := int32(-1); index <= capacity+1; index++ {
				for _, latest := range []int32{-1, 0, 1, 5, capacity, capacity + 1} {
					for _, hashes := range [][]int32{nil, {2}, {1, 3}} {
						q := service.RealtimeQuery{Loop: loop, Index: index, LatestCount: latest, MinElapsed: 3, ObjectHashes: hashes}
						got, gotErr := e.XLogsRealtime(ctx, q)
						want, wantErr := legacyRealtime(e, q)
						if !errors.Is(gotErr, wantErr) || !reflect.DeepEqual(got, want) {
							t.Fatalf("%s %+v: got %+v %v want %+v %v", stage, q, got, gotErr, want, wantErr)
						}
					}
				}
			}
		}
	}
	check("empty")
	for i := int64(1); i <= 3*capacity+5; i++ {
		x := &protocol.XLogPack{TxID: i, Service: 1, ObjHash: int32(i % 4), Elapsed: int32(i % 7), Error: int32(i % 5 / 4), TailVersion: 12}
		if err := e.Accept(ctx, x); err != nil {
			t.Fatal(err)
		}
		if i == capacity/2 || i == capacity || i == capacity+3 || i == 3*capacity+5 {
			check("after " + time.Duration(i).String())
		}
	}
}

// A Client realtime query decodes outside the engine lock: ingest proceeds
// while it decodes, and the query returns the cursor and records it saw when
// it took the slots.
func TestXLogsRealtimeDecodesWithoutEngineLock(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	const capacity = 4096
	e, _, _ := testEngine(t, &now, capacity)
	ctx := context.Background()
	for i := int64(1); i <= capacity+10; i++ {
		if err := e.Accept(ctx, &protocol.XLogPack{TxID: i, Service: 1, ObjHash: 1, Elapsed: 10, TailVersion: 12}); err != nil {
			t.Fatal(err)
		}
	}
	var hookErr error
	ingested := 0
	e.realtimeDecodeHook = func() {
		// Overwrite slots the query already took: the slot bytes are replaced,
		// never mutated, so the query still decodes what it took.
		for i := int64(0); i < 20 && hookErr == nil; i++ {
			hookErr = acceptWithin(e, &protocol.XLogPack{TxID: 100000 + i, Service: 1, ObjHash: 1, Elapsed: 10, TailVersion: 12})
			ingested++
		}
	}
	out, err := e.XLogsRealtime(ctx, service.RealtimeQuery{Loop: 0, Index: 0})
	e.realtimeDecodeHook = nil
	if hookErr != nil {
		t.Fatal("ingest waited for the realtime query:", hookErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if out.Loop != 1 || out.Index != 10 || len(out.Records) != capacity {
		t.Fatalf("cursor loop=%d index=%d records=%d", out.Loop, out.Index, len(out.Records))
	}
	for i, r := range out.Records {
		if r.TxID != int64(i)+11 {
			t.Fatalf("record %d txid %d", i, r.TxID)
		}
	}
	if e.index != 10+int32(ingested) {
		t.Fatal("hook ingest not applied", e.index, ingested)
	}
}

// The 5m sample writes run after the engine lock is released; the samples are
// taken (and their budget released) under it.
func TestSample5MWritesWithoutEngineLock(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 3, 0, 0, time.UTC)
	e, _, c := testEngine(t, &now, 8)
	ctx := context.Background()
	counter := &protocol.PerfCounterPack{ObjName: "/a", Time: now.UnixMilli(), TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 41}}}}}
	if err := e.Accept(ctx, counter); err != nil {
		t.Fatal(err)
	}
	var hookErr error
	e.periodicWriteHook = func() {
		if len(e.samples) != 0 {
			hookErr = errors.New("samples still buffered during the write")
			return
		}
		hookErr = acceptWithin(e, &protocol.XLogPack{TxID: 1, Service: 1, ObjHash: 1, Elapsed: 10, TailVersion: 12})
	}
	if err := e.Sample5M(ctx); err != nil {
		t.Fatal(err)
	}
	e.periodicWriteHook = nil
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	flush(t, e)
	values, err := c.Daily("20261018", storage.CounterHash("/a"), "TPS", 3)
	if err != nil || values[144].Int != 41 {
		t.Fatal(values, err)
	}
	if st := e.Stats(); st.Sample5MDropped != 0 {
		t.Fatal(st.Sample5MDropped)
	}
}

// A failed 5m write ends the pass like before, but the samples were already
// taken (Java swaps the map first): the unwritten ones are counted, not kept.
func TestSample5MWriteErrorCountsDropped(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 3, 0, 0, time.UTC)
	e, _, c := testEngine(t, &now, 8)
	ctx := context.Background()
	for _, name := range []string{"/a", "/b"} {
		p := &protocol.PerfCounterPack{ObjName: name, Time: now.UnixMilli(), TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 1}}, {Key: "Cpu", Value: protocol.Value{Type: protocol.ValueFloat, Float: 2}}}}}
		if err := e.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	samples := len(e.samples)
	if samples < 4 { // plus the NetDataProcessor metadata counters
		t.Fatal("samples", samples)
	}
	flush(t, e) // the realtime counter writes
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.Sample5M(ctx); err != nil {
		t.Fatal(err) // the write is the daily writer's: counted there
	}
	flush(t, e)
	if st := e.Stats(); st.Sample5MDropped != uint64(samples) || st.WriteErrors != uint64(samples) {
		t.Fatal("dropped", st.Sample5MDropped, st.WriteErrors)
	}
	if len(e.samples) != 0 {
		t.Fatal("samples kept after the pass")
	}
}

// Tick builds the alert summary and the group rows under the engine lock and
// writes them after releasing it.
func TestTickWritesWithoutEngineLock(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 4, 59, 0, time.UTC)
	e, s, c := testEngine(t, &now, 8)
	ctx := context.Background()
	if err := e.Accept(ctx, &protocol.AlertPack{ObjHash: 42, ObjType: "java", Title: "t", Level: 1, Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
		t.Fatal(err)
	}
	if err := e.processGroup(ctx, &protocol.XLogPack{Group: 7, ObjHash: 1, Elapsed: 5}, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second) // 12:05:00: summary and group save both due
	var hookErr error
	calls := 0
	e.periodicWriteHook = func() {
		calls++
		if len(e.extra.alertSummaries) != 0 {
			hookErr = errors.New("summary still buffered during the write")
			return
		}
		hookErr = acceptWithin(e, &protocol.XLogPack{TxID: 1, Service: 1, ObjHash: 1, Elapsed: 10, TailVersion: 12})
	}
	if err := e.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	e.periodicWriteHook = nil
	if hookErr != nil || calls != 1 {
		t.Fatal(hookErr, calls)
	}
	flush(t, e)
	out, err := s.SummariesContext(ctx, "20261018", now.Add(-5*time.Minute).UnixMilli(), now.UnixMilli(), 3)
	if err != nil || len(out) != 1 || out[0].ObjHash != 42 {
		t.Fatal(out, err)
	}
	values, err := c.Daily("20261018", 1, "cnt:7", 3)
	if err != nil || values[144].Int != 1 {
		t.Fatal(values, err)
	}
	if e.extra.group.nextSave != (now.UnixMilli()/60000+1)*60000 {
		t.Fatal("nextSave", e.extra.group.nextSave)
	}
}

// A failed summary write is counted and not retried (Java queue semantics).
// The group save of the same tick goes to its own writer (DailyCounterWR) and
// is written.
func TestTickSummaryWriteErrorCountsDroppedAndKeepsGroupRetry(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 4, 59, 0, time.UTC)
	e, s, c := testEngine(t, &now, 8)
	ctx := context.Background()
	for _, hash := range []int32{42, 43} {
		if err := e.Accept(ctx, &protocol.AlertPack{ObjHash: hash, ObjType: "java", Title: "t", Level: 1, Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.processGroup(ctx, &protocol.XLogPack{Group: 7, ObjHash: 1, Elapsed: 5}, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	deadline := e.extra.group.nextSave
	now = now.Add(time.Second)
	flush(t, e) // the alerts
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// The state work and the queuing succeed; the tag count marks of the
	// closed store report it (as the visitor/tag roll did before).
	if err := e.Tick(ctx); !errors.Is(err, storage.ErrClosed) {
		t.Fatal(err)
	}
	flush(t, e)
	if st := e.Stats(); st.AlertSummaryDropped != 2 {
		t.Fatal("dropped", st.AlertSummaryDropped)
	}
	if w := e.WriterStats()[writeSummary]; w.WriteErrors != 2 || w.Written != 0 {
		t.Fatalf("summary writer %+v", w)
	}
	if len(e.extra.alertSummaries) != 0 || e.cacheBytes < 0 {
		t.Fatal("summary state", len(e.extra.alertSummaries), e.cacheBytes)
	}
	if e.extra.group.nextSave == deadline {
		t.Fatal("group save kept due although written")
	}
	values, err := c.Daily("20261018", 1, "cnt:7", 3)
	if err != nil || values[144].Int != 1 {
		t.Fatal(values, err)
	}
}

// Ingest runs concurrently with realtime queries, Tick and Sample5M (for
// -race: the query reads ring slots and the periodic writes read their packs
// without the engine lock).
func TestPeriodicAndRealtimeConcurrentWithIngest(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 5, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 64)
	ctx := context.Background()
	done := make(chan error, 1)
	go func() {
		for i := int64(1); i <= 600; i++ {
			var p protocol.Pack = &protocol.XLogPack{TxID: i, Service: 1, ObjHash: 1, Group: 7, Elapsed: int32(i), TailVersion: 12}
			switch i % 3 {
			case 1:
				p = &protocol.PerfCounterPack{ObjName: "/a", Time: now.UnixMilli(), TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: i}}}}}
			case 2:
				p = &protocol.AlertPack{ObjHash: int32(i % 5), ObjType: "java", Title: "t", Level: 1, Tags: protocol.Value{Type: protocol.ValueMap}}
			}
			if err := e.Accept(ctx, p); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for i := 0; i < 50; i++ {
		if _, err := e.XLogsRealtime(ctx, service.RealtimeQuery{Loop: 0, Index: 0}); err != nil {
			t.Fatal(err)
		}
		if err := e.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		if err := e.Sample5M(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// A failed group row write keeps the minute save due and unmarks the meters it
// did not reach.
func TestGroupWriteErrorUnmarksUnwrittenMeters(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	e, _, c := testEngine(t, &now, 4)
	ctx := context.Background()
	for _, obj := range []int32{1, 2} {
		if err := e.processGroup(ctx, &protocol.XLogPack{Group: 7, ObjHash: obj, Elapsed: 5}, now.UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	// Ten minutes later both rolling sums are zero.
	later := now.Add(10 * time.Minute).UnixMilli()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	prev := e.extra.group.nextSave
	if err := e.groupTick(ctx, later); err != nil {
		t.Fatal(err)
	}
	flush(t, e) // the daily writer fails and restores the save
	if e.extra.group.nextSave != prev || len(e.extra.group.empty) != 0 {
		t.Fatal("retry state", e.extra.group.nextSave, prev, e.extra.group.empty)
	}
}
