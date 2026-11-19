package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/transport"
)

func TestRemoveObjectsInMemoryAndInactive(t *testing.T) {
	now := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)
	e, s, _ := testEngine(t, &now, 3)
	ctx := context.Background()
	a := &protocol.ObjectPack{ObjName: "/a", ObjType: "java", Tags: protocol.Value{Type: protocol.ValueMap}}
	b := &protocol.ObjectPack{ObjName: "/b", ObjType: "java", Tags: protocol.Value{Type: protocol.ValueMap}}
	batch := &protocol.ObjectPack{ObjName: "/batch", ObjType: "batch", Tags: protocol.Value{Type: protocol.ValueMap}}
	for _, p := range []*protocol.ObjectPack{a, b, batch} {
		if err := e.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	hashA, hashB := storage.CounterHash("/a"), storage.CounterHash("/b")
	before := e.cacheBytes
	if err := e.RemoveObjects(ctx, []int32{hashA, 12345}, false); err != nil {
		t.Fatal(err)
	}
	if e.ObjectCount() != 2 || e.cacheBytes >= before {
		t.Fatalf("in-memory removal: count=%d bytes %d -> %d", e.ObjectCount(), before, e.cacheBytes)
	}
	flush(t, e)
	stored, err := s.Objects("20261016")
	if err != nil || len(stored) != 3 {
		t.Fatalf("in-memory removal must not touch the stored record: %v %v", stored, err)
	}
	// The removed object is reinstated by its next heartbeat, like Java objMap.put.
	if err := e.Accept(ctx, a); err != nil {
		t.Fatal(err)
	}
	if e.ObjectCount() != 3 {
		t.Fatal("heartbeat after removal must re-add the object")
	}
	now = now.Add(9 * time.Second)
	if err := e.Accept(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := e.RemoveInactiveObjects(ctx); err != nil {
		t.Fatal(err)
	}
	live, err := e.Objects(ctx, "")
	if err != nil || len(live) != 2 {
		t.Fatalf("inactive removal kept %d objects: %v", len(live), err)
	}
	for _, p := range live {
		if p.ObjHash != hashB && p.ObjType != "batch" {
			t.Fatalf("unexpected survivor %+v", p)
		}
	}
}

func TestRemoveObjectsPermanentNeedsStorageSupport(t *testing.T) {
	now := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 3)
	ctx := context.Background()
	if _, ok := any(e.store).(objectStoreRemover); ok {
		t.Skip("storage implements RemoveObject; the permanent path is covered by the storage interop tests")
	}
	err := e.RemoveObjects(ctx, []int32{1}, true)
	if !errors.Is(err, transport.ErrUnsupported) {
		t.Fatalf("permanent removal without storage support must be explicit, got %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := e.RemoveInactiveObjects(cancelled); err == nil {
		t.Fatal("cancelled context must be honoured")
	}
}

func TestDeleteDatabasePathsDropsTodayCache(t *testing.T) {
	now := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)
	e, s, c := testEngine(t, &now, 3)
	if err := s.AttachCounters(c); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := e.Accept(ctx, &protocol.ObjectPack{ObjName: "/a", ObjType: "java", Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteDatabasePaths(ctx, []string{"lock.dat"}); !errors.Is(err, storage.ErrInvalid) {
		t.Fatalf("lock.dat must be refused, got %v", err)
	}
	if e.ObjectCount() != 1 {
		t.Fatal("refused delete must not touch the cache")
	}
	if err := e.DeleteDatabasePaths(ctx, []string{"20261016"}); err != nil {
		t.Fatal(err)
	}
	if e.ObjectCount() != 0 {
		t.Fatal("deleting today's directory must drop the live object map")
	}
	flush(t, e)
	stored, err := s.Objects("20261016")
	if err != nil || len(stored) != 0 {
		t.Fatalf("date directory survived: %v %v", stored, err)
	}
	if err := e.DeleteDatabasePaths(ctx, []string{"20261017"}); err != nil {
		t.Fatalf("missing path must be a Java-style no-op, got %v", err)
	}
}
