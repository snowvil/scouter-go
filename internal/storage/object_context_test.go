package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

// PutObjectContext gives up waiting for a held Store gate when its context
// ends (the ingest object writer at the Close deadline) and writes nothing;
// with the gate free it writes like PutObject.
func TestPutObjectContextWaitsForGateWithContext(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err = s.PutObjectContext(ctx, "20261021", objectWithHash(1, "obj"))
	cancel()
	s.mu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("held gate: %v", err)
	}
	if out, err := s.Objects("20261021"); err != nil || len(out) != 0 {
		t.Fatal("written although the gate wait was cancelled", out, err)
	}
	if err := s.PutObjectContext(context.Background(), "20261021", objectWithHash(1, "obj")); err != nil {
		t.Fatal(err)
	}
	if out, err := s.Objects("20261021"); err != nil || len(out) != 1 || out[0].ObjHash != 1 {
		t.Fatal(out, err)
	}
}

// The visitor half of the engine tick (VisitorRollContext) needs only the
// visitor lock, so a held Store gate does not stall it (nor the visits the
// ingest visitor writer queues behind it); the tag half (TagTickContext)
// still waits for the gate, with its context.
func TestVisitorRollWithoutGateTagTickWithGate(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC).UnixMilli()
	s.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	rollErr := s.VisitorRollContext(ctx, now)
	cancel()
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	tagErr := s.TagTickContext(ctx, now)
	cancel()
	s.mu.Unlock()
	if rollErr != nil {
		t.Fatal("visitor roll waited for the gate:", rollErr)
	}
	if !errors.Is(tagErr, context.DeadlineExceeded) {
		t.Fatal("tag tick without the gate:", tagErr)
	}
	if err := s.TagTickContext(context.Background(), now+1000); err != nil {
		t.Fatal(err)
	}
	if err := s.PutVisitorContext(context.Background(), now+1000, "java", 7, 1); err != nil {
		t.Fatal(err)
	}
	if n, err := s.VisitorRealtimeContext(context.Background(), now+1000, []int32{1}, ""); err != nil || n != 1 {
		t.Fatal(n, err)
	}
}
