package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestVisitorPersistenceCurrentMergeAndCancellation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	s, err := Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct {
		u int64
		h int32
	}{{1, 42}, {2, 42}, {2, 43}, {3, 43}} {
		if err = s.PutVisitorContext(ctx, now, "java", v.u, v.h); err != nil {
			t.Fatal(err)
		}
	}
	n, err := s.VisitorRealtimeContext(ctx, now, []int32{42, 43}, "")
	if err != nil || n != 3 {
		t.Fatal(n, err)
	}
	n, err = s.VisitorDailyContext(ctx, "20261013", []int32{42, 43}, "")
	if err != nil || n != 0 {
		t.Fatal("unflushed disk", n, err)
	}
	if err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	n, err = s.VisitorHourlyContext(ctx, "20261013", now, []int32{42, 43})
	if err != nil || n != 3 {
		t.Fatal(n, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	n, err = s.VisitorDailyContext(ctx, "20261013", nil, "java")
	if err != nil || n != 3 {
		t.Fatal(n, err)
	}
	// Pinned hourly double-suffix load loses old hourly users upon first post-restart offer.
	if err = s.PutVisitorContext(ctx, now, "java", 99, 42); err != nil {
		t.Fatal(err)
	}
	if err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	n, err = s.VisitorHourlyContext(ctx, "20261013", now, []int32{42})
	if err != nil || n != 1 {
		t.Fatal("source restart suffix", n, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = s.VisitorDailyContext(cancelled, "20261013", nil, "java")
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = s.PutVisitorContext(ctx, now, "../escape", 1, 42); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
func TestVisitorFIFOEvictionFlushAndRollover(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := time.Date(2026, 10, 13, 23, 59, 59, 0, time.UTC).UnixMilli()
	for i := 0; i < 101; i++ {
		if err = s.PutVisitorContext(ctx, tm, fmt.Sprintf("java%d", i), 1, 42); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.visitor.daily) != 101 {
		t.Fatal("100 type + one object cap", len(s.visitor.daily))
	}
	n, err := s.VisitorDailyContext(ctx, "20261013", nil, "java0")
	if err != nil || n != 1 {
		t.Fatal("dirty eviction not persisted", n, err)
	}
	if err = s.VisitorSyncContext(ctx, tm+1000); err != nil {
		t.Fatal(err)
	}
	n, err = s.VisitorDailyContext(ctx, "20261013", nil, "java100")
	if err != nil || n != 0 {
		t.Fatal("source rollover clears before flush", n, err)
	}
}

// A visit that changes no HLL register leaves the entry clean, so the 10s flush
// does not rewrite an identical snapshot (Java rewrites it; the bytes are equal).
func TestVisitorRepeatVisitDoesNotDirty(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.PutVisitorContext(ctx, now, "java", 1, 42); err != nil {
		t.Fatal(err)
	}
	if err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	dirty := func() int {
		s.mu.Lock()
		defer s.mu.Unlock()
		n := 0
		for _, set := range []map[string]*visitorEntry{s.visitor.daily, s.visitor.hourly} {
			for _, e := range set {
				if e.dirty {
					n++
				}
			}
		}
		return n
	}
	if n := dirty(); n != 0 {
		t.Fatalf("dirty after sync: %d", n)
	}
	if err = s.PutVisitorContext(ctx, now, "java", 1, 42); err != nil {
		t.Fatal(err)
	}
	if n := dirty(); n != 0 {
		t.Fatalf("repeat visit dirtied %d entries", n)
	}
	if err = s.PutVisitorContext(ctx, now, "java", 2, 42); err != nil {
		t.Fatal(err)
	}
	if n := dirty(); n != 3 {
		t.Fatalf("new visitor dirtied %d entries, want daily type+object and hourly", n)
	}
}

// A synced snapshot is rewritten page by page in place; the file stays equal
// to the full image. A missing or short file falls back to a full rewrite.
func TestVisitorPageRewriteMatchesFullImage(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	s, err := Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	path := filepath.Join(dir, "20261013", "visit", "java.usr")
	check := func(step string) {
		t.Helper()
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(step, err)
		}
		s.mu.Lock()
		want := s.visitor.daily["t:java"].hll.bytes()
		s.mu.Unlock()
		if !bytes.Equal(got, want) {
			t.Fatal(step, "file differs from the HLL image")
		}
	}
	put := func(from, to int64) {
		for u := from; u < to; u++ {
			if err := s.PutVisitorContext(ctx, now, "java", u, 42); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.flushVisitors(ctx); err != nil {
			t.Fatal(err)
		}
	}
	put(1, 50) // first flush: full rewrite
	check("full")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	put(50, 60) // a few pages
	check("pages")
	after, err := os.Stat(path)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("a synced snapshot must be rewritten in place, not replaced", err)
	}
	if err = os.Truncate(path, 10); err != nil {
		t.Fatal(err)
	}
	put(60, 70)
	check("short file rewritten whole")
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	put(70, 80)
	check("missing file rewritten whole")
}

// A flush pass writes each entry dirty at its start at most once, so it ends
// while new visitors keep arriving.
func TestVisitorFlushPassEndsUnderSteadyVisits(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for h := int32(1); h <= 20; h++ {
		if err = s.PutVisitorContext(ctx, now, "java", int64(h), h); err != nil {
			t.Fatal(err)
		}
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for u := int64(1000); ; u++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := s.PutVisitorContext(ctx, now, "java", u, int32(u%20)+1); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	finished := make(chan error, 1)
	go func() { finished <- s.flushVisitors(ctx) }()
	select {
	case err = <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("flush pass did not end under steady visits")
	}
	close(stop)
	<-done
	if err != nil {
		t.Fatal(err)
	}
}

// VisitorSyncContext runs both halves of the engine tick, VisitorRollContext
// and TagTickContext, in one Store gate hold. It only rolls the visitor
// date/hour and marks visitor and tag work due; the I/O runs in
// FlushVisitorsIfDue/FlushTagCountsIfDue.
func (s *Store) VisitorSyncContext(ctx context.Context, now int64) error {
	if err := s.mu.LockContext(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.visitorMu.Lock()
	err := s.visitorTickLocked(ctx, now)
	s.visitorMu.Unlock()
	return errors.Join(err, s.tagTickLocked(ctx, now))
}
