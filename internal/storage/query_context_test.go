package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestObjectTextQueryLockWaitCancellation(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.mu.Lock()
	for _, call := range []func(context.Context) error{
		func(ctx context.Context) error { _, err := s.ObjectsContext(ctx, "20261013"); return err },
		func(ctx context.Context) error { _, _, err := s.TextContext(ctx, "20261013", "service", 1); return err },
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		done := make(chan error, 1)
		go func() { done <- call(ctx) }()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("query ignored cancellation")
		}
		cancel()
	}
	s.mu.Unlock()
}
