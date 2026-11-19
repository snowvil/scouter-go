package storage

import (
	"testing"
	"time"
)

// /health reads IndexOpenStats while an index walk may hold the Store or
// Counters gate for seconds (a crash restart without checkpoints); reading
// the counts must not wait for that gate.
func TestIndexOpenStatsDoNotWaitForTheGate(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c, err := OpenCounters(t.TempDir(), time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s.mu.Lock()
	c.mu.Lock()
	done := make(chan struct{})
	go func() {
		s.IndexOpenStats()
		c.IndexOpenStats()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("IndexOpenStats waited for the gate")
	}
	c.mu.Unlock()
	s.mu.Unlock()
	<-done
}
