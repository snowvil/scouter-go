package syncx

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCanceledWaiterPreservesOwner(t *testing.T) {
	var m Mutex
	m.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.LockContext(ctx) }()
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	m.Unlock()
	if err := m.LockContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.Unlock()
}
func TestMutexConcurrentZeroValue(t *testing.T) {
	var m Mutex
	var workers sync.WaitGroup
	count := 0
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				m.Lock()
				count++
				m.Unlock()
			}
		}()
	}
	workers.Wait()
	if count != 800 {
		t.Fatal(count)
	}
}

// A tracked Mutex sums acquisitions, held and waited time; Track may start
// while the Mutex is held, and an untracked Mutex reports zero.
func TestMutexTrackSumsHeldAndWaited(t *testing.T) {
	var untracked Mutex
	untracked.Lock()
	untracked.Unlock()
	if s := untracked.Stats(); s != (Stats{}) {
		t.Fatal(s)
	}
	var m Mutex
	m.Lock()
	m.Track() // the current owner was not tracked: its hold is not counted
	m.Unlock()
	m.Lock()
	waited := make(chan struct{})
	go func() {
		m.Lock()
		close(waited)
		m.Unlock()
	}()
	time.Sleep(30 * time.Millisecond)
	m.Unlock()
	<-waited
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	m.Lock()
	if err := m.LockContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	m.Unlock()
	s := m.Stats()
	if s.Acquired != 3 || s.Held < 30*time.Millisecond || s.Waited < 35*time.Millisecond {
		t.Fatalf("%+v", s)
	}
}

func TestTryLock(t *testing.T) {
	var m Mutex
	m.Track()
	if !m.TryLock() {
		t.Fatal("free Mutex not taken")
	}
	if m.TryLock() {
		t.Fatal("held Mutex taken twice")
	}
	m.Unlock()
	if !m.TryLock() {
		t.Fatal("released Mutex not taken")
	}
	m.Unlock()
	if st := m.Stats(); st.Acquired != 2 {
		t.Fatalf("acquired %d, want 2", st.Acquired)
	}
}
