// Package syncx supplies the cancellation boundary used by ingest and storage.
package syncx

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Mutex admits one owner. Its zero value is ready to use. Do not copy after use.
type Mutex struct {
	once  sync.Once
	gate  chan struct{}
	track atomic.Pointer[tracker] // nil unless Track was called
}

// Stats sums a tracked Mutex's acquisitions, the time it was held and the
// time callers waited for it (canceled waits included).
type Stats struct {
	Acquired     uint64
	Held, Waited time.Duration
}

type tracker struct {
	acquired     atomic.Uint64
	held, waited atomic.Int64 // nanoseconds
	since        atomic.Int64 // when the current owner took it (0: untracked owner)
}

// epoch keeps tracked times small and monotonic.
var epoch = time.Now()

func now() int64 { return int64(time.Since(epoch)) + 1 }

func (m *Mutex) init() { m.once.Do(func() { m.gate = make(chan struct{}, 1) }) }

// Track starts summing Stats (measurement only: two clock reads per
// acquisition). It may be called while the Mutex is in use.
func (m *Mutex) Track() { m.track.CompareAndSwap(nil, new(tracker)) }

// Stats reports the sums since Track; zero when not tracked.
func (m *Mutex) Stats() Stats {
	t := m.track.Load()
	if t == nil {
		return Stats{}
	}
	return Stats{Acquired: t.acquired.Load(), Held: time.Duration(t.held.Load()), Waited: time.Duration(t.waited.Load())}
}

func (m *Mutex) Lock() {
	m.init()
	t := m.track.Load()
	if t == nil {
		m.gate <- struct{}{}
		return
	}
	start := now()
	m.gate <- struct{}{}
	t.took(start)
}

func (m *Mutex) Unlock() {
	if t := m.track.Load(); t != nil {
		if since := t.since.Swap(0); since != 0 {
			t.held.Add(now() - since)
		}
	}
	<-m.gate
}

func (t *tracker) took(start int64) {
	n := now()
	t.waited.Add(n - start)
	t.since.Store(n)
	t.acquired.Add(1)
}

// LockContext leaves an active owner undisturbed when the waiter is canceled.
func (m *Mutex) LockContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.init()
	t := m.track.Load()
	var start int64
	if t != nil {
		start = now()
	}
	select {
	case m.gate <- struct{}{}:
		if t != nil {
			t.took(start)
		}
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		if t != nil {
			t.waited.Add(now() - start)
		}
		return ctx.Err()
	}
}
