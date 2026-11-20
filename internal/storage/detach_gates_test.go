package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// A purge or DB delete needs the Store gate, the visitor locks, the Counters
// gate and the tag gate together. While one of the later ones is busy (a
// visit writing its snapshot, a visitor snapshot write, a counter write, a tag
// pass writing its top100), it must not sit on the earlier ones: ingest takes
// the Store gate for every record and visitorMu for every visit.
func TestDetachDoesNotHoldIngestGatesWhileWaiting(t *testing.T) {
	const busy = 400 * time.Millisecond
	old := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		lock   func(s *Store, c *Counters)
		unlock func(s *Store, c *Counters)
	}{
		{"visitorMu", func(s *Store, _ *Counters) { s.visitorMu.Lock() }, func(s *Store, _ *Counters) { s.visitorMu.Unlock() }},
		{"visitorIO", func(s *Store, _ *Counters) { s.visitorIO.Lock() }, func(s *Store, _ *Counters) { s.visitorIO.Unlock() }},
		{"counters", func(_ *Store, c *Counters) { c.mu.Lock() }, func(_ *Store, c *Counters) { c.mu.Unlock() }},
		{"tags", func(s *Store, _ *Counters) { s.tags.mu.Lock() }, func(s *Store, _ *Counters) { s.tags.mu.Unlock() }},
	}
	for _, tc := range cases {
		for _, kind := range []string{"purge", "delete"} {
			t.Run(tc.name+"/"+kind, func(t *testing.T) {
				s, c, _ := retentionFixture(t)
				if err := s.PutXLog(&protocol.XLogPack{EndTime: old, TxID: 1, GxID: 7}); err != nil {
					t.Fatal(err)
				}
				date := JavaDate(old, time.UTC)
				tc.lock(s, c)
				done := make(chan error, 1)
				go func() {
					if kind == "purge" {
						r := &Retention{Store: s, Counters: c, Config: RetentionConfig{XLogDays: 5}}
						actions, err := r.RunOnce(context.Background(), now)
						// The purge log says which gate was busy and for how long.
						if err == nil && (len(actions) != 1 || !strings.Contains(actions[0].Gates, "waited") || !strings.Contains(actions[0].Gates, "held")) {
							err = fmt.Errorf("gate timing not reported: %+v", actions)
						}
						done <- err
						return
					}
					d := &DatabaseDeleter{Store: s, Counters: c}
					_, err := d.Delete(context.Background(), []string{"/" + date + "/xlog"})
					done <- err
				}()
				// Ingest keeps taking its gates while the detach waits.
				var worst time.Duration
				deadline := time.Now().Add(busy)
				for time.Now().Before(deadline) {
					start := time.Now()
					probe, cancel := context.WithTimeout(context.Background(), busy)
					err := s.mu.LockContext(probe)
					cancel()
					if err == nil {
						// The test itself holds visitorMu in that case.
						if tc.name != "visitorMu" {
							if s.visitorMu.TryLock() {
								s.visitorMu.Unlock()
							} else {
								err = context.DeadlineExceeded
							}
						}
						s.mu.Unlock()
					}
					if d := time.Since(start); d > worst {
						worst = d
					}
					if err != nil {
						break // still blocked when the lock is released: worst says how long
					}
					time.Sleep(time.Millisecond)
				}
				early := false
				var earlyErr error
				select {
				case earlyErr = <-done:
					early = true
				default:
				}
				tc.unlock(s, c)
				if early {
					t.Fatalf("detach finished while %s was held: %v", tc.name, earlyErr)
				}
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("detach did not finish after the lock was released")
				}
				if worst > busy/4 {
					t.Fatalf("ingest gates were blocked %v while the detach waited for %s", worst, tc.name)
				}
			})
		}
	}
}

// purge is purgeTimed without the gate timing, for the tests that purge one
// mode of one date directly.
func (r *Retention) purge(ctx context.Context, date, today string, mode PurgeMode) (bool, error) {
	removed, _, err := r.purgeTimed(ctx, date, today, mode)
	return removed, err
}
