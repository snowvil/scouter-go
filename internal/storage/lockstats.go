package storage

import (
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/syncx"
)

// LockStat is one lock's, or one Store gate holder's, measurement-only sums
// since TrackLocks: acquisitions (or holds), time held and time waited.
type LockStat struct {
	Acquired uint64  `json:"acquired"`
	HeldMs   float64 `json:"heldMs"`
	WaitedMs float64 `json:"waitedMs"`
}

func lockStat(s syncx.Stats) LockStat {
	return LockStat{Acquired: s.Acquired, HeldMs: ms(s.Held), WaitedMs: ms(s.Waited)}
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// holdSum sums the Store gate holds of one writer.
type holdSum struct {
	holds atomic.Uint64
	ns    atomic.Int64
}

func (h *holdSum) add(d time.Duration) {
	h.holds.Add(1)
	h.ns.Add(int64(d))
}

func (h *holdSum) stat() LockStat {
	return LockStat{Acquired: h.holds.Load(), HeldMs: ms(time.Duration(h.ns.Load()))}
}

// TrackLocks starts the measurement-only sums LockStats reports: the Store
// gate, the tag gate, and the Store gate time held by each writer (so the
// gate's occupancy can be split by holder). Tracking costs two clock reads
// per acquisition.
func (s *Store) TrackLocks() {
	s.mu.Track()
	s.tags.mu.Track()
	s.lockStats.Store(true)
}

// LockStats is nil until TrackLocks. "store" and "tags" are the gates;
// "store.xlogWriter", "store.profileWriter" and "store.textWriter" are the
// writers' shares of the Store gate's held time.
func (s *Store) LockStats() map[string]LockStat {
	if !s.lockStats.Load() {
		return nil
	}
	return map[string]LockStat{
		"store":               lockStat(s.mu.Stats()),
		"tags":                lockStat(s.tags.mu.Stats()),
		"store.xlogWriter":    s.xlogQ.held.stat(),
		"store.profileWriter": s.profQ.held.stat(),
		"store.textWriter":    s.texts.held.stat(),
	}
}
