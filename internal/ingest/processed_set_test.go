package ingest

import (
	"container/list"
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// listProcessed is the container/list processed gxid set processedSet
// replaced (the earlier addProcessed), kept as the oracle.
type listProcessed struct {
	entries map[int64]*list.Element
	order   list.List
	maximum int
}

func (l *listProcessed) add(txid int64, discard int32) {
	if entry := l.entries[txid]; entry != nil {
		entry.Value.(*processedTrace).discard += discard
		return
	}
	if len(l.entries) >= l.maximum {
		first := l.order.Front()
		delete(l.entries, first.Value.(*processedTrace).txid)
		l.order.Remove(first)
	}
	l.entries[txid] = l.order.PushBack(&processedTrace{txid, discard})
}

type processedTrace struct {
	txid    int64
	discard int32
}

// The ring and map give the list's answers for every key after every add:
// presence (isProcessedGxid), the accumulated flag (isProcessedGxidWithProfile)
// and the count, for bounds from 1 to past the keys used, with re-added keys
// (LongIntLinkedMap.add accumulates in place, no move) and evictions mixed.
func TestProcessedSetMatchesListOracle(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for _, maximum := range []int{1, 2, 3, 7, 64, 1000, 5000} {
		for _, keys := range []int64{4, 50, 3000} {
			oracle := &listProcessed{entries: map[int64]*list.Element{}, maximum: maximum}
			set := newProcessedSet(maximum)
			for step := 0; step < 20000; step++ {
				txid := r.Int64N(keys) - keys/4 // negative and 0 txids too
				discard := int32(r.IntN(6))
				if _, ok := set.get(txid); !ok {
					set.push(txid, discard)
				} else {
					set.accumulate(txid, discard)
				}
				oracle.add(txid, discard)
				if set.len() != len(oracle.entries) || set.full() != (len(oracle.entries) >= maximum) {
					t.Fatalf("max %d keys %d step %d: len %d want %d", maximum, keys, step, set.len(), len(oracle.entries))
				}
				if step%97 != 0 && keys > 50 {
					continue
				}
				for k := -keys / 4; k < keys-keys/4; k++ {
					d, ok := set.get(k)
					var want int32
					entry, wantOK := oracle.entries[k]
					if wantOK {
						want = entry.Value.(*processedTrace).discard
					}
					if ok != wantOK || d != want {
						t.Fatalf("max %d keys %d step %d txid %d: got %d,%v want %d,%v", maximum, keys, step, k, d, ok, want, wantOK)
					}
				}
			}
			// Eviction order: the oldest first, as the list's front.
			for e := oracle.order.Front(); e != nil; e = e.Next() {
				pt := e.Value.(*processedTrace)
				if _, ok := set.get(pt.txid); !ok {
					t.Fatalf("max %d: %d missing", maximum, pt.txid)
				}
			}
		}
	}
}

// The ring grows with the set up to its bound, not past it.
func TestProcessedSetRingCapacity(t *testing.T) {
	set := newProcessedSet(3000)
	for i := range int64(10000) {
		set.push(i, 1)
		if cap(set.order) > 3000 {
			t.Fatalf("ring capacity %d past the bound at %d", cap(set.order), i)
		}
	}
	if set.len() != 3000 {
		t.Fatal(set.len())
	}
	for i := range int64(10000) {
		if _, ok := set.get(i); ok != (i >= 7000) {
			t.Fatalf("txid %d kept %v", i, ok)
		}
	}
}

func drivingXLog(txid int64, discard byte) *protocol.XLogPack {
	return &protocol.XLogPack{TxID: txid, Service: 1, ObjHash: 1, Elapsed: 1, DiscardType: discard, TailVersion: protocol.XLogCurrentTail}
}

// XLogPreCore/ProfilePreCore through Accept: a child of a remembered driving
// XLog is stored at once, a child of an evicted one is delayed, a re-sent
// driving XLog accumulates its discard flag (discardNone twice is no longer
// "with profile"), the bound is the startup one, and a new engine (restart)
// remembers nothing.
func TestProcessedGxidSemantics(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	ctx := context.Background()
	e, _ := testEngineWith(t, &now, func(c *Config) { c.SamplingGxidKeepCount = 2 })
	for _, txid := range []int64{10, 20, 30} {
		if err := e.Accept(ctx, drivingXLog(txid, discardNone)); err != nil {
			t.Fatal(err)
		}
	}
	state := e.extra.sampling
	if _, ok := state.processed.get(10); ok || state.processed.len() != 2 {
		t.Fatal("oldest not evicted", state.processed.len())
	}
	for _, gxid := range []int64{10, 20} {
		if err := e.Accept(ctx, delayedXLog(gxid+100, gxid)); err != nil {
			t.Fatal(err)
		}
	}
	if state.xlogCurrent.entries[10] == nil || state.xlogCurrent.entries[20] != nil {
		t.Fatal("child of the evicted gxid not delayed, or child of a kept one delayed")
	}
	// A live settings change does not move the startup bound.
	s := DefaultSettings()
	e.SetSettings(s)
	if err := e.Accept(ctx, drivingXLog(40, discardNone)); err != nil {
		t.Fatal(err)
	}
	if _, ok := state.processed.get(20); ok || state.processed.len() != 2 {
		t.Fatal("bound changed after SetSettings", state.processed.len())
	}
	// Re-sent driving XLog: 1+1 = 2 is not discardNone, so its delayed
	// child profile is no longer released by the processed check.
	if err := e.Accept(ctx, drivingXLog(40, discardNone)); err != nil {
		t.Fatal(err)
	}
	if d, _ := state.processed.get(40); d != 2 || state.processed.len() != 2 {
		t.Fatal("accumulated", d, state.processed.len())
	}
	if err := e.Accept(ctx, delayedProfile(140, 40)); err != nil {
		t.Fatal(err)
	}
	if state.profileCurrent.entries[40] == nil {
		t.Fatal("profile of an accumulated gxid released")
	}
	if err := e.Accept(ctx, delayedProfile(130, 30)); err != nil {
		t.Fatal(err)
	}
	if state.profileCurrent.entries[30] != nil {
		t.Fatal("profile of a discardNone gxid delayed")
	}
	restarted, _ := testEngineWith(t, &now, func(c *Config) { c.SamplingGxidKeepCount = 2 })
	if restarted.extra.sampling.processed.len() != 0 {
		t.Fatal("processed gxids survive a restart")
	}
	if err := restarted.Accept(ctx, delayedXLog(130, 30)); err != nil {
		t.Fatal(err)
	}
	if restarted.extra.sampling.xlogCurrent.entries[30] == nil {
		t.Fatal("child after restart not delayed")
	}
}

// Past the byte budget a new gxid is not remembered (XLogCacheSkipped) and
// the accounting is 40 bytes per entry; at the bound the replacement costs
// nothing, so it still evicts when the budget is spent.
func TestProcessedGxidBudget(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.SamplingGxidKeepCount = 3 })
	e.mu.Lock()
	defer e.mu.Unlock()
	base := e.cacheBytes
	e.config.MaxCacheBytes = base + e.ringBytes + 80
	for txid := int64(1); txid <= 3; txid++ {
		e.addProcessed(txid, discardNone)
	}
	set := &e.extra.sampling.processed
	if set.len() != 2 || e.cacheBytes != base+80 || e.Stats().XLogCacheSkipped != 1 {
		t.Fatal("budget", set.len(), e.cacheBytes-base, e.Stats().XLogCacheSkipped)
	}
	e.config.MaxCacheBytes += 40
	e.addProcessed(3, discardNone)
	e.addProcessed(4, discardNone) // at the bound: replaces 1 for no bytes
	if _, ok := set.get(1); ok || set.len() != 3 || e.cacheBytes != base+120 || e.Stats().XLogCacheSkipped != 1 {
		t.Fatal("replacement", set.len(), e.cacheBytes-base, e.Stats().XLogCacheSkipped)
	}
	e.addProcessed(4, discardNone) // present: accumulates, no bytes
	if d, _ := set.get(4); d != 2 || e.cacheBytes != base+120 {
		t.Fatal("accumulate", d, e.cacheBytes-base)
	}
}
