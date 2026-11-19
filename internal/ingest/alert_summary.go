package ingest

import (
	"context"
	"github.com/snowvil/scouter-go/internal/protocol"
	"sort"
)

// takeAlertSummariesLocked is the engine-locked half of the five-minute
// AlertSummary.doFlush: like Java (master swapped for a new map before the
// SummaryPacks are built and queued) it builds every pack and clears the
// summaries. The caller queues the packs with queueAlertSummaries after
// releasing Engine.mu.
func (e *Engine) takeAlertSummariesLocked(now int64) []*protocol.SummaryPack {
	if len(e.extra.alertSummaries) == 0 {
		return nil
	}
	packs := make([]*protocol.SummaryPack, 0, len(e.extra.alertSummaries))
	for hash, summary := range e.extra.alertSummaries {
		names := append([]string(nil), summary.titles...)
		capacity := 16
		for len(names) > capacity*3/4 {
			capacity *= 2
		}
		sort.SliceStable(names, func(i, j int) bool {
			a, b := javaStringHash(names[i]), javaStringHash(names[j])
			a ^= a >> 16
			b ^= b >> 16
			return a&uint32(capacity-1) < b&uint32(capacity-1)
		})
		counts, titles, levels := []protocol.Value{}, []protocol.Value{}, []protocol.Value{}
		for _, name := range names {
			count := summary.counts[name]
			counts = append(counts, protocol.Value{Type: protocol.ValueDecimal, Int: int64(count.count)})
			titles = append(titles, protocol.Value{Type: protocol.ValueText, Text: name})
			levels = append(levels, protocol.Value{Type: protocol.ValueDecimal, Int: int64(int8(count.level))})
		}
		p := &protocol.SummaryPack{Time: (now - 10000) / 300000 * 300000, ObjHash: hash, ObjType: summary.objType, SType: 3, Table: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "count", Value: protocol.Value{Type: protocol.ValueList, Values: counts}}, {Key: "title", Value: protocol.Value{Type: protocol.ValueList, Values: titles}}, {Key: "level", Value: protocol.Value{Type: protocol.ValueList, Values: levels}}}}}
		packs = append(packs, p)
		e.cacheBytes -= len(summary.objType) + 16
		for name := range summary.counts {
			e.cacheBytes -= len(name) + 16
		}
		delete(e.extra.alertSummaries, hash)
	}
	return packs
}

// queueAlertSummaries hands the packs of takeAlertSummariesLocked to the
// summary writer as one item (in order). A write error ends the pass (counted
// and logged by the writer); the packs not yet written are not retried (Java
// SummaryCore queue semantics) and are counted in Stats.AlertSummaryDropped,
// as are all of them when the summary queue is full.
func (e *Engine) queueAlertSummaries(packs []*protocol.SummaryPack) {
	if len(packs) == 0 {
		return
	}
	size := 0
	for _, p := range packs {
		size += packHeapBytes(p)
	}
	if e.w.enqueuePass(writeSummary, passAlertSummary, len(packs), size, func(ctx context.Context) (int, error) {
		for i, p := range packs {
			if err := e.store.PutSummaryContext(ctx, p); err != nil {
				e.alertSummaryDropped.Add(uint64(len(packs) - i))
				return i, err
			}
		}
		return len(packs), nil
	}) != nil {
		e.alertSummaryDropped.Add(uint64(len(packs)))
	}
}
