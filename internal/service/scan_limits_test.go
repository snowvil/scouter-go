package service

import (
	"strconv"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Regression: summary, alert-title and alert-history queries streamed from
// storage have no cap on the rows they read (Java SummaryRD/AlertRD handlers
// aggregate or write as they read). Only what Go keeps is bounded.
func TestSummaryAndAlertScansHaveNoReadCap(t *testing.T) {
	const packs, ids = 101, 1000 // 101000 rows > extendedQueryCells
	idValues, titles, counts := make([]protocol.Value, ids), make([]protocol.Value, ids), make([]protocol.Value, ids)
	for i := range idValues {
		idValues[i], titles[i], counts[i] = decimal(int64(i)), text("t"+strconv.Itoa(i)), decimal(1)
	}
	var summaries []*protocol.SummaryPack
	for i := 0; i < packs; i++ {
		summaries = append(summaries,
			&protocol.SummaryPack{SType: 1, ObjHash: 42, Table: summaryTestTable(map[string][]protocol.Value{"id": idValues, "count": counts, "error": counts, "elapsed": counts})},
			&protocol.SummaryPack{Time: 1767225600000, SType: 3, Table: summaryTestTable(map[string][]protocol.Value{"title": titles, "count": counts, "level": counts})})
	}
	alerts := make([]*protocol.AlertPack, extendedQueryCells+1)
	for i := range alerts {
		alerts[i] = &protocol.AlertPack{Time: 1767225600000, Title: "a", Tags: protocol.Value{Type: protocol.ValueMap}}
	}
	b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{}, summaries: summaries, alerts: alerts}
	_, c, r, results := startPacketQuery(t, b)
	defer func() {
		if t.Failed() {
			for len(results) > 0 {
				t.Log(<-results)
			}
		}
	}()
	// 101000 rows and 100001 alerts: allow for -race on a loaded host.
	c.SetDeadline(time.Now().Add(2 * time.Minute))
	send(t, c, "LOAD_SERVICE_SUMMARY", 1, newMap(entry("date", text("20260101"))))
	p := readPacks(t, r)[0].(*protocol.MapPack)
	if got := lookup(p, "id").Values; len(got) != ids || lookup(p, "count").Values[0].Int != packs {
		t.Fatalf("summary ids=%d", len(got))
	}
	send(t, c, "ALERT_TITLE_COUNT", 1, newMap(entry("date", text("20260101"))))
	if got := readPacks(t, r); len(got) != ids {
		t.Fatalf("alert titles=%d", len(got))
	}
	send(t, c, "ALERT_LOAD_TIME", 1, newMap(entry("date", text("20260101")), entry("stime", decimal(1767225600000)), entry("etime", decimal(1767225660000)), entry("count", decimal(5))))
	if got := readPacks(t, r); len(got) != 6 {
		t.Fatalf("alert history count+1, got %d", len(got))
	}
}
