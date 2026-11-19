package storage

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// gateHoldLimit is the PutXLog latency a concurrent range scan may cause.
// A scan holds the store gate for at most scanHoldTime (plus one record or
// index step) at a time, so ingest waits milliseconds; the bound leaves room
// for -race and a loaded host.
func gateHoldLimit() time.Duration {
	if v, err := strconv.Atoi(os.Getenv("SCOUTER_GATE_HOLD_LIMIT_MS")); err == nil && v > 0 {
		return time.Duration(v) * time.Millisecond
	}
	return 150 * time.Millisecond
}

// measureIngestDuringScan runs scan while it times PutXLog calls; it returns
// the scan duration and the largest PutXLog latency seen while the scan ran.
func measureIngestDuringScan(t *testing.T, s *Store, scan func() error) (time.Duration, time.Duration) {
	t.Helper()
	var holdMu sync.Mutex
	var holds int
	var longest time.Duration
	scanHoldObserver = func(d time.Duration) {
		holdMu.Lock()
		holds++
		longest = max(longest, d)
		holdMu.Unlock()
	}
	defer func() {
		scanHoldObserver = nil
		holdMu.Lock()
		t.Logf("scan gate holds: %d, longest %v", holds, longest)
		holdMu.Unlock()
	}()
	done := make(chan error, 1)
	started := time.Now()
	go func() { done <- scan() }()
	var worst time.Duration
	tx := int64(1 << 40)
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			return time.Since(started), worst
		default:
		}
		tx++
		begin := time.Now()
		// A later Java date's bucket: never visited by the scan.
		if err := s.PutXLog(&protocol.XLogPack{EndTime: scanBase + 2*86400000, TxID: tx, ObjHash: 9}); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(begin); d > worst {
			worst = d
		}
		time.Sleep(200 * time.Microsecond)
	}
}

// Regression: one Client range query blocked ingest for seconds.
// A scan whose filter rejects every record kept reading under the store gate
// until 16 MiB were decoded, and a V1 bucket was read whole under the gate.
func TestRangeScanGateHoldIsBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("inserts 120000 XLogs")
	}
	const spread, bucket = 60000, 60000
	s := scanTestStore(t, Config{})
	// spread: one hour of XLogs; bucket: one 500 ms bucket of 60000 keys.
	putScanXLogs(t, s, spread+bucket, func(i int) int64 {
		if i < spread {
			return scanBase + 1000 + int64(i)*59
		}
		return scanBase + 250
	}, func(int) int32 { return 1 })
	date := JavaDate(scanBase, time.UTC)
	for _, tc := range []struct {
		name  string
		scan  XLogScan
		count int
	}{
		{"V1 filter rejects all", XLogScan{From: scanBase, To: scanBase + 3600000}, 0},
		{"V1 reverse", XLogScan{From: scanBase, To: scanBase + 3600000, Reverse: true}, 0},
		{"V2 filter rejects all", XLogScan{From: scanBase, To: scanBase + 3600000, Paged: true}, 0},
	} {
		q := tc.scan
		q.Date = date
		scanned := 0
		q.Match = func(*protocol.XLogPack) bool { scanned++; return false }
		took, worst := measureIngestDuringScan(t, s, func() error {
			return s.ScanXLogsContext(context.Background(), q, func(XLogRecord) (bool, error) { return true, nil })
		})
		t.Logf("%s: scanned %d in %v, worst concurrent PutXLog %v", tc.name, scanned, took, worst)
		if scanned != spread+bucket {
			t.Fatalf("%s: scanned %d", tc.name, scanned)
		}
		if worst > gateHoldLimit() {
			t.Errorf("%s: PutXLog waited %v behind the scan (limit %v)", tc.name, worst, gateHoldLimit())
		}
	}
	// Status/Summary/Alert scans share the cursor.
	ctx := context.Background()
	for i := 0; i < bucket; i++ {
		if err := s.PutSummaryContext(ctx, &protocol.SummaryPack{Time: scanBase + 250, SType: 1, Table: mapValue()}); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	took, worst := measureIngestDuringScan(t, s, func() error {
		return s.ScanSummariesContext(ctx, date, scanBase, scanBase+3600000, 1, func(*protocol.SummaryPack) (bool, error) { n++; return true, nil })
	})
	t.Logf("summary bucket: %d in %v, worst concurrent PutXLog %v", n, took, worst)
	if n != bucket || worst > gateHoldLimit() {
		t.Errorf("summary scan: n=%d worst PutXLog %v (limit %v)", n, worst, gateHoldLimit())
	}
}
