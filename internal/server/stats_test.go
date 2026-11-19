package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Bucket boundaries are monotonic and every microsecond value maps to the
// bucket whose upper bound is the smallest one not below it.
func TestLatencyBucketsAreMonotonicAndTight(t *testing.T) {
	prevUpper := uint64(0)
	for b := 0; b < latencyBuckets-1; b++ {
		upper := latencyUpper(b)
		if upper < prevUpper {
			t.Fatalf("bucket %d upper %d < previous %d", b, upper, prevUpper)
		}
		prevUpper = upper
	}
	for _, us := range []uint64{0, 1, 2, 3, 4, 5, 7, 8, 15, 16, 1000, 1023, 1024, 1500, 999_999, 1 << 26, 1<<26 + 1, 1 << 40} {
		b := latencyBucket(us)
		upper := latencyUpper(b)
		if us > upper && b != latencyBuckets-1 {
			t.Fatalf("%dµs placed in bucket %d with upper %d", us, b, upper)
		}
		if b > 0 && us <= latencyUpper(b-1) && b < latencyBuckets-1 {
			t.Fatalf("%dµs placed in bucket %d but bucket %d (upper %d) already holds it", us, b, b-1, latencyUpper(b-1))
		}
		if upper > us && b < latencyBuckets-1 && float64(upper) > float64(us)*1.125+1 {
			t.Fatalf("%dµs bucket upper %d is more than 12.5%% above", us, upper)
		}
	}
}

func TestLatencyHistogramQuantilesAndReset(t *testing.T) {
	var h latencyHistogram
	for i := 1; i <= 100; i++ {
		h.observe(time.Duration(i) * time.Millisecond)
	}
	h.observe(-time.Second) // clamps to 0, never panics
	s := h.snapshot()
	if s.Count != 101 || s.MaxUs != 100_000 {
		t.Fatalf("%+v", s)
	}
	within := func(got, want uint64) bool { return got >= want && float64(got) <= float64(want)*1.125 }
	if !within(s.P50Us, 50_000) || !within(s.P99Us, 99_000) || !within(s.P95Us, 95_000) || s.MeanUs < 49_000 || s.MeanUs > 51_000 {
		t.Fatalf("%+v", s)
	}
	if again := h.snapshot(); again.Count != 0 || again.MaxUs != 0 || again.P99Us != 0 {
		t.Fatalf("not reset: %+v", again)
	}
	// Boundaries: 2^27µs is the first overflow value; 2^27-1 is regular.
	h.observe(time.Duration(1<<27-1) * time.Microsecond)
	h.observe(time.Duration(1<<27) * time.Microsecond)
	if s = h.snapshot(); s.Count != 2 || s.P50Us != 1<<27-1 || s.P99Us != 1<<27 || latencyBucket(1<<27) != latencyBuckets-1 || latencyBucket(1<<27-1) == latencyBuckets-1 {
		t.Fatalf("%+v", s)
	}
	// max lagging the buckets (the swaps are not one atomic step) must not zero the quantiles.
	h.observe(time.Millisecond)
	h.max.Store(0)
	if s = h.snapshot(); s.Count != 1 || s.P50Us < 1000 {
		t.Fatalf("%+v", s)
	}
	for _, total := range []int{1, 2} {
		for i := 0; i < total; i++ {
			h.observe(5 * time.Millisecond)
		}
		if s = h.snapshot(); s.Count != uint64(total) || s.P50Us < 5000 || s.P50Us > 5625 || s.P99Us != s.P50Us {
			t.Fatalf("total %d: %+v", total, s)
		}
	}
	// Concurrent observers never lose counts.
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				h.observe(time.Duration(i) * time.Microsecond)
			}
		}()
	}
	wg.Wait()
	if s = h.snapshot(); s.Count != 8000 || s.MaxUs != 999 {
		t.Fatalf("%+v", s)
	}
}

func TestStatsLineFormat(t *testing.T) {
	s := IntervalStats{Window: 10 * time.Second, XLogsPerSec: 1912.4, PacksPerSec: 5730, XLogsTotal: 114720, UDPPerSec: 1110, UDPRejected: 3,
		Ingest: LatencySummary{Count: 10, P50Us: 800, P99Us: 12_000, MaxUs: 48_000}, ClientPerSec: 3.2,
		Client: LatencySummary{Count: 32, P50Us: 4_100, P99Us: 210_000, MaxUs: 12_300_000}, TCPActive: 12, ClientWaiting: 4, ClientAbandoned: 2, ClientWaitTimeouts: 5, ClientWaitRejected: 6,
		PulseEnabled: true, PulsePerSec: 1900, PulseFailed: 1, KafkaPending: 7, Agents: 203, HeapBytes: 540 << 20, SysBytes: 812 << 20, RSSBytes: 900 << 20, GCCycles: 14, Goroutines: 88}
	want := "stats 10s: xlog 1,912/s (114,720)  packs 5,730/s  udp 1,110/s reject 3 malformed 0 expired 0" +
		" | ingest p50 800µs p99 12ms max 48ms reject 0 drop 0" +
		" | client 3.2/s p50 4.1ms p99 210ms max 12.3s active 12 err 0 waiting 4 abandoned 2 wait-timeout 5 wait-reject 6" +
		" | pulse 1,900/s fail 1 pending 7" +
		" | agents 203  heap 540MiB sys 812MiB rss 900MiB gc 14 goroutines 88"
	if got := s.String(); got != want {
		t.Fatalf("\n got %s\nwant %s", got, want)
	}
	s.PulseEnabled, s.RSSBytes = false, 0
	if got := s.String(); strings.Contains(got, "pulse") || strings.Contains(got, "rss") || strings.Contains(got, "assembly") || strings.Contains(got, "sockdrop") {
		t.Fatal(got)
	}
	// Reassembly refusals and the Linux socket drops join the UDP figures.
	s.UDPAssembly, s.UDPKernel, s.UDPSocketDrops = 4, true, 9
	if got := s.String(); !strings.Contains(got, "udp 1,110/s reject 3 malformed 0 expired 0 assembly 4 sockdrop 9 | ingest") {
		t.Fatal(got)
	}
	for in, want := range map[uint64]string{0: "0", 50: "50µs", 999: "999µs", 1000: "1.0ms", 9_999: "10.0ms", 10_000: "10ms", 10_499: "10ms", 10_500: "11ms", 9_999_999: "10000ms", 10_000_000: "10.0s"} {
		if got := fmtUs(in); got != want {
			t.Errorf("fmtUs(%d) = %s, want %s", in, got, want)
		}
	}
	for in, want := range map[uint64]string{0: "0", 999: "999", 1000: "1,000", 1_234_567: "1,234,567"} {
		if got := fmtInt(in); got != want {
			t.Errorf("fmtInt(%d) = %s, want %s", in, got, want)
		}
	}
	if fmtBytes(1<<30*10) != "10.0GiB" || fmtBytes(512<<10) != "512KiB" || fmtRate(99.95) != "100.0" || fmtRate(100.4) != "100" {
		t.Fatal(fmtBytes(1<<30*10), fmtBytes(512<<10), fmtRate(99.95), fmtRate(100.4))
	}
}

func TestStatsIntervalZeroAndNegative(t *testing.T) {
	root := t.TempDir()
	base := Config{Directory: root, Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", HTTP: "127.0.0.1:0", ReadyFile: filepath.Join(t.TempDir(), "ready.json"), Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>")}
	neg := base
	neg.StatsInterval = -time.Second
	if _, err := Open(neg); err == nil {
		t.Fatal("negative interval accepted")
	}
	var logs bytes.Buffer
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)
	r, err := Open(base) // StatsInterval 0: no goroutine, no line, no /health stats
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	b, err := os.ReadFile(base.ReadyFile)
	if err != nil {
		t.Fatal(err)
	}
	var endpoints Endpoints
	if err = json.Unmarshal(b, &endpoints); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	resp, err := http.Get("http://" + endpoints.HTTP + "/health")
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	err = json.NewDecoder(resp.Body).Decode(&raw)
	resp.Body.Close()
	cancel()
	if rerr := <-done; rerr != nil {
		t.Fatal(rerr)
	}
	if _, has := raw["stats"]; err != nil || has || strings.Contains(logs.String(), " stats ") {
		t.Fatalf("stats present with interval 0: %v %v\n%s", err, has, logs.String())
	}
}

// --stats-interval logs the line periodically with the UDP ingest and client
// request latency observed in that window, and /health serves the same window.
func TestRuntimeStatsLineAndHealth(t *testing.T) {
	var logs bytes.Buffer
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)
	root := t.TempDir()
	ready := filepath.Join(t.TempDir(), "ready.json")
	cfg := Config{Directory: root, Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", HTTP: "127.0.0.1:0", ReadyFile: ready, Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>"), StatsInterval: 150 * time.Millisecond}
	r, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	b, err := os.ReadFile(ready)
	if err != nil {
		t.Fatal(err)
	}
	var endpoints Endpoints
	if err = json.Unmarshal(b, &endpoints); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	pack := &protocol.XLogPack{EndTime: now, ObjHash: 77, Service: 5, TxID: 123, Elapsed: 10}
	payload, err := protocol.EncodePack(pack, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("udp", endpoints.UDP)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Write(append([]byte("CAFE"), payload...)); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	deadline := time.Now().Add(5 * time.Second)
	var health Health
	for {
		resp, err := http.Get("http://" + endpoints.HTTP + "/health")
		if err != nil {
			t.Fatal(err)
		}
		err = json.NewDecoder(resp.Body).Decode(&health)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if health.Stats != nil && health.Stats.Ingest.Count >= 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	out := logs.String()
	if health.Stats == nil || health.Stats.XLogsTotal != 1 || health.Stats.Ingest.Count == 0 || health.Stats.Agents != 0 || health.Stats.Window < 100*time.Millisecond {
		t.Fatalf("health stats %+v\n%s", health.Stats, out)
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, " stats ") && strings.Contains(line, "ms: xlog ") && strings.Contains(line, "(1)") {
			if !strings.Contains(line, "ingest p50 ") || strings.Contains(line, "ingest p50 0 ") {
				t.Fatalf("no ingest latency in %q", line)
			}
			return
		}
	}
	t.Fatalf("no stats line with the xlog total:\n%s", out)
}
