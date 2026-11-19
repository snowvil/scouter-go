package server

import (
	"fmt"
	"math"
	"math/bits"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// latencyHistogram is a fixed, lock-free histogram for the periodic stats
// line: 8 sub-buckets per power of two of microseconds from 1µs up to
// 2^27µs ≈ 134s (slower goes to the overflow bucket), so a quantile is
// reported as its bucket's upper bound, at most 12.5% above the true value.
// observe never allocates or waits; it runs on the UDP workers and TCP
// client goroutines.
const (
	latencyPowers  = 27 // exponents 0..26 are regular buckets
	latencySubBits = 3
	latencySub     = 1 << latencySubBits
	latencyBuckets = latencyPowers*latencySub + 1 // last bucket is overflow
)

type latencyHistogram struct {
	buckets [latencyBuckets]atomic.Uint64
	sum     atomic.Uint64 // microseconds
	max     atomic.Uint64 // microseconds
}

func latencyBucket(us uint64) int {
	if us < 1 {
		return 0
	}
	exp := 63 - bits.LeadingZeros64(us)
	if exp >= latencyPowers {
		return latencyBuckets - 1
	}
	// The three bits below the leading one select the sub-bucket; below 2^3
	// the value is scaled up so 1, 2-3 and 4-7 land on distinct sub-buckets.
	var sub int
	if exp >= latencySubBits {
		sub = int((us >> (uint(exp) - latencySubBits)) & (latencySub - 1))
	} else {
		sub = int((us << (latencySubBits - uint(exp))) & (latencySub - 1))
	}
	return exp*latencySub + sub
}

// latencyUpper returns the inclusive upper bound (µs) of a bucket:
// 2^exp - 1 + ceil((sub+1) * 2^exp / 8).
func latencyUpper(bucket int) uint64 {
	if bucket >= latencyBuckets-1 {
		return 1 << latencyPowers
	}
	exp, sub := uint(bucket/latencySub), uint64(bucket%latencySub)
	return (uint64(1) << exp) - 1 + (((sub+1)<<exp)+latencySub-1)>>latencySubBits
}

func (h *latencyHistogram) observe(d time.Duration) {
	us := uint64(d / time.Microsecond)
	if d < 0 {
		us = 0
	}
	h.buckets[latencyBucket(us)].Add(1)
	h.sum.Add(us)
	for {
		cur := h.max.Load()
		if us <= cur || h.max.CompareAndSwap(cur, us) {
			return
		}
	}
}

// LatencySummary is one interval's distribution; durations are in
// microseconds in JSON so /health stays integer-only.
type LatencySummary struct {
	Count  uint64 `json:"count"`
	MeanUs uint64 `json:"meanUs"`
	P50Us  uint64 `json:"p50Us"`
	P95Us  uint64 `json:"p95Us"`
	P99Us  uint64 `json:"p99Us"`
	MaxUs  uint64 `json:"maxUs"`
}

// snapshot reads and resets the histogram. Concurrent observes during the
// swaps land in whichever interval wins; counts are never lost. The swaps
// are not one atomic step, so max may lag the buckets: a quantile is clamped
// to max only when max was observed.
func (h *latencyHistogram) snapshot() LatencySummary {
	var counts [latencyBuckets]uint64
	var total uint64
	for i := range h.buckets {
		counts[i] = h.buckets[i].Swap(0)
		total += counts[i]
	}
	s := LatencySummary{Count: total, MaxUs: h.max.Swap(0)}
	sum := h.sum.Swap(0)
	if total == 0 {
		return s
	}
	s.MeanUs = sum / total
	quantile := func(q float64) uint64 {
		target := uint64(math.Ceil(q * float64(total)))
		if target == 0 {
			target = 1
		}
		var cum uint64
		for i, c := range counts {
			cum += c
			if cum >= target {
				if s.MaxUs > 0 {
					return min(latencyUpper(i), s.MaxUs)
				}
				return latencyUpper(i)
			}
		}
		return s.MaxUs
	}
	s.P50Us, s.P95Us, s.P99Us = quantile(0.50), quantile(0.95), quantile(0.99)
	return s
}

// statsCounters is the cumulative counter set the interval deltas are taken
// from; it mirrors what the shutdown log prints.
type statsCounters struct {
	xlogs, packs, ingestRejected, droppedXLogs    uint64
	datagrams, udpRejected, udpMalformed, expired uint64
	udpAssembly, udpSocketDrops                   uint64
	udpKernel                                     bool
	clientRequests, tcpErrors, clientAbandoned    uint64
	clientWaitTimeouts, clientWaitRejected        uint64
	pulseSent, pulseFailed, pulseDropped          uint64
	kafkaFailed, kafkaDropped                     uint64
}

// IntervalStats is the last computed interval, also served by /health.
// UDPAssembly counts fragments refused by a reassembly bound (transport
// AssemblyRejected, not in UDPMalformed); UDPSocketDrops is the kernel's
// drops on the UDP socket (Linux, UDPKernel true only).
type IntervalStats struct {
	Window         time.Duration  `json:"windowNs"`
	Time           time.Time      `json:"time"`
	XLogsPerSec    float64        `json:"xlogsPerSec"`
	PacksPerSec    float64        `json:"packsPerSec"`
	XLogsTotal     uint64         `json:"xlogsTotal"`
	UDPPerSec      float64        `json:"udpDatagramsPerSec"`
	UDPRejected    uint64         `json:"udpRejected"`
	UDPMalformed   uint64         `json:"udpMalformed"`
	UDPExpired     uint64         `json:"udpExpired"`
	UDPAssembly    uint64         `json:"udpAssemblyRejected"`
	UDPSocketDrops uint64         `json:"udpSocketDrops"`
	UDPKernel      bool           `json:"udpKernelCounters"`
	IngestRejected uint64         `json:"ingestRejected"`
	DroppedXLogs   uint64         `json:"droppedXLogs"`
	Ingest         LatencySummary `json:"ingestLatency"`
	ClientPerSec   float64        `json:"clientRequestsPerSec"`
	Client         LatencySummary `json:"clientLatency"`
	TCPActive      int64          `json:"tcpActive"`
	TCPErrors      uint64         `json:"tcpErrors"`
	// ClientWaiting is the current number of Client connections waiting
	// for a net_tcp_service_pool_size permit. The other three are the
	// window's counts of waiting connections that were closed: the peer
	// left (EOF or reset), the wait limit passed, or the waiting set was
	// full (transport.TCPStats).
	ClientWaiting      int     `json:"clientWaiting"`
	ClientAbandoned    uint64  `json:"clientAbandoned"`
	ClientWaitTimeouts uint64  `json:"clientWaitTimeouts"`
	ClientWaitRejected uint64  `json:"clientWaitRejected"`
	PulseEnabled       bool    `json:"pulseEnabled"`
	PulsePerSec        float64 `json:"pulseSentPerSec"`
	PulseFailed        uint64  `json:"pulseFailed"`
	KafkaPending       int64   `json:"kafkaPending"`
	Agents             int     `json:"agents"`
	HeapBytes          uint64  `json:"heapBytes"`
	SysBytes           uint64  `json:"sysBytes"`
	RSSBytes           uint64  `json:"rssBytes,omitempty"`
	GCCycles           uint64  `json:"gcCycles"`
	Goroutines         uint64  `json:"goroutines"`
}

// statsReporter owns the two histograms and the previous counter snapshot.
// Only the stats goroutine calls tick; observe is called from anywhere.
type statsReporter struct {
	ingest, client latencyHistogram
	prev           statsCounters
	prevGC         uint64
	last           atomic.Pointer[IntervalStats]
	started        time.Time
	lastTick       time.Time
	samples        []metrics.Sample
}

func newStatsReporter(now time.Time) *statsReporter {
	s := &statsReporter{started: now, lastTick: now}
	s.samples = []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/total:bytes"},
		{Name: "/gc/cycles/total:gc-cycles"},
		{Name: "/sched/goroutines:goroutines"},
	}
	metrics.Read(s.samples)
	s.prevGC = s.samples[2].Value.Uint64()
	return s
}

func (r *Runtime) statsCounters() statsCounters {
	in, udp, tcp := r.engine.Stats(), r.udp.Stats(), r.tcp.Stats()
	c := statsCounters{xlogs: in.XLogs, packs: in.Completed, ingestRejected: in.Rejected, droppedXLogs: in.DroppedXLogs,
		datagrams: udp.Datagrams, udpRejected: udp.Rejected, udpMalformed: udp.Malformed, expired: udp.Expired,
		udpAssembly: udp.AssemblyRejected, udpSocketDrops: udp.SocketDrops, udpKernel: udp.KernelCounters,
		clientRequests: tcp.ClientRequests, tcpErrors: tcp.Errors, clientAbandoned: tcp.ClientAbandoned,
		clientWaitTimeouts: tcp.ClientWaitTimeouts, clientWaitRejected: tcp.ClientWaitRejected}
	if p := r.pulse; p != nil {
		d := p.dispatcher.Stats()
		c.pulseSent = d.XLog.Sent + d.Profile.Sent
		c.pulseFailed = d.XLog.Failed + d.Profile.Failed
		c.pulseDropped = d.XLog.Dropped + d.Profile.Dropped
		if p.producer != nil {
			k := p.producer.Stats()
			c.kafkaFailed, c.kafkaDropped = k.Failed, k.Dropped
		}
	}
	return c
}

// tick computes the interval since the previous tick and remembers it.
func (r *Runtime) statsTick(now time.Time) *IntervalStats {
	s := r.stats
	cur := r.statsCounters()
	tcpNow := r.tcp.Stats()
	window := now.Sub(s.lastTick)
	if window <= 0 {
		window = time.Nanosecond
	}
	sec := window.Seconds()
	rate := func(a, b uint64) float64 { return float64(a-b) / sec }
	out := &IntervalStats{Window: window, Time: now,
		XLogsPerSec: rate(cur.xlogs, s.prev.xlogs), PacksPerSec: rate(cur.packs, s.prev.packs), XLogsTotal: cur.xlogs,
		UDPPerSec: rate(cur.datagrams, s.prev.datagrams), UDPRejected: cur.udpRejected - s.prev.udpRejected,
		UDPMalformed: cur.udpMalformed - s.prev.udpMalformed, UDPExpired: cur.expired - s.prev.expired,
		UDPAssembly: cur.udpAssembly - s.prev.udpAssembly, UDPKernel: cur.udpKernel,
		IngestRejected: cur.ingestRejected - s.prev.ingestRejected, DroppedXLogs: cur.droppedXLogs - s.prev.droppedXLogs,
		Ingest: s.ingest.snapshot(), ClientPerSec: rate(cur.clientRequests, s.prev.clientRequests), Client: s.client.snapshot(),
		TCPActive: tcpNow.Active, TCPErrors: cur.tcpErrors - s.prev.tcpErrors,
		ClientWaiting: tcpNow.ClientWaiting, ClientAbandoned: cur.clientAbandoned - s.prev.clientAbandoned,
		ClientWaitTimeouts: cur.clientWaitTimeouts - s.prev.clientWaitTimeouts, ClientWaitRejected: cur.clientWaitRejected - s.prev.clientWaitRejected,
		Agents: r.engine.ObjectCount()}
	if cur.udpKernel && cur.udpSocketDrops >= s.prev.udpSocketDrops {
		out.UDPSocketDrops = cur.udpSocketDrops - s.prev.udpSocketDrops
	}
	if p := r.pulse; p != nil {
		out.PulseEnabled = true
		out.PulsePerSec = rate(cur.pulseSent, s.prev.pulseSent)
		// fail covers the dispatcher (convert/enqueue) and the producer (send).
		out.PulseFailed = cur.pulseFailed - s.prev.pulseFailed + cur.pulseDropped - s.prev.pulseDropped +
			cur.kafkaFailed - s.prev.kafkaFailed + cur.kafkaDropped - s.prev.kafkaDropped
		if p.producer != nil {
			out.KafkaPending = p.producer.Stats().Pending
		}
	}
	metrics.Read(s.samples)
	out.HeapBytes, out.SysBytes = s.samples[0].Value.Uint64(), s.samples[1].Value.Uint64()
	gc := s.samples[2].Value.Uint64()
	out.GCCycles, s.prevGC = gc-s.prevGC, gc
	out.Goroutines = s.samples[3].Value.Uint64()
	out.RSSBytes, _ = residentSetBytes()
	s.prev, s.lastTick = cur, now
	s.last.Store(out)
	return out
}

// String is the human-readable log line. Rates are per second over the
// window; the parenthesised xlog figure is the cumulative total; error
// counts are the window's increments.
func (s *IntervalStats) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "stats %s: xlog %s/s (%s)  packs %s/s  udp %s/s reject %d malformed %d expired %d",
		fmtWindow(s.Window), fmtRate(s.XLogsPerSec), fmtInt(s.XLogsTotal), fmtRate(s.PacksPerSec), fmtRate(s.UDPPerSec), s.UDPRejected, s.UDPMalformed, s.UDPExpired)
	if s.UDPAssembly > 0 {
		fmt.Fprintf(&b, " assembly %d", s.UDPAssembly)
	}
	if s.UDPKernel {
		fmt.Fprintf(&b, " sockdrop %d", s.UDPSocketDrops)
	}
	fmt.Fprintf(&b, " | ingest p50 %s p99 %s max %s reject %d drop %d", fmtUs(s.Ingest.P50Us), fmtUs(s.Ingest.P99Us), fmtUs(s.Ingest.MaxUs), s.IngestRejected, s.DroppedXLogs)
	fmt.Fprintf(&b, " | client %s/s p50 %s p99 %s max %s active %d err %d waiting %d abandoned %d wait-timeout %d wait-reject %d", fmtRate(s.ClientPerSec), fmtUs(s.Client.P50Us), fmtUs(s.Client.P99Us), fmtUs(s.Client.MaxUs),
		s.TCPActive, s.TCPErrors, s.ClientWaiting, s.ClientAbandoned, s.ClientWaitTimeouts, s.ClientWaitRejected)
	if s.PulseEnabled {
		fmt.Fprintf(&b, " | pulse %s/s fail %d pending %d", fmtRate(s.PulsePerSec), s.PulseFailed, s.KafkaPending)
	}
	fmt.Fprintf(&b, " | agents %d  heap %s sys %s", s.Agents, fmtBytes(s.HeapBytes), fmtBytes(s.SysBytes))
	if s.RSSBytes > 0 {
		fmt.Fprintf(&b, " rss %s", fmtBytes(s.RSSBytes))
	}
	fmt.Fprintf(&b, " gc %d goroutines %d", s.GCCycles, s.Goroutines)
	return b.String()
}

func fmtWindow(d time.Duration) string {
	if d >= time.Second {
		return strconv.FormatFloat(d.Seconds(), 'f', 0, 64) + "s"
	}
	return d.Round(time.Millisecond).String()
}

// fmtRate keeps one decimal below 100/s, where rounding would hide low traffic.
func fmtRate(v float64) string {
	if v < 100 {
		return strconv.FormatFloat(v, 'f', 1, 64)
	}
	return fmtInt(uint64(v + 0.5))
}

func fmtInt(v uint64) string {
	s := strconv.FormatUint(v, 10)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	head := len(s) % 3
	if head > 0 {
		b.WriteString(s[:head])
	}
	for i := head; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

func fmtUs(us uint64) string {
	switch {
	case us == 0:
		return "0"
	case us < 1000:
		return strconv.FormatUint(us, 10) + "µs"
	case us < 10_000:
		return strconv.FormatFloat(float64(us)/1000, 'f', 1, 64) + "ms"
	case us < 10_000_000:
		return strconv.FormatUint((us+500)/1000, 10) + "ms"
	default:
		return strconv.FormatFloat(float64(us)/1e6, 'f', 1, 64) + "s"
	}
}

func fmtBytes(b uint64) string {
	switch {
	case b >= 10<<30:
		return strconv.FormatFloat(float64(b)/(1<<30), 'f', 1, 64) + "GiB"
	case b >= 1<<20:
		return strconv.FormatUint((b+(1<<19))>>20, 10) + "MiB"
	default:
		return strconv.FormatUint((b+(1<<9))>>10, 10) + "KiB"
	}
}
