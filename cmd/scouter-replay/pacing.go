package main

import (
	"math"
	"math/bits"
	"time"
)

// schedule maps transaction index i to its intended send instant. Latency/lag is
// measured from this instant (not from the actual send) to avoid coordinated omission.
type schedule struct {
	start time.Time
	rate  float64 // transactions per second, > 0
	total uint64  // number of scheduled transactions
	// steadyFrom is the first index whose scheduled time is at or after the warm-up end.
	steadyFrom uint64
}

func newSchedule(start time.Time, rate float64, warmup, duration time.Duration) schedule {
	total := uint64(math.Floor(rate * (warmup + duration).Seconds()))
	steady := uint64(math.Ceil(rate * warmup.Seconds()))
	if steady > total {
		steady = total
	}
	return schedule{start: start, rate: rate, total: total, steadyFrom: steady}
}

// offset is the scheduled delay of index i from the schedule start.
func (s schedule) offset(i uint64) time.Duration {
	return time.Duration(float64(i) * float64(time.Second) / s.rate)
}
func (s schedule) at(i uint64) time.Time { return s.start.Add(s.offset(i)) }

// histogram is a log-linear latency/size histogram with <=1/32 relative bucket
// error. Values are int64 (nanoseconds or bytes). Quantiles report the bucket upper
// bound, which never under-states the true value. Memory is fixed (histBuckets).
type histogram struct {
	counts        [histBuckets]uint64
	count         uint64
	sum           float64
	min, max      int64
	negativeCount uint64
}

const (
	histLinear  = 64
	histSub     = 32
	histBuckets = histLinear + 58*histSub
)

func histIndex(v int64) int {
	if v < histLinear {
		return int(v)
	}
	e := bits.Len64(uint64(v)) - 6 // >= 1
	m := uint64(v) >> uint(e)      // in [32, 63]
	return histLinear + (e-1)*histSub + int(m-histSub)
}
func histUpper(index int) int64 {
	if index < histLinear {
		return int64(index)
	}
	e := (index-histLinear)/histSub + 1
	m := uint64((index-histLinear)%histSub + histSub)
	upper := ((m + 1) << uint(e)) - 1
	if upper > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(upper)
}

func (h *histogram) record(v int64) {
	if v < 0 {
		// A negative lag means the send happened before its scheduled instant
		// (clock granularity); it is recorded as 0 and counted.
		h.negativeCount++
		v = 0
	}
	if h.count == 0 || v < h.min {
		h.min = v
	}
	if v > h.max {
		h.max = v
	}
	h.count++
	h.sum += float64(v)
	h.counts[histIndex(v)]++
}
func (h *histogram) merge(o *histogram) {
	if o.count == 0 {
		return
	}
	if h.count == 0 || o.min < h.min {
		h.min = o.min
	}
	if o.max > h.max {
		h.max = o.max
	}
	h.count += o.count
	h.sum += o.sum
	h.negativeCount += o.negativeCount
	for i := range h.counts {
		h.counts[i] += o.counts[i]
	}
}
func (h *histogram) quantile(q float64) int64 {
	if h.count == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(h.count)))
	if rank < 1 {
		rank = 1
	}
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= rank {
			return min(histUpper(i), h.max)
		}
	}
	return h.max
}

// countAbove counts recorded values whose bucket lies entirely above threshold.
func (h *histogram) countAbove(threshold int64) uint64 {
	var n uint64
	for i, c := range h.counts {
		if i > 0 && histUpper(i-1) >= threshold {
			n += c
		}
	}
	return n
}

type latencySummary struct {
	Count         uint64   `json:"count"`
	MinMS         *float64 `json:"min_ms"`
	P50MS         *float64 `json:"p50_ms"`
	P95MS         *float64 `json:"p95_ms"`
	P99MS         *float64 `json:"p99_ms"`
	MaxMS         *float64 `json:"max_ms"`
	MeanMS        *float64 `json:"mean_ms"`
	NegativeCount uint64   `json:"negative_clamped_count,omitempty"`
	Note          string   `json:"note,omitempty"`
}

func ms(ns int64) *float64 { v := float64(ns) / 1e6; return &v }
func (h *histogram) latency() latencySummary {
	s := latencySummary{Count: h.count, NegativeCount: h.negativeCount}
	if h.count == 0 {
		s.Note = "no samples"
		return s
	}
	mean := h.sum / float64(h.count) / 1e6
	s.MinMS, s.P50MS, s.P95MS, s.P99MS, s.MaxMS, s.MeanMS = ms(h.min), ms(h.quantile(0.50)), ms(h.quantile(0.95)), ms(h.quantile(0.99)), ms(h.max), &mean
	return s
}

type sizeSummary struct {
	Count uint64  `json:"count"`
	Total float64 `json:"total_bytes"`
	Min   int64   `json:"min_bytes"`
	P50   int64   `json:"p50_bytes"`
	P95   int64   `json:"p95_bytes"`
	P99   int64   `json:"p99_bytes"`
	Max   int64   `json:"max_bytes"`
}

func (h *histogram) sizes() sizeSummary {
	return sizeSummary{Count: h.count, Total: h.sum, Min: h.min, P50: h.quantile(0.5), P95: h.quantile(0.95), P99: h.quantile(0.99), Max: h.max}
}
