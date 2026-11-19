package main

import (
	"context"
	"strconv"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
)

// timeRange is an inclusive [From,To] millisecond range within one Java date.
type timeRange struct {
	Date     string
	From, To int64
}

// splitRanges cuts [from,to] into windows of at most window ms that never cross a
// Java fixed-24h date boundary (the server rejects cross-date ranges).
func splitRanges(from, to, window int64, loc *time.Location) []timeRange {
	if window < 1 {
		window = 1
	}
	var out []timeRange
	for start := from; start <= to; {
		date := scoutertime.Date(start, loc)
		dayStart, err := scoutertime.ParseDate(date, loc)
		if err != nil {
			return out
		}
		dayEnd := dayStart + scoutertime.DayMillis - 1
		end := min(start+window-1, to, dayEnd)
		out = append(out, timeRange{Date: date, From: start, To: end})
		start = end + 1
	}
	return out
}

// comparison classifies stored XLogs against the expected transaction index space.
type comparison struct {
	total            uint64
	found            *bitset
	Unique           uint64 `json:"unique_expected_found"`
	Duplicates       uint64 `json:"duplicate_records"`
	Extra            uint64 `json:"extra_records_not_generated"`
	ObjHashMismatch  uint64 `json:"objhash_mismatch"`
	RecordsReturned  uint64 `json:"records_returned"`
	objects          []object
	codec            txidCodec
	extraSamples     []int64
	mismatchSamples  []int64
	duplicateSamples []int64
}

func newComparison(total uint64, codec txidCodec, objects []object) *comparison {
	return &comparison{total: total, found: newBitset(total), codec: codec, objects: objects}
}
func sample(list *[]int64, v int64) {
	if len(*list) < 20 {
		*list = append(*list, v)
	}
}
func (c *comparison) add(x *protocol.XLogPack) {
	c.RecordsReturned++
	i := c.codec.decode(x.TxID)
	if i >= c.total {
		c.Extra++
		sample(&c.extraSamples, x.TxID)
		return
	}
	if c.objects[objectIndex(x.TxID, len(c.objects))].Hash != x.ObjHash {
		c.ObjHashMismatch++
		sample(&c.mismatchSamples, x.TxID)
	}
	if c.found.set(i) {
		c.Duplicates++
		sample(&c.duplicateSamples, x.TxID)
		return
	}
	c.Unique++
}

type verifyReport struct {
	Method             string         `json:"method"`
	Comparison         *comparison    `json:"comparison"`
	Expected           uint64         `json:"expected_sent_ok"`
	Generated          uint64         `json:"generated_transactions"`
	SendFailed         uint64         `json:"send_failed_transactions"`
	NotSent            uint64         `json:"not_sent_transactions"`
	MissingAfterSendOK uint64         `json:"missing_after_successful_send"`
	FoundButSendFailed uint64         `json:"found_although_send_failed"`
	MissingSamples     []int64        `json:"missing_txid_samples"`
	ExtraSamples       []int64        `json:"extra_txid_samples"`
	DuplicateSamples   []int64        `json:"duplicate_txid_samples"`
	MismatchSamples    []int64        `json:"objhash_mismatch_txid_samples"`
	Windows            int            `json:"query_windows"`
	WindowSplits       int            `json:"window_splits_after_error"`
	QueryErrors        int            `json:"query_errors"`
	UnverifiedWindows  []string       `json:"unverified_windows"`
	QueryLatency       latencySummary `json:"query_latency"`
	SteadyFound        uint64         `json:"steady_found"`
	DurationMS         float64        `json:"duration_ms"`
	Complete           bool           `json:"complete"`
	Error              string         `json:"error,omitempty"`
	steadySent         uint64
}

// verifyStored re-queries every window of [from,to] through TRANX_LOAD_TIME_GROUP
// after restart. A failed window is halved (down to 1ms) since the server enforces
// per-query record/byte limits; a 1ms window that still fails is reported as
// unverified rather than counted as missing silently.
func verifyStored(ctx context.Context, dial func() (*client, error), cmp *comparison, from, to, window int64, loc *time.Location, objects []object) (rep verifyReport) {
	started := time.Now()
	rep.Method = "restart + TRANX_LOAD_TIME_GROUP (max=0, limit=0, all objHash) per window"
	defer func() { rep.DurationMS = float64(time.Since(started).Microseconds()) / 1000 }()
	var lat histogram
	defer func() { rep.QueryLatency = lat.latency() }()
	var c *client
	defer func() {
		if c != nil {
			c.Close()
		}
	}()
	stack := splitRanges(from, to, window, loc)
	rep.Windows = len(stack)
	// Process in ascending order: reverse into a LIFO stack.
	for l, r := 0, len(stack)-1; l < r; l, r = l+1, r-1 {
		stack[l], stack[r] = stack[r], stack[l]
	}
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			rep.Error = err.Error()
			for _, w := range stack {
				rep.UnverifiedWindows = append(rep.UnverifiedWindows, w.Date+":"+itoa(w.From)+"-"+itoa(w.To))
			}
			return rep
		}
		w := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if c == nil {
			var err error
			if c, err = dial(); err != nil {
				rep.QueryErrors++
				rep.Error = err.Error()
				rep.UnverifiedWindows = append(rep.UnverifiedWindows, w.Date+":"+itoa(w.From)+"-"+itoa(w.To))
				c = nil
				continue
			}
		}
		var batch []*protocol.XLogPack
		t0 := time.Now()
		_, err := c.history(ctx, w.Date, w.From, w.To, 0, objects, func(x *protocol.XLogPack) { batch = append(batch, x) })
		lat.record(time.Since(t0).Nanoseconds())
		if err != nil {
			rep.QueryErrors++
			rep.Error = err.Error()
			c.conn.Close()
			c = nil
			if w.To > w.From {
				mid := w.From + (w.To-w.From)/2
				// Push the later half first so the earlier half is processed next.
				stack = append(stack, timeRange{w.Date, mid + 1, w.To}, timeRange{w.Date, w.From, mid})
				rep.WindowSplits++
			} else {
				rep.UnverifiedWindows = append(rep.UnverifiedWindows, w.Date+":"+itoa(w.From)+"-"+itoa(w.To))
			}
			continue
		}
		// Records of a failed (partial) window are discarded; only complete windows count.
		for _, x := range batch {
			cmp.add(x)
		}
	}
	rep.Complete = len(rep.UnverifiedWindows) == 0
	return rep
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
