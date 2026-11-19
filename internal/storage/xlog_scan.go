package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

// XLogScan selects one XLog time-index scan (ScanXLogsContext).
//
// Java (XLogRD.readByTime/readFromEndTime/readByTimeLimitCount) hands each
// record to the handler as it reads it, the handler filters and writes it to
// the Client, and the scan ends when the handler reaches max. There is no
// total record or byte cap. The Go scan keeps that shape: records go to visit
// in batches, Match/Max end the scan early, and only one batch is buffered.
type XLogScan struct {
	Date     string
	From, To int64
	// Reverse is the TRANX_LOAD_TIME_GROUP reverse=true order (V1 only).
	Reverse bool
	// Paged selects the TRANX_LOAD_TIME_GROUP_V2 order: 500 ms buckets from
	// LastBucket (0 = From), newest appended first within a bucket, no
	// deduplication. XLogRecord.BucketTime is set only for Paged scans.
	Paged      bool
	LastBucket int64
	// Match runs under the store gate exactly once per record in scan order
	// (so it may keep state) and must not call the Store. nil keeps all.
	Match func(*protocol.XLogPack) bool
	// Max > 0 ends the scan after Max matched records.
	Max int
}

// Batch and gate-hold bounds. A batch is filled under the store gate and
// handed to visit outside it, so ingest never waits for visit (a Client
// socket write). Each gate hold also ends after scanHoldSteps index or data
// reads or scanHoldTime, whichever comes first, whatever Match keeps: ingest
// waits at most one hold (milliseconds), independent of the range size and of
// the records in one bucket. A hold always makes at least one step.
const (
	scanBatchRecords = 512
	scanBatchBytes   = 4 << 20
	scanHoldSteps    = 256
	scanHoldTime     = 2 * time.Millisecond
)

var errPause = errors.New("scan batch full")

// holdBudget meters one store gate hold.
type holdBudget struct {
	steps    int
	deadline time.Time
}

func newHoldBudget() holdBudget { return holdBudget{deadline: time.Now().Add(scanHoldTime)} }

// spend counts one index record, data record or bucket step.
func (b *holdBudget) spend() { b.steps++ }
func (b *holdBudget) exhausted() bool {
	return b.steps >= scanHoldSteps || (b.steps > 0 && b.steps%16 == 0 && time.Now().After(b.deadline))
}

// rangeCursor walks the ScanRange order (RangeSteps buckets) across store
// gate holds. A bucket is collected from its chain in chunks (WalkRangeStep
// resumes at a chain position) into a compact TimeEntries, ordered, then read
// entry by entry, again across holds. Records appended to a bucket after its
// collection began are not seen, as with the former one-hold snapshot and the
// bucket list Java's IndexTimeFile.getSecAll reads before calling handlers.
type rangeCursor struct {
	ready       bool
	step, steps int
	collecting  bool
	at, resume  int64
	entries     legacyindex.TimeEntries
	next        int
	// unsorted: the bucket is collected but not ordered yet; settle orders it
	// without the gate (TimeEntries is owned by the cursor).
	unsorted bool
}

// scanSortInline is the largest bucket ordered inside a gate hold; a larger
// one is ordered between holds so sorting time never extends a hold.
const scanSortInline = 1024

// settle orders the collected bucket and moves to reading it.
func (c *rangeCursor) settle(from, to int64, reverse bool) {
	c.entries.Finish(from, to, reverse)
	c.unsorted, c.next = false, 0
	c.step++
}

// settlePending orders a bucket nextRecord left unsorted. Scans call it
// between gate holds.
func (c *rangeCursor) settlePending(from, to int64, reverse bool) {
	if c.unsorted {
		c.settle(from, to, reverse)
	}
}

// nextRecord returns the next record of the ScanRange order. ok=false with
// end=false means the hold budget ran out first; call again in a later hold.
func (c *rangeCursor) nextRecord(idx *legacyindex.Index, from, to int64, reverse bool, budget *holdBudget) (ms int64, value []byte, ok, end bool, err error) {
	if !c.ready {
		if c.steps, err = idx.RangeSteps(from, to); err != nil {
			return 0, nil, false, false, err
		}
		c.ready = true
	}
	for {
		if c.collecting {
			err = idx.WalkRangeStep(from, to, reverse, c.step, c.at, c.resume, func(r legacyindex.Record) error {
				c.at, c.resume = r.Position, r.PreviousPosition
				if err := c.entries.Add(r); err != nil {
					return err
				}
				budget.spend()
				if budget.exhausted() {
					return errPause
				}
				return nil
			})
			if errors.Is(err, errPause) {
				return 0, nil, false, false, nil
			}
			if err != nil {
				return 0, nil, false, false, err
			}
			c.collecting = false
			if c.entries.Len() > scanSortInline {
				// Order a large bucket outside the gate (settle).
				c.unsorted = true
				return 0, nil, false, false, nil
			}
			c.settle(from, to, reverse)
		}
		if c.unsorted {
			c.settle(from, to, reverse)
		}
		if c.next < c.entries.Len() {
			ms, value = c.entries.At(c.next)
			c.next++
			return ms, value, true, false, nil
		}
		if c.step >= c.steps {
			return 0, nil, false, true, nil
		}
		if budget.exhausted() {
			return 0, nil, false, false, nil
		}
		budget.spend() // an empty bucket still costs a step
		c.entries.Reset()
		c.collecting, c.at, c.resume = true, 0, 0
	}
}

type xlogCursor struct {
	q       XLogScan
	started bool
	done    bool
	matched int
	// V1 order.
	rangeCursor
	// V2: bucket time and the chain resume point (WalkBucketFrom).
	bucket int64
	// miss ends a hold at a record whose compressed block is not cached;
	// the scan decodes it into fetch outside the gate and reads the record
	// again in its next hold (zip.go).
	miss  *zipMiss
	fetch zipFetch
}

// ScanXLogsContext streams the XLogs of one Java date in Java scan order.
// visit runs without the store gate; returning false ends the scan without an
// error. Packs are freshly decoded and owned by visit. The scan observes
// records appended while it runs the way a Java reader would: a later bucket
// is read when the scan reaches it.
func (s *Store) ScanXLogsContext(ctx context.Context, q XLogScan, visit func(XLogRecord) (bool, error)) error {
	if visit == nil {
		return ErrInvalid
	}
	c := &xlogCursor{q: q}
	defer c.fetch.drop()
	batch := make([]XLogRecord, 0, scanBatchRecords)
	for {
		var err error
		batch, err = s.fillXLogBatch(ctx, c, batch[:0])
		if err != nil {
			return err
		}
		for n := range batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			more, err := visit(batch[n])
			batch[n] = XLogRecord{}
			if err != nil || !more {
				return err
			}
		}
		// Decode after visit: a slow Client write never holds a decode
		// slot or a decoded block.
		if m := c.miss; m != nil {
			c.miss = nil
			if err := s.fetchZip(ctx, &c.fetch, m); err != nil {
				return err
			}
		}
		if c.done {
			return ctx.Err()
		}
		c.settlePending(q.From, q.To, q.Reverse)
	}
}

// scanHoldObserver, when set by a test, receives the duration of every scan
// gate hold.
var scanHoldObserver func(time.Duration)

func (s *Store) lockScan(ctx context.Context) (func(), error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	if observe := scanHoldObserver; observe != nil {
		start := time.Now()
		return func() { observe(time.Since(start)); s.mu.Unlock() }, nil
	}
	return s.mu.Unlock, nil
}
func (s *Store) fillXLogBatch(ctx context.Context, c *xlogCursor, batch []XLogRecord) ([]XLogRecord, error) {
	unlock, err := s.lockScan(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	q := &c.q
	if !c.started {
		if err := s.queryRange(q.Date, q.From, q.To); err != nil {
			return nil, err
		}
		c.bucket = q.From
		if q.Paged && q.LastBucket != 0 {
			c.bucket = q.LastBucket
		}
		if c.bucket < q.From || c.bucket > q.To {
			return nil, ErrInvalid
		}
	} else if err := s.check(q.Date); err != nil {
		return nil, err
	}
	idx, err := s.index(s.path(q.Date, "xlog", "xlog_tim"), true, false)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if err != nil {
		return nil, err
	}
	if idx == nil { // no index yet, or the date was purged mid-scan
		c.done = true
		return batch, nil
	}
	c.started = true
	budget := newHoldBudget()
	used := 0
	full := func() bool {
		return c.done || len(batch) >= scanBatchRecords || used >= scanBatchBytes || budget.exhausted()
	}
	add := func(value []byte, tm, bucket int64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		budget.spend()
		p, n, err := s.decodeXLogFetch(ctx, q.Date, value, &c.fetch)
		if err != nil {
			return err
		}
		if p.EndTime != tm {
			return ErrCorrupt
		}
		if q.Match != nil && !q.Match(p) {
			return nil
		}
		used += n
		batch = append(batch, XLogRecord{BucketTime: bucket, Pack: p, Bytes: n})
		c.matched++
		if q.Max > 0 && c.matched >= q.Max {
			c.done = true
		}
		return nil
	}
	if !q.Paged {
		for !full() {
			tm, value, ok, end, err := c.nextRecord(idx, q.From, q.To, q.Reverse, &budget)
			if err != nil {
				return nil, err
			}
			if end {
				c.done = true
			}
			if !ok {
				break
			}
			if err := add(value, tm, 0); err != nil {
				if errors.As(err, &c.miss) {
					c.next-- // read this record again after the decode
					break
				}
				return nil, err
			}
		}
		return batch, nil
	}
	for !full() {
		if c.bucket > q.To {
			c.done = true
			break
		}
		budget.spend() // the bucket step itself
		err := idx.WalkBucketFrom(c.bucket, c.at, c.resume, func(r legacyindex.Record) error {
			tm := int64(binary.BigEndian.Uint64(r.Key))
			if tm >= q.From && tm <= q.To {
				if err := add(r.Value, tm, c.bucket); err != nil {
					if errors.As(err, &c.miss) {
						// Resume at this record after the decode: its own
						// end bounds the chain link to it, as the newer
						// record's position would.
						c.at, c.resume = r.NextPosition, r.Position
						return errPause
					}
					return err
				}
				c.at, c.resume = r.Position, r.PreviousPosition
			} else {
				c.at, c.resume = r.Position, r.PreviousPosition
				budget.spend()
			}
			if full() {
				return errPause
			}
			return nil
		})
		if errors.Is(err, errPause) {
			break
		}
		if err != nil {
			return nil, err
		}
		c.bucket += 500
		c.at, c.resume = 0, 0
	}
	return batch, nil
}
