package sqltable

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// Bounds copied from S/core/SqlTables.scala:36-42 (Java has them all).
const (
	Queue1Capacity   = 500   // MAX_Q1: items keep the SQL text
	Queue2Capacity   = 10000 // MAX_Q2: SQL text dropped, re-read from the text store
	FailSetMax       = 10000 // failSet IntLinkedSet.setMax
	ParsedDatesMax   = 2     // parsed StringKeyLinkedMap.setMax
	ParsedPerDateMax = 10000 // per-date IntLinkedSet.setMax
)

// Text types (scouter.lang.TextTypes.SQL and SQL_TABLES).
const (
	TextTypeSQL    = "sql"
	TextTypeTables = "table"
)

// Logger ids used by SqlTables.
const (
	LogQueueExceeded = "S111" // Logger.println("S111", 10, "queue exceeded!!")
	LogParseFailed   = "S112" // Logger.println("S112", ...) when log_sql_parsing_fail_enabled
)

// TextSink receives the parse result. It stands for TextWR.add(date, div, hash,
// text): the implementation routes the "table" type to the permanent text
// dictionary and applies the Java TextWR/TextPermWR rules (TextCache.put,
// duplicate check, an empty text is not written to the permanent file).
type TextSink interface {
	AddText(ctx context.Context, date, textType string, hash int32, text string) error
}

// TextSource stands for TextRD.getString(date, div, hash): the SQL text of a
// queue2 item whose text was dropped. found=false means Java's null.
type TextSource interface {
	Text(ctx context.Context, date, textType string, hash int32) (text string, found bool, err error)
}

// Options configures Tables. Sink, Source and Enabled are required.
type Options struct {
	Sink   TextSink
	Source TextSource
	// Enabled is sql_table_parsing_enabled, read on every Add.
	Enabled func() bool
	// LogFailEnabled is log_sql_parsing_fail_enabled, read on every failure.
	// nil means false.
	LogFailEnabled func() bool
	// Log mirrors Logger.println(id, sec, message): the logger applies the
	// per-id interval (sec > 0) and mgr_log_ignore_ids. nil discards.
	Log func(id string, intervalSec int, message string)
}

var (
	ErrOptions = errors.New("sqltable: sink, source and enabled are required")
)

// Stats are cumulative counters plus the live queue lengths. Every Add ends in
// exactly one of Disabled, Rejected, RejectedClosed, Enqueued1, Enqueued2.
// Every dequeued item ends in exactly one of the processing outcomes; items
// still queued at Close are counted in RemainingAtClose.
type Stats struct {
	Disabled       uint64 // Add while sql_table_parsing_enabled=false
	Enqueued1      uint64 // queued with text in queue1
	Enqueued2      uint64 // queue1 full: queued without text in queue2
	Rejected       uint64 // both queues full (S111)
	RejectedClosed uint64 // Add after Close

	Stored        uint64 // result handed to the sink without error (Empty included)
	Empty         uint64 // of Stored: result was "" (Java Exception path or no table)
	AlreadyParsed uint64 // hash already in the per-date parsed set
	AlreadyFailed uint64 // hash in failSet
	TextMissing   uint64 // queue2 item whose text the source did not find
	SourceErrors  uint64 // queue2 text lookup returned an error
	ParseFailures uint64 // Go bounds/panic: failSet + S112 (Java Throwable path)
	StorageErrors uint64 // sink returned an error

	RemainingAtClose uint64 // items left in both queues when the worker stopped
	Queue1Len        int
	Queue2Len        int
	FailSetLen       int
}

type item struct {
	date    string
	hash    int32
	sql     string
	hasText bool
}

// Tables owns queue1/queue2, the parsed and failed hash sets and one worker
// goroutine (the "scouter.server.core.SqlTables" daemon). The worker is started
// by New and stopped by Close.
type Tables struct {
	opts   Options
	q1, q2 chan item

	closeMu sync.RWMutex // Add holds R while offering; Close holds W to flip closed
	closed  bool
	stop    chan struct{}
	done    chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc

	// worker-owned state
	parsed []datedSet
	failed *intLinkedSet

	failedLen atomic.Int64
	c         counters
}

type counters struct {
	disabled, enq1, enq2, rejected, rejectedClosed       atomic.Uint64
	stored, empty, alreadyParsed, alreadyFailed          atomic.Uint64
	textMissing, sourceErrors, parseFailures, storageErr atomic.Uint64
	remaining                                            atomic.Uint64
}

type datedSet struct {
	date string
	set  *intLinkedSet
}

// New validates opts and starts the worker goroutine.
func New(opts Options) (*Tables, error) {
	t, err := newTables(opts)
	if err != nil {
		return nil, err
	}
	go t.run()
	return t, nil
}

func newTables(opts Options) (*Tables, error) {
	if opts.Sink == nil || opts.Source == nil || opts.Enabled == nil {
		return nil, ErrOptions
	}
	t := &Tables{
		opts:   opts,
		q1:     make(chan item, Queue1Capacity),
		q2:     make(chan item, Queue2Capacity),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		failed: newIntLinkedSet(FailSetMax),
	}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	return t, nil
}

// Add is SqlTables.add(date, sqlHash, sqlText). It never blocks. date is the
// yyyymmdd TextCore used for the same TextPack.
func (t *Tables) Add(date string, hash int32, sql string) {
	if !t.opts.Enabled() {
		t.c.disabled.Add(1)
		return
	}
	t.closeMu.RLock()
	defer t.closeMu.RUnlock()
	if t.closed {
		t.c.rejectedClosed.Add(1)
		return
	}
	select {
	case t.q1 <- item{date: date, hash: hash, sql: sql, hasText: true}:
		t.c.enq1.Add(1)
		return
	default:
	}
	select {
	case t.q2 <- item{date: date, hash: hash}:
		t.c.enq2.Add(1)
	default:
		t.c.rejected.Add(1)
		t.log(LogQueueExceeded, 10, "queue exceeded!!")
	}
}

// Close stops the worker after its current item. Items still queued are not
// processed (the Java daemon thread simply stops) and are counted in
// RemainingAtClose. If ctx ends first the in-flight sink/source call is
// cancelled and Close still waits for the worker to return.
func (t *Tables) Close(ctx context.Context) error {
	t.closeMu.Lock()
	if t.closed {
		t.closeMu.Unlock()
		<-t.done
		return nil
	}
	t.closed = true
	close(t.stop)
	t.closeMu.Unlock()
	var err error
	select {
	case <-t.done:
	case <-ctx.Done():
		err = ctx.Err()
		t.cancel()
		<-t.done
	}
	t.cancel()
	t.c.remaining.Add(uint64(drain(t.q1) + drain(t.q2)))
	return err
}

func drain(c chan item) int {
	n := 0
	for {
		select {
		case <-c:
			n++
		default:
			return n
		}
	}
}

// Stats returns a snapshot of the counters.
func (t *Tables) Stats() Stats {
	return Stats{
		Disabled:         t.c.disabled.Load(),
		Enqueued1:        t.c.enq1.Load(),
		Enqueued2:        t.c.enq2.Load(),
		Rejected:         t.c.rejected.Load(),
		RejectedClosed:   t.c.rejectedClosed.Load(),
		Stored:           t.c.stored.Load(),
		Empty:            t.c.empty.Load(),
		AlreadyParsed:    t.c.alreadyParsed.Load(),
		AlreadyFailed:    t.c.alreadyFailed.Load(),
		TextMissing:      t.c.textMissing.Load(),
		SourceErrors:     t.c.sourceErrors.Load(),
		ParseFailures:    t.c.parseFailures.Load(),
		StorageErrors:    t.c.storageErr.Load(),
		RemainingAtClose: t.c.remaining.Load(),
		Queue1Len:        len(t.q1),
		Queue2Len:        len(t.q2),
		FailSetLen:       int(t.failedLen.Load()),
	}
}

func (t *Tables) log(id string, sec int, msg string) {
	if t.opts.Log != nil {
		t.opts.Log(id, sec, msg)
	}
}

// run is the daemon loop: queue1 first, then queue2, else wait.
func (t *Tables) run() {
	defer close(t.done)
	for {
		select {
		case <-t.stop:
			return
		default:
		}
		var it item
		select {
		case it = <-t.q1:
		default:
			select {
			case it = <-t.q2:
			default:
				select {
				case it = <-t.q1:
				case it = <-t.q2:
				case <-t.stop:
					return
				}
			}
		}
		t.process(it)
	}
}

// process is SqlTables.process. The order of checks and of the parsed-set
// insertion before the sink call follows the Java code.
func (t *Tables) process(it item) {
	set := t.parsedSet(it.date)
	if set.contains(it.hash) {
		t.c.alreadyParsed.Add(1)
		return
	}
	if t.failed.contains(it.hash) {
		t.c.alreadyFailed.Add(1)
		return
	}
	if !it.hasText {
		text, found, err := t.opts.Source.Text(t.ctx, it.date, TextTypeSQL, it.hash)
		if err != nil {
			// TextRD.getString catches Exception and returns null.
			t.c.sourceErrors.Add(1)
			return
		}
		if !found {
			t.c.textMissing.Add(1)
			return
		}
		it.sql, it.hasText = text, true
	}
	tables, err := safeParse(it.sql)
	if err != nil {
		t.fail(it, err)
		return
	}
	set.put(it.hash)
	if err := t.opts.Sink.AddText(t.ctx, it.date, TextTypeTables, it.hash, tables); err != nil {
		t.c.storageErr.Add(1)
		return
	}
	t.c.stored.Add(1)
	if tables == "" {
		t.c.empty.Add(1)
	}
}

func safeParse(sql string) (s string, err error) {
	defer func() {
		if r := recover(); r != nil {
			s, err = "", fmt.Errorf("sqltable: parser panic: %v", r)
		}
	}()
	return Parse(sql)
}

// fail is the Java catch(Throwable) branch. The SQL text itself is never
// logged (D-S1): only its hash and byte length.
func (t *Tables) fail(it item, err error) {
	t.failed.put(it.hash)
	t.failedLen.Store(int64(t.failed.len()))
	t.c.parseFailures.Add(1)
	if t.opts.LogFailEnabled != nil && t.opts.LogFailEnabled() {
		t.log(LogParseFailed, 0, fmt.Sprintf("sql hash=%d length=%d\n%v", it.hash, len(it.sql), err))
	}
}

// parsedSet is parsed.get(date) with creation on miss; at most ParsedDatesMax
// dates are kept and the oldest inserted date is evicted (no reordering on get).
func (t *Tables) parsedSet(date string) *intLinkedSet {
	for _, d := range t.parsed {
		if d.date == date {
			return d.set
		}
	}
	s := newIntLinkedSet(ParsedPerDateMax)
	if len(t.parsed) >= ParsedDatesMax {
		copy(t.parsed, t.parsed[1:])
		t.parsed = t.parsed[:len(t.parsed)-1]
	}
	t.parsed = append(t.parsed, datedSet{date: date, set: s})
	return s
}

// intLinkedSet is IntLinkedSet with setMax and put(MODE.LAST): a repeated put
// keeps the original position, and a put at capacity evicts the oldest key.
type intLinkedSet struct {
	max  int
	keys map[int32]struct{}
	ring []int32
	head int
}

func newIntLinkedSet(max int) *intLinkedSet {
	return &intLinkedSet{max: max, keys: make(map[int32]struct{})}
}

func (s *intLinkedSet) contains(k int32) bool { _, ok := s.keys[k]; return ok }
func (s *intLinkedSet) len() int              { return len(s.keys) }

func (s *intLinkedSet) put(k int32) {
	if s.contains(k) {
		return
	}
	if len(s.ring) < s.max {
		s.ring = append(s.ring, k)
	} else {
		delete(s.keys, s.ring[s.head])
		s.ring[s.head] = k
		s.head = (s.head + 1) % s.max
	}
	s.keys[k] = struct{}{}
}
