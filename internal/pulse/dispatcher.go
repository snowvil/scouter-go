package pulse

import (
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
)

// Kafka header names and values (the plugin stream constants: APM type
// SCOUTER, serializer version V1). The application header carries the
// extracted application string, never a UUID.
const (
	HeaderAPMType           = "apm_type"
	HeaderSerializerVersion = "xlog_serializer_version"
	HeaderApplication       = "xlog_application"
	APMTypeScouter          = "SCOUTER"
	SerializerVersionV1     = "V1"
)

// FilteredTraceTTL is the plugin TTL of an HTTP-status-filtered txid.
const FilteredTraceTTL = 10 * time.Minute

// Default per-queue byte budgets (Options.MaxXLogQueueBytes and
// MaxProfileQueueBytes; rationale in CONTRACT.md). The Java plugin bounds its
// queues by count only and keeps a reference to the engine's pack.
const (
	DefaultMaxXLogQueueBytes    = 16 << 20
	DefaultMaxProfileQueueBytes = 64 << 20
)

// eventInterval rate-limits the per-queue "dropped" and worker panic Events;
// the counters still count every occurrence.
const eventInterval = 10 * time.Second

// Header is one Kafka record header; Value bytes are UTF-8.
type Header struct {
	Key   string
	Value []byte
}

// Record is the producer-independent Kafka record this package emits. Key is the
// Java LongSerializer encoding of txid (8-byte big-endian).
type Record struct {
	Topic     string
	Partition int32
	Key       []byte
	Value     []byte
	Headers   []Header
}

// Sink accepts a record without waiting for a broker response. It must return
// promptly: an error means the record was not accepted (buffer full, closed).
type Sink interface {
	Enqueue(ctx context.Context, rec Record) error
}

// Event reports a filtered, failed or discarded task to the owner's logger.
type Event struct {
	Queue  string // "xlog-meta" or "xlog-profile"
	Kind   string // filter stage, "sink", "error", "dropped", "shutdown"
	TxID   int64
	Detail string
	Err    error
}

// FilterStage enumerates the plugin filter log call sites. The
// names match the filtered_stage values of the Java golden fixture.
type FilterStage int

const (
	FilterExtractFail          FilterStage = iota // application extraction failed
	FilterApplication                             // excluded application
	FilterHTTPStatus                              // dropped by HTTP status rule
	FilterTransactionExtension                    // excluded transaction extension
	FilterSpringURI                               // Spring URI filter
	FilterFilteredTrace                           // profile skipped by filtered txid
	filterStageCount
)

func (s FilterStage) String() string {
	switch s {
	case FilterExtractFail:
		return "application-extract"
	case FilterApplication:
		return "application-exclude"
	case FilterHTTPStatus:
		return "http-status"
	case FilterTransactionExtension:
		return "transaction-extension"
	case FilterSpringURI:
		return "spring-uri"
	case FilterFilteredTrace:
		return "profile-filtered-txid"
	}
	return fmt.Sprintf("stage(%d)", int(s))
}

// QueueStats are cumulative (Java resets every 10 seconds after logging; this
// port keeps monotonic counters). Enqueued counts accepted offers, Dropped the
// offers rejected by a full queue (not those rejected after Close), Filtered the
// tasks excluded per stage, Sent the records the Sink accepted (not broker
// acknowledgement), Failed the Sink rejections, Errors the lookup/convert/encode
// failures (Java logs these as worker errors without a counter), and
// ShutdownDiscarded the tasks still queued when the Close deadline expired.
// Pending is the current queue length. PendingBytes is the estimated memory of
// the accepted tasks not yet finished (queued plus being processed), bounded
// by MaxPendingBytes; DroppedByBytes is the part of Dropped rejected by that
// byte bound rather than by the task count.
type QueueStats struct {
	Enqueued, Dropped, Sent, Failed, Errors, ShutdownDiscarded uint64
	Filtered                                                   uint64
	FilteredByStage                                            [filterStageCount]uint64
	Pending                                                    int
	PendingBytes, MaxPendingBytes                              int64
	DroppedByBytes                                             uint64
}

type Stats struct {
	XLog, Profile  QueueStats
	FilteredTraces int
}

type queueCounters struct {
	enqueued, dropped, sent, failed, errors, shutdownDiscarded atomic.Uint64
	filtered, droppedByBytes                                   atomic.Uint64
	filteredByStage                                            [filterStageCount]atomic.Uint64
}

func (c *queueCounters) snapshot(pending int) QueueStats {
	s := QueueStats{Enqueued: c.enqueued.Load(), Dropped: c.dropped.Load(), Sent: c.sent.Load(), Failed: c.failed.Load(),
		Errors: c.errors.Load(), ShutdownDiscarded: c.shutdownDiscarded.Load(), Filtered: c.filtered.Load(), Pending: pending,
		DroppedByBytes: c.droppedByBytes.Load()}
	for i := range s.FilteredByStage {
		s.FilteredByStage[i] = c.filteredByStage[i].Load()
	}
	return s
}

// Options configures a Dispatcher. Sink, Lookup and Location are required.
type Options struct {
	Config   Config
	Sink     Sink
	Lookup   TextLookup
	Location *time.Location
	Clock    func() time.Time
	OnEvent  func(Event)
	// TryEvent, when set, is used instead of OnEvent. It must not block and
	// returns false when the event could not be queued; a rate-limited Event
	// (queue drop, worker panic) is then retried on the next occurrence
	// instead of after eventInterval. With OnEvent delivery is assumed.
	TryEvent func(Event) bool
	// MaxFilteredTraces bounds the HTTP-status-filtered txid map. Java keeps an
	// unbounded ConcurrentHashMap that only sheds expired entries once it holds
	// 10,000; here the oldest entry is evicted beyond this size (default 100,000).
	MaxFilteredTraces int
	// MaxXLogQueueBytes and MaxProfileQueueBytes bound the estimated memory
	// of each queue's accepted, unfinished tasks on top of the
	// DispatchQueueCapacity count bound; an offer beyond either is dropped.
	// Zero takes DefaultMaxXLogQueueBytes / DefaultMaxProfileQueueBytes.
	MaxXLogQueueBytes, MaxProfileQueueBytes int64
}

// task is one offered pack. bytes is its charge against the queue budget,
// released when a worker finishes or discards it.
type task struct {
	xlog    *protocol.XLogPack
	profile *protocol.ProfilePack
	bytes   int64
}

type queue struct {
	name     string
	items    chan task
	maxBytes int64
	bytes    atomic.Int64
	stats    queueCounters
	workers  sync.WaitGroup
	// dropEvents and panicEvents rate-limit the "dropped" and panic Events.
	dropEvents, panicEvents eventLimiter
}

// eventLimiter lets at most one delivered Event through per eventInterval.
// now is the dispatcher's elapsed time (monotonic with the default Clock); a
// window further ahead than one interval means the clock went backwards and
// is reset. The window only advances when the Event was delivered. A limiter
// busy in another goroutine is skipped, never waited for.
type eventLimiter struct {
	mu   sync.Mutex
	next time.Duration
}

func (l *eventLimiter) emit(now time.Duration, send func() bool) {
	if !l.mu.TryLock() {
		return
	}
	defer l.mu.Unlock()
	if now < l.next && l.next-now <= eventInterval {
		return
	}
	if send() {
		l.next = now + eventInterval
	}
}

// reserve charges n bytes unless that would exceed maxBytes.
func (q *queue) reserve(n int64) bool {
	for {
		cur := q.bytes.Load()
		if cur+n > q.maxBytes {
			return false
		}
		if q.bytes.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

func (q *queue) statsSnapshot() QueueStats {
	s := q.stats.snapshot(len(q.items))
	s.PendingBytes, s.MaxPendingBytes = q.bytes.Load(), q.maxBytes
	return s
}

// Task byte estimates: the pack struct plus the variable-length fields the
// task keeps alive (its own byte clones and the shared, immutable strings).
// Conversion and JSON buffers of the task being processed are not counted;
// they are bounded by the worker count.
var (
	xlogPackBytes    = int64(reflect.TypeFor[protocol.XLogPack]().Size())
	profilePackBytes = int64(reflect.TypeFor[protocol.ProfilePack]().Size())
)

func xlogTaskBytes(p *protocol.XLogPack) int64 {
	return xlogPackBytes + int64(len(p.IPAddress)+len(p.Extra)+len(p.CountryCode)+
		len(p.Text1)+len(p.Text2)+len(p.Text3)+len(p.Text4)+len(p.Text5))
}

func profileTaskBytes(p *protocol.ProfilePack) int64 {
	return profilePackBytes + int64(len(p.Profile))
}

// Dispatcher owns the two bounded queues and worker pools of the Java plugin
// (one dispatcher per type). Offers never block; a full queue drops
// the new task. Workers convert, filter, serialize and hand records to the Sink.
type Dispatcher struct {
	opts     Options
	xlog     *queue
	profile  *queue
	running  atomic.Bool
	ctx      context.Context
	cancel   context.CancelFunc
	abandon  atomic.Bool
	closeMu  sync.Mutex
	closed   bool
	filtered filteredTraces
	// start is Clock() at New; Clock().Sub(start) is monotonic for time.Now.
	start time.Time
	// offerMu orders Offer (read side) against Close (write side) so a
	// concurrent Offer can never send on a closed queue channel.
	offerMu sync.RWMutex
}

var ErrDispatcherOptions = errors.New("pulse dispatcher options")

// New validates options and starts the worker goroutines.
func New(opts Options) (*Dispatcher, error) {
	if opts.Sink == nil || opts.Lookup == nil || opts.Location == nil {
		return nil, fmt.Errorf("%w: sink, lookup and location are required", ErrDispatcherOptions)
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	if opts.MaxFilteredTraces == 0 {
		opts.MaxFilteredTraces = 100000
	}
	if opts.MaxXLogQueueBytes == 0 {
		opts.MaxXLogQueueBytes = DefaultMaxXLogQueueBytes
	}
	if opts.MaxProfileQueueBytes == 0 {
		opts.MaxProfileQueueBytes = DefaultMaxProfileQueueBytes
	}
	if opts.MaxFilteredTraces < 1 || opts.Config.DispatchQueueCapacity < 1 || opts.Config.DispatchWorkerCount < 1 || opts.Config.TopicPartitionCount < 1 ||
		opts.MaxXLogQueueBytes < 1 || opts.MaxProfileQueueBytes < 1 {
		return nil, fmt.Errorf("%w: bounds must be positive", ErrDispatcherOptions)
	}
	d := &Dispatcher{opts: opts, start: opts.Clock()}
	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.filtered.init(opts.MaxFilteredTraces)
	d.xlog = d.newQueue("xlog-meta", opts.MaxXLogQueueBytes)
	d.profile = d.newQueue("xlog-profile", opts.MaxProfileQueueBytes)
	d.running.Store(true)
	return d, nil
}

func (d *Dispatcher) newQueue(name string, maxBytes int64) *queue {
	q := &queue{name: name, items: make(chan task, d.opts.Config.DispatchQueueCapacity), maxBytes: maxBytes}
	for i := 0; i < d.opts.Config.DispatchWorkerCount; i++ {
		q.workers.Add(1)
		go d.work(q)
	}
	return q
}

// event reports whether e was handed to the owner.
func (d *Dispatcher) event(e Event) bool {
	if !d.hasEvents() {
		return false
	}
	switch e.Kind {
	case "sink", "error", "dropped", "shutdown":
	default: // filter stages: the plugin filter log is gated by ext_plugin_pulse_debug
		if !d.opts.Config.Debug {
			return false
		}
	}
	if d.opts.TryEvent != nil {
		return d.opts.TryEvent(e)
	}
	d.opts.OnEvent(e)
	return true
}

func (d *Dispatcher) hasEvents() bool { return d.opts.TryEvent != nil || d.opts.OnEvent != nil }

func (d *Dispatcher) elapsed() time.Duration { return d.opts.Clock().Sub(d.start) }

// OfferXLog clones the pack and enqueues it. The
// caller keeps ownership of p. false means dropped (queue full by count or
// bytes) or not running.
func (d *Dispatcher) OfferXLog(p *protocol.XLogPack) bool {
	if p == nil || !d.running.Load() {
		return false
	}
	return d.offer(d.xlog, xlogTaskBytes(p), p.TxID, func() task {
		c := *p
		c.IPAddress = cloneBytes(p.IPAddress)
		c.Extra = cloneBytes(p.Extra)
		return task{xlog: &c}
	})
}

// OfferProfile clones the pack and enqueues it. The clone is
// kept: the engine's Profile may alias a whole UDP datagram or reassembly
// buffer, and the ingest hook contract forbids retaining the pack.
func (d *Dispatcher) OfferProfile(p *protocol.ProfilePack) bool {
	if p == nil || !d.running.Load() {
		return false
	}
	return d.offer(d.profile, profileTaskBytes(p), p.TxID, func() task {
		c := *p
		c.Profile = cloneBytes(p.Profile)
		return task{profile: &c}
	})
}

// offer checks both bounds before clone builds the task, so a drop costs no
// copy. It never waits.
func (d *Dispatcher) offer(q *queue, size, txid int64, clone func() task) bool {
	d.offerMu.RLock()
	defer d.offerMu.RUnlock()
	if !d.running.Load() {
		return false
	}
	if len(q.items) == cap(q.items) {
		d.dropped(q, false, txid)
		return false
	}
	if !q.reserve(size) {
		d.dropped(q, true, txid)
		return false
	}
	t := clone()
	t.bytes = size
	select {
	case q.items <- t:
		q.stats.enqueued.Add(1)
		return true
	default:
		q.bytes.Add(-size)
		d.dropped(q, false, txid)
		return false
	}
}

// dropped counts a rejected offer and emits at most one "dropped" Event per
// queue per eventInterval, carrying the totals so far.
func (d *Dispatcher) dropped(q *queue, byBytes bool, txid int64) {
	q.stats.dropped.Add(1)
	reason := "queue full"
	if byBytes {
		q.stats.droppedByBytes.Add(1)
		reason = "queue bytes full"
	}
	if !d.hasEvents() {
		return
	}
	q.dropEvents.emit(d.elapsed(), func() bool {
		s := q.statsSnapshot()
		return d.event(Event{Queue: q.name, Kind: "dropped", TxID: txid, Detail: fmt.Sprintf("%s: tasks %d/%d, bytes %d/%d, dropped %d (by bytes %d)",
			reason, s.Pending, cap(q.items), s.PendingBytes, s.MaxPendingBytes, s.Dropped, s.DroppedByBytes)})
	})
}

// countFailure attributes a worker failure. A task interrupted by the forced
// shutdown (context cancelled after Close gave up draining) is shutdown
// residue, not a processing error or a sink failure.
func (d *Dispatcher) countFailure(q *queue, counter *atomic.Uint64, err error) {
	if d.abandon.Load() && errors.Is(err, context.Canceled) {
		q.stats.shutdownDiscarded.Add(1)
		return
	}
	counter.Add(1)
}

func (d *Dispatcher) work(q *queue) {
	defer q.workers.Done()
	for t := range q.items {
		if d.abandon.Load() {
			q.stats.shutdownDiscarded.Add(1)
		} else {
			d.process(q, t)
		}
		q.bytes.Add(-t.bytes)
	}
}

// process handles one task. A panic in conversion, lookup or the Sink fails
// only this task (the Java worker loop catches Exception per task): it
// counts as an Error with a rate-limited Event and the worker takes the next
// task.
func (d *Dispatcher) process(q *queue, t task) {
	var err error
	var txid int64
	defer func() {
		if r := recover(); r != nil {
			q.stats.errors.Add(1)
			perr := fmt.Errorf("worker panic: %v", r)
			q.panicEvents.emit(d.elapsed(), func() bool {
				return d.event(Event{Queue: q.name, Kind: "error", TxID: txid, Err: perr})
			})
		}
	}()
	if t.profile != nil {
		txid = t.profile.TxID
		err = d.processProfile(q, t.profile, 0)
	} else {
		txid = t.xlog.TxID
		err = d.processXLog(q, t.xlog)
	}
	if err != nil {
		d.countFailure(q, &q.stats.errors, err)
		d.event(Event{Queue: q.name, Kind: "error", TxID: txid, Err: err})
	}
}

func (d *Dispatcher) filteredOut(q *queue, stage FilterStage, txid int64, detail string) {
	q.stats.filtered.Add(1)
	q.stats.filteredByStage[stage].Add(1)
	d.event(Event{Queue: q.name, Kind: stage.String(), TxID: txid, Detail: detail})
}

func (d *Dispatcher) application(ctx context.Context, date string, objHash int32) (string, bool, error) {
	name, found, err := d.opts.Lookup(ctx, date, TextObject, objHash)
	if err != nil {
		return "", false, err
	}
	app, ok := ExtractApplication(name, found)
	return app, ok, nil
}

// processXLog follows the plugin trace worker steps in order.
func (d *Dispatcher) processXLog(q *queue, p *protocol.XLogPack) error {
	cfg := &d.opts.Config
	ctx := d.ctx
	application, ok, err := d.application(ctx, scoutertime.Date(p.EndTime, d.opts.Location), p.ObjHash)
	if err != nil {
		return err
	}
	if !ok {
		d.filteredOut(q, FilterExtractFail, p.TxID, "")
		return nil
	}
	if cfg.IsExcludeApplication(application) {
		d.filteredOut(q, FilterApplication, p.TxID, application)
		return nil
	}
	if cfg.IsDropHTTPStatus(application, p.Status) {
		d.filtered.remember(p.TxID, d.opts.Clock())
		d.filteredOut(q, FilterHTTPStatus, p.TxID, fmt.Sprintf("application=%s status=%d", application, p.Status))
		return nil
	}
	trace, err := ConvertXLog(ctx, d.opts.Lookup, d.opts.Location, p)
	if err != nil {
		return err
	}
	if cfg.IsExcludeTransaction(trace.TransactionDictContent) {
		d.filteredOut(q, FilterTransactionExtension, p.TxID, "")
		return nil
	}
	if cfg.IsSpringBasedApplication(application) && SpringURIFiltered(*trace.TransactionDictContent) {
		d.filteredOut(q, FilterSpringURI, p.TxID, "")
		return nil
	}
	if cond, has := cfg.RebaseConditionFor(application); has {
		trace.Rebase(cond)
	}
	trace.RemoveAfter(cfg.RebaseTransactionGlobal)
	payload, err := trace.MarshalJSON()
	if err != nil {
		return err
	}
	return d.send(q, cfg.XLogMetaTopic, application, p.TxID, payload)
}

// processProfile follows the plugin profile worker steps in order. elapsed is always 0 from the
// worker (see ConvertProfile).
func (d *Dispatcher) processProfile(q *queue, p *protocol.ProfilePack, elapsed int32) error {
	cfg := &d.opts.Config
	ctx := d.ctx
	now := d.opts.Clock()
	if d.filtered.contains(p.TxID, now) {
		d.filteredOut(q, FilterFilteredTrace, p.TxID, "")
		return nil
	}
	application, ok, err := d.application(ctx, scoutertime.Date(p.Time, d.opts.Location), p.ObjHash)
	if err != nil {
		return err
	}
	if !ok {
		d.filteredOut(q, FilterExtractFail, p.TxID, "")
		return nil
	}
	if cfg.IsExcludeApplication(application) {
		d.filteredOut(q, FilterApplication, p.TxID, application)
		return nil
	}
	span, err := ConvertProfile(ctx, d.opts.Lookup, d.opts.Location, p, elapsed, now)
	if err != nil {
		return err
	}
	if cfg.IsExcludeTransaction(span.TransactionDictContent) {
		d.filteredOut(q, FilterTransactionExtension, p.TxID, "")
		return nil
	}
	if cfg.IsSpringBasedApplication(application) && SpringURIFiltered(*span.TransactionDictContent) {
		d.filteredOut(q, FilterSpringURI, p.TxID, "")
		return nil
	}
	if cond, has := cfg.RebaseConditionFor(application); has {
		span.Rebase(cond)
	}
	span.RemoveAfter(cfg.RebaseTransactionGlobal)
	payload, err := span.MarshalJSON()
	if err != nil {
		return err
	}
	return d.send(q, cfg.XLogProfileTopic, application, p.TxID, payload)
}

// BuildRecord assembles the Kafka record exactly as the plugin Kafka dispatcher does.
func BuildRecord(topic string, partitionCount int32, application string, txid int64, payload []byte) Record {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(txid))
	return Record{
		Topic:     topic,
		Partition: Partition(application, partitionCount),
		Key:       key,
		Value:     payload,
		Headers: []Header{
			{Key: HeaderAPMType, Value: []byte(APMTypeScouter)},
			{Key: HeaderSerializerVersion, Value: []byte(SerializerVersionV1)},
			{Key: HeaderApplication, Value: []byte(application)},
		},
	}
}

func (d *Dispatcher) send(q *queue, topic, application string, txid int64, payload []byte) error {
	rec := BuildRecord(topic, d.opts.Config.TopicPartitionCount, application, txid, payload)
	if err := d.opts.Sink.Enqueue(d.ctx, rec); err != nil {
		// Java counts the async callback failure and logs; no re-enqueue.
		d.countFailure(q, &q.stats.failed, err)
		d.event(Event{Queue: q.name, Kind: "sink", TxID: txid, Detail: topic, Err: err})
		return nil
	}
	q.stats.sent.Add(1)
	return nil
}

// Close stops accepting offers and lets workers drain what is queued until ctx
// is done; after that remaining tasks are counted as ShutdownDiscarded and the
// worker context is cancelled. Java calls shutdownNow without draining; the
// bounded drain is an intended difference.
func (d *Dispatcher) Close(ctx context.Context) error {
	d.closeMu.Lock()
	defer d.closeMu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	d.offerMu.Lock()
	d.running.Store(false)
	close(d.xlog.items)
	close(d.profile.items)
	d.offerMu.Unlock()
	done := make(chan struct{})
	go func() {
		d.xlog.workers.Wait()
		d.profile.workers.Wait()
		close(done)
	}()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
		d.abandon.Store(true)
		d.cancel()
		<-done
	}
	d.cancel()
	return err
}

func (d *Dispatcher) Stats() Stats {
	return Stats{XLog: d.xlog.statsSnapshot(), Profile: d.profile.statsSnapshot(), FilteredTraces: d.filtered.size()}
}

// filteredTraces is the shared HTTP-status-filtered txid map with
// a size bound. txid 0 is never recorded.
type filteredTraces struct {
	mu    sync.Mutex
	max   int
	items map[int64]*list.Element
	order *list.List
}

type filteredEntry struct {
	txid    int64
	expires time.Time
}

func (f *filteredTraces) init(max int) {
	f.max = max
	f.items = make(map[int64]*list.Element)
	f.order = list.New()
}

func (f *filteredTraces) remember(txid int64, now time.Time) {
	if txid == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	expires := now.Add(FilteredTraceTTL)
	if el := f.items[txid]; el != nil {
		el.Value.(*filteredEntry).expires = expires
		f.order.MoveToBack(el)
		return
	}
	// Java sheds expired entries once the map holds 10,000.
	if len(f.items) >= 10000 {
		for el := f.order.Front(); el != nil; {
			next := el.Next()
			if e := el.Value.(*filteredEntry); e.expires.Before(now) {
				delete(f.items, e.txid)
				f.order.Remove(el)
			}
			el = next
		}
	}
	for len(f.items) >= f.max {
		first := f.order.Front()
		delete(f.items, first.Value.(*filteredEntry).txid)
		f.order.Remove(first)
	}
	f.items[txid] = f.order.PushBack(&filteredEntry{txid: txid, expires: expires})
}

func (f *filteredTraces) contains(txid int64, now time.Time) bool {
	if txid == 0 {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	el := f.items[txid]
	if el == nil {
		return false
	}
	if e := el.Value.(*filteredEntry); e.expires.Before(now) {
		delete(f.items, txid)
		f.order.Remove(el)
		return false
	}
	return true
}

func (f *filteredTraces) size() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.items)
}
