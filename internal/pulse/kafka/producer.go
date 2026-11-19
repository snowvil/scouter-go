package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Stats are cumulative counters. Pending counts records enqueued but not yet
// completed (sent, failed or abandoned); PendingBytes is the BufferBytes usage.
// DroppedBufferFull is the part of Dropped refused by BufferBytes.
type Stats struct {
	Enqueued, SentBatches, SentRecords, Failed, Dropped uint64
	MetadataRefreshes, MetadataFailures, Reconnects     uint64
	Pending, PendingBytes                               int64
	DroppedBufferFull                                   uint64
}

// Producer batches records per topic partition and ships them to partition
// leaders over one connection per broker. Enqueue is memory-only and never
// waits on the network. Goroutines: one coordinator (metadata, linger,
// dispatch) owned by the Producer, one worker per known broker owned by the
// coordinator. All exit through Close.
type Producer struct {
	cfg Config
	acc accumulator

	wake    chan struct{}
	flush   atomic.Bool
	closing chan struct{}
	done    chan struct{}
	once    sync.Once
	closed  atomic.Bool
	// forceCtx is cancelled when Close's context expires: it aborts dials and
	// closes connections so blocked workers fail fast.
	forceCtx    context.Context
	forceCancel context.CancelFunc

	// coordinator-owned
	meta        *cluster
	metaStale   atomic.Bool
	nextRefresh time.Time
	backoff     time.Duration

	workersMu sync.Mutex
	workers   map[int32]*worker
	wg        sync.WaitGroup

	enqueued, sentBatches, sentRecords, failed, dropped uint64
	droppedBufferFull                                   uint64
	refreshes, refreshFailures, reconnects              uint64
	abandoned                                           int64
}

type cluster struct {
	brokers map[int32]string
	leaders map[topicPartition]int32
	topics  map[string]struct{}
	fetched time.Time
}

func (c *cluster) leader(tp topicPartition) (int32, string, bool) {
	if c == nil {
		return 0, "", false
	}
	node, ok := c.leaders[tp]
	if !ok {
		return 0, "", false
	}
	addr, ok := c.brokers[node]
	return node, addr, ok
}

// NewProducer validates cfg and starts the coordinator. It does not contact a
// broker: metadata is fetched lazily so a down cluster never blocks startup.
func NewProducer(cfg Config) (*Producer, error) {
	cfg, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	p := &Producer{
		cfg:     cfg,
		wake:    make(chan struct{}, 1),
		closing: make(chan struct{}),
		done:    make(chan struct{}),
		workers: make(map[int32]*worker),
	}
	p.acc.init(cfg.MaxTopics)
	p.forceCtx, p.forceCancel = context.WithCancel(context.Background())
	p.backoff = cfg.RetryBackoff
	go p.run()
	return p, nil
}

// Enqueue encodes rec into its partition batch. It returns ErrBufferFull
// without waiting when BufferBytes would be exceeded, ErrRecordTooLarge,
// ErrTooManyTopics, ErrInvalidRecord or ErrClosed. Every rejection is counted
// in Stats.Dropped. The record's byte slices are copied; the caller keeps
// ownership. ctx is only checked on entry since nothing blocks.
func (p *Producer) Enqueue(ctx context.Context, rec Record) error {
	if err := ctx.Err(); err != nil {
		atomic.AddUint64(&p.dropped, 1)
		return err
	}
	if p.closed.Load() {
		atomic.AddUint64(&p.dropped, 1)
		return ErrClosed
	}
	if err := validateRecord(rec); err != nil {
		atomic.AddUint64(&p.dropped, 1)
		return err
	}
	wake, err := p.acc.add(rec, time.Now(), &p.cfg)
	if wake {
		p.signal()
	}
	if err != nil {
		atomic.AddUint64(&p.dropped, 1)
		if errors.Is(err, ErrBufferFull) {
			atomic.AddUint64(&p.droppedBufferFull, 1)
		}
		return err
	}
	atomic.AddUint64(&p.enqueued, 1)
	return nil
}

// Flush seals open batches and waits until nothing is pending or ctx ends.
func (p *Producer) Flush(ctx context.Context) error {
	p.flush.Store(true)
	p.signal()
	idle := p.acc.idleChan()
	if idle == nil {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		if p.acc.pendingRecords() == 0 {
			return nil
		}
		return ErrClosed
	}
}

// Close stops accepting records, tries to deliver what is buffered until ctx
// ends, then abandons the rest. It returns the number of records that were
// never attempted (abandoned); records attempted but failed are in
// Stats.Failed. Close is idempotent; later calls return the same count.
func (p *Producer) Close(ctx context.Context) (int64, error) {
	p.once.Do(func() {
		p.closed.Store(true)
		close(p.closing)
	})
	select {
	case <-p.done:
	case <-ctx.Done():
		p.forceCancel()
		p.closeAllConns()
		<-p.done
	}
	abandoned := atomic.LoadInt64(&p.abandoned)
	if abandoned > 0 {
		return abandoned, fmt.Errorf("%w: %d records abandoned", ErrAbandoned, abandoned)
	}
	return 0, nil
}

func (p *Producer) Stats() Stats {
	pending, bytes := p.acc.pending()
	return Stats{
		Enqueued:          atomic.LoadUint64(&p.enqueued),
		SentBatches:       atomic.LoadUint64(&p.sentBatches),
		SentRecords:       atomic.LoadUint64(&p.sentRecords),
		Failed:            atomic.LoadUint64(&p.failed),
		Dropped:           atomic.LoadUint64(&p.dropped),
		MetadataRefreshes: atomic.LoadUint64(&p.refreshes),
		MetadataFailures:  atomic.LoadUint64(&p.refreshFailures),
		Reconnects:        atomic.LoadUint64(&p.reconnects),
		Pending:           pending,
		PendingBytes:      bytes,
		DroppedBufferFull: atomic.LoadUint64(&p.droppedBufferFull),
	}
}

func (p *Producer) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *Producer) report(op, broker string, err error) {
	if p.cfg.OnError != nil {
		p.cfg.OnError(ErrorEvent{Operation: op, Broker: broker, Err: err})
	}
}

// complete updates the counters, then releases the batch's buffer
// accounting. Callers report the error first so an observer that sees the
// counter move can also see the event, and the counters move before release
// so that Flush (which returns when the accumulator goes idle) never returns
// with a sent or failed batch still uncounted.
func (p *Producer) complete(b *batch, err error) {
	if err != nil {
		atomic.AddUint64(&p.failed, uint64(b.count))
	} else {
		atomic.AddUint64(&p.sentBatches, 1)
		atomic.AddUint64(&p.sentRecords, uint64(b.count))
	}
	p.acc.release(b)
}

// run is the coordinator loop.
func (p *Producer) run() {
	defer close(p.done)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		p.maybeRefreshMetadata()
		next := p.dispatch(time.Now(), false)
		wait := p.waitFor(next)
		timer.Reset(wait)
		select {
		case <-p.wake:
		case <-timer.C:
		case <-p.closing:
			p.shutdown()
			return
		}
	}
}

// waitFor converts the next deadline into a timer wait, bounded above by the
// metadata refresh period so periodic refresh still happens when idle.
func (p *Producer) waitFor(next time.Time) time.Duration {
	wait := p.cfg.MetadataRefresh
	if p.meta != nil {
		if until := time.Until(p.meta.fetched.Add(p.cfg.MetadataRefresh)); until < wait {
			wait = until
		}
	}
	if !next.IsZero() {
		if until := time.Until(next); until < wait {
			wait = until
		}
	}
	if wait < time.Millisecond {
		wait = time.Millisecond
	}
	return wait
}

// shutdown runs the bounded drain: dispatch everything, keep retrying while
// anything stays queued and the force context is alive, then abandon.
func (p *Producer) shutdown() {
	for {
		p.maybeRefreshMetadata()
		p.dispatch(time.Now(), true)
		if p.acc.queued() == 0 || p.forceCtx.Err() != nil {
			break
		}
		select {
		case <-time.After(p.cfg.RetryBackoff):
		case <-p.forceCtx.Done():
		}
	}
	abandoned := p.acc.abandon()
	atomic.AddInt64(&p.abandoned, abandoned)
	p.workersMu.Lock()
	for _, w := range p.workers {
		close(w.jobs)
	}
	p.workersMu.Unlock()
	p.wg.Wait()
}

// maybeRefreshMetadata refreshes when there is no cluster view, it is stale,
// expired or misses a topic, rate-limited by backoff. It runs on the
// coordinator goroutine; Enqueue is unaffected while it blocks on the network.
func (p *Producer) maybeRefreshMetadata() {
	now := time.Now()
	topics := p.acc.topicList()
	if len(topics) == 0 {
		return
	}
	need := p.meta == nil || p.metaStale.Load() || now.Sub(p.meta.fetched) >= p.cfg.MetadataRefresh
	if !need {
		for _, t := range topics {
			if _, ok := p.meta.topics[t]; !ok {
				need = true
				break
			}
		}
	}
	if !need || now.Before(p.nextRefresh) {
		return
	}
	meta, err := p.fetchMetadata(topics)
	if err != nil {
		atomic.AddUint64(&p.refreshFailures, 1)
		p.nextRefresh = now.Add(p.backoff)
		p.backoff = min(p.backoff*2, p.cfg.MaxRetryBackoff)
		return
	}
	p.meta = meta
	p.metaStale.Store(false)
	p.backoff = p.cfg.RetryBackoff
	p.nextRefresh = now.Add(p.cfg.RetryBackoff)
	atomic.AddUint64(&p.refreshes, 1)
}

// fetchMetadata tries the last known brokers first, then bootstrap addresses.
// Each candidate is bounded by DialTimeout + RequestTimeout. The connection
// is short-lived: refreshes are rare (5m or on error with backoff).
func (p *Producer) fetchMetadata(topics []string) (*cluster, error) {
	var candidates []string
	seen := make(map[string]struct{})
	if p.meta != nil {
		nodes := make([]int32, 0, len(p.meta.brokers))
		for node := range p.meta.brokers {
			nodes = append(nodes, node)
		}
		sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
		for _, node := range nodes {
			addr := p.meta.brokers[node]
			if _, dup := seen[addr]; !dup {
				seen[addr] = struct{}{}
				candidates = append(candidates, addr)
			}
		}
	}
	for _, addr := range p.cfg.Bootstrap {
		if _, dup := seen[addr]; !dup {
			seen[addr] = struct{}{}
			candidates = append(candidates, addr)
		}
	}
	var last error
	for _, addr := range candidates {
		if p.forceCtx.Err() != nil {
			return nil, p.forceCtx.Err()
		}
		conn, err := dialBroker(p.forceCtx, &p.cfg, addr)
		if err != nil {
			last = err
			p.report("metadata.dial", addr, err)
			continue
		}
		resp, err := conn.fetchMetadata(topics)
		conn.close()
		if err != nil {
			last = err
			p.report("metadata.fetch", addr, err)
			continue
		}
		return p.buildCluster(resp, addr), nil
	}
	if last == nil {
		last = errors.New("kafka: no metadata candidates")
	}
	return nil, last
}

func (p *Producer) buildCluster(resp *metadataResponse, from string) *cluster {
	c := &cluster{
		brokers: make(map[int32]string, len(resp.Brokers)),
		leaders: make(map[topicPartition]int32),
		topics:  make(map[string]struct{}, len(resp.Topics)),
		fetched: time.Now(),
	}
	for _, b := range resp.Brokers {
		c.brokers[b.NodeID] = fmt.Sprintf("%s:%d", b.Host, b.Port)
	}
	for _, t := range resp.Topics {
		c.topics[t.Name] = struct{}{}
		if t.ErrorCode != 0 {
			p.report("metadata.topic", from, fmt.Errorf("topic %q: %w", t.Name, Error{Code: t.ErrorCode}))
			continue
		}
		for _, part := range t.Partitions {
			if part.ErrorCode != 0 && part.Leader < 0 {
				p.report("metadata.partition", from, fmt.Errorf("%s/%d: %w", t.Name, part.Index, Error{Code: part.ErrorCode}))
				continue
			}
			if part.Leader < 0 {
				continue
			}
			if _, ok := c.brokers[part.Leader]; !ok {
				p.report("metadata.partition", from, fmt.Errorf("%s/%d: leader %d not in broker list", t.Name, part.Index, part.Leader))
				continue
			}
			c.leaders[topicPartition{t.Name, part.Index}] = part.Leader
		}
	}
	return c
}

type job struct {
	addr    string
	batches []*batch
}

type worker struct {
	node int32
	jobs chan *job
	mu   sync.Mutex
	conn *brokerConn
}

// dispatch seals due batches, fails expired ones and hands the rest to
// broker workers, at most one batch per topic partition per job and at most
// MaxRequestBytes per job. Batches without a known leader or whose worker
// queue is full stay queued. It returns the next time the loop should wake.
func (p *Producer) dispatch(now time.Time, final bool) time.Time {
	flush := final || p.flush.Swap(false)
	ready, next := p.acc.collect(now, p.cfg.Linger, flush)
	if len(ready) == 0 {
		return next
	}
	var keep []*batch
	type plan struct {
		w    *worker
		addr string
		jobs []*job
	}
	plans := make(map[int32]*plan)
	var order []int32
	for _, b := range ready {
		if now.Sub(b.created) >= p.cfg.DeliveryTimeout {
			p.report("produce.expired", "", fmt.Errorf("%s/%d: %w", b.tp.topic, b.tp.partition, ErrDeliveryTimeout))
			p.complete(b, ErrDeliveryTimeout)
			continue
		}
		node, addr, ok := p.meta.leader(b.tp)
		if !ok {
			p.metaStale.Store(true)
			keep = append(keep, b)
			continue
		}
		pl := plans[node]
		if pl == nil {
			pl = &plan{w: p.workerFor(node), addr: addr}
			plans[node] = pl
			order = append(order, node)
		}
		placed := false
		for _, j := range pl.jobs {
			if j.fits(b, p.cfg.MaxRequestBytes) {
				j.batches = append(j.batches, b)
				placed = true
				break
			}
		}
		if !placed {
			pl.jobs = append(pl.jobs, &job{addr: addr, batches: []*batch{b}})
		}
	}
	for _, node := range order {
		pl := plans[node]
		keep = append(keep, offerJobs(pl.w, pl.jobs)...)
	}
	if len(keep) > 0 {
		p.acc.requeue(keep)
		retry := now.Add(p.cfg.RetryBackoff)
		if next.IsZero() || retry.Before(next) {
			next = retry
		}
	}
	return next
}

// offerJobs queues jobs without blocking and returns the batches of every job
// that did not fit the worker queue, in order.
func offerJobs(w *worker, jobs []*job) []*batch {
	var rest []*batch
	for i, j := range jobs {
		select {
		case w.jobs <- j:
		default:
			for _, r := range jobs[i:] {
				rest = append(rest, r.batches...)
			}
			return rest
		}
	}
	return nil
}

func (j *job) fits(b *batch, maxBytes int) bool {
	size := 0
	for _, x := range j.batches {
		if x.tp == b.tp {
			return false
		}
		size += x.encodedSize()
	}
	return size+b.encodedSize() <= maxBytes
}

func (p *Producer) workerFor(node int32) *worker {
	p.workersMu.Lock()
	defer p.workersMu.Unlock()
	w := p.workers[node]
	if w == nil {
		w = &worker{node: node, jobs: make(chan *job, p.cfg.JobQueue)}
		p.workers[node] = w
		p.wg.Add(1)
		go p.runWorker(w)
	}
	return w
}

func (p *Producer) closeAllConns() {
	p.workersMu.Lock()
	defer p.workersMu.Unlock()
	for _, w := range p.workers {
		w.mu.Lock()
		w.conn.close()
		w.mu.Unlock()
	}
}

func (p *Producer) runWorker(w *worker) {
	defer p.wg.Done()
	for j := range w.jobs {
		p.send(w, j)
	}
	w.mu.Lock()
	w.conn.close()
	w.conn = nil
	w.mu.Unlock()
}

func (w *worker) setConn(c *brokerConn) {
	w.mu.Lock()
	w.conn = c
	w.mu.Unlock()
}

// send delivers one job over the worker's connection, reconnecting when the
// leader address changed or the previous attempt failed. Java retries=0 is
// kept: a failed job fails all of its batches and marks metadata stale.
func (p *Producer) send(w *worker, j *job) {
	w.mu.Lock()
	conn := w.conn
	w.mu.Unlock()
	if conn != nil && (conn.addr != j.addr || (p.cfg.IdleReconnect > 0 && !conn.lastUse.IsZero() && time.Since(conn.lastUse) > p.cfg.IdleReconnect)) {
		conn.close()
		conn = nil
		w.setConn(nil)
	}
	if conn == nil {
		var err error
		atomic.AddUint64(&p.reconnects, 1)
		conn, err = dialBroker(p.forceCtx, &p.cfg, j.addr)
		if err != nil {
			p.metaStale.Store(true)
			p.report("produce.connect", j.addr, err)
			p.failJob(j, err)
			return
		}
		w.setConn(conn)
	}
	results, err := conn.produce(j.batches)
	conn.lastUse = time.Now()
	if err != nil {
		conn.close()
		w.setConn(nil)
		p.metaStale.Store(true)
		p.report("produce.request", j.addr, err)
		p.failJob(j, err)
		return
	}
	if p.cfg.Acks == 0 {
		for _, b := range j.batches {
			p.complete(b, nil)
		}
		return
	}
	byTP := make(map[topicPartition]partitionResult, len(results))
	for _, r := range results {
		byTP[topicPartition{r.Topic, r.Index}] = r
	}
	for _, b := range j.batches {
		r, ok := byTP[b.tp]
		if !ok {
			err := fmt.Errorf("%w: no produce response for %s/%d", ErrMalformed, b.tp.topic, b.tp.partition)
			p.report("produce.response", j.addr, err)
			p.complete(b, err)
			continue
		}
		if r.ErrorCode != 0 {
			kerr := Error{Code: r.ErrorCode}
			if kerr.refreshesMetadata() {
				p.metaStale.Store(true)
			}
			if r.ErrorMessage != "" {
				p.report("produce.partition", j.addr, fmt.Errorf("%s/%d: %w: %s", b.tp.topic, b.tp.partition, kerr, r.ErrorMessage))
			} else {
				p.report("produce.partition", j.addr, fmt.Errorf("%s/%d: %w", b.tp.topic, b.tp.partition, kerr))
			}
			p.complete(b, kerr)
			continue
		}
		p.complete(b, nil)
	}
}

func (p *Producer) failJob(j *job, err error) {
	for _, b := range j.batches {
		p.complete(b, err)
	}
}

// accumulator owns open and sealed batches and the BufferBytes accounting.
type accumulator struct {
	mu        sync.Mutex
	open      map[topicPartition]*batch
	ready     []*batch
	topics    map[string]struct{}
	maxTopics int
	records   int64
	bytes     int64
	idle      chan struct{} // non-nil and open while records > 0
	closed    bool
	scratch   []byte
}

func (a *accumulator) init(maxTopics int) {
	a.open = make(map[topicPartition]*batch)
	a.topics = make(map[string]struct{})
	a.maxTopics = maxTopics
}

// add encodes rec into its open batch. wake is true when a batch was sealed
// or created so the coordinator can dispatch or arm the linger timer.
func (a *accumulator) add(rec Record, now time.Time, cfg *Config) (wake bool, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return false, ErrClosed
	}
	tp := topicPartition{rec.Topic, rec.Partition}
	if _, ok := a.topics[rec.Topic]; !ok {
		if len(a.topics) >= a.maxTopics {
			return false, ErrTooManyTopics
		}
	}
	b := a.open[tp]
	ts := now.UnixMilli()
	var delta int64
	var offset int32
	if b != nil {
		delta = ts - b.baseTimestamp
		offset = b.count
	}
	a.scratch = encodeRecord(a.scratch[:0], rec, delta, offset)
	size := len(a.scratch)
	if size+batchHeaderLen > cfg.MaxRecordBytes {
		return false, ErrRecordTooLarge
	}
	// A record never fits an existing batch beyond BatchBytes; it starts a
	// new one (Java: oversized records get their own batch).
	if b != nil && len(b.records)+size+batchHeaderLen > cfg.BatchBytes {
		a.ready = append(a.ready, b)
		delete(a.open, tp)
		b = nil
		wake = true
		// delta/offset were computed against the sealed batch: re-encode
		a.scratch = encodeRecord(a.scratch[:0], rec, 0, 0)
		size = len(a.scratch)
	}
	cost := int64(size)
	if b == nil {
		cost += batchHeaderLen
	}
	if a.bytes+cost > cfg.BufferBytes {
		// a batch sealed above stays sealed; only this record is refused
		return wake, ErrBufferFull
	}
	if b == nil {
		b = newBatch(tp, now)
		a.open[tp] = b
		wake = true
	}
	b.appendRecord(a.scratch, ts)
	a.topics[rec.Topic] = struct{}{}
	a.bytes += cost
	if a.records == 0 {
		a.idle = make(chan struct{})
	}
	a.records++
	if b.encodedSize() >= cfg.BatchBytes {
		a.ready = append(a.ready, b)
		delete(a.open, tp)
		wake = true
	}
	return wake, nil
}

// collect seals open batches that reached linger (or all when flush) and
// returns every sealed batch in FIFO order plus the earliest linger deadline
// among batches left open.
func (a *accumulator) collect(now time.Time, linger time.Duration, flush bool) ([]*batch, time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var next time.Time
	for tp, b := range a.open {
		due := b.created.Add(linger)
		if flush || !now.Before(due) {
			a.ready = append(a.ready, b)
			delete(a.open, tp)
			continue
		}
		if next.IsZero() || due.Before(next) {
			next = due
		}
	}
	out := a.ready
	a.ready = nil
	return out, next
}

// requeue puts undispatched batches back ahead of anything sealed meanwhile.
func (a *accumulator) requeue(batches []*batch) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ready = append(batches, a.ready...)
}

func (a *accumulator) release(b *batch) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.bytes -= int64(b.encodedSize())
	a.records -= int64(b.count)
	if a.records == 0 && a.idle != nil {
		close(a.idle)
		a.idle = nil
	}
}

// abandon refuses further records and drops everything still queued.
func (a *accumulator) abandon() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed = true
	var n int64
	for tp, b := range a.open {
		n += int64(b.count)
		a.bytes -= int64(b.encodedSize())
		delete(a.open, tp)
	}
	for _, b := range a.ready {
		n += int64(b.count)
		a.bytes -= int64(b.encodedSize())
	}
	a.ready = nil
	a.records -= n
	if a.records == 0 && a.idle != nil {
		close(a.idle)
		a.idle = nil
	}
	return n
}

func (a *accumulator) queued() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.open) + len(a.ready)
}

func (a *accumulator) pending() (records, bytes int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.records, a.bytes
}

func (a *accumulator) pendingRecords() int64 {
	r, _ := a.pending()
	return r
}

func (a *accumulator) idleChan() chan struct{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.idle
}

func (a *accumulator) topicList() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, 0, len(a.topics))
	for t := range a.topics {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
