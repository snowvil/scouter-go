package ingest

import (
	"context"
	"errors"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/syncx"
)

type Config struct {
	MasterCounters map[string]string
	CounterTypes   map[string]service.CounterTypeMetadata
	Location       *time.Location
	Clock          func() time.Time
	XLogCapacity   int
	// MaxObjects bounds the live object map (Java objMap has no bound) and
	// the per-object tables sized by it (counter names, alert summaries, kube
	// managers, CPU streaks). 0 selects DefaultMaxObjects. At the bound a new
	// object takes the place of the longest-inactive one (see
	// pickEvictionLocked); startup loads at most this many objects.
	MaxObjects int
	// MaxCounters bounds the realtime counter cache and the pending 5m
	// samples (each separately), and the status/interaction and kube seq
	// tables. 0 selects DefaultMaxCounters.
	MaxCounters int
	// MaxCacheBytes is the byte budget of the in-memory caches (objects,
	// counter cache and samples, XLog/alert rings, group meters, sampling
	// buffers, status/interaction). 0 selects DefaultMaxCacheBytes. It counts
	// encoded sizes plus small fixed overheads, not Go heap: see
	// DefaultMaxCacheBytes and CONTRACT.md §12. The objects and the kube
	// pod seq state are bounded by their own bytes only (coreDelta).
	MaxCacheBytes  int
	ObjectDeadTime time.Duration
	AgentCall      func(context.Context, int32, string, *protocol.MapPack) ([]protocol.Pack, error)
	AgentBlobCall  func(context.Context, int32, string, *protocol.MapPack, func([]byte) error) error
	// OnXLog/OnProfile are the PLUGIN_SERVER_XLOG/PLUGIN_SERVER_PROFILE
	// callbacks. They run under the engine lock immediately before the store
	// write (Java XLogCore: PlugInManager.xlog before XLogWR.add; ProfileCore:
	// PlugInManager.profile before XLogProfileWR.add for non-empty profiles) and
	// regardless of whether that write then succeeds. They must not block and
	// must not retain the engine-owned pack beyond the call.
	OnXLog    func(*protocol.XLogPack)
	OnProfile func(*protocol.ProfilePack)
	// ObjectType is CounterEngine.getObjectType(objType) (family name and
	// sub-object flag) for ObjectCpuChecker and the AgentManager alerts; ok
	// is false for an unknown type. Nil: every type is unknown.
	ObjectType func(objType string) (ObjectTypeInfo, bool)
	// AddObjectTypeIfNotExist is CounterManager.addObjectTypeIfNotExist for
	// an unknown objType whose ObjectPack carries a "detected" text tag. It is
	// called outside the engine lock and may write counters.site.xml.
	AddObjectTypeIfNotExist func(objType, detected string)
	// SamplingGxidKeepCount is xlog_sampling_matcher_gxid_keep_memory_count,
	// read once like the XLogDelayingCache constructor; <= 0 uses 500000.
	SamplingGxidKeepCount int
	// Settings are the initial live settings (DefaultSettings with
	// ObjectDeadTime when nil); SetSettings replaces them.
	// GeoIP is the GeoIP city database (GISDataUtil); nil is "not loaded" and
	// makes the XLog GeoIP step a no-op, like GISDataUtil.isOk() == false.
	// Replace it at runtime with SetGeoIP.
	GeoIP    GeoIPLookup
	Settings *Settings
	// OnSQLText is SqlTables.add for a TextPack of xtype "sql", called under
	// the engine lock before the text is stored (TextCore order: SqlTables.add
	// precedes TextWR.add). It must not block (sqltable.Tables.Add queues).
	OnSQLText func(date string, hash int32, text string)
	// PackLog is the NetDataProcessor debug output: called once with the
	// detached pack before processing (processed=false, log_udp_packet) and
	// once after processing whatever the result (processed=true, the
	// per-type log_udp_* line), as Java prints after Core.add. The second
	// call runs under the engine lock with the engine-owned pack: it must not
	// block, keep the pack or call the engine.
	PackLog func(p protocol.Pack, processed bool)
	// ObjectTypeLog is CounterManager.addObjectTypeIfNotExist's
	// log_udp_some_object point for every ObjectPack: known reports a known
	// objType (Java stops there), detected/detectedOK the "detected" text tag
	// (Java getText: false prints null). Called outside the engine lock.
	ObjectTypeLog func(objType string, known bool, detected string, detectedOK bool)
	// ProfileTestLog is the ProfilePreCore log_test_rate point. It is called
	// under the engine lock for every profile at a Java logging point with
	// the case, and for released delayed children the count of lines
	// already written and the number of children; it returns whether it
	// wrote a line (the processDelaying count).
	ProfileTestLog func(c ProfileTestCase, cnt, total int, p *protocol.ProfilePack) bool
}

// ProfileTestCase selects the ProfilePreCore log_test_rate tag.
type ProfileTestCase uint8

const (
	ProfileTestIgnoreCons ProfileTestCase = iota + 1 // [ProfileCoreCase1(IgnoreCons)]
	ProfileTestNormal                                // [ProfileCoreCase2]
	ProfileTestOld                                   // [ProfileCoreCase3(Old)]
	ProfileTestDelaying                              // [processDelaying][cnt/total]
)

// Defaults of the engine bounds (Config zero values).
const (
	// DefaultMaxObjects covers several thousand Agents (one object each,
	// plus host/sub objects) with room for restarts within a day.
	DefaultMaxObjects = 16384
	// DefaultMaxCounters is DefaultMaxObjects x 16 realtime counter keys
	// (Agents send a few dozen keys, host objects fewer).
	DefaultMaxCounters = 262144
	// DefaultMaxCacheBytes is the accounted (encoded-size) cache budget. At
	// the default bounds the accounted demand is about 2 MiB for 16384
	// objects, about 12 MiB for 262144 counter keys plus samples, up to 20 MiB
	// for the 500000 sampling gxids (40 B each), a few MiB for the XLog ring
	// (20480 slots) and alert ring, and the sampling delay buckets take the
	// rest. Exceeding it skips realtime cache updates (counted), recovers
	// delayed sampling children early, and never refuses a pack's storage
	// (XLog, profile, counter, alert, status). Accounted bytes are not heap:
	// measured heap is below 1x (gxids, 0.8x) to 17x (objects) the accounted
	// size (CONTRACT.md §12).
	DefaultMaxCacheBytes = 64 << 20
)

type Stats struct {
	Completed, Rejected, StorageErrors uint64
	XLogs                              uint64
	DroppedXLogs, SkippedStacks        uint64
	// KubeHeld counts ObjectPacks whose registration PodSeqManager stopped
	// (allowProceed=false); KubeLimited the packs rejected because the pod
	// sequence state reached its bound; KubeInitialEvicted the initial host
	// records dropped at the bound.
	KubeHeld, KubeLimited, KubeInitialEvicted uint64
	// CPUStreakLimited counts ObjectCpuChecker streaks not remembered at the bound.
	CPUStreakLimited uint64
	// GeoIPErrors counts XLogs whose GeoIP enrichment failed (XLog kept, not
	// enriched): failed lookups plus GeoIPTextErrors, the failed city text
	// writes among them.
	GeoIPErrors, GeoIPTextErrors uint64
	// ObjectAlertErrors counts INACTIVE_OBJECT/ACTIVATED_OBJECT alerts that
	// could not be stored (the object heartbeat itself is not failed).
	ObjectAlertErrors uint64
	// Sample5MDropped counts 5m samples and AlertSummaryDropped alert summary
	// packs of a periodic pass that were taken out of the engine state but
	// not written because an earlier write of that pass failed (Java swaps
	// the map before writing, so the pass is not retried).
	Sample5MDropped, AlertSummaryDropped uint64
	// KubeSeq* is the OBJECT_SET_KUBE_SEQ queue, CPUDump* the
	// TRIGGER_THREAD_DUMPS_FROM_CONDITIONS queue (see RunAgentCalls). The
	// fields stay flat so the "ingest=%+v" log line keeps scalar fields.
	KubeSeqEnqueued, KubeSeqDone, KubeSeqFailed, KubeSeqDropped, KubeSeqAbandoned uint64
	KubeSeqPending                                                                int
	CPUDumpEnqueued, CPUDumpDone, CPUDumpFailed, CPUDumpDropped, CPUDumpAbandoned uint64
	CPUDumpPending                                                                int
	// ObjectsEvicted counts inactive objects dropped from the live map to
	// register a new object at MaxObjects; ObjectsLimited the new-object
	// heartbeats rejected at MaxObjects with no inactive object to evict
	// (also counted in Rejected); ObjectsNotLoaded the objects of today's
	// file not loaded at startup because of MaxObjects/MaxCacheBytes.
	ObjectsEvicted, ObjectsLimited, ObjectsNotLoaded uint64
	// ObjectsEvictedSilent counts objects that do not die by the Java rule
	// within an hour (batch, or a deadTime tag past exemptDeadTimeMs) dropped
	// from the live map at MaxObjects after exemptEvictAt without a
	// heartbeat, because no dead object was left to evict (R7,
	// pickEvictionLocked).
	ObjectsEvictedSilent uint64
	// XLogCacheSkipped counts realtime cache updates of the XLog path skipped
	// at a cache bound while the XLog itself was stored: the realtime ring
	// slot, a service group meter or service-group memo, a processed gxid.
	// CounterCacheSkipped counts realtime counter packs stored but not put
	// in the counter cache/5m samples. One XLog can count more than once.
	XLogCacheSkipped, CounterCacheSkipped uint64
	// DelayedRecoveredEarly counts delayed child XLogs and profiles (XLog
	// consequent sampling) handed to the storage writer queues by the
	// recover rule (XLog unless discardAll, profile when discardNone; a
	// later queue drop is counted in the queue's Dropped, not taken back
	// here) before their bucket rolled
	// because the cache budget had no room to keep them waiting for their
	// driving XLog: the oldest delayed groups recovered to make room (for a
	// delayed pack, or profiles for the alert ring), or the arriving pack
	// itself when nothing was left to recover. DelayedDiscardedEarly counts
	// the ones the rule dropped. DelayedAbandoned counts delayed packs taken
	// out of a bucket (early or at a roll) but lost because the recovery
	// stopped on an error (canceled ctx, decode, store) before them.
	DelayedRecoveredEarly, DelayedDiscardedEarly, DelayedAbandoned uint64
	// AlertCacheSkipped counts AlertPack updates of the alert summary or the
	// realtime alert ring skipped at a bound (MaxObjects, MaxCounters, cache
	// budget) while the alert itself was stored; StatusCacheSkipped the
	// StatusPacks stored but not put in the status cache.
	AlertCacheSkipped, StatusCacheSkipped uint64
	// VisitorsSkipped counts XLogs with a user id whose object is not live,
	// so no visitor is counted (VisitorCore drops them silently).
	VisitorsSkipped uint64
	// Write* sum the asynchronous record writers of every kind (WriterStats
	// has them per kind): records enqueued, written, failed to write,
	// refused by the storage format (WriteInvalid: Agent input such as an
	// unsupported daily value type or a name over a file limit), dropped at a
	// full queue, left unwritten by Close (Abandoned) and still queued
	// (Pending). Enqueued = Written + WriteErrors + WriteInvalid +
	// WriteAbandoned + WritePending.
	WriteEnqueued, WriteWritten, WriteErrors, WriteInvalid, WriteDropped, WriteAbandoned uint64
	WritePending                                                                         int
	// StoreWritesDeferred counts storage queue writes of the collection path
	// (text, XLog, profile, tag count) not queued under the engine lock
	// because their queue was full (or a write of the same queue in that
	// hold was): they were submitted after it, each with its queue's bounded
	// wait, queued or dropped there (the storage queues count the drops).
	// StoreWritesHeldInline counts the ones submitted under the engine lock
	// instead because the hold reached heldMaxWrites/heldMaxBytes (the
	// previous in-lock wait, CONTRACT.md §11); StoreWritesHeldDropped the
	// ones dropped because the hold panicked.
	StoreWritesDeferred, StoreWritesHeldInline, StoreWritesHeldDropped uint64
	// ServiceTextsAhead counts service texts the group step needed from the
	// files that were read before the XLog's hold, without the engine lock;
	// ServiceTextSkipped the XLogs accepted without a group because the
	// store did not answer within serviceTextWait (a slow Store gate), or
	// because the text of an XLog delayed by sampling was evicted from the
	// storage caches before its driving XLog released it. The text is read
	// ahead, so later XLogs of the service get their group.
	ServiceTextsAhead, ServiceTextSkipped uint64
	// TextDivsLimited counts TextPacks refused (also in Rejected) because
	// their div is none of the original's and has no permanent index while
	// storage.DefaultMaxUnknownTextDivs unknown divs have one (R7,
	// storage.ErrTextDivLimit).
	TextDivsLimited uint64
}

// KubeSeqCalls groups the OBJECT_SET_KUBE_SEQ queue counters.
func (s Stats) KubeSeqCalls() AgentCallStats {
	return AgentCallStats{s.KubeSeqEnqueued, s.KubeSeqDone, s.KubeSeqFailed, s.KubeSeqDropped, s.KubeSeqAbandoned, s.KubeSeqPending}
}

// CPUDumpCalls groups the TRIGGER_THREAD_DUMPS_FROM_CONDITIONS queue counters.
func (s Stats) CPUDumpCalls() AgentCallStats {
	return AgentCallStats{s.CPUDumpEnqueued, s.CPUDumpDone, s.CPUDumpFailed, s.CPUDumpDropped, s.CPUDumpAbandoned, s.CPUDumpPending}
}

type counterKey struct {
	hash int32
	name string
	kind byte
}

// cachedCounter is a realtime counter value in the cache: the encoded Value
// (exact size, read-only, shared with the 5m sample when there is one;
// readers decode a copy) and when it expires (CounterCache, 10s). Its
// accounted bytes are len(data)+len(name), a sample's 16 more.
type cachedCounter struct {
	data   []byte
	expiry int64
}

// counterUpdate is what cacheCounterLocked found and encoded for one entry:
// the encoded value and the lengths of the cached current and sample values
// (-1 when absent).
type counterUpdate struct {
	data        []byte
	cur, sample int
}
type Engine struct {
	mu                                        syncx.Mutex
	config                                    Config
	store                                     *storage.Store
	counters                                  *storage.Counters
	objects                                   map[int32]*protocol.ObjectPack
	current                                   map[counterKey]cachedCounter
	samples                                   map[counterKey][]byte // encoded Value as cachedCounter.data
	counterNames                              map[int32]string
	objectSizes                               map[int32]int
	counterOrder                              []counterKey
	cacheBytes                                int
	coreBytes                                 int // objects and kube state, part of cacheBytes (coreDelta)
	ring                                      [][]byte
	ringBytes                                 int
	index                                     int32
	loop                                      int64
	closed                                    bool
	extra                                     *packetState
	completed, rejected, storageErrors, xlogs atomic.Uint64
	droppedXlogs, skippedStacks               atomic.Uint64
	// objectNames is ObjectMap.objectNameTable (AgentManager.getAgent(objName)).
	objectNames                               map[string]int32
	kube                                      *kubeState
	cpuFirst                                  map[string]int64
	geo                                       *geoState
	kubeCalls, cpuCalls                       *agentCallQueue
	done                                      chan struct{}
	closeOnce                                 sync.Once
	kubeHeld, kubeLimited, kubeInitialEvicted atomic.Uint64
	cpuLimited, geoErrors, geoTextErrors      atomic.Uint64
	objectAlertErrors                         atomic.Uint64
	objState                                  map[int32]*objectState
	objOrder                                  *intKeyOrder
	live                                      atomic.Pointer[Settings]
	gxidKeepCount                             int
	// counterEnc and counterUpdates are cacheCounterLocked's scratch, used
	// under the engine lock only (counterUpdates is cleared after each pack).
	// No slice of counterEnc's buffer leaves cacheCounterLocked: the cache
	// keeps an exact-size copy. Counter and Objects decode it under e.mu;
	// the Sample5M writer pass is the one holder outside the lock. A value
	// past counterEncKeep drops the grown encoder.
	counterEnc     *protocol.Encoder
	counterUpdates []counterUpdate
	// objectNameCache mirrors objHash -> objName of e.objects for readers
	// that must not wait for the engine lock (ObjectNameCached).
	objectNameCache sync.Map
	// sample5MDropped/alertSummaryDropped count the periodic records taken
	// out of engine state but not written because an earlier write of the
	// same pass failed (Stats.Sample5MDropped/AlertSummaryDropped).
	sample5MDropped, alertSummaryDropped atomic.Uint64
	// Object bound counters (Stats.ObjectsEvicted/ObjectsLimited/
	// ObjectsNotLoaded) and skipped cache updates (Stats.*CacheSkipped).
	objectsEvicted, objectsLimited, objectsNotLoaded atomic.Uint64
	objectsEvictedSilent                             atomic.Uint64
	xlogCacheSkipped, counterCacheSkipped            atomic.Uint64
	delayedRecoveredEarly, visitorsSkipped           atomic.Uint64
	alertCacheSkipped, statusCacheSkipped            atomic.Uint64
	delayedDiscardedEarly, delayedAbandoned          atomic.Uint64
	visits                                           []visit // pending visitor offers, see visitBatch
	// w owns the record writer goroutines (writer.go); Close ends them.
	w *writers

	// held is the storage writes the engine lock holder deferred: it
	// submits them after releasing the lock (storewrite.go). heldQueues has
	// a bit per storage.WriteQueue with a deferred write, heldBytes their
	// storage.Write.Size; flights tracks batches being submitted;
	// groupHint is the service text answer of the XLog being accepted.
	held                                                  []deferredWrite
	heldQueues                                            uint8
	heldBytes                                             int
	heldErr                                               error           // of held writes submitted under the lock
	holdCtx                                               context.Context // the holder's ctx, for those submits
	flights                                               heldFlights
	groupHint                                             serviceTextHint
	writesDeferred, serviceTextsAhead, serviceTextSkipped atomic.Uint64
	writesHeldInline, writesHeldDropped                   atomic.Uint64

	// evictHint is a lower bound of the earliest dead time of the live
	// objects, valid while evictHintSettings is the settings in use; it lets
	// a new object at MaxObjects skip the scan while no object can be dead.
	evictHint         int64
	evictHintSettings *Settings
	// nextLimitLog rate-limits the MaxObjects rejection log line,
	// silentEvictLog the silent exempt object eviction one, textDivLog the
	// unknown text div one.
	nextLimitLog   int64
	silentEvictLog rateLog
	textDivLog     rateLog
	// Test hooks, set only before the engine is used: realtimeDecodeHook runs
	// in XLogsRealtime after the engine lock is released and before decoding;
	// periodicWriteHook runs in Tick/Sample5M after the engine lock is
	// released and before the periodic writes; serviceTextReadHook runs in
	// lockForAccept after the service text read ahead and before the engine
	// lock is taken again.
	realtimeDecodeHook, periodicWriteHook, serviceTextReadHook func()
}

// ObjectNameCached is TextCache.get("object", objHash) for log lines: the
// name of a live object without taking the engine lock (it never waits for
// ingest or storage). ok is false for an unknown object.
func (e *Engine) ObjectNameCached(hash int32) (string, bool) {
	v, ok := e.objectNameCache.Load(hash)
	if !ok {
		return "", false
	}
	return v.(string), true
}

func New(cfg Config, s *storage.Store, c *storage.Counters) (*Engine, error) {
	if cfg.Location == nil || s == nil || c == nil {
		return nil, protocol.ErrInvalid
	}
	if cfg.MasterCounters != nil {
		copyMasters := make(map[string]string, len(cfg.MasterCounters))
		for k, v := range cfg.MasterCounters {
			copyMasters[k] = v
		}
		cfg.MasterCounters = copyMasters
	}
	if cfg.CounterTypes != nil {
		types := make(map[string]service.CounterTypeMetadata, len(cfg.CounterTypes))
		for k, v := range cfg.CounterTypes {
			v.Counters = append([]string(nil), v.Counters...)
			types[k] = v
		}
		cfg.CounterTypes = types
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.XLogCapacity == 0 {
		cfg.XLogCapacity = 20480
	}
	if cfg.MaxObjects == 0 {
		cfg.MaxObjects = DefaultMaxObjects
	}
	if cfg.MaxCounters == 0 {
		cfg.MaxCounters = DefaultMaxCounters
	}
	if cfg.MaxCacheBytes == 0 {
		cfg.MaxCacheBytes = DefaultMaxCacheBytes
	}
	if cfg.ObjectDeadTime == 0 {
		cfg.ObjectDeadTime = 8 * time.Second
	}
	if cfg.XLogCapacity < 1 || cfg.MaxObjects < 1 || cfg.MaxCounters < 1 || cfg.MaxCacheBytes < 1 || cfg.ObjectDeadTime < 0 {
		return nil, protocol.ErrInvalid
	}
	e := &Engine{config: cfg, store: s, counters: c, objects: map[int32]*protocol.ObjectPack{}, current: map[counterKey]cachedCounter{}, samples: map[counterKey][]byte{}, counterNames: map[int32]string{}, objectSizes: map[int32]int{}, counterEnc: protocol.NewEncoder(protocol.Limits{}), ring: make([][]byte, cfg.XLogCapacity), extra: newPacketState(cfg.Clock().UnixMilli()),
		objectNames: map[string]int32{}, kube: newKubeState(), cpuFirst: map[string]int64{}, geo: newGeoState(nil),
		kubeCalls: newAgentCallQueue("kube-seq", kubeSeqQueueSize), cpuCalls: newAgentCallQueue("cpu-dump", cpuDumpQueueSize), done: make(chan struct{}),
		objState: map[int32]*objectState{}, objOrder: newIntKeyOrder(), gxidKeepCount: cfg.SamplingGxidKeepCount}
	if e.gxidKeepCount <= 0 {
		e.gxidKeepCount = defaultGxidKeepCount
	}
	e.geo.putText = e.putGeoText
	initial := DefaultSettings()
	initial.ObjectDeadTimeMs = cfg.ObjectDeadTime.Milliseconds()
	if cfg.Settings != nil {
		initial = *cfg.Settings
	}
	e.SetSettings(initial)
	e.extra.sampling = newSamplingState(cfg.Clock().UnixMilli(), e.settings(), e.gxidKeepCount)
	if err := e.loadObjects(); err != nil {
		return nil, err
	}
	e.w = newWriters()
	e.w.start()
	return e, nil
}

// loadObjects puts today's stored objects in the live map (OBJECT_LIST).
// Java keeps no bound; Go loads at most MaxObjects within MaxCacheBytes and
// keeps the most recently active ones (latest wakeup first, then objHash):
// the left-out ones are the oldest, the ones the MaxObjects eviction would
// drop first. A left-out object is only missing from OBJECT_LIST until its
// next heartbeat registers it (its stored record is untouched). The count
// is logged once and reported in Stats.ObjectsNotLoaded.
func (e *Engine) loadObjects() error {
	objects, err := e.store.Objects(storage.JavaDate(e.config.Clock().UnixMilli(), e.config.Location))
	if err != nil {
		return err
	}
	sort.SliceStable(objects, func(i, j int) bool {
		if objects[i].Wakeup != objects[j].Wakeup {
			return objects[i].Wakeup > objects[j].Wakeup
		}
		return objects[i].ObjHash < objects[j].ObjHash
	})
	notLoaded := 0
	for _, p := range objects {
		if _, dup := e.objects[p.ObjHash]; dup {
			continue // the first (latest) record of an objHash wins
		}
		b, err := protocol.EncodePack(p, protocol.Limits{})
		if err != nil {
			return err
		}
		if len(e.objects) >= e.config.MaxObjects || e.coreDelta(len(b)) != nil {
			notLoaded++
			continue
		}
		e.objects[p.ObjHash] = p
		e.objectNames[p.ObjName] = p.ObjHash
		e.objectNameCache.Store(p.ObjHash, p.ObjName)
		e.objectSizes[p.ObjHash] = len(b)
		e.addCoreBytes(len(b))
	}
	if notLoaded > 0 {
		e.objectsNotLoaded.Add(uint64(notLoaded))
		log.Printf("ingest: %d of today's %d objects not loaded at startup (bounds: %d objects, %d cache bytes); the most recently active are loaded, the others register on their next heartbeat", notLoaded, len(objects), e.config.MaxObjects, e.config.MaxCacheBytes)
	}
	return nil
}

// clonePack detaches a pack with the limits it was received under: a pack the
// UDP receiver accepted is not refused here.
func clonePack(p protocol.Pack) (protocol.Pack, error) {
	b, err := protocol.EncodePack(p, protocol.AgentPackLimits)
	if err != nil {
		return nil, err
	}
	return protocol.DecodePack(b, protocol.AgentPackLimits)
}
func (e *Engine) Accept(ctx context.Context, p protocol.Pack) (err error) {
	return e.AcceptFrom(ctx, p, "")
}

// AcceptFrom preserves the UDP peer fallback used by AgentManager. The caller's
// Pack is detached (cloned, before the engine lock) before receiver fields or
// processing state are changed.
func (e *Engine) AcceptFrom(ctx context.Context, p protocol.Pack, peerHost string) (err error) {
	if err = ctx.Err(); err != nil {
		return err
	}
	// CounterManager.addObjectTypeIfNotExist reads the caller's pack only.
	e.addObjectTypeIfNotExist(p)
	owned, err := clonePack(p)
	if err != nil {
		e.rejected.Add(1)
		return err
	}
	return e.acceptOwned(ctx, owned, peerHost)
}

// AcceptOwnedFrom is AcceptFrom for a pack the caller hands over: the UDP
// receiver's freshly decoded pack, which it reads no more (but for its type).
// The clone is skipped; a decoded pack survives encode and decode unchanged
// (protocol FuzzDecodePack), so the result is the same.
func (e *Engine) AcceptOwnedFrom(ctx context.Context, p protocol.Pack, peerHost string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.addObjectTypeIfNotExist(p)
	return e.acceptOwned(ctx, p, peerHost)
}

func (e *Engine) acceptOwned(ctx context.Context, p protocol.Pack, peerHost string) (err error) {
	if err = e.lockForAccept(ctx, p); err != nil {
		e.rejected.Add(1)
		return err
	}
	locked := true
	defer func() {
		if locked { // a panic: the held writes are not submitted
			e.discardHeldLocked()
			e.groupHint = serviceTextHint{}
			e.mu.Unlock()
		}
	}()
	if e.closed {
		locked = false
		e.mu.Unlock()
		return os.ErrClosed
	}
	e.holdCtx = ctx
	if e.config.PackLog != nil {
		e.config.PackLog(p, false)
	}
	now := e.config.Clock().UnixMilli()
	if object, ok := p.(*protocol.ObjectPack); ok && object.Address == "" {
		object.Address = peerHost
	}
	err = e.acceptLocked(ctx, p, now)
	if e.config.PackLog != nil {
		// NetDataProcessor prints the per-type line after Core.add, with
		// the fields the dispatch filled (counter objHash/time, address).
		e.config.PackLog(p, true)
	}
	// The writes whose queue was full wait for room after the lock is
	// released; a submit error (closed store, ctx) is the pack's result as
	// the queuing call's error was. Completed counts after them.
	e.groupHint = serviceTextHint{}
	locked = false
	if heldErr := e.unlockHeld(ctx); err == nil {
		err = heldErr
	}
	if err != nil {
		e.rejected.Add(1)
		if !errors.Is(err, protocol.ErrUnsupported) && !errors.Is(err, protocol.ErrLimit) && !errors.Is(err, protocol.ErrInvalid) {
			e.storageErrors.Add(1)
		}
	} else {
		e.completed.Add(1)
	}
	return err
}

// acceptLocked owns packet processing; implicit Batch heartbeats do not count
// as separate transport packets. All callers hold e.mu.
func (e *Engine) acceptLocked(ctx context.Context, p protocol.Pack, now int64) (err error) {
	date := storage.JavaDate(now, e.config.Location)
	switch v := p.(type) {
	case *protocol.ObjectPack:
		return e.acceptObjectLocked(ctx, v, now, date)
	case *protocol.TextPack:
		if v.XType == "sql" && e.config.OnSQLText != nil {
			e.config.OnSQLText(date, v.Hash, v.Text)
		}
		err = e.putTextLocked(date, v, nil)
		if errors.Is(err, storage.ErrTextDivLimit) && e.textDivLog.allow() {
			log.Printf("ingest: text of div %q not stored: not a Scouter div and the unknown-div limit of permanent text indexes is reached (%d texts refused so far)", v.XType, e.store.TextDivsLimited())
		}
	case *protocol.PerfCounterPack:
		if v.Time == 0 {
			v.Time = now
		}
		if v.TimeType == 0 {
			v.TimeType = 1
		}
		if v.Time <= 0 {
			return protocol.ErrInvalid
		}
		if v.Data.Type != protocol.ValueMap {
			return protocol.ErrInvalid // Counters.PutContext refuses it; rejected before any state
		}
		if v.TimeType > 6 {
			// The daily counter file has no layout for it (counterShape):
			// refused before any state, as the synchronous write did.
			return protocol.ErrUnsupported
		}
		hash := storage.CounterHash(v.ObjName)
		setCounterMetadata(&v.Data, "objHash", int64(hash))
		setCounterMetadata(&v.Data, "time", v.Time)
		// PerfCountCore: RealtimeCounterWR.add (or DailyCounterWR.add) first,
		// then Auto5MSampling/CounterCache and ObjectCpuChecker. The write is
		// queued (the pack is handed over: nothing changes it afterwards); the
		// caches are updated whatever becomes of it.
		kind := writeDaily
		if v.TimeType == 1 {
			kind = writeCounter
		}
		e.w.enqueue(kind, 1, packHeapBytes(v), one(func(ctx context.Context) error { return e.counters.PutContext(ctx, v) }))
		if v.TimeType == 1 {
			if err = e.cacheCounterLocked(v, hash, now); err != nil {
				return err
			}
			// PerfCountCore: ObjectCpuChecker.checkCpu after the realtime
			// write and caches; it reads the pack, not the cache.
			e.checkCPULocked(v)
		}
	case *protocol.XLogPack:
		if v.EndTime == 0 {
			v.EndTime = now
		}
		return e.acceptXLog(ctx, v, now)
	case *protocol.ProfilePack:
		v.Time = now
		return e.acceptProfile(ctx, v, now)
	default:
		return e.acceptExtra(ctx, p, now)
	}
	return err
}

// counterEncKeep is the largest encoded counter value whose grown buffer
// cacheCounterLocked keeps for the next pack.
const counterEncKeep = 64 << 10

// cacheCounterLocked is the Auto5MSampling.add/CounterCache.put part of
// PerfCountCore for a queued realtime pack. The pack is cached as a whole or
// not at all: past MaxCounters (new current keys or new samples), a new
// object name past MaxObjects, or the byte budget, nothing is cached and
// Stats.CounterCacheSkipped counts it. Existing keys keep their previous
// value until they expire (10s) and the 5m sample of the skipped keys is the
// previous one, or none (CONTRACT.md §12).
//
// It runs at counter pack rate under the engine lock, so each entry costs one
// lookup per map (the found lengths give the bounds, the byte delta and the
// order), one encode into the engine's encoder and one exact-size copy.
func (e *Engine) cacheCounterLocked(v *protocol.PerfCounterPack, hash int32, now int64) error {
	updates := e.counterUpdates[:0]
	defer func() { clear(updates); e.counterUpdates = updates[:0] }()
	newKeys, newSamples := 0, 0
	for _, x := range v.Data.Entries {
		key := counterKey{hash, x.Key, 1}
		u := counterUpdate{cur: -1, sample: -1}
		if c, ok := e.current[key]; ok {
			u.cur = len(c.data)
		} else {
			newKeys++
		}
		if numericSample(x.Value) {
			if b, ok := e.samples[key]; ok {
				u.sample = len(b)
			} else {
				newSamples++
			}
		}
		updates = append(updates, u)
	}
	limited := len(e.samples)+newSamples > e.config.MaxCounters || len(e.current)+newKeys > e.config.MaxCounters
	if len(v.Data.Entries) > 0 {
		if _, exists := e.counterNames[hash]; !exists && len(e.counterNames) >= e.config.MaxObjects {
			limited = true
		}
	}
	if limited {
		e.counterCacheSkipped.Add(1)
		return nil
	}
	delta := 0
	for i, x := range v.Data.Entries {
		e.counterEnc.Reset()
		e.counterEnc.Value(x.Value)
		b, err := e.counterEnc.Bytes()
		if err != nil {
			return err
		}
		// Exact-size copy: the encoder buffer is reused for the next entry
		// and must not reach the cache (TestCounterCacheValuesOwned).
		u := &updates[i]
		u.data = append(make([]byte, 0, len(b)), b...)
		if len(b) > counterEncKeep {
			e.counterEnc = protocol.NewEncoder(protocol.Limits{})
		}
		size := len(b) + len(x.Key)
		delta += size
		if u.cur >= 0 {
			delta -= u.cur + len(x.Key)
		}
		if numericSample(x.Value) {
			delta += size + 16
			if u.sample >= 0 {
				delta -= u.sample + len(x.Key) + 16
			}
		}
	}
	if len(v.Data.Entries) > 0 {
		delta += len(v.ObjName) - len(e.counterNames[hash])
	}
	if e.cacheDelta(delta) != nil {
		e.counterCacheSkipped.Add(1)
		return nil
	}
	if len(v.Data.Entries) > 0 {
		e.counterNames[hash] = v.ObjName
	}
	for i, x := range v.Data.Entries {
		key := counterKey{hash, x.Key, 1}
		u := updates[i]
		if u.cur < 0 {
			// A key repeated in the pack is put by its first entry. (Its
			// delta above then counts both entries against the old size, as
			// before; the wire decoder keeps one entry per key, so only a
			// pack built in-process can repeat one.)
			if _, exists := e.current[key]; !exists {
				e.counterOrder = append(e.counterOrder, key)
			}
		}
		e.current[key] = cachedCounter{data: u.data, expiry: now + 10000}
		if numericSample(x.Value) {
			e.samples[key] = u.data
		}
	}
	e.cacheBytes += delta
	return nil
}
func (e *Engine) Stats() Stats {
	st := Stats{Completed: e.completed.Load(), Rejected: e.rejected.Load(), StorageErrors: e.storageErrors.Load(), XLogs: e.xlogs.Load(), DroppedXLogs: e.droppedXlogs.Load(), SkippedStacks: e.skippedStacks.Load(),
		ObjectsEvicted: e.objectsEvicted.Load(), ObjectsLimited: e.objectsLimited.Load(), ObjectsNotLoaded: e.objectsNotLoaded.Load(), ObjectsEvictedSilent: e.objectsEvictedSilent.Load(), XLogCacheSkipped: e.xlogCacheSkipped.Load(), CounterCacheSkipped: e.counterCacheSkipped.Load(), DelayedRecoveredEarly: e.delayedRecoveredEarly.Load(), DelayedDiscardedEarly: e.delayedDiscardedEarly.Load(), DelayedAbandoned: e.delayedAbandoned.Load(), AlertCacheSkipped: e.alertCacheSkipped.Load(), StatusCacheSkipped: e.statusCacheSkipped.Load(),
		KubeHeld: e.kubeHeld.Load(), KubeLimited: e.kubeLimited.Load(), KubeInitialEvicted: e.kubeInitialEvicted.Load(), CPUStreakLimited: e.cpuLimited.Load(), GeoIPErrors: e.geoErrors.Load(), GeoIPTextErrors: e.geoTextErrors.Load(), ObjectAlertErrors: e.objectAlertErrors.Load(),
		Sample5MDropped: e.sample5MDropped.Load(), AlertSummaryDropped: e.alertSummaryDropped.Load(), VisitorsSkipped: e.visitorsSkipped.Load(),
		StoreWritesDeferred: e.writesDeferred.Load(), StoreWritesHeldInline: e.writesHeldInline.Load(), StoreWritesHeldDropped: e.writesHeldDropped.Load(), ServiceTextsAhead: e.serviceTextsAhead.Load(), ServiceTextSkipped: e.serviceTextSkipped.Load(),
		TextDivsLimited: e.store.TextDivsLimited(),
	}
	for _, w := range e.w.stats() {
		st.WriteEnqueued += w.Enqueued
		st.WriteWritten += w.Written
		st.WriteErrors += w.WriteErrors
		st.WriteInvalid += w.WriteInvalid
		st.WriteDropped += w.Dropped
		st.WriteAbandoned += w.Abandoned
		st.WritePending += w.Pending
	}
	k, c := e.kubeCalls.stats(), e.cpuCalls.stats()
	st.KubeSeqEnqueued, st.KubeSeqDone, st.KubeSeqFailed, st.KubeSeqDropped, st.KubeSeqAbandoned, st.KubeSeqPending = k.Enqueued, k.Done, k.Failed, k.Dropped, k.Abandoned, k.Pending
	st.CPUDumpEnqueued, st.CPUDumpDone, st.CPUDumpFailed, st.CPUDumpDropped, st.CPUDumpAbandoned, st.CPUDumpPending = c.Enqueued, c.Done, c.Failed, c.Dropped, c.Abandoned, c.Pending
	return st
}

// Close stops ingest, ends RunAgentCalls and counts the Agent calls still
// queued as abandoned, then lets the record writers finish their queues for
// at most writerCloseDeadline and counts what is left (WriterStats.Abandoned).
// It returns once every writer goroutine has ended, or writerStuckGrace after
// the deadline when a writer is still in a write that ignores cancellation
// (a stalled disk): that writer is left running and its records stay
// WriterStats.Pending (logged per writer by writers.close). The storage may
// be closed after Close; the stuck write then fails or never returns.
func (e *Engine) Close() {
	e.mu.Lock()
	e.closed = true
	e.flushVisitsLocked()
	flights := e.flights.lastID()
	e.mu.Unlock()
	// Writes deferred by holds before Close are submitted (or fail with
	// the store) before Close returns and the store may be closed.
	fctx, cancel := context.WithTimeout(context.Background(), writerCloseDeadline)
	if err := e.flights.wait(fctx, flights); err != nil {
		log.Printf("ingest: deferred storage writes still being submitted after %v", writerCloseDeadline)
	}
	cancel()
	e.closeOnce.Do(func() { close(e.done) })
	e.kubeCalls.stop()
	e.cpuCalls.stop()
	start := time.Now()
	if e.w.close(writerCloseDeadline) {
		// Abandoned and Pending are disjoint (Enqueued = Written +
		// WriteErrors + WriteInvalid + Abandoned + Pending): the total of
		// each, once; the stuck writers' lines name their kinds.
		var abandoned uint64
		pending := 0
		for _, s := range e.w.stats() {
			abandoned += s.Abandoned
			pending += s.Pending
		}
		log.Printf("ingest: writers not drained within %v: %d queued records abandoned, %d left pending in stuck writers", writerCloseDeadline, abandoned, pending)
	} else if d := time.Since(start); d > time.Second {
		log.Printf("ingest: writers drained in %v", d.Round(time.Millisecond))
	}
}

// Flush waits until every record queued by the engine before the call is
// written (or failed, counted). It is the barrier before a storage sync,
// purge or delete, and in tests before a file read. It must not be called
// with the engine lock held. After Close it returns at once.
func (e *Engine) Flush(ctx context.Context) error {
	if err := e.mu.LockContext(ctx); err != nil {
		return err
	}
	e.flushVisitsLocked()
	flights := e.flights.lastID()
	e.mu.Unlock()
	// The storage writes deferred by earlier holds (texts, XLogs, profiles,
	// tag counts) are in their queues, or dropped, before a sync or purge.
	if err := e.flights.wait(ctx, flights); err != nil {
		return err
	}
	return e.w.flush(ctx)
}

// WriterStats reports each record writer queue (kinds in a fixed order).
func (e *Engine) WriterStats() []WriterStats { return e.w.stats() }

// ObjectCount is the number of live agent objects for the stats line.
func (e *Engine) ObjectCount() int { e.mu.Lock(); defer e.mu.Unlock(); return len(e.objects) }
func (e *Engine) Objects(ctx context.Context, date string) ([]*protocol.ObjectPack, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if date != "" {
		return e.store.ObjectsContext(ctx, date)
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer e.mu.Unlock()
	out := make([]*protocol.ObjectPack, 0, len(e.objects))
	now := e.config.Clock().UnixMilli()
	for _, p := range e.objects {
		obj, err := e.liveObjectLocked(p, now)
		if err != nil {
			return nil, err
		}
		out = append(out, obj)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ObjHash < out[j].ObjHash })
	return out, nil
}

// liveObjectLocked returns a detached copy of one live object with its Alive
// flag and master counter tag as a Client sees them. The engine lock is held.
func (e *Engine) liveObjectLocked(p *protocol.ObjectPack, now int64) (*protocol.ObjectPack, error) {
	v, err := clonePack(p)
	if err != nil {
		return nil, err
	}
	obj := v.(*protocol.ObjectPack)
	if obj.ObjType != "batch" {
		obj.Alive = !e.objectDead(obj, now)
	}
	if !obj.Alive {
		return obj, nil
	}
	master := e.config.MasterCounters[obj.ObjType]
	if master == "" {
		for _, tag := range obj.Tags.Entries {
			if tag.Key == "detected" && tag.Value.Type == protocol.ValueText {
				master = e.config.MasterCounters[tag.Value.Text]
			}
		}
	}
	key := counterKey{obj.ObjHash, master, 1}
	if e.current[key].expiry < now {
		e.expireCounter(key)
	}
	if value, exists := e.current[key]; exists && master != "" {
		detached, err := protocol.DecodeValue(value.data, protocol.Limits{})
		if err != nil {
			return nil, err
		}
		replaced := false
		for i := range obj.Tags.Entries {
			if obj.Tags.Entries[i].Key == "counter" {
				obj.Tags.Entries[i].Value = detached
				replaced = true
				break
			}
		}
		if !replaced {
			obj.Tags.Entries = append(obj.Tags.Entries, protocol.Entry{Key: "counter", Value: detached})
		}
	}
	return obj, nil
}

// Object returns one object. A live lookup copies only that object: Client
// commands call it once per object hash, and copying the whole live list each
// time held the engine lock long enough to stall ingest.
func (e *Engine) Object(ctx context.Context, date string, hash int32) (*protocol.ObjectPack, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if date != "" {
		list, err := e.store.ObjectsContext(ctx, date)
		if err != nil {
			return nil, err
		}
		for _, p := range list {
			if p.ObjHash == hash {
				return p, nil
			}
		}
		return nil, nil
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer e.mu.Unlock()
	p := e.objects[hash]
	if p == nil {
		return nil, nil
	}
	return e.liveObjectLocked(p, e.config.Clock().UnixMilli())
}
func (e *Engine) Text(ctx context.Context, date, div string, hash int32) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	if date == "" {
		date = storage.JavaDate(e.config.Clock().UnixMilli(), e.config.Location)
	}
	return e.store.TextContext(ctx, date, div, hash)
}
func (e *Engine) Counter(ctx context.Context, hash int32, name string, kind byte) (protocol.Value, bool, error) {
	if err := ctx.Err(); err != nil {
		return protocol.Value{}, false, err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return protocol.Value{}, false, err
	}
	defer e.mu.Unlock()
	key := counterKey{hash, name, kind}
	if e.current[key].expiry < e.config.Clock().UnixMilli() {
		e.expireCounter(key)
	}
	c, ok := e.current[key]
	if !ok {
		return protocol.Value{}, false, nil
	}
	v, err := protocol.DecodeValue(c.data, protocol.Limits{})
	return v, true, err
}
func (e *Engine) CounterHistory(ctx context.Context, hash int32, name string, start, end int64) ([]service.CounterPoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pts, err := e.counters.RealtimeContext(ctx, storage.JavaDate(start, e.config.Location), hash, name, start, end)
	if err != nil {
		return nil, err
	}
	out := make([]service.CounterPoint, len(pts))
	for i, p := range pts {
		out[i] = service.CounterPoint{Time: p.Time, Value: p.Value}
	}
	return out, nil
}

// CounterHistoryMulti is CounterHistory for several objects in one realtime
// index walk; each gets its points in the order of hashes (one object's
// points are decoded at a time).
func (e *Engine) CounterHistoryMulti(ctx context.Context, hashes []int32, name string, start, end int64, each func(int32, []service.CounterPoint) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	series, err := e.counters.RealtimeMultiContext(ctx, storage.JavaDate(start, e.config.Location), hashes, name, start, end)
	if err != nil {
		return err
	}
	for _, hash := range hashes {
		pts, err := series[hash].Points()
		if err != nil {
			return err
		}
		out := make([]service.CounterPoint, len(pts))
		for i, p := range pts {
			out[i] = service.CounterPoint{Time: p.Time, Value: p.Value}
		}
		if err := each(hash, out); err != nil {
			return err
		}
	}
	return nil
}
func (e *Engine) CounterDaily(ctx context.Context, date string, hash int32, name string, kind byte) ([]protocol.Value, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return e.counters.DailyContext(ctx, date, hash, name, kind)
}
func contains(hashes []int32, hash int32) bool {
	if len(hashes) == 0 {
		return true
	}
	for _, h := range hashes {
		if h == hash {
			return true
		}
	}
	return false
}
func (e *Engine) XLogsRealtime(ctx context.Context, q service.RealtimeQuery) (service.RealtimeResult, error) {
	if err := ctx.Err(); err != nil {
		return service.RealtimeResult{}, err
	}
	result, slots, err := e.realtimeSlots(ctx, q)
	if err != nil {
		return result, err
	}
	if e.realtimeDecodeHook != nil {
		e.realtimeDecodeHook()
	}
	// Decode and filter outside the engine lock: a Client query must not
	// stall ingest. A ring slot is never written in place (storeXLog stores
	// a fresh EncodePack result and only replaces the slot), so the bytes
	// referenced here stay valid and unchanged after the lock is released.
	for _, b := range slots {
		p, err := protocol.DecodePack(b, protocol.Limits{})
		if err != nil {
			return result, err
		}
		v := p.(*protocol.XLogPack)
		if contains(q.ObjectHashes, v.ObjHash) && (q.LatestCount > 0 || v.Elapsed >= q.MinElapsed || v.Error != 0) {
			result.Records = append(result.Records, v)
		}
	}
	return result, nil
}

// realtimeSlots is the engine-locked part of XLogsRealtime: the cursor and the
// encoded ring slots to visit, in visit order (empty slots skipped). The
// returned slices are shared with the ring and must only be read.
func (e *Engine) realtimeSlots(ctx context.Context, q service.RealtimeQuery) (service.RealtimeResult, [][]byte, error) {
	if err := e.mu.LockContext(ctx); err != nil {
		return service.RealtimeResult{}, nil, err
	}
	defer e.mu.Unlock()
	result := service.RealtimeResult{Loop: e.loop, Index: e.index}
	capacity := int32(len(e.ring))
	if q.Index < 0 || q.Index > capacity || q.LatestCount < 0 || q.LatestCount > capacity {
		return result, nil, protocol.ErrInvalid
	}
	var slots [][]byte
	add := func(from, to int32) {
		for i := from; i < to; i++ {
			if e.ring[i] != nil {
				slots = append(slots, e.ring[i])
			}
		}
	}
	if q.LatestCount > 0 {
		count := q.LatestCount
		switch e.loop - q.Loop {
		case 0:
			if gap := e.index - q.Index; gap > 0 && gap < count {
				count = gap
			}
		case 1:
			if gap := capacity - q.Index + e.index; gap < count {
				count = gap
			}
		}
		if count > e.index {
			add(capacity-(count-e.index), capacity)
			add(0, e.index)
		} else {
			add(e.index-count, e.index)
		}
	} else {
		switch e.loop - q.Loop {
		case 0:
			if q.Index < e.index {
				add(q.Index, e.index)
			}
		case 1:
			start := q.Index
			if start <= e.index {
				start = e.index
			}
			add(start, capacity)
			add(0, e.index)
		default:
			add(e.index, capacity)
			add(0, e.index)
		}
	}
	return result, slots, nil
}

// XLogsHistory streams TRANX_LOAD_TIME_GROUP(_V2) like Java XLogService: the
// object filter (and V1 elapsed bound) runs inside the storage scan, which
// ends at max (V1) or at PageCount+1 matches (V2: one more only to set
// hasMore, Java readByLimitCount's counted > limitCount). Nothing beyond one
// storage batch is buffered and there is no total record cap.
func (e *Engine) XLogsHistory(ctx context.Context, q service.HistoryQuery, visit func(*protocol.XLogPack) error) (service.HistoryResult, error) {
	var out service.HistoryResult
	if err := ctx.Err(); err != nil {
		return out, err
	}
	objects := hashSet(q.ObjectHashes)
	if q.PageCount > 0 {
		started := q.LastTxID == 0
		match := func(p *protocol.XLogPack) bool {
			if !started {
				started = p.TxID == q.LastTxID
				return false
			}
			return objects.has(p.ObjHash)
		}
		sent := 0
		scan := storage.XLogScan{Date: q.Date, From: q.From, To: q.To, Paged: true, LastBucket: q.LastBucketTime, Match: match, Max: q.PageCount + 1}
		err := e.store.ScanXLogsContext(ctx, scan, func(r storage.XLogRecord) (bool, error) {
			if sent >= q.PageCount {
				out.HasMore = true
				return false, nil
			}
			if err := visit(r.Pack); err != nil {
				return false, err
			}
			sent++
			out.LastBucketTime = r.BucketTime
			out.LastTxID = r.Pack.TxID
			return true, nil
		})
		return out, err
	}
	scan := storage.XLogScan{Date: q.Date, From: q.From, To: q.To, Reverse: q.Reverse, Max: max(q.Max, 0),
		Match: func(p *protocol.XLogPack) bool { return objects.has(p.ObjHash) && p.Elapsed > q.MinElapsed }}
	err := e.store.ScanXLogsContext(ctx, scan, func(r storage.XLogRecord) (bool, error) {
		return true, visit(r.Pack)
	})
	return out, err
}

// int32Set is an object-hash filter; an empty filter keeps every hash
// (contains semantics).
type int32Set map[int32]struct{}

func hashSet(hashes []int32) int32Set {
	if len(hashes) == 0 {
		return nil
	}
	set := make(int32Set, len(hashes))
	for _, h := range hashes {
		set[h] = struct{}{}
	}
	return set
}
func (s int32Set) has(hash int32) bool {
	if s == nil {
		return true
	}
	_, ok := s[hash]
	return ok
}

// ScanXLogsByTime streams the raw chronological range (service.TraceBackend).
func (e *Engine) ScanXLogsByTime(ctx context.Context, date string, from, to int64, visit func(*protocol.XLogPack) (bool, error)) error {
	return e.store.ScanXLogsContext(ctx, storage.XLogScan{Date: date, From: from, To: to}, func(r storage.XLogRecord) (bool, error) {
		return visit(r.Pack)
	})
}
func (e *Engine) XLog(ctx context.Context, date string, txid int64) (*protocol.XLogPack, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return e.store.XLogContext(ctx, date, txid)
}
func (e *Engine) Profile(ctx context.Context, date string, txid int64) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return e.store.ProfileChunksContext(ctx, date, txid)
}

// Sample5M persists the last numeric/boolean sample for each key at the tick time.
// It is deliberately neither an average nor a maximum.
//
// Like Java Auto5MSampling (workMap swap, then DailyCounterWR.add), the
// samples are taken out of the engine state under the engine lock and handed
// to the daily counter writer after it is released, as one pass item of that
// queue (order with the other daily records kept, never split by a full
// queue, exempt from the byte bound: at most MaxCounters samples). The
// samples stay encoded until the writer builds each pack, so a queued pass
// costs about the encoded bytes, not decoded packs. A write error ends the
// pass (logged and counted by the writer); the samples not yet written are
// not retried and are counted in Stats.Sample5MDropped, as are all of them
// when the pass cannot be queued.
func (e *Engine) Sample5M(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now, samples, err := e.takeSamples(ctx)
	if err != nil || len(samples) == 0 {
		return err
	}
	if e.periodicWriteHook != nil {
		e.periodicWriteHook()
	}
	size := 0
	for _, x := range samples {
		size += sample5MBytes + len(x.value)
	}
	if e.w.enqueuePass(writeDaily, passSample5M, len(samples), size, func(ctx context.Context) (int, error) {
		for i, x := range samples {
			v, err := protocol.DecodeValue(x.value, protocol.Limits{})
			if err == nil {
				err = e.counters.PutContext(ctx, &protocol.PerfCounterPack{Time: now, ObjName: x.objName, TimeType: 3, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: x.name, Value: v}}}})
			}
			if err != nil {
				e.sample5MDropped.Add(uint64(len(samples) - i))
				return i, err
			}
		}
		return len(samples), nil
	}) != nil {
		e.sample5MDropped.Add(uint64(len(samples)))
	}
	return nil
}

// sample5M is one 5m sample of a pass: the object and counter names (shared
// strings) and the encoded value (the read-only slice of the sample map).
type sample5M struct {
	objName, name string
	value         []byte
}

// sample5MBytes is the estimated heap of a sample5M besides its value bytes
// (two string headers and a slice header).
const sample5MBytes = int(unsafe.Sizeof(sample5M{}))

// takeSamples takes the 5m samples and clears them (the engine-locked part of
// Sample5M). The encoded values are read-only and shared with the counter
// cache (cacheCounterLocked replaces slices, never writes into one), so the
// pass may keep them.
func (e *Engine) takeSamples(ctx context.Context) (int64, []sample5M, error) {
	if err := e.mu.LockContext(ctx); err != nil {
		return 0, nil, err
	}
	defer e.mu.Unlock()
	if e.closed {
		return 0, nil, storage.ErrClosed
	}
	if !e.settings().Auto5MSampling {
		// Auto5MSampling: the tick skips the write and keeps the map (the
		// samples keep accumulating until the flag is true again).
		return 0, nil, nil
	}
	now := e.config.Clock().UnixMilli()
	samples := make([]sample5M, 0, len(e.samples))
	for key, b := range e.samples {
		e.cacheBytes -= len(b) + len(key.name) + 16
		objName, exists := e.counterNames[key.hash]
		if !exists {
			continue
		}
		samples = append(samples, sample5M{objName, key.name, b})
	}
	clear(e.samples)
	e.pruneCounterNames()
	return now, samples, nil
}

func (e *Engine) AgentRequest(ctx context.Context, hash int32, cmd string, param *protocol.MapPack) ([]protocol.Pack, error) {
	object, err := e.Object(ctx, "", hash)
	if err != nil {
		return nil, err
	}
	if object == nil {
		return nil, nil
	}
	if e.config.AgentCall == nil {
		return nil, protocol.ErrUnsupported
	}
	return e.config.AgentCall(ctx, hash, cmd, param)
}

// TrackLocks starts the measurement-only sums of the engine lock
// (acquisitions, held and waited time) that LockStats reports.
func (e *Engine) TrackLocks() { e.mu.Track() }

// LockStats reports the engine lock sums; zero until TrackLocks.
func (e *Engine) LockStats() syncx.Stats { return e.mu.Stats() }
