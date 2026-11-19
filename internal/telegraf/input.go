package telegraf

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Acceptor is the ingest entry point (ingest.Engine.AcceptFrom).
type Acceptor interface {
	AcceptFrom(ctx context.Context, p protocol.Pack, peerHost string) error
}

// MaxLines is the TelegrafInputHandler payload limit: a payload with more
// than 1000 lines is dropped entirely (D7).
const MaxLines = 1000

// Options configure an Input. Zero values select the defaults.
type Options struct {
	Limits Limits
	// QueueSize bounds the object type and counter registration queues (Java
	// RequestQueue(1024); a full queue drops the request, as RequestQueue.put).
	QueueSize int
	// Clock defaults to time.Now.
	Clock func() time.Time
	// Logf defaults to log.Printf. Debug line logging is rate limited.
	Logf func(format string, args ...any)
}

// Stats are cumulative Input counters. Lines that produce no packs are
// counted by reason; Accepted counts packs handed to ingest without error.
type Stats struct {
	Requests, DisabledRequests, OversizedPayloads    uint64
	Lines, DuplicateLines, LongLines                 uint64
	UnknownMeasurement, DisabledMeasurement          uint64
	InvalidConfig, Filtered, ParseErrors             uint64
	PendingObjType, PendingCounter                   uint64
	RegisterQueueFull, CounterQueueFull              uint64
	Registered, RegisterErrors, CounterAdds          uint64
	CounterAddErrors, IgnoredEqualCounters, Notifies uint64
	UnregisteredTypes                                uint64
	Points, AcceptedPacks, AcceptErrors              uint64
	SuppressedDebugLogs                              uint64
	// CancelledLines are unique lines left unprocessed because the request
	// context ended (client gone or server shutdown).
	CancelledLines uint64
	Converter      ConverterStats
}

// Input is TelegrafInputHandler: it parses a request body, applies the
// measurement configuration, gates new object types and counters through the
// registrar and hands ObjectPack/PerfCounterPack to ingest. With a nil
// registrar every resolved line is ingested and unknown object types are
// logged once each. Input owns one registration goroutine when a registrar is
// given; Close stops it.
type Input struct {
	cfg       atomic.Pointer[Config]
	registrar TypeRegistrar
	accept    Acceptor
	conv      *Converter
	clock     func() time.Time
	logf      func(string, ...any)

	registerQ chan *Point
	counterQ  chan addCounterParam
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once

	unregisteredMu sync.Mutex
	unregistered   map[string]struct{}

	debugMu     sync.Mutex
	debugSecond int64
	debugCount  int

	requests, disabledRequests, oversized                     atomic.Uint64
	lines, duplicates, longLines                              atomic.Uint64
	unknown, disabled, invalid, filtered, parseErrors         atomic.Uint64
	pendingType, pendingCounter, registerFull, counterFull    atomic.Uint64
	registered, registerErrors, counterAdds, counterAddErrors atomic.Uint64
	ignoredEqual, notifies, unregisteredTypes                 atomic.Uint64
	points, acceptedPacks, acceptErrors, suppressedDebug      atomic.Uint64
	cancelledLines                                            atomic.Uint64
}

type addCounterParam struct {
	objectType ObjectTypeDef
	counter    CounterDef
}

const (
	maxUnregisteredLogged = 1024
	debugLogsPerSecond    = 20
	prevAddedKeep         = 5 * time.Second
	prevAddedMax          = 10000
)

// NewInput builds the input. cfg must not be nil; registrar may be nil.
func NewInput(cfg *Config, registrar TypeRegistrar, accept Acceptor, opts Options) *Input {
	if opts.QueueSize <= 0 {
		opts.QueueSize = 1024
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	if opts.Logf == nil {
		opts.Logf = log.Printf
	}
	in := &Input{registrar: registrar, accept: accept, conv: NewConverter(opts.Limits, opts.Clock), clock: opts.Clock, logf: opts.Logf, unregistered: map[string]struct{}{}, stop: make(chan struct{}), done: make(chan struct{})}
	in.cfg.Store(cfg)
	if registrar == nil {
		close(in.done)
		return in
	}
	in.registerQ = make(chan *Point, opts.QueueSize)
	in.counterQ = make(chan addCounterParam, opts.QueueSize)
	go in.run()
	return in
}

// SetConfig publishes a reloaded configuration for subsequent requests.
func (in *Input) SetConfig(cfg *Config) {
	if cfg != nil {
		in.cfg.Store(cfg)
	}
}

// Config returns the configuration in use.
func (in *Input) Config() *Config { return in.cfg.Load() }

// Close stops the registration goroutine. Queued registrations that were not
// processed are discarded and returned as the residual count.
func (in *Input) Close() (residual int) {
	in.closeOnce.Do(func() {
		close(in.stop)
		<-in.done
	})
	if in.registerQ != nil {
		residual = len(in.registerQ) + len(in.counterQ)
	}
	return residual
}

// Stats returns a snapshot.
func (in *Input) Stats() Stats {
	return Stats{Requests: in.requests.Load(), DisabledRequests: in.disabledRequests.Load(), OversizedPayloads: in.oversized.Load(),
		Lines: in.lines.Load(), DuplicateLines: in.duplicates.Load(), LongLines: in.longLines.Load(),
		UnknownMeasurement: in.unknown.Load(), DisabledMeasurement: in.disabled.Load(), InvalidConfig: in.invalid.Load(), Filtered: in.filtered.Load(), ParseErrors: in.parseErrors.Load(),
		PendingObjType: in.pendingType.Load(), PendingCounter: in.pendingCounter.Load(), RegisterQueueFull: in.registerFull.Load(), CounterQueueFull: in.counterFull.Load(),
		Registered: in.registered.Load(), RegisterErrors: in.registerErrors.Load(), CounterAdds: in.counterAdds.Load(), CounterAddErrors: in.counterAddErrors.Load(),
		IgnoredEqualCounters: in.ignoredEqual.Load(), Notifies: in.notifies.Load(), UnregisteredTypes: in.unregisteredTypes.Load(),
		Points: in.points.Load(), AcceptedPacks: in.acceptedPacks.Load(), AcceptErrors: in.acceptErrors.Load(), SuppressedDebugLogs: in.suppressedDebug.Load(), CancelledLines: in.cancelledLines.Load(),
		Converter: in.conv.Stats()}
}

// Enabled reports input_telegraf_enabled. A disabled input answers requests
// without reading the body.
func (in *Input) Enabled() bool { return in.cfg.Load().Enabled }

// Handle is handlerRequest for an already decoded body. remoteAddr is
// HttpServer.getRemoteAddr (first X-FORWARDED-FOR item or the peer address).
// Lines are split like BufferedReader.readLine; more than MaxLines lines drop
// the whole payload. Lines are de-duplicated by LineKey (last line wins) and
// processed in first-appearance order of their key (Java: HashMap order).
// It returns an error only when ctx is done.
func (in *Input) Handle(ctx context.Context, body string, remoteAddr string) error {
	in.requests.Add(1)
	cfg := in.cfg.Load()
	if !cfg.Enabled {
		in.disabledRequests.Add(1)
		return nil
	}
	received := in.clock().UnixMilli()
	lines := splitLines(body, MaxLines+1)
	if len(lines) > MaxLines {
		in.oversized.Add(1)
		in.logf("TG010 [WARN] Too many line protocol in payload. fast return working. some line could be dropped! (lines>%d, payload dropped)", MaxLines)
		return nil
	}
	index := make(map[string]int, len(lines))
	unique := make([]string, 0, len(lines))
	for _, s := range lines {
		in.lines.Add(1)
		if len(s) > MaxLineBytes {
			in.longLines.Add(1)
			continue
		}
		if cfg.Debug {
			in.debug("TG002 [line protocol received] %s", s)
		}
		key := LineKey(s)
		if i, ok := index[key]; ok {
			in.duplicates.Add(1)
			unique[i] = s
			continue
		}
		index[key] = len(unique)
		unique = append(unique, s)
	}
	peer := ipv4Peer(remoteAddr)
	for i, s := range unique {
		if err := ctx.Err(); err != nil {
			in.cancelledLines.Add(uint64(len(unique) - i))
			return err
		}
		line := ParseLine(s)
		if m := cfg.Measurements[line.Measurement]; m != nil && m.Debug && !cfg.Debug {
			in.debug("TG006 [line protocol received] %s", s)
		}
		p, reason := Resolve(&line, cfg, received)
		if cfg.Debug {
			in.debug("TG003 [line protocol] %s [line parsed] %s", s, p)
		} else if p != nil && p.Measurement.Debug {
			in.debug("TG004 [line protocol] %s [line parsed] %s", s, p)
		}
		switch reason {
		case UnknownMeasurement:
			in.unknown.Add(1)
		case Disabled:
			in.disabled.Add(1)
		case InvalidConfig:
			in.invalid.Add(1)
		case Filtered:
			in.filtered.Add(1)
		case ParseError:
			in.parseErrors.Add(1)
		}
		if p == nil {
			continue
		}
		in.points.Add(1)
		if err := in.count(ctx, p, cfg, remoteAddr, peer); err != nil {
			return err
		}
	}
	return nil
}

// count is TelegrafInputHandler.count.
func (in *Input) count(ctx context.Context, p *Point, cfg *Config, remoteAddr, peer string) error {
	if in.registrar != nil {
		objectType, ok := in.registrar.ObjectType(p.ObjType)
		if !ok {
			in.pendingType.Add(1)
			select {
			case in.registerQ <- p:
			default:
				in.registerFull.Add(1)
			}
			return nil
		}
		if in.hasNewCounterThenRegister(objectType, p) {
			in.pendingCounter.Add(1)
			return nil
		}
	} else {
		in.noteUnregistered(p.ObjType)
	}
	in.deliver(ctx, ObjectPack(p, remoteAddr, cfg.ObjectDeadtimeMs), peer)
	in.deliver(ctx, in.conv.PerfCounterPack(p, cfg), peer)
	return ctx.Err()
}

func (in *Input) deliver(ctx context.Context, pack protocol.Pack, peer string) {
	if err := in.accept.AcceptFrom(ctx, pack, peer); err != nil {
		in.acceptErrors.Add(1)
		return
	}
	in.acceptedPacks.Add(1)
}

func (in *Input) hasNewCounterThenRegister(objectType ObjectTypeDef, p *Point) bool {
	now := in.clock()
	found := false
	for _, f := range p.Fields {
		if !f.Counter.NewOrChanged(objectType, p.Tags, now) {
			continue
		}
		found = true
		for _, c := range f.Counter.Counters(p.Tags) {
			select {
			case in.counterQ <- addCounterParam{objectType: objectType, counter: c}:
			default:
				in.counterFull.Add(1)
			}
		}
	}
	return found
}

func (in *Input) noteUnregistered(objType string) {
	in.unregisteredMu.Lock()
	defer in.unregisteredMu.Unlock()
	if _, seen := in.unregistered[objType]; seen || len(in.unregistered) >= maxUnregisteredLogged {
		return
	}
	in.unregistered[objType] = struct{}{}
	in.unregisteredTypes.Add(1)
	in.logf("telegraf: no counter registrar; object type %q ingested without counters.xml registration", objType)
}

func (in *Input) debug(format string, args ...any) {
	sec := in.clock().Unix()
	in.debugMu.Lock()
	if sec != in.debugSecond {
		in.debugSecond, in.debugCount = sec, 0
	}
	in.debugCount++
	allowed := in.debugCount <= debugLogsPerSecond
	in.debugMu.Unlock()
	if !allowed {
		in.suppressedDebug.Add(1)
		return
	}
	in.logf(format, args...)
}

// run is the registration thread: object types first, then counters, then
// one client notification for the counters added in this round.
func (in *Input) run() {
	defer close(in.done)
	prevAdded := newFIFO[string, prevAdded](prevAddedMax)
	for {
		select {
		case <-in.stop:
			return
		case p := <-in.registerQ:
			in.registerObjType(p)
		case param := <-in.counterQ:
			notify := in.addCounter(param, &prevAdded)
		drain:
			for {
				select {
				case <-in.stop:
					break drain
				case next := <-in.counterQ:
					notify = in.addCounter(next, &prevAdded) || notify
				default:
					break drain
				}
			}
			if notify {
				in.notifies.Add(1)
				in.registrar.NotifyClients()
			}
		}
	}
}

type prevAdded struct {
	counter CounterDef
	expires time.Time
}

// registerObjType is registerNewObjType0.
func (in *Input) registerObjType(p *Point) {
	if _, ok := in.registrar.ObjectType(p.ObjType); ok {
		return
	}
	family := FamilyDef{Name: p.Family}
	for _, f := range p.Fields {
		for _, c := range f.Counter.Counters(p.Tags) {
			family.Counters = upsertCounter(family.Counters, c)
			if family.Master == "" {
				family.Master = c.Name
			}
		}
	}
	objectType := ObjectTypeDef{Name: p.ObjType, DisplayName: p.ObjType, Icon: p.ObjTypeIcon, Family: p.Family}
	err := in.registrar.SafelyAddFamily(family)
	if err == nil {
		err = in.registrar.SafelyAddObjectType(objectType)
	}
	if err != nil {
		in.registerErrors.Add(1)
		in.logf("HT-001 Error on register telegraf type %q: %v", p.ObjType, err)
		return
	}
	in.registered.Add(1)
	in.notifies.Add(1)
	in.registrar.NotifyClients()
}

// addCounter is addCounter0; it reports whether a change notification is due.
func (in *Input) addCounter(param addCounterParam, prev *fifo[string, prevAdded]) bool {
	now := in.clock()
	if last, ok := prev.get(param.counter.Name); ok && now.Before(last.expires) && param.counter.SemanticEquals(last.counter) {
		in.ignoredEqual.Add(1)
		return false
	}
	if err := in.registrar.AddFamilyCounter(param.objectType.Family, param.counter); err != nil {
		in.counterAddErrors.Add(1)
		in.logf("telegraf: add counter %q to family %q: %v", param.counter.Name, param.objectType.Family, err)
		return false
	}
	in.counterAdds.Add(1)
	prev.put(param.counter.Name, prevAdded{counter: param.counter, expires: now.Add(prevAddedKeep)})
	return true
}

func upsertCounter(list []CounterDef, c CounterDef) []CounterDef {
	for i := range list {
		if list[i].Name == c.Name {
			list[i] = c
			return list
		}
	}
	return append(list, c)
}

// splitLines is BufferedReader.readLine over the whole body: '\n', '\r' and
// "\r\n" end a line; a trailing terminator does not create an empty line. At
// most limit lines are returned.
func splitLines(s string, limit int) []string {
	var out []string
	for len(s) > 0 && len(out) < limit {
		i := strings.IndexAny(s, "\r\n")
		if i < 0 {
			out = append(out, s)
			break
		}
		out = append(out, s[:i])
		if s[i] == '\r' && i+1 < len(s) && s[i+1] == '\n' {
			i++
		}
		s = s[i+1:]
	}
	return out
}

// ipv4Peer is InetAddress.getByAddress(IPUtil.toBytes(addr)).getHostAddress():
// a dotted IPv4 address passes through, anything else becomes 0.0.0.0 (Java
// throws for an octet outside 0..255; Go also maps it to 0.0.0.0).
func ipv4Peer(addr string) string {
	parts := splitDropEmpty(addr, '.')
	if len(parts) != 4 {
		return "0.0.0.0"
	}
	var b [4]byte
	for i, p := range parts {
		n, err := strconv.ParseInt(p, 10, 32)
		if err != nil || n < 0 || n > 255 {
			return "0.0.0.0"
		}
		b[i] = byte(n)
	}
	return net.IPv4(b[0], b[1], b[2], b[3]).String()
}

// String renders the point for debug logs (InfluxSingleLine.toString shape).
func (p *Point) String() string {
	if p == nil {
		return "null"
	}
	var fields []string
	for _, f := range p.Fields {
		fields = append(fields, fmt.Sprintf("%s=%v", f.Counter.Name(), valueText(f.Value)))
	}
	return fmt.Sprintf("InfluxSingleLine{measurement='%s', host='%s', objType='%s', objName='%s', objHash=%d, receivedTime=%d, timestampOrigin=%d, tags=%v, numberFields=[%s]}",
		p.Measurement.Name, p.Host, p.ObjType, p.ObjName, p.ObjHash, p.ReceivedTime, p.TimestampMs, p.Tags, strings.Join(fields, ", "))
}

func valueText(v protocol.Value) string {
	if v.Type == protocol.ValueDecimal {
		return strconv.FormatInt(v.Int, 10)
	}
	return strconv.FormatFloat(float64(v.Float), 'g', -1, 32)
}
