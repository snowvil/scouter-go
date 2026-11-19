package server

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/config"
	"github.com/snowvil/scouter-go/internal/counters"
	"github.com/snowvil/scouter-go/internal/ingest"
	"github.com/snowvil/scouter-go/internal/logger"
)

// Live settings (F0, docs/compatibility/config-behavior.md §5.0).
//
// ApplySettings is called once by Open and then by the config.Store
// OnChange callback (cmd/scouter-go), which runs under the reload lock. It
// therefore only stores the new value, runs the registered setters (atomic
// stores, no I/O, no lock the reload path waits on) and signals the owner
// goroutine with a size-1 channel; several changes before the owner runs
// coalesce into one pass over the latest value. Work with I/O (the GeoIP
// file) runs on the owner goroutine, which Run starts and stops with ctx.
//
// Extension points for other packages:
//   - OnSettings(func(prev, next config.Live)): a setter run inside
//     ApplySettings, e.g. transport.TCPServer.SetClientSoTimeout.
//   - OnSettingsOwner(func(ctx, config.Live)): work with I/O on the owner
//     goroutine, e.g. reopening a file.
//   - Settings(): a getter for consumers that take "func() X" (wrap it and
//     convert the fields they need).
//
// Hooks registered after Open must still be cheap; OnSettings hooks run on
// the next ApplySettings, not immediately.
type liveSettings struct {
	current atomic.Pointer[config.Live]
	signal  chan struct{}

	mu     sync.Mutex
	setter []func(prev, next config.Live)
	owner  []func(ctx context.Context, next config.Live)
}

func newLiveSettings() *liveSettings {
	return &liveSettings{signal: make(chan struct{}, 1)}
}

// ApplySettings publishes l (see liveSettings).
func (r *Runtime) ApplySettings(l config.Live) {
	s := r.live
	s.mu.Lock()
	setters := append([]func(prev, next config.Live){}, s.setter...)
	s.mu.Unlock()
	next := l
	prevPtr := s.current.Swap(&next)
	prev := config.DefaultLive()
	if prevPtr != nil {
		prev = *prevPtr
	}
	for _, f := range setters {
		f(prev, next)
	}
	select {
	case s.signal <- struct{}{}:
	default: // a pass is already pending; it reads the latest value
	}
}

// Settings returns the live settings in use.
func (r *Runtime) Settings() config.Live {
	if p := r.live.current.Load(); p != nil {
		return *p
	}
	return config.DefaultLive()
}

// OnSettings registers a setter run by every ApplySettings.
func (r *Runtime) OnSettings(f func(prev, next config.Live)) {
	r.live.mu.Lock()
	defer r.live.mu.Unlock()
	r.live.setter = append(r.live.setter, f)
}

// OnSettingsOwner registers work run on the owner goroutine after changes.
func (r *Runtime) OnSettingsOwner(f func(ctx context.Context, next config.Live)) {
	r.live.mu.Lock()
	defer r.live.mu.Unlock()
	r.live.owner = append(r.live.owner, f)
}

// runSettingsOwner is the owner goroutine: one pass per coalesced signal.
func (r *Runtime) runSettingsOwner(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.live.signal:
			r.live.mu.Lock()
			owners := append([]func(context.Context, config.Live){}, r.live.owner...)
			r.live.mu.Unlock()
			next := r.Settings()
			for _, f := range owners {
				if ctx.Err() != nil {
					return
				}
				f(ctx, next)
			}
		}
	}
}

// initialSettings is the startup Live: the scouter.conf snapshot (flag pins
// applied), or without a file the defaults with the Config values that
// main fills from flags (server id, log directory, compression, dead time,
// retention switch).
func initialSettings(cfg Config) config.Live {
	if cfg.ConfigFile != nil {
		return cfg.ConfigFile.Current().Live()
	}
	l := config.DefaultLive()
	if cfg.ObjectDeadTime > 0 {
		l.ObjectDeadTimeMs = int(cfg.ObjectDeadTime / time.Millisecond)
	}
	l.ServerID = cfg.ServerID
	if l.ServerID == "" {
		l.ServerID = "1" // service.New default
	}
	l.LogDir = cfg.LogDirectory
	l.CompressXLog, l.CompressProfile = cfg.CompressXLog, cfg.CompressProfile
	l.PurgeEnabled = cfg.Retention != nil
	return l
}

// ingestSettings converts the ingest part of Live.
func ingestSettings(l config.Live) ingest.Settings {
	return ingest.Settings{
		ObjectDeadTimeMs: int64(l.ObjectDeadTimeMs), ZipkinDeadTimeMs: int64(l.ObjectZipkinDeadTimeMs),
		InactiveAlertLevel: int32(l.ObjectInactiveAlertLevel), TagCount: l.TagCountEnabled, Auto5MSampling: l.Auto5MSampling,
		XLogKeepCount: l.SamplingXLogKeepCount, XLogKeepMillis: l.SamplingXLogKeepMillis,
		ProfileKeepCount: l.SamplingProfileKeepCount, ProfileKeepSecs: l.SamplingProfileKeepSecs, GeoIP: l.GeoIPEnabled,
	}
}

// wireSettings registers the built-in consumers: ingest (dead times, alert
// level, tag count, 5m sampling, sampling bounds, GeoIP flag), the agent
// pool (T3), the service, transport, storage, retention, logger, webapp
// and LDAP setters (applyLive) and the GeoIP file reload (I7, owner
// goroutine). Open calls it once every consumer exists.
func (r *Runtime) wireSettings() {
	var applied atomic.Bool
	r.OnSettings(func(prev, next config.Live) {
		r.engine.SetSettings(ingestSettings(next))
		r.pool.timing.store(next.AgentKeepAliveIntervalMs, next.AgentConnectionWaitMs)
		first := !applied.Swap(true)
		if l, p := next.Limits, prev.Limits; first || l.AgentRelayBudgetMB != p.AgentRelayBudgetMB || l.AgentRelayMaxLarge != p.AgentRelayMaxLarge || l.AgentRelayServerWaitMs != p.AgentRelayServerWaitMs {
			r.pool.budget.configure(int64(l.AgentRelayBudgetMB)<<20, l.AgentRelayMaxLarge, millis(l.AgentRelayServerWaitMs))
		}
		// go_text_max_unknown_divs (0: the storage default).
		r.store.SetMaxUnknownTextDivs(next.Limits.TextMaxUnknownDivs)
		// go_visitor_resident_mb, go_visitor_delta_mb (0: the storage
		// defaults): stored atomically, applied by the visitor state at its
		// next visit, roll tick or pass.
		if l, p := next.Limits, prev.Limits; first || l.VisitorResidentMB != p.VisitorResidentMB || l.VisitorDeltaMB != p.VisitorDeltaMB {
			r.store.SetVisitorBudgets(l.VisitorResidentMB, l.VisitorDeltaMB)
		}
		r.applyLive(prev, next, first)
	})
	if r.config.GeoIP == nil && r.config.GeoIPFile == "" {
		r.OnSettingsOwner(func(_ context.Context, next config.Live) { r.ReloadGeoIP(next.GeoIPDataCityFile) })
	}
}

// objectType is CounterManager.getCounterEngine().getObjectType(t) for
// ingest: the merged counters.xml + counters.site.xml engine when ConfDir is
// set (so DEFINE_OBJECT_TYPE and addObjectTypeIfNotExist apply), otherwise
// the bundled document. r.countersSite is assigned during Open, before Run
// starts the listeners that reach this hook.
func (r *Runtime) objectType(defaults *counters.Engine) func(string) (ingest.ObjectTypeInfo, bool) {
	return func(objType string) (ingest.ObjectTypeInfo, bool) {
		engine := defaults
		if r.countersSite != nil {
			engine = r.countersSite.Engine()
		}
		t := engine.ObjectType(objType)
		if t == nil {
			return ingest.ObjectTypeInfo{}, false
		}
		info := ingest.ObjectTypeInfo{SubObject: t.SubObject}
		if t.Family != nil {
			info.Family = t.Family.Name
		}
		return info, true
	}
}

// addObjectTypeIfNotExist is CounterManager.addObjectTypeIfNotExist: an
// unknown objType whose "detected" type is known is appended to
// counters.site.xml as {name=objType, disp=objType, icon=detected,
// family=detected's family, sub-object=detected's}. Without a counters
// store (no ConfDir) Go cannot persist it; that is logged once.
func (r *Runtime) addObjectTypeIfNotExist(objType, detected string) {
	site := r.countersSite
	if site == nil {
		r.noSiteOnce.Do(func() {
			log.Printf("counters: object type %q (detected %q) cannot be added without a configuration directory (counters.site.xml)", objType, detected)
		})
		return
	}
	engine := site.Engine()
	if engine.ObjectType(objType) != nil {
		return
	}
	dt := engine.ObjectType(detected)
	if dt == nil {
		return
	}
	spec := counters.ObjectTypeSpec{Name: objType, Display: objType, Icon: dt.Name, SubObject: dt.SubObject}
	if dt.Family != nil {
		spec.Family = dt.Family.Name
	}
	debug := r.config.Logger != nil && r.liveNow().LogUDPSomeObject == objType
	if debug {
		r.config.Logger.Stdout(logger.SomeObjectWillBeAddedLine(objType))
	}
	ok, err := site.DefineObjectType(context.Background(), spec)
	if debug && err == nil && ok {
		// Java prints the XML it wrote; Go prints the saved custom document.
		r.config.Logger.Stdout(logger.SomeObjectNewXMLLine(objType, string(site.CustomXML())))
	}
	if err == nil && !ok && site.Engine().ObjectType(objType) != nil {
		return // another worker added it first
	}
	if err != nil || !ok {
		r.addTypeFailures.Add(1)
		if now := time.Now().UnixMilli(); now >= r.addTypeLogAt.Load() {
			r.addTypeLogAt.Store(now + 10000)
			log.Printf("counters: add object type %q (detected %q): ok=%v err=%v", objType, detected, ok, err)
		}
		return
	}
	log.Printf("counters: object type %q added from detected type %q", objType, detected)
}
