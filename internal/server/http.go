package server

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/httpapi"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/telegraf"
	"github.com/snowvil/scouter-go/internal/webapp"
)

// httpServer is the opt-in operational endpoint: GET /health returns the live
// counters as JSON and /debug/pprof exposes the Go profiler. It serves only
// what --http binds (recommended 127.0.0.1) and never speaks the Scouter wire
// protocol; with the Java servlet api it also serves the servlet and webapp
// paths (operationalMux).
type httpServer struct {
	addr   string
	server *http.Server
	ln     net.Listener
	done   chan struct{}
	once   sync.Once
	err    error
	// started is set by serve; close without serve only releases the listener.
	started atomic.Bool
	// limits is the listener's admission state (http_admission.go).
	limits *httpLimits
}

// newHTTPServerWithAPI is the --http listener; a non-nil api additionally
// mounts the Java HttpServer servlet paths (/counter, /register, /telegraf)
// on it.
func newHTTPServerWithAPI(address string, r *Runtime, api http.Handler) (*httpServer, error) {
	s, err := listenHTTP(address, r.operationalMux(api), r.admission)
	if err != nil {
		return nil, err
	}
	if tcp, ok := s.ln.Addr().(*net.TCPAddr); ok && !tcp.IP.IsLoopback() {
		log.Printf("WARNING: --http %s is not loopback; /debug/pprof is unauthenticated", s.ln.Addr())
	}
	return s, nil
}

// operationalMux serves GET /health and /debug/pprof, plus the servlet and
// webapp paths when api (the scouterHTTP handler) is not nil.
func (r *Runtime) operationalMux(api http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", r.healthHandler)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	if api != nil {
		mountServlets(mux, api)
		mux.Handle("/scouter/", api)
	}
	return mux
}

// mountServlets maps the Jetty "/name/*" servlet patterns: the bare path and
// every sub-path.
func mountServlets(mux *http.ServeMux, api http.Handler) {
	for _, p := range []string{"/counter", "/register", "/telegraf"} {
		mux.Handle(p, api)
		mux.Handle(p+"/", api)
	}
}

// listenHTTP binds address with the admission limits a (zero: the
// defaults); serve starts the server. A go_http_max_connections the open
// file limit cannot hold is lowered and logged.
func listenHTTP(address string, handler http.Handler, a httpAdmission) (*httpServer, error) {
	set := a.maxConns > 0
	a = a.resolved()
	conns := httpConnectionLimit(a.maxConns)
	if set && conns < a.maxConns {
		log.Printf("config: go_http_max_connections=%d lowered to %d for %s by the open file limit (ulimit -n)", a.maxConns, conns, address)
	}
	return listenHTTPWith(address, handler, newHTTPLimits(conns, httpOverflowConns, a.opsInFlight))
}

func listenHTTPWith(address string, handler http.Handler, limits *httpLimits) (*httpServer, error) {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	limits.addr = ln.Addr().String()
	s := &httpServer{addr: ln.Addr().String(), ln: ln, done: make(chan struct{}), limits: limits}
	// Idle keep-alive connections end after ReadTimeout (IdleTimeout unset),
	// like the Jetty connector's 30s idle timeout.
	s.server = &http.Server{Handler: limits.handler(handler), ConnContext: limits.connContext,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 5 * time.Minute, MaxHeaderBytes: 16 << 10}
	return s, nil
}

func (s *httpServer) serve(ctx context.Context) {
	s.started.Store(true)
	go func() {
		defer close(s.done)
		if err := s.server.Serve(s.limits.listener(s.ln)); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.err = err
		}
	}()
	context.AfterFunc(ctx, func() { s.close() })
}

func (s *httpServer) close() error {
	s.once.Do(func() {
		if !s.started.Load() {
			s.err = s.ln.Close()
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.server.Shutdown(ctx); err != nil {
			s.server.Close()
		}
		<-s.done
	})
	return s.err
}

// Health is the /health body. Its counters are also in Run's final stats
// line, except IndexOpens, HTTPAdmission, ScouterHTTP, Stats and Locks
// (/health only) and Pulse (its own shutdown line); they are snapshots, not
// guarantees of durability.
type Health struct {
	Status    string    `json:"status"`
	Time      time.Time `json:"time"`
	Endpoints Endpoints `json:"endpoints"`
	Ingest    any       `json:"ingest"`
	// IngestWriters are the ingest record writer queues per kind (counter,
	// daily, object, status, alert, summary, stack, batch, span, visitor).
	IngestWriters any `json:"ingestWriters"`
	UDP           any `json:"udp"`
	TCP           any `json:"tcp"`
	// AgentOrphans are the Agent relays still reading after their Client
	// left (agent_pool_relay.go).
	AgentOrphans any `json:"agentOrphans"`
	// AgentRelayBudget is the byte budget of all Agent relays, orphans
	// included (agent_relay_budget.go).
	AgentRelayBudget any `json:"agentRelayBudget"`
	// Visitors are the visitor HLL entries, resident/delta memory against
	// their budgets and deferred write-through failures (storage §8.1).
	Visitors any `json:"visitors"`
	// IndexOpens counts how index pairs were opened since start (walked,
	// sealed by .go-clean, checkpointed after a crash, records walked).
	IndexOpens map[string]storage.IndexOpenStats `json:"indexOpens"`
	TagCount   any                               `json:"tagCount"`
	// TextCache is the storage text read cache and the bounded-wait lookups
	// of the ingest path (storage.TextCacheStats).
	TextCache any `json:"textCache"`
	// XLogQueue and ProfileQueue are the storage writer queues (XLogWR,
	// XLogProfileWR): enqueued, written, dropped (S144/S142), write errors.
	XLogQueue    any               `json:"xlogQueue"`
	ProfileQueue any               `json:"profileQueue"`
	Maintenance  map[string]uint64 `json:"maintenance"`
	Pulse        any               `json:"pulse,omitempty"`
	PulseError   string            `json:"pulseError,omitempty"`
	ScouterHTTP  any               `json:"scouterHttp,omitempty"`
	// HTTPAdmission is the connection limit and operational gate per HTTP
	// listener ("http", "scouterHttp") and the webapp gate ("webapp")
	// (http_admission.go). The servlet limit is in ScouterHTTP (Busy,
	// Waited, Abandoned, InFlight).
	HTTPAdmission map[string]any `json:"httpAdmission,omitempty"`
	// Stats is the last --stats-interval window (rates, latency quantiles,
	// memory); absent until the first tick or when the interval is 0.
	Stats *IntervalStats `json:"stats,omitempty"`
	// Locks is present with Config.LockStats (measurement only).
	Locks map[string]storage.LockStat `json:"locks,omitempty"`
}

func (r *Runtime) healthHandler(w http.ResponseWriter, req *http.Request) {
	h := Health{Status: "ok", Time: time.Now(), Ingest: r.engine.Stats(), IngestWriters: r.engine.WriterStats(), UDP: r.udp.Stats(), TCP: r.tcp.Stats(), AgentOrphans: r.pool.orphans.snapshot(), AgentRelayBudget: r.pool.budget.snapshot(), TagCount: r.store.TagCountStats(), TextCache: r.store.TextCacheStats(),
		XLogQueue: r.store.XLogQueueStats(), ProfileQueue: r.store.ProfileQueueStats(),
		Maintenance: map[string]uint64{"errors": r.maintenanceErrors.Load(), "purged": r.purged.Load(), "recoveries": r.recoveries.Load(), "droppedErrorLogs": r.droppedErrorEvents.Load(), "textWriteErrors": r.store.TextWriteErrors(), "textDropped": r.store.TextDropped(), "counterNamesLimited": r.counters.NamesLimited()}}
	h.HTTPAdmission = map[string]any{}
	if r.http != nil {
		h.Endpoints.HTTP = r.http.addr
		h.HTTPAdmission["http"] = r.http.limits.stats()
	}
	// Close clears r.pulse and r.scouterHTTP while a request that outlived
	// the HTTP stage (close does not wait past its 2s Shutdown) may still be
	// here: read the atomic copy and the pointer under its lock. The copy
	// stays after the Pulse stage, so such a late request reports the
	// closed stream's final counters, not an absent Pulse.
	var p *pulseStream
	if r.shutdown != nil {
		p = r.shutdown.pulse.Load()
	}
	if p != nil {
		pulseStats := map[string]any{"dispatcher": p.dispatcher.Stats(), "sinkErrors": p.sinkErrors.Load()}
		if p.producer != nil {
			pulseStats["kafka"] = p.producer.Stats()
		}
		h.Pulse = pulseStats
	}
	h.PulseError = r.pulseError
	if servlets := r.servlets(); servlets != nil {
		h.Endpoints.ScouterHTTP = servlets.addr()
		h.ScouterHTTP = servlets.api.Stats()
		if servlets.server != nil {
			h.HTTPAdmission["scouterHttp"] = servlets.server.limits.stats()
		}
		if servlets.webGate != nil {
			h.HTTPAdmission["webapp"] = servlets.webGate.stats()
		}
	}
	h.Stats = r.stats.last.Load()
	h.Visitors = r.store.VisitorStats()
	h.IndexOpens = map[string]storage.IndexOpenStats{"store": r.store.IndexOpenStats(), "counters": r.counters.IndexOpenStats()}
	if locks := r.store.LockStats(); locks != nil {
		e := r.engine.LockStats()
		locks["engine"] = storage.LockStat{Acquired: e.Acquired, HeldMs: float64(e.Held) / float64(time.Millisecond), WaitedMs: float64(e.Waited) / float64(time.Millisecond)}
		h.Locks = locks
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(h)
}

// ScouterHTTPConfig enables the Java HttpServer servlets (net_http_server_*):
// /counter, /register and /telegraf. Config.ScouterHTTP carries it.
type ScouterHTTPConfig struct {
	// Listen is the net_http_port listener (Java 0.0.0.0:6180). Empty mounts
	// the servlets on the --http listener instead.
	Listen string
	// API configures the servlets. A nil API.Telegraf is loaded from
	// TelegrafProperties and the Telegraf XML path.
	API httpapi.Config
	// TelegrafProperties are the scouter.conf input_telegraf_* /
	// telegraf_object_deadtime_ms keys (deprecated measurement mapping).
	TelegrafProperties map[string]string
	// Webapp enables the scouter.webapp REST API under /scouter/* on the same
	// listener (Java net_http_api_enabled). The runtime fills CollectorAddr,
	// the login and Supported. nil keeps it off.
	Webapp *webapp.Config
	// admission is the go_http_* configuration the runtime fills (zero: the
	// defaults); API.MaxInFlight/QueueWait set by the caller win.
	admission httpAdmission
}

// scouterHTTP owns the servlet API, the optional webapp and, when Listen is
// set, its listener.
type scouterHTTP struct {
	api    *httpapi.API
	webapp *webapp.Server
	// webGate bounds the webapp requests served at once (http_admission.go).
	webGate *httpGate
	handler http.Handler
	server  *httpServer
}

// openScouterHTTP loads the Telegraf configuration when needed, builds the
// API and binds the separate listener when cfg.Listen is set. A Telegraf XML
// that cannot be applied is logged and the Java fallback configuration is
// used (Configure keeps running on JAXBException).
func openScouterHTTP(cfg ScouterHTTPConfig, engine telegraf.Acceptor, telegrafPath string) (*scouterHTTP, error) {
	if cfg.API.Telegraf == nil {
		tg, warnings, err := telegraf.Load(cfg.TelegrafProperties, telegrafPath, time.Now())
		for _, w := range warnings {
			log.Printf("telegraf config: %s", w)
		}
		if err != nil {
			log.Printf("telegraf config: %v (using the default TelegrafConfig)", err)
		}
		cfg.API.Telegraf = tg
	}
	admission := cfg.admission.resolved()
	if cfg.API.MaxInFlight == 0 {
		cfg.API.MaxInFlight = admission.servletInFlight
	}
	if cfg.API.QueueWait == 0 {
		cfg.API.QueueWait = admission.servletWait
	}
	api, err := httpapi.New(engine, cfg.API)
	if err != nil {
		return nil, err
	}
	s := &scouterHTTP{api: api}
	mux := http.NewServeMux()
	mountServlets(mux, api)
	if cfg.Webapp != nil {
		if s.webapp, err = webapp.New(*cfg.Webapp); err != nil {
			api.Close()
			return nil, err
		}
		// Jersey ServletContainer at /scouter/* (WebAppMain.java:126-152).
		s.webGate = newHTTPGate("webapp", admission.webappInFlight, admission.webappWait)
		mux.Handle("/scouter/", s.webGate.handler(s.webapp.Handler()))
	}
	s.handler = mux
	if cfg.Listen != "" {
		if s.server, err = listenHTTP(cfg.Listen, mux, cfg.admission); err != nil {
			s.closeAPI()
			return nil, err
		}
	}
	return s, nil
}

func (s *scouterHTTP) closeAPI() error {
	err := s.api.Close()
	if s.webapp != nil {
		err = errors.Join(err, s.webapp.Close())
	}
	return err
}

// addr is the separate listener address, or "" when mounted on --http.
func (s *scouterHTTP) addr() string {
	if s.server == nil {
		return ""
	}
	return s.server.addr
}

func (s *scouterHTTP) serve(ctx context.Context) {
	if s.server != nil {
		s.server.serve(ctx)
	}
}

// close stops the listener first so no request reaches a closed API, then
// stops the Telegraf registration goroutine and the request log. The --http
// listener that mounts the API must be closed before this.
func (s *scouterHTTP) close() error {
	var err error
	if s.server != nil {
		err = s.server.close()
	}
	return errors.Join(err, s.closeAPI())
}
