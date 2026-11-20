package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/snowvil/scouter-go/internal/account"
	"github.com/snowvil/scouter-go/internal/config"
	"github.com/snowvil/scouter-go/internal/counters"
	"github.com/snowvil/scouter-go/internal/geoip"
	"github.com/snowvil/scouter-go/internal/ingest"
	"github.com/snowvil/scouter-go/internal/logger"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/sqltable"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/telegraf"
	"github.com/snowvil/scouter-go/internal/transport"
	"github.com/snowvil/scouter-go/internal/webapp"
)

type Config struct {
	Directory, UDP, TCP, ReadyFile, Username, Password string
	// ServerID is the Java server_id reported at LOGIN (Client server node name).
	ServerID                      string
	TelegrafConfigurationPath     string
	LogDirectory, PluginDirectory string
	Location                      *time.Location
	CounterXML                    []byte
	MaxConnections                int // Client TCP connections
	// MaxAgentConnections and MaxHandshakes are the Agent and pre-magic TCP
	// budgets (transport defaults when 0).
	MaxAgentConnections, MaxHandshakes int
	// MaxObjects, MaxCounters and MaxCacheBytes size the ingest engine
	// (ingest defaults when 0).
	MaxObjects, MaxCounters, MaxCacheBytes int
	CompressXLog, CompressProfile          bool
	// SyncInterval periodically fsyncs data and index headers; zero disables it.
	// Java flushes dirty hash/time headers every 4s without fsync (FlushCtr).
	SyncInterval time.Duration
	// Retention enables AutoDeleteScheduler-compatible purge; nil disables it.
	Retention         *storage.RetentionConfig
	RetentionInterval time.Duration
	// Pulse enables the built-in Pulse Kafka stream; nil keeps it off.
	Pulse *PulseConfig
	// UDPWorkers/UDPQueue override the receiver's worker count and bounded
	// datagram queue (transport defaults 2/8192); UDPReadBuffer sets SO_RCVBUF
	// when > 0.
	UDPWorkers, UDPQueue, UDPReadBuffer int
	// UDPMaxDatagram is net_udp_packet_buffer_size (transport.UDPPacketBufferSize);
	// zero keeps the transport default 65507.
	UDPMaxDatagram int
	// IndexSizes are the startup *_mb hash index creation sizes (S4).
	IndexSizes storage.IndexSizesMB
	// Logger is the Java-compatible server/request log (L1), owned and
	// closed by the caller after Run returns. Nil disables the server log
	// file, the request log and the log_udp_*/log_test_rate output.
	Logger *logger.Logger
	// HTTP, when set, serves GET /health (JSON counters) and /debug/pprof on a
	// separate listener. It is opt-in and intended for localhost; it is not the
	// original webapp API.
	HTTP string
	// ScouterHTTP enables the Java HttpServer servlets (/counter, /register,
	// /telegraf). nil keeps them off (Java net_http_server_enabled=false).
	ScouterHTTP *ScouterHTTPConfig
	// StatsInterval logs one human-readable throughput/latency line per
	// interval (and serves the same figures in /health); zero disables it.
	StatsInterval time.Duration
	// LockStats turns on the measurement-only lock sums served as "locks" in
	// /health: the engine lock, the Store and tag gates and the writers'
	// shares of the Store gate. Two clock reads per acquisition.
	LockStats bool
	// ConfigFile is the scouter.conf store owned by the caller; it backs the
	// GET/LIST/SET_CONFIGURE_SERVER family. Nil keeps the flag-only rows.
	ConfigFile *config.Store
	// ObjectDeadTime is Java object_deadtime_ms; zero uses 8s.
	ObjectDeadTime time.Duration
	// ConfDir is Java Configure.CONF_DIR for counters.site.xml (CounterManager
	// custom document). Empty disables the custom counters store, which makes
	// SET_CONFIGURE_COUNTERS_SITE and DEFINE/EDIT_OBJECT_TYPE unsupported.
	ConfDir string
	// AccountDir enables Java AccountManager: account.xml/account_group.xml
	// under it back LOGIN, the group policy and the AccountService commands
	// (missing files are created from the Scouter defaults). Empty keeps the
	// single Username/Password credential.
	AccountDir string
	// LDAP is account_use_ldap=true (Authorize binds to the directory).
	LDAP *account.LDAPConfig
	// GeoIP overrides the GeoIP city database for the XLog path
	// (GISDataUtil); nil loads the geoip_data_city_file database.
	GeoIP ingest.GeoIPLookup
	// GeoIPFile fixes the GeoIP file path. Empty follows the live
	// geoip_data_city_file (Java default ./conf/GeoLiteCity.dat relative to
	// the working directory, like new File(path)); a missing file keeps
	// GeoIP off. geoip_enabled comes from the live settings.
	GeoIPFile string
}
type Runtime struct {
	config             Config
	startMaxProcs      int // GOMAXPROCS at Open, restored when go_maxprocs is removed
	store              *storage.Store
	counters           *storage.Counters
	engine             *ingest.Engine
	udp                *transport.UDPReceiver
	tcp                *transport.TCPServer
	pool               agentPool
	lock               *writerLock
	heartbeatCancel    context.CancelFunc
	heartbeatDone      chan struct{}
	heartbeatErrors    chan error
	errorEvents        chan transport.ErrorEvent
	droppedErrorEvents atomic.Uint64
	maintenanceErrors  atomic.Uint64
	purged             atomic.Uint64
	recoveries         atomic.Uint64
	pulse              *pulseStream
	pulseReport        *string
	pulseError         string
	http               *httpServer
	scouterHTTP        *scouterHTTP
	admission          httpAdmission // go_http_* at Open
	scouterHTTPMu      sync.Mutex    // guards scouterHTTP between Close, ReloadTelegraf and /health
	telegrafMu         sync.Mutex    // orders ReloadTelegraf calls
	queries            *service.Service
	countersSite       *counters.Store
	accounts           *account.Manager
	stats              *statsReporter
	geoipMu            sync.Mutex // orders ReloadGeoIP calls
	geoipPath          string
	live               *liveSettings
	// Live setting consumers (applyLive).
	retention       *storage.Retention
	sqlTables       *sqltable.Tables
	fileMetadata    *service.FileMetadata
	webapp          *webapp.Server
	noSiteOnce      sync.Once
	addTypeFailures atomic.Uint64
	addTypeLogAt    atomic.Int64
	shutdown        *shutdownState // teardown stage for main's shutdown budget (shutdown.go)
	// clientCommands are the Client commands in their handler now
	// (ShutdownReport).
	clientCommands atomic.Int64
}

// engineRef defers the engine lookup: the hooks resolve it after Open.
type engineRef struct{ r *Runtime }

func (e engineRef) Text(ctx context.Context, date, div string, hash int32) (string, bool, error) {
	return e.r.engine.Text(ctx, date, div, hash)
}

func storageDate(ms int64, loc *time.Location) string { return storage.JavaDate(ms, loc) }

type Endpoints struct {
	UDP  string `json:"udp"`
	TCP  string `json:"tcp"`
	HTTP string `json:"http,omitempty"`
	// ScouterHTTP is the Java-compatible servlet listener when it is separate
	// from HTTP; when the servlets are mounted on HTTP it stays empty.
	ScouterHTTP string `json:"scouterHttp,omitempty"`
}
type agentPool struct {
	timing agentTiming
	// register is RequestLogger.registerCmd, called before a command is
	// relayed to an agent session (AgentCall); set by Open before Run.
	register func(cmd string)
	mu       sync.Mutex
	agents   map[int32][]*agentSlot
	changed  chan struct{}
	// orphans bounds and counts Pack relays still reading after their
	// caller left (agent_pool_relay.go).
	orphans agentOrphans
	// budget bounds the bytes all relays may hold (agent_relay_budget.go).
	budget relayBudget
}
type agentSlot struct {
	conn *transport.AgentConn
	// life is the connection's handle context (agentSlot.lifetime).
	life context.Context
	done chan struct{}
	once sync.Once
	busy bool
	// lastWrite is TcpAgentWorker.lastWriteTime: the last command written
	// (a checked-out request or a KEEP_ALIVE), or the connection time.
	lastWrite time.Time
}

func (p *agentPool) handle(ctx context.Context, conn *transport.AgentConn) error {
	slot := &agentSlot{conn: conn, life: ctx, done: make(chan struct{}), lastWrite: time.Now()}
	p.mu.Lock()
	if len(p.agents[conn.ObjectHash]) >= 8 {
		p.mu.Unlock()
		return transport.ErrLimit
	}
	p.agents[conn.ObjectHash] = append(p.agents[conn.ObjectHash], slot)
	p.signalLocked()
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		list := p.agents[conn.ObjectHash]
		for i, s := range list {
			if s == slot {
				list = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(list) == 0 {
			delete(p.agents, conn.ObjectHash)
		} else {
			p.agents[conn.ObjectHash] = list
		}
		p.signalLocked()
		// The connection ends with this handler: a request still holding the
		// slot (an orphan relay included) fails at once on the closed socket
		// and releases it, so no relay goroutine outlives its connection.
		if slot.busy {
			_ = slot.conn.Close()
		}
		for slot.busy {
			changed := p.changed
			p.mu.Unlock()
			<-changed
			p.mu.Lock()
		}
		p.mu.Unlock()
	}()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-slot.done:
			return nil
		case <-ticker.C:
			if p.claimIdle(slot, time.Now()) {
				keepCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				_ = slot.conn.RequestPack(keepCtx, "KEEP_ALIVE", &protocol.MapPack{}, func(ctx context.Context, _ protocol.Pack) error { return ctx.Err() })
				cancel()
				p.release(slot)
			}
		}
	}
}
func (p *agentPool) request(ctx context.Context, hash int32, cmd string, param *protocol.MapPack) ([]protocol.Pack, error) {
	// The transport bounds the response bytes per command (agentPackLimits),
	// the relay budget their sum over all relays; this bounds the number of
	// Packs kept. The request is detached from ctx (agentPool.relay): when
	// ctx ends first it is read to the end as an orphan and its Packs are
	// dropped.
	limits := agentPackLimits(cmd)
	start := time.Now()
	permit, err := p.acquireRelay(ctx, hash, cmd, limits.TotalBytes, largeAgentPackCommands[cmd])
	if err != nil || permit == nil {
		return nil, err
	}
	slot, err := p.checkoutAfterBudget(ctx, hash, start)
	if err != nil || slot == nil {
		permit.release()
		return nil, err
	}
	if p.register != nil {
		p.register(cmd)
	}
	var out []protocol.Pack
	err = p.relay(ctx, slot, hash, cmd, permit, func(agentCtx context.Context, orphaned func() bool) error {
		err := slot.conn.RequestPackLimited(agentCtx, cmd, param, limits, func(ctx context.Context, pack protocol.Pack) error {
			if orphaned() {
				out = nil
				return ctx.Err()
			}
			if len(out) >= agentPackCount {
				return fmt.Errorf("%w: more than %d Packs", transport.ErrLimit, agentPackCount)
			}
			out = append(out, pack)
			return ctx.Err()
		})
		if err != nil {
			logRelayLimit(hash, cmd, limits, err)
			if errors.Is(err, transport.ErrUnsupported) {
				// An answer this server cannot decode (an unknown Pack or
				// Value type) is an Agent failure like Java's readPack
				// exception (session closed, AgentCall null), not a backend
				// without agent calls, which ErrUnsupported means to the
				// service's relays.
				err = fmt.Errorf("agent objHash=%d %s answer: %s", hash, cmd, err.Error())
			}
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
func Open(cfg Config) (r *Runtime, err error) {
	if cfg.Directory == "" || cfg.Location == nil {
		return nil, protocol.ErrInvalid
	}
	if cfg.LogDirectory == "" {
		cfg.LogDirectory = "./logs"
	}
	if cfg.TelegrafConfigurationPath == "" {
		cfg.TelegrafConfigurationPath = "./conf/scouter-telegraf.xml"
	}
	if cfg.PluginDirectory == "" {
		cfg.PluginDirectory = "./plugin"
	}
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 1024
	}
	if w := applyFDLimit(&cfg); w != "" {
		log.Print(w)
	}
	if cfg.SyncInterval < 0 || cfg.RetentionInterval < 0 {
		return nil, protocol.ErrInvalid
	}
	if cfg.RetentionInterval == 0 {
		cfg.RetentionInterval = time.Minute
	}
	if cfg.StatsInterval < 0 {
		return nil, protocol.ErrInvalid
	}
	r = &Runtime{config: cfg, errorEvents: make(chan transport.ErrorEvent, 64), pool: agentPool{agents: make(map[int32][]*agentSlot)}, stats: newStatsReporter(time.Now()), live: newLiveSettings(), startMaxProcs: runtime.GOMAXPROCS(0), shutdown: newShutdownState()}
	defer func(opened *Runtime) {
		if err != nil {
			opened.Close()
		}
	}(r)
	r.lock, err = acquireLock(cfg.Directory)
	if err != nil {
		return nil, err
	}
	r.startHeartbeat(time.Second)
	startup := initialSettings(cfg)
	r.store, err = storage.Open(storage.Config{Directory: cfg.Directory, Location: cfg.Location, CompressXLog: startup.CompressXLog, CompressProfile: startup.CompressProfile,
		TextDailyService: startup.TextDailyService, TextDailyAPI: startup.TextDailyAPI, TextDailyUA: startup.TextDailyUA,
		IndexSizes: cfg.IndexSizes, Log: r.storageLog(), OnRecover: r.reportRecovery, DeferVisitorFlush: true,
		XLogQueueSize: startup.XLogQueueSize, ProfileQueueSize: startup.ProfileQueueSize,
		TextCacheBytes: startup.Limits.TextCacheMB << 20})
	if err != nil {
		return nil, err
	}
	r.counters, err = storage.OpenCounters(cfg.Directory, cfg.Location)
	if err != nil {
		return nil, err
	}
	r.counters.OnRecover = r.reportRecovery
	r.counters.SetLog(r.storageLog())
	// AutoDeleteScheduler always exists; RunOnce is a no-op while
	// mgr_purge_enabled is off (applyLive, or the Config value below).
	r.retention = &storage.Retention{Store: r.store, Counters: r.counters}
	if cfg.ConfigFile == nil {
		var c storage.RetentionConfig
		if cfg.Retention != nil {
			c = *cfg.Retention
		}
		r.retention.Apply(cfg.Retention != nil, c)
	}
	// SqlTables (I6): TextCore hands every sql TextPack to it.
	if r.sqlTables, err = sqltable.New(sqltable.Options{Sink: sqlTextStore{r.store}, Source: sqlTextStore{r.store},
		Enabled:        func() bool { return r.liveNow().SQLTableParsing },
		LogFailEnabled: func() bool { return r.liveNow().LogSQLParsingFail },
		Log:            cfg.Logger.PrintlnIDRate}); err != nil {
		return nil, err
	}
	r.pool.register = cfg.Logger.RegisterRequestCommand
	if err = r.store.AttachCounters(r.counters); err != nil {
		return nil, err
	}
	masters, parseErr := masterCounters(cfg.CounterXML)
	if parseErr != nil {
		return nil, parseErr
	}
	types, parseErr := counterTypes(cfg.CounterXML)
	if parseErr != nil {
		return nil, parseErr
	}
	defaultFamilies := counters.NewEngine(0)
	if len(cfg.CounterXML) > 0 {
		if familyErr := defaultFamilies.Parse(cfg.CounterXML); familyErr != nil {
			// CounterEngine.parse failure leaves an empty engine in Java.
			log.Printf("counters.xml families: %v (CPU autodump family check disabled)", familyErr)
		}
	}
	initial := ingestSettings(startup)
	ingestConfig := ingest.Config{CounterTypes: types, Location: cfg.Location, AgentCall: r.pool.request, AgentBlobCall: r.pool.requestBlob, MasterCounters: masters, ObjectDeadTime: cfg.ObjectDeadTime,
		ObjectType: r.objectType(defaultFamilies), AddObjectTypeIfNotExist: r.addObjectTypeIfNotExist, GeoIP: cfg.GeoIP,
		SamplingGxidKeepCount: startup.SamplingGxidKeepCount, Settings: &initial, OnSQLText: r.sqlTables.Add,
		MaxObjects: cfg.MaxObjects, MaxCounters: cfg.MaxCounters, MaxCacheBytes: cfg.MaxCacheBytes}
	if cfg.Logger != nil {
		ingestConfig.PackLog = r.logPack
		ingestConfig.ObjectTypeLog = r.logObjectType
		ingestConfig.ProfileTestLog = r.logProfileTest
	}
	if cfg.Pulse != nil {
		// The dictionary lookup needs the engine, which needs the hooks: the
		// hooks resolve the stream lazily and are only invoked once Run starts.
		lookup := pulseLookup(engineRef{r}, cfg.Location)
		pc := *cfg.Pulse
		if pc.MaxXLogQueueBytes == 0 {
			pc.MaxXLogQueueBytes = int64(startup.Limits.PulseXLogQueueMB) << 20
		}
		if pc.MaxProfileQueueBytes == 0 {
			pc.MaxProfileQueueBytes = int64(startup.Limits.PulseProfileQueueMB) << 20
		}
		if err = r.openPulse(&pc, lookup, cfg.Location); err != nil {
			return nil, err
		}
		r.shutdown.pulse.Store(r.pulse)
		if r.pulse != nil {
			ingestConfig.OnXLog = func(p *protocol.XLogPack) { r.pulse.dispatcher.OfferXLog(p) }
			ingestConfig.OnProfile = func(p *protocol.ProfilePack) { r.pulse.dispatcher.OfferProfile(p) }
		}
	}
	r.engine, err = ingest.New(ingestConfig, r.store, r.counters)
	if err != nil {
		return nil, err
	}
	if cfg.LockStats {
		r.engine.TrackLocks()
		r.store.TrackLocks()
	}
	if cfg.GeoIP == nil {
		path := cfg.GeoIPFile
		if path == "" {
			path = startup.GeoIPDataCityFile
		}
		r.ReloadGeoIP(path)
	}
	if cfg.ConfDir != "" {
		// CounterManager: counters.site.xml under Configure.CONF_DIR, shared by
		// the Client configuration commands and the HTTP registrar.
		if r.countersSite, err = counters.Open(counters.StoreConfig{Dir: cfg.ConfDir, Default: cfg.CounterXML}); err != nil {
			return nil, err
		}
	}
	metadata, err := runtimeMetadataWith(cfg, cfg.ConfigFile, r.reloadConfiguration, r.countersSite)
	if err != nil {
		return nil, err
	}
	switch m := metadata.(type) {
	case *confMetadata:
		// The current snapshot's Live (pins applied) is what ApplySettings
		// receives on the same reload, so LIST shows the applied values.
		conf := cfg.ConfigFile
		m.live = func() map[string]string { return conf.Current().Live().Effective() }
		r.fileMetadata = m.FileMetadata
	case *service.FileMetadata:
		r.fileMetadata = m
	}
	var accounts service.AccountStore
	if cfg.AccountDir != "" {
		if r.accounts, err = account.Open(account.Config{Dir: cfg.AccountDir, LDAP: cfg.LDAP}); err != nil {
			return nil, err
		}
		accounts = r.accounts
	}
	queries, err := service.New(service.Config{Accounts: accounts, Metadata: metadata, ServerID: cfg.ServerID, Location: cfg.Location, Username: cfg.Username, Password: cfg.Password, CounterXML: cfg.CounterXML}, r.engine)
	if err != nil {
		return nil, err
	}
	r.queries = queries
	addr, err := net.ResolveUDPAddr("udp", cfg.UDP)
	if err != nil {
		return nil, err
	}
	udpConn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	onError := r.reportError
	// The receiver sets SO_RCVBUF (ReadBuffer > 0) and reports the requested
	// and effective sizes in UDPStats; <= 0 leaves the OS default.
	r.udp, err = transport.NewUDPReceiver(udpConn, transport.UDPConfig{Workers: cfg.UDPWorkers, QueueCapacity: cfg.UDPQueue, MaxDatagramBytes: cfg.UDPMaxDatagram, ReadBuffer: max(cfg.UDPReadBuffer, 0), CodecLimits: protocol.AgentPackLimits}, func(ctx context.Context, packet transport.Packet) error {
		start := time.Now()
		// The receiver decoded the pack for this call and hands it over.
		// The sender address only fills an ObjectPack without one.
		host := ""
		if _, ok := packet.Pack.(*protocol.ObjectPack); ok {
			host = packet.Sender.Addr().String()
		}
		err := r.engine.AcceptOwnedFrom(ctx, packet.Pack, host)
		r.stats.ingest.observe(time.Since(start))
		return err
	}, onError)
	if err != nil {
		udpConn.Close()
		return nil, err
	}
	if cfg.Logger != nil {
		r.udp.SetLogFunc(r.javaLog)
	}
	r.udp.SetObjectNameFunc(r.engine.ObjectNameCached)
	tcpListener, err := net.Listen("tcp", cfg.TCP)
	if err != nil {
		return nil, err
	}
	handlers := queries.Handlers()
	client := handlers.Client
	handlers.Client = func(ctx context.Context, request transport.ClientRequest, stream *transport.Stream) error {
		start := time.Now()
		// TcpServiceWorker: RequestLogger.add after the session check.
		cfg.Logger.RequestCommand(request.Command, request.Session, r.requestUser)
		// Agent relays of this command wait for the relay budget like
		// Java RequestQueue.get (agent_relay_budget.go).
		r.clientCommands.Add(1)
		defer r.clientCommands.Add(-1)
		err := client(withRelayCommand(ctx), request, stream)
		r.stats.client.observe(time.Since(start))
		return err
	}
	handlers.Agent = r.pool.handle
	handlers.Stack = func(ctx context.Context, meta transport.StackUpload, body io.Reader) error {
		return r.store.PutBatchArchiveContext(ctx, meta.StartTime, meta.ObjectName, meta.FileName, meta.Size, body)
	}
	// go_client_response_budget_mb (restart); 0 keeps the transport default.
	r.tcp, err = transport.NewTCPServer(tcpListener, transport.TCPConfig{MaxConnections: cfg.MaxConnections, MaxAgentConnections: cfg.MaxAgentConnections, MaxHandshakes: cfg.MaxHandshakes,
		MaxClientResponseBytes: int64(startup.Limits.ClientResponseBudgetMB) << 20}, handlers, onError)
	if err != nil {
		tcpListener.Close()
		return nil, err
	}
	if cfg.Logger != nil {
		r.tcp.SetLogFunc(r.javaLog)
	}
	endpoints := Endpoints{UDP: udpConn.LocalAddr().String(), TCP: tcpListener.Addr().String()}
	r.admission = httpAdmissionOf(startup.Limits)
	var mounted http.Handler
	if sc := cfg.ScouterHTTP; sc != nil {
		if sc.Listen == "" && cfg.HTTP == "" {
			return nil, errors.New("scouter http: Listen or --http is required")
		}
		opened := *sc
		opened.admission = r.admission
		if opened.API.LogDir == "" {
			opened.API.LogDir = cfg.LogDirectory
		}
		if opened.API.Location == nil {
			opened.API.Location = cfg.Location
		}
		if opened.API.Registrar == nil && r.countersSite != nil {
			// CounterManager registration for /register and Telegraf, with the
			// coalesced REFETCH_COUNTER_XML fan-out (RegisterHandler.notifyAllClients).
			opened.API.Registrar = counters.NewRegistrar(r.countersSite, func() {
				if r.queries != nil {
					r.queries.BroadcastRemoteControl("REFETCH_COUNTER_XML", nil)
				}
			})
		}
		if opened.Webapp != nil {
			// Java EmbeddedConfigureAdaptor logs in to 127.0.0.1:{tcp port} as
			// __local; without account files the explicit credential is used.
			w := *opened.Webapp
			w.CollectorAddr = webappCollectorAddr(tcpListener.Addr())
			if w.ListenAddr == "" {
				w.ListenAddr = sc.Listen
				if w.ListenAddr == "" {
					w.ListenAddr = cfg.HTTP
				}
			}
			if cfg.AccountDir != "" {
				w.Username, w.Password = "__local", ""
			} else {
				w.Username, w.Password = cfg.Username, cfg.Password
			}
			if w.Location == nil {
				w.Location = cfg.Location
			}
			supported := map[string]bool{}
			for _, c := range queries.SupportedCommands() {
				supported[c] = true
			}
			w.Supported = func(command string) bool { return supported[command] }
			opened.Webapp = &w
		}
		if r.scouterHTTP, err = openScouterHTTP(opened, r.engine, cfg.TelegrafConfigurationPath); err != nil {
			return nil, err
		}
		r.webapp = r.scouterHTTP.webapp
		endpoints.ScouterHTTP = r.scouterHTTP.addr()
		if sc.Listen == "" {
			mounted = r.scouterHTTP.handler
		}
	}
	if cfg.HTTP != "" {
		if r.http, err = newHTTPServerWithAPI(cfg.HTTP, r, mounted); err != nil {
			return nil, err
		}
		endpoints.HTTP = r.http.addr
	}
	if cfg.ReadyFile != "" {
		b, e := json.Marshal(endpoints)
		if e != nil {
			return nil, e
		}
		if e = os.WriteFile(cfg.ReadyFile, b, 0600); e != nil {
			return nil, e
		}
	}
	// Every consumer exists: register the live setters and apply the
	// startup settings once before Run starts the listeners.
	r.wireSettings()
	r.ApplySettings(startup)
	log.Printf("Scouter Go listening UDP=%s TCP=%s timezone=%s", endpoints.UDP, endpoints.TCP, cfg.Location)
	return r, nil
}

// storageLog is the storage Java-id warning sink (S152/S161):
// Logger.println(id, sec, msg). The Store and Counters warners each limit
// their own calls; the logger applies the Java process-wide per-id interval
// across both (and every other caller of the same id) and
// mgr_log_ignore_ids.
func (r *Runtime) storageLog() storage.LogFunc {
	if r.config.Logger == nil {
		return nil
	}
	lg := r.config.Logger
	return func(id string, sec int, message string) { lg.PrintlnIDRate(id, sec, message) }
}

// reportRecovery logs each automatic crash repair; recoveries are rare and
// must stay visible rather than silently absorbed.
func (r *Runtime) reportRecovery(e storage.RecoveryEvent) {
	r.recoveries.Add(1)
	log.Printf("storage recovery %s %s: %s", e.Kind, e.Path, e.Detail)
}

// Error callbacks never wait for stderr or allocate another goroutine.
func (r *Runtime) reportError(event transport.ErrorEvent) {
	select {
	case r.errorEvents <- event:
	default:
		r.droppedErrorEvents.Add(1)
	}
}
func (r *Runtime) startHeartbeat(period time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	r.heartbeatCancel = cancel
	r.heartbeatDone = make(chan struct{})
	r.heartbeatErrors = make(chan error, 1)
	go func() {
		defer close(r.heartbeatDone)
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.lock.touch(); err != nil {
					r.heartbeatErrors <- err
					return
				}
			}
		}
	}()
}
func (r *Runtime) Run(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	results := make(chan error, 2)
	go func() { results <- r.udp.Serve(ctx) }()
	go func() { results <- r.tcp.Serve(ctx) }()
	if r.http != nil {
		r.http.serve(ctx)
	}
	if r.scouterHTTP != nil {
		r.scouterHTTP.serve(ctx)
	}
	// Sampling may wait for ingest/disk. It must never delay the writer heartbeat.
	var sampling sync.WaitGroup
	sampling.Add(3)
	if r.accounts != nil {
		// Java AccountManager daemon: account files are re-read on change.
		sampling.Add(1)
		go func() {
			defer sampling.Done()
			if err := r.accounts.Run(ctx); err != nil {
				log.Printf("account reload: %v", err)
			}
		}()
	}
	if r.config.StatsInterval > 0 {
		sampling.Add(1)
		go func() {
			defer sampling.Done()
			ticker := time.NewTicker(r.config.StatsInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case now := <-ticker.C:
					log.Print(r.statsTick(now).String())
				}
			}
		}()
	}
	go func() {
		defer sampling.Done()
		r.maintain(ctx)
	}()
	// Tag counts own a goroutine: the inbox is applied every 100ms (at high
	// rates it fills well within a second, and a full inbox drops), and the
	// tag record flush (5s) and top100 rebuild (5min), marked due by the
	// engine tick, run here too, so a long tag pass delays neither the visitor
	// flush, sync and purge of maintain nor is delayed by them. A pass in
	// progress drains the inbox in its holds.
	sampling.Add(1)
	go func() {
		defer sampling.Done()
		tick := time.NewTicker(tagDrainInterval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			case <-r.store.TagDrainSignal(): // the inbox filled before the tick
			}
			if err := r.store.FlushTagCountsIfDue(ctx); err != nil && !errors.Is(err, context.Canceled) {
				r.maintenanceErrors.Add(1)
				log.Printf("tag count flush: %v", err)
			}
		}
	}()
	// Kube seq pushes and CPU thread-dump triggers queued by ingest.
	sampling.Add(1)
	go func() {
		defer sampling.Done()
		// Queued server calls: their own relay budget wait.
		r.engine.RunAgentCalls(withServerRelay(ctx))
	}()
	// Live settings owner: work with I/O after a scouter.conf change.
	sampling.Add(1)
	go func() {
		defer sampling.Done()
		r.runSettingsOwner(ctx)
	}()
	// Java TextCacheReset daemon: agents resend their text dictionary.
	sampling.Add(1)
	go func() {
		defer sampling.Done()
		r.newTextCacheResetter().run(ctx, textCacheResetDelay, textCacheResetInterval)
	}()
	go func() {
		defer sampling.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				select {
				case event := <-r.errorEvents:
					message := event.Err.Error()
					if len(message) > 256 {
						message = message[:256]
					}
					log.Printf("%s: %q", event.Operation, message)
				default:
				}
				if r.pulse != nil {
					r.pulse.logEvent()
				}
			}
		}
	}()
	go func() {
		defer sampling.Done()
		timer := time.NewTicker(time.Second)
		defer timer.Stop()
		next := ((time.Now().UnixMilli() / 300000) + 1) * 300000
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-timer.C:
				if err := r.engine.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("ingest tick: %v", err)
				}
				if now.UnixMilli() >= next {
					if err := r.engine.Sample5M(ctx); err != nil && !errors.Is(err, context.Canceled) {
						log.Printf("5m sampling: %v", err)
					}
					next = ((now.UnixMilli() / 300000) + 1) * 300000
				}
			}
		}
	}()
	var result error
	finished := 0
	stopping := ctx.Done()
	for finished < 2 {
		select {
		case <-stopping:
			stopping = nil
			r.beginShutdown()
			r.enterStage(stageListeners)
		case err := <-results:
			finished++
			if err != nil && !errors.Is(err, context.Canceled) {
				result = errors.Join(result, err)
			}
			cancel()
		case err := <-r.heartbeatErrors:
			result = errors.Join(result, err)
			cancel()
		}
	}
	r.beginShutdown()
	r.enterStage(stageBackground)
	sampling.Wait()
	r.enterStage(stageFinalStats)
	log.Printf("ingest=%+v udp=%+v tcp=%+v tagcount=%+v xlogQueue=%+v profileQueue=%+v textWriteErrors=%d textDropped=%d counterNamesLimited=%d deferredErrorLogs=%d droppedErrorLogs=%d maintenanceErrors=%d purged=%d recoveries=%d agentOrphans=%+v agentRelayBudget=%+v visitors=%+v ingestWriters=%+v", r.engine.Stats(), r.udp.Stats(), r.tcp.Stats(), r.store.TagCountStats(), r.store.XLogQueueStats(), r.store.ProfileQueueStats(), r.store.TextWriteErrors(), r.store.TextDropped(), r.counters.NamesLimited(), len(r.errorEvents), r.droppedErrorEvents.Load(), r.maintenanceErrors.Load(), r.purged.Load(), r.recoveries.Load(), r.pool.orphans.snapshot(), r.pool.budget.snapshot(), r.store.VisitorStats(), r.engine.WriterStats())
	err := r.Close()
	if r.pulseReport != nil {
		log.Print(*r.pulseReport)
	}
	return errors.Join(result, err)
}

// tagDrainInterval is how often the tag goroutine applies the inbox and
// checks for a due flush or rebuild.
const tagDrainInterval = 100 * time.Millisecond

// maintain runs periodic durability sync, retention and the visitor flush
// outside the ingest path. They hold the storage gates; failures are logged
// and retried on the next tick. Tag counts have their own goroutine.

func (r *Runtime) maintain(ctx context.Context) {
	// Visitor snapshots (~700KiB each, two per object) are written here, not
	// by the UDP worker whose Pack crossed the 10s tick.
	visitorTick := time.NewTicker(time.Second)
	defer visitorTick.Stop()
	var syncTick <-chan time.Time
	if r.config.SyncInterval > 0 {
		t := time.NewTicker(r.config.SyncInterval)
		defer t.Stop()
		syncTick = t.C
	}
	// AutoDeleteScheduler: one cycle at startup, then every interval; each
	// cycle reads the live mgr_purge_enabled (RunOnce is a no-op when off).
	purgeTicker := time.NewTicker(r.config.RetentionInterval)
	defer purgeTicker.Stop()
	r.purge(ctx, r.retention)
	for {
		select {
		case <-ctx.Done():
			return
		case <-syncTick:
			// The records ingest queued before the sync reach the files
			// first (bounded: a stalled writer leaves them to the next sync).
			r.flushIngest(ctx)
			if err := errors.Join(r.store.SyncData(), r.counters.Sync()); err != nil {
				r.maintenanceErrors.Add(1)
				log.Printf("periodic sync: %v", err)
			}
		case <-purgeTicker.C:
			r.purge(ctx, r.retention)
		case <-visitorTick.C:
			if err := r.store.FlushVisitorsIfDue(ctx); err != nil && !errors.Is(err, context.Canceled) {
				r.maintenanceErrors.Add(1)
				log.Printf("visitor flush: %v", err)
			}
		}
	}
}

// ingestFlushWait bounds the wait for the ingest record writers before a
// periodic sync or purge (Engine.Flush).
const ingestFlushWait = 5 * time.Second

// flushIngest waits up to ingestFlushWait for the records ingest queued so
// far to be written. A timeout is not an error: the records stay queued and
// the writers count their own failures.
func (r *Runtime) flushIngest(ctx context.Context) {
	fctx, cancel := context.WithTimeout(ctx, ingestFlushWait)
	defer cancel()
	if err := r.engine.Flush(fctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		log.Printf("ingest flush: %v", err)
	}
}

func (r *Runtime) purge(ctx context.Context, retention *storage.Retention) {
	// Records queued for a purged day are written before it, as when ingest
	// wrote synchronously (Engine.DeleteDatabasePaths does the same).
	r.flushIngest(ctx)
	actions, err := retention.RunOnce(ctx, time.Now())
	for _, a := range actions {
		if a.Err == "" {
			r.purged.Add(1)
			log.Printf("purge %s %s (%s)", a.Date, a.Mode, a.Reason)
		}
		if a.Gates != "" {
			log.Printf("purge %s %s gates: %s", a.Date, a.Mode, a.Gates)
		}
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		r.maintenanceErrors.Add(1)
		log.Printf("purge failed: %v", err)
	}
}
func (r *Runtime) Close() error {
	var err error
	r.enterStage(stageHTTP)
	// r.http stays set (close is idempotent): a /health request that
	// outlived the 2s Shutdown still reads it.
	if r.http != nil {
		err = errors.Join(err, r.http.close())
	}
	// After every listener that can reach the API, before the engine. The
	// pointer is detached under the lock and closed outside it so a reload
	// waiting on the lock cannot hold up the HTTP shutdown.
	r.scouterHTTPMu.Lock()
	servlets := r.scouterHTTP
	r.scouterHTTP = nil
	r.scouterHTTPMu.Unlock()
	if servlets != nil {
		err = errors.Join(err, servlets.close())
	}
	r.enterStage(stageCloseNet)
	if r.udp != nil {
		err = errors.Join(err, r.udp.Close())
	}
	if r.tcp != nil {
		err = errors.Join(err, r.tcp.Close())
	}
	r.enterStage(stageIngest)
	if r.engine != nil {
		// Drains the ingest record writers (bounded) before the counters and
		// the store below are closed.
		r.engine.Close()
		stats := r.engine.WriterStats()
		log.Printf("ingest writers closed: %+v", stats)
		// A writer still in a write may land it after the files are
		// closed: the shutdown is not clean (storage writes go through the
		// gates Close takes, so they then fail with ErrClosed).
		if left := writersLeft(stats); left > 0 && r.store != nil {
			r.store.SkipCleanMarker()
			log.Printf("WARNING: ingest writers still hold %d records: the clean-shutdown marker is not written (next start validates the indexes from the crash checkpoints)", left)
		}
	}
	r.enterStage(stageSQLTable)
	if r.sqlTables != nil {
		// After the engine: no more Add. The worker finishes the current
		// item; the rest is counted as RemainingAtClose.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = errors.Join(err, r.sqlTables.Close(ctx))
		cancel()
		log.Printf("sqltable=%+v", r.sqlTables.Stats())
		r.sqlTables = nil
	}
	r.enterStage(stagePulse)
	if r.pulse != nil {
		// After the engine: no more hooks fire; drain queues, then the producer.
		stats, kstats, abandoned, perr := r.pulse.close()
		err = errors.Join(err, perr)
		report := fmt.Sprintf("pulse=%+v kafka=%+v kafkaAbandoned=%d pulseSinkErrors=%d pulseDroppedEventLogs=%d", stats, kstats, abandoned, r.pulse.sinkErrors.Load(), r.pulse.dropped.Load())
		r.pulseReport = &report
		r.pulse = nil
	}
	r.enterStage(stageCounters)
	if r.counters != nil {
		err = errors.Join(err, r.counters.Close())
	}
	r.enterStage(stageStore)
	if r.store != nil {
		err = errors.Join(err, r.store.Close())
		// Close wrote the queued XLogs and profiles after the stats line.
		log.Printf("storage writers closed: xlogQueue=%+v profileQueue=%+v", r.store.XLogQueueStats(), r.store.ProfileQueueStats())
	}
	r.enterStage(stageWriterLock)
	if r.heartbeatCancel != nil {
		r.heartbeatCancel()
		<-r.heartbeatDone
	}
	if r.lock != nil {
		err = errors.Join(err, r.lock.Close())
	}
	r.enterStage(stageClosed)
	return err
}

// flock prevents two Go writers; lock.dat heartbeat also participates in the
// Java DBCtr stale-lock convention. Neither server may share a live DB writer.
type writerLock struct {
	mu        sync.Mutex
	file      *os.File
	heartbeat string
	identity  os.FileInfo
	closed    bool
}

func acquireLock(root string) (*writerLock, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(root, ".go-writer.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("DB already has a Go writer: %w", err)
	}
	release := func(e error) (*writerLock, error) {
		return nil, errors.Join(e, syscall.Flock(int(f.Fd()), syscall.LOCK_UN), f.Close())
	}
	heartbeat := filepath.Join(root, "lock.dat")
	info, statErr := os.Stat(heartbeat)
	if statErr == nil {
		if time.Since(info.ModTime()) < 5*time.Second && !goHeartbeat(heartbeat) {
			return release(errors.New("DB has an active lock.dat heartbeat (a Java server uses it, or one stopped less than 5s ago)"))
		}
		// A stale lock.dat (Java DBCtr.createLock), or a fresh one a Go writer
		// left: holding .go-writer.lock proves that writer is gone, so a
		// restart right after a crash need not wait the 5s out. Java never
		// writes lock.dat content, so the tag cannot be a live Java server's.
		if err = os.Remove(heartbeat); err != nil && !os.IsNotExist(err) {
			return release(err)
		}
	} else if !os.IsNotExist(statErr) {
		return release(statErr)
	}
	// Java File.createNewFile and Go O_EXCL compete for the same atomic claim.
	claimed, err := os.OpenFile(heartbeat, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return release(fmt.Errorf("DB writer claim failed: %w", err))
	}
	_, err = claimed.WriteString(goHeartbeatTag)
	if err != nil {
		claimed.Close()
		os.Remove(heartbeat)
		return release(err)
	}
	identity, err := claimed.Stat()
	closeErr := claimed.Close()
	if err != nil || closeErr != nil {
		os.Remove(heartbeat)
		return release(errors.Join(err, closeErr))
	}
	return &writerLock{file: f, heartbeat: heartbeat, identity: identity}, nil
}

// goHeartbeatTag is the content a Go writer gives lock.dat. Java creates it
// empty (File.createNewFile) and only reads its mtime.
const goHeartbeatTag = "scouter-go writer\n"

func goHeartbeat(path string) bool {
	b, err := os.ReadFile(path)
	return err == nil && string(b) == goHeartbeatTag
}

func (l *writerLock) touch() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return os.ErrClosed
	}
	info, err := os.Stat(l.heartbeat)
	if err != nil {
		return err
	}
	if !os.SameFile(info, l.identity) {
		return errors.New("DB writer heartbeat was replaced")
	}
	now := time.Now()
	return os.Chtimes(l.heartbeat, now, now)
}
func (l *writerLock) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var remove error
	info, err := os.Stat(l.heartbeat)
	if err == nil && os.SameFile(info, l.identity) {
		remove = os.Remove(l.heartbeat)
	} else if err != nil && !os.IsNotExist(err) {
		remove = err
	}
	return errors.Join(remove, syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN), l.file.Close())
}

var _ io.Closer = (*Runtime)(nil)

// ReloadTelegraf re-applies the Telegraf input configuration from the
// scouter.conf deprecated keys and the current XML file, the way Configure
// re-reads scouter-telegraf.xml on every reload. It is a no-op without the
// Java HTTP servlets; an unreadable XML keeps the previous configuration.
func (r *Runtime) ReloadTelegraf(properties map[string]string) {
	// telegrafMu orders the reloads (config watcher, Client
	// RELOAD_CONFIGURATION) so an older XML read never lands last; the file
	// is read outside scouterHTTPMu, which Close and /health also take.
	r.telegrafMu.Lock()
	defer r.telegrafMu.Unlock()
	if r.servlets() == nil {
		return
	}
	tg, warnings, err := telegraf.Load(properties, r.config.TelegrafConfigurationPath, time.Now())
	for _, w := range warnings {
		log.Printf("telegraf config: %s", w)
	}
	if err != nil {
		log.Printf("telegraf config: reload failed, keeping the previous configuration: %v", err)
		return
	}
	r.scouterHTTPMu.Lock()
	defer r.scouterHTTPMu.Unlock()
	if r.scouterHTTP == nil {
		return // closed while the file was read
	}
	r.scouterHTTP.api.SetTelegrafConfig(tg)
	log.Printf("telegraf config: reloaded (enabled=%v measurements=%d)", tg.Enabled, len(tg.Measurements))
}

// servlets is r.scouterHTTP read under its lock (nil after Close).
func (r *Runtime) servlets() *scouterHTTP {
	r.scouterHTTPMu.Lock()
	defer r.scouterHTTPMu.Unlock()
	return r.scouterHTTP
}

// reloadConfiguration is the FileMetadataConfig.Reload hook: after a
// SET_CONFIGURE_* write the matching Java side effect runs. "server" is
// Configure.reload(true) (already done by ServerConfigFile), "telegraf"
// re-applies the Telegraf mapping, and "counters-site" is
// RegisterHandler.notifyAllClients: REFETCH_COUNTER_XML queued for every
// Client session so it re-reads GET_XML_COUNTER.
func (r *Runtime) reloadConfiguration(ctx context.Context, kind string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch kind {
	case "telegraf":
		var props map[string]string
		if r.config.ConfigFile != nil {
			props = r.config.ConfigFile.Current().TelegrafDeprecated()
		}
		r.ReloadTelegraf(props)
	case "counters-site":
		if r.queries != nil {
			r.queries.BroadcastRemoteControl("REFETCH_COUNTER_XML", nil)
		}
	}
	return nil
}

// webappCollectorAddr is the address the embedded webapp dials and hashes
// into serverId. Java EmbeddedConfigureAdaptor always uses
// "127.0.0.1" + net_tcp_listen_port, so an unspecified or loopback listener
// maps to 127.0.0.1:<port> (same serverId as Java); a listener bound to one
// non-loopback address keeps that address, and __local then fails because the
// peer is not loopback, which is logged at startup.
func webappCollectorAddr(addr net.Addr) string {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok {
		return addr.String()
	}
	if tcp.IP == nil || tcp.IP.IsUnspecified() || tcp.IP.IsLoopback() {
		if tcp.IP != nil && tcp.IP.IsLoopback() && !tcp.IP.Equal(net.IPv4(127, 0, 0, 1)) {
			log.Printf("WARNING: webapp dials 127.0.0.1:%d like Java while the collector listens on %s; the dial fails unless 127.0.0.1 also answers", tcp.Port, addr)
		}
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(tcp.Port))
	}
	log.Printf("WARNING: webapp dials the collector at %s, which is not loopback; the __local login requires a loopback peer", addr)
	return addr.String()
}

// ReloadGeoIP is GISDataUtil.load for a geoip_data_city_file value: at
// startup, and later only when the value differs from the last one (the
// ConfObserver comparison; the path is remembered even when loading fails).
// A missing file logs S145 and a broken one logs the error; both keep the
// previous database (none at startup = GeoIP off). A loaded file replaces the
// database and clears the IP->Location cache (ingest SetGeoIP).
func (r *Runtime) ReloadGeoIP(path string) {
	r.geoipMu.Lock()
	defer r.geoipMu.Unlock()
	if r.geoipPath != "" && r.geoipPath == path {
		return
	}
	r.geoipPath = path
	shown := path
	if abs, err := filepath.Abs(path); err == nil {
		shown = abs
		if real, err := filepath.EvalSymlinks(abs); err == nil {
			shown = real // File.getCanonicalPath
		}
	}
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			log.Printf("S145 GeoIP db file is not readable : %s", shown)
		} else {
			log.Printf("GeoIP db file %s: %v (previous database kept)", shown, err)
		}
		return
	}
	db, err := geoip.Open(path)
	if err != nil {
		log.Printf("GeoIP db file %s: %v (previous database kept)", shown, err)
		return
	}
	r.engine.SetGeoIP(db)
	log.Printf("S146 GeoIP db file is loaded : %s", shown)
}
