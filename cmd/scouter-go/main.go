package main

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/account"
	"github.com/snowvil/scouter-go/internal/config"
	"github.com/snowvil/scouter-go/internal/httpapi"
	"github.com/snowvil/scouter-go/internal/ingest"
	"github.com/snowvil/scouter-go/internal/logger"
	"github.com/snowvil/scouter-go/internal/pulse"
	"github.com/snowvil/scouter-go/internal/server"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/telegraf"
	"github.com/snowvil/scouter-go/internal/transport"
	"github.com/snowvil/scouter-go/internal/webapp"
)

//go:embed counters.xml
var counterXML []byte

func main() {
	cfg := server.Config{CounterXML: counterXML}
	confPath := flag.String("conf", config.DefaultConfFile, "Java scouter.conf; flags override it, a missing default file means flags only, empty disables it")
	flag.StringVar(&cfg.TelegrafConfigurationPath, "telegraf-config", "./conf/scouter-telegraf.xml", "scouter-telegraf.xml path: Telegraf input settings, read and saved by the Client configuration commands and re-read with scouter.conf")
	var timezone string
	flag.StringVar(&cfg.Directory, "db", "./database", "Scouter file database directory")
	flag.StringVar(&cfg.UDP, "udp", "127.0.0.1:6100", "UDP listen address")
	flag.StringVar(&cfg.TCP, "tcp", "127.0.0.1:6100", "TCP listen address")
	flag.StringVar(&cfg.LogDirectory, "log-dir", "./logs", "Java log_dir: server-yyyyMMdd.log and request-yyyyMMdd.log are written here and read by the Client server log queries")
	flag.StringVar(&cfg.PluginDirectory, "plugin-dir", "./plugin", "Java plugin_dir: .alert/.conf files read and saved by the Client alert script commands (never executed)")
	flag.StringVar(&cfg.ReadyFile, "ready-file", "", "write selected listen addresses as JSON")
	flag.StringVar(&cfg.Username, "user", "admin", "single-account mode login, used instead of the account.xml accounts when --user or --password is given without --account-dir, or when --conf is empty and --account-dir is not given")
	flag.StringVar(&cfg.Password, "password", "admin", "single-account mode password (see --user)")
	flag.StringVar(&cfg.ServerID, "server-id", "", "Java server_id reported at LOGIN; default is the host name")
	flag.StringVar(&timezone, "timezone", "Asia/Seoul", "Java storage timezone")
	flag.IntVar(&cfg.MaxConnections, "max-connections", 1024, "maximum Client TCP connections (sessions and service-pool waiters); Agents have their own budget")
	flag.IntVar(&cfg.MaxAgentConnections, "max-agent-connections", transport.DefaultMaxAgentConnections, "maximum Agent TCP connections (TCP_AGENT, TCP_AGENT_V2, TCP_AGENT_REQ)")
	flag.IntVar(&cfg.MaxHandshakes, "max-handshakes", transport.DefaultMaxHandshakes, "maximum TCP sockets still reading their 4-byte magic")
	flag.IntVar(&cfg.MaxObjects, "max-objects", ingest.DefaultMaxObjects, "maximum objects (Agents) kept in memory; at the limit the longest-inactive one is evicted from memory")
	flag.IntVar(&cfg.MaxCounters, "max-counters", ingest.DefaultMaxCounters, "maximum realtime counter keys kept in memory")
	engineCacheMB := flag.Int("engine-cache-mb", ingest.DefaultMaxCacheBytes>>20, "ingest engine cache budget in MiB for the realtime caches (counter, XLog ring, alert, status, group, delayed sampling); past it XLogs, counters, alerts and status are still stored and only the cache update is skipped and counted, delayed sampling children get the Java recovery rule early, and object/kube state is checked against the budget on its own bytes")
	flag.BoolVar(&cfg.CompressXLog, "compress-xlog", false, "GZipStore for new XLog files")
	flag.BoolVar(&cfg.CompressProfile, "compress-profile", false, "GZipStore for new profile files")
	flag.DurationVar(&cfg.SyncInterval, "sync-interval", 4*time.Second, "periodic fsync of data and index headers; 0 disables")
	flag.IntVar(&cfg.UDPWorkers, "udp-workers", 0, "UDP decode/ingest workers (default 2; _net_udp_worker_thread_count when set)")
	flag.IntVar(&cfg.UDPQueue, "udp-queue", 0, "bounded UDP datagram queue (also bounded to 32MiB of payload); full queue rejects and counts (default 8192)")
	// Java applies net_udp_so_rcvbuf_size (4 MiB) even without the key
	// (Configure.java default_so_rcvbuf, DataUdpServer.open); the OS default
	// (about 200-800 KiB) overflows on short bursts. The OS may cap it
	// (Linux net.core.rmem_max), as it caps Java's.
	flag.IntVar(&cfg.UDPReadBuffer, "udp-read-buffer", 4<<20, "SO_RCVBUF bytes for the UDP socket (Java net_udp_so_rcvbuf_size; 0 = OS default; the kernel may cap it, see udp in /health)")
	flag.DurationVar(&cfg.StatsInterval, "stats-interval", 10*time.Second, "log one human-readable throughput/latency line per interval; 0 disables")
	flag.StringVar(&cfg.HTTP, "http", "", "opt-in GET /health and /debug/pprof listener, e.g. 127.0.0.1:6180 (off by default)")
	scouterHTTP := flag.String("scouter-http", "", "Java HttpServer servlets (/counter, /register, /telegraf) listener, e.g. 0.0.0.0:6180; scouter.conf net_http_server_enabled/net_http_port set it too")
	scouterHTTPCheck := flag.Bool("scouter-http-check-allow-ips", false, "Go-only: restrict the servlet listener to the allow list (the Java servlets accept every address); --scouter-http-allow-ips turns it on")
	scouterHTTPAllow := flag.String("scouter-http-allow-ips", "", "comma-separated allow list used with --scouter-http-check-allow-ips (Java net_http_api_allow_ips; default localhost only, * allows all)")
	scouterHTTPHeader := flag.String("scouter-http-auth-ip-header", "", "header that carries the client IP for the allow list (Java net_http_api_auth_ip_header_key)")
	scouterHTTPGzip := flag.Bool("scouter-http-gzip", true, "gzip servlet responses >= 1024 bytes (Java net_http_api_gzip_enabled)")
	logKeepDays := flag.Int("log-keep-days", 31, "HTTP request log retention days (Java log_keep_days)")
	webappFlag := flag.Bool("webapp", false, "serve the scouter.webapp REST API (/scouter/*) on the servlet listener (Java net_http_api_enabled)")
	accountDir := flag.String("account-dir", "", "directory of account.xml/account_group.xml (Java conf dir; default: the --conf directory; with --user/--password and no --account-dir the files are not used)")
	pulseConf := flag.String("pulse-conf", "", "enable the built-in Pulse Kafka stream with this Java-style properties file (ext_plugin_pulse_* keys)")
	pulseAcks := flag.Int("pulse-kafka-acks", 0, "Pulse producer acks: 0 (plugin contract), 1 or -1")
	shutdownTimeout := flag.Duration("shutdown-timeout", defaultShutdownTimeout, "Go-only: budget for the whole graceful shutdown after SIGTERM/SIGINT/SIGHUP, exit file deletion or console quit; past it the stalled stage and the queued records are logged and the process exits 3 (the clean-shutdown marker exists only if the store had closed: marker=written in that line). 0 disables it; SIGINT/SIGTERM again more than 1s after the first signal always exits at once")
	profileDir := flag.String("profile-dir", "", "measurement only: write cpu/block/mutex/heap pprof files here at shutdown")
	flag.BoolVar(&cfg.LockStats, "lock-stats", false, "measurement only: serve engine lock / Store gate / tag gate held and waited sums as \"locks\" in /health")
	purge := flag.Bool("purge", false, "enable AutoDeleteScheduler-compatible retention like mgr_purge_enabled=true (off unless this or the key is set; Java defaults to on)")
	flag.DurationVar(&cfg.RetentionInterval, "purge-interval", time.Minute, "retention check interval")
	purgeDays := make(map[string]*int)
	for _, key := range storage.JavaRetentionKeys {
		purgeDays[key] = flag.Int(strings.ReplaceAll(key, "_", "-"), 0, "Java "+key+"; unset uses the Java derivation")
	}
	mode := registerModeFlags()
	flag.Parse()
	if flag.NArg() != 0 {
		log.Fatal("unexpected positional arguments")
	}
	explicit := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	if *shutdownTimeout < 0 {
		log.Fatal("--shutdown-timeout must not be negative")
	}
	if *engineCacheMB < 1 {
		log.Fatal("--engine-cache-mb must be at least 1")
	}
	cfg.MaxCacheBytes = *engineCacheMB << 20
	flagPurge := make(map[string]int)
	intervalSet := explicit["purge-interval"]
	for key, v := range purgeDays {
		if explicit[strings.ReplaceAll(key, "_", "-")] {
			flagPurge[key] = *v
		}
	}
	var store *config.Store
	var fromConf confResult
	var live atomic.Pointer[server.Runtime]
	if *confPath != "" {
		var err error
		onChange := func(change config.Change) {
			r := live.Load()
			if r == nil {
				return
			}
			// F0: live keys (config.LiveKeys) are applied by the runtime.
			r.ApplySettings(store.Current().Live())
			if change.Telegraf || telegrafKeys(change.Applied) {
				r.ReloadTelegraf(store.Current().TelegrafDeprecated())
			}
		}
		if store, err = openConf(*confPath, explicit["conf"], cfg.TelegrafConfigurationPath, flagPins(explicit), onChange); err != nil {
			log.Fatal(err)
		}
		if store != nil {
			cfg.ConfigFile = store
			fromConf = applyConf(&cfg, store.Current(), explicit)
			for _, note := range fromConf.notes {
				log.Print(note)
			}
		}
	}
	if cfg.ServerID == "" {
		// Java Configure.server_id defaults to SysJMX.getHostName().
		if host, err := os.Hostname(); err == nil {
			cfg.ServerID = host
		}
	}
	enabled := *purge || (!explicit["purge"] && fromConf.purgeEnabled)
	if (len(flagPurge) > 0 || intervalSet) && !enabled {
		log.Fatal("--mgr-purge-* and --purge-interval options require --purge or mgr_purge_enabled=true")
	}
	if enabled {
		set := make(map[string]int)
		for k, v := range fromConf.purge {
			set[k] = v
		}
		for k, v := range flagPurge {
			set[k] = v
		}
		retention := storage.JavaRetentionConfig(set)
		cfg.Retention = &retention
		log.Printf("retention enabled: %+v", retention)
	} else {
		// Java defaults to mgr_purge_enabled=true; Go keeps deletion opt-in.
		log.Printf("WARNING: retention is off (Java default is on): old data is never deleted and the disk can fill up; set mgr_purge_enabled=true or --purge")
		if len(fromConf.purge) > 0 {
			log.Printf("config: mgr_purge_* keys are not applied while retention is off (set mgr_purge_enabled=true or --purge)")
		}
	}
	var err error
	cfg.Location, err = time.LoadLocation(timezone)
	if err != nil {
		log.Fatal(err)
	}
	// Java Logger (L1): server-yyyyMMdd.log and request-yyyyMMdd.log under
	// log_dir in the --timezone calendar. The standard log keeps writing to
	// stderr and is also recorded in the server log. main owns it: it is
	// closed after the runtime and the reload loop have stopped.
	lg, err := logger.New(server.LoggerConfig(startupLive(store, cfg, *logKeepDays)), logger.Options{Location: cfg.Location})
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(lg.StdLogWriter(os.Stderr))
	cfg.Logger = lg
	// From here on SIGQUIT dumps the goroutines instead of exiting 2.
	stopDump := dumpOnSIGQUIT(os.Stderr)
	stopProfiles := func() error { return nil }
	// finish stops what main owns besides the runtime, the server log last.
	// It runs once, on the normal exit or on fatal.
	finished := false
	finish := func() {
		if finished {
			return
		}
		finished = true
		if err := stopProfiles(); err != nil {
			log.Printf("profile: %v", err)
		}
		stopDump()
		closeLogger(lg)
	}
	fatal := func(v ...any) {
		log.Print(v...)
		finish()
		os.Exit(1)
	}
	if *pulseConf != "" || fromConf.pulse != nil {
		if *pulseAcks != 0 && *pulseAcks != 1 && *pulseAcks != -1 {
			fatal("--pulse-kafka-acks must be 0, 1 or -1")
		}
	}
	if *pulseConf != "" {
		f, err := os.Open(*pulseConf)
		if err != nil {
			fatal(err)
		}
		values, unknown, err := server.ReadPulseProperties(f)
		f.Close()
		if err != nil {
			fatal(err)
		}
		for _, k := range unknown {
			log.Printf("pulse: unknown key %s ignored", k)
		}
		cfg.Pulse = &server.PulseConfig{Values: values, Acks: int16(*pulseAcks)}
	} else if fromConf.pulse != nil {
		cfg.Pulse = &server.PulseConfig{Values: fromConf.pulse, Acks: int16(*pulseAcks)}
	}
	if *profileDir != "" {
		if stopProfiles, err = startProfiles(*profileDir); err != nil {
			fatal(err)
		}
	}
	if *confPath != "" {
		cfg.ConfDir = filepath.Dir(*confPath)
	}
	accountConfig(&cfg, store, explicit, *confPath, *accountDir)
	servletConfig(&cfg, store, explicit, *scouterHTTP, *scouterHTTPAllow, *scouterHTTPHeader, *scouterHTTPGzip, *scouterHTTPCheck || explicit["scouter-http-allow-ips"], *logKeepDays)
	webappConfig(&cfg, store, explicit, *webappFlag)
	if err := listenerConflict(cfg.HTTP, cfg.ScouterHTTP); err != nil {
		fatal(err)
	}
	runtime, err := server.Open(cfg)
	if err != nil {
		fatal(err)
	}
	live.Store(runtime)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The shutdown budget covers Run's teardown and what main stops after
	// it (mainStage names that part in the report).
	var mainStage atomic.Pointer[string]
	setStage := func(stage string) { mainStage.Store(&stage) }
	setStage("run")
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, shutdownSignals()...)
	guard := newShutdownGuard(*shutdownTimeout,
		func() string { return runtime.ShutdownReport() + fmt.Sprintf(" main=%q", *mainStage.Load()) },
		func(line string) {
			// Not through the log package: its lock may be held by a
			// goroutine stuck writing the server log.
			fmt.Fprintf(os.Stderr, "%s %s\n", time.Now().Format("2006/01/02 15:04:05"), line)
			lg.Println(line)
		},
		func() { _ = lg.Close() }, os.Exit)
	guard.watch(sigs, ctx, cancel, runtime.ShutdownStarted())
	runCtx, stopMode, abortMode, err := mode.start(ctx, cancel, runtime, cfg.Location)
	if err != nil {
		if cerr := runtime.Close(); cerr != nil {
			log.Print(cerr)
		}
		fatal(err)
	}
	guard.setAbort(abortMode)
	// main owns the Configure reload loop: it stops with ctx and is awaited.
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		if store != nil {
			_ = store.Watch(ctx)
		}
	}()
	err = runtime.Run(runCtx)
	cancel()
	setStage("mode stop")
	stopMode()
	setStage("config watch")
	<-watchDone
	setStage("finish")
	if err != nil {
		fatal(err)
	}
	finish()
}

// startupLive is the live settings the logger starts with: the scouter.conf
// snapshot (flag pins applied) or, without a file, the defaults with the
// --log-dir and --log-keep-days flags.
func startupLive(store *config.Store, cfg server.Config, keepDays int) config.Live {
	if store != nil {
		return store.Current().Live()
	}
	l := config.DefaultLive()
	l.LogDir, l.LogKeepDays = cfg.LogDirectory, keepDays
	return l
}

// closeLogger flushes and closes the server log last, after every other
// component, and points the standard log back at stderr.
func closeLogger(lg *logger.Logger) {
	log.SetOutput(os.Stderr)
	if err := lg.Close(); err != nil {
		log.Printf("logger: %v", err)
	}
	log.Printf("logger=%+v", lg.Stats())
}

// openConf opens the scouter.conf Store, which also re-reads the telegraf
// XML (--telegraf-config) on every reload. The default path may be absent
// (flags only, like a Java server without the file); an explicit --conf
// path must exist so a typo is not silently ignored.
func openConf(path string, explicit bool, telegrafPath string, pinned map[string]string, onChange func(config.Change)) (*config.Store, error) {
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		if explicit {
			return nil, err
		}
		log.Printf("config: %s not found; using flags and defaults", path)
	}
	return config.Open(path, config.Options{RestartKeys: pulse.JavaKeys(), TelegrafPath: telegrafPath, Pinned: pinned, OnChange: onChange})
}

// flagKeys maps the flags that set a Configure key one to one. An explicit
// flag pins the key (flag > scouter.conf): a reload does not change it and
// the first file change is logged once by config.Store.
// --scouter-http-allow-ips and --scouter-http-auth-ip-header are not here:
// they are Go-only servlet options (servletConfig applies them over the
// file at startup) and must not replace net_http_api_allow_ips /
// net_http_api_auth_ip_header_key of the webapp REST API (Live.Webapp).
var flagKeys = map[string]string{
	"server-id": "server_id", "db": "db_dir", "log-dir": "log_dir", "plugin-dir": "plugin_dir",
	"compress-xlog": "compress_xlog_enabled", "compress-profile": "compress_profile_enabled",
	"udp-read-buffer": "net_udp_so_rcvbuf_size", "log-keep-days": "log_keep_days",
	"scouter-http-gzip": "net_http_api_gzip_enabled", "webapp": "net_http_api_enabled",
	"purge": "mgr_purge_enabled",
}

func flagPins(explicit map[string]bool) map[string]string {
	return flagPinsOf(flag.CommandLine, explicit)
}

func flagPinsOf(fs *flag.FlagSet, explicit map[string]bool) map[string]string {
	pins := map[string]string{}
	fs.Visit(func(f *flag.Flag) {
		if key, ok := flagKeys[f.Name]; ok && explicit[f.Name] {
			pins[key] = f.Value.String()
		}
		if key := strings.ReplaceAll(f.Name, "-", "_"); strings.HasPrefix(key, "mgr_purge_") {
			pins[key] = f.Value.String()
		}
	})
	return pins
}

// confResult carries the scouter.conf values main combines with flags.
type confResult struct {
	purgeEnabled bool
	purge        map[string]int
	pulse        map[string]string
	notes        []string
}

// applyConf fills cfg from the conf snapshot for every mapped key whose flag
// was not given (priority: flag > scouter.conf > default). A key absent
// from the file keeps the Go flag default, which can differ from the Java
// default (e.g. 127.0.0.1 listeners, retention off).
func applyConf(cfg *server.Config, snap *config.Snapshot, explicit map[string]bool) confResult {
	var r confResult
	srv := snap.Server()
	note := func(format string, args ...any) { r.notes = append(r.notes, fmt.Sprintf(format, args...)) }
	str := func(flagName, key string, dst *string) {
		if !explicit[flagName] && snap.Set(key) {
			*dst = snap.String(key, *dst)
		}
	}
	boolean := func(flagName, key string, dst *bool) {
		if !explicit[flagName] && snap.Set(key) {
			*dst = snap.Bool(key, *dst)
		}
	}
	str("server-id", "server_id", &cfg.ServerID)
	str("db", "db_dir", &cfg.Directory)
	str("log-dir", "log_dir", &cfg.LogDirectory)
	str("plugin-dir", "plugin_dir", &cfg.PluginDirectory)
	boolean("compress-xlog", "compress_xlog_enabled", &cfg.CompressXLog)
	boolean("compress-profile", "compress_profile_enabled", &cfg.CompressProfile)
	listen := func(flagName, ipKey, portKey string, dst *string) {
		if explicit[flagName] || !(snap.Set(ipKey) || snap.Set(portKey)) {
			return
		}
		host, port, err := net.SplitHostPort(*dst)
		if err != nil {
			return
		}
		defPort, _ := strconv.Atoi(port)
		if snap.Set(ipKey) {
			host = snap.String(ipKey, host)
		}
		*dst = net.JoinHostPort(host, strconv.Itoa(snap.Int(portKey, defPort)))
	}
	listen("udp", "net_udp_listen_ip", "net_udp_listen_port", &cfg.UDP)
	listen("tcp", "net_tcp_listen_ip", "net_tcp_listen_port", &cfg.TCP)
	if !explicit["udp-read-buffer"] && snap.Set("net_udp_so_rcvbuf_size") {
		cfg.UDPReadBuffer = srv.UDPReceiveBuffer
	}
	if !explicit["scouter-http"] && srv.HTTPServerEnabled {
		// Java HttpServer binds every interface on net_http_port and serves
		// /counter, /register and /telegraf. /health and pprof stay on the
		// separate opt-in --http listener.
		cfg.ScouterHTTP = &server.ScouterHTTPConfig{Listen: net.JoinHostPort("0.0.0.0", strconv.Itoa(srv.HTTPPort))}
		note("config: net_http_server_enabled=true opens the Java HTTP servlets on %s", cfg.ScouterHTTP.Listen)
	}
	if snap.Set("mgr_purge_enabled") {
		r.purgeEnabled = srv.PurgeEnabled
	}
	if len(srv.Purge) > 0 {
		r.purge = srv.Purge
	}
	if !explicit["pulse-conf"] {
		if values := snap.Values(pulse.JavaKeys()); len(values) > 0 {
			r.pulse = values
		}
	}
	if snap.Set("object_deadtime_ms") && srv.ObjectDeadTime > 0 {
		cfg.ObjectDeadTime = srv.ObjectDeadTime
	}
	// Startup-only keys (Key.Restart), used only when set in the file.
	if !explicit["udp-workers"] && snap.Set("_net_udp_worker_thread_count") {
		v := snap.Int("_net_udp_worker_thread_count", transport.DefaultJavaUDPWorkers)
		n, adjusted := transport.UDPWorkerCount(v)
		if adjusted {
			note("config: _net_udp_worker_thread_count=%d adjusted to %d UDP workers", v, n)
		}
		cfg.UDPWorkers = n
	}
	if snap.Set("net_udp_packet_buffer_size") {
		v := snap.Int("net_udp_packet_buffer_size", 65535)
		n, adjusted := transport.UDPPacketBufferSize(v)
		if adjusted {
			note("config: net_udp_packet_buffer_size=%d adjusted to %d bytes (largest UDP payload; larger datagrams are rejected, not truncated)", v, n)
		}
		cfg.UDPMaxDatagram = n
	}
	for _, x := range []struct {
		key string
		dst *int
	}{
		{"_mgr_text_db_index_default_mb", &cfg.IndexSizes.TextDefault}, {"_mgr_text_db_index_service_mb", &cfg.IndexSizes.TextService},
		{"_mgr_text_db_index_api_mb", &cfg.IndexSizes.TextAPI}, {"_mgr_text_db_index_ua_mb", &cfg.IndexSizes.TextUA},
		{"_mgr_text_db_index_login_mb", &cfg.IndexSizes.TextLogin}, {"_mgr_text_db_index_desc_mb", &cfg.IndexSizes.TextDesc},
		{"_mgr_text_db_index_hmsg_mb", &cfg.IndexSizes.TextHMsg}, {"_mgr_text_db_daily_index_mb", &cfg.IndexSizes.TextDaily},
		{"_mgr_kv_store_index_default_mb", &cfg.IndexSizes.KV},
	} {
		v, ok := snap.IntSet(x.key)
		if !ok {
			continue
		}
		if v < 1 || v > storage.MaxIndexMB {
			note("config: %s=%d is outside 1..%d MiB; new index files keep the default size", x.key, v, storage.MaxIndexMB)
			continue
		}
		*x.dst = v
	}
	return r
}

// telegrafKeys reports whether a reloaded key belongs to the deprecated
// Telegraf mapping (input_telegraf_* / telegraf_object_deadtime_ms).
func telegrafKeys(keys []string) bool {
	for _, k := range keys {
		if strings.HasPrefix(k, "input_telegraf_") || k == "telegraf_object_deadtime_ms" {
			return true
		}
	}
	return false
}

// servletConfig finishes cfg.ScouterHTTP from the flags and scouter.conf:
// the listener (flag > conf), the allow list, the IP header, gzip, the
// request log retention and the deprecated Telegraf keys. Like Java
// applyTelegrafInputConfigNew, a Telegraf XML with enabled=true turns the
// HTTP server on even when net_http_server_enabled is false; the port is
// logged so the opt-in is visible.
func servletConfig(cfg *server.Config, store *config.Store, explicit map[string]bool, listen, allow, header string, gzip, checkAllow bool, keepDays int) {
	var snap *config.Snapshot
	if store != nil {
		snap = store.Current()
	}
	if listen != "" {
		cfg.ScouterHTTP = &server.ScouterHTTPConfig{Listen: listen}
	}
	var props map[string]string
	if snap != nil {
		props = snap.TelegrafDeprecated()
	}
	if cfg.ScouterHTTP == nil && snap != nil {
		tg, _, err := telegraf.Load(props, cfg.TelegrafConfigurationPath, time.Now())
		if err == nil && tg.ForcesHTTPServer() {
			cfg.ScouterHTTP = &server.ScouterHTTPConfig{Listen: net.JoinHostPort("0.0.0.0", strconv.Itoa(snap.Server().HTTPPort))}
			log.Printf("config: %s has enabled=true, which turns the Java HTTP server on at %s (Configure.applyTelegrafInputConfigNew)", cfg.TelegrafConfigurationPath, cfg.ScouterHTTP.Listen)
		}
	}
	if cfg.ScouterHTTP == nil {
		return
	}
	sc := cfg.ScouterHTTP
	sc.TelegrafProperties = props
	sc.API.Gzip = gzip
	sc.API.CheckAllowIPs = checkAllow || allow != ""
	sc.API.LogKeepDays = keepDays
	sc.API.AuthIPHeader = header
	if allow != "" {
		sc.API.AllowIPs = httpAllowList(allow)
	}
	if snap != nil {
		if !explicit["scouter-http-gzip"] && snap.Set("net_http_api_gzip_enabled") {
			sc.API.Gzip = snap.Bool("net_http_api_gzip_enabled", gzip)
		}
		if !explicit["log-keep-days"] && snap.Set("log_keep_days") {
			sc.API.LogKeepDays = snap.Int("log_keep_days", keepDays)
		}
		if !explicit["scouter-http-auth-ip-header"] && snap.Set("net_http_api_auth_ip_header_key") {
			sc.API.AuthIPHeader = snap.String("net_http_api_auth_ip_header_key", header)
		}
		if !explicit["scouter-http-allow-ips"] && snap.Set("net_http_api_allow_ips") {
			sc.API.AllowIPs = httpAllowList(snap.String("net_http_api_allow_ips", ""))
		}
	}
}

// listenerConflict rejects --http on the port of the Java HTTP servlet
// listener when the hosts overlap (equal IPs, a wildcard, or the same name):
// the second bind would fail with "address already in use", which names
// neither setting. Different host names are not resolved.
func listenerConflict(httpAddr string, servlet *server.ScouterHTTPConfig) error {
	if httpAddr == "" || servlet == nil {
		return nil
	}
	h1, p1, err1 := net.SplitHostPort(httpAddr)
	h2, p2, err2 := net.SplitHostPort(servlet.Listen)
	if err1 != nil || err2 != nil {
		return nil
	}
	port1, err1 := net.LookupPort("tcp", p1)
	port2, err2 := net.LookupPort("tcp", p2)
	if err1 != nil || err2 != nil || port1 != port2 || port1 == 0 {
		return nil
	}
	ip1, ip2 := net.ParseIP(h1), net.ParseIP(h2)
	wildcard := func(h string, ip net.IP) bool { return h == "" || ip != nil && ip.IsUnspecified() }
	if h1 != h2 && !(ip1 != nil && ip1.Equal(ip2)) && !wildcard(h1, ip1) && !wildcard(h2, ip2) {
		return nil
	}
	move := "another port"
	if port1 < 65535 {
		move = net.JoinHostPort(h1, strconv.Itoa(port1+1))
	}
	return fmt.Errorf("--http %s and the Java HTTP servlet listener %s (net_http_server_enabled, a Telegraf XML with enabled=true or --scouter-http) share port %d; move /health with --http %s or change net_http_port", httpAddr, servlet.Listen, port1, move)
}

// httpAllowList splits net_http_api_allow_ips like Java String.split(","):
// entries are not trimmed and trailing empty entries are dropped.
func httpAllowList(value string) []string {
	return httpapi.ParseAllowIPs(value)
}

// accountConfig selects the credential source. Java always loads
// conf/account.xml; Go keeps the explicit --user/--password pair when either
// flag is given, otherwise the files under --account-dir (default: the
// scouter.conf directory) and, with account_use_ldap=true, the LDAP keys.
func accountConfig(cfg *server.Config, store *config.Store, explicit map[string]bool, confPath, dir string) {
	if dir == "" && !explicit["user"] && !explicit["password"] && confPath != "" {
		dir = filepath.Dir(confPath)
	}
	if dir == "" {
		// Java always reads conf/account.xml. Say so when such a file is
		// being ignored: other accounts cannot log in, group policies other
		// than the single admin's do not apply and the account menus stay off.
		if confPath != "" && (explicit["user"] || explicit["password"]) {
			if _, err := os.Stat(filepath.Join(filepath.Dir(confPath), "account.xml")); err == nil {
				log.Printf("WARNING: --user/--password given: %s is ignored (single admin login; other accounts, group policies and account editing are unavailable). Drop --user/--password or pass --account-dir to use the Java account files", filepath.Join(filepath.Dir(confPath), "account.xml"))
			}
		}
		return
	}
	cfg.AccountDir = dir
	if store == nil {
		return
	}
	ldap := store.Current().Server().LDAP
	if !ldap.Enabled {
		return
	}
	cfg.LDAP = &account.LDAPConfig{URL: ldap.ProviderURL, Auth: ldap.Auth, PrincipalDomain: ldap.PrincipalDomain, BaseDN: ldap.BaseDN, EmailAttr: ldap.EmailID, GroupAttr: ldap.GroupID, Debug: ldap.Debug}
	log.Printf("config: account_use_ldap=true; LOGIN binds to %s and the account files still provide LIST/ADD/EDIT and policies", ldap.ProviderURL)
}

// webappConfig turns the scouter.webapp REST API on from --webapp or
// net_http_api_enabled and maps the net_http_api_* keys (Configure.java
// 219-258). Java mounts it on the HttpServer port, so a servlet listener is
// required; without one the request is reported and ignored.
func webappConfig(cfg *server.Config, store *config.Store, explicit map[string]bool, enabled bool) {
	var snap *config.Snapshot
	if store != nil {
		snap = store.Current()
	}
	if !explicit["webapp"] && snap != nil {
		enabled = snap.Bool("net_http_api_enabled", false)
	}
	if !enabled {
		return
	}
	if cfg.ScouterHTTP == nil {
		log.Print("config: the webapp REST API needs the servlet listener (net_http_server_enabled=true or --scouter-http); ignored")
		return
	}
	w := webapp.DefaultConfig()
	if snap != nil {
		w.CORSAllowOrigin = snap.String("net_http_api_cors_allow_origin", w.CORSAllowOrigin)
		w.CORSAllowCredentials = snap.String("net_http_api_cors_allow_credentials", w.CORSAllowCredentials)
		w.AuthIPEnabled = snap.Bool("net_http_api_auth_ip_enabled", false)
		w.AllowIPs = snap.String("net_http_api_allow_ips", w.AllowIPs)
		w.AuthIPHeaderKey = snap.String("net_http_api_auth_ip_header_key", "")
		w.AuthSessionEnabled = snap.Bool("net_http_api_auth_session_enabled", false)
		w.AuthBearerEnabled = snap.Bool("net_http_api_auth_bearer_token_enabled", false)
		w.SessionTimeout = time.Duration(snap.Int("net_http_api_session_timeout", 86400)) * time.Second
		w.Gzip = snap.Bool("net_http_api_gzip_enabled", true)
		w.PoolSize = snap.Int("net_webapp_tcp_client_pool_size", w.PoolSize)
		// applyConfig default: the current net_tcp_client_so_timeout_ms.
		if v, ok := snap.Value("net_webapp_tcp_client_pool_timeout"); ok {
			if ms, err := strconv.Atoi(v); err == nil {
				w.PoolIdleTimeout = time.Duration(ms) * time.Millisecond
				if ms == 0 {
					// Java closes every idle connection when the timeout is
					// <= 0; a zero webapp.Config value would mean the default.
					w.PoolIdleTimeout = -time.Millisecond
				}
			}
		}
	}
	cfg.ScouterHTTP.Webapp = &w
	log.Printf("config: webapp REST API enabled on the servlet listener (/scouter/*)")
}
