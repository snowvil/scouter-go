// Command scouter-replay runs a reproducible local server-performance baseline:
// it launches dist/scouter-go on a fresh temporary DB, sends Java-agent-framed
// UDP traffic at a scheduled rate while running concurrent TCP Client queries,
// stops the server with SIGTERM, restarts it on the same DB and counts the
// XLog transactions that are stored and re-queryable. Loopback only.
package main

import (
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const toolVersion = "scouter-replay/0.1.0"

type options struct {
	SampleHealth                bool
	Server, DB, WorkDir, Report string
	KeepDB                      bool
	SinkOnly                    bool
	ServerUDP, ServerTCP        string
	ServerPID                   int
	Bind, Timezone              string
	User, Password              string
	CompressXLog, CompressProf  bool
	GoMaxProcs, GoMemLimit      string
	FsyncPolicy                 string

	Rate                     float64
	Duration, Warmup         time.Duration
	Objects, Senders         int
	Seed                     uint64
	ProfileRatio             float64
	ProfileSizes             string
	TextHitRatio             float64
	TextPool                 int
	TextCardinality          int
	ErrorRatio               float64
	Batch, UDPMaxBytes       int
	SocketBuffer             int
	Tick                     time.Duration
	HeartbeatInterval        time.Duration
	CounterInterval          time.Duration
	MaxOverrun               time.Duration
	MaxTransactions          uint64
	QueryConcurrency         int
	QueryInterval            time.Duration
	QueryWindow, QueryTimout time.Duration
	QueryMax                 int
	Quiesce, DrainTimeout    time.Duration
	VerifyDelay              time.Duration
	ShutdownTimeout          time.Duration
	StartTimeout             time.Duration
	RSSInterval              time.Duration
	VerifyWindow             time.Duration
	Restart                  bool
	RestartTimeout           time.Duration
	RestartBackoff           time.Duration
	Stop                     string
	KillAfter                time.Duration
	ServerArgs               []string
}

func parseOptions(args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("scouter-replay", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: scouter-replay [flags]         replay to a managed server (default), an existing one (--server-udp/--server-tcp) or a discard socket (--sink-only)
       scouter-replay agents [flags]  many Agent TCP sessions against a running server (scouter-replay agents -h)

flags:
`)
		fs.PrintDefaults()
	}
	fs.BoolVar(&o.SampleHealth, "sample-health", false, "poll managed Go server /health once per second for ingest backlog")
	fs.BoolVar(&o.SinkOnly, "sink-only", false, "measure generator sends to a local discard socket; no server TPS verification")
	fs.StringVar(&o.ServerUDP, "server-udp", "", "existing server literal loopback UDP IP:port")
	fs.StringVar(&o.ServerTCP, "server-tcp", "", "existing server literal loopback TCP IP:port")
	fs.IntVar(&o.ServerPID, "server-pid", 0, "existing server PID for ps CPU/RSS samples; never signalled")
	fs.StringVar(&o.Server, "server", "dist/scouter-go", "scouter-go server binary")
	fs.StringVar(&o.DB, "db", "", "DB directory (must not exist or be empty); default: new temp dir under --work-dir")
	fs.BoolVar(&o.KeepDB, "keep-db", false, "keep the DB directory after the run")
	fs.StringVar(&o.WorkDir, "work-dir", "", "directory for server logs/ready files (default: new temp dir)")
	fs.StringVar(&o.Report, "report", "", "report JSON path (default: <work-dir>/report.json)")
	fs.StringVar(&o.Bind, "bind", "127.0.0.1", "loopback IP the server listens on (non-loopback is rejected)")
	fs.StringVar(&o.Timezone, "timezone", "Asia/Seoul", "server --timezone")
	fs.StringVar(&o.User, "user", "admin", "server login account")
	fs.StringVar(&o.Password, "password", "admin", "server login password (redacted in report)")
	fs.BoolVar(&o.CompressXLog, "compress-xlog", false, "server --compress-xlog")
	fs.BoolVar(&o.CompressProf, "compress-profile", false, "server --compress-profile")
	fs.StringVar(&o.GoMaxProcs, "gomaxprocs", "", "GOMAXPROCS for the server process (empty: unset)")
	fs.StringVar(&o.GoMemLimit, "gomemlimit", "", "GOMEMLIMIT for the server process (empty: unset)")
	fs.StringVar(&o.FsyncPolicy, "fsync-policy", "", "optional operator note recorded next to the fsync policy derived from --sync-interval")
	fs.Func("server-arg", "extra server flag passed through verbatim (repeatable, e.g. --server-arg=--sync-interval=0)", func(v string) error {
		o.ServerArgs = append(o.ServerArgs, v)
		return nil
	})
	fs.StringVar(&o.Stop, "stop", "term", "server-1 stop: term (drain, SIGTERM) or kill (SIGKILL crash, then restart and measure loss)")
	fs.DurationVar(&o.KillAfter, "kill-after", 0, "with --stop=kill: SIGKILL this long after the schedule start (0 = right after sending ends); senders stop first")
	fs.Float64Var(&o.Rate, "rate", 200, "target transactions (XLogs) per second")
	fs.DurationVar(&o.Duration, "duration", 10*time.Second, "steady measurement duration after warm-up")
	fs.DurationVar(&o.Warmup, "warmup", 0, "warm-up duration (sent and verified, excluded from steady rate)")
	fs.IntVar(&o.Objects, "objects", 20, "number of agent objects (heartbeat + counters)")
	fs.IntVar(&o.Senders, "senders", 1, "parallel UDP sender sockets")
	fs.Uint64Var(&o.Seed, "seed", 1, "payload seed")
	fs.Float64Var(&o.ProfileRatio, "profile-ratio", 0.3, "fraction of transactions with a profile size draw")
	fs.StringVar(&o.ProfileSizes, "profile-sizes", "0:0.1,1024:0.6,8192:0.25,131072:0.05", "profile bytes:weight list; 0 = no ProfilePack; > --udp-max-bytes forces CAFM fragments")
	fs.Float64Var(&o.TextHitRatio, "text-hit-ratio", 0.95, "probability a text reference reuses an already-sent hash (no TextPack)")
	fs.IntVar(&o.TextPool, "text-pool", 10000, "max reusable texts per type held by the generator")
	fs.IntVar(&o.TextCardinality, "text-cardinality", 0, "max distinct texts per type (service, sql, method, error, ua, referer); once created, references only reuse them, so tag and text dictionary growth stays bounded like a real service set (0: unbounded, every miss is a new text)")
	fs.Float64Var(&o.ErrorRatio, "error-ratio", 0.01, "fraction of XLogs with error")
	fs.IntVar(&o.Batch, "batch", 32, "max packs per CAFN datagram (1 = CAFE single per pack)")
	fs.IntVar(&o.UDPMaxBytes, "udp-max-bytes", 60000, "Java net_udp_packet_max_bytes; larger packs are CAFM-fragmented")
	fs.IntVar(&o.SocketBuffer, "socket-buffer", 4<<20, "sender SO_SNDBUF bytes (macOS needs > 9216 for large datagrams)")
	fs.DurationVar(&o.Tick, "tick", time.Millisecond, "minimum sender sleep granularity")
	fs.DurationVar(&o.HeartbeatInterval, "heartbeat-interval", 2*time.Second, "ObjectPack heartbeat interval")
	fs.DurationVar(&o.CounterInterval, "counter-interval", 2*time.Second, "PerfCounterPack interval")
	fs.DurationVar(&o.MaxOverrun, "max-overrun", 30*time.Second, "abandon scheduled sends this long after the schedule end (counted as not_sent)")
	fs.Uint64Var(&o.MaxTransactions, "max-transactions", 500_000_000, "upper bound of scheduled transactions (bitset memory)")
	fs.IntVar(&o.QueryConcurrency, "query-concurrency", 2, "concurrent TCP query workers during ingest (0 = none)")
	fs.DurationVar(&o.QueryInterval, "query-interval", time.Second, "per-worker query interval")
	fs.DurationVar(&o.QueryWindow, "query-window", 10*time.Second, "history query time window")
	fs.DurationVar(&o.QueryTimout, "query-timeout", 30*time.Second, "per-request TCP timeout")
	fs.IntVar(&o.QueryMax, "query-max", 500, "history query max records")
	fs.DurationVar(&o.Quiesce, "quiesce", time.Second, "SIGTERM after the realtime XLog cursor is unchanged this long")
	fs.DurationVar(&o.DrainTimeout, "drain-timeout", 60*time.Second, "max wait for quiescence before SIGTERM")
	fs.DurationVar(&o.VerifyDelay, "verify-delay", 0, "external mode: wait this long after quiescence before the verification query (a Java server flushes its XLog index seconds later); not counted in the drain time")
	fs.DurationVar(&o.ShutdownTimeout, "shutdown-timeout", 120*time.Second, "how long replay waits for the managed server to exit after its stop signal (SIGTERM, or SIGKILL with --stop=kill) before sending SIGKILL; the server's own --shutdown-timeout goes through --server-arg")
	fs.DurationVar(&o.StartTimeout, "start-timeout", 60*time.Second, "server ready-file wait limit")
	fs.DurationVar(&o.RSSInterval, "rss-interval", time.Second, "server RSS sampling interval (0 = off)")
	fs.DurationVar(&o.VerifyWindow, "verify-window", 0, "verification query window (0 = auto from rate)")
	fs.BoolVar(&o.Restart, "restart", true, "restart the server on the same DB and verify stored XLogs")
	fs.DurationVar(&o.RestartTimeout, "restart-timeout", 30*time.Second, "keep retrying a refused restart (e.g. fresh lock.dat after SIGKILL) this long")
	fs.DurationVar(&o.RestartBackoff, "restart-backoff", time.Second, "delay between restart attempts")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() != 0 {
		return o, fmt.Errorf("unexpected arguments %q", fs.Args())
	}
	if o.ServerUDP != "" || o.SinkOnly {
		o.Restart = false
	}
	return o, o.validate()
}

func (o options) validate() error {
	var errs []error
	check := func(ok bool, format string, a ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, a...))
		}
	}
	check(!o.SampleHealth || (!o.SinkOnly && o.ServerUDP == ""), "--sample-health requires managed Go server mode")
	if o.SampleHealth {
		if _, ok := serverArgValue(o.ServerArgs, "http"); ok {
			check(false, "--sample-health manages --http")
		}
	}
	check((o.ServerUDP == "") == (o.ServerTCP == ""), "--server-udp and --server-tcp must be supplied together")
	check(!o.SinkOnly || o.ServerUDP == "", "--sink-only cannot use an external server")
	check(o.ServerPID >= 0 && (o.ServerPID == 0 || o.ServerUDP != ""), "--server-pid requires external server mode")
	check(o.VerifyDelay >= 0 && (o.VerifyDelay == 0 || o.ServerUDP != ""), "--verify-delay requires external server mode")
	if o.ServerUDP != "" {
		check(o.DB == "" && !o.KeepDB && !o.CompressXLog && !o.CompressProf && o.GoMaxProcs == "" && o.GoMemLimit == "" && len(o.ServerArgs) == 0, "external server configuration is operator managed; do not pass DB/compression/runtime/server flags")
		errs = append(errs, requireLoopback(o.ServerUDP), requireLoopback(o.ServerTCP))
	}
	check((!o.SinkOnly && o.ServerUDP == "") || o.Stop == "term", "sink/external modes cannot kill a server")
	errs = append(errs, requireLoopbackIP(o.Bind), validateServerArgs(o.ServerArgs))
	check(o.Stop == "term" || o.Stop == "kill", "--stop must be term or kill")
	check(o.KillAfter >= 0 && (o.KillAfter == 0 || o.Stop == "kill"), "--kill-after requires --stop=kill and must be >= 0")
	check(o.Stop != "kill" || o.Restart, "--stop=kill requires --restart")
	check(o.RestartTimeout >= 0 && o.RestartBackoff > 0, "--restart-timeout must be >= 0 and --restart-backoff > 0")
	check(o.Rate > 0 && !math.IsInf(o.Rate, 0), "--rate must be > 0")
	check(o.Duration > 0 && o.Warmup >= 0, "--duration must be > 0 and --warmup >= 0")
	check(o.Objects >= 1 && o.Objects <= 4096, "--objects must be 1..4096 (server object cache limit 4096)")
	check(o.Senders >= 1 && o.Senders <= o.Objects && o.Senders <= 256, "--senders must be 1..min(objects,256)")
	check(o.ProfileRatio >= 0 && o.ProfileRatio <= 1, "--profile-ratio must be 0..1")
	check(o.TextHitRatio >= 0 && o.TextHitRatio <= 1, "--text-hit-ratio must be 0..1")
	check(o.ErrorRatio >= 0 && o.ErrorRatio <= 1, "--error-ratio must be 0..1")
	check(o.TextPool >= 1 && o.TextPool <= 1_000_000, "--text-pool must be 1..1000000")
	check(o.TextCardinality >= 0 && o.TextCardinality <= o.TextPool, "--text-cardinality must be 0..--text-pool")
	check(o.Batch >= 1 && o.Batch <= 1024, "--batch must be 1..1024 (server MaxBatchPackets 1024)")
	check(o.UDPMaxBytes >= 512 && o.UDPMaxBytes <= 65000, "--udp-max-bytes must be 512..65000")
	check(o.Tick > 0 && o.HeartbeatInterval > 0 && o.CounterInterval > 0, "--tick/--heartbeat-interval/--counter-interval must be > 0")
	check(o.QueryConcurrency >= 0 && o.QueryConcurrency <= 64, "--query-concurrency must be 0..64")
	check(o.QueryConcurrency == 0 || (o.QueryInterval > 0 && o.QueryWindow > 0 && o.QueryWindow <= 24*time.Hour), "--query-interval/--query-window must be > 0 (window <= 24h)")
	check(o.QueryTimout > 0 && o.Quiesce > 0 && o.DrainTimeout > 0 && o.ShutdownTimeout > 0 && o.StartTimeout > 0, "timeouts must be > 0")
	check(o.QueryMax >= 0, "--query-max must be >= 0")
	check(o.MaxTransactions >= 1 && o.MaxTransactions < 1<<62, "--max-transactions out of range")
	if o.Rate > 0 {
		total := o.Rate * (o.Warmup + o.Duration).Seconds()
		check(total <= float64(o.MaxTransactions), "schedule of %.0f transactions exceeds --max-transactions %d", total, o.MaxTransactions)
		check(total >= 1, "schedule has no transactions")
	}
	if _, err := parseProfileSizes(o.ProfileSizes); err != nil {
		errs = append(errs, err)
	}
	if _, err := time.LoadLocation(o.Timezone); err != nil {
		errs = append(errs, fmt.Errorf("--timezone: %w", err))
	}
	return errors.Join(errs...)
}

// prepareDB enforces "fresh DB only": a missing path is created, an existing path
// must be an empty directory. created reports whether the tool owns the directory.
func prepareDB(path string) (created bool, err error) {
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return true, os.MkdirAll(path, 0o700)
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return false, fmt.Errorf("--db %s is not a directory", path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, err
	}
	if len(entries) != 0 {
		return false, fmt.Errorf("--db %s is not empty; replay never writes to an existing DB", path)
	}
	return false, nil
}

func dirBytes(root string) (int64, int, error) {
	var total int64
	files := 0
	err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			total += info.Size()
			files++
		}
		return nil
	})
	return total, files, err
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func gitInfo() (sha string, dirty *bool, err error) {
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "", nil, err
	}
	sha = strings.TrimSpace(string(out))
	if st, e := exec.Command("git", "status", "--porcelain", "--untracked-files=no").Output(); e == nil {
		d := len(strings.TrimSpace(string(st))) > 0
		dirty = &d
	}
	return sha, dirty, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("scouter-replay: ")
	if len(os.Args) > 1 && os.Args[1] == "agents" {
		os.Exit(runAgents(os.Args[2:]))
	}
	o, err := parseOptions(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		log.Print(err)
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	rep, err := run(ctx, o)
	if rep != nil && rep.ReportPath != "" {
		if werr := writeReport(rep.ReportPath, rep); werr != nil {
			log.Printf("write report: %v", werr)
			err = errors.Join(err, werr)
		} else {
			log.Printf("report: %s", rep.ReportPath)
		}
		log.Print(rep.summaryLine())
	}
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
	if rep != nil && !rep.Pass {
		os.Exit(3)
	}
}

func writeReport(path string, rep *report) error {
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func autoVerifyWindow(rate float64) int64 {
	// Keep each verification query well below the server's 100,000 record limit.
	w := int64(20000 * 1000 / rate)
	return max(1, min(w, 60000))
}

func run(ctx context.Context, o options) (rep *report, err error) {
	if o.SinkOnly {
		return runSink(ctx, o)
	}
	loc, _ := time.LoadLocation(o.Timezone)
	sizes, _ := parseProfileSizes(o.ProfileSizes)
	rep = newReport(o, sizes)
	kernelBefore := readKernelUDP()
	defer func() { rep.KernelUDP = kernelDelta(kernelBefore, readKernelUDP()) }()
	external := o.ServerUDP != ""
	serverPath, err := filepath.Abs(o.Server)
	if err != nil {
		return rep, err
	}
	if !external {
		if info, statErr := os.Stat(serverPath); statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
			return rep, fmt.Errorf("--server %s is not an executable file (run make build)", serverPath)
		}
		rep.Server.Path = serverPath
		if rep.Server.SHA256, err = fileSHA256(serverPath); err != nil {
			return rep, err
		}
		rep.ServerConfig["fsync"] = describeFsync(o.ServerArgs, serverPath, o.FsyncPolicy)
		if bi, e := buildinfo.ReadFile(serverPath); e == nil {
			rep.Server.GoVersion = bi.GoVersion
			for _, s := range bi.Settings {
				if strings.HasPrefix(s.Key, "vcs.") {
					rep.Server.VCS[s.Key] = s.Value
				}
			}
		} else {
			rep.Server.BuildInfoError = e.Error()
		}
	}
	workDir := o.WorkDir
	if workDir == "" {
		if workDir, err = os.MkdirTemp("", "scouter-replay-"); err != nil {
			return rep, err
		}
	} else if err = os.MkdirAll(workDir, 0o700); err != nil {
		return rep, err
	}
	workDir, _ = filepath.Abs(workDir)
	rep.WorkDir = workDir
	rep.ReportPath = o.Report
	if rep.ReportPath == "" {
		rep.ReportPath = filepath.Join(workDir, "report.json")
	}
	db := o.DB
	if !external {
		if db == "" {
			db = filepath.Join(workDir, "db")
		}
		db, _ = filepath.Abs(db)
		created, err := prepareDB(db)
		if err != nil {
			return rep, err
		}
		rep.Disk.DBPath = db
		if t, a, tot, e := filesystemInfo(db); e == nil {
			rep.Disk.FSType, rep.Disk.AvailBeforeBytes, rep.Disk.TotalBytes = t, &a, &tot
		} else {
			rep.Disk.Note = e.Error()
		}
		defer func() {
			if b, files, e := dirBytes(db); e == nil {
				rep.Disk.DBBytes, rep.Disk.DBFiles = &b, files
			}
			if _, a, _, e := filesystemInfo(db); e == nil {
				rep.Disk.AvailAfterBytes = &a
			}
			if created && !o.KeepDB {
				rep.Disk.DBRemoved = os.RemoveAll(db) == nil
			}
		}()
	}
	if o.SampleHealth {
		o.ServerArgs = append(o.ServerArgs, "--http=127.0.0.1:0")
	}
	scfg := serverConfig{Binary: serverPath, DB: db, WorkDir: workDir, Bind: o.Bind, Timezone: o.Timezone, User: o.User, Password: o.Password,
		CompressXLog: o.CompressXLog, CompressProf: o.CompressProf, GoMaxProcs: o.GoMaxProcs, GoMemLimit: o.GoMemLimit,
		StartTimeout: o.StartTimeout, RSSInterval: o.RSSInterval, Extra: o.ServerArgs}
	for _, d := range []string{"logs", "plugin", "conf"} {
		os.MkdirAll(filepath.Join(workDir, d), 0o700)
	}
	var srv *serverProc
	if external {
		srv = &serverProc{Endpoints: endpoints{UDP: o.ServerUDP, TCP: o.ServerTCP}}
		rep.ServerConfig = map[string]any{"mode": "external", "udp": o.ServerUDP, "tcp": o.ServerTCP, "pid": o.ServerPID, "configuration": "operator managed; compression/runtime/fsync not inferred"}
		rep.Limitations = append(rep.Limitations, "external server: no restart, disk inspection or shutdown counters; UDP reject and ingest backlog unavailable")
		stopSample := startExternalSamples(o.ServerPID, o.RSSInterval, rep)
		defer stopSample()
	} else {
		scfg.Label = "server-1"
		srv, err = startServer(ctx, scfg)
		if err != nil {
			return rep, err
		}
		defer srv.stop(o.ShutdownTimeout)
	}
	udpAddr, err := net.ResolveUDPAddr("udp", srv.Endpoints.UDP)
	if err != nil {
		return rep, err
	}

	stopHealth := startHealthSamples(ctx, srv.Endpoints.HTTP, rep)
	defer stopHealth()
	objects := makeObjects(o.Objects)
	rep.Payload.ObjectsNegativeHash = 0
	for _, ob := range objects {
		if ob.Hash < 0 {
			rep.Payload.ObjectsNegativeHash++
		}
	}
	gcfg := genConfig{Seed: o.Seed, Objects: objects, ProfileRatio: o.ProfileRatio, ProfileSizes: sizes, TextHitRatio: o.TextHitRatio, TextPool: o.TextPool, TextCardinality: o.TextCardinality, ErrorRatio: o.ErrorRatio}
	codec := newTxidCodec(o.Seed)
	// Start slightly in the future so every sender begins on schedule.
	sched := newSchedule(time.Now().Add(50*time.Millisecond), o.Rate, o.Warmup, o.Duration)
	scheduleEnd := sched.start.Add(o.Warmup + o.Duration)
	failed, sent := newBitset(sched.total), newBitset(sched.total)

	queryCtx, stopQueries := context.WithCancel(ctx)
	var queryRes *queryResult
	var queryWG sync.WaitGroup
	if o.QueryConcurrency > 0 {
		queryWG.Add(1)
		go func() {
			defer queryWG.Done()
			queryRes = runQueries(queryCtx, queryConfig{Addr: srv.Endpoints.TCP, User: o.User, Password: o.Password, Concurrency: o.QueryConcurrency,
				Interval: o.QueryInterval, Window: o.QueryWindow, Timeout: o.QueryTimout, Max: o.QueryMax, Objects: objects, Location: loc, Codec: codec, Schedule: sched})
		}()
	}
	// --stop=kill: senders stop at the kill instant (0 = when the schedule ends) and
	// SIGKILL follows immediately, so queued-but-unstored data measures the crash window.
	senderCtx, stopSenders := context.WithCancel(ctx)
	defer stopSenders()
	var killAt time.Time
	if o.Stop == "kill" && o.KillAfter > 0 {
		killAt = sched.start.Add(o.KillAfter)
		killTimer := time.AfterFunc(time.Until(killAt), stopSenders)
		defer killTimer.Stop()
	}
	cpuBefore := selfCPU()
	results := make([]*senderResult, o.Senders)
	var wg sync.WaitGroup
	for k := 0; k < o.Senders; k++ {
		var owned []object
		for j := k; j < len(objects); j += o.Senders {
			owned = append(owned, objects[j])
		}
		cfg := senderConfig{Index: k, Count: o.Senders, Schedule: sched, HardStop: scheduleEnd.Add(o.MaxOverrun), Tick: o.Tick,
			HeartbeatInterval: o.HeartbeatInterval, CounterInterval: o.CounterInterval, MaxBytes: o.UDPMaxBytes, Batch: o.Batch,
			SocketBuffer: o.SocketBuffer, Target: udpAddr, Objects: owned, Failed: failed, Sent: sent}
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[k] = runSender(senderCtx, cfg, newGenerator(gcfg, k))
		}()
	}
	log.Printf("sending %d transactions at %.1f/s (warmup %s, steady %s) with %d sender(s)", sched.total, o.Rate, o.Warmup, o.Duration, o.Senders)
	wg.Wait()
	sendDone := time.Now()
	cpuAfter := selfCPU()
	stopQueries()
	queryWG.Wait()
	if ctx.Err() != nil {
		return rep, ctx.Err()
	}

	stopHealth()
	var stop1 stopResult
	drainStart := time.Now()
	drainEnd := drainStart
	if o.Stop == "kill" {
		// Sending ended (or was cut at --kill-after); wait for a later kill instant, then crash.
		if wait := time.Until(killAt); wait > 0 {
			select {
			case <-ctx.Done():
				return rep, ctx.Err()
			case <-time.After(wait):
			}
		}
		drainEnd = time.Now()
		stop1 = srv.kill(o.ShutdownTimeout)
		rep.Drain = drainReport{Skipped: "--stop=kill: no drain, SIGKILL"}
		rep.Crash = &crashReport{KillAfterMS: float64(o.KillAfter.Milliseconds()), SendersStoppedToKillMS: msBetween(sendDone, stop1.SignalSentAt),
			KillToExitMS: stop1.ShutdownMS, ExitCode: stop1.ExitCode, WaitError: stop1.WaitError, ExitedBeforeKill: stop1.AlreadyExited}
	} else {
		// Drain: wait until the realtime XLog cursor stops moving, then SIGTERM.
		quiesced, cursorErr := waitQuiescent(ctx, srv.Endpoints.TCP, o)
		drainEnd = time.Now()
		if !external {
			stop1 = srv.stop(o.ShutdownTimeout)
		}
		rep.Drain = drainReport{WaitMS: msBetween(drainStart, drainEnd), Quiesced: quiesced, QuiesceMS: float64(o.Quiesce.Milliseconds()), Error: cursorErr}
		if external && o.VerifyDelay > 0 {
			rep.Drain.VerifyDelayMS = float64(o.VerifyDelay.Milliseconds())
			select {
			case <-ctx.Done():
			case <-time.After(o.VerifyDelay):
			}
		}
	}
	serverExit := srv.exitedAt
	if external {
		serverExit = drainEnd
	} else {
		rep.Servers = append(rep.Servers, srv.report(stop1))
	}
	if !external {
		log.Printf("server-1 %s exit=%d stop=%.0fms timeout-kill=%v", stop1.Signal, stop1.ExitCode, stop1.ShutdownMS, stop1.Killed)
	} else {
		log.Printf("external server remains running; quiesced=%v", rep.Drain.Quiesced)
	}

	aggregateSenders(rep, results, gcfg)
	rep.Send.Scheduled = sched.total
	rep.Send.StartedAt = sched.start
	rep.Send.ScheduleEnd = scheduleEnd
	rep.Send.FinishedAt = sendDone
	rep.Send.SenderCPU = cpuAfter.sub(cpuBefore)
	if queryRes != nil {
		q := queryRes.report()
		rep.Queries = &q
	}

	// Restart and verify stored, re-queryable XLogs.
	verifyWindow := autoVerifyWindow(o.Rate)
	if o.VerifyWindow > 0 {
		verifyWindow = o.VerifyWindow.Milliseconds()
	}
	cmp := newComparison(sched.total, codec, objects)
	if o.Restart {
		srv2, restart := restartServer(ctx, scfg, o, serverExit)
		rep.Restart = restart
		if srv2 == nil {
			rep.Verify.Error = restart.Error
		} else {
			from := sched.start.UnixMilli() - 5000
			to := max(sendDone.UnixMilli(), drainEnd.UnixMilli()) + 5000
			dial := func() (*client, error) { return dialClient(ctx, srv2.Endpoints.TCP, o.User, o.Password, o.QueryTimout) }
			rep.Verify = verifyStored(ctx, dial, cmp, from, to, verifyWindow, loc, objects)
			stop2 := srv2.stop(o.ShutdownTimeout)
			p2 := srv2.report(stop2)
			rep.Servers = append(rep.Servers, p2)
			rep.Restart.ServerErrorLogLines = p2.ErrorLogLines
			rep.Restart.RecoveryEvents = p2.RecoveryEvents
			rep.Restart.ExitCode, rep.Restart.ShutdownKilled, rep.Restart.ExitedEarly = stop2.ExitCode, stop2.Killed, stop2.AlreadyExited
			if st := p2.Stats; st != nil {
				rep.Restart.StorageErrors = st["ingest"]["StorageErrors"]
				rep.Restart.MaintenanceErrors = st["log"]["maintenanceErrors"]
				rep.Restart.StatsRecoveries = st["log"]["recoveries"]
			}
		}
	} else if external {
		dial := func() (*client, error) { return dialClient(ctx, o.ServerTCP, o.User, o.Password, o.QueryTimout) }
		rep.Verify = verifyStored(ctx, dial, cmp, sched.start.UnixMilli(), max(sendDone.UnixMilli(), drainEnd.UnixMilli()), verifyWindow, loc, objects)
		rep.Verify.Method = "external live server + TRANX_LOAD_TIME_GROUP; restart not attempted"
	} else {
		rep.Verify.Method = "skipped (--restart=false); stored transactions are unverified"
	}
	finishVerify(&rep.Verify, cmp, sched, failed, sent, rep.Send)
	rep.Verify.Comparison = cmp
	rep.Verify.ExtraSamples, rep.Verify.DuplicateSamples, rep.Verify.MismatchSamples = cmp.extraSamples, cmp.duplicateSamples, cmp.mismatchSamples

	// Throughput.
	window := serverExit.Sub(sched.start).Seconds()
	// The steady window ends at the schedule end, or earlier when --kill-after cut sending.
	steadySec := min(o.Duration, max(sendDone.Sub(sched.start.Add(o.Warmup)), 0)).Seconds()
	if steadySec <= 0 {
		steadySec = math.NaN()
	}
	rep.Throughput = throughputReport{
		Definition:            "TPS = unique XLog transactions stored and re-queried after restart / (first scheduled send -> server-1 exit after SIGTERM, drain included). Packets, datagrams and ingest Completed are not transactions.",
		TargetRate:            o.Rate,
		VerifiedUnique:        cmp.Unique,
		WindowSeconds:         window,
		DrainInclusiveTPS:     float64(cmp.Unique) / window,
		SteadyWindowSeconds:   steadySec,
		SteadySentOK:          rep.Verify.steadySent,
		SteadySendRate:        float64(rep.Verify.steadySent) / steadySec,
		SteadyVerified:        rep.Verify.SteadyFound,
		SteadyVerifiedRate:    float64(rep.Verify.SteadyFound) / steadySec,
		SendPhaseSeconds:      sendDone.Sub(sched.start).Seconds(),
		DrainAfterSendSeconds: serverExit.Sub(sendDone).Seconds(),
		ScheduledTransactions: sched.total,
		SteadyIndexFrom:       sched.steadyFrom,
		SteadyNote:            "steady values count transactions by scheduled send instant inside [warmup end, schedule end); storage completion time is not observable per transaction",
		VerifiedAvailable:     external || (o.Restart && rep.Restart.OK),
	}
	if o.Stop == "kill" {
		rep.Throughput.Definition = "--stop=kill: recovered unique XLogs / (first scheduled send -> SIGKILL exit). This is a crash-recovery rate, not the drain-inclusive acceptance TPS."
	}
	if steadySec != steadySec { // NaN: no steady sending happened
		rep.Throughput.SteadySendRate, rep.Throughput.SteadyVerifiedRate, rep.Throughput.SteadyWindowSeconds = 0, 0, 0
		rep.Throughput.SteadyNote += "; no steady sending before the kill"
	}
	if !external && (!o.Restart || !rep.Restart.OK) {
		rep.Throughput.DrainInclusiveTPS = 0
		rep.Throughput.Note = "restart verification did not run; representative TPS unavailable"
	}
	var packs uint64
	for _, n := range rep.Payload.PackCount {
		packs += n
	}
	if !external && len(rep.Servers) > 0 && rep.Servers[0].Stats != nil {
		st := rep.Servers[0].Stats
		rep.UDPAccounting = &udpAccounting{SentDatagramsOK: rep.Send.DatagramsOK, ServerDatagrams: st["udp"]["Datagrams"],
			UnreceivedDatagrams: int64(rep.Send.DatagramsOK) - st["udp"]["Datagrams"], ServerRejectedDatagrams: st["udp"]["Rejected"],
			SentPacks: packs, ServerPacks: st["udp"]["Packets"], ServerIngestCompleted: st["ingest"]["Completed"], ServerXLogs: st["ingest"]["XLogs"],
			VerifiedXLogs: cmp.Unique, ServerShutdownDiscarded: st["udp"]["ShutdownDiscarded"],
			ServerAbortedPackets: st["udp"]["AbortedPackets"], ServerAbortedAssemblies: st["udp"]["AbortedAssemblies"],
			Note: "unreceived = sent ok - server received (kernel socket drop on loopback); rejected = size limit or full UDP queue while serving; " +
				"shutdown_discarded/aborted = datagrams/packs/fragment assemblies left in the queue at SIGTERM (shutdown residue); packs/xlogs are separate units"}
	} else if o.Stop == "kill" {
		rep.UDPAccounting = &udpAccounting{SentDatagramsOK: rep.Send.DatagramsOK, SentPacks: packs, ServerDatagrams: -1, UnreceivedDatagrams: -1, ServerRejectedDatagrams: -1,
			ServerPacks: -1, ServerIngestCompleted: -1, ServerXLogs: -1, ServerShutdownDiscarded: -1, ServerAbortedPackets: -1, ServerAbortedAssemblies: -1,
			VerifiedXLogs: cmp.Unique, Note: "server counters unavailable after SIGKILL (-1); only sender counts and recovered XLogs are known"}
	}
	if o.Stop == "kill" {
		c := rep.Crash
		c.SentOKBeforeKill, c.LostAfterSendOK = rep.Verify.Expected, rep.Verify.MissingAfterSendOK
		c.RecoveredUnique = c.SentOKBeforeKill - c.LostAfterSendOK
		if c.SentOKBeforeKill > 0 {
			c.LossRatio = float64(c.LostAfterSendOK) / float64(c.SentOKBeforeKill)
		}
		c.Note = "loss is the measured crash-durability window (UDP-accepted but not stored or not reachable after SIGKILL), not a failure; failure = restart/verification/integrity/server-2 errors. " +
			"SIGKILL is a process crash: the OS page cache survives, so this does not measure power-loss/kernel-crash durability"
		rep.PassCriteria = "kill mode: restart ok and healthy (exit 0, no non-info log lines, no storage/maintenance errors), every verification window complete, 0 extra/duplicate/objHash mismatch; loss and storage recovery events are reported, not judged"
	}
	rep.Pass = decidePass(o.Stop, rep, cmp, stop1)
	if external {
		rep.Throughput.Definition = "unique XLogs verified by TCP after quiescence / send start to quiescence; persistence after restart unverified"
		rep.PassCriteria = "complete TCP verification, scheduled=verified, zero missing/extra/duplicate/objHash mismatch; server reject/backlog not certified"
		rep.Pass = rep.Verify.Complete && cmp.Unique == sched.total && cmp.Extra == 0 && cmp.Duplicates == 0 && cmp.ObjHashMismatch == 0 && rep.Drain.Quiesced
	}
	return rep, nil
}

// decidePass applies the pass criteria. Kill mode records loss and storage
// recovery events without judging them; term mode fails on any missing XLog and
// on any recovery event, since a clean SIGTERM shutdown must not need repair.
func decidePass(mode string, rep *report, cmp *comparison, stop1 stopResult) bool {
	r := rep.Restart
	restartHealthy := r.OK && r.ExitCode == 0 && !r.ShutdownKilled && !r.ExitedEarly &&
		r.ServerErrorLogLines == 0 && r.StorageErrors == 0 && r.MaintenanceErrors == 0
	integrity := rep.Verify.Complete && cmp.Extra == 0 && cmp.Duplicates == 0 && cmp.ObjHashMismatch == 0
	if mode == "kill" {
		return restartHealthy && integrity && !stop1.AlreadyExited
	}
	noRecovery := r.RecoveryEvents.Count == 0 && r.StatsRecoveries == 0
	return restartHealthy && integrity && noRecovery && rep.Verify.MissingAfterSendOK == 0 &&
		!stop1.Killed && !stop1.AlreadyExited && stop1.ExitCode == 0
}

// restartServer starts server-2 on the same DB. A crashed writer leaves lock.dat
// fresh for the Java 5s stale rule, so a refused start is retried every
// --restart-backoff until --restart-timeout; each refusal is recorded.
func restartServer(ctx context.Context, scfg serverConfig, o options, previousExit time.Time) (*serverProc, restartReport) {
	r := restartReport{Attempted: true}
	begin := time.Now()
	deadline := begin.Add(o.RestartTimeout)
	for attempt := 1; ; attempt++ {
		scfg.Label = fmt.Sprintf("server-2-try%d", attempt)
		t0 := time.Now()
		srv, err := startServer(ctx, scfg)
		r.Attempts = attempt
		if err == nil {
			r.OK, r.Label, r.Error = true, scfg.Label, "" // refusals stay in Failures
			r.StartToReadyMS = msBetween(t0, srv.readyAt)
			r.BeginToReadyMS = msBetween(begin, srv.readyAt)
			r.PreviousExitToReadyMS = msBetween(previousExit, srv.readyAt)
			return srv, r
		}
		r.Failures = append(r.Failures, restartFailure{Attempt: attempt, ElapsedMS: msBetween(begin, time.Now()), Error: truncate(err.Error(), 300),
			LogTail: lastLogLine(filepath.Join(scfg.WorkDir, scfg.Label+".log"))})
		r.Error = err.Error()
		if ctx.Err() != nil || time.Now().Add(o.RestartBackoff).After(deadline) {
			return nil, r
		}
		select {
		case <-ctx.Done():
			return nil, r
		case <-time.After(o.RestartBackoff):
		}
	}
}

func lastLogLine(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	return truncate(lines[len(lines)-1], 300)
}

// waitQuiescent polls the realtime cursor until it is unchanged for o.Quiesce.
func waitQuiescent(ctx context.Context, addr string, o options) (bool, string) {
	deadline := time.Now().Add(o.DrainTimeout)
	c, err := dialClient(ctx, addr, o.User, o.Password, o.QueryTimout)
	if err != nil {
		return false, err.Error()
	}
	defer c.Close()
	var lastLoop, lastIndex int64 = -1, -1
	stableSince := time.Now()
	for time.Now().Before(deadline) {
		loop, index, err := c.latestCursor(ctx)
		if err != nil {
			return false, err.Error()
		}
		if loop != lastLoop || index != lastIndex {
			lastLoop, lastIndex, stableSince = loop, index, time.Now()
		} else if time.Since(stableSince) >= o.Quiesce {
			return true, ""
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err().Error()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return false, "drain timeout before the realtime cursor became stable"
}

func finishVerify(v *verifyReport, cmp *comparison, sched schedule, failed, sent *bitset, s sendReport) {
	v.Generated = sched.total
	v.SendFailed, v.NotSent = s.TxFailed, s.TxNotSent
	for i := uint64(0); i < sched.total; i++ {
		ok := sent.has(i)
		if ok {
			v.Expected++
			if i >= sched.steadyFrom {
				v.steadySent++
			}
		}
		found := cmp.found.has(i)
		if found && i >= sched.steadyFrom {
			v.SteadyFound++
		}
		switch {
		case ok && !found:
			v.MissingAfterSendOK++
			if len(v.MissingSamples) < 20 {
				v.MissingSamples = append(v.MissingSamples, cmp.codec.encode(i))
			}
		case !ok && found && failed.has(i):
			v.FoundButSendFailed++
		}
	}
}

func msBetween(a, b time.Time) float64 { return float64(b.Sub(a).Microseconds()) / 1000 }

type cpuTimes struct{ User, Sys float64 }

func selfCPU() cpuTimes {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return cpuTimes{}
	}
	tv := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
	return cpuTimes{tv(ru.Utime), tv(ru.Stime)}
}
func (a cpuTimes) sub(b cpuTimes) cpuTimes { return cpuTimes{a.User - b.User, a.Sys - b.Sys} }

func mergeCorpus(dst, src *corpusStats) {
	dst.XLogBytes.merge(&src.XLogBytes)
	dst.ProfileBytes.merge(&src.ProfileBytes)
	for k, v := range src.ProfileClass {
		dst.ProfileClass[k] += v
	}
	for k, v := range src.PackCount {
		dst.PackCount[k] += v
	}
	for k, v := range src.PackBytes {
		dst.PackBytes[k] += v
	}
	dst.ErrorXLogs += src.ErrorXLogs
	dst.TransactionsMade += src.TransactionsMade
}

func sortedKeys[M ~map[K]V, K int | string, V any](m M) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func aggregateSenders(rep *report, results []*senderResult, gcfg genConfig) {
	var lag histogram
	agg := newGenerator(gcfg, -1) // stats container only
	for _, r := range results {
		lag.merge(&r.Lag)
		rep.Send.TxSentOK += r.TxSent
		rep.Send.TxFailed += r.TxFailed
		rep.Send.TxNotSent += r.TxNotSent
		rep.Send.DatagramBytes += r.DatagramBytes
		rep.Send.DatagramsOK += r.DatagramsOK
		rep.Send.MaxDatagramBytes = max(rep.Send.MaxDatagramBytes, r.MaxDatagram)
		rep.Send.GenerateErrors += r.GenerateErrors
		if r.GenerateLastErr != "" {
			rep.Send.GenerateLastError = r.GenerateLastErr
		}
		if r.SocketError != "" {
			rep.Send.SocketErrors = append(rep.Send.SocketErrors, r.SocketError)
		}
		for k, v := range r.Datagrams {
			rep.Send.Datagrams[k] += v
			rep.Send.DatagramsTotal += v
		}
		for k, v := range r.SendErrors {
			rep.Send.SendErrors[k] += v
		}
		mergeCorpus(&agg.stats, &r.Gen.stats)
		for kind, p := range r.Gen.pools {
			t := rep.Payload.Text[kind]
			t.Hits += p.hits
			t.Misses += p.misses
			rep.Payload.Text[kind] = t
		}
	}
	rep.Send.Lag = lag.latency()
	rep.Send.Lag.Note = "actual datagram write completion - scheduled instant (coordinated-omission safe)"
	rep.Send.LagOver1s = lag.countAbove(int64(time.Second))
	rep.Payload.fill(&agg.stats)
}
