package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// requireLoopback accepts only literal loopback IP host:port (no DNS names).
func requireLoopback(hostport string) error {
	ap, err := netip.ParseAddrPort(hostport)
	if err != nil {
		return fmt.Errorf("address %q must be a literal IP:port: %w", hostport, err)
	}
	if !ap.Addr().Unmap().IsLoopback() {
		return fmt.Errorf("address %q is not loopback; replay targets loopback only", hostport)
	}
	return nil
}
func requireLoopbackIP(ip string) error {
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return fmt.Errorf("bind %q must be a literal IP: %w", ip, err)
	}
	if !a.Unmap().IsLoopback() {
		return fmt.Errorf("bind %q is not loopback; replay targets loopback only", ip)
	}
	return nil
}

type serverConfig struct {
	Binary, DB, WorkDir, Label string
	Bind, Timezone             string
	User, Password             string
	CompressXLog, CompressProf bool
	GoMaxProcs, GoMemLimit     string
	Extra                      []string // --server-arg passthrough, appended last
	StartTimeout               time.Duration
	RSSInterval                time.Duration
}

func (c serverConfig) args(readyFile string) []string {
	bind := c.Bind
	if strings.Contains(bind, ":") {
		bind = "[" + bind + "]"
	}
	a := []string{"--db", c.DB, "--udp", bind + ":0", "--tcp", bind + ":0", "--ready-file", readyFile,
		"--timezone", c.Timezone, "--user", c.User, "--password", c.Password,
		"--log-dir", filepath.Join(c.WorkDir, "logs"), "--plugin-dir", filepath.Join(c.WorkDir, "plugin"),
		"--telegraf-config", filepath.Join(c.WorkDir, "conf", "scouter-telegraf.xml"),
		// The tool stops the server with a signal; no <pid>.scouter file in
		// the working directory.
		"--exit-file=false"}
	if c.CompressXLog {
		a = append(a, "--compress-xlog")
	}
	if c.CompressProf {
		a = append(a, "--compress-profile")
	}
	return append(a, c.Extra...)
}

// childEnv inherits the tool environment except GOMAXPROCS/GOMEMLIMIT, which are
// set only when configured so the report states exactly what the child received.
func (c serverConfig) childEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "GOMAXPROCS=") || strings.HasPrefix(kv, "GOMEMLIMIT=") {
			continue
		}
		env = append(env, kv)
	}
	if c.GoMaxProcs != "" {
		env = append(env, "GOMAXPROCS="+c.GoMaxProcs)
	}
	if c.GoMemLimit != "" {
		env = append(env, "GOMEMLIMIT="+c.GoMemLimit)
	}
	return env
}

type endpoints struct {
	HTTP string `json:"http,omitempty"`
	UDP  string `json:"udp"`
	TCP  string `json:"tcp"`
}

// serverProc owns one child server, its stderr reader, waiter and RSS sampler.
// All three goroutines end when the process exits; stop() bounds that wait.
type serverProc struct {
	cfg        serverConfig
	cmd        *exec.Cmd
	logPath    string
	Endpoints  endpoints
	launchedAt time.Time
	readyAt    time.Time
	exited     chan struct{}
	waitErr    error
	exitedAt   time.Time
	mu         sync.Mutex
	statsLine  string
	logLines   uint64
	otherLines uint64
	otherLog   []string
	recovery   recoveryEvents
	rss        rssSampler
	samplerEnd chan struct{}
	samplerOut chan struct{}
	stopOnce   sync.Once
	stopped    stopResult
}

func startServer(ctx context.Context, cfg serverConfig) (*serverProc, error) {
	readyFile := filepath.Join(cfg.WorkDir, cfg.Label+".ready.json")
	os.Remove(readyFile)
	logPath := filepath.Join(cfg.WorkDir, cfg.Label+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(cfg.Binary, cfg.args(readyFile)...)
	cmd.Dir = cfg.WorkDir
	cmd.Env = cfg.childEnv()
	cmd.Stdout = logFile
	stderr, err := cmd.StderrPipe()
	if err != nil {
		logFile.Close()
		return nil, err
	}
	p := &serverProc{cfg: cfg, cmd: cmd, logPath: logPath, exited: make(chan struct{}), samplerEnd: make(chan struct{}), samplerOut: make(chan struct{})}
	p.launchedAt = time.Now()
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, err
	}
	readerDone := make(chan struct{})
	go func() { // stderr reader: ends at EOF (child exit)
		defer close(readerDone)
		p.copyStderr(stderr, logFile)
	}()
	go func() { // waiter: Wait only after the pipe is drained (os/exec contract)
		<-readerDone
		p.waitErr = cmd.Wait()
		p.exitedAt = time.Now()
		logFile.Close()
		close(p.exited)
	}()
	go func() { // RSS sampler
		defer close(p.samplerOut)
		p.rss.run(cmd.Process.Pid, cfg.RSSInterval, p.samplerEnd, p.exited)
	}()
	if err := p.waitReady(ctx, readyFile); err != nil {
		p.stop(5 * time.Second)
		return nil, fmt.Errorf("%s start: %w (log %s)", cfg.Label, err, logPath)
	}
	return p, nil
}

var statsLinePattern = regexp.MustCompile(`ingest=\{.*\} udp=\{.*\} tcp=\{.*\}`)

func (p *serverProc) copyStderr(r io.Reader, w io.Writer) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		fmt.Fprintln(w, line)
		p.mu.Lock()
		p.logLines++
		switch {
		case statsLinePattern.MatchString(line):
			p.statsLine = line
		case p.recovery.add(line):
			// Crash-repair events are recorded separately, not as errors.
		case !infoLogLine(line):
			p.otherLines++
			if len(p.otherLog) < 20 {
				p.otherLog = append(p.otherLog, truncate(line, 300))
			}
		}
		p.mu.Unlock()
	}
	io.Copy(w, r) // drain anything after an over-long line so the child never blocks
}

// recoveryEvents collects "storage recovery <kind> <path>: <detail>" lines the
// server logs when Open repairs crash residue (e.g. index-relinked).
type recoveryEvents struct {
	Count   uint64            `json:"count"`
	ByKind  map[string]uint64 `json:"by_kind"`
	Samples []string          `json:"samples"`
}

const recoveryPrefix = "storage recovery "

func (r *recoveryEvents) add(line string) bool {
	msg := logPrefix.ReplaceAllString(line, "")
	if !strings.HasPrefix(msg, recoveryPrefix) {
		return false
	}
	kind, _, _ := strings.Cut(strings.TrimPrefix(msg, recoveryPrefix), " ")
	if kind == "" {
		kind = "unknown"
	}
	if r.ByKind == nil {
		r.ByKind = map[string]uint64{}
	}
	r.Count++
	r.ByKind[kind]++
	if len(r.Samples) < 20 {
		r.Samples = append(r.Samples, truncate(line, 300))
	}
	return true
}
func (r recoveryEvents) clone() recoveryEvents {
	out := recoveryEvents{Count: r.Count, ByKind: map[string]uint64{}, Samples: append([]string{}, r.Samples...)}
	for k, v := range r.ByKind {
		out.ByKind[k] = v
	}
	return out
}

var writerTrouble = regexp.MustCompile(`(WriteErrors|WriteInvalid|Dropped|Abandoned):[1-9]`)

var logPrefix = regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(\.\d+)? `)

// infoLogLine reports server lines that are expected lifecycle information. Every
// other line (UDP/TCP operation errors, tick/sync/purge failures, panics) is counted.
func infoLogLine(line string) bool {
	msg := logPrefix.ReplaceAllString(line, "")
	switch {
	case strings.HasPrefix(msg, "Scouter Go listening "), strings.HasPrefix(msg, "retention enabled:"), strings.HasPrefix(msg, "pulse enabled:"), strings.HasPrefix(msg, "pulse="), strings.HasPrefix(msg, "stats "):
		return true
	case strings.HasPrefix(msg, "purge ") && !strings.HasPrefix(msg, "purge failed"):
		return true
	// Startup/shutdown reports of a server without scouter.conf or a GeoIP
	// file (both optional) and the final per-component counters.
	case strings.HasPrefix(msg, "config: ") && strings.HasSuffix(msg, " not found; using flags and defaults"),
		strings.HasPrefix(msg, "S145 GeoIP db file is not readable"),
		strings.HasPrefix(msg, "exit file ") && strings.HasSuffix(msg, ": delete it to stop the server"),
		strings.HasPrefix(msg, "sqltable={"), strings.HasPrefix(msg, "logger={"),
		strings.HasPrefix(msg, "storage writers closed: "),
		// The first-signal line of a graceful shutdown (replay sends SIGTERM).
		strings.HasPrefix(msg, "shutdown: SIGTERM received; graceful shutdown "),
		// Per-kind ingest writer totals: information only while no record
		// failed, was refused, dropped or abandoned.
		strings.HasPrefix(msg, "ingest writers closed: ") && !writerTrouble.MatchString(msg),
		// The replay server keeps retention off on purpose, and a Pulse
		// configuration may leave the application filters unset.
		strings.HasPrefix(msg, "WARNING: retention is off "),
		strings.HasPrefix(msg, "WARNING: pulse: ext_plugin_pulse_") && strings.Contains(msg, " is not set: "):
		return true
	}
	return false
}
func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func (p *serverProc) waitReady(ctx context.Context, readyFile string) error {
	deadline := time.Now().Add(p.cfg.StartTimeout)
	for {
		if b, err := os.ReadFile(readyFile); err == nil {
			var e endpoints
			if json.Unmarshal(b, &e) == nil && e.UDP != "" && e.TCP != "" {
				if err := requireLoopback(e.UDP); err != nil {
					return err
				}
				if err := requireLoopback(e.TCP); err != nil {
					return err
				}
				p.Endpoints = e
				p.readyAt = time.Now()
				return nil
			}
		}
		select {
		case <-p.exited:
			return fmt.Errorf("server exited before ready: %v", p.waitErr)
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return errors.New("ready-file timeout")
		}
	}
}

type stopResult struct {
	Signal       string
	SignalSentAt time.Time
	ShutdownMS   float64
	Killed       bool
	ExitCode     int
	WaitError    string
	// AlreadyExited means the child exited before SIGTERM (crash or early exit).
	AlreadyExited bool
}

// stop sends SIGTERM and waits up to timeout, then SIGKILL (recorded as Killed).
// It is idempotent with kill; later calls return the first result.
func (p *serverProc) stop(timeout time.Duration) stopResult {
	p.stopOnce.Do(func() { p.stopped = p.stopNow(syscall.SIGTERM, timeout) })
	return p.stopped
}

// kill sends SIGKILL (intentional crash for --stop=kill); Killed stays false
// because it is not a shutdown-timeout fallback, Signal records the request.
func (p *serverProc) kill(timeout time.Duration) stopResult {
	p.stopOnce.Do(func() { p.stopped = p.stopNow(syscall.SIGKILL, timeout) })
	return p.stopped
}
func (p *serverProc) stopNow(sig syscall.Signal, timeout time.Duration) stopResult {
	r := stopResult{Signal: sig.String()}
	r.SignalSentAt = time.Now()
	select {
	case <-p.exited:
		r.AlreadyExited = true
	default:
		p.cmd.Process.Signal(sig)
		select {
		case <-p.exited:
		case <-time.After(timeout):
			r.Killed = true
			p.cmd.Process.Kill()
			<-p.exited
		}
	}
	close(p.samplerEnd)
	<-p.samplerOut
	r.ShutdownMS = float64(p.exitedAt.Sub(r.SignalSentAt).Microseconds()) / 1000
	if p.cmd.ProcessState != nil {
		r.ExitCode = p.cmd.ProcessState.ExitCode()
	}
	if p.waitErr != nil {
		r.WaitError = p.waitErr.Error()
	}
	return r
}

func (p *serverProc) lastStatsLine() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.statsLine
}

// parseStatsLine parses the runtime shutdown line
// "ingest={Completed:1 ...} udp={Datagrams:...} tcp={...} deferredErrorLogs=0 droppedErrorLogs=0".
// Field names are taken from the line itself so additions are preserved.
// Booleans count as 1/0; string fields (names ending in Error, such as
// UDPStats.KernelCountersError, whose text may hold spaces) are skipped.
func parseStatsLine(line string) (map[string]map[string]int64, error) {
	out := map[string]map[string]int64{}
	group := regexp.MustCompile(`(\w+)=\{([^}]*)\}`)
	fieldName := regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)
	for _, m := range group.FindAllStringSubmatch(line, -1) {
		fields := map[string]int64{}
		inText := false
		for _, kv := range strings.Fields(m[2]) {
			k, v, ok := strings.Cut(kv, ":")
			if !ok || !fieldName.MatchString(k) {
				if inText {
					continue // the rest of a skipped string value
				}
				return nil, fmt.Errorf("stats field %q", kv)
			}
			inText = false
			if strings.HasSuffix(k, "Error") {
				inText = true
				continue
			}
			switch v {
			case "true":
				fields[k] = 1
				continue
			case "false":
				fields[k] = 0
				continue
			}
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("stats field %q: %w", kv, err)
			}
			fields[k] = n
		}
		out[m[1]] = fields
	}
	scalar := regexp.MustCompile(`\b(\w+)=(\d+)\b`)
	misc := map[string]int64{}
	for _, m := range scalar.FindAllStringSubmatch(line, -1) {
		n, _ := strconv.ParseInt(m[2], 10, 64)
		misc[m[1]] = n
	}
	if len(misc) > 0 {
		out["log"] = misc
	}
	for _, need := range []string{"ingest", "udp", "tcp"} {
		if _, ok := out[need]; !ok {
			return out, fmt.Errorf("stats line missing %s group", need)
		}
	}
	return out, nil
}

// rssSampler polls `ps -o rss= -p PID` (KiB on macOS and Linux).
type rssSampler struct {
	Samples   []rssSample
	Max       int64
	Errors    uint64
	LastError string
	Dropped   uint64
}
type rssSample struct {
	OffsetMS int64 `json:"t_ms"`
	KiB      int64 `json:"rss_kib"`
}

const maxRSSSamples = 7200

func (s *rssSampler) run(pid int, interval time.Duration, stop, exited <-chan struct{}) {
	if interval <= 0 {
		return
	}
	start := time.Now()
	t := time.NewTicker(interval)
	defer t.Stop()
	sample := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
		if err != nil {
			s.Errors++
			s.LastError = err.Error()
			return
		}
		kib, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
		if err != nil {
			s.Errors++
			s.LastError = err.Error()
			return
		}
		s.Max = max(s.Max, kib)
		if len(s.Samples) < maxRSSSamples {
			s.Samples = append(s.Samples, rssSample{time.Since(start).Milliseconds(), kib})
		} else {
			s.Dropped++
		}
	}
	sample()
	for {
		select {
		case <-stop:
			return
		case <-exited:
			return
		case <-t.C:
			sample()
		}
	}
}

type processReport struct {
	Label            string                      `json:"label"`
	Args             []string                    `json:"args"`
	LogPath          string                      `json:"log_path"`
	Endpoints        endpoints                   `json:"endpoints"`
	StartToReadyMS   float64                     `json:"start_to_ready_ms"`
	ShutdownMS       float64                     `json:"sigterm_to_exit_ms"`
	Killed           bool                        `json:"killed_after_timeout"`
	ExitedEarly      bool                        `json:"exited_before_sigterm"`
	ExitCode         int                         `json:"exit_code"`
	WaitError        string                      `json:"wait_error,omitempty"`
	StopSignal       string                      `json:"stop_signal"`
	LogLines         uint64                      `json:"log_lines"`
	ErrorLogLines    uint64                      `json:"non_info_log_lines"`
	ErrorLogSamples  []string                    `json:"non_info_log_samples"`
	RecoveryEvents   recoveryEvents              `json:"recovery_events"`
	StatsLine        string                      `json:"stats_line"`
	Stats            map[string]map[string]int64 `json:"stats"`
	StatsParseError  string                      `json:"stats_parse_error,omitempty"`
	CPUUserSec       *float64                    `json:"cpu_user_seconds"`
	CPUSysSec        *float64                    `json:"cpu_system_seconds"`
	MaxRSSBytes      *int64                      `json:"rusage_max_rss_bytes"`
	InBlock          *int64                      `json:"rusage_inblock"`
	OutBlock         *int64                      `json:"rusage_oublock"`
	RusageNote       string                      `json:"rusage_note"`
	RSSSampledMaxKiB *int64                      `json:"rss_sampled_max_kib"`
	RSSSamples       []rssSample                 `json:"rss_samples"`
	RSSSampleErrors  uint64                      `json:"rss_sample_errors"`
	RSSNote          string                      `json:"rss_note,omitempty"`
}

func (p *serverProc) report(stop stopResult) processReport {
	r := processReport{Label: p.cfg.Label, Args: redactArgs(p.cmd.Args[1:]), LogPath: p.logPath, Endpoints: p.Endpoints,
		StartToReadyMS: float64(p.readyAt.Sub(p.launchedAt).Microseconds()) / 1000, ShutdownMS: stop.ShutdownMS,
		Killed: stop.Killed, ExitedEarly: stop.AlreadyExited, ExitCode: stop.ExitCode, WaitError: stop.WaitError, StatsLine: p.lastStatsLine(), StopSignal: stop.Signal}
	p.mu.Lock()
	r.LogLines, r.ErrorLogLines, r.ErrorLogSamples = p.logLines, p.otherLines, append([]string(nil), p.otherLog...)
	r.RecoveryEvents = p.recovery.clone()
	p.mu.Unlock()
	if r.StatsLine == "" && stop.Signal == syscall.SIGKILL.String() && !stop.AlreadyExited {
		r.StatsParseError = "no stats line: SIGKILL by --stop=kill (expected)"
	} else if r.StatsLine == "" {
		r.StatsParseError = "no ingest/udp/tcp stats line on stderr (killed or crashed?)"
	} else if stats, err := parseStatsLine(r.StatsLine); err != nil {
		r.Stats, r.StatsParseError = stats, err.Error()
	} else {
		r.Stats = stats
	}
	if ps := p.cmd.ProcessState; ps != nil {
		u, s := ps.UserTime().Seconds(), ps.SystemTime().Seconds()
		r.CPUUserSec, r.CPUSysSec = &u, &s
		if ru, ok := ps.SysUsage().(*syscall.Rusage); ok && ru != nil {
			maxrss := int64(ru.Maxrss)
			if runtime.GOOS != "darwin" {
				maxrss *= 1024 // Linux/BSD report KiB; Darwin reports bytes
			}
			in, out := int64(ru.Inblock), int64(ru.Oublock)
			r.MaxRSSBytes, r.InBlock, r.OutBlock = &maxrss, &in, &out
			r.RusageNote = "wait4 rusage of the server process; inblock/oublock are filesystem block operations, page-cache hits are not counted"
		} else {
			r.RusageNote = "rusage unavailable on " + runtime.GOOS
		}
	}
	r.RSSSamples, r.RSSSampleErrors = p.rss.Samples, p.rss.Errors
	if len(p.rss.Samples) > 0 {
		v := p.rss.Max
		r.RSSSampledMaxKiB = &v
	} else {
		r.RSSNote = "no RSS sample: " + p.rss.LastError
	}
	if p.rss.Dropped > 0 {
		r.RSSNote = fmt.Sprintf("%d samples beyond %d not stored (max still tracked)", p.rss.Dropped, maxRSSSamples)
	}
	return r
}

func redactArgs(args []string) []string {
	out := append([]string(nil), args...)
	for i := 0; i+1 < len(out); i++ {
		if out[i] == "--password" {
			out[i+1] = "<redacted>"
		}
	}
	return out
}
