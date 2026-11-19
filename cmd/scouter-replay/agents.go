package main

// `scouter-replay agents` emulates a fleet of Java Agent TCP sessions against a
// running server (loopback only) and samples the server while it holds them:
// connection-scale, restart-storm, connection-budget and fd-limit scenarios.
// It never starts or stops the server; scripts/connection-scale.sh does.
//
// Java Agent TCP behaviour emulated (Scouter dee47fb5):
//   - TcpRequestMgr.run (agent.java .../net/TcpRequestMgr.java:29-56): a loop
//     that sleeps 1000ms, then opens net_collector_tcp_session_count
//     sessions while TcpWorker.LIVE is short; a failed connect sleeps 3000ms
//     more. So a dropped session is retried on the agent's next 1s tick, a
//     refused or timed-out connect 4s later.
//   - TcpWorker.prepare (TcpWorker.java:44-63): Socket.connect with
//     net_collector_tcp_connection_timeout_ms (3000), then setSoTimeout
//     net_collector_tcp_so_timeout_ms (60000) for every read.
//   - TcpWorker.processV2 (TcpWorker.java:93-134, Java agent default):
//     writeInt(TCP_AGENT_V2) writeInt(objHash) flush; then per command
//     readIntBytes -> Text cmd + Pack param, answer [0x03 IntBytes(Pack)] 0x04.
//     TcpWorker.process (TcpWorker.java:65-91, host agent default, V1):
//     TCP_AGENT, Text cmd + Pack param read directly, answer [0x03 Pack] 0x04.
//   - ReqestHandlingProxy.process (ReqestHandlingProxy.java:100-102):
//     KEEP_ALIVE returns null, so the answer is a bare 0x04.
//   - Any read/write exception closes the socket (TcpWorker.run finally).

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Java NetCafe TCP magics of an Agent session.
const (
	tcpAgentV1Magic uint32 = 0xcafe1001
	tcpAgentV2Magic uint32 = 0xcafe1002
)

type agentsOptions struct {
	TCP, UDP, HTTP string
	PID            int
	Report         string
	User, Password string

	Agents, SessionCount int
	V1Ratio              float64
	NamePrefix           string
	Seed                 uint64
	Duration, Spread     time.Duration
	ConnectTimeout       time.Duration
	SoTimeout            time.Duration
	RetryInterval        time.Duration
	FailPause            time.Duration

	HeartbeatInterval, CounterInterval time.Duration
	XLogRate                           float64

	Clients        int
	ClientInterval time.Duration
	ClientTimeout  time.Duration
	ProbeInterval  time.Duration

	RelayClients int
	RelayCommand string
	RelayAt      time.Duration
	AgentDelay   time.Duration
	RelayTimeout time.Duration

	SampleInterval time.Duration
	// Settle is how long a session must stay open before it counts as
	// settled: a budget reject or pool-full close arrives within it.
	Settle time.Duration
	// PIDFile is re-read at every sample, so a restarted server is followed.
	PIDFile string
}

func parseAgentsOptions(args []string) (agentsOptions, error) {
	var o agentsOptions
	fs := flag.NewFlagSet("scouter-replay agents", flag.ContinueOnError)
	fs.StringVar(&o.TCP, "tcp", "127.0.0.1:6100", "server TCP IP:port (loopback)")
	fs.StringVar(&o.UDP, "udp", "", "server UDP IP:port (loopback); empty sends no UDP")
	fs.StringVar(&o.HTTP, "http", "", "server --http IP:port for /health and pprof goroutine samples (loopback); empty disables")
	fs.IntVar(&o.PID, "pid", 0, "server PID for /proc RSS, threads, fd and CPU samples (Linux); 0 disables. Optional when --pid-file is used")
	fs.StringVar(&o.PIDFile, "pid-file", "", "re-read the server PID from this file at every sample (follows a restarted server)")
	fs.StringVar(&o.Report, "report", "agents-report.json", "report JSON path")
	fs.StringVar(&o.User, "user", "admin", "Client login account")
	fs.StringVar(&o.Password, "password", "admin", "Client login password")
	fs.IntVar(&o.Agents, "agents", 100, "number of agent objects (distinct objHash)")
	fs.IntVar(&o.SessionCount, "session-count", 1, "TCP sessions per agent (Java net_collector_tcp_session_count)")
	fs.Float64Var(&o.V1Ratio, "v1-ratio", 0.2, "fraction of agents using TCP_AGENT (V1, host agent); the rest use TCP_AGENT_V2 (Java agent)")
	fs.StringVar(&o.NamePrefix, "name-prefix", "/conn-scale", "object name prefix; names are <prefix>-hostNNN/agentNNNNN")
	fs.Uint64Var(&o.Seed, "seed", 1, "seed for retry phases and payloads")
	fs.DurationVar(&o.Duration, "duration", time.Minute, "run time from the first connect")
	fs.DurationVar(&o.Spread, "spread", 0, "spread the agents' first connect over this time (0: all within the first retry interval, a restart storm)")
	fs.DurationVar(&o.ConnectTimeout, "connect-timeout", 3*time.Second, "Java net_collector_tcp_connection_timeout_ms")
	fs.DurationVar(&o.SoTimeout, "so-timeout", 60*time.Second, "Java net_collector_tcp_so_timeout_ms: per-read limit while waiting for a server command")
	fs.DurationVar(&o.RetryInterval, "retry-interval", time.Second, "Java TcpRequestMgr loop sleep")
	fs.DurationVar(&o.FailPause, "fail-pause", 3*time.Second, "Java TcpRequestMgr extra sleep after a failed connect")
	fs.DurationVar(&o.HeartbeatInterval, "heartbeat-interval", 2*time.Second, "UDP ObjectPack interval per agent")
	fs.DurationVar(&o.CounterInterval, "counter-interval", 2*time.Second, "UDP PerfCounterPack interval per agent")
	fs.Float64Var(&o.XLogRate, "xlog-rate", 0, "UDP XLogs per second over all agents")
	fs.IntVar(&o.Clients, "clients", 0, "Client query workers (persistent session: OBJECT_LIST_REAL_TIME, TRANX_REAL_TIME_GROUP_LATEST)")
	fs.DurationVar(&o.ClientInterval, "client-interval", time.Second, "per query worker interval")
	fs.DurationVar(&o.ClientTimeout, "client-timeout", 8*time.Second, "Client request timeout (Scouter Client so-timeout 8s)")
	fs.DurationVar(&o.ProbeInterval, "probe-interval", 0, "open a new Client connection, LOGIN and CLOSE this often (budget probe); 0 disables")
	fs.IntVar(&o.RelayClients, "relay-clients", 0, "Client connections that each send one --relay-command to a distinct agent at --relay-at")
	fs.StringVar(&o.RelayCommand, "relay-command", "OBJECT_ENV", "Client command relayed to an agent session")
	fs.DurationVar(&o.RelayAt, "relay-at", 10*time.Second, "relay start, after the first connect")
	fs.DurationVar(&o.AgentDelay, "agent-delay", 0, "agents answer every non-KEEP_ALIVE command this late")
	fs.DurationVar(&o.RelayTimeout, "relay-timeout", 8*time.Second, "relay Client gives up (closes its socket) after this")
	fs.DurationVar(&o.SampleInterval, "sample-interval", time.Second, "server and fleet sample interval")
	fs.DurationVar(&o.Settle, "settle", time.Second, "a session open this long counts as settled (full/episode bookkeeping); rejects end sooner")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() != 0 {
		return o, fmt.Errorf("unexpected arguments %q", fs.Args())
	}
	return o, o.validate()
}

func (o agentsOptions) validate() error {
	var errs []error
	check := func(ok bool, format string, a ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, a...))
		}
	}
	errs = append(errs, requireLoopback(o.TCP))
	if o.UDP != "" {
		errs = append(errs, requireLoopback(o.UDP))
	}
	if o.HTTP != "" {
		errs = append(errs, requireLoopback(o.HTTP))
	}
	check(o.Agents >= 1 && o.Agents <= 100000, "--agents must be 1..100000")
	check(o.SessionCount >= 1 && o.SessionCount <= 16, "--session-count must be 1..16")
	check(o.V1Ratio >= 0 && o.V1Ratio <= 1, "--v1-ratio must be 0..1")
	check(o.Duration > 0 && o.Spread >= 0, "--duration must be > 0, --spread >= 0")
	check(o.ConnectTimeout > 0 && o.SoTimeout > 0 && o.RetryInterval > 0 && o.FailPause >= 0, "connect/so timeouts and --retry-interval must be > 0, --fail-pause >= 0")
	check(o.HeartbeatInterval > 0 && o.CounterInterval > 0 && o.XLogRate >= 0, "UDP intervals must be > 0, --xlog-rate >= 0")
	check(o.Clients >= 0 && o.Clients <= 64 && o.ClientInterval > 0 && o.ClientTimeout > 0, "--clients must be 0..64 with positive interval/timeout")
	check(o.ProbeInterval >= 0, "--probe-interval must be >= 0")
	check(o.RelayClients >= 0 && o.RelayClients <= o.Agents && o.RelayAt >= 0 && o.AgentDelay >= 0 && o.RelayTimeout > 0, "--relay-clients must be 0..agents; relay times >= 0")
	check(o.SampleInterval > 0 && o.Settle > 0, "--sample-interval and --settle must be > 0")
	return errors.Join(errs...)
}

// lockedHist is a histogram shared by goroutines (values in nanoseconds).
type lockedHist struct {
	mu sync.Mutex
	h  histogram
}

func (l *lockedHist) record(d time.Duration) { l.mu.Lock(); l.h.record(int64(d)); l.mu.Unlock() }
func (l *lockedHist) summary() latencySummary {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.h.latency()
}

// classCounts counts events by a short error class.
type classCounts struct {
	mu sync.Mutex
	m  map[string]uint64
}

func (c *classCounts) add(class string) {
	c.mu.Lock()
	if c.m == nil {
		c.m = map[string]uint64{}
	}
	c.m[class]++
	c.mu.Unlock()
}
func (c *classCounts) snapshot() map[string]uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]uint64, len(c.m))
	for k, v := range c.m {
		out[k] = v
	}
	return out
}
func (c *classCounts) total() uint64 {
	var n uint64
	for _, v := range c.snapshot() {
		n += v
	}
	return n
}

// netClass names a network error the way an agent would experience it.
func netClass(err error) string {
	var ne net.Error
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "eof"
	case errors.Is(err, syscall.ECONNRESET):
		return "reset"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused"
	case errors.Is(err, syscall.EPIPE):
		return "broken_pipe"
	case errors.Is(err, syscall.EMFILE), errors.Is(err, syscall.ENFILE):
		return "local_fd_limit"
	case errors.Is(err, syscall.EADDRNOTAVAIL):
		return "local_ports"
	case errors.Is(err, net.ErrClosed), errors.Is(err, context.Canceled):
		return "local_close"
	case errors.As(err, &ne) && ne.Timeout():
		return "timeout"
	case errors.Is(err, errInvalidSession):
		return "invalid_session"
	}
	return "other"
}

type agentSpec struct {
	Index   int
	Session int
	Hash    int32
	V1      bool
	// Phase is the agent's position inside its 1s TcpRequestMgr tick.
	Phase time.Duration
}

type fleet struct {
	o     agentsOptions
	start time.Time

	// connected counts open sessions (the agent's TcpWorker.LIVE view);
	// settled those open for at least --settle.
	connected, settled atomic.Int64
	everConnected      atomic.Int64
	attempts           atomic.Uint64
	connectOK          atomic.Uint64
	connectFail        classCounts
	// Sessions that ended before the server sent a command (a budget
	// reject, an Agent pool full close or a dying server) vs. after.
	endedEarly, endedLater classCounts
	keepalives, commands   atomic.Uint64
	responses, writeErrors atomic.Uint64
	keepGap, connectLat    lockedHist
	firstConnect           lockedHist
	sessionLife            lockedHist
	// worstGap is the largest KEEP_ALIVE gap seen; agents time out at --so-timeout.
	worstGap atomic.Int64
}

func (f *fleet) specs() []agentSpec {
	rng := rand.New(rand.NewPCG(f.o.Seed, 0x5eed))
	v1 := int(float64(f.o.Agents)*f.o.V1Ratio + 0.5)
	out := make([]agentSpec, 0, f.o.Agents*f.o.SessionCount)
	for i := 0; i < f.o.Agents; i++ {
		hash := javaHash(agentName(f.o.NamePrefix, i))
		phase := time.Duration(rng.Int64N(int64(f.o.RetryInterval)))
		if f.o.Spread > 0 {
			phase += time.Duration(int64(f.o.Spread) * int64(i) / int64(f.o.Agents))
		}
		for s := 0; s < f.o.SessionCount; s++ {
			out = append(out, agentSpec{Index: i, Session: s, Hash: hash, V1: i < v1, Phase: phase})
		}
	}
	return out
}

func agentName(prefix string, i int) string {
	return fmt.Sprintf("%s-host%03d/agent%05d", prefix, i/32, i)
}

// nextTick is the first instant >= t on the agent's retry grid (start+phase+k*retry).
func nextTick(start time.Time, phase, retry time.Duration, t time.Time) time.Time {
	base := start.Add(phase)
	if !t.After(base) {
		return base
	}
	k := (t.Sub(base) + retry - 1) / retry
	return base.Add(k * retry)
}

func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// runAgent is TcpRequestMgr + TcpWorker for one session slot of one agent.
func (f *fleet) runAgent(ctx context.Context, a agentSpec) {
	dialer := net.Dialer{Timeout: f.o.ConnectTimeout}
	next := nextTick(f.start, a.Phase, f.o.RetryInterval, f.start)
	var first atomic.Bool
	first.Store(true)
	for sleepUntil(ctx, next) {
		f.attempts.Add(1)
		t0 := time.Now()
		conn, err := dialer.DialContext(ctx, "tcp", f.o.TCP)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			f.connectFail.add(netClass(err))
			// prepare() false: sleep 3000, then the loop's own 1000.
			next = nextTick(f.start, a.Phase, f.o.RetryInterval, time.Now().Add(f.o.FailPause))
			continue
		}
		f.connectOK.Add(1)
		f.connectLat.record(time.Since(t0))
		f.session(ctx, conn, a, &first)
		next = nextTick(f.start, a.Phase, f.o.RetryInterval, time.Now())
	}
}

// session runs one connected TcpWorker until the socket fails or ctx ends.
func (f *fleet) session(ctx context.Context, conn net.Conn, a agentSpec, first *atomic.Bool) {
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	defer conn.Close()
	r, w := bufio.NewReaderSize(conn, 4096), bufio.NewWriterSize(conn, 4096)
	magic := tcpAgentV2Magic
	if a.V1 {
		magic = tcpAgentV1Magic
	}
	var hello [8]byte
	binary.BigEndian.PutUint32(hello[:4], magic)
	binary.BigEndian.PutUint32(hello[4:], uint32(a.Hash))
	started := time.Now()
	gotCommand := false
	end := func(err error) {
		class := netClass(err)
		if ctx.Err() != nil {
			class = "local_close"
		}
		if gotCommand {
			f.endedLater.add(class)
		} else {
			f.endedEarly.add(class)
		}
		f.sessionLife.record(time.Since(started))
	}
	conn.SetWriteDeadline(time.Now().Add(f.o.SoTimeout))
	if _, err := conn.Write(hello[:]); err != nil {
		end(err)
		return
	}
	f.connected.Add(1)
	defer f.connected.Add(-1)
	connectedAfter := time.Since(f.start)
	// state: 0 open, 1 settled, 2 ended before settling. The gauge is
	// raised before the state flips, so it never dips below the truth.
	var state atomic.Int32
	time.AfterFunc(f.o.Settle, func() {
		f.settled.Add(1)
		if !state.CompareAndSwap(0, 1) {
			f.settled.Add(-1)
			return
		}
		if first.CompareAndSwap(true, false) {
			f.everConnected.Add(1)
			f.firstConnect.record(connectedAfter)
		}
	})
	defer func() {
		if !state.CompareAndSwap(0, 2) {
			f.settled.Add(-1)
		}
	}()
	lastKeep := started
	for {
		conn.SetReadDeadline(time.Now().Add(f.o.SoTimeout))
		cmd, err := readAgentCommand(r, a.V1)
		if err != nil {
			end(err)
			return
		}
		gotCommand = true
		now := time.Now()
		var answer protocol.Pack
		if cmd == "KEEP_ALIVE" {
			gap := now.Sub(lastKeep)
			lastKeep = now
			f.keepalives.Add(1)
			f.keepGap.record(gap)
			for {
				old := f.worstGap.Load()
				if int64(gap) <= old || f.worstGap.CompareAndSwap(old, int64(gap)) {
					break
				}
			}
		} else {
			f.commands.Add(1)
			if f.o.AgentDelay > 0 && !sleepUntil(ctx, now.Add(f.o.AgentDelay)) {
				end(context.Canceled)
				return
			}
			answer = &protocol.MapPack{Entries: []protocol.Entry{{Key: "agent", Value: protocol.Value{Type: protocol.ValueText, Text: agentName(f.o.NamePrefix, a.Index)}}}}
		}
		conn.SetWriteDeadline(time.Now().Add(f.o.SoTimeout))
		if err := writeAgentAnswer(w, a.V1, answer); err != nil {
			if ctx.Err() == nil {
				f.writeErrors.Add(1)
			}
			end(err)
			return
		}
		if answer != nil {
			f.responses.Add(1)
		}
	}
}

// readJavaBlob reads DataInputX.readBlob: 1-byte length (<=253), 255 + u16,
// or 254 + i32.
func readJavaBlob(r *bufio.Reader, limit int) ([]byte, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	n := int(b)
	switch b {
	case 255:
		var x [2]byte
		if _, err := io.ReadFull(r, x[:]); err != nil {
			return nil, err
		}
		n = int(binary.BigEndian.Uint16(x[:]))
	case 254:
		var x [4]byte
		if _, err := io.ReadFull(r, x[:]); err != nil {
			return nil, err
		}
		n = int(int32(binary.BigEndian.Uint32(x[:])))
	}
	if n < 0 || n > limit {
		return nil, fmt.Errorf("blob length %d out of range", n)
	}
	out := make([]byte, n)
	_, err = io.ReadFull(r, out)
	return out, err
}

const agentCommandLimit = 1 << 20

// readAgentCommand reads one server command (Text cmd, Pack param) and
// returns the command name; the parameter is decoded and dropped.
func readAgentCommand(r *bufio.Reader, v1 bool) (string, error) {
	if v1 {
		cmd, err := readJavaBlob(r, agentCommandLimit)
		if err != nil {
			return "", err
		}
		if _, err := protocol.ReadPack(r, protocol.Limits{}); err != nil {
			return "", err
		}
		return string(cmd), nil
	}
	var x [4]byte
	if _, err := io.ReadFull(r, x[:]); err != nil {
		return "", err
	}
	n := int(int32(binary.BigEndian.Uint32(x[:])))
	if n < 0 || n > agentCommandLimit {
		return "", fmt.Errorf("command frame length %d out of range", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	d := protocol.NewDecoder(buf, protocol.Limits{})
	cmd := d.Text()
	d.Pack()
	return cmd, d.Err()
}

// writeAgentAnswer writes [HasNEXT Pack] NoNEXT and flushes. V2 wraps the Pack
// in writeIntBytes.
func writeAgentAnswer(w *bufio.Writer, v1 bool, answer protocol.Pack) error {
	if answer != nil {
		body, err := protocol.EncodePack(answer, protocol.Limits{})
		if err != nil {
			return err
		}
		w.WriteByte(flagHasNext)
		if !v1 {
			var x [4]byte
			binary.BigEndian.PutUint32(x[:], uint32(len(body)))
			w.Write(x[:])
		}
		w.Write(body)
	}
	w.WriteByte(flagNoNext)
	return w.Flush()
}

// udpStats are the agent UDP sends (heartbeat, counter, XLog).
type udpStats struct {
	Datagrams, Bytes, Packs, XLogs, Errors atomic.Uint64
	lastErr                                atomic.Value
}

// runUDP sends each agent's ObjectPack and PerfCounterPack at their intervals,
// spread evenly, plus --xlog-rate XLogs, in CAFN batches like DataUdpAgent.
func (f *fleet) runUDP(ctx context.Context, objects []object, st *udpStats) {
	addr, err := net.ResolveUDPAddr("udp", f.o.UDP)
	if err != nil {
		st.lastErr.Store(err.Error())
		return
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		st.lastErr.Store(err.Error())
		return
	}
	defer conn.Close()
	conn.SetWriteBuffer(4 << 20)
	g := newGenerator(genConfig{Seed: f.o.Seed, Objects: objects, TextHitRatio: 0.99, TextPool: 1000, TextCardinality: 1000, ErrorRatio: 0.01}, 0)
	var pkid int64
	b := &batcher{maxBytes: 60000, maxPacks: 32, nextPKID: func() int64 { pkid++; return pkid }, emit: func(d datagram) {
		if _, err := conn.Write(d.Bytes); err != nil {
			st.Errors.Add(1)
			st.lastErr.Store(err.Error())
			return
		}
		st.Datagrams.Add(1)
		st.Bytes.Add(uint64(len(d.Bytes)))
	}}
	add := func(p pack, err error) {
		if err != nil {
			st.Errors.Add(1)
			st.lastErr.Store(err.Error())
			return
		}
		st.Packs.Add(1)
		if err := b.add(p); err != nil {
			st.Errors.Add(1)
		}
	}
	n := float64(len(objects))
	var heartbeats, counters, xlogs uint64
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			b.flush()
			return
		case <-tick.C:
		}
		el := time.Since(f.start).Seconds()
		now := time.Now().UnixMilli()
		for due := uint64(el * n / f.o.HeartbeatInterval.Seconds()); heartbeats < due; heartbeats++ {
			add(g.heartbeat(objects[heartbeats%uint64(len(objects))]))
		}
		for due := uint64(el * n / f.o.CounterInterval.Seconds()); counters < due; counters++ {
			add(g.counter(objects[counters%uint64(len(objects))], now))
		}
		for due := uint64(el * f.o.XLogRate); xlogs < due; xlogs++ {
			packs, err := g.transaction(xlogs, now)
			if err != nil {
				add(pack{}, err)
				continue
			}
			for _, p := range packs {
				add(p, nil)
			}
			st.XLogs.Add(1)
		}
		b.flush()
	}
}

type opStats struct {
	OK      atomic.Uint64
	Fail    classCounts
	Latency lockedHist
	// LastErr is the most recent failure message (for the report).
	lastErr atomic.Value
}

func (s *opStats) done(t0 time.Time, err error) {
	if err != nil {
		s.Fail.add(netClass(err))
		s.lastErr.Store(truncate(err.Error(), 300))
		return
	}
	s.OK.Add(1)
	s.Latency.record(time.Since(t0))
}

type opReport struct {
	OK        uint64            `json:"ok"`
	Fail      map[string]uint64 `json:"fail"`
	Latency   latencySummary    `json:"latency"`
	LastError string            `json:"last_error,omitempty"`
}

func (s *opStats) report() opReport {
	r := opReport{OK: s.OK.Load(), Fail: s.Fail.snapshot(), Latency: s.Latency.summary()}
	if v, ok := s.lastErr.Load().(string); ok {
		r.LastError = v
	}
	return r
}

type clientStats struct {
	objectList, latest, dial opStats
	lastObjects, lastAlive   atomic.Int64
}

// runClient is one Scouter Client query worker with a persistent session.
func (f *fleet) runClient(ctx context.Context, st *clientStats) {
	var c *client
	defer func() {
		if c != nil {
			c.Close()
		}
	}()
	tick := time.NewTicker(f.o.ClientInterval)
	defer tick.Stop()
	for {
		if c == nil {
			t0 := time.Now()
			var err error
			c, err = dialClient(ctx, f.o.TCP, f.o.User, f.o.Password, f.o.ClientTimeout)
			if ctx.Err() != nil {
				return
			}
			st.dial.done(t0, err)
		}
		if c != nil {
			t0 := time.Now()
			objects, alive := int64(0), int64(0)
			err := c.call(ctx, "OBJECT_LIST_REAL_TIME", nil, func(p protocol.Pack) error {
				if o, ok := p.(*protocol.ObjectPack); ok {
					objects++
					if o.Alive {
						alive++
					}
				}
				return nil
			})
			if ctx.Err() != nil {
				return
			}
			st.objectList.done(t0, err)
			if err == nil {
				st.lastObjects.Store(objects)
				st.lastAlive.Store(alive)
				t0 = time.Now()
				_, _, err = c.latestCursor(ctx)
				if ctx.Err() != nil {
					return
				}
				st.latest.done(t0, err)
			}
			if err != nil {
				c.conn.Close()
				c = nil
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// runProbe opens a fresh Client connection (magic, LOGIN, CLOSE) every interval.
func (f *fleet) runProbe(ctx context.Context, st *opStats) {
	tick := time.NewTicker(f.o.ProbeInterval)
	defer tick.Stop()
	for {
		t0 := time.Now()
		c, err := dialClient(ctx, f.o.TCP, f.o.User, f.o.Password, f.o.ClientTimeout)
		if ctx.Err() != nil {
			return
		}
		st.done(t0, err)
		if c != nil {
			c.Close()
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// runRelay sends one relayed command for an agent and gives up after
// --relay-timeout by closing the socket, like a Client whose read timed out.
func (f *fleet) runRelay(ctx context.Context, hash int32, st *opStats) {
	t0 := time.Now()
	c, err := dialClient(ctx, f.o.TCP, f.o.User, f.o.Password, f.o.ClientTimeout)
	if err != nil {
		st.done(t0, fmt.Errorf("dial: %w", err))
		return
	}
	defer c.conn.Close()
	c.timeout = f.o.RelayTimeout
	t0 = time.Now()
	err = c.call(ctx, f.o.RelayCommand, &protocol.MapPack{Entries: []protocol.Entry{{Key: "objHash", Value: decimal(int64(hash))}}}, func(protocol.Pack) error { return nil })
	st.done(t0, err)
}

// serverSample is one look at the server: /health, pprof goroutines, /proc.
type serverSample struct {
	At        time.Time `json:"at"`
	ElapsedMS float64   `json:"elapsed_ms"`
	// Fleet side.
	Connected     int64             `json:"sessions_open"`
	Settled       int64             `json:"sessions_settled"`
	Attempts      uint64            `json:"connect_attempts"`
	ConnectFail   uint64            `json:"connect_fail"`
	EndedEarly    uint64            `json:"sessions_ended_before_command"`
	EndedLater    uint64            `json:"sessions_ended_after_command"`
	Keepalives    uint64            `json:"keepalives"`
	TCP           map[string]int64  `json:"tcp,omitempty"`
	AgentOrphans  map[string]int64  `json:"agent_orphans,omitempty"`
	Stats         *healthStats      `json:"stats,omitempty"`
	Goroutines    int64             `json:"goroutines,omitempty"`
	Proc          *procSample       `json:"proc,omitempty"`
	UDP           map[string]uint64 `json:"udp_sent,omitempty"`
	HealthError   string            `json:"health_error,omitempty"`
	ProcError     string            `json:"proc_error,omitempty"`
	GoroutineErr  string            `json:"goroutine_error,omitempty"`
	ServerPID     int               `json:"server_pid,omitempty"`
	ClientObjects int64             `json:"client_objects,omitempty"`
	ClientAlive   int64             `json:"client_alive,omitempty"`
}

type healthStats struct {
	Goroutines uint64  `json:"goroutines"`
	RSSBytes   uint64  `json:"rssBytes"`
	HeapBytes  uint64  `json:"heapBytes"`
	Agents     int     `json:"agents"`
	TCPActive  int64   `json:"tcpActive"`
	TCPErrors  uint64  `json:"tcpErrors"`
	XLogsPerS  float64 `json:"xlogsPerSec"`
	PacksPerS  float64 `json:"packsPerSec"`
	UDPRejects uint64  `json:"udpRejected"`
	Ingest     struct {
		Count uint64 `json:"count"`
		P50Us uint64 `json:"p50Us"`
		P99Us uint64 `json:"p99Us"`
		MaxUs uint64 `json:"maxUs"`
	} `json:"ingestLatency"`
	Client struct {
		Count uint64 `json:"count"`
		P99Us uint64 `json:"p99Us"`
		MaxUs uint64 `json:"maxUs"`
	} `json:"clientLatency"`
}

type procSample struct {
	RSSKiB   int64   `json:"rss_kib"`
	Threads  int64   `json:"threads"`
	FDs      int64   `json:"fds"`
	CPUTicks int64   `json:"cpu_ticks"`
	CPUPct   float64 `json:"cpu_percent"`
}

type sampler struct {
	f       *fleet
	http    *http.Client
	pid     int
	pidFile string
	prev    *procSample
	prevAt  time.Time
	prevPID int
	udp     *udpStats
	clients *clientStats
}

func (s *sampler) sample(ctx context.Context) serverSample {
	now := time.Now()
	out := serverSample{At: now, ElapsedMS: msBetween(s.f.start, now), Connected: s.f.connected.Load(), Settled: s.f.settled.Load(), Attempts: s.f.attempts.Load(),
		ConnectFail: s.f.connectFail.total(), EndedEarly: s.f.endedEarly.total(), EndedLater: s.f.endedLater.total(), Keepalives: s.f.keepalives.Load()}
	if s.udp != nil {
		out.UDP = map[string]uint64{"datagrams": s.udp.Datagrams.Load(), "packs": s.udp.Packs.Load(), "xlogs": s.udp.XLogs.Load(), "errors": s.udp.Errors.Load()}
	}
	if s.clients != nil {
		out.ClientObjects, out.ClientAlive = s.clients.lastObjects.Load(), s.clients.lastAlive.Load()
	}
	if s.f.o.HTTP != "" {
		var h struct {
			TCP          map[string]int64 `json:"tcp"`
			AgentOrphans map[string]int64 `json:"agentOrphans"`
			Stats        *healthStats     `json:"stats"`
		}
		if err := s.getJSON(ctx, "/health", &h); err != nil {
			out.HealthError = truncate(err.Error(), 200)
		} else {
			out.TCP, out.AgentOrphans, out.Stats = h.TCP, h.AgentOrphans, h.Stats
		}
		if n, err := s.goroutines(ctx); err != nil {
			out.GoroutineErr = truncate(err.Error(), 200)
		} else {
			out.Goroutines = n
		}
	}
	pid := s.pid
	if s.pidFile != "" {
		if b, err := os.ReadFile(s.pidFile); err == nil {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(b)))
		}
	}
	if pid > 0 {
		out.ServerPID = pid
		p, err := readProc(pid)
		if err != nil {
			out.ProcError = truncate(err.Error(), 200)
			s.prev = nil
		} else {
			if s.prev != nil && s.prevPID == pid {
				if sec := now.Sub(s.prevAt).Seconds(); sec > 0 {
					p.CPUPct = float64(p.CPUTicks-s.prev.CPUTicks) / clockTicks / sec * 100
				}
			}
			s.prev, s.prevAt, s.prevPID = p, now, pid
			out.Proc = p
		}
	}
	return out
}

func (s *sampler) getJSON(ctx context.Context, path string, v any) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+s.f.o.HTTP+path, nil)
	if err != nil {
		return err
	}
	res, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("HTTP %d", res.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(res.Body, 4<<20)).Decode(v)
}

// goroutines reads "goroutine profile: total N" from the pprof debug=1 page.
func (s *sampler) goroutines(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+s.f.o.HTTP+"/debug/pprof/goroutine?debug=1", nil)
	if err != nil {
		return 0, err
	}
	res, err := s.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	line, err := bufio.NewReader(io.LimitReader(res.Body, 4096)).ReadString('\n')
	if err != nil {
		return 0, err
	}
	_, after, ok := strings.Cut(line, "total ")
	if !ok {
		return 0, fmt.Errorf("unexpected pprof line %q", truncate(line, 80))
	}
	return strconv.ParseInt(strings.TrimSpace(after), 10, 64)
}

// clockTicks is Linux USER_HZ for /proc/<pid>/stat utime/stime (100 on the
// supported arm64/amd64 kernels).
const clockTicks = 100

func readProc(pid int) (*procSample, error) {
	dir := filepath.Join("/proc", strconv.Itoa(pid))
	status, err := os.ReadFile(filepath.Join(dir, "status"))
	if err != nil {
		return nil, err
	}
	p := &procSample{}
	for _, line := range strings.Split(string(status), "\n") {
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		fields := strings.Fields(v)
		if len(fields) == 0 {
			continue
		}
		switch k {
		case "VmRSS":
			p.RSSKiB, _ = strconv.ParseInt(fields[0], 10, 64)
		case "Threads":
			p.Threads, _ = strconv.ParseInt(fields[0], 10, 64)
		}
	}
	fds, err := os.ReadDir(filepath.Join(dir, "fd"))
	if err != nil {
		return nil, err
	}
	p.FDs = int64(len(fds))
	stat, err := os.ReadFile(filepath.Join(dir, "stat"))
	if err != nil {
		return nil, err
	}
	// Fields after the parenthesised comm: state is field 3, utime 14, stime 15.
	if i := strings.LastIndexByte(string(stat), ')'); i >= 0 {
		f := strings.Fields(string(stat)[i+1:])
		if len(f) > 13 {
			u, _ := strconv.ParseInt(f[11], 10, 64)
			s, _ := strconv.ParseInt(f[12], 10, 64)
			p.CPUTicks = u + s
		}
	}
	return p, nil
}

// episode is a stretch where fewer sessions than the target were connected,
// after the target had been reached (a restart or a reject wave).
type episode struct {
	StartMS       float64 `json:"start_ms"`
	EndMS         float64 `json:"end_ms,omitempty"`
	DurationMS    float64 `json:"duration_ms,omitempty"`
	Recovered     bool    `json:"recovered"`
	MinConnected  int64   `json:"min_connected"`
	Attempts      uint64  `json:"connect_attempts"`
	ConnectFail   uint64  `json:"connect_fail"`
	EndedEarly    uint64  `json:"sessions_ended_before_command"`
	attempts0     uint64
	fail0, early0 uint64
}

type agentsReport struct {
	Tool        string        `json:"tool"`
	Options     agentsOptions `json:"options"`
	StartedAt   time.Time     `json:"started_at"`
	FinishedAt  time.Time     `json:"finished_at"`
	Target      int           `json:"target_sessions"`
	V1Agents    int           `json:"v1_agents"`
	FullAtMS    float64       `json:"all_connected_after_ms"`
	FullReached bool          `json:"all_connected"`
	FinalConn   int64         `json:"final_settled"`
	MinAfter    int64         `json:"min_settled_after_full"`
	Episodes    []episode     `json:"episodes"`
	Fleet       struct {
		EverConnected int64             `json:"agents_ever_connected"`
		Attempts      uint64            `json:"connect_attempts"`
		ConnectOK     uint64            `json:"connect_ok"`
		ConnectFail   map[string]uint64 `json:"connect_fail"`
		EndedEarly    map[string]uint64 `json:"sessions_ended_before_command"`
		EndedLater    map[string]uint64 `json:"sessions_ended_after_command"`
		Keepalives    uint64            `json:"keepalives"`
		Commands      uint64            `json:"commands"`
		Responses     uint64            `json:"responses"`
		WriteErrors   uint64            `json:"write_errors"`
		KeepaliveGap  latencySummary    `json:"keepalive_gap"`
		WorstGapMS    float64           `json:"worst_keepalive_gap_ms"`
		ConnectTime   latencySummary    `json:"connect_time"`
		FirstConnect  latencySummary    `json:"first_connect_after_start"`
		SessionLife   latencySummary    `json:"ended_session_life"`
	} `json:"fleet"`
	UDP     map[string]any      `json:"udp,omitempty"`
	Clients map[string]opReport `json:"clients,omitempty"`
	Probe   *opReport           `json:"probe,omitempty"`
	Relay   *opReport           `json:"relay,omitempty"`
	Samples []serverSample      `json:"samples"`
	Peak    map[string]float64  `json:"peak"`
	Notes   []string            `json:"notes"`
	Errors  []string            `json:"errors,omitempty"`
}

func runAgents(args []string) int {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("scouter-replay agents: ")
	o, err := parseAgentsOptions(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		log.Print(err)
		return 2
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	rep := runFleet(ctx, o)
	b, err := json.MarshalIndent(rep, "", "  ")
	if err == nil {
		err = os.WriteFile(o.Report, append(b, '\n'), 0o600)
	}
	if err != nil {
		log.Print(err)
		return 1
	}
	log.Printf("report: %s", o.Report)
	log.Printf("target=%d all_connected=%v after=%.0fms final=%d min_after_full=%d episodes=%d attempts=%d connect_fail=%v ended_early=%v ended_later=%v keepalives=%d worst_gap=%.0fms",
		rep.Target, rep.FullReached, rep.FullAtMS, rep.FinalConn, rep.MinAfter, len(rep.Episodes), rep.Fleet.Attempts, rep.Fleet.ConnectFail, rep.Fleet.EndedEarly, rep.Fleet.EndedLater, rep.Fleet.Keepalives, rep.Fleet.WorstGapMS)
	return 0
}

func runFleet(ctx context.Context, o agentsOptions) *agentsReport {
	f := &fleet{o: o}
	rep := &agentsReport{Tool: toolVersion + " agents", Options: o, Target: o.Agents * o.SessionCount, MinAfter: -1, Peak: map[string]float64{}}
	rep.Options.Password = "<redacted>"
	specs := f.specs()
	for _, s := range specs {
		if s.V1 && s.Session == 0 {
			rep.V1Agents++
		}
	}
	objects := make([]object, o.Agents)
	for i := range objects {
		objects[i] = object{Name: agentName(o.NamePrefix, i), Hash: javaHash(agentName(o.NamePrefix, i))}
	}
	smp := &sampler{f: f, pid: o.PID, pidFile: o.PIDFile, http: &http.Client{Transport: &http.Transport{Proxy: nil}}}
	// Baseline before any agent connects.
	f.start = time.Now()
	rep.Samples = append(rep.Samples, smp.sample(ctx))
	f.start = time.Now().Add(100 * time.Millisecond)
	rep.StartedAt = f.start
	// A cancel, not a deadline: a deadline would reach dials and requests
	// in flight as an i/o timeout before runCtx.Err() reports it.
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	end := time.AfterFunc(time.Until(f.start.Add(o.Duration)), stop)
	defer end.Stop()
	var wg sync.WaitGroup
	for _, a := range specs {
		wg.Add(1)
		go func() { defer wg.Done(); f.runAgent(runCtx, a) }()
	}
	if o.UDP != "" {
		smp.udp = &udpStats{}
		wg.Add(1)
		go func() { defer wg.Done(); f.runUDP(runCtx, objects, smp.udp) }()
	}
	if o.Clients > 0 {
		smp.clients = &clientStats{}
		for k := 0; k < o.Clients; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sleepUntil(runCtx, f.start.Add(o.ClientInterval))
				f.runClient(runCtx, smp.clients)
			}()
		}
	}
	var probe, relay *opStats
	if o.ProbeInterval > 0 {
		probe = &opStats{}
		wg.Add(1)
		go func() { defer wg.Done(); sleepUntil(runCtx, f.start.Add(o.ProbeInterval)); f.runProbe(runCtx, probe) }()
	}
	if o.RelayClients > 0 {
		relay = &opStats{}
		for k := 0; k < o.RelayClients; k++ {
			hash := objects[k].Hash
			wg.Add(1)
			go func() {
				defer wg.Done()
				if sleepUntil(runCtx, f.start.Add(o.RelayAt)) {
					f.runRelay(runCtx, hash, relay)
				}
			}()
		}
	}
	// Watch the connected count at 50ms for the full/episode bookkeeping and
	// sample the server every --sample-interval.
	fine := time.NewTicker(50 * time.Millisecond)
	defer fine.Stop()
	nextSample := f.start.Add(o.SampleInterval)
	var cur *episode
	target := int64(rep.Target)
	for runCtx.Err() == nil {
		select {
		case <-runCtx.Done():
		case now := <-fine.C:
			n := f.settled.Load()
			rep.FinalConn = n
			// A session counts --settle after it opened; report the open time.
			at := now.Add(-o.Settle)
			switch {
			case !rep.FullReached && n >= target:
				rep.FullReached, rep.FullAtMS = true, msBetween(f.start, at)
				rep.MinAfter = n
			case rep.FullReached && cur == nil && n < target:
				cur = &episode{StartMS: msBetween(f.start, now), MinConnected: n, attempts0: f.attempts.Load(), fail0: f.connectFail.total(), early0: f.endedEarly.total()}
			case cur != nil && n >= target:
				cur.EndMS, cur.Recovered = msBetween(f.start, at), true
				cur.DurationMS = cur.EndMS - cur.StartMS
				cur.Attempts, cur.ConnectFail, cur.EndedEarly = f.attempts.Load()-cur.attempts0, f.connectFail.total()-cur.fail0, f.endedEarly.total()-cur.early0
				rep.Episodes = append(rep.Episodes, *cur)
				cur = nil
			}
			if cur != nil {
				cur.MinConnected = min(cur.MinConnected, n)
			}
			if rep.FullReached {
				rep.MinAfter = min(rep.MinAfter, n)
			}
			if !now.Before(nextSample) {
				nextSample = nextSample.Add(o.SampleInterval)
				s := smp.sample(runCtx)
				if len(rep.Samples) < maxRSSSamples {
					rep.Samples = append(rep.Samples, s)
				}
			}
		}
	}
	if cur != nil {
		cur.Attempts, cur.ConnectFail, cur.EndedEarly = f.attempts.Load()-cur.attempts0, f.connectFail.total()-cur.fail0, f.endedEarly.total()-cur.early0
		rep.Episodes = append(rep.Episodes, *cur)
	}
	stop()
	wg.Wait()
	rep.FinishedAt = time.Now()
	fl := &rep.Fleet
	fl.EverConnected, fl.Attempts, fl.ConnectOK = f.everConnected.Load(), f.attempts.Load(), f.connectOK.Load()
	fl.ConnectFail, fl.EndedEarly, fl.EndedLater = f.connectFail.snapshot(), f.endedEarly.snapshot(), f.endedLater.snapshot()
	fl.Keepalives, fl.Commands, fl.Responses, fl.WriteErrors = f.keepalives.Load(), f.commands.Load(), f.responses.Load(), f.writeErrors.Load()
	fl.KeepaliveGap, fl.ConnectTime, fl.FirstConnect, fl.SessionLife = f.keepGap.summary(), f.connectLat.summary(), f.firstConnect.summary(), f.sessionLife.summary()
	fl.WorstGapMS = float64(f.worstGap.Load()) / 1e6
	if smp.udp != nil {
		rep.UDP = map[string]any{"datagrams": smp.udp.Datagrams.Load(), "bytes": smp.udp.Bytes.Load(), "packs": smp.udp.Packs.Load(), "xlogs": smp.udp.XLogs.Load(), "errors": smp.udp.Errors.Load()}
		if v, ok := smp.udp.lastErr.Load().(string); ok {
			rep.UDP["last_error"] = v
		}
	}
	if smp.clients != nil {
		rep.Clients = map[string]opReport{"dial": smp.clients.dial.report(), "OBJECT_LIST_REAL_TIME": smp.clients.objectList.report(), "TRANX_REAL_TIME_GROUP_LATEST": smp.clients.latest.report()}
	}
	if probe != nil {
		r := probe.report()
		rep.Probe = &r
	}
	if relay != nil {
		r := relay.report()
		rep.Relay = &r
	}
	for _, s := range rep.Samples {
		peak := func(k string, v float64) { rep.Peak[k] = max(rep.Peak[k], v) }
		if p := s.Proc; p != nil {
			peak("rss_kib", float64(p.RSSKiB))
			peak("fds", float64(p.FDs))
			peak("threads", float64(p.Threads))
			peak("cpu_percent", p.CPUPct)
		}
		peak("goroutines", float64(s.Goroutines))
		if st := s.Stats; st != nil {
			peak("ingest_p99_us", float64(st.Ingest.P99Us))
			peak("ingest_max_us", float64(st.Ingest.MaxUs))
		}
		for _, k := range []string{"Handshaking", "AgentConnections", "ClientConnections", "Active"} {
			peak("tcp_"+k, float64(s.TCP[k]))
		}
		peak("orphans_active", float64(s.AgentOrphans["Active"]))
	}
	rep.Notes = append(rep.Notes,
		"sessions_ended_before_command: the socket closed before any server command (budget reject = reset, agent pool full = eof, server stop)",
		"connect_fail: Socket.connect failed (refused while the server is down, timeout when the SYN queue is full); the agent then waits fail-pause + its next tick",
		"all_connected_after_ms / episodes: counted on settled sessions (open >= --settle); end times are the open time of the last session (settle subtracted), episode start is when a settled session closed",
		"keepalive_gap: time between KEEP_ALIVE commands on one session (first gap from connect); an agent closes after --so-timeout without a command",
		"cpu_percent: server utime+stime over the sample interval, 100 = one core")
	return rep
}
