package server

import (
	"context"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/snowvil/scouter-go/internal/config"
	"github.com/snowvil/scouter-go/internal/httpapi"
)

// HTTP admission (R3). Java HttpServer is a Jetty 9.4.6 Server(port) with the
// defaults: a QueuedThreadPool of at most 200 threads (a few of them acceptors
// and selectors), an unbounded job queue with no wait limit
// (BlockingArrayQueue(8, 8) grows without a cap), no connection limit and a
// 30s connector idle timeout. Requests past the threads wait; nothing is
// refused. Go bounds every part and refuses with 503 + Retry-After: 1
// instead, an intended difference:
//
//   - connections per listener: httpConnectionLimit (1024 when the open file
//     limit leaves room; it is part of fdReserve). Up to httpOverflowConns
//     more are accepted only to answer 503 with Connection: close (/health and
//     pprof are still served on them); beyond those a connection is closed at
//     accept without a response. Once 7/8 of the limit is open, every
//     response closes its connection (Connection: close), so idle keep-alive
//     connections of push hosts (Telegraf, /counter) rotate instead of holding
//     the slots until the idle timeout: more hosts than the limit can push as
//     long as fewer requests than the limit are open at once.
//   - servlets (/counter, /register, /telegraf): httpapi MaxInFlight 64 with a
//     QueueWait of 3s, the requests that take the ingest engine lock.
//   - webapp (/scouter/*): httpWebappInFlight 64 with the same 3s wait; the
//     collector calls behind them share the webapp pool of 12.
//   - /health and /debug/pprof: their own httpOpsInFlight 16 without a wait,
//     so a flood of servlet or webapp requests never delays the monitoring.
//
// The waits are bounded by the connection limit (one request per HTTP/1.1
// connection at a time). They stay below the clients' own timeouts (Telegraf
// outputs.http 5s, the Scouter Client 8s): a waiting request has not read its
// body and cannot see its client leave, so a longer wait could serve a
// request the client already retried.
//
// The go_http_* keys (config.GoLimits) replace these values at startup
// (httpAdmission); the over-limit connections stay httpOverflowConns.
const (
	httpMaxConnections = 1024
	httpOverflowConns  = 64
	httpOpsInFlight    = 16
	httpWebappInFlight = 64
	httpQueueWait      = 3 * time.Second
	httpRejectLogEvery = time.Minute
)

// httpAdmission is the startup HTTP admission configuration: the go_http_*
// keys over the defaults above. Its zero value is the defaults.
type httpAdmission struct {
	maxConns, opsInFlight           int
	servletInFlight, webappInFlight int
	servletWait, webappWait         time.Duration
}

// httpAdmissionOf takes the go_http_* limits (zero: default).
func httpAdmissionOf(l config.GoLimits) httpAdmission {
	return httpAdmission{maxConns: l.HTTPMaxConnections, opsInFlight: l.HTTPOpsInFlight,
		servletInFlight: l.HTTPServletInFlight, webappInFlight: l.HTTPWebappInFlight,
		servletWait: millis(l.HTTPServletQueueWaitMs), webappWait: millis(l.HTTPWebappQueueWaitMs)}
}

// resolved fills the defaults.
func (a httpAdmission) resolved() httpAdmission {
	def := func(v *int, d int) {
		if *v <= 0 {
			*v = d
		}
	}
	def(&a.maxConns, httpMaxConnections)
	def(&a.opsInFlight, httpOpsInFlight)
	def(&a.servletInFlight, httpapi.DefaultMaxInFlight)
	def(&a.webappInFlight, httpWebappInFlight)
	if a.servletWait <= 0 {
		a.servletWait = httpapi.DefaultQueueWait
	}
	if a.webappWait <= 0 {
		a.webappWait = httpQueueWait
	}
	return a
}

// httpConnectionLimit fits the per-listener connection limit want to the
// open file limit: a listener's connections plus its httpOverflowConns stay
// within a quarter of fdReserve, so the two HTTP listeners leave half of the
// reserve to the database files. fdReserve is at least 1024, so the limit is
// at least 1024/4 - 64 = 192 (or want when smaller).
func httpConnectionLimit(want int) int {
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil || rl.Cur == 0 {
		return want
	}
	return httpConnectionLimitFor(want, rl.Cur)
}

func httpConnectionLimitFor(want int, limit uint64) int {
	return int(min(uint64(want), fdReserve(limit)/4-httpOverflowConns))
}

// rateLog allows one line per httpRejectLogEvery and counts the others.
type rateLog struct {
	mu         sync.Mutex
	last       time.Time
	suppressed uint64
}

// allow reports whether a line may be written now and how many were
// suppressed since the last one.
func (l *rateLog) allow(now time.Time) (bool, uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.last.IsZero() && now.Sub(l.last) < httpRejectLogEvery {
		l.suppressed++
		return false, 0
	}
	more := l.suppressed
	l.last, l.suppressed = now, 0
	return true, more
}

// httpGate bounds the requests served at once, each waiting up to wait for a
// slot (0: no wait).
type httpGate struct {
	name  string
	slots chan struct{}
	wait  time.Duration
	log   rateLog
	logf  func(format string, args ...any)

	waiting                             atomic.Int64
	served, waited, rejected, abandoned atomic.Uint64
}

// httpGateStats is the /health view of a gate.
type httpGateStats struct {
	InFlight, Limit int
	Waiting         int64
	// Served counts admitted requests, Waited those that had to wait first,
	// Rejected the 503 answers, Abandoned the clients that left while waiting.
	Served, Waited, Rejected, Abandoned uint64
}

func newHTTPGate(name string, limit int, wait time.Duration) *httpGate {
	return &httpGate{name: name, slots: make(chan struct{}, limit), wait: wait}
}

func (g *httpGate) stats() httpGateStats {
	return httpGateStats{InFlight: len(g.slots), Limit: cap(g.slots), Waiting: g.waiting.Load(),
		Served: g.served.Load(), Waited: g.waited.Load(), Rejected: g.rejected.Load(), Abandoned: g.abandoned.Load()}
}

func (g *httpGate) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !g.acquire(w, r) {
			return
		}
		defer func() { <-g.slots }()
		g.served.Add(1)
		next.ServeHTTP(w, r)
	})
}

// acquire takes a slot or answers the request itself (503, or nothing to a
// client that left) and returns false.
func (g *httpGate) acquire(w http.ResponseWriter, r *http.Request) bool {
	select {
	case g.slots <- struct{}{}:
		return true
	default:
	}
	if g.wait > 0 {
		g.waited.Add(1)
		g.waiting.Add(1)
		timer := time.NewTimer(g.wait)
		select {
		case g.slots <- struct{}{}:
			timer.Stop()
			g.waiting.Add(-1)
			return true
		case <-r.Context().Done():
			timer.Stop()
			g.waiting.Add(-1)
			g.abandoned.Add(1)
			return false
		case <-timer.C:
			g.waiting.Add(-1)
		}
	}
	g.rejected.Add(1)
	if ok, more := g.log.allow(time.Now()); ok {
		logf(g.logf)("http %s %s from %s: 503, %d %s requests running and none ended within %s (Go HTTP admission limit; %d more 503s since the last line, %d in total)",
			r.Method, r.URL.Path, r.RemoteAddr, cap(g.slots), g.name, g.wait, more, g.rejected.Load())
	}
	unavailable(w)
	return false
}

func logf(f func(string, ...any)) func(string, ...any) {
	if f == nil {
		return log.Printf
	}
	return f
}

func unavailable(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	http.Error(w, "503 Service Unavailable: Go HTTP admission limit", http.StatusServiceUnavailable)
}

// httpLimits is the admission state of one listener: its connection limit
// and the /health + pprof gate.
type httpLimits struct {
	addr  string
	slots chan struct{}
	over  chan struct{}
	ops   *httpGate
	log   rateLog
	logf  func(format string, args ...any)

	// overLimit counts connections accepted past the limit (answered 503),
	// closed those closed at accept, overRejected the 503 answers on them,
	// keepAliveClosed the responses that closed a keep-alive connection near
	// the limit.
	overLimit, closed, overRejected, keepAliveClosed atomic.Uint64
}

// httpLimitsStats is the /health view of a listener.
type httpLimitsStats struct {
	Connections, MaxConnections, OverLimitConnections int
	// OverLimit counts connections accepted past MaxConnections (each answered
	// 503 and closed), ClosedAtAccept those closed without a response because
	// the over-limit connections were in use too, OverLimitRejected the 503
	// answers on them.
	OverLimit, ClosedAtAccept, OverLimitRejected uint64
	// KeepAliveClosed counts responses sent with Connection: close because
	// 7/8 of MaxConnections were open.
	KeepAliveClosed uint64
	Operational     httpGateStats
}

func newHTTPLimits(maxConns, overflow, opsInFlight int) *httpLimits {
	return &httpLimits{slots: make(chan struct{}, maxConns), over: make(chan struct{}, overflow), ops: newHTTPGate("operational", opsInFlight, 0)}
}

func (l *httpLimits) stats() httpLimitsStats {
	return httpLimitsStats{Connections: len(l.slots), MaxConnections: cap(l.slots), OverLimitConnections: len(l.over),
		OverLimit: l.overLimit.Load(), ClosedAtAccept: l.closed.Load(), OverLimitRejected: l.overRejected.Load(), KeepAliveClosed: l.keepAliveClosed.Load(), Operational: l.ops.stats()}
}

// listener wraps ln: each accepted connection holds a slot until closed.
func (l *httpLimits) listener(ln net.Listener) net.Listener {
	return &limitListener{Listener: ln, limits: l}
}

// connContext marks requests on over-limit connections (http.Server.ConnContext).
func (l *httpLimits) connContext(ctx context.Context, c net.Conn) context.Context {
	if lc, ok := c.(*limitedConn); ok && lc.over {
		return context.WithValue(ctx, overLimitKey{}, true)
	}
	return ctx
}

type overLimitKey struct{}

// handler answers 503 on over-limit connections except for the operational
// paths, which go through the ops gate on every connection.
func (l *httpLimits) handler(next http.Handler) http.Handler {
	ops := l.ops.handler(next)
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		over, _ := r.Context().Value(overLimitKey{}).(bool)
		if over {
			rw.Header().Set("Connection", "close")
		} else if len(l.slots)*8 >= cap(l.slots)*7 {
			// Near the limit: free this connection after the answer.
			rw.Header().Set("Connection", "close")
			l.keepAliveClosed.Add(1)
		}
		if operationalPath(r.URL.Path) {
			ops.ServeHTTP(rw, r)
			return
		}
		if over {
			l.overRejected.Add(1)
			unavailable(rw)
			return
		}
		next.ServeHTTP(rw, r)
	})
}

func operationalPath(path string) bool {
	return path == "/health" || strings.HasPrefix(path, "/debug/pprof/")
}

func (l *httpLimits) note(peer net.Addr, closed bool) {
	ok, more := l.log.allow(time.Now())
	if !ok {
		return
	}
	what := "answered 503 and closed"
	if closed {
		what = "closed without a response (the " + strconv.Itoa(cap(l.over)) + " over-limit connections are in use too)"
	}
	logf(l.logf)("http %s: connection from %s %s: %d connections open (Go HTTP connection limit; %d more since the last line, over limit %d, closed at accept %d in total)",
		l.addr, peer, what, cap(l.slots), more, l.overLimit.Load(), l.closed.Load())
}

type limitListener struct {
	net.Listener
	limits *httpLimits
}

func (ln *limitListener) Accept() (net.Conn, error) {
	l := ln.limits
	for {
		c, err := ln.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			return &limitedConn{Conn: c, release: l.slots}, nil
		default:
		}
		select {
		case l.over <- struct{}{}:
			l.overLimit.Add(1)
			l.note(c.RemoteAddr(), false)
			return &limitedConn{Conn: c, release: l.over, over: true}, nil
		default:
		}
		l.closed.Add(1)
		l.note(c.RemoteAddr(), true)
		_ = c.Close()
	}
}

// limitedConn returns its slot once, on the first Close.
type limitedConn struct {
	net.Conn
	release chan struct{}
	over    bool
	once    sync.Once
}

// CloseWrite keeps net/http's graceful close (closeWriteAndWait) on TCP.
func (c *limitedConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}

func (c *limitedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { <-c.release })
	return err
}
