// Package httpapi serves the servlets that the Scouter server's embedded
// Jetty (scouter.server.http.HttpServer) mounts besides the webapp:
// /counter/*, /register/* and /telegraf/*. It adds the Go-side limits (IP
// allow list, body size, request decoding) documented in CONTRACT.md and hands
// packs to ingest through AcceptFrom only.
package httpapi

import (
	"bytes"
	"context"
	"errors"
	"log"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/telegraf"
)

// ingestAcceptor is ingest.Engine.AcceptFrom.
type ingestAcceptor interface {
	AcceptFrom(ctx context.Context, p protocol.Pack, peerHost string) error
}

// DefaultAllowIPs is the Configure default of net_http_api_allow_ips.
const DefaultAllowIPs = "localhost,127.0.0.1,0:0:0:0:0:0:0:1,::1"

// DefaultMaxBodyBytes bounds a request body (Go limit; Java has none).
const DefaultMaxBodyBytes = 1 << 20

// DefaultMaxInFlight bounds concurrently served servlet requests (Go limit;
// Jetty uses its thread pool). With 1 MiB bodies the decoded request memory is
// bounded by roughly MaxInFlight * 3 * MaxBodyBytes.
const DefaultMaxInFlight = 64

// DefaultQueueWait is how long a request waits for a MaxInFlight slot before
// 503. Jetty 9.4 (Java HttpServer) queues requests for its 200 threads in an
// unbounded job queue without a timeout; Go waits a bounded time instead. The
// waiting requests are bounded by the listener's connection limit. 3s stays
// below the Telegraf outputs.http default timeout (5s): a waiting request has
// not read its body and cannot see its client leave, so a longer wait could
// ingest a payload the client already gave up on and sends again.
const DefaultQueueWait = 3 * time.Second

// busyLogEvery bounds the 503 log line.
const busyLogEvery = time.Minute

// Config configures the API. Zero values select the defaults noted per field.
type Config struct {
	// CheckAllowIPs applies AllowIPs to the three servlets. The original
	// servlets have no IP check (net_http_api_allow_ips is evaluated only by
	// the webapp AuthFilter), so the default false keeps the original open
	// behaviour; true is the Go-side restriction for shared hosts.
	CheckAllowIPs bool
	// AllowIPs is net_http_api_allow_ips split like Java String.split(","):
	// exact entries plus StrMatch globs for entries containing '*'. nil uses
	// DefaultAllowIPs. Used only when CheckAllowIPs is set.
	AllowIPs []string
	// AuthIPHeader is net_http_api_auth_ip_header_key: when set, the allow
	// check uses that header (first comma item, trimmed) like the webapp
	// ZZ.getRequestIp; otherwise the TCP peer address. X-FORWARDED-FOR is never
	// trusted for the check.
	AuthIPHeader string
	// LogDir enables the NCSA request log log_dir/http-request-yyyy_MM_dd.log;
	// empty disables it.
	LogDir string
	// LogKeepDays is log_keep_days (Jetty retainDays); <= 0 keeps every file.
	LogKeepDays int
	// Gzip is net_http_api_gzip_enabled: responses of at least 1024 bytes to
	// GET/POST/PUT/DELETE are gzip encoded when the client accepts it.
	Gzip bool
	// Telegraf is the resolved Telegraf input configuration; nil uses
	// telegraf.DefaultConfig().
	Telegraf *telegraf.Config
	// Registrar is the counter catalog side effect of /register and the
	// Telegraf input; nil makes /register answer 500.
	Registrar telegraf.TypeRegistrar
	// TelegrafOptions bounds the Telegraf state and registration queues.
	TelegrafOptions telegraf.Options
	// MaxBodyBytes bounds the request body; 0 uses DefaultMaxBodyBytes.
	MaxBodyBytes int64
	// MaxInFlight bounds requests served at once; more wait up to QueueWait
	// for a slot and are then answered 503 (Retry-After: 1) without reading
	// the body. 0 uses DefaultMaxInFlight.
	MaxInFlight int
	// QueueWait is the slot wait; 0 uses DefaultQueueWait, negative answers
	// 503 at once.
	QueueWait time.Duration
	// Location is the request log time zone (Jetty: JVM default); nil uses
	// time.Local.
	Location *time.Location
	// Clock defaults to time.Now; Logf defaults to log.Printf.
	Clock func() time.Time
	Logf  func(format string, args ...any)
}

// Stats are cumulative API counters.
type Stats struct {
	Requests, Denied, NotFound, MethodNotAllowed uint64
	// Busy counts requests answered 503 because MaxInFlight stayed reached
	// for QueueWait; Waited those that had to wait for a slot (served or
	// not); Abandoned those whose client left while waiting.
	Busy, Waited, Abandoned uint64
	// InFlight is the number of requests being served now, MaxInFlight its
	// bound (Config.MaxInFlight).
	InFlight, MaxInFlight int
	// CounterTooMany counts /counter elements whose distinct counter names
	// exceed MaxCounterNames; their PerfCounterPack is not sent.
	CounterTooMany                             uint64
	TooLarge, UnsupportedMedia, BodyReadErrors uint64
	CounterRequests, CounterParseErrors        uint64
	CounterElementErrors                       uint64
	CounterPacks, CounterAcceptErrors          uint64
	RegisterCreated, RegisterFailed            uint64
	TelegrafRequests                           uint64
	LogLines, LogFailures, LogRemoved          uint64
	Telegraf                                   telegraf.Stats
	TelegrafResidualAtClose                    int
}

// API is the http.Handler for /counter, /register and /telegraf. It owns the
// Telegraf registration goroutine and the request log file; Close releases
// both.
type API struct {
	cfg      Config
	engine   ingestAcceptor
	telegraf *telegraf.Input
	reqlog   *requestLog
	allow    allowList
	handler  http.Handler
	closed   atomic.Bool
	residual atomic.Int64
	slots    chan struct{}
	busy     atomic.Uint64
	tooMany  atomic.Uint64
	// waited/abandoned count slot waits; busyLog rate-limits the 503 line.
	waited, abandoned atomic.Uint64
	busyLog           struct {
		sync.Mutex
		last       time.Time
		suppressed uint64
	}

	requests, denied, notFound, methodNotAllowed atomic.Uint64
	tooLarge, unsupported, bodyErrors            atomic.Uint64
	counterRequests, counterParseErrors          atomic.Uint64
	counterElementErrors                         atomic.Uint64
	counterPacks, counterAcceptErrors            atomic.Uint64
	registerCreated, registerFailed              atomic.Uint64
	telegrafRequests                             atomic.Uint64
}

// New builds the API. engine must not be nil.
func New(engine ingestAcceptor, cfg Config) (*API, error) {
	if engine == nil {
		return nil, errors.New("httpapi: nil ingest engine")
	}
	if cfg.AllowIPs == nil {
		cfg.AllowIPs = strings.Split(DefaultAllowIPs, ",")
	}
	if cfg.Telegraf == nil {
		cfg.Telegraf = telegraf.DefaultConfig()
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = DefaultMaxInFlight
	}
	if cfg.QueueWait == 0 {
		cfg.QueueWait = DefaultQueueWait
	}
	if cfg.Location == nil {
		cfg.Location = time.Local
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	opts := cfg.TelegrafOptions
	if opts.Clock == nil {
		opts.Clock = cfg.Clock
	}
	if opts.Logf == nil {
		opts.Logf = cfg.Logf
	}
	a := &API{cfg: cfg, engine: engine, allow: newAllowList(cfg.AllowIPs), slots: make(chan struct{}, cfg.MaxInFlight)}
	if cfg.LogDir != "" {
		a.reqlog = &requestLog{dir: cfg.LogDir, keepDays: cfg.LogKeepDays, loc: cfg.Location}
	}
	a.telegraf = telegraf.NewInput(cfg.Telegraf, cfg.Registrar, engine, opts)
	var h http.Handler = http.HandlerFunc(a.route)
	if cfg.Gzip {
		h = gzipHandler(h)
	}
	a.handler = h
	return a, nil
}

// SetTelegrafConfig publishes a reloaded Telegraf configuration.
func (a *API) SetTelegrafConfig(cfg *telegraf.Config) { a.telegraf.SetConfig(cfg) }

// Close stops the Telegraf registration goroutine and closes the request log.
// The caller must stop the http.Server first; requests arriving afterwards
// are answered 503.
func (a *API) Close() error {
	if !a.closed.CompareAndSwap(false, true) {
		return nil
	}
	a.residual.Store(int64(a.telegraf.Close()))
	if a.reqlog != nil {
		return a.reqlog.close()
	}
	return nil
}

// Stats returns a snapshot.
func (a *API) Stats() Stats {
	s := Stats{Requests: a.requests.Load(), Busy: a.busy.Load(), Waited: a.waited.Load(), Abandoned: a.abandoned.Load(), InFlight: len(a.slots), MaxInFlight: cap(a.slots), CounterTooMany: a.tooMany.Load(), Denied: a.denied.Load(), NotFound: a.notFound.Load(), MethodNotAllowed: a.methodNotAllowed.Load(),
		TooLarge: a.tooLarge.Load(), UnsupportedMedia: a.unsupported.Load(), BodyReadErrors: a.bodyErrors.Load(),
		CounterRequests: a.counterRequests.Load(), CounterParseErrors: a.counterParseErrors.Load(), CounterElementErrors: a.counterElementErrors.Load(),
		CounterPacks: a.counterPacks.Load(), CounterAcceptErrors: a.counterAcceptErrors.Load(),
		RegisterCreated: a.registerCreated.Load(), RegisterFailed: a.registerFailed.Load(), TelegrafRequests: a.telegrafRequests.Load(),
		Telegraf: a.telegraf.Stats(), TelegrafResidualAtClose: int(a.residual.Load())}
	if a.reqlog != nil {
		s.LogLines, s.LogFailures, s.LogRemoved = a.reqlog.lines.Load(), a.reqlog.failures.Load(), a.reqlog.removed.Load()
	}
	return s
}

// ServeHTTP logs every request (NCSA extended) around the IP check and the
// servlet routing.
func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.requests.Add(1)
	start := a.cfg.Clock()
	rec := &recorder{ResponseWriter: w}
	if !a.allowed(r) {
		a.denied.Add(1)
		rec.WriteHeader(http.StatusForbidden)
	} else if a.closed.Load() {
		rec.WriteHeader(http.StatusServiceUnavailable)
	} else if err := a.acquire(r.Context()); err == nil {
		// The slot goes back even when the handler panics (net/http
		// recovers it, gzip aborts with http.ErrAbortHandler).
		func() {
			defer func() { <-a.slots }()
			a.handler.ServeHTTP(rec, r)
		}()
	} else {
		if errors.Is(err, errBusy) {
			a.busy.Add(1)
			a.noteBusy(r)
		} else {
			a.abandoned.Add(1)
		}
		rec.Header().Set("Retry-After", "1")
		rec.WriteHeader(http.StatusServiceUnavailable)
	}
	if a.reqlog != nil {
		a.reqlog.write(start, ncsaLine(r, rec, start.In(a.cfg.Location)), a.cfg.Logf)
	}
}

var errBusy = errors.New("httpapi: MaxInFlight reached")

// acquire takes a MaxInFlight slot, waiting up to QueueWait. It returns
// errBusy when none was free in time and ctx.Err() when the client left.
func (a *API) acquire(ctx context.Context) error {
	select {
	case a.slots <- struct{}{}:
		return nil
	default:
	}
	if a.cfg.QueueWait < 0 {
		return errBusy
	}
	a.waited.Add(1)
	timer := time.NewTimer(a.cfg.QueueWait)
	defer timer.Stop()
	select {
	case a.slots <- struct{}{}:
		return nil
	case <-timer.C:
		return errBusy
	case <-ctx.Done():
		return ctx.Err()
	}
}

// noteBusy logs one 503 line per busyLogEvery.
func (a *API) noteBusy(r *http.Request) {
	l := &a.busyLog
	l.Lock()
	now := a.cfg.Clock()
	if !l.last.IsZero() && now.Sub(l.last) < busyLogEvery {
		l.suppressed++
		l.Unlock()
		return
	}
	more := l.suppressed
	l.last, l.suppressed = now, 0
	l.Unlock()
	a.cfg.Logf("http %s %s from %s: 503, %d servlet requests running and none ended within %s (Go servlet limit MaxInFlight; %d more 503s since the last line, %d in total)",
		r.Method, r.URL.Path, remoteAddr(r.RemoteAddr), cap(a.slots), a.cfg.QueueWait, more, a.busy.Load())
}

func (a *API) allowed(r *http.Request) bool {
	if !a.cfg.CheckAllowIPs {
		return true
	}
	ip := remoteAddr(r.RemoteAddr)
	if a.cfg.AuthIPHeader != "" {
		if v := r.Header.Get(a.cfg.AuthIPHeader); v != "" {
			ip = v
			if strings.Contains(v, ",") {
				if parts := splitDropEmpty(v, ','); len(parts) > 0 {
					ip = strings.TrimSpace(parts[0])
				}
			}
		}
	}
	if a.allow.allows(ip) {
		return true
	}
	if parsed := net.ParseIP(ip); parsed != nil && parsed.String() != ip {
		return a.allow.allows(parsed.String())
	}
	return false
}

func (a *API) route(w http.ResponseWriter, r *http.Request) {
	var serve func(http.ResponseWriter, *http.Request)
	switch p := r.URL.Path; {
	case servletPath(p, "/counter"):
		serve = a.serveCounter
	case servletPath(p, "/register"):
		serve = a.serveRegister
	case servletPath(p, "/telegraf"):
		serve = a.serveTelegraf
	default:
		a.notFound.Add(1)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	switch r.Method {
	case http.MethodPost:
		serve(w, r)
	case http.MethodOptions:
		w.Header().Set("Allow", "POST, OPTIONS")
		w.WriteHeader(http.StatusOK)
	default:
		a.methodNotAllowed.Add(1)
		w.Header().Set("Allow", "POST, OPTIONS")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// servletPath is the "/name/*" servlet mapping: the exact path or any
// sub-path.
func servletPath(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

// readBody reads the bounded body and decodes it with the request charset.
// It writes the error response and returns ok=false on failure.
func (a *API) readBody(w http.ResponseWriter, r *http.Request) (string, bool) {
	if enc := strings.TrimSpace(r.Header.Get("Content-Encoding")); enc != "" && !strings.EqualFold(enc, "identity") {
		a.unsupported.Add(1)
		w.WriteHeader(http.StatusUnsupportedMediaType)
		return "", false
	}
	var buf bytes.Buffer
	if r.ContentLength > 0 && r.ContentLength <= a.cfg.MaxBodyBytes {
		buf.Grow(int(r.ContentLength) + bytes.MinRead)
	}
	_, err := buf.ReadFrom(http.MaxBytesReader(w, r.Body, a.cfg.MaxBodyBytes))
	body := buf.Bytes()
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			a.tooLarge.Add(1)
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		} else {
			a.bodyErrors.Add(1)
			w.WriteHeader(http.StatusBadRequest)
		}
		return "", false
	}
	text, err := decodeBody(r.Header.Get("Content-Type"), body)
	if err != nil {
		a.unsupported.Add(1)
		w.WriteHeader(http.StatusUnsupportedMediaType)
		return "", false
	}
	return text, true
}

var errCharset = errors.New("httpapi: unsupported charset")

// decodeBody is ServletRequest.getReader on Jetty 9.4: the Content-Type
// charset parameter, else UTF-8 for exactly "application/json" (Jetty
// MimeTypes assumed charset), else ISO-8859-1. Malformed input is replaced
// with U+FFFD. Charsets other than UTF-8, ISO-8859-1 and US-ASCII are
// rejected (Java: UnsupportedEncodingException, 500).
func decodeBody(contentType string, body []byte) (string, error) {
	charset := ""
	if contentType != "" {
		if media, params, err := mime.ParseMediaType(contentType); err == nil {
			charset = params["charset"]
			if charset == "" && media == "application/json" && strings.EqualFold(strings.TrimSpace(contentType), "application/json") {
				charset = "utf-8"
			}
		}
	}
	switch strings.ToLower(strings.TrimSpace(charset)) {
	case "utf-8", "utf8":
		if utf8.Valid(body) {
			return string(body), nil
		}
		return strings.ToValidUTF8(string(body), "\uFFFD"), nil
	case "", "iso-8859-1", "iso8859-1", "iso_8859_1", "latin1", "l1":
		return widen(body, false), nil
	case "us-ascii", "ascii":
		return widen(body, true), nil
	}
	return "", errCharset
}

// widen decodes single-byte text: ISO-8859-1 maps each byte to the same code
// point; US-ASCII (ascii=true) maps bytes >= 0x80 to U+FFFD. The result is
// built in one exactly sized allocation (at most 3x the input).
func widen(body []byte, ascii bool) string {
	high := 0
	for _, c := range body {
		if c >= 0x80 {
			high++
		}
	}
	if high == 0 {
		return string(body)
	}
	extra := 1 // a Latin-1 byte >= 0x80 takes 2 UTF-8 bytes
	if ascii {
		extra = 2 // U+FFFD takes 3
	}
	var b strings.Builder
	b.Grow(len(body) + high*extra)
	for _, c := range body {
		switch {
		case c < 0x80:
			b.WriteByte(c)
		case ascii:
			b.WriteRune(utf8.RuneError)
		default:
			b.WriteRune(rune(c))
		}
	}
	return b.String()
}

func (a *API) serveTelegraf(w http.ResponseWriter, r *http.Request) {
	a.telegrafRequests.Add(1)
	if !a.telegraf.Enabled() {
		// Java returns before reading the body; Handle only counts it.
		a.telegraf.Handle(r.Context(), "", forwardedAddr(r))
		w.WriteHeader(http.StatusOK)
		return
	}
	body, ok := a.readBody(w, r)
	if !ok {
		return
	}
	if err := a.telegraf.Handle(r.Context(), body, forwardedAddr(r)); err != nil {
		// The client went away; nothing can be answered.
		return
	}
	w.WriteHeader(http.StatusOK)
}

// forwardedAddr is HttpServer.getRemoteAddr: the first non-empty
// X-FORWARDED-FOR item (untrimmed) or the peer address in Java form.
func forwardedAddr(r *http.Request) string {
	if v, ok := r.Header["X-Forwarded-For"]; ok && len(v) > 0 {
		if parts := splitDropEmpty(v[0], ','); len(parts) > 0 {
			return parts[0]
		}
	}
	return remoteAddr(r.RemoteAddr)
}

// remoteAddr is ServletRequest.getRemoteAddr: the peer IP without port,
// IPv4-mapped addresses as IPv4 and IPv6 in the uncompressed Java
// Inet6Address.getHostAddress form.
func remoteAddr(hostport string) string {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = hostport
	}
	zone := ""
	if i := strings.IndexByte(host, '%'); i >= 0 {
		host, zone = host[:i], host[i:]
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return host + zone
	}
	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}
	var b strings.Builder
	for i := 0; i < 16; i += 2 {
		if i > 0 {
			b.WriteByte(':')
		}
		b.WriteString(strconv.FormatUint(uint64(ip[i])<<8|uint64(ip[i+1]), 16))
	}
	return b.String() + zone
}

// splitDropEmpty is scouter.util.StringUtil.split(s, c).
func splitDropEmpty(s string, sep byte) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == sep {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}

// recorder captures the status and body size for the request log.
type recorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (r *recorder) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
		r.ResponseWriter.WriteHeader(status)
	}
}

func (r *recorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(p)
	r.bytes += int64(n)
	return n, err
}

func (r *recorder) code() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}
