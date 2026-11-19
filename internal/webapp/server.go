// Package webapp serves the scouter.webapp REST API (/scouter/v1/...) inside
// the Go server. Like the Java webapp it is a Client-protocol TCP client of
// the collector: REST handlers only translate HTTP <-> Client commands, and
// query semantics stay in the TCP handlers. Contract notes: CONTRACT.md and
// docs/compatibility/webapp-api.md.
package webapp

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Config maps the original net_http_api_* / net_webapp_* options.
type Config struct {
	// CollectorAddr is the collector's Client TCP address ("127.0.0.1:6100").
	// serverId is HashUtil.hash(host + port) of this address.
	CollectorAddr string
	// Username/Password log in to the collector (Java embedded: __local).
	// Password is raw; LOGIN sends the salted SHA-256 like the Java client.
	Username, Password string

	PoolSize         int           // net_webapp_tcp_client_pool_size (12)
	PoolIdleTimeout  time.Duration // net_webapp_tcp_client_pool_timeout (8s = default net_tcp_client_so_timeout_ms); 0 = default, negative closes every idle connection (Java <= 0)
	DialTimeout      time.Duration // 3s (ClientTCP connect timeout)
	CallTimeout      time.Duration // per command (30s, net_webapp_tcp_client_so_timeout)
	MaxResponseBytes int64         // bytes one command response may read (64 MiB)
	MaxResponseItems int           // items one command response may carry (1,000,000)
	MaxBodyBytes     int64         // request body cap (4 MiB)

	// Supported reports whether the collector implements a Client command.
	// Unsupported commands answer 501 NOT_IMPLEMENTED without a TCP call.
	// nil assumes every command is supported.
	Supported func(command string) bool

	// CORS*, Auth*, AllowIPs and PoolIdleTimeout are the initial live
	// Settings; SetSettings replaces them after a reload.
	CORSAllowOrigin      string // net_http_api_cors_allow_origin (Java "*"); "" disables CORS
	CORSAllowCredentials string // net_http_api_cors_allow_credentials (Java "true")

	AuthIPEnabled      bool          // net_http_api_auth_ip_enabled
	AllowIPs           string        // net_http_api_allow_ips
	AuthIPHeaderKey    string        // net_http_api_auth_ip_header_key
	AuthSessionEnabled bool          // net_http_api_auth_session_enabled
	AuthBearerEnabled  bool          // net_http_api_auth_bearer_token_enabled
	SessionTimeout     time.Duration // net_http_api_session_timeout (24h)
	MaxSessions        int           // servlet sessions kept (10,000)

	Gzip bool // net_http_api_gzip_enabled (Java true)

	// ListenAddr is the HTTP listen address, used only for the startup
	// warning when all three authentication options are off on a
	// non-loopback address.
	ListenAddr string

	Location *time.Location   // collector timezone (time.Local)
	Clock    func() time.Time // time.Now
	Logf     func(format string, args ...any)
}

// DefaultConfig returns the original defaults (Configure.java:219-258).
func DefaultConfig() Config {
	return Config{
		PoolSize: 12, PoolIdleTimeout: 8 * time.Second, DialTimeout: 3 * time.Second, CallTimeout: 30 * time.Second,
		MaxResponseBytes: 64 << 20, MaxResponseItems: 1_000_000, MaxBodyBytes: 4 << 20,
		CORSAllowOrigin: "*", CORSAllowCredentials: "true",
		AllowIPs:       "localhost,127.0.0.1,0:0:0:0:0:0:0:1,::1",
		SessionTimeout: 24 * time.Hour, MaxSessions: 10000, Gzip: true,
	}
}

// Server is one webapp instance bound to one collector.
type Server struct {
	cfg      Config
	id       int32
	pool     pool
	login    loginState
	texts    *textProxy
	model    modelCache
	sessions *sessionStore
	tokens   *tokenCache
	live     atomic.Pointer[live]
	ridBase  string
	ridSeq   atomic.Int64
	mux      *http.ServeMux
}

// New validates cfg and builds the server. It does not dial: the collector
// LOGIN happens on first use and again after INVALID_SESSION.
func New(cfg Config) (*Server, error) {
	host, port, err := net.SplitHostPort(cfg.CollectorAddr)
	if err != nil {
		return nil, fmt.Errorf("webapp: collector address: %w", err)
	}
	if cfg.Username == "" {
		return nil, errors.New("webapp: collector username required")
	}
	def := DefaultConfig()
	if cfg.PoolSize == 0 {
		cfg.PoolSize = def.PoolSize
	}
	if cfg.PoolIdleTimeout == 0 {
		cfg.PoolIdleTimeout = def.PoolIdleTimeout
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = def.DialTimeout
	}
	if cfg.CallTimeout == 0 {
		cfg.CallTimeout = def.CallTimeout
	}
	if cfg.MaxResponseBytes == 0 {
		cfg.MaxResponseBytes = def.MaxResponseBytes
	}
	if cfg.MaxResponseItems == 0 {
		cfg.MaxResponseItems = def.MaxResponseItems
	}
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = def.MaxBodyBytes
	}
	if cfg.SessionTimeout == 0 {
		cfg.SessionTimeout = def.SessionTimeout
	}
	if cfg.MaxSessions == 0 {
		cfg.MaxSessions = def.MaxSessions
	}
	// A negative PoolIdleTimeout is valid: like Java ConnectionPool with a
	// timeout <= 0 (and like SetSettings), no idle connection is reused.
	if cfg.PoolSize < 1 || cfg.PoolSize > 1024 || cfg.DialTimeout < 0 || cfg.CallTimeout < 0 ||
		cfg.MaxResponseBytes < 1 || cfg.MaxResponseItems < 1 || cfg.MaxBodyBytes < 1 || cfg.SessionTimeout < time.Second || cfg.MaxSessions < 1 {
		return nil, errors.New("webapp: invalid limits")
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
	s := &Server{
		cfg:      cfg,
		id:       javaHash(host + port),
		texts:    newTextProxy(),
		sessions: newSessionStore(cfg.MaxSessions, cfg.SessionTimeout),
		tokens:   newTokenCache(10000),
		ridBase:  "#" + strconv.FormatInt(int64(rand.IntN(30000)+2000), 32),
	}
	s.SetSettings(Settings{
		CORSAllowOrigin: cfg.CORSAllowOrigin, CORSAllowCredentials: cfg.CORSAllowCredentials,
		AuthIPEnabled: cfg.AuthIPEnabled, AllowIPs: cfg.AllowIPs, AuthIPHeaderKey: cfg.AuthIPHeaderKey,
		AuthSessionEnabled: cfg.AuthSessionEnabled, AuthBearerEnabled: cfg.AuthBearerEnabled,
		PoolIdleTimeout: cfg.PoolIdleTimeout,
	})
	s.warnNoAuth(s.Settings())
	s.pool.slots = make(chan struct{}, cfg.PoolSize)
	s.mux = http.NewServeMux()
	s.registerA(s.mux)
	s.registerB(s.mux)
	return s, nil
}

func (s *Server) logf(format string, args ...any) { s.cfg.Logf(format, args...) }

// loopbackAddr reports whether a listen address binds only loopback. An
// empty or unspecified host ("0.0.0.0", "::") is not loopback; "localhost"
// is.
func loopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

// serverID is HashUtil.hash(ip + port) of the collector address (Server.java:69).
func (s *Server) serverID() int32 { return s.id }

// noAuthPatterns are the @NoAuth resources (UserController login and
// loginGetToken, InfoController /server, SimpleViewController /s/{key}),
// matched by the ServeMux pattern rather than the raw request path.
var noAuthPatterns = map[string]bool{
	"POST /scouter/v1/user/login":         true,
	"POST /scouter/v1/user/loginGetToken": true,
	"GET /scouter/v1/info/server":         true,
	"GET /scouter/s/{key}":                true,
}

// Handler returns the HTTP handler. Requests outside /scouter/ are 404 (the
// Java webroot, /extweb and swagger are not provided).
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.serveHTTP)
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/scouter" && !strings.HasPrefix(r.URL.Path, "/scouter/") {
		http.NotFound(w, r)
		return
	}
	// LoggingInitServletFilter: requestId = "#" + base32(random) + counter.
	rid := s.ridBase + strconv.FormatInt(s.ridSeq.Add(1)-1, 10)
	r = r.WithContext(context.WithValue(r.Context(), ctxRequestID, rid))
	// CorsFilter (live net_http_api_cors_*)
	cors := s.live.Load()
	if origin := cors.CORSAllowOrigin; strings.TrimSpace(origin) != "" {
		if cors.CORSAllowCredentials == "true" && origin == "*" {
			if o := r.Header.Get("Origin"); strings.TrimSpace(o) != "" {
				origin = o
			}
		}
		h := w.Header()
		h.Add("Access-Control-Allow-Origin", origin)
		h.Add("Access-Control-Allow-Credentials", cors.CORSAllowCredentials)
		h.Add("Access-Control-Max-Age", "600")
		h.Add("Access-Control-Allow-Headers", "Content-Type, Authorization")
		h.Add("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE, PATCH")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
	}
	// NoCacheFilter
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Expires", "0")
	w.Header().Set("Pragma", "No-cache")
	if s.cfg.Gzip && acceptsGzip(r) {
		gw := &gzipWriter{ResponseWriter: w}
		defer gw.finish()
		w = gw
	}
	if r.Method == http.MethodOptions {
		s.options(w, r)
		return
	}
	h, pattern := s.mux.Handler(r)
	if pattern == "" {
		// Jersey 404/405 through GenericExceptionMapper.
		rec := &statusRecorder{header: http.Header{}}
		h.ServeHTTP(rec, r)
		status := rec.status
		if status == 0 || status == http.StatusOK {
			status = http.StatusNotFound
		}
		if allow := rec.header.Get("Allow"); allow != "" {
			w.Header().Set("Allow", allow)
		}
		s.writeError(w, r, status, status, "HTTP "+strconv.Itoa(status)+" "+http.StatusText(status))
		return
	}
	if !noAuthPatterns[pattern] {
		var err error
		if r, err = s.authenticate(r); err != nil {
			s.writeErr(w, r, err)
			return
		}
	}
	s.mux.ServeHTTP(w, r) // sets PathValue; pattern already resolved above
}

// options answers OPTIONS without CORS like Jersey's default: 200 with Allow
// (no WADL body), or 404 for an unknown resource.
func (s *Server) options(w http.ResponseWriter, r *http.Request) {
	var allow []string
	for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		probe := r.Clone(r.Context())
		probe.Method = m
		if _, pattern := s.mux.Handler(probe); pattern != "" {
			allow = append(allow, m)
		}
	}
	if len(allow) == 0 {
		s.writeError(w, r, http.StatusNotFound, http.StatusNotFound, "HTTP 404 Not Found")
		return
	}
	if contains(allow, http.MethodGet) {
		allow = append(allow, http.MethodHead)
	}
	w.Header().Set("Allow", strings.Join(append(allow, http.MethodOptions), ","))
	w.WriteHeader(http.StatusOK)
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

type statusRecorder struct {
	header http.Header
	status int
}

func (r *statusRecorder) Header() http.Header { return r.header }
func (r *statusRecorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return len(p), nil
}
func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
}

// ---- gzip (Jetty GzipHandler: GET/POST/PUT/DELETE, minGzipSize 1024) ----

const minGzipSize = 1024

func acceptsGzip(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete:
	default:
		return false
	}
	for _, part := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		name, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		if strings.EqualFold(strings.TrimSpace(name), "gzip") && !strings.Contains(strings.ReplaceAll(params, " ", ""), "q=0") {
			return true
		}
	}
	return false
}

// gzipWriter buffers up to minGzipSize bytes before choosing plain or gzip.
type gzipWriter struct {
	http.ResponseWriter
	status  int
	buf     []byte
	decided bool
	gz      *gzip.Writer
}

func (g *gzipWriter) WriteHeader(code int) {
	if g.status == 0 {
		g.status = code
	}
}

func (g *gzipWriter) Write(p []byte) (int, error) {
	if g.status == 0 {
		g.status = http.StatusOK
	}
	if g.decided {
		if g.gz != nil {
			return g.gz.Write(p)
		}
		return g.ResponseWriter.Write(p)
	}
	g.buf = append(g.buf, p...)
	if len(g.buf) >= minGzipSize {
		if err := g.decide(true); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func (g *gzipWriter) decide(compress bool) error {
	g.decided = true
	h := g.ResponseWriter.Header()
	h.Add("Vary", "Accept-Encoding")
	if compress && h.Get("Content-Encoding") == "" && g.status != http.StatusNoContent && g.status != http.StatusNotModified {
		h.Set("Content-Encoding", "gzip")
		h.Del("Content-Length")
		g.ResponseWriter.WriteHeader(g.status)
		g.gz = gzip.NewWriter(g.ResponseWriter)
		_, err := g.gz.Write(g.buf)
		g.buf = nil
		return err
	}
	if g.status != 0 {
		g.ResponseWriter.WriteHeader(g.status)
	}
	_, err := g.ResponseWriter.Write(g.buf)
	g.buf = nil
	return err
}

// Flush supports streaming responses: undecided output is sent as is.
func (g *gzipWriter) Flush() {
	if !g.decided {
		g.decide(len(g.buf) >= minGzipSize)
	}
	if g.gz != nil {
		g.gz.Flush()
	}
	if f, ok := g.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (g *gzipWriter) finish() {
	if !g.decided {
		if g.status == 0 && len(g.buf) == 0 {
			return
		}
		g.decide(len(g.buf) >= minGzipSize)
	}
	if g.gz != nil {
		g.gz.Close()
	}
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (g *gzipWriter) Unwrap() http.ResponseWriter { return g.ResponseWriter }
