package webapp

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Scouter Client TCP protocol as the Java webapp's TcpProxy speaks it
// (framework/client/net/TcpProxy.java, ClientTCP.java): magic 0xcafe2001 once
// per connection; per request Text(command) Int64(session) [Pack|Value body];
// response 0x03+item repeated, terminated by 0x04, or 0x44 INVALID_SESSION.
const (
	tcpClientMagic uint32 = 0xcafe2001
	flagHasNext    byte   = 0x03
	flagNoNext     byte   = 0x04
	flagInvalid    byte   = 0x44
)

// Item is one HasNEXT response item. Exactly one of Pack or Value is set,
// chosen by the command's response kind (see valueResponse).
type Item struct {
	Pack  protocol.Pack
	Value *protocol.Value
}

// valueResponse lists the commands whose response items are Values: every
// TcpProxy.getSingleValue/processValues call site of the pinned webapp.
var valueResponse = map[string]bool{
	"CHECK_LOGIN": true, "GET_CUSTOM_KV": true, "SET_CUSTOM_KV": true, "SET_CUSTOM_TTL": true,
	"GET_GLOBAL_KV": true, "SET_GLOBAL_KV": true, "SET_GLOBAL_TTL": true,
	"VISITOR_REALTIME": true, "VISITOR_REALTIME_TOTAL": true, "VISITOR_REALTIME_GROUP": true,
	"VISITOR_LOADDATE": true, "VISITOR_LOADDATE_TOTAL": true,
}

var (
	errInvalidSession = errors.New("collector returned INVALID_SESSION")
	errClosedByServer = errors.New("collector closed the connection without a response terminator")
)

// staleConnError is an exchange on a connection taken from the idle pool
// that the collector had already closed (its idle timeout racing the
// alive check): EOF or a reset on the request write or on the first
// response byte, so no response byte was read and each was never called.
type staleConnError struct {
	err error
	// unsent: the request write itself failed, so the collector had closed
	// the socket before it could read the request.
	unsent bool
}

func (e *staleConnError) Error() string { return e.err.Error() }
func (e *staleConnError) Unwrap() error { return e.err }

// connClosedByPeer reports EOF or a reset/broken pipe, not a timeout or a
// cancelled deadline.
func connClosedByPeer(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNABORTED)
}

// staleRetryable reports whether a staleConnError may be repeated once on a
// new connection. An unsent request always may. After the request was
// written, a closed connection with no response byte is the idle-close race
// in practice, but the collector may also have read and executed the
// command and then closed (a handler error closes without a terminator), so
// only commands without side effects are repeated: the SET_/SAVE_ family
// (KV, configure, alert scripting), OBJECT_REMOVE_INACTIVE,
// OBJECT_THREAD_CONTROL and the agent-heavy OBJECT_HEAPHISTO are not.
// LOGIN is repeated: a second collector session is harmless.
func staleRetryable(cmd string, err error) bool {
	var stale *staleConnError
	if !errors.As(err, &stale) {
		return false
	}
	return stale.unsent || !sideEffectCommand(cmd)
}

func sideEffectCommand(cmd string) bool {
	for _, p := range []string{"SET_", "SAVE_", "ADD_", "EDIT_", "DELETE_", "REMOVE_", "RESET_"} {
		if strings.HasPrefix(cmd, p) {
			return true
		}
	}
	switch cmd {
	case "OBJECT_REMOVE_INACTIVE", "OBJECT_THREAD_CONTROL", "OBJECT_HEAPHISTO":
		return true
	}
	return false
}

// tcpConn is one pooled Client connection. It is used by one goroutine at a
// time; a connection with an incomplete exchange is closed, never pooled.
type tcpConn struct {
	conn     net.Conn
	r        *bufio.Reader
	budget   budgetReader
	lastUsed time.Time
	reused   bool // taken from the idle pool (exchange may hit the idle-close race)
}

// budgetReader caps the bytes one response may consume.
type budgetReader struct {
	r         io.Reader
	remaining int64
}

func (b *budgetReader) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		return 0, fmt.Errorf("webapp: response exceeds %w", protocol.ErrLimit)
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.r.Read(p)
	b.remaining -= int64(n)
	return n, err
}

// pool bounds open collector connections (active + idle) to PoolSize. Callers
// wait for a slot until their context ends.
type pool struct {
	slots chan struct{}
	mu    sync.Mutex
	idle  []*tcpConn
	close bool
}

type loginState struct {
	mu       sync.Mutex    // guards the fields below; never held across I/O
	inflight chan struct{} // closed when the running LOGIN finishes
	session  int64
	delta    int64 // collector time - local time at login (Server.setDelta)
	name     string
	version  string
	localIP  string
	at       time.Time
	failAt   time.Time // last failed LOGIN; retried after loginBackoff
	failErr  error
}

// loginBackoff spaces LOGIN retries while the collector rejects or is down
// (Java ServerSessionObserver retries every 5s).
const loginBackoff = time.Second

func scouterPassword(password string) string {
	sum := sha256.Sum256([]byte("qwertyuiop!@#$%^&*()zxcvbnm,." + password))
	return hex.EncodeToString(sum[:])
}

func (s *Server) dial(ctx context.Context) (*tcpConn, error) {
	d := net.Dialer{Timeout: s.cfg.DialTimeout}
	conn, err := d.DialContext(ctx, "tcp", s.cfg.CollectorAddr)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetNoDelay(true)
	}
	var magic [4]byte
	binary.BigEndian.PutUint32(magic[:], tcpClientMagic)
	conn.SetDeadline(time.Now().Add(s.cfg.DialTimeout))
	if _, err := conn.Write(magic[:]); err != nil {
		conn.Close()
		return nil, err
	}
	c := &tcpConn{conn: conn}
	c.budget.r = conn
	c.r = bufio.NewReaderSize(&c.budget, 32<<10)
	return c, nil
}

func (c *tcpConn) closeConn() {
	c.conn.SetDeadline(time.Now().Add(200 * time.Millisecond))
	enc := protocol.NewEncoder(protocol.Limits{})
	enc.Text("CLOSE")
	if b, err := enc.Bytes(); err == nil {
		c.conn.Write(b)
	}
	c.conn.Close()
}

// alive drops idle connections the collector already closed (its idle
// timeout) without consuming data: a pooled connection has no pending bytes.
func (c *tcpConn) alive() bool {
	if c.r.Buffered() > 0 {
		return false
	}
	c.conn.SetReadDeadline(time.Now().Add(time.Millisecond))
	_, err := c.r.Peek(1)
	c.conn.SetReadDeadline(time.Time{})
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// acquire takes a slot and an idle connection, or dials one. fresh skips the
// idle pool (the retry after a stale pooled connection).
func (s *Server) acquire(ctx context.Context, fresh bool) (*tcpConn, error) {
	select {
	case s.pool.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("webapp: waiting for a collector connection: %w", ctx.Err())
	}
	for !fresh {
		s.pool.mu.Lock()
		if s.pool.close {
			s.pool.mu.Unlock()
			<-s.pool.slots
			return nil, errors.New("webapp: closed")
		}
		var c *tcpConn
		if n := len(s.pool.idle); n > 0 {
			c = s.pool.idle[0]
			s.pool.idle = s.pool.idle[1:]
		}
		s.pool.mu.Unlock()
		if c == nil {
			break
		}
		if s.now().Sub(c.lastUsed) < s.live.Load().PoolIdleTimeout && c.alive() {
			c.reused = true
			return c, nil
		}
		c.closeConn()
	}
	if fresh {
		s.pool.mu.Lock()
		closed := s.pool.close
		s.pool.mu.Unlock()
		if closed {
			<-s.pool.slots
			return nil, errors.New("webapp: closed")
		}
	}
	c, err := s.dial(ctx)
	if err != nil {
		<-s.pool.slots
		return nil, &apiError{status: http.StatusInternalServerError, code: http.StatusInternalServerError, message: msgNotConnected, cause: err}
	}
	return c, nil
}

func (s *Server) release(c *tcpConn, reusable bool) {
	defer func() { <-s.pool.slots }()
	if !reusable {
		c.closeConn()
		return
	}
	c.lastUsed = s.now()
	s.pool.mu.Lock()
	if s.pool.close {
		s.pool.mu.Unlock()
		c.closeConn()
		return
	}
	s.pool.idle = append(s.pool.idle, c)
	s.pool.mu.Unlock()
}

// Close closes idle collector connections; in-flight calls finish and close
// their connections on release.
func (s *Server) Close() error {
	s.pool.mu.Lock()
	s.pool.close = true
	idle := s.pool.idle
	s.pool.idle = nil
	s.pool.mu.Unlock()
	for _, c := range idle {
		c.closeConn()
	}
	return nil
}

// exchange runs one command on c. It returns reusable=true only after a clean
// NoNEXT terminator.
func (s *Server) exchange(ctx context.Context, c *tcpConn, session int64, cmd string, body func(*protocol.Encoder), each func(Item) error) (bool, error) {
	// CallTimeout is net_webapp_tcp_client_so_timeout: Java sets it as the
	// socket SO_TIMEOUT, a bound on each read, not on the whole command. The
	// deadline is armed again after every response item, so a long answer
	// that keeps arriving (an objType counter range over hours) completes
	// while a collector that stops answering still fails after CallTimeout.
	// A cancelled request ends the exchange; mu keeps a re-arm from undoing
	// that.
	var mu sync.Mutex
	arm := func() {
		mu.Lock()
		defer mu.Unlock()
		if ctx.Err() != nil {
			c.conn.SetDeadline(time.Now())
			return
		}
		deadline := time.Now().Add(s.cfg.CallTimeout)
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
		c.conn.SetDeadline(deadline)
	}
	arm()
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		defer mu.Unlock()
		c.conn.SetDeadline(time.Now())
	})
	defer stop()
	enc := protocol.NewEncoder(protocol.Limits{})
	enc.Text(cmd)
	enc.Int64(session)
	if body != nil {
		body(enc)
	}
	raw, err := enc.Bytes()
	if err != nil {
		return false, err
	}
	if _, err := c.conn.Write(raw); err != nil {
		err = fmt.Errorf("%s request: %w", cmd, err)
		if c.reused && connClosedByPeer(err) {
			return false, &staleConnError{err: err, unsent: true}
		}
		return false, err
	}
	c.budget.remaining = s.cfg.MaxResponseBytes
	isValue := valueResponse[cmd]
	items := 0
	for {
		flag, err := c.r.ReadByte()
		if err != nil {
			// items == 0 here means no response byte was read at all.
			stale := c.reused && items == 0 && connClosedByPeer(err)
			if errors.Is(err, io.EOF) {
				err = errClosedByServer
			}
			err = fmt.Errorf("%s response after %d items: %w", cmd, items, err)
			if stale {
				return false, &staleConnError{err: err}
			}
			return false, err
		}
		switch flag {
		case flagNoNext:
			return true, nil
		case flagInvalid:
			return false, errInvalidSession
		case flagHasNext:
		default:
			return false, fmt.Errorf("%s: unexpected response flag 0x%02x", cmd, flag)
		}
		items++
		if items > s.cfg.MaxResponseItems {
			return false, fmt.Errorf("%s: more than %d response items: %w", cmd, s.cfg.MaxResponseItems, protocol.ErrLimit)
		}
		var item Item
		if isValue {
			v, err := protocol.ReadValue(c.r, responseLimits)
			if err != nil {
				return false, fmt.Errorf("%s value: %w", cmd, err)
			}
			item.Value = &v
		} else {
			p, err := protocol.ReadPack(c.r, responseLimits)
			if err != nil {
				return false, fmt.Errorf("%s pack: %w", cmd, err)
			}
			item.Pack = p
		}
		if err := each(item); err != nil {
			return false, err
		}
		arm()
	}
}

// responseLimits decodes one collector response item. The collection bounds
// are above the request defaults (4096/16384): a COUNTER_PAST_TIME_ALL row
// holds a 2s point per list for its range, 43,200 a day, and the original
// webapp reads it unbounded. Bytes keep the default 8MiB per item.
var responseLimits = protocol.Limits{MaxCollectionEntries: 1 << 18, MaxTotalValues: 1 << 19}

// loginResult is one LOGIN outcome (LoginMgr.loginInternal).
type loginResult struct {
	session       int64
	time          int64
	name, version string
	localIP       string
}

// loginOnce performs LOGIN on a pooled connection without holding login.mu.
// A stale idle connection is replaced by a new one once (staleRetryable).
func (s *Server) loginOnce(ctx context.Context) (loginResult, error) {
	res, err := s.loginAttempt(ctx, false)
	if staleRetryable("LOGIN", err) && ctx.Err() == nil {
		return s.loginAttempt(ctx, true)
	}
	return res, err
}

func (s *Server) loginAttempt(ctx context.Context, fresh bool) (loginResult, error) {
	c, err := s.acquire(ctx, fresh)
	if err != nil {
		return loginResult{}, err
	}
	reusable := false
	defer func() { s.release(c, reusable) }()
	host, _ := os.Hostname()
	ip := ""
	if a, ok := c.conn.LocalAddr().(*net.TCPAddr); ok {
		ip = a.IP.String()
	}
	param := &protocol.MapPack{Entries: []protocol.Entry{
		{Key: "id", Value: textValue(s.cfg.Username)},
		{Key: "pass", Value: textValue(scouterPassword(s.cfg.Password))},
		{Key: "version", Value: textValue("scouter-go webapp")},
		{Key: "hostname", Value: textValue(host)},
		{Key: "internal", Value: textValue("true")},
		{Key: "ip", Value: textValue(ip)},
	}}
	var out *protocol.MapPack
	ok, err := s.exchange(ctx, c, 0, "LOGIN", func(e *protocol.Encoder) { e.Pack(param) }, func(it Item) error {
		if m, isMap := it.Pack.(*protocol.MapPack); isMap {
			out = m
		}
		return nil
	})
	if err != nil || out == nil {
		if err == nil {
			err = errors.New("LOGIN: empty response")
		}
		return loginResult{}, &apiError{status: http.StatusInternalServerError, code: http.StatusInternalServerError, message: msgNotConnected, cause: err}
	}
	res := loginResult{session: mapInt(out, "session"), time: mapInt(out, "time"), name: mapText(out, "server_id"), version: mapText(out, "version"), localIP: ip}
	if res.session == 0 {
		return loginResult{}, &apiError{status: http.StatusInternalServerError, code: http.StatusInternalServerError, message: msgNotConnected, cause: fmt.Errorf("LOGIN rejected for %q: %s", s.cfg.Username, mapText(out, "error"))}
	}
	reusable = ok
	return res, nil
}

// session returns the shared collector session, logging in when needed. One
// LOGIN runs at a time; other callers wait for it or for their own ctx, and
// retry if the running LOGIN's caller gave up.
func (s *Server) session(ctx context.Context) (int64, error) {
	for {
		s.login.mu.Lock()
		if s.login.session != 0 {
			session := s.login.session
			s.login.mu.Unlock()
			return session, nil
		}
		if s.login.failErr != nil && time.Since(s.login.failAt) < loginBackoff {
			err := s.login.failErr
			s.login.mu.Unlock()
			return 0, err
		}
		if wait := s.login.inflight; wait != nil {
			s.login.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return 0, fmt.Errorf("webapp: waiting for collector LOGIN: %w", ctx.Err())
			}
		}
		done := make(chan struct{})
		s.login.inflight = done
		s.login.mu.Unlock()

		// A panic inside loginOnce must not leave inflight set forever
		// (every later caller would wait on a channel nobody closes).
		finished := false
		defer func() {
			if !finished {
				s.login.mu.Lock()
				if s.login.inflight == done {
					s.login.inflight = nil
				}
				s.login.mu.Unlock()
				close(done)
			}
		}()
		res, err := s.loginOnce(ctx)

		s.login.mu.Lock()
		s.login.inflight = nil
		if err != nil {
			var api *apiError // collector down/rejected, not this caller's ctx
			if errors.As(err, &api) && ctx.Err() == nil {
				s.login.failAt, s.login.failErr = time.Now(), err
			}
		} else {
			s.login.failErr = nil
			s.login.session = res.session
			s.login.delta = 0
			if res.time >= 1 {
				s.login.delta = res.time - s.now().UnixMilli()
			}
			s.login.name, s.login.version, s.login.localIP = res.name, res.version, res.localIP
			s.login.at = s.now()
		}
		session := s.login.session
		s.login.mu.Unlock()
		finished = true
		close(done)
		return session, err
	}
}

// invalidate clears the session if it is still the one that was rejected.
func (s *Server) invalidate(session int64) {
	s.login.mu.Lock()
	if s.login.session == session {
		s.login.session = 0
	}
	s.login.mu.Unlock()
}

// do runs cmd with one transparent re-login on INVALID_SESSION. The collector
// rejects such a request before executing it, so the retry cannot repeat work.
// An exchange on a stale idle connection is repeated once on a new
// connection when staleRetryable allows it.
func (s *Server) do(ctx context.Context, cmd string, body func(*protocol.Encoder), each func(Item) error) error {
	if s.cfg.Supported != nil && !s.cfg.Supported(cmd) {
		return notImplemented(cmd)
	}
	relogged, fresh := false, false
	for {
		session, err := s.session(ctx)
		if err != nil {
			return err
		}
		c, err := s.acquire(ctx, fresh)
		if err != nil {
			return err
		}
		err = s.exchangeReleasing(ctx, c, session, cmd, body, each)
		if errors.Is(err, errInvalidSession) {
			s.invalidate(session)
			if !relogged {
				relogged = true
				continue
			}
			return &apiError{status: http.StatusInternalServerError, code: http.StatusInternalServerError, message: msgInternal, cause: err}
		}
		if !fresh && ctx.Err() == nil && staleRetryable(cmd, err) {
			fresh = true
			continue
		}
		return err
	}
}

// exchangeReleasing runs exchange and always returns c's slot, also when
// each panics (the connection is then closed, never pooled).
func (s *Server) exchangeReleasing(ctx context.Context, c *tcpConn, session int64, cmd string, body func(*protocol.Encoder), each func(Item) error) error {
	reusable := false
	defer func() { s.release(c, reusable) }()
	ok, err := s.exchange(ctx, c, session, cmd, body, each)
	reusable = ok
	return err
}

// call sends a Pack body (nil sends no body, like TcpProxy.process(cmd, null)).
func (s *Server) call(ctx context.Context, cmd string, param protocol.Pack, each func(Item) error) error {
	if param == nil {
		return s.do(ctx, cmd, nil, each)
	}
	return s.do(ctx, cmd, func(e *protocol.Encoder) { e.Pack(param) }, each)
}

// callValue sends a Value body (e.g. GET_GLOBAL_KV TextValue).
func (s *Server) callValue(ctx context.Context, cmd string, param protocol.Value, each func(Item) error) error {
	return s.do(ctx, cmd, func(e *protocol.Encoder) { e.Value(param) }, each)
}

// callNoBody sends only command and session.
func (s *Server) callNoBody(ctx context.Context, cmd string, each func(Item) error) error {
	return s.do(ctx, cmd, nil, each)
}

// single returns the first MapPack of a response, or nil (TcpProxy.getSingle).
func (s *Server) single(ctx context.Context, cmd string, param protocol.Pack) (*protocol.MapPack, error) {
	var out *protocol.MapPack
	first := true
	err := s.call(ctx, cmd, param, func(it Item) error {
		if !first {
			return nil
		}
		first = false
		m, ok := it.Pack.(*protocol.MapPack)
		if !ok {
			return fmt.Errorf("%s: response is %T, want MapPack", cmd, it.Pack)
		}
		out = m
		return nil
	})
	return out, err
}
