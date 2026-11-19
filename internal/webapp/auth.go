package webapp

import (
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// AuthFilter (framework/filter/AuthFilter.java:50-95): (1) IP allow list,
// (2) bearer token, (3) servlet session, (4) IP violation. All three disabled
// means no authentication, the original default.

const (
	sessionCookie   = "JSESSIONID"
	webSessionSpace = "__WEB_SESSION__"
	// tokenFutureSkew is how far (seconds) a stored token footprint may lie
	// ahead of the clock; a later footprint (forged or skewed KV value that
	// would effectively never expire) is rejected.
	tokenFutureSkew int64 = 300
)

// strMatch is scouter.util.StrMatch with '*' (at most the first inner '*').
type strMatch struct {
	comp                  int
	start, end, mid, mid2 string
}

const (
	cmpEqu = iota
	cmpStr
	cmpStrMid
	cmpStrEnd
	cmpMid
	cmpMidMid
	cmpMidEnd
	cmpEnd
	cmpAny
)

func newStrMatch(p string) strMatch {
	if p == "*" || p == "**" {
		return strMatch{comp: cmpAny}
	}
	n := len(p)
	if n < 2 {
		return strMatch{comp: cmpEqu, mid: p}
	}
	anyStart, anyEnd := p[0] == '*', p[n-1] == '*'
	x := strings.IndexByte(p[1:], '*')
	if x >= 0 {
		x++
	}
	anyMid := x > 0 && x < n-1
	switch {
	case anyMid && anyStart && anyEnd:
		return strMatch{comp: cmpMidMid, mid: p[1:x], mid2: p[x+1 : n-1]}
	case anyMid && anyStart:
		return strMatch{comp: cmpMidEnd, mid: p[1:x], end: p[x+1:]}
	case anyMid && anyEnd:
		return strMatch{comp: cmpStrMid, start: p[:x], mid: p[x+1 : n-1]}
	case anyMid:
		return strMatch{comp: cmpStrEnd, start: p[:x], end: p[x+1:]}
	case anyStart && anyEnd:
		return strMatch{comp: cmpMid, mid: p[1 : n-1]}
	case anyStart:
		return strMatch{comp: cmpEnd, end: p[1:]}
	case anyEnd:
		return strMatch{comp: cmpStr, start: p[:n-1]}
	}
	return strMatch{comp: cmpEqu, mid: p}
}

func (m strMatch) include(t string) bool {
	if t == "" {
		return false
	}
	switch m.comp {
	case cmpAny:
		return true
	case cmpEqu:
		return t == m.mid
	case cmpStr:
		return strings.HasPrefix(t, m.start)
	case cmpStrMid:
		return strings.HasPrefix(t, m.start) && strings.Contains(t, m.mid)
	case cmpStrEnd:
		return strings.HasPrefix(t, m.start) && strings.HasSuffix(t, m.end)
	case cmpMid:
		return strings.Contains(t, m.mid)
	case cmpMidMid:
		x := strings.Index(t, m.mid)
		return x >= 0 && strings.Contains(t[x+len(m.mid):], m.mid2)
	case cmpMidEnd:
		return strings.Contains(t, m.mid) && strings.HasSuffix(t, m.end)
	case cmpEnd:
		return strings.HasSuffix(t, m.end)
	}
	return false
}

// ipAllow is Configure.allowIpExact/allowIpMatch from net_http_api_allow_ips.
// Go also compares net.ParseIP-normalized forms, so "0:0:0:0:0:0:0:1" and
// "::1", or "::ffff:10.0.0.1" and "10.0.0.1", are the same address.
type ipAllow struct {
	exact map[string]bool
	match []strMatch
}

// normalizeIP returns the canonical text of an IP (IPv4-mapped as IPv4), or
// "" when s is not an IP address.
func normalizeIP(s string) string {
	ip := net.ParseIP(strings.Trim(strings.TrimSpace(s), "[]"))
	if ip == nil {
		return ""
	}
	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}
	return ip.String()
}

func newIPAllow(list string) ipAllow {
	a := ipAllow{exact: map[string]bool{}}
	for _, ip := range strings.Split(list, ",") {
		a.exact[ip] = true
		if n := normalizeIP(ip); n != "" {
			a.exact[n] = true
		}
		if strings.Contains(ip, "*") {
			a.match = append(a.match, newStrMatch(ip))
		}
	}
	return a
}

func (a ipAllow) allowed(ip string) bool {
	candidates := []string{ip}
	if n := normalizeIP(ip); n != "" && n != ip {
		candidates = append(candidates, n)
	}
	for _, c := range candidates {
		if a.exact[c] {
			return true
		}
		for _, m := range a.match {
			if m.include(c) {
				return true
			}
		}
	}
	return false
}

// requestIP is ZZ.getRequestIp: the first comma token of the configured
// header, else the remote address.
func (s *Server) requestIP(r *http.Request, v *live) string {
	if key := strings.TrimSpace(v.AuthIPHeaderKey); key != "" {
		if v := r.Header.Get(key); v != "" {
			if strings.Contains(v, ",") {
				parts := javaSplit(v, ',', 0)
				if len(parts) > 0 {
					return strings.TrimSpace(parts[0])
				}
			}
			return v
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// authenticate applies AuthFilter and returns the request with the user id
// (WebRequestContext user token) in its context.
func (s *Server) authenticate(r *http.Request) (*http.Request, error) {
	v := s.live.Load() // one AuthFilter pass sees one settings value
	violation := false
	if v.AuthIPEnabled {
		if v.allow.allowed(s.requestIP(r, v)) {
			return r, nil
		}
		violation = true
	}
	if v.AuthBearerEnabled {
		if h := r.Header.Get("Authorization"); h != "" {
			if len(h) >= 7 && strings.EqualFold(h[:7], "bearer ") {
				h = h[7:]
			}
			user, err := s.validateBearer(r.Context(), h)
			if err != nil {
				return r, err
			}
			return withUser(r, user), nil
		}
		if !v.AuthSessionEnabled {
			return r, errSessionExpired
		}
	}
	if v.AuthSessionEnabled {
		user, ok := s.sessions.user(r, s.now())
		if !ok {
			return r, errLoginRequired
		}
		r = withUser(r, user)
	}
	if violation {
		return r, errSessionExpired
	}
	return r, nil
}

func withUser(r *http.Request, user string) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), ctxUserID, user))
}

// requestUserID is WebRequestContext.getUserToken().getUserId().
func (s *Server) requestUserID(r *http.Request) (string, bool) {
	u, ok := r.Context().Value(ctxUserID).(string)
	return u, ok
}

// ---- servlet session (Jetty HashSessionManager equivalent) ----

type sessionStore struct {
	mu      sync.Mutex
	max     int
	timeout time.Duration
	byID    map[string]*list.Element
	order   *list.List // least recently used at front
}

type webSession struct {
	id, user string
	last     time.Time
}

func newSessionStore(max int, timeout time.Duration) *sessionStore {
	return &sessionStore{max: max, timeout: timeout, byID: map[string]*list.Element{}, order: list.New()}
}

func (st *sessionStore) user(r *http.Request, now time.Time) (string, bool) {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return "", false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	e, ok := st.byID[c.Value]
	if !ok {
		return "", false
	}
	ws := e.Value.(*webSession)
	if now.Sub(ws.last) > st.timeout {
		st.order.Remove(e)
		delete(st.byID, ws.id)
		return "", false
	}
	ws.last = now
	st.order.MoveToBack(e)
	return ws.user, true
}

// login binds user to a new session and sets the cookie. Unlike Jetty's
// getSession(true) it never reuses the presented session id (fixation): the
// presented session, if any, is dropped first.
func (st *sessionStore) login(w http.ResponseWriter, r *http.Request, user string, now time.Time) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if c, err := r.Cookie(sessionCookie); err == nil {
		if e, ok := st.byID[c.Value]; ok {
			st.order.Remove(e)
			delete(st.byID, c.Value)
		}
	}
	for st.order.Len() > 0 {
		front := st.order.Front()
		ws := front.Value.(*webSession)
		if st.order.Len() < st.max && now.Sub(ws.last) <= st.timeout {
			break
		}
		st.order.Remove(front)
		delete(st.byID, ws.id)
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return err
	}
	id := hex.EncodeToString(b[:])
	st.byID[id] = st.order.PushBack(&webSession{id: id, user: user, last: now})
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: id, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode})
	return nil
}

// ---- bearer token (UserToken, UserTokenService, UserTokenCache) ----

type userToken struct {
	userID    string
	token     string
	footprint int64 // unix seconds
	serverID  int32
}

func (t userToken) storeValue() string {
	return "V1." + strconv.FormatInt(t.footprint, 10) + "." + t.token + "." + t.userID
}
func (t userToken) bearer() string {
	return "V1." + t.token + "." + hexa32(int64(t.serverID)) + "." + t.userID
}

// tokenFromStore is UserToken.fromStoreValue; malformed items are skipped
// (Java throws NumberFormatException or NPE later, W11).
func tokenFromStore(v string) (userToken, bool) {
	arr := javaSplit(v, '.', 4)
	if len(arr) < 4 {
		return userToken{}, false
	}
	fp, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		return userToken{}, false
	}
	return userToken{userID: arr[3], token: arr[2], footprint: fp}, true
}

// tokenCache is UserTokenCache: StringKeyLinkedMap keyed by token, max 10000,
// oldest dropped first; putLast moves a renewed token to the end.
type tokenCache struct {
	mu    sync.Mutex
	max   int
	byKey map[string]*list.Element
	order *list.List
}

func newTokenCache(max int) *tokenCache {
	return &tokenCache{max: max, byKey: map[string]*list.Element{}, order: list.New()}
}

func (c *tokenCache) get(token string) (userToken, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.byKey[token]; ok {
		return e.Value.(userToken), true
	}
	return userToken{}, false
}

func (c *tokenCache) put(t userToken, last bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.byKey[t.token]; ok {
		e.Value = t
		if last {
			c.order.MoveToBack(e)
		}
		return
	}
	c.byKey[t.token] = c.order.PushBack(t)
	for c.order.Len() > c.max {
		front := c.order.Front()
		c.order.Remove(front)
		delete(c.byKey, front.Value.(userToken).token)
	}
}

// sessionExpireSec and touchSec follow UserTokenService's float arithmetic.
func (s *Server) sessionExpireSec() int64 {
	return int64(int32(float32(s.cfg.SessionTimeout.Seconds()) * float32(1.1)))
}
func (s *Server) touchSec() int64 {
	return int64(int32(float32(s.cfg.SessionTimeout.Seconds()) * float32(0.1)))
}

func newTokenString() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return strings.ToUpper(hex.EncodeToString(b[:])), nil
}

// validateBearer is UserToken.fromBearerToken + UserTokenService.validateToken.
// Malformed tokens and missing KV entries are 401 (Java 500, W1/W11), and a
// cached token must belong to the bearer's user id.
func (s *Server) validateBearer(ctx context.Context, bearer string) (string, error) {
	arr := javaSplit(bearer, '.', 4)
	if len(arr) < 4 {
		return "", errSessionExpired
	}
	serverID, err := parseHexa32(arr[2])
	if err != nil {
		return "", errSessionExpired
	}
	given := userToken{userID: arr[3], token: arr[1], serverID: int32(serverID)}
	trusted, ok := s.tokens.get(given.token)
	if ok && trusted.userID != given.userID {
		return "", errSessionExpired
	}
	if !ok {
		stored, err := s.storedTokens(ctx, given.userID)
		if err != nil {
			return "", err
		}
		for _, t := range stored {
			if t.token == given.token {
				trusted, ok = t, true
			}
		}
		if !ok {
			return "", errSessionExpired
		}
		s.tokens.put(trusted, false)
	}
	now := s.now().Unix()
	if trusted.footprint+s.sessionExpireSec() < now || trusted.footprint > now+tokenFutureSkew {
		return "", errSessionExpired
	}
	if trusted.footprint+s.touchSec() < now {
		renewed := userToken{userID: given.userID, token: given.token, footprint: now, serverID: given.serverID}
		s.tokens.put(renewed, true)
		if err := s.storeToken(ctx, renewed); err != nil {
			return "", err
		}
	}
	return given.userID, nil
}

// publishToken is UserTokenService.publishToken.
func (s *Server) publishToken(ctx context.Context, user string) (string, error) {
	token, err := newTokenString()
	if err != nil {
		return "", err
	}
	t := userToken{userID: user, token: token, footprint: s.now().Unix(), serverID: s.serverID()}
	s.tokens.put(t, false)
	if err := s.storeToken(ctx, t); err != nil {
		return "", err
	}
	return t.bearer(), nil
}

func (s *Server) storedText(ctx context.Context, user string) (string, error) {
	var text string
	err := s.call(ctx, "GET_CUSTOM_KV", newMap(entry("keySpace", textValue(webSessionSpace)), entry("key", textValue(user))), func(it Item) error {
		if it.Value != nil && text == "" {
			if v, ok := valueToString(*it.Value).(string); ok {
				text = v
			}
		}
		return nil
	})
	return text, err
}

func (s *Server) storedTokens(ctx context.Context, user string) ([]userToken, error) {
	text, err := s.storedText(ctx, user)
	if err != nil {
		return nil, err
	}
	var out []userToken
	for _, v := range strings.Split(text, ":") {
		if t, ok := tokenFromStore(v); ok {
			out = append(out, t)
		}
	}
	return out, nil
}

// storeToken merges t into the user's stored tokens (mergeStoredTokensWith,
// dropping expired ones) and writes them with ttl = sessionExpireSec.
func (s *Server) storeToken(ctx context.Context, t userToken) error {
	text, err := s.storedText(ctx, t.userID)
	if err != nil {
		return err
	}
	merged := []userToken{}
	if strings.TrimSpace(text) != "" {
		now := s.now().Unix()
		for _, v := range strings.Split(text, ":") {
			old, ok := tokenFromStore(v)
			if !ok || old.footprint+s.sessionExpireSec() < now || old.token == t.token {
				continue
			}
			merged = append(merged, old)
		}
	}
	merged = append(merged, t)
	parts := make([]string, len(merged))
	for i, m := range merged {
		parts[i] = m.storeValue()
	}
	param := newMap(entry("keySpace", textValue(webSessionSpace)), entry("key", textValue(t.userID)),
		entry("value", textValue(strings.Join(parts, ":"))), entry("ttl", decimalValue(s.sessionExpireSec())))
	ok := false
	err = s.call(ctx, "SET_CUSTOM_KV", param, func(it Item) error {
		if it.Value != nil && it.Value.Type == protocol.ValueBoolean {
			ok = it.Value.Bool
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("Error on setting value to kvstore!")
	}
	return nil
}
