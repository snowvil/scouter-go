package service

import "time"

// Login session liveness (Go-only, intended difference from Java).
//
// Java LoginManager keeps a session for 3 hours after its last keepalive
// (okSession on a connection's first authorized command, CHECK_SESSION) and
// never removes it on logout or disconnect, so a consumer that LOGINs again on
// every reconnect leaves one ghost session per reconnect in GET_LOGIN_LIST.
// A Scouter Client recovers from an early removal by itself: TcpProxy.process
// clears the session on INVALID_SESSION (server.setSession(0)) and
// SessionObserver logs in again silently every 5 seconds
// (scouter.client net/TcpProxy.java:166-167, threads/SessionObserver.java:56-62).
//
// Here a session (internal or not) is valid while either holds:
//   - a live Client connection holds it (transport.ClientSessionObserver: the
//     connection carried it in a command header or LOGIN created it). No TTL
//     applies then: a consumer's long-lived pooled connection (a Client API
//     consumer may keep one for days and send commands every few seconds, all
//     under the connection's authorization latch) keeps its session listed
//     and usable;
//   - no connection holds it, its TTL deadline (SessionTTL, internal 7 days,
//     after the last keepalive) has not passed and less than sessionGrace has
//     passed since the last holder closed (or since the login or last request
//     when no connection ever held it).
//
// internal=true sessions follow the same rule: a Client API consumer that logs
// in with internal="true" on every new pooled connection and never reuses a
// session on another socket would, under the Java 7-day rule alone, leave one
// ghost per reconnect for a week. LOGIN, request authorization, CHECK_SESSION and GET_LOGIN_LIST (and
// the REMOTE_CONTROL session list) apply the same rule; an expired session is
// removed when one of them sees it, so a request with it gets INVALID_SESSION.
// The server never closes a connection because a session expired.

// internalSessionTTL is LoginManager.INTERNAL_USER_TIMEOUT for internal=true logins.
const internalSessionTTL = 7 * 24 * time.Hour

// Bounds of sessionGrace.
const (
	minSessionGrace = 30 * time.Second
	maxSessionGrace = 10 * time.Minute
)

type sessionEntry struct {
	user     MetadataLoginUser
	expires  time.Time // TTL deadline
	internal bool
	// conns counts the live Client connections holding the session; idleSince
	// is when it last dropped to 0 (login, last request or last close).
	conns     int
	idleSince time.Time
}

// sessionGrace is 2 x the live net_tcp_client_so_timeout_ms, within
// [30s, 10m]: a Client idle longer than its so-timeout loses its connection
// (Java and Go close it) and reconnects on its next request.
func sessionGrace(soTimeoutMillis int32) time.Duration {
	grace := 2 * time.Duration(soTimeoutMillis) * time.Millisecond
	return min(max(grace, minSessionGrace), maxSessionGrace)
}

func (s *Service) expiredLocked(e *sessionEntry, now time.Time) bool {
	if e.conns > 0 {
		return false
	}
	return now.After(e.expires) || now.After(e.idleSince.Add(sessionGrace(s.Settings().ClientSoTimeoutMillis)))
}

// liveSessionLocked returns a valid session or nil, removing an expired one.
func (s *Service) liveSessionLocked(key int64, now time.Time) *sessionEntry {
	e, ok := s.sessions[key]
	if !ok {
		return nil
	}
	if s.expiredLocked(e, now) {
		s.removeSessionLocked(key)
		return nil
	}
	return e
}

func (s *Service) purgeExpiredLocked(now time.Time) {
	for key, e := range s.sessions {
		if s.expiredLocked(e, now) {
			s.removeSessionLocked(key)
		}
	}
}

// ClientSessionBound implements transport.ClientSessionObserver. An unknown
// or already expired session is not revived: it stays absent.
func (s *Service) ClientSessionBound(_ uint64, key int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.liveSessionLocked(key, s.config.Clock()); e != nil {
		e.conns++
	}
}

// ClientSessionReleased implements transport.ClientSessionObserver. The
// grace period of a session starts when its last holder is released.
func (s *Service) ClientSessionReleased(_ uint64, key int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.sessions[key]
	if !ok || e.conns == 0 {
		return
	}
	if e.conns--; e.conns == 0 {
		e.idleSince = s.config.Clock()
	}
}
