package transport

import "sync"

// MaxClientConnSessions is the number of session ids one Client connection
// holds: only the one it used last. A Scouter Client uses a single session per
// server, and after a re-login it keeps sending commands on its pooled
// connections with the new id (scouter.client LoginMgr/TcpProxy), so a
// connection that carries a new id no longer uses the previous one. Holding
// more would keep every superseded session of a long-lived connection alive
// until the 3h TTL (a Client API consumer re-logging in on the same sockets
// left several sessions per connection).
const MaxClientConnSessions = 1

// ClientSessionObserver learns which Login sessions each live Client
// connection carries (Go-only; Java keeps no such link). Bound runs when an id
// joins a connection's tracked set: a command header carried it or the LOGIN
// handler called ClientConn.BindSession. Released runs once for every Bound,
// when the id is evicted from the set or the connection ends (EOF, error,
// CLOSE, shutdown). Calls for one connection are sequential; calls for
// different connections run concurrently. Both must be fast and nonblocking.
type ClientSessionObserver interface {
	ClientSessionBound(conn uint64, session int64)
	ClientSessionReleased(conn uint64, session int64)
}

// ClientConn identifies the Client connection a request arrived on. ID is
// unique per TCPServer and never 0 for a served connection. The zero value
// (handlers called without a served connection) tracks nothing.
type ClientConn struct {
	ID      uint64
	tracker *clientSessions
}

// BindSession ties session to this connection, as if a later command header
// had carried it. The LOGIN handler calls it with the session it created, so
// the session is held while the connection lives. The handler must call it
// before it returns.
func (c ClientConn) BindSession(session int64) {
	if c.tracker != nil {
		c.tracker.use(session)
	}
}

// clientSessions is the bounded, least-recently-used set of nonzero session
// ids one Client connection has carried (MaxClientConnSessions, so the last one). The connection goroutine owns it;
// mu only guards a handler that calls BindSession from another goroutine.
type clientSessions struct {
	mu       sync.Mutex
	id       uint64
	observer ClientSessionObserver
	keys     []int64 // oldest first
	closed   bool
}

func (c *clientSessions) use(session int64) {
	if session == 0 || c.observer == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	for i, key := range c.keys {
		if key == session {
			copy(c.keys[i:], c.keys[i+1:])
			c.keys[len(c.keys)-1] = session
			return
		}
	}
	if len(c.keys) == MaxClientConnSessions {
		evicted := c.keys[0]
		copy(c.keys, c.keys[1:])
		c.keys = c.keys[:len(c.keys)-1]
		c.observer.ClientSessionReleased(c.id, evicted)
	}
	c.keys = append(c.keys, session)
	c.observer.ClientSessionBound(c.id, session)
}

// release ends tracking: every held id is released once, oldest first.
func (c *clientSessions) release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	for _, key := range c.keys {
		c.observer.ClientSessionReleased(c.id, key)
	}
	c.keys = nil
}
