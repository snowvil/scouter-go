package service

import (
	"bufio"
	"context"
	"encoding/binary"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// sessionClock is the injected Service clock of the session liveness tests.
type sessionClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *sessionClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *sessionClock) Add(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// sessionServer serves one Service on a real transport listener so tests can
// open and close several Client connections.
type sessionServer struct {
	svc   *Service
	addr  string
	clock *sessionClock
}

func startSessionServer(t *testing.T, mutate func(*Config)) *sessionServer {
	t.Helper()
	clock := &sessionClock{now: time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)}
	config := queryConfig()
	config.Clock = clock.Now
	if mutate != nil {
		mutate(&config)
	}
	svc, err := New(config, &fakeBackend{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := transport.NewTCPServer(listener, transport.TCPConfig{}, svc.Handlers(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		server.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("server shutdown timeout")
		}
	})
	return &sessionServer{svc: svc, addr: listener.Addr().String(), clock: clock}
}

type sessionClient struct {
	conn net.Conn
	r    *bufio.Reader
}

func (s *sessionServer) dial(t *testing.T) *sessionClient {
	t.Helper()
	conn, err := net.Dial("tcp", s.addr)
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	var magic [4]byte
	binary.BigEndian.PutUint32(magic[:], transport.TCPClient)
	if _, err := conn.Write(magic[:]); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &sessionClient{conn: conn, r: bufio.NewReader(conn)}
}

func (c *sessionClient) login(t *testing.T, extra ...protocol.Entry) int64 {
	t.Helper()
	entries := append([]protocol.Entry{entry("id", text("synthetic")), entry("pass", text(encrypted("password")))}, extra...)
	send(t, c.conn, "LOGIN", 0, mp(entries...))
	session := mapValue(readPacks(t, c.r)[0].(*protocol.MapPack), "session").Int
	if session == 0 {
		t.Fatal("login failed")
	}
	return session
}

// loginList is GET_LOGIN_LIST: the listed session keys in table order.
func (c *sessionClient) loginList(t *testing.T, session int64) []int64 {
	t.Helper()
	send(t, c.conn, "GET_LOGIN_LIST", session, nil)
	packs := readPacks(t, c.r)
	if len(packs) == 0 {
		return nil
	}
	var keys []int64
	for _, v := range lookup(packs[0].(*protocol.MapPack), "session").Values {
		keys = append(keys, v.Int)
	}
	return keys
}

// waitLoginList advances the clock by step and polls GET_LOGIN_LIST until it
// equals want. Advancing on every poll makes the result independent of when
// the server goroutine notices a closed connection.
func (s *sessionServer) waitLoginList(t *testing.T, c *sessionClient, session int64, step time.Duration, want []int64) {
	t.Helper()
	var got []int64
	for range 50 {
		s.clock.Add(step)
		if got = c.loginList(t, session); reflect.DeepEqual(got, want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("GET_LOGIN_LIST = %v, want %v", got, want)
}

// pulse-rt: a consumer that LOGINs again on every new connection must not
// leave the sessions of its closed connections in the Login List.
func TestClosedConnectionSessionLeavesLoginList(t *testing.T) {
	s := startSessionServer(t, nil)
	a := s.dial(t)
	first := a.login(t)
	a.conn.Close()
	b := s.dial(t)
	second := b.login(t)
	// Within the grace both are listed, in login order.
	if got := b.loginList(t, second); !reflect.DeepEqual(got, []int64{first, second}) {
		t.Fatalf("within grace: %v", got)
	}
	s.waitLoginList(t, b, second, 30*time.Second+time.Millisecond, []int64{second})
}

// A Client API consumer that logs in again on its long-lived pooled
// connections made each connection carry several sessions, and the superseded
// ones never expired. A connection holds only the session it used last.
func TestReloginOnSameConnectionReleasesTheOldSession(t *testing.T) {
	s := startSessionServer(t, nil)
	c := s.dial(t)
	first := c.login(t)
	second := c.login(t) // same socket, new session
	if got := c.loginList(t, second); !reflect.DeepEqual(got, []int64{first, second}) {
		t.Fatalf("within grace: %v", got)
	}
	s.waitLoginList(t, c, second, 30*time.Second+time.Millisecond, []int64{second})
	// The live connection keeps the current session past the grace.
	s.clock.Add(time.Hour)
	if got := c.loginList(t, second); !reflect.DeepEqual(got, []int64{second}) {
		t.Fatalf("after an hour: %v", got)
	}
}

// waitSessionConns waits until key is held by n live connections (or is gone
// when n is 0).
func waitSessionConns(t *testing.T, svc *Service, key int64, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		svc.mu.Lock()
		got := 0
		if e, ok := svc.sessions[key]; ok {
			got = e.conns
		}
		svc.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("session %d held by %d connections, want %d", key, got, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitConns waits until key is held by n live connections (the server
// goroutine notices a close asynchronously).
func (s *sessionServer) waitConns(t *testing.T, key int64, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		s.svc.mu.Lock()
		got := -1
		if e, ok := s.svc.sessions[key]; ok {
			got = e.conns
		}
		s.svc.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("session %d held by %d connections, want %d", key, got, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// expectInvalidSession sends an authorized command with key on a fresh
// connection and expects INVALID_SESSION and the close.
func (s *sessionServer) expectInvalidSession(t *testing.T, key int64) {
	t.Helper()
	c := s.dial(t)
	send(t, c.conn, "GET_LOGIN_LIST", key, nil)
	if flag, err := c.r.ReadByte(); err != nil || flag != transport.InvalidSession {
		t.Fatalf("flag %x %v, want INVALID_SESSION", flag, err)
	}
}

func TestSessionHeldWhileAnotherConnectionUsesIt(t *testing.T) {
	s := startSessionServer(t, nil)
	a := s.dial(t)
	shared := a.login(t)
	c := s.dial(t)
	if got := c.loginList(t, shared); !reflect.DeepEqual(got, []int64{shared}) {
		t.Fatalf("second connection: %v", got)
	}
	s.waitConns(t, shared, 2)
	a.conn.Close()
	s.waitConns(t, shared, 1)
	s.clock.Add(time.Hour) // far past the grace, inside the 3h TTL
	d := s.dial(t)
	other := d.login(t)
	if got := d.loginList(t, other); !reflect.DeepEqual(got, []int64{shared, other}) {
		t.Fatalf("held by a live connection: %v", got)
	}
	c.conn.Close()
	s.waitConns(t, shared, 0)
	s.clock.Add(30 * time.Second)
	if got := d.loginList(t, other); !reflect.DeepEqual(got, []int64{shared, other}) {
		t.Fatalf("grace boundary is inclusive: %v", got)
	}
	s.clock.Add(time.Millisecond)
	if got := d.loginList(t, other); !reflect.DeepEqual(got, []int64{other}) {
		t.Fatalf("after the grace: %v", got)
	}
	s.expectInvalidSession(t, shared)
}

func TestSessionReconnectWithinGraceKeepsSession(t *testing.T) {
	s := startSessionServer(t, nil)
	a := s.dial(t)
	key := a.login(t)
	a.conn.Close()
	s.waitConns(t, key, 0)
	s.clock.Add(29 * time.Second)
	e := s.dial(t)
	if got := e.loginList(t, key); !reflect.DeepEqual(got, []int64{key}) {
		t.Fatalf("reconnect within grace: %v", got)
	}
	s.waitConns(t, key, 1)
	s.clock.Add(time.Hour)
	if !s.svc.validSession(key) {
		t.Fatal("CHECK_SESSION: session held by the new connection")
	}
	e.conn.Close()
	s.waitConns(t, key, 0)
	s.clock.Add(30*time.Second + time.Millisecond)
	if s.svc.validSession(key) {
		t.Fatal("CHECK_SESSION after the grace")
	}
	s.expectInvalidSession(t, key)
}

// internal="true" sessions follow the connection rule too: a Client API
// consumer that logs in with internal="true" on every new pooled connection
// and never reuses a session on another socket would, under the Java 7-day
// rule alone, leave one ghost per reconnect for a week.
func TestInternalSessionFollowsConnections(t *testing.T) {
	s := startSessionServer(t, nil)
	a := s.dial(t)
	released := a.login(t, entry("internal", text("TRUE")))
	a.conn.Close()
	s.waitConns(t, released, 0)
	d := s.dial(t)
	held := d.login(t, entry("internal", text("true")))
	s.waitLoginList(t, d, held, 30*time.Second+time.Millisecond, []int64{held})
	s.expectInvalidSession(t, released)
}

// A session held by a live connection has no TTL: a Client API consumer may
// keep its polling connections for days and send commands every few seconds
// under the connection's authorization latch. The session stays listed past the Java
// 3h/7d keep times and stays valid for CHECK_SESSION.
func TestHeldSessionOutlivesTTL(t *testing.T) {
	s := startSessionServer(t, nil)
	c := s.dial(t)
	normal := c.login(t)
	d := s.dial(t)
	internal := d.login(t, entry("internal", text("true")))
	s.clock.Add(8 * 24 * time.Hour)
	if got := c.loginList(t, normal); !reflect.DeepEqual(got, []int64{normal, internal}) {
		t.Fatalf("held sessions after 8 days: %v", got)
	}
	if !s.svc.validSession(internal) || !s.svc.validSession(normal) {
		t.Fatal("CHECK_SESSION of a held session")
	}
}

func TestSessionGraceFollowsLiveSoTimeout(t *testing.T) {
	s := startSessionServer(t, nil)
	live := DefaultSettings("1")
	live.ClientSoTimeoutMillis = 60000 // grace 120s
	s.svc.SetSettings(live)
	d := s.dial(t)
	other := d.login(t)
	a := s.dial(t)
	key := a.login(t)
	a.conn.Close()
	s.waitConns(t, key, 0)
	s.clock.Add(119 * time.Second)
	if got := d.loginList(t, other); !reflect.DeepEqual(got, []int64{other, key}) {
		t.Fatalf("inside a 120s grace: %v", got)
	}
	s.clock.Add(2 * time.Second)
	if got := d.loginList(t, other); !reflect.DeepEqual(got, []int64{other}) {
		t.Fatalf("after a 120s grace: %v", got)
	}
	// Back to 8000ms: the next close gets the 30s minimum.
	live.ClientSoTimeoutMillis = 8000
	s.svc.SetSettings(live)
	b := s.dial(t)
	again := b.login(t)
	b.conn.Close()
	s.waitConns(t, again, 0)
	s.clock.Add(31 * time.Second)
	if got := d.loginList(t, other); !reflect.DeepEqual(got, []int64{other}) {
		t.Fatalf("after a 30s grace: %v", got)
	}
}

func TestSessionGraceBounds(t *testing.T) {
	for _, tc := range []struct {
		millis int32
		want   time.Duration
	}{{-1, 30 * time.Second}, {0, 30 * time.Second}, {8000, 30 * time.Second}, {15000, 30 * time.Second}, {20000, 40 * time.Second}, {60000, 2 * time.Minute}, {300000, 10 * time.Minute}, {1<<31 - 1, 10 * time.Minute}} {
		if got := sessionGrace(tc.millis); got != tc.want {
			t.Errorf("sessionGrace(%d) = %v, want %v", tc.millis, got, tc.want)
		}
	}
}

// A connection that carries an expired session must not revive it, and the
// release of a binding that was refused is a no-op.
func TestExpiredSessionIsNotRevivedByBinding(t *testing.T) {
	clock := &sessionClock{now: time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)}
	config := queryConfig()
	config.Clock = clock.Now
	svc, err := New(config, &fakeBackend{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key, err := svc.newSession(mp(), "synthetic", false)
	if err != nil || key == 0 {
		t.Fatal(key, err)
	}
	// No connection ever held it: the grace counts from the login.
	clock.Add(30 * time.Second)
	if ok, _ := svc.Authorize(ctx, transport.ClientRequest{Session: key}); !ok {
		t.Fatal("inside the grace")
	}
	clock.Add(30*time.Second + time.Millisecond) // the request restarted the grace
	svc.ClientSessionBound(1, key)
	if ok, _ := svc.Authorize(ctx, transport.ClientRequest{Session: key}); ok {
		t.Fatal("expired session revived by a binding")
	}
	svc.ClientSessionReleased(1, key)
	if _, ok := svc.LoginUser(key); ok {
		t.Fatal("expired session left in the table")
	}
	// Bound/Released pairs of a live session keep the count balanced.
	live, _ := svc.newSession(mp(), "synthetic", false)
	svc.ClientSessionBound(1, live)
	svc.ClientSessionBound(2, live)
	svc.ClientSessionReleased(1, live)
	clock.Add(time.Hour)
	if ok, _ := svc.Authorize(ctx, transport.ClientRequest{Session: live}); !ok {
		t.Fatal("held by connection 2")
	}
	svc.ClientSessionReleased(2, live)
	svc.ClientSessionReleased(2, live) // unbalanced extra release stays at 0
	clock.Add(30*time.Second + time.Millisecond)
	if ok, _ := svc.Authorize(ctx, transport.ClientRequest{Session: live}); ok {
		t.Fatal("released session outlived the grace")
	}
}

// Logins, requests and closes on many connections race with the Login List,
// CHECK_SESSION and live settings changes. Afterwards only the session of the
// open connection remains.
func TestSessionLivenessConcurrent(t *testing.T) {
	s := startSessionServer(t, func(c *Config) { c.MaxSessions = 4096 })
	keeper := s.dial(t)
	kept := keeper.login(t)
	var clients sync.WaitGroup
	for range 6 {
		clients.Add(1)
		go func() {
			defer clients.Done()
			for range 15 {
				conn, err := net.Dial("tcp", s.addr)
				if err != nil {
					t.Error(err)
					return
				}
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				e := protocol.NewEncoder(protocol.Limits{})
				magic := transport.TCPClient
				e.Int32(int32(magic))
				e.Text("LOGIN")
				e.Int64(0)
				e.Pack(mp(entry("id", text("synthetic")), entry("pass", text(encrypted("password")))))
				e.Text("GET_LOGIN_LIST")
				e.Int64(kept)
				raw, err := e.Bytes()
				if err != nil {
					t.Error(err)
					conn.Close()
					return
				}
				if _, err := conn.Write(raw); err == nil {
					discardResponses(bufio.NewReader(conn), 2)
				}
				conn.Close()
			}
		}()
	}
	stop := make(chan struct{})
	settingsDone := make(chan struct{})
	go func() {
		defer close(settingsDone)
		live := DefaultSettings("1")
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			live.ClientSoTimeoutMillis = int32(8000 + i%3*30000)
			s.svc.SetSettings(live)
			s.svc.metadataLoginUsers()
			s.svc.validSession(kept)
			s.clock.Add(time.Millisecond)
		}
	}()
	clients.Wait()
	close(stop)
	<-settingsDone
	s.svc.SetSettings(DefaultSettings("1"))
	s.waitLoginList(t, keeper, kept, 30*time.Second+time.Millisecond, []int64{kept})
}

// discardResponses reads n complete responses; a read error ends it early
// (the race test does not check content).
func discardResponses(r *bufio.Reader, n int) {
	for n > 0 {
		flag, err := r.ReadByte()
		if err != nil {
			return
		}
		switch flag {
		case transport.NoNext:
			n--
		case transport.HasNext:
			if _, err := protocol.ReadPack(r, protocol.Limits{}); err != nil {
				return
			}
		default:
			return
		}
	}
}
