package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/config"
	"github.com/snowvil/scouter-go/internal/httpapi"
)

// rawGet sends one HTTP/1.1 GET on its own connection and returns the
// connection and a reader for the response.
func rawGet(t *testing.T, addr, path string) (net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if _, err := fmt.Fprintf(c, "GET %s HTTP/1.1\r\nHost: x\r\n\r\n", path); err != nil {
		t.Fatal(err)
	}
	return c, bufio.NewReader(c)
}

func readResponse(t *testing.T, c net.Conn, r *bufio.Reader) *http.Response {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(r, nil)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

type httpLines struct {
	mu    sync.Mutex
	lines []string
}

func (l *httpLines) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *httpLines) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.lines)
}

// Connections past the listener limit get 503 + Connection: close, except
// for /health; past the over-limit connections they are closed at accept.
// Slots come back when connections close.
func TestHTTPConnectionLimit(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 8)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok") })
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
	})
	limits := newHTTPLimits(2, 1, 4)
	lines := &httpLines{}
	limits.logf = lines.logf
	s, err := listenHTTPWith("127.0.0.1:0", mux, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		s.close()
	}()
	s.serve(ctx)
	held := make([]net.Conn, 2)
	readers := make([]*bufio.Reader, 2)
	for i := range held {
		held[i], readers[i] = rawGet(t, s.addr, "/scouter/x")
		<-entered
	}
	// The server closes an over-limit connection after its answer; wait for
	// its slot so the next connection is not closed at accept.
	overFree := func() {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for limits.stats().OverLimitConnections != 0 {
			if time.Now().After(deadline) {
				t.Fatalf("over-limit slot not released: %+v", limits.stats())
			}
			time.Sleep(time.Millisecond)
		}
	}
	// Over the limit: 503 and closed, the handler is not reached.
	over, r := rawGet(t, s.addr, "/scouter/x")
	resp := readResponse(t, over, r)
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "1" || !resp.Close {
		t.Fatalf("over-limit response %d %v close=%v", resp.StatusCode, resp.Header, resp.Close)
	}
	overFree()
	// /health is still served on an over-limit connection.
	over, r = rawGet(t, s.addr, "/health")
	if resp := readResponse(t, over, r); resp.StatusCode != 200 || !resp.Close {
		t.Fatalf("health on over-limit connection %d close=%v", resp.StatusCode, resp.Close)
	}
	overFree()
	// Hold the over-limit slot open without a request: the next connection
	// is closed at accept.
	idle, err := net.DialTimeout("tcp", s.addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	deadline := time.Now().Add(2 * time.Second)
	for limits.stats().OverLimitConnections != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("over-limit slot not taken: %+v", limits.stats())
		}
		time.Sleep(time.Millisecond)
	}
	closed, r := rawGet(t, s.addr, "/health")
	closed.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := http.ReadResponse(r, nil); err == nil {
		t.Fatal("connection past the over-limit slots got a response")
	}
	st := limits.stats()
	if st.Connections != 2 || st.MaxConnections != 2 || st.OverLimit != 3 || st.ClosedAtAccept != 1 || st.OverLimitRejected != 1 || st.Operational.Served != 1 {
		t.Fatalf("stats %+v", st)
	}
	if n := lines.count(); n != 1 {
		t.Fatalf("%d log lines, want 1 (rate-limited)", n)
	}
	// Closing a held connection frees its slot for a new one.
	close(release)
	for i := range held {
		readResponse(t, held[i], readers[i])
		held[i].Close()
	}
	idle.Close()
	deadline = time.Now().Add(2 * time.Second)
	for st := limits.stats(); st.Connections != 0 || st.OverLimitConnections != 0; st = limits.stats() {
		if time.Now().After(deadline) {
			t.Fatalf("slots not released: %+v", st)
		}
		time.Sleep(time.Millisecond)
	}
	c, r := rawGet(t, s.addr, "/health")
	if resp := readResponse(t, c, r); resp.StatusCode != 200 || resp.Close {
		t.Fatalf("after release %d close=%v", resp.StatusCode, resp.Close)
	}
}

// The operational gate never waits: a busy /health or pprof answers 503 at
// once; other paths do not use it.
func TestHTTPOperationalGate(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 2)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			entered <- struct{}{}
			<-release
		}
	})
	limits := newHTTPLimits(8, 1, 1)
	limits.logf = func(string, ...any) {}
	h := limits.handler(next)
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/health", nil))
	}()
	<-entered
	start := time.Now()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/debug/pprof/heap", nil))
	if rec.Code != http.StatusServiceUnavailable || time.Since(start) > time.Second {
		t.Fatalf("busy operational gate %d after %v", rec.Code, time.Since(start))
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/counter", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("servlet path used the operational gate: %d", rec.Code)
	}
	close(release)
	<-done
	if st := limits.ops.stats(); st.Served != 1 || st.Rejected != 1 || st.Waited != 0 {
		t.Fatalf("%+v", st)
	}
}

// The webapp gate waits up to its wait: a request is served when a slot frees
// in time, answered 503 after the wait, and counted as abandoned when the
// client leaves first. The 503 line is rate-limited.
func TestHTTPGateWait(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 4)
	g := newHTTPGate("webapp", 1, 100*time.Millisecond)
	lines := &httpLines{}
	g.logf = lines.logf
	h := g.handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		entered <- struct{}{}
		<-release
	}))
	serve := func(ctx context.Context) <-chan int {
		got := make(chan int, 1)
		go func() {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest("GET", "/scouter/v1/x", nil).WithContext(ctx))
			got <- rec.Code
		}()
		return got
	}
	first := serve(context.Background())
	<-entered
	for range 2 {
		if code := <-serve(context.Background()); code != http.StatusServiceUnavailable {
			t.Fatal(code)
		}
	}
	if n := lines.count(); n != 1 || !strings.Contains(lines.lines[0], "webapp") {
		t.Fatalf("lines %q", lines.lines)
	}
	ctx, cancel := context.WithCancel(context.Background())
	left := serve(ctx)
	for g.stats().Waiting != 1 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-left
	waiting := serve(context.Background())
	for g.stats().Waiting != 1 {
		time.Sleep(time.Millisecond)
	}
	release <- struct{}{}
	<-first
	<-entered
	release <- struct{}{}
	if code := <-waiting; code != http.StatusOK {
		t.Fatal(code)
	}
	if st := g.stats(); st.Served != 2 || st.Waited != 4 || st.Rejected != 2 || st.Abandoned != 1 || st.InFlight != 0 || st.Waiting != 0 {
		t.Fatalf("%+v", st)
	}
}

// Idle keep-alive connections do not keep the listener full: past 7/8 of the
// connection limit responses close their connection, so a new client is
// served instead of answered 503 until the idle timeout.
func TestHTTPKeepAliveRotatesNearLimit(t *testing.T) {
	limits := newHTTPLimits(4, 1, 4)
	limits.logf = func(string, ...any) {}
	s, err := listenHTTPWith("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok") }), limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		s.close()
	}()
	s.serve(ctx)
	get := func() int {
		t.Helper()
		tr := &http.Transport{}
		t.Cleanup(tr.CloseIdleConnections)
		resp, err := (&http.Client{Transport: tr, Timeout: 2 * time.Second}).Get("http://" + s.addr + "/telegraf/x")
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	// Four push hosts, each keeping its connection alive.
	for range 4 {
		if code := get(); code != 200 {
			t.Fatal(code)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for limits.stats().Connections >= 4 {
		if time.Now().After(deadline) {
			t.Fatalf("no connection rotated: %+v", limits.stats())
		}
		time.Sleep(time.Millisecond)
	}
	if code := get(); code != 200 {
		t.Fatalf("a fifth host got %d", code)
	}
	if st := limits.stats(); st.KeepAliveClosed == 0 || st.OverLimit != 0 {
		t.Fatalf("%+v", st)
	}
}

// A listener's connections and over-limit connections stay within a quarter
// of the descriptor reserve.
func TestHTTPConnectionLimitFitsReserve(t *testing.T) {
	for fd, want := range map[uint64]int{1024: 192, 4096: 192, 16384: 960, 65536: 1024} {
		got := httpConnectionLimitFor(httpMaxConnections, fd)
		if got != want || uint64(got+httpOverflowConns) > fdReserve(fd)/4 {
			t.Errorf("fd %d: limit %d want %d", fd, got, want)
		}
	}
	// go_http_max_connections raises the cap only as far as the reserve
	// allows, and lowers it below the default.
	for _, c := range []struct {
		want int
		fd   uint64
		got  int
	}{{4096, 1 << 18, 4096}, {4096, 65536, 4032}, {4096, 16384, 960}, {100, 65536, 100}, {100, 1024, 100}} {
		if got := httpConnectionLimitFor(c.want, c.fd); got != c.got {
			t.Errorf("want %d fd %d: limit %d, expected %d", c.want, c.fd, got, c.got)
		}
	}
}

// The go_http_* keys set the listener, servlet, webapp and operational
// bounds; zero fields keep the defaults.
func TestHTTPAdmissionFromLimits(t *testing.T) {
	if d := (httpAdmission{}).resolved(); d != (httpAdmission{maxConns: httpMaxConnections, opsInFlight: httpOpsInFlight, servletInFlight: httpapi.DefaultMaxInFlight,
		webappInFlight: httpWebappInFlight, servletWait: httpapi.DefaultQueueWait, webappWait: httpQueueWait}) {
		t.Fatalf("defaults %+v", d)
	}
	a := httpAdmissionOf(config.GoLimits{HTTPMaxConnections: 300, HTTPOpsInFlight: 3, HTTPServletInFlight: 5, HTTPServletQueueWaitMs: 1500, HTTPWebappInFlight: 7, HTTPWebappQueueWaitMs: 250})
	if got := a.resolved(); got != (httpAdmission{maxConns: 300, opsInFlight: 3, servletInFlight: 5, webappInFlight: 7, servletWait: 1500 * time.Millisecond, webappWait: 250 * time.Millisecond}) {
		t.Fatalf("resolved %+v", got)
	}
	s, err := listenHTTP("127.0.0.1:0", http.NotFoundHandler(), httpAdmission{maxConns: 20, opsInFlight: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if st := s.limits.stats(); st.MaxConnections != 20 || st.Operational.Limit != 3 || cap(s.limits.over) != httpOverflowConns {
		t.Fatalf("listener %+v", st)
	}
}
