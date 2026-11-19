package webapp

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// fakeCollector speaks the Client TCP framing on loopback with fixed
// per-command responses. Unknown commands close the connection, like the Go
// server's unsupported-command path.

type bodyKind int

const (
	bodyPack bodyKind = iota
	bodyValue
	bodyNone
)

type fakeReq struct {
	Cmd   string
	Pack  *protocol.MapPack
	Value protocol.Value
}

type fakeHandler struct {
	body bodyKind
	// fn returns response items: protocol.Pack or protocol.Value.
	fn func(req fakeReq) ([]any, error)
}

type fakeCollector struct {
	t        *testing.T
	ln       net.Listener
	mu       sync.Mutex
	handlers map[string]fakeHandler
	sessions map[int64]bool
	next     int64
	reqs     []fakeReq
	kv       map[string]string
	dials    atomic.Int64
	active   atomic.Int64
	maxAct   atomic.Int64
	logins   atomic.Int64
	wg       sync.WaitGroup
	conns    map[net.Conn]bool
	// loginGate, when set before use, blocks LOGIN until closed.
	loginGate chan struct{}
	// drops[cmd] > 0 closes the connection right after reading that
	// command's name, without a response byte (the idle-close race seen
	// from the client), and decrements the count.
	drops map[string]int
}

func (f *fakeCollector) dropNext(cmd string, n int) {
	f.mu.Lock()
	if f.drops == nil {
		f.drops = map[string]int{}
	}
	f.drops[cmd] = n
	f.mu.Unlock()
}

func (f *fakeCollector) takeDrop(cmd string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.drops[cmd] > 0 {
		f.drops[cmd]--
		return true
	}
	return false
}

func newFakeCollector(t *testing.T) *fakeCollector {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeCollector{t: t, ln: ln, handlers: map[string]fakeHandler{}, sessions: map[int64]bool{}, kv: map[string]string{}, conns: map[net.Conn]bool{}}
	f.handleKV()
	f.wg.Add(1)
	go f.accept()
	t.Cleanup(f.close)
	return f
}

func (f *fakeCollector) addr() string { return f.ln.Addr().String() }

func (f *fakeCollector) close() {
	f.ln.Close()
	f.mu.Lock()
	for c := range f.conns {
		c.Close()
	}
	f.mu.Unlock()
	f.wg.Wait()
}

func (f *fakeCollector) handle(cmd string, body bodyKind, fn func(fakeReq) ([]any, error)) {
	f.mu.Lock()
	f.handlers[cmd] = fakeHandler{body, fn}
	f.mu.Unlock()
}

// static registers a Pack-body command answering fixed items.
func (f *fakeCollector) static(cmd string, items ...any) {
	f.handle(cmd, bodyPack, func(fakeReq) ([]any, error) { return items, nil })
}

func (f *fakeCollector) invalidateSessions() {
	f.mu.Lock()
	f.sessions = map[int64]bool{}
	f.mu.Unlock()
}

func (f *fakeCollector) requests(cmd string) []fakeReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakeReq
	for _, r := range f.reqs {
		if r.Cmd == cmd {
			out = append(out, r)
		}
	}
	return out
}

func (f *fakeCollector) kvGet(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.kv[key]
}

func (f *fakeCollector) handleKV() {
	f.handle("GET_CUSTOM_KV", bodyPack, func(r fakeReq) ([]any, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		v, ok := f.kv[mapText(r.Pack, "keySpace")+"/"+mapText(r.Pack, "key")]
		if !ok {
			return []any{protocol.Value{Type: protocol.ValueNull}}, nil
		}
		return []any{textValue(v)}, nil
	})
	f.handle("SET_CUSTOM_KV", bodyPack, func(r fakeReq) ([]any, error) {
		f.mu.Lock()
		f.kv[mapText(r.Pack, "keySpace")+"/"+mapText(r.Pack, "key")] = mapText(r.Pack, "value")
		f.mu.Unlock()
		return []any{protocol.Value{Type: protocol.ValueBoolean, Bool: true}}, nil
	})
}

func (f *fakeCollector) accept() {
	defer f.wg.Done()
	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		f.dials.Add(1)
		f.mu.Lock()
		f.conns[c] = true
		f.mu.Unlock()
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			defer func() {
				f.mu.Lock()
				delete(f.conns, c)
				f.mu.Unlock()
				c.Close()
			}()
			f.serve(c)
		}()
	}
}

func readText(r *bufio.Reader) (string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	n := int(b)
	switch b {
	case 255:
		var p [2]byte
		if _, err := io.ReadFull(r, p[:]); err != nil {
			return "", err
		}
		n = int(binary.BigEndian.Uint16(p[:]))
	case 254:
		var p [4]byte
		if _, err := io.ReadFull(r, p[:]); err != nil {
			return "", err
		}
		n = int(binary.BigEndian.Uint32(p[:]))
	}
	buf := make([]byte, n)
	_, err = io.ReadFull(r, buf)
	return string(buf), err
}

func (f *fakeCollector) serve(c net.Conn) {
	r := bufio.NewReader(c)
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil || binary.BigEndian.Uint32(magic[:]) != tcpClientMagic {
		return
	}
	authed := false
	for {
		cmd, err := readText(r)
		if err != nil || cmd == "CLOSE" || f.takeDrop(cmd) {
			return
		}
		var sb [8]byte
		if _, err := io.ReadFull(r, sb[:]); err != nil {
			return
		}
		session := int64(binary.BigEndian.Uint64(sb[:]))
		if cmd == "LOGIN" {
			p, err := protocol.ReadPack(r, protocol.Limits{})
			if err != nil {
				return
			}
			m := p.(*protocol.MapPack)
			if f.loginGate != nil {
				<-f.loginGate
			}
			f.logins.Add(1)
			out := newMap(entry("id", textValue(mapText(m, "id"))))
			if mapText(m, "id") == "admin" && mapText(m, "pass") == scouterPassword("secret") {
				f.mu.Lock()
				f.next++
				s := f.next*7919 + 13
				f.sessions[s] = true
				f.mu.Unlock()
				out.Entries = append(out.Entries, entry("session", decimalValue(s)), entry("time", decimalValue(time.Now().UnixMilli())),
					entry("server_id", textValue("fake-server")), entry("version", textValue("fake 1.0")))
			} else {
				out.Entries = append(out.Entries, entry("session", decimalValue(0)), entry("error", textValue("login fail")))
			}
			if !f.write(c, []any{out}) {
				return
			}
			continue
		}
		f.mu.Lock()
		valid := f.sessions[session]
		h, known := f.handlers[cmd]
		f.mu.Unlock()
		if !authed {
			if !valid {
				c.Write([]byte{flagInvalid})
				return
			}
			authed = true
		}
		if !known {
			return // unsupported: close without terminator
		}
		req := fakeReq{Cmd: cmd}
		switch h.body {
		case bodyPack:
			p, err := protocol.ReadPack(r, protocol.Limits{})
			if err != nil {
				return
			}
			req.Pack, _ = p.(*protocol.MapPack)
		case bodyValue:
			if req.Value, err = protocol.ReadValue(r, protocol.Limits{}); err != nil {
				return
			}
		}
		f.mu.Lock()
		f.reqs = append(f.reqs, req)
		f.mu.Unlock()
		n := f.active.Add(1)
		for {
			m := f.maxAct.Load()
			if n <= m || f.maxAct.CompareAndSwap(m, n) {
				break
			}
		}
		items, err := h.fn(req)
		f.active.Add(-1)
		if err != nil {
			return
		}
		if !f.write(c, items) {
			return
		}
	}
}

var fakeResponseLimits = protocol.Limits{MaxCollectionEntries: 1 << 20, MaxTotalValues: 1 << 21}

func (f *fakeCollector) write(c net.Conn, items []any) bool {
	var buf []byte
	for _, it := range items {
		var b []byte
		var err error
		switch v := it.(type) {
		case protocol.Pack: // the collector's Client response limits
			b, err = protocol.EncodePack(v, fakeResponseLimits)
		case protocol.Value:
			b, err = protocol.EncodeValue(v, fakeResponseLimits)
		case time.Duration: // a pause: send what came before, then wait
			if _, err := c.Write(buf); err != nil {
				return false
			}
			buf = buf[:0]
			time.Sleep(v)
			continue
		default:
			err = fmt.Errorf("bad item %T", it)
		}
		if err != nil {
			f.t.Errorf("fake encode: %v", err)
			return false
		}
		buf = append(buf, flagHasNext)
		buf = append(buf, b...)
	}
	buf = append(buf, flagNoNext)
	_, err := c.Write(buf)
	return err == nil
}

// ---- test server helpers ----

func testConfig(f *fakeCollector) Config {
	cfg := DefaultConfig()
	cfg.CollectorAddr = f.addr()
	cfg.Username, cfg.Password = "admin", "secret"
	cfg.Location = time.UTC
	cfg.Logf = func(string, ...any) {}
	cfg.CallTimeout = 5 * time.Second
	return cfg
}

func newTestServer(t *testing.T, cfg Config) (*Server, *httptest.Server) {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(s.Handler())
	t.Cleanup(func() { hs.Close(); s.Close() })
	return s, hs
}

type httpResult struct {
	status int
	header http.Header
	body   string
}

func doHTTP(t *testing.T, client *http.Client, method, url, body string, header map[string]string) httpResult {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	return httpResult{resp.StatusCode, resp.Header, string(b)}
}

// resultOf strips the CommonResultView wrapper prefix up to "result":.
func resultOf(t *testing.T, body string) string {
	t.Helper()
	i := strings.Index(body, `"result":`)
	if i < 0 || !strings.HasSuffix(body, "}") {
		t.Fatalf("not a result view: %s", body)
	}
	return body[i+len(`"result":`) : len(body)-1]
}
