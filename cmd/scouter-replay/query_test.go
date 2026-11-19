package main

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// idleClosingServer speaks the Client framing and closes a connection whose
// next request arrives more than serverIdle after the previous response,
// without answering it (the server idle close seen from the client).
type idleClosingServer struct {
	ln      net.Listener
	wg      sync.WaitGroup
	mu      sync.Mutex
	latest  int
	indexes []int64 // requested TRANX_REAL_TIME_GROUP index, in order
}

func newIdleClosingServer(t *testing.T) *idleClosingServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &idleClosingServer{ln: ln}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer c.Close()
				s.serve(c)
			}()
		}
	}()
	t.Cleanup(func() { ln.Close(); s.wg.Wait() })
	return s
}

func readTestText(r *bufio.Reader) (string, error) {
	n, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	if n >= 254 {
		return "", io.ErrUnexpectedEOF // long texts are not used here
	}
	b := make([]byte, n)
	_, err = io.ReadFull(r, b)
	return string(b), err
}

func mapInt64(m *protocol.MapPack, key string) int64 {
	for _, e := range m.Entries {
		if e.Key == key {
			return e.Value.Int
		}
	}
	return 0
}

func (s *idleClosingServer) serve(c net.Conn) {
	r := bufio.NewReader(c)
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return
	}
	var last time.Time
	for {
		cmd, err := readTestText(r)
		if err != nil || cmd == "CLOSE" {
			return
		}
		if !last.IsZero() && time.Since(last) > serverIdle {
			return // idle close: the request is never answered
		}
		var session [8]byte
		if _, err := io.ReadFull(r, session[:]); err != nil {
			return
		}
		p, err := protocol.ReadPack(r, protocol.Limits{})
		if err != nil {
			return
		}
		param, _ := p.(*protocol.MapPack)
		var out []protocol.Pack
		switch cmd {
		case "LOGIN":
			out = append(out, &protocol.MapPack{Entries: []protocol.Entry{{Key: "session", Value: decimal(42)}}})
		case "TRANX_REAL_TIME_GROUP_LATEST":
			s.mu.Lock()
			s.latest++
			s.mu.Unlock()
			out = append(out, &protocol.MapPack{Entries: []protocol.Entry{{Key: "loop", Value: decimal(1)}, {Key: "index", Value: decimal(100)}}})
		case "TRANX_REAL_TIME_GROUP":
			idx := mapInt64(param, "index")
			s.mu.Lock()
			s.indexes = append(s.indexes, idx)
			s.mu.Unlock()
			out = append(out, &protocol.MapPack{Entries: []protocol.Entry{{Key: "loop", Value: decimal(1)}, {Key: "index", Value: decimal(idx + 1)}}})
		}
		var buf []byte
		for _, x := range out {
			b, err := protocol.EncodePack(x, protocol.Limits{})
			if err != nil {
				return
			}
			buf = append(append(buf, flagHasNext), b...)
		}
		if _, err := c.Write(append(buf, flagNoNext)); err != nil {
			return
		}
		last = time.Now()
	}
}

// serverIdle is the fake server's idle close; the worker's interval (80ms)
// always exceeds it, back-to-back requests (LATEST then realtime) do not.
const serverIdle = 25 * time.Millisecond

func TestQueryWorkerIdleReconnect(t *testing.T) {
	srv := newIdleClosingServer(t)
	cfg := queryConfig{Addr: srv.ln.Addr().String(), User: "admin", Password: "admin", Concurrency: 1,
		Interval: 80 * time.Millisecond, Window: time.Second, Timeout: 2 * time.Second, Location: time.UTC, IdleClose: 20 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rep := runQueries(ctx, cfg).report()
	for cmd, c := range rep.Commands {
		if c.Errors != 0 {
			t.Fatalf("%s: %d errors (%s)", cmd, c.Errors, c.LastError)
		}
	}
	if rep.IdleReconnects < 4 {
		t.Fatalf("idle reconnects %d: %+v", rep.IdleReconnects, rep)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	// The realtime cursor survives the reconnects: one LATEST, then every
	// request continues from the previous response.
	if srv.latest != 1 || len(srv.indexes) < 2 {
		t.Fatalf("latest=%d indexes=%v", srv.latest, srv.indexes)
	}
	for i, idx := range srv.indexes {
		if idx != int64(100+i) {
			t.Fatalf("cursor not kept: %v", srv.indexes)
		}
	}
}

// TestQueryWorkerEOFBeforeIdleIsError: a connection closed before the idle
// threshold is a server error, counted as before.
func TestQueryWorkerEOFBeforeIdleIsError(t *testing.T) {
	srv := newIdleClosingServer(t)
	cfg := queryConfig{Addr: srv.ln.Addr().String(), User: "admin", Password: "admin", Concurrency: 1,
		Interval: 80 * time.Millisecond, Window: time.Second, Timeout: 2 * time.Second, Location: time.UTC, IdleClose: time.Hour}
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	rep := runQueries(ctx, cfg).report()
	var errs uint64
	for _, c := range rep.Commands {
		errs += c.Errors
	}
	if errs == 0 || rep.IdleReconnects != 0 {
		t.Fatalf("errors=%d idle reconnects=%d", errs, rep.IdleReconnects)
	}
}
