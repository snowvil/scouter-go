package service_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/ingest"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/transport"
)

// xlogBoundsDir holds the original XLogService responses to
// TRANX_REAL_TIME_GROUP / TRANX_LOAD_TIME_GROUP under
// xlog_realtime/pasttime_lower_bound_ms and to XLOG_LOAD_BY_TXIDS /
// SEARCH_XLOG_LIST under req_search_xlog_max_count, each value applied by
// Configure.reload(true) (scripts/java-management.py, scenario "xlogbounds").
const xlogBoundsDir = "testdata/management/xlogbounds"

type xlogBoundsCase struct {
	Name           string  `json:"name"`
	Command        string  `json:"command"`
	Request        *string `json:"request"`
	Response       *string `json:"response"`
	RequestSHA256  string  `json:"request_sha256"`
	ResponseSHA256 string  `json:"response_sha256"`
	Bound          *int32  `json:"bound"`
	Max            *int32  `json:"max"`
	ItemCount      *int    `json:"item_count"`
	TxIDs          []int64 `json:"txids"`
}

type xlogHarness struct {
	svc  *service.Service
	conn net.Conn
	r    *bufio.Reader
	sess int64
}

// newXLogHarness ingests the oracle's XLog bytes into a real ingest engine
// over a temporary file store and serves it on a loopback Client listener.
func newXLogHarness(t *testing.T, loc *time.Location, now time.Time, xlogFile string) *xlogHarness {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.Open(storage.Config{Directory: filepath.Join(dir, "db"), Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	counters, err := storage.OpenCounters(filepath.Join(dir, "db"), loc)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := ingest.New(ingest.Config{Location: loc, Clock: func() time.Time { return now }}, store, counters)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close(); store.Close() })
	raw, err := os.ReadFile(filepath.Join(xlogBoundsDir, xlogFile))
	if err != nil {
		t.Fatal(err)
	}
	in := bytes.NewReader(raw)
	for in.Len() > 0 {
		p, err := protocol.ReadPack(in, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if err := engine.Accept(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	}
	svc, err := service.New(service.Config{Username: "synthetic", Password: "password", Location: loc, Clock: func() time.Time { return now }}, engine)
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
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(20 * time.Second))
	var magic [4]byte
	binary.BigEndian.PutUint32(magic[:], transport.TCPClient)
	if _, err := conn.Write(magic[:]); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		cancel()
		server.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("server shutdown timeout")
		}
	})
	h := &xlogHarness{svc: svc, conn: conn, r: bufio.NewReader(conn)}
	sum := sha256.Sum256([]byte("qwertyuiop!@#$%^&*()zxcvbnm,." + "password"))
	login := &protocol.MapPack{Entries: []protocol.Entry{{Key: "id", Value: protocol.Value{Type: protocol.ValueText, Text: "synthetic"}}, {Key: "pass", Value: protocol.Value{Type: protocol.ValueText, Text: hex.EncodeToString(sum[:])}}}}
	h.send(t, "LOGIN", 0, login)
	resp := h.response(t)
	p, err := protocol.DecodePack(resp[1:len(resp)-1], protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range p.(*protocol.MapPack).Entries {
		if e.Key == "session" {
			h.sess = e.Value.Int
		}
	}
	if h.sess == 0 {
		t.Fatal("login")
	}
	return h
}

func (h *xlogHarness) send(t *testing.T, command string, session int64, p protocol.Pack) {
	t.Helper()
	e := protocol.NewEncoder(protocol.Limits{})
	e.Text(command)
	e.Int64(session)
	e.Pack(p)
	raw, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.conn.Write(raw); err != nil {
		t.Fatal(err)
	}
}

// response returns the raw response bytes: HasNEXT+Pack items, then NoNEXT.
func (h *xlogHarness) response(t *testing.T) []byte {
	t.Helper()
	var out bytes.Buffer
	for {
		flag, err := h.r.ReadByte()
		if err != nil {
			t.Fatal(err)
		}
		out.WriteByte(flag)
		if flag == transport.NoNext {
			return out.Bytes()
		}
		if flag != transport.HasNext {
			t.Fatalf("flag %x", flag)
		}
		if _, err := protocol.ReadPack(io.TeeReader(h.r, &out), protocol.Limits{}); err != nil {
			t.Fatal(err)
		}
	}
}

func goldenBytes(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(xlogBoundsDir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func hexSHA(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

// TestXLogBoundsAndSearchCountJavaOracle compares the Go responses byte for
// byte with the Java handlers at the V2 boundaries (0, 1, 499, 500, 501 and
// negative values), with the same live value applied before each request.
func TestXLogBoundsAndSearchCountJavaOracle(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(xlogBoundsDir, "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Cases []xlogBoundsCase `json:"cases"`
		Facts struct {
			Input struct {
				BoundsBase int64 `json:"bounds_base_time"`
				SearchBase int64 `json:"search_base_time"`
			} `json:"input"`
		} `json:"facts"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	seoul, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	if golden.Facts.Input.BoundsBase == 0 || len(golden.Cases) != 47 {
		t.Fatal("golden input", len(golden.Cases))
	}
	bounds := newXLogHarness(t, seoul, time.UnixMilli(golden.Facts.Input.BoundsBase+10_000), "xlogs-bounds.bin")
	search := newXLogHarness(t, seoul, time.UnixMilli(golden.Facts.Input.SearchBase+10_000), "xlogs-search.bin")
	byteEqual, hashEqual := 0, 0
	for _, c := range golden.Cases {
		h := bounds
		live := h.svc.Settings()
		switch {
		case c.Bound != nil:
			live.XLogRealtimeLowerBoundMillis, live.XLogPasttimeLowerBoundMillis = *c.Bound, *c.Bound
		case c.Max != nil:
			h = search
			live = h.svc.Settings()
			live.SearchXLogMaxCount = *c.Max
		default:
			t.Fatal(c.Name, "no live value")
		}
		h.svc.SetSettings(live)
		var request []byte
		if c.Request != nil {
			request = goldenBytes(t, *c.Request)
		} else {
			// Large TXIDS/SEARCH requests are rebuilt from the oracle input.
			request = rebuildSearchRequest(t, c, golden.Facts.Input.SearchBase)
			if hexSHA(request) != c.RequestSHA256 {
				t.Fatalf("%s: rebuilt request differs from the Java request", c.Name)
			}
		}
		p, err := protocol.DecodePack(request, protocol.Limits{})
		if err != nil {
			t.Fatal(c.Name, err)
		}
		h.send(t, c.Command, h.sess, p)
		got := h.response(t)
		if c.Response != nil {
			if want := goldenBytes(t, *c.Response); !bytes.Equal(got, want) {
				t.Fatalf("%s: go %d bytes, java %d bytes", c.Name, len(got), len(want))
			}
			byteEqual++
			continue
		}
		if hexSHA(got) != c.ResponseSHA256 {
			t.Fatalf("%s: response hash differs (java %d items)", c.Name, *c.ItemCount)
		}
		hashEqual++
	}
	if byteEqual != 41 || hashEqual != 6 {
		t.Fatal(byteEqual, hashEqual)
	}
}

// rebuildSearchRequest recreates the TXIDS/SEARCH request bodies whose .bin
// files the oracle does not keep (max >= 499): the same MapPack as
// LiveConfOracle.xlogBounds.
func rebuildSearchRequest(t *testing.T, c xlogBoundsCase, base int64) []byte {
	t.Helper()
	var p protocol.MapPack
	switch c.Command {
	case "XLOG_LOAD_BY_TXIDS":
		ids := make([]protocol.Value, 502)
		for i := range ids {
			ids[i] = protocol.Value{Type: protocol.ValueDecimal, Int: int64(5001 + i)}
		}
		p.Entries = []protocol.Entry{{Key: "date", Value: protocol.Value{Type: protocol.ValueText, Text: "20261014"}}, {Key: "txid", Value: protocol.Value{Type: protocol.ValueList, Values: ids}}}
	case "SEARCH_XLOG_LIST":
		p.Entries = []protocol.Entry{{Key: "stime", Value: protocol.Value{Type: protocol.ValueDecimal, Int: base - 1}}, {Key: "etime", Value: protocol.Value{Type: protocol.ValueDecimal, Int: base + 600_000}}}
	default:
		t.Fatal(c.Name, c.Command)
	}
	b, err := protocol.EncodePack(&p, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
