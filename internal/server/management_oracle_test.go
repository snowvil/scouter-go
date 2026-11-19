package server

// End-to-end Java oracle replay for the configuration, counters-site, alert
// script and server thread commands. The goldens under
// ../service/testdata/management/<scenario> come from scripts/java-management.py,
// which runs the unchanged Scouter 2.21.3 ConfigureService,
// AlertScriptingService, ThreadList, Configure, CounterManager and
// AlertRuleLoader in a temporary working directory (Configure.CONF_DIR
// "./conf/", plugin_dir "./plugin"). Here the whole Go Runtime is opened on the
// same layout (scouter.conf store, ConfDir, PluginDirectory, TelegrafConfigurationPath)
// and every golden request is sent in order over a real Client connection.
//
// Each case is either byte-identical to Java (exact), identical apart from
// MapValue entry order (mapOrder: Java writes Hashtable enumeration order, Go
// insertion order, internal/protocol/value.go), or an intended difference with
// a reason and a Go-specific assertion. Every golden case must be classified.
// A missing golden fails the test.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/config"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

const moGoldenDir = "../service/testdata/management"

type moCase struct {
	Name     string `json:"name"`
	Command  string `json:"command"`
	Request  string `json:"request"`
	Response string `json:"response"`
	ItemKind string `json:"item_kind"`
}

type moGolden struct {
	dir   string
	cases []moCase
	facts map[string]json.RawMessage
}

func moLoad(t *testing.T, scenario string) moGolden {
	t.Helper()
	dir := filepath.Join(moGoldenDir, scenario)
	data, err := os.ReadFile(filepath.Join(dir, "cases.json"))
	if err != nil {
		t.Fatalf("Java oracle golden missing (python3 scripts/java-management.py --generate): %v", err)
	}
	var root struct {
		Scenario string                     `json:"scenario"`
		Cases    []moCase                   `json:"cases"`
		Facts    map[string]json.RawMessage `json:"facts"`
	}
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	if root.Scenario != scenario {
		t.Fatalf("golden %s has scenario %q", dir, root.Scenario)
	}
	return moGolden{dir: dir, cases: root.Cases, facts: root.Facts}
}

func (g moGolden) read(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(g.dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// moItem is one response item; moResponse a whole answer (complete=false when
// the server closed the connection instead of writing NoNEXT).
type moItem struct {
	pack  protocol.Pack
	value protocol.Value
}

type moResponse struct {
	raw      []byte
	items    []moItem
	complete bool
}

type moCapture struct {
	r   io.Reader
	buf bytes.Buffer
}

func (c *moCapture) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.buf.Write(p[:n])
	return n, err
}

func moRead(t *testing.T, r *bufio.Reader, kind string) moResponse {
	t.Helper()
	c := &moCapture{r: r}
	var out moResponse
	for {
		flag, err := r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) {
				out.raw = c.buf.Bytes()
				return out
			}
			t.Fatalf("response flag: %v", err)
		}
		c.buf.WriteByte(flag)
		if flag == transport.NoNext {
			out.raw, out.complete = c.buf.Bytes(), true
			return out
		}
		if flag != transport.HasNext {
			t.Fatalf("response flag %x", flag)
		}
		if kind == "pack" {
			p, err := protocol.ReadPack(c, protocol.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			out.items = append(out.items, moItem{pack: p})
		} else {
			v, err := protocol.ReadValue(c, protocol.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			out.items = append(out.items, moItem{value: v})
		}
	}
}

func moDecodeJava(t *testing.T, raw []byte, kind string) moResponse {
	t.Helper()
	r := moRead(t, bufio.NewReader(bytes.NewReader(raw)), kind)
	if !r.complete || !bytes.Equal(r.raw, raw) {
		t.Fatal("golden response does not round-trip through the Go reader")
	}
	return r
}

func moEncode(t *testing.T, items []moItem) []byte {
	t.Helper()
	e := protocol.NewEncoder(protocol.Limits{})
	for _, it := range items {
		e.Byte(transport.HasNext)
		if it.pack != nil {
			e.Pack(it.pack)
		} else {
			e.Value(it.value)
		}
	}
	e.Byte(transport.NoNext)
	b, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func moSorted(v protocol.Value) protocol.Value {
	switch v.Type {
	case protocol.ValueMap:
		entries := make([]protocol.Entry, len(v.Entries))
		for i, e := range v.Entries {
			entries[i] = protocol.Entry{Key: e.Key, Value: moSorted(e.Value)}
		}
		sort.SliceStable(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
		v.Entries = entries
	case protocol.ValueList:
		values := make([]protocol.Value, len(v.Values))
		for i, x := range v.Values {
			values[i] = moSorted(x)
		}
		v.Values = values
	}
	return v
}

func moMapOrder(items []moItem) []moItem {
	out := make([]moItem, len(items))
	for i, it := range items {
		if m, ok := it.pack.(*protocol.MapPack); ok {
			c := &protocol.MapPack{Entries: make([]protocol.Entry, len(m.Entries))}
			for k, e := range m.Entries {
				c.Entries[k] = protocol.Entry{Key: e.Key, Value: moSorted(e.Value)}
			}
			out[i] = moItem{pack: c}
		} else if it.pack == nil {
			out[i] = moItem{value: moSorted(it.value)}
		} else {
			out[i] = it
		}
	}
	return out
}

type moOutcome int

const (
	moExact moOutcome = iota
	moMapValueOrder
	moIntended
)

type moExpect struct {
	outcome moOutcome
	reason  string
	check   func(t *testing.T, got, java moResponse)
}

func moExactly() moExpect   { return moExpect{outcome: moExact} }
func moOrderOnly() moExpect { return moExpect{outcome: moMapValueOrder} }
func moIntendedDiff(reason string, check func(t *testing.T, got, java moResponse)) moExpect {
	return moExpect{outcome: moIntended, reason: reason, check: check}
}

// moClient is one logged-in Client connection.
type moClient struct {
	owner   *testing.T
	addr    string
	session int64
	conn    net.Conn
	r       *bufio.Reader
}

func moDial(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	var magic [4]byte
	binary.BigEndian.PutUint32(magic[:], transport.TCPClient)
	if _, err := conn.Write(magic[:]); err != nil {
		t.Fatal(err)
	}
	return conn, bufio.NewReader(conn)
}

func moSend(t *testing.T, conn net.Conn, command string, session int64, body []byte) {
	t.Helper()
	e := protocol.NewEncoder(protocol.Limits{})
	e.Text(command)
	e.Int64(session)
	raw, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(append(raw, body...)); err != nil {
		t.Fatal(err)
	}
}

func moLogin(t *testing.T, addr string) *moClient {
	t.Helper()
	conn, r := moDial(t, addr)
	sum := sha256.Sum256([]byte("qwertyuiop!@#$%^&*()zxcvbnm,." + "password"))
	login, err := protocol.EncodePack(&protocol.MapPack{Entries: []protocol.Entry{
		{Key: "id", Value: protocol.Value{Type: protocol.ValueText, Text: "synthetic"}},
		{Key: "pass", Value: protocol.Value{Type: protocol.ValueText, Text: hex.EncodeToString(sum[:])}}}}, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	moSend(t, conn, "LOGIN", 0, login)
	resp := moRead(t, r, "pack")
	if len(resp.items) != 1 {
		t.Fatal("login failed")
	}
	session := moMapGet(resp.items[0].pack.(*protocol.MapPack), "session").Int
	if session == 0 {
		t.Fatal("login failed")
	}
	return &moClient{owner: t, addr: addr, session: session, conn: conn, r: r}
}

func moMapGet(m *protocol.MapPack, key string) protocol.Value {
	for _, e := range m.Entries {
		if e.Key == key {
			return e.Value
		}
	}
	return protocol.Value{}
}

// moStart opens and runs a Runtime; cfg.TCP/UDP/ReadyFile are filled in.
func moStart(t *testing.T, cfg Config) (*Runtime, string) {
	t.Helper()
	cfg.UDP, cfg.TCP = "127.0.0.1:0", "127.0.0.1:0"
	cfg.ReadyFile = filepath.Join(t.TempDir(), "ready.json")
	cfg.Username, cfg.Password = "synthetic", "password"
	if cfg.Location == nil {
		cfg.Location = time.UTC
	}
	r, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(cfg.ReadyFile)
	if err != nil {
		t.Fatal(err)
	}
	var endpoints Endpoints
	if err := json.Unmarshal(b, &endpoints); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("runtime shutdown timeout")
		}
	})
	return r, endpoints.TCP
}

// moReplay sends one golden request (substitute rewrites a request MapPack)
// and checks the answer against the expectation.
func moReplay(t *testing.T, g moGolden, c moCase, client *moClient, substitute func(*protocol.MapPack), want moExpect) moResponse {
	t.Helper()
	body := g.read(t, c.Request)
	if substitute != nil && len(body) > 0 {
		p, err := protocol.DecodePack(body, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if m, ok := p.(*protocol.MapPack); ok {
			substitute(m)
			if body, err = protocol.EncodePack(m, protocol.Limits{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	moSend(t, client.conn, c.Command, client.session, body)
	got := moRead(t, client.r, c.ItemKind)
	if !got.complete {
		client.conn, client.r = moDial(client.owner, client.addr)
	}
	java := moDecodeJava(t, g.read(t, c.Response), c.ItemKind)
	switch want.outcome {
	case moIntended:
		if want.reason == "" || want.check == nil {
			t.Fatal("intended difference without reason/check")
		}
		want.check(t, got, java)
	case moExact:
		if !bytes.Equal(got.raw, java.raw) {
			t.Fatalf("response differs from Java\n got  %s\n java %s", moShow(got), moShow(java))
		}
	case moMapValueOrder:
		if bytes.Equal(got.raw, java.raw) {
			t.Fatal("bytes now identical; change the expectation to moExactly()")
		}
		if !got.complete || !bytes.Equal(moEncode(t, moMapOrder(got.items)), moEncode(t, moMapOrder(java.items))) {
			t.Fatalf("response differs from Java beyond MapValue order\n got  %s\n java %s", moShow(got), moShow(java))
		}
	}
	return got
}

// moShow renders a response for failure messages (bounded).
func moShow(r moResponse) string {
	var b strings.Builder
	if !r.complete {
		b.WriteString("[closed] ")
	}
	for _, it := range r.items {
		var s string
		if it.pack != nil {
			j, _ := json.Marshal(it.pack)
			s = string(j)
		} else {
			j, _ := json.Marshal(it.value)
			s = string(j)
		}
		if len(s) > 600 {
			s = s[:600] + "..."
		}
		b.WriteString(s)
		b.WriteString(" ; ")
	}
	return b.String()
}

func moRequireAll(t *testing.T, g moGolden, expect map[string]moExpect) {
	t.Helper()
	seen := map[string]bool{}
	for _, c := range g.cases {
		if _, ok := expect[c.Name]; !ok {
			t.Errorf("golden case %s has no expectation", c.Name)
		}
		seen[c.Name] = true
	}
	for name := range expect {
		if !seen[name] {
			t.Errorf("expectation %s has no golden case", name)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
}

// moJavaLayout creates the oracle working directory layout.
func moJavaLayout(t *testing.T, serverConf string) (root string, store *config.Store) {
	t.Helper()
	root = t.TempDir()
	for _, d := range []string{"conf", "plugin", "database"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(root, "conf", "scouter.conf")
	if err := os.WriteFile(path, []byte(serverConf), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := config.Open(path, config.Options{Lookup: func(string) (string, bool) { return "", false }, HostName: "oracle-host", Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	return root, s
}

// moJavaDefaultCounters is the classpath counters.xml the Java oracle served
// (GET_XML_COUNTER "default" blob), so the Go engine starts from the same file.
func moJavaDefaultCounters(t *testing.T) []byte {
	t.Helper()
	g := moLoad(t, "counters")
	java := moDecodeJava(t, g.read(t, "get-xml-counter-initial.response.bin"), "pack")
	blob := moMapGet(java.items[0].pack.(*protocol.MapPack), "default")
	if blob.Type != protocol.ValueBlob || len(blob.Bytes) == 0 {
		t.Fatal("default counters.xml blob missing from the golden")
	}
	return blob.Bytes
}

// moRuntime opens a Runtime on the Java oracle layout.
func moRuntime(t *testing.T, serverConf string) (string, string, *config.Store) {
	t.Helper()
	root, store := moJavaLayout(t, serverConf)
	cfg := Config{
		Directory: filepath.Join(root, "database"), ConfigFile: store, ConfDir: filepath.Join(root, "conf"),
		PluginDirectory: filepath.Join(root, "plugin"), TelegrafConfigurationPath: filepath.Join(root, "conf", "scouter-telegraf.xml"),
		LogDirectory: filepath.Join(root, "logs"), CounterXML: moJavaDefaultCounters(t),
	}
	_, addr := moStart(t, cfg)
	return root, addr, store
}
