package service

// Java oracle replay for ManageService (SERVER_DB_DELETE, REMOTE_CONTROL,
// REMOTE_CONTROL_ALL, CHECK_JOB) and AgentInfo (OBJECT_REMOVE*,
// OBJECT_TODAY_FULL_LIST). testdata/management/{manage,agentinfo} was produced
// by scripts/java-management.py from the unchanged Scouter 2.21.3 handlers
// (tools/java-management/README.md). Each golden case is sent in order over a
// loopback Client connection with the same body; the response bytes are
// compared with the Java bytes (the trailing NoNEXT included).
//
// Outcomes per case:
//   - exact: identical bytes after the session-id substitution below.
//   - mapOrder: identical after sorting MapValue entries. Java writes the
//     Hashtable enumeration order of a MapValue; Go keeps insertion order
//     (internal/protocol/value.go). Pack fields and MapPack order still match.
//   - intended differences (oracleIntended) with a reason and their own check.
//
// Java logins are synthetic (101, 102 in manage; none in agentinfo). Go issues
// random sessions, so 101/102 are replaced by the two real sessions in request
// bodies and in the expected CHECK_JOB param echo; the other ids (999, 888, 777,
// 123456) stay unknown sessions.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

type oracleCase struct {
	Name     string `json:"name"`
	Command  string `json:"command"`
	Request  string `json:"request"`
	Response string `json:"response"`
	ItemKind string `json:"item_kind"`
}

// loadOracleCases fails (never skips) when the committed golden is missing.
func loadOracleCases(t *testing.T, scenario string) (string, []oracleCase) {
	t.Helper()
	dir := filepath.Join("testdata", "management", scenario)
	data, err := os.ReadFile(filepath.Join(dir, "cases.json"))
	if err != nil {
		t.Fatalf("Java oracle golden missing (regenerate with python3 scripts/java-management.py --generate): %v", err)
	}
	var root struct {
		Scenario string       `json:"scenario"`
		Cases    []oracleCase `json:"cases"`
	}
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	if root.Scenario != scenario || len(root.Cases) == 0 {
		t.Fatalf("unexpected golden %s: %q with %d cases", dir, root.Scenario, len(root.Cases))
	}
	return dir, root.Cases
}

func readGolden(t *testing.T, dir, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// oracleItem is one decoded response item (Pack or Value).
type oracleItem struct {
	pack  protocol.Pack
	value protocol.Value
}

// oracleResponse is what a Client would read: the raw bytes, the items and
// whether the NoNEXT terminator arrived (false: the server closed the
// connection after a rejected request).
type oracleResponse struct {
	raw      []byte
	items    []oracleItem
	complete bool
}

type captureReader struct {
	r   *bufio.Reader
	buf bytes.Buffer
}

func (c *captureReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.buf.Write(p[:n])
	return n, err
}

func readOracleResponse(t *testing.T, r *bufio.Reader, kind string) oracleResponse {
	t.Helper()
	c := &captureReader{r: r}
	var out oracleResponse
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
			out.items = append(out.items, oracleItem{pack: p})
		} else {
			v, err := protocol.ReadValue(c, protocol.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			out.items = append(out.items, oracleItem{value: v})
		}
	}
}

// decodeJavaResponse parses a golden *.response.bin (always NoNEXT-terminated).
func decodeJavaResponse(t *testing.T, raw []byte, kind string) oracleResponse {
	t.Helper()
	r := readOracleResponse(t, bufio.NewReader(bytes.NewReader(raw)), kind)
	if !r.complete || !bytes.Equal(r.raw, raw) {
		t.Fatalf("golden response does not round-trip through the Go reader")
	}
	return r
}

func encodeOracleResponse(t *testing.T, items []oracleItem, complete bool) []byte {
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
	if complete {
		e.Byte(transport.NoNext)
	}
	b, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// substituteSessions replaces Java session ids in the MapPack session keys.
func substituteSessions(p protocol.Pack, sessions map[int64]int64) protocol.Pack {
	m, ok := p.(*protocol.MapPack)
	if !ok {
		return p
	}
	out := &protocol.MapPack{Entries: append([]protocol.Entry(nil), m.Entries...)}
	for i, e := range out.Entries {
		switch e.Key {
		case "toSession", "fromSession", "session":
			if real, ok := sessions[e.Value.Int]; ok && e.Value.Type == protocol.ValueDecimal {
				out.Entries[i].Value = protocol.Value{Type: protocol.ValueDecimal, Int: real}
			}
		}
	}
	return out
}

func sortedValue(v protocol.Value) protocol.Value {
	switch v.Type {
	case protocol.ValueMap:
		entries := make([]protocol.Entry, len(v.Entries))
		for i, e := range v.Entries {
			entries[i] = protocol.Entry{Key: e.Key, Value: sortedValue(e.Value)}
		}
		sort.SliceStable(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
		v.Entries = entries
	case protocol.ValueList:
		values := make([]protocol.Value, len(v.Values))
		for i, x := range v.Values {
			values[i] = sortedValue(x)
		}
		v.Values = values
	}
	return v
}

// mapOrderNormalized sorts MapValue entries (ObjectPack tags, MapPack values);
// MapPack entry order itself is a LinkedHashMap contract and is kept.
func mapOrderNormalized(items []oracleItem) []oracleItem {
	out := make([]oracleItem, len(items))
	for i, it := range items {
		switch p := it.pack.(type) {
		case *protocol.ObjectPack:
			c := *p
			c.Tags = sortedValue(p.Tags)
			out[i] = oracleItem{pack: &c}
		case *protocol.MapPack:
			c := &protocol.MapPack{Entries: make([]protocol.Entry, len(p.Entries))}
			for k, e := range p.Entries {
				c.Entries[k] = protocol.Entry{Key: e.Key, Value: sortedValue(e.Value)}
			}
			out[i] = oracleItem{pack: c}
		case nil:
			out[i] = oracleItem{value: sortedValue(it.value)}
		default:
			out[i] = it
		}
	}
	return out
}

type oracleOutcome int

const (
	oracleExact oracleOutcome = iota
	oracleMapOrder
	oracleIntended
)

// oracleExpectation names the expected relation to the Java bytes. For
// oracleIntended, reason documents the decision and check verifies the Go
// behaviour instead of the Java bytes.
type oracleExpectation struct {
	outcome oracleOutcome
	reason  string
	check   func(t *testing.T, got oracleResponse)
}

func exactly() oracleExpectation      { return oracleExpectation{outcome: oracleExact} }
func javaMapOrder() oracleExpectation { return oracleExpectation{outcome: oracleMapOrder} }
func intended(reason string, check func(t *testing.T, got oracleResponse)) oracleExpectation {
	return oracleExpectation{outcome: oracleIntended, reason: reason, check: check}
}

// rejected: the request is refused, no item is written and the connection
// closes without NoNEXT (transport error policy).
func rejected(t *testing.T, got oracleResponse) {
	t.Helper()
	if got.complete || len(got.items) != 0 || len(got.raw) != 0 {
		t.Fatalf("want rejected request (no bytes, closed), got complete=%v items=%d raw=%x", got.complete, len(got.items), got.raw)
	}
}

func noItems(t *testing.T, got oracleResponse) {
	t.Helper()
	if !got.complete || len(got.items) != 0 {
		t.Fatalf("want NoNEXT only, got complete=%v items=%d", got.complete, len(got.items))
	}
}

func successFalse(t *testing.T, got oracleResponse) {
	t.Helper()
	if !got.complete || len(got.items) != 1 {
		t.Fatalf("want one MapPack, got complete=%v items=%d", got.complete, len(got.items))
	}
	m, ok := got.items[0].pack.(*protocol.MapPack)
	if !ok || len(m.Entries) != 1 || m.Entries[0].Key != "success" || m.Entries[0].Value.Type != protocol.ValueBoolean || m.Entries[0].Value.Bool {
		t.Fatalf("want {success:false}, got %#v", got.items[0].pack)
	}
}

// oracleClient is one logged-in Client connection that reconnects (keeping the
// server-side session) after the server closes it on a rejected request.
type oracleClient struct {
	owner   *testing.T // the replay test: reconnected sockets must outlive one subtest
	session int64
	conn    net.Conn
	r       *bufio.Reader
}

func (c *oracleClient) reconnect() {
	conn, r := secondClient(c.owner, c.conn)
	c.conn, c.r = conn, r
}

func sendOracleRaw(t *testing.T, conn net.Conn, command string, session int64, body []byte) {
	t.Helper()
	e := protocol.NewEncoder(protocol.Limits{})
	e.Text(command)
	e.Int64(session)
	raw, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(append(raw, body...)); err != nil {
		t.Fatal(err)
	}
}

// replayOracleCase sends one golden request and compares the answer.
func replayOracleCase(t *testing.T, dir string, c oracleCase, client *oracleClient, sessions map[int64]int64, want oracleExpectation) {
	t.Helper()
	body := readGolden(t, dir, c.Request)
	if len(body) > 0 && c.Command != "OBJECT_REMOVE" && c.Command != "OBJECT_REMOVE_IN_MEMORY" {
		p, err := protocol.DecodePack(body, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if body, err = protocol.EncodePack(substituteSessions(p, sessions), protocol.Limits{}); err != nil {
			t.Fatal(err)
		}
	}
	sendOracleRaw(t, client.conn, c.Command, client.session, body)
	got := readOracleResponse(t, client.r, c.ItemKind)
	if !got.complete {
		client.reconnect()
	}
	if want.outcome == oracleIntended {
		if want.reason == "" || want.check == nil {
			t.Fatal("intended difference without reason/check")
		}
		want.check(t, got)
		return
	}
	java := decodeJavaResponse(t, readGolden(t, dir, c.Response), c.ItemKind)
	for i, it := range java.items {
		if it.pack != nil {
			java.items[i].pack = substituteSessions(it.pack, sessions)
		}
	}
	expected := encodeOracleResponse(t, java.items, true)
	if bytes.Equal(got.raw, expected) {
		if want.outcome == oracleMapOrder {
			t.Fatalf("bytes now identical; change the expectation to exactly()")
		}
		return
	}
	if want.outcome == oracleMapOrder && got.complete &&
		bytes.Equal(encodeOracleResponse(t, mapOrderNormalized(got.items), true), encodeOracleResponse(t, mapOrderNormalized(java.items), true)) {
		return
	}
	t.Fatalf("response differs from Java\n got  %x\n java %x", got.raw, expected)
}

func requireAllExpected(t *testing.T, cases []oracleCase, expect map[string]oracleExpectation) {
	t.Helper()
	seen := map[string]bool{}
	for _, c := range cases {
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

func TestManageJavaOracle(t *testing.T) {
	dir, cases := loadOracleCases(t, "manage")
	traversal := "management-commands.md §9-4 decision: only SERVER_DB_LIST style \"/<relative>\" names inside the root are accepted; Java deletes outside db_dir"
	nonText := "Go rejects a non-Text file item; Java stringifies it (\"20260103\" without separator, a sibling of the root) and deletes nothing"
	unknownSession := "§9-5/§9-7 decision: an unknown from/to session queues nothing and answers success=false; Java queues, then NPEs on the log line and answers nothing"
	ownership := "§9-13 decision: CHECK_JOB dequeues only the requesting connection's own session; Java dequeues any session"
	followUp := "consequence of the rejected REMOTE_CONTROL/REMOTE_CONTROL_ALL above: Go queued nothing, Java queued the command"
	expect := map[string]oracleExpectation{
		"db-delete-day":                   exactly(),
		"db-delete-missing-and-file":      exactly(),
		"db-delete-no-list":               exactly(),
		"db-delete-empty-list":            exactly(),
		"db-delete-non-text-item":         intended(nonText, rejected),
		"db-delete-traversal":             intended(traversal, rejected),
		"remote-control":                  exactly(),
		"check-job-102":                   exactly(),
		"check-job-102-empty":             exactly(),
		"remote-control-invalid-to":       intended(unknownSession, successFalse),
		"check-job-invalid-to":            intended(ownership, noItems),
		"remote-control-invalid-from":     intended(unknownSession, successFalse),
		"check-job-after-invalid-from":    intended(followUp, noItems),
		"remote-control-all":              exactly(),
		"check-job-all-101":               exactly(),
		"check-job-all-102":               exactly(),
		"remote-control-all-invalid-from": exactly(), // both answer no items; Java also queued (see next case)
		"check-job-all-invalid-101":       intended(followUp, noItems),
		"check-job-unknown":               exactly(),
		"check-job-no-session":            exactly(),
	}
	for i := 1; i <= 7; i++ {
		expect["remote-control-burst-"+strconv.Itoa(i)] = exactly()
		expect["check-job-burst-"+strconv.Itoa(i)] = exactly()
	}
	requireAllExpected(t, cases, expect)

	captureManageLog(t)
	backend := newManageFake()
	_, connA, rA := startService(t, queryConfig(), backend)
	a := &oracleClient{owner: t, session: loginSession(t, connA, rA), conn: connA, r: rA}
	connB, rB := secondClient(t, connA)
	b := &oracleClient{owner: t, session: loginSession(t, connB, rB), conn: connB, r: rB}
	sessions := map[int64]int64{101: a.session, 102: b.session}
	for _, unknown := range []int64{999, 888, 777, 123456} {
		if unknown == a.session || unknown == b.session {
			t.Fatal("random session collides with a golden unknown id")
		}
	}
	for _, c := range cases {
		client := a
		if c.Command == "CHECK_JOB" {
			// CHECK_JOB is sent by the session it asks for; foreign ids go via A.
			body := readGolden(t, dir, c.Request)
			p, err := protocol.DecodePack(body, protocol.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			if mapValue(p.(*protocol.MapPack), "session").Int == 102 {
				client = b
			}
		}
		t.Run(c.Name, func(t *testing.T) { replayOracleCase(t, dir, c, client, sessions, expect[c.Name]) })
	}
	// Only the accepted requests reach the deleter, cleaned and root-relative.
	if got, want := backend.deletions(), [][]string{{"20260101"}, {"no-such-day", "20260102/xlog/xlog.data"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("deleter calls %q, want %q", got, want)
	}
}

// objectOracleFake reproduces the oracle seeds (ManagementOracle.seedObjects):
// today's object DB holds 1001, 1002, 1003; the live map holds 1001 (alive)
// and 1002 (alive=false).
type objectOracleFake struct {
	*manageFake
	omu    sync.Mutex
	stored []*protocol.ObjectPack
	live   map[int32]*protocol.ObjectPack
	calls  []string
}

func oracleObject(hash int32, typ, name string, alive bool) *protocol.ObjectPack {
	return &protocol.ObjectPack{ObjHash: hash, ObjType: typ, ObjName: name, Address: "10.0.0." + strconv.Itoa(int(hash%250)), Version: "synthetic",
		Alive: alive, Wakeup: 1767193200000, Tags: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "region", Value: protocol.Value{Type: protocol.ValueText, Text: "synthetic"}}}}}
}

func (f *objectOracleFake) seed() {
	f.omu.Lock()
	defer f.omu.Unlock()
	f.stored = []*protocol.ObjectPack{oracleObject(1001, "tomcat", "/synthetic/a", true), oracleObject(1002, "tomcat", "/synthetic/b", false), oracleObject(1003, "host", "/synthetic/c", true)}
	f.live = map[int32]*protocol.ObjectPack{1001: oracleObject(1001, "tomcat", "/synthetic/a", true), 1002: oracleObject(1002, "tomcat", "/synthetic/b", false)}
}

func (f *objectOracleFake) Objects(context.Context, string) ([]*protocol.ObjectPack, error) {
	f.omu.Lock()
	defer f.omu.Unlock()
	return append([]*protocol.ObjectPack(nil), f.stored...), nil
}

func (f *objectOracleFake) Object(_ context.Context, _ string, hash int32) (*protocol.ObjectPack, error) {
	f.omu.Lock()
	defer f.omu.Unlock()
	return f.live[hash], nil
}

func (f *objectOracleFake) RemoveObjects(_ context.Context, hashes []int32, permanent bool) error {
	f.omu.Lock()
	defer f.omu.Unlock()
	for _, h := range hashes {
		delete(f.live, h)
	}
	if permanent {
		f.calls = append(f.calls, "permanent")
	} else {
		f.calls = append(f.calls, "memory")
	}
	return nil
}

func (f *objectOracleFake) RemoveInactiveObjects(context.Context) error {
	f.omu.Lock()
	defer f.omu.Unlock()
	for h, o := range f.live {
		if !o.Alive {
			delete(f.live, h)
		}
	}
	f.calls = append(f.calls, "inactive")
	return nil
}

func TestAgentInfoRemoveJavaOracle(t *testing.T) {
	dir, cases := loadOracleCases(t, "agentinfo")
	strict := "Go rejects ListValue items that are not Decimal within int32; Java maps Text to 0, truncates Float and wraps longs (getLong(i).toInt)"
	notList := "Go rejects a non-ListValue body; Java ClassCastException is swallowed and answers no items"
	expect := map[string]oracleExpectation{
		"today-full-list-initial":               javaMapOrder(),
		"remove-inactive":                       exactly(),
		"today-full-list-after-remove-inactive": javaMapOrder(),
		"remove-in-memory":                      javaMapOrder(),
		"remove-permanent-mixed":                intended(strict, rejected),
		"remove-empty-list":                     javaMapOrder(),
		"remove-mappack-body":                   intended(notList, rejected),
		"remove-text-body":                      intended(notList, rejected),
	}
	requireAllExpected(t, cases, expect)
	// ManagementOracle.agentinfo re-seeds before these cases.
	reseed := map[string]bool{"remove-in-memory": true, "remove-permanent-mixed": true, "remove-empty-list": true, "remove-mappack-body": true, "remove-text-body": true}

	captureManageLog(t)
	backend := &objectOracleFake{manageFake: newManageFake()}
	backend.seed()
	_, conn, r := startService(t, queryConfig(), backend)
	client := &oracleClient{owner: t, session: loginSession(t, conn, r), conn: conn, r: r}
	for _, c := range cases {
		if reseed[c.Name] {
			backend.seed()
		}
		t.Run(c.Name, func(t *testing.T) { replayOracleCase(t, dir, c, client, nil, expect[c.Name]) })
	}
	// OBJECT_REMOVE's disk effect is not compared: the Java golden only holds the
	// response stream (the original ObjectWR writer NPEs on remove Orders,
	// management-commands.md §6.3/§9-6). Here permanent=true reaches the remover.
	if want := []string{"inactive", "memory"}; !reflect.DeepEqual(backend.calls, want) {
		t.Fatalf("remover calls %q, want %q", backend.calls, want)
	}
}
