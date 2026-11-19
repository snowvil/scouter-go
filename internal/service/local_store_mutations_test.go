package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/transport"
)

type localStoreWriteCall struct {
	op, space, key, value string
	ttl, now              int64
}

// localStoreWriteBackend mimics KeyValueStoreRW's boolean contract: empty
// space/key/value answer false without an error.
type localStoreWriteBackend struct {
	*localStoreQueryBackend
	writes []localStoreWriteCall
	fail   error
}

func (b *localStoreWriteBackend) PutKeyValue(ctx context.Context, space, key, value string, ttl, now int64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if b.fail != nil {
		return false, b.fail
	}
	b.writes = append(b.writes, localStoreWriteCall{"put", space, key, value, ttl, now})
	if space == "" || key == "" || value == "" {
		return false, nil
	}
	b.kv[space+":"+key] = value
	return true, nil
}
func (b *localStoreWriteBackend) SetKeyValueTTL(ctx context.Context, space, key string, ttl, now int64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if b.fail != nil {
		return false, b.fail
	}
	b.writes = append(b.writes, localStoreWriteCall{"ttl", space, key, "", ttl, now})
	return space != "" && key != "", nil
}
func readKVBoolean(t *testing.T, r *bufio.Reader) bool {
	t.Helper()
	v := readKVValue(t, r)
	if v.Type != protocol.ValueBoolean {
		t.Fatal("Boolean response", v)
	}
	return v.Bool
}

func TestLocalStoreMutationBodiesAndTTLDefaults(t *testing.T) {
	b := &localStoreWriteBackend{localStoreQueryBackend: &localStoreQueryBackend{fakeBackend: &fakeBackend{}, kv: map[string]string{}}}
	_, c, r := startService(t, queryConfig(), b)
	session := loginSession(t, c, r)
	now := queryConfig().Clock().Unix()
	send(t, c, "SET_GLOBAL_KV", session, newMap(entry("key", stringValue("a")), entry("value", stringValue("alpha"))))
	send(t, c, "SET_GLOBAL_KV", session, newMap(entry("key", stringValue("b")), entry("value", stringValue("beta")), entry("ttl", decimal(3600))))
	send(t, c, "SET_GLOBAL_KV", session, newMap(entry("key", stringValue("f")), entry("value", stringValue("float")), entry("ttl", protocol.Value{Type: protocol.ValueFloat, Float: 90.9})))
	send(t, c, "SET_GLOBAL_KV", session, newMap(entry("key", stringValue("d")), entry("value", stringValue("double")), entry("ttl", protocol.Value{Type: protocol.ValueDouble, Double: -0.5})))
	send(t, c, "SET_GLOBAL_KV", session, newMap(entry("key", stringValue("t")), entry("value", stringValue("text")), entry("ttl", stringValue("5"))))
	send(t, c, "SET_GLOBAL_KV", session, newMap(entry("key", stringValue("c")), entry("value", stringValue(""))))
	send(t, c, "SET_GLOBAL_KV", session, newMap(entry("key", stringValue("c")), entry("value", decimal(9))))
	send(t, c, "SET_GLOBAL_KV", session, newMap(entry("key", stringValue(""))))
	send(t, c, "SET_CUSTOM_KV", session, newMap(entry("keySpace", stringValue("custom")), entry("key", stringValue("a")), entry("value", stringValue("custom-alpha")), entry("ttl", decimal(-1))))
	send(t, c, "SET_CUSTOM_KV", session, newMap(entry("key", stringValue("a")), entry("value", stringValue("orphan"))))
	send(t, c, "SET_GLOBAL_TTL", session, newMap(entry("key", stringValue("a")), entry("ttl", decimal(7200))))
	send(t, c, "SET_GLOBAL_TTL", session, newMap(entry("key", stringValue("b"))))
	send(t, c, "SET_CUSTOM_TTL", session, newMap(entry("keySpace", stringValue("custom")), entry("key", stringValue("a")), entry("ttl", decimal(30))))
	send(t, c, "SET_CUSTOM_TTL", session, newMap(entry("key", stringValue("a")), entry("ttl", decimal(30))))
	for i, want := range []bool{true, true, true, true, true, false, false, false, true, false, true, true, true, false} {
		if got := readKVBoolean(t, r); got != want {
			t.Fatal(i, got, want)
		}
	}
	// A null or non-Text key answers no item, so the pipeline stays aligned.
	send(t, c, "SET_GLOBAL_KV", session, newMap(entry("value", stringValue("orphan"))))
	send(t, c, "SET_GLOBAL_KV", session, newMap(entry("key", decimal(1)), entry("value", stringValue("orphan"))))
	send(t, c, "SET_GLOBAL_TTL", session, newMap(entry("ttl", decimal(5))))
	send(t, c, "SET_CUSTOM_KV", session, newMap(entry("keySpace", stringValue("custom")), entry("value", stringValue("orphan"))))
	send(t, c, "SET_CUSTOM_TTL", session, newMap(entry("keySpace", stringValue("custom"))))
	for i := 0; i < 5; i++ {
		if packs := readPacks(t, r); len(packs) != 0 {
			t.Fatal(i, packs)
		}
	}
	sendKVValue(t, c, session, stringValue("a"))
	if got := readKVValue(t, r); got.Text != "alpha" {
		t.Fatal(got)
	}
	want := []localStoreWriteCall{
		{"put", GlobalKeySpace, "a", "alpha", -1, now}, {"put", GlobalKeySpace, "b", "beta", 3600, now}, {"put", GlobalKeySpace, "f", "float", 90, now},
		{"put", GlobalKeySpace, "d", "double", 0, now}, {"put", GlobalKeySpace, "t", "text", -1, now}, {"put", GlobalKeySpace, "c", "", -1, now},
		{"put", GlobalKeySpace, "c", "", -1, now}, {"put", GlobalKeySpace, "", "", -1, now}, {"put", "custom", "a", "custom-alpha", -1, now},
		{"put", "", "a", "orphan", -1, now}, {"ttl", GlobalKeySpace, "a", "", 7200, now}, {"ttl", GlobalKeySpace, "b", "", -1, now},
		{"ttl", "custom", "a", "", 30, now}, {"ttl", "", "a", "", 30, now},
	}
	if len(b.writes) != len(want) {
		t.Fatal(b.writes)
	}
	for i := range want {
		if b.writes[i] != want[i] {
			t.Fatal(i, b.writes[i], want[i])
		}
	}
}

func TestLocalStoreMutationBulkHashtableOrderAndBodies(t *testing.T) {
	b := &localStoreWriteBackend{localStoreQueryBackend: &localStoreQueryBackend{fakeBackend: &fakeBackend{}, kv: map[string]string{}}}
	_, c, r := startService(t, queryConfig(), b)
	session := loginSession(t, c, r)
	kv := []protocol.Entry{entry("k0", stringValue("v0")), entry("k1", stringValue("v1")), entry("k2", stringValue("v2")), entry("k3", stringValue("v3")), entry("k4", stringValue("v4")), entry("k5", stringValue("v5")), entry("k6", stringValue("v6")), entry("", stringValue("empty-key")), entry("e", stringValue("")), entry("n", decimal(5))}
	send(t, c, "SET_GLOBAL_KV_BULK", session, newMap(entry("ttl", decimal(60)), entry("kv", protocol.Value{Type: protocol.ValueMap, Entries: kv})))
	packs := readPacks(t, r)
	if len(packs) != 1 {
		t.Fatal(packs)
	}
	// Java Hashtable(11) enumeration for this wire insertion order: rehash to
	// 23 buckets before the 9th key, then e shares bucket 9 with k2 and the
	// enumerator walks buckets 22..0 with the newest chain head first.
	wantKeys := []string{"n", "k6", "k5", "k4", "k3", "e", "k2", "k1", "k0", ""}
	wantResults := []bool{false, true, true, true, true, false, true, true, true, false}
	result := packs[0].(*protocol.MapPack)
	if len(result.Entries) != len(wantKeys) {
		t.Fatal(result.Entries)
	}
	for i, e := range result.Entries {
		if e.Key != wantKeys[i] || e.Value.Type != protocol.ValueBoolean || e.Value.Bool != wantResults[i] {
			t.Fatal(i, e, wantKeys[i], wantResults[i])
		}
	}
	if len(b.writes) != 10 || b.writes[0] != (localStoreWriteCall{"put", GlobalKeySpace, "n", "", 60, queryConfig().Clock().Unix()}) || b.writes[8].value != "v0" {
		t.Fatal(b.writes)
	}
	send(t, c, "SET_CUSTOM_KV_BULK", session, newMap(entry("keySpace", stringValue("custom")), entry("ttl", decimal(-3)), entry("kv", protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("x", stringValue("cx")), entry("y", stringValue("cy"))}})))
	send(t, c, "SET_CUSTOM_KV_BULK", session, newMap(entry("kv", protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("x", stringValue("1"))}})))
	// Custom bulk answers an empty MapPack for an empty kv Map; global answers nothing.
	send(t, c, "SET_CUSTOM_KV_BULK", session, newMap(entry("keySpace", stringValue("custom")), entry("kv", protocol.Value{Type: protocol.ValueMap})))
	send(t, c, "SET_GLOBAL_KV_BULK", session, newMap(entry("ttl", decimal(1)), entry("kv", protocol.Value{Type: protocol.ValueMap})))
	send(t, c, "SET_GLOBAL_KV_BULK", session, newMap(entry("ttl", decimal(1))))
	send(t, c, "SET_CUSTOM_KV_BULK", session, newMap())
	for i, want := range [][]string{{"x", "y"}, {"x"}, {}, nil, nil, nil} {
		packs := readPacks(t, r)
		if want == nil {
			if len(packs) != 0 {
				t.Fatal(i, packs)
			}
			continue
		}
		if len(packs) != 1 || len(packs[0].(*protocol.MapPack).Entries) != len(want) {
			t.Fatal(i, packs)
		}
		for n, e := range packs[0].(*protocol.MapPack).Entries {
			if e.Key != want[n] || e.Value.Bool != (i == 0) {
				t.Fatal(i, e)
			}
		}
	}
	if b.kv["custom:x"] != "cx" || b.writes[len(b.writes)-1] != (localStoreWriteCall{"put", "", "x", "1", -1, queryConfig().Clock().Unix()}) {
		t.Fatal(b.kv, b.writes)
	}
}

// dialClient opens another Client connection to the running test server so a
// failing command (connection closed without NoNext) can be observed alone.
func dialClient(t *testing.T, addr net.Addr) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	var magic [4]byte
	binary.BigEndian.PutUint32(magic[:], transport.TCPClient)
	if _, err := conn.Write(magic[:]); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, bufio.NewReader(conn)
}
func sendRaw(t *testing.T, conn net.Conn, command string, session int64, body []byte) {
	t.Helper()
	e := protocol.NewEncoder(protocol.Limits{})
	e.Text(command)
	e.Int64(session)
	prefix, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(append(prefix, body...)); err != nil {
		t.Fatal(command, err)
	}
}

func TestLocalStoreMutationFailuresAreExplicit(t *testing.T) {
	b := &localStoreWriteBackend{localStoreQueryBackend: &localStoreQueryBackend{fakeBackend: &fakeBackend{}, kv: map[string]string{}}}
	_, c, r := startService(t, queryConfig(), b)
	session := loginSession(t, c, r)
	// Original: ClassCastException (global/custom non-Map kv) or
	// NullPointerException (custom absent kv) swallowed by the proxy. Go fails
	// the command without a success terminator and writes nothing.
	for _, test := range []struct {
		command string
		pack    *protocol.MapPack
	}{
		{"SET_GLOBAL_KV_BULK", newMap(entry("kv", stringValue("x")))},
		{"SET_CUSTOM_KV_BULK", newMap(entry("keySpace", stringValue("custom")))},
		{"SET_CUSTOM_KV_BULK", newMap(entry("keySpace", stringValue("custom")), entry("kv", list(nil)))},
	} {
		conn, reader := dialClient(t, c.RemoteAddr())
		send(t, conn, test.command, session, test.pack)
		if flag, err := reader.ReadByte(); err == nil {
			t.Fatal(test.command, "unexpected response", flag)
		}
	}
	if len(b.writes) != 0 {
		t.Fatal("writes before failure", b.writes)
	}
	// Storage failures propagate instead of answering false.
	b.fail = errors.New("synthetic disk failure")
	conn, reader := dialClient(t, c.RemoteAddr())
	send(t, conn, "SET_GLOBAL_KV", session, newMap(entry("key", stringValue("a")), entry("value", stringValue("alpha"))))
	if flag, err := reader.ReadByte(); err == nil {
		t.Fatal("failure answered", flag)
	}
	if len(b.writes) != 0 {
		t.Fatal("writes before failure", b.writes)
	}
	// A backend without the writer fails the command without reading its body.
	reader2, err := New(queryConfig(), &localStoreQueryBackend{fakeBackend: &fakeBackend{}, kv: map[string]string{}})
	if err != nil {
		t.Fatal(err)
	}
	for _, cmd := range LocalStoreMutationCommands() {
		handled, err := reader2.HandleLocalStoreCommand(context.Background(), transport.ClientRequest{Command: cmd, Authenticated: true}, nil)
		if !handled || !errors.Is(err, transport.ErrUnsupported) {
			t.Fatal(cmd, handled, err)
		}
	}
}

func TestLocalStoreJavaLongConversion(t *testing.T) {
	for _, test := range []struct {
		in   float64
		want int64
	}{{90.9, 90}, {-0.5, 0}, {-7.9, -7}, {math.NaN(), 0}, {math.Inf(1), math.MaxInt64}, {math.Inf(-1), math.MinInt64}, {1e30, math.MaxInt64}, {-1e30, math.MinInt64}} {
		if got := javaLong(test.in); got != test.want {
			t.Fatal(test.in, got, test.want)
		}
	}
	p := newMap(entry("ttl", boolean(true)))
	if got := javaLongDefault(p, "ttl", -1); got != -1 {
		t.Fatal(got)
	}
}

func (b *localStoreFixtureBackend) PutKeyValue(ctx context.Context, space, key, value string, ttl, now int64) (bool, error) {
	return b.store.PutKeyValueContext(ctx, space, key, value, ttl, now)
}
func (b *localStoreFixtureBackend) SetKeyValueTTL(ctx context.Context, space, key string, ttl, now int64) (bool, error) {
	return b.store.SetKeyValueTTLContext(ctx, space, key, ttl, now)
}

type kvRecord struct {
	position, expire, previous int64
	deleted                    bool
	key, value                 []byte
}

// walkKeyFile2 parses RealKeyFile2 records (deleted:boolean, expire:long5,
// previous:long5, key:ShortBytes, value:Blob) from the CA FE header onward.
func walkKeyFile2(t *testing.T, raw []byte) []kvRecord {
	t.Helper()
	if len(raw) < 2 || raw[0] != 0xCA || raw[1] != 0xFE {
		t.Fatal("k2file magic")
	}
	long5 := func(p []byte) int64 {
		n := int64(0)
		for _, b := range p[:5] {
			n = n<<8 | int64(b)
		}
		if n >= 1<<39 {
			n -= 1 << 40
		}
		return n
	}
	var records []kvRecord
	pos := 2
	for pos < len(raw) {
		if len(raw)-pos < 13 {
			t.Fatal("truncated record", pos)
		}
		r := kvRecord{position: int64(pos), deleted: raw[pos] != 0, expire: long5(raw[pos+1:]), previous: long5(raw[pos+6:])}
		keyLen := int(int16(binary.BigEndian.Uint16(raw[pos+11:])))
		pos += 13
		if keyLen < 0 || len(raw)-pos < keyLen+1 {
			t.Fatal("key length", keyLen, pos)
		}
		r.key = raw[pos : pos+keyLen]
		pos += keyLen
		size := int(raw[pos])
		pos++
		switch {
		case size == 0xFF:
			size = int(binary.BigEndian.Uint16(raw[pos:]))
			pos += 2
		case size == 0xFE:
			size = int(int32(binary.BigEndian.Uint32(raw[pos:])))
			pos += 4
		}
		if size < 0 || len(raw)-pos < size {
			t.Fatal("blob length", size, pos)
		}
		r.value = raw[pos : pos+size]
		pos += size
		records = append(records, r)
	}
	return records
}

// Actual pinned KeyValueStoreService SET/TTL handlers and RW/index/writer:
// responses are compared byte-for-byte, then the Go-written kv2 files are
// compared with the original files. Only relative expiry seconds differ by
// clock, so each expire is checked against the recorded oracle window.
func TestLocalStoreActualJavaWriteHandlersAndDB(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	original := os.Getenv("SCOUTER_SOURCE")
	if original == "" {
		original = filepath.Join(repo, "..", "scouter")
	}
	if _, err := os.Stat(filepath.Join(original, "scouter.server")); err != nil {
		t.Skip("pinned original source unavailable; local-store write oracle not executed")
	}
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", filepath.Join(repo, "scripts", "java-storage.py"), "--generate-local-store-writes", root, "--source", original)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("actual Java local-store write oracle: %v\n%s", err, output)
	}
	t.Log(strings.TrimSpace(string(output)))
	var observed struct {
		Spaces   map[string]string `json:"-"`
		Before   int64             `json:"before_seconds"`
		After    int64             `json:"after_seconds"`
		NoExpiry int64             `json:"no_expiry"`
		Cases    int               `json:"cases"`
		Throwing int               `json:"throwing_cases"`
	}
	raw, err := os.ReadFile(filepath.Join(root, "kv-write-observations.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &observed); err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	observed.Spaces = map[string]string{}
	for _, name := range []string{"global", "custom", "ttl_space"} {
		observed.Spaces[name], _ = fields[name].(string)
	}
	if observed.NoExpiry != 549755813632 || observed.Before <= 0 || observed.After < observed.Before || observed.Throwing != 3 {
		t.Fatal("oracle observations", string(raw))
	}
	cases, err := protocol.DecodeValue(func() []byte {
		data, err := os.ReadFile(filepath.Join(root, "kv-write-cases.bin"))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}(), protocol.Limits{})
	if err != nil || cases.Type != protocol.ValueList || len(cases.Values) != observed.Cases {
		t.Fatal("write cases", err, len(cases.Values))
	}
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	goRoot := t.TempDir()
	store, err := storage.Open(storage.Config{Directory: goRoot, Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cfg := queryConfig()
	cfg.Location = loc
	_, conn, reader := startService(t, cfg, &localStoreFixtureBackend{fakeBackend: &fakeBackend{}, store: store})
	session := loginSession(t, conn, reader)
	goNow := cfg.Clock().Unix()
	type writeCase struct {
		name, command, errorName string
		request, response        []byte
		throws                   bool
	}
	var throwing []writeCase
	replayed := 0
	for _, v := range cases.Values {
		if v.Type != protocol.ValueMap {
			t.Fatal("case schema", v.Type)
		}
		m := &protocol.MapPack{Entries: v.Entries}
		c := writeCase{name: lookup(m, "name").Text, command: lookup(m, "command").Text, errorName: lookup(m, "error").Text, request: lookup(m, "request").Bytes, response: lookup(m, "response").Bytes, throws: lookup(m, "throws").Bool}
		if c.name == "" || c.command == "" || lookup(m, "request").Type != protocol.ValueBlob || lookup(m, "response").Type != protocol.ValueBlob {
			t.Fatal("case fields", c.name)
		}
		if c.throws {
			throwing = append(throwing, c)
			continue
		}
		sendRaw(t, conn, c.command, session, c.request)
		got := make([]byte, len(c.response))
		if _, err := io.ReadFull(reader, got); err != nil {
			t.Fatal(c.name, err)
		}
		if !bytes.Equal(got, c.response) {
			t.Fatalf("actual original %s (%s) wire mismatch got=%x expected=%x", c.name, c.command, got, c.response)
		}
		if flag, err := reader.ReadByte(); err != nil || flag != transport.NoNext {
			t.Fatal(c.name, "pipeline end", flag, err)
		}
		replayed++
	}
	if replayed != observed.Cases-3 || len(throwing) != 3 {
		t.Fatal("replayed cases", replayed, len(throwing))
	}
	// Original handler exceptions: ServiceHandlingProxy swallows them and the
	// Client sees no body. Go fails the command explicitly without a success
	// terminator, which the original connection never emits for these either.
	for _, c := range throwing {
		if (c.errorName != "java.lang.NullPointerException" && c.errorName != "java.lang.ClassCastException") || len(c.response) != 0 {
			t.Fatal("original exception case", c.name, c.errorName, c.response)
		}
		failing, failingReader := dialClient(t, conn.RemoteAddr())
		sendRaw(t, failing, c.command, session, c.request)
		if flag, err := failingReader.ReadByte(); err == nil {
			t.Fatal(c.name, "unexpected response", flag)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	spaces := []string{observed.Spaces["global"], observed.Spaces["custom"], observed.Spaces["ttl_space"]}
	if spaces[0] != GlobalKeySpace || spaces[1] == "" || spaces[2] == "" {
		t.Fatal(observed.Spaces)
	}
	compared := 0
	for _, space := range spaces {
		files := map[string][2][]byte{}
		for _, suffix := range []string{".hfile", ".k2file", ".kvdata"} {
			relative := filepath.Join("00000000", "kv2", "kv_"+space+suffix)
			want, err := os.ReadFile(filepath.Join(root, relative))
			if err != nil {
				t.Fatal(relative, err)
			}
			got, err := os.ReadFile(filepath.Join(goRoot, relative))
			if err != nil {
				t.Fatal(relative, err)
			}
			files[suffix] = [2][]byte{got, want}
		}
		for _, suffix := range []string{".hfile", ".kvdata"} {
			if !bytes.Equal(files[suffix][0], files[suffix][1]) {
				t.Fatalf("Go/Java KV file bytes differ: %s%s", space, suffix)
			}
		}
		got, want := walkKeyFile2(t, files[".k2file"][0]), walkKeyFile2(t, files[".k2file"][1])
		if len(got) != len(want) || len(files[".k2file"][0]) != len(files[".k2file"][1]) {
			t.Fatalf("%s record count got=%d want=%d", space, len(got), len(want))
		}
		for i := range want {
			g, w := got[i], want[i]
			if g.position != w.position || g.previous != w.previous || g.deleted != w.deleted || !bytes.Equal(g.key, w.key) || !bytes.Equal(g.value, w.value) {
				t.Fatalf("%s record %d differs: go=%+v java=%+v", space, i, g, w)
			}
			if w.expire == observed.NoExpiry {
				if g.expire != observed.NoExpiry {
					t.Fatalf("%s record %d %q expire: go=%d java=sentinel", space, i, g.key, g.expire)
				}
				compared++
				continue
			}
			ttl := g.expire - goNow
			if ttl < 0 || w.expire-ttl < observed.Before || w.expire-ttl > observed.After {
				t.Fatalf("%s record %d %q expire: go=%d (now %d) java=%d window [%d,%d]", space, i, g.key, g.expire, goNow, w.expire, observed.Before, observed.After)
			}
			compared++
		}
	}
	if compared < 20 {
		t.Fatal("records compared", compared)
	}
	// Reopen the Go-written DB and re-read through the fixed clock: the TTL
	// window chosen by the oracle keeps these keys live and the empty/non-Text
	// values absent, as the original READ re-queries showed.
	reopened, err := storage.Open(storage.Config{Directory: goRoot, Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for key, want := range map[string]string{"a": "alpha-2", "b": "beta", "f": "float-ttl", "t": "text-ttl", "한글-key": "한글 값", "k6": "v6", "p": "perm"} {
		got, found, err := reopened.KeyValueContext(context.Background(), GlobalKeySpace, key, goNow)
		if err != nil || !found || got != want {
			t.Fatal(key, got, found, err)
		}
	}
	for _, key := range []string{"c", "e", "n", "", "missing"} {
		if _, found, err := reopened.KeyValueContext(context.Background(), GlobalKeySpace, key, goNow); err != nil || found {
			t.Fatal(key, found, err)
		}
	}
	if _, found, err := reopened.KeyValueContext(context.Background(), GlobalKeySpace, "k0", goNow+61); err != nil || found {
		t.Fatal("bulk ttl 60 expiry", found, err)
	}
	if _, found, err := reopened.KeyValueContext(context.Background(), GlobalKeySpace, "a", goNow+7201); err != nil || found {
		t.Fatal("SET_GLOBAL_TTL 7200 expiry", found, err)
	}
	t.Log("compared records:", strconv.Itoa(compared), fmt.Sprintf("oracle window [%d,%d]", observed.Before, observed.After))
}
