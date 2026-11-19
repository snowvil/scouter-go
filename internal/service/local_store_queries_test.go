package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
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

type localStoreQueryBackend struct {
	*fakeBackend
	kv       map[string]string
	statuses []*protocol.StatusPack
	calls    []string
	dates    []string
	times    [][2]int64
}

func (b *localStoreQueryBackend) KeyValue(ctx context.Context, space, key string, now int64) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	b.calls = append(b.calls, space+":"+key)
	b.times = append(b.times, [2]int64{now, now})
	v, ok := b.kv[space+":"+key]
	return v, ok, nil
}
func (b *localStoreQueryBackend) QueryList(ctx context.Context, date string, from, to int64, hash int32) ([]*protocol.StatusPack, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.dates = append(b.dates, date)
	b.times = append(b.times, [2]int64{from, to})
	return b.statuses, nil
}
func sendKVValue(t *testing.T, c net.Conn, session int64, v protocol.Value) {
	t.Helper()
	e := protocol.NewEncoder(protocol.Limits{})
	e.Text("GET_GLOBAL_KV")
	e.Int64(session)
	e.Value(v)
	raw, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Write(raw); err != nil {
		t.Fatal(err)
	}
}
func readKVValue(t *testing.T, r *bufio.Reader) protocol.Value {
	t.Helper()
	flag, err := r.ReadByte()
	if err != nil || flag != transport.HasNext {
		t.Fatal(flag, err)
	}
	v, err := protocol.ReadValue(r, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	flag, err = r.ReadByte()
	if err != nil || flag != transport.NoNext {
		t.Fatal(flag, err)
	}
	return v
}
func TestLocalStoreKVValueBodiesAndPipeline(t *testing.T) {
	b := &localStoreQueryBackend{fakeBackend: &fakeBackend{}, kv: map[string]string{GlobalKeySpace + ":a": "first", GlobalKeySpace + ":42": "number", "custom:b": "second"}}
	_, c, r := startService(t, queryConfig(), b)
	session := loginSession(t, c, r)
	sendKVValue(t, c, session, stringValue("a"))
	send(t, c, "GET_CUSTOM_KV", session, newMap(entry("keySpace", stringValue("custom")), entry("key", stringValue("b"))))
	sendKVValue(t, c, session, stringValue("missing"))
	sendKVValue(t, c, session, decimal(42))
	sendKVValue(t, c, session, protocol.Value{Type: protocol.ValueNull})
	for _, want := range []string{"first", "second", "", "number", ""} {
		got := readKVValue(t, r)
		if got.Type != protocol.ValueText || got.Text != want {
			t.Fatal(got, want)
		}
	}
	if len(b.calls) != 5 || b.times[0][0] != queryConfig().Clock().Unix() {
		t.Fatal(b.calls, b.times)
	}
	// Missing custom key returns no item, so the next request remains aligned.
	send(t, c, "GET_CUSTOM_KV", session, newMap(entry("keySpace", stringValue("custom"))))
	sendKVValue(t, c, session, stringValue("a"))
	if got := readPacks(t, r); len(got) != 0 {
		t.Fatal(got)
	}
	if got := readKVValue(t, r); got.Text != "first" {
		t.Fatal(got)
	}
}
func TestLocalStoreKVBulkOrderDuplicatesAndMissing(t *testing.T) {
	b := &localStoreQueryBackend{fakeBackend: &fakeBackend{}, kv: map[string]string{GlobalKeySpace + ":a": "A", GlobalKeySpace + ":b": "B", "custom:a": "custom"}}
	_, c, r := startService(t, queryConfig(), b)
	session := loginSession(t, c, r)
	send(t, c, "GET_GLOBAL_KV_BULK", session, newMap(entry("key", list([]protocol.Value{stringValue("b"), stringValue("missing"), stringValue("a"), stringValue("b")}))))
	packs := readPacks(t, r)
	if len(packs) != 1 {
		t.Fatal(packs)
	}
	entries := packs[0].(*protocol.MapPack).Entries
	if len(entries) != 2 || entries[0].Key != "b" || entries[1].Key != "a" {
		t.Fatal(entries)
	}
	send(t, c, "GET_CUSTOM_KV_BULK", session, newMap(entry("keySpace", stringValue("custom")), entry("key", list([]protocol.Value{stringValue("a")}))))
	packs = readPacks(t, r)
	if len(packs) != 1 || lookup(packs[0].(*protocol.MapPack), "a").Text != "custom" {
		t.Fatal(packs)
	}
	send(t, c, "GET_GLOBAL_KV_BULK", session, newMap(entry("key", list(nil))))
	if packs = readPacks(t, r); len(packs) != 0 {
		t.Fatal(packs)
	}
	send(t, c, "GET_GLOBAL_KV_BULK", session, newMap(entry("key", list([]protocol.Value{stringValue("absent")}))))
	if packs = readPacks(t, r); len(packs) != 1 || len(packs[0].(*protocol.MapPack).Entries) != 0 {
		t.Fatal(packs)
	}
}
func TestLocalStoreQueryListExactSchema(t *testing.T) {
	data := protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("query", list([]protocol.Value{stringValue("synthetic")})), entry("elapsed", list([]protocol.Value{decimal(4)}))}}
	b := &localStoreQueryBackend{fakeBackend: &fakeBackend{}, statuses: []*protocol.StatusPack{{Time: 100, ObjHash: 42, Key: "queries", Data: data}, {Time: 101, ObjHash: 7, Key: "queries", Data: data}, {Time: 102, ObjHash: 42, Key: "other", Data: data}}}
	_, c, r := startService(t, queryConfig(), b)
	session := loginSession(t, c, r)
	p := newMap(entry("date", stringValue("20260101")), entry("objHash", decimal(42)), entry("stime", decimal(99)), entry("etime", decimal(102)))
	send(t, c, "EXIST_QUERY_LIST", session, p)
	send(t, c, "LOAD_QUERY_LIST", session, p)
	packs := readPacks(t, r)
	if len(packs) != 1 {
		t.Fatal(packs)
	}
	times := lookup(packs[0].(*protocol.MapPack), "time").Values
	if len(times) != 2 || times[0].Int != 100 || times[1].Int != 102 {
		t.Fatal(times)
	}
	packs = readPacks(t, r)
	if len(packs) != 2 {
		t.Fatal(packs)
	}
	for _, pack := range packs {
		m := pack.(*protocol.MapPack)
		if len(m.Entries) != 2 || m.Entries[0].Key != "elapsed" || lookup(m, "objHash").Type != protocol.ValueNull || lookup(m, "time").Type != protocol.ValueNull {
			t.Fatal(m)
		}
	}
	if len(b.dates) != 2 || b.dates[0] != "20260101" || b.times[0] != [2]int64{99, 102} {
		t.Fatal(b.dates, b.times)
	}
	b.statuses = nil
	send(t, c, "EXIST_QUERY_LIST", session, p)
	packs = readPacks(t, r)
	if len(packs) != 1 || lookup(packs[0].(*protocol.MapPack), "time").Type != protocol.ValueList || len(lookup(packs[0].(*protocol.MapPack), "time").Values) != 0 {
		t.Fatal(packs)
	}
}
func TestLocalStoreGuardBeforeRead(t *testing.T) {
	s, err := New(queryConfig(), &fakeBackend{})
	if err != nil {
		t.Fatal(err)
	}
	for _, cmd := range append(LocalStoreCommands(), LocalStoreMutationCommands()...) {
		ok, err := s.HandleLocalStoreCommand(context.Background(), transport.ClientRequest{Command: cmd}, nil)
		if !ok || !errors.Is(err, transport.ErrUnauthorized) {
			t.Fatal(cmd, ok, err)
		}
		ok, err = s.HandleLocalStoreCommand(context.Background(), transport.ClientRequest{Command: cmd, Authenticated: true}, nil)
		if !ok || !errors.Is(err, transport.ErrUnsupported) {
			t.Fatal(cmd, ok, err)
		}
	}
	ok, err := s.HandleLocalStoreCommand(context.Background(), transport.ClientRequest{Command: "OTHER"}, nil)
	if ok || err != nil {
		t.Fatal(ok, err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	ok, err = s.HandleLocalStoreCommand(cancelled, transport.ClientRequest{Command: "GET_GLOBAL_KV", Authenticated: true}, nil)
	if !ok || !errors.Is(err, context.Canceled) {
		t.Fatal(ok, err)
	}
}

func TestLocalStoreBulkResultByteBound(t *testing.T) {
	b := &localStoreQueryBackend{fakeBackend: &fakeBackend{}, kv: map[string]string{GlobalKeySpace + ":a": strings.Repeat("x", 8<<20)}}
	_, c, r := startService(t, queryConfig(), b)
	session := loginSession(t, c, r)
	send(t, c, "GET_GLOBAL_KV_BULK", session, newMap(entry("key", list([]protocol.Value{stringValue("a")}))))
	// Handler rejects an excessive result before serializing a response item.
	if flag, err := r.ReadByte(); err == nil {
		t.Fatal("unbounded result", flag)
	}
}

// Actual pinned FloatValue/DoubleValue.toString methods. This test
// compares deterministic random bit patterns in addition to edge values.
func TestLocalKVJavaFloatingKeys(t *testing.T) {
	if _, err := exec.LookPath("java"); err != nil {
		t.Skip("Java unavailable; floating key oracle not executed")
	}
	work := t.TempDir()
	source := filepath.Join(work, "FloatingKeys.java")
	java := `import java.io.*;import java.util.Locale;import scouter.lang.value.*;import scouter.io.DataInputX;public class FloatingKeys{public static void main(String[]a)throws Exception{Locale.setDefault(Locale.US);System.out.println(System.getProperty("java.specification.version")+"\t"+new java.text.DecimalFormatSymbols(Locale.US).getNaN());BufferedReader r=new BufferedReader(new InputStreamReader(System.in,"UTF-8"));String s;while((s=r.readLine())!=null){String[]p=s.split(" ");if(p[0].equals("f"))System.out.println(new FloatValue(Float.intBitsToFloat((int)Long.parseUnsignedLong(p[1],16))).toString());else if(p[0].equals("d"))System.out.println(new DoubleValue(Double.longBitsToDouble(Long.parseUnsignedLong(p[1],16))).toString());else{byte[]b=new byte[p[1].length()/2];for(int i=0;i<b.length;i++)b[i]=(byte)Integer.parseInt(p[1].substring(i*2,i*2+2),16);System.out.println(new DataInputX(b).readValue().toString());}}}}`
	if err := os.WriteFile(source, []byte(java), 0600); err != nil {
		t.Fatal(err)
	}
	var input strings.Builder
	var floats []float32
	var doubles []float64
	for _, bits := range []uint32{0, 0x80000000, 1, 2, 3, 0x7f7fffff, 0x7f800000, 0xff800000, 0x7fc00000, math.Float32bits(.001), math.Float32bits(.0009999999), math.Float32bits(1e7), math.Float32bits(1e8)} {
		floats = append(floats, math.Float32frombits(bits))
	}
	for _, bits := range []uint64{0, 0x8000000000000000, 1, 0x7fefffffffffffff, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000000, math.Float64bits(1.2345678901234567), math.Float64bits(1e-18), math.Float64bits(1e23)} {
		doubles = append(doubles, math.Float64frombits(bits))
	}
	random := rand.New(rand.NewSource(7))
	for i := 0; i < 10000; i++ {
		floats = append(floats, math.Float32frombits(random.Uint32()))
		doubles = append(doubles, math.Float64frombits(random.Uint64()))
	}
	for _, v := range floats {
		fmt.Fprintf(&input, "f %x\n", math.Float32bits(v))
	}
	for _, v := range doubles {
		fmt.Fprintf(&input, "d %x\n", math.Float64bits(v))
	}
	cases := []protocol.Value{
		{Type: protocol.ValueTextHash, Int: -42}, {Type: protocol.ValueBlob, Bytes: []byte{1, 2, 3}}, {Type: protocol.ValueIP4, Bytes: []byte{127, 0, 0, 1}},
		{Type: protocol.ValueList, Values: []protocol.Value{stringValue("value"), {Type: protocol.ValueNull}, {Type: protocol.ValueFloat, Float: float32(math.Copysign(0, -1))}}},
		{Type: protocol.ValueIntArray, Ints: []int32{-42, 0, 42}}, {Type: protocol.ValueLongArray, Longs: []int64{math.MinInt64, math.MaxInt64}},
		{Type: protocol.ValueFloatArray, Floats: []float32{1.5, float32(math.NaN()), float32(math.Inf(1))}}, {Type: protocol.ValueTextArray, Texts: []string{"a", "한국어", ""}},
		{Type: protocol.ValueLongSummary, Int: 42, Count: 2, MinInt: -1, MaxInt: 100}, {Type: protocol.ValueDoubleSummary, Double: 1.5, Count: 3, MinDouble: math.Inf(-1), MaxDouble: math.NaN()},
		{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("Aa", decimal(1)), entry("BB", decimal(2)), entry("😀", stringValue("unicode")), entry("null", protocol.Value{Type: protocol.ValueNull})}},
	}
	for _, size := range []int{8, 9, 18, 40} {
		entries := make([]protocol.Entry, size)
		for i := range entries {
			entries[i] = entry(strconv.Itoa(i), decimal(int64(i)))
		}
		cases = append(cases, protocol.Value{Type: protocol.ValueMap, Entries: entries})
	}
	for _, v := range cases {
		raw, err := protocol.EncodeValue(v, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&input, "c %s\n", hex.EncodeToString(raw))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	original, err := filepath.Abs("../../../scouter")
	if err != nil {
		t.Fatal(err)
	}
	if configured := os.Getenv("SCOUTER_SOURCE"); configured != "" {
		original = configured
	}
	commit, err := exec.CommandContext(ctx, "git", "-C", original, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(commit)) != "dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5" {
		t.Fatal("source SHA mismatch")
	}
	dirty, err := exec.CommandContext(ctx, "git", "-C", original, "status", "--porcelain", "--", "scouter.common").Output()
	if err != nil || len(dirty) != 0 {
		t.Fatal("modified original common source", err)
	}
	compile := exec.CommandContext(ctx, "javac", "--release", "8", "-encoding", "UTF-8", "-sourcepath", filepath.Join(original, "scouter.common", "src", "main", "java"), "-d", work, source)
	if output, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("original FloatValue/DoubleValue compile: %v %s", err, output)
	}
	runtimes := []string{"java"}
	if runtime := os.Getenv("SCOUTER_SCALA_JAVA"); runtime != "" {
		runtimes = append(runtimes, runtime)
	} else {
		runtime := "/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home/bin/java"
		if _, err := os.Stat(runtime); err == nil {
			runtimes = append(runtimes, runtime)
		}
	}
	for index, runtime := range runtimes {
		t.Run(fmt.Sprintf("runtime-%d", index), func(t *testing.T) {
			cmd := exec.CommandContext(ctx, runtime, "-cp", work, "FloatingKeys")
			cmd.Stdin = strings.NewReader(input.String())
			out, err := cmd.Output()
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSuffix(string(out), "\n"), "\n")
			metadata := strings.Split(lines[0], "\t")
			if len(metadata) != 2 {
				t.Fatal("JVM formatting metadata", lines[0])
			}
			nanSymbol := metadata[1]
			lines = lines[1:]
			t.Logf("JVM %s DecimalFormat NaN=%q", metadata[0], nanSymbol)
			if len(lines) != len(floats)+len(doubles)+len(cases) {
				t.Fatal("oracle line count", len(lines))
			}
			failures := 0
			for i, v := range floats {
				if got := localJavaFloat(v); got != lines[i] {
					t.Errorf("Float bits %08x got %s expected %s", math.Float32bits(v), got, lines[i])
					failures++
					if failures > 15 {
						break
					}
				}
			}
			for i, v := range doubles {
				if math.IsNaN(v) && nanSymbol != "NaN" {
					if lines[len(floats)+i] != nanSymbol {
						t.Error("source NaN did not use JVM symbol")
					}
					continue
				}
				if got := localJavaDouble(v); got != lines[len(floats)+i] {
					t.Errorf("Double bits %016x got %s expected %s", math.Float64bits(v), got, lines[len(floats)+i])
					failures++
					if failures > 30 {
						break
					}
				}
			}
			for i, v := range cases {
				got, err := localKVKey(v)
				want := lines[len(floats)+len(doubles)+i]
				if v.Type == protocol.ValueDoubleSummary && math.IsNaN(v.MaxDouble) && nanSymbol != "NaN" {
					want = strings.ReplaceAll(want, "max="+nanSymbol, "max=NaN")
				}
				if err != nil || got != want {
					t.Errorf("compound type %d got %s expected %s err=%v", v.Type, got, want, err)
				}
			}

		})
	}

}

type localStoreFixtureBackend struct {
	*fakeBackend
	store *storage.Store
}

func (b *localStoreFixtureBackend) KeyValue(ctx context.Context, space, key string, now int64) (string, bool, error) {
	return b.store.KeyValueContext(ctx, space, key, now)
}
func (b *localStoreFixtureBackend) QueryList(ctx context.Context, date string, from, to int64, hash int32) ([]*protocol.StatusPack, error) {
	return b.store.QueryListContext(ctx, date, from, to, hash)
}

func TestLocalStoreActualJavaHandlersAndDB(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	original := os.Getenv("SCOUTER_SOURCE")
	if original == "" {
		original = filepath.Join(repo, "..", "scouter")
	}
	if _, err := os.Stat(filepath.Join(original, "scouter.server")); err != nil {
		t.Skip("pinned original source unavailable; local-store handler/DB oracle not executed")
	}
	root := t.TempDir()
	// Scala compilation of the pinned sources takes ~30s unloaded; a shared
	// host under -race and concurrent suites needs the same window as the
	// write-oracle test.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", filepath.Join(repo, "scripts", "java-storage.py"), "--generate-local-store", root, "--source", original)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("actual Java local-store oracle: %v\n%s", err, output)
	}
	t.Log(strings.TrimSpace(string(output)))
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	store, err := storage.Open(storage.Config{Directory: root, Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cfg := queryConfig()
	cfg.Location = loc
	_, conn, reader := startService(t, cfg, &localStoreFixtureBackend{fakeBackend: &fakeBackend{}, store: store})
	session := loginSession(t, conn, reader)
	fixture := func(name string) []byte {
		data, err := os.ReadFile(filepath.Join(root, name+".bin"))
		if err != nil {
			t.Fatal(name, err)
		}
		return data
	}
	compare := func(command string, request, response []byte) {
		t.Helper()
		e := protocol.NewEncoder(protocol.Limits{})
		e.Text(command)
		e.Int64(session)
		prefix, err := e.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write(append(prefix, request...)); err != nil {
			t.Fatal(command, err)
		}
		got := make([]byte, len(response))
		if _, err := io.ReadFull(reader, got); err != nil {
			t.Fatal(command, err)
		}
		if !bytes.Equal(got, response) {
			t.Fatalf("actual original %s wire mismatch got=%x expected=%x", command, got, response)
		}
		flag, err := reader.ReadByte()
		if err != nil || flag != transport.NoNext {
			t.Fatal(command, "pipeline end", flag, err)
		}
	}
	for _, test := range []struct{ name, command string }{{"kv-global", "GET_GLOBAL_KV"}, {"kv-global-missing", "GET_GLOBAL_KV"}, {"kv-custom", "GET_CUSTOM_KV"}, {"kv-custom-missing", "GET_CUSTOM_KV"}, {"kv-global-bulk", "GET_GLOBAL_KV_BULK"}, {"kv-custom-bulk", "GET_CUSTOM_KV_BULK"}, {"query-list-exist", "EXIST_QUERY_LIST"}, {"query-list-load", "LOAD_QUERY_LIST"}} {
		compare(test.command, fixture(test.name+"-request"), fixture(test.name+"-response"))
	}
	cases, err := protocol.DecodeValue(fixture("kv-key-cases"), protocol.Limits{})
	if err != nil || cases.Type != protocol.ValueList {
		t.Fatal("key cases", err)
	}
	for _, v := range cases.Values {
		if v.Type != protocol.ValueMap {
			t.Fatal("key case schema", v.Type)
		}
		m := &protocol.MapPack{Entries: v.Entries}
		request := lookup(m, "request")
		response := lookup(m, "response")
		if request.Type != protocol.ValueBlob || response.Type != protocol.ValueBlob {
			t.Fatal("key case blobs")
		}
		compare("GET_GLOBAL_KV", request.Bytes, response.Bytes)
	}
	for _, test := range []struct{ name, command string }{{"kv-global-bulk-empty", "GET_GLOBAL_KV_BULK"}, {"kv-custom-no-key", "GET_CUSTOM_KV"}} {
		compare(test.command, fixture(test.name+"-request"), fixture(test.name+"-response"))
	}
	// Replay original permanent-write inputs through the public Go API and
	// compare all three DB files, including updateOrPut duplicate side effects.
	seeds, err := protocol.DecodeValue(fixture("kv-seeds"), protocol.Limits{})
	if err != nil || seeds.Type != protocol.ValueList {
		t.Fatal("KV seed schema", err)
	}
	goRoot := t.TempDir()
	writer, err := storage.Open(storage.Config{Directory: goRoot, Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	for _, seed := range seeds.Values {
		if seed.Type != protocol.ValueMap {
			t.Fatal("KV seed type")
		}
		m := &protocol.MapPack{Entries: seed.Entries}
		space := lookup(m, "keySpace")
		key := lookup(m, "key")
		value := lookup(m, "value")
		if space.Type != protocol.ValueText || key.Type != protocol.ValueText || value.Type != protocol.ValueText {
			t.Fatal("KV seed fields")
		}
		if ok, err := writer.PutKeyValueContext(context.Background(), space.Text, key.Text, value.Text, -1, 1); err != nil || !ok {
			t.Fatal("Go write replay", space.Text, key.Text, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, space := range []string{GlobalKeySpace, "synthetic-space"} {
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
			if !bytes.Equal(got, want) {
				t.Fatalf("Go/Java KV file bytes differ: %s", relative)
			}
		}
	}
}
