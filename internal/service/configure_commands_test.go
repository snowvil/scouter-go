package service

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/snowvil/scouter-go/internal/counters"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

func TestConfigureCommandsGateBeforeBody(t *testing.T) {
	commands := ConfigureCommands()
	if len(commands) != 12 {
		t.Fatal(commands)
	}
	commands[0] = "changed"
	if ConfigureCommands()[0] == "changed" {
		t.Fatal("borrowed command slice")
	}
	if handled, err := (&Service{}).HandleConfigureCommand(context.Background(), transport.ClientRequest{Command: "GET_CONFIGURE_SERVER", Authenticated: true}, nil); handled || err != nil {
		t.Fatal("GET stays with the metadata handler", handled, err)
	}
	for _, cmd := range ConfigureCommands() {
		s := &Service{}
		handled, err := s.HandleConfigureCommand(context.Background(), transport.ClientRequest{Command: cmd}, nil)
		if !handled || !errors.Is(err, transport.ErrUnauthorized) {
			t.Fatal(cmd, handled, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if handled, err = s.HandleConfigureCommand(ctx, transport.ClientRequest{Command: cmd, Authenticated: true}, nil); !handled || !errors.Is(err, context.Canceled) {
			t.Fatal(cmd, handled, err)
		}
	}
	// No configured owner: explicit failure without touching the (nil) stream.
	for _, cmd := range []string{"SET_CONFIGURE_SERVER", "SET_CONFIGURE_TELEGRAF", "SET_CONFIGURE_COUNTERS_SITE", "DEFINE_OBJECT_TYPE", "EDIT_OBJECT_TYPE", "SAVE_ALERT_SCRIPTING_CONTETNS", "SAVE_ALERT_SCRIPTING_CONFIG_CONTETNS", "GET_ALERT_SCRIPT_LOAD_MESSAGE"} {
		handled, err := (&Service{}).HandleConfigureCommand(context.Background(), transport.ClientRequest{Command: cmd, Authenticated: true}, nil)
		if !handled || !errors.Is(err, transport.ErrUnsupported) {
			t.Fatal(cmd, handled, err)
		}
	}
}

type configureFixture struct {
	dir, plugin, server, telegraf string
	meta                          *FileMetadata
	store                         *counters.Store
	mu                            sync.Mutex
	reloads                       []string
	reloadErr                     error
}

func newConfigureFixture(t *testing.T) *configureFixture {
	t.Helper()
	f := &configureFixture{dir: t.TempDir()}
	f.plugin = filepath.Join(f.dir, "plugin")
	f.server = filepath.Join(f.dir, "conf", "scouter.conf")
	f.telegraf = filepath.Join(f.dir, "conf", "scouter-telegraf.xml")
	def, err := os.ReadFile("../../cmd/scouter-go/counters.xml")
	if err != nil {
		t.Fatal(err)
	}
	f.store, err = counters.Open(counters.StoreConfig{Dir: filepath.Join(f.dir, "conf"), Default: def})
	if err != nil {
		t.Fatal(err)
	}
	sample := "<sample/>"
	f.meta, err = NewFileMetadata(FileMetadataConfig{
		PluginDir: f.plugin, ServerConfigFile: f.server, TelegrafFile: f.telegraf, TelegrafText: &sample, CountersSite: f.store,
		Properties: []MetadataProperty{{Key: "net_tcp_listen_port", Value: "6100", Type: 2}},
		Reload: func(_ context.Context, kind string) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.reloads = append(f.reloads, kind)
			return f.reloadErr
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *configureFixture) start(t *testing.T) (net.Conn, *bufio.Reader, int64) {
	t.Helper()
	cfg := queryConfig()
	cfg.Metadata = f.meta
	_, conn, r := startService(t, cfg, &fakeBackend{})
	return conn, r, loginSession(t, conn, r)
}

func expectNoResponse(t *testing.T, conn net.Conn, session int64, command string, p protocol.Pack) {
	t.Helper()
	c, r := dialClient(t, conn.RemoteAddr())
	send(t, c, command, session, p)
	if flag, err := r.ReadByte(); err == nil {
		t.Fatal(command, "answered an explicit failure", flag)
	}
}

func singleMap(t *testing.T, r *bufio.Reader) *protocol.MapPack {
	t.Helper()
	packs := readPacks(t, r)
	if len(packs) != 1 {
		t.Fatal("items", len(packs))
	}
	m, ok := packs[0].(*protocol.MapPack)
	if !ok {
		t.Fatalf("not a MapPack: %T", packs[0])
	}
	return m
}

func keys(m *protocol.MapPack) []string {
	var out []string
	for _, e := range m.Entries {
		out = append(out, e.Key)
	}
	return out
}

func TestConfigureServerAndTelegrafTCP(t *testing.T) {
	f := newConfigureFixture(t)
	conn, r, session := f.start(t)
	// GET before any file: Configure.loadText null -> "".
	send(t, conn, "GET_CONFIGURE_SERVER", session, nil)
	if m := singleMap(t, r); lookup(m, "serverConfig").Text != "" || len(lookup(m, "configKey").Values) != 1 {
		t.Fatal("initial", m)
	}
	text := "# synthetic\nnet_tcp_listen_port=6200\nkey=한글\n"
	send(t, conn, "SET_CONFIGURE_SERVER", session, newMap(entry("setConfig", stringValue(text))))
	m := singleMap(t, r)
	if v := lookup(m, "result"); v.Type != protocol.ValueText || v.Text != "true" || !reflect.DeepEqual(keys(m), []string{"result"}) {
		t.Fatal("result must be Text \"true\"", m)
	}
	if data, _ := os.ReadFile(f.server); string(data) != text {
		t.Fatal("server file", string(data))
	}
	if info, _ := os.Stat(f.server); info.Mode().Perm() != 0o600 {
		t.Fatal("new server config mode", info.Mode())
	}
	send(t, conn, "GET_CONFIGURE_SERVER", session, nil)
	if m := singleMap(t, r); lookup(m, "serverConfig").Text != text {
		t.Fatal("GET after SET", m)
	}
	// Absent / non-Text setConfig: Java saveText(null) -> "false", no write.
	for _, p := range []*protocol.MapPack{newMap(), newMap(entry("setConfig", decimal(1)))} {
		send(t, conn, "SET_CONFIGURE_SERVER", session, p)
		if m := singleMap(t, r); lookup(m, "result").Text != "false" {
			t.Fatal("null text", m)
		}
	}
	telegraf := "<Telegraf/>"
	send(t, conn, "SET_CONFIGURE_TELEGRAF", session, newMap(entry("tgConfigContents", stringValue(telegraf))))
	if m := singleMap(t, r); lookup(m, "result").Text != "true" {
		t.Fatal("telegraf", m)
	}
	send(t, conn, "GET_CONFIGURE_TELEGRAF", session, nil)
	if m := singleMap(t, r); lookup(m, "tgConfigContents").Text != telegraf {
		t.Fatal("telegraf GET", m)
	}
	// A validator rejection answers "false" and keeps the previous file.
	f.meta.config.ValidateTelegraf = func(text string) error {
		if strings.HasPrefix(text, "<Telegraf") {
			return nil
		}
		return errors.New("synthetic invalid telegraf XML")
	}
	send(t, conn, "SET_CONFIGURE_TELEGRAF", session, newMap(entry("tgConfigContents", stringValue("not xml"))))
	if m := singleMap(t, r); lookup(m, "result").Text != "false" {
		t.Fatal("invalid telegraf", m)
	}
	if data, _ := os.ReadFile(f.telegraf); string(data) != telegraf {
		t.Fatal("rejected telegraf text saved", string(data))
	}
	send(t, conn, "SET_CONFIGURE_TELEGRAF", session, newMap(entry("tgConfigContents", stringValue(telegraf))))
	if m := singleMap(t, r); lookup(m, "result").Text != "true" {
		t.Fatal("valid telegraf", m)
	}
	f.meta.config.ValidateTelegraf = nil
	f.mu.Lock()
	if !reflect.DeepEqual(f.reloads, []string{"server", "telegraf", "telegraf"}) {
		t.Fatal("reloads", f.reloads)
	}
	f.reloadErr = errors.New("synthetic reload failure")
	f.mu.Unlock()
	// Reload failure after the write is logged: Java answers "true" once saved.
	send(t, conn, "SET_CONFIGURE_SERVER", session, newMap(entry("setConfig", stringValue("a=1\n"))))
	if m := singleMap(t, r); lookup(m, "result").Text != "true" {
		t.Fatal("reload failure changed the answer", m)
	}
	if data, _ := os.ReadFile(f.server); string(data) != "a=1\n" {
		t.Fatal("file not written before reload", string(data))
	}
	f.mu.Lock()
	f.reloadErr = nil
	f.mu.Unlock()
	// Symlinked target: refused as a save failure ("false"), target untouched.
	outside := filepath.Join(t.TempDir(), "outside.conf")
	os.WriteFile(outside, []byte("keep"), 0o644)
	os.Remove(f.server)
	os.Symlink(outside, f.server)
	send(t, conn, "SET_CONFIGURE_SERVER", session, newMap(entry("setConfig", stringValue("x=1"))))
	if m := singleMap(t, r); lookup(m, "result").Text != "false" {
		t.Fatal("symlink save", m)
	}
	if data, _ := os.ReadFile(outside); string(data) != "keep" {
		t.Fatal("symlink followed")
	}
	// Unconfigured file owner is unsupported, not a fake success.
	meta, _ := NewFileMetadata(FileMetadataConfig{})
	if err := meta.SaveConfigurationText(context.Background(), "server", "x"); !errors.Is(err, transport.ErrUnsupported) {
		t.Fatal(err)
	}
	if err := f.meta.SaveConfigurationText(context.Background(), "server", strings.Repeat("x", 4<<20+1)); !errors.Is(err, transport.ErrLimit) {
		t.Fatal(err)
	}
	if err := f.meta.SaveConfigurationText(context.Background(), "other", "x"); !errors.Is(err, transport.ErrUnsupported) {
		t.Fatal(err)
	}
	// Unconfigured owner over TCP: the command fails, it is not "false".
	cfg := queryConfig()
	cfg.Metadata = meta
	_, c2, r2 := startService(t, cfg, &fakeBackend{})
	s2 := loginSession(t, c2, r2)
	send(t, c2, "SET_CONFIGURE_TELEGRAF", s2, newMap(entry("tgConfigContents", stringValue("x"))))
	if flag, err := r2.ReadByte(); err == nil {
		t.Fatal("unsupported owner answered", flag)
	}
}

func TestConfigureCountersSiteAndObjectTypesTCP(t *testing.T) {
	f := newConfigureFixture(t)
	conn, r, session := f.start(t)
	custom := filepath.Join(f.dir, "conf", counters.CustomFileName)
	send(t, conn, "GET_CONFIGURE_COUNTERS_SITE", session, nil)
	if m := singleMap(t, r); lookup(m, "contents").Text != "" {
		t.Fatal("missing file text", m)
	}
	contents := `<Counters><Familys><Family name="fam" master="c"><Counter name="c"/></Family></Familys><Types><ObjectType name="t1" family="fam"/></Types></Counters>`
	send(t, conn, "SET_CONFIGURE_COUNTERS_SITE", session, newMap(entry("contents", stringValue(contents))))
	if m := singleMap(t, r); lookup(m, "result").Type != protocol.ValueText || lookup(m, "result").Text != "true" {
		t.Fatal("counters-site set", m)
	}
	want := "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<Counters>\n    <Types>\n" +
		"        <ObjectType name=\"t1\" family=\"fam\"/>\n    </Types>\n    <Familys>\n" +
		"        <Family name=\"fam\" master=\"c\">\n            <Counter name=\"c\"/>\n" +
		"        </Family>\n    </Familys>\n</Counters>\n"
	if data, _ := os.ReadFile(custom); string(data) != want || string(f.store.CustomXML()) != want {
		t.Fatalf("normalized file %q", data)
	}
	send(t, conn, "GET_CONFIGURE_COUNTERS_SITE", session, nil)
	if m := singleMap(t, r); lookup(m, "contents").Text != want {
		t.Fatal("GET after SET", m)
	}
	// Java parse rejection is "false".
	send(t, conn, "SET_CONFIGURE_COUNTERS_SITE", session, newMap(entry("contents", stringValue("<Counters>"))))
	if m := singleMap(t, r); lookup(m, "result").Text != "false" {
		t.Fatal("malformed", m)
	}
	// Java NullPointerException on null contents answers nothing.
	expectNoResponse(t, conn, session, "SET_CONFIGURE_COUNTERS_SITE", newMap())
	if data, _ := os.ReadFile(custom); string(data) != want {
		t.Fatal("failed set changed the file")
	}
	define := func(cmd, name, family string, sub protocol.Value) protocol.Value {
		p := newMap(entry("disp", stringValue("Disp "+name)), entry("family", stringValue(family)), entry("icon", stringValue("ic")), entry("sub-object", sub))
		if name != "" {
			p.Entries = append([]protocol.Entry{entry("name", stringValue(name))}, p.Entries...)
		}
		send(t, conn, cmd, session, p)
		return readKVValue(t, r)
	}
	for _, tc := range []struct {
		cmd, name, family string
		sub               protocol.Value
		want              bool
	}{
		{"DEFINE_OBJECT_TYPE", "t2", "fam", boolean(true), true},
		{"DEFINE_OBJECT_TYPE", "t2", "fam", boolean(true), false},         // exists
		{"DEFINE_OBJECT_TYPE", "tomcat", "javaee", boolean(false), false}, // default type exists
		{"DEFINE_OBJECT_TYPE", "t3", "nofamily", boolean(false), false},
		{"DEFINE_OBJECT_TYPE", "", "fam", boolean(false), false}, // null name
		{"EDIT_OBJECT_TYPE", "t2", "host", stringValue("true"), true},
		{"EDIT_OBJECT_TYPE", "t9", "nofamily", boolean(true), false},
	} {
		v := define(tc.cmd, tc.name, tc.family, tc.sub)
		if v.Type != protocol.ValueBoolean || v.Bool != tc.want {
			t.Fatal(tc.cmd, tc.name, v)
		}
	}
	o := f.store.Engine().ObjectType("t2")
	if o == nil || o.Family == nil || o.Family.Name != "host" || o.SubObject || o.Display != "Disp t2" {
		t.Fatalf("edited type %#v", o)
	}
	f.mu.Lock()
	if !reflect.DeepEqual(f.reloads, []string{"counters-site", "counters-site", "counters-site"}) {
		t.Fatal("reloads", f.reloads)
	}
	f.mu.Unlock()
	// The connection stays usable: no extra body was consumed.
	send(t, conn, "GET_CONFIGURE_COUNTERS_SITE", session, nil)
	if m := singleMap(t, r); !strings.Contains(lookup(m, "contents").Text, `name="t2"`) {
		t.Fatal("final contents", m)
	}
	// Empty <Types/> is accepted (Java NPE, decision §9-3).
	send(t, conn, "SET_CONFIGURE_COUNTERS_SITE", session, newMap(entry("contents", stringValue("<Counters><Types/></Counters>"))))
	if m := singleMap(t, r); lookup(m, "result").Text != "true" {
		t.Fatal("empty Types", m)
	}
	if f.store.Engine().ObjectType("t2") != nil || f.store.Engine().Family("host") == nil {
		t.Fatal("engine after empty custom")
	}
}

func TestAlertScriptSaveAndLoadMessagesTCP(t *testing.T) {
	f := newConfigureFixture(t)
	os.MkdirAll(f.plugin, 0o755)
	os.WriteFile(filepath.Join(f.plugin, "pre.alert"), []byte("old"), 0o644)
	os.Symlink(filepath.Join(f.plugin, "pre.alert"), filepath.Join(f.plugin, "linked.alert"))
	conn, r, session := f.start(t)
	messages := func(loop, index int64) (int64, int64, []string) {
		send(t, conn, "GET_ALERT_SCRIPT_LOAD_MESSAGE", session, newMap(entry("loop", decimal(loop)), entry("index", decimal(index))))
		m := singleMap(t, r)
		if !reflect.DeepEqual(keys(m), []string{"loop", "index", "messages"}) {
			t.Fatal("keys", keys(m))
		}
		var out []string
		for _, v := range lookup(m, "messages").Values {
			out = append(out, v.Text)
		}
		return lookup(m, "loop").Int, lookup(m, "index").Int, out
	}
	loop, index, got := messages(0, 0)
	if loop != 0 || index != 2 || len(got) != 2 || !strings.HasPrefix(got[0], alertScriptUnsupported) || got[1] != alertScriptUnsupported+"; pre.alert detected, not compiled" {
		t.Fatal(loop, index, got)
	}
	send(t, conn, "SAVE_ALERT_SCRIPTING_CONTETNS", session, newMap(entry("counterName", stringValue("Cpu")), entry("contents", stringValue("if ($counter.getIntValue() > 1) {}"))))
	if m := singleMap(t, r); lookup(m, "success").Type != protocol.ValueBoolean || !lookup(m, "success").Bool {
		t.Fatal("save", m)
	}
	send(t, conn, "SAVE_ALERT_SCRIPTING_CONFIG_CONTETNS", session, newMap(entry("counterName", stringValue("Cpu")), entry("contents", stringValue("history_size=150"))))
	if m := singleMap(t, r); !lookup(m, "success").Bool {
		t.Fatal("save conf", m)
	}
	for name, want := range map[string]string{"Cpu.alert": "if ($counter.getIntValue() > 1) {}", "Cpu.conf": "history_size=150"} {
		if data, _ := os.ReadFile(filepath.Join(f.plugin, name)); string(data) != want {
			t.Fatal(name, string(data))
		}
	}
	send(t, conn, "GET_ALERT_SCRIPTING_CONTETNS", session, newMap(entry("counterName", stringValue("Cpu"))))
	if m := singleMap(t, r); lookup(m, "contents").Text != "if ($counter.getIntValue() > 1) {}" {
		t.Fatal("read back", m)
	}
	loop, index, got = messages(loop, index)
	if loop != 0 || index != 4 || !reflect.DeepEqual(got, []string{alertScriptUnsupported + "; Cpu.alert saved, not compiled", alertScriptUnsupported + "; Cpu.conf saved, not applied"}) {
		t.Fatal(loop, index, got)
	}
	// Null contents: Java saveText NPE caught -> success false, no file.
	send(t, conn, "SAVE_ALERT_SCRIPTING_CONTETNS", session, newMap(entry("counterName", stringValue("Nothing"))))
	if m := singleMap(t, r); lookup(m, "success").Bool {
		t.Fatal("null contents", m)
	}
	if _, err := os.Stat(filepath.Join(f.plugin, "Nothing.alert")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("null contents wrote a file")
	}
	// Null counterName is "null.alert", like the Java string concatenation.
	send(t, conn, "SAVE_ALERT_SCRIPTING_CONTETNS", session, newMap(entry("contents", stringValue("n"))))
	if m := singleMap(t, r); !lookup(m, "success").Bool {
		t.Fatal("null name", m)
	}
	if data, _ := os.ReadFile(filepath.Join(f.plugin, "null.alert")); string(data) != "n" {
		t.Fatal("null.alert")
	}
	// External change detection: removal and modification of rule files.
	os.Remove(filepath.Join(f.plugin, "pre.alert"))
	_, index, got = messages(loop, index)
	if index != 6 || !reflect.DeepEqual(got, []string{alertScriptUnsupported + "; null.alert saved, not compiled", alertScriptUnsupported + "; pre.alert removed"}) {
		t.Fatal(index, got)
	}
	// Traversal and symlink targets are refused explicitly.
	expectNoResponse(t, conn, session, "SAVE_ALERT_SCRIPTING_CONTETNS", newMap(entry("counterName", stringValue("../escape")), entry("contents", stringValue("x"))))
	expectNoResponse(t, conn, session, "SAVE_ALERT_SCRIPTING_CONTETNS", newMap(entry("counterName", stringValue("linked")), entry("contents", stringValue("x"))))
	if _, err := os.Stat(filepath.Join(f.dir, "escape.alert")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("traversal wrote")
	}
	// Java ArrayIndexOutOfBounds (current loop, negative index): no response.
	expectNoResponse(t, conn, session, "GET_ALERT_SCRIPT_LOAD_MESSAGE", newMap(entry("index", decimal(-1))))
}

func TestAlertScriptLoopCacheJavaCursor(t *testing.T) {
	c := &alertScriptMessages{rules: map[string]alertRuleStamp{}}
	for i := 0; i < 120; i++ {
		c.putLocked(strings.Repeat("m", 1) + string(rune('A'+i%26)) + "-" + string(rune('0'+i/26)))
	}
	all := func(start, end int) []string {
		var out []string
		for i := start; i < end; i++ {
			out = append(out, c.queue[i])
		}
		return out
	}
	// loop=2, index=20 after 120 puts into 50 slots.
	if c.loop != 2 || c.index != 20 {
		t.Fatal(c.loop, c.index)
	}
	ring := append(all(20, 50), all(0, 20)...)
	for _, tc := range []struct {
		loop  int64
		index int32
		want  []string
	}{
		{2, 5, all(5, 20)},
		{2, 20, nil},
		{2, 30, nil},
		{1, 10, ring},
		{1, 30, append(all(30, 50), all(0, 20)...)},
		{1, 70, all(0, 20)},
		{0, 0, ring},
		{9, 0, ring}, // client ahead of the server: full ring like Java's default case
	} {
		out, err := c.get(tc.loop, tc.index)
		if err != nil || out.Loop != 2 || out.Index != 20 || len(out.Messages) != len(tc.want) || (len(tc.want) > 0 && !reflect.DeepEqual(out.Messages, tc.want)) {
			t.Fatal(tc.loop, tc.index, out, err)
		}
	}
	if _, err := c.get(2, -1); !errors.Is(err, transport.ErrMalformed) {
		t.Fatal(err)
	}
	if out, err := c.get(1, -1); err != nil || len(out.Messages) != 50 {
		t.Fatal(out, err)
	}
}

func TestMethodDescriptorsTCPAndFixture(t *testing.T) {
	data, err := os.ReadFile("testdata/alert_script_desc.json")
	if err != nil {
		t.Fatal(err)
	}
	type fixture struct {
		Desc               string   `json:"desc"`
		MethodName         string   `json:"methodName"`
		ReturnTypeName     string   `json:"returnTypeName"`
		ParameterTypeNames []string `json:"parameterTypeNames"`
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	f := newConfigureFixture(t)
	conn, r, session := f.start(t)
	for cmd, table := range map[string][]MethodDescriptor{"GET_ALERT_REAL_COUNTER_DESC": RealCounterDescriptors(), "GET_PLUGIN_HELPER_DESC": PluginHelperDescriptors()} {
		var want []fixture
		if err := json.Unmarshal(doc[cmd], &want); err != nil {
			t.Fatal(err)
		}
		if len(table) != len(want) {
			t.Fatal(cmd, len(table))
		}
		send(t, conn, cmd, session, nil)
		packs := readPacks(t, r)
		if len(packs) != len(want) {
			t.Fatal(cmd, "items", len(packs))
		}
		for i, p := range packs {
			m := p.(*protocol.MapPack)
			if !reflect.DeepEqual(keys(m), []string{"desc", "methodName", "returnTypeName", "parameterTypeNames"}) {
				t.Fatal(keys(m))
			}
			var params []string
			for _, v := range lookup(m, "parameterTypeNames").Values {
				params = append(params, v.Text)
			}
			got := fixture{lookup(m, "desc").Text, lookup(m, "methodName").Text, lookup(m, "returnTypeName").Text, params}
			w := want[i]
			if len(w.ParameterTypeNames) == 0 {
				w.ParameterTypeNames = nil
			}
			if !reflect.DeepEqual(got, w) || table[i].MethodName != w.MethodName {
				t.Fatal(cmd, i, got, w)
			}
		}
	}
	// §5.1: the Java order depends on the JVM (recorded for JDK 8/18). Each
	// observed order must be a permutation of the Go table's methods.
	var observed map[string]json.RawMessage
	if err := json.Unmarshal(doc["observed_getDeclaredMethods_order"], &observed); err != nil {
		t.Fatal(err)
	}
	for cmd, table := range map[string][]MethodDescriptor{"GET_ALERT_REAL_COUNTER_DESC": RealCounterDescriptors(), "GET_PLUGIN_HELPER_DESC": PluginHelperDescriptors()} {
		var jdk map[string][]string
		if err := json.Unmarshal(observed[cmd], &jdk); err != nil || len(jdk) != 2 {
			t.Fatal(cmd, err)
		}
		var goNames []string
		for _, d := range table {
			goNames = append(goNames, d.MethodName)
		}
		sort.Strings(goNames)
		for vm, labels := range jdk {
			var names []string
			for _, l := range labels {
				names = append(names, strings.SplitN(l, "(", 2)[0])
			}
			sort.Strings(names)
			if !reflect.DeepEqual(names, goNames) {
				t.Fatal(cmd, vm, names)
			}
		}
	}
	// No body was consumed: the next command on the same connection works.
	send(t, conn, "SERVER_TIME", session, nil)
	if m := singleMap(t, r); lookup(m, "time").Type != protocol.ValueDecimal {
		t.Fatal(m)
	}
	table := RealCounterDescriptors()
	table[0].ParameterTypeNames = append(table[0].ParameterTypeNames, "x")
	if len(RealCounterDescriptors()[0].ParameterTypeNames) != 0 {
		t.Fatal("borrowed descriptor slice")
	}
}

// TestMethodDescriptorsMatchJavaSource re-derives both tables from the pinned
// Java sources: public non-static methods without @Deprecated/@Internal,
// @ParamDesc or Class.getName of each parameter, @ConfigDesc. It needs
// SCOUTER_SOURCE (the original repository); without it the test is skipped,
// which is not a pass.
func TestMethodDescriptorsMatchJavaSource(t *testing.T) {
	root := os.Getenv("SCOUTER_SOURCE")
	if root == "" {
		t.Skip("SCOUTER_SOURCE not set: Java source descriptor comparison not executed")
	}
	base := filepath.Join(root, "scouter.server/src/main/java/scouter/server/plugin")
	for file, want := range map[string][]MethodDescriptor{"alert/RealCounter.java": RealCounterDescriptors(), "PluginHelper.java": PluginHelperDescriptors()} {
		src, err := os.ReadFile(filepath.Join(base, file))
		if err != nil {
			t.Fatal(err)
		}
		got := javaSourceDescriptors(t, string(src))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s:\n got %#v\nwant %#v", file, got, want)
		}
	}
}

var (
	javaMethodDecl = regexp.MustCompile(`^\s*public\s+(static\s+)?(?:synchronized\s+)?([\w.<>\[\], ]+?)\s+(\w+)\(([^)]*)\)`)
	javaAnnotation = regexp.MustCompile(`^\s*@(\w+)(?:\("((?:[^"\\]|\\.)*)"\))?\s*$`)
)

func javaSourceDescriptors(t *testing.T, src string) []MethodDescriptor {
	t.Helper()
	simple := map[string]string{"String": "java.lang.String", "Object": "java.lang.Object", "NumberFormat": "java.text.NumberFormat", "HashMap": "java.util.HashMap", "Map": "java.util.Map", "Value": "scouter.lang.value.Value", "Number": "java.lang.Number"}
	typeName := func(s string) string {
		s = strings.TrimSpace(s)
		if i := strings.IndexByte(s, '<'); i >= 0 {
			s = s[:i]
		}
		if full, ok := simple[s]; ok {
			return full
		}
		return s
	}
	var out []MethodDescriptor
	annotations := map[string]string{}
	depth := 0
	for _, line := range strings.Split(src, "\n") {
		if depth == 1 {
			if m := javaAnnotation.FindStringSubmatch(line); m != nil {
				annotations[m[1]] = m[2]
				continue
			}
			if m := javaMethodDecl.FindStringSubmatch(line); m != nil && !strings.Contains(line, " class ") {
				_, deprecated := annotations["Deprecated"]
				_, internal := annotations["Internal"]
				if m[1] == "" && !deprecated && !internal {
					d := MethodDescriptor{Desc: annotations["ConfigDesc"], MethodName: m[3], ReturnTypeName: typeName(m[2]), ParameterTypeNames: []string{}}
					if p, ok := annotations["ParamDesc"]; ok {
						d.ParameterTypeNames = []string{p}
					} else if strings.TrimSpace(m[4]) != "" {
						for _, param := range strings.Split(m[4], ",") {
							fields := strings.Fields(param)
							d.ParameterTypeNames = append(d.ParameterTypeNames, typeName(fields[len(fields)-2]))
						}
					}
					out = append(out, d)
				}
			}
			if strings.TrimSpace(line) != "" {
				annotations = map[string]string{}
			}
		}
		depth += strings.Count(line, "{") - strings.Count(line, "}")
	}
	return out
}

func TestClientEmptyMapPackBytes(t *testing.T) {
	raw, err := protocol.EncodePack(newMap(), protocol.Limits{})
	if err != nil || string(raw) != string(clientEmptyMapPack) {
		t.Fatalf("% x %v", raw, err)
	}
	// transport.Stream implements ConsumeBuffered (§9-11 (b)); a regression
	// here would silently restore the original desync.
	if _, active := any(&transport.Stream{}).(bufferedConsumer); !active {
		t.Fatal("transport.Stream must implement ConsumeBuffered (§9-11 b)")
	}
}

// TestDescriptorTrailingEmptyMapPack sends what the Java Client sends:
// GET_*_DESC followed by an empty MapPack in the same write, then another
// command. It needs transport.Stream.ConsumeBuffered (§9-11 (b)); without it
// the original behaviour (body not read) applies and the test is skipped,
// which is not a pass.
func TestDescriptorTrailingEmptyMapPack(t *testing.T) {
	f := newConfigureFixture(t)
	conn, r, session := f.start(t)
	for _, cmd := range []string{"GET_ALERT_REAL_COUNTER_DESC", "GET_PLUGIN_HELPER_DESC"} {
		sendRaw(t, conn, cmd, session, clientEmptyMapPack)
		if packs := readPacks(t, r); len(packs) < 22 {
			t.Fatal(cmd, len(packs))
		}
	}
	send(t, conn, "SERVER_TIME", session, nil)
	if m := singleMap(t, r); lookup(m, "time").Type != protocol.ValueDecimal {
		t.Fatal(m)
	}
	// Without the trailing body the next command is not consumed either.
	send(t, conn, "GET_ALERT_REAL_COUNTER_DESC", session, nil)
	if len(readPacks(t, r)) != 25 {
		t.Fatal("descriptor without body")
	}
	send(t, conn, "SERVER_TIME", session, nil)
	if m := singleMap(t, r); lookup(m, "time").Type != protocol.ValueDecimal {
		t.Fatal(m)
	}
}
