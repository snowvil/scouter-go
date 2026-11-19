package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

func TestMetadataReadScope(t *testing.T) {
	data, err := os.ReadFile("../../docs/compatibility/query-scope.json")
	if err != nil {
		t.Fatal(err)
	}
	var scope struct {
		Commands []struct {
			Literal  string `json:"command_literal"`
			Category string `json:"category"`
			Handler  string `json:"handler"`
		}
	}
	if err = json.Unmarshal(data, &scope); err != nil {
		t.Fatal(err)
	}
	owners := map[string]bool{"AccountService": true, "LoginService": true, "ConfigureService": true, "ServerDebug": true, "AlertScriptingService": true, "ManageService": true, "ServerInfo": true, "ThreadList": true}
	var want []string
	for _, c := range scope.Commands {
		if c.Category == "local_read" && owners[strings.Split(c.Handler, ".")[0]] {
			want = append(want, c.Literal)
		}
		// CHECK_JOB consumes the RemoteControlManager queue in Java (mutation in
		// the inventory). Its only producers, REMOTE_CONTROL*, are unsupported,
		// so Go answers it with the Java no-job response instead of refusing.
		if c.Literal == "CHECK_JOB" {
			want = append(want, c.Literal)
			continue
		}
		if c.Category == "mutation" {
			handled, err := (&Service{}).HandleMetadataCommand(context.Background(), transport.ClientRequest{Command: c.Literal}, nil)
			if handled || err != nil {
				t.Fatal(c.Literal, handled, err)
			}
		}
	}
	got := MetadataCommands()
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != 26 || !reflect.DeepEqual(got, want) {
		t.Fatal(got, want)
	}
	got[0] = "changed"
	if MetadataCommands()[0] == "changed" {
		t.Fatal("borrowed command slice")
	}
}

func TestMetadataAuthorizationCancellationAndUnsupported(t *testing.T) {
	for _, cmd := range MetadataCommands() {
		s := &Service{}
		handled, err := s.HandleMetadataCommand(context.Background(), transport.ClientRequest{Command: cmd}, nil)
		if !handled || !errors.Is(err, transport.ErrUnauthorized) {
			t.Fatal(cmd, handled, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		handled, err = s.HandleMetadataCommand(ctx, transport.ClientRequest{Command: cmd, Authenticated: true}, nil)
		if !handled || !errors.Is(err, context.Canceled) {
			t.Fatal(cmd, handled, err)
		}
	}
}

func TestMetadataTCPBodiesAndOrder(t *testing.T) {
	cfg := queryConfig()
	meta, err := NewFileMetadata(FileMetadataConfig{Properties: []MetadataProperty{{Key: "net_tcp_client_port", Value: "6100", Default: "6100", Description: "Client port", Type: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Metadata = meta
	_, conn, r := startService(t, cfg, &fakeBackend{})
	session := loginSession(t, conn, r)
	send(t, conn, "CHECK_ACCOUNT_ID", session, newMap(entry("id", stringValue(cfg.Username))))
	packs := readPacks(t, r)
	if len(packs) != 1 || lookup(packs[0].(*protocol.MapPack), "result").Bool {
		t.Fatal(packs)
	}
	// CHECK_JOB with no queued remote-control job: Java writes nothing, so the
	// dispatcher's NoNEXT is the whole response and the connection stays open.
	send(t, conn, "CHECK_JOB", session, newMap(entry("session", decimal(session))))
	if packs := readPacks(t, r); len(packs) != 0 {
		t.Fatal(packs)
	}
	send(t, conn, "CHECK_LOGIN", session, newMap(entry("id", stringValue(cfg.Username)), entry("pass", stringValue("invalid"))))
	aggregationFlag(t, r, transport.HasNext)
	v, err := protocol.ReadValue(r, protocol.Limits{})
	if err != nil || v.Type != protocol.ValueBoolean || v.Bool {
		t.Fatal(v, err)
	}
	aggregationFlag(t, r, transport.NoNext)
	for _, cmd := range []string{"LIST_ACCOUNT_GROUP", "GET_GROUP_POLICY_ALL", "GET_CONFIGURE_SERVER", "LIST_CONFIGURE_SERVER", "CONFIGURE_DESC", "CONFIGURE_VALUE_TYPE", "SERVER_ENV"} {
		var p protocol.Pack
		if strings.HasPrefix(cmd, "CONFIGURE_") {
			p = newMap(entry("objHash", decimal(0)))
		}
		send(t, conn, cmd, session, p)
		packs = readPacks(t, r)
		if len(packs) != 1 {
			t.Fatal(cmd, packs)
		}
		m := packs[0].(*protocol.MapPack)
		if cmd == "GET_CONFIGURE_SERVER" && !reflect.DeepEqual(m, newMap(entry("configKey", list([]protocol.Value{stringValue("net_tcp_client_port")})), entry("serverConfig", stringValue("net_tcp_client_port=6100\n")))) {
			t.Fatal(m)
		}
	}
	send(t, conn, "CONFIGURE_VALUE_TYPE_DESC", session, newMap(entry("objHash", decimal(9))))
	packs = readPacks(t, r)
	if len(packs) != 1 || len(packs[0].(*protocol.MapPack).Entries) != 0 {
		t.Fatal(packs)
	}
}

func TestMetadataFilesAndSafety(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "db")
	log := filepath.Join(dir, "log")
	os.MkdirAll(filepath.Join(db, "20261013", "nested"), 0700)
	os.Mkdir(log, 0700)
	os.WriteFile(filepath.Join(db, "20261013", "nested", "part"), []byte("123"), 0600)
	os.WriteFile(filepath.Join(db, "root"), []byte("45"), 0600)
	os.WriteFile(filepath.Join(log, "server.log"), []byte("synthetic log"), 0600)
	os.WriteFile(filepath.Join(log, "ignore.txt"), []byte("ignore"), 0600)
	os.Symlink(filepath.Join(dir, "outside"), filepath.Join(db, "link"))
	os.Symlink(filepath.Join(db, "root"), filepath.Join(log, "linked.log"))
	meta, err := NewFileMetadata(FileMetadataConfig{DatabaseDir: db, LogDir: log})
	if err != nil {
		t.Fatal(err)
	}
	usage, err := meta.Database(context.Background())
	if err != nil || usage.Total != 5 || len(usage.Directories) != 2 || usage.Directories[0].Name != "/20261013/nested" || usage.Directories[1].Name != "/20261013" {
		t.Fatal(usage, err)
	}
	files, exists, err := meta.Logs(context.Background())
	if err != nil || !exists || len(files) != 1 || files[0].Name != "server.log" {
		t.Fatal(files, exists, err)
	}
	for _, name := range []string{"../db/root", "/absolute.log", "linked.log"} {
		_, _, err = meta.Log(context.Background(), name)
		if !errors.Is(err, transport.ErrMalformed) {
			t.Fatal(name, err)
		}
	}
	text, exists, err := meta.Log(context.Background(), "server.log")
	if err != nil || !exists || text != "synthetic log" {
		t.Fatal(exists, err)
	}
	cfg := queryConfig()
	cfg.Metadata = meta
	_, conn, r := startService(t, cfg, &fakeBackend{})
	session := loginSession(t, conn, r)
	send(t, conn, "SERVER_DB_LIST", session, nil)
	packs := readPacks(t, r)
	m := packs[0].(*protocol.MapPack)
	keys := []string{}
	for _, e := range m.Entries {
		keys = append(keys, e.Key)
	}
	if !reflect.DeepEqual(keys, []string{"name", "size", "lastModified", "free", "total"}) {
		t.Fatal(keys)
	}
	send(t, conn, "SERVER_LOG_LIST", session, nil)
	packs = readPacks(t, r)
	if len(packs) != 1 || lookup(packs[0].(*protocol.MapPack), "name").Values[0].Text != "server.log" {
		t.Fatal(packs)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = meta.Database(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestMetadataJavaWireOracle(t *testing.T) {
	if _, err := exec.LookPath("javac"); err != nil {
		t.Skip("JDK unavailable: Java metadata oracle not run")
	}
	source := scouterSource()
	sha, err := exec.Command("git", "-C", source, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(sha)) != "dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5" {
		t.Fatal("Java source SHA drift")
	}
	dir := t.TempDir()
	oracle := filepath.Join(dir, "MetadataOracle.java")
	java := `import scouter.io.DataOutputX; import scouter.lang.pack.MapPack; import scouter.lang.value.BooleanValue; public class MetadataOracle { public static void main(String[] args)throws Exception{DataOutputX d=new DataOutputX(); MapPack a=new MapPack();a.put("result",new BooleanValue(false));d.writeByte(1);d.writePack(a);d.writeByte(1);d.writeValue(new BooleanValue(false));MapPack c=new MapPack();c.newList("configKey").add("net_tcp_client_port");c.put("serverConfig","net_tcp_client_port=6100\n");d.writeByte(1);d.writePack(c);scouter.lang.Account ac=new scouter.lang.Account();ac.id="synthetic";ac.password="synthetic-digest";ac.email="";ac.group="admin";d.writeByte(1);d.writeValue(new scouter.lang.value.BlobValue(ac.toBytes()));System.out.write(d.toByteArray());} }`
	if err = os.WriteFile(oracle, []byte(java), 0600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("javac", "--release", "11", "-encoding", "UTF-8", "-sourcepath", filepath.Join(source, "scouter.common/src/main/java"), "-d", dir, oracle).CombinedOutput()
	if err != nil {
		t.Fatalf("javac: %v %s", err, out)
	}
	want, err := exec.Command("java", "-cp", dir, "MetadataOracle").Output()
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	for _, p := range []protocol.Pack{newMap(entry("result", boolean(false))), newMap(entry("configKey", list([]protocol.Value{stringValue("net_tcp_client_port")})), entry("serverConfig", stringValue("net_tcp_client_port=6100\n")))} {
		encoded, err := protocol.EncodePack(p, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			b, _ := protocol.EncodeValue(boolean(false), protocol.Limits{})
			got = append(got, 1)
			got = append(got, b...)
		}
		got = append(got, 1)
		got = append(got, encoded...)
	}
	var account []byte
	for _, text := range []string{"synthetic", "synthetic-digest", "", "admin"} {
		account = append(account, byte(len(text)))
		account = append(account, []byte(text)...)
	}
	accountValue, err := protocol.EncodeValue(protocol.Value{Type: protocol.ValueBlob, Bytes: account}, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, 1)
	got = append(got, accountValue...)
	if !bytes.Equal(got, want) {
		t.Fatalf("Go %x Java %x", got, want)
	}
}

func TestMetadataRemainingSupportedBodies(t *testing.T) {
	cfg := queryConfig()
	tg, site := "synthetic telegraf", "<counters/>"
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "server.log"), []byte("synthetic log"), 0600); err != nil {
		t.Fatal(err)
	}
	meta, err := NewFileMetadata(FileMetadataConfig{LogDir: dir, Properties: []MetadataProperty{{Key: "port", Value: "6100", Default: "6100", Type: 2}}, TelegrafText: &tg, CountersSiteText: &site})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Metadata = meta
	s, conn, r := startService(t, cfg, &fakeBackend{})
	session := loginSession(t, conn, r)
	send(t, conn, "CHECK_LOGIN", session, newMap(entry("id", stringValue(cfg.Username)), entry("pass", stringValue(s.passwordDigest))))
	aggregationFlag(t, r, transport.HasNext)
	v, err := protocol.ReadValue(r, protocol.Limits{})
	if err != nil || !v.Bool {
		t.Fatal("valid check-login failed", err)
	}
	aggregationFlag(t, r, transport.NoNext)
	send(t, conn, "LIST_ACCOUNT", session, nil)
	aggregationFlag(t, r, transport.HasNext)
	v, err = protocol.ReadValue(r, protocol.Limits{})
	if err != nil || v.Type != protocol.ValueBlob {
		t.Fatal("account response type", err)
	}
	expected := []byte{}
	for _, str := range []string{cfg.Username, s.passwordDigest, "", "admin"} {
		expected = append(expected, byte(len(str)))
		expected = append(expected, []byte(str)...)
	}
	if !bytes.Equal(v.Bytes, expected) {
		t.Fatal("account custom field lengths/order differ")
	}
	aggregationFlag(t, r, transport.NoNext)
	send(t, conn, "GET_LOGIN_LIST", session, nil)
	packs := readPacks(t, r)
	if len(packs) != 1 {
		t.Fatal("login snapshot missing")
	}
	m := packs[0].(*protocol.MapPack)
	keys := []string{}
	for _, e := range m.Entries {
		keys = append(keys, e.Key)
	}
	if !reflect.DeepEqual(keys, []string{"session", "user", "ip", "logintime", "ver", "host"}) || lookup(m, "session").Values[0].Int != session || lookup(m, "user").Values[0].Text != cfg.Username {
		t.Fatal("login snapshot fields differ")
	}
	for _, tc := range []struct{ command, key, want string }{{"GET_CONFIGURE_TELEGRAF", "tgConfigContents", tg}, {"GET_CONFIGURE_COUNTERS_SITE", "contents", site}} {
		send(t, conn, tc.command, session, nil)
		packs = readPacks(t, r)
		if len(packs) != 1 || lookup(packs[0].(*protocol.MapPack), tc.key).Text != tc.want {
			t.Fatal(tc.command, "response differs")
		}
	}
	send(t, conn, "CONFIGURE_VALUE_TYPE_DESC", session, newMap())
	packs = readPacks(t, r)
	if len(packs) != 1 || len(packs[0].(*protocol.MapPack).Entries) != 0 {
		t.Fatal("unexpected enumeration descriptors")
	}
	send(t, conn, "SERVER_LOG_DETAIL", session, newMap(entry("name", stringValue("server.log"))))
	aggregationFlag(t, r, transport.HasNext)
	v, err = protocol.ReadValue(r, protocol.Limits{})
	if err != nil || v.Type != protocol.ValueText || v.Text != "synthetic log" {
		t.Fatal("log detail response differs", err)
	}
	aggregationFlag(t, r, transport.NoNext)
	send(t, conn, "SERVER_LOG_DETAIL", session, newMap(entry("name", stringValue("missing.log"))))
	aggregationFlag(t, r, transport.NoNext)
}

func TestMetadataFileLimitsAndOwnedConfiguration(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.log", "b.log"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("12345"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	b, err := NewFileMetadata(FileMetadataConfig{LogDir: dir, DatabaseDir: dir, MaxEntries: 1, MaxTextBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = b.Logs(context.Background()); !errors.Is(err, transport.ErrLimit) {
		t.Fatal(err)
	}
	if _, err = b.Database(context.Background()); !errors.Is(err, transport.ErrLimit) {
		t.Fatal(err)
	}
	if _, _, err = b.Log(context.Background(), "a.log"); !errors.Is(err, transport.ErrLimit) {
		t.Fatal(err)
	}
	nested := filepath.Join(dir, "nested", "again")
	if err = os.MkdirAll(nested, 0700); err != nil {
		t.Fatal(err)
	}
	b, err = NewFileMetadata(FileMetadataConfig{DatabaseDir: dir, MaxDepth: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Database(context.Background()); !errors.Is(err, transport.ErrLimit) {
		t.Fatal(err)
	}
	props := []MetadataProperty{{Key: "mode", Value: "active", Default: "active", Type: 1, TypeDescription: []protocol.Entry{entry("strings", list([]protocol.Value{stringValue("active")}))}}}
	b, err = NewFileMetadata(FileMetadataConfig{Properties: props})
	if err != nil {
		t.Fatal(err)
	}
	props[0].TypeDescription[0].Value.Values[0].Text = "mutated"
	got, err := b.Configuration(context.Background())
	if err != nil || got[0].TypeDescription[0].Value.Values[0].Text != "active" {
		t.Fatal("borrowed input", err)
	}
	got[0].TypeDescription[0].Value.Values[0].Text = "mutated"
	got, err = b.Configuration(context.Background())
	if err != nil || got[0].TypeDescription[0].Value.Values[0].Text != "active" {
		t.Fatal("borrowed result", err)
	}
	if _, err = NewFileMetadata(FileMetadataConfig{Properties: []MetadataProperty{{Key: "key", Value: "line\ninjected", Type: 1}}}); err == nil {
		t.Fatal("configuration line injection accepted")
	}
	b, err = NewFileMetadata(FileMetadataConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.ConfigurationText(context.Background(), "telegraf"); !errors.Is(err, transport.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err = b.Configuration(context.Background()); !errors.Is(err, transport.ErrUnsupported) {
		t.Fatal(err)
	}
}

type metadataAlertBackend struct {
	*fakeBackend
	wantDate         string
	wantFrom, wantTo int64
}

func (b *metadataAlertBackend) AlertsRealtime(context.Context, AlertQuery) (AlertResult, error) {
	return AlertResult{}, nil
}
func (b *metadataAlertBackend) ScanAlertHistory(ctx context.Context, date string, from, to int64, visit func(*protocol.AlertPack) (bool, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if date != b.wantDate || from != b.wantFrom || to != b.wantTo {
		return errors.New("unexpected debug day boundaries")
	}
	return scanSlice([]*protocol.AlertPack{{}, {}, {}}, nil, visit)
}
func TestMetadataDebugTodayAndConfigureAgentSelector(t *testing.T) {
	cfg := queryConfig()
	from := cfg.Clock().UnixMilli()
	b := &metadataAlertBackend{fakeBackend: &fakeBackend{}, wantDate: "20261013", wantFrom: from, wantTo: from + 86400000 - 1}
	_, conn, r := startService(t, cfg, b)
	session := loginSession(t, conn, r)
	send(t, conn, "DEBUG_SERVER", session, newMap())
	packs := readPacks(t, r)
	if len(packs) != 1 || lookup(packs[0].(*protocol.MapPack), "msg").Text != "processed 3" {
		t.Fatal("debug count differs")
	}
	relay := &agentReadFake{fakeBackend: &fakeBackend{}, replies: []protocol.Pack{newMap(entry("not-final", decimal(1))), newMap(entry("answer", decimal(2)))}}
	_, conn, r = startService(t, cfg, relay)
	session = loginSession(t, conn, r)
	for _, cmd := range []string{"CONFIGURE_DESC", "CONFIGURE_VALUE_TYPE"} {
		send(t, conn, cmd, session, newMap(entry("objHash", decimal(42))))
		packs = readPacks(t, r)
		if len(packs) != 1 || lookup(packs[0].(*protocol.MapPack), "answer").Int != 2 {
			t.Fatal(cmd, "relay final MapPack differs")
		}
	}
	calls := relay.snapshot()
	if len(calls) != 2 || calls[0].hash != 42 || calls[0].command != "CONFIGURE_DESC" || calls[1].command != "CONFIGURE_VALUE_TYPE" {
		t.Fatal("configure relay selector differs")
	}
}

func TestMetadataAlertScriptFileReadOnly(t *testing.T) {
	dir := t.TempDir()
	for name, text := range map[string]string{"cpu.alert": "synthetic rule", "cpu.conf": "threshold=10", "null.alert": "missing-name rule"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(dir, "cpu.conf"), filepath.Join(dir, "linked.alert")); err != nil {
		t.Fatal(err)
	}
	meta, err := NewFileMetadata(FileMetadataConfig{PluginDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"../cpu", "/cpu", "linked"} {
		_, err = meta.AlertScript(context.Background(), name, false)
		if !errors.Is(err, transport.ErrMalformed) {
			t.Fatal(name, err)
		}
	}
	cfg := queryConfig()
	cfg.Metadata = meta
	_, conn, r := startService(t, cfg, &fakeBackend{})
	session := loginSession(t, conn, r)
	for _, tc := range []struct{ command, name, want string }{{"GET_ALERT_SCRIPTING_CONTETNS", "cpu", "synthetic rule"}, {"GET_ALERT_SCRIPTING_CONFIG_CONTETNS", "cpu", "threshold=10"}, {"GET_ALERT_SCRIPTING_CONTETNS", "not-present", ""}} {
		send(t, conn, tc.command, session, newMap(entry("counterName", stringValue(tc.name))))
		packs := readPacks(t, r)
		if len(packs) != 1 || lookup(packs[0].(*protocol.MapPack), "contents").Text != tc.want {
			t.Fatal(tc.command, "contents differ")
		}
	}
	send(t, conn, "GET_ALERT_SCRIPTING_CONTETNS", session, newMap())
	packs := readPacks(t, r)
	if len(packs) != 1 || lookup(packs[0].(*protocol.MapPack), "contents").Text != "missing-name rule" {
		t.Fatal("Java null filename default differs")
	}
	if got, err := os.ReadFile(filepath.Join(dir, "cpu.alert")); err != nil || string(got) != "synthetic rule" {
		t.Fatal("rule file changed", err)
	}
	missing, err := NewFileMetadata(FileMetadataConfig{PluginDir: filepath.Join(dir, "missing-directory")})
	if err != nil {
		t.Fatal(err)
	}
	if text, err := missing.AlertScript(context.Background(), "cpu", false); err != nil || text != "" {
		t.Fatal("missing file source contract", err)
	}
}

func TestMetadataTelegrafConfiguredFileAndFallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "scouter-telegraf.xml")
	sample := "<synthetic-default/>"
	meta, err := NewFileMetadata(FileMetadataConfig{TelegrafFile: path, TelegrafText: &sample})
	if err != nil {
		t.Fatal(err)
	}
	if text, err := meta.ConfigurationText(context.Background(), "telegraf"); err != nil || text != sample {
		t.Fatal("missing file did not use source sample", err)
	}
	for _, contents := range []string{"<synthetic-current/>", ""} {
		if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		text, err := meta.ConfigurationText(context.Background(), "telegraf")
		if err != nil || text != contents {
			t.Fatal("current file content differs", err)
		}
	}
	current := "<synthetic-tcp/>"
	if err = os.WriteFile(path, []byte(current), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := queryConfig()
	cfg.Metadata = meta
	_, conn, r := startService(t, cfg, &fakeBackend{})
	session := loginSession(t, conn, r)
	send(t, conn, "GET_CONFIGURE_TELEGRAF", session, nil)
	packs := readPacks(t, r)
	if len(packs) != 1 || lookup(packs[0].(*protocol.MapPack), "tgConfigContents").Text != current {
		t.Fatal("TCP telegraf file response differs")
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(filepath.Join(dir, "outside.xml"), path); err != nil {
		t.Fatal(err)
	}
	if _, err = meta.ConfigurationText(context.Background(), "telegraf"); !errors.Is(err, transport.ErrMalformed) {
		t.Fatal("symlink accepted or silently used sample", err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte("12345"), 0600); err != nil {
		t.Fatal(err)
	}
	limited, err := NewFileMetadata(FileMetadataConfig{TelegrafFile: path, MaxTextBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = limited.ConfigurationText(context.Background(), "telegraf"); !errors.Is(err, transport.ErrLimit) {
		t.Fatal("file limit not enforced", err)
	}
	missing, err := NewFileMetadata(FileMetadataConfig{TelegrafFile: filepath.Join(dir, "absent", "file.xml"), TelegrafText: &sample})
	if err != nil {
		t.Fatal(err)
	}
	if text, err := missing.ConfigurationText(context.Background(), "telegraf"); err != nil || text != sample {
		t.Fatal("missing parent did not use sample", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = meta.ConfigurationText(ctx, "telegraf"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// TestSingleCredentialAdminPolicy: without account files the single --user is
// the Java default "admin" group, whose account_group.xml policy allows every
// action; only the account-administration actions that need an account store
// stay false. The Client masks SQL bind parameters as "[******]" unless
// LOGIN's policy has AllowSqlParameter=true (ProfileText.java:1057-1069).
func TestSingleCredentialAdminPolicy(t *testing.T) {
	_, conn, r := startService(t, queryConfig(), &fakeBackend{})
	send(t, conn, "LOGIN", 0, mp(entry("id", text("synthetic")), entry("pass", text(encrypted("password")))))
	login := readPacks(t, r)[0].(*protocol.MapPack)
	session := mapValue(login, "session").Int
	check := func(name string, policy protocol.Value) {
		t.Helper()
		if policy.Type != protocol.ValueMap || len(policy.Entries) != 18 {
			t.Fatalf("%s policy %+v", name, policy)
		}
		for _, e := range policy.Entries {
			want := e.Key != "AllowEditGroupPolicy" && e.Key != "AllowAddAccount" && e.Key != "AllowEditAccount"
			if e.Value.Type != protocol.ValueBoolean || e.Value.Bool != want {
				t.Fatalf("%s %s = %+v, want %v", name, e.Key, e.Value, want)
			}
		}
		if v := lookupValue(policy, "AllowSqlParameter"); v.Type != protocol.ValueBoolean || !v.Bool {
			t.Fatalf("%s AllowSqlParameter %+v", name, v)
		}
	}
	if mapValue(login, "type").Text != "admin" {
		t.Fatal(login)
	}
	check("LOGIN", mapValue(login, "policy"))
	send(t, conn, "GET_GROUP_POLICY_ALL", session, nil)
	check("GET_GROUP_POLICY_ALL", mapValue(readPacks(t, r)[0].(*protocol.MapPack), "admin"))
}

func lookupValue(m protocol.Value, key string) protocol.Value {
	for _, e := range m.Entries {
		if e.Key == key {
			return e.Value
		}
	}
	return protocol.Value{}
}
