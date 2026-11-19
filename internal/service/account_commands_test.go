package service

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/account"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

type accountClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *accountClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *accountClock) Add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// accountService starts a Client listener backed by a real account.Manager
// over a temporary conf directory holding the default files plus one
// synthetic guest account.
func accountService(t *testing.T) (*Service, *account.Manager, string, net.Conn, *bufio.Reader, *accountClock) {
	t.Helper()
	dir := t.TempDir()
	m, err := account.Open(account.Config{Dir: dir, Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := m.Add(account.Account{ID: "synthetic", Password: encrypted("synthetic-pw"), Email: "s@example.invalid", Group: "guest"}); !ok || err != nil {
		t.Fatal(ok, err)
	}
	clock := &accountClock{now: time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)}
	s, conn, r := startService(t, Config{Accounts: m, ServerID: "7", Location: time.UTC, Clock: clock.Now}, &fakeBackend{})
	return s, m, dir, conn, r, clock
}

func accountLoginSession(t *testing.T, conn net.Conn, r *bufio.Reader, extra ...protocol.Entry) *protocol.MapPack {
	t.Helper()
	send(t, conn, "LOGIN", 0, mp(append([]protocol.Entry{entry("id", text("synthetic")), entry("pass", text(encrypted("synthetic-pw")))}, extra...)...))
	return readPacks(t, r)[0].(*protocol.MapPack)
}

func readValues(t *testing.T, r *bufio.Reader) []protocol.Value {
	t.Helper()
	var values []protocol.Value
	for {
		flag, err := r.ReadByte()
		if err != nil {
			t.Fatalf("response flag: %v", err)
		}
		if flag == transport.NoNext {
			return values
		}
		if flag != transport.HasNext {
			t.Fatalf("response flag %x", flag)
		}
		v, err := protocol.ReadValue(r, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, v)
	}
}

func entryKeys(entries []protocol.Entry) string {
	keys := make([]string, len(entries))
	for i, e := range entries {
		keys[i] = e.Key
	}
	return strings.Join(keys, ",")
}

// filePolicyOrder is the decided wire order: policies as in the file.
func filePolicyOrder(policy []account.Policy) []protocol.Entry {
	entries := make([]protocol.Entry, len(policy))
	for i, p := range policy {
		entries[i] = entry(p.Name, boolean(p.Allow))
	}
	return entries
}

func TestAccountLoginResponseAndLocalID(t *testing.T) {
	_, m, _, conn, r, _ := accountService(t)
	fail := mp(entry("id", text("synthetic")), entry("pass", text("wrong")))
	send(t, conn, "LOGIN", 0, fail)
	bad := readPacks(t, r)[0].(*protocol.MapPack)
	if entryKeys(bad.Entries) != "id,session,error" || mapValue(bad, "session").Int != 0 {
		t.Fatalf("%#v", bad)
	}
	ok := accountLoginSession(t, conn, r)
	if got := entryKeys(ok.Entries); got != "id,session,time,server_id,type,version,client_version,email,timezone,policy,menu,so_time_out,ext_link_name,ext_link_url_pattern" {
		t.Fatal(got)
	}
	if mapValue(ok, "type").Text != "guest" || mapValue(ok, "email").Text != "s@example.invalid" || mapValue(ok, "server_id").Text != "7" || mapValue(ok, "pass").Type != protocol.ValueNull {
		t.Fatalf("%#v", ok)
	}
	guest, _ := m.GroupPolicy("guest")
	want := filePolicyOrder(guest)
	policy := mapValue(ok, "policy")
	if policy.Type != protocol.ValueMap || entryKeys(policy.Entries) != entryKeys(want) {
		t.Fatal(entryKeys(policy.Entries))
	}
	for i, e := range policy.Entries {
		if e.Value.Type != protocol.ValueBoolean || e.Value.Bool != want[i].Value.Bool {
			t.Fatal(e)
		}
	}
	// __local from loopback: no password, group "", no email/policy entries.
	send(t, conn, "LOGIN", 0, mp(entry("id", text("__local")), entry("pass", text(""))))
	local := readPacks(t, r)[0].(*protocol.MapPack)
	if mapValue(local, "session").Int == 0 || entryKeys(local.Entries) != "id,session,time,server_id,type,version,client_version,timezone,menu,so_time_out,ext_link_name,ext_link_url_pattern" || mapValue(local, "type").Text != "" {
		t.Fatalf("%#v", local)
	}
	// An account whose group has no policy omits it; CHECK_LOGIN does not
	// take the __local shortcut.
	if ok, err := m.Add(account.Account{ID: "nogroup", Password: "pw", Group: "nobody"}); !ok || err != nil {
		t.Fatal(ok, err)
	}
	send(t, conn, "LOGIN", 0, mp(entry("id", text("nogroup")), entry("pass", text("pw"))))
	ng := readPacks(t, r)[0].(*protocol.MapPack)
	if mapValue(ng, "policy").Type != protocol.ValueNull || mapValue(ng, "email").Text != "" || mapValue(ng, "type").Text != "nobody" {
		t.Fatalf("%#v", ng)
	}
	session := mapValue(ok, "session").Int
	send(t, conn, "CHECK_LOGIN", session, mp(entry("id", text("__local")), entry("pass", text(""))))
	send(t, conn, "CHECK_LOGIN", session, mp(entry("id", text("synthetic")), entry("pass", text(encrypted("synthetic-pw")))))
	send(t, conn, "CHECK_LOGIN", session, mp(entry("id", text("synthetic")), entry("pass", text("wrong"))))
	for i, want := range []bool{false, true, false} {
		v := readValues(t, r)
		if len(v) != 1 || v[0].Type != protocol.ValueBoolean || v[0].Bool != want {
			t.Fatal(i, v)
		}
	}
	for _, peer := range []net.Addr{&net.TCPAddr{IP: net.IPv4(10, 0, 0, 1)}, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 2)}, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}, nil} {
		if loopbackPeer(peer) {
			t.Fatal(peer)
		}
	}
	if !loopbackPeer(&net.TCPAddr{IP: net.IPv6loopback}) || !loopbackPeer(&net.TCPAddr{IP: net.ParseIP("::ffff:127.0.0.1")}) {
		t.Fatal("loopback forms")
	}
}

// Go keeps a session while a live connection holds it and drops it a grace
// after its last connection, internal or not (service CONTRACT §1). Logging in
// again on the same connection releases the previous session.
func TestAccountInternalSessionTTL(t *testing.T) {
	s, _, _, conn, r, clock := accountService(t)
	normal := mapValue(accountLoginSession(t, conn, r), "session").Int
	internal := mapValue(accountLoginSession(t, conn, r, entry("internal", text("TRUE"))), "session").Int
	held := mapValue(accountLoginSession(t, conn, r, entry("internal", text("TRUE"))), "session").Int
	waitSessionConns(t, s, normal, 0)
	waitSessionConns(t, s, internal, 0)
	waitSessionConns(t, s, held, 1)
	clock.Add(4 * time.Hour)
	ctx := context.Background()
	if ok, _ := s.Authorize(ctx, transport.ClientRequest{Session: normal}); ok {
		t.Fatal("released normal session outlived the grace")
	}
	if ok, _ := s.Authorize(ctx, transport.ClientRequest{Session: internal}); ok {
		t.Fatal("released internal session outlived the grace")
	}
	clock.Add(8 * 24 * time.Hour)
	if ok, _ := s.Authorize(ctx, transport.ClientRequest{Session: held}); !ok {
		t.Fatal("internal session held by a live connection expired")
	}
	if !s.validSession(held) {
		t.Fatal("CHECK_SESSION of a held session")
	}
}

func TestAccountReadCommandsWire(t *testing.T) {
	_, m, _, conn, r, _ := accountService(t)
	session := mapValue(accountLoginSession(t, conn, r), "session").Int
	// Body-less commands back to back: no Pack is consumed from the next one.
	send(t, conn, "LIST_ACCOUNT", session, nil)
	send(t, conn, "LIST_ACCOUNT_GROUP", session, nil)
	send(t, conn, "GET_GROUP_POLICY_ALL", session, nil)
	blobs := readValues(t, r)
	accounts := m.Accounts()
	if len(blobs) != 3 || len(accounts) != 3 {
		t.Fatal(len(blobs))
	}
	for i, b := range blobs {
		want, _ := accounts[i].Bytes()
		if b.Type != protocol.ValueBlob || !bytes.Equal(b.Bytes, want) {
			t.Fatal(i, b)
		}
	}
	groups := readPacks(t, r)[0].(*protocol.MapPack)
	if list := mapValue(groups, "group_list"); entryKeys(groups.Entries) != "group_list" || len(list.Values) != 2 || list.Values[0].Text != "admin" || list.Values[1].Text != "guest" {
		t.Fatalf("%#v", groups)
	}
	all := readPacks(t, r)[0].(*protocol.MapPack)
	admin, _ := m.GroupPolicy("admin")
	if entryKeys(all.Entries) != "admin,guest" || entryKeys(mapValue(all, "admin").Entries) != entryKeys(filePolicyOrder(admin)) {
		t.Fatalf("%#v", all)
	}
	send(t, conn, "CHECK_ACCOUNT_ID", session, mp(entry("id", text("synthetic"))))
	send(t, conn, "CHECK_ACCOUNT_ID", session, mp(entry("id", text("fresh"))))
	for _, want := range []bool{false, true} {
		p := readPacks(t, r)
		if len(p) != 1 || entryKeys(p[0].(*protocol.MapPack).Entries) != "result" || mapValue(p[0].(*protocol.MapPack), "result").Bool != want {
			t.Fatal(p)
		}
	}
}

func TestAccountMutationsWire(t *testing.T) {
	_, m, dir, conn, r, _ := accountService(t)
	session := mapValue(accountLoginSession(t, conn, r), "session").Int
	add := mp(entry("id", text("new")), entry("pass", text(encrypted("new-pw"))), entry("email", text("n@example.invalid")), entry("group", text("admin")))
	send(t, conn, "ADD_ACCOUNT", session, add)
	send(t, conn, "ADD_ACCOUNT", session, add)
	send(t, conn, "EDIT_ACCOUNT", session, mp(entry("id", text("new")), entry("pass", text("p2")), entry("group", text("guest"))))
	send(t, conn, "EDIT_ACCOUNT", session, mp(entry("id", text("missing")), entry("pass", text("p"))))
	send(t, conn, "ADD_ACCOUNT", session, mp(entry("id", text("big")), entry("email", text(strings.Repeat("e", 256)))))
	for i, want := range []bool{true, false, true, false, false} {
		p := readPacks(t, r)
		if len(p) != 1 || entryKeys(p[0].(*protocol.MapPack).Entries) != "result" || mapValue(p[0].(*protocol.MapPack), "result").Bool != want {
			t.Fatal(i, p)
		}
	}
	if a, _ := m.Account("new"); a != (account.Account{ID: "new", Password: "p2", Group: "guest"}) {
		t.Fatal(a)
	}
	if !m.AvailableID("big") {
		t.Fatal("oversized field stored")
	}
	file, _ := os.ReadFile(filepath.Join(dir, account.AccountFile))
	if !bytes.Contains(file, []byte(`<Account group="guest" id="new" pass="p2">`)) {
		t.Fatalf("%s", file)
	}
	// ADD_ACCOUNT_GROUP: policy elements follow the received order.
	wire := []protocol.Entry{entry("AllowA", boolean(true)), entry("AllowB", boolean(false)), entry("AllowC", number(1)), entry("AllowDefineObjectType", boolean(true))}
	send(t, conn, "ADD_ACCOUNT_GROUP", session, mp(entry("name", text("ops")), entry("policy", protocol.Value{Type: protocol.ValueMap, Entries: wire})))
	send(t, conn, "ADD_ACCOUNT_GROUP", session, mp(entry("policy", protocol.Value{Type: protocol.ValueMap})))
	send(t, conn, "ADD_ACCOUNT_GROUP", session, mp(entry("name", text("x"))))
	send(t, conn, "EDIT_GROUP_POLICY", session, mp(
		entry("ops", protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("AllowB", boolean(true)), entry("AllowNew", text("false")), entry("AllowNull", protocol.Value{Type: protocol.ValueNull})}}),
		entry("unknown", number(5)),
	))
	send(t, conn, "EDIT_GROUP_POLICY", session, mp(entry("guest", text("not a map"))))
	send(t, conn, "EDIT_GROUP_POLICY", session, mp(entry("guest", protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("AllowX", protocol.Value{Type: protocol.ValueList})}})))
	for i, want := range []bool{true, false, false, true, false, false} {
		v := readValues(t, r)
		if len(v) != 1 || v[0].Type != protocol.ValueBoolean || v[0].Bool != want {
			t.Fatal(i, v)
		}
	}
	groupFile, _ := os.ReadFile(filepath.Join(dir, account.GroupFile))
	var order []string
	for _, e := range wire {
		order = append(order, e.Key)
	}
	opsGroup := groupFile[bytes.Index(groupFile, []byte(`<Group name="ops">`)):]
	last := -1
	for _, key := range order {
		i := bytes.Index(opsGroup, []byte("<"+key+">"))
		if i < 0 || i < last {
			t.Fatalf("element order %v in %s", order, opsGroup)
		}
		last = i
	}
	ops, ok := m.GroupPolicy("ops")
	want := map[string]bool{"AllowA": true, "AllowB": true, "AllowC": false, "AllowDefineObjectType": true, "AllowNew": false}
	if !ok || len(ops) != len(want) {
		t.Fatal(ops)
	}
	for _, p := range ops {
		if w, ok := want[p.Name]; !ok || w != p.Allow {
			t.Fatal(ops)
		}
	}
	if !bytes.Contains(groupFile, []byte("<AllowNull/>")) {
		t.Fatal("NullValue writes an empty element")
	}
	send(t, conn, "GET_GROUP_POLICY_ALL", session, nil)
	if all := readPacks(t, r)[0].(*protocol.MapPack); entryKeys(all.Entries) != "admin,guest,ops" {
		t.Fatal(entryKeys(all.Entries))
	}
	// A policy that is not a MapValue is a malformed request (no answer).
	send(t, conn, "ADD_ACCOUNT_GROUP", session, mp(entry("name", text("bad")), entry("policy", text("x"))))
	accountExpectClosed(t, r)
}

func accountExpectClosed(t *testing.T, r *bufio.Reader) {
	t.Helper()
	b, err := r.ReadByte()
	if err == nil {
		t.Fatalf("connection still answering: %x", b)
	}
	if err != io.EOF && !strings.Contains(err.Error(), "reset") {
		t.Fatal(err)
	}
}

func TestAccountRegistryAndFallback(t *testing.T) {
	m, err := account.Open(account.Config{Dir: t.TempDir(), Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	with := &Service{config: Config{Accounts: m}, backend: &fakeBackend{}}
	without := &Service{backend: &fakeBackend{}}
	has := func(s *Service, command string) bool {
		for _, c := range s.SupportedCommands() {
			if c == command {
				return true
			}
		}
		return false
	}
	for _, command := range AccountCommands() {
		if !has(with, command) {
			t.Fatal("missing", command)
		}
	}
	for _, command := range accountMutations {
		if has(without, command) {
			t.Fatal("mutation advertised without a store", command)
		}
	}
	seen := map[string]bool{}
	for _, c := range with.SupportedCommands() {
		if seen[c] {
			t.Fatal("duplicate", c)
		}
		seen[c] = true
	}
	if _, err := New(Config{Accounts: m}, &fakeBackend{}); err != nil {
		t.Fatal("an account store replaces explicit credentials", err)
	}
	if _, err := New(Config{}, &fakeBackend{}); err == nil {
		t.Fatal("credentials or store required")
	}
	// Without a store the mutations fail explicitly before the body is read.
	_, conn, r := startService(t, queryConfig(), &fakeBackend{})
	session := loginSession(t, conn, r)
	send(t, conn, "LIST_ACCOUNT_GROUP", session, nil)
	if p := readPacks(t, r); len(p) != 1 {
		t.Fatal("fallback metadata answer", p)
	}
	send(t, conn, "ADD_ACCOUNT", session, mp(entry("id", text("x"))))
	accountExpectClosed(t, r)
}

func TestAccountListRejectsOversizedFileField(t *testing.T) {
	dir := t.TempDir()
	long := strings.Repeat("x", 256)
	os.WriteFile(filepath.Join(dir, account.AccountFile), []byte(`<Accounts><Account id="a" pass="p" group="g"><Email>e</Email></Account><Account id="b" pass="`+long+`" group="g"><Email/></Account></Accounts>`), 0o600)
	m, err := account.Open(account.Config{Dir: dir, Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := queryConfig()
	cfg.Accounts = m
	_, conn, r := startService(t, cfg, &fakeBackend{})
	send(t, conn, "LOGIN", 0, mp(entry("id", text("a")), entry("pass", text("p"))))
	session := mapValue(readPacks(t, r)[0].(*protocol.MapPack), "session").Int
	if session == 0 {
		t.Fatal("login")
	}
	// No partial list: the whole command fails before the first item.
	send(t, conn, "LIST_ACCOUNT", session, nil)
	accountExpectClosed(t, r)
}
