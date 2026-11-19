package service

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/account"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// accountGoldenDir holds the actual Java AccountService handler bytes
// (scripts/java-management.py --generate, scenario "account").
const accountGoldenDir = "testdata/management/account"

type accountGoldenCase struct {
	Name     string `json:"name"`
	Command  string `json:"command"`
	Request  string `json:"request"`
	Response string `json:"response"`
	ItemKind string `json:"item_kind"`
}

// accountGoldenAllow lists every case whose Go bytes intentionally differ
// from the Java golden. "order" cases must still decode to the same packs
// apart from MapValue entry order; the others assert the Go result below.
var accountGoldenAllow = map[string]string{
	"group-policy-all-initial":      "order: Java writes the MapValue Hashtable order, Go the file order (decision 5)",
	"group-policy-all-after-add":    "order as above; Java keeps the received MapValue in memory (CustomTextPolicy Text \"yes\"), Go answers the reparsed file (getBoolean -> false)",
	"add-account-not-mappack":       "Java swallows the EOFException and answers NoNEXT; Go closes the connection on a malformed body (transport policy)",
	"add-group-policy-not-map":      "Java swallows the ClassCastException and answers NoNEXT; Go closes the connection on a malformed body",
	"edit-account-u1":               "Java defect cc9bda37: ADD_ACCOUNT never reached the file, so editAccount cannot find u1 (false); Go persisted it (true)",
	"list-account-after-edit":       "follows edit-account-u1 (u1 edited) and add-account-no-id (Go stores id \"\")",
	"group-policy-all-after-edit":   "Java defect: loadGroupFile re-reads the unchanged file, so the edit has no effect; Go persisted it",
	"list-account-after-null-email": "Java Account.toBytes NPE truncates the list after HasNEXT; Go normalizes the null email to \"\" (management §9-12)",
}

// readRawResponse reads one response (items up to NoNEXT) and returns its
// bytes; io.EOF means the server closed the connection instead.
func readRawResponse(r *bufio.Reader, kind string) ([]byte, error) {
	var raw bytes.Buffer
	for {
		flag, err := r.ReadByte()
		if err != nil {
			return raw.Bytes(), err
		}
		raw.WriteByte(flag)
		if flag == transport.NoNext {
			return raw.Bytes(), nil
		}
		if flag != transport.HasNext {
			return raw.Bytes(), errors.New("bad flag")
		}
		var item bytes.Buffer
		src := io.TeeReader(r, &item)
		if kind == "pack" {
			_, err = protocol.ReadPack(src, protocol.Limits{})
		} else {
			_, err = protocol.ReadValue(src, protocol.Limits{})
		}
		raw.Write(item.Bytes())
		if err != nil {
			return raw.Bytes(), err
		}
	}
}

// decodeResponse decodes the items of a raw response for semantic checks.
func decodeResponse(t *testing.T, raw []byte, kind string) []any {
	t.Helper()
	r := bytes.NewReader(raw)
	var out []any
	for {
		flag, err := r.ReadByte()
		if err != nil {
			t.Fatal(err)
		}
		if flag == transport.NoNext {
			return out
		}
		if kind == "pack" {
			p, err := protocol.ReadPack(r, protocol.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, p)
		} else {
			v, err := protocol.ReadValue(r, protocol.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, v)
		}
	}
}

// sortedMapPack is a MapPack whose nested MapValue entries are sorted by key.
func sortedEntries(entries []protocol.Entry) map[string]any {
	out := map[string]any{}
	for _, e := range entries {
		if e.Value.Type == protocol.ValueMap {
			out[e.Key] = sortedEntries(e.Value.Entries)
		} else {
			out[e.Key] = e.Value
		}
	}
	return out
}

func accountsInList(t *testing.T, raw []byte) []account.Account {
	t.Helper()
	var out []account.Account
	for _, item := range decodeResponse(t, raw, "value") {
		b := item.(protocol.Value).Bytes
		var fields []string
		for len(b) > 0 {
			n := int(b[0])
			fields = append(fields, string(b[1:1+n]))
			b = b[1+n:]
		}
		if len(fields) != 4 {
			t.Fatalf("Account.toBytes fields %q", fields)
		}
		out = append(out, account.Account{ID: fields[0], Password: fields[1], Email: fields[2], Group: fields[3]})
	}
	return out
}

func TestAccountJavaGoldenScenario(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(accountGoldenDir, "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Cases []accountGoldenCase `json:"cases"`
		Facts struct {
			Digest1 string `json:"synthetic_password_digest_1"`
			Digest2 string `json:"synthetic_password_digest_2"`
		} `json:"facts"`
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	m, err := account.Open(account.Config{Dir: dir, Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	// The Java resources and the embedded defaults are the same bytes.
	for name, file := range map[string]string{account.AccountFile: "account.default.xml", account.GroupFile: "account_group.default.xml"} {
		want, _ := os.ReadFile(filepath.Join(accountGoldenDir, file))
		got, _ := os.ReadFile(filepath.Join(dir, name))
		if !bytes.Equal(got, want) {
			t.Fatalf("%s differs from the Java resource", name)
		}
	}
	admin, _ := m.Account("admin")
	_, conn, r := startService(t, Config{Accounts: m, ServerID: "1", Location: time.UTC}, &fakeBackend{})
	addr := conn.RemoteAddr()
	send(t, conn, "LOGIN", 0, mp(entry("id", text("admin")), entry("pass", text(admin.Password))))
	session := mapValue(readPacks(t, r)[0].(*protocol.MapPack), "session").Int
	if session == 0 {
		t.Fatal("login")
	}
	responses := map[string][]byte{}
	for _, c := range golden.Cases {
		body, err := os.ReadFile(filepath.Join(accountGoldenDir, c.Request))
		if err != nil {
			t.Fatal(err)
		}
		want, err := os.ReadFile(filepath.Join(accountGoldenDir, c.Response))
		if err != nil {
			t.Fatal(err)
		}
		e := protocol.NewEncoder(protocol.Limits{})
		e.Text(c.Command)
		e.Int64(session)
		head, _ := e.Bytes()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write(append(head, body...)); err != nil {
			t.Fatal(c.Name, err)
		}
		got, err := readRawResponse(r, c.ItemKind)
		closed := err != nil
		if closed {
			if !errors.Is(err, io.EOF) && !isReset(err) {
				t.Fatalf("%s: %v", c.Name, err)
			}
			conn, r = dialClient(t, addr)
		}
		responses[c.Name] = got
		reason, allowed := accountGoldenAllow[c.Name]
		if !allowed {
			if closed || !bytes.Equal(got, want) {
				t.Errorf("%s: Go %x, Java %x", c.Name, got, want)
			}
			continue
		}
		t.Logf("%s: intended difference: %s", c.Name, reason)
		switch c.Name {
		case "group-policy-all-initial", "group-policy-all-after-add":
			g, j := decodeResponse(t, got, "pack"), decodeResponse(t, want, "pack")
			if len(g) != 1 || len(j) != 1 {
				t.Fatal(c.Name, g, j)
			}
			gm, jm := g[0].(*protocol.MapPack), j[0].(*protocol.MapPack)
			if entryKeys(gm.Entries) != entryKeys(jm.Entries) {
				t.Errorf("%s group order %s vs %s", c.Name, entryKeys(gm.Entries), entryKeys(jm.Entries))
			}
			gs, js := sortedEntries(gm.Entries), sortedEntries(jm.Entries)
			if c.Name == "group-policy-all-after-add" {
				group := js["synthetic-group"].(map[string]any)
				if v := group["CustomTextPolicy"].(protocol.Value); v.Type != protocol.ValueText || v.Text != "yes" {
					t.Fatalf("golden changed: %+v", v)
				}
				group["CustomTextPolicy"] = boolean(false)
			}
			if !jsonEqual(gs, js) {
				t.Errorf("%s values differ", c.Name)
			}
		case "add-account-not-mappack", "add-group-policy-not-map":
			if !closed || !bytes.Equal(want, []byte{transport.NoNext}) {
				t.Errorf("%s: Go closed=%v %x", c.Name, closed, got)
			}
		case "edit-account-u1":
			if closed || !bytes.Equal(got, bytes.Replace(want, []byte("result\x0a\x00"), []byte("result\x0a\x01"), 1)) {
				t.Errorf("%s: Go %x", c.Name, got)
			}
		case "list-account-after-edit":
			list := accountsInList(t, got)
			javaList := accountsInList(t, want)
			// Java: admin, guest, u1, unicode. Go also has the edited u1 and the "" id.
			if len(list) != len(javaList)+1 || list[len(list)-1].ID != "" {
				t.Errorf("%s: %+v vs Java %+v", c.Name, list, javaList)
			}
			for i, a := range javaList {
				if a.ID == "u1" {
					if list[i].ID != "u1" || list[i].Password == a.Password {
						t.Errorf("u1 not edited: %+v", list[i])
					}
				} else if list[i] != a {
					t.Errorf("%s[%d]: %+v vs %+v", c.Name, i, list[i], a)
				}
			}
		case "group-policy-all-after-edit":
			g := decodeResponse(t, got, "pack")[0].(*protocol.MapPack)
			guest := sortedEntries(g.Entries)["guest"].(map[string]any)
			editBody, err := os.ReadFile(filepath.Join(accountGoldenDir, "edit-group-policy-guest.request.bin"))
			if err != nil {
				t.Fatal(err)
			}
			edit := decodeRequestEdits(t, editBody)
			if len(edit) == 0 {
				t.Fatal("no edit decoded")
			}
			for key, v := range edit {
				if gv, ok := guest[key].(protocol.Value); !ok || gv.Bool != v {
					t.Errorf("guest %s = %v, edit %v", key, guest[key], v)
				}
			}
		case "list-account-after-null-email":
			// Java: three records, then HasNEXT and the NPE (no item).
			if !bytes.HasSuffix(want, []byte{transport.HasNext, transport.NoNext}) {
				t.Fatalf("golden changed: %x", want[len(want)-2:])
			}
			list := accountsInList(t, got)
			noEmail, err := os.ReadFile(filepath.Join(accountGoldenDir, "add-account-no-email.request.bin"))
			if err != nil {
				t.Fatal(err)
			}
			p, err := protocol.ReadPack(bytes.NewReader(noEmail), protocol.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			last := list[len(list)-1]
			if len(list) != len(m.Accounts()) || last.ID != mapValue(p.(*protocol.MapPack), "id").Text || last.Email != "" {
				t.Errorf("%s: %+v", c.Name, list)
			}
		default:
			t.Fatalf("allowlisted case %s has no Go assertion", c.Name)
		}
	}
	for name := range accountGoldenAllow {
		if _, ran := responses[name]; !ran {
			t.Errorf("allowlist entry %s matches no golden case", name)
		}
	}
	// SCOUTER_ACCOUNT_CONF_OUT keeps a copy of the written files for
	// scripts/java-management.py --verify-xml (original Java parser).
	if out := os.Getenv("SCOUTER_ACCOUNT_CONF_OUT"); out != "" {
		for _, name := range []string{account.AccountFile, account.GroupFile} {
			b, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(out, name), b, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	// The written files load back with the same accounts the server lists.
	m2, err := account.Open(account.Config{Dir: dir, Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	if len(m2.Accounts()) != len(m.Accounts()) {
		t.Fatal("persisted accounts differ")
	}
}

func decodeRequestEdits(t *testing.T, body []byte) map[string]bool {
	t.Helper()
	p, err := protocol.ReadPack(bytes.NewReader(body), protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, e := range p.(*protocol.MapPack).Entries {
		for _, item := range e.Value.Entries {
			out[item.Key] = item.Value.Bool
		}
	}
	return out
}

func jsonEqual(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}

func isReset(err error) bool {
	var op *net.OpError
	return errors.As(err, &op)
}

func TestAccountToBytesGolden(t *testing.T) {
	for _, c := range []struct {
		file string
		a    account.Account
	}{
		{"account-tobytes-empty.bin", account.Account{}},
	} {
		want, err := os.ReadFile(filepath.Join(accountGoldenDir, c.file))
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.a.Bytes()
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: %x vs %x %v", c.file, got, want, err)
		}
	}
	// Every non-wrapping golden record round-trips through the Go encoder.
	for _, file := range []string{"account-tobytes-basic.bin", "account-tobytes-unicode.bin", "account-tobytes-empty.bin"} {
		want, _ := os.ReadFile(filepath.Join(accountGoldenDir, file))
		var fields []string
		for b := want; len(b) > 0; {
			n := int(b[0])
			fields = append(fields, string(b[1:1+n]))
			b = b[1+n:]
		}
		got, err := account.Account{ID: fields[0], Password: fields[1], Email: fields[2], Group: fields[3]}.Bytes()
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: %x vs %x", file, got, want)
		}
	}
	// The 260-byte field wraps its length byte in Java; Go refuses to encode it.
	wrapped, _ := os.ReadFile(filepath.Join(accountGoldenDir, "account-tobytes-260.bin"))
	if wrapped[0] != byte(260&0xff) {
		t.Fatalf("golden is not the wrapped record: %x", wrapped[:1])
	}
	if _, err := (account.Account{ID: string(bytes.Repeat([]byte("x"), 260))}).Bytes(); !errors.Is(err, account.ErrInvalid) {
		t.Fatal(err)
	}
}
