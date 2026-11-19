package account

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (l *logSink) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logSink) count(substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

func openTest(t *testing.T, dir string) (*Manager, *logSink) {
	t.Helper()
	sink := &logSink{}
	m, err := Open(Config{Dir: dir, Logf: sink.logf})
	if err != nil {
		t.Fatal(err)
	}
	return m, sink
}

func readTestFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// expectedAfter applies the same DOM step to the old bytes, i.e. the bytes
// the JDK 8 oracle test pins for these operations.
func expectedAfter(t *testing.T, old []byte, change func(*xmlDocument) error) []byte {
	t.Helper()
	doc, err := parseXML(old)
	if err != nil {
		t.Fatal(err)
	}
	if err := change(doc); err != nil {
		t.Fatal(err)
	}
	return doc.marshal()
}

func TestOpenCreatesDefaultsAndLoadsFileOrder(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "conf")
	m, _ := openTest(t, dir)
	for name, want := range map[string][]byte{AccountFile: defaultAccountXML, GroupFile: defaultGroupXML} {
		path := filepath.Join(dir, name)
		if got := readTestFile(t, path); !bytes.Equal(got, want) {
			t.Fatalf("%s not the default resource", name)
		}
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode %v %v", name, info.Mode(), err)
		}
	}
	accounts := m.Accounts()
	if len(accounts) != 2 || accounts[0].ID != "admin" || accounts[0].Group != "admin" || accounts[0].Email != "admin@scouter.com" || accounts[1].ID != "guest" || len(accounts[1].Password) != 64 {
		t.Fatalf("%+v", accounts)
	}
	if g := m.Groups(); len(g) != 2 || g[0] != "admin" || g[1] != "guest" {
		t.Fatal(g)
	}
	admin, ok := m.GroupPolicy("admin")
	if !ok || len(admin) != 18 || admin[0] != (Policy{"AllowEditGroupPolicy", true}) || admin[17].Name != "AllowDefineObjectType" {
		t.Fatal(admin)
	}
	guest, _ := m.GroupPolicy("guest")
	allowed := []string{}
	for _, p := range guest {
		if p.Allow {
			allowed = append(allowed, p.Name)
		}
	}
	if len(guest) != 18 || strings.Join(allowed, ",") != "AllowThreadDump,AllowSystemGC" {
		t.Fatal(guest)
	}
	if _, ok := m.GroupPolicy("missing"); ok || m.AvailableID("admin") || !m.AvailableID("synthetic") {
		t.Fatal("lookup")
	}
	// Snapshots are copies.
	admin[0].Allow = false
	accounts[0].ID = "changed"
	if p, _ := m.GroupPolicy("admin"); !p[0].Allow || m.Accounts()[0].ID != "admin" {
		t.Fatal("snapshot aliasing")
	}
}

func TestAddEditPersistAndAuthorize(t *testing.T) {
	dir := t.TempDir()
	m, _ := openTest(t, dir)
	path := filepath.Join(dir, AccountFile)
	ctx := context.Background()
	synthetic := Account{ID: "synthetic", Password: "synthetic-digest", Email: "s@example.invalid", Group: "guest"}
	want := expectedAfter(t, readTestFile(t, path), func(d *xmlDocument) error { return addAccountDoc(d, synthetic) })
	if ok, err := m.Add(synthetic); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if got := readTestFile(t, path); !bytes.Equal(got, want) {
		t.Fatalf("written %q want %q", got, want)
	}
	if ok, err := m.Add(Account{ID: "synthetic", Password: "other"}); ok || err != nil {
		t.Fatal("duplicate", ok, err)
	}
	if a, ok, err := m.Authorize(ctx, "synthetic", "synthetic-digest"); !ok || err != nil || a != synthetic {
		t.Fatal(a, ok, err)
	}
	if _, ok, err := m.Authorize(ctx, "synthetic", "wrong"); ok || err != nil {
		t.Fatal("wrong password", ok, err)
	}
	if _, ok, _ := m.Authorize(ctx, "unknown", ""); ok {
		t.Fatal("unknown id")
	}
	edited := Account{ID: "synthetic", Password: "digest-2", Email: "", Group: "admin"}
	want = expectedAfter(t, readTestFile(t, path), func(d *xmlDocument) error { return editAccountDoc(d, edited) })
	if ok, err := m.Edit(edited); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if got := readTestFile(t, path); !bytes.Equal(got, want) || !bytes.Contains(got, []byte("<Email/>")) {
		t.Fatalf("edit wrote %q", got)
	}
	if a, _ := m.Account("synthetic"); a != edited {
		t.Fatal(a)
	}
	if ok, err := m.Edit(Account{ID: "missing"}); ok || err != nil {
		t.Fatal("missing", ok, err)
	}
	// A restart loads what was written.
	m2, _ := openTest(t, dir)
	if got := m2.Accounts(); len(got) != 3 || got[2] != edited {
		t.Fatal(got)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := m.Authorize(cancelled, "synthetic", "digest-2"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestRejectedValuesLeaveFileUnchanged(t *testing.T) {
	dir := t.TempDir()
	m, sink := openTest(t, dir)
	before := readTestFile(t, filepath.Join(dir, AccountFile))
	groupsBefore := readTestFile(t, filepath.Join(dir, GroupFile))
	long := strings.Repeat("x", MaxFieldBytes+1)
	for _, a := range []Account{{ID: long}, {ID: "a", Password: long}, {ID: "a", Email: long}, {ID: "a", Group: long}, {ID: "a", Email: "bad\x01"}, {ID: "bad\xff"}} {
		if ok, err := m.Add(a); ok || !errors.Is(err, ErrInvalid) {
			t.Fatalf("%q: %v %v", a.ID, ok, err)
		}
	}
	if ok, err := m.Edit(Account{ID: "admin", Email: strings.Repeat("é", 128)}); ok || !errors.Is(err, ErrInvalid) {
		t.Fatal("256-byte UTF-8 email", ok, err)
	}
	if ok, err := m.Add(Account{ID: strings.Repeat("é", 127) + "x"}); !ok || err != nil {
		t.Fatal("255 bytes is allowed", ok, err)
	}
	if ok, err := m.AddGroup("g", []Policy{{Name: "1bad", Allow: true}}); ok || !errors.Is(err, ErrInvalid) {
		t.Fatal(ok, err)
	}
	if ok, err := m.EditGroupPolicy([]GroupPolicyEdit{{Group: "admin", Policy: []PolicyText{{Name: "a b", Text: "true"}}}}); ok || !errors.Is(err, ErrInvalid) {
		t.Fatal(ok, err)
	}
	if ok, err := m.EditGroupPolicy([]GroupPolicyEdit{{Group: "admin", Policy: []PolicyText{{Name: "AllowX", Text: "\x02"}}}}); ok || !errors.Is(err, ErrInvalid) {
		t.Fatal(ok, err)
	}
	if !bytes.Equal(readTestFile(t, filepath.Join(dir, GroupFile)), groupsBefore) {
		t.Fatal("group file changed")
	}
	if after := readTestFile(t, filepath.Join(dir, AccountFile)); bytes.Equal(after, before) || len(m.Accounts()) != 3 {
		t.Fatal("only the valid 255-byte account is added")
	}
	if sink.count("account:") == 0 {
		t.Fatal("failures are logged")
	}
}

func TestGroupAddEditAndJavaParseRules(t *testing.T) {
	dir := t.TempDir()
	m, sink := openTest(t, dir)
	path := filepath.Join(dir, GroupFile)
	policy := []Policy{{"AllowB", false}, {"AllowA", true}}
	want := expectedAfter(t, readTestFile(t, path), func(d *xmlDocument) error { return addGroupDoc(d, "ops", policy) })
	if ok, err := m.AddGroup("ops", policy); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if !bytes.Equal(readTestFile(t, path), want) {
		t.Fatal("add group bytes")
	}
	if p, ok := m.GroupPolicy("ops"); !ok || len(p) != 2 || p[0] != policy[0] || p[1] != policy[1] {
		t.Fatal(p)
	}
	edits := []GroupPolicyEdit{
		{Group: "admin", Policy: []PolicyText{{"AllowHeapDump", "false"}, {"AllowNew", "TRUE"}}},
		{Group: "nogroup", Policy: []PolicyText{{"AllowX", "true"}}},
		{Group: "ops", Policy: []PolicyText{{"AllowA", "maybe"}}},
	}
	if ok, err := m.EditGroupPolicy(edits); !ok || err != nil {
		t.Fatal(ok, err)
	}
	admin, _ := m.GroupPolicy("admin")
	if admin[1] != (Policy{"AllowHeapDump", false}) || admin[len(admin)-1] != (Policy{"AllowNew", true}) {
		t.Fatal(admin)
	}
	// "maybe" is written but String.toBoolean rejects it on load: skipped and logged.
	if p, _ := m.GroupPolicy("ops"); len(p) != 1 || p[0].Name != "AllowB" || sink.count(`AllowA="maybe"`) == 0 {
		t.Fatal(p)
	}
	if _, ok := m.GroupPolicy("nogroup"); ok {
		t.Fatal("unknown group must not be created")
	}
	// Java appends a second <Group> for an existing name; the later one wins
	// at the first position.
	if ok, err := m.AddGroup("admin", []Policy{{"AllowOnly", true}}); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if g := m.Groups(); g[0] != "admin" || len(g) != 3 {
		t.Fatal(g)
	}
	if p, _ := m.GroupPolicy("admin"); len(p) != 1 || p[0].Name != "AllowOnly" {
		t.Fatal(p)
	}
	if bytes.Count(readTestFile(t, path), []byte(`<Group name="admin">`)) != 2 {
		t.Fatal("second admin group element")
	}
}

func TestParseDuplicatesAndMissingEmail(t *testing.T) {
	dir := t.TempDir()
	accounts := "\xef\xbb\xbf<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Accounts><Account id=\"a\" pass=\"1\" group=\"g\"/><Account id=\"b\" pass=\"2\" group=\"g\"><Email>b</Email></Account><Account id=\"a\" pass=\"3\" group=\"h\"><Email>x</Email></Account></Accounts>"
	groups := "<Groups><Group name=\"g\"><Policy><P>true</P><Q>false</Q><P>false</P><R> true</R></Policy></Group></Groups>"
	os.WriteFile(filepath.Join(dir, AccountFile), []byte(accounts), 0o600)
	os.WriteFile(filepath.Join(dir, GroupFile), []byte(groups), 0o600)
	m, _ := openTest(t, dir)
	got := m.Accounts()
	if len(got) != 2 || got[0] != (Account{ID: "a", Password: "3", Group: "h", Email: "x"}) || got[1].ID != "b" {
		t.Fatalf("%+v", got)
	}
	if p, _ := m.GroupPolicy("g"); len(p) != 2 || p[0] != (Policy{"P", false}) || p[1] != (Policy{"Q", false}) {
		t.Fatal(p)
	}
	// Edit changes the first <Account id="a">, which has no <Email>: Java NPE.
	if ok, err := m.Edit(Account{ID: "a", Password: "4"}); ok || !errors.Is(err, ErrXML) {
		t.Fatal(ok, err)
	}
}

func TestOpenAndPathErrors(t *testing.T) {
	base := t.TempDir()
	file := filepath.Join(base, "file")
	os.WriteFile(file, []byte("x"), 0o600)
	if _, err := Open(Config{Dir: file, Logf: func(string, ...any) {}}); err == nil {
		t.Fatal("conf dir is a file")
	}
	if _, err := Open(Config{Logf: func(string, ...any) {}}); err == nil {
		t.Fatal("empty dir")
	}
	for name, content := range map[string]string{
		"malformed":        "<Accounts><Account></Accounts>",
		"doctype":          "<!DOCTYPE x [<!ENTITY e \"v\">]><Accounts/>",
		"empty":            "",
		"group-no-policy":  "<Groups><Group name=\"g\"/></Groups>",
		"latin1-encoding":  "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><Accounts/>",
		"too-deep-element": strings.Repeat("<a>", maxXMLDepth+1) + strings.Repeat("</a>", maxXMLDepth+1),
	} {
		dir := filepath.Join(base, name)
		os.MkdirAll(dir, 0o755)
		target := AccountFile
		if name == "group-no-policy" {
			target = GroupFile
		}
		os.WriteFile(filepath.Join(dir, target), []byte(content), 0o600)
		if _, err := Open(Config{Dir: dir, Logf: func(string, ...any) {}}); !errors.Is(err, ErrXML) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	dir := filepath.Join(base, "dir-target")
	os.MkdirAll(filepath.Join(dir, AccountFile), 0o755)
	if _, err := Open(Config{Dir: dir, Logf: func(string, ...any) {}}); err == nil {
		t.Fatal("account.xml is a directory")
	}
	dir = filepath.Join(base, "oversize")
	os.MkdirAll(dir, 0o755)
	os.WriteFile(filepath.Join(dir, AccountFile), append([]byte("<Accounts>"), bytes.Repeat([]byte(" "), 200)...), 0o600)
	if _, err := Open(Config{Dir: dir, MaxFileBytes: 100, Logf: func(string, ...any) {}}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatal(err)
	}
	// A symbolic link is read but never replaced.
	dir = filepath.Join(base, "symlink")
	os.MkdirAll(dir, 0o755)
	real := filepath.Join(base, "real-account.xml")
	os.WriteFile(real, defaultAccountXML, 0o640)
	if err := os.Symlink(real, filepath.Join(dir, AccountFile)); err != nil {
		t.Fatal(err)
	}
	m, _ := openTest(t, dir)
	if len(m.Accounts()) != 2 {
		t.Fatal("symlink read")
	}
	if ok, err := m.Add(Account{ID: "synthetic"}); ok || err == nil {
		t.Fatal("symlink replaced", ok, err)
	}
	if !bytes.Equal(readTestFile(t, real), defaultAccountXML) {
		t.Fatal("symlink target changed")
	}
	// An existing file keeps its permission bits when rewritten.
	dir = filepath.Join(base, "mode")
	os.MkdirAll(dir, 0o755)
	os.WriteFile(filepath.Join(dir, AccountFile), defaultAccountXML, 0o640)
	m, _ = openTest(t, dir)
	if ok, err := m.Add(Account{ID: "synthetic"}); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if info, _ := os.Stat(filepath.Join(dir, AccountFile)); info.Mode().Perm() != 0o640 {
		t.Fatal(info.Mode())
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 2 {
		t.Fatal("temporary files left", entries)
	}
}

// touch rewrites a file with a modification time distinct from the stamp.
func touch(t *testing.T, path string, data []byte, at time.Time) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatal(err)
	}
}

func TestReloadOnChangeKeepsSnapshotOnError(t *testing.T) {
	dir := t.TempDir()
	m, sink := openTest(t, dir)
	path := filepath.Join(dir, AccountFile)
	at := time.Now().Add(time.Hour)
	touch(t, path, []byte(`<Accounts><Account id="only" pass="p" group="g"><Email/></Account></Accounts>`), at)
	m.Reload()
	if got := m.Accounts(); len(got) != 1 || got[0].ID != "only" {
		t.Fatal(got)
	}
	touch(t, path, []byte(`<Accounts><broken`), at.Add(time.Second))
	m.Reload()
	m.Reload()
	if got := m.Accounts(); len(got) != 1 || got[0].ID != "only" {
		t.Fatal("previous snapshot must stay", got)
	}
	if n := sink.count("load " + path); n != 1 {
		t.Fatalf("parse failure logged %d times", n)
	}
	// A failed reload does not block writes on a fixed file; deletion
	// recreates the default like canRead()==false.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	m.Reload()
	if !bytes.Equal(readTestFile(t, path), defaultAccountXML) || len(m.Accounts()) != 2 {
		t.Fatal("deleted file not recreated")
	}
	// Our own write does not trigger a reload (stamp updated).
	if ok, err := m.Add(Account{ID: "synthetic"}); !ok || err != nil {
		t.Fatal(ok, err)
	}
	loads := sink.count("loaded " + path)
	m.Reload()
	if sink.count("loaded "+path) != loads {
		t.Fatal("reloaded own write")
	}
}

func TestRunReloadsUntilCancel(t *testing.T) {
	dir := t.TempDir()
	sink := &logSink{}
	m, err := Open(Config{Dir: dir, Logf: sink.logf, ReloadInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	touch(t, filepath.Join(dir, GroupFile), []byte(`<Groups><Group name="solo"><Policy><AllowX>true</AllowX></Policy></Group></Groups>`), time.Now().Add(time.Hour))
	deadline := time.Now().Add(5 * time.Second)
	for {
		if g := m.Groups(); len(g) == 1 && g[0] == "solo" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("not reloaded")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestConcurrentAddEditAndReads(t *testing.T) {
	dir := t.TempDir()
	m, _ := openTest(t, dir)
	const writers = 8
	const each = 10
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = m.Accounts()
				_, _ = m.GroupPolicy("admin")
				_, _, _ = m.Authorize(context.Background(), "admin", "x")
				m.Reload()
			}
		}()
	}
	var writersWG sync.WaitGroup
	for w := range writers {
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			for i := range each {
				id := fmt.Sprintf("u%d-%d", w, i)
				if ok, err := m.Add(Account{ID: id, Password: "p", Group: "guest"}); !ok || err != nil {
					t.Errorf("add %s: %v %v", id, ok, err)
				}
				if ok, err := m.Edit(Account{ID: id, Password: "q", Group: "admin"}); !ok || err != nil {
					t.Errorf("edit %s: %v %v", id, ok, err)
				}
			}
			if ok, err := m.AddGroup(fmt.Sprintf("g%d", w), []Policy{{"AllowX", true}}); !ok || err != nil {
				t.Errorf("group: %v %v", ok, err)
			}
		}()
	}
	writersWG.Wait()
	close(stop)
	wg.Wait()
	if got := len(m.Accounts()); got != 2+writers*each {
		t.Fatal(got)
	}
	m2, _ := openTest(t, dir)
	if len(m2.Accounts()) != 2+writers*each || len(m2.Groups()) != 2+writers {
		t.Fatal("file lost updates")
	}
	for _, a := range m2.Accounts()[2:] {
		if a.Password != "q" || a.Group != "admin" {
			t.Fatal(a)
		}
	}
}

func TestAccountBytes(t *testing.T) {
	b, err := Account{ID: "id", Password: "pw", Email: "", Group: "g"}.Bytes()
	if err != nil || !bytes.Equal(b, []byte("\x02id\x02pw\x00\x01g")) {
		t.Fatalf("%q %v", b, err)
	}
	if _, err := (Account{Group: strings.Repeat("g", 256)}).Bytes(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

// Size and file-type rejections record the stamp: one log per file version.
func TestReloadRejectionsLoggedOncePerStamp(t *testing.T) {
	dir := t.TempDir()
	sink := &logSink{}
	m, err := Open(Config{Dir: dir, Logf: sink.logf, MaxFileBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, AccountFile)
	touch(t, path, append([]byte("<Accounts>"), bytes.Repeat([]byte(" "), 5000)...), time.Now().Add(time.Hour))
	for range 3 {
		m.Reload()
	}
	if n := sink.count("exceeds 4096 bytes"); n != 1 {
		t.Fatalf("oversize logged %d times", n)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		m.Reload()
	}
	if n := sink.count("is not a regular file"); n != 1 {
		t.Fatalf("non-regular logged %d times", n)
	}
	if len(m.Accounts()) != 2 {
		t.Fatal("previous snapshot must stay")
	}
}

func TestRecreatedDefaultAccountFileWarns(t *testing.T) {
	dir := t.TempDir()
	m, sink := openTest(t, dir)
	if sink.count("WARNING") != 2 || sink.count("well-known admin/guest passwords") != 1 {
		t.Fatal(sink.lines)
	}
	if err := os.Remove(filepath.Join(dir, AccountFile)); err != nil {
		t.Fatal(err)
	}
	m.Reload()
	if sink.count("well-known admin/guest passwords") != 2 {
		t.Fatal(sink.lines)
	}
}
