package account

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// oracleOp is one XmlOracle.java line: op name and fields ("\x00" = Java null).
type oracleOp struct {
	name   string
	fields []string
}

const javaNull = "\x00"

func (o oracleOp) line() string {
	parts := []string{o.name}
	for _, f := range o.fields {
		if f == javaNull {
			parts = append(parts, "-")
		} else {
			parts = append(parts, hex.EncodeToString([]byte(f)))
		}
	}
	return strings.Join(parts, "\t")
}

func nullText(s string) string {
	if s == javaNull {
		return ""
	}
	return s
}

// apply runs the same DOM step in Go.
func (o oracleOp) apply(doc *xmlDocument) error {
	f := make([]string, len(o.fields))
	for i, v := range o.fields {
		f[i] = nullText(v)
	}
	switch o.name {
	case "identity":
		return nil
	case "addAccount":
		return addAccountDoc(doc, Account{ID: f[0], Password: f[1], Group: f[2], Email: f[3]})
	case "editAccount":
		return editAccountDoc(doc, Account{ID: f[0], Password: f[1], Group: f[2], Email: f[3]})
	case "addGroup":
		var p []Policy
		for i := 1; i+1 < len(f); i += 2 {
			p = append(p, Policy{Name: f[i], Allow: strings.EqualFold(f[i+1], "true")})
		}
		return addGroupDoc(doc, f[0], p)
	case "editPolicy":
		var edits []GroupPolicyEdit
		for i := 0; i < len(f); {
			n, err := strconv.Atoi(f[i+1])
			if err != nil {
				return err
			}
			e := GroupPolicyEdit{Group: f[i]}
			for k := range n {
				e.Policy = append(e.Policy, PolicyText{Name: f[i+2+2*k], Text: f[i+3+2*k]})
			}
			edits = append(edits, e)
			i += 2 + 2*n
		}
		return editPolicyDoc(doc, edits)
	}
	return fmt.Errorf("unknown op %s", o.name)
}

// jdk8Home finds a JDK 8: the Scouter server's Java target and the Xalan
// whose whitespace handling Go reproduces.
func jdk8Home(t *testing.T) string {
	if home := os.Getenv("SCOUTER_JDK8_HOME"); home != "" {
		return home
	}
	out, err := exec.Command("/usr/libexec/java_home", "-v", "1.8").Output()
	if err != nil {
		t.Skip("JDK 8 unavailable (set SCOUTER_JDK8_HOME): Xalan oracle not run")
	}
	return strings.TrimSpace(string(out))
}

func runOracle(t *testing.T, home string, classes string, start []byte, ops []oracleOp) []string {
	t.Helper()
	dir := t.TempDir()
	startPath := filepath.Join(dir, "start.xml")
	opsPath := filepath.Join(dir, "ops.txt")
	var lines []string
	for _, o := range ops {
		lines = append(lines, o.line())
	}
	if err := os.WriteFile(startPath, start, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(opsPath, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(filepath.Join(home, "bin", "java"), "-Dfile.encoding=UTF-8", "-cp", classes, "XmlOracle", startPath, opsPath)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("oracle: %v", err)
	}
	var results []string
	s := bufio.NewScanner(bytes.NewReader(out))
	s.Buffer(nil, 64<<20)
	for s.Scan() {
		results = append(results, s.Text())
	}
	if len(results) != len(ops) {
		t.Fatalf("oracle answered %d of %d ops", len(results), len(ops))
	}
	return results
}

func TestXalanJDK8Oracle(t *testing.T) {
	home := jdk8Home(t)
	classes := t.TempDir()
	if out, err := exec.Command(filepath.Join(home, "bin", "javac"), "-encoding", "UTF-8", "-d", classes, "testdata/oracle/XmlOracle.java").CombinedOutput(); err != nil {
		t.Fatalf("javac: %v %s", err, out)
	}
	add := func(id, pass, group, email string) oracleOp {
		return oracleOp{"addAccount", []string{id, pass, group, email}}
	}
	edit := func(id, pass, group, email string) oracleOp {
		return oracleOp{"editAccount", []string{id, pass, group, email}}
	}
	identity := oracleOp{name: "identity"}
	special := "a&b<c>\"d'e\tf\ng\rh]]>iéㄱ\U0001F600\u0085\u007f"
	cases := []struct {
		name  string
		start []byte
		ops   []oracleOp
	}{
		{"default accounts", defaultAccountXML, []oracleOp{
			identity,
			add("synthetic-1", "digest-1", "admin", "one@example.invalid"),
			edit("admin", "digest-2", "guest", "two@example.invalid"),
			add("synthetic-2", "digest-3", "guest", ""),
			add("synthetic-3", javaNull, javaNull, javaNull),
			add(special, special, special, special),
			edit("synthetic-1", "digest-4", "admin", ""),
			edit("missing", "x", "y", "z"),
			identity,
		}},
		{"default groups", defaultGroupXML, []oracleOp{
			identity,
			{"addGroup", []string{"ops", "AllowB", "false", "AllowA", "true"}},
			{"editPolicy", []string{"admin", "2", "AllowHeapDump", "false", "AllowNew", "true", "nogroup", "1", "AllowX", "true", "ops", "1", "AllowB", ""}},
			{"editPolicy", []string{"ops", "3", "AllowZ1", "", "AllowZ2", "true", "AllowA", "false"}},
			{"addGroup", []string{"ops", "AllowC", "true"}},
			{"addGroup", []string{special}},
			{"addGroup", []string{"bad", "1bad", "true"}},
			{"editPolicy", []string{"guest", "1", "a b", "true"}},
			{"editPolicy", []string{"guest", "1", "AllowSystemGC", special}},
			identity,
		}},
		{"prolog epilog standalone", []byte(`<?xml version="1.0" standalone="yes"?><!--c1--><?app x?><Accounts><!--in--><?pi x?><Account id="a"><Email>e</Email></Account></Accounts><!--tail-->`), []oracleOp{
			identity, add("b", "p", "g", "e"), identity,
		}},
		{"compact", []byte(`<Accounts><Account group="g" id="a" pass="p"><Email>e</Email></Account></Accounts>`), []oracleOp{
			add("b", "p", "g", "e"), edit("a", "q", "h", ""), identity,
		}},
		{"mixed text and references", []byte("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n<Accounts>\r\n  x&amp;y<!--c--><Account id=\"a&#9;b\" pass=\"&quot;&apos;\" group=\"&#13;\"><Email>a&#13;b&lt;&gt;\r\nc<x>t</x>z</Email>  </Account>\r\n\t</Accounts>\r\n"), []oracleOp{
			identity, edit("a\tb", "p", "g", "new"), add("c", "p", "g", "e"),
		}},
		{"missing email and nesting", []byte("<Root>\n<Accounts>\n<Account id=\"a\"/>\n<Group><Account id=\"n\"><Email>e</Email></Account></Group>\n</Accounts>\n<Accounts/>\n</Root>"), []oracleOp{
			edit("a", "p", "g", "e"), edit("n", "p", "g", "e2"), add("b", "p", "g", "e"),
		}},
		{"no container", []byte("<Other/>"), []oracleOp{add("a", "p", "g", "e"), {"addGroup", []string{"x"}}}},
		{"group without policy", []byte("<Groups><Group name=\"a\"/></Groups>"), []oracleOp{
			{"editPolicy", []string{"a", "1", "AllowX", "true"}},
			{"editPolicy", []string{"a", "0"}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := runOracle(t, home, classes, tc.start, tc.ops)
			var status []string
			for _, w := range want {
				status = append(status, strings.SplitN(w, " ", 2)[0]+fmt.Sprintf("(%d)", len(w)/2))
			}
			t.Logf("Java results: %v", status)
			current := tc.start
			for i, op := range tc.ops {
				doc, err := parseXML(current)
				if err == nil {
					err = op.apply(doc)
				}
				if strings.HasPrefix(want[i], "ERR ") {
					if err == nil {
						t.Fatalf("op %d %s: Java %s, Go succeeded", i, op.name, want[i])
					}
					continue
				}
				if err != nil {
					t.Fatalf("op %d %s: Go %v, Java OK", i, op.name, err)
				}
				javaOut, decodeErr := hex.DecodeString(strings.TrimPrefix(want[i], "OK "))
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				got := doc.marshal()
				if !bytes.Equal(got, javaOut) {
					t.Fatalf("op %d %s differs\nGo:   %q\nJava: %q", i, op.name, got, javaOut)
				}
				current = got
			}
		})
	}
}
