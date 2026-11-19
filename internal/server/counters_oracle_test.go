package server

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/snowvil/scouter-go/internal/counters"
	"github.com/snowvil/scouter-go/internal/protocol"
)

// javaTotalAttr finds Counter total="..." literals in a JAXB-written file
// (attribute order all, disp, icon, name, total, unit).
var javaTotalAttr = regexp.MustCompile(`<Counter [^>]*name="([^"]*)" total="([^"]*)"`)

// countersView renders the CounterEngine meaning of a counters.site.xml:
// object types (display, icon, family, sub-object, own counters) and
// families (master, counters). Counter total is shown only when the
// attribute is "true"/"false" in the Java file: JAXB writes total="" for a
// missing attribute, which CounterEngine reads as false, while Go keeps the
// attribute absent (engine default true) — internal/counters/CONTRACT.md
// "attribute 순서·기본값", an intended difference.
func countersView(t *testing.T, xml []byte, javaTotals map[string]string) string {
	t.Helper()
	e := counters.NewEngine(counters.DefaultMaxBytes)
	if err := e.Parse(xml); err != nil {
		t.Fatalf("CounterEngine parse: %v\n%s", err, xml)
	}
	counter := func(c *counters.Counter) string {
		s := fmt.Sprintf("%s(disp=%q unit=%q icon=%q all=%v", c.Name, c.Display, c.Unit, c.Icon, c.All)
		if v, ok := javaTotals[c.Name]; ok && v != "" {
			s += fmt.Sprintf(" total=%v", c.Total)
		}
		return s + ")"
	}
	var lines []string
	for _, o := range e.ObjectTypes() {
		family := "<nil>"
		if o.Family != nil {
			family = o.Family.Name
		}
		var own []string
		for _, c := range o.Counters() {
			own = append(own, counter(c))
		}
		lines = append(lines, fmt.Sprintf("type %s disp=%q icon=%q family=%s sub=%v counters=%v", o.Name, o.Display, o.Icon, family, o.SubObject, own))
	}
	for _, f := range e.Families() {
		var cs []string
		for _, c := range f.Counters() {
			cs = append(cs, counter(c))
		}
		lines = append(lines, fmt.Sprintf("family %s master=%q counters=%v", f.Name, f.Master, cs))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func javaTotals(xml []byte) map[string]string {
	out := map[string]string{}
	for _, m := range javaTotalAttr.FindAllSubmatch(xml, -1) {
		out[string(m[1])] = string(m[2])
	}
	return out
}

// sameCountersMeaning compares two counters.site.xml texts by engine meaning.
func sameCountersMeaning(t *testing.T, goXML, javaXML []byte) {
	t.Helper()
	totals := javaTotals(javaXML)
	if a, b := countersView(t, goXML, totals), countersView(t, javaXML, totals); a != b {
		t.Fatalf("counters.site.xml meaning differs\n go:\n%s\n java:\n%s", a, b)
	}
}

// TestCountersSiteJavaOracle replays testdata/management/counters
// (ConfigureService DEFINE/EDIT_OBJECT_TYPE, SET/GET_CONFIGURE_COUNTERS_SITE,
// GET_XML_COUNTER, CounterManager, RegisterHandler.notifyAllClients) on the Go
// runtime. Java logged in sessions 5001/5002 (LoginManager seam); the two Go
// Client sessions stand in for them. The written counters.site.xml differs in
// bytes by design (internal/counters/CONTRACT.md: Go keeps input order and
// attributes, JAXB rewrites them), so files are compared by engine meaning.
func TestCountersSiteJavaOracle(t *testing.T) {
	g := moLoad(t, "counters")
	editedText := func(t *testing.T, got moResponse) string {
		t.Helper()
		if !got.complete || len(got.items) != 1 {
			t.Fatalf("want one MapPack, got %s", moShow(got))
		}
		return moMapGet(got.items[0].pack.(*protocol.MapPack), "contents").Text
	}
	// Parsed alone, the custom file's family "javaee" (defined in the default
	// counters.xml) is unresolved, as in Java CounterEngine.parse of one file.
	edited := "type synthetic_type disp=\"Edited\" icon=\"redis\" family=<nil> sub=true counters=[]"
	expect := map[string]moExpect{
		"get-xml-counter-initial":           moExactly(),
		"get-counters-site-initial":         moExactly(),
		"define-object-type":                moExactly(),
		"define-object-type-duplicate":      moExactly(),
		"define-object-type-unknown-family": moExactly(),
		"edit-object-type":                  moExactly(),
		"get-xml-counter-after-define": moIntendedDiff("custom blob is the Go-normalized file (byte layout differs by design) and carries the EDIT_OBJECT_TYPE result (§9-2: Java discards the edit)",
			func(t *testing.T, got, java moResponse) {
				gm, jm := got.items[0].pack.(*protocol.MapPack), java.items[0].pack.(*protocol.MapPack)
				if !got.complete || moValueString(moMapGet(gm, "default")) != moValueString(moMapGet(jm, "default")) {
					t.Fatal("default counters.xml blob differs")
				}
				if view := countersView(t, moMapGet(gm, "custom").Bytes, nil); view != edited {
					t.Fatalf("custom view %q", view)
				}
			}),
		"get-counters-site-after-define": moIntendedDiff("Go-normalized text with the EDIT_OBJECT_TYPE result (§9-2)",
			func(t *testing.T, got, java moResponse) {
				if view := countersView(t, []byte(editedText(t, got)), nil); view != edited {
					t.Fatalf("contents view %q", view)
				}
			}),
		"set-counters-site":             moExactly(),
		"check-job-after-set-5001":      moExactly(),
		"check-job-after-set-5002":      moExactly(),
		"set-counters-site-invalid-xml": moExactly(),
		"set-counters-site-types-empty": moIntendedDiff("§9-3 decision: an empty <Types/> is accepted (\"true\"); Java normalizeCountersXml NPEs and answers nothing",
			func(t *testing.T, got, java moResponse) {
				if !got.complete || len(got.items) != 1 || moMapGet(got.items[0].pack.(*protocol.MapPack), "result").Text != "true" {
					t.Fatalf("want {result:\"true\"}, got %s", moShow(got))
				}
			}),
		"set-counters-site-no-contents": moIntendedDiff("missing contents: Java NPE is swallowed (no item, connection kept); Go rejects the request and closes the connection (transport error policy)",
			func(t *testing.T, got, java moResponse) {
				if got.complete || len(got.items) != 0 {
					t.Fatalf("want rejected request, got %s", moShow(got))
				}
			}),
		"set-counters-site-empty-root": moExactly(),
	}
	moRequireAll(t, g, expect)
	root, addr, _ := moRuntime(t, "server_id=synthetic-server\n")
	a, b := moLogin(t, addr), moLogin(t, addr)
	site := filepath.Join(root, "conf", "counters.site.xml")
	// Files the Java oracle snapshotted after these cases; compared by meaning.
	snapshots := map[string]string{"define-object-type": "counters.site.after-define.xml", "set-counters-site": "counters.site.after-set.xml", "set-counters-site-empty-root": "counters.site.after-empty-root.xml"}
	for _, c := range g.cases {
		client := a
		if c.Name == "check-job-after-set-5002" {
			client = b
		}
		sessions := func(m *protocol.MapPack) {
			for i, e := range m.Entries {
				if e.Key == "session" && e.Value.Int == 5001 {
					m.Entries[i].Value.Int = a.session
				} else if e.Key == "session" && e.Value.Int == 5002 {
					m.Entries[i].Value.Int = b.session
				}
			}
		}
		t.Run(c.Name, func(t *testing.T) {
			moReplay(t, g, c, client, sessions, expect[c.Name])
			if file, ok := snapshots[c.Name]; ok {
				got, err := os.ReadFile(site)
				if err != nil {
					t.Fatal(err)
				}
				sameCountersMeaning(t, got, g.read(t, file))
			}
		})
	}
}
