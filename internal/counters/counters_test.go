package counters

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func defaultXML(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("../../cmd/scouter-go/counters.xml")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestJavaOracleParseAndNormalize compares against outputs generated from the
// pinned sources (CounterEngine.parse compiled from scouter.common, the
// verbatim CounterManager.normalizeCountersXml + JAXB classes) on JDK 18 with
// jaxb-runtime 2.3.2 (testdata/oracle-src, CONTRACT.md). Parse validity must
// match exactly. The Go layout intentionally differs from the JAXB bytes
// (§9-3), so normalization is compared by meaning: every JAXB entry and
// attribute (JAXB default when absent) must be present in the Go output.
func TestJavaOracleParseAndNormalize(t *testing.T) {
	inputs, err := filepath.Glob("testdata/oracle/*.xml")
	if err != nil || len(inputs) < 40 {
		t.Fatal(len(inputs), err)
	}
	inputs = append(inputs, "../../cmd/scouter-go/counters.xml")
	for _, in := range inputs {
		base := strings.TrimSuffix(in, ".xml")
		if strings.HasSuffix(in, "cmd/scouter-go/counters.xml") {
			base = "testdata/oracle/default_counters"
		}
		name := filepath.Base(base)
		raw, err := os.ReadFile(in)
		if err != nil {
			t.Fatal(err)
		}
		wantParse := readFixture(t, base+".parse") == "true"
		parseErr := NewEngine(0).Parse(raw)
		switch name {
		case "doctype", "bad_colon_name":
			// Intended differences: DOCTYPE is refused; encoding/xml rejects
			// a QName with two colons that Java's non-namespace DOM accepts.
			if !wantParse || !errors.Is(parseErr, ErrMalformed) {
				t.Fatal(name, wantParse, parseErr)
			}
			continue
		}
		if (parseErr == nil) != wantParse {
			t.Fatalf("%s: parse Java=%v Go=%v", name, wantParse, parseErr)
		}
		if parseErr != nil {
			if !errors.Is(parseErr, ErrMalformed) {
				t.Fatal(name, parseErr)
			}
			continue
		}
		java := readFixture(t, base+".norm")
		got, err := normalizeContents(raw, DefaultMaxBytes)
		if err != nil {
			t.Fatal(name, err)
		}
		if err := NewEngine(0).Parse(got); err != nil {
			t.Fatalf("%s: Go output not parseable: %v", name, err)
		}
		switch {
		case java == "JAXBException":
			if !bytes.Equal(got, raw) {
				t.Fatalf("%s: want original text saved, got %q", name, got)
			}
		case strings.HasPrefix(java, "EXC:java.lang.NullPointerException"):
			// Intended difference: an empty <Types> is accepted.
			if !bytes.Contains(got, []byte("<Counters>")) {
				t.Fatalf("%s: %q", name, got)
			}
		default:
			union := name == "double_types" || name == "double_familys"
			compareWithJAXB(t, name, []byte(java), got, union)
		}
	}
}

type layoutEntry struct {
	attrs    map[string]string
	counters []map[string]string
}

// layoutSections reads the single <Types>/<Familys> sections of a
// normalized document into name-keyed entries plus the order of names.
func layoutSections(t *testing.T, data []byte) (types, families map[string]layoutEntry, typeOrder, familyOrder []string) {
	t.Helper()
	doc, err := parseDocument(data, DefaultMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	types, families = map[string]layoutEntry{}, map[string]layoutEntry{}
	for _, section := range doc.root.children {
		target, order, item := types, &typeOrder, "ObjectType"
		if section.qname == "Familys" {
			target, order, item = families, &familyOrder, "Family"
		}
		for _, e := range section.children {
			if e.qname != item {
				t.Fatal("unexpected", e.qname)
			}
			le := layoutEntry{attrs: map[string]string{}}
			for _, a := range e.attrs {
				le.attrs[a.qname] = a.value
			}
			for _, c := range e.children {
				m := map[string]string{}
				for _, a := range c.attrs {
					m[a.qname] = a.value
				}
				le.counters = append(le.counters, m)
			}
			name := strings.ReplaceAll(le.attrs["name"], "\t", " ") // see valueOr
			if _, dup := target[name]; dup {
				t.Fatal("duplicate name in normalized output", name)
			}
			target[name] = le
			*order = append(*order, name)
		}
	}
	return
}

// valueOr reads a Go attribute for comparison with JAXB. A TAB is written
// as &#9; by Go but literally by JAXB, which turns it into a space on re-read.
func valueOr(m map[string]string, k, def string) string {
	if v, ok := m[k]; ok {
		return strings.ReplaceAll(v, "\t", " ")
	}
	return def
}

// compareWithJAXB: JAXB writes every known attribute (default "" or
// Counter all="true"); Go writes only the attributes given. Go may carry
// more: ObjectType counters, unknown attributes, and with union=true the
// entries of earlier <Types>/<Familys> sections JAXB discards.
func compareWithJAXB(t *testing.T, name string, java, got []byte, union bool) {
	t.Helper()
	jt, jf, _, _ := layoutSections(t, java)
	gt, gf, _, _ := layoutSections(t, got)
	if !union && (len(jt) != len(gt) || len(jf) != len(gf)) {
		t.Fatalf("%s: entry count Java %d/%d Go %d/%d\n%s", name, len(jt), len(jf), len(gt), len(gf), got)
	}
	for n, j := range jt {
		g, ok := gt[n]
		if !ok {
			t.Fatalf("%s: ObjectType %q missing", name, n)
		}
		for _, k := range []string{"disp", "family", "icon", "name", "sub-object"} {
			if j.attrs[k] != valueOr(g.attrs, k, "") {
				t.Fatalf("%s: ObjectType %q %s Java %q Go %q", name, n, k, j.attrs[k], g.attrs[k])
			}
		}
	}
	for n, j := range jf {
		g, ok := gf[n]
		if !ok {
			t.Fatalf("%s: Family %q missing", name, n)
		}
		for _, k := range []string{"master", "name"} {
			if j.attrs[k] != valueOr(g.attrs, k, "") {
				t.Fatalf("%s: Family %q %s", name, n, k)
			}
		}
		if len(j.counters) != len(g.counters) {
			t.Fatalf("%s: Family %q counters %d/%d", name, n, len(j.counters), len(g.counters))
		}
		for i, jc := range j.counters {
			for _, k := range []string{"all", "disp", "icon", "name", "total", "unit"} {
				def := ""
				if k == "all" {
					def = "true"
				}
				if jc[k] != valueOr(g.counters[i], k, def) {
					t.Fatalf("%s: Family %q counter %d %s Java %q Go %q", name, n, i, k, jc[k], g.counters[i][k])
				}
			}
		}
	}
}

// TestNormalizeGoLayout pins the Go bytes for the decisions in §9-3.
func TestNormalizeGoLayout(t *testing.T) {
	head := "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<Counters>\n"
	for _, tc := range []struct{ name, want string }{
		{"analyst_sample", head + "    <Types>\n" +
			"        <ObjectType name=\"t1\" disp=\"T1dup\" family=\"myfam\" icon=\"host\" sub-object=\"true\"/>\n" +
			"        <ObjectType name=\"t2\" family=\"myfam\"/>\n    </Types>\n    <Familys>\n" +
			"        <Family name=\"myfam\" master=\"mem\"/>\n    </Familys>\n</Counters>\n"},
		{"family_counters", head + "    <Types>\n        <ObjectType name=\"t\" family=\"f\">\n" +
			"            <Counter name=\"own\"/>\n        </ObjectType>\n    </Types>\n    <Familys>\n" +
			"        <Family name=\"f\" master=\"c\" extra=\"1\">\n            <Counter name=\"c\"/>\n" +
			"            <Counter name=\"d\" all=\"false\" total=\"true\" unit=\"ms\" icon=\"i\" disp=\"D\" foo=\"bar\"/>\n" +
			"        </Family>\n        <Family name=\"g\"/>\n    </Familys>\n</Counters>\n"},
		{"empty_types", head + "    <Types/>\n    <Familys/>\n</Counters>\n"},
		{"double_types", head + "    <Types>\n        <ObjectType name=\"a\"/>\n        <ObjectType name=\"b\"/>\n    </Types>\n    <Familys/>\n</Counters>\n"},
		{"dups_reorder", head + "    <Types>\n        <ObjectType name=\"b\"/>\n        <ObjectType name=\"a\" disp=\"2\"/>\n        <ObjectType name=\"c\"/>\n    </Types>\n    <Familys/>\n</Counters>\n"},
		{"escape2", head + "    <Types>\n        <ObjectType name=\"h&#9;x&#10;y&#13;z&#9;w\" disp=\"crlf lit cr\" icon=\"&amp;#10;\"/>\n    </Types>\n" +
			"    <Familys>\n        <Family name=\"f&#10;\" master=\"&gt;\">\n            <Counter name=\"&#13;\" all=\"\" total=\"t\"/>\n        </Family>\n    </Familys>\n</Counters>\n"},
		{"declared_prefix_child", head + "    <Types>\n        <ObjectType name=\"b\"/>\n    </Types>\n    <Familys/>\n</Counters>\n"},
	} {
		raw, err := os.ReadFile(filepath.Join("testdata/oracle", tc.name+".xml"))
		if err != nil {
			t.Fatal(err)
		}
		got, err := normalizeContents(raw, DefaultMaxBytes)
		if err != nil || string(got) != tc.want {
			t.Fatalf("%s: %v\n--- got\n%s\n--- want\n%s", tc.name, err, got, tc.want)
		}
		// Idempotent, and the value semantics survive a re-read.
		again, err := normalizeContents(got, DefaultMaxBytes)
		if err != nil || !bytes.Equal(again, got) {
			t.Fatalf("%s: not idempotent\n%s", tc.name, again)
		}
	}
	// An absent Counter total keeps CounterEngine's default (true); JAXB's
	// total="" would turn it false.
	got, _ := normalizeContents([]byte(`<Counters><Familys><Family name="f"><Counter name="c"/></Family></Familys></Counters>`), DefaultMaxBytes)
	e := NewEngine(0)
	if err := e.Parse(got); err != nil || !e.Family("f").Counters()[0].Total {
		t.Fatalf("total default lost: %s", got)
	}
}

func readFixture(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestEngineDefaultModel(t *testing.T) {
	e := NewEngine(0)
	if err := e.Parse(defaultXML(t)); err != nil {
		t.Fatal(err)
	}
	host := e.Family("host")
	if host == nil || host.Master != "Cpu" || len(host.Counters()) == 0 {
		t.Fatalf("host family %#v", host)
	}
	cpu := host.Counters()[0]
	if cpu.Name != "Cpu" || cpu.Display != "CPU" || cpu.Unit != "%" || !cpu.All || cpu.Total {
		t.Fatalf("Cpu counter %#v", cpu)
	}
	if len(e.ObjectTypes()) == 0 {
		t.Fatal("no object types")
	}
	for _, o := range e.ObjectTypes() {
		if o.Name == "" {
			t.Fatal("unnamed object type kept")
		}
	}
	// Java put(MODE.LAST): a repeated family keeps its position, new value;
	// a repeated object type keeps the first object and gains counters.
	e2 := NewEngine(0)
	if err := e2.Parse([]byte(`<Counters><Familys><Family name="a" master="1"/><Family name="b"/><Family name="a" master="2"/></Familys>` +
		`<Types><ObjectType name="t" family="a" disp="first"><Counter name="c1"/></ObjectType><ObjectType name="t" disp="second"><Counter name="c2"/></ObjectType>` +
		`<ObjectType family="missing"><Counter name="orphan"/></ObjectType><ObjectType name="u" family="missing"/></Types></Counters>`)); err != nil {
		t.Fatal(err)
	}
	fams := e2.Families()
	if len(fams) != 2 || fams[0].Name != "a" || fams[0].Master != "2" || fams[1].Name != "b" {
		t.Fatalf("families %#v", fams)
	}
	ty := e2.ObjectType("t")
	if ty.Display != "first" || ty.Family != fams[0] || len(ty.Counters()) != 2 {
		t.Fatalf("type %#v", ty)
	}
	if u := e2.ObjectType("u"); u == nil || u.Family != nil {
		t.Fatalf("unknown family %#v", u)
	}
	if len(e2.ObjectTypes()) != 2 {
		t.Fatal(len(e2.ObjectTypes()))
	}
}

func openStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(StoreConfig{Dir: dir, Default: defaultXML(t)})
	if err != nil {
		t.Fatal(err)
	}
	return s, dir
}

func TestStoreSaveNormalizesAndReloads(t *testing.T) {
	s, dir := openStore(t)
	ctx := context.Background()
	if s.CustomXML() != nil {
		t.Fatal("custom without file")
	}
	if text, err := s.Text(ctx); err != nil || text != "" {
		t.Fatal(text, err)
	}
	input := readFixture(t, "testdata/oracle/analyst_sample.xml")
	ok, err := s.Save(ctx, input)
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	want, _ := normalizeContents([]byte(input), DefaultMaxBytes)
	disk, _ := os.ReadFile(filepath.Join(dir, CustomFileName))
	if !bytes.Equal(disk, want) || !bytes.Equal(s.CustomXML(), want) {
		t.Fatalf("saved %q", disk)
	}
	if text, _ := s.Text(ctx); text != string(want) {
		t.Fatal(text)
	}
	if o := s.Engine().ObjectType("t1"); o == nil || o.Display != "T1dup" || o.Family == nil || o.Family.Master != "mem" {
		t.Fatalf("reloaded %#v", o)
	}
	if s.Engine().Family("host") == nil {
		t.Fatal("default families lost on reload")
	}
	// Java parse rejection: "false", file untouched.
	if ok, err := s.Save(ctx, "<Counters>"); ok || err != nil {
		t.Fatal(ok, err)
	}
	disk, _ = os.ReadFile(filepath.Join(dir, CustomFileName))
	if !bytes.Equal(disk, want) {
		t.Fatal("failed save changed the file")
	}
	// Empty <Types/> is accepted (Java NPE, decision §9-3).
	if ok, err := s.Save(ctx, "<Counters><Types/></Counters>"); !ok || err != nil {
		t.Fatal(ok, err)
	}
	// JAXBException: the original text is saved and reloaded.
	raw := `<Root><ObjectType name="rawtype"/></Root>`
	if ok, err := s.Save(ctx, raw); !ok || err != nil {
		t.Fatal(ok, err)
	}
	disk, _ = os.ReadFile(filepath.Join(dir, CustomFileName))
	if string(disk) != raw || s.Engine().ObjectType("rawtype") == nil || s.Engine().ObjectType("t1") != nil {
		t.Fatalf("raw save %q", disk)
	}
	if _, err := s.Save(ctx, strings.Repeat(" ", DefaultMaxBytes+1)); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Save(cancelled, "<Counters/>"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestStoreDefineAndEditObjectType(t *testing.T) {
	s, dir := openStore(t)
	ctx := context.Background()
	spec := ObjectTypeSpec{Name: "myjava", Display: "My Java", Family: "javaee", Icon: "tomcat", SubObject: true}
	if ok, err := s.DefineObjectType(ctx, spec); !ok || err != nil {
		t.Fatal(ok, err)
	}
	want := "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<Counters>\n    <Types>\n" +
		"        <ObjectType name=\"myjava\" disp=\"My Java\" family=\"javaee\" icon=\"tomcat\" sub-object=\"true\"/>\n" +
		"    </Types>\n    <Familys/>\n</Counters>\n"
	disk, _ := os.ReadFile(filepath.Join(dir, CustomFileName))
	if string(disk) != want {
		t.Fatalf("define wrote %q", disk)
	}
	if o := s.Engine().ObjectType("myjava"); o == nil || !o.SubObject || o.Family == nil || o.Family.Name != "javaee" {
		t.Fatalf("engine %#v", o)
	}
	// Existing (custom or default) type, unknown family: Java false.
	for _, bad := range []ObjectTypeSpec{spec, {Name: "tomcat", Family: "javaee"}, {Name: "new", Family: "nofamily"}} {
		if ok, err := s.DefineObjectType(ctx, bad); ok || err != nil {
			t.Fatal(bad, ok, err)
		}
	}
	disk2, _ := os.ReadFile(filepath.Join(dir, CustomFileName))
	if !bytes.Equal(disk, disk2) {
		t.Fatal("rejected define wrote")
	}
	// Edit replaces the matching element; an unknown name is appended.
	if ok, err := s.EditObjectType(ctx, ObjectTypeSpec{Name: "myjava", Display: "Edited", Family: "host", Icon: "i"}); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if ok, err := s.EditObjectType(ctx, ObjectTypeSpec{Name: "other", Display: "O", Family: "host"}); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if o := s.Engine().ObjectType("myjava"); o.Display != "Edited" || o.SubObject || o.Family.Name != "host" {
		t.Fatalf("edit %#v", o)
	}
	if s.Engine().ObjectType("other") == nil {
		t.Fatal("edit append")
	}
	if ok, err := s.EditObjectType(ctx, ObjectTypeSpec{Name: "x", Family: "nofamily"}); ok || err != nil {
		t.Fatal(ok, err)
	}
	// A malformed custom file: getCustomDocument null -> Java false.
	if err := os.WriteFile(filepath.Join(dir, CustomFileName), []byte("<Counters>"), 0o644); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.DefineObjectType(ctx, ObjectTypeSpec{Name: "z", Family: "host"}); ok || err != nil {
		t.Fatal(ok, err)
	}
	// Another document element: DOMException -> false; nested Counters root
	// would need Xalan raw output -> explicit error.
	os.WriteFile(filepath.Join(dir, CustomFileName), []byte("<Other/>"), 0o644)
	if ok, err := s.DefineObjectType(ctx, ObjectTypeSpec{Name: "z", Family: "host"}); ok || err != nil {
		t.Fatal(ok, err)
	}
	os.WriteFile(filepath.Join(dir, CustomFileName), []byte("<Other><Counters/></Other>"), 0o644)
	if ok, err := s.DefineObjectType(ctx, ObjectTypeSpec{Name: "z", Family: "host"}); ok || !errors.Is(err, ErrUnsupportedDocument) {
		t.Fatal(ok, err)
	}
}

func TestStoreOpenKeepsUnparsableCustomBytes(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, CustomFileName), []byte("not xml"), 0o644)
	s, err := Open(StoreConfig{Dir: dir, Default: defaultXML(t)})
	if err != nil {
		t.Fatal(err)
	}
	if string(s.CustomXML()) != "not xml" || s.Engine().Family("host") == nil {
		t.Fatal("custom bytes or default engine")
	}
	if _, err := Open(StoreConfig{Dir: dir, Default: []byte("<bad")}); err == nil {
		t.Fatal("invalid default accepted")
	}
	if _, err := Open(StoreConfig{Default: defaultXML(t)}); err == nil {
		t.Fatal("empty dir accepted")
	}
}

func TestWriteFileSafety(t *testing.T) {
	dir := t.TempDir()
	if err := WriteFile(dir, "a.conf", []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	os.Chmod(filepath.Join(dir, "a.conf"), 0o640)
	if err := WriteFile(dir, "a.conf", []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(filepath.Join(dir, "a.conf"))
	data, _ := os.ReadFile(filepath.Join(dir, "a.conf"))
	if string(data) != "two" || info.Mode().Perm() != 0o640 {
		t.Fatal(string(data), info.Mode())
	}
	outside := filepath.Join(t.TempDir(), "target")
	os.WriteFile(outside, []byte("keep"), 0o644)
	os.Symlink(outside, filepath.Join(dir, "link.conf"))
	os.Mkdir(filepath.Join(dir, "sub"), 0o755)
	for _, name := range []string{"link.conf", "sub", "../x", "a/b", "", ".", ".."} {
		if err := WriteFile(dir, name, []byte("x"), 0o644); !errors.Is(err, ErrUnsafeTarget) {
			t.Fatal(name, err)
		}
	}
	if data, _ := os.ReadFile(outside); string(data) != "keep" {
		t.Fatal("symlink followed")
	}
	if _, _, err := ReadFile(dir, "link.conf", 10); !errors.Is(err, ErrUnsafeTarget) {
		t.Fatal(err)
	}
	if _, _, err := ReadFile(dir, "a.conf", 2); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, found, err := ReadFile(filepath.Join(dir, "missing"), "a.conf", 10); found || err != nil {
		t.Fatal(found, err)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp-") {
			t.Fatal("temporary file left", e.Name())
		}
	}
	// Missing directories are created like Java's parent mkdirs.
	nested := filepath.Join(dir, "new", "conf")
	if err := WriteFile(nested, "b.xml", []byte("b"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestStoreConcurrentMutations(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "c" + string(rune('a'+i))
			if ok, err := s.DefineObjectType(ctx, ObjectTypeSpec{Name: name, Family: "host"}); !ok || err != nil {
				t.Error(name, ok, err)
			}
			_ = s.CustomXML()
			_ = s.Engine().ObjectTypes()
		}(i)
	}
	wg.Wait()
	for i := 0; i < 8; i++ {
		if s.Engine().ObjectType("c"+string(rune('a'+i))) == nil {
			t.Fatal("lost concurrent define", i)
		}
	}
}

// TestJavaOracleDefineEdit compares DEFINE/EDIT output with the Java chain
// DOM append (CounterManager.appendObjectType/editOrAppendObjectType) ->
// XmlUtil Xalan indent -> normalizeCountersXml (testdata/oracle-src/DefineOracle.java).
// The edit fixtures model the written edit (Java discards it; CONTRACT.md).
// Compared by meaning as in TestJavaOracleParseAndNormalize; site.xml has an
// ObjectType child <Counter> and an unknown Family attribute that Go keeps.
func TestJavaOracleDefineEdit(t *testing.T) {
	for _, tc := range []struct {
		custom, out string
		spec        ObjectTypeSpec
		edit        bool
	}{
		{"", "none_define.out", ObjectTypeSpec{"myjava", "My Java", "javaee", "tomcat", true, nil}, false},
		{"site.xml", "site_define.out", ObjectTypeSpec{"new", "N&<>\"'", "host", "i\tx", false, nil}, false},
		{"site.xml", "site_edit.out", ObjectTypeSpec{"old", "Edited", "host", "", true, nil}, true},
		{"site.xml", "site_edit_append.out", ObjectTypeSpec{"absent", "A", "host", "", false, nil}, true},
		{"notypes.xml", "notypes_define.out", ObjectTypeSpec{"t", "T", "host", "", false, nil}, false},
	} {
		dir := t.TempDir()
		if tc.custom != "" {
			data, err := os.ReadFile(filepath.Join("testdata/define", tc.custom))
			if err != nil {
				t.Fatal(err)
			}
			os.WriteFile(filepath.Join(dir, CustomFileName), data, 0o644)
		}
		s, err := Open(StoreConfig{Dir: dir, Default: defaultXML(t)})
		if err != nil {
			t.Fatal(err)
		}
		var ok bool
		if tc.edit {
			ok, err = s.EditObjectType(context.Background(), tc.spec)
		} else {
			ok, err = s.DefineObjectType(context.Background(), tc.spec)
		}
		if !ok || err != nil {
			t.Fatal(tc.out, ok, err)
		}
		want := readFixture(t, filepath.Join("testdata/define", tc.out))
		got, _ := os.ReadFile(filepath.Join(dir, CustomFileName))
		compareWithJAXB(t, tc.out, []byte(want), got, false)
		if tc.custom == "site.xml" && (!bytes.Contains(got, []byte(`<Counter name="own"/>`)) || !bytes.Contains(got, []byte(`x="1"`))) {
			t.Fatalf("%s: preserved content lost\n%s", tc.out, got)
		}
	}
}

// TestRegistrarOperations covers the CounterManager side effects of the HTTP
// inputs (telegraf.TypeRegistrar semantics), starting from a new
// installation without counters.site.xml (Java: NullPointerException).
func TestRegistrarOperations(t *testing.T) {
	s, dir := openStore(t)
	custom := filepath.Join(dir, CustomFileName)
	if s.HasFamily("tg_cpu") || !s.HasFamily("host") {
		t.Fatal("HasFamily")
	}
	fam := FamilySpec{Name: "tg_cpu", Master: "usage", Counters: []CounterSpec{{Name: "usage", Display: "Usage", Unit: "%", All: true, Total: true}}}
	if err := s.SafelyAddFamily(fam); err != nil {
		t.Fatal(err)
	}
	if err := s.SafelyAddObjectType(ObjectTypeSpec{Name: "tg_cpu_host", Display: "tg_cpu_host", Icon: "cpu", Family: "tg_cpu"}); err != nil {
		t.Fatal(err)
	}
	spec, ok := s.ObjectTypeSpec("tg_cpu_host")
	if !ok || spec.Family != "tg_cpu" || len(spec.Counters) != 1 || spec.Counters[0] != fam.Counters[0] {
		t.Fatalf("%#v", spec)
	}
	// Merge, not replace (Java drops the other measurement's counters, Q2):
	// existing counters and master stay, new counters are appended.
	fam2 := FamilySpec{Name: "tg_cpu", Master: "idle", Counters: []CounterSpec{{Name: "idle", All: true}}}
	if err := s.SafelyAddFamily(fam2); err != nil {
		t.Fatal(err)
	}
	if f := s.Engine().Family("tg_cpu"); f.Master != "usage" || len(f.Counters()) != 2 || f.Counters()[0].Name != "usage" || f.Counters()[1].Name != "idle" {
		t.Fatalf("%#v", f.Counters())
	}
	// AddFamilyCounter: add, then replace in place.
	if err := s.AddFamilyCounter("tg_cpu", CounterSpec{Name: "steal", Display: "Steal", Unit: "%", All: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddFamilyCounter("tg_cpu", CounterSpec{Name: "idle", Display: "Idle2", All: false, Total: true}); err != nil {
		t.Fatal(err)
	}
	cs := s.Engine().Family("tg_cpu").Counters()
	if len(cs) != 3 || cs[0].Name != "usage" || cs[1].Name != "idle" || cs[1].Display != "Idle2" || cs[1].All || !cs[1].Total || cs[2].Name != "steal" {
		t.Fatalf("%d %#v", len(cs), cs)
	}
	// A default family is copied into the custom file with the new counter.
	hostCount := len(s.Engine().Family("host").Counters())
	if err := s.AddFamilyCounter("host", CounterSpec{Name: "GoCustom", Display: "Go", All: true, Total: true}); err != nil {
		t.Fatal(err)
	}
	if n := len(s.Engine().Family("host").Counters()); n != hostCount+1 {
		t.Fatal(n, hostCount)
	}
	if err := s.AddFamilyCounter("nofamily", CounterSpec{Name: "x"}); err == nil {
		t.Fatal("unknown family accepted")
	}
	// /register: new family + type, then edit adding own counters.
	if err := s.AddFamilyAndObjectType(FamilySpec{Name: "reg", Master: "a", Counters: []CounterSpec{{Name: "a", All: true, Total: true}}}, ObjectTypeSpec{Name: "regtype", Display: "Reg", Family: "reg"}); err != nil {
		t.Fatal(err)
	}
	edit := ObjectTypeSpec{Name: "regtype", Display: "Reg2", Family: "reg", Counters: []CounterSpec{{Name: "own1", All: true, Total: true}}}
	if err := s.EditObjectTypeCounters(edit); err != nil {
		t.Fatal(err)
	}
	edit.Counters = []CounterSpec{{Name: "own1", Display: "Own", All: true, Total: true}, {Name: "own2", All: true, Total: true}}
	if err := s.EditObjectTypeCounters(edit); err != nil {
		t.Fatal(err)
	}
	spec, _ = s.ObjectTypeSpec("regtype")
	if spec.Display != "Reg2" || len(spec.Counters) != 3 || spec.Counters[0].Display != "Own" || spec.Counters[1].Name != "own2" || spec.Counters[2].Name != "a" {
		t.Fatalf("%#v", spec)
	}
	if err := s.EditObjectTypeCounters(ObjectTypeSpec{Name: "regtype", Family: "nofamily"}); err == nil {
		t.Fatal("unknown family accepted")
	}
	// The file stays a normalized, re-readable document.
	disk, _ := os.ReadFile(custom)
	again, err := normalizeContents(disk, DefaultMaxBytes)
	if err != nil || !bytes.Equal(again, disk) || !bytes.Equal(disk, s.CustomXML()) {
		t.Fatalf("not normalized %v\n%s", err, disk)
	}
	// A malformed custom file fails the registrar explicitly.
	os.WriteFile(custom, []byte("<Counters>"), 0o644)
	if err := s.SafelyAddFamily(fam); !errors.Is(err, ErrMalformed) {
		t.Fatal(err)
	}
}
