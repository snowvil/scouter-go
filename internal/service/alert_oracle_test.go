package service

// Java oracle replay for AlertScriptingService (testdata/management/alert)
// and the RealCounter/PluginHelper descriptor sets
// (testdata/management/descriptors), see manage_oracle_test.go for the
// replay helpers and the outcome classes.
//
// The Java oracle filled AlertScriptLoadMessageCache directly
// (ManagementOracle.alert: messages "synthetic message 1..160" between the
// load-message cases). The Go ring is driven the same way through
// alertScriptMessages.putLocked on an empty ring; the Go FileMetadata seeds
// its ring with an "unsupported" notice at startup, which is a documented Go
// message and not part of this comparison.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// alertOracleBackend serves the oracle's ring through the FileMetadata
// AlertScriptLoadMessages path (PluginDir set: saves land in a temp dir).
func alertOracleBackend(t *testing.T) (*FileMetadata, string) {
	t.Helper()
	plugin := filepath.Join(t.TempDir(), "plugin")
	b, err := NewFileMetadata(FileMetadataConfig{PluginDir: plugin})
	if err != nil {
		t.Fatal(err)
	}
	b.alerts = &alertScriptMessages{rules: map[string]alertRuleStamp{}}
	return b, plugin
}

func sha256Hex(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }

func alertOraclePut(b *FileMetadata, from, to int) {
	b.alerts.mu.Lock()
	defer b.alerts.mu.Unlock()
	for i := from; i <= to; i++ {
		b.alerts.putLocked("synthetic message " + strconv.Itoa(i))
	}
}

// descriptorItems decodes a descriptor stream into canonical item bytes keyed
// by methodName(parameterTypeNames).
func descriptorItems(t *testing.T, items []oracleItem) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	for _, it := range items {
		m, ok := it.pack.(*protocol.MapPack)
		if !ok {
			t.Fatalf("descriptor item %T", it.pack)
		}
		var params []string
		for _, v := range lookup(m, "parameterTypeNames").Values {
			params = append(params, v.Text)
		}
		key := lookup(m, "methodName").Text + "(" + strings.Join(params, ",") + ")"
		raw, err := protocol.EncodePack(m, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if _, dup := out[key]; dup {
			t.Fatalf("duplicate descriptor %s", key)
		}
		out[key] = raw
	}
	return out
}

// sameDescriptorSet: the Java order is Class.getDeclaredMethods() order,
// which differs between runs and JDKs (the golden stores it sorted); Go
// streams the source declaration order. Items are compared as a set of
// byte-identical MapPacks.
func sameDescriptorSet(t *testing.T, got, java oracleResponse) {
	t.Helper()
	if !got.complete {
		t.Fatal("descriptor stream not terminated")
	}
	g, j := descriptorItems(t, got.items), descriptorItems(t, java.items)
	var keys []string
	for k := range j {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !bytes.Equal(g[k], j[k]) {
			t.Errorf("descriptor %s: go %x, java %x", k, g[k], j[k])
		}
	}
	if len(g) != len(j) {
		t.Errorf("go %d descriptors, java %d", len(g), len(j))
	}
}

func TestAlertScriptingJavaOracle(t *testing.T) {
	dir, cases := loadOracleCases(t, "alert")
	descriptorOrder := "Java streams Class.getDeclaredMethods() order (varies by run/JDK; golden sorted); Go streams the declaration order — compared as a set of byte-identical items"
	expect := map[string]oracleExpectation{
		"load-message-empty":              exactly(),
		"load-message-3-from-0":           exactly(),
		"load-message-3-from-2":           exactly(),
		"load-message-3-from-3":           exactly(),
		"load-message-loop1-from-0-3":     exactly(),
		"load-message-loop1-from-0-1":     exactly(),
		"load-message-loop1-current":      exactly(),
		"load-message-loop1-from-1-0":     exactly(),
		"load-message-loop3-stale":        exactly(),
		"load-message-future-loop":        exactly(),
		"load-message-text-cursor":        exactly(),
		"load-message-no-body":            {}, // not replayable over a socket, see below
		"real-counter-desc":               {},
		"plugin-helper-desc":              {},
		"save-alert-script":               exactly(),
		"save-alert-config":               exactly(),
		"get-alert-script":                exactly(),
		"get-alert-config":                exactly(),
		"get-alert-script-missing":        exactly(),
		"save-alert-script-null-contents": exactly(),
		"save-alert-script-null-name":     exactly(),
		"save-alert-script-subdir":        intended("§9-9 decision: counterName must be a plain file name; Java joins it into plugin_dir/sub/Elapsed.alert (mkdirs)", rejected),
		"save-alert-script-overwrite":     exactly(),
	}
	requireAllExpected(t, cases, expect)

	captureManageLog(t)
	backend, plugin := alertOracleBackend(t)
	config := queryConfig()
	config.Metadata = backend
	_, conn, r := startService(t, config, &fakeBackend{})
	client := &oracleClient{owner: t, session: loginSession(t, conn, r), conn: conn, r: r}
	// ManagementOracle.alert puts before these cases.
	puts := map[string][2]int{"load-message-3-from-0": {1, 3}, "load-message-loop1-from-0-3": {4, 52}, "load-message-loop3-stale": {53, 160}}
	for _, c := range cases {
		if c.Name == "load-message-no-body" {
			// The Java oracle fed a finite byte array, so readMapPack hit EOF
			// (EOFException, no item). Over a socket both Java and Go wait for
			// the body; there is no equivalent request to send.
			continue
		}
		if p, ok := puts[c.Name]; ok {
			alertOraclePut(backend, p[0], p[1])
		}
		want := expect[c.Name]
		if c.Name == "real-counter-desc" || c.Name == "plugin-helper-desc" {
			want = intended(descriptorOrder, nil)
			want.check = func(t *testing.T, got oracleResponse) {
				sameDescriptorSet(t, got, decodeJavaResponse(t, readGolden(t, dir, c.Response), c.ItemKind))
			}
		}
		t.Run(c.Name, func(t *testing.T) { replayOracleCase(t, dir, c, client, nil, want) })
	}
	// plugin_dir after the saves (Java facts.plugin_dir_tree: name -> bytes/sha256).
	var root struct {
		Facts struct {
			Tree map[string]struct {
				Bytes  int    `json:"bytes"`
				SHA256 string `json:"sha256"`
			} `json:"plugin_dir_tree"`
		} `json:"facts"`
	}
	if err := json.Unmarshal(readGolden(t, dir, "cases.json"), &root); err != nil {
		t.Fatal(err)
	}
	// Intended: Java FileUtil.saveText(file, null) creates the empty
	// ErrorCount.alert before its NullPointerException (answer false); Go
	// answers false without touching the file. sub/Elapsed.alert is the
	// rejected subdirectory save above.
	javaOnly := map[string]bool{"ErrorCount.alert": true, "sub/Elapsed.alert": true}
	var javaFiles, goFiles []string
	for name, f := range root.Facts.Tree {
		if javaOnly[name] {
			if _, err := os.Stat(filepath.Join(plugin, filepath.FromSlash(name))); err == nil {
				t.Errorf("%s exists in the Go plugin_dir", name)
			}
			continue
		}
		javaFiles = append(javaFiles, fmt.Sprintf("%s:%d:%s", name, f.Bytes, f.SHA256))
	}
	err := filepath.Walk(plugin, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(plugin, path)
		goFiles = append(goFiles, fmt.Sprintf("%s:%d:%s", filepath.ToSlash(rel), len(data), sha256Hex(data)))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(javaFiles)
	sort.Strings(goFiles)
	if strings.Join(goFiles, "\n") != strings.Join(javaFiles, "\n") {
		t.Fatalf("plugin_dir differs\n go   %v\n java %v", goFiles, javaFiles)
	}
}

// TestAlertDescriptorsJavaOracle compares the Go descriptor tables with the
// element sets the original RealCounter/PluginHelper reflection produced on
// JDK 8 and JDK 18 (testdata/management/descriptors).
func TestAlertDescriptorsJavaOracle(t *testing.T) {
	type desc struct {
		MethodName         string   `json:"methodName"`
		ReturnTypeName     string   `json:"returnTypeName"`
		ParameterTypeNames []string `json:"parameterTypeNames"`
		Desc               string   `json:"desc"`
	}
	render := func(d desc) string {
		return fmt.Sprintf("%s(%s) %s %q", d.MethodName, strings.Join(d.ParameterTypeNames, ","), d.ReturnTypeName, d.Desc)
	}
	goSet := func(ds []MethodDescriptor) []string {
		var out []string
		for _, d := range ds {
			out = append(out, render(desc{d.MethodName, d.ReturnTypeName, d.ParameterTypeNames, d.Desc}))
		}
		sort.Strings(out)
		return out
	}
	for _, jdk := range []string{"8", "18"} {
		data, err := os.ReadFile(filepath.Join("testdata", "management", "descriptors", "descriptors.java"+jdk+".json"))
		if err != nil {
			t.Fatalf("Java oracle golden missing: %v", err)
		}
		type set struct {
			Count int    `json:"count"`
			Items []desc `json:"items"`
		}
		var file struct {
			RealCounter  set `json:"RealCounter"`
			PluginHelper set `json:"PluginHelper"`
		}
		if err := json.Unmarshal(data, &file); err != nil {
			t.Fatal(err)
		}
		root := map[string]set{"RealCounter": file.RealCounter, "PluginHelper": file.PluginHelper}
		for name, ds := range map[string][]MethodDescriptor{"RealCounter": RealCounterDescriptors(), "PluginHelper": PluginHelperDescriptors()} {
			var java []string
			for _, d := range root[name].Items {
				java = append(java, render(d))
			}
			sort.Strings(java)
			if got := goSet(ds); strings.Join(got, "\n") != strings.Join(java, "\n") || root[name].Count != len(java) {
				t.Errorf("JDK %s %s descriptors differ\n go   %q\n java %q", jdk, name, got, java)
			}
		}
	}
}
