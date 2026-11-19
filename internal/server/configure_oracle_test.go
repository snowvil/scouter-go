package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/config"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/telegraf"
)

// javaConfigureInternal are public Configure fields that are not settings;
// Java getKeyValueInfo/getConfigureValueType expose them through reflection
// (management-commands.md §3.1, §9-8). Intended difference: Go lists only
// setting keys.
var javaConfigureInternal = map[string]bool{
	"telegrafInputConfigMapDeprecated": true, "telegrafOriginalConfig": true,
	"telegrafInputConfigMap": true, "property": true, "allowIpExact": true, "allowIpMatch": true,
}

// configureRunningOverlay are startup keys whose LIST value is the running
// Go value (internal/server/metadata.go effectiveOverlay): the test runtime's
// directories, ephemeral listeners and disabled retention. Their values are
// not compared; the default column still is.
var configureRunningOverlay = map[string]bool{
	"db_dir": true, "log_dir": true, "plugin_dir": true, "net_udp_listen_ip": true, "net_udp_listen_port": true,
	"net_tcp_listen_ip": true, "net_tcp_listen_port": true, "mgr_purge_enabled": true, "net_http_server_enabled": true,
	"net_http_port": true,
}

// configureTelegrafGlobals are the Configure fields that Java
// applyTelegrafInputConfigNew overwrites from scouter-telegraf.xml (enabled,
// debugEnabled, deltaCounterNormalizeDefault(Seconds), objectDeadtimeMs), so
// Java LIST shows the XML values once the file exists. Lead decision
// (intended difference): Go applies the XML values only to the Telegraf input
// (telegraf.Compose/Load) and LIST shows the scouter.conf values; the Go
// Telegraf configuration itself is checked against the XML below.
var configureTelegrafGlobals = map[string]bool{
	"input_telegraf_enabled": true, "input_telegraf_debug_enabled": true, "input_telegraf_delta_counter_normalize_default": true,
	"input_telegraf_delta_counter_normalize_default_seconds": true, "telegraf_object_deadtime_ms": true,
}

func moValueString(v protocol.Value) string {
	switch v.Type {
	case protocol.ValueText:
		return "Text(" + strconv.Quote(v.Text) + ")"
	case protocol.ValueDecimal:
		return fmt.Sprintf("Decimal(%d)", v.Int)
	case protocol.ValueBoolean:
		return fmt.Sprintf("Boolean(%v)", v.Bool)
	case protocol.ValueNull:
		return "Null"
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// moRedactedPaths reads the cases.json "redactions" of a LIST case: the Java
// texts replaced to keep the golden host/run independent (hostname, identity
// hash, the ${...} substitution result).
func moRedactedPaths(t *testing.T, g moGolden, name string) map[string]bool {
	t.Helper()
	var root struct {
		Cases []struct {
			Name       string `json:"name"`
			Redactions []struct {
				Path string `json:"path"`
			} `json:"redactions"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(g.read(t, "cases.json"), &root); err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, c := range root.Cases {
		if c.Name == name {
			for _, r := range c.Redactions {
				out[r.Path] = true
			}
		}
	}
	return out
}

// checkConfigureTable compares LIST_CONFIGURE_SERVER key/value/default.
func checkConfigureTable(t *testing.T, g moGolden, name string, got, java moResponse) {
	t.Helper()
	if !got.complete || len(got.items) != 1 {
		t.Fatalf("want one MapPack, got %s", moShow(got))
	}
	gm, jm := got.items[0].pack.(*protocol.MapPack), java.items[0].pack.(*protocol.MapPack)
	redacted := moRedactedPaths(t, g, name)
	jk, gk := moMapGet(jm, "key").Values, moMapGet(gm, "key").Values
	var javaKeys []string
	javaIndex := map[string]int{}
	for i, k := range jk {
		if !javaConfigureInternal[k.Text] {
			javaKeys = append(javaKeys, k.Text)
			javaIndex[k.Text] = i
		}
	}
	var goKeys []string
	for _, k := range gk {
		goKeys = append(goKeys, k.Text)
	}
	if strings.Join(goKeys, ",") != strings.Join(javaKeys, ",") {
		t.Fatalf("setting keys differ\n go   %v\n java %v", goKeys, javaKeys)
	}
	var diffs, typeOnly []string
	for _, col := range []string{"value", "default"} {
		gv, jv := moMapGet(gm, col).Values, moMapGet(jm, col).Values
		for i, key := range goKeys {
			ji := javaIndex[key]
			if redacted[fmt.Sprintf("%s[%d]", col, ji)] {
				// Java value was rewritten (hostname / ${...} env dump); Go
				// must not dump the environment either.
				if strings.Contains(gv[i].Text, "PATH=") || strings.Contains(gv[i].Text, "java.version=") {
					diffs = append(diffs, fmt.Sprintf("%s %s leaks the environment: %q", col, key, gv[i].Text))
				}
				continue
			}
			if col == "value" && configureRunningOverlay[key] {
				continue
			}
			if col == "value" && name == "list-configure-server-after-telegraf" && configureTelegrafGlobals[key] {
				continue
			}
			if col == "value" && name != "list-configure-server-initial" && configureRestartKey(key) {
				// Go keeps the running value of restart-only keys after a
				// reload (internal/config CONTRACT, Key.Restart); Java applies
				// them on the spot. The initial LIST still compares them.
				continue
			}
			a, b := moValueString(gv[i]), moValueString(jv[ji])
			if a == b {
				continue
			}
			if gv[i].Type == protocol.ValueText && jv[ji].Type == protocol.ValueText && latin1Text(gv[i].Text) == jv[ji].Text {
				// Intended charset difference (internal/config CONTRACT §2):
				// Java reads scouter.conf bytes as ISO-8859-1, Go keeps UTF-8.
				// A live key (server_id) shows the reloaded value in both.
				continue
			}
			if gv[i].Type == protocol.ValueText && jv[ji].Type != protocol.ValueText && javaValueText(jv[ji]) == gv[i].Text {
				// includes Java NullValue (null field initializer) vs Go "".
				typeOnly = append(typeOnly, fmt.Sprintf("%s %s", col, key))
				continue
			}
			diffs = append(diffs, fmt.Sprintf("%s %s: go %s, java %s", col, key, a, b))
		}
	}
	if len(diffs) > 0 || len(typeOnly) > 0 {
		t.Fatalf("LIST_CONFIGURE_SERVER differs from Java: %d cell(s) where Go sends Text and Java a typed value with the same text (Configure ConfigValueUtil.toValue: Decimal/Boolean), e.g. %v; %d other cell(s):\n%s",
			len(typeOnly), typeOnly[:min(5, len(typeOnly))], len(diffs), strings.Join(diffs, "\n"))
	}
}

// latin1Text is Java new String(utf8Bytes, ISO_8859_1): one char per byte.
func latin1Text(s string) string {
	out := make([]rune, 0, len(s))
	for i := 0; i < len(s); i++ {
		out = append(out, rune(s[i]))
	}
	return string(out)
}

func javaValueText(v protocol.Value) string {
	switch v.Type {
	case protocol.ValueDecimal:
		return strconv.FormatInt(v.Int, 10)
	case protocol.ValueBoolean:
		return strconv.FormatBool(v.Bool)
	}
	return v.Text
}

// configureRestartKey reports keys the Go server applies only at startup,
// including the purge days derived from the restart-only mgr_purge_keep_days.
func configureRestartKey(key string) bool {
	for _, k := range config.Keys {
		if k.Name == key {
			return k.Restart || strings.HasPrefix(key, "mgr_purge_") && key != "mgr_purge_keep_days" && configureRestartKey("mgr_purge_keep_days")
		}
	}
	return false
}

// checkConfigureKeys compares GET_CONFIGURE_SERVER (configKey list minus the
// Java-internal fields, serverConfig text exact).
func checkConfigureKeys(t *testing.T, got, java moResponse) {
	t.Helper()
	if !got.complete || len(got.items) != 1 {
		t.Fatalf("want one MapPack, got %s", moShow(got))
	}
	gm, jm := got.items[0].pack.(*protocol.MapPack), java.items[0].pack.(*protocol.MapPack)
	var javaKeys, goKeys []string
	for _, k := range moMapGet(jm, "configKey").Values {
		if !javaConfigureInternal[k.Text] {
			javaKeys = append(javaKeys, k.Text)
		}
	}
	for _, k := range moMapGet(gm, "configKey").Values {
		goKeys = append(goKeys, k.Text)
	}
	if strings.Join(goKeys, ",") != strings.Join(javaKeys, ",") {
		t.Fatalf("configKey differs\n go   %v\n java %v", goKeys, javaKeys)
	}
	if a, b := moMapGet(gm, "serverConfig"), moMapGet(jm, "serverConfig"); moValueString(a) != moValueString(b) {
		t.Fatalf("serverConfig go %s, java %s", moValueString(a), moValueString(b))
	}
}

// checkConfigureMap compares a key->value MapPack (CONFIGURE_DESC,
// CONFIGURE_VALUE_TYPE) after dropping the Java-internal fields.
func checkConfigureMap(t *testing.T, got, java moResponse) {
	t.Helper()
	if !got.complete || len(got.items) != 1 {
		t.Fatalf("want one MapPack, got %s", moShow(got))
	}
	gm, jm := got.items[0].pack.(*protocol.MapPack), java.items[0].pack.(*protocol.MapPack)
	var want, have []string
	for _, e := range jm.Entries {
		if !javaConfigureInternal[e.Key] {
			want = append(want, e.Key+"="+moValueString(e.Value))
		}
	}
	for _, e := range gm.Entries {
		have = append(have, e.Key+"="+moValueString(e.Value))
	}
	if strings.Join(have, "\n") != strings.Join(want, "\n") {
		javaSet := map[string]bool{}
		for _, w := range want {
			javaSet[w] = true
		}
		var extra []string
		for _, h := range have {
			if !javaSet[h] {
				extra = append(extra, h)
			}
		}
		t.Fatalf("map differs from Java (%d go, %d java entries); go entries not in Java: %v", len(have), len(want), extra)
	}
}

// TestConfigureJavaOracle replays testdata/management/configure
// (ConfigureService + Configure) on the Go runtime.
func TestConfigureJavaOracle(t *testing.T) {
	g := moLoad(t, "configure")
	table := func(name string) moExpect {
		return moIntendedDiff("Java-internal Configure fields omitted (§9-8), running-value overlay for startup keys, redacted Java cells; every other cell compared with its value type",
			func(t *testing.T, got, java moResponse) { checkConfigureTable(t, g, name, got, java) })
	}
	keys := moIntendedDiff("Java-internal Configure fields omitted from configKey (§9-8)", checkConfigureKeys)
	mapCmp := moIntendedDiff("Java-internal Configure fields omitted (§9-8)", checkConfigureMap)
	expect := map[string]moExpect{
		"get-configure-server":             keys,
		"list-configure-server-initial":    table("list-configure-server-initial"),
		"configure-desc":                   mapCmp,
		"configure-value-type":             mapCmp,
		"configure-value-type-desc":        moExactly(),
		"configure-value-type-desc-agent":  moExactly(),
		"set-configure-server":             moExactly(),
		"list-configure-server-after-set":  table("list-configure-server-after-set"),
		"get-configure-server-after-set":   keys,
		"set-configure-server-null":        moExactly(),
		"get-configure-telegraf-sample":    moExactly(),
		"set-configure-telegraf":           moExactly(),
		"get-configure-telegraf-after-set": moExactly(),
		"set-configure-telegraf-null":      moExactly(),
		"set-configure-telegraf-invalid-xml": moIntendedDiff("Go parses the Telegraf XML before saving and answers \"false\"; Java saves invalid XML and answers \"true\" (internal/server/metadata.go runtimeMetadataWith)",
			func(t *testing.T, got, java moResponse) {
				if !got.complete || len(got.items) != 1 || moMapGet(got.items[0].pack.(*protocol.MapPack), "result").Text != "false" {
					t.Fatalf("want {result:\"false\"}, got %s", moShow(got))
				}
			}),
		"list-configure-server-after-telegraf": table("list-configure-server-after-telegraf"),
	}
	moRequireAll(t, g, expect)
	java := moDecodeJava(t, g.read(t, "get-configure-server.response.bin"), "pack")
	initial := moMapGet(java.items[0].pack.(*protocol.MapPack), "serverConfig").Text
	root, addr, store := moRuntime(t, initial)
	client := moLogin(t, addr)
	for _, c := range g.cases {
		if c.Name == "get-configure-telegraf-sample" {
			// ManagementOracle.configure: Configure.saveText(unicode) without
			// reload, between set-configure-server-null and this case.
			if err := os.WriteFile(filepath.Join(root, "conf", "scouter.conf"), []byte("server_id=합성\nnet_http_port=6181\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			// Java re-reads it at the next reload (SET_CONFIGURE_TELEGRAF's
			// reload(true) here); the Go runtime's Store.Watch does the same
			// within config.ReloadInterval. The test store is not watched.
			if _, err := store.Reload(true); err != nil {
				t.Fatal(err)
			}
		}
		t.Run(c.Name, func(t *testing.T) { moReplay(t, g, c, client, nil, expect[c.Name]) })
	}
	// The Telegraf input does use the XML globals (Java facts
	// telegraf_applied_after_set.telegraf_object_deadtime_ms = 45000): load the
	// saved scouter-telegraf.xml the way Runtime.ReloadTelegraf does.
	tg, _, err := telegraf.Load(store.Current().TelegrafDeprecated(), filepath.Join(root, "conf", "scouter-telegraf.xml"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var facts struct {
		Applied struct {
			Deadtime int  `json:"telegraf_object_deadtime_ms"`
			Enabled  bool `json:"input_telegraf_enabled"`
			Debug    bool `json:"input_telegraf_debug_enabled"`
		} `json:"telegraf_applied_after_set"`
	}
	if err := json.Unmarshal(g.facts["telegraf_applied_after_set"], &facts.Applied); err != nil {
		t.Fatal(err)
	}
	if tg.ObjectDeadtimeMs != facts.Applied.Deadtime || tg.Enabled != facts.Applied.Enabled || tg.Debug != facts.Applied.Debug || !tg.FromFile {
		t.Fatalf("Go Telegraf config %+v, Java applied %+v", tg, facts.Applied)
	}
}
