package telegraf

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSampleMapping(t *testing.T) {
	now := time.Unix(1700000000, 0)
	cfg := sampleConfig(t, now)
	if !cfg.FromFile || !cfg.ForcesHTTPServer() || !cfg.Debug || cfg.ObjectDeadtimeMs != 35000 {
		t.Fatalf("%+v", cfg)
	}
	cases := []struct {
		line                           string
		reason                         DropReason
		host, family, objType, objName string
		counters                       []string
	}{
		{"cpu,cpu=cpu-total,host=h1 usage_user=1.5,usage_system=2i,usage_idle=3 1700000000000000000", Accepted,
			"h1", "X$HOST_METRIC", "HOST_NEW", "/h1/X$LINUX_NEW", []string{"cpu_user_cpu-total", "cpu_system_cpu-total"}},
		{"cpu,cpu=cpu-total,host=h1,scouter_obj_type_prefix=p usage_user=1 1", Accepted,
			"h1", "X$HOST_METRIC", "p_HOST_NEW", "/h1/X$LINUX_NEW", []string{"cpu_user_cpu-total"}},
		{"cpu,cpu=cpu-total,scouter_obj_type_prefix= usage_user=1 1", Accepted,
			"unknown", "X$HOST_METRIC", "HOST_NEW", "/unknown/X$LINUX_NEW", []string{"cpu_user_cpu-total"}},
		{"cpu,cpu=cpu0,host=h1 usage_user=1 1", Filtered, "", "", "", "", nil},
		{"cpu,host=h1 usage_user=1 1", Filtered, "", "", "", "", nil},
		{"mem,host=h1 used_percent=10,free=5i,total=7i 1", Accepted,
			"h1", "X$HOST_METRIC", "HOST_NEW", "/h1/X$LINUX_NEW", []string{"mem-used_percent", "mem-free", "mem-total"}},
		{"redis_keyspace,host=h1,port=6379,database=db0 keys=10i,expires=1i 1", Accepted,
			"h1", "X$redis", "redis", "/h1/X$redis_6379", []string{"keys-db0", "expires-db0"}},
		{"redis_keyspace,host=h1 keys=10i 1", Accepted,
			"h1", "X$redis", "redis", "/h1/X$redis_null", []string{"keys-null"}},
		{"mysql,host=h1,server=s:3306 connections=1i 1", Accepted,
			"h1", "X$mysql", "mysql", "/h1/X$_s:3306", []string{"connections"}},
		{"unknown,host=h1 v=1 1", UnknownMeasurement, "", "", "", "", nil},
		{"cpu,cpu=cpu-total,host=h1 usage_user=1", ParseError, "", "", "", "", nil},
		{"cpu,cpu=cpu-total,host=h1 usage_user=1 1.5", ParseError, "", "", "", "", nil},
		{`cpu,cpu=cpu-total,host=h1 usage_user="x" 1`, ParseError, "", "", "", "", nil}, // D2
		{"cpu,cpu=cpu-total,host=h1 usage_user=abc1 1", ParseError, "", "", "", "", nil},
		{"cpu,cpu=cpu-total,host=h1 usage_guest=x 1", Accepted, "h1", "X$HOST_METRIC", "HOST_NEW", "/h1/X$LINUX_NEW", nil},
	}
	for _, c := range cases {
		line := ParseLine(c.line)
		p, reason := Resolve(&line, cfg, 42)
		if reason != c.reason {
			t.Errorf("%s: reason %v want %v", c.line, reason, c.reason)
			continue
		}
		if p == nil {
			continue
		}
		var counters []string
		for _, f := range p.Fields {
			counters = append(counters, f.Counter.TaggedName(p.Tags))
		}
		if p.Host != c.host || p.Family != c.family || p.ObjType != c.objType || p.ObjName != c.objName || p.ObjHash != Hash(c.objName) || strings.Join(counters, ",") != strings.Join(c.counters, ",") || p.ReceivedTime != 42 {
			t.Errorf("%s:\n got %s %s %s %s %v", c.line, p.Host, p.Family, p.ObjType, p.ObjName, counters)
		}
	}
	line := ParseLine("cpu,cpu=cpu-total,host=h1 usage_user=1 1700000000123456789")
	if p, _ := Resolve(&line, cfg, 0); p.TimestampMs != 1700000000123 {
		t.Fatal(p.TimestampMs)
	}
	line = ParseLine("cpu,cpu=cpu-total,host=h1 usage_user=1 -1999999")
	if p, _ := Resolve(&line, cfg, 0); p.TimestampMs != -1 {
		t.Fatal("Java long division truncates toward zero", p.TimestampMs)
	}
}

func TestCounterProtocolNames(t *testing.T) {
	now := time.Unix(0, 0)
	cp := NewCounterProtocol(now)
	cp.SetName("keys-$database$-$x$")
	cp.SetDisplayName("K $database$")
	cp.Unit = "ea"
	cp.DeltaType = DeltaBoth
	tags := map[string]string{"database": "db0"}
	got := cp.Counters(tags)
	if len(got) != 2 || got[0].Name != "keys-db0-null" || got[0].DisplayName != "K db0" || got[1].Name != "keys-db0-null_$delta" || got[1].DisplayName != "K db0_$delta" || got[1].Unit != "ea/s" || !got[0].Total || !got[0].All {
		t.Fatalf("%+v", got)
	}
	cp.SetName("a$$b")
	if cp.TaggedName(tags) != "a*b" {
		t.Fatal(cp.TaggedName(tags))
	}
	for in, want := range map[int]int{0: 0, 3: 0, 4: 4, 30: 30, 61: 60} {
		cp.SetNormalizeSec(in)
		if cp.NormalizeSec != want {
			t.Fatal(in, cp.NormalizeSec)
		}
	}
	delta := NewCounterProtocol(now)
	delta.DeltaType = DeltaDelta
	if delta.HasNormal() || !delta.HasDelta() {
		t.Fatal("DELTA")
	}
	null := NewCounterProtocol(now)
	null.DeltaType = ""
	if !null.HasNormal() || null.HasDelta() {
		t.Fatal("null delta type behaves like NONE")
	}
}

func TestNewOrChangedFreshWindow(t *testing.T) {
	start := time.Unix(1000, 0)
	cp := NewCounterProtocol(start)
	cp.SetName("c")
	cp.SetDisplayName("C")
	same := ObjectTypeDef{Counters: []CounterDef{{Name: "c", DisplayName: "C", Total: true}}}
	changed := ObjectTypeDef{Counters: []CounterDef{{Name: "c", DisplayName: "other", Total: true}}}
	if cp.NewOrChanged(ObjectTypeDef{}, nil, start) != true || cp.NewOrChanged(same, nil, start) {
		t.Fatal("missing/same")
	}
	if !cp.NewOrChanged(changed, nil, start.Add(10*time.Second)) {
		t.Fatal("deep compare while fresh")
	}
	// The first check past 15s still compares deeply and then turns stale.
	if !cp.NewOrChanged(changed, nil, start.Add(16*time.Second)) || cp.NewOrChanged(changed, nil, start.Add(17*time.Second)) {
		t.Fatal("stale transition")
	}
	both := NewCounterProtocol(start)
	both.SetName("d")
	both.SetDisplayName("d")
	both.DeltaType = DeltaBoth
	if !both.NewOrChanged(ObjectTypeDef{Counters: []CounterDef{{Name: "d", DisplayName: "d", Total: true}}}, nil, start) {
		t.Fatal("missing delta counter")
	}
}

func TestFromProperties(t *testing.T) {
	now := time.Unix(0, 0)
	cfg, warnings := FromProperties(map[string]string{
		"input_telegraf_enabled":                                 " false ",
		"input_telegraf_delta_counter_normalize_default_seconds": "10",
		"telegraf_object_deadtime_ms":                            "9000",
		"input_telegraf_$redis$_counter_mappings":                "used_memory:tg-redis-used-memory,&&expired_keys:expired:Expired::false:20,&ops:ops-$port$:,bad,&:x",
		"input_telegraf_$redis$_objFamily_base":                  "redis",
		"input_telegraf_$redis$_objType_base":                    "redis",
		"input_telegraf_$redis$_objType_prepend_tags":            "",
		"input_telegraf_$redis$_objName_append_tags":             "port,,db",
		"input_telegraf_$redis$_host_mappings":                   "h1:sh1,bad,h2:sh2:x",
		"input_telegraf_$redis$_tag_filter":                      "role:master,role:!slave,broken",
		"input_telegraf_$redis$_debug_enabled":                   "TRUE",
		"input_telegraf_$redis$_enabled":                         "yes",
		"input_telegraf_$nodollar":                               "x",
		"input_telegraf_$m$_unknown_postfix":                     "x",
	}, now)
	if cfg.Enabled || cfg.DeltaNormalizeDefaultSeconds != 10 || cfg.ObjectDeadtimeMs != 9000 || cfg.FromFile {
		t.Fatalf("%+v", cfg)
	}
	if len(warnings) != 4 {
		t.Fatalf("warnings %q", warnings)
	}
	r := cfg.Measurements["redis"]
	if r == nil || r.Enabled || !r.Debug || !r.Valid() || r.ObjFamilyBase != "X$redis" || r.ObjNameBase != "X$" || r.ObjTypePrependTags[0] != defaultPrependTag {
		t.Fatalf("%+v", r)
	}
	if strings.Join(r.ObjNameAppendTags, ",") != "port,db" || len(r.HostMapping) != 1 || r.HostMapping["h1"] != "sh1" || len(r.TagFilter["role"]) != 2 {
		t.Fatalf("%+v", r)
	}
	used, exp, ops := r.Counters["used_memory"], r.Counters["expired_keys"], r.Counters["ops"]
	if used == nil || used.Name() != "tg-redis-used-memory" || used.DisplayName() != "tg-redis-used-memory" || !used.Total || used.NormalizeSec != 0 || used.DeltaType != DeltaNone {
		t.Fatalf("%+v", used)
	}
	if exp == nil || exp.DeltaType != DeltaBoth || exp.DisplayName() != "Expired" || exp.Unit != "" || exp.Total || exp.NormalizeSec != 20 {
		t.Fatalf("%+v", exp)
	}
	if ops == nil || ops.DeltaType != DeltaDelta || ops.TaggedName(map[string]string{"port": "1"}) != "ops-1" || ops.DisplayName() != "ops-*" {
		t.Fatalf("%+v", ops)
	}
	if len(r.Counters) != 3 {
		t.Fatal(len(r.Counters))
	}
	m := cfg.Measurements["m"]
	if m == nil || m.Valid() || len(m.ObjTypePrependTags) != 1 {
		t.Fatalf("%+v", m)
	}
	tags := map[string]string{"role": "replica"}
	if !r.TagFilterMatching(tags) || !r.TagFilterMatching(map[string]string{}) || r.TagFilterMatching(map[string]string{"role": "slave"}) {
		t.Fatal("negated filter matches a missing or different tag")
	}
}

func TestApplyFileMergeAndErrors(t *testing.T) {
	now := time.Unix(0, 0)
	base, _ := FromProperties(map[string]string{
		"input_telegraf_$cpu$_objFamily_base":  "OLD",
		"input_telegraf_$keep$_objFamily_base": "K",
	}, now)
	cfg := func(xml string) (*Config, error) {
		f, _, err := ParseFile([]byte(xml))
		if err != nil {
			t.Fatal(err)
		}
		return base.ApplyFile(f, now)
	}
	got, err := cfg(`<TelegrafConfig><enabled>false</enabled><measurements><measurement><measurementName>cpu</measurementName><objFamilyBase>NEW</objFamilyBase>
<tagFilters><tagFilter><tag>a</tag><match>1</match></tagFilter><tagFilter><tag>a</tag><match>2</match></tagFilter></tagFilters></measurement></measurements></TelegrafConfig>`)
	if err != nil || got.Enabled || got.ForcesHTTPServer() || got.Measurements["cpu"].ObjFamilyBase != "X$NEW" || got.Measurements["keep"].ObjFamilyBase != "X$K" || len(got.Measurements["cpu"].TagFilter["a"]) != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	for _, bad := range []string{
		`<counterMappings><counterMapping><tgFieldName>f</tgFieldName></counterMapping><counterMapping><tgFieldName>f</tgFieldName></counterMapping></counterMappings>`,
		`<hostMappings><hostMapping><telegraf>a</telegraf><scouter>b</scouter></hostMapping><hostMapping><telegraf>a</telegraf><scouter>c</scouter></hostMapping></hostMappings>`,
		`<hostMappings><hostMapping><telegraf>a</telegraf></hostMapping></hostMappings>`,
		`<tagFilters><tagFilter><tag>a</tag></tagFilter></tagFilters>`,
		`<tagFilters><tagFilter><tag>a</tag><match></match></tagFilter></tagFilters>`,
		`<counterMappings><counterMapping><counterName>c</counterName></counterMapping></counterMappings>`,
	} {
		if _, err := cfg(`<TelegrafConfig><measurements><measurement><measurementName>x</measurementName>` + bad + `</measurement></measurements></TelegrafConfig>`); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
	if _, err := cfg(`<TelegrafConfig><measurements><measurement/></measurements></TelegrafConfig>`); err == nil {
		t.Error("measurement without name accepted")
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(0, 0)
	props := map[string]string{"input_telegraf_$keep$_objFamily_base": "K", "input_telegraf_debug_enabled": "true"}
	cfg, _, err := Load(props, filepath.Join(dir, "missing.xml"), now)
	if err != nil || cfg.FromFile || !cfg.Debug || cfg.Measurements["keep"] == nil {
		t.Fatalf("%+v %v", cfg, err)
	}
	path := filepath.Join(dir, "scouter-telegraf.xml")
	if err := os.WriteFile(path, sampleXML(t), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, _, err = Load(props, path, now)
	if err != nil || !cfg.FromFile || !cfg.Debug || len(cfg.Measurements) != 8 {
		t.Fatalf("%v %v", cfg, err)
	}
	if err := os.WriteFile(path, []byte("<TelegrafConfig>"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, _, err = Load(props, path, now)
	if err == nil || !cfg.FromFile || !cfg.Enabled || cfg.Debug || len(cfg.Measurements) != 1 {
		t.Fatalf("fallback %+v %v", cfg, err)
	}
}

func TestComposeRebuildsBothSources(t *testing.T) {
	now := time.Unix(0, 0)
	xml := []byte(`<TelegrafConfig><enabled>false</enabled><measurements><measurement><measurementName>x</measurementName><objFamilyBase>F</objFamilyBase></measurement></measurements></TelegrafConfig>`)
	props := map[string]string{"input_telegraf_enabled": "true", "input_telegraf_$d$_objFamily_base": "D"}
	cfg, _, err := Compose(props, xml, true, now)
	if err != nil || cfg.Enabled || cfg.Measurements["x"] == nil || cfg.Measurements["d"] == nil {
		t.Fatalf("%+v %v", cfg, err)
	}
	// A later scouter.conf-only change keeps the XML globals (no D13 revert)
	// and picks up the new deprecated measurement at once.
	props["input_telegraf_$e$_objFamily_base"] = "E"
	cfg, _, err = Compose(props, xml, true, now)
	if err != nil || cfg.Enabled || cfg.Measurements["e"] == nil {
		t.Fatalf("%+v %v", cfg, err)
	}
	if cfg, _, err = Compose(props, nil, false, now); err != nil || !cfg.Enabled || cfg.FromFile {
		t.Fatalf("%+v %v", cfg, err)
	}
	if cfg, _, err = Compose(props, []byte("<bad"), true, now); err == nil || !cfg.FromFile || cfg.Measurements["e"] == nil {
		t.Fatalf("%+v %v", cfg, err)
	}
}

func TestLoadRejectsOversizedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "big.xml")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(MaxFileBytes + 1); err != nil { // sparse: no disk use
		t.Fatal(err)
	}
	f.Close()
	cfg, _, err := Load(nil, path, time.Unix(0, 0))
	if err == nil || !strings.Contains(err.Error(), "exceeds") || !cfg.FromFile {
		t.Fatalf("%v %+v", err, cfg)
	}
	if _, err := readBounded(path, 10); err == nil {
		t.Fatal("limit")
	}
	if data, err := readBounded(path, MaxFileBytes+1); err != nil || len(data) != MaxFileBytes+1 {
		t.Fatal(len(data), err)
	}
}
