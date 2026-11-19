package pulse

import (
	"errors"
	"reflect"
	"testing"
)

func TestDefaultConfigMatchesJavaDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.XLogMetaTopic != "dev-pulse-scouter-xlog-meta" || cfg.XLogProfileTopic != "dev-pulse-scouter-xlog-profile" {
		t.Fatal(cfg.XLogMetaTopic, cfg.XLogProfileTopic)
	}
	if cfg.TopicPartitionCount != 30 || cfg.ProduceTimeoutMillis != 5000 || cfg.DispatchQueueCapacity != 4096 || cfg.DispatchWorkerCount != 2 {
		t.Fatal(cfg)
	}
	if !cfg.Debug || !cfg.Enable || !cfg.EnableSpringURIFilter || cfg.KafkaServers != "" {
		t.Fatal(cfg)
	}
	if !reflect.DeepEqual(cfg.ExcludeTransactionExts, toSet([]string{"woff", "woff2", "eot", "svg", "ttf"})) {
		t.Fatal(cfg.ExcludeTransactionExts)
	}
	// The application-name filters are intentionally empty (the plugin defaults hold names
	// of one deployment): nothing is excluded, rebased or treated as non-Spring.
	if len(cfg.ExcludeApplications) != 0 || len(cfg.RebaseTransactionApps) != 0 || len(cfg.NonSpringBasedApps) != 0 {
		t.Fatal(cfg.ExcludeApplications, cfg.RebaseTransactionApps, cfg.NonSpringBasedApps)
	}
	if cfg.IsExcludeApplication("excluded-app") || !cfg.IsSpringBasedApplication("gateway-app") {
		t.Fatal("empty application filters must not match")
	}
	if !reflect.DeepEqual(cfg.RebaseTransactionGlobal, []string{"$$", "@"}) {
		t.Fatal(cfg.RebaseTransactionGlobal)
	}
	if len(cfg.DropHTTPStatusRules) != 0 {
		t.Fatal(cfg.DropHTTPStatusRules)
	}
}

func TestParseJavaConfigNormalization(t *testing.T) {
	cfg, err := ParseJavaConfig(map[string]string{
		KeyKafkaServers:                 "broker:9092",
		KeyKafkaXLogMetaTopic:           "meta",
		KeyKafkaXLogProfileTopic:        "profile",
		KeyKafkaTopicPartitionCount:     "7",
		KeyKafkaProduceTimeout:          "abc", // Configure.getInt falls back to the default
		KeyDebug:                        "FALSE",
		KeyEnable:                       "yes", // Boolean.parseBoolean -> false
		KeyKafkaDispatchQueueCapacity:   "0",   // Math.max(1, ...)
		KeyKafkaDispatchWorkerCount:     "-3",
		KeyExcludeTransactionExtensions: " WOFF , Svg,",
		KeyExcludeApplications:          "Excluded-App, Other ",
		KeyRebaseTransactionApps:        "Gateway-App|/|3, bad, x|y, noint|/|n, four|/|2|z, Web|-|2",
		KeyRebaseTransactionGlobal:      " $$ , @ ,$$",
		KeyEnableSpringURIFilter:        "false",
		KeyNonSpringBasedApplications:   "Gw",
		KeyDropHTTPStatusRules:          " Api|404 , *|5XX, bad, a||b, x|xxx, y|4x, Z| 200 ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KafkaServers != "broker:9092" || cfg.XLogMetaTopic != "meta" || cfg.XLogProfileTopic != "profile" || cfg.TopicPartitionCount != 7 || cfg.ProduceTimeoutMillis != 5000 {
		t.Fatal(cfg)
	}
	if cfg.Debug || cfg.Enable || cfg.EnableSpringURIFilter || cfg.DispatchQueueCapacity != 1 || cfg.DispatchWorkerCount != 1 {
		t.Fatal(cfg)
	}
	if !reflect.DeepEqual(cfg.ExcludeTransactionExts, toSet([]string{"woff", "svg"})) {
		t.Fatal(cfg.ExcludeTransactionExts)
	}
	if !reflect.DeepEqual(cfg.ExcludeApplications, toSet([]string{"excluded-app", "other"})) {
		t.Fatal(cfg.ExcludeApplications)
	}
	if !reflect.DeepEqual(cfg.RebaseTransactionApps, map[string]RebaseCondition{"gateway-app": {"/", 3}, "web": {"-", 2}}) {
		t.Fatal(cfg.RebaseTransactionApps)
	}
	if !reflect.DeepEqual(cfg.RebaseTransactionGlobal, []string{"$$", "@"}) {
		t.Fatal(cfg.RebaseTransactionGlobal)
	}
	if !reflect.DeepEqual(cfg.NonSpringBasedApps, toSet([]string{"gw"})) {
		t.Fatal(cfg.NonSpringBasedApps)
	}
	var rules []string
	for _, r := range cfg.DropHTTPStatusRules {
		rules = append(rules, r.String())
	}
	if !reflect.DeepEqual(rules, []string{"api|404", "*|5xx", "z|200"}) {
		t.Fatal(rules)
	}
	if _, err := ParseJavaConfig(map[string]string{KeyKafkaTopicPartitionCount: "0"}); !errors.Is(err, ErrConfig) {
		t.Fatal("zero partition count accepted", err)
	}
	if _, err := ParseJavaConfig(map[string]string{KeyRebaseTransactionApps: "a|/|2,A|/|3"}); !errors.Is(err, ErrConfig) {
		t.Fatal("duplicate rebase key accepted (Collectors.toMap would throw)", err)
	}
	empty, err := ParseJavaConfig(map[string]string{KeyExcludeApplications: ""})
	if err != nil || !reflect.DeepEqual(empty.ExcludeApplications, toSet([]string{""})) {
		t.Fatal("present-but-empty value must parse as Java \"\".split(\",\")", empty.ExcludeApplications, err)
	}
}

func TestFilters(t *testing.T) {
	cfg, err := ParseJavaConfig(map[string]string{
		KeyExcludeApplications:        "excluded-app",
		KeyNonSpringBasedApplications: "gateway-app,plain-app-a,plain-app-b,plain-app-c",
		KeyDropHTTPStatusRules:        "api|404,*|5xx,web|200",
	})
	if err != nil {
		t.Fatal(err)
	}
	s := func(v string) *string { return &v }
	// transaction extension exclusion
	for in, want := range map[*string]bool{nil: true, s("/a/b.WOFF"): true, s("/a/b.woff2"): true, s("/a/b.css"): false, s("/a/b."): false, s("noext"): false, s(".svg"): true} {
		if got := cfg.IsExcludeTransaction(in); got != want {
			t.Errorf("IsExcludeTransaction(%v)=%v want %v", in, got, want)
		}
	}
	// application exclusion: extracted application is matched without lowercase.
	if !cfg.IsExcludeApplication("excluded-app") || cfg.IsExcludeApplication("Excluded-App") {
		t.Fatal("application exclusion casing")
	}
	// Spring-based application check
	if cfg.IsSpringBasedApplication("gateway-app") || !cfg.IsSpringBasedApplication("other") || !cfg.IsSpringBasedApplication("GATEWAY-APP") {
		t.Fatal("spring-based detection")
	}
	cfg.EnableSpringURIFilter = false
	if cfg.IsSpringBasedApplication("other") {
		t.Fatal("filter disabled must bypass")
	}
	// Spring URI filter
	for in, want := range map[string]bool{"/users/1": true, "/users/{id}": true, "/users/<id>": false, "<id>": false, "UserService.find": false, "/": true} {
		if got := SpringURIFiltered(in); got != want {
			t.Errorf("springURIFiltered(%q)=%v want %v", in, got, want)
		}
	}
	// HTTP status drop: application trimmed+lowercased only here; status<=0 never drops.
	cases := []struct {
		app    string
		status int32
		want   bool
	}{{"API", 404, true}, {" api ", 404, true}, {"api", 403, false}, {"other", 503, true}, {"other", 499, false}, {"web", 200, true}, {"web", 0, false}, {"web", -200, false}}
	for _, c := range cases {
		if got := cfg.IsDropHTTPStatus(c.app, c.status); got != c.want {
			t.Errorf("IsDropHTTPStatus(%q,%d)=%v want %v", c.app, c.status, got, c.want)
		}
	}
	if DefaultConfig().IsDropHTTPStatus("api", 404) {
		t.Fatal("no rules must not drop")
	}
}

func TestRebaseAndGlobalFilter(t *testing.T) {
	// transaction rebase
	cases := []struct {
		content string
		cond    RebaseCondition
		want    string
	}{
		{"/a/b/c/d", RebaseCondition{"/", 3}, "/a/b"},
		{"/a/b/c/d", RebaseCondition{"/", 2}, "/a"},
		{"/a/b", RebaseCondition{"/", 3}, "/a/b"}, // fewer occurrences
		{"/a/b/c", RebaseCondition{"/", 1}, "/a/b/c"},
		{"/a/b/c", RebaseCondition{"/", 0}, "/a/b/c"},
		{"/a/b/c", RebaseCondition{"/", -2}, "/a/b/c"},
		{"a--b--c--d", RebaseCondition{"--", 3}, "a--b--c"},
		{"abc", RebaseCondition{"", 2}, ""}, // Java indexOf("") matches at the start repeatedly
		{"", RebaseCondition{"/", 2}, ""},
	}
	for _, c := range cases {
		if got := RebaseTransaction(c.content, c.cond); got != c.want {
			t.Errorf("rebase(%q,%+v)=%q want %q", c.content, c.cond, got, c.want)
		}
	}
	// global suffix removal: earliest condition wins.
	global := []string{"$$", "@"}
	for in, want := range map[string]string{"svc$$x@y": "svc", "svc@y$$x": "svc", "plain": "plain", "@lead": "", "": ""} {
		got, changed := RemoveAfterConditions(in, global)
		if got != want || changed != (got != in) {
			t.Errorf("removeAfter(%q)=%q,%v want %q", in, got, changed, want)
		}
	}
	if got, changed := RemoveAfterConditions("a$$b", []string{"", "$$"}); got != "a" || !changed {
		t.Fatal("empty condition must be skipped", got)
	}
	tr := &Trace{TransactionDictContent: strPtr("/api/v1/users@x", true)}
	tr.Rebase(RebaseCondition{"/", 3})
	if *tr.TransactionDictContent != "/api/v1" || tr.TransactionDictID != TransactionHash("/api/v1") {
		t.Fatal(*tr.TransactionDictContent, tr.TransactionDictID)
	}
	tr.RemoveAfter(global)
	if *tr.TransactionDictContent != "/api/v1" || tr.TransactionDictID != -1016607451 {
		t.Fatal("unchanged content must keep hash", tr.TransactionDictID)
	}
	tr.TransactionDictContent = strPtr("/api$$x", true)
	tr.RemoveAfter(global)
	if *tr.TransactionDictContent != "/api" || tr.TransactionDictID != -1957600919 {
		t.Fatal(*tr.TransactionDictContent, tr.TransactionDictID)
	}
}
