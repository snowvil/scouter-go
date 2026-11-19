package config

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestLiveKeysMatchTable(t *testing.T) {
	var marked []string
	for _, k := range Keys {
		if k.Live {
			marked = append(marked, k.Name)
			if k.Restart || k.GoUse == "" {
				t.Fatalf("%s: a live key is not a restart key and names its consumer", k.Name)
			}
		}
	}
	got := append([]string(nil), LiveKeys...)
	sort.Strings(got)
	sort.Strings(marked)
	if len(got) != len(marked) {
		t.Fatalf("LiveKeys %v, marked %v", got, marked)
	}
	for i := range got {
		if got[i] != marked[i] {
			t.Fatalf("LiveKeys %v, marked %v", got, marked)
		}
	}
	eff := DefaultLive().Effective()
	for _, k := range LiveKeys {
		if _, ok := eff[k]; !ok {
			t.Fatalf("Effective lacks %s", k)
		}
	}
}

func TestLiveDefaultsAndJavaParsing(t *testing.T) {
	d := DefaultLive()
	want := Live{ObjectDeadTimeMs: 8000, ObjectZipkinDeadTimeMs: 300000, TagCountEnabled: true, Auto5MSampling: true,
		XLogQueueSize: 10000, ProfileQueueSize: 1000,
		SamplingGxidKeepCount: 500000, SamplingXLogKeepCount: 100000, SamplingXLogKeepMillis: 5000, SamplingProfileKeepCount: 5000, SamplingProfileKeepSecs: 5,
		GeoIPEnabled: true, GeoIPDataCityFile: "./conf/GeoLiteCity.dat", AgentKeepAliveIntervalMs: 5000, AgentConnectionWaitMs: 1000,
		ExtLinkName: "scouter-paper", ExtLinkURLPattern: keyIndex["ext_link_url_pattern"].Default,
		ClientSoTimeoutMs: 8000, SearchXLogMaxCount: 500, LogDir: "./logs",
		AgentSoTimeoutMs: 60000, ServicePoolSize: 100, LogExpiredMultipacket: true,
		Webapp: WebappLive{CORSAllowOrigin: "*", CORSAllowCredentials: "true", AllowIPs: "localhost,127.0.0.1,0:0:0:0:0:0:0:1,::1", PoolTimeoutMs: 8000},
		LDAP:   LDAP{Auth: "simple"},
		// Java field defaults; mgr_purge_enabled is off without the key (Go decision).
		VisitorHourlyCount: true, XLogIDIndexMB: 1, IndexTraversalWarningCount: 100, CompressReadCacheBlocks: 3, CompressDailyCountCacheSize: 3,
		Purge: map[string]int{}, LogRotation: true, LogKeepDays: 31, SQLTableParsing: true, LogUDP: map[string]bool{}}
	d.effective = nil
	if !reflect.DeepEqual(d, want) {
		t.Fatalf("defaults\n%+v\nwant\n%+v", d, want)
	}
	s, _, _ := openTest(t, "object_deadtime_ms=-5\nobject_zipkin_deadtime_ms=abc\nobject_inactive_alert_level=2\ntagcnt_enabled=TRUE\n"+
		"_auto_5m_sampling=yes\nxlog_sampling_matcher_xlog_keep_memory_millis=0\nxlog_sampling_matcher_profile_keep_memory_secs=-1\n"+
		"geoip_data_city_file= /x/y.dat \nnet_tcp_get_agent_connection_wait_ms=0\n", Options{})
	l := s.Current().Live()
	if l.ObjectDeadTimeMs != -5 || l.ObjectZipkinDeadTimeMs != 300000 || l.ObjectInactiveAlertLevel != 2 || !l.TagCountEnabled ||
		l.Auto5MSampling || l.SamplingXLogKeepMillis != 5000 || l.SamplingProfileKeepSecs != 5 || l.GeoIPDataCityFile != "/x/y.dat" || l.AgentConnectionWaitMs != 0 {
		t.Fatalf("parsed %+v", l)
	}
	if e := l.Effective(); e["xlog_sampling_matcher_xlog_keep_memory_millis"] != "5000" || e["object_deadtime_ms"] != "-5" || e["_auto_5m_sampling"] != "false" {
		t.Fatalf("effective %v", e)
	}
}

// A reload changes Live (no restart pin); a flag-pinned key keeps the flag
// value and its file change is logged once.
func TestLiveReloadAndPinned(t *testing.T) {
	clk := &clock{now: time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)}
	var changes []Change
	s, path, sink := openTest(t, "object_deadtime_ms=9000\ntagcnt_enabled=false\n", Options{Now: clk.Now,
		Pinned:   map[string]string{"tagcnt_enabled": "true"},
		OnChange: func(c Change) { changes = append(changes, c) }})
	l := s.Current().Live()
	if l.ObjectDeadTimeMs != 9000 || !l.TagCountEnabled {
		t.Fatalf("startup %+v", l)
	}
	rewrite(t, path, "object_deadtime_ms=12000\ntagcnt_enabled=FALSE\n", clk.Now().Add(time.Minute))
	if _, err := s.Reload(true); err != nil {
		t.Fatal(err)
	}
	l = s.Current().Live()
	if l.ObjectDeadTimeMs != 12000 || !l.TagCountEnabled || len(changes) != 1 {
		t.Fatalf("reloaded %+v changes %v", l, changes)
	}
	if !sink.contains("tagcnt_enabled changed") || sink.contains("object_deadtime_ms changed") {
		t.Fatalf("log %v", sink.lines)
	}
	// The reload line names only keys now in force, not the pinned one.
	for _, line := range sink.lines {
		if containsAll(line, "config: reloaded ") && (!containsAll(line, "object_deadtime_ms") || containsAll(line, "tagcnt_enabled")) {
			t.Fatalf("reload line %q", line)
		}
	}
	if !sink.contains("config: reloaded ") {
		t.Fatalf("no reload line: %v", sink.lines)
	}
	n := len(sink.lines)
	rewrite(t, path, "object_deadtime_ms=12000\ntagcnt_enabled=false\n", clk.Now().Add(2*time.Minute))
	if _, err := s.Reload(true); err != nil {
		t.Fatal(err)
	}
	for _, line := range sink.lines[n:] {
		if line == "" || containsAll(line, "tagcnt_enabled", "fixed by a command-line flag") {
			t.Fatalf("pinned key logged twice: %q", line)
		}
	}
	if s.Current().Pinned()["tagcnt_enabled"] != "true" {
		t.Fatal("Pinned")
	}
}

func containsAll(s string, parts ...string) bool {
	for _, p := range parts {
		found := false
		for i := 0; i+len(p) <= len(s); i++ {
			if s[i:i+len(p)] == p {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// The keys wired after F0: server_id/log_dir are live and pinnable, the Go
// retention default, the webapp pool timeout derivation, LDAP, the
// log_udp_* switches, mgr_log_ignore_ids and the Effective overlay.
func TestLiveWiredKeys(t *testing.T) {
	clk := &clock{now: time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)}
	s, path, sink := openTest(t, "server_id=a\nlog_dir=/l1\nnet_tcp_client_so_timeout_ms=4000\nlog_udp_xlog=true\nlog_udp_text=false\n"+
		"mgr_log_ignore_ids= S1 ,S2,,S1\naccount_use_ldap=true\naccount_ldap_provider_url=ldap://x\n", Options{Now: clk.Now, HostName: "h",
		Pinned: map[string]string{"log_dir": "/flag"}})
	l := s.Current().Live()
	if l.ServerID != "a" || l.LogDir != "/flag" || l.Webapp.PoolTimeoutMs != 4000 || l.PurgeEnabled || len(l.Purge) != 0 ||
		!l.LogUDP["log_udp_xlog"] || l.LogUDP["log_udp_text"] || len(l.LogUDP) != 1 || !reflect.DeepEqual(l.LogIgnoreIDs, []string{"S1", "S2"}) ||
		!l.LDAP.Enabled || l.LDAP.ProviderURL != "ldap://x" || l.LDAP.Auth != "simple" {
		t.Fatalf("startup %+v", l)
	}
	e := l.Effective()
	if e["log_dir"] != "/flag" || e["mgr_purge_enabled"] != "false" || e["net_webapp_tcp_client_pool_timeout"] != "4000" || e["mgr_log_ignore_ids"] != "{S1, S2}" {
		t.Fatalf("effective %v", e)
	}
	rewrite(t, path, "server_id=b\nlog_dir=/l2\nmgr_purge_enabled=true\nmgr_purge_keep_days=3\nnet_webapp_tcp_client_pool_timeout=900\n", clk.Now().Add(time.Minute))
	if _, err := s.Reload(true); err != nil {
		t.Fatal(err)
	}
	l = s.Current().Live()
	if l.ServerID != "b" || l.LogDir != "/flag" || !l.PurgeEnabled || l.Purge["mgr_purge_keep_days"] != 3 || l.Webapp.PoolTimeoutMs != 900 || l.LDAP.Enabled {
		t.Fatalf("reloaded %+v", l)
	}
	if sink.contains("restart required") {
		t.Fatalf("live keys reported as restart keys: %v", sink.lines)
	}
	if e := l.Effective(); e["server_id"] != "b" || e["mgr_purge_counter_keep_days"] != "21" || e["mgr_purge_enabled"] != "true" {
		t.Fatalf("effective %v", e)
	}
	// Startup-only keys stay restart keys.
	for _, k := range []string{"net_udp_packet_buffer_size", "_net_udp_worker_thread_count", "_mgr_kv_store_index_default_mb", "_mgr_text_db_index_default_mb", "db_dir"} {
		if key, _ := Lookup(k); !key.Restart || key.Live {
			t.Fatalf("%s must be a restart key", k)
		}
	}
	if DefaultLive().ServerID != "" {
		t.Fatal("an empty snapshot has no host name")
	}
}
