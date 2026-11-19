package config

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Every go_* limit is known (never reported as unknown), is not a Configure
// key (LIST_CONFIGURE_SERVER), and without the key keeps the Go default (0).
func TestGoLimitsKnownAndDefault(t *testing.T) {
	if (DefaultLive().Limits != GoLimits{}) {
		t.Fatalf("defaults %+v", DefaultLive().Limits)
	}
	text := ""
	for _, k := range goLimitKeys {
		text += k.name + "=" + strconv.Itoa(k.min) + "\n"
		if _, ok := Lookup(k.name); ok {
			t.Fatalf("%s must not be a Configure key", k.name)
		}
		if k.min < 1 {
			t.Fatalf("%s: zero must stay the default marker", k.name)
		}
	}
	s, _, sink := openTest(t, text, Options{})
	if u := s.Current().Unknown(); len(u) != 0 || sink.contains("unknown key") {
		t.Fatalf("unknown %v %v", u, sink.lines)
	}
	l := s.Current().Live().Limits
	for _, k := range goLimitKeys {
		if got := *k.field(&l); got != k.min {
			t.Fatalf("%s = %d, want %d", k.name, got, k.min)
		}
	}
}

func TestGoLimitsParsingAndRange(t *testing.T) {
	s, _, sink := openTest(t, "go_http_max_connections=2048\ngo_http_servlet_max_inflight=abc\ngo_http_webapp_max_inflight=0\n"+
		"go_agent_relay_budget_mb=63\ngo_agent_relay_max_large=65\ngo_text_cache_reset_parallel= 32 \n"+
		"go_client_response_budget_mb=4096\ngo_pulse_xlog_queue_mb=-1\ngo_pulse_profile_queue_mb=4096\n"+
		"go_agent_relay_server_wait_ms=99999999999\ngo_http_servlet_queue_wait_ms=60000\n", Options{})
	got := s.Current().GoLimits()
	want := GoLimits{HTTPMaxConnections: 2048, TextCacheResetParallel: 32, ClientResponseBudgetMB: 4096, PulseProfileQueueMB: 4096, HTTPServletQueueWaitMs: 60000}
	if got != want {
		t.Fatalf("limits\n%+v\nwant\n%+v", got, want)
	}
	for _, k := range []string{"go_http_servlet_max_inflight", "go_agent_relay_budget_mb", "go_agent_relay_max_large", "go_pulse_xlog_queue_mb", "go_agent_relay_server_wait_ms"} {
		if !sink.contains("config: "+k+"=") || !sink.contains("; the Go default applies") {
			t.Fatalf("%s: invalid value not logged: %v", k, sink.lines)
		}
	}
	// A literal 0 is the default, silently (like go_maxprocs).
	for _, k := range []string{"go_http_max_connections", "go_text_cache_reset_parallel", "go_client_response_budget_mb", "go_http_webapp_max_inflight"} {
		if sink.contains("config: " + k + "=") {
			t.Fatalf("%s: valid value logged: %v", k, sink.lines)
		}
	}
}

// The visitor budgets: both bounds of each range are valid, a value past
// either bound or not an integer is logged and keeps the default, 0 is the
// default silently; on reload they apply live (no restart required).
func TestGoLimitsVisitorBudgets(t *testing.T) {
	for _, c := range []struct {
		text   string
		want   GoLimits
		logged []string
	}{
		{"go_visitor_resident_mb=1\ngo_visitor_delta_mb=16\n", GoLimits{VisitorResidentMB: 1, VisitorDeltaMB: 16}, nil},
		{"go_visitor_resident_mb=16384\ngo_visitor_delta_mb=1024\n", GoLimits{VisitorResidentMB: 16384, VisitorDeltaMB: 1024}, nil},
		{"go_visitor_resident_mb=0\ngo_visitor_delta_mb=0\n", GoLimits{}, nil},
		{"go_visitor_resident_mb=16385\ngo_visitor_delta_mb=15\n", GoLimits{}, []string{
			`config: go_visitor_resident_mb="16385" is not an integer in 1..16384; the Go default applies`,
			`config: go_visitor_delta_mb="15" is not an integer in 16..1024; the Go default applies`}},
		{"go_visitor_resident_mb=-1\ngo_visitor_delta_mb=64MB\n", GoLimits{}, []string{
			`config: go_visitor_resident_mb="-1" is not an integer in 1..16384; the Go default applies`,
			`config: go_visitor_delta_mb="64MB" is not an integer in 16..1024; the Go default applies`}},
	} {
		s, _, sink := openTest(t, c.text, Options{})
		if got := s.Current().GoLimits(); got != c.want {
			t.Fatalf("%q: limits %+v, want %+v", c.text, got, c.want)
		}
		for _, line := range c.logged {
			if !sink.contains(line) {
				t.Fatalf("%q: %q not logged: %v", c.text, line, sink.lines)
			}
		}
		if n := countLines(sink, "go_visitor_"); n != len(c.logged) {
			t.Fatalf("%q: %d go_visitor_ lines, want %d: %v", c.text, n, len(c.logged), sink.lines)
		}
	}
	s, path, sink := openTest(t, "go_visitor_resident_mb=512\n", Options{})
	rewrite(t, path, "go_visitor_resident_mb=64\ngo_visitor_delta_mb=32\n", time.Now().Add(-time.Hour))
	c, err := s.Reload(true)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Ignored) != 0 || !reflect.DeepEqual(c.Applied, []string{"go_visitor_delta_mb", "go_visitor_resident_mb"}) {
		t.Fatalf("change %+v", c)
	}
	if got := s.Current().Live().Limits; got != (GoLimits{VisitorResidentMB: 64, VisitorDeltaMB: 32}) {
		t.Fatalf("after reload %+v", got)
	}
	if sink.contains("restart required") {
		t.Fatalf("a live key asked for a restart: %v", sink.lines)
	}
}

// Live keys change on reload; restart keys keep the startup value and are
// reported as Ignored. An invalid value is logged once per change.
func TestGoLimitsReload(t *testing.T) {
	var changes []Change
	s, path, sink := openTest(t, "go_http_max_connections=2048\ngo_agent_relay_budget_mb=1024\n", Options{OnChange: func(c Change) { changes = append(changes, c) }})
	base := time.Now().Add(-time.Hour)
	rewrite(t, path, "go_http_max_connections=4096\ngo_agent_relay_budget_mb=2048\ngo_agent_relay_max_large=4\ngo_text_cache_reset_parallel=8\ngo_agent_relay_server_wait_ms=2000\n", base)
	c, err := s.Reload(true)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Ignored, []string{"go_http_max_connections"}) ||
		!reflect.DeepEqual(c.Applied, []string{"go_agent_relay_budget_mb", "go_agent_relay_max_large", "go_agent_relay_server_wait_ms", "go_text_cache_reset_parallel"}) {
		t.Fatalf("change %+v", c)
	}
	want := GoLimits{HTTPMaxConnections: 2048, AgentRelayBudgetMB: 2048, AgentRelayMaxLarge: 4, AgentRelayServerWaitMs: 2000, TextCacheResetParallel: 8}
	if got := s.Current().Live().Limits; got != want {
		t.Fatalf("after reload %+v, want %+v", got, want)
	}
	if !sink.contains("go_http_max_connections changed") || !sink.contains("restart required") {
		t.Fatalf("logs %v", sink.lines)
	}
	// An invalid live value returns to the default and is logged once.
	rewrite(t, path, "go_http_max_connections=4096\ngo_agent_relay_budget_mb=1\n", base.Add(time.Minute))
	if _, err := s.Reload(true); err != nil {
		t.Fatal(err)
	}
	if got := s.Current().Live().Limits; got != (GoLimits{HTTPMaxConnections: 2048}) {
		t.Fatalf("invalid reload %+v", got)
	}
	n := countLines(sink, "go_agent_relay_budget_mb=\"1\"")
	if sink.contains("go_agent_relay_budget_mb=\"1\" is not an integer in 768..1048576; restart required") {
		t.Fatal("a live key must not ask for a restart")
	}
	rewrite(t, path, "go_http_max_connections=4096\ngo_agent_relay_budget_mb=1\ngo_text_cache_reset_parallel=2\n", base.Add(2*time.Minute))
	if _, err := s.Reload(true); err != nil {
		t.Fatal(err)
	}
	if n != 1 || countLines(sink, "go_agent_relay_budget_mb=\"1\"") != 1 {
		t.Fatalf("invalid value logged %d then %d times: %v", n, countLines(sink, "go_agent_relay_budget_mb=\"1\""), sink.lines)
	}
}

// An invalid restart key changed while running keeps the running value
// until a restart, and the line says so.
func TestGoLimitsInvalidRestartKeyWhileRunning(t *testing.T) {
	s, path, sink := openTest(t, "go_http_ops_max_inflight=8\n", Options{})
	rewrite(t, path, "go_http_ops_max_inflight=0x10\n", time.Now().Add(-time.Hour))
	if _, err := s.Reload(true); err != nil {
		t.Fatal(err)
	}
	if got := s.Current().GoLimits().HTTPOpsInFlight; got != 8 {
		t.Fatalf("running value %d", got)
	}
	if !sink.contains(`go_http_ops_max_inflight="0x10" is not an integer in 1..1024; restart required, the Go default applies after a restart`) {
		t.Fatalf("logs %v", sink.lines)
	}
}

// The environment fallback (Java merges System.getenv) applies to go_*
// limits too, also without scouter.conf; an invalid one is logged at
// startup.
func TestGoLimitsEnvironment(t *testing.T) {
	env := map[string]string{"go_text_cache_reset_parallel": "4", "go_http_ops_max_inflight": "x"}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	for _, text := range []string{"a=1\n", ""} {
		s, _, sink := openTest(t, text, Options{Lookup: lookup})
		if got := s.Current().GoLimits(); got != (GoLimits{TextCacheResetParallel: 4}) {
			t.Fatalf("file %q: env limits %+v", text, got)
		}
		if !sink.contains(`go_http_ops_max_inflight="x" is not an integer in 1..1024; the Go default applies`) || countLines(sink, "go_") != 1 {
			t.Fatalf("file %q: logs %v", text, sink.lines)
		}
	}
}

func countLines(l *logSink, s string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if strings.Contains(line, s) {
			n++
		}
	}
	return n
}
