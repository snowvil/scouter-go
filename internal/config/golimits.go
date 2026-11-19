package config

import (
	"fmt"
	"strconv"
)

// GoLimits are the Go-only resource bounds (go_* keys, CONTRACT.md §6). They
// are not Configure fields, so they never appear in LIST_CONFIGURE_SERVER.
// A zero field means the key is absent, 0 or invalid and the server keeps
// its built-in default; every other valid value is at least 1. A literal 0
// selects the default silently (like go_maxprocs); any other invalid value
// (not an int, or outside the key's range) is logged by the Store when the
// file is loaded or changes. The ranges and their reasons are in
// CONTRACT.md §6.
//
// Restart keys (goLimit.restart) are read once by server.Open; a later file
// change is logged as "restart required" and Current() keeps the startup
// value, so Live().Limits always holds what is in force.
type GoLimits struct {
	// HTTP admission (server http_admission.go), restart: connections per
	// listener (still lowered to fit the open file limit), servlet and webapp
	// requests served at once and their slot waits, /health + pprof requests.
	HTTPMaxConnections                          int // go_http_max_connections
	HTTPServletInFlight, HTTPServletQueueWaitMs int // go_http_servlet_max_inflight, go_http_servlet_queue_wait_ms
	HTTPWebappInFlight, HTTPWebappQueueWaitMs   int // go_http_webapp_max_inflight, go_http_webapp_queue_wait_ms
	HTTPOpsInFlight                             int // go_http_ops_max_inflight
	// Agent relay budget (server agent_relay_budget.go), live: the byte
	// budget in MiB, the large relays at once and the wait of queued server
	// calls.
	AgentRelayBudgetMB     int // go_agent_relay_budget_mb
	AgentRelayMaxLarge     int // go_agent_relay_max_large
	AgentRelayServerWaitMs int // go_agent_relay_server_wait_ms
	// ClientResponseBudgetMB is the transport Client response budget in MiB
	// (TCPConfig.MaxClientResponseBytes), restart.
	ClientResponseBudgetMB int // go_client_response_budget_mb
	// Pulse queue byte bounds in MiB (pulse.Options), restart.
	PulseXLogQueueMB, PulseProfileQueueMB int // go_pulse_xlog_queue_mb, go_pulse_profile_queue_mb
	// TextCacheResetParallel bounds the concurrent OBJECT_RESET_CACHE calls
	// (server text_cache_reset.go), live.
	TextCacheResetParallel int // go_text_cache_reset_parallel
	// TextMaxUnknownDivs bounds the divs outside scouter.lang.TextTypes with
	// a permanent text dictionary (storage Store.SetMaxUnknownTextDivs, R7),
	// live.
	TextMaxUnknownDivs int // go_text_max_unknown_divs
	// TextCacheMB bounds the storage text read cache in MiB
	// (storage.Config.TextCacheBytes), restart.
	TextCacheMB int // go_text_cache_mb
	// Visitor HLL memory budgets in MiB (storage Store.SetVisitorBudgets),
	// live: the resident HLLs and the deltas waiting for the flush.
	VisitorResidentMB, VisitorDeltaMB int // go_visitor_resident_mb, go_visitor_delta_mb
}

type goLimit struct {
	name     string
	min, max int
	restart  bool
	field    func(*GoLimits) *int
}

// goLimitKeys lists the GoLimits keys with their valid ranges.
var goLimitKeys = []goLimit{
	{"go_http_max_connections", 16, 65536, true, func(l *GoLimits) *int { return &l.HTTPMaxConnections }},
	{"go_http_servlet_max_inflight", 1, 1024, true, func(l *GoLimits) *int { return &l.HTTPServletInFlight }},
	{"go_http_servlet_queue_wait_ms", 1, 60000, true, func(l *GoLimits) *int { return &l.HTTPServletQueueWaitMs }},
	{"go_http_webapp_max_inflight", 1, 256, true, func(l *GoLimits) *int { return &l.HTTPWebappInFlight }},
	{"go_http_webapp_queue_wait_ms", 1, 60000, true, func(l *GoLimits) *int { return &l.HTTPWebappQueueWaitMs }},
	{"go_http_ops_max_inflight", 1, 1024, true, func(l *GoLimits) *int { return &l.HTTPOpsInFlight }},
	{"go_agent_relay_budget_mb", 768, 1 << 20, false, func(l *GoLimits) *int { return &l.AgentRelayBudgetMB }},
	{"go_agent_relay_max_large", 1, 64, false, func(l *GoLimits) *int { return &l.AgentRelayMaxLarge }},
	{"go_agent_relay_server_wait_ms", 1, 60000, false, func(l *GoLimits) *int { return &l.AgentRelayServerWaitMs }},
	{"go_client_response_budget_mb", 257, 1 << 20, true, func(l *GoLimits) *int { return &l.ClientResponseBudgetMB }},
	{"go_pulse_xlog_queue_mb", 1, 4096, true, func(l *GoLimits) *int { return &l.PulseXLogQueueMB }},
	{"go_pulse_profile_queue_mb", 1, 4096, true, func(l *GoLimits) *int { return &l.PulseProfileQueueMB }},
	{"go_text_cache_reset_parallel", 1, 64, false, func(l *GoLimits) *int { return &l.TextCacheResetParallel }},
	{"go_text_max_unknown_divs", 1, 4096, false, func(l *GoLimits) *int { return &l.TextMaxUnknownDivs }},
	{"go_text_cache_mb", 16, 8192, true, func(l *GoLimits) *int { return &l.TextCacheMB }},
	{"go_visitor_resident_mb", 1, 16384, false, func(l *GoLimits) *int { return &l.VisitorResidentMB }},
	{"go_visitor_delta_mb", 16, 1024, false, func(l *GoLimits) *int { return &l.VisitorDeltaMB }},
}

// parse returns the value of text (0: the default), or an error naming the
// valid range.
func (k *goLimit) parse(text string) (int, error) {
	n, err := strconv.ParseInt(text, 10, 32)
	if err == nil && n == 0 {
		return 0, nil
	}
	if err != nil || int(n) < k.min || int(n) > k.max {
		return 0, fmt.Errorf("%s=%q is not an integer in %d..%d", k.name, text, k.min, k.max)
	}
	return int(n), nil
}

// invalid is the log line of an invalid value: what the server does with it.
// A restart key changed while running keeps its running value until then.
func (k *goLimit) invalid(err error, running bool) string {
	if running && k.restart {
		return fmt.Sprintf("config: %v; restart required, the Go default applies after a restart", err)
	}
	return fmt.Sprintf("config: %v; the Go default applies", err)
}

// logInvalidGoLimits logs the go_* limits of get that are invalid.
func logInvalidGoLimits(get func(string) (string, bool), running bool, logf func(string, ...any)) {
	for i := range goLimitKeys {
		k := &goLimitKeys[i]
		if v, ok := get(k.name); ok {
			if _, err := k.parse(v); err != nil {
				logf("%s", k.invalid(err, running))
			}
		}
	}
}

// GoLimits returns the valid go_* limit values of this snapshot.
func (s *Snapshot) GoLimits() GoLimits {
	var l GoLimits
	for i := range goLimitKeys {
		k := &goLimitKeys[i]
		if text, ok := s.Get(k.name); ok {
			if n, err := k.parse(text); err == nil {
				*k.field(&l) = n
			}
		}
	}
	return l
}
