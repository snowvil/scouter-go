package service

// Settings are the scouter.conf values the Java handlers read through
// Configure.getInstance() on every request (docs/compatibility/
// config-behavior.md §5.1 V1·V2). The owner of the configuration stores a
// new value with Service.SetSettings after each reload; the next request
// uses it. Values are the applied Java values (getValue trims, getInt falls
// back to the default); this package does not parse scouter.conf.
type Settings struct {
	// ServerID is server_id: LOGIN "server_id" (LoginService.scala:63,127).
	ServerID string
	// ExtLinkName and ExtLinkURLPattern are ext_link_name and
	// ext_link_url_pattern: LOGIN entries of the same names (LoginService.scala:81-82).
	ExtLinkName, ExtLinkURLPattern string
	// ClientSoTimeoutMillis is net_tcp_client_so_timeout_ms: LOGIN
	// "so_time_out" (LoginService.scala:80). It does not change the TCP
	// timeouts here (transport owns them).
	ClientSoTimeoutMillis int32
	// TagCountEnabled is tagcnt_enabled: LOGIN menu "tag_count" (LoginService.scala:79).
	TagCountEnabled bool
	// XLogRealtimeLowerBoundMillis and XLogPasttimeLowerBoundMillis are
	// xlog_realtime_lower_bound_ms and xlog_pasttime_lower_bound_ms: the
	// TRANX_REAL_TIME_GROUP / TRANX_LOAD_TIME_GROUP "limit" becomes
	// max(bound, limit) (XLogService.scala:176-177,265-266).
	XLogRealtimeLowerBoundMillis, XLogPasttimeLowerBoundMillis int32
	// SearchXLogMaxCount is req_search_xlog_max_count. XLOG_LOAD_BY_TXIDS
	// stops when the 1-based attempt count reaches it (at most count-1 ids,
	// XLogService.scala:472-476); SEARCH_XLOG_LIST returns at most count
	// XLogs (:602-605). Zero or negative returns nothing, as in Java.
	SearchXLogMaxCount int32
}

// Java Configure defaults (Configure.java:203,386,394,404,406,438,442).
const (
	DefaultExtLinkName       = "scouter-paper"
	DefaultExtLinkURLPattern = "http://my-scouter-paper-ip:6188/index.html#/paper?&address=localhost&port=6188&realtime=false&xlogElapsedTime=8000&instances=$[objHashes]&from=$[from]&to=$[to]&layout=my-layout-template-01"
	DefaultClientSoTimeout   = 8000
	DefaultSearchXLogMax     = 500
)

// DefaultSettings returns the Java Configure defaults. server_id defaults to
// the host name in Java; the caller supplies it.
func DefaultSettings(serverID string) Settings {
	return Settings{
		ServerID:              serverID,
		ExtLinkName:           DefaultExtLinkName,
		ExtLinkURLPattern:     DefaultExtLinkURLPattern,
		ClientSoTimeoutMillis: DefaultClientSoTimeout,
		TagCountEnabled:       true,
		SearchXLogMaxCount:    DefaultSearchXLogMax,
	}
}

// SetSettings replaces the live settings. It is safe to call concurrently
// with request handling; a request reads one consistent Settings value.
func (s *Service) SetSettings(v Settings) { s.settings.Store(&v) }

// Settings returns the live settings.
func (s *Service) Settings() Settings { return *s.settings.Load() }

// lowerBound is Math.max(bound, limit) of XLogService. With the default
// bound 0 a negative request limit becomes 0; a negative bound passes a
// negative limit through, as in Java.
func lowerBound(bound, limit int32) int32 {
	if bound > limit {
		return bound
	}
	return limit
}
