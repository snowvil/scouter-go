package config

import "strconv"

// Live is the typed view of the Configure keys the Go server applies while
// it runs (F0, docs/compatibility/config-behavior.md §5.0). Store.OnChange
// hands Current().Live() to server.Runtime.ApplySettings, which only stores
// it atomically and signals its owner goroutine; consumers receive setters
// or getter functions, never a config dependency.
//
// Values follow applyConfig (getInt/getBoolean/getValue with the
// applyConfig default). A key fixed by a command-line flag (Options.Pinned)
// keeps the flag value. Go normalizes values that would make a Java cache
// unbounded or throw (see the field comments); Effective reports the value
// actually applied so LIST_CONFIGURE_SERVER can show it.
//
// A Live value is immutable once built: the slice and map fields are shared
// by every copy and must not be modified.
type Live struct {
	// AgentManager (I1): object_deadtime_ms, object_zipkin_deadtime_ms
	// (applyConfig default 300000), object_inactive_alert_level.
	ObjectDeadTimeMs, ObjectZipkinDeadTimeMs int
	ObjectInactiveAlertLevel                 int
	// XLogQueueSize and ProfileQueueSize are xlog_queue_size and
	// profile_queue_size: the XLogWR / XLogProfileWR queue capacities, read
	// once at start like the original's RequestQueue (storage writer queues).
	XLogQueueSize, ProfileQueueSize int
	// TagCountEnabled is tagcnt_enabled (XLogCore/AlertCore tag count, I2;
	// LOGIN menu tag_count, V1).
	TagCountEnabled bool
	// Auto5MSampling is _auto_5m_sampling (Auto5MSampling tick, I4).
	Auto5MSampling bool
	// XLog consequent sampling (I5). Java reads the gxid count once at
	// startup, the xlog/profile counts when a bucket is created and the
	// millis/secs on every delayed pack. A value <= 0 makes the Java map
	// unbounded (count) or throws (millis, secs); Go uses the default.
	SamplingGxidKeepCount, SamplingXLogKeepCount, SamplingXLogKeepMillis int
	SamplingProfileKeepCount, SamplingProfileKeepSecs                    int
	// GeoIP (I7): geoip_enabled per XLog, geoip_data_city_file reloaded by
	// the runtime owner goroutine when it changes.
	GeoIPEnabled      bool
	GeoIPDataCityFile string
	// Agent TCP sessions (T3): net_tcp_agent_keepalive_interval_ms is the
	// idle time since the last write before KEEP_ALIVE,
	// net_tcp_get_agent_connection_wait_ms the wait for an idle session.
	AgentKeepAliveIntervalMs, AgentConnectionWaitMs int

	// ServerID is server_id (LOGIN server_id, V1); the default is the host
	// name (Options.HostName).
	ServerID string
	// ExtLinkName and ExtLinkURLPattern are ext_link_name and
	// ext_link_url_pattern (LOGIN, V1).
	ExtLinkName, ExtLinkURLPattern string
	// ClientSoTimeoutMs is net_tcp_client_so_timeout_ms: LOGIN so_time_out
	// (V1), the Client read timeout of new connections (T1) and the
	// default of net_webapp_tcp_client_pool_timeout (W1).
	ClientSoTimeoutMs int
	// XLogRealtimeLowerBoundMs, XLogPasttimeLowerBoundMs and
	// SearchXLogMaxCount are xlog_realtime_lower_bound_ms,
	// xlog_pasttime_lower_bound_ms and req_search_xlog_max_count (V2).
	XLogRealtimeLowerBoundMs, XLogPasttimeLowerBoundMs, SearchXLogMaxCount int
	// LogDir is log_dir: the server/request log directory (L1) and the
	// SERVER_LOG_LIST/SERVER_LOG_DETAIL root (V3).
	LogDir string

	// Transport (T2, T5, T6): net_tcp_agent_so_timeout_ms,
	// net_tcp_service_pool_size, log_tcp_action_enabled,
	// log_udp_multipacket, log_expired_multipacket.
	AgentSoTimeoutMs, ServicePoolSize                      int
	LogTCPAction, LogUDPMultipacket, LogExpiredMultipacket bool

	// GoMaxProcs is go_maxprocs (Go-only, KeyGoMaxProcs); a negative or
	// unparsable value is 0 (the startup value).
	GoMaxProcs int
	// Limits are the Go-only go_* resource bounds (zero: the Go default).
	Limits GoLimits

	// Webapp holds the keys the scouter.webapp REST API reads per request (W1).
	Webapp WebappLive
	// LDAP holds the AccountManager.authorizeAccount keys read on every
	// LOGIN (A1). They are not Configure fields, so they are not in
	// LiveKeys or Effective.
	LDAP LDAP

	// Storage (S1, S2, S3, S5, I3, _compress_*).
	TextDailyService, TextDailyAPI, TextDailyUA bool
	CompressXLog, CompressProfile               bool
	VisitorHourlyCount                          bool
	XLogIDIndexMB                               int
	IndexTraversalWarningCount                  int
	CompressReadCacheBlocks                     int
	CompressDailyCountCacheSize                 int
	// PurgeEnabled is mgr_purge_enabled. Go decision: without the key it is
	// false (Java default true; deletion is opt-in), CONTRACT.md §6.
	PurgeEnabled bool
	// Purge holds the mgr_purge_* keys set to a valid int (pins included):
	// the input of storage.JavaRetentionConfig.
	Purge map[string]int

	// Server log (L1): log_rotation_enabled, log_keep_days, _trace and
	// mgr_log_ignore_ids (split, trimmed, first occurrence order).
	LogRotation  bool
	LogKeepDays  int
	Trace        bool
	LogIgnoreIDs []string

	// SqlTables (I6): sql_table_parsing_enabled, log_sql_parsing_fail_enabled.
	SQLTableParsing, LogSQLParsingFail bool

	// NetDataProcessor debug output (I8): LogUDP holds the log_udp_* keys
	// (UDPLogKeys) that are true; LogUDPSomeObject is log_udp_some_object
	// and LogTestRate log_test_rate (ProfilePreCore sampling percent).
	LogUDP           map[string]bool
	LogUDPSomeObject string
	LogTestRate      int

	effective map[string]string
}

// WebappLive is the webapp part of Live (webapp.Settings).
type WebappLive struct {
	CORSAllowOrigin, CORSAllowCredentials string // net_http_api_cors_allow_origin/_credentials
	AuthIPEnabled                         bool   // net_http_api_auth_ip_enabled
	AllowIPs                              string // net_http_api_allow_ips
	AuthIPHeaderKey                       string // net_http_api_auth_ip_header_key
	AuthSessionEnabled, AuthBearerEnabled bool   // net_http_api_auth_session_enabled/_bearer_token_enabled
	// PoolTimeoutMs is net_webapp_tcp_client_pool_timeout; without the key
	// applyConfig uses the current net_tcp_client_so_timeout_ms.
	PoolTimeoutMs int
}

// UDPLogKeys are the per-pack NetDataProcessor stdout switches (§1.8):
// log_udp_packet before the type dispatch, the others after it.
var UDPLogKeys = []string{
	"log_udp_packet", "log_udp_counter", "log_udp_interaction_counter", "log_udp_xlog", "log_udp_profile",
	"log_udp_text", "log_udp_alert", "log_udp_object", "log_udp_status", "log_udp_stack",
	"log_udp_summary", "log_udp_batch", "log_udp_span",
}

// LiveKeys are the Configure keys Live covers, in Keys order.
var LiveKeys = func() []string {
	var out []string
	for _, k := range Keys {
		if k.Live {
			out = append(out, k.Name)
		}
	}
	return out
}()

// DefaultLive is Live for an empty configuration.
func DefaultLive() Live { return (&Snapshot{}).Live() }

// Effective returns the applied text of every live key (a copy).
func (l Live) Effective() map[string]string {
	out := make(map[string]string, len(l.effective))
	for k, v := range l.effective {
		out[k] = v
	}
	return out
}

// Live returns the live settings of this snapshot with the pinned (flag)
// values in place of the file values.
func (s *Snapshot) Live() Live {
	v := s
	if len(s.pinned) > 0 {
		c := *s
		c.values = make(map[string]string, len(s.values)+len(s.pinned))
		for k, x := range s.values {
			c.values[k] = x
		}
		for k, x := range s.pinned {
			c.values[k] = x
		}
		v = &c
	}
	positive := func(key string, def int) int {
		if n := v.Int(key, def); n > 0 {
			return n
		}
		return def
	}
	value := func(key string) string {
		x, _ := v.Value(key)
		return x
	}
	atoi := func(key string) int {
		n, _ := strconv.Atoi(value(key))
		return n
	}
	boolean := func(key string) bool { return value(key) == "true" }
	srv := v.Server()
	l := Live{
		ObjectDeadTimeMs:         atoi("object_deadtime_ms"),
		ObjectZipkinDeadTimeMs:   atoi("object_zipkin_deadtime_ms"),
		ObjectInactiveAlertLevel: atoi("object_inactive_alert_level"),
		TagCountEnabled:          boolean("tagcnt_enabled"),
		XLogQueueSize:            min(positive("xlog_queue_size", 10000), 1<<20),
		ProfileQueueSize:         min(positive("profile_queue_size", 1000), 1<<20),
		Auto5MSampling:           boolean("_auto_5m_sampling"),
		SamplingGxidKeepCount:    positive("xlog_sampling_matcher_gxid_keep_memory_count", 500000),
		SamplingXLogKeepCount:    positive("xlog_sampling_matcher_xlog_keep_memory_count", 100000),
		SamplingXLogKeepMillis:   positive("xlog_sampling_matcher_xlog_keep_memory_millis", 5000),
		SamplingProfileKeepCount: positive("xlog_sampling_matcher_profile_keep_memory_count", 5000),
		SamplingProfileKeepSecs:  positive("xlog_sampling_matcher_profile_keep_memory_secs", 5),
		GeoIPEnabled:             boolean("geoip_enabled"),
		GeoIPDataCityFile:        value("geoip_data_city_file"),
		AgentKeepAliveIntervalMs: atoi("net_tcp_agent_keepalive_interval_ms"),
		AgentConnectionWaitMs:    atoi("net_tcp_get_agent_connection_wait_ms"),

		ServerID:                 value("server_id"),
		ExtLinkName:              value("ext_link_name"),
		ExtLinkURLPattern:        value("ext_link_url_pattern"),
		ClientSoTimeoutMs:        atoi("net_tcp_client_so_timeout_ms"),
		XLogRealtimeLowerBoundMs: atoi("xlog_realtime_lower_bound_ms"),
		XLogPasttimeLowerBoundMs: atoi("xlog_pasttime_lower_bound_ms"),
		SearchXLogMaxCount:       atoi("req_search_xlog_max_count"),
		LogDir:                   value("log_dir"),

		AgentSoTimeoutMs:      atoi("net_tcp_agent_so_timeout_ms"),
		ServicePoolSize:       atoi("net_tcp_service_pool_size"),
		LogTCPAction:          boolean("log_tcp_action_enabled"),
		LogUDPMultipacket:     boolean("log_udp_multipacket"),
		LogExpiredMultipacket: boolean("log_expired_multipacket"),
		GoMaxProcs:            max(v.Int(KeyGoMaxProcs, 0), 0),
		Limits:                v.GoLimits(),

		Webapp: WebappLive{
			CORSAllowOrigin:      value("net_http_api_cors_allow_origin"),
			CORSAllowCredentials: value("net_http_api_cors_allow_credentials"),
			AuthIPEnabled:        boolean("net_http_api_auth_ip_enabled"),
			AllowIPs:             value("net_http_api_allow_ips"),
			AuthIPHeaderKey:      value("net_http_api_auth_ip_header_key"),
			AuthSessionEnabled:   boolean("net_http_api_auth_session_enabled"),
			AuthBearerEnabled:    boolean("net_http_api_auth_bearer_token_enabled"),
			PoolTimeoutMs:        atoi("net_webapp_tcp_client_pool_timeout"),
		},
		LDAP: srv.LDAP,

		TextDailyService:            boolean("mgr_text_db_daily_service_enabled"),
		TextDailyAPI:                boolean("mgr_text_db_daily_api_enabled"),
		TextDailyUA:                 boolean("mgr_text_db_daily_ua_enabled"),
		CompressXLog:                boolean("compress_xlog_enabled"),
		CompressProfile:             boolean("compress_profile_enabled"),
		VisitorHourlyCount:          boolean("visitor_hourly_count_enabled"),
		XLogIDIndexMB:               atoi("_mgr_xlog_id_index_mb"),
		IndexTraversalWarningCount:  atoi("log_index_traversal_warning_count"),
		CompressReadCacheBlocks:     atoi("_compress_read_cache_block_count"),
		CompressDailyCountCacheSize: atoi("_compress_dailycount_header_cache_size"),
		PurgeEnabled:                v.Set("mgr_purge_enabled") && srv.PurgeEnabled,
		Purge:                       srv.Purge,

		LogRotation:  boolean("log_rotation_enabled"),
		LogKeepDays:  atoi("log_keep_days"),
		Trace:        boolean("_trace"),
		LogIgnoreIDs: v.StringSet("mgr_log_ignore_ids"),

		SQLTableParsing:   boolean("sql_table_parsing_enabled"),
		LogSQLParsingFail: boolean("log_sql_parsing_fail_enabled"),

		LogUDP:           map[string]bool{},
		LogUDPSomeObject: value("log_udp_some_object"),
		LogTestRate:      atoi("log_test_rate"),
	}
	for _, k := range UDPLogKeys {
		if boolean(k) {
			l.LogUDP[k] = true
		}
	}
	l.effective = make(map[string]string, len(LiveKeys))
	for _, k := range LiveKeys {
		l.effective[k] = value(k)
	}
	// Go normalization and decisions that differ from the applyConfig text.
	itoa := strconv.Itoa
	l.effective["xlog_sampling_matcher_gxid_keep_memory_count"] = itoa(l.SamplingGxidKeepCount)
	l.effective["xlog_sampling_matcher_xlog_keep_memory_count"] = itoa(l.SamplingXLogKeepCount)
	l.effective["xlog_sampling_matcher_xlog_keep_memory_millis"] = itoa(l.SamplingXLogKeepMillis)
	l.effective["xlog_sampling_matcher_profile_keep_memory_count"] = itoa(l.SamplingProfileKeepCount)
	l.effective["xlog_sampling_matcher_profile_keep_memory_secs"] = itoa(l.SamplingProfileKeepSecs)
	l.effective["mgr_purge_enabled"] = strconv.FormatBool(l.PurgeEnabled)
	return l
}

// Pinned returns the keys fixed by command-line flags with their values.
func (s *Snapshot) Pinned() map[string]string {
	out := make(map[string]string, len(s.pinned))
	for k, v := range s.pinned {
		out[k] = v
	}
	return out
}
