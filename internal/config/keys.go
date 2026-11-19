package config

// ValueType is scouter.lang.conf.ValueType (wire numbers of CONFIGURE_VALUE_TYPE).
type ValueType int64

const (
	ValueText     ValueType = 1 // VALUE
	ValueNum      ValueType = 2 // NUM
	ValueBool     ValueType = 3 // BOOL
	ValueCSV      ValueType = 4 // COMMA_SEPARATED_VALUE
	ValueCSVPairs ValueType = 5 // COMMA_COLON_SEPARATED_VALUE
)

// Kind is the Java field type of a Configure key; it selects the
// getValue/getInt/getLong/getBoolean/getStringSet reader of applyConfig.
type Kind uint8

const (
	KindString Kind = iota
	KindInt
	KindLong
	KindBool
	KindStringSet
)

// TypeDesc is the @ConfigValueType strings/booleans of a
// COMMA_COLON_SEPARATED_VALUE key (ValueTypeDesc). The other ValueTypeDesc
// arrays are empty for every server key.
type TypeDesc struct {
	Strings  []string
	Booleans []bool
}

// Key describes one public Configure field (Configure.java:137-550) in
// declaration order, which is the order HotSpot's Class.getFields() returns
// and therefore the LIST_CONFIGURE_SERVER/CONFIGURE_DESC order.
type Key struct {
	Name string
	Kind Kind
	// Default is the field initializer as LIST_CONFIGURE_SERVER "default"
	// shows it (getKeyValueInfo reads a fresh Configure(true)); Null marks a
	// Java null initializer.
	Default string
	Null    bool
	// ApplyDefault is the applyConfig fallback when it differs from Default
	// (Configure.java:667-851); hasApplyDefault distinguishes "".
	ApplyDefault    string
	hasApplyDefault bool
	// NotApplied keys are never read by applyConfig: the value stays at the
	// field initializer whatever the file says.
	NotApplied bool
	Desc       string
	Type       ValueType
	TypeDesc   *TypeDesc
	// Restart marks keys the Go server reads only at startup: a reload that
	// changes them is logged and ignored (the active value is kept).
	Restart bool
	// GoUse names the Go consumer of a restart or live key. Keys without it
	// may still be read once at startup by cmd/scouter-go (CONTRACT.md §5,
	// §6); otherwise their value is only kept and shown.
	GoUse string
	// Live marks keys applied on reload through Snapshot.Live (LiveKeys).
	Live bool
}

func str(name, def, desc string) Key {
	return Key{Name: name, Kind: KindString, Default: def, Desc: desc, Type: ValueText}
}
func num(name, def, desc string) Key {
	return Key{Name: name, Kind: KindInt, Default: def, Desc: desc, Type: ValueNum}
}
func long(name, def, desc string) Key {
	return Key{Name: name, Kind: KindLong, Default: def, Desc: desc, Type: ValueNum}
}
func flag(name, def, desc string) Key {
	return Key{Name: name, Kind: KindBool, Default: def, Desc: desc, Type: ValueBool}
}

func (k Key) applyDefault(v string) Key { k.ApplyDefault, k.hasApplyDefault = v, true; return k }
func (k Key) notApplied() Key           { k.NotApplied = true; return k }
func (k Key) csv() Key                  { k.Type = ValueCSV; return k }
func (k Key) pairs(strings []string, booleans []bool) Key {
	k.Type, k.TypeDesc = ValueCSVPairs, &TypeDesc{Strings: strings, Booleans: booleans}
	return k
}
func (k Key) restart(use string) Key { k.Restart, k.GoUse = true, use; return k }
func (k Key) live(use string) Key    { k.Live, k.GoUse = true, use; return k }

const (
	restartWarnDesc = "[warn] modified this will break the database files.\nbackup old database files before change values.(restart required)"
	tgSample        = "[This option is just a sample. Change $measurement$ to your measurement name like $cpu$.]\n"
	// DefaultConfFile is Configure.getPropertyFile's default (CONF_DIR + "scouter.conf").
	DefaultConfFile = "./conf/scouter.conf"
)

// Keys lists every public non-static Configure field that is a setting. The
// Java reflection also exposes non-setting fields (property,
// telegrafInputConfigMap[Deprecated], telegrafOriginalConfig, allowIpExact,
// allowIpMatch); they are not settings and are omitted (CONTRACT.md).
// server_id's default is the host name, filled by Options.HostName.
var Keys = []Key{
	str("server_id", "", "Server ID").live("LOGIN server_id (--server-id pins it)"),
	num("log_test_rate", "0", "").live("ProfilePreCore log_test_rate sample lines in the server log"),
	flag("log_tcp_action_enabled", "false", "Logging TCP connection related event").live("transport Client/Agent open/close lines"),
	flag("log_udp_multipacket", "false", "Logging incoming MultiPacket").live("transport S160 line"),
	flag("log_expired_multipacket", "true", "Logging expired MultiPacket").live("transport S150 line"),
	flag("log_udp_packet", "false", "Logging all incoming packs").live(udpLogUse),
	flag("log_udp_counter", "false", "Logging incoming CounterPack").live(udpLogUse),
	flag("log_udp_interaction_counter", "false", "Logging incoming PerfInteractionCounterPack").live(udpLogUse),
	flag("log_udp_xlog", "false", "Logging incoming XLogPack").live(udpLogUse),
	flag("log_udp_profile", "false", "Logging incoming ProfilePack").live(udpLogUse),
	flag("log_udp_text", "false", "Logging incoming TextPack").live(udpLogUse),
	flag("log_udp_alert", "false", "Logging incoming AlertPack").live(udpLogUse),
	flag("log_udp_object", "false", "Logging incoming ObjectPack").live(udpLogUse),
	str("log_udp_some_object", "", "Logging some ObjectPack").live("addObjectTypeIfNotExist DEBUG objType stdout lines"),
	flag("log_udp_status", "false", "Logging incoming StatusPack").live(udpLogUse),
	flag("log_udp_stack", "false", "Logging incoming StackPack").live(udpLogUse),
	flag("log_udp_summary", "false", "Logging incoming SummaryPack").live(udpLogUse),
	flag("log_udp_batch", "false", "Logging incoming BatchPack").live(udpLogUse),
	flag("log_service_handler_list", "false", "Logging all request handlers in starting"),
	flag("log_udp_span", "false", "Logging incoming SpanPack").live(udpLogUse),
	num("log_index_traversal_warning_count", "100", "Logging when index traversal is too heavy.").live("storage S152/S161 index chain warning"),
	flag("log_rotation_enabled", "true", "Retaining log according to date").live(loggerUse),
	num("log_keep_days", "31", "Keeping period of log").live("server/request log cleanup (--log-keep-days pins it; the HTTP request log reads it at startup)"),
	flag("log_sql_parsing_fail_enabled", "false", "Logging sql failed to parse").live("SqlTables S112 line"),
	flag("_trace", "false", "").live(loggerUse),

	str("net_udp_listen_ip", "0.0.0.0", "UDP Host").restart("UDP listener (--udp)"),
	num("net_udp_listen_port", "6100", "UDP Port").restart("UDP listener (--udp)"),
	str("net_tcp_listen_ip", "0.0.0.0", "TCP Host").restart("TCP listener (--tcp)"),
	num("net_tcp_listen_port", "6100", "TCP Port").restart("TCP listener (--tcp)"),
	num("net_tcp_client_so_timeout_ms", "8000", "Client Socket Timeout(ms)").live("Client read timeout of new connections, LOGIN so_time_out, webapp pool timeout default"),
	num("net_tcp_agent_so_timeout_ms", "60000", "Agent Socket Timeout(ms)").live("Agent response read timeout of new agent connections"),
	num("net_tcp_agent_keepalive_interval_ms", "5000", "Transfer period(ms) of KEEP_ALIVE").live("agent pool KEEP_ALIVE idle-since-last-write threshold"),
	num("net_tcp_get_agent_connection_wait_ms", "1000", "Waiting time(ms) for agent session").live("agent pool idle session wait"),
	num("net_udp_packet_buffer_size", "65535", "UDP Packet Buffer Size").restart("UDP datagram size bound, only when set in the file"),
	num("net_udp_so_rcvbuf_size", "4194304", "UDP Receiver Buffer Size").restart("UDP SO_RCVBUF (--udp-read-buffer), only when set in the file"),
	num("_net_udp_worker_thread_count", "3", "").restart("UDP workers (--udp-workers wins), only when set in the file"),
	num("net_tcp_service_pool_size", "100", "TCP Thread Pool Size").live("concurrent Client sessions"),
	flag("net_http_server_enabled", "false", "Activating Http Server").restart("Java HTTP servlet listener (0.0.0.0:net_http_port; --scouter-http wins)"),
	num("net_http_port", "6180", "Http Port").restart("Java HTTP servlet listener (0.0.0.0:net_http_port; --scouter-http wins)"),
	str("net_http_extweb_dir", "./extweb", "user extension web root"),
	flag("net_http_api_enabled", "false", "Activating Scouter API"),
	flag("net_http_api_swagger_enabled", "false", "Enable a swagger for HTTP API."),
	str("net_http_api_swagger_host_ip", "", "Swagger option of host's ip or domain to call APIs."),
	str("net_http_api_cors_allow_origin", "*", "API CORS support for Access-Control-Allow-Origin").live(webappUse),
	str("net_http_api_cors_allow_credentials", "true", "Access-Control-Allow-Credentials").live(webappUse),
	num("net_webapp_tcp_client_pool_size", "30", "size of webapp connection pool to collector").applyDefault("12"),
	// applyConfig default is the current net_tcp_client_so_timeout_ms (derived in value()).
	num("net_webapp_tcp_client_pool_timeout", "60000", "timeout of web app connection pool to collector").live("webapp idle collector connection timeout"),
	num("net_webapp_tcp_client_so_timeout", "30000", "So timeout of web app to collector").notApplied(),
	flag("net_http_api_auth_ip_enabled", "false", "Enable api access control by client ip").live(webappUse),
	func() Key {
		k := str("net_http_api_auth_ip_header_key", "", "If get api caller's ip from http header.").applyDefault("").live(webappServletUse)
		k.Null = true
		return k
	}(),
	flag("net_http_api_auth_session_enabled", "false", "Enable api access control by JSESSIONID of Cookie").live(webappUse),
	num("net_http_api_session_timeout", "86400", "api http session timeout"),
	flag("net_http_api_auth_bearer_token_enabled", "false", "Enable api access control by Bearer token(of Authorization http header) - get access token from /user/loginGetToken.").live(webappUse),
	flag("net_http_api_gzip_enabled", "true", "Enable gzip response on api call"),
	str("net_http_api_allow_ips", "localhost,127.0.0.1,0:0:0:0:0:0:0:1,::1", "api access allow ip addresses").csv().live(webappServletUse),

	str("db_dir", "./database", "Store directory of database").restart("file database (--db)"),
	str("log_dir", "./logs", "Path to log directory").live("server/request log directory and SERVER_LOG_* root (--log-dir pins it)"),
	str("plugin_dir", "./plugin", "Path to plugin directory").restart("alert script read root (--plugin-dir)"),
	flag("plugin_enabled", "true", "Script plugin enabled"),
	str("client_dir", "./client", "Path to client related directory"),
	str("temp_dir", "./tempdata", "temp dir"),

	num("object_deadtime_ms", "8000", "Waiting time(ms) until stopped heartbeat of object is determined to be inactive").live("ingest object dead time (alive, inactive alert, kube seq release)"),
	num("object_inactive_alert_level", "0", "inactive object warning level. default 0.(0:info, 1:warn, 2:error, 3:fatal)").live("ingest INACTIVE_OBJECT/ACTIVATED_OBJECT alert level"),
	num("object_zipkin_deadtime_ms", "180000", "Zipkin Waiting time(ms) until stopped heartbeat of object is determined to be inactive").applyDefault("300000").live("ingest dead time of zipkin / z$ object types"),

	flag("compress_xlog_enabled", "false", "Activating XLog data in zip file").live("GZipStore of dates without xlog.service.conf (--compress-xlog pins it)"),
	flag("compress_profile_enabled", "false", "Activating profile data in zip file").live("GZipStore of dates without xlog.profile.conf (--compress-profile pins it)"),
	num("_compress_write_buffer_block_count", "3", ""),
	num("_compress_read_cache_block_count", "3", "").live("GZipStore decoded block cache size"),
	long("_compress_read_cache_expired_ms", "60000", ""),
	num("_compress_dailycount_header_cache_size", "3", "").live("GZipStore per-date writer positions"),
	num("_compress_write_thread", "2", ""),

	flag("_auto_5m_sampling", "true", "").live("runtime 5-minute counter sampling tick"),

	flag("mgr_purge_enabled", "true", "Activating automatic deletion function in the database").live("retention cycle (--purge pins it); see CONTRACT.md for the default"),
	num("mgr_purge_disk_usage_pct", "80", "Condition of disk usage for automatic deletion. if lack, delete profile data first exclude today data.").live(purgeUse),
	num("mgr_purge_profile_keep_days", "10", "Retaining date for automatic deletion. delete profile data first.").live(purgeUse),
	num("mgr_purge_keep_days", "10", "Deprecated : use mgr_purge_profile_keep_days").live(purgeUse),
	num("mgr_purge_xlog_keep_days", "30", "Retaining date for automatic deletion.").live(purgeUse),
	num("mgr_purge_xlog_without_profile_keep_days", "30", "Deprecated : use mgr_purge_xlog_keep_days").live(purgeUse),
	num("mgr_purge_counter_keep_days", "70", "Retaining date for automatic deletion").live(purgeUse),
	num("mgr_purge_realtime_counter_keep_days", "70", "Retaining date for automatic deletion. realtime-counter only.").live(purgeUse),
	num("mgr_purge_tag_counter_keep_days", "70", "Retaining date for automatic deletion. tag-counter only.").live(purgeUse),
	num("mgr_purge_visitor_counter_keep_days", "70", "Retaining date for automatic deletion. visitor-counter only").live(purgeUse),
	num("mgr_purge_daily_text_days", "140", "Retaining date for automatic deletion. daily text dictionary only").live(purgeUse),
	num("mgr_purge_sum_data_days", "60", "Retaining date for automatic deletion. summary(stat) data only").live(purgeUse),
	func() Key {
		k := str("mgr_log_ignore_ids", "{}", "Ignored log ID set").csv().live(loggerUse)
		k.Kind = KindStringSet
		return k
	}(),

	flag("mgr_text_db_daily_service_enabled", "false", "true for daily dictionary mode about service name. default value is false that means it's permanent.").live("text dictionary location per write and read"),
	flag("mgr_text_db_daily_api_enabled", "false", "true for daily dictionary mode about api name. default value is false that means it's permanent.").live("text dictionary location per write and read"),
	flag("mgr_text_db_daily_ua_enabled", "false", "true for daily dictionary mode about user agent. default value is false that means it's permanent.").live("text dictionary location per write and read"),
	num("_mgr_text_db_index_default_mb", "1", "change default memory size of hash index.(MB)"+restartWarnDesc).restart(indexUse),
	num("_mgr_text_db_index_service_mb", "1", "change memory size of hash index for service text.(MB)"+restartWarnDesc).restart(indexUse),
	num("_mgr_text_db_index_api_mb", "1", "change memory size of hash index for apicall text.(MB)"+restartWarnDesc).restart(indexUse),
	num("_mgr_text_db_index_ua_mb", "1", "change memory size of hash index for user agent text.(MB)"+restartWarnDesc).restart(indexUse),
	num("_mgr_text_db_index_login_mb", "1", "change memory size of hash index for login text.(MB)"+restartWarnDesc).restart(indexUse),
	num("_mgr_text_db_index_desc_mb", "1", "change memory size of hash index for desc text.(MB)"+restartWarnDesc).restart(indexUse),
	num("_mgr_text_db_index_hmsg_mb", "1", "change memory size of hash index for hashed message text.(MB)"+restartWarnDesc).restart(indexUse),
	num("_mgr_text_db_daily_index_mb", "1", "change memory size of hash index for daily text db.(MB)"+restartWarnDesc).restart(indexUse),
	num("_mgr_kv_store_index_default_mb", "8", "change default memory size of key value store index.(MB)"+restartWarnDesc).restart(indexUse),
	num("_mgr_xlog_id_index_mb", "1", "change default memory size of xlog txid/gxid index.(MB)"+restartWarnDesc).live("size of xlog_tid/xlog_gid indexes created from now on"),

	str("ext_link_name", "scouter-paper", "name of 3rd party ui").live("LOGIN ext_link_name"),
	str("ext_link_url_pattern", "http://my-scouter-paper-ip:6188/index.html#/paper?&address=localhost&port=6188&realtime=false&xlogElapsedTime=8000&instances=$[objHashes]&from=$[from]&to=$[to]&layout=my-layout-template-01",
		"outgoing link pattern for a 3rd party UI.(client restart required)\n"+
			"Context menu in any chart shows the menu 'Open with 3rd-party UI.'\n"+
			"* variable patterns : \n"+
			"   $[objHashes] : comma separated objHash values\n"+
			"   $[objType] : object type\n"+
			"   $[from] : start time in chart by millis\n"+
			"   $[to] : end time in chart by millis").live("LOGIN ext_link_url_pattern"),

	num("span_queue_size", "1000", "Span Queue Size").notApplied(),
	num("xlog_queue_size", "10000", "XLog Writer Queue Size").restart("storage XLog writer queue (XLogWR)"),
	num("xlog_realtime_lower_bound_ms", "0", "Ignored time(ms) in retrieving XLog in real time").live("TRANX_REAL_TIME_GROUP limit floor"),
	num("xlog_pasttime_lower_bound_ms", "0", "Ignored time(ms) in retrieving previous XLog").live("TRANX_LOAD_TIME_GROUP limit floor"),
	num("profile_queue_size", "1000", "Profile Writer Queue Size").restart("storage profile writer queue (XLogProfileWR)"),
	num("xlog_sampling_matcher_gxid_keep_memory_count", "500000", "gxid keeping count in memory for XLog consequent sampling").live("ingest processed-gxid map bound, read at startup like Java"),
	num("xlog_sampling_matcher_xlog_keep_memory_count", "100000", "xlog keeping count in memory for XLog consequent sampling").live("ingest delayed XLog bucket bound, read when a bucket is created"),
	num("xlog_sampling_matcher_xlog_keep_memory_millis", "5000", "max keeping millis of xlog for XLog consequent sampling").live("ingest delayed XLog bucket period"),
	num("xlog_sampling_matcher_profile_keep_memory_count", "5000", "profile keeping count (in one bucket, 500ms) in memory for XLog consequent sampling").live("ingest delayed profile bucket bound, read when a bucket is created"),
	num("xlog_sampling_matcher_profile_keep_memory_secs", "5", "max keeping seconds of profile for XLog consequent sampling").live("ingest delayed profile bucket count"),

	flag("geoip_enabled", "true", "Activating IP-based city/country extraction").live("ingest GeoIP step per XLog"),
	str("geoip_data_city_file", "./conf/GeoLiteCity.dat", "Path to GeoIP data file").live("GeoIP database, reloaded when the value changes"),
	flag("sql_table_parsing_enabled", "true", "Activating table-based SQL compression").live("SqlTables per sql TextPack"),
	flag("tagcnt_enabled", "true", "Activating TagCount function").live("ingest XLog/alert tag count"),
	num("req_search_xlog_max_count", "500", "search xlog service option - max xlog count to search per request").live("XLOG_LOAD_BY_TXIDS / SEARCH_XLOG_LIST bound"),

	str("input_telegraf_config_file", "./conf/scouter-telegraf.xml", "Path to telegraf config xml file").notApplied(),
	flag("input_telegraf_enabled", "true", "Deprecated use the telegraf config view instead. This value may be ignored."),
	flag("input_telegraf_debug_enabled", "false", "Deprecated use the telegraf config view instead. This value may be ignored."),
	flag("input_telegraf_delta_counter_normalize_default", "true", "Deprecated use the telegraf config view instead. This value may be ignored."),
	num("input_telegraf_delta_counter_normalize_default_seconds", "30", "Deprecated use the telegraf config view instead. This value may be ignored."),
	num("telegraf_object_deadtime_ms", "35000", "Deprecated use the telegraf config view instead. This value may be ignored."),
	flag("input_telegraf_$measurement$_enabled", "true", tgSample+
		"Telegraf http input of the $measurement$ enabled.\n"+
		"$measurement$ is a variable to the measurement name of the line protocol.\n"+
		"eg) input_telegraf_$redis_keyspace$_enabled=true"),
	flag("input_telegraf_$measurement$_debug_enabled", "false", tgSample+
		"print telegraf line protocol of the $measurement$ to STDOUT"),
	str("input_telegraf_$measurement$_tag_filter", "", tgSample+
		"If set, only the metric matching to this tag value is handled.\n"+
		"It can have multiple values. comma separator means 'or' condition. eg) cpu:cpu-total,cpu:cpu0\n"+
		"It also have not(!) condition. eg) cpu:!cpu-total").
		pairs([]string{"filtering tag name(reqiured)", "filtering tag value(reqiured)"}, []bool{true, true}),
	str("input_telegraf_$measurement$_counter_mappings", "", tgSample+
		"which fields of $measurement$ are mapped to scouter's counter.\n"+
		"format: {line-protocol field name}:{scouter counter name}:{display name?}:{unit?}:{hasTotal?}:{normalize sec?}\n"+
		"It can have multiple values.\n"+
		" - {scouter counter name} can be defined in combination with the line protocol's tag variables.\n"+
		"For example, if the value of 'tag1' is 'disk01' and the value of 'tag2' is 'bin', the counter name defined as 'scouter-du-$tag1$-$tag2$' is 'scouter-du-disk01-bin'.\n"+
		" eg)used_memory:tg-redis-used-memory,used_memory_rss:redis-used-memory-rss,redis used rss,bytes:true\n"+
		" eg)cpu:cpu-$cpu-no$ -- this example shows counter definition with tag value.\n"+
		"If {line-protocol field name} is started with '&' or '&&', then It works as delta counter\n"+
		"When specified as a delta type, the difference in values per second is stored. and the counter name ends with '_delta'\n"+
		"double '&&' means BOTH type. AS BOTH type, the value and the difference value both are stored.\n"+
		" - {normalize sec} applies only to a delta counter if the counter is a 'BOTH' type counter. (This value can have min 4 to max 60)").
		pairs([]string{"line protocol field\n(reqiured)", "mapping counter name\n(reqiured)", "display name", "unit", "totalizable\ndefault true", "norm. sec.\ndefault 30"}, []bool{true, true, false, false, false, false}),
	str("input_telegraf_$measurement$_objFamily_base", "", tgSample+
		"define an obj Family prefix. objectType is defined with some tags.\n"+
		"see input_telegraf_$measurement$_objFamily_append_tags option."),
	str("input_telegraf_$measurement$_objFamily_append_tags", "", tgSample+
		"this tags's value is appended to objFamily_base.\nIt can have multiple values. eg)tag1,tag2").csv(),
	str("input_telegraf_$measurement$_objType_base", "", tgSample+
		"define an objectType prefix. objectType is defined with some tags.\n"+
		"see input_telegraf_$measurement$_objType_prepend(or append)_tags option."),
	str("input_telegraf_$measurement$_objType_prepend_tags", "scouter_obj_type_prefix", tgSample+
		"this tags's value is prepended to objType_base.\nIt can have multiple values. eg)tag1,tag2").csv(),
	str("input_telegraf_$measurement$_objType_append_tags", "", tgSample+
		"this tags's value is appended to objType_base.\nIt can have multiple values. eg)tag1,tag2").csv(),
	str("input_telegraf_$measurement$_objType_icon", "", tgSample+
		"this tags's value is object type's icon file name that the scouter client have. eg)redis"),
	str("input_telegraf_$measurement$_objName_base", "", tgSample+
		"define an objectName prefix. objectName is defined with some tags.\n"+
		"see input_telegraf_$measurement$_objName_append_tags option."),
	str("input_telegraf_$measurement$_objName_append_tags", "", tgSample+
		"this tags's value is appended to objName_base.\n"+
		"It can have multiple values. eg)tag1,tag2").csv(),
	str("input_telegraf_$measurement$_host_tag", "host", tgSample+
		"tag name to define host"),
	str("input_telegraf_$measurement$_host_mappings", "", tgSample+
		"which host value defined with $measurement$_host_tag option is mapped to scouter's host.\n"+
		"It can have multiple values. eg)hostValue1:scouterHost1,hostValue2:scouterHost2").
		pairs([]string{"telegraf host name(reqiured)", "scouter host name(reqiured)"}, []bool{true, true}),

	flag("visitor_hourly_count_enabled", "true", "").live("visitor hourly count per visitor"),
}

const (
	purgeUse         = "retention cycle (--mgr-purge-* pins it)"
	udpLogUse        = "NetDataProcessor stdout line per pack"
	loggerUse        = "server log"
	webappUse        = "webapp REST API per request"
	webappServletUse = "webapp REST API per request; the servlet listener reads it at startup"
	indexUse         = "size of hash index files created after a restart"
)

// LDAP keys are read by AccountManager.authorizeAccount through
// conf.getValue/getBoolean; they are not Configure fields, so they never
// appear in LIST_CONFIGURE_SERVER. "account_ldap_auth=" is the Java key
// verbatim (a typo kept by the original; it can only be set as
// account_ldap_auth\==...).
const (
	KeyAccountUseLDAP             = "account_use_ldap"
	KeyAccountLDAPProviderURL     = "account_ldap_provider_url"
	KeyAccountLDAPAuth            = "account_ldap_auth="
	KeyAccountLDAPPrincipalDomain = "account_ldap_principal_domain"
	KeyAccountLDAPDebug           = "account_ldap_debug"
	KeyAccountLDAPBaseDN          = "account_ldap_basedn"
	KeyAccountLDAPEmailID         = "account_ldap_email_id"
	KeyAccountLDAPGroupID         = "account_ldap_group_id"
)

// KeyGoMaxProcs is Go-only: how many CPUs the Go runtime runs Go code on at
// once (GOMAXPROCS), applied live. Absent or 0 keeps the startup value: the
// GOMAXPROCS environment variable, else the runtime default (the logical
// CPUs, capped by a cgroup CPU limit). It is not a Configure field, so it
// never appears in LIST_CONFIGURE_SERVER.
const KeyGoMaxProcs = "go_maxprocs"

// goKeys are the Go-only keys (known, not Configure fields); the go_* resource
// limits are in goLimitKeys.
var goKeys = []string{KeyGoMaxProcs}

var accountKeys = []string{KeyAccountUseLDAP, KeyAccountLDAPProviderURL, KeyAccountLDAPAuth, KeyAccountLDAPPrincipalDomain, KeyAccountLDAPDebug, KeyAccountLDAPBaseDN, KeyAccountLDAPEmailID, KeyAccountLDAPGroupID}

var keyIndex = func() map[string]*Key {
	m := make(map[string]*Key, len(Keys))
	for i := range Keys {
		m[Keys[i].Name] = &Keys[i]
	}
	return m
}()

// Lookup returns the descriptor of a Configure key.
func Lookup(name string) (Key, bool) {
	k, ok := keyIndex[name]
	if !ok {
		return Key{}, false
	}
	return *k, true
}
