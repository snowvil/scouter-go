package config

import (
	"sort"
	"strconv"
	"strings"
	"time"
)

// Snapshot is one immutable applied configuration. Its accessors follow
// Configure.getValue/getInt/getLong/getBoolean (Configure.java:1130-1166).
type Snapshot struct {
	// Exists is false when no file has been loaded yet (all defaults).
	Exists   bool
	values   map[string]string
	lookup   func(string) (string, bool)
	hostName string
	unknown  []string
	telegraf TelegrafFile
	// pinned holds the keys fixed by command-line flags (Options.Pinned).
	pinned map[string]string
}

// TelegrafFile is scouter-telegraf.xml as read with this snapshot's
// scouter.conf. Text is the raw file; parsing and the XML-over-deprecated
// merge belong to the telegraf consumer, which recomposes both on every
// Change (CONTRACT.md §5.1).
type TelegrafFile struct {
	Path    string
	Exists  bool
	Text    string
	ModTime time.Time
}

// Telegraf returns the telegraf file read together with this snapshot.
func (s *Snapshot) Telegraf() TelegrafFile { return s.telegraf }

// TelegrafDeprecated returns the scouter.conf telegraf keys present in the
// file (input_telegraf_* and telegraf_object_deadtime_ms), after ${} and
// trim: the Java applyTelegrafInputConfig input and the deprecated globals
// that the XML overrides.
func (s *Snapshot) TelegrafDeprecated() map[string]string {
	out := map[string]string{}
	for k, v := range s.values {
		if strings.HasPrefix(k, "input_telegraf_") || k == "telegraf_object_deadtime_ms" {
			out[k] = v
		}
	}
	return out
}

// Get is Configure.getValue(key): the trimmed file value, else the trimmed
// environment value (replaceSysProp merges System.getenv into the
// Properties, so an environment variable named like a key is used when the
// file lacks it).
func (s *Snapshot) Get(key string) (string, bool) {
	if v, ok := s.values[key]; ok {
		return v, true
	}
	if s.lookup != nil {
		if v, ok := s.lookup(key); ok {
			return JavaTrim(v), true
		}
	}
	return "", false
}

// FileKeys returns the keys present in the file, sorted.
func (s *Snapshot) FileKeys() []string {
	out := make([]string, 0, len(s.values))
	for k := range s.values {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Unknown returns the file keys that are not known settings.
func (s *Snapshot) Unknown() []string { return append([]string(nil), s.unknown...) }

// Values returns the entries of keys that are set (file or environment).
func (s *Snapshot) Values(keys []string) map[string]string {
	out := map[string]string{}
	for _, k := range keys {
		if v, ok := s.Get(k); ok {
			out[k] = v
		}
	}
	return out
}

// String is getValue(key, def).
func (s *Snapshot) String(key, def string) string {
	if v, ok := s.Get(key); ok {
		return v
	}
	return def
}

// Int is getInt(key, def): Integer.parseInt or def on any failure.
func (s *Snapshot) Int(key string, def int) int {
	if v, ok := s.Get(key); ok {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			return int(n)
		}
	}
	return def
}

// IntSet returns the parsed int and whether the key is set to a valid int
// (the case where applyConfig does not fall back to its default).
func (s *Snapshot) IntSet(key string) (int, bool) {
	if v, ok := s.Get(key); ok {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			return int(n), true
		}
	}
	return 0, false
}

// Long is getLong(key, def).
func (s *Snapshot) Long(key string, def int64) int64 {
	if v, ok := s.Get(key); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

// Bool is getBoolean(key, def): Boolean.parseBoolean, so any set value other
// than case-insensitive "true" is false.
func (s *Snapshot) Bool(key string, def bool) bool {
	if v, ok := s.Get(key); ok {
		return strings.EqualFold(v, "true")
	}
	return def
}

// Set reports whether key is configured (file or environment).
func (s *Snapshot) Set(key string) bool {
	_, ok := s.Get(key)
	return ok
}

// Property is one LIST_CONFIGURE_SERVER/CONFIGURE_* row.
type Property struct {
	Key, Value, Default, Desc string
	Type                      ValueType
	TypeDesc                  *TypeDesc
	// Kind is the Configure field type; ConfigValueUtil.toValue sends
	// numbers as DecimalValue and booleans as BooleanValue. NullDefault marks
	// a null field initializer (NullValue in the "default" column).
	Kind        Kind
	NullDefault bool
	// HasDesc reports a @ConfigDesc annotation; getConfigDescMap skips the
	// fields without one.
	HasDesc bool
}

// Properties returns every Configure key in declaration order with its
// current applied value and field default (getKeyValueInfo,
// getConfigureDesc, getConfigureValueType, getConfigureValueTypeDesc).
func (s *Snapshot) Properties() []Property {
	out := make([]Property, 0, len(Keys))
	purge := s.purgeValues()
	for _, k := range Keys {
		def := k.Default
		if k.Name == "server_id" {
			def = s.hostName
		}
		out = append(out, Property{Key: k.Name, Value: s.value(k, purge), Default: def, Desc: k.Desc, Type: k.Type, TypeDesc: k.TypeDesc, Kind: k.Kind, NullDefault: k.Null, HasDesc: k.Desc != ""})
	}
	return out
}

// Value returns the applied value text of a Configure key.
func (s *Snapshot) Value(key string) (string, bool) {
	k, ok := Lookup(key)
	if !ok {
		return "", false
	}
	return s.value(k, s.purgeValues()), true
}

func (s *Snapshot) value(k Key, purge map[string]int) string {
	if k.NotApplied {
		return k.Default
	}
	if v, ok := purge[k.Name]; ok {
		return strconv.Itoa(v)
	}
	def := k.Default
	if k.hasApplyDefault {
		def = k.ApplyDefault
	}
	switch k.Name {
	case "server_id":
		def = s.hostName
	case "net_webapp_tcp_client_pool_timeout":
		def = strconv.Itoa(s.Int("net_tcp_client_so_timeout_ms", 8000))
	}
	switch k.Kind {
	case KindInt:
		d, _ := strconv.Atoi(def)
		return strconv.Itoa(s.Int(k.Name, d))
	case KindLong:
		d, _ := strconv.ParseInt(def, 10, 64)
		return strconv.FormatInt(s.Long(k.Name, d), 10)
	case KindBool:
		return strconv.FormatBool(s.Bool(k.Name, def == "true"))
	case KindStringSet:
		return "{" + strings.Join(s.StringSet(k.Name), ", ") + "}"
	default:
		return s.String(k.Name, def)
	}
}

// StringSet is getStringSet(key, ","): trimmed non-empty tokens, first
// occurrence order (Java StringSet iterates in hash order; CONTRACT.md).
func (s *Snapshot) StringSet(key string) []string {
	v, ok := s.Get(key)
	if !ok {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	for _, x := range strings.Split(v, ",") {
		x = JavaTrim(x)
		if x != "" && !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}

// purgeValues is the mgr_purge_* derivation of applyConfig
// (Configure.java:772-792).
func (s *Snapshot) purgeValues() map[string]int {
	m := map[string]int{}
	m["mgr_purge_disk_usage_pct"] = s.Int("mgr_purge_disk_usage_pct", 80)
	keep := s.Int("mgr_purge_keep_days", 10)
	m["mgr_purge_keep_days"] = keep
	profile := s.Int("mgr_purge_profile_keep_days", 10)
	if profile == 0 {
		profile = keep
	}
	m["mgr_purge_profile_keep_days"] = profile
	without := s.Int("mgr_purge_xlog_without_profile_keep_days", profile*3)
	m["mgr_purge_xlog_without_profile_keep_days"] = without
	xlog := s.Int("mgr_purge_xlog_keep_days", profile*3)
	if xlog == 0 {
		xlog = without
	}
	m["mgr_purge_xlog_keep_days"] = xlog
	counter := s.Int("mgr_purge_counter_keep_days", keep*7)
	m["mgr_purge_counter_keep_days"] = counter
	m["mgr_purge_realtime_counter_keep_days"] = s.Int("mgr_purge_realtime_counter_keep_days", counter)
	tag := s.Int("mgr_purge_tag_counter_keep_days", counter)
	m["mgr_purge_tag_counter_keep_days"] = tag
	m["mgr_purge_visitor_counter_keep_days"] = s.Int("mgr_purge_visitor_counter_keep_days", counter)
	m["mgr_purge_daily_text_days"] = s.Int("mgr_purge_daily_text_days", max(tag*2, xlog*2))
	m["mgr_purge_sum_data_days"] = s.Int("mgr_purge_sum_data_days", 60)
	return m
}

// PurgeKeys are the mgr_purge_* integer keys (storage.JavaRetentionKeys).
var PurgeKeys = []string{"mgr_purge_disk_usage_pct", "mgr_purge_keep_days", "mgr_purge_profile_keep_days", "mgr_purge_xlog_keep_days", "mgr_purge_xlog_without_profile_keep_days", "mgr_purge_counter_keep_days", "mgr_purge_realtime_counter_keep_days", "mgr_purge_tag_counter_keep_days", "mgr_purge_visitor_counter_keep_days", "mgr_purge_daily_text_days", "mgr_purge_sum_data_days"}

// LDAP holds the AccountManager.authorizeAccount keys. cmd/scouter-go maps
// them to account.LDAPConfig at startup (CONTRACT.md §6).
type LDAP struct {
	Enabled                                                      bool
	ProviderURL, Auth, PrincipalDomain, BaseDN, EmailID, GroupID string
	Debug                                                        bool
}

// Server is the typed view of the keys the Go server consumes or exposes.
type Server struct {
	ServerID                 string
	DBDir, LogDir, PluginDir string
	UDPListenIP              string
	UDPListenPort            int
	TCPListenIP              string
	TCPListenPort            int
	UDPReceiveBuffer         int
	HTTPServerEnabled        bool
	HTTPPort                 int
	CompressXLog             bool
	CompressProfile          bool
	ObjectDeadTime           time.Duration
	ObjectZipkinDeadTime     time.Duration
	ObjectInactiveAlertLevel int
	PurgeEnabled             bool
	// Purge holds the mgr_purge_* keys set to a valid int: the input of
	// storage.JavaRetentionConfig, which applies the same derivation.
	Purge                                             map[string]int
	TextDBDailyService, TextDBDailyAPI, TextDBDailyUA bool
	LDAP                                              LDAP
}

// Server returns the typed values with the applyConfig defaults.
func (s *Snapshot) Server() Server {
	out := Server{
		ServerID:                 s.String("server_id", s.hostName),
		DBDir:                    s.String("db_dir", "./database"),
		LogDir:                   s.String("log_dir", "./logs"),
		PluginDir:                s.String("plugin_dir", "./plugin"),
		UDPListenIP:              s.String("net_udp_listen_ip", "0.0.0.0"),
		UDPListenPort:            s.Int("net_udp_listen_port", 6100),
		TCPListenIP:              s.String("net_tcp_listen_ip", "0.0.0.0"),
		TCPListenPort:            s.Int("net_tcp_listen_port", 6100),
		UDPReceiveBuffer:         s.Int("net_udp_so_rcvbuf_size", 4<<20),
		HTTPServerEnabled:        s.Bool("net_http_server_enabled", false),
		HTTPPort:                 s.Int("net_http_port", 6180),
		CompressXLog:             s.Bool("compress_xlog_enabled", false),
		CompressProfile:          s.Bool("compress_profile_enabled", false),
		ObjectDeadTime:           time.Duration(s.Int("object_deadtime_ms", 8000)) * time.Millisecond,
		ObjectZipkinDeadTime:     time.Duration(s.Int("object_zipkin_deadtime_ms", 300000)) * time.Millisecond,
		ObjectInactiveAlertLevel: s.Int("object_inactive_alert_level", 0),
		PurgeEnabled:             s.Bool("mgr_purge_enabled", true),
		Purge:                    map[string]int{},
		TextDBDailyService:       s.Bool("mgr_text_db_daily_service_enabled", false),
		TextDBDailyAPI:           s.Bool("mgr_text_db_daily_api_enabled", false),
		TextDBDailyUA:            s.Bool("mgr_text_db_daily_ua_enabled", false),
		LDAP: LDAP{
			Enabled:         s.Bool(KeyAccountUseLDAP, false),
			ProviderURL:     s.String(KeyAccountLDAPProviderURL, ""),
			Auth:            s.String(KeyAccountLDAPAuth, "simple"),
			PrincipalDomain: s.String(KeyAccountLDAPPrincipalDomain, ""),
			Debug:           s.Bool(KeyAccountLDAPDebug, false),
			BaseDN:          s.String(KeyAccountLDAPBaseDN, ""),
			EmailID:         s.String(KeyAccountLDAPEmailID, ""),
			GroupID:         s.String(KeyAccountLDAPGroupID, ""),
		},
	}
	for _, k := range PurgeKeys {
		if v, ok := s.IntSet(k); ok {
			out.Purge[k] = v
		}
	}
	return out
}
