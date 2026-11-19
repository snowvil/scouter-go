package pulse

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Java configuration keys (plugin settings loader). The partition count
// key has no "ext_" prefix in the original and that spelling is preserved.
const (
	KeyKafkaServers                 = "ext_plugin_pulse_kafka_servers"
	KeyKafkaXLogMetaTopic           = "ext_plugin_pulse_kafka_xlog_meta_topic"
	KeyKafkaXLogProfileTopic        = "ext_plugin_pulse_kafka_xlog_profile_topic"
	KeyKafkaTopicPartitionCount     = "plugin_pulse_kafka_topic_partition_count"
	KeyKafkaProduceTimeout          = "ext_plugin_pulse_kafka_produce_timeout"
	KeyDebug                        = "ext_plugin_pulse_debug"
	KeyEnable                       = "ext_plugin_pulse_enable"
	KeyKafkaDispatchQueueCapacity   = "ext_plugin_pulse_kafka_dispatch_queue_capacity"
	KeyKafkaDispatchWorkerCount     = "ext_plugin_pulse_kafka_dispatch_worker_count"
	KeyExcludeTransactionExtensions = "ext_plugin_pulse_exclude_transaction_extensions"
	KeyExcludeApplications          = "ext_plugin_pulse_exclude_applications"
	KeyRebaseTransactionApps        = "ext_plugin_pulse_rebase_transaction_applications"
	KeyRebaseTransactionGlobal      = "ext_plugin_pulse_rebase_transaction_global_filter"
	KeyEnableSpringURIFilter        = "ext_plugin_pulse_enable_spring_uri_filter"
	KeyNonSpringBasedApplications   = "ext_plugin_pulse_non_spring_based_applications"
	KeyDropHTTPStatusRules          = "ext_plugin_pulse_drop_http_status_rules"
)

// JavaKeys lists every configuration key the plugin reads.
func JavaKeys() []string {
	return []string{KeyKafkaServers, KeyKafkaXLogMetaTopic, KeyKafkaXLogProfileTopic, KeyKafkaTopicPartitionCount, KeyKafkaProduceTimeout, KeyDebug, KeyEnable, KeyKafkaDispatchQueueCapacity, KeyKafkaDispatchWorkerCount, KeyExcludeTransactionExtensions, KeyExcludeApplications, KeyRebaseTransactionApps, KeyRebaseTransactionGlobal, KeyEnableSpringURIFilter, KeyNonSpringBasedApplications, KeyDropHTTPStatusRules}
}

// Defaults from the plugin settings loader. The Kafka server default of the
// original is an internal address and is deliberately not reproduced.
const (
	DefaultXLogMetaTopic         = "dev-pulse-scouter-xlog-meta"
	DefaultXLogProfileTopic      = "dev-pulse-scouter-xlog-profile"
	DefaultTopicPartitionCount   = 30
	DefaultProduceTimeoutMillis  = 5000
	DefaultDispatchQueueCapacity = 4096
	DefaultDispatchWorkerCount   = 2
)

// Filter defaults from the plugin settings loader. The three application-name
// filters (exclude, rebase, non-Spring) default to names of one
// particular deployment in the original, so they are deliberately empty here: an
// operator lists the applications in scouter.conf.
var (
	defaultExcludeTransactionExtensions = []string{"woff", "woff2", "eot", "svg", "ttf"}
	defaultExcludeApplications          []string
	defaultRebaseTransactionApps        []string
	defaultRebaseTransactionGlobal      = []string{"$$", "@"}
	defaultNonSpringBasedApplications   []string
)

var ErrConfig = errors.New("pulse configuration")

// RebaseCondition is the Java Pair<delimiter, index> of the plugin settings.
type RebaseCondition struct {
	Delimiter string
	Index     int32
}

// HTTPStatusDropRule is the plugin HTTP status drop rule:
// Application is "*" or a trimmed lowercase name; either Exact or Class is set.
type HTTPStatusDropRule struct {
	Application string
	Exact       *int32
	Class       *int32
}

func (r HTTPStatusDropRule) matches(application string, status int32) bool {
	if r.Application != "*" && r.Application != application {
		return false
	}
	if r.Exact != nil {
		return *r.Exact == status
	}
	return r.Class != nil && status/100 == *r.Class
}

func (r HTTPStatusDropRule) String() string {
	if r.Exact != nil {
		return r.Application + "|" + strconv.Itoa(int(*r.Exact))
	}
	return r.Application + "|" + strconv.Itoa(int(*r.Class)) + "xx"
}

// Config holds the parsed plugin settings. Set values are already
// trimmed and lowercased as the Java static initializer does; extracted
// application names are compared without normalization except for HTTP status
// rules.
type Config struct {
	KafkaServers            string
	XLogMetaTopic           string
	XLogProfileTopic        string
	TopicPartitionCount     int32
	ProduceTimeoutMillis    int32
	Debug                   bool
	Enable                  bool
	DispatchQueueCapacity   int
	DispatchWorkerCount     int
	ExcludeTransactionExts  map[string]struct{}
	ExcludeApplications     map[string]struct{}
	RebaseTransactionApps   map[string]RebaseCondition
	RebaseTransactionGlobal []string
	EnableSpringURIFilter   bool
	NonSpringBasedApps      map[string]struct{}
	DropHTTPStatusRules     []HTTPStatusDropRule
}

func toSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, v := range values {
		out[v] = struct{}{}
	}
	return out
}

// DefaultConfig returns the Java defaults with no Kafka server address and
// empty application-name filters (see defaultExcludeApplications).
func DefaultConfig() Config {
	cfg, err := ParseJavaConfig(nil)
	if err != nil {
		panic(err)
	}
	return cfg
}

// javaInt follows Configure.getInt: a missing key or an unparsable value yields
// the default.
func javaInt(values map[string]string, key string, fallback int32) int32 {
	raw, ok := values[key]
	if !ok {
		return fallback
	}
	n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
	if err != nil {
		return fallback
	}
	return int32(n)
}

// javaBool follows Configure.getBoolean: Boolean.parseBoolean of the trimmed
// value, so anything other than "true" (case-insensitive) is false.
func javaBool(values map[string]string, key string, fallback bool) bool {
	raw, ok := values[key]
	if !ok {
		return fallback
	}
	return strings.EqualFold(strings.TrimSpace(raw), "true")
}

// splitTrimLower is Arrays.stream(value.split(",")).map(trim).map(toLowerCase).
// Java String.split drops trailing empty elements; an empty input yields [""].
func splitTrimLower(value string) []string {
	parts := javaSplit(value, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, strings.ToLower(strings.TrimSpace(p)))
	}
	return out
}

func parseRebaseEntries(entries []string) (map[string]RebaseCondition, error) {
	out := make(map[string]RebaseCondition)
	for _, entry := range entries {
		if !strings.Contains(entry, "|") {
			continue
		}
		parts := javaSplit(entry, "|")
		if len(parts) != 3 {
			continue
		}
		index, err := strconv.ParseInt(strings.TrimSpace(parts[2]), 10, 32)
		if err != nil {
			continue
		}
		name := strings.TrimSpace(parts[0])
		if _, dup := out[name]; dup {
			// Collectors.toMap throws IllegalStateException on duplicate keys,
			// which fails the Java static initializer; surface it explicitly.
			return nil, fmt.Errorf("%w: duplicate rebase application %q", ErrConfig, name)
		}
		out[name] = RebaseCondition{Delimiter: strings.TrimSpace(parts[1]), Index: int32(index)}
	}
	return out, nil
}

func parseHTTPStatusRules(value string) []HTTPStatusDropRule {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	var rules []HTTPStatusDropRule
	for _, entry := range splitTrimLower(value) {
		if entry == "" {
			continue
		}
		parts := javaSplit(entry, "|")
		if len(parts) != 2 {
			continue
		}
		application := strings.TrimSpace(parts[0])
		status := strings.TrimSpace(parts[1])
		if application == "" || status == "" {
			continue
		}
		if strings.HasSuffix(status, "xx") && len(status) == 3 {
			class, err := strconv.ParseInt(status[:1], 10, 32)
			if err != nil {
				continue
			}
			c := int32(class)
			rules = append(rules, HTTPStatusDropRule{Application: application, Class: &c})
			continue
		}
		exact, err := strconv.ParseInt(status, 10, 32)
		if err != nil {
			continue
		}
		e := int32(exact)
		rules = append(rules, HTTPStatusDropRule{Application: application, Exact: &e})
	}
	return rules
}

// ParseJavaConfig builds a Config from Scouter configuration values keyed by the
// Java names. A key that is absent takes the default; a key that is present with
// an empty string is parsed as Java would parse "" (an empty element set).
// Partition count must be positive: Java fails each task with
// ArithmeticException when it is zero, and negative counts are rejected here.
func ParseJavaConfig(values map[string]string) (Config, error) {
	get := func(key string, fallback string) string {
		if v, ok := values[key]; ok {
			return v
		}
		return fallback
	}
	cfg := Config{
		KafkaServers:          get(KeyKafkaServers, ""),
		XLogMetaTopic:         get(KeyKafkaXLogMetaTopic, DefaultXLogMetaTopic),
		XLogProfileTopic:      get(KeyKafkaXLogProfileTopic, DefaultXLogProfileTopic),
		TopicPartitionCount:   javaInt(values, KeyKafkaTopicPartitionCount, DefaultTopicPartitionCount),
		ProduceTimeoutMillis:  javaInt(values, KeyKafkaProduceTimeout, DefaultProduceTimeoutMillis),
		Debug:                 javaBool(values, KeyDebug, true),
		Enable:                javaBool(values, KeyEnable, true),
		DispatchQueueCapacity: int(javaInt(values, KeyKafkaDispatchQueueCapacity, DefaultDispatchQueueCapacity)),
		DispatchWorkerCount:   int(javaInt(values, KeyKafkaDispatchWorkerCount, DefaultDispatchWorkerCount)),
		EnableSpringURIFilter: javaBool(values, KeyEnableSpringURIFilter, true),
	}
	if v, ok := values[KeyExcludeTransactionExtensions]; ok {
		cfg.ExcludeTransactionExts = toSet(splitTrimLower(v))
	} else {
		cfg.ExcludeTransactionExts = toSet(defaultExcludeTransactionExtensions)
	}
	if v, ok := values[KeyExcludeApplications]; ok {
		cfg.ExcludeApplications = toSet(splitTrimLower(v))
	} else {
		cfg.ExcludeApplications = toSet(defaultExcludeApplications)
	}
	var err error
	if v, ok := values[KeyRebaseTransactionApps]; ok {
		cfg.RebaseTransactionApps, err = parseRebaseEntries(splitTrimLower(v))
	} else {
		cfg.RebaseTransactionApps, err = parseRebaseEntries(defaultRebaseTransactionApps)
	}
	if err != nil {
		return Config{}, err
	}
	if v, ok := values[KeyRebaseTransactionGlobal]; ok {
		cfg.RebaseTransactionGlobal = uniqueStrings(splitTrimLower(v))
	} else {
		cfg.RebaseTransactionGlobal = uniqueStrings(defaultRebaseTransactionGlobal)
	}
	if v, ok := values[KeyNonSpringBasedApplications]; ok {
		cfg.NonSpringBasedApps = toSet(splitTrimLower(v))
	} else {
		cfg.NonSpringBasedApps = toSet(defaultNonSpringBasedApplications)
	}
	cfg.DropHTTPStatusRules = parseHTTPStatusRules(get(KeyDropHTTPStatusRules, ""))
	if cfg.TopicPartitionCount <= 0 {
		return Config{}, fmt.Errorf("%w: %s must be positive, got %d", ErrConfig, KeyKafkaTopicPartitionCount, cfg.TopicPartitionCount)
	}
	// Math.max(1, value) in the plugin Kafka dispatcher.
	if cfg.DispatchQueueCapacity < 1 {
		cfg.DispatchQueueCapacity = 1
	}
	if cfg.DispatchWorkerCount < 1 {
		cfg.DispatchWorkerCount = 1
	}
	return cfg, nil
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// IsExcludeTransaction is the plugin transaction extension exclusion.
// A null transaction (dictionary miss) is excluded. The extension after the
// last "." is lowercased before the set lookup.
func (c Config) IsExcludeTransaction(transaction *string) bool {
	if transaction == nil {
		return true
	}
	name := *transaction
	dot := strings.LastIndexByte(name, '.')
	if dot < 0 || dot == len(name)-1 {
		return false
	}
	_, excluded := c.ExcludeTransactionExts[strings.ToLower(name[dot+1:])]
	return excluded
}

// IsExcludeApplication is the plugin application exclusion; the
// application is compared as extracted.
func (c Config) IsExcludeApplication(application string) bool {
	_, excluded := c.ExcludeApplications[application]
	return excluded
}

// IsSpringBasedApplication is the plugin Spring-based application check.
func (c Config) IsSpringBasedApplication(application string) bool {
	if !c.EnableSpringURIFilter {
		return false
	}
	_, nonSpring := c.NonSpringBasedApps[application]
	return !nonSpring
}

// RebaseConditionFor returns the rebase condition for an application.
func (c Config) RebaseConditionFor(application string) (RebaseCondition, bool) {
	cond, ok := c.RebaseTransactionApps[application]
	return cond, ok
}

// IsDropHTTPStatus is the plugin HTTP status drop check: only here is the
// application trimmed and
// lowercased; status <= 0 or no rules never match.
func (c Config) IsDropHTTPStatus(application string, status int32) bool {
	if status <= 0 || len(c.DropHTTPStatusRules) == 0 {
		return false
	}
	normalized := strings.ToLower(strings.TrimSpace(application))
	for _, rule := range c.DropHTTPStatusRules {
		if rule.matches(normalized, status) {
			return true
		}
	}
	return false
}

// SpringURIFiltered is the inline check of the plugin dispatcher: a transaction
// that starts with "/" and does not contain both "<" and ">" is excluded for a
// Spring-based application.
func SpringURIFiltered(transaction string) bool {
	return strings.HasPrefix(transaction, "/") && !(strings.Contains(transaction, "<") && strings.Contains(transaction, ">"))
}

// RebaseTransaction is the plugin transaction rebase: the
// content before the index-th occurrence of the delimiter; index < 2 or fewer
// occurrences keep the content unchanged.
func RebaseTransaction(content string, cond RebaseCondition) string {
	if cond.Index < 2 {
		return content
	}
	var occurrence int32
	start := 0
	for start <= len(content) {
		pos := strings.Index(content[start:], cond.Delimiter)
		if pos < 0 {
			break
		}
		pos += start
		occurrence++
		if occurrence == cond.Index {
			return content[:pos]
		}
		// An empty delimiter matches at start without advancing, as Java's
		// indexOf("", n) does; the occurrence counter still reaches index.
		start = pos + len(cond.Delimiter)
	}
	return content
}

// RemoveAfterConditions is the plugin global suffix removal: the
// suffix from the earliest occurrence of any condition is removed. The returned
// bool reports whether the content changed (and the hash must be recomputed).
func RemoveAfterConditions(content string, conditions []string) (string, bool) {
	if len(conditions) == 0 || content == "" {
		return content, false
	}
	earliest := -1
	for _, cond := range conditions {
		if cond == "" {
			continue
		}
		if pos := strings.Index(content, cond); pos >= 0 && (earliest == -1 || pos < earliest) {
			earliest = pos
		}
	}
	if earliest == -1 {
		return content, false
	}
	return content[:earliest], true
}
