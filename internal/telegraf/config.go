package telegraf

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// NamePrefix is ScouterTgMtConfig.X. It is prepended to the family and object
// name bases (not to the object type base).
const NamePrefix = "X$"

// Measurement is the resolved per-measurement mapping (ScouterTgMtConfig).
// It is immutable after construction except for the CounterProtocol fresh
// flags, which are atomic.
type Measurement struct {
	Name           string
	Enabled, Debug bool
	// Counters maps a line protocol field key to its counter mapping.
	Counters            map[string]*CounterProtocol
	ObjFamilyBase       string // NamePrefix + configured base
	ObjFamilyAppendTags []string
	ObjTypeBase         string
	ObjTypePrependTags  []string
	ObjTypeAppendTags   []string
	ObjTypeIcon         string
	ObjNameBase         string // NamePrefix + configured base
	ObjNameAppendTags   []string
	HostTag             string
	HostMapping         map[string]string
	// TagFilter is tag -> match strings; "!" prefix negates.
	TagFilter map[string][]string
}

// NewMeasurement is new ScouterTgMtConfig(name).
func NewMeasurement(name string) *Measurement {
	return &Measurement{Name: name, Enabled: true, Counters: map[string]*CounterProtocol{}, ObjFamilyBase: NamePrefix, ObjNameBase: NamePrefix, HostTag: "host", HostMapping: map[string]string{}, TagFilter: map[string][]string{}}
}

// Family is toFamily. A missing tag appends the Java string "null".
func (m *Measurement) Family(tags map[string]string) string {
	var b strings.Builder
	b.WriteString(m.ObjFamilyBase)
	for _, t := range m.ObjFamilyAppendTags {
		b.WriteByte('_')
		b.WriteString(javaString(tags, t))
	}
	return b.String()
}

// ObjType is toObjType: non-empty prepend tag values + '_', the base, then
// '_' + each append tag value.
func (m *Measurement) ObjType(tags map[string]string) string {
	var b strings.Builder
	for _, t := range m.ObjTypePrependTags {
		if v := tags[t]; v != "" {
			b.WriteString(v)
			b.WriteByte('_')
		}
	}
	b.WriteString(m.ObjTypeBase)
	for _, t := range m.ObjTypeAppendTags {
		b.WriteByte('_')
		b.WriteString(javaString(tags, t))
	}
	return b.String()
}

// Host is toHost: the host tag (or "unknown") after host mapping.
func (m *Measurement) Host(tags map[string]string) string {
	host, ok := tags[m.HostTag]
	if !ok {
		return "unknown"
	}
	if mapped, ok := m.HostMapping[host]; ok {
		return mapped
	}
	return host
}

// ObjName is toObjName: "/" + host + "/" + base + "_" + append tags.
func (m *Measurement) ObjName(host string, tags map[string]string) string {
	var b strings.Builder
	b.WriteByte('/')
	b.WriteString(host)
	b.WriteByte('/')
	b.WriteString(m.ObjNameBase)
	for _, t := range m.ObjNameAppendTags {
		b.WriteByte('_')
		b.WriteString(javaString(tags, t))
	}
	return b.String()
}

// TagFilterMatching is isTagFilterMatching: no filter matches everything;
// otherwise any (tag, match) pair satisfies it. "!v" matches when the tag
// value is not v, including a missing tag. Empty match strings are rejected
// when the configuration is built (Java throws at match time, D4).
func (m *Measurement) TagFilterMatching(tags map[string]string) bool {
	if len(m.TagFilter) == 0 {
		return true
	}
	for tag, matches := range m.TagFilter {
		value, present := tags[tag]
		for _, match := range matches {
			if match[0] == '!' {
				if !present || match[1:] != value {
					return true
				}
			} else if present && match == value {
				return true
			}
		}
	}
	return false
}

// Valid is isValidConfig: the family base must be set.
func (m *Measurement) Valid() bool { return m.ObjFamilyBase != "" && m.ObjFamilyBase != NamePrefix }

func javaString(tags map[string]string, key string) string {
	if v, ok := tags[key]; ok {
		return v
	}
	return "null"
}

// Config is the resolved Telegraf input configuration: the input_telegraf_*
// globals of Configure and telegrafInputConfigMap. A *Config is immutable once
// published; reload builds a new one.
type Config struct {
	Enabled                      bool
	Debug                        bool
	DeltaNormalizeDefault        bool
	DeltaNormalizeDefaultSeconds int
	ObjectDeadtimeMs             int
	// FromFile is true when scouter-telegraf.xml was applied. Java then forces
	// net_http_server_enabled=true when Enabled is set (ForcesHTTPServer).
	FromFile     bool
	Measurements map[string]*Measurement
}

// ForcesHTTPServer reports the applyTelegrafInputConfigNew side effect that
// turns net_http_server_enabled on.
func (c *Config) ForcesHTTPServer() bool { return c != nil && c.FromFile && c.Enabled }

// DefaultConfig is the Configure default with no XML file and no deprecated
// measurement keys.
func DefaultConfig() *Config {
	return &Config{Enabled: true, DeltaNormalizeDefault: true, DeltaNormalizeDefaultSeconds: 30, ObjectDeadtimeMs: 35000, Measurements: map[string]*Measurement{}}
}

const (
	propPrefix        = "input_telegraf_$"
	defaultPrependTag = DefaultTypePrependTag
)

// FromProperties applies the scouter.conf telegraf keys: the globals
// (applyConfig, values trimmed like Configure.getValue) and every
// input_telegraf_$<measurement>$_<postfix> key (applyTelegrafInputConfig,
// values used as given). Keys that make the Java parser throw are skipped and
// reported in warnings instead of aborting the reload.
func FromProperties(props map[string]string, now time.Time) (*Config, []string) {
	c := DefaultConfig()
	var warnings []string
	getBool := func(key string, def bool) bool {
		if v, ok := props[key]; ok {
			return strings.EqualFold(strings.TrimSpace(v), "true")
		}
		return def
	}
	getInt := func(key string, def int) int {
		if v, ok := props[key]; ok {
			if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 32); err == nil {
				return int(n)
			}
		}
		return def
	}
	c.Enabled = getBool("input_telegraf_enabled", true)
	c.Debug = getBool("input_telegraf_debug_enabled", false)
	c.DeltaNormalizeDefault = getBool("input_telegraf_delta_counter_normalize_default", true)
	c.DeltaNormalizeDefaultSeconds = getInt("input_telegraf_delta_counter_normalize_default_seconds", 30)
	c.ObjectDeadtimeMs = getInt("telegraf_object_deadtime_ms", 35000)

	keys := make([]string, 0, len(props))
	for k := range props {
		if strings.HasPrefix(k, propPrefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := props[key]
		simplified := key[len(propPrefix):]
		second := strings.Index(simplified, "$_")
		if second < 0 {
			warnings = append(warnings, fmt.Sprintf("%s: missing $_ after the measurement name; ignored", key))
			continue
		}
		name, postfix := simplified[:second], simplified[second+1:]
		m := c.Measurements[name]
		if m == nil {
			m = NewMeasurement(name)
			m.ObjTypePrependTags = []string{defaultPrependTag}
			c.Measurements[name] = m
		}
		switch postfix {
		case "_enabled":
			m.Enabled = strings.EqualFold(value, "true")
		case "_debug_enabled":
			m.Debug = strings.EqualFold(value, "true")
		case "_tag_filter":
			mappings := splitDropEmpty(value, ',')
			if len(mappings) == 0 {
				continue
			}
			filter := map[string][]string{}
			for _, mapping := range mappings {
				kv := splitDropEmpty(mapping, ':')
				if len(kv) != 2 {
					warnings = append(warnings, fmt.Sprintf("%s: abnormal tag filter %q (CFG003)", key, mapping))
					continue
				}
				filter[kv[0]] = append(filter[kv[0]], kv[1])
			}
			m.TagFilter = filter
		case "_counter_mappings":
			entries := splitDropEmpty(value, ',')
			if len(entries) == 0 {
				continue
			}
			counters := map[string]*CounterProtocol{}
			for _, entry := range entries {
				field, cp, err := parseDeprecatedCounter(entry, now)
				if err != nil {
					warnings = append(warnings, fmt.Sprintf("%s: %v", key, err))
					continue
				}
				counters[field] = cp
			}
			m.Counters = counters
		case "_objFamily_base":
			m.ObjFamilyBase = NamePrefix + value
		case "_objFamily_append_tags":
			if tags := splitDropEmpty(value, ','); len(tags) > 0 {
				m.ObjFamilyAppendTags = tags
			}
		case "_objType_base":
			m.ObjTypeBase = value
		case "_objType_prepend_tags":
			if value == "" {
				value = defaultPrependTag
			}
			if tags := splitDropEmpty(value, ','); len(tags) > 0 {
				m.ObjTypePrependTags = tags
			}
		case "_objType_append_tags":
			if tags := splitDropEmpty(value, ','); len(tags) > 0 {
				m.ObjTypeAppendTags = tags
			}
		case "_objType_icon":
			m.ObjTypeIcon = value
		case "_objName_base":
			m.ObjNameBase = NamePrefix + value
		case "_objName_append_tags":
			if tags := splitDropEmpty(value, ','); len(tags) > 0 {
				m.ObjNameAppendTags = tags
			}
		case "_host_tag":
			m.HostTag = value
		case "_host_mappings":
			mappings := splitDropEmpty(value, ',')
			if len(mappings) == 0 {
				continue
			}
			hosts := map[string]string{}
			for _, mapping := range mappings {
				if kv := splitDropEmpty(mapping, ':'); len(kv) == 2 {
					hosts[kv[0]] = kv[1]
				}
			}
			m.HostMapping = hosts
		}
	}
	return c, warnings
}

// parseDeprecatedCounter is one {field}:{counter}:{display?}:{unit?}:{total?}:{normalizeSec?}
// entry. A field prefixed "&" is DELTA and "&&" BOTH. Defaults: unit "",
// total true, normalize 0.
func parseDeprecatedCounter(entry string, now time.Time) (string, *CounterProtocol, error) {
	split := splitPreserveAll(entry, ':')
	if len(split) < 2 {
		return "", nil, fmt.Errorf("counter mapping %q needs field:counter", entry)
	}
	field := split[0]
	cp := NewCounterProtocol(now)
	switch {
	case field == "" || field == "&":
		return "", nil, fmt.Errorf("counter mapping %q has an empty field name", entry)
	case strings.HasPrefix(field, "&&"):
		cp.DeltaType, field = DeltaBoth, field[2:]
	case field[0] == '&':
		cp.DeltaType, field = DeltaDelta, field[1:]
	}
	cp.SetName(split[1])
	if len(split) >= 3 && split[2] != "" {
		cp.SetDisplayName(split[2])
	} else {
		cp.SetDisplayName(split[1])
	}
	if len(split) >= 4 {
		cp.Unit = split[3]
	}
	if len(split) >= 5 {
		cp.Total = strings.EqualFold(split[4], "true")
	}
	if len(split) >= 6 {
		n, err := strconv.ParseInt(split[5], 10, 32)
		if err != nil {
			n = 0
		}
		cp.SetNormalizeSec(int(n))
	}
	return field, cp, nil
}

// CounterProtocolOf is CounterProtocol.of(TgCounterMapping).
func CounterProtocolOf(m CounterMapping, now time.Time) *CounterProtocol {
	cp := NewCounterProtocol(now)
	counterName := ""
	if m.CounterName != nil {
		counterName = *m.CounterName
	}
	cp.SetName(counterName)
	cp.DeltaType = m.DeltaType
	if m.DisplayName != nil && *m.DisplayName != "" {
		cp.SetDisplayName(*m.DisplayName)
	} else {
		cp.SetDisplayName(counterName)
	}
	cp.Unit = m.Unit
	cp.Total = m.Totalizable
	cp.SetNormalizeSec(m.NormalizeSeconds)
	return cp
}

// ApplyFile is applyTelegrafInputConfigNew: the file globals overwrite the
// receiver's, every file measurement replaces the deprecated one of the same
// name, and deprecated measurements absent from the file are kept. Java
// Collectors.toMap failures (duplicate tgFieldName or hostMapping.telegraf,
// null mapping values, a tagFilter without match) and the empty match strings
// that throw at request time are returned as an error; the receiver is not
// modified.
func (c *Config) ApplyFile(f *File, now time.Time) (*Config, error) {
	out := &Config{Enabled: f.Enabled, Debug: f.DebugEnabled, DeltaNormalizeDefault: f.DeltaCounterNormalizeDefault, DeltaNormalizeDefaultSeconds: f.DeltaCounterNormalizeDefaultSeconds, ObjectDeadtimeMs: f.ObjectDeadtimeMs, FromFile: true, Measurements: map[string]*Measurement{}}
	var errs []error
	for i, mf := range f.Measurements {
		if mf.MeasurementName == nil {
			errs = append(errs, fmt.Errorf("measurement #%d has no measurementName", i+1))
			continue
		}
		name := *mf.MeasurementName
		m := out.Measurements[name]
		if m == nil {
			m = NewMeasurement(name)
			out.Measurements[name] = m
		}
		m.Enabled, m.Debug = mf.Enabled, mf.DebugEnabled
		filter := map[string][]string{}
		for _, tf := range mf.TagFilters {
			if tf.Tag == nil || tf.Match == nil {
				errs = append(errs, fmt.Errorf("measurement %q: tagFilter needs tag and match", name))
				continue
			}
			for _, v := range tf.Match {
				if v == "" {
					errs = append(errs, fmt.Errorf("measurement %q: tagFilter %q has an empty match", name, *tf.Tag))
				}
			}
			filter[*tf.Tag] = append(filter[*tf.Tag], tf.Match...)
		}
		m.TagFilter = filter
		m.ObjFamilyBase = NamePrefix + mf.ObjFamilyBase
		m.ObjFamilyAppendTags = mf.ObjFamilyAppendTags
		m.ObjTypeBase = mf.ObjTypeBase
		m.ObjTypePrependTags = mf.ObjTypePrependTags
		m.ObjTypeAppendTags = mf.ObjTypeAppendTags
		m.ObjTypeIcon = mf.ObjTypeIcon
		m.ObjNameBase = NamePrefix + mf.ObjNameBase
		m.ObjNameAppendTags = mf.ObjNameAppendTags
		m.HostTag = mf.HostTag
		hosts := map[string]string{}
		for _, h := range mf.HostMappings {
			if h.Telegraf == nil || h.Scouter == nil {
				errs = append(errs, fmt.Errorf("measurement %q: hostMapping needs telegraf and scouter", name))
				continue
			}
			if _, dup := hosts[*h.Telegraf]; dup {
				errs = append(errs, fmt.Errorf("measurement %q: duplicate hostMapping %q", name, *h.Telegraf))
				continue
			}
			hosts[*h.Telegraf] = *h.Scouter
		}
		m.HostMapping = hosts
		counters := map[string]*CounterProtocol{}
		for _, cm := range mf.CounterMappings {
			if cm.TgFieldName == nil {
				errs = append(errs, fmt.Errorf("measurement %q: counterMapping without tgFieldName", name))
				continue
			}
			if _, dup := counters[*cm.TgFieldName]; dup {
				errs = append(errs, fmt.Errorf("measurement %q: duplicate tgFieldName %q", name, *cm.TgFieldName))
				continue
			}
			counters[*cm.TgFieldName] = CounterProtocolOf(cm, now)
		}
		m.Counters = counters
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	for name, m := range c.Measurements {
		if _, ok := out.Measurements[name]; !ok {
			out.Measurements[name] = m
		}
	}
	return out, nil
}

// Load resolves the startup configuration like Configure.reload: the
// deprecated properties first, then the XML file when it exists. When the
// file is missing the deprecated configuration is returned with FromFile
// false. When the file cannot be read, is malformed or fails ApplyFile, err is
// non-nil and cfg is the Java fallback (a default TgConfig applied over the
// deprecated measurements, as JAXBException leaves the initial TgConfig in
// place); the caller must report err.
func Load(props map[string]string, path string, now time.Time) (cfg *Config, warnings []string, err error) {
	data, readErr := readBounded(path, MaxFileBytes)
	if errors.Is(readErr, os.ErrNotExist) {
		return Compose(props, nil, false, now)
	}
	if readErr != nil {
		cfg, warnings, _ = Compose(props, nil, false, now)
		cfg, _ = cfg.ApplyFile(DefaultFile(), now)
		return cfg, warnings, readErr
	}
	cfg, warnings, err = Compose(props, data, true, now)
	if err != nil {
		err = fmt.Errorf("telegraf config %s: %w", path, err)
	}
	return cfg, warnings, err
}

// Compose rebuilds the whole configuration from both sources: the
// scouter.conf properties (globals and deprecated measurement keys) and, when
// exists, the XML text. Callers run it on every change of either source, so
// the result never depends on which source changed last (Java D13: a
// scouter.conf-only reload reverts the XML globals and leaves the XML
// measurements stale until the XML mtime changes). err has the Load meaning:
// cfg is then the Java fallback (default TgConfig over the deprecated
// measurements).
func Compose(props map[string]string, xmlText []byte, exists bool, now time.Time) (cfg *Config, warnings []string, err error) {
	base, warnings := FromProperties(props, now)
	if !exists {
		return base, warnings, nil
	}
	fallback := func(cause error) (*Config, []string, error) {
		cfg, _ := base.ApplyFile(DefaultFile(), now)
		return cfg, warnings, cause
	}
	file, fileWarnings, parseErr := ParseFile(xmlText)
	warnings = append(warnings, fileWarnings...)
	if parseErr != nil {
		return fallback(parseErr)
	}
	applied, applyErr := base.ApplyFile(file, now)
	if applyErr != nil {
		return fallback(applyErr)
	}
	return applied, warnings, nil
}

// readBounded reads at most limit bytes: the size is checked before reading
// and the read itself is capped, so a growing or special file cannot make it
// allocate more than limit+1 bytes.
func readBounded(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if info, err := f.Stat(); err != nil {
		return nil, err
	} else if info.Size() > limit {
		return nil, fmt.Errorf("telegraf config: %d bytes exceeds %d", info.Size(), limit)
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("telegraf config: more than %d bytes", limit)
	}
	return data, nil
}
