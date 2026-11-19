// Package telegraf ports the Scouter server's Telegraf HTTP input: the
// scouter-telegraf.xml model (TgConfig), the resolved per-measurement mapping
// (ScouterTgMtConfig), the InfluxDB line protocol reader (InfluxSingleLine),
// delta/normalize counter arithmetic and the ObjectPack/PerfCounterPack
// conversion. It has no network dependency; internal/httpapi mounts it.
package telegraf

import (
	"strings"
	"sync/atomic"
	"time"
)

// DeltaType mirrors scouter.lang.DeltaType.
type DeltaType string

const (
	DeltaNone  DeltaType = "NONE"
	DeltaDelta DeltaType = "DELTA"
	DeltaBoth  DeltaType = "BOTH"
)

// Counter.MIN_NORMALIZE_SEC / MAX_NORMALIZE_SEC.
const (
	MinNormalizeSec = 4
	MaxNormalizeSec = 60
)

// CounterDef carries the scouter.lang.Counter fields that reach counters.xml.
type CounterDef struct {
	Name, DisplayName, Unit, Icon string
	Total, All                    bool
}

// SemanticEquals is Counter.semanticEquals: total/name/displayName/unit/icon.
func (c CounterDef) SemanticEquals(o CounterDef) bool {
	return c.Total == o.Total && c.Name == o.Name && c.DisplayName == o.DisplayName && c.Unit == o.Unit && c.Icon == o.Icon
}

// FamilyDef is scouter.lang.Family as the HTTP handlers build it.
type FamilyDef struct {
	Name, Master string
	Counters     []CounterDef
}

// ObjectTypeDef is scouter.lang.ObjectType as the HTTP handlers see it.
// Counters are the type's own counters plus the family counters
// (ObjectType.getCounter searches both).
type ObjectTypeDef struct {
	Name, DisplayName, Icon, Family string
	SubObject                       bool
	Counters                        []CounterDef
}

// Counter returns the counter named name, as ObjectType.getCounter.
func (t ObjectTypeDef) Counter(name string) (CounterDef, bool) {
	for _, c := range t.Counters {
		if c.Name == name {
			return c, true
		}
	}
	return CounterDef{}, false
}

// TypeRegistrar abstracts the Java CounterManager side effects of the HTTP
// inputs: Telegraf registers unknown object types and counters, /register adds
// families and types, and both notify logged-in clients with
// REFETCH_COUNTER_XML. The Go counter catalog (counters.site.xml) is owned by
// another package. A nil registrar makes the Telegraf input ingest without
// registering (logged once per object type) and makes /register answer 500.
// Implementations must be safe for concurrent use; the Telegraf input calls
// ObjectType from request goroutines and the mutating methods from its single
// registration goroutine.
type TypeRegistrar interface {
	// ObjectType is CounterEngine.getObjectType; the returned counters include
	// the family counters (ObjectType.getCounter searches both).
	ObjectType(name string) (ObjectTypeDef, bool)
	// HasFamily is CounterEngine.getFamily != null.
	HasFamily(name string) bool
	// AddFamilyAndObjectType is CounterManager.addFamilyAndObjectType
	// (/register, new object type).
	AddFamilyAndObjectType(family FamilyDef, objectType ObjectTypeDef) error
	// EditObjectType is CounterManager.editObjectType: the definition carries
	// only the counters to add (/register, existing object type).
	EditObjectType(objectType ObjectTypeDef) error
	// SafelyAddFamily is CounterManager.safelyAddFamily: replace the family of
	// the same name as a whole (Telegraf new object type).
	SafelyAddFamily(family FamilyDef) error
	// SafelyAddObjectType is CounterManager.safelyAddObjectType.
	SafelyAddObjectType(objectType ObjectTypeDef) error
	// AddFamilyCounter is objectType.getFamily().addCounter(counter) followed
	// by safelyAddFamily: add or replace one counter of an existing family
	// (Telegraf new counter).
	AddFamilyCounter(family string, counter CounterDef) error
	// NotifyClients is RegisterHandler.notifyAllClients (REFETCH_COUNTER_XML
	// to every login session). It must not block the caller.
	NotifyClients()
}

// CounterProtocol is scouter.server.http.model.CounterProtocol: a counter
// mapping whose name/display name may embed line protocol tags as $tag$.
type CounterProtocol struct {
	name            string
	nameTags        []string
	displayName     string
	displayNameTags []string
	Unit, Icon      string
	Total, All      bool
	NormalizeSec    int
	DeltaType       DeltaType
	created         time.Time
	// stale is the negation of the Java fresh flag. Java flips it after a
	// check that ran more than 15s after creation, so the first check past the
	// window still compares deeply.
	stale atomic.Bool
}

// keepFreshMillis is CounterProtocol.KEEP_FRESH_MILLIS: while fresh, counter
// comparisons also check display name/unit/icon/total.
const keepFreshMillis = 15 * 1000

// NewCounterProtocol builds the Java default (all=true, total=true, unit "",
// icon "", normalize 0, NONE); callers apply the mapping afterwards.
func NewCounterProtocol(now time.Time) *CounterProtocol {
	return &CounterProtocol{All: true, Total: true, DeltaType: DeltaNone, created: now}
}

// SetName is CounterProtocol.setName: "$tag$" segments become '*' in the stored
// name and the tag names are kept in order.
func (c *CounterProtocol) SetName(nameWithTag string) {
	c.name, c.nameTags = splitTaggedName(nameWithTag)
}

// SetDisplayName is CounterProtocol.setDisplayName.
func (c *CounterProtocol) SetDisplayName(displayNameWithTag string) {
	c.displayName, c.displayNameTags = splitTaggedName(displayNameWithTag)
}

// SetNormalizeSec is CounterProtocol.setNormalizeSec: below the minimum
// disables normalization, above the maximum clamps.
func (c *CounterProtocol) SetNormalizeSec(sec int) {
	switch {
	case sec < MinNormalizeSec:
		c.NormalizeSec = 0
	case sec > MaxNormalizeSec:
		c.NormalizeSec = MaxNormalizeSec
	default:
		c.NormalizeSec = sec
	}
}

// Name is the stored name with '*' placeholders.
func (c *CounterProtocol) Name() string { return c.name }

// DisplayName is the stored display name with '*' placeholders.
func (c *CounterProtocol) DisplayName() string { return c.displayName }

func splitTaggedName(s string) (name string, tags []string) {
	var nameB, tagB strings.Builder
	sink := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '$' {
			sink = !sink
			if !sink {
				nameB.WriteByte('*')
				tagB.WriteByte('*')
			}
			continue
		}
		if sink {
			tagB.WriteByte(ch)
		} else {
			nameB.WriteByte(ch)
		}
	}
	if tagB.Len() > 0 {
		tags = splitDropEmpty(tagB.String(), '*')
	}
	return nameB.String(), tags
}

// generateTaggedName substitutes each '*' with the tag value in order. A
// missing tag yields the Java string "null"; a '*' beyond the tag list, which
// throws IndexOutOfBoundsException in Java, also yields "null" here.
func generateTaggedName(name string, tags []string, tagMap map[string]string) string {
	if len(tags) == 0 || strings.IndexByte(name, '*') < 0 {
		return name
	}
	var b strings.Builder
	b.Grow(len(name))
	n := 0
	for i := 0; i < len(name); i++ {
		if name[i] != '*' {
			b.WriteByte(name[i])
			continue
		}
		if n < len(tags) {
			if v, ok := tagMap[tags[n]]; ok {
				b.WriteString(v)
			} else {
				b.WriteString("null")
			}
		} else {
			b.WriteString("null")
		}
		n++
	}
	return b.String()
}

// TaggedName is CounterProtocol.getTaggedName.
func (c *CounterProtocol) TaggedName(tags map[string]string) string {
	return generateTaggedName(c.name, c.nameTags, tags)
}

// TaggedDeltaName is CounterProtocol.getTaggedDelataName.
func (c *CounterProtocol) TaggedDeltaName(tags map[string]string) string {
	return c.TaggedName(tags) + "_$delta"
}

// TaggedDisplayName is CounterProtocol.getTaggedDisplayName.
func (c *CounterProtocol) TaggedDisplayName(tags map[string]string) string {
	return generateTaggedName(c.displayName, c.displayNameTags, tags)
}

// TaggedDeltaDisplayName is CounterProtocol.getTaggedDeltaDisplayName.
func (c *CounterProtocol) TaggedDeltaDisplayName(tags map[string]string) string {
	return c.TaggedDisplayName(tags) + "_$delta"
}

// DeltaUnit is CounterProtocol.getDelateUnit.
func (c *CounterProtocol) DeltaUnit() string { return c.Unit + "/s" }

// HasDelta is hasDeltaCounter (DELTA or BOTH).
func (c *CounterProtocol) HasDelta() bool {
	return c.DeltaType == DeltaDelta || c.DeltaType == DeltaBoth
}

// HasNormal is hasNormalCounter (anything but DELTA).
func (c *CounterProtocol) HasNormal() bool { return c.DeltaType != DeltaDelta }

// NormalCounter is toNormalCounter.
func (c *CounterProtocol) NormalCounter(tags map[string]string) (CounterDef, bool) {
	if !c.HasNormal() {
		return CounterDef{}, false
	}
	return CounterDef{Name: c.TaggedName(tags), DisplayName: c.TaggedDisplayName(tags), Unit: c.Unit, Icon: c.Icon, Total: c.Total, All: c.All}, true
}

// DeltaCounter is toDeltaCounter.
func (c *CounterProtocol) DeltaCounter(tags map[string]string) (CounterDef, bool) {
	if !c.HasDelta() {
		return CounterDef{}, false
	}
	return CounterDef{Name: c.TaggedDeltaName(tags), DisplayName: c.TaggedDeltaDisplayName(tags), Unit: c.DeltaUnit(), Icon: c.Icon, Total: c.Total, All: c.All}, true
}

// Counters is toCounters: the normal counter then the delta counter.
func (c *CounterProtocol) Counters(tags map[string]string) []CounterDef {
	var out []CounterDef
	if n, ok := c.NormalCounter(tags); ok {
		out = append(out, n)
	}
	if d, ok := c.DeltaCounter(tags); ok {
		out = append(out, d)
	}
	return out
}

// NewOrChanged is isNewOrChangedCounter: true when the object type lacks the
// tagged normal or delta counter, or, while the mapping is fresh, when display
// name/unit/icon/total differ. The delta counter is checked only when the
// normal counter is unchanged. Safe for concurrent use.
func (c *CounterProtocol) NewOrChanged(objectType ObjectTypeDef, tags map[string]string, now time.Time) bool {
	fresh := !c.stale.Load()
	changed := func(delta bool) bool {
		name, display, unit := c.TaggedName(tags), c.TaggedDisplayName(tags), c.Unit
		if delta {
			name, display, unit = c.TaggedDeltaName(tags), c.TaggedDeltaDisplayName(tags), c.DeltaUnit()
		}
		existing, ok := objectType.Counter(name)
		if !ok {
			return true
		}
		return fresh && (display != existing.DisplayName || unit != existing.Unit || c.Icon != existing.Icon || c.Total != existing.Total)
	}
	isNew := c.HasNormal() && changed(false)
	if !isNew {
		isNew = c.HasDelta() && changed(true)
	}
	if now.Sub(c.created) > keepFreshMillis*time.Millisecond {
		c.stale.Store(true)
	}
	return isNew
}

// splitDropEmpty is scouter.util.StringUtil.split: empty tokens are dropped
// and nothing is trimmed.
func splitDropEmpty(s string, sep byte) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == sep {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}

// splitPreserveAll is StringUtil.splitByWholeSeparatorPreserveAllTokens:
// interior empty tokens are kept but a trailing empty token is not.
func splitPreserveAll(s string, sep byte) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}
