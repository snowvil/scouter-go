package counters

import "strings"

// Counter is scouter.lang.Counter. Counters without a name attribute are
// dropped like Java's getName() != null check.
type Counter struct {
	Name, Display, Unit, Icon string
	All, Total                bool
	Attributes                []Attribute
}

// Attribute is an unrecognized XML attribute kept by CounterEngine.parse.
type Attribute struct{ Name, Value string }

// Family is scouter.lang.Family.
type Family struct {
	Name, Master string
	HasMaster    bool
	Attributes   []Attribute
	counters     linked[*Counter]
}

// Counters returns the family counters in StringKeyLinkedMap order.
func (f *Family) Counters() []*Counter { return f.counters.values() }

// ObjectType is scouter.lang.ObjectType. Family is nil when the referenced
// family was unknown at parse time (Java familyMap.get returned null).
type ObjectType struct {
	Name, Display, Icon string
	HasDisplay, HasIcon bool
	Family              *Family
	SubObject           bool
	Attributes          []Attribute
	counters            linked[*Counter]
}

// Counters returns the object type's own counters (listObjectTypeCounters).
func (o *ObjectType) Counters() []*Counter { return o.counters.values() }

// Engine is CounterEngine: families and object types accumulated by one or
// more Parse calls. An Engine is not safe for concurrent mutation; Store
// publishes only fully built engines.
type Engine struct {
	families linked[*Family]
	types    linked[*ObjectType]
	maxBytes int
}

// NewEngine returns an empty engine accepting documents up to maxBytes.
func NewEngine(maxBytes int) *Engine {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	return &Engine{maxBytes: maxBytes}
}

// Family is familyMap.get(name).
func (e *Engine) Family(name string) *Family { return e.families.get(name) }

// ObjectType is objTypeMap.get(name).
func (e *Engine) ObjectType(name string) *ObjectType { return e.types.get(name) }

// Families returns families in insertion order.
func (e *Engine) Families() []*Family { return e.families.values() }

// ObjectTypes returns object types in insertion order.
func (e *Engine) ObjectTypes() []*ObjectType { return e.types.values() }

// Parse is CounterEngine.parse(byte[]): it returns ErrMalformed exactly when
// the Java DOM parse would throw (Java returns false). On success, elements
// are merged into the engine with Java's put/replace semantics.
func (e *Engine) Parse(content []byte) error {
	doc, err := parseDocument(content, e.maxBytes)
	if err != nil {
		return err
	}
	e.apply(doc)
	return nil
}

func javaBoolean(v string) bool { return strings.EqualFold(v, "true") }

func parseCounter(el *element) (*Counter, bool) {
	c := &Counter{All: true, Total: true}
	hasName := false
	for _, a := range el.attrs {
		switch a.qname {
		case "name":
			c.Name, hasName = a.value, true
		case "disp":
			c.Display = a.value
		case "unit":
			c.Unit = a.value
		case "icon":
			c.Icon = a.value
		case "all":
			c.All = javaBoolean(a.value)
		case "total":
			c.Total = javaBoolean(a.value)
		default:
			c.Attributes = append(c.Attributes, Attribute{a.qname, a.value})
		}
	}
	return c, hasName
}

func (e *Engine) apply(doc *document) {
	for _, el := range doc.elementsByTagName("Family") {
		f := &Family{}
		hasName := false
		for _, a := range el.attrs {
			switch a.qname {
			case "name":
				f.Name, hasName = a.value, true
			case "master":
				f.Master, f.HasMaster = a.value, true
			default:
				f.Attributes = append(f.Attributes, Attribute{a.qname, a.value})
			}
		}
		if hasName {
			e.families.put(f.Name, f)
		}
		for _, cel := range descendantsByTagName(el, "Counter") {
			if c, ok := parseCounter(cel); ok {
				f.counters.put(c.Name, c)
			}
		}
	}
	for _, el := range doc.elementsByTagName("ObjectType") {
		o := &ObjectType{}
		hasName := false
		for _, a := range el.attrs {
			switch a.qname {
			case "name":
				o.Name, hasName = a.value, true
			case "disp":
				o.Display, o.HasDisplay = a.value, true
			case "family":
				o.Family = e.families.get(a.value)
			case "icon":
				o.Icon, o.HasIcon = a.value, true
			case "sub-object":
				o.SubObject = javaBoolean(a.value)
			default:
				o.Attributes = append(o.Attributes, Attribute{a.qname, a.value})
			}
		}
		if hasName {
			if exist := e.types.get(o.Name); exist != nil {
				o = exist
			} else {
				e.types.put(o.Name, o)
			}
		}
		for _, cel := range descendantsByTagName(el, "Counter") {
			if c, ok := parseCounter(cel); ok {
				o.counters.put(c.Name, c)
			}
		}
	}
}

// linked is StringKeyLinkedMap with put(MODE.LAST): an existing key keeps
// its position and takes the new value.
type linked[V any] struct {
	index map[string]int
	keys  []string
	vals  []V
}

func (m *linked[V]) put(k string, v V) {
	if m.index == nil {
		m.index = map[string]int{}
	}
	if i, ok := m.index[k]; ok {
		m.vals[i] = v
		return
	}
	m.index[k] = len(m.keys)
	m.keys = append(m.keys, k)
	m.vals = append(m.vals, v)
}

func (m *linked[V]) get(k string) V {
	var zero V
	if i, ok := m.index[k]; ok {
		return m.vals[i]
	}
	return zero
}

func (m *linked[V]) values() []V { return append([]V(nil), m.vals...) }
