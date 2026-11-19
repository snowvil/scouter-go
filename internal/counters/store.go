package counters

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"unicode/utf8"
)

// CustomFileName is CounterManager.CUSTOM_FILENAME.
const CustomFileName = "counters.site.xml"

// DefaultMaxBytes bounds counters XML documents and files.
const DefaultMaxBytes = 4 << 20

// ErrUnsupportedDocument marks a custom document that Java would still
// process through a Xalan serialization this package does not reproduce
// (custom root element other than <Counters>, or namespace usage).
var ErrUnsupportedDocument = errors.New("counters: custom document layout not supported by the Go writer")

// StoreConfig configures the custom counters file owner.
type StoreConfig struct {
	// Dir is the configuration directory (Java Configure.CONF_DIR). The
	// custom file is Dir/counters.site.xml.
	Dir string
	// Default is the bundled counters.xml (scouter/lang/counters/counters.xml).
	Default []byte
	// MaxBytes bounds every document; 0 selects DefaultMaxBytes.
	MaxBytes int
}

// ObjectTypeSpec is the DEFINE/EDIT_OBJECT_TYPE request
// (MapPack name/disp/family/icon/sub-object).
// Counters are the type's own counters (ObjectType.listObjectTypeCounters),
// written as <Counter> children; DEFINE/EDIT_OBJECT_TYPE send none.
type ObjectTypeSpec struct {
	Name, Display, Family, Icon string
	SubObject                   bool
	Counters                    []CounterSpec
}

// CounterSpec is the scouter.lang.Counter subset CounterManager writes.
type CounterSpec struct {
	Name, Display, Unit, Icon string
	All, Total                bool
}

// FamilySpec is the scouter.lang.Family subset CounterManager writes.
type FamilySpec struct {
	Name, Master string
	Counters     []CounterSpec
}

// Store owns the custom counters file and the merged engine
// (CounterManager). Mutations are serialized; readers get immutable
// snapshots. The engine is rebuilt and swapped as a whole, unlike Java's
// clear-then-parse reload.
type Store struct {
	mu       sync.RWMutex
	dir      string
	maxBytes int
	defaults []byte
	custom   []byte
	engine   *Engine
}

// Open parses the default XML (it must be valid) and the current custom file
// if present. An unparsable custom file is kept as bytes (GET_XML_COUNTER
// still returns it) and contributes nothing to the engine, as in Java.
func Open(cfg StoreConfig) (*Store, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("counters: empty configuration directory")
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = DefaultMaxBytes
	}
	if cfg.MaxBytes < 1 || cfg.MaxBytes > DefaultMaxBytes {
		return nil, fmt.Errorf("counters: invalid MaxBytes")
	}
	s := &Store{dir: cfg.Dir, maxBytes: cfg.MaxBytes, defaults: append([]byte(nil), cfg.Default...)}
	if err := NewEngine(cfg.MaxBytes).Parse(s.defaults); err != nil {
		return nil, fmt.Errorf("counters: default XML: %w", err)
	}
	custom, found, err := ReadFile(cfg.Dir, CustomFileName, cfg.MaxBytes)
	if err != nil {
		return nil, err
	}
	if found {
		s.custom = custom
	}
	s.engine = s.build()
	return s, nil
}

func (s *Store) build() *Engine {
	e := NewEngine(s.maxBytes)
	_ = e.Parse(s.defaults) // validated in Open
	if s.custom != nil {
		_ = e.Parse(s.custom) // Java: printStackTrace, engine keeps defaults
	}
	return e
}

// Engine returns the current merged engine snapshot. Callers must not mutate it.
func (s *Store) Engine() *Engine {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.engine
}

// DefaultXML returns a copy of the bundled counters.xml bytes.
func (s *Store) DefaultXML() []byte { return append([]byte(nil), s.defaults...) }

// CustomXML returns a copy of CounterManager.xmlCustomContent: the custom
// bytes read at Open or written by the last successful save; nil when the
// file did not exist. GET_XML_COUNTER sends it as "custom" only when non-empty.
func (s *Store) CustomXML() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.custom == nil {
		return nil
	}
	return append([]byte(nil), s.custom...)
}

// Text is readCountersSiteXml: the current file decoded as UTF-8 (malformed
// bytes become U+FFFD like new String(bytes, "utf-8")), or "" when absent.
func (s *Store) Text(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	data, found, err := ReadFile(s.dir, CustomFileName, s.maxBytes)
	if err != nil || !found {
		return "", err
	}
	return javaUTF8(data), nil
}

func javaUTF8(data []byte) string {
	if utf8.Valid(data) {
		return string(data)
	}
	out := make([]rune, 0, len(data))
	for len(data) > 0 {
		r, n := utf8.DecodeRune(data)
		out = append(out, r) // RuneError for each malformed byte
		data = data[n:]
	}
	return string(out)
}

// Save is SET_CONFIGURE_COUNTERS_SITE after the null check:
// CounterEngine.parse(contents) && saveAndReloadCountersSiteXml(contents).
// false: Java parse rejection. An empty <Types/> is accepted (Java NPE;
// decision §9-3).
// Other errors are Go failures (limit, unsafe target, I/O, cancellation).
func (s *Store) Save(ctx context.Context, contents string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if len(contents) > s.maxBytes {
		return false, ErrLimit
	}
	out, err := normalizeContents([]byte(contents), s.maxBytes)
	if errors.Is(err, ErrMalformed) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitLocked(ctx, out)
}

// normalizeContents is the SET_CONFIGURE_COUNTERS_SITE content pipeline:
// CounterEngine.parse validity (ErrMalformed), then normalizeCountersXml.
// When JAXB throws JAXBException the original contents are returned, as Java
// saves them unchanged. JAXB reads the Java String, so a leading U+FEFF
// (which the byte-level DOM parse skips as a BOM) makes JAXB fail.
func normalizeContents(contents []byte, maxBytes int) ([]byte, error) {
	doc, err := parseDocument(contents, maxBytes)
	if err != nil {
		return nil, err
	}
	out, ok, err := normalize(doc)
	if err != nil {
		return nil, err
	}
	if !ok || bytes.HasPrefix(contents, utf8BOM) {
		return append([]byte(nil), contents...), nil
	}
	return out, nil
}

func (s *Store) commitLocked(ctx context.Context, out []byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := WriteFile(s.dir, CustomFileName, out, 0o644); err != nil {
		return false, err
	}
	s.custom = out
	e := NewEngine(s.maxBytes)
	ok := e.Parse(s.defaults) == nil
	ok = ok && e.Parse(out) == nil
	if !ok {
		// Java reloadEngine returns false after the file is saved; keep the
		// previous engine instead of a partial one.
		return false, nil
	}
	s.engine = e
	return true, nil
}

// DefineObjectType is CounterManager.addObjectType(MapPack): false when the
// type exists, the family is unknown or the custom file cannot be parsed.
func (s *Store) DefineObjectType(ctx context.Context, spec ObjectTypeSpec) (bool, error) {
	return s.mutateType(ctx, spec, false)
}

// EditObjectType is CounterManager.editObjectType(MapPack) with the custom
// file actually written: Java builds the edited document but discards it
// (intended difference, CONTRACT.md).
func (s *Store) EditObjectType(ctx context.Context, spec ObjectTypeSpec) (bool, error) {
	return s.mutateType(ctx, spec, true)
}

func (s *Store) mutateType(ctx context.Context, spec ObjectTypeSpec, edit bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !edit && s.engine.ObjectType(spec.Name) != nil {
		return false, nil
	}
	family := s.engine.Family(spec.Family)
	if family == nil {
		return false, nil // setObjectTypeAttribute NPE caught by append/edit
	}
	err := s.mutateLocked(ctx, func(doc *document, root *element) error {
		var target *element
		if edit {
			for _, el := range doc.elementsByTagName("ObjectType") {
				if v, _ := el.attr("name"); v == spec.Name {
					target = el
					break
				}
			}
		}
		if target == nil {
			target = newElement("ObjectType")
			section(doc, root, "Types").children = append(section(doc, root, "Types").children, target)
		}
		setObjectTypeAttrs(target, spec, family.Name)
		return nil
	})
	switch {
	case errors.Is(err, ErrMalformed), errors.Is(err, errSecondRoot):
		return false, nil // getCustomDocument null / DOMException, caught -> false
	case err != nil:
		return false, err
	}
	return true, nil
}

// errSecondRoot: Java appends a second document element (DOMException).
var errSecondRoot = errors.New("counters: custom document root is not <Counters>")

// mutateLocked applies change to the latest custom document on disk (a
// missing file is an empty document), normalizes, writes and rebuilds the
// engine. The caller holds s.mu.
func (s *Store) mutateLocked(ctx context.Context, change func(doc *document, root *element) error) error {
	raw, found, err := ReadFile(s.dir, CustomFileName, s.maxBytes)
	if err != nil {
		return err
	}
	doc := &document{}
	if found {
		if doc, err = parseDocument(raw, s.maxBytes); err != nil {
			return err
		}
	}
	var root *element
	if found := doc.elementsByTagName("Counters"); len(found) > 0 {
		root = found[0]
	}
	if root == nil {
		if doc.root != nil {
			return errSecondRoot
		}
		root = newElement("Counters")
		doc.root = root
	}
	if doc.root != root || doc.nsError {
		return ErrUnsupportedDocument
	}
	if err := change(doc, root); err != nil {
		return err
	}
	out, ok, err := normalize(doc)
	if err != nil {
		return err
	}
	if !ok {
		return ErrUnsupportedDocument
	}
	saved, err := s.commitLocked(ctx, out)
	if err != nil {
		return err
	}
	if !saved {
		return fmt.Errorf("counters: engine reload failed after save")
	}
	return nil
}

func newElement(name string) *element { return &element{qname: name, local: name} }

// section returns the first element named name anywhere in the document
// (getElementsByTagName(name).item(0)), creating it under root if absent.
func section(doc *document, root *element, name string) *element {
	if found := doc.elementsByTagName(name); len(found) > 0 {
		return found[0]
	}
	e := newElement(name)
	root.children = append(root.children, e)
	return e
}

func boolText(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// setObjectTypeAttrs is CounterManager.setObjectTypeAttribute: attributes
// name, disp, family, icon, sub-object, then the own counters. Go replaces a
// same-name <Counter> child in place instead of appending a duplicate.
func setObjectTypeAttrs(e *element, spec ObjectTypeSpec, family string) {
	e.setAttr("name", spec.Name)
	e.setAttr("disp", spec.Display)
	e.setAttr("family", family)
	e.setAttr("icon", spec.Icon)
	e.setAttr("sub-object", boolText(spec.SubObject))
	for _, c := range spec.Counters {
		upsertChild(e, counterElement(c))
	}
}

func counterElement(c CounterSpec) *element {
	e := newElement("Counter")
	e.setAttr("name", c.Name)
	e.setAttr("disp", c.Display)
	e.setAttr("unit", c.Unit)
	e.setAttr("icon", c.Icon)
	e.setAttr("all", boolText(c.All))
	e.setAttr("total", boolText(c.Total))
	return e
}

func familyElement(f FamilySpec) *element {
	e := newElement("Family")
	e.setAttr("name", f.Name)
	e.setAttr("master", f.Master)
	for _, c := range f.Counters {
		upsertChild(e, counterElement(c))
	}
	return e
}

// upsertChild replaces the child with the same tag and name attribute, or
// appends.
func upsertChild(parent, child *element) {
	name, _ := child.attr("name")
	for i, c := range parent.children {
		if v, ok := c.attr("name"); ok && c.qname == child.qname && v == name {
			parent.children[i] = child
			return
		}
	}
	parent.children = append(parent.children, child)
}

// replaceOrAppend is safelyAddFamily/safelyAddObjectType: the first element
// with the tag and name anywhere is replaced in its parent, else the new
// element is appended to the section.
func replaceOrAppend(doc *document, root *element, sectionName string, e *element) {
	name, _ := e.attr("name")
	var replaced bool
	var visit func(parent *element)
	visit = func(parent *element) {
		for i, c := range parent.children {
			if replaced {
				return
			}
			if v, _ := c.attr("name"); c.qname == e.qname && v == name {
				parent.children[i] = e
				replaced = true
				return
			}
			visit(c)
		}
	}
	visit(doc.root)
	if !replaced {
		sec := section(doc, root, sectionName)
		sec.children = append(sec.children, e)
	}
}

// ObjectTypeSpec is CounterEngine.getObjectType for the HTTP inputs: the
// own counters first, then the family counters they do not shadow (the
// lookup order of ObjectType.getCounter).
func (s *Store) ObjectTypeSpec(name string) (ObjectTypeSpec, bool) {
	o := s.Engine().ObjectType(name)
	if o == nil {
		return ObjectTypeSpec{}, false
	}
	spec := ObjectTypeSpec{Name: o.Name, Display: o.Display, Icon: o.Icon, SubObject: o.SubObject}
	seen := map[string]bool{}
	for _, c := range o.Counters() {
		spec.Counters = append(spec.Counters, counterSpec(c))
		seen[c.Name] = true
	}
	if o.Family != nil {
		spec.Family = o.Family.Name
		for _, c := range o.Family.Counters() {
			if !seen[c.Name] {
				spec.Counters = append(spec.Counters, counterSpec(c))
			}
		}
	}
	return spec, true
}

func counterSpec(c *Counter) CounterSpec {
	return CounterSpec{Name: c.Name, Display: c.Display, Unit: c.Unit, Icon: c.Icon, All: c.All, Total: c.Total}
}

// HasFamily is CounterEngine.getFamily != null.
func (s *Store) HasFamily(name string) bool { return s.Engine().Family(name) != nil }

// AddFamilyAndObjectType is CounterManager.addFamilyAndObjectType (HTTP
// /register, new type). Java builds the document and discards it (§9-2);
// Go writes it. A same-name entry is replaced by normalization (last wins).
func (s *Store) AddFamilyAndObjectType(family FamilySpec, objectType ObjectTypeSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mutateLocked(context.Background(), func(doc *document, root *element) error {
		fams := section(doc, root, "Familys")
		fams.children = append(fams.children, familyElement(family))
		t := newElement("ObjectType")
		setObjectTypeAttrs(t, objectType, objectType.Family)
		types := section(doc, root, "Types")
		types.children = append(types.children, t)
		return nil
	})
}

// EditObjectTypeCounters is CounterManager.editObjectType(ObjectType) for
// /register on an existing type: attributes replaced, objectType.Counters
// added as own counters. Java discards the result (§9-2); Go writes it. The
// family must exist (Java: getFamily().getName() NullPointerException).
func (s *Store) EditObjectTypeCounters(objectType ObjectTypeSpec) error {
	ok, err := s.mutateType(context.Background(), objectType, true)
	if err == nil && !ok {
		err = fmt.Errorf("counters: edit object type %q: unknown family %q or unreadable custom file", objectType.Name, objectType.Family)
	}
	return err
}

// SafelyAddFamily is CounterManager.safelyAddFamily with the original
// defects fixed (intended differences, CONTRACT.md):
//   - Java replaces the same-name <Family> as a whole, so counters that other
//     object types of the family registered disappear (http-contract Q2). Go
//     merges: the current engine family (default or custom) keeps its
//     counters and master, family.Counters are added or replaced in place,
//     and family.Master is used only when the family had none;
//   - Java works on a document cached at the first call and throws a
//     NullPointerException when counters.site.xml does not exist; Go uses the
//     latest file and creates the document.
func (s *Store) SafelyAddFamily(family FamilySpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	merged := family
	if f := s.engine.Family(family.Name); f != nil {
		merged = FamilySpec{Name: f.Name, Master: f.Master}
		if merged.Master == "" {
			merged.Master = family.Master
		}
		for _, c := range f.Counters() {
			merged.Counters = append(merged.Counters, counterSpec(c))
		}
		for _, c := range family.Counters {
			merged.Counters = upsertCounterSpec(merged.Counters, c)
		}
	}
	return s.mutateLocked(context.Background(), func(doc *document, root *element) error {
		replaceOrAppend(doc, root, "Familys", familyElement(merged))
		return nil
	})
}

func upsertCounterSpec(list []CounterSpec, c CounterSpec) []CounterSpec {
	for i := range list {
		if list[i].Name == c.Name {
			list[i] = c
			return list
		}
	}
	return append(list, c)
}

// SafelyAddObjectType is CounterManager.safelyAddObjectType. Java replaces
// the same-name <ObjectType> element; Go updates its attributes in place and
// keeps its own <Counter> children (objectType.Counters are added or
// replaced), else appends a new element.
func (s *Store) SafelyAddObjectType(objectType ObjectTypeSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mutateLocked(context.Background(), func(doc *document, root *element) error {
		for _, el := range doc.elementsByTagName("ObjectType") {
			if v, _ := el.attr("name"); v == objectType.Name {
				setObjectTypeAttrs(el, objectType, objectType.Family)
				return nil
			}
		}
		t := newElement("ObjectType")
		setObjectTypeAttrs(t, objectType, objectType.Family)
		types := section(doc, root, "Types")
		types.children = append(types.children, t)
		return nil
	})
}

// AddFamilyCounter is objectType.getFamily().addCounter(counter) followed by
// safelyAddFamily(family): the engine family (default or custom) is copied
// with the counter added or replaced in place and written as a whole.
// Unknown family attributes are not carried, as in Java.
func (s *Store) AddFamilyCounter(family string, counter CounterSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.engine.Family(family)
	if f == nil {
		return fmt.Errorf("counters: unknown family %q", family)
	}
	spec := FamilySpec{Name: f.Name, Master: f.Master}
	for _, c := range f.Counters() {
		spec.Counters = append(spec.Counters, counterSpec(c))
	}
	spec.Counters = upsertCounterSpec(spec.Counters, counter)
	return s.mutateLocked(context.Background(), func(doc *document, root *element) error {
		replaceOrAppend(doc, root, "Familys", familyElement(spec))
		return nil
	})
}
