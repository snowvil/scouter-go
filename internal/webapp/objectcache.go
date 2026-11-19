package webapp

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// AgentModelThread replacement (framework/client/model/AgentModelThread.java):
// the Java thread refreshes OBJECT_LIST_REAL_TIME and GET_XML_COUNTER every
// 10s. Go refreshes lazily with the same 10s TTL, and a cache miss on an
// objHash forces one refresh (at most once a second) instead of the Java NPE
// or "UNKNOWN" for objects younger than the last refresh (W3).

const (
	modelTTL         = 10 * time.Second
	modelMissRefresh = time.Second
	maxCounterXML    = 8 << 20
	maxXMLElements   = 100000
)

type modelCache struct {
	mu       sync.Mutex
	objects  []*protocol.ObjectPack
	byHash   map[int32]*protocol.ObjectPack
	loadedAt time.Time
	engine   *counterEngine
	engineAt time.Time
}

func (s *Server) refreshObjectsLocked(ctx context.Context) error {
	var list []*protocol.ObjectPack
	err := s.callNoBody(ctx, "OBJECT_LIST_REAL_TIME", func(it Item) error {
		o, ok := it.Pack.(*protocol.ObjectPack)
		if !ok {
			return fmt.Errorf("OBJECT_LIST_REAL_TIME: item is %T, want ObjectPack", it.Pack)
		}
		list = append(list, o)
		return nil
	})
	if err != nil {
		return err
	}
	s.model.objects = list
	s.model.byHash = make(map[int32]*protocol.ObjectPack, len(list))
	for _, o := range list {
		s.model.byHash[o.ObjHash] = o
	}
	s.model.loadedAt = s.now()
	return nil
}

// objects returns the cached object list (10s TTL).
func (s *Server) objects(ctx context.Context) ([]*protocol.ObjectPack, error) {
	s.model.mu.Lock()
	defer s.model.mu.Unlock()
	if s.model.byHash == nil || s.now().Sub(s.model.loadedAt) >= modelTTL {
		if err := s.refreshObjectsLocked(ctx); err != nil {
			return nil, err
		}
	}
	return s.model.objects, nil
}

// object returns the cached object for hash, or nil.
func (s *Server) object(ctx context.Context, hash int32) (*protocol.ObjectPack, error) {
	if _, err := s.objects(ctx); err != nil {
		return nil, err
	}
	s.model.mu.Lock()
	defer s.model.mu.Unlock()
	if o, ok := s.model.byHash[hash]; ok {
		return o, nil
	}
	if s.now().Sub(s.model.loadedAt) >= modelMissRefresh {
		if err := s.refreshObjectsLocked(ctx); err != nil {
			return nil, err
		}
	}
	return s.model.byHash[hash], nil
}

// objName is AgentObject.getObjName with the "UNKNOWN" fallback used by
// SAlert and SActiveServiceStepCount.
func (s *Server) objName(ctx context.Context, hash int32) (string, error) {
	o, err := s.object(ctx, hash)
	if err != nil {
		return "", err
	}
	if o == nil {
		return "UNKNOWN", nil
	}
	return o.ObjName, nil
}

// ---- CounterEngine (scouter.lang.counters.CounterEngine) ----

type ceCounter struct {
	name, display, unit, icon *string
	all, total                bool
}

type ceFamily struct {
	name, master *string
	counters     []*ceCounter
	byName       map[string]*ceCounter
}

type ceObjType struct {
	name, display, icon *string
	family              *ceFamily
	subObject           bool
	counters            map[string]*ceCounter
}

// counterEngine keeps StringKeyLinkedMap insertion order: a re-put key keeps
// its position and takes the new value.
type counterEngine struct {
	familyOrder []string
	families    map[string]*ceFamily
	typeOrder   []string
	types       map[string]*ceObjType
}

func newCounterEngine() *counterEngine {
	return &counterEngine{families: map[string]*ceFamily{}, types: map[string]*ceObjType{}}
}

func (t *ceObjType) counter(name string) *ceCounter {
	if c, ok := t.counters[name]; ok {
		return c
	}
	if t.family != nil {
		return t.family.byName[name]
	}
	return nil
}

// familyName is getFamilyNameFromObjType: "UNKNOWN" for an unknown type (and,
// in Go, for a type whose family is undefined, where Java throws NPE).
func (e *counterEngine) familyName(objType string) string {
	t := e.types[objType]
	if t == nil || t.family == nil || t.family.name == nil {
		return "UNKNOWN"
	}
	return *t.family.name
}

// displayName is getCounterDisplayName ("" when unknown; Java null when the
// counter has no disp attribute).
func (e *counterEngine) displayName(objType, counter string) any {
	t := e.types[objType]
	if t == nil {
		return ""
	}
	c := t.counter(counter)
	if c == nil {
		return ""
	}
	return strPtr(c.display)
}

// unit is getCounterUnit: Java null for an unknown object type.
func (e *counterEngine) unit(objType, counter string) any {
	t := e.types[objType]
	if t == nil {
		return nil
	}
	c := t.counter(counter)
	if c == nil {
		return ""
	}
	return strPtr(c.unit)
}

func strPtr(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}

// xmlNode is a minimal DOM for getElementsByTagName traversal.
type xmlNode struct {
	name     string
	attrs    []xml.Attr
	children []*xmlNode
}

func parseXMLTree(content []byte) (*xmlNode, error) {
	dec := xml.NewDecoder(bytes.NewReader(content))
	dec.Strict = true
	root := &xmlNode{}
	stack := []*xmlNode{root}
	count := 0
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			count++
			if count > maxXMLElements {
				return nil, errors.New("counter XML has too many elements")
			}
			n := &xmlNode{name: t.Name.Local, attrs: t.Attr}
			parent := stack[len(stack)-1]
			parent.children = append(parent.children, n)
			stack = append(stack, n)
		case xml.EndElement:
			if len(stack) > 1 {
				stack = stack[:len(stack)-1]
			}
		}
	}
	return root, nil
}

func (n *xmlNode) elements(name string, out []*xmlNode) []*xmlNode {
	for _, c := range n.children {
		if c.name == name {
			out = append(out, c)
		}
		out = c.elements(name, out)
	}
	return out
}

func javaBool(v string) bool { return strings.EqualFold(v, "true") }

func parseCounter(n *xmlNode) *ceCounter {
	c := &ceCounter{}
	for _, a := range n.attrs {
		v := a.Value
		switch a.Name.Local {
		case "name":
			c.name = &v
		case "disp":
			c.display = &v
		case "unit":
			c.unit = &v
		case "icon":
			c.icon = &v
		case "all":
			c.all = javaBool(v)
		case "total":
			c.total = javaBool(v)
		}
	}
	return c
}

// parse is CounterEngine.parse applied to one XML document.
func (e *counterEngine) parse(content []byte) error {
	if len(content) > maxCounterXML {
		return errors.New("counter XML too large")
	}
	root, err := parseXMLTree(content)
	if err != nil {
		return err
	}
	for _, fn := range root.elements("Family", nil) {
		f := &ceFamily{byName: map[string]*ceCounter{}}
		for _, a := range fn.attrs {
			v := a.Value
			switch a.Name.Local {
			case "name":
				f.name = &v
			case "master":
				f.master = &v
			}
		}
		if f.name != nil {
			if _, ok := e.families[*f.name]; !ok {
				e.familyOrder = append(e.familyOrder, *f.name)
			}
			e.families[*f.name] = f
		}
		for _, cn := range fn.elements("Counter", nil) {
			c := parseCounter(cn)
			if c.name == nil {
				continue
			}
			if old, ok := f.byName[*c.name]; ok {
				*old = *c
				continue
			}
			f.byName[*c.name] = c
			f.counters = append(f.counters, c)
		}
	}
	for _, tn := range root.elements("ObjectType", nil) {
		t := &ceObjType{counters: map[string]*ceCounter{}}
		for _, a := range tn.attrs {
			v := a.Value
			switch a.Name.Local {
			case "name":
				t.name = &v
			case "disp":
				t.display = &v
			case "family":
				t.family = e.families[v]
			case "icon":
				t.icon = &v
			case "sub-object":
				t.subObject = javaBool(v)
			}
		}
		if t.name != nil {
			if exist, ok := e.types[*t.name]; ok {
				t = exist
			} else {
				e.types[*t.name] = t
				e.typeOrder = append(e.typeOrder, *t.name)
			}
		}
		for _, cn := range tn.elements("Counter", nil) {
			if c := parseCounter(cn); c.name != nil {
				t.counters[*c.name] = c
			}
		}
	}
	return nil
}

// counterEngine returns the cached engine built from GET_XML_COUNTER
// default + custom (LoginMgr.refreshCounterEngine), refreshed every 10s.
func (s *Server) counterEngine(ctx context.Context) (*counterEngine, error) {
	s.model.mu.Lock()
	defer s.model.mu.Unlock()
	if s.model.engine != nil && s.now().Sub(s.model.engineAt) < modelTTL {
		return s.model.engine, nil
	}
	pack, err := s.single(ctx, "GET_XML_COUNTER", nil)
	if err != nil {
		if s.model.engine != nil {
			s.logf("webapp GET_XML_COUNTER refresh: %v", err)
			return s.model.engine, nil
		}
		return nil, err
	}
	engine := newCounterEngine()
	if pack != nil {
		for _, key := range []string{"default", "custom"} {
			v, ok := mapGet(pack, key)
			if !ok || v.Type != protocol.ValueBlob {
				continue
			}
			if err := engine.parse(v.Bytes); err != nil {
				return nil, fmt.Errorf("GET_XML_COUNTER %s: %w", key, err)
			}
		}
	}
	s.model.engine, s.model.engineAt = engine, s.now()
	return engine, nil
}

// objTypeFamily is CounterEngine.getFamilyNameFromObjType ("UNKNOWN" if absent).
func (s *Server) objTypeFamily(ctx context.Context, objType string) string {
	e, err := s.counterEngine(ctx)
	if err != nil {
		s.logf("webapp counter engine: %v", err)
		return "UNKNOWN"
	}
	return e.familyName(objType)
}
