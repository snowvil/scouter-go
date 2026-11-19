package counters

import (
	"sync/atomic"

	"github.com/snowvil/scouter-go/internal/telegraf"
)

// Registrar implements telegraf.TypeRegistrar (the CounterManager side
// effects of the Telegraf input and HTTP /register) on a Store. It converts
// the telegraf definitions to the Store specs; the counters.xml semantics and
// the intended differences from Java are documented on the Store methods and
// in CONTRACT.md.
type Registrar struct {
	store  *Store
	notify func()
	// dirty: a notification is requested; running: the single notifier
	// goroutine exists.
	dirty, running atomic.Bool
}

var _ telegraf.TypeRegistrar = (*Registrar)(nil)

// NewRegistrar returns the registrar for store. notify is
// RegisterHandler.notifyAllClients (REFETCH_COUNTER_XML to every login
// session); nil makes NotifyClients a no-op.
func NewRegistrar(store *Store, notify func()) *Registrar {
	return &Registrar{store: store, notify: notify}
}

func counterSpecOf(c telegraf.CounterDef) CounterSpec {
	return CounterSpec{Name: c.Name, Display: c.DisplayName, Unit: c.Unit, Icon: c.Icon, All: c.All, Total: c.Total}
}

func counterDefOf(c CounterSpec) telegraf.CounterDef {
	return telegraf.CounterDef{Name: c.Name, DisplayName: c.Display, Unit: c.Unit, Icon: c.Icon, All: c.All, Total: c.Total}
}

func familySpecOf(f telegraf.FamilyDef) FamilySpec {
	spec := FamilySpec{Name: f.Name, Master: f.Master}
	for _, c := range f.Counters {
		spec.Counters = append(spec.Counters, counterSpecOf(c))
	}
	return spec
}

func objectTypeSpecOf(o telegraf.ObjectTypeDef) ObjectTypeSpec {
	spec := ObjectTypeSpec{Name: o.Name, Display: o.DisplayName, Family: o.Family, Icon: o.Icon, SubObject: o.SubObject}
	for _, c := range o.Counters {
		spec.Counters = append(spec.Counters, counterSpecOf(c))
	}
	return spec
}

// ObjectType is CounterEngine.getObjectType: the type's own counters first,
// then the family counters they do not shadow (ObjectType.getCounter order).
func (r *Registrar) ObjectType(name string) (telegraf.ObjectTypeDef, bool) {
	spec, ok := r.store.ObjectTypeSpec(name)
	if !ok {
		return telegraf.ObjectTypeDef{}, false
	}
	def := telegraf.ObjectTypeDef{Name: spec.Name, DisplayName: spec.Display, Icon: spec.Icon, Family: spec.Family, SubObject: spec.SubObject}
	for _, c := range spec.Counters {
		def.Counters = append(def.Counters, counterDefOf(c))
	}
	return def, true
}

// HasFamily is CounterEngine.getFamily != null.
func (r *Registrar) HasFamily(name string) bool { return r.store.HasFamily(name) }

// AddFamilyAndObjectType is CounterManager.addFamilyAndObjectType (written;
// Java discards the document).
func (r *Registrar) AddFamilyAndObjectType(family telegraf.FamilyDef, objectType telegraf.ObjectTypeDef) error {
	return r.store.AddFamilyAndObjectType(familySpecOf(family), objectTypeSpecOf(objectType))
}

// EditObjectType is CounterManager.editObjectType(ObjectType): objectType
// carries only the counters to add (written; Java discards the document).
func (r *Registrar) EditObjectType(objectType telegraf.ObjectTypeDef) error {
	return r.store.EditObjectTypeCounters(objectTypeSpecOf(objectType))
}

// SafelyAddFamily merges into the existing family instead of replacing it.
func (r *Registrar) SafelyAddFamily(family telegraf.FamilyDef) error {
	return r.store.SafelyAddFamily(familySpecOf(family))
}

// SafelyAddObjectType is CounterManager.safelyAddObjectType.
func (r *Registrar) SafelyAddObjectType(objectType telegraf.ObjectTypeDef) error {
	return r.store.SafelyAddObjectType(objectTypeSpecOf(objectType))
}

// AddFamilyCounter adds or replaces one counter of an existing family.
func (r *Registrar) AddFamilyCounter(family string, counter telegraf.CounterDef) error {
	return r.store.AddFamilyCounter(family, counterSpecOf(counter))
}

// NotifyClients never blocks: like Java's AsyncRun it hands the work to a
// background goroutine. Requests made while a notification runs are
// coalesced into one more call (a Client refetches the whole XML), and at
// most one goroutine exists; it exits when no request is pending.
func (r *Registrar) NotifyClients() {
	if r.notify == nil {
		return
	}
	r.dirty.Store(true)
	if r.running.CompareAndSwap(false, true) {
		go r.drain()
	}
}

func (r *Registrar) drain() {
	for {
		for r.dirty.Swap(false) {
			r.notify()
		}
		r.running.Store(false)
		// A request that arrived after the last Swap but before running was
		// cleared saw running=true and returned; pick it up here.
		if !r.dirty.Load() || !r.running.CompareAndSwap(false, true) {
			return
		}
	}
}
