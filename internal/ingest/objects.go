package ingest

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

// AgentManager object lifecycle (core/AgentManager.scala): the alive flag
// of every object in objMap, the 1s inactive check, the INACTIVE_OBJECT /
// ACTIVATED_OBJECT / MAYBE_OBJ_DUP alerts and the "updated" counter.
//
// objState exists only for objects AgentManager.active has registered in
// this process. Objects read from today's object file at startup are in
// e.objects (OBJECT_LIST) but not in Java's objMap (AgentManager.read is
// never called): they raise no alert and their first heartbeat is a new
// registration, as in Java.

// ObjectTypeInfo is the CounterEngine ObjectType of an object type.
type ObjectTypeInfo struct {
	Family    string // getFamily().getName(); "" when the family is unknown
	SubObject bool   // isSubObject()
}

type objectState struct {
	alive   bool  // ObjectPack.alive
	updated int32 // ObjectPack.updated (transient)
	// unsaved is set by the object writer when the last ObjectWR.add of this
	// registration failed on I/O or was not queued: the next unchanged
	// heartbeat writes the record again instead of taking the no-write fast
	// path (the original never retries; CONTRACT.md §13).
	unsaved atomic.Bool
}

const (
	alertInactive  = "INACTIVE_OBJECT"
	alertActivated = "ACTIVATED_OBJECT"
	alertDup       = "MAYBE_OBJ_DUP"
	tagDetected    = "detected" // ScouterConstants.TAG_OBJ_DETECTED_TYPE
	tagDeadTime    = "deadTime" // ObjectPack.TAG_KEY_DEAD_TIME
)

func (e *Engine) objectType(objType string) (ObjectTypeInfo, bool) {
	if e.config.ObjectType == nil {
		return ObjectTypeInfo{}, false
	}
	return e.config.ObjectType(objType)
}

// primaryObject is CounterEngine.isPrimaryObject: a known, non-sub type.
func (e *Engine) primaryObject(objType string) bool {
	t, ok := e.objectType(objType)
	return ok && !t.SubObject
}

// objectDeadTimeMs is the AgentManager daemon rule: the deadTime tag
// (MapValue.getInt) when it is not 0, else object_zipkin_deadtime_ms for
// "zipkin" and "z$..." types and object_deadtime_ms for the others.
func (e *Engine) objectDeadTimeMs(p *protocol.ObjectPack) int64 {
	if d := javaGetInt(p.Tags, tagDeadTime); d != 0 {
		return int64(d)
	}
	s := e.settings()
	if p.ObjType == "zipkin" || strings.HasPrefix(p.ObjType, "z$") {
		return s.ZipkinDeadTimeMs
	}
	return s.ObjectDeadTimeMs
}

// objectDead is "now > wakeup + adjustDeadTime" (batch objects never die).
func (e *Engine) objectDead(p *protocol.ObjectPack, now int64) bool {
	return p.ObjType != "batch" && now > p.Wakeup+e.objectDeadTimeMs(p)
}

// acceptObjectLocked is AgentManager.active.
func (e *Engine) acceptObjectLocked(ctx context.Context, v *protocol.ObjectPack, now int64, date string) error {
	if v.ObjHash == 0 {
		v.ObjHash = storage.CounterHash(v.ObjName)
	}
	kube, err := e.activeKubeLocked(v, now)
	if err != nil {
		return err
	}
	if !kube.allowProceed {
		// PodSeqManager stopped the registration; the Agent gets its seq.
		e.kubeHeld.Add(1)
		return nil
	}
	old := e.objects[v.ObjHash]
	var evict *objectEviction // made only once the new object is accepted
	if old == nil && len(e.objects) >= e.config.MaxObjects {
		victim, ok := e.pickEvictionLocked(now)
		if !ok {
			e.objectLimitedLocked(v, now)
			return protocol.ErrLimit
		}
		evict = &victim
	}
	st := e.objState[v.ObjHash]
	registered := old != nil && st != nil // Java objMap.getObject(objHash) != null
	// alertReactiveObject reads the stored pack before it is updated (old is
	// never mutated: both branches replace the map entry), but is raised only
	// once the heartbeat is accepted. A rejected heartbeat (object byte
	// budget, storage) keeps the object inactive and raises nothing, so a
	// saturated engine does not repeat ACTIVATED_OBJECT on every heartbeat.
	reactivated := registered && !st.alive && e.primaryObject(old.ObjType)
	// An object not in objMap (loaded from today's file at startup) takes the
	// Java "objPack == null" branch: ObjectWR.add even when nothing changed.
	if registered && old.Address == v.Address && old.ObjType == v.ObjType && old.Version == v.Version && storage.JavaDate(old.Wakeup, e.config.Location) == date {
		next := *old
		next.Tags = v.Tags
		next.Wakeup = now
		next.Alive = true
		b, err := protocol.EncodePack(&next, protocol.Limits{})
		if err != nil {
			return err
		}
		delta := len(b) - e.objectSizes[v.ObjHash]
		if e.coreDelta(delta) != nil {
			return protocol.ErrLimit
		}
		e.objects[v.ObjHash] = &next
		e.objectSizes[v.ObjHash] = len(b)
		e.addCoreBytes(delta)
		st.alive = true // objPack.wakeup(); kube fields of the stored pack unchanged
		e.noteObjectDeadlineLocked(&next)
		if st.unsaved.Swap(false) {
			// The record of this registration was not written: write the
			// current one (a copy, as below) without counting a change.
			if stored, err := protocol.DecodePack(b, protocol.Limits{}); err == nil {
				e.queueObjectWrite(date, stored.(*protocol.ObjectPack), st)
			} else {
				st.unsaved.Store(true)
			}
		}
		if reactivated {
			e.emitReactivated(ctx, old, now)
		}
		return nil
	}
	updated := int32(0)
	var dup *protocol.AlertPack
	if registered {
		updated = st.updated
		if storage.JavaDate(old.Wakeup, e.config.Location) != date {
			updated = 0
		}
		updated++
		if updated%20 == 0 {
			// alertTooManyChange(objPack): the stored pack after wakeup() and
			// "objPack.tags = p.tags", still with its old address/type/version.
			// Raised after ACTIVATED_OBJECT (Java order) and, like it, only
			// once the heartbeat is accepted.
			shown := *old
			shown.Tags, shown.Wakeup = v.Tags, now
			dup = &protocol.AlertPack{Level: 0, Title: alertDup, Message: "Maybe duplicated objNames!! Please check " + javaObjectString(&shown, true, e.config.Location), ObjType: "scouter", Tags: protocol.Value{Type: protocol.ValueMap}}
		}
	}
	v.Alive = true
	v.Wakeup = now
	objectBytes, err := protocol.EncodePack(v, protocol.Limits{})
	if err != nil {
		return err
	}
	delta := len(objectBytes) - e.objectSizes[v.ObjHash]
	freed := 0
	if evict != nil {
		freed = e.objectSizes[evict.hash]
	}
	if e.coreDelta(delta-freed) != nil {
		return protocol.ErrLimit
	}
	// The writer gets its own copy: the engine keeps v as the live object and
	// replaces or evicts it later (a decoded pack shares nothing with v).
	stored, err := protocol.DecodePack(objectBytes, protocol.Limits{})
	if err != nil {
		return err
	}
	// AgentManager.active: objMap.put, TextCore.add, ObjectWR.add. The text
	// is checked and queued first so that an object whose name cannot be
	// stored is still refused before any state changes; ObjectWR.add is
	// queued after the live map is updated, which does not depend on the
	// write (the date is fixed now, Java's ObjectWR uses its own clock).
	if err = e.putTextLocked(date, &protocol.TextPack{XType: "object", Hash: v.ObjHash, Text: v.ObjName}, nil); err != nil {
		return err
	}
	if evict != nil {
		e.evictObjectLocked(*evict, now)
	}
	if old != nil && old.ObjType != v.ObjType {
		// ObjectMap.clearInvalidObjType removes the old entry and its name.
		delete(e.objectNames, old.ObjName)
		e.objOrder.remove(v.ObjHash)
	}
	e.objOrder.put(v.ObjHash)
	e.objects[v.ObjHash] = v
	e.objectNames[v.ObjName] = v.ObjHash
	e.objectNameCache.Store(v.ObjHash, v.ObjName)
	e.objectSizes[v.ObjHash] = len(objectBytes)
	e.addCoreBytes(delta)
	st = &objectState{alive: true, updated: updated}
	e.objState[v.ObjHash] = st
	e.rememberKubeObject(v.ObjHash, kube)
	e.noteObjectDeadlineLocked(v)
	e.queueObjectWrite(date, stored.(*protocol.ObjectPack), st)
	if reactivated {
		e.emitReactivated(ctx, old, now)
	}
	if dup != nil {
		e.cacheAlertLocked(ctx, dup, now)
	}
	return nil
}

// queueObjectWrite queues ObjectWR.add of an owned copy p. An I/O failure or
// a refused enqueue marks st unsaved (a record the storage format refuses is
// not retried: it would fail again on every heartbeat).
func (e *Engine) queueObjectWrite(date string, p *protocol.ObjectPack, st *objectState) {
	err := e.w.enqueue(writeObject, 1, packHeapBytes(p), one(func(ctx context.Context) error {
		err := e.store.PutObjectContext(ctx, date, p)
		if err != nil && !invalidRecord(err) {
			st.unsaved.Store(true)
		}
		return err
	}))
	if err != nil {
		st.unsaved.Store(true)
	}
}

// emitReactivated is alertReactiveObject on the stored pack before the update.
func (e *Engine) emitReactivated(ctx context.Context, old *protocol.ObjectPack, now int64) {
	e.emitObjectAlert(ctx, alertActivated, old, old.ObjName+" is running now. "+javaObjectString(old, false, e.config.Location), now)
}

// inactivateObjectsLocked is the AgentManager 1s daemon: an alive object
// past its dead time becomes inactive once; a known non-sub type raises
// INACTIVE_OBJECT and a kube object releases its pod seq.
func (e *Engine) inactivateObjectsLocked(ctx context.Context, now int64) {
	// objMap.objects() order decides the order of the alerts of one pass.
	for _, hash := range e.objOrder.keys() {
		st := e.objState[hash]
		if st == nil || !st.alive {
			continue
		}
		p := e.objects[hash]
		if p == nil {
			continue
		}
		if !e.objectDead(p, now) {
			continue
		}
		st.alive = false
		if e.primaryObject(p.ObjType) {
			e.emitObjectAlert(ctx, alertInactive, p, p.ObjName+" is not running. "+javaObjectString(p, false, e.config.Location), now)
		}
		if o := e.kube.objects[hash]; o != nil {
			e.objectInactivated(o.podName, o.hostName, o.hostOK)
		}
	}
}

// emitObjectAlert is alertInactiveObject/alertReactiveObject + AlertCore.add:
// level object_inactive_alert_level as a byte, objType "scouter", the full
// AlertCore path (store, summary, realtime cache, tag count). A failure is
// counted; it never fails the object heartbeat.
func (e *Engine) emitObjectAlert(ctx context.Context, title string, p *protocol.ObjectPack, message string, now int64) {
	a := &protocol.AlertPack{Level: byte(e.settings().InactiveAlertLevel), ObjHash: p.ObjHash, Title: title, Message: message, ObjType: "scouter", Tags: protocol.Value{Type: protocol.ValueMap}}
	if err := e.acceptAlertLocked(ctx, a, now, e.objectAlertTagFailed); err != nil {
		e.objectAlertErrors.Add(1)
	}
}

// objectAlertTagFailed counts an object alert whose tag count write, submitted
// after the engine lock, failed (closed store, canceled ctx).
func (e *Engine) objectAlertTagFailed(error) { e.objectAlertErrors.Add(1) }

// addObjectTypeIfNotExist is CounterManager.addObjectTypeIfNotExist, the
// first step of AgentManager.active. It runs before the engine lock because
// it may write counters.site.xml.
func (e *Engine) addObjectTypeIfNotExist(p protocol.Pack) {
	o, ok := p.(*protocol.ObjectPack)
	if !ok || (e.config.AddObjectTypeIfNotExist == nil && e.config.ObjectTypeLog == nil) {
		return
	}
	_, known := e.objectType(o.ObjType)
	var detected string
	detectedOK := false
	if !known {
		detected, detectedOK = javaGetText(o.Tags, tagDetected)
	}
	if e.config.ObjectTypeLog != nil {
		e.config.ObjectTypeLog(o.ObjType, known, detected, detectedOK)
	}
	if known || !detectedOK || e.config.AddObjectTypeIfNotExist == nil {
		return
	}
	e.config.AddObjectTypeIfNotExist(o.ObjType, detected)
}
