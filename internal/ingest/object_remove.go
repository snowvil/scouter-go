package ingest

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/transport"
)

// objectStoreRemover is the storage seam for Java ObjectWR.remove. It is a
// local interface so the ingest package compiles before and after the storage
// implementation lands; a store without it makes permanent removal an explicit
// unsupported error rather than a silent in-memory-only success.
type objectStoreRemover interface {
	RemoveObject(ctx context.Context, hash int32) error
}

// RemoveObjects is Java AgentManager.removeAgents: every hash leaves the live
// object map, and with permanent=true the stored record is removed too
// (ObjectWR.remove). Hashes that are not cached are still removed from the
// store, like the Java objMap.remove/ObjectWR.remove pair that ignores misses.
//
// The removal is queued to the object writer like ObjectWR.remove, behind the
// object writes queued before it (one FIFO), so a queued write never adds a
// removed record back, and the engine lock is not held for the disk. The
// command returns once it is queued; a refused enqueue (full or closed
// queue) is returned and nothing changes. A failed removal is counted by the
// object writer (WriteErrors), as ObjectWR logs it.
func (e *Engine) RemoveObjects(ctx context.Context, hashes []int32, permanent bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var remover objectStoreRemover
	if permanent {
		r, ok := any(e.store).(objectStoreRemover)
		if !ok {
			return fmt.Errorf("%w: permanent object removal needs storage support", transport.ErrUnsupported)
		}
		remover = r
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return err
	}
	defer e.mu.Unlock()
	if e.closed {
		return protocol.ErrInvalid
	}
	if remover != nil && len(hashes) > 0 {
		owned := append([]int32(nil), hashes...)
		err := e.w.enqueue(writeObject, len(owned), 4*len(owned), func(ctx context.Context) (int, error) {
			for i, hash := range owned {
				if err := remover.RemoveObject(ctx, hash); err != nil {
					return i, err
				}
			}
			return len(owned), nil
		})
		if err != nil {
			return fmt.Errorf("object removal not queued: %w", err)
		}
	}
	for _, hash := range hashes {
		e.dropObjectLocked(hash)
	}
	return nil
}

// RemoveInactiveObjects is Java AgentManager.clearInactive: cached objects
// whose heartbeat is older than their dead time leave the live map. Batch
// objects never expire by heartbeat, matching Objects().
func (e *Engine) RemoveInactiveObjects(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.mu.LockContext(ctx); err != nil {
		return err
	}
	defer e.mu.Unlock()
	if e.closed {
		return protocol.ErrInvalid
	}
	now := e.config.Clock().UnixMilli()
	for hash, p := range e.objects {
		if e.objectDead(p, now) {
			e.dropObjectLocked(hash)
		}
	}
	return nil
}

// objectEviction is the object a new object takes the place of at
// MaxObjects (pickEvictionLocked); silent marks an exempt object.
type objectEviction struct {
	hash   int32
	silent bool
}

// pickEvictionLocked chooses the object a new object replaces at
// MaxObjects; the caller drops it (evictObjectLocked) only once the new
// object is accepted, so a refused registration evicts nothing. Java objMap
// has no bound; Go drops the object that has been inactive the longest
// (earliest wakeup + dead time among the objects RemoveInactiveObjects would
// remove, objHash breaks ties) from the live map only, exactly like
// RemoveInactiveObjects (AgentManager.clearInactive): the stored record,
// alerts and kube pod seq are untouched and its next heartbeat registers it
// again.
//
// Without such an object, and with no dead object waiting for the 1s Tick
// (it becomes a candidate then), it picks the exempt object silent the
// longest (exemptEvictAt): one that does not die by the Java rule within an
// hour (batch, or a deadTime tag past exemptDeadTimeMs). R7: one burst of
// such ObjectPacks would otherwise refuse every new Agent for good. It
// reports false when there is no candidate.
//
// The scan is O(live objects); evictHint skips it while no object can be
// dead or evictable yet (a heartbeat flood of new objects at the bound with
// every object alive would otherwise scan on every rejected heartbeat).
func (e *Engine) pickEvictionLocked(now int64) (objectEviction, bool) {
	s := e.settings()
	if e.evictHintSettings == s && now <= e.evictHint {
		return objectEviction{}, false
	}
	var (
		victim, silent            int32
		victimDead, silentAt      int64
		found, foundSilent, ticks bool
		earliestLive              int64 = math.MaxInt64
	)
	for hash, p := range e.objects {
		deadline := int64(math.MaxInt64) // batch objects never expire by heartbeat
		if p.ObjType != "batch" {
			deadline = p.Wakeup + e.objectDeadTimeMs(p)
		}
		if now <= deadline { // !objectDead
			earliestLive = min(earliestLive, deadline)
			if at := e.exemptEvictAt(p); at < deadline {
				if now <= at {
					earliestLive = min(earliestLive, at)
				} else if !foundSilent || at < silentAt || (at == silentAt && hash < silent) {
					silent, silentAt, foundSilent = hash, at, true
				}
			}
			continue
		}
		if st := e.objState[hash]; st != nil && st.alive {
			// Dead but not yet inactivated by the 1s Tick: Java
			// clearInactive removes only alive == false objects, after
			// inactive() raised INACTIVE_OBJECT and released the kube seq.
			// It becomes a candidate on the next Tick.
			earliestLive = min(earliestLive, deadline)
			ticks = true
			continue
		}
		if !found || deadline < victimDead || (deadline == victimDead && hash < victim) {
			victim, victimDead, found = hash, deadline, true
		}
	}
	switch {
	case found:
		return objectEviction{hash: victim}, true
	case foundSilent && !ticks:
		return objectEviction{hash: silent, silent: true}, true
	}
	e.evictHint, e.evictHintSettings = earliestLive, s
	return objectEviction{}, false
}

// evictObjectLocked drops the object pickEvictionLocked chose and counts it.
func (e *Engine) evictObjectLocked(v objectEviction, now int64) {
	if !v.silent {
		e.dropObjectLocked(v.hash)
		e.objectsEvicted.Add(1)
		return
	}
	p := e.objects[v.hash]
	e.dropObjectLocked(v.hash)
	n := e.objectsEvictedSilent.Add(1)
	if p != nil && e.silentEvictLog.allow() {
		log.Printf("ingest: object %q (%s) evicted at max objects: no heartbeat for %d ms and no inactive object to evict (%d such evictions so far)", p.ObjName, p.ObjType, now-p.Wakeup, n)
	}
}

// exemptObjectSilenceMs is the silence after which a batch object (the
// batch Agent sends its ObjectPack every 10s, scouter.agent.batch
// Main.java:66-70) may be evicted at MaxObjects; exemptDeadTimeMs is the
// deadTime tag from which an object is exempt and the silence after which
// it may be. An object whose deadTime is shorter dies by the Java rule
// (INACTIVE_OBJECT, kube seq release) and is evicted as a dead object;
// Telegraf objects default to 35s (telegraf_object_deadtime_ms).
const (
	exemptObjectSilenceMs = 60000
	exemptDeadTimeMs      = 3600000
)

// exemptEvictAt is the time after which p may be evicted at MaxObjects as an
// exempt object; p is exempt when this is before its dead time (always for
// batch). The silence is exemptObjectSilenceMs for batch and
// exemptDeadTimeMs otherwise, at least the dead time of its type without a
// deadTime tag (object_deadtime_ms, object_zipkin_deadtime_ms).
func (e *Engine) exemptEvictAt(p *protocol.ObjectPack) int64 {
	s := e.settings()
	std := s.ObjectDeadTimeMs
	if p.ObjType == "zipkin" || strings.HasPrefix(p.ObjType, "z$") {
		std = s.ZipkinDeadTimeMs
	}
	silence := int64(exemptDeadTimeMs)
	if p.ObjType == "batch" {
		silence = exemptObjectSilenceMs
	}
	return p.Wakeup + max(std, silence)
}

// noteObjectDeadlineLocked keeps evictHint a lower bound after an object's
// wakeup or deadTime tag changed.
func (e *Engine) noteObjectDeadlineLocked(p *protocol.ObjectPack) {
	if e.evictHintSettings == nil {
		return
	}
	at := e.exemptEvictAt(p)
	if p.ObjType != "batch" {
		at = min(at, p.Wakeup+e.objectDeadTimeMs(p))
	}
	e.evictHint = min(e.evictHint, at)
}

// objectLimitedLocked counts a new object rejected at MaxObjects and logs at
// most one line per 10s (the object name is the Agent's own identity).
func (e *Engine) objectLimitedLocked(p *protocol.ObjectPack, now int64) {
	n := e.objectsLimited.Add(1)
	if now >= e.nextLimitLog {
		e.nextLimitLog = now + objectLimitLogInterval
		log.Printf("ingest: object %q not registered: %d live objects reached max objects and none is inactive (%d rejected so far)", p.ObjName, len(e.objects), n)
	}
}

const objectLimitLogInterval = 10000

func (e *Engine) dropObjectLocked(hash int32) {
	p, ok := e.objects[hash]
	if !ok {
		return
	}
	// ObjectMap.remove drops the name entry of the removed object even when it
	// was re-pointed; it does not call AgentManager.inactive, so the pod seq of
	// a removed kube object is not released (Java behavior).
	delete(e.objectNames, p.ObjName)
	delete(e.objState, hash)
	e.objOrder.remove(hash)
	delete(e.kube.objects, hash)
	delete(e.cpuFirst, p.ObjName)
	delete(e.objects, hash)
	e.objectNameCache.Delete(hash)
	e.addCoreBytes(-e.objectSizes[hash])
	delete(e.objectSizes, hash)
}

// DeleteDatabasePaths is the SERVER_DB_DELETE seam: the service passes
// root-relative cleaned names, storage takes the SERVER_DB_LIST form with the
// leading "/". The engine owns the Store, so the mutation goes through here
// and the live object map is dropped for a deleted current date directory.
func (e *Engine) DeleteDatabasePaths(ctx context.Context, paths []string) error {
	names := make([]string, len(paths))
	for i, p := range paths {
		names[i] = "/" + p
	}
	// Records queued before the delete are written before it, as they were
	// when ingest wrote synchronously (a later write recreates its file, as
	// before; CONTRACT.md §13).
	if err := e.Flush(ctx); err != nil {
		return err
	}
	deleted, err := e.store.DeleteDatabasePaths(ctx, names)
	if deleted == 0 && err != nil {
		return err
	}
	// Whatever storage reported after a removal, the live map must not keep
	// objects whose today's record directory is gone: the heartbeat fast path
	// would otherwise skip PutObject until the date changes (review M2/R2).
	today := storage.JavaDate(e.config.Clock().UnixMilli(), e.config.Location)
	for _, p := range paths {
		if p == today || p == today+"/object" || strings.HasPrefix(p, today+"/object/") {
			if lockErr := e.mu.LockContext(ctx); lockErr != nil {
				return errors.Join(err, lockErr)
			}
			for hash := range e.objects {
				e.dropObjectLocked(hash)
			}
			e.mu.Unlock()
			break
		}
	}
	return err
}
