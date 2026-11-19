package ingest

import (
	"container/list"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Kubernetes pod sequence: scouter.server.kube.PodSeqManager and its callers
// in AgentManager.active/inactive. The kube fields of the Java ObjectPack
// (podName, hostName, podSeq, initialTime, allowProceed, pushSeq) are all
// transient: addKubeProp/copyKubePropFrom never change the encoded pack, the
// stored object or any Client response. They only decide whether the object is
// registered and what OBJECT_SET_KUBE_SEQ sends to the Agent.
//
// All state is owned by the engine lock (Java: a per-podName lock plus
// ConcurrentHashMaps; ingest is already serialized by e.mu). Bounds, which Java
// does not have, are listed in CONTRACT.md.
const (
	tagPodName    = "podName"    // ScouterConstants.POD_NAME
	tagHostName   = "hostName"   // ScouterConstants.HOST_NAME
	tagUseKubeSeq = "useKubeSeq" // ScouterConstants.USE_KUBE_SEQ
	tagKubeSeq    = "kubeSeq"    // ScouterConstants.KUBE_SEQ

	kubeSeqSearch   = 10000 // getNewPodSeq scans 0..9999
	kubeInitialHold = 15000 // ms an initial registration holds the host's objects
	// kubeInitialMax bounds the global initialHostObjMap (unbounded in Java).
	kubeInitialMax = 10000

	kubeManagerBytes = 64
	kubeEntryBytes   = 24
	kubeInitialBytes = 64
)

type podSeqManager struct {
	hostSeq map[string]int32 // hostSeqMap
	seqHost map[int32]string // seqHostMap
}

// kubeInitial is the part of the initial ObjectPack that copyKubePropFrom reads.
type kubeInitial struct {
	host        string
	initialTime int64
	podName     string
	podSeq      int32
}

// kubeObject is the kube part of the stored ObjectPack in AgentManager's
// objMap: AgentManager.inactive calls
// PodSeqManager.getInstance(podName).objectInactivated(hostName) once per
// alive period (objects.go). hostOK=false is a Java null hostName.
type kubeObject struct {
	podName, hostName string
	hostOK            bool
}

type kubeState struct {
	managers     map[string]*podSeqManager
	entries      int // keys over every hostSeq and seqHost map
	initial      map[string]*list.Element
	initialOrder list.List
	objects      map[int32]*kubeObject
}

func newKubeState() *kubeState {
	return &kubeState{managers: map[string]*podSeqManager{}, initial: map[string]*list.Element{}, objects: map[int32]*kubeObject{}}
}

// kubeResult is the transient kube outcome for one received ObjectPack.
type kubeResult struct {
	applied           bool // addKubeProp ran: Java podName != null
	podName, hostName string
	hostOK            bool
	podSeq            int32
	pushSeq           bool
	allowProceed      bool
}

// activeKubeLocked is the kube branch of AgentManager.active: it runs before
// the object is looked up or registered, and a held object (allowProceed=false)
// is not registered at all. A required push is queued, not awaited.
func (e *Engine) activeKubeLocked(p *protocol.ObjectPack, now int64) (kubeResult, error) {
	podName, ok := javaGetText(p.Tags, tagPodName)
	if !javaGetBoolean(p.Tags, tagUseKubeSeq) || !ok || podName == "" {
		return kubeResult{allowProceed: true}, nil
	}
	r, err := e.applyPodSeqLocked(podName, p.Tags, now)
	if err != nil {
		return r, err
	}
	if r.pushSeq {
		// PodSeqManager.pushSeqToAgent: MapPack{"seq": DecimalValue(podSeq)}.
		param := &protocol.MapPack{Entries: []protocol.Entry{{Key: "seq", Value: protocol.Value{Type: protocol.ValueDecimal, Int: int64(r.podSeq)}}}}
		e.kubeCalls.offer(agentCall{hash: p.ObjHash, cmd: cmdSetKubeSeq, param: param})
	}
	return r, nil
}

// applyPodSeqLocked is PodSeqManager.applyPodSeq for manager podName.
func (e *Engine) applyPodSeqLocked(podName string, tags protocol.Value, now int64) (kubeResult, error) {
	k := e.kube
	host, hostOK := javaGetText(tags, tagHostName)
	kubeSeq := javaGetInt(tags, tagKubeSeq)
	r := kubeResult{applied: true, podName: podName, hostName: host, hostOK: hostOK, podSeq: -1, allowProceed: true}
	if !javaGetBoolean(tags, tagUseKubeSeq) || !hostOK || host == "" {
		return r, nil
	}
	// Every later step is infallible: what the branch taken below adds (map
	// keys, bytes, a new manager) is reserved up front so a saturated state
	// rejects the pack before anything changes. A branch that adds nothing, such
	// as the heartbeat of a confirmed seq, is never rejected.
	m := k.managers[podName]
	entries, bytes := e.podSeqNeed(m, podName, host, kubeSeq)
	if entries > 0 {
		if m == nil {
			if len(k.managers) >= e.config.MaxObjects {
				e.kubeLimited.Add(1)
				return r, protocol.ErrLimit
			}
			bytes += len(podName) + kubeManagerBytes
		}
		if k.entries+entries > e.config.MaxCounters {
			e.kubeLimited.Add(1)
			return r, protocol.ErrLimit
		}
	}
	if err := e.coreDelta(bytes); err != nil {
		e.kubeLimited.Add(1)
		return r, err
	}
	switch {
	case m != nil:
	case entries > 0:
		m = &podSeqManager{hostSeq: map[string]int32{}, seqHost: map[int32]string{}}
		k.managers[podName] = m
		e.addCoreBytes(len(podName) + kubeManagerBytes)
	default:
		// getInstance would create an empty manager that the branch leaves
		// empty; an unregistered empty one behaves the same and costs nothing.
		m = &podSeqManager{}
	}
	defer e.pruneManager(podName, m)
	if kubeSeq <= -1 {
		e.applyNewPodSeq(m, podName, host, now, &r)
		return r, nil
	}
	r.podSeq = kubeSeq
	hostInMap, taken := m.seqHost[kubeSeq]
	switch {
	case !taken:
		// recover host's pod seq
		e.setSeqHost(m, kubeSeq, host)
		e.setHostSeq(m, host, kubeSeq)
	case hostInMap != host:
		// already taken: initialize again
		e.applyNewPodSeq(m, podName, host, now, &r)
	default:
		e.removeInitial(host) // the seq is confirmed
	}
	return r, nil
}

// podSeqNeed is what applyPodSeqLocked's branch for (host, kubeSeq) adds:
// new hostSeq/seqHost keys and the bytes of those keys and of a new initial
// record (an eviction from a full initial map is not credited). m may be nil.
func (e *Engine) podSeqNeed(m *podSeqManager, podName, host string, kubeSeq int32) (entries, bytes int) {
	var hostSeq map[string]int32
	var seqHost map[int32]string
	if m != nil {
		hostSeq, seqHost = m.hostSeq, m.seqHost
	}
	if kubeSeq > -1 {
		owner, taken := seqHost[kubeSeq]
		if !taken {
			// recover: setSeqHost(kubeSeq) + setHostSeq(host)
			entries, bytes = 1, len(host)+kubeEntryBytes
			if _, ok := hostSeq[host]; !ok {
				entries++
				bytes += len(host) + kubeEntryBytes
			}
			return entries, bytes
		}
		if owner == host {
			return 0, 0 // confirmed: removeInitial only
		}
	}
	if e.kube.initial[host] != nil {
		return 0, 0 // copyKubePropFrom, or removePodSeq after the hold
	}
	bytes = len(host) + len(podName) + kubeInitialBytes // putInitial
	if prev, ok := hostSeq[host]; ok {
		if old, ok := seqHost[prev]; ok {
			return 0, bytes + max(0, len(host)-len(old))
		}
		return 1, bytes + len(host) + kubeEntryBytes
	}
	// a free seq: setHostSeq + setSeqHost (nothing when 0..9999 are taken)
	return 2, bytes + 2*(len(host)+kubeEntryBytes)
}

// applyNewPodSeq is PodSeqManager.applyNewPodSeqToPack.
func (e *Engine) applyNewPodSeq(m *podSeqManager, podName, host string, now int64, r *kubeResult) {
	k := e.kube
	r.pushSeq = true
	el := k.initial[host]
	if el == nil {
		seq := e.newPodSeq(m, host)
		e.putInitial(kubeInitial{host: host, initialTime: now, podName: podName, podSeq: seq})
		r.podSeq = seq
		r.allowProceed = false // stop the registration
		return
	}
	initial := el.Value.(*kubeInitial)
	// copyKubePropFrom: the initial pack may belong to another podName.
	r.podName, r.hostName, r.hostOK, r.podSeq = initial.podName, initial.host, true, initial.podSeq
	if initial.initialTime > now-kubeInitialHold {
		r.allowProceed = false
		return
	}
	// After 15s the object proceeds without a push; the initial record stays.
	r.pushSeq = false
	e.removePodSeq(m, host)
}

// newPodSeq is PodSeqManager.getNewPodSeq.
func (e *Engine) newPodSeq(m *podSeqManager, host string) int32 {
	if prev, ok := m.hostSeq[host]; ok {
		e.setSeqHost(m, prev, host)
		return prev
	}
	for i := int32(0); i < kubeSeqSearch; i++ {
		if _, taken := m.seqHost[i]; !taken {
			e.setHostSeq(m, host, i)
			e.setSeqHost(m, i, host)
			return i
		}
	}
	return -1
}

func (e *Engine) setSeqHost(m *podSeqManager, seq int32, host string) {
	if old, ok := m.seqHost[seq]; ok {
		e.addCoreBytes(-(len(old) + kubeEntryBytes))
	} else {
		e.kube.entries++
	}
	m.seqHost[seq] = host
	e.addCoreBytes(len(host) + kubeEntryBytes)
}

func (e *Engine) setHostSeq(m *podSeqManager, host string, seq int32) {
	if _, ok := m.hostSeq[host]; !ok {
		e.kube.entries++
		e.addCoreBytes(len(host) + kubeEntryBytes)
	}
	m.hostSeq[host] = seq
}

// removePodSeq is PodSeqManager.removePodSeq: the host's current seq leaves
// both maps; an older seq still mapped to the host stays, as in Java.
func (e *Engine) removePodSeq(m *podSeqManager, host string) {
	seq, ok := m.hostSeq[host]
	if !ok {
		return
	}
	delete(m.hostSeq, host)
	e.kube.entries--
	e.addCoreBytes(-(len(host) + kubeEntryBytes))
	if owner, ok := m.seqHost[seq]; ok {
		delete(m.seqHost, seq)
		e.kube.entries--
		e.addCoreBytes(-(len(owner) + kubeEntryBytes))
	}
}

// pruneManager drops an empty manager. getInstance would recreate an empty
// one, which behaves exactly like an absent one.
func (e *Engine) pruneManager(podName string, m *podSeqManager) {
	if len(m.hostSeq) == 0 && len(m.seqHost) == 0 && e.kube.managers[podName] == m {
		delete(e.kube.managers, podName)
		e.addCoreBytes(-(len(podName) + kubeManagerBytes))
	}
}

// putInitial is initialHostObjMap.put for an absent host. Java never bounds
// the map; Go evicts the oldest record, which only means that host would be
// held for another 15s on its next init.
func (e *Engine) putInitial(v kubeInitial) {
	k := e.kube
	if len(k.initial) >= kubeInitialMax {
		e.removeInitial(k.initialOrder.Front().Value.(*kubeInitial).host)
		e.kubeInitialEvicted.Add(1)
	}
	k.initial[v.host] = k.initialOrder.PushBack(&v)
	e.addCoreBytes(len(v.host) + len(v.podName) + kubeInitialBytes)
}

func (e *Engine) removeInitial(host string) {
	k := e.kube
	el := k.initial[host]
	if el == nil {
		return
	}
	v := el.Value.(*kubeInitial)
	k.initialOrder.Remove(el)
	delete(k.initial, host)
	e.addCoreBytes(-(len(v.host) + len(v.podName) + kubeInitialBytes))
}

// objectInactivated is PodSeqManager.getInstance(podName).objectInactivated(hostName).
func (e *Engine) objectInactivated(podName, host string, hostOK bool) {
	if !hostOK {
		return
	}
	if m := e.kube.managers[podName]; m != nil {
		e.removePodSeq(m, host)
		e.pruneManager(podName, m)
	}
	e.removeInitial(host)
}

// rememberKubeObject records the kube fields of the pack that AgentManager
// stores in objMap (a new object or a saved update); a pack without the kube
// branch clears them (Java podName == null).
func (e *Engine) rememberKubeObject(hash int32, r kubeResult) {
	if !r.applied {
		delete(e.kube.objects, hash)
		return
	}
	e.kube.objects[hash] = &kubeObject{podName: r.podName, hostName: r.hostName, hostOK: r.hostOK}
}
