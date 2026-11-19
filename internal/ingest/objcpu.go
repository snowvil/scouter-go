package ingest

import (
	"github.com/snowvil/scouter-go/internal/protocol"
)

// ObjectCpuChecker.checkCpu (core/app/ObjectCpuChecker.scala): a javaee-family
// Agent with the autodump tags gets TRIGGER_THREAD_DUMPS_FROM_CONDITIONS once
// its ProcCpu stays above the threshold for longer than the duration.
const (
	tagAutodumpCPUEnabled   = "ADC"    // ScouterConstants.TAG_AUTODUMP_CPU_ENABLED
	tagAutodumpCPUThreshold = "ADC_TH" // TAG_AUTODUMP_CPU_THRESHOLD
	tagAutodumpCPUDuration  = "ADC_D"  // TAG_AUTODUMP_CPU_DURATION
	counterProcCPU          = "ProcCpu"
	familyJavaEE            = "javaee"
)

// checkCPULocked runs after a REALTIME counter pack has been stored and cached
// (PerfCountCore). Java keeps an ObjectCpuStatus per objName forever; a status
// with firstOccurenceTime <= 0 (new = -1 or cleared = 0) behaves the same in
// every branch, so Go keeps only the running streaks (first > 0) and deletes a
// name on clear() and when its object leaves the live map. overCount is never
// read in Java and is not kept.
func (e *Engine) checkCPULocked(p *protocol.PerfCounterPack) {
	hash, ok := e.objectNames[p.ObjName] // AgentManager.getAgent(objName)
	if !ok {
		return
	}
	agent := e.objects[hash]
	if agent == nil {
		return
	}
	if t, ok := e.objectType(agent.ObjType); !ok || t.Family != familyJavaEE {
		return
	}
	first := e.cpuFirst[p.ObjName]
	if !javaGetBoolean(agent.Tags, tagAutodumpCPUEnabled) {
		delete(e.cpuFirst, p.ObjName)
		return
	}
	duration := javaGetInt(agent.Tags, tagAutodumpCPUDuration)
	threshold := javaGetInt(agent.Tags, tagAutodumpCPUThreshold)
	// Scala Int arithmetic: duration*2 wraps before widening to Long.
	if first > 0 && first < p.Time-int64(duration*2) {
		delete(e.cpuFirst, p.ObjName)
		return
	}
	if _, ok := mapEntry(p.Data, counterProcCPU); !ok {
		return
	}
	if !(float32(threshold) < javaGetFloat(p.Data, counterProcCPU)) {
		delete(e.cpuFirst, p.ObjName)
		return
	}
	if first <= 0 {
		first = p.Time
		if len(e.cpuFirst) < e.config.MaxObjects {
			e.cpuFirst[p.ObjName] = first
		} else {
			// Unreachable while names map to live objects; kept as a hard
			// bound. The streak is not remembered (counted), this pack is
			// still evaluated below.
			e.cpuLimited.Add(1)
		}
	}
	if first < p.Time-int64(duration) {
		delete(e.cpuFirst, p.ObjName)
		param := &protocol.MapPack{Entries: []protocol.Entry{
			{Key: "objHash", Value: protocol.Value{Type: protocol.ValueDecimal, Int: int64(agent.ObjHash)}},
			{Key: keyTriggerDumpReason, Value: protocol.Value{Type: protocol.ValueText, Text: dumpReasonCPU}},
		}}
		e.cpuCalls.offer(agentCall{hash: agent.ObjHash, cmd: cmdTriggerDumps, param: param})
	}
}
