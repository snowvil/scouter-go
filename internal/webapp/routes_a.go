package webapp

import "net/http"

// registerA registers the A-owned controllers (38 endpoints). Patterns carry
// the /scouter context path; filters run in Server.serveHTTP.
func (s *Server) registerA(mux *http.ServeMux) {
	const p = "/scouter/v1"
	// UserController
	mux.HandleFunc("POST "+p+"/user/login", s.userLogin)
	mux.HandleFunc("POST "+p+"/user/loginGetToken", s.userLoginGetToken)
	// InfoController
	mux.HandleFunc("GET "+p+"/info/server", s.infoServer)
	mux.HandleFunc("GET "+p+"/info/counter-model", s.infoCounterModel)
	// ObjectController
	mux.HandleFunc("GET "+p+"/object", s.objectList)
	mux.HandleFunc("GET "+p+"/object/remove/inactive", s.objectRemoveInactive)
	mux.HandleFunc("GET "+p+"/object/remove/inactive/server", s.objectRemoveInactive)
	mux.HandleFunc("GET "+p+"/object/threadList/{objHash}", s.objectThreadList)
	mux.HandleFunc("GET "+p+"/object/threadDump/{objHash}", s.objectThreadDump)
	mux.HandleFunc("GET "+p+"/object/heapHistogram/{objHash}", s.objectHeapHistogram)
	mux.HandleFunc("GET "+p+"/object/env/{objHash}", s.objectEnv)
	mux.HandleFunc("GET "+p+"/object/socket/{objHash}", s.objectSocket)
	// HostObjectRequestController
	mux.HandleFunc("GET "+p+"/object/host/realTime/top/ofObject/{objHash}", s.hostTop)
	mux.HandleFunc("GET "+p+"/object/host/realTime/disk/ofObject/{objHash}", s.hostDisk)
	// ActiveServiceController
	mux.HandleFunc("GET "+p+"/activeService/stepCount/ofType/{objType}", s.activeStepCount)
	mux.HandleFunc("GET "+p+"/activeService/ofType/{objType}", s.activeListByType)
	mux.HandleFunc("GET "+p+"/activeService/ofObject/{objHash}", s.activeListByObject)
	mux.HandleFunc("GET "+p+"/activeService/control/thread/{threadId}/ofObject/{objHash}", s.activeControlThread)
	mux.HandleFunc("GET "+p+"/activeService/thread/{threadId}/ofObject/{objHash}", s.activeThread)
	// ConfigureController
	mux.HandleFunc("GET "+p+"/configure/server", s.configureServer)
	mux.HandleFunc("GET "+p+"/configure/object/{objHash}", s.configureObject)
	mux.HandleFunc("POST "+p+"/configure/set/server", s.configureSetServer)
	mux.HandleFunc("POST "+p+"/configure/set/object/{objHash}", s.configureSetObject)
	mux.HandleFunc("PUT "+p+"/configure/set/kv/server", s.configureSetKVServer)
	mux.HandleFunc("PUT "+p+"/configure/set/kv/ofType/{objType}", s.configureSetKVType)
	// AlertController
	mux.HandleFunc("GET "+p+"/alert/realTime/{offset1}/{offset2}", s.alertRealTime)
	mux.HandleFunc("GET "+p+"/alert/scripting/{counterName}", s.alertScripting)
	mux.HandleFunc("POST "+p+"/alert/set/config/scripting/{counterName}", s.alertSetConfig)
	mux.HandleFunc("POST "+p+"/alert/set/rule/scripting/{counterName}", s.alertSetRule)
	mux.HandleFunc("GET "+p+"/alert/read/scripting/{loop}/{index}", s.alertReadScripting)
	// CounterController
	mux.HandleFunc("GET "+p+"/counter/realTime/{counters}/ofType/{objType}", s.counterRealTimeByType)
	mux.HandleFunc("GET "+p+"/counter/realTime/{counters}", s.counterRealTimeByHashes)
	mux.HandleFunc("GET "+p+"/counter/{counter}/ofType/{objType}", s.counterPastByType)
	mux.HandleFunc("GET "+p+"/counter/{counter}", s.counterPastByHashes)
	mux.HandleFunc("GET "+p+"/counter/stat/{counter}/ofType/{objType}", s.counterStatByType)
	mux.HandleFunc("GET "+p+"/counter/stat/{counter}", s.counterStatByHashes)
	mux.HandleFunc("GET "+p+"/counter/{counter}/latest/{latestSec}/ofType/{objType}", s.counterLatestByType)
	mux.HandleFunc("GET "+p+"/counter/{counter}/latest/{latestSec}", s.counterLatestByHashes)
}
