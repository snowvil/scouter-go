package webapp

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// ActiveServiceController (ActiveServiceController.java:50-175,
// ActiveServiceConsumer.java).

type sActiveServiceStepCount struct {
	ObjHash    int32  `json:"objHash"`
	ObjName    string `json:"objName"`
	Step1Count int32  `json:"step1Count"`
	Step2Count int32  `json:"step2Count"`
	Step3Count int32  `json:"step3Count"`
}

// GET /v1/activeService/stepCount/ofType/{objType}: ACTIVESPEED_REAL_TIME {objType}.
func (s *Server) activeStepCount(w http.ResponseWriter, r *http.Request) {
	if err := serverIDParam(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	var packs []*protocol.MapPack
	err := s.call(r.Context(), "ACTIVESPEED_REAL_TIME", newMap(entry("objType", textValue(r.PathValue("objType")))), func(it Item) error {
		m, ok := it.Pack.(*protocol.MapPack)
		if !ok {
			return fmt.Errorf("ACTIVESPEED_REAL_TIME: item is %T, want MapPack", it.Pack)
		}
		packs = append(packs, m)
		return nil
	})
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	out := make([]sActiveServiceStepCount, 0, len(packs))
	for _, m := range packs {
		hash := mapInt32(m, "objHash")
		name, err := s.objName(r.Context(), hash)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		out = append(out, sActiveServiceStepCount{ObjHash: hash, ObjName: name,
			Step1Count: mapInt32(m, "act1"), Step2Count: mapInt32(m, "act2"), Step3Count: mapInt32(m, "act3")})
	}
	s.writeJSON(w, r, out)
}

type sActiveService struct {
	ObjHash       int32  `json:"objHash"`
	ObjName       string `json:"objName"`
	ThreadID      int64  `json:"threadId"`
	ThreadName    any    `json:"threadName"`
	ThreadStatus  any    `json:"threadStatus"`
	ThreadCPUTime int64  `json:"threadCpuTime"`
	TxidName      any    `json:"txidName"`
	ActiveDate    string `json:"activeDate"`
	Txid          int64  `json:"txid"`
	Elapsed       int64  `json:"elapsed"`
	ServiceName   any    `json:"serviceName"`
	Ipaddr        any    `json:"ipaddr"`
	Note          any    `json:"note"`
	Mode          string `json:"mode"`
}

// GET /v1/activeService/ofType/{objType}: OBJECT_ACTIVE_SERVICE_LIST {objType}.
func (s *Server) activeListByType(w http.ResponseWriter, r *http.Request) {
	if err := serverIDParam(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.activeList(w, r, newMap(entry("objType", textValue(r.PathValue("objType")))))
}

// GET /v1/activeService/ofObject/{objHash}: OBJECT_ACTIVE_SERVICE_LIST {objHash}.
func (s *Server) activeListByObject(w http.ResponseWriter, r *http.Request) {
	hash, err := objHashRequest(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.activeList(w, r, objHashParam(hash))
}

func (s *Server) activeList(w http.ResponseWriter, r *http.Request, param *protocol.MapPack) {
	const cmd = "OBJECT_ACTIVE_SERVICE_LIST"
	var packs []*protocol.MapPack
	err := s.call(r.Context(), cmd, param, func(it Item) error {
		m, ok := it.Pack.(*protocol.MapPack)
		if !ok {
			return fmt.Errorf("%s: item is %T, want MapPack", cmd, it.Pack)
		}
		packs = append(packs, m)
		return nil
	})
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	out, err := s.activeServices(r, packs)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, out)
}

// activeServices is SActiveService.ofPackList: packs of objects missing from
// the object cache are skipped; the result is sorted by elapsed descending.
func (s *Server) activeServices(r *http.Request, packs []*protocol.MapPack) ([]sActiveService, error) {
	const cmd = "OBJECT_ACTIVE_SERVICE_LIST"
	today := s.today()
	out := []sActiveService{}
	for _, m := range packs {
		hash := mapInt32(m, "objHash")
		o, err := s.object(r.Context(), hash)
		if err != nil {
			return nil, err
		}
		if o == nil {
			continue
		}
		ids, ok := mapList(m, "id")
		if !ok || len(ids) == 0 {
			continue
		}
		lists, err := requireLists(cmd, m, "name", "stat", "cpu", "txid", "elapsed", "service", "sql", "subcall")
		if err != nil {
			return nil, err
		}
		ips, hasIP := mapList(m, "ip")
		for i := range ids {
			v := make([]protocol.Value, len(lists))
			for k, l := range lists {
				if v[k], err = listAt(cmd, "column", l, i); err != nil {
					return nil, err
				}
			}
			a := sActiveService{ObjHash: hash, ObjName: o.ObjName, ThreadID: numInt64(ids[i]), ThreadName: valueString(v[0]),
				ThreadStatus: valueString(v[1]), ThreadCPUTime: numInt64(v[2]), TxidName: valueString(v[3]), Elapsed: numInt64(v[4]),
				ServiceName: valueString(v[5]), Mode: "NONE", ActiveDate: today}
			if sql, _ := valueString(v[6]).(string); sql != "" {
				a.Note, a.Mode = sql, "SQL"
			} else if api, _ := valueString(v[7]).(string); api != "" {
				a.Note, a.Mode = api, "SUBCALL"
			}
			if hasIP {
				ip, err := listAt(cmd, "ip", ips, i)
				if err != nil {
					return nil, err
				}
				a.Ipaddr = valueString(ip)
			}
			txidName, _ := a.TxidName.(string)
			txid, err := parseHexa32(txidName)
			if err != nil {
				return nil, fmt.Errorf("%s txid %q: %w", cmd, txidName, err)
			}
			a.Txid = txid
			out = append(out, a)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Elapsed > out[j].Elapsed })
	return out, nil
}

type keyValueData struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

type threadContents struct {
	ServiceThread    bool           `json:"serviceThread"`
	Time             int32          `json:"time"`
	KeyValueDataList []keyValueData `json:"keyValueDataList"`
}

// GET /v1/activeService/control/thread/{threadId}/ofObject/{objHash}?action=:
// OBJECT_THREAD_CONTROL {objHash, id, action} -> ThreadContents.
func (s *Server) activeControlThread(w http.ResponseWriter, r *http.Request) {
	const cmd = "OBJECT_THREAD_CONTROL"
	hash, err := objHashRequest(r)
	var threadID int64
	if err == nil {
		threadID, err = pathInt64(r, "threadId")
	}
	if err == nil && !r.URL.Query().Has("action") {
		err = newAPIError(http.StatusBadRequest, http.StatusBadRequest, "[ValidationException] InterruptedRealTimeActiveServiceByObjHash.arg2 may not be null")
	}
	var m *protocol.MapPack
	if err == nil {
		param := newMap(entry("objHash", decimalValue(int64(hash))), entry("id", decimalValue(threadID)), entry("action", textValue(r.URL.Query().Get("action"))))
		m, err = s.mustSingle(r.Context(), cmd, param)
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, threadContentsOf(m))
}

// threadContentsOf is ThreadContents.of: keys sorted, "Stack Trace" skipped,
// Text/Decimal/Double/Float values kept; any other kept key starting with
// "Service" marks a service thread.
func threadContentsOf(m *protocol.MapPack) threadContents {
	out := threadContents{KeyValueDataList: []keyValueData{}}
	elapsed, _ := mapGet(m, "Service Elapsed")
	if isNumber(elapsed) {
		out.Time = numInt32(elapsed)
	} else if t, ok := valueString(elapsed).(string); ok {
		out.Time = parseIntOrZero32(t)
	}
	keys := make([]string, 0, len(m.Entries))
	seen := map[string]bool{}
	for _, e := range m.Entries {
		if !seen[e.Key] {
			seen[e.Key] = true
			keys = append(keys, e.Key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == "Stack Trace" {
			continue
		}
		v, _ := mapGet(m, key)
		switch v.Type {
		case protocol.ValueText:
			out.KeyValueDataList = append(out.KeyValueDataList, keyValueData{key, v.Text})
		case protocol.ValueDecimal:
			out.KeyValueDataList = append(out.KeyValueDataList, keyValueData{key, v.Int})
		case protocol.ValueDouble:
			out.KeyValueDataList = append(out.KeyValueDataList, keyValueData{key, v.Double})
		case protocol.ValueFloat:
			out.KeyValueDataList = append(out.KeyValueDataList, keyValueData{key, v.Float})
		}
		if strings.HasPrefix(key, "Service") {
			out.ServiceThread = true
		}
	}
	return out
}

type activeThread struct {
	TxidName        any    `json:"txidName"`
	Elapsed         int64  `json:"elapsed"`
	ServiceName     any    `json:"serviceName"`
	Note            any    `json:"note"`
	Mode            string `json:"mode"`
	ThreadID        int64  `json:"threadId"`
	ThreadName      any    `json:"threadName"`
	ThreadStatus    any    `json:"threadStatus"`
	ThreadCPUTime   int64  `json:"threadCpuTime"`
	ThreadUserTime  int64  `json:"threadUserTime"`
	BlockCount      int64  `json:"blockCount"`
	BlockTime       int64  `json:"blockTime"`
	WaitedCount     int64  `json:"waitedCount"`
	WaitedTime      int64  `json:"waitedTime"`
	LockOwnerID     int64  `json:"lockOwnerId"`
	LockName        any    `json:"lockName"`
	LockOwnerName   any    `json:"lockOwnerName"`
	StackTrace      any    `json:"stackTrace"`
	SQLActiveBindVa any    `json:"sqlActiveBindVar"`
}

// GET /v1/activeService/thread/{threadId}/ofObject/{objHash}?txidName=|txid=:
// OBJECT_THREAD_DETAIL {objHash, id, txid} -> ActiveThread.
func (s *Server) activeThread(w http.ResponseWriter, r *http.Request) {
	const cmd = "OBJECT_THREAD_DETAIL"
	hash, err := objHashRequest(r)
	var threadID, txid int64
	if err == nil {
		threadID, err = pathInt64(r, "threadId")
	}
	if err == nil {
		txid, err = queryInt64(r, "txid")
	}
	if name := r.URL.Query().Get("txidName"); err == nil && txid == 0 && strings.TrimSpace(name) != "" {
		if txid, err = parseHexa32(name); err != nil {
			err = validateError("txidName is not a hexa32 value: " + name)
		}
	}
	var m *protocol.MapPack
	if err == nil {
		param := newMap(entry("objHash", decimalValue(int64(hash))), entry("id", decimalValue(threadID)), entry("txid", decimalValue(txid)))
		m, err = s.mustSingle(r.Context(), cmd, param)
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	a := activeThread{TxidName: mapTextNull(m, "Service Txid"), Elapsed: mapInt(m, "Service Elapsed"), ServiceName: mapTextNull(m, "Service Name"), Mode: "NONE"}
	if sql := mapText(m, "SQL"); sql != "" {
		a.Note, a.Mode = sql, "SQL"
	} else if sub := mapText(m, "Subcall"); sub != "" {
		a.Note, a.Mode = sub, "SUBCALL"
	}
	if bind := mapText(m, "SQLActiveBindVar"); bind != "" {
		a.SQLActiveBindVa = bind
	}
	a.ThreadID = mapInt(m, "Thread Id")
	a.ThreadName = mapTextNull(m, "Thread Name")
	a.ThreadStatus = mapTextNull(m, "State")
	a.ThreadCPUTime = mapInt(m, "Thread Cpu Time")
	a.ThreadUserTime = mapInt(m, "Thread User Time")
	a.BlockCount = mapInt(m, "Blocked Count")
	a.BlockTime = mapInt(m, "Blocked Time")
	a.WaitedCount = mapInt(m, "Waited Count")
	a.WaitedTime = mapInt(m, "Waited Time")
	a.LockOwnerID = mapInt(m, "Lock Owner Id")
	a.LockName = mapTextNull(m, "Lock Name")
	a.LockOwnerName = mapTextNull(m, "Lock Owner Name")
	a.StackTrace = mapTextNull(m, "Stack Trace")
	s.writeJSON(w, r, a)
}
