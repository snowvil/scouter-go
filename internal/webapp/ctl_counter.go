package webapp

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// CounterController (CounterController.java:54-207, CounterConsumer.java).

const (
	counterRangeLimitSec = 6 * 60 * 60 // CounterRequest.limitRangeSec
	latestLimitSec       = 60 * 60     // LatestCounterRequest.limitRangeSec
)

type sCounter struct {
	ObjHash     int32  `json:"objHash"`
	ObjName     string `json:"objName"`
	Name        any    `json:"name"`
	DisplayName any    `json:"displayName"`
	Unit        any    `json:"unit"`
	Value       any    `json:"value"`
}

// agentInfo resolves objName/objType for counter views. Java dereferences the
// AgentModelThread entry (NPE -> 500, W3); Go reports "UNKNOWN" with the
// counter engine's unknown-type display/unit.
func (s *Server) agentInfo(ctx context.Context, hash int32) (name, objType string, err error) {
	o, err := s.object(ctx, hash)
	if err != nil {
		return "", "", err
	}
	if o == nil {
		return "UNKNOWN", "", nil
	}
	return o.ObjName, o.ObjType, nil
}

// realTimeCounters is retrieveRealTimeCountersByObjTypeOrObjHashes:
// COUNTER_REAL_TIME_ALL_MULTI {objType, counter[], objHash[]}.
func (s *Server) realTimeCounters(w http.ResponseWriter, r *http.Request, objType string, hashes []int32) {
	ctx := r.Context()
	counters := javaStringSetOrder(splitParam(r.PathValue("counters")))
	names := make([]protocol.Value, len(counters))
	for i, c := range counters {
		names[i] = textValue(c)
	}
	hashValues := make([]protocol.Value, len(hashes))
	for i, h := range hashes {
		hashValues[i] = decimalValue(int64(h))
	}
	const cmd = "COUNTER_REAL_TIME_ALL_MULTI"
	m, err := s.single(ctx, cmd, newMap(entry("objType", textValue(objType)), entry("counter", listValue(names)), entry("objHash", listValue(hashValues))))
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	out := []sCounter{}
	if m == nil {
		s.writeJSON(w, r, out)
		return
	}
	lists, err := requireLists(cmd, m, "objHash", "counter", "value")
	var engine *counterEngine
	if err == nil {
		engine, err = s.counterEngine(ctx)
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	for i := range lists[0] {
		vals := make([]protocol.Value, 3)
		for k, key := range []string{"objHash", "counter", "value"} {
			if vals[k], err = listAt(cmd, key, lists[k], i); err != nil {
				s.writeErr(w, r, err)
				return
			}
		}
		hash := numInt32(vals[0])
		if !isNumber(vals[0]) {
			if t, ok := valueString(vals[0]).(string); ok {
				hash = parseIntOrZero32(t)
			}
		}
		name, typ, err := s.agentInfo(ctx, hash)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		counter := valueString(vals[1])
		cname, _ := counter.(string)
		out = append(out, sCounter{ObjHash: hash, ObjName: name, Name: counter, DisplayName: engine.displayName(typ, cname),
			Unit: engine.unit(typ, cname), Value: javaObject(vals[2])})
	}
	s.writeJSON(w, r, out)
}

// GET /v1/counter/realTime/{counters}/ofType/{objType}.
func (s *Server) counterRealTimeByType(w http.ResponseWriter, r *http.Request) {
	if err := serverIDParam(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.realTimeCounters(w, r, r.PathValue("objType"), nil)
}

// requiredObjHashes reads the @NotNull objHashes query (HashSet<Integer> order).
func requiredObjHashes(r *http.Request, method string) ([]int32, error) {
	if !r.URL.Query().Has("objHashes") {
		return nil, newAPIError(http.StatusBadRequest, http.StatusBadRequest, "[ValidationException] "+method+" may not be null")
	}
	hashes, err := splitParamInt32("objHashes", r.URL.Query().Get("objHashes"))
	if err != nil {
		return nil, err
	}
	return javaIntSetOrder(hashes), nil
}

// GET /v1/counter/realTime/{counters}?objHashes=: objType is Java null,
// written as an empty Text.
func (s *Server) counterRealTimeByHashes(w http.ResponseWriter, r *http.Request) {
	err := serverIDParam(r)
	var hashes []int32
	if err == nil {
		hashes, err = requiredObjHashes(r, "retrieveRealTimeCountersByObjHashes.arg0")
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.realTimeCounters(w, r, "", hashes)
}

type counterView struct {
	ObjHash         int32     `json:"objHash"`
	ObjName         string    `json:"objName"`
	StartTimeMillis int64     `json:"startTimeMillis"`
	EndTimeMillis   int64     `json:"endTimeMillis"`
	Name            string    `json:"name"`
	DisplayName     any       `json:"displayName"`
	Unit            any       `json:"unit"`
	TimeList        []int64   `json:"timeList"`
	ValueList       []float64 `json:"valueList"`
}

// counterTarget is the objType or objHashes selector of a request.
type counterTarget struct {
	objType string
	hashes  []int32
	byType  bool
}

func (t counterTarget) entry() protocol.Entry {
	if t.byType {
		return entry("objType", textValue(t.objType))
	}
	values := make([]protocol.Value, len(t.hashes))
	for i, h := range t.hashes {
		values[i] = decimalValue(int64(h))
	}
	return entry("objHash", listValue(values))
}

// counterSeries reads one {objHash, time[], value[]} pack.
func counterSeries(cmd string, m *protocol.MapPack) (int32, []int64, []float64, error) {
	lists, err := requireLists(cmd, m, "time", "value")
	if err != nil {
		return 0, nil, nil, err
	}
	times := make([]int64, len(lists[0]))
	values := make([]float64, len(lists[0]))
	for i, t := range lists[0] {
		if t.Type != protocol.ValueDecimal {
			return 0, nil, nil, fmt.Errorf("%s time[%d] is not a Decimal", cmd, i)
		}
		times[i] = t.Int
		v, err := listAt(cmd, "value", lists[1], i)
		if err != nil {
			return 0, nil, nil, err
		}
		values[i] = numFloat64(v)
	}
	return mapInt32(m, "objHash"), times, values, nil
}

// pastCounters is retrieveCounterByObjTypeOrObjHashes: one
// COUNTER_PAST_TIME_ALL per date unit, merged per objHash.
func (s *Server) pastCounters(ctx context.Context, target counterTarget, counter string, start, end int64) ([]*counterView, error) {
	const cmd = "COUNTER_PAST_TIME_ALL"
	engine, err := s.counterEngine(ctx)
	if err != nil {
		return nil, err
	}
	loc := s.location()
	byHash := map[int32]*counterView{}
	var order []int32
	for unit := dateUnit(start, loc); dateUnitMillis(unit, loc) < end; unit++ {
		day := dateUnitMillis(unit, loc)
		param := newMap(target.entry(), entry("counter", textValue(counter)),
			entry("stime", decimalValue(max(day+1000, start))), entry("etime", decimalValue(min(day+86400000-1000, end))))
		// Each answer pack is reduced to its series as it arrives: a decoded
		// pack keeps a ~250-byte Value per point, and an objType answer over
		// hours has millions of points. Object names are looked up after the
		// call (it holds a collector connection).
		type series struct {
			hash   int32
			times  []int64
			values []float64
		}
		var got []series
		err := s.call(ctx, cmd, param, func(it Item) error {
			m, ok := it.Pack.(*protocol.MapPack)
			if !ok {
				return fmt.Errorf("%s: item is %T, want MapPack", cmd, it.Pack)
			}
			hash, times, values, err := counterSeries(cmd, m)
			if err != nil {
				return err
			}
			got = append(got, series{hash, times, values})
			return nil
		})
		if err != nil {
			return nil, err
		}
		for _, g := range got {
			hash, times, values := g.hash, g.times, g.values
			if v, ok := byHash[hash]; ok {
				v.TimeList = append(v.TimeList, times...)
				v.ValueList = append(v.ValueList, values...)
				v.EndTimeMillis = end
				continue
			}
			name, typ, err := s.agentInfo(ctx, hash)
			if err != nil {
				return nil, err
			}
			byHash[hash] = &counterView{ObjHash: hash, ObjName: name, StartTimeMillis: start, EndTimeMillis: end, Name: counter,
				DisplayName: engine.displayName(typ, counter), Unit: engine.unit(typ, counter), TimeList: times, ValueList: values}
			order = append(order, hash)
		}
	}
	out := make([]*counterView, 0, len(order))
	for _, i := range javaHashOrder(order) { // new ArrayList<>(HashMap<Integer,..>.values())
		out = append(out, byHash[order[i]])
	}
	return out, nil
}

// pastRange is CounterRequest.validate.
func (s *Server) pastRange(r *http.Request) (int64, int64, error) {
	q := r.URL.Query()
	startYmdHms, endYmdHms := q.Get("startYmdHms"), q.Get("endYmdHms")
	start, err := queryInt64(r, "startTimeMillis")
	if err != nil {
		return 0, 0, err
	}
	end, err := queryInt64(r, "endTimeMillis")
	if err != nil {
		return 0, 0, err
	}
	if strings.TrimSpace(startYmdHms) != "" || strings.TrimSpace(endYmdHms) != "" {
		if strings.TrimSpace(startYmdHms) == "" || strings.TrimSpace(endYmdHms) == "" {
			return 0, 0, validateError("startYmdHms and endYmdHms should be not null !")
		}
		if start > 0 || end > 0 {
			return 0, 0, validateError("startYmdHms, endYmdHms and startTimeMillis, endTimeMills must not coexist!")
		}
		// Java DateTimeParseException is 500 (W12); Go answers 400.
		st, err := time.ParseInLocation("20060102150405", startYmdHms, s.location())
		if err != nil {
			return 0, 0, validateError("startYmdHms is not yyyyMMddHHmmss: " + startYmdHms)
		}
		et, err := time.ParseInLocation("20060102150405", endYmdHms, s.location())
		if err != nil {
			return 0, 0, validateError("endYmdHms is not yyyyMMddHHmmss: " + endYmdHms)
		}
		start, end = st.Unix()*1000, et.Unix()*1000
	} else if start <= 0 || end <= 0 {
		return 0, 0, validateError("startTimeMillis and endTimeMillis must have value!")
	}
	if end-start > counterRangeLimitSec*1000 {
		return 0, 0, validateError("query range should be lower than " + strconv.Itoa(counterRangeLimitSec) + " seconds!")
	}
	return start, end, nil
}

func (s *Server) writeCounterViews(w http.ResponseWriter, r *http.Request, target counterTarget, start, end int64) {
	views, err := s.pastCounters(r.Context(), target, r.PathValue("counter"), start, end)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, views)
}

// GET /v1/counter/{counter}/ofType/{objType}.
func (s *Server) counterPastByType(w http.ResponseWriter, r *http.Request) {
	err := serverIDParam(r)
	var start, end int64
	if err == nil {
		start, end, err = s.pastRange(r)
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeCounterViews(w, r, counterTarget{objType: r.PathValue("objType"), byType: true}, start, end)
}

// GET /v1/counter/{counter}?objHashes=.
func (s *Server) counterPastByHashes(w http.ResponseWriter, r *http.Request) {
	err := serverIDParam(r)
	var hashes []int32
	if err == nil {
		hashes, err = requiredObjHashes(r, "retrieveCounterByObjHashes.arg0.objHashes")
	}
	var start, end int64
	if err == nil {
		start, end, err = s.pastRange(r)
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeCounterViews(w, r, counterTarget{hashes: hashes}, start, end)
}

// latestRange is LatestCounterRequest.validate + toCounterRequestBy*.
func (s *Server) latestRange(r *http.Request) (int64, int64, error) {
	sec, err := pathInt32(r, "latestSec")
	if err != nil {
		return 0, 0, err
	}
	if sec > latestLimitSec {
		return 0, 0, validateError("query range should be lower than " + strconv.Itoa(latestLimitSec) + " seconds!")
	}
	end := s.collectorTime()
	return end - int64(sec*1000), end, nil
}

// GET /v1/counter/{counter}/latest/{latestSec}/ofType/{objType}.
func (s *Server) counterLatestByType(w http.ResponseWriter, r *http.Request) {
	err := serverIDParam(r)
	var start, end int64
	if err == nil {
		start, end, err = s.latestRange(r)
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeCounterViews(w, r, counterTarget{objType: r.PathValue("objType"), byType: true}, start, end)
}

// GET /v1/counter/{counter}/latest/{latestSec}?objHashes=.
func (s *Server) counterLatestByHashes(w http.ResponseWriter, r *http.Request) {
	err := serverIDParam(r)
	var hashes []int32
	if err == nil {
		hashes, err = requiredObjHashes(r, "retrieveLatestCounterByObjHash.arg0.objHashes")
	}
	var start, end int64
	if err == nil {
		start, end, err = s.latestRange(r)
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeCounterViews(w, r, counterTarget{hashes: hashes}, start, end)
}

type avgCounterView struct {
	ObjHash     int32     `json:"objHash"`
	ObjName     string    `json:"objName"`
	FromYmd     string    `json:"fromYmd"`
	ToYmd       string    `json:"toYmd"`
	Name        string    `json:"name"`
	DisplayName any       `json:"displayName"`
	Unit        any       `json:"unit"`
	TimeList    []int64   `json:"timeList"`
	ValueList   []float64 `json:"valueList"`
}

// statCounters is retrieveAvgCounterByObjTypeOrHashes: COUNTER_PAST_LONGDATE_ALL
// {objType|objHash[], sDate, eDate, counter}.
func (s *Server) statCounters(w http.ResponseWriter, r *http.Request, target counterTarget, method string) {
	const cmd = "COUNTER_PAST_LONGDATE_ALL"
	q := r.URL.Query()
	for _, name := range []string{"startYmd", "endYmd"} {
		if !q.Has(name) {
			s.writeErr(w, r, newAPIError(http.StatusBadRequest, http.StatusBadRequest, "[ValidationException] "+method+".arg0."+name+" may not be null"))
			return
		}
	}
	ctx, counter, startYmd, endYmd := r.Context(), r.PathValue("counter"), q.Get("startYmd"), q.Get("endYmd")
	engine, err := s.counterEngine(ctx)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	param := newMap(target.entry(), entry("sDate", textValue(startYmd)), entry("eDate", textValue(endYmd)), entry("counter", textValue(counter)))
	var packs []*protocol.MapPack
	err = s.call(ctx, cmd, param, func(it Item) error {
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
	byHash := map[int32]*avgCounterView{}
	var order []int32
	for _, m := range packs {
		hash, times, values, err := counterSeries(cmd, m)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		if v, ok := byHash[hash]; ok {
			v.TimeList = append(v.TimeList, times...)
			v.ValueList = append(v.ValueList, values...)
			continue
		}
		name, typ, err := s.agentInfo(ctx, hash)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		byHash[hash] = &avgCounterView{ObjHash: hash, ObjName: name, FromYmd: startYmd, ToYmd: endYmd, Name: counter,
			DisplayName: engine.displayName(typ, counter), Unit: engine.unit(typ, counter), TimeList: times, ValueList: values}
		order = append(order, hash)
	}
	out := make([]*avgCounterView, 0, len(order))
	for _, i := range javaHashOrder(order) {
		out = append(out, byHash[order[i]])
	}
	s.writeJSON(w, r, out)
}

// GET /v1/counter/stat/{counter}/ofType/{objType}?startYmd=&endYmd=.
func (s *Server) counterStatByType(w http.ResponseWriter, r *http.Request) {
	if err := serverIDParam(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.statCounters(w, r, counterTarget{objType: r.PathValue("objType"), byType: true}, "retrieveAvgCounterByObjType")
}

// GET /v1/counter/stat/{counter}?objHashes=&startYmd=&endYmd=.
func (s *Server) counterStatByHashes(w http.ResponseWriter, r *http.Request) {
	err := serverIDParam(r)
	var hashes []int32
	if err == nil {
		hashes, err = requiredObjHashes(r, "retrieveAvgCounterByObjHash.arg0.objHashes")
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.statCounters(w, r, counterTarget{hashes: hashes}, "retrieveAvgCounterByObjHash")
}
