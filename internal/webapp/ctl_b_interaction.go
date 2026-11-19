package webapp

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// InteractionCounterController (/v1/interactionCounter).

type bCounterValue struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
}

type bInteractionData struct {
	ObjName         string          `json:"objName"`
	InteractionType string          `json:"interactionType"`
	FromObjHash     int32           `json:"fromObjHash"`
	FromObjName     string          `json:"fromObjName"`
	FromObjCounters []bCounterValue `json:"fromObjCounters"`
	ToObjHash       int32           `json:"toObjHash"`
	ToObjName       string          `json:"toObjName"`
	ToObjCounters   []bCounterValue `json:"toObjCounters"`
	Period          int32           `json:"period"`
	Count           int32           `json:"count"`
	ErrorCount      int32           `json:"errorCount"`
	TotalElapsed    int64           `json:"totalElapsed"`
}

// bInteractions issues INTR_COUNTER_REAL_TIME_BY_OBJ {objType, objHash[]}.
// objType is MapPack.put(String) of a possibly-null String -> "".
func (s *Server) bInteractions(ctx context.Context, objType string, hashes []int32) ([]*bInteractionData, error) {
	param := bMap(bEntry("objType", bTextV(objType)), bEntry("objHash", bDecList(hashes)))
	var packs []*protocol.InteractionPerfCounterPack
	err := s.call(ctx, "INTR_COUNTER_REAL_TIME_BY_OBJ", param, func(it Item) error {
		p, ok := it.Pack.(*protocol.InteractionPerfCounterPack)
		if !ok {
			return bInternal("unexpected INTR_COUNTER_REAL_TIME_BY_OBJ response")
		}
		packs = append(packs, p)
		return nil
	})
	if err != nil {
		return nil, err
	}
	// InteractionCounterData.of: object names come from a GET_TEXT_ANY_TYPE preload.
	// Text lookups run after the call so no collector slot is held (A's pool rule).
	keys := make([]textKey, 0, 2*len(packs))
	for _, p := range packs {
		keys = append(keys, textKey{Type: "object", Hash: p.FromHash}, textKey{Type: "object", Hash: p.ToHash})
	}
	if err := s.bPreload(ctx, keys); err != nil {
		return nil, err
	}
	out := make([]*bInteractionData, 0, len(packs))
	for _, p := range packs {
		out = append(out, &bInteractionData{
			ObjName: p.ObjName, InteractionType: p.InteractionType,
			FromObjHash: p.FromHash, FromObjName: s.text(ctx, "", "object", p.FromHash), FromObjCounters: []bCounterValue{},
			ToObjHash: p.ToHash, ToObjName: s.text(ctx, "", "object", p.ToHash), ToObjCounters: []bCounterValue{},
			Period: p.Period, Count: p.Count, ErrorCount: p.ErrorCount, TotalElapsed: p.TotalElapsed,
		})
	}
	return out, nil
}

// GET /v1/interactionCounter/realTime/ofType/{objType}
func (s *Server) bInteractionByType(w http.ResponseWriter, r *http.Request) {
	if err := bServerID(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	list, err := s.bInteractions(r.Context(), r.PathValue("objType"), nil)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, bInteractionList(list))
}

func bInteractionList(list []*bInteractionData) []*bInteractionData {
	if list == nil {
		return []*bInteractionData{}
	}
	return list
}

// bParentName is AgentObject.getParentName.
func bParentName(objName string) string {
	pos := strings.LastIndex(objName, "/")
	if pos <= 0 {
		return "/"
	}
	return objName[:pos]
}

// bRealtimeCounters is CounterConsumer.retrieveRealTimeCountersByObjHashes
// (COUNTER_REAL_TIME_ALL_MULTI) keyed "objHash:name". Java builds SCounter only
// for objects known to AgentModelThread (NPE otherwise, W3); unknown hashes are
// skipped here. An empty hash set is not sent (the result would be empty).
func (s *Server) bRealtimeCounters(ctx context.Context, hashes []int32, counter string, known map[int32]*protocol.ObjectPack) (map[string]bCounterValue, error) {
	out := map[string]bCounterValue{}
	if len(hashes) == 0 {
		return out, nil
	}
	param := bMap(bEntry("objType", bTextV("")), bEntry("counter", bTextList([]string{counter})), bEntry("objHash", bDecList(hashes)))
	pack, err := s.bSingle(ctx, "COUNTER_REAL_TIME_ALL_MULTI", param)
	if err != nil {
		return nil, err
	}
	mp, _ := pack.(*protocol.MapPack)
	if mp == nil {
		return out, nil
	}
	hs, names, values := bMapList(mp, "objHash"), bMapList(mp, "counter"), bMapList(mp, "value")
	for i := range hs {
		h := bInt(hs[i])
		if known[h] == nil {
			continue
		}
		name := ""
		if n := bListString(names, i); n != nil {
			name = *n
		}
		out[strconv.Itoa(int(h))+":"+name] = bCounterValue{Name: name, Value: bValueJava(bAt(values, i))}
	}
	return out, nil
}

// GET /v1/interactionCounter/realTime?objHashes=
func (s *Server) bInteractionByHashes(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := bServerID(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	raw, err := bRequiredQuery(r, "objHashes")
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	hashes, err := bSplitInt32Set("objHashes", raw)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	list, err := s.bInteractions(ctx, "", hashes)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	objs, err := s.objects(ctx)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	known := make(map[int32]*protocol.ObjectPack, len(objs))
	for _, o := range objs {
		if o != nil {
			known[o.ObjHash] = o
		}
	}
	var ends []int32
	for _, d := range list {
		ends = append(ends, d.FromObjHash, d.ToObjHash)
	}
	ends = bJavaIntSet(ends)
	var parents, javas []int32
	for _, h := range ends {
		o := known[h]
		if o == nil {
			continue
		}
		parents = append(parents, bJavaHash(bParentName(o.ObjName)))
		if s.objTypeFamily(ctx, o.ObjType) == "javaee" {
			javas = append(javas, o.ObjHash)
		}
	}
	host, err := s.bRealtimeCounters(ctx, bJavaIntSet(parents), "Cpu", known)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	java, err := s.bRealtimeCounters(ctx, bJavaIntSet(javas), "ActiveSpeed", known)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	add := func(dst *[]bCounterValue, m map[string]bCounterValue, key string, ok bool) {
		if v, found := m[key]; ok && found {
			*dst = append(*dst, v)
		}
	}
	for _, d := range list {
		from, to := known[d.FromObjHash], known[d.ToObjHash]
		fromKey, toKey := "", ""
		if from != nil {
			fromKey = strconv.Itoa(int(bJavaHash(bParentName(from.ObjName)))) + ":Cpu"
		}
		if to != nil {
			toKey = strconv.Itoa(int(bJavaHash(bParentName(to.ObjName)))) + ":Cpu"
		}
		add(&d.FromObjCounters, host, fromKey, from != nil)
		add(&d.FromObjCounters, java, strconv.Itoa(int(d.FromObjHash))+":ActiveSpeed", true)
		add(&d.ToObjCounters, host, toKey, to != nil)
		add(&d.ToObjCounters, java, strconv.Itoa(int(d.ToObjHash))+":ActiveSpeed", true)
	}
	s.writeJSON(w, r, bInteractionList(list))
}
