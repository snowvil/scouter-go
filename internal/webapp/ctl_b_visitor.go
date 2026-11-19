package webapp

import (
	"net/http"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// VisitorController (/v1/visitor).

type bVisitorGroup struct {
	Time  *string `json:"time"`
	Value int64   `json:"value"`
}

// bVisitorGroupOf is VisitorGroup.of(MapPack): getText("time"), getLong("value").
func bVisitorGroupOf(p *protocol.MapPack) bVisitorGroup {
	return bVisitorGroup{Time: bMapText(p, "time"), Value: bMapLong(p, "value")}
}

// bVisitorValue writes the Long from getSingleValue. Java casts to DecimalValue;
// a missing or non-Decimal value is a NullPointer/ClassCast 500 unless nullAsZero.
func (s *Server) bVisitorValue(w http.ResponseWriter, r *http.Request, cmd string, param *protocol.MapPack, nullAsZero bool) {
	v, err := s.bSingleValue(r.Context(), cmd, param)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	if v == nil && nullAsZero {
		s.writeJSON(w, r, int64(0))
		return
	}
	if v == nil || v.Type != protocol.ValueDecimal {
		s.writeErr(w, r, bInternal("null"))
		return
	}
	s.writeJSON(w, r, v.Int)
}

// GET /v1/visitor/realTime/ofObject/{objHash}
func (s *Server) bVisitorRealtimeObj(w http.ResponseWriter, r *http.Request) {
	h, err := bPathInt32(r, "objHash")
	if err == nil {
		err = bServerID(r)
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.bVisitorValue(w, r, "VISITOR_REALTIME", bMap(bEntry("objHash", bDec(int64(h)))), false)
}

// GET /v1/visitor/realTime/ofType/{objType}
func (s *Server) bVisitorRealtimeType(w http.ResponseWriter, r *http.Request) {
	if err := bServerID(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.bVisitorValue(w, r, "VISITOR_REALTIME_TOTAL", bMap(bEntry("objType", bTextV(r.PathValue("objType")))), false)
}

// GET /v1/visitor/realTime?objHashes=
func (s *Server) bVisitorRealtimeGroup(w http.ResponseWriter, r *http.Request) {
	if err := bServerID(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	raw, _ := bQueryRaw(r, "objHashes")
	hashes, err := bSplitInt32("objHashes", raw)
	if err == nil && len(hashes) == 0 {
		err = validateError("Query parameter 'objHashes' is required!")
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.bVisitorValue(w, r, "VISITOR_REALTIME_GROUP", bMap(bEntry("objHash", bDecList(hashes))), true)
}

// GET /v1/visitor/{yyyymmdd}/ofObject/{objHash}
func (s *Server) bVisitorDateObj(w http.ResponseWriter, r *http.Request) {
	h, err := bPathInt32(r, "objHash")
	if err == nil {
		err = bServerID(r)
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.bVisitorValue(w, r, "VISITOR_LOADDATE", bMap(bEntry("objHash", bDec(int64(h))), bEntry("date", bTextV(r.PathValue("yyyymmdd")))), false)
}

// GET /v1/visitor/{yyyymmdd}/ofType/{objType}
func (s *Server) bVisitorDateType(w http.ResponseWriter, r *http.Request) {
	if err := bServerID(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.bVisitorValue(w, r, "VISITOR_LOADDATE_TOTAL", bMap(bEntry("objType", bTextV(r.PathValue("objType"))), bEntry("date", bTextV(r.PathValue("yyyymmdd")))), false)
}

type bVisitorGroupReq struct {
	startYmd, endYmd   *string
	startYmdH, endYmdH int64
	objHashes          []int32
}

// bParseVisitorGroup is VisitorGroupRequest.validate() followed by the
// consumer's objHashes check.
func (s *Server) bParseVisitorGroup(r *http.Request) (bVisitorGroupReq, error) {
	var q bVisitorGroupReq
	var err error
	if err = bServerID(r); err != nil {
		return q, err
	}
	if q.startYmdH, err = bQueryInt64(r, "startYmdH"); err != nil {
		return q, err
	}
	if q.endYmdH, err = bQueryInt64(r, "endYmdH"); err != nil {
		return q, err
	}
	q.startYmd, q.endYmd = bQueryString(r, "startYmd"), bQueryString(r, "endYmd")
	if !bBlank(q.startYmd) || !bBlank(q.endYmd) {
		if bBlank(q.startYmd) || bBlank(q.endYmd) {
			return q, validateError("startYmd and endYmd should be not null !")
		}
		if q.startYmdH > 0 || q.endYmdH > 0 {
			return q, validateError("startYmd, endYmd and startYmdH, endYmdH must not coexist!")
		}
		var ok1, ok2 bool
		q.startYmdH, ok1 = bParseStrictLocal(*q.startYmd, false, s.location())
		q.endYmdH, ok2 = bParseStrictLocal(*q.endYmd, false, s.location())
		if !ok1 || !ok2 {
			return q, validateError("startYmd/endYmd must be yyyyMMddHH")
		}
	} else if q.startYmdH <= 0 || q.endYmdH <= 0 {
		return q, validateError("startYmdH and endYmdH must have value!")
	}
	if q.objHashes, err = bSplitInt32("objHashes", r.PathValue("objHashes")); err != nil {
		return q, err
	}
	if len(q.objHashes) == 0 {
		return q, validateError("Query parameter 'objHashes' is required!")
	}
	return q, nil
}

// GET /v1/visitor/ofObject/{objHashes}
//
// Reproduces the original request: sDate/eDate carry the raw startYmd/endYmd
// text (W6). The collector handler reads startDate/endDate, so this endpoint
// fails on the original server as well; the result field mapping
// (getText("time"), getLong("value")) is kept as is.
func (s *Server) bVisitorGroup(w http.ResponseWriter, r *http.Request) {
	q, err := s.bParseVisitorGroup(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	param := bMap(bEntry("objHash", bDecList(q.objHashes)), bEntry("sDate", bOptText(q.startYmd)), bEntry("eDate", bOptText(q.endYmd)))
	pack, err := s.bSingle(r.Context(), "VISITOR_LOADDATE_GROUP", param)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	mp, ok := pack.(*protocol.MapPack)
	if !ok {
		s.writeErr(w, r, bInternal("null"))
		return
	}
	s.writeJSON(w, r, bVisitorGroupOf(mp))
}

// GET /v1/visitor/hourly/ofObject/{objHashes}
//
// Original mapping kept: the collector returns MapPacks of time/value
// ListValues, which VisitorGroup.of reads as getText/getLong (null / 0) (W15).
func (s *Server) bVisitorHourly(w http.ResponseWriter, r *http.Request) {
	q, err := s.bParseVisitorGroup(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	param := bMap(bEntry("objHash", bDecList(q.objHashes)), bEntry("stime", bDec(q.startYmdH)), bEntry("etime", bDec(q.endYmdH)))
	items, err := s.bCollect(r.Context(), "VISITOR_LOADHOUR_GROUP", param)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	out := make([]bVisitorGroup, 0, len(items))
	for _, it := range items {
		mp, ok := it.Pack.(*protocol.MapPack)
		if !ok {
			s.writeErr(w, r, bInternal("unexpected VISITOR_LOADHOUR_GROUP response"))
			return
		}
		out = append(out, bVisitorGroupOf(mp))
	}
	s.writeJSON(w, r, out)
}
