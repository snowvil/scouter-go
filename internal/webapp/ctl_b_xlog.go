package webapp

import (
	"context"
	"net/http"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// XLogController (/v1/xlog) and XLogDataController (/v1/xlog-data).

const bRealtimeXLogCount = 10_000 // XLogConsumer firstRetrieveLimit

// bXLogTimeRange is the shared validate() of PageableXLogRequest/SearchXLogRequest.
func (s *Server) bXLogTimeRange(date string, start, end int64, startHms, endHms *string) (int64, int64, error) {
	if !bBlank(startHms) || !bBlank(endHms) {
		if bBlank(startHms) || bBlank(endHms) {
			return 0, 0, validateError("startHms and endHms should be not null !")
		}
		if start > 0 || end > 0 {
			return 0, 0, validateError("startYmdHms, endYmdHms and startTimeMillis, endTimeMills must not coexist!")
		}
		st, ok1 := bParseLenientYmdHms(date+*startHms, s.location())
		et, ok2 := bParseLenientYmdHms(date+*endHms, s.location())
		if !ok1 || !ok2 {
			return 0, 0, validateError("date is invalid!")
		}
		return st, et, nil
	}
	if start <= 0 || end <= 0 {
		return 0, 0, validateError("startTimeMillis and endTimeMillis must have value!")
	}
	return start, end, nil
}

type bPageReq struct {
	date               string
	start, end         int64
	objHashes          []int32
	pageCount          int32
	lastTxid, lastTime int64
}

// bParsePageable mirrors PageableXLogRequest: Jersey conversion (404), bean
// validation (@NotNull objHashes, @Max pageCount: 400) then validate().
func (s *Server) bParsePageable(r *http.Request) (bPageReq, error) {
	var q bPageReq
	var err error
	q.date = r.PathValue("yyyymmdd")
	if err = bServerID(r); err != nil {
		return q, err
	}
	if q.start, err = bQueryInt64(r, "startTimeMillis"); err != nil {
		return q, err
	}
	if q.end, err = bQueryInt64(r, "endTimeMillis"); err != nil {
		return q, err
	}
	if q.pageCount, err = bQueryInt32(r, "pageCount"); err != nil {
		return q, err
	}
	if q.lastTxid, err = bQueryInt64(r, "lastTxid"); err != nil {
		return q, err
	}
	if q.lastTime, err = bQueryInt64(r, "lastXLogTime"); err != nil {
		return q, err
	}
	if q.pageCount <= 0 {
		q.pageCount = bDefaultPageCount
	}
	raw, err := bRequiredQuery(r, "objHashes")
	if err != nil {
		return q, err
	}
	if q.pageCount > bMaxPageCount {
		return q, bConstraint("pageCount must be less than or equal to 30000")
	}
	if q.objHashes, err = bSplitInt32Set("objHashes", raw); err != nil {
		return q, err
	}
	if (q.lastTxid != 0) != (q.lastTime != 0) {
		return q, validateError("lastTxid and lastXlogTime must coexist!")
	}
	q.start, q.end, err = s.bXLogTimeRange(q.date, q.start, q.end, bQueryString(r, "startHms"), bQueryString(r, "endHms"))
	return q, err
}

func (q bPageReq) pack() *protocol.MapPack {
	return bMap(
		bEntry("date", bTextV(q.date)),
		bEntry("stime", bDec(q.start)),
		bEntry("txid", bDec(q.lastTxid)),
		bEntry("etime", bDec(q.end)),
		bEntry("lastBucketTime", bDec(q.lastTime)),
		bEntry("pageCount", bDec(int64(q.pageCount))),
		bEntry("objHash", bDecList(q.objHashes)),
	)
}

// bParseSearch mirrors SearchXLogRequest and builds the SEARCH_XLOG_LIST param.
func (s *Server) bParseSearch(r *http.Request) (*protocol.MapPack, error) {
	date := r.PathValue("yyyymmdd")
	if err := bServerID(r); err != nil {
		return nil, err
	}
	start, err := bQueryInt64(r, "startTimeMillis")
	if err != nil {
		return nil, err
	}
	end, err := bQueryInt64(r, "endTimeMillis")
	if err != nil {
		return nil, err
	}
	objHash, err := bQueryInt64(r, "objHash")
	if err != nil {
		return nil, err
	}
	if start, end, err = s.bXLogTimeRange(date, start, end, bQueryString(r, "startHms"), bQueryString(r, "endHms")); err != nil {
		return nil, err
	}
	p := bMap(bEntry("date", bTextV(date)), bEntry("stime", bDec(start)), bEntry("etime", bDec(end)))
	add := func(key, query string) {
		if v := bQueryString(r, query); v != nil {
			p.Entries = append(p.Entries, bEntry(key, bTextV(*v)))
		}
	}
	add("service", "service")
	if objHash != 0 {
		p.Entries = append(p.Entries, bEntry("objHash", bDec(objHash)))
	}
	for _, k := range []string{"ip", "login", "desc", "text1", "text2", "text3", "text4", "text5"} {
		add(k, k)
	}
	return p, nil
}

// bParseRealtime parses {loop}/{index} path params and the required objHashes.
func bParseRealtime(r *http.Request, loopName, indexName string, checkMin bool) (int64, int32, []int32, error) {
	if err := bServerID(r); err != nil {
		return 0, 0, nil, err
	}
	loop, err := bPathInt64(r, loopName)
	if err != nil {
		return 0, 0, nil, err
	}
	index, err := bPathInt32(r, indexName)
	if err != nil {
		return 0, 0, nil, err
	}
	raw, err := bRequiredQuery(r, "objHashes")
	if err != nil {
		return 0, 0, nil, err
	}
	if checkMin && loop < 0 {
		return 0, 0, nil, bConstraint("xLogLoop must be greater than or equal to 0")
	}
	if checkMin && index < 0 {
		return 0, 0, nil, bConstraint("xLogIndex must be greater than or equal to 0")
	}
	hashes, err := bSplitInt32Set("objHashes", raw)
	return loop, index, hashes, err
}

// bRealtimeXLogs issues TRANX_REAL_TIME_GROUP(_LATEST) and returns the meta
// (loop,index) MapPack and the XLogPacks in arrival order.
func (s *Server) bRealtimeXLogs(ctx context.Context, loop int64, index int32, hashes []int32) (*protocol.MapPack, []*protocol.XLogPack, error) {
	cmd := "TRANX_REAL_TIME_GROUP"
	if loop == 0 && index == 0 {
		cmd = "TRANX_REAL_TIME_GROUP_LATEST"
	}
	param := bMap(
		bEntry("index", bDec(int64(index))),
		bEntry("loop", bDec(loop)),
		bEntry("count", bDec(bRealtimeXLogCount)),
		bEntry("objHash", bDecList(hashes)),
	)
	var meta *protocol.MapPack
	var xlogs []*protocol.XLogPack
	err := s.call(ctx, cmd, param, func(it Item) error {
		switch p := it.Pack.(type) {
		case *protocol.MapPack:
			meta = p
		case *protocol.XLogPack:
			xlogs = append(xlogs, p)
		default:
			return bInternal("unexpected realtime xlog response")
		}
		return nil
	})
	if err == nil && meta == nil {
		err = bInternal("realtime xlog meta pack is missing")
	}
	return meta, xlogs, err
}

// bPageableXLogs issues TRANX_LOAD_TIME_GROUP_V2: XLogPacks then the meta MapPack.
func (s *Server) bPageableXLogs(ctx context.Context, q bPageReq) ([]*protocol.XLogPack, *protocol.MapPack, error) {
	var meta *protocol.MapPack
	var xlogs []*protocol.XLogPack
	err := s.call(ctx, "TRANX_LOAD_TIME_GROUP_V2", q.pack(), func(it Item) error {
		switch p := it.Pack.(type) {
		case *protocol.MapPack:
			meta = p
		case *protocol.XLogPack:
			xlogs = append(xlogs, p)
		default:
			return bInternal("unexpected pageable xlog response")
		}
		return nil
	})
	if err == nil && meta == nil {
		err = bInternal("pageable xlog meta pack is missing")
	}
	return xlogs, meta, err
}

// bXLogPacks collects a stream that must contain only XLogPacks (Java casts each).
func (s *Server) bXLogPacks(ctx context.Context, cmd string, param protocol.Pack) ([]*protocol.XLogPack, error) {
	var out []*protocol.XLogPack
	err := s.call(ctx, cmd, param, func(it Item) error {
		p, ok := it.Pack.(*protocol.XLogPack)
		if !ok {
			return bInternal("unexpected " + cmd + " response")
		}
		out = append(out, p)
		return nil
	})
	return out, err
}

func (s *Server) bSingleXLog(ctx context.Context, r *http.Request, idName, msg string) (*protocol.XLogPack, error) {
	if err := bServerID(r); err != nil {
		return nil, err
	}
	id, err := bPathInt64(r, idName)
	if err != nil {
		return nil, err
	}
	date := r.PathValue("yyyymmdd")
	if id == 0 || date == "" {
		return nil, validateError(msg)
	}
	p, err := s.bSingle(ctx, "XLOG_READ_BY_TXID", bMap(bEntry("date", bTextV(date)), bEntry("txid", bDec(id))))
	if err != nil || p == nil {
		return nil, err
	}
	x, ok := p.(*protocol.XLogPack)
	if !ok {
		return nil, bInternal("unexpected XLOG_READ_BY_TXID response")
	}
	return x, nil
}

func (s *Server) bGxidXLogs(ctx context.Context, r *http.Request) ([]*protocol.XLogPack, error) {
	if err := bServerID(r); err != nil {
		return nil, err
	}
	gxid, err := bPathInt64(r, "gxid")
	if err != nil {
		return nil, err
	}
	date := r.PathValue("yyyymmdd")
	if gxid == 0 || date == "" {
		return nil, validateError("gxid and yyyymmdd must coexist!")
	}
	return s.bXLogPacks(ctx, "XLOG_READ_BY_GXID", bMap(bEntry("date", bTextV(date)), bEntry("gxid", bDec(gxid))))
}

// ---- /v1/xlog ----

// GET /v1/xlog/realTime/{xlogLoop}/{xlogIndex}
func (s *Server) bXLogRealtime(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	loop, index, hashes, err := bParseRealtime(r, "xlogLoop", "xlogIndex", true)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	meta, xlogs, err := s.bRealtimeXLogs(ctx, loop, index, hashes)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	list := make([]bSXLog, len(xlogs))
	for i, p := range xlogs {
		list[i] = bToSXLog(p)
	}
	// Java writes metaPack.getInt(loop/index): loop is narrowed to int.
	s.bWriteStream(w, r, jsonObject{
		{Key: "xlogLoop", Value: bMapInt(meta, "loop")},
		{Key: "xlogIndex", Value: bMapInt(meta, "index")},
		{Key: "xlogs", Value: list},
		{Key: "count", Value: len(list)},
	})
}

// GET /v1/xlog/{yyyymmdd}
func (s *Server) bXLogPageable(w http.ResponseWriter, r *http.Request) {
	q, err := s.bParsePageable(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	xlogs, meta, err := s.bPageableXLogs(r.Context(), q)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	list := make([]bSXLog, len(xlogs))
	for i, p := range xlogs {
		list[i] = bToSXLog(p)
	}
	s.bWriteStream(w, r, bPageResult(list, meta, len(list)))
}

func bPageResult(list any, meta *protocol.MapPack, count int) jsonObject {
	return jsonObject{
		{Key: "xlogs", Value: list},
		{Key: "hasMore", Value: bMapBool(meta, "hasMore")},
		{Key: "lastTxid", Value: bMapLong(meta, "lastTxid")},
		{Key: "lastXLogTime", Value: bMapLong(meta, "lastXLogTime")},
		{Key: "count", Value: count},
	}
}

// GET /v1/xlog/{yyyymmdd}/{txid}
func (s *Server) bXLogSingle(w http.ResponseWriter, r *http.Request) {
	p, err := s.bSingleXLog(r.Context(), r, "txid", "txid and yyyymmdd must coexist!")
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	if p == nil {
		s.writeJSON(w, r, nil)
		return
	}
	s.writeJSON(w, r, bToSXLog(p))
}

// GET /v1/xlog/{yyyymmdd}/gxid/{gxid}
func (s *Server) bXLogGxid(w http.ResponseWriter, r *http.Request) {
	xlogs, err := s.bGxidXLogs(r.Context(), r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	list := make([]bSXLog, len(xlogs))
	for i, p := range xlogs {
		list[i] = bToSXLog(p)
	}
	s.writeJSON(w, r, list)
}

// GET /v1/xlog/search/{yyyymmdd}
func (s *Server) bXLogSearch(w http.ResponseWriter, r *http.Request) {
	param, err := s.bParseSearch(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	xlogs, err := s.bXLogPacks(r.Context(), "SEARCH_XLOG_LIST", param)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	list := make([]bSXLog, len(xlogs))
	for i, p := range xlogs {
		list[i] = bToSXLog(p)
	}
	s.writeJSON(w, r, list)
}

// bXLogThree dispatches GET /v1/xlog/{a}/{b}/{c}: Jersey prefers the template
// with more literal characters, so "realTime/{loop}/{index}" wins over
// "{yyyymmdd}/gxid/{gxid}" (Go 1.22 patterns would conflict here).
func (s *Server) bXLogThree(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.PathValue("p1") == "realTime":
		r.SetPathValue("xlogLoop", r.PathValue("p2"))
		r.SetPathValue("xlogIndex", r.PathValue("p3"))
		s.bXLogRealtime(w, r)
	case r.PathValue("p2") == "gxid":
		r.SetPathValue("yyyymmdd", r.PathValue("p1"))
		r.SetPathValue("gxid", r.PathValue("p3"))
		s.bXLogGxid(w, r)
	default:
		s.writeErr(w, r, bNotFound())
	}
}

// ---- /v1/xlog-data ----

// bXLogDataList mirrors XLogData.of for each pack; the TextLoader preload is
// done once for the whole list (after the collector call has finished).
func (s *Server) bXLogDataList(ctx context.Context, xlogs []*protocol.XLogPack) []bXLogData {
	keys := make([]textKey, 0, 11*len(xlogs))
	for _, p := range xlogs {
		keys = append(keys,
			textKey{"service", p.Service}, textKey{"hmsg", p.ThreadNameHash}, textKey{"error", p.Error},
			textKey{"ua", p.UserAgent}, textKey{"referer", p.Referer}, textKey{"group", p.Group},
			textKey{"city", p.City}, textKey{"login", p.Login}, textKey{"desc", p.Desc},
			textKey{"hmsg", p.QueuingHostHash}, textKey{"hmsg", p.Queuing2ndHostHash})
	}
	_ = s.bPreload(ctx, keys)
	list := make([]bXLogData, len(xlogs))
	for i, p := range xlogs {
		list[i] = s.bToXLogData(ctx, p)
	}
	return list
}

// GET /v1/xlog-data/realTime/{offset1}/{offset2}
//
// Intended difference (webapp-api.md W10, §6.5): the Java webapp reads its own
// 40,000-slot XLogLoopCache whose offsets are webapp-local. The embedded Go
// server exposes the collector TRANX_REAL_TIME_GROUP cursor directly:
// offset1 = collector loop, offset2 = collector index, (0,0) = latest 10,000.
// The 2,000 ms dictionary hold-back of the ring is not applied.
func (s *Server) bXLogDataRealtime(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	loop, index, hashes, err := bParseRealtime(r, "offset1", "offset2", false)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	meta, xlogs, err := s.bRealtimeXLogs(ctx, loop, index, hashes)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	list := s.bXLogDataList(ctx, xlogs)
	s.bWriteStream(w, r, jsonObject{
		{Key: "xlogs", Value: list},
		{Key: "offset1", Value: bMapLong(meta, "loop")},
		{Key: "offset2", Value: bMapInt(meta, "index")},
		{Key: "count", Value: len(list)},
	})
}

// GET /v1/xlog-data/{yyyymmdd}
func (s *Server) bXLogDataPageable(w http.ResponseWriter, r *http.Request) {
	q, err := s.bParsePageable(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	xlogs, meta, err := s.bPageableXLogs(r.Context(), q)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	list := s.bXLogDataList(r.Context(), xlogs)
	s.bWriteStream(w, r, bPageResult(list, meta, len(list)))
}

// GET /v1/xlog-data/{yyyymmdd}/{txid}
func (s *Server) bXLogDataSingle(w http.ResponseWriter, r *http.Request) {
	p, err := s.bSingleXLog(r.Context(), r, "txid", "txid and yyyymmdd must coexist!")
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	if p == nil {
		s.writeJSON(w, r, nil)
		return
	}
	s.writeJSON(w, r, s.bXLogDataList(r.Context(), []*protocol.XLogPack{p})[0])
}

// GET /v1/xlog-data/{yyyymmdd}/multi/{txidList}
func (s *Server) bXLogDataMulti(w http.ResponseWriter, r *http.Request) {
	if err := bServerID(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	txids, err := bSplitInt64("txidList", r.PathValue("txidList"))
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	vals := make([]protocol.Value, len(txids))
	for i, t := range txids {
		vals[i] = bDec(t)
	}
	param := bMap(bEntry("date", bTextV(r.PathValue("yyyymmdd"))), bEntry("txid", protocol.Value{Type: protocol.ValueList, Values: vals}))
	xlogs, err := s.bXLogPacks(r.Context(), "XLOG_LOAD_BY_TXIDS", param)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, s.bXLogDataList(r.Context(), xlogs))
}

// GET /v1/xlog-data/{yyyymmdd}/gxid/{gxid}
func (s *Server) bXLogDataGxid(w http.ResponseWriter, r *http.Request) {
	xlogs, err := s.bGxidXLogs(r.Context(), r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, s.bXLogDataList(r.Context(), xlogs))
}

// GET /v1/xlog-data/search/{yyyymmdd}
func (s *Server) bXLogDataSearch(w http.ResponseWriter, r *http.Request) {
	param, err := s.bParseSearch(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	xlogs, err := s.bXLogPacks(r.Context(), "SEARCH_XLOG_LIST", param)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, s.bXLogDataList(r.Context(), xlogs))
}

// bXLogDataThree dispatches GET /v1/xlog-data/{a}/{b}/{c} by Jersey literal
// precedence: realTime (8 chars) > multi (5) > gxid (4).
func (s *Server) bXLogDataThree(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.PathValue("p1") == "realTime":
		r.SetPathValue("offset1", r.PathValue("p2"))
		r.SetPathValue("offset2", r.PathValue("p3"))
		s.bXLogDataRealtime(w, r)
	case r.PathValue("p2") == "multi":
		r.SetPathValue("yyyymmdd", r.PathValue("p1"))
		r.SetPathValue("txidList", r.PathValue("p3"))
		s.bXLogDataMulti(w, r)
	case r.PathValue("p2") == "gxid":
		r.SetPathValue("yyyymmdd", r.PathValue("p1"))
		r.SetPathValue("gxid", r.PathValue("p3"))
		s.bXLogDataGxid(w, r)
	default:
		s.writeErr(w, r, bNotFound())
	}
}
