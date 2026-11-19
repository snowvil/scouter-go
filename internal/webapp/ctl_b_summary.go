package webapp

import (
	"context"
	"net/http"
	"strconv"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// SummaryController (/v1/summary): 7 kinds x {ofType/{objType}, ofObject/{objHash}}.

type bSummaryKind struct {
	path string
	cmd  string
	// build converts one day's MapPack into items (in list order).
	build func(s *Server, ctx context.Context, date string, p *protocol.MapPack) []*bSummaryItem
}

// bSummaryItem holds every SummaryItem subclass field; fields() picks the
// subclass's Jackson property order (SummaryItem fields first).
type bSummaryItem struct {
	kind                                         string
	key                                          int32
	keyName                                      string
	count                                        int32
	errorCount                                   int32
	elapsedSum, cpuSum, memorySum                int64
	errText, service, errorMessage, sql, apiCall string
	fullStack                                    string
	txid                                         int64
	level                                        string
}

func (it *bSummaryItem) merge(n *bSummaryItem) {
	it.count += n.count
	switch it.kind {
	case "service":
		it.errorCount += n.errorCount
		it.elapsedSum += n.elapsedSum
		it.cpuSum += n.cpuSum
		it.memorySum += n.memorySum
	case "sql", "apiCall":
		it.errorCount += n.errorCount
		it.elapsedSum += n.elapsedSum
	case "error":
		// ErrorSummaryItem.merge overwrites every detail with the newer day.
		it.errText, it.service, it.errorMessage = n.errText, n.service, n.errorMessage
		it.txid, it.sql, it.apiCall, it.fullStack = n.txid, n.sql, n.apiCall, n.fullStack
	}
}

func (it *bSummaryItem) json() jsonObject {
	o := jsonObject{
		{Key: "summaryKey", Value: it.key},
		{Key: "summaryKeyName", Value: it.keyName},
		{Key: "count", Value: it.count},
	}
	switch it.kind {
	case "service":
		o = append(o, jsonField{Key: "errorCount", Value: it.errorCount}, jsonField{Key: "elapsedSum", Value: it.elapsedSum},
			jsonField{Key: "cpuSum", Value: it.cpuSum}, jsonField{Key: "memorySum", Value: it.memorySum})
	case "sql", "apiCall":
		o = append(o, jsonField{Key: "errorCount", Value: it.errorCount}, jsonField{Key: "elapsedSum", Value: it.elapsedSum})
	case "error":
		o = append(o, jsonField{Key: "error", Value: it.errText}, jsonField{Key: "service", Value: it.service},
			jsonField{Key: "errorMessage", Value: it.errorMessage}, jsonField{Key: "txid", Value: it.txid},
			jsonField{Key: "sql", Value: it.sql}, jsonField{Key: "apiCall", Value: it.apiCall},
			jsonField{Key: "fullStack", Value: it.fullStack})
	case "alert":
		o = append(o, jsonField{Key: "level", Value: it.level})
	}
	return o
}

// bAlertLevel is AlertLevelEnum.of(int).name() (unknown -> INFO).
func bAlertLevel(n int32) string {
	switch n {
	case 1:
		return "WARN"
	case 2:
		return "ERROR"
	case 3:
		return "FATAL"
	}
	return "INFO"
}

func bSummaryLists(p *protocol.MapPack, keys ...string) [][]protocol.Value {
	out := make([][]protocol.Value, len(keys))
	for i, k := range keys {
		out[i] = bMapList(p, k)
	}
	return out
}

var bSummaryKinds = []bSummaryKind{
	{"service", "LOAD_SERVICE_SUMMARY", func(s *Server, ctx context.Context, date string, p *protocol.MapPack) []*bSummaryItem {
		l := bSummaryLists(p, "id", "count", "error", "elapsed", "cpu", "mem")
		items := make([]*bSummaryItem, len(l[0]))
		for i := range l[0] {
			id := bInt(l[0][i])
			items[i] = &bSummaryItem{kind: "service", key: id, keyName: s.text(ctx, date, "service", id),
				count: bInt(bAt(l[1], i)), errorCount: bInt(bAt(l[2], i)), elapsedSum: bLong(bAt(l[3], i)),
				cpuSum: bLong(bAt(l[4], i)), memorySum: bLong(bAt(l[5], i))}
		}
		return items
	}},
	{"sql", "LOAD_SQL_SUMMARY", func(s *Server, ctx context.Context, date string, p *protocol.MapPack) []*bSummaryItem {
		return bSummaryCall(s, ctx, date, p, "sql", "sql")
	}},
	{"apiCall", "LOAD_APICALL_SUMMARY", func(s *Server, ctx context.Context, date string, p *protocol.MapPack) []*bSummaryItem {
		return bSummaryCall(s, ctx, date, p, "apiCall", "apicall")
	}},
	{"ip", "LOAD_IP_SUMMARY", func(s *Server, ctx context.Context, date string, p *protocol.MapPack) []*bSummaryItem {
		l := bSummaryLists(p, "id", "count")
		items := make([]*bSummaryItem, len(l[0]))
		for i := range l[0] {
			id := bInt(l[0][i])
			items[i] = &bSummaryItem{kind: "ip", key: id, keyName: bIPInt(id), count: bInt(bAt(l[1], i))}
		}
		return items
	}},
	{"userAgent", "LOAD_UA_SUMMARY", func(s *Server, ctx context.Context, date string, p *protocol.MapPack) []*bSummaryItem {
		l := bSummaryLists(p, "id", "count")
		items := make([]*bSummaryItem, len(l[0]))
		for i := range l[0] {
			id := bInt(l[0][i])
			items[i] = &bSummaryItem{kind: "userAgent", key: id, keyName: s.text(ctx, date, "ua", id), count: bInt(bAt(l[1], i))}
		}
		return items
	}},
	{"error", "LOAD_SERVICE_ERROR_SUMMARY", func(s *Server, ctx context.Context, date string, p *protocol.MapPack) []*bSummaryItem {
		l := bSummaryLists(p, "id", "count", "error", "service", "message", "txid", "sql", "apicall", "fullstack")
		items := make([]*bSummaryItem, len(l[0]))
		for i := range l[0] {
			errText := s.text(ctx, date, "error", bInt(bAt(l[2], i)))
			service := s.text(ctx, date, "service", bInt(bAt(l[3], i)))
			items[i] = &bSummaryItem{kind: "error", key: bInt(l[0][i]), keyName: errText + " [:of:] " + service,
				count: bInt(bAt(l[1], i)), errText: errText, service: service,
				errorMessage: s.text(ctx, date, "error", bInt(bAt(l[4], i))), txid: bLong(bAt(l[5], i)),
				sql: s.text(ctx, date, "sql", bInt(bAt(l[6], i))), apiCall: s.text(ctx, date, "apicall", bInt(bAt(l[7], i))),
				fullStack: s.text(ctx, date, "error", bInt(bAt(l[8], i)))}
		}
		return items
	}},
	{"alert", "LOAD_ALERT_SUMMARY", func(s *Server, ctx context.Context, date string, p *protocol.MapPack) []*bSummaryItem {
		l := bSummaryLists(p, "id", "title", "count", "level")
		items := make([]*bSummaryItem, len(l[0]))
		for i := range l[0] {
			title := ""
			if t := bListString(l[1], i); t != nil {
				title = *t
			}
			items[i] = &bSummaryItem{kind: "alert", key: bInt(l[0][i]), keyName: title,
				count: bInt(bAt(l[2], i)), level: bAlertLevel(bInt(bAt(l[3], i)))}
		}
		return items
	}},
}

// bSummaryTextKeys lists the dictionary hashes a summary MapPack references.
func bSummaryTextKeys(kind string, p *protocol.MapPack) []textKey {
	var keys []textKey
	add := func(list, typ string) {
		for _, v := range bMapList(p, list) {
			keys = append(keys, textKey{Type: typ, Hash: bInt(v)})
		}
	}
	switch kind {
	case "service":
		add("id", "service")
	case "sql":
		add("id", "sql")
	case "apiCall":
		add("id", "apicall")
	case "userAgent":
		add("id", "ua")
	case "error":
		add("error", "error")
		add("service", "service")
		add("message", "error")
		add("sql", "sql")
		add("apicall", "apicall")
		add("fullstack", "error")
	}
	return keys
}

func bSummaryCall(s *Server, ctx context.Context, date string, p *protocol.MapPack, kind, textType string) []*bSummaryItem {
	l := bSummaryLists(p, "id", "count", "error", "elapsed")
	items := make([]*bSummaryItem, len(l[0]))
	for i := range l[0] {
		id := bInt(l[0][i])
		items[i] = &bSummaryItem{kind: kind, key: id, keyName: s.text(ctx, date, textType, id),
			count: bInt(bAt(l[1], i)), errorCount: bInt(bAt(l[2], i)), elapsedSum: bLong(bAt(l[3], i))}
	}
	return items
}

// bSummaryRange is SummaryRequest.validate().
func (s *Server) bSummaryRange(r *http.Request) (int64, int64, error) {
	if err := bServerID(r); err != nil {
		return 0, 0, err
	}
	start, err := bQueryInt64(r, "startTimeMillis")
	if err != nil {
		return 0, 0, err
	}
	end, err := bQueryInt64(r, "endTimeMillis")
	if err != nil {
		return 0, 0, err
	}
	sYmdHm, eYmdHm := bQueryString(r, "startYmdHm"), bQueryString(r, "endYmdHm")
	if !bBlank(sYmdHm) || !bBlank(eYmdHm) {
		if bBlank(sYmdHm) || bBlank(eYmdHm) {
			return 0, 0, validateError("startYmdHms and endYmdHms should be not null !")
		}
		if start > 0 || end > 0 {
			return 0, 0, validateError("startYmdHms, endYmdHms and startTimeMillis, endTimeMills must not coexist!")
		}
		var ok1, ok2 bool
		start, ok1 = bParseStrictLocal(*sYmdHm, true, s.location())
		end, ok2 = bParseStrictLocal(*eYmdHm, true, s.location())
		if !ok1 || !ok2 {
			return 0, 0, validateError("startYmdHm/endYmdHm must be yyyyMMddHHmm")
		}
		return start, end, nil
	}
	if start <= 0 || end <= 0 {
		return 0, 0, validateError("startTimeMillis and endTimeMillis must have value!")
	}
	return start, end, nil
}

func (s *Server) bSummaryHandler(kind bSummaryKind, byType bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var objType *string
		var objHash int32
		if byType {
			t := r.PathValue("objType")
			objType = &t
		} else {
			var err error
			if objHash, err = bPathInt32(r, "objHash"); err != nil {
				s.writeErr(w, r, err)
				return
			}
		}
		start, end, err := s.bSummaryRange(r)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		dt := bDateTime{loc: s.location()}
		su, eu := dt.unit(start), dt.unit(end)
		if eu-su >= bMaxDays {
			s.writeErr(w, r, validateError("summary range must not exceed 366 days"))
			return
		}
		var order []int32
		items := map[int32]*bSummaryItem{}
		for u := su; u <= eu; u++ {
			// SummaryConsumer.generateSearchCondition
			st := start
			if u != su {
				st = dt.unitMillis(u)
			}
			et := end
			if u < eu {
				et = dt.unitMillis(u) + 86_400_000 - 1000
			}
			date := dt.ymd(st)
			param := bMap(bEntry("date", bTextV(date)), bEntry("stime", bDec(st)), bEntry("etime", bDec(et)))
			if objType != nil {
				param.Entries = append(param.Entries, bEntry("objType", bTextV(*objType)))
			}
			param.Entries = append(param.Entries, bEntry("objHash", bDec(int64(objHash))))
			pack, err := s.bSingle(ctx, kind.cmd, param)
			if err != nil {
				s.writeErr(w, r, err)
				return
			}
			mp, ok := pack.(*protocol.MapPack)
			if !ok {
				// Java: null/foreign pack -> NullPointerException/ClassCastException -> 500.
				s.writeErr(w, r, bInternal("null"))
				return
			}
			// Batch the dictionary hashes first (Java loads them one by one per day).
			_ = s.bPreload(ctx, bSummaryTextKeys(kind.path, mp))
			for _, it := range kind.build(s, ctx, date, mp) {
				if old, ok := items[it.key]; ok {
					old.merge(it)
					continue
				}
				items[it.key] = it
				order = append(order, it.key)
			}
		}
		// Summary.itemMap is a HashMap<Integer, T>: emulate its key order.
		itemMap := make(jsonObject, 0, len(order))
		for _, at := range bJavaHashOrder(order) {
			k := order[at]
			itemMap = append(itemMap, jsonField{Key: strconv.Itoa(int(k)), Value: items[k].json()})
		}
		s.writeJSON(w, r, jsonObject{{Key: "itemMap", Value: itemMap}})
	}
}
