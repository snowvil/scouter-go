package webapp

import "net/http"

// registerB registers every B route. Filters (CORS, no-cache, requestId,
// authentication with the "/scouter/s/" NoAuth prefix) are applied by A's
// Handler in front of the mux.
func (s *Server) registerB(mux *http.ServeMux) {
	const p = "/scouter"
	h := func(pattern string, fn http.HandlerFunc) { mux.HandleFunc(pattern, fn) }
	// XLog: "/realTime/{l}/{i}" and "/{yyyymmdd}/gxid/{gxid}" overlap -> dispatcher.
	h("GET "+p+"/v1/xlog/{yyyymmdd}", s.bXLogPageable)
	h("GET "+p+"/v1/xlog/{yyyymmdd}/{txid}", s.bXLogSingle)
	h("GET "+p+"/v1/xlog/search/{yyyymmdd}", s.bXLogSearch)
	h("GET "+p+"/v1/xlog/{p1}/{p2}/{p3}", s.bXLogThree)

	h("GET "+p+"/v1/xlog-data/{yyyymmdd}", s.bXLogDataPageable)
	h("GET "+p+"/v1/xlog-data/{yyyymmdd}/{txid}", s.bXLogDataSingle)
	h("GET "+p+"/v1/xlog-data/search/{yyyymmdd}", s.bXLogDataSearch)
	h("GET "+p+"/v1/xlog-data/{p1}/{p2}/{p3}", s.bXLogDataThree)

	h("GET "+p+"/v1/profile/{yyyymmdd}/{txid}", s.bProfile)
	h("GET "+p+"/v1/profile-data/{yyyymmdd}/{txid}", s.bProfileData)

	for _, k := range bSummaryKinds {
		h("GET "+p+"/v1/summary/"+k.path+"/ofType/{objType}", s.bSummaryHandler(k, true))
		h("GET "+p+"/v1/summary/"+k.path+"/ofObject/{objHash}", s.bSummaryHandler(k, false))
	}

	h("GET "+p+"/v1/visitor/realTime/ofObject/{objHash}", s.bVisitorRealtimeObj)
	h("GET "+p+"/v1/visitor/realTime/ofType/{objType}", s.bVisitorRealtimeType)
	h("GET "+p+"/v1/visitor/realTime", s.bVisitorRealtimeGroup)
	h("GET "+p+"/v1/visitor/{yyyymmdd}/ofObject/{objHash}", s.bVisitorDateObj)
	h("GET "+p+"/v1/visitor/{yyyymmdd}/ofType/{objType}", s.bVisitorDateType)
	h("GET "+p+"/v1/visitor/ofObject/{objHashes}", s.bVisitorGroup)
	h("GET "+p+"/v1/visitor/hourly/ofObject/{objHashes}", s.bVisitorHourly)

	h("GET "+p+"/v1/dictionary/{yyyymmdd}", s.bDictionary)

	global := bKvStore{global: true}
	custom := bKvStore{space: func(r *http.Request) string { return r.PathValue("keySpace") }}
	private := bKvStore{private: true}

	h("GET "+p+"/v1/kv/{key}", s.bGlobalReserved(s.bKvGetHandler(global), "key"))
	h("PUT "+p+"/v1/kv", s.bKvSetHandler(global))
	h("PUT "+p+"/v1/kv/{a}/{b}", s.bKvPutTwo(s.bKvSetHandler(custom), s.bKvTTLHandler(global)))
	h("GET "+p+"/v1/kv/{keys}/:bulk", s.bGlobalReserved(s.bKvBulkGetHandler(global), "keys"))
	h("PUT "+p+"/v1/kv/:bulk", s.bKvBulkSetHandler(global))

	h("GET "+p+"/v1/kv/space/{keySpace}/{key}", s.bKvGetHandler(custom))
	h("PUT "+p+"/v1/kv/space/{keySpace}/{key}/:ttl", s.bKvTTLHandler(custom))
	h("GET "+p+"/v1/kv/space/{keySpace}/{keys}/:bulk", s.bKvBulkGetHandler(custom))
	h("PUT "+p+"/v1/kv/space/{keySpace}/:bulk", s.bKvBulkSetHandler(custom))

	h("GET "+p+"/v1/kv-private/{key}", s.bKvGetHandler(private))
	h("PUT "+p+"/v1/kv-private", s.bKvSetHandler(private))
	h("PUT "+p+"/v1/kv-private/{key}/:ttl", s.bKvTTLHandler(private))
	h("GET "+p+"/v1/kv-private/{keys}/:bulk", s.bKvBulkGetHandler(private))
	h("PUT "+p+"/v1/kv-private/:bulk", s.bKvBulkSetHandler(private))

	h("GET "+p+"/v1/shortener/{key}", s.bShortenGet)
	h("POST "+p+"/v1/shortener", s.bShortenMake)
	h("GET "+p+"/s/{key}", s.bSimpleView)

	h("GET "+p+"/v1/interactionCounter/realTime/ofType/{objType}", s.bInteractionByType)
	h("GET "+p+"/v1/interactionCounter/realTime", s.bInteractionByHashes)
}
