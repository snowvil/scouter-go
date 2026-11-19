package webapp

import (
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// bRoutes lists the 55 B endpoints (Java method + path, without "/scouter")
// for TestBRoutesRegistered. Several Java templates share one Go pattern
// (dispatchers) because Go 1.22 rejects overlapping patterns that Jersey
// resolves by literal-character precedence.
var bRoutes = []string{
	// XLogController (5)
	"GET /v1/xlog/realTime/{xlogLoop}/{xlogIndex}", "GET /v1/xlog/{yyyymmdd}", "GET /v1/xlog/{yyyymmdd}/{txid}",
	"GET /v1/xlog/{yyyymmdd}/gxid/{gxid}", "GET /v1/xlog/search/{yyyymmdd}",
	// XLogDataController (6)
	"GET /v1/xlog-data/realTime/{offset1}/{offset2}", "GET /v1/xlog-data/{yyyymmdd}", "GET /v1/xlog-data/{yyyymmdd}/{txid}",
	"GET /v1/xlog-data/{yyyymmdd}/multi/{txidList}", "GET /v1/xlog-data/{yyyymmdd}/gxid/{gxid}", "GET /v1/xlog-data/search/{yyyymmdd}",
	// ProfileController, ProfileDataController (2)
	"GET /v1/profile/{yyyymmdd}/{txid}", "GET /v1/profile-data/{yyyymmdd}/{txid}",
	// SummaryController (14)
	"GET /v1/summary/service/ofType/{objType}", "GET /v1/summary/service/ofObject/{objHash}",
	"GET /v1/summary/sql/ofType/{objType}", "GET /v1/summary/sql/ofObject/{objHash}",
	"GET /v1/summary/apiCall/ofType/{objType}", "GET /v1/summary/apiCall/ofObject/{objHash}",
	"GET /v1/summary/ip/ofType/{objType}", "GET /v1/summary/ip/ofObject/{objHash}",
	"GET /v1/summary/userAgent/ofType/{objType}", "GET /v1/summary/userAgent/ofObject/{objHash}",
	"GET /v1/summary/error/ofType/{objType}", "GET /v1/summary/error/ofObject/{objHash}",
	"GET /v1/summary/alert/ofType/{objType}", "GET /v1/summary/alert/ofObject/{objHash}",
	// VisitorController (7)
	"GET /v1/visitor/realTime/ofObject/{objHash}", "GET /v1/visitor/realTime/ofType/{objType}", "GET /v1/visitor/realTime",
	"GET /v1/visitor/{yyyymmdd}/ofObject/{objHash}", "GET /v1/visitor/{yyyymmdd}/ofType/{objType}",
	"GET /v1/visitor/ofObject/{objHashes}", "GET /v1/visitor/hourly/ofObject/{objHashes}",
	// DictionaryController (1)
	"GET /v1/dictionary/{yyyymmdd}",
	// GlobalKvStoreController (5)
	"GET /v1/kv/{key}", "PUT /v1/kv", "PUT /v1/kv/{key}/:ttl", "GET /v1/kv/{keys}/:bulk", "PUT /v1/kv/:bulk",
	// CustomKvStoreController (5)
	"GET /v1/kv/space/{keySpace}/{key}", "PUT /v1/kv/space/{keySpace}", "PUT /v1/kv/space/{keySpace}/{key}/:ttl",
	"GET /v1/kv/space/{keySpace}/{keys}/:bulk", "PUT /v1/kv/space/{keySpace}/:bulk",
	// PrivateKvStoreController (5)
	"GET /v1/kv-private/{key}", "PUT /v1/kv-private", "PUT /v1/kv-private/{key}/:ttl", "GET /v1/kv-private/{keys}/:bulk", "PUT /v1/kv-private/:bulk",
	// ShortenController (2), SimpleViewController (1, @NoAuth)
	"GET /v1/shortener/{key}", "POST /v1/shortener", "GET /s/{key}",
	// InteractionCounterController (2)
	"GET /v1/interactionCounter/realTime/ofType/{objType}", "GET /v1/interactionCounter/realTime",
}

func TestBRoutesRegistered(t *testing.T) {
	if len(bRoutes) != 55 {
		t.Fatalf("%d B routes, want 55", len(bRoutes))
	}
	_, s, _ := bSetup(t, nil)
	mux := http.NewServeMux()
	s.registerB(mux) // panics on a conflicting Go 1.22 pattern
	sample := strings.NewReplacer("{xlogLoop}", "0", "{xlogIndex}", "0", "{offset1}", "0", "{offset2}", "0", "{yyyymmdd}", "20240101",
		"{txid}", "1", "{gxid}", "1", "{txidList}", "1,2", "{objType}", "tomcat", "{objHash}", "1", "{objHashes}", "1",
		"{key}", "k", "{keys}", "k", "{keySpace}", "ns")
	for _, route := range bRoutes {
		method, path, _ := strings.Cut(route, " ")
		req := httptest.NewRequest(method, "/scouter"+sample.Replace(path), nil)
		if _, pattern := mux.Handler(req); pattern == "" {
			t.Errorf("%s is not routed", route)
		}
	}
}

func TestBVisitor(t *testing.T) {
	f, _, hs := bSetup(t, nil)
	for _, c := range []string{"VISITOR_REALTIME", "VISITOR_REALTIME_TOTAL", "VISITOR_LOADDATE", "VISITOR_LOADDATE_TOTAL"} {
		f.handle(c, bodyPack, func(fakeReq) ([]any, error) { return []any{bDec(1234)}, nil })
	}
	f.handle("VISITOR_REALTIME_GROUP", bodyPack, func(fakeReq) ([]any, error) { return nil, nil })
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/visitor/realTime/ofObject/-5", ""), 200, `"1234"`)
	if got := bPackString(bLastReq(t, f, "VISITOR_REALTIME").Pack); got != "objHash=D:-5" {
		t.Fatalf("request %s", got)
	}
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/visitor/realTime/ofType/tomcat", ""), 200, `"1234"`)
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/visitor/20240101/ofObject/7", ""), 200, `"1234"`)
	if got := bPackString(bLastReq(t, f, "VISITOR_LOADDATE").Pack); got != "objHash=D:7,date=T:20240101" {
		t.Fatalf("request %s", got)
	}
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/visitor/20240101/ofType/tomcat", ""), 200, `"1234"`)
	if got := bPackString(bLastReq(t, f, "VISITOR_LOADDATE_TOTAL").Pack); got != "objType=T:tomcat,date=T:20240101" {
		t.Fatalf("request %s", got)
	}
	// Missing group value -> 0 (VisitorConsumer null check).
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/visitor/realTime?objHashes=[3,3,1]", ""), 200, `"0"`)
	if got := bPackString(bLastReq(t, f, "VISITOR_REALTIME_GROUP").Pack); got != "objHash=L[D:3 D:3 D:1]" {
		t.Fatalf("request %s", got)
	}
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/visitor/realTime?objHashes=[]", ""), 400, "fail to validate input parameters. : Query parameter 'objHashes' is required!")
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/visitor/realTime/ofObject/abc", ""), 400, `fail to validate input parameters. : objHash is not an int: "abc"`)
}

func TestBVisitorGroups(t *testing.T) {
	f, _, hs := bSetup(t, nil)
	f.static("VISITOR_LOADDATE_GROUP", bMap(bEntry("time", bTextV("20240101")), bEntry("value", bDec(3))))
	f.static("VISITOR_LOADHOUR_GROUP", bMap(bEntry("time", bDecs(1, 2)), bEntry("value", bDecs(5, 6))))

	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/visitor/ofObject/1,2?startYmd=2024010100&endYmd=2024010223", ""), 200, `{"time":"20240101","value":"3"}`)
	if got := bPackString(bLastReq(t, f, "VISITOR_LOADDATE_GROUP").Pack); got != "objHash=L[D:1 D:2],sDate=T:2024010100,eDate=T:2024010223" {
		t.Fatalf("request %s", got)
	}
	// Original VisitorGroup.of reads time/value ListValues as getText/getLong -> null/0 (W15).
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/visitor/hourly/ofObject/[1]?startYmd=2024010100&endYmd=2024010102", ""), 200, `[{"time":null,"value":"0"}]`)
	if got := bPackString(bLastReq(t, f, "VISITOR_LOADHOUR_GROUP").Pack); got != "objHash=L[D:1],stime=D:1704034800000,etime=D:1704042000000" {
		t.Fatalf("request %s", got)
	}
	bHTTP(t, hs, "GET", "/scouter/v1/visitor/hourly/ofObject/1?startYmdH=10&endYmdH=20", "")
	if got := bPackString(bLastReq(t, f, "VISITOR_LOADHOUR_GROUP").Pack); got != "objHash=L[D:1],stime=D:10,etime=D:20" {
		t.Fatalf("request %s", got)
	}
	p := "/scouter/v1/visitor/ofObject/1"
	bExpectError(t, bHTTP(t, hs, "GET", p, ""), 400, "fail to validate input parameters. : startYmdH and endYmdH must have value!")
	bExpectError(t, bHTTP(t, hs, "GET", p+"?startYmd=2024010100", ""), 400, "fail to validate input parameters. : startYmd and endYmd should be not null !")
	bExpectError(t, bHTTP(t, hs, "GET", p+"?startYmd=2024010100&endYmd=2024010101&startYmdH=1", ""), 400, "fail to validate input parameters. : startYmd, endYmd and startYmdH, endYmdH must not coexist!")
	bExpectError(t, bHTTP(t, hs, "GET", p+"?startYmd=20240101&endYmd=2024010101", ""), 400, "fail to validate input parameters. : startYmd/endYmd must be yyyyMMddHH")
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/visitor/ofObject/,?startYmdH=1&endYmdH=2", ""), 400, "fail to validate input parameters. : Query parameter 'objHashes' is required!")
}

func TestBDictionary(t *testing.T) {
	f, _, hs := bSetup(t, nil)
	f.handle("GET_TEXT_PACK", bodyPack, func(r fakeReq) ([]any, error) {
		typ := *bMapText(r.Pack, "type")
		var out []any
		for _, h := range bMapList(r.Pack, "hash") {
			out = append(out, &protocol.TextPack{XType: typ, Hash: bInt(h), Text: typ + "-text"})
		}
		return out, nil
	})
	res := bHTTP(t, hs, "GET", "/scouter/v1/dictionary/20240101?dictKeys=[service:12345,sql:-678,service:12345,service:1]", "")
	want := bStreamPrefix + `[{"textType":"service","dictKey":"1","text":"service-text"},{"textType":"service","dictKey":"12345","text":"service-text"},` +
		`{"textType":"sql","dictKey":"-678","text":"sql-text"}]}`
	if res.status != 200 || res.body != want {
		t.Fatalf("dictionary\n got %s\nwant %s", res.body, want)
	}
	reqs := f.requests("GET_TEXT_PACK")
	if len(reqs) != 2 || bPackString(reqs[0].Pack) != "date=T:20240101,type=T:service,hash=L[D:1 D:12345]" || bPackString(reqs[1].Pack) != "date=T:20240101,type=T:sql,hash=L[D:-678]" {
		t.Fatalf("requests %v", reqs)
	}
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/dictionary/20240101", ""), 400, "[ValidationException] dictSets may not be null")
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/dictionary/20240101?dictKeys=service", ""), 400, `fail to validate input parameters. : dictKeys item must be "type:hash": "service"`)
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/dictionary/20240101?dictKeys=service:x", ""), 400, `fail to validate input parameters. : dictKeys has an invalid number: "x"`)
}

func TestBGlobalKv(t *testing.T) {
	f, _, hs := bSetup(t, nil)
	f.handle("GET_GLOBAL_KV", bodyValue, func(r fakeReq) ([]any, error) {
		if r.Value.Text == "missing" {
			return []any{protocol.Value{Type: protocol.ValueNull}}, nil
		}
		return []any{bTextV("v:" + r.Value.Text)}, nil
	})
	ok := true
	set := func(fakeReq) ([]any, error) { return []any{protocol.Value{Type: protocol.ValueBoolean, Bool: ok}}, nil }
	f.handle("SET_GLOBAL_KV", bodyPack, set)
	f.handle("SET_GLOBAL_TTL", bodyPack, set)
	f.static("GET_GLOBAL_KV_BULK", bMap(bEntry("b", bTextV("2")), bEntry("a", bTextV("1"))))
	f.static("SET_GLOBAL_KV_BULK", bMap(bEntry("a", protocol.Value{Type: protocol.ValueBoolean, Bool: true}), bEntry("b", protocol.Value{Type: protocol.ValueBoolean})))

	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/kv/k1", ""), 200, `"v:k1"`)
	if r := bLastReq(t, f, "GET_GLOBAL_KV"); r.Value.Type != protocol.ValueText || r.Value.Text != "k1" {
		t.Fatalf("GET_GLOBAL_KV must send a TextValue: %+v", r)
	}
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/kv/missing", ""), 200, "null")
	bExpect(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv", `{"key":"k","value":12,"serverId":0}`), 200, "true")
	if got := bPackString(bLastReq(t, f, "SET_GLOBAL_KV").Pack); got != "key=T:k,value=T:12,ttl=D:-1" {
		t.Fatalf("request %s", got)
	}
	bExpect(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv/k/:ttl", `{"ttl":60}`), 200, "true")
	if got := bPackString(bLastReq(t, f, "SET_GLOBAL_TTL").Pack); got != "key=T:k,ttl=D:60" {
		t.Fatalf("request %s", got)
	}
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/kv/[a,b]/:bulk", ""), 200, `[{"key":"b","value":"2"},{"key":"a","value":"1"}]`)
	if got := bPackString(bLastReq(t, f, "GET_GLOBAL_KV_BULK").Pack); got != "key=L[T:a T:b]" {
		t.Fatalf("request %s", got)
	}
	bExpect(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv/:bulk", `{"kvList":[{"key":"b","value":"x"},{"key":"a","value":true}],"ttl":5}`), 200, `[{"key":"a","value":true},{"key":"b","value":false}]`)
	if got := bPackString(bLastReq(t, f, "SET_GLOBAL_KV_BULK").Pack); got != "kv=M{a=T:true b=T:x},ttl=D:5" {
		t.Fatalf("request %s", got)
	}
	ok = false
	bExpectError(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv", `{"key":"k","value":"v"}`), 500, "Error on setting value to kvstore!")
	bExpectError(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv", ``), 400, "fail to validate input parameters. : request body is required")
	bExpectError(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv/:bulk", `{"ttl":5}`), 400, "fail to validate input parameters. : kvList is required")
	// Jersey routes /v1/kv/space/... to the custom store, so global "space" is unreachable.
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/kv/space", ""), 404, "HTTP 404 Not Found")
	bExpectError(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv/a/b", `{"ttl":1}`), 404, "HTTP 404 Not Found")
}

func TestBCustomAndPrivateKv(t *testing.T) {
	f, s, hs := bSetup(t, nil)
	f.handle("SET_CUSTOM_TTL", bodyPack, func(fakeReq) ([]any, error) {
		return []any{protocol.Value{Type: protocol.ValueBoolean, Bool: true}}, nil
	})
	f.static("GET_CUSTOM_KV_BULK", bMap(bEntry("]alice:x", bTextV("1")), bEntry("other", bTextV("2"))))

	bExpect(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv/space/ns", `{"key":"k","value":"v","ttl":30}`), 200, "true")
	if got := bPackString(bLastReq(t, f, "SET_CUSTOM_KV").Pack); got != "keySpace=T:ns,key=T:k,value=T:v,ttl=D:30" {
		t.Fatalf("request %s", got)
	}
	if v := f.kvGet("ns/k"); v != "v" {
		t.Fatalf("kv %q", v)
	}
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/kv/space/ns/k", ""), 200, `"v"`)
	bExpect(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv/space/ns/k/:ttl", `{}`), 200, "true")
	if got := bPackString(bLastReq(t, f, "SET_CUSTOM_TTL").Pack); got != "keySpace=T:ns,key=T:k,ttl=D:-1" {
		t.Fatalf("request %s", got)
	}

	// Private store without an authenticated user: LOGIN_REQUIRED (Java NPE 500, W5).
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/kv-private/x", ""), 401, "login required.")

	mux := http.NewServeMux()
	s.registerB(mux)
	call := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, withUser(req, "alice"))
		return rec
	}
	if rec := call("PUT", "/scouter/v1/kv-private", `{"key":"x","value":"1"}`); rec.Code != 200 {
		t.Fatalf("private set %d %s", rec.Code, rec.Body)
	}
	if v := f.kvGet("___WEB_SESSION_PRIVATE___/]alice:x"); v != "1" {
		t.Fatalf("private kv %q", v)
	}
	rec := call("GET", "/scouter/v1/kv-private/x", "")
	if rec.Code != 200 || resultOf(t, rec.Body.String()) != `"1"` {
		t.Fatalf("private get %d %s", rec.Code, rec.Body)
	}
	rec = call("GET", "/scouter/v1/kv-private/x,y/:bulk", "")
	// The prefix is cut by length even when absent (original toGeneralKey never throws).
	if rec.Code != 200 || resultOf(t, rec.Body.String()) != `[{"key":"x","value":"1"},{"key":"","value":"2"}]` {
		t.Fatalf("private bulk %d %s", rec.Code, rec.Body)
	}
	if got := bPackString(bLastReq(t, f, "GET_CUSTOM_KV_BULK").Pack); got != "keySpace=T:___WEB_SESSION_PRIVATE___,key=L[T:]alice:x T:]alice:y]" {
		t.Fatalf("request %s", got)
	}
}

func TestBShortenerAndSimpleView(t *testing.T) {
	f, _, hs := bSetup(t, nil)
	target := "http://localhost:6180/index.html?a=1"
	res := bHTTP(t, hs, "POST", "/scouter/v1/shortener?url="+strings.ReplaceAll(strings.ReplaceAll(target, "?", "%3F"), "&", "%26"), "")
	bExpect(t, res, 200, `"`+hs.URL+`/scouter/s/xtd1h7p"`)
	if v := f.kvGet("shortner/xtd1h7p"); v != target {
		t.Fatalf("stored %q", v)
	}
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/shortener/xtd1h7p", ""), 200, `"`+target+`"`)

	client := hs.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	r := doHTTP(t, client, "GET", hs.URL+"/scouter/s/xtd1h7p", "", nil)
	if r.status != http.StatusSeeOther || r.header.Get("Location") != target {
		t.Fatalf("simple view %d %q", r.status, r.header.Get("Location"))
	}
	r = doHTTP(t, client, "GET", hs.URL+"/scouter/s/none", "", nil)
	if r.status != http.StatusNotFound || r.body != "" {
		t.Fatalf("simple view missing %d %q", r.status, r.body)
	}
	bExpectError(t, bHTTP(t, hs, "POST", "/scouter/v1/shortener", ""), 400, "fail to validate input parameters. : url is required")
}

func TestBInteractionCounter(t *testing.T) {
	f, _, hs := bSetup(t, nil)
	bTexts(f, map[string]string{"object:10": "/host1/tomcat1", "object:20": "/host2/db"})
	f.static("INTR_COUNTER_REAL_TIME_BY_OBJ", &protocol.InteractionPerfCounterPack{ObjName: "/host1/tomcat1", InteractionType: "INTR_DB_CALL",
		FromHash: 10, ToHash: 20, Period: 30, Count: 4, ErrorCount: 1, TotalElapsed: 99, CustomData: protocol.Value{Type: protocol.ValueMap}})
	f.handle("OBJECT_LIST_REAL_TIME", bodyNone, func(fakeReq) ([]any, error) {
		return []any{
			&protocol.ObjectPack{ObjType: "tomcat", ObjHash: 10, ObjName: "/host1/tomcat1", Alive: true, Tags: protocol.Value{Type: protocol.ValueMap}},
			&protocol.ObjectPack{ObjType: "linux", ObjHash: bJavaHash("/host1"), ObjName: "/host1", Alive: true, Tags: protocol.Value{Type: protocol.ValueMap}},
		}, nil
	})
	xmlBlob := []byte(`<Counters><Familys><Family name="javaee"/></Familys><Types><ObjectType name="tomcat" family="javaee"/><ObjectType name="linux" family="host"/></Types></Counters>`)
	f.handle("GET_XML_COUNTER", bodyNone, func(fakeReq) ([]any, error) {
		return []any{bMap(bEntry("default", protocol.Value{Type: protocol.ValueBlob, Bytes: xmlBlob}))}, nil
	})
	f.handle("COUNTER_REAL_TIME_ALL_MULTI", bodyPack, func(r fakeReq) ([]any, error) {
		name := bMapList(r.Pack, "counter")[0].Text
		h := bInt(bMapList(r.Pack, "objHash")[0])
		v := protocol.Value{Type: protocol.ValueFloat, Float: 12.5}
		if name == "ActiveSpeed" {
			v = bDecs(1, 2, 3)
		}
		return []any{bMap(bEntry("objHash", bDecs(int64(h))), bEntry("counter", bTextList([]string{name})), bEntry("value", protocol.Value{Type: protocol.ValueList, Values: []protocol.Value{v}}))}, nil
	})

	base := `"objName":"/host1/tomcat1","interactionType":"INTR_DB_CALL","fromObjHash":"10","fromObjName":"/host1/tomcat1",`
	tail := `"toObjHash":"20","toObjName":"/host2/db",%s"period":"30","count":"4","errorCount":"1","totalElapsed":"99"}]`
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/interactionCounter/realTime/ofType/tomcat", ""), 200,
		"[{"+base+`"fromObjCounters":[],`+strings.Replace(tail, "%s", `"toObjCounters":[],`, 1))
	if got := bPackString(bLastReq(t, f, "INTR_COUNTER_REAL_TIME_BY_OBJ").Pack); got != "objType=T:tomcat,objHash=L[]" {
		t.Fatalf("request %s", got)
	}
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/interactionCounter/realTime?objHashes=20,10", ""), 200,
		"[{"+base+`"fromObjCounters":[{"name":"Cpu","value":"12.5"},{"name":"ActiveSpeed","value":["1","2","3"]}],`+strings.Replace(tail, "%s", `"toObjCounters":[],`, 1))
	if got := bPackString(bLastReq(t, f, "INTR_COUNTER_REAL_TIME_BY_OBJ").Pack); got != "objType=T:,objHash=L[D:20 D:10]" {
		t.Fatalf("request %s", got)
	}
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/interactionCounter/realTime", ""), 400, "[ValidationException] objHashes may not be null")
}

func TestBProfile(t *testing.T) {
	o := bLoadOracle(t)
	var profile []byte
	var raws []string
	for _, c := range o.Steps {
		b, _ := hex.DecodeString(c.Hex)
		profile = append(profile, b...)
		raws = append(raws, c.JSON)
	}
	f, _, hs := bSetup(t, nil)
	bTexts(f, map[string]string{"method:11": "com.A.run()", "sql:21": "select ?", "stackelem:81": "at A.b"})
	f.static("TRANX_PROFILE", &protocol.ProfilePack{Profile: profile})

	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/profile/20240101/-77", ""), 200, "["+strings.Join(raws, ",")+"]")
	if got := bPackString(bLastReq(t, f, "TRANX_PROFILE").Pack); got != "date=T:20240101,txid=D:-77,max=D:10" {
		t.Fatalf("request %s", got)
	}
	res := bHTTP(t, hs, "GET", "/scouter/v1/profile-data/20240101/1", "")
	bExpect(t, res, 200, "")
	body := resultOf(t, res.body)
	for _, k := range []string{
		`{"mainValue":"com.A.run()","additionalValueList":[],"step":` + raws[0] + `}`,
		`{"mainValue":"select ?","additionalValueList":[],"step":` + raws[2] + `}`,
		`{"mainValue":"hello \"world\"\n","additionalValueList":[]`,
		`{"mainValue":"127.0.0.1","additionalValueList":[]`,
		`{"mainValue":"","additionalValueList":["at A.b","**unlabeled**:stackelem:-82"]`,
		`{"mainValue":"**unlabeled**:hmsg:41","additionalValueList":[]`,
		`"localEndpoint":{"hash":"111","serviceName":"**unlabeled**:object:111","ip":"10.0.0.1","port":"8080"}`,
		`{"mainValue":"","additionalValueList":[],"step":` + raws[15] + `}`,
	} {
		if !strings.Contains(body, k) {
			t.Errorf("profile-data missing %s", k)
		}
	}
	f.static("TRANX_PROFILE")
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/profile/20240101/1", ""), 500, "null")
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/profile/20240101/x", ""), 400, `fail to validate input parameters. : txid is not a long: "x"`)
}
