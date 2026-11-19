package webapp

import (
	"strings"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func bTestXLog(txid int64, objHash int32) *protocol.XLogPack {
	return &protocol.XLogPack{EndTime: 1704080096000, ObjHash: objHash, Service: 77, TxID: txid, GxID: 9, Elapsed: 12,
		IPAddress: []byte{10, 1, 2, 3}, CountryCode: "KR", XType: 1, Text1: "a", TailVersion: protocol.XLogCurrentTail}
}

func TestBXLogRealtime(t *testing.T) {
	f, _, hs := bSetup(t, nil)
	x1, x2 := bTestXLog(1, 3), bTestXLog(2, 1)
	meta := bMap(bEntry("loop", bDec(5)), bEntry("index", bDec(7)))
	f.static("TRANX_REAL_TIME_GROUP_LATEST", meta, x1, x2)
	f.static("TRANX_REAL_TIME_GROUP", meta)

	res := bHTTP(t, hs, "GET", "/scouter/v1/xlog/realTime/0/0?objHashes=[3,1]", "")
	want := bStreamPrefix + `{"xlogLoop":"5","xlogIndex":"7","xlogs":[` + bMustJSON(t, bToSXLog(x1)) + "," + bMustJSON(t, bToSXLog(x2)) + `],"count":"2"}}`
	if res.status != 200 || res.body != want {
		t.Fatalf("realtime %d\n got %s\nwant %s", res.status, res.body, want)
	}
	if got := bPackString(bLastReq(t, f, "TRANX_REAL_TIME_GROUP_LATEST").Pack); got != "index=D:0,loop=D:0,count=D:10000,objHash=L[D:1 D:3]" {
		t.Fatalf("request %s", got)
	}
	// A non-zero cursor uses TRANX_REAL_TIME_GROUP.
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog/realTime/4/6?objHashes=1", ""), 200, `{"xlogLoop":"5","xlogIndex":"7","xlogs":[],"count":"0"}`)
	if got := bPackString(bLastReq(t, f, "TRANX_REAL_TIME_GROUP").Pack); got != "index=D:6,loop=D:4,count=D:10000,objHash=L[D:1]" {
		t.Fatalf("request %s", got)
	}

	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog/realTime/-1/0?objHashes=1", ""), 400, "[ValidationException] xLogLoop must be greater than or equal to 0")
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog/realTime/x/0?objHashes=1", ""), 400, `fail to validate input parameters. : xlogLoop is not a long: "x"`)
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog-data/realTime/0/1.5?objHashes=1", ""), 400, `fail to validate input parameters. : offset2 is not an int: "1.5"`)
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog/realTime/0/0", ""), 400, "[ValidationException] objHashes may not be null")
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog/realTime/0/0?objHashes=a", ""), 400, `fail to validate input parameters. : objHashes has an invalid number: "a"`)
	// Jersey literal precedence: realTime wins over {yyyymmdd}/gxid/{gxid}.
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog/realTime/gxid/1?objHashes=1", ""), 400, `fail to validate input parameters. : xlogLoop is not a long: "gxid"`)
}

func TestBXLogPageable(t *testing.T) {
	f, _, hs := bSetup(t, nil)
	x := bTestXLog(11, 1)
	f.static("TRANX_LOAD_TIME_GROUP_V2", x, bMap(bEntry("hasMore", protocol.Value{Type: protocol.ValueBoolean, Bool: true}), bEntry("lastXLogTime", bDec(1704080096000)), bEntry("lastTxid", bDec(11))))

	res := bHTTP(t, hs, "GET", "/scouter/v1/xlog/20240101?startHms=000000&endHms=235959&objHashes=1,2&pageCount=0", "")
	want := bStreamPrefix + `{"xlogs":[` + bMustJSON(t, bToSXLog(x)) + `],"hasMore":true,"lastTxid":"11","lastXLogTime":"1704080096000","count":"1"}}`
	if res.status != 200 || res.body != want {
		t.Fatalf("pageable %d\n got %s\nwant %s", res.status, res.body, want)
	}
	// startHms/endHms are local (server TZ) times of {yyyymmdd}; pageCount<=0 -> 10000.
	if got := bPackString(bLastReq(t, f, "TRANX_LOAD_TIME_GROUP_V2").Pack); got != "date=T:20240101,stime=D:1704034800000,txid=D:0,etime=D:1704121199000,lastBucketTime=D:0,pageCount=D:10000,objHash=L[D:1 D:2]" {
		t.Fatalf("request %s", got)
	}
	bHTTP(t, hs, "GET", "/scouter/v1/xlog/20240101?startTimeMillis=1&endTimeMillis=2&objHashes=1&pageCount=30000&lastTxid=5&lastXLogTime=6", "")
	if got := bPackString(bLastReq(t, f, "TRANX_LOAD_TIME_GROUP_V2").Pack); got != "date=T:20240101,stime=D:1,txid=D:5,etime=D:2,lastBucketTime=D:6,pageCount=D:30000,objHash=L[D:1]" {
		t.Fatalf("request %s", got)
	}

	base := "/scouter/v1/xlog/20240101?objHashes=1&"
	for q, msg := range map[string]string{
		"startTimeMillis=1&endTimeMillis=2&pageCount=30001": "[ValidationException] pageCount must be less than or equal to 30000",
		"startTimeMillis=1&endTimeMillis=2&lastTxid=5":      "fail to validate input parameters. : lastTxid and lastXlogTime must coexist!",
		"startHms=000000": "fail to validate input parameters. : startHms and endHms should be not null !",
		"startHms=000000&endHms=010000&startTimeMillis=1": "fail to validate input parameters. : startYmdHms, endYmdHms and startTimeMillis, endTimeMills must not coexist!",
		"startHms=00ab00&endHms=010000":                   "fail to validate input parameters. : date is invalid!",
		"startTimeMillis=1":                               "fail to validate input parameters. : startTimeMillis and endTimeMillis must have value!",
		"":                                                "fail to validate input parameters. : startTimeMillis and endTimeMillis must have value!",
	} {
		bExpectError(t, bHTTP(t, hs, "GET", base+q, ""), 400, msg)
	}
	bExpectError(t, bHTTP(t, hs, "GET", base+"startTimeMillis=x&endTimeMillis=2", ""), 400, `fail to validate input parameters. : startTimeMillis is not a long: "x"`)
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog/20240101?startTimeMillis=1&endTimeMillis=2", ""), 400, "[ValidationException] objHashes may not be null")
}

func TestBXLogSingleGxidSearch(t *testing.T) {
	f, _, hs := bSetup(t, nil)
	x := bTestXLog(42, 1)
	f.static("XLOG_READ_BY_TXID", x)
	f.static("XLOG_READ_BY_GXID", x, x)
	f.static("SEARCH_XLOG_LIST", x)

	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog/20240101/42", ""), 200, bMustJSON(t, bToSXLog(x)))
	if got := bPackString(bLastReq(t, f, "XLOG_READ_BY_TXID").Pack); got != "date=T:20240101,txid=D:42" {
		t.Fatalf("request %s", got)
	}
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog/20240101/0", ""), 400, "fail to validate input parameters. : txid and yyyymmdd must coexist!")
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog/20240101/zz", ""), 400, `fail to validate input parameters. : txid is not a long: "zz"`)

	sx := bMustJSON(t, bToSXLog(x))
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog/20240101/gxid/9", ""), 200, "["+sx+","+sx+"]")
	if got := bPackString(bLastReq(t, f, "XLOG_READ_BY_GXID").Pack); got != "date=T:20240101,gxid=D:9" {
		t.Fatalf("request %s", got)
	}
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog/20240101/gxid/0", ""), 400, "fail to validate input parameters. : gxid and yyyymmdd must coexist!")

	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog/search/20240101?startTimeMillis=10&endTimeMillis=20&service=&objHash=-5&login=u&text3=t", ""), 200, "["+sx+"]")
	if got := bPackString(bLastReq(t, f, "SEARCH_XLOG_LIST").Pack); got != "date=T:20240101,stime=D:10,etime=D:20,service=T:,objHash=D:-5,login=T:u,text3=T:t" {
		t.Fatalf("request %s", got)
	}
	bHTTP(t, hs, "GET", "/scouter/v1/xlog/search/20240101?startTimeMillis=10&endTimeMillis=20", "")
	if got := bPackString(bLastReq(t, f, "SEARCH_XLOG_LIST").Pack); got != "date=T:20240101,stime=D:10,etime=D:20" {
		t.Fatalf("request %s", got)
	}

	// Not found -> result null.
	f.static("XLOG_READ_BY_TXID")
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog/20240101/43", ""), 200, "null")
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog-data/20240101/43", ""), 200, "null")
}

func TestBXLogData(t *testing.T) {
	f, _, hs := bSetup(t, nil)
	bTexts(f, map[string]string{"service:77": "/svc"})
	x := bTestXLog(42, 1)
	f.static("XLOG_LOAD_BY_TXIDS", x)
	f.static("XLOG_READ_BY_TXID", x)
	f.static("TRANX_REAL_TIME_GROUP", bMap(bEntry("loop", bDec(1<<40)), bEntry("index", bDec(9))), x)

	res := bHTTP(t, hs, "GET", "/scouter/v1/xlog-data/20240101/42", "")
	bExpect(t, res, 200, "")
	got := resultOf(t, res.body)
	for _, k := range []string{`"service":"/svc"`, `"error":""`, `"ipAddr":"10.1.2.3"`, `"xlogType":"APP_SERVICE"}`, `"threadName":""`} {
		if !strings.Contains(got, k) {
			t.Fatalf("XLogData missing %s: %s", k, got)
		}
	}
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog-data/20240101/multi/[42,-7,42]", ""), 200, "["+got+"]")
	if r := bPackString(bLastReq(t, f, "XLOG_LOAD_BY_TXIDS").Pack); r != "date=T:20240101,txid=L[D:42 D:-7 D:42]" {
		t.Fatalf("request %s", r)
	}
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/xlog-data/20240101/multi/1,x", ""), 400, `fail to validate input parameters. : txidList has an invalid number: "x"`)

	// realTime: the collector cursor is exposed as offset1/offset2 (loop stays long).
	rt := bHTTP(t, hs, "GET", "/scouter/v1/xlog-data/realTime/3/4?objHashes=1", "")
	if want := bStreamPrefix + `{"xlogs":[` + got + `],"offset1":"1099511627776","offset2":"9","count":"1"}}`; rt.body != want {
		t.Fatalf("realtime\n got %s\nwant %s", rt.body, want)
	}
	if r := bPackString(bLastReq(t, f, "TRANX_REAL_TIME_GROUP").Pack); r != "index=D:4,loop=D:3,count=D:10000,objHash=L[D:1]" {
		t.Fatalf("request %s", r)
	}
}

func TestBXLogCollectorErrors(t *testing.T) {
	_, _, hs := bSetup(t, nil)
	// Unregistered collector command: the call fails -> 500 wrapper (no partial 200 stream).
	res := bHTTP(t, hs, "GET", "/scouter/v1/xlog/20240101?startTimeMillis=1&endTimeMillis=2&objHashes=1", "")
	bExpectError(t, res, 500, "")
	res = bHTTP(t, hs, "GET", "/scouter/v1/xlog-data/realTime/0/0?objHashes=1", "")
	bExpectError(t, res, 500, "")
}
