package webapp

import (
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func bDecs(ns ...int64) protocol.Value {
	vs := make([]protocol.Value, len(ns))
	for i, n := range ns {
		vs[i] = bDec(n)
	}
	return protocol.Value{Type: protocol.ValueList, Values: vs}
}

func TestBSummaryServiceTwoDays(t *testing.T) {
	f, _, hs := bSetup(t, nil)
	bTexts(f, map[string]string{"service:1": "/a", "service:100": "/c"})
	f.handle("LOAD_SERVICE_SUMMARY", bodyPack, func(r fakeReq) ([]any, error) {
		if *bMapText(r.Pack, "date") == "20240101" {
			return []any{bMap(bEntry("id", bDecs(100, 1)), bEntry("count", bDecs(1, 2)), bEntry("error", bDecs(0, 1)),
				bEntry("elapsed", bDecs(10, 20)), bEntry("cpu", bDecs(1, 2)), bEntry("mem", bDecs(3, 4)))}, nil
		}
		return []any{bMap(bEntry("id", bDecs(17, 1)), bEntry("count", bDecs(5, 3)), bEntry("error", bDecs(0, 0)),
			bEntry("elapsed", bDecs(50, 30)), bEntry("cpu", bDecs(5, 3)), bEntry("mem", bDecs(6, 7)))}, nil
	})
	res := bHTTP(t, hs, "GET", "/scouter/v1/summary/service/ofType/tomcat?startYmdHm=202401012300&endYmdHm=202401020100", "")
	bExpect(t, res, 200, `{"itemMap":{`+
		`"1":{"summaryKey":"1","summaryKeyName":"/a","count":"5","errorCount":"1","elapsedSum":"50","cpuSum":"5","memorySum":"11"},`+
		`"17":{"summaryKey":"17","summaryKeyName":"**unlabeled**:service:17","count":"5","errorCount":"0","elapsedSum":"50","cpuSum":"5","memorySum":"6"},`+
		`"100":{"summaryKey":"100","summaryKeyName":"/c","count":"1","errorCount":"0","elapsedSum":"10","cpuSum":"1","memorySum":"3"}}}`)
	reqs := f.requests("LOAD_SERVICE_SUMMARY")
	if len(reqs) != 2 {
		t.Fatalf("%d day requests", len(reqs))
	}
	want := []string{
		"date=T:20240101,stime=D:1704117600000,etime=D:1704121199000,objType=T:tomcat,objHash=D:0",
		"date=T:20240102,stime=D:1704121200000,etime=D:1704124800000,objType=T:tomcat,objHash=D:0",
	}
	for i := range want {
		if got := bPackString(reqs[i].Pack); got != want[i] {
			t.Fatalf("day %d request %s", i, got)
		}
	}
}

func TestBSummaryKinds(t *testing.T) {
	f, _, hs := bSetup(t, nil)
	bTexts(f, map[string]string{"error:7": "NPE", "service:8": "/s", "sql:9": "select 1"})
	f.static("LOAD_IP_SUMMARY", bMap(bEntry("id", bDecs(0x0A000001)), bEntry("count", bDecs(4))))
	f.static("LOAD_SERVICE_ERROR_SUMMARY", bMap(bEntry("id", bDecs(3)), bEntry("count", bDecs(2)), bEntry("error", bDecs(7)),
		bEntry("service", bDecs(8)), bEntry("message", bDecs(0)), bEntry("txid", bDecs(-9)), bEntry("sql", bDecs(9)),
		bEntry("apicall", bDecs(0)), bEntry("fullstack", bDecs(0))))
	f.static("LOAD_ALERT_SUMMARY", bMap(bEntry("id", bDecs(5)), bEntry("title", bTextList([]string{"GC"})), bEntry("count", bDecs(1)), bEntry("level", bDecs(2))))
	f.static("LOAD_SQL_SUMMARY", bMap(bEntry("id", bDecs(9)), bEntry("count", bDecs(1)), bEntry("error", bDecs(0)), bEntry("elapsed", bDecs(4))))
	f.static("LOAD_APICALL_SUMMARY", bMap())
	f.static("LOAD_UA_SUMMARY", bMap(bEntry("id", bDecs(6)), bEntry("count", bDecs(1))))

	q := "?startTimeMillis=1704067200000&endTimeMillis=1704070800000"
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/summary/ip/ofObject/5"+q, ""), 200, `{"itemMap":{"167772161":{"summaryKey":"167772161","summaryKeyName":"10.0.0.1","count":"4"}}}`)
	if got := bPackString(bLastReq(t, f, "LOAD_IP_SUMMARY").Pack); got != "date=T:20240101,stime=D:1704067200000,etime=D:1704070800000,objHash=D:5" {
		t.Fatalf("request %s", got)
	}
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/summary/error/ofType/tomcat"+q, ""), 200, `{"itemMap":{"3":{"summaryKey":"3","summaryKeyName":"NPE [:of:] /s","count":"2",`+
		`"error":"NPE","service":"/s","errorMessage":"","txid":"-9","sql":"select 1","apiCall":"","fullStack":""}}}`)
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/summary/alert/ofType/tomcat"+q, ""), 200, `{"itemMap":{"5":{"summaryKey":"5","summaryKeyName":"GC","count":"1","level":"ERROR"}}}`)
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/summary/sql/ofObject/1"+q, ""), 200, `{"itemMap":{"9":{"summaryKey":"9","summaryKeyName":"select 1","count":"1","errorCount":"0","elapsedSum":"4"}}}`)
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/summary/apiCall/ofObject/1"+q, ""), 200, `{"itemMap":{}}`)
	bExpect(t, bHTTP(t, hs, "GET", "/scouter/v1/summary/userAgent/ofObject/1"+q, ""), 200, `{"itemMap":{"6":{"summaryKey":"6","summaryKeyName":"**unlabeled**:ua:6","count":"1"}}}`)
}

func TestBSummaryValidation(t *testing.T) {
	_, _, hs := bSetup(t, nil)
	p := "/scouter/v1/summary/service/ofType/tomcat"
	bExpectError(t, bHTTP(t, hs, "GET", p, ""), 400, "fail to validate input parameters. : startTimeMillis and endTimeMillis must have value!")
	bExpectError(t, bHTTP(t, hs, "GET", p+"?startYmdHm=202401010000", ""), 400, "fail to validate input parameters. : startYmdHms and endYmdHms should be not null !")
	bExpectError(t, bHTTP(t, hs, "GET", p+"?startYmdHm=202401010000&endYmdHm=202401010100&endTimeMillis=5", ""), 400,
		"fail to validate input parameters. : startYmdHms, endYmdHms and startTimeMillis, endTimeMills must not coexist!")
	bExpectError(t, bHTTP(t, hs, "GET", p+"?startYmdHm=2024010100&endYmdHm=202401010100", ""), 400, "fail to validate input parameters. : startYmdHm/endYmdHm must be yyyyMMddHHmm")
	bExpectError(t, bHTTP(t, hs, "GET", p+"?startYmdHm=202301010000&endYmdHm=202401020100", ""), 400, "fail to validate input parameters. : summary range must not exceed 366 days")
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/summary/service/ofObject/x?startTimeMillis=1&endTimeMillis=2", ""), 400, `fail to validate input parameters. : objHash is not an int: "x"`)
}
