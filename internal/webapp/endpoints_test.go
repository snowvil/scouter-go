package webapp

import (
	"compress/gzip"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

const testCounterXML = `<Counters><Familys><Family name="javaee" master="TPS"><Counter name="TPS" disp="Tps" unit="cnt"/><Counter name="Cpu" disp="CPU" unit="%"/></Family></Familys>
<Types><ObjectType name="tomcat" disp="Tomcat" family="javaee"/></Types></Counters>`

func vlist(values ...protocol.Value) protocol.Value { return listValue(values) }
func texts(ss ...string) protocol.Value {
	out := make([]protocol.Value, len(ss))
	for i, s := range ss {
		out[i] = textValue(s)
	}
	return listValue(out)
}
func decs(ns ...int64) protocol.Value {
	out := make([]protocol.Value, len(ns))
	for i, n := range ns {
		out[i] = decimalValue(n)
	}
	return listValue(out)
}

// setupEndpoints registers fixed responses for every A command.
func setupEndpoints(f *fakeCollector) {
	tomcat := &protocol.ObjectPack{ObjType: "tomcat", ObjHash: 100, ObjName: "/host/tc1", Address: "10.0.0.1", Version: "2.21", Alive: true, Wakeup: 1700000000000,
		Tags: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("k", textValue("v")), entry("n", decimalValue(3))}}}
	dead := &protocol.ObjectPack{ObjType: "tomcat", ObjHash: 200, ObjName: "/host/tc2", Alive: false, Tags: protocol.Value{Type: protocol.ValueMap}}
	f.handle("OBJECT_LIST_REAL_TIME", bodyNone, func(fakeReq) ([]any, error) { return []any{tomcat, dead}, nil })
	f.handle("GET_XML_COUNTER", bodyNone, func(fakeReq) ([]any, error) {
		return []any{newMap(entry("default", protocol.Value{Type: protocol.ValueBlob, Bytes: []byte(testCounterXML)}))}, nil
	})
	f.handle("OBJECT_REMOVE_INACTIVE", bodyNone, func(fakeReq) ([]any, error) { return nil, nil })
	f.static("OBJECT_THREAD_LIST", newMap(entry("id", decs(1, 2)), entry("name", texts("main", "w")), entry("stat", texts("RUNNABLE", "WAITING")),
		entry("cpu", decs(10, 20)), entry("txid", texts("x1", "")), entry("elapsed", texts("5", "0")), entry("service", texts("/a", ""))))
	f.static("OBJECT_THREAD_DUMP", newMap(entry("threadDump", texts("l1", "l2"))))
	f.static("OBJECT_HEAPHISTO", newMap(entry("heaphisto", texts("1: 10 100 [I", "bad line", "2: 3 30 java.lang.String"))))
	f.static("OBJECT_ENV", newMap(entry("java.version", textValue("17")), entry("pid", decimalValue(42))))
	f.static("SOCKET", newMap(entry("key", decs(7)), entry("host", vlist(protocol.Value{Type: protocol.ValueBlob, Bytes: []byte{10, 0, 0, 9}})),
		entry("port", decs(8080)), entry("count", decs(3)), entry("service", decs(555)), entry("txid", decs(99)),
		entry("order", vlist(protocol.Value{Type: protocol.ValueBoolean, Bool: true})), entry("stack", texts("st"))))
	f.static("GET_TEXT_100", newMap(entry(hexa32(555), textValue("/svc"))))
	f.static("HOST_TOP", newMap(entry("PID", decs(1)), entry("USER", texts("root")), entry("CPU", vlist(protocol.Value{Type: protocol.ValueFloat, Float: 1.5})),
		entry("MEM", decs(2)), entry("TIME", decs(3)), entry("NAME", texts("init"))))
	f.static("HOST_DISK_USAGE", newMap(entry("Device", texts("/dev/sda")), entry("Total", decs(100)), entry("Used", decs(40)), entry("Free", decs(60)),
		entry("Pct", decs(40)), entry("Type", texts("ext4")), entry("Mount", texts("/"))))
	f.static("ACTIVESPEED_REAL_TIME", newMap(entry("objHash", decimalValue(100)), entry("act1", decimalValue(1)), entry("act2", decimalValue(2)), entry("act3", decimalValue(3))),
		newMap(entry("objHash", decimalValue(999)), entry("act1", decimalValue(0)), entry("act2", decimalValue(0)), entry("act3", decimalValue(0))))
	f.static("OBJECT_ACTIVE_SERVICE_LIST", newMap(entry("objHash", decimalValue(100)), entry("id", decs(1, 2)), entry("name", texts("t1", "t2")),
		entry("stat", texts("R", "W")), entry("cpu", decs(5, 6)), entry("txid", texts("xa", "z1")), entry("elapsed", decs(10, 30)),
		entry("service", texts("/a", "/b")), entry("ip", texts("1.1.1.1", "2.2.2.2")), entry("sql", texts("select 1", "")), entry("subcall", texts("", "http://x"))),
		newMap(entry("objHash", decimalValue(999)), entry("id", decs(9))))
	f.static("OBJECT_THREAD_CONTROL", newMap(entry("Stack Trace", textValue("s")), entry("Thread Name", textValue("t")), entry("Service Elapsed", decimalValue(77)),
		entry("Blob", protocol.Value{Type: protocol.ValueBlob, Bytes: []byte{1}}), entry("Cpu", protocol.Value{Type: protocol.ValueDouble, Double: 1.5})))
	f.static("OBJECT_THREAD_DETAIL", newMap(entry("Service Txid", textValue("xa")), entry("Service Elapsed", decimalValue(12)), entry("SQL", textValue("q")),
		entry("Thread Id", decimalValue(5)), entry("State", textValue("RUNNABLE")), entry("Stack Trace", textValue("trace"))))
	f.handle("GET_CONFIGURE_SERVER", bodyNone, func(fakeReq) ([]any, error) {
		return []any{newMap(entry("configKey", texts("a")), entry("serverConfig", textValue("a=1\n")))}, nil
	})
	f.handle("LIST_CONFIGURE_SERVER", bodyNone, func(fakeReq) ([]any, error) {
		return []any{newMap(entry("key", texts("a")), entry("value", texts("1")), entry("default", vlist(decimalValue(0))))}, nil
	})
	desc := protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("strings", texts("x")), entry("strings1", texts()), entry("strings2", texts()),
		entry("strings3", texts()), entry("booleans", vlist(protocol.Value{Type: protocol.ValueBoolean, Bool: true})), entry("booleans1", vlist()), entry("ints", decs(4)), entry("ints1", vlist())}}
	f.static("CONFIGURE_DESC", newMap(entry("a", textValue("desc a")), entry("$x$b", textValue("desc b"))))
	f.static("CONFIGURE_VALUE_TYPE", newMap(entry("a", decimalValue(2)), entry("z", decimalValue(9))))
	f.static("CONFIGURE_VALUE_TYPE_DESC", newMap(entry("a", desc)))
	f.static("GET_CONFIGURE_WAS", newMap(entry("agentConfig", textValue("k=1\n"))))
	f.static("LIST_CONFIGURE_WAS", newMap(entry("key", texts("k")), entry("value", texts("1")), entry("default", texts("0"))))
	f.static("SET_CONFIGURE_SERVER", newMap(entry("result", textValue("true"))))
	f.static("SET_CONFIGURE_WAS", newMap(entry("result", textValue("TRUE"))))
	f.static("ALERT_REAL_TIME", newMap(entry("loop", decimalValue(3)), entry("index", decimalValue(4))),
		&protocol.AlertPack{Time: 1000, Level: 2, ObjType: "tomcat", ObjHash: 100, Title: "t", Message: "m", Tags: protocol.Value{Type: protocol.ValueMap}},
		&protocol.AlertPack{Time: 2000, Level: 9, ObjType: "x", ObjHash: 999, Title: "u", Message: "n", Tags: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("a", protocol.Value{Type: protocol.ValueBoolean, Bool: true})}}})
	f.static("GET_ALERT_SCRIPTING_CONTETNS", newMap(entry("contents", textValue(""))))
	f.static("GET_ALERT_SCRIPTING_CONFIG_CONTETNS", newMap(entry("contents", textValue("cfg"))))
	f.static("GET_ALERT_REAL_COUNTER_DESC", newMap(entry("desc", textValue("d")), entry("methodName", textValue("value")), entry("returnTypeName", textValue("int")), entry("parameterTypeNames", texts("String", "int"))),
		newMap(entry("desc", textValue("a")), entry("methodName", textValue("avg")), entry("returnTypeName", textValue("double")), entry("parameterTypeNames", texts())))
	f.static("GET_PLUGIN_HELPER_DESC", newMap(entry("desc", textValue("h")), entry("methodName", textValue("log")), entry("returnTypeName", textValue("void")), entry("parameterTypeNames", texts("Object"))))
	f.static("SAVE_ALERT_SCRIPTING_CONTETNS", newMap(entry("success", protocol.Value{Type: protocol.ValueBoolean, Bool: true})))
	f.static("SAVE_ALERT_SCRIPTING_CONFIG_CONTETNS", newMap(entry("success", protocol.Value{Type: protocol.ValueBoolean, Bool: false})))
	f.static("GET_ALERT_SCRIPT_LOAD_MESSAGE", newMap(entry("loop", decimalValue(1)), entry("index", decimalValue(2)), entry("messages", texts("m1"))))
	f.static("COUNTER_REAL_TIME_ALL_MULTI", newMap(entry("objHash", decs(100, 999)), entry("counter", texts("TPS", "Cpu")),
		entry("value", vlist(protocol.Value{Type: protocol.ValueFloat, Float: 1.5}, decimalValue(7)))))
	f.handle("COUNTER_PAST_TIME_ALL", bodyPack, func(r fakeReq) ([]any, error) {
		st := mapInt(r.Pack, "stime")
		return []any{newMap(entry("objHash", decimalValue(100)), entry("time", decs(st)), entry("value", vlist(protocol.Value{Type: protocol.ValueDouble, Double: 2})))}, nil
	})
	f.static("COUNTER_PAST_LONGDATE_ALL", newMap(entry("objHash", decimalValue(100)), entry("time", decs(1, 2)), entry("value", decs(3, 4))))
}

func TestEndpointJSON(t *testing.T) {
	f := newFakeCollector(t)
	setupEndpoints(f)
	cfg := testConfig(f)
	fixed := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	cfg.Clock = func() time.Time { return fixed }
	_, hs := newTestServer(t, cfg)
	cases := []struct {
		method, path, body string
		want               string
	}{
		{"GET", "/scouter/v1/object", "", `[{"objType":"tomcat","objFamily":"javaee","objHash":"100","objName":"/host/tc1","address":"10.0.0.1","version":"2.21","alive":true,"lastWakeUpTime":"1700000000000","tags":{"k":"v","n":"3"}},` +
			`{"objType":"tomcat","objFamily":"javaee","objHash":"200","objName":"/host/tc2","address":"","version":"","alive":false,"lastWakeUpTime":"0","tags":{}}]`},
		{"GET", "/scouter/v1/object/remove/inactive", "", `true`},
		{"GET", "/scouter/v1/object/remove/inactive/server?serverId=1", "", `true`},
		{"GET", "/scouter/v1/object/threadList/100", "", `[{"id":"1","name":"main","stat":"RUNNABLE","cpu":"10","txid":"x1","elapsed":"5","service":"/a"},{"id":"2","name":"w","stat":"WAITING","cpu":"20","txid":"","elapsed":"0","service":""}]`},
		{"GET", "/scouter/v1/object/threadDump/100", "", `"l1\nl2\n"`},
		{"GET", "/scouter/v1/object/heapHistogram/100", "", `[{"no":"1","count":"10","size":"100","name":"int[]"},{"no":"2","count":"3","size":"30","name":"java.lang.String"}]`},
		{"GET", "/scouter/v1/object/env/100", "", `[{"name":"java.version","value":"17"},{"name":"pid","value":"42"}]`},
		{"GET", "/scouter/v1/object/socket/100", "", `[{"key":"7","host":"10.0.0.9","port":"8080","count":"3","service":"/svc","txid":"99","standby":true,"stack":"st"}]`},
		{"GET", "/scouter/v1/object/host/realTime/top/ofObject/100", "", `[{"pid":"1","user":"root","cpu":"1.5","mem":"2","time":"3","name":"init"}]`},
		{"GET", "/scouter/v1/object/host/realTime/disk/ofObject/100", "", `[{"device":"/dev/sda","total":"100","used":"40","free":"60","pct":"40.0","type":"ext4","mount":"/"}]`},
		{"GET", "/scouter/v1/activeService/stepCount/ofType/tomcat", "", `[{"objHash":"100","objName":"/host/tc1","step1Count":"1","step2Count":"2","step3Count":"3"},{"objHash":"999","objName":"UNKNOWN","step1Count":"0","step2Count":"0","step3Count":"0"}]`},
		{"GET", "/scouter/v1/activeService/ofType/tomcat", "", `[{"objHash":"100","objName":"/host/tc1","threadId":"2","threadName":"t2","threadStatus":"W","threadCpuTime":"6","txidName":"z1","activeDate":"20261015","txid":"-1","elapsed":"30","serviceName":"/b","ipaddr":"2.2.2.2","note":"http://x","mode":"SUBCALL"},` +
			`{"objHash":"100","objName":"/host/tc1","threadId":"1","threadName":"t1","threadStatus":"R","threadCpuTime":"5","txidName":"xa","activeDate":"20261015","txid":"10","elapsed":"10","serviceName":"/a","ipaddr":"1.1.1.1","note":"select 1","mode":"SQL"}]`},
		{"GET", "/scouter/v1/activeService/control/thread/5/ofObject/100?action=interrupt", "", `{"serviceThread":true,"time":"77","keyValueDataList":[{"key":"Cpu","value":"1.5"},{"key":"Service Elapsed","value":"77"},{"key":"Thread Name","value":"t"}]}`},
		{"GET", "/scouter/v1/activeService/thread/5/ofObject/100?txidName=xa", "", `{"txidName":"xa","elapsed":"12","serviceName":null,"note":"q","mode":"SQL","threadId":"5","threadName":null,"threadStatus":"RUNNABLE","threadCpuTime":"0","threadUserTime":"0",` +
			`"blockCount":"0","blockTime":"0","waitedCount":"0","waitedTime":"0","lockOwnerId":"0","lockName":null,"lockOwnerName":null,"stackTrace":"trace","sqlActiveBindVar":null}`},
		{"GET", "/scouter/v1/configure/server", "", `{"contents":"a=1\n","configStateList":[{"key":"a","value":"1","def":"0"}],"descMap":{"a":"desc a","$x$b":"desc b","b":"desc b"},"valueTypeMap":{"a":"NUM","z":"VALUE"},` +
			`"valueTypeDescMap":{"a":{"strings":["x"],"strings1":[],"strings2":[],"strings3":[],"booleans":[true],"booleans1":[],"ints":["4"],"ints1":[]}},"status":"200"}`},
		{"GET", "/scouter/v1/configure/object/100", "", `{"contents":"k=1\n","configStateList":[{"key":"k","value":"1","def":"0"}],"descMap":{"a":"desc a","$x$b":"desc b","b":"desc b"},"valueTypeMap":{"a":"NUM","z":"VALUE"},` +
			`"valueTypeDescMap":{"a":{"strings":["x"],"strings1":[],"strings2":[],"strings3":[],"booleans":[true],"booleans1":[],"ints":["4"],"ints1":[]}},"status":"200"}`},
		{"POST", "/scouter/v1/configure/set/server", `{"values":"a=c:\\x"}`, `{"contents":"","configStateList":[{"key":"a","value":"1","def":"0"}],"descMap":{},"valueTypeMap":{},"valueTypeDescMap":{},"status":"200"}`},
		{"POST", "/scouter/v1/configure/set/object/100", `{"values":"k=2"}`, `{"contents":"","configStateList":[{"key":"k","value":"1","def":"0"}],"descMap":{},"valueTypeMap":{},"valueTypeDescMap":{},"status":"200"}`},
		{"PUT", "/scouter/v1/configure/set/kv/server", `{"key":"a","value":"2"}`, `true`},
		{"PUT", "/scouter/v1/configure/set/kv/ofType/tomcat", `{"key":"k","value":"2"}`, `[{"objHash":"100","success":true},{"objHash":"200","success":false}]`},
		{"GET", "/scouter/v1/alert/realTime/0/0?objType=tomcat", "", `{"offset1":"3","offset2":"4","alerts":[{"time":"1000","objType":"tomcat","objHash":"100","objName":"/host/tc1","level":"ERROR","title":"t","message":"m","tagMap":{}},` +
			`{"time":"2000","objType":"x","objHash":"999","objName":"UNKNOWN","level":"INFO","title":"u","message":"n","tagMap":{"a":true}}]}`},
		{"GET", "/scouter/v1/alert/scripting/TPS", "", `{"ruleText":"// void process(RealCounter $counter, PluginHelper $$) {\n// create your java code below..\n\n\n// }","configText":"cfg","configHash":"` +
			itoa32(javaHash("cfg")) + `","ruleTextHash":"` + itoa32(javaHash(defaultRuleContents)) + `","realCounterDescMap":{"$counter.avg()":{"desc":"a","methodName":"avg","returnTypeName":"double","fullSignature":"$counter.avg()"},` +
			`"$counter.value(String, int)":{"desc":"d","methodName":"value","returnTypeName":"int","fullSignature":"$counter.value(String, int)"}},"pluginHelperDescMap":{"$$.log(Object)":{"desc":"h","methodName":"log","returnTypeName":"void","fullSignature":"$$.log(Object)"}}}`},
		{"POST", "/scouter/v1/alert/set/rule/scripting/TPS", `{"values":"rule"}`, `{"status":"200","message":"Settings save success"}`},
		{"POST", "/scouter/v1/alert/set/config/scripting/TPS", `{"values":"other"}`, `{"status":"404","message":"Failed to save settings"}`},
		{"POST", "/scouter/v1/alert/set/config/scripting/TPS", `{"values":"cfg"}`, `{"status":"301","message":"No settings have been changed"}`},
		{"GET", "/scouter/v1/alert/read/scripting/0/0", "", `{"loop":"1","index":"2","message":["m1"]}`},
		{"GET", "/scouter/v1/counter/realTime/TPS,Cpu/ofType/tomcat", "", `[{"objHash":"100","objName":"/host/tc1","name":"TPS","displayName":"Tps","unit":"cnt","value":"1.5"},{"objHash":"999","objName":"UNKNOWN","name":"Cpu","displayName":"","unit":null,"value":"7"}]`},
		{"GET", "/scouter/v1/counter/realTime/[TPS]?objHashes=[100]", "", `[{"objHash":"100","objName":"/host/tc1","name":"TPS","displayName":"Tps","unit":"cnt","value":"1.5"},{"objHash":"999","objName":"UNKNOWN","name":"Cpu","displayName":"","unit":null,"value":"7"}]`},
		{"GET", "/scouter/v1/counter/TPS/ofType/tomcat?startTimeMillis=1760486400000&endTimeMillis=1760490000000", "", `[{"objHash":"100","objName":"/host/tc1","startTimeMillis":"1760486400000","endTimeMillis":"1760490000000","name":"TPS","displayName":"Tps","unit":"cnt","timeList":["1760486401000"],"valueList":["2.0"]}]`},
		{"GET", "/scouter/v1/counter/TPS?objHashes=100&startYmdHms=20251014233000&endYmdHms=20251015003000", "", `[{"objHash":"100","objName":"/host/tc1","startTimeMillis":"1760484600000","endTimeMillis":"1760488200000","name":"TPS","displayName":"Tps","unit":"cnt","timeList":["1760484600000","1760486401000"],"valueList":["2.0","2.0"]}]`},
		{"GET", "/scouter/v1/counter/stat/TPS/ofType/tomcat?startYmd=20261001&endYmd=20261002", "", `[{"objHash":"100","objName":"/host/tc1","fromYmd":"20261001","toYmd":"20261002","name":"TPS","displayName":"Tps","unit":"cnt","timeList":["1","2"],"valueList":["3.0","4.0"]}]`},
		{"GET", "/scouter/v1/counter/stat/TPS?objHashes=100&startYmd=20261001&endYmd=20261002", "", `[{"objHash":"100","objName":"/host/tc1","fromYmd":"20261001","toYmd":"20261002","name":"TPS","displayName":"Tps","unit":"cnt","timeList":["1","2"],"valueList":["3.0","4.0"]}]`},
		{"GET", "/scouter/v1/counter/TPS/latest/60/ofType/tomcat", "", ``},
		{"GET", "/scouter/v1/counter/TPS/latest/60?objHashes=100", "", ``},
		{"GET", "/scouter/v1/info/counter-model", "", `{"families":[{"name":"javaee","counters":[{"name":"TPS","displayName":"Tps","unit":"cnt","totalizable":false,"icon":null,"master":true},{"name":"Cpu","displayName":"CPU","unit":"%","totalizable":false,"icon":null,"master":false}]}],"objTypes":[{"name":"tomcat","familyName":"javaee","displayName":"Tomcat","icon":null,"subObject":false}]}`},
	}
	for _, c := range cases {
		res := doHTTP(t, nil, c.method, hs.URL+c.path, c.body, map[string]string{"Content-Type": "application/json"})
		if res.status != 200 {
			t.Errorf("%s %s: status %d %s", c.method, c.path, res.status, res.body)
			continue
		}
		if !strings.HasPrefix(res.body, `{"status":"200","requestId":"#`) || !strings.Contains(res.body, `"resultCode":"0","message":"success","result":`) {
			t.Errorf("%s %s: wrapper %s", c.method, c.path, res.body)
			continue
		}
		if c.want != "" && resultOf(t, res.body) != c.want {
			t.Errorf("%s %s:\n got %s\nwant %s", c.method, c.path, resultOf(t, res.body), c.want)
		}
	}
	// Request MapPack contracts.
	if r := f.requests("SET_CONFIGURE_SERVER"); len(r) < 1 || mapText(r[0].Pack, "setConfig") != `a=c:\\x` {
		t.Errorf("setConfig escaping: %+v", r)
	}
	if r := f.requests("OBJECT_THREAD_CONTROL"); len(r) != 1 || mapText(r[0].Pack, "action") != "interrupt" || mapInt(r[0].Pack, "id") != 5 {
		t.Errorf("thread control param %+v", r)
	}
	if r := f.requests("OBJECT_THREAD_DETAIL"); len(r) != 1 || mapInt(r[0].Pack, "txid") != 10 {
		t.Errorf("thread detail txid %+v", r)
	}
	if r := f.requests("COUNTER_REAL_TIME_ALL_MULTI"); len(r) != 2 {
		t.Errorf("realtime requests %d", len(r))
	} else {
		if l, _ := mapList(r[0].Pack, "counter"); len(l) != 2 || l[0].Text != "TPS" || l[1].Text != "Cpu" {
			t.Errorf("counter HashSet order %+v", l)
		}
		if mapText(r[1].Pack, "objType") != "" || r[1].Pack.Entries[0].Key != "objType" {
			t.Errorf("objType null -> empty Text first: %+v", r[1].Pack.Entries)
		}
	}
	past := f.requests("COUNTER_PAST_TIME_ALL")
	if len(past) < 3 || mapInt(past[1].Pack, "stime") != 1760484600000 || mapInt(past[1].Pack, "etime") != 1760486400000-1000 ||
		mapInt(past[2].Pack, "stime") != 1760486400000+1000 || past[1].Pack.Entries[0].Key != "objHash" {
		t.Errorf("day split %+v", past)
	}
	latest := f.requests("COUNTER_PAST_TIME_ALL")
	last := latest[len(latest)-1].Pack
	if d := mapInt(last, "etime") - mapInt(last, "stime"); d > 60000 || d < 59000 {
		t.Errorf("latest range %d", d)
	}
	if r := f.requests("ALERT_REAL_TIME"); len(r) != 1 || mapText(r[0].Pack, "objType") != "tomcat" {
		t.Errorf("alert param %+v", r)
	}
	if r := f.requests("SET_CONFIGURE_WAS"); len(r) < 2 || mapInt(r[len(r)-1].Pack, "objHash") != 100 || !strings.Contains(mapText(r[len(r)-1].Pack, "setConfig"), "k=2\n") {
		t.Errorf("kv ofType %+v", r)
	}
	if r := f.requests("SAVE_ALERT_SCRIPTING_CONFIG_CONTETNS"); len(r) != 1 {
		t.Errorf("unchanged config must not be saved: %d", len(r))
	}
}

func itoa32(n int32) string { b, _ := appendJSON(nil, n); return strings.Trim(string(b), `"`) }

func TestValidationAndNotImplemented(t *testing.T) {
	f := newFakeCollector(t)
	setupEndpoints(f)
	cfg := testConfig(f)
	unsupported := map[string]bool{"OBJECT_REMOVE_INACTIVE": true, "SET_CONFIGURE_SERVER": true, "OBJECT_THREAD_CONTROL": true, "GET_PLUGIN_HELPER_DESC": true,
		"SAVE_ALERT_SCRIPTING_CONTETNS": true, "GET_ALERT_SCRIPT_LOAD_MESSAGE": true, "SET_CONFIGURE_WAS": true}
	cfg.Supported = func(c string) bool { return !unsupported[c] }
	_, hs := newTestServer(t, cfg)
	for _, c := range []struct{ method, path, body string }{
		{"GET", "/scouter/v1/object/remove/inactive", ""},
		{"GET", "/scouter/v1/object/remove/inactive/server", ""},
		{"POST", "/scouter/v1/configure/set/server", `{"values":"x"}`},
		{"POST", "/scouter/v1/configure/set/object/1", `{"values":"x"}`},
		{"PUT", "/scouter/v1/configure/set/kv/server", `{"key":"a","value":"b"}`},
		{"PUT", "/scouter/v1/configure/set/kv/ofType/tomcat", `{"key":"a","value":"b"}`},
		{"GET", "/scouter/v1/activeService/control/thread/1/ofObject/1?action=x", ""},
		{"GET", "/scouter/v1/alert/scripting/TPS", ""},
		{"POST", "/scouter/v1/alert/set/rule/scripting/TPS", `{"values":"x"}`},
		{"GET", "/scouter/v1/alert/read/scripting/0/0", ""},
	} {
		res := doHTTP(t, nil, c.method, hs.URL+c.path, c.body, nil)
		if res.status != 501 || !strings.Contains(res.body, `"status":"501"`) || !strings.Contains(res.body, `"resultCode":"501","message":"This API is not yet implemented.","result":null}`) {
			t.Errorf("%s %s: %d %s", c.method, c.path, res.status, res.body)
		}
	}
	for _, c := range []struct {
		path   string
		status int
		msg    string
	}{
		{"/scouter/v1/counter/TPS/ofType/tomcat", 400, "fail to validate input parameters. : startTimeMillis and endTimeMillis must have value!"},
		{"/scouter/v1/counter/TPS/ofType/tomcat?startTimeMillis=1&endTimeMillis=21600002", 400, "fail to validate input parameters. : query range should be lower than 21600 seconds!"},
		{"/scouter/v1/counter/TPS/ofType/tomcat?startYmdHms=20260101000000", 400, "fail to validate input parameters. : startYmdHms and endYmdHms should be not null !"},
		{"/scouter/v1/counter/TPS/ofType/tomcat?startYmdHms=20260101000000&endYmdHms=20260101000001&startTimeMillis=5", 400, "fail to validate input parameters. : startYmdHms, endYmdHms and startTimeMillis, endTimeMills must not coexist!"},
		{"/scouter/v1/counter/TPS/ofType/tomcat?startYmdHms=2026&endYmdHms=20260101000001", 400, "fail to validate input parameters. : startYmdHms is not yyyyMMddHHmmss: 2026"},
		{"/scouter/v1/counter/TPS/latest/3601/ofType/tomcat", 400, "fail to validate input parameters. : query range should be lower than 3600 seconds!"},
		{"/scouter/v1/counter/TPS?startTimeMillis=1&endTimeMillis=2", 400, "[ValidationException] retrieveCounterByObjHashes.arg0.objHashes may not be null"},
		{"/scouter/v1/counter/realTime/TPS?objHashes=1,x", 400, `fail to validate input parameters. : objHashes has an invalid integer \"x\"`},
		{"/scouter/v1/alert/realTime/-1/0", 400, "[ValidationException] offset must be greater than or equal to 0"},
		{"/scouter/v1/activeService/control/thread/1/ofObject/1", 400, "[ValidationException] InterruptedRealTimeActiveServiceByObjHash.arg2 may not be null"},
		{"/scouter/v1/object/threadList/abc", 400, `fail to validate input parameters. : objHash is not an int: \"abc\"`},
		{"/scouter/v1/object?serverId=x", 400, `fail to validate input parameters. : serverId is not an int: \"x\"`},
		{"/scouter/v1/nope", 404, "HTTP 404 Not Found"},
		{"/scouter/v1/info/counter-model?x", 200, ""},
	} {
		res := doHTTP(t, nil, "GET", hs.URL+c.path, "", nil)
		if res.status != c.status {
			t.Errorf("%s: %d %s", c.path, res.status, res.body)
			continue
		}
		if c.msg != "" {
			want := `"resultCode":"` + itoa32(int32(c.status)) + `","message":"` + c.msg + `","result":null}`
			if !strings.HasSuffix(res.body, want) {
				t.Errorf("%s:\n got %s\nwant suffix %s", c.path, res.body, want)
			}
		}
	}
	res := doHTTP(t, nil, "POST", hs.URL+"/scouter/v1/object", "", nil)
	if res.status != 405 || !strings.Contains(res.body, `"message":"HTTP 405 Method Not Allowed"`) {
		t.Errorf("405: %d %s", res.status, res.body)
	}
	res = doHTTP(t, nil, "POST", hs.URL+"/scouter/v1/configure/set/server", `{}`, nil)
	if res.status != 501 {
		t.Errorf("unsupported checked before body: %d %s", res.status, res.body)
	}
	if res := doHTTP(t, nil, "GET", hs.URL+"/scouter/swagger.json", "", nil); res.status != 404 {
		t.Errorf("swagger %d", res.status)
	}
	if res := doHTTP(t, nil, "GET", hs.URL+"/swagger", "", nil); res.status != 404 {
		t.Errorf("/swagger %d", res.status)
	}
	if res := doHTTP(t, nil, "GET", hs.URL+"/scouter/v0/temp/test", "", nil); res.status != 404 {
		t.Errorf("temp test %d", res.status)
	}
}

func TestFiltersCORSNoCacheRequestIDGzip(t *testing.T) {
	f := newFakeCollector(t)
	setupEndpoints(f)
	_, hs := newTestServer(t, testConfig(f))
	res := doHTTP(t, nil, "OPTIONS", hs.URL+"/scouter/v1/object", "", map[string]string{"Origin": "http://ui.example"})
	if res.status != 200 || res.body != "" || res.header.Get("Access-Control-Allow-Origin") != "http://ui.example" ||
		res.header.Get("Access-Control-Allow-Credentials") != "true" || res.header.Get("Access-Control-Max-Age") != "600" ||
		res.header.Get("Access-Control-Allow-Headers") != "Content-Type, Authorization" ||
		res.header.Get("Access-Control-Allow-Methods") != "POST, GET, OPTIONS, PUT, DELETE, PATCH" || res.header.Get("Cache-Control") != "" {
		t.Fatalf("preflight %+v", res)
	}
	res = doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/object", "", nil)
	if res.header.Get("Cache-Control") != "no-cache" || res.header.Get("Expires") != "0" || res.header.Get("Pragma") != "No-cache" || res.header.Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("headers %+v", res.header)
	}
	ids := regexp.MustCompile(`"requestId":"(#[0-9a-v]+?)(\d+)"`).FindStringSubmatch(res.body)
	res2 := doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/object", "", nil)
	ids2 := regexp.MustCompile(`"requestId":"(#[0-9a-v]+?)(\d+)"`).FindStringSubmatch(res2.body)
	if ids == nil || ids2 == nil || ids[1]+ids[2] == ids2[1]+ids2[2] {
		t.Fatalf("requestId %v %v", ids, ids2)
	}

	cfg := testConfig(f)
	cfg.CORSAllowOrigin = ""
	_, plain := newTestServer(t, cfg)
	res = doHTTP(t, nil, "OPTIONS", plain.URL+"/scouter/v1/object", "", map[string]string{"Origin": "http://ui.example"})
	if res.status != 200 || res.header.Get("Access-Control-Allow-Origin") != "" || !strings.Contains(res.header.Get("Allow"), "GET") {
		t.Fatalf("OPTIONS without CORS %+v", res)
	}

	// gzip: transport auto-decompression is disabled to see the encoding.
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	req, _ := http.NewRequest("GET", hs.URL+"/scouter/v1/configure/server", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	f.static("CONFIGURE_DESC", newMap(entry("a", textValue(strings.Repeat("long description ", 100)))))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("large body not gzipped: %v", resp.Header)
	}
	zr, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(zr)
	if !strings.Contains(string(body), "long description") {
		t.Fatal("gzip body")
	}
	req, _ = http.NewRequest("GET", hs.URL+"/scouter/v1/object/remove/inactive", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp2, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.Header.Get("Content-Encoding") != "" {
		t.Fatal("small body gzipped")
	}
}

func TestInfoServer(t *testing.T) {
	f := newFakeCollector(t)
	_, hs := newTestServer(t, testConfig(f))
	res := doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/info/server", "", nil)
	port := f.addr()[len("127.0.0.1:"):]
	want := `[{"id":"` + itoa32(javaHash("127.0.0.1"+port)) + `","name":"fake-server","connected":true,"serverTime":"`
	if res.status != 200 || !strings.HasPrefix(resultOf(t, res.body), want) || !strings.HasSuffix(resultOf(t, res.body), `","version":"fake 1.0"}]`) {
		t.Fatal(res.body)
	}
	// Unreachable collector: still 200 with connected=false.
	cfg := testConfig(f)
	cfg.CollectorAddr = "127.0.0.1:1"
	_, down := newTestServer(t, cfg)
	res = doHTTP(t, nil, "GET", down.URL+"/scouter/v1/info/server", "", nil)
	if res.status != 200 || !strings.Contains(res.body, `"name":"127.0.0.1:1","connected":false`) {
		t.Fatal(res.body)
	}
}

// A 6-hour counter range answers 10,800 2s points per object, past the
// request decoder's 4096 entries: the response is read whole.
func TestCounterRangeLargeResponse(t *testing.T) {
	f := newFakeCollector(t)
	setupEndpoints(f)
	const points = 10800
	f.handle("COUNTER_PAST_TIME_ALL", bodyPack, func(r fakeReq) ([]any, error) {
		st := mapInt(r.Pack, "stime")
		times, values := make([]int64, points), make([]int64, points)
		for i := range times {
			times[i], values[i] = st+int64(i)*2000, 1
		}
		return []any{newMap(entry("objHash", decimalValue(100)), entry("time", decs(times...)), entry("value", decs(values...)))}, nil
	})
	_, hs := newTestServer(t, testConfig(f))
	res := doHTTP(t, nil, "GET", hs.URL+"/scouter/v1/counter/TPS/ofType/tomcat?startTimeMillis=1760486400000&endTimeMillis=1760508000000", "", nil)
	if res.status != 200 || strings.Count(res.body, `"1.0"`)+strings.Count(res.body, `"1"`) < points {
		t.Fatal(res.status, len(res.body), res.body[:min(len(res.body), 300)])
	}
}

// CallTimeout bounds each wait for the collector (Java SO_TIMEOUT), not the
// whole command: an answer whose items keep arriving completes past it, and
// a collector that stops answering fails after it.
func TestCallTimeoutPerResponseItem(t *testing.T) {
	f := newFakeCollector(t)
	setupEndpoints(f)
	item := func(st int64) any {
		return newMap(entry("objHash", decimalValue(100)), entry("time", decs(st)), entry("value", decs(1)))
	}
	var stall atomic.Int64 // nanoseconds before the first item
	f.handle("COUNTER_PAST_TIME_ALL", bodyPack, func(r fakeReq) ([]any, error) {
		st := mapInt(r.Pack, "stime")
		if d := time.Duration(stall.Load()); d > 0 {
			return []any{d, item(st)}, nil
		}
		out := []any{}
		for i := int64(0); i < 6; i++ {
			out = append(out, item(st+i*1000), 150*time.Millisecond)
		}
		return out, nil
	})
	cfg := testConfig(f)
	cfg.CallTimeout = 400 * time.Millisecond
	_, hs := newTestServer(t, cfg)
	url := hs.URL + "/scouter/v1/counter/TPS/ofType/tomcat?startTimeMillis=1760486400000&endTimeMillis=1760486500000"
	start := time.Now()
	res := doHTTP(t, nil, "GET", url, "", nil)
	if res.status != 200 || time.Since(start) < cfg.CallTimeout {
		t.Fatal("streaming answer", res.status, time.Since(start), res.body)
	}
	stall.Store(int64(2 * cfg.CallTimeout))
	if res = doHTTP(t, nil, "GET", url, "", nil); res.status == 200 {
		t.Fatal("stalled collector answered", res.body)
	}
}
