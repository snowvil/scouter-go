// RealCounter/PluginHelper descriptor data: Copyright 2015 Scouter authors,
// Apache-2.0. Transcribed from the pinned source dee47fb5.
package service

// MethodDescriptor is RealCounter.Desc / PluginHelper.Desc: one public,
// non-static method without @Deprecated or @Internal. ParameterTypeNames is
// the single @ParamDesc string when present, else Class.getName of each
// parameter; Desc is @ConfigDesc.
type MethodDescriptor struct {
	Desc, MethodName, ReturnTypeName string
	ParameterTypeNames               []string
}

// Java builds these lists with Class.getDeclaredMethods(), whose order is
// unspecified (the server caches the first result). Go fixes the source
// declaration order of scouter.server/src/main/java/scouter/server/plugin/
// alert/RealCounter.java and plugin/PluginHelper.java. They describe the Java
// alert scripting API for the Client editor; Go does not execute scripts.
var realCounterDescriptors = []MethodDescriptor{
	{"get current counter name.", "getCounter", "java.lang.String", []string{}},
	{"get current object hash value.", "getObjHash", "int", []string{}},
	{"get current object name.", "getObjName", "java.lang.String", []string{}},
	{"get current object type name.", "getObjType", "java.lang.String", []string{}},
	{"get comma separated object hashes string of the given object type.", "getObjHashListString", "java.lang.String", []string{"java.lang.String"}},
	{"get comma separated object hashes string of the given object type.", "getObjHashListWithParentsString", "java.lang.String", []string{"java.lang.String"}},
	{"get value of current counter as int.", "getIntValue", "int", []string{}},
	{"get value of current counter as float.", "getFloatValue", "float", []string{}},
	{"get the history size of the counter.", "getHistorySize", "int", []string{}},
	{"get how many times exceed the given value of the counter.", "getOverCount", "int", []string{"int value, int sec"}},
	{"get how many times exceed the given value of the counter.", "getOverCount", "int", []string{"float value, int sec"}},
	{"get average value of the counter in the given duration as int.", "getAvgToInt", "int", []string{"int fromAgoSec, int durationSec"}},
	{"get latest average value of the counter in the given duration.", "getLatestAvg", "float", []string{"int durationSec"}},
	{"get latest average value of the counter in the given duration as int.", "getLatestAvgToInt", "int", []string{"int durationSec"}},
	{"get average value of the counter in the given duration.", "getAvg", "float", []string{"int fromAgoSec, int durationSec"}},
	{"get oldest time of the counter's history.", "getHistoryOldestTime", "long", []string{}},
	{"get how many values (of the current counter) exist in the seconds.", "getHistoryCountInSec", "int", []string{"int"}},
	{"get the counter's value as float.", "getFloatValue", "float", []string{"String counter"}},
	{"get the another objects counter's value as float.", "getFloatValue", "float", []string{"String objectFullName, String counter"}},
	{"get the counter's value as int.", "getIntValue", "int", []string{"String counter"}},
	{"get the another objects counter's value as int.", "getIntValue", "int", []string{"String objectFullName, String counter"}},
	{"alert on info level.", "info", "void", []string{"String title, String message"}},
	{"alert on warn level.", "warn", "void", []string{"String title, String message"}},
	{"alert on error level.", "error", "void", []string{"String title, String message"}},
	{"alert on fatal level.", "fatal", "void", []string{"String title, String message"}},
}

var pluginHelperDescriptors = []MethodDescriptor{
	{"logging", "log", "void", []string{"java.lang.Object"}},
	{"System.out.println", "println", "void", []string{"java.lang.Object"}},
	{"url encoding", "urlEncode", "java.lang.String", []string{"java.lang.String"}},
	{"url decoding", "urlDecode", "java.lang.String", []string{"java.lang.String"}},
	{"make shorten url.", "toShortenUrl", "java.lang.String", []string{"java.lang.String", "java.lang.String"}},
	{"get NumberFormatter set fraction 1", "getNumberFormatter", "java.text.NumberFormat", []string{}},
	{"get NumberFormatter set the fraction", "getNumberFormatter", "java.text.NumberFormat", []string{"int fractionDigits"}},
	{"format the number as fraction 1", "formatNumber", "java.lang.String", []string{"float"}},
	{"format the number as given fraction", "formatNumber", "java.lang.String", []string{"float f, int fractionDigits"}},
	{"format the number as fraction 1", "formatNumber", "java.lang.String", []string{"double"}},
	{"format the number as given fraction", "formatNumber", "java.lang.String", []string{"double v, int fractionDigits"}},
	{"format the number as fraction 1", "formatNumber", "java.lang.String", []string{"int"}},
	{"format the number as given fraction", "formatNumber", "java.lang.String", []string{"int v, int fractionDigits"}},
	{"format the number as fraction 1", "formatNumber", "java.lang.String", []string{"long"}},
	{"format the number as given fraction", "formatNumber", "java.lang.String", []string{"long v, int fractionDigits"}},
	{"alert as info level", "alertInfo", "void", []string{"int objHash, String objType, String title, String message"}},
	{"alert as warn level", "alertWarn", "void", []string{"int objHash, String objType, String title, String message"}},
	{"alert as error level", "alertError", "void", []string{"int objHash, String objType, String title, String message"}},
	{"alert as fatal level", "alertFatal", "void", []string{"int objHash, String objType, String title, String message"}},
	{"make hashMap easily", "toMap", "java.util.HashMap", []string{"String[] keyValueArray"}},
	{"request http get", "httpGet", "void", []string{"String url, Map<String, String> paramMap"}},
	{"request http get", "httpGet", "void", []string{"String url, Map<String, String> paramMap, int timeoutMillis"}},
}

// RealCounterDescriptors returns a copy of the GET_ALERT_REAL_COUNTER_DESC table.
func RealCounterDescriptors() []MethodDescriptor { return copyDescriptors(realCounterDescriptors) }

// PluginHelperDescriptors returns a copy of the GET_PLUGIN_HELPER_DESC table.
func PluginHelperDescriptors() []MethodDescriptor { return copyDescriptors(pluginHelperDescriptors) }

func copyDescriptors(in []MethodDescriptor) []MethodDescriptor {
	out := make([]MethodDescriptor, len(in))
	for i, d := range in {
		out[i] = d
		out[i].ParameterTypeNames = append([]string{}, d.ParameterTypeNames...)
	}
	return out
}
