package webapp

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// AlertController (AlertController.java:39-114, AlertConsumer.java,
// AlertScriptingConsumer.java).

type sAlert struct {
	Time    int64      `json:"time"`
	ObjType string     `json:"objType"`
	ObjHash int32      `json:"objHash"`
	ObjName string     `json:"objName"`
	Level   string     `json:"level"`
	Title   string     `json:"title"`
	Message string     `json:"message"`
	TagMap  jsonObject `json:"tagMap"`
}

type realTimeAlertView struct {
	Offset1 int64    `json:"offset1"`
	Offset2 int32    `json:"offset2"`
	Alerts  []sAlert `json:"alerts"`
}

var alertLevels = []string{"INFO", "WARN", "ERROR", "FATAL"}

// alertLevel is AlertLevelEnum.of (unknown -> INFO).
func alertLevel(b byte) string {
	if int(b) < len(alertLevels) {
		return alertLevels[b]
	}
	return "INFO"
}

// GET /v1/alert/realTime/{offset1}/{offset2}?objType=: ALERT_REAL_TIME
// {loop, index, objType} -> meta MapPack + AlertPack stream.
func (s *Server) alertRealTime(w http.ResponseWriter, r *http.Request) {
	err := serverIDParam(r)
	var loop int64
	var index int32
	if err == nil {
		loop, err = pathInt64(r, "offset1")
	}
	if err == nil {
		index, err = pathInt32(r, "offset2")
	}
	if err == nil && (loop < 0 || index < 0) {
		err = newAPIError(http.StatusBadRequest, http.StatusBadRequest, "[ValidationException] offset must be greater than or equal to 0")
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	param := newMap(entry("loop", decimalValue(loop)), entry("index", decimalValue(int64(index))), entry("objType", textValue(r.URL.Query().Get("objType"))))
	view := realTimeAlertView{Alerts: []sAlert{}}
	var alerts []*protocol.AlertPack
	err = s.call(r.Context(), "ALERT_REAL_TIME", param, func(it Item) error {
		switch p := it.Pack.(type) {
		case *protocol.MapPack:
			view.Offset1, view.Offset2 = mapInt(p, "loop"), mapInt32(p, "index")
		case *protocol.AlertPack:
			alerts = append(alerts, p)
		default:
			return fmt.Errorf("ALERT_REAL_TIME: unexpected %T", it.Pack)
		}
		return nil
	})
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	for _, a := range alerts {
		name, err := s.objName(r.Context(), a.ObjHash)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		view.Alerts = append(view.Alerts, sAlert{Time: a.Time, ObjType: a.ObjType, ObjHash: a.ObjHash, ObjName: name,
			Level: alertLevel(a.Level), Title: a.Title, Message: a.Message, TagMap: tagsObject(a.Tags)})
	}
	s.writeJSON(w, r, view)
}

const (
	defaultRuleContents   = "// void process(RealCounter $counter, PluginHelper $$) {\n// create your java code below..\n\n\n// }"
	defaultConfigContents = "#history_size=150\n#silent_time=300\n#check_term=20"
)

type apiDesc struct {
	Desc           any    `json:"desc"`
	MethodName     any    `json:"methodName"`
	ReturnTypeName any    `json:"returnTypeName"`
	FullSignature  string `json:"fullSignature"`
}

type scriptingLoadData struct {
	RuleText            string     `json:"ruleText"`
	ConfigText          string     `json:"configText"`
	ConfigHash          int32      `json:"configHash"`
	RuleTextHash        int32      `json:"ruleTextHash"`
	RealCounterDescMap  jsonObject `json:"realCounterDescMap"`
	PluginHelperDescMap jsonObject `json:"pluginHelperDescMap"`
}

// scriptText is StringUtil.emptyToDefault(getText("contents"), def).
func (s *Server) scriptText(ctx context.Context, cmd, counterName, def string) (string, error) {
	m, err := s.mustSingle(ctx, cmd, newMap(entry("counterName", textValue(counterName))))
	if err != nil {
		return "", err
	}
	if t := mapText(m, "contents"); t != "" {
		return t, nil
	}
	return def, nil
}

// apiDescMap is AlertScriptingConsumer.loadApiDesc into a TreeMap.
func (s *Server) apiDescMap(ctx context.Context, cmd, label string) (jsonObject, error) {
	byKey := map[string]apiDesc{}
	err := s.call(ctx, cmd, newMap(), func(it Item) error {
		m, ok := it.Pack.(*protocol.MapPack)
		if !ok {
			return fmt.Errorf("%s: item is %T, want MapPack", cmd, it.Pack)
		}
		params, ok := mapList(m, "parameterTypeNames")
		if !ok {
			return fmt.Errorf("%s: no parameterTypeNames", cmd)
		}
		names := make([]string, len(params))
		for i, p := range params {
			names[i], _ = valueToString(p).(string)
		}
		method := mapText(m, "methodName")
		d := apiDesc{Desc: mapTextNull(m, "desc"), MethodName: mapTextNull(m, "methodName"), ReturnTypeName: mapTextNull(m, "returnTypeName")}
		if d.MethodName == nil {
			method = "null"
		}
		d.FullSignature = label + "." + method + "(" + strings.Join(names, ", ") + ")"
		byKey[d.FullSignature] = d
		return nil
	})
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys) // TreeMap<String> natural order
	out := make(jsonObject, 0, len(keys))
	for _, k := range keys {
		out = append(out, jsonField{k, byKey[k]})
	}
	return out, nil
}

// GET /v1/alert/scripting/{counterName}. The desc maps need
// GET_ALERT_REAL_COUNTER_DESC/GET_PLUGIN_HELPER_DESC; when the collector lacks
// any of the four commands the whole endpoint is 501 rather than a partial
// object with empty maps.
func (s *Server) alertScripting(w http.ResponseWriter, r *http.Request) {
	err := serverIDParam(r)
	if err == nil {
		err = s.requireSupported("GET_ALERT_SCRIPTING_CONTETNS", "GET_ALERT_SCRIPTING_CONFIG_CONTETNS", "GET_ALERT_REAL_COUNTER_DESC", "GET_PLUGIN_HELPER_DESC")
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	ctx, name := r.Context(), r.PathValue("counterName")
	data := scriptingLoadData{}
	data.RuleText, err = s.scriptText(ctx, "GET_ALERT_SCRIPTING_CONTETNS", name, defaultRuleContents)
	if err == nil {
		data.ConfigText, err = s.scriptText(ctx, "GET_ALERT_SCRIPTING_CONFIG_CONTETNS", name, defaultConfigContents)
	}
	if err == nil {
		data.RealCounterDescMap, err = s.apiDescMap(ctx, "GET_ALERT_REAL_COUNTER_DESC", "$counter")
	}
	if err == nil {
		data.PluginHelperDescMap, err = s.apiDescMap(ctx, "GET_PLUGIN_HELPER_DESC", "$$")
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	data.ConfigHash, data.RuleTextHash = javaHash(data.ConfigText), javaHash(data.RuleText)
	s.writeJSON(w, r, data)
}

type scriptingSaveStateData struct {
	Status  int32  `json:"status"`
	Message string `json:"message"`
}

// saveScript is setConfigScripting/setRuleScripting: save only when the hash
// of the new text differs from the current (defaulted) text.
func (s *Server) saveScript(w http.ResponseWriter, r *http.Request, getCmd, saveCmd, def, method string) {
	err := serverIDParam(r)
	if err == nil {
		err = s.requireSupported(getCmd, saveCmd)
	}
	var values string
	if err == nil {
		values, err = s.readSetConfig(w, r, method)
	}
	name := r.PathValue("counterName")
	var current string
	if err == nil {
		current, err = s.scriptText(r.Context(), getCmd, name, def)
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	state := scriptingSaveStateData{Status: 301, Message: "No settings have been changed"}
	if javaHash(current) != javaHash(values) {
		param := newMap(entry("counterName", textValue(name)), entry("contents", textValue(values)))
		out, err := s.single(r.Context(), saveCmd, param)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		if out != nil && mapBool(out, "success") {
			state = scriptingSaveStateData{Status: 200, Message: "Settings save success"}
		} else {
			state = scriptingSaveStateData{Status: 404, Message: "Failed to save settings"}
		}
	}
	s.writeJSON(w, r, state)
}

// POST /v1/alert/set/config/scripting/{counterName} body {values}.
func (s *Server) alertSetConfig(w http.ResponseWriter, r *http.Request) {
	s.saveScript(w, r, "GET_ALERT_SCRIPTING_CONFIG_CONTETNS", "SAVE_ALERT_SCRIPTING_CONFIG_CONTETNS", defaultConfigContents, "saveConfigAlertScripting")
}

// POST /v1/alert/set/rule/scripting/{counterName} body {values}.
func (s *Server) alertSetRule(w http.ResponseWriter, r *http.Request) {
	s.saveScript(w, r, "GET_ALERT_SCRIPTING_CONTETNS", "SAVE_ALERT_SCRIPTING_CONTETNS", defaultRuleContents, "setRuleAlertScripting")
}

type scriptingLogStateData struct {
	Loop    int64    `json:"loop"`
	Index   int64    `json:"index"`
	Message []string `json:"message"`
}

// GET /v1/alert/read/scripting/{loop}/{index}: GET_ALERT_SCRIPT_LOAD_MESSAGE.
func (s *Server) alertReadScripting(w http.ResponseWriter, r *http.Request) {
	const cmd = "GET_ALERT_SCRIPT_LOAD_MESSAGE"
	err := serverIDParam(r)
	var loop, index int64
	if err == nil {
		loop, err = pathInt64(r, "loop")
	}
	if err == nil {
		index, err = pathInt64(r, "index")
	}
	var m *protocol.MapPack
	if err == nil {
		m, err = s.mustSingle(r.Context(), cmd, newMap(entry("loop", decimalValue(loop)), entry("index", decimalValue(index))))
	}
	var lists [][]protocol.Value
	if err == nil {
		lists, err = requireLists(cmd, m, "messages")
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	out := scriptingLogStateData{Loop: mapInt(m, "loop"), Index: int64(mapInt32(m, "index")), Message: []string{}}
	for _, v := range lists[0] {
		t, _ := valueToString(v).(string)
		out.Message = append(out.Message, t)
	}
	s.writeJSON(w, r, out)
}
