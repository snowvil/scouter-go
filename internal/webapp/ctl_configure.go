package webapp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// ConfigureController (ConfigureController.java:40-135, ConfigureConsumer.java,
// ConfigureKVHandleConsumer.java).

type confObject struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Def   string `json:"def"`
}

type configureData struct {
	Contents         any          `json:"contents"`
	ConfigStateList  []confObject `json:"configStateList"`
	DescMap          jsonObject   `json:"descMap"`
	ValueTypeMap     jsonObject   `json:"valueTypeMap"`
	ValueTypeDescMap jsonObject   `json:"valueTypeDescMap"`
	Status           int32        `json:"status"`
}

func newConfigureData() *configureData {
	return &configureData{Contents: "", ConfigStateList: []confObject{}, DescMap: jsonObject{}, ValueTypeMap: jsonObject{}, ValueTypeDescMap: jsonObject{}}
}

// put is LinkedHashMap.put: an existing key keeps its position.
func (o *jsonObject) put(key string, value any) {
	for i := range *o {
		if (*o)[i].Key == key {
			(*o)[i].Value = value
			return
		}
	}
	*o = append(*o, jsonField{key, value})
}

// removeVariableString drops $...$ sections from a key.
func removeVariableString(text string) string {
	var b strings.Builder
	sink := false
	for _, r := range text {
		if r == '$' {
			sink = !sink
			continue
		}
		if !sink {
			b.WriteRune(r)
		}
	}
	return b.String()
}

var valueTypeNames = map[int32]string{1: "VALUE", 2: "NUM", 3: "BOOL", 4: "COMMA_SEPARATED_VALUE", 5: "COMMA_COLON_SEPARATED_VALUE"}

func valueTypeName(n int32) string {
	if name, ok := valueTypeNames[n]; ok {
		return name
	}
	return "VALUE"
}

type valueTypeDesc struct {
	Strings   []any   `json:"strings"`
	Strings1  []any   `json:"strings1"`
	Strings2  []any   `json:"strings2"`
	Strings3  []any   `json:"strings3"`
	Booleans  []bool  `json:"booleans"`
	Booleans1 []bool  `json:"booleans1"`
	Ints      []int32 `json:"ints"`
	Ints1     []int32 `json:"ints1"`
}

// valueTypeDescOf is ValueTypeDesc.of(MapValue); a missing list is an error
// (Java NPE).
func valueTypeDescOf(v protocol.Value) (valueTypeDesc, error) {
	if v.Type != protocol.ValueMap {
		return valueTypeDesc{}, errors.New("CONFIGURE_VALUE_TYPE_DESC value is not a MapValue")
	}
	m := &protocol.MapPack{Entries: v.Entries}
	lists, err := requireLists("CONFIGURE_VALUE_TYPE_DESC", m, "strings", "strings1", "strings2", "strings3", "booleans", "booleans1", "ints", "ints1")
	if err != nil {
		return valueTypeDesc{}, err
	}
	strs := func(l []protocol.Value) []any {
		out := make([]any, len(l))
		for i, e := range l {
			out[i] = valueString(e)
		}
		return out
	}
	bools := func(l []protocol.Value) []bool {
		out := make([]bool, len(l))
		for i, e := range l {
			out[i] = e.Type == protocol.ValueBoolean && e.Bool
		}
		return out
	}
	ints := func(l []protocol.Value) []int32 {
		out := make([]int32, len(l))
		for i, e := range l {
			out[i] = numInt32(e)
		}
		return out
	}
	return valueTypeDesc{strs(lists[0]), strs(lists[1]), strs(lists[2]), strs(lists[3]), bools(lists[4]), bools(lists[5]), ints(lists[6]), ints(lists[7])}, nil
}

// loadConfigList is ConfigureConsumer.loadConfigList.
func (s *Server) loadConfigList(ctx context.Context, data *configureData, cmd string, param protocol.Pack) error {
	m, err := s.single(ctx, cmd, param)
	if err != nil || m == nil {
		return err
	}
	lists, err := requireLists(cmd, m, "key", "value", "default")
	if err != nil {
		return err
	}
	for i := range lists[0] {
		var row [3]string
		for k := range row {
			v, err := listAt(cmd, []string{"key", "value", "default"}[k], lists[k], i)
			if err != nil {
				return err
			}
			row[k], _ = cString(v).(string) // Strings.nullToEmpty
		}
		data.ConfigStateList = append(data.ConfigStateList, confObject{row[0], row[1], row[2]})
	}
	return nil
}

// loadConfigMeta fills descMap/valueTypeMap/valueTypeDescMap.
func (s *Server) loadConfigMeta(ctx context.Context, data *configureData, param *protocol.MapPack) error {
	m, err := s.single(ctx, "CONFIGURE_DESC", param)
	if err != nil {
		return err
	}
	if m != nil {
		for _, e := range m.Entries {
			text := mapTextNull(m, e.Key)
			data.DescMap.put(e.Key, text)
			if strings.Contains(e.Key, "$") {
				data.DescMap.put(removeVariableString(e.Key), text)
			}
		}
	}
	if m, err = s.single(ctx, "CONFIGURE_VALUE_TYPE", param); err != nil {
		return err
	}
	if m != nil {
		for _, e := range m.Entries {
			name := valueTypeName(mapInt32(m, e.Key))
			data.ValueTypeMap.put(e.Key, name)
			if strings.Contains(e.Key, "$") {
				data.ValueTypeMap.put(removeVariableString(e.Key), name)
			}
		}
	}
	if m, err = s.single(ctx, "CONFIGURE_VALUE_TYPE_DESC", param); err != nil {
		return err
	}
	if m != nil {
		for _, e := range m.Entries {
			v, _ := mapGet(m, e.Key)
			desc, err := valueTypeDescOf(v)
			if err != nil {
				return err
			}
			data.ValueTypeDescMap.put(e.Key, desc)
			if strings.Contains(e.Key, "$") {
				data.ValueTypeDescMap.put(removeVariableString(e.Key), desc)
			}
		}
	}
	return nil
}

// GET /v1/configure/server: GET_CONFIGURE_SERVER and LIST_CONFIGURE_SERVER
// without body, then CONFIGURE_DESC/VALUE_TYPE/VALUE_TYPE_DESC with {}.
func (s *Server) configureServer(w http.ResponseWriter, r *http.Request) {
	if err := serverIDParam(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	ctx := r.Context()
	data := newConfigureData()
	m, err := s.single(ctx, "GET_CONFIGURE_SERVER", nil)
	if err == nil && m != nil {
		data.Contents = mapTextNull(m, "serverConfig")
	}
	if err == nil {
		err = s.loadConfigList(ctx, data, "LIST_CONFIGURE_SERVER", nil)
	}
	if err == nil {
		err = s.loadConfigMeta(ctx, data, newMap())
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	data.Status = 200
	s.writeJSON(w, r, data)
}

// GET /v1/configure/object/{objHash}: the same commands with {objHash}
// (GET/LIST_CONFIGURE_WAS relayed to the agent).
func (s *Server) configureObject(w http.ResponseWriter, r *http.Request) {
	hash, err := objHashRequest(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	ctx := r.Context()
	data := newConfigureData()
	param := objHashParam(hash)
	m, err := s.single(ctx, "GET_CONFIGURE_WAS", param)
	if err == nil && m != nil {
		data.Contents = mapTextNull(m, "agentConfig")
	}
	if err == nil {
		err = s.loadConfigList(ctx, data, "LIST_CONFIGURE_WAS", param)
	}
	if err == nil {
		err = s.loadConfigMeta(ctx, data, param)
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	data.Status = 200
	s.writeJSON(w, r, data)
}

type setConfigRequest struct {
	Values *string `json:"values"`
}

func (s *Server) readSetConfig(w http.ResponseWriter, r *http.Request, method string) (string, error) {
	var req setConfigRequest
	if err := s.readBody(w, r, &req); err != nil {
		return "", err
	}
	if req.Values == nil {
		return "", newAPIError(http.StatusBadRequest, http.StatusBadRequest, "[ValidationException] "+method+".arg1.values may not be null")
	}
	return *req.Values, nil
}

// escapeSetConfig is values.replaceAll("\\\\", "\\\\\\\\"): each '\' doubled.
func escapeSetConfig(text string) string { return strings.ReplaceAll(text, `\`, `\\`) }

// requireSupported answers 501 before a mutation that the collector cannot run.
func (s *Server) requireSupported(commands ...string) error {
	if s.cfg.Supported == nil {
		return nil
	}
	for _, c := range commands {
		if !s.cfg.Supported(c) {
			return notImplemented(c)
		}
	}
	return nil
}

// saveConfig is ConfigureConsumer.saveServerConfig/saveObjectConfig.
func (s *Server) saveConfig(ctx context.Context, setCmd, listCmd string, values string, hash int32) (*configureData, error) {
	data := newConfigureData()
	param := newMap(entry("setConfig", textValue(escapeSetConfig(values))))
	if hash != 0 || setCmd == "SET_CONFIGURE_WAS" {
		param.Entries = append(param.Entries, entry("objHash", decimalValue(int64(hash))))
	}
	out, err := s.single(ctx, setCmd, param)
	if err != nil || out == nil {
		return data, err
	}
	if strings.EqualFold(mapText(out, "result"), "true") {
		var listParam protocol.Pack
		if setCmd == "SET_CONFIGURE_WAS" {
			listParam = objHashParam(hash)
		}
		if err := s.loadConfigList(ctx, data, listCmd, listParam); err != nil {
			return nil, err
		}
		data.Status = 200
	} else {
		data.Status = 404
	}
	return data, nil
}

// POST /v1/configure/set/server body {values}: SET_CONFIGURE_SERVER.
func (s *Server) configureSetServer(w http.ResponseWriter, r *http.Request) {
	err := serverIDParam(r)
	if err == nil {
		err = s.requireSupported("SET_CONFIGURE_SERVER", "LIST_CONFIGURE_SERVER")
	}
	var values string
	if err == nil {
		values, err = s.readSetConfig(w, r, "setServerConfig")
	}
	var data *configureData
	if err == nil {
		data, err = s.saveConfig(r.Context(), "SET_CONFIGURE_SERVER", "LIST_CONFIGURE_SERVER", values, 0)
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, data)
}

// POST /v1/configure/set/object/{objHash} body {values}: SET_CONFIGURE_WAS.
func (s *Server) configureSetObject(w http.ResponseWriter, r *http.Request) {
	hash, err := objHashRequest(r)
	if err == nil {
		err = s.requireSupported("SET_CONFIGURE_WAS", "LIST_CONFIGURE_WAS")
	}
	var values string
	if err == nil {
		values, err = s.readSetConfig(w, r, "setObjectConfig")
	}
	var data *configureData
	if err == nil {
		data, err = s.saveConfig(r.Context(), "SET_CONFIGURE_WAS", "LIST_CONFIGURE_WAS", values, hash)
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, data)
}

type setConfigKVRequest struct {
	Key   *string `json:"key"`
	Value *string `json:"value"`
}

func (s *Server) readSetConfigKV(w http.ResponseWriter, r *http.Request, method string) (string, string, error) {
	var req setConfigKVRequest
	if err := s.readBody(w, r, &req); err != nil {
		return "", "", err
	}
	if req.Key == nil {
		return "", "", newAPIError(http.StatusBadRequest, http.StatusBadRequest, "[ValidationException] "+method+".arg1.key may not be null")
	}
	if req.Value == nil {
		return "", "", newAPIError(http.StatusBadRequest, http.StatusBadRequest, "[ValidationException] "+method+".arg1.value may not be null")
	}
	return *req.Key, *req.Value, nil
}

// replaceConfigKey replaces the first "key\s*=..." line or appends
// "\n\n#auto-added\nkey=value\n". Go quotes the key and inserts the
// replacement literally (Java compiles the raw key as a regex and interprets
// $/\ in the value, W4).
func replaceConfigKey(text, key, value string) string {
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(key) + `\s*=.*\n?`)
	replacement := key + "=" + value + "\n"
	if loc := re.FindStringIndex(text); loc != nil {
		return text[:loc[0]] + replacement + text[loc[1]:]
	}
	return text + "\n\n#auto-added\n" + replacement
}

// loadConfigText is ConfigureKVHandleConsumer.loadConfigureText.
func (s *Server) loadConfigText(ctx context.Context, hash int32) (string, bool, error) {
	var m *protocol.MapPack
	var err error
	if hash == 0 {
		// Java sends an empty MapPack the collector never reads, leaving the
		// stream misaligned (W15); Go sends no body like retrieveServerConfig.
		m, err = s.single(ctx, "GET_CONFIGURE_SERVER", nil)
	} else {
		m, err = s.single(ctx, "GET_CONFIGURE_WAS", objHashParam(hash))
	}
	if err != nil || m == nil {
		return "", false, err
	}
	key := "agentConfig"
	if hash == 0 {
		key = "serverConfig"
	}
	v, ok := mapGet(m, key)
	if !ok || v.Type != protocol.ValueText {
		return "", false, nil
	}
	return v.Text, true, nil
}

// saveConfigText is ConfigureKVHandleConsumer.saveConfigure.
func (s *Server) saveConfigText(ctx context.Context, hash int32, text string) (bool, error) {
	param := newMap(entry("setConfig", textValue(escapeSetConfig(text))))
	cmd := "SET_CONFIGURE_SERVER"
	if hash != 0 {
		cmd = "SET_CONFIGURE_WAS"
		param.Entries = append(param.Entries, entry("objHash", decimalValue(int64(hash))))
	}
	out, err := s.single(ctx, cmd, param)
	if err != nil || out == nil {
		return false, err
	}
	return strings.EqualFold(mapText(out, "result"), "true"), nil
}

// applyConfigKV loads, edits and saves one config text. Java swallows every
// failure into false; Go keeps that except for NOT_IMPLEMENTED (checked
// before) and logs the cause.
func (s *Server) applyConfigKV(ctx context.Context, hash int32, key, value string) bool {
	text, ok, err := s.loadConfigText(ctx, hash)
	if err != nil || !ok {
		if err != nil {
			s.logf("webapp configure kv load %d: %v", hash, err)
		}
		return false
	}
	saved, err := s.saveConfigText(ctx, hash, replaceConfigKey(text, key, value))
	if err != nil {
		s.logf("webapp configure kv save %d: %v", hash, err)
		return false
	}
	return saved
}

// PUT /v1/configure/set/kv/server body {key, value} -> Boolean.
func (s *Server) configureSetKVServer(w http.ResponseWriter, r *http.Request) {
	err := serverIDParam(r)
	if err == nil {
		err = s.requireSupported("GET_CONFIGURE_SERVER", "SET_CONFIGURE_SERVER")
	}
	var key, value string
	if err == nil {
		key, value, err = s.readSetConfigKV(w, r, "setKVServerConfig")
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, s.applyConfigKV(r.Context(), 0, key, value))
}

type confObjectState struct {
	ObjHash int32 `json:"objHash"`
	Success bool  `json:"success"`
}

// PUT /v1/configure/set/kv/ofType/{objType} body {key, value}: every object
// of the type (dead ones false).
func (s *Server) configureSetKVType(w http.ResponseWriter, r *http.Request) {
	err := serverIDParam(r)
	if err == nil {
		err = s.requireSupported("OBJECT_LIST_REAL_TIME", "GET_CONFIGURE_WAS", "SET_CONFIGURE_WAS")
	}
	var key, value string
	if err == nil {
		key, value, err = s.readSetConfigKV(w, r, "setKVObjectConfig")
	}
	var list []*protocol.ObjectPack
	if err == nil {
		err = s.callNoBody(r.Context(), "OBJECT_LIST_REAL_TIME", func(it Item) error {
			o, ok := it.Pack.(*protocol.ObjectPack)
			if !ok {
				return fmt.Errorf("OBJECT_LIST_REAL_TIME: item is %T, want ObjectPack", it.Pack)
			}
			list = append(list, o)
			return nil
		})
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	objType := r.PathValue("objType")
	out := []confObjectState{}
	for _, o := range list {
		if o.ObjType != objType {
			continue
		}
		ok := o.Alive && s.applyConfigKV(r.Context(), o.ObjHash, key, value)
		out = append(out, confObjectState{o.ObjHash, ok})
	}
	s.writeJSON(w, r, out)
}
