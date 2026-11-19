package webapp

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"unicode"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// ObjectController (layer/controller/ObjectController.java:47-206,
// layer/consumer/ObjectConsumer.java).

type sObject struct {
	ObjType        string `json:"objType"`
	ObjFamily      string `json:"objFamily"`
	ObjHash        int32  `json:"objHash"`
	ObjName        string `json:"objName"`
	Address        string `json:"address"`
	Version        string `json:"version"`
	Alive          bool   `json:"alive"`
	LastWakeUpTime int64  `json:"lastWakeUpTime"`
	Tags           any    `json:"tags"`
}

// GET /v1/object: OBJECT_LIST_REAL_TIME (no body) -> SObject list.
func (s *Server) objectList(w http.ResponseWriter, r *http.Request) {
	if err := serverIDParam(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	var list []*protocol.ObjectPack
	err := s.callNoBody(r.Context(), "OBJECT_LIST_REAL_TIME", func(it Item) error {
		o, ok := it.Pack.(*protocol.ObjectPack)
		if !ok {
			return fmt.Errorf("OBJECT_LIST_REAL_TIME: item is %T, want ObjectPack", it.Pack)
		}
		list = append(list, o)
		return nil
	})
	var engine *counterEngine
	if err == nil {
		engine, err = s.counterEngine(r.Context())
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	out := make([]sObject, 0, len(list))
	for _, o := range list {
		out = append(out, sObject{ObjType: o.ObjType, ObjFamily: engine.familyName(o.ObjType), ObjHash: o.ObjHash, ObjName: o.ObjName,
			Address: o.Address, Version: o.Version, Alive: o.Alive, LastWakeUpTime: o.Wakeup, Tags: tagsObject(o.Tags)})
	}
	s.writeJSON(w, r, out)
}

// tagsObject is MapValue.toMap -> toJavaObject; a non-map tag value is {}.
func tagsObject(v protocol.Value) jsonObject {
	if v.Type != protocol.ValueMap {
		return jsonObject{}
	}
	return entriesObject(v.Entries)
}

// GET /v1/object/remove/inactive and /remove/inactive/server:
// OBJECT_REMOVE_INACTIVE (no body) on the single collector.
func (s *Server) objectRemoveInactive(w http.ResponseWriter, r *http.Request) {
	if err := serverIDParam(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	if err := s.callNoBody(r.Context(), "OBJECT_REMOVE_INACTIVE", func(Item) error { return nil }); err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, true)
}

// objHashRequest parses path objHash and the serverId query.
func objHashRequest(r *http.Request) (int32, error) {
	if err := serverIDParam(r); err != nil {
		return 0, err
	}
	return pathInt32(r, "objHash")
}

func objHashParam(hash int32) *protocol.MapPack {
	return newMap(entry("objHash", decimalValue(int64(hash))))
}

// mustSingle is getSingle where the caller dereferences the result (Java
// NPE -> 500 for a missing response).
func (s *Server) mustSingle(ctx context.Context, cmd string, param protocol.Pack) (*protocol.MapPack, error) {
	m, err := s.single(ctx, cmd, param)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("%s returned no response", cmd)
	}
	return m, nil
}

type threadObjectData struct {
	ID      int64 `json:"id"`
	Name    any   `json:"name"`
	Stat    any   `json:"stat"`
	CPU     int64 `json:"cpu"`
	Txid    any   `json:"txid"`
	Elapsed any   `json:"elapsed"`
	Service any   `json:"service"`
}

// GET /v1/object/threadList/{objHash}: OBJECT_THREAD_LIST.
func (s *Server) objectThreadList(w http.ResponseWriter, r *http.Request) {
	out, err := s.threadList(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, out)
}

func (s *Server) threadList(r *http.Request) ([]threadObjectData, error) {
	const cmd = "OBJECT_THREAD_LIST"
	hash, err := objHashRequest(r)
	if err != nil {
		return nil, err
	}
	m, err := s.mustSingle(r.Context(), cmd, objHashParam(hash))
	if err != nil {
		return nil, err
	}
	keys := []string{"id", "name", "stat", "cpu", "txid", "elapsed", "service"}
	lists, err := requireLists(cmd, m, keys...)
	if err != nil {
		return nil, err
	}
	out := []threadObjectData{}
	for i := range lists[0] {
		vals := make([]protocol.Value, len(keys))
		for k := range keys {
			if vals[k], err = listAt(cmd, keys[k], lists[k], i); err != nil {
				return nil, err
			}
		}
		out = append(out, threadObjectData{ID: numInt64(vals[0]), Name: valueString(vals[1]), Stat: valueString(vals[2]),
			CPU: numInt64(vals[3]), Txid: valueString(vals[4]), Elapsed: valueString(vals[5]), Service: valueString(vals[6])})
	}
	return out, nil
}

// GET /v1/object/threadDump/{objHash}: OBJECT_THREAD_DUMP list "threadDump"
// joined with "\n" after every line.
func (s *Server) objectThreadDump(w http.ResponseWriter, r *http.Request) {
	const cmd = "OBJECT_THREAD_DUMP"
	hash, err := objHashRequest(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	m, err := s.single(r.Context(), cmd, objHashParam(hash))
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	var sb strings.Builder
	if m != nil {
		lists, err := requireLists(cmd, m, "threadDump")
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		for _, v := range lists[0] {
			line, _ := valueString(v).(string)
			if valueString(v) == nil {
				line = "null"
			}
			sb.WriteString(line)
			sb.WriteByte('\n')
		}
	}
	s.writeJSON(w, r, sb.String())
}

type heapHistogramData struct {
	No    int32  `json:"no"`
	Count int32  `json:"count"`
	Size  int64  `json:"size"`
	Name  string `json:"name"`
}

// GET /v1/object/heapHistogram/{objHash}: OBJECT_HEAPHISTO list "heaphisto".
func (s *Server) objectHeapHistogram(w http.ResponseWriter, r *http.Request) {
	const cmd = "OBJECT_HEAPHISTO"
	hash, err := objHashRequest(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	m, err := s.mustSingle(r.Context(), cmd, objHashParam(hash))
	var lists [][]protocol.Value
	if err == nil {
		lists, err = requireLists(cmd, m, "heaphisto")
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	out := []heapHistogramData{}
	for _, v := range lists[0] {
		line, _ := valueString(v).(string)
		tokens := strings.Fields(line)
		if len(tokens) < 4 {
			continue
		}
		out = append(out, heapHistogramData{No: javaCint(digitsOnly(tokens[0])), Count: javaCint(tokens[1]),
			Size: javaClong(tokens[2]), Name: canonicalName(tokens[3])})
	}
	s.writeJSON(w, r, out)
}

func digitsOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// javaCint/javaClong are CastUtil.cint/clong on a String (parse failure 0).
func javaCint(s string) int32 { return parseIntOrZero32(s) }

func parseIntOrZero32(s string) int32 {
	var n int64
	neg := false
	if s == "" {
		return 0
	}
	i := 0
	if s[0] == '-' || s[0] == '+' {
		neg = s[0] == '-'
		i = 1
		if len(s) == 1 {
			return 0
		}
	}
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0
		}
		n = n*10 + int64(s[i]-'0')
		if n > 1<<31 {
			return 0
		}
	}
	if neg {
		n = -n
	}
	if n > 1<<31-1 || n < -(1<<31) {
		return 0
	}
	return int32(n)
}

func javaClong(s string) int64 {
	var n int64
	neg := false
	if s == "" {
		return 0
	}
	i := 0
	if s[0] == '-' || s[0] == '+' {
		neg = s[0] == '-'
		i = 1
		if len(s) == 1 {
			return 0
		}
	}
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0
		}
		d := int64(s[i] - '0')
		if n > (1<<63-1-d)/10 {
			return 0
		}
		n = n*10 + d
	}
	if neg {
		return -n
	}
	return n
}

// canonicalName is ObjectConsumer.getCanonicalName (JVM descriptor -> name).
func canonicalName(className string) string {
	if className == "" {
		return className
	}
	arrays := 0
	var sb strings.Builder
scan:
	for offset := 0; offset < len(className); offset++ {
		c := className[offset]
		if c == '[' {
			arrays++
			continue
		}
		switch c {
		case 'L':
			if strings.HasSuffix(className, ";") && len(className)-1 >= offset+1 {
				sb.WriteString(className[offset+1 : len(className)-1])
			} else {
				sb.WriteString(className[offset+1:])
			}
		case 'V':
			sb.WriteString("void")
		case 'Z':
			sb.WriteString("boolean")
		case 'C':
			sb.WriteString("char")
		case 'B':
			sb.WriteString("byte")
		case 'S':
			sb.WriteString("short")
		case 'I':
			sb.WriteString("int")
		case 'F':
			sb.WriteString("float")
		case 'J':
			sb.WriteString("long")
		case 'D':
			sb.WriteString("double")
		default:
			sb.WriteString(className)
		}
		break scan
	}
	for ; arrays > 0; arrays-- {
		sb.WriteString("[]")
	}
	return sb.String()
}

type variableData struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
}

// GET /v1/object/env/{objHash}: OBJECT_ENV {objHash, userSession, userIp};
// a missing response is result null.
func (s *Server) objectEnv(w http.ResponseWriter, r *http.Request) {
	hash, err := objHashRequest(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	session, err := s.session(r.Context())
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.login.mu.Lock()
	ip := s.login.localIP
	s.login.mu.Unlock()
	param := newMap(entry("objHash", decimalValue(int64(hash))), entry("userSession", decimalValue(session)), entry("userIp", textValue(ip)))
	m, err := s.single(r.Context(), "OBJECT_ENV", param)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	if m == nil {
		s.writeJSON(w, r, nil)
		return
	}
	out := []variableData{}
	for _, e := range m.Entries {
		out = append(out, variableData{Name: e.Key, Value: cString(e.Value)})
	}
	s.writeJSON(w, r, out)
}

type socketObjectData struct {
	Key     int64  `json:"key"`
	Host    string `json:"host"`
	Port    int32  `json:"port"`
	Count   int64  `json:"count"`
	Service string `json:"service"`
	Txid    int64  `json:"txid"`
	Standby bool   `json:"standby"`
	Stack   any    `json:"stack"`
}

// GET /v1/object/socket/{objHash}: OBJECT_SOCKET (literal "SOCKET").
func (s *Server) objectSocket(w http.ResponseWriter, r *http.Request) {
	const cmd = "SOCKET"
	hash, err := objHashRequest(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	m, err := s.mustSingle(r.Context(), cmd, objHashParam(hash))
	keys := []string{"key", "host", "port", "count", "service", "txid", "order", "stack"}
	var lists [][]protocol.Value
	if err == nil {
		lists, err = requireLists(cmd, m, keys...)
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	today := s.today()
	out := []socketObjectData{}
	for i := range lists[0] {
		vals := make([]protocol.Value, len(keys))
		for k := range keys {
			if vals[k], err = listAt(cmd, keys[k], lists[k], i); err != nil {
				s.writeErr(w, r, err)
				return
			}
		}
		if vals[1].Type != protocol.ValueBlob {
			s.writeErr(w, r, fmt.Errorf("%s host[%d] is not a Blob", cmd, i))
			return
		}
		stack := valueString(vals[7])
		out = append(out, socketObjectData{Key: numInt64(vals[0]), Host: ipString(vals[1].Bytes), Port: numInt32(vals[2]),
			Count: int64(numInt32(vals[3])), Service: s.text(r.Context(), today, "service", numInt32(vals[4])),
			Txid: numInt64(vals[5]), Standby: vals[6].Type == protocol.ValueBoolean && vals[6].Bool, Stack: stack})
	}
	s.writeJSON(w, r, out)
}

// ipString is IPUtil.toString: the first four bytes dotted, "0.0.0.0" when
// fewer than four.
func ipString(b []byte) string {
	if len(b) < 4 {
		return "0.0.0.0"
	}
	return fmt.Sprintf("%d.%d.%d.%d", b[0], b[1], b[2], b[3])
}
