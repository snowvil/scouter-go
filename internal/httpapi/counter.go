package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/telegraf"
)

// MaxJSONDepth bounds nested JSON in /counter and /register bodies.
const MaxJSONDepth = 64

var errJSON = errors.New("httpapi: invalid JSON body")

// parseJSON is json-simple JSONParser.parse over the whole body: exactly one
// value, numbers kept as json.Number. Integer literals (no '.', 'e', 'E') must
// fit in int64 like Long.valueOf; otherwise the body is a parse error.
func parseJSON(body string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errJSON
	}
	if err := checkJSON(v, 0); err != nil {
		return nil, err
	}
	return v, nil
}

func checkJSON(v any, depth int) error {
	if depth > MaxJSONDepth {
		return errJSON
	}
	switch x := v.(type) {
	case json.Number:
		if isIntegerLiteral(string(x)) {
			if _, err := strconv.ParseInt(string(x), 10, 64); err != nil {
				return errJSON
			}
		}
	case []any:
		for _, e := range x {
			if err := checkJSON(e, depth+1); err != nil {
				return err
			}
		}
	case map[string]any:
		for _, e := range x {
			if err := checkJSON(e, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func isIntegerLiteral(s string) bool { return !strings.ContainsAny(s, ".eE") }

// javaFloat is Number.floatValue for a json-simple Long or Double.
func javaFloat(n json.Number) float32 {
	s := string(n)
	if isIntegerLiteral(s) {
		i, _ := strconv.ParseInt(s, 10, 64)
		return float32(i)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return float32(math.NaN())
	}
	return float32(f)
}

// serveCounter is CounterServlet/CounterHandler. The answer is always 200 with
// an empty body. A JSON array is processed element by element; the first
// element that would throw in Java (wrong type, null name, bad value, address
// octet out of range) stops the remaining elements, while packs already
// handed to ingest stay.
func (a *API) serveCounter(w http.ResponseWriter, r *http.Request) {
	a.counterRequests.Add(1)
	body, ok := a.readBody(w, r)
	if !ok {
		return
	}
	v, err := parseJSON(body)
	if err != nil {
		a.counterParseErrors.Add(1)
		a.cfg.Logf("SC-8000 Http body parsing error: %v", err)
		w.WriteHeader(http.StatusOK)
		return
	}
	var elements []any
	switch x := v.(type) {
	case []any:
		elements = x
	case map[string]any:
		elements = []any{x}
	default:
		a.counterParseErrors.Add(1)
		a.cfg.Logf("SC-8001 Incorrect body")
		w.WriteHeader(http.StatusOK)
		return
	}
	for _, e := range elements {
		obj, isObject := e.(map[string]any)
		if !isObject {
			continue
		}
		if err := a.counterElement(r, obj); err != nil {
			a.counterElementErrors.Add(1)
			a.cfg.Logf("SC-8000 Http body parsing error: %v", err)
			break
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (a *API) counterElement(r *http.Request, element map[string]any) error {
	rawObject, present := element["object"]
	if !present || rawObject == nil {
		return nil
	}
	object, ok := rawObject.(map[string]any)
	if !ok {
		return fmt.Errorf("object is %T", rawObject)
	}
	host, err := optString(object, "host")
	if err != nil {
		return err
	}
	name, err := optString(object, "name")
	if err != nil {
		return err
	}
	objType, err := optString(object, "type")
	if err != nil {
		return err
	}
	address, err := optString(object, "address")
	if err != nil {
		return err
	}
	objName := "/" + javaNull(host)
	if name != nil && *name != "" {
		objName += "/" + *name
	}
	peer, ok := ipv4Address(address)
	if !ok {
		return fmt.Errorf("address %q has an octet outside 0..255", *address)
	}
	// Java ObjectPack field defaults (ObjectPack.java:58,62,66): alive=true,
	// wakeup=0, tags=empty MapValue; CounterHandler leaves them and sets no
	// version.
	pack := &protocol.ObjectPack{ObjHash: telegraf.Hash(objName), ObjName: objName, Alive: true, Tags: protocol.Value{Type: protocol.ValueMap}}
	if objType != nil {
		pack.ObjType = *objType
	}
	if address != nil {
		pack.Address = *address
	}
	a.acceptCounter(r, pack, peer)
	rawCounters, present := element["counters"]
	if !present || rawCounters == nil {
		return nil
	}
	counters, ok := rawCounters.([]any)
	if !ok {
		return fmt.Errorf("counters is %T", rawCounters)
	}
	perf := &protocol.PerfCounterPack{Time: a.cfg.Clock().UnixMilli(), TimeType: 1, ObjName: objName, Data: protocol.Value{Type: protocol.ValueMap}}
	index := make(map[string]int, min(len(counters), MaxCounterNames+1))
	for _, rc := range counters {
		c, ok := rc.(map[string]any)
		if !ok {
			return fmt.Errorf("counter is %T", rc)
		}
		cname, err := optString(c, "name")
		if err != nil {
			return err
		}
		if cname == nil {
			return errors.New("counter name is null")
		}
		n, ok := c["value"].(json.Number)
		if !ok {
			return fmt.Errorf("counter %q value is not a number", *cname)
		}
		v := protocol.Value{Type: protocol.ValueFloat, Float: javaFloat(n)}
		if i, dup := index[*cname]; dup {
			perf.Data.Entries[i].Value = v // MapValue.put keeps the first position
			continue
		}
		if len(perf.Data.Entries) == MaxCounterNames {
			// The pack could not be encoded; Java has no limit. Count it and
			// go on with the next element (Java would not throw here).
			a.tooMany.Add(1)
			a.cfg.Logf("SC-8000 /counter %s: more than %d counter names; PerfCounterPack not sent", objName, MaxCounterNames)
			return nil
		}
		index[*cname] = len(perf.Data.Entries)
		perf.Data.Entries = append(perf.Data.Entries, protocol.Entry{Key: *cname, Value: v})
	}
	a.acceptCounter(r, perf, peer)
	return nil
}

// MaxCounterNames bounds the distinct counter names of one /counter element:
// the protocol's default MapValue entry limit (4096) minus the objHash/time
// entries ingest adds (NetDataProcessor semantics).
const MaxCounterNames = 4096 - 2

func (a *API) acceptCounter(r *http.Request, p protocol.Pack, peer string) {
	if err := a.engine.AcceptFrom(r.Context(), p, peer); err != nil {
		a.counterAcceptErrors.Add(1)
		return
	}
	a.counterPacks.Add(1)
}

// optString is a (String) cast of a JSON member: absent or null is nil, a
// non-string is the Java ClassCastException.
func optString(m map[string]any, key string) (*string, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return nil, nil
	}
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("%s is %T, not a string", key, v)
	}
	return &s, nil
}

func javaNull(s *string) string {
	if s == nil {
		return "null"
	}
	return *s
}

// ipv4Address is InetAddress.getByAddress(IPUtil.toBytes(address)): a dotted
// IPv4 address, otherwise 0.0.0.0; ok=false is the Java NPE for an octet
// outside 0..255.
func ipv4Address(address *string) (string, bool) {
	if address == nil {
		return "0.0.0.0", true
	}
	parts := splitDropEmpty(*address, '.')
	if len(parts) != 4 {
		return "0.0.0.0", true
	}
	var octets [4]string
	for i, p := range parts {
		n, err := strconv.ParseInt(p, 10, 32)
		if err != nil {
			return "0.0.0.0", true
		}
		if n < 0 || n > 255 {
			return "", false
		}
		octets[i] = strconv.FormatInt(n, 10)
	}
	return strings.Join(octets[:], "."), true
}
