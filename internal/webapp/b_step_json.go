package webapp

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf16"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Jackson serialization of scouter.lang.step.* objects (GET /v1/profile) and of
// the webapp SSpanStep/SSpanCallStep (GET /v1/profile-data).
//
// Property order rule used (Jackson POJOPropertiesCollector, no annotations):
// properties backed by a field come first, in field order (superclass first,
// including non-public fields that have a public getter); getter-only
// properties follow (subclass getters before superclass getters). Getter
// method order is JVM reflection order and was not captured from a running
// Java webapp (unverified); declaration order is assumed.

var bStepTypeNames = map[byte]string{
	protocol.StepMethod: "METHOD", protocol.StepMethod2: "METHOD2", protocol.StepSQL: "SQL",
	protocol.StepSQL2: "SQL2", protocol.StepSQL3: "SQL3", protocol.StepMessage: "MESSAGE",
	protocol.StepSocket: "SOCKET", protocol.StepAPICall: "APICALL", protocol.StepAPICall2: "APICALL2",
	protocol.StepThreadSubmit: "THREAD_SUBMIT", protocol.StepHashedMessage: "HASHED_MESSAGE",
	protocol.StepParameterizedMessage: "PARAMETERIZED_MESSAGE", protocol.StepDump: "DUMP",
	protocol.StepDispatch: "DISPATCH", protocol.StepThreadCallPossible: "THREAD_CALL_POSSIBLE",
	protocol.StepMethodSum: "METHOD_SUM", protocol.StepSQLSum: "SQL_SUM", protocol.StepMessageSum: "MESSAGE_SUM",
	protocol.StepSocketSum: "SOCKET_SUM", protocol.StepAPICallSum: "APICALL_SUM", protocol.StepControlType: "CONTROL",
	protocol.StepSpan: "SPAN", protocol.StepSpanCall: "SPANCALL",
}

func bF(k string, v any) jsonField { return jsonField{Key: k, Value: v} }

// bSingleBase: StepSingle fields parent, index, start_time, start_cpu, spanPack(null).
func bSingleBase(s protocol.SingleStep) jsonObject {
	return jsonObject{bF("parent", s.Parent), bF("index", s.Index), bF("start_time", s.StartTime), bF("start_cpu", s.StartCPU), bF("spanPack", nil)}
}

// bStepTail appends getter-only properties: stepType, class-specific extras,
// order (StepSingle: index, StepSummary: Integer.MAX_VALUE), stepTypeName.
func bStepTail(o jsonObject, typ byte, order int32, extras ...jsonField) jsonObject {
	o = append(o, bF("stepType", int8(typ)))
	o = append(o, extras...)
	return append(o, bF("order", order), bF("stepTypeName", bStepTypeNames[typ]))
}

// bSqlXType is SqlXType.toString(xtype).
func bSqlXType(x byte) string {
	switch x & 0x0f {
	case 1:
		return "PRE> "
	case 2:
		return "DYN> "
	}
	return "STM> "
}

// bParamLevel is ParameterizedMessageLevel.of(byte).name(); unknown values throw in Java.
func bParamLevel(level int8) (string, error) {
	names := []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
	if level < 0 || int(level) >= len(names) {
		return "", bInternal("not matched ParameterizedMessageLevel value.")
	}
	return names[level], nil
}

func bCRC(b []byte) int32 { return bJavaHash(string(b)) }

// bListValueJSON / bMapValueJSON: Jackson bean view of ListValue/MapValue
// (only getValueType()/isEmpty() getters exist).
func bListValueJSON(v protocol.Value) any {
	if v.Type == protocol.ValueNull {
		return nil
	}
	return jsonObject{bF("valueType", int8(v.Type))}
}

func bMapValueJSON(v protocol.Value) any {
	if v.Type == protocol.ValueNull {
		return nil
	}
	// Oracle (Jackson 2.12.6.1): getValueType() is collected before isEmpty().
	return jsonObject{bF("valueType", int8(v.Type)), bF("empty", len(v.Entries) == 0)}
}

// bAddr: ApiCallStep/DispatchStep/SpanCallStep read the address only when opt == 1 (null otherwise).
func bAddr(opt byte, address string) any {
	if opt != 1 {
		return nil
	}
	return address
}

// bJavaHashtableOrder returns the iteration order of a java.util.Hashtable
// (MapValue) filled with keys in the given (wire) order: capacity 11, load
// factor 0.75, head insertion, rehash to 2n+1 walking buckets from the top,
// enumeration from the highest bucket down.
func bJavaHashtableOrder(keys []string) []string {
	type node struct {
		key  string
		hash int32
	}
	capacity := 11
	threshold := int(float64(capacity) * 0.75)
	table := make([][]node, capacity)
	count := 0
	seen := map[string]bool{}
	index := func(h int32, c int) int { return int(uint32(h)&0x7fffffff) % c }
	for _, k := range keys {
		if seen[k] {
			continue // put on an existing key replaces the value in place
		}
		seen[k] = true
		h := bJavaStringHash(k)
		if count >= threshold {
			nc := capacity*2 + 1
			nt := make([][]node, nc)
			for i := capacity - 1; i >= 0; i-- {
				for _, n := range table[i] {
					at := index(n.hash, nc)
					nt[at] = append([]node{n}, nt[at]...)
				}
			}
			table, capacity, threshold = nt, nc, int(float64(nc)*0.75)
		}
		at := index(h, capacity)
		table[at] = append([]node{{k, h}}, table[at]...)
		count++
	}
	out := make([]string, 0, count)
	for i := capacity - 1; i >= 0; i-- {
		for _, n := range table[i] {
			out = append(out, n.key)
		}
	}
	return out
}

func bCallFields(o jsonObject, c protocol.CallStepFields) jsonObject {
	return append(o, bF("txid", c.TxID), bF("hash", c.Hash), bF("elapsed", c.Elapsed), bF("cputime", c.CPUTime), bF("error", c.Error))
}

// bRawStepJSON serializes a scouter.lang.step.Step as Jackson does.
func bRawStepJSON(step protocol.Step) (jsonObject, error) {
	typ := step.StepType()
	switch st := step.(type) {
	case *protocol.MethodStep:
		o := append(bSingleBase(st.SingleStep), bF("hash", st.Hash), bF("elapsed", st.Elapsed), bF("cputime", st.CPUTime))
		return bStepTail(o, typ, st.Index), nil
	case *protocol.MethodStep2:
		o := append(bSingleBase(st.SingleStep), bF("hash", st.Hash), bF("elapsed", st.Elapsed), bF("cputime", st.CPUTime), bF("error", st.Error))
		return bStepTail(o, typ, st.Index), nil
	case *protocol.SQLStep:
		return bStepTail(bSQLBase(*st), typ, st.Index), nil
	case *protocol.SQLStep2:
		o := append(bSQLBase(st.SQLStep), bF("xtype", int8(st.XType)))
		return bStepTail(o, typ, st.Index, bF("xtypePrefix", bSqlXType(st.XType))), nil
	case *protocol.SQLStep3:
		o := append(bSQLBase(st.SQLStep), bF("xtype", int8(st.XType)), bF("updated", st.Updated))
		return bStepTail(o, typ, st.Index, bF("xtypePrefix", bSqlXType(st.XType))), nil
	case *protocol.MessageStep:
		return bStepTail(append(bSingleBase(st.SingleStep), bF("message", st.Message)), typ, st.Index), nil
	case *protocol.HashedMessageStep:
		o := append(bSingleBase(st.SingleStep), bF("hash", st.Hash), bF("time", st.Time), bF("value", st.Value))
		return bStepTail(o, typ, st.Index), nil
	case *protocol.ParameterizedMessageStep:
		level, err := bParamLevel(st.Level)
		if err != nil {
			return nil, err
		}
		// private hash/elapsed/level have getters; paramString/tempMap do not.
		o := append(bSingleBase(st.SingleStep), bF("hash", st.Hash), bF("elapsed", st.Elapsed), bF("level", level))
		return bStepTail(o, typ, st.Index), nil
	case *protocol.SocketStep:
		if st.IPAddress == nil {
			return nil, bInternal("null") // getSocketId(): HashUtil.hash(null) NPE
		}
		o := append(bSingleBase(st.SingleStep), bF("ipaddr", bBase64(st.IPAddress)), bF("port", st.Port), bF("elapsed", st.Elapsed), bF("error", st.Error))
		socketID := int64(bCRC(st.IPAddress))<<32 | int64(uint32(st.Port))
		return bStepTail(o, typ, st.Index, bF("socketId", socketID)), nil
	case *protocol.APICallStep:
		o := bCallFields(bSingleBase(st.SingleStep), st.CallStepFields)
		o = append(o, bF("opt", int8(st.Opt)), bF("address", bAddr(st.Opt, st.Address)))
		return bStepTail(o, typ, st.Index), nil
	case *protocol.APICallStep2:
		o := bCallFields(bSingleBase(st.SingleStep), st.CallStepFields)
		o = append(o, bF("opt", int8(st.Opt)), bF("address", bAddr(st.Opt, st.Address)), bF("start", int64(0)), bF("async", int8(st.Async)))
		return bStepTail(o, typ, st.Index), nil
	case *protocol.DispatchStep:
		o := bCallFields(bSingleBase(st.SingleStep), st.CallStepFields)
		o = append(o, bF("opt", int8(st.Opt)), bF("address", bAddr(st.Opt, st.Address)))
		return bStepTail(o, typ, st.Index), nil
	case *protocol.ThreadSubmitStep:
		return bStepTail(bCallFields(bSingleBase(st.SingleStep), st.CallStepFields), typ, st.Index), nil
	case *protocol.ThreadCallPossibleStep:
		o := append(bSingleBase(st.SingleStep), bF("txid", st.TxID), bF("hash", st.Hash), bF("elapsed", st.Elapsed),
			bF("threaded", int8(st.Threaded)), bF("nameTemp", nil), bF("isIgnoreIfNoThreaded", false))
		return bStepTail(o, typ, st.Index), nil
	case *protocol.DumpStep:
		stacks := st.Stacks
		if stacks == nil {
			stacks = []int32{}
		}
		o := append(bSingleBase(st.SingleStep), bF("stacks", stacks), bF("threadId", st.ThreadID), bF("threadName", st.ThreadName),
			bF("threadState", st.ThreadState), bF("lockOwnerId", st.LockOwnerID), bF("lockName", st.LockName), bF("lockOwnerName", st.LockOwnerName))
		return bStepTail(o, typ, st.Index), nil
	case *protocol.MethodSum:
		return bStepTail(bSumBase(*st), typ, math.MaxInt32), nil
	case *protocol.SQLSum:
		o := append(bSumBase(st.MethodSum), bF("error", st.Error), bF("param", st.Param), bF("param_error", st.ParamError))
		return bStepTail(o, typ, math.MaxInt32, bF("paramError", st.ParamError)), nil
	case *protocol.APICallSum:
		o := append(bSumBase(st.MethodSum), bF("error", st.Error), bF("opt", int8(st.Opt)))
		return bStepTail(o, typ, math.MaxInt32), nil
	case *protocol.SocketSum:
		o := jsonObject{bF("ipaddr", bBase64(st.IPAddress)), bF("port", st.Port), bF("count", st.Count), bF("elapsed", st.Elapsed), bF("error", st.Error)}
		return bStepTail(o, typ, math.MaxInt32), nil
	case *protocol.MessageSum:
		return bStepTail(jsonObject{bF("message", st.Message), bF("count", st.Count)}, typ, math.MaxInt32), nil
	case *protocol.StepControl:
		return bStepTail(jsonObject{bF("message", st.Message), bF("code", st.Code)}, typ, math.MaxInt32), nil
	case *protocol.SpanStep:
		return bStepTail(bRawSpanBase(st.CommonSpanStep), typ, st.Index), nil
	case *protocol.SpanCallStep:
		// opt is a transient public field without getter: not serialized.
		o := append(bRawSpanBase(st.CommonSpanStep), bF("txid", st.TxID), bF("address", bAddr(st.Opt, st.Address)), bF("async", int8(st.Async)))
		return bStepTail(o, typ, st.Index), nil
	}
	return nil, bInternal(fmt.Sprintf("unknown profile type=%d", typ))
}

func bSQLBase(st protocol.SQLStep) jsonObject {
	return append(bSingleBase(st.SingleStep), bF("hash", st.Hash), bF("elapsed", st.Elapsed), bF("cputime", st.CPUTime), bF("param", st.Param), bF("error", st.Error))
}

func bSumBase(st protocol.MethodSum) jsonObject {
	return jsonObject{bF("hash", st.Hash), bF("count", st.Count), bF("elapsed", st.Elapsed), bF("cputime", st.CPUTime)}
}

func bRawSpanBase(c protocol.CommonSpanStep) jsonObject {
	return append(bSingleBase(c.SingleStep), bF("nameDebug", nil), bF("hash", c.Hash), bF("elapsed", c.Elapsed), bF("error", c.Error),
		bF("timestamp", c.Timestamp), bF("spanType", int8(c.SpanType)),
		bF("localEndpointServiceName", c.LocalEndpointServiceName), bF("localEndpointIp", bBase64(c.LocalEndpointIP)), bF("localEndpointPort", c.LocalEndpointPort),
		bF("remoteEndpointServiceName", c.RemoteEndpointServiceName), bF("remoteEndpointIp", bBase64(c.RemoteEndpointIP)), bF("remoteEndpointPort", c.RemoteEndpointPort),
		bF("debug", c.Debug), bF("shared", c.Shared),
		bF("annotationTimestamps", bListValueJSON(c.AnnotationTimestamps)), bF("annotationValues", bListValueJSON(c.AnnotationValues)),
		bF("tags", bMapValueJSON(c.Tags)))
}

// bSSpanJSON is the webapp SSpanStep/SSpanCallStep (SCommonSpanStep.setProps)
// with endpoint serviceName filled by the OBJECT dictionary.
func bSSpanJSON(c protocol.CommonSpanStep, call *protocol.SpanCallStep, typ byte, objText func(int32) string) jsonObject {
	endpoint := func(hash int32, ip []byte, port int16) jsonObject {
		return jsonObject{bF("hash", hash), bF("serviceName", objText(hash)), bF("ip", bIPStringNullable(ip)), bF("port", int32(port))}
	}
	ts, vs := bListOf(c.AnnotationTimestamps), bListOf(c.AnnotationValues)
	annotations := make([]jsonObject, 0, len(ts))
	for i := range ts {
		annotations = append(annotations, jsonObject{bF("timestamp", bLong(ts[i])), bF("value", bListString(vs, i))})
	}
	var keys []string
	vals := map[string]*string{}
	for _, e := range c.Tags.Entries {
		keys = append(keys, e.Key)
		vals[e.Key] = bValueString(e.Value)
	}
	tags := jsonObject{}
	// Wire order -> receiving MapValue Hashtable order -> HashMap<String,String> order.
	for _, k := range bJavaStringSet(bJavaHashtableOrder(keys)) {
		tags = append(tags, bF(k, vals[k]))
	}
	base := c.SingleStep
	base.StartCPU = 0 // SCommonSpanStep.setProps does not copy start_cpu
	o := append(bSingleBase(base), bF("hash", c.Hash), bF("elapsed", c.Elapsed), bF("error", c.Error), bF("timestamp", c.Timestamp),
		bF("spanType", int8(c.SpanType)),
		bF("localEndpoint", endpoint(c.LocalEndpointServiceName, c.LocalEndpointIP, c.LocalEndpointPort)),
		bF("remoteEndpoint", endpoint(c.RemoteEndpointServiceName, c.RemoteEndpointIP, c.RemoteEndpointPort)),
		bF("debug", c.Debug), bF("shared", c.Shared), bF("annotations", annotations), bF("tags", tags))
	if call != nil {
		// Lombok @Getter exposes the transient opt (never copied: 0).
		o = append(o, bF("txid", call.TxID), bF("opt", int8(0)), bF("address", bAddr(call.Opt, call.Address)), bF("async", int8(call.Async)))
	}
	return bStepTail(o, typ, c.Index)
}

func bIPStringNullable(ip []byte) string { return bIPString(ip) }

func bListOf(v protocol.Value) []protocol.Value {
	if v.Type != protocol.ValueList {
		return nil
	}
	return v.Values
}

// bJavaFormat implements the subset of java.util.Formatter reachable with
// String arguments (ParameterizedMessageStep.buildMessasge): %s %S %b %B %h %H
// %n %% with '-' flag, width, precision, explicit "n$" and relative "<" index.
// Any other conversion throws in Java (IllegalFormatConversion/Unknown...),
// which buildMessasge turns into the unformatted message: ok=false.
func bJavaFormat(format string, args []string) (string, bool) {
	var b strings.Builder
	ordinary, last := 0, -1
	for i := 0; i < len(format); i++ {
		c := format[i]
		if c != '%' {
			b.WriteByte(c)
			continue
		}
		j := i + 1
		// argument index
		explicit := -1
		relative := false
		k := j
		for k < len(format) && format[k] >= '0' && format[k] <= '9' {
			k++
		}
		if k > j && k < len(format) && format[k] == '$' {
			n, _ := strconv.Atoi(format[j:k])
			explicit = n - 1
			j = k + 1
		} else if j < len(format) && format[j] == '<' {
			relative = true
			j++
		}
		left := false
		for j < len(format) && strings.IndexByte("-#+ 0,(", format[j]) >= 0 {
			if format[j] == '-' {
				left = true
			} else {
				return "", false
			}
			j++
		}
		width := -1
		k = j
		for k < len(format) && format[k] >= '0' && format[k] <= '9' {
			k++
		}
		if k > j {
			width, _ = strconv.Atoi(format[j:k])
			j = k
		}
		prec := -1
		if j < len(format) && format[j] == '.' {
			k = j + 1
			for k < len(format) && format[k] >= '0' && format[k] <= '9' {
				k++
			}
			if k == j+1 {
				return "", false
			}
			prec, _ = strconv.Atoi(format[j+1 : k])
			j = k
		}
		if j >= len(format) {
			return "", false
		}
		conv := format[j]
		i = j
		switch conv {
		case '%':
			b.WriteString(bPad("%", width, left))
			continue
		case 'n':
			b.WriteString("\n")
			continue
		case 's', 'S', 'b', 'B', 'h', 'H':
		default:
			return "", false
		}
		idx := explicit
		switch {
		case relative:
			idx = last
		case explicit < 0:
			idx = ordinary
			ordinary++
		}
		if idx < 0 || idx >= len(args) {
			return "", false
		}
		last = idx
		var s string
		switch conv {
		case 's', 'S':
			s = args[idx]
		case 'b', 'B':
			s = "true" // non-null, non-Boolean argument
		case 'h', 'H':
			s = strconv.FormatUint(uint64(uint32(bJavaStringHash(args[idx]))), 16)
		}
		if prec >= 0 {
			u := utf16.Encode([]rune(s))
			if prec < len(u) {
				s = string(utf16.Decode(u[:prec]))
			}
		}
		if conv == 'S' || conv == 'B' || conv == 'H' {
			s = strings.ToUpper(s)
		}
		b.WriteString(bPad(s, width, left))
	}
	return b.String(), true
}

func bPad(s string, width int, left bool) string {
	n := bUTF16Len(s)
	if width <= n {
		return s
	}
	pad := strings.Repeat(" ", width-n)
	if left {
		return s + pad
	}
	return pad + s
}

// bBuildMessage is ParameterizedMessageStep.buildMessasge.
func bBuildMessage(format, paramString string) string {
	var params []string
	for _, p := range strings.Split(paramString, "\x03") {
		if p != "" {
			params = append(params, p)
		}
	}
	if len(params) == 0 {
		return format
	}
	out, ok := bJavaFormat(format, params)
	if !ok {
		return format
	}
	return out
}
