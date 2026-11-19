package pulse

import (
	"context"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
)

// Scouter text dictionary types (scouter.lang.TextTypes).
const (
	TextObject       = "object"
	TextService      = "service"
	TextHashMessage  = "hmsg"
	TextError        = "error"
	TextMethod       = "method"
	TextSQL          = "sql"
	TextAPICall      = "apicall"
	TextStackElement = "stackelem"
)

// TextLookup resolves a dictionary string for a Java yyyymmdd date and a text
// type. found=false is the Java null; an error aborts the task (Java TextRD
// swallows exceptions as null, which this port does not reproduce).
type TextLookup func(ctx context.Context, date, div string, hash int32) (string, bool, error)

// Trace is the plugin trace stream model. Pointer strings are Java String
// references that may be null; IPAddress nil is a Java null byte[] and an empty
// slice is Java's new byte[0] (what the wire reader produces for a zero-length
// Blob).
type Trace struct {
	TraceID                        int64
	TraceEndTime                   int64
	ObjectDictID                   int32
	TransactionDictID              int32
	ThreadDictID                   int32
	ParentTraceID                  int64
	GlobalTraceID                  int64
	Elapsed                        int32
	ErrorDictID                    int32
	CPU                            int32
	SQLCount                       int32
	SQLTime                        int32
	IPAddress                      []byte
	KBytes                         int32
	APICallCount                   int32
	APICallTime                    int32
	XType                          byte
	ProfileCount                   int32
	B3Mode                         bool
	ProfileSize                    int32
	DiscardType                    byte
	IgnoreGlobalConsequentSampling bool
	ObjectDictContent              *string
	TransactionDictContent         *string
	ThreadDictContent              *string
	ErrorDictContent               *string
}

func strPtr(s string, found bool) *string {
	if !found {
		return nil
	}
	return &s
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append(make([]byte, 0, len(b)), b...)
}

// ConvertXLog is the plugin XLog converter. Dictionary lookups use the
// Java date of pack.endTime in the configured zone. errorDictContent is looked
// up only when error != 0 (otherwise null).
func ConvertXLog(ctx context.Context, lookup TextLookup, loc *time.Location, p *protocol.XLogPack) (*Trace, error) {
	t := &Trace{
		TraceID: p.TxID, TraceEndTime: p.EndTime, ObjectDictID: p.ObjHash, TransactionDictID: p.Service,
		ThreadDictID: p.ThreadNameHash, ParentTraceID: p.Caller, GlobalTraceID: p.GxID, Elapsed: p.Elapsed,
		ErrorDictID: p.Error, CPU: p.CPU, SQLCount: p.SQLCount, SQLTime: p.SQLTime, IPAddress: cloneBytes(p.IPAddress),
		KBytes: p.KBytes, APICallCount: p.APICallCount, APICallTime: p.APICallTime, XType: p.XType,
		ProfileCount: p.ProfileCount, B3Mode: p.B3Mode, ProfileSize: p.ProfileSize, DiscardType: p.DiscardType,
		IgnoreGlobalConsequentSampling: p.IgnoreGlobalConsequentSampling,
	}
	date := scoutertime.Date(p.EndTime, loc)
	object, found, err := lookup(ctx, date, TextObject, p.ObjHash)
	if err != nil {
		return nil, err
	}
	t.ObjectDictContent = strPtr(object, found)
	service, found, err := lookup(ctx, date, TextService, p.Service)
	if err != nil {
		return nil, err
	}
	t.TransactionDictContent = strPtr(service, found)
	thread, found, err := lookup(ctx, date, TextHashMessage, p.ThreadNameHash)
	if err != nil {
		return nil, err
	}
	t.ThreadDictContent = strPtr(thread, found)
	if p.Error != 0 {
		errText, found, err := lookup(ctx, date, TextError, p.Error)
		if err != nil {
			return nil, err
		}
		t.ErrorDictContent = strPtr(errText, found)
	}
	return t, nil
}

// Rebase applies the plugin transaction rebase and recomputes the Scouter hash.
// The Java code would throw on a null content; callers exclude null first.
func (t *Trace) Rebase(cond RebaseCondition) {
	if t.TransactionDictContent == nil {
		return
	}
	rebased := RebaseTransaction(*t.TransactionDictContent, cond)
	t.TransactionDictContent = &rebased
	t.TransactionDictID = TransactionHash(rebased)
}

// RemoveAfter applies the plugin global suffix removal.
func (t *Trace) RemoveAfter(conditions []string) {
	if t.TransactionDictContent == nil {
		return
	}
	next, changed := RemoveAfterConditions(*t.TransactionDictContent, conditions)
	if !changed {
		return
	}
	t.TransactionDictContent = &next
	t.TransactionDictID = TransactionHash(next)
}

// traceKeyOrder documents the inferred Jackson property order: fields of
// the base trace model in declaration order, then the trace stream model fields, and
// finally "xtype". The getter of that type field yields the implicit name "xtype"
// (BeanUtil.legacyManglePropertyName lowercases the leading uppercase run), so
// it is a property distinct from the invisible private field "xType" and is
// appended when getters are collected after all fields. The Java golden fixture
// is authoritative; adjust here if it disagrees.
var traceKeyOrder = []string{
	"traceId", "traceEndTime", "objectDictId", "transactionDictId", "threadDictId", "parentTraceId",
	"globalTraceId", "elapsed", "errorDictId", "cpu", "sqlCount", "sqlTime", "ipaddr", "kbytes",
	"apicallCount", "apicallTime", "profileCount", "b3Mode", "profileSize", "discardType",
	"ignoreGlobalConsequentSampling", "objectDictContent", "transactionDictContent",
	"threadDictContent", "errorDictContent", "xtype",
}

// MarshalJSON writes the Jackson serialization of the trace stream model.
func (t *Trace) MarshalJSON() ([]byte, error) {
	w := newJSONWriter()
	w.beginObject()
	for _, k := range traceKeyOrder {
		switch k {
		case "traceId":
			w.keyInt64(k, t.TraceID)
		case "traceEndTime":
			w.keyInt64(k, t.TraceEndTime)
		case "objectDictId":
			w.keyInt64(k, int64(t.ObjectDictID))
		case "transactionDictId":
			w.keyInt64(k, int64(t.TransactionDictID))
		case "threadDictId":
			w.keyInt64(k, int64(t.ThreadDictID))
		case "parentTraceId":
			w.keyInt64(k, t.ParentTraceID)
		case "globalTraceId":
			w.keyInt64(k, t.GlobalTraceID)
		case "elapsed":
			w.keyInt64(k, int64(t.Elapsed))
		case "errorDictId":
			w.keyInt64(k, int64(t.ErrorDictID))
		case "cpu":
			w.keyInt64(k, int64(t.CPU))
		case "sqlCount":
			w.keyInt64(k, int64(t.SQLCount))
		case "sqlTime":
			w.keyInt64(k, int64(t.SQLTime))
		case "ipaddr":
			w.keyBytes(k, t.IPAddress)
		case "kbytes":
			w.keyInt64(k, int64(t.KBytes))
		case "apicallCount":
			w.keyInt64(k, int64(t.APICallCount))
		case "apicallTime":
			w.keyInt64(k, int64(t.APICallTime))
		case "xtype":
			w.keyInt64(k, int64(int8(t.XType)))
		case "profileCount":
			w.keyInt64(k, int64(t.ProfileCount))
		case "b3Mode":
			w.keyBool(k, t.B3Mode)
		case "profileSize":
			w.keyInt64(k, int64(t.ProfileSize))
		case "discardType":
			w.keyInt64(k, int64(int8(t.DiscardType)))
		case "ignoreGlobalConsequentSampling":
			w.keyBool(k, t.IgnoreGlobalConsequentSampling)
		case "objectDictContent":
			w.keyNullableString(k, t.ObjectDictContent)
		case "transactionDictContent":
			w.keyNullableString(k, t.TransactionDictContent)
		case "threadDictContent":
			w.keyNullableString(k, t.ThreadDictContent)
		case "errorDictContent":
			w.keyNullableString(k, t.ErrorDictContent)
		}
	}
	w.endObject()
	return w.buf, nil
}
