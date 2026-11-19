package pulse

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
)

// ProfileStepData is the plugin profile step model. Step holds the
// bytes that the plugin step serializer Base64-encodes: Step.toBytes(List.of(step)).
type ProfileStepData struct {
	MainValue           *string
	AdditionalValueList []string
	Step                []byte
}

// Span is the plugin span stream model. Profile nil is a Java null list
// (pack.profile == null); an empty slice is an empty list.
type Span struct {
	TraceID                int64
	Time                   int64
	ObjectDictID           int32
	TransactionDictID      int32
	Elapsed                int32
	Profile                []ProfileStepData
	ObjectDictContent      *string
	TransactionDictContent *string
}

// StepLimits bounds profile Step decoding; profiles are stored unparsed by the
// server, so the agent-side chunking is the only upstream limit.
var StepLimits = protocol.Limits{MaxCollectionEntries: 1 << 16, MaxTotalValues: 1 << 20}

// EncodeSingletonStep is Step.toBytes(List.of(step)) as the plugin emits it.
// SPAN/SPANCALL are replaced by the plugin span step models, which do
// not override write(), so only the type byte and StepSingle fields
// (parent, index, start_time, start_cpu; scouter.common StepSingle.write) are written, and
// their property copy does not copy start_cpu, so it is
// always 0. That truncated encoding is reproduced on purpose; a standard
// CommonSpanStep reader cannot decode it (scouter.common CommonSpanStep.read) and this is recorded as
// a defect candidate, not fixed.
func EncodeSingletonStep(step protocol.Step) ([]byte, error) {
	switch s := step.(type) {
	case *protocol.SpanStep:
		single := s.SingleStep
		single.StartCPU = 0
		return encodeStepSingle(protocol.StepSpan, single)
	case *protocol.SpanCallStep:
		single := s.SingleStep
		single.StartCPU = 0
		return encodeStepSingle(protocol.StepSpanCall, single)
	default:
		return protocol.EncodeStep(step, StepLimits)
	}
}

func encodeStepSingle(kind byte, s protocol.SingleStep) ([]byte, error) {
	e := protocol.NewEncoder(StepLimits)
	e.Byte(kind)
	e.Decimal(int64(s.Parent))
	e.Decimal(int64(s.Index))
	e.Decimal(int64(s.StartTime))
	e.Decimal(int64(s.StartCPU))
	return e.Bytes()
}

// mainTextType is StepEnum.Type.associatedMainTextTypeName for the step types
// whose main value comes from a dictionary (S: StepEnum.java:49-71).
func mainTextHash(step protocol.Step) (div string, hash int32, ok bool) {
	switch s := step.(type) {
	case *protocol.MethodStep:
		return TextMethod, s.Hash, true
	case *protocol.MethodStep2:
		return TextMethod, s.Hash, true
	case *protocol.SQLStep:
		return TextSQL, s.Hash, true
	case *protocol.SQLStep2:
		return TextSQL, s.Hash, true
	case *protocol.SQLStep3:
		return TextSQL, s.Hash, true
	case *protocol.APICallStep:
		return TextAPICall, s.Hash, true
	case *protocol.APICallStep2:
		return TextAPICall, s.Hash, true
	case *protocol.ThreadSubmitStep:
		return TextAPICall, s.Hash, true
	case *protocol.HashedMessageStep:
		return TextHashMessage, s.Hash, true
	case *protocol.ParameterizedMessageStep:
		return TextHashMessage, s.Hash, true
	case *protocol.DispatchStep:
		return TextAPICall, s.Hash, true
	case *protocol.ThreadCallPossibleStep:
		return TextAPICall, s.Hash, true
	case *protocol.SpanStep:
		return TextService, s.Hash, true
	case *protocol.SpanCallStep:
		return TextService, s.Hash, true
	}
	return "", 0, false
}

// javaIPString is scouter.util.IPUtil.toString(byte[]): null or fewer than four
// bytes yields "0.0.0.0"; longer arrays render only the first four bytes.
func javaIPString(ip []byte) string {
	if len(ip) < 4 {
		return "0.0.0.0"
	}
	return strconv.Itoa(int(ip[0])) + "." + strconv.Itoa(int(ip[1])) + "." + strconv.Itoa(int(ip[2])) + "." + strconv.Itoa(int(ip[3]))
}

// textIfNullDefault is the plugin text lookup with default: hash 0 is
// "", a miss or blank text is "**unlabeled**:<type>:<hash>".
func textIfNullDefault(ctx context.Context, lookup TextLookup, date, div string, hash int32) (string, error) {
	if hash == 0 {
		return "", nil
	}
	text, found, err := lookup(ctx, date, div, hash)
	if err != nil {
		return "", err
	}
	if !found || javaIsBlank(text) {
		return "**unlabeled**:" + div + ":" + strconv.Itoa(int(hash)), nil
	}
	return text, nil
}

// ConvertProfileSteps is the plugin profile step conversion. Steps are decoded
// in order; dictionary values for main hashes and DUMP stack frames are read at
// nowDate, the date of the plugin text preload. A miss at
// that date is cached as null in the Java text cache, so the later event-date
// lookup never runs for those hashes; it is therefore not
// performed here either. SPAN endpoint OBJECT names are looked up at the event
// date in Java but only populate fields that are never serialized, so those
// lookups are skipped.
func ConvertProfileSteps(ctx context.Context, lookup TextLookup, profile []byte, nowDate string) ([]ProfileStepData, error) {
	if profile == nil {
		return nil, nil
	}
	steps, err := protocol.DecodeSteps(profile, StepLimits)
	if err != nil {
		return nil, fmt.Errorf("profile steps: %w", err)
	}
	out := make([]ProfileStepData, 0, len(steps))
	for _, step := range steps {
		data := ProfileStepData{AdditionalValueList: []string{}}
		if div, hash, ok := mainTextHash(step); ok {
			text, err := textIfNullDefault(ctx, lookup, nowDate, div, hash)
			if err != nil {
				return nil, err
			}
			if pm, isParam := step.(*protocol.ParameterizedMessageStep); isParam {
				text = buildParameterizedMessage(text, pm.ParamString)
			}
			data.MainValue = &text
		} else {
			empty := ""
			data.MainValue = &empty
			switch s := step.(type) {
			case *protocol.MessageStep:
				msg := s.Message
				data.MainValue = &msg
			case *protocol.SocketStep:
				ip := javaIPString(s.IPAddress)
				data.MainValue = &ip
			case *protocol.DumpStep:
				for _, frame := range s.Stacks {
					text, err := textIfNullDefault(ctx, lookup, nowDate, TextStackElement, frame)
					if err != nil {
						return nil, err
					}
					data.AdditionalValueList = append(data.AdditionalValueList, text)
				}
			}
		}
		data.Step, err = EncodeSingletonStep(step)
		if err != nil {
			return nil, fmt.Errorf("profile step %T: %w", step, err)
		}
		out = append(out, data)
	}
	return out, nil
}

// buildParameterizedMessage is ParameterizedMessageStep.buildMessasge
// (reference ParameterizedMessageStep.java:115-132): parameters are split on
// ETX (0x03) with empty tokens dropped and applied with String.format; any
// formatting failure returns the format unchanged.
func buildParameterizedMessage(format, paramString string) string {
	params := javaSplitChar(paramString, 3)
	if len(params) == 0 {
		return format
	}
	if formatted, ok := javaFormat(format, params); ok {
		return formatted
	}
	return format
}

// ConvertProfile is the plugin profile converter. now is the wall
// clock used for the Java text preload date. elapsed is the Java
// XLogProfilePack.elapsed field: it is not on the wire and the Java server
// never assigns it, so the dispatcher passes 0; the Java oracle sets it in
// memory and the golden test passes that value through.
func ConvertProfile(ctx context.Context, lookup TextLookup, loc *time.Location, p *protocol.ProfilePack, elapsed int32, now time.Time) (*Span, error) {
	s := &Span{TraceID: p.TxID, Time: p.Time, ObjectDictID: p.ObjHash, TransactionDictID: p.Service, Elapsed: elapsed}
	profile, err := ConvertProfileSteps(ctx, lookup, p.Profile, scoutertime.Date(now.UnixMilli(), loc))
	if err != nil {
		return nil, err
	}
	s.Profile = profile
	date := scoutertime.Date(p.Time, loc)
	object, found, err := lookup(ctx, date, TextObject, p.ObjHash)
	if err != nil {
		return nil, err
	}
	s.ObjectDictContent = strPtr(object, found)
	service, found, err := lookup(ctx, date, TextService, p.Service)
	if err != nil {
		return nil, err
	}
	s.TransactionDictContent = strPtr(service, found)
	return s, nil
}

func (s *Span) Rebase(cond RebaseCondition) {
	if s.TransactionDictContent == nil {
		return
	}
	rebased := RebaseTransaction(*s.TransactionDictContent, cond)
	s.TransactionDictContent = &rebased
	s.TransactionDictID = TransactionHash(rebased)
}

func (s *Span) RemoveAfter(conditions []string) {
	if s.TransactionDictContent == nil {
		return
	}
	next, changed := RemoveAfterConditions(*s.TransactionDictContent, conditions)
	if !changed {
		return
	}
	s.TransactionDictContent = &next
	s.TransactionDictID = TransactionHash(next)
}

// spanKeyOrder is the inferred Jackson order: base span model fields then
// span stream model fields; no getter produces a mangled name here.
var spanKeyOrder = []string{"traceId", "time", "objectDictId", "transactionDictId", "elapsed", "profile", "objectDictContent", "transactionDictContent"}

// profileStepKeyOrder follows the profile step model field declaration.
var profileStepKeyOrder = []string{"mainValue", "additionalValueList", "step"}

// MarshalJSON writes the Jackson serialization of the span stream model.
func (s *Span) MarshalJSON() ([]byte, error) {
	w := newJSONWriter()
	w.beginObject()
	for _, k := range spanKeyOrder {
		switch k {
		case "traceId":
			w.keyInt64(k, s.TraceID)
		case "time":
			w.keyInt64(k, s.Time)
		case "objectDictId":
			w.keyInt64(k, int64(s.ObjectDictID))
		case "transactionDictId":
			w.keyInt64(k, int64(s.TransactionDictID))
		case "elapsed":
			w.keyInt64(k, int64(s.Elapsed))
		case "profile":
			w.key(k)
			if s.Profile == nil {
				w.null()
				break
			}
			w.beginArray()
			for i := range s.Profile {
				w.element()
				writeProfileStep(w, &s.Profile[i])
			}
			w.endArray()
		case "objectDictContent":
			w.keyNullableString(k, s.ObjectDictContent)
		case "transactionDictContent":
			w.keyNullableString(k, s.TransactionDictContent)
		}
	}
	w.endObject()
	return w.buf, nil
}

func writeProfileStep(w *jsonWriter, d *ProfileStepData) {
	w.beginObject()
	for _, k := range profileStepKeyOrder {
		switch k {
		case "mainValue":
			w.keyNullableString(k, d.MainValue)
		case "additionalValueList":
			w.key(k)
			if d.AdditionalValueList == nil {
				w.null()
				break
			}
			w.beginArray()
			for _, v := range d.AdditionalValueList {
				w.stringValue(v)
			}
			w.endArray()
		case "step":
			w.keyBytes(k, d.Step)
		}
	}
	w.endObject()
}
