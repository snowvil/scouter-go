package webapp

import (
	"context"
	"net/http"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// ProfileController (/v1/profile) and ProfileDataController (/v1/profile-data).

const bMaxProfileBlock = 10 // ProfileConsumer.MAX_PROFILE_BLOCK

// bProfileLimits bounds Step decoding of one TRANX_PROFILE answer. The Go
// collector caps the joined profile at 4 MiB (internal/service xlog.go); the
// entry/value bounds follow internal/pulse StepLimits (65,536 steps, 1,048,576
// nested values), far above the 10 blocks x agent step limit of one request.
// The default protocol.Limits (4,096 entries) would reject ordinary profiles.
var bProfileLimits = protocol.Limits{MaxBytes: 4 << 20, MaxCollectionEntries: 1 << 16, MaxTotalValues: 1 << 20}

// bProfileSteps runs TRANX_PROFILE {date, txid, max=10} and decodes the steps.
// Java dereferences a missing XLogProfilePack (NPE -> 500 "null"); kept.
func (s *Server) bProfileSteps(ctx context.Context, r *http.Request) ([]protocol.Step, error) {
	if err := bServerID(r); err != nil {
		return nil, err
	}
	txid, err := bPathInt64(r, "txid")
	if err != nil {
		return nil, err
	}
	param := bMap(bEntry("date", bTextV(r.PathValue("yyyymmdd"))), bEntry("txid", bDec(txid)), bEntry("max", bDec(bMaxProfileBlock)))
	pack, err := s.bSingle(ctx, "TRANX_PROFILE", param)
	if err != nil {
		return nil, err
	}
	pp, ok := pack.(*protocol.ProfilePack)
	if !ok || pp == nil {
		return nil, bInternal("null")
	}
	steps, err := protocol.DecodeSteps(pp.Profile, bProfileLimits)
	if err != nil {
		return nil, bInternal(err.Error())
	}
	return steps, nil
}

// GET /v1/profile/{yyyymmdd}/{txid}: List<Step> as raw Jackson beans.
func (s *Server) bProfile(w http.ResponseWriter, r *http.Request) {
	steps, err := s.bProfileSteps(r.Context(), r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	out := make([]jsonObject, 0, len(steps))
	for _, st := range steps {
		o, err := bRawStepJSON(st)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		out = append(out, o)
	}
	s.writeJSON(w, r, out)
}

// bStepMainText is StepEnum.Type.getAssociatedMainTextTypeName for the step
// types whose mainValue is dictionary text, with the hash to resolve.
func bStepMainText(st protocol.Step) (string, int32, bool) {
	switch v := st.(type) {
	case *protocol.MethodStep:
		return "method", v.Hash, true
	case *protocol.MethodStep2:
		return "method", v.Hash, true
	case *protocol.SQLStep:
		return "sql", v.Hash, true
	case *protocol.SQLStep2:
		return "sql", v.Hash, true
	case *protocol.SQLStep3:
		return "sql", v.Hash, true
	case *protocol.APICallStep:
		return "apicall", v.Hash, true
	case *protocol.APICallStep2:
		return "apicall", v.Hash, true
	case *protocol.ThreadSubmitStep:
		return "apicall", v.Hash, true
	case *protocol.HashedMessageStep:
		return "hmsg", v.Hash, true
	case *protocol.ParameterizedMessageStep:
		return "hmsg", v.Hash, true
	case *protocol.DispatchStep:
		return "apicall", v.Hash, true
	case *protocol.ThreadCallPossibleStep:
		return "apicall", v.Hash, true
	case *protocol.SpanStep:
		return "service", v.Hash, true
	case *protocol.SpanCallStep:
		return "service", v.Hash, true
	}
	return "", 0, false
}

// GET /v1/profile-data/{yyyymmdd}/{txid}: List<ProfileStepData{mainValue,
// additionalValueList, step}>. Java resolves text after a GET_TEXT_ANY_TYPE
// preload (no date), so lookups pass an empty date.
func (s *Server) bProfileData(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	steps, err := s.bProfileSteps(ctx, r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	// ProfileStepData.toList: TextLoader preload of main-value and dump-stack hashes.
	var keys []textKey
	for _, st := range steps {
		if typ, h, ok := bStepMainText(st); ok {
			keys = append(keys, textKey{Type: typ, Hash: h})
		}
		if d, ok := st.(*protocol.DumpStep); ok {
			for _, h := range d.Stacks {
				keys = append(keys, textKey{Type: "stackelem", Hash: h})
			}
		}
	}
	_ = s.bPreload(ctx, keys)
	text := func(typ string, h int32) string { return s.text(ctx, "", typ, h) }
	out := make([]jsonObject, 0, len(steps))
	for _, st := range steps {
		main := ""
		if typ, h, ok := bStepMainText(st); ok {
			main = text(typ, h)
		}
		additional := []string{}
		var step jsonObject
		switch v := st.(type) {
		case *protocol.ParameterizedMessageStep:
			main = bBuildMessage(main, v.ParamString)
		case *protocol.MessageStep:
			main = v.Message
		case *protocol.SocketStep:
			main = bIPString(v.IPAddress)
		case *protocol.DumpStep:
			for _, h := range v.Stacks {
				additional = append(additional, text("stackelem", h))
			}
		case *protocol.SpanStep:
			step = bSSpanJSON(v.CommonSpanStep, nil, protocol.StepSpan, func(h int32) string { return text("object", h) })
		case *protocol.SpanCallStep:
			step = bSSpanJSON(v.CommonSpanStep, v, protocol.StepSpanCall, func(h int32) string { return text("object", h) })
		}
		if step == nil {
			if step, err = bRawStepJSON(st); err != nil {
				s.writeErr(w, r, err)
				return
			}
		}
		out = append(out, jsonObject{{Key: "mainValue", Value: main}, {Key: "additionalValueList", Value: additional}, {Key: "step", Value: step}})
	}
	s.writeJSON(w, r, out)
}
