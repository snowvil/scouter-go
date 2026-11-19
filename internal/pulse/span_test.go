package pulse

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func TestEncodeSingletonStep(t *testing.T) {
	method := &protocol.MethodStep{SingleStep: protocol.SingleStep{Parent: -1, Index: 2, StartTime: 300}, Hash: 10, Elapsed: 5, CPUTime: 1}
	got, err := EncodeSingletonStep(method)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := protocol.EncodeStep(method, StepLimits)
	if !bytes.Equal(got, want) {
		t.Fatalf("ordinary step must equal DataOutputX.writeStep: %x vs %x", got, want)
	}
	// Step.toBytes(List.of(step)) is exactly one writeStep with no count prefix.
	if seq, _ := protocol.EncodeSteps([]protocol.Step{method}, StepLimits); !bytes.Equal(seq, want) {
		t.Fatalf("EncodeSteps singleton %x vs %x", seq, want)
	}
	// The plugin span step models inherit StepSingle.write: type byte plus four
	// Decimals (parent=-1, index=2, start_time=300, start_cpu forced to 0
	// because the plugin property copy does not copy it).
	single := protocol.SingleStep{Parent: -1, Index: 2, StartTime: 300, StartCPU: 9}
	span := &protocol.SpanStep{CommonSpanStep: protocol.CommonSpanStep{SingleStep: single, Hash: 77, Elapsed: 9, Timestamp: 123456789,
		LocalEndpointIP: []byte{1, 2, 3, 4}, AnnotationTimestamps: protocol.Value{Type: protocol.ValueList}, AnnotationValues: protocol.Value{Type: protocol.ValueList}, Tags: protocol.Value{Type: protocol.ValueMap}}}
	got, err = EncodeSingletonStep(span)
	if err != nil {
		t.Fatal(err)
	}
	wantSpan := []byte{51, 1, 0xFF, 1, 2, 2, 0x01, 0x2C, 0}
	if !bytes.Equal(got, wantSpan) {
		t.Fatalf("SPAN step bytes %x want %x", got, wantSpan)
	}
	full, _ := protocol.EncodeStep(span, StepLimits)
	if bytes.Equal(got, full) {
		t.Fatal("plugin span encoding must not equal the standard CommonSpanStep encoding")
	}
	call := &protocol.SpanCallStep{CommonSpanStep: span.CommonSpanStep, TxID: 5, Opt: 1, Address: "x", Async: 1}
	got, _ = EncodeSingletonStep(call)
	if !bytes.Equal(got, append([]byte{52}, wantSpan[1:]...)) {
		t.Fatalf("SPANCALL step bytes %x", got)
	}
	// The truncated bytes are not decodable as a standard SpanStep.
	if _, err := protocol.DecodeStep(got, StepLimits); err == nil {
		t.Fatal("standard decoder accepted truncated span bytes")
	}
}

func TestConvertProfileStepsMainValues(t *testing.T) {
	d := newDict()
	now := "20261014"
	d.put(now, TextMethod, 10, "Svc.find()")
	d.put(now, TextSQL, 20, "select 1")
	d.put(now, TextAPICall, 30, "GET /remote")
	d.put(now, TextHashMessage, 40, "x=%s y=%s")
	d.put(now, TextHashMessage, 41, "  ")
	d.put(now, TextStackElement, 1, "a.b.C.m(C.java:1)")
	d.put(now, TextService, 50, "/span/svc")
	d.put("20261013", TextMethod, 11, "only-event-date")
	steps := []protocol.Step{
		&protocol.MethodStep{Hash: 10},
		&protocol.MethodStep2{MethodStep: protocol.MethodStep{Hash: 11}}, // present only at event date -> unlabeled
		&protocol.SQLStep3{SQLStep2: protocol.SQLStep2{SQLStep: protocol.SQLStep{Hash: 20}}},
		&protocol.SQLStep{Hash: 0},
		&protocol.APICallStep2{APICallStep: protocol.APICallStep{CallStepFields: protocol.CallStepFields{Hash: 30}}},
		&protocol.ThreadSubmitStep{CallStepFields: protocol.CallStepFields{Hash: 30}},
		&protocol.DispatchStep{CallStepFields: protocol.CallStepFields{Hash: 31}},
		&protocol.ThreadCallPossibleStep{Hash: 30, Threaded: 0},
		&protocol.HashedMessageStep{Hash: 41},
		&protocol.ParameterizedMessageStep{Hash: 40, ParamString: "1\x032"},
		&protocol.ParameterizedMessageStep{Hash: 42, ParamString: "1"},
		&protocol.MessageStep{Message: "hello \"msg\""},
		&protocol.MessageStep{},
		&protocol.SocketStep{IPAddress: []byte{10, 1, 2, 3}, Port: 80},
		&protocol.SocketStep{IPAddress: []byte{1, 2}},
		&protocol.SocketStep{IPAddress: []byte{192, 168, 0, 1, 5, 6}},
		&protocol.DumpStep{Stacks: []int32{1, 2, 0}},
		&protocol.DumpStep{},
		&protocol.SpanStep{CommonSpanStep: spanCommon(50)},
		&protocol.SpanCallStep{CommonSpanStep: spanCommon(51)},
		&protocol.MethodSum{Hash: 10},
		&protocol.StepControl{Message: "c"},
	}
	profile, err := protocol.EncodeSteps(steps, StepLimits)
	if err != nil {
		t.Fatal(err)
	}
	out, err := ConvertProfileSteps(context.Background(), d.lookup, profile, now)
	if err != nil {
		t.Fatal(err)
	}
	wantMain := []string{"Svc.find()", "**unlabeled**:method:11", "select 1", "", "GET /remote", "GET /remote", "**unlabeled**:apicall:31", "GET /remote",
		"**unlabeled**:hmsg:41", "x=1 y=2", "**unlabeled**:hmsg:42", "hello \"msg\"", "", "10.1.2.3", "0.0.0.0", "192.168.0.1", "", "", "/span/svc", "**unlabeled**:service:51", "", ""}
	if len(out) != len(wantMain) {
		t.Fatalf("%d steps, want %d", len(out), len(wantMain))
	}
	for i, w := range wantMain {
		if out[i].MainValue == nil || *out[i].MainValue != w {
			t.Errorf("step %d (%T) main=%v want %q", i, steps[i], out[i].MainValue, w)
		}
		if i != 16 && len(out[i].AdditionalValueList) != 0 {
			t.Errorf("step %d additional %v", i, out[i].AdditionalValueList)
		}
		want, _ := EncodeSingletonStep(steps[i])
		if !bytes.Equal(out[i].Step, want) {
			t.Errorf("step %d bytes %x want %x", i, out[i].Step, want)
		}
	}
	if fmt.Sprint(out[16].AdditionalValueList) != fmt.Sprint([]string{"a.b.C.m(C.java:1)", "**unlabeled**:stackelem:2", ""}) {
		t.Fatal(out[16].AdditionalValueList)
	}
	// Main/stack lookups use the preload (current) date only; SPAN endpoint
	// hashes are never looked up because they are not serialized.
	for _, c := range d.calls {
		if c.date != now {
			t.Fatalf("lookup at %v", c)
		}
		if c.div == TextObject {
			t.Fatalf("endpoint lookup %v", c)
		}
	}
	if empty, err := ConvertProfileSteps(context.Background(), d.lookup, []byte{}, now); err != nil || empty == nil || len(empty) != 0 {
		t.Fatal("empty profile must be an empty list", empty, err)
	}
	if nilList, err := ConvertProfileSteps(context.Background(), d.lookup, nil, now); err != nil || nilList != nil {
		t.Fatal("null profile must stay null", nilList, err)
	}
	if _, err := ConvertProfileSteps(context.Background(), d.lookup, []byte{200, 1}, now); err == nil {
		t.Fatal("unknown step type must fail the task")
	}
}

func spanCommon(hash int32) protocol.CommonSpanStep {
	return protocol.CommonSpanStep{Hash: hash, LocalEndpointServiceName: 123, RemoteEndpointServiceName: 124,
		AnnotationTimestamps: protocol.Value{Type: protocol.ValueList}, AnnotationValues: protocol.Value{Type: protocol.ValueList}, Tags: protocol.Value{Type: protocol.ValueMap}}
}

func TestConvertProfileAndJSON(t *testing.T) {
	d := newDict()
	eventTime := time.Date(2026, 10, 13, 23, 30, 0, 0, seoul)
	now := time.Date(2026, 10, 14, 0, 10, 0, 0, seoul) // preload date differs from the event date
	d.put("20261013", TextObject, 100, "/host/app@1")
	d.put("20261013", TextService, 200, "/api/v1/users")
	d.put("20261014", TextMethod, 10, "Svc.find()")
	d.put("20261013", TextMethod, 10, "stale")
	steps := []protocol.Step{&protocol.MethodStep{SingleStep: protocol.SingleStep{Index: 1}, Hash: 10}, &protocol.MessageStep{Message: "m"}}
	profile, _ := protocol.EncodeSteps(steps, StepLimits)
	p := &protocol.ProfilePack{Time: eventTime.UnixMilli(), ObjHash: 100, Service: 200, TxID: 42, Profile: profile}
	span, err := ConvertProfile(context.Background(), d.lookup, seoul, p, 0, now)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := span.MarshalJSON()
	step0, _ := EncodeSingletonStep(steps[0])
	step1, _ := EncodeSingletonStep(steps[1])
	want := `{"traceId":42,"time":` + fmt.Sprint(p.Time) + `,"objectDictId":100,"transactionDictId":200,"elapsed":0,"profile":[` +
		`{"mainValue":"Svc.find()","additionalValueList":[],"step":"` + base64.StdEncoding.EncodeToString(step0) + `"},` +
		`{"mainValue":"m","additionalValueList":[],"step":"` + base64.StdEncoding.EncodeToString(step1) + `"}],` +
		`"objectDictContent":"/host/app@1","transactionDictContent":"/api/v1/users"}`
	if string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	wantCalls := []dictKey{{"20261014", TextMethod, 10}, {"20261013", TextObject, 100}, {"20261013", TextService, 200}}
	if fmt.Sprint(d.calls) != fmt.Sprint(wantCalls) {
		t.Fatalf("calls %v", d.calls)
	}
	nilProfile := &protocol.ProfilePack{Time: eventTime.UnixMilli(), ObjHash: 100, Service: 0}
	span, _ = ConvertProfile(context.Background(), d.lookup, seoul, nilProfile, 0, now)
	if got, _ := span.MarshalJSON(); string(got) != `{"traceId":0,"time":`+fmt.Sprint(p.Time)+`,"objectDictId":100,"transactionDictId":0,"elapsed":0,"profile":null,"objectDictContent":"/host/app@1","transactionDictContent":null}` {
		t.Fatal(string(got))
	}
	span.Rebase(RebaseCondition{"/", 2})
	if span.TransactionDictContent != nil {
		t.Fatal("null content stays null")
	}
}
