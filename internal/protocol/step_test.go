package protocol

import (
	"bytes"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

var stepGoldenIDs = map[string]byte{
	"METHOD": 1, "METHOD2": 10, "SQL": 2, "SQL2": 8, "SQL3": 16, "MESSAGE": 3, "SOCKET": 5,
	"APICALL": 6, "APICALL2": 15, "THREAD_SUBMIT": 7, "HASHED_MESSAGE": 9, "PARAMETERIZED_MESSAGE": 17,
	"DUMP": 12, "DISPATCH": 13, "THREAD_CALL_POSSIBLE": 14, "METHOD_SUM": 11, "SQL_SUM": 21,
	"MESSAGE_SUM": 31, "SOCKET_SUM": 42, "APICALL_SUM": 43, "CONTROL": 99, "SPAN": 51, "SPANCALL": 52,
}

func TestJavaStepGolden(t *testing.T) {
	for name, id := range stepGoldenIDs {
		for _, suffix := range []string{"", "-zero"} {
			t.Run(name+suffix, func(t *testing.T) {
				raw := fixture(t, "step-"+name+suffix)
				s, err := DecodeStep(raw, Limits{})
				if err != nil {
					t.Fatal(err)
				}
				if s.StepType() != id {
					t.Fatalf("Step ID %d != %d", s.StepType(), id)
				}
				encoded, err := EncodeStep(s, Limits{})
				if err != nil || !bytes.Equal(encoded, raw) {
					t.Fatalf("Java Step byte mismatch: %v", err)
				}
				if suffix == "" {
					assertStepSample(t, s)
				}
				// Every single-Step prefix is incomplete. Empty is only valid as a sequence.
				for n := 0; n < len(raw); n++ {
					if _, err := DecodeStep(raw[:n], Limits{}); err == nil {
						t.Fatalf("truncated Step %s accepted at %d", name, n)
					}
				}
			})
		}
	}
}

func assertStepSample(t *testing.T, s Step) {
	t.Helper()
	single := SingleStep{Parent: -7, Index: 17, StartTime: 128, StartCPU: 127}
	method := MethodStep{SingleStep: single, Hash: -1234567, Elapsed: 83, CPUTime: 84}
	sql := SQLStep{SingleStep: single, Hash: -1234567, Elapsed: 83, CPUTime: 84, Param: "synthetic-param 한글", Error: -32769}
	calls := CallStepFields{SingleStep: single, TxID: math.MinInt64 + 7, Hash: -1234567, Elapsed: 83, CPUTime: 84, Error: -32769}
	summary := MethodSum{Hash: -1234567, Count: 3, Elapsed: 549755813887, CPUTime: math.MaxInt64 - 7}
	var want Step
	switch s.(type) {
	case *MethodStep:
		want = &method
	case *MethodStep2:
		want = &MethodStep2{MethodStep: method, Error: -32769}
	case *SQLStep:
		want = &sql
	case *SQLStep2:
		want = &SQLStep2{SQLStep: sql, XType: 249}
	case *SQLStep3:
		want = &SQLStep3{SQLStep2: SQLStep2{SQLStep: sql, XType: 249}, Updated: -2}
	case *MessageStep:
		want = &MessageStep{SingleStep: single, Message: "synthetic-message 한글"}
	case *SocketStep:
		want = &SocketStep{SingleStep: single, IPAddress: []byte{192, 0, 2, 7}, Port: 33000, Elapsed: 83, Error: -32769}
	case *APICallStep:
		want = &APICallStep{CallStepFields: calls, Opt: 1, Address: "synthetic-address 한글"}
	case *APICallStep2:
		want = &APICallStep2{APICallStep: APICallStep{CallStepFields: calls, Opt: 1, Address: "synthetic-address 한글"}, Async: 249}
	case *ThreadSubmitStep:
		want = &ThreadSubmitStep{CallStepFields: calls}
	case *DispatchStep:
		want = &DispatchStep{CallStepFields: calls, Opt: 1, Address: "synthetic-address 한글"}
	case *ThreadCallPossibleStep:
		want = &ThreadCallPossibleStep{SingleStep: single, TxID: math.MinInt64 + 7, Hash: -1234567, Elapsed: 83, Threaded: 1}
	case *HashedMessageStep:
		want = &HashedMessageStep{SingleStep: single, Hash: -1234567, Time: 85, Value: 86}
	case *ParameterizedMessageStep:
		want = &ParameterizedMessageStep{SingleStep: single, Hash: -1234567, Elapsed: 83, Level: -7, ParamString: "synthetic\x03한글\x03"}
	case *DumpStep:
		want = &DumpStep{SingleStep: single, Stacks: []int32{math.MinInt32, -7, 0, math.MaxInt32}, ThreadID: math.MinInt64 + 7, ThreadName: "synthetic-threadName 한글", ThreadState: "synthetic-threadState 한글", LockOwnerID: math.MaxInt64 - 9, LockName: "synthetic-lockName 한글", LockOwnerName: "synthetic-lockOwnerName 한글"}
	case *MethodSum:
		want = &summary
	case *SQLSum:
		want = &SQLSum{MethodSum: summary, Error: -32769, Param: "synthetic-param 한글", ParamError: "synthetic-param_error 한글"}
	case *APICallSum:
		want = &APICallSum{MethodSum: summary, Error: -32769, Opt: 0}
	case *SocketSum:
		want = &SocketSum{IPAddress: []byte{192, 0, 2, 7}, Port: 33000, Count: 3, Elapsed: 549755813887, Error: -32769}
	case *MessageSum:
		want = &MessageSum{Message: "synthetic-message 한글", Count: 3}
	case *StepControl:
		want = &StepControl{Message: "synthetic-message 한글", Code: 99}
	case *SpanStep:
		assertSpanSample(t, &s.(*SpanStep).CommonSpanStep)
		return
	case *SpanCallStep:
		v := s.(*SpanCallStep)
		assertSpanSample(t, &v.CommonSpanStep)
		if v.TxID != math.MinInt64+7 || v.Opt != 1 || v.Address != "synthetic-address 한글" || v.Async != 249 {
			t.Fatal("SpanCall suffix")
		}
		return
	default:
		t.Fatalf("unexpected Step implementation %T", s)
	}
	if !reflect.DeepEqual(s, want) {
		t.Fatalf("Java semantics mismatch:\ngot %#v\nwant %#v", s, want)
	}
}

func assertSpanSample(t *testing.T, v *CommonSpanStep) {
	t.Helper()
	if v.SingleStep != (SingleStep{Parent: -7, Index: 17, StartTime: 128, StartCPU: 127}) || v.Hash != -1234567 || v.Elapsed != 83 || v.Error != -32769 || v.Timestamp != 1700000000123 || v.SpanType != 249 || v.LocalEndpointServiceName != 101 || !bytes.Equal(v.LocalEndpointIP, []byte{192, 0, 2, 7}) || v.LocalEndpointPort != -32536 || v.RemoteEndpointServiceName != 102 || !bytes.Equal(v.RemoteEndpointIP, []byte{192, 0, 2, 7}) || v.RemoteEndpointPort != -32536 || !v.Debug || !v.Shared {
		t.Fatal("CommonSpan scalar fields")
	}
	times := v.AnnotationTimestamps
	if times.Type != ValueList || len(times.Values) != 2 || times.Values[0].Type != ValueDecimal || times.Values[0].Int != 1700000000123 || times.Values[1].Int != 1700000000999 {
		t.Fatal("Span timestamp list")
	}
	vals := v.AnnotationValues
	if vals.Type != ValueList || len(vals.Values) != 2 || vals.Values[0].Type != ValueText || vals.Values[0].Text != "synthetic-start" || vals.Values[1].Text != "synthetic-end" {
		t.Fatal("Span annotation values")
	}
	if v.Tags.Type != ValueMap || len(v.Tags.Entries) != 2 {
		t.Fatal("Span tag map")
	}
	for _, entry := range v.Tags.Entries {
		switch entry.Key {
		case "z":
			if entry.Value.Type != ValueDecimal || entry.Value.Int != 9 {
				t.Fatal("Span decimal tag")
			}
		case "a":
			if entry.Value.Type != ValueText || entry.Value.Text != "synthetic-tag" {
				t.Fatal("Span text tag")
			}
		default:
			t.Fatal("Span unknown tag")
		}
	}
}

func TestJavaStepOptionalAddresses(t *testing.T) {
	for _, name := range []string{"APICALL", "APICALL2", "DISPATCH", "SPANCALL"} {
		for _, suffix := range []string{"-opt0", "-opt255"} {
			t.Run(name+suffix, func(t *testing.T) {
				raw := fixture(t, "step-"+name+suffix)
				step, err := DecodeStep(raw, Limits{})
				if err != nil {
					t.Fatal(err)
				}
				opt := byte(0)
				if suffix == "-opt255" {
					opt = 255
				}
				switch v := step.(type) {
				case *APICallStep:
					if v.Opt != opt || v.Address != "" {
						t.Fatal("API option")
					}
				case *APICallStep2:
					if v.Opt != opt || v.Address != "" || v.Async != 249 {
						t.Fatal("API2 option")
					}
				case *DispatchStep:
					if v.Opt != opt || v.Address != "" {
						t.Fatal("Dispatch option")
					}
				case *SpanCallStep:
					if v.Opt != opt || v.Address != "" || v.Async != 249 {
						t.Fatal("SpanCall option")
					}
				}
				got, err := EncodeStep(step, Limits{})
				if err != nil || !bytes.Equal(got, raw) {
					t.Fatal("optional address framing", err)
				}
			})
		}
	}
}

func TestJavaEOFStepSequences(t *testing.T) {
	forward := []byte{1, 10, 2, 8, 16, 3, 5, 6, 15, 7, 9, 17, 12, 13, 14, 11, 21, 31, 42, 43, 99, 51, 52}
	for _, name := range []string{"step-sequence", "step-sequence-reversed", "step-empty"} {
		t.Run(name, func(t *testing.T) {
			raw := fixture(t, name)
			steps, err := DecodeSteps(raw, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			want := forward
			if name == "step-empty" {
				want = nil
			}
			if len(steps) != len(want) {
				t.Fatal("EOF sequence count", len(steps))
			}
			for i, s := range steps {
				at := i
				if name == "step-sequence-reversed" {
					at = len(want) - 1 - i
				}
				if s.StepType() != want[at] {
					t.Fatal("Step input order lost", i)
				}
			}
			encoded, err := EncodeSteps(steps, Limits{})
			if err != nil || !bytes.Equal(encoded, raw) {
				t.Fatal("Java EOF sequence byte mismatch", err)
			}
		})
	}
	raw := fixture(t, "step-METHOD")
	if _, err := DecodeStep(append(bytes.Clone(raw), raw...), Limits{}); !errors.Is(err, ErrTrailing) {
		t.Fatal("single Step consumed sequence", err)
	}
}

type unknownTestStep struct{}

func (unknownTestStep) StepType() byte { return 255 }

func TestStepMalformedAndLimits(t *testing.T) {
	if _, err := DecodeStep([]byte{255}, Limits{}); !errors.Is(err, ErrUnsupported) {
		t.Fatal("unknown Step", err)
	}
	if _, err := EncodeStep(unknownTestStep{}, Limits{}); !errors.Is(err, ErrUnsupported) {
		t.Fatal("unknown Step encoding", err)
	}
	if _, err := EncodeStep(nil, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil Step", err)
	}
	var nilMethod *MethodStep
	var nilSQL *SQLStep3
	var nilSpan *SpanStep
	var nilDump *DumpStep
	for _, s := range []Step{nilMethod, nilSQL, nilSpan, nilDump} {
		if _, err := EncodeStep(s, Limits{}); !errors.Is(err, ErrInvalid) {
			t.Fatal("typed nil Step", err)
		}
	}
	raw := []byte{12, 0, 0, 0, 0, 255, 255}
	if _, err := DecodeStep(raw, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("negative Dump array count", err)
	}
	raw = []byte{12, 0, 0, 0, 0, 0, 4}
	if _, err := DecodeStep(raw, Limits{MaxCollectionEntries: 3}); !errors.Is(err, ErrLimit) {
		t.Fatal("Dump count bound", err)
	}
	raw = []byte{3, 0, 0, 0, 0, 254, 255, 255, 255, 255}
	if _, err := DecodeStep(raw, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("negative Message length", err)
	}
	raw = []byte{3, 0, 0, 0, 0, 254, 127, 255, 255, 255}
	if _, err := DecodeStep(raw, Limits{}); !errors.Is(err, ErrLimit) {
		t.Fatal("Message length bound", err)
	}
	raw = fixture(t, "step-METHOD")
	sequence := append(bytes.Clone(raw), raw...)
	if _, err := DecodeSteps(sequence, Limits{MaxCollectionEntries: 1}); !errors.Is(err, ErrLimit) {
		t.Fatal("sequence count bound", err)
	}
	if _, err := DecodeSteps(sequence, Limits{MaxTotalValues: 1}); !errors.Is(err, ErrLimit) {
		t.Fatal("aggregate Step budget", err)
	}
	if _, err := DecodeSteps(sequence, Limits{MaxBytes: len(raw)}); !errors.Is(err, ErrLimit) {
		t.Fatal("sequence frame bound", err)
	}
	if _, err := EncodeSteps([]Step{&MethodStep{}, &MethodStep{}}, Limits{MaxCollectionEntries: 1}); !errors.Is(err, ErrLimit) {
		t.Fatal("encoded sequence count", err)
	}
	if _, err := EncodeSteps([]Step{&MethodStep{}, &MethodStep{}}, Limits{MaxTotalValues: 1}); !errors.Is(err, ErrLimit) {
		t.Fatal("encoded aggregate budget", err)
	}
	if _, err := EncodeStep(&MessageStep{Message: "ab"}, Limits{MaxBlobBytes: 1}); !errors.Is(err, ErrLimit) {
		t.Fatal("encoded Message limit", err)
	}
	if _, err := EncodeStep(&DumpStep{Stacks: []int32{1, 2}}, Limits{MaxCollectionEntries: 1}); !errors.Is(err, ErrLimit) {
		t.Fatal("encoded Dump count limit", err)
	}
	if _, err := EncodeStep(&APICallSum{Opt: 1}, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("Java writer rejects API sum opt1", err)
	}
	badSum := fixture(t, "step-APICALL_SUM")
	badSum[len(badSum)-1] = 1
	if _, err := DecodeStep(badSum, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("non-roundtrippable API sum option", err)
	}
	badLevel := []byte{17, 0, 0, 0, 0, 0, 0, 2, 0, 128, 0}
	if _, err := DecodeStep(badLevel, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("int8 Parameterized level overflow", err)
	}
	badIndex := []byte{1, 0, 8, 127, 255, 255, 255, 255, 255, 255, 255}
	if _, err := DecodeStep(badIndex, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("int32 Single index overflow", err)
	}
}

func TestSpanStepValueContractsAndDepth(t *testing.T) {
	valid := CommonSpanStep{AnnotationTimestamps: Value{Type: ValueList}, AnnotationValues: Value{Type: ValueList}, Tags: Value{Type: ValueMap}}
	if _, err := EncodeStep(&SpanStep{}, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("Span invalid typed default Values", err)
	}
	nested := valid
	nested.Tags = Value{Type: ValueMap, Entries: []Entry{{Key: "nested", Value: Value{Type: ValueList, Values: []Value{{Type: ValueNull}}}}}}
	raw, err := EncodeStep(&SpanStep{CommonSpanStep: nested}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeStep(raw, Limits{MaxDepth: 2}); !errors.Is(err, ErrLimit) {
		t.Fatal("Span nested depth bound", err)
	}
	if _, err := EncodeStep(&SpanStep{CommonSpanStep: nested}, Limits{MaxDepth: 2}); !errors.Is(err, ErrLimit) {
		t.Fatal("Span encode depth bound", err)
	}
	if _, err := DecodeStep(fixture(t, "step-SPAN"), Limits{MaxTotalValues: 3}); !errors.Is(err, ErrLimit) {
		t.Fatal("Step+Value aggregate count", err)
	}
	encoded, err := EncodeStep(&SpanStep{CommonSpanStep: valid}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	// End is ValueList(count0), ValueList(count0), ValueMap(count0).
	encoded[len(encoded)-6] = ValueMap
	if _, err := DecodeStep(encoded, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("Span annotation List cast contract", err)
	}
}

func TestStepBytesSurviveLeaseReuse(t *testing.T) {
	for _, name := range []string{"SOCKET", "DUMP", "SPAN", "SPANCALL"} {
		raw := fixture(t, "step-"+name)
		want := bytes.Clone(raw)
		step, err := DecodeStep(raw, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range raw {
			raw[i] = 0
		}
		got, err := EncodeStep(step, Limits{})
		if err != nil || !bytes.Equal(got, want) {
			t.Fatal("Step aliases reused input", name, err)
		}
	}
}

func FuzzDecodeSteps(f *testing.F) {
	paths, err := filepath.Glob("testdata/java/step-*.bin")
	if err != nil || len(paths) != 57 {
		f.Fatal("Step oracle coverage missing", len(paths), err)
	}
	for _, p := range paths {
		f.Add(fixture(f, strings.TrimSuffix(filepath.Base(p), ".bin")))
	}
	f.Add([]byte{255})
	f.Add([]byte{12, 0, 0, 0, 0, 255, 255})
	f.Fuzz(func(t *testing.T, raw []byte) {
		limits := Limits{MaxBytes: 1 << 16, MaxBlobBytes: 1 << 15, MaxCollectionEntries: 128, MaxTotalValues: 512, MaxDepth: 8}
		steps, err := DecodeSteps(raw, limits)
		if err != nil {
			return
		}
		encoded, err := EncodeSteps(steps, limits)
		if err != nil {
			t.Fatal("decoded Steps cannot encode", err)
		}
		decoded, err := DecodeSteps(encoded, limits)
		if err != nil || len(decoded) != len(steps) {
			t.Fatal("Step reencoding invalid", err)
		}
	})
}
