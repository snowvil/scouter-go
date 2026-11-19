// Scouter wire format: Copyright 2015 the original author or authors,
// https://github.com/scouter-project/scouter, Apache License 2.0.
// Independently implemented from the pinned Java StepEnum and concrete Steps.
package protocol

const (
	StepMethod               byte = 1
	StepSQL                  byte = 2
	StepMessage              byte = 3
	StepSocket               byte = 5
	StepAPICall              byte = 6
	StepThreadSubmit         byte = 7
	StepSQL2                 byte = 8
	StepHashedMessage        byte = 9
	StepMethod2              byte = 10
	StepMethodSum            byte = 11
	StepDump                 byte = 12
	StepDispatch             byte = 13
	StepThreadCallPossible   byte = 14
	StepAPICall2             byte = 15
	StepSQL3                 byte = 16
	StepParameterizedMessage byte = 17
	StepSQLSum               byte = 21
	StepMessageSum           byte = 31
	StepSocketSum            byte = 42
	StepAPICallSum           byte = 43
	StepSpan                 byte = 51
	StepSpanCall             byte = 52
	StepControlType          byte = 99
)

// Step is one wire record, not a producer's filtering/sorting instruction.
// All 23 IDs in the pinned Java StepEnum are implemented. Unknown types fail.
type Step interface{ StepType() byte }
type SingleStep struct{ Parent, Index, StartTime, StartCPU int32 }

type MethodStep struct {
	SingleStep
	Hash, Elapsed, CPUTime int32
}

func (*MethodStep) StepType() byte { return StepMethod }

type MethodStep2 struct {
	MethodStep
	Error int32
}

func (*MethodStep2) StepType() byte { return StepMethod2 }

type SQLStep struct {
	SingleStep
	Hash, Elapsed, CPUTime int32
	Param                  string
	Error                  int32
}

func (*SQLStep) StepType() byte { return StepSQL }

type SQLStep2 struct {
	SQLStep
	XType byte
}

func (*SQLStep2) StepType() byte { return StepSQL2 }

type SQLStep3 struct {
	SQLStep2
	Updated int32
}

func (*SQLStep3) StepType() byte { return StepSQL3 }

type MessageStep struct {
	SingleStep
	Message string
}

func (*MessageStep) StepType() byte { return StepMessage }

type HashedMessageStep struct {
	SingleStep
	Hash, Time, Value int32
}

func (*HashedMessageStep) StepType() byte { return StepHashedMessage }

type ParameterizedMessageStep struct {
	SingleStep
	Hash, Elapsed int32
	Level         int8
	ParamString   string
}

func (*ParameterizedMessageStep) StepType() byte { return StepParameterizedMessage }

type SocketStep struct {
	SingleStep
	IPAddress            []byte
	Port, Elapsed, Error int32
}

func (*SocketStep) StepType() byte { return StepSocket }

// Three Java Step classes share these call fields in exactly this wire order.
type CallStepFields struct {
	SingleStep
	TxID                          int64
	Hash, Elapsed, CPUTime, Error int32
}
type APICallStep struct {
	CallStepFields
	Opt     byte
	Address string
}

func (*APICallStep) StepType() byte { return StepAPICall }

type APICallStep2 struct {
	APICallStep
	Async byte
}

func (*APICallStep2) StepType() byte { return StepAPICall2 }

type DispatchStep struct {
	CallStepFields
	Opt     byte
	Address string
}

func (*DispatchStep) StepType() byte { return StepDispatch }

type ThreadSubmitStep struct{ CallStepFields }

func (*ThreadSubmitStep) StepType() byte { return StepThreadSubmit }

type ThreadCallPossibleStep struct {
	SingleStep
	TxID          int64
	Hash, Elapsed int32
	Threaded      byte
}

func (*ThreadCallPossibleStep) StepType() byte { return StepThreadCallPossible }

type DumpStep struct {
	SingleStep
	Stacks                  []int32
	ThreadID                int64
	ThreadName, ThreadState string
	LockOwnerID             int64
	LockName, LockOwnerName string
}

func (*DumpStep) StepType() byte { return StepDump }

type MethodSum struct {
	Hash, Count      int32
	Elapsed, CPUTime int64
}

func (*MethodSum) StepType() byte { return StepMethodSum }

type SQLSum struct {
	MethodSum
	Error             int32
	Param, ParamError string
}

func (*SQLSum) StepType() byte { return StepSQLSum }

type APICallSum struct {
	MethodSum
	Error int32
	Opt   byte
}

func (*APICallSum) StepType() byte { return StepAPICallSum }

type SocketSum struct {
	IPAddress   []byte
	Port, Count int32
	Elapsed     int64
	Error       int32
}

func (*SocketSum) StepType() byte { return StepSocketSum }

type MessageSum struct {
	Message string
	Count   int32
}

func (*MessageSum) StepType() byte { return StepMessageSum }

type StepControl struct {
	Message string
	Code    int32
}

func (*StepControl) StepType() byte { return StepControlType }

type CommonSpanStep struct {
	SingleStep
	Hash, Elapsed, Error                         int32
	Timestamp                                    int64
	SpanType                                     byte
	LocalEndpointServiceName                     int32
	LocalEndpointIP                              []byte
	LocalEndpointPort                            int16
	RemoteEndpointServiceName                    int32
	RemoteEndpointIP                             []byte
	RemoteEndpointPort                           int16
	Debug, Shared                                bool
	AnnotationTimestamps, AnnotationValues, Tags Value
}
type SpanStep struct{ CommonSpanStep }

func (*SpanStep) StepType() byte { return StepSpan }

type SpanCallStep struct {
	CommonSpanStep
	TxID    int64
	Opt     byte
	Address string
	Async   byte
}

func (*SpanCallStep) StepType() byte { return StepSpanCall }
