// Scouter wire format: Copyright 2015 the original author or authors,
// https://github.com/scouter-project/scouter, Apache License 2.0.
package protocol

import (
	"fmt"
	"math"
	"reflect"
)

func (e *Encoder) stepSingle(p SingleStep) {
	e.Decimal(int64(p.Parent))
	e.Decimal(int64(p.Index))
	e.Decimal(int64(p.StartTime))
	e.Decimal(int64(p.StartCPU))
}
func (e *Encoder) stepMethod(p MethodStep) {
	e.stepSingle(p.SingleStep)
	e.Decimal(int64(p.Hash))
	e.Decimal(int64(p.Elapsed))
	e.Decimal(int64(p.CPUTime))
}
func (e *Encoder) stepSQL(p SQLStep) {
	e.stepSingle(p.SingleStep)
	e.Decimal(int64(p.Hash))
	e.Decimal(int64(p.Elapsed))
	e.Decimal(int64(p.CPUTime))
	e.Text(p.Param)
	e.Decimal(int64(p.Error))
}
func (e *Encoder) stepCallFields(p CallStepFields) {
	e.stepSingle(p.SingleStep)
	e.Decimal(p.TxID)
	e.Decimal(int64(p.Hash))
	e.Decimal(int64(p.Elapsed))
	e.Decimal(int64(p.CPUTime))
	e.Decimal(int64(p.Error))
}
func (e *Encoder) stepAddress(opt byte, address string) {
	e.Byte(opt)
	if opt == 1 {
		e.Text(address)
	}
}
func (e *Encoder) stepAPICall(p APICallStep) {
	e.stepCallFields(p.CallStepFields)
	e.stepAddress(p.Opt, p.Address)
}
func (e *Encoder) stepSummary(p MethodSum) {
	e.Decimal(int64(p.Hash))
	e.Decimal(int64(p.Count))
	e.Decimal(p.Elapsed)
	e.Decimal(p.CPUTime)
}
func (e *Encoder) stepSpan(p CommonSpanStep) {
	if p.AnnotationTimestamps.Type != ValueList || p.AnnotationValues.Type != ValueList || p.Tags.Type != ValueMap {
		e.fail(ErrInvalid, "Span requires List/List/Map values")
		return
	}
	e.stepSingle(p.SingleStep)
	e.Decimal(int64(p.Hash))
	e.Decimal(int64(p.Elapsed))
	e.Decimal(int64(p.Error))
	e.Decimal(p.Timestamp)
	e.Byte(p.SpanType)
	e.Decimal(int64(p.LocalEndpointServiceName))
	e.Blob(p.LocalEndpointIP)
	e.Int16(p.LocalEndpointPort)
	e.Decimal(int64(p.RemoteEndpointServiceName))
	e.Blob(p.RemoteEndpointIP)
	e.Int16(p.RemoteEndpointPort)
	e.Bool(p.Debug)
	e.Bool(p.Shared)
	e.Value(p.AnnotationTimestamps)
	e.Value(p.AnnotationValues)
	e.Value(p.Tags)
}

func (e *Encoder) Step(step Step) {
	if e.err != nil {
		return
	}
	if step == nil {
		e.fail(ErrInvalid, "nil Step")
		return
	}
	// The API is an interface; catch typed nil before any StepType/body access.
	value := reflect.ValueOf(step)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		e.fail(ErrInvalid, "typed nil Step")
		return
	}
	if e.values >= e.limits.MaxTotalValues {
		e.fail(ErrLimit, "encoded Step aggregate count")
		return
	}
	e.values++
	switch p := step.(type) {
	case *MethodStep:
		e.Byte(StepMethod)
		e.stepMethod(*p)
	case *MethodStep2:
		e.Byte(StepMethod2)
		e.stepMethod(p.MethodStep)
		e.Decimal(int64(p.Error))
	case *SQLStep:
		e.Byte(StepSQL)
		e.stepSQL(*p)
	case *SQLStep2:
		e.Byte(StepSQL2)
		e.stepSQL(p.SQLStep)
		e.Byte(p.XType)
	case *SQLStep3:
		e.Byte(StepSQL3)
		e.stepSQL(p.SQLStep)
		e.Byte(p.XType)
		e.Decimal(int64(p.Updated))
	case *MessageStep:
		e.Byte(StepMessage)
		e.stepSingle(p.SingleStep)
		e.Text(p.Message)
	case *HashedMessageStep:
		e.Byte(StepHashedMessage)
		e.stepSingle(p.SingleStep)
		e.Decimal(int64(p.Hash))
		e.Decimal(int64(p.Time))
		e.Decimal(int64(p.Value))
	case *ParameterizedMessageStep:
		e.Byte(StepParameterizedMessage)
		e.stepSingle(p.SingleStep)
		e.Decimal(int64(p.Hash))
		e.Decimal(int64(p.Elapsed))
		e.Decimal(int64(p.Level))
		e.Text(p.ParamString)
	case *SocketStep:
		e.Byte(StepSocket)
		e.stepSingle(p.SingleStep)
		e.Blob(p.IPAddress)
		e.Decimal(int64(p.Port))
		e.Decimal(int64(p.Elapsed))
		e.Decimal(int64(p.Error))
	case *APICallStep:
		e.Byte(StepAPICall)
		e.stepAPICall(*p)
	case *APICallStep2:
		e.Byte(StepAPICall2)
		e.stepAPICall(p.APICallStep)
		e.Byte(p.Async)
	case *DispatchStep:
		e.Byte(StepDispatch)
		e.stepCallFields(p.CallStepFields)
		e.stepAddress(p.Opt, p.Address)
	case *ThreadSubmitStep:
		e.Byte(StepThreadSubmit)
		e.stepCallFields(p.CallStepFields)
	case *ThreadCallPossibleStep:
		e.Byte(StepThreadCallPossible)
		e.stepSingle(p.SingleStep)
		e.Decimal(p.TxID)
		e.Decimal(int64(p.Hash))
		e.Decimal(int64(p.Elapsed))
		e.Byte(p.Threaded)
	case *DumpStep:
		e.count(len(p.Stacks))
		if len(p.Stacks) > math.MaxInt16 {
			e.fail(ErrInvalid, "Dump stack count does not fit signed int16")
		}
		if e.err != nil {
			return
		}
		e.values += len(p.Stacks)
		e.Byte(StepDump)
		e.stepSingle(p.SingleStep)
		e.Int16(int16(len(p.Stacks)))
		for _, frame := range p.Stacks {
			e.Int32(frame)
		}
		e.Int64(p.ThreadID)
		e.Text(p.ThreadName)
		e.Text(p.ThreadState)
		e.Int64(p.LockOwnerID)
		e.Text(p.LockName)
		e.Text(p.LockOwnerName)
	case *MethodSum:
		e.Byte(StepMethodSum)
		e.stepSummary(*p)
	case *SQLSum:
		e.Byte(StepSQLSum)
		e.stepSummary(p.MethodSum)
		e.Decimal(int64(p.Error))
		e.Text(p.Param)
		e.Text(p.ParamError)
	case *APICallSum:
		if p.Opt != 0 {
			e.fail(ErrInvalid, "API sum writer permits only opt=0")
			return
		}
		e.Byte(StepAPICallSum)
		e.stepSummary(p.MethodSum)
		e.Decimal(int64(p.Error))
		e.Byte(p.Opt)
	case *SocketSum:
		e.Byte(StepSocketSum)
		e.Blob(p.IPAddress)
		e.Decimal(int64(p.Port))
		e.Decimal(int64(p.Count))
		e.Decimal(p.Elapsed)
		e.Decimal(int64(p.Error))
	case *MessageSum:
		e.Byte(StepMessageSum)
		e.Text(p.Message)
		e.Decimal(int64(p.Count))
	case *StepControl:
		e.Byte(StepControlType)
		e.Text(p.Message)
		e.Decimal(int64(p.Code))
	case *SpanStep:
		e.Byte(StepSpan)
		e.stepSpan(p.CommonSpanStep)
	case *SpanCallStep:
		e.Byte(StepSpanCall)
		e.stepSpan(p.CommonSpanStep)
		e.Decimal(p.TxID)
		e.stepAddress(p.Opt, p.Address)
		e.Byte(p.Async)
	default:
		e.fail(ErrUnsupported, fmt.Sprintf("Step implementation %T", step))
	}
}
