// Scouter wire format: Copyright 2015 the original author or authors,
// https://github.com/scouter-project/scouter, Apache License 2.0.
package protocol

import (
	"fmt"
	"math"
)

func (d *Decoder) stepSingle() SingleStep {
	return SingleStep{Parent: d.decimalInt32(), Index: d.decimalInt32(), StartTime: d.decimalInt32(), StartCPU: d.decimalInt32()}
}
func (d *Decoder) stepMethod() MethodStep {
	return MethodStep{SingleStep: d.stepSingle(), Hash: d.decimalInt32(), Elapsed: d.decimalInt32(), CPUTime: d.decimalInt32()}
}
func (d *Decoder) stepSQL() SQLStep {
	return SQLStep{SingleStep: d.stepSingle(), Hash: d.decimalInt32(), Elapsed: d.decimalInt32(), CPUTime: d.decimalInt32(), Param: d.Text(), Error: d.decimalInt32()}
}
func (d *Decoder) stepCallFields() CallStepFields {
	return CallStepFields{SingleStep: d.stepSingle(), TxID: d.Decimal(), Hash: d.decimalInt32(), Elapsed: d.decimalInt32(), CPUTime: d.decimalInt32(), Error: d.decimalInt32()}
}
func (d *Decoder) stepAddress() (byte, string) {
	opt := d.Byte()
	if opt == 1 {
		return opt, d.Text()
	}
	return opt, ""
}
func (d *Decoder) stepAPICall() APICallStep {
	p := APICallStep{CallStepFields: d.stepCallFields()}
	p.Opt, p.Address = d.stepAddress()
	return p
}
func (d *Decoder) stepSummary() MethodSum {
	return MethodSum{Hash: d.decimalInt32(), Count: d.decimalInt32(), Elapsed: d.Decimal(), CPUTime: d.Decimal()}
}
func (d *Decoder) stepSpan() CommonSpanStep {
	p := CommonSpanStep{SingleStep: d.stepSingle(), Hash: d.decimalInt32(), Elapsed: d.decimalInt32(), Error: d.decimalInt32(), Timestamp: d.Decimal(), SpanType: d.Byte(), LocalEndpointServiceName: d.decimalInt32(), LocalEndpointIP: d.Blob(), LocalEndpointPort: d.Int16(), RemoteEndpointServiceName: d.decimalInt32(), RemoteEndpointIP: d.Blob(), RemoteEndpointPort: d.Int16(), Debug: d.Bool(), Shared: d.Bool(), AnnotationTimestamps: d.Value(), AnnotationValues: d.Value(), Tags: d.Value()}
	if d.err == nil && (p.AnnotationTimestamps.Type != ValueList || p.AnnotationValues.Type != ValueList || p.Tags.Type != ValueMap) {
		d.fail(ErrInvalid, "Span requires List/List/Map values")
	}
	return p
}

// Step uses the same sticky error and aggregate allocation budget as Value/Pack.
// One Step plus each Dump stack item consumes one MaxTotalValues budget item.
func (d *Decoder) Step() Step {
	if d.err != nil {
		return nil
	}
	if d.values >= d.limits.MaxTotalValues {
		d.fail(ErrLimit, "Step aggregate count")
		return nil
	}
	d.values++
	switch kind := d.Byte(); kind {
	case StepMethod:
		p := d.stepMethod()
		return &p
	case StepMethod2:
		return &MethodStep2{MethodStep: d.stepMethod(), Error: d.decimalInt32()}
	case StepSQL:
		p := d.stepSQL()
		return &p
	case StepSQL2:
		return &SQLStep2{SQLStep: d.stepSQL(), XType: d.Byte()}
	case StepSQL3:
		return &SQLStep3{SQLStep2: SQLStep2{SQLStep: d.stepSQL(), XType: d.Byte()}, Updated: d.decimalInt32()}
	case StepMessage:
		return &MessageStep{SingleStep: d.stepSingle(), Message: d.Text()}
	case StepHashedMessage:
		return &HashedMessageStep{SingleStep: d.stepSingle(), Hash: d.decimalInt32(), Time: d.decimalInt32(), Value: d.decimalInt32()}
	case StepParameterizedMessage:
		p := &ParameterizedMessageStep{SingleStep: d.stepSingle(), Hash: d.decimalInt32(), Elapsed: d.decimalInt32()}
		level := d.Decimal()
		if level < math.MinInt8 || level > math.MaxInt8 {
			d.fail(ErrInvalid, "Parameterized message level does not fit int8")
		}
		p.Level = int8(level)
		p.ParamString = d.Text()
		return p
	case StepSocket:
		return &SocketStep{SingleStep: d.stepSingle(), IPAddress: d.Blob(), Port: d.decimalInt32(), Elapsed: d.decimalInt32(), Error: d.decimalInt32()}
	case StepAPICall:
		p := d.stepAPICall()
		return &p
	case StepAPICall2:
		return &APICallStep2{APICallStep: d.stepAPICall(), Async: d.Byte()}
	case StepDispatch:
		p := &DispatchStep{CallStepFields: d.stepCallFields()}
		p.Opt, p.Address = d.stepAddress()
		return p
	case StepThreadSubmit:
		return &ThreadSubmitStep{CallStepFields: d.stepCallFields()}
	case StepThreadCallPossible:
		return &ThreadCallPossibleStep{SingleStep: d.stepSingle(), TxID: d.Decimal(), Hash: d.decimalInt32(), Elapsed: d.decimalInt32(), Threaded: d.Byte()}
	case StepDump:
		p := &DumpStep{SingleStep: d.stepSingle()}
		n := d.count(int64(d.Int16()), 4)
		if d.err != nil {
			return nil
		}
		d.values += n
		p.Stacks = make([]int32, n)
		for i := range p.Stacks {
			p.Stacks[i] = d.Int32()
		}
		p.ThreadID = d.Int64()
		p.ThreadName = d.Text()
		p.ThreadState = d.Text()
		p.LockOwnerID = d.Int64()
		p.LockName = d.Text()
		p.LockOwnerName = d.Text()
		return p
	case StepMethodSum:
		p := d.stepSummary()
		return &p
	case StepSQLSum:
		return &SQLSum{MethodSum: d.stepSummary(), Error: d.decimalInt32(), Param: d.Text(), ParamError: d.Text()}
	case StepAPICallSum:
		p := &APICallSum{MethodSum: d.stepSummary(), Error: d.decimalInt32(), Opt: d.Byte()}
		// Java reader accepts arbitrary opt, but the corresponding writer throws
		// for nonzero opt. Reject a record that cannot be safely reencoded.
		if d.err == nil && p.Opt != 0 {
			d.fail(ErrInvalid, "API sum writer permits only opt=0")
		}
		return p
	case StepSocketSum:
		return &SocketSum{IPAddress: d.Blob(), Port: d.decimalInt32(), Count: d.decimalInt32(), Elapsed: d.Decimal(), Error: d.decimalInt32()}
	case StepMessageSum:
		return &MessageSum{Message: d.Text(), Count: d.decimalInt32()}
	case StepControlType:
		return &StepControl{Message: d.Text(), Code: d.decimalInt32()}
	case StepSpan:
		return &SpanStep{CommonSpanStep: d.stepSpan()}
	case StepSpanCall:
		p := &SpanCallStep{CommonSpanStep: d.stepSpan(), TxID: d.Decimal()}
		p.Opt, p.Address = d.stepAddress()
		p.Async = d.Byte()
		return p
	default:
		d.fail(ErrUnsupported, fmt.Sprintf("Step type %d", kind))
		return nil
	}
}
