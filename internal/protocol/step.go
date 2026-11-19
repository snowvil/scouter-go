// Scouter wire format: Copyright 2015 the original author or authors,
// https://github.com/scouter-project/scouter, Apache License 2.0.
package protocol

// DecodeStep consumes exactly one Step and rejects trailing bytes. ProfilePack
// remains opaque: its consumer explicitly calls DecodeSteps on Profile bytes.
func DecodeStep(data []byte, limits Limits) (Step, error) {
	var d Decoder
	d.init(data, limits)
	step := d.Step()
	if d.err == nil && d.Remaining() != 0 {
		d.fail(ErrTrailing, "Step frame")
	}
	if d.err != nil {
		return nil, d.err
	}
	return step, nil
}
func EncodeStep(step Step, limits Limits) ([]byte, error) {
	var e Encoder
	e.init(limits, stepSizeHint)
	e.Step(step)
	return e.Bytes()
}

// stepSizeHint approximates one encoded Step for buffer/slice pre-sizing; it is
// only a capacity hint and never a validation bound.
const stepSizeHint = 24

// DecodeSteps reads the Java EOF-terminated sequence with no count prefix. Input
// order is preserved; no sorting, ThreadCallPossible filtering, or partial success
// is performed. Result byte slices own their data and survive input pool reuse.
func DecodeSteps(data []byte, limits Limits) ([]Step, error) {
	var d Decoder
	d.init(data, limits)
	if d.err != nil {
		return nil, d.err
	}
	// Capacity is bounded by the already validated input length and the
	// collection limit; it is a hint, not an allocation of MaxCollectionEntries.
	steps := make([]Step, 0, min(len(data)/stepSizeHint+1, d.limits.MaxCollectionEntries))
	for d.err == nil && d.Remaining() > 0 {
		if len(steps) >= d.limits.MaxCollectionEntries {
			d.fail(ErrLimit, "Step sequence count")
			break
		}
		step := d.Step()
		if d.err == nil {
			steps = append(steps, step)
		}
	}
	if d.err != nil {
		return nil, d.err
	}
	return steps, nil
}

// EncodeSteps follows the Java List<Step> writer (and the raw wire representation).
// A producer's Java Step[] filtering flag is not serialized and is not inferred.
func EncodeSteps(steps []Step, limits Limits) ([]byte, error) {
	var e Encoder
	e.init(limits, encoderSizeHint+stepSizeHint*len(steps))
	e.count(len(steps))
	if e.err != nil {
		return nil, e.err
	}
	for _, step := range steps {
		e.Step(step)
		if e.err != nil {
			break
		}
	}
	return e.Bytes()
}
