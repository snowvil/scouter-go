package protocol

import "io"

// ReadPack consumes exactly one Pack without reading ahead into the next object.
// MaxBytes applies to this Pack. The caller owns buffering, deadlines and closing
// the stream after an error: unknown or malformed Pack bodies cannot be skipped.
// Results own their byte slices, as with DecodePack.
func ReadPack(r io.Reader, limits Limits) (Pack, error) {
	var d Decoder
	d.initStream(r, limits)
	p := d.Pack()
	if d.err != nil {
		return nil, d.err
	}
	return p, nil
}

// ReadValue consumes exactly one Value, subject to the same contract as ReadPack.
func ReadValue(r io.Reader, limits Limits) (Value, error) {
	var d Decoder
	d.initStream(r, limits)
	v := d.Value()
	if d.err != nil {
		return Value{}, d.err
	}
	return v, nil
}

func (d *Decoder) initStream(r io.Reader, limits Limits) {
	d.init(nil, limits)
	if r == nil {
		d.fail(ErrInvalid, "nil stream reader")
	}
	d.reader = r
	d.scratch = make([]byte, 8)
}
