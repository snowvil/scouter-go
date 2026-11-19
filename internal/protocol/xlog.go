// Scouter wire format: Copyright 2015 the original author or authors,
// https://github.com/scouter-project/scouter, Apache License 2.0.
// Independent Go implementation; see CONTRACT.md for pinned source provenance.
package protocol

// XLogCurrentTail is the latest append group in the pinned Java XLogPack.
const XLogCurrentTail byte = 12

// XLogPack preserves legacy append-group boundaries. TailVersion 0 emits the
// original required fields only; set XLogCurrentTail for newly produced current
// packets. Extra owns unknown bytes after all 12 known groups, matching Java's
// forward-tolerant Blob reader while preventing loss during a migration rewrite.
type XLogPack struct {
	EndTime                        int64
	ObjHash                        int32
	Service                        int32
	TxID                           int64
	Caller                         int64
	GxID                           int64
	Elapsed                        int32
	Error                          int32
	CPU                            int32
	SQLCount                       int32
	SQLTime                        int32
	IPAddress                      []byte
	KBytes                         int32
	Status                         int32
	UserID                         int64
	UserAgent                      int32
	Referer                        int32
	Group                          int32
	APICallCount                   int32
	APICallTime                    int32
	CountryCode                    string
	City                           int32
	XType                          byte
	Login                          int32
	Desc                           int32
	WebHash                        int32
	WebTime                        int32
	HasDump                        byte
	ThreadNameHash                 int32
	Text1                          string
	Text2                          string
	QueuingHostHash                int32
	QueuingTime                    int32
	Queuing2ndHostHash             int32
	Queuing2ndTime                 int32
	Text3                          string
	Text4                          string
	Text5                          string
	ProfileCount                   int32
	B3Mode                         bool
	ProfileSize                    int32
	DiscardType                    byte
	IgnoreGlobalConsequentSampling bool
	TailVersion                    byte
	Extra                          []byte
}

func (*XLogPack) Type() byte { return PackXLog }

func (d *Decoder) xlog() *XLogPack {
	body := d.blobView()
	if d.err != nil {
		return nil
	}
	// The body is decoded in place; inner is d re-scoped to the Blob so that
	// Remaining() reports body bytes for the append-group boundaries.
	outer := d.enterBody(body)
	defer d.leaveBody(outer)
	inner := d
	p := &XLogPack{}
	p.EndTime = inner.Decimal()
	p.ObjHash = inner.decimalInt32()
	p.Service = inner.decimalInt32()
	p.TxID = inner.Int64()
	p.Caller = inner.Int64()
	p.GxID = inner.Int64()
	p.Elapsed = inner.decimalInt32()
	p.Error = inner.decimalInt32()
	p.CPU = inner.decimalInt32()
	p.SQLCount = inner.decimalInt32()
	p.SQLTime = inner.decimalInt32()
	p.IPAddress = inner.Blob()
	p.KBytes = inner.decimalInt32()
	p.Status = inner.decimalInt32()
	p.UserID = inner.Decimal()
	p.UserAgent = inner.decimalInt32()
	p.Referer = inner.decimalInt32()
	p.Group = inner.decimalInt32()
	p.APICallCount = inner.decimalInt32()
	p.APICallTime = inner.decimalInt32()
	if inner.err == nil && inner.Remaining() > 0 {
		p.CountryCode = inner.Text()
		p.City = inner.decimalInt32()
		p.TailVersion = 1
	}
	if inner.err == nil && inner.Remaining() > 0 {
		p.XType = inner.Byte()
		p.TailVersion = 2
	}
	if inner.err == nil && inner.Remaining() > 0 {
		p.Login = inner.decimalInt32()
		p.Desc = inner.decimalInt32()
		p.TailVersion = 3
	}
	if inner.err == nil && inner.Remaining() > 0 {
		p.WebHash = inner.decimalInt32()
		p.WebTime = inner.decimalInt32()
		p.TailVersion = 4
	}
	if inner.err == nil && inner.Remaining() > 0 {
		p.HasDump = inner.Byte()
		p.TailVersion = 5
	}
	if inner.err == nil && inner.Remaining() > 0 {
		p.ThreadNameHash = inner.decimalInt32()
		p.TailVersion = 6
	}
	if inner.err == nil && inner.Remaining() > 0 {
		p.Text1 = inner.Text()
		p.Text2 = inner.Text()
		p.TailVersion = 7
	}
	if inner.err == nil && inner.Remaining() > 0 {
		p.QueuingHostHash = inner.decimalInt32()
		p.QueuingTime = inner.decimalInt32()
		p.Queuing2ndHostHash = inner.decimalInt32()
		p.Queuing2ndTime = inner.decimalInt32()
		p.TailVersion = 8
	}
	if inner.err == nil && inner.Remaining() > 0 {
		p.Text3 = inner.Text()
		p.Text4 = inner.Text()
		p.Text5 = inner.Text()
		p.TailVersion = 9
	}
	if inner.err == nil && inner.Remaining() > 0 {
		p.ProfileCount = inner.decimalInt32()
		p.TailVersion = 10
	}
	if inner.err == nil && inner.Remaining() > 0 {
		p.B3Mode = inner.Bool()
		p.TailVersion = 11
	}
	if inner.err == nil && inner.Remaining() > 0 {
		p.ProfileSize = inner.decimalInt32()
		p.DiscardType = inner.Byte()
		p.IgnoreGlobalConsequentSampling = inner.Bool()
		p.TailVersion = 12
	}
	if inner.err != nil {
		return nil
	}
	if inner.Remaining() > 0 {
		p.Extra = inner.owned(inner.take(inner.Remaining()))
	}
	return p
}

func (e *Encoder) xlog(p *XLogPack) {
	if p.TailVersion > XLogCurrentTail || (len(p.Extra) > 0 && p.TailVersion != XLogCurrentTail) {
		e.fail(ErrInvalid, "XLog tail version")
		return
	}
	// The body is framed in place: beginBlob reserves the length prefix and
	// endBlob widens it after the body is appended, avoiding a nested buffer.
	start := e.beginBlob()
	body := e
	body.Decimal(p.EndTime)
	body.Decimal(int64(p.ObjHash))
	body.Decimal(int64(p.Service))
	body.Int64(p.TxID)
	body.Int64(p.Caller)
	body.Int64(p.GxID)
	body.Decimal(int64(p.Elapsed))
	body.Decimal(int64(p.Error))
	body.Decimal(int64(p.CPU))
	body.Decimal(int64(p.SQLCount))
	body.Decimal(int64(p.SQLTime))
	body.Blob(p.IPAddress)
	body.Decimal(int64(p.KBytes))
	body.Decimal(int64(p.Status))
	body.Decimal(p.UserID)
	body.Decimal(int64(p.UserAgent))
	body.Decimal(int64(p.Referer))
	body.Decimal(int64(p.Group))
	body.Decimal(int64(p.APICallCount))
	body.Decimal(int64(p.APICallTime))
	if p.TailVersion >= 1 {
		body.Text(p.CountryCode)
		body.Decimal(int64(p.City))
	}
	if p.TailVersion >= 2 {
		body.Byte(p.XType)
	}
	if p.TailVersion >= 3 {
		body.Decimal(int64(p.Login))
		body.Decimal(int64(p.Desc))
	}
	if p.TailVersion >= 4 {
		body.Decimal(int64(p.WebHash))
		body.Decimal(int64(p.WebTime))
	}
	if p.TailVersion >= 5 {
		body.Byte(p.HasDump)
	}
	if p.TailVersion >= 6 {
		body.Decimal(int64(p.ThreadNameHash))
	}
	if p.TailVersion >= 7 {
		body.Text(p.Text1)
		body.Text(p.Text2)
	}
	if p.TailVersion >= 8 {
		body.Decimal(int64(p.QueuingHostHash))
		body.Decimal(int64(p.QueuingTime))
		body.Decimal(int64(p.Queuing2ndHostHash))
		body.Decimal(int64(p.Queuing2ndTime))
	}
	if p.TailVersion >= 9 {
		body.Text(p.Text3)
		body.Text(p.Text4)
		body.Text(p.Text5)
	}
	if p.TailVersion >= 10 {
		body.Decimal(int64(p.ProfileCount))
	}
	if p.TailVersion >= 11 {
		body.Bool(p.B3Mode)
	}
	if p.TailVersion >= 12 {
		body.Decimal(int64(p.ProfileSize))
		body.Byte(p.DiscardType)
		body.Bool(p.IgnoreGlobalConsequentSampling)
	}
	body.raw(p.Extra)
	e.endBlob(start)
}
