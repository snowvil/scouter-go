// Scouter wire format: Copyright 2015 the original author or authors,
// https://github.com/scouter-project/scouter, Apache License 2.0.
// Independent Go implementation from the pinned source; see compatibility/packs.md.
package protocol

type DroppedXLogPack struct{ GxID, TxID int64 }

func (*DroppedXLogPack) Type() byte { return PackDroppedXLog }

type SpanPack struct {
	GxID, TxID, Caller, Timestamp                int64
	Elapsed                                      int32
	SpanType                                     byte
	Name, ObjHash, Error                         int32
	LocalEndpointServiceName                     int32
	LocalEndpointIP                              []byte
	LocalEndpointPort                            int16
	RemoteEndpointServiceName                    int32
	RemoteEndpointIP                             []byte
	RemoteEndpointPort                           int16
	Debug, Shared                                bool
	AnnotationTimestamps, AnnotationValues, Tags Value
}

func (*SpanPack) Type() byte { return PackSpan }

// Spans is an owned opaque Blob containing concatenated SpanPack wire packets.
// The Pack codec preserves declared SpanCount; nested ingest validation is separate.
type SpanContainerPack struct {
	GxID      int64
	SpanCount int32
	Timestamp int64
	Spans     []byte
}

func (*SpanContainerPack) Type() byte { return PackSpanContainer }

type StatusPack struct {
	Time    int64
	ObjType string
	ObjHash int32
	Key     string
	Data    Value
}

func (*StatusPack) Type() byte { return PackStatus }

// Data is owned opaque compressed stack content; decoding does not decompress it.
type StackPack struct {
	Time    int64
	ObjHash int32
	Data    []byte
}

func (*StackPack) Type() byte { return PackStack }

type SummaryPack struct {
	Time    int64
	ObjHash int32
	ObjType string
	SType   byte
	Table   Value
}

func (*SummaryPack) Type() byte { return PackSummary }

type InteractionPerfCounterPack struct {
	Time                                        int64
	ObjName, InteractionType                    string
	FromHash, ToHash, Period, Count, ErrorCount int32
	TotalElapsed                                int64
	CustomData                                  Value
}

func (*InteractionPerfCounterPack) Type() byte { return PackInteractionCounter }

type AlertPack struct {
	Time           int64
	Level          byte
	ObjType        string
	ObjHash        int32
	Title, Message string
	Tags           Value
}

func (*AlertPack) Type() byte { return PackAlert }

type BatchSQLStat struct {
	HashValue, Runs                                                int32
	StartTime, EndTime, TotalTime, MinTime, MaxTime, ProcessedRows int64
	Rowed                                                          bool
}

// BatchSQLText retains Java HashMap's transmitted order rather than assuming
// insertion order. These wire entries are not independently SQLTotalCount-free.
type BatchSQLText struct {
	Hash int32
	Text string
}
type BatchPack struct {
	StartTime                                int64
	ObjHash                                  int32
	BatchJobID, Args                         string
	PID                                      int32
	ElapsedTime                              int64
	ThreadCount                              int32
	CPUTime, GCTime, GCCount                 int64
	SQLTotalCount                            int32
	SQLTotalTime, SQLTotalRows, SQLTotalRuns int64
	IsStack                                  bool
	ObjName, ObjType                         string
	Position                                 int64
	SQLStats                                 []BatchSQLStat
	UniqueSQLs                               []BatchSQLText
	Extra                                    []byte
}

func (*BatchPack) Type() byte { return PackBatch }

func (d *Decoder) requireValue(v Value, kind byte, reason string) {
	if d.err == nil && v.Type != kind {
		d.fail(ErrInvalid, reason)
	}
}
func (e *Encoder) requireValue(v Value, kind byte, reason string) bool {
	if v.Type != kind {
		e.fail(ErrInvalid, reason)
	}
	return e.err == nil
}

func (d *Decoder) extraPack(kind byte) Pack {
	switch kind {
	case PackDroppedXLog:
		return &DroppedXLogPack{GxID: d.Int64(), TxID: d.Int64()}
	case PackSpan:
		p := &SpanPack{GxID: d.Int64(), TxID: d.Int64(), Caller: d.Int64(), Timestamp: d.Int64(), Elapsed: d.decimalInt32(), SpanType: d.Byte(), Name: d.decimalInt32(), ObjHash: d.decimalInt32(), Error: d.decimalInt32()}
		p.LocalEndpointServiceName = d.decimalInt32()
		p.LocalEndpointIP = d.Blob()
		p.LocalEndpointPort = d.Int16()
		p.RemoteEndpointServiceName = d.decimalInt32()
		p.RemoteEndpointIP = d.Blob()
		p.RemoteEndpointPort = d.Int16()
		p.Debug = d.Bool()
		p.Shared = d.Bool()
		p.AnnotationTimestamps = d.Value()
		p.AnnotationValues = d.Value()
		p.Tags = d.Value()
		d.requireValue(p.AnnotationTimestamps, ValueList, "Span annotation timestamps must be ListValue")
		d.requireValue(p.AnnotationValues, ValueList, "Span annotation values must be ListValue")
		d.requireValue(p.Tags, ValueMap, "Span tags must be MapValue")
		return p
	case PackSpanContainer:
		return &SpanContainerPack{GxID: d.Int64(), SpanCount: d.decimalInt32(), Timestamp: d.Int64(), Spans: d.Blob()}
	case PackStatus:
		p := &StatusPack{Time: d.Decimal(), ObjType: d.Text(), ObjHash: d.decimalInt32(), Key: d.Text(), Data: d.Value()}
		d.requireValue(p.Data, ValueMap, "Status data must be MapValue")
		return p
	case PackStack:
		return &StackPack{Time: d.Decimal(), ObjHash: d.decimalInt32(), Data: d.Blob()}
	case PackSummary:
		p := &SummaryPack{Time: d.Decimal(), ObjHash: d.Int32(), ObjType: d.Text(), SType: d.Byte(), Table: d.Value()}
		d.requireValue(p.Table, ValueMap, "Summary table must be MapValue")
		return p
	case PackInteractionCounter:
		p := &InteractionPerfCounterPack{Time: d.Int64(), ObjName: d.Text(), InteractionType: d.Text(), FromHash: d.Int32(), ToHash: d.Int32(), Period: d.Int32(), Count: d.Int32(), ErrorCount: d.Int32(), TotalElapsed: d.Int64(), CustomData: d.Value()}
		d.requireValue(p.CustomData, ValueMap, "Interaction custom data must be MapValue")
		return p
	case PackAlert:
		p := &AlertPack{Time: d.Int64(), Level: d.Byte(), ObjType: d.Text(), ObjHash: d.Int32(), Title: d.Text(), Message: d.Text(), Tags: d.Value()}
		d.requireValue(p.Tags, ValueMap, "Alert tags must be MapValue")
		return p
	case PackBatch:
		return d.batch()
	}
	d.fail(ErrUnsupported, "Pack type")
	return nil
}

func (e *Encoder) extraPack(p Pack) {
	switch v := p.(type) {
	case *DroppedXLogPack:
		if v == nil {
			e.fail(ErrInvalid, "nil DroppedXLogPack")
			return
		}
		e.Byte(PackDroppedXLog)
		e.Int64(v.GxID)
		e.Int64(v.TxID)
	case *SpanPack:
		if v == nil {
			e.fail(ErrInvalid, "nil SpanPack")
			return
		}
		if !e.requireValue(v.AnnotationTimestamps, ValueList, "Span annotation timestamps must be ListValue") || !e.requireValue(v.AnnotationValues, ValueList, "Span annotation values must be ListValue") || !e.requireValue(v.Tags, ValueMap, "Span tags must be MapValue") {
			return
		}
		e.Byte(PackSpan)
		e.Int64(v.GxID)
		e.Int64(v.TxID)
		e.Int64(v.Caller)
		e.Int64(v.Timestamp)
		e.Decimal(int64(v.Elapsed))
		e.Byte(v.SpanType)
		e.Decimal(int64(v.Name))
		e.Decimal(int64(v.ObjHash))
		e.Decimal(int64(v.Error))
		e.Decimal(int64(v.LocalEndpointServiceName))
		e.Blob(v.LocalEndpointIP)
		e.Int16(v.LocalEndpointPort)
		e.Decimal(int64(v.RemoteEndpointServiceName))
		e.Blob(v.RemoteEndpointIP)
		e.Int16(v.RemoteEndpointPort)
		e.Bool(v.Debug)
		e.Bool(v.Shared)
		e.Value(v.AnnotationTimestamps)
		e.Value(v.AnnotationValues)
		e.Value(v.Tags)
	case *SpanContainerPack:
		if v == nil {
			e.fail(ErrInvalid, "nil SpanContainerPack")
			return
		}
		e.Byte(PackSpanContainer)
		e.Int64(v.GxID)
		e.Decimal(int64(v.SpanCount))
		e.Int64(v.Timestamp)
		e.Blob(v.Spans)
	case *StatusPack:
		if v == nil {
			e.fail(ErrInvalid, "nil StatusPack")
			return
		}
		if !e.requireValue(v.Data, ValueMap, "Status data must be MapValue") {
			return
		}
		e.Byte(PackStatus)
		e.Decimal(v.Time)
		e.Text(v.ObjType)
		e.Decimal(int64(v.ObjHash))
		e.Text(v.Key)
		e.Value(v.Data)
	case *StackPack:
		if v == nil {
			e.fail(ErrInvalid, "nil StackPack")
			return
		}
		e.Byte(PackStack)
		e.Decimal(v.Time)
		e.Decimal(int64(v.ObjHash))
		e.Blob(v.Data)
	case *SummaryPack:
		if v == nil {
			e.fail(ErrInvalid, "nil SummaryPack")
			return
		}
		if !e.requireValue(v.Table, ValueMap, "Summary table must be MapValue") {
			return
		}
		e.Byte(PackSummary)
		e.Decimal(v.Time)
		e.Int32(v.ObjHash)
		e.Text(v.ObjType)
		e.Byte(v.SType)
		e.Value(v.Table)
	case *InteractionPerfCounterPack:
		if v == nil {
			e.fail(ErrInvalid, "nil InteractionPerfCounterPack")
			return
		}
		if !e.requireValue(v.CustomData, ValueMap, "Interaction custom data must be MapValue") {
			return
		}
		e.Byte(PackInteractionCounter)
		e.Int64(v.Time)
		e.Text(v.ObjName)
		e.Text(v.InteractionType)
		e.Int32(v.FromHash)
		e.Int32(v.ToHash)
		e.Int32(v.Period)
		e.Int32(v.Count)
		e.Int32(v.ErrorCount)
		e.Int64(v.TotalElapsed)
		e.Value(v.CustomData)
	case *AlertPack:
		if v == nil {
			e.fail(ErrInvalid, "nil AlertPack")
			return
		}
		if !e.requireValue(v.Tags, ValueMap, "Alert tags must be MapValue") {
			return
		}
		e.Byte(PackAlert)
		e.Int64(v.Time)
		e.Byte(v.Level)
		e.Text(v.ObjType)
		e.Int32(v.ObjHash)
		e.Text(v.Title)
		e.Text(v.Message)
		e.Value(v.Tags)
	case *BatchPack:
		if v == nil {
			e.fail(ErrInvalid, "nil BatchPack")
			return
		}
		e.Byte(PackBatch)
		e.batch(v)
	}
}

func (d *Decoder) batch() *BatchPack {
	data := d.blobView()
	if d.err != nil {
		return nil
	}
	outer := d.enterBody(data)
	defer d.leaveBody(outer)
	b := d
	p := &BatchPack{StartTime: b.Int64(), ObjHash: b.Int32(), BatchJobID: b.Text(), Args: b.Text(), PID: b.Int32(), ElapsedTime: b.Int64(), ThreadCount: b.Int32(), CPUTime: b.Int64(), GCTime: b.Int64(), GCCount: b.Int64(), SQLTotalCount: b.Int32(), SQLTotalTime: b.Int64(), SQLTotalRows: b.Int64(), SQLTotalRuns: b.Int64(), IsStack: b.Bool(), ObjName: b.Text(), ObjType: b.Text(), Position: b.Int64()}
	// Each SQL stat needs 57 bytes and each dictionary entry at least 5 bytes.
	n := b.count(int64(p.SQLTotalCount), 62)
	if b.err == nil {
		b.values += n
		b.count(int64(n), 5) // account for both arrays before either allocation
	}
	if b.err == nil && n > 0 {
		b.values += n
		p.SQLStats = make([]BatchSQLStat, n)
		for i := range p.SQLStats {
			p.SQLStats[i] = BatchSQLStat{HashValue: b.Int32(), Runs: b.Int32(), StartTime: b.Int64(), EndTime: b.Int64(), TotalTime: b.Int64(), MinTime: b.Int64(), MaxTime: b.Int64(), ProcessedRows: b.Int64(), Rowed: b.Bool()}
		}
		p.UniqueSQLs = make([]BatchSQLText, n)
		for i := range p.UniqueSQLs {
			p.UniqueSQLs[i] = BatchSQLText{Hash: b.Int32(), Text: b.Text()}
		}
	}
	if b.err != nil {
		return nil
	}
	if b.Remaining() > 0 {
		p.Extra = b.owned(b.take(b.Remaining()))
	}
	return p
}
func (e *Encoder) batch(p *BatchPack) {
	n := len(p.SQLStats)
	if p.SQLTotalCount < 0 || int64(p.SQLTotalCount) != int64(n) || len(p.UniqueSQLs) != n {
		e.fail(ErrInvalid, "Batch SQL count mismatch")
		return
	}
	b := e
	b.count(n)
	if b.err == nil {
		b.values += n
		b.count(n)
	}
	if b.err != nil {
		return
	}
	b.values += n
	start := b.beginBlob()
	b.Int64(p.StartTime)
	b.Int32(p.ObjHash)
	b.Text(p.BatchJobID)
	b.Text(p.Args)
	b.Int32(p.PID)
	b.Int64(p.ElapsedTime)
	b.Int32(p.ThreadCount)
	b.Int64(p.CPUTime)
	b.Int64(p.GCTime)
	b.Int64(p.GCCount)
	b.Int32(p.SQLTotalCount)
	b.Int64(p.SQLTotalTime)
	b.Int64(p.SQLTotalRows)
	b.Int64(p.SQLTotalRuns)
	b.Bool(p.IsStack)
	b.Text(p.ObjName)
	b.Text(p.ObjType)
	b.Int64(p.Position)
	for _, s := range p.SQLStats {
		b.Int32(s.HashValue)
		b.Int32(s.Runs)
		b.Int64(s.StartTime)
		b.Int64(s.EndTime)
		b.Int64(s.TotalTime)
		b.Int64(s.MinTime)
		b.Int64(s.MaxTime)
		b.Int64(s.ProcessedRows)
		b.Bool(s.Rowed)
	}
	for _, s := range p.UniqueSQLs {
		b.Int32(s.Hash)
		b.Text(s.Text)
	}
	b.raw(p.Extra)
	e.endBlob(start)
}
