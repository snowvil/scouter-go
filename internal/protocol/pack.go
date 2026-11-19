// Scouter wire format: Copyright 2015 the original author or authors,
// https://github.com/scouter-project/scouter, Apache License 2.0.
// This Go implementation is independently derived; provenance is in CONTRACT.md.
package protocol

import "fmt"

const (
	PackMap                byte = 10
	PackXLog               byte = 21
	PackDroppedXLog        byte = 22
	PackProfile            byte = 26
	PackProfile2           byte = 27
	PackSpan               byte = 31
	PackSpanContainer      byte = 32
	PackText               byte = 50
	PackPerfCounter        byte = 60
	PackStatus             byte = 61
	PackStack              byte = 62
	PackSummary            byte = 63
	PackBatch              byte = 64
	PackInteractionCounter byte = 65
	PackAlert              byte = 70
	PackObject             byte = 80
)

// Pack is a supported wire packet. Unknown/extension types fail explicitly;
// there is no universal Pack length with which to safely skip an unknown body.
type Pack interface{ Type() byte }
type MapPack struct{ Entries []Entry }

func (*MapPack) Type() byte { return PackMap }

type ObjectPack struct {
	ObjType                   string
	ObjHash                   int32
	ObjName, Address, Version string
	Alive                     bool
	Wakeup                    int64
	Tags                      Value
}

func (*ObjectPack) Type() byte { return PackObject }

type PerfCounterPack struct {
	Time     int64
	ObjName  string
	TimeType byte
	Data     Value
}

func (*PerfCounterPack) Type() byte { return PackPerfCounter }

type TextPack struct {
	XType string
	Hash  int32
	Text  string
}

func (*TextPack) Type() byte { return PackText }

// Profile is deliberately opaque owned bytes. This unit does not implement Step
// decoding, profile aggregation, or unsent Java elapsed/internal processing fields.
type ProfilePack struct {
	Time                           int64
	ObjHash, Service               int32
	TxID                           int64
	Profile                        []byte
	Version2                       bool
	GxID                           int64
	XType, DiscardType             byte
	IgnoreGlobalConsequentSampling bool
}

func (p *ProfilePack) Type() byte {
	if p != nil && p.Version2 {
		return PackProfile2
	}
	return PackProfile
}

func (d *Decoder) Pack() Pack {
	if d.err != nil {
		return nil
	}
	switch kind := d.Byte(); kind {
	case PackMap:
		return &MapPack{Entries: d.entries()}
	case PackObject:
		p := &ObjectPack{ObjType: d.Text(), ObjHash: d.decimalInt32(), ObjName: d.Text(), Address: d.Text(), Version: d.Text(), Alive: d.Bool(), Wakeup: d.Decimal(), Tags: d.Value()}
		if d.err == nil && p.Tags.Type != ValueMap {
			d.fail(ErrInvalid, "Object tags must be MapValue")
		}
		return p
	case PackPerfCounter:
		p := &PerfCounterPack{Time: d.Int64(), ObjName: d.Text(), TimeType: d.Byte(), Data: d.Value()}
		if d.err == nil && p.Data.Type != ValueMap {
			d.fail(ErrInvalid, "counter data must be MapValue")
		}
		return p
	case PackText:
		return &TextPack{XType: d.Text(), Hash: d.Int32(), Text: d.Text()}
	case PackXLog:
		return d.xlog()
	case PackDroppedXLog, PackSpan, PackSpanContainer, PackStatus, PackStack, PackSummary, PackBatch, PackInteractionCounter, PackAlert:
		return d.extraPack(kind)
	case PackProfile, PackProfile2:
		p := &ProfilePack{Time: d.Decimal(), ObjHash: d.decimalInt32(), Service: d.decimalInt32(), TxID: d.Int64(), Profile: d.Blob(), Version2: kind == PackProfile2}
		if p.Version2 {
			p.GxID = d.Int64()
			p.XType = d.Byte()
			p.DiscardType = d.Byte()
			p.IgnoreGlobalConsequentSampling = d.Bool()
		}
		return p
	default:
		d.fail(ErrUnsupported, fmt.Sprintf("Pack type %d", kind))
		return nil
	}
}
func (e *Encoder) Pack(p Pack) {
	if e.err != nil {
		return
	}
	if p == nil {
		e.fail(ErrInvalid, "nil Pack")
		return
	}
	switch v := p.(type) {
	case *MapPack:
		if v == nil {
			e.fail(ErrInvalid, "nil MapPack")
			return
		}
		e.Byte(PackMap)
		e.entries(v.Entries)
	case *ObjectPack:
		if v == nil {
			e.fail(ErrInvalid, "nil ObjectPack")
			return
		}
		if v.Tags.Type != ValueMap {
			e.fail(ErrInvalid, "Object tags must be MapValue")
			return
		}
		e.Byte(PackObject)
		e.Text(v.ObjType)
		e.Decimal(int64(v.ObjHash))
		e.Text(v.ObjName)
		e.Text(v.Address)
		e.Text(v.Version)
		e.Bool(v.Alive)
		e.Decimal(v.Wakeup)
		e.Value(v.Tags)
	case *PerfCounterPack:
		if v == nil {
			e.fail(ErrInvalid, "nil PerfCounterPack")
			return
		}
		if v.Data.Type != ValueMap {
			e.fail(ErrInvalid, "counter data must be MapValue")
			return
		}
		e.Byte(PackPerfCounter)
		e.Int64(v.Time)
		e.Text(v.ObjName)
		e.Byte(v.TimeType)
		e.Value(v.Data)
	case *TextPack:
		if v == nil {
			e.fail(ErrInvalid, "nil TextPack")
			return
		}
		e.Byte(PackText)
		e.Text(v.XType)
		e.Int32(v.Hash)
		e.Text(v.Text)
	case *XLogPack:
		if v == nil {
			e.fail(ErrInvalid, "nil XLogPack")
			return
		}
		e.Byte(PackXLog)
		e.xlog(v)
	case *ProfilePack:
		if v == nil {
			e.fail(ErrInvalid, "nil ProfilePack")
			return
		}
		e.Byte(v.Type())
		e.Decimal(v.Time)
		e.Decimal(int64(v.ObjHash))
		e.Decimal(int64(v.Service))
		e.Int64(v.TxID)
		e.Blob(v.Profile)
		if v.Version2 {
			e.Int64(v.GxID)
			e.Byte(v.XType)
			e.Byte(v.DiscardType)
			e.Bool(v.IgnoreGlobalConsequentSampling)
		}
	case *DroppedXLogPack, *SpanPack, *SpanContainerPack, *StatusPack, *StackPack, *SummaryPack, *BatchPack, *InteractionPerfCounterPack, *AlertPack:
		e.extraPack(p)
	default:
		e.fail(ErrUnsupported, fmt.Sprintf("Pack implementation %T", p))
	}
}
