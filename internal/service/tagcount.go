package service

import (
	"context"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// Pinned StringSet enumeration; getTagNames is not registration/alphabetic order.
func tagNames(group string) []string {
	switch group {
	case "service":
		return []string{"apitime-sum", "service-kbyte-sum", "@total", "city", "sqlcount-sum", "service-time-sum", "service", "nation", "service-error-sum", "sqltime-sum", "referer", "error", "login", "desc", "apicount-sum", "group", "object", "user-agent"}
	case "alert":
		return []string{"@total", "title", "level", "object"}
	}
	return nil
}
func (s *Service) tagQuery(ctx context.Context, command string, p *protocol.MapPack, stream *transport.Stream) error {
	backend, ok := s.backend.(TagBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	group, err := getText(p, "tagGroup")
	if err != nil {
		return err
	}
	switch command {
	case "TAGCNT_DIV_NAMES", "TAGCNT_TAG_NAMES":
		names := tagNames(group)
		if command == "TAGCNT_DIV_NAMES" {
			names = []string{"service", "alert"}
		}
		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := stream.ResponseValue(stringValue(name)); err != nil {
				return err
			}
		}
		return nil
	}
	objType, err := getText(p, "objType")
	if err != nil {
		return err
	}
	date, err := s.date(p, command != "TAGCNT_TAG_ACTUAL_DATA")
	if err != nil {
		return err
	}
	switch command {
	case "TAGCNT_TAG_VALUES":
		names, err := getList(p, "tagName")
		if err != nil {
			return err
		}
		for _, name := range names {
			if name.Type != protocol.ValueText {
				return badParam("tagName")
			}
			total, err := backend.TagValues(ctx, date, objType, group, name.Text)
			if err != nil {
				return err
			}
			e := protocol.NewEncoder(protocol.Limits{})
			e.Text(name.Text)
			e.Int32(int32(total.HowManyValues))
			e.Float32(total.TotalCount)
			e.Int32(int32(len(total.Values)))
			for _, v := range total.Values {
				e.Value(v.Value)
				e.Float32(float32(v.Count))
			}
			b, err := e.Bytes()
			if err != nil {
				return err
			}
			if err = stream.ResponseRaw(b); err != nil {
				return err
			}
		}
		return nil
	case "TAGCNT_TAG_VALUE_DATA":
		name, err := getText(p, "tagName")
		if err != nil {
			return err
		}
		values, err := backend.TagValueSeries(ctx, date, objType, group, name, lookup(p, "tagValue"))
		if err != nil {
			return err
		}
		if values == nil {
			return nil
		}
		if len(values) > 32767 {
			return transport.ErrLimit
		}
		e := protocol.NewEncoder(protocol.Limits{})
		e.Int16(int16(len(values)))
		for _, n := range values {
			e.Float32(n)
		}
		b, err := e.Bytes()
		if err != nil {
			return err
		}
		return stream.ResponseRaw(b)
	case "TAGCNT_TAG_ACTUAL_DATA":
		from, err := getNumber(p, "stime")
		if err != nil {
			return err
		}
		to, err := getNumber(p, "etime")
		if err != nil {
			return err
		}
		max, err := getInt(p, "max")
		if err != nil {
			return err
		}
		reverse, err := getBool(p, "reverse")
		if err != nil {
			return err
		}
		txid, err := getNumber(p, "txid")
		if err != nil {
			return err
		}
		filter := lookup(p, "filter")
		if filter.Type != protocol.ValueNull && filter.Type != protocol.ValueMap {
			return badParam("filter")
		}
		records, err := backend.TagActual(ctx, TagActualQuery{Date: date, ObjType: objType, Group: group, From: from, To: to, Max: int(max), Reverse: reverse, TxID: txid, Filter: filter})
		if err != nil {
			return err
		}
		for _, raw := range records {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := stream.ResponseRaw(raw); err != nil {
				return err
			}
		}
		return nil
	}
	return transport.ErrUnsupported
}
