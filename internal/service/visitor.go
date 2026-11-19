package service

import (
	"context"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/transport"
)

func (s *Service) visitorQuery(ctx context.Context, command string, p *protocol.MapPack, stream *transport.Stream) error {
	backend, ok := s.backend.(VisitorBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	var hashes []int32
	var objType string
	var err error
	switch command {
	case "VISITOR_REALTIME", "VISITOR_LOADDATE":
		var hash int32
		hash, err = getInt(p, "objHash")
		hashes = []int32{hash}
	case "VISITOR_REALTIME_TOTAL", "VISITOR_LOADDATE_TOTAL":
		objType, err = getText(p, "objType")
	default:
		hashes, err = getHashes(p, "objHash")
	}
	if err != nil {
		return err
	}
	switch command {
	case "VISITOR_REALTIME", "VISITOR_REALTIME_TOTAL", "VISITOR_REALTIME_GROUP":
		n, err := backend.VisitorRealtime(ctx, hashes, objType)
		if err != nil {
			return err
		}
		return stream.ResponseValue(decimal(n))
	case "VISITOR_LOADDATE", "VISITOR_LOADDATE_TOTAL":
		date, err := s.date(p, false)
		if err != nil {
			return err
		}
		n, err := backend.VisitorDaily(ctx, date, hashes, objType)
		if err != nil {
			return err
		}
		return stream.ResponseValue(decimal(n))
	case "VISITOR_LOADDATE_GROUP":
		start, err := getText(p, "startDate")
		if err != nil {
			return err
		}
		end, err := getText(p, "endDate")
		if err != nil {
			return err
		}
		from, err := scoutertime.ParseDate(start, s.config.Location)
		if err != nil {
			return badParam("startDate")
		}
		to, err := scoutertime.ParseDate(end, s.config.Location)
		if err != nil {
			return badParam("endDate")
		}
		if to-from > 366*scoutertime.DayMillis {
			return transport.ErrLimit
		}
		for tm := from; tm <= to; tm += scoutertime.DayMillis {
			if err := ctx.Err(); err != nil {
				return err
			}
			date := scoutertime.Date(tm, s.config.Location)
			n, err := backend.VisitorDaily(ctx, date, hashes, "")
			if err != nil {
				return err
			}
			if err = stream.ResponsePack(newMap(entry("date", stringValue(date)), entry("value", decimal(n)))); err != nil {
				return err
			}
		}
		return nil
	case "VISITOR_LOADHOUR_GROUP":
		from, err := getNumber(p, "stime")
		if err != nil {
			return err
		}
		to, err := getNumber(p, "etime")
		if err != nil {
			return err
		}
		if from <= 0 || to <= 0 {
			return badParam("stime/etime")
		}
		if to > from && to-from > 366*scoutertime.DayMillis {
			return transport.ErrLimit
		}
		date := scoutertime.Date(from, s.config.Location)
		times, values := []protocol.Value{}, []protocol.Value{}
		flush := func() error {
			return stream.ResponsePack(newMap(entry("time", list(times)), entry("value", list(values))))
		}
		for tm := from; tm <= to; tm += 3600000 {
			if err := ctx.Err(); err != nil {
				return err
			}
			dt := scoutertime.Date(tm, s.config.Location)
			if dt != date {
				if err := flush(); err != nil {
					return err
				}
				date = dt
				times = []protocol.Value{}
				values = []protocol.Value{}
			}
			n, err := backend.VisitorHourly(ctx, date, tm, hashes)
			if err != nil {
				return err
			}
			times = append(times, decimal(tm))
			values = append(values, decimal(n))
		}
		return flush()
	}
	return transport.ErrUnsupported
}
