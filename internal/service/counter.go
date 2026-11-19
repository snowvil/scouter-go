package service

import (
	"context"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/transport"
)

func (s *Service) counterQuery(ctx context.Context, command string, p *protocol.MapPack, stream *transport.Stream) error {
	counter := ""
	var err error
	if command != "COUNTER_REAL_TIME_MULTI" {
		counter, err = getText(p, "counter")
		if err != nil {
			return err
		}
	}
	hash, err := getInt(p, "objHash")
	if err != nil && command != "COUNTER_PAST_TIME_ALL" {
		return err
	}
	switch command {
	case "COUNTER_REAL_TIME":
		tt, err := getInt(p, "timetype")
		if err != nil {
			return err
		}
		if tt < 0 || tt > 255 {
			return badParam("timetype")
		}
		value, found, err := s.backend.Counter(ctx, hash, counter, byte(tt))
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		return stream.ResponseValue(value)
	case "COUNTER_REAL_TIME_MULTI":
		counters, err := getList(p, "counter")
		if err != nil {
			return err
		}
		names, values := []protocol.Value{}, []protocol.Value{}
		for _, name := range counters {
			if name.Type != protocol.ValueText {
				return badParam("counter")
			}
			value, found, err := s.backend.Counter(ctx, hash, name.Text, 1)
			if err != nil {
				return err
			}
			if found {
				names = append(names, name)
				values = append(values, value)
			}
		}
		return stream.ResponsePack(newMap(entry("counter", list(names)), entry("value", list(values))))
	case "COUNTER_REAL_TIME_ALL":
		typ, err := getText(p, "objType")
		if err != nil {
			return err
		}
		if typ == "" {
			return nil
		}
		objects, err := s.backend.Objects(ctx, "")
		if err != nil {
			return err
		}
		hashes, values := []protocol.Value{}, []protocol.Value{}
		for _, object := range objects {
			if object.ObjType != typ || !object.Alive {
				continue
			}
			value, found, err := s.backend.Counter(ctx, object.ObjHash, counter, 1)
			if err != nil {
				return err
			}
			if found {
				hashes = append(hashes, decimal(int64(object.ObjHash)))
				values = append(values, value)
			}
		}
		return stream.ResponsePack(newMap(entry("objHash", list(hashes)), entry("value", list(values))))
	case "COUNTER_PAST_TIME", "COUNTER_PAST_TIME_ALL":
		from, err := getNumber(p, "stime")
		if err != nil {
			return err
		}
		to, err := getNumber(p, "etime")
		if err != nil {
			return err
		}
		if to < from {
			return badParam("stime/etime")
		}
		if command == "COUNTER_PAST_TIME" {
			points, err := s.backend.CounterHistory(ctx, hash, counter, from, to)
			if err != nil {
				return err
			}
			return stream.ResponsePack(counterSeries(points))
		}
		hashes, err := getHashes(p, "objHash")
		if err != nil {
			return err
		}
		if len(hashes) == 0 {
			typ, err := getText(p, "objType")
			if err != nil {
				return err
			}
			if typ == "" {
				return nil
			}
			objects, err := s.backend.Objects(ctx, scoutertime.Date(from, s.config.Location))
			if err != nil {
				return err
			}
			for _, object := range objects {
				if object.ObjType == typ {
					hashes = append(hashes, object.ObjHash)
				}
			}
		}
		return s.counterHistories(ctx, hashes, counter, from, to, func(hash int32, points []CounterPoint) error {
			series := counterSeries(points)
			series.Entries = append([]protocol.Entry{entry("objHash", decimal(int64(hash)))}, series.Entries...)
			return stream.ResponsePack(series)
		})
	case "COUNTER_TODAY", "COUNTER_PAST_DATE":
		date := scoutertime.Date(s.config.Clock().UnixMilli(), s.config.Location)
		tt := int32(3)
		if command == "COUNTER_TODAY" {
			tt, err = getInt(p, "timetype")
			if err != nil {
				return err
			}
		} else {
			date, err = s.date(p, false)
			if err != nil {
				return err
			}
			if date == "" {
				return badParam("date")
			}
		}
		delta := counterDelta(tt)
		if delta == 0 {
			return badParam("timetype")
		}
		values, err := s.backend.CounterDaily(ctx, date, hash, counter, byte(tt))
		if err != nil {
			return err
		}
		if values == nil && command == "COUNTER_PAST_DATE" {
			return nil
		}
		midnight, err := scoutertime.ParseDate(date, s.config.Location)
		if err != nil {
			return badParam("date")
		}
		times := make([]protocol.Value, len(values))
		for i := range values {
			times[i] = decimal(midnight + int64(i)*delta)
		}
		return stream.ResponsePack(newMap(entry("time", list(times)), entry("value", list(values))))
	}
	return transport.ErrUnsupported
}

// CounterHistoryMultiBackend reads several objects' history in one pass
// (Java COUNTER_PAST_TIME_ALL reads the range once, RealtimeCounterRD.readBulk);
// each is called per hash in order.
type CounterHistoryMultiBackend interface {
	CounterHistoryMulti(ctx context.Context, hashes []int32, name string, from, to int64, each func(int32, []CounterPoint) error) error
}

// counterHistories calls each with the history of every hash, in order, in
// one pass when the backend can.
func (s *Service) counterHistories(ctx context.Context, hashes []int32, name string, from, to int64, each func(int32, []CounterPoint) error) error {
	if b, ok := s.backend.(CounterHistoryMultiBackend); ok {
		return b.CounterHistoryMulti(ctx, hashes, name, from, to, each)
	}
	for _, hash := range hashes {
		points, err := s.backend.CounterHistory(ctx, hash, name, from, to)
		if err != nil {
			return err
		}
		if err := each(hash, points); err != nil {
			return err
		}
	}
	return nil
}
func counterSeries(points []CounterPoint) *protocol.MapPack {
	times, values := []protocol.Value{}, []protocol.Value{}
	for _, point := range points {
		times = append(times, decimal(point.Time))
		values = append(values, point.Value)
	}
	return newMap(entry("time", list(times)), entry("value", list(values)))
}
func counterDelta(tt int32) int64 {
	switch tt {
	case 1:
		return 2000
	case 2:
		return 60000
	case 3:
		return 300000
	case 5:
		return 3600000
	case 6:
		return 86400000
	}
	return 0
}
