package service

import (
	"context"
	"fmt"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/transport"
	"sort"
	"time"
)

func (s *Service) exportCounterQuery(ctx context.Context, cmd string, p *protocol.MapPack, stream *transport.Stream) error {
	catalog, ok := s.backend.(CounterCatalogBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	hash, err := getInt(p, "objHash")
	if err != nil {
		return err
	}
	date, err := s.date(p, false)
	if err != nil {
		return err
	}
	midnight, err := scoutertime.ParseDate(date, s.config.Location)
	if err != nil {
		return badParam("date")
	}
	object, err := s.backend.Object(ctx, date, hash)
	if err != nil {
		return err
	}
	if object == nil {
		return fmt.Errorf("%w: export object missing", transport.ErrMalformed)
	}
	meta, err := catalog.CounterType(ctx, object.ObjType)
	if err != nil {
		return err
	}
	names := append([]string{}, meta.Counters...)
	if cmd == "EXPORT_OBJECT_REGULAR_COUNTER" {
		stored, err := catalog.CounterNamesForDate(ctx, date, hash)
		if err != nil {
			return err
		}
		names = append(names, stored...)
	}
	unique := map[string]bool{}
	columns := []string{}
	for _, name := range names {
		if !unique[name] {
			unique[name] = true
			columns = append(columns, name)
		}
	}
	sort.Strings(columns)
	if len(columns) > extendedQueryCells {
		return transport.ErrLimit
	}
	header := []protocol.Value{stringValue("time")}
	for _, name := range columns {
		header = append(header, stringValue(name))
	}
	if cmd == "EXPORT_OBJECT_TIME_COUNTER" {
		rowsBackend, ok := s.backend.(CounterRowsBackend)
		if !ok {
			return transport.ErrUnsupported
		}
		from, to, err := extendedTimeRange(p)
		if err != nil {
			return err
		}
		if from == 0 {
			from, to = midnight, midnight+scoutertime.DayMillis
		}
		rows, err := rowsBackend.CounterRows(ctx, date, hash, from, to)
		if err != nil {
			return err
		}
		if len(rows) > extendedQueryCells || len(columns) > 0 && len(rows) > extendedQueryCells/len(columns) {
			return transport.ErrLimit
		}
		if err := stream.ResponsePack(newMap(entry("values", list(header)))); err != nil {
			return err
		}
		for _, row := range rows {
			if err := ctx.Err(); err != nil {
				return err
			}
			if row.Values.Type != protocol.ValueMap {
				return badParam("counter row MapValue")
			}
			values := []protocol.Value{stringValue(extendedClockText(row.Time, s.config.Location, false))}
			for _, name := range columns {
				v := stringValue("")
				for _, e := range row.Values.Entries {
					if e.Key == name {
						v = e.Value
						break
					}
				}
				values = append(values, v)
			}
			if err := stream.ResponsePack(newMap(entry("values", list(values)))); err != nil {
				return err
			}
		}
		return nil
	}
	tt, err := getInt(p, "timeType")
	if err != nil {
		return err
	}
	delta := counterDelta(tt)
	if delta == 0 {
		return badParam("timeType")
	}
	n := int(scoutertime.DayMillis / delta)
	if len(columns) > 0 && n > extendedQueryCells/len(columns) {
		return transport.ErrLimit
	}
	series := make([][]protocol.Value, len(columns))
	for i, name := range columns {
		if err := ctx.Err(); err != nil {
			return err
		}
		series[i], err = s.backend.CounterDaily(ctx, date, hash, name, byte(tt))
		if err != nil {
			return err
		}
	}
	if err := stream.ResponsePack(newMap(entry("values", list(header)))); err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		values := []protocol.Value{stringValue(extendedClockText(midnight+int64(i)*delta, s.config.Location, true))}
		for _, data := range series {
			v := protocol.Value{Type: protocol.ValueNull}
			if i < len(data) {
				v = data[i]
			}
			values = append(values, v)
		}
		if err := stream.ResponsePack(newMap(entry("values", list(values)))); err != nil {
			return err
		}
	}
	return nil
}
func extendedClockText(ms int64, location *time.Location, minute bool) string {
	seconds := scoutertime.Millis(ms, location) / 1000
	h, m, s := seconds/3600, (seconds/60)%60, seconds%60
	if minute {
		return fmt.Sprintf("%02d:%02d", h, m)
	}
	return fmt.Sprintf("%02d%02d%02d", h, m, s)
}
