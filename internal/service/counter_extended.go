package service

import (
	"context"
	"fmt"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/transport"
	"math"
	"strconv"
	"strings"
)

// CounterCatalogBackend is optional. Missing catalog support is an explicit
// unsupported result, never an empty successful counter list.
type CounterCatalogBackend interface {
	CounterNames(context.Context, int32) ([]string, error)
	CounterNamesForDate(context.Context, string, int32) ([]string, error)
	CounterType(context.Context, string) (CounterTypeMetadata, error)
}
type CounterTypeMetadata struct {
	DisplayName string
	Counters    []string
}
type CounterRow struct {
	Time   int64
	Values protocol.Value
}

// CounterRowsBackend preserves each stored realtime row, including rows whose
// requested counters are all absent and repeated timestamps.
type CounterRowsBackend interface {
	CounterRows(context.Context, string, int32, int64, int64) ([]CounterRow, error)
}

// CounterBulkRowsBackend follows Java readBulk's minute chunks and physical
// offset spans. Query grouping uses embedded objHash/time, not index metadata.
type CounterBulkRowsBackend interface {
	CounterBulkRows(context.Context, string, int64, int64) ([]CounterRow, error)
}

const extendedQueryCells = 100000
const extendedQueryDays = 3660

func ExtendedCounterCommands() []string {
	return []string{
		"COUNTER_REAL_TIME_OBJECT_ALL", "COUNTER_REAL_TIME_OBJECT_TYPE_ALL", "COUNTER_REAL_TIME_ALL_MULTI", "COUNTER_REAL_TIME_TOT", "COUNTER_PAST_TIME_TOT", "COUNTER_PAST_TIME_GROUP", "COUNTER_REAL_TIME_GROUP",
		"COUNTER_TODAY_ALL", "COUNTER_TODAY_TOT", "COUNTER_PAST_DATE_TOT", "COUNTER_PAST_DATE_ALL", "COUNTER_PAST_LONGDATE_ALL", "COUNTER_PAST_LONGDATE_TOT", "GET_COUNTER_EXIST_DAYS", "COUNTER_PAST_DATE_GROUP", "COUNTER_TODAY_GROUP", "COUNTER_PAST_LONGDATE_GROUP",
		"ACTIVESPEED_REAL_TIME", "ACTIVESPEED_GROUP_REAL_TIME", "ACTIVESPEED_REAL_TIME_GROUP", "ACTIVESPEED_GROUP_REAL_TIME_GROUP", "SHOW_REAL_TIME_STRING", "COUNTER_MAP_REAL_TIME",
		"EXPORT_OBJECT_TIME_COUNTER", "EXPORT_OBJECT_REGULAR_COUNTER", "OBJECT_TODAY_FULL_LIST",
	}
}

// HandleExtendedCounterCommand consumes only a recognized command's body.
// The owning dispatcher appends NoNEXT; failures preserve already emitted items.
func (s *Service) HandleExtendedCounterCommand(ctx context.Context, request transport.ClientRequest, stream *transport.Stream) (bool, error) {
	recognized := false
	for _, cmd := range ExtendedCounterCommands() {
		if cmd == request.Command {
			recognized = true
			break
		}
	}
	if !recognized {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if !request.Authenticated {
		return true, transport.ErrUnauthorized
	}
	if request.Command == "OBJECT_TODAY_FULL_LIST" {
		return true, s.objectTodayFullList(ctx, stream)
	}
	p, err := readMap(stream)
	if err != nil {
		return true, err
	}
	return true, s.extendedCounterQuery(ctx, request.Command, p, stream)
}

func (s *Service) extendedCounterQuery(ctx context.Context, cmd string, p *protocol.MapPack, stream *transport.Stream) error {
	switch cmd {
	case "EXPORT_OBJECT_TIME_COUNTER", "EXPORT_OBJECT_REGULAR_COUNTER":
		return s.exportCounterQuery(ctx, cmd, p, stream)
	case "ACTIVESPEED_REAL_TIME", "ACTIVESPEED_GROUP_REAL_TIME", "ACTIVESPEED_REAL_TIME_GROUP", "ACTIVESPEED_GROUP_REAL_TIME_GROUP", "SHOW_REAL_TIME_STRING", "COUNTER_MAP_REAL_TIME":
		return s.specialCounterQuery(ctx, cmd, p, stream)
	case "COUNTER_REAL_TIME_OBJECT_ALL", "COUNTER_REAL_TIME_OBJECT_TYPE_ALL":
		return s.extendedCounterCatalog(ctx, cmd, p, stream)
	case "COUNTER_REAL_TIME_ALL_MULTI", "COUNTER_REAL_TIME_GROUP", "COUNTER_REAL_TIME_TOT":
		return s.extendedCounterRealtime(ctx, cmd, p, stream)
	case "COUNTER_PAST_TIME_GROUP":
		return s.extendedCounterPastGroup(ctx, p, stream)
	case "COUNTER_PAST_TIME_TOT":
		return s.extendedCounterPastTotal(ctx, p, stream)
	case "COUNTER_TODAY_ALL", "COUNTER_PAST_DATE_ALL", "COUNTER_PAST_DATE_GROUP", "COUNTER_TODAY_GROUP", "COUNTER_PAST_LONGDATE_ALL", "COUNTER_PAST_LONGDATE_GROUP":
		return s.extendedCounterDailyGroups(ctx, cmd, p, stream)
	case "COUNTER_TODAY_TOT", "COUNTER_PAST_DATE_TOT", "COUNTER_PAST_LONGDATE_TOT":
		return s.extendedCounterDailyTotal(ctx, cmd, p, stream)
	case "GET_COUNTER_EXIST_DAYS":
		return s.extendedCounterExistDays(ctx, p, stream)
	}
	return transport.ErrUnsupported
}

// CounterBulkRowsForBackend reads the bulk rows of the requested objects only,
// so the other objects' rows are neither decoded nor counted against limits.
type CounterBulkRowsForBackend interface {
	CounterBulkRowsFor(ctx context.Context, date string, from, to int64, hashes map[int32]bool) ([]CounterRow, error)
}

// bulkRowsFor returns the rows the caller's own objHash filter can select.
func bulkRowsFor(ctx context.Context, backend CounterBulkRowsBackend, date string, from, to int64, hashes []int32) ([]CounterRow, error) {
	filtered, ok := backend.(CounterBulkRowsForBackend)
	if !ok {
		return backend.CounterBulkRows(ctx, date, from, to)
	}
	set := make(map[int32]bool, len(hashes))
	for _, hash := range hashes {
		set[hash] = true
	}
	return filtered.CounterBulkRowsFor(ctx, date, from, to, set)
}

func (s *Service) extendedCounterPastGroup(ctx context.Context, p *protocol.MapPack, stream *transport.Stream) error {
	backend, ok := s.backend.(CounterBulkRowsBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	name, err := getText(p, "counter")
	if err != nil {
		return err
	}
	hashes, err := getHashes(p, "objHash")
	if err != nil {
		return err
	}
	if len(hashes) > extendedQueryCells {
		return transport.ErrLimit
	}
	from, to, err := extendedTimeRange(p)
	if err != nil {
		return err
	}
	rows, err := bulkRowsFor(ctx, backend, scoutertime.Date(from, s.config.Location), from, to, hashes)
	if err != nil {
		return err
	}
	if len(rows) > extendedQueryCells {
		return transport.ErrLimit
	}
	series := map[int32][]CounterPoint{}
	for _, hash := range hashes {
		series[hash] = []CounterPoint{}
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if row.Values.Type == protocol.ValueNull {
			continue
		}
		if row.Values.Type != protocol.ValueMap {
			return badParam("bulk counter MapValue")
		}
		m := &protocol.MapPack{Entries: row.Values.Entries}
		hash := extendedInt(lookup(m, "objHash"))
		points, selected := series[hash]
		if !selected {
			continue
		}
		for _, e := range m.Entries {
			if e.Key == name {
				series[hash] = append(points, CounterPoint{Time: extendedLong(lookup(m, "time")), Value: e.Value})
				break
			}
		}
	}
	cells := 0
	for _, hash := range hashes {
		cells += len(series[hash])
		if cells > extendedQueryCells {
			return transport.ErrLimit
		}
		pack := counterSeries(series[hash])
		pack.Entries = append([]protocol.Entry{entry("objHash", decimal(int64(hash)))}, pack.Entries...)
		if err := stream.ResponsePack(pack); err != nil {
			return err
		}
	}
	return nil
}

// Actual pinned CounterService execution confirms that an empty TimedSeries
// emits empty lists, while addEnd on any populated series throws before output.
func (s *Service) extendedCounterPastTotal(ctx context.Context, p *protocol.MapPack, stream *transport.Stream) error {
	name, err := getText(p, "counter")
	if err != nil {
		return err
	}
	typ, err := getText(p, "objType")
	if err != nil || typ == "" {
		return err
	}
	from, to, err := extendedTimeRange(p)
	if err != nil {
		return err
	}
	objects, err := s.extendedObjects(ctx, scoutertime.Date(from, s.config.Location), typ, false)
	if err != nil {
		return err
	}
	populated := false
	cells := 0
	hashes := make([]int32, len(objects))
	for i, o := range objects {
		hashes[i] = o.ObjHash
	}
	err = s.counterHistories(ctx, hashes, name, from, to, func(_ int32, points []CounterPoint) error {
		cells += len(points)
		if cells > extendedQueryCells {
			return transport.ErrLimit
		}
		populated = populated || len(points) > 0
		return nil
	})
	if err != nil {
		return err
	}
	if populated {
		return fmt.Errorf("%w: pinned TimedSeries.addEnd ClassCastException", transport.ErrMalformed)
	}
	return stream.ResponsePack(newMap(entry("time", list(nil)), entry("value", list(nil))))
}

func (s *Service) extendedObjects(ctx context.Context, date, typ string, live bool) ([]*protocol.ObjectPack, error) {
	objects, err := s.backend.Objects(ctx, date)
	if err != nil {
		return nil, err
	}
	if len(objects) > extendedQueryCells {
		return nil, transport.ErrLimit
	}
	out := make([]*protocol.ObjectPack, 0, len(objects))
	for _, o := range objects {
		if o != nil && o.ObjType == typ && (!live || o.Alive) {
			out = append(out, o)
		}
	}
	return out, nil
}
func (s *Service) extendedCounterCatalog(ctx context.Context, cmd string, p *protocol.MapPack, stream *transport.Stream) error {
	catalog, ok := s.backend.(CounterCatalogBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	if cmd == "COUNTER_REAL_TIME_OBJECT_ALL" {
		hash, err := getInt(p, "objHash")
		if err != nil {
			return err
		}
		names, err := catalog.CounterNames(ctx, hash)
		if err != nil {
			return err
		}
		if len(names) > extendedQueryCells {
			return transport.ErrLimit
		}
		ks, vs := []protocol.Value{}, []protocol.Value{}
		for _, name := range names {
			v, found, err := s.backend.Counter(ctx, hash, name, 1)
			if err != nil {
				return err
			}
			if found {
				ks = append(ks, stringValue(name))
				vs = append(vs, v)
			}
		}
		return stream.ResponsePack(newMap(entry("counter", list(ks)), entry("value", list(vs))))
	}
	typ, err := getText(p, "objType")
	if err != nil {
		return err
	}
	objects, err := s.extendedObjects(ctx, "", typ, true)
	if err != nil {
		return err
	}
	result := newMap()
	cells := 0
	for _, o := range objects {
		names, err := catalog.CounterNames(ctx, o.ObjHash)
		if err != nil {
			return err
		}
		if len(names) > extendedQueryCells-cells {
			return transport.ErrLimit
		}
		entries := []protocol.Entry{}
		for _, name := range names {
			v, found, err := s.backend.Counter(ctx, o.ObjHash, name, 1)
			if err != nil {
				return err
			}
			if found {
				entries = append(entries, entry(name, v))
				cells++
				if cells > extendedQueryCells {
					return transport.ErrLimit
				}
			}
		}
		if len(entries) > 0 {
			extendedSetEntry(&result.Entries, o.ObjName, protocol.Value{Type: protocol.ValueMap, Entries: entries})
		}
	}
	return stream.ResponsePack(result)
}
func extendedSetEntry(entries *[]protocol.Entry, key string, value protocol.Value) {
	for i := range *entries {
		if (*entries)[i].Key == key {
			(*entries)[i].Value = value
			return
		}
	}
	*entries = append(*entries, entry(key, value))
}
func (s *Service) extendedCounterRealtime(ctx context.Context, cmd string, p *protocol.MapPack, stream *transport.Stream) error {
	name, err := getText(p, "counter")
	if cmd == "COUNTER_REAL_TIME_ALL_MULTI" {
		err = nil
	}
	if err != nil {
		return err
	}
	typ, err := getText(p, "objType")
	if err != nil {
		return err
	}
	hashes := []int32{}
	if cmd != "COUNTER_REAL_TIME_TOT" {
		hashes, err = getHashes(p, "objHash")
		if err != nil {
			return err
		}
	}
	if cmd == "COUNTER_REAL_TIME_TOT" || cmd == "COUNTER_REAL_TIME_ALL_MULTI" && len(hashes) == 0 {
		if typ == "" {
			return nil
		}
		objects, err := s.extendedObjects(ctx, "", typ, true)
		if err != nil {
			return err
		}
		for _, o := range objects {
			hashes = append(hashes, o.ObjHash)
		}
	}
	if cmd == "COUNTER_REAL_TIME_TOT" {
		mode, err := getText(p, "mode")
		if err != nil {
			return err
		}
		sum, count := 0.0, 0
		for _, hash := range hashes {
			v, found, err := s.backend.Counter(ctx, hash, name, 1)
			if err != nil {
				return err
			}
			if found {
				sum += extendedDouble(v)
				count++
			}
		}
		if count > 0 && strings.EqualFold(mode, "avg") {
			sum /= float64(count)
		}
		return stream.ResponseValue(protocol.Value{Type: protocol.ValueDouble, Double: sum})
	}
	names := []protocol.Value{stringValue(name)}
	if cmd == "COUNTER_REAL_TIME_ALL_MULTI" {
		names, err = getList(p, "counter")
		if err != nil {
			return err
		}
	}
	hs, cs, vs := []protocol.Value{}, []protocol.Value{}, []protocol.Value{}
	if len(hashes) > extendedQueryCells || len(names) > 0 && len(hashes) > extendedQueryCells/len(names) {
		return transport.ErrLimit
	}
	for _, hash := range hashes {
		for _, n := range names {
			if n.Type != protocol.ValueText {
				return badParam("counter")
			}
			v, found, err := s.backend.Counter(ctx, hash, n.Text, 1)
			if err != nil {
				return err
			}
			if found {
				hs = append(hs, decimal(int64(hash)))
				cs = append(cs, n)
				vs = append(vs, v)
				if len(vs) > extendedQueryCells {
					return transport.ErrLimit
				}
			}
		}
	}
	if cmd == "COUNTER_REAL_TIME_GROUP" {
		return stream.ResponsePack(newMap(entry("objHash", list(hs)), entry("value", list(vs))))
	}
	return stream.ResponsePack(newMap(entry("objHash", list(hs)), entry("counter", list(cs)), entry("value", list(vs))))
}
func extendedTimeRange(p *protocol.MapPack) (int64, int64, error) {
	from, err := getNumber(p, "stime")
	if err != nil {
		return 0, 0, err
	}
	to, err := getNumber(p, "etime")
	if err != nil {
		return 0, 0, err
	}
	if to < from {
		return 0, 0, badParam("stime/etime")
	}
	return from, to, nil
}
func extendedDouble(v protocol.Value) float64 {
	switch v.Type {
	case protocol.ValueDecimal:
		return float64(v.Int)
	case protocol.ValueFloat:
		return float64(v.Float)
	case protocol.ValueDouble:
		return v.Double
	case protocol.ValueText:
		n, _ := strconv.ParseFloat(v.Text, 64)
		return n
	}
	return 0
}
func extendedLong(v protocol.Value) int64 {
	switch v.Type {
	case protocol.ValueDecimal:
		return v.Int
	case protocol.ValueText:
		n, _ := strconv.ParseInt(v.Text, 10, 64)
		return n
	}
	n := extendedDouble(v)
	if math.IsNaN(n) {
		return 0
	}
	if n >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	if n <= float64(math.MinInt64) {
		return math.MinInt64
	}
	return int64(n)
}

func (s *Service) extendedDailyDates(cmd string, p *protocol.MapPack) ([]string, error) {
	today := scoutertime.Date(s.config.Clock().UnixMilli(), s.config.Location)
	if strings.HasPrefix(cmd, "COUNTER_TODAY") {
		return []string{today}, nil
	}
	if !strings.Contains(cmd, "LONGDATE") {
		date, err := s.date(p, false)
		if err != nil {
			return nil, err
		}
		if date == "" {
			return nil, badParam("date")
		}
		return []string{date}, nil
	}
	var from, to int64
	var err error
	if cmd == "COUNTER_PAST_LONGDATE_GROUP" {
		from, to, err = extendedTimeRange(p)
		if err != nil {
			return nil, err
		}
		from, err = scoutertime.ParseDate(scoutertime.Date(from, s.config.Location), s.config.Location)
		if err != nil {
			return nil, err
		}
		to, err = scoutertime.ParseDate(scoutertime.Date(to, s.config.Location), s.config.Location)
	} else {
		a, e := getText(p, "sDate")
		if e != nil {
			return nil, e
		}
		b, e := getText(p, "eDate")
		if e != nil {
			return nil, e
		}
		from, err = scoutertime.ParseDate(a, s.config.Location)
		if err == nil {
			to, err = scoutertime.ParseDate(b, s.config.Location)
		}
	}
	if err != nil {
		return nil, badParam("date range")
	}
	if to < from {
		return []string{}, nil
	}
	days := (to-from)/scoutertime.DayMillis + 1
	if days > extendedQueryDays {
		return nil, transport.ErrLimit
	}
	dates := make([]string, 0, int(days))
	for t := from; t <= to; t += scoutertime.DayMillis {
		dates = append(dates, scoutertime.Date(t, s.config.Location))
	}
	return dates, nil
}
func (s *Service) extendedDailyHashes(ctx context.Context, cmd string, p *protocol.MapPack, dates []string) ([]int32, error) {
	if strings.HasSuffix(cmd, "GROUP") {
		return getHashes(p, "objHash")
	}
	if cmd == "COUNTER_PAST_LONGDATE_ALL" {
		hashes, err := getHashes(p, "objHash")
		if err != nil || len(hashes) > 0 {
			return hashes, err
		}
	}
	typ, err := getText(p, "objType")
	if err != nil || typ == "" {
		return nil, err
	}
	hashes := []int32{}
	seen := map[int32]bool{}
	datesToRead := dates
	if strings.HasPrefix(cmd, "COUNTER_TODAY") {
		datesToRead = []string{""}
	}
	for _, date := range datesToRead {
		objects, err := s.backend.Objects(ctx, date)
		if err != nil {
			return nil, err
		}
		if len(objects) > extendedQueryCells {
			return nil, transport.ErrLimit
		}
		for _, o := range objects {
			if o != nil && !seen[o.ObjHash] {
				seen[o.ObjHash] = true
				if o.ObjType == typ {
					hashes = append(hashes, o.ObjHash)
				}
				if len(hashes) > extendedQueryCells {
					return nil, transport.ErrLimit
				}
			}
		}
	}
	return hashes, nil
}
func (s *Service) extendedDailyPack(date string, hash int32, values []protocol.Value) (*protocol.MapPack, error) {
	midnight, err := scoutertime.ParseDate(date, s.config.Location)
	if err != nil {
		return nil, err
	}
	times := make([]protocol.Value, len(values))
	for i := range times {
		times[i] = decimal(midnight + int64(i)*300000)
	}
	return newMap(entry("objHash", decimal(int64(hash))), entry("time", list(times)), entry("value", list(values))), nil
}
func (s *Service) extendedCounterDailyGroups(ctx context.Context, cmd string, p *protocol.MapPack, stream *transport.Stream) error {
	name, err := getText(p, "counter")
	if err != nil {
		return err
	}
	dates, err := s.extendedDailyDates(cmd, p)
	if err != nil {
		return err
	}
	hashes, err := s.extendedDailyHashes(ctx, cmd, p, dates)
	if err != nil {
		return err
	}
	cells := 0
	if cmd == "COUNTER_PAST_LONGDATE_GROUP" {
		for _, hash := range hashes {
			allTimes, allValues := []protocol.Value{}, []protocol.Value{}
			for _, date := range dates {
				values, err := s.backend.CounterDaily(ctx, date, hash, name, 3)
				if err != nil {
					return err
				}
				if values == nil {
					values = make([]protocol.Value, 288)
				}
				cells += len(values)
				if cells > extendedQueryCells {
					return transport.ErrLimit
				}
				pack, err := s.extendedDailyPack(date, hash, values)
				if err != nil {
					return err
				}
				allTimes = append(allTimes, lookup(pack, "time").Values...)
				allValues = append(allValues, values...)
			}
			if err := stream.ResponsePack(newMap(entry("objHash", decimal(int64(hash))), entry("time", list(allTimes)), entry("value", list(allValues)))); err != nil {
				return err
			}
		}
		return nil
	}
	for _, date := range dates {
		for _, hash := range hashes {
			if err := ctx.Err(); err != nil {
				return err
			}
			values, err := s.backend.CounterDaily(ctx, date, hash, name, 3)
			if err != nil {
				return err
			}
			if cmd == "COUNTER_TODAY_ALL" && values == nil {
				continue
			}
			cells += len(values)
			if cells > extendedQueryCells {
				return transport.ErrLimit
			}
			pack, err := s.extendedDailyPack(date, hash, values)
			if err != nil {
				return err
			}
			if err = stream.ResponsePack(pack); err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *Service) extendedCounterDailyTotal(ctx context.Context, cmd string, p *protocol.MapPack, stream *transport.Stream) error {
	if cmd == "COUNTER_TODAY_TOT" {
		return s.extendedCounterTodayTotal(ctx, p, stream)
	}
	name, err := getText(p, "counter")
	if err != nil {
		return err
	}
	typ, err := getText(p, "objType")
	if err != nil || typ == "" {
		return err
	}
	mode, err := getText(p, "mode")
	if err != nil {
		return err
	}
	dates, err := s.extendedDailyDates(cmd, p)
	if err != nil {
		return err
	}
	if len(dates)*288 > extendedQueryCells {
		return transport.ErrLimit
	}
	hashes, err := s.extendedDailyHashes(ctx, cmd, p, dates)
	if err != nil {
		return err
	}
	sums := make([]float64, len(dates)*288)
	counts := make([]int, len(sums))
	times := make([]protocol.Value, len(sums))
	for d, date := range dates {
		midnight, _ := scoutertime.ParseDate(date, s.config.Location)
		for i := 0; i < 288; i++ {
			times[d*288+i] = decimal(midnight + int64(i)*300000)
		}
		for _, hash := range hashes {
			values, err := s.backend.CounterDaily(ctx, date, hash, name, 3)
			if err != nil {
				return err
			}
			if len(values) > 288 {
				return transport.ErrLimit
			}
			for i, v := range values {
				n := extendedDouble(v)
				if cmd == "COUNTER_PAST_LONGDATE_TOT" {
					if n <= 0 || math.IsNaN(n) {
						continue
					}
				} else if v.Type == protocol.ValueNull {
					continue
				}
				sums[d*288+i] += n
				counts[d*288+i]++
			}
		}
	}
	values := make([]protocol.Value, len(sums))
	for i, n := range sums {
		if strings.EqualFold(mode, "avg") && counts[i] > 1 {
			n /= float64(counts[i])
		}
		values[i] = protocol.Value{Type: protocol.ValueDouble, Double: n}
	}
	return stream.ResponsePack(newMap(entry("time", list(times)), entry("value", list(values))))
}

// The original Scala Array[Double](288)/Array[Int](288) initializes a one-
// element array containing 288. A later positive slot throws inside the
// per-object catch; already accumulated slot zero still reaches the response.
func (s *Service) extendedCounterTodayTotal(ctx context.Context, p *protocol.MapPack, stream *transport.Stream) error {
	name, err := getText(p, "counter")
	if err != nil {
		return err
	}
	typ, err := getText(p, "objType")
	if err != nil || typ == "" {
		return err
	}
	mode, err := getText(p, "mode")
	if err != nil {
		return err
	}
	objects, err := s.extendedObjects(ctx, "", typ, false)
	if err != nil {
		return err
	}
	date := scoutertime.Date(s.config.Clock().UnixMilli(), s.config.Location)
	sum, count := 288.0, 288
	for _, o := range objects {
		values, err := s.backend.CounterDaily(ctx, date, o.ObjHash, name, 3)
		if err != nil {
			return err
		}
		if len(values) > extendedQueryCells {
			return transport.ErrLimit
		}
		for i, v := range values {
			n := extendedDouble(v)
			if n > 0 {
				if i != 0 {
					break
				}
				sum += n
				count++
			}
		}
	}
	if strings.EqualFold(mode, "avg") && count > 1 {
		sum /= float64(count)
	}
	midnight, err := scoutertime.ParseDate(date, s.config.Location)
	if err != nil {
		return err
	}
	return stream.ResponsePack(newMap(entry("time", list([]protocol.Value{decimal(midnight)})), entry("value", list([]protocol.Value{{Type: protocol.ValueDouble, Double: sum}}))))
}
func (s *Service) extendedCounterExistDays(ctx context.Context, p *protocol.MapPack, stream *transport.Stream) error {
	name, err := getText(p, "counter")
	if err != nil {
		return err
	}
	typ, err := getText(p, "objType")
	if err != nil {
		return err
	}
	duration, err := getInt(p, "duration")
	if err != nil {
		return err
	}
	end, err := getNumber(p, "etime")
	if err != nil {
		return err
	}
	if duration < 0 {
		return stream.ResponsePack(newMap(entry("date", list(nil)), entry("exist", list(nil))))
	}
	if duration >= extendedQueryDays {
		return transport.ErrLimit
	}
	dates, exist := []protocol.Value{}, []protocol.Value{}
	for i := int32(0); i <= duration; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		date := scoutertime.Date(end-int64(duration-i)*scoutertime.DayMillis, s.config.Location)
		objects, err := s.extendedObjects(ctx, date, typ, false)
		if err != nil {
			return err
		}
		found := false
		for _, o := range objects {
			values, err := s.backend.CounterDaily(ctx, date, o.ObjHash, name, 3)
			if err != nil {
				return err
			}
			if len(values) > 0 {
				found = true
			}
		}
		dates = append(dates, stringValue(date))
		exist = append(exist, boolean(found))
	}
	return stream.ResponsePack(newMap(entry("date", list(dates)), entry("exist", list(exist))))
}
