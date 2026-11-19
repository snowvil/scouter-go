package service

import (
	"context"
	"math"
	"strconv"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/transport"
)

// MariaService.scala, CubridService.scala and StatusRD.scala at the pinned
// source commit define these handlers. MapValue has unordered keys; MapPack's
// insertion order and each ListValue's order are preserved.
const databaseDigestKey = "events_statements_summary_by_digest"
const databaseCurrentKey = "events_statements_current"

func DatabaseCounterCommands() []string {
	return []string{"DB_REALTIME_CONNECTIONS", "DB_REALTIME_ACTIVITY", "DB_REALTIME_HIT_RATIO", "DB_REALTIME_RESPONSE_TIME", "DB_DAILY_CONNECTIONS", "DB_DAILY_ACTIVITY", "DB_DIGEST_TABLE", "DB_LAST_DIGEST_TABLE", "DB_MAX_TIMER_WAIT_THREAD", "DB_LOAD_DIGEST_COUNTER", "CUBRID_ACTIVE_DB_LIST", "CUBRID_DB_REALTIME_DML", "CUBRID_DB_REALTIME_MULTI_DATA", "CUBRID_DB_PERIOD_MULTI_DATA", "CUBRID_DB_LONG_PERIOD_MULTI_DATA", "CUBRID_DB_REALTIME_STATUS", "CUBRID_DB_SERVER_INFO", "CUBRID_DB_LONG_TRANSACTION_DATA"}
}

func (s *Service) HandleDatabaseCounterCommand(ctx context.Context, r transport.ClientRequest, st *transport.Stream) (bool, error) {
	recognized := false
	for _, cmd := range DatabaseCounterCommands() {
		if cmd == r.Command {
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
	if !r.Authenticated {
		return true, transport.ErrUnauthorized
	}
	p, err := readMap(st)
	if err != nil {
		return true, err
	}
	hashes, err := databaseHashes(p, r.Command == "CUBRID_DB_LONG_PERIOD_MULTI_DATA")
	if err != nil {
		return true, err
	}
	switch r.Command {
	case "DB_REALTIME_CONNECTIONS", "DB_REALTIME_ACTIVITY", "DB_REALTIME_HIT_RATIO", "CUBRID_DB_REALTIME_DML", "CUBRID_DB_REALTIME_MULTI_DATA":
		return true, s.databaseRealtime(ctx, r.Command, p, hashes, st)
	case "DB_DAILY_CONNECTIONS", "DB_DAILY_ACTIVITY", "CUBRID_DB_LONG_PERIOD_MULTI_DATA":
		return true, s.databaseDaily(ctx, r.Command, p, hashes, st)
	case "CUBRID_DB_PERIOD_MULTI_DATA":
		// Same readBulk grouping contract, including duplicate object requests.
		return true, s.databasePeriod(ctx, p, hashes, st)
	default:
		return true, s.databaseStatusQuery(ctx, r.Command, p, hashes, st)
	}
}

func databaseHashes(p *protocol.MapPack, optional bool) ([]int32, error) {
	v := lookup(p, "objHash")
	if optional && v.Type == protocol.ValueNull {
		return nil, nil
	}
	if v.Type != protocol.ValueList {
		return nil, badParam("objHash ListValue")
	}
	if len(v.Values) > extendedQueryCells {
		return nil, transport.ErrLimit
	}
	hashes := make([]int32, len(v.Values))
	for i, v := range v.Values {
		hashes[i] = databaseInt(v)
	}
	return hashes, nil
}

// MapPack/ListValue getters accept NumberValue only, whereas CounterCache
// handlers use CastUtil and also accept a parseable TextValue.
func databaseLong(v protocol.Value) int64 {
	if v.Type == protocol.ValueDecimal || v.Type == protocol.ValueFloat || v.Type == protocol.ValueDouble {
		return extendedLong(v)
	}
	return 0
}
func databaseInt(v protocol.Value) int32 {
	if v.Type == protocol.ValueDecimal {
		return int32(v.Int)
	}
	if v.Type == protocol.ValueFloat || v.Type == protocol.ValueDouble {
		return extendedInt(v)
	}
	return 0
}
func databaseDouble(v protocol.Value) float64 {
	if v.Type == protocol.ValueDecimal || v.Type == protocol.ValueFloat || v.Type == protocol.ValueDouble {
		return extendedDouble(v)
	}
	return 0
}
func databaseList(p *protocol.StatusPack, key string) ([]protocol.Value, error) {
	if p.Data.Type != protocol.ValueMap {
		return nil, badParam("StatusPack MapValue")
	}
	v := lookup(newMap(p.Data.Entries...), key)
	if v.Type != protocol.ValueList {
		return nil, badParam("StatusPack " + key + " ListValue")
	}
	if len(v.Values) > extendedQueryCells {
		return nil, transport.ErrLimit
	}
	return v.Values, nil
}
func databaseAt(p *protocol.StatusPack, key string, i int) (protocol.Value, error) {
	v, err := databaseList(p, key)
	if err != nil {
		return protocol.Value{}, err
	}
	if i < 0 || i >= len(v) {
		return protocol.Value{}, badParam("StatusPack aligned " + key)
	}
	return v[i], nil
}

func (s *Service) databaseRealtime(ctx context.Context, cmd string, p *protocol.MapPack, hashes []int32, st *transport.Stream) error {
	if len(hashes) == 0 {
		return nil
	}
	if cmd == "DB_REALTIME_HIT_RATIO" {
		out := newMap()
		for _, field := range []struct{ name, key string }{{"INNO_BUF_HIT", "innodb_buffer"}, {"KEY_RATIO", "key_cache"}, {"QCACHE_RATIO", "query_cache"}, {"TCACHE_RATIO", "thread_cache"}} {
			var sum float32
			count := 0
			for _, h := range hashes {
				if err := ctx.Err(); err != nil {
					return err
				}
				v, ok, err := s.backend.Counter(ctx, h, field.name, 1)
				if err != nil {
					return err
				}
				if ok {
					sum += extendedFloat(v)
					count++
				}
			}
			if count > 0 {
				out.Entries = append(out.Entries, entry(field.key, protocol.Value{Type: protocol.ValueFloat, Float: sum / float32(count)}))
			}
		}
		return st.ResponsePack(out)
	}
	fields := []struct{ name, key string }{{"THREADS_TOTAL", "total"}, {"THREADS_RUNNING", "active"}}
	switch cmd {
	case "DB_REALTIME_ACTIVITY":
		fields = []struct{ name, key string }{{"COM_CALL_PROCEDURE", "call"}, {"COM_SELECT", "select"}, {"COM_UPDATE", "update"}, {"COM_INSERT", "insert"}, {"COM_DELETE", "delete"}}
	case "CUBRID_DB_REALTIME_DML":
		fields = []struct{ name, key string }{{"num_query_selects", "select"}, {"num_query_updates", "update"}, {"num_query_inserts", "insert"}, {"num_query_deletes", "delete"}}
	case "CUBRID_DB_REALTIME_MULTI_DATA":
		name, err := getText(p, "counter")
		if err != nil {
			return err
		}
		fields = []struct{ name, key string }{{name, name}}
	}
	sums := make([]int64, len(fields))
	dbname := ""
	for _, h := range hashes {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i, field := range fields {
			v, ok, err := s.backend.Counter(ctx, h, field.name, 1)
			if err != nil {
				return err
			}
			if ok {
				sums[i] += extendedLong(v)
			}
		}
		if cmd == "CUBRID_DB_REALTIME_DML" {
			v, ok, err := s.backend.Counter(ctx, h, "db_num", 1)
			if err != nil {
				return err
			}
			if ok {
				dbname += strconv.FormatInt(extendedLong(v), 10)
			}
		}
	}
	out := protocol.Value{Type: protocol.ValueMap}
	if cmd == "CUBRID_DB_REALTIME_DML" {
		out.Entries = append(out.Entries, entry("dbname", stringValue(dbname)))
	}
	for i, field := range fields {
		out.Entries = append(out.Entries, entry(field.key, decimal(sums[i])))
	}
	return st.ResponseValue(out)
}

func (s *Service) databaseDaily(ctx context.Context, cmd string, p *protocol.MapPack, hashes []int32, st *transport.Stream) error {
	a, b := "sdate", "edate"
	if cmd == "CUBRID_DB_LONG_PERIOD_MULTI_DATA" {
		a, b = "sDate", "eDate"
	}
	start, err := getText(p, a)
	if err != nil {
		return err
	}
	end, err := getText(p, b)
	if err != nil {
		return err
	}
	from, err := scoutertime.ParseDate(start, s.config.Location)
	if err != nil {
		return badParam(a)
	}
	last, err := scoutertime.ParseDate(end, s.config.Location)
	if err != nil {
		return badParam(b)
	}
	if last < from {
		if cmd == "CUBRID_DB_LONG_PERIOD_MULTI_DATA" {
			return nil
		}
		return badParam("date range")
	}
	if last > math.MaxInt64-scoutertime.DayMillis {
		return badParam("date range")
	}
	days := (last-from)/scoutertime.DayMillis + 1
	if days > extendedQueryDays {
		return transport.ErrLimit
	}
	if cmd == "CUBRID_DB_LONG_PERIOD_MULTI_DATA" {
		if days*int64(len(hashes)) > extendedQueryCells {
			return transport.ErrLimit
		}
		counter, err := getText(p, "counter")
		if err != nil {
			return err
		}
		cells := 0
		for day := from; day <= last; day += scoutertime.DayMillis {
			for _, h := range hashes {
				if err := ctx.Err(); err != nil {
					return err
				}
				v, err := s.backend.CounterDaily(ctx, scoutertime.Date(day, s.config.Location), h, counter, 3)
				if err != nil {
					return err
				}
				cells += len(v)
				if cells > extendedQueryCells {
					return transport.ErrLimit
				}
				times := make([]protocol.Value, len(v))
				for i := range v {
					times[i] = decimal(day + int64(i)*300000)
				}
				if err := st.ResponsePack(newMap(entry("objHash", decimal(int64(h))), entry("time", list(times)), entry("value", list(v)))); err != nil {
					return err
				}
			}
		}
		return nil
	}
	fields := []struct{ name, key string }{{"THREADS_TOTAL", "total"}, {"THREADS_RUNNING", "active"}}
	if cmd == "DB_DAILY_ACTIVITY" {
		fields = []struct{ name, key string }{{"COM_CALL_PROCEDURE", "call"}, {"COM_SELECT", "select"}, {"COM_INSERT", "insert"}, {"COM_UPDATE", "update"}, {"COM_DELETE", "delete"}}
	}
	length := (last + scoutertime.DayMillis - from) / 300000
	if length*int64(len(fields)+1) > extendedQueryCells || days*int64(len(hashes))*int64(len(fields)) > extendedQueryCells {
		return transport.ErrLimit
	}
	sums := make([][]float64, len(fields))
	for i := range sums {
		sums[i] = make([]float64, int(length))
	}
	dayIndex := 0
	for day := from; day <= last; day += scoutertime.DayMillis {
		for _, h := range hashes {
			for i, field := range fields {
				if err := ctx.Err(); err != nil {
					return err
				}
				values, err := s.backend.CounterDaily(ctx, scoutertime.Date(day, s.config.Location), h, field.name, 3)
				if err != nil {
					return err
				}
				if len(values) > 288 {
					return badParam("daily counter length")
				}
				for j, v := range values {
					if dayIndex+j >= int(length) {
						return badParam("daily counter range")
					}
					sums[i][dayIndex+j] += extendedDouble(v)
				}
			}
		}
		dayIndex += 288
	}
	times := make([]protocol.Value, int(length))
	for i := range times {
		times[i] = decimal(from + int64(i)*300000)
	}
	out := newMap(entry("time", list(times)))
	for i, field := range fields {
		v := make([]protocol.Value, int(length))
		for j, n := range sums[i] {
			v[j] = protocol.Value{Type: protocol.ValueDouble, Double: n}
		}
		out.Entries = append(out.Entries, entry(field.key, list(v)))
	}
	return st.ResponsePack(out)
}

func (s *Service) databaseStatusQuery(ctx context.Context, cmd string, p *protocol.MapPack, hashes []int32, st *transport.Stream) error {
	b, ok := s.backend.(StatusBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	if cmd == "CUBRID_ACTIVE_DB_LIST" || cmd == "DB_REALTIME_RESPONSE_TIME" {
		key := databaseDigestKey
		if cmd == "CUBRID_ACTIVE_DB_LIST" {
			var err error
			key, err = getText(p, "key")
			if err != nil {
				return err
			}
		}
		threshold := s.config.Clock().UnixMilli() - 300000
		cells := 0
		for _, h := range hashes {
			if err := ctx.Err(); err != nil {
				return err
			}
			status, err := b.Status(ctx, h, key)
			if err != nil {
				return err
			}
			if status == nil {
				continue
			}
			if cmd == "CUBRID_ACTIVE_DB_LIST" {
				if err := st.ResponsePack(status); err != nil {
					return err
				}
				continue
			}
			values, err := databaseList(status, "AVG_TIMER_WAIT")
			if err != nil {
				return err
			}
			cells += len(values)
			if cells > extendedQueryCells {
				return transport.ErrLimit
			}
			sum := 0.0
			count := int32(0)
			for i, v := range values {
				seen, err := databaseAt(status, "LAST_SEEN", i)
				if err != nil {
					return err
				}
				if databaseLong(seen) <= threshold {
					continue
				}
				n, err := databaseAt(status, "COUNT_STAR", i)
				if err != nil {
					return err
				}
				c := databaseInt(n)
				sum += databaseDouble(v) * float64(c)
				count += c
			}
			value := 0.0
			if count > 0 {
				value = sum / float64(count)
			}
			if err := st.ResponsePack(newMap(entry("objHash", decimal(int64(h))), entry("value", protocol.Value{Type: protocol.ValueDouble, Double: value}))); err != nil {
				return err
			}
		}
		return nil
	}
	date, err := s.date(p, false)
	if err != nil {
		return err
	}
	if date == "" {
		return badParam("date")
	}
	from, to := databaseLong(lookup(p, "stime")), databaseLong(lookup(p, "etime"))
	if cmd == "DB_LAST_DIGEST_TABLE" {
		to = databaseLong(lookup(p, "time"))
		if to < math.MinInt64+300000 {
			return badParam("time")
		}
		from = to - 300000
	}
	if to < from {
		return badParam("stime/etime")
	}
	if cmd == "DB_DIGEST_TABLE" && to == from {
		return badParam("nonzero digest period")
	}
	records, err := b.StatusHistory(ctx, date, from, to)
	if err != nil {
		return err
	}
	if len(records) > extendedQueryCells {
		return transport.ErrLimit
	}
	selected := map[int32]bool{}
	for _, h := range hashes {
		selected[h] = true
	}
	switch cmd {
	case "CUBRID_DB_REALTIME_STATUS", "CUBRID_DB_SERVER_INFO", "CUBRID_DB_LONG_TRANSACTION_DATA":
		key, err := getText(p, "key")
		if err != nil {
			return err
		}
		// The handler's nonlocal return is caught by StatusRD.catch Throwable,
		// stopping that read. The outer object loop repeats the same reverse scan.
		var match *protocol.StatusPack
		for i := len(records) - 1; i >= 0; i-- {
			if err := ctx.Err(); err != nil {
				return err
			}
			v := records[i]
			if v != nil && selected[v.ObjHash] && v.Key == key {
				match = v
				break
			}
		}
		if match != nil {
			for range hashes {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := st.ResponsePack(match); err != nil {
					return err
				}
			}
		}
		return nil
	case "DB_LAST_DIGEST_TABLE":
		for i := len(records) - 1; i >= 0; i-- {
			if err := ctx.Err(); err != nil {
				return err
			}
			v := records[i]
			if v != nil && selected[v.ObjHash] && v.Key == databaseDigestKey {
				if err := st.ResponsePack(v); err != nil {
					return err
				}
				delete(selected, v.ObjHash)
				if len(selected) == 0 {
					break
				}
			}
		}
		return nil
	case "DB_DIGEST_TABLE":
		return s.databaseDigestSummary(ctx, records, selected, from, to, st)
	case "DB_LOAD_DIGEST_COUNTER":
		return s.databaseDigestSeries(ctx, p, records, hashes, st)
	case "DB_MAX_TIMER_WAIT_THREAD":
		return s.databaseMaxTimer(ctx, p, records, selected, st)
	}
	return transport.ErrUnsupported
}

type databaseDigestID struct{ object, digest int32 }

func (s *Service) databaseDigestSummary(ctx context.Context, records []*protocol.StatusPack, selected map[int32]bool, from, to int64, st *transport.Stream) error {
	summaries := map[databaseDigestID]*protocol.MapPack{}
	order := []databaseDigestID{}
	last := map[int32]*protocol.StatusPack{}
	objects := []int32{}
	cells := 0
	for _, pk := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if pk == nil || !selected[pk.ObjHash] || pk.Key != databaseDigestKey {
			continue
		}
		digests, err := databaseList(pk, "DIGEST_TEXT")
		if err != nil {
			return err
		}
		cells += len(digests)
		if cells > extendedQueryCells {
			return transport.ErrLimit
		}
		for i, v := range digests {
			seen, err := databaseAt(pk, "LAST_SEEN", i)
			if err != nil {
				return err
			}
			when := databaseLong(seen)
			if when < from || when > to {
				continue
			}
			key := databaseDigestID{pk.ObjHash, databaseInt(v)}
			sum := summaries[key]
			if sum == nil {
				sum = newMap(entry("objHash", decimal(int64(pk.ObjHash))), entry("digestHash", decimal(int64(key.digest))), entry("MIN_TIMER_WAIT", decimal(math.MaxInt64)), entry("MAX_TIMER_WAIT", decimal(math.MinInt64)), entry("AVG_TIMER_WAIT", decimal(0)), entry("count", decimal(0)))
				summaries[key] = sum
				order = append(order, key)
			}
			for _, field := range []string{"MIN_TIMER_WAIT", "MAX_TIMER_WAIT", "AVG_TIMER_WAIT"} {
				v, err := databaseAt(pk, field, i)
				if err != nil {
					return err
				}
				n, old := databaseLong(v), lookup(sum, field).Int
				switch field {
				case "MIN_TIMER_WAIT":
					if old < n {
						n = old
					}
				case "MAX_TIMER_WAIT":
					if old > n {
						n = old
					}
				default:
					n += old
				}
				extendedSetEntry(&sum.Entries, field, decimal(n))
			}
			extendedSetEntry(&sum.Entries, "count", decimal(int64(int32(lookup(sum, "count").Int)+1)))
		}
		if last[pk.ObjHash] == nil {
			objects = append(objects, pk.ObjHash)
		}
		last[pk.ObjHash] = pk
		if err := st.ResponsePack(newMap(entry("percent", decimal(((pk.Time-from)*100)/(to-from))))); err != nil {
			return err
		}
	}
	for _, key := range order {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := st.ResponsePack(summaries[key]); err != nil {
			return err
		}
	}
	for _, h := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := st.ResponsePack(last[h]); err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) databaseDigestSeries(ctx context.Context, p *protocol.MapPack, records []*protocol.StatusPack, hashes []int32, st *transport.Stream) error {
	target := databaseInt(lookup(p, "digest"))
	column, err := getText(p, "column")
	if err != nil {
		return err
	}
	series := map[int32][]CounterPoint{}
	order := []int32{}
	for _, h := range hashes {
		if _, ok := series[h]; !ok {
			order = append(order, h)
			series[h] = []CounterPoint{}
		}
	}
	cells := 0
	for _, pk := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if pk == nil || pk.Key != databaseDigestKey {
			continue
		}
		points, ok := series[pk.ObjHash]
		if !ok {
			continue
		}
		digests, err := databaseList(pk, "DIGEST_TEXT")
		if err != nil {
			return err
		}
		cells += len(digests)
		if cells > extendedQueryCells {
			return transport.ErrLimit
		}
		for i, v := range digests {
			if databaseInt(v) == target {
				value, err := databaseAt(pk, column, i)
				if err != nil {
					return err
				}
				series[pk.ObjHash] = append(points, CounterPoint{Time: pk.Time, Value: value})
				break
			}
		}
	}
	for _, h := range order {
		if err := ctx.Err(); err != nil {
			return err
		}
		out := counterSeries(series[h])
		out.Entries = append([]protocol.Entry{entry("objHash", decimal(int64(h)))}, out.Entries...)
		if err := st.ResponsePack(out); err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) databaseMaxTimer(ctx context.Context, p *protocol.MapPack, records []*protocol.StatusPack, selected map[int32]bool, st *transport.Stream) error {
	target := databaseInt(lookup(p, "digest"))
	max := int64(0)
	out := newMap()
	cells := 0
	for _, pk := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if pk == nil || pk.Key != databaseCurrentKey || !selected[pk.ObjHash] {
			continue
		}
		if lookup(newMap(pk.Data.Entries...), "DIGEST_TEXT").Type == protocol.ValueNull {
			continue
		}
		digests, err := databaseList(pk, "DIGEST_TEXT")
		if err != nil {
			return err
		}
		cells += len(digests)
		if cells > extendedQueryCells {
			return transport.ErrLimit
		}
		for i, v := range digests {
			if databaseInt(v) != target {
				continue
			}
			timer, err := databaseAt(pk, "TIMER_WAIT", i)
			if err != nil {
				return err
			}
			n := databaseLong(timer)
			if n > max {
				thread, err := databaseAt(pk, "THREAD_ID", i)
				if err != nil {
					return err
				}
				lock, err := databaseAt(pk, "LOCK_TIME", i)
				if err != nil {
					return err
				}
				sql, err := databaseAt(pk, "SQL_TEXT", i)
				if err != nil {
					return err
				}
				max = n
				out = newMap(entry("time", decimal(pk.Time)), entry("thread_id", decimal(databaseLong(thread))), entry("timer", decimal(n)), entry("lock_time", decimal(databaseLong(lock))), entry("sql_text", decimal(int64(databaseInt(sql)))))
			}
			break
		}
	}
	return st.ResponsePack(out)
}

// RealtimeCounterRD.readBulk uses embedded objHash/time and preserves each
// selected counter value. Request order and duplicate hashes survive emission.
func (s *Service) databasePeriod(ctx context.Context, p *protocol.MapPack, hashes []int32, st *transport.Stream) error {
	b, ok := s.backend.(CounterBulkRowsBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	counter, err := getText(p, "counter")
	if err != nil {
		return err
	}
	from, to := databaseLong(lookup(p, "stime")), databaseLong(lookup(p, "etime"))
	if to < from {
		return badParam("stime/etime")
	}
	rows, err := bulkRowsFor(ctx, b, scoutertime.Date(from, s.config.Location), from, to, hashes)
	if err != nil {
		return err
	}
	if len(rows) > extendedQueryCells {
		return transport.ErrLimit
	}
	series := map[int32][]CounterPoint{}
	for _, h := range hashes {
		series[h] = []CounterPoint{}
	}
	for _, r := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.Values.Type == protocol.ValueNull {
			continue
		}
		if r.Values.Type != protocol.ValueMap {
			return badParam("bulk counter MapValue")
		}
		m := newMap(r.Values.Entries...)
		h := databaseInt(lookup(m, "objHash"))
		if points, ok := series[h]; ok {
			for _, e := range m.Entries {
				if e.Key == counter {
					series[h] = append(points, CounterPoint{Time: databaseLong(lookup(m, "time")), Value: e.Value})
					break
				}
			}
		}
	}
	cells := 0
	for _, h := range hashes {
		if err := ctx.Err(); err != nil {
			return err
		}
		cells += len(series[h])
		if cells > extendedQueryCells {
			return transport.ErrLimit
		}
		p := counterSeries(series[h])
		p.Entries = append([]protocol.Entry{entry("objHash", decimal(int64(h)))}, p.Entries...)
		if err := st.ResponsePack(p); err != nil {
			return err
		}
	}
	return nil
}
