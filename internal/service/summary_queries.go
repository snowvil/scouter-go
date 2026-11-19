package service

import (
	"context"
	"fmt"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/transport"
	"hash/crc32"
	"sort"
	"strconv"
	"unicode/utf16"
)

type summarySchema struct {
	kind                                  byte
	columns, first, sum32, sum64, nonzero []string
	max                                   int
	endUser                               bool
}

var navigationFields = []string{"navigationStart", "unloadEventStart", "unloadEventEnd", "fetchStart", "domainLookupStart", "domainLookupEnd", "connectStart", "connectEnd", "requestStart", "responseStart", "responseEnd", "domLoading", "domInteractive", "domContentLoadedEventStart", "domContentLoadedEventEnd", "domComplete", "loadEventStart", "loadEventEnd"}

func summaryCommandSchema(cmd string) (summarySchema, bool) {
	schema := summarySchema{max: 50000}
	switch cmd {
	case "LOAD_SERVICE_SUMMARY", "LOAD_SQL_SUMMARY", "LOAD_APICALL_SUMMARY":
		schema.kind = 1
		if cmd == "LOAD_SQL_SUMMARY" {
			schema.kind = 2
		}
		if cmd == "LOAD_APICALL_SUMMARY" {
			schema.kind = 5
		}
		schema.columns = []string{"id", "count", "error", "elapsed"}
		schema.sum32 = []string{"count", "error"}
		schema.sum64 = []string{"elapsed"}
		if schema.kind == 1 {
			schema.columns = append(schema.columns, "cpu", "mem")
		}
	case "LOAD_IP_SUMMARY", "LOAD_UA_SUMMARY":
		schema.kind = 4
		if cmd == "LOAD_UA_SUMMARY" {
			schema.kind = 8
		}
		schema.columns = []string{"id", "count"}
		schema.sum32 = []string{"count"}
	case "LOAD_SERVICE_ERROR_SUMMARY":
		schema.kind = 9
		schema.max = 10000
		schema.columns = []string{"id", "error", "service", "message", "count", "txid", "sql", "apicall", "fullstack"}
		schema.first = []string{"error", "service", "txid"}
		schema.sum32 = []string{"count"}
		schema.nonzero = []string{"message", "sql", "apicall", "fullstack"}
	case "LOAD_ALERT_SUMMARY":
		schema.kind = 3
		schema.columns = []string{"id", "title", "count", "level"}
		schema.first = []string{"level"}
		schema.sum32 = []string{"count"}
	case "LOAD_ENDUSER_AJAX_SUMMARY":
		schema.kind = 11
		schema.endUser = true
		schema.columns = []string{"id", "uri", "ip", "count", "duration", "userAgent"}
		schema.first = []string{"uri", "ip", "userAgent"}
		schema.sum32 = []string{"count"}
		schema.sum64 = []string{"duration"}
	case "LOAD_ENDUSER_NAV_SUMMARY":
		schema.kind = 10
		schema.endUser = true
		schema.columns = append([]string{"id", "uri", "ip", "count"}, navigationFields...)
		schema.first = []string{"uri", "ip"}
		schema.sum32 = []string{"count"}
		schema.sum64 = navigationFields
	case "LOAD_ENDUSER_ERROR_SUMMARY":
		schema.kind = 12
		schema.endUser = true
		schema.columns = []string{"id", "host", "stacktrace", "userAgent", "uri", "message", "file", "lineNumber", "columnNumber"}
		schema.first = []string{"host", "stacktrace", "userAgent", "uri", "message", "file", "lineNumber", "columnNumber"}
		schema.sum32 = []string{"count"}
	default:
		return summarySchema{}, false
	}
	return schema, true
}

type summaryAggregate struct {
	id     int64
	title  string
	fields map[string]int64
}
type summaryGroup struct {
	key       string
	aggregate *summaryAggregate
}

func summaryList(table protocol.Value, name string) ([]protocol.Value, error) {
	v := lookup(&protocol.MapPack{Entries: table.Entries}, name)
	if v.Type != protocol.ValueList {
		return nil, badParam("summary " + name + " ListValue")
	}
	return v.Values, nil
}
func summaryCell(table protocol.Value, name string, i int) (protocol.Value, error) {
	values, err := summaryList(table, name)
	if err != nil {
		return protocol.Value{}, err
	}
	if i >= len(values) {
		return protocol.Value{}, badParam("summary parallel list length")
	}
	return values[i], nil
}
func (s *Service) summaryQuery(ctx context.Context, cmd string, p *protocol.MapPack, stream *transport.Stream) error {
	schema, ok := summaryCommandSchema(cmd)
	if !ok {
		return transport.ErrUnsupported
	}
	backend, ok := s.backend.(SummaryBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	date, from, to, err := s.packetRange(p)
	if err != nil {
		return err
	}
	typ, err := getText(p, "objType")
	if err != nil {
		return err
	}
	hash, err := getInt(p, "objHash")
	if err != nil {
		return err
	}
	// Java SummaryService aggregates SummaryRD.readByTime as it reads. The
	// aggregate is bounded by schema.max groups; the scan has no total cap.
	groups := map[string]*summaryGroup{}
	ordered := []*summaryGroup{}
	head := 0
	err = backend.ScanSummaries(ctx, date, from, to, schema.kind, func(pack *protocol.SummaryPack) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if pack == nil || pack.SType != schema.kind {
			return true, nil
		}
		anyType := lookup(p, "objType").Type == protocol.ValueNull || schema.kind == 1 || schema.kind == 2 || schema.kind == 5
		if !anyType || typ != "" && lookup(p, "objType").Type != protocol.ValueNull {
			if pack.ObjType != typ {
				return true, nil
			}
		}
		if !schema.endUser && hash != 0 && pack.ObjHash != hash {
			return true, nil
		}
		if pack.Table.Type != protocol.ValueMap {
			return false, badParam("summary Table MapValue")
		}
		idField := "id"
		if schema.kind == 3 {
			idField = "title"
		}
		ids, err := summaryList(pack.Table, idField)
		if err != nil {
			return false, err
		}
		for i, idValue := range ids {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			key, actualKey := strconv.FormatInt(int64(extendedInt(idValue)), 10), ""
			id := int64(extendedInt(idValue))
			title := ""
			if schema.kind == 3 {
				if idValue.Type != protocol.ValueText {
					return false, badParam("summary title")
				}
				title = idValue.Text
				key = title
				id = int64(int32(crc32.ChecksumIEEE([]byte(title))))
			} else if schema.endUser {
				id = extendedLong(idValue)
			}
			actualKey = key
			if schema.endUser {
				actualKey = strconv.FormatInt(id, 10)
			}
			group := groups[key]
			fresh := group == nil
			if fresh {
				agg := &summaryAggregate{id: id, title: title, fields: map[string]int64{}}
				for _, name := range schema.first {
					field := name
					if schema.kind == 12 && name == "host" {
						field = "stacktrace"
					}
					v, err := summaryCell(pack.Table, field, i)
					if err != nil {
						return false, err
					}
					if name == "txid" {
						agg.fields[name] = extendedLong(v)
					} else {
						agg.fields[name] = int64(extendedInt(v))
						if schema.kind == 3 && name == "level" {
							agg.fields[name] = int64(int8(extendedInt(v)))
						}
					}
				}
				if existing := groups[actualKey]; existing != nil {
					existing.aggregate = agg
					group = existing
				} else {
					if len(groups) >= schema.max {
						for head < len(ordered) {
							old := ordered[head]
							head++
							if groups[old.key] == old {
								delete(groups, old.key)
								break
							}
						}
					}
					group = &summaryGroup{key: actualKey, aggregate: agg}
					groups[actualKey] = group
					if head > len(ordered)/2 {
						// Drop the evicted prefix so ordered stays within
						// 2*schema.max on an unbounded scan; order is kept.
						ordered = append(ordered[:0], ordered[head:]...)
						head = 0
					}
					ordered = append(ordered, group)
				}
			}
			agg := group.aggregate
			for _, name := range schema.sum32 {
				v, err := summaryCell(pack.Table, name, i)
				if err != nil {
					return false, err
				}
				agg.fields[name] = int64(int32(agg.fields[name]) + extendedInt(v))
			}
			for _, name := range schema.sum64 {
				v, err := summaryCell(pack.Table, name, i)
				if err != nil {
					return false, err
				}
				agg.fields[name] += extendedLong(v)
			}
			if schema.kind == 1 || schema.kind == 2 || schema.kind == 5 {
				cpu := lookup(&protocol.MapPack{Entries: pack.Table.Entries}, "cpu")
				mem := lookup(&protocol.MapPack{Entries: pack.Table.Entries}, "mem")
				if cpu.Type != protocol.ValueNull && mem.Type != protocol.ValueNull {
					for _, name := range []string{"cpu", "mem"} {
						v, err := summaryCell(pack.Table, name, i)
						if err != nil {
							return false, err
						}
						agg.fields[name] += extendedLong(v)
					}
				}
			}
			for _, name := range schema.nonzero {
				if agg.fields[name] == 0 {
					v, err := summaryCell(pack.Table, name, i)
					if err != nil {
						return false, err
					}
					agg.fields[name] = int64(extendedInt(v))
				}
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	result := newMap()
	for _, name := range schema.columns {
		result.Entries = append(result.Entries, entry(name, list([]protocol.Value{})))
	}
	for _, group := range ordered {
		if groups[group.key] != group {
			continue
		}
		agg := group.aggregate
		for i, name := range schema.columns {
			v := decimal(agg.fields[name])
			if name == "id" {
				v = decimal(agg.id)
			}
			if name == "title" {
				v = stringValue(agg.title)
			}
			result.Entries[i].Value.Values = append(result.Entries[i].Value.Values, v)
		}
	}
	return stream.ResponsePack(result)
}
func (s *Service) alertTitleCountQuery(ctx context.Context, p *protocol.MapPack, stream *transport.Stream) error {
	backend, ok := s.backend.(SummaryBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	date, from, to, err := s.packetRange(p)
	if err != nil {
		return err
	}
	// Java AlertSummary streams SummaryRD.readByTime. Go bounds only what it
	// keeps: distinct (title, minute) cells, not the rows it reads.
	groups := map[string]*protocol.MapPack{}
	titles := []string{}
	cells := 0
	err = backend.ScanSummaries(ctx, date, from, to, 3, func(pack *protocol.SummaryPack) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if pack == nil || pack.SType != 3 {
			return true, nil
		}
		values, err := summaryList(pack.Table, "title")
		if err != nil {
			return false, err
		}
		minute := scoutertime.Millis(pack.Time, s.config.Location) / 60000
		hhmm := fmt.Sprintf("%02d%02d", minute/60, minute%60)
		for i, v := range values {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if v.Type != protocol.ValueText {
				return false, badParam("summary title")
			}
			level, err := summaryCell(pack.Table, "level", i)
			if err != nil {
				return false, err
			}
			count, err := summaryCell(pack.Table, "count", i)
			if err != nil {
				return false, err
			}
			group := groups[v.Text]
			if group == nil {
				group = newMap(entry("title", v), entry("level", decimal(int64(int8(extendedLong(level))))), entry("count", protocol.Value{Type: protocol.ValueMap}))
				groups[v.Text] = group
				titles = append(titles, v.Text)
			}
			mv := &group.Entries[2].Value
			before := len(mv.Entries)
			extendedSetEntry(&mv.Entries, hhmm, decimal(int64(extendedInt(count))))
			if len(mv.Entries) > before {
				cells++
			}
			if cells > extendedQueryCells {
				return false, transport.ErrLimit
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	capacity := 16
	for len(titles) > capacity*3/4 {
		capacity *= 2
	}
	hash := func(s string) uint32 {
		var h uint32
		for _, c := range utf16.Encode([]rune(s)) {
			h = h*31 + uint32(c)
		}
		return h ^ (h >> 16)
	}
	sort.SliceStable(titles, func(i, j int) bool { return hash(titles[i])&uint32(capacity-1) < hash(titles[j])&uint32(capacity-1) })
	for _, title := range titles {
		if err := stream.ResponsePack(groups[title]); err != nil {
			return err
		}
	}
	return nil
}
