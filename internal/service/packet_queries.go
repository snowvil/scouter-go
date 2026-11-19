package service

import (
	"context"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/transport"
	"math"
	"strings"
)

func PacketCommands() []string {
	return []string{"REALTIME_SERVICE_GROUP", "STATUS_AROUND_VALUE", "ALERT_REAL_TIME", "ALERT_LOAD_TIME", "ALERT_TITLE_COUNT", "GET_STACK_ANALYZER", "GET_STACK_INDEX", "BATCH_HISTORY_LIST", "BATCH_HISTORY_DETAIL", "BATCH_HISTORY_STACK", "OBJECT_BATCH_ACTIVE_LIST", "BATCH_ACTIVE_STACK", "INTR_COUNTER_REAL_TIME_BY_OBJ", "LOAD_SERVICE_SUMMARY", "LOAD_SQL_SUMMARY", "LOAD_APICALL_SUMMARY", "LOAD_IP_SUMMARY", "LOAD_UA_SUMMARY", "LOAD_SERVICE_ERROR_SUMMARY", "LOAD_ALERT_SUMMARY", "LOAD_ENDUSER_NAV_SUMMARY", "LOAD_ENDUSER_AJAX_SUMMARY", "LOAD_ENDUSER_ERROR_SUMMARY"}
}

type RealtimeGroupBackend interface {
	RealtimeServiceGroup(context.Context, []int32) (*protocol.MapPack, error)
}

func (s *Service) HandlePacketCommand(ctx context.Context, r transport.ClientRequest, stream *transport.Stream) (bool, error) {
	recognized := false
	for _, cmd := range PacketCommands() {
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
	p, err := readMap(stream)
	if err != nil {
		return true, err
	}
	return true, s.packetQuery(ctx, r.Command, p, stream)
}
func (s *Service) packetQuery(ctx context.Context, cmd string, p *protocol.MapPack, stream *transport.Stream) error {
	switch cmd {
	case "LOAD_SERVICE_SUMMARY", "LOAD_SQL_SUMMARY", "LOAD_APICALL_SUMMARY", "LOAD_IP_SUMMARY", "LOAD_UA_SUMMARY", "LOAD_SERVICE_ERROR_SUMMARY", "LOAD_ALERT_SUMMARY", "LOAD_ENDUSER_NAV_SUMMARY", "LOAD_ENDUSER_AJAX_SUMMARY", "LOAD_ENDUSER_ERROR_SUMMARY":
		return s.summaryQuery(ctx, cmd, p, stream)
	case "ALERT_TITLE_COUNT":
		return s.alertTitleCountQuery(ctx, p, stream)
	case "REALTIME_SERVICE_GROUP":
		b, ok := s.backend.(RealtimeGroupBackend)
		if !ok {
			return transport.ErrUnsupported
		}
		hashes, err := getHashes(p, "objHash")
		if err != nil {
			return err
		}
		pack, err := b.RealtimeServiceGroup(ctx, hashes)
		if err != nil {
			return err
		}
		if pack == nil {
			return nil
		}
		return stream.ResponsePack(pack)
	case "STATUS_AROUND_VALUE":
		b, ok := s.backend.(StatusBackend)
		if !ok {
			return transport.ErrUnsupported
		}
		hash, err := getInt(p, "objHash")
		if err != nil {
			return err
		}
		key, err := getText(p, "key")
		if err != nil {
			return err
		}
		when, err := getNumber(p, "time")
		if err != nil {
			return err
		}
		if when > math.MaxInt64-60000 {
			return badParam("time")
		}
		records, err := b.StatusHistory(ctx, scoutertime.Date(when, s.config.Location), when, when+60000)
		if err != nil {
			return err
		}
		if len(records) > extendedQueryCells {
			return transport.ErrLimit
		}
		for _, v := range records {
			if err := ctx.Err(); err != nil {
				return err
			}
			if v != nil && v.ObjHash == hash && v.Key == key {
				return stream.ResponsePack(v)
			}
		}
		return nil
	case "ALERT_REAL_TIME":
		b, ok := s.backend.(AlertBackend)
		if !ok {
			return transport.ErrUnsupported
		}
		typ, err := getText(p, "objType")
		if err != nil {
			return err
		}
		loop, err := getNumber(p, "loop")
		if err != nil {
			return err
		}
		index, err := getInt(p, "index")
		if err != nil {
			return err
		}
		first, err := getBool(p, "first")
		if err != nil {
			return err
		}
		result, err := b.AlertsRealtime(ctx, AlertQuery{ObjType: typ, Loop: loop, Index: index})
		if err != nil {
			return err
		}
		if len(result.Records) > extendedQueryCells {
			return transport.ErrLimit
		}
		if err := stream.ResponsePack(newMap(entry("loop", decimal(result.Loop)), entry("index", decimal(int64(result.Index))))); err != nil {
			return err
		}
		if first {
			return nil
		}
		for _, v := range result.Records {
			if err := ctx.Err(); err != nil {
				return err
			}
			if v != nil {
				if err := stream.ResponsePack(v); err != nil {
					return err
				}
			}
		}
		return nil
	case "ALERT_LOAD_TIME":
		return s.packetAlertHistory(ctx, p, stream)
	case "GET_STACK_ANALYZER", "GET_STACK_INDEX":
		b, ok := s.backend.(StackBackend)
		if !ok {
			return transport.ErrUnsupported
		}
		name, err := getText(p, "objName")
		if err != nil {
			return err
		}
		from, err := getNumber(p, "from")
		if err != nil {
			return err
		}
		to, err := getNumber(p, "to")
		if err != nil {
			return err
		}
		if from <= 0 || to <= from {
			date, err := getText(p, "date")
			if err != nil {
				return err
			}
			if lookup(p, "date").Type == protocol.ValueNull {
				return nil
			}
			midnight, err := scoutertime.ParseDate(date, s.config.Location)
			if err != nil {
				return badParam("date")
			}
			hour := extendedInt(lookup(p, "hour"))
			from = midnight + int64(hour*3600*1000)
			to = from + 3600000
		}
		records, err := b.Stacks(ctx, name, from, to)
		if err != nil {
			return err
		}
		if len(records) > extendedQueryCells {
			return transport.ErrLimit
		}
		for _, v := range records {
			if err := ctx.Err(); err != nil {
				return err
			}
			if v == nil {
				continue
			}
			if cmd == "GET_STACK_INDEX" {
				e := protocol.NewEncoder(protocol.Limits{})
				e.Int64(v.Time)
				data, err := e.Bytes()
				if err != nil {
					return err
				}
				if err := stream.ResponseRaw(data); err != nil {
					return err
				}
			} else if err := stream.ResponsePack(v); err != nil {
				return err
			}
		}
		return nil
	case "BATCH_HISTORY_LIST", "BATCH_HISTORY_DETAIL", "BATCH_HISTORY_STACK":
		return s.packetBatchHistory(ctx, cmd, p, stream)
	case "OBJECT_BATCH_ACTIVE_LIST", "BATCH_ACTIVE_STACK":
		return s.packetBatchRelay(ctx, cmd, p, stream)
	case "INTR_COUNTER_REAL_TIME_BY_OBJ":
		b, ok := s.backend.(InteractionBackend)
		if !ok {
			return transport.ErrUnsupported
		}
		typ, err := getText(p, "objType")
		if err != nil {
			return err
		}
		hashes, err := getHashes(p, "objHash")
		if err != nil {
			return err
		}
		if len(hashes) == 0 {
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
		if len(hashes) > extendedQueryCells {
			return transport.ErrLimit
		}
		count := 0
		for _, hash := range hashes {
			records, err := b.InteractionCounters(ctx, hash)
			if err != nil {
				return err
			}
			count += len(records)
			if count > extendedQueryCells {
				return transport.ErrLimit
			}
			for _, v := range records {
				if err := ctx.Err(); err != nil {
					return err
				}
				if v != nil {
					if err := stream.ResponsePack(v); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	return transport.ErrUnsupported
}
func (s *Service) packetRange(p *protocol.MapPack) (string, int64, int64, error) {
	date, err := s.date(p, false)
	if err != nil {
		return "", 0, 0, err
	}
	if date == "" {
		return "", 0, 0, badParam("date")
	}
	from, to, err := extendedTimeRange(p)
	return date, from, to, err
}
func (s *Service) packetAlertHistory(ctx context.Context, p *protocol.MapPack, stream *transport.Stream) error {
	b, ok := s.backend.(AlertBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	date, from, to, err := s.packetRange(p)
	if err != nil {
		return err
	}
	max, err := getInt(p, "count")
	if err != nil {
		return err
	}
	if max < 1 || max > 1000 {
		max = 500
	}
	level, err := getText(p, "level")
	if err != nil {
		return err
	}
	object, err := getText(p, "object")
	if err != nil {
		return err
	}
	key, err := getText(p, "key")
	if err != nil {
		return err
	}
	levelCode := byte(0)
	switch level {
	case "WARN":
		levelCode = 1
	case "ERROR":
		levelCode = 2
	case "FATAL":
		levelCode = 3
	}
	// Java AlertService streams AlertRD.readByTime and stops once count > max.
	names := map[int32]*protocol.ObjectPack{}
	count := int32(0)
	return b.ScanAlertHistory(ctx, date, from, to, func(v *protocol.AlertPack) (bool, error) {
		if count > max {
			return false, nil
		}
		if v == nil {
			return true, nil
		}
		if lookup(p, "level").Type != protocol.ValueNull && v.Level != levelCode {
			return true, nil
		}
		if lookup(p, "object").Type != protocol.ValueNull {
			o, known := names[v.ObjHash]
			if !known {
				var err error
				o, err = s.backend.Object(ctx, date, v.ObjHash)
				if err != nil {
					return false, err
				}
				names[v.ObjHash] = o
			}
			if o == nil || !strings.Contains(o.ObjName, object) {
				return true, nil
			}
		}
		if lookup(p, "key").Type != protocol.ValueNull && !strings.Contains(v.Title, key) && !strings.Contains(v.Message, key) {
			return true, nil
		}
		count++
		return count <= max, stream.ResponsePack(v)
	})
}
func (s *Service) packetBatchHistory(ctx context.Context, cmd string, p *protocol.MapPack, stream *transport.Stream) error {
	if cmd == "BATCH_HISTORY_STACK" {
		if _, ok := s.backend.(BatchArchiveBackend); !ok {
			return transport.ErrUnsupported
		}
	}
	hash, err := getInt(p, "objHash")
	if err != nil {
		return err
	}
	object, err := s.backend.Object(ctx, "", hash)
	if err != nil {
		return err
	}
	if object == nil {
		return nil
	}
	if cmd == "BATCH_HISTORY_STACK" {
		when, err := getNumber(p, "time")
		if err != nil {
			return err
		}
		name, err := getText(p, "filename")
		if err != nil {
			return err
		}
		chunks, err := s.backend.(BatchArchiveBackend).BatchStack(ctx, object.ObjName, when, name)
		if err != nil {
			return err
		}
		if len(chunks) > extendedQueryCells {
			return transport.ErrLimit
		}
		for _, data := range chunks {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := stream.ResponseBlob(data); err != nil {
				return err
			}
		}
		return nil
	}
	b, ok := s.backend.(BatchBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	if cmd == "BATCH_HISTORY_DETAIL" {
		start, err := getNumber(p, "startTime")
		if err != nil {
			return err
		}
		elapsed, err := getNumber(p, "elapsedTime")
		if err != nil {
			return err
		}
		position, err := getNumber(p, "position")
		if err != nil {
			return err
		}
		v, err := b.BatchAt(ctx, object.ObjName, start+elapsed, position)
		if err != nil {
			return err
		}
		if v == nil {
			return nil
		}
		return stream.ResponsePack(v)
	}
	from, err := getNumber(p, "from")
	if err != nil {
		return err
	}
	to, err := getNumber(p, "to")
	if err != nil {
		return err
	}
	if from <= 0 || to <= from {
		return nil
	}
	filter, err := getText(p, "filter")
	if err != nil {
		return err
	}
	min, err := getNumber(p, "response")
	if err != nil {
		return err
	}
	records, err := b.Batches(ctx, object.ObjName, from, to)
	if err != nil {
		return err
	}
	if len(records) > extendedQueryCells {
		return transport.ErrLimit
	}
	for _, v := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if v == nil || lookup(p, "filter").Type != protocol.ValueNull && !strings.Contains(v.BatchJobID, filter) || min > 0 && v.ElapsedTime < min {
			continue
		}
		data, err := protocol.EncodeBatchSimple(v, protocol.Limits{})
		if err != nil {
			return err
		}
		if err := stream.ResponseRaw(data); err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) packetBatchRelay(ctx context.Context, cmd string, p *protocol.MapPack, stream *transport.Stream) error {
	b, ok := s.backend.(AgentBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	hash, err := getInt(p, "objHash")
	if err != nil {
		return err
	}
	hashes := []int32{hash}
	if cmd == "OBJECT_BATCH_ACTIVE_LIST" && hash == 0 {
		if lookup(p, "objType").Type == protocol.ValueNull {
			return nil
		}
		typ, err := getText(p, "objType")
		if err != nil {
			return err
		}
		objects, err := s.extendedObjects(ctx, "", typ, true)
		if err != nil {
			return err
		}
		hashes = nil
		for _, o := range objects {
			hashes = append(hashes, o.ObjHash)
		}
	}
	for _, hash := range hashes {
		records, err := awaitAgent(ctx, stream, b, hash, cmd, p)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			if cmd == "OBJECT_BATCH_ACTIVE_LIST" {
				if err := stream.ResponsePack(newMap(entry("objHash", decimal(int64(hash))))); err != nil {
					return err
				}
			}
			continue
		}
		if len(records) != 1 {
			return badParam("single Agent MapPack response")
		}
		v, ok := records[0].(*protocol.MapPack)
		if !ok {
			return badParam("Agent MapPack response")
		}
		copy := &protocol.MapPack{Entries: append([]protocol.Entry{}, v.Entries...)}
		if cmd == "OBJECT_BATCH_ACTIVE_LIST" {
			extendedSetEntry(&copy.Entries, "objHash", decimal(int64(hash)))
		}
		if err := stream.ResponsePack(copy); err != nil {
			return err
		}
	}
	return nil
}
