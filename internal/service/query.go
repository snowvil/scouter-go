package service

import (
	"context"
	"fmt"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

func (s *Service) query(ctx context.Context, command string, p *protocol.MapPack, stream *transport.Stream) error {
	switch command {
	case "VISITOR_REALTIME", "VISITOR_REALTIME_TOTAL", "VISITOR_REALTIME_GROUP", "VISITOR_LOADDATE", "VISITOR_LOADDATE_TOTAL", "VISITOR_LOADDATE_GROUP", "VISITOR_LOADHOUR_GROUP":
		return s.visitorQuery(ctx, command, p, stream)
	case "TAGCNT_DIV_NAMES", "TAGCNT_TAG_NAMES", "TAGCNT_TAG_VALUES", "TAGCNT_TAG_VALUE_DATA", "TAGCNT_TAG_ACTUAL_DATA":
		return s.tagQuery(ctx, command, p, stream)
	case "XLOG_READ_BY_GXID", "XLOG_LOAD_BY_TXIDS", "XLOG_LOAD_BY_GXID", "QUICKSEARCH_XLOG_LIST", "SEARCH_XLOG_LIST":
		return s.traceQuery(ctx, command, p, stream)
	case "CHECK_SESSION":
		key, err := getNumber(p, "session")
		if err != nil {
			return err
		}
		valid := int64(0)
		if s.validSession(key) {
			valid = 1
		}
		result := newMap(entry("session", decimal(key)), entry("validSession", decimal(valid)))
		if valid == 0 {
			result.Entries = append(result.Entries, entry("error", stringValue("login fail")))
		}
		return stream.ResponsePack(result)
	case "OBJECT_ENV":
		hash, err := getInt(p, "objHash")
		if err != nil {
			return err
		}
		backend, ok := s.backend.(AgentBackend)
		if !ok {
			return transport.ErrUnsupported
		}
		results, err := awaitAgent(ctx, stream, backend, hash, command, p)
		if err != nil {
			return err
		}
		for _, result := range results {
			if err := stream.ResponsePack(result); err != nil {
				return err
			}
		}
		return nil
	case "OBJECT_INFO":
		date, err := s.date(p, true)
		if err != nil {
			return err
		}
		hash, err := getInt(p, "objHash")
		if err != nil {
			return err
		}
		object, err := s.backend.Object(ctx, date, hash)
		if err != nil {
			return err
		}
		if object == nil {
			return nil
		}
		return stream.ResponsePack(object)
	case "OBJECT_LIST_LOAD_DATE":
		date, err := s.date(p, false)
		if err != nil {
			return err
		}
		if date == "" {
			return nil
		}
		objects, err := s.backend.Objects(ctx, date)
		if err != nil {
			return err
		}
		types, hashes, names := []protocol.Value{}, []protocol.Value{}, []protocol.Value{}
		for _, object := range objects {
			types = append(types, stringValue(object.ObjType))
			hashes = append(hashes, decimal(int64(object.ObjHash)))
			names = append(names, stringValue(object.ObjName))
		}
		return stream.ResponsePack(newMap(entry("objType", list(types)), entry("objHash", list(hashes)), entry("objName", list(names))))
	case "GET_TEXT", "GET_TEXT_100", "GET_TEXT_PACK", "GET_TEXT_ANY_TYPE":
		return s.textQuery(ctx, command, p, stream)
	case "COUNTER_REAL_TIME", "COUNTER_REAL_TIME_ALL", "COUNTER_REAL_TIME_MULTI", "COUNTER_PAST_TIME", "COUNTER_PAST_TIME_ALL", "COUNTER_TODAY", "COUNTER_PAST_DATE":
		return s.counterQuery(ctx, command, p, stream)
	case "TRANX_REAL_TIME_GROUP", "TRANX_REAL_TIME_GROUP_LATEST", "TRANX_LOAD_TIME_GROUP", "TRANX_LOAD_TIME_GROUP_V2", "XLOG_READ_BY_TXID", "TRANX_PROFILE", "TRANX_PROFILE_FULL":
		return s.xlogQuery(ctx, command, p, stream)
	default:
		return fmt.Errorf("%w: command %s", transport.ErrUnsupported, command)
	}
}

func (s *Service) textQuery(ctx context.Context, command string, p *protocol.MapPack, stream *transport.Stream) error {
	if lookup(p, "hash").Type == protocol.ValueNull {
		return nil
	}
	date, err := s.date(p, lookup(p, "date").Type == protocol.ValueNull)
	if err != nil {
		return err
	}
	hashes, err := getHashes(p, "hash")
	if err != nil {
		return err
	}
	var types []string
	if command == "GET_TEXT_ANY_TYPE" {
		values, err := getList(p, "type")
		if err != nil {
			return err
		}
		if len(values) != len(hashes) {
			return badParam("type/hash length")
		}
		for _, v := range values {
			if v.Type != protocol.ValueText {
				return badParam("type")
			}
			types = append(types, v.Text)
		}
	} else {
		xtype, err := getText(p, "type")
		if err != nil {
			return err
		}
		for range hashes {
			types = append(types, xtype)
		}
	}
	result := newMap()
	for i, hash := range hashes {
		value, found, err := s.backend.Text(ctx, date, types[i], hash)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if command == "GET_TEXT_PACK" || command == "GET_TEXT_ANY_TYPE" {
			if err := stream.ResponsePack(&protocol.TextPack{XType: types[i], Hash: hash, Text: value}); err != nil {
				return err
			}
			continue
		}
		key := hashKey(hash)
		// LinkedHashMap replacement preserves the first insertion's position.
		replaced := false
		for j := range result.Entries {
			if result.Entries[j].Key == key {
				result.Entries[j].Value = stringValue(value)
				replaced = true
				break
			}
		}
		if !replaced {
			result.Entries = append(result.Entries, entry(key, stringValue(value)))
		}
		if command == "GET_TEXT_100" && len(result.Entries) == 100 {
			if err := stream.ResponsePack(result); err != nil {
				return err
			}
			result = newMap()
		}
	}
	if command == "GET_TEXT" || command == "GET_TEXT_100" && len(result.Entries) > 0 {
		return stream.ResponsePack(result)
	}
	return nil
}
