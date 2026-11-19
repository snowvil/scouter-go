// KeyValueStoreService SET handlers: Copyright 2015 Scouter authors,
// Apache-2.0. Independent implementation of pinned source dee47fb5.
package service

import (
	"context"
	"math"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// LocalStoreWriter is optional. ttl is seconds and any negative ttl is
// permanent; now is epoch seconds. Implementations own the index/data files
// and report KeyValueStoreRW.set/setTTL's boolean, not a parse result.
type LocalStoreWriter interface {
	PutKeyValue(ctx context.Context, space, key, value string, ttl, now int64) (bool, error)
	SetKeyValueTTL(ctx context.Context, space, key string, ttl, now int64) (bool, error)
}

// javaTTLPermanent is ParamConstant.TTL_PERMANENT.
const javaTTLPermanent = int64(-1)

func LocalStoreMutationCommands() []string {
	return []string{"SET_GLOBAL_KV", "SET_GLOBAL_TTL", "SET_GLOBAL_KV_BULK", "SET_CUSTOM_KV", "SET_CUSTOM_TTL", "SET_CUSTOM_KV_BULK"}
}

func localStoreMutation(command string) bool {
	for _, cmd := range LocalStoreMutationCommands() {
		if command == cmd {
			return true
		}
	}
	return false
}

// javaText mirrors MapPack.getText: a Text entry yields its value, any
// absent or non-Text entry is null.
func javaText(p *protocol.MapPack, key string) (string, bool) {
	v := lookup(p, key)
	if v.Type != protocol.ValueText {
		return "", false
	}
	return v.Text, true
}

// javaLong mirrors Java's (long) primitive conversion used by
// FloatValue/DoubleValue.longValue: NaN is 0, out-of-range saturates.
func javaLong(f float64) int64 {
	switch {
	case math.IsNaN(f):
		return 0
	case f >= math.MaxInt64:
		return math.MaxInt64
	case f <= math.MinInt64:
		return math.MinInt64
	}
	return int64(f)
}

// javaLongDefault mirrors MapPack.getLongDefault: only NumberValue
// (Decimal/Float/Double) entries supply a value; anything else is the default.
func javaLongDefault(p *protocol.MapPack, key string, def int64) int64 {
	v := lookup(p, key)
	switch v.Type {
	case protocol.ValueDecimal:
		return v.Int
	case protocol.ValueFloat:
		return javaLong(float64(v.Float))
	case protocol.ValueDouble:
		return javaLong(v.Double)
	}
	return def
}

func (s *Service) localStoreMutation(ctx context.Context, command string, stream *transport.Stream) error {
	w, ok := s.backend.(LocalStoreWriter)
	if !ok {
		return transport.ErrUnsupported
	}
	p, err := readMap(stream)
	if err != nil {
		return err
	}
	now := s.config.Clock().Unix()
	ttl := javaLongDefault(p, "ttl", javaTTLPermanent)
	space := GlobalKeySpace
	if command == "SET_CUSTOM_KV" || command == "SET_CUSTOM_TTL" || command == "SET_CUSTOM_KV_BULK" {
		// A null/empty keySpace reaches KeyValueStoreRW, which answers false.
		space, _ = javaText(p, "keySpace")
	}
	switch command {
	case "SET_GLOBAL_KV", "SET_CUSTOM_KV":
		key, ok := javaText(p, "key")
		if !ok {
			return nil
		}
		// MapPack.getText yields null for a non-Text value; RW.set reports false.
		value, _ := javaText(p, "value")
		success, err := w.PutKeyValue(ctx, space, key, value, ttl, now)
		if err != nil {
			return err
		}
		return s.localBooleanResponse(ctx, success, stream)
	case "SET_GLOBAL_TTL", "SET_CUSTOM_TTL":
		key, ok := javaText(p, "key")
		if !ok {
			return nil
		}
		success, err := w.SetKeyValueTTL(ctx, space, key, ttl, now)
		if err != nil {
			return err
		}
		return s.localBooleanResponse(ctx, success, stream)
	case "SET_GLOBAL_KV_BULK":
		kv := lookup(p, "kv")
		if kv.Type == protocol.ValueNull || kv.Type == protocol.ValueMap && len(kv.Entries) == 0 {
			return nil
		}
		return s.localBulkPut(ctx, w, space, kv, ttl, now, stream)
	case "SET_CUSTOM_KV_BULK":
		if len(p.Entries) == 0 {
			return nil
		}
		// Original dereferences kv without a null check: an absent kv is a
		// NullPointerException, a non-Map kv a ClassCastException. Both are
		// explicit failures here, and an empty Map answers an empty MapPack.
		return s.localBulkPut(ctx, w, space, lookup(p, "kv"), ttl, now, stream)
	}
	return badParam("command")
}

func (s *Service) localBooleanResponse(ctx context.Context, success bool, stream *transport.Stream) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.ResponseValue(boolean(success))
}

// localBulkPut writes each kv entry in MapValue's Hashtable enumeration order
// and answers MapPack{key: Boolean} in that same order. The decoder already
// collapsed duplicate wire keys to their first position with the last value,
// which is Hashtable.put's count/chain behaviour.
func (s *Service) localBulkPut(ctx context.Context, w LocalStoreWriter, space string, kv protocol.Value, ttl, now int64, stream *transport.Stream) error {
	if kv.Type != protocol.ValueMap {
		return badParam("kv")
	}
	if len(kv.Entries) > extendedQueryCells {
		return transport.ErrLimit
	}
	result := newMap()
	for _, item := range localJavaMapEntries(kv.Entries) {
		if err := ctx.Err(); err != nil {
			return err
		}
		// MapValue.getText: a non-Text value is null and RW.set reports false.
		value := ""
		if item.Value.Type == protocol.ValueText {
			value = item.Value.Text
		}
		success, err := w.PutKeyValue(ctx, space, item.Key, value, ttl, now)
		if err != nil {
			return err
		}
		result.Entries = append(result.Entries, entry(item.Key, boolean(success)))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.ResponsePack(result)
}
