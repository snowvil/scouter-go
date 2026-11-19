package service

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

func (s *Service) specialCounterQuery(ctx context.Context, cmd string, p *protocol.MapPack, stream *transport.Stream) error {
	typ, err := getText(p, "objType")
	if err != nil {
		return err
	}
	if cmd == "COUNTER_MAP_REAL_TIME" {
		name, err := getText(p, "counter")
		if err != nil {
			return err
		}
		hashes, err := getHashes(p, "objHash")
		if err != nil {
			return err
		}
		for _, hash := range hashes {
			v, found, err := s.backend.Counter(ctx, hash, name, 1)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("%w: pinned COUNTER_MAP_REAL_TIME missing cache value", transport.ErrMalformed)
			}
			if v.Type == protocol.ValueMap {
				if err := stream.ResponseValue(v); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if cmd == "SHOW_REAL_TIME_STRING" {
		name, err := getText(p, "counter")
		if err != nil {
			return err
		}
		catalog, ok := s.backend.(CounterCatalogBackend)
		if !ok {
			return transport.ErrUnsupported
		}
		objects, err := s.extendedObjects(ctx, "", typ, true)
		if err != nil {
			return err
		}
		var sum protocol.Value
		foundFirst := false
		for _, o := range objects {
			v, found, err := s.backend.Counter(ctx, o.ObjHash, name, 1)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			if v.Type != protocol.ValueDecimal && v.Type != protocol.ValueFloat && v.Type != protocol.ValueDouble {
				return badParam("numeric counter value")
			}
			if !foundFirst {
				sum = v
				foundFirst = true
				continue
			}
			switch sum.Type {
			case protocol.ValueDecimal:
				sum.Int += extendedLong(v)
			case protocol.ValueFloat:
				sum.Float += extendedFloat(v)
			case protocol.ValueDouble:
				return fmt.Errorf("%w: pinned NumberValue.add DoubleValue cast failure", transport.ErrMalformed)
			}
		}
		if !foundFirst {
			sum = decimal(0)
		}
		meta, err := catalog.CounterType(ctx, typ)
		if err != nil {
			return err
		}
		return stream.ResponsePack(newMap(entry("desc", stringValue(name+" of "+meta.DisplayName)), entry("result", sum)))
	}
	hashes := []int32{}
	aggregate := cmd == "ACTIVESPEED_REAL_TIME_GROUP" || cmd == "ACTIVESPEED_GROUP_REAL_TIME_GROUP"
	if cmd == "ACTIVESPEED_REAL_TIME" || cmd == "ACTIVESPEED_REAL_TIME_GROUP" {
		objects, err := s.extendedObjects(ctx, "", typ, aggregate)
		if err != nil {
			return err
		}
		for _, o := range objects {
			hashes = append(hashes, o.ObjHash)
		}
	} else {
		hashes, err = getHashes(p, "objHash")
		if err != nil {
			return err
		}
	}
	var total [3]int32
	var tps float32
	for _, hash := range hashes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if cmd == "ACTIVESPEED_GROUP_REAL_TIME_GROUP" {
			o, err := s.backend.Object(ctx, "", hash)
			if err != nil {
				return err
			}
			if o == nil || !o.Alive {
				continue
			}
		}
		speed := [3]int32{}
		if aggregate {
			v, found, err := s.backend.Counter(ctx, hash, "TPS", 1)
			if err != nil {
				return err
			}
			if found {
				tps += extendedFloat(v)
			}
		}
		v, found, err := s.backend.Counter(ctx, hash, "ActiveSpeed", 1)
		if err != nil {
			return err
		}
		if found {
			if v.Type != protocol.ValueList {
				return badParam("ActiveSpeed ListValue")
			}
			if len(v.Values) >= 3 {
				for i := 0; i < 3; i++ {
					speed[i] = extendedInt(v.Values[i])
				}
			}
		}
		if aggregate {
			for i := 0; i < 3; i++ {
				total[i] += speed[i]
			}
		} else {
			if err := stream.ResponsePack(newMap(entry("objHash", decimal(int64(hash))), entry("act1", decimal(int64(speed[0]))), entry("act2", decimal(int64(speed[1]))), entry("act3", decimal(int64(speed[2]))))); err != nil {
				return err
			}
		}
	}
	if aggregate {
		return stream.ResponsePack(newMap(entry("act1", decimal(int64(total[0]))), entry("act2", decimal(int64(total[1]))), entry("act3", decimal(int64(total[2]))), entry("tps", protocol.Value{Type: protocol.ValueFloat, Float: tps})))
	}
	return nil
}

func extendedFloat(v protocol.Value) float32 {
	switch v.Type {
	case protocol.ValueDecimal:
		return float32(v.Int)
	case protocol.ValueFloat:
		return v.Float
	case protocol.ValueDouble:
		return float32(v.Double)
	case protocol.ValueText:
		n, _ := strconv.ParseFloat(v.Text, 32)
		return float32(n)
	}
	return 0
}
func extendedInt(v protocol.Value) int32 {
	if v.Type == protocol.ValueDecimal {
		return int32(v.Int)
	}
	n := extendedDouble(v)
	if math.IsNaN(n) {
		return 0
	}
	if n >= math.MaxInt32 {
		return math.MaxInt32
	}
	if n <= math.MinInt32 {
		return math.MinInt32
	}
	return int32(n)
}
