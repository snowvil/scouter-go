package service

import (
	"fmt"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"math"
	"strconv"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

func entry(key string, value protocol.Value) protocol.Entry {
	return protocol.Entry{Key: key, Value: value}
}
func newMap(entries ...protocol.Entry) *protocol.MapPack { return &protocol.MapPack{Entries: entries} }
func decimal(n int64) protocol.Value                     { return protocol.Value{Type: protocol.ValueDecimal, Int: n} }
func stringValue(s string) protocol.Value                { return protocol.Value{Type: protocol.ValueText, Text: s} }
func boolean(b bool) protocol.Value                      { return protocol.Value{Type: protocol.ValueBoolean, Bool: b} }
func list(values []protocol.Value) protocol.Value {
	return protocol.Value{Type: protocol.ValueList, Values: values}
}
func lookup(p *protocol.MapPack, key string) protocol.Value {
	for _, e := range p.Entries {
		if e.Key == key {
			return e.Value
		}
	}
	return protocol.Value{Type: protocol.ValueNull}
}
func readMap(stream *transport.Stream) (*protocol.MapPack, error) {
	p, err := stream.ReadPack()
	if err != nil {
		return nil, err
	}
	m, ok := p.(*protocol.MapPack)
	if !ok {
		return nil, fmt.Errorf("%w: expected MapPack", transport.ErrMalformed)
	}
	return m, nil
}
func badParam(key string) error { return fmt.Errorf("%w: parameter %s", transport.ErrMalformed, key) }
func getText(p *protocol.MapPack, key string) (string, error) {
	v := lookup(p, key)
	if v.Type == protocol.ValueNull {
		return "", nil
	}
	if v.Type != protocol.ValueText {
		return "", badParam(key)
	}
	return v.Text, nil
}
func getNumber(p *protocol.MapPack, key string) (int64, error) {
	v := lookup(p, key)
	if v.Type == protocol.ValueNull {
		return 0, nil
	}
	if v.Type != protocol.ValueDecimal {
		return 0, badParam(key)
	}
	return v.Int, nil
}
func getInt(p *protocol.MapPack, key string) (int32, error) {
	n, err := getNumber(p, key)
	if err != nil {
		return 0, err
	}
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, badParam(key)
	}
	return int32(n), nil
}
func getBool(p *protocol.MapPack, key string) (bool, error) {
	v := lookup(p, key)
	if v.Type == protocol.ValueNull {
		return false, nil
	}
	if v.Type != protocol.ValueBoolean {
		return false, badParam(key)
	}
	return v.Bool, nil
}
func getList(p *protocol.MapPack, key string) ([]protocol.Value, error) {
	v := lookup(p, key)
	if v.Type == protocol.ValueNull {
		return nil, nil
	}
	if v.Type != protocol.ValueList {
		return nil, badParam(key)
	}
	return v.Values, nil
}
func getHashes(p *protocol.MapPack, key string) ([]int32, error) {
	values, err := getList(p, key)
	if err != nil {
		return nil, err
	}
	hashes := make([]int32, 0, len(values))
	for _, v := range values {
		if v.Type != protocol.ValueDecimal || v.Int < math.MinInt32 || v.Int > math.MaxInt32 {
			return nil, badParam(key)
		}
		hashes = append(hashes, int32(v.Int))
	}
	return hashes, nil
}
func (s *Service) date(p *protocol.MapPack, defaultToday bool) (string, error) {
	date, err := getText(p, "date")
	if err != nil {
		return "", err
	}
	if date == "" && defaultToday {
		date = scoutertime.Date(s.config.Clock().UnixMilli(), s.config.Location)
	}
	if date != "" {
		if _, err := scoutertime.ParseDate(date, s.config.Location); err != nil {
			return "", badParam("date")
		}
	}
	return date, nil
}
func hashKey(hash int32) string {
	n := int64(hash)
	if n < 0 {
		return "z" + strconv.FormatInt(-n, 32)
	}
	if n < 10 {
		return strconv.FormatInt(n, 10)
	}
	return "x" + strconv.FormatInt(n, 32)
}
