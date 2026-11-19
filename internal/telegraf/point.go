package telegraf

import (
	"errors"
	"hash/crc32"
	"strconv"
	"strings"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Point is a line accepted by its measurement configuration
// (InfluxSingleLine after of()). It references, but does not own, the
// Measurement and CounterProtocol values of the Config it was resolved with.
type Point struct {
	Measurement *Measurement
	Host        string
	Family      string
	ObjType     string
	ObjTypeIcon string
	ObjName     string
	ObjHash     int32
	// ReceivedTime is the request receive time in ms; TimestampMs is the line
	// timestamp (ns) / 1e6, truncated toward zero.
	ReceivedTime, TimestampMs int64
	Tags                      map[string]string
	// Fields are the mapped numeric fields in line order: DecimalValue for an
	// "i" suffix, FloatValue for a trailing digit.
	Fields []NumberField
}

// NumberField pairs a counter mapping with its parsed value.
type NumberField struct {
	Counter *CounterProtocol
	Value   protocol.Value
}

// DropReason classifies why a line produced no Point.
type DropReason int

const (
	Accepted           DropReason = iota
	UnknownMeasurement            // no configuration for the measurement
	Disabled                      // measurement enabled=false
	InvalidConfig                 // objFamilyBase empty
	Filtered                      // tag filter did not match
	ParseError                    // timestamp or mapped field value unparsable
)

var errField = errors.New("telegraf: unparsable mapped field")

// Hash is scouter.util.HashUtil.hash(String): CRC32 of the UTF-8 bytes as int.
// The JVM uses its default charset; UTF-8 is assumed.
func Hash(s string) int32 { return int32(crc32.ChecksumIEEE([]byte(s))) }

// Resolve is InfluxSingleLine.of after the character scan. The checks run in
// the Java order: configuration lookup, enabled, valid, tag filter, then the
// timestamp and mapped field values (any parse failure drops the whole line).
func Resolve(line *Line, cfg *Config, receivedTime int64) (*Point, DropReason) {
	m := cfg.Measurements[line.Measurement]
	if m == nil {
		return nil, UnknownMeasurement
	}
	if !m.Enabled {
		return nil, Disabled
	}
	if !m.Valid() {
		return nil, InvalidConfig
	}
	tags := make(map[string]string, len(line.Tags))
	for _, kv := range line.Tags {
		tags[kv.Key] = kv.Value
	}
	if !m.TagFilterMatching(tags) {
		return nil, Filtered
	}
	ns, err := strconv.ParseInt(line.Timestamp, 10, 64)
	if err != nil {
		return nil, ParseError
	}
	p := &Point{Measurement: m, Tags: tags, ReceivedTime: receivedTime, TimestampMs: ns / 1000000}
	p.Host = m.Host(tags)
	p.Family = m.Family(tags)
	p.ObjType = m.ObjType(tags)
	p.ObjTypeIcon = m.ObjTypeIcon
	p.ObjName = m.ObjName(p.Host, tags)
	p.ObjHash = Hash(p.ObjName)
	for _, f := range line.Fields {
		cp := m.Counters[f.Key]
		if cp == nil {
			continue
		}
		v, ok, err := parseNumber(f.Value)
		if err != nil {
			return nil, ParseError
		}
		if ok {
			p.Fields = append(p.Fields, NumberField{Counter: cp, Value: v})
		}
	}
	return p, Accepted
}

// parseNumber is addNumField: "...i" is Long.parseLong, a trailing digit is
// Float.parseFloat, anything else (bool, unsigned "u", string) is skipped.
// An empty value is the Java charAt(-1) failure.
func parseNumber(s string) (protocol.Value, bool, error) {
	if s == "" {
		return protocol.Value{}, false, errField
	}
	last := s[len(s)-1]
	switch {
	case last == 'i':
		n, err := strconv.ParseInt(s[:len(s)-1], 10, 64)
		if err != nil {
			return protocol.Value{}, false, errField
		}
		return protocol.Value{Type: protocol.ValueDecimal, Int: n}, true, nil
	case last >= '0' && last <= '9':
		f, err := parseJavaFloat(s)
		if err != nil {
			return protocol.Value{}, false, errField
		}
		return protocol.Value{Type: protocol.ValueFloat, Float: f}, true, nil
	}
	return protocol.Value{}, false, nil
}

// parseJavaFloat approximates Float.parseFloat for values ending in a digit:
// surrounding ASCII control/space is trimmed, decimal and hexadecimal (p
// exponent) forms are accepted, overflow becomes ±Inf and underflow 0.
// Underscores, which Go accepts in some forms, are rejected like Java.
func parseJavaFloat(s string) (float32, error) {
	s = strings.TrimFunc(s, func(r rune) bool { return r <= ' ' })
	if s == "" || strings.ContainsRune(s, '_') {
		return 0, errField
	}
	f, err := strconv.ParseFloat(s, 32)
	if err != nil {
		var numErr *strconv.NumError
		if errors.As(err, &numErr) && errors.Is(numErr.Err, strconv.ErrRange) {
			return float32(f), nil
		}
		return 0, err
	}
	return float32(f), nil
}
