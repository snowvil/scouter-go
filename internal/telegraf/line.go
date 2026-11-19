package telegraf

import "strings"

// KV is one tag or field in the order it appeared on the line.
type KV struct{ Key, Value string }

// Line is the raw result of the InfluxSingleLine.of character scan before any
// measurement configuration is applied. Keys are unique; a repeated key keeps
// its first position and the last value (Java HashMap.put). Values are the
// unescaped text exactly as the Java scan produced it: the scan has no notion
// of quoted strings beyond dropping their characters, so a string field's
// value is empty.
type Line struct {
	Measurement string
	Tags        []KV
	Fields      []KV
	Timestamp   string
}

// Tag returns the tag value and whether it exists.
func (l *Line) Tag(key string) (string, bool) { return lookup(l.Tags, key) }

func lookup(kv []KV, key string) (string, bool) {
	for i := range kv {
		if kv[i].Key == key {
			return kv[i].Value, true
		}
	}
	return "", false
}

// kvSet is an ordered key/value list with Java HashMap.put semantics. Lookups
// scan while the list is small and switch to a map index after linearKeys
// entries, so a long line with many distinct keys stays linear.
type kvSet struct {
	list  []KV
	index map[string]int
}

const linearKeys = 16

func (s *kvSet) put(key, value string) {
	if s.index == nil {
		for i := range s.list {
			if s.list[i].Key == key {
				s.list[i].Value = value
				return
			}
		}
		if len(s.list) == linearKeys {
			s.index = make(map[string]int, 2*linearKeys)
			for i, kv := range s.list {
				s.index[kv.Key] = i
			}
		}
	} else if i, ok := s.index[key]; ok {
		s.list[i].Value = value
		return
	}
	if s.index != nil {
		s.index[key] = len(s.list)
	}
	s.list = append(s.list, KV{key, value})
}

// MaxLineBytes bounds a single line protocol line. Java has no limit; the
// value keeps a hostile request from building oversized keys.
const MaxLineBytes = 64 << 10

// ParseLine is the InfluxSingleLine.of scan, byte for byte. It never fails: a
// malformed line produces whatever the Java scan produces, and the later
// configuration lookup or number parsing decides whether it is dropped. The
// scan works on bytes; every delimiter is ASCII, so multi-byte UTF-8 passes
// through unchanged exactly as the Java char loop passes surrogate pairs.
//
// Known original behaviours preserved on purpose: a measurement without tags
// absorbs the rest of the line (no ' ' handling in mode 0); '=' inside a tag
// value is dropped; characters inside double quotes are dropped and a quoted
// backslash is ignored.
func ParseLine(s string) Line {
	var out Line
	var tags, fields kvSet
	var sink byte
	mode := 0
	var measurement, key, value strings.Builder
	keyMode := true
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch mode {
		case 0:
			if sink == '\\' {
				measurement.WriteByte(c)
				sink = 0
				continue
			}
			switch c {
			case '\\':
				sink = '\\'
			case ',':
				mode = 1
			default:
				measurement.WriteByte(c)
			}
		case 1:
			if sink == '\\' {
				if keyMode {
					key.WriteByte(c)
				} else {
					value.WriteByte(c)
				}
				sink = 0
				continue
			}
			switch c {
			case '\\':
				sink = '\\'
			case ' ':
				mode = 2
				if key.Len() > 0 {
					tags.put(key.String(), value.String())
				}
				key.Reset()
				value.Reset()
				keyMode = true
			case '=':
				keyMode = false
			case ',':
				keyMode = true
				tags.put(key.String(), value.String())
				key.Reset()
				value.Reset()
			default:
				if keyMode {
					key.WriteByte(c)
				} else {
					value.WriteByte(c)
				}
			}
		case 2:
			if sink == '\\' {
				if keyMode {
					key.WriteByte(c)
				} else {
					value.WriteByte(c)
				}
				sink = 0
				continue
			}
			switch c {
			case '\\':
				if sink != '"' {
					sink = '\\'
				}
			case '"':
				if sink == '"' {
					sink = 0
				} else {
					sink = '"'
				}
			case ' ':
				if sink != '"' {
					mode = 3
					if key.Len() > 0 {
						fields.put(key.String(), value.String())
					}
				}
			case '=':
				if sink != '"' {
					keyMode = false
				}
			case ',':
				if sink != '"' {
					keyMode = true
					fields.put(key.String(), value.String())
					key.Reset()
					value.Reset()
				}
			default:
				if sink != '"' {
					if keyMode {
						key.WriteByte(c)
					} else {
						value.WriteByte(c)
					}
				}
			}
		default:
			// Java appends every remaining char to the timestamp builder.
			out.Timestamp = s[i:]
			i = len(s)
		}
	}
	out.Measurement = measurement.String()
	out.Tags, out.Fields = tags.list, fields.list
	return out
}

// LineKey is InfluxSingleLine.toLineStringKey: measurement, tags and the first
// field key. The HTTP input keeps one line per key per request.
func LineKey(s string) string {
	var b strings.Builder
	b.Grow(min(len(s), 80))
	var sink byte
	mode := 0
	for i := 0; i < len(s) && mode < 2; i++ {
		c := s[i]
		if sink == '\\' {
			b.WriteByte(c)
			sink = 0
			continue
		}
		switch c {
		case '\\':
			sink = '\\'
		case ' ':
			if mode == 0 {
				b.WriteByte(' ')
			}
			mode++
		case '=', ',':
			if mode == 1 {
				mode++
			} else {
				b.WriteByte(c)
			}
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}
