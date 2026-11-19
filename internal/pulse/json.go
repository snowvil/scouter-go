package pulse

import (
	"encoding/base64"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

// jsonWriter emits bytes the way Jackson 2.15.2 does for the plugin's
// ObjectWriter (observed in the Java golden): no whitespace, '"' and '\\'
// escaped, control characters below 0x20 as \b \t \n \f \r or \u00XX with
// uppercase hex, supplementary-plane characters as a \uXXXX surrogate pair with
// uppercase hex, and everything else ('/', '<', '>', 0x7F, BMP non-ASCII) as
// raw UTF-8.
type jsonWriter struct {
	buf   []byte
	first bool
}

func newJSONWriter() *jsonWriter { return &jsonWriter{buf: make([]byte, 0, 512)} }

func (w *jsonWriter) beginObject() { w.buf = append(w.buf, '{'); w.first = true }
func (w *jsonWriter) endObject()   { w.buf = append(w.buf, '}'); w.first = false }
func (w *jsonWriter) beginArray()  { w.buf = append(w.buf, '['); w.first = true }
func (w *jsonWriter) endArray()    { w.buf = append(w.buf, ']'); w.first = false }

func (w *jsonWriter) separator() {
	if w.first {
		w.first = false
		return
	}
	w.buf = append(w.buf, ',')
}

// key writes the separator and name; the following value writer emits no comma.
func (w *jsonWriter) key(name string) {
	w.separator()
	w.writeString(name)
	w.buf = append(w.buf, ':')
	w.first = true
}
func (w *jsonWriter) element() { w.separator() }

const hexUpper = "0123456789ABCDEF"

func (w *jsonWriter) writeString(s string) {
	w.buf = append(w.buf, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c >= utf8.RuneSelf {
			r, size := utf8.DecodeRuneInString(s[i:])
			if r >= 0x10000 {
				hi, lo := utf16.EncodeRune(r)
				w.unicodeEscape(uint16(hi))
				w.unicodeEscape(uint16(lo))
			} else {
				w.buf = append(w.buf, s[i:i+size]...)
			}
			i += size
			continue
		}
		i++
		switch {
		case c == '"':
			w.buf = append(w.buf, '\\', '"')
		case c == '\\':
			w.buf = append(w.buf, '\\', '\\')
		case c >= 0x20:
			w.buf = append(w.buf, c)
		case c == '\b':
			w.buf = append(w.buf, '\\', 'b')
		case c == '\t':
			w.buf = append(w.buf, '\\', 't')
		case c == '\n':
			w.buf = append(w.buf, '\\', 'n')
		case c == '\f':
			w.buf = append(w.buf, '\\', 'f')
		case c == '\r':
			w.buf = append(w.buf, '\\', 'r')
		default:
			w.unicodeEscape(uint16(c))
		}
	}
	w.buf = append(w.buf, '"')
}

func (w *jsonWriter) unicodeEscape(u uint16) {
	w.buf = append(w.buf, '\\', 'u', hexUpper[u>>12], hexUpper[(u>>8)&0xF], hexUpper[(u>>4)&0xF], hexUpper[u&0xF])
}

func (w *jsonWriter) stringValue(s string)       { w.element(); w.writeString(s) }
func (w *jsonWriter) null()                      { w.element(); w.buf = append(w.buf, "null"...) }
func (w *jsonWriter) int64Value(v int64)         { w.element(); w.buf = strconv.AppendInt(w.buf, v, 10) }
func (w *jsonWriter) boolValue(v bool)           { w.element(); w.buf = strconv.AppendBool(w.buf, v) }
func (w *jsonWriter) keyInt64(k string, v int64) { w.key(k); w.int64Value(v) }
func (w *jsonWriter) keyBool(k string, v bool)   { w.key(k); w.boolValue(v) }

// keyNullableString writes a Java String property: nil is JSON null.
func (w *jsonWriter) keyNullableString(k string, v *string) {
	w.key(k)
	if v == nil {
		w.null()
		return
	}
	w.stringValue(*v)
}

// keyBytes writes a Java byte[] property: nil is null, otherwise standard
// Base64 with padding and no line feeds (Jackson's default Base64Variant).
func (w *jsonWriter) keyBytes(k string, v []byte) {
	w.key(k)
	if v == nil {
		w.null()
		return
	}
	w.stringValue(base64.StdEncoding.EncodeToString(v))
}
