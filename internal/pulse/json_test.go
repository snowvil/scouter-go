package pulse

import "testing"

// Jackson 2.15.2 as observed in the Java golden: '"' and '\\' escaped, controls
// as short escapes or \u00XX uppercase hex, supplementary characters as a
// surrogate-pair escape, '/' and 0x7F and BMP non-ASCII raw.
func TestJSONWriterEscaping(t *testing.T) {
	w := newJSONWriter()
	w.beginObject()
	w.key("k")
	w.stringValue("a\"b\\c/d\n\t\r\b\f\x01\x1f\x7f한글😀")
	w.key("n")
	w.null()
	w.key("arr")
	w.beginArray()
	w.int64Value(-9223372036854775808)
	w.boolValue(true)
	w.stringValue("")
	w.endArray()
	w.key("empty")
	w.beginArray()
	w.endArray()
	w.keyBytes("b", []byte{10, 0, 0, 1})
	w.keyBytes("bn", nil)
	w.keyBytes("be", []byte{})
	w.endObject()
	want := `{"k":"a\"b\\c/d\n\t\r\b\f\u0001\u001F` + "\x7f한글" + `\uD83D\uDE00","n":null,"arr":[-9223372036854775808,true,""],"empty":[],"b":"CgAAAQ==","bn":null,"be":""}`
	if string(w.buf) != want {
		t.Fatalf("got  %s\nwant %s", w.buf, want)
	}
}
