// Package config reads the Java scouter.conf file the way
// scouter.server.Configure does: java.util.Properties syntax, ${...} system
// property replacement, Configure.applyConfig() typed defaults, the 3 second
// mtime poll and loadText/saveText. CONTRACT.md records the fixed semantics
// and the intended differences from the Java original.
package config

import (
	"errors"
	"io"
	"strings"
	"unicode/utf8"
)

// MaxFileBytes bounds one scouter.conf read or write. Java reads the whole
// file without a limit; the Go server rejects larger files explicitly.
const MaxFileBytes = 4 << 20

var (
	ErrTooLarge         = errors.New("config: file exceeds the size limit")
	ErrMalformedUnicode = errors.New("config: malformed \\uxxxx encoding")
)

// Properties is the result of one java.util.Properties.load pass. Duplicate
// keys keep the last value, like the Java Hashtable; Keys reports first
// appearance order for diagnostics only, since Java keeps no order.
type Properties struct {
	values map[string]string
	order  []string
}

func (p *Properties) Get(key string) (string, bool) {
	v, ok := p.values[key]
	return v, ok
}
func (p *Properties) Len() int { return len(p.values) }

// Keys returns the keys in first appearance order.
func (p *Properties) Keys() []string { return append([]string(nil), p.order...) }

// Map returns a copy of the entries.
func (p *Properties) Map() map[string]string {
	out := make(map[string]string, len(p.values))
	for k, v := range p.values {
		out[k] = v
	}
	return out
}

func (p *Properties) put(key, value string) {
	if _, ok := p.values[key]; !ok {
		p.order = append(p.order, key)
	}
	p.values[key] = value
}

// ParseProperties reads r with java.util.Properties.load(InputStream)
// semantics (JDK LineReader/load0/loadConvert): leading whitespace is
// stripped, '#'/'!' start comment lines, a key ends at the first unescaped
// '=', ':' or whitespace, an odd trailing backslash continues the logical
// line on the next physical line (whose leading whitespace is dropped), and
// \t \n \r \f \uXXXX escapes are decoded while any other escaped character
// stands for itself. A malformed \uXXXX stops the parse like the Java
// IllegalArgumentException; the entries parsed before it are returned with
// ErrMalformedUnicode, which mirrors Configure.reloadConfig applying the
// partially loaded Properties after printing the exception.
func ParseProperties(r io.Reader) (*Properties, error) {
	data, err := io.ReadAll(io.LimitReader(r, MaxFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxFileBytes {
		return nil, ErrTooLarge
	}
	return parseProperties(data)
}

func parseProperties(data []byte) (*Properties, error) {
	p := &Properties{values: map[string]string{}}
	lr := lineReader{in: data}
	for {
		line, ok := lr.readLine()
		if !ok {
			return p, nil
		}
		keyLen, valueStart, hasSep, precedingBackslash := 0, len(line), false, false
		for keyLen < len(line) {
			c := line[keyLen]
			if (c == '=' || c == ':') && !precedingBackslash {
				valueStart = keyLen + 1
				hasSep = true
				break
			} else if (c == ' ' || c == '\t' || c == '\f') && !precedingBackslash {
				valueStart = keyLen + 1
				break
			}
			if c == '\\' {
				precedingBackslash = !precedingBackslash
			} else {
				precedingBackslash = false
			}
			keyLen++
		}
		for valueStart < len(line) {
			c := line[valueStart]
			if c != ' ' && c != '\t' && c != '\f' {
				if !hasSep && (c == '=' || c == ':') {
					hasSep = true
				} else {
					break
				}
			}
			valueStart++
		}
		key, err := loadConvert(line[:keyLen])
		if err != nil {
			return p, err
		}
		value, err := loadConvert(line[valueStart:])
		if err != nil {
			return p, err
		}
		p.put(key, value)
	}
}

// lineReader is the JDK Properties.LineReader over bytes. Java decodes the
// stream as ISO-8859-1 (one char per byte); Go keeps the bytes, so UTF-8 text
// stays UTF-8 instead of becoming mojibake (CONTRACT.md).
type lineReader struct {
	in  []byte
	off int
}

func (l *lineReader) readLine() ([]byte, bool) {
	var buf []byte
	skipWhiteSpace, isCommentLine, isNewLine := true, false, true
	appendedLineBegin, precedingBackslash, skipLF := false, false, false
	for {
		if l.off >= len(l.in) {
			if len(buf) == 0 || isCommentLine {
				return nil, false
			}
			if precedingBackslash {
				buf = buf[:len(buf)-1]
			}
			return buf, true
		}
		c := l.in[l.off]
		l.off++
		if skipLF {
			skipLF = false
			if c == '\n' {
				continue
			}
		}
		if skipWhiteSpace {
			if c == ' ' || c == '\t' || c == '\f' {
				continue
			}
			if !appendedLineBegin && (c == '\r' || c == '\n') {
				continue
			}
			skipWhiteSpace = false
			appendedLineBegin = false
		}
		if isNewLine {
			isNewLine = false
			if c == '#' || c == '!' {
				// Comment: consume the rest of the physical line (JDK 9+ form;
				// a backslash inside a comment never continues it).
				for l.off < len(l.in) && l.in[l.off] != '\n' && l.in[l.off] != '\r' {
					l.off++
				}
				isNewLine = true
				skipWhiteSpace = true
				continue
			}
		}
		if c != '\n' && c != '\r' {
			buf = append(buf, c)
			if c == '\\' {
				precedingBackslash = !precedingBackslash
			} else {
				precedingBackslash = false
			}
			continue
		}
		// End of a physical line.
		if isCommentLine || len(buf) == 0 {
			isCommentLine = false
			isNewLine = true
			skipWhiteSpace = true
			buf = buf[:0]
			continue
		}
		if l.off >= len(l.in) {
			if precedingBackslash {
				buf = buf[:len(buf)-1]
			}
			return buf, true
		}
		if precedingBackslash {
			buf = buf[:len(buf)-1]
			skipWhiteSpace = true
			appendedLineBegin = true
			precedingBackslash = false
			if c == '\r' {
				skipLF = true
			}
			continue
		}
		return buf, true
	}
}

// loadConvert is Properties.loadConvert: escapes are decoded, \uXXXX becomes
// the UTF-8 encoding of the code point (an unpaired surrogate becomes U+FFFD,
// which Java cannot express in UTF-8 either), and other bytes pass through.
func loadConvert(in []byte) (string, error) {
	if !hasByte(in, '\\') {
		return string(in), nil
	}
	out := make([]byte, 0, len(in))
	for i := 0; i < len(in); i++ {
		c := in[i]
		if c != '\\' {
			out = append(out, c)
			continue
		}
		i++
		if i >= len(in) {
			// Unreachable from readLine (a trailing backslash is consumed as a
			// continuation); Java would read past the buffer. Drop it.
			break
		}
		c = in[i]
		switch c {
		case 'u':
			if i+4 >= len(in) { // JDK 9+: off > end-4 → IllegalArgumentException
				return string(out), ErrMalformedUnicode
			}
			var value rune
			for j := 1; j <= 4; j++ {
				h := in[i+j]
				switch {
				case h >= '0' && h <= '9':
					value = value<<4 + rune(h-'0')
				case h >= 'a' && h <= 'f':
					value = value<<4 + rune(h-'a'+10)
				case h >= 'A' && h <= 'F':
					value = value<<4 + rune(h-'A'+10)
				default:
					return string(out), ErrMalformedUnicode
				}
			}
			i += 4
			out = utf8.AppendRune(out, value)
		case 't':
			out = append(out, '\t')
		case 'r':
			out = append(out, '\r')
		case 'n':
			out = append(out, '\n')
		case 'f':
			out = append(out, '\f')
		default:
			out = append(out, c)
		}
	}
	return string(out), nil
}

func hasByte(b []byte, c byte) bool {
	for _, x := range b {
		if x == c {
			return true
		}
	}
	return false
}

// JavaTrim is String.trim: it removes leading and trailing bytes <= ' '.
func JavaTrim(s string) string {
	start, end := 0, len(s)
	for start < end && s[start] <= ' ' {
		start++
	}
	for end > start && s[end-1] <= ' ' {
		end--
	}
	return s[start:end]
}

// ReplaceSysProp is ConfigValueUtil.replaceSysProp applied to the file
// entries: each value is trimmed (String.trim) and its ${name} references are
// replaced through lookup (by default the process environment). Keys are not
// replaced. ParamText tokenization is kept: the name inside the braces is
// trimmed and text after an unterminated "${" is dropped.
//
// Intended difference (CONTRACT.md): Java passes a Map<Object,Object> and so
// binds ParamText.getText(Object), which replaces every ${...} with the
// toString() of the whole environment+system-property map (Java oracle run
// recorded in CONTRACT.md). Go substitutes the named variable and keeps an
// unknown reference as the literal "${name}".
func ReplaceSysProp(values map[string]string, lookup func(string) (string, bool)) map[string]string {
	out := make(map[string]string, len(values))
	for k, v := range values {
		out[k] = ExpandParams(JavaTrim(v), lookup)
	}
	return out
}

// ExpandParams applies scouter.util.ParamText("${", "}") to text.
func ExpandParams(text string, lookup func(string) (string, bool)) string {
	if !strings.Contains(text, "${") {
		return text
	}
	var b strings.Builder
	for len(text) > 0 {
		pos := strings.Index(text, "${")
		if pos < 0 {
			b.WriteString(text)
			break
		}
		if pos > 0 {
			b.WriteString(text[:pos])
			text = text[pos:]
			continue
		}
		end := strings.Index(text[2:], "}")
		if end < 0 {
			break
		}
		name := JavaTrim(text[2 : 2+end])
		if lookup != nil {
			if v, ok := lookup(name); ok {
				b.WriteString(v)
				text = text[2+end+1:]
				continue
			}
		}
		b.WriteString("${")
		b.WriteString(name)
		b.WriteString("}")
		text = text[2+end+1:]
	}
	return b.String()
}
