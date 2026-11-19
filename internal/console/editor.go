package console

import (
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// MaxHistory bounds the in-memory history (jline MemoryHistory default).
const MaxHistory = 500

// maxLineRunes bounds the edited line; further input rings the bell.
const maxLineRunes = MaxLineBytes / 4

// editor is the raw-mode line editor that stands in for the jline
// ConsoleReader: insert/delete, cursor movement, in-memory history (Up/Down,
// consecutive duplicates and empty lines skipped) and Tab completion over
// Words with jline StringsCompleter + CandidateListCompletionHandler rules.
// It assumes every rune occupies one terminal cell.
type editor struct {
	out    io.Writer
	words  []string // sorted completion candidates
	prompt string

	buf []rune
	pos int

	history []string
	histPos int    // index into history while browsing; len(history) = editing line
	saved   []rune // the line being edited before browsing history

	pending []byte // undecoded input: a partial UTF-8 rune or escape sequence
}

func newEditor(out io.Writer, words []string) *editor {
	w := append([]string(nil), words...)
	sort.Strings(w)
	return &editor{out: out, words: w}
}

// begin starts a new line and draws the prompt.
func (e *editor) begin(prompt string) {
	e.prompt, e.buf, e.pos = prompt, e.buf[:0], 0
	e.histPos, e.saved = len(e.history), nil
	e.write(prompt)
}

// result of feeding input.
type editResult int

const (
	editMore editResult = iota
	editLine
	editEOF
)

// feed consumes input bytes. It stops at the first completed line or EOF
// and keeps the remaining bytes for the next line.
func (e *editor) feed(data []byte) (editResult, string) {
	e.pending = append(e.pending, data...)
	for len(e.pending) > 0 {
		n, key, ok := decodeKey(e.pending, false)
		if !ok {
			return editMore, ""
		}
		e.pending = e.pending[n:]
		if r, line := e.key(key); r != editMore {
			return r, line
		}
	}
	return editMore, ""
}

// idle runs when no input arrived for a read interval: an incomplete
// escape sequence is then taken as a lone ESC (ignored).
func (e *editor) idle() (editResult, string) {
	for len(e.pending) > 0 {
		n, key, ok := decodeKey(e.pending, true)
		if !ok {
			e.pending = e.pending[:0]
			break
		}
		e.pending = e.pending[n:]
		if r, line := e.key(key); r != editMore {
			return r, line
		}
	}
	return editMore, ""
}

type keyKind int

const (
	keyRune keyKind = iota
	keyEnter
	keyBackspace
	keyDelete
	keyLeft
	keyRight
	keyHome
	keyEnd
	keyUp
	keyDown
	keyTab
	keyEOF // Ctrl-D
	keyKillEnd
	keyKillStart
	keyKillWord
	keyRedraw
	keyIgnore
)

type key struct {
	kind keyKind
	r    rune
}

// decodeKey decodes one key from b. ok=false asks for more bytes unless
// final is set, in which case a partial sequence decodes as keyIgnore.
func decodeKey(b []byte, final bool) (int, key, bool) {
	c := b[0]
	switch c {
	case '\r', '\n':
		return 1, key{kind: keyEnter}, true
	case 0x7f, 0x08:
		return 1, key{kind: keyBackspace}, true
	case '\t':
		return 1, key{kind: keyTab}, true
	case 0x01:
		return 1, key{kind: keyHome}, true
	case 0x05:
		return 1, key{kind: keyEnd}, true
	case 0x02:
		return 1, key{kind: keyLeft}, true
	case 0x06:
		return 1, key{kind: keyRight}, true
	case 0x10:
		return 1, key{kind: keyUp}, true
	case 0x0e:
		return 1, key{kind: keyDown}, true
	case 0x04:
		return 1, key{kind: keyEOF}, true
	case 0x0b:
		return 1, key{kind: keyKillEnd}, true
	case 0x15:
		return 1, key{kind: keyKillStart}, true
	case 0x17:
		return 1, key{kind: keyKillWord}, true
	case 0x0c:
		return 1, key{kind: keyRedraw}, true
	case 0x1b:
		return decodeEscape(b, final)
	}
	if c < 0x20 {
		return 1, key{kind: keyIgnore}, true
	}
	if !utf8.FullRune(b) {
		if final {
			return len(b), key{kind: keyIgnore}, true
		}
		return 0, key{}, false
	}
	r, n := utf8.DecodeRune(b)
	if r == utf8.RuneError && n <= 1 {
		return 1, key{kind: keyIgnore}, true
	}
	return n, key{kind: keyRune, r: r}, true
}

// decodeEscape handles ESC [ ... final and ESC O x sequences.
func decodeEscape(b []byte, final bool) (int, key, bool) {
	if len(b) < 2 {
		if final {
			return 1, key{kind: keyIgnore}, true
		}
		return 0, key{}, false
	}
	switch b[1] {
	case 'O':
		if len(b) < 3 {
			if final {
				return len(b), key{kind: keyIgnore}, true
			}
			return 0, key{}, false
		}
		return 3, key{kind: escapeFinal(b[2], "")}, true
	case '[':
		for i := 2; i < len(b); i++ {
			if b[i] >= 0x40 && b[i] <= 0x7e {
				return i + 1, key{kind: escapeFinal(b[i], string(b[2:i]))}, true
			}
			if i >= 16 {
				return i + 1, key{kind: keyIgnore}, true
			}
		}
		if final {
			return len(b), key{kind: keyIgnore}, true
		}
		return 0, key{}, false
	}
	return 2, key{kind: keyIgnore}, true // Alt+key: ignored
}

func escapeFinal(f byte, params string) keyKind {
	switch f {
	case 'A':
		return keyUp
	case 'B':
		return keyDown
	case 'C':
		return keyRight
	case 'D':
		return keyLeft
	case 'H':
		return keyHome
	case 'F':
		return keyEnd
	case '~':
		switch params {
		case "1", "7":
			return keyHome
		case "4", "8":
			return keyEnd
		case "3":
			return keyDelete
		}
	}
	return keyIgnore
}

func (e *editor) key(k key) (editResult, string) {
	switch k.kind {
	case keyRune:
		if len(e.buf) >= maxLineRunes {
			e.write("\a")
			return editMore, ""
		}
		e.buf = append(e.buf, 0)
		copy(e.buf[e.pos+1:], e.buf[e.pos:])
		e.buf[e.pos] = k.r
		e.pos++
	case keyEnter:
		line := string(e.buf)
		e.write("\n")
		e.remember(line)
		return editLine, line
	case keyBackspace:
		if e.pos == 0 {
			return editMore, ""
		}
		e.buf = append(e.buf[:e.pos-1], e.buf[e.pos:]...)
		e.pos--
	case keyDelete:
		if e.pos >= len(e.buf) {
			return editMore, ""
		}
		e.buf = append(e.buf[:e.pos], e.buf[e.pos+1:]...)
	case keyEOF:
		if len(e.buf) == 0 {
			e.write("\n")
			return editEOF, ""
		}
		if e.pos < len(e.buf) {
			e.buf = append(e.buf[:e.pos], e.buf[e.pos+1:]...)
		}
	case keyLeft:
		if e.pos > 0 {
			e.pos--
		}
	case keyRight:
		if e.pos < len(e.buf) {
			e.pos++
		}
	case keyHome:
		e.pos = 0
	case keyEnd:
		e.pos = len(e.buf)
	case keyKillEnd:
		e.buf = e.buf[:e.pos]
	case keyKillStart:
		e.buf = append(e.buf[:0], e.buf[e.pos:]...)
		e.pos = 0
	case keyKillWord:
		i := e.pos
		for i > 0 && e.buf[i-1] == ' ' {
			i--
		}
		for i > 0 && e.buf[i-1] != ' ' {
			i--
		}
		e.buf = append(e.buf[:i], e.buf[e.pos:]...)
		e.pos = i
	case keyUp:
		if e.histPos == 0 {
			return editMore, ""
		}
		if e.histPos == len(e.history) {
			e.saved = append([]rune(nil), e.buf...)
		}
		e.histPos--
		e.setLine([]rune(e.history[e.histPos]))
	case keyDown:
		if e.histPos >= len(e.history) {
			return editMore, ""
		}
		e.histPos++
		if e.histPos == len(e.history) {
			e.setLine(e.saved)
		} else {
			e.setLine([]rune(e.history[e.histPos]))
		}
	case keyTab:
		e.complete()
		return editMore, ""
	case keyRedraw:
	default:
		return editMore, ""
	}
	e.redraw()
	return editMore, ""
}

func (e *editor) setLine(r []rune) {
	e.buf = append(e.buf[:0], r...)
	e.pos = len(e.buf)
}

// remember adds a non-empty line unless it repeats the previous entry.
func (e *editor) remember(line string) {
	if line == "" || (len(e.history) > 0 && e.history[len(e.history)-1] == line) {
		return
	}
	if len(e.history) >= MaxHistory {
		copy(e.history, e.history[1:])
		e.history = e.history[:len(e.history)-1]
	}
	e.history = append(e.history, line)
}

// complete applies jline StringsCompleter (candidates are the words that
// start with the whole buffer; a single one gets a trailing space) and
// CandidateListCompletionHandler (the text before the cursor is replaced by
// the candidate or the common prefix; several candidates are listed).
func (e *editor) complete() {
	buffer := string(e.buf)
	var cands []string
	for _, w := range e.words {
		if strings.HasPrefix(w, buffer) {
			cands = append(cands, w)
		}
	}
	switch len(cands) {
	case 0:
		e.write("\a")
		return
	case 1:
		value := cands[0] + " "
		if value == buffer {
			return
		}
		e.replaceHead(value)
		e.redraw()
		return
	}
	prefix := cands[0]
	for _, c := range cands[1:] {
		for !strings.HasPrefix(c, prefix) {
			prefix = prefix[:len(prefix)-1]
		}
	}
	e.replaceHead(prefix)
	e.write("\n" + strings.Join(cands, "  ") + "\n")
	e.redraw()
}

func (e *editor) replaceHead(value string) {
	tail := append([]rune(nil), e.buf[e.pos:]...)
	e.buf = append(append(e.buf[:0], []rune(value)...), tail...)
	e.pos = len([]rune(value))
}

// redraw rewrites the prompt and the line and places the cursor.
func (e *editor) redraw() {
	var b strings.Builder
	b.WriteString("\r")
	b.WriteString(e.prompt)
	b.WriteString(string(e.buf))
	b.WriteString("\x1b[K")
	if back := len(e.buf) - e.pos; back > 0 {
		b.WriteString("\x1b[" + strconv.Itoa(back) + "D")
	}
	e.write(b.String())
}

func (e *editor) write(s string) { _, _ = io.WriteString(e.out, s) }
