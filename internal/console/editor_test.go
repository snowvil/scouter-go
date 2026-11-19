package console

import (
	"bytes"
	"strings"
	"testing"
)

// typeKeys feeds input byte by byte (worst case for sequence splitting) and
// returns every completed line, "<EOF>" for Ctrl-D on an empty line.
func typeKeys(e *editor, input string) []string {
	var lines []string
	e.begin(Prompt)
	for i := 0; i < len(input); i++ {
		r, line := e.feed([]byte{input[i]})
		switch r {
		case editLine:
			lines = append(lines, line)
			e.begin(Prompt)
		case editEOF:
			lines = append(lines, "<EOF>")
			e.begin(Prompt)
		}
	}
	if r, line := e.idle(); r == editLine {
		lines = append(lines, line)
	}
	return lines
}

func TestEditorEditing(t *testing.T) {
	out := &bytes.Buffer{}
	e := newEditor(out, Words)
	got := typeKeys(e, "helo\x1b[Dl\r"+ // left arrow, insert
		"abc\x01X\x05Y\n"+ // Ctrl-A / Ctrl-E
		"one two\x17three\r"+ // Ctrl-W kills "two"
		"xyz\x7f\x7f\r"+ // backspace
		"abcd\x1b[D\x1b[D\x1b[3~\r"+ // delete under the cursor
		"kill\x15ok\r"+ // Ctrl-U
		"tail\x1b[H\x0b\r"+ // Home + Ctrl-K
		"d\x04\x04\r"+ // Ctrl-D deletes at end? no: at end it is a no-op
		"\x04"+ // Ctrl-D on an empty line is EOF
		"\x1bOHz\x1b[F!\r"+ // ESC O H home, ESC [ F end
		"\x1b[1;5C\x1b[99~\x1bx\x03ok2\r"+ // unknown sequences, Alt-x and ^C are ignored
		"한글\r")
	want := []string{"hello", "XabcY", "one three", "x", "abd", "ok", "", "d", "<EOF>", "z!", "ok2", "한글"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("lines %q\nwant  %q", got, want)
	}
	// A lone ESC followed by idle time is dropped, the line continues.
	e.begin(Prompt)
	e.feed([]byte("ab\x1b"))
	e.idle()
	if r, line := e.feed([]byte("c\r")); r != editLine || line != "abc" {
		t.Fatalf("%v %q", r, line)
	}
	if !strings.Contains(out.String(), "\r"+Prompt+"helo\x1b[K\x1b[1D") {
		t.Fatalf("redraw sequence missing: %q", out.String())
	}
}

func TestEditorHistory(t *testing.T) {
	e := newEditor(&bytes.Buffer{}, Words)
	typeKeys(e, "one\rtwo\rtwo\r\rthree\r")
	if strings.Join(e.history, "|") != "one|two|three" {
		t.Fatalf("history %q (empty and consecutive duplicates are skipped)", e.history)
	}
	got := typeKeys(e, "dra\x1b[A\x1b[A\r"+ // Up Up -> two
		"\x10\x10\x10\x10\r"+ // Ctrl-P past the oldest stays on it
		"new\x1b[A\x1b[B\r"+ // Up then Down restores the edited line
		"\x1b[B\r") // Down at the bottom does nothing
	if strings.Join(got, "|") != "two|one|new|" {
		t.Fatalf("got %q", got)
	}
	for i := 0; i < MaxHistory+20; i++ {
		e.remember(strings.Repeat("x", i%7+1) + string(rune('a'+i%26)))
	}
	if len(e.history) != MaxHistory {
		t.Fatalf("history not bounded: %d", len(e.history))
	}
	e.begin(Prompt)
	for i := 0; i < maxLineRunes+5; i++ {
		e.feed([]byte("y"))
	}
	if len(e.buf) != maxLineRunes {
		t.Fatalf("line not bounded: %d", len(e.buf))
	}
}

func TestEditorCompletion(t *testing.T) {
	out := &bytes.Buffer{}
	e := newEditor(out, Words)
	got := typeKeys(e, "da\t\r"+ // single candidate + space
		"obj\t\r"+ // object/objtype: common prefix "obj" listed
		"objt\t\r"+
		"q\tx\r"+
		"zz\t\r"+ // no candidate: bell
		"dashboard \t\r"+ // exact single candidate equal to buffer: nothing
		"re\x01\x05\t\r") // cursor at the end again
	want := []string{"dashboard ", "obj", "objtype ", "quit x", "zz", "dashboard ", "realtime "}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %q want %q", got, want)
	}
	if !strings.Contains(out.String(), "\nobject  objtype\n") || !strings.Contains(out.String(), "\a") {
		t.Fatalf("candidate list or bell missing: %q", out.String())
	}
	out.Reset()
	typeKeys(e, "\t\r")
	if !strings.Contains(out.String(), "\ncounter  dashboard  help  object  objtype  quit  realtime  tagcnt\n") {
		t.Fatalf("all eight words expected: %q", out.String())
	}
	// Completion replaces the text before the cursor and keeps the rest.
	e.begin(Prompt)
	e.feed([]byte("hel tail\x01\x1b[C\x1b[C\x1b[C"))
	if e.pos != 3 {
		t.Fatal(e.pos)
	}
	// "hel tail" matches no word (StringsCompleter uses the whole buffer).
	e.feed([]byte("\t"))
	if string(e.buf) != "hel tail" {
		t.Fatal(string(e.buf))
	}
}
