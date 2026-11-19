package pulse

import "testing"

// Expected values follow java.util.Formatter with String arguments; specifiers
// that Java rejects map to ok=false (buildMessasge then returns the raw format).
func TestJavaFormat(t *testing.T) {
	cases := []struct {
		format string
		args   []string
		want   string
		ok     bool
	}{
		{"plain", []string{"a"}, "plain", true},
		{"%s-%s", []string{"a", "b"}, "a-b", true},
		{"%s", []string{"a", "b"}, "a", true}, // extra arguments are ignored
		{"%2$s %1$s", []string{"a", "b"}, "b a", true},
		{"%s %<s", []string{"a"}, "a a", true},
		{"%<s", []string{"a"}, "", false}, // MissingFormatArgumentException
		{"%s %s", []string{"a"}, "", false},
		{"%5s|%-5s|", []string{"ab", "cd"}, "   ab|cd   |", true},
		{"%.1s", []string{"abc"}, "a", true},
		{"%S", []string{"ab"}, "AB", true},
		{"%b %B", []string{"x", "y"}, "true TRUE", true},
		{"%h", []string{"hello"}, "5e918d2", true},
		{"100%%", []string{"x"}, "100%", true},
		{"a%nb", []string{"x"}, "a\nb", true},
		{"%", []string{"x"}, "", false},   // UnknownFormatConversionException
		{"%d", []string{"1"}, "", false},  // IllegalFormatConversionException
		{"%-s", []string{"a"}, "", false}, // MissingFormatWidthException
		{"%+s", []string{"a"}, "", false}, // FormatFlagsConversionMismatchException
		{"%#s", []string{"a"}, "", false},
		{"%--5s", []string{"a"}, "", false}, // DuplicateFormatFlagsException
		{"%.2%", []string{"a"}, "", false},  // IllegalFormatPrecisionException
		{"%5n", []string{"a"}, "", false},   // IllegalFormatWidthException
		{"%q", []string{"a"}, "", false},
		{"한글 %s", []string{"값"}, "한글 값", true},
	}
	for _, c := range cases {
		got, ok := javaFormat(c.format, c.args)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("format(%q,%q)=(%q,%v) want (%q,%v)", c.format, c.args, got, ok, c.want, c.ok)
		}
	}
}

func TestBuildParameterizedMessage(t *testing.T) {
	if got := buildParameterizedMessage("x=%s y=%s", "1\x032\x03"); got != "x=1 y=2" {
		t.Fatal(got)
	}
	if got := buildParameterizedMessage("x=%s", ""); got != "x=%s" {
		t.Fatal("empty params must return the format", got)
	}
	if got := buildParameterizedMessage("x=%d", "1"); got != "x=%d" {
		t.Fatal("format failure must return the format", got)
	}
	if got := buildParameterizedMessage("**unlabeled**:hmsg:5", "1"); got != "**unlabeled**:hmsg:5" {
		t.Fatal(got)
	}
}

func TestJavaIsBlank(t *testing.T) {
	for in, want := range map[string]bool{"": true, " \t\n": true, "\u001c": true, " ": false, " ": true, "x": false, " x ": false} {
		if got := javaIsBlank(in); got != want {
			t.Errorf("isBlank(%q)=%v want %v", in, got, want)
		}
	}
}
