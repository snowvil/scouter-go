package pulse

import (
	"reflect"
	"testing"
)

func TestJavaSplit(t *testing.T) {
	cases := []struct {
		in, sep string
		want    []string
	}{
		{"", "/", []string{""}},
		{"a", "/", []string{"a"}},
		{"/a/b", "/", []string{"", "a", "b"}},
		{"a/b/", "/", []string{"a", "b"}},
		{"///", "/", []string{}},
		{"a,", ",", []string{"a"}},
		{",a", ",", []string{"", "a"}},
		{"a||b", "|", []string{"a", "", "b"}},
	}
	for _, c := range cases {
		got := javaSplit(c.in, c.sep)
		if len(got) == 0 && len(c.want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("split(%q,%q)=%q want %q", c.in, c.sep, got, c.want)
		}
	}
}

// The plugin application-name extraction: split on "/", take element 2,
// split on "@", take element 0. Java failure modes (index out of bounds, null
// dictionary string) are unified as ok=false.
func TestExtractApplication(t *testing.T) {
	cases := []struct {
		name  string
		found bool
		want  string
		ok    bool
	}{
		{"/host/app@1", true, "app", true},
		{"/host/App-Name@1@2", true, "App-Name", true}, // no lowercase, first "@"
		{"/host/app", true, "app", true},
		{"/host/app@1/extra", true, "app", true},
		{"//app", true, "app", true},
		{"/host/app@1/", true, "app", true}, // trailing empty element removed
		{" /host/ app @1", true, " app ", true},
		{"host/app@1", false, "", false}, // dictionary miss (Java NullPointerException)
		{"host/app@1", true, "", false},  // length 2: Java ArrayIndexOutOfBoundsException
		{"noslash", true, "", false},     // length 1: Java returns ""
		{"", true, "", false},            // length 1: Java returns ""
		{"/a//b", true, "", false},       // element 2 is "" -> "" (isEmpty)
		{"/h/@1", true, "", false},       // leading "@" -> ""
		{"/h/@@", true, "", false},       // only "@": zero-length split (Java exception)
		{"/h/한글@1", true, "한글", true},
	}
	for _, c := range cases {
		got, ok := ExtractApplication(c.name, c.found)
		if got != c.want || ok != c.ok {
			t.Errorf("extract(%q,%v)=(%q,%v) want (%q,%v)", c.name, c.found, got, ok, c.want, c.ok)
		}
	}
}
