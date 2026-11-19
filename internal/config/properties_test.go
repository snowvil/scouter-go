package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// TestParsePropertiesJavaOracle compares ParseProperties with
// java.util.Properties.load on the testdata inputs. expected.json was
// produced by testdata/oracle/PropertiesOracle.java on JDK 8 and JDK 17
// (identical output); see CONTRACT.md.
func TestParsePropertiesJavaOracle(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "properties", "expected.json"))
	if err != nil {
		t.Fatal(err)
	}
	var expected map[string]struct {
		Error   *string           `json:"error"`
		Entries map[string]string `json:"entries"`
	}
	if err := json.Unmarshal(raw, &expected); err != nil {
		t.Fatal(err)
	}
	if len(expected) < 8 {
		t.Fatalf("fixture has %d cases", len(expected))
	}
	for name, want := range expected {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", "properties", name))
			if err != nil {
				t.Fatal(err)
			}
			p, err := ParseProperties(bytes.NewReader(data))
			if (want.Error != nil) != (err != nil) {
				t.Fatalf("error %v, Java %v", err, want.Error)
			}
			if want.Error != nil && !errors.Is(err, ErrMalformedUnicode) {
				t.Fatalf("error %v is not ErrMalformedUnicode", err)
			}
			if !reflect.DeepEqual(p.Map(), want.Entries) {
				t.Fatalf("entries\n got %q\nwant %q", p.Map(), want.Entries)
			}
		})
	}
}

func TestParsePropertiesOrderAndLimit(t *testing.T) {
	p, err := ParseProperties(strings.NewReader("b=1\na=2\nb=3\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Keys(); !reflect.DeepEqual(got, []string{"b", "a"}) {
		t.Fatalf("keys %v", got)
	}
	if v, _ := p.Get("b"); v != "3" {
		t.Fatalf("duplicate keeps the last value: %q", v)
	}
	if _, err := ParseProperties(strings.NewReader(strings.Repeat("x", MaxFileBytes+1))); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversize: %v", err)
	}
	// UTF-8 bytes stay UTF-8 (Java would decode ISO-8859-1 mojibake).
	p, _ = ParseProperties(strings.NewReader("name=한글\n"))
	if v, _ := p.Get("name"); v != "한글" {
		t.Fatalf("utf-8 value %q", v)
	}
}

func TestReplaceSysProp(t *testing.T) {
	env := map[string]string{"HOME_DIR": "/srv", "PORT": "6200"}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	got := ReplaceSysProp(map[string]string{
		"a": "  ${HOME_DIR}/db  ",
		"b": "${ PORT }",
		"c": "x${UNDEFINED}y",
		"d": "keep${",
		"e": "${HOME_DIR}${PORT}",
		"f": "plain",
	}, lookup)
	want := map[string]string{"a": "/srv/db", "b": "6200", "c": "x${UNDEFINED}y", "d": "keep", "e": "/srv6200", "f": "plain"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q want %q", got, want)
	}
	if JavaTrim("\x00 a\t\x1f") != "a" {
		t.Fatal("JavaTrim must strip bytes <= ' '")
	}
}

func FuzzParseProperties(f *testing.F) {
	for _, s := range []string{"a=b\n", "k\\\n v", "\\u0041=\\u12", "#x\\\ny=z", "a:b c\r\nd"} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := parseProperties(data)
		if p == nil {
			t.Fatal("nil Properties")
		}
		if err != nil && !errors.Is(err, ErrMalformedUnicode) {
			t.Fatalf("unexpected error %v", err)
		}
		if len(p.Keys()) != p.Len() {
			t.Fatal("key order and map disagree")
		}
	})
}
