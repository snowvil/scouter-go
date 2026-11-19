package sqltable

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"unicode/utf8"
)

type goldenCase struct {
	Name   string `json:"name"`
	SQL    string `json:"sql"`
	Crud   string `json:"crud"`
	Thrown string `json:"thrown"`
}

type golden struct {
	SourceCommit string       `json:"source_commit"`
	JavaVersion  string       `json:"java_specification_version"`
	Cases        []goldenCase `json:"cases"`
}

func loadGolden(tb testing.TB) golden {
	tb.Helper()
	path := "testdata/java_crud_golden.json"
	if env := os.Getenv("SQLTABLE_GOLDEN"); env != "" {
		path = env // ad-hoc larger run from scripts/java-sqltable.py --extra-out
	}
	b, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	var g golden
	if err := json.Unmarshal(b, &g); err != nil {
		tb.Fatal(err)
	}
	if g.SourceCommit != "dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5" || g.JavaVersion != "1.8" || len(g.Cases) == 0 {
		tb.Fatalf("unexpected golden header: %+v cases=%d", g.SourceCommit, len(g.Cases))
	}
	return g
}

// TestJavaCrudGolden compares CrudInfo with SQLSimpleParser.getCrudInfo run by
// scripts/java-sqltable.py on JDK 8 at the pinned commit.
func TestJavaCrudGolden(t *testing.T) {
	g := loadGolden(t)
	nonEmpty := 0
	for _, c := range g.Cases {
		if c.Thrown != "" {
			t.Fatalf("%s: golden recorded a Java Throwable %s; such cases are a documented difference", c.Name, c.Thrown)
		}
		got, err := Parse(c.SQL)
		if err != nil {
			t.Errorf("%s: unexpected Go bound error %v", c.Name, err)
			continue
		}
		if got != c.Crud {
			t.Errorf("%s:\n go   %q\n java %q", c.Name, got, c.Crud)
		}
		if got != "" {
			nonEmpty++
		}
	}
	t.Logf("%d cases, %d non-empty", len(g.Cases), nonEmpty)
}

func TestParseBounds(t *testing.T) {
	if _, err := Parse(strings.Repeat("a", MaxSQLBytes+1)); !errors.Is(err, ErrTooLong) {
		t.Fatalf("too long: %v", err)
	}
	// Each "( SELECT" re-scans the rest of the text (the Java inner loop
	// does not break while parentheses stay open) and every later FROM adds a
	// node, so this input is super-quadratic and must hit the budget.
	sql := strings.Repeat("( SELECT a FROM t ", 40000)
	if _, err := Parse(sql); !errors.Is(err, ErrBudget) {
		t.Fatalf("budget: %v", err)
	}
	if got := CrudInfo(sql); got != "" {
		t.Fatalf("CrudInfo on budget failure = %q", got)
	}
	// 200 repetitions take the Java oracle ~3 s (result "t(R)", observed with
	// the scripts/java-sqltable.py harness); Go must stay within its bounds.
	if got, err := Parse(strings.Repeat("( SELECT a FROM t ", 200)); err != nil || got != "t(R)" {
		t.Fatalf("nested 200: %q %v", got, err)
	}
	if _, err := Parse(strings.Repeat("SELECT ", MaxNodes+1)); !errors.Is(err, ErrBudget) {
		t.Fatalf("node cap: %v", err)
	}
	if got, err := Parse(strings.Repeat("SELECT ", MaxNodes)); err != nil || got != "" {
		t.Fatalf("node cap boundary: %q %v", got, err)
	}
}

func TestParserReuseIsolation(t *testing.T) {
	// SqlTables builds a new SQLSimpleParser per text; Parse must not leak state.
	a := CrudInfo("SELECT a FROM t1 WHERE b IN (SELECT b FROM t2")
	b := CrudInfo("SELECT a FROM t3")
	if a != "t1(R),t2(R)" || b != "t3(R)" {
		t.Fatalf("a=%q b=%q", a, b)
	}
}

func FuzzCrudInfo(f *testing.F) {
	for _, c := range loadGolden(f).Cases {
		if len(c.SQL) <= 4096 {
			f.Add(c.SQL)
		}
	}
	f.Fuzz(func(t *testing.T, sql string) {
		if len(sql) > 1<<16 {
			t.Skip()
		}
		got, err := Parse(sql)
		if err != nil && !errors.Is(err, ErrBudget) {
			t.Fatalf("unexpected error %v", err)
		}
		again, _ := Parse(sql)
		if got != again {
			t.Fatalf("non-deterministic: %q vs %q", got, again)
		}
		if utf8.ValidString(sql) && !utf8.ValidString(got) {
			t.Fatalf("invalid UTF-8 result %q", got)
		}
	})
}
