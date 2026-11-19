package scoutertime

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestJavaCalendarOracle(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	source := os.Getenv("SCOUTER_SOURCE")
	if source == "" {
		source = filepath.Join(root, "..", "scouter")
	}
	common := filepath.Join(source, "scouter.common", "src", "main", "java")
	if _, err = os.Stat(common); err != nil {
		t.Skip("pinned Java source unavailable; calendar oracle not run")
	}
	if _, err = exec.LookPath("javac"); err != nil {
		t.Skip("JDK unavailable; calendar oracle not run")
	}
	head, err := exec.Command("git", "-C", source, "rev-parse", "HEAD").Output()
	if err != nil || strings.TrimSpace(string(head)) != "dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5" {
		t.Fatal("calendar source SHA differs", err)
	}
	clean, err := exec.Command("git", "-C", source, "status", "--porcelain", "--", "scouter.common").Output()
	if err != nil || len(clean) != 0 {
		t.Fatal("modified Java calendar source", err)
	}
	classes := t.TempDir()
	command := exec.Command("javac", "--release", "11", "-encoding", "UTF-8", "-sourcepath", common, "-d", classes, filepath.Join("testdata", "DateOracle.java"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("original Java calendar compile: %v\n%s", err, output)
	}
	for _, zone := range []string{"Asia/Seoul", "America/New_York"} {
		loc, err := time.LoadLocation(zone)
		if err != nil {
			t.Fatal(err)
		}
		ms := time.Date(2026, 7, 1, 0, 0, 0, 0, loc).UnixMilli()
		output, err := exec.Command("java", "-cp", classes, "DateOracle", zone, strconv.FormatInt(ms, 10), "20260701").CombinedOutput()
		if err != nil {
			t.Fatalf("calendar oracle %s: %v %s", zone, err, output)
		}
		fields := strings.Fields(string(output))
		start, err := ParseDate("20260701", loc)
		if err != nil {
			t.Fatal(err)
		}
		expected := []string{Date(ms, loc), strconv.FormatInt(Millis(ms, loc), 10), strconv.FormatInt(start, 10), Date(0, loc), strconv.FormatInt(Millis(0, loc), 10)}
		if strings.Join(fields, "|") != strings.Join(expected, "|") {
			t.Fatalf("Java vs Go %s: %v != %v", zone, fields, expected)
		}
	}
}
