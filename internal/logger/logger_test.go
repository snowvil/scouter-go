package logger

import (
	"bytes"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func (s *syncBuffer) Reset() {
	s.mu.Lock()
	s.b.Reset()
	s.mu.Unlock()
}

var seoul = func() *time.Location {
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		panic(err)
	}
	return loc
}()

type harness struct {
	t      *testing.T
	l      *Logger
	clock  *fakeClock
	stdout *syncBuffer
	stderr *syncBuffer
	today  string
}

func newHarness(t *testing.T, cfg Config, at time.Time) *harness {
	t.Helper()
	h := &harness{t: t, clock: &fakeClock{t: at}, stdout: &syncBuffer{}, stderr: &syncBuffer{}}
	l, err := New(cfg, Options{Location: seoul, Now: h.clock.Now, Stdout: h.stdout, Stderr: h.stderr})
	if err != nil {
		t.Fatal(err)
	}
	h.l = l
	h.today = newCalendar(seoul).yyyymmdd(at.UnixMilli())
	t.Cleanup(func() { l.Close() })
	return h
}

func (h *harness) sync() {
	h.t.Helper()
	if !h.l.Sync() {
		h.t.Fatal("sync on closed logger")
	}
}

var datetimePrefix = regexp.MustCompile(`^(\d{8}) \d{2}:\d{2}:\d{2} `)
var timestampPrefix = regexp.MustCompile(`^(\d{8}) \d{2}:\d{2}:\d{2}\.\d{3} `)

// normalize mirrors LoggerOracle.normalize: the Java golden replaces the
// datetime prefix of today's lines with "<datetime> ".
func (h *harness) normalize(re *regexp.Regexp, tag, line string) string {
	m := re.FindStringSubmatchIndex(line)
	if m == nil {
		return line
	}
	if line[m[2]:m[3]] != h.today {
		h.t.Fatalf("line date is not today %s: %q", h.today, line)
	}
	return tag + " " + line[m[1]:]
}

func (h *harness) lines(path string, re *regexp.Regexp, tag string) []string {
	h.t.Helper()
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return []string{}
	}
	if err != nil {
		h.t.Fatal(err)
	}
	out := []string{}
	for _, line := range strings.Split(strings.TrimSuffix(string(b), "\n"), "\n") {
		if strings.HasPrefix(line, "\t") {
			continue
		}
		out = append(out, h.normalize(re, tag, line))
	}
	return out
}

func names(t *testing.T, dir, today string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return []string{}
	}
	if err != nil {
		t.Fatal(err)
	}
	out := []string{}
	for _, e := range entries {
		n := strings.ReplaceAll(e.Name(), today, "<today>")
		if e.IsDir() {
			n += "/"
		}
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func equal(t *testing.T, what string, got, want []string) {
	t.Helper()
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Errorf("%s:\n go   %q\n java %q", what, got, want)
	}
}

type loggerGolden map[string][]string

func loadLoggerGolden(t *testing.T) loggerGolden {
	var g loggerGolden
	loadJSON(t, "logger.json", &g)
	return g
}

// 2025-10-15 10:00:00 KST: far from midnight so one test never crosses a date.
var noon = time.Date(2025, 10, 15, 10, 0, 0, 0, seoul)

func TestJavaGoldenFormats(t *testing.T) {
	g := loadLoggerGolden(t)
	dir := filepath.Join(t.TempDir(), "logs-a")
	cfg := DefaultConfig()
	cfg.Dir = dir
	cfg.IgnoreIDs = []string{" S999 ", ""}
	h := newHarness(t, cfg, noon)
	l := h.l
	l.Println("plain message")
	l.PrintlnID("S100", "id message")
	l.PrintlnID("S999", "ignored by mgr_log_ignore_ids")
	l.PrintlnIDRate("S999", 10, "ignored with rate")
	l.PrintlnIDRate("S101", 10, "rate first")
	l.PrintlnIDRate("S101", 10, "rate second suppressed")
	l.PrintlnIDRate("S102", 0, "sec zero first")
	l.PrintlnIDRate("S102", 0, "sec zero second")
	l.PrintlnIDRateDetail("S103", 10, "with throwable", "java.lang.IllegalStateException: synthetic failure\n\tframe")
	l.Trace("trace while _trace=false")
	cfg.Trace = true
	l.SetConfig(cfg)
	l.Trace("trace while _trace=true")
	l.PrintlnID("S104", "copied to stdout while _trace=true")
	cfg.Trace = false
	l.SetConfig(cfg)
	l.Println("multi\nline message")
	l.Println("")
	h.sync()
	equal(t, "files", names(t, dir, h.today), g["formats_files"])
	equal(t, "lines", h.lines(filepath.Join(dir, "server-"+h.today+".log"), datetimePrefix, "<datetime>"), g["formats_lines"])
	var out []string
	for _, line := range strings.Split(strings.TrimSuffix(h.stdout.String(), "\n"), "\n") {
		out = append(out, h.normalize(datetimePrefix, "<datetime>", line))
	}
	equal(t, "stdout", out, g["formats_stdout"])
	if s := l.Stats(); s.Filtered != 2 || s.RateLimited != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestJavaGoldenRateLimitEviction(t *testing.T) {
	g := loadLoggerGolden(t)
	dir := t.TempDir()
	h := newHarness(t, Config{Dir: dir, Rotation: true, KeepDays: 31}, noon)
	for i := 0; i <= 1000; i++ {
		h.l.PrintlnIDRate("R"+strconv.Itoa(i), 100, "first R"+strconv.Itoa(i))
	}
	h.l.PrintlnIDRate("R0", 100, "second R0 after eviction")
	h.l.PrintlnIDRate("R1", 100, "second R1 still limited")
	h.l.PrintlnIDRate("R1000", 100, "second R1000 still limited")
	h.sync()
	var got []string
	for _, line := range h.lines(filepath.Join(dir, "server-"+h.today+".log"), datetimePrefix, "<datetime>") {
		if strings.Contains(line, "second") {
			got = append(got, line)
		}
	}
	equal(t, "rate limit", got, g["rate_limit_after_1001_ids"])
	// After the window the id prints again.
	h.clock.Add(100 * time.Second)
	h.l.PrintlnIDRate("R1", 100, "third R1")
	h.sync()
	if s := h.l.Stats(); s.RateLimited != 1 { // only R1000: re-adding R0 evicted R1
		t.Fatalf("rate limited %d", s.RateLimited)
	}
}

func TestRotationOffAppendsInsteadOfTruncating(t *testing.T) {
	g := loadLoggerGolden(t)
	dir := t.TempDir()
	h := newHarness(t, Config{Dir: dir, Rotation: false, KeepDays: 31}, noon)
	h.l.Println("rotation off first open")
	h.sync()
	path := filepath.Join(dir, "server.log")
	equal(t, "first open", h.lines(path, datetimePrefix, "<datetime>"), g["rotation_off_first_open"])
	equal(t, "files", names(t, dir, h.today), g["rotation_off_files"])
	// Reopen through a rotation flip, as the Java oracle does.
	h.l.SetConfig(Config{Dir: dir, Rotation: true, KeepDays: 31})
	h.sync()
	h.l.SetConfig(Config{Dir: dir, Rotation: false, KeepDays: 31})
	h.l.Println("rotation off second open")
	h.sync()
	// Java (D-L1) truncates server.log on every open; Go appends (intended difference).
	java := g["rotation_off_after_reopen"]
	if len(java) != 1 || java[0] != "<datetime> rotation off second open" {
		t.Fatalf("java golden no longer shows D-L1: %q", java)
	}
	equal(t, "append", h.lines(path, datetimePrefix, "<datetime>"), []string{"<datetime> rotation off first open", "<datetime> rotation off second open"})
}

func TestLogDirChangeReopens(t *testing.T) {
	g := loadLoggerGolden(t)
	root := t.TempDir()
	a, c := filepath.Join(root, "a"), filepath.Join(root, "c")
	h := newHarness(t, Config{Dir: a, Rotation: true, KeepDays: 31}, noon)
	h.l.Println("before")
	h.sync()
	h.l.SetConfig(Config{Dir: c, Rotation: true, KeepDays: 31})
	h.l.Println("after log_dir change")
	h.sync()
	equal(t, "new dir", names(t, c, h.today), g["dir_change_files"])
	equal(t, "old dir", h.lines(filepath.Join(a, "server-"+h.today+".log"), datetimePrefix, "<datetime>"), []string{"<datetime> before"})
}

var templateRe = regexp.MustCompile(`<today([+-]\d+)?>`)

func expand(cal calendar, nowMs int64, tmpl string) string {
	return templateRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		off := 0
		if sub := templateRe.FindStringSubmatch(m); sub[1] != "" {
			off, _ = strconv.Atoi(strings.TrimPrefix(sub[1], "+"))
		}
		return cal.yyyymmdd(nowMs + int64(off)*dayMillis)
	})
}

const mixedCaseTemplate = "Server-<today-30>.log"

var clearTemplates = []string{"server-<today>.log", "server-<today-3>.log", "server-<today-4>.log", "server-<today-30>.log",
	"request-<today-3>.log", "request-<today-4>.log", "request-<today-30>.txt", "server-<today-30>.log.1",
	"server-<today-30>", "other-<today-30>.log", "server-19991231.log", "server-21000101.log",
	"server-20261340.log", "server-abcdefgh.log", "server-+0201015.log", "server-1.log",
	"request-<today+1>.log", mixedCaseTemplate, "server-<today-30>.gz.log"}

func makeClearDir(t *testing.T, dir string, cal calendar, nowMs int64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "server-"+expand(cal, nowMs, "<today-30>")+".log.d"), 0o750); err != nil {
		t.Fatal(err)
	}
	for _, n := range clearTemplates {
		if err := os.WriteFile(filepath.Join(dir, expand(cal, nowMs, n)), nil, 0o640); err != nil {
			t.Fatal(err)
		}
	}
}

func templateNames(t *testing.T, dir string, cal calendar, nowMs int64) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := []string{}
	for _, e := range entries {
		n := toTemplate(cal, nowMs, e.Name(), e.IsDir())
		if n == mixedCaseTemplate {
			// The golden lists come from a case-insensitive filesystem, where
			// this name is the same file as the lower-case one. On a
			// case-sensitive filesystem it is a separate file that matches no
			// prefix and always remains.
			continue
		}
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func toTemplate(cal calendar, nowMs int64, name string, dir bool) string {
	if dir {
		if name == expand(cal, nowMs, "server-<today-30>.log.d") {
			return "server-<today-30>.log.d/"
		}
		return name + "/"
	}
	for _, tmpl := range clearTemplates {
		if expand(cal, nowMs, tmpl) == name {
			return tmpl
		}
	}
	return name
}

func TestJavaGoldenClearOldLogs(t *testing.T) {
	g := loadLoggerGolden(t)
	dir := filepath.Join(t.TempDir(), "logs-d")
	h := newHarness(t, Config{Dir: dir, Rotation: true, KeepDays: 3}, noon)
	cal, now := newCalendar(seoul), noon.UnixMilli()
	makeClearDir(t, dir, cal, now)
	if !h.l.runCleanup(now) {
		t.Fatal("cleanup")
	}
	h.sync()
	equal(t, "remaining", templateNames(t, dir, cal, now), g["clear_keep3_remaining"])
	deleted := []string{}
	for _, line := range h.lines(filepath.Join(dir, "server-"+h.today+".log"), datetimePrefix, "<datetime>") {
		if !strings.Contains(line, "delete log file") {
			continue
		}
		line = strings.Replace(line, javaAbsPath(dir, ""), "<log_dir>/", 1)
		slash := strings.LastIndexByte(line, '/')
		deleted = append(deleted, line[:slash+1]+toTemplate(cal, now, line[slash+1:], false))
	}
	sort.Strings(deleted)
	equal(t, "delete lines", deleted, g["clear_keep3_log_lines"])
	if s := h.l.Stats(); s.Deleted != uint64(len(deleted)) || s.DeleteErrors != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestClearOldLogsDisabled(t *testing.T) {
	g := loadLoggerGolden(t)
	for _, cfg := range []Config{{KeepDays: 0, Rotation: true}, {KeepDays: 3, Rotation: false}} {
		dir := t.TempDir()
		cfg.Dir = dir
		h := newHarness(t, cfg, noon)
		cal, now := newCalendar(seoul), noon.UnixMilli()
		makeClearDir(t, dir, cal, now)
		h.l.runCleanup(now)
		got := []string{}
		for _, n := range templateNames(t, dir, cal, now) {
			if !strings.HasSuffix(n, "/") {
				got = append(got, n)
			}
		}
		key := "clear_keep0_remaining"
		if !cfg.Rotation {
			key = "clear_rotation_off_remaining"
		}
		equal(t, key, got, g[key])
	}
}

func TestJavaAbsPath(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for in, want := range map[string]string{
		"/var//log/":  "/var/log/x",
		"/":           "/x",
		"./logs":      wd + "/./logs/x",
		"logs//a/":    wd + "/logs/a/x",
		"":            wd + "/x",
		"/a/../b/./c": "/a/../b/./c/x",
	} {
		if got := javaAbsPath(in, "x"); got != want {
			t.Errorf("javaAbsPath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestJavaGoldenRequestLog(t *testing.T) {
	g := loadLoggerGolden(t)
	root := t.TempDir()
	dir, dir2 := filepath.Join(root, "logs-q"), filepath.Join(root, "logs-q2")
	h := newHarness(t, Config{Dir: dir, Rotation: true, KeepDays: 31}, noon)
	lookup := func(session int64) (string, string, bool) {
		if session == 77 {
			return "synthetic-user", "192.0.2.10", true
		}
		return "", "", false
	}
	l := h.l
	results := []string{
		"SERVER_DB_DELETE known=" + strconv.FormatBool(l.RequestCommand("SERVER_DB_DELETE", 77, lookup)),
		"REMOTE_CONTROL unknown=" + strconv.FormatBool(l.RequestCommand("REMOTE_CONTROL", 999, lookup)),
		"REMOTE_CONTROL_ALL known=" + strconv.FormatBool(l.RequestCommand("REMOTE_CONTROL_ALL", 77, lookup)),
		"TRANX_LOAD_TIME_GROUP known=" + strconv.FormatBool(l.RequestCommand("TRANX_LOAD_TIME_GROUP", 77, lookup)),
		"TRANX_LOAD_TIME_GROUP_V2 known=" + strconv.FormatBool(l.RequestCommand("TRANX_LOAD_TIME_GROUP_V2", 77, lookup)),
		"OBJECT_LIST_REAL_TIME known=" + strconv.FormatBool(l.RequestCommand("OBJECT_LIST_REAL_TIME", 77, lookup)),
	}
	l.RegisterRequestCommand("OBJECT_RESET_CACHE")
	l.RegisterRequestCommand("OBJECT_RESET_CACHE")
	l.RegisterRequestCommand("SERVER_DB_DELETE")
	l.RegisterRequestCommand("ACTIVE_QUERY_LIST")
	results = append(results,
		"OBJECT_RESET_CACHE known="+strconv.FormatBool(l.RequestCommand("OBJECT_RESET_CACHE", 77, lookup)),
		"ACTIVE_QUERY_LIST known="+strconv.FormatBool(l.RequestCommand("ACTIVE_QUERY_LIST", 77, lookup)))
	h.sync()
	l.SetConfig(Config{Dir: dir2, Rotation: true, KeepDays: 31})
	results = append(results, "SERVER_DB_DELETE after log_dir change known="+strconv.FormatBool(l.RequestCommand("SERVER_DB_DELETE", 77, lookup)))
	h.sync()
	equal(t, "add results", results, g["request_add_results"])
	equal(t, "files", names(t, dir, h.today), g["request_files"])
	javaLines := g["request_lines"]
	equal(t, "lines", h.lines(filepath.Join(dir, "request-"+h.today+".log"), timestampPrefix, "<timestamp>"), javaLines[:len(javaLines)-1])
	// D-L2: Java keeps writing the class-init log_dir; Go follows the live log_dir.
	if len(g["request_files_new_dir"]) != 0 {
		t.Fatalf("java golden no longer shows D-L2: %q", g["request_files_new_dir"])
	}
	equal(t, "new dir", h.lines(filepath.Join(dir2, "request-"+h.today+".log"), timestampPrefix, "<timestamp>"), javaLines[len(javaLines)-1:])
}

func TestDateBoundaryReopens(t *testing.T) {
	dir := t.TempDir()
	before := time.Date(2025, 10, 15, 23, 59, 59, 999e6, seoul)
	h := newHarness(t, Config{Dir: dir, Rotation: true, KeepDays: 31}, before)
	h.l.Println("last of day")
	h.l.RequestCommand("SERVER_DB_DELETE", 1, nil)
	h.clock.Add(time.Millisecond)
	h.l.Println("first of next day")
	h.l.RequestCommand("SERVER_DB_DELETE", 1, nil)
	h.sync()
	day1, err := os.ReadFile(filepath.Join(dir, "server-20251015.log"))
	if err != nil {
		t.Fatal(err)
	}
	day2, err := os.ReadFile(filepath.Join(dir, "server-20251016.log"))
	if err != nil {
		t.Fatal(err)
	}
	if string(day1) != "20251015 23:59:59 last of day\n" || string(day2) != "20251016 00:00:00 first of next day\n" {
		t.Fatalf("day1 %q day2 %q", day1, day2)
	}
	r1, _ := os.ReadFile(filepath.Join(dir, "request-20251015.log"))
	r2, _ := os.ReadFile(filepath.Join(dir, "request-20251016.log"))
	if string(r1) != "20251015 23:59:59.999 unknown1() SERVER_DB_DELETE\n" || string(r2) != "20251016 00:00:00.000 unknown1() SERVER_DB_DELETE\n" {
		t.Fatalf("request %q %q", r1, r2)
	}
}

// DST zone: the Scouter calendar keeps the 2000-01-01 offset, so the file date
// changes at 01:00 wall time during New York summer time (as Java does).
func TestFixedOffsetCalendarInDSTZone(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	clock := &fakeClock{t: time.Date(2025, 7, 2, 0, 59, 59, 0, ny)}
	l, err := New(Config{Dir: dir, Rotation: true}, Options{Location: ny, Now: clock.Now, Stdout: io.Discard, Stderr: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	l.Println("a")
	clock.Add(time.Second)
	l.Println("b")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	a, _ := os.ReadFile(filepath.Join(dir, "server-20250701.log"))
	b, _ := os.ReadFile(filepath.Join(dir, "server-20250702.log"))
	if string(a) != "20250701 23:59:59 a\n" || string(b) != "20250702 00:00:00 b\n" {
		t.Fatalf("%q %q", a, b)
	}
}

func TestCleanupTickerRunsOnOwner(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{t: noon}
	old := filepath.Join(dir, "server-20250101.log")
	if err := os.WriteFile(old, nil, 0o640); err != nil {
		t.Fatal(err)
	}
	l, err := New(Config{Dir: dir, Rotation: true, KeepDays: 3}, Options{Location: seoul, Now: clock.Now, Stdout: io.Discard, Stderr: io.Discard, CleanupInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	deadline := time.Now().Add(5 * time.Second)
	for l.Stats().Deleted == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old log not removed: %v", err)
	}
}

func TestCloseDrainsAndRejects(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t, Config{Dir: dir, Rotation: true, KeepDays: 31}, noon)
	for i := 0; i < 100; i++ {
		h.l.Println("line " + strconv.Itoa(i))
	}
	h.l.Stdout("stdout line")
	if err := h.l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.l.Close(); err != nil {
		t.Fatal("second close", err)
	}
	h.l.Println("after close")
	if h.l.Sync() {
		t.Fatal("sync after close")
	}
	s := h.l.Stats()
	if s.Enqueued != 101 || s.Written != 101 || s.DroppedClosed != 1 || s.Dropped != 0 {
		t.Fatalf("stats %+v", s)
	}
	b, _ := os.ReadFile(filepath.Join(dir, "server-"+h.today+".log"))
	if strings.Count(string(b), "\n") != 100 || h.stdout.String() != "stdout line\n" {
		t.Fatalf("file lines %d stdout %q", strings.Count(string(b), "\n"), h.stdout.String())
	}
}

// blockingWriter blocks every Write until release is closed.
type blockingWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}

func TestQueueFullDropsAndCounts(t *testing.T) {
	w := &blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
	l, err := New(Config{Dir: t.TempDir(), Rotation: true}, Options{Location: seoul, Now: func() time.Time { return noon }, Stdout: w, Stderr: io.Discard, QueueSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	l.Stdout("blocks the owner in flush")
	<-w.entered
	for i := 0; i < 10; i++ {
		l.Println("x")
	}
	close(w.release)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	s := l.Stats()
	if s.Enqueued != 5 || s.Dropped != 6 || s.Written != 5 || s.Remaining > 4 {
		t.Fatalf("stats %+v", s)
	}
}

func TestDiskFailureFallsBackToStdoutAndWarnsOnce(t *testing.T) {
	root := t.TempDir()
	notDir := filepath.Join(root, "file")
	if err := os.WriteFile(notDir, nil, 0o640); err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, Config{Dir: filepath.Join(notDir, "logs"), Rotation: true, KeepDays: 31}, noon)
	h.l.Println("one")
	h.l.Println("two")
	h.l.RequestCommand("SERVER_DB_DELETE", 1, nil)
	h.sync()
	if got := h.stdout.String(); got != "20251015 10:00:00 one\n20251015 10:00:00 two\n" {
		t.Fatalf("stdout %q", got)
	}
	if n := strings.Count(h.stderr.String(), "scouter-go logger: server log:"); n != 1 {
		t.Fatalf("server warnings %d: %q", n, h.stderr.String())
	}
	s := h.l.Stats()
	if s.StdoutFallback != 2 || s.RequestLost != 1 || s.FileErrors != 3 {
		t.Fatalf("stats %+v", s)
	}
	// Recovery after log_dir is fixed: the file is written and warnings re-arm.
	good := filepath.Join(root, "good")
	h.l.SetConfig(Config{Dir: good, Rotation: true, KeepDays: 31})
	h.clock.Add(2 * time.Second) // past the open retry wait
	h.l.Println("three")
	h.sync()
	b, err := os.ReadFile(filepath.Join(good, "server-20251015.log"))
	if err != nil || string(b) != "20251015 10:00:02 three\n" {
		t.Fatalf("%q %v", b, err)
	}
}

func TestIgnoreIDsAndTraceAreLive(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t, Config{Dir: dir, Rotation: true}, noon)
	h.l.PrintlnID("A1", "shown")
	h.l.SetConfig(Config{Dir: dir, Rotation: true, IgnoreIDs: []string{"A1"}, Trace: true})
	if !h.l.TraceEnabled() {
		t.Fatal("trace")
	}
	h.l.PrintlnID("A1", "hidden")
	h.l.Println("id-less lines are never filtered")
	h.sync()
	b, _ := os.ReadFile(filepath.Join(dir, "server-20251015.log"))
	if string(b) != "20251015 10:00:00 [A1] shown\n20251015 10:00:00 id-less lines are never filtered\n" {
		t.Fatalf("%q", b)
	}
	if h.stdout.String() != "20251015 10:00:00 id-less lines are never filtered\n" {
		t.Fatalf("stdout %q", h.stdout.String())
	}
	if got := h.l.Config().IgnoreIDs; len(got) != 1 || got[0] != "A1" {
		t.Fatal(got)
	}
}

func TestStdLogWriter(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t, Config{Dir: dir, Rotation: true}, noon)
	var stream bytes.Buffer
	lg := log.New(h.l.StdLogWriter(&stream), "", log.LstdFlags|log.Lmicroseconds)
	lg.Print("from std log")
	plain := log.New(h.l.StdLogWriter(nil), "", 0)
	plain.Print("2024/01/02 not a prefix without time? kept")
	plain.Print("multi\nline")
	h.sync()
	if !strings.HasSuffix(stream.String(), " from std log\n") || !regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{6} `).MatchString(stream.String()) {
		t.Fatalf("stream %q", stream.String())
	}
	b, _ := os.ReadFile(filepath.Join(dir, "server-20251015.log"))
	want := "20251015 10:00:00 from std log\n20251015 10:00:00 not a prefix without time? kept\n20251015 10:00:00 multi\nline\n"
	if string(b) != want {
		t.Fatalf("%q", b)
	}
	for in, out := range map[string]string{
		"2024/01/02 15:04:05 msg":        "msg",
		"2024/01/02 15:04:05.123456 msg": "msg",
		"2024/01/02 msg":                 "msg",
		"2024/1/02 msg":                  "2024/1/02 msg",
		"plain":                          "plain",
	} {
		if got := stripStdLogPrefix(in); got != out {
			t.Errorf("%q -> %q", in, got)
		}
	}
}

func TestNilLoggerIsNoop(t *testing.T) {
	var l *Logger
	l.Println("x")
	l.PrintlnID("a", "b")
	l.Trace("x")
	l.Stdout("x")
	l.SetConfig(Config{})
	l.RegisterRequestCommand("X")
	if l.RequestCommand("SERVER_DB_DELETE", 1, nil) || l.Sync() || l.Close() != nil || l.TraceEnabled() {
		t.Fatal("nil logger")
	}
	if _, err := New(Config{}, Options{}); err == nil {
		t.Fatal("missing location must fail")
	}
}

func TestConcurrentUseRace(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t, Config{Dir: dir, Rotation: true, KeepDays: 3}, noon)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				switch i % 5 {
				case 0:
					h.l.Println("p")
				case 1:
					h.l.PrintlnIDRate("ID"+strconv.Itoa(i%7), 1, "r")
				case 2:
					h.l.RequestCommand("SERVER_DB_DELETE", int64(g), nil)
				case 3:
					h.l.SetConfig(Config{Dir: filepath.Join(dir, strconv.Itoa(i%2)), Rotation: i%3 != 0, KeepDays: 3, Trace: i%4 == 0})
				case 4:
					h.l.RegisterRequestCommand("CMD" + strconv.Itoa(i))
					h.clock.Add(time.Millisecond)
				}
			}
		}(g)
	}
	wg.Wait()
	h.l.runCleanup(h.clock.Now().UnixMilli())
	if err := h.l.Close(); err != nil {
		t.Fatal(err)
	}
	s := h.l.Stats()
	if s.Enqueued != s.Written+s.StdoutFallback+s.RequestLost {
		t.Fatalf("accounting %+v", s)
	}
}

// runCleanup runs the hourly cleanup now, as of nowMs, on the owner goroutine.
func (l *Logger) runCleanup(nowMs int64) bool {
	return l.control(record{kind: kindCleanup, ms: nowMs})
}
