package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/ingest"
	"github.com/snowvil/scouter-go/internal/server"
	"github.com/snowvil/scouter-go/internal/storage"
)

// TestMain re-executes the test binary as the server when asked to, so the
// integration tests drive the real main() (flags, signals, exit codes).
func TestMain(m *testing.M) {
	if os.Getenv("SCOUTER_GO_RUN_MAIN") == "1" {
		os.Args = append([]string{"scouter-go"}, os.Args[1:]...)
		if stage := os.Getenv("SCOUTER_GO_TEST_STALL_STAGE"); stage != "" {
			// A shutdown stage ("store", or a Store.Close step such as
			// "store/sync") that never returns and sees no context, like a
			// write on a stalled disk (shutdown_test.go).
			stall := func(s string) {
				log.Printf("test: shutdown stage %q stalled", s)
				<-make(chan struct{})
			}
			server.ShutdownStageHook = func(s string) {
				if s == stage {
					stall(s)
				}
			}
			storage.CloseStageHook = func(s string) {
				if "store/"+s == stage {
					stall("store/" + s)
				}
			}
		}
		if kind := os.Getenv("SCOUTER_GO_TEST_STALL_WRITER"); kind != "" {
			ingest.StallWriterHook = func(k string) {
				if k == kind {
					log.Printf("test: %s writer stalled", k)
					<-make(chan struct{})
				}
			}
		}
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type child struct {
	t      *testing.T
	cmd    *exec.Cmd
	dir    string
	db     string
	logs   string
	ready  string
	stdin  io.WriteCloser
	mu     sync.Mutex
	stdout bytes.Buffer
	stderr bytes.Buffer
	waited chan error
}

func (s *child) output() (string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stdout.String(), s.stderr.String()
}

type lockedWriter struct {
	mu *sync.Mutex
	b  *bytes.Buffer
}

func (w lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

// startServer runs main() in a child process with cwd dir and extra flags.
func startServer(t *testing.T, dir string, env []string, extra ...string) *child {
	t.Helper()
	return startServerWith(t, dir, env, "", extra...)
}

// startServerWith runs the child through wrapper (e.g. nohup) when set; the
// wrapper must exec the server so the pid stays the same.
func startServerWith(t *testing.T, dir string, env []string, wrapper string, extra ...string) *child {
	t.Helper()
	return startServerOn(t, dir, filepath.Join(t.TempDir(), "db"), env, wrapper, extra...)
}

// startServerOn is startServerWith on the database directory db.
func startServerOn(t *testing.T, dir, db string, env []string, wrapper string, extra ...string) *child {
	t.Helper()
	ready := filepath.Join(t.TempDir(), "ready.json")
	logs := filepath.Join(t.TempDir(), "logs") // the Java Logger directory stays out of dir
	args := append([]string{"-db", db, "-log-dir", logs, "-udp", "127.0.0.1:0", "-tcp", "127.0.0.1:0", "-conf", "", "-ready-file", ready, "-stats-interval", "0", "-timezone", "UTC"}, extra...)
	cmd := exec.Command(os.Args[0], args...)
	if wrapper != "" {
		cmd = exec.Command(wrapper, append([]string{os.Args[0]}, args...)...)
	}
	cmd.Dir = dir
	cmd.Env = append(append(os.Environ(), "SCOUTER_GO_RUN_MAIN=1"), env...)
	s := &child{t: t, cmd: cmd, dir: dir, db: db, logs: logs, ready: ready, waited: make(chan error, 1)}
	cmd.Stdout = lockedWriter{&s.mu, &s.stdout}
	cmd.Stderr = lockedWriter{&s.mu, &s.stderr}
	var err error
	if s.stdin, err = cmd.StdinPipe(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { s.waited <- cmd.Wait() }()
	t.Cleanup(func() {
		select {
		case <-s.waited:
		default:
			_ = cmd.Process.Kill()
			<-s.waited
		}
	})
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			return s
		}
		select {
		case err := <-s.waited:
			s.waited <- err
			return s // exited during start; the caller checks the result
		default:
		}
		if time.Now().After(deadline) {
			_, e := s.output()
			t.Fatalf("server not ready\n%s", e)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// wait returns the exit code within d, failing the test on a timeout.
func (s *child) wait(d time.Duration) int {
	s.t.Helper()
	select {
	case err := <-s.waited:
		s.waited <- err
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return exit.ExitCode()
		}
		if err != nil {
			s.t.Fatal(err)
		}
		return 0
	case <-time.After(d):
		_, e := s.output()
		s.t.Fatalf("server still running after %v\n%s", d, e)
	}
	return -1
}

// exitFile waits for <pid>.scouter. The file is created after the signal
// handler is installed, so it also marks the point from which SIGTERM is
// handled (the ready file is written earlier, by server.Open).
func (s *child) exitFile() string {
	s.t.Helper()
	path := filepath.Join(s.dir, strconv.Itoa(s.cmd.Process.Pid)+".scouter")
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return path
		}
		if time.Now().After(deadline) {
			_, e := s.output()
			s.t.Fatalf("exit file %s not created\n%s", path, e)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *child) assertLockReleased() {
	s.t.Helper()
	if _, err := os.Stat(filepath.Join(s.db, "lock.dat")); !errors.Is(err, fs.ErrNotExist) {
		s.t.Fatalf("lock.dat not released: %v", err)
	}
}

func TestExitFileDeletionShutsDown(t *testing.T) {
	s := startServer(t, t.TempDir(), nil)
	path := s.exitFile()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if code := s.wait(10 * time.Second); code != 0 {
		_, e := s.output()
		t.Fatalf("exit code %d\n%s", code, e)
	}
	_, logs := s.output()
	if !strings.Contains(logs, "was deleted; shutting down") || !strings.Contains(logs, "ingest=") {
		t.Fatalf("graceful shutdown log missing:\n%s", logs)
	}
	// The Java server log gets the standard log lines (timestamp rewritten)
	// up to the final shutdown report: the logger is closed last.
	files, _ := filepath.Glob(filepath.Join(s.logs, "server-*.log"))
	if len(files) != 1 {
		t.Fatalf("server log files %v", files)
	}
	b, err := os.ReadFile(files[0])
	if err != nil || !strings.Contains(string(b), " Scouter Go listening UDP=") || !strings.Contains(string(b), " ingest={") || !strings.Contains(string(b), " sqltable={") {
		t.Fatalf("server log: %v\n%s", err, b)
	}
	s.assertLockReleased()
}

func TestExitFileRemovedOnSignalAndOptional(t *testing.T) {
	s := startServer(t, t.TempDir(), nil)
	path := s.exitFile()
	if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if code := s.wait(10 * time.Second); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("exit file left behind: %v", err)
	}
	s.assertLockReleased()

	dir := t.TempDir()
	s = startServer(t, dir, nil, "-exit-file=false")
	// No marker exists without the exit file: give main the moment between
	// server.Open (ready file) and signal.NotifyContext.
	time.Sleep(500 * time.Millisecond)
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("-exit-file=false created %v", entries)
	}
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	if code := s.wait(10 * time.Second); code != 0 {
		t.Fatalf("exit code %d", code)
	}
}

// TestExitFileUncreatableExits1: like Java System.exit(1) when neither the
// working directory nor user.home accepts the file; the DB lock is released.
func TestExitFileUncreatableExits1(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir, home := t.TempDir(), t.TempDir()
	for _, d := range []string{dir, home} {
		if err := os.Chmod(d, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(d, 0o700) })
	}
	s := startServer(t, dir, []string{"HOME=" + home})
	if code := s.wait(10 * time.Second); code != 1 {
		t.Fatalf("exit code %d", code)
	}
	_, logs := s.output()
	if !strings.Contains(logs, "exit file:") {
		t.Fatal(logs)
	}
	s.assertLockReleased()
	// The startup failure goes through main's fatal path: the server log is
	// flushed and closed (the runtime shutdown report is in it).
	files, _ := filepath.Glob(filepath.Join(s.logs, "server-*.log"))
	if len(files) != 1 {
		t.Fatalf("server log files %v", files)
	}
	if b, err := os.ReadFile(files[0]); err != nil || !strings.Contains(string(b), " exit file: ") || !strings.Contains(string(b), " sqltable={") {
		t.Fatalf("server log: %v\n%s", err, b)
	}
}

// TestSIGQUITDumpsAndContinues: like the JVM thread dump, SIGQUIT writes
// every goroutine's stack to stderr and the server keeps running; a later
// SIGTERM still shuts down gracefully (exit file removed, lock released).
func TestSIGQUITDumpsAndContinues(t *testing.T) {
	for _, mode := range [][]string{nil, {"-console", "-noansi"}} {
		s := startServer(t, t.TempDir(), nil, mode...)
		path := ""
		if mode == nil {
			path = s.exitFile()
		} else {
			time.Sleep(300 * time.Millisecond) // signal handlers are installed before Open; give the console its start
		}
		if err := s.cmd.Process.Signal(syscall.SIGQUIT); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			_, e := s.output()
			if strings.Contains(e, "SIGQUIT: goroutine dump follows") && strings.Contains(e, "goroutine 1 [") && strings.Contains(e, "main.main()") {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%v: no goroutine dump\n%s", mode, e)
			}
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(300 * time.Millisecond)
		select {
		case err := <-s.waited:
			s.waited <- err
			_, e := s.output()
			t.Fatalf("%v: SIGQUIT stopped the server: %v\n%s", mode, err, e)
		default:
		}
		if mode != nil {
			if _, err := io.WriteString(s.stdin, "objtype\n"); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if code := s.wait(10 * time.Second); code != 0 {
			_, e := s.output()
			t.Fatalf("%v: exit code %d\n%s", mode, code, e)
		}
		if path != "" {
			if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("exit file left behind: %v", err)
			}
		}
		if _, logs := s.output(); !strings.Contains(logs, "ingest=") {
			t.Fatalf("%v: no graceful shutdown:\n%s", mode, logs)
		}
		s.assertLockReleased()
	}
}

func TestConsoleQuitShutsDown(t *testing.T) {
	dir := t.TempDir()
	s := startServer(t, dir, nil, "-console", "-noansi")
	if _, err := io.WriteString(s.stdin, "objtype\nhelp\nquit\n"); err != nil {
		t.Fatal(err)
	}
	if code := s.wait(10 * time.Second); code != 0 {
		_, e := s.output()
		t.Fatalf("exit code %d\n%s", code, e)
	}
	out, logs := s.output()
	if !strings.Contains(out, "scouter> ") || !strings.Contains(out, "\tdebug queue\n") || !strings.HasSuffix(out, "bye bye!!\n") || strings.Contains(out, "\x1b[") {
		t.Fatalf("stdout %q", out)
	}
	if !strings.Contains(logs, "ingest=") {
		t.Fatalf("no graceful shutdown:\n%s", logs)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("console mode created %v", entries)
	}
	s.assertLockReleased()
}

// TestConsoleEOFKeepsServer: stdin EOF ends the console only; the server keeps
// running (no busy loop) until a signal stops it.
func TestConsoleEOFKeepsServer(t *testing.T) {
	s := startServer(t, t.TempDir(), nil, "-console")
	s.stdin.Close()
	time.Sleep(1500 * time.Millisecond)
	select {
	case err := <-s.waited:
		s.waited <- err
		_, e := s.output()
		t.Fatalf("server stopped on EOF: %v\n%s", err, e)
	default:
	}
	_, logs := s.output()
	// Count the message itself: t.TempDir() paths in other log lines (the
	// GeoIP S145 line) contain the test name, which contains "EOF".
	if strings.Count(logs, "console: input reached EOF") != 1 {
		t.Fatalf("EOF log:\n%s", logs)
	}
	start := time.Now()
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	if code := s.wait(10 * time.Second); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("shutdown took %v", d)
	}
	s.assertLockReleased()
}

// TestSIGHUPGracefulUnlessIgnored: SIGHUP runs the same graceful shutdown as
// SIGTERM (Java shutdown hook); under nohup (SIGHUP ignored) it stays ignored.
func TestSIGHUPGracefulUnlessIgnored(t *testing.T) {
	s := startServer(t, t.TempDir(), nil)
	path := s.exitFile()
	if err := s.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	if code := s.wait(10 * time.Second); code != 0 {
		_, e := s.output()
		t.Fatalf("SIGHUP exit code %d\n%s", code, e)
	}
	if _, logs := s.output(); !strings.Contains(logs, "ingest=") {
		t.Fatalf("no graceful shutdown:\n%s", logs)
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("exit file left behind: %v", err)
	}
	s.assertLockReleased()

	nohup, err := exec.LookPath("nohup")
	if err != nil {
		t.Skip("nohup unavailable; ignored SIGHUP not run")
	}
	s = startServerWith(t, t.TempDir(), nil, nohup)
	s.exitFile()
	if err := s.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	select {
	case err := <-s.waited:
		s.waited <- err
		_, e := s.output()
		t.Fatalf("ignored SIGHUP stopped the server: %v\n%s", err, e)
	default:
	}
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	if code := s.wait(10 * time.Second); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	s.assertLockReleased()
}

// TestShutdownSignals checks the subscribed set in this (not ignored)
// process. The ignored case runs under nohup in a child process
// (TestSIGHUPGracefulUnlessIgnored): signal.Ignore here would stay recorded
// for the test process and be inherited by later children.
func TestShutdownSignals(t *testing.T) {
	if signal.Ignored(syscall.SIGHUP) {
		t.Skip("test process runs with SIGHUP ignored")
	}
	sigs := shutdownSignals()
	if len(sigs) != 3 || sigs[0] != os.Interrupt || sigs[1] != syscall.SIGTERM || sigs[2] != syscall.SIGHUP {
		t.Fatalf("signals %v", sigs)
	}
}

func TestCreateExitFileFallback(t *testing.T) {
	dir, home := t.TempDir(), t.TempDir()
	path, err := createExitFile(dir, home, 42)
	if err != nil || path != filepath.Join(dir, "42.scouter") {
		t.Fatal(path, err)
	}
	// An existing file is reused (File.createNewFile returns false).
	if again, err := createExitFile(dir, home, 42); err != nil || again != path {
		t.Fatal(again, err)
	}
	missing := filepath.Join(dir, "missing")
	path, err = createExitFile(missing, home, 43)
	if err != nil || path != filepath.Join(home, "43.scouter.run") {
		t.Fatal(path, err)
	}
	if _, err = createExitFile(missing, filepath.Join(home, "missing"), 44); err == nil {
		t.Fatal("both locations missing must fail")
	}
	if _, err = createExitFile(missing, "", 45); err == nil {
		t.Fatal("no home must fail")
	}
}

func TestWatchExitFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "1.scouter")
	if err := touch(path); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	reasons := make(chan string, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchExitFile(ctx, path, 5*time.Millisecond, func(r string) { reasons <- r })
	}()
	time.Sleep(20 * time.Millisecond)
	select {
	case r := <-reasons:
		t.Fatalf("early %s", r)
	default:
	}
	removeExitFile(path)
	if r := <-reasons; r != "was deleted" {
		t.Fatal(r)
	}
	<-done
	cancel()
	removeExitFile(path) // already gone: no error

	// ctx ends the watcher without a callback.
	if err := touch(path); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	done = make(chan struct{})
	go func() {
		defer close(done)
		watchExitFile(ctx, path, time.Millisecond, func(string) { t.Error("callback after cancel") })
	}()
	cancel()
	<-done
}

func TestANSIEnabled(t *testing.T) {
	for _, c := range []struct {
		goos         string
		ansi, noansi bool
		want         bool
	}{{"linux", false, false, true}, {"windows", false, false, false}, {"linux", false, true, false}, {"windows", true, false, true}, {"linux", true, true, true}} {
		if got := ansiEnabled(c.goos, c.ansi, c.noansi); got != c.want {
			t.Errorf("%+v: %v", c, got)
		}
	}
}
