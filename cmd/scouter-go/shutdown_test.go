package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/server"
	"github.com/snowvil/scouter-go/internal/storage"
)

// stall makes the child's shutdown stage never return (TestMain).
func stall(stage string) []string { return []string{"SCOUTER_GO_TEST_STALL_STAGE=" + stage} }

func (s *child) waitLog(want string, d time.Duration) {
	s.t.Helper()
	deadline := time.Now().Add(d)
	for {
		if _, e := s.output(); strings.Contains(e, want) {
			return
		}
		if time.Now().After(deadline) {
			_, e := s.output()
			s.t.Fatalf("log %q not seen\n%s", want, e)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *child) assertNoCleanMarker() {
	s.t.Helper()
	// A forced exit skips the race detector's exit status; its report is
	// still on stderr (go test -race).
	if _, e := s.output(); strings.Contains(e, "DATA RACE") {
		s.t.Fatalf("data race in the child\n%s", e)
	}
	if _, err := os.Stat(filepath.Join(s.db, ".go-clean")); !errors.Is(err, fs.ErrNotExist) {
		s.t.Fatalf(".go-clean after a forced exit: %v", err)
	}
}

// A stage stuck in a call that sees no context (a hung disk) no longer holds
// the process until the stop script's SIGKILL: past --shutdown-timeout the
// stage and the queues are logged and the process exits 3 without the clean
// marker, so the next start validates the indexes.
func TestShutdownBudgetEndsStalledStage(t *testing.T) {
	for _, stage := range []string{"background", "ingest writers", "store", "store/sync"} {
		t.Run(stage, func(t *testing.T) {
			s := startServer(t, t.TempDir(), stall(stage), "-shutdown-timeout", "1s")
			exitFile := s.exitFile()
			start := time.Now()
			if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			code := s.wait(15 * time.Second)
			_, logs := s.output()
			if code != exitShutdownTimeout {
				t.Fatalf("exit code %d\n%s", code, logs)
			}
			if d := time.Since(start); d > 5*time.Second {
				t.Fatalf("forced exit took %v", d)
			}
			for _, want := range []string{"shutdown: SIGTERM received", "shutdown: budget 1s exceeded", `stage="` + stage + `"`, "clientCommands=0 agentOrphans=0 agentRelayBytes=0", "ingestPending=", "xlogQueue=0 profileQueue=0 marker=absent"} {
				if !strings.Contains(logs, want) {
					t.Fatalf("missing %q\n%s", want, logs)
				}
			}
			s.assertNoCleanMarker()
			// The forced exit skips the run mode's stop; its cleanup still
			// deletes the exit file.
			if _, err := os.Stat(exitFile); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("exit file left behind: %v", err)
			}
		})
	}
}

// A second shutdown signal ends the process at once, with one log line.
func TestSecondSignalExitsAtOnce(t *testing.T) {
	s := startServer(t, t.TempDir(), stall("store"), "-shutdown-timeout", "1m")
	s.exitFile()
	if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	first := time.Now()
	s.waitLog(`shutdown stage "store" stalled`, 15*time.Second)
	time.Sleep(time.Until(first.Add(signalDupWindow + 100*time.Millisecond)))
	start := time.Now()
	if err := s.cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	code := s.wait(5 * time.Second)
	_, logs := s.output()
	if code != 128+int(syscall.SIGINT) {
		t.Fatalf("exit code %d\n%s", code, logs)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("second signal exit took %v", d)
	}
	if !strings.Contains(logs, `shutdown: second signal SIGINT; exiting now stage="store"`) {
		t.Fatalf("no second-signal line\n%s", logs)
	}
	s.assertNoCleanMarker()
}

// The normal shutdown is unchanged: exit 0 and the clean marker written.
func TestCleanShutdownWritesMarker(t *testing.T) {
	s := startServer(t, t.TempDir(), nil, "-shutdown-timeout", "20s")
	s.exitFile()
	if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	code := s.wait(15 * time.Second)
	_, logs := s.output()
	if code != 0 || strings.Contains(logs, "budget") {
		t.Fatalf("exit code %d\n%s", code, logs)
	}
	if _, err := os.Stat(filepath.Join(s.db, ".go-clean")); err != nil {
		t.Fatalf("no clean marker: %v\n%s", err, logs)
	}
	s.assertLockReleased()
}

func (s *child) endpoints() server.Endpoints {
	s.t.Helper()
	b, err := os.ReadFile(s.ready)
	if err != nil {
		s.t.Fatal(err)
	}
	var e server.Endpoints
	if err = json.Unmarshal(b, &e); err != nil {
		s.t.Fatal(err)
	}
	return e
}

func (s *child) indexOpens() map[string]storage.IndexOpenStats {
	s.t.Helper()
	resp, err := http.Get("http://" + s.endpoints().HTTP + "/health")
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()
	var h struct {
		IndexOpens map[string]storage.IndexOpenStats `json:"indexOpens"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&h); err != nil {
		s.t.Fatal(err)
	}
	return h.IndexOpens
}

func (s *child) sendXLog(txid int64) int64 {
	s.t.Helper()
	now := time.Now().UnixMilli()
	payload, err := protocol.EncodePack(&protocol.XLogPack{EndTime: now, ObjHash: 77, Service: 5, TxID: txid, Elapsed: 10}, protocol.Limits{})
	if err != nil {
		s.t.Fatal(err)
	}
	conn, err := net.Dial("udp", s.endpoints().UDP)
	if err != nil {
		s.t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write(append([]byte("CAFE"), payload...)); err != nil {
		s.t.Fatal(err)
	}
	return now
}

// xlogCount is the record count in the time index header.
func xlogCount(t *testing.T, db string, now int64) uint32 {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(db, storage.JavaDate(now, time.UTC), "xlog", "xlog_tim.hfile"))
	if err != nil || len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint32(b[4:8])
}

// After a forced exit (budget expired, no clean marker) the next start
// opens the indexes from the crash checkpoint the periodic sync left (S4),
// keeps the records synced before the exit and closes cleanly again.
func TestRestartAfterForcedShutdownRecovers(t *testing.T) {
	for _, stage := range []string{"store", "store/index close"} {
		t.Run(stage, func(t *testing.T) { restartAfterForcedShutdown(t, stage) })
	}
}

func restartAfterForcedShutdown(t *testing.T, stage string) {
	db := filepath.Join(t.TempDir(), "db")
	flags := []string{"-http", "127.0.0.1:0", "-sync-interval", "50ms"}
	a := startServerOn(t, t.TempDir(), db, stall(stage), "", append(flags, "-shutdown-timeout", "1s")...)
	a.exitFile()
	now := a.sendXLog(1)
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := os.Stat(filepath.Join(db, ".go-checkpoint"))
		if xlogCount(t, db, now) >= 1 && err == nil {
			break
		}
		if time.Now().After(deadline) {
			_, e := a.output()
			t.Fatalf("XLog not synced or no crash checkpoint (%v)\n%s", err, e)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := a.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if code := a.wait(15 * time.Second); code != exitShutdownTimeout {
		_, e := a.output()
		t.Fatalf("exit code %d\n%s", code, e)
	}
	a.assertNoCleanMarker()
	// The header count of the time index per XLog is an index detail; the
	// second run must add as many as the first.
	before := xlogCount(t, db, now)

	b := startServerOn(t, t.TempDir(), db, nil, "", append(flags, "-shutdown-timeout", "20s")...)
	b.exitFile()
	if st := b.indexOpens()["store"]; st.CleanMarker {
		t.Fatalf("clean marker after a forced exit: %+v", st)
	}
	b.sendXLog(2) // opens the day's XLog indexes again
	deadline = time.Now().Add(10 * time.Second)
	for {
		st := b.indexOpens()["store"]
		if st.Checkpointed > 0 {
			if st.Failed != 0 || st.CheckpointRejected != 0 {
				t.Fatalf("index opens %+v", st)
			}
			break
		}
		if time.Now().After(deadline) {
			_, e := b.output()
			t.Fatalf("no checkpointed open: %+v\n%s", st, e)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := b.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if code := b.wait(15 * time.Second); code != 0 {
		_, e := b.output()
		t.Fatalf("exit code %d\n%s", code, e)
	}
	if _, err := os.Stat(filepath.Join(db, ".go-clean")); err != nil {
		t.Fatalf("no clean marker after the clean restart: %v", err)
	}
	if n := xlogCount(t, db, now); before == 0 || n != 2*before {
		t.Fatalf("time index count %d, want twice %d (one XLog before the forced exit, one after)", n, before)
	}
}

// The budget also runs when no signal began the shutdown: exit file
// deletion, console quit, and the runtime's own teardown after the writer
// heartbeat fails.
func TestBudgetWithoutSignal(t *testing.T) {
	cases := []struct {
		name  string
		extra []string
		stop  func(s *child)
	}{
		{"exit file", nil, func(s *child) {
			if err := os.Remove(s.exitFile()); err != nil {
				s.t.Fatal(err)
			}
		}},
		{"console quit", []string{"-console", "-noansi"}, func(s *child) {
			if _, err := io.WriteString(s.stdin, "quit\n"); err != nil {
				s.t.Fatal(err)
			}
		}},
		{"heartbeat failure", nil, func(s *child) {
			s.exitFile()
			if err := os.Remove(filepath.Join(s.db, "lock.dat")); err != nil {
				s.t.Fatal(err)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := startServer(t, t.TempDir(), stall("store"), append([]string{"-shutdown-timeout", "1s"}, c.extra...)...)
			c.stop(s)
			code := s.wait(15 * time.Second)
			_, logs := s.output()
			if code != exitShutdownTimeout || !strings.Contains(logs, "shutdown: budget 1s exceeded") || !strings.Contains(logs, `stage="store"`) {
				t.Fatalf("exit code %d\n%s", code, logs)
			}
			s.assertNoCleanMarker()
		})
	}
}

// The first signal after another trigger began the shutdown is graceful;
// the next one, past the repeat window, exits at once.
func TestFirstSignalAfterExitFileIsGraceful(t *testing.T) {
	s := startServer(t, t.TempDir(), stall("store"), "-shutdown-timeout", "1m")
	if err := os.Remove(s.exitFile()); err != nil {
		t.Fatal(err)
	}
	s.waitLog(`shutdown stage "store" stalled`, 15*time.Second)
	if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	s.waitLog("shutdown: SIGTERM received", 5*time.Second)
	time.Sleep(signalDupWindow + 100*time.Millisecond)
	select {
	case err := <-s.waited:
		s.waited <- err
		_, e := s.output()
		t.Fatalf("first SIGTERM ended the process: %v\n%s", err, e)
	default:
	}
	if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if code := s.wait(5 * time.Second); code != 128+int(syscall.SIGTERM) {
		_, e := s.output()
		t.Fatalf("exit code %d\n%s", code, e)
	}
	s.assertNoCleanMarker()
}

// --shutdown-timeout 0 waits for a stalled stage; a later SIGTERM still
// ends it.
func TestZeroBudgetWaits(t *testing.T) {
	s := startServer(t, t.TempDir(), stall("store"), "-shutdown-timeout", "0")
	s.exitFile()
	if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	s.waitLog("no shutdown budget", 5*time.Second)
	s.waitLog(`shutdown stage "store" stalled`, 15*time.Second)
	time.Sleep(2 * time.Second)
	select {
	case err := <-s.waited:
		s.waited <- err
		_, e := s.output()
		t.Fatalf("budget 0 ended the process: %v\n%s", err, e)
	default:
	}
	if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if code := s.wait(5 * time.Second); code != 128+int(syscall.SIGTERM) {
		_, e := s.output()
		t.Fatalf("exit code %d\n%s", code, e)
	}
}

// An ingest writer stuck in a write that ignores its context holds the
// shutdown only for its close deadline: the counters and the store still
// close, but without the clean-shutdown marker.
func TestStuckWriterStillClosesStore(t *testing.T) {
	s := startServer(t, t.TempDir(), []string{"SCOUTER_GO_TEST_STALL_WRITER=object"}, "-shutdown-timeout", "20s")
	s.exitFile()
	payload, err := protocol.EncodePack(&protocol.ObjectPack{ObjName: "/stuck", ObjType: "java", Version: "1", Tags: protocol.Value{Type: protocol.ValueMap}}, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("udp", s.endpoints().UDP)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Write(append([]byte("CAFE"), payload...)); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	s.waitLog("test: object writer stalled", 10*time.Second)
	if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	code := s.wait(15 * time.Second)
	_, logs := s.output()
	if code != 0 {
		t.Fatalf("exit code %d\n%s", code, logs)
	}
	for _, want := range []string{"object writer still in a write", "the clean-shutdown marker is not written", "storage writers closed"} {
		if !strings.Contains(logs, want) {
			t.Fatalf("missing %q\n%s", want, logs)
		}
	}
	s.assertNoCleanMarker()
}
