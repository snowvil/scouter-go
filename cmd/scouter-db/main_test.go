package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

func TestCommandLifecycle(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	src, dst := filepath.Join(tmp, "src"), filepath.Join(tmp, "dst")
	s, err := storage.Open(storage.Config{Directory: src, Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.PutXLog(&protocol.XLogPack{TxID: 1, EndTime: 1767193200125, ObjHash: 42}); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	cli := func(args ...string) (int, string, string) {
		var out, errOut bytes.Buffer
		code := run(context.Background(), args, &out, &errOut)
		return code, out.String(), errOut.String()
	}
	if code, _, _ := cli(); code != 2 {
		t.Fatalf("no args %d", code)
	}
	if code, _, msg := cli("help"); code != 0 || !strings.Contains(msg, "scouter-db migrate") {
		t.Fatalf("help %d %s", code, msg)
	}
	if code, _, _ := cli("bogus"); code != 2 {
		t.Fatalf("unknown command %d", code)
	}
	if code, _, _ := cli("inspect"); code != 2 {
		t.Fatalf("missing --db %d", code)
	}
	if code, _, _ := cli("inspect", "--db", src, "--timezone", "No/Such_Zone"); code != 2 {
		t.Fatalf("bad timezone %d", code)
	}
	if code, _, msg := cli("inspect", "--db", src, "--report", filepath.Join(src, "r.json")); code != 2 || !strings.Contains(msg, "inside DB") {
		t.Fatalf("report inside DB %d %s", code, msg)
	}
	code, out, _ := cli("inspect", "--db", src)
	var rep struct {
		Summary struct{ Errors int } `json:"summary"`
	}
	if code != 0 || json.Unmarshal([]byte(out), &rep) != nil {
		t.Fatalf("inspect %d %s", code, out)
	}
	report := filepath.Join(tmp, "dry.json")
	if code, _, msg := cli("migrate", "--src", src, "--dst", dst, "--dry-run", "--report", report); code != 0 || !strings.Contains(msg, "status=planned") {
		t.Fatalf("dry-run %d %s", code, msg)
	}
	if _, err = os.Stat(dst); !os.IsNotExist(err) {
		t.Fatal("dry-run created dst")
	}
	if code, _, msg := cli("migrate", "--src", src, "--dst", dst, "--dry-run", "--accept-recoverable"); code != 0 || !strings.Contains(msg, "accepted=0") {
		t.Fatalf("accept-recoverable dry-run %d %s", code, msg)
	}
	if code, _, msg := cli("migrate", "--src", src, "--dst", dst, "--report", filepath.Join(tmp, "m.json")); code != 0 {
		t.Fatalf("migrate %d %s", code, msg)
	}
	if code, _, msg := cli("verify", "--src", src, "--dst", dst); code != 0 || !strings.Contains(msg, "status=ok") {
		t.Fatalf("verify %d %s", code, msg)
	}
	if code, _, msg := cli("rollback", "--dst", dst, "--report", filepath.Join(src, "rb.json")); code != 2 || !strings.Contains(msg, "inside DB") {
		t.Fatalf("rollback report inside src %d %s", code, msg)
	}
	if code, _, msg := cli("rollback", "--dst", dst); code != 0 || !strings.Contains(msg, "status=rolled-back") {
		t.Fatalf("rollback %d %s", code, msg)
	}
	// A corrupt DB exits 1 with the report still emitted.
	if err = os.Truncate(filepath.Join(src, "20260101", "xlog", "xlog_tim.kfile"), 3); err != nil {
		t.Fatal(err)
	}
	if code, out, _ := cli("inspect", "--db", src); code != 1 || json.Unmarshal([]byte(out), &rep) != nil || rep.Summary.Errors == 0 {
		t.Fatalf("corrupt inspect %d %s", code, out)
	}
}

func TestCommandIncremental(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	src, dst := filepath.Join(tmp, "src"), filepath.Join(tmp, "dst")
	s, err := storage.Open(storage.Config{Directory: src, Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.PutXLog(&protocol.XLogPack{TxID: 1, EndTime: 1767193200125, ObjHash: 42}); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	// A running Java server keeps lock.dat fresh.
	lock := filepath.Join(src, "lock.dat")
	if err = os.WriteFile(lock, nil, 0600); err != nil {
		t.Fatal(err)
	}
	cli := func(args ...string) (int, string, string) {
		var out, errOut bytes.Buffer
		code := run(context.Background(), args, &out, &errOut)
		return code, out.String(), errOut.String()
	}
	for _, bad := range [][]string{
		{"--precopy", "--finish"}, {"--precopy", "--accept-recoverable"}, {"--finish", "--fresh"},
	} {
		if code, _, _ := cli(append([]string{"migrate", "--src", src, "--dst", dst}, bad...)...); code != 2 {
			t.Fatalf("%v accepted: %d", bad, code)
		}
	}
	if code, out, msg := cli("migrate", "--precopy", "--dry-run", "--src", src, "--dst", dst); code != 0 || !strings.Contains(msg, "status=planned") || !strings.Contains(out, `"phase": "precopy"`) {
		t.Fatalf("precopy dry-run %d %s %s", code, msg, out)
	}
	if code, _, msg := cli("migrate", "--precopy", "--src", src, "--dst", dst, "--report", filepath.Join(src, "p.json")); code != 2 || !strings.Contains(msg, "inside DB") {
		t.Fatalf("precopy report inside src %d %s", code, msg)
	}
	code, out, msg := cli("migrate", "--precopy", "--src", src, "--dst", dst)
	var res struct {
		Status string `json:"status"`
		State  string `json:"state"`
		Round  struct {
			JavaActive bool `json:"java_active"`
		} `json:"round"`
	}
	if code != 0 || json.Unmarshal([]byte(out), &res) != nil || res.Status != "precopied" || !res.Round.JavaActive || !strings.Contains(msg, "java_active=true") {
		t.Fatalf("precopy %d %s %s", code, out, msg)
	}
	if _, err = os.Stat(res.State); err != nil {
		t.Fatalf("state %s: %v", res.State, err)
	}
	if code, _, msg := cli("migrate", "--finish", "--src", src, "--dst", dst); code != 1 || !strings.Contains(msg, "live writer") {
		t.Fatalf("finish beside Java %d %s", code, msg)
	}
	// Stale by the 5s rule, but Java may still be flushing: needs confirmation.
	recent := time.Now().Add(-10 * time.Second)
	if err = os.Chtimes(lock, recent, recent); err != nil {
		t.Fatal(err)
	}
	if code, _, msg := cli("migrate", "--finish", "--src", src, "--dst", dst); code != 1 || !strings.Contains(msg, "--java-stopped") {
		t.Fatalf("finish right after the heartbeat stopped %d %s", code, msg)
	}
	if code, _, msg := cli("migrate", "--precopy", "--src", src, "--dst", dst, "--report", filepath.Join(dst+".partial", "r.json")); code != 2 || !strings.Contains(msg, "inside DB") {
		t.Fatalf("report inside staging %d %s", code, msg)
	}
	if code, _, _ := cli("migrate", "--precopy", "--rehash-staging", "--src", src, "--dst", dst); code != 2 {
		t.Fatalf("--rehash-staging without --finish %d", code)
	}
	if code, _, msg := cli("migrate", "--finish", "--dry-run", "--java-stopped", "--src", src, "--dst", dst); code != 0 || !strings.Contains(msg, "status=planned") {
		t.Fatalf("finish dry-run %d %s", code, msg)
	}
	if code, _, msg := cli("migrate", "--finish", "--java-stopped", "--rehash-staging", "--src", src, "--dst", dst); code != 0 || !strings.Contains(msg, "status=completed") || !strings.Contains(msg, "finish: copied=") {
		t.Fatalf("finish %d %s", code, msg)
	}
	if code, _, msg := cli("verify", "--src", src, "--dst", dst); code != 0 || !strings.Contains(msg, "status=ok") {
		t.Fatalf("verify %d %s", code, msg)
	}
}
