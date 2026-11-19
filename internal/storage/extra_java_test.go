package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func TestJavaExtraStorageInterop(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	source := os.Getenv("SCOUTER_SOURCE")
	if source == "" {
		source = filepath.Join(repo, "..", "scouter")
	}
	if _, err = os.Stat(filepath.Join(source, "scouter.server")); err != nil {
		t.Skip("pinned source unavailable; actual extra DB oracle not run")
	}
	work := t.TempDir()
	javaDB, goDB := filepath.Join(work, "java-db"), filepath.Join(work, "go-db")
	oracle := func(mode, dir string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		command := exec.CommandContext(ctx, "python3", filepath.Join(repo, "scripts/java-storage.py"), mode, dir, "--source", source)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("actual original extra oracle %s: %v\n%s", mode, err, output)
		}
		t.Log(strings.TrimSpace(string(output)))
	}
	oracle("--generate-extra", javaDB)
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	golden := func(name string) []byte {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(javaDB, "oracle-extra", name+".bin"))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	input := func(name string) protocol.Pack {
		t.Helper()
		p, err := protocol.DecodePack(golden(name), protocol.Limits{})
		if err != nil {
			t.Fatal(name, err)
		}
		return p
	}
	comparePack := func(want protocol.Pack, got protocol.Pack) {
		t.Helper()
		wb, err := protocol.EncodePack(want, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		gb, err := protocol.EncodePack(got, protocol.Limits{})
		if err != nil || !bytes.Equal(wb, gb) {
			t.Fatalf("Java->Go fields differ type=%d err=%v", want.Type(), err)
		}
	}
	verifyGo := func(root string) {
		t.Helper()
		s, err := Open(Config{Directory: root, Location: loc})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		ctx := context.Background()
		statuses, err := s.StatusesContext(ctx, "20260101", oracleTime-1, oracleTime+999)
		if err != nil || len(statuses) != 1 {
			t.Fatalf("Status %+v %v", statuses, err)
		}
		comparePack(input("status"), statuses[0])
		sums, err := s.SummariesContext(ctx, "20260101", oracleTime-1, oracleTime+999, 1)
		if err != nil || len(sums) != 1 {
			t.Fatalf("Summary %+v %v", sums, err)
		}
		comparePack(input("summary"), sums[0])
		alerts, err := s.AlertsContext(ctx, "20260101", oracleTime-1, oracleTime+999)
		if err != nil || len(alerts) != 1 {
			t.Fatalf("Alert %+v %v", alerts, err)
		}
		comparePack(input("alert"), alerts[0])
		stacks, err := s.StacksContext(ctx, "/oracle/app", oracleTime-1, oracleTime+1000)
		if err != nil || len(stacks) != 2 {
			t.Fatalf("Stack %+v %v", stacks, err)
		}
		batches, err := s.BatchesContext(ctx, "/oracle/app", oracleTime-1, oracleTime+1000)
		if err != nil || len(batches) != 2 {
			t.Fatalf("Batch %+v %v", batches, err)
		}
		for n := 0; n < 2; n++ {
			comparePack(input(fmt.Sprintf("stack-%d", n+1)), stacks[n])
			if batches[n].SQLTotalCount != 1 || batches[n].SQLStats != nil || batches[n].UniqueSQLs != nil || batches[n].Extra != nil {
				t.Fatalf("Java Batch history metadata projection %+v", batches[n])
			}
			simple, err := protocol.EncodeBatchSimple(batches[n], protocol.Limits{})
			if err != nil || !bytes.Equal(simple, golden(fmt.Sprintf("batch-simple-%d", n+1))) {
				t.Fatalf("actual Java writeSimple mismatch batch%d %v", n+1, err)
			}
			decoded, err := protocol.DecodeBatchSimple(simple, protocol.Limits{})
			if err != nil || decoded.SQLTotalCount != 1 || decoded.Position != batches[n].Position || decoded.SQLStats != nil {
				t.Fatalf("simple decode %+v %v", decoded, err)
			}
			detail, err := s.BatchAtContext(ctx, "/oracle/app", batches[n].StartTime+batches[n].ElapsedTime, batches[n].Position)
			if err != nil || detail == nil {
				t.Fatalf("detail %+v %v", detail, err)
			}
			comparePack(input(fmt.Sprintf("batch-%d", n+1)), detail)
		}
	}
	verifyGo(javaDB)
	s, err := Open(Config{Directory: goDB, Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, name := range []string{"status", "summary", "alert", "stack-1", "stack-2", "batch-1", "batch-2", "span-container"} {
		switch p := input(name).(type) {
		case *protocol.StatusPack:
			err = s.PutStatusContext(ctx, p)
		case *protocol.SummaryPack:
			err = s.PutSummaryContext(ctx, p)
		case *protocol.AlertPack:
			err = s.PutAlertContext(ctx, p)
		case *protocol.StackPack:
			err = s.PutStackContext(ctx, "/oracle/app", p)
		case *protocol.BatchPack:
			err = s.PutBatchContext(ctx, "/oracle/app", p)
		case *protocol.SpanContainerPack:
			err = s.PutSpanContainerContext(ctx, p)
		default:
			t.Fatalf("unexpected extra input %s", name)
		}
		if err != nil {
			t.Fatal(name, err)
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	verifyGo(goDB)
	// Prefixes, original index formats and physical offsets must match actual writer bytes.
	for _, name := range []string{"status/status.pstat", "status/status_tim.kfile", "status/status_tim.hfile", "sum/sum.sum", "sum/sum_app_tim.kfile", "sum/sum_app_tim.hfile", "alert/alert.alert", "alert/alert_tim.kfile", "alert/alert_tim.hfile", "oracle/app/stack.dat", "oracle/app/stack.idx", "oracle/app/batch.dat", "oracle/app/batch.idx"} {
		jb, err := os.ReadFile(filepath.Join(javaDB, "20260101", name))
		if err != nil {
			t.Fatal(err)
		}
		gb, err := os.ReadFile(filepath.Join(goDB, "20260101", name))
		if err != nil || !bytes.Equal(jb, gb) {
			t.Fatalf("actual Java->Go file byte difference %s: %v", name, err)
		}
	}
	// The actual original readers verify the Go-created files, including GZip stack text.
	oracle("--verify-extra", goDB)
}
