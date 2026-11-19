package migrate

import (
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

// halfMember is the first half of one gzip member holding raw: the shape of a
// crash inside a member append.
func halfMember(raw []byte) []byte {
	var out bytes.Buffer
	w := gzip.NewWriter(&out)
	w.Write(raw)
	w.Close()
	return out.Bytes()[:out.Len()/2]
}

// A torn final member of the count.dat block (count.dat still before it) is
// the Go writer's own crash shape: reported as an error, marked recoverable,
// accepted by --accept-recoverable, and repaired by the writer on dst.
func TestInspectTornZipMemberRecoverable(t *testing.T) {
	_, src, dst := fixture(t, true)
	block := filepath.Join(src, testDate, "xlog", "data", "xlog.0")
	appendFile(t, block, halfMember(zipRecord(4000)))
	rep, err := Inspect(context.Background(), InspectOptions{Root: src, Location: seoul(t), Now: 100})
	if err != nil || !hasProblem(rep, SeverityError, "zip.torn_member") || !hasProblem(rep, SeverityInfo, "zip.auto_recoverable") || hasProblem(rep, SeverityError, "zip.block_gzip") {
		t.Fatalf("inspect %v %+v", err, rep.Problems)
	}
	if rep.Summary.Dates[testDate]["zip_records"] != 5 {
		t.Fatalf("records of complete members %v", rep.Summary.Dates[testDate])
	}
	accepted, rejected := splitRecoverable(rep.Problems)
	if len(rejected) != 0 || len(accepted) != 1 || accepted[0].Code != "zip.torn_member" {
		t.Fatalf("accepted %+v rejected %+v", accepted, rejected)
	}
	if _, err = Migrate(context.Background(), migrateOpts(t, src, dst)); err == nil {
		t.Fatal("torn member migrated without --accept-recoverable")
	}
	opt := migrateOpts(t, src, filepath.Join(filepath.Dir(dst), "dst2"))
	opt.AcceptRecoverable = true
	m, err := Migrate(context.Background(), opt)
	if err != nil || len(m.AcceptedProblems) != 1 {
		t.Fatalf("migrate %v %+v", err, m)
	}
	var events []storage.RecoveryEvent
	s, err := storage.Open(storage.Config{Directory: opt.Dst, Location: seoul(t), OnRecover: func(e storage.RecoveryEvent) { events = append(events, e) }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.PutXLog(&protocol.XLogPack{TxID: 105, EndTime: baseTime + 800, ObjHash: 42}); err != nil || len(events) != 1 || events[0].Kind != "zip-torn-member" {
		t.Fatal(err, events)
	}
	if x, err := s.XLog(testDate, 105); err != nil || x == nil {
		t.Fatal(x, err)
	}
}

// Torn members the writer does not repair stay non-recoverable: count.dat
// past the complete members, and a torn member in a block before count's.
func TestInspectTornZipMemberNotRecoverable(t *testing.T) {
	_, src, _ := fixture(t, true)
	data := filepath.Join(src, testDate, "xlog", "data")
	count := readCount(t, filepath.Join(data, "count.dat"))
	appendFile(t, filepath.Join(data, "xlog.0"), halfMember(zipRecord(4000)))
	setCount(t, filepath.Join(data, "count.dat"), count+8+4000)
	rep, err := Inspect(context.Background(), InspectOptions{Root: src, Location: seoul(t), Now: 100})
	if err != nil || !hasProblem(rep, SeverityError, "zip.torn_member") || hasProblem(rep, SeverityInfo, "zip.auto_recoverable") {
		t.Fatalf("count past torn member %v %+v", err, rep.Problems)
	}
	if _, rejected := splitRecoverable(rep.Problems); len(rejected) == 0 {
		t.Fatal("count past torn member accepted")
	}

	root := zipOnlyDB(t, nil)
	data = filepath.Join(root, testDate, "xlog", "data")
	appendFile(t, filepath.Join(data, "xlog.0"), halfMember(zipRecord(1)))
	writeZipBlock(t, filepath.Join(data, "xlog.1"), zipRecord(30))
	setCount(t, filepath.Join(data, "count.dat"), zipBlockBytes+38)
	rep, err = Inspect(context.Background(), InspectOptions{Root: root, Location: seoul(t), Now: 100})
	if err != nil || !hasProblem(rep, SeverityError, "zip.torn_member") {
		t.Fatalf("earlier block %v %+v", err, rep.Problems)
	}
	if _, rejected := splitRecoverable(rep.Problems); len(rejected) == 0 {
		t.Fatal("torn member before the count block accepted")
	}
}

// A crash inside the rollover leaves an empty or torn next block; with count
// still in the full block it is a recoverable orphan, as in the writer.
func TestInspectEmptyOrTornRolloverOrphan(t *testing.T) {
	for name, orphan := range map[string][]byte{
		"empty": nil,
		"torn":  halfMember(zipRecord(20)),
	} {
		t.Run(name, func(t *testing.T) {
			root := zipOnlyDB(t, nil)
			if err := os.WriteFile(filepath.Join(root, testDate, "xlog", "data", "xlog.1"), orphan, 0600); err != nil {
				t.Fatal(err)
			}
			rep, err := Inspect(context.Background(), InspectOptions{Root: root, Location: seoul(t), Now: 100})
			if err != nil || !hasProblem(rep, SeverityError, "zip.untracked_block") || !hasProblem(rep, SeverityInfo, "zip.auto_recoverable") {
				t.Fatalf("inspect %v %+v", err, rep.Problems)
			}
			if _, rejected := splitRecoverable(rep.Problems); len(rejected) != 0 {
				t.Fatalf("rejected %+v", rejected)
			}
		})
	}
}
