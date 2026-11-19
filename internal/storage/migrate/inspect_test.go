package migrate

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

func TestInspectGoWrittenDB(t *testing.T) {
	for _, zip := range []bool{false, true} {
		dir := t.TempDir()
		buildDB(t, dir, zip)
		before := capture(t, dir)
		rep, err := Inspect(context.Background(), InspectOptions{Root: dir, Location: seoul(t), Now: 100})
		if err != nil {
			t.Fatal(err)
		}
		assertUnchanged(t, before, dir)
		if rep.Summary.Errors != 0 || rep.Summary.Warnings != 0 {
			t.Fatalf("zip=%v problems %+v", zip, rep.Problems)
		}
		d := rep.Summary.Dates[testDate]
		if d["xlog"] != 3 || d["xlog_tid"] != 3 || d["xlog_gid"] != 2 || d["profile"] != 2 || d["object"] != 1 || d["status"] != 1 || d["alert"] != 1 || d["text"] != 1 || d["profile_ref_missing"] != 0 {
			t.Fatalf("zip=%v date counts %v", zip, d)
		}
		if zip && d["zip_records"] != 5 {
			t.Fatalf("zip records %v", d)
		}
		p := rep.Summary.Permanent
		if p["text"] != 1 || p["ttl_live"] != 2 || p["ttl_expired"] != 0 {
			t.Fatalf("permanent %v", p)
		}
		later, err := Inspect(context.Background(), InspectOptions{Root: dir, Location: seoul(t), Now: 111})
		if err != nil || later.Summary.Permanent["ttl_expired"] != 1 || later.Summary.Permanent["ttl_live"] != 1 {
			t.Fatalf("TTL now boundary %v %v", later.Summary.Permanent, err)
		}
		for _, f := range rep.Files {
			if f.Path == "notes/readme.txt" && (f.Status != StatusUnknown || f.SHA256 == "") {
				t.Fatalf("unknown file %+v", f)
			}
			if f.Status != StatusExcluded && f.SHA256 == "" {
				t.Fatalf("missing checksum %+v", f)
			}
		}
		if !hasProblem(rep, SeverityInfo, "layout.unknown") {
			t.Fatal("unknown file not reported")
		}
	}
}

func TestInspectCorruption(t *testing.T) {
	xlog := func(dir string) string { return filepath.Join(dir, testDate, "xlog") }
	cases := []struct {
		name, severity, code string
		zip                  bool
		utc                  bool
		mutate               func(t *testing.T, dir string)
	}{
		{name: "truncated kfile", severity: SeverityError, code: "index.truncated", mutate: func(t *testing.T, dir string) {
			truncateBy(t, filepath.Join(xlog(dir), "xlog_tim.kfile"), 3)
		}},
		{name: "truncated hfile", severity: SeverityError, code: "index.size", mutate: func(t *testing.T, dir string) {
			truncateBy(t, filepath.Join(xlog(dir), "xlog_tim.hfile"), 5)
		}},
		{name: "magic", severity: SeverityError, code: "index.magic", mutate: func(t *testing.T, dir string) {
			patchFile(t, filepath.Join(dir, testDate, "object", "obj.kfile"), 0, []byte{0})
		}},
		{name: "chain cycle", severity: SeverityError, code: "index.cycle", mutate: func(t *testing.T, dir string) {
			pos := recordPositions(t, filepath.Join(xlog(dir), "xlog_tid"))[1]
			patchFile(t, filepath.Join(xlog(dir), "xlog_tid.kfile"), pos+1, long5(pos))
		}},
		{name: "missing hfile", severity: SeverityError, code: "index.missing_pair", mutate: func(t *testing.T, dir string) {
			os.Remove(filepath.Join(dir, testDate, "object", "obj.hfile"))
		}},
		{name: "orphan hfile", severity: SeverityError, code: "index.missing_pair", mutate: func(t *testing.T, dir string) {
			os.Remove(filepath.Join(dir, testDate, "object", "obj.kfile"))
		}},
		{name: "data truncated", severity: SeverityError, code: "data.offset_eof", mutate: func(t *testing.T, dir string) {
			truncateBy(t, filepath.Join(xlog(dir), "xlog.service"), 5)
		}},
		{name: "data decode", severity: SeverityError, code: "data.decode", mutate: func(t *testing.T, dir string) {
			patchFile(t, filepath.Join(xlog(dir), "xlog.service"), 2, []byte{0xee})
		}},
		{name: "object key", severity: SeverityError, code: "data.key_mismatch", mutate: func(t *testing.T, dir string) {
			s, err := storage.Open(storage.Config{Directory: dir, Location: seoul(t)})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			// Rewrite the slot of objHash 42 with a pack claiming objHash 43.
			b, err := protocol.EncodePack(&protocol.ObjectPack{ObjHash: 43, ObjType: "java", ObjName: "x", Tags: mapValue()}, protocol.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			patchFile(t, filepath.Join(dir, testDate, "object", "obj.data"), 4, b)
		}},
		{name: "date mismatch", severity: SeverityError, code: "time.date_mismatch", mutate: func(t *testing.T, dir string) {
			if err := os.Rename(filepath.Join(dir, testDate), filepath.Join(dir, "20260102")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "wrong timezone", severity: SeverityError, code: "index.head", utc: true, mutate: func(*testing.T, string) {}},
		{name: "tag chain", severity: SeverityWarning, code: "tagcount.torn_tail", mutate: func(t *testing.T, dir string) {
			s, err := storage.Open(storage.Config{Directory: dir, Location: seoul(t)})
			if err != nil {
				t.Fatal(err)
			}
			day, err := time.ParseInLocation("20060102", testDate, seoul(t))
			if err != nil {
				t.Fatal(err)
			}
			if err = s.PutXLogTagsContext(context.Background(), "java", &protocol.XLogPack{EndTime: day.Add(time.Hour).UnixMilli(), ObjHash: 1, Service: 5}); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			truncateBy(t, filepath.Join(dir, testDate, "tagcnt", "java", "first.kfile"), 3)
		}},
		{name: "truncated tail", severity: SeverityWarning, code: "data.truncated_tail", mutate: func(t *testing.T, dir string) {
			appendFile(t, filepath.Join(xlog(dir), "xlog.service"), []byte{0, 10, 1})
		}},
		{name: "unindexed tail", severity: SeverityWarning, code: "data.unindexed_tail", mutate: func(t *testing.T, dir string) {
			appendFile(t, filepath.Join(xlog(dir), "xlog.service"), []byte{0, 1, 0x55})
		}},
		{name: "profile without xlog", severity: SeverityWarning, code: "profile.xlog_missing", mutate: func(t *testing.T, dir string) {
			s, err := storage.Open(storage.Config{Directory: dir, Location: seoul(t)})
			if err != nil {
				t.Fatal(err)
			}
			if err = s.PutProfile(&protocol.ProfilePack{Time: baseTime, TxID: 999, Profile: []byte{9}}); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink", severity: SeverityError, code: "fs.symlink", mutate: func(t *testing.T, dir string) {
			if err := os.Symlink("/etc/hosts", filepath.Join(dir, "link")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "count.dat ahead", severity: SeverityError, code: "zip.count_mismatch", zip: true, mutate: func(t *testing.T, dir string) {
			path := filepath.Join(xlog(dir), "data", "count.dat")
			setCount(t, path, readCount(t, path)+10)
		}},
		{name: "count.dat size", severity: SeverityError, code: "zip.count_size", zip: true, mutate: func(t *testing.T, dir string) {
			truncateBy(t, filepath.Join(xlog(dir), "data", "count.dat"), 1)
		}},
		{name: "untracked block", severity: SeverityError, code: "zip.untracked_block", zip: true, mutate: func(t *testing.T, dir string) {
			b, _ := os.ReadFile(filepath.Join(xlog(dir), "data", "xlog.0"))
			os.WriteFile(filepath.Join(xlog(dir), "data", "xlog.3"), b, 0600)
		}},
		{name: "linked cycle", severity: SeverityError, code: "zip.cycle", zip: true, mutate: func(t *testing.T, dir string) {
			rewriteZip(t, filepath.Join(xlog(dir), "data", "xlog.0"), func(b []byte) []byte {
				second := 8 + zipRecordLen(b, 0)
				copy(b[second:], long5(int64(second)))
				return b
			})
		}},
		{name: "linked out of range", severity: SeverityError, code: "zip.offset", zip: true, mutate: func(t *testing.T, dir string) {
			rewriteZip(t, filepath.Join(xlog(dir), "data", "xlog.0"), func(b []byte) []byte {
				copy(b, long5(1<<30))
				return b
			})
		}},
		{name: "block record bounds", severity: SeverityError, code: "zip.record_bounds", zip: true, mutate: func(t *testing.T, dir string) {
			rewriteZip(t, filepath.Join(xlog(dir), "data", "xlog.0"), func(b []byte) []byte {
				b[5], b[6], b[7] = 0x7f, 0xff, 0xff
				return b
			})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			buildDB(t, dir, c.zip)
			c.mutate(t, dir)
			loc := seoul(t)
			if c.utc {
				loc = time.UTC
			}
			before := capture(t, dir)
			rep, err := Inspect(context.Background(), InspectOptions{Root: dir, Location: loc, Now: 100})
			if err != nil {
				t.Fatal(err)
			}
			assertUnchanged(t, before, dir)
			if !hasProblem(rep, c.severity, c.code) {
				t.Fatalf("missing %s %s in %+v", c.severity, c.code, rep.Problems)
			}
			if c.severity == SeverityWarning && rep.HasErrors() {
				t.Fatalf("warning case produced errors %+v", rep.Problems)
			}
			if c.severity == SeverityError && !rep.HasErrors() {
				t.Fatal("error not counted")
			}
		})
	}
}

func TestInspectLockDetection(t *testing.T) {
	dir := t.TempDir()
	buildDB(t, dir, false)
	lock := filepath.Join(dir, goLockName)
	if err := os.WriteFile(lock, nil, 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Minute)
	if err := os.WriteFile(filepath.Join(dir, javaLockName), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(dir, javaLockName), old, old); err != nil {
		t.Fatal(err)
	}
	rep, err := Inspect(context.Background(), InspectOptions{Root: dir, Location: seoul(t)})
	if err != nil || rep.HasErrors() || !hasProblem(rep, SeverityInfo, "lock.go_stale") || !hasProblem(rep, SeverityInfo, "lock.java_stale") {
		t.Fatalf("stale locks %+v %v", rep.Problems, err)
	}
	f, err := os.OpenFile(lock, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err = os.Chtimes(filepath.Join(dir, javaLockName), now, now); err != nil {
		t.Fatal(err)
	}
	rep, err = Inspect(context.Background(), InspectOptions{Root: dir, Location: seoul(t)})
	if err != nil || !hasProblem(rep, SeverityError, "lock.go_active") || !hasProblem(rep, SeverityError, "lock.java_active") {
		t.Fatalf("live locks %+v %v", rep.Problems, err)
	}
	for _, f := range rep.Files {
		if isLockFile(f.Path) && (f.Status != StatusExcluded || f.SHA256 != "") {
			t.Fatalf("lock file not excluded %+v", f)
		}
	}
}

func TestInspectCancellation(t *testing.T) {
	dir := t.TempDir()
	buildDB(t, dir, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Inspect(ctx, InspectOptions{Root: dir, Location: seoul(t)}); err == nil {
		t.Fatal("cancelled inspect succeeded")
	}
}

// count.dat behind the block (crash after the block save) is classified as
// auto-recoverable only when the tail is whole next=0 records.
func TestInspectZipCountBehindBlock(t *testing.T) {
	for _, c := range []struct {
		name        string
		back        func(b []byte) int64 // bytes count.dat moves back
		recoverable bool
	}{
		{"last record boundary", func(b []byte) int64 {
			at, last := 0, 0
			for at < len(b) {
				last = at
				at += 8 + zipRecordLen(b, at)
			}
			return int64(len(b) - last)
		}, true},
		{"inside a record", func([]byte) int64 { return 3 }, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			buildDB(t, dir, true)
			data := filepath.Join(dir, testDate, "xlog", "data")
			var block []byte
			rewriteZip(t, filepath.Join(data, "xlog.0"), func(b []byte) []byte { block = append([]byte(nil), b...); return b })
			countPath := filepath.Join(data, "count.dat")
			setCount(t, countPath, readCount(t, countPath)-c.back(block))
			rep, err := Inspect(context.Background(), InspectOptions{Root: dir, Location: seoul(t), Now: 100})
			if err != nil {
				t.Fatal(err)
			}
			if !hasProblem(rep, SeverityError, "zip.count_mismatch") {
				t.Fatalf("mismatch not reported %+v", rep.Problems)
			}
			if hasProblem(rep, SeverityInfo, "zip.auto_recoverable") != c.recoverable {
				t.Fatalf("auto-recoverable=%v %+v", !c.recoverable, rep.Problems)
			}
		})
	}
}

// tagcnt first/next key files share the .kfile extension with legacy indexes
// but not the format; inspect must not parse them as index pairs (found by
// the independent Java replay oracle: every real DB has tagcnt files).
func TestInspectDoesNotParseTagCountFilesAsIndexes(t *testing.T) {
	dir := t.TempDir()
	s, err := storage.Open(storage.Config{Directory: dir, Location: seoul(t)})
	if err != nil {
		t.Fatal(err)
	}
	tm := time.Date(2026, 1, 1, 1, 2, 0, 0, seoul(t)).UnixMilli()
	for svc := int32(1); svc <= 3; svc++ {
		if err = s.PutXLogTagsContext(context.Background(), "tomcat", &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: svc, Elapsed: 5}); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.SyncData(); err != nil { // flushes pending tag deltas to first/next files
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	rep, err := Inspect(context.Background(), InspectOptions{Root: dir, Location: seoul(t)})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Summary.Errors != 0 || rep.Summary.Warnings != 0 {
		t.Fatalf("problems %+v", rep.Problems)
	}
	tagFiles := 0
	for _, f := range rep.Files {
		if strings.Contains(f.Path, "/tagcnt/") {
			tagFiles++
			if f.Category != "tagcount" || f.Status != StatusUnchecked {
				t.Fatalf("%+v", f)
			}
		}
	}
	if tagFiles == 0 {
		t.Fatal("no tagcnt files written")
	}
}
