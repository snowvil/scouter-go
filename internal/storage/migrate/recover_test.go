package migrate

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

func copyDir(t *testing.T, from, to string) {
	t.Helper()
	if err := os.CopyFS(to, os.DirFS(from)); err != nil {
		t.Fatal(err)
	}
}

// doubleCrashDB leaves xlog_tim with orphan A (prev=H) and fork B (prev=H):
// Java restarted from a stale header and crashed again before flushing.
func doubleCrashDB(t *testing.T) (tmp, src, dst string) {
	t.Helper()
	tmp, src, dst = fixture(t, false)
	tim := filepath.Join(src, testDate, "xlog", "xlog_tim")
	stale, err := os.ReadFile(tim + ".hfile")
	if err != nil {
		t.Fatal(err)
	}
	s, err := storage.Open(storage.Config{Directory: src, Location: seoul(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.PutXLog(&protocol.XLogPack{TxID: 104, EndTime: baseTime + 700, ObjHash: 42}); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(tim+".hfile", stale, 0600); err != nil {
		t.Fatal(err)
	}
	ro, err := legacyindex.OpenReadOnly(tim, true, seoul(t))
	if err != nil {
		t.Fatal(err)
	}
	records, err := ro.Records()
	ro.Close()
	if err != nil {
		t.Fatal(err)
	}
	a := records[len(records)-1]
	if a.PreviousPosition == 0 {
		t.Fatal("fixture orphan has no saved head to fork from")
	}
	rec := make([]byte, 8+len(a.Key)+1+len(a.Value))
	copy(rec[1:6], long5(a.PreviousPosition))
	binary.BigEndian.PutUint16(rec[6:8], uint16(len(a.Key)))
	copy(rec[8:], a.Key)
	rec[8+len(a.Key)] = byte(len(a.Value))
	copy(rec[9+len(a.Key):], a.Value)
	appendFile(t, tim+".kfile", rec)
	return tmp, src, dst
}

func TestDoubleCrashIsWarningAndMigratesUnchanged(t *testing.T) {
	_, src, dst := doubleCrashDB(t)
	rep, err := Inspect(context.Background(), InspectOptions{Root: src, Location: seoul(t), Now: 100})
	if err != nil || rep.HasErrors() || !hasProblem(rep, SeverityWarning, "index.unreachable_records") ||
		!hasProblem(rep, SeverityWarning, "index.relink_refused") || hasProblem(rep, SeverityInfo, "index.auto_recoverable") {
		t.Fatalf("inspect %v %+v", err, rep.Problems)
	}
	before := capture(t, src)
	m, err := Migrate(context.Background(), migrateOpts(t, src, dst))
	if err != nil || m.Status != ManifestCompleted {
		t.Fatalf("migrate %v %+v", err, m)
	}
	assertUnchanged(t, before, src)
	// A writable Go open tombstones the refused orphans, so the comparison
	// that would open the staging copy writable is skipped for that index.
	skipped := false
	for _, p := range m.Query.Problems {
		skipped = skipped || (p.Code == "query.not_compared" && strings.HasSuffix(p.Path, "/xlog/xlog_tim"))
	}
	if !skipped {
		t.Fatalf("refused relink index must not be compared through a writable open: %+v", m.Query)
	}
	// A writable Go open of dst succeeds with the Java semantics; the only
	// change is the tombstone flag of the refused orphans in xlog_tim.kfile.
	d, err := storage.Open(storage.Config{Directory: dst, Location: seoul(t)})
	if err != nil {
		t.Fatal(err)
	}
	logs, err := d.XLogs(testDate, baseTime, baseTime+1000, false)
	if err != nil || len(logs) != 3 {
		t.Fatalf("double-crash dst XLogs %d %v", len(logs), err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	v, err := Verify(context.Background(), VerifyOptions{Dst: dst, Location: seoul(t), Now: 100})
	if err != nil || v.Failed() {
		t.Fatalf("verify %v %+v", err, v)
	}
	if len(v.PostMigration.Modified) != 1 || !strings.HasSuffix(v.PostMigration.Modified[0], "/xlog/xlog_tim.kfile") {
		t.Fatalf("tombstone must be the only post-migration change: %+v", v.PostMigration)
	}
	// Reopening dst is quiet: the tombstoned fork is no longer an orphan.
	rep, err = Inspect(context.Background(), InspectOptions{Root: dst, Location: seoul(t), Now: 100})
	if err != nil || rep.HasErrors() || hasProblem(rep, SeverityWarning, "index.relink_refused") {
		t.Fatalf("post-tombstone inspect %v %+v", err, rep.Problems)
	}
}

func TestAcceptRecoverableTornIndexTail(t *testing.T) {
	_, src, dst := fixture(t, false)
	tid := filepath.Join(src, testDate, "xlog", "xlog_tid.kfile")
	appendFile(t, tid, []byte{0, 0, 0, 0, 0, 0, 0, 8}) // torn: 8 of a 21-byte record
	rep, err := Inspect(context.Background(), InspectOptions{Root: src, Location: seoul(t), Now: 100})
	if err != nil || !hasProblem(rep, SeverityError, "index.truncated") || !hasProblem(rep, SeverityInfo, "index.auto_recoverable") {
		t.Fatalf("inspect %v %+v", err, rep.Problems)
	}
	if _, err = Migrate(context.Background(), migrateOpts(t, src, dst)); err == nil || !strings.Contains(err.Error(), "--accept-recoverable") {
		t.Fatalf("migration without the flag: %v", err)
	}
	before := capture(t, src)
	opt := migrateOpts(t, src, dst)
	opt.AcceptRecoverable = true
	m, err := Migrate(context.Background(), opt)
	if err != nil || m.Status != ManifestCompleted || len(m.AcceptedProblems) != 1 || m.AcceptedProblems[0].Code != "index.truncated" {
		t.Fatalf("accepted migration %v %+v", err, m)
	}
	assertUnchanged(t, before, src)
	got, _ := os.ReadFile(filepath.Join(dst, testDate, "xlog", "xlog_tid.kfile"))
	want, _ := os.ReadFile(tid)
	if !bytes.Equal(got, want) {
		t.Fatal("torn tail must be copied unchanged")
	}
	v, err := Verify(context.Background(), VerifyOptions{Dst: dst, Location: seoul(t), Now: 100})
	if err != nil || v.Failed() {
		t.Fatalf("verify before repair %v %+v", err, v.Problems)
	}
	d, err := storage.Open(storage.Config{Directory: dst, Location: seoul(t)})
	if err != nil {
		t.Fatal(err)
	}
	if x, err := d.XLog(testDate, 101); err != nil || x == nil {
		t.Fatalf("XLog after repair %v %v", x, err)
	}
	d.Close()
	v, err = Verify(context.Background(), VerifyOptions{Dst: dst, Location: seoul(t), Now: 100})
	if err != nil || v.Failed() || !reflect.DeepEqual(v.PostMigration.Modified, []string{testDate + "/xlog/xlog_tid.kfile"}) || v.DstSummary.Errors != 0 {
		t.Fatalf("verify after repair %v %+v %+v", err, v.PostMigration, v.Problems)
	}
	// Other errors are still refused even with the flag.
	other := filepath.Join(t.TempDir(), "other")
	copyDir(t, src, other)
	os.Remove(filepath.Join(other, javaLockName)) // CopyFS refreshes its mtime
	patchFile(t, filepath.Join(other, testDate, "object", "obj.kfile"), 0, []byte{0})
	opt = migrateOpts(t, other, filepath.Join(t.TempDir(), "dst"))
	opt.AcceptRecoverable = true
	if _, err = Migrate(context.Background(), opt); err == nil || !strings.Contains(err.Error(), "not auto-recoverable") {
		t.Fatalf("non-recoverable error accepted: %v", err)
	}
}

// A Go writer crash after the gzip block save but before count.dat/index.
func TestAcceptRecoverableZipCount(t *testing.T) {
	tmp := t.TempDir()
	live := filepath.Join(tmp, "live")
	buildDB(t, live, true)
	src := filepath.Join(tmp, "src")
	copyDir(t, live, src)
	s, err := storage.Open(storage.Config{Directory: live, Location: seoul(t), CompressXLog: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.PutXLog(&protocol.XLogPack{TxID: 104, EndTime: baseTime + 700, ObjHash: 42}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	block, _ := os.ReadFile(filepath.Join(live, testDate, "xlog", "data", "xlog.0"))
	if err = os.WriteFile(filepath.Join(src, testDate, "xlog", "data", "xlog.0"), block, 0600); err != nil {
		t.Fatal(err)
	}
	rep, err := Inspect(context.Background(), InspectOptions{Root: src, Location: seoul(t), Now: 100})
	if err != nil || !hasProblem(rep, SeverityError, "zip.count_mismatch") || !hasProblem(rep, SeverityInfo, "zip.auto_recoverable") || hasProblem(rep, SeverityError, "zip.offset") {
		t.Fatalf("inspect %v %+v", err, rep.Problems)
	}
	opt := migrateOpts(t, src, filepath.Join(tmp, "dst"))
	opt.AcceptRecoverable = true
	m, err := Migrate(context.Background(), opt)
	if err != nil || len(m.AcceptedProblems) != 1 {
		t.Fatalf("zip accepted migration %v %+v", err, m)
	}
}

// Bounded descriptors give the same report as an unbounded inspection.
func TestInspectBoundsOpenDataFiles(t *testing.T) {
	dir := t.TempDir()
	buildDB(t, dir, false)
	full, err := Inspect(context.Background(), InspectOptions{Root: dir, Location: seoul(t), Now: 100})
	if err != nil {
		t.Fatal(err)
	}
	old := maxOpenDataFiles
	maxOpenDataFiles = 2
	peak := 0
	testHookMaxOpen = func(n int) { peak = n }
	defer func() { maxOpenDataFiles, testHookMaxOpen = old, nil }()
	bounded, err := Inspect(context.Background(), InspectOptions{Root: dir, Location: seoul(t), Now: 100})
	if err != nil {
		t.Fatal(err)
	}
	if peak != 2 || !reflect.DeepEqual(full.Files, bounded.Files) || !reflect.DeepEqual(full.Problems, bounded.Problems) || !reflect.DeepEqual(full.Summary, bounded.Summary) {
		t.Fatalf("bounded inspection differs (peak %d): %+v", peak, bounded.Problems)
	}
}

// An accepted torn index must still get record-level data checks: a torn
// tail together with an out-of-file data offset is refused.
func TestTornIndexRecordsAreStillChecked(t *testing.T) {
	_, src, dst := fixture(t, false)
	tid := filepath.Join(src, testDate, "xlog", "xlog_tid.kfile")
	appendFile(t, tid, []byte{0, 0, 0, 0, 0, 0, 0, 8})
	rep, err := Inspect(context.Background(), InspectOptions{Root: src, Location: seoul(t), Now: 100})
	if err != nil || !hasProblem(rep, SeverityInfo, "index.auto_recoverable") || rep.Summary.Dates[testDate]["xlog_tid"] != 3 || hasProblem(rep, SeverityError, "data.offset_eof") {
		t.Fatalf("torn index records %v %v %+v", err, rep.Summary.Dates, rep.Problems)
	}
	// First record: deleted(1) prev(5) keylen(2) key(8) blob prefix(1) -> value at 19.
	patchFile(t, tid, 19, long5(1<<38))
	rep, err = Inspect(context.Background(), InspectOptions{Root: src, Location: seoul(t), Now: 100})
	if err != nil || !hasProblem(rep, SeverityError, "index.truncated") || !hasProblem(rep, SeverityError, "data.offset_eof") {
		t.Fatalf("data error behind torn tail not reported %v %+v", err, rep.Problems)
	}
	opt := migrateOpts(t, src, dst)
	opt.AcceptRecoverable = true
	if _, err = Migrate(context.Background(), opt); err == nil || !strings.Contains(err.Error(), "not auto-recoverable") {
		t.Fatalf("torn tail + data.offset_eof accepted: %v", err)
	}
}

func writeZipBlock(t *testing.T, path string, raw []byte) {
	t.Helper()
	var out bytes.Buffer
	w := gzip.NewWriter(&out)
	w.Write(raw)
	w.Close()
	if err := os.WriteFile(path, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
}
func zipRecord(n int) []byte {
	rec := make([]byte, 8+n)
	rec[5], rec[6], rec[7] = byte(n>>16), byte(n>>8), byte(n)
	return rec
}

// zipOnlyDB holds just a compressed store: block 0 ends 10 bytes short of
// 32MiB and count.dat is its end, so a 20-byte record had to roll over.
func zipOnlyDB(t *testing.T, extra map[int64][]byte) string {
	t.Helper()
	root := t.TempDir()
	data := filepath.Join(root, testDate, "xlog", "data")
	if err := os.MkdirAll(data, 0700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(root, testDate, "xlog", "xlog.service.conf"), []byte("compress_xlog_enabled=true\n"), 0600)
	// Int3 lengths are signed (max 8388607), so fill with several records.
	var block0 []byte
	for remaining := zipBlockBytes - 10; remaining > 0; {
		n := min(remaining-8, 0x7fffff)
		block0 = append(block0, zipRecord(n)...)
		remaining -= 8 + n
	}
	writeZipBlock(t, filepath.Join(data, "xlog.0"), block0)
	setCount(t, filepath.Join(data, "count.dat"), int64(len(block0)))
	for n, raw := range extra {
		writeZipBlock(t, filepath.Join(data, fmt.Sprintf("xlog.%d", n)), raw)
	}
	return root
}

func TestZipOrphanRolloverBlock(t *testing.T) {
	orphan := append(zipRecord(20), zipRecord(5)...)
	cases := []struct {
		name        string
		extra       map[int64][]byte
		recoverable bool
	}{
		{"valid orphan N+1", map[int64][]byte{1: orphan}, true},
		{"N+2 without N+1", map[int64][]byte{2: orphan}, false},
		{"N+1 and N+2", map[int64][]byte{1: orphan, 2: orphan}, false},
		{"first record fits without rollover", map[int64][]byte{1: zipRecord(1)}, false},
		{"incomplete orphan", map[int64][]byte{1: append(zipRecord(20), 0, 0)}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := zipOnlyDB(t, c.extra)
			rep, err := Inspect(context.Background(), InspectOptions{Root: root, Location: seoul(t), Now: 100})
			if err != nil || !hasProblem(rep, SeverityError, "zip.untracked_block") {
				t.Fatalf("inspect %v %+v", err, rep.Problems)
			}
			if hasProblem(rep, SeverityInfo, "zip.auto_recoverable") != c.recoverable {
				t.Fatalf("auto_recoverable want %v: %+v", c.recoverable, rep.Problems)
			}
			accepted, rejected := splitRecoverable(rep.Problems)
			if c.recoverable && (len(rejected) != 0 || len(accepted) != 1 || accepted[0].Path != testDate+"/xlog/data/xlog.1") {
				t.Fatalf("accepted %+v rejected %+v", accepted, rejected)
			}
			if !c.recoverable && len(rejected) == 0 {
				t.Fatalf("non-recoverable shape accepted %+v", accepted)
			}
		})
	}
}

func TestPurgeTrashAndTempLeftoversAreExcluded(t *testing.T) {
	_, src, dst := fixture(t, false)
	trash := filepath.Join(src, ".purge-trash", "20251201-ALL-1-1-0", "xlog")
	if err := os.MkdirAll(trash, 0700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(trash, "xlog_tim.kfile"), []byte("garbage"), 0600)
	leftovers := []string{testDate + "/xlog/.scouter-123", testDate + "/counter/.counter-head-9", ".scouter-7"}
	for _, rel := range leftovers {
		path := filepath.Join(src, filepath.FromSlash(rel))
		os.MkdirAll(filepath.Dir(path), 0700)
		os.WriteFile(path, []byte("partial"), 0600)
	}
	rep, err := Inspect(context.Background(), InspectOptions{Root: src, Location: seoul(t), Now: 100})
	if err != nil || rep.HasErrors() || !hasProblem(rep, SeverityWarning, "purge.trash_leftover") || !hasProblem(rep, SeverityWarning, "temp_leftover") {
		t.Fatalf("inspect %v %+v", err, rep.Problems)
	}
	for _, f := range rep.Files {
		if (isPurgeTrash(f.Path) || isTempLeftover(f.Path)) && (f.Status != StatusExcluded || f.SHA256 != "") {
			t.Fatalf("leftover not excluded %+v", f)
		}
	}
	before := capture(t, src)
	m, err := Migrate(context.Background(), migrateOpts(t, src, dst))
	if err != nil || m.Status != ManifestCompleted {
		t.Fatalf("migrate %v %+v", err, m)
	}
	assertUnchanged(t, before, src)
	for _, rel := range append(leftovers, ".purge-trash") {
		if exists(filepath.Join(dst, filepath.FromSlash(rel))) {
			t.Fatalf("%s copied", rel)
		}
	}
	excluded := strings.Join(m.Excluded, ",")
	for _, rel := range append(leftovers, ".purge-trash/20251201-ALL-1-1-0/xlog/xlog_tim.kfile") {
		if !strings.Contains(excluded, rel) {
			t.Fatalf("%s not recorded as excluded: %v", rel, m.Excluded)
		}
	}
	if v, err := Verify(context.Background(), VerifyOptions{Dst: dst, Location: seoul(t), Now: 100}); err != nil || v.Failed() {
		t.Fatalf("verify %v %+v", err, v)
	}
	if r, err := Rollback(context.Background(), RollbackOptions{Dst: dst}); err != nil || r.Status != "rolled-back" {
		t.Fatalf("rollback %v %+v", err, r)
	}
}

// Targets retention or DB delete renamed to a quarantine name (trash
// unusable) are never copied, compared or checksummed, in the root or inside
// a date directory.
func TestPurgeQuarantineIsExcluded(t *testing.T) {
	_, src, dst := fixture(t, false)
	leftovers := []string{
		".purge-quarantine-20251201/xlog/xlog_tim.kfile",
		testDate + "/.purge-quarantine-xlog/xlog_tid.kfile",
		testDate + "/xlog/.purge-quarantine-xlog_profile.kfile",
	}
	for _, rel := range leftovers {
		path := filepath.Join(src, filepath.FromSlash(rel))
		os.MkdirAll(filepath.Dir(path), 0700)
		os.WriteFile(path, []byte("garbage"), 0600)
		if !isExcluded(rel) {
			t.Fatalf("%s not excluded", rel)
		}
	}
	for _, rel := range []string{testDate + "/xlog/xlog_tid.kfile", "purge-quarantine-x", testDate + "/xlog/x.purge-quarantine-y"} {
		if isExcluded(rel) {
			t.Fatalf("%s excluded", rel)
		}
	}
	rep, err := Inspect(context.Background(), InspectOptions{Root: src, Location: seoul(t), Now: 100})
	if err != nil || rep.HasErrors() {
		t.Fatalf("inspect %v %+v", err, rep.Problems)
	}
	for _, f := range rep.Files {
		if isPurgeTrash(f.Path) && (f.Status != StatusExcluded || f.SHA256 != "") {
			t.Fatalf("quarantine not excluded %+v", f)
		}
	}
	for _, d := range rep.Dirs {
		if isPurgeTrash(d) {
			t.Fatalf("quarantine directory listed %s", d)
		}
	}
	m, err := Migrate(context.Background(), migrateOpts(t, src, dst))
	if err != nil || m.Status != ManifestCompleted {
		t.Fatalf("migrate %v %+v", err, m)
	}
	excluded := strings.Join(m.Excluded, ",")
	for _, rel := range leftovers {
		if exists(filepath.Join(dst, filepath.FromSlash(rel))) {
			t.Fatalf("%s copied", rel)
		}
		if !strings.Contains(excluded, rel) {
			t.Fatalf("%s not recorded as excluded: %v", rel, m.Excluded)
		}
	}
	for _, rel := range []string{".purge-quarantine-20251201", testDate + "/.purge-quarantine-xlog"} {
		if exists(filepath.Join(dst, filepath.FromSlash(rel))) {
			t.Fatalf("%s created in the copy", rel)
		}
	}
	if v, err := Verify(context.Background(), VerifyOptions{Dst: dst, Location: seoul(t), Now: 100}); err != nil || v.Failed() {
		t.Fatalf("verify %v %+v", err, v)
	}
}
