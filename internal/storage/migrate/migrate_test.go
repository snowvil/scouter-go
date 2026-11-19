package migrate

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

// fixture builds <tmp>/src with a stale Java lock.dat and returns tmp, src, dst.
func fixture(t *testing.T, zip bool) (string, string, string) {
	t.Helper()
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	buildDB(t, src, zip)
	stale := time.Now().Add(-time.Hour)
	lock := filepath.Join(src, javaLockName)
	if err := os.WriteFile(lock, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(lock, stale, stale); err != nil {
		t.Fatal(err)
	}
	return tmp, src, filepath.Join(tmp, "dst")
}
func migrateOpts(t *testing.T, src, dst string) MigrateOptions {
	return MigrateOptions{Src: src, Dst: dst, Location: seoul(t), Now: 100, Sample: 10}
}
func exists(path string) bool { _, err := os.Lstat(path); return err == nil }

func TestMigrateVerifyRollback(t *testing.T) {
	for _, zip := range []bool{false, true} {
		tmp, src, dst := fixture(t, zip)
		before := capture(t, src)
		m, err := Migrate(context.Background(), migrateOpts(t, src, dst))
		if err != nil {
			t.Fatalf("zip=%v migrate: %v %+v", zip, err, m)
		}
		assertUnchanged(t, before, src)
		if m.Status != ManifestCompleted || !exists(dst) || exists(StagingPath(dst)) || exists(partialManifestPath(dst)) {
			t.Fatalf("migration state %s", m.Status)
		}
		// buildDB closes a Go Store, which leaves its clean-shutdown marker
		// and its crash checkpoint log; neither is copied.
		var runtime []string
		for _, name := range append([]string{goCleanName, javaLockName}, goCheckpointNames...) {
			if exists(filepath.Join(src, name)) {
				runtime = append(runtime, name)
			}
			if exists(filepath.Join(dst, name)) {
				t.Fatalf("%s copied", name)
			}
		}
		sort.Strings(runtime)
		if len(runtime) < 3 || !reflect.DeepEqual(m.Excluded, runtime) {
			t.Fatalf("lock file copied/excluded wrongly: %v, want %v", m.Excluded, runtime)
		}
		for _, f := range m.Files {
			if !f.Verified {
				t.Fatalf("unverified %s", f.Path)
			}
		}
		for _, k := range []string{"object", "xlog", "xlog_txid", "profile", "status", "alert", "text_daily", "text_permanent", "kv"} {
			if m.Query.Compared[k] == 0 {
				t.Fatalf("zip=%v query %s not compared: %v", zip, k, m.Query.Compared)
			}
		}
		var stored Manifest
		if err = readJSON(ManifestPath(dst), &stored); err != nil || stored.Status != ManifestCompleted || len(stored.Files) != len(m.Files) {
			t.Fatalf("manifest %v %+v", err, stored.Status)
		}
		v, err := Verify(context.Background(), VerifyOptions{Src: src, Dst: dst, Location: seoul(t), Now: 100})
		if err != nil || v.Failed() {
			t.Fatalf("verify %v %+v", err, v)
		}
		// Later Go writes to dst are reported, not errors.
		s, err := storage.Open(storage.Config{Directory: dst, Location: seoul(t), CompressXLog: zip})
		if err != nil {
			t.Fatal(err)
		}
		if err = s.PutXLog(&protocol.XLogPack{TxID: 104, EndTime: baseTime + 900, ObjHash: 42}); err != nil {
			t.Fatal(err)
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		v, err = Verify(context.Background(), VerifyOptions{Dst: dst, Location: seoul(t), Now: 100})
		if err != nil || v.Failed() || len(v.PostMigration.Modified) == 0 || v.Status != "ok" {
			t.Fatalf("post-migration verify %v %+v", err, v)
		}
		r, err := Rollback(context.Background(), RollbackOptions{Dst: dst})
		if err != nil || r.Status != "rolled-back" || exists(dst) || !exists(r.RenamedTo) || r.VerifiedFiles != len(m.Files) {
			t.Fatalf("rollback %v %+v", err, r)
		}
		assertUnchanged(t, before, src)
		if err = readJSON(ManifestPath(dst), &stored); err != nil || stored.Status != ManifestRolledBack || stored.RolledBackTo != r.RenamedTo {
			t.Fatalf("rolled back manifest %v %+v", err, stored.Status)
		}
		if _, err = Migrate(context.Background(), migrateOpts(t, src, dst)); err == nil || !strings.Contains(err.Error(), "manifest") {
			t.Fatalf("re-migration over existing manifest: %v", err)
		}
		_ = tmp
	}
}

func TestMigrateDryRunWritesNothing(t *testing.T) {
	tmp, src, dst := fixture(t, false)
	before := capture(t, tmp)
	m, err := Migrate(context.Background(), MigrateOptions{Src: src, Dst: dst, Location: seoul(t), DryRun: true})
	if err != nil || m.Status != ManifestPlanned || m.CopyBytes == 0 || m.RequiredBytes == 0 || m.FreeBytes == 0 {
		t.Fatalf("dry-run %v %+v", err, m)
	}
	assertUnchanged(t, before, tmp)
}

// cancelAt cancels the migration when a progress line contains marker.
type cancelAt struct {
	marker string
	cancel context.CancelFunc
}

func (c cancelAt) Write(b []byte) (int, error) {
	if strings.Contains(string(b), c.marker) {
		c.cancel()
	}
	return len(b), nil
}

func failMidway(t *testing.T, src, dst string) *Manifest {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := migrateOpts(t, src, dst)
	opt.Progress = cancelAt{"comparing Go storage queries", cancel}
	m, err := Migrate(ctx, opt)
	var me *MigrateError
	if !errors.As(err, &me) || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation failure, got %v", err)
	}
	if exists(dst) || exists(ManifestPath(dst)) || !exists(StagingPath(dst)) {
		t.Fatal("failure must leave staging and no dst/manifest")
	}
	var partial Manifest
	if err = readJSON(partialManifestPath(dst), &partial); err != nil || partial.Status != ManifestFailed || partial.Error == "" {
		t.Fatalf("partial manifest %v %+v", err, partial.Status)
	}
	return m
}

func TestMigrateFailureAndResume(t *testing.T) {
	_, src, dst := fixture(t, true)
	before := capture(t, src)
	failed := failMidway(t, src, dst)
	assertUnchanged(t, before, src)
	m, err := Migrate(context.Background(), migrateOpts(t, src, dst))
	if err != nil || m.Status != ManifestCompleted || m.ReusedFiles != len(failed.Files) || m.CopyBytes != 0 {
		t.Fatalf("resume %v reused=%d/%d copy=%d", err, m.ReusedFiles, len(failed.Files), m.CopyBytes)
	}
	assertUnchanged(t, before, src)
}

func TestMigrateResumeRestartsWhenSourceChanged(t *testing.T) {
	_, src, dst := fixture(t, false)
	failMidway(t, src, dst)
	appendFile(t, filepath.Join(src, "notes", "readme.txt"), []byte(" changed"))
	m, err := Migrate(context.Background(), migrateOpts(t, src, dst))
	if err != nil || m.Status != ManifestCompleted || m.ReusedFiles != 0 {
		t.Fatalf("restart %v reused=%d", err, m.ReusedFiles)
	}
}

func TestMigrateFresh(t *testing.T) {
	_, src, dst := fixture(t, false)
	failMidway(t, src, dst)
	// A damaged staging file is not reused even without --fresh.
	opt := migrateOpts(t, src, dst)
	opt.Fresh = true
	m, err := Migrate(context.Background(), opt)
	if err != nil || m.ReusedFiles != 0 || m.Status != ManifestCompleted {
		t.Fatalf("fresh %v %+v", err, m)
	}
}

func TestMigrateResumeRecopiesDamagedStaging(t *testing.T) {
	_, src, dst := fixture(t, false)
	failed := failMidway(t, src, dst)
	appendFile(t, filepath.Join(StagingPath(dst), "notes", "readme.txt"), []byte("x"))
	m, err := Migrate(context.Background(), migrateOpts(t, src, dst))
	if err != nil || m.ReusedFiles != len(failed.Files)-1 {
		t.Fatalf("damaged staging %v reused=%d", err, m.ReusedFiles)
	}
}

func TestMigrateStagingWithoutManifest(t *testing.T) {
	_, src, dst := fixture(t, false)
	if err := os.MkdirAll(StagingPath(dst), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := Migrate(context.Background(), migrateOpts(t, src, dst)); err == nil || !strings.Contains(err.Error(), "--fresh") {
		t.Fatalf("unknown staging: %v", err)
	}
	if exists(dst) {
		t.Fatal("dst created")
	}
}

func TestMigrateRefusals(t *testing.T) {
	t.Run("java lock", func(t *testing.T) {
		_, src, dst := fixture(t, false)
		now := time.Now()
		os.Chtimes(filepath.Join(src, javaLockName), now, now)
		before := capture(t, src)
		if _, err := Migrate(context.Background(), migrateOpts(t, src, dst)); err == nil || !strings.Contains(err.Error(), "live writer") {
			t.Fatalf("live java: %v", err)
		}
		assertUnchanged(t, before, src)
		if exists(dst) || exists(StagingPath(dst)) || exists(partialManifestPath(dst)) {
			t.Fatal("refusal wrote destination files")
		}
	})
	t.Run("go writer", func(t *testing.T) {
		_, src, dst := fixture(t, false)
		f, err := os.OpenFile(filepath.Join(src, goLockName), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			t.Fatal(err)
		}
		if _, err = Migrate(context.Background(), migrateOpts(t, src, dst)); err == nil || !strings.Contains(err.Error(), "live writer") {
			t.Fatalf("live go: %v", err)
		}
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		m, err := Migrate(context.Background(), migrateOpts(t, src, dst))
		// .go-checkpoint, .go-clean, .go-writer.lock, lock.dat
		if err != nil || exists(filepath.Join(dst, goLockName)) || len(m.Excluded) != 4 {
			t.Fatalf("stale go lock must be excluded: %v %v", err, m.Excluded)
		}
	})
	t.Run("paths", func(t *testing.T) {
		tmp, src, _ := fixture(t, false)
		for _, dst := range []string{src, filepath.Join(src, "sub"), tmp, filepath.Join(src, "..", "src")} {
			if _, err := Migrate(context.Background(), migrateOpts(t, src, dst)); err == nil {
				t.Fatalf("accepted overlapping dst %s", dst)
			}
		}
		link := filepath.Join(tmp, "link")
		if err := os.Symlink(src, link); err != nil {
			t.Fatal(err)
		}
		if _, err := Migrate(context.Background(), migrateOpts(t, link, filepath.Join(tmp, "x"))); err == nil {
			t.Fatal("accepted symlink src")
		}
		if _, err := Migrate(context.Background(), migrateOpts(t, src, filepath.Join(link, "inner"))); err == nil {
			t.Fatal("accepted dst nested through symlink alias")
		}
		full := filepath.Join(tmp, "full")
		os.MkdirAll(full, 0700)
		os.WriteFile(filepath.Join(full, "x"), nil, 0600)
		if _, err := Migrate(context.Background(), migrateOpts(t, src, full)); err == nil || !strings.Contains(err.Error(), "not empty") {
			t.Fatalf("non-empty dst: %v", err)
		}
		empty := filepath.Join(tmp, "empty")
		os.MkdirAll(empty, 0700)
		if m, err := Migrate(context.Background(), migrateOpts(t, src, empty)); err != nil || m.Status != ManifestCompleted {
			t.Fatalf("empty dst: %v", err)
		}
	})
	t.Run("corrupt src", func(t *testing.T) {
		_, src, dst := fixture(t, false)
		truncateBy(t, filepath.Join(src, testDate, "xlog", "xlog_tim.kfile"), 2)
		_, err := Migrate(context.Background(), migrateOpts(t, src, dst))
		var me *MigrateError
		if !errors.As(err, &me) || me.Report == nil || !me.Report.HasErrors() || exists(dst) || exists(StagingPath(dst)) {
			t.Fatalf("corrupt src: %v", err)
		}
	})
}

func TestVerifyDetectsSourceChange(t *testing.T) {
	_, src, dst := fixture(t, false)
	if _, err := Migrate(context.Background(), migrateOpts(t, src, dst)); err != nil {
		t.Fatal(err)
	}
	appendFile(t, filepath.Join(src, "notes", "readme.txt"), []byte("!"))
	v, err := Verify(context.Background(), VerifyOptions{Dst: dst, Location: seoul(t)})
	if err != nil || !v.Failed() {
		t.Fatalf("verify %v %+v", err, v)
	}
	found := false
	for _, p := range v.Problems {
		found = found || p.Code == "src.changed"
	}
	if !found {
		t.Fatalf("src change not reported %+v", v.Problems)
	}
	r, err := Rollback(context.Background(), RollbackOptions{Dst: dst})
	if err == nil || r.Status != "refused" || !exists(dst) || len(r.Problems) == 0 {
		t.Fatalf("rollback with changed src: %v %+v", err, r)
	}
}

func TestRollbackRefusesLiveDestinationWriter(t *testing.T) {
	_, src, dst := fixture(t, false)
	if _, err := Migrate(context.Background(), migrateOpts(t, src, dst)); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dst, goLockName), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	r, err := Rollback(context.Background(), RollbackOptions{Dst: dst})
	if err == nil || r.Status != "refused" || !exists(dst) {
		t.Fatalf("rollback with live Go writer: %v %+v", err, r)
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	now := time.Now()
	os.WriteFile(filepath.Join(dst, javaLockName), nil, 0600)
	os.Chtimes(filepath.Join(dst, javaLockName), now, now)
	if r, err = Rollback(context.Background(), RollbackOptions{Dst: dst}); err == nil || !exists(dst) {
		t.Fatalf("rollback with fresh heartbeat: %v %+v", err, r)
	}
	old := now.Add(-time.Minute)
	os.Chtimes(filepath.Join(dst, javaLockName), old, old)
	if r, err = Rollback(context.Background(), RollbackOptions{Dst: dst}); err != nil || exists(dst) {
		t.Fatalf("rollback after writer stopped: %v %+v", err, r)
	}
}

// A crash inside the header flush window leaves xlog_tim records newer than
// their saved bucket head. Inspect keeps the warning and adds the dry-run
// repair; migration copies bytes unchanged and skips the Store comparison that
// would rewrite the copy; the first writable Go open of dst relinks them.
func TestMigrateStaleHeaderIsReportedAndCopiedUnchanged(t *testing.T) {
	_, src, dst := fixture(t, false)
	tim := filepath.Join(src, testDate, "xlog", "xlog_tim.hfile")
	stale, err := os.ReadFile(tim)
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
	if err = os.WriteFile(tim, stale, 0600); err != nil {
		t.Fatal(err)
	}
	rep, err := Inspect(context.Background(), InspectOptions{Root: src, Location: seoul(t), Now: 100})
	if err != nil || rep.HasErrors() || !hasProblem(rep, SeverityWarning, "index.unreachable_records") || !hasProblem(rep, SeverityInfo, "index.auto_recoverable") {
		t.Fatalf("inspect %v %+v", err, rep.Problems)
	}
	before := capture(t, src)
	m, err := Migrate(context.Background(), migrateOpts(t, src, dst))
	if err != nil || m.Status != ManifestCompleted {
		t.Fatalf("migrate %v %+v", err, m)
	}
	assertUnchanged(t, before, src)
	skipped := false
	for _, p := range m.Query.Problems {
		skipped = skipped || (p.Code == "query.not_compared" && p.Path == testDate+"/xlog/xlog_tim")
	}
	if !skipped {
		t.Fatalf("recoverable index compared through Store: %+v", m.Query.Problems)
	}
	got, _ := os.ReadFile(filepath.Join(dst, testDate, "xlog", "xlog_tim.hfile"))
	if !bytes.Equal(got, stale) {
		t.Fatal("migration modified the copied stale header")
	}
	// Go server use of dst repairs it; verify classifies that as post-migration.
	d, err := storage.Open(storage.Config{Directory: dst, Location: seoul(t)})
	if err != nil {
		t.Fatal(err)
	}
	logs, err := d.XLogs(testDate, baseTime, baseTime+1000, false)
	if err != nil || len(logs) != 4 {
		t.Fatalf("recovered XLogs %d %v", len(logs), err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	v, err := Verify(context.Background(), VerifyOptions{Dst: dst, Location: seoul(t), Now: 100})
	if err != nil || v.Failed() || len(v.PostMigration.Modified) != 1 {
		t.Fatalf("verify after repair %v %+v", err, v)
	}
}
