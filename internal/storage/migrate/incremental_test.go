package migrate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

// relFiles maps regular files below root (slash paths) to their state.
func relFiles(t testing.TB, root string) map[string]nodeState {
	t.Helper()
	out := map[string]nodeState{}
	for path, st := range capture(t, root) {
		if st.mode.IsRegular() {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				t.Fatal(err)
			}
			out[filepath.ToSlash(rel)] = st
		}
	}
	return out
}

// changedFiles lists non-excluded files that are new or differ from before.
func changedFiles(before, after map[string]nodeState) []string {
	var out []string
	for rel, st := range after {
		if isExcluded(rel) {
			continue
		}
		if b, ok := before[rel]; !ok || b.sum != st.sum || b.size != st.size || !b.mtime.Equal(st.mtime) {
			out = append(out, rel)
		}
	}
	sort.Strings(out)
	return out
}

var ageStep time.Duration

// age moves the mtime of rels an hour back, like a writer that went quiet
// long before the next copy (otherwise every fresh file is racy).
func age(t testing.TB, root string, rels []string) {
	t.Helper()
	ageStep += time.Second
	old := time.Now().Add(-time.Hour - ageStep)
	for _, rel := range rels {
		if err := os.Chtimes(filepath.Join(root, filepath.FromSlash(rel)), old, old); err != nil {
			t.Fatal(err)
		}
	}
}

func ageAll(t testing.TB, root string) {
	t.Helper()
	var rels []string
	for rel := range relFiles(t, root) {
		if !isExcluded(rel) {
			rels = append(rels, rel)
		}
	}
	age(t, root, rels)
}

func setJavaLock(t testing.TB, src string, live bool) {
	t.Helper()
	at := time.Now()
	if !live {
		at = at.Add(-time.Hour)
	}
	if err := os.Chtimes(filepath.Join(src, javaLockName), at, at); err != nil {
		t.Fatal(err)
	}
}

// javaWrites appends to an existing date (data append and header rewrite),
// starts a new date and leaves the Go writer's runtime files, as a running
// server would between two copies.
func javaWrites(t testing.TB, src string, zip bool, tx int64) {
	t.Helper()
	s, err := storage.Open(storage.Config{Directory: src, Location: seoul(t), CompressXLog: zip, CompressProfile: zip})
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range []protocol.XLogPack{
		{TxID: tx, GxID: 9001, EndTime: baseTime + 700 + tx, ObjHash: 42, Service: 7, Elapsed: 5},
		{TxID: tx + 1, EndTime: baseTime + 86400000 + tx, ObjHash: 42, Service: 7, Elapsed: 5},
	} {
		if err = s.PutXLog(&x); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.PutProfile(&protocol.ProfilePack{Time: baseTime + 86400000, TxID: tx + 1, Profile: []byte{5, 6}}); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeFile(t testing.TB, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

// sameDst requires byte-identical regular files with equal mtime and mode.
func sameDst(t testing.TB, a, b string) {
	t.Helper()
	fa, fb := relFiles(t, a), relFiles(t, b)
	if len(fa) != len(fb) {
		t.Fatalf("file counts differ: %d vs %d", len(fa), len(fb))
	}
	for rel, x := range fa {
		y, ok := fb[rel]
		if !ok || x.sum != y.sum || x.size != y.size || !x.mtime.Equal(y.mtime) || x.mode != y.mode {
			t.Fatalf("%s differs: %+v vs %+v", rel, x, y)
		}
	}
}

// sameManifest compares what a verify/rollback and an operator rely on.
func sameManifest(t testing.TB, inc, full *Manifest) {
	t.Helper()
	if inc.Status != full.Status || !reflect.DeepEqual(inc.Files, full.Files) || !reflect.DeepEqual(inc.Excluded, full.Excluded) ||
		!reflect.DeepEqual(inc.Dirs, full.Dirs) || inc.TotalBytes != full.TotalBytes {
		t.Fatalf("manifest differs:\n%+v\n%+v", inc, full)
	}
	if !reflect.DeepEqual(*inc.SrcSummary, *full.SrcSummary) {
		t.Fatalf("summary differs:\n%+v\n%+v", *inc.SrcSummary, *full.SrcSummary)
	}
	if !reflect.DeepEqual(inc.Query.Compared, full.Query.Compared) || problemKeys(inc.AcceptedProblems) != problemKeys(full.AcceptedProblems) {
		t.Fatalf("query/accepted differ: %v %v / %v %v", inc.Query.Compared, inc.AcceptedProblems, full.Query.Compared, full.AcceptedProblems)
	}
}

func problemKeys(ps []Problem) string {
	var out []string
	for _, p := range ps {
		off := int64(-1)
		if p.Offset != nil {
			off = *p.Offset
		}
		out = append(out, strings.Join([]string{p.Severity, p.Code, p.Path, time.Duration(off).String(), time.Duration(p.Count).String()}, "|"))
	}
	sort.Strings(out)
	return strings.Join(out, "\n")
}

// fullMigrate migrates a byte copy of src (the reference result).
func fullMigrate(t *testing.T, src string) (*Manifest, string) {
	t.Helper()
	tmp := t.TempDir()
	copySrc, dst := filepath.Join(tmp, "src"), filepath.Join(tmp, "dst")
	if err := os.CopyFS(copySrc, os.DirFS(src)); err != nil {
		t.Fatal(err)
	}
	// os.CopyFS does not keep mtimes or modes; restore them.
	for rel, st := range relFiles(t, src) {
		p := filepath.Join(copySrc, filepath.FromSlash(rel))
		if err := os.Chmod(p, st.mode.Perm()); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, st.mtime, st.mtime); err != nil {
			t.Fatal(err)
		}
	}
	opt := migrateOpts(t, copySrc, dst)
	opt.JavaStopped = true // the reference copy is never written
	m, err := Migrate(context.Background(), opt)
	if err != nil {
		t.Fatalf("full migrate: %v %+v", err, m)
	}
	// Paths in the manifest differ; everything else must match.
	return m, dst
}

// The group-wise inspection that precopy/finish reuse must reproduce a full
// Inspect exactly, including errors, suppression and per-date counts.
func TestGroupInspectionEqualsFullInspect(t *testing.T) {
	cases := map[string]func(t *testing.T, src string){
		"clean": func(*testing.T, string) {},
		"damaged": func(t *testing.T, src string) {
			truncateBy(t, filepath.Join(src, testDate, "xlog", "xlog_tim.kfile"), 2)
			patchFile(t, filepath.Join(src, testDate, "object", "obj.data"), 10, []byte{0xff, 0xff, 0xff})
			if err := os.Symlink("/etc/hosts", filepath.Join(src, testDate, "xlog", "link")); err != nil {
				t.Fatal(err)
			}
			os.MkdirAll(filepath.Join(src, purgeTrashDir, "20250101"), 0700)
			writeFile(t, filepath.Join(src, purgeTrashDir, "20250101", "x"), "trash")
			writeFile(t, filepath.Join(src, testDate, "xlog", ".scouter-123"), "tmp")
			os.MkdirAll(filepath.Join(src, "20261399"), 0700)
			writeFile(t, filepath.Join(src, "20261399", "a.kfile"), "bad")
			writeFile(t, filepath.Join(src, "top.hfile"), "orphan")
		},
	}
	for name, damage := range cases {
		for _, zip := range []bool{false, true} {
			t.Run(name, func(t *testing.T) {
				_, src, _ := fixture(t, zip)
				javaWrites(t, src, zip, 300)
				damage(t, src)
				ctx := context.Background()
				full, err := Inspect(ctx, InspectOptions{Root: src, Location: seoul(t), Now: 100})
				if err != nil {
					t.Fatal(err)
				}
				tr, err := walkTree(ctx, src)
				if err != nil {
					t.Fatal(err)
				}
				r := &incremental{ctx: ctx, opt: &MigrateOptions{Location: seoul(t), Now: 100}, p: pathSet{src: full.Root, staging: full.Root},
					st: &PrecopyState{Groups: map[string]*GroupResult{}}, records: map[string]PrecopyFile{}, src: tr}
				r.groups = map[string]*tree{}
				for _, list := range []struct {
					entries []treeEntry
					add     func(*tree, treeEntry)
				}{
					{tr.files, func(g *tree, e treeEntry) { g.files = append(g.files, e) }},
					{tr.symlinks, func(g *tree, e treeEntry) { g.symlinks = append(g.symlinks, e) }},
					{tr.others, func(g *tree, e treeEntry) { g.others = append(g.others, e) }},
				} {
					for _, e := range list.entries {
						if isExcluded(e.rel) && e.info.Mode().IsRegular() {
							continue
						}
						g := groupOf(e.rel)
						if r.groups[g] == nil {
							r.groups[g] = &tree{}
						}
						list.add(r.groups[g], e)
					}
				}
				r.finish = true
				if err = r.inspect(); err != nil {
					t.Fatal(err)
				}
				got := r.report(full.Locks, nil)
				if !reflect.DeepEqual(got.Files, full.Files) {
					t.Fatalf("files differ:\n%+v\n%+v", got.Files, full.Files)
				}
				if !reflect.DeepEqual(got.Summary, full.Summary) {
					t.Fatalf("summary differs:\n%+v\n%+v", got.Summary, full.Summary)
				}
				if problemKeys(got.Problems) != problemKeys(full.Problems) || !reflect.DeepEqual(got.Dirs, full.Dirs) {
					t.Fatalf("problems differ:\n%s\n--\n%s", problemKeys(got.Problems), problemKeys(full.Problems))
				}
				if name == "damaged" && !full.HasErrors() {
					t.Fatal("damage produced no errors")
				}
			})
		}
	}
}

func TestPrecopyFinishMatchesFullMigrate(t *testing.T) {
	for _, zip := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "gzip"}[zip], func(t *testing.T) {
			_, src, dst := fixture(t, zip)
			writeFile(t, filepath.Join(src, "notes", "gone.txt"), "deleted later")
			writeFile(t, filepath.Join(src, "notes", "same.txt"), "AAAA")
			writeFile(t, filepath.Join(src, "notes", "ctime.txt"), "BBBB")
			os.MkdirAll(filepath.Join(src, "20251231", "xlog"), 0700)
			writeFile(t, filepath.Join(src, "20251231", "xlog", "old.txt"), "old date removed by retention")
			ageAll(t, src)
			setJavaLock(t, src, true) // Java is running
			opt := migrateOpts(t, src, dst)
			ctx := context.Background()

			before := capture(t, src)
			st, err := Precopy(ctx, opt)
			if err != nil || st.Status != PrecopyReady || st.Rounds != 1 || !st.Last.JavaActive {
				t.Fatalf("precopy 1: %v %+v", err, st)
			}
			assertUnchanged(t, before, src)
			if st.Last.RacyFiles != 0 || st.Last.CopiedFiles != st.Last.Files || st.Last.PendingGroups != 0 || exists(dst) {
				t.Fatalf("precopy 1 stats %+v", st.Last)
			}

			// Between precopy rounds the server keeps writing.
			base := relFiles(t, src)
			javaWrites(t, src, zip, 200)
			writeFile(t, filepath.Join(src, "notes", "same.txt"), "aaaa") // same size, new content and mtime
			if err = os.Remove(filepath.Join(src, "notes", "gone.txt")); err != nil {
				t.Fatal(err)
			}
			age(t, src, changedFiles(base, relFiles(t, src)))
			setJavaLock(t, src, true)
			before = capture(t, src)
			st, err = Precopy(ctx, opt)
			if err != nil || st.Rounds != 2 || st.Last.ReusedFiles == 0 || st.Last.CopiedFiles == 0 || st.Last.RemovedFiles != 1 || st.Last.ReusedGroups == 0 {
				t.Fatalf("precopy 2: %v %+v", err, st.Last)
			}
			assertUnchanged(t, before, src)

			// Last writes before Java stops: an append, a new file, a
			// rewrite that restores the mtime (only ctime shows it) and a
			// whole date removed by retention.
			base = relFiles(t, src)
			javaWrites(t, src, zip, 400)
			if err = os.RemoveAll(filepath.Join(src, "20251231")); err != nil {
				t.Fatal(err)
			}
			changed := changedFiles(base, relFiles(t, src))
			age(t, src, changed)
			ctimeFile := filepath.Join(src, "notes", "ctime.txt")
			info, _ := os.Stat(ctimeFile)
			writeFile(t, ctimeFile, "bbbb")
			if err = os.Chtimes(ctimeFile, info.ModTime(), info.ModTime()); err != nil {
				t.Fatal(err)
			}
			changed = append(changed, "notes/ctime.txt")
			var changedBytes int64
			final := relFiles(t, src)
			for _, rel := range changed {
				changedBytes += final[rel].size
			}

			// --finish refuses while Java's lock.dat is fresh.
			setJavaLock(t, src, true)
			if _, err = Finish(ctx, opt); err == nil || !strings.Contains(err.Error(), "live writer") || exists(dst) {
				t.Fatalf("finish with live Java: %v", err)
			}
			setJavaLock(t, src, false)
			before = capture(t, src)
			var mu sync.Mutex
			var roots []string
			testHookInspectorRoot = func(root string) { mu.Lock(); roots = append(roots, root); mu.Unlock() }
			m, err := Finish(ctx, opt)
			testHookInspectorRoot = nil
			if err != nil || m.Status != ManifestCompleted {
				t.Fatalf("finish: %v %+v", err, m)
			}
			assertUnchanged(t, before, src)
			for _, root := range roots {
				if root == m.Src {
					t.Fatal("finish inspected src")
				}
			}
			inc := m.Incremental
			if inc == nil || inc.PrecopyRounds != 2 || inc.Finish.CopiedFiles != len(changed) || inc.Finish.RemovedFiles != 1 {
				t.Fatalf("finish stats %+v, changed %v", inc, changed)
			}
			// Only changed files are read: once to copy, once to re-hash.
			if inc.Finish.SrcReadBytes != 2*changedBytes || inc.Finish.ReusedGroups == 0 || inc.Finish.ReusedBytes == 0 {
				t.Fatalf("finish read %d bytes, changed files hold %d; stats %+v", inc.Finish.SrcReadBytes, changedBytes, inc.Finish)
			}
			if exists(StagingPath(dst)) || exists(PrecopyStatePath(dst)) || exists(partialManifestPath(dst)) {
				t.Fatal("finish left staging state")
			}
			full, fullDst := fullMigrate(t, src)
			sameManifest(t, m, full)
			sameDst(t, dst, fullDst)
			v, err := Verify(ctx, VerifyOptions{Dst: dst, Location: seoul(t), Now: 100})
			if err != nil || v.Failed() {
				t.Fatalf("verify %v %+v", err, v)
			}
			r, err := Rollback(ctx, RollbackOptions{Dst: dst})
			if err != nil || r.Status != "rolled-back" {
				t.Fatalf("rollback %v %+v", err, r)
			}
		})
	}
}

// Files written within racyWindow are copied again however their stat looks;
// their groups are inspected only by --finish.
func TestPrecopyRacyFilesAreCopiedAgain(t *testing.T) {
	_, src, dst := fixture(t, false)
	setJavaLock(t, src, true)
	st, err := Precopy(context.Background(), migrateOpts(t, src, dst))
	if err != nil || st.Last.RacyFiles != st.Last.Files || st.Last.InspectedGroups != 0 || st.Last.PendingGroups == 0 {
		t.Fatalf("precopy of fresh files: %v %+v", err, st.Last)
	}
	setJavaLock(t, src, false)
	m, err := Finish(context.Background(), migrateOpts(t, src, dst))
	if err != nil || m.Incremental.Finish.CopiedFiles != st.Last.Files || m.Incremental.Finish.InspectedGroups == 0 {
		t.Fatalf("finish %v %+v", err, m.Incremental)
	}
	full, _ := fullMigrate(t, src)
	sameManifest(t, m, full)
}

func TestIncrementalRefusals(t *testing.T) {
	ctx := context.Background()
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
		if _, err = Precopy(ctx, migrateOpts(t, src, dst)); err == nil || !strings.Contains(err.Error(), "Go writer") || exists(StagingPath(dst)) {
			t.Fatalf("precopy beside a Go writer: %v", err)
		}
	})
	t.Run("order and options", func(t *testing.T) {
		_, src, dst := fixture(t, false)
		if _, err := Finish(ctx, migrateOpts(t, src, dst)); err == nil || !strings.Contains(err.Error(), "--precopy") {
			t.Fatalf("finish without precopy: %v", err)
		}
		ageAll(t, src)
		if _, err := Precopy(ctx, migrateOpts(t, src, dst)); err != nil {
			t.Fatal(err)
		}
		if _, err := Migrate(ctx, migrateOpts(t, src, dst)); err == nil || !strings.Contains(err.Error(), "precopy state") {
			t.Fatalf("full migrate over a precopy: %v", err)
		}
		other := migrateOpts(t, src, dst)
		other.Sample = 5
		if _, err := Finish(ctx, other); err == nil || !strings.Contains(err.Error(), "--sample") {
			t.Fatalf("finish with other sample: %v", err)
		}
		other = migrateOpts(t, src, dst)
		other.Now = 101
		if _, err := Precopy(ctx, other); err == nil || !strings.Contains(err.Error(), "--now") {
			t.Fatalf("precopy with other now: %v", err)
		}
		other = migrateOpts(t, src, dst)
		other.Fresh = true
		if _, err := Finish(ctx, other); err == nil {
			t.Fatal("finish --fresh accepted")
		}
		// --fresh restarts the precopy from nothing.
		st, err := Precopy(ctx, other)
		if err != nil || st.Rounds != 1 || st.Last.ReusedFiles != 0 {
			t.Fatalf("fresh precopy %v %+v", err, st)
		}
	})
	t.Run("full migrate staging", func(t *testing.T) {
		_, src, dst := fixture(t, false)
		failMidway(t, src, dst)
		if _, err := Precopy(ctx, migrateOpts(t, src, dst)); err == nil || !strings.Contains(err.Error(), "full migrate") {
			t.Fatalf("precopy over a full migrate staging: %v", err)
		}
	})
}

// A precopy taken while the src was damaged keeps the errors of the
// untouched group; --finish refuses exactly like a full migrate, without
// reading the damaged files again.
func TestFinishRefusesLikeFullMigrate(t *testing.T) {
	_, src, dst := fixture(t, false)
	truncateBy(t, filepath.Join(src, testDate, "xlog", "xlog.service"), 10)
	ageAll(t, src)
	st, err := Precopy(context.Background(), migrateOpts(t, src, dst))
	if err != nil || st.Last.QueriedGroups == 0 || st.Groups[testDate+"/xlog"] == nil || st.Groups[testDate+"/xlog"].Query != nil {
		t.Fatalf("precopy of damaged src %v %+v", err, st.Last)
	}
	m, err := Finish(context.Background(), migrateOpts(t, src, dst))
	var me *MigrateError
	if !errors.As(err, &me) || me.Report == nil || exists(dst) || m.Incremental.Finish.SrcReadBytes != 0 {
		t.Fatalf("finish over damaged src: %v %+v", err, m)
	}
	tmp := t.TempDir()
	_, fullErr := Migrate(context.Background(), migrateOpts(t, src, filepath.Join(tmp, "dst")))
	var fe *MigrateError
	if !errors.As(fullErr, &fe) || fe.Report.Summary.Errors != me.Report.Summary.Errors || problemKeys(fe.Report.Problems) != problemKeys(me.Report.Problems) {
		t.Fatalf("verdicts differ:\n%v\n%v", fullErr, err)
	}
}

func TestFinishInterruptedAndRerun(t *testing.T) {
	for _, marker := range []string{"finish: copying", "finish: comparing Go storage queries", "finish: re-checking src"} {
		t.Run(marker, func(t *testing.T) {
			_, src, dst := fixture(t, true)
			ageAll(t, src)
			ctx := context.Background()
			if _, err := Precopy(ctx, migrateOpts(t, src, dst)); err != nil {
				t.Fatal(err)
			}
			base := relFiles(t, src)
			javaWrites(t, src, true, 500)
			age(t, src, changedFiles(base, relFiles(t, src)))
			before := capture(t, src)
			cctx, cancel := context.WithCancel(ctx)
			opt := migrateOpts(t, src, dst)
			opt.Progress = cancelAt{marker, cancel}
			m, err := Finish(cctx, opt)
			cancel()
			if !errors.Is(err, context.Canceled) || exists(dst) || exists(ManifestPath(dst)) || m.Status != ManifestFailed {
				t.Fatalf("interrupted finish: %v %+v", err, m)
			}
			var st PrecopyState
			if err = readJSON(PrecopyStatePath(dst), &st); err != nil || st.Status != PrecopyFailed || st.Error == "" {
				t.Fatalf("state after interruption %v %+v", err, st.Status)
			}
			assertUnchanged(t, before, src)
			m, err = Finish(ctx, migrateOpts(t, src, dst))
			if err != nil || m.Status != ManifestCompleted {
				t.Fatalf("rerun %v %+v", err, m)
			}
			assertUnchanged(t, before, src)
			full, fullDst := fullMigrate(t, src)
			sameManifest(t, m, full)
			sameDst(t, dst, fullDst)
		})
	}
}

// A staging copy changed behind the state (disk error, operator) is copied
// again instead of being trusted.
func TestFinishRecopiesTamperedStaging(t *testing.T) {
	_, src, dst := fixture(t, false)
	ageAll(t, src)
	if _, err := Precopy(context.Background(), migrateOpts(t, src, dst)); err != nil {
		t.Fatal(err)
	}
	appendFile(t, filepath.Join(StagingPath(dst), "notes", "readme.txt"), []byte("x"))
	m, err := Finish(context.Background(), migrateOpts(t, src, dst))
	if err != nil || m.Incremental.Finish.CopiedFiles != 1 {
		t.Fatalf("finish %v %+v", err, m.Incremental)
	}
	full, fullDst := fullMigrate(t, src)
	sameManifest(t, m, full)
	sameDst(t, dst, fullDst)
}

// agedFixture is a src whose files went quiet long ago, with a stale lock.dat.
func agedFixture(t *testing.T) (string, string) {
	t.Helper()
	_, src, dst := fixture(t, false)
	writeFile(t, filepath.Join(src, "notes", "gone.txt"), "deleted later")
	writeFile(t, filepath.Join(src, "notes", "same.txt"), "AAAA")
	ageAll(t, src)
	return src, dst
}

func finishMatchesFull(t *testing.T, src, dst string, opt MigrateOptions) *Manifest {
	t.Helper()
	m, err := Finish(context.Background(), opt)
	if err != nil || m.Status != ManifestCompleted {
		t.Fatalf("finish %v %+v", err, m)
	}
	full, fullDst := fullMigrate(t, src)
	sameManifest(t, m, full)
	sameDst(t, dst, fullDst)
	return m
}

func TestIncrementalConcurrentRunsRefused(t *testing.T) {
	ctx := context.Background()
	src, dst := agedFixture(t)
	if _, err := Precopy(ctx, migrateOpts(t, src, dst)); err != nil {
		t.Fatal(err)
	}
	real, err := realPath(dst)
	if err != nil {
		t.Fatal(err)
	}
	release, err := lockDestination(real)
	if err != nil {
		t.Fatal(err)
	}
	for name, run := range map[string]func() error{
		"precopy": func() error { _, err := Precopy(ctx, migrateOpts(t, src, dst)); return err },
		"finish":  func() error { _, err := Finish(ctx, migrateOpts(t, src, dst)); return err },
		"migrate": func() error {
			o := migrateOpts(t, src, dst)
			o.Fresh = true
			_, err := Migrate(ctx, o)
			return err
		},
	} {
		if err := run(); err == nil || !strings.Contains(err.Error(), "holds") {
			t.Fatalf("%s while another run holds the lock: %v", name, err)
		}
	}
	release()
	finishMatchesFull(t, src, dst, migrateOpts(t, src, dst))
	if release, err = lockDestination(real); err != nil {
		t.Fatal(err)
	}
	if _, err = Rollback(ctx, RollbackOptions{Dst: dst}); err == nil || !strings.Contains(err.Error(), "holds") || !exists(dst) {
		t.Fatalf("rollback while locked: %v", err)
	}
	release()
}

// Java deletes lock.dat on a normal exit and keeps flushing after its
// heartbeat stops, so a lock.dat a few seconds old is not proof of a stop.
func TestCopyRequiresJavaExit(t *testing.T) {
	ctx := context.Background()
	src, dst := agedFixture(t)
	if _, err := Precopy(ctx, migrateOpts(t, src, dst)); err != nil {
		t.Fatal(err)
	}
	recent := time.Now().Add(-10 * time.Second)
	os.Chtimes(filepath.Join(src, javaLockName), recent, recent)
	if _, err := Finish(ctx, migrateOpts(t, src, dst)); err == nil || !strings.Contains(err.Error(), "--java-stopped") {
		t.Fatalf("finish with a 10s-old lock.dat: %v", err)
	}
	if _, err := Migrate(ctx, migrateOpts(t, src, filepath.Join(t.TempDir(), "x"))); err == nil || !strings.Contains(err.Error(), "--java-stopped") {
		t.Fatalf("migrate with a 10s-old lock.dat: %v", err)
	}
	opt := migrateOpts(t, src, dst)
	opt.JavaStopped = true
	finishMatchesFull(t, src, dst, opt)
	// A normal Java exit leaves no lock.dat at all.
	src2, dst2 := agedFixture(t)
	if err := os.Remove(filepath.Join(src2, javaLockName)); err != nil {
		t.Fatal(err)
	}
	if _, err := Precopy(ctx, migrateOpts(t, src2, dst2)); err != nil {
		t.Fatal(err)
	}
	finishMatchesFull(t, src2, dst2, migrateOpts(t, src2, dst2))
}

// A write that lands while a file is read makes the precopy copy racy and
// fails --finish; files that vanish between the walk and the copy (or during
// the walk) are removals.
func TestCopyRacesAndVanishingFiles(t *testing.T) {
	ctx := context.Background()
	src, dst := agedFixture(t)
	os.MkdirAll(filepath.Join(src, "20251231", "xlog"), 0700)
	writeFile(t, filepath.Join(src, "20251231", "xlog", "old.txt"), "retention removes this date")
	ageAll(t, src)
	target := filepath.Join(src, "notes", "readme.txt")
	appended := false
	testHookCopied = func(path string) {
		if strings.HasSuffix(path, "/notes/readme.txt") && !appended {
			appended = true
			appendFile(t, target, []byte(" more"))
		}
		if exists(filepath.Join(src, "notes", "gone.txt")) {
			os.Remove(filepath.Join(src, "notes", "gone.txt")) // planned, then gone
		}
	}
	testHookWalk = func(root, rel string) {
		if rel == "20251231" && strings.HasSuffix(root, "src") {
			os.RemoveAll(filepath.Join(src, "20251231"))
		}
	}
	st, err := Precopy(ctx, migrateOpts(t, src, dst))
	testHookCopied, testHookWalk = nil, nil
	if err != nil || st.Last.RacyFiles != 1 || st.Last.RemovedFiles != 1 || st.Last.PendingGroups != 1 {
		t.Fatalf("precopy with races: %v %+v", err, st.Last)
	}
	if exists(filepath.Join(StagingPath(dst), "20251231")) || exists(filepath.Join(StagingPath(dst), "notes", "gone.txt")) {
		t.Fatal("vanished entries staged")
	}
	age(t, src, []string{"notes/readme.txt"})
	appended = false
	testHookCopied = func(path string) {
		if strings.HasSuffix(path, "/notes/readme.txt") && !appended {
			appended = true
			appendFile(t, target, []byte("!"))
		}
	}
	_, err = Finish(ctx, migrateOpts(t, src, dst))
	testHookCopied = nil
	if err == nil || !strings.Contains(err.Error(), "changed while it was copied") || exists(dst) {
		t.Fatalf("finish with a write during the copy: %v", err)
	}
	age(t, src, []string{"notes/readme.txt"})
	finishMatchesFull(t, src, dst, migrateOpts(t, src, dst))
}

func TestPrecopyInterruptedAndRerun(t *testing.T) {
	ctx := context.Background()
	src, dst := agedFixture(t)
	cctx, cancel := context.WithCancel(ctx)
	copies := 0
	testHookCopied = func(string) {
		if copies++; copies == 5 {
			cancel()
		}
	}
	_, err := Precopy(cctx, migrateOpts(t, src, dst))
	testHookCopied = nil
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted precopy: %v", err)
	}
	var st PrecopyState
	if err = readJSON(PrecopyStatePath(dst), &st); err != nil || st.Status != PrecopyFailed {
		t.Fatalf("state %v %s", err, st.Status)
	}
	if _, err = Precopy(ctx, migrateOpts(t, src, dst)); err != nil {
		t.Fatal(err)
	}
	finishMatchesFull(t, src, dst, migrateOpts(t, src, dst))
}

// Changes only stat identity reveals: a symlink appearing in a reused group,
// a cp -p style replacement (same size and mtime, new inode), and a write
// just before --finish.
func TestFinishDetectsIdentityChanges(t *testing.T) {
	ctx := context.Background()
	src, dst := agedFixture(t)
	if _, err := Precopy(ctx, migrateOpts(t, src, dst)); err != nil {
		t.Fatal(err)
	}
	same := filepath.Join(src, "notes", "same.txt")
	info, _ := os.Stat(same)
	tmp := filepath.Join(src, "notes", "same.tmp")
	writeFile(t, tmp, "aaaa")
	os.Chtimes(tmp, info.ModTime(), info.ModTime())
	if err := os.Rename(tmp, same); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(src, "notes", "readme.txt"), "operator NOTE") // same size, fresh mtime
	m := finishMatchesFull(t, src, dst, migrateOpts(t, src, dst))
	if m.Incremental.Finish.CopiedFiles != 2 {
		t.Fatalf("copied %+v", m.Incremental.Finish)
	}

	src, dst = agedFixture(t)
	if _, err := Precopy(ctx, migrateOpts(t, src, dst)); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/etc/hosts", filepath.Join(src, testDate, "xlog", "link")); err != nil {
		t.Fatal(err)
	}
	_, err := Finish(ctx, migrateOpts(t, src, dst))
	var me *MigrateError
	_, fullErr := Migrate(ctx, migrateOpts(t, src, filepath.Join(t.TempDir(), "full")))
	var fe *MigrateError
	if !errors.As(err, &me) || !errors.As(fullErr, &fe) || !hasProblem(me.Report, SeverityError, "fs.symlink") || problemKeys(me.Report.Problems) != problemKeys(fe.Report.Problems) {
		t.Fatalf("symlink in a reused group: %v / %v", err, fullErr)
	}
}

// Without --rehash-staging a staging copy rewritten in place with its size
// and mtime kept is trusted; with it the copy is checked and replaced.
func TestFinishRehashStaging(t *testing.T) {
	src, dst := agedFixture(t)
	if _, err := Precopy(context.Background(), migrateOpts(t, src, dst)); err != nil {
		t.Fatal(err)
	}
	staged := filepath.Join(StagingPath(dst), "notes", "same.txt")
	info, _ := os.Stat(staged)
	patchFile(t, staged, 0, []byte("Z"))
	os.Chtimes(staged, info.ModTime(), info.ModTime())
	opt := migrateOpts(t, src, dst)
	opt.RehashStaging = true
	m := finishMatchesFull(t, src, dst, opt)
	if m.Incremental.Finish.CopiedFiles != 1 || m.Incremental.Finish.StagingRehashBytes == 0 || m.Incremental.Finish.SrcReadBytes != 2*4 {
		t.Fatalf("rehash-staging %+v", m.Incremental.Finish)
	}
}

// Query steps of a selected group run even when the report given to the
// comparison has no record counts for its date.
func TestQueryGroupsRunSelectedDates(t *testing.T) {
	_, src, _ := fixture(t, false)
	other := filepath.Join(t.TempDir(), "other")
	s, err := storage.Open(storage.Config{Directory: other, Location: seoul(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.PutXLog(&protocol.XLogPack{TxID: 1, EndTime: baseTime, ObjHash: 42}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	rep := &Report{Summary: Summary{Dates: map[string]map[string]int64{}}}
	g := testDate + "/object"
	res, err := compareQueryGroups(context.Background(), src, other, rep, queryOptions{Location: seoul(t), Now: 100, Sample: 10}, map[string]bool{g: true})
	if err != nil || len(res[g].Problems) == 0 || res[g].Problems[0].Code != "query.mismatch" {
		t.Fatalf("selected object group not compared: %v %+v", err, res[g])
	}
}

// Tag count files in retention trash are not DB content and are not checked.
func TestInspectSkipsTrashTagChains(t *testing.T) {
	_, src, _ := fixture(t, false)
	trash := filepath.Join(src, purgeTrashDir, "20250101", "tagcnt", "java")
	os.MkdirAll(trash, 0700)
	writeFile(t, filepath.Join(trash, "first.kfile"), "garbage")
	writeFile(t, filepath.Join(trash, "first.hfile"), "garbage")
	rep, err := Inspect(context.Background(), InspectOptions{Root: src, Location: seoul(t), Now: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range rep.Problems {
		if strings.HasPrefix(p.Code, "tagcount.") {
			t.Fatalf("trash tag chain checked: %+v", p)
		}
	}
}

// A symlinked staging or state path could redirect writes into src.
func TestIncrementalRefusesSymlinkedSiblings(t *testing.T) {
	src, dst := agedFixture(t)
	if err := os.Symlink(src, StagingPath(dst)); err != nil {
		t.Fatal(err)
	}
	before := capture(t, src)
	for name, run := range map[string]func() error{
		"precopy": func() error { _, err := Precopy(context.Background(), migrateOpts(t, src, dst)); return err },
		"migrate": func() error { _, err := Migrate(context.Background(), migrateOpts(t, src, dst)); return err },
	} {
		if err := run(); err == nil || !strings.Contains(err.Error(), "symbolic link") {
			t.Fatalf("%s through a symlinked staging: %v", name, err)
		}
	}
	assertUnchanged(t, before, src)
}
