// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
)

// Manifest status values.
const (
	ManifestPlanned    = "planned" // dry-run only; never written by Migrate
	ManifestCopying    = "copying"
	ManifestFailed     = "failed"
	ManifestVerified   = "verified" // staging verified, final rename pending
	ManifestCompleted  = "completed"
	ManifestRolledBack = "rolled-back"
)

type ManifestFile struct {
	Path     string `json:"path"`
	Bytes    int64  `json:"bytes"`
	SHA256   string `json:"sha256"`
	Mode     uint32 `json:"mode"`
	MTime    int64  `json:"mtime_unix_nano"`
	Verified bool   `json:"verified,omitempty"` // staging copy re-read and matched
}

type Manifest struct {
	Tool           string         `json:"tool"`
	Status         string         `json:"status"`
	Src            string         `json:"src"`
	Dst            string         `json:"dst"`
	Staging        string         `json:"staging"`
	Timezone       string         `json:"timezone"`
	Now            int64          `json:"kv_now_unix_seconds"`
	Started        string         `json:"started"`
	Finished       string         `json:"finished,omitempty"`
	Error          string         `json:"error,omitempty"`
	DryRun         bool           `json:"dry_run,omitempty"`
	Dirs           []string       `json:"dirs"`
	Files          []ManifestFile `json:"files"`
	Excluded       []string       `json:"excluded"`
	TotalBytes     int64          `json:"total_bytes"`
	CopyBytes      int64          `json:"copy_bytes"`
	ReusedFiles    int            `json:"reused_files"`
	FreeBytes      uint64         `json:"free_bytes"`
	RequiredBytes  uint64         `json:"required_bytes"`
	SrcSummary     *Summary       `json:"src_summary,omitempty"`
	StagingSummary *Summary       `json:"staging_summary,omitempty"`
	Query          *QueryResult   `json:"query,omitempty"`
	Problems       []Problem      `json:"problems,omitempty"`
	// AcceptedProblems are src errors allowed by --accept-recoverable.
	AcceptedProblems []Problem `json:"accepted_problems,omitempty"`
	RolledBackTo     string    `json:"rolled_back_to,omitempty"`
	RolledBackAt     string    `json:"rolled_back_at,omitempty"`
	// Incremental is set when the copy was finished from a precopy (--finish).
	Incremental *IncrementalStats `json:"incremental,omitempty"`
}

// Sibling paths of the destination; none of them is inside src or dst.
func StagingPath(dst string) string         { return dst + ".partial" }
func partialManifestPath(dst string) string { return dst + ".partial.json" }
func ManifestPath(dst string) string        { return dst + ".manifest.json" }
func PrecopyStatePath(dst string) string    { return dst + ".precopy.json" }

type MigrateOptions struct {
	Src, Dst string
	Location *time.Location
	Now      int64 // KV TTL Unix seconds; 0 means current time
	DryRun   bool
	// AcceptRecoverable copies a src whose only errors are ones a writable Go
	// open repairs (torn index tail, gzip count behind its block, torn final
	// gzip member of the count.dat block or a rollover orphan). Bytes are
	// copied unchanged; the repairs happen on dst later.
	AcceptRecoverable bool
	Fresh             bool  // discard an existing staging directory first
	QueryLimit        int64 // records per date and timed category; 0 means all
	Sample            int   // keyed query lookups per date/category; 0 selects 1000
	Progress          io.Writer
	// JavaStopped is the operator's confirmation that the Java process
	// exited although lock.dat is younger than javaStoppedAge.
	JavaStopped bool
	// RacyWindow (precopy/finish) is how recently modified a file may be and
	// still be trusted by stat later; 0 selects defaultRacyWindow.
	RacyWindow time.Duration
	// RehashStaging (finish) re-hashes every reused staging copy against its
	// record instead of trusting its stat.
	RehashStaging bool
}

// MigrateError carries the failed manifest or the inspect report that blocked it.
type MigrateError struct {
	Err      error
	Manifest *Manifest
	Report   *Report
}

func (e *MigrateError) Error() string { return e.Err.Error() }
func (e *MigrateError) Unwrap() error { return e.Err }

type pathSet struct{ src, dst, staging, partial, manifest, precopy string }

func resolvePaths(srcArg, dstArg string) (pathSet, error) {
	var p pathSet
	if srcArg == "" || dstArg == "" {
		return p, errors.New("--src and --dst are required")
	}
	srcAbs, err := filepath.Abs(srcArg)
	if err != nil {
		return p, err
	}
	info, err := os.Lstat(srcAbs)
	if err != nil {
		return p, err
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.IsDir() {
		return p, fmt.Errorf("src %s must be a real directory, not a symlink", srcAbs)
	}
	if p.src, err = realPath(srcAbs); err != nil {
		return p, err
	}
	if p.dst, err = realPath(dstArg); err != nil {
		return p, err
	}
	p.staging, p.partial, p.manifest, p.precopy = StagingPath(p.dst), partialManifestPath(p.dst), ManifestPath(p.dst), PrecopyStatePath(p.dst)
	for _, other := range []string{p.dst, p.staging, p.partial, p.manifest, p.precopy, p.dst + migrateLockSuffix} {
		if overlaps(p.src, other) {
			return p, fmt.Errorf("src %s and destination path %s must not be the same or nested", p.src, other)
		}
		// A symlinked sibling would redirect staging or state writes (into
		// src, for example); only real directories and files are used.
		info, err := os.Lstat(other)
		switch {
		case errors.Is(err, fs.ErrNotExist):
		case err != nil:
			return p, err
		case info.Mode()&fs.ModeSymlink != 0:
			return p, fmt.Errorf("%s is a symbolic link; refusing to write through it", other)
		case (other == p.dst || other == p.staging) != info.IsDir():
			return p, fmt.Errorf("%s has an unexpected file type %s", other, info.Mode().Type())
		}
	}
	return p, nil
}

func (o *MigrateOptions) logf(format string, args ...any) {
	if o.Progress != nil {
		fmt.Fprintf(o.Progress, format+"\n", args...)
	}
}

// checkDestination requires an absent or empty real directory under an existing parent.
func checkDestination(p pathSet) error {
	parent, err := os.Stat(filepath.Dir(p.dst))
	if err != nil {
		return fmt.Errorf("destination parent: %w", err)
	}
	if !parent.IsDir() {
		return fmt.Errorf("destination parent %s is not a directory", filepath.Dir(p.dst))
	}
	info, err := os.Lstat(p.dst)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("destination %s exists and is not a directory", p.dst)
	}
	entries, err := os.ReadDir(p.dst)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("destination %s is not empty", p.dst)
	}
	return nil
}

// Migrate copies src to dst through a sibling staging directory and verifies
// checksums, inspect results and Go storage queries before an atomic rename.
// src is never written. On failure dst is not created.
func Migrate(ctx context.Context, opt MigrateOptions) (*Manifest, error) {
	if opt.Location == nil {
		return nil, errors.New("timezone required")
	}
	if opt.Now == 0 {
		opt.Now = time.Now().Unix()
	}
	if opt.Sample <= 0 {
		opt.Sample = 1000
	}
	p, err := resolvePaths(opt.Src, opt.Dst)
	if err != nil {
		return nil, err
	}
	if !opt.DryRun {
		release, err := lockDestination(p.dst)
		if err != nil {
			return nil, err
		}
		defer release()
	}
	if err = checkDestination(p); err != nil {
		return nil, err
	}
	if _, err = os.Lstat(p.manifest); err == nil {
		return nil, fmt.Errorf("manifest %s already exists; a previous migration completed or was rolled back", p.manifest)
	}
	if _, err = os.Lstat(p.precopy); err == nil && !opt.Fresh {
		return nil, fmt.Errorf("precopy state %s exists; finish it with --finish or discard it with --fresh", p.precopy)
	}
	locks, err := checkLocks(p.src)
	if err != nil {
		return nil, err
	}
	if locks.Live() {
		return nil, fmt.Errorf("src has a live writer (java=%v go=%v); stop it before migration", locks.JavaActive, locks.GoWriterActive)
	}
	if err = locks.checkJavaStopped(opt.JavaStopped); err != nil {
		return nil, err
	}
	m := &Manifest{Tool: ToolVersion, Status: ManifestCopying, Src: p.src, Dst: p.dst, Staging: p.staging,
		Timezone: opt.Location.String(), Now: opt.Now, Started: nowText(), DryRun: opt.DryRun}
	opt.logf("inspecting src %s", p.src)
	report, err := Inspect(ctx, InspectOptions{Root: p.src, Location: opt.Location, Now: opt.Now})
	if err != nil {
		return nil, err
	}
	m.SrcSummary = &report.Summary
	if report.HasErrors() {
		accepted, rejected := splitRecoverable(report.Problems)
		if !opt.AcceptRecoverable || len(rejected) > 0 {
			m.Status = ManifestFailed
			m.Problems = report.Problems
			hint := ""
			if len(rejected) == 0 {
				hint = "; all are auto-recoverable by the Go writer, rerun with --accept-recoverable"
			}
			return m, &MigrateError{Err: fmt.Errorf("src inspect found %d errors (%d not auto-recoverable); refusing to migrate%s", report.Summary.Errors, countErrors(rejected), hint), Manifest: m, Report: report}
		}
		m.AcceptedProblems = accepted
		opt.logf("accepting %d auto-recoverable error(s); the first Go writer open of dst repairs them", len(accepted))
	}
	m.Dirs = report.Dirs
	for _, f := range report.Files {
		if f.Status == StatusExcluded {
			m.Excluded = append(m.Excluded, f.Path)
			continue
		}
		m.Files = append(m.Files, ManifestFile{Path: f.Path, Bytes: f.Bytes, SHA256: f.SHA256, Mode: f.Mode, MTime: f.MTime})
		m.TotalBytes += f.Bytes
	}
	prev, err := resumeState(p, m, opt.Fresh, opt.DryRun)
	if err != nil {
		return nil, err
	}
	reusable := map[string]bool{}
	for _, f := range prev {
		if f.Verified {
			reusable[f.Path] = true
		}
	}
	for _, f := range m.Files {
		if !reusable[f.Path] {
			m.CopyBytes += f.Bytes
		}
	}
	if m.FreeBytes, err = freeBytes(filepath.Dir(p.dst)); err != nil {
		return nil, err
	}
	m.RequiredBytes = uint64(m.CopyBytes) + uint64(m.CopyBytes)/100 + 16<<20
	if m.FreeBytes < m.RequiredBytes {
		return m, &MigrateError{Err: fmt.Errorf("insufficient free space: need %d bytes, available %d", m.RequiredBytes, m.FreeBytes), Manifest: m}
	}
	if opt.DryRun {
		m.Status = ManifestPlanned
		m.Problems = report.Problems
		m.Finished = nowText()
		return m, nil
	}
	if err = run(ctx, &opt, p, m, report, reusable); err != nil {
		m.Status, m.Error, m.Finished = ManifestFailed, err.Error(), nowText()
		if _, statErr := os.Lstat(p.staging); statErr == nil {
			err = errors.Join(err, writeJSONAtomic(p.partial, m))
		}
		return m, &MigrateError{Err: err, Manifest: m}
	}
	return m, nil
}

// resumeState returns verified files of a previous staging run for the same
// src/dst, or resets staging when --fresh is set or the source changed.
func resumeState(p pathSet, m *Manifest, fresh, dryRun bool) ([]ManifestFile, error) {
	_, stageErr := os.Lstat(p.staging)
	stagingExists := stageErr == nil
	var prev Manifest
	partialErr := readJSON(p.partial, &prev)
	if fresh {
		if !dryRun {
			if err := discardStaging(p); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	if !stagingExists {
		return nil, nil
	}
	if partialErr != nil {
		return nil, fmt.Errorf("staging %s exists without a readable %s (%v); rerun with --fresh", p.staging, p.partial, partialErr)
	}
	if prev.Src != p.src || prev.Dst != p.dst || prev.Tool != ToolVersion {
		return nil, fmt.Errorf("staging %s belongs to src %s dst %s (%s); rerun with --fresh", p.staging, prev.Src, prev.Dst, prev.Tool)
	}
	current := map[string]ManifestFile{}
	for _, f := range m.Files {
		current[f.Path] = f
	}
	for _, f := range prev.Files {
		c, ok := current[f.Path]
		if !ok || c.Bytes != f.Bytes || c.SHA256 != f.SHA256 {
			// The source changed since the interrupted run: restart from scratch.
			if !dryRun {
				if err := os.RemoveAll(p.staging); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}
	}
	return prev.Files, nil
}

func run(ctx context.Context, opt *MigrateOptions, p pathSet, m *Manifest, report *Report, reusable map[string]bool) error {
	rootInfo, err := os.Stat(p.src)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(p.staging, rootInfo.Mode().Perm()|0700); err != nil {
		return err
	}
	if err = writeJSONAtomic(p.partial, m); err != nil {
		return err
	}
	dirModes := map[string]fs.FileMode{}
	for _, d := range m.Dirs {
		info, err := os.Lstat(filepath.Join(p.src, filepath.FromSlash(d)))
		if err != nil {
			return err
		}
		dirModes[d] = info.Mode().Perm() | 0700
		if err = os.MkdirAll(filepath.Join(p.staging, filepath.FromSlash(d)), dirModes[d]); err != nil {
			return err
		}
	}
	var sinceSave int64
	for n := range m.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		f := &m.Files[n]
		target := filepath.Join(p.staging, filepath.FromSlash(f.Path))
		if reusable[f.Path] {
			if sum, size, err := hashFile(ctx, target); err == nil && sum == f.SHA256 && size == f.Bytes {
				f.Verified = true
				m.ReusedFiles++
				continue
			}
		}
		if err := copyFile(ctx, filepath.Join(p.src, filepath.FromSlash(f.Path)), target, *f); err != nil {
			return fmt.Errorf("copy %s: %w", f.Path, err)
		}
		f.Verified = true
		sinceSave += f.Bytes + 1
		if sinceSave >= 1<<30 || n%256 == 255 {
			if err := writeJSONAtomic(p.partial, m); err != nil {
				return err
			}
			sinceSave = 0
		}
	}
	for d := range dirModes {
		if err := syncDir(filepath.Join(p.staging, filepath.FromSlash(d))); err != nil {
			return err
		}
	}
	if err = syncDir(p.staging); err != nil {
		return err
	}
	if err = writeJSONAtomic(p.partial, m); err != nil {
		return err
	}
	opt.logf("copied %d files (%d reused); inspecting staging", len(m.Files), m.ReusedFiles)
	staged, err := Inspect(ctx, InspectOptions{Root: p.staging, Location: opt.Location, Now: opt.Now})
	if err != nil {
		return err
	}
	m.StagingSummary = &staged.Summary
	if diffs := compareReports(report, staged); len(diffs) > 0 {
		m.Problems = diffs
		return fmt.Errorf("staging inspect differs from src (%d differences)", len(diffs))
	}
	before, err := statSnapshot(p.staging)
	if err != nil {
		return err
	}
	opt.logf("comparing Go storage queries")
	m.Query, err = compareQueries(ctx, p.src, p.staging, report, queryOptions{Location: opt.Location, Now: opt.Now, Limit: opt.QueryLimit, Sample: opt.Sample})
	if err != nil {
		return err
	}
	after, err := statSnapshot(p.staging)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(before, after) {
		return errors.New("staging changed while Go storage queries ran")
	}
	for _, q := range m.Query.Problems {
		if q.Severity == SeverityError {
			m.Problems = m.Query.Problems
			return errors.New("Go storage query results differ from the src read-only walk")
		}
	}
	opt.logf("re-hashing src")
	if diffs, err := diffTree(ctx, p.src, m.Files); err != nil {
		return err
	} else if len(diffs) > 0 {
		m.Problems = diffs
		return errors.New("src changed during migration")
	}
	if locks, err := checkLocks(p.src); err != nil {
		return err
	} else if locks.Live() {
		return errors.New("a writer started on src during migration")
	}
	return commit(p, m)
}

// discardStaging removes the staging directory and both resume states.
func discardStaging(p pathSet) error {
	if err := os.RemoveAll(p.staging); err != nil {
		return err
	}
	for _, path := range []string{p.partial, p.precopy} {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

// commit records the verified manifest, renames staging to dst and marks the
// manifest completed. The resume states are removed last.
func commit(p pathSet, m *Manifest) error {
	m.Status = ManifestVerified
	err := writeJSONAtomic(p.manifest, m)
	if err != nil {
		return err
	}
	// rename(2) onto an empty directory is not portable (macOS returns EEXIST).
	// os.Remove only succeeds while the pre-checked destination is still empty.
	removedEmpty := false
	if info, err := os.Lstat(p.dst); err == nil && info.IsDir() {
		if err = os.Remove(p.dst); err != nil {
			return fmt.Errorf("empty destination: %w", err)
		}
		removedEmpty = true
	}
	if err = os.Rename(p.staging, p.dst); err != nil {
		if removedEmpty {
			err = errors.Join(err, os.Mkdir(p.dst, 0700))
		}
		return err
	}
	if err = syncDir(filepath.Dir(p.dst)); err != nil {
		return err
	}
	m.Status, m.Finished = ManifestCompleted, nowText()
	if err = writeJSONAtomic(p.manifest, m); err != nil {
		return err
	}
	for _, path := range []string{p.partial, p.precopy} {
		if err = os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return syncDir(filepath.Dir(p.dst))
}

// copyFile streams src to a staging file, fsyncs it and re-reads it. Both the
// streamed source bytes and the copy must match the inspected checksum.
func copyFile(ctx context.Context, src, dst string, f ManifestFile) error {
	in, err := os.OpenFile(src, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fs.FileMode(f.Mode)|0600)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, h), cancelReader{ctx, in})
	if err == nil {
		err = out.Sync()
	}
	if err = errors.Join(err, out.Close()); err != nil {
		return err
	}
	if sum := hex.EncodeToString(h.Sum(nil)); sum != f.SHA256 || n != f.Bytes {
		return fmt.Errorf("src bytes changed since inspect (sha %s size %d, inspected %s size %d)", sum, n, f.SHA256, f.Bytes)
	}
	if err = os.Chtimes(dst, time.Unix(0, f.MTime), time.Unix(0, f.MTime)); err != nil {
		return err
	}
	sum, size, err := hashFile(ctx, dst)
	if err != nil {
		return err
	}
	if sum != f.SHA256 || size != f.Bytes {
		return fmt.Errorf("staging copy checksum %s size %d differs from src %s size %d", sum, size, f.SHA256, f.Bytes)
	}
	return nil
}

type statKey struct {
	Size  int64
	MTime int64
}

func statSnapshot(root string) (map[string]statKey, error) {
	t, err := walkTree(context.Background(), root)
	if err != nil {
		return nil, err
	}
	out := map[string]statKey{}
	for _, f := range t.files {
		out[f.rel] = statKey{f.info.Size(), f.info.ModTime().UnixNano()}
	}
	for _, d := range t.dirs {
		out[d.rel+"/"] = statKey{}
	}
	return out, nil
}

// diffTree re-hashes root and compares it with manifest files, ignoring lock files.
func diffTree(ctx context.Context, root string, files []ManifestFile) ([]Problem, error) {
	t, err := walkTree(ctx, root)
	if err != nil {
		return nil, err
	}
	c := newCollector()
	want := map[string]ManifestFile{}
	for _, f := range files {
		want[f.Path] = f
	}
	for _, e := range t.symlinks {
		c.add(SeverityError, "src.changed", e.rel, -1, "symlink appeared")
	}
	for _, e := range t.others {
		c.add(SeverityError, "src.changed", e.rel, -1, "non-regular file appeared")
	}
	for _, e := range t.files {
		if isExcluded(e.rel) {
			continue
		}
		f, ok := want[e.rel]
		if !ok {
			c.add(SeverityError, "src.changed", e.rel, -1, "file added after the manifest")
			continue
		}
		delete(want, e.rel)
		sum, size, err := hashFile(ctx, filepath.Join(root, filepath.FromSlash(e.rel)))
		if err != nil {
			return nil, err
		}
		if sum != f.SHA256 || size != f.Bytes {
			c.add(SeverityError, "src.changed", e.rel, -1, "sha256 %s size %d, manifest %s size %d", sum, size, f.SHA256, f.Bytes)
		}
	}
	missing := make([]string, 0, len(want))
	for path := range want {
		missing = append(missing, path)
	}
	sort.Strings(missing)
	for _, path := range missing {
		c.add(SeverityError, "src.changed", path, -1, "file removed after the manifest")
	}
	return c.finish(), nil
}

// compareReports compares two inspections of byte-identical trees. Lock files
// and lock problems are ignored; problem details may contain absolute paths.
func compareReports(a, b *Report) []Problem {
	c := newCollector()
	files := map[string]FileEntry{}
	for _, f := range a.Files {
		if f.Status != StatusExcluded {
			files[f.Path] = f
		}
	}
	for _, f := range b.Files {
		if f.Status == StatusExcluded {
			continue
		}
		g, ok := files[f.Path]
		delete(files, f.Path)
		if !ok {
			c.add(SeverityError, "inspect.mismatch", f.Path, -1, "file only in the copy")
			continue
		}
		if g.Bytes != f.Bytes || g.SHA256 != f.SHA256 || g.Category != f.Category || g.Status != f.Status || !reflect.DeepEqual(g.Records, f.Records) {
			c.add(SeverityError, "inspect.mismatch", f.Path, -1, "file result differs")
		}
	}
	for path := range files {
		c.add(SeverityError, "inspect.mismatch", path, -1, "file missing from the copy")
	}
	key := func(ps []Problem) []string {
		var out []string
		for _, p := range ps {
			// Excluded files (locks, purge trash, temp leftovers) exist only in src.
			if strings.HasPrefix(p.Code, "lock.") || p.Code == "purge.trash_leftover" || p.Code == "temp_leftover" {
				continue
			}
			off := int64(-1)
			if p.Offset != nil {
				off = *p.Offset
			}
			out = append(out, fmt.Sprintf("%s|%s|%s|%d|%d", p.Severity, p.Code, p.Path, off, p.Count))
		}
		sort.Strings(out)
		return out
	}
	if !reflect.DeepEqual(key(a.Problems), key(b.Problems)) {
		c.add(SeverityError, "inspect.mismatch", "", -1, "problem lists differ")
	}
	if !reflect.DeepEqual(a.Summary.Dates, b.Summary.Dates) || !reflect.DeepEqual(a.Summary.Permanent, b.Summary.Permanent) {
		c.add(SeverityError, "inspect.mismatch", "", -1, "per-date/permanent counts differ")
	}
	if !reflect.DeepEqual(a.Dirs, b.Dirs) {
		c.add(SeverityError, "inspect.mismatch", "", -1, "directory lists differ")
	}
	return c.finish()
}

// splitRecoverable separates error findings that the Go writer repairs from all
// others. Only a torn index tail with an index.auto_recoverable dry run and a
// gzip count/block/torn-member finding on a block marked
// zip.auto_recoverable_block qualify.
func splitRecoverable(problems []Problem) (accepted, rejected []Problem) {
	index, zip := map[string]bool{}, map[string]bool{}
	for _, p := range problems {
		switch p.Code {
		case "index.auto_recoverable":
			index[p.Path] = true
		case "zip.auto_recoverable_block":
			zip[p.Path] = true
		}
	}
	for _, p := range problems {
		if p.Severity != SeverityError {
			continue
		}
		ok := false
		switch p.Code {
		case "index.truncated":
			ok = index[p.Path]
		case "zip.count_mismatch", "zip.untracked_block", "zip.torn_member":
			ok = zip[p.Path]
		}
		if ok {
			accepted = append(accepted, p)
		} else {
			rejected = append(rejected, p)
		}
	}
	return accepted, rejected
}

// countErrors counts listed error occurrences, including suppressed repeats.
func countErrors(problems []Problem) int64 {
	var n int64
	for _, p := range problems {
		if p.Severity == SeverityError {
			n += max(1, p.Count)
		}
	}
	return n
}

// unacceptedErrors counts error findings not covered by accepted (code, path).
func unacceptedErrors(problems, accepted []Problem) int64 {
	ok := map[[2]string]bool{}
	for _, p := range accepted {
		ok[[2]string{p.Code, p.Path}] = true
	}
	var rest []Problem
	for _, p := range problems {
		if !ok[[2]string{p.Code, p.Path}] {
			rest = append(rest, p)
		}
	}
	return countErrors(rest)
}
