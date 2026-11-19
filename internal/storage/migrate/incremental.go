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
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"
)

// Incremental migration (migration.md "증분 이관"): Precopy copies src into
// the staging directory while Java may still write it and records each
// file's size, ns mtime, ctime, inode and SHA-256 at copy time. Finish runs
// after Java stopped: files whose stat still equals the record are reused
// without reading src; changed, new and racy files are copied again. Inspect
// and query comparison run per group on the staging copy, and results of
// unchanged groups are reused, so the final report equals a full inspection.

// defaultRacyWindow: a file modified this recently before its copy started
// is never trusted by stat alone. Two effects need the margin: a later write
// within the same filesystem timestamp tick (Linux coarse clock, 1 s on some
// filesystems) leaves the stat unchanged, and the kernel updates mtime before
// it copies the written data (file_update_time precedes the copy in the
// ext4/xfs/APFS write paths), so a read racing a stalled same-size write can
// see the new mtime with the old bytes. 60 s covers stalls far beyond a
// page fault or a writeback wait; MigrateOptions.RacyWindow overrides it.
const defaultRacyWindow = 60 * time.Second

// Precopy state status values.
const (
	PrecopyCopying   = "copying"
	PrecopyReady     = "precopied"
	PrecopyFinishing = "finishing"
	PrecopyFailed    = "failed"
)

// PrecopyFile is a staged src file and the src stat it was copied at.
type PrecopyFile struct {
	ManifestFile
	CTime int64  `json:"ctime_unix_nano"`
	Inode uint64 `json:"inode"`
	// StagedMTime is the staging copy's mtime after it was set (a filesystem
	// may round it); a different staging stat means the copy was touched.
	StagedMTime int64 `json:"staged_mtime_unix_nano"`
	// Racy copies changed while read or were modified within the racy window
	// before the copy; they are always copied again.
	Racy bool `json:"racy,omitempty"`
}

func (f *PrecopyFile) sameStat(info fs.FileInfo) bool {
	ctime, ino := fileIdentity(info)
	return f.Bytes == info.Size() && f.MTime == info.ModTime().UnixNano() && f.CTime == ctime && f.Inode == ino && f.Mode == uint32(info.Mode().Perm())
}

func sameInfo(a, b fs.FileInfo) bool {
	ac, ai := fileIdentity(a)
	bc, bi := fileIdentity(b)
	return a.Size() == b.Size() && a.ModTime().Equal(b.ModTime()) && ac == bc && ai == bi && a.Mode() == b.Mode()
}

// GroupResult is the inspection of one group (groupOf) of the staging copy.
type GroupResult struct {
	Files     []FileEntry                 `json:"files"`
	Problems  []Problem                   `json:"problems"`
	Dates     map[string]map[string]int64 `json:"dates"`
	Permanent map[string]int64            `json:"permanent"`
	Specials  []string                    `json:"specials,omitempty"` // src symlinks and non-regular entries
	Query     *QueryResult                `json:"query,omitempty"`    // nil until compared
}

// RoundStats describes one precopy or finish run.
type RoundStats struct {
	Phase           string `json:"phase"` // precopy or finish
	DryRun          bool   `json:"dry_run,omitempty"`
	JavaActive      bool   `json:"java_active"`
	Files           int    `json:"files"`
	Bytes           int64  `json:"bytes"`
	CopiedFiles     int    `json:"copied_files"`
	CopiedBytes     int64  `json:"copied_bytes"`
	ReusedFiles     int    `json:"reused_files"`
	ReusedBytes     int64  `json:"reused_bytes"`
	RacyFiles       int    `json:"racy_files"`
	RemovedFiles    int    `json:"removed_files"`
	InspectedGroups int    `json:"inspected_groups"`
	ReusedGroups    int    `json:"reused_groups"`
	QueriedGroups   int    `json:"queried_groups"`
	PendingGroups   int    `json:"pending_groups"` // precopy: left to --finish (racy files or unaccepted errors)
	// SrcReadBytes counts every src byte read: copies and the final re-hash.
	SrcReadBytes int64 `json:"src_read_bytes"`
	// StagingRehashBytes counts staging bytes re-hashed by --rehash-staging.
	StagingRehashBytes int64  `json:"staging_rehash_bytes,omitempty"`
	FreeBytes          uint64 `json:"free_bytes"`
	RequiredBytes      uint64 `json:"required_bytes"`
	CopyMillis         int64  `json:"copy_ms"`
	InspectMillis      int64  `json:"inspect_ms"`
	QueryMillis        int64  `json:"query_ms"`
	CheckMillis        int64  `json:"check_ms"`
	TotalMillis        int64  `json:"total_ms"`
}

// IncrementalStats is recorded in the manifest of a finished precopy.
type IncrementalStats struct {
	PrecopyRounds int        `json:"precopy_rounds"`
	Finish        RoundStats `json:"finish"`
}

// PrecopyState is <dst>.precopy.json, the resume state of an incremental
// migration. Group results are deleted (and saved) before any of their staged
// files change, so a cached result never describes other bytes.
type PrecopyState struct {
	Tool       string                  `json:"tool"`
	Status     string                  `json:"status"`
	Src        string                  `json:"src"`
	Dst        string                  `json:"dst"`
	Staging    string                  `json:"staging"`
	Timezone   string                  `json:"timezone"`
	Now        int64                   `json:"kv_now_unix_seconds"`
	QueryLimit int64                   `json:"query_limit"`
	Sample     int                     `json:"sample"`
	Rounds     int                     `json:"rounds"`
	Started    string                  `json:"started"`
	Finished   string                  `json:"finished,omitempty"`
	Error      string                  `json:"error,omitempty"`
	Last       *RoundStats             `json:"last_round,omitempty"`
	Files      []PrecopyFile           `json:"files"`
	Groups     map[string]*GroupResult `json:"groups"`
}

// groupOf names the unit of inspection reuse. Every inspect check and query
// comparison reads only files of one group: a dated directory's first-level
// subdirectory (xlog with its gzip blocks and profiles, object, text, ...),
// otherwise the path without its extension, which keeps .kfile/.hfile/.data
// (and 00000000 text_<div>, kv_<space>) together.
func groupOf(rel string) string {
	parts := strings.SplitN(rel, "/", 3)
	if len(parts) == 3 && isDateDir(parts[0]) && parts[0] != permanentDir {
		return parts[0] + "/" + parts[1]
	}
	return strings.TrimSuffix(rel, filepath.Ext(rel))
}

// testHookInspectorRoot observes the root of every inspector (tests prove
// that finish never inspects src).
var testHookInspectorRoot func(string)

// inspectGroup inspects one group's subtree of root.
func inspectGroup(ctx context.Context, opt InspectOptions, t *tree) (*GroupResult, error) {
	in := newInspector(ctx, opt, opt.Root)
	defer in.closeData()
	files, err := in.inspectTree(t)
	if err != nil {
		return nil, err
	}
	return &GroupResult{Files: files, Problems: in.c.finish(), Dates: in.dates, Permanent: in.perm}, ctx.Err()
}

// summarize rebuilds a Summary from merged parts. Problem counts include
// suppressed repeats, as collector counts do.
func summarize(files []FileEntry, problems []Problem, dates map[string]map[string]int64, perm map[string]int64, dirs int) Summary {
	s := Summary{Files: len(files), Dirs: dirs, Statuses: map[string]int{}, Codes: map[string]int64{}, Dates: dates, Permanent: perm}
	for _, f := range files {
		s.Bytes += f.Bytes
		s.Statuses[f.Status]++
	}
	for _, p := range problems {
		n := max(1, p.Count)
		s.Codes[p.Code] += n
		switch p.Severity {
		case SeverityError:
			s.Errors += n
		case SeverityWarning:
			s.Warnings += n
		case SeverityInfo:
			s.Infos += n
		}
	}
	return s
}

type incremental struct {
	ctx     context.Context
	opt     *MigrateOptions
	p       pathSet
	st      *PrecopyState
	finish  bool
	stats   RoundStats
	records map[string]PrecopyFile
	copied  map[string]bool // copied in this run
	src     *tree           // src walk of this run
	groups  map[string]*tree
	started time.Time
}

// prepareIncremental validates paths and loads or creates the precopy state.
// The returned release ends the <dst>.migrate.lock hold (a no-op for dry runs).
func prepareIncremental(opt *MigrateOptions, finish bool) (pathSet, *PrecopyState, func(), error) {
	release := func() {}
	p, st, err := prepareLocked(opt, finish, &release)
	if err != nil {
		release()
		return p, nil, func() {}, err
	}
	return p, st, release, nil
}

func prepareLocked(opt *MigrateOptions, finish bool, release *func()) (pathSet, *PrecopyState, error) {
	if !identitySupported {
		return pathSet{}, nil, errors.New("precopy/finish need ctime and inode change detection (darwin, linux); use the full migrate here")
	}
	if opt.Location == nil {
		return pathSet{}, nil, errors.New("timezone required")
	}
	if opt.Sample <= 0 {
		opt.Sample = 1000
	}
	if opt.RacyWindow <= 0 {
		opt.RacyWindow = defaultRacyWindow
	}
	p, err := resolvePaths(opt.Src, opt.Dst)
	if err != nil {
		return p, nil, err
	}
	if !opt.DryRun {
		unlock, err := lockDestination(p.dst)
		if err != nil {
			return p, nil, err
		}
		*release = unlock
	}
	if err = checkDestination(p); err != nil {
		return p, nil, err
	}
	if _, err = os.Lstat(p.manifest); err == nil {
		return p, nil, fmt.Errorf("manifest %s already exists; a previous migration completed or was rolled back", p.manifest)
	}
	if finish && opt.Fresh {
		return p, nil, errors.New("--fresh discards the precopy; use it with --precopy")
	}
	if opt.Fresh && !opt.DryRun {
		if err = discardStaging(p); err != nil {
			return p, nil, err
		}
	}
	if _, err = os.Lstat(p.partial); err == nil && !opt.Fresh {
		return p, nil, fmt.Errorf("a full migrate left %s; rerun the full migrate or discard it with --precopy --fresh", p.partial)
	}
	_, stageErr := os.Lstat(p.staging)
	st := &PrecopyState{}
	err = readJSON(p.precopy, st)
	switch {
	case opt.Fresh || errors.Is(err, fs.ErrNotExist):
		if finish {
			return p, nil, fmt.Errorf("no precopy state %s; run migrate --precopy first", p.precopy)
		}
		if stageErr == nil && !opt.Fresh {
			return p, nil, fmt.Errorf("staging %s exists without %s; rerun with --fresh", p.staging, p.precopy)
		}
		if opt.Now == 0 {
			opt.Now = time.Now().Unix()
		}
		st = &PrecopyState{Tool: ToolVersion, Src: p.src, Dst: p.dst, Staging: p.staging, Timezone: opt.Location.String(),
			Now: opt.Now, QueryLimit: opt.QueryLimit, Sample: opt.Sample, Started: nowText(), Groups: map[string]*GroupResult{}}
		return p, st, nil
	case err != nil:
		return p, nil, err
	}
	switch {
	case st.Tool != ToolVersion || st.Src != p.src || st.Dst != p.dst:
		return p, nil, fmt.Errorf("precopy state %s belongs to src %s dst %s (%s); rerun with --precopy --fresh", p.precopy, st.Src, st.Dst, st.Tool)
	case st.Timezone != opt.Location.String() || st.Sample != opt.Sample || st.QueryLimit != opt.QueryLimit:
		return p, nil, fmt.Errorf("precopy used --timezone %s --sample %d --query-limit %d; use the same values or --precopy --fresh", st.Timezone, st.Sample, st.QueryLimit)
	case opt.Now != 0 && opt.Now != st.Now:
		return p, nil, fmt.Errorf("precopy used --now %d (KV TTL state of reused results); omit --now or use --precopy --fresh", st.Now)
	case stageErr != nil:
		return p, nil, fmt.Errorf("staging %s is missing (%v); rerun with --precopy --fresh", p.staging, stageErr)
	}
	if st.Groups == nil {
		st.Groups = map[string]*GroupResult{}
	}
	opt.Now = st.Now
	return p, st, nil
}

func newIncremental(ctx context.Context, opt *MigrateOptions, p pathSet, st *PrecopyState, finish bool) *incremental {
	r := &incremental{ctx: ctx, opt: opt, p: p, st: st, finish: finish, records: map[string]PrecopyFile{}, copied: map[string]bool{}, started: time.Now()}
	r.stats.Phase, r.stats.DryRun = "precopy", opt.DryRun
	if finish {
		r.stats.Phase = "finish"
	}
	for _, f := range st.Files {
		r.records[f.Path] = f
	}
	return r
}

func (r *incremental) save(status string) error {
	r.st.Status = status
	r.st.Files = r.st.Files[:0]
	for _, f := range r.records {
		r.st.Files = append(r.st.Files, f)
	}
	sort.Slice(r.st.Files, func(i, j int) bool { return r.st.Files[i].Path < r.st.Files[j].Path })
	stats := r.stats
	r.st.Last = &stats
	return writeJSONAtomic(r.p.precopy, r.st)
}

func (r *incremental) stagingAbs(rel string) string {
	return filepath.Join(r.p.staging, filepath.FromSlash(rel))
}

// testHookCopied runs after a src file was read, before its second fstat.
var testHookCopied func(src string)

// stagingRehashed re-hashes a reused staging copy when --rehash-staging asks
// for it at finish; the bytes read are staging bytes, never src.
func (r *incremental) stagingRehashed(f PrecopyFile) bool {
	if !r.finish || !r.opt.RehashStaging {
		return true
	}
	sum, size, err := hashFile(r.ctx, r.stagingAbs(f.Path))
	r.stats.StagingRehashBytes += size
	return err == nil && sum == f.SHA256 && size == f.Bytes
}

// stagingIntact checks a reused copy by stat only: it was verified by a
// re-read when written and nothing else writes the staging directory.
func (r *incremental) stagingIntact(f PrecopyFile) bool {
	info, err := os.Lstat(r.stagingAbs(f.Path))
	return err == nil && info.Mode().IsRegular() && info.Size() == f.Bytes && info.ModTime().UnixNano() == f.StagedMTime
}

// specials lists src symlinks and non-regular entries by group.
func (r *incremental) specials() map[string][]string {
	out := map[string][]string{}
	for _, list := range [][]treeEntry{r.src.symlinks, r.src.others} {
		for _, e := range list {
			g := groupOf(e.rel)
			out[g] = append(out[g], e.rel)
		}
	}
	for _, list := range out {
		sort.Strings(list)
	}
	return out
}

// sync copies new, changed and racy src files into staging, removes
// vanished ones and invalidates the results of every touched group.
func (r *incremental) sync() error {
	started := time.Now()
	defer func() { r.stats.CopyMillis = time.Since(started).Milliseconds() }()
	var err error
	// Java retention may delete dates during a precopy; finish runs on a
	// stopped src, where a vanishing entry is an error.
	if r.src, err = walkTreeVanishing(r.ctx, r.p.src, !r.finish); err != nil {
		return err
	}
	current := map[string]bool{}
	changed := map[string]bool{}
	var plan []treeEntry
	var planBytes int64
	for _, e := range r.src.files {
		if isExcluded(e.rel) {
			continue
		}
		current[e.rel] = true
		r.stats.Files++
		r.stats.Bytes += e.info.Size()
		if f, ok := r.records[e.rel]; ok && f.Verified && !f.Racy && f.sameStat(e.info) && r.stagingIntact(f) && r.stagingRehashed(f) {
			r.stats.ReusedFiles++
			r.stats.ReusedBytes += f.Bytes
			continue
		}
		plan = append(plan, e)
		planBytes += e.info.Size()
		changed[groupOf(e.rel)] = true
	}
	var removed []string
	for rel := range r.records {
		if !current[rel] {
			removed = append(removed, rel)
			changed[groupOf(rel)] = true
		}
	}
	sort.Strings(removed)
	specials := r.specials()
	for g, res := range r.st.Groups {
		if !slices.Equal(res.Specials, specials[g]) {
			changed[g] = true
		}
	}
	if r.stats.FreeBytes, err = freeBytes(filepath.Dir(r.p.dst)); err != nil {
		return err
	}
	r.stats.RequiredBytes = uint64(planBytes) + uint64(planBytes)/100 + 16<<20
	if r.stats.FreeBytes < r.stats.RequiredBytes {
		return fmt.Errorf("insufficient free space: need %d bytes, available %d", r.stats.RequiredBytes, r.stats.FreeBytes)
	}
	if r.opt.DryRun {
		r.stats.CopiedFiles, r.stats.CopiedBytes, r.stats.RemovedFiles = len(plan), planBytes, len(removed)
		return nil
	}
	rootInfo, err := os.Stat(r.p.src)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(r.p.staging, rootInfo.Mode().Perm()|0700); err != nil {
		return err
	}
	// Cached results go first, then the bytes they describe.
	for g := range changed {
		delete(r.st.Groups, g)
	}
	if err = r.save(PrecopyCopying); err != nil {
		return err
	}
	for _, rel := range removed {
		if err := os.Remove(r.stagingAbs(rel)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		delete(r.records, rel)
		r.stats.RemovedFiles++
	}
	srcDirs := map[string]bool{}
	for _, d := range reportDirs(r.src) {
		srcDirs[d] = true
		if err := os.MkdirAll(r.stagingAbs(d), dirMode(d, r.src)); err != nil {
			return err
		}
	}
	r.opt.logf("%s: copying %d files (%d bytes), reusing %d", r.stats.Phase, len(plan), planBytes, r.stats.ReusedFiles)
	// The state is rewritten whole, so it is saved by time and volume, not
	// per file count (O(n^2) on many small files). Unsaved records only mean
	// those changed files are copied again after a crash.
	var sinceSave int64
	lastSave := time.Now()
	for _, e := range plan {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		f, moved, read, err := snapshotCopy(r.ctx, filepath.Join(r.p.src, filepath.FromSlash(e.rel)), r.stagingAbs(e.rel), r.opt.RacyWindow)
		r.stats.SrcReadBytes += read
		if errors.Is(err, fs.ErrNotExist) && !r.finish {
			// Java retention removed it after the walk.
			if err := os.Remove(r.stagingAbs(e.rel)); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			delete(r.records, e.rel)
			r.stats.RemovedFiles++
			continue
		}
		if err != nil {
			return fmt.Errorf("copy %s: %w", e.rel, err)
		}
		if moved && r.finish {
			return fmt.Errorf("src %s changed while it was copied; a writer is active", e.rel)
		}
		f.Path = e.rel
		r.records[e.rel] = f
		r.copied[e.rel] = true
		r.stats.CopiedFiles++
		r.stats.CopiedBytes += f.Bytes
		if f.Racy {
			r.stats.RacyFiles++
		}
		sinceSave += f.Bytes + 1
		if sinceSave >= 1<<30 || time.Since(lastSave) >= 10*time.Second {
			if err := r.save(PrecopyCopying); err != nil {
				return err
			}
			sinceSave, lastSave = 0, time.Now()
		}
	}
	if err = r.cleanStaging(srcDirs); err != nil {
		return err
	}
	return r.save(PrecopyCopying)
}

// dirMode is the staging mode of a src directory.
func dirMode(rel string, t *tree) fs.FileMode {
	i := sort.Search(len(t.dirs), func(i int) bool { return t.dirs[i].rel >= rel })
	if i < len(t.dirs) && t.dirs[i].rel == rel {
		return t.dirs[i].info.Mode().Perm() | 0700
	}
	return 0700
}

// cleanStaging removes staging entries that no record or src directory owns
// (left by an interrupted run or a src deletion) and fsyncs the directories.
func (r *incremental) cleanStaging(srcDirs map[string]bool) error {
	t, err := walkTree(r.ctx, r.p.staging)
	if err != nil {
		return err
	}
	for _, list := range [][]treeEntry{t.files, t.symlinks, t.others} {
		for _, e := range list {
			if _, ok := r.records[e.rel]; !ok || !e.info.Mode().IsRegular() {
				if err := os.Remove(r.stagingAbs(e.rel)); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return err
				}
			}
		}
	}
	for n := len(t.dirs) - 1; n >= 0; n-- { // children sort after parents
		if !srcDirs[t.dirs[n].rel] {
			if err := os.RemoveAll(r.stagingAbs(t.dirs[n].rel)); err != nil {
				return err
			}
		}
	}
	for d := range srcDirs {
		if err := syncDir(r.stagingAbs(d)); err != nil {
			return err
		}
	}
	return syncDir(r.p.staging)
}

// snapshotCopy copies one src file to staging, fsyncs and re-reads the copy.
// moved reports a src change while it was read; read counts src bytes.
func snapshotCopy(ctx context.Context, src, dst string, racyWindow time.Duration) (f PrecopyFile, moved bool, read int64, err error) {
	start := time.Now()
	in, err := os.OpenFile(src, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return f, false, 0, err
	}
	defer in.Close()
	before, err := in.Stat()
	if err != nil {
		return f, false, 0, err
	}
	if !before.Mode().IsRegular() {
		return f, false, 0, fmt.Errorf("%s is not a regular file", src)
	}
	perm := before.Mode().Perm()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm|0600)
	if err != nil {
		return f, false, 0, err
	}
	err = out.Chmod(perm | 0600)
	h := sha256.New()
	if err == nil {
		read, err = io.Copy(io.MultiWriter(out, h), cancelReader{ctx, in})
	}
	if err == nil {
		err = out.Sync()
	}
	if err = errors.Join(err, out.Close()); err != nil {
		return f, false, read, err
	}
	if testHookCopied != nil {
		testHookCopied(src)
	}
	after, err := in.Stat()
	if err != nil {
		return f, false, read, err
	}
	ctime, ino := fileIdentity(before)
	mtime := before.ModTime()
	f = PrecopyFile{ManifestFile: ManifestFile{Bytes: read, SHA256: hex.EncodeToString(h.Sum(nil)), Mode: uint32(perm), MTime: mtime.UnixNano()}, CTime: ctime, Inode: ino}
	moved = read != before.Size() || !sameInfo(before, after)
	f.Racy = moved || !mtime.Before(start.Add(-racyWindow))
	if err = os.Chtimes(dst, mtime, mtime); err != nil {
		return f, moved, read, err
	}
	sum, size, err := hashFile(ctx, dst)
	if err != nil {
		return f, moved, read, err
	}
	if sum != f.SHA256 || size != f.Bytes {
		return f, moved, read, fmt.Errorf("staging copy checksum %s size %d differs from the copied %s size %d", sum, size, f.SHA256, f.Bytes)
	}
	info, err := os.Lstat(dst)
	if err != nil {
		return f, moved, read, err
	}
	f.StagedMTime, f.Verified = info.ModTime().UnixNano(), true
	return f, moved, read, nil
}

// splitGroups builds per-group subtrees of the staging copy (whose files are
// exactly the records after sync) plus the src specials, and drops cached
// results of groups that no longer exist.
func (r *incremental) splitGroups() error {
	staged, err := walkTree(r.ctx, r.p.staging)
	if err != nil {
		return err
	}
	r.groups = map[string]*tree{}
	get := func(g string) *tree {
		if r.groups[g] == nil {
			r.groups[g] = &tree{}
		}
		return r.groups[g]
	}
	for _, e := range staged.files {
		g := get(groupOf(e.rel))
		g.files = append(g.files, e)
	}
	for _, e := range r.src.symlinks {
		g := get(groupOf(e.rel))
		g.symlinks = append(g.symlinks, e)
	}
	for _, e := range r.src.others {
		g := get(groupOf(e.rel))
		g.others = append(g.others, e)
	}
	for g := range r.st.Groups {
		if r.groups[g] == nil {
			delete(r.st.Groups, g)
		}
	}
	return nil
}

func (r *incremental) groupNames() []string {
	names := make([]string, 0, len(r.groups))
	for g := range r.groups {
		names = append(names, g)
	}
	sort.Strings(names)
	return names
}

func (r *incremental) racy(g string) bool {
	for _, e := range r.groups[g].files {
		if r.records[e.rel].Racy {
			return true
		}
	}
	return false
}

// inspect inspects the staging copy of every group without a cached result.
// A precopy skips groups with racy files: they are copied again by --finish.
func (r *incremental) inspect() error {
	started := time.Now()
	defer func() { r.stats.InspectMillis = time.Since(started).Milliseconds() }()
	specials := r.specials()
	opt := InspectOptions{Root: r.p.staging, Location: r.opt.Location, Now: r.opt.Now, ProfileRefSample: defaultProfileRefCheck}
	for _, g := range r.groupNames() {
		if r.st.Groups[g] != nil {
			r.stats.ReusedGroups++
			continue
		}
		if !r.finish && r.racy(g) {
			r.stats.PendingGroups++
			continue
		}
		res, err := inspectGroup(r.ctx, opt, r.groups[g])
		if err != nil {
			return err
		}
		res.Specials = specials[g]
		r.st.Groups[g] = res
		r.stats.InspectedGroups++
	}
	r.opt.logf("%s: inspected %d groups of the staging copy, reused %d", r.stats.Phase, r.stats.InspectedGroups, r.stats.ReusedGroups)
	return nil
}

// report merges group results into the report a full Inspect of src gives:
// file entries carry the src stat, excluded files and locks come from this
// run's src walk. only limits it to some groups (nil: the whole tree).
func (r *incremental) report(locks LockState, only map[string]bool) *Report {
	rep := &Report{Tool: ToolVersion, Root: r.p.src, Timezone: r.opt.Location.String(), Now: r.opt.Now, Started: nowText(), Locks: locks, Dirs: reportDirs(r.src)}
	dates, perm := map[string]map[string]int64{}, map[string]int64{}
	if only == nil {
		// Excluded files are never read; this pass only classifies them.
		in := &inspector{ctx: r.ctx, root: r.p.src, c: newCollector(), indexBases: map[string]bool{}}
		in.lockProblems(locks)
		for _, e := range r.src.files {
			if isExcluded(e.rel) {
				entry, _ := in.fileEntry(e)
				rep.Files = append(rep.Files, *entry)
			}
		}
		rep.Problems = in.c.finish()
	}
	names := make([]string, 0, len(r.st.Groups))
	for g := range r.st.Groups {
		if only == nil || only[g] {
			names = append(names, g)
		}
	}
	sort.Strings(names)
	for _, g := range names {
		res := r.st.Groups[g]
		for _, f := range res.Files {
			if rec, ok := r.records[f.Path]; ok {
				f.Mode, f.MTime = rec.Mode, rec.MTime
			}
			rep.Files = append(rep.Files, f)
		}
		rep.Problems = append(rep.Problems, res.Problems...)
		for date, counts := range res.Dates {
			if dates[date] == nil {
				dates[date] = map[string]int64{}
			}
			for k, v := range counts {
				dates[date][k] += v
			}
		}
		for k, v := range res.Permanent {
			perm[k] += v
		}
	}
	sort.Slice(rep.Files, func(i, j int) bool { return rep.Files[i].Path < rep.Files[j].Path })
	rep.Summary = summarize(rep.Files, rep.Problems, dates, perm, len(r.src.dirs))
	rep.Finished = nowText()
	return rep
}

// query compares Go storage queries for groups without a cached comparison.
// Both readers use the staging copy, whose bytes equal src by checksum. A
// precopy leaves groups with unaccepted errors to --finish, which refuses
// them anyway unless they change.
func (r *incremental) query() error {
	started := time.Now()
	defer func() { r.stats.QueryMillis = time.Since(started).Milliseconds() }()
	need := map[string]bool{}
	for g, res := range r.st.Groups {
		if res.Query != nil {
			continue
		}
		if _, rejected := splitRecoverable(res.Problems); r.finish || len(rejected) == 0 {
			need[g] = true
		}
	}
	if len(need) == 0 {
		return nil
	}
	rep := r.report(LockState{}, need)
	// Dates come from every group, as in the full report.
	for _, res := range r.st.Groups {
		for d := range res.Dates {
			if rep.Summary.Dates[d] == nil {
				rep.Summary.Dates[d] = map[string]int64{}
			}
		}
	}
	before, err := statSnapshot(r.p.staging)
	if err != nil {
		return err
	}
	r.opt.logf("%s: comparing Go storage queries of %d groups", r.stats.Phase, len(need))
	results, err := compareQueryGroups(r.ctx, r.p.staging, r.p.staging, rep, queryOptions{Location: r.opt.Location, Now: r.opt.Now, Limit: r.opt.QueryLimit, Sample: r.opt.Sample}, need)
	if err != nil {
		return err
	}
	after, err := statSnapshot(r.p.staging)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(before, after) {
		return errors.New("staging changed while Go storage queries ran")
	}
	for g := range need {
		r.st.Groups[g].Query = results[g]
	}
	r.stats.QueriedGroups = len(need)
	return r.save(PrecopyCopying)
}

// finalCheck re-stats all of src against the records and re-hashes the files
// this run copied, which is what a full migrate's src re-hash proves for
// files that could have changed since they were read.
func (r *incremental) finalCheck() ([]Problem, error) {
	t, err := walkTree(r.ctx, r.p.src)
	if err != nil {
		return nil, err
	}
	c := newCollector()
	for _, e := range t.symlinks {
		c.add(SeverityError, "src.changed", e.rel, -1, "symlink appeared")
	}
	for _, e := range t.others {
		c.add(SeverityError, "src.changed", e.rel, -1, "non-regular file appeared")
	}
	seen := map[string]bool{}
	for _, e := range t.files {
		if isExcluded(e.rel) {
			continue
		}
		seen[e.rel] = true
		f, ok := r.records[e.rel]
		switch {
		case !ok:
			c.add(SeverityError, "src.changed", e.rel, -1, "file added after the copy")
		case !f.sameStat(e.info):
			c.add(SeverityError, "src.changed", e.rel, -1, "stat changed after the copy")
		case r.copied[e.rel]:
			sum, size, err := hashFile(r.ctx, filepath.Join(r.p.src, filepath.FromSlash(e.rel)))
			r.stats.SrcReadBytes += size
			if err != nil {
				return nil, err
			}
			if sum != f.SHA256 || size != f.Bytes {
				c.add(SeverityError, "src.changed", e.rel, -1, "sha256 %s size %d, copied %s size %d", sum, size, f.SHA256, f.Bytes)
			}
		}
	}
	missing := make([]string, 0)
	for rel := range r.records {
		if !seen[rel] {
			missing = append(missing, rel)
		}
	}
	sort.Strings(missing)
	for _, rel := range missing {
		c.add(SeverityError, "src.changed", rel, -1, "file removed after the copy")
	}
	return c.finish(), nil
}

// Precopy copies src into <dst>.partial while the Java server may still run
// (a fresh lock.dat is allowed; a live Go writer flock is not). Each round
// copies only files that are new, changed or racy since the last round and
// inspects and query-compares the staging copy of groups whose files are all
// stable. It never writes src and never creates dst.
func Precopy(ctx context.Context, opt MigrateOptions) (*PrecopyState, error) {
	p, st, release, err := prepareIncremental(&opt, false)
	if err != nil {
		return nil, err
	}
	defer release()
	locks, err := checkLocks(p.src)
	if err != nil {
		return nil, err
	}
	if locks.GoWriterActive {
		return nil, errors.New("src has a live Go writer; precopy only runs beside a Java server")
	}
	r := newIncremental(ctx, &opt, p, st, false)
	r.stats.JavaActive = locks.JavaActive
	err = r.sync()
	if err == nil && !opt.DryRun {
		err = r.splitGroups()
		if err == nil {
			err = r.inspect()
		}
		if err == nil {
			err = r.save(PrecopyCopying)
		}
		if err == nil {
			err = r.query()
		}
	}
	r.stats.TotalMillis = time.Since(r.started).Milliseconds()
	if opt.DryRun {
		st.Status, st.Last = ManifestPlanned, &r.stats
		return st, err
	}
	if err != nil {
		st.Error = err.Error()
		if _, statErr := os.Lstat(p.staging); statErr == nil {
			err = errors.Join(err, r.save(PrecopyFailed))
		}
		return st, err
	}
	st.Rounds++
	st.Error, st.Finished = "", nowText()
	return st, r.save(PrecopyReady)
}

// Finish completes a precopy after the Java server stopped: it copies what
// changed, inspects and compares only the touched groups, reuses the rest,
// re-checks src and renames staging to dst like Migrate. The result carries
// the same file list, report summary and verification rules as a full
// migrate of the final src.
func Finish(ctx context.Context, opt MigrateOptions) (*Manifest, error) {
	p, st, release, err := prepareIncremental(&opt, true)
	if err != nil {
		return nil, err
	}
	defer release()
	locks, err := checkLocks(p.src)
	if err != nil {
		return nil, err
	}
	if locks.Live() {
		return nil, fmt.Errorf("src has a live writer (java=%v go=%v); stop the Java server before --finish", locks.JavaActive, locks.GoWriterActive)
	}
	if err = locks.checkJavaStopped(opt.JavaStopped); err != nil {
		return nil, err
	}
	r := newIncremental(ctx, &opt, p, st, true)
	m := &Manifest{Tool: ToolVersion, Status: ManifestCopying, Src: p.src, Dst: p.dst, Staging: p.staging,
		Timezone: opt.Location.String(), Now: opt.Now, Started: nowText(), DryRun: opt.DryRun}
	rep, err := r.runFinish(m, locks)
	r.stats.TotalMillis = time.Since(r.started).Milliseconds()
	m.FreeBytes, m.RequiredBytes = r.stats.FreeBytes, r.stats.RequiredBytes
	m.Incremental = &IncrementalStats{PrecopyRounds: st.Rounds, Finish: r.stats}
	if opt.DryRun {
		m.Status, m.Finished = ManifestPlanned, nowText()
		return m, err
	}
	if err != nil {
		m.Status, m.Error, m.Finished = ManifestFailed, err.Error(), nowText()
		st.Error = err.Error()
		if _, statErr := os.Lstat(p.staging); statErr == nil {
			err = errors.Join(err, r.save(PrecopyFailed))
		}
		var me *MigrateError
		if errors.As(err, &me) {
			return m, err
		}
		return m, &MigrateError{Err: err, Manifest: m, Report: rep}
	}
	return m, nil
}

// runFinish returns the merged report when it was built (for refusals).
func (r *incremental) runFinish(m *Manifest, locks LockState) (*Report, error) {
	if err := r.sync(); err != nil || r.opt.DryRun {
		return nil, err
	}
	if err := r.splitGroups(); err != nil {
		return nil, err
	}
	if err := r.inspect(); err != nil {
		return nil, err
	}
	if err := r.save(PrecopyFinishing); err != nil {
		return nil, err
	}
	rep := r.report(locks, nil)
	m.SrcSummary, m.StagingSummary, m.Dirs = &rep.Summary, &rep.Summary, rep.Dirs
	for _, f := range rep.Files {
		if f.Status == StatusExcluded {
			m.Excluded = append(m.Excluded, f.Path)
			continue
		}
		mf := r.records[f.Path].ManifestFile
		m.Files = append(m.Files, mf)
		m.TotalBytes += mf.Bytes
	}
	m.CopyBytes, m.ReusedFiles = r.stats.CopiedBytes, r.stats.ReusedFiles
	if rep.HasErrors() {
		accepted, rejected := splitRecoverable(rep.Problems)
		if !r.opt.AcceptRecoverable || len(rejected) > 0 {
			m.Problems = rep.Problems
			hint := ""
			if len(rejected) == 0 {
				hint = "; all are auto-recoverable by the Go writer, rerun with --accept-recoverable"
			}
			return rep, &MigrateError{Err: fmt.Errorf("src inspect found %d errors (%d not auto-recoverable); refusing to migrate%s", rep.Summary.Errors, countErrors(rejected), hint), Manifest: m, Report: rep}
		}
		m.AcceptedProblems = accepted
		r.opt.logf("accepting %d auto-recoverable error(s); the first Go writer open of dst repairs them", len(accepted))
	}
	if err := r.query(); err != nil {
		return rep, err
	}
	groups := map[string]*QueryResult{}
	for g, res := range r.st.Groups {
		if res.Query == nil {
			return rep, fmt.Errorf("group %s has no query comparison", g)
		}
		groups[g] = res.Query
	}
	m.Query = mergeQueryResults(groups)
	for _, q := range m.Query.Problems {
		if q.Severity == SeverityError {
			m.Problems = m.Query.Problems
			return rep, errors.New("Go storage query results differ from the src read-only walk")
		}
	}
	checked := time.Now()
	r.opt.logf("finish: re-checking src (stat all, re-hash %d copied files)", len(r.copied))
	diffs, err := r.finalCheck()
	r.stats.CheckMillis = time.Since(checked).Milliseconds()
	if err != nil {
		return rep, err
	}
	if len(diffs) > 0 {
		m.Problems = diffs
		return rep, errors.New("src changed during migration")
	}
	if locks, err := checkLocks(r.p.src); err != nil {
		return rep, err
	} else if locks.Live() {
		return rep, errors.New("a writer started on src during migration")
	}
	r.stats.TotalMillis = time.Since(r.started).Milliseconds()
	m.Incremental = &IncrementalStats{PrecopyRounds: r.st.Rounds, Finish: r.stats}
	return rep, commit(r.p, m)
}
