// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
package migrate

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

type VerifyOptions struct {
	Src, Dst string
	Manifest string // default: <dst>.manifest.json
	Location *time.Location
	Now      int64
}

type PostMigration struct {
	Modified []string `json:"modified"`
	Added    []string `json:"added"`
	Removed  []string `json:"removed"`
}

type VerifyResult struct {
	Tool          string        `json:"tool"`
	Status        string        `json:"status"` // ok or failed
	Src           string        `json:"src"`
	Dst           string        `json:"dst"`
	Manifest      string        `json:"manifest"`
	Started       string        `json:"started"`
	Finished      string        `json:"finished"`
	SrcLocks      LockState     `json:"src_locks"`
	DstLocks      LockState     `json:"dst_locks"`
	SrcSummary    *Summary      `json:"src_summary,omitempty"`
	DstSummary    *Summary      `json:"dst_summary,omitempty"`
	PostMigration PostMigration `json:"post_migration"`
	Problems      []Problem     `json:"problems"`
}

// Failed reports any error-severity problem.
func (r *VerifyResult) Failed() bool {
	for _, p := range r.Problems {
		if p.Severity == SeverityError {
			return true
		}
	}
	return false
}

func loadManifest(dst, path string) (string, *Manifest, error) {
	if path == "" {
		d, err := realPath(dst)
		if err != nil {
			return "", nil, err
		}
		path = ManifestPath(d)
	}
	m := &Manifest{}
	if err := readJSON(path, m); err != nil {
		return path, nil, err
	}
	if m.Tool != ToolVersion {
		return path, nil, fmt.Errorf("manifest %s was written by %q, expected %q", path, m.Tool, ToolVersion)
	}
	return path, m, nil
}

// Verify checks that src still matches the manifest (Java rollback evidence),
// classifies later Go writes in dst as post-migration changes, and compares
// both inspections when dst is unchanged.
func Verify(ctx context.Context, opt VerifyOptions) (*VerifyResult, error) {
	if opt.Location == nil {
		return nil, errors.New("timezone required")
	}
	path, m, err := loadManifest(opt.Dst, opt.Manifest)
	if err != nil {
		return nil, err
	}
	if m.Status != ManifestCompleted {
		return nil, fmt.Errorf("manifest status %q, expected %q", m.Status, ManifestCompleted)
	}
	dst, err := realPath(opt.Dst)
	if err != nil {
		return nil, err
	}
	src := m.Src
	if opt.Src != "" {
		if src, err = realPath(opt.Src); err != nil {
			return nil, err
		}
	}
	if src != m.Src || dst != m.Dst {
		return nil, fmt.Errorf("manifest records src %s dst %s, not %s %s", m.Src, m.Dst, src, dst)
	}
	res := &VerifyResult{Tool: ToolVersion, Src: src, Dst: dst, Manifest: path, Started: nowText(),
		PostMigration: PostMigration{Modified: []string{}, Added: []string{}, Removed: []string{}}}
	c := newCollector()
	srcRep, err := Inspect(ctx, InspectOptions{Root: src, Location: opt.Location, Now: opt.Now})
	if err != nil {
		return nil, err
	}
	dstRep, err := Inspect(ctx, InspectOptions{Root: dst, Location: opt.Location, Now: opt.Now})
	if err != nil {
		return nil, err
	}
	res.SrcLocks, res.DstLocks = srcRep.Locks, dstRep.Locks
	res.SrcSummary, res.DstSummary = &srcRep.Summary, &dstRep.Summary
	if srcRep.Locks.Live() {
		c.add(SeverityError, "lock.src_active", src, -1, "a writer is active on src; it no longer matches the migration snapshot")
	}
	if dstRep.Locks.GoWriterActive || dstRep.Locks.JavaActive {
		c.add(SeverityWarning, "lock.dst_active", dst, -1, "a writer is active on dst; results describe a moving target")
	}
	want := map[string]ManifestFile{}
	for _, f := range m.Files {
		want[f.Path] = f
	}
	// src must equal the manifest exactly.
	seen := map[string]bool{}
	for _, f := range srcRep.Files {
		if f.Status == StatusExcluded {
			continue
		}
		seen[f.Path] = true
		w, ok := want[f.Path]
		if !ok {
			c.add(SeverityError, "src.changed", f.Path, -1, "file added to src after migration")
		} else if w.SHA256 != f.SHA256 || w.Bytes != f.Bytes {
			c.add(SeverityError, "src.changed", f.Path, -1, "src file differs from manifest")
		}
	}
	for _, f := range m.Files {
		if !seen[f.Path] {
			c.add(SeverityError, "src.changed", f.Path, -1, "file removed from src after migration")
		}
	}
	// dst differences are later Go writes, not migration errors.
	seen = map[string]bool{}
	for _, f := range dstRep.Files {
		if f.Status == StatusExcluded {
			continue
		}
		seen[f.Path] = true
		w, ok := want[f.Path]
		if !ok {
			res.PostMigration.Added = append(res.PostMigration.Added, f.Path)
		} else if w.SHA256 != f.SHA256 || w.Bytes != f.Bytes {
			res.PostMigration.Modified = append(res.PostMigration.Modified, f.Path)
		}
	}
	for _, f := range m.Files {
		if !seen[f.Path] {
			res.PostMigration.Removed = append(res.PostMigration.Removed, f.Path)
		}
	}
	changes := len(res.PostMigration.Added) + len(res.PostMigration.Modified) + len(res.PostMigration.Removed)
	if changes > 0 {
		c.add(SeverityInfo, "post_migration.changes", dst, -1, "%d modified, %d added, %d removed files since migration", len(res.PostMigration.Modified), len(res.PostMigration.Added), len(res.PostMigration.Removed))
	} else {
		for _, p := range compareReports(srcRep, dstRep) {
			c.add(p.Severity, p.Code, p.Path, -1, "%s", p.Detail)
		}
	}
	// Errors accepted by --accept-recoverable remain until a Go writer repairs dst.
	if n := unacceptedErrors(srcRep.Problems, m.AcceptedProblems); n > 0 {
		c.add(SeverityError, "src.inspect_errors", src, -1, "src inspection found %d errors not accepted at migration", n)
	}
	if n := unacceptedErrors(dstRep.Problems, m.AcceptedProblems); n > 0 {
		c.add(SeverityError, "dst.inspect_errors", dst, -1, "dst inspection found %d errors not accepted at migration", n)
	}
	if len(m.AcceptedProblems) > 0 {
		c.add(SeverityInfo, "migrate.accepted_problems", dst, -1, "%d auto-recoverable src errors were accepted at migration", len(m.AcceptedProblems))
	}
	res.Problems = c.finish()
	res.Status = "ok"
	if res.Failed() {
		res.Status = "failed"
	}
	res.Finished = nowText()
	return res, nil
}

type RollbackOptions struct {
	Dst      string
	Manifest string
}

type RollbackResult struct {
	Tool          string    `json:"tool"`
	Status        string    `json:"status"` // rolled-back or refused
	Src           string    `json:"src"`
	Dst           string    `json:"dst"`
	RenamedTo     string    `json:"renamed_to,omitempty"`
	Manifest      string    `json:"manifest"`
	Started       string    `json:"started"`
	Finished      string    `json:"finished"`
	VerifiedFiles int       `json:"verified_src_files"`
	SrcLocks      LockState `json:"src_locks"`
	DstLocks      LockState `json:"dst_locks"`
	Problems      []Problem `json:"problems"`
}

// Rollback returns operations to the Java source: it proves src still equals
// the manifest checksums and renames dst aside (never deletes it). It refuses
// while any writer is live on dst or a Java writer already runs on src.
func Rollback(ctx context.Context, opt RollbackOptions) (*RollbackResult, error) {
	path, m, err := loadManifest(opt.Dst, opt.Manifest)
	if err != nil {
		return nil, err
	}
	dst, err := realPath(opt.Dst)
	if err != nil {
		return nil, err
	}
	release, err := lockDestination(dst)
	if err != nil {
		return nil, err
	}
	defer release()
	res := &RollbackResult{Tool: ToolVersion, Status: "refused", Src: m.Src, Dst: dst, Manifest: path, Started: nowText()}
	finish := func(format string, args ...any) (*RollbackResult, error) {
		res.Finished = nowText()
		return res, fmt.Errorf(format, args...)
	}
	if m.Dst != dst {
		return finish("manifest destination %s differs from %s", m.Dst, dst)
	}
	if m.Status != ManifestCompleted {
		return finish("manifest status %q, expected %q", m.Status, ManifestCompleted)
	}
	info, err := os.Lstat(dst)
	if err != nil {
		return finish("dst: %w", err)
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.IsDir() {
		return finish("dst %s is not a directory", dst)
	}
	if res.DstLocks, err = checkLocks(dst); err != nil {
		return nil, err
	}
	if res.DstLocks.Live() {
		return finish("dst has a live writer (go=%v heartbeat=%v); stop the Go server first", res.DstLocks.GoWriterActive, res.DstLocks.JavaActive)
	}
	if res.SrcLocks, err = checkLocks(m.Src); err != nil {
		return nil, err
	}
	if res.SrcLocks.Live() {
		return finish("src already has a live writer; verify before restarting Java")
	}
	problems, err := diffTree(ctx, m.Src, m.Files)
	if err != nil {
		return nil, err
	}
	if len(problems) > 0 {
		res.Problems = problems
		return finish("src differs from the migration manifest in %d places; Java restart on src is not proven safe", len(problems))
	}
	res.VerifiedFiles = len(m.Files)
	target := fmt.Sprintf("%s.rolledback-%s", dst, time.Now().UTC().Format("20060102T150405.000000000Z"))
	if _, err := os.Lstat(target); err == nil {
		return finish("rollback target %s already exists", target)
	}
	// Re-check immediately before the rename to narrow the race with a starting writer.
	if locks, err := checkLocks(dst); err != nil {
		return finish("dst lock check: %w", err)
	} else if locks.Live() {
		return finish("a writer started on dst before the rename")
	}
	if err = os.Rename(dst, target); err != nil {
		return finish("rename dst: %w", err)
	}
	if err = syncDir(filepath.Dir(dst)); err != nil {
		return finish("sync parent: %w", err)
	}
	res.RenamedTo, res.Status = target, "rolled-back"
	m.Status, m.RolledBackTo, m.RolledBackAt = ManifestRolledBack, target, nowText()
	if err = writeJSONAtomic(path, m); err != nil {
		return finish("dst renamed to %s but manifest update failed: %w", target, err)
	}
	res.Finished = nowText()
	return res, nil
}

// LoadManifest reads <dst>.manifest.json, or path when given.
func LoadManifest(dst, path string) (string, *Manifest, error) { return loadManifest(dst, path) }
