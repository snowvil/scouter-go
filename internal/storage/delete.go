// Copyright 2015 Scouter authors, Apache-2.0.
// ManageService.deleteDbFiles (SERVER_DB_DELETE) at pinned source
// dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5: every requested name is
// `new File(rootPath + filename)`, deleted recursively, missing names are
// ignored. See storage-contract.md "DB 경로 삭제".
package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// maxDeletePaths bounds one request; Java accepts any list size.
const maxDeletePaths = 1024

// maxDeletePathBytes bounds one requested name.
const maxDeletePathBytes = 4096

// permanentDate is the date directory that holds the permanent text
// dictionary and the kv2 store; it is never a retention candidate in Java.
const permanentDate = "00000000"

// DatabaseDeleter removes DB paths named relative to the Store root the way
// SERVER_DB_DELETE does, after the cached handles below them are closed.
// Counters shares the root; without it a target that holds counter files is
// refused (a counter table left open would keep writing into unlinked files).
type DatabaseDeleter struct {
	Store    *Store
	Counters *Counters
}

// AttachCounters registers the Counters that share the Store root, so
// DeleteDatabasePaths closes their tables too (the runtime opens both on the
// same directory, as Retention uses them). c must have the Store's root.
func (s *Store) AttachCounters(c *Counters) error {
	if c == nil {
		return ErrInvalid
	}
	base, err := filepath.Abs(c.root)
	if err != nil {
		return err
	}
	if base != s.config.Directory {
		return fmt.Errorf("counters root %q is not the store root: %w", c.root, ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.counters = c
	s.handCounterSealsLocked(c) // clean-shutdown seals of counter pairs (clean.go)
	return nil
}

// DeleteDatabasePaths is DatabaseDeleter{Store: s, Counters: attached}.Delete
// with the Counters registered by AttachCounters (none by default).
func (s *Store) DeleteDatabasePaths(ctx context.Context, relPaths []string) (int, error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return 0, err
	}
	c := s.counters
	s.mu.Unlock()
	return (&DatabaseDeleter{Store: s, Counters: c}).Delete(ctx, relPaths)
}

type deleteTarget struct {
	rel  string // cleaned, slash-separated name below the root
	path string // absolute path below the Store root
	date string // first component: a yyyymmdd date or permanentDate
}

// Delete removes the named paths and returns how many existing paths left the
// DB tree. Names are the SERVER_DB_LIST form: "/" + a path relative to the DB
// root (Java builds them as getAbsolutePath().substring(rootPath.length())).
// Intended differences from Java's unchecked `rootPath + filename`:
//   - every name is validated first and nothing is deleted when one is
//     refused (ErrInvalid): an empty name, one without the leading "/" (Java
//     would concatenate it into a sibling of the root), any ".." component,
//     a NUL, backslash or invalid UTF-8, a first component that is not a
//     yyyymmdd date directory or below 00000000 (this refuses host absolute
//     paths, lock.dat, .go-writer.lock, the purge trash and 00000000 itself),
//     and an existing symlink component; more than maxDeletePaths names or a
//     name longer than maxDeletePathBytes is ErrLimit;
//   - a target that holds counter files is refused when no Counters are
//     known (see DatabaseDeleter);
//   - cached index/data/gzip/TTL handles and counter tables below a target are
//     closed first and the pending visitor/tag aggregation below it is
//     discarded, so later writes recreate files instead of writing into
//     unlinked inodes and deleted aggregates are not flushed back; the removal
//     itself is the retention rename under the gates (trash, or a quarantine
//     name next to the target when the trash is unusable, moveAway) with the
//     handle closes and the unlink after them; a target no rename can move
//     stays in place and fails (never unlinked under the gates);
//   - a failed removal is counted out of the result and returned as an error
//     instead of being ignored.
//
// A missing name is skipped (Java: file.exists() == false). The Store gate,
// then the Counters gate, serialize the call against ingest and queries; a
// query of a deleted date afterwards is an empty, error-free read, and a
// query that was already inside the gate completed before the deletion.
func (d *DatabaseDeleter) Delete(ctx context.Context, relPaths []string) (int, error) {
	s := d.Store
	if s == nil {
		return 0, ErrInvalid
	}
	if len(relPaths) > maxDeletePaths {
		return 0, ErrLimit
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	targets, err := s.deleteTargets(relPaths)
	if err != nil {
		return 0, err
	}
	trash := filepath.Join(s.config.Directory, purgeTrash)
	trashErr := os.MkdirAll(trash, 0700) // outside the gates
	var closes doomedCloses
	moved, deleted, err := d.detach(ctx, targets, trash, trashErr, &closes)
	errs := []error{err}
	// Outside the gates: close the doomed handles, then unlink.
	if err := closes.run(); err != nil {
		errs = append(errs, fmt.Errorf("db delete: close: %w", err))
	}
	for _, p := range moved {
		if err := removeOwned(p); err != nil {
			errs = append(errs, fmt.Errorf("db delete: trash cleanup %s: %w", p, err))
		}
	}
	return deleted, errors.Join(errs...)
}

// deleteTargets validates and resolves every name before anything is touched.
func (s *Store) deleteTargets(relPaths []string) ([]deleteTarget, error) {
	seen := make(map[string]bool, len(relPaths))
	targets := make([]deleteTarget, 0, len(relPaths))
	for _, rel := range relPaths {
		t, err := s.deleteTarget(rel)
		if err != nil {
			return nil, err
		}
		if seen[t.path] {
			continue
		}
		seen[t.path] = true
		targets = append(targets, t)
	}
	// Parents sort before their children, so a nested name is simply absent
	// by the time it is reached.
	sort.Slice(targets, func(i, j int) bool { return targets[i].path < targets[j].path })
	return targets, nil
}

func (s *Store) deleteTarget(rel string) (deleteTarget, error) {
	refuse := func(why string) (deleteTarget, error) {
		return deleteTarget{}, fmt.Errorf("db delete %q: %s: %w", rel, why, ErrInvalid)
	}
	if len(rel) > maxDeletePathBytes {
		return deleteTarget{}, fmt.Errorf("db delete: name length: %w", ErrLimit)
	}
	if rel == "" || !utf8.ValidString(rel) || strings.ContainsAny(rel, "\\\x00") {
		return refuse("invalid name")
	}
	if rel[0] != '/' {
		return refuse("name must start with / (Java rootPath + name)")
	}
	// ".." is refused before cleaning: path.Clean of "/../x" would silently
	// fold it back under the root while Java resolves it outside.
	for _, part := range strings.Split(rel, "/") {
		if part == ".." {
			return refuse("outside DB root")
		}
	}
	clean := path.Clean(rel)
	if clean == "/" {
		return refuse("DB root")
	}
	clean = clean[1:]
	parts := strings.Split(clean, "/")
	switch {
	case parts[0] == permanentDate && len(parts) == 1:
		return refuse("permanent directory")
	case parts[0] != permanentDate && validDate(parts[0]) != nil:
		return refuse("not below a date directory")
	}
	// Existing components must not be symlinks: listing below a symlinked
	// directory would delete files outside the DB tree.
	abs := s.config.Directory
	for _, part := range parts {
		abs = filepath.Join(abs, part)
		info, err := os.Lstat(abs)
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			return deleteTarget{}, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return refuse("symlink component")
		}
	}
	return deleteTarget{rel: clean, path: filepath.Join(s.config.Directory, filepath.FromSlash(clean)), date: parts[0]}, nil
}

// detach closes the state below every existing target and moves the target
// out of the DB tree under the Store gate (then the Counters gate), in the
// order Retention.detach uses. It returns the moved paths to unlink after the
// gates and the number of targets that left the tree. Nothing is unlinked
// under the gates; a target that cannot be moved stays and is an error.
func (d *DatabaseDeleter) detach(ctx context.Context, targets []deleteTarget, trash string, trashErr error, closes *doomedCloses) (moved []string, deleted int, err error) {
	s := d.Store
	// visitorMu and visitorIO: no visit and no in-flight snapshot write
	// recreates a deleted date. The tag gate stays held until the targets are
	// renamed away, so no tag drain, flush or query reopens a doomed tag file
	// in between.
	unlock, err := s.lockDetachGates(ctx, d.Counters, nil)
	if err != nil {
		return nil, 0, err
	}
	defer unlock()
	if d.Counters != nil && d.Counters.closed {
		return nil, 0, ErrClosed
	}
	if d.Counters == nil {
		// Checked under the Store gate before anything is removed.
		for _, t := range targets {
			counterDir := filepath.Join(s.config.Directory, t.date, "counter")
			if !under(counterDir, []string{t.path}) && !under(t.path, []string{counterDir}) {
				continue
			}
			if _, err := os.Lstat(counterDir); err == nil {
				return nil, 0, fmt.Errorf("db delete %q: counter files without attached Counters: %w", t.rel, ErrInvalid)
			} else if !os.IsNotExist(err) {
				return nil, 0, err
			}
		}
	}
	var errs []error
	for i, t := range targets {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			return moved, deleted, errors.Join(errs...)
		}
		if _, err := os.Lstat(t.path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			errs = append(errs, fmt.Errorf("db delete %s: %w", t.rel, err))
			continue
		}
		roots := []string{t.path}
		s.discardDeletedLocked(t, roots)
		// Close errors of doomed handles are reported but never keep the
		// files; they are about to be unlinked anyway.
		errs = append(errs, s.releaseDoomedLocked(roots, closes), s.releaseTTLLocked(roots, closes))
		if d.Counters != nil {
			d.Counters.markPurgedLocked(t.date)
			errs = append(errs, d.Counters.releaseUnderLocked(roots, closes))
		}
		s.clock++
		dst, _, err := moveAway(trash, trashErr, t.path, fmt.Sprintf("delete-%d-%d-%d", time.Now().UnixNano(), s.clock, i))
		if err != nil {
			// Its handles go back into their caches (doomedCloses).
			errs = append(errs, fmt.Errorf("db delete %s: not moved, left in place: %w", t.rel, err),
				s.settleDoomedLocked(d.Counters, closes, nil, roots))
			continue
		}
		errs = append(errs, s.settleDoomedLocked(d.Counters, closes, roots, nil))
		moved = append(moved, dst)
		deleted++
	}
	return moved, deleted, errors.Join(errs...)
}

// discardDeletedLocked drops pending aggregation that would recreate a deleted
// path on its next flush: tag-count tables below roots, and the visitor daily
// (visit) or hourly (visit_hourly) set of the current visitor date when the
// target covers or lies inside that directory.
func (s *Store) discardDeletedLocked(t deleteTarget, roots []string) {
	s.discardAggregationLocked(t.date, "", roots) // mode "": tag tables by path only
	v := s.visitor
	if v == nil || v.date != t.date {
		return
	}
	touches := func(dir string) bool {
		p := filepath.Join(s.config.Directory, t.date, dir)
		return under(p, roots) || under(t.path, []string{p})
	}
	if touches("visit") {
		v.daily = map[string]*visitorEntry{}
	}
	if touches("visit_hourly") {
		v.hourly = map[string]*visitorEntry{}
	}
}

// releaseTTLLocked closes cached kv2 TTL indexes whose files lie below roots
// (their .kvdata handles are in s.files and leave with releaseDoomedLocked).
func (s *Store) releaseTTLLocked(roots []string, closes *doomedCloses) error {
	var errs []error
	for p, idx := range s.ttlIndexes {
		if under(p, roots) || under(p+".k2file", roots) || under(p+".hfile", roots) {
			errs = append(errs, closes.take(&s.closes, &doomedHandle{files: []string{p + ".k2file", p + ".hfile"}, close: idx.Close,
				restore: func() { s.ttlIndexes[p] = idx }}))
			delete(s.ttlIndexes, p)
		}
	}
	return errors.Join(errs...)
}

// releaseUnderLocked closes counter tables whose files lie below roots.
func (c *Counters) releaseUnderLocked(roots []string, closes *doomedCloses) error {
	base, err := filepath.Abs(c.root)
	if err != nil {
		return err
	}
	var errs []error
	for key, t := range c.tables {
		date, prefix, _ := strings.Cut(key, "/")
		p := filepath.Join(base, date, "counter", prefix)
		if !under(p+".data", roots) && !under(p+".kfile", roots) && !under(p+".hfile", roots) && !under(p+".head", roots) {
			continue
		}
		errs = append(errs, closes.take(&c.closes, c.doomedTable(key, t)))
		delete(c.tables, key)
	}
	return errors.Join(errs...)
}
