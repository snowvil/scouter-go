// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// Writer lock files at the DB root. They are never copied or compared.
const (
	javaLockName = "lock.dat"        // Java DBCtr heartbeat, stale after 5 s
	goLockName   = ".go-writer.lock" // Go runtime flock owner
	javaLockLive = 5 * time.Second
	// javaStoppedAge: Java's exit path stops the 1 s lock.dat heartbeat and
	// then flushes GZipStore blocks and closes indexes and KV synchronously
	// (Main.java:88-95, ShutdownManager); a normal exit deletes lock.dat
	// (DBCtr.scala:56 deleteOnExit). A lock.dat only 5 s old can therefore
	// belong to a JVM that is still writing. Writers that copy (migrate,
	// finish) require it to be absent or this old, unless the operator
	// confirmed the process exited.
	javaStoppedAge = 60 * time.Second
)

// migrateLockSuffix names the sibling flock file that serializes every
// migrate, precopy, finish and rollback of one destination.
const migrateLockSuffix = ".migrate.lock"

// lockDestination takes an exclusive, non-blocking flock on
// <dst>.migrate.lock for the whole operation. The file is left in place
// (unlinking a lock file races with a waiting locker).
func lockDestination(dst string) (func(), error) {
	path := dst + migrateLockSuffix
	if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("another scouter-db migrate/precopy/finish/rollback holds %s", path)
		}
		return nil, err
	}
	return func() { syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
}

func isLockFile(rel string) bool { return rel == javaLockName || rel == goLockName }

// goCleanName is the Go Store clean-shutdown marker and goCheckpointNames
// are the Go crash checkpoint logs of the Store and the Counters (all in
// storage/clean.go). They certify index files of that DB only, so they are
// never copied or compared: a copy must not inherit the decision to skip
// index validation.
const goCleanName = ".go-clean"

var goCheckpointNames = []string{".go-checkpoint", ".go-checkpoint-counter"}

func isRuntimeState(rel string) bool {
	return rel == goCleanName || rel == goCheckpointNames[0] || rel == goCheckpointNames[1]
}

// purgeTrashDir is where Go retention renames purge targets before deleting
// them outside the storage gates; a crash can leave entries behind.
const purgeTrashDir = ".purge-trash"

// purgeQuarantinePrefix names a purge or DB delete target that retention
// renamed next to itself because the trash was unusable (storage
// quarantinePrefix); it can appear in the root or inside a date directory and
// is unlinked later. Like the trash, it is never DB content.
const purgeQuarantinePrefix = ".purge-quarantine-"

// isPurgeTrash reports the trash directory, quarantined targets anywhere in
// the tree, and everything below them.
func isPurgeTrash(rel string) bool {
	if rel == purgeTrashDir || strings.HasPrefix(rel, purgeTrashDir+"/") {
		return true
	}
	for _, part := range strings.Split(rel, "/") {
		if strings.HasPrefix(part, purgeQuarantinePrefix) {
			return true
		}
	}
	return false
}

// tempPrefixes are os.CreateTemp patterns of Go storage atomic writes
// (xlog/visitor ".scouter-*", counter head, batch archive).
var tempPrefixes = []string{".scouter-", ".counter-head-", ".batch-archive-"}

func isTempLeftover(rel string) bool {
	base := path.Base(rel)
	for _, p := range tempPrefixes {
		if strings.HasPrefix(base, p) {
			return true
		}
	}
	return false
}

// isExcluded reports paths that are never copied, compared or checksummed.
func isExcluded(rel string) bool {
	return isLockFile(rel) || isRuntimeState(rel) || isPurgeTrash(rel) || isTempLeftover(rel)
}

// LockState is observed without creating, modifying or touching any file.
type LockState struct {
	JavaLockExists bool   `json:"java_lock_exists"`
	JavaLockMTime  string `json:"java_lock_mtime,omitempty"`
	JavaActive     bool   `json:"java_active"`
	GoLockExists   bool   `json:"go_lock_exists"`
	GoWriterActive bool   `json:"go_writer_active"`
	javaLockAge    time.Duration
}

// checkJavaStopped refuses a copy that must see Java's final bytes while
// lock.dat is younger than javaStoppedAge, unless confirmed is set.
func (l LockState) checkJavaStopped(confirmed bool) error {
	if !l.JavaLockExists || confirmed || l.javaLockAge >= javaStoppedAge {
		return nil
	}
	return fmt.Errorf("lock.dat is still present and was modified %s ago; a Java server deletes it on a normal exit and may still flush gzip blocks, indexes and KV after its heartbeat stops. Confirm the Java process has exited (its PID is gone), then rerun with --java-stopped or wait until lock.dat is %s old", l.javaLockAge.Round(time.Second), javaStoppedAge)
}

// Live reports a writer that makes the DB contents unstable.
func (l LockState) Live() bool { return l.JavaActive || l.GoWriterActive }

// checkLocks follows Java DBCtr.createLock's 5 s stale rule (a future mtime is
// treated as live) and probes the Go runtime flock with a shared, non-blocking
// lock on an O_RDONLY descriptor that is released immediately.
func checkLocks(root string) (LockState, error) {
	var st LockState
	info, err := os.Lstat(filepath.Join(root, javaLockName))
	if err == nil {
		st.JavaLockExists = true
		st.JavaLockMTime = info.ModTime().UTC().Format(time.RFC3339Nano)
		st.javaLockAge = time.Since(info.ModTime())
		st.JavaActive = st.javaLockAge < javaLockLive
	} else if !errors.Is(err, fs.ErrNotExist) {
		return st, err
	}
	path := filepath.Join(root, goLockName)
	info, err = os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	st.GoLockExists = true
	if !info.Mode().IsRegular() {
		return st, fmt.Errorf("%s is not a regular file", path)
	}
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return st, err
	}
	defer f.Close()
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			st.GoWriterActive = true
			return st, nil
		}
		return st, err
	}
	return st, syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

type treeEntry struct {
	rel  string // slash separated
	info fs.FileInfo
}
type tree struct {
	dirs, files, symlinks, others []treeEntry
}

// testHookWalk observes each walked entry before it is classified.
var testHookWalk func(root, rel string)

// walkTree never follows symbolic links. Entries are sorted by path.
func walkTree(ctx context.Context, root string) (*tree, error) {
	return walkTreeVanishing(ctx, root, false)
}

// walkTreeVanishing optionally skips entries deleted during the walk (Java
// retention removing a date while a precopy runs) instead of failing.
func walkTreeVanishing(ctx context.Context, root string, skipVanished bool) (*tree, error) {
	info, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("DB root %s must be a real directory (symlinks are not followed)", root)
	}
	t := &tree{}
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if skipVanished && path != root && errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if testHookWalk != nil {
			testHookWalk(root, filepath.ToSlash(rel))
		}
		info, err := d.Info()
		if err != nil {
			if skipVanished && errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		e := treeEntry{rel: filepath.ToSlash(rel), info: info}
		switch {
		case info.Mode()&fs.ModeSymlink != 0:
			t.symlinks = append(t.symlinks, e)
		case info.IsDir():
			t.dirs = append(t.dirs, e)
		case info.Mode().IsRegular():
			t.files = append(t.files, e)
		default:
			t.others = append(t.others, e)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, list := range [][]treeEntry{t.dirs, t.files, t.symlinks, t.others} {
		sort.Slice(list, func(i, j int) bool { return list[i].rel < list[j].rel })
	}
	return t, nil
}

// cancelReader checks cancellation between reads.
type cancelReader struct {
	ctx context.Context
	r   io.Reader
}

func (c cancelReader) Read(b []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(b)
}

// hashFile streams one O_RDONLY file through SHA-256.
func hashFile(ctx context.Context, path string) (string, int64, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, cancelReader{ctx, f})
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// syncDir makes a create/rename durable in its parent directory.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

// writeJSONAtomic replaces path through a synced temporary file in the same directory.
func writeJSONAtomic(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// freeBytes is the space available to an unprivileged writer.
func freeBytes(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}

// realPath resolves symlinks of the longest existing ancestor so nested checks
// cannot be bypassed through aliases. The returned path is absolute and clean.
func realPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	rest := ""
	for cur := abs; ; {
		resolved, err := filepath.EvalSymlinks(cur)
		if err == nil {
			return filepath.Join(resolved, rest), nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return abs, nil
		}
		rest = filepath.Join(filepath.Base(cur), rest)
		cur = parent
	}
}

// within reports whether path equals root or is below it.
func within(root, path string) bool {
	if root == path {
		return true
	}
	return strings.HasPrefix(path, strings.TrimSuffix(root, string(filepath.Separator))+string(filepath.Separator))
}
func overlaps(a, b string) bool { return within(a, b) || within(b, a) }

// CheckOutsideRoot rejects an output path (report or manifest) inside root.
func CheckOutsideRoot(root, output string) error {
	r, err := realPath(root)
	if err != nil {
		return err
	}
	o, err := realPath(output)
	if err != nil {
		return err
	}
	if within(r, o) {
		return fmt.Errorf("output %s must not be written inside DB %s", output, root)
	}
	return nil
}

func nowText() string { return time.Now().UTC().Format(time.RFC3339Nano) }
