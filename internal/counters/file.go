package counters

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// ErrUnsafeTarget marks a refused file target: a name that is not a plain
// base name, or an existing symlink/non-regular file. Java FileUtil.saveText
// follows symlinks and accepts any path; refusing is an intended difference.
var ErrUnsafeTarget = errors.New("counters: unsafe file target")

// WriteFile replaces dir/name with data: a temporary file in the same
// directory is written, fsynced and renamed over the target, then the
// directory is fsynced. All access is anchored with os.Root. dir is created
// like Java's parent mkdirs. An existing regular file keeps its permission
// bits; a new file gets perm. Shared by the counters store and the service
// configuration/alert writers.
func WriteFile(dir, name string, data []byte, perm fs.FileMode) error {
	if !PlainName(name) {
		return fmt.Errorf("%w: %q", ErrUnsafeTarget, name)
	}
	if dir == "" {
		return fmt.Errorf("%w: empty directory", ErrUnsafeTarget)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()
	mode := perm
	info, err := root.Lstat(name)
	switch {
	case err == nil:
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%w: %q is not a regular file", ErrUnsafeTarget, name)
		}
		mode = info.Mode().Perm()
	case !errors.Is(err, fs.ErrNotExist):
		return err
	}
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return err
	}
	tmp := "." + name + ".tmp-" + hex.EncodeToString(suffix[:])
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, mode)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			root.Remove(tmp)
		}
	}()
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Chmod(mode); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := root.Rename(tmp, name); err != nil {
		return err
	}
	committed = true
	d, err := root.Open(".")
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// ReadFile reads dir/name without following symlinks. A missing directory or
// file reports found=false. Non-regular files are refused; files above
// maxBytes are ErrLimit.
func ReadFile(dir, name string, maxBytes int) ([]byte, bool, error) {
	if !PlainName(name) {
		return nil, false, fmt.Errorf("%w: %q", ErrUnsafeTarget, name)
	}
	root, err := os.OpenRoot(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer root.Close()
	if info, err := root.Lstat(name); err == nil && !info.Mode().IsRegular() {
		return nil, false, fmt.Errorf("%w: %q is not a regular file", ErrUnsafeTarget, name)
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, false, fmt.Errorf("%w: %q is a symlink", ErrUnsafeTarget, name)
		}
		return nil, false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	if !info.Mode().IsRegular() {
		return nil, false, fmt.Errorf("%w: %q is not a regular file", ErrUnsafeTarget, name)
	}
	if info.Size() > int64(maxBytes) {
		return nil, false, ErrLimit
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(maxBytes)+1))
	if err != nil {
		return nil, false, err
	}
	if len(data) > maxBytes {
		return nil, false, ErrLimit
	}
	return data, true, nil
}

// PlainName reports whether name is a single path element.
func PlainName(name string) bool {
	return name != "" && name != "." && name != ".." && name == filepath.Base(name) && !strings.ContainsAny(name, "/\\\x00")
}
