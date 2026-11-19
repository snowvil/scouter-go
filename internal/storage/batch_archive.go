// BatchZipDB: Copyright 2015 Scouter authors, Apache-2.0.
// Independent implementation from source dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5.
package storage

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"
)

const batchArchiveChunkBytes = 120000

// batchArchiveConcurrent bounds the batch archive uploads and the reads in
// progress at once: each may hold a temporary file or the chunks of up to
// MaxQueryBytes, and without the Store gate nothing else serializes them.
// A caller past the bound waits for a slot until its ctx ends.
const batchArchiveConcurrent = 4

// acquireArchiveSlot takes a slot of slots (nil: unbounded) and returns its
// release.
func acquireArchiveSlot(ctx context.Context, slots chan struct{}) (func(), error) {
	if slots == nil {
		return func() {}, nil
	}
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type archiveUploadReader struct {
	reader     queryReader
	emptyReads int
}

func (r *archiveUploadReader) Read(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	if n < 0 || n > len(b) {
		return 0, ErrInvalid
	}
	if n == 0 && err == nil {
		r.emptyReads++
		if r.emptyReads >= 100 {
			return 0, io.ErrNoProgress
		}
	} else {
		r.emptyReads = 0
	}
	return n, err
}

// Unlike original BatchZipDB path concatenation, path/filename traversal and
// existing symlinks are rejected. Archive bytes are already compressed by the
// Agent; the .zip suffix does not request another compression layer.
func (s *Store) batchArchivePath(tm int64, objName, filename string) (string, error) {
	if tm <= 0 || filename == "" || filename == "." || filename == ".." || !utf8.ValidString(filename) || strings.ContainsAny(filename, "/\\\x00") {
		return "", ErrInvalid
	}
	date := JavaDate(tm, s.config.Location)
	hour := strconv.FormatInt(JavaMillis(tm, s.config.Location)/3600000, 10)
	dir, err := s.objectRecordPath(date, objName, hour)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(dir)
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	if err == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
		return "", ErrInvalid
	}
	path := filepath.Join(dir, filename)
	info, err = os.Lstat(path)
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	if err == nil && !info.Mode().IsRegular() {
		return "", ErrInvalid
	}
	return path, nil
}

// PutBatchArchiveContext preserves BatchZipDB.write's filename+'.zip' and raw
// bytes, but consumes exactly size bytes. Short uploads and failures/cancellation
// before rename leave the old archive intact. A post-rename directory sync error
// or cancellation can report an error after publication. Original short/size
// overshoot behavior is rejected for malformed input. Published files are synced
// and closed here; a blocking caller Reader still needs its own IO deadline.
//
// The Store gate is held only to validate the target and create the temporary
// file, and again to re-validate the target, drop a cached handle and rename.
// Reading the body (up to MaxQueryBytes from a possibly slow Agent) and the
// file and directory fsyncs run without the gate, so an upload never stalls
// PutXLog. A purge or DB delete of the hour directory meanwhile moves the
// temporary file away and the rename fails: nothing is published. Concurrent
// uploads of one key each write their own temporary file; the last rename
// wins and readers see one complete archive or the other. At most
// batchArchiveConcurrent uploads run at once.
func (s *Store) PutBatchArchiveContext(ctx context.Context, tm int64, objName, filename string, size int64, r io.Reader) error {
	release, err := acquireArchiveSlot(ctx, s.archiveUploads)
	if err != nil {
		return err
	}
	defer release()
	temp, err := s.createBatchArchiveTemp(ctx, tm, objName, filename, size, r)
	if err != nil {
		return err
	}
	defer func() { _ = temp.Close(); _ = os.Remove(temp.Name()) }()
	n, err := io.CopyBuffer(temp, io.LimitReader(&archiveUploadReader{reader: queryReader{ctx, r}}, size), make([]byte, 32768))
	if err != nil {
		return err
	}
	if n != size {
		return io.ErrUnexpectedEOF
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = errors.Join(temp.Sync(), temp.Close()); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	path, err := s.publishBatchArchive(ctx, tm, objName, filename, temp.Name())
	if err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	if err = errors.Join(dir.Sync(), dir.Close()); err != nil {
		return err
	}
	return ctx.Err()
}

// createBatchArchiveTemp validates an upload and creates its temporary file
// in the target directory under the Store gate.
func (s *Store) createBatchArchiveTemp(ctx context.Context, tm int64, objName, filename string, size int64, r io.Reader) (*os.File, error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if size < 0 || r == nil || filename == "" || filename == "." || filename == ".." {
		return nil, ErrInvalid
	}
	maxRecords, maxBytes := s.extraQueryBudget()
	if size > int64(maxBytes) || (size+batchArchiveChunkBytes-1)/batchArchiveChunkBytes > int64(maxRecords) {
		return nil, ErrLimit
	}
	// Empty/dot names were rejected before suffixing; inspect only the actual
	// target, since an unrelated directory may share the unsuffixed name.
	path, err := s.batchArchivePath(tm, objName, filename+".zip")
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	return os.CreateTemp(filepath.Dir(path), ".batch-archive-*")
}

// publishBatchArchive renames a synced temporary file over the target under
// the Store gate, after validating the target again: it may have become a
// symlink or a directory, or the Store closed, while the body was read.
func (s *Store) publishBatchArchive(ctx context.Context, tm int64, objName, filename, temp string) (string, error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return "", err
	}
	defer s.mu.Unlock()
	if s.closed {
		return "", ErrClosed
	}
	path, err := s.batchArchivePath(tm, objName, filename+".zip")
	if err != nil {
		return "", err
	}
	if filepath.Dir(temp) != filepath.Dir(path) {
		return "", ErrInvalid
	}
	if err = ctx.Err(); err != nil {
		return "", err
	}
	// A cached reader must not keep observing the replaced inode. The old
	// handle is never re-adopted (key ""): a later open must see the new file.
	if old := s.files[path]; old != nil {
		delete(s.files, path)
		delete(s.ends, path)
		delete(s.lastUse, path)
		if err = s.closes.release(&pendingClose{close: func() error { return errors.Join(s.closes.syncFile(old), old.Close()) }}); err != nil {
			return "", err
		}
	}
	// A purge or DB delete since createBatchArchiveTemp moved temp away: the
	// rename fails and nothing is published.
	return path, os.Rename(temp, path)
}

// BatchArchiveContext returns owned payload chunks, without HasNEXT/Blob wire
// headers. The caller streams those headers. read uses filename unchanged.
// Original missing directory returns no chunks; an existing directory with a
// missing file returns an error. No archive decompression is performed.
//
// The target is validated and opened (a private read-only handle) under the
// Store gate; the chunks are read after the gate is released, so a large
// archive read never stalls ingest. The handle keeps reading the inode it
// opened: a concurrent upload or purge does not tear the result. At most
// batchArchiveConcurrent reads run at once (each collects the whole archive).
func (s *Store) BatchArchiveContext(ctx context.Context, tm int64, objName, filename string) ([][]byte, error) {
	release, err := acquireArchiveSlot(ctx, s.archiveReads)
	if err != nil {
		return nil, err
	}
	defer release()
	f, err := s.openBatchArchive(ctx, tm, objName, filename)
	if err != nil || f == nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	maxRecords, maxBytes := s.extraQueryBudget()
	if st.Size() > int64(maxBytes) || (st.Size()+batchArchiveChunkBytes-1)/batchArchiveChunkBytes > int64(maxRecords) {
		return nil, ErrLimit
	}
	var chunks [][]byte
	for pos := int64(0); pos < st.Size(); {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		n := min(int64(batchArchiveChunkBytes), st.Size()-pos)
		b := make([]byte, int(n))
		if _, err = io.ReadFull(queryReader{ctx, io.NewSectionReader(f, pos, n)}, b); err != nil {
			return nil, err
		}
		chunks = append(chunks, b)
		pos += n
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return chunks, nil
}

// openBatchArchive validates and opens the archive under the Store gate. A
// missing directory returns nil, nil (no chunks).
func (s *Store) openBatchArchive(ctx context.Context, tm int64, objName, filename string) (*os.File, error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	path, err := s.batchArchivePath(tm, objName, filename)
	if err != nil {
		return nil, err
	}
	if _, err = os.Stat(filepath.Dir(path)); err != nil {
		if cancelled := ctx.Err(); cancelled != nil {
			return nil, cancelled
		}
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	f, err := os.Open(path)
	if cancelled := ctx.Err(); cancelled != nil {
		if err == nil {
			_ = f.Close()
		}
		return nil, cancelled
	}
	if os.IsNotExist(err) {
		return nil, os.ErrNotExist
	}
	if err != nil {
		return nil, err
	}
	return f, nil
}
