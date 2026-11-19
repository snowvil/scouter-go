// KeyValueStoreRW: Copyright 2015 Scouter authors, Apache-2.0.
// Independent implementation from dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5.
package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

const maxKVTime = int64(1<<39 - 1)

// Namespace is a single UTF-8 filename component. Existing symlinks and
// nonregular files are rejected, unlike Java's unchecked path concatenation.
func (s *Store) keyValuePath(space string) (string, error) {
	if !utf8.ValidString(space) || strings.ContainsAny(space, "/\\\x00") || space == "." || space == ".." {
		return "", ErrInvalid
	}
	if len(space) > 245 {
		return "", ErrLimit
	}
	dir := filepath.Join(s.config.Directory, "00000000", "kv2")
	for _, path := range []string{s.config.Directory, filepath.Dir(dir), dir} {
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", ErrInvalid
		}
	}
	path := filepath.Join(dir, "kv_"+space)
	for _, suffix := range []string{".k2file", ".hfile", ".kvdata"} {
		info, err := os.Lstat(path + suffix)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		if !info.Mode().IsRegular() {
			return "", ErrInvalid
		}
	}
	return path, nil
}
func (s *Store) keyValueIndex(ctx context.Context, path string, create bool) (*legacyindex.TTLIndex, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if idx := s.ttlIndexes[path]; idx != nil {
		return idx, nil
	}
	if s.closes.adopt(path) {
		if len(s.ttlIndexes) > 16 {
			if err := s.evictTTLIndexLocked(path); err != nil {
				return nil, err
			}
		}
		return s.ttlIndexes[path], nil
	}
	if !create {
		_, keyErr := os.Stat(path + ".k2file")
		_, hashErr := os.Stat(path + ".hfile")
		if os.IsNotExist(keyErr) && os.IsNotExist(hashErr) {
			return nil, nil
		}
		if keyErr != nil || hashErr != nil {
			return nil, errors.Join(ErrCorrupt, keyErr, hashErr)
		}
	}
	if len(s.ttlIndexes) >= 16 {
		if err := s.evictTTLIndexLocked(""); err != nil {
			return nil, err
		}
	}
	if create {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, err
		}
	}
	idx, err := legacyindex.OpenTTLCheckpointed(ctx, path, s.kvHashSize(), s.clean.takeSeal(path), s.checkpoints.get(path))
	if err != nil {
		s.opens.failed()
		return nil, err
	}
	s.countOpen(path, idx.OpenInfo())
	idx.SetDeepSearch(s.live.warn.hook(javaKVKeyText))
	s.reportIndexRecovery(path, idx.Recovered())
	if s.ttlIndexes == nil {
		s.ttlIndexes = make(map[string]*legacyindex.TTLIndex)
	}
	s.ttlIndexes[path] = idx
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return idx, nil
}

// evictTTLIndexLocked closes the first TTL index in path order other than keep
// (see Store.evictIndexLocked for the data-before-header rule and the deferred
// close during a sync pass).
func (s *Store) evictTTLIndexLocked(keep string) error {
	keys := make([]string, 0, len(s.ttlIndexes))
	for key := range s.ttlIndexes {
		if key != keep {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	victim := keys[0]
	idx := s.ttlIndexes[victim]
	files := s.filesUnderLocked(filepath.Dir(victim))
	if !s.closes.syncing {
		for _, f := range files {
			if err := s.closes.syncFile(f); err != nil {
				return err
			}
		}
		files = nil
	}
	delete(s.ttlIndexes, victim)
	dataOK := false
	return s.closes.release(&pendingClose{
		key:   victim,
		adopt: func() { s.ttlIndexes[victim] = idx },
		close: func() error {
			var errs []error
			for _, f := range files {
				errs = append(errs, s.closes.syncClosedOK(f))
			}
			dataErr := errors.Join(errs...)
			dataOK = dataErr == nil
			return errors.Join(dataErr, idx.Sync(), idx.Close())
		},
		after: func(forgotten bool) {
			if dataOK && !forgotten {
				s.recordEvictedLocked(victim, idx.Checkpoint)
			}
		},
	})
}
func keyValueArgs(space, key string, now int64) error {
	if now < 0 || now > maxKVTime {
		return ErrInvalid
	}
	if !utf8.ValidString(key) || !utf8.ValidString(space) {
		return ErrInvalid
	}
	// RealKeyFile2.getDataPos uses signed readShort, so larger keys cannot be
	// reliably read by Java even though DataOutputX.writeShortBytes accepts them.
	if len(key) > 32767 {
		return ErrLimit
	}
	return nil
}
func keyValueExpiry(now, ttl int64) (int64, error) {
	if now < 0 || now > maxKVTime {
		return 0, ErrInvalid
	}
	if ttl < 0 {
		return -1, nil
	}
	if ttl > maxKVTime-now {
		return 0, ErrLimit
	}
	return now + ttl, nil
}

// KeyValueContext returns missing for empty space/key, expired or deleted
// latest records. Equality at the expiry second remains live, as in Java.
func (s *Store) KeyValueContext(ctx context.Context, space, key string, now int64) (string, bool, error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return "", false, err
	}
	defer s.mu.Unlock()
	if s.closed {
		return "", false, ErrClosed
	}
	if err := keyValueArgs(space, key, now); err != nil {
		return "", false, err
	}
	if space == "" || key == "" {
		return "", false, nil
	}
	path, err := s.keyValuePath(space)
	if err != nil {
		return "", false, err
	}
	idx, err := s.keyValueIndex(ctx, path, false)
	if err != nil || idx == nil {
		return "", false, err
	}
	offset, err := idx.GetContext(ctx, []byte(key), now)
	if err != nil || offset == nil {
		return "", false, err
	}
	pos, err := dataOffset(offset)
	if err != nil {
		return "", false, err
	}
	_, maxBytes := s.extraQueryBudget()
	data, err := s.extraReadData(ctx, path+".kvdata", pos, 4, maxBytes)
	if err != nil {
		return "", false, err
	}
	if !utf8.Valid(data) {
		return "", false, ErrCorrupt
	}
	return string(data), true, ctx.Err()
}

// PutKeyValueContext writes UTF-8 plus signed int32 length to .kvdata, then
// publishes its signed long5 offset in RealKeyFile2. ttl is seconds; any
// negative ttl is permanent. Empty space/key/value returns false, as in Java.
func (s *Store) PutKeyValueContext(ctx context.Context, space, key, value string, ttl, now int64) (bool, error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return false, err
	}
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrClosed
	}
	if err := keyValueArgs(space, key, now); err != nil {
		return false, err
	}
	if !utf8.ValidString(value) {
		return false, ErrInvalid
	}
	if space == "" || key == "" || value == "" {
		return false, nil
	}
	if len(value) > s.config.MaxRecordBytes {
		return false, ErrLimit
	}
	expires, err := keyValueExpiry(now, ttl)
	if err != nil {
		return false, err
	}
	path, err := s.keyValuePath(space)
	if err != nil {
		return false, err
	}
	idx, err := s.keyValueIndex(ctx, path, true)
	if err != nil {
		return false, err
	}
	pos, err := s.appendData(path+".kvdata", []byte(value), 4)
	if err != nil {
		return false, err
	}
	if _, err = idx.UpdateOrPutContext(ctx, []byte(key), offsetBytes(pos), expires); err != nil {
		return false, err
	}
	return true, ctx.Err()
}

// SetKeyValueTTLContext mirrors KeyValueStoreRW.setTTL: a valid/open namespace
// returns true even when its IndexKeyFile2 found no live key to update.
func (s *Store) SetKeyValueTTLContext(ctx context.Context, space, key string, ttl, now int64) (bool, error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return false, err
	}
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrClosed
	}
	if err := keyValueArgs(space, key, now); err != nil {
		return false, err
	}
	if space == "" || key == "" {
		return false, nil
	}
	expires, err := keyValueExpiry(now, ttl)
	if err != nil {
		return false, err
	}
	path, err := s.keyValuePath(space)
	if err != nil {
		return false, err
	}
	idx, err := s.keyValueIndex(ctx, path, true)
	if err != nil {
		return false, err
	}
	if _, err = s.file(path+".kvdata", true); err != nil {
		return false, err
	}
	_, err = idx.SetTTLContext(ctx, []byte(key), now, expires)
	return err == nil, err
}
func (s *Store) DeleteKeyValueContext(ctx context.Context, space, key string) (int, error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return 0, err
	}
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	if err := keyValueArgs(space, key, 0); err != nil {
		return 0, err
	}
	if space == "" || key == "" {
		return -1, nil
	}
	path, err := s.keyValuePath(space)
	if err != nil {
		return 0, err
	}
	idx, err := s.keyValueIndex(ctx, path, true)
	if err != nil {
		return 0, err
	}
	if _, err = s.file(path+".kvdata", true); err != nil {
		return 0, err
	}
	deleted, err := idx.DeleteContext(ctx, []byte(key))
	if err != nil {
		return 0, err
	}
	if deleted {
		return 1, nil
	}
	return 0, nil
}
