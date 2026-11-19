// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0

package legacyindex

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

// KeyChain is one key's hash chain as it was when Index.KeyChain returned:
// the bucket head and the key file length then, read through a .kfile
// handle of its own. Appends (Put, PutBatch) write after that length and
// only link to records before them, so the records of the chain are never
// rewritten by later appends, and ScanContext visits exactly what
// ScanKeyContext would have visited at that moment (LookupContext: what
// Index.LookupContext would have returned and reported). It needs no index
// lock: the index can append, sync, be closed, or have its files renamed or
// removed meanwhile (the handle keeps the inode).
//
// Update and Delete rewrite a record in place; a walk that reaches such a
// record afterwards sees the new value or deleted flag. On an index marked
// SetAppendOnly they are refused, and the snapshot is exact. A KeyChain is
// owned by one goroutine; Close releases its handle.
type KeyChain struct {
	// view walks the chain: its own handle, the length and capacity at the
	// snapshot, no header. nil when the chain was empty.
	view   *Index
	key    []byte
	bucket int
	head   int64
	closed bool
	// deep is the index's DeepSearchFunc at the snapshot (LookupContext).
	deep DeepSearchFunc
}

// KeyChain snapshots the hash chain of key. It holds the index read lock
// only to read the head and open the key file, which must still be the
// index's own (the caller keeps the pair at its path meanwhile, as a Store
// gate does against purge renames). Time indexes have no key chains.
func (i *Index) KeyChain(key []byte) (*KeyChain, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if err := i.ready(); err != nil {
		return nil, err
	}
	if i.timeIndex {
		return nil, errors.New("key chain of a time index")
	}
	bucket, err := i.bucket(key)
	if err != nil {
		return nil, err
	}
	c := &KeyChain{key: bytes.Clone(key), bucket: bucket, head: i.head(bucket), deep: i.deepSearch}
	if c.head == 0 {
		return c, nil
	}
	f, err := os.Open(i.keys.Name())
	if err != nil {
		return nil, err
	}
	if err := sameOpenFile(i.keys, f); err != nil {
		f.Close()
		return nil, err
	}
	c.view = &Index{keys: f, length: i.length, viewBuckets: i.buckets(), readOnly: true}
	return c, nil
}

func sameOpenFile(a, b *os.File) error {
	sa, err := a.Stat()
	if err != nil {
		return err
	}
	sb, err := b.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(sa, sb) {
		return fmt.Errorf("%s: key file replaced while open", b.Name())
	}
	return nil
}

// Empty reports a chain that had no records (nothing to walk, no handle).
func (c *KeyChain) Empty() bool { return c.head == 0 }

// ScanContext is ScanKeyContext on the snapshot: the live records of the
// key, newest first, with the same chain checks and errors. Cancellation is
// checked at every live (not deleted) chain record, matching or not.
func (c *KeyChain) ScanContext(ctx context.Context, visit func(Record) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.closed {
		return ErrClosed
	}
	if visit == nil {
		return errors.New("nil visitor")
	}
	if c.view == nil {
		return nil
	}
	_, err := c.view.walkChainFrom(c.bucket, c.head, c.view.length, func(m metadata) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !bytes.Equal(m.Key, c.key) {
			return nil
		}
		r, err := c.view.record(m)
		if err != nil {
			return err
		}
		return visit(r)
	})
	return err
}

// LookupContext is Index.LookupContext on the snapshot (Java
// IndexKeyFile.get): the newest live record's value, or nil, with the same
// chain checks and errors. A miss, an empty chain included, reports the
// walked chain length (deleted records counted) to the index's
// DeepSearchFunc as DeepGet; here the observer runs without the index lock.
// Cancellation is checked at every live (not deleted) chain record.
func (c *KeyChain) LookupContext(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.closed {
		return nil, ErrClosed
	}
	looped := 0
	if c.view != nil {
		var out []byte
		n, err := c.view.walkChainFrom(c.bucket, c.head, c.view.length, func(m metadata) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !bytes.Equal(m.Key, c.key) {
				return nil
			}
			r, err := c.view.record(m)
			if err != nil {
				return err
			}
			out = r.Value
			return errFound
		})
		if errors.Is(err, errFound) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		looped = n
	}
	if c.deep != nil {
		c.deep(DeepGet, c.key, looped)
	}
	return nil, nil
}

// LookupWithin is LookupContext walking at most limit chain records
// (deleted ones counted; a negative limit is 0) under the index read lock,
// and, with a non-zero deadline, not past it (checked every 16 records). A
// longer walk ends with done false, nothing returned and nothing reported;
// the caller walks the chain from a KeyChain instead. Otherwise done is true and the value,
// error and DeepGet report are LookupContext's. It lets a caller that holds
// a lock of its own finish short lookups in place, for a bounded time,
// without opening a KeyChain handle.
func (i *Index) LookupWithin(ctx context.Context, key []byte, limit int, deadline time.Time) (value []byte, done bool, err error) {
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	for !i.mu.TryRLock() {
		select {
		case <-ctx.Done():
			return nil, true, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	defer i.mu.RUnlock()
	if err := i.ready(); err != nil {
		return nil, true, err
	}
	bucket, err := i.bucket(key)
	if err != nil {
		return nil, true, err
	}
	looped, err := i.walkChainLimit(bucket, i.head(bucket), i.length, max(limit, 0), deadline, func(m metadata) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !bytes.Equal(m.Key, key) {
			return nil
		}
		r, e := i.record(m)
		if e != nil {
			return e
		}
		value = r.Value
		return errFound
	})
	switch {
	case errors.Is(err, errFound):
		return value, true, nil
	case errors.Is(err, errChainLimit):
		return nil, false, nil
	case err != nil:
		return nil, true, err
	}
	if i.deepSearch != nil {
		i.deepSearch(DeepGet, key, looped)
	}
	return nil, true, nil
}

// Close releases the key file handle; it is idempotent. A closed chain's
// ScanContext is ErrClosed.
func (c *KeyChain) Close() error {
	if c == nil || c.closed {
		return nil
	}
	c.closed = true
	if c.view == nil {
		return nil
	}
	err := c.view.keys.Close()
	c.view = nil
	return err
}
