// Copyright 2015 Scouter authors, Apache-2.0.
// ObjectWR.remove / ObjectIndex.delete at pinned source
// dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5. See storage-contract.md
// "Object 영구 삭제".
package storage

import (
	"context"
	"time"
)

// maxRemoveObjects bounds one RemoveObjectsOn call. Java has no bound beyond
// its queue; AgentManager.removeAgents passes the client's objHash list.
const maxRemoveObjects = 10000

// RemoveObject is Java ObjectWR.remove(objHash) as the writer thread was meant
// to apply it: the record is marked deleted in the object index of the
// current date (ObjectWR's curDbw is always DateUtil.yyyymmdd()), never in
// earlier dates. Deleting is the Java IndexKeyFile delete flag; obj.data keeps
// its 4096-byte slot and the next PutObject of the same hash appends a fresh
// slot, as Java does after its index.get returns -1. A hash that is not live
// is a silent no-op, as in Java.
//
// Intended differences, see storage-contract.md "Object 영구 삭제":
//   - at the pinned source the Java writer never reaches the delete branch:
//     ObjectWR.Order.add defaults to true and createRm leaves it, so the
//     queued order first runs the add branch with pack == null, throws a
//     NullPointerException that the loop swallows, and the index is never
//     touched. Go applies the delete the API and the IndexKeyFile format
//     describe; a Java ObjectRD then reads the same list Go returns.
//   - the call is synchronous instead of a bounded queue that drops (S132)
//     when full, so the caller sees errors and the removal is visible to the
//     next Objects call.
func (s *Store) RemoveObject(ctx context.Context, hash int32) error {
	_, err := s.RemoveObjectsOn(ctx, s.today(), []int32{hash})
	return err
}

// RemoveObjectsOn removes hashes from the object index of date and returns
// how many were live before the call. Intended difference: a date without an
// object DB stays without one (Java's open(date) would create empty
// obj.kfile/obj.hfile first); a Java reader sees the same empty list either
// way. The flag write reaches disk with the next SyncData/Close like every
// other index write (no per-call fsync, Java does none either).
func (s *Store) RemoveObjectsOn(ctx context.Context, date string, hashes []int32) (int, error) {
	if len(hashes) > maxRemoveObjects {
		return 0, ErrLimit
	}
	if err := s.mu.LockContext(ctx); err != nil {
		return 0, err
	}
	defer s.mu.Unlock()
	if err := s.check(date); err != nil {
		return 0, err
	}
	if len(hashes) == 0 {
		return 0, nil
	}
	idx, err := s.index(s.path(date, "object", "obj"), false, false)
	if err != nil || idx == nil {
		return 0, err
	}
	removed := 0
	for _, hash := range hashes {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		n, err := idx.Delete(intKey(hash))
		if err != nil {
			return removed, err
		}
		if n > 0 {
			removed++
		}
	}
	return removed, nil
}

// today is DateUtil.yyyymmdd() in the Store's Java timezone.
func (s *Store) today() string {
	return JavaDate(time.Now().UnixMilli(), s.config.Location)
}
