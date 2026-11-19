// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
package legacyindex

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"time"
)

// Checkpoint describes a durable, validated state of a writable pair: the
// first KeyBytes of the key file were fsynced and the .hfile on disk held, as
// fsynced, a header that references exactly the records below KeyBytes (no
// record below KeyBytes is newer than its bucket head). A Sync, a
// SyncPoint.Commit or a Close establishes it after its fsyncs, in the order
// key file, header, checkpoint (see Index.Checkpoint, SyncPoint.Checkpoint).
//
// A crash after a checkpoint leaves the key file grown past KeyBytes (records
// appended later, possibly torn) while the header is still the checkpoint's
// as long as nothing rewrote it. OpenCheckpointed then validates and recovers
// only the records from KeyBytes on: the records below are a prefix the
// checkpoint already validated, and key files only grow (Java and Go writers
// append; their in-place writes - deleted flags, same-length values, TTL
// expiry, tombstones - never change a record boundary or link).
//
// HeaderSum is the CRC32 of the big-endian CRC32s of the header's 4 KiB pages
// (headerPage), so a writer that rewrites only dirty pages can keep it
// without hashing the whole header. TailCRC is the CRC32 of the last
// min(64, KeyBytes) key file bytes below KeyBytes; with the header it
// identifies the pair against a deleted and recreated file of the same path.
type Checkpoint struct {
	KeyBytes  int64  `json:"key_bytes"`
	HashBytes int64  `json:"hash_bytes"`
	HeaderSum uint32 `json:"header_sum"`
	TailCRC   uint32 `json:"tail_crc32"`
}

// headerPage is the unit of dirty tracking and partial header writes.
const headerPage = 4096

// checkpointTail is how many key bytes before KeyBytes TailCRC covers.
const checkpointTail = 64

// errTailFallback stops a checkpointed (tail) validation whose outcome the
// tail alone cannot decide exactly; the caller re-validates every record.
var errTailFallback = errors.New("tail validation needs the full walk")

// OpenInfo reports how a writable Open accepted an existing pair.
type OpenInfo struct {
	Check OpenCheck
	// WalkedRecords/WalkedBytes are the records and key bytes the open
	// validated one by one: all of them for CheckWalked, those after the
	// checkpoint for CheckTail, none for CheckSealed (a tail attempt that fell
	// back is included).
	WalkedRecords, WalkedBytes int64
	// CheckpointRejected says why a given checkpoint was not used, or why a
	// tail validation fell back to the full walk; empty otherwise.
	CheckpointRejected string
}

// OpenInfo returns how this handle's Open accepted the pair.
func (i *Index) OpenInfo() OpenInfo {
	i.mu.RLock()
	defer i.mu.RUnlock()
	info := i.openInfo
	info.Check = i.openCheck
	return info
}

// OpenInfo returns how this handle's Open accepted the pair.
func (t *TTLIndex) OpenInfo() OpenInfo { return t.index.OpenInfo() }

// OpenCheckpointed is OpenSealed that, when seal does not match, uses cp to
// validate only the records appended after the checkpoint (CheckTail). A nil,
// mismatching or unusable checkpoint, and a tail whose validation would fail
// or refuse a relink, take the full validation of OpenChecked instead, so the
// result (records, recovery, errors) is the one the full walk gives.
func OpenCheckpointed(path string, hashSizeBytes int, timeIndex bool, location *time.Location, check RelinkCheck, seal *Seal, cp *Checkpoint) (*Index, error) {
	return openIndex(path, hashSizeBytes, timeIndex, location, modeWrite, check, seal, cp)
}

// OpenTTLCheckpointed is OpenTTLSealed with a Checkpoint (see OpenCheckpointed).
func OpenTTLCheckpointed(ctx context.Context, path string, hashSizeBytes int, seal *Seal, cp *Checkpoint) (*TTLIndex, error) {
	return openTTLIndex(ctx, path, hashSizeBytes, modeWrite, seal, cp)
}

// Checkpoint returns the latest checkpoint this writable handle established
// (by Sync, SyncPoint.Commit or Close), also after Close. ok is false when
// none was established or a write or fsync error made the on-disk state
// unknown for the rest of this handle's life.
func (i *Index) Checkpoint() (cp Checkpoint, ok bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.cp, i.cpOK
}

// Checkpoint returns the latest checkpoint of a TTL pair; see Index.Checkpoint.
func (t *TTLIndex) Checkpoint() (Checkpoint, bool) { return t.index.Checkpoint() }

// initPages prepares dirty-page tracking and the page sums of the header as
// read from (or written to) disk. Writable handles only.
func (i *Index) initPages() {
	n := (len(i.header) + headerPage - 1) / headerPage
	i.dirtyBits = make([]uint64, (n+63)/64)
	i.pendingRefs = make([]uint32, n)
	i.sums = make([]uint32, n)
	for p := range i.sums {
		i.sums[p] = crc32.ChecksumIEEE(i.page(p))
	}
	i.sumsOK = true
}

func (i *Index) page(p int) []byte {
	a := p * headerPage
	return i.header[a:min(a+headerPage, len(i.header))]
}

// markHeader records that header bytes [off, off+n) changed in memory.
func (i *Index) markHeader(off, n int) {
	i.dirty = true
	for p := off / headerPage; p <= (off+n-1)/headerPage; p++ {
		i.dirtyBits[p>>6] |= 1 << (uint(p) & 63)
	}
}

// unsyncedPages lists, ascending, the pages whose disk bytes may differ from
// memory: dirty ones and those an uncommitted SyncPoint holds (its Commit may
// not write them, see SyncPoint.Commit). Caller holds mu.
func (i *Index) unsyncedPages() []int {
	var out []int
	for w, word := range i.dirtyBits {
		for b := 0; b < 64; b++ {
			p := w*64 + b
			if p >= len(i.pendingRefs) {
				break
			}
			if word&(1<<uint(b)) != 0 || i.pendingRefs[p] > 0 {
				out = append(out, p)
			}
		}
	}
	return out
}

func setPages(set []uint64, pages []int, on bool) {
	for _, p := range pages {
		if on {
			set[p>>6] |= 1 << (uint(p) & 63)
		} else {
			set[p>>6] &^= 1 << (uint(p) & 63)
		}
	}
}

// event runs the test hook of an I/O step; nil in production.
func (i *Index) event(op string) error {
	if i.hook != nil {
		return i.hook(op)
	}
	return nil
}

// syncKeys fsyncs the key file. A failure makes later checkpoints of this
// handle untrustworthy (the kernel may have dropped the unsynced pages).
func (i *Index) syncKeys() error {
	err := i.event("keys-sync")
	if err == nil {
		err = i.keys.Sync()
	}
	if err != nil {
		i.lost = true
	}
	return err
}

// writePages writes the given header pages, in ascending order, from src,
// which holds exactly those pages back to back (each headerPage bytes but the
// header's last, shorter page). Consecutive pages go out in one write. Caller
// holds headerIO.
func (i *Index) writePages(pages []int, src []byte) error {
	at := 0
	for n := 0; n < len(pages); {
		m := n + 1
		for m < len(pages) && pages[m] == pages[m-1]+1 {
			m++
		}
		off := pages[n] * headerPage
		end := min(pages[m-1]*headerPage+headerPage, len(i.header))
		if err := i.event("header-write"); err != nil {
			return err
		}
		if _, err := i.heads.WriteAt(src[at:at+end-off], int64(off)); err != nil {
			return err
		}
		at += end - off
		n = m
	}
	if err := i.event("header-sync"); err != nil {
		return err
	}
	return i.heads.Sync()
}

// copyPages copies the given pages of the in-memory header back to back.
func (i *Index) copyPages(pages []int) []byte {
	size := 0
	for _, p := range pages {
		size += len(i.page(p))
	}
	out := make([]byte, 0, size)
	for _, p := range pages {
		out = append(out, i.page(p)...)
	}
	return out
}

// headerSum folds the page sums into Checkpoint.HeaderSum.
func headerSum(sums []uint32) uint32 {
	var buf [1024]byte
	crc := uint32(0)
	for len(sums) > 0 {
		n := min(len(sums), len(buf)/4)
		for k := 0; k < n; k++ {
			binary.BigEndian.PutUint32(buf[k*4:], sums[k])
		}
		crc = crc32.Update(crc, crc32.IEEETable, buf[:n*4])
		sums = sums[n:]
	}
	return crc
}

func (i *Index) tailCRC(length int64) (uint32, error) {
	n := min(int64(checkpointTail), length)
	b := make([]byte, n)
	if _, err := i.keys.ReadAt(b, length-n); err != nil {
		return 0, err
	}
	return crc32.ChecksumIEEE(b), nil
}

// checkpointLocked computes the checkpoint of the on-disk state after a
// successful key fsync and header write: key bytes below length, header as
// the page sums describe it. Caller holds headerIO.
func (i *Index) checkpointLocked(length int64) (Checkpoint, bool) {
	if i.readOnly || i.lost || !i.sumsOK || i.sums == nil {
		return Checkpoint{}, false
	}
	tail, err := i.tailCRC(length)
	if err != nil {
		return Checkpoint{}, false
	}
	return Checkpoint{KeyBytes: length, HashBytes: int64(len(i.header)), HeaderSum: headerSum(i.sums), TailCRC: tail}, true
}

// publishCheckpoint keeps the newest checkpoint; gen orders them (taken
// under headerIO). Caller holds mu.
func (i *Index) publishCheckpoint(cp Checkpoint, gen uint64) {
	if gen > i.cpGen {
		i.cp, i.cpOK, i.cpGen = cp, true, gen
	}
}

// checkpointStart returns the position from which the records must still be
// validated, or a reason the checkpoint does not describe this pair. Called
// after the header and magic checks with i.length set to the key file size.
func (i *Index) checkpointStart(cp *Checkpoint) (int64, string) {
	switch {
	case cp.HashBytes != int64(len(i.header)):
		return 2, "hash size differs"
	case cp.KeyBytes < 2 || cp.KeyBytes > i.length:
		return 2, fmt.Sprintf("key file has %d bytes, checkpoint %d", i.length, cp.KeyBytes)
	case i.sums == nil || headerSum(i.sums) != cp.HeaderSum:
		return 2, "header changed"
	}
	tail, err := i.tailCRC(cp.KeyBytes)
	if err != nil {
		return 2, fmt.Sprintf("read key tail: %v", err)
	}
	if tail != cp.TailCRC {
		return 2, "key bytes before the checkpoint differ"
	}
	return cp.KeyBytes, ""
}

// validateFrom runs validation from a checkpoint when cp is usable and falls
// back to the full walk otherwise. run validates from the given position.
func (i *Index) validateFrom(cp *Checkpoint, run func(start int64) error) error {
	size := i.length
	if cp != nil {
		start, why := i.checkpointStart(cp)
		i.openInfo.CheckpointRejected = why
		if why == "" && start > 2 {
			err := run(start)
			if err == nil {
				i.openCheck = CheckTail
				return nil
			}
			if !errors.Is(err, errTailFallback) && !errors.Is(err, ErrCorrupt) {
				return err
			}
			// The full walk decides: same records, recovery and errors as
			// an open without a checkpoint.
			i.openInfo.CheckpointRejected = "tail validation: " + err.Error()
			i.recovery = RecoveryInfo{}
			i.length = size
		}
	}
	if err := run(2); err != nil {
		return err
	}
	i.openCheck = CheckWalked
	return nil
}
