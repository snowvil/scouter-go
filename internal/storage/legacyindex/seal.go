// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
package legacyindex

import (
	"errors"
	"hash/crc32"
	"os"
	"time"
)

// Seal fingerprints a .kfile/.hfile pair at the moment a writable handle
// closed it cleanly (CloseSealed): both files fully written and fsynced, the
// header on disk equal to the in-memory one. OpenSealed accepts an existing
// pair without walking its records only while the pair still matches the
// Seal exactly. Any write after the Seal (by Go, Java or a tool) changes a
// size, an mtime or the header checksum; a crash after the Seal does too,
// because the crashed writer appended or rewrote something first.
//
// Limit: mtime has the file system's timestamp granularity (Linux uses the
// coarse kernel clock, a few ms). An in-place .kfile write that keeps the size
// and the header, made within that granularity after the Seal, would go
// unnoticed. The Store holds the DB writer lock until after its Close, so no
// other writer can start within that window.
type Seal struct {
	KeyBytes  int64  `json:"key_bytes"`
	KeyMTime  int64  `json:"key_mtime_ns"`
	HashBytes int64  `json:"hash_bytes"`
	HashMTime int64  `json:"hash_mtime_ns"`
	HeaderCRC uint32 `json:"header_crc32"`
}

// LatestMTime is the newer of the two file mtimes.
func (s Seal) LatestMTime() time.Time { return time.Unix(0, max(s.KeyMTime, s.HashMTime)) }

// Matches reports whether two stats still describe the sealed pair (sizes and
// mtimes only; OpenSealed also compares the header checksum).
func (s Seal) Matches(keys, heads os.FileInfo) bool {
	return keys.Size() == s.KeyBytes && keys.ModTime().UnixNano() == s.KeyMTime &&
		heads.Size() == s.HashBytes && heads.ModTime().UnixNano() == s.HashMTime
}

// OpenSealed is OpenChecked that skips the record walk (and therefore any
// recovery) when seal matches the pair: the files are exactly as a clean
// CloseSealed left them, so there is nothing to recover. A nil seal, a
// mismatch, or a failed O(1) check falls back to the full validation of
// OpenChecked. OpenCheck tells which path was taken.
func OpenSealed(path string, hashSizeBytes int, timeIndex bool, location *time.Location, check RelinkCheck, seal *Seal) (*Index, error) {
	return openIndex(path, hashSizeBytes, timeIndex, location, modeWrite, check, seal, nil)
}

// sealedOpen reports whether the opened descriptors match seal and pass the
// O(1) checks; it then sets length. Called after the header and magic checks.
func (i *Index) sealedOpen(seal *Seal) bool {
	if seal == nil {
		return false
	}
	ks, err := i.keys.Stat()
	if err != nil {
		return false
	}
	hs, err := i.heads.Stat()
	if err != nil || !seal.Matches(ks, hs) || hs.Size() != int64(len(i.header)) || i.headerCRC() != seal.HeaderCRC {
		return false
	}
	length := ks.Size()
	buckets := (len(i.header) - headerBytes) / 5
	for bucket := 0; bucket < buckets; bucket++ {
		if pos := i.head(bucket); pos != 0 && (pos < 2 || pos >= length) {
			return false
		}
	}
	i.length = length
	return true
}

// CloseSealed closes a writable handle like Close and, when every step
// succeeded, returns the Seal of the closed pair. The fingerprint is taken
// after the final fsyncs and before the descriptors close.
func (i *Index) CloseSealed() (Seal, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return Seal{}, ErrClosed
	}
	if i.readOnly {
		i.closed = true
		return Seal{}, errors.Join(ErrReadOnly, i.keys.Close(), i.heads.Close())
	}
	err := errors.Join(i.fault, i.sync())
	var seal Seal
	if err == nil {
		seal, err = i.seal()
	}
	err = errors.Join(err, i.keys.Close(), i.heads.Close())
	i.closed = true
	if err != nil {
		return Seal{}, err
	}
	return seal, nil
}

func (i *Index) seal() (Seal, error) {
	ks, err := i.keys.Stat()
	if err != nil {
		return Seal{}, err
	}
	hs, err := i.heads.Stat()
	if err != nil {
		return Seal{}, err
	}
	if ks.Size() != i.length || hs.Size() != int64(len(i.header)) {
		return Seal{}, corrupt(CodeSize, -1, "sealed sizes %d/%d differ from handle %d/%d", ks.Size(), hs.Size(), i.length, len(i.header))
	}
	return Seal{KeyBytes: ks.Size(), KeyMTime: ks.ModTime().UnixNano(), HashBytes: hs.Size(), HashMTime: hs.ModTime().UnixNano(), HeaderCRC: i.headerCRC()}, nil
}

func (i *Index) headerCRC() uint32 { return crc32.ChecksumIEEE(i.header) }
