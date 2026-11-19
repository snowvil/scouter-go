// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
package migrate

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const maxIndexValueBytes = 16 << 20 // legacyindex.MaxValueBytes

// keyRecord is one physical RealKeyFile/RealKeyFile2 record.
type keyRecord struct {
	pos        int64
	deleted    bool
	expires    int64 // RealKeyFile2 only
	key, value []byte
}

// scanKeyRecords parses records in [2, end) of a key file whose read-only
// legacyindex open fails only because of a torn tail after end (already
// proven by AnalyzeRecovery). It reads with O_RDONLY and streams the file.
// Layout: deleted(1) [expire Long5 for .k2file] prev Long5, ushort key
// length, key, Blob value (1-byte length, 0xFF+uint16 or 0xFE+int32).
func scanKeyRecords(abs string, ttl bool, end int64, visit func(keyRecord) error) error {
	f, err := os.OpenFile(abs, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(io.NewSectionReader(f, 0, end), 1<<20)
	var magic [2]byte
	if _, err = io.ReadFull(r, magic[:]); err != nil || magic != [2]byte{0xca, 0xfe} {
		return fmt.Errorf("key file magic: %v", err)
	}
	fixed := 8
	if ttl {
		fixed = 13
	}
	head := make([]byte, fixed)
	for pos := int64(2); pos < end; {
		if _, err = io.ReadFull(r, head); err != nil {
			return fmt.Errorf("record at %d: %w", pos, err)
		}
		rec := keyRecord{pos: pos, deleted: head[0] != 0}
		if ttl {
			rec.expires = readLong5(head[1:6])
		}
		rec.key = make([]byte, binary.BigEndian.Uint16(head[fixed-2:]))
		if _, err = io.ReadFull(r, rec.key); err != nil {
			return fmt.Errorf("record at %d key: %w", pos, err)
		}
		width := 1
		b, err := r.ReadByte()
		if err != nil {
			return fmt.Errorf("record at %d blob: %w", pos, err)
		}
		n := int64(b)
		if b == 0xff || b == 0xfe {
			ext := make([]byte, 2)
			if b == 0xfe {
				ext = make([]byte, 4)
			}
			if _, err = io.ReadFull(r, ext); err != nil {
				return fmt.Errorf("record at %d blob length: %w", pos, err)
			}
			width += len(ext)
			if b == 0xff {
				n = int64(binary.BigEndian.Uint16(ext))
			} else {
				n = int64(int32(binary.BigEndian.Uint32(ext)))
			}
		}
		if n < 0 || n > maxIndexValueBytes {
			return fmt.Errorf("record at %d blob length %d", pos, n)
		}
		rec.value = make([]byte, n)
		if _, err = io.ReadFull(r, rec.value); err != nil {
			return fmt.Errorf("record at %d value: %w", pos, err)
		}
		if err = visit(rec); err != nil {
			return err
		}
		pos += int64(fixed) + int64(len(rec.key)) + int64(width) + n
	}
	return nil
}

// checkTornRecords runs the normal per-record key/date/data checks for an
// auto-recoverable torn index over its complete records before the cut.
func (in *inspector) checkTornRecords(base, rel string, ttl bool, end int64) {
	spec := specFor(base, ttl)
	var records, deleted, live, expired int64
	stop := errors.New("cancelled")
	err := scanKeyRecords(in.abs(rel), ttl, end, func(r keyRecord) error {
		if records%4096 == 0 && in.ctx.Err() != nil {
			return stop
		}
		records++
		switch {
		case r.deleted:
			deleted++
		case ttl && r.expires < in.opt.Now:
			expired++
		default:
			live++
		}
		if ttl || !r.deleted {
			in.checkRecord(spec, rel, r.key, r.value, r.pos)
		}
		return nil
	})
	if errors.Is(err, stop) {
		return
	}
	if err != nil {
		in.c.add(SeverityError, "index.record", rel, -1, "complete records before the torn tail: %v", err)
		return
	}
	in.setRecords(rel, records)
	if ttl {
		in.count(spec.date, "ttl_live", live)
		in.count(spec.date, "ttl_expired", expired)
		in.count(spec.date, "ttl_deleted", deleted)
	} else {
		in.count(spec.date, spec.countKey, records-deleted)
	}
}
