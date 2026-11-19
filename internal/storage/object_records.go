// Copyright 2015 Scouter authors, Apache-2.0.
// StackAnalyzerDB/BatchDB and BinSearch.searchBE at pinned source
// dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5. See pack-storage.md.
package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/snowvil/scouter-go/internal/protocol"
)

const objectRecordIndexBytes = int64(13)

func validRecordObjectName(name string) error {
	if !utf8.ValidString(name) || !strings.HasPrefix(name, "/") || strings.ContainsAny(name, "\\\x00") {
		return ErrInvalid
	}
	parts := strings.Split(name[1:], "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return ErrInvalid
		}
	}
	return nil
}
func (s *Store) objectRecordPath(date, objName, kind string) (string, error) {
	if err := validDate(date); err != nil {
		return "", err
	}
	if err := validRecordObjectName(objName); err != nil {
		return "", err
	}
	// Reject existing symlink components beneath the configured DB root.
	path := s.config.Directory
	for _, component := range append([]string{date}, strings.Split(objName[1:], "/")...) {
		path = filepath.Join(path, component)
		info, err := os.Lstat(path)
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return "", ErrInvalid
		}
	}
	return filepath.Join(path, kind), nil
}
func batchCompletion(p *protocol.BatchPack) (int64, error) {
	if p == nil || p.StartTime <= 0 || p.ElapsedTime < 0 || p.StartTime > math.MaxInt64-p.ElapsedTime {
		return 0, ErrInvalid
	}
	return p.StartTime + p.ElapsedTime, nil
}
func (s *Store) PutStackContext(ctx context.Context, objName string, p *protocol.StackPack) error {
	if p == nil {
		return ErrInvalid
	}
	return s.putObjectRecord(ctx, objName, "stack", p.Time, p)
}
func (s *Store) PutBatchContext(ctx context.Context, objName string, p *protocol.BatchPack) error {
	tm, err := batchCompletion(p)
	if err != nil {
		return err
	}
	return s.putObjectRecord(ctx, objName, "batch", tm, p)
}
func (s *Store) putObjectRecord(ctx context.Context, objName, kind string, tm int64, p protocol.Pack) error {
	if err := s.mu.LockContext(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if tm <= 0 {
		return ErrInvalid
	}
	date := JavaDate(tm, s.config.Location)
	path, err := s.objectRecordPath(date, objName, kind)
	if err != nil {
		return err
	}
	b, err := protocol.EncodePack(p, s.config.CodecLimits)
	if err != nil {
		return err
	}
	if len(b) > s.config.MaxRecordBytes {
		return ErrLimit
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	idx, err := s.file(path+".idx", true)
	if err != nil {
		return err
	}
	st, err := idx.Stat()
	if err != nil {
		return err
	}
	if st.Size()%objectRecordIndexBytes != 0 {
		return ErrCorrupt
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	pos, err := s.appendData(path+".dat", b, 4)
	if err != nil {
		return err
	}
	// Opening the data file may evict the earlier index handle from the bounded cache.
	idx, err = s.file(path+".idx", true)
	if err != nil {
		return err
	}
	record := make([]byte, objectRecordIndexBytes)
	binary.BigEndian.PutUint64(record, uint64(tm))
	copy(record[8:], offsetBytes(pos))
	n, err := idx.WriteAt(record, st.Size())
	if err != nil {
		return err
	}
	if n != len(record) {
		return io.ErrShortWrite
	}
	return ctx.Err()
}
func readObjectIndex(ctx context.Context, f *os.File, entry int64) (int64, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	var b [13]byte
	if _, err := f.ReadAt(b[:], entry*objectRecordIndexBytes); err != nil {
		return 0, 0, errors.Join(ErrCorrupt, err)
	}
	tm := int64(binary.BigEndian.Uint64(b[:8]))
	pos, err := dataOffset(b[8:])
	if tm <= 0 {
		return 0, 0, ErrCorrupt
	}
	return tm, pos, err
}

// objectSearchBE reproduces Java's midpoint equality and signed Int32 comparator.
// It does not rewind earlier equal timestamps or repair unsorted legacy indexes.
func objectSearchBE(ctx context.Context, f *os.File, length, target int64) (int64, error) {
	low, high := int64(0), length-1
	for high >= low {
		mid := (low + high) / 2
		tm, _, err := readObjectIndex(ctx, f, mid)
		if err != nil {
			return 0, err
		}
		comparison := int32(target - tm)
		if comparison < 0 {
			high = mid - 1
		} else if comparison > 0 {
			low = mid + 1
		} else {
			return mid, nil
		}
	}
	if low < length {
		return low, nil
	}
	return -1, nil
}
func (s *Store) scanObjectRecords(ctx context.Context, objName, kind string, from, to int64) ([]protocol.Pack, error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	date := JavaDate(from, s.config.Location)
	if err := s.extraQueryRange(date, from, to); err != nil {
		return nil, err
	}
	path, err := s.objectRecordPath(date, objName, kind)
	if err != nil {
		return nil, err
	}
	idx, err := s.file(path+".idx", false)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if err != nil || idx == nil {
		return nil, err
	}
	st, err := idx.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size()%objectRecordIndexBytes != 0 {
		return nil, ErrCorrupt
	}
	count := st.Size() / objectRecordIndexBytes
	entry, err := objectSearchBE(ctx, idx, count, from)
	if err != nil {
		return nil, err
	}
	if entry < 0 {
		return nil, nil
	}
	maxRecords, maxBytes := s.extraQueryBudget()
	used := 0
	var out []protocol.Pack
	for ; entry < count; entry++ {
		// A data read may have evicted this index handle. Acquire its current owner.
		idx, err = s.file(path+".idx", false)
		if err != nil || idx == nil {
			if err == nil {
				err = ErrCorrupt
			}
			return nil, err
		}
		tm, pos, err := readObjectIndex(ctx, idx, entry)
		if err != nil {
			return nil, err
		}
		if tm > to {
			break
		}
		if len(out) >= maxRecords {
			return nil, ErrLimit
		}
		b, err := s.extraReadData(ctx, path+".dat", pos, 4, maxBytes-used)
		if err != nil {
			return nil, err
		}
		used += len(b)
		p, err := protocol.DecodePack(b, s.config.CodecLimits)
		if err != nil {
			return nil, errors.Join(ErrCorrupt, err)
		}
		switch v := p.(type) {
		case *protocol.StackPack:
			if kind != "stack" || v.Time != tm {
				return nil, ErrCorrupt
			}
		case *protocol.BatchPack:
			actual, err := batchCompletion(v)
			if err != nil || kind != "batch" || actual != tm {
				return nil, ErrCorrupt
			}
			v.Position = pos
			// Java BatchDB history readSimplePack returns metadata only.
			v.SQLStats, v.UniqueSQLs, v.Extra = nil, nil, nil
		default:
			return nil, ErrCorrupt
		}
		out = append(out, p)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
func (s *Store) StacksContext(ctx context.Context, objName string, from, to int64) ([]*protocol.StackPack, error) {
	packs, err := s.scanObjectRecords(ctx, objName, "stack", from, to)
	if err != nil {
		return nil, err
	}
	out := make([]*protocol.StackPack, len(packs))
	for n, p := range packs {
		out[n] = p.(*protocol.StackPack)
	}
	return out, nil
}
func (s *Store) BatchesContext(ctx context.Context, objName string, from, to int64) ([]*protocol.BatchPack, error) {
	packs, err := s.scanObjectRecords(ctx, objName, "batch", from, to)
	if err != nil {
		return nil, err
	}
	out := make([]*protocol.BatchPack, len(packs))
	for n, p := range packs {
		out[n] = p.(*protocol.BatchPack)
	}
	return out, nil
}
func (s *Store) BatchAtContext(ctx context.Context, objName string, tm, position int64) (*protocol.BatchPack, error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if tm <= 0 || position < 0 || position > 1<<39-1 {
		return nil, ErrInvalid
	}
	date := JavaDate(tm, s.config.Location)
	path, err := s.objectRecordPath(date, objName, "batch")
	if err != nil {
		return nil, err
	}
	idx, err := s.file(path+".idx", false)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if err != nil || idx == nil {
		return nil, err
	}
	st, err := idx.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size()%objectRecordIndexBytes != 0 {
		return nil, ErrCorrupt
	}
	_, maxBytes := s.extraQueryBudget()
	b, err := s.extraReadData(ctx, path+".dat", position, 4, maxBytes)
	if err != nil {
		return nil, err
	}
	p, err := protocol.DecodePack(b, s.config.CodecLimits)
	if err != nil {
		return nil, errors.Join(ErrCorrupt, err)
	}
	batch, ok := p.(*protocol.BatchPack)
	if !ok {
		return nil, ErrCorrupt
	}
	actual, err := batchCompletion(batch)
	if err != nil || JavaDate(actual, s.config.Location) != date {
		return nil, ErrCorrupt
	}
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	return batch, nil
}
