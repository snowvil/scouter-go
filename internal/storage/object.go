package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

const objectSlotBytes = 4096

func (s *Store) PutObject(date string, p *protocol.ObjectPack) error {
	return s.PutObjectContext(context.Background(), date, p)
}

// PutObjectContext is PutObject that gives up waiting for the Store gate when
// ctx ends (the ingest object writer, whose context Close cancels past its
// deadline). Once it holds the gate the write is not interrupted.
func (s *Store) PutObjectContext(ctx context.Context, date string, p *protocol.ObjectPack) error {
	if err := s.mu.LockContext(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if err := s.check(date); err != nil {
		return err
	}
	if p == nil {
		return ErrInvalid
	}
	b, err := protocol.EncodePack(p, s.config.CodecLimits)
	if err != nil {
		return err
	}
	if len(b) > objectSlotBytes || objectSlotBytes > s.config.MaxRecordBytes {
		return ErrLimit
	}
	slot := make([]byte, objectSlotBytes)
	copy(slot, b)
	path := s.path(date, "object", "obj")
	idx, err := s.index(path, false, true)
	if err != nil {
		return err
	}
	old, err := idx.LookupContext(context.Background(), intKey(p.ObjHash)) // ObjectWR index.get
	if err != nil {
		return err
	}
	if old == nil {
		pos, err := s.appendData(path+".data", slot, 4)
		if err != nil {
			return err
		}
		_, err = idx.Put(intKey(p.ObjHash), offsetBytes(pos))
		return err
	}
	pos, err := dataOffset(old)
	if err != nil {
		return err
	}
	f, err := s.file(path+".data", false)
	if err != nil {
		return err
	}
	if f == nil {
		return ErrCorrupt
	}
	existing, err := s.readData(path+".data", pos, 4)
	if err != nil {
		return err
	}
	if len(existing) != objectSlotBytes {
		return fmt.Errorf("object slot size: %w", ErrCorrupt)
	}
	record := make([]byte, objectSlotBytes+4)
	binary.BigEndian.PutUint32(record, objectSlotBytes)
	copy(record[4:], slot)
	_, err = f.WriteAt(record, pos)
	return err
}
func (s *Store) Objects(date string) ([]*protocol.ObjectPack, error) {
	return s.ObjectsContext(context.Background(), date)
}
func (s *Store) ObjectsContext(ctx context.Context, date string) ([]*protocol.ObjectPack, error) {
	if err := s.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	if err := s.check(date); err != nil {
		return nil, err
	}
	path := s.path(date, "object", "obj")
	idx, err := s.index(path, false, false)
	if err != nil || idx == nil {
		return nil, err
	}
	out := make([]*protocol.ObjectPack, 0)
	used := 0
	err = idx.Scan(func(r legacyindex.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(out) >= s.config.MaxQueryRecords {
			return ErrLimit
		}
		pos, err := dataOffset(r.Value)
		if err != nil {
			return err
		}
		b, err := s.readData(path+".data", pos, 4)
		if err != nil {
			return err
		}
		used += len(b)
		if used > s.config.MaxQueryBytes {
			return ErrLimit
		}
		d := protocol.NewDecoder(b, s.config.CodecLimits)
		p := d.Pack()
		if d.Err() != nil {
			return d.Err()
		}
		o, ok := p.(*protocol.ObjectPack)
		if !ok {
			return ErrCorrupt
		}
		if len(r.Key) != 4 || o.ObjHash != int32(binary.BigEndian.Uint32(r.Key)) {
			return ErrCorrupt
		}
		out = append(out, o)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
