// Scouter Zipkin span DB: Copyright 2015 Scouter authors, Apache-2.0.
// SpanContainer records use signed-short length in span/span.data; no XLog synthesis.
package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

func (s *Store) PutSpanContainer(p *protocol.SpanContainerPack) error {
	return s.PutSpanContainerContext(context.Background(), p)
}
func (s *Store) PutSpanContainerContext(ctx context.Context, p *protocol.SpanContainerPack) error {
	if err := s.mu.LockContext(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if p == nil || p.Timestamp <= 0 {
		return ErrInvalid
	}
	date := JavaDate(p.Timestamp, s.config.Location)
	if err := validDate(date); err != nil {
		return err
	}
	raw, err := protocol.EncodePack(p, s.config.CodecLimits)
	if err != nil {
		return err
	}
	if len(raw) > 32767 || len(raw) > s.config.MaxRecordBytes {
		return ErrLimit
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path := s.path(date, "span", "span")
	pos, err := s.appendData(path+".data", raw, 2)
	if err != nil {
		return err
	}
	// Finish the indexes once data is appended. Cancellation must not intentionally orphan a record.
	timed, err := s.index(path+"_tim", true, true)
	if err != nil {
		return err
	}
	if _, err := timed.Put(longKey(p.Timestamp), offsetBytes(pos)); err != nil {
		return err
	}
	if p.GxID != 0 {
		idx, err := s.index(path+"_gid", false, true)
		if err != nil {
			return err
		}
		if _, err := idx.Put(longKey(p.GxID), offsetBytes(pos)); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// spanIndexLocked is the index hold of a gxid span query: the chain
// snapshot and the source of its records. src == nil (with err == nil)
// means the date has no span gxid index.
func (s *Store) spanIndexLocked(ctx context.Context, date string, gxid int64) (*legacyindex.KeyChain, *recordSource, error) {
	if err := s.check(date); err != nil {
		return nil, nil, err
	}
	path := s.path(date, "span", "span")
	idx, err := s.index(path+"_gid", false, false)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, nil, cancelled
	}
	if err != nil || idx == nil {
		return nil, nil, err
	}
	chain, err := idx.KeyChain(longKey(gxid))
	if err != nil {
		return nil, nil, err
	}
	return chain, s.openRecordSourceLocked(date, path+".data", 2, false, !chain.Empty()), nil
}

// SpansByGxIDContext: GXID order is newest index link first, followed by
// each container's wire order. Declared SpanCount is retained in storage;
// Java reads the Blob until EOF. One Store gate hold takes the index
// snapshot; the walk and span.data run without the gate (collect queries,
// profile.go).
func (s *Store) SpansByGxIDContext(ctx context.Context, date string, gxid int64) ([]*protocol.SpanPack, error) {
	unlock, err := s.lockScan(ctx)
	if err != nil {
		return nil, err
	}
	chain, src, err := s.spanIndexLocked(ctx, date, gxid)
	unlock()
	if err != nil || src == nil {
		return nil, err
	}
	defer chain.Close()
	defer src.close()
	k := scanKeyValues(ctx, chain, s.config.MaxQueryRecords)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	var out []*protocol.SpanPack
	used := 0
	_, err = s.eachRecord(ctx, src, len(k.values), func(i int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		pos, err := dataOffset(k.values[i])
		if err != nil {
			return err
		}
		raw, err := s.readRecord(ctx, src, pos)
		if err != nil {
			return err
		}
		if used > s.config.MaxQueryBytes-len(raw) {
			return ErrLimit
		}
		used += len(raw)
		pack, err := protocol.DecodePack(raw, s.config.CodecLimits)
		if err != nil {
			return err
		}
		container, ok := pack.(*protocol.SpanContainerPack)
		if !ok || container.GxID != gxid {
			return ErrCorrupt
		}
		if out == nil {
			out = make([]*protocol.SpanPack, 0)
		}
		reader := bytes.NewReader(container.Spans)
		for reader.Len() > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(out) >= s.config.MaxQueryRecords {
				return ErrLimit
			}
			nested, err := protocol.ReadPack(reader, s.config.CodecLimits)
			if err != nil {
				return err
			}
			span, ok := nested.(*protocol.SpanPack)
			if !ok {
				return fmt.Errorf("%w: span Blob contains another Pack", ErrCorrupt)
			}
			out = append(out, span)
		}
		return nil
	})
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if err != nil {
		return nil, err
	}
	if k.over {
		return nil, ErrLimit // one more container than MaxQueryRecords
	}
	if k.walkErr != nil {
		return nil, k.walkErr
	}
	return out, nil
}

// gxidIndexLocked is the index hold of a gxid XLog query: the chain
// snapshot and the source of its records. src == nil (with err == nil)
// means the date has no gxid index. The XLog compression mode is read here;
// its error is reported by the first record read, as before.
func (s *Store) gxidIndexLocked(ctx context.Context, date string, gxid int64) (*legacyindex.KeyChain, *recordSource, error) {
	if err := s.check(date); err != nil {
		return nil, nil, err
	}
	idx, err := s.index(s.path(date, "xlog", "xlog_gid"), false, false)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, nil, cancelled
	}
	if err != nil || idx == nil {
		return nil, nil, err
	}
	chain, err := idx.KeyChain(longKey(gxid))
	if err != nil {
		return nil, nil, err
	}
	path := s.path(date, "xlog", "xlog")
	zip, modeErr := s.compressionMode(path+".service.conf", "compress_xlog_enabled", s.live.compressXLog.Load(), false)
	if modeErr != nil {
		src := s.openRecordSourceLocked(date, path+".service", 2, false, false)
		src.modeErr = modeErr
		return chain, src, nil
	}
	return chain, s.openRecordSourceLocked(date, path+".service", 2, zip, !chain.Empty()), nil
}

// XLogsByGxIDContext returns the gxid's XLogs, newest index link first. One
// Store gate hold takes the index snapshot; the walk and the records run as
// described in profile.go (collect queries).
func (s *Store) XLogsByGxIDContext(ctx context.Context, date string, gxid int64) ([]*protocol.XLogPack, error) {
	return restartOnPurgeUnknown(func() ([]*protocol.XLogPack, error) { return s.xlogsByGxIDOnce(ctx, date, gxid) })
}

func (s *Store) xlogsByGxIDOnce(ctx context.Context, date string, gxid int64) ([]*protocol.XLogPack, error) {
	unlock, err := s.lockScan(ctx)
	if err != nil {
		return nil, err
	}
	chain, src, err := s.gxidIndexLocked(ctx, date, gxid)
	unlock()
	if err != nil || src == nil {
		return nil, err
	}
	defer chain.Close()
	defer src.close()
	k := scanKeyValues(ctx, chain, s.config.MaxQueryRecords)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	var out []*protocol.XLogPack
	used := 0
	purged, err := s.eachRecord(ctx, src, len(k.values), func(i int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		pack, n, err := s.decodeXLogFrom(ctx, src, k.values[i])
		if err != nil {
			return err
		}
		if pack.GxID != gxid {
			return ErrCorrupt
		}
		if used > s.config.MaxQueryBytes-n {
			return ErrLimit
		}
		used += n
		out = append(out, pack)
		return nil
	})
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if err != nil {
		return nil, err
	}
	if purged {
		return nil, nil // as a query after the purge: no gxid index
	}
	if k.over {
		return nil, ErrLimit // one more XLog than MaxQueryRecords
	}
	if k.walkErr != nil {
		return nil, k.walkErr
	}
	return out, nil
}

// XLogsByTimeContext is the raw chronological range: no elapsed/error/object filter.
func (s *Store) XLogsByTimeContext(ctx context.Context, date string, from, to int64) ([]*protocol.XLogPack, error) {
	return s.XLogsContext(ctx, date, from, to, false)
}
