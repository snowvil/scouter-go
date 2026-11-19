// Copyright 2015 Scouter authors, Apache-2.0.
// Independent pinned-source Status/Summary/Alert storage implementation.
package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

type timedPackSpec struct {
	dir, base, extension, index string
	prefix                      int
	kind                        byte
}

var statusRecordSpec = timedPackSpec{"status", "status", ".pstat", "_tim", 4, protocol.PackStatus}
var alertRecordSpec = timedPackSpec{"alert", "alert", ".alert", "_tim", 2, protocol.PackAlert}

func summaryRecordSpec(stype byte) timedPackSpec {
	index := "_other_tim"
	switch stype {
	case 1:
		index = "_app_tim"
	case 2:
		index = "_sql_tim"
	case 10, 11, 12:
		index = "_enduser_tim"
	}
	return timedPackSpec{"sum", "sum", ".sum", index, 4, protocol.PackSummary}
}

// timedPackLimits are the codec limits of a stored Status/Summary/Alert pack:
// the Store's own, raised to what an Agent pack may hold (the record size is
// bounded by MaxRecordBytes), so a pack the receiver accepted is stored and
// read back.
func (s *Store) timedPackLimits() protocol.Limits {
	limits := s.config.CodecLimits
	if limits.MaxCollectionEntries < protocol.AgentPackLimits.MaxCollectionEntries {
		limits.MaxCollectionEntries = protocol.AgentPackLimits.MaxCollectionEntries
	}
	if limits.MaxTotalValues < protocol.AgentPackLimits.MaxTotalValues {
		limits.MaxTotalValues = protocol.AgentPackLimits.MaxTotalValues
	}
	return limits
}

func (s *Store) PutStatusContext(ctx context.Context, p *protocol.StatusPack) error {
	if p == nil {
		return ErrInvalid
	}
	return s.putTimedPack(ctx, p, p.Time, statusRecordSpec)
}
func (s *Store) PutSummaryContext(ctx context.Context, p *protocol.SummaryPack) error {
	if p == nil {
		return ErrInvalid
	}
	return s.putTimedPack(ctx, p, p.Time, summaryRecordSpec(p.SType))
}
func (s *Store) PutAlertContext(ctx context.Context, p *protocol.AlertPack) error {
	if p == nil {
		return ErrInvalid
	}
	return s.putTimedPack(ctx, p, p.Time, alertRecordSpec)
}

func (s *Store) putTimedPack(ctx context.Context, p protocol.Pack, tm int64, spec timedPackSpec) error {
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
	if err := validDate(date); err != nil {
		return err
	}
	b, err := protocol.EncodePack(p, s.timedPackLimits())
	if err != nil {
		return err
	}
	if len(b) > s.config.MaxRecordBytes || (spec.prefix == 2 && len(b) > 32767) {
		return ErrLimit
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	path := s.path(date, spec.dir, spec.base)
	idx, err := s.index(path+spec.index, true, true)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	pos, err := s.appendData(path+spec.extension, b, spec.prefix)
	if err != nil {
		return err
	}
	if _, err = idx.Put(longKey(tm), offsetBytes(pos)); err != nil {
		return err
	}
	return ctx.Err()
}

// extraReadData validates both the physical extent and remaining result budget
// before allocation. It uses the Store file cache for Sync/Close ownership.
func (s *Store) extraReadData(ctx context.Context, path string, pos int64, prefix, remaining int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pos < 0 || pos > 1<<39-1 {
		return nil, ErrCorrupt
	}
	f, err := s.file(path, false)
	if err != nil {
		return nil, err
	}
	if f == nil {
		return nil, fmt.Errorf("missing Pack data: %w", ErrCorrupt)
	}
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if pos > st.Size()-int64(prefix) {
		return nil, fmt.Errorf("Pack data prefix: %w", ErrCorrupt)
	}
	var head [4]byte
	if _, err = f.ReadAt(head[:prefix], pos); err != nil {
		return nil, errors.Join(ErrCorrupt, err)
	}
	n := int(int32(binary.BigEndian.Uint32(head[:])))
	if prefix == 2 {
		n = int(int16(binary.BigEndian.Uint16(head[:2])))
	}
	if n < 0 || int64(n) > st.Size()-pos-int64(prefix) {
		return nil, fmt.Errorf("Pack data extent: %w", ErrCorrupt)
	}
	if n > s.config.MaxRecordBytes || n > remaining {
		return nil, ErrLimit
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	b := make([]byte, n)
	_, err = io.ReadFull(queryReader{ctx, io.NewSectionReader(f, pos+int64(prefix), int64(n))}, b)
	if err != nil {
		return nil, err
	}
	return b, nil
}
func (s *Store) extraQueryBudget() (int, int) {
	records, bytes := s.config.MaxQueryRecords, s.config.MaxQueryBytes
	if records > 100000 {
		records = 100000
	}
	if bytes > 64<<20 {
		bytes = 64 << 20
	}
	return records, bytes
}
func (s *Store) extraQueryRange(date string, from, to int64) error {
	if err := s.queryRange(date, from, to); err != nil {
		return err
	}
	if to-from > int64(24*time.Hour/time.Millisecond) {
		return ErrLimit
	}
	return nil
}
func timedRecordTime(p protocol.Pack) (int64, bool) {
	switch v := p.(type) {
	case *protocol.StatusPack:
		return v.Time, true
	case *protocol.SummaryPack:
		return v.Time, true
	case *protocol.AlertPack:
		return v.Time, true
	}
	return 0, false
}

// streamTimedPacks reads a Status/Summary/Alert time index in Java readByTime
// order (forward ScanRange) and hands each accepted Pack to visit with its
// stored size. Like ScanXLogsContext it buffers one batch under the store gate
// and calls visit outside it; there is no total scanned record or byte cap
// (Java SummaryRD/AlertRD/StatusRD stream to the handler). accept runs under
// the gate and must not call the Store.
func (s *Store) streamTimedPacks(ctx context.Context, date string, from, to int64, spec timedPackSpec, accept func(protocol.Pack) bool, visit func(protocol.Pack, int) (bool, error)) error {
	type item struct {
		pack  protocol.Pack
		bytes int
	}
	var cursor rangeCursor
	started, done := false, false
	batch := make([]item, 0, scanBatchRecords)
	fill := func() error {
		unlock, err := s.lockScan(ctx)
		if err != nil {
			return err
		}
		defer unlock()
		if !started {
			if err := s.extraQueryRange(date, from, to); err != nil {
				return err
			}
		} else if err := s.check(date); err != nil {
			return err
		}
		path := s.path(date, spec.dir, spec.base)
		idx, err := s.index(path+spec.index, true, false)
		if cancelled := ctx.Err(); cancelled != nil {
			return cancelled
		}
		if err != nil {
			return err
		}
		if idx == nil {
			done = true
			return nil
		}
		started = true
		_, maxBytes := s.extraQueryBudget()
		budget := newHoldBudget()
		used := 0
		for !done && len(batch) < scanBatchRecords && used < scanBatchBytes && !budget.exhausted() {
			if err := ctx.Err(); err != nil {
				return err
			}
			tm, value, ok, end, err := cursor.nextRecord(idx, from, to, false, &budget)
			if err != nil {
				return err
			}
			if end {
				done = true
			}
			if !ok {
				break
			}
			budget.spend()
			pos, err := dataOffset(value)
			if err != nil {
				return err
			}
			b, err := s.extraReadData(ctx, path+spec.extension, pos, spec.prefix, maxBytes)
			if err != nil {
				return err
			}
			p, err := protocol.DecodePack(b, s.timedPackLimits())
			if err != nil {
				return errors.Join(ErrCorrupt, err)
			}
			if p.Type() != spec.kind {
				return ErrCorrupt
			}
			if packTime, ok := timedRecordTime(p); !ok || packTime != tm {
				return ErrCorrupt
			}
			if accept == nil || accept(p) {
				used += len(b)
				batch = append(batch, item{p, len(b)})
			}
		}
		return nil
	}
	for {
		batch = batch[:0]
		if err := fill(); err != nil {
			return err
		}
		for n := range batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			more, err := visit(batch[n].pack, batch[n].bytes)
			batch[n] = item{}
			if err != nil || !more {
				return err
			}
		}
		if done {
			return ctx.Err()
		}
		cursor.settlePending(from, to, false)
	}
}

// scanTimedPacks collects streamTimedPacks. The collected result keeps the
// extraQueryBudget caps (records and stored bytes of the accepted packs);
// Client queries that aggregate or stream use the Scan* variants instead.
func (s *Store) scanTimedPacks(ctx context.Context, date string, from, to int64, spec timedPackSpec, accept func(protocol.Pack) bool) ([]protocol.Pack, error) {
	maxRecords, maxBytes := s.extraQueryBudget()
	used := 0
	var out []protocol.Pack
	err := s.streamTimedPacks(ctx, date, from, to, spec, accept, func(p protocol.Pack, n int) (bool, error) {
		if len(out) >= maxRecords || used > maxBytes-n {
			return false, ErrLimit
		}
		used += n
		out = append(out, p)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ScanSummariesContext streams the SummaryPacks of one type (Java
// SummaryRD.readByTime); visit runs outside the store gate and returning
// false ends the scan.
func (s *Store) ScanSummariesContext(ctx context.Context, date string, from, to int64, stype byte, visit func(*protocol.SummaryPack) (bool, error)) error {
	accept := func(p protocol.Pack) bool { return p.(*protocol.SummaryPack).SType == stype }
	return s.streamTimedPacks(ctx, date, from, to, summaryRecordSpec(stype), accept, func(p protocol.Pack, _ int) (bool, error) {
		return visit(p.(*protocol.SummaryPack))
	})
}

// ScanAlertsContext streams the stored AlertPacks (Java AlertRD.readByTime).
func (s *Store) ScanAlertsContext(ctx context.Context, date string, from, to int64, visit func(*protocol.AlertPack) (bool, error)) error {
	return s.streamTimedPacks(ctx, date, from, to, alertRecordSpec, nil, func(p protocol.Pack, _ int) (bool, error) {
		return visit(p.(*protocol.AlertPack))
	})
}
func (s *Store) StatusesContext(ctx context.Context, date string, from, to int64) ([]*protocol.StatusPack, error) {
	packs, err := s.scanTimedPacks(ctx, date, from, to, statusRecordSpec, nil)
	if err != nil {
		return nil, err
	}
	out := make([]*protocol.StatusPack, len(packs))
	for n, p := range packs {
		out[n] = p.(*protocol.StatusPack)
	}
	return out, nil
}
func (s *Store) SummariesContext(ctx context.Context, date string, from, to int64, stype byte) ([]*protocol.SummaryPack, error) {
	packs, err := s.scanTimedPacks(ctx, date, from, to, summaryRecordSpec(stype), func(p protocol.Pack) bool { return p.(*protocol.SummaryPack).SType == stype })
	if err != nil {
		return nil, err
	}
	out := make([]*protocol.SummaryPack, len(packs))
	for n, p := range packs {
		out[n] = p.(*protocol.SummaryPack)
	}
	return out, nil
}
func (s *Store) AlertsContext(ctx context.Context, date string, from, to int64) ([]*protocol.AlertPack, error) {
	packs, err := s.scanTimedPacks(ctx, date, from, to, alertRecordSpec, nil)
	if err != nil {
		return nil, err
	}
	out := make([]*protocol.AlertPack, len(packs))
	for n, p := range packs {
		out[n] = p.(*protocol.AlertPack)
	}
	return out, nil
}
