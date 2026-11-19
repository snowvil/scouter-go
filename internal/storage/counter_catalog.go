// Scouter counter queries: Copyright 2015 Scouter authors, Apache-2.0.
// Independently implemented from source dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5.
package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

// CounterRow owns its decoded values. It represents one stored NetData row,
// including an empty Map and another row with the same timestamp.
type CounterRow struct {
	Time   int64
	Values protocol.Value
}

const counterCatalogRecords = 100000
const counterCatalogBytes = 64 << 20

// CounterBulkRowsContext follows RealtimeCounterRD.readBulk's 60-second chunks
// (lines 36-64) and RealtimeCounterData.readBulk (lines 72-80). Bucket endpoints
// include deleted records and select physical oldest/newest, without filtering
// their timestamps. Offset-zero chunks are omitted by the original >0 gate.
// Rows preserve physical data order and their embedded metadata; Time is zero
// because this path has no per-row index timestamp. No objHash/time is invented.
func (c *Counters) CounterBulkRowsContext(ctx context.Context, date string, start, end int64) ([]CounterRow, error) {
	return c.CounterBulkRowsForContext(ctx, date, start, end, nil)
}

// counterBulkScanRecords bounds the records one filtered bulk query may walk.
const counterBulkScanRecords = 8000000

// CounterBulkRowsForContext is CounterBulkRowsContext for a caller that uses
// only the rows of the given object hashes (COUNTER_PAST_TIME_GROUP and the
// database period query filter on the embedded objHash). Rows of other objects
// are dropped before they are decoded into rows and do not count against the
// row and byte limits, which otherwise cap the range at a few minutes of a
// large installation's counters. A row whose objHash is absent or not a decimal is
// kept for the caller to judge. A nil set keeps every row.
func (c *Counters) CounterBulkRowsForContext(ctx context.Context, date string, start, end int64, hashes map[int32]bool) ([]CounterRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := counterDate(date); err != nil {
		return nil, errors.Join(ErrInvalid, err)
	}
	if start <= 0 || end < start || end-start > 86400000 || JavaDate(start, c.location) != date {
		return nil, ErrInvalid
	}
	// The scan runs in bounded Counters gate holds (scanRealtimeHolds); the
	// cursor is the minute chunk and the physical position inside it.
	var rows []CounterRow
	used, collected, walked := 0, 0, 0
	minute, inChunk, pos, last := start, false, int64(0), int64(0)
	fill := func(t *counterTable, budget *holdBudget, batch []counterBody) ([]counterBody, bool, error) {
		batchBytes := 0
		bodies := counterBodyReader{f: t.data}
		for len(batch) < scanBatchRecords && batchBytes < scanBatchBytes && !budget.exhausted() {
			budget.spend()
			if !inChunk {
				if minute > end {
					return batch, true, nil
				}
				chunkEnd := min(minute+59999, end)
				oldest, err := counterBulkEndpoint(ctx, t.index, minute, chunkEnd, false)
				if err != nil {
					return batch, false, err
				}
				newest, err := counterBulkEndpoint(ctx, t.index, minute, chunkEnd, true)
				if err != nil {
					return batch, false, err
				}
				if oldest == nil || newest == nil {
					minute += 60000
					continue
				}
				if len(oldest.Value) != 9 || len(newest.Value) != 9 {
					return batch, false, ErrCorrupt
				}
				first, err := readFive(oldest.Value)
				if err != nil {
					return batch, false, errors.Join(ErrCorrupt, err)
				}
				if last, err = readFive(newest.Value); err != nil {
					return batch, false, errors.Join(ErrCorrupt, err)
				}
				if first == 0 || last == 0 {
					minute += 60000
					continue
				}
				if last < first {
					return batch, false, ErrCorrupt
				}
				inChunk, pos = true, first
				continue
			}
			// Read each physical record rather than allocating the complete chunk.
			// Each prefix/extent/budget is checked before its bounded allocation.
			// With a hash filter the row and byte limits count the selected
			// rows, in deliver; the walk itself is bounded here.
			if collected+len(batch) >= counterCatalogRecords || walked >= counterBulkScanRecords {
				return batch, false, ErrLimit
			}
			body, err := bodies.body(ctx, pos, counterCatalogBytes-used-batchBytes-4*len(batch))
			if err != nil {
				return batch, false, err
			}
			walked++
			batchBytes += len(body)
			batch = append(batch, counterBody{body: body, names: t.names})
			if pos == last {
				inChunk = false
				minute += 60000
				continue
			}
			pos += int64(len(body)) + 4
			if pos > last {
				return batch, false, ErrCorrupt
			}
		}
		return batch, false, nil
	}
	// The selection runs here, without the Counters gate: a filtered scan
	// that decided under the gate held it almost continuously and starved
	// the counter writers.
	deliver := func(r counterBody) error {
		if hashes != nil && !counterBulkSelected(r.body, r.names, hashes) {
			return nil
		}
		values, err := counterCatalogMap(r.body, r.names)
		if err != nil {
			return err
		}
		used += len(r.body) + 4
		collected++
		rows = append(rows, CounterRow{Values: values})
		return nil
	}
	found, err := c.scanRealtimeHolds(ctx, date, fill, deliver, func() {})
	if err != nil || !found {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

// IndexTimeFile.getDataPosFirst/Last (lines 92-145) fetches one extra bucket
// after crossing the endpoint: the range condition is checked before stepping.
func counterBulkEndpoint(ctx context.Context, idx *legacyindex.Index, start, end int64, newest bool) (*legacyindex.Record, error) {
	tm := start
	if newest {
		tm = end
	}
	get := func() (*legacyindex.Record, error) {
		first, last, err := idx.BucketEndpointsContext(ctx, tm)
		if err != nil {
			if errors.Is(err, legacyindex.ErrLimit) {
				err = errors.Join(ErrLimit, err)
			}
			if errors.Is(err, legacyindex.ErrCorrupt) {
				err = errors.Join(ErrCorrupt, err)
			}
			return nil, err
		}
		if newest {
			return last, nil
		}
		return first, nil
	}
	r, err := get()
	for n := 0; err == nil && r == nil && n < 172800 && ((!newest && tm <= end) || (newest && tm >= start)); n++ {
		if newest {
			tm -= 500
		} else {
			tm += 500
		}
		r, err = get()
	}
	return r, err
}

// CounterRowsContext follows RealtimeCounterRD.read -> RealtimeCounterKeyFile.read
// (source lines 20/43), with IndexTimeFile V1 time/signed-value ordering. The
// existing legacyindex endpoint correction remains the same as RealtimeContext.
// A date's next-midnight inclusive endpoint is allowed, as in ExportService.
func (c *Counters) CounterRowsContext(ctx context.Context, date string, hash int32, start, end int64) ([]CounterRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := counterDate(date); err != nil {
		return nil, errors.Join(ErrInvalid, err)
	}
	if start <= 0 || end < start || end-start > 86400000 || JavaDate(start, c.location) != date {
		return nil, ErrInvalid
	}
	// The scan runs in bounded Counters gate holds (scanRealtimeHolds) with
	// the ScanRange order cursor of the XLog scan.
	var rows []CounterRow
	scanned, used := 0, 0
	var cur rangeCursor
	fill := func(t *counterTable, budget *holdBudget, batch []counterBody) ([]counterBody, bool, error) {
		batchBytes := 0
		for len(batch) < scanBatchRecords && batchBytes < scanBatchBytes && !budget.exhausted() {
			tm, value, ok, last, err := cur.nextRecord(t.index, start, end, false, budget)
			if err != nil || last {
				return batch, last, err
			}
			if !ok {
				break
			}
			if err := ctx.Err(); err != nil {
				return batch, false, err
			}
			budget.spend()
			if scanned >= counterCatalogRecords || used > counterCatalogBytes-8-len(value) {
				return batch, false, ErrLimit
			}
			scanned++
			used += 8 + len(value)
			if len(value) != 9 {
				return batch, false, ErrCorrupt
			}
			if int32(binary.BigEndian.Uint32(value[5:])) != hash {
				continue
			}
			pos, err := readFive(value)
			if err != nil {
				return batch, false, errors.Join(ErrCorrupt, err)
			}
			body, err := counterCatalogBody(ctx, t.data, pos, counterCatalogBytes-used)
			if err != nil {
				return batch, false, err
			}
			used += len(body) + 4
			batchBytes += len(body)
			batch = append(batch, counterBody{time: tm, body: body, names: t.names})
		}
		return batch, false, nil
	}
	deliver := func(r counterBody) error {
		values, err := counterCatalogMap(r.body, r.names)
		if err != nil {
			return err
		}
		rows = append(rows, CounterRow{Time: r.time, Values: values})
		return ctx.Err()
	}
	found, err := c.scanRealtimeHolds(ctx, date, fill, deliver, func() { cur.settlePending(start, end, false) })
	if err != nil {
		if errors.Is(err, legacyindex.ErrLimit) {
			return nil, errors.Join(ErrLimit, err)
		}
		if errors.Is(err, legacyindex.ErrCorrupt) {
			return nil, errors.Join(ErrCorrupt, err)
		}
		return nil, err
	}
	if !found {
		return nil, nil
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func counterCatalogBody(ctx context.Context, f *os.File, pos int64, remaining int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if pos < 0 || pos > st.Size()-4 {
		return nil, ErrCorrupt
	}
	var head [4]byte
	if _, err = f.ReadAt(head[:], pos); err != nil {
		return nil, errors.Join(ErrCorrupt, err)
	}
	n := int64(int32(binary.BigEndian.Uint32(head[:])))
	if n < 0 || n > st.Size()-pos-4 {
		return nil, ErrCorrupt
	}
	if n > 8<<20 || remaining < 4 || n > int64(remaining-4) {
		return nil, ErrLimit
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	body := make([]byte, int(n))
	if _, err = io.ReadFull(queryReader{ctx, io.NewSectionReader(f, pos+4, n)}, body); err != nil {
		return nil, errors.Join(ErrCorrupt, err)
	}
	return body, nil
}

// counterBodyBlock is the read-ahead of counterBodyReader: the bulk scan reads
// consecutive records, a few hundred of which fit one block.
const counterBodyBlock = 128 << 10

// counterBodyReader reads the record bodies of one Counters gate hold with the
// checks and errors of counterCatalogBody, from shared read-ahead blocks in
// place of a stat and two reads per record. The file does not change during
// the hold. A returned body is a view of a block that is never written again:
// a later read past the block takes a new one.
type counterBodyReader struct {
	f     *os.File
	size  int64
	sized bool
	block []byte
	at    int64 // file offset of block[0]
}

// view returns n bytes at pos from the current block, reading a new block
// there when it does not hold them. n is at most counterBodyBlock.
func (r *counterBodyReader) view(pos int64, n int) ([]byte, error) {
	if pos < r.at || pos+int64(n) > r.at+int64(len(r.block)) {
		block := make([]byte, min(int64(counterBodyBlock), r.size-pos))
		if _, err := r.f.ReadAt(block, pos); err != nil {
			return nil, errors.Join(ErrCorrupt, err)
		}
		r.block, r.at = block, pos
	}
	from := int(pos - r.at)
	return r.block[from : from+n : from+n], nil
}

func (r *counterBodyReader) body(ctx context.Context, pos int64, remaining int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !r.sized {
		st, err := r.f.Stat()
		if err != nil {
			return nil, err
		}
		r.size, r.sized = st.Size(), true
	}
	if pos < 0 || pos > r.size-4 {
		return nil, ErrCorrupt
	}
	head, err := r.view(pos, 4)
	if err != nil {
		return nil, err
	}
	n := int64(int32(binary.BigEndian.Uint32(head)))
	if n < 0 || n > r.size-pos-4 {
		return nil, ErrCorrupt
	}
	if n > 8<<20 || remaining < 4 || n > int64(remaining-4) {
		return nil, ErrLimit
	}
	if n+4 > counterBodyBlock {
		// Larger than a block: its own read, as counterCatalogBody.
		body := make([]byte, int(n))
		if _, err = io.ReadFull(queryReader{ctx, io.NewSectionReader(r.f, pos+4, n)}, body); err != nil {
			return nil, errors.Join(ErrCorrupt, err)
		}
		return body, nil
	}
	// One view of head and body: a record split by the block end is read
	// again with the next block, which starts at this record.
	record, err := r.view(pos, int(n)+4)
	if err != nil {
		return nil, err
	}
	return record[4:], nil
}

// counterBulkSelected reports whether a stored row belongs to one of hashes.
// It reads entries only up to the first objHash. Anything it cannot decide is
// selected, so the full decode and the caller's own filter see the row
// unchanged.
func counterBulkSelected(body []byte, names []string, hashes map[int32]bool) bool {
	id := int64(0)
	for i, name := range names {
		if name == "objHash" {
			id = int64(i + 1)
			break
		}
	}
	if id == 0 {
		return true
	}
	d := protocol.NewDecoder(body, protocol.Limits{})
	count := d.Decimal()
	if d.Err() != nil || count < 0 || count > 4096 {
		return true
	}
	for n := int64(0); n < count; n++ {
		entry := d.Decimal()
		hash, decimal := d.DecimalValue()
		if d.Err() != nil {
			return true
		}
		if entry != id {
			continue
		}
		if !decimal || hash < -1<<31 || hash > 1<<31-1 {
			return true
		}
		return hashes[int32(hash)]
	}
	return true
}

// RealtimeCounterDBHelper.setTagBytes (lines 46-61) maps dictionary IDs to
// names and replaces repeated names. Entry order here preserves stored bytes;
// Java MapValue Hashtable enumeration is not an insertion-order contract.
func counterCatalogMap(body []byte, names []string) (protocol.Value, error) {
	d := protocol.NewDecoder(body, protocol.Limits{})
	count := d.Decimal()
	if d.Err() != nil {
		return protocol.Value{}, errors.Join(ErrCorrupt, d.Err())
	}
	if count < 0 {
		return protocol.Value{}, ErrCorrupt
	}
	if count > 4096 {
		return protocol.Value{}, ErrLimit
	}
	v := protocol.Value{Type: protocol.ValueMap}
	positions := make(map[string]int)
	for n := int64(0); n < count; n++ {
		id := d.Decimal()
		value := d.Value()
		if d.Err() != nil {
			return protocol.Value{}, errors.Join(ErrCorrupt, d.Err())
		}
		if id < 1 || id > int64(len(names)) {
			return protocol.Value{}, ErrCorrupt
		}
		name := names[id-1]
		if position, exists := positions[name]; exists {
			v.Entries[position].Value = value
		} else {
			positions[name] = len(v.Entries)
			v.Entries = append(v.Entries, protocol.Entry{Key: name, Value: value})
		}
	}
	if d.Remaining() != 0 {
		return protocol.Value{}, errors.Join(ErrCorrupt, protocol.ErrTrailing)
	}
	return v, nil
}

// DailyCounterNamesContext follows DailyCounterRD.readKey (line 77) and
// IndexKeyFile.read (line 166): live physical records in append order, filtered
// by object hash. Names shared by different TimeTypes occur once, at first use.
// ExportService sorts the union with configured counters after this enumeration.
func (c *Counters) DailyCounterNamesContext(ctx context.Context, date string, hash int32) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := counterDate(date); err != nil {
		return nil, errors.Join(ErrInvalid, err)
	}
	if err := c.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	t, err := c.table(date, false, false)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if err != nil || t == nil {
		return nil, err
	}
	var names []string
	seen := make(map[string]bool)
	scanned, used := 0, 0
	err = t.index.Scan(func(r legacyindex.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.Deleted {
			return nil
		}
		if scanned >= counterCatalogRecords || used > counterCatalogBytes-len(r.Key)-len(r.Value) {
			return ErrLimit
		}
		scanned++
		used += len(r.Key) + len(r.Value)
		d := protocol.NewDecoder(r.Key, protocol.Limits{})
		objHash, name, kind := d.Int32(), d.Text(), d.Byte()
		if d.Err() != nil || d.Remaining() != 0 || len(r.Value) != 5 || kind < 2 || kind > 6 {
			return errors.Join(ErrCorrupt, d.Err())
		}
		if _, err := readFive(r.Value); err != nil {
			return errors.Join(ErrCorrupt, err)
		}
		if objHash == hash && !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, legacyindex.ErrLimit) {
			return nil, errors.Join(ErrLimit, err)
		}
		if errors.Is(err, legacyindex.ErrCorrupt) {
			return nil, errors.Join(ErrCorrupt, err)
		}
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return names, nil
}
