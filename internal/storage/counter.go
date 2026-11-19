// Scouter counter file formats: Copyright 2015 Scouter authors, Apache-2.0.
// Independently implemented from the pinned source; see compatibility/storage-contract.md.
package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
	"github.com/snowvil/scouter-go/internal/syncx"
)

type CounterPoint struct {
	Time  int64
	Value protocol.Value
}
type Counters struct {
	// OnRecover observes automatic index recovery; set before first use.
	OnRecover func(RecoveryEvent)
	mu        syncx.Mutex
	// closes/syncRun: see Store; Sync fsyncs outside mu and closes handles
	// evicted meanwhile afterwards.
	closes   closeQueue
	syncRun  sync.Mutex
	root     string
	location *time.Location
	closed   bool
	tables   map[string]*counterTable
	warn     traversalWarner
	// opens counts how table() accepted index pairs (own leaf lock, see
	// Store.opens).
	opens openCounts
	// seals are the clean-shutdown seals handed over by Store.AttachCounters
	// (clean.go), consumed per open; closedSeals are the pairs the last
	// successful Close sealed. Both guarded by mu.
	seals, closedSeals map[string]legacyindex.Seal
	// checkpoints is the crash checkpoint log of the counter pairs
	// (counterCheckpointName, clean.go); it has its own lock.
	checkpoints *checkpointLog
	// purged counts the purges and DB deletes of each date (guarded by mu),
	// so a scan between its holds tells a purged and recreated table from
	// an evicted and reopened one (scanRealtimeHolds). One small entry per
	// purged date.
	purged map[string]uint64
	// namesMax bounds a day's whole realtime dictionary (Java names
	// included) to which putRealtime adds names (realtimeNamesDefaultMax;
	// tests lower it). namesLimited counts the PerfCounterPack entries
	// dropped at that bound.
	namesMax     int
	namesLimited atomic.Uint64
	// headFailures remembers, by .head path and file identity, a realtime
	// dictionary that failed to load, so packs for that date do not reread it
	// under the gate until the file changes (guarded by mu, at most
	// headFailuresMax entries).
	headFailures map[string]headFailure
}

type headFailure struct {
	size int64
	mod  time.Time
	err  error
}

const headFailuresMax = 64

// Realtime counter name dictionary ({date}/counter/real.head) bounds. Java
// (RealtimeCounterDBHeader.intern) has none: it keeps every name of the day in
// two maps and rewrites the whole dictionary on each pack that adds one. The
// names are day-wide, not per object: built-in counter names, CustomJmx and
// plugin counters, and Telegraf counters whose names may embed tag values.
//
// Reading accepts any dictionary up to realtimeHeadMaxBody bytes and
// realtimeNamesReadMax names, an explicit ErrLimit beyond (memory guard: the
// name bytes plus about 100 bytes a name for the slice and ids map, so at most
// about 160-220MB a table). Writing adds names while the whole dictionary,
// Java names included, stays within Counters.namesMax names and
// realtimeNamesMaxBody bytes (about 16MB of names plus 6.5MB a table), so
// Go's own dictionaries always stay readable; a Java dictionary already past
// that bound still loads and keeps storing its known names.
const (
	realtimeHeadMaxBody     = 64 << 20
	realtimeNamesReadMax    = 1 << 20
	realtimeNamesDefaultMax = 1 << 16
	realtimeNamesMaxBody    = 16 << 20
)

// NamesLimited is the number of realtime PerfCounterPack entries dropped
// because their new name did not fit the day's dictionary bound; the other
// entries of those packs were stored.
func (c *Counters) NamesLimited() uint64 { return c.namesLimited.Load() }

// markPurgedLocked records a purge or DB delete below date (c.mu held).
func (c *Counters) markPurgedLocked(date string) {
	if c.purged == nil {
		c.purged = map[string]uint64{}
	}
	c.purged[date]++
}

// IndexOpenStats returns how counter index pairs were opened since
// OpenCounters (see Store.IndexOpenStats; the marker fields stay zero).
func (c *Counters) IndexOpenStats() IndexOpenStats { return c.opens.get() }

// SetLog sets the sink of Java-id warnings (S152 daily counter index
// traversal, as DailyCounterIndex.get); nil drops them.
func (c *Counters) SetLog(f LogFunc) { c.warn.setLog(f) }

// SetIndexTraversalWarningCount applies log_index_traversal_warning_count.
func (c *Counters) SetIndexTraversalWarningCount(n int) { c.warn.setLimit(n) }

type counterTable struct {
	index *legacyindex.Index
	data  *os.File
	// names (id-1 → name) and ids (name → id) are the realtime dictionary,
	// as on disk; headBody is the dictionary body length in .head (count and
	// entries; 0 when there is none). Changed only by putRealtime under the
	// Counters gate, which only appends to names (see counterBody).
	names    []string
	ids      map[string]int32
	headBody int
	head     string
	path     string // index path without suffix (clean-shutdown seal key)
}

func OpenCounters(root string, location *time.Location) (*Counters, error) {
	if location == nil {
		return nil, errors.New("counter timezone required")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	return &Counters{root: root, location: location, tables: make(map[string]*counterTable), checkpoints: loadCheckpoints(root, counterCheckpointName), namesMax: realtimeNamesDefaultMax}, nil
}
func CounterHash(name string) int32 { return int32(crc32.ChecksumIEEE([]byte(name))) }
func counterDate(date string) error {
	if len(date) != 8 {
		return errors.New("invalid date")
	}
	if _, err := time.Parse("20060102", date); err != nil {
		return err
	}
	return nil
}
func (c *Counters) table(date string, realtime, create bool) (*counterTable, error) {
	if c.closed {
		return nil, os.ErrClosed
	}
	if err := counterDate(date); err != nil {
		return nil, err
	}
	prefix := "5m"
	if realtime {
		prefix = "real"
	}
	key := date + "/" + prefix
	if t := c.tables[key]; t != nil {
		return t, nil
	}
	if c.closes.adopt(key) {
		if len(c.tables) > 8 {
			if err := c.evictTableLocked(key); err != nil {
				return nil, err
			}
		}
		return c.tables[key], nil
	}
	path := filepath.Join(c.root, date, "counter", prefix)
	if !create {
		if _, err := os.Stat(path + ".data"); err != nil {
			if os.IsNotExist(err) {
				return nil, nil
			}
			return nil, err
		}
	}
	// The dictionary is read before the index pair is opened: an unreadable
	// one fails without walking the index (or evicting another table).
	t := &counterTable{head: path + ".head", path: path}
	if realtime {
		if err := c.loadNamesLocked(t); err != nil {
			return nil, err
		}
	}
	if len(c.tables) >= 8 { // bounded open days; every eviction closes and flushes the index.
		if err := c.evictTableLocked(""); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	seal := takeSeal(c.seals, path)
	idx, err := legacyindex.OpenCheckpointed(path, 1<<20, realtime, c.location, nil, seal, c.checkpoints.get(path))
	if err == nil && !realtime {
		idx.SetDeepSearch(c.warn.hook(javaIndexKeyText))
	}
	if err != nil {
		c.opens.failed()
		return nil, err
	}
	info := idx.OpenInfo()
	c.opens.count(info)
	if info.Check == legacyindex.CheckCreated {
		c.checkpoints.drop(path)
	}
	reportIndexRecovery(c.OnRecover, path, idx.Recovered())
	f, err := os.OpenFile(path+".data", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		idx.Close()
		return nil, err
	}
	t.index, t.data = idx, f
	c.tables[key] = t
	return t, nil
}

// loadNamesLocked is loadNames with the headFailures cache (c.mu held).
func (c *Counters) loadNamesLocked(t *counterTable) error {
	st, statErr := os.Stat(t.head)
	if f, ok := c.headFailures[t.head]; ok && statErr == nil && f.size == st.Size() && f.mod.Equal(st.ModTime()) {
		return f.err
	}
	err := t.loadNames()
	if err == nil || statErr != nil {
		delete(c.headFailures, t.head)
		return err
	}
	if c.headFailures == nil || len(c.headFailures) >= headFailuresMax {
		c.headFailures = map[string]headFailure{}
	}
	c.headFailures[t.head] = headFailure{st.Size(), st.ModTime(), err}
	return err
}

// loadNames reads the dictionary as the RealtimeCounterDBHeader constructor
// does: the file as if padded with zeros (openHeaderFile pads a file shorter
// than 2048 bytes, a short read leaves zeros), int32 body length at 512 (none
// unless positive), then int32 count (none unless positive) and count
// (text, int32 id) entries in any order. Bytes after the count entries are
// ignored, as Java does; they are what a torn Java saveTags (length written,
// body not) or a torn Go append leaves. headBody is the end of the parsed
// entries, so the next append overwrites that tail and its length covers
// exactly the dictionary. A name on several ids resolves to the entry last in
// the file (Java tagStrInt.put order); a repeated id, an id out of range, an
// invalid text or a count the body cannot hold is ErrInvalid (Java would keep
// the entries parsed before its exception).
func (t *counterTable) loadNames() error {
	t.ids = map[string]int32{}
	f, err := os.Open(t.head)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	var size [4]byte
	if _, err = f.ReadAt(size[:], 512); err != nil && err != io.EOF {
		return err
	}
	n := int64(int32(binary.BigEndian.Uint32(size[:])))
	if n < 4 { // Java: no body, or a readInt failure keeping nothing
		return nil
	}
	if n > realtimeHeadMaxBody {
		return protocol.ErrLimit
	}
	b := make([]byte, n)
	if _, err = f.ReadAt(b, 516); err != nil && err != io.EOF {
		return err
	}
	d := protocol.NewDecoder(b, protocol.Limits{MaxBytes: realtimeHeadMaxBody, MaxBlobBytes: realtimeHeadMaxBody})
	count := max(d.Int32(), 0)
	if d.Err() != nil {
		return d.Err()
	}
	if int64(count) > (n-4)/5 { // an entry is at least 5 bytes
		return protocol.ErrInvalid
	}
	if count > realtimeNamesReadMax {
		return protocol.ErrLimit
	}
	names := make([]string, count)
	seen := make([]bool, count)
	ids := make(map[string]int32, count)
	for i := int32(0); i < count; i++ {
		name := d.Text()
		id := d.Int32()
		if d.Err() != nil {
			return d.Err()
		}
		if id < 1 || id > count || seen[id-1] {
			return protocol.ErrInvalid
		}
		names[id-1], seen[id-1] = name, true
		ids[name] = id
	}
	t.names, t.ids, t.headBody = names, ids, d.Offset()
	return nil
}

// counterNameSize is the encoded size of one dictionary entry.
func counterNameSize(name string) int {
	switch n := len(name); {
	case n <= 253:
		return 1 + n + 4
	case n <= 65535:
		return 3 + n + 4
	default:
		return 5 + n + 4
	}
}

// saveNames persists t.names[from:], the names putRealtime just added, before
// any record refers to their ids. An existing .head is extended in place as
// Java rewrites it in place: the new entries are written after the body and
// fsynced, then the length and count (8 bytes at 512, one sector) are
// rewritten and fsynced. Until that second write the file still holds the
// previous dictionary; entries after its length are ignored by both readers
// and overwritten by the next save. A missing .head (a new day, or one removed
// under the open table) is created with the whole dictionary through a
// temporary file and rename. On error the on-disk dictionary is the previous
// one or (the length write reached the file but its fsync failed) a superset
// no record refers to; headBody is left for the previous one.
func (t *counterTable) saveNames(from int) error {
	f, err := os.OpenFile(t.head, os.O_RDWR, 0)
	create := os.IsNotExist(err)
	if err != nil && !create {
		return err
	}
	base := t.headBody
	if create {
		from, base = 0, 0
	}
	e := protocol.NewEncoder(protocol.Limits{MaxBytes: realtimeHeadMaxBody, MaxBlobBytes: realtimeHeadMaxBody})
	for i, name := range t.names[from:] {
		e.Text(name)
		e.Int32(int32(from + i + 1))
	}
	entries, err := e.Bytes()
	body := max(base, 4) + len(entries)
	var head [8]byte
	binary.BigEndian.PutUint32(head[:4], uint32(body))
	binary.BigEndian.PutUint32(head[4:], uint32(len(t.names)))
	if create {
		if err == nil {
			out := make([]byte, max(2048, 516+body))
			copy(out[512:], head[:])
			copy(out[520:], entries)
			err = t.createNames(out)
		}
	} else {
		if err == nil {
			_, err = f.WriteAt(entries, 516+int64(max(base, 4)))
			err = counterHeadStep("entries", err)
		}
		if err == nil {
			err = counterHeadStep("entries-sync", f.Sync())
		}
		if err == nil {
			_, err = f.WriteAt(head[:], 512)
			err = counterHeadStep("length", err)
		}
		if err == nil {
			err = counterHeadStep("length-sync", f.Sync())
		}
		err = errors.Join(err, f.Close())
	}
	if err == nil {
		t.headBody = body
	}
	return err
}

// counterHeadFault, when set by a test, fails a step of an in-place
// dictionary save after it ran.
var counterHeadFault func(step string) error

func counterHeadStep(step string, err error) error {
	if err == nil && counterHeadFault != nil {
		return counterHeadFault(step)
	}
	return err
}

// createNames writes a new .head through a temporary file, rename and a
// directory fsync, so the name is durable before a record refers to its ids.
func (t *counterTable) createNames(out []byte) error {
	// A replacement prevents a partially written dictionary from becoming visible.
	tmp, err := os.CreateTemp(filepath.Dir(t.head), ".counter-head-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(out); err == nil {
		err = tmp.Sync()
	}
	err = errors.Join(err, tmp.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(tmp.Name(), t.head); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(t.head))
}
func counterKey(hash int32, name string, kind byte) ([]byte, error) {
	e := protocol.NewEncoder(protocol.Limits{})
	e.Int32(hash)
	e.Text(name)
	e.Byte(kind)
	return e.Bytes()
}
func fiveBytes(v int64) []byte {
	return []byte{byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}
func readFive(b []byte) (int64, error) {
	if len(b) < 5 {
		return 0, io.ErrUnexpectedEOF
	}
	if b[0] >= 128 {
		return 0, protocol.ErrInvalid
	}
	return int64(b[0])<<32 | int64(b[1])<<24 | int64(b[2])<<16 | int64(b[3])<<8 | int64(b[4]), nil
}
func counterShape(kind, vt byte) (count, width int, err error) {
	switch kind {
	case 2:
		count = 1440
	case 3:
		count = 288
	case 4:
		count = 144
	case 5:
		count = 24
	case 6:
		count = 1
	default:
		return 0, 0, protocol.ErrUnsupported
	}
	switch vt {
	case protocol.ValueBoolean:
		width = 2
	case protocol.ValueFloat:
		width = 5
	case protocol.ValueDouble:
		width = 9
	case protocol.ValueDecimal:
		width = 10
	default:
		return 0, 0, protocol.ErrUnsupported
	}
	return
}
func (c *Counters) Put(p *protocol.PerfCounterPack) error {
	return c.PutContext(context.Background(), p)
}
func (c *Counters) PutContext(ctx context.Context, p *protocol.PerfCounterPack) error {
	if err := c.mu.LockContext(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if p == nil || p.Time <= 0 || p.Data.Type != protocol.ValueMap {
		return protocol.ErrInvalid
	}
	date := JavaDate(p.Time, c.location)
	minute := int(JavaMillis(p.Time, c.location) / 60000)
	t, err := c.table(date, p.TimeType == 1, true)
	if err != nil {
		return err
	}
	if p.TimeType == 1 {
		dropped, first, err := t.putRealtime(p, c.namesMax)
		if dropped > 0 {
			c.namesLimited.Add(uint64(dropped))
			if log := c.warn.log.Load(); log != nil && c.warn.allow("S133") {
				(*log)("S133", 10, fmt.Sprintf("[warn] realtime counter names of %s reached the limit (%d names): dropped %d new counter(s) of %.200q, first %.64q", date, len(t.names), dropped, p.ObjName, first))
			}
		}
		return err
	}
	for _, entry := range p.Data.Entries {
		if err := t.putDaily(CounterHash(p.ObjName), entry.Key, p.TimeType, minute, entry.Value); err != nil {
			return err
		}
	}
	return nil
}

// putRealtime is RealtimeCounterWR: intern the pack's names (one dictionary
// save for all new ones), then append the record (Decimal count, Decimal id
// and Value per entry, in pack order) and index time → (Long5 position,
// objHash). A new name beyond the dictionary bound (maxNames names or
// realtimeNamesMaxBody bytes) is dropped with its entry; the rest of the pack
// is stored, and a pack left without entries writes no record. Java never
// drops a name. dropped and first report the dropped entries.
func (t *counterTable) putRealtime(p *protocol.PerfCounterPack, maxNames int) (dropped int, first string, err error) {
	old := len(t.names)
	body := max(t.headBody, 4)
	for _, entry := range p.Data.Entries {
		if _, ok := t.ids[entry.Key]; ok {
			continue
		}
		size := counterNameSize(entry.Key)
		if len(t.names) >= maxNames || body+size > realtimeNamesMaxBody {
			if dropped == 0 {
				first = entry.Key
			}
			dropped++
			continue
		}
		t.names = append(t.names, entry.Key)
		t.ids[entry.Key] = int32(len(t.names))
		body += size
	}
	if len(t.names) > old {
		if err = t.saveNames(old); err != nil {
			for _, name := range t.names[old:] {
				delete(t.ids, name)
			}
			t.names = t.names[:old]
			return 0, "", err
		}
	}
	kept := len(p.Data.Entries) - dropped
	if kept == 0 && dropped > 0 {
		return dropped, first, nil
	}
	e := protocol.NewEncoder(protocol.Limits{})
	e.Decimal(int64(kept))
	for _, entry := range p.Data.Entries {
		if id, ok := t.ids[entry.Key]; ok {
			e.Decimal(int64(id))
			e.Value(entry.Value)
		}
	}
	b, err := e.Bytes()
	if err != nil {
		return dropped, first, err
	}
	pos, err := t.data.Seek(0, io.SeekEnd)
	if err != nil {
		return dropped, first, err
	}
	if pos > 1<<39-1 {
		return dropped, first, protocol.ErrLimit
	}
	record := make([]byte, 4+len(b))
	binary.BigEndian.PutUint32(record, uint32(len(b)))
	copy(record[4:], b)
	if _, err = t.data.Write(record); err != nil {
		return dropped, first, err
	}
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(p.Time))
	value := append(fiveBytes(pos), make([]byte, 4)...)
	binary.BigEndian.PutUint32(value[5:], uint32(CounterHash(p.ObjName)))
	_, err = t.index.Put(key, value)
	return dropped, first, err
}
func (t *counterTable) putDaily(hash int32, name string, kind byte, minute int, v protocol.Value) error {
	key, err := counterKey(hash, name, kind)
	if err != nil {
		return err
	}
	old, err := t.index.LookupContext(context.Background(), key) // DailyCounterWR index.get
	if err != nil {
		return err
	}
	var pos int64
	var count, width int
	if old == nil {
		if v.Type == protocol.ValueNull {
			return nil
		}
		count, width, err = counterShape(kind, v.Type)
		if err != nil {
			return err
		}
		pos, err = t.data.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		if pos > 1<<39-1 {
			return protocol.ErrLimit
		}
		b := make([]byte, 2+count*width)
		b[0] = v.Type
		b[1] = kind
		if _, err = t.data.Write(b); err != nil {
			return err
		}
		if _, err = t.index.Put(key, fiveBytes(pos)); err != nil {
			return err
		}
	} else {
		pos, err = readFive(old)
		if err != nil {
			return err
		}
		head := make([]byte, 2)
		if _, err = t.data.ReadAt(head, pos); err != nil {
			return err
		}
		if head[1] != kind || (v.Type != protocol.ValueNull && head[0] != v.Type) {
			return fmt.Errorf("counter bucket value type changed: %w", protocol.ErrInvalid)
		}
		count, width, err = counterShape(kind, head[0])
		if err != nil {
			return err
		}
	}
	interval := 1440 / count
	bucket := minute / interval
	if minute < 0 || minute >= 1440 || bucket >= count {
		return protocol.ErrInvalid
	}
	b, err := protocol.EncodeValue(v, protocol.Limits{})
	if err != nil {
		return err
	}
	if len(b) > width {
		return protocol.ErrInvalid
	}
	padded := make([]byte, width)
	copy(padded, b)
	_, err = t.data.WriteAt(padded, pos+2+int64(bucket*width))
	return err
}
func (c *Counters) Daily(date string, hash int32, name string, kind byte) ([]protocol.Value, error) {
	return c.DailyContext(context.Background(), date, hash, name, kind)
}
func (c *Counters) DailyContext(ctx context.Context, date string, hash int32, name string, kind byte) ([]protocol.Value, error) {
	if err := c.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	t, err := c.table(date, false, false)
	if err != nil || t == nil {
		return nil, err
	}
	key, err := counterKey(hash, name, kind)
	if err != nil {
		return nil, err
	}
	b, err := t.index.LookupContext(ctx, key) // DailyCounterRD idx.get
	if err != nil || b == nil {
		return nil, err
	}
	pos, err := readFive(b)
	if err != nil {
		return nil, err
	}
	head := make([]byte, 2)
	if _, err = t.data.ReadAt(head, pos); err != nil {
		return nil, err
	}
	count, width, err := counterShape(head[1], head[0])
	if err != nil {
		return nil, err
	}
	if head[1] != kind {
		return nil, protocol.ErrInvalid
	}
	data := make([]byte, count*width)
	if _, err = t.data.ReadAt(data, pos+2); err != nil {
		return nil, err
	}
	out := make([]protocol.Value, count)
	for i := range out {
		d := protocol.NewDecoder(data[i*width:(i+1)*width], protocol.Limits{})
		out[i] = d.Value()
		if d.Err() != nil {
			return nil, d.Err()
		}
	}
	return out, nil
}
func (c *Counters) Realtime(date string, hash int32, name string, start, end int64) ([]CounterPoint, error) {
	return c.RealtimeContext(context.Background(), date, hash, name, start, end)
}

// RealtimeContext reads one counter's realtime history in ScanRange order.
// The scan runs in bounded Counters gate holds (scanRealtimeHolds), so
// PerfCounterPack ingest waits at most one hold, not the whole range. Records
// appended to a bucket after the scan began collecting it are not seen; a
// later bucket is read when the scan reaches it.
func (c *Counters) RealtimeContext(ctx context.Context, date string, hash int32, name string, start, end int64) ([]CounterPoint, error) {
	series, err := c.RealtimeMultiContext(ctx, date, []int32{hash}, name, start, end)
	if err != nil || series == nil { // nil: no realtime table for date
		return nil, err
	}
	return series[hash].Points()
}

// CounterSeries is one object's points of a RealtimeMultiContext scan, kept
// compact: a time and the encoded Value per point (Points decodes them).
type CounterSeries struct {
	times  []int64
	values []byte
}

// Len is the number of points.
func (s *CounterSeries) Len() int {
	if s == nil {
		return 0
	}
	return len(s.times)
}

// Points decodes the series (an empty, non-nil slice when there is none).
func (s *CounterSeries) Points() ([]CounterPoint, error) {
	out := make([]CounterPoint, 0, s.Len())
	if s == nil {
		return out, nil
	}
	// Each value passed the record's limits when it was scanned; only the
	// totals of the concatenation exceed the defaults.
	d := protocol.NewDecoder(s.values, protocol.Limits{MaxBytes: max(len(s.values), 8<<20), MaxTotalValues: math.MaxInt32})
	for _, tm := range s.times {
		v := d.Value()
		if err := d.Err(); err != nil {
			return nil, err
		}
		out = append(out, CounterPoint{tm, v})
	}
	return out, nil
}

// realtimeSeriesPoints bounds one object's points (as RealtimeContext always
// did) and realtimeSeriesBytes the encoded values of a whole multi-object scan
// (about 13 bytes a point; a day of 2s points of hundreds of objects fits).
const (
	realtimeSeriesPoints = 100000
	realtimeSeriesBytes  = 256 << 20
)

// RealtimeMultiContext is RealtimeContext for several objects in one walk of
// the realtime index. The index value holds the objHash next to the data
// position, so records of other objects are skipped without reading their
// data; COUNTER_PAST_TIME_ALL over an objType walked the whole range once per
// object before. Each object's points are those RealtimeContext returns for
// it, in the same order. The result has an entry for every hash with points.
func (c *Counters) RealtimeMultiContext(ctx context.Context, date string, hashes []int32, name string, start, end int64) (map[int32]*CounterSeries, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if start <= 0 || end < start || end-start > 86400000 {
		return nil, protocol.ErrInvalid
	}
	want := make(map[int32]bool, len(hashes))
	for _, h := range hashes {
		want[h] = true
	}
	// Only the series are retained: each record (all counters of the object
	// at one time) is read, the named counter kept and the rest dropped, so
	// the scan has no total byte cap beyond the kept values (Java reads the
	// whole range too).
	out := map[int32]*CounterSeries{}
	kept := 0
	var cur rangeCursor
	fill := func(t *counterTable, budget *holdBudget, batch []counterBody) ([]counterBody, bool, error) {
		used := 0
		for len(batch) < scanBatchRecords && used < scanBatchBytes && !budget.exhausted() {
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
			if len(value) != 9 {
				return batch, false, protocol.ErrInvalid
			}
			hash := int32(binary.BigEndian.Uint32(value[5:]))
			if !want[hash] {
				continue
			}
			pos, err := readFive(value)
			if err != nil {
				return batch, false, err
			}
			head := make([]byte, 4)
			if _, err = t.data.ReadAt(head, pos); err != nil {
				return batch, false, err
			}
			n := int(binary.BigEndian.Uint32(head))
			if n > 8<<20 {
				return batch, false, protocol.ErrLimit
			}
			b := make([]byte, n)
			if _, err = t.data.ReadAt(b, pos+4); err != nil {
				return batch, false, err
			}
			used += n
			batch = append(batch, counterBody{time: tm, hash: hash, body: b, names: t.names})
		}
		return batch, false, nil
	}
	deliver := func(r counterBody) error {
		d := protocol.NewDecoder(r.body, protocol.Limits{})
		count := d.Decimal()
		if count < 0 || count > 4096 {
			return protocol.ErrLimit
		}
		for i := int64(0); i < count; i++ {
			id := d.Decimal()
			at := d.Offset()
			d.Value()
			if d.Err() != nil {
				return d.Err()
			}
			if id < 1 || id > int64(len(r.names)) {
				return protocol.ErrInvalid
			}
			if r.names[id-1] == name {
				s := out[r.hash]
				if s == nil {
					s = &CounterSeries{}
					out[r.hash] = s
				}
				raw := r.body[at:d.Offset()]
				if len(s.times) >= realtimeSeriesPoints || kept+len(raw) > realtimeSeriesBytes {
					return protocol.ErrLimit
				}
				s.times = append(s.times, r.time)
				s.values = append(s.values, raw...)
				kept += len(raw)
			}
		}
		if d.Err() != nil {
			return d.Err()
		}
		if d.Remaining() != 0 {
			return protocol.ErrTrailing
		}
		return nil
	}
	found, err := c.scanRealtimeHolds(ctx, date, fill, deliver, func() { cur.settlePending(start, end, false) })
	if err != nil || !found {
		return nil, err
	}
	return out, nil
}

// counterBody is one realtime data record read under the Counters gate and
// decoded after the hold. names is the table's id dictionary at that hold.
// putRealtime only appends ids (a failed save restores the previous slice
// within the same hold), so names[:len(names)] is never rewritten and may be
// read without the gate.
type counterBody struct {
	time  int64
	hash  int32
	body  []byte
	names []string
}

// counterScanBetweenHolds, when set by a test, runs between two gate holds of
// a realtime counter scan, with the Counters gate free.
var counterScanBetweenHolds func()

// scanRealtimeHolds runs a scan of date's realtime counter table in bounded
// Counters gate holds, as ScanXLogsContext does for XLogs: each hold ends
// after scanHoldSteps steps or scanHoldTime (holdBudget), so PerfCounterPack
// ingest (Counters.PutContext from the ingest writers) waits at most one hold.
//
// fill runs under the gate with the table of that hold, keeps its own resume
// cursor, appends the records it read to batch and reports done at the end of
// the scan. After the gate is released each batch record goes to deliver in
// order, then fill's error, if any, is returned: the first failing record in
// scan order decides the error as in a single hold. between runs without the
// gate after each hold. found is false when date has no realtime table.
//
// Between holds the table may be evicted and reopened (the same file; cursor
// positions stay valid) or purged. A purge ends the scan without an error,
// keeping what was delivered (as a purged XLog scan): date was purged or
// deleted (markPurgedLocked), the table is missing, or its data file is
// another file (os.SameFile) or shorter than at the start.
func (c *Counters) scanRealtimeHolds(ctx context.Context, date string,
	fill func(t *counterTable, budget *holdBudget, batch []counterBody) ([]counterBody, bool, error),
	deliver func(counterBody) error, between func()) (found bool, err error) {
	var ident os.FileInfo
	var last *counterTable
	var gen uint64
	batch := make([]counterBody, 0, scanBatchRecords)
	for {
		if err = c.mu.LockContext(ctx); err != nil {
			return found, err
		}
		if !found {
			gen = c.purged[date]
		} else if c.purged[date] != gen {
			c.mu.Unlock()
			return found, nil // purged since the scan started
		}
		t, err := c.table(date, true, false)
		if err == nil {
			err = ctx.Err()
		}
		if err == nil && t != nil && t != last {
			var st os.FileInfo
			if st, err = t.data.Stat(); err == nil {
				if ident == nil {
					ident = st
				} else if !os.SameFile(st, ident) || st.Size() < ident.Size() {
					t = nil // replaced by a purge since the scan started
				}
			}
		}
		if err != nil || t == nil {
			c.mu.Unlock()
			return found, err
		}
		found, last = true, t
		budget := newHoldBudget()
		var done bool
		batch, done, err = fill(t, &budget, batch[:0])
		c.mu.Unlock()
		for n := range batch {
			if e := deliver(batch[n]); e != nil {
				return true, e
			}
			batch[n] = counterBody{}
		}
		if err != nil || done {
			return true, err
		}
		between()
		if hook := counterScanBetweenHolds; hook != nil {
			hook()
		}
	}
}

// evictTableLocked closes the first table in key order other than keep, data
// fsync before the index header (its Close). During a Sync pass the close is
// deferred to that pass (closeQueue); a table re-requested meanwhile is
// re-adopted instead of reopened.
func (c *Counters) evictTableLocked(keep string) error {
	keys := make([]string, 0, len(c.tables))
	for k := range c.tables {
		if k != keep {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	victim := keys[0]
	old := c.tables[victim]
	delete(c.tables, victim)
	dataOK := false
	return c.closes.release(&pendingClose{
		key:   victim,
		adopt: func() { c.tables[victim] = old },
		close: func() error {
			syncErr := c.closes.syncFile(old.data)
			dataOK = syncErr == nil
			return errors.Join(syncErr, old.index.Close(), old.data.Close())
		},
		// Under c.mu once closed (pendingClose.after): not for a key a purge
		// or DB delete forgot, a table cached again, or a pair now gone.
		after: func(forgotten bool) {
			if !dataOK || forgotten || c.tables[victim] != nil {
				return
			}
			if _, err := os.Stat(old.path + ".kfile"); err != nil {
				return
			}
			cp, ok := old.index.Checkpoint() // established by that close's sync
			c.checkpoints.record(old.path, cp, ok)
		},
	})
}

// Sync fsyncs counter data before index headers outside the Counters gate, so
// counter ingest is not stalled behind fsync (same scheme as Store.SyncData,
// including the deferred close of tables evicted meanwhile).
func (c *Counters) Sync() error {
	c.syncRun.Lock()
	defer c.syncRun.Unlock()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return os.ErrClosed
	}
	var errs []error
	var points []*legacyindex.SyncPoint // nil where BeginSync failed
	var tables []*counterTable
	var keys []string
	for key, t := range c.tables {
		p, err := t.index.BeginSync()
		errs = append(errs, err)
		points, tables, keys = append(points, p), append(tables, t), append(keys, key)
	}
	c.closes.begin()
	c.mu.Unlock()
	dataOK := make([]bool, len(tables))
	for n, t := range tables {
		err := c.closes.syncClosedOK(t.data)
		dataOK[n] = err == nil
		errs = append(errs, err)
	}
	for _, p := range points {
		if p == nil {
			continue
		}
		if err := p.Commit(); err != nil && !errors.Is(err, os.ErrClosed) {
			errs = append(errs, err)
		}
	}
	// Only tables still cached and whose data reached disk (see
	// Store.SyncData).
	c.mu.Lock()
	for n, p := range points {
		if p == nil || !dataOK[n] || c.tables[keys[n]] != tables[n] {
			continue
		}
		if cp, ok := p.Checkpoint(); ok {
			c.checkpoints.set(tables[n].path, cp)
		}
	}
	c.mu.Unlock()
	errs = append(errs, c.closes.drain(&c.mu))
	// After every header fsync above (clean.go, crash checkpoints).
	return errors.Join(append(errs, c.checkpoints.persist())...)
}
func (c *Counters) Close() error {
	// syncRun before mu, as Store.Close: an in-flight Sync finishes (and
	// closes its deferred tables) before the final headers are written.
	c.syncRun.Lock()
	defer c.syncRun.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var err error
	sealed := make(map[string]legacyindex.Seal, len(c.tables))
	for _, t := range c.tables {
		syncErr := c.closes.syncFile(t.data)
		var seal legacyindex.Seal
		var closeErr error
		if syncErr == nil {
			seal, closeErr = t.index.CloseSealed() // data fsynced before the sealed header
		} else {
			closeErr = t.index.Close()
		}
		if closeErr == nil && syncErr == nil {
			sealed[t.path] = seal
		}
		if syncErr == nil {
			cp, ok := t.index.Checkpoint()
			c.checkpoints.record(t.path, cp, ok)
		}
		err = errors.Join(err, syncErr, closeErr, t.data.Close())
	}
	err = errors.Join(err, c.closes.closeAll())
	if err == nil {
		c.closedSeals = sealed // published into the Store's marker (clean.go)
	}
	return errors.Join(err, c.checkpoints.persist())
}

// PutDailyContext preserves an explicit Agent hash for server-derived counters.
func (c *Counters) PutDailyContext(ctx context.Context, date string, hash int32, name string, kind byte, minute int, v protocol.Value) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if name == "" || kind == 1 || minute < 0 || minute >= 1440 {
		return protocol.ErrInvalid
	}
	if err := c.mu.LockContext(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	t, err := c.table(date, false, true)
	if err != nil {
		return err
	}
	return t.putDaily(hash, name, kind, minute, v)
}
