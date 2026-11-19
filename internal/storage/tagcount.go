// Copyright Scouter authors, Apache-2.0. First100 and overflow minute aggregation.
package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"sort"
	"strconv"
	"unicode/utf16"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

type TagValueCount struct {
	Value protocol.Value
	Count float64
}
type TagValueTotal struct {
	HowManyValues int
	TotalCount    float32
	Values        []TagValueCount
}
type TagActualQuery struct {
	Date, ObjType, Group string
	From, To, TxID       int64
	Max                  int
	Reverse              bool
	Filter               protocol.Value
}

// tagDelta is the unflushed contribution to one record, kept per hour like
// the original MoveToNextCollector (Array[Float](60) per hour key): a key
// touched in one hour holds 240 bytes, not a 1440-minute day.
type tagDelta struct {
	key   []byte
	id    tagValueKey
	text  string // a Text value (its id.n is the table's text id)
	first bool
	hours [24]*[60]float32
	bytes int // charged to tagCountState.pendingBytes
}

// tagValueKey is the pending-table key of a record (tag and value), so a
// contribution is counted without encoding its record key (tagRecordKey):
// the tag, the Value type and its number (TextHash, Decimal; 0 for Null). A
// Text value's number is the table's id of the text (tagTable.texts), which
// keeps the key fixed-size and hashed in one pass. Contributions make each
// value in one way, so it maps one to one to the encoded key. Text ids are
// never reused in a Store (tagCountState.lastText): a flush pass resolves its
// snapshot by these keys between holds, and a table evicted or purged and made
// again meanwhile must not give another text an id of the snapshot.
type tagValueKey struct {
	tag  int64
	n    int64
	kind byte
}

// tagTextID is the id of a Text value in a table's pending deltas and the
// number of them that use it; it is dropped with the last one.
type tagTextID struct {
	id   int64
	refs int
}

// Pending-bytes charges: one delta and one hour block.
const (
	tagDeltaOverhead = 256
	tagHourBytes     = 60 * 4
)

type tagTable struct {
	first         map[int64]map[string][]byte
	pending       map[tagValueKey]*tagDelta
	texts         map[string]*tagTextID
	metadataBytes int
}

// dropDelta removes delta from the pending table and releases its text id.
func (t *tagTable) dropDelta(delta *tagDelta) {
	delete(t.pending, delta.id)
	if delta.id.kind != protocol.ValueText {
		return
	}
	if ref := t.texts[delta.text]; ref != nil {
		if ref.refs--; ref.refs <= 0 {
			delete(t.texts, delta.text)
		}
	}
}

type tagCountState struct {
	tables        map[string]*tagTable
	pendingBytes  int
	metadataBytes int
	lastText      int64 // the last Text id given out (tagValueKey)
	cachePaths    map[string]bool
	// cacheLimited holds the paths whose top100 build ended at a resource
	// limit (tagRebuildCaches): they are not rebuilt again.
	cacheLimited map[string]bool
}

func formatTagKey(key int64) string { return strconv.FormatInt(key, 10) }
func (s *Store) tagTableLocked(ctx context.Context, path string) (*tagTable, error) {
	if s.tagCounts == nil {
		s.tagCounts = &tagCountState{tables: map[string]*tagTable{}, cachePaths: map[string]bool{}, cacheLimited: map[string]bool{}}
	}
	state := s.tagCounts
	if !state.cachePaths[path] && len(state.cachePaths) >= 1000 {
		return nil, ErrLimit
	}
	if table := state.tables[path]; table != nil {
		return table, nil
	}
	if len(state.tables) >= 30 {
		keys := make([]string, 0, len(state.tables))
		for key := range state.tables {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if err := s.tagFlushTableLocked(ctx, keys[0], state.tables[keys[0]]); err != nil {
			return nil, err
		}
		state.metadataBytes -= state.tables[keys[0]].metadataBytes
		delete(state.tables, keys[0])
	}
	// Only keys are retained, so the scan streams and the metadata bound below
	// is the memory bound, not the file size.
	table := &tagTable{first: map[int64]map[string][]byte{}, pending: map[tagValueKey]*tagDelta{}, texts: map[string]*tagTextID{}}
	err := s.tagScanLocked(ctx, filepath.Join(path, "first"), true, tagScanMaxRecords, func(r tagRecord) error {
		set := table.first[r.tag]
		if set == nil {
			set = map[string][]byte{}
			table.first[r.tag] = set
		}
		set[string(r.key)] = r.key
		table.metadataBytes += 2*len(r.key) + 64
		if state.metadataBytes+table.metadataBytes > 64<<20 {
			return ErrLimit
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	state.metadataBytes += table.metadataBytes
	state.tables[path] = table
	return table, nil
}

// tagAddLocked adds the count of it to its record in table
// (tagTableLocked) at minute. The key is encoded only for a new delta, into
// a reused buffer, and copied for the delta.
func (s *Store) tagAddLocked(ctx context.Context, table *tagTable, it tagItem, minute int) error {
	id, count := it.id, it.count
	var pending *tagDelta
	var text *tagTextID
	if id.kind != protocol.ValueText {
		pending = table.pending[id]
	} else if text = table.texts[it.text]; text != nil {
		id.n = text.id
		pending = table.pending[id]
	}
	var key []byte
	need := 0
	if pending == nil {
		var err error
		if key, err = s.tagScratchKey(id.tag, it.value()); err != nil {
			return err
		}
		need = tagDeltaOverhead + len(key) + tagHourBytes
	} else if pending.hours[minute/60] == nil {
		need = tagHourBytes
	}
	if need > 0 {
		max := s.tagPendingMax()
		if s.tagCounts.pendingBytes+need > max {
			// Backpressure fallback; the periodic flush normally keeps
			// pending far below the bound (it stops yielding past half).
			s.tagInline.Add(1)
			if err := s.tagFlushLocked(ctx); err != nil {
				return err
			}
			pending = nil
			if key == nil { // only an hour was needed: the delta was known
				var err error
				if key, err = s.tagScratchKey(id.tag, it.value()); err != nil {
					return err
				}
			}
			need = tagDeltaOverhead + len(key) + tagHourBytes
			if need > max {
				return ErrLimit
			}
		}
	}
	if pending == nil {
		key = append([]byte(nil), key...) // owned by the delta and the First set
		set := table.first[id.tag]
		if set == nil {
			set = map[string][]byte{}
			table.first[id.tag] = set
		}
		_, first := set[string(key)]
		if !first && len(set) < 100 {
			bytes := 2*len(key) + 64
			if s.tagCounts.metadataBytes+bytes > 64<<20 {
				return ErrLimit
			}
			table.metadataBytes += bytes
			s.tagCounts.metadataBytes += bytes
			first = true
			set[string(key)] = key
		}
		if id.kind == protocol.ValueText {
			// Looked up again: an inline flush above dropped every delta
			// (it returns on any failure), and with them the ids.
			if text = table.texts[it.text]; text == nil {
				s.tagCounts.lastText++
				text = &tagTextID{id: s.tagCounts.lastText}
				table.texts[it.text] = text
			}
			id.n = text.id
			text.refs++ // a new delta: none of this id is pending
		}
		pending = &tagDelta{key: key, id: id, first: first, bytes: tagDeltaOverhead + len(key)}
		if id.kind == protocol.ValueText {
			pending.text = it.text
		}
		table.pending[id] = pending
		s.tagCounts.pendingBytes += pending.bytes
	}
	hour := pending.hours[minute/60]
	if hour == nil {
		hour = new([60]float32)
		pending.hours[minute/60] = hour
		pending.bytes += tagHourBytes
		s.tagCounts.pendingBytes += tagHourBytes
	}
	hour[minute%60] += count
	return nil
}

// tagContribution is one tag of an alert. The value is kept as its type,
// number and text (the only fields the tag values use) instead of a whole
// protocol.Value, which is ~250 bytes. XLogs carry tagXLogFields instead.
type tagContribution struct {
	name, text string
	n          int64
	count      float32
	kind       byte
}

// tagItem is one contribution to apply: its record (id; text for a Text
// value, whose id.n the table assigns) and count.
type tagItem struct {
	id    tagValueKey
	text  string
	count float32
}

func (it tagItem) value() protocol.Value {
	return protocol.Value{Type: it.id.kind, Int: it.id.n, Text: it.text}
}

// tagXLogFields are the XLog fields its tag contributions are made of,
// carried in the inbox entry by value: preparing an XLog's tags allocates
// nothing under the engine lock, and the contributions are made where they
// are applied (items), with the tag keys computed once (tagXLogKeys).
type tagXLogFields struct {
	objHash, service, elapsed, kbytes, sqlTime, sqlCount, apiTime, apiCount int32
	error, group, userAgent, referer, city, login, desc                     int32
	country                                                                 string
}

// The tags of an XLog (XLogTagCount.process), in the order Go counts them.
// The original counts error-sum, error, group, user-agent, referer, city and
// nation before the SQL and API sums; Go keeps its earlier order. Values
// enter a tag's First 100 in arrival order per tag, so the order within an
// XLog only shows when a tag fails in the middle of an entry (the tags before
// it stay counted; TestTagEntryFailureKeepsEarlierTags).
const (
	tagXTotal = iota
	tagXObject
	tagXService
	tagXTimeSum
	tagXKByteSum
	tagXSQLTimeSum
	tagXSQLCountSum
	tagXAPITimeSum
	tagXAPICountSum
	tagXErrorSum
	tagXError
	tagXGroup
	tagXUserAgent
	tagXReferer
	tagXCity
	tagXNation
	tagXLogin
	tagXDesc
	tagXMax // the most one XLog contributes
)

var tagXLogKeys = func() (keys [tagXMax]int64) {
	names := [tagXMax]string{"@total", "object", "service", "service-time-sum", "service-kbyte-sum", "sqltime-sum", "sqlcount-sum", "apitime-sum", "apicount-sum",
		"service-error-sum", "error", "group", "user-agent", "referer", "city", "nation", "login", "desc"}
	for i, name := range names {
		keys[i] = tagKey("service", name)
	}
	return keys
}()

// items appends the contributions of x in counting order.
func (x *tagXLogFields) items(out []tagItem) []tagItem {
	add := func(tag int, kind byte, n int32, count float32) {
		out = append(out, tagItem{id: tagValueKey{tag: tagXLogKeys[tag], kind: kind, n: int64(n)}, count: count})
	}
	out = append(out, tagItem{id: tagValueKey{tag: tagXLogKeys[tagXTotal], kind: protocol.ValueNull}, count: 1})
	add(tagXObject, protocol.ValueTextHash, x.objHash, 1)
	add(tagXService, protocol.ValueTextHash, x.service, 1)
	for _, f := range [...]struct {
		tag int
		n   int32
	}{{tagXTimeSum, x.elapsed}, {tagXKByteSum, x.kbytes}, {tagXSQLTimeSum, x.sqlTime}, {tagXSQLCountSum, x.sqlCount}, {tagXAPITimeSum, x.apiTime}, {tagXAPICountSum, x.apiCount}} {
		if f.n > 0 {
			add(f.tag, protocol.ValueTextHash, x.service, float32(f.n))
		}
	}
	if x.error != 0 {
		add(tagXErrorSum, protocol.ValueTextHash, x.service, 1)
		add(tagXError, protocol.ValueTextHash, x.error, 1)
	}
	for _, f := range [...]struct {
		tag int
		n   int32
	}{{tagXGroup, x.group}, {tagXUserAgent, x.userAgent}, {tagXReferer, x.referer}, {tagXCity, x.city}} {
		if f.n != 0 {
			add(f.tag, protocol.ValueTextHash, f.n, 1)
		}
	}
	if x.country != "" {
		out = append(out, tagItem{id: tagValueKey{tag: tagXLogKeys[tagXNation], kind: protocol.ValueText}, text: x.country, count: 1})
	}
	if x.login != 0 {
		add(tagXLogin, protocol.ValueDecimal, x.login, 1)
	}
	if x.desc != 0 {
		add(tagXDesc, protocol.ValueDecimal, x.desc, 1)
	}
	return out
}

// tagScratchKey is tagRecordKey into the tag state's reused encoder (the
// caller holds the tag gate); the result is valid until the next call.
func (s *Store) tagScratchKey(tag int64, value protocol.Value) ([]byte, error) {
	e := s.tags.keyEnc
	if e == nil {
		e = protocol.NewEncoder(s.config.CodecLimits)
		s.tags.keyEnc = e
	} else {
		e.Reset()
	}
	e.Int64(tag)
	e.Value(value)
	b, err := e.Bytes()
	if err != nil {
		s.tags.keyEnc = nil
		return nil, err
	}
	if len(b) > 32767 {
		s.tags.keyEnc = nil // do not keep the buffer an oversized key grew
		return nil, ErrLimit
	}
	return b, nil
}
func (s *Store) PutXLogTagsContext(ctx context.Context, objType string, p *protocol.XLogPack) error {
	w, err := s.PrepareXLogTags(objType, p)
	if err != nil {
		return err
	}
	return s.submitTags(ctx, w)
}

// PrepareXLogTags validates the tag contributions of PutXLogTagsContext and
// returns them as a Write for Offer/Submit (ingestwrite.go).
func (s *Store) PrepareXLogTags(objType string, p *protocol.XLogPack) (Write, error) {
	if p == nil {
		return Write{}, ErrInvalid
	}
	w, err := s.prepareTags(p.EndTime, objType, "service", nil)
	if err != nil {
		return Write{}, err
	}
	w.tag.xlog = true
	w.tag.x = tagXLogFields{
		objHash: p.ObjHash, service: p.Service, elapsed: p.Elapsed, kbytes: p.KBytes, sqlTime: p.SQLTime, sqlCount: p.SQLCount,
		apiTime: p.APICallTime, apiCount: p.APICallCount, error: p.Error, group: p.Group, userAgent: p.UserAgent, referer: p.Referer,
		city: p.City, login: p.Login, desc: p.Desc, country: p.CountryCode,
	}
	return w, nil
}
func (s *Store) PutAlertTagsContext(ctx context.Context, objType string, p *protocol.AlertPack) error {
	w, err := s.PrepareAlertTags(objType, p)
	if err != nil {
		return err
	}
	return s.submitTags(ctx, w)
}

// PrepareAlertTags is PrepareXLogTags for PutAlertTagsContext.
func (s *Store) PrepareAlertTags(objType string, p *protocol.AlertPack) (Write, error) {
	if p == nil {
		return Write{}, ErrInvalid
	}
	return s.prepareTags(p.Time, objType, "alert", []tagContribution{
		{name: "@total", kind: protocol.ValueNull, count: 1}, {name: "object", kind: protocol.ValueTextHash, n: int64(p.ObjHash), count: 1},
		{name: "level", kind: protocol.ValueDecimal, n: int64(p.Level), count: 1},
		{name: "title", kind: protocol.ValueText, text: p.Title, count: 1}})
}

// submitTags is the ctx check and the waiting offer of the Put*TagsContext
// calls.
func (s *Store) submitTags(ctx context.Context, w Write) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Submit(ctx, w)
}
func (s *Store) prepareTags(tm int64, objType, group string, tags []tagContribution) (Write, error) {
	if tm <= 0 {
		return Write{}, ErrInvalid
	}
	if err := aggregationName(objType); err != nil {
		return Write{}, err
	}
	date := JavaDate(tm, s.config.Location)
	if err := validDate(date); err != nil {
		return Write{}, err
	}
	return Write{tag: tagInboxEntry{path: s.tagTablePath(date, objType), group: group, minute: int(JavaMillis(tm, s.config.Location) / 60000), tags: tags, gen: s.tagPurgeGen()}, queue: TagQueue}, nil
}

// tagTablePath is {Directory}/{date}/tagcnt/{objType}, kept for the few
// date and object type pairs a day receives (at most tagPathMax, then the
// cache starts over). pathMu is a leaf lock.
func (s *Store) tagTablePath(date, objType string) string {
	t := &s.tags
	key := [2]string{date, objType}
	t.pathMu.Lock()
	defer t.pathMu.Unlock()
	if p, ok := t.paths[key]; ok {
		return p
	}
	if t.paths == nil || len(t.paths) >= tagPathMax {
		t.paths = map[[2]string]string{}
	}
	p := filepath.Join(s.config.Directory, date, "tagcnt", objType)
	t.paths[key] = p
	return p
}

func (s *Store) tagFlushTableLocked(ctx context.Context, path string, table *tagTable) error {
	deltas := make([]*tagDelta, 0, len(table.pending))
	for _, delta := range table.pending {
		deltas = append(deltas, delta)
	}
	sort.Slice(deltas, func(i, j int) bool { return bytes.Compare(deltas[i].key, deltas[j].key) < 0 })
	var failed error
	for _, first := range []bool{true, false} {
		for _, delta := range deltas {
			if delta.first != first {
				continue
			}
			// A failed record does not stop the others (Close flushes
			// everything it can); the first error is returned.
			if err := s.tagFlushRecordLocked(ctx, path, table, delta); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				failed = cmpFirst(failed, err)
			}
		}
	}
	return errors.Join(failed, ctx.Err())
}

func cmpFirst(first, err error) error {
	if first != nil {
		return first
	}
	return err
}

// tagFlushRecordLocked applies one pending delta to its First or Next record
// (find, add, save) and then drops it from pending. The whole read-modify-write
// runs in one Store gate hold, so a query or another flush sees the record
// either before or after it, never half applied. On error the delta stays
// pending and is retried by the next flush.
func (s *Store) tagFlushRecordLocked(ctx context.Context, path string, table *tagTable, delta *tagDelta) error {
	first := delta.first
	base := filepath.Join(path, "next")
	if first {
		base = filepath.Join(path, "first")
	}
	var wrote bool
	var err error
	if s.tagFullRewrite {
		wrote, err = s.tagRewriteRecordLocked(ctx, base, first, delta)
	} else {
		var r tagRecord
		var ok bool
		if r, ok, err = s.tagLocateRecordLocked(ctx, base, first, delta.key); err != nil {
			return err
		}
		if ok {
			wrote, err = s.tagUpdateRecordLocked(base, first, r, delta)
		} else {
			wrote, err = s.tagSaveRecord(ctx, base, first, s.tagNewRecord(delta))
		}
	}
	if err != nil && !wrote {
		return err // nothing counted yet: the delta stays pending and is retried
	}
	if err != nil {
		// Some counts reached the file: a retry would add them twice, so the
		// delta is dropped and counted, as the original logs a failed
		// record and goes on (S204).
		s.tagDropped.Add(1)
	}
	table.dropDelta(delta)
	s.tagCounts.pendingBytes -= delta.bytes
	return err
}

// tagNewRecord is the record a delta of a key without one starts.
// tagNewRecord builds the record of a key not yet on file. Its counts use
// the tag gate's buffer (the caller holds the gate and saves the record at
// once, tagSaveRecord), so a storm of new tag values does not allocate a
// day of counts per record.
func (s *Store) tagNewRecord(delta *tagDelta) tagRecord {
	d := protocol.NewDecoder(delta.key, s.config.CodecLimits)
	counts := s.tags.newCounts
	if len(counts) != 1440 {
		counts = make([]float32, 1440)
		s.tags.newCounts = counts
	} else {
		clear(counts)
	}
	r := tagRecord{key: delta.key, tag: d.Int64(), value: d.Value(), counts: counts, next: !delta.first}
	for hour, counts := range delta.hours {
		if counts == nil {
			continue
		}
		copy(r.counts[hour*60:], counts[:])
		if !delta.first {
			r.totalCount += tagSum(counts[:])
		}
	}
	return r
}

// tagRewriteRecordLocked is the whole-record flush (find, add, rewrite),
// kept as the reference the in-place update is tested against.
func (s *Store) tagRewriteRecordLocked(ctx context.Context, base string, first bool, delta *tagDelta) (bool, error) {
	r, ok, err := s.tagFindRecordLocked(ctx, base, first, delta.key)
	if err != nil {
		return false, err
	}
	if !ok {
		return s.tagSaveRecord(ctx, base, first, s.tagNewRecord(delta))
	}
	for hour, counts := range delta.hours {
		if counts == nil {
			continue
		}
		for m, n := range counts {
			r.counts[hour*60+m] += n
		}
		if !first {
			r.totalCount += tagSum(counts[:])
		}
	}
	return s.tagSaveRecord(ctx, base, first, r)
}
func (s *Store) tagFlushLocked(ctx context.Context) error {
	if s.tagCounts == nil {
		return ctx.Err()
	}
	keys := make([]string, 0, len(s.tagCounts.tables))
	for key := range s.tagCounts.tables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var failed error
	for _, key := range keys {
		if err := s.tagFlushTableLocked(ctx, key, s.tagCounts.tables[key]); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			failed = cmpFirst(failed, err)
		}
	}
	return errors.Join(failed, ctx.Err())
}

// tagTickLocked runs on the engine tick (TagTickContext, after the ingest
// engine lock is released). It only marks the 5s record flush and the 5-minute top100 rebuild as
// due; FlushTagCountsIfDue does that I/O from the runtime tag goroutine,
// as the original XLogTagCount/MoveToNextCollector/Top100FileCache threads do
// it off the collection thread.
func (s *Store) tagTickLocked(ctx context.Context, now int64) error {
	t := &s.tags
	t.inboxMu.Lock()
	defer t.inboxMu.Unlock()
	if t.lastFlush == 0 {
		t.lastFlush = now
		t.lastCache = now
	}
	if now-t.lastFlush >= 5000 {
		t.flushDue = true
		t.lastFlush = now
	}
	if now-t.lastCache >= 300000 {
		t.cacheDue = true
		t.lastCache = now
	}
	return ctx.Err()
}

// tagQueryPath runs under the tag gate.
func (s *Store) tagQueryPath(date, objType string) (string, error) {
	if s.tags.closed {
		return "", ErrClosed
	}
	if err := validDate(date); err != nil {
		return "", err
	}
	if err := aggregationName(objType); err != nil {
		return "", err
	}
	return filepath.Join(s.config.Directory, date, "tagcnt", objType), nil
}
func (s *Store) TagValuesContext(ctx context.Context, date, objType, group, name string) (TagValueTotal, error) {
	if err := s.tags.mu.LockContext(ctx); err != nil {
		return TagValueTotal{}, err
	}
	path, err := s.tagQueryPath(date, objType)
	if err == nil {
		// Queries see what ingest already handed over (Java reads its files
		// only). Not cancelled by the request: an entry is applied whole or
		// not at all.
		err = s.tagDrainAllLocked(context.Background())
	}
	tag := tagKey(group, name)
	if err == nil {
		var cached TagValueTotal
		var ok bool
		if cached, ok, err = s.tagReadCacheLocked(ctx, tagCachePath(path, tag)); err == nil && ok {
			s.tags.mu.Unlock()
			return cached, nil
		}
	}
	s.tags.mu.Unlock()
	if err != nil {
		return TagValueTotal{}, err
	}
	// Stream First in tag gate holds and keep only the requested tag's
	// values and sums.
	var selected []tagValueSum
	known := map[string]bool{}
	err = s.tagScanInHolds(ctx, filepath.Join(path, "first"), true, func(r tagRecord) error {
		if r.tag != tag {
			return nil
		}
		if len(selected) >= s.config.MaxQueryRecords {
			return ErrLimit
		}
		selected = append(selected, tagValueSum{r.value, tagSum(r.counts)})
		known[string(r.key)] = true
		return nil
	})
	if errors.Is(err, errTagPassPurged) {
		return tagTotal(nil, 100), ctx.Err()
	}
	if err != nil {
		return TagValueTotal{}, err
	}
	if err = s.tags.mu.LockContext(ctx); err != nil {
		return TagValueTotal{}, err
	}
	defer s.tags.mu.Unlock()
	if s.tagCounts != nil {
		if t := s.tagCounts.tables[path]; t != nil {
			for id, key := range t.first[tag] {
				if !known[id] {
					d := protocol.NewDecoder(key, s.config.CodecLimits)
					d.Int64()
					selected = append(selected, tagValueSum{value: d.Value()})
				}
			}
		}
	}
	return tagTotal(selected, 100), ctx.Err()
}

// tagValueSum is one First Value and the float32 sum of its minute buckets.
type tagValueSum struct {
	value protocol.Value
	count float32
}

func javaTagValueHash(v protocol.Value) uint32 {
	switch v.Type {
	case protocol.ValueDecimal:
		return uint32(uint64(v.Int) ^ uint64(v.Int)>>32)
	case protocol.ValueTextHash:
		return uint32(v.Int)
	case protocol.ValueText:
		var h uint32
		for _, c := range utf16.Encode([]rune(v.Text)) {
			h = h*31 + uint32(c)
		}
		return h
	}
	return 0
}
func tagTotal(values []tagValueSum, limit int) TagValueTotal {
	javaHashBucketOrder(values, func(v tagValueSum) uint32 { return javaTagValueHash(v.value) })
	out := TagValueTotal{HowManyValues: len(values)}
	for _, v := range values {
		out.TotalCount += v.count
		out.Values = append(out.Values, TagValueCount{v.value, float64(v.count)})
	}
	sort.SliceStable(out.Values, func(i, j int) bool { return int32(out.Values[j].Count-out.Values[i].Count) < 0 })
	if limit > 0 && len(out.Values) > limit {
		out.Values = out.Values[:limit]
	}
	return out
}

// javaHashBucketOrder reorders items as a Java HashMap/HashSet of len(items)
// entries iterates: by spread-hash bucket; stable collision order follows file
// insertion.
func javaHashBucketOrder[T any](items []T, hash func(T) uint32) {
	capacity := 16
	for len(items) > capacity*3/4 {
		capacity *= 2
	}
	sort.SliceStable(items, func(i, j int) bool {
		a, b := hash(items[i]), hash(items[j])
		return (a^(a>>16))&uint32(capacity-1) < (b^(b>>16))&uint32(capacity-1)
	})
}

// tagTopLimit is the makeTop100 limit (Top100FileCache.scala:65).
const tagTopLimit = 100

// tagTopEntry is one TopN ValueCount. key is the persisted record key (tag +
// Value wire); the Value is decoded only when the cache is written, so the
// retained memory is the key bytes.
type tagTopEntry struct {
	key   []byte
	count float64
}

// tagTopN reproduces scouter.util.TopN(limit, DESC) (TopN.java:44-108) with
// ValueCount.compareTo (ValueCount.java:39-44): binary insertion into an
// ascending list, equal-count insertion at mid, and removal of the first
// (smallest) item once the list exceeds limit. Equal-count placement is
// observable in persisted cache ordering.
type tagTopN struct {
	limit     int
	ascending []tagTopEntry
}

// add inserts item and returns the entry evicted by the limit, if any.
func (t *tagTopN) add(item tagTopEntry) (tagTopEntry, bool) {
	at := 0
	if len(t.ascending) > 0 {
		low, high := 0, len(t.ascending)-1
		for {
			mid := (high + low) / 2
			c := t.ascending[mid].count
			if c == item.count {
				at = mid
				break
			}
			if c < item.count {
				low = mid
			} else {
				high = mid
			}
			if high == low {
				at = high
				if t.ascending[high].count <= item.count {
					at++
				}
				break
			}
			if high-low == 1 {
				if t.ascending[low].count > item.count {
					at = low
				} else if t.ascending[high].count < item.count {
					at = high + 1
				} else {
					at = high
				}
				break
			}
		}
	}
	t.ascending = append(t.ascending, tagTopEntry{})
	copy(t.ascending[at+1:], t.ascending[at:])
	t.ascending[at] = item
	if len(t.ascending) <= t.limit {
		return tagTopEntry{}, false
	}
	evicted := t.ascending[0]
	copy(t.ascending, t.ascending[1:])
	t.ascending[len(t.ascending)-1] = tagTopEntry{}
	t.ascending = t.ascending[:len(t.ascending)-1]
	return evicted, true
}

// tagTopItem is Top100FileCache.TopItem (Top100FileCache.scala:113-117).
type tagTopItem struct {
	kinds int
	sum   float32
	top   tagTopN
}

func (t *tagTopItem) add(key []byte, count float32) (tagTopEntry, bool) {
	t.sum += count
	t.kinds++
	return t.top.add(tagTopEntry{key, float64(count)})
}

// tagFirstValue is one First Value retained for the merge step.
type tagFirstValue struct {
	key   []byte
	hash  uint32
	count float32
}

// Retained-memory accounting of one top100 build. Costs are estimates of the
// Go heap held per retained key and per distinct tag.
const (
	tagCacheMaxRetained = 64 << 20
	tagRetainedOverhead = 64
	tagTagOverhead      = 256
)

// tagTop100Builder accumulates makeTop100 (Top100FileCache.scala:125-150):
// First records in file order (addFirst), then Next records in file order
// (addNext) feeding sumOfValue/kindsOfValue/TopN; finish adds each tag's First
// Values in Java HashSet order with sums of their 1440 buckets
// (FirstTagCountDB.scala:200-222). Tags with First values only get no cache,
// as in the original. Memory is the TopN keys, the First keys and one record;
// retained bytes above maxRetained return ErrLimit.
type tagTop100Builder struct {
	maxRetained, retained int
	first                 map[int64][]tagFirstValue
	items                 map[int64]*tagTopItem
}

func newTagTop100Builder(maxRetained int) *tagTop100Builder {
	return &tagTop100Builder{maxRetained: maxRetained, first: map[int64][]tagFirstValue{}, items: map[int64]*tagTopItem{}}
}
func (b *tagTop100Builder) charge(n int) error {
	b.retained += n
	if b.retained > b.maxRetained {
		return ErrLimit
	}
	return nil
}
func (b *tagTop100Builder) addFirst(r tagRecord) error {
	cost := len(r.key) + tagRetainedOverhead
	if _, ok := b.first[r.tag]; !ok {
		cost += tagTagOverhead
	}
	if err := b.charge(cost); err != nil {
		return err
	}
	b.first[r.tag] = append(b.first[r.tag], tagFirstValue{r.key, javaTagValueHash(r.value), tagSum(r.counts)})
	return nil
}
func (b *tagTop100Builder) addNext(r tagRecord) error {
	t := b.items[r.tag]
	if t == nil {
		if err := b.charge(tagTagOverhead); err != nil {
			return err
		}
		t = &tagTopItem{top: tagTopN{limit: tagTopLimit}}
		b.items[r.tag] = t
	}
	if err := b.charge(len(r.key) + tagRetainedOverhead); err != nil {
		return err
	}
	if evicted, ok := t.add(r.key, r.totalCount); ok {
		b.retained -= len(evicted.key) + tagRetainedOverhead
	}
	return nil
}
func (b *tagTop100Builder) finish() map[int64]*tagTopItem {
	for tag, t := range b.items {
		values := b.first[tag]
		javaHashBucketOrder(values, func(v tagFirstValue) uint32 { return v.hash })
		for _, v := range values {
			t.add(v.key, v.count)
		}
		delete(b.first, tag)
	}
	return b.items
}

// tagEncodeTop100 encodes ValueCountTotal: kinds, total, n and the TopN list
// in descending order (TopN.getList).
func tagEncodeTop100(t *tagTopItem, limits protocol.Limits) ([]byte, error) {
	e := protocol.NewEncoder(limits)
	e.Int32(int32(t.kinds))
	e.Float32(t.sum)
	e.Int32(int32(len(t.top.ascending)))
	for i := len(t.top.ascending) - 1; i >= 0; i-- {
		item := t.top.ascending[i]
		d := protocol.NewDecoder(item.key, limits)
		d.Int64()
		v := d.Value()
		if d.Err() != nil || d.Remaining() != 0 {
			return nil, ErrCorrupt
		}
		e.Value(v)
		e.Float64(item.count)
	}
	return e.Bytes()
}
func (s *Store) TagValueSeriesContext(ctx context.Context, date, objType, group, name string, value protocol.Value) ([]float32, error) {
	if err := s.tags.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	path, err := s.tagQueryPath(date, objType)
	if err == nil {
		// Queries see what ingest already handed over (Java reads its files
		// only). Not cancelled by the request: an entry is applied whole or
		// not at all.
		err = s.tagDrainAllLocked(context.Background())
	}
	s.tags.mu.Unlock()
	if err != nil {
		return nil, err
	}
	key, err := tagRecordKey(tagKey(group, name), value, s.config.CodecLimits)
	if err != nil {
		return nil, err
	}
	// The first matching record in file order, First before Next; the scan
	// stops there and holds one record at a time.
	for _, first := range []bool{true, false} {
		base := "next"
		if first {
			base = "first"
		}
		var found []float32
		err = s.tagScanInHolds(ctx, filepath.Join(path, base), first, func(r tagRecord) error {
			if string(r.key) == string(key) {
				found = r.counts
				return errLookupDone
			}
			return nil
		})
		if errors.Is(err, errLookupDone) {
			return found, ctx.Err()
		}
		if errors.Is(err, errTagPassPurged) {
			return nil, ctx.Err()
		}
		if err != nil {
			return nil, err
		}
	}
	return nil, ctx.Err()
}
func (s *Store) MakeTagTop100Context(ctx context.Context, date, objType string) error {
	if err := s.tags.mu.LockContext(ctx); err != nil {
		return err
	}
	path, err := s.tagQueryPath(date, objType)
	if err == nil {
		// Applied whole, as in the other queries.
		err = s.tagDrainAllLocked(context.Background())
	}
	s.tags.mu.Unlock()
	if err != nil {
		return err
	}
	// Built in tag gate holds like the periodic rebuild; a Close or purge
	// between holds ends it quietly (Close flushed, the files are gone).
	err = s.tagMakeCacheHolds(ctx, path)
	if errors.Is(err, errTagPassClosed) || errors.Is(err, errTagPassPurged) {
		return ctx.Err()
	}
	return err
}

// tagWriteTop100Locked replaces the top100.{tag}.dat file of every built tag,
// in tag order.
func (s *Store) tagWriteTop100Locked(ctx context.Context, path string, items map[int64]*tagTopItem) error {
	tags := make([]int64, 0, len(items))
	for tag := range items {
		tags = append(tags, tag)
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	for _, tag := range tags {
		b, err := tagEncodeTop100(items[tag], s.config.CodecLimits)
		if err != nil {
			return err
		}
		if len(b) > s.config.MaxQueryBytes {
			return ErrLimit
		}
		path := tagCachePath(path, tag)
		if err = s.tagWriteAt(ctx, path, b, 0); err != nil {
			return err
		}
		f, err := s.tagFile(path, false)
		if err != nil {
			return err
		}
		if err = f.Truncate(int64(len(b))); err != nil {
			delete(s.tags.ends, path)
			return err
		}
		if s.tags.ends == nil {
			s.tags.ends = make(map[string]int64)
		}
		s.tags.ends[path] = int64(len(b)) // tagWriteAt keeps a larger old size
		s.tags.mark(&s.tags.dirty, path)
	}
	return ctx.Err()
}
func filterNumber(v protocol.Value, n int64) bool {
	return (v.Type == protocol.ValueTextHash || v.Type == protocol.ValueDecimal) && v.Int == n
}
func TagServiceFilterOK(filter protocol.Value, x *protocol.XLogPack) bool {
	if filter.Type == protocol.ValueNull {
		return true
	}
	if filter.Type != protocol.ValueMap {
		return false
	}
	for _, e := range filter.Entries {
		if e.Value.Type != protocol.ValueList {
			continue
		}
		for _, v := range e.Value.Values {
			var n int64
			numeric := true
			switch e.Key {
			case "error":
				n = int64(x.Error)
			case "user-agent":
				n = int64(x.UserAgent)
			case "object":
				n = int64(x.ObjHash)
			case "city":
				n = int64(x.City)
			case "referer":
				n = int64(x.Referer)
			case "group":
				n = int64(x.Group)
			case "service", "service-time-sum", "service-kbyte-sum", "service-error-sum", "sqlcount-sum", "sqltime-sum", "apicount-sum", "apitime-sum":
				n = int64(x.Service)
			case "nation":
				if v.Type == protocol.ValueText && v.Text == x.CountryCode {
					return true
				}
				numeric = false
			default:
				numeric = false
			}
			if numeric && filterNumber(v, n) {
				return true
			}
		}
	}
	return false
}
func TagAlertFilterOK(filter protocol.Value, x *protocol.AlertPack) bool {
	if filter.Type == protocol.ValueNull {
		return true
	}
	if filter.Type != protocol.ValueMap {
		return false
	}
	for _, e := range filter.Entries {
		if e.Value.Type != protocol.ValueList {
			continue
		}
		for _, v := range e.Value.Values {
			switch e.Key {
			case "object":
				if filterNumber(v, int64(x.ObjHash)) {
					return true
				}
			case "level":
				if v.Type == protocol.ValueDecimal && v.Int == int64(x.Level) {
					return true
				}
			case "title":
				if v.Type == protocol.ValueText && v.Text == x.Title {
					return true
				}
			}
		}
	}
	return false
}

// TagActualContext lists the XLogs (service) or alerts behind a tag count, in
// the ScanRange order of the date's timed index, as encoded on disk. The scan
// runs in bounded Store gate holds like ScanXLogsContext (rangeCursor,
// scanHoldSteps/scanHoldTime), so a long range does not stop ingest; the
// results, their order, the scan budget (extraQueryBudget) and the errors are
// those of a single hold. A purge between holds ends the scan with what it
// found.
func (s *Store) TagActualContext(ctx context.Context, q TagActualQuery) ([][]byte, error) {
	hashes, ok, err := s.tagActualObjects(ctx, q)
	if err != nil || !ok {
		return nil, err
	}
	a := &tagActualScan{q: q, hashes: hashes, ready: q.TxID == 0}
	defer a.fetch.drop()
	a.maxRecords, a.maxBytes = s.extraQueryBudget()
	for {
		done, err := s.tagActualHold(ctx, a)
		if err != nil {
			return nil, err
		}
		if done {
			return a.out, ctx.Err()
		}
		if m := a.miss; m != nil {
			a.miss = nil
			if err := s.fetchZip(ctx, &a.fetch, m); err != nil {
				return nil, err
			}
		}
		a.cur.settlePending(q.From, q.To, q.Reverse)
	}
}

// tagActualObjects validates q and returns the objHashes of its objType from
// the daily ObjectPack registrations (ok=false: a group with no actual list).
func (s *Store) tagActualObjects(ctx context.Context, q TagActualQuery) (map[int32]bool, bool, error) {
	unlock, err := s.lockScan(ctx)
	if err != nil {
		return nil, false, err
	}
	defer unlock()
	if err := s.extraQueryRange(q.Date, q.From, q.To); err != nil {
		return nil, false, err
	}
	if q.Group != "service" && q.Group != "alert" {
		return nil, false, nil
	}
	if err := aggregationName(q.ObjType); err != nil {
		return nil, false, err
	}
	// Daily ObjectPack registrations constrain the exact original objType.
	objects, err := s.objectsLockedForTags(ctx, q.Date)
	if err != nil {
		return nil, false, err
	}
	hashes := map[int32]bool{}
	for _, object := range objects {
		if object.ObjType == q.ObjType {
			hashes[object.ObjHash] = true
		}
	}
	return hashes, true, nil
}

// tagActualScan is the state of a TagActualContext scan across holds.
type tagActualScan struct {
	q                    TagActualQuery
	hashes               map[int32]bool
	cur                  rangeCursor
	maxRecords, maxBytes int
	used, scanned        int
	ready                bool
	out                  [][]byte
	// miss ends a hold at an XLog whose compressed block is not cached; it is
	// decoded into fetch outside the gate and read again (zip.go).
	miss  *zipMiss
	fetch zipFetch
}

// Implemented with the existing timed index/data APIs; no parallel history store.
func (s *Store) tagActualHold(ctx context.Context, a *tagActualScan) (bool, error) {
	unlock, err := s.lockScan(ctx)
	if err != nil {
		return false, err
	}
	defer unlock()
	q := &a.q
	if err := s.check(q.Date); err != nil {
		return false, err
	}
	spec := alertRecordSpec
	if q.Group == "service" {
		spec = timedPackSpec{"xlog", "xlog", ".xlog", "_tim", 4, protocol.PackXLog}
	}
	path := s.path(q.Date, spec.dir, spec.base)
	idx, err := s.index(path+spec.index, true, false)
	if err != nil {
		return false, err
	}
	if idx == nil { // no index, or purged since the last hold
		return true, nil
	}
	budget := newHoldBudget()
	for !budget.exhausted() {
		tm, value, ok, end, err := a.cur.nextRecord(idx, q.From, q.To, q.Reverse, &budget)
		if err != nil {
			return false, err
		}
		if !ok {
			return end, nil
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		budget.spend()
		if a.scanned >= a.maxRecords {
			return false, ErrLimit
		}
		a.scanned++
		pos, err := dataOffset(value)
		if err != nil {
			return false, err
		}
		var b []byte
		var p protocol.Pack
		if q.Group == "service" {
			b, err = s.xlogBytesFetch(ctx, q.Date, pos, &a.fetch)
			if errors.As(err, &a.miss) {
				// Read this record again after the decode.
				a.cur.next--
				a.scanned--
				return false, nil
			}
			if err != nil {
				return false, err
			}
			a.used += len(b)
			p, err = protocol.DecodePack(b, s.config.CodecLimits)
			if err != nil {
				return false, err
			}
		} else {
			b, err = s.extraReadData(ctx, path+spec.extension, pos, spec.prefix, a.maxBytes-a.used)
			if err != nil {
				return false, err
			}
			a.used += len(b)
			p, err = protocol.DecodePack(b, s.config.CodecLimits)
			if err != nil {
				return false, err
			}
		}
		if a.used > a.maxBytes {
			return false, ErrLimit
		}
		if q.Group == "service" {
			x, ok := p.(*protocol.XLogPack)
			if !ok {
				return false, ErrCorrupt
			}
			if x.EndTime != tm {
				return false, ErrCorrupt
			}
			if a.hashes[x.ObjHash] {
				if !a.ready {
					a.ready = x.TxID == q.TxID
				} else if TagServiceFilterOK(q.Filter, x) {
					a.out = append(a.out, append([]byte(nil), b...))
				}
			}
		} else {
			x, ok := p.(*protocol.AlertPack)
			if !ok {
				return false, ErrCorrupt
			}
			if a.hashes[x.ObjHash] && TagAlertFilterOK(q.Filter, x) {
				a.out = append(a.out, b)
			}
		}
		if len(a.out) >= q.Max {
			return true, nil
		}
	}
	return false, nil
}
func (s *Store) objectsLockedForTags(ctx context.Context, date string) ([]*protocol.ObjectPack, error) {
	path := s.path(date, "object", "obj")
	idx, err := s.index(path, false, false)
	if err != nil || idx == nil {
		return nil, err
	}
	var out []*protocol.ObjectPack
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
		b, err := s.extraReadData(ctx, path+".data", pos, 4, s.config.MaxQueryBytes-used)
		if err != nil {
			return err
		}
		used += len(b)
		d := protocol.NewDecoder(b, s.config.CodecLimits)
		p, ok := d.Pack().(*protocol.ObjectPack)
		if !ok || d.Err() != nil {
			return ErrCorrupt
		}
		if len(r.Key) != 4 || p.ObjHash != int32(binary.BigEndian.Uint32(r.Key)) {
			return ErrCorrupt
		}
		out = append(out, p)
		return nil
	})
	return out, err
}
