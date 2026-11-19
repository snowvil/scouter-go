package storage

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

// dailyText is !TextPermWR.isA(div): error is always daily, service/apicall/ua
// follow the live mgr_text_db_daily_*_enabled values, every other div
// (sql, table, city, login, desc, ...) is permanent. Like Java it is evaluated
// for every write and every read.
func (s *Store) dailyText(div string) bool {
	switch div {
	case "error":
		return true
	case "service":
		return s.live.textDailyService.Load()
	case "apicall":
		return s.live.textDailyAPI.Load()
	case "ua":
		return s.live.textDailyUA.Load()
	}
	return false
}
func textDivValid(div string) bool {
	return div != "" && len(div) <= maxTextDivBytes && div != "." && div != ".." && !strings.ContainsAny(div, "/\\\x00") && utf8.ValidString(div)
}

// maxTextDivBytes keeps text_<div>.hfile, the longest name of a permanent
// text dictionary, within the 255-byte file name limit of common file
// systems (Java fails to open such a file and loses the text).
const maxTextDivBytes = 255 - len("text_") - len(".hfile")

// knownTextDivs are the divs the original writes: scouter.lang.TextTypes
// (the Java/Host Agents, the server's object/group/table/city texts and the
// Client lookups). Their permanent indexes are created without a bound, as
// in Java.
var knownTextDivs = map[string]bool{
	"error": true, "apicall": true, "method": true, "service": true, "sql": true,
	"object": true, "referer": true, "ua": true, "group": true, "city": true,
	"table": true, "maria": true, "login": true, "desc": true, "web": true,
	"hmsg": true, "stackelem": true,
}

// DefaultMaxUnknownTextDivs bounds the divs outside knownTextDivs that have
// a permanent text index (TextPermWR.isA is true for every unknown div, so
// each one is a new 00000000/text/text_<div> index of
// _mgr_text_db_index_default_mb). It is almost four times the 17 divs of the
// original, room for the custom divs of third-party Agents. Java has no
// bound (intended difference, docs/compatibility/storage-contract.md).
const DefaultMaxUnknownTextDivs = 64

// ErrTextDivLimit refuses a text of an unknown div that has no permanent
// index when DefaultMaxUnknownTextDivs (SetMaxUnknownTextDivs) unknown divs
// already have one. It is an ErrLimit: the TextPack is rejected, not a
// storage error.
var ErrTextDivLimit = fmt.Errorf("new permanent text div over the unknown-div limit: %w", ErrLimit)

// textDivs admits the unknown divs of permanent texts (admitTextDiv). Its
// mutex is taken under the ingest engine lock but never with Store.mu; it
// is taken before textQueue.mu (rescan reads pending).
type textDivs struct {
	mu sync.Mutex
	// unknown are the unknown divs counted against the bound: the ones with
	// a permanent file or a queued text at the last scan, and the ones
	// admitted since (with their admission time). nil until a scan succeeds.
	unknown map[string]time.Time
	// scanned is the time of the last scan; scanErr its error, returned
	// without reading the directory again until textDivRescan has passed.
	scanned time.Time
	scanErr error
	max     int // 0: DefaultMaxUnknownTextDivs, < 0: no bound
	limited atomic.Uint64
	clock   func() time.Time // time.Now; tests replace it
}

// textDivRescan is the shortest time between two reads of the permanent
// text directory: after a failed read, and when the bound is reached (a div
// admitted but never written, or whose files were deleted, frees its place
// at the next rescan).
const textDivRescan = 10 * time.Second

// SetMaxUnknownTextDivs sets the bound of admitTextDiv (go_text_max_unknown_divs):
// 0 restores DefaultMaxUnknownTextDivs, a negative value removes the bound
// (Java). It applies to the next text.
func (s *Store) SetMaxUnknownTextDivs(n int) {
	d := &s.texts.divs
	d.mu.Lock()
	d.max = n
	d.mu.Unlock()
}

// MaxUnknownTextDivs is the value SetMaxUnknownTextDivs set last (0: the
// default).
func (s *Store) MaxUnknownTextDivs() int {
	d := &s.texts.divs
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.max
}

// TextDivsLimited counts the texts refused with ErrTextDivLimit.
func (s *Store) TextDivsLimited() uint64 { return s.texts.divs.limited.Load() }

// admitTextDiv decides whether a permanent text of div may be queued: a
// known div, an unknown div counted already, and a new unknown div while
// fewer than the bound are counted are; any other is refused
// (ErrTextDivLimit) and counted. The permanent text directory is read on the
// first unknown div and again at the bound, at most every textDivRescan.
func (s *Store) admitTextDiv(div string) error {
	if knownTextDivs[div] {
		return nil
	}
	d := &s.texts.divs
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if d.clock != nil {
		now = d.clock()
	}
	if d.unknown == nil {
		if err := s.rescanTextDivsLocked(now); err != nil {
			return err
		}
	}
	if _, ok := d.unknown[div]; ok {
		return nil
	}
	limit := d.max
	if limit == 0 {
		limit = DefaultMaxUnknownTextDivs
	}
	if limit > 0 && len(d.unknown) >= limit && now.Sub(d.scanned) >= textDivRescan {
		_ = s.rescanTextDivsLocked(now) // on error the counted divs stay
		if _, ok := d.unknown[div]; ok {
			return nil
		}
	}
	if limit > 0 && len(d.unknown) >= limit {
		d.limited.Add(1)
		return ErrTextDivLimit
	}
	d.unknown[div] = now
	return nil
}

// rescanTextDivsLocked recounts the unknown divs (textDivs.mu held): those
// with a permanent file, those with a text still queued, and those admitted
// since the previous scan (their text may not be queued yet). A failed read
// is returned again, without reading, until textDivRescan has passed.
func (s *Store) rescanTextDivsLocked(now time.Time) error {
	d := &s.texts.divs
	if d.scanErr != nil && now.Sub(d.scanned) < textDivRescan {
		return d.scanErr
	}
	prev := d.scanned
	d.scanned = now
	divs, err := s.permTextDivsOnDisk()
	if d.scanErr = err; err != nil {
		return err
	}
	for div, at := range d.unknown {
		if at.After(prev) {
			divs[div] = at
		}
	}
	prefix := s.permTextPath("")
	q := &s.texts
	q.mu.Lock()
	for ref := range q.pending {
		if div, ok := strings.CutPrefix(ref.path, prefix); ok && !knownTextDivs[div] {
			if _, counted := divs[div]; !counted {
				divs[div] = time.Time{}
			}
		}
	}
	q.mu.Unlock()
	d.unknown = divs
	return nil
}

// permTextDivsOnDisk returns the unknown divs with a text_<div> index or
// data file in the permanent text directory.
func (s *Store) permTextDivsOnDisk() (map[string]time.Time, error) {
	divs := map[string]time.Time{}
	entries, err := os.ReadDir(s.path("00000000", "text", ""))
	if errors.Is(err, os.ErrNotExist) {
		return divs, nil
	}
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		name, ok := strings.CutPrefix(e.Name(), "text_")
		if !ok {
			continue
		}
		for _, ext := range []string{".hfile", ".kfile", ".data"} {
			if div, ok := strings.CutSuffix(name, ext); ok && !knownTextDivs[div] {
				divs[div] = time.Time{}
			}
		}
	}
	return divs, nil
}

func (s *Store) dailyTextPath(date string) string { return s.path(date, "text", "text") }
func (s *Store) permTextPath(div string) string {
	return s.path("00000000", "text", "text_"+div)
}
func textKey(div string, hash int32, daily bool) []byte {
	key := intKey(hash)
	if daily {
		return append(intKey(int32(crc32.ChecksumIEEE([]byte(div)))), key...)
	}
	return key
}

// PutText is TextWR.process: a daily div goes to {date}/text/text (value =
// UTF-8 text in the key file), anything else to 00000000/text/text_{div}
// (value = .data offset; empty text ignored). The first value of a key wins.
// This is also the write API for the permanent "table" (SqlTables) and "city"
// (GeoIP) dictionaries.
func (s *Store) PutText(date string, p *protocol.TextPack) error {
	return s.PutTextContext(context.Background(), date, p)
}

// PutTextContext validates p and queues it for the text writer
// (textqueue.go); TextContext sees it at once. ctx ends a wait for a full
// queue. Storage errors of the write itself are counted (TextWriteErrors),
// as the original TextWR logs them.
func (s *Store) PutTextContext(ctx context.Context, date string, p *protocol.TextPack) error {
	w, err := s.PrepareText(date, p)
	if err != nil || w.queue == NoQueue {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.enqueueText(ctx, w.text)
}

// PrepareText validates the text of PutTextContext and returns it as a Write
// for Offer/Submit (ingestwrite.go); an empty permanent text, which is not
// stored, is the empty Write. The daily/permanent target is decided now, and
// a permanent text of a new unknown div over the bound is refused
// (admitTextDiv, ErrTextDivLimit).
func (s *Store) PrepareText(date string, p *protocol.TextPack) (Write, error) {
	if err := validDate(date); err != nil {
		return Write{}, err
	}
	if p == nil || !textDivValid(p.XType) || !utf8.ValidString(p.Text) {
		return Write{}, ErrInvalid
	}
	if len(p.Text) > s.config.MaxRecordBytes {
		return Write{}, ErrLimit
	}
	daily := s.dailyText(p.XType)
	path := s.permTextPath(p.XType)
	if daily {
		path = s.dailyTextPath(date)
	} else if p.Text == "" {
		return Write{}, nil
	} else if err := s.admitTextDiv(p.XType); err != nil {
		return Write{}, err
	}
	return Write{queue: TextQueue, text: &textItem{ref: textRef{path, string(textKey(p.XType, p.Hash, daily))}, date: date, daily: daily, pack: *p}}, nil
}

// putTextBatchLocked is putTextLocked for texts queued for one dictionary
// path, in order: the first of a key wins (also within the batch), the data
// of the new permanent texts goes in one write and their index records in
// one more (PutBatch), the bytes consecutive putTextLocked calls would leave.
func (s *Store) putTextBatchLocked(path string, items []*textItem) (wrote []bool, errs []error) {
	wrote, errs = make([]bool, len(items)), make([]error, len(items))
	if len(items) == 1 {
		it := items[0]
		wrote[0], errs[0] = s.putTextLocked(context.Background(), it.date, path, it.daily, &it.pack)
		return wrote, errs
	}
	idx, err := s.index(path, false, true)
	if err != nil {
		for n := range errs {
			errs[n] = err
		}
		return wrote, errs
	}
	seen := make(map[string]bool, len(items))
	var fresh []int
	for n, it := range items {
		if err := s.check(it.date); err != nil {
			errs[n] = err
			continue
		}
		if seen[it.ref.key] {
			continue // an earlier text of this batch has the key
		}
		seen[it.ref.key] = true
		old, err := idx.Get([]byte(it.ref.key)) // TextTable/TextPermIndex hasKey: no S152
		if err != nil {
			errs[n] = err
			continue
		}
		if old == nil {
			fresh = append(fresh, n)
		}
	}
	if len(fresh) == 0 {
		return wrote, errs
	}
	fail := func(err error) {
		for _, n := range fresh {
			errs[n] = err
		}
	}
	values := make([][]byte, len(fresh))
	for k, n := range fresh {
		values[k] = []byte(items[n].pack.Text)
	}
	if !items[fresh[0]].daily {
		pos, err := s.appendDataBatch(path+".data", values, 4)
		if err != nil {
			fail(err)
			return wrote, errs
		}
		for k := range values {
			values[k] = offsetBytes(pos[k])
		}
	}
	entries := make([]legacyindex.Entry, len(fresh))
	for k, n := range fresh {
		entries[k] = legacyindex.Entry{Key: []byte(items[n].ref.key), Value: values[k]}
	}
	if _, err := idx.PutBatch(entries); err != nil {
		fail(err)
		return wrote, errs
	}
	for _, n := range fresh {
		wrote[n] = true
	}
	return wrote, errs
}

// putTextLocked is the TextWR.process write of one text to path (the caller
// holds Store.mu). The daily/permanent target was decided when the text was
// queued, so a live mgr_text_db_daily_* change applies from the next PutText,
// as it did when the write was synchronous. wrote reports a new record (a
// key already stored keeps its first value).
func (s *Store) putTextLocked(ctx context.Context, date, path string, daily bool, p *protocol.TextPack) (wrote bool, err error) {
	if err := s.check(date); err != nil {
		return false, err
	}
	idx, err := s.index(path, false, true)
	if err != nil {
		return false, err
	}
	key := textKey(p.XType, p.Hash, daily)
	old, err := idx.Get(key) // TextTable/TextPermIndex hasKey: no S152
	if err != nil || old != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err // last point before any record byte is written
	}
	value := []byte(p.Text)
	if !daily {
		pos, err := s.appendData(path+".data", value, 4)
		if err != nil {
			return false, err
		}
		value = offsetBytes(pos)
	}
	if _, err = idx.Put(key, value); err != nil {
		return false, err
	}
	return true, nil
}

// TextCached is TextContext answered without the Store gate, or known=false
// when TextContext would read the files. A text still queued for the writer,
// or read before (the read cache, TextCache of the original), is answered
// without the gate (ingest group lookups would otherwise wait for the
// writers' holds); so is a text the files did not have (and none queued
// since, knownMissing), as "not found". The first place TextRD looks is
// checked, then the permanent one when the first is known missing, so the
// lookup order stays the original's. It never blocks
// on more than the text queue's leaf lock: the ingest engine calls it under
// its lock and reads the files only after releasing it (ingest CONTRACT §11).
func (s *Store) TextCached(date, div string, hash int32) (text string, found, known bool) {
	if text, found, known = s.textCached(date, div, hash); known {
		s.texts.stats.hits.Add(1)
	}
	return text, found, known
}
func (s *Store) textCached(date, div string, hash int32) (text string, found, known bool) {
	if validDate(date) != nil || !textDivValid(div) {
		return "", false, false // TextContext reports the error
	}
	perm := textRef{s.permTextPath(div), string(textKey(div, hash, false))}
	first := perm
	if s.dailyText(div) {
		first = textRef{s.dailyTextPath(date), string(textKey(div, hash, true))}
	}
	if text, ok := s.pendingText(first); ok {
		return text, true, true
	}
	if s.knownMissing(first) {
		if first == perm {
			return "", false, true
		}
		if text, ok := s.pendingText(perm); ok {
			return text, true, true
		}
		if s.knownMissing(perm) {
			return "", false, true
		}
	}
	return "", false, false
}

func (s *Store) Text(date, div string, hash int32) (string, bool, error) {
	return s.TextContext(context.Background(), date, div, hash)
}

// TextContext is TextRD.getString, with a bounded read cache in place of
// TextCache (rememberRead): a permanent div reads
// only text_{div}; a daily div reads the date's table and, on a miss, falls
// back to text_{div} (TextRD.scala:40-41). A daily text written while the div
// was daily is not found after the div becomes permanent (Java D-T1, kept).
// Unlike Java, a read never creates the date's text files.
func (s *Store) TextContext(ctx context.Context, date, div string, hash int32) (string, bool, error) {
	return s.textContext(ctx, date, div, hash, true)
}

// textContext is TextContext; queued false skips the readers' slot (the
// fetcher of TextWithin: the ingest path's reads wait for the gate, not
// behind every other reader).
func (s *Store) textContext(ctx context.Context, date, div string, hash int32, queued bool) (string, bool, error) {
	if text, ok, known := s.TextCached(date, div, hash); known {
		return text, ok, nil
	}
	// A request no file can answer does not wait for the slot or the gate
	// (an XLog with an end time past year 9999 has such a date).
	if err := validDate(date); err != nil {
		return "", false, err
	}
	if !textDivValid(div) {
		return "", false, ErrInvalid
	}
	// One file read at a time (textQueue.slot); the reader before this one
	// may have read the same text.
	if slot := s.texts.slot; queued && slot != nil {
		select {
		case slot <- struct{}{}:
		case <-ctx.Done():
			return "", false, ctx.Err()
		}
		defer func() { <-slot }()
		if text, ok, known := s.textCached(date, div, hash); known {
			return text, ok, nil
		}
	}
	if err := s.mu.LockContext(ctx); err != nil {
		return "", false, err
	}
	defer s.mu.Unlock()
	if err := s.check(date); err != nil {
		return "", false, err
	}
	s.texts.stats.fileReads.Add(1)
	if s.dailyText(div) {
		if text, ok := s.pendingText(textRef{s.dailyTextPath(date), string(textKey(div, hash, true))}); ok {
			return text, true, nil
		}
		text, ok, err := s.dailyTextLocked(ctx, date, div, hash)
		if err == nil {
			if ref := (textRef{s.dailyTextPath(date), string(textKey(div, hash, true))}); ok {
				s.rememberRead(ref, text)
			} else {
				s.rememberMissing(ref) // under the gate: a text queued since is pending
			}
		}
		if err != nil || ok {
			return text, ok, err
		}
	}
	ref := textRef{s.permTextPath(div), string(textKey(div, hash, false))}
	if text, ok := s.pendingText(ref); ok {
		return text, true, nil
	}
	text, ok, err := s.permTextLocked(ctx, div, hash)
	if err == nil {
		if ok {
			s.rememberRead(ref, text)
		} else {
			s.rememberMissing(ref)
		}
	}
	return text, ok, err
}
func (s *Store) dailyTextLocked(ctx context.Context, date, div string, hash int32) (string, bool, error) {
	idx, err := s.index(s.dailyTextPath(date), false, false)
	if err != nil || idx == nil {
		return "", false, err
	}
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	b, err := queryGet(ctx, idx, textKey(div, hash, true))
	if err != nil || b == nil {
		return "", false, err
	}
	return s.textValue(b)
}
func (s *Store) permTextLocked(ctx context.Context, div string, hash int32) (string, bool, error) {
	path := s.permTextPath(div)
	idx, err := s.index(path, false, false)
	if err != nil || idx == nil {
		return "", false, err
	}
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	b, err := queryGet(ctx, idx, textKey(div, hash, false))
	if err != nil || b == nil {
		return "", false, err
	}
	pos, err := dataOffset(b)
	if err != nil {
		return "", false, err
	}
	if b, err = s.readData(path+".data", pos, 4); err != nil {
		return "", false, err
	}
	return s.textValue(b)
}
func (s *Store) textValue(b []byte) (string, bool, error) {
	if len(b) > s.config.MaxRecordBytes {
		return "", false, ErrLimit
	}
	if !utf8.Valid(b) {
		return "", false, ErrCorrupt
	}
	return string(b), true, nil
}
