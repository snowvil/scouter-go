// Copyright Scouter authors, Apache-2.0. Independent pinned VisitorDB implementation.
package storage

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

type visitorEntry struct {
	hll   *visitorHLL
	dirty bool
	// synced: the file at the entry's save path equals the HLL except for the
	// pages marked since, so a flush may rewrite only those pages. It is set
	// by a full write; a loaded hourly HLL comes from a different path
	// (double suffix) and an empty HLL has no file, so both start unsynced.
	// For a partial entry it means the file holds the base the visits add to,
	// so a write merges them into it.
	synced bool
	// partial: hll holds only the registers raised since the entry's last
	// write, not the file's; see visitorState.
	partial bool
	// writes counts flush passes writing the entry outside visitorMu; gen
	// changes with every snapshot taken and every demotion, so a pass
	// installs a promoted HLL only if nothing else wrote or demoted meanwhile.
	writes   int
	gen      uint64
	sequence uint64 // creation order, matched by visitorFIFO items
	used     uint64 // last offer, the demotion order
	bytes    int64  // accounted size of hll (visitorHLL.memBytes)
	load     string // name the base is loaded from (the hourly double suffix)
}

// visitorState is the set of HLLs being counted. An entry starts partial:
// a visit only raises registers in memory, so the ingest path reads and
// writes no file however many objects send user ids (Java VisitorDB loads the
// snapshot on a miss and keeps 500 per set, which thrashes past 500 objects).
// The flush writes a partial entry by merging its registers into the file and,
// while resident HLLs fit residentMax, promotes it: loads the file once and
// keeps the full HLL, so later visits that change no register cost nothing.
// Over residentMax (a visit that grows a resident HLL, or a flush) the least
// recently used resident entries go back to partial down to 7/8 of it,
// keeping their unwritten registers as the delta. A dirty entry whose delta
// would not fit deltaMax stays resident for the next pass to write (see
// demoteVisitorsLocked). The file bytes are those of an HLL kept for the
// whole day or hour: max is order independent.
type visitorState struct {
	date          string
	hour          int64
	lastFlush     int64
	flushDue      bool
	daily, hourly map[string]*visitorEntry
	sequence      uint64
	uses          uint64
	// typeQ, objectQ and hourQ are the creation order of the daily objType,
	// daily object and hourly entries, the FIFO eviction past their caps.
	typeQ, objectQ, hourQ visitorFIFO
	// residentBytes and deltaBytes are the accounted HLL sizes of the resident
	// and partial entries of daily and hourly, whose sizes they were counted
	// at (purge and delete replace a set; checkSets then recounts).
	residentBytes, deltaBytes int64
	dailyLen, hourlyLen       int
	residentMax, deltaMax     int64
	typesMax, objectsMax      int // entry caps (visitorTypesMax, visitorObjectsMax)
	// demoteWait: the last demotion kept dirty resident entries because
	// their deltas did not fit deltaMax. Until the next pass end a visit
	// past residentMax demotes only the entry it grew, if that fits (it does
	// not sort every resident entry again); the pass writes the kept entries
	// and demotes them clean.
	demoteWait bool
	// budgetGen is the Store.visitorBudgetGen applied to the budgets.
	budgetGen uint64
	// hourWritten holds the hourly keys of this hour evicted after a write:
	// a key coming back merges into that file instead of replacing it from
	// the double suffix load (bounded by objectsMax, then not recorded).
	hourWritten map[string]struct{}
	// writeErr is the first failed write-through since the last flush pass,
	// which reports it; its registers stay in memory for the next write.
	writeErr                     error
	writeThroughs, writeFailures uint64
	// names and typeKeys keep the entry keys putVisitorLocked builds for
	// every XLog, per object and object type of this day (bounded by
	// visitorNamesMax, then started over).
	names    map[int32]*visitorNames
	typeKeys map[string]string
}

// Entry caps: objType entries like Java (100). Object entries are no longer
// Java's 500 but enough for every agent of a large server: a partial entry is
// about 200 bytes. Past a cap the oldest entry is written and dropped.
const (
	visitorTypesMax   = 100
	visitorObjectsMax = 32768
	// visitorResidentMax is the default budget of resident HLLs: about 380
	// dense HLLs (Java keeps up to 1100 dense HLLs, ~733MiB), or many more
	// sparse ones (an object with a few thousand users a day is ~100KiB).
	visitorResidentMax = 256 << 20
	// visitorDeltaMax bounds the registers waiting for the flush (normally a
	// few MiB: 10s of visits). Past it a visit to a partial entry is written
	// through at once and the flush is marked due, so a stalled flush slows
	// the visitor writer instead of growing memory.
	visitorDeltaMax = 64 << 20
)

// visitorFIFO is a queue of entry references in creation order. An item whose
// entry left its set (purge, delete) is stale and skipped.
type visitorFIFO struct {
	items []visitorRef
	head  int
}
type visitorRef struct {
	key      string
	sequence uint64
}

func (q *visitorFIFO) len() int { return len(q.items) - q.head }
func (q *visitorFIFO) push(r visitorRef) {
	if q.head > 0 && q.head >= len(q.items)/2 {
		q.items = append(q.items[:0], q.items[q.head:]...)
		clear(q.items[len(q.items):cap(q.items)])
		q.head = 0
	}
	q.items = append(q.items, r)
}
func (q *visitorFIFO) pop() visitorRef {
	r := q.items[q.head]
	q.items[q.head] = visitorRef{}
	q.head++
	return r
}

// account updates the budget totals with e's current HLL size.
func (v *visitorState) account(e *visitorEntry) {
	n := e.hll.memBytes()
	if e.partial {
		v.deltaBytes += n - e.bytes
	} else {
		v.residentBytes += n - e.bytes
	}
	e.bytes = n
}
func (v *visitorState) unaccount(e *visitorEntry) {
	if e.partial {
		v.deltaBytes -= e.bytes
	} else {
		v.residentBytes -= e.bytes
	}
	e.bytes = 0
}

// checkSets recounts the totals when a set was replaced outside this file
// (purge and delete assign an empty map; only this file adds entries, and it
// checks first).
func (v *visitorState) checkSets() {
	if len(v.daily) == v.dailyLen && len(v.hourly) == v.hourlyLen {
		return
	}
	v.residentBytes, v.deltaBytes = 0, 0
	for _, set := range [...]map[string]*visitorEntry{v.daily, v.hourly} {
		for _, e := range set {
			if e.partial {
				v.deltaBytes += e.bytes
			} else {
				v.residentBytes += e.bytes
			}
		}
	}
	v.dailyLen, v.hourlyLen = len(v.daily), len(v.hourly)
}

// visitorNames are the keys and names of one object's daily and hourly
// entries; the hourly ones belong to hour.
type visitorNames struct {
	key, name                  string // "h:"+hash, hash
	hour                       int64
	hourlyName, hourlyLoadName string
}

const visitorNamesMax = visitorObjectsMax

func (v *visitorState) objectNames(objHash int32) *visitorNames {
	n := v.names[objHash]
	if n == nil {
		if v.names == nil || len(v.names) >= visitorNamesMax {
			v.names = map[int32]*visitorNames{}
		}
		h := aggregationHash(objHash)
		n = &visitorNames{key: "h:" + h, name: h, hour: -1}
		v.names[objHash] = n
	}
	if n.hour != v.hour {
		n.hour = v.hour
		n.hourlyName = n.name + "_" + strconv.FormatInt(v.hour, 10)
		// The source getNewObject applies the hour suffix twice while loading after restart.
		n.hourlyLoadName = n.hourlyName + "_" + strconv.FormatInt(v.hour, 10)
	}
	return n
}

func (v *visitorState) typeKey(objType string) string {
	k, ok := v.typeKeys[objType]
	if !ok {
		if v.typeKeys == nil || len(v.typeKeys) >= visitorNamesMax {
			v.typeKeys = map[string]string{}
		}
		k = "t:" + objType
		v.typeKeys[objType] = k
	}
	return k
}

func aggregationName(v string) error {
	if v == "" || len(v) > 1024 || !utf8.ValidString(v) || v == "." || v == ".." || strings.ContainsAny(v, "/\\\x00") {
		return ErrInvalid
	}
	return nil
}

// walkSafePath rejects a path outside the root or through a symlink.
func (s *Store) walkSafePath(path string) error {
	rel, err := filepath.Rel(s.config.Directory, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return ErrInvalid
	}
	current := s.config.Directory
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return ErrInvalid
		}
	}
	return nil
}
func aggregationHash(hash int32) string {
	v := int64(hash)
	if v < 0 {
		return "z" + strconv.FormatInt(-v, 32)
	}
	if v < 10 {
		return strconv.FormatInt(v, 10)
	}
	return "x" + strconv.FormatInt(v, 32)
}

// lockVisitor takes visitorMu unless ctx ends first (the lock is held only for
// in-memory work and one snapshot copy, so the wait is short).
func (s *Store) lockVisitor(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.visitorMu.Lock()
	if err := ctx.Err(); err != nil {
		s.visitorMu.Unlock()
		return err
	}
	return nil
}
func (s *Store) visitorInit(now int64) {
	if s.visitor == nil {
		s.visitor = &visitorState{date: JavaDate(now, s.config.Location), hour: JavaMillis(now, s.config.Location) / 3600000, lastFlush: now, daily: map[string]*visitorEntry{}, hourly: map[string]*visitorEntry{},
			typesMax: visitorTypesMax, objectsMax: visitorObjectsMax}
		s.visitor.budgetGen = s.visitorBudgetGen.Load()
		s.visitor.residentMax, s.visitor.deltaMax = s.visitorBudgets()
	}
}

// visitorBudgets returns the budgets SetVisitorBudgets set.
func (s *Store) visitorBudgets() (resident, delta int64) {
	resident, delta = visitorResidentMax, visitorDeltaMax
	if mb := s.visitorResidentMB.Load(); mb > 0 {
		resident = int64(mb) << 20
	}
	if mb := s.visitorDeltaMB.Load(); mb > 0 {
		delta = int64(mb) << 20
	}
	return resident, delta
}

// SetVisitorBudgets sets the resident HLL and delta budgets in MiB
// (go_visitor_resident_mb, go_visitor_delta_mb); 0 restores the default
// (visitorResidentMax, visitorDeltaMax). They change memory, CPU and I/O,
// never the file bytes. It only stores the values (it runs on the config
// reload path, which must not wait for visitorMu); the visitor state applies
// them at its next visit, roll tick, stats read or pass
// (applyVisitorBudgetsLocked).
func (s *Store) SetVisitorBudgets(residentMB, deltaMB int) {
	s.visitorResidentMB.Store(int32(max(residentMB, 0)))
	s.visitorDeltaMB.Store(int32(max(deltaMB, 0)))
	s.visitorBudgetGen.Add(1)
}

// applyVisitorBudgetsLocked applies a SetVisitorBudgets change (visitorMu
// held): a lower resident budget demotes now, in memory (an entry whose
// delta would not fit waits for a pass, see demoteVisitorsLocked); a higher
// one promotes from the next pass. Past the delta budget (lowered, or raised
// by the demotion) the flush is marked due, so the pass writes the deltas
// before visits write them through.
func (s *Store) applyVisitorBudgetsLocked() {
	v := s.visitor
	if v == nil || s.visitorClosed {
		return
	}
	// The generation is read before the values: a change racing this keeps
	// a newer generation and is applied again next time.
	gen := s.visitorBudgetGen.Load()
	if gen == v.budgetGen {
		return
	}
	v.budgetGen = gen
	v.residentMax, v.deltaMax = s.visitorBudgets()
	s.demoteVisitorsLocked(true)
	if v.deltaBytes > v.deltaMax {
		v.flushDue = true
	}
}

// Rollovers intentionally clear before periodic flush, matching the pinned Java daemon.
func (s *Store) visitorTickLocked(ctx context.Context, now int64) error {
	s.visitorInit(now)
	s.applyVisitorBudgetsLocked()
	v := s.visitor
	date, hour := JavaDate(now, s.config.Location), JavaMillis(now, s.config.Location)/3600000
	if v.date != date {
		v.date = date
		v.daily = map[string]*visitorEntry{}
		v.typeQ, v.objectQ = visitorFIFO{}, visitorFIFO{}
	}
	if v.hour != hour {
		v.hour = hour
		v.hourly = map[string]*visitorEntry{}
		v.hourQ, v.hourWritten = visitorFIFO{}, nil
	}
	v.checkSets()
	if now-v.lastFlush >= 10000 {
		v.lastFlush = now
		v.flushDue = true
	}
	return ctx.Err()
}
func (s *Store) loadVisitorLocked(ctx context.Context, path string) (*visitorHLL, error) {
	// Visitor files never enter the handle cache, so the path is walked
	// directly.
	if err := s.walkSafePath(path); err != nil {
		return nil, err
	}
	// Visitor files are whole-file snapshots; they bypass the bounded handle
	// cache so a flush of many objects does not evict hot XLog/index handles.
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() != visitorFileBytes {
		return nil, ErrCorrupt
	}
	if visitorFileBytes > s.config.MaxQueryBytes {
		return nil, ErrLimit
	}
	b := make([]byte, visitorFileBytes)
	_, err = io.ReadFull(queryReader{ctx, io.NewSectionReader(f, 0, visitorFileBytes)}, b)
	if err != nil {
		return nil, err
	}
	return decodeVisitorHLL(b)
}

// writeVisitorPages rewrites the marked pages of an existing snapshot in
// place. The file keeps its size and header, and HLL registers only grow, so
// an interrupted write leaves a valid snapshot that misses some of the
// unflushed visits, which unsynced snapshots may lose anyway (Java truncates
// and rewrites the whole file). It returns false, nothing written, when the
// file is missing or not a full snapshot; the caller then rewrites it whole.
func writeVisitorPages(path string, image []byte, pages [(visitorPages + 63) / 64]uint64) (bool, error) {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	st, err := f.Stat()
	if err != nil || st.Size() != int64(len(image)) {
		return false, errors.Join(err, f.Close())
	}
	for page := 0; page < visitorPages; {
		if pages[page/64]&(1<<(page%64)) == 0 {
			page++
			continue
		}
		end := page + 1
		for end < visitorPages && pages[end/64]&(1<<(end%64)) != 0 {
			end++
		}
		lo, hi := page*visitorPageBytes, min(end*visitorPageBytes, len(image))
		if _, err = f.WriteAt(image[lo:hi], int64(lo)); err != nil {
			return true, errors.Join(err, f.Close())
		}
		page = end
	}
	return true, f.Close()
}

// writeVisitorFile replaces a snapshot. Like Java VisitorDB it is not fsynced,
// but rename means a crash never leaves a torn file that later loads as ErrCorrupt.
func writeVisitorFile(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".scouter-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	n, err := f.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	return os.Rename(name, path)
}

// raiseVisitorFile raises the registers of an existing snapshot to delta's in
// place: it reads only the pages delta touches and rewrites only those that
// change, so an interrupted write leaves a valid snapshot (registers only
// grow). It returns false, nothing written, when the file is missing; a file
// that is not a full snapshot is ErrCorrupt (it holds registers delta lacks).
func raiseVisitorFile(path string, delta *visitorHLL, page []byte) (ok bool, err error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	st, err := f.Stat()
	if err != nil {
		return true, err
	}
	if st.Size() != visitorFileBytes {
		return true, ErrCorrupt
	}
	page = page[:visitorPageBytes]
	if _, err = f.ReadAt(page[:8], 0); err != nil {
		return true, err
	}
	if binary.BigEndian.Uint32(page) != visitorPrecision || binary.BigEndian.Uint32(page[4:]) != visitorWords {
		return true, ErrCorrupt
	}
	buckets := delta.buckets()
	for i := 0; i < len(buckets); {
		lo := (8 + int(buckets[i])*4) / visitorPageBytes * visitorPageBytes
		hi := min(lo+visitorPageBytes, visitorFileBytes)
		b := page[:hi-lo]
		if _, err = f.ReadAt(b, int64(lo)); err != nil {
			return true, err
		}
		changed := false
		// A word never straddles a page: offsets and the page size are
		// multiples of 4.
		for ; i < len(buckets) && 8+int(buckets[i])*4 < hi; i++ {
			off := 8 + int(buckets[i])*4 - lo
			w := binary.BigEndian.Uint32(b[off:])
			if w>>30 != 0 {
				return true, ErrCorrupt
			}
			if m := visitorMaxWord(w, delta.word(buckets[i])); m != w {
				binary.BigEndian.PutUint32(b[off:], m)
				changed = true
			}
		}
		if changed {
			if _, err = f.WriteAt(b, int64(lo)); err != nil {
				return true, err
			}
		}
	}
	return true, nil
}

// visitorSnap is one entry write, taken under visitorMu and visitorIO and
// done holding visitorIO only. image is visitorBuf, owned by the visitorIO holder.
type visitorSnap struct {
	path, loadPath string
	gen            uint64
	// resident: image and the pages changed since the last write.
	image []byte
	pages [(visitorPages + 63) / 64]uint64
	// partial: the registers to add, taken from the entry.
	delta *visitorHLL
	// inPlace: the file is the base (resident: pages rewrite; partial: merge).
	inPlace, promote bool
}

// takeVisitorLocked takes e's write and marks it clean (visitorMu and
// visitorIO held). promote asks a partial entry write to load the full HLL.
func (s *Store) takeVisitorLocked(e *visitorEntry, path, loadPath string, promote bool) visitorSnap {
	v := s.visitor
	e.gen++
	w := visitorSnap{path: path, loadPath: loadPath, gen: e.gen, inPlace: e.synced}
	if e.partial {
		w.delta, w.promote = e.hll, promote
		e.hll = &visitorHLL{}
		v.account(e)
	} else {
		s.visitorBuf = e.hll.encode(s.visitorBuf)
		w.image, w.pages = s.visitorBuf, e.hll.takePages()
	}
	e.dirty, e.synced = false, true
	return w
}

// writeVisitorSnap does the write (visitorIO held). For a promoted partial
// entry it returns the full HLL written, for the caller to install.
func (s *Store) writeVisitorSnap(ctx context.Context, w *visitorSnap) (*visitorHLL, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(w.path), 0700); err != nil {
		return nil, err
	}
	if w.delta == nil {
		partial := w.inPlace
		var err error
		if partial {
			partial, err = writeVisitorPages(w.path, w.image, w.pages)
		}
		if err == nil && !partial {
			err = writeVisitorFile(w.path, w.image)
		}
		return nil, err
	}
	if w.inPlace && !w.promote {
		if len(s.visitorBuf) < visitorPageBytes {
			s.visitorBuf = make([]byte, visitorFileBytes)
		}
		ok, err := raiseVisitorFile(w.path, w.delta, s.visitorBuf)
		if ok || err != nil {
			return nil, err
		}
		s.visitorBuf = w.delta.encode(s.visitorBuf)
		return nil, writeVisitorFile(w.path, s.visitorBuf)
	}
	// The base comes from the save path, or, for an unsynced entry, from the
	// load path (the hourly double suffix): Java's first write of an entry
	// replaces the file with what it loaded plus the visits.
	from := w.path
	if !w.inPlace {
		from = w.loadPath
	}
	full, err := s.loadVisitorLocked(ctx, from)
	if err != nil {
		return nil, err
	}
	exists := full != nil && w.inPlace
	if full == nil {
		full = newVisitorHLL()
	}
	full.takePages()
	full.raise(w.delta)
	s.visitorBuf = full.encode(s.visitorBuf)
	pages := full.takePages()
	if exists {
		exists, err = writeVisitorPages(w.path, s.visitorBuf, pages)
	}
	if err == nil && !exists {
		err = writeVisitorFile(w.path, s.visitorBuf)
	}
	if err != nil || !w.promote {
		return nil, err
	}
	return full, nil
}

// restoreVisitorLocked puts back a write that failed (visitorMu held): it is
// retried by the next flush, as a full rewrite for a resident entry since
// the pages taken may be partly written. Registers only grow, so a partial
// entry merges its delta back whatever was written since.
func (s *Store) restoreVisitorLocked(e *visitorEntry, w *visitorSnap) {
	e.dirty = true
	if w.delta == nil {
		if !e.partial {
			e.synced = false
		}
		return
	}
	// A resident entry here was promoted by a later pass from a file that
	// lacks the delta; raising marks the pages to write.
	e.hll.raise(w.delta)
	s.visitor.account(e)
	if e.gen == w.gen && !w.inPlace {
		e.synced = false // the first rewrite (load path base) is still due
	}
}

// installVisitorLocked makes a promoted entry resident (visitorMu held) unless
// another write or a demotion came between.
func (s *Store) installVisitorLocked(e *visitorEntry, w *visitorSnap, full *visitorHLL) {
	v := s.visitor
	if !e.partial || e.gen != w.gen || e.writes != 0 || v.residentBytes+full.memBytes() > v.residentMax {
		return
	}
	full.takePages()
	if full.raise(e.hll) {
		e.dirty = true
	} else {
		e.dirty = false
	}
	v.unaccount(e)
	e.hll, e.partial = full, false
	v.account(e)
}

// writeVisitorNowLocked writes e in the caller's lock hold (visitorMu and
// visitorIO held): eviction, write-through and Close.
func (s *Store) writeVisitorNowLocked(ctx context.Context, w visitorWork) error {
	if err := s.walkSafePath(w.path); err != nil {
		return err
	}
	snap := s.takeVisitorLocked(w.entry, w.path, w.loadPath, false)
	if _, err := s.writeVisitorSnap(ctx, &snap); err != nil {
		s.restoreVisitorLocked(w.entry, &snap)
		return err
	}
	return nil
}

// flushVisitors writes dirty snapshots outside the visitor lock, one entry at
// a time: visitorMu is held only to take one entry's write (a copy of a
// resident HLL, ~700KiB at Java rsd=20, or a partial entry's registers) and
// mark it clean. Java VisitorDB also flushes from its own thread. visitorIO,
// taken before visitorMu is released, keeps writes of one path in snapshot order.
//
// A pass writes each entry dirty at its start at most once: entries dirtied
// again meanwhile wait for the next pass, so a steady stream of new visitors
// cannot keep the pass (and the maintenance loop) running. A failed entry is
// kept dirty and the pass goes on with the others; it returns the first error.
func (s *Store) flushVisitors(ctx context.Context) error {
	if err := s.lockVisitor(ctx); err != nil {
		return err
	}
	if s.visitorClosed || s.visitor == nil {
		s.visitorMu.Unlock()
		return nil
	}
	s.applyVisitorBudgetsLocked()
	s.visitor.checkSets()
	work := s.dirtyVisitorsLocked()
	s.visitorMu.Unlock()
	var first error
	safeDirs := map[string]bool{}
	for _, w := range work {
		if err := s.lockVisitor(ctx); err != nil {
			return errors.Join(first, err)
		}
		v := s.visitor
		if s.visitorClosed || v == nil {
			s.visitorMu.Unlock()
			return first
		}
		// Rolled over, purged or evicted meanwhile: the entry is no longer in
		// the live set (an evicted dirty entry was written by the eviction).
		if w.set()[w.key] != w.entry || !w.entry.dirty {
			s.visitorMu.Unlock()
			continue
		}
		w = s.visitorWorkLocked(w.hourly, w.key, w.entry)
		if err := s.safeVisitorPath(w.path, safeDirs); err != nil {
			s.visitorMu.Unlock()
			if first == nil {
				first = err
			}
			continue
		}
		s.visitorIO.Lock()
		snap := s.takeVisitorLocked(w.entry, w.path, w.loadPath, v.residentBytes+visitorDenseBytes <= v.residentMax)
		w.entry.writes++
		s.visitorMu.Unlock()
		full, err := s.writeVisitorSnap(ctx, &snap)
		s.visitorIO.Unlock()
		s.visitorMu.Lock()
		err = s.finishVisitorWriteLocked(ctx, w, &snap, full, err)
		s.visitorMu.Unlock()
		if err != nil {
			if ctx.Err() != nil {
				return errors.Join(first, ctx.Err())
			}
			if first == nil {
				first = err
			}
		}
	}
	if err := s.lockVisitor(ctx); err != nil {
		return errors.Join(first, err)
	}
	if !s.visitorClosed && s.visitor != nil {
		// After a failed write the entries kept from demotion wait for the
		// 10s tick, not a pass every second that fails again.
		s.demoteVisitorsLocked(first == nil)
		first = errors.Join(first, s.visitor.writeErr)
		s.visitor.writeErr = nil
	}
	s.visitorMu.Unlock()
	return errors.Join(first, ctx.Err())
}

// finishVisitorWriteLocked settles a pass write done outside visitorMu
// (visitorMu held again): a failed write is put back, a promoted HLL
// installed, a just written entry demoted if over the budget. A write that
// failed while Close ran (Close writes the entries in memory and returns) is
// retried once here: a visitor snapshot is a standalone file, so writing it
// after Close is allowed, and its error goes to the pass's caller.
func (s *Store) finishVisitorWriteLocked(ctx context.Context, w visitorWork, snap *visitorSnap, full *visitorHLL, err error) error {
	v := s.visitor
	w.entry.writes--
	if w.set()[w.key] != w.entry {
		return err
	}
	switch {
	case err != nil:
		s.restoreVisitorLocked(w.entry, snap)
		if s.visitorClosed && ctx.Err() == nil {
			s.visitorIO.Lock()
			err = s.writeVisitorNowLocked(ctx, w)
			s.visitorIO.Unlock()
		}
	case full != nil:
		s.installVisitorLocked(w.entry, snap, full)
	case !w.entry.partial && !w.entry.dirty && w.entry.writes == 0 && v.residentBytes > v.residentMax:
		v.demoteLocked(w.entry) // just written: equal to its file
	}
	return err
}

// VisitorStats are the visitor memory budgets and write-throughs.
type VisitorStats struct {
	Entries                    int   // daily (objType, object) and hourly entries
	ResidentBytes, ResidentMax int64 // HLLs kept whole (estimated heap)
	DeltaBytes, DeltaMax       int64 // registers waiting for the flush
	// WriteThroughs counts the visits past DeltaMax written on the ingest
	// path; WriteThroughFailures those that failed (their registers stay for
	// the next write and the next pass returns the first error).
	WriteThroughs, WriteThroughFailures uint64
}

func (s *Store) VisitorStats() VisitorStats {
	s.visitorMu.Lock()
	defer s.visitorMu.Unlock()
	v := s.visitor
	if v == nil {
		resident, delta := s.visitorBudgets()
		return VisitorStats{ResidentMax: resident, DeltaMax: delta}
	}
	s.applyVisitorBudgetsLocked()
	v.checkSets()
	return VisitorStats{Entries: len(v.daily) + len(v.hourly), ResidentBytes: v.residentBytes, ResidentMax: v.residentMax,
		DeltaBytes: v.deltaBytes, DeltaMax: v.deltaMax, WriteThroughs: v.writeThroughs, WriteThroughFailures: v.writeFailures}
}

// safeVisitorPath is walkSafePath for a pass: each directory is walked once
// per pass, then only the file itself is checked (the pass would otherwise
// Lstat every component for every entry).
func (s *Store) safeVisitorPath(path string, dirs map[string]bool) error {
	dir := filepath.Dir(path)
	if !dirs[dir] {
		if err := s.walkSafePath(dir); err != nil {
			return err
		}
		dirs[dir] = true
	}
	info, err := os.Lstat(path)
	if err == nil && info.Mode()&os.ModeSymlink != 0 {
		return ErrInvalid
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// demoteVisitorsLocked turns the least recently used resident entries back
// to partial until the resident HLLs fit 7/8 of residentMax (the margin keeps
// a visit near the budget from sorting every time). Entries a pass is writing
// stay. A dirty entry whose delta (at most its HLL) would take the deltas
// past deltaMax stays too: demoting it frees little (an unsynced entry's
// delta is its whole HLL) and would make the next visits write through on
// the ingest path, as a large cut of the budget otherwise would. Those wait
// (demoteWait) for a pass, which writes them and demotes them clean; rearm
// marks the flush due if they leave the resident HLLs over the budget (a
// pass whose writes failed leaves that to the 10s tick).
func (s *Store) demoteVisitorsLocked(rearm bool) {
	v := s.visitor
	v.checkSets()
	v.demoteWait = false
	if v.residentBytes <= v.residentMax {
		return
	}
	var resident []*visitorEntry
	for _, set := range [...]map[string]*visitorEntry{v.daily, v.hourly} {
		for _, e := range set {
			if !e.partial && e.writes == 0 {
				resident = append(resident, e)
			}
		}
	}
	slices.SortFunc(resident, func(a, b *visitorEntry) int { return cmp.Compare(a.used, b.used) })
	skipped := false
	for _, e := range resident {
		if v.residentBytes <= v.residentMax-v.residentMax/8 {
			break
		}
		if e.dirty && v.deltaBytes+e.bytes > v.deltaMax {
			skipped = true
			continue
		}
		v.demoteLocked(e)
	}
	// Even back under the budget, a skip makes visits stop sorting until
	// the pass (they would skip the same entries again).
	v.demoteWait = skipped
	if skipped && rearm && v.residentBytes > v.residentMax {
		v.flushDue = true
	}
}

// demoteLocked makes a resident entry partial without losing a visit. A
// synced entry's file equals the HLL outside the pages marked since its last
// write, and registers only grow, so the words of those pages are the delta
// to merge. An unsynced one keeps every word for its pending whole write
// (base from the load path, then the delta).
func (v *visitorState) demoteLocked(e *visitorEntry) {
	delta := &visitorHLL{}
	if e.dirty {
		if e.synced {
			delta = e.hll.markedWords()
		} else {
			delta = e.hll
		}
	}
	v.unaccount(e)
	e.hll, e.partial = delta, true
	e.gen++
	v.account(e)
}

type visitorWork struct {
	path, loadPath, key string
	entry               *visitorEntry
	hourly              bool
	visitor             *visitorState
}

// set is the live set the entry belongs to (visitorMu held).
func (w visitorWork) set() map[string]*visitorEntry {
	if w.hourly {
		return w.visitor.hourly
	}
	return w.visitor.daily
}

func (s *Store) visitorWorkLocked(hourly bool, key string, e *visitorEntry) visitorWork {
	v := s.visitor
	dir, name := "visit", key
	if hourly {
		dir = "visit_hourly"
	} else {
		name = key[2:]
	}
	load := e.load
	if load == "" {
		load = name
	}
	return visitorWork{s.path(v.date, dir, name+".usr"), s.path(v.date, dir, load+".usr"), key, e, hourly, v}
}

func (s *Store) dirtyVisitorsLocked() []visitorWork {
	v := s.visitor
	var out []visitorWork
	for _, hourly := range []bool{false, true} {
		set := v.daily
		if hourly {
			set = v.hourly
		}
		for k, entry := range set {
			if entry.dirty {
				// Paths are built when the entry is written.
				out = append(out, visitorWork{key: k, entry: entry, hourly: hourly, visitor: v})
			}
		}
	}
	return out
}

// visitorFlushLocked writes every dirty entry in the Close hold (visitorMu
// and visitorIO held), going on past a failed entry.
func (s *Store) visitorFlushLocked(ctx context.Context) error {
	if s.visitor == nil {
		return ctx.Err()
	}
	v := s.visitor
	v.checkSets()
	var errs []error
	for _, hourly := range []bool{false, true} {
		set := v.daily
		if hourly {
			set = v.hourly
		}
		keys := make([]string, 0, len(set))
		for k := range set {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if e := set[k]; e.dirty {
				if err := s.writeVisitorNowLocked(ctx, s.visitorWorkLocked(hourly, k, e)); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}
	return errors.Join(append(errs, ctx.Err())...)
}
func (s *Store) PutVisitorContext(ctx context.Context, now int64, objType string, userID int64, objHash int32) error {
	if err := aggregationName(objType); err != nil {
		return err
	}
	if now <= 0 {
		return ErrInvalid
	}
	// Ingest takes only the visitor lock, not the Store gate, so a visit does
	// not queue behind the XLog/profile writers' gate holds under the engine
	// lock.
	if err := s.lockVisitor(ctx); err != nil {
		return err
	}
	err := s.putVisitorLocked(ctx, now, objType, userID, objHash)
	flush := !s.config.DeferVisitorFlush && s.visitor != nil && s.visitor.flushDue
	if flush {
		s.visitor.flushDue = false
	}
	s.visitorMu.Unlock()
	if flush {
		err = errors.Join(err, s.flushVisitors(ctx))
	}
	return err
}

// FlushVisitorsIfDue writes visitor snapshots once the 10s tick has passed. It
// is the DeferVisitorFlush owner's entry point and never holds the gate during I/O.
func (s *Store) FlushVisitorsIfDue(ctx context.Context) error {
	if err := s.lockVisitor(ctx); err != nil {
		return err
	}
	s.applyVisitorBudgetsLocked()
	due := !s.visitorClosed && s.visitor != nil && s.visitor.flushDue
	if due {
		s.visitor.flushDue = false
	}
	s.visitorMu.Unlock()
	if !due {
		return nil
	}
	return s.flushVisitors(ctx)
}
func (s *Store) putVisitorLocked(ctx context.Context, now int64, objType string, userID int64, objHash int32) error {
	if s.visitorClosed {
		return ErrClosed
	}
	if userID == 0 {
		return nil
	}
	if err := s.visitorTickLocked(ctx, now); err != nil {
		return err
	}
	v := s.visitor
	names := v.objectNames(objHash)
	// The three offers are independent: one refused (a symlinked path, a
	// victim that cannot be written) does not drop the others.
	err := errors.Join(
		s.offerVisitorLocked(ctx, false, &v.typeQ, v.typeKey(objType), objType, v.typesMax, userID),
		s.offerVisitorLocked(ctx, false, &v.objectQ, names.key, names.name, v.objectsMax, userID))
	if s.live.visitorHourlyOff.Load() {
		return errors.Join(err, ctx.Err())
	}
	return errors.Join(err, s.offerVisitorLocked(ctx, true, &v.hourQ, names.hourlyName, names.hourlyLoadName, v.objectsMax, userID))
}

// offerVisitorLocked counts one visit in memory. A new entry is partial and
// reads no file; only past a cap (eviction) or past deltaMax (write-through)
// does the visit write a snapshot here.
func (s *Store) offerVisitorLocked(ctx context.Context, hourly bool, q *visitorFIFO, key, loadName string, max int, userID int64) error {
	v := s.visitor
	entries := v.daily
	if hourly {
		entries = v.hourly
	}
	e := entries[key]
	if e == nil {
		failed := 0
		for skipped := 0; q.len() >= max; {
			r := q.pop()
			old := entries[r.key]
			if old == nil || old.sequence != r.sequence {
				continue // left the set (purge, delete)
			}
			if old.writes != 0 {
				// A flush pass is writing it and may need to put back a
				// failed write: the next oldest goes instead.
				q.push(r)
				if skipped++; skipped > q.len() {
					break // every entry is being written: over the cap until then
				}
				continue
			}
			if old.dirty {
				// Ordered with flushVisitors, which writes outside visitorMu.
				s.visitorIO.Lock()
				err := s.writeVisitorNowLocked(ctx, s.visitorWorkLocked(hourly, r.key, old))
				s.visitorIO.Unlock()
				if err != nil {
					// Kept (registers restored), retried as a later victim;
					// the next oldest is tried, up to three victims.
					q.push(r)
					if failed++; failed == 3 || ctx.Err() != nil {
						return err
					}
					continue
				}
			}
			if hourly && old.synced {
				// Its file now holds the hour's visits so far.
				v.markHourWritten(r.key)
			}
			v.unaccount(old)
			delete(entries, r.key)
			v.dailyLen, v.hourlyLen = len(v.daily), len(v.hourly)
		}
		// The path is checked once per entry, as the miss load did: a
		// symlinked date or directory rejects the visit, not only the flush.
		dir, name := "visit", key[2:]
		if hourly {
			dir, name = "visit_hourly", key
		}
		if err := s.walkSafePath(s.path(v.date, dir, name+".usr")); err != nil {
			return err
		}
		v.sequence++
		e = &visitorEntry{hll: &visitorHLL{}, partial: true, sequence: v.sequence}
		// A daily entry loads and saves the same name, so its file is the
		// base; an hourly one loads the double suffix name (see objectNames).
		// An hourly key evicted after a write this hour merges into that
		// file: replacing it from the load path would drop the hour's visits
		// (Java loses them on eviction anyway).
		if hourly {
			e.load = loadName
			_, e.synced = v.hourWritten[key]
		} else {
			e.synced = true
		}
		entries[key] = e
		q.push(visitorRef{key, e.sequence})
		v.dailyLen, v.hourlyLen = len(v.daily), len(v.hourly)
	}
	v.uses++
	e.used = v.uses
	// Java marks the HLL dirty on every visit (VisitorDB.getNewObject); a
	// visit that changes no register would rewrite identical bytes, so only a
	// changed register marks it dirty.
	if !e.hll.offer(userID) {
		return ctx.Err()
	}
	e.dirty = true
	v.account(e)
	if !e.partial && v.residentBytes > v.residentMax {
		// A resident HLL grew (densified) past the budget: demote now, in
		// memory, rather than wait for a pass that may not keep up. While
		// entries wait for a pass, only this one goes, if its delta fits.
		if !v.demoteWait {
			v.flushDue = true
			s.demoteVisitorsLocked(true)
		} else if e.writes == 0 && v.deltaBytes+e.bytes <= v.deltaMax {
			v.demoteLocked(e)
		}
	}
	if e.partial && v.deltaBytes > v.deltaMax {
		v.flushDue = true
		v.writeThroughs++
		s.visitorIO.Lock()
		err := s.writeVisitorNowLocked(ctx, s.visitorWorkLocked(hourly, key, e))
		s.visitorIO.Unlock()
		if err != nil {
			// The visit is kept (registers restored for the next write), so
			// it is not an error of this visit: the next pass reports it.
			v.writeFailures++
			if v.writeErr == nil {
				v.writeErr = err
			}
		}
	}
	return ctx.Err()
}

func (v *visitorState) markHourWritten(key string) {
	if v.hourWritten == nil {
		v.hourWritten = map[string]struct{}{}
	}
	if len(v.hourWritten) < v.objectsMax {
		v.hourWritten[key] = struct{}{}
	}
}
func (s *Store) VisitorRealtimeContext(ctx context.Context, now int64, hashes []int32, objType string) (int64, error) {
	return s.visitorQuery(ctx, now, JavaDate(now, s.config.Location), "visit", hashes, objType, -1, true)
}
func (s *Store) VisitorDailyContext(ctx context.Context, date string, hashes []int32, objType string) (int64, error) {
	if err := validDate(date); err != nil {
		return 0, err
	}
	return s.visitorQuery(ctx, 0, date, "visit", hashes, objType, -1, false)
}
func (s *Store) VisitorHourlyContext(ctx context.Context, date string, tm int64, hashes []int32) (int64, error) {
	if err := validDate(date); err != nil {
		return 0, err
	}
	return s.visitorQuery(ctx, 0, date, "visit_hourly", hashes, "", JavaMillis(tm, s.config.Location)/3600000, false)
}

// visitorQuery merges the in-memory HLLs (realtime) under visitorMu, then
// reads the files without it, each under visitorIO: a query of many objects
// (up to ~66MiB of files) does not stall the visits and the flush pass, and
// never reads a file a write is changing. A partial entry's delta is merged
// first; a flush that takes it meanwhile writes it to the file before the
// read (max merges are idempotent).
func (s *Store) visitorQuery(ctx context.Context, now int64, date, dir string, hashes []int32, objType string, hour int64, realtime bool) (int64, error) {
	if len(hashes) > s.config.MaxQueryRecords || len(hashes) > s.config.MaxQueryBytes/visitorFileBytes {
		return 0, ErrLimit
	}
	names := make([]string, 0, len(hashes))
	if objType != "" {
		if err := aggregationName(objType); err != nil {
			return 0, err
		}
		names = append(names, objType)
	} else {
		for _, h := range hashes {
			name := aggregationHash(h)
			if hour >= 0 {
				name += "_" + strconv.FormatInt(hour, 10)
			}
			names = append(names, name)
		}
	}
	if err := s.lockVisitor(ctx); err != nil {
		return 0, err
	}
	if s.visitorClosed {
		s.visitorMu.Unlock()
		return 0, ErrClosed
	}
	var total *visitorHLL
	add := func(h *visitorHLL) {
		if total == nil {
			total = newVisitorHLL()
		}
		total.merge(h)
	}
	load := names
	if realtime {
		s.visitorInit(now)
		load = nil
		for _, name := range names {
			var e *visitorEntry
			if s.visitor.date == date {
				key := "h:" + name
				if objType != "" {
					key = "t:" + name
				}
				e = s.visitor.daily[key]
			}
			if e == nil || e.partial {
				load = append(load, name)
			}
			if e != nil {
				add(e.hll)
			}
		}
	}
	s.visitorMu.Unlock()
	for _, name := range load {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		s.visitorIO.Lock()
		h, err := s.loadVisitorLocked(ctx, filepath.Join(s.config.Directory, date, dir, name+".usr"))
		s.visitorIO.Unlock()
		if err != nil {
			return 0, err
		}
		if h != nil {
			add(h)
		}
	}
	if total == nil {
		return 0, ctx.Err()
	}
	return total.cardinality(), ctx.Err()
}

// VisitorRollContext is the visitor half of the engine tick (date/hour
// roll, 10s flush mark) under the visitor lock only, without the Store gate:
// the ingest visitor writer runs it in order with the visits it offers, so a
// long gate hold does not stall the visits queued behind it.
func (s *Store) VisitorRollContext(ctx context.Context, now int64) error {
	if err := s.lockVisitor(ctx); err != nil {
		return err
	}
	defer s.visitorMu.Unlock()
	if s.visitorClosed {
		return ErrClosed
	}
	return s.visitorTickLocked(ctx, now)
}

// TagTickContext is the tag count half of the engine tick (flush and top100
// rebuild marks), under the Store gate. Its order with the visits
// does not matter: it only compares now with the last marks.
func (s *Store) TagTickContext(ctx context.Context, now int64) error {
	if err := s.mu.LockContext(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.tagTickLocked(ctx, now)
}
