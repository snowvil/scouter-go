package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

// Clean-shutdown marker.
//
// Store.Close, after every cached index and data file was fsynced and closed
// without error, atomically writes cleanMarkerName in the DB root. It lists a
// legacyindex.Seal (sizes, mtimes, header checksum) per index pair the Store
// closed cleanly. Store.Open reads the marker into memory and removes it
// (with a directory fsync) before anything else touches the DB, so a crash of
// this run leaves no marker and the next run validates everything.
//
// index() opens a pair through legacyindex.OpenCheckpointed with its Seal: a
// pair still exactly as sealed skips the O(records) validation and recovery
// walk (the restart ingest stall); anything else - no marker, a pair changed
// after the marker (Go crash, Java or a tool wrote it), a truncated or
// damaged pair, a size or header mismatch - is validated as without the
// marker (in full, or after its crash checkpoint, below). Seals of
// pairs this run did not open are carried into the next marker while the files
// still match them, so a clean chain of restarts keeps them; a pair that a
// crashed run left behind is never sealed because that run wrote no marker.
//
// Scope: Store index pairs (XLog, profile, text, object, status, summary,
// alert, span), KV TTL pairs (.k2file, also owned by the Store) and the
// counter pairs (real/5m) of the Counters attached with AttachCounters. The
// Store hands the counter seals to the Counters at AttachCounters and collects
// the seals of a Counters closed cleanly before the Store (the runtime order)
// at Store.Close. Counters that are not attached, or not closed before the
// Store, simply get no seals (their pairs are fully validated). The writer
// locks (.go-writer.lock flock, lock.dat) are unchanged and are held by the
// runtime until after Store.Close returns.
const (
	cleanMarkerName     = ".go-clean"
	cleanMarkerFormat   = 1
	cleanMarkerWriter   = "scouter-go"
	maxCleanSeals       = 1024
	maxCleanMarkerBytes = 1 << 20
)

type cleanMarker struct {
	Format   int    `json:"format"`
	Writer   string `json:"writer"`
	ClosedAt int64  `json:"closed_at_ns"`
	Closed   string `json:"closed_at"` // RFC 3339, informational
	// Indexes are keyed by slash paths below the root, without .kfile/.hfile.
	Indexes map[string]legacyindex.Seal `json:"indexes"`
}

// cleanState is what Open learned from the marker; guarded by Store.mu.
type cleanState struct {
	found    bool
	closedAt time.Time
	seals    map[string]legacyindex.Seal // absolute index path -> seal
}

// consumeCleanMarker reads and removes the marker. A malformed marker is
// removed and ignored (everything is validated). Failing to remove it is an
// error: the Store must not run while a stale marker could outlive a crash.
func consumeCleanMarker(root string) (cleanState, error) {
	name := filepath.Join(root, cleanMarkerName)
	info, err := os.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return cleanState{}, nil
	}
	if err != nil {
		return cleanState{}, err
	}
	state := cleanState{}
	if info.Mode().IsRegular() {
		state = readCleanMarker(root, name)
	}
	if err = os.Remove(name); err != nil {
		return cleanState{}, fmt.Errorf("remove clean shutdown marker: %w", err)
	}
	if err = syncDirectory(root); err != nil {
		return cleanState{}, fmt.Errorf("remove clean shutdown marker: %w", err)
	}
	return state, nil
}

func readCleanMarker(root, name string) cleanState {
	f, err := os.Open(name)
	if err != nil {
		return cleanState{}
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxCleanMarkerBytes+1))
	if err != nil || len(b) > maxCleanMarkerBytes {
		return cleanState{}
	}
	var m cleanMarker
	if json.Unmarshal(b, &m) != nil || m.Format != cleanMarkerFormat || m.Writer != cleanMarkerWriter || m.ClosedAt <= 0 || len(m.Indexes) > maxCleanSeals {
		return cleanState{}
	}
	state := cleanState{found: true, closedAt: time.Unix(0, m.ClosedAt), seals: make(map[string]legacyindex.Seal, len(m.Indexes))}
	for rel, seal := range m.Indexes {
		local := filepath.FromSlash(rel)
		if !filepath.IsLocal(local) || filepath.ToSlash(filepath.Clean(local)) != rel {
			continue
		}
		// A file cannot have been sealed after the marker was written.
		if seal.LatestMTime().After(state.closedAt) {
			continue
		}
		state.seals[filepath.Join(root, local)] = seal
	}
	return state
}

// takeSeal removes and returns the seal of an index path; every open consumes
// it, because the opened pair is written from now on.
func (c *cleanState) takeSeal(path string) *legacyindex.Seal { return takeSeal(c.seals, path) }

func takeSeal(seals map[string]legacyindex.Seal, path string) *legacyindex.Seal {
	seal, ok := seals[path]
	if !ok {
		return nil
	}
	delete(seals, path)
	return &seal
}

// isCounterIndex reports the {date}/counter/{real,5m} pairs owned by Counters;
// no Store index lives in a "counter" directory.
func isCounterIndex(path string) bool { return filepath.Base(filepath.Dir(path)) == "counter" }

// handCounterSealsLocked moves the counter seals to c (AttachCounters). Lock
// order Store.mu then Counters.mu, as retention and DB delete.
func (s *Store) handCounterSealsLocked(c *Counters) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seals == nil {
		c.seals = make(map[string]legacyindex.Seal)
	}
	for path, seal := range s.clean.seals {
		if isCounterIndex(path) {
			c.seals[path] = seal
			delete(s.clean.seals, path)
		}
	}
}

// counterSealsLocked returns the seals the attached Counters contribute to the
// marker: unopened carried seals, plus the pairs of a successful Close.
func (s *Store) counterSealsLocked() (carried, sealed map[string]legacyindex.Seal) {
	c := s.counters
	if c == nil {
		return map[string]legacyindex.Seal{}, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	carried = make(map[string]legacyindex.Seal, len(c.seals))
	for path, seal := range c.seals {
		carried[path] = seal
	}
	if c.closed {
		return carried, c.closedSeals
	}
	return carried, nil
}

// keyFileOf is the existing key file of an index path (.kfile or TTL .k2file).
func keyFileOf(path string) string {
	if _, err := os.Stat(path + ".kfile"); err == nil {
		return path + ".kfile"
	}
	return path + ".k2file"
}

// writeCleanMarker writes the marker for sealed (absolute index path -> seal)
// plus the carried seals whose files still match. It replaces the file
// atomically and fsyncs the directory.
func writeCleanMarker(root string, carried, sealed map[string]legacyindex.Seal, now time.Time) error {
	all := make(map[string]legacyindex.Seal, len(carried)+len(sealed))
	for path, seal := range carried {
		ks, kerr := os.Stat(keyFileOf(path))
		hs, herr := os.Stat(path + ".hfile")
		if kerr == nil && herr == nil && seal.Matches(ks, hs) {
			all[path] = seal
		}
	}
	for path, seal := range sealed {
		all[path] = seal
	}
	paths := make([]string, 0, len(all))
	for path := range all {
		paths = append(paths, path)
	}
	// Bounded: keep the most recently written pairs; dropping a seal only
	// costs a full validation later.
	sort.Slice(paths, func(a, b int) bool {
		ta, tb := all[paths[a]].LatestMTime(), all[paths[b]].LatestMTime()
		if !ta.Equal(tb) {
			return ta.After(tb)
		}
		return paths[a] < paths[b]
	})
	m := cleanMarker{Format: cleanMarkerFormat, Writer: cleanMarkerWriter, ClosedAt: now.UnixNano(), Closed: now.Format(time.RFC3339Nano), Indexes: make(map[string]legacyindex.Seal)}
	for _, path := range paths {
		if len(m.Indexes) == maxCleanSeals {
			break
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || !filepath.IsLocal(rel) {
			continue
		}
		seal := all[path]
		if seal.LatestMTime().After(now) { // clock stepped back: never seal ahead of the marker
			continue
		}
		m.Indexes[filepath.ToSlash(rel)] = seal
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return replaceFile(root, cleanMarkerName, b)
}

func syncDirectory(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

// Crash checkpoints.
//
// The clean marker only helps after a clean Close. After a crash (SIGKILL,
// OOM, a hung stop finished by SIGKILL) every index of the day used to be
// walked record by record when it was first opened, under the Store gate,
// stalling ingest for tens of seconds on a large day. A checkpoint log keeps,
// per index pair, the legacyindex.Checkpoint of the last durable state the
// owner established: key bytes fsynced, header written and fsynced. Opening a
// pair whose header is still the checkpoint's walks only the records appended
// after it (legacyindex.OpenCheckpointed, CheckTail); anything else, or a
// tail that the full walk might judge differently, is validated in full.
//
// Order: SyncData commits every header (key fsync, header write, header
// fsync) and only then persists the log, so an entry never describes a state
// that is not on disk. Store.Close and the closes of evicted handles record
// the state their final sync established; eviction re-opens in the same run
// use the in-memory entry. The log file is replaced atomically (temp file,
// fsync, rename, directory fsync), like the clean marker. A missing, damaged
// or foreign file is ignored (everything is fully validated) and replaced by
// the next persist. Unlike the clean marker it is not consumed by Open: its
// entries stay true across crashes until a header changes, which the header
// sum detects.
//
// Files: checkpointName for the Store (index and KV pairs) and
// counterCheckpointName for Counters, both in the DB root, so the two owners
// never share a file or a lock. Java ignores both (it only lists date
// directories in the DB root); Go retention and DB delete only touch date
// directories; migrate never copies or compares them (runtime state).
const (
	checkpointName        = ".go-checkpoint"
	counterCheckpointName = ".go-checkpoint-counter"
	checkpointFormat      = 1
	maxCheckpoints        = 4096
	maxCheckpointBytes    = 4 << 20
)

type checkpointFile struct {
	Format    int    `json:"format"`
	Writer    string `json:"writer"`
	WrittenAt string `json:"written_at"` // RFC 3339, informational
	// Indexes are keyed by slash paths below the root, without suffixes.
	Indexes map[string]checkpointEntry `json:"indexes"`
}

type checkpointEntry struct {
	legacyindex.Checkpoint
	// Seq orders entries by update (the oldest are dropped beyond the bound).
	Seq uint64 `json:"seq"`
}

// checkpointLog owns one checkpoint file. mu guards entries, seq and changed
// and is never held across I/O except by persist's file write, which io
// serializes (one persist at a time). Lock order: owner gate, then mu.
type checkpointLog struct {
	mu      sync.Mutex
	io      sync.Mutex
	root    string
	name    string
	entries map[string]checkpointEntry // absolute index path -> entry
	seq     uint64
	changed bool
	// writeHook, when set, runs before the file is written (tests).
	writeHook func() error
}

// loadCheckpoints reads the log of root. Entries whose key file no longer
// exists (purged dates) are dropped; any read or format problem yields an
// empty log, never an error: the log only ever saves work.
func loadCheckpoints(root, name string) *checkpointLog {
	l := &checkpointLog{root: root, name: name, entries: map[string]checkpointEntry{}}
	f, err := os.Open(filepath.Join(root, name))
	if err != nil {
		return l
	}
	defer f.Close()
	if info, err := f.Stat(); err != nil || !info.Mode().IsRegular() {
		return l
	}
	b, err := io.ReadAll(io.LimitReader(f, maxCheckpointBytes+1))
	if err != nil || len(b) > maxCheckpointBytes {
		return l
	}
	var m checkpointFile
	if json.Unmarshal(b, &m) != nil || m.Format != checkpointFormat || m.Writer != cleanMarkerWriter || len(m.Indexes) > maxCheckpoints {
		return l
	}
	for rel, e := range m.Indexes {
		local := filepath.FromSlash(rel)
		if !filepath.IsLocal(local) || filepath.ToSlash(filepath.Clean(local)) != rel {
			continue
		}
		path := filepath.Join(root, local)
		if _, err := os.Stat(keyFileOf(path)); err != nil {
			l.changed = true
			continue
		}
		l.entries[path] = e
		l.seq = max(l.seq, e.Seq)
	}
	return l
}

// get returns the checkpoint of an index path, or nil.
func (l *checkpointLog) get(path string) *legacyindex.Checkpoint {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.entries[path]
	if !ok {
		return nil
	}
	cp := e.Checkpoint
	return &cp
}

func (l *checkpointLog) set(path string, cp legacyindex.Checkpoint) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.entries[path]; ok && e.Checkpoint == cp {
		return
	}
	l.seq++
	l.entries[path] = checkpointEntry{Checkpoint: cp, Seq: l.seq}
	l.changed = true
}

// record sets the checkpoint a handle established, if any.
func (l *checkpointLog) record(path string, cp legacyindex.Checkpoint, ok bool) {
	if ok {
		l.set(path, cp)
	}
}

// drop forgets a path (its pair was created anew).
func (l *checkpointLog) drop(path string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.entries[path]; ok {
		delete(l.entries, path)
		l.changed = true
	}
}

// forget drops the entries of every index pair at or below root (absolute):
// root is the pair's base path, a directory above it, or one of its files.
func (l *checkpointLog) forget(root string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for path := range l.entries {
		if checkpointCovered(path, root) {
			delete(l.entries, path)
			l.changed = true
		}
	}
}

func checkpointCovered(index, root string) bool {
	for _, p := range []string{index, index + ".kfile", index + ".k2file", index + ".hfile"} {
		if p == root || strings.HasPrefix(p, root+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// checkpointRoot resolves a purge/delete root against the DB root.
func checkpointRoot(dbRoot, root string) string {
	if !filepath.IsAbs(root) {
		root = filepath.Join(dbRoot, root)
	}
	return filepath.Clean(root)
}

// forgetCheckpointsLocked drops the crash checkpoints of every Store index or
// KV pair at or below root, so a purged or deleted path is not rewritten into
// the log by every later persist (the next persist writes the log without
// them). root is what purge and DB delete detach: an absolute path below the
// DB root, as retention builds it (filepath.Join(Directory, date, target):
// a date directory for PurgeAll, a kind directory such as {date}/xlog, or one
// file such as {date}/xlog/xlog_profile.kfile) and DB delete builds it
// (deleteTarget.path). A relative root is taken relative to the DB root.
// Index paths are keyed without .kfile/.k2file/.hfile; a root naming one of
// those files drops that pair. Caller holds s.mu (the detach section), which
// also orders it against SyncData, which records only handles still cached.
func (s *Store) forgetCheckpointsLocked(root string) {
	s.checkpoints.forget(checkpointRoot(s.config.Directory, root))
}

// forgetCheckpointsLocked is Store.forgetCheckpointsLocked for the counter
// pairs ({date}/counter/real, 5m), with root relative to the Counters root
// when not absolute. Counter pairs are keyed filepath.Join(Counters root,
// ...), so an absolute root matches when the Counters were opened with an
// absolute root (the runtime passes the Store's directory). Caller holds c.mu.
func (c *Counters) forgetCheckpointsLocked(root string) {
	c.checkpoints.forget(checkpointRoot(c.root, root))
}

// persist writes the log when it changed since the last write. Callers run it
// only after the headers its entries describe were fsynced.
func (l *checkpointLog) persist() error {
	if l == nil {
		return nil
	}
	l.io.Lock()
	defer l.io.Unlock()
	l.mu.Lock()
	if !l.changed {
		l.mu.Unlock()
		return nil
	}
	paths := make([]string, 0, len(l.entries))
	for path := range l.entries {
		paths = append(paths, path)
	}
	// Bounded: keep the most recently updated entries; a dropped entry only
	// costs a full validation after a crash.
	sort.Slice(paths, func(a, b int) bool { return l.entries[paths[a]].Seq > l.entries[paths[b]].Seq })
	if len(paths) > maxCheckpoints {
		for _, path := range paths[maxCheckpoints:] {
			delete(l.entries, path)
		}
		paths = paths[:maxCheckpoints]
	}
	now := time.Now()
	m := checkpointFile{Format: checkpointFormat, Writer: cleanMarkerWriter, WrittenAt: now.Format(time.RFC3339Nano), Indexes: make(map[string]checkpointEntry, len(paths))}
	for _, path := range paths {
		rel, err := filepath.Rel(l.root, path)
		if err != nil || !filepath.IsLocal(rel) {
			continue
		}
		m.Indexes[filepath.ToSlash(rel)] = l.entries[path]
	}
	l.changed = false
	l.mu.Unlock()
	err := l.write(m)
	if err != nil {
		l.mu.Lock()
		l.changed = true
		l.mu.Unlock()
		return fmt.Errorf("index checkpoint log %s (crash restarts validate fully): %w", l.name, err)
	}
	return nil
}

func (l *checkpointLog) write(m checkpointFile) error {
	if l.writeHook != nil {
		if err := l.writeHook(); err != nil {
			return err
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return replaceFile(l.root, l.name, b)
}

// replaceFile atomically replaces root/name with b and fsyncs the directory.
func replaceFile(root, name string, b []byte) error {
	f, err := os.CreateTemp(root, ".scouter-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp, filepath.Join(root, name))
	}
	if err == nil {
		err = syncDirectory(root)
	}
	return err
}
