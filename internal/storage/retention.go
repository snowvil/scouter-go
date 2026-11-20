package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// PurgeMode is one AutoDeleteScheduler deletion scope. See
// docs/compatibility/recovery-retention.md for the pinned Java source.
type PurgeMode string

const (
	PurgeProfile         PurgeMode = "PROFILE"
	PurgeXLog            PurgeMode = "XLOG"
	PurgeTag             PurgeMode = "TAG"
	PurgeText            PurgeMode = "TEXT"
	PurgeSum             PurgeMode = "SUM"
	PurgeVisitor         PurgeMode = "VISITOR"
	PurgeRealtimeCounter PurgeMode = "REAL_COUNTER"
	PurgeAll             PurgeMode = "ALL"
)

// purgeTargets mirrors AutoDeleteScheduler.deleteData. Compressed profiles share
// xlog/data and, like Java, survive PROFILE mode; real.head and visit_hourly remain.
func purgeTargets(mode PurgeMode) ([]string, error) {
	switch mode {
	case PurgeAll:
		return []string{"."}, nil
	case PurgeXLog:
		return []string{"xlog", "span"}, nil
	case PurgeProfile:
		return []string{"xlog/xlog_profile.hfile", "xlog/xlog_profile.kfile", "xlog/xlog.profile"}, nil
	case PurgeVisitor:
		return []string{"visit"}, nil
	case PurgeTag:
		return []string{"tagcnt"}, nil
	case PurgeText:
		return []string{"text"}, nil
	case PurgeSum:
		return []string{"sum"}, nil
	case PurgeRealtimeCounter:
		return []string{"counter/real.data", "counter/real.hfile", "counter/real.kfile"}, nil
	}
	return nil, fmt.Errorf("purge mode %q: %w", mode, ErrInvalid)
}

// RetentionConfig holds effective day limits; zero or negative disables a mode.
type RetentionConfig struct {
	ProfileDays, XLogDays, CounterDays        int
	RealtimeCounterDays, TagDays, VisitorDays int
	TextDays, SumDays                         int
	DiskUsagePct                              int
}

// JavaRetentionDefaults is JavaRetentionConfig with no key set.
func JavaRetentionDefaults() RetentionConfig { return JavaRetentionConfig(nil) }

// JavaRetentionConfig applies Configure.apply() (Configure.java:772-792) to the
// mgr_purge_* keys present in set; absent keys use the Java derivation.
func JavaRetentionConfig(set map[string]int) RetentionConfig {
	get := func(key string, fallback int) int {
		if v, ok := set[key]; ok {
			return v
		}
		return fallback
	}
	keep := get("mgr_purge_keep_days", 10)
	var c RetentionConfig
	c.DiskUsagePct = get("mgr_purge_disk_usage_pct", 80)
	if c.ProfileDays = get("mgr_purge_profile_keep_days", 10); c.ProfileDays == 0 {
		c.ProfileDays = keep
	}
	withoutProfile := get("mgr_purge_xlog_without_profile_keep_days", c.ProfileDays*3)
	if c.XLogDays = get("mgr_purge_xlog_keep_days", c.ProfileDays*3); c.XLogDays == 0 {
		c.XLogDays = withoutProfile
	}
	c.CounterDays = get("mgr_purge_counter_keep_days", keep*7)
	c.RealtimeCounterDays = get("mgr_purge_realtime_counter_keep_days", c.CounterDays)
	c.TagDays = get("mgr_purge_tag_counter_keep_days", c.CounterDays)
	c.VisitorDays = get("mgr_purge_visitor_counter_keep_days", c.CounterDays)
	c.TextDays = get("mgr_purge_daily_text_days", max(c.TagDays*2, c.XLogDays*2))
	c.SumDays = get("mgr_purge_sum_data_days", 60)
	return c
}

// JavaRetentionKeys are the mgr_purge_* integer keys accepted by JavaRetentionConfig.
var JavaRetentionKeys = []string{"mgr_purge_disk_usage_pct", "mgr_purge_keep_days", "mgr_purge_profile_keep_days", "mgr_purge_xlog_keep_days", "mgr_purge_xlog_without_profile_keep_days", "mgr_purge_counter_keep_days", "mgr_purge_realtime_counter_keep_days", "mgr_purge_tag_counter_keep_days", "mgr_purge_visitor_counter_keep_days", "mgr_purge_daily_text_days", "mgr_purge_sum_data_days"}

// Retention deletes old date directories while holding the Store and Counters
// gates, so cached handles of the target date are synced and closed first.
type Retention struct {
	Store    *Store
	Counters *Counters
	// Config is used, enabled, until the first Apply.
	Config RetentionConfig
	// DiskUsage returns used percent of the filesystem holding root; nil uses statfs.
	DiskUsage func(root string) (float64, error)

	mu      sync.Mutex
	applied bool
	enabled bool
	live    RetentionConfig
	// quarantined counts targets moved to a quarantine name because the
	// trash was unusable; stuck counts targets no rename could move (left in
	// place, reported, retried next cycle). See DetachStats.
	quarantined, stuck atomic.Uint64
}

// DetachStats returns the targets purged through a quarantine name (the
// trash was unusable) and the targets left in place because neither rename
// worked, since the Retention was created.
func (r *Retention) DetachStats() (quarantined, stuck uint64) {
	return r.quarantined.Load(), r.stuck.Load()
}

// Apply sets mgr_purge_enabled and the effective mgr_purge_* limits (build
// them with JavaRetentionConfig from the keys present, which recomputes the
// derived defaults like Configure.apply on every reload). Safe from any
// goroutine; the next RunOnce uses them, as Java's 1-minute loop reads conf
// directly. It reports whether anything differs from the settings in force.
//
// Java AutoDeleteScheduler's ConfObserver also clears its deleted-date sets
// when a value changes so that already handled dates are considered again.
// Go keeps no such history: every RunOnce lists the dates on disk again, so
// nothing needs clearing and the outcome is the same set of removed files.
func (r *Retention) Apply(enabled bool, c RetentionConfig) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	oldEnabled, old := r.settingsLocked()
	r.applied, r.enabled, r.live = true, enabled, c
	return oldEnabled != enabled || old != c
}

// Settings returns the settings RunOnce would use now.
func (r *Retention) Settings() (enabled bool, c RetentionConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.settingsLocked()
}
func (r *Retention) settingsLocked() (bool, RetentionConfig) {
	if !r.applied {
		return true, r.Config
	}
	return r.enabled, r.live
}

// PurgeAction records one deletion attempt. Err is non-empty when removal failed.
type PurgeAction struct {
	Date   string    `json:"date"`
	Mode   PurgeMode `json:"mode"`
	Reason string    `json:"reason"`
	Err    string    `json:"error,omitempty"`
	// Gates is set when the purge waited for or held its gates for
	// detachSlow or longer: which gate was busy, for how long, and how long
	// the gates were then held (the time ingest could not take them).
	Gates string `json:"gates,omitempty"`
}

// RunOnce applies one AutoDeleteScheduler cycle at now. Unlike Java, deletion
// history is recomputed every cycle, so a resurrected date is purged again and a
// failed removal is retried; failures are returned instead of logged as success.
func (r *Retention) RunOnce(ctx context.Context, now time.Time) ([]PurgeAction, error) {
	if r.Store == nil {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	enabled, c := r.Settings()
	if !enabled {
		return nil, nil // Java: if (conf.mgr_purge_enabled) { ... }
	}
	loc := r.Store.config.Location
	ms := now.UnixMilli()
	today := JavaDate(ms, loc)
	var actions []PurgeAction
	var errs []error
	if err := r.cleanTrash(); err != nil {
		errs = append(errs, err)
	}
	run := func(date string, mode PurgeMode, reason string) {
		a := PurgeAction{Date: date, Mode: mode, Reason: reason}
		removed, gates, err := r.purgeTimed(ctx, date, today, mode)
		a.Gates = gates
		if err != nil {
			a.Err = err.Error()
			errs = append(errs, err)
		}
		if removed || err != nil {
			actions = append(actions, a)
		}
	}
	if err := r.diskPurge(ctx, c.DiskUsagePct, today, run); err != nil {
		errs = append(errs, err)
	}
	textDays := max(c.TextDays, max(c.TagDays, c.XLogDays))
	for _, step := range []struct {
		mode PurgeMode
		days int
	}{{PurgeProfile, c.ProfileDays}, {PurgeXLog, c.XLogDays}, {PurgeTag, c.TagDays}, {PurgeText, textDays}, {PurgeSum, c.SumDays}, {PurgeVisitor, c.VisitorDays}, {PurgeRealtimeCounter, c.RealtimeCounterDays}, {PurgeAll, c.CounterDays}} {
		if step.days <= 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return actions, err
		}
		limit := JavaDate(ms-int64(step.days)*86400000, loc)
		dates, err := r.dates(today)
		if err != nil {
			return actions, errors.Join(append(errs, err)...)
		}
		for _, d := range dates {
			if d > limit {
				break
			}
			run(d, step.mode, fmt.Sprintf("day limit %d", step.days))
		}
	}
	return actions, errors.Join(errs...)
}

func (r *Retention) diskPurge(ctx context.Context, pct int, today string, run func(string, PurgeMode, string)) error {
	if pct <= 0 {
		return nil
	}
	usage := r.DiskUsage
	if usage == nil {
		usage = diskUsedPercent
	}
	for _, step := range []struct {
		mode      PurgeMode
		threshold float64
	}{{PurgeProfile, float64(pct) - float64(100-pct)*0.4}, {PurgeXLog, float64(pct) - float64(100-pct)*0.2}, {PurgeAll, float64(pct)}} {
		dates, err := r.dates(today)
		if err != nil {
			return err
		}
		used, err := usage(r.Store.config.Directory)
		if err != nil {
			return err
		}
		// Each date is tried once per mode and cycle; a failed or ineffective
		// removal cannot loop forever while usage stays high.
		for _, d := range dates {
			if err = ctx.Err(); err != nil {
				return err
			}
			if used <= step.threshold {
				break
			}
			run(d, step.mode, fmt.Sprintf("disk used %.2f%% > %.2f%%", used, step.threshold))
			if used, err = usage(r.Store.config.Directory); err != nil {
				return err
			}
		}
	}
	return nil
}

// dates lists purge candidates oldest first. Java accepts any Integer.valueOf
// name without "0000"; Go only accepts valid yyyymmdd names before today so an
// unrelated numeric directory is never removed (intended defensive difference).
func (r *Retention) dates(today string) ([]string, error) {
	entries, err := os.ReadDir(r.Store.config.Directory)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || strings.Contains(name, "0000") || name >= today || validDate(name) != nil {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// purgeTrash is a hidden, non-date directory on the DB filesystem. Targets are
// renamed into it under the storage gates and removed after the gates are
// released, so a large unlink never stalls ingest (Java deletes without locks).
const purgeTrash = ".purge-trash"

// quarantinePrefix names a target renamed next to itself (same directory)
// when the trash cannot take it: the trash directory cannot be created
// (ENOSPC, a file in the way) or lies on another filesystem (EXDEV). It is
// hidden and not a date, so no Store path, retention date or DB delete name
// can reach it; it is unlinked after the gates like a trash entry, and
// cleanTrash retries leftovers.
const quarantinePrefix = ".purge-quarantine-"

// Test hooks: the rename and the unlink of a purge or DB delete.
var (
	purgeRename    = os.Rename
	purgeRemoveAll = os.RemoveAll
)

// moveAway renames p out of the DB tree under the gates: into the trash, or
// when that fails to a quarantine name in p's own directory. Both are
// metadata-only. It never unlinks anything: the caller removes the returned
// path after releasing the gates. An error leaves p in place (the target
// is reported and retried; it is never removed in place under the gates).
func moveAway(trash string, trashErr error, p, name string) (dst string, quarantined bool, err error) {
	if trashErr == nil {
		dst = filepath.Join(trash, name)
		if err = renameOwned(p, dst); err == nil {
			return dst, false, nil
		}
	} else {
		err = trashErr
	}
	q := filepath.Join(filepath.Dir(p), quarantinePrefix+name)
	if qerr := renameOwned(p, q); qerr != nil {
		return "", false, fmt.Errorf("trash: %v; quarantine: %w", err, qerr)
	}
	return q, true, nil
}

// removing holds the trash and quarantine paths a purge or DB delete has
// renamed and is about to unlink itself (process-wide: the paths are
// absolute). cleanTrash, which may run at the same time from the retention
// loop, leaves them alone instead of racing the owner's RemoveAll. A path is
// registered before its rename, so a listing that sees it also sees this.
var removing sync.Map

func renameOwned(from, to string) error {
	removing.Store(to, struct{}{})
	if err := purgeRename(from, to); err != nil {
		removing.Delete(to)
		return err
	}
	return nil
}

// removeOwned unlinks a path moveAway returned; a failure leaves it to
// cleanTrash.
func removeOwned(p string) error {
	defer removing.Delete(p)
	return purgeRemoveAll(p)
}

// doomedHandle is a handle a purge or DB delete took out of its cache
// (releaseDoomedLocked and friends). What happens to it is decided once the
// renames are done (doomedCloses.settle).
type doomedHandle struct {
	files   []string     // the files it has open
	data    *os.File     // its data file, fsynced before an in-place close; nil for an index
	close   func() error // closes it (no fsync: the files are leaving the tree)
	restore func()       // puts it back into its cache
	q       *closeQueue
}

// doomedCloses collects the handles released for a purge or DB delete and
// settles them under the gates once the targets were renamed:
//   - all its files moved: a closed index fsyncs, so the close runs after the
//     gates (run, by the caller), or by the sync pass in flight
//     (closeQueue.release) so no handle closes under that pass's Commit;
//   - all its files stayed (no rename worked): the files and the handle are
//     unchanged, so it goes back into its cache; nothing is closed, and a
//     writer after the gates keeps appending through the same handle;
//   - some moved, some stayed (a target of several files): closed in place,
//     after every data file that stays was fsynced (data before header).
//
// Handles evicted earlier and still waiting for a sync pass (closeQueue
// pending, key = their path) are not re-adopted once their files moved: a
// purged path that is opened again must get the new files, not the old
// handle on the renamed ones.
type doomedCloses struct {
	handles []*doomedHandle
	later   []func() error
}

func (d *doomedCloses) take(q *closeQueue, h *doomedHandle) error {
	if d == nil { // test releases: at once, or by the sync pass in flight
		return q.release(&pendingClose{close: h.close})
	}
	h.q = q
	d.handles = append(d.handles, h)
	return nil
}

// settle decides the released handles (see doomedCloses) after the renames;
// moved and stayed are the target roots that left the tree and that stayed.
// The caller holds the gates of every queue involved.
func (d *doomedCloses) settle(moved, stayed []string) error {
	var errs []error
	var inPlace, kept []*doomedHandle
	for _, h := range d.handles {
		anyMoved, anyStayed := false, false
		for _, f := range h.files {
			anyMoved = anyMoved || under(f, moved)
			anyStayed = anyStayed || under(f, stayed)
		}
		switch {
		case anyMoved && !anyStayed:
			if h.q.syncing {
				errs = append(errs, h.q.release(&pendingClose{close: h.close}))
			} else {
				d.later = append(d.later, h.close)
			}
		case anyMoved:
			inPlace = append(inPlace, h)
		default:
			kept = append(kept, h)
		}
	}
	if len(inPlace) > 0 {
		// An index closed here writes and fsyncs its header in the tree:
		// every data file that stays (closed here or kept open) reaches
		// the disk first.
		for _, h := range append(inPlace, kept...) {
			if h.data != nil {
				errs = append(errs, h.q.syncFile(h.data))
			}
		}
		for _, h := range inPlace {
			errs = append(errs, h.close())
		}
	}
	for _, h := range kept {
		h.restore()
	}
	d.handles = nil
	return errors.Join(errs...)
}

// run closes the moved handles; the caller runs it after the gates.
func (d *doomedCloses) run() error {
	var errs []error
	for _, close := range d.later {
		errs = append(errs, close())
	}
	d.later = nil
	return errors.Join(errs...)
}

// forgetPendingLocked stops handles waiting in q for a sync pass from being
// re-adopted when their files moved away (closeQueue.adopt). files maps a
// pending key to its files. The handles are still closed by that pass.
func forgetPendingLocked(q *closeQueue, moved []string, files func(key string) []string) {
	if len(moved) == 0 {
		return
	}
	for _, p := range q.pending {
		if p.key == "" {
			continue
		}
		for _, f := range files(p.key) {
			if under(f, moved) {
				p.key, p.adopt = "", nil
				break
			}
		}
	}
}

// storeKeyFiles are the files of a Store cache key: a data file path, or an
// index path without its .kfile/.hfile (.k2file for KV) suffix.
func storeKeyFiles(key string) []string {
	return []string{key, key + ".kfile", key + ".hfile", key + ".k2file"}
}

// counterKeyFiles are the files of a Counters table key ("date/prefix").
func (c *Counters) counterKeyFiles(key string) []string {
	base, err := filepath.Abs(c.root)
	if err != nil {
		base = c.root
	}
	date, prefix, _ := strings.Cut(key, "/")
	p := filepath.Join(base, date, "counter", prefix)
	return []string{p + ".data", p + ".kfile", p + ".hfile", p + ".head"}
}

// settleDoomedLocked settles closes and forgets the pending evictions of
// moved targets, in the Store queue and, when given, the Counters queue.
// The caller holds both gates.
// The crash checkpoints of the pairs below the moved roots are dropped too,
// so no later persist writes them back for files that left the tree; a root
// that stayed keeps its files and their checkpoints.
func (s *Store) settleDoomedLocked(c *Counters, closes *doomedCloses, moved, stayed []string) error {
	forgetPendingLocked(&s.closes, moved, storeKeyFiles)
	if c != nil {
		forgetPendingLocked(&c.closes, moved, c.counterKeyFiles)
	}
	for _, root := range moved {
		s.forgetCheckpointsLocked(root)
		if c != nil {
			c.forgetCheckpointsLocked(root)
		}
	}
	return closes.settle(moved, stayed)
}

// purgeTimed removes one mode of one date. It reports removed=false when no
// target exists, so an already purged date is not re-reported every cycle,
// and the gate timing when it was slow (detachTiming.String).
func (r *Retention) purgeTimed(ctx context.Context, date, today string, mode PurgeMode) (removed bool, gates string, err error) {
	if err := validDate(date); err != nil {
		return false, "", err
	}
	if date >= today || strings.Contains(date, "0000") {
		return false, "", fmt.Errorf("refuse to purge current or reserved date %s: %w", date, ErrInvalid)
	}
	targets, err := purgeTargets(mode)
	if err != nil {
		return false, "", err
	}
	s := r.Store
	dayRoot := filepath.Join(s.config.Directory, date)
	var paths []string
	for _, t := range targets {
		p := filepath.Join(dayRoot, filepath.FromSlash(t))
		if _, err := os.Lstat(p); err == nil {
			paths = append(paths, p)
		} else if !os.IsNotExist(err) {
			return false, "", err
		}
	}
	if len(paths) == 0 {
		return false, "", ctx.Err()
	}
	trash := filepath.Join(s.config.Directory, purgeTrash)
	trashErr := os.MkdirAll(trash, 0700) // outside the gates
	var closes doomedCloses
	var timing detachTiming
	moved, attempted, err := r.detach(ctx, date, mode, paths, trash, trashErr, &closes, &timing)
	errs := []error{err}
	// Outside the gates: close the doomed handles, then unlink.
	if err := closes.run(); err != nil {
		errs = append(errs, fmt.Errorf("purge %s %s: close: %w", mode, date, err))
	}
	for _, p := range moved {
		if err := removeOwned(p); err != nil {
			errs = append(errs, fmt.Errorf("purge %s %s: trash cleanup: %w", mode, p, err))
		}
	}
	return attempted, timing.String(), errors.Join(errs...)
}

// detach releases handles and in-memory state below paths and moves them out of
// the DB tree while holding the Store and Counters gates (in that order). Close
// errors of the doomed handles are reported but never block reclaiming space.
// Nothing is unlinked here: a target is renamed into the trash, or next to
// itself when the trash is unusable (moveAway), and the caller removes the
// returned paths after the gates. A target neither rename can move (EBUSY
// for a mount point, a read-only parent) stays in place and is reported;
// Java would delete it in place, Go retries it next cycle instead of
// unlinking a large tree under the gates. The doomed handles are settled
// here (doomedCloses); the caller closes the moved ones after the gates.
func (r *Retention) detach(ctx context.Context, date string, mode PurgeMode, paths []string, trash string, trashErr error, closes *doomedCloses, timing *detachTiming) (moved []string, attempted bool, err error) {
	s := r.Store
	// The tag gate stays held until the paths are renamed away, so no tag
	// drain, flush or query reopens a doomed tag file in between; visitorMu
	// and visitorIO keep a visit or an in-flight snapshot write from
	// recreating the purged date.
	unlock, err := s.lockDetachGates(ctx, r.Counters, timing)
	if err != nil {
		return nil, false, err
	}
	held := time.Now()
	defer func() {
		unlock()
		timing.held = time.Since(held)
	}()
	// Pending aggregation of the purged date is discarded, not flushed: a
	// flush could recreate the files, and an unrelated flush failure (for
	// example ENOSPC) must not block reclaiming space.
	s.discardAggregationLocked(date, mode, paths)
	// Doomed handles leave the caches now and are settled after the renames
	// (doomedCloses): closed after the gates when moved, back in their cache
	// when nothing moved.
	notes := []error{s.releaseDoomedLocked(paths, closes)}
	if r.Counters != nil && (mode == PurgeAll || mode == PurgeRealtimeCounter) {
		notes = append(notes, r.Counters.releaseDateDoomedLocked(date, mode == PurgeRealtimeCounter, closes))
	}
	var gone, stayed []string
	for i, p := range paths {
		s.clock++
		dst, quarantined, err := moveAway(trash, trashErr, p, fmt.Sprintf("%s-%s-%d-%d-%d", date, mode, time.Now().UnixNano(), s.clock, i))
		if err != nil {
			r.stuck.Add(1)
			stayed = append(stayed, p)
			notes = append(notes, fmt.Errorf("purge %s %s: not moved, left in place: %w", mode, p, err))
			continue
		}
		if quarantined {
			r.quarantined.Add(1)
		}
		gone = append(gone, p)
		moved = append(moved, dst)
	}
	notes = append(notes, s.settleDoomedLocked(r.Counters, closes, gone, stayed))
	return moved, true, errors.Join(notes...)
}

// detachGateRounds bounds how often lockDetachGates starts over before it
// waits for a busy gate in place.
const detachGateRounds = 64

// detachSlow is the gate wait or hold from which a purge reports its timing.
const detachSlow = 100 * time.Millisecond

// detachTiming is how long lockDetachGates waited for each gate that was busy
// (with nothing held, except in the last round), in how many rounds, and how
// long the caller then held the gates.
type detachTiming struct {
	visitorMu, visitorIO, counters, tags time.Duration
	rounds                               int
	inPlace                              bool
	held                                 time.Duration
}

func (t *detachTiming) wait(d *time.Duration, lock func() error) error {
	if t == nil {
		return lock()
	}
	start := time.Now()
	err := lock()
	*d += time.Since(start)
	return err
}

// String is empty below detachSlow.
func (t *detachTiming) String() string {
	if t == nil || (t.visitorMu+t.visitorIO+t.counters+t.tags < detachSlow && t.held < detachSlow) {
		return ""
	}
	r := time.Millisecond
	s := fmt.Sprintf("waited visitorMu %v visitorIO %v counters %v tags %v in %d rounds, held %v",
		t.visitorMu.Round(r), t.visitorIO.Round(r), t.counters.Round(r), t.tags.Round(r), t.rounds, t.held.Round(r))
	if t.inPlace {
		s += ", last wait with the ingest gates held"
	}
	return s
}

// lockDetachGates takes the gates a purge or DB delete holds together, in
// their order: the Store gate, visitorMu, visitorIO, the Counters gate (c
// may be nil) and the tag gate. Ingest takes the Store gate for every record
// and visitorMu for every visit, so neither is held while waiting for a later
// gate: when one is busy (a visit writing its snapshot under visitorMu, a
// visitor snapshot write, a counter write, a tag pass writing its top100)
// everything is released, the busy gate is waited for alone, and the sequence
// starts over. A gate still busy after detachGateRounds rounds is waited for
// in place, so the purge is never postponed without bound. timing, when not
// nil, receives the waits. It fails with ErrClosed when the Store is closed.
func (s *Store) lockDetachGates(ctx context.Context, c *Counters, timing *detachTiming) (unlock func(), err error) {
	var none detachTiming
	t := timing
	if t == nil {
		t = &none
	}
	all := func() {
		s.tags.mu.Unlock()
		if c != nil {
			c.mu.Unlock()
		}
		s.visitorIO.Unlock()
		s.visitorMu.Unlock()
		s.mu.Unlock()
	}
	lockVisitorMu := func() error { s.visitorMu.Lock(); return nil }
	lockVisitorIO := func() error { s.visitorIO.Lock(); return nil }
	lockCounters := func() error { return c.mu.LockContext(ctx) }
	lockTags := func() error { return s.tags.mu.LockContext(ctx) }
	for round := 0; ; round++ {
		t.rounds = round + 1
		if err := s.mu.LockContext(ctx); err != nil {
			return nil, err
		}
		if s.closed {
			s.mu.Unlock()
			return nil, ErrClosed
		}
		if round >= detachGateRounds {
			t.inPlace = true
			_ = t.wait(&t.visitorMu, lockVisitorMu)
			_ = t.wait(&t.visitorIO, lockVisitorIO)
			if c != nil {
				if err := t.wait(&t.counters, lockCounters); err != nil {
					s.visitorIO.Unlock()
					s.visitorMu.Unlock()
					s.mu.Unlock()
					return nil, err
				}
			}
			if err := t.wait(&t.tags, lockTags); err != nil {
				if c != nil {
					c.mu.Unlock()
				}
				s.visitorIO.Unlock()
				s.visitorMu.Unlock()
				s.mu.Unlock()
				return nil, err
			}
			return all, nil
		}
		if !s.visitorMu.TryLock() {
			s.mu.Unlock()
			_ = t.wait(&t.visitorMu, lockVisitorMu)
			s.visitorMu.Unlock()
			continue
		}
		ingest := func() { s.visitorMu.Unlock(); s.mu.Unlock() }
		if !s.visitorIO.TryLock() {
			ingest()
			_ = t.wait(&t.visitorIO, lockVisitorIO)
			s.visitorIO.Unlock()
			continue
		}
		if c != nil && !c.mu.TryLock() {
			s.visitorIO.Unlock()
			ingest()
			if err := t.wait(&t.counters, lockCounters); err != nil {
				return nil, err
			}
			c.mu.Unlock()
			continue
		}
		if !s.tags.mu.TryLock() {
			if c != nil {
				c.mu.Unlock()
			}
			s.visitorIO.Unlock()
			ingest()
			if err := t.wait(&t.tags, lockTags); err != nil {
				return nil, err
			}
			s.tags.mu.Unlock()
			continue
		}
		return all, nil
	}
}

// cleanTrash retries removal left by an earlier failed cycle, outside the
// gates: the trash entries, and the quarantine names at the root (a
// quarantined date directory), directly in a date directory and one level
// below it (the targets of the purge modes and of most DB deletes). Paths a
// purge or DB delete is unlinking right now are skipped (removing); a path
// gone meanwhile is not an error. A quarantine name deeper in a date
// directory goes with that directory's next purge.
func (r *Retention) cleanTrash() error {
	root := r.Store.config.Directory
	var errs []error
	remove := func(p, what string) {
		if _, busy := removing.Load(p); busy {
			return
		}
		if err := purgeRemoveAll(p); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("purge %s cleanup: %w", what, err))
		}
	}
	patterns := []string{filepath.Join(root, quarantinePrefix+"*")}
	if entries, err := os.ReadDir(root); err != nil {
		errs = append(errs, err)
	} else {
		for _, e := range entries {
			if !e.IsDir() || (e.Name() != permanentDate && validDate(e.Name()) != nil) {
				continue
			}
			day := filepath.Join(root, e.Name())
			patterns = append(patterns, filepath.Join(day, quarantinePrefix+"*"), filepath.Join(day, "*", quarantinePrefix+"*"))
		}
	}
	for _, pattern := range patterns {
		leftovers, err := filepath.Glob(pattern)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, p := range leftovers {
			remove(p, "quarantine")
		}
	}
	trash := filepath.Join(root, purgeTrash)
	entries, err := os.ReadDir(trash)
	if os.IsNotExist(err) {
		return errors.Join(errs...)
	}
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	for _, e := range entries {
		remove(filepath.Join(trash, e.Name()), "trash")
	}
	return errors.Join(errs...)
}

// discardAggregationLocked runs under Store.mu and visitorMu (detach, delete).
func (s *Store) discardAggregationLocked(date string, mode PurgeMode, paths []string) {
	if v := s.visitor; v != nil && v.date == date {
		switch mode {
		case PurgeAll:
			v.daily, v.hourly = map[string]*visitorEntry{}, map[string]*visitorEntry{}
		case PurgeVisitor:
			v.daily = map[string]*visitorEntry{}
		}
	}
	s.discardTextsLocked(paths)
	s.discardPacksLocked(paths)
	s.tagDiscardLocked(paths) // the caller holds the tag gate (lockDetachGates)
}

func under(path string, roots []string) bool {
	for _, root := range roots {
		if path == root || strings.HasPrefix(path, root+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// releaseDoomedLocked takes cached index/data handles whose path is a target
// or lies below one out of the caches; closes closes them (doomedCloses; nil:
// at once or by the sync pass in flight; only the tests pass nil).
// Index keys are file paths without .kfile/.hfile suffix.
func (s *Store) releaseDoomedLocked(roots []string, closes *doomedCloses) error {
	var errs []error
	for p, f := range s.files {
		if under(p, roots) {
			errs = append(errs, closes.take(&s.closes, &doomedHandle{files: []string{p}, data: f, close: f.Close, // about to be deleted: no fsync
				restore: func() { s.files[p] = f; s.touch(p) }}))
			delete(s.files, p)
			delete(s.ends, p)
			delete(s.lastUse, p)
		}
	}
	for p, idx := range s.indexes {
		if under(p, roots) || under(p+".kfile", roots) || under(p+".hfile", roots) {
			errs = append(errs, closes.take(&s.closes, &doomedHandle{files: []string{p + ".kfile", p + ".hfile"}, close: idx.Close,
				restore: func() { s.indexes[p] = idx; s.touch(p) }}))
			delete(s.indexes, p)
			delete(s.lastUse, p)
		}
	}
	for p := range s.modes {
		if under(p, roots) {
			delete(s.modes, p)
		}
	}
	for _, root := range roots {
		s.zipReleaseLocked(root)
	}
	return errors.Join(errs...)
}

// doomedTable is a counter table released for a purge or DB delete.
func (c *Counters) doomedTable(key string, t *counterTable) *doomedHandle {
	return &doomedHandle{files: c.counterKeyFiles(key), data: t.data,
		close:   func() error { return errors.Join(t.index.Close(), t.data.Close()) },
		restore: func() { c.tables[key] = t }}
}

func (c *Counters) releaseDateDoomedLocked(date string, realtimeOnly bool, closes *doomedCloses) error {
	c.markPurgedLocked(date)
	var errs []error
	for key, t := range c.tables {
		if !strings.HasPrefix(key, date+"/") || (realtimeOnly && key != date+"/real") {
			continue
		}
		errs = append(errs, closes.take(&c.closes, c.doomedTable(key, t)))
		delete(c.tables, key)
	}
	return errors.Join(errs...)
}

func diskUsedPercent(root string) (float64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(root, &st); err != nil {
		return 0, err
	}
	total := float64(st.Blocks) * float64(st.Bsize)
	if total <= 0 {
		return 0, fmt.Errorf("statfs %s: zero size", root)
	}
	return 100 - float64(st.Bavail)*float64(st.Bsize)*100/total, nil
}
