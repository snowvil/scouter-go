// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
package migrate

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

const (
	permanentDir           = "00000000"
	objectSlotBytes        = 4096
	defaultProfileRefCheck = 10000
)

// InspectOptions selects the DB root and the Java JVM timezone of that DB.
type InspectOptions struct {
	Root     string
	Location *time.Location
	// Now is the Unix second used for KV TTL state; 0 means the current time.
	Now int64
	// ProfileRefSample bounds distinct profile txids per date looked up in
	// xlog_tid. 0 selects 10000; a negative value disables the lookup.
	ProfileRefSample int
}

type payloadKind int

const (
	payloadPack payloadKind = iota
	payloadObject
	payloadText
	payloadRaw
)

type dataSpec struct {
	file     string // slash path of the plain data file
	prefix   int
	zip      string // "", "xlog" or "profile": GZipStore when the .conf selects it
	kind     payloadKind
	packType byte
	match    func(key []byte, p protocol.Pack) bool
	field    string
	primary  bool
}

type indexSpec struct {
	category string
	time     bool
	keyLen   int
	date     string // Java date directory for key date checks and counts
	countKey string
	data     *dataSpec
}

type inspector struct {
	ctx         context.Context
	opt         InspectOptions
	root        string
	c           *collector
	files       map[string]*FileEntry
	dates       map[string]map[string]int64
	perm        map[string]int64
	data        map[string]*dataFile
	zips        map[string]*zipStore
	zipModes    map[string]bool
	indexBases  map[string]bool // slash base path -> TTL
	open        []*dataFile     // LRU of data files holding a descriptor
	maxOpenSeen int
}

// Inspect walks root without writing or following symlinks. Data problems are
// returned in the report; the error result is for I/O failure or cancellation.
func Inspect(ctx context.Context, opt InspectOptions) (*Report, error) {
	if opt.Location == nil {
		return nil, errors.New("timezone required")
	}
	root, err := filepath.Abs(opt.Root)
	if err != nil {
		return nil, err
	}
	if opt.Now == 0 {
		opt.Now = time.Now().Unix()
	}
	if opt.ProfileRefSample == 0 {
		opt.ProfileRefSample = defaultProfileRefCheck
	}
	in := newInspector(ctx, opt, root)
	rep := &Report{Tool: ToolVersion, Root: root, Timezone: opt.Location.String(), Now: opt.Now, Started: nowText()}
	defer in.closeData()
	if rep.Locks, err = checkLocks(root); err != nil {
		return nil, err
	}
	t, err := walkTree(ctx, root)
	if err != nil {
		return nil, err
	}
	in.lockProblems(rep.Locks)
	rep.Dirs = reportDirs(t)
	if rep.Files, err = in.inspectTree(t); err != nil {
		return nil, err
	}
	rep.Problems = in.c.finish()
	rep.Summary = in.summary(rep, len(t.dirs))
	rep.Finished = nowText()
	return rep, ctx.Err()
}

func newInspector(ctx context.Context, opt InspectOptions, root string) *inspector {
	if testHookInspectorRoot != nil {
		testHookInspectorRoot(root)
	}
	return &inspector{ctx: ctx, opt: opt, root: root, c: newCollector(), files: map[string]*FileEntry{},
		dates: map[string]map[string]int64{}, perm: map[string]int64{}, data: map[string]*dataFile{},
		zips: map[string]*zipStore{}, zipModes: map[string]bool{}, indexBases: map[string]bool{}}
}

// reportDirs lists walked directories except retention trash.
func reportDirs(t *tree) []string {
	var dirs []string
	for _, d := range t.dirs {
		if !isPurgeTrash(d.rel) {
			dirs = append(dirs, d.rel)
		}
	}
	return dirs
}

// inspectTree checks the entries of t and returns their file entries. Every
// check reads only files of t's own groups (see groupOf), so a subtree of
// whole groups gives the same findings as the full tree.
func (in *inspector) inspectTree(t *tree) ([]FileEntry, error) {
	for _, e := range t.symlinks {
		in.c.add(SeverityError, "fs.symlink", e.rel, -1, "symbolic link is not followed, copied or verified")
	}
	for _, e := range t.others {
		in.c.add(SeverityError, "fs.nonregular", e.rel, -1, "non-regular file mode %s", e.info.Mode())
	}
	files := make([]FileEntry, 0, len(t.files))
	for _, e := range t.files {
		entry, err := in.fileEntry(e)
		if err != nil {
			return nil, err
		}
		files = append(files, *entry)
	}
	for n := range files {
		in.files[files[n].Path] = &files[n]
	}
	for _, check := range []func() error{in.checkIndexes, in.checkProfileRefs, in.checkDataTails, in.checkZips, in.checkTagChains} {
		if err := check(); err != nil {
			return nil, err
		}
	}
	if testHookMaxOpen != nil {
		testHookMaxOpen(in.maxOpenSeen)
	}
	return files, nil
}

func (in *inspector) lockProblems(l LockState) {
	if l.JavaActive {
		in.c.add(SeverityError, "lock.java_active", javaLockName, -1, "lock.dat modified within %s (Java DBCtr live writer rule); stop the Java server first", javaLockLive)
	} else if l.JavaLockExists {
		in.c.add(SeverityInfo, "lock.java_stale", javaLockName, -1, "stale lock.dat (mtime %s); excluded from copy", l.JavaLockMTime)
	}
	if l.GoWriterActive {
		in.c.add(SeverityError, "lock.go_active", goLockName, -1, "a Go writer holds the flock; stop it first")
	} else if l.GoLockExists {
		in.c.add(SeverityInfo, "lock.go_stale", goLockName, -1, "unheld Go writer lock file; excluded from copy")
	}
}

func isDateDir(name string) bool {
	if name == permanentDir {
		return true
	}
	if len(name) != 8 {
		return false
	}
	_, err := time.Parse("20060102", name)
	return err == nil
}
func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// classify maps a slash path to a category and verification status.
func classify(rel string) (string, string) {
	if isLockFile(rel) {
		return "lock", StatusExcluded
	}
	if isRuntimeState(rel) {
		return "runtime-state", StatusExcluded
	}
	if isPurgeTrash(rel) {
		return "purge-trash", StatusExcluded
	}
	if isTempLeftover(rel) {
		return "temp", StatusExcluded
	}
	parts := strings.Split(rel, "/")
	if len(parts) < 2 || !isDateDir(parts[0]) {
		switch path.Ext(rel) {
		case ".kfile", ".hfile", ".k2file":
			return "index", StatusStructureOnly
		}
		return "other", StatusUnknown
	}
	sub := strings.Join(parts[1:], "/")
	dir, name := path.Split(sub)
	dir = strings.TrimSuffix(dir, "/")
	ext := path.Ext(name)
	base := strings.TrimSuffix(name, ext)
	index := ext == ".kfile" || ext == ".hfile" || ext == ".k2file"
	if parts[0] == permanentDir {
		switch {
		case dir == "text" && strings.HasPrefix(base, "text_") && (ext == ".kfile" || ext == ".hfile" || ext == ".data"):
			return "text.permanent", StatusChecked
		case dir == "kv2" && strings.HasPrefix(base, "kv_") && (ext == ".k2file" || ext == ".hfile" || ext == ".kvdata"):
			return "kv", StatusChecked
		case index:
			return "index", StatusStructureOnly
		}
		return "other", StatusUnknown
	}
	switch {
	case dir == "object" && base == "obj" && (index || ext == ".data"):
		return "object", StatusChecked
	case dir == "text" && base == "text" && index:
		return "text.daily", StatusChecked
	case dir == "xlog" && (name == "xlog.service" || name == "xlog.service.conf" || (index && (base == "xlog_tim" || base == "xlog_tid" || base == "xlog_gid"))):
		return "xlog", StatusChecked
	case dir == "xlog" && (name == "xlog.profile" || name == "xlog.profile.conf" || (index && base == "xlog_profile")):
		return "profile", StatusChecked
	case dir == "xlog/data":
		if _, ok := parseBlockName(name); ok || name == "count.dat" {
			return "xlog.zip", StatusChecked
		}
	case dir == "status" && (name == "status.pstat" || (index && base == "status_tim")):
		return "status", StatusChecked
	case dir == "sum" && (name == "sum.sum" || (index && strings.HasPrefix(base, "sum_") && strings.HasSuffix(base, "_tim"))):
		return "summary", StatusChecked
	case dir == "alert" && (name == "alert.alert" || (index && base == "alert_tim")):
		return "alert", StatusChecked
	case dir == "span" && (name == "span.data" || (index && (base == "span_tim" || base == "span_gid"))):
		return "span", StatusChecked
	case dir == "counter" && index && (base == "real" || base == "5m"):
		return "counter", StatusStructureOnly
	case dir == "counter":
		return "counter", StatusUnchecked
	case strings.HasPrefix(sub, "tagcnt/"):
		return "tagcount", StatusUnchecked
	case ext == ".usr":
		return "visitor", StatusUnchecked
	case name == "stack.dat" || name == "stack.idx":
		return "stack", StatusUnchecked
	case name == "batch.dat" || name == "batch.idx":
		return "batch", StatusUnchecked
	}
	if index {
		return "index", StatusStructureOnly
	}
	return "other", StatusUnknown
}

func (in *inspector) fileEntry(e treeEntry) (*FileEntry, error) {
	category, status := classify(e.rel)
	entry := &FileEntry{Path: e.rel, Bytes: e.info.Size(), Mode: uint32(e.info.Mode().Perm()), MTime: e.info.ModTime().UnixNano(), Category: category, Status: status}
	if status != StatusExcluded {
		sum, n, err := hashFile(in.ctx, filepath.Join(in.root, filepath.FromSlash(e.rel)))
		if err != nil {
			return nil, err
		}
		if n != entry.Bytes {
			in.c.add(SeverityError, "fs.changed", e.rel, -1, "size changed while hashing (%d -> %d); a writer may be active", entry.Bytes, n)
		}
		entry.SHA256 = sum
	}
	switch category {
	case "purge-trash":
		in.c.add(SeverityWarning, "purge.trash_leftover", purgeTrashDir, -1, "%s: retention trash left by an interrupted purge; not copied", e.rel)
	case "temp":
		in.c.add(SeverityWarning, "temp_leftover", e.rel, -1, "Go atomic-write temporary file left by an interrupted write; not copied")
	}
	if status == StatusUnknown {
		in.c.add(SeverityInfo, "layout.unknown", e.rel, -1, "unrecognised file; copied and checksummed only")
	} else if status == StatusUnchecked {
		in.c.add(SeverityInfo, "layout.unchecked", e.rel, -1, "%s data is not verified by Go; copied and checksummed only", category)
	}
	parts := strings.Split(e.rel, "/")
	if len(parts) > 1 && len(parts[0]) == 8 && allDigits(parts[0]) && !isDateDir(parts[0]) {
		in.c.add(SeverityWarning, "layout.invalid_date", e.rel, -1, "8-digit directory is not a valid Java date")
	}
	// Only legacy-index categories are parsed as .kfile/.hfile pairs; tagcnt
	// first/next key files share the extension but not the format.
	switch ext := path.Ext(e.rel); {
	case status == StatusExcluded || status == StatusUnchecked || status == StatusUnknown:
	case ext == ".kfile":
		in.indexBases[strings.TrimSuffix(e.rel, ".kfile")] = false
	case ext == ".k2file":
		in.indexBases[strings.TrimSuffix(e.rel, ".k2file")] = true
	}
	return entry, nil
}

func (in *inspector) count(date, key string, n int64) {
	if date == "" || date == permanentDir {
		in.perm[key] += n
		return
	}
	m := in.dates[date]
	if m == nil {
		m = map[string]int64{}
		in.dates[date] = m
	}
	m[key] += n
}

func xlogMatch(field string) func([]byte, protocol.Pack) bool {
	return func(key []byte, p protocol.Pack) bool {
		x := p.(*protocol.XLogPack)
		v := int64(binary.BigEndian.Uint64(key))
		switch field {
		case "EndTime":
			return x.EndTime == v
		case "TxID":
			return x.TxID == v
		}
		return x.GxID == v
	}
}
func timeMatch(key []byte, p protocol.Pack) bool {
	v := int64(binary.BigEndian.Uint64(key))
	switch x := p.(type) {
	case *protocol.StatusPack:
		return x.Time == v
	case *protocol.SummaryPack:
		return x.Time == v
	case *protocol.AlertPack:
		return x.Time == v
	case *protocol.SpanContainerPack:
		return x.Timestamp == v
	}
	return false
}
func spanGxIDMatch(key []byte, p protocol.Pack) bool {
	return p.(*protocol.SpanContainerPack).GxID == int64(binary.BigEndian.Uint64(key))
}

// specFor follows the pinned Java directory layout (storage-contract.md, pack-storage.md).
func specFor(base string, ttl bool) indexSpec {
	parts := strings.Split(base, "/")
	date := ""
	if len(parts) > 1 && isDateDir(parts[0]) {
		date = parts[0]
	}
	dir, name := path.Split(base)
	dir = strings.TrimSuffix(dir, "/")
	sub := strings.Join(parts[min(1, len(parts)-1):], "/")
	generic := indexSpec{category: "index", time: !ttl && strings.HasSuffix(name, "_tim"), date: date, countKey: "index_records"}
	if date == "" {
		return generic
	}
	if date == permanentDir {
		switch {
		case !ttl && sub == "text/"+name && strings.HasPrefix(name, "text_"):
			return indexSpec{category: "text.permanent", keyLen: 4, date: date, countKey: "text",
				data: &dataSpec{file: base + ".data", prefix: 4, kind: payloadText, primary: true}}
		case ttl && sub == "kv2/"+name && strings.HasPrefix(name, "kv_"):
			return indexSpec{category: "kv", date: date, countKey: "kv_records",
				data: &dataSpec{file: base + ".kvdata", prefix: 4, kind: payloadText, primary: true}}
		}
		return generic
	}
	if ttl {
		return generic
	}
	xlog := func(field string, timed, primary bool, key string) indexSpec {
		return indexSpec{category: "xlog", time: timed, keyLen: 8, date: date, countKey: key,
			data: &dataSpec{file: dir + "/xlog.service", prefix: 2, zip: "xlog", kind: payloadPack, packType: protocol.PackXLog, match: xlogMatch(field), field: field, primary: primary}}
	}
	timed := func(category, data string, prefix int, kind byte, key string) indexSpec {
		return indexSpec{category: category, time: true, keyLen: 8, date: date, countKey: key,
			data: &dataSpec{file: dir + "/" + data, prefix: prefix, kind: payloadPack, packType: kind, match: timeMatch, field: "time", primary: true}}
	}
	switch sub {
	case "object/obj":
		return indexSpec{category: "object", keyLen: 4, date: date, countKey: "object",
			data: &dataSpec{file: base + ".data", prefix: 4, kind: payloadObject, primary: true}}
	case "text/text":
		return indexSpec{category: "text.daily", keyLen: 8, date: date, countKey: "text"}
	case "xlog/xlog_tim":
		return xlog("EndTime", true, true, "xlog")
	case "xlog/xlog_tid":
		return xlog("TxID", false, false, "xlog_tid")
	case "xlog/xlog_gid":
		return xlog("GxID", false, false, "xlog_gid")
	case "xlog/xlog_profile":
		return indexSpec{category: "profile", keyLen: 8, date: date, countKey: "profile",
			data: &dataSpec{file: dir + "/xlog.profile", prefix: 4, zip: "profile", kind: payloadRaw, primary: true}}
	case "status/status_tim":
		return timed("status", "status.pstat", 4, protocol.PackStatus, "status")
	case "alert/alert_tim":
		return timed("alert", "alert.alert", 2, protocol.PackAlert, "alert")
	case "span/span_tim":
		return timed("span", "span.data", 2, protocol.PackSpanContainer, "span")
	case "span/span_gid":
		return indexSpec{category: "span", keyLen: 8, date: date, countKey: "span_gid",
			data: &dataSpec{file: dir + "/span.data", prefix: 2, kind: payloadPack, packType: protocol.PackSpanContainer, match: spanGxIDMatch, field: "GxID"}}
	case "counter/real":
		return indexSpec{category: "counter", time: true, date: date, countKey: "counter_real"}
	case "counter/5m":
		return indexSpec{category: "counter", date: date, countKey: "counter_5m"}
	}
	if dir == date+"/sum" && strings.HasPrefix(name, "sum_") && strings.HasSuffix(name, "_tim") {
		return timed("summary", "sum.sum", 4, protocol.PackSummary, "summary")
	}
	return generic
}

func (in *inspector) indexError(rel string, err error) {
	var ce *legacyindex.CorruptError
	switch {
	case errors.As(err, &ce):
		in.c.add(SeverityError, "index."+ce.Code, rel, ce.Offset, "%v", err)
	case errors.Is(err, legacyindex.ErrLimit):
		in.c.add(SeverityError, "index.limit", rel, -1, "%v", err)
	case errors.Is(err, os.ErrNotExist):
		in.c.add(SeverityError, "index.missing_pair", rel, -1, "%v", err)
	default:
		in.c.add(SeverityError, "index.io", rel, -1, "%v", err)
	}
}

func (in *inspector) checkIndexes() error {
	for rel, entry := range in.files {
		if strings.HasSuffix(rel, ".hfile") {
			base := strings.TrimSuffix(rel, ".hfile")
			if _, ok := in.indexBases[base]; !ok && entry.Status != StatusExcluded && entry.Status != StatusUnchecked && entry.Status != StatusUnknown {
				in.c.add(SeverityError, "index.missing_pair", rel, -1, ".hfile without .kfile/.k2file")
			}
		}
	}
	bases := make([]string, 0, len(in.indexBases))
	for b := range in.indexBases {
		bases = append(bases, b)
	}
	sort.Strings(bases)
	for _, base := range bases {
		if err := in.ctx.Err(); err != nil {
			return err
		}
		ttl := in.indexBases[base]
		rel := base + ".kfile"
		if ttl {
			rel = base + ".k2file"
		}
		drift := func() int64 {
			return in.c.seen[[2]string{"index.truncated", rel}] + in.c.seen[[2]string{"index.unreachable_records", rel}]
		}
		before := drift()
		var err error
		if ttl {
			err = in.checkTTL(base)
		} else {
			err = in.checkIndex(base)
		}
		if err != nil {
			return err
		}
		if drift() > before {
			in.autoRecoverable(base, rel, ttl)
		}
	}
	return nil
}

// autoRecoverable adds the repair a writable Go open would apply (dry run,
// O_RDONLY). Severity of the original findings is unchanged.
func (in *inspector) autoRecoverable(base, rel string, ttl bool) {
	var info legacyindex.RecoveryInfo
	var err error
	if ttl {
		info, err = legacyindex.AnalyzeTTLRecovery(in.ctx, in.abs(base))
	} else {
		check, release := storage.NewRelinkCheck(in.abs(base), false, false)
		info, err = legacyindex.AnalyzeRecoveryChecked(in.abs(base), specFor(base, false).time, in.opt.Location, check)
		release()
	}
	if err != nil {
		if ctxErr := in.ctx.Err(); ctxErr != nil {
			return
		}
		in.c.add(SeverityWarning, "index.not_recoverable", rel, -1, "a writable Go open would fail: %v", err)
		return
	}
	if info.RelinkRefused != "" {
		in.c.add(SeverityWarning, "index.relink_refused", rel, -1, "%d records stay unreachable (original Java semantics); a writable Go open marks them deleted instead of relinking because %s", info.Unreachable, info.RelinkRefused)
	}
	if !info.Recovered() {
		return
	}
	in.c.add(SeverityInfo, "index.auto_recoverable", rel, -1, "a writable Go open relinks %d records in %d buckets (count %d -> %d) and truncates %d torn bytes; Java never rebuilds (intended difference)",
		info.RelinkedRecords, info.RelinkedBuckets, info.CountBefore, info.CountAfter, info.TruncatedTailBytes)
	if info.TruncatedTailBytes > 0 {
		// The read-only open failed on the torn tail, so the per-record checks
		// never ran; run them over the complete records before the cut.
		in.checkTornRecords(base, rel, ttl, info.TruncatedAt)
	}
}

func (in *inspector) abs(rel string) string { return filepath.Join(in.root, filepath.FromSlash(rel)) }

func (in *inspector) setRecords(rel string, n int64) {
	if e := in.files[rel]; e != nil {
		e.Records = &n
	}
}

// statsProblems reports Java-tolerated header/reachability drift as warnings.
func (in *inspector) statsProblems(rel string, st legacyindex.Stats) {
	if int64(st.HeaderCount) != st.ExpectedCount {
		in.c.add(SeverityWarning, "index.count_mismatch", rel, -1, "header count %d, Java count rule gives %d (stale 4s header flush or deletion)", st.HeaderCount, st.ExpectedCount)
	}
	if st.ReachableRecords < st.Records {
		in.c.add(SeverityWarning, "index.unreachable_records", rel, -1, "%d of %d records are not reachable from bucket heads (e.g. .hfile not flushed before an abnormal stop); Java readers cannot see them either", st.Records-st.ReachableRecords, st.Records)
	}
}

func (in *inspector) checkIndex(base string) error {
	spec := specFor(base, false)
	rel := base + ".kfile"
	idx, err := legacyindex.OpenReadOnly(in.abs(base), spec.time, in.opt.Location)
	if err != nil {
		var ce *legacyindex.CorruptError
		if spec.time && errors.As(err, &ce) && ce.Code == legacyindex.CodeHead {
			err = fmt.Errorf("%w (time bucket layout depends on the Java JVM timezone; check --timezone)", err)
		}
		in.indexError(rel, err)
		return in.ctx.Err()
	}
	defer idx.Close()
	st, err := idx.Inspect(in.ctx)
	if err != nil {
		if ctxErr := in.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		in.indexError(rel, err)
		return nil
	}
	in.setRecords(rel, st.Records)
	in.statsProblems(rel, st)
	in.count(spec.date, spec.countKey, st.Records-st.Deleted)
	err = idx.ScanAllContext(in.ctx, func(r legacyindex.Record) error {
		if r.Deleted {
			return nil
		}
		in.checkRecord(spec, rel, r.Key, r.Value, r.Position)
		return nil
	})
	if err != nil {
		if ctxErr := in.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		in.indexError(rel, err)
	}
	return nil
}

func (in *inspector) checkTTL(base string) error {
	spec := specFor(base, true)
	rel := base + ".k2file"
	idx, err := legacyindex.OpenTTLReadOnly(in.ctx, in.abs(base))
	if err != nil {
		if ctxErr := in.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		in.indexError(rel, err)
		return nil
	}
	defer idx.Close()
	st, err := idx.Inspect(in.ctx, in.opt.Now)
	if err != nil {
		if ctxErr := in.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		in.indexError(rel, err)
		return nil
	}
	in.setRecords(rel, st.Records)
	in.statsProblems(rel, st)
	in.count(spec.date, "ttl_live", st.Live)
	in.count(spec.date, "ttl_expired", st.Expired)
	in.count(spec.date, "ttl_deleted", st.Deleted)
	err = idx.ScanContext(in.ctx, func(r legacyindex.TTLRecord) error {
		in.checkRecord(spec, rel, r.Key, r.Value, r.Position)
		return nil
	})
	if err != nil {
		if ctxErr := in.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		in.indexError(rel, err)
	}
	return nil
}

// checkRecord validates key shape, Java date and the referenced payload.
func (in *inspector) checkRecord(spec indexSpec, rel string, key, value []byte, at int64) {
	if spec.keyLen > 0 && len(key) != spec.keyLen {
		in.c.add(SeverityError, "index.key_length", rel, at, "key length %d, expected %d", len(key), spec.keyLen)
		return
	}
	if spec.time && spec.date != "" && spec.date != permanentDir && len(key) == 8 {
		ms := int64(binary.BigEndian.Uint64(key))
		if d := scoutertime.Date(ms, in.opt.Location); d != spec.date {
			in.c.add(SeverityError, "time.date_mismatch", rel, at, "time %d belongs to Java date %s in %s, not %s (wrong --timezone or misplaced record)", ms, d, in.opt.Location, spec.date)
		}
	}
	if spec.category == "text.daily" && !utf8.Valid(value) {
		in.c.add(SeverityError, "data.utf8", rel, at, "daily text value is not UTF-8")
	}
	if spec.data == nil {
		return
	}
	d := spec.data
	if len(value) != 5 {
		in.c.add(SeverityError, "index.value_length", rel, at, "data offset value has %d bytes, expected Long5", len(value))
		return
	}
	pos := readLong5(value)
	payload, where, err := in.readPayload(spec, d, pos)
	if err != nil {
		var de *dataError
		if errors.As(err, &de) {
			in.c.add(SeverityError, de.code, where, de.offset, "%s (from %s record %d)", de.detail, rel, at)
		} else {
			in.c.add(SeverityError, "data.io", where, pos, "%v (from %s record %d)", err, rel, at)
		}
		return
	}
	switch d.kind {
	case payloadText:
		if !utf8.Valid(payload) {
			in.c.add(SeverityError, "data.utf8", where, pos, "text payload is not UTF-8 (from %s)", rel)
		}
	case payloadObject:
		if len(payload) != objectSlotBytes {
			in.c.add(SeverityError, "object.slot_size", where, pos, "object slot %d bytes, expected %d", len(payload), objectSlotBytes)
			return
		}
		dec := protocol.NewDecoder(payload, protocol.Limits{})
		p := dec.Pack()
		if dec.Err() != nil {
			in.c.add(SeverityError, "data.decode", where, pos, "ObjectPack: %v", dec.Err())
			return
		}
		o, ok := p.(*protocol.ObjectPack)
		if !ok {
			in.c.add(SeverityError, "data.pack_type", where, pos, "pack type %d, expected ObjectPack", p.Type())
		} else if o.ObjHash != int32(binary.BigEndian.Uint32(key)) {
			in.c.add(SeverityError, "data.key_mismatch", where, pos, "objHash %d != index key %d", o.ObjHash, int32(binary.BigEndian.Uint32(key)))
		}
	case payloadPack:
		p, err := protocol.DecodePack(payload, protocol.Limits{})
		if err != nil {
			in.c.add(SeverityError, "data.decode", where, pos, "%v (from %s)", err, rel)
			return
		}
		if p.Type() != d.packType {
			in.c.add(SeverityError, "data.pack_type", where, pos, "pack type %d, expected %d (from %s)", p.Type(), d.packType, rel)
			return
		}
		if !d.match(key, p) {
			in.c.add(SeverityError, "data.key_mismatch", where, pos, "%s does not equal index key %d (from %s)", d.field, int64(binary.BigEndian.Uint64(key)), rel)
		}
	}
}

func (in *inspector) zipMode(date, which string) bool {
	k := date + "/" + which
	if mode, ok := in.zipModes[k]; ok {
		return mode
	}
	conf, key := date+"/xlog/xlog.service.conf", "compress_xlog_enabled"
	plain := date + "/xlog/xlog.service"
	if which == "profile" {
		conf, key, plain = date+"/xlog/xlog.profile.conf", "compress_profile_enabled", date+"/xlog/xlog.profile"
	}
	mode, present, err := storage.ReadCompressionMode(in.abs(conf), key, false)
	if err != nil {
		in.c.add(SeverityError, "xlog.conf", conf, -1, "%v", err)
	} else if !present {
		_, plainErr := os.Lstat(in.abs(plain))
		_, zipErr := os.Lstat(in.abs(date + "/xlog/data/count.dat"))
		if os.IsNotExist(plainErr) && zipErr == nil {
			mode = true
			in.c.add(SeverityWarning, "xlog.conf_missing", conf, -1, "missing %s; GZipStore data present, inspected as compressed. Go reads it only with the matching --compress-%s setting", path.Base(conf), which)
		} else {
			in.c.add(SeverityWarning, "xlog.conf_missing", conf, -1, "missing %s; inspected as uncompressed (Java/Go default)", path.Base(conf))
		}
	}
	in.zipModes[k] = mode
	return mode
}

func (in *inspector) zip(date string) *zipStore {
	if z := in.zips[date]; z != nil {
		return z
	}
	rel := date + "/xlog/data"
	z := &zipStore{rel: rel, dir: in.abs(rel), count: -1, blocks: map[int64]bool{}, cache: map[int64][]byte{}}
	in.zips[date] = z
	entries, _ := os.ReadDir(z.dir)
	for _, e := range entries {
		if n, ok := parseBlockName(e.Name()); ok && e.Type().IsRegular() {
			z.blocks[n] = true
		}
	}
	countRel := rel + "/count.dat"
	b, err := os.ReadFile(in.abs(countRel))
	switch {
	case os.IsNotExist(err):
		in.c.add(SeverityError, "zip.count_missing", countRel, -1, "compressed mode without count.dat")
	case err != nil:
		in.c.add(SeverityError, "data.io", countRel, -1, "%v", err)
	case len(b) != 8:
		in.c.add(SeverityError, "zip.count_size", countRel, -1, "count.dat has %d bytes, expected 8", len(b))
	default:
		c := int64(binary.BigEndian.Uint64(b))
		if c < 0 || c > maxLong5 {
			in.c.add(SeverityError, "zip.count_range", countRel, -1, "count %d outside signed Long5", c)
		} else {
			z.count = c
		}
	}
	return z
}

// maxOpenDataFiles bounds data descriptors held across a whole inspection so a
// large DB does not hit the process file limit (macOS default 256).
var maxOpenDataFiles = 16

// testHookMaxOpen observes the peak number of open data descriptors.
var testHookMaxOpen func(int)

// dataFile returns an open handle, reopening an evicted one. Evicted files keep
// their counters; a size change between opens is reported as fs.changed.
func (in *inspector) dataFile(rel string, prefix int) (*dataFile, error) {
	d := in.data[rel]
	switch {
	case d == nil:
		var err error
		if d, err = openDataFile(in.root, rel, prefix); err != nil {
			return nil, err
		}
		in.data[rel] = d
		if d.f == nil {
			return d, nil
		}
	case d.missing:
		return d, nil
	case d.f == nil:
		f, err := os.OpenFile(d.abs, os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		if st.Size() != d.size {
			in.c.add(SeverityError, "fs.changed", rel, -1, "size changed during inspection (%d -> %d); a writer may be active", d.size, st.Size())
		}
		d.f = f
	default:
		for n, o := range in.open { // most recently used moves to the end
			if o == d {
				in.open = append(in.open[:n], in.open[n+1:]...)
				break
			}
		}
	}
	in.open = append(in.open, d)
	for len(in.open) > maxOpenDataFiles {
		in.open[0].close()
		in.open = in.open[1:]
	}
	in.maxOpenSeen = max(in.maxOpenSeen, len(in.open))
	return d, nil
}

func (in *inspector) readPayload(spec indexSpec, d *dataSpec, pos int64) ([]byte, string, error) {
	if d.zip != "" && in.zipMode(spec.date, d.zip) {
		z := in.zip(spec.date)
		b, err := z.read(pos)
		if err == nil && d.primary {
			z.refs++
		}
		return b, z.rel, err
	}
	f, err := in.dataFile(d.file, d.prefix)
	if err != nil {
		return nil, d.file, err
	}
	b, err := f.read(pos)
	if err == nil && d.primary {
		f.refs++
	}
	return b, d.file, err
}

// checkProfileRefs samples profile txids and looks them up in the same date's
// xlog_tid. Java XLogProfileWR stores by receive time and XLog may be dropped
// or dated by endTime, so a miss is a warning rather than corruption.
func (in *inspector) checkProfileRefs() error {
	if in.opt.ProfileRefSample < 0 {
		return nil
	}
	for base := range in.indexBases {
		if !strings.HasSuffix(base, "/xlog/xlog_profile") {
			continue
		}
		date := strings.SplitN(base, "/", 2)[0]
		if !isDateDir(date) || date == permanentDir {
			continue
		}
		prof, err := legacyindex.OpenReadOnly(in.abs(base), false, in.opt.Location)
		if err != nil {
			continue // already reported by checkIndex
		}
		tid, tidErr := legacyindex.OpenReadOnly(in.abs(date+"/xlog/xlog_tid"), false, in.opt.Location)
		var checked, missing int64
		var last []byte
		stop := errors.New("sample complete")
		err = prof.Scan(func(r legacyindex.Record) error {
			if err := in.ctx.Err(); err != nil {
				return err
			}
			if bytes.Equal(r.Key, last) {
				return nil
			}
			if checked >= int64(in.opt.ProfileRefSample) {
				return stop
			}
			last = r.Key
			checked++
			if tidErr != nil {
				missing++
				return nil
			}
			v, err := tid.Get(r.Key)
			if err != nil {
				return err
			}
			if v == nil {
				missing++
			}
			return nil
		})
		prof.Close()
		if tid != nil {
			tid.Close()
		}
		if ctxErr := in.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil && !errors.Is(err, stop) {
			in.indexError(date+"/xlog/xlog_tid.kfile", err)
			continue
		}
		in.count(date, "profile_ref_checked", checked)
		in.count(date, "profile_ref_missing", missing)
		if missing > 0 {
			in.c.add(SeverityWarning, "profile.xlog_missing", base+".kfile", -1, "%d of %d sampled profile txids have no xlog_tid entry in the same date; can be normal (receive-time profile date, dropped/sampled XLog)", missing, checked)
		}
	}
	return nil
}

// plainDataPrefix lists every data file that has an index-referenced layout.
func plainDataPrefix(rel string) (int, bool) {
	parts := strings.Split(rel, "/")
	if len(parts) < 3 || !isDateDir(parts[0]) {
		return 0, false
	}
	sub := strings.Join(parts[1:], "/")
	if parts[0] == permanentDir {
		name := path.Base(rel)
		if sub == "text/"+name && strings.HasPrefix(name, "text_") && strings.HasSuffix(name, ".data") {
			return 4, true
		}
		if sub == "kv2/"+name && strings.HasPrefix(name, "kv_") && strings.HasSuffix(name, ".kvdata") {
			return 4, true
		}
		return 0, false
	}
	switch sub {
	case "object/obj.data", "xlog/xlog.profile", "status/status.pstat", "sum/sum.sum":
		return 4, true
	case "xlog/xlog.service", "alert/alert.alert", "span/span.data":
		return 2, true
	}
	return 0, false
}

// checkDataTails walks every data file sequentially after index references
// recorded the furthest referenced end.
func (in *inspector) checkDataTails() error {
	rels := make([]string, 0)
	for rel := range in.files {
		if _, ok := plainDataPrefix(rel); ok {
			rels = append(rels, rel)
		}
	}
	sort.Strings(rels)
	for _, rel := range rels {
		prefix, _ := plainDataPrefix(rel)
		d, err := in.dataFile(rel, prefix)
		if err != nil {
			return err
		}
		w, err := d.walk(in.ctx)
		if err != nil {
			if ctxErr := in.ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			in.c.add(SeverityError, "data.io", rel, -1, "%v", err)
			continue
		}
		in.setRecords(rel, w.records)
		if w.stop < d.size {
			if w.stop >= d.maxEnd {
				in.c.add(SeverityWarning, "data.truncated_tail", rel, w.stop, "%d bytes after the last indexed record are not a complete record (%s); typical of an interrupted append, unreachable by Java readers", d.size-w.stop, w.reason)
			} else {
				in.c.add(SeverityError, "data.unparsable", rel, w.stop, "sequential parse stopped inside the indexed region: %s", w.reason)
			}
		}
		if w.afterMaxEnd > 0 {
			in.c.add(SeverityWarning, "data.unindexed_tail", rel, d.maxEnd, "%d complete records after the last indexed record (data appended without index, e.g. abnormal stop)", w.afterMaxEnd)
		}
		if interior := w.records - w.afterMaxEnd - d.refs; interior > 0 {
			in.c.add(SeverityWarning, "data.unreferenced_records", rel, -1, "%d records before the last indexed record have no primary index reference", interior)
		} else if interior < 0 {
			in.c.add(SeverityInfo, "data.duplicate_references", rel, -1, "%d more primary index references than physical records", -interior)
		}
	}
	return nil
}

// checkZips validates count.dat, block numbering and every block's record chain.
func (in *inspector) checkZips() error {
	dates := map[string]bool{}
	for rel, e := range in.files {
		if e.Category == "xlog.zip" {
			dates[strings.SplitN(rel, "/", 2)[0]] = true
		}
	}
	for date := range in.zips {
		dates[date] = true
	}
	sorted := make([]string, 0, len(dates))
	for d := range dates {
		sorted = append(sorted, d)
	}
	sort.Strings(sorted)
	for _, date := range sorted {
		if err := in.checkZip(in.zip(date)); err != nil {
			return err
		}
	}
	return nil
}
func (in *inspector) checkZip(z *zipStore) error {
	numbers := make([]int64, 0, len(z.blocks))
	for n := range z.blocks {
		numbers = append(numbers, n)
	}
	sort.Slice(numbers, func(i, j int) bool { return numbers[i] < numbers[j] })
	last := int64(-1)
	if z.count > 0 {
		last = (z.count - 1) / zipBlockBytes
		for n := int64(0); n <= last; n++ {
			if !z.blocks[n] {
				in.c.add(SeverityError, "zip.missing_block", z.rel, n*zipBlockBytes, "xlog.%d missing below count.dat %d", n, z.count)
			}
		}
	}
	var records, linked int64
	for _, n := range numbers {
		if err := in.ctx.Err(); err != nil {
			return err
		}
		blockRel := fmt.Sprintf("%s/xlog.%d", z.rel, n)
		if z.count >= 0 && n > last {
			in.c.add(SeverityError, "zip.untracked_block", blockRel, -1, "block beyond count.dat %d (Java updates count before the asynchronous block store)", z.count)
			if _, err := z.block(n); err == nil {
				in.zipTornFinding(z, n, blockRel)
			}
			continue
		}
		b, err := z.block(n)
		if err != nil {
			var de *dataError
			if errors.As(err, &de) {
				in.c.add(SeverityError, de.code, blockRel, -1, "%s", de.detail)
			} else {
				in.c.add(SeverityError, "data.io", blockRel, -1, "%v", err)
			}
			continue
		}
		in.zipTornFinding(z, n, blockRel)
		if n == last {
			if want := z.count - n*zipBlockBytes; int64(len(b)) != want {
				in.c.add(SeverityError, "zip.count_mismatch", blockRel, -1, "uncompressed length %d, count.dat requires %d", len(b), want)
			}
		}
		var blockRecords int64
		for at := 0; at < len(b); {
			if len(b)-at < 8 {
				in.c.add(SeverityError, "zip.record_bounds", blockRel, int64(at), "%d trailing bytes shorter than a record header", len(b)-at)
				break
			}
			size := int(int8(b[at+5]))<<16 | int(b[at+6])<<8 | int(b[at+7])
			if size < 0 || size > len(b)-at-8 {
				in.c.add(SeverityError, "zip.record_bounds", blockRel, int64(at), "record length %d beyond block length %d", size, len(b))
				break
			}
			if next := readLong5(b[at : at+5]); next != 0 {
				linked++
				logical := n*zipBlockBytes + int64(at)
				if _, err := z.read(logical); err != nil {
					var de *dataError
					if errors.As(err, &de) {
						in.c.add(SeverityError, de.code, blockRel, int64(at), "%s", de.detail)
					}
				}
				// z.read may evict this block from the bounded cache; reload it.
				if b, err = z.block(n); err != nil {
					return err
				}
			}
			blockRecords++
			at += 8 + size
		}
		records += blockRecords
	}
	in.zipCountRecoverable(z)
	date := strings.SplitN(z.rel, "/", 2)[0]
	in.count(date, "zip_records", records)
	in.count(date, "zip_linked_records", linked)
	if records > z.refs {
		in.c.add(SeverityWarning, "zip.unreferenced_records", z.rel, -1, "%d of %d compressed records have no primary XLog/Profile index reference", records-z.refs, records)
	}
	return nil
}

func (in *inspector) closeData() {
	for _, d := range in.open {
		d.close()
	}
	in.open = nil
}

func (in *inspector) summary(rep *Report, dirs int) Summary {
	s := Summary{Files: len(rep.Files), Dirs: dirs, Statuses: map[string]int{}, Codes: in.c.counts,
		Dates: in.dates, Permanent: in.perm, Errors: in.c.bySev[SeverityError], Warnings: in.c.bySev[SeverityWarning], Infos: in.c.bySev[SeverityInfo]}
	for _, f := range rep.Files {
		s.Bytes += f.Bytes
		s.Statuses[f.Status]++
	}
	return s
}

// zipTornFinding reports an incomplete final gzip member of a decoded block.
// Its records are excluded from the block (reads into it fail); only the
// count.dat block and a rollover orphan qualify for the writer repair.
func (in *inspector) zipTornFinding(z *zipStore, n int64, blockRel string) {
	if t, ok := z.torn[n]; ok {
		in.c.add(SeverityError, "zip.torn_member", blockRel, t.end, "incomplete final gzip member of %d bytes after %d complete members (a crash inside a member append)", t.bytes, t.members)
	}
}

// zipCompleteRecords reports whether b[start:] is only complete next=0
// records and returns the length of the first one.
func zipCompleteRecords(b []byte, start int) (records, first int, ok bool) {
	first = -1
	for at := start; at < len(b); records++ {
		if len(b)-at < 8 || readLong5(b[at:at+5]) != 0 || b[at+5]&0x80 != 0 {
			return records, first, false
		}
		n := int(b[at+5])<<16 | int(b[at+6])<<8 | int(b[at+7])
		if at+8+n > len(b) {
			return records, first, false
		}
		if first < 0 {
			first = n
		}
		at += 8 + n
	}
	return records, first, true
}

// zipCountRecoverable mirrors the Go writer's load-time repairs (N =
// count/32MiB, start = count%32MiB):
//   - block N, when present, may end in a torn gzip member (truncated by the
//     writer); its complete members hold at least start bytes and only
//     complete next=0 records after start (count advanced);
//   - at most one orphan block N+1, and only when no block >= N+2 exists. It
//     may be empty or end in a torn member (a crash inside the rollover);
//     records in its complete members must be complete next=0 records whose
//     first one needed a rollover (start+8+len > 32MiB).
//
// The error findings stay; each covered block gets zip.auto_recoverable_block
// so --accept-recoverable accepts exactly those blocks.
func (in *inspector) zipCountRecoverable(z *zipStore) {
	if z.count < 0 {
		return
	}
	number, start := z.count/zipBlockBytes, int(z.count%zipBlockBytes)
	for n := range z.blocks {
		if n >= number+2 {
			return
		}
	}
	var tail int
	covered := []int64{}
	if z.blocks[number] {
		b, err := z.block(number)
		if err != nil || len(b) < start {
			return
		}
		records, _, ok := zipCompleteRecords(b, start)
		if !ok {
			return
		}
		if records > 0 {
			tail = len(b) - start
		}
		if _, torn := z.torn[number]; records > 0 || torn {
			covered = append(covered, number)
		}
	} else if start > 0 {
		return
	}
	orphan := ""
	if z.blocks[number+1] {
		b, err := z.block(number + 1)
		if err != nil {
			return
		}
		records, first, ok := zipCompleteRecords(b, 0)
		if !ok || (records > 0 && start+8+first <= zipBlockBytes) {
			return
		}
		covered = append(covered, number+1)
		orphan = fmt.Sprintf("; orphan rollover block xlog.%d (%d records) is kept until the next rollover truncates it", number+1, records)
	}
	if len(covered) == 0 {
		return
	}
	for _, n := range covered {
		in.c.add(SeverityInfo, "zip.auto_recoverable_block", fmt.Sprintf("%s/xlog.%d", z.rel, n), -1, "covered by the Go writer count.dat recovery")
	}
	next := number*zipBlockBytes + int64(start+tail)
	repair := fmt.Sprintf("advances count %d -> %d, leaving the unindexed records unreferenced", z.count, next)
	if next == z.count {
		repair = fmt.Sprintf("keeps count %d", z.count)
	}
	if t, ok := z.torn[number]; ok {
		repair = fmt.Sprintf("truncates the torn final member of xlog.%d (%d bytes) and %s", number, t.bytes, repair)
	}
	in.c.add(SeverityInfo, "zip.auto_recoverable", z.rel+"/count.dat", -1, "the next Go writer append %s%s; Java would overwrite them (intended difference)", repair, orphan)
}

// checkTagChains runs the read-only tag count chain check on every First and
// Next key file. A broken chain (from a write torn by a crash or, before the
// fix, by a shutdown) makes tag queries of that bucket wrong or ErrCorrupt;
// the data is copied unchanged, so it is a warning. A Java DB copied while
// the server ran or after it crashed shows the same findings (the original
// saves its hash table every 4s), so stop the writer before inspecting.
func (in *inspector) checkTagChains() error {
	var bases []string
	for rel, e := range in.files {
		// Retention trash and quarantine are never DB content.
		if e.Status == StatusExcluded || !strings.Contains(rel, "/tagcnt/") || !strings.HasSuffix(rel, ".kfile") {
			continue
		}
		switch path.Base(rel) {
		case "first.kfile", "next.kfile":
			bases = append(bases, strings.TrimSuffix(rel, ".kfile"))
		}
	}
	sort.Strings(bases)
	for _, base := range bases {
		if err := in.ctx.Err(); err != nil {
			return err
		}
		rep, err := storage.VerifyTagCountFiles(in.abs(base), path.Base(base) == "first")
		if err != nil {
			in.c.add(SeverityWarning, "tagcount.unreadable", base+".kfile", -1, "tag count chain check: %v", err)
			continue
		}
		for _, p := range rep.Problems {
			in.c.add(SeverityWarning, "tagcount."+p.Kind, base+".kfile", p.Pos, "%s", p.Detail)
		}
		if rep.MoreProblems > 0 {
			in.c.add(SeverityWarning, "tagcount.more", base+".kfile", -1, "%d more chain problems", rep.MoreProblems)
		}
	}
	return nil
}
