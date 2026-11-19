// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
package migrate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

// QueryResult compares Go storage queries on the copy with an independent
// read-only index/data walk of the source.
type QueryResult struct {
	Compared    map[string]int64 `json:"compared"`
	Partial     []string         `json:"partial,omitempty"`
	NotCompared []string         `json:"not_compared"`
	Problems    []Problem        `json:"problems,omitempty"`
}

type queryOptions struct {
	Location *time.Location
	Now      int64
	Limit    int64 // records per date and window category; 0 means all
	Sample   int   // keyed lookups per date and category
}

const queryWindow = int64(time.Hour / time.Millisecond)

type digest struct {
	h hash.Hash
	n int64
}

func newDigest() *digest { return &digest{h: sha256.New()} }
func (d *digest) add(b []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(b)))
	d.h.Write(n[:])
	d.h.Write(b)
	d.n++
}
func (d *digest) sum() string { return fmt.Sprintf("%d:%s", d.n, hex.EncodeToString(d.h.Sum(nil))) }

type queryCompare struct {
	// recoverable index bases are opened writable by storage.Store, which
	// would relink them and rewrite the copy's header; they are not compared.
	recoverable map[string]bool
	ctx         context.Context
	opt         queryOptions
	src         *inspector // read-only source reader; its collector is discarded
	def         *storage.Store
	daily       *storage.Store
	res         *QueryResult // current group (use)
	problems    *collector
	groups      map[string]*queryGroup
}

var errStopScan = errors.New("query comparison limit")

func reencode(raw []byte) ([]byte, error) {
	p, err := protocol.DecodePack(raw, protocol.Limits{})
	if err != nil {
		return nil, err
	}
	return protocol.EncodePack(p, protocol.Limits{})
}

// queryNotCompared lists query families without a Go/source comparison.
var queryNotCompared = []string{"summary", "span", "counter", "tagcount", "visitor", "stack", "batch"}

// compareQueries opens the staging copy with storage.Open. Callers must finish
// checksum verification before and confirm the copy is unchanged afterwards.
func compareQueries(ctx context.Context, srcRoot, staging string, rep *Report, opt queryOptions) (*QueryResult, error) {
	groups, err := compareQueryGroups(ctx, srcRoot, staging, rep, opt, nil)
	if err != nil {
		return nil, err
	}
	return mergeQueryResults(groups), nil
}

// queryStep is one per-date comparison; its group is date/sub (see groupOf).
type queryStep struct {
	sub string
	run func(*queryCompare, string) error
}

var querySteps = []queryStep{
	{"object", (*queryCompare).objects}, {"xlog", (*queryCompare).xlogs}, {"xlog", (*queryCompare).xlogByTxID},
	{"xlog", (*queryCompare).profiles}, {"status", (*queryCompare).statuses}, {"alert", (*queryCompare).alerts},
	{"text", (*queryCompare).dailyText},
}

// compareQueryGroups runs the comparisons of the groups in only (nil: all)
// and returns one result per group; every group of only gets a result, empty
// when it has nothing to compare. Each step reads only its group's files.
func compareQueryGroups(ctx context.Context, srcRoot, staging string, rep *Report, opt queryOptions, only map[string]bool) (map[string]*QueryResult, error) {
	// Registered first, so it runs after both Stores closed: their clean
	// Close writes the marker and the checkpoint log, which must not change
	// the staging tree.
	defer func() {
		for _, name := range append([]string{goCleanName}, goCheckpointNames...) {
			os.Remove(filepath.Join(staging, name))
		}
	}()
	def, err := storage.Open(storage.Config{Directory: staging, Location: opt.Location})
	if err != nil {
		return nil, err
	}
	defer def.Close()
	daily, err := storage.Open(storage.Config{Directory: staging, Location: opt.Location, TextDailyService: true, TextDailyAPI: true, TextDailyUA: true})
	if err != nil {
		return nil, err
	}
	defer daily.Close()
	src := newInspector(ctx, InspectOptions{Root: srcRoot, Location: opt.Location, Now: opt.Now}, srcRoot)
	defer src.closeData()
	q := &queryCompare{ctx: ctx, opt: opt, src: src, def: def, daily: daily, recoverable: map[string]bool{}, groups: map[string]*queryGroup{}}
	for _, p := range rep.Problems {
		// A writable Go open relinks (auto_recoverable) or tombstones the
		// refused orphans (relink_refused); either would modify the copy.
		if p.Code == "index.auto_recoverable" || p.Code == "index.relink_refused" {
			q.recoverable[strings.TrimSuffix(strings.TrimSuffix(p.Path, ".kfile"), ".k2file")] = true
		}
	}
	for g := range only {
		q.use(g)
	}
	dateSet := map[string]bool{}
	for d := range rep.Summary.Dates {
		dateSet[d] = true
	}
	// A selected group's date runs even when no inspected group counted
	// records for it (as a full run would through the date's other groups).
	for g := range only {
		if parts := strings.SplitN(g, "/", 2); len(parts) == 2 && isDateDir(parts[0]) {
			dateSet[parts[0]] = true
		}
	}
	dates := make([]string, 0, len(dateSet))
	for d := range dateSet {
		if d != permanentDir {
			dates = append(dates, d)
		}
	}
	sort.Strings(dates)
	for _, date := range dates {
		for _, step := range querySteps {
			g := date + "/" + step.sub
			if only != nil && !only[g] {
				continue
			}
			q.use(g)
			if err := step.run(q, date); err != nil {
				return nil, err
			}
		}
	}
	if err := q.permanent(rep, only); err != nil {
		return nil, err
	}
	out := make(map[string]*QueryResult, len(q.groups))
	for g, qg := range q.groups {
		qg.res.Problems = qg.problems.finish()
		out[g] = qg.res
	}
	return out, ctx.Err()
}

// mergeQueryResults combines group results in group order.
func mergeQueryResults(groups map[string]*QueryResult) *QueryResult {
	res := &QueryResult{Compared: map[string]int64{}, NotCompared: queryNotCompared}
	names := make([]string, 0, len(groups))
	for g := range groups {
		names = append(names, g)
	}
	sort.Strings(names)
	for _, g := range names {
		r := groups[g]
		for k, v := range r.Compared {
			res.Compared[k] += v
		}
		res.Partial = append(res.Partial, r.Partial...)
		res.Problems = append(res.Problems, r.Problems...)
	}
	return res
}

type queryGroup struct {
	res      *QueryResult
	problems *collector
}

// use directs the following comparisons to group g.
func (q *queryCompare) use(g string) {
	qg := q.groups[g]
	if qg == nil {
		qg = &queryGroup{res: &QueryResult{Compared: map[string]int64{}, NotCompared: queryNotCompared}, problems: newCollector()}
		q.groups[g] = qg
	}
	q.res, q.problems = qg.res, qg.problems
}

// skip reports a comparison left out because its index is auto-recoverable.
func (q *queryCompare) skip(base string) bool {
	if !q.recoverable[base] {
		return false
	}
	q.problems.add(SeverityInfo, "query.not_compared", base, -1, "index has a stale header, torn tail or refused relink that a writable Go open would rewrite; comparing would modify the copy")
	return true
}

func (q *queryCompare) mismatch(category, date, format string, args ...any) {
	q.problems.add(SeverityError, "query.mismatch", date+"/"+category, -1, format, args...)
}

// failed separates cancellation from comparison failures.
func (q *queryCompare) failed(category, date string, err error) error {
	if ctxErr := q.ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	q.problems.add(SeverityError, "query.error", date+"/"+category, -1, "%v", err)
	return nil
}

func (q *queryCompare) openSrc(rel string, timed bool) (*legacyindex.Index, error) {
	idx, err := legacyindex.OpenReadOnly(q.src.abs(rel), timed, q.opt.Location)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return idx, err
}

func (q *queryCompare) objects(date string) error {
	base := date + "/object/obj"
	if q.skip(base) {
		return nil
	}
	idx, err := q.openSrc(base, false)
	if err != nil {
		return q.failed("object", date, err)
	}
	want := newDigest()
	if idx != nil {
		spec := specFor(base, false)
		err = idx.Scan(func(r legacyindex.Record) error {
			raw, _, err := q.src.readPayload(spec, spec.data, readLong5(r.Value))
			if err != nil {
				return err
			}
			dec := protocol.NewDecoder(raw, protocol.Limits{})
			p := dec.Pack()
			if dec.Err() != nil {
				return dec.Err()
			}
			b, err := protocol.EncodePack(p, protocol.Limits{})
			want.add(b)
			return err
		})
		idx.Close()
		if err != nil {
			return q.failed("object", date, err)
		}
	}
	got := newDigest()
	objects, err := q.def.ObjectsContext(q.ctx, date)
	if err != nil {
		return q.failed("object", date, err)
	}
	for _, o := range objects {
		b, err := protocol.EncodePack(o, protocol.Limits{})
		if err != nil {
			return q.failed("object", date, err)
		}
		got.add(b)
	}
	if want.sum() != got.sum() {
		q.mismatch("object", date, "Objects: source %s, Go storage %s", want.sum(), got.sum())
	}
	if idx == nil && got.n == 0 {
		// Nothing to compare: keep the category absent like the other steps,
		// so a run limited to existing groups reports the same counts.
		return nil
	}
	q.res.Compared["object"] += got.n
	return nil
}

// timedCompare walks one Java day in hourly windows, splitting a window when
// the Go query limit is reached. Both sides use V1 order and deduplication.
func (q *queryCompare) timedCompare(category, date, base string, query func(from, to int64) ([][]byte, error)) error {
	if q.skip(base) {
		return nil
	}
	idx, err := q.openSrc(base, true)
	if err != nil {
		return q.failed(category, date, err)
	}
	if idx == nil {
		return nil
	}
	defer idx.Close()
	start, err := scoutertime.ParseDate(date, q.opt.Location)
	if err != nil {
		return q.failed(category, date, err)
	}
	spec := specFor(base, false)
	var compared int64
	var window func(from, to int64) (bool, error)
	window = func(from, to int64) (bool, error) {
		got, err := query(from, to)
		if errors.Is(err, storage.ErrLimit) && to > from {
			mid := from + (to-from)/2
			if ok, err := window(from, mid); !ok || err != nil {
				return ok, err
			}
			return window(mid+1, to)
		}
		if err != nil {
			return false, err
		}
		want := newDigest()
		err = idx.ScanRange(from, to, false, func(r legacyindex.Record) error {
			raw, _, err := q.src.readPayload(spec, spec.data, readLong5(r.Value))
			if err != nil {
				return err
			}
			b, err := reencode(raw)
			want.add(b)
			return err
		})
		if err != nil {
			return false, err
		}
		have := newDigest()
		for _, b := range got {
			have.add(b)
		}
		if want.sum() != have.sum() {
			q.mismatch(category, date, "window %d..%d: source %s, Go storage %s", from, to, want.sum(), have.sum())
			return false, nil
		}
		compared += have.n
		return true, nil
	}
	end := start + scoutertime.DayMillis - 1
	for from := start; from <= end; from += queryWindow {
		if q.opt.Limit > 0 && compared >= q.opt.Limit {
			q.res.Partial = append(q.res.Partial, date+"/"+category)
			break
		}
		ok, err := window(from, min(from+queryWindow-1, end))
		if err != nil {
			return q.failed(category, date, err)
		}
		if !ok {
			break
		}
	}
	q.res.Compared[category] += compared
	return nil
}

func encodeAll[T protocol.Pack](packs []T, err error) ([][]byte, error) {
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(packs))
	for _, p := range packs {
		b, err := protocol.EncodePack(p, protocol.Limits{})
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

func (q *queryCompare) xlogs(date string) error {
	return q.timedCompare("xlog", date, date+"/xlog/xlog_tim", func(from, to int64) ([][]byte, error) {
		return encodeAll(q.def.XLogsContext(q.ctx, date, from, to, false))
	})
}
func (q *queryCompare) statuses(date string) error {
	return q.timedCompare("status", date, date+"/status/status_tim", func(from, to int64) ([][]byte, error) {
		return encodeAll(q.def.StatusesContext(q.ctx, date, from, to))
	})
}
func (q *queryCompare) alerts(date string) error {
	return q.timedCompare("alert", date, date+"/alert/alert_tim", func(from, to int64) ([][]byte, error) {
		return encodeAll(q.def.AlertsContext(q.ctx, date, from, to))
	})
}

// sampleKeys returns up to n distinct non-deleted keys in physical order.
func sampleKeys(idx *legacyindex.Index, n int) ([][]byte, error) {
	seen := map[string]bool{}
	var out [][]byte
	err := idx.Scan(func(r legacyindex.Record) error {
		if len(out) >= n {
			return errStopScan
		}
		if !seen[string(r.Key)] {
			seen[string(r.Key)] = true
			out = append(out, r.Key)
		}
		return nil
	})
	if errors.Is(err, errStopScan) {
		err = nil
	}
	return out, err
}

func (q *queryCompare) xlogByTxID(date string) error {
	base := date + "/xlog/xlog_tid"
	if q.skip(base) {
		return nil
	}
	idx, err := q.openSrc(base, false)
	if err != nil {
		return q.failed("xlog_txid", date, err)
	}
	if idx == nil {
		return nil
	}
	defer idx.Close()
	keys, err := sampleKeys(idx, q.opt.Sample)
	if err != nil {
		return q.failed("xlog_txid", date, err)
	}
	spec := specFor(base, false)
	for _, key := range keys {
		value, err := idx.Get(key)
		if err != nil {
			return q.failed("xlog_txid", date, err)
		}
		raw, _, err := q.src.readPayload(spec, spec.data, readLong5(value))
		if err != nil {
			return q.failed("xlog_txid", date, err)
		}
		want, err := reencode(raw)
		if err != nil {
			return q.failed("xlog_txid", date, err)
		}
		txid := int64(binary.BigEndian.Uint64(key))
		p, err := q.def.XLogContext(q.ctx, date, txid)
		if err != nil {
			return q.failed("xlog_txid", date, err)
		}
		var got []byte
		if p != nil {
			got, _ = protocol.EncodePack(p, protocol.Limits{})
		}
		if !bytes.Equal(want, got) {
			q.mismatch("xlog_txid", date, "XLog txid %d differs", txid)
			return nil
		}
		q.res.Compared["xlog_txid"]++
	}
	return nil
}

func (q *queryCompare) profiles(date string) error {
	base := date + "/xlog/xlog_profile"
	if q.skip(base) {
		return nil
	}
	idx, err := q.openSrc(base, false)
	if err != nil {
		return q.failed("profile", date, err)
	}
	if idx == nil {
		return nil
	}
	defer idx.Close()
	keys, err := sampleKeys(idx, q.opt.Sample)
	if err != nil {
		return q.failed("profile", date, err)
	}
	spec := specFor(base, false)
	for _, key := range keys {
		var offsets []int64
		err := idx.ScanKey(key, func(r legacyindex.Record) error {
			offsets = append(offsets, readLong5(r.Value))
			return nil
		})
		if err != nil {
			return q.failed("profile", date, err)
		}
		sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		want := newDigest()
		for _, pos := range offsets {
			b, _, err := q.src.readPayload(spec, spec.data, pos)
			if err != nil {
				return q.failed("profile", date, err)
			}
			want.add(b)
		}
		txid := int64(binary.BigEndian.Uint64(key))
		chunks, err := q.def.ProfileChunksContext(q.ctx, date, txid)
		if err != nil {
			return q.failed("profile", date, err)
		}
		got := newDigest()
		for _, c := range chunks {
			got.add(c)
		}
		if want.sum() != got.sum() {
			q.mismatch("profile", date, "ProfileChunks txid %d differ", txid)
			return nil
		}
		q.res.Compared["profile"]++
	}
	return nil
}

var dailyTextDivs = []string{"error", "service", "apicall", "ua"} // TextTypes eligible for daily tables

func (q *queryCompare) dailyText(date string) error {
	if q.skip(date + "/text/text") {
		return nil
	}
	idx, err := q.openSrc(date+"/text/text", false)
	if err != nil {
		return q.failed("text_daily", date, err)
	}
	if idx == nil {
		return nil
	}
	defer idx.Close()
	divs := map[uint32]string{}
	for _, d := range dailyTextDivs {
		divs[crc32.ChecksumIEEE([]byte(d))] = d
	}
	var compared, unknown int64
	err = idx.Scan(func(r legacyindex.Record) error {
		if compared >= int64(q.opt.Sample) {
			return errStopScan
		}
		div, ok := divs[binary.BigEndian.Uint32(r.Key[:4])]
		if !ok {
			unknown++
			return nil
		}
		hash := int32(binary.BigEndian.Uint32(r.Key[4:]))
		text, found, err := q.daily.TextContext(q.ctx, date, div, hash)
		if err != nil {
			return err
		}
		// Go storage, like Java IndexKeyFile.get, returns the newest matching key.
		want, err := idx.Get(r.Key)
		if err != nil {
			return err
		}
		if !found || text != string(want) {
			q.mismatch("text_daily", date, "daily text %s/%d differs", div, hash)
			return errStopScan
		}
		compared++
		return nil
	})
	if err != nil && !errors.Is(err, errStopScan) {
		return q.failed("text_daily", date, err)
	}
	if unknown > 0 {
		q.problems.add(SeverityInfo, "query.text_unknown_div", date+"/text/text.kfile", -1, "%d daily text keys use a div outside %v and were not queried", unknown, dailyTextDivs)
	}
	q.res.Compared["text_daily"] += compared
	return nil
}

// permanent compares 00000000 text dictionaries and KV namespaces by sample.
func (q *queryCompare) permanent(rep *Report, only map[string]bool) error {
	for _, f := range rep.Files {
		text := f.Category == "text.permanent" && strings.HasSuffix(f.Path, ".kfile")
		kv := f.Category == "kv" && strings.HasSuffix(f.Path, ".k2file")
		g := groupOf(f.Path)
		if (!text && !kv) || (only != nil && !only[g]) {
			continue
		}
		q.use(g)
		switch {
		case text:
			div := strings.TrimPrefix(strings.TrimSuffix(path.Base(f.Path), ".kfile"), "text_")
			if div == "error" { // Go and Java always read error text from the daily table.
				q.problems.add(SeverityInfo, "query.not_compared", f.Path, -1, "permanent error text is unreachable by the Java/Go daily error lookup")
				continue
			}
			if err := q.permanentText(strings.TrimSuffix(f.Path, ".kfile"), div); err != nil {
				return err
			}
		case kv:
			space := strings.TrimPrefix(strings.TrimSuffix(path.Base(f.Path), ".k2file"), "kv_")
			if err := q.keyValues(strings.TrimSuffix(f.Path, ".k2file"), space); err != nil {
				return err
			}
		}
	}
	return nil
}

func (q *queryCompare) permanentText(base, div string) error {
	if q.skip(base) {
		return nil
	}
	idx, err := q.openSrc(base, false)
	if err != nil {
		return q.failed("text_permanent", permanentDir, err)
	}
	if idx == nil {
		return nil
	}
	defer idx.Close()
	keys, err := sampleKeys(idx, q.opt.Sample)
	if err != nil {
		return q.failed("text_permanent", permanentDir, err)
	}
	spec := specFor(base, false)
	for _, key := range keys {
		value, err := idx.Get(key)
		if err != nil {
			return q.failed("text_permanent", permanentDir, err)
		}
		raw, _, err := q.src.readPayload(spec, spec.data, readLong5(value))
		if err != nil {
			return q.failed("text_permanent", permanentDir, err)
		}
		hash := int32(binary.BigEndian.Uint32(key))
		// The date only satisfies the Store API; permanent text ignores it.
		text, found, err := q.def.TextContext(q.ctx, "20000101", div, hash)
		if err != nil {
			return q.failed("text_permanent", permanentDir, err)
		}
		if !found || text != string(raw) {
			q.mismatch("text_permanent", permanentDir, "text %s/%d differs", div, hash)
			return nil
		}
		q.res.Compared["text_permanent"]++
	}
	return nil
}

func (q *queryCompare) keyValues(base, space string) error {
	if q.skip(base) {
		return nil
	}
	idx, err := legacyindex.OpenTTLReadOnly(q.ctx, q.src.abs(base))
	if err != nil {
		return q.failed("kv", permanentDir, err)
	}
	defer idx.Close()
	seen := map[string]bool{}
	var keys [][]byte
	err = idx.ScanContext(q.ctx, func(r legacyindex.TTLRecord) error {
		if len(keys) >= q.opt.Sample {
			return errStopScan
		}
		if len(r.Key) > 0 && !seen[string(r.Key)] {
			seen[string(r.Key)] = true
			keys = append(keys, r.Key)
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStopScan) {
		return q.failed("kv", permanentDir, err)
	}
	spec := specFor(base, true)
	for _, key := range keys {
		offset, err := idx.GetContext(q.ctx, key, q.opt.Now)
		if err != nil {
			return q.failed("kv", permanentDir, err)
		}
		var want *string
		if offset != nil {
			raw, _, err := q.src.readPayload(spec, spec.data, readLong5(offset))
			if err != nil {
				return q.failed("kv", permanentDir, err)
			}
			s := string(raw)
			want = &s
		}
		got, found, err := q.def.KeyValueContext(q.ctx, space, string(key), q.opt.Now)
		if err != nil {
			return q.failed("kv", permanentDir, err)
		}
		if (want != nil) != found || (want != nil && *want != got) {
			q.mismatch("kv", permanentDir, "KV %s/%q differs", space, key)
			return nil
		}
		q.res.Compared["kv"]++
	}
	return nil
}
