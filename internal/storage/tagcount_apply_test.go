package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// tagApplyGolden are the digests the tag apply path produced before the
// compact XLog inbox entry and the value-keyed pending index (the earlier
// string-keyed pending map and per-tag key encoding), on tagApplyWorkload.
// files covers every tag file (First/Next key, hash and data files, top100
// caches) byte for byte; state the pending and First accounting after each
// drain, the counters and the query results.
var tagApplyGolden = map[string][2]string{
	"default": {"eb229eca5d5be05ffaa3f3a1796317ffa909251132f9d8891dd7dfbc3f2e29b0", "29b99fc31193e2a3f5fbe849110ada6f0adc62a9cf2d5a3b36553231579d35c4"},
	"bounded": {"e89248fbb6fdcffa0c513e79ffd3d8380ba40bbe08d9a6a6d92ec2a603a2fbff", "f62693d064d1cb2f2312036f420407daa50f2a48638aa55c7d0f287c01549c5b"},
}

// tagApplyWorkload contributes XLogs with every tag field and alerts, to two
// dates and two object types over several hours: more than 100 values per
// tag (First then Next), negative and large decimals, text values with
// non-ASCII and one alert title past the 32767-byte key bound.
func tagApplyWorkload(t *testing.T, s *Store, round int) {
	t.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(int64(round + 1)))
	days := []time.Time{tagFlushTestDay, tagFlushTestDay.AddDate(0, 0, 1)}
	for i := 0; i < 3000; i++ {
		day := days[rng.Intn(2)]
		objType := []string{"java", "node"}[rng.Intn(2)]
		tm := day.Add(time.Duration(rng.Intn(5)+round*3)*time.Hour + time.Duration(rng.Intn(3600))*time.Second).UnixMilli()
		x := &protocol.XLogPack{
			EndTime: tm, ObjHash: int32(rng.Intn(130)) - 3, Service: int32(rng.Intn(260)) - 5,
			Elapsed: int32(rng.Intn(50)) - 5, KBytes: int32(rng.Intn(9)) - 2, SQLTime: int32(rng.Intn(30)) - 3, SQLCount: int32(rng.Intn(6)) - 1,
			APICallTime: int32(rng.Intn(20)) - 4, APICallCount: int32(rng.Intn(4)) - 1,
		}
		if rng.Intn(5) == 0 {
			x.Error = int32(rng.Intn(150)) - 10
		}
		if rng.Intn(2) == 0 {
			x.Group = int32(rng.Intn(140))
		}
		if rng.Intn(2) == 0 {
			x.UserAgent = int32(rng.Intn(120)) - 7
		}
		if rng.Intn(3) == 0 {
			x.Referer = int32(rng.Intn(110))
		}
		if rng.Intn(3) == 0 {
			x.City = int32(rng.Intn(105))
		}
		if rng.Intn(2) == 0 {
			x.CountryCode = []string{"KR", "US", "日本", ""}[rng.Intn(4)] + fmt.Sprint(rng.Intn(40))
		}
		if rng.Intn(3) == 0 {
			x.Login = int32(rng.Intn(400)) - 200
		}
		if rng.Intn(4) == 0 {
			x.Desc = int32(rng.Int31()) * int32(1-2*rng.Intn(2))
		}
		if err := s.PutXLogTagsContext(ctx, objType, x); err != nil {
			t.Fatal(round, i, err)
		}
		if i%4 == 0 {
			a := &protocol.AlertPack{Time: tm, ObjHash: int32(rng.Intn(20)), Level: byte(rng.Intn(4)), Title: fmt.Sprintf("경고-%d", rng.Intn(150))}
			if i == 1000 {
				a.Title = strings.Repeat("x", 40000) // the tags before it stay applied (ApplyErrors)
			}
			if err := s.PutAlertTagsContext(ctx, objType, a); err != nil {
				t.Fatal(round, i, err)
			}
		}
	}
}

// tagApplyState describes the pending tables after a drain: per table its
// pending deltas (key, First, hours, minute counts) and First sets, and the
// global byte accounting.
func tagApplyState(s *Store) string {
	var b strings.Builder
	st := s.tagCounts
	if st == nil {
		return "nil"
	}
	fmt.Fprintf(&b, "pending=%d metadata=%d cachePaths=%d\n", st.pendingBytes, st.metadataBytes, len(st.cachePaths))
	paths := make([]string, 0, len(st.tables))
	for p := range st.tables {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		table := st.tables[p]
		rel, err := filepath.Rel(s.config.Directory, p)
		if err != nil {
			return err.Error()
		}
		var lines []string
		for _, d := range table.pending {
			h := sha256.New()
			for hour, counts := range d.hours {
				if counts != nil {
					fmt.Fprintf(h, "%d:%v;", hour, *counts)
				}
			}
			lines = append(lines, fmt.Sprintf("%x %v %d %x", d.key, d.first, d.bytes, h.Sum(nil)[:8]))
		}
		sort.Strings(lines)
		var firsts []string
		for tag, set := range table.first {
			for id := range set {
				firsts = append(firsts, fmt.Sprintf("%d %x", tag, id))
			}
		}
		sort.Strings(firsts)
		fmt.Fprintf(&b, "%s meta=%d\n%s\n%s\n", rel, table.metadataBytes, strings.Join(lines, "\n"), strings.Join(firsts, "\n"))
	}
	return b.String()
}

// tagCheckTexts checks that each table's Text ids are those of its pending
// Text deltas, one id per text, with as many references as deltas, and with
// empty set the tables hold nothing pending. The caller holds the tag gate.
func tagCheckTexts(s *Store, empty bool) error {
	if s.tagCounts == nil {
		return nil
	}
	for path, table := range s.tagCounts.tables {
		if empty && (len(table.pending) != 0 || len(table.texts) != 0) {
			return fmt.Errorf("%s: left after the flush: %d deltas, %d texts", path, len(table.pending), len(table.texts))
		}
		refs := map[string]int{}
		for id, d := range table.pending {
			if id != d.id {
				return fmt.Errorf("%s: delta under another id %v %v", path, id, d.id)
			}
			if id.kind != protocol.ValueText {
				continue
			}
			if ref := table.texts[d.text]; ref == nil || ref.id != id.n {
				return fmt.Errorf("%s: text %q id %d, table %v", path, d.text, id.n, ref)
			}
			refs[d.text]++
		}
		if len(refs) != len(table.texts) {
			return fmt.Errorf("%s: %d texts, %d used by deltas", path, len(table.texts), len(refs))
		}
		for text, n := range refs {
			if table.texts[text].refs != n {
				return fmt.Errorf("%s: text %q refs %d, deltas %d", path, text, table.texts[text].refs, n)
			}
		}
	}
	return nil
}

func tagTreeDigest(t *testing.T, root string) string {
	t.Helper()
	h := sha256.New()
	var names []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !tagUnder(path) {
			return err
		}
		names = append(names, path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	for _, name := range names {
		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		rel, _ := filepath.Rel(root, name)
		fmt.Fprintf(h, "%s %d\n", rel, len(b))
		h.Write(b)
	}
	if len(names) < 20 {
		t.Fatal("too few tag files", len(names))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// The apply path writes the same tag files, keeps the same pending and First
// state and answers the same queries as the implementation it replaced
// (tagApplyGolden), with the default pending bound and with one small enough
// to drop entries and flush inline.
func TestTagApplyMatchesPreviousImplementation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		maxBytes int
	}{{"default", 0}, {"bounded", 7 << 20}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			s, err := Open(Config{Directory: root, Location: time.UTC, HashSizeBytes: 4096, MaxQueryBytes: tc.maxBytes})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			state := sha256.New()
			for round := 0; round < 3; round++ {
				tagApplyWorkload(t, s, round)
				lockTags(s)
				fmt.Fprintln(state, tagApplyState(s))
				err = tagCheckTexts(s, false)
				s.tags.mu.Unlock()
				if err != nil {
					t.Fatal(round, err)
				}
				if err = s.flushTagCounts(ctx); err != nil {
					t.Fatal(round, err)
				}
				lockTags(s)
				err = tagCheckTexts(s, true)
				s.tags.mu.Unlock()
				if err != nil {
					t.Fatal(round, err)
				}
				if round < 2 {
					continue
				}
				for _, day := range []string{"20261013", "20261014"} {
					for _, objType := range []string{"java", "node"} {
						for _, tag := range [][2]string{{"service", "service"}, {"service", "nation"}, {"service", "desc"}, {"alert", "title"}} {
							total, err := s.TagValuesContext(ctx, day, objType, tag[0], tag[1])
							fmt.Fprintln(state, day, objType, tag, total, err)
						}
						for _, q := range []struct {
							name string
							v    protocol.Value
						}{{"service", textHash(7)}, {"service-time-sum", textHash(250)}, {"nation", protocol.Value{Type: protocol.ValueText, Text: "KR3"}}, {"login", protocol.Value{Type: protocol.ValueDecimal, Int: -17}}} {
							series, err := s.TagValueSeriesContext(ctx, day, objType, "service", q.name, q.v)
							fmt.Fprintln(state, day, objType, q.name, tagSum(series), len(series), err)
						}
					}
				}
			}
			for _, day := range []string{"20261013", "20261014"} {
				for _, objType := range []string{"java", "node"} {
					if err = s.MakeTagTop100Context(ctx, day, objType); err != nil {
						t.Fatal(err)
					}
					total, err := s.TagValuesContext(ctx, day, objType, "service", "service")
					fmt.Fprintln(state, day, objType, total, err)
				}
			}
			stats := s.TagCountStats()
			fmt.Fprintln(state, stats.InlineFlushes, stats.PendingDropped, stats.ApplyErrors, stats.DroppedDeltas)
			if stats.ApplyErrors == 0 || tc.maxBytes > 0 && stats.PendingDropped == 0 {
				t.Fatalf("workload misses a case: %+v", stats)
			}
			got := [2]string{tagTreeDigest(t, root), hex.EncodeToString(state.Sum(nil))}
			if want := tagApplyGolden[tc.name]; got != want {
				t.Fatalf("files/state digests %q, want %q", got, want)
			}
		})
	}
}

func tagAlertAt(t *testing.T, s *Store, objType string, tm int64, title string) {
	t.Helper()
	if err := s.PutAlertTagsContext(context.Background(), objType, &protocol.AlertPack{Time: tm, ObjHash: 1, Level: 1, Title: title}); err != nil {
		t.Fatal(err)
	}
}

// A flush pass resolves its snapshot by value keys between holds. When the
// table is evicted and made again meanwhile, its new Text values get ids no
// snapshot item has: they stay pending for the next pass, as a new key
// behind the pass does, instead of being written in another key's place.
func TestTagTextIDsNotReusedAfterTableRemade(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := tagFlushTestDay.Add(7 * time.Hour).UnixMilli()
	for i := 0; i < 10; i++ {
		tagAlertAt(t, s, "a", tm, fmt.Sprintf("K%d", i))
	}
	lockTags(s) // applied before the pass, so its holds are the flush's
	s.tags.mu.Unlock()
	s.tagHoldSteps = 1
	remade := false
	s.tagBetweenHolds = func() {
		if remade {
			return
		}
		remade = true
		// 30 other tables evict "a", the smallest path; "a" is made again.
		for i := 1; i <= 30; i++ {
			if err := s.PutXLogTagsContext(ctx, fmt.Sprintf("b%02d", i), &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: 1}); err != nil {
				t.Error(err)
			}
		}
		lockTags(s)
		s.tags.mu.Unlock()
		for i := 0; i < 5; i++ {
			tagAlertAt(t, s, "a", tm, fmt.Sprintf("U%d", i))
		}
		lockTags(s)
		s.tags.mu.Unlock()
	}
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	if !remade {
		t.Fatal("pass ran in one hold")
	}
	lockTags(s)
	table := s.tagCounts.tables[filepath.Join(root, "20261013", "tagcnt", "a")]
	pending := 0
	if table != nil {
		for _, d := range table.pending {
			if strings.HasPrefix(d.text, "U") {
				pending++
			}
		}
	}
	err = tagCheckTexts(s, false)
	s.tags.mu.Unlock()
	if err != nil || pending != 5 {
		t.Fatal("new texts written by the pass that started before them", pending, err)
	}
	s.tagBetweenHolds = nil
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	total, err := s.TagValuesContext(ctx, "20261013", "a", "alert", "title")
	if err != nil || total.HowManyValues != 15 || total.TotalCount != 15 {
		t.Fatal(total, err)
	}
}

// A tag that fails in the middle of an XLog's entry (a country code past the
// 32767-byte key bound) leaves the tags before it counted, in Go's counting
// order (tagXTotal...): the SQL sums, error and city before nation are
// counted, login and desc after it are not.
func TestTagEntryFailureKeepsEarlierTags(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := tagFlushTestDay.Add(7 * time.Hour).UnixMilli()
	x := &protocol.XLogPack{EndTime: tm, ObjHash: 1, Service: 2, Elapsed: 3, SQLCount: 4, APICallCount: 5, Error: 6, City: 7,
		CountryCode: strings.Repeat("x", 40000), Login: 8, Desc: 9}
	if err = s.PutXLogTagsContext(ctx, "java", x); err != nil {
		t.Fatal(err)
	}
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	if st := s.TagCountStats(); st.ApplyErrors != 1 {
		t.Fatal(st)
	}
	for _, c := range []struct {
		name string
		v    protocol.Value
		want float32
	}{
		{"@total", protocol.Value{}, 1}, {"service-time-sum", textHash(2), 3}, {"sqlcount-sum", textHash(2), 4}, {"apicount-sum", textHash(2), 5},
		{"error", textHash(6), 1}, {"city", textHash(7), 1},
		{"login", protocol.Value{Type: protocol.ValueDecimal, Int: 8}, 0}, {"desc", protocol.Value{Type: protocol.ValueDecimal, Int: 9}, 0},
	} {
		series, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", c.name, c.v)
		if err != nil || tagSum(series) != c.want {
			t.Fatal(c.name, tagSum(series), c.want, err)
		}
	}
}

// The inline pending-bytes flush inside tagAddLocked (a fallback the entry
// pre-check normally keeps from running) leaves the Text ids consistent and
// loses no count, for new values and new hours of known values.
func TestTagInlineFlushKeepsTextIDs(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC, MaxQueryBytes: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	path := filepath.Join(root, "20261013", "tagcnt", "java")
	tag := tagKey("alert", "title")
	s.tags.mu.Lock()
	for i := 0; i < 40; i++ {
		for _, minute := range []int{7*60 + 1, 9*60 + 2} {
			table, err := s.tagTableLocked(ctx, path)
			if err == nil {
				err = s.tagAddLocked(ctx, table, tagItem{id: tagValueKey{tag: tag, kind: protocol.ValueText}, text: fmt.Sprintf("t%02d", i), count: 1}, minute)
			}
			if err == nil {
				err = tagCheckTexts(s, false)
			}
			if err != nil {
				s.tags.mu.Unlock()
				t.Fatal(i, minute, err)
			}
		}
	}
	s.tags.mu.Unlock()
	if s.TagCountStats().InlineFlushes == 0 {
		t.Fatal("no inline flush")
	}
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		series, err := s.TagValueSeriesContext(ctx, "20261013", "java", "alert", "title", protocol.Value{Type: protocol.ValueText, Text: fmt.Sprintf("t%02d", i)})
		if err != nil || len(series) != 1440 || series[7*60+1] != 1 || series[9*60+2] != 1 {
			t.Fatal(i, err)
		}
	}
}

// Text ids stay consistent when a flush fails before writing (the deltas
// stay pending) and after writing part of a record (the delta is dropped),
// and a purge drops them with the table.
func TestTagTextIDsAfterFlushFailureAndPurge(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := tagFlushTestDay.Add(7 * time.Hour).UnixMilli()
	check := func(empty bool) {
		t.Helper()
		lockTags(s)
		err := tagCheckTexts(s, empty)
		texts := 0
		for _, table := range s.tagCounts.tables {
			texts += len(table.texts)
		}
		s.tags.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if !empty && texts == 0 {
			t.Fatal("no text pending")
		}
	}
	for i := 0; i <= 100; i++ { // the 101st title goes to Next
		tagAlertAt(t, s, "java", tm, fmt.Sprintf("T%03d", i))
	}
	enospc := errors.New("no space")
	s.tagWriteHook = func(string, int64, int) error { return enospc }
	if err = s.flushTagCounts(ctx); !errors.Is(err, enospc) {
		t.Fatal(err)
	}
	check(false) // nothing written: all pending
	s.tagWriteHook = nil
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	check(true)
	tagAlertAt(t, s, "java", tm, "T100")
	s.tagWriteHook = func(path string, pos int64, n int) error {
		if strings.HasSuffix(path, "next.kfile") && n == 129 { // the head, after the in-place add
			return enospc
		}
		return nil
	}
	if err = s.flushTagCounts(ctx); !errors.Is(err, enospc) {
		t.Fatal(err)
	}
	s.tagWriteHook = nil
	if s.TagCountStats().DroppedDeltas == 0 {
		t.Fatal("no partial write", s.TagCountStats())
	}
	check(true)
	tagAlertAt(t, s, "java", tm, "P")
	tagAlertAt(t, s, "java", tm+24*3600000, "Q")
	check(false)
	if _, err = s.DeleteDatabasePaths(ctx, []string{"/20261013"}); err != nil {
		t.Fatal(err)
	}
	lockTags(s)
	_, kept := s.tagCounts.tables[filepath.Join(root, "20261013", "tagcnt", "java")]
	other := s.tagCounts.tables[filepath.Join(root, "20261014", "tagcnt", "java")]
	err = tagCheckTexts(s, false)
	s.tags.mu.Unlock()
	if kept || other == nil || len(other.texts) != 1 || err != nil {
		t.Fatal("purged table kept", kept, other, err)
	}
}

// Ingest offers while the tag goroutine drains: the drained batch arrays go
// back to the inbox (spare) and every contribution is counted once.
func TestTagSpareReuseConcurrentOfferAndDrain(t *testing.T) {
	ctx := context.Background()
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := tagFlushTestDay.Add(7 * time.Hour).UnixMilli()
	const n = 60000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			w, err := s.PrepareXLogTags("java", &protocol.XLogPack{EndTime: tm, ObjHash: int32(i % 50), Service: int32(i % 300), CountryCode: fmt.Sprintf("C%d", i%7)})
			if err == nil {
				var ok bool
				if ok, err = s.Offer(w); err == nil && !ok {
					err = s.Submit(ctx, w)
				}
			}
			if err != nil {
				t.Error(err)
				return
			}
		}
	}()
	reused := false
	for running := true; running; {
		select {
		case <-done:
			running = false
		default:
		}
		if err := s.FlushTagCountsIfDue(ctx); err != nil {
			t.Fatal(err)
		}
		s.tags.inboxMu.Lock()
		reused = reused || s.tags.spare != nil || cap(s.tags.inbox) > tagInboxMax
		if cap(s.tags.inbox) > tagInboxMax {
			s.tags.inboxMu.Unlock()
			t.Fatal("inbox grew past its bound", cap(s.tags.inbox))
		}
		s.tags.inboxMu.Unlock()
	}
	if err = s.flushTagCounts(ctx); err != nil {
		t.Fatal(err)
	}
	if !reused {
		t.Fatal("no batch array was kept for the inbox")
	}
	series, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "@total", protocol.Value{})
	if err != nil {
		t.Fatal(err)
	}
	if want := float32(n - s.TagCountStats().InboxDropped); tagSum(series) != want {
		t.Fatal("counted", tagSum(series), "want", want, s.TagCountStats())
	}
}

// An XLog's tag Write is sized by its entry, held by value, and the country
// code; an alert's adds its contributions.
func TestTagWriteSize(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tm := tagFlushTestDay.UnixMilli()
	w, err := s.PrepareXLogTags("java", &protocol.XLogPack{EndTime: tm, CountryCode: "KR"})
	if err != nil || w.Size() != int(unsafe.Sizeof(tagInboxEntry{}))+2 {
		t.Fatal(w.Size(), err)
	}
	a, err := s.PrepareAlertTags("java", &protocol.AlertPack{Time: tm, Title: "t"})
	if err != nil || a.Size() <= int(unsafe.Sizeof(tagInboxEntry{}))+4*int(unsafe.Sizeof(tagContribution{})) {
		t.Fatal(a.Size(), err)
	}
}

// tagUnder reports whether path is a tag count file (below {date}/tagcnt).
func tagUnder(path string) bool {
	return strings.Contains(path, string(filepath.Separator)+"tagcnt"+string(filepath.Separator))
}
