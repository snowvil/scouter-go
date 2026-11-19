package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// refJavaTagRecordOrder, refTagTop100Total, refTagTotal and refTagMakeCache are
// the pre-streaming implementation kept as the reference the
// streaming top100 build must reproduce byte for byte.
func refJavaTagRecordOrder(records []tagRecord) {
	capacity := 16
	for len(records) > capacity*3/4 {
		capacity *= 2
	}
	sort.SliceStable(records, func(i, j int) bool {
		a, b := javaTagValueHash(records[i].value), javaTagValueHash(records[j].value)
		return (a^(a>>16))&uint32(capacity-1) < (b^(b>>16))&uint32(capacity-1)
	})
}

func refTagTop100Total(records []tagRecord) TagValueTotal {
	out := TagValueTotal{HowManyValues: len(records)}
	var ascending []TagValueCount
	for _, r := range records {
		n := tagSum(r.counts)
		if r.next {
			n = r.totalCount
		}
		out.TotalCount += n
		item := TagValueCount{r.value, float64(n)}
		at := 0
		if len(ascending) > 0 {
			low, high := 0, len(ascending)-1
			for {
				mid := (high + low) / 2
				c := ascending[mid].Count
				if c == item.Count {
					at = mid
					break
				}
				if c < item.Count {
					low = mid
				} else {
					high = mid
				}
				if high == low {
					at = high
					if ascending[high].Count <= item.Count {
						at++
					}
					break
				}
				if high-low == 1 {
					if ascending[low].Count > item.Count {
						at = low
					} else if ascending[high].Count < item.Count {
						at = high + 1
					} else {
						at = high
					}
					break
				}
			}
		}
		ascending = append(ascending, TagValueCount{})
		copy(ascending[at+1:], ascending[at:])
		ascending[at] = item
		if len(ascending) > 100 {
			ascending = ascending[1:]
		}
	}
	for i := len(ascending) - 1; i >= 0; i-- {
		out.Values = append(out.Values, ascending[i])
	}
	return out
}

func refTagTotal(records []tagRecord, limit int) TagValueTotal {
	refJavaTagRecordOrder(records)
	out := TagValueTotal{HowManyValues: len(records)}
	for _, r := range records {
		n := tagSum(r.counts)
		out.TotalCount += n
		out.Values = append(out.Values, TagValueCount{r.value, float64(n)})
	}
	sort.SliceStable(out.Values, func(i, j int) bool { return int32(out.Values[j].Count-out.Values[i].Count) < 0 })
	if limit > 0 && len(out.Values) > limit {
		out.Values = out.Values[:limit]
	}
	return out
}

func refEncodeTotal(t *testing.T, total TagValueTotal) []byte {
	t.Helper()
	e := protocol.NewEncoder(protocol.Limits{})
	e.Int32(int32(total.HowManyValues))
	e.Float32(total.TotalCount)
	e.Int32(int32(len(total.Values)))
	for _, v := range total.Values {
		e.Value(v.Value)
		e.Float64(v.Count)
	}
	b, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// refTagRecords loads every record without a budget, as tagRecordsLocked did.
func refTagRecords(t *testing.T, s *Store, path string, first bool) []tagRecord {
	t.Helper()
	var out []tagRecord
	if err := s.tagScanLocked(context.Background(), path, first, 1<<30, func(r tagRecord) error {
		out = append(out, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func refTagMakeCache(t *testing.T, s *Store, path string) map[string][]byte {
	t.Helper()
	first := refTagRecords(t, s, filepath.Join(path, "first"), true)
	next := refTagRecords(t, s, filepath.Join(path, "next"), false)
	byTag := map[int64][]tagRecord{}
	for _, r := range next {
		byTag[r.tag] = append(byTag[r.tag], r)
	}
	firstByTag := map[int64][]tagRecord{}
	for _, r := range first {
		firstByTag[r.tag] = append(firstByTag[r.tag], r)
	}
	for tag, records := range firstByTag {
		if _, ok := byTag[tag]; ok {
			refJavaTagRecordOrder(records)
			byTag[tag] = append(byTag[tag], records...)
		}
	}
	out := map[string][]byte{}
	for tag, records := range byTag {
		out[filepath.Base(tagCachePath(path, tag))] = refEncodeTotal(t, refTagTop100Total(records))
	}
	return out
}

func readTop100Files(t *testing.T, path string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]byte{}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "top100.") {
			b, err := os.ReadFile(filepath.Join(path, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			out[e.Name()] = b
		}
	}
	return out
}

// TopN insertion, equal-count placement, eviction and the float32 total match
// the previous slice implementation for random sequences with many ties.
func TestTagTopNMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	tag := tagKey("service", "service")
	for round := 0; round < 300; round++ {
		n := rng.Intn(320)
		distinct := 1 + rng.Intn(40)
		records := make([]tagRecord, n)
		item := &tagTopItem{top: tagTopN{limit: tagTopLimit}}
		for i := range records {
			v := textHash(int32(i + 1))
			if i%3 == 0 {
				v = protocol.Value{Type: protocol.ValueText, Text: fmt.Sprintf("v%d", i)}
			}
			key, err := tagRecordKey(tag, v, protocol.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			count := float32(rng.Intn(distinct)) / float32(1+rng.Intn(2))
			records[i] = tagRecord{key: key, tag: tag, value: v, next: true, totalCount: count}
			item.add(key, count)
			if len(item.top.ascending) > tagTopLimit {
				t.Fatal("TopN exceeded limit", len(item.top.ascending))
			}
		}
		got, err := tagEncodeTop100(item, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if want := refEncodeTotal(t, refTagTop100Total(records)); !bytes.Equal(got, want) {
			t.Fatalf("round %d n=%d: TopN bytes differ", round, n)
		}
	}
}

// putTagMix writes First and Next values with many equal counts: services and
// alert titles beyond 100 per tag over several hours, so each tag mixes Next
// records with First values and the files exceed 64KiB.
func putTagMix(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	base := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	for i := 1; i <= 260; i++ {
		for k := 0; k <= i%3; k++ {
			tm := base.Add(time.Duration(i%24)*time.Hour + time.Duration(i%60+k)*time.Minute).UnixMilli()
			p := &protocol.XLogPack{EndTime: tm, ObjHash: int32(i % 7), Service: int32(i), Elapsed: int32(i%5 + 1), SQLCount: int32(i % 4)}
			if i%9 == 0 {
				p.Error = int32(i)
			}
			p.CountryCode = fmt.Sprintf("C%d", i%130)
			if err := s.PutXLogTagsContext(ctx, "java", p); err != nil {
				t.Fatal(i, err)
			}
		}
		if i <= 180 {
			a := &protocol.AlertPack{Time: base.Add(time.Duration(i%24) * time.Hour).UnixMilli(), ObjHash: int32(i % 7), Level: byte(i % 4), Title: fmt.Sprintf("title-%d", i%150)}
			if err := s.PutAlertTagsContext(ctx, "java", a); err != nil {
				t.Fatal(i, err)
			}
		}
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestTagTop100StreamingMatchesReference(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	putTagMix(t, s)
	path := filepath.Join(root, "20261013", "tagcnt", "java")
	// The previous loader failed on files above MaxQueryBytes.
	for _, name := range []string{"first.kfile", "next.data"} {
		st, err := os.Stat(filepath.Join(path, name))
		if err != nil || st.Size() <= 64<<10 {
			t.Fatal(name, "too small for the 64KiB budget", err)
		}
	}
	lockTags(s)
	want := refTagMakeCache(t, s, path)
	s.tags.mu.Unlock()
	if len(want) < 5 {
		t.Fatal("too few tags with Next values", len(want))
	}
	if err = s.MakeTagTop100Context(ctx, "20261013", "java"); err != nil {
		t.Fatal(err)
	}
	if got := readTop100Files(t, path); !reflect.DeepEqual(got, want) {
		t.Fatal("default budget: top100 cache bytes differ from reference", len(got), len(want))
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	for name := range want {
		if err = os.Remove(filepath.Join(path, name)); err != nil {
			t.Fatal(err)
		}
	}
	small, err := Open(Config{Directory: root, Location: time.UTC, MaxQueryBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer small.Close()
	if err = small.MakeTagTop100Context(ctx, "20261013", "java"); err != nil {
		t.Fatal("64KiB budget", err)
	}
	if got := readTop100Files(t, path); !reflect.DeepEqual(got, want) {
		t.Fatal("64KiB budget: top100 cache bytes differ", len(got), len(want))
	}
	cached, err := small.TagValuesContext(ctx, "20261013", "java", "service", "service")
	if err != nil || cached.HowManyValues != 260 || len(cached.Values) != 100 {
		t.Fatal(cached.HowManyValues, len(cached.Values), err)
	}
}

// Queries keep their results while streaming a First file above MaxQueryBytes.
func TestTagQueriesStreamUnderSmallBudget(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	putTagMix(t, s)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{Directory: root, Location: time.UTC, MaxQueryBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	path := filepath.Join(root, "20261013", "tagcnt", "java")
	for _, name := range [][2]string{{"service", "service"}, {"service", "service-time-sum"}, {"service", "nation"}, {"alert", "title"}, {"alert", "level"}} {
		tag := tagKey(name[0], name[1])
		lockTags(s)
		var selected []tagRecord
		for _, r := range refTagRecords(t, s, filepath.Join(path, "first"), true) {
			if r.tag == tag {
				selected = append(selected, r)
			}
		}
		s.tags.mu.Unlock()
		want := refTagTotal(selected, 100)
		got, err := s.TagValuesContext(ctx, "20261013", "java", name[0], name[1])
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal(name, err)
		}
	}
	// Next value (service 260 is past the first 100) and a First value.
	for _, svc := range []int32{1, 260} {
		series, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service", textHash(svc))
		if err != nil || len(series) != 1440 || tagSum(series) != float32(svc%3+1) {
			t.Fatal(svc, err)
		}
	}
	// Ingest on a fresh table also streams the First file.
	if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: time.Date(2026, 10, 13, 5, 0, 0, 0, time.UTC).UnixMilli(), ObjHash: 1, Service: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestTagTop100BuildLimitsAndCancel(t *testing.T) {
	root := t.TempDir()
	s, err := Open(Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	putTagMix(t, s)
	path := filepath.Join(root, "20261013", "tagcnt", "java")
	lockTags(s)
	defer s.tags.mu.Unlock()
	ctx := context.Background()
	if _, err = s.tagBuildTop100Locked(ctx, path, 10, tagCacheMaxRetained); !errors.Is(err, ErrLimit) {
		t.Fatal("record limit", err)
	}
	if _, err = s.tagBuildTop100Locked(ctx, path, tagScanMaxRecords, 4<<10); !errors.Is(err, ErrLimit) {
		t.Fatal("retained limit", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.tagBuildTop100Locked(cancelled, path, tagScanMaxRecords, tagCacheMaxRetained); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel", err)
	}
	items, err := s.tagBuildTop100Locked(ctx, path, tagScanMaxRecords, tagCacheMaxRetained)
	if err != nil {
		t.Fatal(err)
	}
	for tag, item := range items {
		if len(item.top.ascending) > tagTopLimit || item.kinds < len(item.top.ascending) {
			t.Fatal(tag, item.kinds, len(item.top.ascending))
		}
	}
}

// tagBuildTop100Locked streams First then Next in one Store gate hold and
// returns the TopItem of every tag that has Next values (tagTop100Builder).
// More than maxRecords records in one file return ErrLimit. It is the
// one-hold reference of tagMakeCacheHolds, which uses the same builder
// across bounded holds.
func (s *Store) tagBuildTop100Locked(ctx context.Context, path string, maxRecords, maxRetained int) (map[int64]*tagTopItem, error) {
	b := newTagTop100Builder(maxRetained)
	if err := s.tagScanLocked(ctx, filepath.Join(path, "first"), true, maxRecords, b.addFirst); err != nil {
		return nil, err
	}
	if err := s.tagScanLocked(ctx, filepath.Join(path, "next"), false, maxRecords, b.addNext); err != nil {
		return nil, err
	}
	return b.finish(), ctx.Err()
}

func (s *Store) tagMakeCacheLocked(ctx context.Context, path string) error {
	items, err := s.tagBuildTop100Locked(ctx, path, tagScanMaxRecords, tagCacheMaxRetained)
	if err != nil {
		return err
	}
	return s.tagWriteTop100Locked(ctx, path, items)
}
