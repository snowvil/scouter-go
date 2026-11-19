package storage

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func TestJavaAggregationInterop(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	source := os.Getenv("SCOUTER_SOURCE")
	if source == "" {
		source = filepath.Join(root, "..", "scouter")
	}
	if _, err = os.Stat(filepath.Join(source, "scouter.server")); err != nil {
		t.Skip("pinned aggregation source unavailable")
	}
	work := t.TempDir()
	output, err := exec.Command("python3", filepath.Join(root, "scripts/java-aggregation.py"), "--source", source, "--classes-dir", filepath.Join(work, "compiled")).CombinedOutput()
	if err != nil {
		t.Fatalf("actual aggregation compile %v\n%s", err, output)
	}
	cp := strings.TrimSpace(string(output))
	javaDB := filepath.Join(work, "java")
	output, err = exec.Command("java", "-Duser.timezone=UTC", "-cp", cp, "AggregationOracle", "generate", javaDB).CombinedOutput()
	if err != nil {
		t.Fatalf("Java generate %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "alert|@total|title|level|object") {
		t.Fatal(string(output))
	}
	t.Log(strings.TrimSpace(string(output)))
	h := newVisitorHLL()
	for _, v := range []int64{1, 1, -1, 2, -1 << 63, 1<<63 - 1, 0} {
		h.offer(v)
	}
	b, err := os.ReadFile(filepath.Join(javaDB, "hll.bin"))
	if err != nil || !bytes.Equal(b, h.bytes()) {
		t.Fatal("original HLL byte mismatch", err)
	}
	other := newVisitorHLL()
	other.offer(2)
	other.offer(3)
	h.merge(other)
	b, err = os.ReadFile(filepath.Join(javaDB, "merged.bin"))
	if err != nil || !bytes.Equal(b, h.bytes()) {
		t.Fatal("original merge byte mismatch", err)
	}
	// The visitor partial path (visits merged into an existing snapshot in
	// place, nothing resident) turns the Java-written file into Java's merge.
	javaHLL, err := os.ReadFile(filepath.Join(javaDB, "hll.bin"))
	if err != nil {
		t.Fatal(err)
	}
	merged := javaVisitorPartialMerge(t, javaHLL, 2, 3)
	if !bytes.Equal(merged, b) {
		t.Fatal("Go partial merge of the Java snapshot differs from the Java merge")
	}
	// The original writer's key/hash pairs satisfy the chain invariants the
	// read-only check (scouter-db inspect) relies on.
	for _, first := range []bool{true, false} {
		base := filepath.Join(javaDB, "20261013", "tagcnt", "java", map[bool]string{true: "first", false: "next"}[first])
		rep, err := VerifyTagCountFiles(base, first)
		if err != nil || rep.Records == 0 || len(rep.Problems) != 0 {
			t.Fatal("Java tag chain", first, err, rep)
		}
	}
	// Go updates of the Java-written records, in place and by whole rewrite
	// (each on a copy), give the same files and keep the chain invariants.
	inPlaceDB, wholeDB := filepath.Join(work, "inplace"), filepath.Join(work, "whole")
	for _, dst := range []string{inPlaceDB, wholeDB} {
		copyTree(t, javaDB, dst)
		g, err := Open(Config{Directory: dst, Location: time.UTC})
		if err != nil {
			t.Fatal(err)
		}
		g.tagFullRewrite = dst == wholeDB
		for round := 0; round < 3; round++ {
			for i := 1; i <= 101; i++ { // existing First (1..100) and Next (101) keys
				tm := time.Date(2026, 10, 13, 7+round*3, 10+i%40, 0, 0, time.UTC).UnixMilli()
				if err = g.PutXLogTagsContext(context.Background(), "java", &protocol.XLogPack{EndTime: tm, ObjHash: 42, Service: int32(i), Elapsed: int32(round + 1)}); err != nil {
					t.Fatal(err)
				}
			}
			if err = g.flushTagCounts(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if err = g.Close(); err != nil {
			t.Fatal(err)
		}
		for _, first := range []bool{true, false} {
			rep, err := VerifyTagCountFiles(filepath.Join(dst, "20261013", "tagcnt", "java", map[bool]string{true: "first", false: "next"}[first]), first)
			if err != nil || len(rep.Problems) != 0 {
				t.Fatal("Go update of Java files", dst, first, err, rep)
			}
		}
	}
	for _, name := range []string{"first.kfile", "first.hfile", "next.kfile", "next.hfile", "next.data"} {
		a, errA := os.ReadFile(filepath.Join(inPlaceDB, "20261013", "tagcnt", "java", name))
		b, errB := os.ReadFile(filepath.Join(wholeDB, "20261013", "tagcnt", "java", name))
		if errA != nil || errB != nil || !bytes.Equal(a, b) {
			t.Fatal("in-place update of Java records differs", name, errA, errB)
		}
	}
	ctx := context.Background()
	s, err := Open(Config{Directory: javaDB, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	total, err := s.TagValuesContext(ctx, "20261013", "java", "service", "service")
	if err != nil || total.HowManyValues != 100 || total.TotalCount != 100 {
		t.Fatal(total, err)
	}
	ties, err := s.TagValuesContext(ctx, "20261013", "java", "service", "service-time-sum")
	if err != nil || ties.TotalCount != 2.5 || len(ties.Values) != 3 {
		t.Fatal("fractional total", ties, err)
	}
	order := "TIE_ORDER="
	for _, v := range ties.Values {
		order += strconv.FormatInt(v.Value.Int, 10) + ","
	}
	if !strings.Contains(string(output), order) {
		t.Fatal("original float comparator tie", order, string(output))
	}
	series, err := s.TagValueSeriesContext(ctx, "20261013", "java", "service", "service", textHash(101))
	if err != nil || len(series) != 1440 || series[430] != 1 {
		t.Fatal(series, err)
	}
	cache, err := os.ReadFile(filepath.Join(javaDB, "cache.bin"))
	if err != nil {
		t.Fatal(err)
	}
	c, _, err := func() (TagValueTotal, bool, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.tagReadCacheLocked(ctx, filepath.Join(javaDB, "cache.bin"))
	}()
	if err != nil || c.HowManyValues != 101 || len(c.Values) != 100 {
		t.Fatal(len(cache), c, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	goDB := filepath.Join(work, "go")
	s, err = Open(Config{Directory: goDB, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	tm := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	for i := 1; i <= 101; i++ {
		if err = s.PutXLogTagsContext(ctx, "java", &protocol.XLogPack{EndTime: tm, ObjHash: 42, Service: int32(i), Elapsed: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = s.MakeTagTop100Context(ctx, "20261013", "java"); err != nil {
		t.Fatal(err)
	}
	goCache, err := os.ReadFile(filepath.Join(goDB, "20261013/tagcnt/java/top100."+formatTagKey(tagKey("service", "service"))+".dat"))
	if err != nil || !bytes.Equal(goCache, cache) {
		t.Fatal("actual Java TopN tie/cache bytes", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	// Java verifies a snapshot the partial path wrote: user 0 (never a visit)
	// is in the base file, the other values were merged in by visits.
	partial := javaVisitorPartialMerge(t, visitorHLLOf(0).bytes(), 1, 1, -1, 2, -1<<63, 1<<63-1)
	if err = os.WriteFile(filepath.Join(goDB, "hll.bin"), partial, 0600); err != nil {
		t.Fatal(err)
	}
	output, err = exec.Command("java", "-Duser.timezone=UTC", "-cp", cp, "AggregationOracle", "verify", goDB).CombinedOutput()
	if err != nil {
		t.Fatalf("Java verify Go %v\n%s", err, output)
	}
	t.Log(strings.TrimSpace(string(output)))
}

// javaVisitorPartialMerge places base as today's objType snapshot, visits it
// through partial entries (nothing resident, two flushes) and returns the file.
func javaVisitorPartialMerge(t *testing.T, base []byte, users ...int64) []byte {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "20261013", "visit", "java.usr")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, base, 0600); err != nil {
		t.Fatal(err)
	}
	s := openVisitorStore(t, dir, func(v *visitorState) { v.residentMax = 0 })
	ctx := context.Background()
	for i, u := range users {
		if err := s.PutVisitorContext(ctx, visitorTestNow, "java", u, 42); err != nil {
			t.Fatal(err)
		}
		if i == len(users)/2 {
			if err := s.flushVisitors(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.flushVisitors(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
