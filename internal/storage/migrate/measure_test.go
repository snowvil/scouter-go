package migrate

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

func envInt(t *testing.T, name string, def int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return n
}

// TestIncrementalMeasure compares a full migrate with precopy + finish on a
// synthetic multi-date DB. It runs only with SCOUTER_MIGRATE_MEASURE_DIR set
// (an empty work directory); sizes come from SCOUTER_MIGRATE_MEASURE_DATES,
// _XLOGS (per date) and _PROFILE_MB (per date).
func TestIncrementalMeasure(t *testing.T) {
	work := os.Getenv("SCOUTER_MIGRATE_MEASURE_DIR")
	if work == "" {
		t.Skip("SCOUTER_MIGRATE_MEASURE_DIR not set; measurement not run")
	}
	dates := envInt(t, "SCOUTER_MIGRATE_MEASURE_DATES", 8)
	xlogs := envInt(t, "SCOUTER_MIGRATE_MEASURE_XLOGS", 200000)
	profileMB := envInt(t, "SCOUTER_MIGRATE_MEASURE_PROFILE_MB", 480)
	loc := seoul(t)
	src := filepath.Join(work, "src")
	day := int64(86400000)
	chunk := make([]byte, 64<<10)
	for i := range chunk {
		chunk[i] = byte(i * 7)
	}
	var tx int64
	// write appends n XLogs (and profile bytes) to date index d.
	write := func(d int, n int, profileBytes int) {
		s, err := storage.Open(storage.Config{Directory: src, Location: loc})
		if err != nil {
			t.Fatal(err)
		}
		at := baseTime + int64(d)*day
		for i := 0; i < n; i++ {
			tx++
			x := &protocol.XLogPack{TxID: tx, GxID: tx / 3, EndTime: at + int64(i)%(day-1000), ObjHash: 42, Service: int32(tx % 500), Elapsed: int32(tx % 3000), Error: int32(tx % 7), CPU: 3, SQLCount: 2, SQLTime: 9, IPAddress: []byte{10, 0, 0, byte(tx)}, UserAgent: 5}
			if err := s.PutXLog(x); err != nil {
				t.Fatal(err)
			}
		}
		for written := 0; written < profileBytes; written += len(chunk) {
			tx++
			if err := s.PutProfile(&protocol.ProfilePack{Time: at + int64(written%1000), TxID: tx, Profile: chunk}); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 200; i++ {
			if err := s.PutText(testDate, &protocol.TextPack{XType: "sql", Hash: int32(tx) + int32(i), Text: "select * from t where id = ?"}); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(src); os.IsNotExist(err) {
		start := time.Now()
		for d := 0; d < dates; d++ {
			write(d, xlogs, profileMB<<20)
		}
		if err := os.WriteFile(filepath.Join(src, javaLockName), nil, 0600); err != nil {
			t.Fatal(err)
		}
		t.Logf("generated %d dates in %s", dates, time.Since(start))
	} else {
		t.Fatal("SCOUTER_MIGRATE_MEASURE_DIR must be empty")
	}
	ageAll(t, src)
	size := func(root string) (n int64) {
		for _, st := range relFiles(t, root) {
			n += st.size
		}
		return n
	}
	t.Logf("src %d bytes", size(src))
	opt := func(dst string) MigrateOptions {
		return MigrateOptions{Src: src, Dst: dst, Location: loc}
	}
	ctx := context.Background()
	stale := func() {
		old := time.Now().Add(-time.Hour)
		os.Chtimes(filepath.Join(src, javaLockName), old, old)
	}
	stale()
	start := time.Now()
	full, err := Migrate(ctx, opt(filepath.Join(work, "full")))
	if err != nil {
		t.Fatalf("full migrate %v", err)
	}
	t.Logf("full migrate: %s, %d files, %d bytes", time.Since(start), len(full.Files), full.TotalBytes)
	if err = os.RemoveAll(filepath.Join(work, "full")); err != nil {
		t.Fatal(err)
	}

	// Java runs: precopy, then "today" (the last date) keeps growing.
	dst := filepath.Join(work, "inc")
	today := dates - 1
	round := func(label string) {
		touch := time.Now()
		os.Chtimes(filepath.Join(src, javaLockName), touch, touch)
		start := time.Now()
		st, err := Precopy(ctx, opt(dst))
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		l := st.Last
		t.Logf("%s: %s (java_active=%v copied %d files %d bytes, reused %d files %d bytes, inspected %d groups, reused %d, pending %d)",
			label, time.Since(start), l.JavaActive, l.CopiedFiles, l.CopiedBytes, l.ReusedFiles, l.ReusedBytes, l.InspectedGroups, l.ReusedGroups, l.PendingGroups)
	}
	grow := func(n, profileBytes int) {
		base := relFiles(t, src)
		write(today, n, profileBytes)
		changed := changedFiles(base, relFiles(t, src))
		age(t, src, changed)
	}
	round("precopy 1")
	grow(xlogs/10, profileMB<<20/10)
	round("precopy 2")
	grow(xlogs/100, profileMB<<20/100)
	stale()
	start = time.Now()
	m, err := Finish(ctx, opt(dst))
	if err != nil {
		t.Fatalf("finish %v", err)
	}
	f := m.Incremental.Finish
	t.Logf("finish: %s (copied %d files %d bytes, reused %d files %d bytes, inspected %d groups, reused %d, src read %d bytes; copy %d ms, inspect %d ms, query %d ms, check %d ms)",
		time.Since(start), f.CopiedFiles, f.CopiedBytes, f.ReusedFiles, f.ReusedBytes, f.InspectedGroups, f.ReusedGroups, f.SrcReadBytes, f.CopyMillis, f.InspectMillis, f.QueryMillis, f.CheckMillis)
	start = time.Now()
	full, err = Migrate(ctx, opt(filepath.Join(work, "full2")))
	if err != nil {
		t.Fatalf("full migrate 2 %v", err)
	}
	t.Logf("full migrate of the final src: %s, %d bytes", time.Since(start), full.TotalBytes)
	sameManifest(t, m, full)
}
