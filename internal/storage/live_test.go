package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func liveStore(t *testing.T, c Config) (*Store, string) {
	t.Helper()
	if c.Directory == "" {
		c.Directory = t.TempDir()
	}
	c.Location = time.UTC
	s, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, c.Directory
}

func putText(t *testing.T, s *Store, date, div string, hash int32, text string) {
	t.Helper()
	if err := s.PutText(date, &protocol.TextPack{XType: div, Hash: hash, Text: text}); err != nil {
		t.Fatal(err)
	}
	// The text writer is asynchronous; these tests inspect the files.
	if err := s.flushTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func wantText(t *testing.T, s *Store, date, div string, hash int32, want string, found bool) {
	t.Helper()
	got, ok, err := s.Text(date, div, hash)
	if err != nil || ok != found || got != want {
		t.Fatalf("Text(%s,%s,%d)=%q,%v,%v want %q,%v", date, div, hash, got, ok, err, want, found)
	}
}

// The mgr_text_db_daily_* switch is read on every write and every read
// (TextPermWR.isA), a daily miss falls back to the permanent dictionary
// (TextRD.scala:40-41) and daily-only text disappears after daily -> permanent
// (D-T1, kept as Java).
func TestTextDailyLiveSwitchFallbackAndDT1(t *testing.T) {
	s, root := liveStore(t, Config{})
	const date = "20261013"
	putText(t, s, date, "service", 1, "perm-before") // daily off: permanent
	s.SetTextDaily(true, true, true)
	putText(t, s, date, "service", 2, "daily-only")
	putText(t, s, date, "service", 1, "daily-dup") // Java writes the daily table again
	putText(t, s, date, "apicall", 3, "api-daily")
	putText(t, s, date, "ua", 4, "ua-daily")
	putText(t, s, date, "sql", 5, "select 1") // never daily
	for _, f := range []string{"20261013/text/text.kfile", "20261013/text/text.hfile", "00000000/text/text_service.kfile", "00000000/text/text_sql.data"} {
		if _, err := os.Stat(filepath.Join(root, f)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "00000000/text/text_apicall.kfile")); !os.IsNotExist(err) {
		t.Fatal("daily apicall reached the permanent dictionary", err)
	}
	// Daily on: the daily value wins, a miss falls back to text_service.
	wantText(t, s, date, "service", 1, "daily-dup", true)
	wantText(t, s, date, "service", 2, "daily-only", true)
	wantText(t, s, "20261014", "service", 1, "perm-before", true) // no daily table that date
	wantText(t, s, "20261014", "service", 2, "", false)
	wantText(t, s, date, "sql", 5, "select 1", true)
	if _, err := os.Stat(filepath.Join(root, "20261014")); !os.IsNotExist(err) {
		t.Fatal("a read created the daily table", err)
	}
	// Daily off: permanent only (D-T1: daily-only text is not found).
	s.SetTextDaily(false, false, false)
	wantText(t, s, date, "service", 1, "perm-before", true)
	wantText(t, s, date, "service", 2, "", false)
	wantText(t, s, date, "apicall", 3, "", false)
	putText(t, s, date, "apicall", 3, "api-perm")
	wantText(t, s, date, "apicall", 3, "api-perm", true)
	// error is daily regardless and also falls back.
	putText(t, s, date, "error", 6, "err")
	wantText(t, s, date, "error", 6, "err", true)
	s.SetTextDaily(false, true, false)
	wantText(t, s, date, "apicall", 3, "api-daily", true)
	wantText(t, s, date, "ua", 4, "", false)
}

// table (SqlTables) and city (GeoIP) are permanent dictionaries written by PutText.
func TestTextTableAndCityArePermanent(t *testing.T) {
	s, root := liveStore(t, Config{TextDailyService: true, TextDailyAPI: true, TextDailyUA: true})
	putText(t, s, "20261013", "table", 7, "EMP(S),DEPT(U)")
	putText(t, s, "20261013", "city", 8, "Seoul")
	for _, div := range []string{"table", "city"} {
		if _, err := os.Stat(filepath.Join(root, "00000000/text/text_"+div+".data")); err != nil {
			t.Fatal(div, err)
		}
	}
	wantText(t, s, "20261101", "table", 7, "EMP(S),DEPT(U)", true)
	wantText(t, s, "20261101", "city", 8, "Seoul", true)
}

// compress_*_enabled applies to a date without .conf; a written date keeps its mode.
func TestCompressionLiveForNewDatesOnly(t *testing.T) {
	s, root := liveStore(t, Config{})
	day := func(d int) int64 { return time.Date(2026, 10, d, 1, 0, 0, 0, time.UTC).UnixMilli() }
	put := func(d int, tx int64) {
		t.Helper()
		if err := s.PutXLog(&protocol.XLogPack{EndTime: day(d), TxID: tx, ObjHash: 1}); err != nil {
			t.Fatal(err)
		}
		if err := s.PutProfile(&protocol.ProfilePack{Time: day(d), TxID: tx, Profile: []byte{1, byte(tx)}}); err != nil {
			t.Fatal(err)
		}
	}
	put(13, 1)
	s.SetCompression(true, true)
	put(13, 2) // existing date keeps plain
	put(14, 3) // new date compressed
	s.SetCompression(false, false)
	put(14, 4) // keeps compressed
	conf := func(date, name string) string {
		b, err := os.ReadFile(filepath.Join(root, date, "xlog", name))
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(b))
	}
	if conf("20261013", "xlog.service.conf") != "compress_xlog_enabled=false" || conf("20261014", "xlog.service.conf") != "compress_xlog_enabled=true" || conf("20261014", "xlog.profile.conf") != "compress_profile_enabled=true" {
		t.Fatal("conf files")
	}
	if _, err := os.Stat(filepath.Join(root, "20261014/xlog/data/count.dat")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "20261014/xlog/xlog.service")); !os.IsNotExist(err) {
		t.Fatal("plain data on compressed date", err)
	}
	for _, c := range []struct {
		date string
		tx   int64
	}{{"20261013", 1}, {"20261013", 2}, {"20261014", 3}, {"20261014", 4}} {
		p, err := s.XLog(c.date, c.tx)
		if err != nil || p == nil || p.TxID != c.tx {
			t.Fatal(c, p, err)
		}
		chunks, err := s.ProfileChunks(c.date, c.tx)
		if err != nil || len(chunks) != 1 || chunks[0][1] != byte(c.tx) {
			t.Fatal(c, chunks, err)
		}
	}
}

func TestVisitorHourlyLive(t *testing.T) {
	ctx := context.Background()
	s, _ := liveStore(t, Config{})
	now := time.Date(2026, 10, 13, 7, 10, 0, 0, time.UTC).UnixMilli()
	s.SetVisitorHourlyCount(false)
	if err := s.PutVisitorContext(ctx, now, "java", 1, 42); err != nil {
		t.Fatal(err)
	}
	s.SetVisitorHourlyCount(true)
	if err := s.PutVisitorContext(ctx, now, "java", 2, 42); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if n, err := s.VisitorHourlyContext(ctx, "20261013", now, []int32{42}); err != nil || n != 1 {
		t.Fatal("hourly counts only users seen while enabled", n, err)
	}
	if n, err := s.VisitorDailyContext(ctx, "20261013", []int32{42}, ""); err != nil || n != 2 {
		t.Fatal("daily unaffected", n, err)
	}
}

func hfileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

// *_mb keys size only newly created .hfile files; KV defaults to 8 MiB.
func TestIndexCreationSizes(t *testing.T) {
	ctx := context.Background()
	sizes := IndexSizesMB{TextDefault: 2, TextService: 3, TextAPI: 4, TextUA: 5, TextLogin: 6, TextDesc: 7, TextHMsg: 8, TextDaily: 9}
	s, root := liveStore(t, Config{IndexSizes: sizes})
	const date = "20261013"
	for div, mb := range map[string]int{"service": 3, "apicall": 4, "ua": 5, "login": 6, "desc": 7, "hmsg": 8, "sql": 2, "table": 2} {
		putText(t, s, date, div, 1, "x")
		if got := hfileSize(t, filepath.Join(root, "00000000/text/text_"+div+".hfile")); got != int64(1024+mb<<20) {
			t.Fatal(div, got)
		}
	}
	putText(t, s, date, "error", 1, "x")
	if got := hfileSize(t, filepath.Join(root, date, "text/text.hfile")); got != 1024+9<<20 {
		t.Fatal("daily", got)
	}
	if _, err := s.PutKeyValueContext(ctx, "space", "k", "v", -1, 1); err != nil {
		t.Fatal(err)
	}
	if got := hfileSize(t, filepath.Join(root, "00000000/kv2/kv_space.hfile")); got != 1024+8<<20 {
		t.Fatal("kv default", got)
	}
	end := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	if err := s.PutXLog(&protocol.XLogPack{EndTime: end, TxID: 1, GxID: 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetXLogIDIndexMB(3); err != nil {
		t.Fatal(err)
	}
	if err := s.PutXLog(&protocol.XLogPack{EndTime: end + 86400000, TxID: 1, GxID: 2}); err != nil {
		t.Fatal(err)
	}
	for date, want := range map[string]int64{"20261013": 1024 + 1<<20, "20261014": 1024 + 3<<20} {
		for _, f := range []string{"xlog_tid.hfile", "xlog_gid.hfile"} {
			if got := hfileSize(t, filepath.Join(root, date, "xlog", f)); got != want {
				t.Fatal(date, f, got)
			}
		}
	}
	if got := hfileSize(t, filepath.Join(root, "20261014/xlog/xlog_tim.hfile")); got != 1024+172800*5 {
		t.Fatal("time index is fixed", got)
	}
	if err := s.SetXLogIDIndexMB(MaxIndexMB + 1); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopened with other sizes, existing files keep their length.
	s2, _ := liveStore(t, Config{Directory: root, IndexSizes: IndexSizesMB{TextService: 1, KV: 1}})
	putText(t, s2, date, "service", 2, "y")
	wantText(t, s2, date, "service", 2, "y", true)
	if got := hfileSize(t, filepath.Join(root, "00000000/text/text_service.hfile")); got != 1024+3<<20 {
		t.Fatal("existing size changed", got)
	}
	if _, err := s2.PutKeyValueContext(ctx, "other", "k", "v", -1, 1); err != nil {
		t.Fatal(err)
	}
	if got := hfileSize(t, filepath.Join(root, "00000000/kv2/kv_other.hfile")); got != 1024+1<<20 {
		t.Fatal("kv configured", got)
	}
	if _, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, IndexSizes: IndexSizesMB{KV: -1}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

type logged struct {
	mu    sync.Mutex
	lines []string
}

func (l *logged) log(id string, sec int, msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, id+"|"+msg)
}
func (l *logged) take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.lines
	l.lines = nil
	return out
}

// log_index_traversal_warning_count: a lookup miss whose chain is longer
// than the count logs S152 (IndexKeyFile.get); hasKey-style writes do not.
func TestIndexTraversalWarning(t *testing.T) {
	var l logged
	// A 5-byte hash area has one bucket: every key shares one chain.
	s, _ := liveStore(t, Config{HashSizeBytes: 5, Log: l.log})
	clock := time.Unix(1000, 0)
	s.live.warn.now = func() time.Time { return clock }
	s.SetIndexTraversalWarningCount(2)
	const date = "20261013"
	for h := int32(1); h <= 3; h++ { // object index uses HashSizeBytes
		if err := s.PutObject(date, objectWithHash(h, "/o")); err != nil {
			t.Fatal(err)
		}
	}
	// Third put missed after walking 2 records: not > 2.
	if got := l.take(); len(got) != 0 {
		t.Fatal(got)
	}
	if err := s.PutObject(date, objectWithHash(4, "/o")); err != nil {
		t.Fatal(err)
	}
	if got := l.take(); len(got) != 1 || got[0] != "S152|[warn] Too many index deep searching. 4" {
		t.Fatal(got)
	}
	// Rate limit: 10 seconds per id.
	if err := s.PutObject(date, objectWithHash(5, "/o")); err != nil {
		t.Fatal(err)
	}
	if got := l.take(); len(got) != 0 {
		t.Fatal("rate limit", got)
	}
	clock = clock.Add(10 * time.Second)
	// A hit never warns, however deep.
	if err := s.PutObject(date, objectWithHash(1, "/o2")); err != nil {
		t.Fatal(err)
	}
	if got := l.take(); len(got) != 0 {
		t.Fatal("hit warned", got)
	}
	// Text writes use hasKey (no warning); the read miss warns.
	s2, _ := liveStore(t, Config{HashSizeBytes: 5, IndexSizes: IndexSizesMB{}, Log: l.log})
	s2.SetIndexTraversalWarningCount(0)
	putText(t, s2, date, "sql", 1, "a")
	putText(t, s2, date, "sql", 2, "b")
	if got := l.take(); len(got) != 0 {
		t.Fatal("hasKey warned", got)
	}
	wantText(t, s2, date, "sql", 9, "", false)
	if got := l.take(); len(got) != 1 || got[0] != "S152|[warn] Too many index deep searching. 9" {
		t.Fatal(got)
	}
	s2.SetLog(nil)
	wantText(t, s2, date, "sql", 8, "", false)
	if got := l.take(); len(got) != 0 {
		t.Fatal("nil sink", got)
	}
}

func TestKeyValueTraversalWarningIDs(t *testing.T) {
	ctx := context.Background()
	var l logged
	s, _ := liveStore(t, Config{Log: l.log})
	// KV is 8 MiB, so chains are short: a negative count makes even an empty
	// chain (looped 0) exceed it, as Java's "looping > count" would.
	s.SetIndexTraversalWarningCount(-1)
	if _, err := s.PutKeyValueContext(ctx, "sp", "a", "1", -1, 1); err != nil {
		t.Fatal(err)
	}
	if got := l.take(); len(got) != 1 || got[0] != "S161|[warn] Too many index deep searching. a" {
		t.Fatal("updateOrPut", got) // empty chain: looped 0 > -1
	}
	s.live.warn.now = func() time.Time { return time.Now().Add(time.Hour) }
	if _, ok, err := s.KeyValueContext(ctx, "sp", "zz", 1); err != nil || ok {
		t.Fatal(ok, err)
	}
	if got := l.take(); len(got) != 1 || got[0] != "S152|[warn] Too many index deep searching. zz" {
		t.Fatal("get miss", got)
	}
	if _, ok, err := s.KeyValueContext(ctx, "sp", "a", 1); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if got := l.take(); len(got) != 0 {
		t.Fatal("get hit warned", got)
	}
}

// mgr_purge_enabled and the limits are applied for the next cycle.
func TestRetentionApplyLive(t *testing.T) {
	s, c, root := retentionFixture(t)
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	for _, d := range []string{"20261001", "20260920"} {
		touch(t, filepath.Join(root, d, "xlog/xlog.service"))
		touch(t, filepath.Join(root, d, "counter/5m.data"))
	}
	r := &Retention{Store: s, Counters: c, Config: RetentionConfig{XLogDays: 30}}
	if on, cfg := r.Settings(); !on || cfg.XLogDays != 30 {
		t.Fatal(on, cfg)
	}
	if r.Apply(false, JavaRetentionDefaults()) != true {
		t.Fatal("change not reported")
	}
	if actions, err := r.RunOnce(context.Background(), now); err != nil || len(actions) != 0 {
		t.Fatal("disabled purge ran", actions, err)
	}
	cfg := JavaRetentionConfig(map[string]int{"mgr_purge_xlog_keep_days": 15, "mgr_purge_disk_usage_pct": 0})
	if !r.Apply(true, cfg) || r.Apply(true, cfg) {
		t.Fatal("change detection")
	}
	actions, err := r.RunOnce(context.Background(), now)
	if err != nil || len(actions) != 1 || actions[0].Date != "20260920" || actions[0].Mode != PurgeXLog {
		t.Fatal(actions, err)
	}
	cfg.XLogDays = 5
	r.Apply(true, cfg)
	actions, err = r.RunOnce(context.Background(), now)
	if err != nil || len(actions) != 1 || actions[0].Date != "20261001" {
		t.Fatal(actions, err)
	}
	for _, d := range []string{"20261001", "20260920"} {
		if _, err := os.Stat(filepath.Join(root, d, "counter/5m.data")); err != nil {
			t.Fatal(err)
		}
	}
}

func TestZipLiveCacheBounds(t *testing.T) {
	s, _ := liveStore(t, Config{CompressXLog: true})
	for d := 1; d <= 5; d++ {
		end := time.Date(2026, 10, d, 1, 0, 0, 0, time.UTC).UnixMilli()
		if err := s.PutXLog(&protocol.XLogPack{EndTime: end, TxID: int64(d)}); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.Lock()
	blocks, writers := len(s.zip.blocks), len(s.zip.writers)
	s.mu.Unlock()
	if blocks != zipCacheBlocks || writers != 5 {
		t.Fatal(blocks, writers)
	}
	s.SetCompressReadCacheBlocks(2)
	s.SetCompressDailyCountCacheSize(3) // Java keeps n-1 dates
	end := time.Date(2026, 10, 6, 1, 0, 0, 0, time.UTC).UnixMilli()
	if err := s.PutXLog(&protocol.XLogPack{EndTime: end, TxID: 6}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	blocks, writers = len(s.zip.blocks), len(s.zip.writers)
	s.mu.Unlock()
	if blocks != 2 || writers != 2 {
		t.Fatal(blocks, writers)
	}
	for d := 1; d <= 6; d++ {
		if p, err := s.XLog(time.Date(2026, 10, d, 0, 0, 0, 0, time.UTC).Format("20060102"), int64(d)); err != nil || p == nil {
			t.Fatal(d, p, err)
		}
	}
	s.SetCompressReadCacheBlocks(0)
	s.SetCompressDailyCountCacheSize(1)
	if n := s.zipCacheLimit(); n != MaxZipCacheBlocks {
		t.Fatal(n)
	}
	if n := s.zipWriterLimit(); n != 1 {
		t.Fatal(n)
	}
}

// Setters run concurrently with ingest and queries (config reload thread).
func TestLiveSettersConcurrent(t *testing.T) {
	s, _ := liveStore(t, Config{Log: func(string, int, string) {}})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			on := i%2 == 0
			s.SetTextDaily(on, on, on)
			s.SetCompression(on, on)
			s.SetVisitorHourlyCount(on)
			_ = s.SetXLogIDIndexMB(1 + i%2)
			s.SetIndexTraversalWarningCount(i % 3)
			s.SetCompressReadCacheBlocks(1 + i%3)
			s.SetCompressDailyCountCacheSize(2 + i%3)
		}
	}()
	go func() {
		defer wg.Done()
		base := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
		for i := 0; ctx.Err() == nil; i++ {
			putText(t, s, "20261013", "service", int32(i%50), "s")
			_, _, _ = s.Text("20261013", "service", int32(i%60))
			if err := s.PutXLog(&protocol.XLogPack{EndTime: base + int64(i%3)*86400000, TxID: int64(i)}); err != nil {
				t.Error(err)
				return
			}
			_ = s.PutVisitorContext(context.Background(), base, "java", int64(i), 1)
		}
	}()
	wg.Wait()
}
