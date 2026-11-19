package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

func zipStore(t *testing.T, dir string, events *[]RecoveryEvent) *Store {
	t.Helper()
	s, err := Open(Config{Directory: dir, Location: time.UTC, CompressXLog: true, OnRecover: func(e RecoveryEvent) { *events = append(*events, e) }})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// A crash after the gzip block rename but before count.dat leaves complete
// unindexed records after count; the next append advances count and reports it.
func TestZipCountRecoveryAfterBlockBeforeCount(t *testing.T) {
	dir := t.TempDir()
	var events []RecoveryEvent
	s := zipStore(t, dir, &events)
	tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	if err := s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 1}); err != nil {
		t.Fatal(err)
	}
	count := filepath.Join(dir, "20261013", "xlog", "data", "count.dat")
	before, err := os.ReadFile(count)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 2}); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(count, before, 0600); err != nil { // count.dat never saved
		t.Fatal(err)
	}
	s = zipStore(t, dir, &events)
	defer s.Close()
	if err = s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 3}); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Kind != "zip-count-advanced" || !strings.Contains(events[0].Detail, "1 unindexed") {
		t.Fatal(events)
	}
	for _, tx := range []int64{1, 2, 3} {
		if x, err := s.XLog("20261013", tx); err != nil || x == nil || x.TxID != tx {
			t.Fatal(tx, x, err)
		}
	}
	after, _ := os.ReadFile(count)
	if binary.BigEndian.Uint64(after) <= binary.BigEndian.Uint64(before) {
		t.Fatal("count not advanced")
	}
}

// The first-ever record: block written, count.dat missing.
func TestZipCountRecoveryMissingCount(t *testing.T) {
	dir := t.TempDir()
	var events []RecoveryEvent
	s := zipStore(t, dir, &events)
	tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	if err := s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 1}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err := os.Remove(filepath.Join(dir, "20261013", "xlog", "data", "count.dat")); err != nil {
		t.Fatal(err)
	}
	s = zipStore(t, dir, &events)
	defer s.Close()
	if err := s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 2}); err != nil || len(events) != 1 {
		t.Fatal(events, err)
	}
}

// Other count/block disagreements are not a crash shape of this writer and stay corrupt.
func TestZipCountMismatchStillCorrupt(t *testing.T) {
	tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	for name, damage := range map[string]func(dir string) error{
		"count beyond block": func(dir string) error {
			return os.WriteFile(filepath.Join(dir, "count.dat"), longKey(1000), 0600)
		},
		"later blocks exist": func(dir string) error {
			if err := os.WriteFile(filepath.Join(dir, "count.dat"), longKey(0), 0600); err != nil {
				return err
			}
			b, err := os.ReadFile(filepath.Join(dir, "xlog.0"))
			if err != nil {
				return err
			}
			if err = os.WriteFile(filepath.Join(dir, "xlog.1"), b, 0600); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(dir, "xlog.2"), b, 0600)
		},
		"linked tail": func(dir string) error {
			return os.WriteFile(filepath.Join(dir, "count.dat"), longKey(0), 0600)
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			var events []RecoveryEvent
			s := zipStore(t, root, &events)
			if err := s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 1}); err != nil {
				t.Fatal(err)
			}
			s.Close()
			data := filepath.Join(root, "20261013", "xlog", "data")
			if name == "linked tail" {
				// Rewrite block 0 with next=1 in the only record header.
				st := &Store{config: Config{Directory: root, Location: time.UTC}}
				block, err := st.zipBlock("20261013", 0)
				if err != nil {
					t.Fatal(err)
				}
				block[4] = 1
				if err = writeZipBlock(filepath.Join(data, "xlog.0"), block); err != nil {
					t.Fatal(err)
				}
			}
			if err := damage(data); err != nil {
				t.Fatal(err)
			}
			s = zipStore(t, root, &events)
			defer s.Close()
			if err := s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 2}); !errors.Is(err, ErrCorrupt) || len(events) != 0 {
				t.Fatal(err, events)
			}
		})
	}
}

func writeZipBlock(path string, block []byte) error {
	var out bytes.Buffer
	z := gzip.NewWriter(&out)
	if _, err := z.Write(block); err != nil {
		return err
	}
	if err := z.Close(); err != nil {
		return err
	}
	return os.WriteFile(path, out.Bytes(), 0600)
}

// A process crash after Sync leaves every later index append unreachable from
// the saved .hfile. Reopening relinks them and reports one event per index.
func TestStoreRecoversXLogsAppendedAfterLastSync(t *testing.T) {
	dir := t.TempDir()
	var events []RecoveryEvent
	s, err := Open(Config{Directory: dir, Location: time.UTC, OnRecover: func(e RecoveryEvent) { events = append(events, e) }})
	if err != nil {
		t.Fatal(err)
	}
	tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	if err = s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 1, GxID: 9}); err != nil {
		t.Fatal(err)
	}
	if err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	snapshot := map[string][]byte{}
	for _, name := range []string{"xlog_tim", "xlog_tid", "xlog_gid"} {
		p := filepath.Join(dir, "20261013", "xlog", name+".hfile")
		if snapshot[p], err = os.ReadFile(p); err != nil {
			t.Fatal(err)
		}
	}
	for tx := int64(2); tx <= 5; tx++ {
		if err = s.PutXLog(&protocol.XLogPack{EndTime: tm + tx, TxID: tx, GxID: 9}); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	for p, b := range snapshot { // the crash never saved these headers
		if err = os.WriteFile(p, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	s, err = Open(Config{Directory: dir, Location: time.UTC, OnRecover: func(e RecoveryEvent) { events = append(events, e) }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	logs, err := s.XLogs("20261013", tm, tm+10, false)
	if err != nil || len(logs) != 5 {
		t.Fatal(len(logs), err)
	}
	for tx := int64(1); tx <= 5; tx++ {
		if x, err := s.XLog("20261013", tx); err != nil || x == nil {
			t.Fatal(tx, err)
		}
	}
	if gx, err := s.XLogsByGxIDContext(context.Background(), "20261013", 9); err != nil || len(gx) != 5 {
		t.Fatal(len(gx), err)
	}
	if len(events) != 3 {
		t.Fatal(events)
	}
	for _, e := range events {
		if e.Kind != "index-relinked" || !strings.Contains(e.Detail, "relinked 4 record") {
			t.Fatal(e)
		}
	}
}

func zipRecords(lengths ...int) []byte {
	var b []byte
	for _, n := range lengths {
		b = append(b, 0, 0, 0, 0, 0, byte(n>>16), byte(n>>8), byte(n))
		b = append(b, make([]byte, n)...)
	}
	return b
}

// A rollover crash leaves block N+1 (count still in N); after restart a record
// still fits in N and a block/count crash follows. Recovery accepts exactly one
// orphan whose first record satisfies the rollover condition, and only that.
func TestZipCountRecoveryOrphanRolloverBlock(t *testing.T) {
	// Four maximal records leave a 32-byte gap at the end of block 0.
	base := zipRecords(0x7ffff0, 0x7ffff0, 0x7ffff0, 0x7ffff0)
	gap := zipBlockBytes - len(base)
	if gap <= 8+10 || gap > 1000 {
		t.Fatal("fixture gap", gap)
	}
	cases := map[string]struct {
		orphanFirst int
		extra       string
		ok          bool
	}{
		"rollover orphan":    {orphanFirst: gap, ok: true},
		"not a rollover":     {orphanFirst: 1},
		"later block exists": {orphanFirst: gap, extra: "xlog.2"},
		"gap block only":     {orphanFirst: -1, extra: "xlog.3"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			data := filepath.Join(dir, "20261013", "xlog", "data")
			if err := os.MkdirAll(data, 0700); err != nil {
				t.Fatal(err)
			}
			conf := filepath.Join(dir, "20261013", "xlog", "xlog.service.conf")
			if err := os.WriteFile(conf, []byte("compress_xlog_enabled=true\n"), 0600); err != nil {
				t.Fatal(err)
			}
			// count = len(base); block 0 gained one small unindexed record.
			if err := writeZipBlock(filepath.Join(data, "xlog.0"), append(append([]byte{}, base...), zipRecords(10)...)); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(data, "count.dat"), longKey(int64(len(base))), 0600); err != nil {
				t.Fatal(err)
			}
			if c.orphanFirst >= 0 {
				if err := writeZipBlock(filepath.Join(data, "xlog.1"), zipRecords(c.orphanFirst)); err != nil {
					t.Fatal(err)
				}
			}
			if c.extra != "" {
				if err := writeZipBlock(filepath.Join(data, c.extra), zipRecords(1)); err != nil {
					t.Fatal(err)
				}
			}
			var events []RecoveryEvent
			s := zipStore(t, dir, &events)
			defer s.Close()
			tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
			err := s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 7})
			if !c.ok {
				if !errors.Is(err, ErrCorrupt) || len(events) != 0 {
					t.Fatal(err, events)
				}
				return
			}
			if err != nil || len(events) != 1 || !strings.Contains(events[0].Detail, "orphan block 1") {
				t.Fatal(err, events)
			}
			// The new record did not fit in block 0: the rollover overwrote the orphan.
			if x, err := s.XLog("20261013", 7); err != nil || x == nil || x.TxID != 7 {
				t.Fatal(x, err)
			}
		})
	}
}

// A refused relink (inconsistent orphan chain) is not a repair, but it must
// still be visible to operators.
func TestRefusedRelinkIsReported(t *testing.T) {
	var events []RecoveryEvent
	hook := func(e RecoveryEvent) { events = append(events, e) }
	reportIndexRecovery(hook, "p", legacyindex.RecoveryInfo{RelinkRefused: "orphan prev 2 != 9", Unreachable: 2})
	reportIndexRecovery(hook, "p", legacyindex.RecoveryInfo{})
	if len(events) != 1 || events[0].Kind != "index-relink-refused" || !strings.Contains(events[0].Detail, "2 unreachable") {
		t.Fatal(events)
	}
}

// SyncData and Counters.Sync fsync outside their gates while writers keep
// evicting cached handles across many dates; closes wait for the pinned fsync
// (run with -race). Everything written stays readable after Close/reopen.
func TestSyncOutsideGateWithEvictions(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	c, err := OpenCounters(dir, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 1, 1, 0, 0, 0, time.UTC)
	stop := make(chan struct{})
	syncErrs := make(chan error, 2)
	go func() {
		for {
			select {
			case <-stop:
				syncErrs <- nil
				return
			default:
			}
			if err := errors.Join(s.SyncData(), c.Sync()); err != nil {
				syncErrs <- err
				return
			}
		}
	}()
	for day := 0; day < 40; day++ { // > 32 indexes and > 8 counter tables: evictions
		tm := base.AddDate(0, 0, day).UnixMilli()
		if err = s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: int64(day + 1)}); err != nil {
			t.Fatal(err)
		}
		if err = c.Put(&protocol.PerfCounterPack{Time: tm, ObjName: "/a", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: int64(day)}}}}}); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	if err = <-syncErrs; err != nil {
		t.Fatal(err)
	}
	if err = errors.Join(s.Close(), c.Close()); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for day := 0; day < 40; day++ {
		date := JavaDate(base.AddDate(0, 0, day).UnixMilli(), time.UTC)
		if x, err := s.XLog(date, int64(day+1)); err != nil || x == nil {
			t.Fatal(day, err)
		}
	}
}

// With DeferVisitorFlush the ingest path never writes snapshots; the owner's
// FlushVisitorsIfDue does, once the 10s tick has passed.
func TestDeferredVisitorFlush(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Config{Directory: dir, Location: time.UTC, DeferVisitorFlush: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	now := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	for i, tm := range []int64{now, now + 10001} {
		if err = s.PutVisitorContext(ctx, tm, "java", int64(i+1), 42); err != nil {
			t.Fatal(err)
		}
	}
	usr := filepath.Join(dir, "20261013", "visit", "java.usr")
	if _, err = os.Stat(usr); !os.IsNotExist(err) {
		t.Fatal("ingest path wrote a snapshot", err)
	}
	if err = s.FlushVisitorsIfDue(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(usr); err != nil {
		t.Fatal("due flush did not write", err)
	}
	if err = s.FlushVisitorsIfDue(ctx); err != nil { // not due again: no-op
		t.Fatal(err)
	}
}

// The cached append offset stays exact across handle eviction and reopen.
func TestAppendOffsetCacheAcrossEviction(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.config.Directory, "a.data")
	var want int64
	for i := 0; i < 50; i++ {
		pos, err := s.appendData(path, []byte{byte(i), 1, 2}, 4)
		if err != nil || pos != want {
			t.Fatal(i, pos, want, err)
		}
		want += 7
		if i%10 == 9 { // force eviction of a.data through 16 other handles
			for j := 0; j < 17; j++ {
				if _, err = s.file(filepath.Join(s.config.Directory, "x", strconv.Itoa(j)), true); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	st, err := os.Stat(path)
	if err != nil || st.Size() != want {
		t.Fatal(st.Size(), want, err)
	}
}

// Power loss: key records survived but the data they point at did not. The
// relink is refused (Java semantics: records stay unreachable) and reported,
// so history queries keep returning the synced data instead of failing.
func TestRelinkRefusedWhenDataNotDurable(t *testing.T) {
	dir := t.TempDir()
	var events []RecoveryEvent
	open := func() *Store {
		s, err := Open(Config{Directory: dir, Location: time.UTC, OnRecover: func(e RecoveryEvent) { events = append(events, e) }})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := open()
	tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	if err := s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.SyncData(); err != nil {
		t.Fatal(err)
	}
	xdir := filepath.Join(dir, "20261013", "xlog")
	headers := map[string][]byte{}
	for _, name := range []string{"xlog_tim", "xlog_tid"} {
		b, err := os.ReadFile(filepath.Join(xdir, name+".hfile"))
		if err != nil {
			t.Fatal(err)
		}
		headers[name] = b
	}
	st, err := os.Stat(filepath.Join(xdir, "xlog.service"))
	if err != nil {
		t.Fatal(err)
	}
	for tx := int64(2); tx <= 4; tx++ {
		if err = s.PutXLog(&protocol.XLogPack{EndTime: tm + tx, TxID: tx}); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	for name, b := range headers {
		if err = os.WriteFile(filepath.Join(xdir, name+".hfile"), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.Truncate(filepath.Join(xdir, "xlog.service"), st.Size()); err != nil {
		t.Fatal(err)
	}
	s = open()
	defer s.Close()
	logs, err := s.XLogs("20261013", tm, tm+10, false)
	if err != nil || len(logs) != 1 || logs[0].TxID != 1 {
		t.Fatal(len(logs), err)
	}
	if x, err := s.XLog("20261013", 3); err != nil || x != nil {
		t.Fatal("unsynced record became visible", x, err)
	}
	refused := 0
	for _, e := range events {
		if e.Kind == "index-relink-refused" && strings.Contains(e.Detail, "beyond data end") {
			refused++
		}
		if e.Kind == "index-relinked" {
			t.Fatal("relinked records whose data is gone", e)
		}
	}
	if refused != 2 {
		t.Fatal(events)
	}
}

// Retention returns the compressed block cache and writer position of a
// purged date immediately.
func TestRetentionReleasesZipState(t *testing.T) {
	dir := t.TempDir()
	var events []RecoveryEvent
	s := zipStore(t, dir, &events)
	defer s.Close()
	tm := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC).UnixMilli()
	if err := s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 1}); err != nil {
		t.Fatal(err)
	}
	if x, err := s.XLog("20261001", 1); err != nil || x == nil {
		t.Fatal(x, err)
	}
	s.mu.Lock()
	cached := len(s.zip.blocks) + len(s.zip.writers)
	s.mu.Unlock()
	if cached == 0 {
		t.Fatal("no zip state to release")
	}
	r := &Retention{Store: s, Config: RetentionConfig{XLogDays: 3}}
	if _, err := r.RunOnce(context.Background(), time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.zip.blocks)+len(s.zip.writers) != 0 {
		t.Fatal(len(s.zip.blocks), len(s.zip.writers))
	}
}

// Review M1: after a refused relink the date keeps receiving data, so the
// data file grows past the lost offsets. Reopening (eviction or restart) must
// not relink the old orphans onto the new data. Plain and GZipStore modes.
func TestRefusedRelinkStaysRefusedAfterDataRegrows(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "gzip"}[compressed], func(t *testing.T) {
			dir := t.TempDir()
			var events []RecoveryEvent
			open := func() *Store {
				s, err := Open(Config{Directory: dir, Location: time.UTC, CompressXLog: compressed, OnRecover: func(e RecoveryEvent) { events = append(events, e) }})
				if err != nil {
					t.Fatal(err)
				}
				return s
			}
			s := open()
			tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
			if err := s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 1}); err != nil {
				t.Fatal(err)
			}
			if err := s.SyncData(); err != nil {
				t.Fatal(err)
			}
			xdir := filepath.Join(dir, "20261013", "xlog")
			headers := map[string][]byte{}
			for _, name := range []string{"xlog_tim", "xlog_tid"} {
				headers[name], _ = os.ReadFile(filepath.Join(xdir, name+".hfile"))
			}
			dataPath := filepath.Join(xdir, "xlog.service")
			if compressed {
				dataPath = filepath.Join(xdir, "data", "xlog.0")
			}
			dataBefore, err := os.ReadFile(dataPath)
			if err != nil {
				t.Fatal(err)
			}
			var countBefore []byte
			if compressed {
				countBefore, _ = os.ReadFile(filepath.Join(xdir, "data", "count.dat"))
			}
			for tx := int64(2); tx <= 4; tx++ {
				if err = s.PutXLog(&protocol.XLogPack{EndTime: tm + tx, TxID: tx, Service: 77}); err != nil {
					t.Fatal(err)
				}
			}
			s.Close()
			// Power loss: index pages survived, data pages did not.
			for name, b := range headers {
				os.WriteFile(filepath.Join(xdir, name+".hfile"), b, 0600)
			}
			os.WriteFile(dataPath, dataBefore, 0600)
			if compressed {
				os.WriteFile(filepath.Join(xdir, "data", "count.dat"), countBefore, 0600)
			}
			s = open()
			// New traffic regrows the data file over the lost offsets.
			for tx := int64(10); tx <= 20; tx++ {
				if err = s.PutXLog(&protocol.XLogPack{EndTime: tm + tx, TxID: tx, Service: 99}); err != nil {
					t.Fatal(err)
				}
			}
			s.Close()
			s = open()
			defer s.Close()
			for _, tx := range []int64{2, 3, 4} {
				x, err := s.XLog("20261013", tx)
				if err != nil || x != nil {
					t.Fatal("lost record resurfaced or errored", tx, x, err)
				}
			}
			logs, err := s.XLogs("20261013", tm, tm+30, false)
			if err != nil || len(logs) != 12 {
				t.Fatal(len(logs), err)
			}
			for _, x := range logs {
				if x.TxID >= 2 && x.TxID <= 4 {
					t.Fatal("relinked onto unrelated data", x.TxID)
				}
			}
			for _, e := range events {
				if e.Kind == "index-relinked" {
					t.Fatal(e)
				}
			}
		})
	}
}

// Review M2: Close while SyncData is committing an older header snapshot must
// leave the newest header on disk.
func TestCloseDoesNotLoseHeaderToConcurrentSync(t *testing.T) {
	for round := 0; round < 20; round++ {
		dir := t.TempDir()
		s, err := Open(Config{Directory: dir, Location: time.UTC})
		if err != nil {
			t.Fatal(err)
		}
		tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
		if err = s.PutXLog(&protocol.XLogPack{EndTime: tm, TxID: 1}); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- s.SyncData() }()
		if err = s.PutXLog(&protocol.XLogPack{EndTime: tm + 1, TxID: 2}); err != nil && !errors.Is(err, ErrClosed) {
			t.Fatal(err)
		}
		closeErr := s.Close()
		if err = <-done; err != nil && !errors.Is(err, ErrClosed) {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		// Read-only: the saved header alone must reach every stored record.
		ro, err := legacyindex.OpenReadOnly(filepath.Join(dir, "20261013", "xlog", "xlog_tid"), false, nil)
		if err != nil {
			t.Fatal(err)
		}
		v, err := ro.Get(longKey(2))
		ro.Close()
		if err != nil || v == nil {
			t.Fatal("newest header lost", round, v, err)
		}
	}
}
