package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

var cleanDay = time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()

func cleanStore(t *testing.T, dir string, events *[]RecoveryEvent) *Store {
	t.Helper()
	s, err := Open(Config{Directory: dir, Location: time.UTC, OnRecover: func(e RecoveryEvent) {
		if events != nil {
			*events = append(*events, e)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func putXLogsAt(t *testing.T, s *Store, day, from, to int64) {
	t.Helper()
	for tx := from; tx <= to; tx++ {
		if err := s.PutXLog(&protocol.XLogPack{EndTime: day + tx, TxID: tx, GxID: 9}); err != nil {
			t.Fatal(err)
		}
	}
}

func putXLogs(t *testing.T, s *Store, from, to int64) {
	t.Helper()
	putXLogsAt(t, s, cleanDay, from, to)
}

func markerExists(t *testing.T, dir string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, cleanMarkerName))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return err == nil
}

func checkXLogs(t *testing.T, s *Store, date string, from, to int64) {
	t.Helper()
	for tx := from; tx <= to; tx++ {
		if x, err := s.XLog(date, tx); err != nil || x == nil || x.TxID != tx {
			t.Fatal(tx, x, err)
		}
	}
}

// After a clean Close the first write of the next run must not walk the
// XLog indexes of the day record by record (the restart ingest stall). The
// marker is consumed by Open.
func TestCleanRestartSkipsFullValidation(t *testing.T) {
	dir := t.TempDir()
	s := cleanStore(t, dir, nil)
	putXLogs(t, s, 1, 50)
	if markerExists(t, dir) {
		t.Fatal("marker while running")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !markerExists(t, dir) {
		t.Fatal("clean Close wrote no marker")
	}
	var events []RecoveryEvent
	s = cleanStore(t, dir, &events)
	defer s.Close()
	if markerExists(t, dir) {
		t.Fatal("Open must remove the marker before any write")
	}
	putXLogs(t, s, 51, 60)
	st := s.IndexOpenStats()
	if st.Walked != 0 || st.Sealed != 3 || !st.CleanMarker || st.CleanClosedAt.IsZero() {
		t.Fatalf("clean restart: %+v", st)
	}
	checkXLogs(t, s, "20261013", 1, 60)
	if logs, err := s.XLogs("20261013", cleanDay, cleanDay+100, false); err != nil || len(logs) != 60 {
		t.Fatal(len(logs), err)
	}
	if len(events) != 0 {
		t.Fatal(events)
	}
}

// No marker is a crash: headers saved by the last Sync only, records after it
// unreachable until the full validation relinks them.
func TestCrashWithoutMarkerIsFullyValidated(t *testing.T) {
	dir := t.TempDir()
	crashed := cleanStore(t, dir, nil)
	putXLogs(t, crashed, 1, 3)
	if err := crashed.Sync(); err != nil {
		t.Fatal(err)
	}
	putXLogs(t, crashed, 4, 6) // never synced; the process dies here
	// The crash happened before any checkpoint reached the log (the tail
	// path is TestCrashRestartValidatesOnlyTail).
	dropCheckpointLogs(t, dir)
	var events []RecoveryEvent
	s := cleanStore(t, dir, &events)
	defer s.Close()
	putXLogs(t, s, 7, 7)
	if st := s.IndexOpenStats(); st.Walked != 3 || st.Sealed != 0 || st.CleanMarker {
		t.Fatalf("crash restart: %+v", st)
	}
	if len(events) != 3 || events[0].Kind != "index-relinked" {
		t.Fatal(events)
	}
	checkXLogs(t, s, "20261013", 1, 7)
}

// The marker exists, but a later writer (Java, a tool) changed one pair.
func TestPairChangedAfterMarkerIsFullyValidated(t *testing.T) {
	dir := t.TempDir()
	s := cleanStore(t, dir, nil)
	putXLogs(t, s, 1, 5)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	other, err := legacyindex.Open(filepath.Join(dir, "20261013", "xlog", "xlog_tid"), 0, false, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = other.Put(longKey(99), offsetBytes(0)); err != nil {
		t.Fatal(err)
	}
	if err = other.Close(); err != nil {
		t.Fatal(err)
	}
	s = cleanStore(t, dir, nil)
	defer s.Close()
	putXLogs(t, s, 6, 6)
	if st := s.IndexOpenStats(); st.Walked != 1 || st.Sealed != 2 {
		t.Fatalf("changed pair: %+v", st)
	}
}

// A truncated .kfile changes the sealed size; the full validation then finds
// the head that points at the torn record (an O(1) check alone would not).
func TestTruncatedPairWithMarkerIsFullyValidated(t *testing.T) {
	dir := t.TempDir()
	s := cleanStore(t, dir, nil)
	putXLogs(t, s, 1, 5)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	kfile := filepath.Join(dir, "20261013", "xlog", "xlog_tid.kfile")
	info, err := os.Stat(kfile)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Truncate(kfile, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	s = cleanStore(t, dir, nil)
	defer s.Close()
	_, err = s.XLog("20261013", 5)
	if !errors.Is(err, legacyindex.ErrCorrupt) {
		t.Fatal(err)
	}
	if st := s.IndexOpenStats(); st.Failed != 1 || st.Sealed != 0 {
		t.Fatalf("truncated pair: %+v", st)
	}
}

// A damaged header with sizes and mtimes restored still fails the sealed
// header checksum and is fully validated.
func TestDamagedHeaderWithRestoredMTimeIsFullyValidated(t *testing.T) {
	dir := t.TempDir()
	s := cleanStore(t, dir, nil)
	putXLogs(t, s, 1, 5)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	hfile := filepath.Join(dir, "20261013", "xlog", "xlog_tid.hfile")
	info, err := os.Stat(hfile)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(hfile)
	if err != nil {
		t.Fatal(err)
	}
	// Point an empty bucket at offset 3, inside the first record: in range
	// for the O(1) check, not a record boundary for the full validation.
	for at := 1024; at+5 <= len(b); at += 5 {
		if string(b[at:at+5]) == "\x00\x00\x00\x00\x00" {
			b[at+4] = 3
			break
		}
	}
	if err = os.WriteFile(hfile, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(hfile, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	s = cleanStore(t, dir, nil)
	defer s.Close()
	if _, err = s.XLog("20261013", 5); !errors.Is(err, legacyindex.ErrCorrupt) {
		t.Fatal(err)
	}
	if st := s.IndexOpenStats(); st.Failed != 1 || st.Sealed != 0 {
		t.Fatalf("damaged header: %+v", st)
	}
}

// Seals survive a clean run that did not open their pair, but a pair a crashed
// run left behind is never sealed by a later clean run that did not open it.
func TestSealsCarriedOnlyThroughCleanRuns(t *testing.T) {
	dayB := time.Date(2026, 10, 14, 1, 0, 0, 0, time.UTC).UnixMilli()

	dir := t.TempDir()
	s := cleanStore(t, dir, nil)
	putXLogs(t, s, 1, 3)
	putXLogsAt(t, s, dayB, 1, 3)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = cleanStore(t, dir, nil) // touches day A only
	putXLogs(t, s, 4, 4)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = cleanStore(t, dir, nil)
	putXLogsAt(t, s, dayB, 4, 4)
	if st := s.IndexOpenStats(); st.Walked != 0 || st.Sealed != 3 {
		t.Fatalf("carried seal: %+v", st)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Crash while day B has unsynced appends.
	crashed := cleanStore(t, dir, nil)
	putXLogsAt(t, crashed, dayB, 5, 6)
	// A clean run that never opens day B.
	s = cleanStore(t, dir, nil)
	putXLogs(t, s, 5, 5)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var events []RecoveryEvent
	s = cleanStore(t, dir, &events)
	defer s.Close()
	checkXLogs(t, s, "20261014", 1, 6)
	// XLog by txid opens xlog_tid only; never through the seal (the crash
	// checkpoint of the earlier clean Close still describes its header).
	if st := s.IndexOpenStats(); st.Walked+st.Checkpointed != 1 || st.Sealed != 0 {
		t.Fatalf("crashed pair: %+v", st)
	}
	if len(events) == 0 || !strings.Contains(events[0].Path, "20261014") {
		t.Fatal(events)
	}
}

// A malformed marker is removed and ignored.
func TestMalformedMarkerIsIgnored(t *testing.T) {
	dir := t.TempDir()
	s := cleanStore(t, dir, nil)
	putXLogs(t, s, 1, 2)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, cleanMarkerName), []byte("{not json"), 0600); err != nil {
		t.Fatal(err)
	}
	s = cleanStore(t, dir, nil)
	defer s.Close()
	if markerExists(t, dir) {
		t.Fatal("malformed marker kept")
	}
	putXLogs(t, s, 3, 3)
	if st := s.IndexOpenStats(); st.Walked+st.Checkpointed != 3 || st.Sealed != 0 || st.CleanMarker {
		t.Fatalf("malformed marker: %+v", st)
	}
}

// The marker only names pairs below the root, newer seals are dropped, and a
// seal newer than the marker is never trusted.
func TestMarkerEntriesAreBounded(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	seal := legacyindex.Seal{KeyBytes: 2, KeyMTime: now.Add(time.Hour).UnixNano()}
	carried := map[string]legacyindex.Seal{}
	sealed := map[string]legacyindex.Seal{filepath.Join(dir, "future"): seal, filepath.Join(filepath.Dir(dir), "outside"): {}}
	if err := writeCleanMarker(dir, carried, sealed, now); err != nil {
		t.Fatal(err)
	}
	state, err := consumeCleanMarker(dir)
	if err != nil || !state.found || len(state.seals) != 0 {
		t.Fatal(state, err)
	}
	sealed = map[string]legacyindex.Seal{}
	for n := 0; n < maxCleanSeals+10; n++ {
		sealed[filepath.Join(dir, "p", string(rune('a'+n%26))+strings.Repeat("x", n/26))] = legacyindex.Seal{KeyMTime: now.Add(-time.Duration(n) * time.Second).UnixNano()}
	}
	if err = writeCleanMarker(dir, carried, sealed, now); err != nil {
		t.Fatal(err)
	}
	if state, err = consumeCleanMarker(dir); err != nil || len(state.seals) != maxCleanSeals {
		t.Fatal(len(state.seals), err)
	}
	for _, s := range state.seals {
		if s.KeyMTime < now.Add(-time.Duration(maxCleanSeals)*time.Second).UnixNano() {
			t.Fatal("kept an older seal over a newer one")
		}
	}
}
