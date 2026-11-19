package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// countLoadDecodes counts the block decodes of writer loads (loadZipWriter).
func countLoadDecodes(t *testing.T) *int {
	t.Helper()
	n := new(int)
	zipLoadDecodeHook = func() error { *n++; return nil }
	t.Cleanup(func() { zipLoadDecodeHook = nil })
	return n
}

// zipRecordBytes is the logical record appendZip writes for b.
func zipRecordBytes(b []byte) []byte {
	return append([]byte{0, 0, 0, 0, 0, byte(len(b) >> 16), byte(len(b) >> 8), byte(len(b))}, b...)
}

// Dates that take turns beyond the writer bound reload their writer on every
// turn. The reload validates count.dat against the cached block (the decode
// of the same complete members) instead of decoding the block again.
func TestZipWriterReloadReusesCachedBlock(t *testing.T) {
	dir := t.TempDir()
	var events []RecoveryEvent
	s := zipTestStore(t, dir, &events)
	s.SetCompressDailyCountCacheSize(2) // one writer position
	decodes := countLoadDecodes(t)
	dates := []string{zipTestDate, "20261014"}
	type rec struct {
		date string
		pos  int64
		b    []byte
	}
	var recs []rec
	want := map[string][]byte{}
	for i := 0; i < 8; i++ {
		date, b := dates[i%2], zipPayload(i, 100+i)
		pos, err := s.appendZip(date, b)
		if err != nil {
			t.Fatal(i, err)
		}
		if pos != int64(len(want[date])) {
			t.Fatal(i, pos, len(want[date]))
		}
		recs = append(recs, rec{date, pos, b})
		want[date] = append(want[date], zipRecordBytes(b)...)
	}
	if *decodes != 0 || len(events) != 0 {
		t.Fatal("reloads decoded", *decodes, events)
	}
	for _, r := range recs {
		if got, err := s.readZip(r.date, r.pos); err != nil || !bytes.Equal(got, r.b) {
			t.Fatal(r.date, r.pos, err)
		}
	}
	for _, date := range dates {
		b := decodeZipFile(t, filepath.Join(dir, date, "xlog", "data", "xlog.0"))
		if b.Members != 4 || b.Torn != 0 || !bytes.Equal(b.Data, want[date]) {
			t.Fatal(date, b.Members, b.Torn, len(b.Data))
		}
	}
}

// A member write that fails (here torn by ENOSPC) is truncated away and the
// writer reloads. The truncated block holds the cached members again, so the
// reloads do not decode it, and appends resume at the same offset.
func TestZipWriterWriteFailureReloadWithoutDecode(t *testing.T) {
	dir := t.TempDir()
	var events []RecoveryEvent
	s := zipTestStore(t, dir, &events)
	pos, _ := writeZipRecords(t, s, []byte{1}, []byte{2, 2})
	decodes := countLoadDecodes(t)
	fail := 3
	zipMemberWriteHook = func(f *os.File, member []byte, off int64) (int, error) {
		if fail == 0 {
			return f.WriteAt(member, off)
		}
		fail--
		n, _ := f.WriteAt(member[:len(member)/2], off)
		return n, syscall.ENOSPC
	}
	t.Cleanup(func() { zipMemberWriteHook = nil })
	for i := 0; i < 3; i++ {
		if _, err := s.appendZip(zipTestDate, []byte{3, 3, 3}); !errors.Is(err, syscall.ENOSPC) {
			t.Fatal(i, err)
		}
	}
	next, err := s.appendZip(zipTestDate, []byte{4})
	if err != nil || next != pos[1]+10 {
		t.Fatal(next, err)
	}
	if *decodes != 0 || len(events) != 0 {
		t.Fatal("reloads decoded", *decodes, events)
	}
	block := filepath.Join(zipTestData(dir), "xlog.0")
	b := decodeZipFile(t, block)
	wantData := append(append(zipRecordBytes([]byte{1}), zipRecordBytes([]byte{2, 2})...), zipRecordBytes([]byte{4})...)
	if b.Members != 3 || b.Torn != 0 || !bytes.Equal(b.Data, wantData) {
		t.Fatal(b.Members, b.Torn, len(b.Data))
	}
	s.Close()
	s = zipTestStore(t, dir, &events)
	for i, p := range []int64{pos[0], pos[1], next} {
		if got, err := s.readZip(zipTestDate, p); err != nil || !bytes.Equal(got, [][]byte{{1}, {2, 2}, {4}}[i]) {
			t.Fatal(i, got, err)
		}
	}
	if len(events) != 0 {
		t.Fatal(events)
	}
}

// refusedZipStore leaves count.dat past the block's complete members (count
// beyond block, ErrCorrupt) and opens the Store. It returns the logical end
// of the three records.
func refusedZipStore(t *testing.T, dir string, events *[]RecoveryEvent) (*Store, int64) {
	t.Helper()
	s := zipTestStore(t, dir, events)
	pos, _ := writeZipRecords(t, s, []byte{1}, []byte{2, 2}, []byte{3, 3, 3})
	s.Close()
	end := pos[2] + 11
	if err := os.WriteFile(filepath.Join(zipTestData(dir), "count.dat"), longKey(end+5), 0600); err != nil {
		t.Fatal(err)
	}
	return zipTestStore(t, dir, events), end
}

// A writer load refused by validation is remembered while the files it read
// are unchanged: appends fail with the same error without decoding the
// block again and leave the files untouched. Any change to what the load
// reads (count.dat, the block, the directory listing) or a purge validates
// again; an I/O error is never remembered.
func TestZipWriterLoadFailureRemembered(t *testing.T) {
	refuse := func(t *testing.T, s *Store, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := s.appendZip(zipTestDate, []byte{4}); !errors.Is(err, ErrCorrupt) {
				t.Fatal(i, err)
			}
		}
	}
	t.Run("unchanged", func(t *testing.T) {
		dir := t.TempDir()
		var events []RecoveryEvent
		s, _ := refusedZipStore(t, dir, &events)
		block := filepath.Join(zipTestData(dir), "xlog.0")
		before, _ := os.ReadFile(block)
		decodes := countLoadDecodes(t)
		refuse(t, s, 5)
		after, _ := os.ReadFile(block)
		if *decodes != 1 || len(events) != 0 || !bytes.Equal(before, after) {
			t.Fatal(*decodes, events)
		}
	})
	t.Run("count.dat changed", func(t *testing.T) {
		dir := t.TempDir()
		s, end := refusedZipStore(t, dir, nil)
		decodes := countLoadDecodes(t)
		refuse(t, s, 2)
		if err := os.WriteFile(filepath.Join(zipTestData(dir), "count.dat"), longKey(end), 0600); err != nil {
			t.Fatal(err)
		}
		if pos, err := s.appendZip(zipTestDate, []byte{4}); err != nil || pos != end || *decodes != 2 {
			t.Fatal(pos, err, *decodes)
		}
		if _, ok := s.zip.failed[zipTestDate]; ok {
			t.Fatal("failure kept after a successful load")
		}
	})
	t.Run("block truncated", func(t *testing.T) {
		dir := t.TempDir()
		s, _ := refusedZipStore(t, dir, nil)
		decodes := countLoadDecodes(t)
		refuse(t, s, 2)
		block := filepath.Join(zipTestData(dir), "xlog.0")
		st, err := os.Stat(block)
		if err != nil {
			t.Fatal(err)
		}
		// A torn last member: validated again and still refused (count
		// beyond the complete members), before any repair.
		if err = os.Truncate(block, st.Size()-1); err != nil {
			t.Fatal(err)
		}
		refuse(t, s, 3)
		if *decodes != 2 {
			t.Fatal(*decodes)
		}
		if now, _ := os.Stat(block); now.Size() != st.Size()-1 {
			t.Fatal("refused block repaired", now.Size())
		}
	})
	t.Run("block mtime changed", func(t *testing.T) {
		dir := t.TempDir()
		s, _ := refusedZipStore(t, dir, nil)
		decodes := countLoadDecodes(t)
		refuse(t, s, 2)
		later := time.Now().Add(time.Hour)
		if err := os.Chtimes(filepath.Join(zipTestData(dir), "xlog.0"), later, later); err != nil {
			t.Fatal(err)
		}
		refuse(t, s, 2)
		if *decodes != 2 {
			t.Fatal(*decodes)
		}
	})
	t.Run("later block removed", func(t *testing.T) {
		dir := t.TempDir()
		var events []RecoveryEvent
		s := zipTestStore(t, dir, &events)
		pos, _ := writeZipRecords(t, s, []byte{1}, []byte{2, 2})
		s.Close()
		data := zipTestData(dir)
		// count behind complete records is recoverable only without blocks
		// beyond the next one.
		if err := os.WriteFile(filepath.Join(data, "count.dat"), longKey(pos[1]), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(data, "xlog.5"), nil, 0600); err != nil {
			t.Fatal(err)
		}
		s = zipTestStore(t, dir, &events)
		decodes := countLoadDecodes(t)
		refuse(t, s, 3)
		if *decodes != 1 {
			t.Fatal(*decodes)
		}
		if err := os.Remove(filepath.Join(data, "xlog.5")); err != nil {
			t.Fatal(err)
		}
		// The removal changes the directory mtime; a coarse filesystem clock
		// may not have ticked since the refused load, so move it explicitly.
		later := time.Now().Add(time.Hour)
		if err := os.Chtimes(data, later, later); err != nil {
			t.Fatal(err)
		}
		if p, err := s.appendZip(zipTestDate, []byte{4}); err != nil || p != pos[1]+10 || *decodes != 2 {
			t.Fatal(p, err, *decodes)
		}
		if len(events) != 1 || events[0].Kind != "zip-count-advanced" {
			t.Fatal(events)
		}
	})
	t.Run("purge", func(t *testing.T) {
		dir := t.TempDir()
		s, _ := refusedZipStore(t, dir, nil)
		decodes := countLoadDecodes(t)
		refuse(t, s, 2)
		if n, err := s.DeleteDatabasePaths(context.Background(), []string{"/" + zipTestDate}); err != nil || n != 1 {
			t.Fatal(n, err)
		}
		if len(s.zip.failed) != 0 {
			t.Fatal("purge kept the refused load", s.zip.failed)
		}
		if pos, err := s.appendZip(zipTestDate, []byte{4}); err != nil || pos != 0 || *decodes != 1 {
			t.Fatal(pos, err, *decodes)
		}
	})
	t.Run("io error not remembered", func(t *testing.T) {
		dir := t.TempDir()
		s := zipTestStore(t, dir, nil)
		pos, _ := writeZipRecords(t, s, []byte{1}, []byte{2, 2})
		s.Close()
		s = zipTestStore(t, dir, nil)
		decodes := 0
		zipLoadDecodeHook = func() error {
			if decodes++; decodes == 1 {
				return syscall.EIO
			}
			return nil
		}
		t.Cleanup(func() { zipLoadDecodeHook = nil })
		if _, err := s.appendZip(zipTestDate, []byte{4}); !errors.Is(err, syscall.EIO) {
			t.Fatal(err)
		}
		if p, err := s.appendZip(zipTestDate, []byte{4}); err != nil || p != pos[1]+10 || decodes != 2 {
			t.Fatal(p, err, decodes)
		}
	})
}

// Refused loads are bounded by zipLoadFailures (not the writer bound): the
// least recently refused date is forgotten first.
func TestZipLoadFailuresBounded(t *testing.T) {
	dir := t.TempDir()
	s := zipTestStore(t, dir, nil)
	var dates []string
	for i := 0; i <= zipLoadFailures; i++ {
		date := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, i).Format("20060102")
		dates = append(dates, date)
		data := filepath.Join(dir, date, "xlog", "data")
		if err := os.MkdirAll(data, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(data, "count.dat"), longKey(5), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := s.appendZip(date, []byte{1}); !errors.Is(err, ErrCorrupt) {
			t.Fatal(date, err)
		}
	}
	if len(s.zip.failed) != zipLoadFailures || s.zip.failed[dates[0]] != nil || s.zip.failed[dates[1]] == nil {
		t.Fatal(len(s.zip.failed))
	}
}

// refuseZip appends n records to date, each refused with ErrCorrupt.
func refuseZip(t *testing.T, s *Store, date string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := s.appendZip(date, []byte{4}); !errors.Is(err, ErrCorrupt) {
			t.Fatal(date, i, err)
		}
	}
}

// evictZipHandles pushes the Store file cache past its bound so the next load
// reopens count.dat and the block by path.
func evictZipHandles(t *testing.T, s *Store, dir string) {
	t.Helper()
	for i := 0; i < 20; i++ {
		if _, err := s.appendData(filepath.Join(dir, "evict", "f"+string(rune('a'+i))), []byte{1}, 4); err != nil {
			t.Fatal(err)
		}
	}
}

// appendZipMember appends the gzip member of record b to a block file, as
// another writer would.
func appendZipMember(t *testing.T, path string, b []byte) {
	t.Helper()
	var out bytes.Buffer
	z, err := gzip.NewWriterLevel(&out, zipLevel)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = z.Write(zipRecordBytes(b)); err != nil {
		t.Fatal(err)
	}
	if err = z.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err = f.Write(out.Bytes()); err != nil {
		t.Fatal(err)
	}
}

// The failure key holds the stat of the count.dat and block handles the load
// read. A file replaced by path (mv during a manual repair) is read, and so
// validated, once the Store reopens it; until then the cached handle still
// names the refused file and the load refuses as before.
func TestZipLoadFailureKeyFollowsHandles(t *testing.T) {
	replace := func(t *testing.T, path string, b []byte) {
		t.Helper()
		tmp := path + ".new"
		if err := os.WriteFile(tmp, b, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, path); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("count.dat replaced", func(t *testing.T) {
		dir := t.TempDir()
		s, end := refusedZipStore(t, dir, nil)
		decodes := countLoadDecodes(t)
		refuseZip(t, s, zipTestDate, 2)
		replace(t, filepath.Join(zipTestData(dir), "count.dat"), longKey(end))
		refuseZip(t, s, zipTestDate, 2) // the cached handle is the old count.dat
		before := *decodes
		evictZipHandles(t, s, dir)
		if pos, err := s.appendZip(zipTestDate, []byte{4}); err != nil || pos != end || *decodes != before+1 {
			t.Fatal(pos, err, *decodes, before)
		}
	})
	t.Run("block replaced", func(t *testing.T) {
		dir := t.TempDir()
		s := zipTestStore(t, dir, nil)
		pos, sizes := writeZipRecords(t, s, []byte{1}, []byte{2, 2}, []byte{3, 3, 3})
		s.Close()
		block := filepath.Join(zipTestData(dir), "xlog.0")
		good, err := os.ReadFile(block)
		if err != nil {
			t.Fatal(err)
		}
		bad := bytes.Clone(good)
		bad[sizes[2]-5] ^= 0xff // CRC32 of member 1
		if err = os.WriteFile(block, bad, 0600); err != nil {
			t.Fatal(err)
		}
		s = zipTestStore(t, dir, nil)
		decodes := countLoadDecodes(t)
		refuseZip(t, s, zipTestDate, 2)
		replace(t, block, good)
		refuseZip(t, s, zipTestDate, 2) // the cached handle is the damaged block
		before := *decodes
		evictZipHandles(t, s, dir)
		if p, err := s.appendZip(zipTestDate, []byte{4}); err != nil || p != pos[2]+11 || *decodes != before+1 {
			t.Fatal(p, err, *decodes, before)
		}
	})
}

// Refused dates that take turns beyond the writer bound stay remembered: each
// block is decoded once.
func TestZipLoadFailuresBeyondWriterBound(t *testing.T) {
	dir := t.TempDir()
	s := zipTestStore(t, dir, nil)
	dates := []string{"20261011", "20261012", "20261013"}
	ends := map[string]int64{}
	for _, date := range dates {
		for _, b := range [][]byte{{1}, {2, 2}} {
			p, err := s.appendZip(date, b)
			if err != nil {
				t.Fatal(err)
			}
			ends[date] = p + 8 + int64(len(b))
		}
	}
	s.Close()
	for _, date := range dates {
		if err := os.WriteFile(filepath.Join(dir, date, "xlog", "data", "count.dat"), longKey(ends[date]+5), 0600); err != nil {
			t.Fatal(err)
		}
	}
	s = zipTestStore(t, dir, nil)
	s.SetCompressDailyCountCacheSize(2) // one writer position
	decodes := countLoadDecodes(t)
	for round := 0; round < 3; round++ {
		for _, date := range dates {
			refuseZip(t, s, date, 1)
		}
	}
	if *decodes != len(dates) {
		t.Fatal(*decodes)
	}
}

// A read cached a block with a torn final member (end < size): the writer
// load does not take it as its decode; it decodes, truncates the torn member
// (zip-torn-member) and appends at count.
func TestZipLoadSkipsTornReadCache(t *testing.T) {
	dir := t.TempDir()
	var events []RecoveryEvent
	s := zipTestStore(t, dir, &events)
	pos, sizes := writeZipRecords(t, s, []byte{1}, []byte{2, 2}, zipPayload(9, 64<<10))
	s.Close()
	data := zipTestData(dir)
	block := filepath.Join(data, "xlog.0")
	st, err := os.Stat(block)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Truncate(block, sizes[2]+(st.Size()-sizes[2])/2); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(data, "count.dat"), longKey(pos[2]), 0600); err != nil {
		t.Fatal(err)
	}
	s = zipTestStore(t, dir, &events)
	if got, err := s.readZip(zipTestDate, pos[1]); err != nil || !bytes.Equal(got, []byte{2, 2}) {
		t.Fatal(got, err)
	}
	if c := s.zip.blocks[block]; c == nil || c.end != sizes[2] || c.info.Size() == c.end {
		t.Fatal("torn block not cached by the read", c)
	}
	decodes := countLoadDecodes(t)
	if next, err := s.appendZip(zipTestDate, []byte{3}); err != nil || next != pos[2] || *decodes != 1 {
		t.Fatal(next, err, *decodes)
	}
	if len(events) != 1 || events[0].Kind != "zip-torn-member" {
		t.Fatal(events)
	}
	if got, err := s.readZip(zipTestDate, pos[2]); err != nil || !bytes.Equal(got, []byte{3}) {
		t.Fatal(got, err)
	}
}

// A block a query decoded outside the gate and joined with the members
// appended meanwhile (useFetchedLocked) is cached with end at the joined
// tail, so the writer load takes it without decoding.
func TestZipLoadReusesFetchedTail(t *testing.T) {
	dir := t.TempDir()
	var events []RecoveryEvent
	s := zipTestStore(t, dir, &events)
	pos, _ := writeZipRecords(t, s, []byte{1}, []byte{2, 2})
	s.Close()
	data := zipTestData(dir)
	block := filepath.Join(data, "xlog.0")
	s = zipTestStore(t, dir, &events)
	ctx := context.Background()
	fetch := &zipFetch{}
	_, err := s.readZipFetch(ctx, zipTestDate, pos[0], fetch)
	var miss *zipMiss
	if !errors.As(err, &miss) {
		t.Fatal(err)
	}
	if err = s.fetchZip(ctx, fetch, miss); err != nil {
		t.Fatal(err)
	}
	appendZipMember(t, block, []byte{3, 3, 3})
	end := pos[1] + 10 + 11
	if err = os.WriteFile(filepath.Join(data, "count.dat"), longKey(end), 0600); err != nil {
		t.Fatal(err)
	}
	tails := 0
	zipTailHook = func(int64) { tails++ }
	t.Cleanup(func() { zipTailHook = nil })
	if got, err := s.readZipFetch(ctx, zipTestDate, pos[0], fetch); err != nil || !bytes.Equal(got, []byte{1}) || tails != 1 {
		t.Fatal(got, err, tails)
	}
	st, err := os.Stat(block)
	if err != nil {
		t.Fatal(err)
	}
	if c := s.zip.blocks[block]; c == nil || c.end != st.Size() {
		t.Fatal("joined block not cached to the file end", c)
	}
	decodes := countLoadDecodes(t)
	if p, err := s.appendZip(zipTestDate, []byte{4}); err != nil || p != end || *decodes != 0 || len(events) != 0 {
		t.Fatal(p, err, *decodes, events)
	}
	for _, r := range []struct {
		pos  int64
		want []byte
	}{{pos[1], []byte{2, 2}}, {pos[1] + 10, []byte{3, 3, 3}}, {end, []byte{4}}} {
		if got, err := s.readZip(zipTestDate, r.pos); err != nil || !bytes.Equal(got, r.want) {
			t.Fatal(r.pos, got, err)
		}
	}
}

// A cached block changed outside the writer (another process appended, or
// only its mtime moved) is not the cached decode: the reload decodes it.
func TestZipLoadDecodesExternallyChangedBlock(t *testing.T) {
	for name, change := range map[string]func(t *testing.T, data string) int64{
		"append": func(t *testing.T, data string) int64 {
			appendZipMember(t, filepath.Join(data, "xlog.0"), []byte{3, 3, 3})
			if err := os.WriteFile(filepath.Join(data, "count.dat"), longKey(30), 0600); err != nil {
				t.Fatal(err)
			}
			return 30
		},
		"chtimes": func(t *testing.T, data string) int64 {
			later := time.Now().Add(time.Hour)
			if err := os.Chtimes(filepath.Join(data, "xlog.0"), later, later); err != nil {
				t.Fatal(err)
			}
			return 19
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			var events []RecoveryEvent
			s := zipTestStore(t, dir, &events)
			s.SetCompressDailyCountCacheSize(2) // one writer position
			writeZipRecords(t, s, []byte{1}, []byte{2, 2})
			if _, err := s.appendZip("20261014", []byte{9}); err != nil { // evicts the writer
				t.Fatal(err)
			}
			want := change(t, zipTestData(dir))
			decodes := countLoadDecodes(t)
			if p, err := s.appendZip(zipTestDate, []byte{4}); err != nil || p != want || *decodes != 1 || len(events) != 0 {
				t.Fatal(p, err, *decodes, events)
			}
		})
	}
}

// A failed count.dat write leaves the member in the block and in the cache:
// the reload advances count over it (zip-count-advanced) without decoding.
func TestZipCountWriteFailureReloadWithoutDecode(t *testing.T) {
	dir := t.TempDir()
	var events []RecoveryEvent
	s := zipTestStore(t, dir, &events)
	pos, _ := writeZipRecords(t, s, []byte{1}, []byte{2, 2})
	countPath := filepath.Join(zipTestData(dir), "count.dat")
	rw := s.files[countPath]
	if rw == nil {
		t.Fatal("count.dat handle not cached")
	}
	ro, err := os.Open(countPath)
	if err != nil {
		t.Fatal(err)
	}
	s.files[countPath] = ro // the writer rebinds to it; its WriteAt fails
	decodes := countLoadDecodes(t)
	if _, err = s.appendZip(zipTestDate, []byte{3, 3, 3}); err == nil {
		t.Fatal("count write on a read-only handle succeeded")
	}
	s.files[countPath] = rw
	ro.Close()
	if p, err := s.appendZip(zipTestDate, []byte{4}); err != nil || p != pos[1]+10+11 || *decodes != 0 {
		t.Fatal(p, err, *decodes)
	}
	if len(events) != 1 || events[0].Kind != "zip-count-advanced" {
		t.Fatal(events)
	}
	if got, err := s.readZip(zipTestDate, pos[1]+10+11); err != nil || !bytes.Equal(got, []byte{4}) {
		t.Fatal(got, err)
	}
}

// A member write whose truncation also fails drops the cached block (its file
// is no longer known to match), so the reload decodes the block again.
func TestZipWriteAndTruncateFailureDecodesAgain(t *testing.T) {
	dir := t.TempDir()
	var events []RecoveryEvent
	s := zipTestStore(t, dir, &events)
	pos, _ := writeZipRecords(t, s, []byte{1}, []byte{2, 2})
	block := filepath.Join(zipTestData(dir), "xlog.0")
	rw := s.files[block]
	if rw == nil {
		t.Fatal("block handle not cached")
	}
	ro, err := os.Open(block)
	if err != nil {
		t.Fatal(err)
	}
	s.files[block] = ro // the writer rebinds to it; WriteAt and Truncate fail
	decodes := countLoadDecodes(t)
	if _, err = s.appendZip(zipTestDate, []byte{3, 3, 3}); err == nil {
		t.Fatal("member write on a read-only handle succeeded")
	}
	if s.zip.blocks[block] != nil {
		t.Fatal("cached block kept after a failed truncation")
	}
	s.files[block] = rw
	ro.Close()
	if p, err := s.appendZip(zipTestDate, []byte{4}); err != nil || p != pos[1]+10 || *decodes != 1 || len(events) != 0 {
		t.Fatal(p, err, *decodes, events)
	}
}

// A failed first member of a rollover block is truncated away; the reload
// takes the full previous block from the cache and the rollover resumes.
func TestZipRolloverWriteFailureResumes(t *testing.T) {
	dir := t.TempDir()
	data := zipTestData(dir)
	base := zipRecords(0x7ffff0, 0x7ffff0, 0x7ffff0, 0x7ffff0)
	writeGzipTestBlock(t, filepath.Join(data, "xlog.0"), base)
	if err := os.WriteFile(filepath.Join(data, "count.dat"), longKey(int64(len(base))), 0600); err != nil {
		t.Fatal(err)
	}
	var events []RecoveryEvent
	s := zipTestStore(t, dir, &events)
	fail := 1
	zipMemberWriteHook = func(f *os.File, member []byte, off int64) (int, error) {
		if fail == 0 {
			return f.WriteAt(member, off)
		}
		fail--
		n, _ := f.WriteAt(member[:len(member)/2], off)
		return n, syscall.ENOSPC
	}
	t.Cleanup(func() { zipMemberWriteHook = nil })
	b := zipPayload(1, 100)
	if _, err := s.appendZip(zipTestDate, b); !errors.Is(err, syscall.ENOSPC) {
		t.Fatal(err)
	}
	if st, err := os.Stat(filepath.Join(data, "xlog.1")); err != nil || st.Size() != 0 {
		t.Fatal("failed rollover member not truncated", err)
	}
	decodes := countLoadDecodes(t)
	if p, err := s.appendZip(zipTestDate, b); err != nil || p != zipBlockBytes || *decodes != 0 || len(events) != 0 {
		t.Fatal(p, err, *decodes, events)
	}
	if got, err := s.readZip(zipTestDate, zipBlockBytes); err != nil || !bytes.Equal(got, b) {
		t.Fatal(len(got), err)
	}
	if blk := decodeZipFile(t, filepath.Join(data, "xlog.1")); blk.Members != 1 || blk.Torn != 0 {
		t.Fatal(blk.Members, blk.Torn)
	}
}
