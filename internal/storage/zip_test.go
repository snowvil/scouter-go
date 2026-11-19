package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

const zipTestDate = "20261013"

func zipTestStore(t *testing.T, dir string, events *[]RecoveryEvent) *Store {
	t.Helper()
	s, err := Open(Config{Directory: dir, Location: time.UTC, OnRecover: func(e RecoveryEvent) {
		if events != nil {
			*events = append(*events, e)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func zipTestData(dir string) string { return filepath.Join(dir, zipTestDate, "xlog", "data") }
func decodeZipFile(t *testing.T, path string) ZipBlock {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	b, err := DecodeZipBlock(context.Background(), f, st.Size())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// zipPayload is a deterministic record of n bytes.
func zipPayload(i, n int) []byte {
	b := make([]byte, n)
	for j := range b {
		b[j] = byte(i*31 + j*7)
	}
	return b
}

// Every append is one gzip member; the stdlib multistream reader (the
// GZIPInputStream contract) sees the same logical bytes, and reads interleaved
// with appends stay coherent with the cached block.
func TestZipMultiMemberAppendRead(t *testing.T) {
	dir := t.TempDir()
	s := zipTestStore(t, dir, nil)
	type rec struct {
		pos  int64
		data []byte
	}
	var recs []rec
	for i := 0; i < 40; i++ {
		b := zipPayload(i, 1+i*37%2000)
		pos, err := s.appendZip(zipTestDate, b)
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, rec{pos, b})
		if i%3 == 0 { // read an older record between appends
			r := recs[i/2]
			if got, err := s.readZip(zipTestDate, r.pos); err != nil || !bytes.Equal(got, r.data) {
				t.Fatal(i, err)
			}
		}
	}
	block := decodeZipFile(t, filepath.Join(zipTestData(dir), "xlog.0"))
	if block.Members != 40 || block.Torn != 0 {
		t.Fatalf("members %d torn %d", block.Members, block.Torn)
	}
	raw, err := os.ReadFile(filepath.Join(zipTestData(dir), "xlog.0"))
	if err != nil {
		t.Fatal(err)
	}
	z, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	multistream, err := io.ReadAll(z)
	if err != nil || !bytes.Equal(multistream, block.Data) {
		t.Fatal("multistream decode differs", err)
	}
	count, err := os.ReadFile(filepath.Join(zipTestData(dir), "count.dat"))
	if err != nil || readSignedLong5(count[3:]) != int64(len(block.Data)) {
		t.Fatalf("count %x logical %d %v", count, len(block.Data), err)
	}
	s.Close()
	s = zipTestStore(t, dir, nil)
	for _, r := range recs {
		if got, err := s.readZip(zipTestDate, r.pos); err != nil || !bytes.Equal(got, r.data) {
			t.Fatal(r.pos, err)
		}
	}
}

// A Java block (a single member saved by IOChannel) is continued with members.
func TestZipContinuesJavaSingleMemberBlock(t *testing.T) {
	dir := t.TempDir()
	data := zipTestData(dir)
	java := zipRecords(3, 5000)
	java[8], java[19] = 7, 9
	writeGzipTestBlock(t, filepath.Join(data, "xlog.0"), java)
	if err := os.WriteFile(filepath.Join(data, "count.dat"), longKey(int64(len(java))), 0600); err != nil {
		t.Fatal(err)
	}
	var events []RecoveryEvent
	s := zipTestStore(t, dir, &events)
	pos, err := s.appendZip(zipTestDate, []byte{4, 2})
	if err != nil || pos != int64(len(java)) || len(events) != 0 {
		t.Fatal(pos, err, events)
	}
	block := decodeZipFile(t, filepath.Join(data, "xlog.0"))
	if block.Members != 2 || !bytes.Equal(block.Data[:len(java)], java) {
		t.Fatal("java member changed", block.Members)
	}
	for _, c := range []struct {
		pos  int64
		want []byte
	}{{0, []byte{7, 0, 0}}, {pos, []byte{4, 2}}} {
		if got, err := s.readZip(zipTestDate, c.pos); err != nil || !bytes.Equal(got, c.want) {
			t.Fatal(c.pos, got, err)
		}
	}
	if got, err := s.readZip(zipTestDate, 11); err != nil || len(got) != 5000 || got[0] != 9 {
		t.Fatal(err)
	}
}

// writeZipRecords appends payloads and returns their positions and the file size
// of xlog.0 before each append.
func writeZipRecords(t *testing.T, s *Store, payloads ...[]byte) (pos, sizes []int64) {
	t.Helper()
	path := s.path(zipTestDate, "xlog", filepath.Join("data", "xlog.0"))
	for _, b := range payloads {
		size := int64(0)
		if st, err := os.Stat(path); err == nil {
			size = st.Size()
		}
		p, err := s.appendZip(zipTestDate, b)
		if err != nil {
			t.Fatal(err)
		}
		pos, sizes = append(pos, p), append(sizes, size)
	}
	return pos, sizes
}

// A crash inside the last member write (count.dat not yet rewritten) leaves a
// torn member. Reads of complete members still work, a read of the torn record
// fails instead of returning partial bytes, and the writer truncates it.
func TestZipTornMemberTruncatedByWriter(t *testing.T) {
	big := zipPayload(9, 64<<10)
	for name, keep := range map[string]func(start, end int64) int64{
		"header":  func(start, end int64) int64 { return start + 2 },
		"deflate": func(start, end int64) int64 { return start + (end-start)/2 },
		"trailer": func(start, end int64) int64 { return end - 3 },
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			var events []RecoveryEvent
			s := zipTestStore(t, dir, &events)
			pos, sizes := writeZipRecords(t, s, []byte{1}, []byte{2, 2}, big)
			s.Close()
			data := zipTestData(dir)
			block := filepath.Join(data, "xlog.0")
			st, err := os.Stat(block)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.Truncate(block, keep(sizes[2], st.Size())); err != nil {
				t.Fatal(err)
			}
			// count.dat is written after the member: it still ends at record 2.
			if err = os.WriteFile(filepath.Join(data, "count.dat"), longKey(pos[2]), 0600); err != nil {
				t.Fatal(err)
			}
			s = zipTestStore(t, dir, &events)
			if got, err := s.readZip(zipTestDate, pos[1]); err != nil || !bytes.Equal(got, []byte{2, 2}) {
				t.Fatal("complete member before the torn tail", got, err)
			}
			if got, err := s.readZip(zipTestDate, pos[2]); err == nil || got != nil {
				t.Fatal("torn record returned", len(got))
			}
			next, err := s.appendZip(zipTestDate, []byte{3})
			if err != nil || next != pos[2] {
				t.Fatal(next, err)
			}
			if len(events) != 1 || events[0].Kind != "zip-torn-member" || events[0].Path != block {
				t.Fatal(events)
			}
			b := decodeZipFile(t, block)
			if b.Torn != 0 || b.Members != 3 {
				t.Fatal(b.Members, b.Torn)
			}
			for i, want := range [][]byte{{1}, {2, 2}, {3}} {
				if got, err := s.readZip(zipTestDate, pos[i]); err != nil || !bytes.Equal(got, want) {
					t.Fatal(i, got, err)
				}
			}
		})
	}
}

// Shapes this writer cannot produce stay ErrCorrupt and are not modified:
// count past the complete members (count is written after the member), damage
// inside an earlier member, and trailing bytes that are not a gzip member.
func TestZipCorruptMembersRejectedUntouched(t *testing.T) {
	for name, damage := range map[string]func(t *testing.T, data string, pos, sizes []int64){
		"count past torn member": func(t *testing.T, data string, pos, sizes []int64) {
			st, _ := os.Stat(filepath.Join(data, "xlog.0"))
			if err := os.Truncate(filepath.Join(data, "xlog.0"), st.Size()-4); err != nil {
				t.Fatal(err)
			}
		},
		"middle member checksum": func(t *testing.T, data string, pos, sizes []int64) {
			b, _ := os.ReadFile(filepath.Join(data, "xlog.0"))
			b[sizes[2]-5] ^= 0xff // CRC32 of member 1
			if err := os.WriteFile(filepath.Join(data, "xlog.0"), b, 0600); err != nil {
				t.Fatal(err)
			}
		},
		"middle member header": func(t *testing.T, data string, pos, sizes []int64) {
			b, _ := os.ReadFile(filepath.Join(data, "xlog.0"))
			b[sizes[1]] = 0
			if err := os.WriteFile(filepath.Join(data, "xlog.0"), b, 0600); err != nil {
				t.Fatal(err)
			}
		},
		"garbage tail": func(t *testing.T, data string, pos, sizes []int64) {
			f, err := os.OpenFile(filepath.Join(data, "xlog.0"), os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if _, err = f.Write([]byte{0, 0, 0, 0}); err != nil {
				t.Fatal(err)
			}
		},
		"zero-filled tail": func(t *testing.T, data string, pos, sizes []int64) {
			f, err := os.OpenFile(filepath.Join(data, "xlog.0"), os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if _, err = f.Write(make([]byte, 4096)); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			var events []RecoveryEvent
			s := zipTestStore(t, dir, &events)
			pos, sizes := writeZipRecords(t, s, []byte{1}, []byte{2, 2}, []byte{3, 3, 3})
			s.Close()
			data := zipTestData(dir)
			damage(t, data, pos, sizes)
			before, _ := os.ReadFile(filepath.Join(data, "xlog.0"))
			s = zipTestStore(t, dir, &events)
			if _, err := s.appendZip(zipTestDate, []byte{4}); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
			after, _ := os.ReadFile(filepath.Join(data, "xlog.0"))
			if !bytes.Equal(before, after) || len(events) != 0 {
				t.Fatal("refused state modified", events)
			}
			if name != "count past torn member" {
				if _, err := s.readZip(zipTestDate, pos[0]); !errors.Is(err, ErrCorrupt) {
					t.Fatal("read of damaged block", err)
				}
			}
		})
	}
}

// Rollover leaves the full block untouched, truncates a leftover orphan and
// keeps appending members to the new block.
func TestZipRolloverTruncatesOrphan(t *testing.T) {
	dir := t.TempDir()
	data := zipTestData(dir)
	base := zipRecords(0x7ffff0, 0x7ffff0, 0x7ffff0, 0x7ffff0)
	writeGzipTestBlock(t, filepath.Join(data, "xlog.0"), base)
	writeGzipTestBlock(t, filepath.Join(data, "xlog.1"), zipRecords(4000)) // rollover orphan
	if err := os.WriteFile(filepath.Join(data, "count.dat"), longKey(int64(len(base))), 0600); err != nil {
		t.Fatal(err)
	}
	var events []RecoveryEvent
	s := zipTestStore(t, dir, &events)
	before, _ := os.ReadFile(filepath.Join(data, "xlog.0"))
	pos, _ := writeZipRecords(t, s, zipPayload(1, 100), []byte{5}, []byte{6})
	if pos[0] != zipBlockBytes || pos[1] != zipBlockBytes+108 || len(events) != 0 {
		t.Fatal(pos, events)
	}
	after, _ := os.ReadFile(filepath.Join(data, "xlog.0"))
	if !bytes.Equal(before, after) {
		t.Fatal("full block rewritten")
	}
	b := decodeZipFile(t, filepath.Join(data, "xlog.1"))
	if b.Members != 3 || len(b.Data) != 108+9+9 {
		t.Fatal(b.Members, len(b.Data))
	}
	if got, err := s.readZip(zipTestDate, pos[2]); err != nil || !bytes.Equal(got, []byte{6}) {
		t.Fatal(got, err)
	}
}

// A rollover crash after the orphan truncation (empty next block) or inside
// the first member write (torn next block) is still a recoverable orphan when
// count.dat must also be advanced in the previous block.
func TestZipCountRecoveryEmptyOrTornOrphan(t *testing.T) {
	base := zipRecords(0x7ffff0, 0x7ffff0, 0x7ffff0, 0x7ffff0)
	for name, orphan := range map[string]func(path string) error{
		"empty": func(path string) error { return os.WriteFile(path, nil, 0600) },
		"torn": func(path string) error {
			var out bytes.Buffer
			z := gzip.NewWriter(&out)
			z.Write(zipRecords(1000))
			z.Close()
			return os.WriteFile(path, out.Bytes()[:out.Len()/2], 0600)
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			data := zipTestData(dir)
			writeGzipTestBlock(t, filepath.Join(data, "xlog.0"), append(append([]byte{}, base...), zipRecords(10)...))
			if err := os.WriteFile(filepath.Join(data, "count.dat"), longKey(int64(len(base))), 0600); err != nil {
				t.Fatal(err)
			}
			if err := orphan(filepath.Join(data, "xlog.1")); err != nil {
				t.Fatal(err)
			}
			var events []RecoveryEvent
			s := zipTestStore(t, dir, &events)
			pos, err := s.appendZip(zipTestDate, []byte{8})
			if err != nil || pos != int64(len(base))+18 || len(events) != 1 || !strings.Contains(events[0].Detail, "orphan block 1") {
				t.Fatal(pos, err, events)
			}
		})
	}
}

// Store file-cache eviction closes the writer's handles; the reopened handles
// name the same files and appends continue without a reload or a repair.
func TestZipWriterSurvivesHandleEviction(t *testing.T) {
	dir := t.TempDir()
	var events []RecoveryEvent
	s := zipTestStore(t, dir, &events)
	first, _ := writeZipRecords(t, s, []byte{1})
	for i := 0; i < 20; i++ {
		if _, err := s.appendData(filepath.Join(dir, "evict", "f"+string(rune('a'+i))), []byte{1}, 4); err != nil {
			t.Fatal(err)
		}
	}
	second, _ := writeZipRecords(t, s, []byte{2})
	if second[0] != first[0]+9 || len(events) != 0 {
		t.Fatal(first, second, events)
	}
	s.Close()
	s = zipTestStore(t, dir, &events)
	for i, p := range []int64{first[0], second[0]} {
		if got, err := s.readZip(zipTestDate, p); err != nil || !bytes.Equal(got, []byte{byte(i + 1)}) {
			t.Fatal(got, err)
		}
	}
	if len(events) != 0 {
		t.Fatal(events)
	}
}

// DecodeZipBlock classifies tails: a short gzip-header prefix is torn, other
// short bytes are corrupt, and an incomplete member longer than one record
// member cannot be a torn append.
func TestDecodeZipBlockTailClassification(t *testing.T) {
	var member bytes.Buffer
	z := gzip.NewWriter(&member)
	z.Write(zipRecords(20))
	z.Close()
	for name, c := range map[string]struct {
		tail    []byte
		torn    bool
		corrupt bool
	}{
		"magic prefix":  {tail: []byte{0x1f, 0x8b}, torn: true},
		"one byte":      {tail: []byte{0x1f}, torn: true},
		"not magic":     {tail: []byte{0x1f, 0x00}, corrupt: true},
		"half member":   {tail: member.Bytes()[:member.Len()/2], torn: true},
		"clean members": {tail: member.Bytes()},
	} {
		t.Run(name, func(t *testing.T) {
			file := append(append([]byte{}, member.Bytes()...), c.tail...)
			b, err := DecodeZipBlock(context.Background(), bytes.NewReader(file), int64(len(file)))
			if c.corrupt {
				if !errors.Is(err, ErrCorrupt) {
					t.Fatal(err)
				}
				return
			}
			if err != nil || (b.Torn > 0) != c.torn || b.End != int64(member.Len()) && c.torn {
				t.Fatal(b.Torn, b.End, err)
			}
			if c.torn && (len(b.Data) != 28 || b.Members != 1) {
				t.Fatal(len(b.Data), b.Members)
			}
		})
	}
	// A valid but incomplete member longer than maxZipTornBytes (stored
	// deflate blocks) cannot be one torn record append: corrupt.
	var stored bytes.Buffer
	zs, _ := gzip.NewWriterLevel(&stored, gzip.NoCompression)
	zs.Write(make([]byte, maxZipTornBytes+(1<<20)))
	zs.Close()
	large := stored.Bytes()[:maxZipTornBytes+1000]
	if _, err := DecodeZipBlock(context.Background(), bytes.NewReader(large), int64(len(large))); !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "incomplete") {
		t.Fatal(err)
	}
}

// zipBlock returns a copy of logical block number.
func (s *Store) zipBlock(date string, number int64) ([]byte, error) {
	b, err := s.zipDataFetch(context.Background(), date, number, nil)
	return slices.Clone(b), err
}

func (s *Store) readZip(date string, pos int64) ([]byte, error) {
	return s.readZipContext(context.Background(), date, pos)
}

func (s *Store) readZipContext(ctx context.Context, date string, pos int64) ([]byte, error) {
	return s.readZipFetch(ctx, date, pos, nil)
}
