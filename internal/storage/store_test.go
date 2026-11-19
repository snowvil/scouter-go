package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"github.com/snowvil/scouter-go/internal/protocol"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func sampleObject(name string) *protocol.ObjectPack {
	return &protocol.ObjectPack{ObjType: "java", ObjHash: 42, ObjName: name, Alive: true, Tags: protocol.Value{Type: protocol.ValueMap}}
}
func TestStoreObjectsTextRestart(t *testing.T) {
	dir := t.TempDir()
	c := Config{Directory: dir, Location: time.UTC}
	s, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"first", "last"} {
		if err = s.PutObject("20261013", sampleObject(name)); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []*protocol.TextPack{{XType: "service", Hash: 7, Text: "first"}, {XType: "service", Hash: 7, Text: "last"}, {XType: "error", Hash: 8, Text: "에러"}} {
		if err = s.PutText("20261013", p); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "20261013/object/obj.data"))
	if err != nil || len(b) != 4100 {
		t.Fatalf("fixed object slot: %d %v", len(b), err)
	}
	s, err = Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	objects, err := s.Objects("20261013")
	if err != nil || len(objects) != 1 || objects[0].ObjName != "last" {
		t.Fatalf("objects: %v %v", objects, err)
	}
	for _, q := range []struct {
		date, div string
		hash      int32
		want      string
	}{{"20261014", "service", 7, "first"}, {"20261013", "error", 8, "에러"}} {
		text, ok, err := s.Text(q.date, q.div, q.hash)
		if err != nil || !ok || text != q.want {
			t.Fatalf("text: %q %t %v", text, ok, err)
		}
	}
	if _, ok, err := s.Text("20261014", "error", 8); err != nil || ok {
		t.Fatalf("daily leaked: %t %v", ok, err)
	}
}
func TestStoreXLogProfileRestart(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "gzip"}[compressed], func(t *testing.T) {
			dir := t.TempDir()
			c := Config{Directory: dir, Location: time.UTC, CompressXLog: compressed, CompressProfile: compressed}
			s, err := Open(c)
			if err != nil {
				t.Fatal(err)
			}
			start := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC).UnixMilli()
			for _, p := range []*protocol.XLogPack{{EndTime: start + 499, TxID: 1, GxID: 4}, {EndTime: start + 500, TxID: 2, GxID: 4}, {EndTime: start + 500, TxID: 3}} {
				if err = s.PutXLog(p); err != nil {
					t.Fatal(err)
				}
			}
			for _, b := range [][]byte{{1, 2}, {3, 4}} {
				if err = s.PutProfile(&protocol.ProfilePack{Time: start, TxID: 1, Profile: b}); err != nil {
					t.Fatal(err)
				}
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			// Persisted per-file compression mode wins over changed process defaults.
			c.CompressXLog = !compressed
			c.CompressProfile = !compressed
			s, err = Open(c)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			x, err := s.XLog("20261013", 2)
			if err != nil || x == nil || x.TxID != 2 {
				t.Fatalf("txid %v %v", x, err)
			}
			list, err := s.XLogs("20261013", start+499, start+500, false)
			if err != nil || len(list) != 3 || list[0].TxID != 1 || list[2].TxID != 3 {
				t.Fatalf("range %v %v", list, err)
			}
			list, err = s.XLogs("20261013", start+499, start+500, true)
			if err != nil || len(list) != 3 || list[0].TxID != 3 {
				t.Fatalf("reverse %v %v", list, err)
			}
			b, err := s.Profile("20261013", 1)
			if err != nil || string(b) != string([]byte{1, 2, 3, 4}) {
				t.Fatalf("profile %x %v", b, err)
			}
			chunks, err := s.ProfileChunks("20261013", 1)
			if err != nil || len(chunks) != 2 {
				t.Fatalf("chunks %v %v", chunks, err)
			}
			if compressed {
				if _, err = os.Stat(filepath.Join(dir, "20261013/xlog/data/count.dat")); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestStoreLimitsAndClosed(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, MaxQueryRecords: 1, MaxQueryBytes: 100, MaxQuerySpan: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.PutObject("../escape", sampleObject("x")); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err = s.PutObject("20261013", sampleObject(strings.Repeat("x", 4096))); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	start := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC).UnixMilli()
	for i := 0; i < 2; i++ {
		if err = s.PutXLog(&protocol.XLogPack{EndTime: start + int64(i), TxID: int64(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.XLogs("20261013", start, start+1, false); !errors.Is(err, ErrLimit) {
		t.Fatalf("count limit %v", err)
	}
	if _, err = s.XLogs("20261013", start, start+int64(2*time.Hour/time.Millisecond), false); !errors.Is(err, ErrLimit) {
		t.Fatalf("span limit %v", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Objects("20261013"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestStoreV2BucketOrderAndOwnedResults(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC).UnixMilli()
	for _, p := range []*protocol.XLogPack{{EndTime: start + 200, TxID: 1}, {EndTime: start + 100, TxID: 2}, {EndTime: start + 200, TxID: 3}, {EndTime: start + 550, TxID: 4}} {
		if err = s.PutXLog(p); err != nil {
			t.Fatal(err)
		}
	}
	records, err := s.XLogsV2("20261013", start, start+600, 0)
	if err != nil || len(records) != 4 {
		t.Fatalf("records %v %v", records, err)
	}
	for i, want := range []int64{3, 2, 1, 4} {
		if records[i].Pack.TxID != want {
			t.Fatalf("v2 order %d got %d want %d", i, records[i].Pack.TxID, want)
		}
	}
	if records[3].BucketTime != start+500 {
		t.Fatal(records[3].BucketTime)
	}
	records[0].Pack.TxID = 999
	original, err := s.XLog("20261013", 3)
	if err != nil || original.TxID != 3 {
		t.Fatalf("borrowed result: %v %v", original, err)
	}
	records, err = s.XLogsV2("20261013", start, start+600, start+500)
	if err != nil || len(records) != 1 || records[0].Pack.TxID != 4 {
		t.Fatalf("resume %v %v", records, err)
	}
}
func TestStoreDailyTextOptionsAndValidation(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, TextDailyService: true, TextDailyAPI: true, TextDailyUA: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, div := range []string{"service", "apicall", "ua"} {
		if err = s.PutText("20261013", &protocol.TextPack{XType: div, Hash: 1, Text: "daily"}); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := s.Text("20261014", div, 1); err != nil || ok {
			t.Fatalf("date leakage %s %v", div, err)
		}
	}
	if err = s.PutText("20261013", &protocol.TextPack{XType: "../escape", Hash: 1, Text: "x"}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err = Open(Config{Directory: t.TempDir()}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err = Open(Config{Directory: t.TempDir(), Location: time.UTC, MaxQueryRecords: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
func TestStoreTruncatedDataAndCompressedCorruption(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "gzip"}[compressed], func(t *testing.T) {
			dir := t.TempDir()
			s, err := Open(Config{Directory: dir, Location: time.UTC, CompressProfile: compressed})
			if err != nil {
				t.Fatal(err)
			}
			start := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC).UnixMilli()
			if err = s.PutProfile(&protocol.ProfilePack{Time: start, TxID: 1, Profile: []byte{1, 2}}); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "20261013/xlog/xlog.profile")
			if compressed {
				path = filepath.Join(dir, "20261013/xlog/data/xlog.0")
			}
			if err = os.Truncate(path, 1); err != nil {
				t.Fatal(err)
			}
			s, err = Open(Config{Directory: dir, Location: time.UTC})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if _, err = s.Profile("20261013", 1); err == nil {
				t.Fatal("truncated data reported success")
			}
		})
	}
}
func TestStoreConcurrentLifecycleAndCacheBound(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				p := sampleObject("object")
				p.ObjHash = int32(i)
				if err := s.PutObject("20261013", p); err != nil {
					t.Error(err)
				}
				if _, err := s.Objects("20261013"); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < 36; i++ {
		date := time.Date(2026, 10, 1+i, 0, 0, 0, 0, time.UTC).Format("20060102")
		if err = s.PutObject(date, sampleObject("evict")); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.indexes) > 32 || len(s.files) > 16 {
		t.Fatalf("unbounded caches %d/%d", len(s.indexes), len(s.files))
	}
	if err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeGzipTestBlock(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	z := gzip.NewWriter(&out)
	if _, err := z.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
}
func TestCompressedLogicalOffsetsLinksAndRollover(t *testing.T) {
	dir := t.TempDir()
	// Files are rewritten behind the Store only between Store lifetimes: an
	// open Store caches decoded blocks and the count.dat position.
	open := func() *Store {
		s, err := Open(Config{Directory: dir, Location: time.UTC})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		return s
	}
	s := open()
	data := filepath.Join(dir, "20261013/xlog/data")
	// Java Long5 next + Int3 size: first logical record at0 points to record10.
	block := []byte{0, 0, 0, 0, 10, 0, 0, 2, 1, 2, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4}
	writeGzipTestBlock(t, filepath.Join(data, "xlog.0"), block)
	got, err := s.readZip("20261013", 0)
	if err != nil || !bytes.Equal(got, []byte{1, 2, 3, 4}) {
		t.Fatalf("linked %x %v", got, err)
	}
	s.Close()
	block[10+4] = 10
	writeGzipTestBlock(t, filepath.Join(data, "xlog.0"), block)
	s = open()
	if _, err = s.readZip("20261013", 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("cycle %v", err)
	}
	s.Close()
	// An almost-full logical block rolls to the next 32MiB offset, not physical gzip filesize.
	block = make([]byte, zipBlockBytes-6)
	writeGzipTestBlock(t, filepath.Join(data, "xlog.0"), block)
	if err = os.WriteFile(filepath.Join(data, "count.dat"), longKey(int64(len(block))), 0600); err != nil {
		t.Fatal(err)
	}
	s = open()
	pos, err := s.appendZip("20261013", []byte{5, 6})
	if err != nil || pos != zipBlockBytes {
		t.Fatalf("rollover %d %v", pos, err)
	}
	got, err = s.readZip("20261013", pos)
	if err != nil || !bytes.Equal(got, []byte{5, 6}) {
		t.Fatalf("rolled read %x %v", got, err)
	}
	s.Close()
	if err = os.WriteFile(filepath.Join(data, "count.dat"), longKey(pos+9), 0600); err != nil {
		t.Fatal(err)
	}
	s = open()
	if _, err = s.appendZip("20261013", []byte{7}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("count mismatch %v", err)
	}
}

func TestLegacySignedLengthsAndOffsets(t *testing.T) {
	if pos, err := dataOffset([]byte{0xff, 0xff, 0xff, 0xff, 0xff}); err == nil || pos >= 0 {
		t.Fatalf("negative Long5 accepted: %d %v", pos, err)
	}
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, MaxRecordBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	data := filepath.Join(s.config.Directory, "20261013/xlog/data")
	writeGzipTestBlock(t, filepath.Join(data, "xlog.0"), []byte{0, 0, 0, 0, 0, 0x80, 0, 0})
	if _, err = s.readZip("20261013", 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("negative signed Int3 accepted: %v", err)
	}
	if _, err = s.appendZip("20261013", make([]byte, 0x800000)); !errors.Is(err, ErrLimit) {
		t.Fatalf("unreadable Java length accepted: %v", err)
	}
	if err = os.WriteFile(filepath.Join(data, "count.dat"), longKey(1<<39), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.appendZip("20261013", []byte{1}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("unreadable Java offset accepted: %v", err)
	}
}

func FuzzCompressedRecords(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 2, 1, 2})
	f.Add([]byte{0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0})
	f.Add([]byte{0, 0, 0, 0, 0, 0x80, 0, 0})
	var members bytes.Buffer
	for _, b := range [][]byte{{0, 0, 0, 0, 0, 0, 0, 1, 7}, {0, 0, 0, 0, 0, 0, 0, 2, 8, 9}} {
		z := gzip.NewWriter(&members)
		z.Write(b)
		z.Close()
	}
	f.Add(members.Bytes())
	f.Add(members.Bytes()[:members.Len()-3])
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64<<10 {
			t.Skip()
		}
		s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC, MaxRecordBytes: 64 << 10, MaxQueryRecords: 32})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		writeGzipTestBlock(t, filepath.Join(s.config.Directory, "20261013/xlog/data/xlog.0"), data)
		b, err := s.readZip("20261013", 0)
		if err == nil && len(b) > 64<<10 {
			t.Fatal("unbounded output")
		}
		// The same bytes as a raw xlog.N file (member framing itself fuzzed):
		// decoding never panics, stays bounded and accounts for every byte.
		blk, err := DecodeZipBlock(context.Background(), bytes.NewReader(data), int64(len(data)))
		if err == nil && (blk.End+blk.Torn != int64(len(data)) || len(blk.Data) > zipBlockBytes || (blk.Torn > 0) != (blk.End < int64(len(data)))) {
			t.Fatalf("decode accounting end %d torn %d size %d", blk.End, blk.Torn, len(data))
		}
		if err != nil && !errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrLimit) {
			t.Fatalf("unclassified decode error %v", err)
		}
	})
}

func TestQueryContextCancellationWhileWaitingForStore(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC).UnixMilli()
	calls := map[string]func(context.Context) error{
		"xlog": func(ctx context.Context) error { _, err := s.XLogContext(ctx, "20261013", 1); return err },
		"range": func(ctx context.Context) error {
			_, err := s.XLogsContext(ctx, "20261013", start, start+500, false)
			return err
		},
		"v2": func(ctx context.Context) error {
			_, err := s.XLogsV2Context(ctx, "20261013", start, start+500, 0)
			return err
		},
		"profile": func(ctx context.Context) error { _, err := s.ProfileChunksContext(ctx, "20261013", 1); return err },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			s.mu.Lock()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- call(ctx) }()
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("cancelled query: %v", err)
				}
			case <-time.After(time.Second):
				t.Error("cancelled query blocked behind store mutex")
			}
			s.mu.Unlock()
		})
	}
}
func TestCompressedReaderContextCancellation(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = s.readZipContext(ctx, "20261013", 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestCompressedDecodeHonorsDeadline(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// A valid large Java block forces decompression to span several read calls.
	block := make([]byte, zipBlockBytes)
	block[7] = 1
	block[8] = 42
	writeGzipTestBlock(t, filepath.Join(s.config.Directory, "20261013/xlog/data/xlog.0"), block)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err = s.readZipContext(ctx, "20261013", 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("decompression ignored deadline: %v", err)
	}
}
