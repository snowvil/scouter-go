package migrate

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

// Same synthetic instant as tools/java-storage: 2026-01-01 00:00:00.125 Asia/Seoul.
const baseTime int64 = 1767193200125
const testDate = "20260101"

func seoul(t testing.TB) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

func mapValue() protocol.Value {
	return protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "k", Value: protocol.Value{Type: protocol.ValueText, Text: "v"}}}}
}

// buildDB writes a small synthetic DB with the Go storage writer.
func buildDB(t testing.TB, dir string, zip bool) {
	t.Helper()
	ctx := context.Background()
	s, err := storage.Open(storage.Config{Directory: dir, Location: seoul(t), CompressXLog: zip, CompressProfile: zip})
	if err != nil {
		t.Fatal(err)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(s.PutObject(testDate, &protocol.ObjectPack{ObjHash: 42, ObjType: "java", ObjName: "/synthetic/app", Alive: true, Wakeup: baseTime, Tags: mapValue()}))
	must(s.PutText(testDate, &protocol.TextPack{XType: "service", Hash: 7, Text: "서비스"}))
	must(s.PutText(testDate, &protocol.TextPack{XType: "error", Hash: 8, Text: "오류"}))
	for _, x := range []struct{ tx, gx, offset int64 }{{101, 9001, 200}, {102, 9001, 100}, {103, 0, 550}} {
		must(s.PutXLog(&protocol.XLogPack{TxID: x.tx, GxID: x.gx, EndTime: baseTime + x.offset, ObjHash: 42, Service: 7, Elapsed: 10}))
	}
	must(s.PutProfile(&protocol.ProfilePack{Time: baseTime, TxID: 101, Profile: []byte{1, 2}}))
	must(s.PutProfile(&protocol.ProfilePack{Time: baseTime, TxID: 101, Profile: []byte{3, 4}}))
	must(s.PutStatusContext(ctx, &protocol.StatusPack{Time: baseTime, ObjHash: 42, Key: "status", Data: mapValue()}))
	must(s.PutAlertContext(ctx, &protocol.AlertPack{Time: baseTime + 1, ObjHash: 42, Title: "alert", Tags: mapValue()}))
	if _, err = s.PutKeyValueContext(ctx, "space", "a", "alpha", -1, 100); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PutKeyValueContext(ctx, "space", "b", "beta", 10, 100); err != nil {
		t.Fatal(err)
	}
	must(s.Close())
	must(os.MkdirAll(filepath.Join(dir, "notes"), 0700))
	must(os.WriteFile(filepath.Join(dir, "notes", "readme.txt"), []byte("operator note"), 0600))
}

type nodeState struct {
	sum   string
	size  int64
	mtime time.Time
	mode  fs.FileMode
}

// capture records content, size, mode and mtime of every file and directory.
func capture(t testing.TB, root string) map[string]nodeState {
	t.Helper()
	out := map[string]nodeState{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		st := nodeState{size: info.Size(), mtime: info.ModTime(), mode: info.Mode()}
		if info.Mode().IsRegular() {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			h := sha256.Sum256(b)
			st.sum = hex.EncodeToString(h[:])
		}
		if info.IsDir() {
			st.size = 0 // directory sizes are filesystem-specific
		}
		out[path] = st
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func assertUnchanged(t testing.TB, before map[string]nodeState, root string) {
	t.Helper()
	after := capture(t, root)
	for p, want := range before {
		got, ok := after[p]
		if !ok {
			t.Fatalf("%s removed", p)
		}
		if got.sum != want.sum || got.size != want.size || !got.mtime.Equal(want.mtime) || got.mode != want.mode {
			t.Fatalf("%s changed: %+v -> %+v", p, want, got)
		}
	}
	for p := range after {
		if _, ok := before[p]; !ok {
			t.Fatalf("%s created", p)
		}
	}
}

func hasProblem(r *Report, severity, code string) bool {
	for _, p := range r.Problems {
		if p.Severity == severity && p.Code == code {
			return true
		}
	}
	return false
}

func patchFile(t testing.TB, path string, at int64, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err = f.WriteAt(b, at); err != nil {
		t.Fatal(err)
	}
}
func truncateBy(t testing.TB, path string, n int64) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Truncate(path, st.Size()-n); err != nil {
		t.Fatal(err)
	}
}
func appendFile(t testing.TB, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err = f.Write(b); err != nil {
		t.Fatal(err)
	}
}
func long5(v int64) []byte {
	return []byte{byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

// recordPositions lists physical record positions of an index.
func recordPositions(t testing.TB, base string) []int64 {
	t.Helper()
	idx, err := legacyindex.OpenReadOnly(base, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	var out []int64
	if err = idx.Scan(func(r legacyindex.Record) error { out = append(out, r.Position); return nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

// rewriteZip decompresses a GZipStore block, mutates it and recompresses it.
func rewriteZip(t testing.TB, path string, mutate func([]byte) []byte) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(z)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	b = mutate(b)
	var out bytes.Buffer
	w := gzip.NewWriter(&out)
	w.Write(b)
	w.Close()
	if err = os.WriteFile(path, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
}
func zipRecordLen(b []byte, at int) int {
	return int(int8(b[at+5]))<<16 | int(b[at+6])<<8 | int(b[at+7])
}
func setCount(t testing.TB, path string, v int64) {
	t.Helper()
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	if err := os.WriteFile(path, b[:], 0600); err != nil {
		t.Fatal(err)
	}
}
func readCount(t testing.TB, path string) int64 {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return int64(binary.BigEndian.Uint64(b))
}
