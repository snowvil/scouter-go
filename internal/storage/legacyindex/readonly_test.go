package legacyindex

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fileState struct {
	data  []byte
	mtime time.Time
}

func snapshot(t *testing.T, paths ...string) map[string]fileState {
	t.Helper()
	out := map[string]fileState{}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		out[p] = fileState{b, st.ModTime()}
	}
	return out
}
func sameSnapshot(t *testing.T, before map[string]fileState) {
	t.Helper()
	for p, want := range before {
		got := snapshot(t, p)[p]
		if !bytes.Equal(got.data, want.data) || !got.mtime.Equal(want.mtime) {
			t.Fatalf("%s changed by read-only handle", p)
		}
	}
}

func TestOpenReadOnlyNeverWrites(t *testing.T) {
	loc := time.FixedZone("KST", 9*3600)
	dir := t.TempDir()
	path := filepath.Join(dir, "tim")
	idx, err := Open(path, 0, true, loc)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 125e6, loc).UnixMilli()
	mustPut(t, idx, timeKey(base), []byte{1, 2, 3, 4, 5})
	mustPut(t, idx, timeKey(base), []byte{1, 2, 3, 4, 6})
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}
	// Make files read-only on disk too: any O_RDWR open would fail.
	for _, s := range []string{".kfile", ".hfile"} {
		if err = os.Chmod(path+s, 0400); err != nil {
			t.Fatal(err)
		}
	}
	before := snapshot(t, path+".kfile", path+".hfile")
	ro, err := OpenReadOnly(path, true, loc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ro.Put(timeKey(base), []byte{1}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Put %v", err)
	}
	if _, err = ro.Update(timeKey(base), []byte{9, 9, 9, 9, 9}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Update %v", err)
	}
	if err = ro.Sync(); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Sync %v", err)
	}
	records, err := ro.Range(base, base, false)
	if err != nil || len(records) != 2 {
		t.Fatalf("range %v %v", records, err)
	}
	st, err := ro.Inspect(context.Background())
	if err != nil || st.Records != 2 || st.OccupiedBuckets != 1 || st.ReachableRecords != 2 || st.ExpectedCount != 3 || int64(st.HeaderCount) != st.ExpectedCount {
		t.Fatalf("stats %+v %v", st, err)
	}
	if err = ro.Close(); err != nil {
		t.Fatal(err)
	}
	sameSnapshot(t, before)
	if _, err = OpenReadOnly(filepath.Join(dir, "absent"), false, nil); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("absent %v", err)
	}
	if _, err = os.Stat(filepath.Join(dir, "absent.kfile")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("read-only open created a file")
	}
}

func TestTTLReadOnlyNeverWrites(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "kv")
	idx, err := OpenTTL(path, 5*7)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct {
		key string
		exp int64
	}{{"live", 200}, {"old", 50}, {"gone", 300}} {
		if _, err = idx.PutContext(ctx, []byte(v.key), []byte{0, 0, 0, 0, 1}, v.exp); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = idx.DeleteContext(ctx, []byte("gone")); err != nil {
		t.Fatal(err)
	}
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, path+".k2file", path+".hfile")
	ro, err := OpenTTLReadOnly(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	for name, err := range map[string]error{
		"put":    func() error { _, e := ro.PutContext(ctx, []byte("x"), []byte{1}, 1); return e }(),
		"update": func() error { _, e := ro.UpdateOrPutContext(ctx, []byte("live"), []byte{0, 0, 0, 0, 2}, 1); return e }(),
		"delete": func() error { _, e := ro.DeleteContext(ctx, []byte("live")); return e }(),
		"ttl":    func() error { _, e := ro.SetTTLContext(ctx, []byte("live"), 0, 1); return e }(),
		"sync":   ro.Sync(),
	} {
		if !errors.Is(err, ErrReadOnly) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	st, err := ro.Inspect(ctx, 100)
	if err != nil || st.Records != 3 || st.Live != 1 || st.Expired != 1 || st.Deleted != 1 {
		t.Fatalf("ttl stats %+v %v", st, err)
	}
	var n int
	if err = ro.ScanContext(ctx, func(r TTLRecord) error { n++; return nil }); err != nil || n != 3 {
		t.Fatalf("scan %d %v", n, err)
	}
	if v, err := ro.GetContext(ctx, []byte("live"), 100); err != nil || v == nil {
		t.Fatalf("get %v %v", v, err)
	}
	if err = ro.Close(); err != nil {
		t.Fatal(err)
	}
	sameSnapshot(t, before)
}

func TestCorruptErrorCodes(t *testing.T) {
	build := func(t *testing.T) string {
		path := filepath.Join(t.TempDir(), "idx")
		idx, err := Open(path, 64, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		mustPut(t, idx, []byte("a"), []byte{1, 2, 3, 4, 5})
		mustPut(t, idx, []byte("b"), []byte{1, 2, 3, 4, 6})
		if err = idx.Close(); err != nil {
			t.Fatal(err)
		}
		return path
	}
	cases := []struct {
		name, code string
		mutate     func(t *testing.T, path string)
	}{
		{"magic", CodeMagic, func(t *testing.T, path string) { patch(t, path+".kfile", 0, []byte{0}) }},
		{"truncated", CodeTruncated, func(t *testing.T, path string) { truncateBy(t, path+".kfile", 2) }},
		{"cycle", CodeCycle, func(t *testing.T, path string) {
			// Second record starts after the 15-byte first record; point prev to itself.
			patch(t, path+".kfile", 2+15+1, []byte{0, 0, 0, 0, 17})
		}},
		{"missing pair", CodeMissingPair, func(t *testing.T, path string) { os.Remove(path + ".hfile") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := build(t)
			c.mutate(t, path)
			_, err := OpenReadOnly(path, false, nil)
			var ce *CorruptError
			if !errors.As(err, &ce) || ce.Code != c.code || !errors.Is(err, ErrCorrupt) {
				t.Fatalf("got %v (%+v), want code %s", err, ce, c.code)
			}
		})
	}
}

func patch(t *testing.T, path string, at int64, b []byte) {
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
func truncateBy(t *testing.T, path string, n int64) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Truncate(path, st.Size()-n); err != nil {
		t.Fatal(err)
	}
}
