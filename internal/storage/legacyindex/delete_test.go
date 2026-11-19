package legacyindex

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDeleteMarksEveryLiveMatchLikeJava(t *testing.T) {
	path := filepath.Join(t.TempDir(), "obj")
	idx, err := Open(path, 5, false, nil) // one bucket: every key shares the chain
	if err != nil {
		t.Fatal(err)
	}
	a, b := []byte{0, 0, 0, 1}, []byte{0, 0, 0, 2}
	posA1, err := idx.Put(a, []byte("a1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = idx.Put(b, []byte("b1")); err != nil {
		t.Fatal(err)
	}
	posA2, err := idx.Put(a, []byte("a2"))
	if err != nil {
		t.Fatal(err)
	}
	countBefore, err := idx.Count()
	if err != nil {
		t.Fatal(err)
	}
	if n, err := idx.Delete(a); err != nil || n != 2 {
		t.Fatalf("delete: %d %v", n, err)
	}
	if n, err := idx.Delete(a); err != nil || n != 0 {
		t.Fatalf("second delete: %d %v", n, err)
	}
	if n, err := idx.Delete([]byte{0, 0, 0, 9}); err != nil || n != 0 {
		t.Fatalf("absent key: %d %v", n, err)
	}
	check := func(idx *Index) {
		t.Helper()
		if v, err := idx.Get(a); err != nil || v != nil {
			t.Fatalf("deleted key visible: %q %v", v, err)
		}
		if all, err := idx.GetAll(a); err != nil || len(all) != 0 {
			t.Fatalf("deleted chain members visible: %q %v", all, err)
		}
		if v, err := idx.Get(b); err != nil || string(v) != "b1" {
			t.Fatalf("other key: %q %v", v, err)
		}
		records, err := idx.Records()
		if err != nil || len(records) != 1 || string(records[0].Value) != "b1" {
			t.Fatalf("scan: %v %v", records, err)
		}
		if count, err := idx.Count(); err != nil || count != countBefore {
			t.Fatalf("header count changed by delete: %d != %d %v", count, countBefore, err)
		}
	}
	check(idx)
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}
	// On disk: Java's writeBoolean(true) byte at each matching record, nothing else moved.
	raw, err := os.ReadFile(path + ".kfile")
	if err != nil {
		t.Fatal(err)
	}
	if raw[posA1] != 1 || raw[posA2] != 1 {
		t.Fatalf("deleted flags: %d %d", raw[posA1], raw[posA2])
	}
	if raw[posA2+1+5+2+4] != 2 || string(raw[posA2+1+5+2+4+1:posA2+1+5+2+4+3]) != "a2" {
		t.Fatal("record body changed")
	}
	idx, err = Open(path, 5, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	check(idx)
	// A later put of the same key is a new live chain head.
	if _, err = idx.Put(a, []byte("a3")); err != nil {
		t.Fatal(err)
	}
	if v, err := idx.Get(a); err != nil || string(v) != "a3" {
		t.Fatalf("re-added key: %q %v", v, err)
	}
	if all, err := idx.GetAll(a); err != nil || len(all) != 1 {
		t.Fatalf("re-added chain: %q %v", all, err)
	}
}

func TestDeleteRejectsReadOnlyAndClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "obj")
	idx, err := Open(path, 1024, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = idx.Put([]byte{1}, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = idx.Delete([]byte{1}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	ro, err := OpenReadOnly(path, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if _, err = ro.Delete([]byte{1}); !errors.Is(err, ErrReadOnly) {
		t.Fatal(err)
	}
	if _, err = ro.Delete(nil); err == nil {
		t.Fatal("nil key accepted")
	}
}
