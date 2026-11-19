package legacyindex

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func copyPair(t *testing.T, from, to string) {
	t.Helper()
	for _, ext := range []string{".kfile", ".hfile"} {
		b, err := os.ReadFile(from + ext)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(to+ext, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

// A committed header only references records appended before BeginSync; later
// records keep the index dirty and are reachable again after the next sync.
func TestSyncPointHeaderSnapshotOrdering(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idx")
	idx, err := Open(path, 1<<10, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	if _, err = idx.Put([]byte("a"), []byte{1}); err != nil {
		t.Fatal(err)
	}
	p, err := idx.BeginSync()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = idx.Put([]byte("b"), []byte{2}); err != nil {
		t.Fatal(err)
	}
	if err = p.Commit(); err != nil {
		t.Fatal(err)
	}
	if !idx.dirty {
		t.Fatal("index changed after BeginSync but was marked clean")
	}
	crash := filepath.Join(dir, "crash")
	copyPair(t, path, crash)
	ro, err := OpenReadOnly(crash, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := ro.Get([]byte("a"))
	b, _ := ro.Get([]byte("b"))
	ro.Close()
	if len(a) != 1 || b != nil {
		t.Fatal("saved header must reference only the snapshot", a, b)
	}
	p, err = idx.BeginSync()
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Commit(); err != nil {
		t.Fatal(err)
	}
	if idx.dirty {
		t.Fatal("unchanged index stayed dirty")
	}
	copyPair(t, path, crash)
	ro, err = OpenReadOnly(crash, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if b, _ = ro.Get([]byte("b")); len(b) != 1 {
		t.Fatal("second sync lost b")
	}
	// A clean snapshot commits only the key fsync.
	if p, err = idx.BeginSync(); err != nil || p.pages != nil {
		t.Fatal(p, err)
	}
}

// Commit runs without the index lock: concurrent Put/Get proceed (run -race).
func TestSyncPointConcurrentWriters(t *testing.T) {
	idx, err := Open(filepath.Join(t.TempDir(), "idx"), 1<<10, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			if _, err := idx.Put([]byte{byte(i), byte(i >> 8)}, []byte{1}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			p, err := idx.BeginSync()
			if err == nil {
				err = p.Commit()
			}
			if err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
	if err = idx.Sync(); err != nil || idx.dirty {
		t.Fatal(err, idx.dirty)
	}
}
