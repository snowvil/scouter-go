package legacyindex

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A RelinkCheck error refuses the whole relink: the file is left untouched and
// RecoveryInfo reports the reason; a passing check relinks as before.
func TestOpenCheckedRelinkCheck(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idx")
	idx, err := Open(path, 1<<10, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = idx.Put([]byte("a"), []byte{1}); err != nil {
		t.Fatal(err)
	}
	if err = idx.Sync(); err != nil {
		t.Fatal(err)
	}
	header, err := os.ReadFile(path + ".hfile")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"b", "c"} {
		if _, err = idx.Put([]byte(k), []byte{2}); err != nil {
			t.Fatal(err)
		}
	}
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path+".hfile", header, 0600); err != nil { // header flush lost
		t.Fatal(err)
	}
	keys, _ := os.ReadFile(path + ".kfile")
	fresh := filepath.Join(dir, "fresh") // same crash state for the passing check
	copyPair(t, path, fresh)
	refuse := func(key, value []byte) error {
		if string(key) == "c" {
			return errors.New("data missing")
		}
		return nil
	}
	idx, err = OpenChecked(path, 1<<10, false, nil, refuse)
	if err != nil {
		t.Fatal(err)
	}
	info := idx.Recovered()
	b, _ := idx.Get([]byte("b"))
	if info.Recovered() || info.RelinkRefused == "" || info.Unreachable != 2 || b != nil {
		t.Fatal(info, b)
	}
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}
	if after, _ := os.ReadFile(path + ".kfile"); !onlyTombstones(keys, after) || string(after) == string(keys) {
		t.Fatal("refused relink must only tombstone the orphans")
	}
	if after, _ := os.ReadFile(path + ".hfile"); string(after) != string(header) {
		t.Fatal("refused relink changed the header")
	}
	idx, err = OpenChecked(fresh, 1<<10, false, nil, func([]byte, []byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	if info = idx.Recovered(); info.RelinkedRecords != 2 {
		t.Fatal(info)
	}
}

// A refused relink is persisted: the orphans are marked deleted, so a later
// open (when the check would pass) never relinks them, and the analysis
// preview reports the same decision as the writable open.
func TestRefusedRelinkIsTombstoned(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idx")
	idx, err := Open(path, 1<<10, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = idx.Put([]byte("a"), []byte{1}); err != nil {
		t.Fatal(err)
	}
	if err = idx.Sync(); err != nil {
		t.Fatal(err)
	}
	header, _ := os.ReadFile(path + ".hfile")
	for _, k := range []string{"b", "c"} {
		if _, err = idx.Put([]byte(k), []byte{2}); err != nil {
			t.Fatal(err)
		}
	}
	idx.Close()
	if err = os.WriteFile(path+".hfile", header, 0600); err != nil {
		t.Fatal(err)
	}
	refuse := func([]byte, []byte) error { return errors.New("data missing") }
	preview, err := AnalyzeRecoveryChecked(path, false, nil, refuse)
	if err != nil || preview.Tombstoned != 2 || preview.Applied {
		t.Fatal(preview, err)
	}
	idx, err = OpenChecked(path, 1<<10, false, nil, refuse)
	if err != nil {
		t.Fatal(err)
	}
	info := idx.Recovered()
	if info.Tombstoned != 2 || !info.Applied || info.RelinkRefused == "" {
		t.Fatal(info)
	}
	idx.Close()
	// The data "returns" (file grows over the old offsets): the check now
	// passes, but the tombstoned records must stay unreachable.
	idx, err = OpenChecked(path, 1<<10, false, nil, func([]byte, []byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	info = idx.Recovered()
	b, _ := idx.Get([]byte("b"))
	if info.Recovered() || info.RelinkRefused != "" || info.Unreachable != 0 || b != nil {
		t.Fatal(info, b)
	}
	if _, err = idx.Put([]byte("d"), []byte{3}); err != nil { // normal writes continue
		t.Fatal(err)
	}
	if d, _ := idx.Get([]byte("d")); len(d) != 1 {
		t.Fatal(d)
	}
}

// onlyTombstones reports whether after differs from before only by deleted
// flags raised to the tombstone value (what a refused relink writes) and nothing else.
func onlyTombstones(before, after []byte) bool {
	if len(before) != len(after) {
		return false
	}
	for i := range before {
		if before[i] != after[i] && !(before[i] <= 1 && after[i] == tombstoneFlag) {
			return false
		}
	}
	return true
}
