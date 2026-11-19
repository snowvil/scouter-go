package legacyindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type indexKind struct {
	name    string
	timed   bool
	ttl     bool
	keyFile string
}

var recoveryKinds = []indexKind{{"hash", false, false, ".kfile"}, {"time", true, false, ".kfile"}, {"ttl", false, true, ".k2file"}}

var recoveryLoc = time.FixedZone("KST", 9*3600)
var recoveryBase = time.Date(2026, 1, 1, 0, 0, 0, 125e6, recoveryLoc).UnixMilli()

// writer abstracts the three index kinds for the crash scenarios.
type writer struct {
	t    *testing.T
	kind indexKind
	idx  *Index
	ttl  *TTLIndex
}

func openWriter(t *testing.T, kind indexKind, path string) *writer {
	t.Helper()
	w := &writer{t: t, kind: kind}
	var err error
	switch {
	case kind.ttl:
		w.ttl, err = OpenTTL(path, 5*7)
	case kind.timed:
		w.idx, err = Open(path, 0, true, recoveryLoc)
	default:
		w.idx, err = Open(path, 5*7, false, nil) // 7 buckets: many collisions
	}
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func (w *writer) key(n int) []byte {
	if w.kind.timed {
		return timeKey(recoveryBase + int64(n%13)*500 + int64(n%3))
	}
	return []byte(fmt.Sprintf("key-%d", n%23))
}
func (w *writer) put(n int) {
	w.t.Helper()
	value := []byte{0, 0, 0, byte(n >> 8), byte(n)}
	var err error
	if w.ttl != nil {
		_, err = w.ttl.PutContext(context.Background(), w.key(n), value, TTLNoExpiry)
	} else {
		_, err = w.idx.Put(w.key(n), value)
	}
	if err != nil {
		w.t.Fatal(err)
	}
}
func (w *writer) sync() error {
	if w.ttl != nil {
		return w.ttl.Sync()
	}
	return w.idx.Sync()
}
func (w *writer) close() error {
	if w.ttl != nil {
		return w.ttl.Close()
	}
	return w.idx.Close()
}
func (w *writer) recovered() RecoveryInfo {
	if w.ttl != nil {
		return w.ttl.Recovered()
	}
	return w.idx.Recovered()
}
func reopen(kind indexKind, path string) (*writer, error) {
	w := &writer{kind: kind}
	var err error
	switch {
	case kind.ttl:
		w.ttl, err = OpenTTL(path, 5*7)
	case kind.timed:
		w.idx, err = Open(path, 0, true, recoveryLoc)
	default:
		w.idx, err = Open(path, 5*7, false, nil)
	}
	return w, err
}
func analyze(kind indexKind, path string) (RecoveryInfo, error) {
	if kind.ttl {
		return AnalyzeTTLRecovery(context.Background(), path)
	}
	return AnalyzeRecovery(path, kind.timed, recoveryLoc)
}
func openRO(kind indexKind, path string) error {
	var err error
	if kind.ttl {
		var idx *TTLIndex
		if idx, err = OpenTTLReadOnly(context.Background(), path); err == nil {
			idx.Close()
		}
	} else {
		var idx *Index
		if idx, err = OpenReadOnly(path, kind.timed, recoveryLoc); err == nil {
			idx.Close()
		}
	}
	return err
}

// newest returns the latest live value of every written key by direct lookup.
func (w *writer) lookup(t *testing.T, n int) []byte {
	t.Helper()
	var v []byte
	var err error
	if w.ttl != nil {
		v, err = w.ttl.GetContext(context.Background(), w.key(n), 0)
	} else {
		v, err = w.idx.Get(w.key(n))
	}
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// crashCopy builds the state after a process crash: the key file holds every
// append (page cache survives) while the .hfile is the last synced snapshot.
// clean is the same history closed normally, as the reference.
func crashCopy(t *testing.T, kind indexKind, synced, unsynced int) (crash, clean string) {
	t.Helper()
	dir := t.TempDir()
	clean = filepath.Join(dir, "clean")
	w := openWriter(t, kind, clean)
	for n := 0; n < synced; n++ {
		w.put(n)
	}
	if err := w.sync(); err != nil {
		t.Fatal(err)
	}
	staleHead, err := os.ReadFile(clean + ".hfile")
	if err != nil {
		t.Fatal(err)
	}
	for n := synced; n < synced+unsynced; n++ {
		w.put(n)
	}
	keys, err := os.ReadFile(clean + kind.keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.close(); err != nil {
		t.Fatal(err)
	}
	crash = filepath.Join(dir, "crash")
	if err = os.WriteFile(crash+kind.keyFile, keys, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(crash+".hfile", staleHead, 0600); err != nil {
		t.Fatal(err)
	}
	return crash, clean
}

func fileBytes(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRecoverStaleHeader(t *testing.T) {
	for _, kind := range recoveryKinds {
		for _, c := range []struct{ synced, unsynced int }{{0, 40}, {30, 1}, {30, 50}} {
			t.Run(fmt.Sprintf("%s/%d+%d", kind.name, c.synced, c.unsynced), func(t *testing.T) {
				crash, clean := crashCopy(t, kind, c.synced, c.unsynced)
				staleKeys, staleHead := fileBytes(t, crash+kind.keyFile), fileBytes(t, crash+".hfile")
				// Read-only paths see the same files and never change them.
				if err := openRO(kind, crash); err != nil {
					t.Fatalf("read-only open of a stale header must stay valid: %v", err)
				}
				planned, err := analyze(kind, crash)
				if err != nil || planned.RelinkedRecords != int64(c.unsynced) || planned.Applied {
					t.Fatalf("analyze %+v %v", planned, err)
				}
				if !bytes.Equal(staleKeys, fileBytes(t, crash+kind.keyFile)) || !bytes.Equal(staleHead, fileBytes(t, crash+".hfile")) {
					t.Fatal("read-only/analyze modified files")
				}
				w, err := reopen(kind, crash)
				if err != nil {
					t.Fatal(err)
				}
				info := w.recovered()
				if !info.Applied || info.RelinkedRecords != int64(c.unsynced) || info.RelinkedBuckets == 0 || info.TruncatedTailBytes != 0 {
					t.Fatalf("recovery %+v", info)
				}
				ref, err := reopen(kind, clean)
				if err != nil {
					t.Fatal(err)
				}
				for n := 0; n < c.synced+c.unsynced; n++ {
					if got, want := w.lookup(t, n), ref.lookup(t, n); !bytes.Equal(got, want) || got == nil {
						t.Fatalf("key %d: recovered %x clean %x", n, got, want)
					}
				}
				ref.close()
				if err = w.close(); err != nil {
					t.Fatal(err)
				}
				// The persisted header equals a normal close of the same history,
				// including the Java count increments.
				if !bytes.Equal(fileBytes(t, crash+".hfile"), fileBytes(t, clean+".hfile")) {
					t.Fatal("recovered header differs from the clean header")
				}
				if info.CountAfter != int32(binary.BigEndian.Uint32(fileBytes(t, clean+".hfile")[4:8])) {
					t.Fatalf("count %+v", info)
				}
				again, err := reopen(kind, crash)
				if err != nil || again.recovered().Recovered() {
					t.Fatalf("second open recovered again %+v %v", again.recovered(), err)
				}
				again.close()
			})
		}
	}
}

// tornRecord returns the first n bytes of a valid record for kind.
func tornRecord(kind indexKind, prev int64, n int) []byte {
	key := []byte("torn")
	if kind.timed {
		key = timeKey(recoveryBase)
	}
	var rec []byte
	if kind.ttl {
		rec = ttlRecord(key, []byte{1, 2, 3, 4, 5}, prev, TTLNoExpiry)
	} else {
		rec = make([]byte, 8+len(key)+1+5)
		writeLong5(rec[1:6], prev)
		binary.BigEndian.PutUint16(rec[6:8], uint16(len(key)))
		copy(rec[8:], key)
		rec[8+len(key)] = 5
	}
	return rec[:n]
}

func TestRecoverTornTail(t *testing.T) {
	for _, kind := range recoveryKinds {
		for _, cut := range []int{1, 7, 14} {
			t.Run(fmt.Sprintf("%s/%d", kind.name, cut), func(t *testing.T) {
				crash, clean := crashCopy(t, kind, 10, 5)
				keys := fileBytes(t, crash+kind.keyFile)
				validLen := int64(len(keys))
				f, err := os.OpenFile(crash+kind.keyFile, os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					t.Fatal(err)
				}
				f.Write(tornRecord(kind, 0, cut))
				f.Close()
				before := fileBytes(t, crash+kind.keyFile)
				// Read-only open reports the torn tail as before; analyze plans the cut.
				var ce *CorruptError
				if err = openRO(kind, crash); !errors.As(err, &ce) || ce.Code != CodeTruncated {
					t.Fatalf("read-only torn tail %v", err)
				}
				planned, err := analyze(kind, crash)
				if err != nil || planned.TruncatedTailBytes != int64(cut) || planned.TruncatedAt != validLen || planned.RelinkedRecords != 5 {
					t.Fatalf("analyze %+v %v", planned, err)
				}
				if !bytes.Equal(before, fileBytes(t, crash+kind.keyFile)) {
					t.Fatal("analyze truncated")
				}
				w, err := reopen(kind, crash)
				if err != nil {
					t.Fatal(err)
				}
				w.t = t
				info := w.recovered()
				if info.TruncatedTailBytes != int64(cut) || info.RelinkedRecords != 5 {
					t.Fatalf("recovery %+v", info)
				}
				if st, _ := os.Stat(crash + kind.keyFile); st.Size() != validLen {
					t.Fatalf("not truncated to %d: %d", validLen, st.Size())
				}
				w.put(99) // the next append lands on the record boundary
				if err = w.close(); err != nil {
					t.Fatal(err)
				}
				again, err := reopen(kind, crash)
				if err != nil || again.recovered().Recovered() || again.lookup(t, 99) == nil {
					t.Fatalf("reopen after torn recovery %+v %v", again.recovered(), err)
				}
				again.close()
				_ = clean
			})
		}
	}
}

func TestRecoverRefusesInconsistentFiles(t *testing.T) {
	for _, kind := range recoveryKinds {
		cases := []struct {
			name, code string // code "" accepts truncated or cycle
			mutate     func(t *testing.T, crash string, positions []int64)
		}{
			{"torn tail referenced by a head", CodeTruncated, func(t *testing.T, crash string, positions []int64) {
				st, _ := os.Stat(crash + kind.keyFile)
				f, _ := os.OpenFile(crash+kind.keyFile, os.O_WRONLY|os.O_APPEND, 0)
				f.Write(tornRecord(kind, 0, 7))
				f.Close()
				head := make([]byte, 5)
				writeLong5(head, st.Size())
				patch(t, crash+".hfile", headerBytes, head)
			}},
			{"torn tail with invalid flag", CodeTruncated, func(t *testing.T, crash string, positions []int64) {
				rec := tornRecord(kind, 0, 7)
				rec[0] = 7
				f, _ := os.OpenFile(crash+kind.keyFile, os.O_WRONLY|os.O_APPEND, 0)
				f.Write(rec)
				f.Close()
			}},
			{"torn tail with forward link", "", func(t *testing.T, crash string, positions []int64) {
				f, _ := os.OpenFile(crash+kind.keyFile, os.O_WRONLY|os.O_APPEND, 0)
				f.Write(tornRecord(kind, 1<<30, 13))
				f.Close()
			}},
		}
		for _, c := range cases {
			t.Run(kind.name+"/"+c.name, func(t *testing.T) {
				crash, _ := crashCopy(t, kind, 10, 30)
				var positions []int64
				if kind.ttl {
					ro, err := OpenTTLReadOnly(context.Background(), crash)
					if err != nil {
						t.Fatal(err)
					}
					ro.ScanContext(context.Background(), func(r TTLRecord) error { positions = append(positions, r.Position); return nil })
					ro.Close()
				} else {
					ro, err := OpenReadOnly(crash, kind.timed, recoveryLoc)
					if err != nil {
						t.Fatal(err)
					}
					ro.ScanAllContext(context.Background(), func(r Record) error { positions = append(positions, r.Position); return nil })
					ro.Close()
				}
				c.mutate(t, crash, positions)
				keys, head := fileBytes(t, crash+kind.keyFile), fileBytes(t, crash+".hfile")
				_, err := reopen(kind, crash)
				var ce *CorruptError
				if !errors.As(err, &ce) || (c.code != "" && ce.Code != c.code) || (c.code == "" && ce.Code != CodeTruncated && ce.Code != CodeCycle) {
					t.Fatalf("want %s, got %v", c.code, err)
				}
				if _, aerr := analyze(kind, crash); !errors.As(aerr, &ce) {
					t.Fatalf("analyze accepted inconsistent files: %v", aerr)
				}
				if !bytes.Equal(keys, fileBytes(t, crash+kind.keyFile)) || !bytes.Equal(head, fileBytes(t, crash+".hfile")) {
					t.Fatal("refused recovery modified files")
				}
			})
		}
	}
}

func TestRecoverMidFileGarbageIsNotTruncated(t *testing.T) {
	kind := recoveryKinds[0]
	crash, _ := crashCopy(t, kind, 0, 200)
	keys := fileBytes(t, crash+kind.keyFile)
	// Damage the key length of an early record: the remaining file would parse
	// as one oversized record. The head of later buckets points beyond it.
	binary.BigEndian.PutUint16(keys[2+6:], 0xffff)
	if err := os.WriteFile(crash+kind.keyFile, keys, 0600); err != nil {
		t.Fatal(err)
	}
	// Restore a header that references later records so the region is "referenced".
	_, clean := crashCopy(t, kind, 200, 0)
	if err := os.WriteFile(crash+".hfile", fileBytes(t, clean+".hfile"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := reopen(kind, crash); err == nil {
		t.Fatal("mid-file damage truncated as a torn tail")
	}
	if !bytes.Equal(keys, fileBytes(t, crash+kind.keyFile)) {
		t.Fatal("key file modified")
	}
}

// doubleCrash reproduces a Java restart from a stale header followed by a
// second crash before its first flush: orphan A continues saved head H, then
// the restarted writer appends B with prev=H again (a fork).
func doubleCrash(t *testing.T, kind indexKind) (path string, forkKey []byte) {
	t.Helper()
	crash, _ := crashCopy(t, kind, 10, 30) // 30 orphans after the saved header
	w := &writer{t: t, kind: kind}
	key := w.key(39) // the newest orphan's key; its bucket has a saved head
	var bucket int
	var err error
	if kind.ttl {
		ro, e := OpenTTLReadOnly(context.Background(), crash)
		if e != nil {
			t.Fatal(e)
		}
		bucket, err = ro.index.bucket(key)
		ro.Close()
	} else {
		ro, e := OpenReadOnly(crash, kind.timed, recoveryLoc)
		if e != nil {
			t.Fatal(e)
		}
		bucket, err = ro.bucket(key)
		ro.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	head := readLong5(fileBytes(t, crash+".hfile")[headerBytes+bucket*5:])
	var rec []byte
	value := []byte{0, 0, 0, 0x7f, 0x7f}
	if kind.ttl {
		rec = ttlRecord(key, value, head, TTLNoExpiry)
	} else {
		rec = make([]byte, 8+len(key)+1+len(value))
		writeLong5(rec[1:6], head)
		binary.BigEndian.PutUint16(rec[6:8], uint16(len(key)))
		copy(rec[8:], key)
		rec[8+len(key)] = byte(len(value))
		copy(rec[9+len(key):], value)
	}
	f, err := os.OpenFile(crash+kind.keyFile, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(rec)
	f.Close()
	return crash, key
}

func TestRecoverRefusedRelinkOpensWithJavaSemantics(t *testing.T) {
	for _, kind := range recoveryKinds {
		t.Run(kind.name, func(t *testing.T) {
			crash, key := doubleCrash(t, kind)
			keys, head := fileBytes(t, crash+kind.keyFile), fileBytes(t, crash+".hfile")
			planned, err := analyze(kind, crash)
			if err != nil || planned.RelinkRefused == "" || planned.Unreachable != 31 || planned.Recovered() {
				t.Fatalf("analyze %+v %v", planned, err)
			}
			w, err := reopen(kind, crash)
			if err != nil {
				t.Fatalf("double-crash files must still open: %v", err)
			}
			w.t = t
			info := w.recovered()
			if info.RelinkRefused == "" || info.Unreachable != 31 || info.RelinkedRecords != 0 || info.Recovered() {
				t.Fatalf("recovery %+v", info)
			}
			// Original semantics: the fork is unreachable, the saved head answers.
			var got []byte
			if w.ttl != nil {
				got, err = w.ttl.GetContext(context.Background(), key, 0)
			} else {
				got, err = w.idx.Get(key)
			}
			if err != nil || bytes.Equal(got, []byte{0, 0, 0, 0x7f, 0x7f}) {
				t.Fatalf("unreachable fork returned %x %v", got, err)
			}
			if err = w.close(); err != nil {
				t.Fatal(err)
			}
			if !onlyTombstones(keys, fileBytes(t, crash+kind.keyFile)) || !bytes.Equal(head, fileBytes(t, crash+".hfile")) {
				t.Fatal("refused relink may only tombstone orphans")
			}
			if err = openRO(kind, crash); err != nil {
				t.Fatalf("read-only open %v", err)
			}
			// A torn tail is still judged separately and truncated.
			f, _ := os.OpenFile(crash+kind.keyFile, os.O_WRONLY|os.O_APPEND, 0)
			f.Write(tornRecord(kind, 0, 7))
			f.Close()
			w, err = reopen(kind, crash)
			if err != nil {
				t.Fatal(err)
			}
			// The earlier refusal tombstoned the fork, so it does not recur;
			// the torn tail is still truncated.
			if info = w.recovered(); info.TruncatedTailBytes != 7 || info.RelinkRefused != "" || info.Unreachable != 0 {
				t.Fatalf("torn tail after a tombstoned fork %+v", info)
			}
			w.close()
			if !onlyTombstones(keys, fileBytes(t, crash+kind.keyFile)) || !bytes.Equal(head, fileBytes(t, crash+".hfile")) {
				t.Fatal("torn-tail truncation changed more than the torn bytes")
			}
		})
	}
}

// A non-EOF read failure is an I/O error, never a torn tail to truncate.
func TestMetaReadErrorIsNotTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "io")
	idx, err := Open(path, 35, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, idx, []byte("a"), []byte{1, 2, 3, 4, 5})
	idx.keys.Close() // reads now fail with os.ErrClosed, not io.EOF
	_, err = idx.meta(2)
	var ce *CorruptError
	if err == nil || errors.As(err, &ce) || errors.Is(err, ErrCorrupt) || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("I/O error classified as corruption: %v", err)
	}
	ttl, err := OpenTTL(filepath.Join(t.TempDir(), "ttl"), 35)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ttl.PutContext(context.Background(), []byte("a"), []byte{1}, 1); err != nil {
		t.Fatal(err)
	}
	ttl.index.keys.Close()
	if _, err = ttl.meta(2); err == nil || errors.As(err, &ce) || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("TTL I/O error classified as corruption: %v", err)
	}
}
