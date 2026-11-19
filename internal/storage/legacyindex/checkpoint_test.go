package legacyindex

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func (w *writer) checkpoint() (Checkpoint, bool) {
	if w.ttl != nil {
		return w.ttl.Checkpoint()
	}
	return w.idx.Checkpoint()
}

func (w *writer) openInfo() OpenInfo {
	if w.ttl != nil {
		return w.ttl.OpenInfo()
	}
	return w.idx.OpenInfo()
}

func reopenCP(kind indexKind, path string, cp *Checkpoint) (*writer, error) {
	w := &writer{kind: kind}
	var err error
	switch {
	case kind.ttl:
		w.ttl, err = OpenTTLCheckpointed(context.Background(), path, 5*7, nil, cp)
	case kind.timed:
		w.idx, err = OpenCheckpointed(path, 0, true, recoveryLoc, nil, nil, cp)
	default:
		w.idx, err = OpenCheckpointed(path, 5*7, false, nil, nil, nil, cp)
	}
	return w, err
}

// checkpointCrash writes synced records, takes the checkpoint of Sync, appends
// unsynced records and returns the files a process crash leaves (key file as
// written, header as last synced) plus the checkpoint. extra runs on the
// crash image before it is returned.
func checkpointCrash(t *testing.T, kind indexKind, synced, unsynced int) (string, Checkpoint) {
	t.Helper()
	dir := t.TempDir()
	live := filepath.Join(dir, "live")
	w := openWriter(t, kind, live)
	for n := 0; n < synced; n++ {
		w.put(n)
	}
	if err := w.sync(); err != nil {
		t.Fatal(err)
	}
	cp, ok := w.checkpoint()
	if !ok {
		t.Fatal("Sync established no checkpoint")
	}
	head := fileBytes(t, live+".hfile")
	for n := synced; n < synced+unsynced; n++ {
		w.put(n)
	}
	keys := fileBytes(t, live+kind.keyFile)
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	crash := filepath.Join(dir, "crash")
	if err := os.WriteFile(crash+kind.keyFile, keys, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(crash+".hfile", head, 0600); err != nil {
		t.Fatal(err)
	}
	return crash, cp
}

func copyKindPair(t *testing.T, kind indexKind, from, to string) {
	t.Helper()
	for _, ext := range []string{kind.keyFile, ".hfile"} {
		if err := os.WriteFile(to+ext, fileBytes(t, from+ext), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

type openResult struct {
	err      string
	info     OpenInfo
	recovery RecoveryInfo
	keys     []byte
	head     []byte
	values   [][]byte
}

// openBoth opens one copy of path with cp and another without and closes
// both; the results (error, recovery, files after Close, lookups) must match.
func openBoth(t *testing.T, kind indexKind, path string, cp *Checkpoint, keys int) (withCP, full openResult) {
	t.Helper()
	run := func(name string, cp *Checkpoint) openResult {
		dst := filepath.Join(t.TempDir(), name)
		copyKindPair(t, kind, path, dst)
		w, err := reopenCP(kind, dst, cp)
		if err != nil {
			return openResult{err: err.Error(), keys: fileBytes(t, dst+kind.keyFile), head: fileBytes(t, dst+".hfile")}
		}
		w.t = t
		r := openResult{info: w.openInfo(), recovery: w.recovered()}
		for n := 0; n < keys; n++ {
			r.values = append(r.values, w.lookup(t, n))
		}
		if err = w.close(); err != nil {
			t.Fatal(err)
		}
		r.keys, r.head = fileBytes(t, dst+kind.keyFile), fileBytes(t, dst+".hfile")
		return r
	}
	return run("cp", cp), run("full", nil)
}

func sameOutcome(t *testing.T, a, b openResult) {
	t.Helper()
	if a.err != b.err {
		t.Fatalf("errors differ: %q vs %q", a.err, b.err)
	}
	if a.recovery != b.recovery {
		t.Fatalf("recovery differs:\n%+v\n%+v", a.recovery, b.recovery)
	}
	if !bytes.Equal(a.keys, b.keys) || !bytes.Equal(a.head, b.head) {
		t.Fatal("files differ after open+close")
	}
	if fmt.Sprint(a.values) != fmt.Sprint(b.values) {
		t.Fatal("lookups differ", a.values, b.values)
	}
}

// (a) After a crash, a checkpoint limits the walk to the records appended
// after it; the outcome is the full walk's.
func TestCheckpointTailOnlyAfterCrash(t *testing.T) {
	for _, kind := range recoveryKinds {
		t.Run(kind.name, func(t *testing.T) {
			crash, cp := checkpointCrash(t, kind, 200, 30)
			a, b := openBoth(t, kind, crash, &cp, 23)
			sameOutcome(t, a, b)
			if a.info.Check != CheckTail || a.info.WalkedRecords != 30 || a.info.CheckpointRejected != "" {
				t.Fatalf("checkpointed open: %+v", a.info)
			}
			if b.info.Check != CheckWalked || b.info.WalkedRecords != 230 {
				t.Fatalf("full open: %+v", b.info)
			}
			if a.recovery.RelinkedRecords != 30 {
				t.Fatalf("recovery: %+v", a.recovery)
			}
			// Nothing appended after the checkpoint: nothing walked.
			crash, cp = checkpointCrash(t, kind, 50, 0)
			a, b = openBoth(t, kind, crash, &cp, 23)
			sameOutcome(t, a, b)
			if a.info.Check != CheckTail || a.info.WalkedRecords != 0 || a.info.WalkedBytes != 0 {
				t.Fatalf("checkpointed open without tail: %+v", a.info)
			}
		})
	}
}

// (b) A checkpoint that does not describe the pair is never used.
func TestCheckpointMismatchFallsBackToFullWalk(t *testing.T) {
	for _, kind := range recoveryKinds {
		t.Run(kind.name, func(t *testing.T) {
			cases := map[string]func(path string, cp *Checkpoint){
				"header sum": func(_ string, cp *Checkpoint) { cp.HeaderSum++ },
				"tail crc":   func(_ string, cp *Checkpoint) { cp.TailCRC++ },
				"hash bytes": func(_ string, cp *Checkpoint) { cp.HashBytes += 5 },
				"key bytes past the file": func(path string, cp *Checkpoint) {
					info, err := os.Stat(path + kind.keyFile)
					if err != nil {
						t.Fatal(err)
					}
					cp.KeyBytes = info.Size() + 1
				},
				"key bytes below magic": func(_ string, cp *Checkpoint) { cp.KeyBytes = 1 },
				"key bytes not a boundary": func(_ string, cp *Checkpoint) {
					cp.KeyBytes -= 3 // TailCRC then covers other bytes
				},
				"truncated key file": func(path string, cp *Checkpoint) {
					if err := os.Truncate(path+kind.keyFile, cp.KeyBytes-1); err != nil {
						t.Fatal(err)
					}
				},
				"header rewritten later": func(path string, _ *Checkpoint) {
					w, err := reopen(kind, path)
					if err != nil {
						t.Fatal(err)
					}
					w.t = t
					w.put(999)
					if err = w.close(); err != nil {
						t.Fatal(err)
					}
				},
				"damaged header page": func(path string, _ *Checkpoint) {
					f, err := os.OpenFile(path+".hfile", os.O_RDWR, 0)
					if err != nil {
						t.Fatal(err)
					}
					// An unused count byte: the header stays structurally valid.
					f.WriteAt([]byte{0x5a}, 9)
					f.Close()
				},
				"recreated pair": func(path string, _ *Checkpoint) {
					os.Remove(path + kind.keyFile)
					os.Remove(path + ".hfile")
					w := openWriter(t, kind, path)
					for n := 0; n < 7; n++ {
						w.put(n + 500)
					}
					if err := w.close(); err != nil {
						t.Fatal(err)
					}
				},
			}
			for name, mutate := range cases {
				t.Run(name, func(t *testing.T) {
					crash, cp := checkpointCrash(t, kind, 120, 15)
					mutate(crash, &cp)
					a, b := openBoth(t, kind, crash, &cp, 23)
					sameOutcome(t, a, b)
					if a.err == "" && (a.info.Check != CheckWalked || a.info.CheckpointRejected == "") {
						t.Fatalf("mismatching checkpoint used: %+v", a.info)
					}
				})
			}
		})
	}
}

func appendBytes(t *testing.T, path string, b []byte) {
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

// (c) A damaged tail gets exactly the outcome of the existing recovery rules:
// a torn final record is truncated, garbage inside the tail is an error, a
// fork in the tail is a refused relink (tombstones, Java semantics).
func TestCheckpointDamagedTailMatchesFullWalk(t *testing.T) {
	for _, kind := range recoveryKinds {
		t.Run(kind.name, func(t *testing.T) {
			t.Run("torn tail", func(t *testing.T) {
				crash, cp := checkpointCrash(t, kind, 80, 12)
				appendBytes(t, crash+kind.keyFile, tornRecord(kind, 0, 7))
				a, b := openBoth(t, kind, crash, &cp, 23)
				sameOutcome(t, a, b)
				if a.info.Check != CheckTail || a.recovery.TruncatedTailBytes != 7 || a.recovery.RelinkedRecords != 12 {
					t.Fatalf("%+v %+v", a.info, a.recovery)
				}
			})
			t.Run("garbage inside the tail", func(t *testing.T) {
				crash, cp := checkpointCrash(t, kind, 80, 12)
				keys := fileBytes(t, crash+kind.keyFile)
				// Break the previous link of the first tail record: a link
				// forward is a corrupt chain, not a torn tail.
				prevAt := 1
				if kind.ttl {
					prevAt = 6
				}
				writeLong5(keys[cp.KeyBytes+int64(prevAt):], cp.KeyBytes+1)
				if err := os.WriteFile(crash+kind.keyFile, keys, 0600); err != nil {
					t.Fatal(err)
				}
				a, b := openBoth(t, kind, crash, &cp, 0)
				sameOutcome(t, a, b)
				if a.err == "" || !strings.Contains(a.err, "previous link") {
					t.Fatalf("damaged tail accepted: %q", a.err)
				}
			})
			t.Run("fork in the tail", func(t *testing.T) {
				crash, cp := checkpointCrash(t, kind, 10, 30)
				// A second writer restarted from the checkpoint header and
				// appended a record linking to the saved head again.
				w := &writer{t: t, kind: kind}
				key := w.key(39)
				bucketOf := func() int {
					idx := &Index{header: fileBytes(t, crash+".hfile"), timeIndex: kind.timed}
					if kind.timed {
						idx.base = time.Date(2000, 1, 1, 0, 0, 0, 0, recoveryLoc).UnixMilli()
					}
					b, err := idx.bucket(key)
					if err != nil {
						t.Fatal(err)
					}
					return b
				}
				head := readLong5(fileBytes(t, crash+".hfile")[headerBytes+bucketOf()*5:])
				value := []byte{0, 0, 0, 0x7f, 0x7f}
				var rec []byte
				if kind.ttl {
					rec = ttlRecord(key, value, head, TTLNoExpiry)
				} else {
					rec = make([]byte, 8+len(key)+1+len(value))
					writeLong5(rec[1:6], head)
					rec[6], rec[7] = byte(len(key)>>8), byte(len(key))
					copy(rec[8:], key)
					rec[8+len(key)] = byte(len(value))
					copy(rec[9+len(key):], value)
				}
				appendBytes(t, crash+kind.keyFile, rec)
				a, b := openBoth(t, kind, crash, &cp, 23)
				sameOutcome(t, a, b)
				if a.recovery.RelinkRefused == "" || a.recovery.Tombstoned == 0 {
					t.Fatalf("fork not refused: %+v", a.recovery)
				}
				if a.info.Check != CheckWalked || !strings.Contains(a.info.CheckpointRejected, "tail validation") {
					t.Fatalf("refusal must be decided by the full walk: %+v", a.info)
				}
			})
			t.Run("tombstones before the checkpoint", func(t *testing.T) {
				// A refused relink leaves tombstoned orphans in the prefix;
				// records appended to their buckets after the checkpoint
				// must still be adopted exactly as the full walk adopts them.
				forked, _ := doubleCrash(t, kind)
				w, err := reopen(kind, forked)
				if err != nil {
					t.Fatal(err)
				}
				w.t = t
				if w.recovered().Tombstoned == 0 {
					t.Fatal("expected tombstones")
				}
				if err = w.sync(); err != nil {
					t.Fatal(err)
				}
				cp, ok := w.checkpoint()
				if !ok {
					t.Fatal("no checkpoint")
				}
				head := fileBytes(t, forked+".hfile")
				for n := 40; n < 90; n++ {
					w.put(n)
				}
				keys := fileBytes(t, forked+kind.keyFile)
				if err = w.close(); err != nil {
					t.Fatal(err)
				}
				os.WriteFile(forked+kind.keyFile, keys, 0600)
				os.WriteFile(forked+".hfile", head, 0600)
				a, b := openBoth(t, kind, forked, &cp, 23)
				sameOutcome(t, a, b)
				if a.info.Check != CheckTail || a.info.WalkedRecords != 50 {
					t.Fatalf("%+v %+v", a.info, a.recovery)
				}
			})
		})
	}
}

// Close and an evicting close establish the checkpoint of the final state:
// reopening with it walks nothing, also for a pair opened by a checkpoint.
func TestCheckpointAfterCloseWalksNothing(t *testing.T) {
	for _, kind := range recoveryKinds {
		t.Run(kind.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "x")
			w := openWriter(t, kind, path)
			for round := 0; round < 3; round++ {
				for n := 0; n < 40; n++ {
					w.put(round*40 + n)
				}
				if err := w.close(); err != nil {
					t.Fatal(err)
				}
				cp, ok := w.checkpoint()
				if !ok {
					t.Fatal("Close established no checkpoint")
				}
				var err error
				if w, err = reopenCP(kind, path, &cp); err != nil {
					t.Fatal(err)
				}
				w.t = t
				if info := w.openInfo(); info.Check != CheckTail || info.WalkedRecords != 0 {
					t.Fatalf("round %d: %+v", round, info)
				}
			}
			if err := w.close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// (e) R1: Commit writes only the dirty 4 KiB pages, yet the header on disk is
// byte for byte the header snapshot at BeginSync, as a full write would be.
func TestPartialHeaderWriteMatchesFullHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x")
	const hashBytes = 5 * 40000 // 49 pages
	idx, err := Open(path, hashBytes, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	var written int
	idx.hook = func(op string) error {
		if op == "header-write" {
			written++
		}
		return nil
	}
	rnd := rand.New(rand.NewSource(7))
	pages := (len(idx.header) + headerPage - 1) / headerPage
	for round := 0; round < 60; round++ {
		puts := 1 + rnd.Intn(4)
		if round%10 == 9 {
			puts = 3000 // most pages dirty
		}
		for n := 0; n < puts; n++ {
			if _, err = idx.Put([]byte(fmt.Sprint("k", rnd.Int63())), []byte{byte(n)}); err != nil {
				t.Fatal(err)
			}
		}
		if round%7 == 6 {
			if err = idx.Sync(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(fileBytes(t, path+".hfile"), idx.header) {
				t.Fatal("Sync: disk header differs from memory")
			}
			continue
		}
		p, err := idx.BeginSync()
		if err != nil {
			t.Fatal(err)
		}
		snapshot := append([]byte(nil), idx.header...)
		if puts < 5 && len(p.pages) > 2+puts {
			t.Fatalf("%d puts snapshot %d pages", puts, len(p.pages))
		}
		if len(p.buf) > len(p.pages)*headerPage {
			t.Fatal("snapshot larger than its pages")
		}
		// Changes after BeginSync must not reach this commit.
		if _, err = idx.Put([]byte("late"), []byte{1}); err != nil {
			t.Fatal(err)
		}
		written = 0
		if err = p.Commit(); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(fileBytes(t, path+".hfile"), snapshot) {
			t.Fatalf("round %d: disk header differs from the BeginSync snapshot", round)
		}
		if written > len(p.pages) || (len(p.pages) > 0 && written == 0) {
			t.Fatalf("round %d: %d writes for %d pages", round, written, len(p.pages))
		}
		if cp, ok := p.Checkpoint(); !ok || cp.KeyBytes != p.length {
			t.Fatal("commit established no checkpoint", cp, ok)
		}
	}
	if err = idx.Sync(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fileBytes(t, path+".hfile"), idx.header) || idx.dirty {
		t.Fatal("final sync")
	}
	t.Logf("%d header pages", pages)
}

// (e) Commit order: key fsync, header writes, header fsync; the checkpoint
// exists only after all three succeeded.
func TestCommitOrderAndFailures(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x")
	idx, err := Open(path, 5*2000, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	var events []string
	fail := ""
	idx.hook = func(op string) error {
		events = append(events, op)
		if op == fail {
			return errors.New("injected " + op)
		}
		return nil
	}
	put := func(k string) {
		if _, err := idx.Put([]byte(k), []byte{1}); err != nil {
			t.Fatal(err)
		}
	}
	put("a")
	p, _ := idx.BeginSync()
	if err = p.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(events, ","); got != "keys-sync,header-write,header-sync" {
		t.Fatal(got)
	}
	if _, ok := p.Checkpoint(); !ok {
		t.Fatal("no checkpoint")
	}
	for _, op := range []string{"keys-sync", "header-write", "header-sync"} {
		t.Run(op, func(t *testing.T) {
			put("b" + op)
			before := append([]byte(nil), fileBytes(t, path+".hfile")...)
			fail, events = op, nil
			p, _ := idx.BeginSync()
			if err := p.Commit(); err == nil {
				t.Fatal("injected failure ignored")
			}
			fail = ""
			if _, ok := p.Checkpoint(); ok {
				t.Fatal("checkpoint after a failed commit")
			}
			if _, ok := idx.Checkpoint(); ok {
				t.Fatal("index still offers a checkpoint after a failed commit")
			}
			if !idx.dirty {
				t.Fatal("failed pages must stay dirty")
			}
			if op == "keys-sync" && !bytes.Equal(before, fileBytes(t, path+".hfile")) {
				t.Fatal("header written although the key fsync failed")
			}
			if err := idx.Sync(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(fileBytes(t, path+".hfile"), idx.header) {
				t.Fatal("retry did not write the failed pages")
			}
			// The disk state was unknown for a while: this handle never
			// offers a checkpoint again (a reopen validates fully).
			if _, ok := idx.Checkpoint(); ok {
				t.Fatal("checkpoint after an unknown disk state")
			}
		})
	}
}

// A Sync between BeginSync and Commit (callers avoid it) writes the current
// bytes; the older snapshot must then not be written over them.
func TestSupersededCommitWritesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x")
	idx, err := Open(path, 5*2000, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	idx.Put([]byte("a"), []byte{1})
	p, _ := idx.BeginSync()
	idx.Put([]byte("a"), []byte{2}) // same bucket: same page, newer head
	if err = idx.Sync(); err != nil {
		t.Fatal(err)
	}
	var events []string
	idx.hook = func(op string) error { events = append(events, op); return nil }
	if err = p.Commit(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(events, ",") != "keys-sync" {
		t.Fatal(events)
	}
	if _, ok := p.Checkpoint(); ok {
		t.Fatal("superseded commit established a checkpoint")
	}
	if !bytes.Equal(fileBytes(t, path+".hfile"), idx.header) {
		t.Fatal("older snapshot overwrote the newer header")
	}
	if !idx.dirty { // conservatively rewritten by the next sync
		t.Fatal("superseded pages not marked dirty")
	}
	if cp, ok := idx.Checkpoint(); !ok || cp.KeyBytes != idx.length {
		t.Fatal("Sync's checkpoint lost", cp, ok)
	}
}

// Any bytes appended after a checkpoint, and any byte of the tail flipped,
// open with exactly the outcome of the full walk.
func FuzzCheckpointTail(f *testing.F) {
	f.Add(uint8(0), []byte{}, uint16(0), byte(0))
	f.Add(uint8(1), tornRecord(recoveryKinds[1], 0, 9), uint16(3), byte(0))
	f.Add(uint8(2), tornRecord(recoveryKinds[2], 0, 20), uint16(7), byte(0x80))
	f.Add(uint8(0), []byte{0, 0, 0, 0, 0, 2, 0, 1, 'k', 1, 9}, uint16(1), byte(1))
	f.Fuzz(func(t *testing.T, kindN uint8, extra []byte, at uint16, x byte) {
		kind := recoveryKinds[int(kindN)%len(recoveryKinds)]
		crash, cp := checkpointCrash(t, kind, 40, 10)
		keys := fileBytes(t, crash+kind.keyFile)
		if len(extra) > 4096 {
			extra = extra[:4096]
		}
		keys = append(keys, extra...)
		if tail := int64(len(keys)) - cp.KeyBytes; tail > 0 {
			keys[cp.KeyBytes+int64(at)%tail] ^= x
		}
		if err := os.WriteFile(crash+kind.keyFile, keys, 0600); err != nil {
			t.Fatal(err)
		}
		a, b := openBoth(t, kind, crash, &cp, 23)
		sameOutcome(t, a, b)
	})
}

// keysOnPages returns n keys whose buckets lie on distinct header pages,
// none of them page 0 (the count's page, dirty on every new bucket).
func keysOnPages(t *testing.T, idx *Index, n int) [][]byte {
	t.Helper()
	used := map[int]bool{0: true}
	var out [][]byte
	for k := 0; len(out) < n && k < 100000; k++ {
		key := []byte(fmt.Sprint("page-key-", k))
		b, err := idx.bucket(key)
		if err != nil {
			t.Fatal(err)
		}
		if pg := (headerBytes + b*5) / headerPage; !used[pg] {
			used[pg] = true
			out = append(out, key)
		}
	}
	if len(out) < n {
		t.Fatal("not enough pages")
	}
	return out
}

// Nested SyncPoints (callers run one pass at a time, but the contract must
// hold anyway): a later point that commits first writes the pages an
// earlier uncommitted point holds, so its checkpoint is true; the earlier
// point then writes nothing and leaves its pages dirty, never stuck.
func TestNestedSyncPoints(t *testing.T) {
	for _, laterFirst := range []bool{true, false} {
		t.Run(fmt.Sprint("later first ", laterFirst), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "x")
			idx, err := Open(path, 5*4000, false, nil) // 5 pages
			if err != nil {
				t.Fatal(err)
			}
			defer idx.Close()
			keys := keysOnPages(t, idx, 2)
			put := func(k []byte) {
				if _, err := idx.Put(k, []byte{1}); err != nil {
					t.Fatal(err)
				}
			}
			if err = idx.Sync(); err != nil {
				t.Fatal(err)
			}
			put(keys[0])
			a, _ := idx.BeginSync()
			put(keys[1])
			b, _ := idx.BeginSync()
			atB := append([]byte(nil), idx.header...)
			first, second := a, b
			if laterFirst {
				first, second = b, a
			}
			if err = first.Commit(); err != nil {
				t.Fatal(err)
			}
			if laterFirst {
				if !bytes.Equal(fileBytes(t, path+".hfile"), atB) {
					t.Fatal("later point left an earlier point's page unwritten")
				}
				cp, ok := b.Checkpoint()
				if !ok {
					t.Fatal("no checkpoint")
				}
				// Its checkpoint claims every record below KeyBytes is
				// reachable from the disk header: check on a crash copy.
				crash := filepath.Join(dir, "crash")
				copyPair(t, path, crash)
				ro, err := OpenCheckpointed(crash, 0, false, nil, nil, nil, &cp)
				if err != nil {
					t.Fatal(err)
				}
				for _, k := range keys {
					if v, err := ro.Get(k); err != nil || v == nil {
						t.Fatalf("%s unreachable through the checkpoint header: %v", k, err)
					}
				}
				if info := ro.OpenInfo(); info.Check != CheckTail || info.WalkedRecords != 0 {
					t.Fatalf("%+v", info)
				}
				ro.Close()
			}
			var writes int
			idx.hook = func(op string) error {
				if op == "header-write" {
					writes++
				}
				return nil
			}
			if err = second.Commit(); err != nil {
				t.Fatal(err)
			}
			idx.hook = nil
			if writes != 0 {
				t.Fatal("superseded point wrote its older snapshot")
			}
			if _, ok := second.Checkpoint(); ok {
				t.Fatal("superseded point established a checkpoint")
			}
			if !idx.dirty {
				t.Fatal("superseded point's pages are not dirty again")
			}
			for pg, refs := range idx.pendingRefs {
				if refs != 0 {
					t.Fatalf("page %d still held by %d points", pg, refs)
				}
			}
			if idx.outstanding != 0 {
				t.Fatal("outstanding points", idx.outstanding)
			}
			// The next pass writes everything that is still missing.
			p, _ := idx.BeginSync()
			if err = p.Commit(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(fileBytes(t, path+".hfile"), idx.header) {
				t.Fatal("pages stuck after the nested commits")
			}
			if err = p.Commit(); err == nil {
				t.Fatal("second Commit of one point accepted")
			}
		})
	}
}
