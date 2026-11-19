package legacyindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func openTTLTest(t *testing.T, size int) (*TTLIndex, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ttl")
	idx, err := OpenTTL(path, size)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })
	return idx, path
}
func putTTLTest(t *testing.T, idx *TTLIndex, key, value []byte, expires int64) int64 {
	t.Helper()
	pos, err := idx.PutContext(context.Background(), key, value, expires)
	if err != nil {
		t.Fatal(err)
	}
	return pos
}

func TestTTLFormatExpireDeleteAndRestart(t *testing.T) {
	idx, path := openTTLTest(t, 6)
	ctx := context.Background()
	p1 := putTTLTest(t, idx, []byte("a"), []byte{1}, -1)
	p2 := putTTLTest(t, idx, []byte("b"), []byte{}, 100)
	p3 := putTTLTest(t, idx, []byte("a"), []byte{2}, 100)
	if p1 != 2 || p2 != 18 || p3 != 33 {
		t.Fatal(p1, p2, p3)
	}
	v, err := idx.GetContext(ctx, []byte("a"), 100)
	if err != nil || !bytes.Equal(v, []byte{2}) {
		t.Fatal(v, err)
	}
	v[0] = 99
	owned, ownedErr := idx.GetContext(ctx, []byte("a"), 100)
	if ownedErr != nil || !bytes.Equal(owned, []byte{2}) {
		t.Fatal("caller mutation changed index", owned, ownedErr)
	}
	v, err = idx.GetContext(ctx, []byte("a"), 101)
	if v != nil || err != nil {
		t.Fatal("expired newest must hide old never-expiring record", v, err)
	}
	changed, err := idx.SetTTLContext(ctx, []byte("a"), 101, 200)
	if changed || err != nil {
		t.Fatal(changed, err)
	}
	changed, err = idx.SetTTLContext(ctx, []byte("a"), 100, 200)
	if !changed || err != nil {
		t.Fatal(changed, err)
	}
	v, err = idx.GetContext(ctx, []byte("b"), 100)
	if v == nil || len(v) != 0 || err != nil {
		t.Fatal("empty must be distinguishable from missing", v, err)
	}
	deleted, err := idx.DeleteContext(ctx, []byte("a"))
	if !deleted || err != nil {
		t.Fatal(deleted, err)
	}
	deleted, err = idx.DeleteContext(ctx, []byte("a"))
	if deleted || err != nil {
		t.Fatal(deleted, err)
	}
	if err := idx.Sync(); err != nil {
		t.Fatal(err)
	}
	k, _ := os.ReadFile(path + ".k2file")
	h, _ := os.ReadFile(path + ".hfile")
	if !bytes.Equal(k[p1+1:p1+6], []byte{0x7f, 0xff, 0xff, 0xff, 0x00}) {
		t.Fatal("Java LONG5_MAX_VALUE bytes", k[p1+1:p1+6])
	}
	if len(h) != 1030 || h[0] != 0xca || h[1] != 0xfe || binary.BigEndian.Uint32(h[4:8]) != 1 || readLong5(h[1024:]) != p3 {
		t.Fatal("header")
	}
	if k[0] != 0xca || k[1] != 0xfe || readLong5(k[p1+1:]) != TTLNoExpiry || readLong5(k[p3+1:]) != 200 || readLong5(k[p3+6:]) != p2 || k[p3] != 1 {
		t.Fatalf("record %x", k)
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := OpenTTL(path, 500)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if v, err := again.GetContext(ctx, []byte("a"), 100); v != nil || err != nil {
		t.Fatal(v, err)
	}
	if err := again.Close(); err != nil {
		t.Fatal(err)
	}
	if err := again.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := again.GetContext(ctx, []byte("b"), 100); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestTTLUpdateOrPutPhysicalDuplicates(t *testing.T) {
	idx, path := openTTLTest(t, 5)
	ctx := context.Background()
	p1 := putTTLTest(t, idx, []byte("key"), []byte{1, 2, 3, 4, 5}, 10)
	putTTLTest(t, idx, []byte("collision"), []byte{6}, -1)
	p2 := putTTLTest(t, idx, []byte("key"), []byte{6, 7, 8, 9, 0}, -1)
	if _, err := idx.DeleteContext(ctx, []byte("key")); err != nil {
		t.Fatal(err)
	}
	p3, err := idx.UpdateOrPutContext(ctx, []byte("key"), []byte{9, 8, 7, 6, 5}, 200)
	if err != nil || p3 <= p2 {
		t.Fatal(p3, err)
	}
	if err := idx.Sync(); err != nil {
		t.Fatal(err)
	}
	k, _ := os.ReadFile(path + ".k2file")
	for _, pos := range []int64{p1, p2, p3} {
		if k[pos] != 0 || readLong5(k[pos+1:]) != 200 || !bytes.Equal(k[pos+17:pos+22], []byte{9, 8, 7, 6, 5}) {
			t.Fatal("every match is rewritten and a new record appended", pos, k)
		}
	}
	if v, err := idx.GetContext(ctx, []byte("key"), 200); err != nil || !bytes.Equal(v, []byte{9, 8, 7, 6, 5}) {
		t.Fatal(v, err)
	}
	// A shrinking Blob cannot replace the physical record without leaving
	// orphan bytes; append preserves a valid file instead of reproducing that bug.
	if _, err := idx.UpdateOrPutContext(ctx, []byte("key"), []byte{1}, 200); err != nil {
		t.Fatal(err)
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := OpenTTL(path, 5)
	if err != nil {
		t.Fatal(err)
	}
	again.Close()
}

func TestTTLBoundsCorruptAndContext(t *testing.T) {
	idx, path := openTTLTest(t, 5)
	ctx := context.Background()
	if _, err := idx.PutContext(ctx, make([]byte, 65536), []byte{}, 1); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := idx.PutContext(ctx, []byte{}, make([]byte, MaxValueBytes+1), 1); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := idx.PutContext(ctx, []byte{}, []byte{}, maxPosition+1); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := idx.PutContext(ctx, nil, []byte{}, 1); err == nil {
		t.Fatal("nil key")
	}
	cancel, stop := context.WithCancel(ctx)
	stop()
	if _, err := idx.GetContext(cancel, []byte{}, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := idx.PutContext(cancel, []byte{}, []byte{}, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	pos := putTTLTest(t, idx, []byte{}, []byte{}, -1)
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path+".k2file", os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	var bad [5]byte
	writeLong5(bad[:], pos)
	f.WriteAt(bad[:], pos+6)
	f.Close()
	if _, err := OpenTTL(path, 5); !errors.Is(err, ErrCorrupt) {
		t.Fatal("cycle", err)
	}
	missing := filepath.Join(t.TempDir(), "missing")
	os.WriteFile(missing+".k2file", []byte{0xca, 0xfe}, 0600)
	if _, err := OpenTTL(missing, 5); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestTTLConcurrentAndWaitingCancellation(t *testing.T) {
	idx, _ := openTTLTest(t, 6)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := []byte{byte(n)}
			for i := 0; i < 25; i++ {
				if _, err := idx.UpdateOrPutContext(ctx, key, []byte{byte(i)}, 200); err != nil {
					errs <- err
					return
				}
				if _, err := idx.GetContext(ctx, key, 100); err != nil {
					errs <- err
					return
				}
			}
		}(n)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	idx.index.mu.Lock()
	waiting, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	defer stop()
	if _, err := idx.GetContext(waiting, []byte{1}, 100); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	idx.index.mu.Unlock()
}

func TestTTLBlobPrefixesAndGrowth(t *testing.T) {
	idx, path := openTTLTest(t, 6)
	ctx := context.Background()
	for n, size := range []int{0, 253, 254, 65535, 65536} {
		key := []byte{byte(n)}
		value := bytes.Repeat([]byte{byte(n + 1)}, size)
		pos := putTTLTest(t, idx, key, value, -1)
		if err := idx.Sync(); err != nil {
			t.Fatal(err)
		}
		k, err := os.ReadFile(path + ".k2file")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(k[pos+14:pos+14+int64(len(blobPrefix(size)))], blobPrefix(size)) {
			t.Fatal("Blob prefix", size)
		}
		got, err := idx.GetContext(ctx, key, 100)
		if err != nil || !bytes.Equal(got, value) || got == nil {
			t.Fatal(size, err)
		}
	}
	key := []byte{9}
	first := putTTLTest(t, idx, key, []byte{1}, 100)
	last, err := idx.UpdateOrPutContext(ctx, key, []byte{2, 3}, 200)
	if err != nil || last <= first {
		t.Fatal(last, err)
	}
	m, err := idx.meta(first)
	if err != nil || m.expires != 100 || m.valueBytes != 1 {
		t.Fatal("growth must append without rewriting old record", m, err)
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	other, err := OpenTTL(path, 6)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	got, err := other.GetContext(ctx, key, 200)
	if err != nil || !bytes.Equal(got, []byte{2, 3}) {
		t.Fatal(got, err)
	}
}

type ttlCancelAfter struct {
	context.Context
	calls int
}

func (c *ttlCancelAfter) Err() error {
	c.calls--
	if c.calls <= 0 {
		return context.Canceled
	}
	return nil
}
func TestTTLCancellationInsideCollisionChain(t *testing.T) {
	idx, _ := openTTLTest(t, 5)
	for n := 0; n < 40; n++ {
		putTTLTest(t, idx, []byte{byte(n)}, []byte{}, -1)
	}
	ctx := &ttlCancelAfter{Context: context.Background(), calls: 10}
	if _, err := idx.GetContext(ctx, []byte("missing"), 100); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	cancel, stop := context.WithCancel(context.Background())
	stop()
	path := filepath.Join(t.TempDir(), "cancelled")
	if _, err := OpenTTLContext(cancel, path, 5); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".k2file"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("cancelled open created a file", err)
	}
}

func TestTTLBadHeadersHeadsAndTruncation(t *testing.T) {
	idx, path := openTTLTest(t, 5)
	putTTLTest(t, idx, []byte("a"), []byte{1}, -1)
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	key, _ := os.ReadFile(path + ".k2file")
	head, _ := os.ReadFile(path + ".hfile")
	for _, name := range []string{"magic", "count", "head-middle", "negative-blob", "truncated"} {
		t.Run(name, func(t *testing.T) {
			k := bytes.Clone(key)
			h := bytes.Clone(head)
			base := filepath.Join(t.TempDir(), "bad")
			switch name {
			case "magic":
				k[0] = 0
			case "count":
				binary.BigEndian.PutUint32(h[4:8], 0xffffffff)
			case "head-middle":
				writeLong5(h[1024:], 3)
			case "negative-blob":
				k = append(k[:16], []byte{254, 255, 255, 255, 255}...)
			case "truncated":
				k = k[:len(k)-1]
			}
			os.WriteFile(base+".k2file", k, 0600)
			os.WriteFile(base+".hfile", h, 0600)
			if got, err := OpenTTL(base, 5); !errors.Is(err, ErrCorrupt) {
				if got != nil {
					got.Close()
				}
				t.Fatal(err)
			}
		})
	}
}

func FuzzTTLIndexFiles(f *testing.F) {
	path := filepath.Join(f.TempDir(), "seed")
	idx, err := OpenTTL(path, 5)
	if err != nil {
		f.Fatal(err)
	}
	if _, err = idx.PutContext(context.Background(), []byte("key"), []byte{1}, TTLNoExpiry); err != nil {
		f.Fatal(err)
	}
	if err = idx.Close(); err != nil {
		f.Fatal(err)
	}
	data, err := os.ReadFile(path + ".k2file")
	if err != nil {
		f.Fatal(err)
	}
	head, err := os.ReadFile(path + ".hfile")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(data, append([]byte(nil), head[1024:]...))
	f.Add([]byte{0xca, 0xfe}, []byte{0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data, bucket []byte) {
		if len(data) > 4096 || len(bucket) != 5 {
			return
		}
		path := filepath.Join(t.TempDir(), "fuzz")
		header := make([]byte, 1029)
		header[0], header[1] = 0xca, 0xfe
		copy(header[1024:], bucket)
		if err := os.WriteFile(path+".k2file", data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path+".hfile", header, 0600); err != nil {
			t.Fatal(err)
		}
		planned, planErr := AnalyzeTTLRecovery(context.Background(), path)
		idx, err := OpenTTL(path, 5)
		if (err == nil) != (planErr == nil) {
			t.Fatalf("analyze %v disagrees with writable open %v", planErr, err)
		}
		if err != nil {
			return
		}
		if got := idx.Recovered(); got.RelinkedRecords != planned.RelinkedRecords || got.TruncatedTailBytes != planned.TruncatedTailBytes {
			t.Fatalf("analyze %+v, applied %+v", planned, got)
		}
		_, _ = idx.GetContext(context.Background(), []byte("key"), 1)
		_, _ = idx.SetTTLContext(context.Background(), []byte("key"), 1, 2)
		_, _ = idx.DeleteContext(context.Background(), []byte("key"))
		if err = idx.Close(); err != nil {
			return
		}
		// Recovery is persisted by Close and is idempotent.
		again, err := OpenTTL(path, 5)
		if err != nil {
			t.Fatalf("reopen after recovery: %v", err)
		}
		if again.Recovered().Recovered() {
			t.Fatalf("second open recovered again: %+v", again.Recovered())
		}
		again.Close()
	})
}

// Both modes compile and execute the pinned original Scala index classes.
// No original DB is changed: generation and every write use t.TempDir copies.
func TestTTLActualScalaBidirectionalFiles(t *testing.T) {
	ctx := context.Background()
	run := func(option, dir string) {
		t.Helper()
		commandCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
		defer cancel()
		out, err := exec.CommandContext(commandCtx, "python3", "../../../scripts/java-storage.py", option, dir).CombinedOutput()
		if err != nil {
			t.Fatalf("actual Scala %s: %v\n%s", option, err, out)
		}
		if !strings.Contains(string(out), "PASS actual TTLIndex") {
			t.Fatalf("missing actual Scala success: %s", out)
		}
		t.Log(strings.TrimSpace(string(out)))
	}
	javaDir := t.TempDir()
	run("--generate-ttl", javaDir)
	verifyJavaTTLStaleHeaderRecovery(t, javaDir, run)
	var meta struct {
		HashSize  int    `json:"hash_size_bytes"`
		NoExpiry  int64  `json:"no_expiry"`
		Collision string `json:"collision_key"`
		FirstA    int64  `json:"first_a_position"`
		LatestA   int64  `json:"latest_a_position"`
		Expired   int64  `json:"expired_position"`
		Future    int64  `json:"future_position"`
		Source    string `json:"source_commit"`
	}
	metadata, err := os.ReadFile(filepath.Join(javaDir, "ttl-metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(metadata, &meta); err != nil {
		t.Fatal(err)
	}
	if meta.Source != "dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5" || meta.NoExpiry != 549755813632 || meta.HashSize != 1048576 || meta.Collision == "" {
		t.Fatalf("wrong oracle metadata: %+v", meta)
	}
	offset := func(n int64) []byte { b := make([]byte, 5); writeLong5(b, n); return b }
	original := make(map[string][]byte)
	for _, suffix := range []string{".k2file", ".hfile"} {
		original[suffix], err = os.ReadFile(filepath.Join(javaDir, "ttl") + suffix)
		if err != nil {
			t.Fatal(err)
		}
	}
	idx, err := OpenTTL(filepath.Join(javaDir, "ttl"), 5)
	if err != nil {
		t.Fatal(err)
	}
	check := func(index *TTLIndex, key string, now int64, want []byte) {
		t.Helper()
		got, err := index.GetContext(ctx, []byte(key), now)
		if err != nil || !bytes.Equal(got, want) || (got == nil) != (want == nil) {
			t.Fatalf("actual Scala key=%s now=%d got=%x want=%x err=%v", key, now, got, want, err)
		}
	}
	check(idx, "a", 101, offset(44))
	check(idx, meta.Collision, 101, offset(22))
	check(idx, "expired", 100, offset(55))
	check(idx, "expired", 101, nil)
	check(idx, "deleted", 100, nil)
	check(idx, "empty", 100, []byte{})
	check(idx, "future", 4294967295, offset(77))
	check(idx, "future", 4294967296, nil)
	for _, entry := range []struct{ pos, expiry int64 }{{meta.FirstA, meta.NoExpiry}, {meta.LatestA, meta.NoExpiry}, {meta.Expired, 100}, {meta.Future, 4294967295}} {
		record, err := idx.meta(entry.pos)
		if err != nil || record.expires != entry.expiry {
			t.Fatal("original expiry bytes", entry, record, err)
		}
	}
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}

	goDir := t.TempDir()
	idx, err = OpenTTL(filepath.Join(goDir, "ttl"), meta.HashSize)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	put := func(key string, value []byte) int64 {
		t.Helper()
		position, err := idx.PutContext(ctx, []byte(key), value, -1)
		if err != nil {
			t.Fatal(err)
		}
		return position
	}
	set := func(key string, now, expires int64) {
		t.Helper()
		ok, err := idx.SetTTLContext(ctx, []byte(key), now, expires)
		if err != nil || !ok {
			t.Fatal("SetTTL", key, ok, err)
		}
	}
	first := put("a", offset(11))
	set("a", 0, 100)
	put(meta.Collision, offset(22))
	put("a", offset(33))
	if deleted, err := idx.DeleteContext(ctx, []byte("a")); err != nil || !deleted {
		t.Fatal(deleted, err)
	}
	latest, err := idx.UpdateOrPutContext(ctx, []byte("a"), offset(44), -1)
	if err != nil {
		t.Fatal(err)
	}
	expired := put("expired", offset(55))
	set("expired", 0, 100)
	put("deleted", offset(66))
	if deleted, err := idx.DeleteContext(ctx, []byte("deleted")); err != nil || !deleted {
		t.Fatal(deleted, err)
	}
	put("empty", []byte{})
	future := put("future", offset(77))
	set("future", 100, 4294967295)
	if first != meta.FirstA || latest != meta.LatestA || expired != meta.Expired || future != meta.Future {
		t.Fatalf("original physical positions: %d %d %d %d vs %+v", first, latest, expired, future, meta)
	}
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}
	goWritten := make(map[string][]byte)
	for _, suffix := range []string{".k2file", ".hfile"} {
		got, err := os.ReadFile(filepath.Join(goDir, "ttl") + suffix)
		if err != nil || !bytes.Equal(got, original[suffix]) {
			t.Fatal("Go writer differs from actual Scala whole-file bytes", suffix, err)
		}
		goWritten[suffix] = got
	}
	// The original verifier writes only into an additional Go DB copy.
	verifyDir := t.TempDir()
	for _, suffix := range []string{".k2file", ".hfile"} {
		if err = os.WriteFile(filepath.Join(verifyDir, "ttl")+suffix, goWritten[suffix], 0600); err != nil {
			t.Fatal(err)
		}
	}
	run("--verify-ttl", verifyDir)

	// Mutation checks use a fresh original-generated copy and validate exactly
	// which physical records change, including the latest-only SetTTL/delete.
	mutateDir := t.TempDir()
	for _, suffix := range []string{".k2file", ".hfile"} {
		if err = os.WriteFile(filepath.Join(mutateDir, "ttl")+suffix, original[suffix], 0600); err != nil {
			t.Fatal(err)
		}
	}
	mutated, err := OpenTTL(filepath.Join(mutateDir, "ttl"), meta.HashSize)
	if err != nil {
		t.Fatal(err)
	}
	defer mutated.Close()
	before, err := os.ReadFile(filepath.Join(mutateDir, "ttl.k2file"))
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := mutated.SetTTLContext(ctx, []byte("expired"), 101, 200); ok || err != nil {
		t.Fatal("expired TTL mutation", ok, err)
	}
	if ok, err := mutated.SetTTLContext(ctx, []byte("a"), 100, 200); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if err = mutated.Sync(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(mutateDir, "ttl.k2file"))
	if err != nil {
		t.Fatal(err)
	}
	expected := bytes.Clone(before)
	writeLong5(expected[meta.LatestA+1:meta.LatestA+6], 200)
	if !bytes.Equal(after, expected) {
		t.Fatal("SetTTL must only change newest a expire field")
	}
	check(mutated, "a", 200, offset(44))
	check(mutated, "a", 201, nil)
	if deleted, err := mutated.DeleteContext(ctx, []byte("a")); err != nil || !deleted {
		t.Fatal(deleted, err)
	}
	check(mutated, "a", 100, nil)
	if err = mutated.Sync(); err != nil {
		t.Fatal(err)
	}
	after, err = os.ReadFile(filepath.Join(mutateDir, "ttl.k2file"))
	if err != nil {
		t.Fatal(err)
	}
	expected[meta.LatestA] = 1
	if !bytes.Equal(after, expected) {
		t.Fatal("delete must only change newest a flag")
	}
	position, err := mutated.UpdateOrPutContext(ctx, []byte("a"), offset(88), 300)
	if err != nil || position != int64(len(before)) {
		t.Fatal(position, err)
	}
	for _, pos := range []int64{meta.FirstA, meta.LatestA, position} {
		record, err := mutated.meta(pos)
		if err != nil || record.Deleted || record.expires != 300 {
			t.Fatal("matching resurrection/update", pos, record, err)
		}
		value, err := mutated.index.record(record.metadata)
		if err != nil || !bytes.Equal(value.Value, offset(88)) {
			t.Fatal(value, err)
		}
	}
	check(mutated, "a", 300, offset(88))
	if err = mutated.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{".k2file", ".hfile"} {
		untouched, err := os.ReadFile(filepath.Join(javaDir, "ttl") + suffix)
		if err != nil || !bytes.Equal(untouched, original[suffix]) {
			t.Fatal("original generated DB was modified", suffix, err)
		}
	}
}

// verifyJavaTTLStaleHeaderRecovery rolls a copy of the Java-written TTL header
// back to its first flushed state (no heads, count 0), lets the writable Go
// open relink every record, and re-verifies the copy with the original reader.
func verifyJavaTTLStaleHeaderRecovery(t *testing.T, javaDir string, run func(string, string)) {
	t.Helper()
	dir := t.TempDir()
	entries, err := os.ReadDir(javaDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(javaDir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, e.Name()), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	javaHead, err := os.ReadFile(filepath.Join(javaDir, "ttl.hfile"))
	if err != nil {
		t.Fatal(err)
	}
	stale := append([]byte(nil), javaHead...)
	for n := 4; n < len(stale); n++ {
		if n < 8 || n >= headerBytes {
			stale[n] = 0
		}
	}
	if err = os.WriteFile(filepath.Join(dir, "ttl.hfile"), stale, 0600); err != nil {
		t.Fatal(err)
	}
	idx, err := OpenTTL(filepath.Join(dir, "ttl"), 5)
	if err != nil {
		t.Fatalf("TTL recovery open: %v", err)
	}
	info := idx.Recovered()
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "ttl.hfile"))
	if !info.Applied || info.RelinkedRecords == 0 || !bytes.Equal(got, javaHead) {
		t.Fatalf("TTL stale-header recovery %+v; header equal=%v", info, bytes.Equal(got, javaHead))
	}
	t.Logf("TTL stale-header recovery: %+v", info)
	run("--verify-ttl", dir)
}
