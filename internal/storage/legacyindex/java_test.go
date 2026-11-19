package legacyindex

import (
	"bytes"
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOriginalJavaStorageInterop(t *testing.T) {
	for _, tool := range []string{"python3", "javac", "java"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("actual Java oracle unavailable: %s: %v", tool, err)
		}
	}
	source := os.Getenv("SCOUTER_SOURCE")
	if source == "" {
		source = "../../../../scouter"
	}
	if _, err := os.Stat(source + "/scouter.deploy/lib/scala/scala-compiler.jar"); err != nil {
		t.Skipf("actual pinned Scouter source unavailable: %v", err)
	}
	work := t.TempDir()
	cmd := exec.Command("python3", "testdata/run_oracle.py", "--classes-dir", work, "--source", source)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("compile actual original Java/Scala: %v\n%s", err, output)
	}
	cp := strings.TrimSpace(string(output))
	classes := filepath.Join(work, "classes")
	output, err = exec.Command("javac", "--release", "8", "-cp", cp, "-d", classes, "testdata/IndexOracle.java").CombinedOutput()
	if err != nil {
		t.Fatalf("compile oracle: %v\n%s", err, output)
	}
	run := func(mode, dir string) {
		t.Helper()
		output, err := exec.Command("java", "-Duser.timezone=Asia/Seoul", "-cp", cp, "IndexOracle", mode, dir).CombinedOutput()
		if err != nil {
			t.Fatalf("original Java %s: %v\n%s", mode, err, output)
		}
		t.Logf("%s", output)
	}
	javaDir, goDir := filepath.Join(work, "java"), filepath.Join(work, "go")
	run("generate", javaDir)
	if err = os.Mkdir(goDir, 0700); err != nil {
		t.Fatal(err)
	}
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	hash, err := Open(filepath.Join(javaDir, "hash"), 5, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := hash.Get([]byte{0, 255, 2})
	if err != nil || !bytes.Equal(got, []byte{3}) {
		t.Fatalf("Java->Go hash %x %v", got, err)
	}
	all, err := hash.GetAll([]byte{0, 255, 2})
	if err != nil || len(all) != 1 {
		t.Fatalf("deleted chain %v %v", all, err)
	}
	count, err := hash.Count()
	if err != nil || count != 6 {
		t.Fatalf("Java hash bucket count %d %v", count, err)
	}
	hash.Close()
	const ms = int64(1767193200125)
	timed, err := Open(filepath.Join(javaDir, "time"), 0, true, loc)
	if err != nil {
		t.Fatal(err)
	}
	records, err := timed.Range(ms, ms+500, false)
	if err != nil || len(records) != 3 || records[0].Value[0] != 128 {
		t.Fatalf("Java->Go time %v %v", records, err)
	}
	timed.Close()
	hash, err = Open(filepath.Join(goDir, "hash"), 1<<20, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, hash, []byte{0, 255, 2}, []byte{128})
	mustPut(t, hash, []byte{0, 255, 2}, []byte{3})
	for _, n := range []int{0, 253, 254, 65535, 65536} {
		mustPut(t, hash, []byte{byte(n >> 16), byte(n >> 8), byte(n)}, bytes.Repeat([]byte{129}, n))
	}
	if err = hash.Close(); err != nil {
		t.Fatal(err)
	}
	k, err := os.OpenFile(filepath.Join(goDir, "hash.kfile"), os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.WriteAt([]byte{1}, 2); err != nil {
		t.Fatal(err)
	}
	k.Close()
	timed, err = Open(filepath.Join(goDir, "time"), 0, true, loc)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []byte{127, 128, 128} {
		mustPut(t, timed, timeKey(ms), []byte{v})
	}
	mustPut(t, timed, timeKey(ms+400), []byte{0})
	if err = timed.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"hash.kfile", "hash.hfile", "time.kfile", "time.hfile"} {
		want, err := os.ReadFile(filepath.Join(javaDir, name))
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(goDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("byte mismatch actual Java versus Go: %s", name)
		}
	}
	run("verify", goDir)
	h, _ := os.ReadFile(filepath.Join(goDir, "time.hfile"))
	if binary.BigEndian.Uint32(h[4:8]) != 6 {
		t.Fatal("count differs")
	}
	verifyJavaStaleHeaderRecovery(t, javaDir, filepath.Join(work, "recovered"), loc, run)
	verifyJavaDoubleCrash(t, javaDir, filepath.Join(work, "double-crash"), loc, run)
}

// verifyJavaDoubleCrash appends two records of one new key that both link to
// the empty saved head (Java restarted from a stale header, crashed again).
// Go must open it with a refused relink and leave it byte-identical so the
// original Java reader still accepts it.
func verifyJavaDoubleCrash(t *testing.T, javaDir, dir string, loc *time.Location, run func(string, string)) {
	t.Helper()
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"hash.kfile", "hash.hfile", "time.kfile", "time.hfile"} {
		b, err := os.ReadFile(filepath.Join(javaDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, name), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	key := []byte("double-crash")
	ro, err := OpenReadOnly(filepath.Join(dir, "hash"), false, nil)
	if err != nil {
		t.Fatal(err)
	}
	bucket, _ := ro.bucket(key)
	saved := ro.head(bucket)
	ro.Close()
	if saved != 0 {
		t.Fatalf("fixture bucket %d unexpectedly occupied", bucket)
	}
	f, err := os.OpenFile(filepath.Join(dir, "hash.kfile"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []byte{1, 2} {
		rec := make([]byte, 8+len(key)+2)
		binary.BigEndian.PutUint16(rec[6:8], uint16(len(key))) // prev 0 both times
		copy(rec[8:], key)
		rec[8+len(key)], rec[9+len(key)] = 1, v
		f.Write(rec)
	}
	f.Close()
	before, _ := os.ReadFile(filepath.Join(dir, "hash.kfile"))
	beforeHead, _ := os.ReadFile(filepath.Join(dir, "hash.hfile"))
	idx, err := Open(filepath.Join(dir, "hash"), 0, false, loc)
	if err != nil {
		t.Fatalf("double-crash open: %v", err)
	}
	info := idx.Recovered()
	got, err := idx.Get(key)
	if info.RelinkRefused == "" || info.Unreachable != 2 || got != nil || err != nil {
		t.Fatalf("double-crash %+v get=%x %v", info, got, err)
	}
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "hash.kfile"))
	afterHead, _ := os.ReadFile(filepath.Join(dir, "hash.hfile"))
	if info.Tombstoned != 2 || !onlyTombstones(before, after) || !bytes.Equal(beforeHead, afterHead) {
		t.Fatal("refused relink must tombstone exactly the two orphans and keep the header")
	}
	t.Logf("double-crash refused relink: %+v", info)
	// Java get/getAll stay empty; its scan counts 6 active and the 2 tombstoned as deleted.
	run("verify-double-crash", dir)
}

// verifyJavaStaleHeaderRecovery rolls copies of Java-written headers back to
// earlier valid snapshots (the state after a crash inside the 4 s FlushCtr
// window), lets the Go writable Open relink the newer key records, and checks
// the persisted header against Java's own and with the original Java reader.
func verifyJavaStaleHeaderRecovery(t *testing.T, javaDir, dir string, loc *time.Location, run func(string, string)) {
	t.Helper()
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"hash.kfile", "hash.hfile", "time.kfile", "time.hfile"} {
		b, err := os.ReadFile(filepath.Join(javaDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, name), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// hash: the header as first flushed by MemHashBlock (no heads, count 0).
	hashHead, _ := os.ReadFile(filepath.Join(dir, "hash.hfile"))
	for n := 4; n < len(hashHead); n++ {
		if n >= 8 && n < 1024 {
			continue
		}
		hashHead[n] = 0
	}
	if err := os.WriteFile(filepath.Join(dir, "hash.hfile"), hashHead, 0600); err != nil {
		t.Fatal(err)
	}
	// time: the snapshot after the first two of four puts in the TIME bucket.
	ro, err := OpenReadOnly(filepath.Join(dir, "time"), true, loc)
	if err != nil {
		t.Fatal(err)
	}
	records, err := ro.Records()
	ro.Close()
	if err != nil || len(records) != 4 {
		t.Fatalf("Java time records %v %v", records, err)
	}
	timeHead, _ := os.ReadFile(filepath.Join(dir, "time.hfile"))
	bucket := int(((1767193200125 - time.Date(2000, 1, 1, 0, 0, 0, 0, loc).UnixMilli()) % dayMillis) / 500)
	for n := 1024; n < len(timeHead); n++ {
		timeHead[n] = 0
	}
	writeLong5(timeHead[1024+bucket*5:], records[1].Position)
	binary.BigEndian.PutUint32(timeHead[4:8], 3) // 2 records + 1 occupied bucket
	if err = os.WriteFile(filepath.Join(dir, "time.hfile"), timeHead, 0600); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name     string
		timed    bool
		relinked int64
	}{{"hash", false, 7}, {"time", true, 2}} {
		idx, err := Open(filepath.Join(dir, c.name), 0, c.timed, loc)
		if err != nil {
			t.Fatalf("%s recovery open: %v", c.name, err)
		}
		info := idx.Recovered()
		if !info.Applied || info.RelinkedRecords != c.relinked || info.CountAfter != 6 {
			t.Fatalf("%s recovery %+v", c.name, info)
		}
		if err = idx.Close(); err != nil {
			t.Fatal(err)
		}
		want, _ := os.ReadFile(filepath.Join(javaDir, c.name+".hfile"))
		got, _ := os.ReadFile(filepath.Join(dir, c.name+".hfile"))
		if !bytes.Equal(want, got) {
			t.Fatalf("%s: recovered header differs from the Java-flushed header", c.name)
		}
		t.Logf("%s stale-header recovery: %+v", c.name, info)
	}
	run("verify", dir)
}
