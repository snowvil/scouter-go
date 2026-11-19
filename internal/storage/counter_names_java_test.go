package storage

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// TestJavaStorageInteropRealtimeCounterNames runs the original
// RealtimeCounterDBHeader/Data/Index (tools/java-storage/
// RealtimeCounterNamesOracle.java) with a name dictionary beyond Go's former
// 4096-name read limit: Java write -> Go read -> Go append (a new name) -> Java
// read and append (another name) -> Go read. The name matches the
// `make storage-check` filter.
func TestJavaStorageInteropRealtimeCounterNames(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	source := os.Getenv("SCOUTER_SOURCE")
	if source == "" {
		source = filepath.Join(repo, "..", "scouter")
	}
	if _, err = os.Stat(filepath.Join(source, "scouter.server")); err != nil {
		t.Skip("pinned Java source unavailable; realtime counter name oracle not run")
	}
	work := t.TempDir()
	output, err := exec.Command("python3", filepath.Join(repo, "scripts", "java-storage.py"), "--classes-dir", filepath.Join(work, "compiled"), "--source", source).CombinedOutput()
	if err != nil {
		t.Fatalf("actual Java/Scala storage compilation: %v\n%s", err, output)
	}
	classpath := strings.TrimSpace(string(output))
	oracleClasses := filepath.Join(work, "oracle")
	if output, err = exec.Command("javac", "--release", "8", "-encoding", "UTF-8", "-cp", classpath, "-d", oracleClasses, filepath.Join(repo, "tools", "java-storage", "RealtimeCounterNamesOracle.java")).CombinedOutput(); err != nil {
		t.Fatalf("oracle compilation: %v\n%s", err, output)
	}
	const names = 5000
	oracle := func(mode, root string) {
		t.Helper()
		output, err := exec.Command("java", "-Duser.timezone=Asia/Seoul", "-cp", oracleClasses+string(os.PathListSeparator)+classpath, "RealtimeCounterNamesOracle", mode, root, strconv.Itoa(names)).CombinedOutput()
		if err != nil {
			t.Fatalf("Java realtime counter oracle %s: %v\n%s", mode, err, output)
		}
		t.Log(strings.TrimSpace(string(output)))
	}
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(work, "db")
	oracle("generate", root)
	hash := CounterHash("/oracle/app")
	check := func(c *Counters, name string, want map[int64]int64) {
		t.Helper()
		points, err := c.Realtime("20260101", hash, name, oracleTime, oracleTime+3000)
		if err != nil || len(points) != len(want) {
			t.Fatalf("%s: %v %v", name, points, err)
		}
		for _, p := range points {
			if v, ok := want[p.Time-oracleTime]; !ok || p.Value.Type != protocol.ValueDecimal || p.Value.Int != v {
				t.Fatalf("%s: %v", name, points)
			}
		}
	}
	c, err := OpenCounters(root, loc)
	if err != nil {
		t.Fatal(err)
	}
	check(c, "c00000", map[int64]int64{0: 0, 1000: -1})
	check(c, "c04999", map[int64]int64{490: 4999, 1000: -2})
	p := &protocol.PerfCounterPack{Time: oracleTime + 2000, ObjName: "/oracle/app", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{
		{Key: "c04500", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 7}},
		{Key: "go-new", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 8}},
	}}}
	if err = c.Put(p); err != nil {
		t.Fatal("Go append to a Java dictionary:", err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	if c, err = OpenCounters(root, loc); err != nil {
		t.Fatal(err)
	}
	check(c, "c04500", map[int64]int64{450: 4500, 2000: 7})
	check(c, "go-new", map[int64]int64{2000: 8})
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	oracle("verify", root)
	if c, err = OpenCounters(root, loc); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	check(c, "go-new", map[int64]int64{2000: 8, 2500: 10})
	check(c, "java-after", map[int64]int64{2500: 9})

	// A day Go creates: createNames, a stale tail left by a failed append
	// (length step), a partially stored pack (d over the name bound).
	goRoot := filepath.Join(work, "go-db")
	g, err := OpenCounters(goRoot, loc)
	if err != nil {
		t.Fatal(err)
	}
	g.namesMax = 3
	pack := func(tm int64, kv ...any) *protocol.PerfCounterPack {
		p := realtimePack(tm, kv...)
		p.ObjName = "/oracle/app"
		return p
	}
	if err = g.Put(pack(oracleTime, "a", 1, "b", 2)); err != nil {
		t.Fatal(err)
	}
	counterHeadFault = func(step string) error {
		if step == "length" {
			return os.ErrDeadlineExceeded
		}
		return nil
	}
	err = g.Put(pack(oracleTime+5, "stale-name-longer-than-the-next-one", 9))
	counterHeadFault = nil
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal(err)
	}
	if err = g.Put(pack(oracleTime+10, "c", 3)); err != nil {
		t.Fatal(err)
	}
	if err = g.Put(pack(oracleTime+20, "a", 4, "d", 5)); err != nil || g.NamesLimited() != 1 {
		t.Fatal(err, g.NamesLimited())
	}
	if err = g.Close(); err != nil {
		t.Fatal(err)
	}
	head, err := os.ReadFile(filepath.Join(goRoot, "20260101", "counter", "real.head"))
	// c overwrote the start of the failed entry; the rest stays after the
	// length as a stale tail both readers ignore.
	if err != nil || !bytes.Contains(head, []byte("-name-longer-than")) {
		t.Fatal("no stale tail to ignore", err)
	}
	output, err = exec.Command("java", "-Duser.timezone=Asia/Seoul", "-cp", oracleClasses+string(os.PathListSeparator)+classpath, "RealtimeCounterNamesOracle", "verify-go", goRoot).CombinedOutput()
	if err != nil {
		t.Fatalf("Java realtime counter oracle verify-go: %v\n%s", err, output)
	}
	t.Log(strings.TrimSpace(string(output)))
}
