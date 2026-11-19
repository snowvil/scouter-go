package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// configOracle compiles tools/java-storage/ConfigStorageOracle.java with the
// pinned original TextWR/TextRD/TextPermWR/TextPermRD, KeyValueStoreIndex and
// AutoDeleteScheduler (scripts/java-storage.py --config-classes-dir) and
// returns a runner. Tests skip when the pinned source is absent; a skip is not
// a compatibility pass.
func configOracle(t *testing.T) func(args ...string) string {
	t.Helper()
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	source := os.Getenv("SCOUTER_SOURCE")
	if source == "" {
		source = filepath.Join(repo, "..", "scouter")
	}
	if _, err = os.Stat(filepath.Join(source, "scouter.server")); err != nil {
		t.Skip("pinned Java source unavailable; config storage oracle not run")
	}
	classes := filepath.Join(t.TempDir(), "compiled")
	output, err := exec.Command("python3", filepath.Join(repo, "scripts", "java-storage.py"), "--config-classes-dir", classes, "--source", source).CombinedOutput()
	if err != nil {
		t.Fatalf("original Java/Scala config oracle compilation: %v\n%s", err, output)
	}
	classpath := strings.TrimSpace(string(output))
	return func(args ...string) string {
		t.Helper()
		out, err := exec.Command("java", append([]string{"-Duser.timezone=Asia/Seoul", "-cp", classpath, "ConfigStorageOracle"}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("Java config oracle %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
}

func seoul(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

// treeBytes maps every file below root to its content.
func treeBytes(t *testing.T, root string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		rel, _ := filepath.Rel(root, p)
		out[filepath.ToSlash(rel)] = b
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func sameTrees(t *testing.T, java, golang string) {
	t.Helper()
	j, g := treeBytes(t, java), treeBytes(t, golang)
	dropRuntimeState(g) // Go runtime state (clean.go), not Java DB files
	var names []string
	for n := range j {
		names = append(names, n)
	}
	for n := range g {
		if _, ok := j[n]; !ok {
			t.Errorf("only Go wrote %s", n)
		}
	}
	sort.Strings(names)
	for _, n := range names {
		gb, ok := g[n]
		if !ok {
			t.Errorf("only Java wrote %s", n)
			continue
		}
		if !bytes.Equal(j[n], gb) {
			t.Errorf("%s differs: Java %d bytes, Go %d bytes", n, len(j[n]), len(gb))
		}
	}
	t.Logf("compared %d files byte for byte", len(names))
}

// V-TEXT: mgr_text_db_daily_*_enabled switched while writing must leave the
// same files as Java TextWR.process/TextPermWR, and TextRD.getString with each
// switch state (fresh process, empty TextCache) must find the same texts,
// including the daily-miss permanent fallback and D-T1.
func TestJavaStorageInteropConfigTextDaily(t *testing.T) {
	oracle := configOracle(t)
	work := t.TempDir()
	const date = "20261013"
	type op struct {
		daily []bool // nil: put
		div   string
		hash  int32
		text  string
	}
	ops := []op{
		{div: "service", hash: 1, text: "perm-before"},
		{div: "apicall", hash: 2, text: "api-perm"},
		{daily: []bool{true, true, true}},
		{div: "service", hash: 3, text: "daily-only 서비스"},
		{div: "service", hash: 1, text: "daily-dup"},
		{div: "apicall", hash: 4, text: "api-daily"},
		{div: "ua", hash: 5, text: "ua-daily"},
		{div: "error", hash: 6, text: "err"},
		{div: "error", hash: 7, text: ""},
		{div: "sql", hash: 8, text: "select 1"},
		{div: "table", hash: 9, text: "EMP(S)"},
		{div: "city", hash: 10, text: "Seoul"},
		{daily: []bool{false, false, false}},
		{div: "service", hash: 11, text: "perm-after"},
		{div: "error", hash: 12, text: "err-2"},
		{div: "ua", hash: 13, text: ""},
		{div: "service", hash: 3, text: "perm-copy-of-3"},
	}
	flag := func(b bool) string {
		if b {
			return "1"
		}
		return "0"
	}
	var script strings.Builder
	goRoot := filepath.Join(work, "go")
	s, err := Open(Config{Directory: goRoot, Location: seoul(t)})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range ops {
		if o.daily != nil {
			fmt.Fprintf(&script, "daily %s %s %s\n", flag(o.daily[0]), flag(o.daily[1]), flag(o.daily[2]))
			s.SetTextDaily(o.daily[0], o.daily[1], o.daily[2])
			continue
		}
		fmt.Fprintf(&script, "put %s %s %d %s\n", date, o.div, o.hash, o.text)
		if err := s.PutText(date, &protocol.TextPack{XType: o.div, Hash: o.hash, Text: o.text}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	javaRoot := filepath.Join(work, "java")
	if err := os.MkdirAll(javaRoot, 0700); err != nil {
		t.Fatal(err)
	}
	writeScript := filepath.Join(work, "write.txt")
	if err := os.WriteFile(writeScript, []byte(script.String()), 0600); err != nil {
		t.Fatal(err)
	}
	oracle("text", javaRoot, writeScript)
	sameTrees(t, javaRoot, goRoot)

	// Java TextCache is keyed by (div, hash) only, so each process reads one
	// date and every key once: the results then come from the files.
	type key struct {
		div  string
		hash int32
	}
	var keys []key
	seen := map[key]bool{}
	for _, o := range ops {
		if k := (key{o.div, o.hash}); o.daily == nil && !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	keys = append(keys, key{"service", 99}, key{"error", 99})
	for _, flags := range [][]bool{{false, false, false}, {true, true, true}, {true, false, true}} {
		for _, readDate := range []string{date, "20261014"} {
			var reads strings.Builder
			fmt.Fprintf(&reads, "daily %s %s %s\n", flag(flags[0]), flag(flags[1]), flag(flags[2]))
			var want []string
			g, err := Open(Config{Directory: goRoot, Location: seoul(t)})
			if err != nil {
				t.Fatal(err)
			}
			g.SetTextDaily(flags[0], flags[1], flags[2])
			for _, k := range keys {
				fmt.Fprintf(&reads, "get %s %s %d\n", readDate, k.div, k.hash)
				text, ok, err := g.Text(readDate, k.div, k.hash)
				if err != nil {
					t.Fatal(err)
				}
				value := "null"
				if ok {
					value = text
				}
				want = append(want, fmt.Sprintf("get %s %s %d=%s", readDate, k.div, k.hash, value))
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(work, "read.txt")
			if err := os.WriteFile(path, []byte(reads.String()), 0600); err != nil {
				t.Fatal(err)
			}
			got := strings.Split(strings.TrimSpace(oracle("text", javaRoot, path)), "\n")
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Fatalf("daily %v date %s reads\nJava:\n%s\nGo:\n%s", flags, readDate, strings.Join(got, "\n"), strings.Join(want, "\n"))
			}
			t.Logf("daily %v date %s: %d reads agree", flags, readDate, len(want))
		}
	}
}

// S4: *_mb keys size only new files; Java and Go create byte-identical index
// files for the same sizes and records, and each reads the other's files with
// default settings (MemHashBlock takes the size from the file length).
func TestJavaStorageInteropConfigIndexSizes(t *testing.T) {
	oracle := configOracle(t)
	work := t.TempDir()
	javaRoot, goRoot := filepath.Join(work, "java"), filepath.Join(work, "go")
	if err := os.MkdirAll(javaRoot, 0700); err != nil {
		t.Fatal(err)
	}
	// default service api ua login desc hmsg daily kv xlogid
	oracle("sizes", javaRoot, "2", "3", "4", "5", "6", "7", "8", "9", "8", "3")
	loc := seoul(t)
	s, err := Open(Config{Directory: goRoot, Location: loc, IndexSizes: IndexSizesMB{TextDefault: 2, TextService: 3, TextAPI: 4, TextUA: 5, TextLogin: 6, TextDesc: 7, TextHMsg: 8, TextDaily: 9}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetXLogIDIndexMB(3); err != nil {
		t.Fatal(err)
	}
	const date = "20261013"
	for _, div := range []string{"service", "apicall", "ua", "login", "desc", "hmsg", "sql"} {
		if err := s.PutText(date, &protocol.TextPack{XType: div, Hash: 1, Text: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.PutText(date, &protocol.TextPack{XType: "error", Hash: 1, Text: "x"}); err != nil {
		t.Fatal(err)
	}
	end := time.Date(2026, 10, 13, 9, 0, 0, 0, loc).UnixMilli()
	if err := s.PutXLog(&protocol.XLogPack{EndTime: end, TxID: 1, GxID: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutKeyValueContext(context.Background(), "space", "k", "v", -1, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	j, g := treeBytes(t, javaRoot), treeBytes(t, goRoot)
	compared := 0
	for name, jb := range j {
		gb, ok := g[name]
		if !ok {
			t.Errorf("Go did not create %s", name)
			continue
		}
		if !bytes.Equal(jb, gb) {
			t.Errorf("%s: Java %d bytes, Go %d bytes", name, len(jb), len(gb))
		}
		compared++
	}
	for name, size := range map[string]int{"00000000/text/text_service.hfile": 3, "00000000/text/text_apicall.hfile": 4, "00000000/text/text_ua.hfile": 5, "00000000/text/text_login.hfile": 6, "00000000/text/text_desc.hfile": 7, "00000000/text/text_hmsg.hfile": 8, "00000000/text/text_sql.hfile": 2, date + "/text/text.hfile": 9, "00000000/kv2/kv_space.hfile": 8, date + "/xlog/xlog_tid.hfile": 3, date + "/xlog/xlog_gid.hfile": 3} {
		if len(j[name]) != 1024+size<<20 {
			t.Errorf("Java %s is %d bytes", name, len(j[name]))
		}
	}
	t.Logf("compared %d Java index files with Go", compared)
	// Java reads the Go files with its default (1 MiB / 8 MiB) settings.
	if out := oracle("read-sizes", goRoot); !strings.Contains(out, "read-sizes ok") {
		t.Fatal(out)
	}
	// Go reads the Java files with its defaults.
	r, err := Open(Config{Directory: javaRoot, Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, div := range []string{"service", "apicall", "ua", "login", "desc", "hmsg", "sql", "error"} {
		if text, ok, err := r.Text(date, div, 1); err != nil || !ok || text != "x" {
			t.Fatal(div, text, ok, err)
		}
	}
}

// S2: one AutoDeleteScheduler.run() iteration per configuration phase, the
// later phases applied through its ConfObserver (history cleared), must
// remove the same files as Retention.Apply + RunOnce on a copy of the DB.
func TestJavaStorageInteropConfigPurge(t *testing.T) {
	oracle := configOracle(t)
	loc := seoul(t)
	work := t.TempDir()
	base := filepath.Join(work, "base")
	now := time.Now().In(loc)
	full := []string{"xlog/xlog.profile", "xlog/xlog_profile.hfile", "xlog/xlog_profile.kfile", "xlog/xlog.profile.conf", "xlog/xlog.service", "span/span.data", "tagcnt/java/x", "text/text.kfile", "sum/sum.sum", "visit/a.usr", "visit_hourly/a.usr", "counter/real.data", "counter/real.hfile", "counter/real.kfile", "counter/real.head", "counter/5m.data", "object/obj.data"}
	for _, ago := range []int{0, 1, 3, 5, 9, 12, 20, 35, 75, 150} {
		d := now.AddDate(0, 0, -ago).Format("20060102")
		for _, f := range full {
			touch(t, filepath.Join(base, d, f))
		}
	}
	touch(t, filepath.Join(base, "00000000/text/text_service.data"))
	// Each phase: the Java Configure fields set before that cycle.
	phases := []string{
		"mgr_purge_disk_usage_pct=0",
		"mgr_purge_profile_keep_days=2,mgr_purge_xlog_keep_days=4,mgr_purge_sum_data_days=3,mgr_purge_daily_text_days=0,mgr_purge_tag_counter_keep_days=11",
		"mgr_purge_enabled=false,mgr_purge_counter_keep_days=2",
		"mgr_purge_enabled=true,mgr_purge_counter_keep_days=10,mgr_purge_visitor_counter_keep_days=1,mgr_purge_realtime_counter_keep_days=8",
	}
	javaRoot, goRoot := filepath.Join(work, "java"), filepath.Join(work, "go")
	copyTree(t, base, javaRoot)
	copyTree(t, base, goRoot)
	javaOut := oracle(append([]string{"purge", javaRoot}, phases...)...)

	conf := map[string]int{"mgr_purge_enabled": 1, "mgr_purge_disk_usage_pct": 0, "mgr_purge_profile_keep_days": 10, "mgr_purge_xlog_keep_days": 30, "mgr_purge_counter_keep_days": 70, "mgr_purge_realtime_counter_keep_days": 70, "mgr_purge_visitor_counter_keep_days": 70, "mgr_purge_tag_counter_keep_days": 70, "mgr_purge_daily_text_days": 140, "mgr_purge_sum_data_days": 60}
	s, err := Open(Config{Directory: goRoot, Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	c, err := OpenCounters(goRoot, loc)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close(); c.Close() }()
	r := &Retention{Store: s, Counters: c}
	var goOut strings.Builder
	for i, phase := range phases {
		for _, kv := range strings.Split(phase, ",") {
			k, v, _ := strings.Cut(kv, "=")
			if v == "true" || v == "false" {
				conf[k] = map[string]int{"true": 1, "false": 0}[v]
				continue
			}
			n, err := strconv.Atoi(v)
			if err != nil {
				t.Fatal(err)
			}
			conf[k] = n
		}
		// The stub Configure has no derivation, so the effective values are the fields.
		r.Apply(conf["mgr_purge_enabled"] == 1, RetentionConfig{ProfileDays: conf["mgr_purge_profile_keep_days"], XLogDays: conf["mgr_purge_xlog_keep_days"], CounterDays: conf["mgr_purge_counter_keep_days"], RealtimeCounterDays: conf["mgr_purge_realtime_counter_keep_days"], TagDays: conf["mgr_purge_tag_counter_keep_days"], VisitorDays: conf["mgr_purge_visitor_counter_keep_days"], TextDays: conf["mgr_purge_daily_text_days"], SumDays: conf["mgr_purge_sum_data_days"], DiskUsagePct: conf["mgr_purge_disk_usage_pct"]})
		if _, err := r.RunOnce(context.Background(), time.Now()); err != nil {
			t.Fatal(i, err)
		}
		for _, f := range listFiles(t, goRoot) {
			fmt.Fprintf(&goOut, "phase %d %s\n", i, f)
		}
	}
	if strings.TrimSpace(javaOut) != strings.TrimSpace(goOut.String()) {
		t.Fatalf("purge listings differ\nJava:\n%s\nGo:\n%s", javaOut, goOut.String())
	}
	t.Logf("%d purge phases, %d listed file states agree", len(phases), strings.Count(javaOut, "\n"))
}
