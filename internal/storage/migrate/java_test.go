package migrate

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestJavaOracleMigration generates DBs with the pinned original Java/Scala
// writers, migrates them, and verifies byte-copied destinations again with the
// original Java readers. Missing tools or source skip the test; a skip is not
// interoperability evidence.
func TestJavaOracleMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode: actual Java oracle migration not run")
	}
	repo, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	source := os.Getenv("SCOUTER_SOURCE")
	if source == "" {
		source = filepath.Join(repo, "..", "scouter")
	}
	for _, tool := range []string{"python3", "java", "javac"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("actual Java oracle unavailable: %s: %v", tool, err)
		}
	}
	if _, err = os.Stat(filepath.Join(source, "scouter.server")); err != nil {
		t.Skipf("pinned Scouter source unavailable (%v); Java oracle migration not run", err)
	}
	work := t.TempDir()
	oracle := func(mode, dir string) string {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		out, err := exec.CommandContext(ctx, "python3", filepath.Join(repo, "scripts", "java-storage.py"), mode, dir, "--source", source).CombinedOutput()
		if err != nil {
			t.Errorf("java-storage.py %s %s: %v\n%s", mode, dir, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	generated := map[string]string{"base": "--generate", "extra": "--generate-extra", "ttl": "--generate-ttl", "local": "--generate-local-store"}
	var wg sync.WaitGroup
	for name, mode := range generated {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Logf("%s: %s", mode, oracle(mode, filepath.Join(work, "java", name)))
		}()
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	loc := seoul(t)
	now := time.Now().Unix()
	roots := []string{"base/plain", "base/gzip", "extra", "ttl", "local"}
	for _, name := range roots {
		src := filepath.Join(work, "java", filepath.FromSlash(name))
		dst := filepath.Join(work, "migrated", strings.ReplaceAll(name, "/", "-"))
		if err := os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
			t.Fatal(err)
		}
		ageAll(t, src) // so that precopy can reuse the files below
		before := capture(t, src)
		rep, err := Inspect(context.Background(), InspectOptions{Root: src, Location: loc, Now: now})
		if err != nil || rep.HasErrors() || rep.Summary.Warnings != 0 {
			t.Fatalf("%s inspect %v %+v", name, err, rep.Problems)
		}
		m, err := Migrate(context.Background(), MigrateOptions{Src: src, Dst: dst, Location: loc, Now: now})
		if err != nil || m.Status != ManifestCompleted {
			t.Fatalf("%s migrate %v %+v", name, err, m)
		}
		v, err := Verify(context.Background(), VerifyOptions{Src: src, Dst: dst, Location: loc, Now: now})
		if err != nil || v.Failed() {
			t.Fatalf("%s verify %v %+v", name, err, v.Problems)
		}
		assertUnchanged(t, before, src)
		// precopy + finish of the same Java DB gives the same manifest and bytes.
		incDst := filepath.Join(work, "incremental", strings.ReplaceAll(name, "/", "-"))
		if err := os.MkdirAll(filepath.Dir(incDst), 0700); err != nil {
			t.Fatal(err)
		}
		incOpt := MigrateOptions{Src: src, Dst: incDst, Location: loc, Now: now}
		if st, err := Precopy(context.Background(), incOpt); err != nil || st.Last.PendingGroups != 0 {
			t.Fatalf("%s precopy %v %+v", name, err, st)
		}
		inc, err := Finish(context.Background(), incOpt)
		if err != nil || inc.Incremental.Finish.CopiedFiles != 0 || inc.Incremental.Finish.InspectedGroups != 0 {
			t.Fatalf("%s finish %v %+v", name, err, inc)
		}
		sameManifest(t, inc, m)
		sameDst(t, incDst, dst)
		assertUnchanged(t, before, src)
		t.Logf("%s: %d files, statuses %v, dates %v, permanent %v, query compared %v", name, len(m.Files), rep.Summary.Statuses, rep.Summary.Dates, rep.Summary.Permanent, m.Query.Compared)
	}
	// The original readers may flush headers, so they only read further copies.
	copyTree := func(from, to string) {
		if err := os.MkdirAll(filepath.Dir(to), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.CopyFS(to, os.DirFS(from)); err != nil {
			t.Fatal(err)
		}
	}
	migrated := func(name string) string { return filepath.Join(work, "migrated", name) }
	copyTree(migrated("base-plain"), filepath.Join(work, "java-verify", "base", "plain"))
	copyTree(migrated("base-gzip"), filepath.Join(work, "java-verify", "base", "gzip"))
	copyTree(migrated("extra"), filepath.Join(work, "java-verify", "extra"))
	copyTree(migrated("ttl"), filepath.Join(work, "java-verify", "ttl"))
	t.Logf("--verify: %s", oracle("--verify", filepath.Join(work, "java-verify", "base")))
	t.Logf("--verify-extra: %s", oracle("--verify-extra", filepath.Join(work, "java-verify", "extra")))
	t.Logf("--verify-ttl: %s", oracle("--verify-ttl", filepath.Join(work, "java-verify", "ttl")))
	t.Log("local-store: java-storage.py has no verify mode; checked by Go inspect, checksum and KV query comparison only")
}
