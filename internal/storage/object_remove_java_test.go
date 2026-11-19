package storage

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestJavaStorageInteropObjectRemove runs the original ObjectData/ObjectIndex
// (tools/java-storage/ObjectRemoveOracle.java) against Go in both directions:
// Java write+delete -> Go read -> Go delete -> Java read, Go write+delete ->
// Java read -> Java delete -> Go read, and the same delete applied by Java and
// by Go to copies of one Java DB must leave identical files. The name matches
// the `make storage-check` filter.
func TestJavaStorageInteropObjectRemove(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	source := os.Getenv("SCOUTER_SOURCE")
	if source == "" {
		source = filepath.Join(repo, "..", "scouter")
	}
	if _, err = os.Stat(filepath.Join(source, "scouter.server")); err != nil {
		t.Skip("pinned Java source unavailable; object remove oracle not run")
	}
	work := t.TempDir()
	classes := filepath.Join(work, "compiled")
	output, err := exec.Command("python3", filepath.Join(repo, "scripts", "java-storage.py"), "--classes-dir", classes, "--source", source).CombinedOutput()
	if err != nil {
		t.Fatalf("actual Java/Scala storage compilation: %v\n%s", err, output)
	}
	classpath := strings.TrimSpace(string(output))
	oracle := func(args ...string) {
		t.Helper()
		output, err := exec.Command("java", append([]string{"-Duser.timezone=Asia/Seoul", "-cp", classpath, "ObjectRemoveOracle"}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("Java object oracle %v: %v\n%s", args, err, output)
		}
		t.Log(strings.TrimSpace(string(output)))
	}
	const date = "20260101"
	ctx := context.Background()
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	open := func(root string) *Store {
		t.Helper()
		s, err := Open(Config{Directory: root, Location: loc})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	t.Run("Java-write-delete/Go-read-delete/Java-read", func(t *testing.T) {
		root := filepath.Join(work, "java-db")
		oracle("generate", root) // objects 1..5, Java ObjectIndex.delete(3)
		s := open(root)
		if got := objectHashes(t, s, date); !equalHashes(got, []int32{1, 2, 4, 5}) {
			t.Fatal("Go read after Java delete", got)
		}
		if n, err := s.RemoveObjectsOn(ctx, date, []int32{5, 3}); err != nil || n != 1 {
			t.Fatalf("Go remove: %d %v", n, err)
		}
		if got := objectHashes(t, s, date); !equalHashes(got, []int32{1, 2, 4}) {
			t.Fatal("Go read after Go delete", got)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		oracle("verify", root, "1,2,4", "3,5")
	})
	t.Run("Go-write-delete/Java-read-delete/Go-read", func(t *testing.T) {
		root := filepath.Join(work, "go-db")
		s := open(root)
		for _, h := range []int32{1, 2, 3, 4, 5} {
			p := objectWithHash(h, "/remove-oracle-"+strconv.Itoa(int(h)))
			p.Address, p.Version = "127.0.0.1", "oracle"
			if err := s.PutObject(date, p); err != nil {
				t.Fatal(err)
			}
		}
		if n, err := s.RemoveObjectsOn(ctx, date, []int32{2}); err != nil || n != 1 {
			t.Fatalf("Go remove: %d %v", n, err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		oracle("verify", root, "1,3,4,5", "2")
		oracle("remove", root, "4")
		oracle("verify", root, "1,3,5", "2,4")
		s = open(root)
		defer s.Close()
		if got := objectHashes(t, s, date); !equalHashes(got, []int32{1, 3, 5}) {
			t.Fatal("Go read after Java delete", got)
		}
		// Re-adding a Java-deleted hash appends a fresh slot the Java reader lists again.
		p := objectWithHash(4, "/remove-oracle-4")
		p.Address, p.Version = "127.0.0.1", "oracle"
		if err := s.PutObject(date, p); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		oracle("verify", root, "1,3,4,5", "2")
	})
	t.Run("Java-delete/Go-delete/identical-files", func(t *testing.T) {
		base := filepath.Join(work, "same-base")
		oracle("generate", base)
		javaRoot, goRoot := filepath.Join(work, "same-java"), filepath.Join(work, "same-go")
		copyTree(t, base, javaRoot)
		copyTree(t, base, goRoot)
		oracle("remove", javaRoot, "5")
		s := open(goRoot)
		if n, err := s.RemoveObjectsOn(ctx, date, []int32{5}); err != nil || n != 1 {
			t.Fatalf("Go remove: %d %v", n, err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"obj.kfile", "obj.hfile", "obj.data"} {
			want, err := os.ReadFile(filepath.Join(javaRoot, date, "object", name))
			if err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(filepath.Join(goRoot, date, "object", name))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("%s differs after the same delete: go %d bytes, java %d bytes", name, len(got), len(want))
			}
		}
		oracle("verify", goRoot, "1,2,4", "3,5")
	})
}

func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, 0600)
	})
	if err != nil {
		t.Fatal(err)
	}
}
