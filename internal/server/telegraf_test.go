package server

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTelegrafSampleActualJava(t *testing.T) {
	source := os.Getenv("SCOUTER_SOURCE")
	if source == "" {
		source = filepath.Clean("../../../scouter")
	}
	git := exec.Command("git", "-C", source, "rev-parse", "HEAD")
	head, err := git.Output()
	if err != nil || strings.TrimSpace(string(head)) != "dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5" {
		t.Fatalf("pinned source: %s %v", head, err)
	}
	clean, err := exec.Command("git", "-C", source, "status", "--porcelain").Output()
	if err != nil || len(clean) != 0 {
		t.Fatalf("source modified: %v", err)
	}
	dir := t.TempDir()
	helper := filepath.Join(dir, "TelegrafOracle.java")
	code := `import java.nio.file.*; import scouter.server.support.telegraf.TgConfig; public class TelegrafOracle { public static void main(String[] a) throws Exception { Files.write(Paths.get(a[0]), TgConfig.getSampleContents().getBytes("UTF-8")); } }`
	if err := os.WriteFile(helper, []byte(code), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"--release", "8", "-d", dir, helper, filepath.Join(source, "scouter.common/src/main/java/scouter/lang/DeltaType.java")}
	for _, name := range []string{"TgConfig", "TgmConfig", "TgCounterMapping"} {
		args = append(args, filepath.Join(source, "scouter.server/src/main/java/scouter/server/support/telegraf", name+".java"))
	}
	if out, err := exec.Command("javac", args...).CombinedOutput(); err != nil {
		t.Fatalf("javac: %s %v", out, err)
	}
	path := filepath.Join(dir, "sample.xml")
	if out, err := exec.Command("java", "-cp", dir, "TelegrafOracle", path).CombinedOutput(); err != nil {
		t.Fatalf("java: %s %v", out, err)
	}
	actual, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(actual, []byte(telegrafSample)) {
		t.Fatalf("sample differs: %v", err)
	}
	cfg := Config{Directory: dir, Location: time.UTC, TelegrafConfigurationPath: filepath.Join(dir, "missing.xml")}
	metadata, err := runtimeMetadataWith(cfg, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	text, err := metadata.ConfigurationText(context.Background(), "telegraf")
	if err != nil || text != telegrafSample {
		t.Fatalf("fallback: %v", err)
	}
	if err := os.WriteFile(cfg.TelegrafConfigurationPath, []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	text, err = metadata.ConfigurationText(context.Background(), "telegraf")
	if err != nil || text != "changed" {
		t.Fatalf("current file: %s %v", text, err)
	}
}
