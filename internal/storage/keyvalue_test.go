package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestKeyValueTTLDeleteAndRestart(t *testing.T) {
	s, _ := extraStore(t, Config{HashSizeBytes: 100})
	ctx := context.Background()
	now := int64(1790000000)
	for _, key := range []string{"a", "한국어"} {
		ok, err := s.PutKeyValueContext(ctx, "space", key, "value", 10, now)
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		got, found, err := s.KeyValueContext(ctx, "space", key, now+10)
		if err != nil || !found || got != "value" {
			t.Fatal(got, found, err)
		}
		_, found, err = s.KeyValueContext(ctx, "space", key, now+11)
		if err != nil || found {
			t.Fatal("expiry", found, err)
		}
	}
	ok, err := s.PutKeyValueContext(ctx, "space", "a", "latest", -1, now)
	if err != nil || !ok {
		t.Fatal(err)
	}
	deleted, err := s.DeleteKeyValueContext(ctx, "space", "a")
	if err != nil || deleted != 1 {
		t.Fatal(deleted, err)
	}
	_, found, err := s.KeyValueContext(ctx, "space", "a", now)
	if err != nil || found {
		t.Fatal("deleted fallback", found, err)
	}
	if deleted, err = s.DeleteKeyValueContext(ctx, "space", "a"); err != nil || deleted != 0 {
		t.Fatal(deleted, err)
	}
	ok, err = s.PutKeyValueContext(ctx, "space", "a", "revived", -2, now)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if ok, err = s.SetKeyValueTTLContext(ctx, "space", "a", 0, now); err != nil || !ok {
		t.Fatal(err)
	}
	if _, found, err = s.KeyValueContext(ctx, "space", "a", now+1); err != nil || found {
		t.Fatal(found, err)
	}
	// Java wrapper reports success even for absent/expired key, no revival.
	if ok, err = s.SetKeyValueTTLContext(ctx, "space", "absent", 99, now); err != nil || !ok {
		t.Fatal(err)
	}
	if ok, err = s.PutKeyValueContext(ctx, "space", "permanent", "kept", -1, now); err != nil || !ok {
		t.Fatal(err)
	}
	root := s.config.Directory
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Config{Directory: root, Location: time.UTC, HashSizeBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, found, err := reopened.KeyValueContext(ctx, "space", "permanent", now+100)
	if err != nil || !found || got != "kept" {
		t.Fatal(got, found, err)
	}
	for _, suffix := range []string{".kvdata", ".k2file", ".hfile"} {
		if _, err = os.Stat(filepath.Join(root, "00000000", "kv2", "kv_space"+suffix)); err != nil {
			t.Fatal(err)
		}
	}
}
func TestKeyValueBoundsMissingPathsAndCancellation(t *testing.T) {
	s, _ := extraStore(t, Config{MaxRecordBytes: 16, HashSizeBytes: 100})
	ctx := context.Background()
	if _, found, err := s.KeyValueContext(ctx, "missing", "key", 1); err != nil || found {
		t.Fatal(found, err)
	}
	if _, err := os.Stat(filepath.Join(s.config.Directory, "00000000")); !os.IsNotExist(err) {
		t.Fatal("read created namespace", err)
	}
	for _, space := range []string{"../escape", "/tmp/evil", "a\\b", "a\x00b"} {
		if _, err := s.PutKeyValueContext(ctx, space, "k", "v", -1, 1); !errors.Is(err, ErrInvalid) {
			t.Fatal(space, err)
		}
	}
	if _, err := s.PutKeyValueContext(ctx, "space", "k", strings.Repeat("v", 17), -1, 1); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := s.PutKeyValueContext(ctx, "space", strings.Repeat("k", 32768), "v", -1, 1); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := s.PutKeyValueContext(ctx, "space", "k", "v", maxKVTime, 1); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	for _, args := range [][3]string{{"", "k", "v"}, {"s", "", "v"}, {"s", "k", ""}} {
		if ok, err := s.PutKeyValueContext(ctx, args[0], args[1], args[2], -1, 1); ok || err != nil {
			t.Fatal(ok, err)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.PutKeyValueContext(cancelled, "space", "k", "v", -1, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, err := s.KeyValueContext(cancelled, "space", "k", 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.DeleteKeyValueContext(cancelled, "space", "k"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(s.config.Directory, "00000000"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(s.config.Directory, "00000000", "kv2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutKeyValueContext(ctx, "space", "k", "v", -1, 1); !errors.Is(err, ErrInvalid) {
		t.Fatal("symlink", err)
	}
}

func TestKeyValueCacheEvictionAndCorruptDataExtent(t *testing.T) {
	s, _ := extraStore(t, Config{HashSizeBytes: 100})
	ctx := context.Background()
	for i := 0; i < 17; i++ {
		space := string(rune('a' + i))
		if ok, err := s.PutKeyValueContext(ctx, space, "key", "value", -1, 1); err != nil || !ok {
			t.Fatal(space, err)
		}
	}
	if len(s.ttlIndexes) > 16 {
		t.Fatal("unbounded TTL cache", len(s.ttlIndexes))
	}
	if v, found, err := s.KeyValueContext(ctx, "a", "key", 1); err != nil || !found || v != "value" {
		t.Fatal("evicted index", v, found, err)
	}
	path := filepath.Join(s.config.Directory, "00000000", "kv2", "kv_a.kvdata")
	f, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte{0x7f, 0xff, 0xff, 0xff}, 0)
	if closeErr := f.Close(); err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	if _, _, err := s.KeyValueContext(ctx, "a", "key", 1); !errors.Is(err, ErrCorrupt) {
		t.Fatal("corrupt extent", err)
	}
}
