package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func objectWithHash(hash int32, name string) *protocol.ObjectPack {
	p := sampleObject(name)
	p.ObjHash = hash
	return p
}

func objectHashes(t *testing.T, s *Store, date string) []int32 {
	t.Helper()
	objects, err := s.Objects(date)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]int32, 0, len(objects))
	for _, o := range objects {
		out = append(out, o.ObjHash)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func equalHashes(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRemoveObjectsOnMarksIndexOnly(t *testing.T) {
	dir := t.TempDir()
	c := Config{Directory: dir, Location: time.UTC}
	s, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	const date, other = "20261013", "20261012"
	for _, h := range []int32{1, 2, 3} {
		if err = s.PutObject(date, objectWithHash(h, "obj")); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.PutObject(other, objectWithHash(2, "other")); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// 99 was never live; 2 is marked once. Java: index.get >= 0 then delete.
	if n, err := s.RemoveObjectsOn(ctx, date, []int32{2, 99}); err != nil || n != 1 {
		t.Fatalf("remove: %d %v", n, err)
	}
	if n, err := s.RemoveObjectsOn(ctx, date, []int32{2}); err != nil || n != 0 {
		t.Fatalf("second remove: %d %v", n, err)
	}
	if got := objectHashes(t, s, date); !equalHashes(got, []int32{1, 3}) {
		t.Fatal("after remove", got)
	}
	if got := objectHashes(t, s, other); !equalHashes(got, []int32{2}) {
		t.Fatal("other date touched", got)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	// obj.data keeps every 4096-byte slot: the delete is an index flag only.
	if st, err := os.Stat(filepath.Join(dir, date, "object", "obj.data")); err != nil || st.Size() != 3*4100 {
		t.Fatalf("data rewritten: %v %v", st, err)
	}
	s, err = Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := objectHashes(t, s, date); !equalHashes(got, []int32{1, 3}) {
		t.Fatal("after reopen", got)
	}
	// A later PutObject of the removed hash appends a new slot, as Java does
	// once index.get returns -1 for the deleted key.
	if err = s.PutObject(date, objectWithHash(2, "again")); err != nil {
		t.Fatal(err)
	}
	objects, err := s.Objects(date)
	if err != nil || len(objects) != 3 {
		t.Fatal(objects, err)
	}
	for _, o := range objects {
		if o.ObjHash == 2 && o.ObjName != "again" {
			t.Fatal("stale slot returned", o)
		}
	}
	if err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(filepath.Join(dir, date, "object", "obj.data")); err != nil || st.Size() != 4*4100 {
		t.Fatalf("re-add slot: %v %v", st, err)
	}
}

func TestRemoveObjectUsesCurrentDate(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(Config{Directory: t.TempDir(), Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	today := JavaDate(time.Now().UnixMilli(), loc)
	yesterday := JavaDate(time.Now().Add(-24*time.Hour).UnixMilli(), loc)
	for _, date := range []string{today, yesterday} {
		if err = s.PutObject(date, objectWithHash(7, "x")); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.RemoveObject(context.Background(), 7); err != nil {
		t.Fatal(err)
	}
	if got := objectHashes(t, s, today); len(got) != 0 {
		t.Fatal("today still lists", got)
	}
	// ObjectWR.remove only touches curDbw (today); earlier dates keep the object.
	if got := objectHashes(t, s, yesterday); !equalHashes(got, []int32{7}) {
		t.Fatal("yesterday changed", got)
	}
	// A hash that is not live is a silent no-op, as in Java.
	if err = s.RemoveObject(context.Background(), 8); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveObjectsOnWithoutDBAndErrors(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// Intended difference: no empty object DB is created for a date without one.
	if n, err := s.RemoveObjectsOn(ctx, "20261013", []int32{1}); err != nil || n != 0 {
		t.Fatalf("absent date: %d %v", n, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "20261013")); !os.IsNotExist(err) {
		t.Fatal("date directory created by remove", err)
	}
	if _, err := s.RemoveObjectsOn(ctx, "2026101", []int32{1}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := s.RemoveObjectsOn(ctx, "20261013", make([]int32, maxRemoveObjects+1)); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if n, err := s.RemoveObjectsOn(ctx, "20261013", nil); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if err = s.PutObject("20261013", objectWithHash(1, "x")); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.RemoveObjectsOn(cancelled, "20261013", []int32{1}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if got := objectHashes(t, s, "20261013"); !equalHashes(got, []int32{1}) {
		t.Fatal("cancelled call removed", got)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RemoveObjectsOn(ctx, "20261013", []int32{1}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := s.RemoveObject(ctx, 1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestRemoveObjectsConcurrentWithPutAndQuery(t *testing.T) {
	s, err := Open(Config{Directory: t.TempDir(), Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const date = "20261013"
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if err := s.PutObject(date, objectWithHash(int32(i%20), "obj")); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if _, err := s.Objects(date); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if _, err := s.RemoveObjectsOn(context.Background(), date, []int32{int32(i % 20)}); err != nil {
				errs <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if _, err := s.Objects(date); err != nil {
		t.Fatal(err)
	}
}
