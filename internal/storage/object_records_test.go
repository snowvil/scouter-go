package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func TestObjectRecordsOutOfOrderDuplicatesAndDetail(t *testing.T) {
	s, start := extraStore(t, Config{})
	ctx := context.Background()
	name := "/host/app"
	for _, offset := range []int64{550, 100, 100} {
		if err := s.PutStackContext(ctx, name, &protocol.StackPack{Time: start + offset, ObjHash: 42, Data: []byte{1, 2}}); err != nil {
			t.Fatal(err)
		}
		if err := s.PutBatchContext(ctx, name, &protocol.BatchPack{StartTime: start + offset - 10, ElapsedTime: 10, ObjHash: 42, ObjName: name, BatchJobID: "job"}); err != nil {
			t.Fatal(err)
		}
	}
	stacks, err := s.StacksContext(ctx, name, start, start+600)
	if err != nil || len(stacks) != 3 || stacks[0].Time != start+550 || stacks[2].Time != start+100 {
		t.Fatalf("stacks %+v %v", stacks, err)
	}
	batches, err := s.BatchesContext(ctx, name, start, start+600)
	if err != nil || len(batches) != 3 || batches[0].Position == batches[1].Position {
		t.Fatalf("batches %+v %v", batches, err)
	}
	got, err := s.BatchAtContext(ctx, name, start+550, batches[0].Position)
	if err != nil || got == nil || got.BatchJobID != "job" {
		t.Fatalf("detail %+v %v", got, err)
	}
	idx, err := os.ReadFile(filepath.Join(s.config.Directory, "20260101/host/app/stack.idx"))
	if err != nil || len(idx) != 39 || int64(binary.BigEndian.Uint64(idx)) != start+550 {
		t.Fatalf("idx %x %v", idx, err)
	}
	stacks[0].Data[0] = 99
	again, err := s.StacksContext(ctx, name, start, start+600)
	if err != nil || again[0].Data[0] != 1 {
		t.Fatal("borrowed stack")
	}
	if _, err = s.BatchAtContext(ctx, name, start+100, batches[0].Position+1); err == nil {
		t.Fatal("nonrecord detail position accepted")
	}
}
func TestObjectRecordsValidationAndLimits(t *testing.T) {
	s, start := extraStore(t, Config{MaxQueryRecords: 1})
	ctx := context.Background()
	for _, name := range []string{"", "/", "../escape", "/host/../escape", "/host//app", "/host\\app", "/host\x00app"} {
		if err := s.PutStackContext(ctx, name, &protocol.StackPack{Time: start}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("path %q %v", name, err)
		}
	}
	if err := s.PutBatchContext(ctx, "/host/app", &protocol.BatchPack{StartTime: math.MaxInt64, ElapsedTime: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("overflow %v", err)
	}
	if err := s.PutStackContext(ctx, "/host/app", nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := s.PutStackContext(ctx, "/host/app", &protocol.StackPack{Time: start + int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.StacksContext(ctx, "/host/app", start, start+1); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.BatchesContext(cancelled, "/host/app", start, start+1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestObjectRecordsJavaMidpointAndEarlyStop(t *testing.T) {
	s, start := extraStore(t, Config{})
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := s.PutStackContext(ctx, "/duplicate/app", &protocol.StackPack{Time: start + 100, Data: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.StacksContext(ctx, "/duplicate/app", start+100, start+100)
	if err != nil || len(got) != 2 || got[0].Data[0] != 1 || got[1].Data[0] != 2 {
		t.Fatalf("Java midpoint equality %+v %v", got, err)
	}
	for _, offset := range []int64{550, 100, 100} {
		if err = s.PutStackContext(ctx, "/unsorted/app", &protocol.StackPack{Time: start + offset}); err != nil {
			t.Fatal(err)
		}
	}
	got, err = s.StacksContext(ctx, "/unsorted/app", start, start+200)
	if err != nil || len(got) != 0 {
		t.Fatalf("Java stops at first >to %+v %v", got, err)
	}
}
func TestObjectRecordsCorruptionAndDate(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"stack", "batch"} {
		t.Run(kind, func(t *testing.T) {
			s, start := extraStore(t, Config{})
			if kind == "stack" {
				if err := s.PutStackContext(ctx, "/host/app", &protocol.StackPack{Time: start + 1}); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := s.PutBatchContext(ctx, "/host/app", &protocol.BatchPack{StartTime: start, ElapsedTime: 1}); err != nil {
					t.Fatal(err)
				}
			}
			idx := filepath.Join(s.config.Directory, "20260101/host/app", kind+".idx")
			f, err := os.OpenFile(idx, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = f.Write([]byte{1})
			f.Close()
			if kind == "stack" {
				_, err = s.StacksContext(ctx, "/host/app", start, start+1)
			} else {
				_, err = s.BatchesContext(ctx, "/host/app", start, start+1)
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("partial idx %v", err)
			}
		})
	}
	s, start := extraStore(t, Config{})
	if _, err := s.StacksContext(ctx, "/host/app", start+86399999, start+86400000); !errors.Is(err, ErrInvalid) {
		t.Fatalf("crossdate %v", err)
	}
	if err := os.MkdirAll(filepath.Join(s.config.Directory, "20260101"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(s.config.Directory, "20260101/escape")); err != nil {
		t.Fatal(err)
	}
	if err := s.PutStackContext(ctx, "/escape/app", &protocol.StackPack{Time: start}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("symlink escape %v", err)
	}
}
func TestObjectRecordsFileCacheEviction(t *testing.T) {
	s, start := extraStore(t, Config{})
	ctx := context.Background()
	// Populate all sixteen cache slots, then open a lexically earlier object pair.
	for n := 0; n < 8; n++ {
		name := fmt.Sprintf("/zz%d/app", n)
		if err := s.PutStackContext(ctx, name, &protocol.StackPack{Time: start}); err != nil {
			t.Fatal(err)
		}
	}
	for n := 0; n < 3; n++ {
		if err := s.PutStackContext(ctx, "/aa/app", &protocol.StackPack{Time: start + int64(n)}); err != nil {
			t.Fatalf("evicted idx write %v", err)
		}
	}
	for n := 0; n < 8; n++ {
		if _, err := s.StacksContext(ctx, fmt.Sprintf("/zz%d/app", n), start, start+1); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.StacksContext(ctx, "/aa/app", start, start+3)
	if err != nil || len(got) != 3 {
		t.Fatalf("evicted idx scan %+v %v", got, err)
	}
}
