package legacyindex

import (
	"context"
	"path/filepath"
	"testing"
)

type deepCall struct {
	op     DeepSearchOp
	key    string
	looped int
}

// Java IndexKeyFile.get counts every chain record (deleted included) and
// compares only after a miss; IndexKeyFile2 does the same for get/setTTL and
// for every updateOrPut that walks the whole chain.
func TestDeepSearchLooping(t *testing.T) {
	ctx := context.Background()
	// 5-byte hash area: one bucket, every key on one chain.
	idx, err := Open(filepath.Join(t.TempDir(), "x"), 5, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	var calls []deepCall
	idx.SetDeepSearch(func(op DeepSearchOp, key []byte, looped int) {
		calls = append(calls, deepCall{op, string(key), looped})
	})
	for _, k := range []string{"a", "b", "c"} {
		if _, err := idx.Put([]byte(k), []byte{1}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := idx.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if v, err := idx.LookupContext(ctx, []byte("a")); err != nil || v == nil {
		t.Fatal(v, err)
	}
	if v, err := idx.LookupContext(ctx, []byte("b")); err != nil || v != nil {
		t.Fatal("deleted", v, err)
	}
	if _, err := idx.Get([]byte("zz")); err != nil { // hasKey-style: no report
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0] != (deepCall{DeepGet, "b", 3}) {
		t.Fatal(calls)
	}

	ttl, err := OpenTTL(filepath.Join(t.TempDir(), "kv"), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer ttl.Close()
	calls = nil
	ttl.SetDeepSearch(func(op DeepSearchOp, key []byte, looped int) {
		calls = append(calls, deepCall{op, string(key), looped})
	})
	for _, k := range []string{"a", "b"} {
		if _, err := ttl.UpdateOrPutContext(ctx, []byte(k), []byte{1, 2, 3, 4, 5}, -1); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ttl.UpdateOrPutContext(ctx, []byte("a"), []byte{9, 9, 9, 9, 9}, -1); err != nil {
		t.Fatal(err)
	}
	// Different value width: Java returns put() at the match without logging.
	if _, err := ttl.UpdateOrPutContext(ctx, []byte("b"), []byte{1}, -1); err != nil {
		t.Fatal(err)
	}
	if v, err := ttl.GetContext(ctx, []byte("a"), 0); err != nil || v == nil {
		t.Fatal(v, err)
	}
	if _, err := ttl.GetContext(ctx, []byte("q"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ttl.SetTTLContext(ctx, []byte("q"), 0, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := ttl.SetTTLContext(ctx, []byte("a"), 0, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := ttl.DeleteContext(ctx, []byte("q")); err != nil {
		t.Fatal(err)
	}
	want := []deepCall{{DeepUpdate, "a", 0}, {DeepUpdate, "b", 1}, {DeepUpdate, "a", 2}, {DeepGet, "q", 4}, {DeepUpdate, "q", 4}}
	if len(calls) != len(want) {
		t.Fatal(calls)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatal(i, calls)
		}
	}
}
