package legacyindex

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func sealedPair(t *testing.T, timed bool) (string, Seal) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "x")
	idx, err := Open(path, 64, timed, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	for n := int64(1); n <= 20; n++ {
		if _, err = idx.Put(key8(recoveryBase+n), []byte{byte(n)}); err != nil {
			t.Fatal(err)
		}
	}
	seal, err := idx.CloseSealed()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = idx.CloseSealed(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	return path, seal
}

func key8(v int64) []byte {
	b := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		b[i] = byte(v)
		v >>= 8
	}
	return b
}

func TestOpenSealedSkipsWalkOnlyForExactPair(t *testing.T) {
	for _, timed := range []bool{false, true} {
		path, seal := sealedPair(t, timed)
		idx, err := OpenSealed(path, 64, timed, time.UTC, nil, &seal)
		if err != nil || idx.OpenCheck() != CheckSealed {
			t.Fatal(timed, err)
		}
		if v, err := idx.Get(key8(recoveryBase + 7)); err != nil || len(v) != 1 || v[0] != 7 {
			t.Fatal(v, err)
		}
		if _, err = idx.Put(key8(recoveryBase+21), []byte{21}); err != nil {
			t.Fatal(err)
		}
		next, err := idx.CloseSealed()
		if err != nil || next == seal {
			t.Fatal(next, err)
		}
		// The old seal no longer matches the appended pair.
		if idx, err = OpenSealed(path, 64, timed, time.UTC, nil, &seal); err != nil || idx.OpenCheck() != CheckWalked {
			t.Fatal(err)
		}
		if err = idx.Close(); err != nil {
			t.Fatal(err)
		}
		// Without a seal, Open always walks.
		if idx, err = OpenChecked(path, 64, timed, time.UTC, nil); err != nil || idx.OpenCheck() != CheckWalked {
			t.Fatal(err)
		}
		idx.Close()
	}
}

func TestOpenSealedFallsBackOnMismatch(t *testing.T) {
	cases := map[string]func(path string, seal *Seal){
		"header crc": func(_ string, s *Seal) { s.HeaderCRC++ },
		"key size":   func(_ string, s *Seal) { s.KeyBytes++ },
		"key mtime":  func(_ string, s *Seal) { s.KeyMTime++ },
		"hash mtime": func(_ string, s *Seal) { s.HashMTime-- },
		"head out of range": func(path string, s *Seal) {
			// A sealed header whose head points past the key file: the
			// O(1) range check refuses it and the walk reports corruption.
			b, err := os.ReadFile(path + ".hfile")
			if err != nil {
				t.Fatal(err)
			}
			info, _ := os.Stat(path + ".hfile")
			copy(b[headerBytes:], []byte{0, 0, 0, 0x7f, 0xff})
			if err = os.WriteFile(path+".hfile", b, 0600); err != nil {
				t.Fatal(err)
			}
			os.Chtimes(path+".hfile", info.ModTime(), info.ModTime())
			idx := &Index{header: b}
			*s = Seal{KeyBytes: s.KeyBytes, KeyMTime: s.KeyMTime, HashBytes: s.HashBytes, HashMTime: info.ModTime().UnixNano(), HeaderCRC: idx.headerCRC()}
		},
	}
	for name, mutate := range cases {
		path, seal := sealedPair(t, false)
		mutate(path, &seal)
		idx, err := OpenSealed(path, 64, false, time.UTC, nil, &seal)
		if name == "head out of range" {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatal(name, err)
			}
			continue
		}
		if err != nil || idx.OpenCheck() != CheckWalked {
			t.Fatal(name, err)
		}
		idx.Close()
	}
}

func TestCloseSealedReadOnly(t *testing.T) {
	path, _ := sealedPair(t, false)
	ro, err := OpenReadOnly(path, false, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ro.CloseSealed(); !errors.Is(err, ErrReadOnly) {
		t.Fatal(err)
	}
}

func TestOpenTTLSealed(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "kv")
	idx, err := OpenTTLContext(ctx, path, 64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = idx.PutContext(ctx, []byte("k"), []byte("v"), -1); err != nil {
		t.Fatal(err)
	}
	seal, err := idx.CloseSealed()
	if err != nil {
		t.Fatal(err)
	}
	if idx, err = OpenTTLSealed(ctx, path, 64, &seal); err != nil || idx.OpenCheck() != CheckSealed {
		t.Fatal(err)
	}
	if v, err := idx.GetContext(ctx, []byte("k"), 1); err != nil || string(v) != "v" {
		t.Fatal(string(v), err)
	}
	if _, err = idx.PutContext(ctx, []byte("k2"), []byte("v"), -1); err != nil {
		t.Fatal(err)
	}
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}
	if idx, err = OpenTTLSealed(ctx, path, 64, &seal); err != nil || idx.OpenCheck() != CheckWalked {
		t.Fatal(err)
	}
	idx.Close()
}
