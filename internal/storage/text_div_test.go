package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// permTextDivsOnDisk lists the divs with a permanent index file.
func permTextDivsOnDisk(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "00000000", "text"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var divs []string
	for _, e := range entries {
		if name, ok := strings.CutSuffix(e.Name(), ".kfile"); ok {
			divs = append(divs, strings.TrimPrefix(name, "text_"))
		}
	}
	sort.Strings(divs)
	return divs
}

// R7: a TextPack of a div the original never writes created a permanent
// 00000000/text/text_<div> index per div without a bound. New unknown divs
// are bounded (DefaultMaxUnknownTextDivs); the texts over it are refused
// with ErrTextDivLimit (an ErrLimit) and create no file.
func TestUnknownTextDivsBounded(t *testing.T) {
	s, root := liveStore(t, Config{HashSizeBytes: 4096})
	const date = "20261023"
	limited := 0
	for i := 0; i < 1000; i++ {
		err := s.PutText(date, &protocol.TextPack{XType: fmt.Sprintf("x%d", i), Hash: int32(i + 1), Text: "t"})
		switch {
		case err == nil:
		case errors.Is(err, ErrTextDivLimit) && errors.Is(err, ErrLimit):
			limited++
		default:
			t.Fatal(i, err)
		}
	}
	if err := s.flushTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(permTextDivsOnDisk(t, root)); got != DefaultMaxUnknownTextDivs {
		t.Fatalf("permanent indexes %d, want %d", got, DefaultMaxUnknownTextDivs)
	}
	if limited != 1000-DefaultMaxUnknownTextDivs {
		t.Fatalf("limited %d", limited)
	}
	// An admitted unknown div keeps storing new keys; a refused one is not
	// stored (and never becomes readable).
	putText(t, s, date, "x0", 99, "more")
	wantText(t, s, date, "x0", 99, "more", true)
	wantText(t, s, date, "x999", 1000, "", false)
	// An empty permanent text is ignored as before, also for a refused div.
	if err := s.PutText(date, &protocol.TextPack{XType: "y-empty", Hash: 1}); err != nil {
		t.Fatal(err)
	}
}

// Every div of the original (scouter.lang.TextTypes plus the server's
// "table"/"city") is stored as before after the unknown-div bound is
// reached, permanent or daily.
func TestKnownTextDivsUnbounded(t *testing.T) {
	s, root := liveStore(t, Config{HashSizeBytes: 4096})
	s.SetMaxUnknownTextDivs(1)
	const date = "20261023"
	putText(t, s, date, "custom", 1, "c")
	if err := s.PutText(date, &protocol.TextPack{XType: "custom2", Hash: 1, Text: "c"}); !errors.Is(err, ErrTextDivLimit) {
		t.Fatal("second unknown div", err)
	}
	known := []string{"error", "apicall", "method", "service", "sql", "object", "referer", "ua", "group", "city", "table", "maria", "login", "desc", "web", "hmsg", "stackelem"}
	for i, div := range known {
		putText(t, s, date, div, int32(i+1), "v-"+div)
	}
	for i, div := range known {
		wantText(t, s, date, div, int32(i+1), "v-"+div, true)
	}
	// error is daily, the others permanent with the default settings.
	for _, div := range permTextDivsOnDisk(t, root) {
		if div == "custom2" || div == "error" {
			t.Fatal("unexpected permanent index", div)
		}
	}
	if got := s.TextDivsLimited(); got != 1 {
		t.Fatal("limited", got)
	}
}

// A permanent index already on disk (written by Java or an earlier run) of
// an unknown div keeps being written whatever the bound; those divs count
// toward the bound for new ones.
func TestExistingUnknownTextDivKept(t *testing.T) {
	root := t.TempDir()
	const date = "20261023"
	first, _ := liveStore(t, Config{Directory: root, HashSizeBytes: 4096})
	first.SetMaxUnknownTextDivs(-1) // unbounded
	for _, div := range []string{"c1", "c2", "c3"} {
		putText(t, first, date, div, 1, div)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	s, _ := liveStore(t, Config{Directory: root, HashSizeBytes: 4096})
	s.SetMaxUnknownTextDivs(2)
	putText(t, s, date, "c3", 2, "again")
	wantText(t, s, date, "c3", 2, "again", true)
	if err := s.PutText(date, &protocol.TextPack{XType: "c4", Hash: 1, Text: "new"}); !errors.Is(err, ErrTextDivLimit) {
		t.Fatal("new div over the bound", err)
	}
	if got := s.TextDivsLimited(); got != 1 {
		t.Fatal("limited", got)
	}
	if got := permTextDivsOnDisk(t, root); strings.Join(got, ",") != "c1,c2,c3" {
		t.Fatal(got)
	}
}

// Concurrent texts of new unknown divs admit exactly the bound.
func TestUnknownTextDivsConcurrentBound(t *testing.T) {
	s, root := liveStore(t, Config{HashSizeBytes: 4096})
	const date = "20261023"
	var wg sync.WaitGroup
	var ok, limited atomic.Int64
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				err := s.PutText(date, &protocol.TextPack{XType: fmt.Sprintf("g%d-%d", g, i), Hash: 1, Text: "t"})
				switch {
				case err == nil:
					ok.Add(1)
				case errors.Is(err, ErrTextDivLimit):
					limited.Add(1)
				default:
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if err := s.flushTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ok.Load() != DefaultMaxUnknownTextDivs || limited.Load() != 320-DefaultMaxUnknownTextDivs || s.TextDivsLimited() != uint64(limited.Load()) {
		t.Fatal(ok.Load(), limited.Load(), s.TextDivsLimited())
	}
	if got := len(permTextDivsOnDisk(t, root)); got != DefaultMaxUnknownTextDivs {
		t.Fatal("indexes", got)
	}
}

// A failed read of the permanent text directory is an error (not a limit)
// and is not repeated for textDivRescan.
func TestUnknownTextDivScanErrorCached(t *testing.T) {
	s, root := liveStore(t, Config{HashSizeBytes: 4096})
	clock := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	s.texts.divs.clock = func() time.Time { return clock }
	const date = "20261023"
	dir := filepath.Join(root, "00000000", "text")
	if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir, nil, 0o600); err != nil { // not a directory
		t.Fatal(err)
	}
	put := func(div string) error {
		return s.PutText(date, &protocol.TextPack{XType: div, Hash: 1, Text: "t"})
	}
	if err := put("u1"); err == nil || errors.Is(err, ErrLimit) {
		t.Fatal("scan error", err)
	}
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if err := put("u1"); err == nil {
		t.Fatal("scan error not cached")
	}
	clock = clock.Add(textDivRescan)
	if err := put("u1"); err != nil {
		t.Fatal(err)
	}
	if err := put("sql"); err != nil { // a known div never reads the directory
		t.Fatal(err)
	}
	if s.TextDivsLimited() != 0 {
		t.Fatal("limited", s.TextDivsLimited())
	}
}

// A div admitted but never written, and divs whose files were deleted, free
// their place at the next rescan of a full bound.
func TestUnknownTextDivsRescanFreesPlaces(t *testing.T) {
	s, _ := liveStore(t, Config{HashSizeBytes: 4096})
	clock := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	s.texts.divs.clock = func() time.Time { return clock }
	s.SetMaxUnknownTextDivs(2)
	const date = "20261023"
	if _, err := s.PrepareText(date, &protocol.TextPack{XType: "never", Hash: 1, Text: "t"}); err != nil {
		t.Fatal(err) // admitted, the Write is never submitted
	}
	putText(t, s, date, "w1", 1, "t")
	if err := s.PutText(date, &protocol.TextPack{XType: "w2", Hash: 1, Text: "t"}); !errors.Is(err, ErrTextDivLimit) {
		t.Fatal("bound", err)
	}
	clock = clock.Add(textDivRescan)
	putText(t, s, date, "w2", 1, "t") // "never" freed
	if err := s.PutText(date, &protocol.TextPack{XType: "w3", Hash: 1, Text: "t"}); !errors.Is(err, ErrTextDivLimit) {
		t.Fatal("bound after rescan", err)
	}
	if _, err := s.DeleteDatabasePaths(context.Background(), []string{"/00000000/text"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutText(date, &protocol.TextPack{XType: "w3", Hash: 1, Text: "t"}); !errors.Is(err, ErrTextDivLimit) {
		t.Fatal("rescan before textDivRescan", err)
	}
	clock = clock.Add(textDivRescan)
	putText(t, s, date, "w3", 1, "t")
	putText(t, s, date, "w4", 1, "t")
	wantText(t, s, date, "w4", 1, "t", true)
}

// A div longer than maxTextDivBytes cannot name a file: it is invalid.
func TestTextDivLength(t *testing.T) {
	s, _ := liveStore(t, Config{HashSizeBytes: 4096})
	const date = "20261023"
	putText(t, s, date, strings.Repeat("d", maxTextDivBytes), 1, "t")
	if err := s.PutText(date, &protocol.TextPack{XType: strings.Repeat("d", maxTextDivBytes+1), Hash: 1, Text: "t"}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
