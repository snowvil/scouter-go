package geoip

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestOpenMissingFile(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "GeoLiteCity.dat"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("err = %v, want fs.ErrNotExist", err)
	}
}

func TestOpenRejectsDirectory(t *testing.T) {
	_, err := Open(t.TempDir())
	var ge *Error
	if !errors.As(err, &ge) || ge.Java != "" {
		t.Fatalf("err = %v, want Go-only *Error", err)
	}
}

// 크기 상한은 stat 단계에서 거절하므로 sparse 파일로 할당 없이 확인한다.
func TestOpenRejectsOversizedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "big.dat")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(MaxFileSize + 1); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = Open(path)
	var ge *Error
	if !errors.As(err, &ge) || ge.Java != "" || ge.Op != "open" {
		t.Fatalf("err = %v, want size-limit *Error", err)
	}
}

func TestReadString(t *testing.T) {
	for _, tc := range []struct {
		rec     string
		pos     int
		want    string
		wantPos int
	}{
		{"\x00ab\x00", 1, "ab", 4},
		{"\x00\x00", 1, "", 2},      // 빈 문자열은 null
		{"\x00abc", 1, "ab", 4},     // NUL 없이 끝: 마지막 byte를 버린다
		{"\x00a", 1, "", 2},         // 1 byte 남고 NUL 없음: null, position은 limit
		{"\x00", 1, "", 1},          // 남은 byte 없음: null, position 유지
		{"\x00\xfc\x00", 1, "ü", 3}, // ISO-8859-1
	} {
		got, pos := readString([]byte(tc.rec), tc.pos)
		if got != tc.want || pos != tc.wantPos {
			t.Errorf("readString(%q, %d) = %q, %d; want %q, %d", tc.rec, tc.pos, got, pos, tc.want, tc.wantPos)
		}
	}
}

func TestToAngle(t *testing.T) {
	if got := toAngle(0); got != -180 {
		t.Errorf("toAngle(0) = %v", got)
	}
	if got := toAngle(1800000); got != 0 {
		t.Errorf("toAngle(1800000) = %v", got)
	}
}

// Lookup은 공유 상태가 없으므로 동시 호출 결과가 순차 결과와 같아야 한다(-race로 검증).
func TestConcurrentLookup(t *testing.T) {
	db, err := Open(filepath.Join(oracleDir, "city_rev1.dat"))
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		loc   Location
		found bool
		err   string
	}
	ips := make([][4]byte, 0, 1024)
	for i := 0; i < 1024; i++ {
		ips = append(ips, [4]byte{byte(i % 14), byte(i * 7), byte(i * 13), byte(i)})
	}
	want := make([]result, len(ips))
	for i, ip := range ips {
		loc, found, err := db.Lookup(ip)
		want[i] = result{loc, found, errString(err)}
	}
	var wg sync.WaitGroup
	errs := make(chan string, 8)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; k < 4; k++ {
				for i := range ips {
					j := (i + g*131) % len(ips)
					loc, found, err := db.Lookup(ips[j])
					if (result{loc, found, errString(err)}) != want[j] {
						errs <- "concurrent result differs for " + string(ips[j][:])
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func BenchmarkLookup(b *testing.B) {
	db, err := Open(filepath.Join(oracleDir, "city_rev1.dat"))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if _, _, err := db.Lookup([4]byte{1, byte(i), 3, 4}); err != nil {
			b.Fatal(err)
		}
	}
}
