package geoip

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// FuzzFromBytes는 임의 .dat 내용과 IP에 대해 panic이 없고, 형식 오류·조회 실패가 항상 원본 예외 class를 가진
// *Error이며, 같은 조회가 결정적인지 확인한다. seed는 oracle 입력 .dat 전체다.
func FuzzFromBytes(f *testing.F) {
	entries, err := os.ReadDir(oracleDir)
	if err != nil {
		f.Fatal(err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".dat") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(oracleDir, e.Name()))
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data, uint32(0x01020304))
		f.Add(data, uint32(0xffffffff))
		f.Add(data, uint32(0x07000001))
	}
	f.Fuzz(func(t *testing.T, data []byte, ip uint32) {
		db, err := FromBytes(append([]byte(nil), data...))
		if err != nil {
			var ge *Error
			if !errors.As(err, &ge) || ge.Java == "" || ge.Op != "open" {
				t.Fatalf("open error without java class: %v", err)
			}
			return
		}
		for _, addr := range [][4]byte{
			{byte(ip >> 24), byte(ip >> 16), byte(ip >> 8), byte(ip)},
			{byte(ip), byte(ip >> 8), byte(ip >> 16), byte(ip >> 24)},
		} {
			loc, found, err := db.Lookup(addr)
			loc2, found2, err2 := db.Lookup(addr)
			if loc != loc2 || found != found2 || errString(err) != errString(err2) {
				t.Fatalf("non-deterministic lookup for %v", addr)
			}
			if err != nil {
				var ge *Error
				if !errors.As(err, &ge) || ge.Java == "" || ge.Op != "lookup" || found {
					t.Fatalf("lookup error without java class: %v (found=%v)", err, found)
				}
				continue
			}
			if found && loc.CountryCode == "" {
				t.Fatalf("found location without country code: %+v", loc)
			}
			if !found && loc != (Location{}) {
				t.Fatalf("not-found returned data: %+v", loc)
			}
		}
	})
}
