package storage

import (
	"hash/crc32"
	"testing"
)

func TestCRC32StringMatchesChecksum(t *testing.T) {
	for _, s := range []string{"", "a", "service", "service-time-sum", "@total", "alert", "나라", "\x00\xff", "user-agent"} {
		if got, want := crc32String(s), crc32.ChecksumIEEE([]byte(s)); got != want {
			t.Fatalf("%q: %08x want %08x", s, got, want)
		}
	}
}

func FuzzCRC32String(f *testing.F) {
	f.Add("service")
	f.Fuzz(func(t *testing.T, s string) {
		if got, want := crc32String(s), crc32.ChecksumIEEE([]byte(s)); got != want {
			t.Fatalf("%q: %08x want %08x", s, got, want)
		}
	})
}

// tagTablePath is the joined path, also after the cache starts over.
func TestTagTablePathCache(t *testing.T) {
	s := &Store{config: Config{Directory: "/db"}}
	for round := 0; round < 2; round++ {
		for i := 0; i < tagPathMax+5; i++ {
			objType := "t" + string(rune('a'+i%26)) + string(rune('a'+i/26%26)) + string(rune('a'+i/676))
			if got, want := s.tagTablePath("20261019", objType), "/db/20261019/tagcnt/"+objType; got != want {
				t.Fatal(got, want)
			}
		}
	}
	if len(s.tags.paths) > tagPathMax {
		t.Fatal("unbounded", len(s.tags.paths))
	}
}
