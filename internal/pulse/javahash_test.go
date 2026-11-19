package pulse

import "testing"

// Reference values were produced independently from the Go implementation with
// a UTF-16 code unit loop (java.lang.String.hashCode) and zlib.crc32 (which is
// the same CRC-32 as scouter.util.HashUtil, HashUtil.java:56-66).
func TestJavaStringHashCode(t *testing.T) {
	cases := map[string]int32{
		"":                   0,
		"a":                  97,
		"hello":              99162322,
		"gateway-app":        -436505832,
		"excluded-app":       -1353132130,
		"plain-app-a":        160852466,
		"한글앱":                53921229,
		"😀app":               1276923358, // U+1F600 contributes two surrogates
		"polygenelubricants": -2147483648,
	}
	for s, want := range cases {
		if got := JavaStringHashCode(s); got != want {
			t.Errorf("hashCode(%q)=%d want %d", s, got, want)
		}
	}
}

func TestFloorModAndPartition(t *testing.T) {
	cases := []struct {
		x, n, want int32
	}{{7, 30, 7}, {-7, 30, 23}, {-30, 30, 0}, {30, 30, 0}, {-2147483648, 30, 22}, {-1, 1, 0}}
	for _, c := range cases {
		if got := FloorMod(c.x, c.n); got != c.want {
			t.Errorf("floorMod(%d,%d)=%d want %d", c.x, c.n, got, c.want)
		}
	}
	// Math.floorMod(application.hashCode(), count).
	for app, want := range map[string]int32{"excluded-app": 20, "plain-app-a": 26, "한글앱": 9, "😀app": 28, "polygenelubricants": 22} {
		if got := Partition(app, 30); got != want {
			t.Errorf("partition(%q)=%d want %d", app, got, want)
		}
	}
	if Partition("gateway-app", 7) != FloorMod(-436505832, 7) {
		t.Fatal("partition count not applied")
	}
}

func TestTransactionHash(t *testing.T) {
	cases := map[string]int32{
		"hello world":      222957957, // HashUtil.main prints this value
		"**&^%hello world": -1066873438,
		"/api/v1":          -1016607451,
		"한글":               -1866879643, // UTF-8 bytes of the JVM default charset
		"":                 0,
	}
	for s, want := range cases {
		if got := TransactionHash(s); got != want {
			t.Errorf("HashUtil.hash(%q)=%d want %d", s, got, want)
		}
	}
}
