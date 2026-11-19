package legacyindex

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"
	"time"
)

// Chunked collection (WalkRangeStep paused every few records) plus
// TimeEntries yields exactly the RangeBucket order: signed-byte value order,
// TreeSet duplicates, reverse, endpoint filter, empty and pre-base buckets.
func TestTimeEntriesMatchRangeBucket(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	idx, _ := openTest(t, 0, true, loc)
	base := time.Date(2000, 1, 1, 0, 0, 0, 0, loc).UnixMilli()
	start := base + 183*86400000 + 125
	rng := rand.New(rand.NewSource(1))
	for n := 0; n < 3000; n++ {
		ms := start + int64(rng.Intn(2600))
		v := []byte{byte(rng.Intn(256)), byte(rng.Intn(3))}
		mustPut(t, idx, timeKey(ms), v)
		if n%7 == 0 {
			mustPut(t, idx, timeKey(ms), v) // duplicate (time, value)
		}
	}
	mustPut(t, idx, timeKey(base-5000), []byte{1}) // before Java's base: bucket zero
	errPauseTest := errors.New("pause")
	for _, rg := range [][2]int64{{start + 3, start + 2500}, {start, start + 499}, {base - 6000, base + 10}} {
		for _, reverse := range []bool{false, true} {
			steps, err := idx.RangeSteps(rg[0], rg[1])
			if err != nil {
				t.Fatal(err)
			}
			var entries TimeEntries
			for step := 0; step < steps; step++ {
				want, err := idx.RangeBucket(rg[0], rg[1], reverse, step)
				if err != nil {
					t.Fatal(err)
				}
				entries.Reset()
				var at, resume int64
				for {
					seen := 0
					err := idx.WalkRangeStep(rg[0], rg[1], reverse, step, at, resume, func(r Record) error {
						at, resume = r.Position, r.PreviousPosition
						if err := entries.Add(r); err != nil {
							return err
						}
						if seen++; seen == 5 {
							return errPauseTest
						}
						return nil
					})
					if errors.Is(err, errPauseTest) {
						continue
					}
					if err != nil {
						t.Fatal(err)
					}
					break
				}
				entries.Finish(rg[0], rg[1], reverse)
				if entries.Len() != len(want) {
					t.Fatalf("range %v reverse=%v step %d: %d entries, RangeBucket %d", rg, reverse, step, entries.Len(), len(want))
				}
				for n, r := range want {
					ms, v := entries.At(n)
					if !bytes.Equal(timeKey(ms), r.Key) || !bytes.Equal(v, r.Value) {
						t.Fatalf("range %v reverse=%v step %d entry %d differs", rg, reverse, step, n)
					}
				}
			}
		}
	}
}
