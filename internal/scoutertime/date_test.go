package scoutertime

import (
	"testing"
	"time"
)

func TestFixedJavaCalendar(t *testing.T) {
	loc, _ := time.LoadLocation("America/New_York")
	ms := time.Date(2026, 7, 1, 0, 0, 0, 0, loc).UnixMilli()
	if Date(ms, loc) != "20260630" || Millis(ms, loc) != 23*3600000 {
		t.Fatal(Date(ms, loc), Millis(ms, loc))
	}
	start, err := ParseDate("20260701", loc)
	if err != nil || time.UnixMilli(start).In(loc).Hour() != 1 {
		t.Fatal(start, err)
	}
	if Date(start, loc) != "20260701" || Millis(start, loc) != 0 {
		t.Fatal("inverse calendar")
	}
	if _, err := ParseDate("../escape", loc); err == nil {
		t.Fatal("path accepted")
	}
	if Date(0, loc) != "20000101" || Millis(0, loc) != 0 {
		t.Fatal("Java before-base clamp")
	}
}

// The cached base and day name give the uncached results for any mix of
// locations and days, also from concurrent callers.
func TestDateCacheMatchesFormula(t *testing.T) {
	seoul, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Skip(err)
	}
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip(err)
	}
	locations := []*time.Location{time.UTC, seoul, ny}
	want := func(ms int64, loc *time.Location) (string, int64) {
		b := time.Date(2000, 1, 1, 0, 0, 0, 0, loc).UnixMilli()
		days := (ms - b) / DayMillis
		if days < 0 {
			days = 0
		}
		millis := int64(0)
		if ms >= b {
			millis = (ms - b) % DayMillis
		}
		return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(days)).Format("20060102"), millis
	}
	start := time.Date(2026, 10, 18, 22, 0, 0, 0, time.UTC).UnixMilli()
	done := make(chan struct{})
	for g := 0; g < 4; g++ {
		go func(g int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 5000; i++ {
				ms := start + int64((i*7919+g*104729)%(3*86400))*1000
				if i%97 == 0 {
					ms = 0 // before the base
				}
				loc := locations[(i+g)%len(locations)]
				date, millis := want(ms, loc)
				if got := Date(ms, loc); got != date {
					t.Error(ms, loc, got, date)
					return
				}
				if got := Millis(ms, loc); got != millis {
					t.Error(ms, loc, got, millis)
					return
				}
			}
		}(g)
	}
	for g := 0; g < 4; g++ {
		<-done
	}
}
