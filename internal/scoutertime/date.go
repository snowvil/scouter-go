// Package scoutertime preserves the pinned Java DateTimeHelper calendar.
// Scouter source Copyright 2015 authors, Apache License 2.0.
package scoutertime

import (
	"fmt"
	"sync/atomic"
	"time"
)

const DayMillis int64 = 86400000

// Every stored record asks for its day and minute, nearly always in the same
// location and day: the last base and the last day's name are kept (each a
// pure function of its key, so a lost race only recomputes).
type baseEntry struct {
	location *time.Location
	ms       int64
}
type dayEntry struct {
	days int64
	name string
}

var lastBase atomic.Pointer[baseEntry]
var lastDay atomic.Pointer[dayEntry]

func base(location *time.Location) int64 {
	if b := lastBase.Load(); b != nil && b.location == location {
		return b.ms
	}
	ms := time.Date(2000, 1, 1, 0, 0, 0, 0, location).UnixMilli()
	lastBase.Store(&baseEntry{location, ms})
	return ms
}

// Millis is the fixed 24h offset, rather than local wall time during DST.
func Millis(ms int64, location *time.Location) int64 {
	b := base(location)
	if ms < b {
		return 0
	}
	return (ms - b) % DayMillis
}
func Date(ms int64, location *time.Location) string {
	days := (ms - base(location)) / DayMillis
	if days < 0 {
		days = 0
	}
	if d := lastDay.Load(); d != nil && d.days == days {
		return d.name
	}
	name := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(days)).Format("20060102")
	lastDay.Store(&dayEntry{days, name})
	return name
}

// ParseDate selects the matching Java fixed-day boundary. Unknown/out-of-domain
// dates are rejected before query/file access rather than clamped silently.
func ParseDate(date string, location *time.Location) (int64, error) {
	if location == nil || len(date) != 8 {
		return 0, fmt.Errorf("invalid Scouter date")
	}
	d, err := time.Parse("20060102", date)
	if err != nil {
		return 0, err
	}
	if d.Year() < 2000 || d.Year() >= 2100 {
		return 0, fmt.Errorf("Scouter date outside 2000..2099")
	}
	days := int64(d.Sub(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) / (24 * time.Hour))
	return base(location) + days*DayMillis, nil
}
