package logger

import (
	"strconv"
	"strings"
	"time"
)

const dayMillis int64 = 86400000

// calendar is scouter.util.DateTimeHelper for one time zone: the base is
// 2000-01-01 00:00 local time and every later day is exactly 24h, so the
// date/clock of a DST zone follows the fixed offset of 2000-01-01 rather than
// the wall clock (the same rule as internal/scoutertime).
type calendar struct {
	loc  *time.Location
	base int64
}

func newCalendar(loc *time.Location) calendar {
	return calendar{loc: loc, base: time.Date(2000, 1, 1, 0, 0, 0, 0, loc).UnixMilli()}
}

// dateUnit is DateTimeHelper.getDateUnit(time): days since the base,
// truncated toward zero like Java long division.
func (c calendar) dateUnit(ms int64) int64 { return (ms - c.base) / dayMillis }

func dateOfUnit(unit int64) string {
	if unit < 0 {
		unit = 0
	}
	return time.Date(2000, 1, 1+int(unit), 0, 0, 0, 0, time.UTC).Format("20060102")
}

// yyyymmdd is DateTimeHelper.yyyymmdd(long).
func (c calendar) yyyymmdd(ms int64) string { return dateOfUnit(c.dateUnit(ms)) }

func pad(n int64, width int) string {
	s := strconv.FormatInt(n, 10)
	if len(s) < width {
		s = strings.Repeat("0", width-len(s)) + s
	}
	return s
}

// datetime is DateTimeHelper.datetime: "yyyyMMdd HH:mm:ss".
func (c calendar) datetime(ms int64) string {
	if ms < c.base {
		return "20000101 00:00:00"
	}
	d := (ms - c.base) % dayMillis
	return dateOfUnit(c.dateUnit(ms)) + " " + pad(d/3600000, 2) + ":" + pad(d/60000%60, 2) + ":" + pad(d/1000%60, 2)
}

// timestamp is DateTimeHelper.timestamp: "yyyyMMdd HH:mm:ss.SSS".
func (c calendar) timestamp(ms int64) string {
	if ms < c.base {
		return "20000101 00:00:00.000"
	}
	return c.datetime(ms) + "." + pad((ms-c.base)%1000, 3)
}

// sqlTimestamp is java.sql.Timestamp(ms).toString() in the zone's real
// (DST-aware) calendar: "yyyy-mm-dd hh:mm:ss.f" where the fraction is the
// nanoseconds without trailing zeros, or "0".
func sqlTimestamp(ms int64, loc *time.Location) string {
	t := time.UnixMilli(ms).In(loc)
	nanos := int64(t.Nanosecond())
	frac := "0"
	if nanos != 0 {
		frac = strings.TrimRight(pad(nanos, 9), "0")
	}
	return pad(int64(t.Year()), 4) + t.Format("-01-02 15:04:05") + "." + frac
}

// javaParseInt is Integer.parseInt for ASCII input: an optional leading sign
// followed by at least one decimal digit.
func javaParseInt(s string) (int, bool) {
	body := s
	if body != "" && (body[0] == '+' || body[0] == '-') {
		body = body[1:]
	}
	if body == "" {
		return 0, false
	}
	for i := 0; i < len(body); i++ {
		if body[i] < '0' || body[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.Atoi(s)
	return n, err == nil
}

// parseDateUnit is DateUtil.getDateUnit(DateUtil.yyyymmdd(date)) for an
// 8-character date: years before 2000 map to the base (unit 0), years from
// 2100 to the day after 2099-12-31, and an impossible month/day or a
// non-numeric part is an error (Java throws and Logger.clearOldLog skips it).
// Non-ASCII digits that Java's parseInt would accept are treated as errors.
func parseDateUnit(date string) (int64, bool) {
	if len(date) != 8 {
		return 0, false
	}
	year, ok1 := javaParseInt(date[0:4])
	month, ok2 := javaParseInt(date[4:6])
	day, ok3 := javaParseInt(date[6:8])
	if !ok1 || !ok2 || !ok3 {
		return 0, false
	}
	if year >= 2100 {
		return 36525, true
	}
	if year < 2000 {
		return 0, true
	}
	if month < 1 || month > 12 || day < 1 {
		return 0, false
	}
	t := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	if t.Day() != day {
		return 0, false
	}
	return int64(t.Sub(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) / (24 * time.Hour)), true
}
