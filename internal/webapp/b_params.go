package webapp

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/snowvil/scouter-go/internal/scoutertime"
)

// Common request-parameter rules for the B controllers (webapp-api.md §1).
// Parse failures that the Java webapp turns into a 500 (NumberFormatException,
// NullPointerException, DateTimeParseException: W12) are reported as
// VALIDATE_ERROR 400 instead; this is a recorded intended difference.

const (
	bDefaultPageCount = 10_000
	bMaxPageCount     = 30_000
	bTTLPermanent     = int64(-1) // ParamConstant.TTL_PERMANENT
	// bMaxDays bounds per-day fan-out loops (Summary). The Java webapp has no
	// bound; an unbounded loop would issue one collector call per day.
	bMaxDays = 366
)

// bNotFound is the route-level 404 (no Jersey template matches) used by the B
// dispatchers that share one Go pattern across several Java templates.
func bNotFound() error {
	return newAPIError(http.StatusNotFound, http.StatusNotFound, "HTTP 404 Not Found")
}

// bConvError reports a @PathParam/@QueryParam that cannot be converted to its
// primitive type. Jersey answers 404 (ParamException); like A's params.go the
// Go webapp answers 400 VALIDATE_ERROR with the same message format (intended difference).
func bConvError(name, v string, bits int) error {
	kind := "an int"
	if bits == 64 {
		kind = "a long"
	}
	return validateError(fmt.Sprintf("%s is not %s: %q", name, kind, v))
}

// bConstraint mirrors ValidationExceptionMapper's "[ValidationException] ..." 400.
// The Java property path (method.argN.field) is not reproduced.
func bConstraint(msg string) error {
	return newAPIError(http.StatusBadRequest, http.StatusBadRequest, "[ValidationException] "+msg)
}

// bQueryRaw returns the first value of a query parameter and whether it exists.
func bQueryRaw(r *http.Request, name string) (string, bool) {
	vs, ok := r.URL.Query()[name]
	if !ok || len(vs) == 0 {
		return "", false
	}
	return vs[0], true
}

// bQueryString mirrors a Jersey String @QueryParam: absent -> nil, present -> value (possibly "").
func bQueryString(r *http.Request, name string) *string {
	v, ok := bQueryRaw(r, name)
	if !ok {
		return nil
	}
	return &v
}

// bQueryInt64 mirrors a Jersey primitive long @QueryParam: absent or blank -> 0,
// otherwise Long.valueOf without trimming; failure -> 400 (bConvError).
func bQueryInt64(r *http.Request, name string) (int64, error) {
	v, ok := bQueryRaw(r, name)
	if !ok || strings.TrimSpace(v) == "" {
		return 0, nil
	}
	n, err := bParseJavaLong(v, 64)
	if err != nil {
		return 0, bConvError(name, v, 64)
	}
	return n, nil
}

func bQueryInt32(r *http.Request, name string) (int32, error) {
	v, ok := bQueryRaw(r, name)
	if !ok || strings.TrimSpace(v) == "" {
		return 0, nil
	}
	n, err := bParseJavaLong(v, 32)
	if err != nil {
		return 0, bConvError(name, v, 32)
	}
	return int32(n), nil
}

func bPathInt64(r *http.Request, name string) (int64, error) {
	v := r.PathValue(name)
	n, err := bParseJavaLong(v, 64)
	if err != nil {
		return 0, bConvError(name, v, 64)
	}
	return n, nil
}

func bPathInt32(r *http.Request, name string) (int32, error) {
	v := r.PathValue(name)
	n, err := bParseJavaLong(v, 32)
	if err != nil {
		return 0, bConvError(name, v, 32)
	}
	return int32(n), nil
}

// bParseJavaLong follows Integer.parseInt/Long.parseLong for ASCII input: an
// optional single sign, decimal digits only, no whitespace, range checked.
func bParseJavaLong(s string, bits int) (int64, error) {
	if s == "" || strings.ContainsAny(s, "_ \t") {
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseInt(s, 10, bits)
}

// bServerID validates the optional serverId query. The embedded Go server has a
// single collector, so like ServerManager.getServerIfNullDefault with one server
// any id resolves to it; only a conversion failure is reported (400).
func bServerID(r *http.Request) error {
	_, err := bQueryInt32(r, "serverId")
	return err
}

// bSplitParam is ZZ.splitParam: strip a leading '[' and the last character,
// then Apache StringUtils.split(s, ','), which drops empty tokens. A missing or
// empty value throws in Java (NPE/StringIndexOutOfBounds -> 500); here it is a
// validation error.
func bSplitParam(name, s string) ([]string, error) {
	if s == "" {
		return nil, validateError(name + " is required")
	}
	if s[0] == '[' {
		if len(s) < 2 {
			return nil, validateError(name + " is invalid")
		}
		s = s[1 : len(s)-1]
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out, nil
}

// bSplitInt32 is ZZ.splitParamAsInteger (List: order and duplicates kept).
func bSplitInt32(name, s string) ([]int32, error) {
	parts, err := bSplitParam(name, s)
	if err != nil {
		return nil, err
	}
	out := make([]int32, 0, len(parts))
	for _, p := range parts {
		n, err := bParseJavaLong(p, 32)
		if err != nil {
			return nil, validateError(name + ` has an invalid number: "` + p + `"`)
		}
		out = append(out, int32(n))
	}
	return out, nil
}

// bSplitInt32Set is ZZ.splitParamAsIntegerSet: a java.util.HashSet<Integer>
// whose iteration order (used to build the request ListValue) is emulated.
func bSplitInt32Set(name, s string) ([]int32, error) {
	list, err := bSplitInt32(name, s)
	if err != nil {
		return nil, err
	}
	return bJavaIntSet(list), nil
}

// bSplitInt64 is ZZ.splitParamAsLong (List).
func bSplitInt64(name, s string) ([]int64, error) {
	parts, err := bSplitParam(name, s)
	if err != nil {
		return nil, err
	}
	out := make([]int64, 0, len(parts))
	for _, p := range parts {
		n, err := bParseJavaLong(p, 64)
		if err != nil {
			return nil, validateError(name + ` has an invalid number: "` + p + `"`)
		}
		out = append(out, n)
	}
	return out, nil
}

// bRequiredQuery returns a required query parameter (Java @NotNull); absence is 400.
func bRequiredQuery(r *http.Request, name string) (string, error) {
	v, ok := bQueryRaw(r, name)
	if !ok {
		return "", bConstraint(name + " may not be null")
	}
	return v, nil
}

func bBlank(s *string) bool { return s == nil || strings.TrimSpace(*s) == "" }

// bParseLenientYmdHms follows SimpleDateFormat("yyyyMMddHHmmss").parse in its
// default lenient mode: abutting numeric fields take fixed widths 4,2,2,2,2 and
// the last field takes all remaining digits; trailing non-digits are ignored;
// out-of-range values roll over (time.Date normalizes like a lenient Calendar).
func bParseLenientYmdHms(s string, loc *time.Location) (int64, bool) {
	widths := []int{4, 2, 2, 2, 2}
	vals := make([]int, 0, 6)
	pos := 0
	for _, w := range widths {
		if pos+w > len(s) {
			return 0, false
		}
		n, ok := bDigits(s[pos : pos+w])
		if !ok {
			return 0, false
		}
		vals = append(vals, n)
		pos += w
	}
	end := pos
	for end < len(s) && s[end] >= '0' && s[end] <= '9' && end-pos < 9 {
		end++
	}
	if end == pos {
		return 0, false
	}
	sec, _ := bDigits(s[pos:end])
	vals = append(vals, sec)
	t := time.Date(vals[0], time.Month(vals[1]), vals[2], vals[3], vals[4], vals[5], 0, loc)
	return t.UnixMilli(), true
}

func bDigits(s string) (int, bool) {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + int(s[i]-'0')
	}
	return n, true
}

// bParseStrictLocal follows LocalDateTime.parse(s, ofPattern(p)) with the SMART
// resolver for the digit-only patterns used by the webapp ("yyyyMMddHHmm",
// "yyyyMMddHH"): exact length, month 1..12, day 1..31 clamped to the month end,
// hour 0..23 (24 only at minute 0 -> next day), minute 0..59. The result is
// atZone(location).toEpochSecond()*1000.
func bParseStrictLocal(s string, withMinute bool, loc *time.Location) (int64, bool) {
	want := 10
	if withMinute {
		want = 12
	}
	if len(s) != want {
		return 0, false
	}
	year, ok1 := bDigits(s[0:4])
	month, ok2 := bDigits(s[4:6])
	day, ok3 := bDigits(s[6:8])
	hour, ok4 := bDigits(s[8:10])
	minute := 0
	ok5 := true
	if withMinute {
		minute, ok5 = bDigits(s[10:12])
	}
	if !(ok1 && ok2 && ok3 && ok4 && ok5) {
		return 0, false
	}
	if month < 1 || month > 12 || day < 1 || day > 31 || minute > 59 || hour > 24 {
		return 0, false
	}
	if hour == 24 && minute != 0 {
		return 0, false
	}
	last := time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
	if day > last {
		day = last
	}
	t := time.Date(year, time.Month(month), day, hour, minute, 0, 0, loc)
	return t.Unix() * 1000, true
}

// bDateTime is the subset of DateTimeHelper used by the Summary day split.
type bDateTime struct{ loc *time.Location }

func (d bDateTime) base() int64 { return time.Date(2000, 1, 1, 0, 0, 0, 0, d.loc).UnixMilli() }
func (d bDateTime) unit(ms int64) int64 {
	return (ms - d.base()) / scoutertime.DayMillis // Java long division truncates toward zero
}
func (d bDateTime) unitMillis(unit int64) int64 { return unit*scoutertime.DayMillis + d.base() }
func (d bDateTime) ymd(ms int64) string         { return scoutertime.Date(ms, d.loc) }

// bHexa32 is scouter.util.Hexa32.toString32.
func bHexa32(n int64) string {
	switch {
	case n == -1<<63:
		return "z8000000000000"
	case n < 0:
		return "z" + strconv.FormatInt(-n, 32)
	case n < 10:
		return strconv.FormatInt(n, 10)
	default:
		return "x" + strconv.FormatInt(n, 32)
	}
}

// bUTF16Substring is StringUtils.substring(s, start) measured in UTF-16 units.
func bUTF16Substring(s string, start int) string {
	u := utf16.Encode([]rune(s))
	if start >= len(u) {
		return ""
	}
	return string(utf16.Decode(u[start:]))
}

func bUTF16Len(s string) int { return len(utf16.Encode([]rune(s))) }
