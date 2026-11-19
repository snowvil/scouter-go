package httpapi

import "strings"

// strMatch is scouter.util.StrMatch with '*': "*"/"**" match anything; one
// leading, trailing and/or a single interior '*' select prefix/suffix/contains
// matching; anything else (or a pattern shorter than 2) is equality. An empty
// target never matches.
type strMatch struct {
	comp                  matchComp
	start, end, mid, mid2 string
}

type matchComp int

const (
	compEqu matchComp = iota
	compStr
	compStrMid
	compStrEnd
	compMid
	compMidMid
	compMidEnd
	compEnd
	compAny
)

func newStrMatch(pattern string) strMatch {
	if pattern == "*" || pattern == "**" {
		return strMatch{comp: compAny}
	}
	n := len(pattern)
	if n < 2 {
		return strMatch{comp: compEqu, mid: pattern}
	}
	anyStart, anyEnd := pattern[0] == '*', pattern[n-1] == '*'
	x := -1
	if i := strings.IndexByte(pattern[1:], '*'); i >= 0 {
		x = i + 1
	}
	if x > 0 && x < n-1 {
		switch {
		case anyStart && anyEnd:
			return strMatch{comp: compMidMid, mid: pattern[1:x], mid2: pattern[x+1 : n-1]}
		case anyStart:
			return strMatch{comp: compMidEnd, mid: pattern[1:x], end: pattern[x+1:]}
		case anyEnd:
			return strMatch{comp: compStrMid, start: pattern[:x], mid: pattern[x+1 : n-1]}
		default:
			return strMatch{comp: compStrEnd, start: pattern[:x], end: pattern[x+1:]}
		}
	}
	switch {
	case anyStart && anyEnd:
		return strMatch{comp: compMid, mid: pattern[1 : n-1]}
	case anyStart:
		return strMatch{comp: compEnd, end: pattern[1:]}
	case anyEnd:
		return strMatch{comp: compStr, start: pattern[:n-1]}
	}
	return strMatch{comp: compEqu, mid: pattern}
}

func (m strMatch) include(t string) bool {
	if t == "" {
		return false
	}
	switch m.comp {
	case compAny:
		return true
	case compEqu:
		return t == m.mid
	case compStr:
		return strings.HasPrefix(t, m.start)
	case compStrMid:
		return strings.HasPrefix(t, m.start) && strings.Contains(t, m.mid)
	case compStrEnd:
		return strings.HasPrefix(t, m.start) && strings.HasSuffix(t, m.end)
	case compMid:
		return strings.Contains(t, m.mid)
	case compMidMid:
		x := strings.Index(t, m.mid)
		return x >= 0 && strings.Contains(t[x+len(m.mid):], m.mid2)
	case compMidEnd:
		return strings.Contains(t, m.mid) && strings.HasSuffix(t, m.end)
	case compEnd:
		return strings.HasSuffix(t, m.end)
	}
	return false
}

// allowList is Configure.allowIpExact + allowIpMatch.
type allowList struct {
	exact   map[string]struct{}
	matches []strMatch
}

func newAllowList(entries []string) allowList {
	l := allowList{exact: map[string]struct{}{}}
	for _, e := range entries {
		l.exact[e] = struct{}{}
		if strings.Contains(e, "*") {
			l.matches = append(l.matches, newStrMatch(e))
		}
	}
	return l
}

func (l allowList) allows(ip string) bool {
	if _, ok := l.exact[ip]; ok {
		return true
	}
	for _, m := range l.matches {
		if m.include(ip) {
			return true
		}
	}
	return false
}

// ParseAllowIPs splits net_http_api_allow_ips like Java String.split(",") for
// the inputs that matter (an all-empty input such as ",," gives [""] here,
// an empty array in Java; neither entry matches an address):
// entries are not trimmed and trailing empty entries are removed.
func ParseAllowIPs(s string) []string {
	parts := strings.Split(s, ",")
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	if len(parts) == 0 {
		return []string{""}
	}
	return parts
}
