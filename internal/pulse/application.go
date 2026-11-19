package pulse

import "strings"

// javaSplit reproduces java.lang.String.split(literal) with limit 0 for a
// literal single-character or multi-character separator: a leading empty
// element is kept, trailing empty elements are removed, and an input without any
// match yields the input itself (so "" gives [""]).
func javaSplit(s, sep string) []string {
	if !strings.Contains(s, sep) {
		return []string{s}
	}
	parts := strings.Split(s, sep)
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

// ExtractApplication is the application-name extraction of the plugin XLog
// and profile converters: the OBJECT dictionary
// string is split on "/", element 2 is split on "@" and its first element is the
// application. No trim or lowercase is applied.
//
// ok is false when the Java code would fail: split length <= 1 returns "" (which
// the dispatcher treats as an extraction failure), and a missing element
// 2 or an "@"-only element throws ArrayIndexOutOfBoundsException, which fails the
// Java task. A dictionary miss (Java null) also fails the task with a
// NullPointerException. All of these are unified as "extraction failure" here.
func ExtractApplication(objName string, found bool) (application string, ok bool) {
	if !found {
		return "", false
	}
	info := javaSplit(objName, "/")
	if len(info) <= 1 {
		return "", false
	}
	if len(info) < 3 {
		return "", false
	}
	info2 := javaSplit(info[2], "@")
	if len(info2) == 0 {
		return "", false
	}
	if info2[0] == "" {
		return "", false
	}
	return info2[0], true
}
