//go:build !linux

package server

// residentSetBytes is unavailable without /proc; the stats line omits rss and
// prints the Go runtime's heap/sys figures only.
func residentSetBytes() (uint64, bool) { return 0, false }
