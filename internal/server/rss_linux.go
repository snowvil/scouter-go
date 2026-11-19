package server

import (
	"os"
	"strconv"
	"strings"
)

// residentSetBytes reads the current RSS from /proc/self/statm (second field,
// pages). It is the figure operators compare with ps; Go's heap metrics do not
// include freed-but-unreturned spans or non-Go memory.
func residentSetBytes() (uint64, bool) {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return pages * uint64(os.Getpagesize()), true
}
