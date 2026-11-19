//go:build !linux

package transport

import (
	"net"
	"runtime"
)

// Kernel UDP drop counters are read on Linux only (udp_diag.go).
func socketInode(*net.UDPConn) uint64 { return 0 }
func readKernelUDP(uint64) kernelCounters {
	return kernelCounters{err: "kernel UDP counters not supported on " + runtime.GOOS}
}
