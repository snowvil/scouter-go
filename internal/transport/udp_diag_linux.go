package transport

import (
	"errors"
	"net"
	"os"
	"syscall"
)

func socketInode(conn *net.UDPConn) uint64 {
	rc, err := conn.SyscallConn()
	if err != nil {
		return 0
	}
	var inode uint64
	rc.Control(func(fd uintptr) {
		var st syscall.Stat_t
		if syscall.Fstat(int(fd), &st) == nil {
			inode = st.Ino
		}
	})
	return inode
}

// readKernelUDP reads this socket's row of /proc/net/udp (an IPv4 socket) or
// /proc/net/udp6 (an IPv6 or dual-stack one), and apart from it the host-wide
// UDP counters of /proc/net/snmp plus /proc/net/snmp6 (absent when IPv6 is
// off: then IPv4 only). A part that fails is reported as not read, with the
// first reason in err.
func readKernelUDP(inode uint64) kernelCounters {
	var c kernelCounters
	fail := func(reason string) {
		if c.err == "" {
			c.err = reason
		}
	}
	switch inode {
	case 0:
		fail("socket inode unknown")
	default:
		for _, path := range []string{"/proc/net/udp", "/proc/net/udp6"} {
			table, err := os.ReadFile(path)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) || path == "/proc/net/udp" {
					fail(err.Error())
				}
				continue
			}
			if c.drops, c.queued, c.socket = parseProcUDP(table, inode); c.socket {
				break
			}
		}
		if !c.socket {
			fail("socket not found in /proc/net/udp{,6} (closed?)")
		}
	}
	snmp, err := os.ReadFile("/proc/net/snmp")
	if err != nil {
		fail(err.Error())
		return c
	}
	if c.rcvbuf, c.inErrors, c.host = parseSNMP(snmp); !c.host {
		fail("/proc/net/snmp: no Udp counters")
		return c
	}
	snmp6, err := os.ReadFile("/proc/net/snmp6")
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		c.host = false
		fail(err.Error())
	default:
		rcvbuf, inErrors, ok := parseSNMP6(snmp6)
		if !ok {
			c.host = false
			fail("/proc/net/snmp6: no Udp6 counters")
			break
		}
		c.rcvbuf += rcvbuf
		c.inErrors += inErrors
	}
	if !c.host {
		c.rcvbuf, c.inErrors = 0, 0
	}
	return c
}
