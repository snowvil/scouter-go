package transport

import (
	"bufio"
	"bytes"
	"net"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// UDP socket diagnostics (Go only, R8): the SO_RCVBUF the kernel granted and,
// on Linux, the kernel's receive drops. The receive loop never reads them;
// Stats reads the kernel at most once per kernelStatsEvery (/health and the
// stats line call it, each read scans /proc/net/udp once). On other systems
// UDPStats.KernelCounters and HostCounters stay false: macOS has no
// per-socket drop counter, and its host-wide one (netstat -s "dropped due to
// full socket buffers") is not read.
const kernelStatsEvery = time.Second

// kernelCounters: the socket part and the host part are read and fail
// independently; err names the first failure.
type kernelCounters struct {
	socket, host                    bool
	err                             string
	drops, queued, rcvbuf, inErrors uint64
}

type udpDiag struct {
	requested, effective int
	inode                uint64 // Linux socket inode, 0 when unknown
	mu                   sync.Mutex
	at                   time.Time
	last                 kernelCounters
}

// newUDPDiag sets SO_RCVBUF when requested > 0 and reads what the kernel
// granted. A failed read leaves the effective size 0.
func newUDPDiag(conn *net.UDPConn, requested int) (*udpDiag, error) {
	if requested > 0 {
		if err := conn.SetReadBuffer(requested); err != nil {
			return nil, err
		}
	}
	d := &udpDiag{requested: requested, inode: socketInode(conn)}
	if rc, err := conn.SyscallConn(); err == nil {
		rc.Control(func(fd uintptr) {
			if v, err := syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF); err == nil {
				d.effective = v
			}
		})
	}
	return d, nil
}

func (d *udpDiag) fill(st *UDPStats, now time.Time) {
	st.ReadBufferRequested, st.ReadBufferEffective = d.requested, d.effective
	d.mu.Lock()
	if d.at.IsZero() || now.Sub(d.at) >= kernelStatsEvery {
		d.last, d.at = readKernelUDP(d.inode), now
	}
	c := d.last
	d.mu.Unlock()
	st.KernelCounters, st.HostCounters, st.KernelCountersError = c.socket, c.host, c.err
	if c.socket {
		st.SocketDrops, st.SocketQueuedBytes = c.drops, c.queued
	}
	if c.host {
		st.HostRcvbufErrors, st.HostInErrors = c.rcvbuf, c.inErrors
	}
}

// parseProcUDP finds inode in a /proc/net/udp{,6} table and returns its drops
// and rx_queue columns.
func parseProcUDP(table []byte, inode uint64) (drops, queued uint64, ok bool) {
	want := strconv.FormatUint(inode, 10)
	lines := bufio.NewScanner(bytes.NewReader(table))
	lines.Scan() // header
	for lines.Scan() {
		f := bytes.Fields(lines.Bytes())
		// sl local rem st tx_queue:rx_queue tr:tm retrnsmt uid timeout inode ref pointer drops
		if len(f) < 13 || string(f[9]) != want {
			continue
		}
		_, rx, found := bytes.Cut(f[4], []byte(":"))
		q, err1 := strconv.ParseUint(string(rx), 16, 64)
		d, err2 := strconv.ParseUint(string(f[12]), 10, 64)
		if !found || err1 != nil || err2 != nil {
			return 0, 0, false
		}
		return d, q, true
	}
	return 0, 0, false
}

// parseSNMP returns RcvbufErrors and InErrors of the "Udp:" rows of
// /proc/net/snmp (a header row of names, then a row of values).
func parseSNMP(data []byte) (rcvbuf, inErrors uint64, ok bool) {
	var names [][]byte
	lines := bufio.NewScanner(bytes.NewReader(data))
	for lines.Scan() {
		f := bytes.Fields(lines.Bytes())
		if len(f) == 0 || string(f[0]) != "Udp:" {
			continue
		}
		if names == nil {
			names = f
			continue
		}
		found := 0
		for i := 1; i < len(f) && i < len(names); i++ {
			v, err := strconv.ParseUint(string(f[i]), 10, 64)
			switch string(names[i]) {
			case "RcvbufErrors":
				rcvbuf, found = v, found+1
			case "InErrors":
				inErrors, found = v, found+1
			default:
				continue
			}
			if err != nil {
				return 0, 0, false
			}
		}
		return rcvbuf, inErrors, found == 2
	}
	return 0, 0, false
}

// parseSNMP6 returns Udp6RcvbufErrors and Udp6InErrors of /proc/net/snmp6.
func parseSNMP6(data []byte) (rcvbuf, inErrors uint64, ok bool) {
	found := 0
	lines := bufio.NewScanner(bytes.NewReader(data))
	for lines.Scan() {
		f := bytes.Fields(lines.Bytes())
		if len(f) != 2 {
			continue
		}
		var p *uint64
		switch string(f[0]) {
		case "Udp6RcvbufErrors":
			p = &rcvbuf
		case "Udp6InErrors":
			p = &inErrors
		default:
			continue
		}
		v, err := strconv.ParseUint(string(f[1]), 10, 64)
		if err != nil {
			return 0, 0, false
		}
		*p = v
		found++
	}
	return rcvbuf, inErrors, found == 2
}
