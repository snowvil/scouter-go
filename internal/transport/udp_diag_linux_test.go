package transport

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// R8 (Linux): datagrams the kernel dropped because nobody read the socket
// show in SocketDrops and the host-wide RcvbufErrors.
func TestUDPKernelDropsLinux(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := NewUDPReceiver(conn, UDPConfig{ReadBuffer: 4096}, func(context.Context, Packet) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	before := receiver.Stats()
	client, err := net.DialUDP("udp", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	payload := make([]byte, 1000)
	for i := 0; i < 200; i++ { // not served: the small buffer overflows
		client.Write(payload)
	}
	time.Sleep(kernelStatsEvery + 100*time.Millisecond) // past the stats cache
	st := receiver.Stats()
	if !st.KernelCounters || !st.HostCounters || st.SocketDrops == 0 || st.SocketQueuedBytes == 0 || st.HostRcvbufErrors <= before.HostRcvbufErrors {
		t.Fatalf("before %+v after %+v", before, st)
	}
}

// A wildcard listener is an IPv6 dual-stack socket (/proc/net/udp6) where
// IPv6 is available, an IPv4 one (/proc/net/udp) otherwise.
func TestUDPKernelCountersWildcardLinux(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := NewUDPReceiver(conn, UDPConfig{}, func(context.Context, Packet) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	if st := receiver.Stats(); !st.KernelCounters || !st.HostCounters || st.KernelCountersError != "" || st.ReadBufferRequested != 0 || st.ReadBufferEffective == 0 {
		t.Fatalf("%s: %+v", conn.LocalAddr(), st)
	}
}

// A closed socket loses its /proc row; the host counters are still read.
func TestUDPKernelCountersPartsLinux(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := NewUDPReceiver(conn, UDPConfig{}, func(context.Context, Packet) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	receiver.Close()
	if st := receiver.Stats(); st.KernelCounters || !st.HostCounters || !strings.Contains(st.KernelCountersError, "socket not found") {
		t.Fatalf("%+v", st)
	}
	if c := readKernelUDP(0); c.socket || !c.host || c.err != "socket inode unknown" {
		t.Fatalf("%+v", c)
	}
}
