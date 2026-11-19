package transport

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// R8: reassembly bounds are counted apart from Malformed; the error stays an
// ErrLimit.
func TestUDPAssemblyLimitErrors(t *testing.T) {
	first := fixture(t, "udp-cafm-1")
	now := time.Now()
	p1, p2 := netip.MustParseAddrPort("127.0.0.1:1"), netip.MustParseAddrPort("127.0.0.1:2")
	pending, _ := NewReassembler(UDPConfig{MaxAssemblies: 1})
	if _, err := pending.Decode(p1, first, now); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		config UDPConfig
		sender netip.AddrPort
		a      *Reassembler
	}{
		"pending assemblies": {sender: p2, a: pending},
		"assembly bytes":     {config: UDPConfig{MaxAssemblyBytes: 5}, sender: p1},
		"buffered bytes":     {config: UDPConfig{MaxBufferedBytes: 5}, sender: p1},
		"fragment count":     {config: UDPConfig{MaxFragments: 1}, sender: p1},
	} {
		a := c.a
		if a == nil {
			a, _ = NewReassembler(c.config)
		}
		if _, err := a.Decode(c.sender, first, now); !errors.Is(err, ErrAssemblyLimit) || !errors.Is(err, ErrLimit) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestUDPReceiverCountsAssemblyLimitApart(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan ErrorEvent, 8)
	receiver, err := NewUDPReceiver(conn, UDPConfig{MaxAssemblies: 1}, func(context.Context, Packet) error { return nil }, func(e ErrorEvent) { events <- e })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- receiver.Serve(ctx) }()
	defer func() { cancel(); <-done }()
	for i := 0; i < 2; i++ { // two senders: the second assembly is over the bound
		client, err := net.DialUDP("udp", nil, conn.LocalAddr().(*net.UDPAddr))
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		if _, err := client.Write(fixture(t, "udp-cafm-1")); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case e := <-events:
		if e.Operation != "udp.assembly" || !errors.Is(e.Err, ErrAssemblyLimit) {
			t.Fatalf("event %+v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("assembly limit not reported")
	}
	if st := receiver.Stats(); st.AssemblyRejected != 1 || st.Malformed != 0 {
		t.Fatalf("stats %+v", st)
	}
}

// R8: the SO_RCVBUF the receiver set and the one the kernel reports.
func TestUDPReceiverReadBufferStats(t *testing.T) {
	const requested = 256 << 10
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := NewUDPReceiver(conn, UDPConfig{ReadBuffer: requested}, func(context.Context, Packet) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	st := receiver.Stats()
	want := requested
	switch runtime.GOOS {
	case "linux": // twice the granted size, capped by net.core.rmem_max
		raw, err := os.ReadFile("/proc/sys/net/core/rmem_max")
		if err != nil {
			t.Fatal(err)
		}
		rmemMax, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil {
			t.Fatal(err)
		}
		want = 2 * min(requested, rmemMax)
		if !st.KernelCounters || !st.HostCounters || st.KernelCountersError != "" {
			t.Fatalf("Linux kernel counters not read: %+v", st)
		}
	case "darwin":
		if st.KernelCounters || st.HostCounters || st.KernelCountersError != "kernel UDP counters not supported on darwin" {
			t.Fatalf("kernel counters on darwin: %+v", st)
		}
	}
	if st.ReadBufferRequested != requested || st.ReadBufferEffective != want {
		t.Fatalf("read buffer requested=%d effective=%d want %d", st.ReadBufferRequested, st.ReadBufferEffective, want)
	}
	if _, err := NewUDPReceiver(conn, UDPConfig{ReadBuffer: -1}, func(context.Context, Packet) error { return nil }, nil); !errors.Is(err, ErrMalformed) {
		t.Fatal(err)
	}
}

func TestParseProcUDP(t *testing.T) {
	const table = `   sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
  410: 00000000:17D4 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 4242 2 0000000000000000 0
  411: 0100007F:18B4 00000000:0000 07 00000000:00001200 00:00000000 00000000   501        0 31337 2 0000000000000000 17
`
	drops, queued, ok := parseProcUDP([]byte(table), 31337)
	if !ok || drops != 17 || queued != 0x1200 {
		t.Fatal(drops, queued, ok)
	}
	if _, _, ok := parseProcUDP([]byte(table), 1); ok {
		t.Fatal("missing inode found")
	}
	const snmp = `Ip: Forwarding DefaultTTL
Ip: 1 64
Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors InCsumErrors IgnoredMulti MemErrors
Udp: 100 2 7 90 5 0 1 0 0
UdpLite: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors InCsumErrors IgnoredMulti MemErrors
UdpLite: 0 0 99 0 99 0 0 0 0
`
	rcvbuf, in, ok := parseSNMP([]byte(snmp))
	if !ok || rcvbuf != 5 || in != 7 {
		t.Fatal(rcvbuf, in, ok)
	}
	const snmp6 = "Udp6InDatagrams                 \t10\nUdp6InErrors                    \t3\nUdp6RcvbufErrors                \t2\nUdpLite6InErrors                \t50\n"
	rcvbuf, in, ok = parseSNMP6([]byte(snmp6))
	if !ok || rcvbuf != 2 || in != 3 {
		t.Fatal(rcvbuf, in, ok)
	}
}

func TestParseProcAbnormal(t *testing.T) {
	for name, table := range map[string]string{
		"empty":        "",
		"header only":  "   sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops\n",
		"short row":    "h\n  1: 0:0 0:0 07 0:0 00:0 0 0 0 31337\n",
		"bad rx_queue": "h\n  1: 0:0 0:0 07 00000000 00:0 0 0 0 31337 2 0 0\n",
		"bad hex":      "h\n  1: 0:0 0:0 07 0:zz 00:0 0 0 0 31337 2 0 0\n",
		"bad drops":    "h\n  1: 0:0 0:0 07 0:0 00:0 0 0 0 31337 2 0 -1\n",
		"inode prefix": "h\n  1: 0:0 0:0 07 0:0 00:0 0 0 0 313370 2 0 5\n",
	} {
		if d, q, ok := parseProcUDP([]byte(table), 31337); ok {
			t.Errorf("%s: parsed %d %d", name, d, q)
		}
	}
	for name, snmp := range map[string]string{
		"empty":        "",
		"header only":  "Udp: InDatagrams InErrors RcvbufErrors\n",
		"no rcvbuf":    "Udp: InDatagrams InErrors\nUdp: 1 2\n",
		"bad value":    "Udp: InErrors RcvbufErrors\nUdp: 1 x\n",
		"short values": "Udp: InErrors RcvbufErrors\nUdp: 1\n",
	} {
		if r, i, ok := parseSNMP([]byte(snmp)); ok {
			t.Errorf("snmp %s: parsed %d %d", name, r, i)
		}
	}
	for name, snmp6 := range map[string]string{
		"empty":     "",
		"one":       "Udp6InErrors 3\n",
		"bad value": "Udp6InErrors 3\nUdp6RcvbufErrors -2\n",
	} {
		if r, i, ok := parseSNMP6([]byte(snmp6)); ok {
			t.Errorf("snmp6 %s: parsed %d %d", name, r, i)
		}
	}
}
