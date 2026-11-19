package main

import "testing"

func TestKernelUDPCounters(t *testing.T) {
	darwin := "udp:\n 17 dropped due to no socket\n 4 dropped due to full socket buffers\n 2 with bad checksum\n"
	d, err := parseKernelUDP("darwin", darwin)
	if err != nil || d["dropped due to full socket buffers"] != 4 {
		t.Fatalf("%v %v", d, err)
	}
	l, err := parseKernelUDP("linux", "Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors\nUdp: 9 0 4 10 4 0\n")
	if err != nil {
		t.Fatal(err)
	}
	b := map[string]uint64{}
	for k := range l {
		b[k] = 0
	}
	delta := kernelDelta(kernelUDPSnapshot{Counters: b}, kernelUDPSnapshot{Counters: l})
	if !delta.Available || delta.Loss != 4 {
		t.Fatalf("double counted Linux errors: %+v", delta)
	}
	for _, platform := range []string{"darwin", "linux", "other"} {
		if _, err := parseKernelUDP(platform, "unknown"); err == nil {
			t.Fatal("missing counters accepted")
		}
	}
	delta = kernelDelta(kernelUDPSnapshot{Counters: l}, kernelUDPSnapshot{Counters: b})
	if delta.Available {
		t.Fatal("counter reset must be unknown")
	}
}
