package main

import "testing"

func TestReplayModes(t *testing.T) {
	for _, args := range [][]string{
		{"--sink-only", "--senders", "2"},
		{"--server-udp", "127.0.0.1:6100", "--server-tcp", "127.0.0.1:6100", "--server-pid", "123"},
	} {
		if _, err := parseOptions(args); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{
		{"--server-udp", "10.0.0.1:6100", "--server-tcp", "127.0.0.1:6100"},
		{"--server-udp", "127.0.0.1:6100"},
		{"--sink-only", "--server-tcp", "127.0.0.1:6100", "--server-udp", "127.0.0.1:6100"},
		{"--server-udp", "127.0.0.1:6100", "--server-tcp", "127.0.0.1:6100", "--stop", "kill"},
	} {
		if _, err := parseOptions(args); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
