package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type kernelUDPSnapshot struct {
	Counters map[string]uint64 `json:"counters"`
	Error    string            `json:"error,omitempty"`
}
type kernelUDPReport struct {
	Available bool              `json:"available"`
	Before    kernelUDPSnapshot `json:"before"`
	After     kernelUDPSnapshot `json:"after"`
	Delta     map[string]uint64 `json:"delta"`
	Loss      uint64            `json:"loss"`
	Note      string            `json:"note"`
}

func readKernelUDP() kernelUDPSnapshot {
	var data []byte
	var err error
	switch runtime.GOOS {
	case "darwin":
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		data, err = exec.CommandContext(ctx, "netstat", "-s", "-p", "udp").Output()
	case "linux":
		data, err = os.ReadFile("/proc/net/snmp")
	default:
		err = fmt.Errorf("unsupported OS %s", runtime.GOOS)
	}
	if err != nil {
		return kernelUDPSnapshot{Error: err.Error()}
	}
	counts, err := parseKernelUDP(runtime.GOOS, string(data))
	if err != nil {
		return kernelUDPSnapshot{Error: err.Error()}
	}
	return kernelUDPSnapshot{Counters: counts}
}
func parseKernelUDP(platform, data string) (map[string]uint64, error) {
	out := map[string]uint64{}
	if platform == "darwin" {
		for _, line := range strings.Split(data, "\n") {
			f := strings.Fields(line)
			if len(f) < 2 {
				continue
			}
			n, err := strconv.ParseUint(f[0], 10, 64)
			if err != nil {
				continue
			}
			label := strings.Join(f[1:], " ")
			if strings.Contains(label, "dropped") || strings.Contains(label, "bad checksum") || strings.Contains(label, "bad data length") || strings.Contains(label, "incomplete header") {
				out[label] = n
			}
		}
		// A recognized mandatory field prevents treating unknown output as zero loss.
		if _, ok := out["dropped due to full socket buffers"]; !ok {
			return nil, fmt.Errorf("netstat full socket buffer counter missing")
		}
	} else if platform == "linux" {
		var keys []string
		for _, line := range strings.Split(data, "\n") {
			f := strings.Fields(line)
			if len(f) < 2 || f[0] != "Udp:" {
				continue
			}
			if keys == nil {
				keys = f[1:]
				continue
			}
			if len(keys) != len(f)-1 {
				return nil, fmt.Errorf("UDP SNMP field mismatch")
			}
			for i, k := range keys {
				n, err := strconv.ParseUint(f[i+1], 10, 64)
				if err != nil {
					return nil, err
				}
				out[k] = n
			}
			break
		}
		if _, ok := out["InErrors"]; !ok {
			return nil, fmt.Errorf("UDP InErrors missing")
		}
	} else {
		return nil, fmt.Errorf("unsupported platform")
	}
	return out, nil
}
func kernelDelta(before, after kernelUDPSnapshot) kernelUDPReport {
	r := kernelUDPReport{Before: before, After: after, Delta: map[string]uint64{}, Note: "host-wide counters, including unrelated processes; Linux InErrors includes RcvbufErrors (not summed twice)"}
	if before.Error != "" || after.Error != "" || len(before.Counters) == 0 {
		return r
	}
	for k, b := range before.Counters {
		a, ok := after.Counters[k]
		if !ok || a < b {
			r.Note += "; missing/reset counter"
			return r
		}
		r.Delta[k] = a - b
	}
	r.Available = true
	if _, ok := r.Delta["InErrors"]; ok {
		r.Loss = r.Delta["InErrors"] + r.Delta["NoPorts"] + r.Delta["SndbufErrors"]
	} else {
		for _, v := range r.Delta {
			r.Loss += v
		}
	}
	return r
}
