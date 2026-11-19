package main

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

// managedServerFlags are set by the tool (fresh DB, loopback ports, ready file,
// credentials, isolated directories) or by a dedicated tool option. Passing them
// through --server-arg would bypass the safety checks, so they are rejected.
var managedServerFlags = map[string]string{
	"db": "--db", "udp": "--bind", "tcp": "--bind", "ready-file": "(tool managed)",
	"user": "--user", "password": "--password", "timezone": "--timezone",
	"log-dir": "(tool managed)", "plugin-dir": "(tool managed)", "telegraf-config": "(tool managed)",
	"compress-xlog": "--compress-xlog", "compress-profile": "--compress-profile",
}

// flagName returns the Go flag name of a "-x", "--x" or "--x=v" argument.
func flagName(arg string) (string, bool) {
	if !strings.HasPrefix(arg, "-") || arg == "-" || arg == "--" {
		return "", false
	}
	name := strings.TrimLeft(arg, "-")
	name, _, _ = strings.Cut(name, "=")
	return name, name != ""
}

func validateServerArgs(args []string) error {
	for _, a := range args {
		if name, ok := flagName(a); ok {
			if opt, managed := managedServerFlags[name]; managed {
				return fmt.Errorf("--server-arg %q: server flag -%s is managed by the tool (use %s)", a, name, opt)
			}
		}
	}
	return nil
}

// serverArgValue finds the last value of a flag in the passthrough args. A bare
// boolean flag returns "true".
func serverArgValue(args []string, want string) (string, bool) {
	value, found := "", false
	for i := 0; i < len(args); i++ {
		name, ok := flagName(args[i])
		if !ok || name != want {
			continue
		}
		found = true
		if _, v, hasValue := strings.Cut(strings.TrimLeft(args[i], "-"), "="); hasValue {
			value = v
		} else if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			value = args[i+1]
			i++
		} else {
			value = "true"
		}
	}
	return value, found
}

// binaryFlagDefault reads a flag default from the binary's Go flag usage (-h).
func binaryFlagDefault(binary, name string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, _ := exec.CommandContext(ctx, binary, "-h").CombinedOutput() // -h exits non-zero on some versions
	return parseFlagDefault(string(out), name)
}

func parseFlagDefault(usage, name string) (string, error) {
	re := regexp.MustCompile(`(?m)^\s+-` + regexp.QuoteMeta(name) + `(?:\s+\S+)?\s*\n((?:\s{4,}.*\n?)+)`)
	m := re.FindStringSubmatch(usage)
	if m == nil {
		return "", fmt.Errorf("server binary has no -%s flag", name)
	}
	if d := regexp.MustCompile(`\(default ([^)]*)\)`).FindStringSubmatch(m[1]); d != nil {
		return d[1], nil
	}
	return "", nil // Go omits zero defaults (false, 0, "")
}

type fsyncInfo struct {
	SyncInterval string `json:"sync_interval"`
	Source       string `json:"sync_interval_source"`
	Policy       string `json:"fsync_policy"`
	Note         string `json:"fsync_policy_operator_note,omitempty"`
	Error        string `json:"error,omitempty"`
}

// describeFsync derives the fsync policy from --sync-interval (passthrough value,
// otherwise the binary default). It describes the server contract; syscalls are
// not traced.
func describeFsync(args []string, binary, note string) fsyncInfo {
	info := fsyncInfo{Note: note}
	if v, ok := serverArgValue(args, "sync-interval"); ok {
		info.SyncInterval, info.Source = v, "--server-arg"
	} else if v, err := binaryFlagDefault(binary, "sync-interval"); err == nil {
		info.SyncInterval, info.Source = v, "server binary default (-h)"
	} else {
		info.Source, info.Error = "unavailable", err.Error()
		info.Policy = "unknown: server binary has no --sync-interval; assume sync only on file eviction/Close"
		return info
	}
	d, err := time.ParseDuration(info.SyncInterval)
	switch {
	case info.SyncInterval == "" || (err == nil && d == 0):
		info.Policy = "periodic fsync disabled (--sync-interval=0); data is synced on file eviction/Close only"
	case err != nil:
		info.Policy = "unparsed --sync-interval " + info.SyncInterval
		info.Error = err.Error()
	default:
		info.Policy = fmt.Sprintf("periodic fsync of data files and index headers every %s, plus sync on eviction/Close; up to one interval of acknowledged data may be lost on crash", d)
	}
	return info
}
