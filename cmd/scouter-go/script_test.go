package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestScriptArgs runs scripts/scouter-go.sh args (POSIX sh) and checks the
// --conf and --http it passes against how the server reads the same files:
// /health moves off port 6180 only when the Java servlets take it, and a
// missing default scouter.conf is left to the server as optional.
func TestScriptArgs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX sh script")
	}
	src, err := os.ReadFile("../../scripts/scouter-go.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name, conf, telegraf             string
		env                              []string
		wantConf, wantHTTP, wantShutdown string // "" = flag absent
		wantErr                          string // the script refuses with this message
	}{
		{name: "no conf", wantHTTP: "127.0.0.1:6180"},
		{name: "servlet off", conf: "net_http_server_enabled=false\n", wantConf: "conf/scouter.conf", wantHTTP: "127.0.0.1:6180"},
		{name: "servlet on", conf: "net_http_server_enabled = TRUE \r\n", wantConf: "conf/scouter.conf", wantHTTP: "127.0.0.1:6181"},
		{name: "space separator", conf: "net_http_server_enabled true\n", wantConf: "conf/scouter.conf", wantHTTP: "127.0.0.1:6181"},
		{name: "leading zero port", conf: "net_http_server_enabled=true\nnet_http_port=+06180\n", wantConf: "conf/scouter.conf", wantHTTP: "127.0.0.1:6181"},
		{name: "other port", conf: "net_http_server_enabled=true\nnet_http_port: 7000 \n", wantConf: "conf/scouter.conf", wantHTTP: "127.0.0.1:6180"},
		// Java Properties: the value after the '=' separator is "=true".
		{name: "double equals", conf: "net_http_server_enabled==true\n", wantConf: "conf/scouter.conf", wantHTTP: "127.0.0.1:6180"},
		{name: "space then equals", conf: "net_http_server_enabled = =true\n", wantConf: "conf/scouter.conf", wantHTTP: "127.0.0.1:6180"},
		{name: "bare key resets", conf: "net_http_server_enabled=true\nnet_http_server_enabled\n", wantConf: "conf/scouter.conf", wantHTTP: "127.0.0.1:6180"},
		{name: "commented", conf: "#net_http_server_enabled=true\n", wantConf: "conf/scouter.conf", wantHTTP: "127.0.0.1:6180"},
		{name: "explicit http", conf: "net_http_server_enabled=true\n", env: []string{"SCOUTER_HTTP=127.0.0.1:6180"}, wantConf: "conf/scouter.conf", wantHTTP: "127.0.0.1:6180"},
		{name: "http off", conf: "net_http_server_enabled=true\n", env: []string{"SCOUTER_HTTP="}, wantConf: "conf/scouter.conf"},
		{name: "conf off", conf: "net_http_server_enabled=true\n", telegraf: "<TelegrafConfig/>\n", env: []string{"SCOUTER_CONF="}, wantConf: "=", wantHTTP: "127.0.0.1:6180"},
		{name: "telegraf default on", telegraf: "<TelegrafConfig>\n<measurements/>\n</TelegrafConfig>\n", wantHTTP: "127.0.0.1:6181"},
		{name: "telegraf on", telegraf: "<TelegrafConfig>\n  <enabled> 1</enabled>\n</TelegrafConfig>\n", wantHTTP: "127.0.0.1:6181"},
		{name: "telegraf off", telegraf: "<TelegrafConfig>\n  <enabled>false</enabled>\n  <measurements><measurement><enabled>true</enabled></measurement></measurements>\n</TelegrafConfig>\n", wantHTTP: "127.0.0.1:6180"},
		{name: "telegraf empty", telegraf: "<TelegrafConfig>\n  <enabled/>\n</TelegrafConfig>\n", wantHTTP: "127.0.0.1:6180"},
		{name: "explicit missing conf", env: []string{"SCOUTER_CONF=conf/other.conf"}, wantConf: "conf/other.conf", wantHTTP: "127.0.0.1:6180"},
		{name: "stop timeout", env: []string{"SCOUTER_STOP_TIMEOUT=40"}, wantHTTP: "127.0.0.1:6180", wantShutdown: "30s"},
		// Decimal, not octal: $((08 - 10)) fails in sh.
		{name: "stop timeout leading zero", env: []string{"SCOUTER_STOP_TIMEOUT=08"}, wantHTTP: "127.0.0.1:6180", wantShutdown: "5s"},
		// Not whole seconds: stop would never send SIGKILL.
		{name: "stop timeout unit", env: []string{"SCOUTER_STOP_TIMEOUT=30s"}, wantErr: "SCOUTER_STOP_TIMEOUT must be whole seconds"},
		{name: "stop timeout empty", env: []string{"SCOUTER_STOP_TIMEOUT="}, wantHTTP: "127.0.0.1:6180"}, // the 30s default
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			script := filepath.Join(dir, "scouter-go.sh")
			if err := os.WriteFile(script, src, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(dir, "conf"), 0o755); err != nil {
				t.Fatal(err)
			}
			if c.conf != "" {
				if err := os.WriteFile(filepath.Join(dir, "conf", "scouter.conf"), []byte(c.conf), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if c.telegraf != "" {
				if err := os.WriteFile(filepath.Join(dir, "conf", "scouter-telegraf.xml"), []byte(c.telegraf), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command("sh", script, "args")
			// Run elsewhere: relative paths must resolve in the script directory.
			cmd.Dir = t.TempDir()
			cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "SCOUTER_ENV_FILE=/dev/null"}, c.env...)
			out, err := cmd.CombinedOutput()
			if c.wantErr != "" {
				if cmd.ProcessState.ExitCode() != 2 || !strings.Contains(string(out), c.wantErr) {
					t.Fatalf("want exit 2 with %q, got %v: %s", c.wantErr, err, out)
				}
				return
			}
			if err != nil {
				t.Fatalf("%v: %s", err, out)
			}
			args := strings.Split(strings.TrimSpace(string(out)), "\n")
			value := func(name string) string {
				for i, a := range args {
					if a == name+"=" {
						return "="
					}
					if a == name && i+1 < len(args) {
						return strings.TrimPrefix(args[i+1], dir+"/")
					}
				}
				return ""
			}
			if got := value("--conf"); got != c.wantConf {
				t.Errorf("--conf %q, want %q (%q)", got, c.wantConf, args)
			}
			if got := value("--http"); got != c.wantHTTP {
				t.Errorf("--http %q, want %q (%q)", got, c.wantHTTP, args)
			}
			if got := value("--shutdown-timeout"); got != c.wantShutdown {
				t.Errorf("--shutdown-timeout %q, want %q (%q)", got, c.wantShutdown, args)
			}
		})
	}
}
