package service

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/account"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// loginGoldenDir holds the original LoginService LOGIN responses after
// scouter.conf changes applied by Configure.reload(true)
// (scripts/java-management.py, scenario "login").
const loginGoldenDir = "testdata/management/login"

type loginApplied struct {
	ServerID          string `json:"server_id"`
	ExtLinkName       string `json:"ext_link_name"`
	ExtLinkURLPattern string `json:"ext_link_url_pattern"`
	TagCount          bool   `json:"tagcnt_enabled"`
	SoTimeout         int32  `json:"net_tcp_client_so_timeout_ms"`
}

type loginGoldenCase struct {
	Name     string        `json:"name"`
	Command  string        `json:"command"`
	Request  string        `json:"request"`
	Response string        `json:"response"`
	Applied  *loginApplied `json:"applied"`
}

func readGoldenPack(t *testing.T, dir, name string) protocol.Pack {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	p, err := protocol.DecodePack(b, protocol.Limits{})
	if err != nil {
		t.Fatal(name, err)
	}
	return p
}

// javaLoginResponse decodes HasNEXT + MapPack + NoNEXT.
func javaLoginResponse(t *testing.T, name string) *protocol.MapPack {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(loginGoldenDir, name))
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 2 || b[0] != transport.HasNext || b[len(b)-1] != transport.NoNext {
		t.Fatal(name, "framing")
	}
	p, err := protocol.DecodePack(b[1:len(b)-1], protocol.Limits{})
	if err != nil {
		t.Fatal(name, err)
	}
	return p.(*protocol.MapPack)
}

func keysWithout(entries []protocol.Entry, skip ...string) string {
	var keys []string
	for _, e := range entries {
		drop := false
		for _, s := range skip {
			drop = drop || e.Key == s
		}
		if !drop {
			keys = append(keys, e.Key)
		}
	}
	return strings.Join(keys, ",")
}

// TestLoginLiveSettingsJavaOracle sends the golden LOGIN requests to the Go
// handler with the Settings that Configure held at each case (facts
// "applied") and compares the per-request entries with the Java response.
func TestLoginLiveSettingsJavaOracle(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(loginGoldenDir, "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Cases []loginGoldenCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	if len(golden.Cases) != 5 {
		t.Fatalf("golden cases %d", len(golden.Cases))
	}
	m, err := account.Open(account.Config{Dir: t.TempDir(), Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	seoul, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	s, conn, r := startService(t, Config{Accounts: m, ServerID: "<hostname>", Location: seoul}, &fakeBackend{})
	compared := 0
	for _, c := range golden.Cases {
		if c.Command != "LOGIN" {
			t.Fatal(c.Name, c.Command)
		}
		java := javaLoginResponse(t, c.Response)
		if c.Applied != nil {
			if c.Name == "login-default" {
				// Configure defaults with no key in scouter.conf: DefaultSettings.
				want := DefaultSettings(c.Applied.ServerID)
				got := Settings{ServerID: c.Applied.ServerID, ExtLinkName: c.Applied.ExtLinkName, ExtLinkURLPattern: c.Applied.ExtLinkURLPattern, ClientSoTimeoutMillis: c.Applied.SoTimeout, TagCountEnabled: c.Applied.TagCount, SearchXLogMaxCount: DefaultSearchXLogMax}
				if got != want || s.Settings() != want {
					t.Fatalf("defaults: java %#v go %#v", got, s.Settings())
				}
			} else {
				live := s.Settings()
				live.ServerID, live.ExtLinkName, live.ExtLinkURLPattern = c.Applied.ServerID, c.Applied.ExtLinkName, c.Applied.ExtLinkURLPattern
				live.ClientSoTimeoutMillis, live.TagCountEnabled = c.Applied.SoTimeout, c.Applied.TagCount
				s.SetSettings(live)
			}
		}
		send(t, conn, "LOGIN", 0, readGoldenPack(t, loginGoldenDir, c.Request))
		goPack := readPacks(t, r)[0].(*protocol.MapPack)
		if c.Applied == nil {
			// Failure: session 0 and error; Go does not echo the request entries (intended difference).
			if !reflect.DeepEqual(mapValue(goPack, "session"), mapValue(java, "session")) || !reflect.DeepEqual(mapValue(goPack, "error"), mapValue(java, "error")) || keysWithout(goPack.Entries) != "id,session,error" {
				t.Fatalf("%s: %#v", c.Name, goPack)
			}
			continue
		}
		for _, key := range []string{"server_id", "menu", "so_time_out", "ext_link_name", "ext_link_url_pattern", "type", "email"} {
			if !reflect.DeepEqual(mapValue(goPack, key), mapValue(java, key)) {
				t.Fatalf("%s %s: go %#v java %#v", c.Name, key, mapValue(goPack, key), mapValue(java, key))
			}
		}
		// Entry order apart from the echoed request entries and the version
		// entry, whose Java position is the request's "version" key.
		if g, j := keysWithout(goPack.Entries, "version"), keysWithout(java.Entries, "pass", "ip", "hostname", "version"); g != j {
			t.Fatalf("%s order: go %s java %s", c.Name, g, j)
		}
		compared++
	}
	if compared != 4 {
		t.Fatal(compared)
	}
}
