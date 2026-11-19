package service

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/snowvil/scouter-go/internal/account"
)

func TestReadRegistryMatchesSourceScope(t *testing.T) {
	data, err := os.ReadFile("../../docs/compatibility/query-scope.json")
	if err != nil {
		t.Fatal(err)
	}
	var scope struct {
		Base     []string `json:"supported_at_scope_start"`
		Commands []struct {
			Command  string `json:"command_literal"`
			Category string `json:"category"`
		} `json:"commands"`
	}
	if err := json.Unmarshal(data, &scope); err != nil {
		t.Fatal(err)
	}
	read := map[string]bool{}
	mutations := map[string]bool{}
	for _, command := range scope.Base {
		read[command] = true
	}
	for _, entry := range scope.Commands {
		if entry.Category == "mutation" {
			mutations[entry.Command] = true
		} else {
			read[entry.Command] = true
		}
	}
	// The only mutations registered are the six KV/TTL writes verified against
	// the original KeyValueStoreService oracle; every other mutation stays out.
	allowedMutations := map[string]bool{}
	for _, command := range LocalStoreMutationCommands() {
		if !mutations[command] {
			t.Errorf("KV mutation missing from scope: %s", command)
		}
		allowedMutations[command] = true
	}
	// The management handler owns the RemoteControlManager queue (CHECK_JOB
	// included), DB delete, object removal and the Agent mutation relays; the
	// configure handler owns the server/telegraf/counters-site writes, object
	// types and alert script saves (plus the five formerly JVM-only READs);
	// the account handler owns the AccountService writes. Together with the KV
	// writes every one of the 45 mutations is registered.
	for _, group := range [][]string{ManageCommands(), ConfigureCommands(), AccountCommands()} {
		for _, command := range group {
			if mutations[command] {
				allowedMutations[command] = true
			}
		}
	}
	// The AccountService writes register only with an account store, so the
	// registry is checked with one (Java always has the account files).
	store, err := account.Open(account.Config{Dir: t.TempDir(), Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{config: Config{Accounts: store}, backend: &agentReadFake{fakeBackend: &fakeBackend{}}}
	implemented := map[string]bool{}
	implementedMutations := 0
	for _, command := range s.SupportedCommands() {
		if implemented[command] {
			t.Errorf("duplicate registration: %s", command)
		}
		if allowedMutations[command] {
			implementedMutations++
			implemented[command] = true
			continue
		}
		if !read[command] || mutations[command] {
			t.Errorf("out of READ scope: %s", command)
		}
		implemented[command] = true
	}
	if implementedMutations != len(allowedMutations) {
		t.Errorf("mutation registrations: %d, allowed %d", implementedMutations, len(allowedMutations))
	}
	for command := range read {
		if !implemented[command] {
			t.Errorf("unhandled READ: %s", command)
		}
	}
	if len(allowedMutations) != 45 {
		t.Errorf("registered mutations: %d of 45", len(allowedMutations))
	}
	if len(read) != 184 || len(implemented) != len(read)+implementedMutations || len(mutations) != 45 {
		t.Fatalf("scope read=%d implemented=%d mutations=%d", len(read), len(implemented), len(mutations))
	}
}
