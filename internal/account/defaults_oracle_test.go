package account

// Java oracle check of the account file parser: the original
// AccountFileHandler.parse / GroupFileHandler.parse results for the Scouter
// default account.xml / account_group.xml (testdata/management/account
// facts.parsed_default_accounts / parsed_default_groups, produced by
// scripts/java-management.py). The rest of the AccountService golden is
// replayed by internal/service/account_golden_test.go.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const javaAccountGolden = "../service/testdata/management/account"

func TestDefaultFilesJavaOracleParse(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(javaAccountGolden, "cases.json"))
	if err != nil {
		t.Fatalf("Java oracle golden missing: %v", err)
	}
	var root struct {
		Facts struct {
			Accounts []struct {
				ID    string  `json:"id"`
				Pass  string  `json:"pass"`
				Email *string `json:"email"`
				Group string  `json:"group"`
			} `json:"parsed_default_accounts"`
			Groups map[string]struct {
				V map[string]struct {
					T string `json:"t"`
					V bool   `json:"v"`
				} `json:"v"`
			} `json:"parsed_default_groups"`
		} `json:"facts"`
	}
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	if len(root.Facts.Accounts) == 0 || len(root.Facts.Groups) == 0 {
		t.Fatal("parsed_default_* facts missing from the golden")
	}
	dir := t.TempDir()
	for name, golden := range map[string]string{AccountFile: "account.default.xml", GroupFile: "account_group.default.xml"} {
		b, err := os.ReadFile(filepath.Join(javaAccountGolden, golden))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	m, err := Open(Config{Dir: dir, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	var got, want []string
	for _, a := range m.Accounts() {
		got = append(got, fmt.Sprintf("%s|%s|%s|%s", a.ID, a.Password, a.Email, a.Group))
	}
	for _, a := range root.Facts.Accounts {
		email := "<null>"
		if a.Email != nil {
			email = *a.Email
		}
		want = append(want, fmt.Sprintf("%s|%s|%s|%s", a.ID, a.Pass, email, a.Group))
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("accounts (file order)\n go   %q\n java %q", got, want)
	}
	// Java keeps the policies in a MapValue (Hashtable order); Go keeps file
	// order (decision 5). Compared per group as a set, groups in file order.
	var groupsGot, groupsWant []string
	for _, g := range m.GroupPolicies() {
		var ps []string
		for _, p := range g.Policy {
			ps = append(ps, fmt.Sprintf("%s=%v", p.Name, p.Allow))
		}
		sort.Strings(ps)
		groupsGot = append(groupsGot, g.Name+":"+strings.Join(ps, ","))
	}
	sort.Strings(groupsGot)
	for name, g := range root.Facts.Groups {
		var ps []string
		for p, v := range g.V {
			if v.T != "boolean" {
				t.Fatalf("%s.%s is %s", name, p, v.T)
			}
			ps = append(ps, fmt.Sprintf("%s=%v", p, v.V))
		}
		sort.Strings(ps)
		groupsWant = append(groupsWant, name+":"+strings.Join(ps, ","))
	}
	sort.Strings(groupsWant)
	if strings.Join(groupsGot, "\n") != strings.Join(groupsWant, "\n") {
		t.Fatalf("group policies\n go   %q\n java %q", groupsGot, groupsWant)
	}
}
