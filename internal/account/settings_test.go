package account

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestSetLDAPAppliesToTheNextLogin follows AccountManager.authorizeAccount,
// which reads account_use_ldap and account_ldap_* on every call.
func TestSetLDAPAppliesToTheNextLogin(t *testing.T) {
	one := &fakeDirectory{bindName: "synthetic", password: "pw", entries: [][]ldapAttribute{syntheticEntry()}}
	two := &fakeDirectory{bindName: "synthetic@two.invalid", password: "pw", entries: [][]ldapAttribute{syntheticEntry()}}
	addrOne, addrTwo := startDirectory(t, one, nil), startDirectory(t, two, nil)
	sink := &logSink{}
	m, err := Open(Config{Dir: t.TempDir(), Logf: sink.logf})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	admin, _ := m.Account("admin")
	if _, ok, err := m.Authorize(ctx, "admin", admin.Password); !ok || err != nil {
		t.Fatal("file authentication", ok, err)
	}

	// account_use_ldap=true: the directory decides, the file password no longer works.
	cfg := &LDAPConfig{URL: "ldap://" + addrOne, EmailAttr: "mail", GroupAttr: "memberOf", Timeout: 2 * time.Second}
	if err := m.SetLDAP(cfg); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := m.Authorize(ctx, "synthetic", "pw"); !ok || err != nil || got.Group != "ops" {
		t.Fatal(got, ok, err)
	}
	if _, ok, _ := m.Authorize(ctx, "admin", admin.Password); ok {
		t.Fatal("file password accepted in LDAP mode")
	}

	// Another URL and principal domain are used by the next login.
	cfg2 := *cfg
	cfg2.URL, cfg2.PrincipalDomain, cfg2.GroupAttr = "ldap://"+addrTwo, "@two.invalid", "mail"
	if err := m.SetLDAP(&cfg2); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := m.Authorize(ctx, "synthetic", "pw"); !ok || err != nil || got.Group != "s@example.invalid" {
		t.Fatal(got, ok, err)
	}
	if two.conns.Load() == 0 {
		t.Fatal("second directory not used")
	}

	// account_use_ldap=false again: file authentication.
	if err := m.SetLDAP(nil); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := m.Authorize(ctx, "admin", admin.Password); !ok || err != nil {
		t.Fatal("file authentication after LDAP off", ok, err)
	}
	if _, ok, _ := m.Authorize(ctx, "synthetic", "pw"); ok {
		t.Fatal("LDAP still used")
	}
}

func TestSetLDAPConfigurationErrors(t *testing.T) {
	sink := &logSink{}
	m, err := Open(Config{Dir: t.TempDir(), Logf: sink.logf})
	if err != nil {
		t.Fatal(err)
	}
	bad := &LDAPConfig{URL: "bogus://x"}
	for i := 0; i < 3; i++ { // a reload repeating the same values logs once
		if err := m.SetLDAP(bad); err != nil {
			t.Fatal(err)
		}
	}
	if n := sink.count("LDAP configuration"); n != 1 {
		t.Fatal(n, sink.lines)
	}
	if _, ok, err := m.Authorize(context.Background(), "synthetic", "pw"); ok || !errors.Is(err, ErrLDAP) {
		t.Fatal(ok, err)
	}
	// A negative limit is a program error; the previous mode stays.
	if err := m.SetLDAP(&LDAPConfig{URL: "ldap://127.0.0.1", Timeout: -1}); !errors.Is(err, ErrLDAP) {
		t.Fatal(err)
	}
	if _, _, err := m.Authorize(context.Background(), "synthetic", "pw"); !errors.Is(err, ErrLDAP) {
		t.Fatal("previous LDAP mode replaced", err)
	}
}

// TestSetLDAPConcurrentWithAuthorize runs under -race.
func TestSetLDAPConcurrentWithAuthorize(t *testing.T) {
	m, err := Open(Config{Dir: t.TempDir(), Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	admin, _ := m.Account("admin")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if i%2 == 0 {
				_ = m.SetLDAP(&LDAPConfig{URL: "bogus://x", BaseDN: string(rune('a' + i%26))})
			} else {
				_ = m.SetLDAP(nil)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, _, _ = m.Authorize(context.Background(), "admin", admin.Password)
		}
	}()
	wg.Wait()
}
