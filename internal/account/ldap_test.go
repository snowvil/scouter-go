package account

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"math"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeDirectory is a loopback LDAP v3 server for one synthetic user.
type fakeDirectory struct {
	t         *testing.T
	bindName  string
	password  string
	bindCode  int64
	entries   [][]ldapAttribute
	reference bool
	// raw, when set, replaces the reply to the request with this op tag.
	raw   map[int][]byte
	stall bool

	mu       sync.Mutex
	searches []searchSeen
	conns    atomic.Int32
}

type searchSeen struct {
	base          string
	scope, deref  int64
	filterTag     int
	cn, assertion string
}

func ldapMessage(id int64, op []byte) []byte {
	return berTLV(asn1.ClassUniversal, asn1.TagSequence, true, concat(berMarshal(id), op))
}

func ldapResultOp(tag int, code int64) []byte {
	return berTLV(asn1.ClassApplication, tag, true, concat(berMarshal(asn1.Enumerated(code)), berMarshal([]byte{}), berMarshal([]byte("synthetic diagnostic"))))
}

func entryOp(dn string, attrs []ldapAttribute) []byte {
	var list []byte
	for _, a := range attrs {
		var vals []byte
		for _, v := range a.values {
			vals = append(vals, berMarshal(v)...)
		}
		list = append(list, berTLV(asn1.ClassUniversal, asn1.TagSequence, true, concat(berMarshal([]byte(a.name)), berTLV(asn1.ClassUniversal, asn1.TagSet, true, vals)))...)
	}
	return berTLV(asn1.ClassApplication, ldapSearchEntry, true, concat(berMarshal([]byte(dn)), berTLV(asn1.ClassUniversal, asn1.TagSequence, true, list)))
}

func (f *fakeDirectory) serve(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		f.conns.Add(1)
		go f.handle(conn)
	}
}

func (f *fakeDirectory) handle(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)
	for {
		raw, err := readBER(r, 1<<20)
		if err != nil {
			return
		}
		msg, err := parseBER(raw)
		if err != nil {
			f.t.Errorf("fake server parse: %v", err)
			return
		}
		parts, _ := berChildren(msg.Bytes)
		id, _ := berInt(parts[0], asn1.ClassUniversal, asn1.TagInteger)
		op := parts[1]
		if f.stall {
			time.Sleep(3 * time.Second)
			return
		}
		if reply, ok := f.raw[op.Tag]; ok {
			conn.Write(reply)
			continue
		}
		switch op.Tag {
		case ldapBindRequest:
			fields, _ := berChildren(op.Bytes)
			version, _ := berInt(fields[0], asn1.ClassUniversal, asn1.TagInteger)
			code := f.bindCode
			if version != 3 || string(fields[1].Bytes) != f.bindName || fields[2].Class != asn1.ClassContextSpecific || fields[2].Tag != 0 || string(fields[2].Bytes) != f.password {
				code = ldapResultInvalidCredentials
			}
			conn.Write(ldapMessage(id, ldapResultOp(ldapBindResponse, code)))
		case ldapSearchRequest:
			fields, _ := berChildren(op.Bytes)
			s := searchSeen{base: string(fields[0].Bytes), filterTag: fields[6].Tag}
			s.scope, _ = berInt(fields[1], asn1.ClassUniversal, asn1.TagEnum)
			s.deref, _ = berInt(fields[2], asn1.ClassUniversal, asn1.TagEnum)
			if ava, err := berChildren(fields[6].Bytes); err == nil && len(ava) == 2 {
				s.cn, s.assertion = string(ava[0].Bytes), string(ava[1].Bytes)
			}
			f.mu.Lock()
			f.searches = append(f.searches, s)
			f.mu.Unlock()
			var out []byte
			if f.reference {
				out = append(out, ldapMessage(id, berTLV(asn1.ClassApplication, ldapSearchReference, true, berMarshal([]byte("ldap://elsewhere/"))))...)
			}
			for _, e := range f.entries {
				out = append(out, ldapMessage(id, entryOp("cn=synthetic", e))...)
			}
			out = append(out, ldapMessage(id, ldapResultOp(ldapSearchDone, 0))...)
			conn.Write(out)
		case ldapUnbindRequest:
			return
		}
	}
}

func startDirectory(t *testing.T, f *fakeDirectory, tlsConfig *tls.Config) string {
	t.Helper()
	f.t = t
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if tlsConfig != nil {
		l = tls.NewListener(l, tlsConfig)
	}
	t.Cleanup(func() { l.Close() })
	go f.serve(l)
	return l.Addr().String()
}

func syntheticEntry() []ldapAttribute {
	return []ldapAttribute{
		{name: "objectClass", values: [][]byte{[]byte("person")}},
		{name: "cn", values: [][]byte{[]byte("synthetic"), []byte("alias")}},
		{name: "MAIL", values: [][]byte{[]byte("s@example.invalid")}},
		{name: "memberOf", values: [][]byte{[]byte("ops")}},
	}
}

func newTestLDAP(t *testing.T, cfg LDAPConfig) (*ldapAuthenticator, *logSink) {
	t.Helper()
	sink := &logSink{}
	if cfg.Timeout == 0 {
		cfg.Timeout = 2 * time.Second
	}
	a, err := newLDAPAuthenticator(cfg, sink.logf)
	if err != nil {
		t.Fatal(err)
	}
	return a, sink
}

func TestLDAPBindSearchAndAccountRules(t *testing.T) {
	f := &fakeDirectory{bindName: "synthetic@example.invalid", password: "synthetic-secret", entries: [][]ldapAttribute{syntheticEntry()}, reference: true}
	addr := startDirectory(t, f, nil)
	a, sink := newTestLDAP(t, LDAPConfig{URL: "ldap://" + addr + "/dc=example,dc=invalid", PrincipalDomain: "@example.invalid", BaseDN: "ou=people", EmailAttr: "mail", GroupAttr: "memberof", Debug: true})
	ctx := context.Background()
	got, ok, err := a.authorize(ctx, "synthetic", "synthetic-secret")
	if err != nil || !ok || got != (Account{ID: "synthetic", Email: "s@example.invalid", Group: "ops"}) {
		t.Fatal(got, ok, err)
	}
	f.mu.Lock()
	s := f.searches[0]
	f.mu.Unlock()
	if s != (searchSeen{base: "ou=people,dc=example,dc=invalid", scope: 2, deref: 3, filterTag: 3, cn: "cn", assertion: "synthetic"}) {
		t.Fatalf("%+v", s)
	}
	if sink.count("synthetic-secret") != 0 || sink.count(`bind="synthetic@example.invalid"`) != 1 {
		t.Fatal("debug log", sink.lines)
	}
	// Invalid credentials are a plain failure.
	if _, ok, err := a.authorize(ctx, "synthetic", "wrong"); ok || err != nil {
		t.Fatal(ok, err)
	}
	// The empty password never reaches the directory (unauthenticated bind).
	before := f.conns.Load()
	if _, ok, err := a.authorize(ctx, "synthetic", ""); ok || err != nil || f.conns.Load() != before {
		t.Fatal("empty password", ok, err)
	}
	// Filter metacharacters stay literal in the equalityMatch.
	g := &fakeDirectory{bindName: "*)(cn=*@example.invalid", password: "synthetic-secret", entries: [][]ldapAttribute{syntheticEntry()}}
	a, _ = newTestLDAP(t, LDAPConfig{URL: "ldap://" + startDirectory(t, g, nil), PrincipalDomain: "@example.invalid"})
	if _, ok, err := a.authorize(ctx, "*)(cn=*", "synthetic-secret"); ok || err != nil {
		t.Fatal(ok, err)
	}
	g.mu.Lock()
	last := g.searches[0]
	g.mu.Unlock()
	if last.filterTag != 3 || last.assertion != "*)(cn=*" {
		t.Fatalf("%+v", last)
	}
}

func TestLDAPEntryOutcomes(t *testing.T) {
	cases := []struct {
		name    string
		entries [][]ldapAttribute
		want    Account
		ok      bool
	}{
		{"no entry", nil, Account{}, false},
		{"no attribute equals id", [][]ldapAttribute{{{name: "cn", values: [][]byte{[]byte("alias"), []byte("synthetic")}}}}, Account{}, false},
		{"missing email leaves group empty", [][]ldapAttribute{{{name: "uid", values: [][]byte{[]byte("synthetic")}}, {name: "memberOf", values: [][]byte{[]byte("ops")}}}}, Account{ID: "synthetic"}, true},
		{"missing group", [][]ldapAttribute{{{name: "uid", values: [][]byte{[]byte("synthetic")}}, {name: "mail", values: [][]byte{[]byte("m")}}}}, Account{ID: "synthetic", Email: "m"}, true},
		{"first entry only", [][]ldapAttribute{{{name: "cn", values: [][]byte{[]byte("other")}}}, syntheticEntry()}, Account{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeDirectory{bindName: "synthetic", password: "pw", entries: tc.entries}
			a, _ := newTestLDAP(t, LDAPConfig{URL: "ldap://" + startDirectory(t, f, nil), EmailAttr: "mail", GroupAttr: "memberOf"})
			got, ok, err := a.authorize(context.Background(), "synthetic", "pw")
			if err != nil || ok != tc.ok || got != tc.want {
				t.Fatal(got, ok, err)
			}
		})
	}
}

func TestLDAPProtocolErrors(t *testing.T) {
	long := func(n int) []byte { return []byte{0x30, 0x84, byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)} }
	cases := []struct {
		name string
		f    *fakeDirectory
		cfg  LDAPConfig
		want string
	}{
		{"indefinite length", &fakeDirectory{raw: map[int][]byte{ldapBindRequest: {0x30, 0x80, 0, 0}}}, LDAPConfig{}, "indefinite"},
		{"oversized message", &fakeDirectory{raw: map[int][]byte{ldapBindRequest: long(2 << 20)}}, LDAPConfig{}, "exceeds"},
		{"configured size limit", &fakeDirectory{raw: map[int][]byte{ldapBindRequest: long(200)}}, LDAPConfig{MaxMessageBytes: 100}, "exceeds"},
		{"truncated element", &fakeDirectory{raw: map[int][]byte{ldapBindRequest: {0x30, 0x03, 0x02, 0x05, 0x01}}}, LDAPConfig{}, "exceeds element"},
		{"wrong message id", &fakeDirectory{raw: map[int][]byte{ldapBindRequest: ldapMessage(7, ldapResultOp(ldapBindResponse, 0))}}, LDAPConfig{}, "message id 7"},
		{"notice of disconnection", &fakeDirectory{raw: map[int][]byte{ldapBindRequest: ldapMessage(0, berTLV(asn1.ClassApplication, ldapExtendedResponse, true, nil))}}, LDAPConfig{}, "disconnection"},
		{"bind other error", &fakeDirectory{bindName: "synthetic", password: "pw", bindCode: 53}, LDAPConfig{}, "bind result 53"},
		{"timeout", &fakeDirectory{stall: true}, LDAPConfig{Timeout: 200 * time.Millisecond}, "deadline"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.cfg.URL = "ldap://" + startDirectory(t, tc.f, nil)
			a, _ := newTestLDAP(t, tc.cfg)
			start := time.Now()
			_, ok, err := a.authorize(context.Background(), "synthetic", "pw")
			if ok || !errors.Is(err, ErrLDAP) || !strings.Contains(err.Error(), tc.want) {
				t.Fatal(ok, err)
			}
			if time.Since(start) > 2*time.Second {
				t.Fatal("not bounded by the deadline")
			}
		})
	}
	// Non-minimal long-form lengths (Active Directory style) are accepted.
	ok := ldapResultOp(ldapBindResponse, 0)
	inner := concat(berMarshal(int64(1)), ok)
	msg := append([]byte{0x30, 0x84, 0, 0, 0, byte(len(inner))}, inner...)
	f := &fakeDirectory{raw: map[int][]byte{ldapBindRequest: msg}, entries: [][]ldapAttribute{syntheticEntry()}}
	a, _ := newTestLDAP(t, LDAPConfig{URL: "ldap://" + startDirectory(t, f, nil)})
	if got, okay, err := a.authorize(context.Background(), "synthetic", "pw"); !okay || err != nil || got.ID != "synthetic" {
		t.Fatal(got, okay, err)
	}
	// A cancelled caller context stops the exchange.
	stall := &fakeDirectory{stall: true}
	a, _ = newTestLDAP(t, LDAPConfig{URL: "ldap://" + startDirectory(t, stall, nil), Timeout: 5 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, ok, err := a.authorize(ctx, "synthetic", "pw"); ok || !errors.Is(err, ErrLDAP) {
		t.Fatal(ok, err)
	}
}

func TestLDAPConfigAndFailover(t *testing.T) {
	// Invalid settings do not stop the server: every LDAP login fails with the error.
	for _, cfg := range []LDAPConfig{{URL: ""}, {URL: "http://x"}, {URL: "ldap://x", Auth: "DIGEST-MD5"}, {URL: "ldap://u@x"}, {URL: "ldap://%zz"}} {
		sink := &logSink{}
		a, err := newLDAPAuthenticator(cfg, sink.logf)
		if err != nil || sink.count("every LDAP login fails") != 1 {
			t.Fatalf("%+v: %v", cfg, err)
		}
		if _, ok, err := a.authorize(context.Background(), "synthetic", "pw"); ok || !errors.Is(err, ErrLDAP) {
			t.Fatalf("%+v: %v %v", cfg, ok, err)
		}
	}
	if _, err := newLDAPAuthenticator(LDAPConfig{URL: "ldap://x", Timeout: -1}, func(string, ...any) {}); !errors.Is(err, ErrLDAP) {
		t.Fatal(err)
	}
	// The first URL refuses connections; the second answers.
	dead, _ := net.Listen("tcp", "127.0.0.1:0")
	deadAddr := dead.Addr().String()
	dead.Close()
	f := &fakeDirectory{bindName: "synthetic", password: "pw", entries: [][]ldapAttribute{syntheticEntry()}}
	a, _ := newTestLDAP(t, LDAPConfig{URL: "ldap://" + deadAddr + " LDAP://" + startDirectory(t, f, nil), Auth: "Simple"})
	if _, ok, err := a.authorize(context.Background(), "synthetic", "pw"); !ok || err != nil {
		t.Fatal(ok, err)
	}
	a, _ = newTestLDAP(t, LDAPConfig{URL: "ldap://" + deadAddr})
	if _, ok, err := a.authorize(context.Background(), "synthetic", "pw"); ok || !errors.Is(err, ErrLDAP) {
		t.Fatal(ok, err)
	}
	if searchBase("", "dc=x") != "dc=x" || searchBase("ou=p", "") != "ou=p" {
		t.Fatal("search base")
	}
}

func TestLDAPS(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "synthetic"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	f := &fakeDirectory{bindName: "synthetic", password: "pw", entries: [][]ldapAttribute{syntheticEntry()}}
	addr := startDirectory(t, f, &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}})
	a, _ := newTestLDAP(t, LDAPConfig{URL: "ldaps://" + addr, TLSConfig: &tls.Config{RootCAs: pool}})
	if got, ok, err := a.authorize(context.Background(), "synthetic", "pw"); !ok || err != nil || got.ID != "synthetic" {
		t.Fatal(got, ok, err)
	}
	// An untrusted certificate fails the handshake.
	a, _ = newTestLDAP(t, LDAPConfig{URL: "ldaps://" + addr})
	if _, ok, err := a.authorize(context.Background(), "synthetic", "pw"); ok || !errors.Is(err, ErrLDAP) {
		t.Fatal(ok, err)
	}
}

func TestManagerLDAPMode(t *testing.T) {
	f := &fakeDirectory{bindName: "synthetic", password: "pw", entries: [][]ldapAttribute{syntheticEntry()}}
	addr := startDirectory(t, f, nil)
	sink := &logSink{}
	m, err := Open(Config{Dir: t.TempDir(), Logf: sink.logf, LDAP: &LDAPConfig{URL: "ldap://" + addr, EmailAttr: "mail", GroupAttr: "memberOf", Timeout: 2 * time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, err := m.Authorize(context.Background(), "synthetic", "pw"); !ok || err != nil || got.Group != "ops" {
		t.Fatal(got, ok, err)
	}
	// The local file is not consulted for the password in LDAP mode.
	admin, _ := m.Account("admin")
	if _, ok, _ := m.Authorize(context.Background(), "admin", admin.Password); ok {
		t.Fatal("local password accepted in LDAP mode")
	}
	bad, err := Open(Config{Dir: t.TempDir(), Logf: func(string, ...any) {}, LDAP: &LDAPConfig{URL: "bogus://x"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := bad.Authorize(context.Background(), "synthetic", "pw"); ok || !errors.Is(err, ErrLDAP) {
		t.Fatal(ok, err)
	}
}

func FuzzBER(f *testing.F) {
	f.Add(ldapMessage(1, ldapResultOp(ldapBindResponse, 0)))
	f.Add(ldapMessage(2, entryOp("cn=x", syntheticEntry())))
	f.Add([]byte{0x30, 0x80, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := parseBER(b)
		if err != nil {
			return
		}
		if len(v.FullBytes) != len(b) {
			t.Fatal("length")
		}
		if v.IsCompound {
			_, _ = berChildren(v.Bytes)
		}
		_, _ = entryAttributes(v)
		_, _, _ = ldapResult(v)
	})
}

// Four length bytes of 0xff exceed a 32-bit int; the length is accumulated
// in int64 and rejected instead of wrapping negative.
func TestBERHugeLengthRejected(t *testing.T) {
	huge := []byte{0x30, 0x84, 0xff, 0xff, 0xff, 0xff}
	if _, err := readBER(bufio.NewReader(bytes.NewReader(huge)), 1<<20); !errors.Is(err, ErrLDAP) || !strings.Contains(err.Error(), "exceeds") {
		t.Fatal(err)
	}
	if _, err := readBER(bufio.NewReader(bytes.NewReader(huge)), math.MaxInt32); !errors.Is(err, ErrLDAP) || !strings.Contains(err.Error(), "exceeds") {
		t.Fatal(err)
	}
	nested := []byte{0x30, 0x06, 0x04, 0x84, 0xff, 0xff, 0xff, 0xff}
	if _, err := parseBER(nested); !errors.Is(err, ErrLDAP) || !strings.Contains(err.Error(), "exceeds element") {
		t.Fatal(err)
	}
}

func TestLDAPEmptyIDDoesNotConnect(t *testing.T) {
	f := &fakeDirectory{bindName: "@example.invalid", password: "pw", entries: [][]ldapAttribute{syntheticEntry()}}
	a, _ := newTestLDAP(t, LDAPConfig{URL: "ldap://" + startDirectory(t, f, nil), PrincipalDomain: "@example.invalid"})
	if _, ok, err := a.authorize(context.Background(), "", "pw"); ok || err != nil || f.conns.Load() != 0 {
		t.Fatal(ok, err, f.conns.Load())
	}
}
