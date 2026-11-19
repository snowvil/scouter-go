package account

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/asn1"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"time"
)

// LDAPConfig is the account_ldap_* configuration of AccountManager.authorizeAccount.
type LDAPConfig struct {
	// URL is account_ldap_provider_url (JNDI PROVIDER_URL): one or more
	// space separated ldap://host[:port][/dn] or ldaps:// URLs, tried in
	// order until one connects. A DN in the URL is the context the BaseDN is
	// relative to, as in JNDI.
	URL string
	// Auth is "account_ldap_auth=" (the key really ends with '='). Only
	// "simple" (the default) is supported.
	Auth string
	// PrincipalDomain is appended to the login id for the bind name.
	PrincipalDomain string
	// BaseDN is account_ldap_basedn, the subtree search base.
	BaseDN string
	// EmailAttr and GroupAttr are account_ldap_email_id/account_ldap_group_id.
	EmailAttr, GroupAttr string
	// Debug is account_ldap_debug: logs the id, bind name and URL (never the
	// password, unlike Java).
	Debug bool
	// Timeout bounds dial, bind and search together. Default 10s.
	Timeout time.Duration
	// TLSConfig is cloned for ldaps:// URLs; ServerName defaults to the host.
	TLSConfig *tls.Config
	// MaxMessageBytes bounds one received LDAPMessage. Default 1 MiB.
	MaxMessageBytes int
}

const (
	defaultLDAPTimeout    = 10 * time.Second
	defaultLDAPMaxMessage = 1 << 20
	// maxLDAPSearchMessages bounds references read before the first entry.
	maxLDAPSearchMessages = 1024
	maxBERDepth           = 32

	ldapResultSuccess            = 0
	ldapResultInvalidCredentials = 49

	ldapBindRequest      = 0
	ldapBindResponse     = 1
	ldapUnbindRequest    = 2
	ldapSearchRequest    = 3
	ldapSearchEntry      = 4
	ldapSearchDone       = 5
	ldapSearchReference  = 19
	ldapExtendedResponse = 24
)

// ErrLDAP marks a protocol violation or an LDAP failure other than invalid credentials.
var ErrLDAP = errors.New("account: LDAP")

type ldapTarget struct {
	tls        bool
	host, addr string
	dn         string
}

type ldapAuthenticator struct {
	// input is the configuration as given (before defaults), compared by SetLDAP.
	input   LDAPConfig
	cfg     LDAPConfig
	targets []ldapTarget
	logf    func(string, ...any)
	// configErr is an invalid URL/authentication setting. Java throws while
	// building the JNDI properties and the LOGIN gets no answer; Go answers
	// login fail and reports this error on every attempt.
	configErr error
}

func newLDAPAuthenticator(cfg LDAPConfig, logf func(string, ...any)) (*ldapAuthenticator, error) {
	input := cfg
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultLDAPTimeout
	}
	if cfg.MaxMessageBytes == 0 {
		cfg.MaxMessageBytes = defaultLDAPMaxMessage
	}
	if cfg.Timeout < 0 || cfg.MaxMessageBytes < 0 {
		return nil, fmt.Errorf("%w: negative limit", ErrLDAP)
	}
	a := &ldapAuthenticator{input: input, cfg: cfg, logf: logf}
	if a.configErr = a.parseTargets(); a.configErr != nil {
		a.targets = nil
		logf("account: LDAP configuration: %v (every LDAP login fails)", a.configErr)
	}
	return a, nil
}

func (a *ldapAuthenticator) parseTargets() error {
	cfg := a.cfg
	if cfg.Auth != "" && !strings.EqualFold(cfg.Auth, "simple") {
		return fmt.Errorf("%w: authentication %q unsupported (simple only)", ErrLDAP, cfg.Auth)
	}
	for _, raw := range strings.Fields(cfg.URL) {
		u, err := url.Parse(raw)
		if err != nil {
			return fmt.Errorf("%w: provider URL: %v", ErrLDAP, err)
		}
		t := ldapTarget{host: u.Hostname()}
		port := u.Port()
		switch strings.ToLower(u.Scheme) {
		case "ldap":
			if port == "" {
				port = "389"
			}
		case "ldaps":
			t.tls = true
			if port == "" {
				port = "636"
			}
		default:
			return fmt.Errorf("%w: provider URL scheme %q unsupported", ErrLDAP, u.Scheme)
		}
		if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("%w: provider URL extensions unsupported", ErrLDAP)
		}
		if t.host == "" {
			t.host = "localhost"
		}
		t.addr = net.JoinHostPort(t.host, port)
		t.dn = strings.TrimPrefix(u.Path, "/")
		a.targets = append(a.targets, t)
	}
	if len(a.targets) == 0 {
		return fmt.Errorf("%w: account_ldap_provider_url is empty", ErrLDAP)
	}
	return nil
}

// searchBase joins the BaseDN to the URL's context DN like JNDI's composite name.
func searchBase(base, contextDN string) string {
	switch {
	case contextDN == "":
		return base
	case base == "":
		return contextDN
	default:
		return base + "," + contextDN
	}
}

func (a *ldapAuthenticator) dial(ctx context.Context) (net.Conn, ldapTarget, error) {
	var last error
	for _, t := range a.targets {
		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", t.addr)
		if err == nil && t.tls {
			cfg := &tls.Config{MinVersion: tls.VersionTLS12}
			if a.cfg.TLSConfig != nil {
				cfg = a.cfg.TLSConfig.Clone()
			}
			if cfg.ServerName == "" {
				cfg.ServerName = t.host
			}
			tc := tls.Client(conn, cfg)
			if err = tc.HandshakeContext(ctx); err != nil {
				conn.Close()
			} else {
				conn = tc
			}
		}
		if err == nil {
			return conn, t, nil
		}
		last = err
		if ctx.Err() != nil {
			break
		}
	}
	return nil, ldapTarget{}, fmt.Errorf("%w: connect: %v", ErrLDAP, last)
}

// authorize binds as id+PrincipalDomain with pass, searches (cn=<id>) in the
// subtree and accepts the first entry when one of its attributes has id as
// its first value. Invalid credentials and "no entry" are (false, nil).
func (a *ldapAuthenticator) authorize(ctx context.Context, id, pass string) (Account, bool, error) {
	// RFC 4513 5.1.2: a simple bind with an empty password is an
	// unauthenticated bind that many servers accept; never treat it as a login.
	if pass == "" {
		return Account{}, false, nil
	}
	// An empty id binds as PrincipalDomain alone and searches (cn=); it can
	// never name a user, so it fails without contacting the directory.
	if id == "" {
		return Account{}, false, nil
	}
	if a.configErr != nil {
		return Account{}, false, a.configErr
	}
	ctx, cancel := context.WithTimeout(ctx, a.cfg.Timeout)
	defer cancel()
	conn, target, err := a.dial(ctx)
	if err != nil {
		return Account{}, false, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	}
	bindName := id + a.cfg.PrincipalDomain
	base := searchBase(a.cfg.BaseDN, target.dn)
	if a.cfg.Debug {
		a.logf("account: ldap id=%q bind=%q url=%q base=%q", id, bindName, target.addr, base)
	}
	c := &ldapConn{conn: conn, r: bufio.NewReader(conn), max: a.cfg.MaxMessageBytes}
	account, ok, err := c.authorize(id, bindName, pass, base, a.cfg.EmailAttr, a.cfg.GroupAttr)
	if err != nil {
		// The connection deadline can fire just before ctx's own timer.
		cause := ctx.Err()
		if deadline, ok := ctx.Deadline(); cause == nil && ok && !time.Now().Before(deadline) {
			cause = context.DeadlineExceeded
		}
		if cause != nil {
			err = fmt.Errorf("%w: %v (%v)", ErrLDAP, cause, err)
		}
	}
	return account, ok, err
}

type ldapConn struct {
	conn io.Writer
	r    *bufio.Reader
	max  int
	next int64
}

func berTLV(class, tag int, compound bool, content []byte) []byte {
	b, err := asn1.Marshal(asn1.RawValue{Class: class, Tag: tag, IsCompound: compound, Bytes: content})
	if err != nil {
		panic(err) // only tags/classes fixed in this file are passed
	}
	return b
}

func berMarshal(v any) []byte {
	b, err := asn1.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func (c *ldapConn) send(op []byte) (int64, error) {
	c.next++
	msg := berTLV(asn1.ClassUniversal, asn1.TagSequence, true, concat(berMarshal(c.next), op))
	_, err := c.conn.Write(msg)
	if err != nil {
		return 0, fmt.Errorf("%w: write: %v", ErrLDAP, err)
	}
	return c.next, nil
}

// receive reads one LDAPMessage for id and returns its protocolOp.
func (c *ldapConn) receive(id int64) (asn1.RawValue, error) {
	raw, err := readBER(c.r, c.max)
	if err != nil {
		return asn1.RawValue{}, err
	}
	msg, err := parseBER(raw)
	if err != nil {
		return asn1.RawValue{}, err
	}
	if msg.Class != asn1.ClassUniversal || msg.Tag != asn1.TagSequence || !msg.IsCompound {
		return asn1.RawValue{}, fmt.Errorf("%w: message is not a SEQUENCE", ErrLDAP)
	}
	parts, err := berChildren(msg.Bytes)
	if err != nil {
		return asn1.RawValue{}, err
	}
	if len(parts) < 2 || len(parts) > 3 {
		return asn1.RawValue{}, fmt.Errorf("%w: message has %d elements", ErrLDAP, len(parts))
	}
	got, err := berInt(parts[0], asn1.ClassUniversal, asn1.TagInteger)
	if err != nil {
		return asn1.RawValue{}, err
	}
	op := parts[1]
	if got == 0 && op.Class == asn1.ClassApplication && op.Tag == ldapExtendedResponse {
		return asn1.RawValue{}, fmt.Errorf("%w: server sent a notice of disconnection", ErrLDAP)
	}
	if got != id {
		return asn1.RawValue{}, fmt.Errorf("%w: message id %d, want %d", ErrLDAP, got, id)
	}
	if op.Class != asn1.ClassApplication {
		return asn1.RawValue{}, fmt.Errorf("%w: protocolOp class %d", ErrLDAP, op.Class)
	}
	return op, nil
}

// ldapResult decodes the resultCode and diagnosticMessage of an LDAPResult.
func ldapResult(op asn1.RawValue) (int64, string, error) {
	if !op.IsCompound {
		return 0, "", fmt.Errorf("%w: LDAPResult is primitive", ErrLDAP)
	}
	parts, err := berChildren(op.Bytes)
	if err != nil {
		return 0, "", err
	}
	if len(parts) < 3 {
		return 0, "", fmt.Errorf("%w: LDAPResult has %d elements", ErrLDAP, len(parts))
	}
	code, err := berInt(parts[0], asn1.ClassUniversal, asn1.TagEnum)
	if err != nil {
		return 0, "", err
	}
	if parts[2].Class != asn1.ClassUniversal || parts[2].Tag != asn1.TagOctetString || parts[2].IsCompound {
		return 0, "", fmt.Errorf("%w: diagnosticMessage", ErrLDAP)
	}
	return code, string(parts[2].Bytes), nil
}

func (c *ldapConn) authorize(id, bindName, pass, base, emailAttr, groupAttr string) (Account, bool, error) {
	bind := berTLV(asn1.ClassApplication, ldapBindRequest, true, concat(
		berMarshal(3),
		berMarshal([]byte(bindName)),
		berTLV(asn1.ClassContextSpecific, 0, false, []byte(pass)),
	))
	msgID, err := c.send(bind)
	if err != nil {
		return Account{}, false, err
	}
	op, err := c.receive(msgID)
	if err != nil {
		return Account{}, false, err
	}
	if op.Tag != ldapBindResponse {
		return Account{}, false, fmt.Errorf("%w: bind answered with op %d", ErrLDAP, op.Tag)
	}
	code, diag, err := ldapResult(op)
	if err != nil {
		return Account{}, false, err
	}
	switch code {
	case ldapResultSuccess:
	case ldapResultInvalidCredentials:
		return Account{}, false, nil
	default:
		return Account{}, false, fmt.Errorf("%w: bind result %d %q", ErrLDAP, code, diag)
	}
	defer c.send(berTLV(asn1.ClassApplication, ldapUnbindRequest, false, nil))
	// JNDI SearchControls defaults: subtree scope, derefAliases always, no
	// size/time limit, all user attributes. The filter is equalityMatch on
	// the literal id, the BER form of "(cn=" + RFC 4515 escape(id) + ")";
	// Java concatenates the id unescaped.
	filter := berTLV(asn1.ClassContextSpecific, 3, true, concat(berMarshal([]byte("cn")), berMarshal([]byte(id))))
	search := berTLV(asn1.ClassApplication, ldapSearchRequest, true, concat(
		berMarshal([]byte(base)),
		berMarshal(asn1.Enumerated(2)),
		berMarshal(asn1.Enumerated(3)),
		berMarshal(0),
		berMarshal(0),
		berMarshal(false),
		filter,
		berTLV(asn1.ClassUniversal, asn1.TagSequence, true, nil),
	))
	if msgID, err = c.send(search); err != nil {
		return Account{}, false, err
	}
	for range maxLDAPSearchMessages {
		op, err := c.receive(msgID)
		if err != nil {
			return Account{}, false, err
		}
		switch op.Tag {
		case ldapSearchReference:
			continue // JNDI REFERRAL default "ignore"
		case ldapSearchDone:
			code, diag, err := ldapResult(op)
			if err != nil {
				return Account{}, false, err
			}
			if code != ldapResultSuccess {
				return Account{}, false, fmt.Errorf("%w: search result %d %q", ErrLDAP, code, diag)
			}
			return Account{}, false, nil
		case ldapSearchEntry:
			attrs, err := entryAttributes(op)
			if err != nil {
				return Account{}, false, err
			}
			return accountFromEntry(id, attrs, emailAttr, groupAttr)
		default:
			return Account{}, false, fmt.Errorf("%w: search answered with op %d", ErrLDAP, op.Tag)
		}
	}
	return Account{}, false, fmt.Errorf("%w: more than %d search references", ErrLDAP, maxLDAPSearchMessages)
}

type ldapAttribute struct {
	name   string
	values [][]byte
}

func entryAttributes(op asn1.RawValue) ([]ldapAttribute, error) {
	if !op.IsCompound {
		return nil, fmt.Errorf("%w: entry is primitive", ErrLDAP)
	}
	parts, err := berChildren(op.Bytes)
	if err != nil {
		return nil, err
	}
	if len(parts) != 2 || parts[1].Tag != asn1.TagSequence || !parts[1].IsCompound {
		return nil, fmt.Errorf("%w: malformed SearchResultEntry", ErrLDAP)
	}
	list, err := berChildren(parts[1].Bytes)
	if err != nil {
		return nil, err
	}
	out := make([]ldapAttribute, 0, len(list))
	for _, item := range list {
		fields, err := berChildren(item.Bytes)
		if err != nil {
			return nil, err
		}
		if item.Tag != asn1.TagSequence || !item.IsCompound || len(fields) != 2 || fields[0].Tag != asn1.TagOctetString || fields[1].Tag != asn1.TagSet || !fields[1].IsCompound {
			return nil, fmt.Errorf("%w: malformed PartialAttribute", ErrLDAP)
		}
		values, err := berChildren(fields[1].Bytes)
		if err != nil {
			return nil, err
		}
		a := ldapAttribute{name: string(fields[0].Bytes)}
		for _, v := range values {
			if v.Tag != asn1.TagOctetString || v.IsCompound {
				return nil, fmt.Errorf("%w: malformed attribute value", ErrLDAP)
			}
			a.values = append(a.values, v.Bytes)
		}
		out = append(out, a)
	}
	return out, nil
}

// accountFromEntry is the Java attribute loop: any attribute whose first
// value equals id yields the account. Email and group are read in one try
// block, so a missing email attribute also leaves group empty.
func accountFromEntry(id string, attrs []ldapAttribute, emailAttr, groupAttr string) (Account, bool, error) {
	first := func(name string) (string, bool) {
		for _, a := range attrs { // BasicAttributes(ignoreCase=true)
			if strings.EqualFold(a.name, name) && len(a.values) > 0 {
				return string(a.values[0]), true
			}
		}
		return "", false
	}
	for _, a := range attrs {
		if len(a.values) == 0 || string(a.values[0]) != id {
			continue
		}
		account := Account{ID: id}
		if email, ok := first(emailAttr); ok {
			account.Email = email
			account.Group, _ = first(groupAttr)
		}
		return account, true, nil
	}
	return Account{}, false, nil
}

// readBER reads one definite-length BER element. Long-form lengths need not
// be minimal (Active Directory sends 4-byte lengths); indefinite length and
// elements above max are rejected before allocation.
func readBER(r *bufio.Reader, max int) ([]byte, error) {
	tag, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("%w: read: %v", ErrLDAP, err)
	}
	if tag&0x1f == 0x1f {
		return nil, fmt.Errorf("%w: high tag number at message level", ErrLDAP)
	}
	header := []byte{tag}
	first, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("%w: read: %v", ErrLDAP, err)
	}
	header = append(header, first)
	// Accumulate in int64: four length bytes overflow a 32-bit int.
	length := int64(first)
	if first&0x80 != 0 {
		n := int(first & 0x7f)
		if n == 0 {
			return nil, fmt.Errorf("%w: indefinite length", ErrLDAP)
		}
		if n > 4 {
			return nil, fmt.Errorf("%w: %d-byte length", ErrLDAP, n)
		}
		length = 0
		for range n {
			b, err := r.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("%w: read: %v", ErrLDAP, err)
			}
			header = append(header, b)
			length = length<<8 | int64(b)
		}
	}
	if length < 0 || length > int64(max) {
		return nil, fmt.Errorf("%w: message of %d bytes exceeds %d", ErrLDAP, length, max)
	}
	out := make([]byte, len(header)+int(length))
	copy(out, header)
	if _, err := io.ReadFull(r, out[len(header):]); err != nil {
		return nil, fmt.Errorf("%w: read: %v", ErrLDAP, err)
	}
	return out, nil
}

// parseBER decodes exactly one element; trailing bytes are an error.
func parseBER(b []byte) (asn1.RawValue, error) {
	v, rest, err := berElement(b, 0)
	if err != nil {
		return asn1.RawValue{}, err
	}
	if len(rest) != 0 {
		return asn1.RawValue{}, fmt.Errorf("%w: trailing bytes", ErrLDAP)
	}
	return v, nil
}

func berChildren(b []byte) ([]asn1.RawValue, error) {
	var out []asn1.RawValue
	for len(b) > 0 {
		v, rest, err := berElement(b, 0)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
		b = rest
	}
	return out, nil
}

func berElement(b []byte, depth int) (asn1.RawValue, []byte, error) {
	if depth > maxBERDepth {
		return asn1.RawValue{}, nil, fmt.Errorf("%w: nesting too deep", ErrLDAP)
	}
	if len(b) < 2 {
		return asn1.RawValue{}, nil, fmt.Errorf("%w: truncated element", ErrLDAP)
	}
	v := asn1.RawValue{Class: int(b[0] >> 6), IsCompound: b[0]&0x20 != 0, Tag: int(b[0] & 0x1f)}
	i := 1
	if v.Tag == 0x1f {
		v.Tag = 0
		for {
			if i >= len(b) || i > 4 {
				return asn1.RawValue{}, nil, fmt.Errorf("%w: bad tag", ErrLDAP)
			}
			c := b[i]
			i++
			v.Tag = v.Tag<<7 | int(c&0x7f)
			if c&0x80 == 0 {
				break
			}
		}
	}
	if i >= len(b) {
		return asn1.RawValue{}, nil, fmt.Errorf("%w: truncated length", ErrLDAP)
	}
	first := b[i]
	i++
	length64 := int64(first)
	if first&0x80 != 0 {
		n := int(first & 0x7f)
		if n == 0 {
			return asn1.RawValue{}, nil, fmt.Errorf("%w: indefinite length", ErrLDAP)
		}
		if n > 4 || i+n > len(b) {
			return asn1.RawValue{}, nil, fmt.Errorf("%w: bad length", ErrLDAP)
		}
		length64 = 0
		for _, c := range b[i : i+n] {
			length64 = length64<<8 | int64(c)
		}
		i += n
	}
	if length64 < 0 || length64 > int64(len(b)-i) {
		return asn1.RawValue{}, nil, fmt.Errorf("%w: length %d exceeds element", ErrLDAP, length64)
	}
	length := int(length64)
	v.Bytes = b[i : i+length]
	v.FullBytes = b[:i+length]
	if v.IsCompound {
		// Validate nested framing eagerly so later child walks cannot fail differently.
		rest := v.Bytes
		for len(rest) > 0 {
			var err error
			if _, rest, err = berElement(rest, depth+1); err != nil {
				return asn1.RawValue{}, nil, err
			}
		}
	}
	return v, b[i+length:], nil
}

// berInt decodes a small INTEGER/ENUMERATED (two's complement, up to 8 bytes).
func berInt(v asn1.RawValue, class, tag int) (int64, error) {
	if v.Class != class || v.Tag != tag || v.IsCompound || len(v.Bytes) == 0 || len(v.Bytes) > 8 {
		return 0, fmt.Errorf("%w: expected integer tag %d", ErrLDAP, tag)
	}
	n := int64(int8(v.Bytes[0]))
	for _, c := range v.Bytes[1:] {
		n = n<<8 | int64(c)
	}
	return n, nil
}
