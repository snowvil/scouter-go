// Package account owns conf/account.xml and conf/account_group.xml like the
// Scouter AccountManager (pinned source dee47fb5) and authenticates Client
// logins against them or, when configured, an LDAP v3 directory.
package account

import (
	"errors"
	"fmt"
)

// Account mirrors scouter.lang.Account. Password is the stored text that the
// Client's "pass" field is compared with (normally the salted SHA-256 hex).
type Account struct {
	ID, Password, Email, Group string
}

// Policy is one <AllowX>true|false</AllowX> entry of a group, in file order.
type Policy struct {
	Name  string
	Allow bool
}

// Group is one <Group name> with its parsed policies in file order.
type Group struct {
	Name   string
	Policy []Policy
}

// PolicyText is one EDIT_GROUP_POLICY item: the policy element name and the
// text written into it (CastUtil.cString of the received Value).
type PolicyText struct {
	Name, Text string
}

// GroupPolicyEdit is one group key of an EDIT_GROUP_POLICY MapPack.
type GroupPolicyEdit struct {
	Group  string
	Policy []PolicyText
}

// MaxFieldBytes is the largest field Account.toBytes can length-prefix with
// its single unsigned byte.
const MaxFieldBytes = 255

// ErrInvalid marks a request that cannot be stored as the Java server would
// read it back (oversized Account.toBytes field, invalid XML name or text).
var ErrInvalid = errors.New("account: invalid value")

// Bytes is Account.toBytes: four (1-byte length, UTF-8 bytes) fields. Java
// writes only the low byte of the length; longer fields are rejected instead
// of emitting a wrapped, unparseable record.
func (a Account) Bytes() ([]byte, error) {
	if err := a.checkFieldSizes(); err != nil {
		return nil, err
	}
	out := make([]byte, 0, 4+len(a.ID)+len(a.Password)+len(a.Email)+len(a.Group))
	for _, field := range [...]string{a.ID, a.Password, a.Email, a.Group} {
		out = append(out, byte(len(field)))
		out = append(out, field...)
	}
	return out, nil
}

func (a Account) checkFieldSizes() error {
	for _, field := range [...]struct{ name, value string }{{"id", a.ID}, {"pass", a.Password}, {"email", a.Email}, {"group", a.Group}} {
		if len(field.value) > MaxFieldBytes {
			return fmt.Errorf("%w: %s is %d bytes, Account.toBytes allows %d", ErrInvalid, field.name, len(field.value), MaxFieldBytes)
		}
	}
	return nil
}
