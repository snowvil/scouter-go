package service

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/snowvil/scouter-go/internal/account"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// AccountStore is the account.Manager surface the AccountService/LoginService
// handlers use. Errors returned by the mutations are already logged by the
// store; the handlers answer false like the Java catch blocks.
type AccountStore interface {
	Authorize(ctx context.Context, id, pass string) (account.Account, bool, error)
	Account(id string) (account.Account, bool)
	Accounts() []account.Account
	AvailableID(id string) bool
	Add(account.Account) (bool, error)
	Edit(account.Account) (bool, error)
	GroupPolicy(name string) ([]account.Policy, bool)
	GroupPolicies() []account.Group
	AddGroup(name string, policy []account.Policy) (bool, error)
	EditGroupPolicy([]account.GroupPolicyEdit) (bool, error)
}

// localID is NetConstants.LOCAL_ID: a password-less login accepted only from
// a loopback peer (LoginManager.login).
const localID = "__local"

var accountMutations = []string{"ADD_ACCOUNT", "EDIT_ACCOUNT", "ADD_ACCOUNT_GROUP", "EDIT_GROUP_POLICY"}

// AccountCommands lists the AccountService commands and CHECK_LOGIN, served
// from Config.Accounts. The READ commands among them are otherwise answered
// by the single-credential metadata handler; the four mutations are only
// registered (SupportedCommands) when an AccountStore is configured.
func AccountCommands() []string {
	return append([]string{"CHECK_ACCOUNT_ID", "CHECK_LOGIN", "LIST_ACCOUNT", "LIST_ACCOUNT_GROUP", "GET_GROUP_POLICY_ALL"}, accountMutations...)
}

// HandleAccountCommand recognizes its commands before reading the body and leaves
// the stream intact for unknown commands.
func (s *Service) HandleAccountCommand(ctx context.Context, r transport.ClientRequest, stream *transport.Stream) (bool, error) {
	recognized := false
	for _, command := range AccountCommands() {
		if command == r.Command {
			recognized = true
			break
		}
	}
	if !recognized {
		return false, nil
	}
	store := s.config.Accounts
	if store == nil {
		for _, command := range accountMutations {
			if command == r.Command {
				return true, fmt.Errorf("%w: %s requires an account store", transport.ErrUnsupported, r.Command)
			}
		}
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if !r.Authenticated {
		return true, transport.ErrUnauthorized
	}
	return true, s.accountCommand(ctx, store, r.Command, stream)
}

func (s *Service) accountCommand(ctx context.Context, store AccountStore, command string, stream *transport.Stream) error {
	switch command {
	// No request body: the Java handlers never read one.
	case "LIST_ACCOUNT":
		accounts := store.Accounts()
		blobs := make([][]byte, 0, len(accounts))
		for _, a := range accounts {
			b, err := a.Bytes()
			if err != nil {
				return fmt.Errorf("%w: %v", transport.ErrLimit, err)
			}
			blobs = append(blobs, b)
		}
		for _, b := range blobs {
			if err := stream.ResponseValue(protocol.Value{Type: protocol.ValueBlob, Bytes: b}); err != nil {
				return err
			}
		}
		return nil
	case "LIST_ACCOUNT_GROUP":
		groups := store.GroupPolicies()
		names := make([]protocol.Value, 0, len(groups))
		for _, g := range groups {
			names = append(names, stringValue(g.Name))
		}
		return stream.ResponsePack(newMap(entry("group_list", list(names))))
	case "GET_GROUP_POLICY_ALL":
		result := newMap()
		for _, g := range store.GroupPolicies() {
			result.Entries = append(result.Entries, entry(g.Name, policyValue(g.Policy)))
		}
		return stream.ResponsePack(result)
	}
	p, err := readMap(stream)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	switch command {
	case "CHECK_ACCOUNT_ID":
		id, err := getText(p, "id")
		if err != nil {
			return err
		}
		return stream.ResponsePack(newMap(entry("result", boolean(store.AvailableID(id)))))
	case "CHECK_LOGIN":
		id, err := getText(p, "id")
		if err != nil {
			return err
		}
		pass, err := getText(p, "pass")
		if err != nil {
			return err
		}
		// authorizeAccount directly: no __local shortcut, failures are false.
		_, ok, err := store.Authorize(ctx, id, pass)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return stream.ResponseValue(boolean(ok && err == nil))
	case "ADD_ACCOUNT", "EDIT_ACCOUNT":
		a := accountParam(p)
		var ok bool
		if command == "ADD_ACCOUNT" {
			ok, _ = store.Add(a)
		} else {
			ok, _ = store.Edit(a)
		}
		return stream.ResponsePack(newMap(entry("result", boolean(ok))))
	case "ADD_ACCOUNT_GROUP":
		name, policy := lookup(p, "name"), lookup(p, "policy")
		ok := false
		switch {
		case name.Type != protocol.ValueText || policy.Type == protocol.ValueNull:
			// getText/get null -> false before any file access.
		case policy.Type != protocol.ValueMap:
			// asInstanceOf[MapValue] throws outside the handler's try.
			return badParam("policy")
		default:
			// Received (wire) order: Java writes the Hashtable enumeration
			// order; Go keeps the received order instead (intended difference).
			var policies []account.Policy
			for _, e := range policy.Entries {
				// MapValue.getBoolean: anything but a BooleanValue is false.
				policies = append(policies, account.Policy{Name: e.Key, Allow: e.Value.Type == protocol.ValueBoolean && e.Value.Bool})
			}
			ok, _ = store.AddGroup(name.Text, policies)
		}
		return stream.ResponseValue(boolean(ok))
	case "EDIT_GROUP_POLICY":
		edits, ok := s.policyEdits(store, p)
		if ok {
			ok, _ = store.EditGroupPolicy(edits)
		}
		return stream.ResponseValue(boolean(ok))
	}
	return fmt.Errorf("%w: command %s", transport.ErrUnsupported, command)
}

// accountParam reads ADD/EDIT_ACCOUNT with MapPack.getText semantics: a
// missing or non-Text field is Java null, normalized to "" (management
// §9-12). A null id makes Java's StringKeyLinkedMap.put ignore the account
// while answering true; Go stores the "" id instead of discarding it.
func accountParam(p *protocol.MapPack) account.Account {
	text := func(key string) string {
		if v := lookup(p, key); v.Type == protocol.ValueText {
			return v.Text
		}
		return ""
	}
	return account.Account{ID: text("id"), Password: text("pass"), Email: text("email"), Group: text("group")}
}

// policyEdits builds EDIT_GROUP_POLICY in MapPack key order with each
// group's policies in received order (Java: Hashtable order). A known group
// whose value is not a MapValue (ClassCastException) or a value Go cannot
// render like CastUtil.cString answers false without writing.
func (s *Service) policyEdits(store AccountStore, p *protocol.MapPack) ([]account.GroupPolicyEdit, bool) {
	edits := make([]account.GroupPolicyEdit, 0, len(p.Entries))
	for _, e := range p.Entries {
		_, known := store.GroupPolicy(e.Key)
		if e.Value.Type != protocol.ValueMap {
			if known {
				return nil, false
			}
			continue
		}
		edit := account.GroupPolicyEdit{Group: e.Key}
		for _, item := range e.Value.Entries {
			text, ok := castString(item.Value)
			if !ok {
				if known {
					return nil, false
				}
				continue
			}
			edit.Policy = append(edit.Policy, account.PolicyText{Name: item.Key, Text: text})
		}
		edits = append(edits, edit)
	}
	return edits, true
}

// castString is CastUtil.cString for the Value types whose Java toString Go
// reproduces exactly; NullValue.toString is null, which setTextContent
// writes as an empty element.
func castString(v protocol.Value) (string, bool) {
	switch v.Type {
	case protocol.ValueBoolean:
		return strconv.FormatBool(v.Bool), true
	case protocol.ValueText:
		return v.Text, true
	case protocol.ValueDecimal:
		return strconv.FormatInt(v.Int, 10), true
	case protocol.ValueNull:
		return "", true
	}
	return "", false
}

// policyValue renders a group's policies as a MapValue in file order. Java
// writes its Hashtable enumeration order; Go uses file order on purpose
// (MapValue is a map for the Client, the order carries no meaning).
func policyValue(policy []account.Policy) protocol.Value {
	entries := make([]protocol.Entry, 0, len(policy))
	for _, p := range policy {
		entries = append(entries, entry(p.Name, boolean(p.Allow)))
	}
	return protocol.Value{Type: protocol.ValueMap, Entries: entries}
}

type accountLogin struct {
	ok    bool
	group string
}

// accountLogin is LoginManager.login's account step.
func (s *Service) accountLogin(ctx context.Context, r transport.ClientRequest, id, pass string) (accountLogin, error) {
	if id == localID {
		// Java compares the remote IP text with localhost,127.0.0.1,::1 forms.
		return accountLogin{ok: loopbackPeer(r.Peer)}, nil
	}
	a, ok, err := s.config.Accounts.Authorize(ctx, id, pass)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return accountLogin{}, ctxErr
	}
	// Directory/protocol errors are a failed login (Java catches them); the
	// store has logged the cause.
	return accountLogin{ok: ok && err == nil, group: a.Group}, nil
}

func loopbackPeer(addr net.Addr) bool {
	var ip net.IP
	switch a := addr.(type) {
	case *net.TCPAddr:
		ip = a.IP
	default:
		return false
	}
	return ip.Equal(net.IPv4(127, 0, 0, 1)) || ip.Equal(net.IPv6loopback)
}

// accountLoginEntries follows LoginService.login after time/server_id:
// type is the group, email comes from the local account file (also in LDAP
// mode) and is omitted without an entry, policy (file order) is omitted for
// an unknown group.
func (s *Service) accountLoginEntries(id, group string) []protocol.Entry {
	entries := []protocol.Entry{entry("type", stringValue(group)), entry("version", stringValue(serverVersion)), entry("client_version", stringValue(clientVersion))}
	if a, ok := s.config.Accounts.Account(id); ok {
		entries = append(entries, entry("email", stringValue(a.Email)))
	}
	entries = append(entries, entry("timezone", stringValue(s.config.Location.String())))
	if policy, ok := s.config.Accounts.GroupPolicy(group); ok {
		entries = append(entries, entry("policy", policyValue(policy)))
	}
	return entries
}
