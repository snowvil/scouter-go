package account

import (
	"context"
	"crypto/subtle"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// AccountFile and GroupFile are AccountManager.ACCOUNT_FILENAME/GROUP_FILENAME.
	AccountFile = "account.xml"
	GroupFile   = "account_group.xml"

	// DefaultReloadInterval is the AccountManager daemon period.
	DefaultReloadInterval = 5 * time.Second
	// DefaultMaxFileBytes bounds one XML file read.
	DefaultMaxFileBytes = 4 << 20
)

// The classpath resources scouter/server/account/*.xml, byte for byte.
var (
	//go:embed defaults/account.xml
	defaultAccountXML []byte
	//go:embed defaults/account_group.xml
	defaultGroupXML []byte
)

// Config locates the account files. Dir is Configure.CONF_DIR (Java "./conf/").
type Config struct {
	Dir string
	// LDAP, when non-nil, is account_use_ldap=true: Authorize binds to the
	// directory; the files still back LIST/ADD/EDIT and the LOGIN email/policy.
	// It is the initial value; SetLDAP replaces it after a reload.
	LDAP *LDAPConfig
	// Logf receives load/parse/write failures (Java printStackTrace). Default log.Printf.
	Logf           func(format string, args ...any)
	ReloadInterval time.Duration
	MaxFileBytes   int64
}

type fileStamp struct {
	mod  time.Time
	size int64
}

type accountState struct {
	list  []Account
	index map[string]int
}

type groupState struct {
	list  []Group
	index map[string]int
}

// Manager is the AccountManager object. Reads use immutable snapshots; every
// load and write holds mu, so file I/O and the stamps are serialized.
type Manager struct {
	cfg                      Config
	accountPath, groupPath   string
	mu                       sync.Mutex
	accountStamp, groupStamp fileStamp
	accountBad, groupBad     fileStamp // last stamp whose parse failed (logged once)
	accounts                 atomic.Pointer[accountState]
	groups                   atomic.Pointer[groupState]
	// ldap is the live LDAP mode; nil is file authentication.
	ldap   atomic.Pointer[ldapAuthenticator]
	ldapMu sync.Mutex // serializes SetLDAP
}

// Open creates the directory and any missing file from the embedded default
// (AccountManager's classpath copy), then loads both files. Unlike Java, a
// file that cannot be loaded at start is an error instead of an empty map.
func Open(cfg Config) (*Manager, error) {
	if cfg.Dir == "" {
		return nil, errors.New("account: empty configuration directory")
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	if cfg.ReloadInterval == 0 {
		cfg.ReloadInterval = DefaultReloadInterval
	}
	if cfg.MaxFileBytes == 0 {
		cfg.MaxFileBytes = DefaultMaxFileBytes
	}
	if cfg.ReloadInterval < 0 || cfg.MaxFileBytes < 0 {
		return nil, errors.New("account: negative limit")
	}
	m := &Manager{cfg: cfg, accountPath: filepath.Join(cfg.Dir, AccountFile), groupPath: filepath.Join(cfg.Dir, GroupFile)}
	if err := m.SetLDAP(cfg.LDAP); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("account: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Java loads the group file first.
	if err := m.loadGroupsLocked(true); err != nil {
		return nil, err
	}
	if err := m.loadAccountsLocked(true); err != nil {
		return nil, err
	}
	return m, nil
}

// Run reloads a file whose modification time or size changed, every
// ReloadInterval, until ctx ends. Load errors are logged and the previous
// snapshot stays, like the Java daemon.
func (m *Manager) Run(ctx context.Context) error {
	t := time.NewTicker(m.cfg.ReloadInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			m.Reload()
		}
	}
}

// Reload performs one daemon pass synchronously.
func (m *Manager) Reload() {
	m.mu.Lock()
	defer m.mu.Unlock()
	_ = m.loadGroupsLocked(false)
	_ = m.loadAccountsLocked(false)
}

// ensureFile is the canRead()==false branch: a missing file is created from
// the default resource. An existing but unreadable file is not overwritten
// (Java's FileOutputStream would replace it).
func (m *Manager) ensureFile(path string, data []byte) error {
	_, err := os.Stat(path)
	if err == nil {
		return nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("account: %w", err)
	}
	if filepath.Base(path) == AccountFile {
		m.cfg.Logf("account: WARNING %s is missing: recreating the DEFAULT account file with the well-known admin/guest passwords (anyone can log in as admin); change them or restore the file", path)
	} else {
		m.cfg.Logf("account: WARNING %s is missing: recreating the default group file (admin/guest policies)", path)
	}
	return writeAtomic(path, data, 0o600)
}

// readFile stats before reading so a change racing the read is seen again
// on the next pass.
func (m *Manager) readFile(path string) ([]byte, fileStamp, error) {
	// A FIFO or device would block Open while the mutex is held; only a
	// regular file is read (Java File.canRead has the same intent).
	if st, err := os.Stat(path); err != nil {
		return nil, fileStamp{}, fmt.Errorf("account: %w", err)
	} else if !st.Mode().IsRegular() {
		return nil, fileStamp{mod: st.ModTime(), size: st.Size()}, fmt.Errorf("account: %s is not a regular file", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fileStamp{}, fmt.Errorf("account: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fileStamp{}, fmt.Errorf("account: %w", err)
	}
	// Rejections carry the stamp so the same file version is logged once.
	stamp := fileStamp{mod: info.ModTime(), size: info.Size()}
	if !info.Mode().IsRegular() {
		return nil, stamp, fmt.Errorf("account: %s is not a regular file", path)
	}
	if info.Size() > m.cfg.MaxFileBytes {
		return nil, stamp, fmt.Errorf("account: %s exceeds %d bytes", path, m.cfg.MaxFileBytes)
	}
	data, err := io.ReadAll(io.LimitReader(f, m.cfg.MaxFileBytes+1))
	if err != nil {
		return nil, stamp, fmt.Errorf("account: %w", err)
	}
	if int64(len(data)) > m.cfg.MaxFileBytes {
		return nil, stamp, fmt.Errorf("account: %s exceeds %d bytes", path, m.cfg.MaxFileBytes)
	}
	return data, stamp, nil
}

func statStamp(path string) (fileStamp, bool) {
	info, err := os.Stat(path)
	if err != nil {
		return fileStamp{}, false
	}
	return fileStamp{mod: info.ModTime(), size: info.Size()}, true
}

func (m *Manager) readDoc(path string, def []byte) (*xmlDocument, fileStamp, error) {
	if err := m.ensureFile(path, def); err != nil {
		return nil, fileStamp{}, err
	}
	data, stamp, err := m.readFile(path)
	if err != nil {
		return nil, stamp, err
	}
	doc, err := parseXML(data)
	if err != nil {
		return nil, stamp, fmt.Errorf("%s: %w", path, err)
	}
	return doc, stamp, nil
}

// loadLocked runs one load of path when forced or when its stamp changed
// (a deleted file has a changed stamp and is recreated, as in Java).
func (m *Manager) loadLocked(force bool, path string, def []byte, current, bad *fileStamp, apply func(*xmlDocument) error) error {
	if !force {
		if stamp, ok := statStamp(path); ok && (stamp.mod.Equal(current.mod) && stamp.size == current.size || stamp.mod.Equal(bad.mod) && stamp.size == bad.size) {
			return nil
		}
	}
	doc, stamp, err := m.readDoc(path, def)
	if err == nil {
		err = apply(doc)
	}
	if err != nil {
		if stamp != (fileStamp{}) {
			*bad = stamp
		}
		m.cfg.Logf("account: load %s: %v", path, err)
		return err
	}
	*current, *bad = stamp, fileStamp{}
	m.cfg.Logf("account: loaded %s", path)
	return nil
}

func (m *Manager) loadAccountsLocked(force bool) error {
	return m.loadLocked(force, m.accountPath, defaultAccountXML, &m.accountStamp, &m.accountBad, func(doc *xmlDocument) error {
		m.accounts.Store(newAccountState(parseAccounts(doc)))
		return nil
	})
}

func (m *Manager) loadGroupsLocked(force bool) error {
	return m.loadLocked(force, m.groupPath, defaultGroupXML, &m.groupStamp, &m.groupBad, func(doc *xmlDocument) error {
		groups, err := parseGroups(doc, m.cfg.Logf)
		if err != nil {
			return err
		}
		m.groups.Store(newGroupState(groups))
		return nil
	})
}

func newAccountState(list []Account) *accountState {
	s := &accountState{list: list, index: make(map[string]int, len(list))}
	for i, a := range list {
		s.index[a.ID] = i
	}
	return s
}

func newGroupState(list []Group) *groupState {
	s := &groupState{list: list, index: make(map[string]int, len(list))}
	for i, g := range list {
		s.index[g.Name] = i
	}
	return s
}

// writeAtomic replaces path through a synced temporary file in the same
// directory. A symbolic link or other non-regular target is refused.
func writeAtomic(path string, data []byte, perm fs.FileMode) error {
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("account: refusing to replace non-regular %s", path)
		}
		perm = info.Mode().Perm()
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("account: %w", err)
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("account: %w", err)
	}
	name := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			tmp.Close()
			os.Remove(name)
		}
	}()
	if _, err = tmp.Write(data); err == nil {
		if err = tmp.Chmod(perm); err == nil {
			if err = tmp.Sync(); err == nil {
				err = tmp.Close()
			}
		}
	}
	if err != nil {
		return fmt.Errorf("account: %w", err)
	}
	if err = os.Rename(name, path); err != nil {
		return fmt.Errorf("account: %w", err)
	}
	ok = true
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// mutate parses the current file, applies change and persists the result.
// Java's writeXmlFileWithIndent has returned the text without writing it
// since cc9bda37; persisting it is the intended difference. The snapshot is
// rebuilt from the written document, i.e. what a restart would load.
func (m *Manager) mutate(path string, def []byte, current, bad *fileStamp, change func(*xmlDocument) error, apply func(*xmlDocument) error) error {
	doc, _, err := m.readDoc(path, def)
	if err != nil {
		return err
	}
	if err := change(doc); err != nil {
		return err
	}
	out := doc.marshal()
	// The written bytes must load again before they replace the file; the
	// snapshot comes from this parse, not from the in-memory tree.
	reparsed, err := parseXML(out)
	if err != nil {
		return fmt.Errorf("account: serialized %s does not parse: %w", path, err)
	}
	if err := apply(reparsed); err != nil {
		return err
	}
	if err := writeAtomic(path, out, 0o600); err != nil {
		return err
	}
	if stamp, ok := statStamp(path); ok {
		*current, *bad = stamp, fileStamp{}
	}
	return nil
}

func (m *Manager) logResult(op string, err error) (bool, error) {
	if err != nil {
		m.cfg.Logf("account: %s: %v", op, err)
		return false, err
	}
	return true, nil
}

// Add is AccountManager.addAccount: false when the id exists. The id
// existence check uses the file as currently on disk.
func (m *Manager) Add(a Account) (bool, error) {
	if err := checkAccount(a); err != nil {
		return m.logResult("add account", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	exists := false
	var staged *accountState
	err := m.mutate(m.accountPath, defaultAccountXML, &m.accountStamp, &m.accountBad, func(doc *xmlDocument) error {
		for _, cur := range parseAccounts(doc) {
			if cur.ID == a.ID {
				exists = true
				return errExists
			}
		}
		return addAccountDoc(doc, a)
	}, func(doc *xmlDocument) error {
		staged = newAccountState(parseAccounts(doc))
		return nil
	})
	if exists {
		return false, nil
	}
	if err == nil {
		m.accounts.Store(staged)
	}
	return m.logResult("add account", err)
}

var errExists = errors.New("account: exists")

// Edit is AccountManager.editAccount: false when the id is unknown.
func (m *Manager) Edit(a Account) (bool, error) {
	if err := checkAccount(a); err != nil {
		return m.logResult("edit account", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	missing := false
	var staged *accountState
	err := m.mutate(m.accountPath, defaultAccountXML, &m.accountStamp, &m.accountBad, func(doc *xmlDocument) error {
		found := false
		for _, cur := range parseAccounts(doc) {
			found = found || cur.ID == a.ID
		}
		if !found {
			missing = true
			return errExists
		}
		return editAccountDoc(doc, a)
	}, func(doc *xmlDocument) error {
		staged = newAccountState(parseAccounts(doc))
		return nil
	})
	if missing {
		return false, nil
	}
	if err == nil {
		m.accounts.Store(staged)
	}
	return m.logResult("edit account", err)
}

// AddGroup is AccountManager.addAccountGroup/GroupFileHandler.addAccountGroup.
// Elements are written in the given order (the service passes the received
// order; Java writes the MapValue's Hashtable enumeration order). Java does
// not reject an existing name: another <Group> is appended and the later one wins.
func (m *Manager) AddGroup(name string, policy []Policy) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var staged *groupState
	err := m.mutate(m.groupPath, defaultGroupXML, &m.groupStamp, &m.groupBad, func(doc *xmlDocument) error {
		return addGroupDoc(doc, name, policy)
	}, m.stageGroups(&staged))
	if err == nil {
		m.groups.Store(staged)
	}
	return m.logResult("add group", err)
}

// EditGroupPolicy is GroupFileHandler.editGroupPolicy followed by
// loadGroupFile; an unknown group is ignored and still answers true.
func (m *Manager) EditGroupPolicy(edits []GroupPolicyEdit) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var staged *groupState
	err := m.mutate(m.groupPath, defaultGroupXML, &m.groupStamp, &m.groupBad, func(doc *xmlDocument) error {
		return editPolicyDoc(doc, edits)
	}, m.stageGroups(&staged))
	if err == nil {
		m.groups.Store(staged)
	}
	return m.logResult("edit group policy", err)
}

func (m *Manager) stageGroups(staged **groupState) func(*xmlDocument) error {
	return func(doc *xmlDocument) error {
		groups, err := parseGroups(doc, m.cfg.Logf)
		if err != nil {
			return err
		}
		*staged = newGroupState(groups)
		return nil
	}
}

// Authorize is AccountManager.authorizeAccount. With LDAP the directory
// decides and builds the Account; otherwise the stored password text must
// equal pass. Errors (directory unreachable, protocol) mean "not authorized"
// to the Client and are also returned for the caller's log.
func (m *Manager) Authorize(ctx context.Context, id, pass string) (Account, bool, error) {
	if err := ctx.Err(); err != nil {
		return Account{}, false, err
	}
	if l := m.ldap.Load(); l != nil {
		a, ok, err := l.authorize(ctx, id, pass)
		if err != nil {
			m.cfg.Logf("account: LDAP authorize %q: %v", id, err)
		}
		return a, ok, err
	}
	a, ok := m.Account(id)
	if !ok || subtle.ConstantTimeCompare([]byte(a.Password), []byte(pass)) != 1 {
		return Account{}, false, nil
	}
	return a, true, nil
}

// SetLDAP replaces the LDAP settings used by the next Authorize
// (AccountManager.authorizeAccount reads account_use_ldap and
// account_ldap_* from Configure on every LOGIN/CHECK_LOGIN). nil is
// account_use_ldap=false: file authentication. An unchanged configuration
// keeps the current authenticator, so a reload does not log the same
// configuration error again. An invalid URL or authentication is reported
// once and makes every LDAP login fail (see Open); only a negative
// Timeout/MaxMessageBytes is an error, and then the previous mode stays.
// An Authorize already in progress finishes with the settings it started with.
func (m *Manager) SetLDAP(cfg *LDAPConfig) error {
	m.ldapMu.Lock()
	defer m.ldapMu.Unlock()
	cur := m.ldap.Load()
	if cfg == nil {
		m.ldap.Store(nil)
		return nil
	}
	if cur != nil && cur.input == *cfg {
		return nil
	}
	a, err := newLDAPAuthenticator(*cfg, m.cfg.Logf)
	if err != nil {
		return err
	}
	m.ldap.Store(a)
	return nil
}

// Account is AccountManager.getAccount from the local file.
func (m *Manager) Account(id string) (Account, bool) {
	s := m.accounts.Load()
	i, ok := s.index[id]
	if !ok {
		return Account{}, false
	}
	return s.list[i], true
}

// Accounts returns the accounts in file order.
func (m *Manager) Accounts() []Account {
	return append([]Account(nil), m.accounts.Load().list...)
}

// AvailableID is AccountManager.avaliableId.
func (m *Manager) AvailableID(id string) bool {
	_, exists := m.accounts.Load().index[id]
	return !exists
}

// Groups returns the group names in file order (getGroupList).
func (m *Manager) Groups() []string {
	list := m.groups.Load().list
	out := make([]string, len(list))
	for i, g := range list {
		out[i] = g.Name
	}
	return out
}

// GroupPolicy returns one group's policies in file order.
func (m *Manager) GroupPolicy(name string) ([]Policy, bool) {
	s := m.groups.Load()
	i, ok := s.index[name]
	if !ok {
		return nil, false
	}
	return append([]Policy(nil), s.list[i].Policy...), true
}

// GroupPolicies returns every group and its policies from one snapshot.
func (m *Manager) GroupPolicies() []Group {
	list := m.groups.Load().list
	out := make([]Group, len(list))
	for i, g := range list {
		out[i] = Group{Name: g.Name, Policy: append([]Policy(nil), g.Policy...)}
	}
	return out
}
