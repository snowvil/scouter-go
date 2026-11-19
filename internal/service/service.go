package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// Config holds explicit local credentials. Password is raw configuration;
// incoming Java Client passwords use CipherUtil's salted SHA256 representation.
// When Accounts is set it replaces Username/Password for LOGIN/CHECK_LOGIN
// and enables the AccountService commands (AccountCommands).
//
// ServerID and SearchXLogMaxCount are the initial live Settings (with the
// other Java defaults); SetSettings replaces them after a reload.
type Config struct {
	ServerID           string
	Location           *time.Location
	Username, Password string
	Clock              func() time.Time
	CounterXML         []byte
	SessionTTL         time.Duration
	MaxSessions        int
	SearchXLogMaxCount int
	Metadata           MetadataBackend
	Accounts           AccountStore
}

type Service struct {
	config         Config
	backend        Backend
	passwordDigest string
	mu             sync.Mutex
	sessions       map[int64]*sessionEntry
	sessionOrder   []int64
	remoteControls *remoteControlTable
	settings       atomic.Pointer[Settings]
}

// AgentBackend is optional: query handlers otherwise have no transport pool dependency.
type AgentBackend interface {
	AgentRequest(context.Context, int32, string, *protocol.MapPack) ([]protocol.Pack, error)
}

func New(config Config, backend Backend) (*Service, error) {
	if backend == nil {
		return nil, fmt.Errorf("service: nil backend")
	}
	if config.Accounts == nil && (config.Username == "" || config.Password == "") {
		return nil, fmt.Errorf("service: explicit credentials required")
	}
	if config.ServerID == "" {
		config.ServerID = "1"
	}
	if config.Location == nil {
		config.Location = time.Local
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.SessionTTL == 0 {
		config.SessionTTL = 3 * time.Hour
	}
	if config.MaxSessions == 0 {
		config.MaxSessions = 1024
	}
	if config.SearchXLogMaxCount == 0 {
		config.SearchXLogMaxCount = 500
	}
	if config.SessionTTL < 0 || config.MaxSessions < 1 || config.SearchXLogMaxCount < 1 || config.SearchXLogMaxCount > 1<<31-1 {
		return nil, fmt.Errorf("service: invalid session limits")
	}
	config.CounterXML = append([]byte(nil), config.CounterXML...)
	digest := sha256.Sum256([]byte("qwertyuiop!@#$%^&*()zxcvbnm,." + config.Password))
	config.Password = ""
	s := &Service{config: config, backend: backend, passwordDigest: hex.EncodeToString(digest[:]), sessions: make(map[int64]*sessionEntry), remoteControls: newRemoteControlTable()}
	settings := DefaultSettings(config.ServerID)
	settings.SearchXLogMaxCount = int32(config.SearchXLogMaxCount)
	s.SetSettings(settings)
	return s, nil
}

func (s *Service) Handlers() transport.TCPHandlers {
	return transport.TCPHandlers{Client: s.Handle, Authorize: s.Authorize, Sessions: s}
}

// validSession is LoginManager.validSession (CHECK_SESSION): getKeepAlive
// with the default keep time, even for an internal session.
func (s *Service) validSession(key int64) bool { return s.touchSession(key, false) }

// touchSession extends a live session. okSession (connection authorization)
// keeps an internal session for internalSessionTTL.
func (s *Service) touchSession(key int64, keepInternal bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.config.Clock()
	e := s.liveSessionLocked(key, now)
	if e == nil {
		return false
	}
	ttl := s.config.SessionTTL
	if keepInternal && e.internal {
		ttl = internalSessionTTL
	}
	e.expires = now.Add(ttl)
	if e.conns == 0 {
		e.idleSince = now // a request restarts the grace of a connectionless session
	}
	return true
}

func (s *Service) Authorize(ctx context.Context, request transport.ClientRequest) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return s.touchSession(request.Session, true), nil
}

func freeCommand(command string) bool {
	return command == "LOGIN" || command == "SERVER_VERSION" || command == "SERVER_TIME"
}

// SupportedCommands is a detached, explicit list; unsupported commands fail before body parsing.
func (s *Service) SupportedCommands() []string {
	commands := []string{"LOGIN", "CHECK_SESSION", "SERVER_VERSION", "SERVER_TIME", "SERVER_STATUS", "GET_XML_COUNTER", "OBJECT_LIST_REAL_TIME", "OBJECT_LIST_LOAD_DATE", "OBJECT_INFO", "GET_TEXT", "GET_TEXT_100", "GET_TEXT_PACK", "GET_TEXT_ANY_TYPE", "COUNTER_REAL_TIME", "COUNTER_REAL_TIME_ALL", "COUNTER_REAL_TIME_MULTI", "COUNTER_PAST_TIME", "COUNTER_PAST_TIME_ALL", "COUNTER_TODAY", "COUNTER_PAST_DATE", "TRANX_REAL_TIME_GROUP", "TRANX_REAL_TIME_GROUP_LATEST", "TRANX_LOAD_TIME_GROUP", "TRANX_LOAD_TIME_GROUP_V2", "XLOG_READ_BY_TXID", "TRANX_PROFILE", "TRANX_PROFILE_FULL"}
	if _, ok := s.backend.(AgentBackend); ok {
		commands = append(commands, "OBJECT_ENV")
	}
	commands = append(commands, ExtendedCounterCommands()...)
	commands = append(commands, PacketCommands()...)
	commands = append(commands, AgentReadCommands()...)
	commands = append(commands, DatabaseCounterCommands()...)
	commands = append(commands, LocalStoreCommands()...)
	commands = append(commands, LocalStoreMutationCommands()...)
	if s.config.Accounts != nil {
		commands = append(commands, AccountCommands()...)
	}
	commands = append(commands, ConfigureCommands()...)
	commands = append(commands, ManageCommands()...)
	commands = append(commands, MetadataCommands()...)
	commands = append(commands, VisitorCommands...)
	commands = append(commands, TagCommands...)
	commands = append(commands, "XLOG_READ_BY_GXID", "XLOG_LOAD_BY_TXIDS", "XLOG_LOAD_BY_GXID", "QUICKSEARCH_XLOG_LIST", "SEARCH_XLOG_LIST")
	seen := make(map[string]bool, len(commands))
	unique := commands[:0]
	for _, command := range commands {
		if !seen[command] {
			seen[command] = true
			unique = append(unique, command)
		}
	}
	return unique
}

func (s *Service) Handle(ctx context.Context, request transport.ClientRequest, stream *transport.Stream) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !freeCommand(request.Command) && !request.Authenticated {
		return transport.ErrUnauthorized
	}
	if handled, err := s.HandleAccountCommand(ctx, request, stream); handled {
		return err
	}
	if handled, err := s.HandleConfigureCommand(ctx, request, stream); handled {
		return err
	}
	if handled, err := s.HandleManageCommand(ctx, request, stream); handled {
		return err
	}
	if handled, err := s.HandleLocalStoreCommand(ctx, request, stream); handled {
		return err
	}
	if handled, err := s.HandleMetadataCommand(ctx, request, stream); handled {
		return err
	}
	if handled, err := s.HandleDatabaseCounterCommand(ctx, request, stream); handled {
		return err
	}
	if handled, err := s.HandleAgentReadCommand(ctx, request, stream); handled {
		return err
	}
	if handled, err := s.HandlePacketCommand(ctx, request, stream); handled {
		return err
	}
	if handled, err := s.HandleExtendedCounterCommand(ctx, request, stream); handled {
		return err
	}
	switch request.Command {
	case "LOGIN":
		return s.login(ctx, request, stream)
	case "SERVER_TIME":
		return stream.ResponsePack(newMap(entry("time", decimal(s.config.Clock().UnixMilli()))))
	case "SERVER_VERSION":
		if _, err := readMap(stream); err != nil {
			return err
		}
		return stream.ResponsePack(newMap(entry("version", stringValue(serverVersion))))
	case "SERVER_STATUS":
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return stream.ResponsePack(newMap(entry("used", decimal(int64(m.HeapAlloc))), entry("total", decimal(int64(m.HeapSys))), entry("time", decimal(s.config.Clock().UnixMilli()))))
	case "GET_XML_COUNTER":
		if len(s.config.CounterXML) == 0 {
			return fmt.Errorf("%w: counter XML not configured", transport.ErrUnsupported)
		}
		response := newMap(entry("default", protocol.Value{Type: protocol.ValueBlob, Bytes: s.config.CounterXML}))
		// CounterManager.getXmlCustomContent: counters.site.xml when present.
		if custom, ok := s.config.Metadata.(interface{ CustomCounterXML() []byte }); ok {
			if b := custom.CustomCounterXML(); len(b) > 0 {
				response.Entries = append(response.Entries, protocol.Entry{Key: "custom", Value: protocol.Value{Type: protocol.ValueBlob, Bytes: b}})
			}
		}
		return stream.ResponsePack(response)
	case "OBJECT_LIST_REAL_TIME":
		objects, err := s.backend.Objects(ctx, "")
		if err != nil {
			return err
		}
		for _, object := range objects {
			if err := stream.ResponsePack(object); err != nil {
				return err
			}
		}
		return nil
	}
	supported := false
	for _, command := range s.SupportedCommands() {
		if command == request.Command {
			supported = true
			break
		}
	}
	if !supported {
		return fmt.Errorf("%w: command %s", transport.ErrUnsupported, request.Command)
	}
	param, err := readMap(stream)
	if err != nil {
		return err
	}
	return s.query(ctx, request.Command, param, stream)
}

func (s *Service) login(ctx context.Context, request transport.ClientRequest, stream *transport.Stream) error {
	p, err := readMap(stream)
	if err != nil {
		return err
	}
	id, err := getText(p, "id")
	if err != nil {
		return err
	}
	password, err := getText(p, "pass")
	if err != nil {
		return err
	}
	var account accountLogin
	if s.config.Accounts != nil {
		if account, err = s.accountLogin(ctx, request, id, password); err != nil {
			return err
		}
	} else {
		account.ok = subtle.ConstantTimeCompare([]byte(id), []byte(s.config.Username)) == 1 && subtle.ConstantTimeCompare([]byte(password), []byte(s.passwordDigest)) == 1
	}
	internal := lookup(p, "internal")
	session := int64(0)
	if account.ok {
		if session, err = s.newSession(p, id, internal.Type == protocol.ValueText && strings.EqualFold(internal.Text, "true")); err != nil {
			return err
		}
		// Held by the LOGIN connection at once (session liveness, sessions.go).
		request.Conn.BindSession(session)
	}
	response := newMap(entry("id", stringValue(id)), entry("session", decimal(session)))
	if session == 0 {
		response.Entries = append(response.Entries, entry("error", stringValue("login fail")))
		return stream.ResponsePack(response)
	}
	// One Settings value per LOGIN; Java reads Configure.getInstance() per field.
	live := s.Settings()
	response.Entries = append(response.Entries, entry("time", decimal(s.config.Clock().UnixMilli())), entry("server_id", stringValue(live.ServerID)))
	if s.config.Accounts != nil {
		response.Entries = append(response.Entries, s.accountLoginEntries(id, account.group)...)
	} else {
		response.Entries = append(response.Entries,
			entry("type", stringValue("admin")), entry("version", stringValue(serverVersion)), entry("client_version", stringValue(clientVersion)), entry("email", stringValue("")),
			entry("timezone", stringValue(s.config.Location.String())), entry("policy", singleAdminPolicy()))
	}
	response.Entries = append(response.Entries, entry("menu", protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{entry("tag_count", boolean(live.TagCountEnabled))}}),
		entry("so_time_out", decimal(int64(live.ClientSoTimeoutMillis))), entry("ext_link_name", stringValue(live.ExtLinkName)), entry("ext_link_url_pattern", stringValue(live.ExtLinkURLPattern)))
	return stream.ResponsePack(response)
}

const (
	serverVersion = "Scouter Go development (wire 2.21.3)"
	clientVersion = "2.21.3"
)

// newSession registers a login; 0 means the session table is full.
func (s *Service) newSession(p *protocol.MapPack, id string, internal bool) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.config.Clock()
	s.purgeExpiredLocked(now)
	if len(s.sessions) >= s.config.MaxSessions {
		return 0, nil
	}
	session := int64(0)
	for session == 0 {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return 0, err
		}
		candidate := int64(binary.BigEndian.Uint64(b[:]))
		if candidate != 0 {
			if _, exists := s.sessions[candidate]; !exists {
				session = candidate
			}
		}
	}
	ttl := s.config.SessionTTL
	if internal {
		ttl = internalSessionTTL
	}
	s.sessions[session] = &sessionEntry{
		user:    MetadataLoginUser{Session: session, ID: id, IP: lookup(p, "ip").Text, Host: lookup(p, "hostname").Text, Version: lookup(p, "version").Text, LoginTime: now.UnixMilli()},
		expires: now.Add(ttl), internal: internal, idleSince: now,
	}
	s.sessionOrder = append(s.sessionOrder, session)
	return session, nil
}
