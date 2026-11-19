package logger

// RequestLogger command set (RequestLogger.java static block). Commands that
// AgentCall relays to an agent are added at run time by
// RegisterRequestCommand; ACTIVE_QUERY_LIST (demandSet) is never added.
var requestCommands = []string{"SERVER_DB_DELETE", "REMOTE_CONTROL", "REMOTE_CONTROL_ALL", "TRANX_LOAD_TIME_GROUP"}

const (
	demandCommand = "ACTIVE_QUERY_LIST"
	// maxRequestCommands bounds the registered set. Java's set is unbounded
	// but only grows with AgentCall command constants.
	maxRequestCommands = 1024
)

func defaultRequestCommands() map[string]struct{} {
	m := make(map[string]struct{}, len(requestCommands))
	for _, c := range requestCommands {
		m[c] = struct{}{}
	}
	return m
}

// RequestLookup resolves a Client session to its login id and address
// (LoginManager.getUser). ok is false for an unknown session.
type RequestLookup func(session int64) (id, ip string, ok bool)

// RequestCommand is RequestLogger.add(cmd, session), called for every Client
// TCP command before it is handled. When cmd is in the logged set it queues
// "yyyyMMdd HH:mm:ss.SSS <id>(<ip>) <cmd>" for log_dir/request-yyyyMMdd.log
// and returns true if the session was known; an unknown session is written
// as "unknown<session>()" and returns false. Other commands return false
// without calling lookup. The request log follows the live log_dir.
func (l *Logger) RequestCommand(cmd string, session int64, lookup RequestLookup) bool {
	if l == nil {
		return false
	}
	l.reqMu.Lock()
	_, logged := l.reqCmds[cmd]
	l.reqMu.Unlock()
	if !logged {
		return false
	}
	id, ip, ok := "", "", false
	if lookup != nil {
		id, ip, ok = lookup(session)
	}
	if !ok {
		id, ip = "unknown"+itoa64(session), ""
	}
	l.request(cmd, id, ip)
	return ok
}

// RegisterRequestCommand is RequestLogger.registerCmd(cmd), called by the
// agent relay (AgentCall) before it forwards cmd: the first registration adds
// cmd to the logged set and writes "<timestamp> unknown(register) <cmd>".
func (l *Logger) RegisterRequestCommand(cmd string) {
	if l == nil || cmd == demandCommand {
		return
	}
	l.reqMu.Lock()
	_, exists := l.reqCmds[cmd]
	full := len(l.reqCmds) >= maxRequestCommands
	if !exists && !full {
		l.reqCmds[cmd] = struct{}{}
	}
	l.reqMu.Unlock()
	if exists {
		return
	}
	if full {
		l.dropped.Add(1)
		return
	}
	l.request(cmd, "unknown", "register")
}

func (l *Logger) request(cmd, id, ip string) {
	ms := l.now().UnixMilli()
	l.enqueue(record{kind: kindRequest, ms: ms, text: l.cal.timestamp(ms) + " " + id + "(" + ip + ") " + cmd})
}
