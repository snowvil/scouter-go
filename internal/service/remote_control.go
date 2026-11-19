package service

import (
	"context"
	"sync"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// remoteControlQueueSize is RemoteControlManager.scala:36 `new Queue(5)`.
const remoteControlQueueSize = 5

// remoteControl is RemoteControl.scala: the command text, the full request
// MapPack (CHECK_JOB returns it verbatim), the enqueue time and the sender.
type remoteControl struct {
	command   string
	param     *protocol.MapPack
	time      int64
	commander int64
}

// remoteControlTable is RemoteControlManager.commandTable: one bounded FIFO
// per target session. Like scouter.util.Queue(5).push, a sixth command drops
// the oldest entry, is queued and the add reports false (S148). Queues of
// sessions that are no longer logged in are pruned on every REMOTE_CONTROL/
// REMOTE_CONTROL_ALL/CHECK_JOB instead of by Java's 5s daemon, and the number
// of queues is bounded by MaxSessions.
type remoteControlTable struct {
	mu     sync.Mutex
	queues map[int64][]remoteControl
}

// newRemoteControlTable builds the per-Service queue table; Service.New
// stores it in Service.remoteControls.
func newRemoteControlTable() *remoteControlTable {
	return &remoteControlTable{queues: make(map[int64][]remoteControl)}
}

func (s *Service) remoteControlTable() *remoteControlTable {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.remoteControls == nil { // Services built without New (tests)
		s.remoteControls = newRemoteControlTable()
	}
	return s.remoteControls
}

// remoteControlMaxParamBytes bounds one queued command's encoded parameter
// (Java keeps the whole MapPack with only the count bound of 5 per session;
// AGENTS.md requires a byte bound). A larger parameter is refused like a full
// queue: not queued, success=false, logged.
const remoteControlMaxParamBytes = 64 << 10

// BroadcastRemoteControl is RegisterHandler.notifyAllClients: it queues the
// command for every logged-in session (commander 0) without blocking. A full
// queue drops its oldest entry like Java; the caller is the configuration
// reload path, not a Client request.
func (s *Service) BroadcastRemoteControl(command string, param *protocol.MapPack) {
	if param == nil {
		param = &protocol.MapPack{}
	}
	control := remoteControl{command: command, param: param, time: s.config.Clock().UnixMilli()}
	for _, u := range s.metadataLoginUsers() {
		s.remoteControlTable().add(u.Session, control, s.config.MaxSessions)
	}
}

// prune removes every queue whose session is not in live.
func (t *remoteControlTable) prune(live map[int64]bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for session := range t.queues {
		if !live[session] {
			delete(t.queues, session)
		}
	}
}

// add is RemoteControlManager.add. It returns false without queueing when
// maxQueues targets already have queues. A full queue drops its oldest entry,
// queues c and returns false (Queue.java:31-41).
func (t *remoteControlTable) add(session int64, c remoteControl, maxQueues int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	q, exists := t.queues[session]
	if !exists && len(t.queues) >= maxQueues {
		return false
	}
	if len(q) >= remoteControlQueueSize {
		rest := make([]remoteControl, remoteControlQueueSize-1, remoteControlQueueSize)
		copy(rest, q[len(q)-remoteControlQueueSize+1:])
		t.queues[session] = append(rest, c)
		return false
	}
	t.queues[session] = append(q, c)
	return true
}

// take is RemoteControlManager.getCommand: FIFO dequeue, ok=false when empty.
func (t *remoteControlTable) take(session int64) (remoteControl, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	q := t.queues[session]
	if len(q) == 0 {
		return remoteControl{}, false
	}
	c := q[0]
	rest := make([]remoteControl, len(q)-1, remoteControlQueueSize)
	copy(rest, q[1:])
	t.queues[session] = rest
	return c, true
}

// remoteControlSessions returns the current login users and their session set,
// expiring stale sessions first.
func (s *Service) remoteControlSessions() ([]MetadataLoginUser, map[int64]bool) {
	users := s.metadataLoginUsers()
	live := make(map[int64]bool, len(users))
	for _, u := range users {
		live[u.Session] = true
	}
	return users, live
}

func remoteControlIP(users []MetadataLoginUser, session int64) string {
	for _, u := range users {
		if u.Session == session {
			return u.IP
		}
	}
	return ""
}

// remoteControlCommand implements ManageService.java:75-126 with the
// management-commands.md §9-5/§9-13 decisions.
//
//   - REMOTE_CONTROL: MapPack{toSession, fromSession, command, ...} → enqueue for
//     toSession → MapPack{success: Boolean}. Java logs both users' IPs after
//     enqueueing and throws NullPointerException for a session that is not
//     logged in (command queued, no answer); here such a request queues
//     nothing and answers success=false. A full queue answers false too.
//   - REMOTE_CONTROL_ALL: same body, enqueue for every login session (the
//     sender included), no response items; a full queue is logged (S148). An
//     unknown fromSession queues nothing (Java: NPE after queueing).
//   - CHECK_JOB: MapPack{session} → dequeue → TextPack(command) + MapPack(param),
//     or no items when nothing is queued. Java dequeues any session's queue;
//     here only the requesting connection's own session is dequeued and a
//     foreign session is logged and answered with no items.
func (s *Service) remoteControlCommand(ctx context.Context, r transport.ClientRequest, stream *transport.Stream) error {
	p, err := readMap(stream)
	if err != nil {
		return err
	}
	table := s.remoteControlTable()
	users, live := s.remoteControlSessions()
	table.prune(live)
	if r.Command == "CHECK_JOB" {
		session, err := getNumber(p, "session")
		if err != nil {
			return err
		}
		if session != r.Session {
			manageLogf("[CHECK_JOB] session=%d does not own the requested queue %d; no job returned peer=%v", r.Session, session, r.Peer)
			return nil
		}
		control, ok := table.take(session)
		if !ok {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := stream.ResponsePack(&protocol.TextPack{Text: control.command}); err != nil {
			return err
		}
		return stream.ResponsePack(control.param)
	}
	// MapPack.getText: a non-Text command is null; TextPack(null) is not a
	// meaningful job, so it is an explicit failure here.
	command, ok := javaText(p, "command")
	if !ok {
		return badParam("command")
	}
	from := javaLongDefault(p, "fromSession", 0)
	control := remoteControl{command: command, param: p, time: s.config.Clock().UnixMilli(), commander: from}
	if encoded, err := protocol.EncodePack(p, protocol.Limits{}); err != nil || len(encoded) > remoteControlMaxParamBytes {
		if err != nil {
			manageLogf("[%s] rejected: parameter cannot be encoded: %v session=%d peer=%v", command, err, r.Session, r.Peer)
		} else {
			manageLogf("[%s] rejected: parameter %d bytes exceeds %d session=%d peer=%v", command, len(encoded), remoteControlMaxParamBytes, r.Session, r.Peer)
		}
		if r.Command == "REMOTE_CONTROL_ALL" {
			return ctx.Err()
		}
		return stream.ResponsePack(newMap(entry("success", boolean(false))))
	}
	if r.Command == "REMOTE_CONTROL_ALL" {
		if !live[from] {
			manageLogf("[REMOTE_CONTROL_ALL] %s rejected: fromSession=%d is not logged in session=%d peer=%v", command, from, r.Session, r.Peer)
			return ctx.Err()
		}
		for _, u := range users {
			if !table.add(u.Session, control, s.config.MaxSessions) {
				manageLogf("[REMOTE_CONTROL_ALL] S148 RemoteControlManager queue exceeded! command:%s toSession=%d", command, u.Session)
			}
		}
		manageLogf("[REMOTE_CONTROL_ALL] %s from %s session=%d peer=%v", command, remoteControlIP(users, from), r.Session, r.Peer)
		return ctx.Err()
	}
	to := javaLongDefault(p, "toSession", 0)
	success := false
	if !live[from] || !live[to] {
		manageLogf("[REMOTE_CONTROL] %s rejected: fromSession=%d live=%v toSession=%d live=%v session=%d peer=%v", command, from, live[from], to, live[to], r.Session, r.Peer)
	} else {
		success = table.add(to, control, s.config.MaxSessions)
		if !success {
			manageLogf("[REMOTE_CONTROL] S148 RemoteControlManager queue exceeded! command:%s toSession=%d", command, to)
		}
		manageLogf("[REMOTE_CONTROL] %s from %s to %s %v session=%d peer=%v", command, remoteControlIP(users, from), remoteControlIP(users, to), success, r.Session, r.Peer)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.ResponsePack(newMap(entry("success", boolean(success))))
}
