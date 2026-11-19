package service

import (
	"bufio"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func checkJob(t *testing.T, r *bufio.Reader, want *protocol.MapPack) {
	t.Helper()
	packs := readPacks(t, r)
	if want == nil {
		if len(packs) != 0 {
			t.Fatal("unexpected job", packs)
		}
		return
	}
	if len(packs) != 2 {
		t.Fatal(packs)
	}
	text, ok := packs[0].(*protocol.TextPack)
	if !ok || text.Text != mapValue(want, "command").Text {
		t.Fatal(packs[0])
	}
	if !reflect.DeepEqual(packs[1], want) {
		t.Fatal(packs[1], want)
	}
}

func remoteControlParam(from, to int64, command string, extra ...protocol.Entry) *protocol.MapPack {
	p := newMap(entry("toSession", decimal(to)), entry("fromSession", decimal(from)), entry("command", stringValue(command)))
	p.Entries = append(p.Entries, extra...)
	return p
}

func TestRemoteControlEnqueueDequeueAndOverflow(t *testing.T) {
	logs := captureManageLog(t)
	s, connA, rA := startService(t, queryConfig(), newManageFake())
	a := loginSession(t, connA, rA)
	connB, rB := secondClient(t, connA)
	b := loginSession(t, connB, rB)
	// Nothing queued: CHECK_JOB answers no items (ManageService.java:113-126).
	send(t, connB, "CHECK_JOB", b, newMap(entry("session", decimal(b))))
	checkJob(t, rB, nil)

	param := remoteControlParam(a, b, "OPEN_VIEW", entry("extra", stringValue("kept")), entry("count", decimal(3)))
	send(t, connA, "REMOTE_CONTROL", a, param)
	packs := readPacks(t, rA)
	if len(packs) != 1 || !mapValue(packs[0].(*protocol.MapPack), "success").Bool {
		t.Fatal(packs)
	}
	if lines := logs.snapshot(); len(lines) != 1 || !strings.HasPrefix(lines[0], "[REMOTE_CONTROL] OPEN_VIEW from ") || !strings.Contains(lines[0], " true ") {
		t.Fatal(lines)
	}
	// The sender's queue is untouched; the target gets TextPack + the verbatim
	// request MapPack, once.
	send(t, connA, "CHECK_JOB", a, newMap(entry("session", decimal(a))))
	checkJob(t, rA, nil)
	send(t, connB, "CHECK_JOB", b, newMap(entry("session", decimal(b))))
	checkJob(t, rB, param)
	send(t, connB, "CHECK_JOB", b, newMap(entry("session", decimal(b))))
	checkJob(t, rB, nil)

	// Queue(5).push: the sixth command answers success=false, drops the
	// oldest entry and is queued; the remaining five dequeue in FIFO order.
	var params []*protocol.MapPack
	for i := 0; i < remoteControlQueueSize+1; i++ {
		p := remoteControlParam(a, b, fmt.Sprintf("CMD%d", i))
		params = append(params, p)
		send(t, connA, "REMOTE_CONTROL", a, p)
		packs := readPacks(t, rA)
		if len(packs) != 1 || mapValue(packs[0].(*protocol.MapPack), "success").Bool != (i < remoteControlQueueSize) {
			t.Fatal(i, packs)
		}
	}
	if s.remoteControlTable().count(b) != remoteControlQueueSize {
		t.Fatal(s.remoteControlTable().count(b))
	}
	for i := 1; i <= remoteControlQueueSize; i++ {
		send(t, connB, "CHECK_JOB", b, newMap(entry("session", decimal(b))))
		checkJob(t, rB, params[i])
	}
	send(t, connB, "CHECK_JOB", b, newMap(entry("session", decimal(b))))
	checkJob(t, rB, nil)
	overflow := 0
	for _, line := range logs.snapshot() {
		if strings.Contains(line, "S148") {
			overflow++
		}
	}
	if overflow != 1 {
		t.Fatal(logs.snapshot())
	}
}

func TestRemoteControlAllAndUnknownSessions(t *testing.T) {
	logs := captureManageLog(t)
	s, connA, rA := startService(t, queryConfig(), newManageFake())
	a := loginSession(t, connA, rA)
	connB, rB := secondClient(t, connA)
	b := loginSession(t, connB, rB)
	param := newMap(entry("fromSession", decimal(a)), entry("command", stringValue("BROADCAST")))
	send(t, connA, "REMOTE_CONTROL_ALL", a, param)
	if packs := readPacks(t, rA); len(packs) != 0 {
		t.Fatal("REMOTE_CONTROL_ALL answered", packs)
	}
	if lines := logs.snapshot(); len(lines) != 1 || !strings.HasPrefix(lines[0], "[REMOTE_CONTROL_ALL] BROADCAST from ") {
		t.Fatal(lines)
	}
	// Every login session, the sender included, receives the job once.
	send(t, connA, "CHECK_JOB", a, newMap(entry("session", decimal(a))))
	checkJob(t, rA, param)
	send(t, connB, "CHECK_JOB", b, newMap(entry("session", decimal(b))))
	checkJob(t, rB, param)
	send(t, connA, "CHECK_JOB", a, newMap(entry("session", decimal(a))))
	checkJob(t, rA, nil)
	// CHECK_JOB for a session that never had a queue answers nothing.
	send(t, connA, "CHECK_JOB", a, newMap(entry("session", decimal(12345))))
	checkJob(t, rA, nil)

	// §9-5: Java queues first and then NullPointerExceptions on the log line
	// for an unknown session (no answer); here nothing is queued and the
	// request answers success=false on a connection that stays usable.
	for name, p := range map[string]*protocol.MapPack{
		"unknown to":   remoteControlParam(a, 12345, "X"),
		"unknown from": remoteControlParam(12345, b, "X"),
		"missing to":   newMap(entry("fromSession", decimal(a)), entry("command", stringValue("X"))),
	} {
		t.Run(name, func(t *testing.T) {
			connC, rC := secondClient(t, connA)
			c := loginSession(t, connC, rC)
			send(t, connC, "REMOTE_CONTROL", c, p)
			packs := readPacks(t, rC)
			if len(packs) != 1 || mapValue(packs[0].(*protocol.MapPack), "success").Type != protocol.ValueBoolean || mapValue(packs[0].(*protocol.MapPack), "success").Bool {
				t.Fatal(packs)
			}
			if s.remoteControlTable().count(b) != 0 || s.remoteControlTable().count(12345) != 0 {
				t.Fatal("rejected command queued")
			}
			send(t, connC, "SERVER_TIME", c, nil)
			if packs := readPacks(t, rC); len(packs) != 1 {
				t.Fatal(packs)
			}
		})
	}
	for name, p := range map[string]*protocol.MapPack{
		"missing command":  newMap(entry("toSession", decimal(b)), entry("fromSession", decimal(a))),
		"non-text command": newMap(entry("toSession", decimal(b)), entry("fromSession", decimal(a)), entry("command", decimal(1))),
	} {
		t.Run(name, func(t *testing.T) {
			connC, rC := secondClient(t, connA)
			c := loginSession(t, connC, rC)
			before := len(logs.snapshot())
			send(t, connC, "REMOTE_CONTROL", c, p)
			expectClosed(t, rC)
			if s.remoteControlTable().count(b) != 0 {
				t.Fatal("rejected command queued")
			}
			if lines := logs.snapshot()[before:]; len(lines) != 1 || !strings.HasPrefix(lines[0], "[REMOTE_CONTROL] rejected: ") {
				t.Fatal("rejection not logged", lines)
			}
		})
	}
	t.Run("all unknown from", func(t *testing.T) {
		connC, rC := secondClient(t, connA)
		c := loginSession(t, connC, rC)
		send(t, connC, "REMOTE_CONTROL_ALL", c, newMap(entry("fromSession", decimal(12345)), entry("command", stringValue("X"))))
		if packs := readPacks(t, rC); len(packs) != 0 {
			t.Fatal(packs)
		}
		if s.remoteControlTable().count(b) != 0 || s.remoteControlTable().count(a) != 0 {
			t.Fatal("rejected broadcast queued")
		}
	})
	t.Run("malformed", func(t *testing.T) {
		for _, body := range []protocol.Pack{&protocol.TextPack{}, newMap(entry("session", stringValue("1")))} {
			connC, rC := secondClient(t, connA)
			c := loginSession(t, connC, rC)
			send(t, connC, "CHECK_JOB", c, body)
			expectClosed(t, rC)
		}
	})
}

func TestRemoteControlSessionExpiryPrunesQueues(t *testing.T) {
	captureManageLog(t)
	var mu sync.Mutex
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	config := queryConfig()
	config.SessionTTL = time.Minute
	config.Clock = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	s, connA, rA := startService(t, config, newManageFake())
	a := loginSession(t, connA, rA)
	connB, rB := secondClient(t, connA)
	b := loginSession(t, connB, rB)
	send(t, connA, "REMOTE_CONTROL", a, remoteControlParam(a, b, "LATER"))
	if packs := readPacks(t, rA); len(packs) != 1 || !mapValue(packs[0].(*protocol.MapPack), "success").Bool {
		t.Fatal(packs)
	}
	connB.Close() // B's last connection: its session expires after the grace
	waitSessionConns(t, s, b, 0)
	mu.Lock()
	now = now.Add(2 * time.Minute)
	mu.Unlock()
	// The connection latch keeps A authenticated, but B's session has expired:
	// the next management command prunes B's queue (Java: 5s daemon).
	send(t, connA, "CHECK_JOB", a, newMap(entry("session", decimal(b))))
	checkJob(t, rA, nil)
	s.remoteControlTable().mu.Lock()
	queues := len(s.remoteControlTable().queues)
	s.remoteControlTable().mu.Unlock()
	if queues != 0 {
		t.Fatal("expired session queue kept", queues)
	}
	// A new login cannot be targeted by the expired sender either.
	connC, rC := secondClient(t, connA)
	connA.Close()
	waitSessionConns(t, s, a, 0)
	mu.Lock()
	now = now.Add(2 * time.Minute)
	mu.Unlock()
	c := loginSession(t, connC, rC)
	send(t, connC, "REMOTE_CONTROL", c, remoteControlParam(a, c, "STALE"))
	if packs := readPacks(t, rC); len(packs) != 1 || mapValue(packs[0].(*protocol.MapPack), "success").Bool {
		t.Fatal(packs)
	}
	if s.remoteControlTable().count(c) != 0 {
		t.Fatal("stale sender queued")
	}
}

func TestRemoteControlTableBounds(t *testing.T) {
	table := &remoteControlTable{queues: make(map[int64][]remoteControl)}
	for session := int64(1); session <= 3; session++ {
		if !table.add(session, remoteControl{command: "x"}, 3) {
			t.Fatal(session)
		}
	}
	if table.add(4, remoteControl{command: "x"}, 3) {
		t.Fatal("queue count exceeded MaxSessions")
	}
	if !table.add(3, remoteControl{command: "y"}, 3) {
		t.Fatal("existing queue refused")
	}
	for i := 2; i < remoteControlQueueSize; i++ {
		table.add(3, remoteControl{command: "z"}, 3)
	}
	// Queue.push on a full queue: false, oldest ("x") dropped, new kept.
	if table.add(3, remoteControl{command: "overflow"}, 3) || table.count(3) != remoteControlQueueSize {
		t.Fatal(table.count(3))
	}
	table.prune(map[int64]bool{3: true})
	if table.count(1) != 0 || table.count(2) != 0 || table.count(3) != remoteControlQueueSize {
		t.Fatal("prune")
	}
	for _, want := range []string{"y", "z", "z", "z", "overflow"} {
		if c, ok := table.take(3); !ok || c.command != want {
			t.Fatal(c, ok, want)
		}
	}
	if _, ok := table.take(3); ok {
		t.Fatal("queue not drained")
	}
	if _, ok := table.take(1); ok {
		t.Fatal("pruned queue answered")
	}
}

// §9-13: CHECK_JOB dequeues only the requesting connection's own session.
func TestCheckJobRequiresOwnSession(t *testing.T) {
	logs := captureManageLog(t)
	s, connA, rA := startService(t, queryConfig(), newManageFake())
	a := loginSession(t, connA, rA)
	connB, rB := secondClient(t, connA)
	b := loginSession(t, connB, rB)
	param := remoteControlParam(a, b, "POPUP_MESSAGE")
	send(t, connA, "REMOTE_CONTROL", a, param)
	if packs := readPacks(t, rA); len(packs) != 1 || !mapValue(packs[0].(*protocol.MapPack), "success").Bool {
		t.Fatal(packs)
	}
	before := len(logs.snapshot())
	send(t, connA, "CHECK_JOB", a, newMap(entry("session", decimal(b))))
	checkJob(t, rA, nil)
	if lines := logs.snapshot()[before:]; len(lines) != 1 || !strings.HasPrefix(lines[0], "[CHECK_JOB] session=") {
		t.Fatal(lines)
	}
	if s.remoteControlTable().count(b) != 1 {
		t.Fatal("foreign CHECK_JOB consumed the job")
	}
	// The connection stays usable and the owner still receives the job.
	send(t, connA, "SERVER_TIME", a, nil)
	if packs := readPacks(t, rA); len(packs) != 1 {
		t.Fatal(packs)
	}
	send(t, connB, "CHECK_JOB", b, newMap(entry("session", decimal(b))))
	checkJob(t, rB, param)
}

// count reports queued commands for a session.
func (t *remoteControlTable) count(session int64) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.queues[session])
}
