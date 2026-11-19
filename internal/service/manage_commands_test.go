package service

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// manageFake adds the optional management capabilities to the Agent relay fake.
type manageFake struct {
	*agentReadFake
	mmu       sync.Mutex
	removed   [][]int32
	permanent []bool
	inactive  int
	deleted   [][]string
	removeErr error
	deleteErr error
}

func newManageFake() *manageFake {
	return &manageFake{agentReadFake: &agentReadFake{fakeBackend: &fakeBackend{}}}
}
func (b *manageFake) RemoveObjects(_ context.Context, hashes []int32, permanent bool) error {
	b.mmu.Lock()
	defer b.mmu.Unlock()
	b.removed = append(b.removed, append([]int32(nil), hashes...))
	b.permanent = append(b.permanent, permanent)
	return b.removeErr
}
func (b *manageFake) RemoveInactiveObjects(context.Context) error {
	b.mmu.Lock()
	defer b.mmu.Unlock()
	b.inactive++
	return b.removeErr
}
func (b *manageFake) DeleteDatabasePaths(_ context.Context, paths []string) error {
	b.mmu.Lock()
	defer b.mmu.Unlock()
	b.deleted = append(b.deleted, append([]string(nil), paths...))
	return b.deleteErr
}
func (b *manageFake) deletions() [][]string {
	b.mmu.Lock()
	defer b.mmu.Unlock()
	return append([][]string(nil), b.deleted...)
}

// manageLogs captures manageLogf for one test.
type manageLogs struct {
	mu    sync.Mutex
	lines []string
}

func captureManageLog(t *testing.T) *manageLogs {
	t.Helper()
	logs := &manageLogs{}
	previous := manageLogf
	manageLogf = func(format string, args ...any) {
		logs.mu.Lock()
		defer logs.mu.Unlock()
		logs.lines = append(logs.lines, fmt.Sprintf(format, args...))
	}
	t.Cleanup(func() { manageLogf = previous })
	return logs
}
func (l *manageLogs) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

// secondClient opens another Client connection to the server behind conn.
func secondClient(t *testing.T, conn net.Conn) (net.Conn, *bufio.Reader) {
	t.Helper()
	other, err := net.Dial("tcp", conn.RemoteAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	other.SetDeadline(time.Now().Add(2 * time.Second))
	var magic [4]byte
	binary.BigEndian.PutUint32(magic[:], transport.TCPClient)
	if _, err := other.Write(magic[:]); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { other.Close() })
	return other, bufio.NewReader(other)
}

// expectClosed expects EOF (the account tests keep their own helper that
// also accepts a reset).
func expectClosed(t *testing.T, r *bufio.Reader) {
	t.Helper()
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatal("expected closed connection", err)
	}
}

// expectAborted accepts EOF or a reset: a server that rejects a request while
// unread body bytes remain closes with RST.
func expectAborted(t *testing.T, r *bufio.Reader) {
	t.Helper()
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatal("expected closed connection", err)
	}
}

func TestManageCommandsScopeAndRecognition(t *testing.T) {
	data, err := os.ReadFile("../../docs/compatibility/query-scope.json")
	if err != nil {
		t.Fatal(err)
	}
	var scope struct {
		Commands []struct {
			Literal  string `json:"command_literal"`
			Category string `json:"category"`
		}
	}
	if err = json.Unmarshal(data, &scope); err != nil {
		t.Fatal(err)
	}
	category := map[string]string{}
	for _, c := range scope.Commands {
		category[c.Literal] = c.Category
	}
	commands := ManageCommands()
	seen := map[string]bool{}
	for _, command := range commands {
		if category[command] != "mutation" || seen[command] {
			t.Fatal("not a unique mutation", command)
		}
		seen[command] = true
	}
	if len(commands) != 28 || len(AgentMutationCommands()) != 21 {
		t.Fatal(len(commands), len(AgentMutationCommands()))
	}
	commands[0] = "changed"
	if ManageCommands()[0] == "changed" {
		t.Fatal("borrowed command list")
	}
	for _, command := range AgentMutationCommands() {
		for _, read := range AgentReadCommands() {
			if read == command {
				t.Fatal("mutation listed as read relay", command)
			}
		}
	}
	s := &Service{}
	if ok, err := s.HandleManageCommand(context.Background(), transport.ClientRequest{Command: "HOST_TOP", Authenticated: true}, nil); ok || err != nil {
		t.Fatal(ok, err)
	}
	if ok, err := s.HandleManageCommand(context.Background(), transport.ClientRequest{Command: "OBJECT_SYSTEM_GC"}, nil); !ok || !errors.Is(err, transport.ErrUnauthorized) {
		t.Fatal(ok, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ok, err := s.HandleManageCommand(ctx, transport.ClientRequest{Command: "CHECK_JOB", Authenticated: true}, nil); !ok || !errors.Is(err, context.Canceled) {
		t.Fatal(ok, err)
	}
}

func TestServerDBDeletePathsAndResponse(t *testing.T) {
	logs := captureManageLog(t)
	b := newManageFake()
	_, conn, r := startService(t, queryConfig(), b)
	session := loginSession(t, conn, r)
	send(t, conn, "SERVER_DB_DELETE", session, newMap(entry("file", list([]protocol.Value{stringValue("/20261013"), stringValue("/20261013/xlog/"), stringValue("/./20261012/../20261011")}))))
	packs := readPacks(t, r)
	if len(packs) != 1 || mapValue(packs[0].(*protocol.MapPack), "size").Int != 3 {
		t.Fatal(packs)
	}
	if got := b.deletions(); !reflect.DeepEqual(got, [][]string{{"20261013", "20261013/xlog", "20261011"}}) {
		t.Fatal(got)
	}
	if lines := logs.snapshot(); len(lines) != 1 || !strings.Contains(lines[0], "[SERVER_DB_DELETE] 3 path(s)") || !strings.Contains(lines[0], fmt.Sprintf("session=%d", session)) {
		t.Fatal(lines)
	}
	// ManageService.java:61-68: no file list answers an empty MapPack; an
	// empty list answers size 0 without touching the file system.
	send(t, conn, "SERVER_DB_DELETE", session, newMap())
	packs = readPacks(t, r)
	if len(packs) != 1 || len(packs[0].(*protocol.MapPack).Entries) != 0 {
		t.Fatal(packs)
	}
	send(t, conn, "SERVER_DB_DELETE", session, newMap(entry("file", list(nil))))
	packs = readPacks(t, r)
	if len(packs) != 1 || mapValue(packs[0].(*protocol.MapPack), "size").Int != 0 || len(b.deletions()) != 1 {
		t.Fatal(packs, b.deletions())
	}
	for _, bad := range []protocol.Value{
		stringValue("20261013"), stringValue(""), stringValue("/"), stringValue("/."), stringValue("/.."), stringValue("/../etc"), stringValue("/20261013/../../etc"),
		stringValue("//etc"), stringValue("/lock.dat"), stringValue("/./lock.dat"), stringValue("/a\\b"), stringValue("/a\x00b"), decimal(20261013),
	} {
		t.Run(fmt.Sprintf("%q", bad.Text), func(t *testing.T) {
			b := newManageFake()
			_, conn, r := startService(t, queryConfig(), b)
			session := loginSession(t, conn, r)
			send(t, conn, "SERVER_DB_DELETE", session, newMap(entry("file", list([]protocol.Value{stringValue("/20261013"), bad}))))
			expectClosed(t, r)
			if len(b.deletions()) != 0 {
				t.Fatal("rejected request reached the deleter", b.deletions())
			}
		})
	}
	t.Run("non-list", func(t *testing.T) {
		_, conn, r := startService(t, queryConfig(), newManageFake())
		session := loginSession(t, conn, r)
		send(t, conn, "SERVER_DB_DELETE", session, newMap(entry("file", stringValue("/20261013"))))
		expectClosed(t, r)
	})
	t.Run("deleter error", func(t *testing.T) {
		b := newManageFake()
		b.deleteErr = errors.New("synthetic failure")
		_, conn, r := startService(t, queryConfig(), b)
		session := loginSession(t, conn, r)
		send(t, conn, "SERVER_DB_DELETE", session, newMap(entry("file", list([]protocol.Value{stringValue("/20261013")}))))
		expectClosed(t, r)
	})
	t.Run("no deleter", func(t *testing.T) {
		_, conn, r := startService(t, queryConfig(), &agentReadFake{fakeBackend: &fakeBackend{}})
		session := loginSession(t, conn, r)
		send(t, conn, "SERVER_DB_DELETE", session, newMap(entry("file", list(nil))))
		expectClosed(t, r)
	})
	t.Run("list limit", func(t *testing.T) {
		b := newManageFake()
		_, conn, r := startService(t, queryConfig(), b)
		session := loginSession(t, conn, r)
		values := make([]protocol.Value, manageMaxItems+1)
		for i := range values {
			values[i] = stringValue("/20261013")
		}
		// The default codec limit would refuse to encode it; the server must
		// reject the oversized list before any deletion.
		e := protocol.NewEncoder(protocol.Limits{MaxCollectionEntries: 2 * manageMaxItems})
		e.Text("SERVER_DB_DELETE")
		e.Int64(session)
		e.Pack(newMap(entry("file", list(values))))
		raw, err := e.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = conn.Write(raw); err != nil {
			t.Fatal(err)
		}
		expectAborted(t, r)
		if len(b.deletions()) != 0 {
			t.Fatal(b.deletions())
		}
	})
}

type metadataDeleterFake struct {
	MetadataBackend
	paths []string
}

func (m *metadataDeleterFake) DeleteDatabasePaths(_ context.Context, paths []string) error {
	m.paths = append([]string(nil), paths...)
	return nil
}

func TestServerDBDeleteMetadataOwnedDeleter(t *testing.T) {
	captureManageLog(t)
	config := queryConfig()
	deleter := &metadataDeleterFake{}
	config.Metadata = deleter
	_, conn, r := startService(t, config, &agentReadFake{fakeBackend: &fakeBackend{}})
	session := loginSession(t, conn, r)
	send(t, conn, "SERVER_DB_DELETE", session, newMap(entry("file", list([]protocol.Value{stringValue("/20261013")}))))
	packs := readPacks(t, r)
	if len(packs) != 1 || mapValue(packs[0].(*protocol.MapPack), "size").Int != 1 || !reflect.DeepEqual(deleter.paths, []string{"20261013"}) {
		t.Fatal(packs, deleter.paths)
	}
}

func TestDatabaseRelativePath(t *testing.T) {
	for name, want := range map[string]string{"/20261013": "20261013", "/20261013/": "20261013", "/a/./b": "a/b", "/a/b/../c": "a/c", "/.hidden": ".hidden", "/lock.dat/inner": "lock.dat/inner"} {
		if got, err := databaseRelativePath(name); err != nil || got != want {
			t.Fatal(name, got, err)
		}
	}
	for _, name := range []string{"", "20261013", "/", "//", "/.", "/..", "/../x", "/a/../..", "/a/../../b", "//etc/passwd", "/lock.dat", "/x/../lock.dat", "/a\\b", "/a\x00"} {
		if got, err := databaseRelativePath(name); !errors.Is(err, transport.ErrMalformed) {
			t.Fatal(name, got, err)
		}
	}
}
