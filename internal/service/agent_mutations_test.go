package service

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

func TestAgentMutationRelayBodiesAndResponses(t *testing.T) {
	for _, cmd := range AgentMutationCommands() {
		t.Run(cmd, func(t *testing.T) {
			logs := captureManageLog(t)
			b := &agentReadFake{fakeBackend: &fakeBackend{}, replies: []protocol.Pack{newMap(entry("ignored", decimal(0))), newMap(entry("answer", decimal(1)))}}
			_, conn, r := startService(t, queryConfig(), b)
			session := loginSession(t, conn, r)
			p := newMap(entry("objHash", decimal(42)), entry("opaque", protocol.Value{Type: protocol.ValueBlob, Bytes: []byte{0, 255}}), entry("time", decimal(30000)))
			send(t, conn, cmd, session, p)
			packs := readPacks(t, r)
			if agentMutationSilent(cmd) {
				if len(packs) != 0 {
					t.Fatal("silent handler answered", packs)
				}
			} else if len(packs) != 1 || mapValue(packs[0].(*protocol.MapPack), "answer").Int != 1 {
				t.Fatal(packs)
			}
			calls := b.snapshot()
			if len(calls) != 1 || calls[0].hash != 42 || calls[0].command != cmd || !reflect.DeepEqual(calls[0].param, p) {
				t.Fatal(calls, p)
			}
			lines := logs.snapshot()
			if len(lines) != 1 || !strings.HasPrefix(lines[0], "["+cmd+"] objHash=42 ") || !strings.Contains(lines[0], fmt.Sprintf("session=%d", session)) {
				t.Fatal(lines)
			}
			// The connection stays usable after both response shapes.
			send(t, conn, "SERVER_TIME", session, nil)
			if len(readPacks(t, r)) != 1 {
				t.Fatal("stream desynchronized")
			}
		})
	}
}

func TestAgentMutationOfflineNonMapAndFailures(t *testing.T) {
	for _, cmd := range []string{"OBJECT_CALL_HEAP_DUMP", "OBJECT_SYSTEM_GC"} {
		for _, tc := range []struct {
			name    string
			replies []protocol.Pack
		}{{"offline", nil}, {"last nonmap", []protocol.Pack{newMap(entry("answer", decimal(1))), &protocol.TextPack{Text: "synthetic"}}}} {
			t.Run(cmd+"/"+tc.name, func(t *testing.T) {
				captureManageLog(t)
				b := &agentReadFake{fakeBackend: &fakeBackend{}, replies: tc.replies}
				_, conn, r := startService(t, queryConfig(), b)
				session := loginSession(t, conn, r)
				send(t, conn, cmd, session, newMap(entry("objHash", decimal(42))))
				if packs := readPacks(t, r); len(packs) != 0 {
					t.Fatal(packs)
				}
				if len(b.snapshot()) != 1 {
					t.Fatal("not relayed")
				}
			})
		}
		// Java AgentCall returns null after an agent failure, so the
		// handler answers no item (TestAgentReadSingleTargetFailureIsJavaNull).
		t.Run(cmd+"/agent failure", func(t *testing.T) {
			captureManageLog(t)
			b := &agentReadFake{fakeBackend: &fakeBackend{}, err: errors.New("synthetic failure")}
			_, conn, r := startService(t, queryConfig(), b)
			session := loginSession(t, conn, r)
			send(t, conn, cmd, session, newMap(entry("objHash", decimal(42))))
			if packs := readPacks(t, r); len(packs) != 0 {
				t.Fatal(packs)
			}
		})
		t.Run(cmd+"/relay limit", func(t *testing.T) {
			captureManageLog(t)
			b := &agentReadFake{fakeBackend: &fakeBackend{}, err: transport.ErrLimit}
			_, conn, r := startService(t, queryConfig(), b)
			session := loginSession(t, conn, r)
			send(t, conn, cmd, session, newMap(entry("objHash", decimal(42))))
			expectClosed(t, r)
		})
		t.Run(cmd+"/no agent backend", func(t *testing.T) {
			captureManageLog(t)
			_, conn, r := startService(t, queryConfig(), &fakeBackend{})
			session := loginSession(t, conn, r)
			send(t, conn, cmd, session, newMap(entry("objHash", decimal(42))))
			expectClosed(t, r)
		})
		t.Run(cmd+"/malformed", func(t *testing.T) {
			captureManageLog(t)
			for _, body := range []protocol.Pack{&protocol.TextPack{}, newMap(entry("objHash", decimal(1<<40)))} {
				b := &agentReadFake{fakeBackend: &fakeBackend{}}
				_, conn, r := startService(t, queryConfig(), b)
				session := loginSession(t, conn, r)
				send(t, conn, cmd, session, body)
				expectClosed(t, r)
				if len(b.snapshot()) != 0 {
					t.Fatal("invalid request relayed")
				}
			}
		})
		t.Run(cmd+"/response limit", func(t *testing.T) {
			captureManageLog(t)
			b := &agentReadFake{fakeBackend: &fakeBackend{}, replies: make([]protocol.Pack, manageMaxItems+1)}
			_, conn, r := startService(t, queryConfig(), b)
			session := loginSession(t, conn, r)
			send(t, conn, cmd, session, newMap(entry("objHash", decimal(42))))
			expectClosed(t, r)
		})
	}
}
