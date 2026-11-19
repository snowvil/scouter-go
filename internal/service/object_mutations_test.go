package service

import (
	"errors"
	"reflect"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func objectRemoveFixture() *manageFake {
	b := newManageFake()
	b.objects = []*protocol.ObjectPack{{ObjHash: 7, ObjType: "java", ObjName: "/host/a", Alive: true, Tags: protocol.Value{Type: protocol.ValueMap}}, {ObjHash: 9, ObjType: "java", ObjName: "/host/b", Alive: true, Tags: protocol.Value{Type: protocol.ValueMap}}}
	return b
}

func TestObjectRemoveListBodyAndFullListResponse(t *testing.T) {
	for _, tc := range []struct {
		command   string
		permanent bool
	}{{"OBJECT_REMOVE", true}, {"OBJECT_REMOVE_IN_MEMORY", false}} {
		t.Run(tc.command, func(t *testing.T) {
			captureManageLog(t)
			b := objectRemoveFixture()
			_, conn, r := startService(t, queryConfig(), b)
			session := loginSession(t, conn, r)
			// AgentInfo.scala:160/198: the body is a bare ListValue, not a MapPack.
			e := protocol.NewEncoder(protocol.Limits{})
			e.Text(tc.command)
			e.Int64(session)
			e.Value(list([]protocol.Value{decimal(7), decimal(-2147483648), decimal(2147483647)}))
			raw, err := e.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = conn.Write(raw); err != nil {
				t.Fatal(err)
			}
			packs := readPacks(t, r)
			// getFullAgentList: today's stored objects with status tags; the
			// fake has no current object so every pack is dead/alive=false.
			if len(packs) != 2 {
				t.Fatal(packs)
			}
			for i, hash := range []int32{7, 9} {
				o, ok := packs[i].(*protocol.ObjectPack)
				if !ok || o.ObjHash != hash || o.Alive || lookup(&protocol.MapPack{Entries: o.Tags.Entries}, "status").Text != "dead" {
					t.Fatal(packs[i])
				}
			}
			b.mmu.Lock()
			removed, permanent := b.removed, b.permanent
			b.mmu.Unlock()
			if !reflect.DeepEqual(removed, [][]int32{{7, -2147483648, 2147483647}}) || !reflect.DeepEqual(permanent, []bool{tc.permanent}) {
				t.Fatal(removed, permanent)
			}
			if !reflect.DeepEqual(b.fakeBackend.calls, []string{"objects:20261013", "object:", "object:"}) {
				t.Fatal(b.fakeBackend.calls)
			}
		})
	}
}

func TestObjectRemoveEmptyListAndInactive(t *testing.T) {
	captureManageLog(t)
	b := objectRemoveFixture()
	_, conn, r := startService(t, queryConfig(), b)
	session := loginSession(t, conn, r)
	e := protocol.NewEncoder(protocol.Limits{})
	e.Text("OBJECT_REMOVE")
	e.Int64(session)
	e.Value(list(nil))
	raw, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Write(raw); err != nil {
		t.Fatal(err)
	}
	if packs := readPacks(t, r); len(packs) != 2 {
		t.Fatal(packs)
	}
	// OBJECT_REMOVE_INACTIVE has no body and no items; the next command must
	// not be consumed as its body.
	send(t, conn, "OBJECT_REMOVE_INACTIVE", session, nil)
	if packs := readPacks(t, r); len(packs) != 0 {
		t.Fatal(packs)
	}
	send(t, conn, "SERVER_TIME", session, nil)
	if packs := readPacks(t, r); len(packs) != 1 {
		t.Fatal(packs)
	}
	b.mmu.Lock()
	defer b.mmu.Unlock()
	if len(b.removed) != 0 || b.inactive != 1 {
		t.Fatal(b.removed, b.inactive)
	}
}

func TestObjectRemoveFailures(t *testing.T) {
	captureManageLog(t)
	for _, tc := range []struct {
		name string
		body protocol.Value
	}{
		{"text element", list([]protocol.Value{stringValue("7")})},
		{"float element", list([]protocol.Value{{Type: protocol.ValueFloat, Float: 7}})},
		{"out of int32", list([]protocol.Value{decimal(1 << 40)})},
		{"not a list", decimal(7)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := objectRemoveFixture()
			_, conn, r := startService(t, queryConfig(), b)
			session := loginSession(t, conn, r)
			e := protocol.NewEncoder(protocol.Limits{})
			e.Text("OBJECT_REMOVE")
			e.Int64(session)
			e.Value(tc.body)
			raw, err := e.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = conn.Write(raw); err != nil {
				t.Fatal(err)
			}
			expectClosed(t, r)
			b.mmu.Lock()
			defer b.mmu.Unlock()
			if len(b.removed) != 0 {
				t.Fatal("invalid request removed objects", b.removed)
			}
		})
	}
	t.Run("mappack body", func(t *testing.T) {
		b := objectRemoveFixture()
		_, conn, r := startService(t, queryConfig(), b)
		session := loginSession(t, conn, r)
		send(t, conn, "OBJECT_REMOVE_IN_MEMORY", session, newMap(entry("objHash", list([]protocol.Value{decimal(7)}))))
		expectClosed(t, r)
	})
	t.Run("remover error", func(t *testing.T) {
		b := objectRemoveFixture()
		b.removeErr = errors.New("synthetic failure")
		_, conn, r := startService(t, queryConfig(), b)
		session := loginSession(t, conn, r)
		send(t, conn, "OBJECT_REMOVE_INACTIVE", session, nil)
		expectClosed(t, r)
	})
	t.Run("no remover", func(t *testing.T) {
		_, conn, r := startService(t, queryConfig(), &agentReadFake{fakeBackend: &fakeBackend{}})
		session := loginSession(t, conn, r)
		send(t, conn, "OBJECT_REMOVE_INACTIVE", session, nil)
		expectClosed(t, r)
	})
	t.Run("list limit", func(t *testing.T) {
		b := objectRemoveFixture()
		_, conn, r := startService(t, queryConfig(), b)
		session := loginSession(t, conn, r)
		values := make([]protocol.Value, manageMaxItems+1)
		for i := range values {
			values[i] = decimal(7)
		}
		// The default codec limit would refuse to encode it; the server must
		// reject the oversized list before any removal.
		e := protocol.NewEncoder(protocol.Limits{MaxCollectionEntries: 2 * manageMaxItems})
		e.Text("OBJECT_REMOVE")
		e.Int64(session)
		e.Value(list(values))
		raw, err := e.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = conn.Write(raw); err != nil {
			t.Fatal(err)
		}
		expectAborted(t, r)
		b.mmu.Lock()
		defer b.mmu.Unlock()
		if len(b.removed) != 0 {
			t.Fatal(b.removed)
		}
	})
}
