package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

// The Pulse callbacks mirror Java XLogCore/ProfileCore: PlugInManager.xlog runs
// before XLogWR.add and PlugInManager.profile before XLogProfileWR.add, without
// waiting for the write result.
func TestPulseHooksRunBeforeStoreWrite(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	root := t.TempDir()
	s, err := storage.Open(storage.Config{Directory: root, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	c, err := storage.OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	var xlogs, profiles []int64
	var storedAtHook []bool
	cfg := Config{Location: time.UTC, Clock: func() time.Time { return now }, XLogCapacity: 8}
	cfg.OnXLog = func(p *protocol.XLogPack) {
		xlogs = append(xlogs, p.TxID)
		stored, err := s.XLog("20261013", p.TxID)
		storedAtHook = append(storedAtHook, err == nil && stored != nil)
	}
	cfg.OnProfile = func(p *protocol.ProfilePack) {
		profiles = append(profiles, p.TxID)
		b, err := s.Profile("20261013", p.TxID)
		storedAtHook = append(storedAtHook, err == nil && len(b) > 0)
	}
	e, err := New(cfg, s, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close(); c.Close(); s.Close() })
	ctx := context.Background()
	ms := now.UnixMilli()
	if err := e.Accept(ctx, &protocol.XLogPack{TxID: 1, GxID: 1, Service: 1, XType: 2, DiscardType: 1, TailVersion: 12, EndTime: ms}); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, &protocol.ProfilePack{TxID: 1, Profile: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, &protocol.ProfilePack{TxID: 2}); err != nil {
		t.Fatal(err)
	}
	// A delayed child profile is released by its driving XLog through the same
	// write helper, so the hook also sees released and recovered profiles.
	child := &protocol.ProfilePack{Version2: true, TxID: 11, GxID: 10, XType: 4, DiscardType: 1, Profile: []byte{2}}
	if err := e.Accept(ctx, child); err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 1 {
		t.Fatal("delayed child must not reach the hook before release", profiles)
	}
	if err := e.Accept(ctx, &protocol.XLogPack{TxID: 10, GxID: 10, Service: 1, XType: 2, DiscardType: 1, TailVersion: 12, EndTime: ms}); err != nil {
		t.Fatal(err)
	}
	if len(xlogs) != 2 || xlogs[0] != 1 || xlogs[1] != 10 || len(profiles) != 2 || profiles[0] != 1 || profiles[1] != 11 {
		t.Fatalf("xlogs=%v profiles=%v", xlogs, profiles)
	}
	for i, stored := range storedAtHook {
		if stored {
			t.Fatalf("hook %d ran after the store write", i)
		}
	}
	// The hook runs even when the store write then fails.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, &protocol.XLogPack{TxID: 3, GxID: 3, Service: 1, XType: 2, DiscardType: 1, TailVersion: 12, EndTime: ms}); err == nil {
		t.Fatal("closed store must fail the write")
	}
	if len(xlogs) != 3 || xlogs[2] != 3 {
		t.Fatal("hook skipped on store failure", xlogs)
	}
}
