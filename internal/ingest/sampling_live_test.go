package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func delayedXLog(txid, gxid int64) *protocol.XLogPack {
	return &protocol.XLogPack{TxID: txid, GxID: gxid, Service: 1, ObjHash: 1, Elapsed: 1, XType: 2, DiscardType: discardAll, TailVersion: protocol.XLogCurrentTail}
}

func delayedProfile(txid, gxid int64) *protocol.ProfilePack {
	return &protocol.ProfilePack{TxID: txid, GxID: gxid, ObjHash: 1, Profile: []byte{1}, Version2: true, XType: 2, DiscardType: discardAll}
}

// xlog_sampling_matcher_*: gxid count at startup, bucket counts when a bucket
// is created, millis/secs when the next delayed pack arrives.
func TestSamplingSettingsApplyAtJavaPoints(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, func(c *Config) { c.SamplingGxidKeepCount = 3 })
	ctx := context.Background()
	for i := int64(1); i <= 4; i++ {
		x := &protocol.XLogPack{TxID: i, Service: 1, ObjHash: 1, Elapsed: 1, DiscardType: discardNone, TailVersion: protocol.XLogCurrentTail}
		if err := e.Accept(ctx, x); err != nil {
			t.Fatal(err)
		}
	}
	if n := e.extra.sampling.processed.len(); n != 3 {
		t.Fatalf("processed gxid bound from startup: %d", n)
	}
	// The current bucket keeps its creation bound; a new one takes the setting.
	s := DefaultSettings()
	s.XLogKeepCount = 2
	s.XLogKeepMillis = 1000
	e.SetSettings(s)
	for g := int64(100); g < 103; g++ {
		if err := e.Accept(ctx, delayedXLog(g+1000, g)); err != nil {
			t.Fatal(err)
		}
	}
	// now/1000 differs from the startup now/5000 unit: the first delayed pack rolled.
	state := e.extra.sampling
	if state.xlogUnit != now.UnixMilli()/1000 || state.xlogCurrent.maximum != 2 || len(state.xlogCurrent.entries) != 2 {
		t.Fatalf("unit %d max %d entries %d", state.xlogUnit, state.xlogCurrent.maximum, len(state.xlogCurrent.entries))
	}
	// Profile buckets: secs 1 keeps 2 old buckets.
	s.ProfileKeepSecs = 1
	s.ProfileKeepCount = 7
	e.SetSettings(s)
	for i := 0; i < 5; i++ {
		now = now.Add(600 * time.Millisecond)
		if err := e.Accept(ctx, delayedProfile(int64(200+i), int64(300+i))); err != nil {
			t.Fatal(err)
		}
	}
	if len(state.profileOld) != 2 || state.profileCurrent.maximum != 7 {
		t.Fatalf("profile old %d max %d", len(state.profileOld), state.profileCurrent.maximum)
	}
}

func TestSettingsNormalization(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, nil)
	e.SetSettings(Settings{XLogKeepCount: -1, XLogKeepMillis: 0, ProfileKeepCount: 0, ProfileKeepSecs: -3})
	got := e.Settings()
	if got.XLogKeepCount != defaultXLogKeepCount || got.XLogKeepMillis != defaultXLogKeepMillis || got.ProfileKeepCount != defaultProfileKeepCount || got.ProfileKeepSecs != defaultProfileKeepSecs {
		t.Fatalf("normalized %+v", got)
	}
	big := Settings{ProfileKeepSecs: 1 << 30}
	if n := big.profileBuckets(); n != 10 {
		t.Fatalf("overflowing secs*1000 falls back to 10 buckets, got %d", n)
	}
}
