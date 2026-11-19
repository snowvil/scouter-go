package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

// runtimePair opens Store and Counters on one root and attaches them, as the
// server runtime does.
func runtimePair(t *testing.T, dir string) (*Store, *Counters) {
	t.Helper()
	s := cleanStore(t, dir, nil)
	c, err := OpenCounters(dir, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.AttachCounters(c); err != nil {
		t.Fatal(err)
	}
	return s, c
}

// closeRuntimePair closes in the runtime order: Counters, then Store.
func closeRuntimePair(t *testing.T, s *Store, c *Counters) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func putCounters(t *testing.T, c *Counters, from, to int64) {
	t.Helper()
	for n := from; n <= to; n++ {
		for _, tt := range []byte{1, 3} { // realtime (real.*) and 5-minute (5m.*)
			p := &protocol.PerfCounterPack{Time: cleanDay + n*2000, ObjName: "/host/app", TimeType: tt,
				Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueDecimal, Int: n}}}}}
			if err := c.Put(p); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func putKV(t *testing.T, s *Store, key string) {
	t.Helper()
	if _, err := s.PutKeyValueContext(context.Background(), "space", key, "v-"+key, -1, 1); err != nil {
		t.Fatal(err)
	}
}

// After a clean shutdown the first counter (real and 5m) and KV writes of the
// next run must not walk their index pairs either.
func TestCleanRestartSkipsCounterAndKVValidation(t *testing.T) {
	dir := t.TempDir()
	s, c := runtimePair(t, dir)
	putCounters(t, c, 1, 50)
	putKV(t, s, "a")
	closeRuntimePair(t, s, c)

	s, c = runtimePair(t, dir)
	defer closeRuntimePair(t, s, c)
	putCounters(t, c, 51, 60)
	putKV(t, s, "b")
	if st := c.IndexOpenStats(); st.Walked != 0 || st.Sealed != 2 {
		t.Fatalf("counters after clean restart: %+v", st)
	}
	if st := s.IndexOpenStats(); st.Walked != 0 || st.Sealed != 1 {
		t.Fatalf("KV after clean restart: %+v", st)
	}
	points, err := c.Realtime("20261013", CounterHash("/host/app"), "TPS", cleanDay, cleanDay+200000)
	if err != nil || len(points) != 60 {
		t.Fatal(len(points), err)
	}
	for _, k := range []string{"a", "b"} {
		if v, ok, err := s.KeyValueContext(context.Background(), "space", k, 1); err != nil || !ok || v != "v-"+k {
			t.Fatal(k, v, ok, err)
		}
	}
}

// Crash and change paths never use a seal: a pair changed by another writer
// is fully validated, a crashed pair only after its last checkpoint.
func TestCounterAndKVCrashPathsAreFullyValidated(t *testing.T) {
	dir := t.TempDir()
	s, c := runtimePair(t, dir)
	putCounters(t, c, 1, 5)
	putKV(t, s, "a")
	closeRuntimePair(t, s, c)

	// Marker present, but another writer appended to the realtime pair.
	other, err := legacyindex.Open(filepath.Join(dir, "20261013", "counter", "real"), 0, true, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = other.Put(longKey(cleanDay+99_000), fiveBytes(0)); err != nil {
		t.Fatal(err)
	}
	if err = other.Close(); err != nil {
		t.Fatal(err)
	}
	s, c = runtimePair(t, dir)
	putCounters(t, c, 6, 6)
	if st := c.IndexOpenStats(); st.Walked != 1 || st.Sealed != 1 {
		t.Fatalf("changed counter pair: %+v", st)
	}

	// Crash: no Close at all, then a fresh runtime. Nothing is sealed.
	putKV(t, s, "b")
	s2, c2 := runtimePair(t, dir)
	defer closeRuntimePair(t, s2, c2)
	putCounters(t, c2, 7, 7)
	putKV(t, s2, "c")
	// real: its header was rewritten by the other writer after the last
	// checkpoint (full walk); 5m and KV: header still the checkpoint's.
	if st := c2.IndexOpenStats(); st.Walked != 1 || st.Checkpointed != 1 || st.CheckpointRejected != 1 || st.Sealed != 0 {
		t.Fatalf("counters after crash: %+v", st)
	}
	if st := s2.IndexOpenStats(); st.Walked != 0 || st.Checkpointed != 1 || st.WalkedRecords != 1 || st.Sealed != 0 || st.CleanMarker {
		t.Fatalf("KV after crash: %+v", st)
	}
}
