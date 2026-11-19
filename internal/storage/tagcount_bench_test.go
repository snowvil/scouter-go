package storage

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// tagBenchXLogs is a replay-like XLog mix (cmd/scouter-replay generator with
// --text-cardinality 2000): 200 objects, 2000 services, 1000 user agents and
// referers, SQL on most and errors on 1% of XLogs, ending at rate per second
// from day+10h. About nine tag contributions per XLog.
func tagBenchXLogs(n, rate int) []*protocol.XLogPack {
	rng := rand.New(rand.NewSource(1))
	base := tagFlushTestDay.Add(10 * time.Hour).UnixMilli()
	out := make([]*protocol.XLogPack, n)
	for i := range out {
		x := &protocol.XLogPack{
			EndTime: base + int64(i)*1000/int64(rate), ObjHash: int32(1000 + rng.Intn(200)), Service: int32(5000 + rng.Intn(2000)),
			Elapsed: 1 + int32(rng.ExpFloat64()*80), SQLCount: int32(rng.Intn(10)), KBytes: int32(rng.Intn(64)),
			UserAgent: int32(9000 + rng.Intn(1000)), Referer: int32(19000 + rng.Intn(1000)),
		}
		x.SQLTime = x.SQLCount * int32(rng.Intn(5))
		if rng.Float64() < 0.01 {
			x.Error = int32(30000 + rng.Intn(1000))
		}
		out[i] = x
	}
	return out
}

func openTagBench(b *testing.B) *Store {
	b.Helper()
	s, err := Open(Config{Directory: b.TempDir(), Location: time.UTC})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

// tagBenchEntries prepares the inbox entries of xs.
func tagBenchEntries(b *testing.B, s *Store, xs []*protocol.XLogPack) []tagInboxEntry {
	b.Helper()
	out := make([]tagInboxEntry, len(xs))
	for i, x := range xs {
		w, err := s.PrepareXLogTags("java", x)
		if err != nil {
			b.Fatal(err)
		}
		out[i] = w.tag
	}
	return out
}

// tagBenchFlushWindow is one 5s flush window at 30,000 XLog/s.
const tagBenchFlushWindow = 150000

// BenchmarkTagPrepareXLog is the engine-lock part: one XLog's contributions.
func BenchmarkTagPrepareXLog(b *testing.B) {
	s := openTagBench(b)
	xs := tagBenchXLogs(4096, 30000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.PrepareXLogTags("java", xs[i%len(xs)]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTagApplyXLog applies one XLog's inbox entry to the pending tables
// (the drain), with a flush every 5s window outside the timer, so deltas are
// made again after each flush as in the server.
func BenchmarkTagApplyXLog(b *testing.B) {
	s := openTagBench(b)
	entries := tagBenchEntries(b, s, tagBenchXLogs(tagBenchFlushWindow, 30000))
	ctx := context.Background()
	lockTags(s)
	defer s.tags.mu.Unlock()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(entries)
		if j == 0 && i > 0 {
			b.StopTimer()
			if err := s.tagFlushLocked(ctx); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
		if err := s.tagApplyLocked(ctx, &entries[j]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTagFlushWindow is the runtime flush pass (tagFlushPass, without
// the rest between holds) of one 5s window's pending records into files that
// already hold them (the steady state: in-place updates). The apply is
// outside the timer; one op is one window.
func BenchmarkTagFlushWindow(b *testing.B) {
	s := openTagBench(b)
	s.tagNoRest = true
	entries := tagBenchEntries(b, s, tagBenchXLogs(tagBenchFlushWindow, 30000))
	ctx := context.Background()
	apply := func() {
		lockTags(s)
		defer s.tags.mu.Unlock()
		for i := range entries {
			if err := s.tagApplyLocked(ctx, &entries[i]); err != nil {
				b.Fatal(err)
			}
		}
	}
	apply()
	if err := s.flushTagCounts(ctx); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		apply()
		b.StartTimer()
		if err := s.flushTagCounts(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTagIngestXLog is one XLog's tags from ingest to the pending
// tables: Prepare and Offer as the engine does, and the drain of the inbox
// each time it holds 8192 entries (in the timer), with a flush every 5s
// window outside it.
func BenchmarkTagIngestXLog(b *testing.B) {
	s := openTagBench(b)
	xs := tagBenchXLogs(tagBenchFlushWindow, 30000)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(xs)
		if j == 0 && i > 0 {
			b.StopTimer()
			lockTags(s)
			err := s.tagFlushLocked(ctx)
			s.tags.mu.Unlock()
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
		w, err := s.PrepareXLogTags("java", xs[j])
		if err != nil {
			b.Fatal(err)
		}
		if done, err := s.Offer(w); !done || err != nil {
			b.Fatal(done, err)
		}
		if i%8192 == 8191 {
			lockTags(s)
			s.tags.mu.Unlock()
		}
	}
}
