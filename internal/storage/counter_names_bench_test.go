package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// BenchmarkCounterPutRealtimeKnownNames is one realtime PerfCounterPack of 20
// counters whose names are already in a dictionary of the given size (the
// steady state after the first packs of a day; R6).
func BenchmarkCounterPutRealtimeKnownNames(b *testing.B) {
	for _, size := range []int{300, 4000} {
		b.Run(fmt.Sprint("names=", size), func(b *testing.B) {
			c, err := OpenCounters(b.TempDir(), time.UTC)
			if err != nil {
				b.Fatal(err)
			}
			defer c.Close()
			start := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC).UnixMilli()
			fill := &protocol.PerfCounterPack{Time: start, ObjName: "/bench", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap}}
			for i := 0; i < size; i++ {
				fill.Data.Entries = append(fill.Data.Entries, protocol.Entry{Key: fmt.Sprintf("counter-%05d", i), Value: protocol.Value{Type: protocol.ValueDecimal, Int: int64(i)}})
				if len(fill.Data.Entries) == 1000 || i == size-1 {
					if err = c.Put(fill); err != nil {
						b.Fatal(err)
					}
					fill.Data.Entries = fill.Data.Entries[:0]
				}
			}
			p := &protocol.PerfCounterPack{ObjName: "/bench", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap}}
			for i := 0; i < 20; i++ {
				p.Data.Entries = append(p.Data.Entries, protocol.Entry{Key: fmt.Sprintf("counter-%05d", i*size/20), Value: protocol.Value{Type: protocol.ValueFloat, Float: float32(i)}})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p.Time = start + 1 + int64(i%80000000)
				if err = c.Put(p); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCounterPutRealtimeNewName is a realtime pack of 20 known counters
// and one new name over a dictionary of the given size: the dictionary save
// (in place: entries write+fsync, length write+fsync) on the Counters
// gate. Run with a small -benchtime (each op adds a name).
func BenchmarkCounterPutRealtimeNewName(b *testing.B) {
	const size = 3000
	c, err := OpenCounters(b.TempDir(), time.UTC)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	start := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC).UnixMilli()
	fill := &protocol.PerfCounterPack{Time: start, ObjName: "/bench", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap}}
	for i := 0; i < size; i++ {
		fill.Data.Entries = append(fill.Data.Entries, protocol.Entry{Key: fmt.Sprintf("counter-%05d", i), Value: protocol.Value{Type: protocol.ValueDecimal, Int: int64(i)}})
		if len(fill.Data.Entries) == 1000 || i == size-1 {
			if err = c.Put(fill); err != nil {
				b.Fatal(err)
			}
			fill.Data.Entries = fill.Data.Entries[:0]
		}
	}
	p := &protocol.PerfCounterPack{ObjName: "/bench", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap}}
	for i := 0; i < 20; i++ {
		p.Data.Entries = append(p.Data.Entries, protocol.Entry{Key: fmt.Sprintf("counter-%05d", i*size/20), Value: protocol.Value{Type: protocol.ValueFloat, Float: float32(i)}})
	}
	p.Data.Entries = append(p.Data.Entries, protocol.Entry{Value: protocol.Value{Type: protocol.ValueFloat}})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Time = start + 1 + int64(i)
		p.Data.Entries[20].Key = fmt.Sprintf("new-%07d", i)
		if err = c.Put(p); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCounterRealtimeDayRollover is the first packs of a day: 100
// objects, each with 40 counters shared by all and 2 of its own, so the
// first pack creates the dictionary and each later one appends 2 names. One
// op is one day (a new realtime table, index included).
func BenchmarkCounterRealtimeDayRollover(b *testing.B) {
	c, err := OpenCounters(b.TempDir(), time.UTC)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	day := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p := &protocol.PerfCounterPack{TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := day.AddDate(0, 0, i).UnixMilli()
		for o := 0; o < 100; o++ {
			p.Time, p.ObjName = start+int64(o), fmt.Sprintf("/obj/%03d", o)
			p.Data.Entries = p.Data.Entries[:0]
			for k := 0; k < 40; k++ {
				p.Data.Entries = append(p.Data.Entries, protocol.Entry{Key: fmt.Sprintf("shared-%02d", k), Value: protocol.Value{Type: protocol.ValueFloat, Float: float32(k)}})
			}
			for k := 0; k < 2; k++ {
				p.Data.Entries = append(p.Data.Entries, protocol.Entry{Key: fmt.Sprintf("own-%03d-%d", o, k), Value: protocol.Value{Type: protocol.ValueFloat}})
			}
			if err = c.Put(p); err != nil {
				b.Fatal(err)
			}
		}
	}
}
