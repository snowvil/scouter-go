package main

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"syscall"
	"time"
)

// bitset is a fixed-size, concurrency-safe set of transaction indexes.
type bitset struct{ words []atomic.Uint64 }

func newBitset(n uint64) *bitset { return &bitset{words: make([]atomic.Uint64, (n+63)/64)} }

// set reports whether the bit was already set.
func (b *bitset) set(i uint64) bool {
	mask := uint64(1) << (i % 64)
	return b.words[i/64].Or(mask)&mask != 0
}
func (b *bitset) has(i uint64) bool { return b.words[i/64].Load()&(uint64(1)<<(i%64)) != 0 }

type senderConfig struct {
	Index, Count      int
	Schedule          schedule
	HardStop          time.Time
	Tick              time.Duration
	HeartbeatInterval time.Duration
	CounterInterval   time.Duration
	MaxBytes          int
	Batch             int
	SocketBuffer      int
	Target            *net.UDPAddr
	Objects           []object // objects whose heartbeat/counters this sender emits
	Failed            *bitset  // tx whose XLog datagram failed to send
	Sent              *bitset  // tx whose XLog datagram(s) were all handed to the kernel
}

type senderResult struct {
	Lag             histogram
	Gen             *generator
	Datagrams       map[string]uint64
	DatagramBytes   uint64
	DatagramsOK     uint64
	MaxDatagram     int
	SendErrors      map[string]uint64
	TxSent          uint64
	TxFailed        uint64
	TxNotSent       uint64 // scheduled but abandoned at the hard stop
	SocketError     string
	ScheduledLast   time.Time
	FinishedAt      time.Time
	GenerateErrors  uint64
	GenerateLastErr string
}

func classifySendError(err error) string {
	switch {
	case errors.Is(err, syscall.ENOBUFS):
		return "ENOBUFS"
	case errors.Is(err, syscall.EMSGSIZE):
		return "EMSGSIZE"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "ECONNREFUSED"
	case errors.Is(err, syscall.EAGAIN):
		return "EAGAIN"
	default:
		return "other"
	}
}

// runSender owns its UDP socket, generator and batcher. It sends transactions
// i = Index, Index+Count, ... at their scheduled instants; when behind it sends
// immediately (no skipping) and lag records the delay. It returns when all of its
// scheduled transactions are sent, ctx is cancelled, or HardStop passes.
func runSender(ctx context.Context, cfg senderConfig, gen *generator) *senderResult {
	res := &senderResult{Gen: gen, Datagrams: map[string]uint64{}, SendErrors: map[string]uint64{}}
	defer func() { res.FinishedAt = time.Now() }()
	conn, err := net.DialUDP("udp", nil, cfg.Target)
	if err != nil {
		res.SocketError = err.Error()
		return res
	}
	defer conn.Close()
	if cfg.SocketBuffer > 0 {
		if err := conn.SetWriteBuffer(cfg.SocketBuffer); err != nil {
			res.SendErrors["set_write_buffer"]++
		}
	}
	sched := cfg.Schedule
	pkid := uint64(cfg.Index) << 48
	failedTx := map[int64]bool{} // only for the current datagram group; bounded by batch
	emit := func(d datagram) {
		_, err := conn.Write(d.Bytes)
		now := time.Now()
		res.Datagrams[d.Kind]++
		if err != nil {
			res.SendErrors[classifySendError(err)]++
			for _, tx := range d.Txs {
				failedTx[tx] = true
			}
		} else {
			res.DatagramsOK++
			res.DatagramBytes += uint64(len(d.Bytes))
			res.MaxDatagram = max(res.MaxDatagram, len(d.Bytes))
		}
		if !d.Final {
			return
		}
		for _, tx := range d.Txs {
			i := uint64(tx)
			if failedTx[tx] {
				cfg.Failed.set(i)
				res.TxFailed++
				delete(failedTx, tx)
				continue
			}
			cfg.Sent.set(i)
			res.TxSent++
			res.Lag.record(now.Sub(sched.at(i)).Nanoseconds())
		}
	}
	b := &batcher{maxBytes: cfg.MaxBytes, maxPacks: cfg.Batch, objHash: cfg.Objects0Hash(), emit: emit,
		nextPKID: func() int64 { pkid++; return int64(splitmix(pkid ^ gen.cfg.Seed)) }}
	addAll := func(packs []pack) {
		for _, p := range packs {
			if err := b.add(p); err != nil {
				res.GenerateErrors++
				res.GenerateLastErr = err.Error()
			}
		}
	}
	periodic := func(now time.Time, heartbeat bool) {
		for _, o := range cfg.Objects {
			var p pack
			var err error
			if heartbeat {
				p, err = gen.heartbeat(o)
			} else {
				p, err = gen.counter(o, now.UnixMilli())
			}
			if err != nil {
				res.GenerateErrors++
				res.GenerateLastErr = err.Error()
				continue
			}
			addAll([]pack{p})
		}
	}
	// Agents register (heartbeat) before their first transaction.
	start := time.Now()
	periodic(start, true)
	b.flush()
	nextHeartbeat := start.Add(cfg.HeartbeatInterval)
	nextCounter := start
	i := uint64(cfg.Index)
	step := uint64(cfg.Count)
	for {
		if ctx.Err() != nil {
			break
		}
		now := time.Now()
		if now.After(cfg.HardStop) {
			break
		}
		if len(cfg.Objects) > 0 && !now.Before(nextHeartbeat) {
			periodic(now, true)
			for !nextHeartbeat.After(now) {
				nextHeartbeat = nextHeartbeat.Add(cfg.HeartbeatInterval)
			}
		}
		if len(cfg.Objects) > 0 && !now.Before(nextCounter) {
			periodic(now, false)
			for !nextCounter.After(now) {
				nextCounter = nextCounter.Add(cfg.CounterInterval)
			}
		}
		// Bounded catch-up per cycle keeps periodic packets and ctx checks timely.
		for n := 0; n < 4096 && i < sched.total && !sched.at(i).After(now); n++ {
			packs, err := gen.transaction(i, time.Now().UnixMilli())
			if err != nil {
				res.GenerateErrors++
				res.GenerateLastErr = err.Error()
				cfg.Failed.set(i)
				res.TxFailed++
			} else {
				addAll(packs)
			}
			res.ScheduledLast = sched.at(i)
			i += step
		}
		b.flush()
		if i >= sched.total {
			break
		}
		wake := sched.at(i)
		if len(cfg.Objects) > 0 {
			wake = earliest(wake, nextHeartbeat, nextCounter)
		}
		d := time.Until(wake)
		if d <= 0 {
			continue
		}
		d = max(d, cfg.Tick)
		timer := time.NewTimer(d)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	b.flush()
	for ; i < sched.total; i += step {
		res.TxNotSent++
	}
	return res
}

func earliest(t time.Time, others ...time.Time) time.Time {
	for _, o := range others {
		if o.Before(t) {
			t = o
		}
	}
	return t
}

// Objects0Hash is the fragment header objHash (Java uses the agent's own objHash).
func (c senderConfig) Objects0Hash() int32 {
	if len(c.Objects) == 0 {
		return 0
	}
	return c.Objects[0].Hash
}
