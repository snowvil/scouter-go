package main

import (
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// javaHash is the Scouter HashUtil.hash (CRC32 of UTF-8 bytes as Java int).
func javaHash(s string) int32 { return int32(crc32.ChecksumIEEE([]byte(s))) }

type object struct {
	Name string `json:"name"`
	Hash int32  `json:"hash"`
}

// makeObjects returns n deterministic agent objects. When n >= 2 it guarantees at
// least one negative and one non-negative objHash so signed hash handling is exercised.
func makeObjects(n int) []object {
	out := make([]object, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("/replay-host%02d/tomcat%03d", i/8, i)
		out = append(out, object{Name: name, Hash: javaHash(name)})
	}
	if n < 2 {
		return out
	}
	neg, pos := 0, 0
	for _, o := range out {
		if o.Hash < 0 {
			neg++
		} else {
			pos++
		}
	}
	for k := 0; neg == 0 || pos == 0; k++ {
		last := &out[n-1]
		wantNegative := neg == 0
		name := fmt.Sprintf("/replay-host%02d/tomcat%03d-%d", (n-1)/8, n-1, k)
		h := javaHash(name)
		if (h < 0) == wantNegative {
			if last.Hash < 0 {
				neg--
			} else {
				pos--
			}
			*last = object{Name: name, Hash: h}
			if h < 0 {
				neg++
			} else {
				pos++
			}
		}
	}
	return out
}

// txidCodec is a seed-keyed bijection between transaction index and txid. It lets the
// verifier keep a bitset instead of a txid set: decode(encode(i)) == i, and txid
// is never 0 for i < 2^62.
type txidCodec struct{ key uint64 }

const (
	mixA = 0xbf58476d1ce4e5b9
	mixB = 0x94d049bb133111eb
)

var mixAInv, mixBInv = oddInverse(mixA), oddInverse(mixB)

func oddInverse(a uint64) uint64 {
	x := a // Newton iteration doubles correct low bits each step.
	for range 6 {
		x *= 2 - a*x
	}
	return x
}
func unxorshift(z uint64, s uint) uint64 {
	x := z
	for range 64/s + 1 {
		x = z ^ (x >> s)
	}
	return x
}
func newTxidCodec(seed uint64) txidCodec {
	k := splitmix(seed ^ 0x5c0e7e9a11ce5eed)
	return txidCodec{key: (k | 1) & (1<<63 - 1) &^ (1 << 62)}
}
func splitmix(x uint64) uint64 {
	x ^= x >> 30
	x *= mixA
	x ^= x >> 27
	x *= mixB
	x ^= x >> 31
	return x
}
func (c txidCodec) encode(i uint64) int64 { return int64(splitmix(i + c.key)) }
func (c txidCodec) decode(txid int64) uint64 {
	x := unxorshift(uint64(txid), 31)
	x *= mixBInv
	x = unxorshift(x, 27)
	x *= mixAInv
	x = unxorshift(x, 30)
	return x - c.key
}

// objectIndex is derived from the txid only, so the verifier can check objHash.
func objectIndex(txid int64, n int) int { return int(uint64(txid) % uint64(n)) }

type sizeWeight struct {
	Bytes  int     `json:"bytes"`
	Weight float64 `json:"weight"`
}

// parseProfileSizes parses "0:0.2,1024:0.5,8192:0.25,131072:0.05". Size 0 means the
// transaction sends no ProfilePack (Java never sends an empty profile).
func parseProfileSizes(spec string) ([]sizeWeight, error) {
	var out []sizeWeight
	total := 0.0
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		size, weight, ok := strings.Cut(part, ":")
		if !ok {
			return nil, fmt.Errorf("profile size %q: want bytes:weight", part)
		}
		b, err := strconv.Atoi(size)
		if err != nil || b < 0 || b > 4<<20 {
			return nil, fmt.Errorf("profile size %q: bytes must be 0..4194304", part)
		}
		w, err := strconv.ParseFloat(weight, 64)
		if err != nil || w < 0 {
			return nil, fmt.Errorf("profile size %q: invalid weight", part)
		}
		out = append(out, sizeWeight{b, w})
		total += w
	}
	if len(out) == 0 || total <= 0 {
		return nil, fmt.Errorf("profile sizes: at least one positive weight required")
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Bytes < out[j].Bytes })
	return out, nil
}

type genConfig struct {
	Seed         uint64
	Objects      []object
	ProfileRatio float64
	ProfileSizes []sizeWeight
	TextHitRatio float64
	TextPool     int
	// TextCardinality caps the distinct texts created per type; once
	// reached, every reference reuses one (0: unbounded, each miss is new).
	TextCardinality int
	ErrorRatio      float64
}

// textPool emulates the agent-side text cache: a hit reuses an already-sent hash and
// sends no TextPack; a miss creates a new unique text and sends a TextPack first.
type textPool struct {
	kind    string
	hashes  []int32
	next    int
	created uint64
	hits    uint64
	misses  uint64
}

// generator is single-goroutine state owned by one sender. Output depends only on
// seed, stream id, call order and the injected clock values.
type generator struct {
	cfg    genConfig
	stream int
	rng    *rand.Rand
	codec  txidCodec
	pools  map[string]*textPool
	stats  corpusStats
}

type corpusStats struct {
	XLogBytes        histogram         `json:"-"`
	ProfileBytes     histogram         `json:"-"`
	ProfileClass     map[int]uint64    `json:"-"`
	PackCount        map[string]uint64 `json:"-"`
	PackBytes        map[string]uint64 `json:"-"`
	ErrorXLogs       uint64            `json:"-"`
	TransactionsMade uint64            `json:"-"`
}

func newGenerator(cfg genConfig, stream int) *generator {
	g := &generator{cfg: cfg, stream: stream, rng: rand.New(rand.NewPCG(cfg.Seed, uint64(stream)+0x9e3779b97f4a7c15)), codec: newTxidCodec(cfg.Seed), pools: map[string]*textPool{}}
	g.stats.ProfileClass = map[int]uint64{}
	g.stats.PackCount = map[string]uint64{}
	g.stats.PackBytes = map[string]uint64{}
	for _, kind := range []string{"service", "sql", "method", "error", "ua", "referer"} {
		g.pools[kind] = &textPool{kind: kind}
	}
	return g
}

// pack is one encoded Pack ready for UDP framing. Tx is the transaction index for
// XLog packs (the unit that is counted for TPS) and -1 otherwise.
type pack struct {
	Kind  string
	Bytes []byte
	Tx    int64
}

func (g *generator) encode(kind string, p protocol.Pack, tx int64) (pack, error) {
	b, err := protocol.EncodePack(p, protocol.Limits{})
	if err != nil {
		return pack{}, fmt.Errorf("encode %s: %w", kind, err)
	}
	g.stats.PackCount[kind]++
	g.stats.PackBytes[kind] += uint64(len(b))
	return pack{Kind: kind, Bytes: b, Tx: tx}, nil
}

func (g *generator) text(kind string, out *[]pack, hitRatio float64, maxPool int) (int32, error) {
	pool := g.pools[kind]
	capped := g.cfg.TextCardinality > 0 && pool.created >= uint64(g.cfg.TextCardinality)
	if len(pool.hashes) > 0 && (capped || g.rng.Float64() < hitRatio) {
		pool.hits++
		return pool.hashes[g.rng.IntN(len(pool.hashes))], nil
	}
	pool.misses++
	pool.created++
	var s string
	switch kind {
	case "service":
		s = fmt.Sprintf("/replay/s%d/api/v1/resource/%d", g.stream, pool.created)
	case "sql":
		s = fmt.Sprintf("SELECT c1, c2, c3 FROM replay_s%d_t%d WHERE id = ? AND status = ?", g.stream, pool.created)
	case "method":
		s = fmt.Sprintf("com.example.replay.S%d.Service%d#handle(Ljava/lang/String;)V", g.stream, pool.created)
	case "error":
		s = fmt.Sprintf("java.lang.IllegalStateException: replay synthetic error s%d-%d", g.stream, pool.created)
	case "ua":
		s = fmt.Sprintf("Mozilla/5.0 (replay; s%d; %d)", g.stream, pool.created)
	default:
		s = fmt.Sprintf("https://replay.invalid/s%d/ref/%d", g.stream, pool.created)
	}
	h := javaHash(s)
	p, err := g.encode("text", &protocol.TextPack{XType: kind, Hash: h, Text: s}, -1)
	if err != nil {
		return 0, err
	}
	*out = append(*out, p)
	if maxPool < 1 {
		maxPool = 1
	}
	if len(pool.hashes) < maxPool {
		pool.hashes = append(pool.hashes, h)
	} else {
		pool.hashes[pool.next] = h
		pool.next = (pool.next + 1) % maxPool
	}
	return h, nil
}

func (g *generator) profileSize() int {
	total := 0.0
	for _, s := range g.cfg.ProfileSizes {
		total += s.Weight
	}
	r := g.rng.Float64() * total
	for _, s := range g.cfg.ProfileSizes {
		if r < s.Weight {
			return s.Bytes
		}
		r -= s.Weight
	}
	return g.cfg.ProfileSizes[len(g.cfg.ProfileSizes)-1].Bytes
}

// profile builds a real Step sequence (Java Step.toBytes: no count prefix) of
// approximately target bytes. It returns the encoded profile and step count.
func (g *generator) profile(target int, out *[]pack, elapsed int32) ([]byte, int32, error) {
	var buf []byte
	var steps int32
	start := int32(0)
	for len(buf) < target {
		remaining := target - len(buf)
		var step protocol.Step
		single := protocol.SingleStep{Parent: -1, Index: steps, StartTime: start, StartCPU: start / 4}
		switch {
		case remaining <= 48:
			step = &protocol.MessageStep{SingleStep: single, Message: strings.Repeat("m", max(remaining-12, 1))}
		case steps%3 == 2:
			h, err := g.text("method", out, g.cfg.TextHitRatio, g.cfg.TextPool)
			if err != nil {
				return nil, 0, err
			}
			step = &protocol.MethodStep2{MethodStep: protocol.MethodStep{SingleStep: single, Hash: h, Elapsed: 1 + int32(g.rng.IntN(20)), CPUTime: 1}}
		default:
			h, err := g.text("sql", out, g.cfg.TextHitRatio, g.cfg.TextPool)
			if err != nil {
				return nil, 0, err
			}
			param := strconv.FormatInt(g.rng.Int64N(1_000_000_000), 10)
			if remaining > 600 {
				// Bind parameter text dominates real profile size; vary it.
				param += "," + strings.Repeat("p", g.rng.IntN(400))
			}
			step = &protocol.SQLStep3{SQLStep2: protocol.SQLStep2{SQLStep: protocol.SQLStep{SingleStep: single, Hash: h, Elapsed: 1 + int32(g.rng.IntN(50)), CPUTime: 1, Param: param}}, Updated: int32(g.rng.IntN(3))}
		}
		b, err := protocol.EncodeStep(step, protocol.Limits{})
		if err != nil {
			return nil, 0, err
		}
		buf = append(buf, b...)
		steps++
		if elapsed > 0 {
			start = min(start+1+int32(g.rng.IntN(5)), elapsed)
		}
	}
	return buf, steps, nil
}

// transaction emits the packs for transaction i in the Java agent order: new texts,
// ProfilePack2 (if any), then the completed XLog. endTime is the current wall clock.
func (g *generator) transaction(i uint64, endTime int64) ([]pack, error) {
	var out []pack
	txid := g.codec.encode(i)
	obj := g.cfg.Objects[objectIndex(txid, len(g.cfg.Objects))]
	service, err := g.text("service", &out, g.cfg.TextHitRatio, g.cfg.TextPool)
	if err != nil {
		return nil, err
	}
	ua, err := g.text("ua", &out, max(g.cfg.TextHitRatio, 0.99), 64)
	if err != nil {
		return nil, err
	}
	referer, err := g.text("referer", &out, max(g.cfg.TextHitRatio, 0.9), 256)
	if err != nil {
		return nil, err
	}
	elapsed := 1 + int32(min(g.rng.ExpFloat64()*80, 60000))
	x := &protocol.XLogPack{
		EndTime: endTime, ObjHash: obj.Hash, Service: service, TxID: txid, Elapsed: elapsed,
		CPU: elapsed / 4, SQLCount: int32(g.rng.IntN(10)), IPAddress: []byte{10, byte(g.stream), byte(i >> 8), byte(i)},
		KBytes: int32(g.rng.IntN(64)), Status: 200, UserID: g.rng.Int64(), UserAgent: ua, Referer: referer,
		XType: 0, DiscardType: 1, TailVersion: protocol.XLogCurrentTail,
	}
	x.SQLTime = x.SQLCount * int32(g.rng.IntN(5))
	if g.rng.Float64() < g.cfg.ErrorRatio {
		x.Error, err = g.text("error", &out, g.cfg.TextHitRatio, g.cfg.TextPool)
		if err != nil {
			return nil, err
		}
		x.Status = 500
		g.stats.ErrorXLogs++
	}
	if g.rng.Float64() < g.cfg.ProfileRatio {
		size := g.profileSize()
		g.stats.ProfileClass[size]++
		if size > 0 {
			raw, steps, err := g.profile(size, &out, elapsed)
			if err != nil {
				return nil, err
			}
			p, err := g.encode("profile", &protocol.ProfilePack{ObjHash: obj.Hash, Service: service, TxID: txid, Profile: raw, Version2: true, DiscardType: 1}, -1)
			if err != nil {
				return nil, err
			}
			out = append(out, p)
			g.stats.ProfileBytes.record(int64(len(raw)))
			x.ProfileCount, x.ProfileSize = steps, int32(len(raw))
		}
	}
	p, err := g.encode("xlog", x, int64(i))
	if err != nil {
		return nil, err
	}
	g.stats.XLogBytes.record(int64(len(p.Bytes)))
	g.stats.TransactionsMade++
	return append(out, p), nil
}

func (g *generator) heartbeat(o object) (pack, error) {
	return g.encode("object", &protocol.ObjectPack{ObjType: "tomcat", ObjHash: o.Hash, ObjName: o.Name, Address: "127.0.0.1", Version: "scouter-replay", Alive: true, Tags: protocol.Value{Type: protocol.ValueMap}}, -1)
}

func (g *generator) counter(o object, now int64) (pack, error) {
	f := func(v float64) protocol.Value { return protocol.Value{Type: protocol.ValueFloat, Float: float32(v)} }
	d := func(v int64) protocol.Value { return protocol.Value{Type: protocol.ValueDecimal, Int: v} }
	data := protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{
		{Key: "TPS", Value: f(g.rng.Float64() * 100)},
		{Key: "ElapsedTime", Value: d(g.rng.Int64N(500))},
		{Key: "ActiveService", Value: d(g.rng.Int64N(20))},
		{Key: "ErrorRate", Value: f(g.rng.Float64())},
		{Key: "HeapUsed", Value: f(256 + g.rng.Float64()*512)},
	}}
	return g.encode("counter", &protocol.PerfCounterPack{Time: now, ObjName: o.Name, TimeType: 1, Data: data}, -1)
}
