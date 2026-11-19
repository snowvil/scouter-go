package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/ingest"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/server"
	"github.com/snowvil/scouter-go/internal/transport"
)

func TestScheduleOffsets(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	s := newSchedule(start, 200, time.Second, 10*time.Second)
	if s.total != 2200 || s.steadyFrom != 200 {
		t.Fatalf("total=%d steadyFrom=%d", s.total, s.steadyFrom)
	}
	if got := s.offset(1); got != 5*time.Millisecond {
		t.Fatalf("offset(1)=%v", got)
	}
	if got := s.at(2199); got != start.Add(10995*time.Millisecond) {
		t.Fatalf("at(last)=%v", got.Sub(start))
	}
	// Fractional rate: 2.5 tx/s for 2s -> 5 transactions spaced 400ms.
	f := newSchedule(start, 2.5, 0, 2*time.Second)
	if f.total != 5 || f.offset(4) != 1600*time.Millisecond || f.steadyFrom != 0 {
		t.Fatalf("fractional %+v offset=%v", f, f.offset(4))
	}
	// Offsets are independent of send progress (no cumulative drift).
	big := newSchedule(start, 100000, 0, time.Hour)
	if got := big.offset(360_000_000 - 1); got != time.Hour-10*time.Microsecond {
		t.Fatalf("large offset %v", got)
	}
}

func TestHistogramQuantiles(t *testing.T) {
	var h histogram
	for i := int64(1); i <= 1000; i++ {
		h.record(i * int64(time.Millisecond))
	}
	h.record(-5) // clamped
	if h.count != 1001 || h.negativeCount != 1 || h.min != 0 || h.max != int64(time.Second) {
		t.Fatalf("count=%d neg=%d min=%d max=%d", h.count, h.negativeCount, h.min, h.max)
	}
	for _, q := range []struct {
		q    float64
		want float64
	}{{0.5, 500e6}, {0.95, 950e6}, {0.99, 990e6}} {
		got := float64(h.quantile(q.q))
		if got < q.want*0.99 || got > q.want*1.04 {
			t.Fatalf("q%.2f=%v want ~%v", q.q, got, q.want)
		}
	}
	if n := h.countAbove(int64(900 * time.Millisecond)); n < 90 || n > 110 {
		t.Fatalf("countAbove=%d", n)
	}
	for _, v := range []int64{0, 63, 64, 65, 1 << 20, 1<<62 + 12345} {
		if i := histIndex(v); histUpper(i) < v || (i > 0 && histUpper(i-1) >= v) {
			t.Fatalf("bucket of %d: index %d upper %d", v, i, histUpper(i))
		}
	}
	var m histogram
	m.merge(&h)
	if m.quantile(0.5) != h.quantile(0.5) || m.count != h.count {
		t.Fatal("merge")
	}
}

func TestTxidCodec(t *testing.T) {
	for _, seed := range []uint64{0, 1, 42, 1<<64 - 1} {
		c := newTxidCodec(seed)
		seen := make(map[int64]bool, 100000)
		for i := uint64(0); i < 100000; i++ {
			tx := c.encode(i)
			if tx == 0 || seen[tx] {
				t.Fatalf("seed %d index %d txid %d zero/duplicate", seed, i, tx)
			}
			seen[tx] = true
			if got := c.decode(tx); got != i {
				t.Fatalf("decode(encode(%d))=%d", i, got)
			}
		}
		if c.decode(newTxidCodec(seed+1).encode(5)) < 1<<40 {
			t.Fatalf("seed %d: another seed's txid decoded into a small index", seed)
		}
	}
}

func TestMakeObjectsSigns(t *testing.T) {
	for n := 2; n <= 40; n++ {
		objs := makeObjects(n)
		neg, pos := 0, 0
		names := map[string]bool{}
		for _, o := range objs {
			if o.Hash != javaHash(o.Name) || names[o.Name] {
				t.Fatalf("n=%d object %+v", n, o)
			}
			names[o.Name] = true
			if o.Hash < 0 {
				neg++
			} else {
				pos++
			}
		}
		if neg == 0 || pos == 0 || len(objs) != n {
			t.Fatalf("n=%d neg=%d pos=%d", n, neg, pos)
		}
	}
	// Java HashUtil.hash is CRC32 of UTF-8 as a signed int.
	if javaHash("123456789") != int32(-873187034) {
		t.Fatalf("crc32 check value %d", javaHash("123456789"))
	}
}

func testGenConfig(seed uint64) genConfig {
	sizes, _ := parseProfileSizes("0:0.2,1024:0.4,8192:0.3,70000:0.1")
	return genConfig{Seed: seed, Objects: makeObjects(7), ProfileRatio: 0.5, ProfileSizes: sizes, TextHitRatio: 0.8, TextPool: 50, ErrorRatio: 0.1}
}

func generate(t *testing.T, seed uint64, n int) [][]byte {
	t.Helper()
	g := newGenerator(testGenConfig(seed), 3)
	var out [][]byte
	for i := 0; i < n; i++ {
		packs, err := g.transaction(uint64(i), 1_760_000_000_000+int64(i))
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range packs {
			out = append(out, p.Bytes)
		}
		hb, _ := g.heartbeat(g.cfg.Objects[i%7])
		ct, _ := g.counter(g.cfg.Objects[i%7], 1_760_000_000_000)
		out = append(out, hb.Bytes, ct.Bytes)
	}
	return out
}

func TestGeneratorDeterministic(t *testing.T) {
	a, b := generate(t, 7, 300), generate(t, 7, 300)
	if len(a) != len(b) {
		t.Fatalf("lengths %d %d", len(a), len(b))
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			t.Fatalf("pack %d differs for the same seed", i)
		}
	}
	c := generate(t, 8, 300)
	same := len(a) == len(c)
	for i := 0; same && i < len(a); i++ {
		same = bytes.Equal(a[i], c[i])
	}
	if same {
		t.Fatal("different seeds produced identical payloads")
	}
}

func TestGeneratorPayloadSemantics(t *testing.T) {
	g := newGenerator(testGenConfig(11), 0)
	profiles, xlogs := 0, 0
	for i := uint64(0); i < 400; i++ {
		packs, err := g.transaction(i, 1_760_000_000_000)
		if err != nil {
			t.Fatal(err)
		}
		last := packs[len(packs)-1]
		if last.Kind != "xlog" || last.Tx != int64(i) {
			t.Fatalf("tx %d: last pack %s/%d", i, last.Kind, last.Tx)
		}
		p, err := protocol.DecodePack(last.Bytes, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		x := p.(*protocol.XLogPack)
		xlogs++
		if x.TxID != g.codec.encode(i) || x.Service == 0 || x.Elapsed < 1 || x.DiscardType != 1 || x.GxID != 0 ||
			x.ObjHash != g.cfg.Objects[objectIndex(x.TxID, 7)].Hash {
			t.Fatalf("xlog %+v", x)
		}
		for _, pk := range packs[:len(packs)-1] {
			if pk.Kind != "profile" {
				continue
			}
			profiles++
			pp, err := protocol.DecodePack(pk.Bytes, protocol.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			prof := pp.(*protocol.ProfilePack)
			steps, err := protocol.DecodeSteps(prof.Profile, protocol.Limits{})
			if err != nil {
				t.Fatalf("profile steps: %v", err)
			}
			if !prof.Version2 || prof.DiscardType != 1 || prof.TxID != x.TxID || int32(len(steps)) != x.ProfileCount || int32(len(prof.Profile)) != x.ProfileSize {
				t.Fatalf("profile/xlog mismatch steps=%d count=%d", len(steps), x.ProfileCount)
			}
		}
	}
	hits, misses := uint64(0), uint64(0)
	for _, p := range g.pools {
		hits += p.hits
		misses += p.misses
	}
	ratio := float64(hits) / float64(hits+misses)
	if profiles == 0 || ratio < 0.6 || ratio > 0.97 || g.stats.PackCount["text"] != misses {
		t.Fatalf("profiles=%d hit ratio=%.3f text packs=%d misses=%d", profiles, ratio, g.stats.PackCount["text"], misses)
	}
}

// --text-cardinality bounds the distinct texts of each type: after the cap,
// references only reuse sent hashes and no more TextPacks are sent.
func TestGeneratorTextCardinality(t *testing.T) {
	cfg := testGenConfig(5)
	cfg.TextHitRatio, cfg.TextCardinality = 0.5, 12
	g := newGenerator(cfg, 0)
	services := map[int32]bool{}
	for i := uint64(0); i < 2000; i++ {
		packs, err := g.transaction(i, 1_760_000_000_000)
		if err != nil {
			t.Fatal(err)
		}
		p, err := protocol.DecodePack(packs[len(packs)-1].Bytes, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		services[p.(*protocol.XLogPack).Service] = true
	}
	if len(services) != 12 {
		t.Fatal("distinct services", len(services))
	}
	for kind, p := range g.pools {
		if p.created > 12 {
			t.Fatal(kind, "created", p.created)
		}
	}
}

// The framing is verified with the server's own reassembler (Java framing contract).
func TestFramingDecodesWithServerReassembler(t *testing.T) {
	for _, tc := range []struct{ batch, maxBytes int }{{1, 60000}, {32, 60000}, {1024, 60000}, {8, 2000}} {
		t.Run(fmt.Sprintf("batch%d_max%d", tc.batch, tc.maxBytes), func(t *testing.T) {
			g := newGenerator(testGenConfig(5), 0)
			var want [][]byte
			var grams []datagram
			pk := uint64(0)
			b := &batcher{maxBytes: tc.maxBytes, maxPacks: tc.batch, objHash: -77, emit: func(d datagram) { grams = append(grams, d) },
				nextPKID: func() int64 { pk++; return int64(pk) }}
			for i := uint64(0); i < 120; i++ {
				packs, err := g.transaction(i, 1_760_000_000_000)
				if err != nil {
					t.Fatal(err)
				}
				for _, p := range packs {
					want = append(want, p.Bytes)
					if err := b.add(p); err != nil {
						t.Fatal(err)
					}
				}
			}
			b.flush()
			r, err := transport.NewReassembler(transport.UDPConfig{})
			if err != nil {
				t.Fatal(err)
			}
			sender := netip.MustParseAddrPort("127.0.0.1:40000")
			var got [][]byte
			finals := map[int64]int{}
			kinds := map[string]int{}
			for _, d := range grams {
				kinds[d.Kind]++
				if len(d.Bytes) > tc.maxBytes+32 {
					t.Fatalf("datagram %d bytes exceeds max %d", len(d.Bytes), tc.maxBytes)
				}
				if d.Final {
					for _, tx := range d.Txs {
						finals[tx]++
					}
				}
				packs, err := r.Decode(sender, d.Bytes, time.Now())
				if err != nil {
					t.Fatalf("%s datagram: %v", d.Kind, err)
				}
				for _, p := range packs {
					enc, err := protocol.EncodePack(p, protocol.Limits{})
					if err != nil {
						t.Fatal(err)
					}
					got = append(got, enc)
				}
			}
			if len(got) != len(want) {
				t.Fatalf("decoded %d packs, want %d (kinds %v)", len(got), len(want), kinds)
			}
			// Fragments are emitted ahead of buffered packs (Java order), so compare as multisets.
			count := map[string]int{}
			for _, w := range want {
				count[string(w)]++
			}
			for _, g := range got {
				count[string(g)]--
			}
			for _, v := range count {
				if v != 0 {
					t.Fatal("decoded pack bytes differ from the sent packs")
				}
			}
			for i := int64(0); i < 120; i++ {
				if finals[i] != 1 {
					t.Fatalf("tx %d final datagram count %d", i, finals[i])
				}
			}
			if tc.batch == 1 && kinds["batch"] != 0 || tc.batch > 1 && kinds["single"] != 0 || kinds["fragment"] == 0 {
				t.Fatalf("framing kinds %v", kinds)
			}
		})
	}
}

func TestFragmentHeaderLayout(t *testing.T) {
	data := bytes.Repeat([]byte{0xab}, 250)
	frags, err := frameFragments(-2, 0x0102030405060708, data, 100)
	if err != nil || len(frags) != 3 {
		t.Fatal(len(frags), err)
	}
	f := frags[2]
	if binary.BigEndian.Uint32(f) != magicFragment || int32(binary.BigEndian.Uint32(f[4:])) != -2 ||
		binary.BigEndian.Uint64(f[8:]) != 0x0102030405060708 || binary.BigEndian.Uint16(f[16:]) != 3 ||
		binary.BigEndian.Uint16(f[18:]) != 2 || f[20] != 50 || len(f) != 21+50 {
		t.Fatalf("header % x", f[:21])
	}
	if b := javaBlob(nil, make([]byte, 300)); b[0] != 255 || binary.BigEndian.Uint16(b[1:]) != 300 {
		t.Fatal("blob 300")
	}
	if b := javaBlob(nil, make([]byte, 70000)); b[0] != 254 || binary.BigEndian.Uint32(b[1:]) != 70000 {
		t.Fatal("blob 70000")
	}
}

func TestParseStatsLine(t *testing.T) {
	line := fmt.Sprintf("2026/10/13 10:00:00 ingest=%+v udp=%+v tcp=%+v deferredErrorLogs=%d droppedErrorLogs=%d",
		ingest.Stats{Completed: 10, Rejected: 2, StorageErrors: 1, XLogs: 7, DroppedXLogs: 3, SkippedStacks: 4},
		transport.UDPStats{Datagrams: 9, Packets: 12, Rejected: 1, Malformed: 2, Expired: 3, HandlerErrors: 4, AbortedAssemblies: 5, AbortedPackets: 6, KernelCounters: true, KernelCountersError: "open /proc/net/snmp: no such file", SocketDrops: 7},
		transport.TCPStats{Accepted: 3, Rejected: 0, Errors: 1, ClientRequests: 40, StackUploads: 0, Active: -1}, 5, 6)
	if !statsLinePattern.MatchString(line) {
		t.Fatal("pattern")
	}
	s, err := parseStatsLine(line)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]map[string]int64{
		"ingest": {"Completed": 10, "Rejected": 2, "StorageErrors": 1, "XLogs": 7, "DroppedXLogs": 3, "SkippedStacks": 4},
		"udp":    {"Datagrams": 9, "Packets": 12, "Rejected": 1, "Malformed": 2, "Expired": 3, "HandlerErrors": 4, "AbortedAssemblies": 5, "AbortedPackets": 6, "KernelCounters": 1, "HostCounters": 0, "SocketDrops": 7},
		"tcp":    {"Accepted": 3, "Rejected": 0, "Errors": 1, "ClientRequests": 40, "StackUploads": 0, "Active": -1},
		"log":    {"deferredErrorLogs": 5, "droppedErrorLogs": 6},
	}
	for g, fields := range want {
		for k, v := range fields {
			if s[g][k] != v {
				t.Fatalf("%s.%s=%d want %d (%v)", g, k, s[g][k], v, s)
			}
		}
		// Fields added to the server Stats structs later are preserved, not rejected.
		if len(s[g]) < len(fields) {
			t.Fatalf("%s fields %v", g, s[g])
		}
	}
	if _, err := parseStatsLine("ingest={Completed:1} udp={Datagrams:x} tcp={}"); err == nil {
		t.Fatal("bad value accepted")
	}
	if _, err := parseStatsLine("ingest={Completed:1}"); err == nil {
		t.Fatal("missing groups accepted")
	}
}

func TestLoopbackOnly(t *testing.T) {
	for _, ok := range []string{"127.0.0.1:6100", "127.9.9.9:1", "[::1]:6100", "[::ffff:127.0.0.1]:5"} {
		if err := requireLoopback(ok); err != nil {
			t.Fatalf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{"10.0.0.1:6100", "0.0.0.0:6100", "localhost:6100", "[::]:1", "192.168.1.5:6100", "example.com:1", "127.0.0.1"} {
		if requireLoopback(bad) == nil {
			t.Fatalf("%s accepted", bad)
		}
	}
	if requireLoopbackIP("127.0.0.1") != nil || requireLoopbackIP("::1") != nil || requireLoopbackIP("10.1.1.1") == nil || requireLoopbackIP("localhost") == nil {
		t.Fatal("bind validation")
	}
	if _, err := parseOptions([]string{"--bind", "192.168.0.10"}); err == nil || !strings.Contains(err.Error(), "not loopback") {
		t.Fatalf("non-loopback bind: %v", err)
	}
	if _, err := parseOptions([]string{"--rate", "0"}); err == nil {
		t.Fatal("rate 0 accepted")
	}
	if _, err := parseOptions([]string{"--rate", "1000", "--duration", "1h", "--max-transactions", "1000"}); err == nil {
		t.Fatal("unbounded schedule accepted")
	}
	if _, err := parseOptions(nil); err != nil {
		t.Fatalf("defaults: %v", err)
	}
	if args := (serverConfig{Bind: "::1", DB: "d", WorkDir: "w", Timezone: "UTC"}).args("r"); args[3] != "[::1]:0" {
		t.Fatalf("ipv6 bind args %v", args)
	}
	// The child never creates <pid>.scouter in the working directory.
	if args := (serverConfig{Bind: "127.0.0.1"}).args("r"); !strings.Contains(strings.Join(args, " "), " --exit-file=false") {
		t.Fatalf("exit file not disabled: %v", args)
	}
}

func TestPrepareDB(t *testing.T) {
	root := t.TempDir()
	created, err := prepareDB(filepath.Join(root, "new"))
	if err != nil || !created {
		t.Fatal(created, err)
	}
	created, err = prepareDB(filepath.Join(root, "new"))
	if err != nil || created {
		t.Fatal("existing empty dir", created, err)
	}
	os.WriteFile(filepath.Join(root, "new", "xlog.data"), []byte("x"), 0o600)
	if _, err := prepareDB(filepath.Join(root, "new")); err == nil {
		t.Fatal("non-empty DB accepted")
	}
	os.WriteFile(filepath.Join(root, "file"), nil, 0o600)
	if _, err := prepareDB(filepath.Join(root, "file")); err == nil {
		t.Fatal("file accepted")
	}
}

func TestComparisonAndFinishVerify(t *testing.T) {
	objs := makeObjects(5)
	codec := newTxidCodec(9)
	sched := newSchedule(time.Unix(0, 0), 10, 300*time.Millisecond, 1700*time.Millisecond) // 20 tx, steady from 3
	failed, sent := newBitset(20), newBitset(20)
	for i := uint64(0); i < 20; i++ {
		if i == 4 || i == 15 {
			failed.set(i)
		} else {
			sent.set(i)
		}
	}
	cmp := newComparison(20, codec, objs)
	xlog := func(i uint64) *protocol.XLogPack {
		tx := codec.encode(i)
		return &protocol.XLogPack{TxID: tx, ObjHash: objs[objectIndex(tx, 5)].Hash}
	}
	for i := uint64(0); i < 20; i++ {
		if i == 7 || i == 8 || i == 15 { // 7,8 lost; 15 failed send and lost
			continue
		}
		cmp.add(xlog(i))
	}
	cmp.add(xlog(2))                                              // duplicate
	cmp.add(&protocol.XLogPack{TxID: newTxidCodec(10).encode(3)}) // extra (other seed)
	bad := xlog(1)
	bad.ObjHash++
	cmp2 := newComparison(20, codec, objs)
	cmp2.add(bad)
	if cmp2.ObjHashMismatch != 1 {
		t.Fatal("objHash mismatch not detected")
	}
	var v verifyReport
	finishVerify(&v, cmp, sched, failed, sent, sendReport{TxFailed: 2})
	if cmp.Unique != 17 || cmp.Duplicates != 1 || cmp.Extra != 1 || cmp.RecordsReturned != 19 {
		t.Fatalf("cmp %+v", cmp)
	}
	if v.Expected != 18 || v.MissingAfterSendOK != 2 || v.FoundButSendFailed != 1 || v.SendFailed != 2 || v.SteadyFound != 14 || v.steadySent != 15 {
		t.Fatalf("verify %+v", v)
	}
	if len(v.MissingSamples) != 2 || v.MissingSamples[0] != codec.encode(7) {
		t.Fatalf("missing samples %v", v.MissingSamples)
	}
}

func TestSplitRanges(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Skip("tzdata unavailable")
	}
	day, _ := scoutertime.ParseDate("20261014", loc)
	from, to := day-25_000, day+12_345
	ranges := splitRanges(from, to, 10_000, loc)
	next := from
	for _, r := range ranges {
		if r.From != next || r.To < r.From || r.To-r.From >= 10_000 {
			t.Fatalf("range %+v next %d", r, next)
		}
		if scoutertime.Date(r.From, loc) != r.Date || scoutertime.Date(r.To, loc) != r.Date {
			t.Fatalf("range crosses date %+v", r)
		}
		next = r.To + 1
	}
	if next != to+1 || ranges[2].To != day-1 || ranges[3].From != day {
		t.Fatalf("coverage %v", ranges)
	}
	if got := autoVerifyWindow(200); got != 60000 {
		t.Fatal(got)
	}
	if got := autoVerifyWindow(100000); got != 200 {
		t.Fatal(got)
	}
}

// runSender against a loopback socket: every scheduled XLog arrives, lag is recorded.
func TestSenderLoopback(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	r, _ := transport.NewReassembler(transport.UDPConfig{})
	var mu sync.Mutex
	got := map[int64]bool{}
	kinds := map[byte]int{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 70000)
		for {
			n, from, err := conn.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			packs, err := r.Decode(from, append([]byte(nil), buf[:n]...), time.Now())
			if err != nil {
				t.Errorf("decode: %v", err)
				continue
			}
			mu.Lock()
			for _, p := range packs {
				kinds[p.Type()]++
				if x, ok := p.(*protocol.XLogPack); ok {
					got[x.TxID] = true
				}
			}
			mu.Unlock()
		}
	}()
	cfg := testGenConfig(3)
	sched := newSchedule(time.Now().Add(10*time.Millisecond), 400, 0, 250*time.Millisecond)
	failed, sent := newBitset(sched.total), newBitset(sched.total)
	res := runSender(context.Background(), senderConfig{Index: 0, Count: 1, Schedule: sched, HardStop: time.Now().Add(10 * time.Second), Tick: time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond, CounterInterval: 100 * time.Millisecond, MaxBytes: 60000, Batch: 16, SocketBuffer: 4 << 20,
		Target: conn.LocalAddr().(*net.UDPAddr), Objects: cfg.Objects, Failed: failed, Sent: sent}, newGenerator(cfg, 0))
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if uint64(n) >= res.TxSent || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	conn.Close()
	<-done
	if sched.total != 100 || res.TxSent+res.TxFailed != 100 || res.Lag.count != res.TxSent || res.TxNotSent != 0 {
		t.Fatalf("total=%d sent=%d failed=%d lag=%d notSent=%d errs=%v", sched.total, res.TxSent, res.TxFailed, res.Lag.count, res.TxNotSent, res.SendErrors)
	}
	mu.Lock()
	defer mu.Unlock()
	for i := uint64(0); i < sched.total; i++ {
		if sent.has(i) && !got[newTxidCodec(3).encode(i)] {
			t.Fatalf("tx %d sent but not received on loopback", i)
		}
	}
	if kinds[protocol.PackObject] < len(cfg.Objects) || kinds[protocol.PackPerfCounter] < len(cfg.Objects) || kinds[protocol.PackText] == 0 {
		t.Fatalf("pack kinds %v", kinds)
	}
}

func TestServerArgsAndFsyncPolicy(t *testing.T) {
	for _, bad := range []string{"--db=/x", "-udp", "--tcp=0.0.0.0:6100", "--password=x", "--ready-file", "--timezone=UTC", "--compress-xlog"} {
		if validateServerArgs([]string{bad}) == nil {
			t.Fatalf("%s accepted", bad)
		}
	}
	ok := []string{"--sync-interval=0", "--purge", "--purge-interval", "30s", "--max-connections=64"}
	if err := validateServerArgs(ok); err != nil {
		t.Fatal(err)
	}
	if v, found := serverArgValue(ok, "sync-interval"); !found || v != "0" {
		t.Fatal(v, found)
	}
	if v, _ := serverArgValue(ok, "purge-interval"); v != "30s" {
		t.Fatal(v)
	}
	if v, found := serverArgValue(ok, "purge"); !found || v != "true" {
		t.Fatal(v, found)
	}
	if _, found := serverArgValue(ok, "compress-profile"); found {
		t.Fatal("absent flag found")
	}
	if args := (serverConfig{Bind: "127.0.0.1", Extra: ok}).args("r"); strings.Join(args[len(args)-5:], " ") != strings.Join(ok, " ") {
		t.Fatalf("passthrough not appended last: %v", args)
	}
	usage := "Usage of scouter-go:\n  -purge\n    \tenable retention\n  -sync-interval duration\n    \tperiodic fsync of data and index headers; 0 disables (default 4s)\n  -tcp string\n    \tTCP listen address (default \"127.0.0.1:6100\")\n"
	if d, err := parseFlagDefault(usage, "sync-interval"); err != nil || d != "4s" {
		t.Fatal(d, err)
	}
	if d, err := parseFlagDefault(usage, "purge"); err != nil || d != "" {
		t.Fatal(d, err)
	}
	if _, err := parseFlagDefault(usage, "missing"); err == nil {
		t.Fatal("missing flag")
	}
	if f := describeFsync([]string{"--sync-interval=0"}, "/nonexistent", "note"); f.Source != "--server-arg" || !strings.Contains(f.Policy, "disabled") || f.Note != "note" {
		t.Fatalf("%+v", f)
	}
	if f := describeFsync([]string{"--sync-interval", "250ms"}, "/nonexistent", ""); !strings.Contains(f.Policy, "every 250ms") {
		t.Fatalf("%+v", f)
	}
	if f := describeFsync(nil, "/nonexistent", ""); f.Source != "unavailable" || f.Error == "" {
		t.Fatalf("missing binary must be reported, got %+v", f)
	}
	for _, bad := range [][]string{{"--stop=crash"}, {"--kill-after=1s"}, {"--stop=kill", "--restart=false"}, {"--server-arg=--db=/tmp/x"}} {
		if _, err := parseOptions(bad); err == nil {
			t.Fatalf("%v accepted", bad)
		}
	}
	if o, err := parseOptions([]string{"--stop=kill", "--kill-after=3s", "--server-arg=--sync-interval=0", "--server-arg=--purge"}); err != nil || len(o.ServerArgs) != 2 {
		t.Fatal(o.ServerArgs, err)
	}
}

func TestStatsLineMaintenanceAndShutdownResidue(t *testing.T) {
	line := "2026/10/13 19:05:13 ingest={Completed:4 Rejected:0 StorageErrors:0 XLogs:2 DroppedXLogs:0 SkippedStacks:0} " +
		"udp={Datagrams:9 Packets:4 Rejected:1 Malformed:0 Expired:0 HandlerErrors:0 AbortedAssemblies:2 AbortedPackets:3 ShutdownDiscarded:5} " +
		"tcp={Accepted:1 Rejected:0 Errors:0 ClientRequests:2 StackUploads:0 Active:0} deferredErrorLogs=0 droppedErrorLogs=0 maintenanceErrors=7 purged=8"
	s, err := parseStatsLine(line)
	if err != nil {
		t.Fatal(err)
	}
	if s["udp"]["ShutdownDiscarded"] != 5 || s["udp"]["AbortedPackets"] != 3 || s["udp"]["AbortedAssemblies"] != 2 || s["udp"]["Rejected"] != 1 ||
		s["log"]["maintenanceErrors"] != 7 || s["log"]["purged"] != 8 {
		t.Fatalf("%v", s)
	}
}

// The final stats line also carries the Agent relay and visitor counters
// before the ingest writer list; they parse as their own groups.
func TestStatsLineRelayAndVisitorGroups(t *testing.T) {
	line := "2026/10/25 10:00:00 ingest={Completed:4 Rejected:0} udp={Datagrams:9 Packets:4} tcp={Accepted:1 Active:0} " +
		"deferredErrorLogs=0 droppedErrorLogs=0 maintenanceErrors=0 purged=0 recoveries=0 " +
		"agentOrphans={Active:0 Bytes:0 Orphaned:2 Completed:1 Failed:1 Rejected:0} " +
		"agentRelayBudget={Bytes:0 Limit:1073741824 Large:0 MaxLarge:2 Waiting:0 Granted:5 Waited:1 Refused:1 LargeRefused:0 ServerRefused:0} " +
		"visitors={Entries:2 ResidentBytes:10 ResidentMax:20 DeltaBytes:0 DeltaMax:30 WriteThroughs:3 WriteThroughFailures:0} " +
		"ingestWriters=[{Kind:counter Enqueued:3 Written:3 WriteErrors:0 WriteInvalid:0 Dropped:0 Abandoned:0 Pending:0}]"
	if !statsLinePattern.MatchString(line) {
		t.Fatal("not recognized as the stats line")
	}
	s, err := parseStatsLine(line)
	if err != nil {
		t.Fatal(err)
	}
	if s["agentOrphans"]["Orphaned"] != 2 || s["agentRelayBudget"]["Refused"] != 1 || s["visitors"]["WriteThroughs"] != 3 || s["ingest"]["Completed"] != 4 {
		t.Fatalf("%v", s)
	}
}

func TestInfoLogLine(t *testing.T) {
	for _, info := range []string{"2026/10/13 19:05:13 Scouter Go listening UDP=127.0.0.1:1 TCP=127.0.0.1:2 timezone=UTC",
		"2026/10/13 19:05:13 retention enabled: {}", "2026/10/13 19:05:13 purge 20261001 all (age)",
		"2026/10/14 11:53:06 pulse enabled: topics a/b partitions 30 workers 2 queue 4096 acks 0", "2026/10/14 11:55:08 pulse={XLog:{Enqueued:1}} kafka={Enqueued:1}",
		"2026/10/14 15:33:35 stats 10s: xlog 497/s (4,970)  packs 1,166/s  udp 511/s reject 0 malformed 0 expired 0 | ingest p50 40µs p99 2.0ms max 60ms reject 0 drop 0 | client 2.4/s p50 1.8ms p99 53ms max 53ms active 2 err 0 waiting 0 abandoned 0 wait-timeout 0 wait-reject 0 | agents 20  heap 54MiB sys 106MiB gc 12 goroutines 24",
		"2026/10/15 19:05:13 config: ./conf/scouter.conf not found; using flags and defaults",
		"2026/10/15 19:05:13 S145 GeoIP db file is not readable : ./conf/GeoLiteCity.dat",
		"2026/10/15 19:05:13 exit file 4242.scouter: delete it to stop the server",
		"2026/10/15 19:05:13 sqltable={Disabled:0 Enqueued1:0 Stored:0}", "2026/10/15 19:05:13 logger={Written:3 Dropped:0}",
		"2026/10/21 13:22:02 ingest writers closed: [{Kind:counter Enqueued:3 Written:3 WriteErrors:0 WriteInvalid:0 Dropped:0 Abandoned:0 Pending:0}]", "2026/10/15 19:05:13 WARNING: retention is off (Java default is on): old data is never deleted and the disk can fill up; set mgr_purge_enabled=true or --purge",
		"2026/10/24 18:24:49 shutdown: SIGTERM received; graceful shutdown within 20s (SIGINT/SIGTERM again after 1s exits at once)",
		"2026/10/26 10:00:00 WARNING: pulse: ext_plugin_pulse_exclude_applications is not set: no application is excluded (the original plugin default listed deployment-specific names; set ext_plugin_pulse_exclude_applications= to leave it empty)"} {
		if !infoLogLine(info) {
			t.Fatalf("info line counted as error: %s", info)
		}
	}
	for _, bad := range []string{"2026/10/13 19:05:13 udp.queue: \"limit\"", "2026/10/13 19:05:13 periodic sync: EIO",
		"2026/10/13 19:05:13 purge failed: x", "panic: runtime error", "2026/10/13 19:05:13 ingest tick: x", "2026/10/14 11:53:06 pulse xlog-meta sink txid=1: \"buffer full\"",
		"2026/10/15 19:05:13 config: reloaded ./conf/scouter.conf: server_id", "2026/10/15 19:05:13 exit file: open 1.scouter: permission denied",
		"2026/10/15 19:05:13 sqltable: storage error", "2026/10/26 10:00:00 WARNING: pulse: dispatcher stopped", "2026/10/26 10:00:00 WARNING: pulse: kafka is not set: x",
		"2026/10/24 18:24:49 shutdown: budget 20s exceeded; exiting 3 stage=\"store/sync\" marker=absent",
		"2026/10/24 18:24:49 shutdown: second signal SIGTERM; exiting now stage=\"store\"",
		"2026/10/21 13:22:02 ingest writers closed: [{Kind:counter Enqueued:3 Written:2 WriteErrors:1 WriteInvalid:0 Dropped:0 Abandoned:0 Pending:0}]",
		"2026/10/21 13:22:02 ingest writers closed: [{Kind:alert Enqueued:3 Written:3 WriteErrors:0 WriteInvalid:0 Dropped:12 Abandoned:0 Pending:0}]"} {
		if infoLogLine(bad) {
			t.Fatalf("error line treated as info: %s", bad)
		}
	}
}

func TestRecoveryEventsSeparatedFromErrors(t *testing.T) {
	p := &serverProc{}
	var out bytes.Buffer
	lines := strings.Join([]string{
		"2026/10/13 19:30:00 Scouter Go listening UDP=127.0.0.1:1 TCP=127.0.0.1:2 timezone=Asia/Seoul",
		"2026/10/13 19:30:00 storage recovery index-relinked /db/20261013/xlog/xlog_tim: 3 records relinked",
		"2026/10/13 19:30:00 storage recovery index-relinked /db/20261013/xlog/xlog_tid: 3 records relinked",
		"2026/10/13 19:30:00 storage recovery zip-count-advanced /db/20261013/xlog/xlog.zip: 1 -> 2",
		"2026/10/13 19:30:01 udp.queue: \"Scouter resource limit exceeded\"",
		"2026/10/13 19:30:05 ingest={Completed:0 Rejected:0 StorageErrors:0 XLogs:0 DroppedXLogs:0 SkippedStacks:0} udp={Datagrams:0 Packets:0 Rejected:0 Malformed:0 Expired:0 HandlerErrors:0 AbortedAssemblies:0 AbortedPackets:0 ShutdownDiscarded:0} tcp={Accepted:1 Rejected:0 Errors:0 ClientRequests:2 StackUploads:0 Active:0} deferredErrorLogs=0 droppedErrorLogs=0 maintenanceErrors=0 purged=0 recoveries=3",
	}, "\n") + "\n"
	p.copyStderr(strings.NewReader(lines), &out)
	if p.recovery.Count != 3 || p.recovery.ByKind["index-relinked"] != 2 || p.recovery.ByKind["zip-count-advanced"] != 1 || len(p.recovery.Samples) != 3 {
		t.Fatalf("recovery %+v", p.recovery)
	}
	if p.otherLines != 1 || !strings.Contains(p.otherLog[0], "udp.queue") || p.logLines != 6 {
		t.Fatalf("non-info lines=%d %v total=%d", p.otherLines, p.otherLog, p.logLines)
	}
	st, err := parseStatsLine(p.lastStatsLine())
	if err != nil || st["log"]["recoveries"] != 3 || st["log"]["maintenanceErrors"] != 0 {
		t.Fatalf("stats %v %v", st, err)
	}
	if out.String() != lines {
		t.Fatal("log file copy differs")
	}
	var r recoveryEvents
	for i := 0; i < 30; i++ {
		r.add("2026/10/13 19:30:00 storage recovery index-relinked /p: d")
	}
	if r.Count != 30 || len(r.Samples) != 20 || r.add("2026/10/13 19:30:00 periodic sync: x") {
		t.Fatalf("bounded samples %d/%d", r.Count, len(r.Samples))
	}
}

func TestDecidePassRecoveryByMode(t *testing.T) {
	cmp := newComparison(1, newTxidCodec(1), makeObjects(2))
	base := func() *report {
		return &report{Restart: restartReport{OK: true}, Verify: verifyReport{Complete: true}}
	}
	clean := stopResult{Signal: "terminated"}
	killed := stopResult{Signal: "killed", ExitCode: -1}
	if !decidePass("term", base(), cmp, clean) || !decidePass("kill", base(), cmp, killed) {
		t.Fatal("healthy baseline must pass")
	}
	withLog := base()
	withLog.Restart.RecoveryEvents = recoveryEvents{Count: 1, ByKind: map[string]uint64{"index-relinked": 1}}
	withStats := base()
	withStats.Restart.StatsRecoveries = 2
	for name, r := range map[string]*report{"log": withLog, "stats": withStats} {
		if decidePass("term", r, cmp, clean) {
			t.Fatalf("term mode with recovery (%s) passed", name)
		}
		if !decidePass("kill", r, cmp, killed) {
			t.Fatalf("kill mode with recovery (%s) failed; recovery must only be recorded", name)
		}
	}
	lost := base()
	lost.Verify.MissingAfterSendOK = 5
	if decidePass("term", lost, cmp, clean) || !decidePass("kill", lost, cmp, killed) {
		t.Fatal("missing: term fails, kill records")
	}
	bad := base()
	bad.Restart.ServerErrorLogLines = 1
	if decidePass("kill", bad, cmp, killed) {
		t.Fatal("server-2 error lines must fail kill mode")
	}
	incomplete := base()
	incomplete.Verify.Complete = false
	if decidePass("kill", incomplete, cmp, killed) {
		t.Fatal("unverified windows must fail kill mode")
	}
	refused := base()
	refused.Restart.OK = false
	if decidePass("kill", refused, cmp, killed) {
		t.Fatal("restart failure must fail")
	}
}

// The final counter line a real server.Run prints parses with
// parseStatsLine, the Agent relay and visitor groups included.
func TestRunFinalStatsLineParses(t *testing.T) {
	root := t.TempDir()
	r, err := server.Open(server.Config{Directory: filepath.Join(root, "db"), LogDirectory: filepath.Join(root, "logs"), Location: time.UTC,
		UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>")})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	log.SetOutput(&out)
	defer log.SetOutput(os.Stderr)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	log.SetOutput(os.Stderr)
	var line string
	for _, l := range strings.Split(out.String(), "\n") {
		if statsLinePattern.MatchString(l) {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("no final stats line in\n%s", out.String())
	}
	s, err := parseStatsLine(line)
	if err != nil {
		t.Fatalf("%v\n%s", err, line)
	}
	for _, group := range []string{"ingest", "udp", "tcp", "xlogQueue", "profileQueue", "agentOrphans", "agentRelayBudget", "visitors"} {
		if s[group] == nil {
			t.Fatalf("group %s missing from %v\n%s", group, s, line)
		}
	}
	if _, ok := s["visitors"]["WriteThroughs"]; !ok {
		t.Fatalf("visitors %v", s["visitors"])
	}
	if _, ok := s["agentRelayBudget"]["Limit"]; !ok {
		t.Fatalf("agentRelayBudget %v", s["agentRelayBudget"])
	}
}
