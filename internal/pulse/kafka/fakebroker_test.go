package kafka

// In-process fake Kafka broker used by the producer tests. It speaks the same
// subset as the producer (ApiVersions v0/v3, Metadata v1..v13, Produce
// v3..v12, request header v1/v2, response header v0/v1) and decodes received
// RecordBatches with CRC verification and LZ4 decompression so tests can
// assert on the exact records a real broker would append. With unknownTags it
// adds an unknown tagged field to every flexible tagged section it writes.
//
// This fake is NOT a real broker: real-broker interoperability is checked by
// realbroker_test.go against scripts/kafka-local.py (see doc.go).

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type decodedHeader struct {
	Key   string
	Value []byte
	null  bool
}
type decodedRecord struct {
	key         []byte
	keyNull     bool
	value       []byte
	valueNull   bool
	headers     []decodedHeader
	timestamp   int64
	offsetDelta int32
}
type batchMeta struct {
	baseTimestamp, maxTimestamp int64
	count, lastOffsetDelta      int32
	attributes                  int16
}

type varReader struct {
	buf []byte
	off int
}

func (r *varReader) varint() (int64, error) {
	u, n := binary.Uvarint(r.buf[r.off:])
	if n <= 0 {
		return 0, errors.New("bad varint")
	}
	r.off += n
	return int64(u>>1) ^ -int64(u&1), nil
}
func (r *varReader) bytes(n int64) ([]byte, error) {
	if n < 0 || int64(len(r.buf)-r.off) < n {
		return nil, fmt.Errorf("bad length %d", n)
	}
	out := append([]byte{}, r.buf[r.off:r.off+int(n)]...)
	r.off += int(n)
	return out, nil
}

// decodeBatch verifies the magic-2 header, batch length and CRC32C and
// returns the records with absolute timestamps.
func decodeBatch(buf []byte) (batchMeta, []decodedRecord, error) {
	var meta batchMeta
	if len(buf) < batchHeaderLen {
		return meta, nil, fmt.Errorf("batch too short: %d", len(buf))
	}
	if l := int32(binary.BigEndian.Uint32(buf[8:])); int(l) != len(buf)-12 {
		return meta, nil, fmt.Errorf("batchLength %d != %d", l, len(buf)-12)
	}
	if e := int32(binary.BigEndian.Uint32(buf[12:])); e != -1 {
		return meta, nil, fmt.Errorf("partitionLeaderEpoch %d", e)
	}
	if buf[16] != batchMagic {
		return meta, nil, fmt.Errorf("magic %d", buf[16])
	}
	if crc := binary.BigEndian.Uint32(buf[17:]); crc != crc32.Checksum(buf[21:], castagnoli) {
		return meta, nil, errors.New("crc mismatch")
	}
	meta.attributes = int16(binary.BigEndian.Uint16(buf[21:]))
	meta.lastOffsetDelta = int32(binary.BigEndian.Uint32(buf[23:]))
	meta.baseTimestamp = int64(binary.BigEndian.Uint64(buf[27:]))
	meta.maxTimestamp = int64(binary.BigEndian.Uint64(buf[35:]))
	if pid := int64(binary.BigEndian.Uint64(buf[43:])); pid != -1 {
		return meta, nil, fmt.Errorf("producerId %d", pid)
	}
	if ep := int16(binary.BigEndian.Uint16(buf[51:])); ep != -1 {
		return meta, nil, fmt.Errorf("producerEpoch %d", ep)
	}
	if seq := int32(binary.BigEndian.Uint32(buf[53:])); seq != -1 {
		return meta, nil, fmt.Errorf("baseSequence %d", seq)
	}
	meta.count = int32(binary.BigEndian.Uint32(buf[57:]))
	if meta.count <= 0 || meta.lastOffsetDelta != meta.count-1 {
		return meta, nil, fmt.Errorf("count %d lastOffsetDelta %d", meta.count, meta.lastOffsetDelta)
	}
	if meta.attributes&^7 != 0 {
		return meta, nil, fmt.Errorf("attributes %#x", meta.attributes)
	}
	buf = buf[batchHeaderLen:]
	switch meta.attributes & 7 {
	case 0:
	case batchCodecLZ4:
		plain, err := lz4DecodeFrame(buf)
		if err != nil {
			return meta, nil, fmt.Errorf("lz4: %w", err)
		}
		buf = plain
	default:
		return meta, nil, fmt.Errorf("compression codec %d", meta.attributes&7)
	}
	r := &varReader{buf: buf}
	records := make([]decodedRecord, 0, meta.count)
	for i := int32(0); i < meta.count; i++ {
		length, err := r.varint()
		if err != nil {
			return meta, nil, err
		}
		end := r.off + int(length)
		if length < 0 || end > len(buf) {
			return meta, nil, fmt.Errorf("record length %d", length)
		}
		var rec decodedRecord
		if r.buf[r.off] != 0 {
			return meta, nil, fmt.Errorf("record attributes %d", r.buf[r.off])
		}
		r.off++
		delta, err := r.varint()
		if err != nil {
			return meta, nil, err
		}
		rec.timestamp = meta.baseTimestamp + delta
		od, err := r.varint()
		if err != nil {
			return meta, nil, err
		}
		rec.offsetDelta = int32(od)
		if rec.offsetDelta != i {
			return meta, nil, fmt.Errorf("offsetDelta %d at %d", rec.offsetDelta, i)
		}
		kl, err := r.varint()
		if err != nil {
			return meta, nil, err
		}
		if kl == -1 {
			rec.keyNull = true
		} else if rec.key, err = r.bytes(kl); err != nil {
			return meta, nil, err
		}
		vl, err := r.varint()
		if err != nil {
			return meta, nil, err
		}
		if vl == -1 {
			rec.valueNull = true
		} else if rec.value, err = r.bytes(vl); err != nil {
			return meta, nil, err
		}
		hc, err := r.varint()
		if err != nil {
			return meta, nil, err
		}
		if hc < 0 || hc > int64(len(buf)) {
			return meta, nil, fmt.Errorf("header count %d", hc)
		}
		for range hc {
			var h decodedHeader
			hkl, err := r.varint()
			if err != nil {
				return meta, nil, err
			}
			hk, err := r.bytes(hkl)
			if err != nil {
				return meta, nil, err
			}
			h.Key = string(hk)
			hvl, err := r.varint()
			if err != nil {
				return meta, nil, err
			}
			if hvl == -1 {
				h.null = true
			} else if h.Value, err = r.bytes(hvl); err != nil {
				return meta, nil, err
			}
			rec.headers = append(rec.headers, h)
		}
		if r.off != end {
			return meta, nil, fmt.Errorf("record %d consumed %d of %d", i, r.off-(end-int(length)), length)
		}
		records = append(records, rec)
	}
	if r.off != len(buf) {
		return meta, nil, fmt.Errorf("%d trailing bytes", len(buf)-r.off)
	}
	return meta, records, nil
}

// received is one record as the fake broker appended it.
type received struct {
	node      int32
	topic     string
	partition int32
	decodedRecord
	corr int32
}

// clusterView is the leader map shared by all fake brokers of one test.
type clusterView struct {
	mu      sync.Mutex
	brokers []*fakeBroker
	leaders map[topicPartition]int32
}

func (v *clusterView) setLeader(topic string, partition, node int32) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.leaders[topicPartition{topic, partition}] = node
}

type fakeBroker struct {
	t        testing.TB
	node     int32
	view     *clusterView
	ln       net.Listener
	addr     string
	portNum  int32
	versions map[int16]versionRange
	// hooks (set before traffic; read under mu)
	mu           sync.Mutex
	produceError func(topic string, partition int32, corr int32) int16
	metadataMode int // 0 normal, 1 oversized size prefix, 2 truncated body, 3 wrong correlation
	conns        map[net.Conn]struct{}
	received     []received
	requests     map[int16]int
	versionsSeen [][2]int16 // (api, version) of every request
	codecs       []int16    // compression codec of every received batch
	offset       int64
	wg           sync.WaitGroup
	closed       atomic.Bool
	// unknownTags adds unknown tagged fields to every flexible tagged section.
	unknownTags bool
	// rejectFlexibleApiVersions answers ApiVersions v3 like a pre-2.4 broker.
	rejectFlexibleApiVersions bool
}

// defaultBrokerVersions mirrors a Kafka 4.0 broker (kafka-clients 4.0.0
// ApiKeys): Produce 3..12, Metadata 0..13, ApiVersions 0..4.
func defaultBrokerVersions() map[int16]versionRange {
	return map[int16]versionRange{apiProduce: {3, 12}, apiMetadata: {0, 13}, apiApiVersions: {0, 4}, 1: {4, 17}}
}

func (b *fakeBroker) seen(api int16) []int16 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []int16
	for _, s := range b.versionsSeen {
		if s[0] == api {
			out = append(out, s[1])
		}
	}
	return out
}

func (b *fakeBroker) receivedCodecs() []int16 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int16{}, b.codecs...)
}

func startFakeBroker(t testing.TB, view *clusterView, node int32) *fakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &fakeBroker{t: t, node: node, view: view, ln: ln, addr: ln.Addr().String(), portNum: int32(ln.Addr().(*net.TCPAddr).Port), versions: defaultBrokerVersions(), conns: make(map[net.Conn]struct{}), requests: make(map[int16]int)}
	view.mu.Lock()
	view.brokers = append(view.brokers, b)
	view.mu.Unlock()
	b.wg.Add(1)
	go b.accept()
	t.Cleanup(b.stop)
	return b
}

func (b *fakeBroker) stop() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}
	b.ln.Close()
	b.closeConns()
	b.wg.Wait()
}

func (b *fakeBroker) port() int32 { return b.portNum }

// closeConns drops every active connection (simulated broker restart).
func (b *fakeBroker) closeConns() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for c := range b.conns {
		c.Close()
	}
}

func (b *fakeBroker) records() []received {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]received{}, b.received...)
}

func (b *fakeBroker) requestCount(api int16) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.requests[api]
}

func (b *fakeBroker) accept() {
	defer b.wg.Done()
	for {
		c, err := b.ln.Accept()
		if err != nil {
			return
		}
		b.mu.Lock()
		if b.closed.Load() {
			b.mu.Unlock()
			c.Close()
			return
		}
		b.conns[c] = struct{}{}
		b.mu.Unlock()
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			defer func() {
				c.Close()
				b.mu.Lock()
				delete(b.conns, c)
				b.mu.Unlock()
			}()
			b.serve(c)
		}()
	}
}

func (b *fakeBroker) serve(c net.Conn) {
	for {
		var head [4]byte
		if _, err := io.ReadFull(c, head[:]); err != nil {
			return
		}
		size := int32(binary.BigEndian.Uint32(head[:]))
		if size < 8 || size > 16<<20 {
			b.t.Errorf("fake broker %d: bad request size %d", b.node, size)
			return
		}
		frame := make([]byte, size)
		if _, err := io.ReadFull(c, frame); err != nil {
			return
		}
		d := &decoder{buf: frame, limits: defaultLimits}
		api, _ := d.int16()
		version, _ := d.int16()
		corr, _ := d.int32()
		if _, _, err := d.nullableStr(); err != nil {
			b.t.Errorf("fake broker %d: client id: %v", b.node, err)
			return
		}
		b.mu.Lock()
		b.requests[api]++
		b.versionsSeen = append(b.versionsSeen, [2]int16{api, version})
		rejectV3 := b.rejectFlexibleApiVersions
		b.mu.Unlock()
		if api == apiApiVersions && version >= 3 && rejectV3 {
			// A pre-KIP-511 broker: header parsed as v1, body ignored, error in v0 layout.
			resp := b.frame(corr, false, func(e *encoder) {
				e.int16(errUnsupportedVersionCode)
				e.int32(1)
				e.int16(apiApiVersions)
				e.int16(0)
				e.int16(2)
			})
			if _, err := c.Write(resp); err != nil {
				return
			}
			continue
		}
		if isFlexible(api, version) {
			if err := d.skipTags(); err != nil {
				b.t.Errorf("fake broker %d: request header tags: %v", b.node, err)
				return
			}
		}
		resp, err := b.handle(api, version, corr, d)
		if err != nil {
			b.t.Errorf("fake broker %d: api %d v%d: %v", b.node, api, version, err)
			return
		}
		if resp == nil {
			continue
		}
		if _, err := c.Write(resp); err != nil {
			return
		}
	}
}

// writeTags writes a tagged field section; with unknown it carries two
// fields with tags the producer does not know.
func writeTags(e *encoder, flex, unknown bool) {
	if !flex {
		return
	}
	if !unknown {
		e.emptyTags()
		return
	}
	e.uvarint(2)
	e.uvarint(7)
	e.uvarint(3)
	e.buf = append(e.buf, 1, 2, 3)
	e.uvarint(300) // multi-byte tag number
	e.uvarint(0)
}

func (b *fakeBroker) frame(corr int32, flexHeader bool, body func(*encoder)) []byte {
	e := &encoder{}
	e.int32(0)
	e.int32(corr)
	writeTags(e, flexHeader, b.unknownTags)
	body(e)
	binary.BigEndian.PutUint32(e.buf, uint32(len(e.buf)-4))
	return e.buf
}

func (b *fakeBroker) handle(api, version int16, corr int32, d *decoder) ([]byte, error) {
	switch api {
	case apiApiVersions:
		return b.handleApiVersions(version, corr, d)
	case apiMetadata:
		return b.handleMetadata(version, corr, d)
	case apiProduce:
		return b.handleProduce(version, corr, d)
	}
	return nil, fmt.Errorf("unsupported api %d", api)
}

func (b *fakeBroker) handleApiVersions(version int16, corr int32, d *decoder) ([]byte, error) {
	flex := version >= 3
	switch version {
	case 0:
	case 3:
		name, err := d.compactStr()
		if err != nil {
			return nil, err
		}
		sw, err := d.compactStr()
		if err != nil {
			return nil, err
		}
		if name != clientSoftwareName || sw != clientSoftwareVersion {
			return nil, fmt.Errorf("client software %q %q", name, sw)
		}
		if err := d.skipTags(); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unexpected ApiVersions version %d", version)
	}
	if err := d.done(); err != nil {
		return nil, err
	}
	return b.frame(corr, false, func(e *encoder) {
		e.int16(0)
		keys := make([]int, 0, len(b.versions))
		for k := range b.versions {
			keys = append(keys, int(k))
		}
		sort.Ints(keys)
		e.arrayLenF(flex, len(keys))
		for _, k := range keys {
			r := b.versions[int16(k)]
			e.int16(int16(k))
			e.int16(r.min)
			e.int16(r.max)
			writeTags(e, flex, b.unknownTags)
		}
		if flex {
			e.int32(0) // throttle
			writeTags(e, flex, b.unknownTags)
		}
	}), nil
}

func (b *fakeBroker) handleMetadata(version int16, corr int32, d *decoder) ([]byte, error) {
	if !b.versions[apiMetadata].contains(version) || version < 1 || version > 13 {
		return nil, fmt.Errorf("metadata version %d outside fake support", version)
	}
	flex := isFlexible(apiMetadata, version)
	n, err := d.countF(flex, 1, 1<<10)
	if err != nil {
		return nil, err
	}
	topics := make([]string, 0, n)
	for range n {
		if version >= 10 {
			if err := d.skip(16); err != nil {
				return nil, err
			}
		}
		name, err := d.strF(flex)
		if err != nil {
			return nil, err
		}
		if err := d.tagsF(flex); err != nil {
			return nil, err
		}
		topics = append(topics, name)
	}
	bools := 0
	if version >= 4 {
		bools++
	}
	if version >= 8 && version <= 10 {
		bools++
	}
	if version >= 8 {
		bools++
	}
	for range bools {
		if _, err := d.bool(); err != nil {
			return nil, err
		}
	}
	if err := d.tagsF(flex); err != nil {
		return nil, err
	}
	if err := d.done(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	mode := b.metadataMode
	unknown := b.unknownTags
	b.mu.Unlock()
	body := func(e *encoder) { encodeMetadataResponse(e, version, b.view, topics, unknown) }
	switch mode {
	case 1:
		e := &encoder{}
		e.int32(math.MaxInt32)
		e.int32(corr)
		return e.buf, nil
	case 2:
		f := b.frame(corr, flex, body)
		cut := f[:len(f)-5]
		binary.BigEndian.PutUint32(cut, uint32(len(cut)-4))
		return cut, nil
	case 3:
		return b.frame(corr+1000, flex, body), nil
	}
	return b.frame(corr, flex, body), nil
}

// encodeMetadataResponse encodes the view for response versions 1..13.
func encodeMetadataResponse(e *encoder, version int16, view *clusterView, topics []string, unknownTags bool) {
	view.mu.Lock()
	defer view.mu.Unlock()
	flex := isFlexible(apiMetadata, version)
	if version >= 3 {
		e.int32(0) // throttle
	}
	e.arrayLenF(flex, len(view.brokers))
	for _, br := range view.brokers {
		e.int32(br.node)
		e.strF(flex, "127.0.0.1")
		e.int32(br.port())
		e.nullStrF(flex) // rack
		writeTags(e, flex, unknownTags)
	}
	if version >= 2 {
		e.strF(flex, "fake-cluster")
	}
	controller := int32(-1)
	if len(view.brokers) > 0 {
		controller = view.brokers[0].node
	}
	e.int32(controller)
	e.arrayLenF(flex, len(topics))
	for _, topic := range topics {
		var parts []int32
		for tp := range view.leaders {
			if tp.topic == topic {
				parts = append(parts, tp.partition)
			}
		}
		sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
		if len(parts) == 0 {
			e.int16(3) // UNKNOWN_TOPIC_OR_PARTITION
		} else {
			e.int16(0)
		}
		e.strF(flex, topic)
		if version >= 10 {
			e.buf = append(e.buf, bytes.Repeat([]byte{0xab}, 16)...) // topic_id
		}
		e.bool(false) // is_internal
		e.arrayLenF(flex, len(parts))
		for _, p := range parts {
			leader := view.leaders[topicPartition{topic, p}]
			if leader < 0 {
				e.int16(5) // LEADER_NOT_AVAILABLE
			} else {
				e.int16(0)
			}
			e.int32(p)
			e.int32(leader)
			if version >= 7 {
				e.int32(0) // leader_epoch
			}
			e.arrayLenF(flex, 1)
			e.int32(leader) // replicas
			e.arrayLenF(flex, 1)
			e.int32(leader) // isr
			if version >= 5 {
				e.arrayLenF(flex, 0) // offline replicas
			}
			writeTags(e, flex, unknownTags)
		}
		if version >= 8 {
			e.int32(math.MinInt32)
		}
		writeTags(e, flex, unknownTags)
	}
	if version >= 8 && version <= 10 {
		e.int32(math.MinInt32)
	}
	if version >= 13 {
		e.int16(0)
	}
	writeTags(e, flex, unknownTags)
}

func (b *fakeBroker) handleProduce(version int16, corr int32, d *decoder) ([]byte, error) {
	if !b.versions[apiProduce].contains(version) || version < 3 || version > 12 {
		return nil, fmt.Errorf("produce version %d outside fake support", version)
	}
	flex := isFlexible(apiProduce, version)
	if _, null, err := d.nullableStrF(flex); err != nil || !null {
		return nil, fmt.Errorf("transactional id null=%v err=%v", null, err)
	}
	acks, err := d.int16()
	if err != nil {
		return nil, err
	}
	if _, err := d.int32(); err != nil { // timeout
		return nil, err
	}
	nt, err := d.countF(flex, 2, 1<<10)
	if err != nil {
		return nil, err
	}
	type partResp struct {
		index int32
		code  int16
	}
	type topicResp struct {
		name  string
		parts []partResp
	}
	var responses []topicResp
	for range nt {
		name, err := d.strF(flex)
		if err != nil {
			return nil, err
		}
		np, err := d.countF(flex, 5, 1<<16)
		if err != nil {
			return nil, err
		}
		tr := topicResp{name: name}
		seen := make(map[int32]bool)
		for range np {
			index, err := d.int32()
			if err != nil {
				return nil, err
			}
			if seen[index] {
				return nil, fmt.Errorf("partition %d repeated in one request", index)
			}
			seen[index] = true
			var size int
			if flex {
				n, null, err := d.compactLen()
				if err != nil || null {
					return nil, fmt.Errorf("compact records null=%v err=%v", null, err)
				}
				size = n
			} else {
				n, err := d.int32()
				if err != nil {
					return nil, err
				}
				if n < 0 {
					return nil, fmt.Errorf("records size %d", n)
				}
				size = int(n)
			}
			if err := d.need(size); err != nil {
				return nil, err
			}
			raw := d.buf[d.off : d.off+size]
			d.off += size
			meta, recs, err := decodeBatch(raw)
			if err != nil {
				return nil, fmt.Errorf("%s/%d: %w", name, index, err)
			}
			if err := d.tagsF(flex); err != nil {
				return nil, err
			}
			b.mu.Lock()
			b.codecs = append(b.codecs, meta.attributes&7)
			code := int16(0)
			if b.produceError != nil {
				code = b.produceError(name, index, corr)
			}
			if code == 0 {
				for _, r := range recs {
					b.received = append(b.received, received{node: b.node, topic: name, partition: index, decodedRecord: r, corr: corr})
				}
				b.offset += int64(len(recs))
			}
			b.mu.Unlock()
			tr.parts = append(tr.parts, partResp{index, code})
		}
		if err := d.tagsF(flex); err != nil {
			return nil, err
		}
		responses = append(responses, tr)
	}
	if err := d.tagsF(flex); err != nil {
		return nil, err
	}
	if err := d.done(); err != nil {
		return nil, err
	}
	if acks == 0 {
		return nil, nil
	}
	unknown := b.unknownTags
	return b.frame(corr, flex, func(e *encoder) {
		encodeProduceResponse(e, version, unknown, func(add func(topic string, index int32, code int16)) {
			for _, tr := range responses {
				for _, p := range tr.parts {
					add(tr.name, p.index, p.code)
				}
			}
		})
	}), nil
}

// encodeProduceResponse writes a response for versions 3..12. Partitions are
// grouped by consecutive topic name. A non-zero code carries record_errors and
// error_message from v8 on.
func encodeProduceResponse(e *encoder, version int16, unknownTags bool, each func(add func(topic string, index int32, code int16))) {
	flex := isFlexible(apiProduce, version)
	type part struct {
		index int32
		code  int16
	}
	type topic struct {
		name  string
		parts []part
	}
	var topics []*topic
	each(func(name string, index int32, code int16) {
		if len(topics) == 0 || topics[len(topics)-1].name != name {
			topics = append(topics, &topic{name: name})
		}
		t := topics[len(topics)-1]
		t.parts = append(t.parts, part{index, code})
	})
	e.arrayLenF(flex, len(topics))
	for _, t := range topics {
		e.strF(flex, t.name)
		e.arrayLenF(flex, len(t.parts))
		for _, p := range t.parts {
			e.int32(p.index)
			e.int16(p.code)
			e.int64(0)  // base offset
			e.int64(-1) // log append time
			if version >= 5 {
				e.int64(0) // log start offset
			}
			if version >= 8 {
				if p.code != 0 {
					e.arrayLenF(flex, 1)
					e.int32(0)
					e.strF(flex, "bad record")
					writeTags(e, flex, unknownTags)
					e.strF(flex, "fake error")
				} else {
					e.arrayLenF(flex, 0)
					e.nullStrF(flex)
				}
			}
			writeTags(e, flex, unknownTags)
		}
		writeTags(e, flex, unknownTags)
	}
	e.int32(0) // throttle
	writeTags(e, flex, unknownTags)
}

// --- shared test helpers ---

func newView() *clusterView { return &clusterView{leaders: make(map[topicPartition]int32)} }

func waitUntil(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type errorLog struct {
	mu     sync.Mutex
	events []ErrorEvent
}

func (l *errorLog) handler(ev ErrorEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, ev)
}

func (l *errorLog) has(target error) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ev := range l.events {
		if errors.Is(ev.Err, target) {
			return true
		}
	}
	return false
}

func (l *errorLog) find(pred func(ErrorEvent) bool) (ErrorEvent, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ev := range l.events {
		if pred(ev) {
			return ev, true
		}
	}
	return ErrorEvent{}, false
}
