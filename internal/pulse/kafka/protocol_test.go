package kafka

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestNegotiate(t *testing.T) {
	broker := map[int16]versionRange{apiProduce: {3, 12}, apiMetadata: {4, 13}}
	if v, err := negotiate(broker, apiProduce); err != nil || v != 12 {
		t.Fatalf("produce %d %v", v, err)
	}
	if v, err := negotiate(broker, apiMetadata); err != nil || v != 13 {
		t.Fatalf("metadata %d %v", v, err)
	}
	// a newer broker: the highest common version is ours
	if v, err := negotiate(map[int16]versionRange{apiProduce: {3, 20}}, apiProduce); err != nil || v != 12 {
		t.Fatalf("newer broker %d %v", v, err)
	}
	// an older broker: non-flexible versions are still used
	if v, err := negotiate(map[int16]versionRange{apiProduce: {0, 7}}, apiProduce); err != nil || v != 7 {
		t.Fatalf("older broker %d %v", v, err)
	}
	if _, err := negotiate(map[int16]versionRange{apiProduce: {13, 14}}, apiProduce); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("newer-only: %v", err)
	}
	if _, err := negotiate(map[int16]versionRange{apiProduce: {0, 2}}, apiProduce); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("too old: %v", err)
	}
	if _, err := negotiate(map[int16]versionRange{}, apiMetadata); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("missing: %v", err)
	}
}

func TestFlexibleVersionBoundaries(t *testing.T) {
	for _, tc := range []struct {
		key, version int16
		flex, header bool
	}{
		{apiProduce, 8, false, false}, {apiProduce, 9, true, true}, {apiProduce, 12, true, true},
		{apiMetadata, 8, false, false}, {apiMetadata, 9, true, true}, {apiMetadata, 13, true, true},
		{apiApiVersions, 0, false, false}, {apiApiVersions, 3, true, false},
	} {
		if isFlexible(tc.key, tc.version) != tc.flex || responseHeaderFlexible(tc.key, tc.version) != tc.header {
			t.Errorf("api %d v%d", tc.key, tc.version)
		}
	}
	e := &encoder{}
	encodeRequestHeader(e, apiMetadata, 9, 7, "c")
	want := []byte{0, 3, 0, 9, 0, 0, 0, 7, 0, 1, 'c', 0}
	if !bytes.Equal(e.buf, want) {
		t.Fatalf("header v2 %x want %x", e.buf, want)
	}
	e = &encoder{}
	encodeApiVersionsRequest(e, 3)
	want = append([]byte{byte(len(clientSoftwareName) + 1)}, clientSoftwareName...)
	want = append(append(want, byte(len(clientSoftwareVersion)+1)), clientSoftwareVersion...)
	want = append(want, 0)
	if !bytes.Equal(e.buf, want) {
		t.Fatalf("api versions v3 body %x want %x", e.buf, want)
	}
}

func TestCompactPrimitives(t *testing.T) {
	e := &encoder{}
	e.compactStr("ab")
	e.compactNull()
	e.compactArrayLen(200) // 201 = 0xc9 0x01
	e.compactBytes([]byte{9})
	e.emptyTags()
	want := []byte{3, 'a', 'b', 0, 0xc9, 0x01, 2, 9, 0}
	if !bytes.Equal(e.buf, want) {
		t.Fatalf("%x want %x", e.buf, want)
	}
	d := &decoder{buf: want, limits: defaultLimits}
	if s, err := d.compactStr(); err != nil || s != "ab" {
		t.Fatal(s, err)
	}
	if _, null, err := d.compactNullableStr(); err != nil || !null {
		t.Fatal(null, err)
	}
	if _, err := d.compactCount(0, 1000); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		in   []byte
		want error
	}{
		{[]byte{0x80, 0x80, 0x80, 0x80, 0x10}, ErrMalformed}, // > 32 bits
		{[]byte{0x80, 0x80}, ErrMalformed},                   // truncated
		{[]byte{0}, ErrMalformed},                            // null where non-nullable
	} {
		d := &decoder{buf: tc.in, limits: defaultLimits}
		if _, err := d.compactStr(); !errors.Is(err, tc.want) {
			t.Errorf("%x: %v", tc.in, err)
		}
	}
	// max 5-byte varint (2^32-1) is accepted by uvarint
	d = &decoder{buf: []byte{0xff, 0xff, 0xff, 0xff, 0x0f}, limits: defaultLimits}
	if v, err := d.uvarint(); err != nil || v != 1<<32-1 {
		t.Fatal(v, err)
	}
	// string longer than MaxStringBytes is a limit error before allocation
	big := &encoder{}
	big.uvarint(uint32(defaultLimits.MaxStringBytes) + 2)
	d = &decoder{buf: big.buf, limits: defaultLimits}
	if _, err := d.compactStr(); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	// array count beyond remaining bytes
	arr := &encoder{}
	arr.compactArrayLen(100)
	d = &decoder{buf: arr.buf, limits: defaultLimits}
	if _, err := d.compactCount(4, 1000); !errors.Is(err, ErrMalformed) {
		t.Fatal(err)
	}
	d = &decoder{buf: arr.buf, limits: defaultLimits}
	if _, err := d.compactCount(4, 10); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
}

func TestSkipTags(t *testing.T) {
	e := &encoder{}
	writeTags(e, true, true)
	e.int8(42)
	d := &decoder{buf: e.buf, limits: defaultLimits}
	if err := d.skipTags(); err != nil {
		t.Fatal(err)
	}
	if v, _ := d.int8(); v != 42 || d.remaining() != 0 {
		t.Fatalf("tags not fully skipped: %d %d", v, d.remaining())
	}
	for name, tc := range map[string]struct {
		in   []byte
		want error
	}{
		"size beyond remaining": {[]byte{1, 0, 10, 1, 2}, ErrMalformed},
		"count beyond bytes":    {[]byte{5, 0, 0}, ErrMalformed},
		"count limit":           {append([]byte{65}, make([]byte, 200)...), ErrLimit},
		"truncated tag":         {[]byte{1, 0x80}, ErrMalformed},
	} {
		d := &decoder{buf: tc.in, limits: defaultLimits}
		if err := d.skipTags(); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func apiVersionsBody(version int16, unknownTags bool, ranges map[int16]versionRange) []byte {
	flex := version >= 3
	e := &encoder{}
	e.int16(0)
	e.arrayLenF(flex, len(ranges))
	for _, k := range []int16{apiProduce, apiMetadata, apiApiVersions} {
		r, ok := ranges[k]
		if !ok {
			continue
		}
		e.int16(k)
		e.int16(r.min)
		e.int16(r.max)
		writeTags(e, flex, unknownTags)
	}
	if flex {
		e.int32(0)
		writeTags(e, flex, unknownTags)
	}
	return e.buf
}

func TestApiVersionsResponseParsing(t *testing.T) {
	ranges := map[int16]versionRange{apiProduce: {0, 9}, apiMetadata: {1, 12}}
	for _, v := range []int16{0, 3} {
		for _, unknown := range []bool{false, true} {
			if v == 0 && unknown {
				continue
			}
			body := apiVersionsBody(v, unknown, ranges)
			got, err := parseApiVersionsResponse(v, body, defaultLimits)
			if err != nil || got[apiProduce] != (versionRange{0, 9}) || got[apiMetadata] != (versionRange{1, 12}) {
				t.Fatalf("v%d unknown=%v: %v %v", v, unknown, got, err)
			}
			if _, err := parseApiVersionsResponse(v, append(append([]byte{}, body...), 0), defaultLimits); !errors.Is(err, ErrMalformed) {
				t.Fatalf("v%d trailing: %v", v, err)
			}
			if _, err := parseApiVersionsResponse(v, body[:len(body)-1], defaultLimits); !errors.Is(err, ErrMalformed) {
				t.Fatalf("v%d truncated: %v", v, err)
			}
			limits := defaultLimits
			limits.MaxAPIKeys = 1
			if _, err := parseApiVersionsResponse(v, body, limits); !errors.Is(err, ErrLimit) {
				t.Fatalf("v%d api key limit: %v", v, err)
			}
		}
	}
	errResp := &encoder{}
	errResp.int16(35)
	errResp.int32(0)
	var kerr Error
	if _, err := parseApiVersionsResponse(3, errResp.buf, defaultLimits); !errors.As(err, &kerr) || kerr.Code != 35 {
		t.Fatalf("error code: %v", err)
	}
	huge := &encoder{}
	huge.int16(0)
	huge.int32(1 << 30)
	if _, err := parseApiVersionsResponse(0, huge.buf, defaultLimits); !errors.Is(err, ErrLimit) {
		t.Fatalf("huge count: %v", err)
	}
	limitsWide := defaultLimits
	limitsWide.MaxAPIKeys = 1 << 30
	if _, err := parseApiVersionsResponse(0, huge.buf, limitsWide); !errors.Is(err, ErrMalformed) {
		t.Fatalf("huge count beyond bytes: %v", err)
	}
	if _, err := parseApiVersionsResponse(1, nil, defaultLimits); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("v1: %v", err)
	}
}

func sampleView(t *testing.T) *clusterView {
	t.Helper()
	view := newView()
	startFakeBroker(t, view, 7)
	startFakeBroker(t, view, 9)
	view.setLeader("meta", 0, 7)
	view.setLeader("meta", 1, 9)
	view.setLeader("meta", 2, -1)
	return view
}

func TestMetadataRequestEncoding(t *testing.T) {
	for v := int16(1); v <= 13; v++ {
		e := &encoder{}
		encodeMetadataRequest(e, v, []string{"ab"})
		flex := v >= 9
		d := &decoder{buf: e.buf, limits: defaultLimits}
		n, err := d.countF(flex, 1, 10)
		if err != nil || n != 1 {
			t.Fatalf("v%d count %d %v", v, n, err)
		}
		if v >= 10 {
			if !bytes.Equal(d.buf[d.off:d.off+16], make([]byte, 16)) {
				t.Fatalf("v%d topic id not zero", v)
			}
			d.off += 16
		}
		if s, err := d.strF(flex); err != nil || s != "ab" {
			t.Fatalf("v%d name %q %v", v, s, err)
		}
		if err := d.tagsF(flex); err != nil {
			t.Fatal(err)
		}
		bools := map[bool]int{true: 1}[v >= 4] + map[bool]int{true: 1}[v >= 8 && v <= 10] + map[bool]int{true: 1}[v >= 8]
		if v >= 4 {
			if b, _ := d.bool(); !b {
				t.Fatalf("v%d allow_auto_topic_creation false", v)
			}
			bools--
		}
		for range bools {
			if b, _ := d.bool(); b {
				t.Fatalf("v%d authorized operations requested", v)
			}
		}
		if err := d.tagsF(flex); err != nil || d.remaining() != 0 {
			t.Fatalf("v%d tail %v %d", v, err, d.remaining())
		}
	}
}

func TestMetadataResponseParsing(t *testing.T) {
	view := sampleView(t)
	for v := int16(1); v <= 13; v++ {
		for _, unknown := range []bool{false, true} {
			if v < 9 && unknown {
				continue
			}
			e := &encoder{}
			encodeMetadataResponse(e, v, view, []string{"meta", "none"}, unknown)
			got, err := parseMetadataResponse(v, e.buf, defaultLimits)
			if err != nil {
				t.Fatalf("v%d: %v", v, err)
			}
			if len(got.Brokers) != 2 || got.Brokers[0].NodeID != 7 || got.Brokers[0].Host != "127.0.0.1" || got.Brokers[1].Port != view.brokers[1].port() {
				t.Fatalf("v%d brokers %+v", v, got.Brokers)
			}
			if len(got.Topics) != 2 || got.Topics[0].Name != "meta" || len(got.Topics[0].Partitions) != 3 || got.Topics[1].ErrorCode != 3 {
				t.Fatalf("v%d topics %+v", v, got.Topics)
			}
			if p := got.Topics[0].Partitions; p[0].Leader != 7 || p[1].Leader != 9 || p[2].Leader != -1 || p[2].ErrorCode != 5 {
				t.Fatalf("v%d partitions %+v", v, p)
			}
			if _, err := parseMetadataResponse(v, e.buf[:len(e.buf)-1], defaultLimits); !errors.Is(err, ErrMalformed) {
				t.Fatalf("v%d truncated: %v", v, err)
			}
			if _, err := parseMetadataResponse(v, append(append([]byte{}, e.buf...), 0), defaultLimits); !errors.Is(err, ErrMalformed) {
				t.Fatalf("v%d trailing: %v", v, err)
			}
			limits := defaultLimits
			limits.MaxPartitions = 2
			if _, err := parseMetadataResponse(v, e.buf, limits); !errors.Is(err, ErrLimit) {
				t.Fatalf("v%d partition limit: %v", v, err)
			}
			limits = defaultLimits
			limits.MaxBrokers = 1
			if _, err := parseMetadataResponse(v, e.buf, limits); !errors.Is(err, ErrLimit) {
				t.Fatalf("v%d broker limit: %v", v, err)
			}
			if unknown {
				limits = defaultLimits
				limits.MaxTaggedFields = 1
				if _, err := parseMetadataResponse(v, e.buf, limits); !errors.Is(err, ErrLimit) {
					t.Fatalf("v%d tagged field limit: %v", v, err)
				}
			}
		}
	}
	if _, err := parseMetadataResponse(0, nil, defaultLimits); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("v0: %v", err)
	}
	if _, err := parseMetadataResponse(14, nil, defaultLimits); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("v14: %v", err)
	}
	// v13 top-level error code is returned as a broker error
	e := &encoder{}
	encodeMetadataResponse(e, 13, view, []string{"meta"}, false)
	e.buf[len(e.buf)-2] = 5 // error_code low byte (before the empty tags byte)
	var kerr Error
	if _, err := parseMetadataResponse(13, e.buf, defaultLimits); !errors.As(err, &kerr) || kerr.Code == 0 {
		t.Fatalf("v13 top-level error: %v", err)
	}
	// A broker entry with port 0 or empty host is rejected before use.
	bad := &encoder{}
	bad.int32(1)
	bad.int32(1)
	bad.str("")
	bad.int32(9092)
	bad.nullableStr("", true)
	bad.int32(1)
	bad.int32(0)
	if _, err := parseMetadataResponse(1, bad.buf, defaultLimits); !errors.Is(err, ErrMalformed) {
		t.Fatalf("empty host: %v", err)
	}
}

func TestProduceRequestEncodingAndResponseParsing(t *testing.T) {
	b1 := newBatch(topicPartition{"t", 0}, time.Now())
	b1.appendRecord(encodeRecord(nil, Record{Topic: "t", Value: []byte("a")}, 0, 0), 5)
	b2 := newBatch(topicPartition{"u", 4}, time.Now())
	b2.appendRecord(encodeRecord(nil, Record{Topic: "u", Value: []byte("b")}, 0, 0), 5)
	b3 := newBatch(topicPartition{"t", 1}, time.Now())
	b3.appendRecord(encodeRecord(nil, Record{Topic: "t", Value: []byte("c")}, 0, 0), 5)
	for _, v := range []int16{3, 8, 9, 12} {
		flex := v >= 9
		e := &encoder{}
		encodeProduceRequest(e, v, -1, 1234, []*batch{b1, b2, b3}, newBatchWriter(CompressionNone))
		d := &decoder{buf: e.buf, limits: defaultLimits}
		if _, null, err := d.nullableStrF(flex); err != nil || !null {
			t.Fatalf("v%d transactional id null=%v err=%v", v, null, err)
		}
		if acks, _ := d.int16(); acks != -1 {
			t.Fatalf("acks %d", acks)
		}
		if timeout, _ := d.int32(); timeout != 1234 {
			t.Fatalf("timeout %d", timeout)
		}
		nt, _ := d.countF(flex, 1, 10)
		if nt != 2 {
			t.Fatalf("v%d topics %d", v, nt)
		}
		name, _ := d.strF(flex)
		np, _ := d.countF(flex, 1, 10)
		if name != "t" || np != 2 {
			t.Fatalf("v%d first topic %s/%d partitions", v, name, np)
		}
		idx, _ := d.int32()
		var size int
		if flex {
			size, _, _ = d.compactLen()
		} else {
			n, _ := d.int32()
			size = int(n)
		}
		if idx != 0 || size != b1.encodedSize() {
			t.Fatalf("v%d partition %d size %d", v, idx, size)
		}
		if !bytes.Equal(d.buf[d.off:d.off+size], b1.appendEncoded(nil, nil)) {
			t.Fatalf("v%d batch bytes differ", v)
		}
	}

	for v := int16(3); v <= 12; v++ {
		for _, unknown := range []bool{false, true} {
			if v < 9 && unknown {
				continue
			}
			r := &encoder{}
			encodeProduceResponse(r, v, unknown, func(add func(string, int32, int16)) {
				add("t", 0, 0)
				add("t", 1, 6)
			})
			got, err := parseProduceResponse(v, r.buf, defaultLimits)
			if err != nil || len(got) != 2 || got[0].ErrorCode != 0 || got[1].ErrorCode != 6 || got[1].Index != 1 || got[1].Topic != "t" {
				t.Fatalf("v%d: %+v %v", v, got, err)
			}
			if wantMsg := map[bool]string{true: "fake error"}[v >= 8]; got[1].ErrorMessage != wantMsg || got[0].ErrorMessage != "" {
				t.Fatalf("v%d error message %q", v, got[1].ErrorMessage)
			}
			if _, err := parseProduceResponse(v, r.buf[:len(r.buf)-2], defaultLimits); !errors.Is(err, ErrMalformed) {
				t.Fatalf("v%d truncated: %v", v, err)
			}
			if _, err := parseProduceResponse(v, append(append([]byte{}, r.buf...), 1), defaultLimits); !errors.Is(err, ErrMalformed) {
				t.Fatalf("v%d trailing: %v", v, err)
			}
		}
	}
	if _, err := parseProduceResponse(13, nil, defaultLimits); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("v13: %v", err)
	}
}

func TestErrorStrings(t *testing.T) {
	if s := (Error{Code: 6}).Error(); s != "kafka: broker error 6 NOT_LEADER_OR_FOLLOWER" {
		t.Fatal(s)
	}
	if s := (Error{Code: 200}).Error(); s != "kafka: broker error code 200" {
		t.Fatal(s)
	}
	if !(Error{Code: 6}).refreshesMetadata() || (Error{Code: 19}).refreshesMetadata() {
		t.Fatal("refresh classification")
	}
}

func fuzzLimits() Limits {
	limits := defaultLimits
	limits.MaxBrokers, limits.MaxTopics, limits.MaxPartitions, limits.MaxStringBytes = 64, 64, 64, 1024
	return limits
}

func checkErrorClass(t *testing.T, err error) {
	t.Helper()
	var kerr Error
	if err != nil && !errors.Is(err, ErrMalformed) && !errors.Is(err, ErrLimit) && !errors.As(err, &kerr) {
		t.Fatalf("unexpected error class: %v", err)
	}
}

// FuzzMetadataResponse covers versions 1..13 (flexible from 9).
func FuzzMetadataResponse(f *testing.F) {
	view := newView()
	view.brokers = []*fakeBroker{{node: 1, portNum: 9092}, {node: 2, portNum: 9093}}
	view.setLeader("a", 0, 1)
	view.setLeader("a", 1, 2)
	view.setLeader("b", 0, -1)
	for v := int16(1); v <= 13; v++ {
		e := &encoder{}
		encodeMetadataResponse(e, v, view, []string{"a", "b", "c"}, v >= 9)
		f.Add(byte(v), e.buf)
	}
	f.Add(byte(3), []byte{})
	limits := fuzzLimits()
	f.Fuzz(func(t *testing.T, version byte, body []byte) {
		v := int16(version%13) + 1
		got, err := parseMetadataResponse(v, body, limits)
		if err != nil {
			checkErrorClass(t, err)
			return
		}
		if len(got.Brokers) > limits.MaxBrokers || len(got.Topics) > limits.MaxTopics {
			t.Fatalf("limits not enforced: %d brokers %d topics", len(got.Brokers), len(got.Topics))
		}
		for _, tp := range got.Topics {
			if len(tp.Partitions) > limits.MaxPartitions {
				t.Fatalf("partition limit not enforced")
			}
		}
	})
}

// FuzzProduceResponse covers versions 3..12 (flexible from 9).
func FuzzProduceResponse(f *testing.F) {
	for v := int16(3); v <= 12; v++ {
		r := &encoder{}
		encodeProduceResponse(r, v, v >= 9, func(add func(string, int32, int16)) {
			add("t", 0, 0)
			add("t", 1, 6)
		})
		f.Add(byte(v), r.buf)
	}
	limits := fuzzLimits()
	f.Fuzz(func(t *testing.T, version byte, body []byte) {
		v := int16(version%10) + 3
		got, err := parseProduceResponse(v, body, limits)
		checkErrorClass(t, err)
		if err == nil && len(got) > limits.MaxTopics*limits.MaxPartitions {
			t.Fatalf("limits not enforced: %d results", len(got))
		}
	})
}

// FuzzApiVersionsResponse covers versions 0 and 3.
func FuzzApiVersionsResponse(f *testing.F) {
	f.Add(false, []byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 9})
	f.Add(true, apiVersionsBody(3, true, map[int16]versionRange{apiProduce: {3, 12}, apiMetadata: {0, 13}}))
	f.Fuzz(func(t *testing.T, flex bool, body []byte) {
		v := int16(0)
		if flex {
			v = 3
		}
		got, err := parseApiVersionsResponse(v, body, defaultLimits)
		checkErrorClass(t, err)
		if err == nil && len(got) > defaultLimits.MaxAPIKeys {
			t.Fatalf("api key limit not enforced")
		}
	})
}

// FuzzSkipTags checks that any tagged field section is either skipped within
// bounds or rejected with a classified error.
func FuzzSkipTags(f *testing.F) {
	e := &encoder{}
	writeTags(e, true, true)
	f.Add(e.buf)
	f.Add([]byte{0})
	f.Fuzz(func(t *testing.T, body []byte) {
		d := &decoder{buf: body, limits: defaultLimits}
		err := d.skipTags()
		checkErrorClass(t, err)
		if d.off > len(body) {
			t.Fatalf("offset %d beyond %d", d.off, len(body))
		}
	})
}
