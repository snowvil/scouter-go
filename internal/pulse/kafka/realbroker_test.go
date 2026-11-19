package kafka

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestRealBrokerProduce sends records to an isolated real broker started by
// scripts/kafka-local.py and writes the expected records as JSON lines for an
// independent Java consumer (scripts/kafka-local.py consume + compare) to
// check.
//
// It needs SCOUTER_KAFKA_BOOTSTRAP, SCOUTER_KAFKA_TOPIC and
// SCOUTER_KAFKA_EXPECT_OUT; without them it is skipped, and a skip is not a
// pass. Optional: SCOUTER_KAFKA_ACKS (0, 1, -1), SCOUTER_KAFKA_COMPRESSION
// (none, lz4; default lz4) and SCOUTER_KAFKA_NONFLEXIBLE=1, which caps the
// client at the non-flexible versions (Produce 8, Metadata 8, ApiVersions 0)
// to exercise the path used with brokers older than 2.4. The negotiated
// versions are asserted and logged. The validation record states when it
// actually ran.
func TestRealBrokerProduce(t *testing.T) {
	bootstrap := os.Getenv("SCOUTER_KAFKA_BOOTSTRAP")
	topic := os.Getenv("SCOUTER_KAFKA_TOPIC")
	out := os.Getenv("SCOUTER_KAFKA_EXPECT_OUT")
	if bootstrap == "" || topic == "" || out == "" {
		t.Skip("real broker not configured (SCOUTER_KAFKA_BOOTSTRAP/TOPIC/EXPECT_OUT)")
	}
	acks := int16(0)
	if v := os.Getenv("SCOUTER_KAFKA_ACKS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatal(err)
		}
		acks = int16(n)
	}
	compression, err := ParseCompression(os.Getenv("SCOUTER_KAFKA_COMPRESSION"))
	if os.Getenv("SCOUTER_KAFKA_COMPRESSION") == "" {
		compression, err = CompressionLZ4, nil
	}
	if err != nil {
		t.Fatal(err)
	}
	nonFlexible := os.Getenv("SCOUTER_KAFKA_NONFLEXIBLE") == "1"
	if nonFlexible {
		saved := supportedVersions
		supportedVersions = map[int16]versionRange{apiProduce: {3, 8}, apiMetadata: {1, 8}, apiApiVersions: {0, 0}}
		t.Cleanup(func() { supportedVersions = saved })
	}

	// Negotiation as the producer does it on every connection.
	cfg, err := Config{Bootstrap: []string{bootstrap}, Acks: acks, Compression: compression}.normalized()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := dialBroker(context.Background(), &cfg, bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	conn.close()
	t.Logf("broker ranges produce=%v metadata=%v apiversions=%v; negotiated ApiVersions v%d Metadata v%d Produce v%d",
		conn.versions[apiProduce], conn.versions[apiMetadata], conn.versions[apiApiVersions], conn.apiVersionsVersion, conn.metadataVersion, conn.produceVersion)
	flexible := isFlexible(apiProduce, conn.produceVersion) && isFlexible(apiMetadata, conn.metadataVersion) && conn.apiVersionsVersion == 3
	if nonFlexible == flexible || isFlexible(apiProduce, conn.produceVersion) != isFlexible(apiMetadata, conn.metadataVersion) {
		t.Fatalf("negotiated versions do not match nonflexible=%v", nonFlexible)
	}

	type expect struct {
		Partition int32               `json:"partition"`
		KeyHex    string              `json:"key_hex"`
		ValueHex  string              `json:"value_hex"`
		Headers   []map[string]string `json:"headers"`
	}
	var log errorLog
	p, err := NewProducer(Config{Bootstrap: []string{bootstrap}, Acks: acks, Compression: compression, Linger: 20 * time.Millisecond, OnError: log.handler})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var expected []expect
	add := func(rec Record) {
		t.Helper()
		if err := p.Enqueue(ctx, rec); err != nil {
			t.Fatal(err)
		}
		e := expect{Partition: rec.Partition, KeyHex: hex.EncodeToString(rec.Key), ValueHex: hex.EncodeToString(rec.Value)}
		for _, h := range rec.Headers {
			e.Headers = append(e.Headers, map[string]string{"key": h.Key, "value_hex": hex.EncodeToString(h.Value)})
		}
		expected = append(expected, e)
	}
	// Pulse-shaped records: int64 txid key (big-endian), three UTF-8 headers in
	// contract order, JSON value; partitions spread like floorMod over 30.
	for i := 0; i < 90; i++ {
		txid := int64(-1_234_567_890_123) + int64(i)*7_777_777_777
		key := binary.BigEndian.AppendUint64(nil, uint64(txid))
		value := []byte(fmt.Sprintf(`{"traceId":%d,"seq":%d,"text":"한글 \"quote\" \\ /"}`, txid, i))
		add(Record{Topic: topic, Partition: int32(i % 30), Key: key, Value: value, Headers: []Header{
			{Key: "apm_type", Value: []byte("SCOUTER")},
			{Key: "xlog_serializer_version", Value: []byte("V1")},
			{Key: "xlog_application", Value: []byte(fmt.Sprintf("app-%d", i%4))},
		}})
	}
	// Profile-sized records: each is its own batch spanning several 64KiB LZ4
	// blocks; one is incompressible and must arrive as an uncompressed batch.
	r := rand.New(rand.NewSource(42))
	for i := 0; i < 6; i++ {
		value := []byte(strings.Repeat(fmt.Sprintf(`{"step":%d,"sql":"select * from t where id = ?"},`, i), 3000))
		if i == 5 {
			value = make([]byte, 150_000)
			r.Read(value)
		}
		add(Record{Topic: topic, Partition: int32(i), Key: binary.BigEndian.AppendUint64(nil, uint64(i)), Value: value,
			Headers: []Header{{Key: "apm_type", Value: []byte("SCOUTER")}}})
	}
	if err = p.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	abandoned, err := p.Close(ctx)
	if err != nil || abandoned != 0 {
		t.Fatal(abandoned, err)
	}
	st := p.Stats()
	if _, failed := log.find(func(ErrorEvent) bool { return true }); st.Enqueued != 96 || st.SentRecords != 96 || st.Failed != 0 || st.Dropped != 0 || failed {
		t.Fatalf("stats %+v errors %+v", st, log.events)
	}
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, e := range expected {
		if err = enc.Encode(e); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("acks=%d compression=%s stats=%+v", acks, compression, st)
}

// TestRealBrokerUnsupportedApiVersions checks the assumption behind the
// ApiVersions v3 -> v0 fallback on a real broker: an ApiVersions request above
// the broker's range is answered in the v0 layout with UNSUPPORTED_VERSION and
// the broker keeps the connection usable for the v0 retry.
func TestRealBrokerUnsupportedApiVersions(t *testing.T) {
	bootstrap := os.Getenv("SCOUTER_KAFKA_BOOTSTRAP")
	if bootstrap == "" {
		t.Skip("real broker not configured (SCOUTER_KAFKA_BOOTSTRAP)")
	}
	cfg, err := Config{Bootstrap: []string{bootstrap}}.normalized()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := (&net.Dialer{Timeout: cfg.DialTimeout}).Dial("tcp", bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	c := &brokerConn{addr: bootstrap, conn: raw, cfg: &cfg}
	defer c.close()
	const tooNew = 99
	body, err := c.exchange(apiApiVersions, tooNew, func(e *encoder) { encodeApiVersionsRequest(e, 3) }, true)
	if err != nil {
		t.Fatal(err)
	}
	d := &decoder{buf: body, limits: cfg.Limits}
	code, err := d.int16()
	if err != nil || code != errUnsupportedVersionCode {
		t.Fatalf("error code %d %v", code, err)
	}
	n, err := d.count(6, cfg.Limits.MaxAPIKeys)
	if err != nil {
		t.Fatal(err)
	}
	var own versionRange
	for range n {
		key, _ := d.int16()
		lo, _ := d.int16()
		hi, err := d.int16()
		if err != nil {
			t.Fatal(err)
		}
		if key == apiApiVersions {
			own = versionRange{lo, hi}
		}
	}
	if err := d.done(); err != nil {
		t.Fatalf("v0 layout: %v", err)
	}
	body, err = c.exchange(apiApiVersions, 0, func(*encoder) {}, true)
	if err != nil {
		t.Fatal(err)
	}
	versions, err := parseApiVersionsResponse(0, body, cfg.Limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("v%d -> UNSUPPORTED_VERSION in v0 layout, %d entries, ApiVersions range %v; v0 retry on same connection: %d api keys", tooNew, n, own, len(versions))
}
