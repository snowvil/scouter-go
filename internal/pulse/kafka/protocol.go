package kafka

import (
	"fmt"
	"math"
)

// Kafka protocol API keys used by the producer.
const (
	apiProduce     int16 = 0
	apiMetadata    int16 = 3
	apiApiVersions int16 = 18
)

type versionRange struct{ min, max int16 }

func (r versionRange) contains(v int16) bool { return v >= r.min && v <= r.max }

// supportedVersions is what this implementation speaks. The field layouts are
// those of kafka-clients 4.0.0 (the Pulse plugin's client, see
// docs/compatibility/baseline.json): Produce 3..12, Metadata 1..13 and
// ApiVersions 0 and 3 (v1/v2 are never sent).
var supportedVersions = map[int16]versionRange{
	apiProduce:     {3, 12},
	apiMetadata:    {1, 13},
	apiApiVersions: {0, 3},
}

// flexibleFrom is the first flexible version (KIP-482: request header v2,
// compact strings/arrays, tagged fields) of each api.
var flexibleFrom = map[int16]int16{
	apiProduce:     9,
	apiMetadata:    9,
	apiApiVersions: 3,
}

func isFlexible(key, version int16) bool {
	from, ok := flexibleFrom[key]
	return ok && version >= from
}

// responseHeaderFlexible reports whether the response header carries a tagged
// field section (header v1). ApiVersions responses always use header v0 so a
// client can read the error of an unsupported request version (KIP-511).
func responseHeaderFlexible(key, version int16) bool {
	return key != apiApiVersions && isFlexible(key, version)
}

// negotiate picks the highest version both sides support.
func negotiate(broker map[int16]versionRange, key int16) (int16, error) {
	ours, ok := supportedVersions[key]
	if !ok {
		return 0, fmt.Errorf("%w: api key %d", ErrUnsupportedVersion, key)
	}
	theirs, ok := broker[key]
	if !ok {
		return 0, fmt.Errorf("%w: broker does not advertise api key %d", ErrUnsupportedVersion, key)
	}
	v := min(ours.max, theirs.max)
	if v < ours.min || v < theirs.min {
		return 0, fmt.Errorf("%w: api key %d broker [%d,%d] client [%d,%d]", ErrUnsupportedVersion, key, theirs.min, theirs.max, ours.min, ours.max)
	}
	return v, nil
}

// Error is a Kafka protocol error code returned by a broker.
type Error struct{ Code int16 }

const errUnsupportedVersionCode int16 = 35

var errorNames = map[int16]string{
	-1: "UNKNOWN_SERVER_ERROR",
	1:  "OFFSET_OUT_OF_RANGE",
	2:  "CORRUPT_MESSAGE",
	3:  "UNKNOWN_TOPIC_OR_PARTITION",
	4:  "INVALID_FETCH_SIZE",
	5:  "LEADER_NOT_AVAILABLE",
	6:  "NOT_LEADER_OR_FOLLOWER",
	7:  "REQUEST_TIMED_OUT",
	8:  "BROKER_NOT_AVAILABLE",
	9:  "REPLICA_NOT_AVAILABLE",
	10: "MESSAGE_TOO_LARGE",
	17: "INVALID_TOPIC_EXCEPTION",
	18: "RECORD_LIST_TOO_LARGE",
	19: "NOT_ENOUGH_REPLICAS",
	20: "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
	29: "TOPIC_AUTHORIZATION_FAILED",
	31: "CLUSTER_AUTHORIZATION_FAILED",
	32: "INVALID_TIMESTAMP",
	35: "UNSUPPORTED_VERSION",
	42: "INVALID_REQUEST",
	43: "UNSUPPORTED_FOR_MESSAGE_FORMAT",
	73: "KAFKA_STORAGE_ERROR",
	76: "UNSUPPORTED_COMPRESSION_TYPE",
	87: "INVALID_RECORD",
}

func (e Error) Error() string {
	if name, ok := errorNames[e.Code]; ok {
		return fmt.Sprintf("kafka: broker error %d %s", e.Code, name)
	}
	return fmt.Sprintf("kafka: broker error code %d", e.Code)
}

// refreshesMetadata reports codes after which the cached leader map is suspect.
func (e Error) refreshesMetadata() bool {
	switch e.Code {
	case 3, 5, 6, 7, 8, 9, 73:
		return true
	}
	return false
}

// Version-dependent primitives: flex selects the compact (flexible) form.

func (e *encoder) strF(flex bool, s string) {
	if flex {
		e.compactStr(s)
	} else {
		e.str(s)
	}
}

func (e *encoder) nullStrF(flex bool) {
	if flex {
		e.compactNull()
	} else {
		e.nullableStr("", true)
	}
}

func (e *encoder) arrayLenF(flex bool, n int) {
	if flex {
		e.compactArrayLen(n)
	} else {
		e.int32(int32(n))
	}
}

func (e *encoder) tagsF(flex bool) {
	if flex {
		e.emptyTags()
	}
}

func (d *decoder) strF(flex bool) (string, error) {
	if flex {
		return d.compactStr()
	}
	return d.str()
}

func (d *decoder) nullableStrF(flex bool) (string, bool, error) {
	if flex {
		return d.compactNullableStr()
	}
	return d.nullableStr()
}

func (d *decoder) countF(flex bool, minElem, max int) (int, error) {
	if flex {
		return d.compactCount(minElem, max)
	}
	return d.count(minElem, max)
}

func (d *decoder) int32ArrayF(flex bool, max int) ([]int32, error) {
	if flex {
		return d.compactInt32Array(max)
	}
	return d.int32Array(max)
}

func (d *decoder) tagsF(flex bool) error {
	if flex {
		return d.skipTags()
	}
	return nil
}

// Request header v1 (api_key, api_version, correlation_id, nullable client_id)
// for non-flexible versions; header v2 appends an empty tagged field section.
// client_id stays a non-compact NULLABLE_STRING in v2.
func encodeRequestHeader(e *encoder, key, version int16, correlation int32, clientID string) {
	e.int16(key)
	e.int16(version)
	e.int32(correlation)
	e.nullableStr(clientID, false)
	e.tagsF(isFlexible(key, version))
}

// ClientSoftwareName/Version are sent in ApiVersions v3+ (KIP-511). Brokers
// reject values outside [a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])? with
// INVALID_REQUEST, so they are fixed constants.
const (
	clientSoftwareName    = "scouter-go-kafka"
	clientSoftwareVersion = "dev"
)

// ApiVersions request: v0 has an empty body; v3 is
// client_software_name compact string, client_software_version compact
// string, tagged fields.
func encodeApiVersionsRequest(e *encoder, version int16) {
	if version >= 3 {
		e.compactStr(clientSoftwareName)
		e.compactStr(clientSoftwareVersion)
		e.emptyTags()
	}
}

// parseApiVersionsResponse handles response versions 0 and 3:
//
//	v0: error_code int16, api_keys [api_key int16, min_version int16, max_version int16]
//	v3: error_code int16, api_keys compact [api_key, min_version, max_version, tags],
//	    throttle_time_ms int32, tags (SupportedFeatures etc., skipped)
//
// A broker that does not support the requested version answers in the v0
// layout with UNSUPPORTED_VERSION; that case returns Error{35} without parsing
// the rest so the caller can retry with v0 on the same connection, as the
// Java client does.
func parseApiVersionsResponse(version int16, body []byte, limits Limits) (map[int16]versionRange, error) {
	if version != 0 && version != 3 {
		return nil, fmt.Errorf("%w: api versions response version %d", ErrUnsupportedVersion, version)
	}
	flex := version >= 3
	d := &decoder{buf: body, limits: limits}
	code, err := d.int16()
	if err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, Error{Code: code}
	}
	minEntry := 6
	if flex {
		minEntry = 7
	}
	n, err := d.countF(flex, minEntry, limits.MaxAPIKeys)
	if err != nil {
		return nil, err
	}
	out := make(map[int16]versionRange, n)
	for range n {
		key, err := d.int16()
		if err != nil {
			return nil, err
		}
		lo, err := d.int16()
		if err != nil {
			return nil, err
		}
		hi, err := d.int16()
		if err != nil {
			return nil, err
		}
		if lo < 0 || hi < lo {
			return nil, fmt.Errorf("%w: api key %d version range [%d,%d]", ErrMalformed, key, lo, hi)
		}
		if err = d.tagsF(flex); err != nil {
			return nil, err
		}
		out[key] = versionRange{lo, hi}
	}
	if flex {
		if _, err = d.int32(); err != nil { // throttle_time_ms
			return nil, err
		}
		if err = d.skipTags(); err != nil {
			return nil, err
		}
	}
	// v0 has no throttle_time_ms; trailing bytes mean a mis-negotiated layout.
	return out, d.done()
}

// Metadata request (allow_auto_topic_creation is true like the Java producer;
// authorized-operation flags are false):
//
//	v1..v3:  topics [name string]
//	v4..v7:  + allow_auto_topic_creation bool
//	v8:      + include_cluster_authorized_operations bool, include_topic_authorized_operations bool
//	v9:      compact topics [name compact string, tags], ..., tags
//	v10:     topics [topic_id uuid (zero: look up by name), name compact nullable string, tags]
//	v11..13: include_cluster_authorized_operations removed
func encodeMetadataRequest(e *encoder, version int16, topics []string) {
	flex := isFlexible(apiMetadata, version)
	e.arrayLenF(flex, len(topics))
	for _, t := range topics {
		if version >= 10 {
			e.buf = append(e.buf, make([]byte, 16)...) // Uuid.ZERO_UUID
		}
		e.strF(flex, t)
		e.tagsF(flex)
	}
	if version >= 4 {
		e.bool(true)
	}
	if version >= 8 && version <= 10 {
		e.bool(false) // include_cluster_authorized_operations
	}
	if version >= 8 {
		e.bool(false) // include_topic_authorized_operations
	}
	e.tagsF(flex)
}

type brokerInfo struct {
	NodeID int32
	Host   string
	Port   int32
}
type partitionInfo struct {
	ErrorCode int16
	Index     int32
	Leader    int32
}
type topicInfo struct {
	ErrorCode  int16
	Name       string
	Partitions []partitionInfo
}
type metadataResponse struct {
	Brokers []brokerInfo
	Topics  []topicInfo
}

// parseMetadataResponse handles response versions 1..13:
//
//	throttle_time_ms int32 (v3+)
//	brokers [node_id int32, host string, port int32, rack nullable string (v1+), tags (v9+)]
//	cluster_id nullable string (v2+)
//	controller_id int32 (v1+)
//	topics [error_code int16, name string (nullable v12+), topic_id uuid (v10+),
//	        is_internal bool (v1+),
//	        partitions [error_code int16, partition_index int32, leader_id int32,
//	                    leader_epoch int32 (v7+), replica_nodes []int32,
//	                    isr_nodes []int32, offline_replicas []int32 (v5+), tags (v9+)],
//	        topic_authorized_operations int32 (v8+), tags (v9+)]
//	cluster_authorized_operations int32 (v8..v10)
//	error_code int16 (v13+)
//	tags (v9+)
//
// v9+ use compact strings/arrays. A topic with a null name (v12+, only for
// topic-id lookups, which this producer never makes) is skipped.
func parseMetadataResponse(version int16, body []byte, limits Limits) (*metadataResponse, error) {
	if !supportedVersions[apiMetadata].contains(version) {
		return nil, fmt.Errorf("%w: metadata response version %d", ErrUnsupportedVersion, version)
	}
	flex := isFlexible(apiMetadata, version)
	d := &decoder{buf: body, limits: limits}
	if version >= 3 {
		if _, err := d.int32(); err != nil {
			return nil, err
		}
	}
	minBroker := 4 + 2 + 4 + 2
	if flex {
		minBroker = 4 + 1 + 4 + 1 + 1
	}
	nb, err := d.countF(flex, minBroker, limits.MaxBrokers)
	if err != nil {
		return nil, err
	}
	out := &metadataResponse{Brokers: make([]brokerInfo, 0, nb)}
	for range nb {
		var b brokerInfo
		if b.NodeID, err = d.int32(); err != nil {
			return nil, err
		}
		if b.Host, err = d.strF(flex); err != nil {
			return nil, err
		}
		if b.Port, err = d.int32(); err != nil {
			return nil, err
		}
		if _, _, err = d.nullableStrF(flex); err != nil { // rack
			return nil, err
		}
		if err = d.tagsF(flex); err != nil {
			return nil, err
		}
		if b.Port <= 0 || b.Port > math.MaxUint16 || b.Host == "" {
			return nil, fmt.Errorf("%w: broker %d endpoint %q:%d", ErrMalformed, b.NodeID, b.Host, b.Port)
		}
		out.Brokers = append(out.Brokers, b)
	}
	if version >= 2 {
		if _, _, err = d.nullableStrF(flex); err != nil { // cluster_id
			return nil, err
		}
	}
	if _, err = d.int32(); err != nil { // controller_id
		return nil, err
	}
	minTopic := 2 + 2 + 1 + 4
	if flex {
		minTopic = 2 + 1 + 1 + 1 + 4 + 1
		if version >= 10 {
			minTopic += 16
		}
	}
	nt, err := d.countF(flex, minTopic, limits.MaxTopics)
	if err != nil {
		return nil, err
	}
	minPart := 2 + 4 + 4 + 4 + 4
	if version >= 7 {
		minPart += 4
	}
	if version >= 5 {
		minPart += 4
	}
	if flex {
		minPart = 2 + 4 + 4 + 4 + 1 + 1 + 1 + 1
	}
	out.Topics = make([]topicInfo, 0, nt)
	for range nt {
		var t topicInfo
		if t.ErrorCode, err = d.int16(); err != nil {
			return nil, err
		}
		var nullName bool
		if version >= 12 {
			t.Name, nullName, err = d.nullableStrF(flex)
		} else {
			t.Name, err = d.strF(flex)
		}
		if err != nil {
			return nil, err
		}
		if version >= 10 {
			if err = d.skip(16); err != nil { // topic_id
				return nil, err
			}
		}
		if _, err = d.bool(); err != nil { // is_internal
			return nil, err
		}
		np, err := d.countF(flex, minPart, limits.MaxPartitions)
		if err != nil {
			return nil, err
		}
		t.Partitions = make([]partitionInfo, 0, np)
		for range np {
			var p partitionInfo
			if p.ErrorCode, err = d.int16(); err != nil {
				return nil, err
			}
			if p.Index, err = d.int32(); err != nil {
				return nil, err
			}
			if p.Leader, err = d.int32(); err != nil {
				return nil, err
			}
			if version >= 7 {
				if _, err = d.int32(); err != nil { // leader_epoch
					return nil, err
				}
			}
			if _, err = d.int32ArrayF(flex, limits.MaxBrokers); err != nil { // replicas
				return nil, err
			}
			if _, err = d.int32ArrayF(flex, limits.MaxBrokers); err != nil { // isr
				return nil, err
			}
			if version >= 5 {
				if _, err = d.int32ArrayF(flex, limits.MaxBrokers); err != nil { // offline
					return nil, err
				}
			}
			if err = d.tagsF(flex); err != nil {
				return nil, err
			}
			t.Partitions = append(t.Partitions, p)
		}
		if version >= 8 {
			if _, err = d.int32(); err != nil { // topic_authorized_operations
				return nil, err
			}
		}
		if err = d.tagsF(flex); err != nil {
			return nil, err
		}
		if !nullName {
			out.Topics = append(out.Topics, t)
		}
	}
	if version >= 8 && version <= 10 {
		if _, err = d.int32(); err != nil { // cluster_authorized_operations
			return nil, err
		}
	}
	var topCode int16
	if version >= 13 {
		if topCode, err = d.int16(); err != nil {
			return nil, err
		}
	}
	if err = d.tagsF(flex); err != nil {
		return nil, err
	}
	if err = d.done(); err != nil {
		return nil, err
	}
	if topCode != 0 {
		return nil, Error{Code: topCode}
	}
	return out, nil
}

// Produce request v3..v12:
//
//	transactional_id nullable string, acks int16, timeout_ms int32,
//	topic_data [name string, partition_data [index int32, records nullable bytes, tags (v9+)], tags (v9+)]
//	tags (v9+)
//
// v3..v8 share one non-flexible layout and v9..v12 one flexible layout
// (compact strings/arrays, COMPACT_RECORDS). Each partition carries exactly
// one magic-2 RecordBatch, as v3+ requires. batches are already grouped so
// that no topic/partition repeats. w encodes each batch (and compresses it)
// into its scratch buffer, which is copied into the request immediately.
func encodeProduceRequest(e *encoder, version, acks int16, timeoutMs int32, batches []*batch, w *batchWriter) {
	flex := isFlexible(apiProduce, version)
	e.nullStrF(flex)
	e.int16(acks)
	e.int32(timeoutMs)
	type group struct {
		topic   string
		batches []*batch
	}
	var groups []*group
	index := make(map[string]*group, 2)
	for _, b := range batches {
		g := index[b.tp.topic]
		if g == nil {
			g = &group{topic: b.tp.topic}
			index[b.tp.topic] = g
			groups = append(groups, g)
		}
		g.batches = append(g.batches, b)
	}
	e.arrayLenF(flex, len(groups))
	for _, g := range groups {
		e.strF(flex, g.topic)
		e.arrayLenF(flex, len(g.batches))
		for _, b := range g.batches {
			e.int32(b.tp.partition)
			wire := w.encode(b)
			if flex {
				e.compactBytes(wire)
			} else {
				e.nullableBytes(wire)
			}
			e.tagsF(flex)
		}
		e.tagsF(flex)
	}
	e.tagsF(flex)
}

// batchWriter encodes batches for one connection and is owned by that
// connection's single user. scratch is reused between batches (its capacity
// stays bounded by the largest batch, <= MaxRequestBytes plus one LZ4 block of
// worst-case expansion); the slice returned by encode is valid until the next
// call and is only ever copied synchronously into the request buffer. The
// 16KiB LZ4 table is allocated on the first compressed batch, so metadata-only
// connections never pay for it.
type batchWriter struct {
	compression Compression
	lz          *lz4Compressor
	scratch     []byte
}

func newBatchWriter(c Compression) *batchWriter { return &batchWriter{compression: c} }

func (w *batchWriter) encode(b *batch) []byte {
	if w.compression == CompressionLZ4 && w.lz == nil {
		w.lz = &lz4Compressor{}
	}
	w.scratch = b.appendEncoded(w.scratch[:0], w.lz)
	return w.scratch
}

type partitionResult struct {
	Topic        string
	Index        int32
	ErrorCode    int16
	ErrorMessage string // v8+, empty when null
}

// parseProduceResponse handles response versions 3..12:
//
//	responses [name string, partition_responses [index int32, error_code int16,
//	           base_offset int64, log_append_time_ms int64,
//	           log_start_offset int64 (v5+),
//	           record_errors [batch_index int32, batch_index_error_message nullable string, tags (v9+)] (v8+),
//	           error_message nullable string (v8+), tags (v9+; v10+ CurrentLeader skipped)],
//	           tags (v9+)]
//	throttle_time_ms int32
//	tags (v9+; v10+ NodeEndpoints skipped)
//
// The v10+ leader hints are not used: a NOT_LEADER error refreshes metadata.
func parseProduceResponse(version int16, body []byte, limits Limits) ([]partitionResult, error) {
	if !supportedVersions[apiProduce].contains(version) {
		return nil, fmt.Errorf("%w: produce response version %d", ErrUnsupportedVersion, version)
	}
	flex := isFlexible(apiProduce, version)
	d := &decoder{buf: body, limits: limits}
	minTopic := 2 + 4
	if flex {
		minTopic = 1 + 1 + 1
	}
	nt, err := d.countF(flex, minTopic, limits.MaxTopics)
	if err != nil {
		return nil, err
	}
	minPart := 4 + 2 + 8 + 8
	if version >= 5 {
		minPart += 8
	}
	if version >= 8 {
		if flex {
			minPart += 1 + 1 + 1
		} else {
			minPart += 4 + 2
		}
	}
	minRecordError := 4 + 2
	if flex {
		minRecordError = 4 + 1 + 1
	}
	var out []partitionResult
	for range nt {
		name, err := d.strF(flex)
		if err != nil {
			return nil, err
		}
		np, err := d.countF(flex, minPart, limits.MaxPartitions)
		if err != nil {
			return nil, err
		}
		for range np {
			r := partitionResult{Topic: name}
			if r.Index, err = d.int32(); err != nil {
				return nil, err
			}
			if r.ErrorCode, err = d.int16(); err != nil {
				return nil, err
			}
			if _, err = d.int64(); err != nil { // base_offset
				return nil, err
			}
			if _, err = d.int64(); err != nil { // log_append_time_ms
				return nil, err
			}
			if version >= 5 {
				if _, err = d.int64(); err != nil { // log_start_offset
					return nil, err
				}
			}
			if version >= 8 {
				// record_errors are not kept; the remaining-bytes check bounds the loop.
				ne, err := d.countF(flex, minRecordError, limits.MaxResponseBytes)
				if err != nil {
					return nil, err
				}
				for range ne {
					if _, err = d.int32(); err != nil { // batch_index
						return nil, err
					}
					if _, _, err = d.nullableStrF(flex); err != nil {
						return nil, err
					}
					if err = d.tagsF(flex); err != nil {
						return nil, err
					}
				}
				if r.ErrorMessage, _, err = d.nullableStrF(flex); err != nil {
					return nil, err
				}
			}
			if err = d.tagsF(flex); err != nil {
				return nil, err
			}
			out = append(out, r)
		}
		if err = d.tagsF(flex); err != nil {
			return nil, err
		}
	}
	if _, err = d.int32(); err != nil { // throttle_time_ms
		return nil, err
	}
	if err = d.tagsF(flex); err != nil {
		return nil, err
	}
	return out, d.done()
}
