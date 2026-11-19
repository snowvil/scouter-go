// Package kafka is a minimal Kafka producer wire client built on the standard
// library only. It exists for the Pulse integration (docs/compatibility/
// pulse-contract.md) whose contract is acks=0, retries=0 fire-and-forget.
//
// Protocol scope (Kafka wire protocol, big-endian; field layouts checked
// against kafka-clients 4.0.0, the Pulse plugin's client):
//
//   - ApiVersions on every new connection: v3 (flexible, request header v2,
//     client_software_name/version), falling back to v0 on the same
//     connection when the broker answers UNSUPPORTED_VERSION (brokers before
//     2.4), as the Java NetworkClient does.
//   - Metadata v1..v13 and Produce v3..v12; the highest version both sides
//     advertise is used. Metadata 9+ and Produce 9+ are flexible (KIP-482):
//     request header v2, response header v1, compact strings/arrays/records,
//     tagged field sections. Every tagged field the broker sends is skipped
//     (known ones such as ApiVersions SupportedFeatures and Produce v10+
//     CurrentLeader/NodeEndpoints included); count and size are checked
//     against Limits.MaxTaggedFields and the remaining bytes. Metadata v10+
//     asks by name with the zero topic id; the v13 top-level error code fails
//     the fetch. Produce v8+ error_message is reported with the error code.
//   - One magic-2 RecordBatch per topic partition, CRC32C (Castagnoli) over
//     attributes..end, producerId/producerEpoch/baseSequence = -1.
//   - compression.type: lz4 (default, the plugin's hardcoded value) or none.
//     LZ4 output is the Kafka LZ4 frame of Lz4BlockOutputStream defaults
//     (lz4.go): 64KiB independent blocks, no block/content checksum, header
//     checksum from xxHash32; a block that does not shrink is stored raw.
//     gzip, snappy and zstd are rejected by ParseCompression/Config.
//
// Intended differences from the Java plugin's KafkaProducer, none of which
// changes the record bytes a consumer observes:
//
//   - LZ4 bytes are not identical to lz4-java's (another valid match finder);
//     any LZ4 frame reader restores the same records. A batch whose LZ4 frame
//     is not smaller than its plain records is sent uncompressed (codec 0)
//     instead of as a raw-block LZ4 frame, so the wire size never exceeds the
//     uncompressed size that BufferBytes and MaxRequestBytes account for.
//   - Compression runs on the broker worker goroutine when a request is
//     built, not in Enqueue; BufferBytes counts uncompressed bytes.
//   - Produce v10+ leader hints (CurrentLeader/NodeEndpoints) are not used: a
//     NOT_LEADER_OR_FOLLOWER error refreshes metadata instead.
//   - ApiVersions v1/v2/v4 are never sent; v4 differs from v3 only in feature
//     version semantics this producer ignores.
//   - max.in.flight.requests.per.connection is effectively 1: one request at
//     a time per broker connection, no pipelining. Batching across partitions
//     per request compensates; with acks=0 no response is awaited anyway.
//   - buffer.memory defaults to 64MiB instead of the Java 512MiB (shared host,
//     drop-new contract; see Config.BufferBytes).
//   - Shutdown is a bounded drain (Close) rather than the Java shutdownNow
//     without drain; records not attempted are reported as abandoned.
//
// Verification: an in-process fake broker (fakebroker_test.go) for every
// version and tagged-field path; TestJavaLZ4Oracle, which has kafka-clients
// 4.0.0 decode Go batches/frames and the test decoder read Java ones; and
// TestRealBrokerProduce/TestRealBrokerUnsupportedApiVersions against an
// isolated broker from scripts/kafka-local.py (results in docs/validation/2026-10-15-remaining-gaps.md).
// The env-gated tests are skipped otherwise, and a skip is not a pass.
package kafka

// Idle connections: brokers close connections idle for connections.max.idle.ms
// (10 minutes by default). With acks=0 nothing would reveal that close before
// the next write, so a connection unused for IdleReconnect (9 minutes) is
// redialled first; the Java client instead observes the FIN on its selector.
