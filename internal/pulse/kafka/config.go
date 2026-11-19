package kafka

import (
	"fmt"
	"math"
	"time"
)

// ErrorEvent is reported for every failed metadata fetch, connection, request
// or batch. Broker is "host:port" when known.
type ErrorEvent struct {
	Operation string
	Broker    string
	Err       error
}

// ErrorHandler runs synchronously on producer goroutines. It must be
// concurrency-safe and nonblocking; it must not call Flush or Close.
type ErrorHandler func(ErrorEvent)

// Compression is a Java producer compression.type value.
type Compression string

const (
	CompressionNone Compression = "none"
	CompressionLZ4  Compression = "lz4"
)

// ParseCompression accepts the Java compression.type names (exact, lowercase,
// as the Java ConfigDef validator requires). gzip, snappy and zstd are valid
// Java values that this package does not implement; they are rejected
// explicitly instead of silently sending uncompressed batches.
func ParseCompression(s string) (Compression, error) {
	switch Compression(s) {
	case CompressionNone, CompressionLZ4:
		return Compression(s), nil
	case "gzip", "snappy", "zstd":
		return "", fmt.Errorf("%w: compression.type %q is not implemented (none, lz4)", ErrInvalidConfig, s)
	}
	return "", fmt.Errorf("%w: unknown compression.type %q", ErrInvalidConfig, s)
}

// Config mirrors the Java producer settings recorded in
// docs/compatibility/pulse-contract.md where a Go equivalent exists. Zero
// values take the defaults below; negative values are rejected.
type Config struct {
	Bootstrap []string // "host:port" list; at least one required
	ClientID  string   // request header client_id; default "scouter-go"
	// Acks: 0 (default, contract: fire-and-forget, no response read), 1 or -1.
	Acks int16
	// Compression is the Java compression.type. Empty means lz4, which the
	// Pulse plugin hardcodes (its producer props
	// "compression.type"="lz4"; no scouter.conf key). "none" disables it.
	Compression Compression
	// Linger (Java linger.ms=20) bounds how long an open batch waits for more
	// records. BatchBytes (Java batch.size=128KiB) seals a batch early.
	Linger     time.Duration
	BatchBytes int
	// BufferBytes bounds all encoded, unsent record bytes including in-flight
	// batches. Enqueue fails immediately with ErrBufferFull beyond it. Default
	// 64MiB, not the Java 512MiB: the host is shared, the contract is acks=0
	// drop-new, and 64MiB already holds ~4s of 10,000 TPS at 1.5KiB/record.
	BufferBytes int64
	// MaxRecordBytes (Java max.request.size=1MiB) rejects a single record.
	// MaxRequestBytes bounds one Produce request (several batches).
	MaxRecordBytes, MaxRequestBytes int
	// RequestTimeout (Java request.timeout.ms = produce timeout 5000ms) is the
	// write/read deadline per request. DeliveryTimeout (Java delivery.timeout =
	// T+100ms) fails batches that could not be dispatched in time.
	RequestTimeout, DeliveryTimeout time.Duration
	// MetadataRefresh (Java metadata.max.age.ms=5m) forces a periodic refresh.
	// RetryBackoff/MaxRetryBackoff (Java 100ms/1s) rate-limit metadata retries.
	MetadataRefresh, RetryBackoff, MaxRetryBackoff time.Duration
	DialTimeout                                    time.Duration
	// IdleReconnect redials a broker connection unused for this long before
	// the next Produce. Brokers close idle connections (connections.max.idle.ms,
	// default 10m) and with acks=0 the first write after that would succeed
	// into a dead socket and be counted as sent; the Java client sees the FIN
	// through its selector instead. Default 9m.
	IdleReconnect time.Duration
	// MaxTopics bounds distinct topics ever enqueued (metadata request size).
	MaxTopics int
	// JobQueue bounds queued Produce jobs per broker worker.
	JobQueue int
	Limits   Limits
	OnError  ErrorHandler
}

func (c Config) normalized() (Config, error) {
	if len(c.Bootstrap) == 0 {
		return c, fmt.Errorf("%w: Bootstrap required", ErrInvalidConfig)
	}
	for _, addr := range c.Bootstrap {
		if addr == "" {
			return c, fmt.Errorf("%w: empty bootstrap address", ErrInvalidConfig)
		}
	}
	if c.ClientID == "" {
		c.ClientID = "scouter-go"
	}
	if len(c.ClientID) > math.MaxInt16 {
		return c, fmt.Errorf("%w: ClientID too long", ErrInvalidConfig)
	}
	switch c.Acks {
	case 0, 1, -1:
	default:
		return c, fmt.Errorf("%w: Acks must be 0, 1 or -1", ErrInvalidConfig)
	}
	if c.Compression == "" {
		c.Compression = CompressionLZ4
	}
	if _, err := ParseCompression(string(c.Compression)); err != nil {
		return c, err
	}
	for _, f := range []struct {
		p        *int
		fallback int
	}{
		{&c.BatchBytes, 128 << 10},
		{&c.MaxRecordBytes, 1 << 20},
		{&c.MaxRequestBytes, 1 << 20},
		{&c.MaxTopics, 64},
		{&c.JobQueue, 4},
	} {
		if *f.p < 0 {
			return c, fmt.Errorf("%w: negative size", ErrInvalidConfig)
		}
		if *f.p == 0 {
			*f.p = f.fallback
		}
	}
	if c.BufferBytes < 0 {
		return c, fmt.Errorf("%w: negative BufferBytes", ErrInvalidConfig)
	}
	if c.BufferBytes == 0 {
		c.BufferBytes = 64 << 20
	}
	if c.MaxRecordBytes > math.MaxInt32-batchHeaderLen || c.MaxRequestBytes > math.MaxInt32 {
		return c, fmt.Errorf("%w: record/request size exceeds int32", ErrInvalidConfig)
	}
	if c.MaxRecordBytes > c.MaxRequestBytes {
		return c, fmt.Errorf("%w: MaxRecordBytes > MaxRequestBytes", ErrInvalidConfig)
	}
	for _, f := range []struct {
		p        *time.Duration
		fallback time.Duration
	}{
		{&c.Linger, 20 * time.Millisecond},
		{&c.RequestTimeout, 5 * time.Second},
		{&c.MetadataRefresh, 5 * time.Minute},
		{&c.RetryBackoff, 100 * time.Millisecond},
		{&c.MaxRetryBackoff, time.Second},
		{&c.DialTimeout, 5 * time.Second},
		{&c.IdleReconnect, 9 * time.Minute},
	} {
		if *f.p < 0 {
			return c, fmt.Errorf("%w: negative duration", ErrInvalidConfig)
		}
		if *f.p == 0 {
			*f.p = f.fallback
		}
	}
	if c.DeliveryTimeout < 0 {
		return c, fmt.Errorf("%w: negative DeliveryTimeout", ErrInvalidConfig)
	}
	if c.DeliveryTimeout == 0 {
		c.DeliveryTimeout = c.RequestTimeout + 100*time.Millisecond
	}
	if c.RequestTimeout > time.Duration(math.MaxInt32)*time.Millisecond {
		return c, fmt.Errorf("%w: RequestTimeout exceeds int32 ms", ErrInvalidConfig)
	}
	if c.MaxRetryBackoff < c.RetryBackoff {
		c.MaxRetryBackoff = c.RetryBackoff
	}
	var err error
	c.Limits, err = c.Limits.normalized()
	return c, err
}
