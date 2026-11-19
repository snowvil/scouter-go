package kafka

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// brokerConn is one TCP connection to one broker with the api versions it
// negotiated. Requests are issued one at a time (no pipelining), so the
// correlation id of the next response is always known. The owner (a worker or
// the metadata fetch) serializes use; closeConn may be called concurrently to
// abort blocked I/O.
type brokerConn struct {
	addr               string
	conn               net.Conn
	cfg                *Config
	correlation        int32
	versions           map[int16]versionRange
	apiVersionsVersion int16
	produceVersion     int16
	metadataVersion    int16
	lastUse            time.Time
	writer             *batchWriter
}

// dialBroker connects, exchanges ApiVersions and negotiates Produce and
// Metadata versions. Both must be available or the connection is rejected.
//
// ApiVersions is first sent as v3 (flexible, with client software name and
// version). A broker that does not know v3 answers UNSUPPORTED_VERSION in the
// v0 layout; the request is then repeated as v0 on the same connection, like
// the Java NetworkClient does.
func dialBroker(ctx context.Context, cfg *Config, addr string) (*brokerConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	if tcp, ok := raw.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	c := &brokerConn{addr: addr, conn: raw, cfg: cfg}
	for _, v := range []int16{supportedVersions[apiApiVersions].max, 0} {
		c.apiVersionsVersion = v
		var body []byte
		body, err = c.exchange(apiApiVersions, v, func(e *encoder) { encodeApiVersionsRequest(e, v) }, true)
		if err == nil {
			c.versions, err = parseApiVersionsResponse(v, body, cfg.Limits)
		}
		var kerr Error
		if v != 0 && errors.As(err, &kerr) && kerr.Code == errUnsupportedVersionCode {
			continue
		}
		break
	}
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("api versions %s: %w", addr, err)
	}
	if c.produceVersion, err = negotiate(c.versions, apiProduce); err != nil {
		raw.Close()
		return nil, fmt.Errorf("%s: %w", addr, err)
	}
	if c.metadataVersion, err = negotiate(c.versions, apiMetadata); err != nil {
		raw.Close()
		return nil, fmt.Errorf("%s: %w", addr, err)
	}
	c.writer = newBatchWriter(cfg.Compression)
	return c, nil
}

func (c *brokerConn) close() {
	if c != nil && c.conn != nil {
		c.conn.Close()
	}
}

// exchange writes one request and, when wantResponse, reads exactly one
// response frame and returns its body after the response header (correlation
// id, plus the tagged field section of header v1). Both directions share one
// RequestTimeout deadline. Any error leaves the connection unusable; the
// caller must close it.
func (c *brokerConn) exchange(key, version int16, body func(*encoder), wantResponse bool) ([]byte, error) {
	c.correlation++
	corr := c.correlation
	e := &encoder{buf: make([]byte, 0, 256)}
	e.int32(0)
	encodeRequestHeader(e, key, version, corr, c.cfg.ClientID)
	body(e)
	binary.BigEndian.PutUint32(e.buf, uint32(len(e.buf)-4))
	deadline := time.Now().Add(c.cfg.RequestTimeout)
	if err := c.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	if _, err := c.conn.Write(e.buf); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}
	if !wantResponse {
		return nil, nil
	}
	var head [4]byte
	if _, err := io.ReadFull(c.conn, head[:]); err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	size := int32(binary.BigEndian.Uint32(head[:]))
	if size < 4 {
		return nil, fmt.Errorf("%w: response size %d", ErrMalformed, size)
	}
	if int(size) > c.cfg.Limits.MaxResponseBytes {
		return nil, fmt.Errorf("%w: response size %d > %d", ErrLimit, size, c.cfg.Limits.MaxResponseBytes)
	}
	frame := make([]byte, size)
	if _, err := io.ReadFull(c.conn, frame); err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	if got := int32(binary.BigEndian.Uint32(frame)); got != corr {
		return nil, fmt.Errorf("%w: correlation id %d, want %d", ErrMalformed, got, corr)
	}
	if !responseHeaderFlexible(key, version) {
		return frame[4:], nil
	}
	d := &decoder{buf: frame, off: 4, limits: c.cfg.Limits}
	if err := d.skipTags(); err != nil {
		return nil, fmt.Errorf("response header: %w", err)
	}
	return frame[d.off:], nil
}

// fetchMetadata asks this broker for the given topics.
func (c *brokerConn) fetchMetadata(topics []string) (*metadataResponse, error) {
	v := c.metadataVersion
	body, err := c.exchange(apiMetadata, v, func(e *encoder) { encodeMetadataRequest(e, v, topics) }, true)
	if err != nil {
		return nil, err
	}
	return parseMetadataResponse(v, body, c.cfg.Limits)
}

// produce sends batches (unique topic/partitions) and returns per-partition
// results when acks != 0. With acks=0 the broker sends nothing back.
func (c *brokerConn) produce(batches []*batch) ([]partitionResult, error) {
	v := c.produceVersion
	acks := c.cfg.Acks
	timeout := int32(c.cfg.RequestTimeout / time.Millisecond)
	body, err := c.exchange(apiProduce, v, func(e *encoder) { encodeProduceRequest(e, v, acks, timeout, batches, c.writer) }, acks != 0)
	if err != nil {
		return nil, err
	}
	if acks == 0 {
		return nil, nil
	}
	return parseProduceResponse(v, body, c.cfg.Limits)
}
