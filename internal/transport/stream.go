package transport

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Stream is a synchronous, bounded command stream, not a universal envelope.
// Dispatcher reads only its command's body and writes its response items. The
// same buffered reader must be used throughout a connection. Do not retain or
// concurrently access a Client handler's Stream after the handler returns.
type Stream struct {
	reader                io.Reader
	writer                io.Writer
	config                TCPConfig
	readBytes, writeBytes int
	// client is set for Client connections only (serveClient). It replaces the
	// cumulative per-command write budget with a per-item pending-send cap and
	// records progress for the per-command idle watchdog. Agent connections
	// keep client == nil and the cumulative MaxFrameBytes budget.
	client *clientIO
}

// MaxResponseValueBytes bounds one Client response value (a Blob, or the Blob
// of a Pack) in the response encoder. It is response-only: request decoding
// keeps TCPConfig.CodecLimits. Java has no such cap; an encoded item is held
// in memory before it is sent, so this cap is what bounds that send buffer.
const MaxResponseValueBytes = 256 << 20

// maxResponseValues bounds the entries of one collection and the values of
// one Client response item. Java has no such cap: a COUNTER_PAST_TIME_ALL
// row holds a 2s point per counter for the whole range (43,200 a day), past
// the request decoder's 4096/16384. Every encoded value takes at least one
// byte, so clientPendingLimit is what bounds a response; this only keeps the
// count arithmetic finite.
const maxResponseValues = clientPendingValues

const clientPendingValues = MaxResponseValueBytes + responseEnvelopeBytes

// responseEnvelopeBytes is the room one encoded response item may use around
// its largest value (Pack fields, length prefixes).
const responseEnvelopeBytes = 1 << 20

// clientFlushInterval: a Client response item is flushed to the socket when
// the previous flush is at least this old, so a slow streaming scan delivers
// what it found instead of holding it in the 4 KiB bufio buffer until the end
// (Java flushes after each XLog of a history scan).
const clientFlushInterval = 200 * time.Millisecond

// clientPendingLimit is the largest single encoded Client response item: the
// bytes held in memory before they are handed to the socket writer. Tests in
// this package lower it; production never changes it.
var clientPendingLimit = MaxResponseValueBytes + responseEnvelopeBytes

// progressEpoch is the monotonic base of progress stamps.
var progressEpoch = time.Now()

func progressNow() int64 { return int64(time.Since(progressEpoch)) }

type clientIO struct {
	pending   int
	limits    protocol.Limits
	flush     func() error
	lastFlush int64
	progress  atomic.Int64
	budget    *clientBudget // nil: no shared response budget (tests)
	// done ends a response budget wait: the running command's context
	// (progress limit, connection or server end). nil while no command runs.
	done <-chan struct{}
	// await runs an upstream wait of the running command (AwaitUpstream). nil
	// while no command runs.
	await func(wait func() error) error
}

// asClient switches s to the Client response policy. flush flushes the
// connection's bufio.Writer; only the handler goroutine calls it.
func (s *Stream) asClient(flush func() error) {
	limits := s.config.CodecLimits
	if limits.MaxBlobBytes < MaxResponseValueBytes {
		limits.MaxBlobBytes = MaxResponseValueBytes
	}
	if limits.MaxBytes < clientPendingLimit {
		limits.MaxBytes = clientPendingLimit
	}
	if limits.MaxCollectionEntries < maxResponseValues {
		limits.MaxCollectionEntries = maxResponseValues
	}
	if limits.MaxTotalValues < maxResponseValues {
		limits.MaxTotalValues = maxResponseValues
	}
	c := &clientIO{pending: clientPendingLimit, limits: limits, flush: flush}
	c.progress.Store(progressNow())
	s.client = c
}
func (s *Stream) touch() {
	if s.client != nil {
		s.client.progress.Store(progressNow())
	}
}

// AwaitUpstream runs wait, in which a Client command handler waits for an
// upstream (Agent) answer, without the command's progress limit: Java waits
// for the Agent as long as the Agent read timeout allows, so wait must be
// bounded by its own deadline (the Agent relay's) and end when the handler's
// context ends. A Client that closes or resets the connection meanwhile ends
// that context with ErrClientGone. wait must not read from s: the connection
// reader is probed meanwhile. It may write response items (a Blob relay writes
// each chunk as it arrives); each socket write chunk keeps its RequestTimeout
// deadline, so a Client that stops reading still ends the command. Like the
// rest of Stream it must not be called concurrently; a nested call (from
// inside wait) just runs its wait under the outer one. Outside a Client
// command (or on a nil Stream) it just runs wait.
func (s *Stream) AwaitUpstream(wait func() error) error {
	if s == nil || s.client == nil || s.client.await == nil {
		return wait()
	}
	await := s.client.await
	s.client.await = nil // a nested call must not start a second probe
	defer func() { s.client.await = await }()
	return await(wait)
}

// idle reports how long the Client stream made no read or write progress.
// Safe from any goroutine.
func (s *Stream) idle() time.Duration {
	return time.Duration(progressNow() - s.client.progress.Load())
}

func newStream(reader io.Reader, writer io.Writer, config TCPConfig) *Stream {
	if config.MaxFrameBytes == 0 {
		config.MaxFrameBytes = 8 << 20
	}
	if config.MaxTextBytes == 0 {
		config.MaxTextBytes = 64 << 10
	}
	return &Stream{reader: reader, writer: writer, config: config}
}
func (s *Stream) reset() {
	s.readBytes = 0
	s.writeBytes = 0
	if s.client != nil {
		now := progressNow()
		s.client.progress.Store(now)
		s.client.lastFlush = now
	}
}
func (s *Stream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	remaining := s.config.MaxFrameBytes - s.readBytes
	if remaining <= 0 {
		return 0, ErrLimit
	}
	if len(p) > remaining {
		p = p[:remaining]
	}
	n, err := s.reader.Read(p)
	s.readBytes += n
	if n > 0 {
		s.touch()
	}
	return n, err
}

// writeRoom is the largest write accepted now: the remaining command budget
// for Agent streams, the pending-send cap per item for Client streams.
func (s *Stream) writeRoom() int {
	if s.client != nil {
		return s.client.pending
	}
	return s.config.MaxFrameBytes - s.writeBytes
}
func (s *Stream) Write(p []byte) (int, error) {
	if len(p) > s.writeRoom() {
		return 0, ErrLimit
	}
	n, err := s.writer.Write(p)
	s.writeBytes += n
	if n > 0 {
		s.touch()
	}
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}
func (s *Stream) ReadExact(n int) ([]byte, error) {
	if n < 0 {
		return nil, ErrMalformed
	}
	if n > s.config.MaxFrameBytes-s.readBytes {
		return nil, ErrLimit
	}
	data := make([]byte, n)
	_, err := io.ReadFull(s, data)
	return data, err
}

// ConsumeBuffered discards prefix only when the reader already buffers at
// least len(prefix) bytes and they match exactly; it never blocks for more
// input. The service uses it for the empty MapPack the Java Client appends
// to GET_ALERT_REAL_COUNTER_DESC/GET_PLUGIN_HELPER_DESC (decision §9-11 b).
func (s *Stream) ConsumeBuffered(prefix []byte) (bool, error) {
	b, ok := s.reader.(interface {
		Buffered() int
		Peek(int) ([]byte, error)
		Discard(int) (int, error)
	})
	if !ok || len(prefix) == 0 || b.Buffered() < len(prefix) || len(prefix) > s.config.MaxFrameBytes-s.readBytes {
		return false, nil
	}
	p, err := b.Peek(len(prefix))
	if err != nil || !bytes.Equal(p, prefix) {
		return false, err
	}
	n, err := b.Discard(len(prefix))
	s.readBytes += n
	return err == nil, err
}

func (s *Stream) ReadByte() (byte, error) {
	p, err := s.ReadExact(1)
	if err != nil {
		return 0, err
	}
	return p[0], nil
}
func (s *Stream) ReadInt16() (int16, error) {
	p, err := s.ReadExact(2)
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(p)), nil
}
func (s *Stream) ReadInt32() (int32, error) {
	p, err := s.ReadExact(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(p)), nil
}
func (s *Stream) ReadInt64() (int64, error) {
	p, err := s.ReadExact(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(p)), nil
}
func (s *Stream) readBlob(max int) ([]byte, error) {
	prefix, err := s.ReadByte()
	if err != nil {
		return nil, err
	}
	size := int64(prefix)
	switch prefix {
	case 255:
		v, e := s.ReadInt16()
		err = e
		size = int64(uint16(v))
	case 254:
		v, e := s.ReadInt32()
		err = e
		size = int64(v)
	}
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, ErrMalformed
	}
	if size > int64(max) {
		return nil, ErrLimit
	}
	return s.ReadExact(int(size))
}
func (s *Stream) ReadBlob() ([]byte, error) { return s.readBlob(s.config.MaxFrameBytes) }
func (s *Stream) ReadText() (string, error) {
	p, err := s.readBlob(s.config.MaxTextBytes)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(p) {
		return "", fmt.Errorf("%w: invalid UTF-8 Text", ErrMalformed)
	}
	return string(p), nil
}
func (s *Stream) ReadIntBytes() ([]byte, error) {
	n, err := s.ReadInt32()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, ErrMalformed
	}
	return s.ReadExact(int(n))
}
func (s *Stream) readLimits() (protocol.Limits, error) {
	remaining := s.config.MaxFrameBytes - s.readBytes
	if remaining <= 0 {
		return protocol.Limits{}, ErrLimit
	}
	limits := s.config.CodecLimits
	maxBytes := limits.MaxBytes
	if maxBytes == 0 {
		maxBytes = 8 << 20
	}
	if remaining < maxBytes {
		limits.MaxBytes = remaining
	}
	maxBlob := limits.MaxBlobBytes
	if maxBlob == 0 {
		maxBlob = 4 << 20
	}
	if remaining < maxBlob {
		limits.MaxBlobBytes = remaining
	}
	return limits, nil
}
func (s *Stream) ReadPack() (protocol.Pack, error) {
	limits, err := s.readLimits()
	if err != nil {
		return nil, err
	}
	return protocol.ReadPack(s, limits)
}
func (s *Stream) ReadValue() (protocol.Value, error) {
	limits, err := s.readLimits()
	if err != nil {
		return protocol.Value{}, err
	}
	return protocol.ReadValue(s, limits)
}
func (s *Stream) WriteByte(v byte) error { _, err := s.Write([]byte{v}); return err }
func (s *Stream) WriteInt32(v int32) error {
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], uint32(v))
	_, err := s.Write(p[:])
	return err
}
func (s *Stream) WriteInt64(v int64) error {
	var p [8]byte
	binary.BigEndian.PutUint64(p[:], uint64(v))
	_, err := s.Write(p[:])
	return err
}
func (s *Stream) WriteIntBytes(p []byte) error {
	if len(p) > math.MaxInt32 || len(p) > s.writeRoom()-4 {
		return ErrLimit
	}
	if err := s.WriteInt32(int32(len(p))); err != nil {
		return err
	}
	_, err := s.Write(p)
	return err
}
func (s *Stream) encoded(encode func(*protocol.Encoder)) ([]byte, error) {
	e := protocol.NewEncoder(s.config.CodecLimits)
	encode(e)
	return e.Bytes()
}
func (s *Stream) WriteText(text string) error {
	if len(text) > s.config.MaxTextBytes {
		return ErrLimit
	}
	p, err := s.encoded(func(e *protocol.Encoder) { e.Text(text) })
	if err != nil {
		return err
	}
	_, err = s.Write(p)
	return err
}
func (s *Stream) WriteBlob(data []byte) error {
	p, err := s.encoded(func(e *protocol.Encoder) { e.Blob(data) })
	if err != nil {
		return err
	}
	_, err = s.Write(p)
	return err
}
func (s *Stream) WritePack(pack protocol.Pack) error {
	p, err := protocol.EncodePack(pack, s.config.CodecLimits)
	if err != nil {
		return err
	}
	_, err = s.Write(p)
	return err
}

// responseLimits raises the single-value caps to MaxResponseValueBytes for
// Client responses only; every other encoder keeps TCPConfig.CodecLimits.
func (s *Stream) responseLimits() protocol.Limits {
	if s.client != nil {
		return s.client.limits
	}
	return s.config.CodecLimits
}
func (s *Stream) ResponsePack(pack protocol.Pack) error {
	p, err := protocol.EncodePack(pack, s.responseLimits())
	if err != nil {
		return err
	}
	return s.response(p)
}
func (s *Stream) ResponseValue(value protocol.Value) error {
	p, err := protocol.EncodeValue(value, s.responseLimits())
	if err != nil {
		return err
	}
	return s.response(p)
}
func (s *Stream) ResponseBlob(data []byte) error { return s.responseBlob(data, true) }

// ResponseRelayBlob is ResponseBlob for a chunk whose memory a caller's own
// budget already counts (the server's Agent relay permit for Blob streams).
// It skips the Client response memory budget, so a full budget never makes
// the relay wait or fail and close the Agent session (client_budget.go).
func (s *Stream) ResponseRelayBlob(data []byte) error { return s.responseBlob(data, false) }

func (s *Stream) responseBlob(data []byte, budgeted bool) error {
	e := protocol.NewEncoder(s.responseLimits())
	e.Blob(data)
	p, err := e.Bytes()
	if err != nil {
		return err
	}
	return s.write(p, budgeted)
}

// ResponseRaw writes one command-specific body with HasNEXT and the same byte
// budget as Pack/Value/Blob responses. The caller owns its framing contract.
func (s *Stream) ResponseRaw(data []byte) error { return s.response(data) }

func (s *Stream) response(p []byte) error { return s.write(p, true) }

// write sends one HasNEXT item; budgeted items of a Client stream take the
// shared Client response memory budget while they are written.
func (s *Stream) write(p []byte, budgeted bool) error {
	if len(p) > s.writeRoom()-1 {
		return ErrLimit
	}
	if s.client != nil && budgeted {
		release, err := s.client.budget.hold(len(p), s.client.done)
		if err != nil {
			return err
		}
		defer release()
	}
	if err := s.WriteByte(HasNext); err != nil {
		return err
	}
	if _, err := s.Write(p); err != nil {
		return err
	}
	if c := s.client; c != nil && c.flush != nil {
		if now := progressNow(); time.Duration(now-c.lastFlush) >= clientFlushInterval {
			c.lastFlush = now
			return c.flush()
		}
	}
	return nil
}
