package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// Client command time policy (CONTRACT §2·§5): Java bounds only each socket
// read (SO_TIMEOUT) and never a whole command, so a Client response streams as
// long as it keeps moving. Go replaces the former total RequestTimeout deadline
// with progress-based limits:
//   - each socket read inside a command: so-timeout, or RequestTimeout when the
//     so-timeout is 0 (idleReader);
//   - each socket write chunk: RequestTimeout (progressWriter);
//   - the handler: cancelled once the Stream made no read or write progress
//     for RequestTimeout (progressContext), so a handler that stalls without
//     sending anything still ends. An Agent Pack relay the handler waits on
//     does not use this context for the Agent socket (internal/server
//     agentPool.relay): its end finishes the Client side only, and the Agent
//     answer is read to the end under the relay's own deadline.
//   - Stream.AwaitUpstream (an Agent answer the handler waits for, or a
//     Blob stream relayed chunk by chunk): the progress limit is held, like
//     Java, whose service thread waits for the Agent whatever the Client does
//     (AgentCall.scala:46-91, TcpServiceWorker.scala:111-130). The relay's
//     own deadline (a Blob stream: the Agent so_timeout per read) bounds the
//     wait instead, writes inside it keep the per-chunk write deadline, and a
//     Client that closes or resets the connection meanwhile ends the command
//     at once (ErrClientGone, awaitUpstream).

// clientWriteChunk splits large writes so the write deadline measures
// progress, not the time to send a whole multi-MiB item.
const clientWriteChunk = 64 << 10

// progressWriter is the Client connection's socket writer under bufio.Writer.
// Every chunk gets a fresh write deadline of timeout.
type progressWriter struct {
	conn    net.Conn
	timeout time.Duration
}

func (w *progressWriter) Write(p []byte) (int, error) {
	written := 0
	for written < len(p) {
		end := min(len(p), written+clientWriteChunk)
		if err := w.conn.SetWriteDeadline(time.Now().Add(w.timeout)); err != nil {
			return written, err
		}
		n, err := w.conn.Write(p[written:end])
		written += n
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

// ErrClientGone ends a command whose Client closed or reset the connection
// while the handler waited upstream (Stream.AwaitUpstream). The command
// context's Err wraps it with the socket error and also matches
// context.Canceled.
var ErrClientGone = errors.New("transport: client left during an upstream wait")

type clientGoneError struct{ cause error }

func (e *clientGoneError) Error() string { return ErrClientGone.Error() + ": " + e.cause.Error() }
func (e *clientGoneError) Unwrap() error { return e.cause }
func (e *clientGoneError) Is(target error) bool {
	return target == ErrClientGone || target == context.Canceled
}

// progressContext is a command context that ends when its parent ends or when
// the Client stream made no progress for limit outside a hold. An idle end
// reports context.DeadlineExceeded like the former total deadline did.
type progressContext struct {
	context.Context
	done  chan struct{}
	mu    sync.Mutex
	err   error
	holds int
	timer *time.Timer
	stop  func() bool
}

func withProgress(parent context.Context, stream *Stream, limit time.Duration) (*progressContext, func()) {
	c := &progressContext{Context: parent, done: make(chan struct{})}
	// Both callbacks take mu, so neither runs before the fields are set.
	c.mu.Lock()
	c.timer = time.AfterFunc(limit, func() { c.check(stream, limit) })
	c.stop = context.AfterFunc(parent, func() { c.finish(parent.Err()) })
	c.mu.Unlock()
	return c, func() { c.finish(context.Canceled) }
}
func (c *progressContext) check(stream *Stream, limit time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return
	}
	next := limit
	if c.holds == 0 {
		idle := stream.idle()
		if idle >= limit {
			c.finishLocked(context.DeadlineExceeded)
			return
		}
		next = limit - idle
	}
	c.timer.Reset(next)
}

// hold suspends the progress limit until the returned release runs. Release
// counts as progress, so the limit restarts in full when the hold ends.
func (c *progressContext) hold(stream *Stream) (release func()) {
	c.mu.Lock()
	c.holds++
	c.mu.Unlock()
	return func() {
		stream.touch()
		c.mu.Lock()
		c.holds--
		c.mu.Unlock()
	}
}
func (c *progressContext) finish(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finishLocked(err)
}
func (c *progressContext) finishLocked(err error) {
	if c.err != nil {
		return
	}
	c.err = err
	c.timer.Stop()
	close(c.done)
	if c.stop != nil {
		c.stop()
	}
}
func (c *progressContext) Done() <-chan struct{} { return c.done }
func (c *progressContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}
