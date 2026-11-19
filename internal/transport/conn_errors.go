package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"time"
)

// Connection errors (a served connection that ended with an error, counted in
// TCPStats.Errors) reach the ErrorHandler as "tcp.connection" events at most
// once per class every rejectLogEvery. Many Clients ending at the progress
// limit at once, or many Agents dropping together, would otherwise write one
// line each. The next event of a class carries how many were left out; the
// total stays exact in TCPStats.Errors.
//
// Unknown magics and requests (scanners, probes) and invalid sessions have
// classes of their own so they cannot hide a storage or handler error, which
// falls in "other". TCPStats counts every class.
const (
	connErrorTimeout      = iota // a deadline or the Client progress limit
	connErrorClosed              // the peer closed or reset the connection
	connErrorLimit               // a Go resource limit (ErrLimit)
	connErrorUnsupported         // unknown magic or request (ErrUnsupported)
	connErrorUnauthorized        // invalid Client session (ErrUnauthorized)
	connErrorOther
	connErrorClasses
)

var connErrorNames = [connErrorClasses]string{"timeout", "closed", "limit", "unsupported", "unauthorized", "other"}

type connErrorLog struct {
	log             rateLimit
	unlogged, total atomic.Uint64
}

func connErrorClass(err error) int {
	var netErr net.Error
	switch {
	case errors.Is(err, ErrUnauthorized):
		return connErrorUnauthorized
	case errors.Is(err, ErrUnsupported):
		return connErrorUnsupported
	case errors.Is(err, ErrLimit):
		return connErrorLimit
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()):
		return connErrorTimeout
	case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE):
		return connErrorClosed
	}
	return connErrorOther
}

func (s *TCPServer) reportConnError(peer string, err error, now time.Time) {
	class := connErrorClass(err)
	l := &s.connErrors[class]
	l.total.Add(1)
	if !l.log.allow(now, rejectLogEvery) {
		l.unlogged.Add(1)
		return
	}
	if more := l.unlogged.Swap(0); more > 0 {
		err = fmt.Errorf("%w (%d more %s errors since the last line)", err, more, connErrorNames[class])
	}
	report(s.errors, "tcp.connection", peer, err)
}

// connErrorStats fills the per-class totals of TCPStats.
func (s *TCPServer) connErrorStats(st *TCPStats) {
	for class, p := range [connErrorClasses]*uint64{&st.TimeoutErrors, &st.ClosedErrors, &st.LimitErrors, &st.UnsupportedErrors, &st.UnauthorizedErrors, &st.OtherErrors} {
		*p = s.connErrors[class].total.Load()
	}
}
