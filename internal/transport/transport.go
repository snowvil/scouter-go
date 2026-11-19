// Package transport owns Scouter socket framing and bounded receive lifecycles.
// Wire source: Copyright 2015 the original author or authors,
// https://github.com/scouter-project/scouter, Apache License 2.0.
package transport

import (
	"errors"
	"fmt"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

var (
	ErrLimit        = protocol.ErrLimit
	ErrMalformed    = protocol.ErrInvalid
	ErrUnsupported  = protocol.ErrUnsupported
	ErrClosed       = errors.New("transport closed or already serving")
	ErrUnauthorized = errors.New("invalid Scouter session")
	ErrUnreadBody   = errors.New("handler did not consume the complete body")
	ErrExpired      = errors.New("UDP fragment assembly expired")
	// ErrAssemblyLimit is a UDP fragment refused by a reassembly bound
	// (UDPConfig MaxAssemblies, MaxBufferedBytes, MaxAssemblyBytes,
	// MaxFragments). errors.Is(err, ErrLimit) holds too.
	ErrAssemblyLimit = fmt.Errorf("%w: UDP fragment assembly", ErrLimit)
)

// ErrorHandler runs synchronously on receive/worker goroutines. It must be
// concurrency-safe, bounded and nonblocking. It must not call Serve or wait for
// shutdown. Handler errors are reported without claiming successful delivery.
type ErrorHandler func(ErrorEvent)
type ErrorEvent struct {
	Operation string
	Peer      string
	Err       error
}

func report(handler ErrorHandler, operation, peer string, err error) {
	if handler != nil {
		handler(ErrorEvent{Operation: operation, Peer: peer, Err: err})
	}
}

func positive(value, fallback int) (int, error) {
	if value < 0 {
		return 0, fmt.Errorf("%w: negative configuration", ErrMalformed)
	}
	if value == 0 {
		return fallback, nil
	}
	return value, nil
}
func duration(value, fallback time.Duration) (time.Duration, error) {
	if value < 0 {
		return 0, fmt.Errorf("%w: negative timeout", ErrMalformed)
	}
	if value == 0 {
		return fallback, nil
	}
	return value, nil
}

func validCodecLimits(limits protocol.Limits) error {
	// The pure decoder is the authority for normalization and invalid limits.
	return protocol.NewDecoder(nil, limits).Err()
}
