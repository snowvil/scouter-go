package service

import (
	"context"
	"github.com/snowvil/scouter-go/internal/protocol"
)

// TraceBackend returns detached snapshots in pinned index/wire order.
// Time scans are raw (including zero elapsed); filtering belongs to the handler.
// SpansByGxID returns nil for no matching container, but a non-nil empty slice
// for an existing group of empty containers (Java Span-map absence differs).
// ScanXLogsByTime streams the raw chronological range of one date to visit
// as it reads (Java XLogRD.readByTime); visit returning false ends the scan
// without an error. visit may call the Backend (Text lookups).
type TraceBackend interface {
	XLogsByGxID(context.Context, string, int64) ([]*protocol.XLogPack, error)
	SpansByGxID(context.Context, string, int64) ([]*protocol.SpanPack, error)
	ScanXLogsByTime(ctx context.Context, date string, from, to int64, visit func(*protocol.XLogPack) (bool, error)) error
}
