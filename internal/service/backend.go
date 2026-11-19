// Package service implements the pinned Scouter Client query boundary.
// Storage and ingest implement Backend; handlers do not depend on either package.
package service

import (
	"context"

	"github.com/snowvil/scouter-go/internal/protocol"
)

type CounterPoint struct {
	Time  int64
	Value protocol.Value
}

type RealtimeQuery struct {
	Loop         int64
	Index        int32
	ObjectHashes []int32
	MinElapsed   int32
	LatestCount  int32
}

type RealtimeResult struct {
	Loop    int64
	Index   int32
	Records []*protocol.XLogPack
}

type HistoryQuery struct {
	Date                     string
	From, To                 int64
	ObjectHashes             []int32
	MinElapsed               int32
	Reverse                  bool
	Max, PageCount           int
	LastBucketTime, LastTxID int64
}

// HistoryResult is the V2 paging state after the records went to visit.
type HistoryResult struct {
	HasMore                  bool
	LastBucketTime, LastTxID int64
}

// Results are detached snapshots. Empty date selects current objects only.
// Text distinguishes a missing entry from an existing empty string.
// Profile returns ordered raw Step chunks, not Pack envelopes.
// HistoryResult.LastBucketTime is the paging bucket, not the last XLog EndTime.
// XLogsHistory streams each selected record to visit in Java order as it
// reads (object filter, elapsed bound, max and page count applied in the
// scan, Java XLogService); a visit error ends the query with that error.
type Backend interface {
	Objects(context.Context, string) ([]*protocol.ObjectPack, error)
	Object(context.Context, string, int32) (*protocol.ObjectPack, error)
	Text(context.Context, string, string, int32) (string, bool, error)
	Counter(context.Context, int32, string, byte) (protocol.Value, bool, error)
	CounterHistory(context.Context, int32, string, int64, int64) ([]CounterPoint, error)
	CounterDaily(context.Context, string, int32, string, byte) ([]protocol.Value, error)
	XLogsRealtime(context.Context, RealtimeQuery) (RealtimeResult, error)
	XLogsHistory(ctx context.Context, q HistoryQuery, visit func(*protocol.XLogPack) error) (HistoryResult, error)
	XLog(context.Context, string, int64) (*protocol.XLogPack, error)
	Profile(context.Context, string, int64) ([][]byte, error)
}
