package service

import (
	"context"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Optional backends keep each packet family's query contract explicit.
// Every result is an owned snapshot; missing records are nil without an error.
type StatusBackend interface {
	StatusHistory(context.Context, string, int64, int64) ([]*protocol.StatusPack, error)
	Status(context.Context, int32, string) (*protocol.StatusPack, error)
}

type AlertQuery struct {
	ObjType string
	Loop    int64
	Index   int32
}

type AlertResult struct {
	Loop    int64
	Index   int32
	Records []*protocol.AlertPack
}

// ScanAlertHistory and ScanSummaries stream one date's time range in stored
// order (Java AlertRD/SummaryRD.readByTime handlers) with no total cap; visit
// returning false ends the scan without an error. visit may call the Backend.
type AlertBackend interface {
	AlertsRealtime(context.Context, AlertQuery) (AlertResult, error)
	ScanAlertHistory(ctx context.Context, date string, from, to int64, visit func(*protocol.AlertPack) (bool, error)) error
}

type SummaryBackend interface {
	ScanSummaries(ctx context.Context, date string, from, to int64, stype byte, visit func(*protocol.SummaryPack) (bool, error)) error
}

type StackBackend interface {
	Stacks(context.Context, string, int64, int64) ([]*protocol.StackPack, error)
}

type BatchBackend interface {
	Batches(context.Context, string, int64, int64) ([]*protocol.BatchPack, error)
	BatchAt(context.Context, string, int64, int64) (*protocol.BatchPack, error)
}

type BatchArchiveBackend interface {
	BatchStack(context.Context, string, int64, string) ([][]byte, error)
}

type InteractionBackend interface {
	InteractionCounters(context.Context, int32) ([]*protocol.InteractionPerfCounterPack, error)
}
