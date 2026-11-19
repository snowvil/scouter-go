package service

import (
	"context"
	"github.com/snowvil/scouter-go/internal/protocol"
)

var VisitorCommands = []string{"VISITOR_REALTIME", "VISITOR_REALTIME_TOTAL", "VISITOR_REALTIME_GROUP", "VISITOR_LOADDATE", "VISITOR_LOADDATE_TOTAL", "VISITOR_LOADDATE_GROUP", "VISITOR_LOADHOUR_GROUP"}
var TagCommands = []string{"TAGCNT_DIV_NAMES", "TAGCNT_TAG_NAMES", "TAGCNT_TAG_VALUES", "TAGCNT_TAG_VALUE_DATA", "TAGCNT_TAG_ACTUAL_DATA"}

type VisitorBackend interface {
	VisitorRealtime(context.Context, []int32, string) (int64, error)
	VisitorDaily(context.Context, string, []int32, string) (int64, error)
	VisitorHourly(context.Context, string, int64, []int32) (int64, error)
}
type TagValueCount struct {
	Value protocol.Value
	Count float64
}
type TagValueTotal struct {
	HowManyValues int
	TotalCount    float32
	Values        []TagValueCount
}
type TagActualQuery struct {
	Date, ObjType, Group string
	From, To, TxID       int64
	Max                  int
	Reverse              bool
	Filter               protocol.Value
}
type TagBackend interface {
	TagValues(context.Context, string, string, string, string) (TagValueTotal, error)
	TagValueSeries(context.Context, string, string, string, string, protocol.Value) ([]float32, error)
	TagActual(context.Context, TagActualQuery) ([][]byte, error)
}
