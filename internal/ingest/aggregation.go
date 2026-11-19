package ingest

import (
	"context"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/storage"
)

func (e *Engine) VisitorRealtime(ctx context.Context, hashes []int32, typ string) (int64, error) {
	return e.store.VisitorRealtimeContext(ctx, e.config.Clock().UnixMilli(), hashes, typ)
}
func (e *Engine) VisitorDaily(ctx context.Context, date string, hashes []int32, typ string) (int64, error) {
	return e.store.VisitorDailyContext(ctx, date, hashes, typ)
}
func (e *Engine) VisitorHourly(ctx context.Context, date string, tm int64, hashes []int32) (int64, error) {
	return e.store.VisitorHourlyContext(ctx, date, tm, hashes)
}
func (e *Engine) TagValues(ctx context.Context, date, typ, group, name string) (service.TagValueTotal, error) {
	v, err := e.store.TagValuesContext(ctx, date, typ, group, name)
	out := service.TagValueTotal{HowManyValues: v.HowManyValues, TotalCount: v.TotalCount}
	for _, item := range v.Values {
		out.Values = append(out.Values, service.TagValueCount{Value: item.Value, Count: item.Count})
	}
	return out, err
}
func (e *Engine) TagValueSeries(ctx context.Context, date, typ, group, name string, v protocol.Value) ([]float32, error) {
	return e.store.TagValueSeriesContext(ctx, date, typ, group, name, v)
}
func (e *Engine) TagActual(ctx context.Context, q service.TagActualQuery) ([][]byte, error) {
	return e.store.TagActualContext(ctx, storage.TagActualQuery{Date: q.Date, ObjType: q.ObjType, Group: q.Group, From: q.From, To: q.To, TxID: q.TxID, Max: q.Max, Reverse: q.Reverse, Filter: q.Filter})
}
