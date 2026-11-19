// StatusRD / DatabaseService: Copyright 2015 Scouter authors, Apache-2.0.
// Independent implementation from dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5.
package storage

import (
	"context"
	"github.com/snowvil/scouter-go/internal/protocol"
)

// QueryListContext reads the same forward Status index as StatusRD.readByTime.
// Every StatusPack for the requested object is included, irrespective of key.
// Returned packs and recursively nested data are owned by the caller.
func (s *Store) QueryListContext(ctx context.Context, date string, from, to int64, objHash int32) ([]*protocol.StatusPack, error) {
	packs, err := s.scanTimedPacks(ctx, date, from, to, statusRecordSpec, func(p protocol.Pack) bool { return p.(*protocol.StatusPack).ObjHash == objHash })
	if err != nil {
		return nil, err
	}
	out := make([]*protocol.StatusPack, len(packs))
	for i, p := range packs {
		out[i] = p.(*protocol.StatusPack)
	}
	return out, nil
}
