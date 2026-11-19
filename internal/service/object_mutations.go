package service

import (
	"context"
	"fmt"
	"math"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// ObjectRemover is optional and owned by ingest/storage. RemoveObjects is
// AgentManager.removeAgents: drop each hash from the live object map and, when
// permanent, from the object DB (ObjectWR.remove). RemoveInactiveObjects is
// AgentManager.clearInactive: drop every live-map object with alive=false.
type ObjectRemover interface {
	RemoveObjects(ctx context.Context, hashes []int32, permanent bool) error
	RemoveInactiveObjects(ctx context.Context) error
}

// objectRemoveCommand implements AgentInfo.scala:47-49, 158-168, 196-206.
// OBJECT_REMOVE and OBJECT_REMOVE_IN_MEMORY read a bare ListValue (not a
// MapPack) of objHash numbers, remove them (permanent only for OBJECT_REMOVE)
// and answer the OBJECT_TODAY_FULL_LIST ObjectPack sequence.
// OBJECT_REMOVE_INACTIVE has no body and no response items.
func (s *Service) objectRemoveCommand(ctx context.Context, r transport.ClientRequest, stream *transport.Stream) error {
	remover, ok := s.backend.(ObjectRemover)
	if !ok {
		return fmt.Errorf("%w: %s requires an object remover", transport.ErrUnsupported, r.Command)
	}
	if r.Command == "OBJECT_REMOVE_INACTIVE" {
		manageLogf("[OBJECT_REMOVE_INACTIVE] session=%d peer=%v", r.Session, r.Peer)
		return remover.RemoveInactiveObjects(ctx)
	}
	v, err := stream.ReadValue()
	if err != nil {
		return err
	}
	if v.Type != protocol.ValueList {
		return fmt.Errorf("%w: expected ListValue body", transport.ErrMalformed)
	}
	if len(v.Values) > manageMaxItems {
		return transport.ErrLimit
	}
	hashes := make([]int32, 0, len(v.Values))
	for _, item := range v.Values {
		// ListValue.getLong(i).toInt converts any Number and truncates; other
		// types become hash 0. The existing Go guard rejects both instead.
		if item.Type != protocol.ValueDecimal || item.Int < math.MinInt32 || item.Int > math.MaxInt32 {
			return badParam("objHash")
		}
		hashes = append(hashes, int32(item.Int))
	}
	permanent := r.Command == "OBJECT_REMOVE"
	manageLogf("[%s] objHash=%v permanent=%v session=%d peer=%v", r.Command, hashes, permanent, r.Session, r.Peer)
	if len(hashes) > 0 {
		if err := remover.RemoveObjects(ctx, hashes, permanent); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.objectTodayFullList(ctx, stream)
}
