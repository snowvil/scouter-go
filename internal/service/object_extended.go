package service

import (
	"context"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/transport"
)

func (s *Service) objectTodayFullList(ctx context.Context, stream *transport.Stream) error {
	date := scoutertime.Date(s.config.Clock().UnixMilli(), s.config.Location)
	stored, err := s.backend.Objects(ctx, date)
	if err != nil {
		return err
	}
	if len(stored) > extendedQueryCells {
		return transport.ErrLimit
	}
	for _, o := range stored {
		if err := ctx.Err(); err != nil {
			return err
		}
		if o == nil {
			continue
		}
		current, err := s.backend.Object(ctx, "", o.ObjHash)
		if err != nil {
			return err
		}
		source, status := o, "dead"
		if current != nil {
			source = current
			status = "inactive"
			if current.Alive {
				status = "active"
			}
		}
		copy := *source
		copy.Tags.Entries = append([]protocol.Entry{}, source.Tags.Entries...)
		copy.Tags.Type = protocol.ValueMap
		extendedSetEntry(&copy.Tags.Entries, "status", stringValue(status))
		if current == nil {
			copy.Alive = false
		}
		if err := stream.ResponsePack(&copy); err != nil {
			return err
		}
	}
	return nil
}
