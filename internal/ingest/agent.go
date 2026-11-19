package ingest

import (
	"context"
	"github.com/snowvil/scouter-go/internal/protocol"
)

// AgentBlobRequest relays a raw Blob stream from the object's agent session,
// passing each chunk to emit as it arrives (Java AgentCall.call with a
// handler). An unknown object relays nothing, like AgentCall with a null
// ObjectPack.
func (e *Engine) AgentBlobRequest(ctx context.Context, hash int32, command string, p *protocol.MapPack, emit func([]byte) error) error {
	object, err := e.Object(ctx, "", hash)
	if err != nil {
		return err
	}
	if object == nil {
		return nil
	}
	if e.config.AgentBlobCall == nil {
		return protocol.ErrUnsupported
	}
	return e.config.AgentBlobCall(ctx, hash, command, p, emit)
}
