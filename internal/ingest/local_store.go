package ingest

import (
	"context"
	"github.com/snowvil/scouter-go/internal/protocol"
)

func (e *Engine) KeyValue(ctx context.Context, keySpace, key string, now int64) (string, bool, error) {
	return e.store.KeyValueContext(ctx, keySpace, key, now)
}

// PutKeyValue and SetKeyValueTTL back the SET_*_KV/_TTL commands
// (KeyValueStoreService); storage applies the original RW semantics.
func (e *Engine) PutKeyValue(ctx context.Context, space, key, value string, ttl, now int64) (bool, error) {
	return e.store.PutKeyValueContext(ctx, space, key, value, ttl, now)
}
func (e *Engine) SetKeyValueTTL(ctx context.Context, space, key string, ttl, now int64) (bool, error) {
	return e.store.SetKeyValueTTLContext(ctx, space, key, ttl, now)
}
func (e *Engine) QueryList(ctx context.Context, date string, from, to int64, hash int32) ([]*protocol.StatusPack, error) {
	return e.store.QueryListContext(ctx, date, from, to, hash)
}
