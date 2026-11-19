package server

import (
	"context"
	"errors"
	"log"

	"github.com/snowvil/scouter-go/internal/config"
	"github.com/snowvil/scouter-go/internal/transport"
)

// ServerConfigFile is the SET_CONFIGURE_SERVER side of a config.Store
// (ConfigureService.setConfigureServer: Configure.saveText then
// reload(true)). Only the "server" kind is owned here.
type ServerConfigFile struct {
	Store *config.Store
}

// SaveConfigurationText replaces scouter.conf with text and applies it at
// once. A nil error is the Java result "true". The text is saved even when
// the reload reports a problem (a malformed \uXXXX applies the entries
// before it, a restart-only key is kept at its running value): Java answers
// "true" once saveText succeeded, and the reload logs the details.
func (f ServerConfigFile) SaveConfigurationText(ctx context.Context, kind, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if kind != "server" || f.Store == nil {
		return transport.ErrUnsupported
	}
	if err := f.Store.SaveText(text); err != nil {
		return err
	}
	if _, err := f.Store.Reload(true); err != nil && !errors.Is(err, config.ErrMalformedUnicode) {
		log.Printf("config: reload after SET_CONFIGURE_SERVER: %v", err)
	}
	return nil
}
