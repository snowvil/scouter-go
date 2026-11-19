package storage

import (
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"time"
)

func JavaMillis(ms int64, location *time.Location) int64 { return scoutertime.Millis(ms, location) }
func JavaDate(ms int64, location *time.Location) string  { return scoutertime.Date(ms, location) }
