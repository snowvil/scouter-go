package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func extraStore(t *testing.T, c Config) (*Store, int64) {
	t.Helper()
	if c.Directory == "" {
		c.Directory = t.TempDir()
	}
	if c.Location == nil {
		c.Location = time.UTC
	}
	s, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, time.Date(2026, 1, 1, 0, 0, 0, 0, c.Location).UnixMilli()
}
func mapValue() protocol.Value { return protocol.Value{Type: protocol.ValueMap} }
func TestPackRecordsRestartAndBranches(t *testing.T) {
	s, start := extraStore(t, Config{})
	ctx := context.Background()
	for _, offset := range []int64{550, 100, 100} {
		if err := s.PutStatusContext(ctx, &protocol.StatusPack{Time: start + offset, ObjHash: 42, Key: "key", Data: mapValue()}); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []byte{1, 2, 3, 4, 10, 11, 12} {
		if err := s.PutSummaryContext(ctx, &protocol.SummaryPack{Time: start + 100, ObjHash: 42, SType: kind, Table: mapValue()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.PutAlertContext(ctx, &protocol.AlertPack{Time: start + 100, Title: "alert", Tags: mapValue()}); err != nil {
		t.Fatal(err)
	}
	dir := s.config.Directory
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(Config{Directory: dir, Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	statuses, err := s.StatusesContext(ctx, "20260101", start, start+600)
	if err != nil || len(statuses) != 3 || statuses[0].Time != start+100 || statuses[2].Time != start+550 {
		t.Fatalf("statuses %+v %v", statuses, err)
	}
	statuses[0].Key = "mutated"
	again, err := s.StatusesContext(ctx, "20260101", start, start+600)
	if err != nil || again[0].Key != "key" {
		t.Fatal("outputs not owned")
	}
	for _, kind := range []byte{1, 2, 3, 4, 10, 11, 12} {
		got, err := s.SummariesContext(ctx, "20260101", start, start+600, kind)
		if err != nil || len(got) != 1 || got[0].SType != kind {
			t.Fatalf("summary %d %+v %v", kind, got, err)
		}
	}
	alerts, err := s.AlertsContext(ctx, "20260101", start, start+600)
	if err != nil || len(alerts) != 1 || alerts[0].Title != "alert" {
		t.Fatalf("alerts %+v %v", alerts, err)
	}
	for _, path := range []string{"status/status.pstat", "status/status_tim.hfile", "alert/alert.alert", "sum/sum.sum", "sum/sum_app_tim.hfile", "sum/sum_sql_tim.hfile", "sum/sum_enduser_tim.hfile", "sum/sum_other_tim.hfile"} {
		if _, err = os.Stat(filepath.Join(dir, "20260101", path)); err != nil {
			t.Fatal(path, err)
		}
	}
}
func TestPackRecordsBoundariesAndCancel(t *testing.T) {
	s, start := extraStore(t, Config{MaxQueryRecords: 1, MaxQueryBytes: 1000})
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := s.PutStatusContext(ctx, &protocol.StatusPack{Time: start + int64(i), Data: mapValue()}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.StatusesContext(ctx, "20260101", start, start+500); !errors.Is(err, ErrLimit) {
		t.Fatalf("query limit %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.PutAlertContext(cancelled, &protocol.AlertPack{Time: start, Tags: mapValue()}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.SummariesContext(cancelled, "20260101", start, start+500, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.AlertsContext(ctx, "../escape", start, start+1); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := s.StatusesContext(ctx, "20260101", start, start+86400001); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if err := s.PutAlertContext(ctx, &protocol.AlertPack{Time: start, Message: string(make([]byte, 32768)), Tags: mapValue()}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if err := s.PutStatusContext(ctx, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AlertsContext(ctx, "20260101", start, start+1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func TestPackRecordsPhysicalBoundsAndCancelLock(t *testing.T) {
	s, start := extraStore(t, Config{MaxQueryBytes: 8})
	ctx := context.Background()
	if err := s.PutStatusContext(ctx, &protocol.StatusPack{Time: start, Data: mapValue()}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StatusesContext(ctx, "20260101", start, start+1); !errors.Is(err, ErrLimit) {
		t.Fatalf("byte bound %v", err)
	}
	locked, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	cancel()
	_, err := s.AlertsContext(locked, "20260101", start, start+1)
	s.mu.Unlock()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("lock cancel %v", err)
	}
}
func TestPackRecordsTruncatedAndWrongType(t *testing.T) {
	for _, mutation := range []string{"truncate", "kind"} {
		t.Run(mutation, func(t *testing.T) {
			s, start := extraStore(t, Config{})
			ctx := context.Background()
			if err := s.PutStatusContext(ctx, &protocol.StatusPack{Time: start, Data: mapValue()}); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(s.config.Directory, "20260101/status/status.pstat")
			if mutation == "truncate" {
				st, _ := os.Stat(path)
				if err := os.Truncate(path, st.Size()-1); err != nil {
					t.Fatal(err)
				}
			} else {
				f, err := os.OpenFile(path, os.O_RDWR, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = f.WriteAt([]byte{protocol.PackAlert}, 4)
				f.Close()
			}
			if _, err := s.StatusesContext(ctx, "20260101", start, start+1); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("%s %v", mutation, err)
			}
		})
	}
}
