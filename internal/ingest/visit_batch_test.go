package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

// A visitor batch tries every visit: the writer counts the written, the
// invalid (refused on content) and the failed (I/O) visits apart.
func TestVisitBatchCountsWrittenInvalidAndFailed(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 3, 0, 0, time.UTC)
	e, _, _ := testEngine(t, &now, 16)
	e.w.enqueue(writeVisitor, 6, 0, func(context.Context) (int, error) {
		return 3, &visitBatchError{invalid: 2, invalidErr: protocol.ErrInvalid, failedErr: errors.New("disk")}
	})
	flush(t, e)
	if w := writerStats(e, writeVisitor); w.Enqueued != 6 || w.Written != 3 || w.WriteInvalid != 2 || w.WriteErrors != 1 || w.Pending != 0 {
		t.Fatalf("%+v", w)
	}

	// The real batch: visits refused on content do not drop the visits after them.
	hash := storage.CounterHash("/a")
	ms := now.UnixMilli()
	e.mu.Lock()
	e.visits = append(e.visits,
		visit{now: ms, objType: "java", userID: 1, objHash: hash},
		visit{now: ms, objType: "bad/type", userID: 2, objHash: hash}, // invalid name
		visit{now: 0, objType: "java", userID: 3, objHash: hash},      // invalid time
		visit{now: ms, objType: "java", userID: 4, objHash: hash})
	e.flushVisitsLocked()
	e.mu.Unlock()
	flush(t, e)
	if w := writerStats(e, writeVisitor); w.Enqueued != 10 || w.Written != 5 || w.WriteInvalid != 4 || w.WriteErrors != 1 {
		t.Fatalf("%+v", w)
	}
	if n, err := e.VisitorRealtime(context.Background(), []int32{hash}, ""); err != nil || n != 2 {
		t.Fatal("visits after the refused ones", n, err)
	}
}
