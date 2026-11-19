package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// fullTextQueue holds the Store gate and fills the text queue by count; the
// caller releases the gate.
func fullTextQueue(t *testing.T, s *Store) {
	t.Helper()
	const date = "20261022"
	put := func(h int32) {
		if err := s.PutTextContext(context.Background(), date, &protocol.TextPack{XType: "sql", Hash: h, Text: "q"}); err != nil {
			s.mu.Unlock()
			t.Fatal(err)
		}
	}
	s.mu.Lock()
	put(1)
	for deadline := time.Now().Add(5 * time.Second); len(s.texts.queue) != 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			s.mu.Unlock()
			t.Fatal("writer did not take the first text")
		}
	}
	for h := int32(2); h <= textQueueMax+1; h++ {
		put(h)
	}
}

// A text Offer finds no room for stays pending (readable) without queue
// bytes; Submit queues it once there is room, and it is written.
func TestOfferHeldTextReadableThenSubmitted(t *testing.T) {
	s, _ := liveStore(t, Config{})
	const date = "20261022"
	fullTextQueue(t, s)
	w, err := s.PrepareText(date, &protocol.TextPack{XType: "service", Hash: 7, Text: "/a/b"})
	if err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	done, err := s.Offer(w)
	if done || err != nil {
		s.mu.Unlock()
		t.Fatal("offer to a full queue", done, err)
	}
	if text, found, known := s.TextCached(date, "service", 7); !known || !found || text != "/a/b" {
		s.mu.Unlock()
		t.Fatal("held text not readable", text, found, known)
	}
	time.AfterFunc(20*time.Millisecond, s.mu.Unlock)
	if err := s.Submit(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	if err := s.flushTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.TextDropped() != 0 {
		t.Fatal("dropped", s.TextDropped())
	}
	s.texts.mu.Lock()
	_, readCached := s.texts.read[w.text.ref] // written: TextDupCheck cache
	s.texts.mu.Unlock()
	if !readCached {
		t.Fatal("held text not written")
	}
}

// A purge or delete that removes a held text from pending discards it:
// Submit does not write it (into a directory deleted after it was accepted).
func TestHeldTextPurgedBeforeSubmitIsNotWritten(t *testing.T) {
	s, dir := liveStore(t, Config{})
	const date = "20261022"
	fullTextQueue(t, s)
	w, err := s.PrepareText(date, &protocol.TextPack{XType: "service", Hash: 7, Text: "/a/b"})
	if err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	if done, err := s.Offer(w); done || err != nil {
		s.mu.Unlock()
		t.Fatal("offer to a full queue", done, err)
	}
	s.discardTextsLocked([]string{filepath.Join(dir, permanentDate)}) // a purge, under the gate
	s.mu.Unlock()
	if err := s.Submit(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	if err := s.flushTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if text, found, err := s.TextContext(context.Background(), date, "service", 7); err != nil || found {
		t.Fatal("purged held text written", text, found, err)
	}
	if s.TextDropped() != 0 {
		t.Fatal("a discard is not a drop", s.TextDropped())
	}
}

// Drop of a held text counts it as the full queue's drop and moves it from
// pending to the dropped-text cache (still readable, as TextCache).
func TestDropHeldText(t *testing.T) {
	s, _ := liveStore(t, Config{})
	const date = "20261022"
	fullTextQueue(t, s)
	defer s.mu.Unlock()
	w, err := s.PrepareText(date, &protocol.TextPack{XType: "service", Hash: 7, Text: "/a/b"})
	if err != nil {
		t.Fatal(err)
	}
	if done, err := s.Offer(w); done || err != nil {
		t.Fatal("offer to a full queue", done, err)
	}
	s.Drop(w)
	s.texts.mu.Lock()
	pending := s.texts.pending[w.text.ref] != nil
	s.texts.mu.Unlock()
	if pending || s.TextDropped() != 1 {
		t.Fatal("drop", pending, s.TextDropped())
	}
	if text, found, known := s.TextCached(date, "service", 7); !known || !found || text != "/a/b" {
		t.Fatal("dropped text not readable", text, found, known)
	}
}

// Tag contributions made before a purge of their date are discarded when
// they reach the inbox afterwards (Submit after the engine lock), as the
// entries already in the inbox are.
func TestTagEntryPurgedBeforeSubmitIsDiscarded(t *testing.T) {
	s, dir := liveStore(t, Config{})
	const date = "20261022"
	tm := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC).UnixMilli()
	x := &protocol.XLogPack{EndTime: tm, TxID: 1, Service: 1, ObjHash: 2}
	for n := 0; n < tagInboxMax; n++ {
		w, err := s.PrepareXLogTags("java", x)
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := s.tagEnqueue(w.tag); !ok || err != nil {
			t.Fatal("fill", ok, err)
		}
	}
	w, err := s.PrepareXLogTags("java", x)
	if err != nil {
		t.Fatal(err)
	}
	if done, err := s.Offer(w); done || err != nil {
		t.Fatal("offer to a full inbox", done, err)
	}
	s.tags.mu.Lock()
	s.tagDiscardLocked([]string{filepath.Join(dir, date)})
	s.tags.mu.Unlock()
	if err := s.Submit(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	s.tags.inboxMu.Lock()
	n := len(s.tags.inbox)
	s.tags.inboxMu.Unlock()
	if st := s.TagCountStats(); n != 0 || st.InboxDropped != 0 || st.Backpressure != 0 {
		t.Fatal("purged entry reached the inbox", n, st)
	}
	// An entry made after the purge is queued.
	w, err = s.PrepareXLogTags("java", x)
	if err != nil {
		t.Fatal(err)
	}
	if done, err := s.Offer(w); !done || err != nil {
		t.Fatal("offer after the purge", done, err)
	}
	s.tags.inboxMu.Lock()
	n = len(s.tags.inbox)
	s.tags.inboxMu.Unlock()
	if n != 1 {
		t.Fatal("inbox", n)
	}
}

// An inbox entry older than the purge log (more than tagPurgeLogMax purges
// since it was made) is kept: the log cannot tell whether its path was
// purged, and dropping it would drop contributions of every path.
func TestTagEntryOlderThanPurgeLogIsKept(t *testing.T) {
	s, dir := liveStore(t, Config{})
	tm := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC).UnixMilli()
	w, err := s.PrepareXLogTags("java", &protocol.XLogPack{EndTime: tm, TxID: 1, Service: 1, ObjHash: 2})
	if err != nil {
		t.Fatal(err)
	}
	s.tags.mu.Lock()
	for n := 0; n <= tagPurgeLogMax; n++ {
		s.tagDiscardLocked([]string{filepath.Join(dir, "20261021")}) // another date
	}
	s.tags.mu.Unlock()
	if err := s.Submit(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	s.tags.inboxMu.Lock()
	n := len(s.tags.inbox)
	s.tags.inboxMu.Unlock()
	if n != 1 {
		t.Fatal("entry dropped although its path was not purged", n)
	}
}
