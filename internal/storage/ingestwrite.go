// Copyright Scouter authors, Apache-2.0. Ingest writes offered without waiting.
package storage

import (
	"context"
	"unsafe"
)

// Write is one validated ingest-path record for a Store writer queue: a text
// (PrepareText), an XLog (PrepareXLogEncoded), a profile (PrepareProfile) or
// the tag contributions of an XLog or alert (PrepareXLogTags,
// PrepareAlertTags). Prepare* report the errors the queuing call would
// (validation, never a full queue) and do not touch the queue.
//
// Offer queues a Write when that needs no wait; Submit queues it with the
// bounded wait of its queue (text, XLog and profile queues 100ms, tag inbox
// tagInboxWait, once per saturation, then a drop, counted as before). The
// ingest engine offers under its engine lock and submits after releasing it
// the writes whose queue was full, so a full queue makes only the collector
// that has to wait wait, not every collector (ingest CONTRACT §11). Writes of
// one queue enter it in the order they are offered and submitted by one
// goroutine; the caller keeps that order.
//
// A Write owns what it was prepared from as the queuing calls do (the
// profile bytes, the shared XLog encoding). The zero Write has nothing to
// write.
type Write struct {
	queue WriteQueue
	text  *textItem
	pack  *packItem
	packQ *packQueue
	tag   tagInboxEntry
}

// WriteQueue names the queue a Write goes to.
type WriteQueue uint8

const (
	NoQueue WriteQueue = iota
	TextQueue
	XLogQueue
	ProfileQueue
	TagQueue
)

// Queue is the queue w goes to, NoQueue for the zero Write.
func (w Write) Queue() WriteQueue { return w.queue }

// Offer queues w unless its queue is full and not saturated: done=false then,
// and the caller submits (or drops) w later. Nothing is queued then, but a
// text stays pending, readable as while PutText waited for room (and a purge
// before Submit discards it). A saturated queue drops w at once (done,
// counted and logged as by the queuing calls). It takes only the queue's
// leaf lock and never waits. The error is ErrClosed after Close.
func (s *Store) Offer(w Write) (done bool, err error) {
	return s.offer(context.Background(), w, false)
}

// Submit queues w as the queuing call (PutTextContext, EnqueueXLogContext,
// ...) would: a full queue is waited for at most its bound, then w is dropped
// and counted (nil error). ctx ends the wait. A record whose directory a
// purge or delete removed since Prepare is not written (XLog, profile: the
// writer discards it; text: taken out of pending; tag: discarded at the
// inbox).
func (s *Store) Submit(ctx context.Context, w Write) error {
	_, err := s.offer(ctx, w, true)
	return err
}

// Drop drops w as its queue drops a record it has no room for (counted and
// logged as such; a text stays readable in the dropped-text cache), for a
// caller that cannot keep it to submit later.
func (s *Store) Drop(w Write) {
	switch w.queue {
	case TextQueue:
		_ = s.dropText(w.text)
	case XLogQueue, ProfileQueue:
		w.packQ.dropped.Add(1)
		s.logJava(w.packQ.code, w.packQ.what)
	case TagQueue:
		s.tagInboxDropped.Add(1)
		s.logJava("S188", "XLogTagCount queue exceeded!!")
	}
}

// Size estimates the memory w holds while it waits to be submitted.
func (w Write) Size() int {
	switch w.queue {
	case TextQueue:
		return len(w.text.pack.Text) + len(w.text.ref.path) + len(w.text.ref.key) + 64
	case XLogQueue, ProfileQueue:
		return w.pack.size
	case TagQueue:
		// The entry with the XLog fields is held by value; an alert's
		// contributions are a slice of their own.
		n := int(unsafe.Sizeof(w.tag)) + len(w.tag.x.country)
		for _, t := range w.tag.tags {
			n += int(unsafe.Sizeof(t)) + len(t.name) + len(t.text)
		}
		return n
	}
	return 0
}

func (s *Store) offer(ctx context.Context, w Write, wait bool) (bool, error) {
	switch w.queue {
	case TextQueue:
		return s.offerText(ctx, w.text, wait)
	case XLogQueue, ProfileQueue:
		return s.offerPack(ctx, w.packQ, w.pack, wait)
	case TagQueue:
		return s.tagOffer(ctx, w.tag, wait)
	}
	return true, nil
}
