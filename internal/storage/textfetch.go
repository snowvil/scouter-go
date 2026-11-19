// Copyright Scouter authors, Apache-2.0. Bounded-wait text lookups for the
// ingest path.
package storage

import (
	"context"
	"sync/atomic"
	"time"
)

// A text the caches cannot answer is read from the files under the Store
// gate. The gate is normally free within microseconds, but a file read (or
// a write another holder is in) waits for the disk when the page cache is
// cold, and then every caller queued at the gate waits with it. The ingest
// path must not: a UDP worker that waits there stops taking datagrams, the
// receive queue fills and datagrams are rejected. TextWithin gives such a
// caller a bounded wait instead. The read itself is done by the Store's
// fetcher goroutine and ends in the caches, so the next lookup of the text
// is answered from memory whether or not this caller waited for it.

// textFetchQueueMax bounds the reads waiting for the fetcher; textSlowHold
// is how long TextWithin stops waiting after one wait timed out. After it,
// the callers wait again (each at most its wait) until one of them times
// out: a store that is still slow costs a caller one wait per textSlowHold.
//
// The fetcher reads one text at a time. Right after a start, when the read
// cache is empty and many texts are asked for at once, the reads queue
// behind each other and some callers time out although the store is not
// slow: their XLogs keep no group for that moment (about as long as the
// backlog takes, then textSlowHold).
const (
	textFetchQueueMax = 4096
	textSlowHold      = time.Second
)

type textFetchKey struct {
	date, div string
	hash      int32
}

// textFetch is one read in flight; text, found and err are set before done
// is closed.
type textFetch struct {
	key   textFetchKey
	done  chan struct{}
	text  string
	found bool
	err   error
}

type textCacheCounters struct {
	hits, fileReads, evicted                     atomic.Uint64
	waited, unanswered, fetchRejected, slowdowns atomic.Uint64
}

// TextCacheStats are the text cache counters (/health). Hits are lookups
// answered without the Store gate, FileReads those that read the files
// under it. Of the TextWithin lookups that missed the caches, Waited got
// their answer within the wait, Unanswered did not (or did not wait, see
// Slowdowns) and FetchRejected found the read-ahead queue full; Slowdowns
// counts the waits that timed out.
type TextCacheStats struct {
	Entries, Missing   int
	Bytes, MaxBytes    int
	Hits, FileReads    uint64
	Evicted            uint64
	Waited, Unanswered uint64
	FetchRejected      uint64
	Slowdowns          uint64
	FetchPending       int
	Slow               bool
}

func (s *Store) TextCacheStats() TextCacheStats {
	q := &s.texts
	q.mu.Lock()
	out := TextCacheStats{Entries: len(q.read), Missing: len(q.missing), Bytes: q.readBytes, MaxBytes: q.readMax, FetchPending: len(q.fetching)}
	q.mu.Unlock()
	if out.MaxBytes <= 0 {
		out.MaxBytes = TextCacheDefaultBytes
	}
	c := &q.stats
	out.Hits, out.FileReads, out.Evicted = c.hits.Load(), c.fileReads.Load(), c.evicted.Load()
	out.Waited, out.Unanswered, out.FetchRejected, out.Slowdowns = c.waited.Load(), c.unanswered.Load(), c.fetchRejected.Load(), c.slowdowns.Load()
	out.Slow = time.Now().UnixNano() < q.slowUntil.Load()
	return out
}

func (s *Store) startTextFetcher() {
	q := &s.texts
	ctx, cancel := context.WithCancel(context.Background())
	q.fetching = map[textFetchKey]*textFetch{}
	q.fetchQ = make(chan *textFetch, textFetchQueueMax)
	q.fetchStop, q.fetchDone = cancel, make(chan struct{})
	go s.textFetcher(ctx)
}

// stopTextFetcher ends the fetcher (closeTexts, with textQueue.closed set):
// the read in flight is canceled and the queued ones are answered with
// ErrClosed.
func (s *Store) stopTextFetcher() {
	q := &s.texts
	if q.fetchStop == nil {
		return
	}
	q.fetchStop()
	<-q.fetchDone
}

func (s *Store) textFetcher(ctx context.Context) {
	q := &s.texts
	defer close(q.fetchDone)
	finish := func(f *textFetch) {
		q.mu.Lock()
		delete(q.fetching, f.key)
		q.mu.Unlock()
		close(f.done)
	}
	for {
		select {
		case f := <-q.fetchQ:
			f.text, f.found, f.err = s.textContext(ctx, f.key.date, f.key.div, f.key.hash, false)
			if ctx.Err() != nil {
				f.err = ErrClosed
			}
			finish(f)
		case <-ctx.Done():
			// closed is set, so nothing is queued from now on.
			for {
				select {
				case f := <-q.fetchQ:
					f.err = ErrClosed
					finish(f)
				default:
					return
				}
			}
		}
	}
}

// TextWithin is TextContext for a caller that must not wait for the Store
// gate longer than wait. known reports whether the answer (text, found, err)
// is the store's; when it is not, the text is being read ahead (unless the
// read-ahead queue is full) and a later lookup finds it in the caches. After
// a wait timed out, lookups do not wait at all for textSlowHold. wait <= 0
// only starts the read ahead.
func (s *Store) TextWithin(ctx context.Context, date, div string, hash int32, wait time.Duration) (text string, found, known bool, err error) {
	if text, found, known = s.TextCached(date, div, hash); known {
		return text, found, true, nil
	}
	q := &s.texts
	key := textFetchKey{date, div, hash}
	q.mu.Lock()
	if q.closed && q.fetchQ != nil {
		q.mu.Unlock()
		return "", false, true, ErrClosed
	}
	if q.fetchQ == nil || validDate(date) != nil || !textDivValid(div) {
		q.mu.Unlock()
		// No fetcher (a Store without its writers), or an invalid request,
		// which TextContext refuses before the gate.
		text, found, err = s.TextContext(ctx, date, div, hash)
		return text, found, true, err
	}
	f := q.fetching[key]
	if f == nil {
		f = &textFetch{key: key, done: make(chan struct{})}
		select {
		case q.fetchQ <- f:
			q.fetching[key] = f
		default:
			q.mu.Unlock()
			q.stats.fetchRejected.Add(1)
			return "", false, false, nil
		}
	}
	q.mu.Unlock()
	if wait <= 0 || time.Now().UnixNano() < q.slowUntil.Load() {
		select {
		case <-f.done: // answered meanwhile
			q.stats.waited.Add(1)
			return f.text, f.found, true, f.err
		default:
		}
		q.stats.unanswered.Add(1)
		return "", false, false, nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-f.done:
		q.stats.waited.Add(1)
		return f.text, f.found, true, f.err
	case <-ctx.Done():
		return "", false, false, ctx.Err()
	case <-timer.C:
		q.slowUntil.Store(time.Now().Add(textSlowHold).UnixNano())
		q.stats.slowdowns.Add(1)
		// Not a Java id: the original reads under its own lock and waits.
		s.logJava("GO-TEXT", "text read did not answer within "+wait.String()+" (slow Store gate or disk): lookups do not wait, XLogs are stored without their group meanwhile")
		q.stats.unanswered.Add(1)
		return "", false, false, nil
	}
}
