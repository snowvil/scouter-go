package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
)

// textCacheResetter mirrors scouter.server.core.TextCacheReset. The Java agent
// sends each dictionary text (service, apicall, sql, ...) once and then keeps
// it in DataProxy's sent-set until DataProxy.reset(): its own date-unit change
// or an OBJECT_RESET_CACHE request from the server. The Java server therefore
// calls every live agent 30s after start (oldunit starts at 0) and again on
// each date-unit change, so a freshly started server with an empty or new
// text database receives the texts again instead of showing empty service
// names until midnight. Agents without a TCP connection are skipped like
// AgentCall S501; a failed call is logged and the loop continues (S114).
//
// Intended difference: Java tries each live agent once per date unit, so an
// agent whose TCP session was not up 30s after a restart (or was busy, or
// failed) keeps its sent-set until midnight and the texts it lost while the
// server was down stay empty. Go keeps those object hashes for the rest of
// the date unit and calls them again once they have a session, within the
// textCacheRetry* bounds. A repeated reset only makes the agent resend texts
// lazily, so an extra call is harmless; the bounds stop a storm.
//
// Intended difference: Java calls the agents one after another and waits up
// to the agent so_timeout (60s) for each, so slow agents delay the reset of
// every agent after them. The agents are independent, so Go runs up to
// textCacheResetParallel calls at once. A call has no deadline of its own: it
// waits for the answer under the relay deadline (so_timeout+RequestTimeout)
// rather than leaving an orphan relay behind, so the pass holds at most
// textCacheResetParallel relay budget permits and no orphan slot.
type textCacheResetter struct {
	location *time.Location
	now      func() time.Time
	live     func(ctx context.Context) ([]int32, error)
	// call is made from up to parallel goroutines at once.
	call func(ctx context.Context, hash int32) (bool, error)
	// parallel bounds the concurrent calls; 0 asks liveParallel (the live
	// go_text_cache_reset_parallel), and its 0 uses textCacheResetParallel.
	parallel     int
	liveParallel func() int
	// connected reports whether hash has an agent session now. nil treats
	// every pending hash as connected (each retry is then a real call).
	connected func(hash int32) bool
	lastDate  string
	// pending holds the hashes the current date unit's pass missed, in pass
	// order. It never outlives the date unit.
	pending   []*textCacheRetry
	stats     textCacheRetryStats
	statsDate string
	lastLog   time.Time
	// errLogAt and errSuppressed rate-limit the per-agent error line.
	errLogAt      time.Time
	errSuppressed int
}

type textCacheRetry struct {
	hash     int32
	attempts int
	next     time.Time
}

const (
	textCacheResetDelay    = 30 * time.Second
	textCacheResetInterval = 2 * time.Second
	textCacheResetCommand  = "OBJECT_RESET_CACHE"
	// textCacheResetParallel bounds the concurrent calls of a pass or retry
	// round (16 * 8MiB of the 1GiB relay budget); go_text_cache_reset_parallel
	// replaces it live, from the next pass or round.
	textCacheResetParallel = 16

	// textCacheRetryMax bounds the hashes kept per date unit; the pass only
	// adds hashes from the live list, so it is also at most the live count.
	// It matches ingest.DefaultMaxObjects (an entry is a few dozen bytes), so
	// objects that never have a TCP session (telegraf, zipkin, HTTP register)
	// cannot push a late Java Agent out of the retry list.
	textCacheRetryMax = 16384
	// textCacheRetryPerRound bounds the calls of one retry round (one tick
	// every textCacheResetInterval): at most 4 calls, made at once.
	textCacheRetryPerRound = 4
	// textCacheRetryGap is the wait after a busy/failed call, doubled after
	// each further failure (10s, 20s, 40s, 80s).
	textCacheRetryGap = 10 * time.Second
	// textCacheRetryAttempts is the number of retry calls per hash per date
	// unit before it is given up.
	textCacheRetryAttempts = 5
	// textCacheRetryLogEvery bounds the retry summary line.
	textCacheRetryLogEvery = time.Minute
	// textCacheErrorLogEvery bounds the per-agent error line of a pass; the
	// pass summary line still counts every error.
	textCacheErrorLogEvery = 10 * time.Second
)

// textCacheResetResult is one date-change pass over the live agents.
type textCacheResetResult struct {
	Date    string
	Live    int
	Called  int
	Missing int
	Errors  int
	// Refused counts the agents not called because the relay budget was
	// full (errRelayBudget); they are kept for retry like Missing.
	Refused int
	// Pending is the number of missed hashes kept for retry; Overflow the
	// missed hashes over textCacheRetryMax that are not retried.
	Pending  int
	Overflow int
}

// textCacheRetryStats sums retry outcomes until the next summary line.
type textCacheRetryStats struct {
	Called    int // retry call reached the agent: removed
	Failed    int // retry call missing/busy/error/budget refused: kept for a later round
	GaveUp    int // textCacheRetryAttempts used: removed
	Discarded int // still pending when the date unit changed: removed
}

func (s textCacheRetryStats) empty() bool { return s == textCacheRetryStats{} }

// touch dates the sums by the date unit of their first outcome, so the line
// flushed after a date-change pass names the date the sums belong to.
func (t *textCacheResetter) touch() {
	if t.stats.empty() {
		t.statsDate = t.lastDate
	}
}

// tick returns a result only for a pass on a Java date-unit change. Between
// passes it runs one bounded retry round over the pending hashes.
func (t *textCacheResetter) tick(ctx context.Context) (*textCacheResetResult, error) {
	date := scoutertime.Date(t.now().UnixMilli(), t.location)
	if date == t.lastDate {
		return nil, t.retry(ctx)
	}
	hashes, err := t.live(ctx)
	if err != nil {
		return nil, err
	}
	// The previous date unit's misses are covered by this pass.
	if len(t.pending) > 0 {
		t.touch()
		t.stats.Discarded += len(t.pending)
		clear(t.pending)
		t.pending = t.pending[:0]
	}
	// Advance only after the live list was read so a transient error retries.
	t.lastDate = date
	result := &textCacheResetResult{Date: date, Live: len(hashes)}
	for i, c := range t.callAll(ctx, hashes) {
		hash := hashes[i]
		switch {
		case !c.made || (c.err != nil && errors.Is(c.err, context.Canceled) && ctx.Err() != nil):
			return result, ctx.Err()
		case errors.Is(c.err, errRelayBudget):
			result.Refused++
			t.keep(result, hash, t.now().Add(textCacheRetryGap))
		case c.err != nil:
			result.Errors++
			t.logAgentError(hash, c.err)
			t.keep(result, hash, t.now().Add(textCacheRetryGap))
		case c.called:
			result.Called++
		default:
			// No session now, or none free within the wait (the pool does not
			// tell them apart): retry once connected reports a session.
			result.Missing++
			t.keep(result, hash, t.now())
		}
	}
	return result, nil
}

// logAgentError writes one per-agent error line per textCacheErrorLogEvery
// with the number left out since the previous one.
func (t *textCacheResetter) logAgentError(hash int32, err error) {
	now := t.now()
	if !t.errLogAt.IsZero() && now.Sub(t.errLogAt) < textCacheErrorLogEvery {
		t.errSuppressed++
		return
	}
	more := t.errSuppressed
	t.errLogAt, t.errSuppressed = now, 0
	if more > 0 {
		log.Printf("text cache reset: agent %d: %v (%d more agent errors since the last line)", hash, err, more)
		return
	}
	log.Printf("text cache reset: agent %d: %v", hash, err)
}

func (t *textCacheResetter) keep(result *textCacheResetResult, hash int32, next time.Time) {
	if len(t.pending) >= textCacheRetryMax {
		result.Overflow++
		return
	}
	t.pending = append(t.pending, &textCacheRetry{hash: hash, next: next})
	result.Pending++
}

// textCacheCall is the outcome of one call; made is false for a hash not
// called because ctx ended first.
type textCacheCall struct {
	made, called bool
	err          error
}

// callAll calls hashes with at most parallel calls at once and returns the
// outcomes in hashes order. It returns once every started call has ended.
func (t *textCacheResetter) callAll(ctx context.Context, hashes []int32) []textCacheCall {
	out := make([]textCacheCall, len(hashes))
	parallel := t.parallel
	if parallel <= 0 && t.liveParallel != nil {
		parallel = t.liveParallel()
	}
	if parallel <= 0 {
		parallel = textCacheResetParallel
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(parallel, len(hashes)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(hashes) || ctx.Err() != nil {
					return
				}
				called, err := t.call(ctx, hashes[i])
				out[i] = textCacheCall{made: true, called: called, err: err}
			}
		}()
	}
	wg.Wait()
	return out
}

// retry calls at most textCacheRetryPerRound pending hashes that are due and
// connected, in pass order and at once. Per-agent errors are only counted:
// the pass already logged each hash once.
func (t *textCacheResetter) retry(ctx context.Context) error {
	if len(t.pending) == 0 {
		return nil
	}
	var due []*textCacheRetry
	for _, r := range t.pending {
		if len(due) >= textCacheRetryPerRound {
			break
		}
		if t.now().Before(r.next) || (t.connected != nil && !t.connected(r.hash)) {
			continue
		}
		due = append(due, r)
	}
	if len(due) == 0 {
		return ctx.Err()
	}
	hashes := make([]int32, len(due))
	for i, r := range due {
		hashes[i] = r.hash
	}
	done := map[*textCacheRetry]bool{}
	for i, c := range t.callAll(ctx, hashes) {
		r := due[i]
		if !c.made || (c.err != nil && ctx.Err() != nil) {
			// Shutdown: not this agent's failure.
			continue
		}
		t.touch()
		if c.err == nil && c.called {
			t.stats.Called++
			done[r] = true
			continue
		}
		if errors.Is(c.err, errRelayBudget) {
			// The budget was full: not the agent's attempt, so a long
			// saturation does not give the agent up for the day.
			t.stats.Failed++
			r.next = t.now().Add(textCacheRetryGap)
			continue
		}
		r.attempts++
		if r.attempts >= textCacheRetryAttempts {
			t.stats.GaveUp++
			done[r] = true
			continue
		}
		t.stats.Failed++
		r.next = t.now().Add(textCacheRetryGap << (r.attempts - 1))
	}
	kept := t.pending[:0]
	for _, r := range t.pending {
		if !done[r] {
			kept = append(kept, r)
		}
	}
	clear(t.pending[len(kept):])
	t.pending = kept
	return ctx.Err()
}

// retryLine returns the retry summary at most once per textCacheRetryLogEvery
// and resets the sums it reports. force reports at once (a pass happened).
func (t *textCacheResetter) retryLine(force bool) (string, bool) {
	if t.stats.empty() {
		return "", false
	}
	now := t.now()
	if !force && !t.lastLog.IsZero() && now.Sub(t.lastLog) < textCacheRetryLogEvery {
		return "", false
	}
	t.lastLog = now
	s := t.stats
	t.stats = textCacheRetryStats{}
	pending := len(t.pending)
	if t.statsDate != t.lastDate {
		pending = 0 // that date unit's hashes were discarded
	}
	return formatRetryLine(t.statsDate, s, pending), true
}

func formatRetryLine(date string, s textCacheRetryStats, pending int) string {
	return fmt.Sprintf("text cache reset: date %s retry called %d failed %d gave up %d discarded %d pending %d",
		date, s.Called, s.Failed, s.GaveUp, s.Discarded, pending)
}

// run is the daemon loop: ThreadUtil.sleep(30000) then a 2s poll.
func (t *textCacheResetter) run(ctx context.Context, delay, interval time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		result, err := t.tick(ctx)
		if err != nil && ctx.Err() != nil {
			return
		}
		// The previous date unit's retry sums go out before the new pass.
		if line, ok := t.retryLine(result != nil); ok {
			log.Print(line)
		}
		if err != nil {
			log.Printf("text cache reset: %v", err)
		} else if result != nil {
			line := fmt.Sprintf("text cache reset: date %s live %d called %d missing %d errors %d", result.Date, result.Live, result.Called, result.Missing, result.Errors)
			if result.Refused > 0 {
				line += fmt.Sprintf(" refused %d (relay budget full)", result.Refused)
			}
			log.Print(line)
			if result.Pending > 0 || result.Overflow > 0 {
				log.Printf("text cache reset: date %s retry pending %d overflow %d", result.Date, result.Pending, result.Overflow)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// newTextCacheResetter wires the resetter to the ingest object table and the
// agent connection pool.
func (r *Runtime) newTextCacheResetter() *textCacheResetter {
	return &textCacheResetter{
		location: r.config.Location,
		now:      time.Now,
		live: func(ctx context.Context) ([]int32, error) {
			objects, err := r.engine.Objects(ctx, "")
			if err != nil {
				return nil, err
			}
			hashes := make([]int32, 0, len(objects))
			for _, o := range objects {
				if o.Alive {
					hashes = append(hashes, o.ObjHash)
				}
			}
			return hashes, nil
		},
		call: func(ctx context.Context, hash int32) (bool, error) {
			// AgentCall sends an empty MapPack when the parameter is null and
			// discards the agent's echo of it. A host agent has no handler and
			// answers NoNEXT only; that still counts as called.
			return r.pool.requestDiscard(ctx, hash, textCacheResetCommand, &protocol.MapPack{})
		},
		connected:    r.pool.connected,
		liveParallel: func() int { return r.liveNow().Limits.TextCacheResetParallel },
	}
}
