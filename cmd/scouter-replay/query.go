package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
)

type queryConfig struct {
	Addr, User, Password string
	Concurrency          int
	Interval, Window     time.Duration
	Timeout              time.Duration
	Max                  int
	Objects              []object
	Location             *time.Location
	Codec                txidCodec
	Schedule             schedule
	// IdleClose is the connection idle time from which a request that got
	// no response byte (EOF/reset) is taken as the server's idle close and
	// not as a server error; 0 uses defaultIdleClose.
	IdleClose time.Duration
}

// defaultIdleClose: the server closes a Client connection idle for
// net_tcp_client_so_timeout_ms (default 8000, Java and Go), so with
// --query-interval >= 8s every request may find its connection closed. The
// worker measures idle from the last complete response, which is no earlier
// than the server's timer start; 500ms absorb timer and scheduling skew.
const defaultIdleClose = 8*time.Second - 500*time.Millisecond

type queryStats struct {
	Latency        histogram
	Requests       uint64
	Errors         uint64
	Records        uint64
	LastError      string
	lateStarts     uint64
	idleReconnects uint64
}

type queryResult struct {
	mu         sync.Mutex
	byCommand  map[string]*queryStats
	visibility histogram // scheduled send instant -> first seen by a realtime query
}

func (r *queryResult) stats(cmd string) *queryStats {
	s := r.byCommand[cmd]
	if s == nil {
		s = &queryStats{}
		r.byCommand[cmd] = s
	}
	return s
}

// runQueries runs Concurrency workers until ctx is done. Each worker owns one TCP
// connection and alternates TRANX_LOAD_TIME_GROUP (recent Window) and
// TRANX_REAL_TIME_GROUP (own cursor) every Interval. A request is timed from its
// scheduled tick; when a request overruns, the next tick is not skipped silently
// (late starts are counted) so latency is not hidden by coordinated omission.
func runQueries(ctx context.Context, cfg queryConfig) *queryResult {
	res := &queryResult{byCommand: map[string]*queryStats{}}
	var wg sync.WaitGroup
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			queryWorker(ctx, cfg, worker, res)
		}(w)
	}
	wg.Wait()
	return res
}

func queryWorker(ctx context.Context, cfg queryConfig, worker int, res *queryResult) {
	idleClose := cfg.IdleClose
	if idleClose <= 0 {
		idleClose = defaultIdleClose
	}
	var c *client
	defer func() {
		if c != nil {
			c.Close()
		}
	}()
	var loop, index int64
	cursorReady := false
	// Stagger workers across the interval.
	next := time.Now().Add(cfg.Interval * time.Duration(worker) / time.Duration(max(cfg.Concurrency, 1)))
	for n := worker; ; n++ {
		wait := time.Until(next)
		if wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		} else if ctx.Err() != nil {
			return
		}
		scheduled := next
		next = next.Add(cfg.Interval)
		if time.Now().After(next) {
			res.mu.Lock()
			res.stats("_worker").lateStarts++
			res.mu.Unlock()
		}
		if c == nil {
			var err error
			c, err = dialClient(ctx, cfg.Addr, cfg.User, cfg.Password, cfg.Timeout)
			res.mu.Lock()
			s := res.stats("LOGIN")
			s.Requests++
			if err != nil {
				s.Errors++
				s.LastError = err.Error()
			} else {
				s.Latency.record(time.Since(scheduled).Nanoseconds())
			}
			res.mu.Unlock()
			if err != nil {
				c = nil
				continue
			}
			cursorReady = false
		}
		var cmd string
		var records int
		var seen []int64
		request := func(c *client) error {
			records, seen = 0, nil
			var err error
			if n%2 == 0 {
				cmd = "TRANX_LOAD_TIME_GROUP"
				now := time.Now().UnixMilli()
				to := now
				from := now - cfg.Window.Milliseconds()
				date := scoutertime.Date(to, cfg.Location)
				if dayStart, e := scoutertime.ParseDate(date, cfg.Location); e == nil && from < dayStart {
					from = dayStart
				}
				records, err = c.history(ctx, date, from, to, cfg.Max, cfg.Objects, nil)
				return err
			}
			cmd = "TRANX_REAL_TIME_GROUP"
			if !cursorReady {
				// Start from the current head; the ring backlog is not a visibility sample.
				loop, index, err = c.latestCursor(ctx)
				cursorReady = err == nil
			}
			if err == nil {
				records, err = c.realtimeCursor(ctx, &loop, &index, cfg.Objects, func(x *protocol.XLogPack) {
					seen = append(seen, x.TxID)
				})
			}
			return err
		}
		idle := time.Since(c.lastUsed)
		err := request(c)
		if err != nil && ctx.Err() == nil && errors.Is(err, errClosedBeforeResponse) && idle >= idleClose {
			// The server closed the idle connection (not a server error):
			// reconnect quietly and repeat the request once. The realtime
			// cursor (loop, index) is a position in the server's global XLog
			// ring, not session state, so it is kept: XLogs that arrived
			// while idle are still delivered as visibility samples.
			c.conn.Close()
			res.mu.Lock()
			res.stats("_worker").idleReconnects++
			res.mu.Unlock()
			var derr error
			if c, derr = dialClient(ctx, cfg.Addr, cfg.User, cfg.Password, cfg.Timeout); derr != nil {
				res.mu.Lock()
				s := res.stats("LOGIN")
				s.Requests++
				s.Errors++
				s.LastError = derr.Error()
				res.mu.Unlock()
				c = nil
				continue
			}
			err = request(c)
		}
		done := time.Now()
		if ctx.Err() != nil && err != nil {
			return // cancelled mid-request at shutdown; not a server error
		}
		res.mu.Lock()
		s := res.stats(cmd)
		s.Requests++
		s.Records += uint64(records)
		s.Latency.record(done.Sub(scheduled).Nanoseconds())
		if err != nil {
			s.Errors++
			s.LastError = err.Error()
		}
		for _, txid := range seen {
			if i := cfg.Codec.decode(txid); i < cfg.Schedule.total {
				res.visibility.record(done.Sub(cfg.Schedule.at(i)).Nanoseconds())
			}
		}
		res.mu.Unlock()
		if err != nil {
			c.conn.Close()
			c = nil
		}
	}
}

type queryReport struct {
	Commands         map[string]queryCommandReport `json:"commands"`
	LateWorkerStarts uint64                        `json:"late_worker_starts"`
	// IdleReconnects counts requests that found their connection closed by
	// the server's idle timeout and were repeated on a new connection (not
	// errors; the repeated request's latency includes the reconnect).
	IdleReconnects       uint64         `json:"idle_reconnects"`
	RealtimeVisibility   latencySummary `json:"realtime_visibility_latency"`
	RealtimeVisibilityNB string         `json:"realtime_visibility_note"`
}
type queryCommandReport struct {
	Requests  uint64         `json:"requests"`
	Errors    uint64         `json:"errors"`
	Records   uint64         `json:"records"`
	LastError string         `json:"last_error,omitempty"`
	Latency   latencySummary `json:"latency_from_scheduled_tick"`
}

func (r *queryResult) report() queryReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := queryReport{Commands: map[string]queryCommandReport{}, RealtimeVisibility: r.visibility.latency(),
		RealtimeVisibilityNB: "scheduled send instant -> realtime query response; includes the polling interval, so it is an upper bound of server visibility delay"}
	for cmd, s := range r.byCommand {
		if cmd == "_worker" {
			out.LateWorkerStarts, out.IdleReconnects = s.lateStarts, s.idleReconnects
			continue
		}
		out.Commands[cmd] = queryCommandReport{Requests: s.Requests, Errors: s.Errors, Records: s.Records, LastError: s.LastError, Latency: s.Latency.latency()}
	}
	return out
}
