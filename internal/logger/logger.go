// Package logger reproduces the Scouter server log files: scouter.server.Logger
// (log_dir/server-yyyyMMdd.log or server.log, ID filter, rate limit, _trace
// stdout copy, hourly cleanup) and scouter.server.logs.RequestLogger
// (request-yyyyMMdd.log), plus the Java Pack/Value toString text used by the
// log_udp_* debug keys. See CONTRACT.md.
package logger

import (
	"bufio"
	"container/list"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds the live scouter.conf keys the logger reads. The caller parses
// scouter.conf (internal/config) and hands a new value to SetConfig on reload.
type Config struct {
	Dir       string   // log_dir
	Rotation  bool     // log_rotation_enabled
	KeepDays  int      // log_keep_days
	Trace     bool     // _trace
	IgnoreIDs []string // mgr_log_ignore_ids, already split on ','
}

// DefaultConfig is the Configure.java default.
func DefaultConfig() Config { return Config{Dir: "./logs", Rotation: true, KeepDays: 31} }

// Options are fixed for the Logger's lifetime.
type Options struct {
	Location        *time.Location   // server time zone for dates (required)
	Now             func() time.Time // clock; default time.Now
	Stdout          io.Writer        // _trace copy, log_udp_* lines, disk fallback; default os.Stdout
	Stderr          io.Writer        // logger's own warnings; default os.Stderr
	QueueSize       int              // records waiting for the owner goroutine; default 8192
	CleanupInterval time.Duration    // Java: 1 hour, first run one interval after start
}

const (
	rateLimitIDs  = 1000 // StringLongLinkedMap.setMax(1000)
	openRetryWait = time.Second
)

type recordKind uint8

const (
	kindServer recordKind = iota
	kindRequest
	kindStdout
	kindBarrier // flush and close ack; used by Sync
	kindCleanup // run clearOldLogs(ms) on the owner, then ack like a barrier
)

type record struct {
	kind   recordKind
	ms     int64  // call time; selects the dated file
	text   string // formatted line(s) without the final newline
	stdout bool   // _trace copy to stdout
	ack    chan struct{}
}

type liveConfig struct {
	Config
	ignore map[string]struct{}
}

// Stats are cumulative counters. Every accepted record ends in exactly one of
// Written, StdoutFallback or RequestLost, so Enqueued minus those three is the
// number still queued. Dropped, DroppedClosed, Filtered and RateLimited never
// reach the queue. Written means handed to the file's buffered writer; a
// later flush failure is counted in FileErrors.
type Stats struct {
	Enqueued       uint64 // accepted into the queue (cleanup's own delete lines included)
	Written        uint64 // written to a server/request log file, or to stdout for Stdout()
	Dropped        uint64 // queue full (or request command set full): not written
	DroppedClosed  uint64 // offered after Close: not written
	Filtered       uint64 // ID in mgr_log_ignore_ids
	RateLimited    uint64 // suppressed by println(id, sec, msg)
	StdoutFallback uint64 // server lines printed to stdout because the file could not be opened/written (Java fallback)
	RequestLost    uint64 // request lines not written because the file failed (Java drops them too)
	FileErrors     uint64 // open/write/flush/close failures of any log file
	Deleted        uint64 // old log files removed by cleanup
	DeleteErrors   uint64 // cleanup listing or remove failures
	Remaining      uint64 // records still queued at Close, written during Close
}

// Logger is safe for concurrent use. All file I/O happens on one owner
// goroutine started by New and stopped by Close. Methods on a nil *Logger are
// no-ops so components can hold an optional logger.
type Logger struct {
	cal     calendar
	now     func() time.Time
	stdout  io.Writer
	stderr  io.Writer
	cfg     atomic.Pointer[liveConfig]
	queue   chan record
	changed chan struct{}
	done    chan struct{}
	exited  chan struct{}

	sendMu sync.RWMutex // write: Close; read: enqueue (no send after Close)
	closed bool

	rateMu sync.Mutex
	rate   map[string]*list.Element // value: *rateEntry
	order  *list.List               // insertion order for eviction

	reqMu   sync.Mutex
	reqCmds map[string]struct{}

	enqueued, written, dropped, droppedClosed, filtered, rateLimited atomic.Uint64
	stdoutFallback, requestLost, fileErrors, deleted, deleteErrors   atomic.Uint64
	remaining                                                        atomic.Uint64

	// Owner goroutine state.
	serverSink   sink
	requestSink  sink
	cleanupSink  sink // warnings only; never opened
	stdoutBuf    *bufio.Writer
	cleanupEvery time.Duration
	closeErr     error
}

type rateEntry struct {
	id   string
	last int64
}

type sink struct {
	name      string
	dir, path string
	file      *os.File
	buf       *bufio.Writer
	failUntil time.Time
	warned    bool
}

// New starts the owner goroutine. The log file is opened lazily on the first
// record, as in Java.
func New(cfg Config, opt Options) (*Logger, error) {
	if opt.Location == nil {
		return nil, errors.New("logger: Location is required")
	}
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.Stdout == nil {
		opt.Stdout = os.Stdout
	}
	if opt.Stderr == nil {
		opt.Stderr = os.Stderr
	}
	if opt.QueueSize <= 0 {
		opt.QueueSize = 8192
	}
	if opt.CleanupInterval <= 0 {
		opt.CleanupInterval = time.Hour
	}
	l := &Logger{
		cal:          newCalendar(opt.Location),
		now:          opt.Now,
		stdout:       opt.Stdout,
		stderr:       opt.Stderr,
		queue:        make(chan record, opt.QueueSize),
		changed:      make(chan struct{}, 1),
		done:         make(chan struct{}),
		exited:       make(chan struct{}),
		rate:         make(map[string]*list.Element),
		order:        list.New(),
		reqCmds:      defaultRequestCommands(),
		stdoutBuf:    bufio.NewWriter(opt.Stdout),
		serverSink:   sink{name: "server"},
		requestSink:  sink{name: "request"},
		cleanupSink:  sink{name: "cleanup"},
		cleanupEvery: opt.CleanupInterval,
	}
	l.cfg.Store(newLive(cfg))
	go l.run()
	return l, nil
}

func newLive(c Config) *liveConfig {
	lc := &liveConfig{Config: c, ignore: make(map[string]struct{}, len(c.IgnoreIDs))}
	lc.IgnoreIDs = nil
	for _, id := range c.IgnoreIDs {
		if id = strings.TrimSpace(id); id != "" {
			lc.ignore[id] = struct{}{}
			lc.IgnoreIDs = append(lc.IgnoreIDs, id)
		}
	}
	return lc
}

// SetConfig atomically replaces the live settings (Java ConfObserver
// "Logger"). A log_dir or log_rotation_enabled change closes the open server
// file on the owner goroutine; the next line opens the new path.
// mgr_log_ignore_ids and _trace apply to the next call.
func (l *Logger) SetConfig(c Config) {
	if l == nil {
		return
	}
	l.cfg.Store(newLive(c))
	select {
	case l.changed <- struct{}{}:
	default:
	}
}

// Config returns the current live settings.
func (l *Logger) Config() Config {
	if l == nil {
		return Config{}
	}
	return l.cfg.Load().Config
}

// TraceEnabled reports _trace so callers can skip building trace messages.
func (l *Logger) TraceEnabled() bool { return l != nil && l.cfg.Load().Trace }

// Println is Logger.println(message): "yyyyMMdd HH:mm:ss message".
func (l *Logger) Println(msg string) {
	if l == nil {
		return
	}
	ms := l.now().UnixMilli()
	l.server(ms, l.cal.datetime(ms)+" "+msg)
}

// PrintlnID is Logger.println(id, message): "yyyyMMdd HH:mm:ss [id] message",
// dropped when id is in mgr_log_ignore_ids.
func (l *Logger) PrintlnID(id, msg string) { l.PrintlnIDRate(id, 0, msg) }

// PrintlnIDRate is Logger.println(id, sec, message): additionally at most one
// line per id every sec seconds (sec <= 0: no limit). The last-print time of
// up to 1000 ids is kept; the oldest inserted id is forgotten first.
func (l *Logger) PrintlnIDRate(id string, sec int, msg string) {
	l.PrintlnIDRateDetail(id, sec, msg, "")
}

// PrintlnIDRateDetail is Logger.println(id, sec, message, throwable): the
// detail (Java: the stack trace) follows on the next line(s) without a prefix.
func (l *Logger) PrintlnIDRateDetail(id string, sec int, msg, detail string) {
	if l == nil {
		return
	}
	ms := l.now().UnixMilli()
	if !l.checkOK(id, sec, ms) {
		return
	}
	text := l.cal.datetime(ms) + " [" + id + "] " + msg
	if detail != "" {
		text += "\n" + detail
	}
	l.server(ms, text)
}

// Trace is Logger.trace(message): "yyyyMMdd HH:mm:ss [TRACE] message" only
// while _trace is true. It ignores mgr_log_ignore_ids.
func (l *Logger) Trace(msg string) {
	if l == nil || !l.cfg.Load().Trace {
		return
	}
	ms := l.now().UnixMilli()
	l.server(ms, l.cal.datetime(ms)+" [TRACE] "+msg)
}

// Stdout writes one line to stdout through the owner goroutine (Java
// System.out.println for log_udp_* and log_udp_some_object). It never blocks
// the caller; a full queue counts Dropped.
func (l *Logger) Stdout(line string) {
	if l == nil {
		return
	}
	l.enqueue(record{kind: kindStdout, ms: l.now().UnixMilli(), text: line})
}

func (l *Logger) server(ms int64, text string) {
	l.enqueue(record{kind: kindServer, ms: ms, text: text, stdout: l.cfg.Load().Trace})
}

func (l *Logger) checkOK(id string, sec int, ms int64) bool {
	if _, ignored := l.cfg.Load().ignore[id]; ignored {
		l.filtered.Add(1)
		return false
	}
	if sec <= 0 {
		return true
	}
	l.rateMu.Lock()
	defer l.rateMu.Unlock()
	var last int64
	el := l.rate[id]
	if el != nil {
		last = el.Value.(*rateEntry).last
	}
	if ms < last+int64(sec)*1000 {
		l.rateLimited.Add(1)
		return false
	}
	if el != nil {
		el.Value.(*rateEntry).last = ms // put on an existing key keeps its position
		return true
	}
	for l.order.Len() >= rateLimitIDs {
		oldest := l.order.Front()
		delete(l.rate, oldest.Value.(*rateEntry).id)
		l.order.Remove(oldest)
	}
	l.rate[id] = l.order.PushBack(&rateEntry{id: id, last: ms})
	return true
}

func (l *Logger) enqueue(r record) bool {
	l.sendMu.RLock()
	defer l.sendMu.RUnlock()
	if l.closed {
		l.droppedClosed.Add(1)
		return false
	}
	select {
	case l.queue <- r:
		l.enqueued.Add(1)
		return true
	default:
		l.dropped.Add(1)
		return false
	}
}

// Sync waits until every record accepted before the call is written and the
// files and stdout are flushed. It returns false if the logger is closed.
// Unlike logging calls it waits for queue space instead of dropping.
func (l *Logger) Sync() bool {
	if l == nil {
		return false
	}
	return l.control(record{kind: kindBarrier})
}

// control sends a barrier-like record and waits for the owner to ack it.
func (l *Logger) control(r record) bool {
	ack := make(chan struct{})
	r.ack = ack
	l.sendMu.RLock()
	if l.closed {
		l.sendMu.RUnlock()
		return false
	}
	// The owner keeps draining until Close, and Close cannot start while the
	// read lock is held, so this blocking send always completes.
	l.queue <- r
	l.sendMu.RUnlock()
	select {
	case <-ack:
		return true
	case <-l.exited:
		return false
	}
}

// Stats returns a snapshot of the counters.
func (l *Logger) Stats() Stats {
	if l == nil {
		return Stats{}
	}
	return Stats{
		Enqueued: l.enqueued.Load(), Written: l.written.Load(), Dropped: l.dropped.Load(),
		DroppedClosed: l.droppedClosed.Load(), Filtered: l.filtered.Load(), RateLimited: l.rateLimited.Load(),
		StdoutFallback: l.stdoutFallback.Load(), RequestLost: l.requestLost.Load(), FileErrors: l.fileErrors.Load(), Deleted: l.deleted.Load(),
		DeleteErrors: l.deleteErrors.Load(), Remaining: l.remaining.Load(),
	}
}

// Close stops accepting records, writes everything already queued, closes the
// files and waits for the owner goroutine. It is idempotent and returns the
// first error from flushing or closing the files.
func (l *Logger) Close() error {
	if l == nil {
		return nil
	}
	l.sendMu.Lock()
	first := !l.closed
	l.closed = true
	l.sendMu.Unlock()
	if first {
		close(l.done)
	}
	<-l.exited
	return l.closeErr
}

// run is the owner goroutine: the only code that touches files and stdout.
func (l *Logger) run() {
	defer close(l.exited)
	ticker := time.NewTicker(l.cleanupEvery)
	defer ticker.Stop()
	for {
		select {
		case r := <-l.queue:
			l.handle(r)
			if len(l.queue) == 0 {
				l.flush()
			}
		case <-l.changed:
			l.applyConfig()
		case <-ticker.C:
			l.clearOldLogs(l.now().UnixMilli())
			l.flush()
		case <-l.done:
			// Close holds sendMu while setting closed, so nothing is sent after
			// this point: the queue length is final.
			for n := len(l.queue); n > 0; n-- {
				r := <-l.queue
				if r.ack == nil {
					l.remaining.Add(1)
				}
				l.handle(r)
			}
			l.flush()
			l.closeErr = errors.Join(l.serverSink.close(), l.requestSink.close())
			return
		}
	}
}

func (l *Logger) applyConfig() {
	cfg := l.cfg.Load()
	if l.serverSink.file != nil && l.serverSink.path != l.serverPath(cfg, l.now().UnixMilli()) {
		l.noteErr(&l.serverSink, l.serverSink.close())
	}
	if l.requestSink.file != nil && l.requestSink.dir != cfg.Dir {
		l.noteErr(&l.requestSink, l.requestSink.close())
	}
}

func (l *Logger) serverPath(cfg *liveConfig, ms int64) string {
	if cfg.Rotation {
		return filepath.Join(cfg.Dir, "server-"+l.cal.yyyymmdd(ms)+".log")
	}
	return filepath.Join(cfg.Dir, "server.log")
}

func (l *Logger) handle(r record) {
	switch r.kind {
	case kindCleanup:
		l.clearOldLogs(r.ms)
		l.flush()
		close(r.ack)
	case kindBarrier:
		l.flush()
		close(r.ack)
	case kindStdout:
		l.writeStdout(r.text)
		l.written.Add(1)
	case kindServer:
		if r.stdout {
			l.writeStdout(r.text)
		}
		cfg := l.cfg.Load()
		if err := l.serverSink.write(l, cfg.Dir, l.serverPath(cfg, r.ms), r.text); err != nil {
			l.warn(&l.serverSink, err)
			l.stdoutFallback.Add(1)
			if !r.stdout {
				l.writeStdout(r.text) // Java prints the message to System.out when the file fails
			}
			return
		}
		l.written.Add(1)
	case kindRequest:
		cfg := l.cfg.Load()
		if err := l.requestSink.write(l, cfg.Dir, filepath.Join(cfg.Dir, "request-"+l.cal.yyyymmdd(r.ms)+".log"), r.text); err != nil {
			l.warn(&l.requestSink, err) // Java prints the stack trace and drops the record
			l.requestLost.Add(1)
			return
		}
		l.written.Add(1)
	}
}

func (l *Logger) writeStdout(text string) {
	l.stdoutBuf.WriteString(text)
	l.stdoutBuf.WriteByte('\n')
}

func (l *Logger) flush() {
	if err := l.stdoutBuf.Flush(); err != nil {
		l.stdoutBuf.Reset(l.stdout) // stdout errors are not retried
	}
	l.noteErr(&l.serverSink, l.serverSink.flush())
	l.noteErr(&l.requestSink, l.requestSink.flush())
}

func (l *Logger) noteErr(s *sink, err error) {
	if err != nil {
		l.warn(s, err)
	}
}

// warn counts the failure and prints one stderr warning per failure episode
// of a sink; a later successful open/write re-arms the warning.
func (l *Logger) warn(s *sink, err error) {
	l.fileErrors.Add(1)
	if s.warned {
		return
	}
	s.warned = true
	io.WriteString(l.stderr, "scouter-go logger: "+s.name+" log: "+err.Error()+" (further errors are counted until it recovers)\n")
}

func (s *sink) write(l *Logger, dir, path, text string) error {
	if s.file == nil || s.path != path {
		l.noteErr(s, s.close())
		now := l.now()
		if now.Before(s.failUntil) {
			return errors.New("open retry pending after an earlier failure")
		}
		if err := os.MkdirAll(dir, 0o750); err != nil {
			s.failUntil = now.Add(openRetryWait)
			return err
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o640)
		if err != nil {
			s.failUntil = now.Add(openRetryWait)
			return err
		}
		s.file, s.path, s.dir = f, path, dir
		if s.buf == nil {
			s.buf = bufio.NewWriterSize(f, 32<<10)
		} else {
			s.buf.Reset(f)
		}
	}
	s.buf.WriteString(text)
	if err := s.buf.WriteByte('\n'); err != nil {
		s.close()
		return err
	}
	s.warned = false
	return nil
}

func (s *sink) flush() error {
	if s.file == nil {
		return nil
	}
	if err := s.buf.Flush(); err != nil {
		s.buf.Reset(s.file)
		return err
	}
	return nil
}

func (s *sink) close() error {
	if s.file == nil {
		return nil
	}
	err := s.buf.Flush()
	if cerr := s.file.Close(); err == nil {
		err = cerr
	}
	s.file, s.path, s.dir = nil, "", ""
	return err
}
