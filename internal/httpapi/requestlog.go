package httpapi

import (
	"bufio"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	requestLogPrefix = "http-request-"
	requestLogSuffix = ".log"
	// requestLogDate is Jetty's yyyy_MM_dd file name date.
	requestLogDate = "2006_01_02"
	// ncsaTime is NCSARequestLog's "dd/MMM/yyyy:HH:mm:ss Z" (English months).
	ncsaTime = "02/Jan/2006:15:04:05 -0700"
)

// requestLog is Jetty NCSARequestLog(log_dir/http-request-yyyy_mm_dd.log,
// append, extended, no cookies) over RolloverFileOutputStream. The file is
// opened lazily and rolled over on the first request of a new local day; at
// each rollover files with the same prefix/suffix whose modification time is
// older than keepDays days are removed (Jetty removeOldFiles). Every line is
// flushed before the request returns. Failures are counted and logged at most
// once per minute; the request itself is not affected. Lines arriving after
// close are not written (the API answers them 503).
type requestLog struct {
	dir      string
	keepDays int
	loc      *time.Location

	mu       sync.Mutex
	file     *os.File
	buf      *bufio.Writer
	day      string
	lastWarn time.Time
	closed   bool

	lines, failures, removed atomic.Uint64
}

func (l *requestLog) write(now time.Time, line string, logf func(string, ...any)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	day := now.In(l.loc).Format(requestLogDate)
	if l.file == nil || day != l.day {
		if err := l.rollover(now, day); err != nil {
			l.fail(now, err, logf)
			return
		}
	}
	l.buf.WriteString(line)
	l.buf.WriteByte('\n')
	if err := l.buf.Flush(); err != nil {
		l.fail(now, err, logf)
		return
	}
	l.lines.Add(1)
}

func (l *requestLog) fail(now time.Time, err error, logf func(string, ...any)) {
	l.failures.Add(1)
	if now.Sub(l.lastWarn) >= time.Minute {
		l.lastWarn = now
		logf("http request log: %v", err)
	}
}

func (l *requestLog) rollover(now time.Time, day string) error {
	if l.file != nil {
		l.buf.Flush()
		l.file.Close()
		l.file, l.buf = nil, nil
	}
	if err := os.MkdirAll(l.dir, 0o750); err != nil {
		return err
	}
	name := filepath.Join(l.dir, requestLogPrefix+day+requestLogSuffix)
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	l.file, l.buf, l.day = f, bufio.NewWriter(f), day
	l.removeOld(now, filepath.Base(name))
	return nil
}

func (l *requestLog) removeOld(now time.Time, current string) {
	if l.keepDays <= 0 {
		return
	}
	expired := now.AddDate(0, 0, -l.keepDays)
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		l.failures.Add(1)
		return
	}
	for _, e := range entries {
		name := e.Name()
		if name == current || e.IsDir() || !strings.HasPrefix(name, requestLogPrefix) || !strings.Contains(name[len(requestLogPrefix):], requestLogSuffix) {
			continue
		}
		info, err := e.Info()
		if err != nil || !info.ModTime().Before(expired) {
			continue
		}
		if err := os.Remove(filepath.Join(l.dir, name)); err != nil {
			l.failures.Add(1)
			continue
		}
		l.removed.Add(1)
	}
}

func (l *requestLog) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	if l.file == nil {
		return nil
	}
	err := l.buf.Flush()
	if cerr := l.file.Close(); err == nil {
		err = cerr
	}
	l.file, l.buf = nil, nil
	return err
}

// ncsaLine is AbstractNCSARequestLog.log with extended=true:
// addr - - [time] "METHOD URI PROTOCOL" status bytes "referer" "agent".
func ncsaLine(r *http.Request, rec *recorder, at time.Time) string {
	var b strings.Builder
	b.Grow(160)
	b.WriteString(remoteAddr(r.RemoteAddr))
	b.WriteString(" - - [")
	b.WriteString(at.Format(ncsaTime))
	b.WriteString("] \"")
	b.WriteString(dash(r.Method))
	b.WriteByte(' ')
	b.WriteString(dash(r.RequestURI))
	b.WriteByte(' ')
	b.WriteString(dash(r.Proto))
	b.WriteString("\" ")
	b.WriteString(strconv.Itoa(rec.code()))
	b.WriteByte(' ')
	b.WriteString(strconv.FormatInt(rec.bytes, 10))
	b.WriteByte(' ')
	header(&b, r, "Referer")
	b.WriteByte(' ')
	header(&b, r, "User-Agent")
	return b.String()
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// header writes "value", or "-" when the header is absent.
func header(b *strings.Builder, r *http.Request, name string) {
	b.WriteByte('"')
	if v, ok := r.Header[name]; ok && len(v) > 0 {
		b.WriteString(v[0])
	} else {
		b.WriteByte('-')
	}
	b.WriteByte('"')
}
