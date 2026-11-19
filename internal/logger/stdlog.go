package logger

import (
	"io"
	"strings"
)

// StdLogWriter returns an io.Writer for log.SetOutput (or log.New) that keeps
// the existing stream and also records each message in the server log file:
//
//	log.SetOutput(lg.StdLogWriter(os.Stderr))
//
// Every Write is first passed unchanged to stream (nil: file only). For the
// file, a leading standard-library timestamp ("2006/01/02 ", optionally
// "15:04:05" with ".000000" fraction and a space) is removed, the trailing
// newline is dropped and the rest is logged like Logger.println(message):
// "yyyyMMdd HH:mm:ss message" in the server time zone. A multi-line message
// stays one record. These lines carry no ID, so mgr_log_ignore_ids cannot
// filter them (the same as Java's ID-less Logger.println).
func (l *Logger) StdLogWriter(stream io.Writer) io.Writer {
	return stdLogWriter{l: l, stream: stream}
}

type stdLogWriter struct {
	l      *Logger
	stream io.Writer
}

func (w stdLogWriter) Write(p []byte) (int, error) {
	var err error
	if w.stream != nil {
		_, err = w.stream.Write(p)
	}
	if w.l != nil {
		w.l.Println(stripStdLogPrefix(strings.TrimSuffix(string(p), "\n")))
	}
	return len(p), err
}

// stripStdLogPrefix removes "yyyy/mm/dd " and an optional "hh:mm:ss[.ffffff] ".
func stripStdLogPrefix(s string) string {
	if !matchDigits(s, "dddd/dd/dd ") {
		return s
	}
	s = s[11:]
	if !matchDigits(s, "dd:dd:dd") {
		return s
	}
	rest := s[8:]
	if strings.HasPrefix(rest, ".") {
		i := 1
		for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
			i++
		}
		rest = rest[i:]
	}
	if strings.HasPrefix(rest, " ") {
		return rest[1:]
	}
	return s
}

func matchDigits(s, pattern string) bool {
	if len(s) < len(pattern) {
		return false
	}
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == 'd' {
			if s[i] < '0' || s[i] > '9' {
				return false
			}
		} else if s[i] != pattern[i] {
			return false
		}
	}
	return true
}
