// ThreadList/ThreadUtil response schema: Copyright 2015 Scouter authors,
// Apache-2.0. Independent implementation of pinned source dee47fb5.
package service

import (
	"bytes"
	"context"
	"runtime"
	"strconv"
	"strings"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

const (
	maxServerGoroutines = 10000
	maxServerStackBytes = 4 << 20
	// ThreadUtil.appendThreadDetail: tmb.getThreadInfo(id, 500).
	maxServerStackFrames = 500
)

// goroutineInfo is one goroutine of a runtime.Stack(all=true) dump. A Go
// goroutine is not a JVM thread: it has no name, CPU time, lock or
// contention counters. See management-commands.md for the mapping.
type goroutineInfo struct {
	id     int64
	status string // full bracket text, e.g. "chan receive, 2 minutes"
	frames []string
	entry  string // function the goroutine started in, "" if elided
}

func (g goroutineInfo) name() string {
	if g.entry == "" {
		return "goroutine " + strconv.FormatInt(g.id, 10)
	}
	return "goroutine " + strconv.FormatInt(g.id, 10) + " " + g.entry
}

// state is the wait reason / run state, the first bracket component.
func (g goroutineInfo) state() string {
	if i := strings.IndexByte(g.status, ','); i >= 0 {
		return g.status[:i]
	}
	return g.status
}

// goroutineDump captures every goroutine. Over 10,000 goroutines or a dump
// over 4 MiB is ErrLimit, never a silently truncated list.
func goroutineDump(ctx context.Context) ([]goroutineInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runtime.NumGoroutine() > maxServerGoroutines {
		return nil, transport.ErrLimit
	}
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		if len(buf) >= maxServerStackBytes {
			return nil, transport.ErrLimit
		}
		buf = make([]byte, min(2*len(buf), maxServerStackBytes))
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	out := parseGoroutineDump(buf)
	if len(out) > maxServerGoroutines {
		return nil, transport.ErrLimit
	}
	return out, ctx.Err()
}

// parseGoroutineDump reads the runtime traceback text format: blocks
// separated by a blank line, a "goroutine N [...]:" header, then function /
// "\tfile:line +0xpc" pairs and an optional "created by" pair.
func parseGoroutineDump(dump []byte) []goroutineInfo {
	var out []goroutineInfo
	for _, block := range bytes.Split(dump, []byte("\n\n")) {
		lines := strings.Split(strings.TrimRight(string(block), "\n"), "\n")
		header := lines[0]
		if !strings.HasPrefix(header, "goroutine ") || !strings.HasSuffix(header, "]:") {
			continue
		}
		rest := header[len("goroutine "):]
		end := strings.IndexAny(rest, " [")
		if end < 0 {
			continue
		}
		id, err := strconv.ParseInt(rest[:end], 10, 64)
		if err != nil {
			continue
		}
		g := goroutineInfo{id: id}
		if open := strings.IndexByte(header, '['); open >= 0 {
			g.status = header[open+1 : len(header)-2]
		}
		elided := false
		last := ""
		for i := 1; i < len(lines); i++ {
			line := lines[i]
			if strings.HasPrefix(line, "\t") {
				continue
			}
			if strings.HasPrefix(line, "...") {
				elided = true
				continue
			}
			location := ""
			if i+1 < len(lines) && strings.HasPrefix(lines[i+1], "\t") {
				location = strings.TrimPrefix(lines[i+1], "\t")
				if pc := strings.LastIndex(location, " +0x"); pc >= 0 {
					location = location[:pc]
				}
			}
			if strings.HasPrefix(line, "created by ") {
				frame := line
				if location != "" {
					frame += " (" + location + ")"
				}
				g.frames = append(g.frames, frame)
				continue
			}
			fn := stripCallArgs(line)
			last = fn
			g.frames = append(g.frames, fn+"("+location+")")
		}
		if !elided {
			g.entry = last
		}
		out = append(out, g)
	}
	return out
}

// stripCallArgs removes the trailing argument list "(0x1, ...)" of a
// traceback function line, keeping receiver parentheses like "(*T)".
func stripCallArgs(line string) string {
	if !strings.HasSuffix(line, ")") {
		return line
	}
	depth := 0
	for i := len(line) - 1; i >= 0; i-- {
		switch line[i] {
		case ')':
			depth++
		case '(':
			depth--
			if depth == 0 {
				return line[:i]
			}
		}
	}
	return line
}

// serverThreadList is ThreadUtil.getThreadList's MapPack: id, name, stat,
// cpu lists in that order. CPU time is not measurable per goroutine: 0.
func (s *Service) serverThreadList(ctx context.Context, stream *transport.Stream) error {
	gs, err := goroutineDump(ctx)
	if err != nil {
		return err
	}
	ids := make([]protocol.Value, 0, len(gs))
	names := make([]protocol.Value, 0, len(gs))
	stats := make([]protocol.Value, 0, len(gs))
	cpus := make([]protocol.Value, 0, len(gs))
	for _, g := range gs {
		ids = append(ids, decimal(g.id))
		names = append(names, stringValue(g.name()))
		stats = append(stats, stringValue(g.state()))
		cpus = append(cpus, decimal(0))
	}
	return stream.ResponsePack(newMap(entry("id", list(ids)), entry("name", list(names)), entry("stat", list(stats)), entry("cpu", list(cpus))))
}

// serverThreadDetail is ThreadUtil.getThreadDetail: id 0 or an unknown id is
// an empty MapPack (the item is still sent).
func (s *Service) serverThreadDetail(ctx context.Context, id int64, stream *transport.Stream) error {
	if id == 0 {
		return stream.ResponsePack(newMap())
	}
	gs, err := goroutineDump(ctx)
	if err != nil {
		return err
	}
	for _, g := range gs {
		if g.id != id {
			continue
		}
		frames := g.frames
		if len(frames) > maxServerStackFrames {
			frames = frames[:maxServerStackFrames]
		}
		return stream.ResponsePack(newMap(
			entry("Thread Id", decimal(g.id)),
			entry("Thread Cpu Time", decimal(0)),
			entry("Thread User Time", decimal(0)),
			entry("Blocked Count", decimal(0)),
			entry("Blocked Time", decimal(-1)),
			entry("Waited Count", decimal(0)),
			entry("Waited Time", decimal(-1)),
			entry("Lock Owner Id", decimal(-1)),
			entry("Lock Name", stringValue("")),
			entry("Lock Owner Name", stringValue("")),
			entry("Thread Name", stringValue(g.name())),
			entry("Stack Trace", stringValue(strings.Join(frames, "\n"))),
			entry("State", stringValue(g.status)),
		))
	}
	return stream.ResponsePack(newMap())
}
