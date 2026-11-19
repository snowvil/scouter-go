// Pinned Scouter trace query/SpanStepBuilder semantics (Apache-2.0 source).
package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/transport"
)

// HandleTraceCommand lets the dispatcher register the five additional selectors.
func (s *Service) HandleTraceCommand(ctx context.Context, r transport.ClientRequest, stream *transport.Stream) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !r.Authenticated {
		return transport.ErrUnauthorized
	}
	switch r.Command {
	case "XLOG_READ_BY_GXID", "XLOG_LOAD_BY_TXIDS", "XLOG_LOAD_BY_GXID", "QUICKSEARCH_XLOG_LIST", "SEARCH_XLOG_LIST":
	default:
		return transport.ErrUnsupported
	}
	p, err := readMap(stream)
	if err != nil {
		return err
	}
	return s.traceQuery(ctx, r.Command, p, stream)
}

// traceLimit is req_search_xlog_max_count, read at every check like Java.
func (s *Service) traceLimit() int { return int(s.settings.Load().SearchXLogMaxCount) }
func (s *Service) traceQuery(ctx context.Context, command string, p *protocol.MapPack, stream *transport.Stream) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if command == "SEARCH_XLOG_LIST" {
		return s.searchTrace(ctx, p, stream)
	}
	if command == "XLOG_LOAD_BY_GXID" {
		from, err := getNumber(p, "stime")
		if err != nil {
			return err
		}
		to, err := getNumber(p, "etime")
		if err != nil {
			return err
		}
		gxid, err := getNumber(p, "gxid")
		if err != nil {
			return err
		}
		first, last := scoutertime.Date(from, s.config.Location), scoutertime.Date(to, s.config.Location)
		if err := s.writeGxID(ctx, stream, first, gxid); err != nil {
			return err
		}
		if last != first {
			return s.writeGxID(ctx, stream, last, gxid)
		}
		return nil
	}
	date, err := s.date(p, false)
	if err != nil {
		return err
	}
	if date == "" {
		return nil
	}
	if command == "XLOG_LOAD_BY_TXIDS" {
		ids, err := getList(p, "txid")
		if err != nil {
			return err
		}
		for i, id := range ids {
			if i+1 >= s.traceLimit() {
				break
			}
			if id.Type != protocol.ValueDecimal {
				return badParam("txid")
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			x, err := s.backend.XLog(ctx, date, id.Int)
			if err != nil {
				return err
			}
			if x != nil {
				if err := stream.ResponsePack(x); err != nil {
					return err
				}
			}
		}
		return nil
	}
	gxid, err := getNumber(p, "gxid")
	if err != nil {
		return err
	}
	if command == "XLOG_READ_BY_GXID" {
		return s.writeGxID(ctx, stream, date, gxid)
	}
	txid, err := getNumber(p, "txid")
	if err != nil {
		return err
	}
	if txid != 0 {
		x, err := s.backend.XLog(ctx, date, txid)
		if err != nil {
			return err
		}
		if x != nil {
			if err := stream.ResponsePack(x); err != nil {
				return err
			}
		}
	}
	if gxid != 0 {
		return s.writeGxID(ctx, stream, date, gxid)
	}
	return nil
}
func (s *Service) writeGxID(ctx context.Context, stream *transport.Stream, date string, gxid int64) error {
	backend, ok := s.backend.(TraceBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	xs, err := backend.XLogsByGxID(ctx, date, gxid)
	if err != nil {
		return err
	}
	for _, x := range xs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := stream.ResponsePack(x); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) searchTrace(ctx context.Context, p *protocol.MapPack, stream *transport.Stream) error {
	backend, ok := s.backend.(TraceBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	from, err := getNumber(p, "stime")
	if err != nil {
		return err
	}
	to, err := getNumber(p, "etime")
	if err != nil {
		return err
	}
	if from <= 0 || to < from {
		return badParam("stime/etime")
	}
	hash, err := getInt(p, "objHash")
	if err != nil {
		return err
	}
	patterns := make(map[string]string)
	for _, key := range []string{"service", "ip", "login", "desc", "text1", "text2", "text3", "text4", "text5"} {
		if lookup(p, key).Type != protocol.ValueNull {
			pattern, err := getText(p, key)
			if err != nil {
				return err
			}
			patterns[key] = pattern
		}
	}
	first, last := scoutertime.Date(from, s.config.Location), scoutertime.Date(to, s.config.Location)
	dates := []string{first}
	if last != first {
		dates = append(dates, last)
	}
	split := int64(0)
	if len(dates) == 2 {
		split, err = scoutertime.ParseDate(last, s.config.Location)
		if err != nil {
			return badParam("etime")
		}
	}
	count := 0
	for i, date := range dates {
		start, end := from, to
		if len(dates) == 2 {
			if i == 0 {
				firstStart, err := scoutertime.ParseDate(first, s.config.Location)
				if err != nil {
					return badParam("stime")
				}
				// Java reads only the first date's file even for a longer range.
				end = firstStart + scoutertime.DayMillis - 1
			} else {
				start = split
			}
		}
		if count >= s.traceLimit() {
			return nil
		}
		// Java streams each match as it reads and stops at
		// req_search_xlog_max_count; no buffered result, no total cap.
		err := backend.ScanXLogsByTime(ctx, date, start, end, func(x *protocol.XLogPack) (bool, error) {
			if count >= s.traceLimit() {
				return false, nil
			}
			if hash != 0 && hash != x.ObjHash {
				return true, nil
			}
			var err error
			matches := true
			for key, pattern := range patterns {
				var value string
				switch key {
				case "service", "login", "desc":
					textHash := x.Service
					if key == "login" {
						textHash = x.Login
					} else if key == "desc" {
						textHash = x.Desc
					}
					value, _, err = s.backend.Text(ctx, scoutertime.Date(x.EndTime, s.config.Location), key, textHash)
					if err != nil {
						return false, err
					}
				case "ip":
					value = "0.0.0.0"
					if len(x.IPAddress) >= 4 {
						value = fmt.Sprintf("%d.%d.%d.%d", x.IPAddress[0], x.IPAddress[1], x.IPAddress[2], x.IPAddress[3])
					}
				case "text1":
					value = x.Text1
				case "text2":
					value = x.Text2
				case "text3":
					value = x.Text3
				case "text4":
					value = x.Text4
				case "text5":
					value = x.Text5
				}
				if !traceMatch(pattern, value) {
					matches = false
				}
			}
			if matches {
				if err := stream.ResponsePack(x); err != nil {
					return false, err
				}
				count++
			}
			return count < s.traceLimit(), nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// traceMatch mirrors StrMatch's limited star syntax, rather than a general glob.
func traceMatch(pattern, target string) bool {
	if target == "" {
		return false
	}
	if pattern == "*" || pattern == "**" {
		return true
	}
	if len(pattern) < 2 {
		return target == pattern
	}
	first, last := pattern[0] == '*', pattern[len(pattern)-1] == '*'
	middle := -1
	if n := strings.IndexByte(pattern[1:], '*'); n >= 0 {
		middle = n + 1
	}
	if middle > 0 && middle < len(pattern)-1 {
		switch {
		case first && last:
			one, two := pattern[1:middle], pattern[middle+1:len(pattern)-1]
			at := strings.Index(target, one)
			return at >= 0 && strings.Contains(target[at+len(one):], two)
		case first:
			return strings.Contains(target, pattern[1:middle]) && strings.HasSuffix(target, pattern[middle+1:])
		case last:
			return strings.HasPrefix(target, pattern[:middle]) && strings.Contains(target, pattern[middle+1:len(pattern)-1])
		default:
			return strings.HasPrefix(target, pattern[:middle]) && strings.HasSuffix(target, pattern[middle+1:])
		}
	}
	switch {
	case first && last:
		return strings.Contains(target, pattern[1:len(pattern)-1])
	case first:
		return strings.HasSuffix(target, pattern[1:])
	case last:
		return strings.HasPrefix(target, pattern[:len(pattern)-1])
	default:
		return target == pattern
	}
}

// getXLoggableParent accepts CLIENT/PRODUCER as well as SERVER/CONSUMER:
// CLIENT and SERVER can intentionally share one transaction ID.
func traceParent(ctx context.Context, id int64, spans []*protocol.SpanPack) (int64, error) {
	if len(spans) > 100000 {
		return 0, transport.ErrLimit
	}
	byID := make(map[int64]*protocol.SpanPack, len(spans))
	for _, span := range spans {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if span == nil {
			return 0, transport.ErrMalformed
		}
		byID[span.TxID] = span
	}
	visited := make(map[int64]bool)
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		span := byID[id]
		if span == nil {
			return 0, nil
		}
		if span.SpanType <= 3 {
			return id, nil
		}
		if visited[id] {
			return 0, fmt.Errorf("%w: cyclic span parents", transport.ErrMalformed)
		}
		visited[id] = true
		id = span.Caller
	}
}
func (s *Service) traceXLog(ctx context.Context, date string, txid, gxid int64) (*protocol.XLogPack, error) {
	x, err := s.backend.XLog(ctx, date, txid)
	if err != nil {
		return nil, err
	}
	checked := false
	if x == nil && gxid != 0 {
		b, ok := s.backend.(TraceBackend)
		if !ok {
			return nil, transport.ErrUnsupported
		}
		spans, err := b.SpansByGxID(ctx, date, gxid)
		if err != nil {
			return nil, err
		}
		if len(spans) > 0 {
			parent, err := traceParent(ctx, txid, spans)
			if err != nil {
				return nil, err
			}
			x, err = s.backend.XLog(ctx, date, parent)
			if err != nil {
				return nil, err
			}
			checked = true
		}
	}
	if x != nil && !checked && (x.XType == 5 || x.B3Mode) && x.Caller != 0 && x.Caller != x.GxID {
		b, ok := s.backend.(TraceBackend)
		if !ok {
			return nil, transport.ErrUnsupported
		}
		spans, err := b.SpansByGxID(ctx, date, x.GxID)
		if err != nil {
			return nil, err
		}
		// Java's missing Span DB throws while obtaining the map, before writing any XLog.
		if spans == nil {
			return nil, nil
		}
		parent, err := traceParent(ctx, x.Caller, spans)
		if err != nil {
			return nil, err
		}
		detached := *x
		detached.Caller = parent
		x = &detached
	}
	return x, nil
}

var errMissingSpanRoot = errors.New("missing XLoggable span")

type spanRelation struct {
	pack     *protocol.SpanPack
	children []*spanRelation
	times    map[int64]bool
}

func (r *spanRelation) add(child *spanRelation) {
	if r.times == nil {
		r.times = make(map[int64]bool)
	}
	if !r.times[child.pack.Timestamp] {
		r.times[child.pack.Timestamp] = true
		r.children = append(r.children, child)
	}
}

// BuildSpanProfile preserves Java map replacement, timestamp-only sibling TreeSet,
// shared IDs and boundary traversal. Step fields borrow the detached input Values;
// callers must encode synchronously and must not mutate inputs during the call.
func BuildSpanProfile(ctx context.Context, gxid, txid int64, spans []*protocol.SpanPack) ([]protocol.Step, *protocol.SpanPack, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if len(spans) > 100000 {
		return nil, nil, transport.ErrLimit
	}
	normal, xloggable := make(map[int64]*spanRelation), make(map[int64]*spanRelation)
	for _, span := range spans {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if span == nil {
			return nil, nil, transport.ErrMalformed
		}
		r := &spanRelation{pack: span}
		if span.SpanType == 1 || span.SpanType == 3 {
			xloggable[span.TxID] = r
		} else {
			normal[span.TxID] = r
		}
	}
	root := xloggable[txid]
	if root == nil {
		return nil, nil, fmt.Errorf("%w: %w", transport.ErrMalformed, errMissingSpanRoot)
	}
	for _, span := range spans {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		child := normal[span.TxID]
		if span.Caller != 0 && child != nil {
			if r := normal[span.Caller]; r != nil {
				r.add(child)
			}
			if r := xloggable[span.Caller]; r != nil {
				r.add(child)
			}
		}
	}
	for _, r := range normal {
		sort.SliceStable(r.children, func(i, j int) bool { return r.children[i].pack.Timestamp < r.children[j].pack.Timestamp })
	}
	for _, r := range xloggable {
		sort.SliceStable(r.children, func(i, j int) bool { return r.children[i].pack.Timestamp < r.children[j].pack.Timestamp })
	}
	type frame struct {
		relation *spanRelation
		exit     bool
	}
	stack := []frame{{relation: root}}
	path := make(map[*spanRelation]bool)
	var arranged []*protocol.SpanPack
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if f.exit {
			delete(path, f.relation)
			continue
		}
		if path[f.relation] {
			return nil, nil, fmt.Errorf("%w: cyclic span hierarchy", transport.ErrMalformed)
		}
		if len(arranged) >= 100000 {
			return nil, nil, transport.ErrLimit
		}
		path[f.relation] = true
		arranged = append(arranged, f.relation.pack)
		stack = append(stack, frame{relation: f.relation, exit: true})
		if f.relation.pack.TxID != txid && f.relation.pack.SpanType <= 3 {
			continue
		}
		for i := len(f.relation.children) - 1; i >= 0; i-- {
			stack = append(stack, frame{relation: f.relation.children[i]})
		}
	}
	indexes := make(map[int64]int32, len(arranged))
	for i, span := range arranged {
		indexes[span.TxID] = int32(i)
	}
	steps := make([]protocol.Step, 0, len(arranged))
	initial := arranged[0].Timestamp
	for i, span := range arranged {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		parent := int32(-1)
		if n, ok := indexes[span.Caller]; ok {
			parent = n
		}
		common := protocol.CommonSpanStep{SingleStep: protocol.SingleStep{Parent: parent, Index: int32(i), StartTime: int32(span.Timestamp - initial)}, Hash: span.Name, Elapsed: span.Elapsed, Error: span.Error, Timestamp: span.Timestamp, SpanType: span.SpanType, LocalEndpointServiceName: span.LocalEndpointServiceName, LocalEndpointIP: span.LocalEndpointIP, LocalEndpointPort: span.LocalEndpointPort, RemoteEndpointServiceName: span.RemoteEndpointServiceName, RemoteEndpointIP: span.RemoteEndpointIP, RemoteEndpointPort: span.RemoteEndpointPort, Debug: span.Debug, Shared: span.Shared, AnnotationTimestamps: span.AnnotationTimestamps, AnnotationValues: span.AnnotationValues, Tags: span.Tags}
		if span.SpanType == 0 || span.SpanType == 2 {
			call := &protocol.SpanCallStep{CommonSpanStep: common, TxID: span.TxID}
			for _, tag := range span.Tags.Entries {
				if tag.Key == "http.url" {
					call.Opt = 1
					call.Address = tag.Value.Text
					break
				}
			}
			steps = append(steps, call)
		} else {
			steps = append(steps, &protocol.SpanStep{CommonSpanStep: common})
		}
	}
	return steps, root.pack, nil
}
