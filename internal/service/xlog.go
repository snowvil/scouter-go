package service

import (
	"context"
	"errors"
	"log"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

func (s *Service) xlogQuery(ctx context.Context, command string, p *protocol.MapPack, stream *transport.Stream) error {
	if command == "TRANX_REAL_TIME_GROUP" || command == "TRANX_REAL_TIME_GROUP_LATEST" {
		loop, err := getNumber(p, "loop")
		if err != nil {
			return err
		}
		index, err := getInt(p, "index")
		if err != nil {
			return err
		}
		hashes, err := getHashes(p, "objHash")
		if err != nil {
			return err
		}
		query := RealtimeQuery{Loop: loop, Index: index, ObjectHashes: hashes}
		if command == "TRANX_REAL_TIME_GROUP" {
			query.MinElapsed, err = getInt(p, "limit")
			query.MinElapsed = lowerBound(s.settings.Load().XLogRealtimeLowerBoundMillis, query.MinElapsed)
		} else {
			query.LatestCount, err = getInt(p, "count")
			if query.LatestCount <= 0 {
				return badParam("count")
			}
		}
		if err != nil {
			return err
		}
		result, err := s.backend.XLogsRealtime(ctx, query)
		if err != nil {
			return err
		}
		if err := stream.ResponsePack(newMap(entry("loop", decimal(result.Loop)), entry("index", decimal(int64(result.Index))))); err != nil {
			return err
		}
		return writeXLogs(stream, result.Records)
	}
	date, err := s.date(p, command == "TRANX_PROFILE" || command == "TRANX_PROFILE_FULL")
	if err != nil {
		return err
	}
	if command == "TRANX_LOAD_TIME_GROUP" || command == "TRANX_LOAD_TIME_GROUP_V2" {
		from, err := getNumber(p, "stime")
		if err != nil {
			return err
		}
		to, err := getNumber(p, "etime")
		if err != nil {
			return err
		}
		if to < from {
			return badParam("stime/etime")
		}
		hashes, err := getHashes(p, "objHash")
		if err != nil {
			return err
		}
		q := HistoryQuery{Date: date, From: from, To: to, ObjectHashes: hashes}
		if command == "TRANX_LOAD_TIME_GROUP" {
			if len(hashes) == 0 {
				return nil
			}
			q.MinElapsed, err = getInt(p, "limit")
			if err != nil {
				return err
			}
			q.MinElapsed = lowerBound(s.settings.Load().XLogPasttimeLowerBoundMillis, q.MinElapsed)
			max, err := getInt(p, "max")
			if err != nil {
				return err
			}
			q.Max = int(max)
			q.Reverse, err = getBool(p, "reverse")
			if err != nil {
				return err
			}
		} else {
			page, err := getInt(p, "pageCount")
			if err != nil {
				return err
			}
			q.PageCount = int(page)
			q.LastBucketTime, err = getNumber(p, "lastBucketTime")
			if err != nil {
				return err
			}
			q.LastTxID, err = getNumber(p, "txid")
			if err != nil {
				return err
			}
			if len(hashes) == 0 || page == 0 || (q.LastBucketTime == 0) != (q.LastTxID == 0) {
				return writeHistoryMeta(stream, HistoryResult{})
			}
			if page < 0 {
				return badParam("pageCount")
			}
		}
		if date == "" {
			if command == "TRANX_LOAD_TIME_GROUP_V2" {
				return writeHistoryMeta(stream, HistoryResult{})
			}
			return nil
		}
		result, err := s.backend.XLogsHistory(ctx, q, func(x *protocol.XLogPack) error { return stream.ResponsePack(x) })
		if err != nil {
			return err
		}
		if command == "TRANX_LOAD_TIME_GROUP_V2" {
			return writeHistoryMeta(stream, result)
		}
		return nil
	}
	txid, err := getNumber(p, "txid")
	if err != nil {
		return err
	}
	gxid, err := getNumber(p, "gxid")
	if err != nil {
		return err
	}
	if command == "XLOG_READ_BY_TXID" {
		if date == "" {
			return nil
		}
		result, err := s.traceXLog(ctx, date, txid, gxid)
		if err != nil {
			return err
		}
		if result == nil {
			return nil
		}
		return stream.ResponsePack(result)
	}
	xtype, err := getInt(p, "xlogType")
	if err != nil {
		return err
	}
	if gxid == 0 {
		record, err := s.backend.XLog(ctx, date, txid)
		if err != nil {
			return err
		}
		if record == nil {
			return nil
		}
		gxid, xtype = record.GxID, int32(record.XType)
	}
	if xtype == 5 {
		backend, ok := s.backend.(TraceBackend)
		if !ok {
			return transport.ErrUnsupported
		}
		spans, err := backend.SpansByGxID(ctx, date, gxid)
		if err != nil {
			return err
		}
		if len(spans) == 0 {
			if command == "TRANX_PROFILE" {
				return nil
			}
			return transport.ErrMalformed
		}
		steps, owner, err := BuildSpanProfile(ctx, gxid, txid, spans)
		if err != nil {
			if command == "TRANX_PROFILE" && errors.Is(err, errMissingSpanRoot) {
				return nil
			}
			return err
		}
		raw, err := protocol.EncodeSteps(steps, protocol.Limits{MaxBytes: maxProfileResponseBytes})
		if err != nil {
			logProfileLimit(command, date, txid, err)
			return err
		}
		if command == "TRANX_PROFILE_FULL" {
			return stream.ResponseBlob(raw)
		}
		return stream.ResponsePack(&protocol.ProfilePack{TxID: txid, ObjHash: owner.ObjHash, Service: owner.Name, Profile: raw})
	}
	chunks, err := s.backend.Profile(ctx, date, txid)
	if err != nil {
		return err
	}
	if command == "TRANX_PROFILE_FULL" {
		for _, chunk := range chunks {
			if chunk != nil {
				if err := stream.ResponseBlob(chunk); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if chunks == nil {
		return nil
	}
	max, err := getInt(p, "max")
	if err != nil {
		return err
	}
	var profile []byte
	count := int32(0)
	for _, chunk := range chunks {
		if max > 0 && count >= max {
			control, err := protocol.EncodeStep(&protocol.StepControl{Message: " ** Profile Truncated ** "}, protocol.Limits{})
			if err != nil {
				return err
			}
			if len(profile) > maxProfileResponseBytes-len(control) {
				logProfileLimit(command, date, txid, transport.ErrLimit)
				return transport.ErrLimit
			}
			profile = append(profile, control...)
			break
		}
		if chunk != nil {
			if len(profile)+len(chunk) > maxProfileResponseBytes {
				logProfileLimit(command, date, txid, transport.ErrLimit)
				return transport.ErrLimit
			}
			profile = append(profile, chunk...)
			count++
		}
	}
	return stream.ResponsePack(&protocol.ProfilePack{Profile: profile})
}
func writeXLogs(stream *transport.Stream, records []*protocol.XLogPack) error {
	for _, record := range records {
		if err := stream.ResponsePack(record); err != nil {
			return err
		}
	}
	return nil
}
func writeHistoryMeta(stream *transport.Stream, result HistoryResult) error {
	return stream.ResponsePack(newMap(entry("hasMore", boolean(result.HasMore)), entry("lastXLogTime", decimal(result.LastBucketTime)), entry("lastTxid", decimal(result.LastTxID))))
}

// maxProfileResponseBytes bounds the one profile Blob TRANX_PROFILE sends.
// Java XLogProfileRD.getProfile concatenates every block (up to "max") with
// no size limit and never cuts on size; Go keeps a memory bound equal to the
// Client response value limit (transport.MaxResponseValueBytes, 256MiB).
const maxProfileResponseBytes = transport.MaxResponseValueBytes

// logProfileLimit names the refused profile; the Client sees the request fail.
func logProfileLimit(command, date string, txid int64, err error) {
	if errors.Is(err, transport.ErrLimit) || errors.Is(err, protocol.ErrLimit) {
		log.Printf("service: %s date=%s txid=%d refused: profile over the Go response limit %dMiB: %v", command, date, txid, maxProfileResponseBytes>>20, err)
	}
}
