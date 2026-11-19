package storage

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

// LogFunc receives a Java Logger.println(id, intervalSeconds, message) call.
// The Store applies Java's per-id interval itself before calling it, so the
// sink may print unconditionally. It may run under the Store gate and an index
// lock: it must not block on I/O for long and must not call the Store.
type LogFunc func(id string, intervalSeconds int, message string)

// IndexSizesMB are the creation sizes (MiB of hash area) of new .hfile files
// that Java sizes from scouter.conf when the server starts (TextPermIndex,
// TextTable and KeyValueStoreIndex read them into object vals). Existing files
// keep the size given by their length. Zero leaves a field at its default:
// Config.HashSizeBytes (1 MiB) for text, 8 MiB for KV, as Configure.java. The
// _mgr_xlog_id_index_mb size is live instead (Store.SetXLogIDIndexMB).
type IndexSizesMB struct {
	TextDefault int // _mgr_text_db_index_default_mb (text_<div> not listed below)
	TextService int // _mgr_text_db_index_service_mb
	TextAPI     int // _mgr_text_db_index_api_mb (apicall)
	TextUA      int // _mgr_text_db_index_ua_mb
	TextLogin   int // _mgr_text_db_index_login_mb
	TextDesc    int // _mgr_text_db_index_desc_mb
	TextHMsg    int // _mgr_text_db_index_hmsg_mb
	TextDaily   int // _mgr_text_db_daily_index_mb ({date}/text/text)
	KV          int // _mgr_kv_store_index_default_mb (00000000/kv2/kv_*)
}

// MaxIndexMB bounds every *_mb key. Java multiplies by 1 MiB in int and keeps
// the whole hash area in memory; Go keeps the 64 MiB Config.HashSizeBytes cap.
const MaxIndexMB = 64

const defaultKVIndexMB = 8

func checkIndexMB(name string, mb int) error {
	if mb < 0 || mb > MaxIndexMB {
		return fmt.Errorf("%s=%d outside 0..%d MiB: %w", name, mb, MaxIndexMB, ErrInvalid)
	}
	return nil
}

func (z IndexSizesMB) validate() error {
	for _, f := range []struct {
		name string
		mb   int
	}{{"_mgr_text_db_index_default_mb", z.TextDefault}, {"_mgr_text_db_index_service_mb", z.TextService}, {"_mgr_text_db_index_api_mb", z.TextAPI}, {"_mgr_text_db_index_ua_mb", z.TextUA}, {"_mgr_text_db_index_login_mb", z.TextLogin}, {"_mgr_text_db_index_desc_mb", z.TextDesc}, {"_mgr_text_db_index_hmsg_mb", z.TextHMsg}, {"_mgr_text_db_daily_index_mb", z.TextDaily}, {"_mgr_kv_store_index_default_mb", z.KV}} {
		if err := checkIndexMB(f.name, f.mb); err != nil {
			return err
		}
	}
	return nil
}

// live holds the scouter.conf values the Store reads on every use. Setters
// store atomically and never take the Store gate, so a config reload callback
// can call them while a long query holds the gate.
type live struct {
	textDailyService, textDailyAPI, textDailyUA atomic.Bool
	compressXLog, compressProfile               atomic.Bool
	visitorHourlyOff                            atomic.Bool
	xlogIDIndexMB                               atomic.Int32 // 0: Config.HashSizeBytes
	zipReadCacheBlocks                          atomic.Int32 // 0: zipCacheBlocks
	zipCountCache                               atomic.Int32 // 0: zipWriters
	warn                                        traversalWarner
}

// SetTextDaily applies mgr_text_db_daily_service_enabled,
// mgr_text_db_daily_api_enabled and mgr_text_db_daily_ua_enabled. Java
// TextPermWR.isA reads them for every write and every read, so the next
// PutText/Text call uses the new location (see CONTRACT.md D-T1).
func (s *Store) SetTextDaily(service, api, ua bool) {
	s.live.textDailyService.Store(service)
	s.live.textDailyAPI.Store(api)
	s.live.textDailyUA.Store(ua)
}

// SetCompression applies compress_xlog_enabled and compress_profile_enabled.
// Like Java XLogDataWriter/XLogProfileDataWriter, the value is used only when
// a date has no xlog.service.conf / xlog.profile.conf yet: a date already
// written keeps its recorded mode.
func (s *Store) SetCompression(xlog, profile bool) {
	s.live.compressXLog.Store(xlog)
	s.live.compressProfile.Store(profile)
}

// SetVisitorHourlyCount applies visitor_hourly_count_enabled (Java default
// true; VisitorCore reads it for every visitor).
func (s *Store) SetVisitorHourlyCount(enabled bool) { s.live.visitorHourlyOff.Store(!enabled) }

// SetXLogIDIndexMB applies _mgr_xlog_id_index_mb. Java reads it when a date's
// XLogIndex is created, so it sizes xlog_tid/xlog_gid files created from now
// on; existing files keep their length. Zero restores Config.HashSizeBytes.
func (s *Store) SetXLogIDIndexMB(mb int) error {
	if err := checkIndexMB("_mgr_xlog_id_index_mb", mb); err != nil {
		return err
	}
	s.live.xlogIDIndexMB.Store(int32(mb))
	return nil
}

// SetIndexTraversalWarningCount applies log_index_traversal_warning_count
// (Java default 100, read on every lookup).
func (s *Store) SetIndexTraversalWarningCount(n int) { s.live.warn.setLimit(n) }

// SetLog replaces Config.Log, the sink of Java-id warnings; nil drops them.
func (s *Store) SetLog(f LogFunc) { s.live.warn.setLog(f) }

// SetCompressReadCacheBlocks applies _compress_read_cache_block_count to the
// decoded GZipStore block cache (Java IOChannel readCache rows). Java treats
// a value <= 0 as unbounded (expiring after _compress_read_cache_expired_ms);
// Go has no expiry, so <= 0 and values above MaxZipCacheBlocks use
// MaxZipCacheBlocks. A smaller value evicts on the next cache insert.
func (s *Store) SetCompressReadCacheBlocks(n int) {
	if n <= 0 || n > MaxZipCacheBlocks {
		n = MaxZipCacheBlocks
	}
	s.live.zipReadCacheBlocks.Store(int32(n))
}

// SetCompressDailyCountCacheSize applies _compress_dailycount_header_cache_size
// to the per-date count.dat writer positions (Java IOChannel headers). Java
// keeps n-1 dates and loops forever for n <= 1; Go keeps max(n-1, 1), at most
// MaxZipWriters.
func (s *Store) SetCompressDailyCountCacheSize(n int) {
	n--
	if n < 1 {
		n = 1
	}
	if n > MaxZipWriters {
		n = MaxZipWriters
	}
	s.live.zipCountCache.Store(int32(n))
}

const (
	// MaxZipCacheBlocks bounds the decoded block cache (x 32 MiB logical).
	MaxZipCacheBlocks = 8
	// MaxZipWriters bounds the per-date writer positions.
	MaxZipWriters = 64
)

func (s *Store) zipCacheLimit() int {
	if n := s.live.zipReadCacheBlocks.Load(); n > 0 {
		return int(n)
	}
	return zipCacheBlocks
}
func (s *Store) zipWriterLimit() int {
	if n := s.live.zipCountCache.Load(); n > 0 {
		return int(n)
	}
	return zipWriters
}

// hashSizeFor returns the hash area of a new index file at path.
func (s *Store) hashSizeFor(path string) int {
	mb := 0
	dir, base := filepath.Split(path)
	switch kind := filepath.Base(filepath.Clean(dir)); {
	case kind == "xlog" && (base == "xlog_tid" || base == "xlog_gid"):
		mb = int(s.live.xlogIDIndexMB.Load())
	case kind == "text" && base == "text":
		mb = s.config.IndexSizes.TextDaily
	case kind == "text" && strings.HasPrefix(base, "text_"):
		z := s.config.IndexSizes
		switch strings.TrimPrefix(base, "text_") {
		case "service":
			mb = z.TextService
		case "apicall":
			mb = z.TextAPI
		case "ua":
			mb = z.TextUA
		case "login":
			mb = z.TextLogin
		case "desc":
			mb = z.TextDesc
		case "hmsg":
			mb = z.TextHMsg
		default:
			mb = z.TextDefault
		}
	}
	if mb > 0 {
		return mb << 20
	}
	return s.config.HashSizeBytes
}
func (s *Store) kvHashSize() int {
	if mb := s.config.IndexSizes.KV; mb > 0 {
		return mb << 20
	}
	return defaultKVIndexMB << 20
}

// traversalWarner reproduces Java's "Too many index deep searching" log:
// looping > log_index_traversal_warning_count after a lookup, at most once per
// 10 seconds per id (Logger.println(id, 10, ...)).
type traversalWarner struct {
	limit atomic.Int32
	set   atomic.Bool
	log   atomic.Pointer[LogFunc]
	mu    sync.Mutex
	last  map[string]time.Time
	now   func() time.Time // tests
}

const defaultTraversalWarning = 100

func (w *traversalWarner) setLog(f LogFunc) {
	if f == nil {
		w.log.Store(nil)
		return
	}
	w.log.Store(&f)
}

// logJava emits a Java-id warning through Config.Log, at most once per id
// every 10 seconds (the original Logger.println interval).
func (s *Store) logJava(id, message string) {
	w := &s.live.warn
	if log := w.log.Load(); log != nil && w.allow(id) {
		(*log)(id, 10, message)
	}
}
func (w *traversalWarner) setLimit(n int) {
	if n > 1<<30 {
		n = 1 << 30
	}
	if n < -1<<30 {
		n = -1 << 30
	}
	w.limit.Store(int32(n))
	w.set.Store(true)
}
func (w *traversalWarner) threshold() int {
	if !w.set.Load() {
		return defaultTraversalWarning
	}
	return int(w.limit.Load())
}

// hook returns the observer for one index; keyText renders the key the way
// the Java class that owns this file prints it.
func (w *traversalWarner) hook(keyText func(op legacyindex.DeepSearchOp, key []byte) string) legacyindex.DeepSearchFunc {
	return func(op legacyindex.DeepSearchOp, key []byte, looped int) {
		log := w.log.Load()
		if log == nil || looped <= w.threshold() {
			return
		}
		id := "S152"
		if op == legacyindex.DeepUpdate {
			id = "S161"
		}
		if !w.allow(id) {
			return
		}
		(*log)(id, 10, "[warn] Too many index deep searching. "+keyText(op, key))
	}
}
func (w *traversalWarner) allow(id string) bool {
	now := time.Now
	if w.now != nil {
		now = w.now
	}
	t := now()
	w.mu.Lock()
	defer w.mu.Unlock()
	if last, ok := w.last[id]; ok && t.Before(last.Add(10*time.Second)) {
		return false
	}
	if w.last == nil {
		w.last = make(map[string]time.Time)
	}
	w.last[id] = t
	return true
}

// javaIndexKeyText is IndexKeyFile.get's message key: DataInputX.toInt of the
// first four bytes (Java prints toLong only for 7-byte keys, which Scouter
// never uses), so an 8-byte txid or daily text key prints its high half.
func javaIndexKeyText(_ legacyindex.DeepSearchOp, key []byte) string {
	if len(key) < 4 {
		return ""
	}
	return fmt.Sprint(int32(binary.BigEndian.Uint32(key)))
}

// javaKVKeyText is IndexKeyFile2's new String(key, "UTF8").
func javaKVKeyText(_ legacyindex.DeepSearchOp, key []byte) string {
	if utf8.Valid(key) {
		return string(key)
	}
	return strings.ToValidUTF8(string(key), "�")
}
