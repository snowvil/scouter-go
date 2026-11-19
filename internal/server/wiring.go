package server

import (
	"context"
	"log"
	"math/rand/v2"
	"os"
	"runtime"
	"time"

	"github.com/snowvil/scouter-go/internal/account"
	"github.com/snowvil/scouter-go/internal/config"
	"github.com/snowvil/scouter-go/internal/ingest"
	"github.com/snowvil/scouter-go/internal/logger"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/webapp"
)

// Live setting consumers outside ingest (docs/compatibility/config-behavior.md
// §5.1). Every setter below only stores atomically, so it may run inside
// ApplySettings under the config reload lock; values that cannot be applied
// are logged once per change and the previous value stays in force.

// LoggerConfig is the logger.Config of the live settings (L1).
func LoggerConfig(l config.Live) logger.Config {
	return logger.Config{Dir: l.LogDir, Rotation: l.LogRotation, KeepDays: l.LogKeepDays, Trace: l.Trace, IgnoreIDs: l.LogIgnoreIDs}
}

// serviceSettings converts the Client query part of Live (V1, V2).
func serviceSettings(l config.Live) service.Settings {
	return service.Settings{
		ServerID: l.ServerID, ExtLinkName: l.ExtLinkName, ExtLinkURLPattern: l.ExtLinkURLPattern,
		ClientSoTimeoutMillis: int32(l.ClientSoTimeoutMs), TagCountEnabled: l.TagCountEnabled,
		XLogRealtimeLowerBoundMillis: int32(l.XLogRealtimeLowerBoundMs), XLogPasttimeLowerBoundMillis: int32(l.XLogPasttimeLowerBoundMs),
		SearchXLogMaxCount: int32(l.SearchXLogMaxCount),
	}
}

// webappSettings converts the webapp part of Live (W1).
func webappSettings(w config.WebappLive) webapp.Settings {
	return webapp.Settings{
		CORSAllowOrigin: w.CORSAllowOrigin, CORSAllowCredentials: w.CORSAllowCredentials,
		AuthIPEnabled: w.AuthIPEnabled, AllowIPs: w.AllowIPs, AuthIPHeaderKey: w.AuthIPHeaderKey,
		AuthSessionEnabled: w.AuthSessionEnabled, AuthBearerEnabled: w.AuthBearerEnabled,
		PoolIdleTimeout: time.Duration(w.PoolTimeoutMs) * time.Millisecond,
	}
}

// LDAPConfig converts the AccountManager LDAP keys (A1); nil is
// account_use_ldap=false.
func LDAPConfig(l config.LDAP) *account.LDAPConfig {
	if !l.Enabled {
		return nil
	}
	return &account.LDAPConfig{URL: l.ProviderURL, Auth: l.Auth, PrincipalDomain: l.PrincipalDomain, BaseDN: l.BaseDN, EmailAttr: l.EmailID, GroupAttr: l.GroupID, Debug: l.Debug}
}

func millis(ms int) time.Duration { return time.Duration(ms) * time.Millisecond }

// applyLive runs the setters of every consumer except ingest and the agent
// pool (wireSettings). first is the Open call: everything is applied and
// checked once; later calls touch only what changed.
func (r *Runtime) applyLive(prev, next config.Live, first bool) {
	conf := r.config.ConfigFile != nil
	// service (V1, V2, V3)
	r.queries.SetSettings(serviceSettings(next))
	if r.fileMetadata != nil {
		r.fileMetadata.SetLogDir(next.LogDir)
	}
	// transport (T1, T2, T5, T6)
	if first || prev.ClientSoTimeoutMs != next.ClientSoTimeoutMs {
		if err := r.tcp.SetClientSoTimeout(millis(next.ClientSoTimeoutMs)); err != nil {
			log.Printf("config: net_tcp_client_so_timeout_ms=%d not applied: %v", next.ClientSoTimeoutMs, err)
		}
	}
	if first || prev.AgentSoTimeoutMs != next.AgentSoTimeoutMs {
		if err := r.tcp.SetAgentSoTimeout(millis(next.AgentSoTimeoutMs)); err != nil {
			log.Printf("config: net_tcp_agent_so_timeout_ms=%d not applied: %v", next.AgentSoTimeoutMs, err)
		}
	}
	if first || prev.ServicePoolSize != next.ServicePoolSize {
		if err := r.tcp.SetServicePoolSize(next.ServicePoolSize); err != nil {
			log.Printf("config: net_tcp_service_pool_size=%d not applied: %v", next.ServicePoolSize, err)
		} else if next.ServicePoolSize > r.config.MaxConnections {
			log.Printf("WARNING: net_tcp_service_pool_size=%d exceeds --max-connections=%d; at most %d Client connections are accepted", next.ServicePoolSize, r.config.MaxConnections, r.config.MaxConnections)
		}
	}
	r.tcp.SetActionLog(next.LogTCPAction)
	r.udp.SetLogUDPMultipacket(next.LogUDPMultipacket)
	r.udp.SetLogExpiredMultipacket(next.LogExpiredMultipacket)
	// storage (S1, S3, S4, S5, I3, _compress_*)
	r.store.SetTextDaily(next.TextDailyService, next.TextDailyAPI, next.TextDailyUA)
	r.store.SetCompression(next.CompressXLog, next.CompressProfile)
	r.store.SetVisitorHourlyCount(next.VisitorHourlyCount)
	if first || prev.XLogIDIndexMB != next.XLogIDIndexMB {
		if err := r.store.SetXLogIDIndexMB(next.XLogIDIndexMB); err != nil {
			log.Printf("config: _mgr_xlog_id_index_mb=%d not applied: %v", next.XLogIDIndexMB, err)
		}
	}
	r.store.SetIndexTraversalWarningCount(next.IndexTraversalWarningCount)
	r.counters.SetIndexTraversalWarningCount(next.IndexTraversalWarningCount)
	r.store.SetCompressReadCacheBlocks(next.CompressReadCacheBlocks)
	r.store.SetCompressDailyCountCacheSize(next.CompressDailyCountCacheSize)
	if !conf {
		// Without scouter.conf nothing changes after startup: retention,
		// the logger, the webapp and LDAP keep the Config values.
		return
	}
	if first || prev.GoMaxProcs != next.GoMaxProcs {
		r.applyMaxProcs(next.GoMaxProcs)
	}
	// retention (S2): Go keeps "off without mgr_purge_enabled" (Live).
	if changed := r.retention.Apply(next.PurgeEnabled, storage.JavaRetentionConfig(next.Purge)); changed && !first {
		enabled, c := r.retention.Settings()
		log.Printf("config: retention enabled=%v %+v", enabled, c)
	}
	// logger (L1)
	r.config.Logger.SetConfig(LoggerConfig(next))
	// webapp (W1)
	if r.webapp != nil {
		r.webapp.SetSettings(webappSettings(next.Webapp))
	}
	// account LDAP (A1): the same value keeps the authenticator.
	if r.accounts != nil && (first || prev.LDAP != next.LDAP) {
		if err := r.accounts.SetLDAP(LDAPConfig(next.LDAP)); err != nil {
			log.Printf("config: account_ldap_* not applied: %v", err)
		}
	}
}

// liveNow returns the live settings without copying them (hooks on the
// ingest path read one or two fields per pack).
func (r *Runtime) liveNow() *config.Live {
	if p := r.live.current.Load(); p != nil {
		return p
	}
	return &startupDefaults
}

var startupDefaults = config.DefaultLive()

// logPack is ingest.Config.PackLog: NetDataProcessor's log_udp_packet line
// before the dispatch and the log_udp_<type> line after it (§1.8). The text
// is FormatPack, which masks sql and login TextPacks (D-S1).
func (r *Runtime) logPack(p protocol.Pack, processed bool) {
	l := r.liveNow()
	if len(l.LogUDP) == 0 {
		return
	}
	if !processed {
		if l.LogUDP["log_udp_packet"] {
			r.config.Logger.Stdout(logger.FormatPack(p, r.config.Location))
		}
		return
	}
	if key, prefix, ok := logger.UDPLog(p); ok && l.LogUDP[key] {
		r.config.Logger.Stdout(prefix + logger.FormatPack(p, r.config.Location))
	}
}

// logObjectType is ingest.Config.ObjectTypeLog: the first two
// log_udp_some_object lines of CounterManager.addObjectTypeIfNotExist.
func (r *Runtime) logObjectType(objType string, known bool, detected string, detectedOK bool) {
	if r.liveNow().LogUDPSomeObject != objType {
		return
	}
	r.config.Logger.Stdout(logger.SomeObjectLine(objType))
	if known {
		return
	}
	if !detectedOK {
		detected = logger.JavaNull
	}
	r.config.Logger.Stdout(logger.SomeObjectDetectedLine(objType, detected))
}

// logProfileTest is ingest.Config.ProfileTestLog: ProfilePreCore writes
// "<bytes>\t<tag> <toDebugString(service name)>" for log_test_rate percent
// of the profiles (rnd.nextInt(100) < rate). The service name is
// TextRD.getString(yyyymmdd(pack.time), "service", pack.service).
//
// The hook runs on the ingest path with the engine lock held, and Go has no
// TextCache to answer from memory (ingest writes each TextPack straight to
// the store), so the lookup is a store read bounded by profileTestTextWait:
// a slow disk or a busy store lock costs that ingest path at most this long
// instead of blocking it. A timeout or error prints the name as null, as a
// miss does in Java.
func (r *Runtime) logProfileTest(c ingest.ProfileTestCase, cnt, total int, p *protocol.ProfilePack) bool {
	rate := r.liveNow().LogTestRate
	if rate <= 0 || rand.IntN(100) >= rate {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), profileTestTextWait)
	name, ok, err := r.store.TextContext(ctx, storage.JavaDate(p.Time, r.config.Location), "service", p.Service)
	cancel()
	if err != nil || !ok {
		name = logger.JavaNull
	}
	tag := logger.ProfileCaseNormal
	switch c {
	case ingest.ProfileTestIgnoreCons:
		tag = logger.ProfileCaseIgnoreCons
	case ingest.ProfileTestOld:
		tag = logger.ProfileCaseOld
	case ingest.ProfileTestDelaying:
		tag = logger.ProfileCaseDelaying(cnt, total)
	}
	r.config.Logger.Println(logger.ProfileTestLine(tag, p, name))
	return true
}

// profileTestTextWait bounds the log_test_rate service name lookup.
const profileTestTextWait = 100 * time.Millisecond

// javaLog adapts a Logger.println(id, msg) caller (transport) to the logger.
func (r *Runtime) javaLog(id, msg string) {
	if id == "" {
		r.config.Logger.Println(msg)
		return
	}
	r.config.Logger.PrintlnID(id, msg)
}

// requestUser is RequestLogger's LoginManager.getUser(session).
func (r *Runtime) requestUser(session int64) (id, ip string, ok bool) {
	u, ok := r.queries.LoginUser(session)
	return u.ID, u.IP, ok
}

// sqlTextStore is the sqltable Sink and Source over the file database:
// TextWR.add(date, "table", ...) goes to the permanent text_table dictionary
// and TextRD.getString(date, "sql", hash) reads the stored SQL. Instead of a
// TextCache, ingest puts each TextPack in the store's pending read-through
// (or its cache of dropped texts) before it queues the write, so a queued
// SQL is readable when the worker looks it up.
type sqlTextStore struct{ store *storage.Store }

// AddText stops waiting for the store once Tables.Close gives up (its ctx
// ended): PutTextContext cancels its wait for room in the text queue; a
// queued text is written by the store's text writer.
func (s sqlTextStore) AddText(ctx context.Context, date, textType string, hash int32, text string) error {
	return s.store.PutTextContext(ctx, date, &protocol.TextPack{XType: textType, Hash: hash, Text: text})
}

func (s sqlTextStore) Text(ctx context.Context, date, textType string, hash int32) (string, bool, error) {
	return s.store.TextContext(ctx, date, textType, hash)
}

// applyMaxProcs applies go_maxprocs: n > 0 caps the CPUs the Go runtime runs
// Go code on at once; 0 returns to the startup value, the GOMAXPROCS
// environment variable or else the runtime default (the logical CPUs capped
// by a cgroup limit, kept up to date by the runtime). Fewer than the CPUs
// spends less CPU on idle GC marking and spinning for the same work on a
// shared host, at the cost of headroom for bursts and Client queries
// (docs/performance-structural-review-2026-10-20.md §10).
func (r *Runtime) applyMaxProcs(n int) {
	before := runtime.GOMAXPROCS(0)
	switch {
	case n > 0:
		runtime.GOMAXPROCS(n)
	case os.Getenv("GOMAXPROCS") != "":
		runtime.GOMAXPROCS(r.startMaxProcs)
	default:
		runtime.SetDefaultGOMAXPROCS()
	}
	if after := runtime.GOMAXPROCS(0); after != before {
		log.Printf("config: go_maxprocs=%d: GOMAXPROCS %d -> %d", n, before, after)
	}
}
