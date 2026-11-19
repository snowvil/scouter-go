package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime"
	"time"
)

type report struct {
	HealthSamplesDropped   uint64           `json:"health_samples_dropped"`
	ExternalSamplesDropped uint64           `json:"external_process_samples_dropped"`
	HealthSamples          []healthSample   `json:"health_samples,omitempty"`
	KernelUDP              kernelUDPReport  `json:"kernel_udp"`
	GeneratorOnly          *generatorReport `json:"generator_only,omitempty"`
	ExternalSamples        []externalSample `json:"external_process_samples,omitempty"`
	Tool                   toolReport       `json:"tool"`
	LocalMeasurement       bool             `json:"local_measurement"`
	TargetHostVerified     bool             `json:"target_host_verified"`
	Pass                   bool             `json:"pass"`
	PassCriteria           string           `json:"pass_criteria"`
	ReportPath             string           `json:"-"`
	WorkDir                string           `json:"work_dir"`
	Host                   hostReport       `json:"host"`
	Server                 serverInfo       `json:"server_binary"`
	ServerConfig           map[string]any   `json:"server_config"`
	Disk                   diskReport       `json:"disk"`
	Payload                payloadReport    `json:"payload"`
	Throughput             throughputReport `json:"throughput"`
	Send                   sendReport       `json:"send"`
	Queries                *queryReport     `json:"queries_during_ingest"`
	Drain                  drainReport      `json:"drain"`
	UDPAccounting          *udpAccounting   `json:"udp_accounting"`
	Crash                  *crashReport     `json:"crash"`
	Servers                []processReport  `json:"server_processes"`
	Restart                restartReport    `json:"restart"`
	Verify                 verifyReport     `json:"verification"`
	Limitations            []string         `json:"limitations"`
}

type toolReport struct {
	Version     string         `json:"version"`
	GitSHA      string         `json:"git_sha"`
	GitDirty    *bool          `json:"git_dirty_tracked"`
	GitError    string         `json:"git_error,omitempty"`
	GoVersion   string         `json:"go_version"`
	Options     map[string]any `json:"options"`
	OptionsHash string         `json:"options_sha256"`
	StartedAt   time.Time      `json:"started_at"`
}
type hostReport struct {
	OS             string `json:"os"`
	Arch           string `json:"arch"`
	NumCPU         int    `json:"num_cpu"`
	ToolGOMAXPROCS int    `json:"tool_gomaxprocs"`
	Hostname       string `json:"hostname_redacted"`
	SharedHostNote string `json:"note"`
}
type serverInfo struct {
	Path           string            `json:"path"`
	SHA256         string            `json:"sha256"`
	GoVersion      string            `json:"go_version"`
	VCS            map[string]string `json:"vcs"`
	BuildInfoError string            `json:"build_info_error,omitempty"`
}
type diskReport struct {
	DBPath           string  `json:"db_path"`
	FSType           string  `json:"filesystem_type"`
	TotalBytes       *uint64 `json:"filesystem_total_bytes"`
	AvailBeforeBytes *uint64 `json:"filesystem_avail_before_bytes"`
	AvailAfterBytes  *uint64 `json:"filesystem_avail_after_bytes"`
	DBBytes          *int64  `json:"db_final_bytes"`
	DBFiles          int     `json:"db_final_files"`
	DBRemoved        bool    `json:"db_removed_after_run"`
	Note             string  `json:"note,omitempty"`
}
type textReport struct {
	Hits     uint64  `json:"hits"`
	Misses   uint64  `json:"misses_textpack_sent"`
	HitRatio float64 `json:"hit_ratio"`
}
type payloadReport struct {
	Seed                uint64                `json:"seed"`
	Objects             int                   `json:"objects"`
	ObjectsNegativeHash int                   `json:"objects_negative_objhash"`
	ProfileRatio        float64               `json:"profile_ratio"`
	ProfileSizes        []sizeWeight          `json:"profile_size_weights"`
	ProfileClassCounts  map[string]uint64     `json:"profile_size_class_counts"`
	ProfileBytes        sizeSummary           `json:"profile_bytes"`
	XLogBytes           sizeSummary           `json:"xlog_pack_bytes"`
	TextHitTarget       float64               `json:"text_hit_ratio_target"`
	TextCardinality     int                   `json:"text_cardinality"`
	Text                map[string]textReport `json:"text_dictionary"`
	ErrorRatio          float64               `json:"error_ratio"`
	ErrorXLogs          uint64                `json:"error_xlogs"`
	PackCount           map[string]uint64     `json:"packs_by_type"`
	PackBytes           map[string]uint64     `json:"pack_bytes_by_type"`
	Batch               int                   `json:"batch_max_packs"`
	UDPMaxBytes         int                   `json:"udp_max_bytes"`
	Framing             string                `json:"framing"`
	Determinism         string                `json:"determinism"`
}
type sendReport struct {
	Scheduled         uint64            `json:"scheduled_transactions"`
	TxSentOK          uint64            `json:"transactions_sent_ok"`
	TxFailed          uint64            `json:"transactions_send_failed"`
	TxNotSent         uint64            `json:"transactions_not_sent_overrun"`
	Datagrams         map[string]uint64 `json:"datagrams_by_framing"`
	DatagramsTotal    uint64            `json:"datagrams_total"`
	DatagramsOK       uint64            `json:"datagrams_sent_ok"`
	DatagramBytes     uint64            `json:"datagram_bytes_ok"`
	MaxDatagramBytes  int               `json:"max_datagram_bytes"`
	SendErrors        map[string]uint64 `json:"send_errors"`
	SocketErrors      []string          `json:"socket_errors,omitempty"`
	GenerateErrors    uint64            `json:"generate_errors"`
	GenerateLastError string            `json:"generate_last_error,omitempty"`
	Lag               latencySummary    `json:"send_lag"`
	LagOver1s         uint64            `json:"send_lag_over_1s"`
	StartedAt         time.Time         `json:"schedule_start"`
	ScheduleEnd       time.Time         `json:"schedule_end"`
	FinishedAt        time.Time         `json:"send_finished"`
	SenderCPU         cpuTimes          `json:"generator_process_cpu_seconds"`
}

// udpAccounting joins sender counts with the server-1 shutdown stats. Units differ
// per field: datagrams, decoded packs and XLog transactions are never summed.
type udpAccounting struct {
	SentDatagramsOK         uint64 `json:"sent_datagrams_ok"`
	ServerDatagrams         int64  `json:"server_received_datagrams"`
	UnreceivedDatagrams     int64  `json:"unreceived_datagrams_estimate"`
	ServerRejectedDatagrams int64  `json:"server_rejected_datagrams"`
	SentPacks               uint64 `json:"sent_packs"`
	ServerPacks             int64  `json:"server_dispatched_packs"`
	ServerIngestCompleted   int64  `json:"server_ingest_completed_packs"`
	ServerXLogs             int64  `json:"server_stored_xlogs_counter"`
	VerifiedXLogs           uint64 `json:"verified_unique_xlogs"`
	ServerShutdownDiscarded int64  `json:"server_shutdown_discarded_datagrams"`
	ServerAbortedPackets    int64  `json:"server_shutdown_aborted_packs"`
	ServerAbortedAssemblies int64  `json:"server_shutdown_aborted_fragment_assemblies"`
	Note                    string `json:"note"`
}

// crashReport is the --stop=kill measurement (null in term mode).
type crashReport struct {
	KillAfterMS            float64 `json:"kill_after_ms"`
	SendersStoppedToKillMS float64 `json:"senders_stopped_to_sigkill_ms"`
	KillToExitMS           float64 `json:"sigkill_to_exit_ms"`
	ExitCode               int     `json:"exit_code"`
	WaitError              string  `json:"wait_error,omitempty"`
	ExitedBeforeKill       bool    `json:"exited_before_sigkill"`
	SentOKBeforeKill       uint64  `json:"sent_ok_before_kill"`
	RecoveredUnique        uint64  `json:"recovered_unique_after_restart"`
	LostAfterSendOK        uint64  `json:"lost_after_successful_send"`
	LossRatio              float64 `json:"loss_ratio"`
	Note                   string  `json:"note"`
}

type drainReport struct {
	VerifyDelayMS float64 `json:"verify_delay_ms,omitempty"`
	Skipped       string  `json:"skipped,omitempty"`
	WaitMS        float64 `json:"wait_ms"`
	Quiesced      bool    `json:"quiesced"`
	QuiesceMS     float64 `json:"quiesce_ms"`
	Error         string  `json:"error,omitempty"`
}
type restartFailure struct {
	Attempt   int     `json:"attempt"`
	ElapsedMS float64 `json:"elapsed_ms"`
	Error     string  `json:"error"`
	LogTail   string  `json:"log_tail"`
}
type restartReport struct {
	Attempted             bool             `json:"attempted"`
	OK                    bool             `json:"ok"`
	Label                 string           `json:"label,omitempty"`
	Attempts              int              `json:"attempts"`
	Failures              []restartFailure `json:"refused_attempts"`
	StartToReadyMS        float64          `json:"start_to_ready_ms"`
	BeginToReadyMS        float64          `json:"first_attempt_to_ready_ms"`
	PreviousExitToReadyMS float64          `json:"server1_exit_to_ready_ms"`
	ExitCode              int              `json:"server2_exit_code"`
	ShutdownKilled        bool             `json:"server2_sigkill_after_timeout"`
	ExitedEarly           bool             `json:"server2_exited_before_sigterm"`
	ServerErrorLogLines   uint64           `json:"server2_non_info_log_lines"`
	StorageErrors         int64            `json:"server2_storage_errors"`
	MaintenanceErrors     int64            `json:"server2_maintenance_errors"`
	RecoveryEvents        recoveryEvents   `json:"recovery_events"`
	StatsRecoveries       int64            `json:"server2_stats_recoveries"`
	Error                 string           `json:"error,omitempty"`
}
type throughputReport struct {
	Definition            string  `json:"definition"`
	TargetRate            float64 `json:"target_rate_tps"`
	VerifiedAvailable     bool    `json:"verified_available"`
	VerifiedUnique        uint64  `json:"verified_unique_xlogs"`
	WindowSeconds         float64 `json:"window_seconds_send_start_to_server_exit"`
	DrainInclusiveTPS     float64 `json:"representative_tps_drain_inclusive"`
	SteadyWindowSeconds   float64 `json:"steady_window_seconds"`
	SteadySentOK          uint64  `json:"steady_sent_ok"`
	SteadySendRate        float64 `json:"steady_send_rate_tps"`
	SteadyVerified        uint64  `json:"steady_verified"`
	SteadyVerifiedRate    float64 `json:"steady_verified_rate_tps"`
	SendPhaseSeconds      float64 `json:"send_phase_seconds"`
	DrainAfterSendSeconds float64 `json:"send_end_to_server_exit_seconds"`
	ScheduledTransactions uint64  `json:"scheduled_transactions"`
	SteadyIndexFrom       uint64  `json:"steady_first_index"`
	SteadyNote            string  `json:"steady_note"`
	Note                  string  `json:"note,omitempty"`
}

func newReport(o options, sizes []sizeWeight) *report {
	opts := map[string]any{}
	b, _ := json.Marshal(o)
	json.Unmarshal(b, &opts)
	opts["Password"] = "<redacted>"
	canon, _ := json.Marshal(opts) // map keys are sorted by encoding/json
	sum := sha256.Sum256(canon)
	r := &report{
		Tool:             toolReport{Version: toolVersion, GoVersion: runtime.Version(), Options: opts, OptionsHash: hex.EncodeToString(sum[:]), StartedAt: time.Now()},
		LocalMeasurement: true, TargetHostVerified: false,
		PassCriteria: "restart ok, every verification window complete, 0 missing after successful send, 0 extra/duplicate/objHash mismatch, server-1 exit 0 after SIGTERM (not crashed earlier, not SIGKILLed), server-2 healthy with 0 storage recovery events (log lines and stats recoveries)",
		Host: hostReport{OS: runtime.GOOS, Arch: runtime.GOARCH, NumCPU: runtime.NumCPU(), ToolGOMAXPROCS: runtime.GOMAXPROCS(0), Hostname: "<not recorded>",
			SharedHostNote: "generator, query workers and server share this host; results are local shared-host measurements"},
		Server: serverInfo{VCS: map[string]string{}},
		ServerConfig: map[string]any{
			"timezone": o.Timezone, "compress_xlog": o.CompressXLog, "compress_profile": o.CompressProf,
			"GOMAXPROCS": envOrUnset(o.GoMaxProcs), "GOMEMLIMIT": envOrUnset(o.GoMemLimit),
			"server_args_passthrough": append([]string{}, o.ServerArgs...), "stop_mode": o.Stop, "kill_after": o.KillAfter.String(),
			"bind": o.Bind, "defaults_note": "server flags not listed in server_args_passthrough use the binary defaults",
		},
		Payload: payloadReport{Seed: o.Seed, Objects: o.Objects, ProfileRatio: o.ProfileRatio, ProfileSizes: sizes, TextHitTarget: o.TextHitRatio, TextCardinality: o.TextCardinality,
			Text: map[string]textReport{}, ErrorRatio: o.ErrorRatio, Batch: o.Batch, UDPMaxBytes: o.UDPMaxBytes,
			Framing:     "Java DataUdpAgent: CAFE single (batch=1) or CAFN list up to udp-max-bytes, CAFM writeMTU fragments for larger packs; ProfilePack2 discardType=NONE before XLog",
			Determinism: "pack content is a function of seed, sender index and send order except wall-clock fields (XLog endTime, counter time) and UserID/ordering effects of multiple senders",
		},
		Send: sendReport{Datagrams: map[string]uint64{}, SendErrors: map[string]uint64{}},
		Limitations: []string{
			"loopback UDP on the same host: no NIC; kernel UDP counters are host-wide and can include unrelated traffic",
			"shutdown counters plus optional --sample-health periodic queue depth; no alloc/GC samples",
			"rusage inblock/oublock count block I/O operations, not bytes; page cache effects are not separated",
			"RSS is sampled with ps at the configured interval; short peaks between samples are covered only by rusage max RSS",
			"verification uses TRANX_LOAD_TIME_GROUP which only returns XLogs with elapsed > 0 (the generator always sets elapsed >= 1)",
			"fsync policy is the operator-supplied description; the tool does not inspect server syscalls",
			"the drain-inclusive denominator includes the --quiesce detection wait and SIGTERM shutdown; short runs understate TPS, prefer minutes-long steady windows",
		},
	}
	if sha, dirty, err := gitInfo(); err == nil {
		r.Tool.GitSHA, r.Tool.GitDirty = sha, dirty
	} else {
		r.Tool.GitError = err.Error()
	}
	return r
}

// envOrUnset reports nil when the variable is removed from the child environment.
func envOrUnset(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func (p *payloadReport) fill(s *corpusStats) {
	p.XLogBytes = s.XLogBytes.sizes()
	p.ProfileBytes = s.ProfileBytes.sizes()
	p.ProfileClassCounts = map[string]uint64{}
	for _, k := range sortedKeys(s.ProfileClass) {
		p.ProfileClassCounts[fmt.Sprint(k)] = s.ProfileClass[k]
	}
	p.PackCount, p.PackBytes, p.ErrorXLogs = s.PackCount, s.PackBytes, s.ErrorXLogs
	for k, t := range p.Text {
		if t.Hits+t.Misses > 0 {
			t.HitRatio = float64(t.Hits) / float64(t.Hits+t.Misses)
		}
		p.Text[k] = t
	}
}

func (r *report) summaryLine() string {
	if r.GeneratorOnly != nil {
		return fmt.Sprintf("sink-only pass=%v scheduled=%d sent_ok=%d generator_sent_tx/s=%.1f datagram/s=%.1f (server TPS unverified)", r.Pass, r.Send.Scheduled, r.Send.TxSentOK, r.GeneratorOnly.TransactionsPerSecond, r.GeneratorOnly.DatagramsPerSecond)
	}
	crash := "stop=term"
	if r.ServerConfig["mode"] == "external" {
		crash = "external (server left running)"
	}
	if r.Crash != nil {
		crash = fmt.Sprintf(" stop=kill sent_before_kill=%d recovered=%d lost=%d loss_ratio=%.4f", r.Crash.SentOKBeforeKill, r.Crash.RecoveredUnique, r.Crash.LostAfterSendOK, r.Crash.LossRatio)
	}
	return crash + fmt.Sprintf(" pass=%v scheduled=%d sent_ok=%d verified=%d missing=%d extra=%d dup=%d tps=%.1f steady_send=%.1f steady_verified=%.1f server1_shutdown_ms=%.0f restart_ready_ms=%.0f",
		r.Pass, r.Send.Scheduled, r.Send.TxSentOK, r.Throughput.VerifiedUnique, r.Verify.MissingAfterSendOK, extraOf(r), dupOf(r),
		r.Throughput.DrainInclusiveTPS, r.Throughput.SteadySendRate, r.Throughput.SteadyVerifiedRate, firstShutdown(r), r.Restart.StartToReadyMS)
}
func extraOf(r *report) uint64 {
	if r.Verify.Comparison != nil {
		return r.Verify.Comparison.Extra
	}
	return 0
}
func dupOf(r *report) uint64 {
	if r.Verify.Comparison != nil {
		return r.Verify.Comparison.Duplicates
	}
	return 0
}
func firstShutdown(r *report) float64 {
	if len(r.Servers) > 0 {
		return r.Servers[0].ShutdownMS
	}
	return 0
}
