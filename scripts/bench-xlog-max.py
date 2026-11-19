#!/usr/bin/env python3
"""단일 시나리오의 완료 XLog 지속 rate를 지수 증가/이분 탐색한다 (stdlib only).

--smoke는 구조 검증이며 성능 결론이 아니다. external 모드는 준비된 격리 서버만
사용하며 재시작/종료하지 않는다. unknown Java reject/backlog는 명시해 남긴다.
"""
import argparse
import datetime as dt
import ipaddress
import json
import math
from pathlib import Path
import shlex
import subprocess
import time


def timestamp(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def classify(report, rate, args):
    """판정 근거와 실패 원인을 함께 반환한다; 없는 관측값을 0으로 바꾸지 않는다."""
    reasons, unknown = [], []
    send = report.get("send", {})
    verify = report.get("verification", {})
    compare = verify.get("comparison") or {}
    throughput = report.get("throughput", {})
    scheduled = send.get("scheduled_transactions")
    if not report.get("pass"):
        reasons.append("replay_validation_failed")
    if not scheduled or compare.get("unique_expected_found") != scheduled:
        reasons.append("missing_or_unsent_xlogs")
    if any(compare.get(k, 0) != 0 for k in
           ("duplicate_records", "extra_records_not_generated", "objhash_mismatch")):
        reasons.append("integrity_error")
    if not verify.get("complete"):
        reasons.append("verification_incomplete")
    if (send.get("transactions_sent_ok") != scheduled or
            send.get("generate_errors", 0) or send.get("socket_errors") or
            any(send.get("send_errors", {}).values())):
        reasons.append("generator_limited_or_send_error")
    if throughput.get("steady_verified_rate_tps", 0) < rate * 0.99:
        reasons.append("steady_verified_below_99_percent")
    # Scheduled-window verified rate alone could hide a generator that caught up
    # long after the schedule. Bound its wall time as well.
    if send.get("send_finished") and send.get("schedule_end"):
        if timestamp(send["send_finished"]) - timestamp(send["schedule_end"]) > args.max_send_overrun:
            reasons.append("generator_schedule_overrun")
    kernel = report.get("kernel_udp", {})
    if not kernel.get("available"):
        reasons.append("kernel_loss_unobservable")
    elif kernel.get("loss", 0) != 0:
        # netstat counters are host-wide: other processes' drops land here too.
        # Fail on them only when XLogs went missing or when asked to.
        if args.kernel_loss_fails or "missing_or_unsent_xlogs" in reasons:
            reasons.append("kernel_udp_loss")
        else:
            unknown.append("kernel_udp_loss_host_wide")
    udp = report.get("udp_accounting") or {}
    rejected = udp.get("server_rejected_datagrams")
    if rejected is None or rejected < 0:
        unknown.append("server_udp_reject")
        if args.kind == "go":
            reasons.append("server_udp_reject_unobservable")
    elif rejected:
        reasons.append("server_udp_reject")
    drain = report.get("drain", {})
    if not drain.get("quiesced") or drain.get("wait_ms", math.inf) > args.max_drain * 1000:
        reasons.append("drain_timeout_or_overload")
    backlog = {"available": False}
    if args.kind == "go":
        start = timestamp(send["schedule_start"]) + args.warmup if send.get("schedule_start") else 0
        end = start + args.duration
        samples = [s for s in report.get("health_samples", [])
                   if start <= timestamp(s["at"]) < end]
        valid = [s for s in samples if not s.get("error") and
                 "QueueLen" in (s.get("udp") or {})]
        first = [s["udp"]["QueueLen"] for s in valid if timestamp(s["at"]) < start + args.duration / 3]
        last = [s["udp"]["QueueLen"] for s in valid if timestamp(s["at"]) >= end - args.duration / 3]
        if report.get("health_samples_dropped") or not first or not last or len(valid) < 3 or len(valid) != len(samples):
            reasons.append("backlog_unobservable")
        else:
            early, late = sum(first) / len(first), sum(last) / len(last)
            backlog = {"available": True, "first_third_mean": early,
                       "last_third_mean": late, "growth": late - early,
                       "allowed_growth": args.backlog_growth, "samples": len(valid)}
            if late - early > args.backlog_growth:
                reasons.append("ingest_backlog_growth")
        for s in valid:
            ing = s.get("ingest") or {}
            # Write*: the ingest record writers (internal/ingest CONTRACT.md
            # §13); WritePending is a queue length, not an error.
            if any(ing.get(k, 0) for k in ("Rejected", "StorageErrors", "DroppedXLogs", "WriteErrors",
                                            "WriteInvalid", "WriteDropped", "WriteAbandoned")):
                reasons.append("server_ingest_error")
                break
    else:
        unknown.append("ingest_backlog")
    return {"passed": not reasons, "failure_reasons": sorted(set(reasons)),
            "unknown": unknown, "backlog": backlog,
            "evidence_policy": "go_queue_samples" if args.kind == "go" else
            "external: TCP completeness + kernel loss + drain; reject/backlog unknown"}


def command(args, rate, index, directory):
    cmd = [str(Path(args.replay).resolve()), "--rate", str(rate),
           "--duration", f"{args.duration}s", "--warmup", f"{args.warmup}s",
           "--objects", str(args.objects), "--senders", str(args.senders),
           "--batch", str(args.batch), "--profile-ratio", str(args.profile_ratio),
           "--profile-sizes", args.profile_sizes, "--query-concurrency", str(args.query_concurrency),
           "--seed", str(args.seed + index), "--work-dir", str(directory),
           "--report", str(directory / "report.json"),
           "--max-overrun", f"{args.max_send_overrun}s",
           "--drain-timeout", f"{max(3, args.max_drain + 1)}s"]
    if args.kind == "go":
        cmd += ["--server", str(Path(args.server).resolve()), "--sample-health"]
        if args.gomaxprocs:
            cmd += ["--gomaxprocs", args.gomaxprocs]
        if args.compress:
            cmd += ["--compress-xlog", "--compress-profile"]
        cmd += [f"--server-arg={arg}" for arg in args.server_arg]
    else:
        cmd += ["--server-udp", args.server_udp, "--server-tcp", args.server_tcp,
                "--server-pid", str(args.server_pid)]
    # Extra replay flags, e.g. --quiesce=20s for a Java server whose XLog
    # index is flushed to disk seconds after the cursor stops moving.
    cmd += list(args.replay_arg)
    return cmd


def save_summary(args, trials, low, high, stop):
    result = {"scenario": vars(args), "trials": trials,
              "best_passing_rate": low or None, "failing_upper_rate": high,
              "termination": stop, "smoke_only": args.smoke,
              "conclusion": ("smoke_only" if args.smoke else "bounded_lower_estimate"),
              "limitations": ["공유 로컬 호스트, 단일 반복; 운영 TPS 증거가 아님",
                              "rate에 따른 성공의 단조성을 가정하며 반복 검증이 필요함",
                              "상한까지 성공하면 최대치가 아니라 확인된 하한임",
                              "steady_verified는 steady에 예정된 XLog의 사후 조회 수이며 저장 완료 시각별 TPS가 아님",
                              "kernel UDP 통계는 호스트 전체 값이므로 다른 프로세스 손실을 포함할 수 있음"]}
    output = Path(args.output)
    (output / "summary.json").write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    rows = ["# XLog rate 탐색", "", f"종료: {stop}; 확인된 성공 rate: {low or '없음'}; 실패 상한: {high}", "",
            "| 시도 | 예정 XLog/s | 판정 | 실패 원인 |", "|---|---:|---|---|"]
    for t in trials:
        d = t.get("decision", {})
        rows.append(f"| {t['index']} | {t['rate']} | {'dry-run' if args.dry_run else d.get('passed')} | {', '.join(d.get('failure_reasons', []))} |")
    rows += ["", *[f"- {s}" for s in result["limitations"]]]
    if args.kind == "external":
        rows += ["- 외부 서버 reject/backlog는 unknown이며 TCP 무누락·커널 손실·drain 기준으로만 판정한다."]
    (output / "summary.md").write_text("\n".join(rows) + "\n")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--kind", choices=("go", "external"), default="go")
    p.add_argument("--replay", default="dist/scouter-replay")
    p.add_argument("--server", default="dist/scouter-go")
    p.add_argument("--server-udp", default="")
    p.add_argument("--server-tcp", default="")
    p.add_argument("--server-pid", type=int, default=0)
    p.add_argument("--output", required=True, help="새 출력 디렉터리 (덮어쓰기 금지)")
    p.add_argument("--gomaxprocs", default="")
    p.add_argument("--batch", type=int, default=32)
    p.add_argument("--profile-ratio", type=float, default=0.3)
    p.add_argument("--profile-sizes", default="0:0.1,1024:0.6,8192:0.25,131072:0.05")
    p.add_argument("--objects", type=int, default=20)
    p.add_argument("--compress", action="store_true", help="Go 서버만; external 설정은 운영자가 관리")
    p.add_argument("--query-concurrency", type=int, default=2)
    p.add_argument("--senders", type=int, default=1)
    p.add_argument("--server-arg", action="append", default=[])
    p.add_argument("--replay-arg", action="append", default=[],
                   help="scouter-replay에 그대로 넘길 인자(예: --replay-arg=--quiesce=20s)")
    p.add_argument("--initial-rate", type=int, default=500)
    p.add_argument("--max-rate", type=int, default=100000)
    p.add_argument("--min-rate", type=int, default=1)
    p.add_argument("--resolution", type=int, default=50)
    p.add_argument("--max-attempts", type=int, default=20)
    p.add_argument("--duration", type=float, default=30)
    p.add_argument("--warmup", type=float, default=3)
    p.add_argument("--cooldown", type=float, default=5)
    p.add_argument("--max-drain", type=float, default=2)
    p.add_argument("--max-send-overrun", type=float, default=0.5)
    p.add_argument("--kernel-loss-fails", action="store_true",
                   help="host-wide netstat UDP drop을 XLog 누락 없이도 실패로 본다(기본: 누락이 있을 때만 실패, 아니면 unknown 기록)")
    p.add_argument("--backlog-growth", type=float, default=64,
                   help="마지막/처음 steady 1/3 queue 평균의 허용 증가량")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--dry-run", action="store_true", help="실행 없이 지수 증가 후보 명령 출력")
    p.add_argument("--smoke", action="store_true", help="4초/0초 warmup, 2000/s 이하, 최대 3회")
    a = p.parse_args(argv)
    if a.smoke:
        a.duration, a.warmup = 4, 0
        a.max_rate, a.initial_rate = min(a.max_rate, 2000), min(a.initial_rate, 2000)
        a.max_attempts = min(a.max_attempts, 3)
    if not 1 <= a.min_rate <= a.initial_rate <= a.max_rate:
        p.error("require 1 <= min-rate <= initial-rate <= max-rate")
    if a.resolution < 1 or not 1 <= a.max_attempts <= 100:
        p.error("resolution >= 1; max-attempts 1..100")
    if any(not math.isfinite(v) or v < 0 for v in
           (a.duration, a.warmup, a.cooldown, a.max_drain, a.max_send_overrun, a.backlog_growth)) or a.duration <= 0:
        p.error("invalid duration/threshold")
    if not 1 <= a.objects <= 4096 or not 1 <= a.senders <= min(a.objects, 256):
        p.error("objects 1..4096; senders 1..min(objects,256)")
    if not 1 <= a.batch <= 1024 or not 0 <= a.profile_ratio <= 1 or not 0 <= a.query_concurrency <= 64:
        p.error("invalid batch/profile-ratio/query-concurrency")
    if not 0 <= a.seed < 2**64 - a.max_attempts:
        p.error("seed plus attempt index must fit uint64")
    if a.kind == "external":
        if a.compress or a.gomaxprocs or a.server_arg:
            p.error("external server runtime/compression flags are operator managed")
        for addr in (a.server_udp, a.server_tcp):
            try:
                host, port = addr.rsplit(":", 1)
                if not ipaddress.ip_address(host.strip("[]")).is_loopback or not 0 < int(port) < 65536:
                    raise ValueError()
            except ValueError:
                p.error("external endpoints must be literal loopback IP:port")
        if a.server_pid < 0:
            p.error("server-pid must be nonnegative")
    elif a.server_udp or a.server_tcp or a.server_pid:
        p.error("server endpoints/PID require --kind external")
    return a


def main(argv=None):
    args = parse_args(argv)
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    args.output = str(output)
    trials, low, high = [], 0, None
    rate, stop = args.initial_rate, "attempt_limit"
    for index in range(1, args.max_attempts + 1):
        directory = output / f"attempt-{index:03d}-rate-{rate}"
        directory.mkdir()
        cmd = command(args, rate, index, directory)
        trial = {"index": index, "rate": rate, "command": cmd, "report": str(directory / "report.json")}
        trials.append(trial)
        (directory / "command.json").write_text(json.dumps(cmd, indent=2) + "\n")
        print(shlex.join(cmd), flush=True)
        if args.dry_run:
            if rate == args.max_rate:
                stop = "dry_run_candidates"
                break
            rate = min(rate * 2, args.max_rate)
            continue
        try:
            with (directory / "replay.log").open("w") as log:
                # No shell and no process-group killing: replay owns and joins its
                # managed server; external server is never signalled.
                proc = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT)
            trial["exit_code"] = proc.returncode
            report_path = directory / "report.json"
            if report_path.exists():
                trial["decision"] = classify(json.loads(report_path.read_text()), rate, args)
            else:
                trial["decision"] = {"passed": False, "failure_reasons": ["missing_report"]}
            if proc.returncode != 0:
                trial["decision"]["passed"] = False
                trial["decision"]["failure_reasons"].append("replay_exit_nonzero")
        except (OSError, ValueError, KeyError) as exc:
            trial["decision"] = {"passed": False, "failure_reasons": ["execution_or_report_error"], "error": str(exc)}
        (directory / "decision.json").write_text(json.dumps(trial, indent=2) + "\n")
        passed = trial["decision"]["passed"]
        if passed:
            low = rate
            if rate == args.max_rate:
                stop = "max_rate_reached_lower_bound"
                break
            next_rate = min(rate * 2, args.max_rate) if high is None else (low + high) // 2
        else:
            high = rate
            if not low and rate <= args.min_rate:
                stop = "no_passing_rate"
                break
            next_rate = max(args.min_rate, rate // 2) if not low else (low + high) // 2
        if low and high is not None and high - low <= args.resolution:
            stop = "bracket_within_resolution"
            break
        save_summary(args, trials, low, high, "in_progress")
        rate = next_rate
        if index < args.max_attempts:
            time.sleep(args.cooldown)
    save_summary(args, trials, low, high, stop)
    return 0 if args.dry_run or low else 3


if __name__ == "__main__":
    raise SystemExit(main())
