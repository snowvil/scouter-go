#!/usr/bin/env python3
"""Summarize scripts/connection-scale.sh results: one block per scenario.

Usage: connection-scale-summary.py DIR [DIR...]
Each DIR is a scenario directory (agents.json, server.log) or a run directory
holding several. Steady values are taken over the last half of the samples
in which every target session was settled (or the last half of the run).
"""
import json
import os
import statistics
import sys


def scenario_dirs(paths):
    for p in paths:
        if os.path.exists(os.path.join(p, "agents.json")):
            yield p
            continue
        for name in sorted(os.listdir(p)):
            d = os.path.join(p, name)
            if os.path.exists(os.path.join(d, "agents.json")):
                yield d


def med(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def summarize(d):
    r = json.load(open(os.path.join(d, "agents.json")))
    samples = r.get("samples") or []
    base = samples[0] if samples else {}
    target = r["target_sessions"]
    full = [s for s in samples[1:] if s.get("sessions_settled") == target]
    steady = full[len(full) // 2:] if full else samples[len(samples) // 2:]
    proc = [s["proc"] for s in steady if s.get("proc")]
    rss = med([p["rss_kib"] for p in proc])
    base_rss = (base.get("proc") or {}).get("rss_kib")
    sessions = med([s.get("sessions_settled") for s in steady]) or 0
    per_conn = None
    if rss and base_rss and sessions:
        per_conn = (rss - base_rss) / sessions
    cpu = [p["cpu_percent"] for p in proc if p.get("cpu_percent")]
    ingest_p99 = [s["stats"]["ingestLatency"]["p99Us"] for s in steady if s.get("stats")]
    last = samples[-1] if samples else {}
    tcp = last.get("tcp") or {}
    log = open(os.path.join(d, "server.log"), errors="replace").read().splitlines()
    rejected_lines = [l for l in log if " rejected" in l and "connections=" in l]
    accept_lines = [l for l in log if "tcp.accept" in l]
    orphan_lines = [l for l in log if "agent relay" in l]
    fl = r["fleet"]
    out = {
        "scenario": os.path.basename(d),
        "target": target,
        "v1_agents": r.get("v1_agents"),
        "all_connected": r["all_connected"],
        "all_connected_after_s": round(r["all_connected_after_ms"] / 1000, 2),
        "final_settled": r["final_settled"],
        "min_settled_after_full": r["min_settled_after_full"],
        "episodes": [{k: e.get(k) for k in ("start_ms", "duration_ms", "recovered", "min_connected", "connect_attempts", "connect_fail", "sessions_ended_before_command")} for e in r.get("episodes") or []],
        "connect_attempts": fl["connect_attempts"],
        "connect_fail": fl["connect_fail"],
        "ended_before_command": fl["sessions_ended_before_command"],
        "ended_after_command": fl["sessions_ended_after_command"],
        "keepalives": fl["keepalives"],
        "keepalive_gap_ms": {k: round(fl["keepalive_gap"].get(k) or 0, 1) for k in ("p50_ms", "p99_ms", "max_ms")},
        "write_errors": fl["write_errors"],
        "commands": fl["commands"],
        "responses": fl["responses"],
        "baseline_rss_mib": round(base_rss / 1024, 1) if base_rss else None,
        "steady_rss_mib": round(rss / 1024, 1) if rss else None,
        "rss_per_session_kib": round(per_conn, 1) if per_conn else None,
        "steady_fds": med([p["fds"] for p in proc]),
        "steady_threads": med([p["threads"] for p in proc]),
        "steady_goroutines": med([s.get("goroutines") for s in steady]),
        "steady_cpu_pct_median": round(med(cpu), 1) if cpu else None,
        "steady_cpu_pct_mean": round(statistics.mean(cpu), 1) if cpu else None,
        "peak": {k: round(v, 1) for k, v in (r.get("peak") or {}).items()},
        "steady_ingest_p99_us_max": max(ingest_p99) if ingest_p99 else None,
        "tcp_last": {k: tcp.get(k) for k in ("Accepted", "AgentConnections", "ClientConnections", "HandshakeRejected", "AgentRejected", "ClientRejected", "AcceptRetries", "Errors", "ClientWaitRejected")},
        "orphans_last": last.get("agent_orphans"),
        "clients": {k: {"ok": v["ok"], "fail": v["fail"], "p99_ms": v["latency"].get("p99_ms"), "max_ms": v["latency"].get("max_ms")} for k, v in (r.get("clients") or {}).items()},
        "probe": r.get("probe") and {"ok": r["probe"]["ok"], "fail": r["probe"]["fail"], "p99_ms": r["probe"]["latency"].get("p99_ms"), "last_error": r["probe"].get("last_error")},
        "relay": r.get("relay") and {"ok": r["relay"]["ok"], "fail": r["relay"]["fail"], "last_error": r["relay"].get("last_error")},
        "udp": r.get("udp"),
        "client_objects_alive_last": [s.get("client_alive") for s in samples[-3:]],
        "server_log_lines": len(log),
        "server_reject_lines": len(rejected_lines),
        "server_accept_lines": len(accept_lines),
        "server_orphan_lines": len(orphan_lines),
        "sample_errors": sorted({s.get(k) for s in samples for k in ("health_error", "proc_error", "goroutine_error") if s.get(k)})[:5],
    }
    return out


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    for d in scenario_dirs(sys.argv[1:]):
        print(json.dumps(summarize(d), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
