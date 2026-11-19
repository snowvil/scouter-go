#!/usr/bin/env python3
"""원본 Java Agent 송신 계층(DataProxy/UDPDataSendThread/DataUdpAgent)으로 XLog 부하를 보낸다.

원본 source는 고정 commit과 git blob으로 확인한 뒤 임시 디렉터리에서 수정 없이 컴파일한다.
--server를 주면 임시 Go 서버(loopback, 임시 DB)를 띄워 송신 후 종료 stats와 송신 수를 대조한다.
사용법은 tools/java-loadgen/README.md.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
TOOL = ROOT / "tools/java-loadgen"
AGENT = "scouter.agent.java/src/main/java/scouter/agent"
AGENT_FILES = ["netio/data/DataProxy.java", "netio/data/UDPDataSendThread.java", "netio/data/net/DataUdpAgent.java"]
GUARDED = ["scouter.common/src/main", f"{AGENT}/Configure.java", *[f"{AGENT}/{name}" for name in AGENT_FILES]]
# Seam fields whose initial value is not a Configure.getXxx default (documented in README).
SEAM_ONLY = {"obj_type"}


def run(command, *, cwd=None, timeout=300):
    result = subprocess.run([str(c) for c in command], cwd=cwd, timeout=timeout, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {command[0]}\n"
                           + result.stdout[-8000:] + result.stderr[-8000:])
    return result.stdout


def guard_sources(source):
    baseline = json.loads((ROOT / "docs/compatibility/baseline.json").read_text())["scouter"]
    commit = run(["git", "-C", source, "rev-parse", "HEAD"]).strip()
    if commit != baseline["commit"]:
        raise RuntimeError(f"source SHA mismatch: expected {baseline['commit']}, got {commit}")
    if run(["git", "-C", source, "status", "--porcelain", "--", *GUARDED]):
        raise RuntimeError("original common/agent send-path sources are modified; load generator rejected")
    expected = {}
    for line in run(["git", "-C", source, "ls-tree", "-r", "HEAD", "--", *GUARDED]).splitlines():
        metadata, name = line.split("\t", 1)
        expected[name] = metadata.split()[2]
    common = sorted((source / "scouter.common/src/main/java").rglob("*.java"))
    agent = [source / AGENT / name for name in AGENT_FILES]
    resources = sorted(p for p in (source / "scouter.common/src/main/resources").rglob("*") if p.is_file())
    for path in common + agent + resources + [source / AGENT / "Configure.java"]:
        data = path.read_bytes()
        blob = hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()
        if expected.get(path.relative_to(source).as_posix()) != blob:
            raise RuntimeError(f"original source differs from git blob: {path.relative_to(source)}")
    return commit, baseline["version"], common, agent, resources


def check_seam_defaults(source):
    """Every seam Configure field must start at the original Configure.getXxx default."""
    original = (source / AGENT / "Configure.java").read_text()
    seam = (TOOL / "seams/scouter/agent/Configure.java").read_text()
    checked = {}
    for kind, name, value in re.findall(r"public (\w+) (\w+) = ([^;]+);", seam):
        if name in SEAM_ONLY:
            continue
        match = re.search(r"this\." + name + r"\s*=\s*get(\w+)\(\"" + name + r"\"(?:,\s*([^,)]+))?", original)
        if not match:
            raise RuntimeError(f"seam field {name} has no original Configure getter")
        default = match.group(2)
        if default is None:
            default = {"Value": "null", "Boolean": "false"}.get(match.group(1))
        if default is None or default.strip() != value.strip():
            raise RuntimeError(f"seam default {name}={value} differs from original {match.group(0)}")
        checked[name] = value
    return checked


def java_tools(java_home):
    home = Path(java_home or os.environ.get("JAVA_HOME") or "/Library/Java/JavaVirtualMachines/zulu-18.jdk/Contents/Home")
    java, javac = home / "bin/java", home / "bin/javac"
    if not java.is_file() or not javac.is_file():
        raise RuntimeError("JDK 11+ is required; set --java-home")
    return java, javac


def compile_loadgen(source, temp, javac):
    commit, version, common, agent, resources = guard_sources(source)
    defaults = check_seam_defaults(source)
    classes = temp / "classes"
    classes.mkdir()
    tool_sources = sorted(TOOL.rglob("*.java"))
    argfile = temp / "sources.txt"
    argfile.write_text("\n".join('"' + str(p).replace("\\", "\\\\").replace('"', '\\"') + '"'
                                 for p in common + agent + tool_sources), encoding="utf-8")
    run([javac, "-J-XX:ActiveProcessorCount=2", "--release", "11", "-nowarn", "-encoding", "UTF-8",
         "-sourcepath", "", "-d", classes, "@" + str(argfile)], cwd=temp)
    root = source / "scouter.common/src/main/resources"
    for resource in resources:
        destination = classes / resource.relative_to(root)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(resource, destination)
    return classes, {"source_commit": commit, "source_version": version,
                     "original_agent_sources": [f"{AGENT}/{name}" for name in AGENT_FILES],
                     "original_common_sources": len(common),
                     "seams": [p.relative_to(ROOT).as_posix() for p in tool_sources if "seams" in p.parts],
                     "seam_defaults_checked": defaults}


def start_server(binary, temp):
    ready = temp / "ready.json"
    log_path = temp / "server.log"
    with log_path.open("w") as log:
        process = subprocess.Popen([str(binary), "--udp", "127.0.0.1:0", "--tcp", "127.0.0.1:0",
                                    "--db", str(temp / "db"), "--user", "admin", "--password", "admin",
                                    "--conf", "", "--ready-file", str(ready), "--exit-file=false"],
                                   cwd=temp, stdout=log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Go server exited before readiness\n" + log_path.read_text()[-4000:])
        try:
            info = json.loads(ready.read_text())
            return process, info["udp"], info["tcp"]
        except (OSError, json.JSONDecodeError, KeyError):
            time.sleep(0.05)
    stop_server(process)
    raise RuntimeError("Go server ready-file timeout")


def stop_server(process):
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            raise RuntimeError("Go server did not stop within 20 seconds")
    return process.returncode


def server_statistics(log_text):
    matches = re.findall(r"ingest=\{([^}]*)\} udp=\{([^}]*)\} tcp=\{([^}]*)\}", log_text)
    if not matches:
        raise RuntimeError("Go server final ingest/udp/tcp statistics line is missing")
    return {name: {k: int(v) for k, v in re.findall(r"(\w+):(-?\d+)", body)}
            for name, body in zip(("ingest", "udp", "tcp"), matches[-1])}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", type=Path, default=Path(os.environ.get("SCOUTER_SOURCE", ROOT.parent / "scouter")))
    parser.add_argument("--java-home")
    parser.add_argument("--target", help="127.0.0.1:port (loopback only); --server와 함께 쓰지 않는다")
    parser.add_argument("--server", type=Path, help="임시 Go 서버로 쓸 binary(예: dist/scouter-go)")
    parser.add_argument("--objects", type=int, default=1)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--rate", type=float, default=100, help="전체 목표 XLog/s, 0은 무제한")
    parser.add_argument("--duration", type=float, default=10)
    parser.add_argument("--profile-ratio", type=float, default=0)
    parser.add_argument("--profile-sizes", default="4096", help="쉼표 구분 profile byte 크기(Step.toBytes 기준 하한)")
    parser.add_argument("--services", type=int, default=100)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--run-id", type=int, help="txid 상위 32bit(기본은 seed에서 결정)")
    parser.add_argument("--set", action="append", default=[], metavar="NAME=VALUE",
                        help="net_udp_packet_max_bytes, net_udp_collection_interval_ms, profile_step_max_count만 허용")
    parser.add_argument("--heap", default="256m", help="JVM -Xmx")
    parser.add_argument("--jvm-cpus", type=int, help="JVM -XX:ActiveProcessorCount")
    parser.add_argument("--txid-file", type=Path, help="성공 송신한 txid를 한 줄에 하나씩 기록")
    parser.add_argument("--report", type=Path, help="최종 JSON 저장 경로")
    parser.add_argument("--keep-temp", action="store_true")
    args = parser.parse_args()
    if bool(args.target) == bool(args.server):
        parser.error("--target 또는 --server 중 하나만 지정한다")

    source = args.source.expanduser().resolve()
    java, javac = java_tools(args.java_home)
    temp = Path(tempfile.mkdtemp(prefix="scouter-java-loadgen-"))
    server = None
    exit_code = 1
    try:
        classes, build = compile_loadgen(source, temp, javac)
        target = args.target
        if args.server:
            binary = args.server.expanduser().resolve()
            server, target, tcp = start_server(binary, temp)
            build["server"] = {"binary": str(binary), "udp": target, "tcp": tcp,
                               "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
        command = [java, f"-Xmx{args.heap}", "-Djava.awt.headless=true", "-Dfile.encoding=UTF-8"]
        if args.jvm_cpus:
            command.append(f"-XX:ActiveProcessorCount={args.jvm_cpus}")
        command += ["-cp", classes, "AgentLoadGen", "--target", target,
                    "--objects", args.objects, "--threads", args.threads, "--rate", args.rate,
                    "--duration", args.duration, "--profile-ratio", args.profile_ratio,
                    "--profile-sizes", args.profile_sizes, "--services", args.services, "--seed", args.seed]
        if args.run_id is not None:
            command += ["--run-id", args.run_id]
        for item in args.set:
            name, _, value = item.partition("=")
            command += [f"--set.{name}", value]
        if args.txid_file:
            command += ["--txid-file", args.txid_file.expanduser().resolve()]
        jvm = subprocess.run([str(c) for c in command], cwd=temp, capture_output=True, text=True,
                             timeout=args.duration + 120)
        if jvm.stderr.strip():
            print(jvm.stderr.strip()[-4000:], file=sys.stderr)
        lines = [line for line in jvm.stdout.splitlines() if line.startswith("{")]
        if not lines:
            raise RuntimeError(f"load generator produced no report (exit {jvm.returncode})\n{jvm.stdout[-4000:]}")
        report = json.loads(lines[-1])
        report["jvm_exit_code"] = jvm.returncode
        report["build"] = build
        report["jvm_command"] = [str(c) for c in command[:command.index("-cp")]]
        exit_code = jvm.returncode
        if server is not None:
            time.sleep(1.0)  # let the server drain its UDP socket before shutdown statistics
            code = stop_server(server)
            server = None
            stats = server_statistics((temp / "server.log").read_text())
            checks = {
                "ingest.XLogs == xlog.sent": [stats["ingest"].get("XLogs"), report["xlog"]["sent"]],
                "udp.Datagrams == udp.datagrams": [stats["udp"].get("Datagrams"), report["udp"]["datagrams"]],
                "udp.Packets == udp.expected_server_udp_packets":
                    [stats["udp"].get("Packets"), report["udp"]["expected_server_udp_packets"]],
            }
            report["server_check"] = {"server_exit_code": code, "statistics": stats,
                                      "checks": {k: {"server": v[0], "sender": v[1], "equal": v[0] == v[1]}
                                                 for k, v in checks.items()}}
            if code != 0 or not all(v[0] == v[1] for v in checks.values()):
                exit_code = exit_code or 4
        text = json.dumps(report, indent=2, ensure_ascii=False)
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(text + "\n")
        print(text)
    finally:
        if server is not None:
            stop_server(server)
        if args.keep_temp:
            print(f"temporary directory: {temp}", file=sys.stderr)
        else:
            shutil.rmtree(temp, ignore_errors=True)
    return exit_code


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        raise SystemExit(str(error)) from error
