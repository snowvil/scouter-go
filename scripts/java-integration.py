#!/usr/bin/env python3
"""고정 원본 Host/Client 클래스와 임시 Go 서버의 실제 socket 상호운용 검증."""
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

ROOT = Path(__file__).resolve().parents[1]
HOST_FILES = [
    "Configure.java", "Logger.java", "netio/data/HostAgentDataProxy.java",
    "netio/data/net/DataUdpAgent.java", "netio/data/net/TcpWorker.java",
    "netio/request/ReqestHandlingProxy.java", "netio/request/anotation/RequestHandler.java",
    "netio/request/handle/HostAgentEnv.java",
]
CLIENT_FILES = [
    "net/ClientTCP.java", "net/TcpProxy.java", "net/ConnectionPool.java",
    "net/DummyTcpProxy.java", "net/INetReader.java", "net/LoginMgr.java", "net/LoginResult.java",
    "server/Server.java", "server/ServerManager.java",
    "preferences/ServerPrefUtil.java", "preferences/PreferenceConstants.java",
]


def run(command, *, cwd=ROOT, timeout=120, env=None):
    result = subprocess.run(command, cwd=cwd, env=env, timeout=timeout,
                            capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}): {command[0]}\n"
                           + result.stdout[-12000:] + result.stderr[-12000:])
    return result.stdout


def source_files(source):
    files = sorted((source / "scouter.common/src/main/java").rglob("*.java"))
    files += [source / "scouter.agent.host/src/main/java/scouter/agent" / name for name in HOST_FILES]
    files += [source / "scouter.client/src/scouter/client" / name for name in CLIENT_FILES]
    if not files or any(not path.is_file() for path in files):
        raise RuntimeError("Required original Java source files are missing")
    return files


def guard_sources(source, files):
    baseline = json.loads((ROOT / "docs/compatibility/baseline.json").read_text())["scouter"]
    commit = run(["git", "-C", str(source), "rev-parse", "HEAD"]).strip()
    if commit != baseline["commit"]:
        raise RuntimeError(f"Source SHA mismatch: expected {baseline['commit']}, got {commit}")
    paths = ["scouter.common", "scouter.agent.host/src/main/java", "scouter.client/src"]
    if run(["git", "-C", str(source), "status", "--porcelain", "--", *paths]):
        raise RuntimeError("Original common/Host/Client sources are modified; integration rejected")
    records = run(["git", "-C", str(source), "ls-tree", "-r", "HEAD", "--", *paths])
    expected = {}
    for line in records.splitlines():
        metadata, name = line.split("\t", 1)
        expected[name] = metadata.split()[2]
    hashes = []
    for path in files:
        name = path.relative_to(source).as_posix()
        data = path.read_bytes()
        blob = hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()
        if expected.get(name) != blob:
            raise RuntimeError(f"Original source differs from baseline git blob: {name}")
        hashes.append({"path": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                       "git_blob": blob})
    return {"source_commit": commit, "source_version": baseline["version"],
            "original_sources": hashes, "external_jars": [], "javac_release": 11}


def java_tools(java_home):
    if java_home:
        home = Path(java_home).expanduser().resolve()
        java, javac = home / "bin/java", home / "bin/javac"
    elif sys.platform == "darwin" and Path("/Library/Java/JavaVirtualMachines/zulu-18.jdk/Contents/Home").is_dir():
        home = Path("/Library/Java/JavaVirtualMachines/zulu-18.jdk/Contents/Home")
        java, javac = home / "bin/java", home / "bin/javac"
    elif os.environ.get("JAVA_HOME"):
        home = Path(os.environ["JAVA_HOME"])
        java, javac = home / "bin/java", home / "bin/javac"
    else:
        java, javac = shutil.which("java"), shutil.which("javac")
    if not java or not javac or not Path(java).is_file() or not Path(javac).is_file():
        raise RuntimeError("JDK 11+ is required; set --java-home")
    return str(java), str(javac)


def compile_java(source, temp, java, javac):
    originals = source_files(source)
    resource_root = source / "scouter.common/src/main/resources"
    resources = sorted(path for path in resource_root.rglob("*") if path.is_file())
    report = guard_sources(source, originals + resources)
    report["original_resources"] = report["original_sources"][len(originals):]
    report["original_sources"] = report["original_sources"][:len(originals)]
    classes = temp / "classes"
    classes.mkdir()
    adapters = sorted((ROOT / "tools/java-integration").rglob("*.java"))
    adapters += [ROOT / "tools/java-storage/ExtraDataset.java"]
    sources = originals + adapters
    argfile = temp / "sources.txt"
    argfile.write_text("\n".join('"' + str(path).replace("\\", "\\\\").replace('"', '\\"') + '"'
                                for path in sources), encoding="utf-8")
    # All original dependencies are explicit. No implicit source lookup or external jars.
    run([javac, "-J-XX:ActiveProcessorCount=2", "--release", "11", "-encoding", "UTF-8", "-sourcepath", "", "-cp", str(classes),
         "-d", str(classes), "@" + str(argfile)], cwd=temp)
    for resource in resources:
        destination = classes / resource.relative_to(resource_root)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(resource, destination)
    version = subprocess.run([java, "-version"], capture_output=True, text=True, check=True)
    report.update({"java_version": version.stderr.strip(), "class_files": len(list(classes.rglob("*.class"))),
                   "harness_sources": [{"path": path.relative_to(ROOT).as_posix(),
                                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                                       for path in adapters],
                   "adapter": "scouter.client.preferences.PManager: UI preferences only"})
    report["planned_query_commands"] = sorted({name for path in adapters if path.name.endswith("Harness.java")
                                                for name in re.findall(r"RequestCmd\.([A-Z0-9_]+)", path.read_text())}
                                               | {"LOGIN", "GET_XML_COUNTER"})
    (temp / "source-report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(f"PASS original Java compilation: {len(originals)} original sources, "
          f"{report['class_files']} classes, --release 11, no external jars", flush=True)
    return classes, report


def stop_server(process):
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            raise RuntimeError("Go server did not stop gracefully within 15 seconds")
    if process.returncode != 0:
        raise RuntimeError(f"Go server exit code {process.returncode}; expected graceful exit 0")


def server_statistics(temp, sequence, extra_datagrams=0):
    text = (temp / f"server-{sequence}.log").read_text()
    matches = re.findall(r"ingest=\{([^}]*)\} udp=\{([^}]*)\} tcp=\{([^}]*)\}", text)
    if not matches:
        raise RuntimeError("Go server final ingest/UDP/TCP statistics are missing")
    statistics = {name: {key: int(value) for key, value in re.findall(r"(\w+):(\d+)", body)}
                  for name, body in zip(("ingest", "udp", "tcp"), matches[-1])}
    expected = {"ingest": {"Completed": 41 if sequence == 1 else 0,
                            "XLogs": 5 if sequence == 1 else 0,
                            "DroppedXLogs": 1 if sequence == 1 else 0, "SkippedStacks": 0,
                            "Rejected": 0, "StorageErrors": 0,
                            # Asynchronous record writers (internal/ingest CONTRACT.md §13).
                            # WritePending is left out: this line is printed before Close drains.
                            "WriteErrors": 0, "WriteInvalid": 0, "WriteDropped": 0,
                            "WriteAbandoned": 0, "VisitorsSkipped": 0, "Sample5MDropped": 0},
                "udp": {"Packets": 41 if sequence == 1 else 0,
                        "Datagrams": 14 + extra_datagrams if sequence == 1 else 0,
                        "Rejected": 0, "Malformed": 0, "HandlerErrors": 0,
                        "AbortedAssemblies": 0, "AbortedPackets": 0},
                "tcp": {"Rejected": 0, "Errors": 0, "Active": 0}}
    for category, counts in expected.items():
        for key, value in counts.items():
            if statistics[category].get(key) != value:
                raise RuntimeError(f"Server statistics mismatch: {category}.{key} "
                                   f"expected {value}, got {statistics[category].get(key)}")
    return statistics


def endpoint(value):
    if not isinstance(value, str):
        raise RuntimeError("ready-file endpoint must be a string")
    host, port = value.rsplit(":", 1)
    if host != "127.0.0.1" or not 0 < int(port) < 65536:
        raise RuntimeError("Integration requires ephemeral loopback endpoints")
    return int(port)


def start_server(binary, temp, sequence):
    ready = temp / f"ready-{sequence}.json"
    log_path = temp / f"server-{sequence}.log"
    log = log_path.open("w")
    environment = os.environ.copy()
    environment["GOMAXPROCS"] = "2"
    # The FIVE_MIN checks expect only the seeded bucket. The server's own
    # 5-minute sampling (Java Auto5MSampling) would add a bucket whenever a run
    # crosses a 5-minute boundary, so it is switched off for the harness.
    conf = temp / "scouter.conf"
    conf.write_text("_auto_5m_sampling=false\n", encoding="utf-8")
    process = subprocess.Popen([str(binary), "--udp", "127.0.0.1:0", "--tcp", "127.0.0.1:0",
                                "--db", str(temp / "db"), "--timezone", "Asia/Seoul",
                                "--user", "admin", "--password", "admin", "--ready-file", str(ready),
                                "--conf", str(conf), "--exit-file=false"],
                               cwd=temp, env=environment, stdout=log, stderr=subprocess.STDOUT)
    log.close()
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("Go server exited before readiness")
            if ready.is_file():
                try:
                    info = json.loads(ready.read_text())
                    udp, tcp = endpoint(info["udp"]), endpoint(info["tcp"])
                    return process, udp, tcp
                except (json.JSONDecodeError, KeyError):
                    pass
            time.sleep(0.05)
        raise RuntimeError("Go server ready-file timeout")
    except Exception as error:
        try:
            stop_server(process)
        except RuntimeError:
            pass
        raise RuntimeError(f"{error}\n{log_path.read_text()[-12000:]}") from error


def harness(java, classes, temp, udp, tcp, timestamp, mode):
    config = temp / f"host-{mode}.conf"
    config.write_text("\n".join([
        "obj_name=java-interop-synthetic", "obj_type=host", "net_collector_ip=127.0.0.1",
        f"net_collector_udp_port={udp}", f"net_collector_tcp_port={tcp}",
        "net_local_udp_ip=127.0.0.1", "net_local_udp_port=0", "net_udp_packet_max_bytes=512",
        "net_collector_tcp_so_timeout_ms=60000", f"log_dir={temp / 'host-logs'}",
        "log_udp_object=false", "log_udp_pack=false", "",
    ]), encoding="utf-8")
    output = run([java, "-XX:ActiveProcessorCount=2", "-Xmx128m", "-Djava.awt.headless=true",
                  "-Duser.timezone=Asia/Seoul", f"-Dscouter.config={config}",
                  "-Dscouter.integration.marker=original-host-tcp", "-cp", str(classes),
                  "InteropHarness", str(tcp), str(timestamp), mode], cwd=temp, timeout=90)
    print(output.strip(), flush=True)
    return [line for line in output.splitlines() if line.startswith("PASS ")]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, help="검증할 Go binary; 생략하면 임시 경로에 새로 빌드")
    parser.add_argument("--source", type=Path, default=Path(os.environ.get("SCOUTER_SOURCE", ROOT.parent / "scouter")))
    parser.add_argument("--java-home", help="JDK 경로; macOS 로컬 JDK18을 우선 사용")
    parser.add_argument("--compile-only", action="store_true", help="원본 SHA guard와 실제 클래스 컴파일만 수행")
    parser.add_argument("--report", type=Path, help="컴파일 source checksum과 검증 결과 JSON 저장")
    parser.add_argument("--keep-temp", action="store_true", help="성공/실패 임시 classes/db/log를 보존")
    args = parser.parse_args()
    source = args.source.expanduser().resolve()
    java, javac = java_tools(args.java_home)
    temp = Path(tempfile.mkdtemp(prefix="scouter-java-integration-"))
    process = None
    report = None
    try:
        classes, report = compile_java(source, temp, java, javac)
        report["checks"] = []
        report["server_statistics"] = []
        report["integration_passed"] = False
        if args.compile_only:
            report["integration_not_run"] = "--compile-only"
            return
        binary = args.server.expanduser().resolve() if args.server else temp / "scouter-go"
        if not args.server:
            go = ROOT / ".tools/go/bin/go"
            go_command = str(go) if go.is_file() else shutil.which("go")
            if not go_command:
                raise RuntimeError("Go toolchain missing; bootstrap Go or provide --server")
            environment = os.environ.copy()
            environment["GOMAXPROCS"] = "2"
            run([go_command, "build", "-o", str(binary), "./cmd/scouter-go"], env=environment)
        if not binary.is_file():
            raise RuntimeError(f"Go server binary missing: {binary}")
        report["server_binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
        # Keep stime=timestamp-1 in the current date even during the midnight minute.
        # All synthetic records fit within this minute and start on a 500ms bucket.
        timestamp = int(time.time() * 1000) // 60000 * 60000 + 1000
        report["synthetic_timestamp_ms"] = timestamp
        process, udp, tcp = start_server(binary, temp, 1)
        report["checks"] += harness(java, classes, temp, udp, tcp, timestamp, "seed")
        stop_server(process)
        extras = [re.search(r"extra UDP packs=29 datagrams=(\d+)", line) for line in report["checks"]]
        extra_datagrams = [int(match.group(1)) for match in extras if match]
        if len(extra_datagrams) != 1:
            raise RuntimeError("Extra seed expected datagram count missing")
        report["server_statistics"].append({"phase": "seed", **server_statistics(temp, 1, extra_datagrams[0])})
        process = None
        process, udp, tcp = start_server(binary, temp, 2)
        report["checks"] += harness(java, classes, temp, udp, tcp, timestamp, "verify")
        stop_server(process)
        report["server_statistics"].append({"phase": "restart", **server_statistics(temp, 2)})
        process = None
        report["integration_passed"] = True
        print("PASS graceful SIGTERM, same-db restart and actual Client history queries", flush=True)
    finally:
        try:
            if process is not None:
                stop_server(process)
        finally:
            if report is not None and args.report:
                args.report.parent.mkdir(parents=True, exist_ok=True)
                args.report.write_text(json.dumps(report, indent=2) + "\n")
            if args.keep_temp:
                print(f"Temporary evidence: {temp}", flush=True)
            else:
                shutil.rmtree(temp)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        raise SystemExit(str(error)) from error
