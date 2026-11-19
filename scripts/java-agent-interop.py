#!/usr/bin/env python3
"""실제 scouter.agent.java(-javaagent)를 임시 Go 서버(루프백, 임시 DB)에 붙인 상호운용 검증.

구성(모두 127.0.0.1, 임시 directory):
  샘플 JVM(-javaagent:scouter.agent.jar) --UDP--> Go 서버 A
                                       --TCP V2--> 기록 proxy --> Go 서버 A
  원본 Client 클래스(TcpProxy/LoginMgr)로 만든 driver --> Go 서버
기록 proxy는 서버→Agent 명령 frame과 Agent 응답(HasNEXT/NoNEXT)을 시각과 함께 남긴다.
시나리오: 수집, relay, 고아 relay·Agent 실패·응답 상한, KEEP_ALIVE, 재기동 후 text reset,
conf의 collector 주소 변경(같은 서버의 다른 포트, 새 서버), 서버 재기동 후 재연결 시간,
서버 발신 명령(kube Host Agent의 OBJECT_SET_KUBE_SEQ, CPU 초과 TRIGGER_THREAD_DUMPS_FROM_CONDITIONS).
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools/java-agent-interop"
OBJ_NAME = "/interop-host/interop-sample"
MARKER = "agent-interop-synthetic"
BLOB_COMMANDS = {"OBJECT_DUMP_FILE_DETAIL", "OBJECT_DOWNLOAD_HEAP_DUMP"}
# 기록 proxy가 파라미터 MapPack까지 풀어 남기는 서버 발신 명령.
PARAM_COMMANDS = {"TRIGGER_THREAD_DUMPS_FROM_CONDITIONS"}
# kube Host Agent: KUBERNETES_SERVICE_HOST + 18자 넘는 host 이름(앞 두 '-' 조각을 뺀 것이 pod 이름).
KUBE_HOST_NAME = "interop-kube-pod-7d9f8b6c5d-x2x4z"
KUBE_POD = "interop-kube-pod"
# CPU autodump(Java Agent 설정 키와 같은 이름).
AUTODUMP_DURATION_MS = 8000
AUTODUMP_INTERVAL_MS = 2000
AUTODUMP_CNT = 3
# git diff v2.20.0..baseline에 대해 비어 있어야 prebuilt 2.20.0 jar를 baseline 대용으로 쓴다.
WIRE_PATHS = [
    "scouter.agent.java/src/main/java/scouter/agent/netio",
    "scouter.agent.java/src/main/java/scouter/agent/counter",
    "scouter.agent.java/src/main/java/scouter/agent/Configure.java",
    "scouter.agent.java/src/main/java/scouter/agent/JavaAgent.java",
    "scouter.agent.java/src/main/java/scouter/agent/AgentBoot.java",
    "scouter.common/src/main/java/scouter/net",
    "scouter.common/src/main/java/scouter/io",
    "scouter.common/src/main/java/scouter/lang",
]


def load_java_integration():
    spec = importlib.util.spec_from_file_location("java_integration", ROOT / "scripts/java-integration.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


JI = load_java_integration()
run = JI.run


def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def free_port():
    """같은 번호의 TCP·UDP가 모두 비어 있는 loopback 포트."""
    for _ in range(50):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as t:
            t.bind(("127.0.0.1", 0))
            port = t.getsockname()[1]
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as u:
                    u.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise RuntimeError("no free loopback port")


# ---------------------------------------------------------------- 기록 proxy

class Feed:
    """generator parser에 byte를 흘려 넣는다. generator는 ('read', n) 또는 ('skip', n)을 yield한다."""

    def __init__(self, generator, on_error):
        self.generator = generator
        self.on_error = on_error
        self.buffer = bytearray()
        self.want = next(generator)

    def push(self, data):
        if self.want is None:
            return
        self.buffer += data
        try:
            while self.want is not None:
                kind, n = self.want
                if kind == "read":
                    if len(self.buffer) < n:
                        return
                    chunk = bytes(self.buffer[:n])
                    del self.buffer[:n]
                    self.want = self.generator.send(chunk)
                else:
                    take = min(n, len(self.buffer))
                    del self.buffer[:take]
                    if n - take:
                        self.want = ("skip", n - take)
                        return
                    self.want = self.generator.send(None)
        except StopIteration:
            self.want = None
        except Exception as error:  # parser 오류는 기록만 하고 전달은 계속한다
            self.want = None
            self.on_error(repr(error))


def read_blob(body, offset):
    """DataInputX.readBlob: (bytes, 다음 offset)."""
    n = body[offset]
    offset += 1
    if n == 255:
        n = struct.unpack(">H", body[offset:offset + 2])[0]
        offset += 2
    elif n == 254:
        n = struct.unpack(">i", body[offset:offset + 4])[0]
        offset += 4
    if offset + n > len(body):
        raise ValueError("blob past the frame")
    return body[offset:offset + n], offset + n


def read_text(body):
    return read_blob(body, 0)[0].decode("utf-8", "replace")


def read_decimal(body, offset):
    width = struct.unpack(">b", body[offset:offset + 1])[0]
    offset += 1
    width = width if 0 <= width <= 5 else 8
    return int.from_bytes(body[offset:offset + width], "big", signed=True) if width else 0, offset + width


def read_map_pack(body, offset):
    """readPack()이 MapPack(10)이고 값이 Null/Boolean/Decimal/Text인 파라미터만 푼다."""
    if body[offset] != 10:
        raise ValueError(f"pack type {body[offset]}")
    count, offset = read_decimal(body, offset + 1)
    result = {}
    for _ in range(count):
        key, offset = read_blob(body, offset)
        kind = body[offset]
        offset += 1
        if kind == 0:
            value = None
        elif kind == 10:
            value, offset = body[offset] != 0, offset + 1
        elif kind == 20:
            value, offset = read_decimal(body, offset)
        elif kind == 50:
            raw, offset = read_blob(body, offset)
            value = raw.decode("utf-8", "replace")
        else:
            raise ValueError(f"value type {kind}")
        result[key.decode("utf-8", "replace")] = value
    if offset != len(body):
        raise ValueError(f"{len(body) - offset} bytes after the parameter")
    return result


class ProxyConn:
    def __init__(self, proxy, conn_id, agent, upstream):
        self.proxy, self.id, self.agent, self.upstream = proxy, conn_id, agent, upstream
        self.pending = []  # 응답을 기다리는 명령(V2는 한 번에 하나)
        self.items = 0
        self.item_bytes = 0
        self.closed = False
        self.lock = threading.Lock()

    def event(self, kind, **fields):
        self.proxy.record(conn=self.id, event=kind, **fields)

    def server_frames(self):
        while True:
            n = struct.unpack(">i", (yield ("read", 4)))[0]
            if n < 0 or n > (64 << 20):
                self.event("bad_frame", length=n)
                return
            head = yield ("read", min(n, 4096))
            if n > len(head):
                yield ("skip", n - len(head))
            text, offset = read_blob(head, 0)
            command = text.decode("utf-8", "replace")
            fields = {}
            if command in PARAM_COMMANDS and n == len(head):
                try:
                    fields["param"] = read_map_pack(head, offset)
                except (ValueError, IndexError, struct.error) as error:
                    fields["paramError"] = repr(error)
            with self.lock:
                self.pending.append(command)
            self.event("command", cmd=command, bytes=n, **fields)

    def agent_stream(self):
        magic, obj_hash = struct.unpack(">Ii", (yield ("read", 8)))
        self.event("magic", magic=hex(magic), objHash=obj_hash)
        while True:
            flag = (yield ("read", 1))[0]
            with self.lock:
                command = self.pending[0] if self.pending else None
            if flag == 4:
                with self.lock:
                    if self.pending:
                        self.pending.pop(0)
                self.event("answer", cmd=command, items=self.items, bytes=self.item_bytes)
                self.items = self.item_bytes = 0
                continue
            if flag != 3:
                self.event("bad_flag", flag=flag, cmd=command)
                return
            if command in BLOB_COMMANDS:
                h = (yield ("read", 1))[0]
                if h == 255:
                    n = struct.unpack(">H", (yield ("read", 2)))[0]
                elif h == 254:
                    n = struct.unpack(">i", (yield ("read", 4)))[0]
                else:
                    n = h
            else:
                n = struct.unpack(">i", (yield ("read", 4)))[0]
            if n:
                yield ("skip", n)
            self.items += 1
            self.item_bytes += n

    def pump(self, source, target, feed, side):
        try:
            while True:
                data = source.recv(65536)
                if not data:
                    break
                feed.push(data)
                target.sendall(data)
        except OSError:
            pass
        self.close(side)

    def close(self, side):
        with self.lock:
            if self.closed:
                return
            self.closed = True
        self.event("close", by=side)
        for s in (self.agent, self.upstream):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            s.close()


class FrameProxy:
    def __init__(self, listen_port, upstream_port, path):
        self.upstream_port = upstream_port
        self.events = []
        self.lock = threading.Lock()
        self.file = open(path, "w", encoding="utf-8")
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", listen_port))
        self.listener.listen(16)
        self.next_id = 0
        self.conns = {}
        threading.Thread(target=self.accept_loop, daemon=True).start()

    def record(self, **fields):
        fields["t"] = time.time()
        with self.lock:
            self.events.append(fields)
            self.file.write(json.dumps(fields) + "\n")
            self.file.flush()

    def accept_loop(self):
        while True:
            try:
                agent, _ = self.listener.accept()
            except OSError:
                return
            self.next_id += 1
            conn_id = self.next_id
            try:
                upstream = socket.create_connection(("127.0.0.1", self.upstream_port), timeout=2)
                upstream.settimeout(None)
            except OSError as error:
                self.record(conn=conn_id, event="upstream_refused", error=str(error))
                agent.close()
                continue
            conn = ProxyConn(self, conn_id, agent, upstream)
            self.conns[conn_id] = conn
            self.record(conn=conn_id, event="open")
            server_feed = Feed(conn.server_frames(), lambda e: conn.event("parse_error", side="server", error=e))
            agent_feed = Feed(conn.agent_stream(), lambda e: conn.event("parse_error", side="agent", error=e))
            threading.Thread(target=conn.pump, args=(agent, upstream, agent_feed, "agent"), daemon=True).start()
            threading.Thread(target=conn.pump, args=(upstream, agent, server_feed, "server"), daemon=True).start()

    def snapshot(self, since=0.0, **match):
        with self.lock:
            return [e for e in self.events if e["t"] >= since and all(e.get(k) == v for k, v in match.items())]

    def open_conns(self):
        with self.lock:
            opened = {e["conn"] for e in self.events if e["event"] == "open"}
            closed = {e["conn"] for e in self.events if e["event"] == "close"}
        return sorted(opened - closed)

    def stop(self):
        self.listener.close()
        for conn in list(self.conns.values()):
            conn.close("proxy")
        self.file.close()


# ---------------------------------------------------------------- 프로세스

class GoServer:
    def __init__(self, binary, temp, name, port, http_port, db):
        self.binary, self.temp, self.name = binary, temp, name
        self.port, self.http_port, self.db = port, http_port, db
        self.process = None
        self.starts = 0
        self.conf_text = ""  # 서버 conf 내용. 기동 때 쓰고, write_conf로 실행 중에 바꾼다(3초 reload)

    def start(self):
        self.starts += 1
        ready = self.temp / f"ready-{self.name}-{self.starts}.json"
        self.log_path = self.temp / f"server-{self.name}-{self.starts}.log"
        conf = self.temp / f"server-{self.name}.conf"
        conf.write_text(self.conf_text, encoding="utf-8")
        environment = os.environ.copy()
        environment["GOMAXPROCS"] = "2"
        with self.log_path.open("w") as out:
            self.process = subprocess.Popen(
                [str(self.binary), "--udp", f"127.0.0.1:{self.port}", "--tcp", f"127.0.0.1:{self.port}",
                 "--http", f"127.0.0.1:{self.http_port}", "--db", str(self.db), "--timezone", "Asia/Seoul",
                 "--user", "admin", "--password", "admin", "--ready-file", str(ready), "--conf", str(conf),
                 "--exit-file=false", "--log-dir", str(self.temp / f"logs-{self.name}")],
                cwd=self.temp, env=environment, stdout=out, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"Go server {self.name} exited before ready:\n" + self.log_path.read_text()[-4000:])
            if ready.is_file():
                try:
                    json.loads(ready.read_text())
                    self.ready_at = time.time()
                    return self.ready_at
                except json.JSONDecodeError:
                    pass
            time.sleep(0.02)
        raise RuntimeError(f"Go server {self.name} ready timeout")

    def stop(self):
        if self.process is None or self.process.poll() is not None:
            return None
        self.process.send_signal(signal.SIGTERM)
        try:
            self.process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
            raise RuntimeError(f"Go server {self.name} did not stop within 20s")
        self.stopped_at = time.time()
        return self.process.returncode

    def write_conf(self, text):
        """실행 중인 서버의 conf를 바꾼다. Go는 원본 Configure thread처럼 3초마다 다시 읽는다."""
        self.conf_text = text
        tmp = self.temp / f"server-{self.name}.conf.tmp"
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, self.temp / f"server-{self.name}.conf")

    def health(self):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.http_port}/health", timeout=5) as response:
            return json.loads(response.read())

    def agent_connections(self):
        try:
            return self.health()["tcp"]["AgentConnections"]
        except OSError:
            return None

    def logs(self):
        return "".join(p.read_text(errors="replace") for p in sorted(self.temp.glob(f"server-{self.name}-*.log")))


class Client:
    """tools/java-agent-interop/client/AgentInteropClient (원본 TcpProxy) line driver."""

    def __init__(self, java, classes, temp):
        self.out = (temp / "client.log").open("w")
        self.process = subprocess.Popen(
            [java, "-XX:ActiveProcessorCount=2", "-Xmx256m", "-Djava.awt.headless=true",
             "-Duser.timezone=Asia/Seoul", "-cp", str(classes), "AgentInteropClient"],
            cwd=temp, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.out, text=True, bufsize=1)
        self.next_id = 0

    def call(self, op, **kv):
        self.next_id += 1
        args = " ".join(f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in kv.items())
        self.process.stdin.write(f"{self.next_id} {op} {args}\n")
        self.process.stdin.flush()
        prefix = f"R {self.next_id} "
        while True:
            line = self.process.stdout.readline()
            if not line:
                raise RuntimeError(f"Client exited during {op}")
            if line.startswith(prefix):
                return json.loads(line[len(prefix):])
            self.out.write("stdout: " + line)

    def close(self):
        try:
            self.call("quit")
        except (RuntimeError, OSError, ValueError):
            pass
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
        self.out.close()


class Agent:
    def __init__(self, java, temp, agent_jar, hooks_jar, sample_classes, conf, extra_jvm):
        self.conf = conf
        self.stdout_path = temp / "agent-stdout.log"
        self.hook_log = temp / "agent-hook.log"
        self.log_dir = temp / "agent-logs"
        out = self.stdout_path.open("w")
        self.process = subprocess.Popen(
            [java, "-XX:ActiveProcessorCount=2", "-Xmx128m", "-Djava.awt.headless=true",
             f"-javaagent:{agent_jar}", f"-Dscouter.config={conf}", "-Dscouter.handler=zzinterop",
             f"-Xbootclasspath/a:{hooks_jar}", f"-Dinterop.hookLog={self.hook_log}",
             f"-Dinterop.marker={MARKER}", *extra_jvm, "-cp", str(sample_classes), "interop.SampleApp"],
            cwd=temp, env=agent_env(), stdin=subprocess.PIPE, stdout=out, stderr=subprocess.STDOUT)
        out.close()
        self.started_at = time.time()

    def write_conf(self, **values):
        return write_conf(self.conf, values)

    def hook_calls(self):
        if not self.hook_log.is_file():
            return []
        rows = []
        for line in self.hook_log.read_text().splitlines():
            t, call, mode, tag, phase = line.split(" ")
            rows.append({"t": int(t) / 1000.0, "call": int(call), "mode": mode, "tag": tag, "phase": phase})
        return rows

    def output(self):
        text = self.stdout_path.read_text(errors="replace")
        if self.log_dir.is_dir():
            text += "".join(p.read_text(errors="replace") for p in sorted(self.log_dir.glob("*")))
        return text

    def stop(self):
        if self.process.poll() is None:
            try:
                self.process.stdin.close()
            except OSError:
                pass
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)


def agent_env():
    """Agent 프로세스 환경. 실행 환경의 KUBERNETES_SERVICE_HOST가 비-kube Agent의 이름 규칙을 바꾸지 않게 뺀다."""
    environment = os.environ.copy()
    environment.pop("KUBERNETES_SERVICE_HOST", None)
    return environment


def write_conf(path, values):
    tmp = path.with_suffix(".tmp")
    tmp.write_text("".join(f"{k}={v}\n" for k, v in values.items()), encoding="utf-8")
    os.replace(tmp, path)
    # Configure.run은 3초마다 lastModified 변경을 본다.
    now = time.time()
    os.utime(path, (now, now))
    return now


# ---------------------------------------------------------------- 컴파일

def compile_all(source, temp, java, javac, jar_tool, agent_jar_source):
    report = {}
    # 1. 원본 Client 통신 클래스 + common (java-integration과 같은 guard)
    common = sorted((source / "scouter.common/src/main/java").rglob("*.java"))
    client = [source / "scouter.client/src/scouter/client" / name for name in JI.CLIENT_FILES]
    resource_root = source / "scouter.common/src/main/resources"
    resources = sorted(p for p in resource_root.rglob("*") if p.is_file())
    guard = JI.guard_sources(source, common + client + resources)
    report["source_commit"] = guard["source_commit"]
    report["source_version"] = guard["source_version"]
    report["original_client_sources"] = len(common) + len(client)
    classes = temp / "classes-client"
    classes.mkdir()
    adapters = [ROOT / "tools/java-integration/scouter/client/preferences/PManager.java",
                TOOLS / "client/AgentInteropClient.java"]
    argfile = temp / "client-sources.txt"
    argfile.write_text("\n".join(f'"{p}"' for p in common + client + adapters), encoding="utf-8")
    run([javac, "-J-XX:ActiveProcessorCount=2", "--release", "11", "-encoding", "UTF-8", "-sourcepath", "",
         "-cp", str(classes), "-d", str(classes), "@" + str(argfile)], cwd=temp)
    for resource in resources:
        destination = classes / resource.relative_to(resource_root)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(resource, destination)

    # 2. Agent jar: 임시 directory에 복사(agent_dir = jar 위치라 dump·plugin 기본 경로가 원본 밖에 생기지 않게)
    agent_dir = temp / "agent"
    agent_dir.mkdir()
    agent_jar = agent_dir / "scouter.agent.jar"
    shutil.copyfile(agent_jar_source, agent_jar)
    with zipfile.ZipFile(agent_jar) as z:
        manifest = z.read("META-INF/MANIFEST.MF").decode()
        pom = z.read("META-INF/maven/io.github.scouter-project/scouter-agent-java/pom.properties").decode()
    if "Premain-Class: scouter.agent.JavaAgent" not in manifest:
        raise RuntimeError("agent jar has no scouter.agent.JavaAgent premain")
    version = next((line.split("=", 1)[1] for line in pom.splitlines() if line.startswith("version=")), "")
    report["agent_jar"] = {"sha256": sha256(agent_jar), "bytes": agent_jar.stat().st_size, "version": version}
    tag = f"v{version}"
    try:
        diff = run(["git", "-C", str(source), "diff", "--stat", tag, guard["source_commit"], "--", *WIRE_PATHS])
        report["agent_wire_diff_vs_baseline"] = {"tag": tag, "paths": WIRE_PATHS, "stat": diff.strip()}
        if diff.strip():
            raise RuntimeError(f"agent {tag} wire/network sources differ from baseline; prebuilt jar is not a stand-in:\n{diff}")
    except RuntimeError as error:
        if "differ" in str(error):
            raise
        report["agent_wire_diff_vs_baseline"] = {"tag": tag, "error": "tag not found; unverified"}

    # 3. 샘플 앱과 test 전용 handler
    sample = temp / "classes-sample"
    sample.mkdir()
    run([javac, "--release", "8", "-encoding", "UTF-8", "-d", str(sample),
         *map(str, sorted((TOOLS / "sample").rglob("*.java")))], cwd=temp)
    hooks = temp / "classes-hooks"
    hooks.mkdir()
    run([javac, "--release", "8", "-encoding", "UTF-8", "-cp", str(agent_jar), "-d", str(hooks),
         *map(str, sorted((TOOLS / "hooks").rglob("*.java")))], cwd=temp)
    hooks_jar = temp / "interop-hooks.jar"
    run([jar_tool, "cf", str(hooks_jar), "-C", str(hooks), "."], cwd=temp)
    report["harness_sources"] = {p.relative_to(ROOT).as_posix(): sha256(p)
                                 for p in sorted(TOOLS.rglob("*.java")) + [Path(__file__).resolve()]}
    log(f"compiled: client {report['original_client_sources']} original sources, agent {version}, sample, hooks")
    return classes, agent_jar, hooks_jar, sample, report


# ---------------------------------------------------------------- 검증 도구

class Checks:
    def __init__(self):
        self.rows = []

    def add(self, item, name, ok, **evidence):
        row = {"item": item, "name": name, "status": "pass" if ok else "fail", **evidence}
        self.rows.append(row)
        log(("PASS " if ok else "FAIL ") + f"[{item}] {name} {json.dumps(evidence, ensure_ascii=False)[:600]}")
        return ok


def eventually(predicate, timeout, interval=0.5):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() >= deadline:
            return value
        time.sleep(interval)


def find_object(client):
    objects = client.call("objects").get("objects", [])
    for o in objects:
        if o["name"] == OBJ_NAME:
            return o
    return None


def agent_conf(port_udp, port_tcp, temp):
    return {
        "obj_name": "interop-sample", "obj_host_name": "interop-host",
        "net_collector_ip": "127.0.0.1", "net_collector_udp_port": port_udp, "net_collector_tcp_port": port_tcp,
        "hook_service_patterns": "interop.SampleService.handle",
        "hook_method_patterns": "interop.SampleService.*",
        "log_dir": temp / "agent-logs", "dump_dir": temp / "agent-dump", "plugin_dir": temp / "agent-plugin",
        # <pid>.scouter(objName) 등록 위치. 기본 /tmp/scouter는 이 장비의 다른 Agent와 공유되므로 임시 directory를 쓴다.
        "counter_object_registry_path": temp / "obj-registry",
    }


# ---------------------------------------------------------------- 시나리오

def scenario_collect(ctx, checks):
    client, server = ctx["client"], ctx["server_a"]
    obj = eventually(lambda: find_object(client), 60)
    if not checks.add(1, "object heartbeat registered", bool(obj and obj["alive"]), object=obj):
        raise RuntimeError("agent object never appeared; later scenarios need it")
    ctx["obj_hash"] = obj["hash"]
    h = obj["hash"]
    checks.add(1, "object type detected by agent", obj["type"] == "java", type=obj["type"])
    def enough_xlogs():
        found = client.call("xlogs", objHash=h)["xlogs"]
        return found if len(found) >= 10 and any(x["error"] for x in found) and any(x["elapsed"] >= 2400 for x in found) else None
    xlogs = eventually(enough_xlogs, 40) or client.call("xlogs", objHash=h)["xlogs"]
    errors = [x for x in xlogs if x["error"]]
    slow = [x for x in xlogs if x["elapsed"] >= 2400]
    checks.add(1, "XLog stored and read by realtime query", len(xlogs) >= 10 and bool(errors) and bool(slow),
               xlogs=len(xlogs), errors=len(errors), slow=len(slow))
    service = client.call("text", type="service", hash=xlogs[0]["service"]) if xlogs else {}
    checks.add(1, "service text dictionary", bool(service.get("found")) and "SampleService" in (service.get("text") or ""),
               found=service.get("found"), text=service.get("text"))
    if errors:
        error = client.call("text", type="error", hash=errors[0]["error"])
        checks.add(1, "error text dictionary", bool(error.get("found")) and "interop synthetic error" in (error.get("text") or ""),
                   found=error.get("found"), text=error.get("text"))
    fast = [x for x in xlogs if not x["error"] and x["elapsed"] < 2400]
    profile = client.call("profile", txid=fast[0]["txid"]) if fast else {}
    method_ok = False
    if profile.get("methodHashes"):
        method = client.call("text", type="method", hash=profile["methodHashes"][0])
        method_ok = bool(method.get("found"))
        profile["methodText"] = method.get("text")
    checks.add(1, "profile stored, decoded by original Step decoder, method text", profile.get("steps", 0) >= 2 and method_ok,
               **{k: profile.get(k) for k in ("packs", "steps", "stepTypes", "methodText")})
    counters = {}
    for name in ("TPS", "ElapsedTime", "HeapUsed", "ActiveService"):
        counters[name] = eventually(lambda: client.call("counter", objHash=h, name=name).get("present") and
                                    client.call("counter", objHash=h, name=name), 20)
    checks.add(1, "realtime counters", all(counters.values()),
               counters={k: (v or {}).get("valueType") for k, v in counters.items()})
    health = server.health()
    checks.add(1, "Go ingest without parse/reject errors",
               health["udp"]["Malformed"] == 0 and health["udp"]["Rejected"] == 0 and health["ingest"]["Rejected"] == 0,
               udp={k: health["udp"][k] for k in ("Packets", "Datagrams", "Malformed", "Rejected")},
               ingest={k: health["ingest"].get(k) for k in ("Completed", "XLogs", "Rejected", "StorageErrors")})
    magic = ctx["proxy"].snapshot(event="magic")
    checks.add(1, "agent TCP is V2 with the object hash", bool(magic) and magic[0]["magic"] == "0xcafe1002"
               and magic[0]["objHash"] == h, magic=magic[:1])


def relay(ctx, command, **kv):
    return ctx["client"].call("relay", cmd=command, objHash=ctx["obj_hash"], **kv)


def scenario_relay(ctx, checks):
    proxy = ctx["proxy"]
    since = time.time()
    r = relay(ctx, "OBJECT_THREAD_LIST", expectWorker="interop-worker-0", expectIdle="interop-idle-99")
    keys = r["packs"][0]["keys"] if r.get("packs") else {}
    checks.add(2, "OBJECT_THREAD_LIST", r["valid"] and r["count"] == 1 and r["expectWorker"] and r["expectIdle"],
               count=r["count"], id=keys.get("id"), elapsedMs=r["elapsedMs"])
    r = relay(ctx, "OBJECT_ENV", expectMarker=f"interop.marker={MARKER}")
    checks.add(2, "OBJECT_ENV (marker only, values not recorded)", r["valid"] and r["count"] == 1 and r["expectMarker"],
               count=r["count"], keys=len(r["packs"][0]["keys"]) if r.get("packs") else 0)
    active = None
    for _ in range(10):
        r = relay(ctx, "OBJECT_ACTIVE_SERVICE_LIST", expectService="SampleService")
        if r["valid"] and r["count"] == 1 and r["expectService"]:
            active = r
            break
        time.sleep(0.4)
    checks.add(2, "OBJECT_ACTIVE_SERVICE_LIST shows the slow service", active is not None,
               keys=(active or r)["packs"][0]["keys"] if (active or r).get("packs") else None)
    r = relay(ctx, "OBJECT_THREAD_DUMP", expectIdle="interop-idle-", expectWorker="interop-worker-")
    pack = r["packs"][0] if r.get("packs") else {}
    checks.add(2, "OBJECT_THREAD_DUMP", r["valid"] and r["count"] == 1 and r["expectIdle"] and r["expectWorker"],
               count=r["count"], textChars=r.get("textChars"), keys=pack.get("keys"), error=pack.get("errorText"),
               elapsedMs=r["elapsedMs"])
    r = relay(ctx, "OBJECT_HEAPHISTO", expectString="java.lang.String")
    pack = r["packs"][0] if r.get("packs") else {}
    checks.add(2, "OBJECT_HEAPHISTO", r["valid"] and r["count"] == 1 and r["expectString"],
               count=r["count"], textChars=r.get("textChars"), keys=pack.get("keys"), error=pack.get("errorText"),
               elapsedMs=r["elapsedMs"])
    big = ctx["dump_file"]
    r = relay(ctx, "OBJECT_DUMP_FILE_LIST", expectName=big.name)
    checks.add(2, "OBJECT_DUMP_FILE_LIST", r["valid"] and r["count"] == 1 and r["expectName"], count=r["count"])
    r = relay(ctx, "OBJECT_DUMP_FILE_DETAIL", blob=1, **{"p.name": big.name})
    checks.add(2, "OBJECT_DUMP_FILE_DETAIL blob stream (byte-equal)", r["valid"] and r["blobBytes"] == big.stat().st_size
               and r["blobSha256"] == sha256(big), chunks=r["count"], bytes=r["blobBytes"], elapsedMs=r["elapsedMs"])
    r = relay(ctx, "GET_CONFIGURE_WAS", expectConf="obj_name=interop-sample")
    checks.add(2, "GET_CONFIGURE_WAS", r["valid"] and r["count"] == 1 and r["expectConf"], count=r["count"])
    r = relay(ctx, "LIST_CONFIGURE_WAS")
    checks.add(2, "LIST_CONFIGURE_WAS", r["valid"] and r["count"] == 1, count=r["count"],
               keys=r["packs"][0]["keys"] if r.get("packs") else None)
    r = relay(ctx, "OBJECT_CLASS_LIST", expectClass="interop.SampleService")
    checks.add(2, "OBJECT_CLASS_LIST", r["valid"] and r["count"] == 1, count=r["count"], expectClass=r.get("expectClass"),
               keys=r["packs"][0]["keys"] if r.get("packs") else None)
    before = ctx["agent"].output().count("A127")
    r = relay(ctx, "OBJECT_SYSTEM_GC")
    gc_logged = eventually(lambda: ctx["agent"].output().count("A127") > before, 5, 0.2)
    checks.add(2, "OBJECT_SYSTEM_GC (management relay, result discarded like Java)", r["valid"] and r["count"] == 0 and gc_logged,
               count=r["count"], agentLogA127=bool(gc_logged))
    time.sleep(0.5)
    seen = [e["cmd"] for e in proxy.snapshot(since, event="command") if e["cmd"] != "KEEP_ALIVE"]
    answers = [e for e in proxy.snapshot(since, event="answer") if e["cmd"] != "KEEP_ALIVE"]
    expected = ["OBJECT_THREAD_LIST", "OBJECT_ENV", "OBJECT_THREAD_DUMP", "OBJECT_HEAPHISTO", "OBJECT_DUMP_FILE_LIST",
                "OBJECT_DUMP_FILE_DETAIL", "GET_CONFIGURE_WAS", "LIST_CONFIGURE_WAS", "OBJECT_CLASS_LIST", "OBJECT_SYSTEM_GC"]
    checks.add(2, "proxy saw each relay once and a complete agent answer", all(c in seen for c in expected)
               and len(answers) == len(seen) and not proxy.snapshot(since, event="parse_error"),
               commands=seen, answers=[(a["cmd"], a["items"], a["bytes"]) for a in answers])


def single_conn(ctx):
    conns = ctx["proxy"].open_conns()
    return conns[0] if len(conns) == 1 else None


def hook(ctx, tag, phase="end"):
    return [c for c in ctx["agent"].hook_calls() if c["tag"] == tag and c["phase"] == phase]


def scenario_orphan(ctx, checks):
    server, proxy = ctx["server_a"], ctx["proxy"]
    conn = single_conn(ctx)
    accepted = server.health()["tcp"]["Accepted"]

    # 3a. Client가 자기 so_timeout(2초)으로 먼저 떠나고 Agent는 6초 뒤 답한다.
    r = relay(ctx, "OBJECT_CHECK_RESOURCE_FILE", soTimeout=2000, **{"p.interopMode": "sleep", "i.sleepMs": 6000,
                                                                   "p.interopTag": "early-leave"})
    ended = eventually(lambda: hook(ctx, "early-leave"), 10, 0.2)
    time.sleep(1.0)
    after = relay(ctx, "OBJECT_CHECK_RESOURCE_FILE", **{"p.interopMode": "sleep", "i.sleepMs": 0, "p.interopTag": "after-early"})
    checks.add(3, "Client left at its so_timeout; agent answered later; same agent session serves the next command",
               not r["valid"] and 1500 <= r["elapsedMs"] < 4000 and bool(ended) and single_conn(ctx) == conn
               and after["valid"] and after["count"] == 1 and after["packs"][0].get("interopTag") == "after-early",
               clientElapsedMs=r["elapsedMs"], clientValid=r["valid"], agentEnded=bool(ended), conn=conn,
               connAfter=single_conn(ctx), nextCall=after["packs"][0].get("interopCall") if after.get("packs") else None)

    # 3b. Agent 응답(40초)이 Go RequestTimeout(30초)보다 늦지만 Client so_timeout(60초) 안이다. Go는 Agent를 기다리는
    #     동안 Client 무진전 감시를 보류하므로 원본처럼 Client가 40초에 응답을 받고 고아는 생기지 않는다.
    #     valid는 응답 뒤에도 Client socket이 열려 있다는 뜻이다(서버가 Client 연결을 끝내지 않음).
    orphans0 = server.health()["agentOrphans"]
    r = relay(ctx, "OBJECT_CHECK_RESOURCE_FILE", soTimeout=60000, **{"p.interopMode": "sleep", "i.sleepMs": 40000,
                                                                    "p.interopTag": "slow40"})
    orphans1 = server.health()["agentOrphans"]
    tag = r["packs"][0].get("interopTag") if r.get("packs") else None
    checks.add(3, "relay slower than RequestTimeout (30s) within the Client so_timeout (60s): Client gets the 40s answer, "
                  "no orphan, Client connection and agent session kept",
               r["valid"] and r["count"] == 1 and tag == "slow40" and 38000 <= r["elapsedMs"] <= 45000
               and orphans1["Orphaned"] == orphans0["Orphaned"] and orphans1["Active"] == 0 and single_conn(ctx) == conn,
               clientElapsedMs=r["elapsedMs"], clientValid=r["valid"], clientCount=r["count"], tag=tag,
               orphansBefore=orphans0, orphansAfter=orphans1, conn=conn, connAfter=single_conn(ctx))

    # 3c. Client가 자기 so_timeout(8초, 기본값)으로 먼저 떠나고 Agent는 40초에 답한다. Go는 Agent 대기 중 Client socket을
    #     probe해 이탈을 바로 감지하고 relay를 고아로 넘긴다. 고아는 응답을 끝까지 읽고 세션을 유지하며,
    #     그 사이 다른 Client 연결의 같은 Agent 재요청은 원본 busy 세션처럼 빈 응답을 받는다.
    orphans0 = server.health()["agentOrphans"]
    calls0 = len([c for c in ctx["agent"].hook_calls() if c["phase"] == "start"])
    started = time.time()
    r = relay(ctx, "OBJECT_CHECK_RESOURCE_FILE", soTimeout=8000, **{"p.interopMode": "sleep", "i.sleepMs": 40000,
                                                                   "p.interopTag": "orphan"})
    left = time.time()

    def orphaned_now():
        current = server.health()["agentOrphans"]
        return current if current["Orphaned"] > orphans0["Orphaned"] else None
    orphaned = eventually(orphaned_now, 3, 0.05)
    detected_sec = round(time.time() - left, 2) if orphaned else None
    during = relay(ctx, "OBJECT_CHECK_RESOURCE_FILE", **{"p.interopMode": "sleep", "i.sleepMs": 0, "p.interopTag": "during-orphan"})
    during_at = time.time() - started
    ended = eventually(lambda: hook(ctx, "orphan"), 40, 0.2)
    time.sleep(1.5)
    orphans1 = server.health()["agentOrphans"]
    after = relay(ctx, "OBJECT_CHECK_RESOURCE_FILE", **{"p.interopMode": "sleep", "i.sleepMs": 0, "p.interopTag": "after-orphan"})
    calls = [c for c in ctx["agent"].hook_calls() if c["phase"] == "start"]
    checks.add(3, "Client leaves at its so_timeout (8s) while the agent works (40s): Go sees it at once, "
                  "the orphan reads the answer, session kept",
               not r["valid"] and 7000 <= r["elapsedMs"] <= 10000 and bool(orphaned) and detected_sec <= 2.0
               and orphaned["Active"] == 1 and bool(ended)
               and orphans1["Orphaned"] == orphans0["Orphaned"] + 1 and orphans1["Completed"] == orphans0["Completed"] + 1
               and orphans1["Failed"] == orphans0["Failed"] and orphans1["Active"] == 0
               and single_conn(ctx) == conn and after["valid"] and after["count"] == 1,
               clientElapsedMs=r["elapsedMs"], clientValid=r["valid"], orphanSeenSecAfterClientLeft=detected_sec,
               orphansBefore=orphans0, orphansMid=orphaned, orphansAfter=orphans1,
               agentAnsweredSec=round(ended[0]["t"] - started, 2) if ended else None, conn=conn, connAfter=single_conn(ctx))
    checks.add(3, "request during the orphan gets Java's empty answer and is not sent to the agent",
               during["valid"] and during["count"] == 0 and not hook(ctx, "during-orphan", "start")
               and len(calls) == calls0 + 2,
               duringAtSec=round(during_at, 1), duringElapsedMs=during["elapsedMs"], duringCount=during["count"],
               hookStartsAdded=len(calls) - calls0)

    # 3d. Agent socket이 요청 중에 끊긴다(단일 대상 relay의 Java null).
    before_conns = {e["conn"] for e in proxy.snapshot(event="open")}
    t0 = time.time()
    r = relay(ctx, "OBJECT_CHECK_RESOURCE_FILE", **{"p.interopMode": "close", "p.interopTag": "close"})
    reconnected = eventually(lambda: [e for e in proxy.snapshot(t0, event="open") if e["conn"] not in before_conns], 10, 0.1)
    reconnect_s = round(reconnected[0]["t"] - t0, 2) if reconnected else None
    agents = eventually(lambda: server.agent_connections() == 1, 5, 0.1)
    after = relay(ctx, "OBJECT_CHECK_RESOURCE_FILE", **{"p.interopMode": "sleep", "i.sleepMs": 0, "p.interopTag": "after-close"})
    skipped = "skipped objHash" in server.logs()
    checks.add(3, "agent socket lost mid-request: Client gets the empty answer, agent reconnects",
               r["valid"] and r["count"] == 0 and bool(reconnected) and bool(agents) and after["valid"] and after["count"] == 1,
               clientValid=r["valid"], clientCount=r["count"], clientElapsedMs=r["elapsedMs"], reconnectSec=reconnect_s,
               goLogSkipped=skipped)

    # 3e. 일반 relay 응답 상한(8MiB): 7MiB 통과, 9MiB는 거절·세션 종료·재연결.
    r = relay(ctx, "OBJECT_CHECK_RESOURCE_FILE", **{"p.interopMode": "big", "i.bytes": 7 << 20, "p.interopTag": "big7"})
    checks.add(3, "7MiB agent answer relayed", r["valid"] and r["count"] == 1 and r["blobBytes"] == 7 << 20,
               bytes=r.get("blobBytes"), elapsedMs=r["elapsedMs"])
    before_conns = {e["conn"] for e in proxy.snapshot(event="open")}
    t0 = time.time()
    r = relay(ctx, "OBJECT_CHECK_RESOURCE_FILE", **{"p.interopMode": "big", "i.bytes": 9 << 20, "p.interopTag": "big9"})
    reconnected = eventually(lambda: [e for e in proxy.snapshot(t0, event="open") if e["conn"] not in before_conns], 10, 0.1)
    after = relay(ctx, "OBJECT_CHECK_RESOURCE_FILE", **{"p.interopMode": "sleep", "i.sleepMs": 0, "p.interopTag": "after-big9"})
    refused = "response over the Go relay limit" in server.logs()
    checks.add(3, "9MiB answer refused by the 8MiB relay limit: Client fails, agent reconnects, next command works",
               not r["valid"] and refused and bool(reconnected) and after["valid"] and after["count"] == 1,
               clientValid=r["valid"], goLogRefused=refused,
               reconnectSec=round(reconnected[0]["t"] - t0, 2) if reconnected else None)
    ctx["accepted_before_orphan"] = accepted


def scenario_keepalive(ctx, checks, idle_seconds):
    proxy = ctx["proxy"]
    conn = single_conn(ctx)
    t0 = time.time()
    log(f"idle {idle_seconds}s on agent connection {conn} for KEEP_ALIVE")
    time.sleep(idle_seconds)
    commands = proxy.snapshot(t0, conn=conn, event="command")
    answers = proxy.snapshot(t0, conn=conn, event="answer")
    keepalive = [e["t"] for e in commands if e["cmd"] == "KEEP_ALIVE"]
    gaps = [round(b - a, 2) for a, b in zip(keepalive, keepalive[1:])]
    latency = []
    for c in commands:
        a = next((x for x in answers if x["t"] >= c["t"] and x["cmd"] == c["cmd"]), None)
        if a:
            latency.append(round((a["t"] - c["t"]) * 1000, 1))
    other = sorted({e["cmd"] for e in commands if e["cmd"] != "KEEP_ALIVE"})
    checks.add(4, "KEEP_ALIVE every 5-10s on an idle session, answered with NoNEXT only",
               len(gaps) >= 3 and all(4.5 <= g <= 10.6 for g in gaps) and all(a["items"] == 0 for a in answers
                                                                            if a["cmd"] == "KEEP_ALIVE"),
               keepalives=len(keepalive), gapsSec=gaps, answerLatencyMs=(min(latency), max(latency)) if latency else None,
               otherCommands=other)
    checks.add(4, f"agent session survives {idle_seconds}s idle (> agent so_timeout 60s) without reconnect",
               single_conn(ctx) == conn and not proxy.snapshot(t0, event="open"), conn=conn, connAfter=single_conn(ctx))


def scenario_restart_text_reset(ctx, checks):
    """새 빈 DB로 재기동: reset 전에는 새 XLog의 service text가 없고, OBJECT_RESET_CACHE 뒤에 다시 온다."""
    server, proxy, client = ctx["server_a"], ctx["proxy"], ctx["client"]
    first_reset = proxy.snapshot(event="command", cmd="OBJECT_RESET_CACHE")
    delay = first_reset[0]["t"] - ctx["server_a_ready"] if first_reset else None
    # 30초 pass에서 세션이 다른 relay(고아 relay 등)로 busy였으면 Go는 같은 날짜 안에서 다시 보낸다(의도된 차이).
    busy_retry = "retry pending 1" in server.logs()
    checks.add(4, "first OBJECT_RESET_CACHE 30s after start, or Go's same-day retry if the session was busy then",
               delay is not None and (29.0 <= delay <= 34.0 or (busy_retry and delay > 34.0)),
               delaySec=round(delay, 2) if delay is not None else None, busyAt30sRetried=busy_retry)
    ctx["server_a_first_health"] = {k: server.health()[k] for k in ("tcp", "udp", "ingest", "agentOrphans")}
    code = server.stop()
    stopped = time.time()
    checks.add(4, "Go graceful stop (SIGTERM) with the agent attached", code == 0, exitCode=code)
    time.sleep(5)
    server.db = ctx["temp"] / "db-a2"
    ready = server.start()
    ctx["server_a_ready"] = ready
    conn = eventually(lambda: [e for e in proxy.snapshot(ready, event="open")], 20, 0.05)
    checks.add(4, "agent reconnects after restart (through the proxy: refused upstream looks like EOF to the agent)",
               bool(conn), downSec=round(ready - stopped, 1), reconnectSec=round(conn[0]["t"] - ready, 2) if conn else None)
    client.call("login", port=server.port)
    h = ctx["obj_hash"]
    obj = eventually(lambda: find_object(client), 20)
    # reset 전(재기동 후 ~10-25초): 새 XLog의 service text가 새 DB에 없다.
    time.sleep(max(0.0, ready + 12 - time.time()))
    xlogs = client.call("xlogs", objHash=h)["xlogs"]
    services = sorted({x["service"] for x in xlogs})
    before = {s: client.call("text", type="service", hash=s).get("found") for s in services}
    resets_before = proxy.snapshot(ready, event="command", cmd="OBJECT_RESET_CACHE")
    checks.add(4, "before the reset the agent does not resend texts lost while the server was down",
               bool(obj) and bool(xlogs) and not any(before.values()) and not resets_before,
               xlogs=len(xlogs), serviceHashes=len(services), found=sum(bool(v) for v in before.values()),
               secondsAfterReady=round(time.time() - ready, 1))
    reset = eventually(lambda: proxy.snapshot(ready, event="command", cmd="OBJECT_RESET_CACHE"), 30, 0.2)
    answered = eventually(lambda: proxy.snapshot(ready, event="answer", cmd="OBJECT_RESET_CACHE"), 5, 0.2)
    found = eventually(lambda: all(client.call("text", type="service", hash=s).get("found") for s in services), 20, 1)
    checks.add(4, "OBJECT_RESET_CACHE ~30s after restart; agent answers and resends the service text",
               bool(reset) and bool(answered) and bool(found) and 29.0 <= reset[0]["t"] - ready <= 34.0,
               resetSec=round(reset[0]["t"] - ready, 2) if reset else None,
               answerItems=answered[0]["items"] if answered else None, textsFound=bool(found))
    reset_log = [line for line in server.logs().splitlines() if "text cache reset" in line]
    checks.add(4, "Go text cache reset log reports the agent as called", any("called 1" in l for l in reset_log),
               lines=reset_log[-2:])


def scenario_agent_so_timeout(ctx, checks):
    """3f. Agent 응답(40초)이 Go의 Agent so_timeout보다 늦다. 원본 TcpAgentWorker처럼 Agent 세션을 닫고 Client에는
    빈 응답(NoNEXT)을 보내며 Client 연결은 유지한다(고아 아님). so_timeout은 Agent 등록 때 읽으므로 conf를
    15초로 바꾼 뒤 socket 끊김(close)으로 재등록시키고, 응답 뒤 기본값으로 되돌려 다음 등록은 60초가 되게 한다.
    시나리오 3 안에서 돌리면 기동 30초 뒤 첫 OBJECT_RESET_CACHE의 재시도가 이 세션 공백까지 겹쳐 마지막
    재시도(약 190초)로 밀렸다. 그래서 4b 재기동(그 reset이 끝난 뒤)으로 옮겼다."""
    server, proxy = ctx["server_a"], ctx["proxy"]
    server.write_conf("net_tcp_agent_so_timeout_ms=15000\n")
    time.sleep(4)  # Go conf reload 주기 3초
    before_conns = {e["conn"] for e in proxy.snapshot(event="open")}
    t0 = time.time()
    relay(ctx, "OBJECT_CHECK_RESOURCE_FILE", **{"p.interopMode": "close", "p.interopTag": "reregister"})
    eventually(lambda: [e for e in proxy.snapshot(t0, event="open") if e["conn"] not in before_conns], 10, 0.1)
    eventually(lambda: server.agent_connections() == 1 and single_conn(ctx) not in (None, *before_conns), 5, 0.1)
    short = single_conn(ctx)
    orphans0 = server.health()["agentOrphans"]
    t0 = time.time()
    r = relay(ctx, "OBJECT_CHECK_RESOURCE_FILE", soTimeout=60000, **{"p.interopMode": "sleep", "i.sleepMs": 40000,
                                                                    "p.interopTag": "agent-so"})
    closed = eventually(lambda: proxy.snapshot(t0, event="close", conn=short), 3, 0.1)
    server.write_conf("")
    ended = eventually(lambda: hook(ctx, "agent-so"), 30, 0.2)
    rejoined = eventually(lambda: server.agent_connections() == 1 and single_conn(ctx) not in (None, short), 15, 0.2)
    after = relay(ctx, "OBJECT_CHECK_RESOURCE_FILE", **{"p.interopMode": "sleep", "i.sleepMs": 0, "p.interopTag": "after-agent-so"})
    orphans1 = server.health()["agentOrphans"]
    answers = [e for e in proxy.snapshot(t0, event="answer", conn=short) if e["cmd"] == "OBJECT_CHECK_RESOURCE_FILE"]
    checks.add(3, "agent slower than the Go agent so_timeout (15s): Client gets Java's empty answer at 15s and keeps its "
                  "connection, Go closes the agent session, no orphan, agent comes back after its handler",
               r["valid"] and r["count"] == 0 and 14000 <= r["elapsedMs"] <= 18000 and bool(closed)
               and closed[0].get("by") == "server" and 14.0 <= closed[0]["t"] - t0 <= 18.0 and not answers
               and orphans1["Orphaned"] == orphans0["Orphaned"] and bool(ended) and bool(rejoined)
               and after["valid"] and after["count"] == 1,
               clientElapsedMs=r["elapsedMs"], clientValid=r["valid"], clientCount=r["count"], session=short,
               sessionClosedSec=round(closed[0]["t"] - t0, 2) if closed else None,
               closedBy=closed[0].get("by") if closed else None,
               agentAnsweredSec=round(ended[0]["t"] - t0, 2) if ended else None,
               rejoinedConn=single_conn(ctx), orphansBefore=orphans0, orphansAfter=orphans1)


def scenario_move(ctx, checks):
    agent, proxy = ctx["agent"], ctx["proxy"]
    a, temp = ctx["server_a"], ctx["temp"]
    # 5a. 같은 서버의 다른 TCP 주소(proxy → 직접).
    accepted = a.health()["tcp"]["Accepted"]
    conn = single_conn(ctx)
    written = agent.write_conf(**agent_conf(a.port, a.port, temp))
    closed = eventually(lambda: proxy.snapshot(written, conn=conn, event="close"), 25, 0.1)
    direct = eventually(lambda: a.health()["tcp"]["Accepted"] > accepted and a.agent_connections() == 1
                        and not proxy.open_conns() and time.time(), 25, 0.1)
    last_cmd = [e for e in proxy.snapshot(written, conn=conn) if e["event"] in ("command", "answer")]
    checks.add(5, "conf tcp port change moves the V2 session (agent closes it after the next server command)",
               bool(closed) and closed[0]["by"] == "agent" and bool(direct),
               closeSec=round(closed[0]["t"] - written, 2) if closed else None,
               directSec=round(direct - written, 2) if direct else None,
               lastFramesOnOldSession=[(e["event"], e["cmd"]) for e in last_cmd][-2:])
    # 5b. 새 서버 B로 UDP·TCP 모두 이동.
    b = GoServer(ctx["binary"], temp, "b", free_port(), free_port(), temp / "db-b")
    ctx["server_b"] = b
    b.start()
    a_xlogs = a.health()["ingest"]["XLogs"]
    written = agent.write_conf(**agent_conf(b.port, b.port, temp))
    moved = eventually(lambda: b.agent_connections() == 1 and a.agent_connections() == 0 and time.time(), 30, 0.1)
    client = ctx["client"]
    client.call("login", port=b.port)
    obj = eventually(lambda: find_object(client), 20)
    xlogs = eventually(lambda: client.call("xlogs", objHash=ctx["obj_hash"])["count"] >= 3, 20)
    time.sleep(2)
    a_after = a.health()["ingest"]["XLogs"]
    time.sleep(3)
    a_later = a.health()["ingest"]["XLogs"]
    r = relay(ctx, "OBJECT_THREAD_LIST", expectWorker="interop-worker-0")
    checks.add(5, "conf collector change to a new server moves TCP session, UDP data and relays",
               bool(moved) and bool(obj) and bool(xlogs) and a_later == a_after and r["valid"] and r["expectWorker"],
               moveSec=round(moved - written, 2) if moved else None, objectOnB=bool(obj), xlogsOnB=bool(xlogs),
               oldServerXLogsAfterMove=a_later - a_after, oldServerXLogsAtSwitch=a_after - a_xlogs)


def scenario_direct_restart(ctx, checks, gaps):
    b, client = ctx["server_b"], ctx["client"]
    results = []
    for gap in gaps:
        code = b.stop()
        stopped = time.time()
        time.sleep(gap)
        ready = b.start()
        connected = eventually(lambda: b.agent_connections() == 1 and time.time(), 20, 0.05)
        results.append({"downSec": round(ready - stopped, 1), "exit": code,
                        "reconnectSec": round(connected - ready, 2) if connected else None})
    client.call("login", port=b.port)
    t = time.time()
    flowing = eventually(lambda: b.health()["ingest"]["XLogs"] > 0, 15)
    obj = eventually(lambda: (find_object(client) or {}).get("alive") and find_object(client), 15, 0.2)
    alive_sec = round(time.time() - ready, 1)
    checks.add(4, "agent reconnects directly after server restart (connect refused -> 3s+1s retry)",
               all(r["reconnectSec"] is not None and r["reconnectSec"] <= 6.0 and r["exit"] == 0 for r in results)
               and bool(flowing) and bool(obj and obj["alive"]), restarts=results,
               xlogsFlowSec=round(time.time() - t, 1), objectAlive=bool(obj and obj["alive"]),
               objectAliveAfterReadySec=alive_sec)


class HostAgent:
    """원본 Host Agent 배포 jar(scouter.boot.Boot)를 임시 directory에서 실행한다(V1 TCP)."""

    def __init__(self, java, temp, host_jar, conf, name="host-agent", env=None):
        self.conf = conf
        home = temp / name
        home.mkdir()
        shutil.copyfile(host_jar, home / "scouter.host.jar")
        self.stdout_path = temp / f"{name}-stdout.log"
        self.log_dir = temp / f"{name}-logs"
        environment = agent_env()
        environment.update(env or {})
        out = self.stdout_path.open("w")
        self.process = subprocess.Popen(
            [java, "-XX:ActiveProcessorCount=2", "-Xmx64m", f"-Dscouter.config={conf}", f"-Dinterop.marker={MARKER}",
             "-classpath", "./scouter.host.jar", "scouter.boot.Boot", "./lib"],
            cwd=home, env=environment, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT)
        out.close()

    def output(self):
        text = self.stdout_path.read_text(errors="replace")
        if self.log_dir.is_dir():
            text += "".join(p.read_text(errors="replace") for p in sorted(self.log_dir.glob("*")))
        return text

    def stop(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)


def host_conf(port, temp):
    return {"obj_name": "interop-host-agent", "net_collector_ip": "127.0.0.1", "net_collector_udp_port": port,
            "net_collector_tcp_port": port, "log_dir": temp / "host-agent-logs",
            # 샘플 JVM의 등록 파일을 읽지 않게(ProcCpu는 시나리오 7의 kube Host Agent만 보낸다) 따로 둔다.
            "counter_object_registry_path": temp / "host-registry"}


def kube_conf(port, temp):
    """obj_name 없이 kube 이름 규칙을 쓰는 Host Agent. 샘플 JVM과 등록 directory를 공유해 ProcCpu도 보낸다."""
    return {"net_collector_ip": "127.0.0.1", "net_collector_udp_port": port, "net_collector_tcp_port": port,
            "log_dir": temp / "kube-agent-logs", "test_host_name": KUBE_HOST_NAME,
            "counter_object_registry_path": temp / "obj-registry", "_cpu_value_avg_sec": 2}


def scenario_host_agent(ctx, checks):
    """Host Agent(V1): 수집·relay, conf 변경 시 UDP만 옮겨 가고 TCP는 기존 서버가 끊길 때까지 남는다(runbook §4)."""
    a, b, client, temp = ctx["server_a"], ctx["server_b"], ctx["client"], ctx["temp"]
    conf = temp / "host-agent.conf"
    write_conf(conf, host_conf(b.port, temp))
    java_agents = b.agent_connections()
    host = HostAgent(ctx["agent_java"], temp, ctx["host_jar"], conf)
    ctx["host_agent"] = host
    client.call("login", port=b.port)
    name = "/interop-host-agent"
    obj = eventually(lambda: next((o for o in client.call("objects")["objects"] if o["name"] == name), None), 40)
    if not checks.add(6, "Host Agent object heartbeat", bool(obj and obj["alive"]), object=obj):
        return
    h = obj["hash"]
    cpu = eventually(lambda: client.call("counter", objHash=h, name="Cpu").get("present") and
                     client.call("counter", objHash=h, name="Cpu"), 30)
    connected = eventually(lambda: b.agent_connections() == java_agents + 1, 10, 0.2)
    checks.add(6, "Host Agent counter Cpu and V1 TCP session", bool(cpu) and bool(connected),
               valueType=(cpu or {}).get("valueType"), agentConnections=b.agent_connections())
    results = {}
    for command, extra in (("OBJECT_ENV", {"expectMarker": f"interop.marker={MARKER}"}), ("HOST_TOP", {}),
                           ("HOST_DISK_USAGE", {}), ("LIST_CONFIGURE_WAS", {})):
        r = client.call("relay", cmd=command, objHash=h, **extra)
        results[command] = {"valid": r["valid"], "count": r["count"], "elapsedMs": r["elapsedMs"],
                            "marker": r.get("expectMarker"),
                            # OBJECT_ENV(Host는 환경변수)의 key 이름은 기록하지 않는다.
                            "keys": None if command == "OBJECT_ENV" or not r.get("packs") else sorted(r["packs"][0]["keys"])[:6]}
    checks.add(6, "Host Agent relays (V1 framing)", all(v["valid"] and v["count"] == 1 for v in results.values())
               and results["OBJECT_ENV"]["marker"], relays=results)
    # conf 변경: UDP는 A로, V1 TCP는 B에 남는다.
    written = write_conf(conf, host_conf(a.port, temp))
    client.call("login", port=a.port)
    on_a = eventually(lambda: next((o for o in client.call("objects")["objects"] if o["name"] == name), None), 20)
    moved_udp = time.time()
    time.sleep(max(0.0, written + 20 - time.time()))
    client.call("login", port=b.port)
    r = client.call("relay", cmd="OBJECT_ENV", objHash=h, expectMarker=f"interop.marker={MARKER}")
    a_agents = a.agent_connections()
    checks.add(6, "Host Agent conf change: UDP moves at once, V1 TCP stays on the old server",
               bool(on_a) and r["valid"] and r["count"] == 1 and r["expectMarker"] and a_agents == 0,
               udpOnNewSec=round(moved_udp - written, 1), relayViaOldAfterSec=round(time.time() - written, 1),
               newServerAgentConnections=a_agents)
    # 기존 서버를 멈추면 V1 TCP가 새 주소로 다시 붙는다.
    code = b.stop()
    stopped = time.time()
    moved = eventually(lambda: a.agent_connections() == 1 and time.time(), 20, 0.1)
    client.call("login", port=a.port)
    r = client.call("relay", cmd="OBJECT_ENV", objHash=h, expectMarker=f"interop.marker={MARKER}")
    checks.add(6, "Host Agent V1 TCP moves to the new address when the old server stops",
               code == 0 and bool(moved) and r["valid"] and r["count"] == 1 and r["expectMarker"],
               moveSec=round(moved - stopped, 2) if moved else None)
    text = host.output()
    checks.add(6, "Host Agent process alive without exceptions", host.process.poll() is None
               and "Exception" not in text, exceptionLines=text.count("Exception"))


def ingest_calls(server):
    ingest = server.health()["ingest"]
    return {k: ingest.get(k) for k in ("KubeHeld", "KubeSeqEnqueued", "KubeSeqDone", "KubeSeqFailed", "KubeSeqDropped",
                                        "CPUDumpEnqueued", "CPUDumpDone", "CPUDumpFailed", "CPUDumpDropped")}


def scenario_kube_seq(ctx, checks):
    """kube Host Agent: 첫 heartbeat는 등록을 멈추고(PodSeqManager) 서버가 OBJECT_SET_KUBE_SEQ로 seq를 준다.

    Agent는 그 seq로 obj_name을 <pod>_<seq>로 바꾸고 새 objHash로 TCP를 다시 연다(HostAgentEnv.setKubePodSeq,
    TcpWorker.process). 원본 PodSeqManager.getNewPodSeq: 새 pod 이름의 첫 host는 seq 0.
    """
    a, client, temp = ctx["server_a"], ctx["client"], ctx["temp"]
    client.call("login", port=a.port)
    before = ingest_calls(a)
    conf = temp / "kube-agent.conf"
    write_conf(conf, kube_conf(a.port, temp))
    started = time.time()
    # Configure.isKube()는 KUBERNETES_SERVICE_HOST가 비어 있지 않은지만 본다(접속하지 않음).
    kube = HostAgent(ctx["agent_java"], temp, ctx["host_jar"], conf, name="kube-agent",
                     env={"KUBERNETES_SERVICE_HOST": "127.0.0.1"})
    ctx["kube_agent"] = kube
    prefix = f"/{KUBE_POD}_"

    def named():
        return [o for o in client.call("objects")["objects"] if o["name"].startswith(prefix)]
    obj = eventually(lambda: next((o for o in named() if o["alive"] and o["name"][len(prefix):].isdigit()), None), 40)
    registered_sec = round(time.time() - started, 1)
    time.sleep(3)  # 이름 변경 뒤 heartbeat 몇 번
    names = sorted(o["name"] for o in named())
    after = ingest_calls(a)
    delta = {k: after[k] - before[k] for k in after}
    registry = temp / "obj-registry"
    seq_files = {p.name: p.read_text() for p in registry.glob("*.scouterkubeseq")}
    checks.add(7, "OBJECT_SET_KUBE_SEQ: kube Host Agent registers as <pod>_<seq> (seq 0), the held _rnd name never",
               bool(obj) and obj["name"] == prefix + "0" and names == [prefix + "0"]
               and delta["KubeHeld"] >= 1 and delta["KubeSeqDone"] >= 1 and delta["KubeSeqDropped"] == 0
               and seq_files == {"0.scouterkubeseq": f"{KUBE_POD}_0"},
               object=obj, objectsWithPod=names, registeredSec=registered_sec, ingestDelta=delta, registryFiles=seq_files)
    if not obj:
        return
    # 새 objHash로 다시 연 V1 세션으로 relay가 간다.
    r = client.call("relay", cmd="OBJECT_ENV", objHash=obj["hash"], expectMarker=f"interop.marker={MARKER}")
    checks.add(7, "relay reaches the renamed kube Host Agent (V1 session reopened with the new objHash)",
               r["valid"] and r["count"] == 1 and r["expectMarker"], count=r["count"], elapsedMs=r["elapsedMs"])
    ctx["kube_obj"] = obj


def dump_files(ctx):
    return sorted((ctx["temp"] / "agent-dump").glob("scouter.threaddump.*.dump"))


def scenario_cpu_autodump(ctx, checks):
    """CPU 초과 autodump: Host Agent ProcPerf가 샘플 JVM의 ProcCpu를 보내고, Agent heartbeat의 ADC tag가 켜져 있으면
    서버(ObjectCpuChecker)가 ProcCpu > ADC_TH 상태가 ADC_D를 넘을 때 TRIGGER_THREAD_DUMPS_FROM_CONDITIONS를 보낸다.

    Agent(DumpOnCpuExceedanceWorker)는 reason이 TRIGGER_DUMP_REASON_TYPE_CPU_EXCEEDED이면 autodump_cpu_exceeded_*
    가드(dump_cnt회, dump_interval 간격)를 따르고, 다른 reason이면 가드 없이 thread dump 1개를 만든다.
    """
    a, proxy, client, agent, temp = ctx["server_a"], ctx["proxy"], ctx["client"], ctx["agent"], ctx["temp"]
    client.call("login", port=a.port)
    h = ctx["obj_hash"]
    base_files = set(dump_files(ctx))
    output_before = agent.output()
    autodump = {"autodump_cpu_exceeded_enabled": "true", "autodump_cpu_exceeded_duration_ms": AUTODUMP_DURATION_MS,
                "autodump_cpu_exceeded_dump_interval_ms": AUTODUMP_INTERVAL_MS,
                "autodump_cpu_exceeded_dump_cnt": AUTODUMP_CNT}

    def conf(threshold):
        return {**agent_conf(a.port, ctx["proxy_port"], temp), **autodump,
                "autodump_cpu_exceeded_threshold_pct": threshold}
    # 1) A(기록 proxy 경유)로 돌아오게 하고 ADC를 켜되 threshold는 기본 90.
    written = agent.write_conf(**conf(90))
    # 개발 실행(--server-calls-only)에서는 이미 proxy 세션이 있어 새 magic이 없을 수 있다.
    session = eventually(lambda: [e for e in proxy.snapshot(event="magic")
                                  if e["objHash"] == h and e["conn"] in proxy.open_conns()], 30, 0.2)
    obj = eventually(lambda: (find_object(client) or {}).get("alive") and find_object(client), 30, 0.5)

    def proc_cpu():
        r = client.call("counter", objHash=h, name="ProcCpu")
        return r if r.get("present") else None
    cpu = eventually(proc_cpu, 30)
    checks.add(7, "Host Agent ProcPerf ProcCpu for the sample JVM reaches the Java object on the server",
               bool(session) and bool(obj) and bool(cpu), agentSessionViaProxy=bool(session), objectAlive=bool(obj),
               procCpu=(cpu or {}).get("value"), valueType=(cpu or {}).get("valueType"))

    def adc_tags(threshold):
        want = {"ADC": "true", "ADC_TH": str(threshold), "ADC_D": str(AUTODUMP_DURATION_MS)}
        tags = (find_object(client) or {}).get("autodumpTags")
        return tags if tags == want else None
    tags = eventually(lambda: adc_tags(90), 20, 0.5)
    checks.add(7, "server object carries the agent heartbeat ADC tags (ADC, ADC_TH 90, ADC_D)", bool(tags),
               tags=tags or (find_object(client) or {}).get("autodumpTags"))
    # 2) ProcCpu <= ADC_TH 동안은 보내지 않는다(ADC_D + heartbeat·conf reload 여유).
    calls0 = ingest_calls(a)
    t0 = time.time()
    time.sleep(AUTODUMP_DURATION_MS / 1000 + 8)
    calls1 = ingest_calls(a)
    sent = proxy.snapshot(t0, event="command", cmd="TRIGGER_THREAD_DUMPS_FROM_CONDITIONS")
    samples = [proc_cpu() for _ in range(3)]
    # ProcCpu가 0~1 비율이라(검증 문서 §2.8 관찰 1) 이 단계는 threshold 비교의 변별력이 없다.
    # ADC tag가 반영된 상태에서 잘못된 전송이 없다는 것만 확인한다.
    checks.add(7, "no CPU dump trigger while ProcCpu <= ADC_TH (90) with the ADC tags in effect",
               calls1["CPUDumpEnqueued"] == calls0["CPUDumpEnqueued"] and not sent and set(dump_files(ctx)) == base_files,
               waitedSec=round(time.time() - t0, 1), procCpu=[(s or {}).get("value") for s in samples])
    # 3) threshold 0: ProcCpu > 0이 ADC_D를 넘기면 보낸다. 첫 trigger 뒤 바로 90으로 되돌려 반복을 막는다.
    written = agent.write_conf(**conf(0))
    tags_zero = eventually(lambda: adc_tags(0), 15, 0.2)
    tags_zero_sec = round(time.time() - written, 2) if tags_zero else None
    trigger = eventually(lambda: proxy.snapshot(written, event="command", cmd="TRIGGER_THREAD_DUMPS_FROM_CONDITIONS"), 45, 0.1)
    restored = agent.write_conf(**conf(90))
    answer = eventually(lambda: proxy.snapshot(written, event="answer", cmd="TRIGGER_THREAD_DUMPS_FROM_CONDITIONS"), 5, 0.1)
    param = trigger[0].get("param") if trigger else None
    checks.add(7, "server sends TRIGGER_THREAD_DUMPS_FROM_CONDITIONS {objHash, CPU_EXCEEDED reason} after ADC_D; "
                  "agent answers NoNEXT only (handler returns null)",
               bool(tags_zero) and bool(trigger)
               and param == {"objHash": h, "TRIGGER_DUMP_REASON": "TRIGGER_DUMP_REASON_TYPE_CPU_EXCEEDED"}
               and trigger[0]["t"] - written >= AUTODUMP_DURATION_MS / 1000 - 0.5
               and bool(answer) and answer[0]["items"] == 0,
               thresholdTagZeroSec=tags_zero_sec, triggerSec=round(trigger[0]["t"] - written, 2) if trigger else None,
               param=param,
               paramError=trigger[0].get("paramError") if trigger else None,
               answerItems=answer[0]["items"] if answer else None,
               answerMs=round((answer[0]["t"] - trigger[0]["t"]) * 1000, 1) if trigger and answer else None)
    # 4) Agent 쪽 가드: dump_cnt회, dump_interval 간격(첫 notify는 DumpUtil thread 기동과 겹치면 사라질 수 있다).
    time.sleep(AUTODUMP_CNT * AUTODUMP_INTERVAL_MS / 1000 + 4)
    calls2 = ingest_calls(a)
    triggers = len(proxy.snapshot(written, event="command", cmd="TRIGGER_THREAD_DUMPS_FROM_CONDITIONS"))
    done = calls2["CPUDumpDone"] - calls0["CPUDumpDone"]
    checks.add(7, "Go CPU dump queue: every trigger enqueued is done, no failure or drop",
               done >= 1 and done == triggers and calls2["CPUDumpEnqueued"] - calls0["CPUDumpEnqueued"] == done
               and calls2["CPUDumpFailed"] == calls0["CPUDumpFailed"] and calls2["CPUDumpDropped"] == calls0["CPUDumpDropped"],
               before=calls0, after=calls2, triggersSeenByProxy=triggers, restoredAfterSec=round(restored - written, 2))
    files = [f for f in dump_files(ctx) if f not in base_files]
    times = sorted(f.stat().st_mtime for f in files)
    gaps = [round(b - a_, 2) for a_, b in zip(times, times[1:])]
    sample = files[0].read_text(errors="replace") if files else ""
    # 가드 없는 경로(다른 reason)는 trigger마다 dump 1개다. 파일이 trigger 수보다 많아야 dump_cnt loop(가드 경로)의 근거다.
    checks.add(7, "agent takes the CPU-exceeded guard: triggers < dump files <= dump_cnt x triggers, dump_interval apart",
               triggers >= 1 and triggers < len(files) <= AUTODUMP_CNT * triggers
               and all(g >= AUTODUMP_INTERVAL_MS / 1000 - 0.5 for g in gaps) and "interop-idle-" in sample,
               files=len(files), triggers=triggers, dumpCnt=AUTODUMP_CNT, gapsSec=gaps, firstFileBytes=len(sample))
    output = agent.output()[len(output_before):]
    checks.add(7, "sample JVM alive; no exception or unknown command around the dump",
               agent.process.poll() is None and output.count("Exception") == 0 and output.count("A130") == 0,
               exceptions=output.count("Exception"), a130=output.count("A130"))


def scenario_agent_health(ctx, checks):
    agent = ctx["agent"]
    text = agent.output()
    alive = agent.process.poll() is None
    counts = {key: text.count(key) for key in ("A129", "A130", "Exception", "Error")}
    checks.add(0, "sample JVM alive; agent got no unknown command (A130); hook registered (A129)",
               alive and counts["A130"] == 0 and counts["A129"] >= 1, alive=alive, counts=counts)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server", type=Path, help="Go binary; 생략하면 임시 directory에 ./cmd/scouter-go를 빌드")
    parser.add_argument("--source", type=Path, default=Path(os.environ.get("SCOUTER_SOURCE", ROOT.parent / "scouter")))
    parser.add_argument("--agent-jar", type=Path, default=os.environ.get("SCOUTER_AGENT_JAR"),
                        help="scouter.agent.jar (기본: <source>/../scouter-bin/agent.java/scouter.agent.jar)")
    parser.add_argument("--host-jar", type=Path, default=os.environ.get("SCOUTER_HOST_JAR"),
                        help="scouter.host.jar (기본: <source>/../scouter-bin/agent.host/scouter.host.jar)")
    parser.add_argument("--java-home", help="Client 컴파일·실행 JDK(11+); 기본은 java-integration과 같다")
    parser.add_argument("--agent-java-home", help="샘플 JVM JDK; 기본 zulu-8(있으면)")
    parser.add_argument("--server-rev", default="HEAD", help="--server가 없을 때 빌드할 git revision(기본 HEAD, 미커밋 변경 제외)")
    parser.add_argument("--agent-jvm-opt", action="append", default=[],
                        help="샘플 JVM 추가 옵션(반복 가능). JDK 9+에서 thread dump·heap histogram은 원본 안내대로 "
                             "-Djdk.attach.allowAttachSelf=true가 필요하다")
    parser.add_argument("--keepalive-idle", type=int, default=70, help="KEEP_ALIVE 관찰 유휴 시간(초)")
    parser.add_argument("--compile-only", action="store_true")
    parser.add_argument("--server-calls-only", action="store_true",
                        help="개발용: 수집(1) 뒤 서버 발신 명령 시나리오(7)만 실행한다. 판정에는 전체 실행을 쓴다")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--keep-temp", action="store_true")
    args = parser.parse_args()

    source = args.source.expanduser().resolve()
    agent_jar_source = (args.agent_jar or source.parent / "scouter-bin/agent.java/scouter.agent.jar")
    agent_jar_source = Path(agent_jar_source).expanduser().resolve()
    if not agent_jar_source.is_file():
        raise RuntimeError(f"agent jar not found: {agent_jar_source}")
    host_jar = Path(args.host_jar or source.parent / "scouter-bin/agent.host/scouter.host.jar").expanduser().resolve()
    if not host_jar.is_file():
        raise RuntimeError(f"host agent jar not found: {host_jar}")
    java, javac = JI.java_tools(args.java_home)
    jar_tool = str(Path(javac).with_name("jar"))
    agent_home = args.agent_java_home
    if not agent_home and Path("/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home").is_dir():
        agent_home = "/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home"
    agent_java = str(Path(agent_home) / "bin/java") if agent_home else java

    temp = Path(tempfile.mkdtemp(prefix="scouter-java-agent-interop-"))
    checks = Checks()
    report = {"checks": checks.rows, "passed": False}
    ctx = {"temp": temp, "agent_java": agent_java, "host_jar": host_jar}
    started = time.time()
    try:
        classes, agent_jar, hooks_jar, sample, compiled = compile_all(source, temp, java, javac, jar_tool, agent_jar_source)
        report.update(compiled)
        with zipfile.ZipFile(host_jar) as z:
            inner = sorted(n for n in z.namelist() if n.startswith("scouter-agent-host-") and n.endswith(".jar"))
        report["host_agent_jar"] = {"sha256": sha256(host_jar), "bytes": host_jar.stat().st_size, "inner": inner}
        host_diff = run(["git", "-C", str(source), "diff", "--stat", report["agent_wire_diff_vs_baseline"]["tag"],
                         compiled["source_commit"], "--", "scouter.agent.host/src"]).strip()
        report["host_agent_src_diff_vs_baseline"] = host_diff
        if host_diff:
            raise RuntimeError(f"host agent sources differ from baseline:\n{host_diff}")
        for name, path in (("client_java", java), ("agent_java", agent_java)):
            version = subprocess.run([path, "-version"], capture_output=True, text=True)
            report[name] = version.stderr.strip().splitlines()[0] if version.stderr else path
        if args.compile_only:
            report["not_run"] = "--compile-only"
            return
        binary = args.server.expanduser().resolve() if args.server else temp / "scouter-go"
        if not args.server:
            # 작업 tree의 미커밋 변경이 섞이지 않게 지정 revision을 git archive로 풀어 빌드한다.
            commit = run(["git", "-C", str(ROOT), "rev-parse", args.server_rev]).strip()
            tree = temp / "server-src"
            tree.mkdir()
            archive = subprocess.run(["git", "-C", str(ROOT), "archive", commit], capture_output=True, check=True).stdout
            subprocess.run(["tar", "-x", "-C", str(tree)], input=archive, check=True)
            go = ROOT / ".tools/go/bin/go"
            go_command = str(go) if go.is_file() else shutil.which("go")
            environment = os.environ.copy()
            environment["GOMAXPROCS"] = "2"
            environment["GOTOOLCHAIN"] = "local"
            run([go_command, "build", "-o", str(binary), "./cmd/scouter-go"], cwd=tree, env=environment)
            report["server_build"] = {"commit": commit, "source": "git archive (clean tree)"}
        else:
            report["server_build"] = {"binary": str(binary), "worktree_head": run(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).strip()}
        ctx["binary"] = binary
        report["server_binary_sha256"] = sha256(binary)

        dump = temp / "agent-dump"
        dump.mkdir()
        big = dump / "interop-dump-9MiB.bin"
        big.write_bytes(os.urandom(9 << 20))
        ctx["dump_file"] = big
        (temp / "agent-logs").mkdir()
        (temp / "agent-plugin").mkdir()
        (temp / "obj-registry").mkdir()

        a = GoServer(binary, temp, "a", free_port(), free_port(), temp / "db-a")
        ctx["server_a"] = a
        ctx["server_a_ready"] = a.start()
        proxy_port = free_port()
        proxy = FrameProxy(proxy_port, a.port, temp / "proxy-frames.jsonl")
        ctx["proxy"] = proxy
        ctx["proxy_port"] = proxy_port
        conf = temp / "scouter.agent.conf"
        write_conf(conf, agent_conf(a.port, proxy_port, temp))
        agent = Agent(agent_java, temp, agent_jar, hooks_jar, sample, conf, args.agent_jvm_opt)
        report["agent_jvm_opts"] = args.agent_jvm_opt
        ctx["agent"] = agent
        client = Client(java, classes, temp)
        ctx["client"] = client
        login = client.call("login", port=a.port)
        if not login.get("ok"):
            raise RuntimeError(f"Client login failed: {login}")
        report["client_login"] = login

        log("scenario 1: collection")
        scenario_collect(ctx, checks)
        if args.server_calls_only:
            report["partial"] = "--server-calls-only: scenarios 2-6 skipped"
        else:
            log("scenario 2: relays")
            scenario_relay(ctx, checks)
            log("scenario 3: orphan relay, agent failure, response limit")
            scenario_orphan(ctx, checks)
            log("scenario 4a: KEEP_ALIVE")
            scenario_keepalive(ctx, checks, args.keepalive_idle)
            log("scenario 4b: restart with a new DB, text reset")
            scenario_restart_text_reset(ctx, checks)
            log("scenario 3f: agent slower than the Go agent so_timeout (after the 4b restart and its reset)")
            scenario_agent_so_timeout(ctx, checks)
            log("scenario 5: collector address change")
            scenario_move(ctx, checks)
            log("scenario 4c: direct restart reconnect time")
            scenario_direct_restart(ctx, checks, [5, 12])
            scenario_agent_health(ctx, checks)
            report["server_health_before_host"] = {name: {"tcp": s.health()["tcp"], "agentOrphans": s.health()["agentOrphans"]}
                                                   for name, s in (("a", ctx["server_a"]), ("b", ctx["server_b"]))}
            log("scenario 6: host agent")
            scenario_host_agent(ctx, checks)
        log("scenario 7a: kube Host Agent, OBJECT_SET_KUBE_SEQ")
        scenario_kube_seq(ctx, checks)
        log("scenario 7b: CPU-exceeded thread dump trigger")
        scenario_cpu_autodump(ctx, checks)
        report["server_a_agent_calls"] = ingest_calls(ctx["server_a"])
        report["passed"] = all(row["status"] == "pass" for row in checks.rows) and not args.server_calls_only
    finally:
        report["elapsed_sec"] = round(time.time() - started, 1)
        for key in ("client",):
            if key in ctx:
                ctx["client"].close()
        for key in ("agent", "host_agent", "kube_agent"):
            if key in ctx:
                ctx[key].stop()
        if "proxy" in ctx:
            ctx["proxy"].stop()
        exits = {}
        for name in ("server_a", "server_b"):
            if name in ctx:
                try:
                    exits[name] = ctx[name].stop()
                except RuntimeError as error:
                    exits[name] = str(error)
        report["server_exits"] = exits
        report["server_a_first_health"] = ctx.get("server_a_first_health")
        for name in ("server_a", "server_b"):
            if name in ctx:
                relevant = [line for line in ctx[name].logs().splitlines()
                            if any(k in line for k in ("agent relay", "skipped objHash", "text cache reset", "orphan"))]
                report[f"{name}_log_lines"] = relevant[-40:]
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str) + "\n")
        failed = [r for r in checks.rows if r["status"] != "pass"]
        log(f"checks: {len(checks.rows) - len(failed)} pass, {len(failed)} fail; elapsed {report['elapsed_sec']}s")
        if args.keep_temp:
            log(f"Temporary evidence: {temp}")
        else:
            shutil.rmtree(temp, ignore_errors=True)
    if not report["passed"] and not args.compile_only and not (args.server_calls_only and not failed):
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        raise SystemExit(str(error)) from error
