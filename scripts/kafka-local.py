#!/usr/bin/env python3
"""격리된 로컬 Apache Kafka broker(KRaft 단일 노드) 기동/정지/검증 도구.

목적: Pulse(3단계) Kafka 연동을 운영 Kafka 없이 로컬에서 검증한다.
- 네트워크 다운로드를 하지 않는다. 이미 풀어 둔 공식 binary 배포본(`--kafka-home` 또는 KAFKA_LOCAL_HOME,
  예: kafka_2.13-4.0.0, `<home>/libs/*.jar`)을 우선 사용하고, 없으면 Gradle 캐시의 Kafka 3.0.1 jar 로 fallback 한다.
  배포본 디렉터리는 읽기 전용으로 취급한다(bin/*.sh 는 <home>/logs 를 만들므로 쓰지 않고 같은 main class 를 직접 실행).
- broker는 임시 디렉터리와 127.0.0.1의 임의(또는 지정) 포트에서만 뜬다.
- 모든 산출물(데이터, 로그, pid, 컴파일된 helper class)은 `--dir` 아래에만 생긴다.
- 모드: KRaft(process.roles=broker,controller, ZooKeeper 없음). 4.0 은 KRaft 전용(dynamic quorum, `format --standalone`),
  3.0.1 fallback 은 static quorum(controller.quorum.voters)이며 early-access 다.
- helper(topic 생성/produce/consume)는 kafka-clients 4.0.0 jar로 임시 컴파일한 작은 Java 프로그램이다.

하위 명령:
  start        --dir D [--port N] [--controller-port N] [--java-home P] [--kafka-home P] [--timeout S]
  stop         --dir D [--timeout S]
  status       --dir D
  topic-create --dir D --name T --partitions N [--replication-factor 1]
  produce      --dir D --topic T --spec FILE       (TSV: partition<TAB>key_hex<TAB>value_hex<TAB>hk_hex=hv_hex;...)
  consume      --dir D --topic T [--from-beginning] [--max N] [--timeout S]   (JSON lines 출력)
  produce-test --dir D [--topic T] [--partitions N]
  selftest     --dir D [--partitions N]            (start → topic-create → produce-test → stop)
  classpath    [--java-home P]                     (jar 해석 결과 출력)
  compare      --expect FILE --actual FILE         (Go 기대 JSON lines 와 consume 출력의 partition별 순서·bytes 비교)
  dump-log     --dir D --topic T                   (broker 가 저장한 batch 의 compresscodec 집계, DumpLogSegments)
"""

import argparse
import base64
import hashlib
import json
import os
import secrets
import signal
import socket
import struct
import subprocess
import sys
import time

KAFKA_VERSION = "3.0.1"  # Gradle 캐시 fallback 버전
CLIENTS_VERSION = "4.0.0"
DEFAULT_JAVA_HOME = "/Library/Java/JavaVirtualMachines/zulu-18.jdk/Contents/Home"

# 배포본 모드(log4j2) 로깅 설정. ${sys:kafka.logs.dir} 아래 파일에만 기록한다.
LOG4J2_PROPERTIES = """status = warn
name = kafka-local
appender.file.type = File
appender.file.name = FILE
appender.file.fileName = ${sys:kafka.logs.dir}/${sys:kafka.local.logname}
appender.file.append = true
appender.file.layout.type = PatternLayout
appender.file.layout.pattern = [%d{yyyy-MM-dd HH:mm:ss.SSS}] %p %m (%c)%n
rootLogger.level = ${sys:kafka.local.loglevel}
rootLogger.appenderRef.file.ref = FILE
"""

# broker classpath: kafka_2.13-3.0.1.pom 의 compile/runtime 의존성 + kafka-clients 의 runtime 의존성.
# (group, artifact, 허용 버전 목록: 앞에서부터 우선)
BROKER_ARTIFACTS = [
    ("org.apache.kafka", "kafka_2.13", [KAFKA_VERSION]),
    ("org.apache.kafka", "kafka-clients", [KAFKA_VERSION]),
    ("org.apache.kafka", "kafka-server-common", [KAFKA_VERSION]),
    ("org.apache.kafka", "kafka-metadata", [KAFKA_VERSION]),
    ("org.apache.kafka", "kafka-raft", [KAFKA_VERSION]),
    ("org.apache.kafka", "kafka-storage", [KAFKA_VERSION]),
    ("org.apache.kafka", "kafka-storage-api", [KAFKA_VERSION]),
    ("org.scala-lang", "scala-library", ["2.13.6"]),
    ("org.scala-lang", "scala-reflect", ["2.13.6"]),
    ("org.scala-lang.modules", "scala-collection-compat_2.13", ["2.4.4"]),
    ("org.scala-lang.modules", "scala-java8-compat_2.13", ["1.0.0"]),
    ("com.typesafe.scala-logging", "scala-logging_2.13", ["3.9.3"]),
    # pom 은 jackson 2.12.6 을 요구하지만 캐시에는 2.13.2 계열만 있다(모듈 간 동일 minor 로 맞춤).
    ("com.fasterxml.jackson.core", "jackson-core", ["2.12.6", "2.13.2"]),
    ("com.fasterxml.jackson.core", "jackson-databind", ["2.12.6", "2.13.2.1", "2.13.2"]),
    ("com.fasterxml.jackson.core", "jackson-annotations", ["2.12.6", "2.13.2"]),
    ("com.fasterxml.jackson.module", "jackson-module-scala_2.13", ["2.12.6", "2.13.2"]),
    ("com.fasterxml.jackson.dataformat", "jackson-dataformat-csv", ["2.12.6", "2.13.2"]),
    ("com.fasterxml.jackson.datatype", "jackson-datatype-jdk8", ["2.12.6", "2.13.2"]),
    ("com.thoughtworks.paranamer", "paranamer", ["2.8"]),
    ("net.sf.jopt-simple", "jopt-simple", ["5.0.4"]),
    ("net.sourceforge.argparse4j", "argparse4j", ["0.7.0"]),
    ("com.yammer.metrics", "metrics-core", ["2.2.0"]),
    ("io.dropwizard.metrics", "metrics-core", ["4.1.12.1", "4.2.9"]),
    ("org.apache.zookeeper", "zookeeper", ["3.6.3"]),
    ("org.apache.zookeeper", "zookeeper-jute", ["3.6.3"]),
    ("org.apache.yetus", "audience-annotations", ["0.5.0"]),
    ("commons-cli", "commons-cli", ["1.4"]),
    ("org.lz4", "lz4-java", ["1.7.1", "1.8.0"]),
    ("org.xerial.snappy", "snappy-java", ["1.1.8.1"]),
    ("com.github.luben", "zstd-jni", ["1.5.0-2"]),
    # 로깅: Kafka 는 slf4j API 로만 로그를 낸다. slf4j-simple 로 파일에 기록한다.
    ("org.slf4j", "slf4j-api", ["2.0.6"]),
    ("org.slf4j", "slf4j-simple", ["2.0.6"]),
]

HELPER_ARTIFACTS = [
    ("org.apache.kafka", "kafka-clients", [CLIENTS_VERSION]),
    ("org.slf4j", "slf4j-api", ["2.0.6"]),
    ("org.slf4j", "slf4j-simple", ["2.0.6"]),
    ("org.lz4", "lz4-java", ["1.8.0", "1.7.1"]),
    ("org.xerial.snappy", "snappy-java", ["1.1.10.5", "1.1.8.1"]),
    ("com.github.luben", "zstd-jni", ["1.5.6-6", "1.5.6-4", "1.5.0-2"]),
]

HELPER_CLASS = "KafkaLocalHelper"
HELPER_SOURCE = r'''
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class KafkaLocalHelper {
    static String hex(byte[] b) {
        if (b == null) return null;
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) sb.append(String.format("%02x", x & 0xff));
        return sb.toString();
    }
    static byte[] unhex(String s) {
        if (s == null || s.equals("-")) return null;
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) out[i] = (byte) Integer.parseInt(s.substring(2 * i, 2 * i + 2), 16);
        return out;
    }
    static String jstr(String s) {
        if (s == null) return "null";
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            if (c == '"' || c == '\\') sb.append('\\').append(c);
            else if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
            else sb.append(c);
        }
        return sb.append('"').toString();
    }
    static Properties base(String bootstrap) {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("request.timeout.ms", "10000");
        p.put("default.api.timeout.ms", "30000");
        return p;
    }

    public static void main(String[] args) throws Exception {
        String cmd = args[0];
        String bootstrap = args[1];
        switch (cmd) {
            case "ready": ready(bootstrap, Long.parseLong(args[2])); break;
            case "topic-create": topicCreate(bootstrap, args[2], Integer.parseInt(args[3]), Short.parseShort(args[4])); break;
            case "produce": produce(bootstrap, args[2], args[3]); break;
            case "consume": consume(bootstrap, args[2], Boolean.parseBoolean(args[3]), Integer.parseInt(args[4]), Long.parseLong(args[5])); break;
            default: System.err.println("unknown command " + cmd); System.exit(2);
        }
    }

    static void ready(String bootstrap, long deadlineMs) throws Exception {
        long deadline = System.currentTimeMillis() + deadlineMs;
        Properties p = base(bootstrap);
        p.put("request.timeout.ms", "3000");
        p.put("default.api.timeout.ms", "3000");
        Exception last = null;
        try (Admin admin = Admin.create(p)) {
            while (System.currentTimeMillis() < deadline) {
                try {
                    DescribeClusterResult r = admin.describeCluster();
                    Collection<org.apache.kafka.common.Node> nodes = r.nodes().get();
                    String clusterId = r.clusterId().get();
                    if (!nodes.isEmpty()) {
                        // controller 가 topic 생성을 받을 수 있는지 metadata 조회까지 확인한다.
                        admin.listTopics().names().get();
                        System.out.println("{\"ready\":true,\"cluster_id\":" + jstr(clusterId) + ",\"nodes\":" + nodes.size() + "}");
                        return;
                    }
                } catch (Exception e) {
                    last = e;
                }
                Thread.sleep(500);
            }
        }
        System.err.println("not ready: " + (last == null ? "no nodes" : last));
        System.exit(1);
    }

    static void topicCreate(String bootstrap, String name, int partitions, short rf) throws Exception {
        try (Admin admin = Admin.create(base(bootstrap))) {
            admin.createTopics(Collections.singleton(new NewTopic(name, partitions, rf))).all().get();
            TopicDescription d = admin.describeTopics(Collections.singleton(name)).allTopicNames().get().get(name);
            System.out.println("{\"topic\":" + jstr(name) + ",\"partitions\":" + d.partitions().size() + "}");
        }
    }

    // spec: TSV lines  partition \t key_hex|- \t value_hex|- \t hk_hex=hv_hex;...
    static void produce(String bootstrap, String topic, String specFile) throws Exception {
        Properties p = base(bootstrap);
        p.put("acks", "all");
        p.put("enable.idempotence", "false");
        p.put("linger.ms", "0");
        p.put("key.serializer", ByteArraySerializer.class.getName());
        p.put("value.serializer", ByteArraySerializer.class.getName());
        List<String> lines = Files.readAllLines(Paths.get(specFile), StandardCharsets.UTF_8);
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(p)) {
            List<java.util.concurrent.Future<RecordMetadata>> futures = new ArrayList<>();
            for (String line : lines) {
                if (line.isEmpty()) continue;
                String[] f = line.split("\t", -1);
                Integer partition = f[0].equals("-") ? null : Integer.valueOf(f[0]);
                byte[] key = unhex(f[1]);
                byte[] value = unhex(f[2]);
                List<Header> headers = new ArrayList<>();
                if (f.length > 3 && !f[3].isEmpty()) {
                    for (String h : f[3].split(";")) {
                        int eq = h.indexOf('=');
                        String hk = new String(unhex(h.substring(0, eq)), StandardCharsets.UTF_8);
                        byte[] hv = unhex(h.substring(eq + 1));
                        headers.add(new RecordHeader(hk, hv));
                    }
                }
                futures.add(producer.send(new ProducerRecord<>(topic, partition, key, value, headers)));
            }
            producer.flush();
            for (java.util.concurrent.Future<RecordMetadata> f : futures) {
                RecordMetadata m = f.get();
                System.out.println("{\"topic\":" + jstr(m.topic()) + ",\"partition\":" + m.partition() + ",\"offset\":" + m.offset() + "}");
            }
        }
    }

    static void consume(String bootstrap, String topic, boolean fromBeginning, int max, long timeoutMs) throws Exception {
        Properties p = base(bootstrap);
        p.put("key.deserializer", ByteArrayDeserializer.class.getName());
        p.put("value.deserializer", ByteArrayDeserializer.class.getName());
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", fromBeginning ? "earliest" : "latest");
        long deadline = System.currentTimeMillis() + timeoutMs;
        int count = 0;
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : consumer.partitionsFor(topic, Duration.ofMillis(timeoutMs))) tps.add(new TopicPartition(topic, pi.partition()));
            tps.sort(Comparator.comparingInt(TopicPartition::partition));
            consumer.assign(tps);
            if (fromBeginning) consumer.seekToBeginning(tps); else consumer.seekToEnd(tps);
            PrintStream out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, "UTF-8");
            while (count < max && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> recs = consumer.poll(Duration.ofMillis(Math.max(1, Math.min(1000, deadline - System.currentTimeMillis()))));
                for (ConsumerRecord<byte[], byte[]> r : recs) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("{\"topic\":").append(jstr(r.topic()))
                      .append(",\"partition\":").append(r.partition())
                      .append(",\"offset\":").append(r.offset())
                      .append(",\"timestamp\":").append(r.timestamp())
                      .append(",\"timestamp_type\":").append(jstr(r.timestampType().name))
                      .append(",\"key_hex\":").append(jstr(hex(r.key())))
                      .append(",\"value_hex\":").append(jstr(hex(r.value())))
                      .append(",\"headers\":[");
                    boolean first = true;
                    for (Header h : r.headers()) {
                        if (!first) sb.append(',');
                        first = false;
                        sb.append("{\"key\":").append(jstr(h.key())).append(",\"value_hex\":").append(jstr(hex(h.value()))).append('}');
                    }
                    sb.append("]}");
                    out.println(sb);
                    count++;
                    if (count >= max) break;
                }
            }
        }
        System.err.println("consumed " + count + " record(s)");
        if (count < max) System.exit(3);
    }
}
'''


def die(msg, code=1):
    print(f"kafka-local: {msg}", file=sys.stderr)
    sys.exit(code)


def gradle_cache(args):
    if getattr(args, "gradle_cache", None):
        return args.gradle_cache
    home = os.environ.get("GRADLE_USER_HOME") or os.path.join(os.path.expanduser("~"), ".gradle")
    return os.path.join(home, "caches", "modules-2", "files-2.1")


def java_home(args):
    for cand in (getattr(args, "java_home", None), os.environ.get("KAFKA_LOCAL_JAVA_HOME"), DEFAULT_JAVA_HOME, os.environ.get("JAVA_HOME")):
        if cand and os.path.isfile(os.path.join(cand, "bin", "java")):
            return cand
    die("JDK를 찾지 못했습니다. --java-home 또는 KAFKA_LOCAL_JAVA_HOME 을 지정하세요.")


def find_jar(cache, group, artifact, versions):
    """캐시에서 (group, artifact, version) 의 메인 jar 를 찾는다. 없으면 None."""
    for ver in versions:
        base = os.path.join(cache, group, artifact, ver)
        if not os.path.isdir(base):
            continue
        candidates = []
        for sha, _, files in os.walk(base):
            for f in files:
                if not f.endswith(".jar"):
                    continue
                if any(tag in f for tag in ("-sources", "-javadoc", "-test", "-tests")):
                    continue
                if f == f"{artifact}-{ver}.jar":
                    candidates.append(os.path.join(sha, f))
        if candidates:
            return ver, sorted(candidates)[0]
    return None, None


def resolve_classpath(cache, artifacts):
    jars, missing = [], []
    for group, artifact, versions in artifacts:
        ver, path = find_jar(cache, group, artifact, versions)
        if path is None:
            missing.append(f"{group}:{artifact}:{'|'.join(versions)}")
        else:
            jars.append((f"{group}:{artifact}:{ver}", path))
    return jars, missing


def classpath_or_die(cache, artifacts, label):
    jars, missing = resolve_classpath(cache, artifacts)
    if missing:
        print(f"kafka-local: {label} classpath 구성 실패. Gradle 캐시({cache})에 다음 jar 가 없습니다:", file=sys.stderr)
        for m in missing:
            print(f"  MISSING {m}", file=sys.stderr)
        print("시도한 classpath(발견분):", file=sys.stderr)
        for coord, path in jars:
            print(f"  {coord} -> {path}", file=sys.stderr)
        sys.exit(4)
    return jars


def kafka_home(args):
    for cand in (getattr(args, "kafka_home", None), os.environ.get("KAFKA_LOCAL_HOME")):
        if cand:
            cand = os.path.abspath(cand)
            if not os.path.isdir(os.path.join(cand, "libs")):
                die(f"--kafka-home {cand} 아래에 libs/ 가 없습니다.")
            return cand
    return None


def resolve_runtime(args, p=None):
    """broker/helper 실행에 필요한 정보를 한 번에 해석한다.

    반환 dict: mode(dist|gradle), kafka_version, kafka_home, broker_cp(list), helper_compile_cp(list),
    helper_run_cp(list), dynamic_quorum(bool), format_extra(list), log_opts(fn(logs_dir, name, level) -> list)
    """
    home = kafka_home(args)
    if home:
        libs = sorted(os.path.join(home, "libs", f) for f in os.listdir(os.path.join(home, "libs")) if f.endswith(".jar"))
        core = [j for j in libs if os.path.basename(j).startswith("kafka_2.13-")]
        clients = [j for j in libs if os.path.basename(j).startswith("kafka-clients-")]
        if not core or not clients:
            die(f"{home}/libs 에 kafka_2.13-*.jar 또는 kafka-clients-*.jar 가 없습니다.", 4)
        version = os.path.basename(core[0])[len("kafka_2.13-"):-len(".jar")]
        major = int(version.split(".")[0])
        log4j_cfg = None
        if p is not None:
            log4j_cfg = os.path.join(p["dir"], "log4j2.properties")
            if not os.path.isfile(log4j_cfg):
                with open(log4j_cfg, "w", encoding="utf-8") as f:
                    f.write(LOG4J2_PROPERTIES)

        def log_opts(logs_dir, name, level):
            return ["-Dlog4j2.configurationFile=" + log4j_cfg, "-Dkafka.logs.dir=" + logs_dir,
                    "-Dkafka.local.logname=" + name, "-Dkafka.local.loglevel=" + level]
        return {
            "mode": "dist", "kafka_version": version, "kafka_home": home, "broker_cp": libs,
            "helper_compile_cp": [clients[0]], "helper_run_cp": libs,
            # 4.0+: dynamic quorum(KIP-853) 이므로 controller.quorum.bootstrap.servers + format --standalone
            "dynamic_quorum": major >= 4, "format_extra": ["--standalone"] if major >= 4 else [],
            "log_opts": log_opts,
        }
    cache = gradle_cache(args)
    broker = classpath_or_die(cache, BROKER_ARTIFACTS, "broker")
    helper = classpath_or_die(cache, HELPER_ARTIFACTS, "helper")

    def log_opts(logs_dir, name, level):
        return ["-Dorg.slf4j.simpleLogger.logFile=" + os.path.join(logs_dir, name),
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=" + level,
                "-Dorg.slf4j.simpleLogger.showDateTime=true",
                "-Dorg.slf4j.simpleLogger.dateTimeFormat=yyyy-MM-dd HH:mm:ss.SSS",
                "-Dorg.slf4j.simpleLogger.showThreadName=false"]
    return {
        "mode": "gradle", "kafka_version": KAFKA_VERSION, "kafka_home": None,
        "broker_cp": [path for _, path in broker], "helper_compile_cp": [path for _, path in helper],
        "helper_run_cp": [path for _, path in helper],
        "dynamic_quorum": False, "format_extra": [], "log_opts": log_opts,
    }


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def port_open(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def paths(d):
    d = os.path.abspath(d)
    return {
        "dir": d,
        "data": os.path.join(d, "data"),
        "logs": os.path.join(d, "logs"),
        "helper": os.path.join(d, "helper"),
        "config": os.path.join(d, "server.properties"),
        "pid": os.path.join(d, "broker.pid"),
        "ready": os.path.join(d, "ready.json"),
        "cluster_id": os.path.join(d, "cluster-id"),
    }


def pid_alive(pid):
    """살아 있는 프로세스인지 판정한다. 우리 자식이면 zombie 를 회수하고, 다른 프로세스의 zombie(stat Z)도 종료로 본다."""
    try:
        wpid, _ = os.waitpid(pid, os.WNOHANG)
        if wpid == pid:
            return False
    except ChildProcessError:
        pass
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:
        stat = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True, check=False).stdout.strip()
    except OSError:
        return True
    return bool(stat) and not stat.startswith("Z")


def pid_is_our_broker(pid, config_path):
    try:
        out = subprocess.run(["ps", "-o", "command=", "-p", str(pid)], capture_output=True, text=True, check=False).stdout
    except OSError:
        return False
    return "kafka.Kafka" in out and config_path in out


def read_ready(p):
    if not os.path.isfile(p["ready"]):
        die(f"{p['ready']} 가 없습니다. 먼저 start 를 실행하세요.")
    with open(p["ready"], encoding="utf-8") as f:
        return json.load(f)


def ensure_helper(args, p):
    """helper Java 소스를 --dir/helper 아래에 쓰고 kafka-clients 4.0.0 으로 컴파일한다. 소스 해시가 같으면 재사용."""
    rt = resolve_runtime(args, p)
    cp = os.pathsep.join(rt["helper_compile_cp"])
    run_cp = os.pathsep.join(rt["helper_run_cp"])
    jh = java_home(args)
    os.makedirs(p["helper"], exist_ok=True)
    digest = hashlib.sha256((HELPER_SOURCE + cp + jh).encode()).hexdigest()
    classes = os.path.join(p["helper"], "classes")
    stamp = os.path.join(classes, "source.sha256")
    if not (os.path.isfile(stamp) and open(stamp).read().strip() == digest and os.path.isfile(os.path.join(classes, HELPER_CLASS + ".class"))):
        src = os.path.join(p["helper"], HELPER_CLASS + ".java")
        with open(src, "w", encoding="utf-8") as f:
            f.write(HELPER_SOURCE)
        os.makedirs(classes, exist_ok=True)
        log = os.path.join(p["helper"], "javac.log")
        with open(log, "w") as lf:
            rc = subprocess.run([os.path.join(jh, "bin", "javac"), "-encoding", "UTF-8", "-nowarn", "-d", classes, "-cp", cp, src], stdout=lf, stderr=subprocess.STDOUT).returncode
        if rc != 0:
            die(f"helper 컴파일 실패 (exit {rc}); {log} 참조", 5)
        with open(stamp, "w") as f:
            f.write(digest)
    return jh, classes + os.pathsep + run_cp, rt


def run_helper(args, p, helper_args, capture=False):
    jh, cp, rt = ensure_helper(args, p)
    os.makedirs(p["logs"], exist_ok=True)
    cmd = [os.path.join(jh, "bin", "java"), "-Xmx256m", *rt["log_opts"](p["logs"], "helper.log", "warn"),
           "-cp", cp, HELPER_CLASS] + [str(a) for a in helper_args]
    if capture:
        return subprocess.run(cmd, capture_output=True, text=True)
    return subprocess.run(cmd)


# ---------------------------------------------------------------- start / stop

def write_config(p, port, cport, rt):
    if rt["dynamic_quorum"]:
        quorum = f"controller.quorum.bootstrap.servers=127.0.0.1:{cport}"
        advertised = f"PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{cport}"
    else:
        quorum = f"controller.quorum.voters=1@127.0.0.1:{cport}"
        advertised = f"PLAINTEXT://127.0.0.1:{port}"
    cfg = f"""# generated by scripts/kafka-local.py (Kafka {rt['kafka_version']}, KRaft single node, loopback only)
process.roles=broker,controller
node.id=1
{quorum}
listeners=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{cport}
advertised.listeners={advertised}
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs={p['data']}
metadata.log.dir={p['data']}
num.partitions=1
default.replication.factor=1
offsets.topic.replication.factor=1
offsets.topic.num.partitions=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
log.retention.check.interval.ms=300000
num.network.threads=2
num.io.threads=4
num.recovery.threads.per.data.dir=1
"""
    with open(p["config"], "w", encoding="utf-8") as f:
        f.write(cfg)


def cmd_start(args):
    p = paths(args.dir)
    os.makedirs(p["data"], exist_ok=True)
    os.makedirs(p["logs"], exist_ok=True)
    if os.path.isfile(p["pid"]):
        try:
            old = int(open(p["pid"]).read().strip())
        except ValueError:
            old = -1
        if old > 0 and pid_alive(old) and pid_is_our_broker(old, p["config"]):
            die(f"이미 broker 가 실행 중입니다 (pid {old}). 먼저 stop 하세요.", 6)
        os.remove(p["pid"])

    jh = java_home(args)
    rt = resolve_runtime(args, p)
    cp = os.pathsep.join(rt["broker_cp"])
    with open(os.path.join(p["logs"], "classpath.txt"), "w") as f:
        f.write(f"# mode={rt['mode']} kafka_version={rt['kafka_version']} kafka_home={rt['kafka_home']}\n")
        for path in rt["broker_cp"]:
            f.write(path + "\n")

    port = args.port or free_port()
    cport = args.controller_port or free_port()
    while cport == port:
        cport = free_port()
    if port_open(port):
        die(f"포트 {port} 가 이미 사용 중입니다.", 6)
    write_config(p, port, cport, rt)

    java = os.path.join(jh, "bin", "java")
    # cluster id: Kafka Uuid 문자열 형식(16 byte, URL-safe base64, padding 없음)
    if os.path.isfile(p["cluster_id"]):
        cluster_id = open(p["cluster_id"]).read().strip()
    else:
        cluster_id = base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip("=")
        with open(p["cluster_id"], "w") as f:
            f.write(cluster_id)
    log_opts = rt["log_opts"](p["logs"], "broker.log", args.log_level)
    # bin/kafka-storage.sh format 상응: kafka.tools.StorageTool 을 직접 실행(meta.properties 생성).
    # 4.0 은 --standalone(single-node dynamic quorum), 3.0.1 은 static voters.
    fmt_log = os.path.join(p["logs"], "storage-format.log")
    with open(fmt_log, "w") as lf:
        rc = subprocess.run([java, "-Xmx256m", *rt["log_opts"](p["logs"], "storage-format-log4j.log", "warn"),
                             "-cp", cp, "kafka.tools.StorageTool", "format", "--ignore-formatted",
                             "--cluster-id", cluster_id, "--config", p["config"], *rt["format_extra"]],
                            stdout=lf, stderr=subprocess.STDOUT).returncode
    if rc != 0:
        die(f"StorageTool format 실패 (exit {rc}); {fmt_log} 참조", 7)

    out = open(os.path.join(p["logs"], "broker.out"), "ab")
    proc = subprocess.Popen([java, "-Xms256m", "-Xmx" + args.heap, "-XX:+UseG1GC", *log_opts, "-cp", cp,
                             "kafka.Kafka", p["config"]],
                            stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                            start_new_session=True, cwd=p["dir"])
    out.close()
    with open(p["pid"], "w") as f:
        f.write(str(proc.pid))

    bootstrap = f"127.0.0.1:{port}"
    deadline = time.time() + args.timeout
    started_line = False
    while time.time() < deadline:
        if proc.poll() is not None:
            tail = tail_lines(os.path.join(p["logs"], "broker.log"), 30) + tail_lines(os.path.join(p["logs"], "broker.out"), 30)
            os.remove(p["pid"])
            die(f"broker 가 기동 중 종료됨 (exit {proc.returncode}).\n" + tail, 8)
        if not started_line:
            started_line = "Kafka Server started" in read_text(os.path.join(p["logs"], "broker.log"))
        if started_line and port_open(port):
            break
        time.sleep(0.3)
    else:
        stop_pid(proc.pid, p, 10)
        die("broker 기동 대기 시간 초과 (Kafka Server started 미확인).\n" + tail_lines(os.path.join(p["logs"], "broker.log"), 40), 8)

    remain = max(5, int(deadline - time.time()))
    r = run_helper(args, p, ["ready", bootstrap, remain * 1000], capture=True)
    if r.returncode != 0:
        stop_pid(proc.pid, p, 10)
        die("AdminClient 준비 확인 실패:\n" + r.stderr + tail_lines(os.path.join(p["logs"], "broker.log"), 40), 8)
    ready = {
        "bootstrap": bootstrap, "port": port, "controller_port": cport, "pid": proc.pid,
        "dir": p["dir"], "mode": "kraft", "quorum": "dynamic" if rt["dynamic_quorum"] else "static",
        "source": rt["mode"], "kafka_home": rt["kafka_home"], "kafka_version": rt["kafka_version"],
        "clients_version": CLIENTS_VERSION, "java_home": jh, "cluster_id": cluster_id,
        "admin": json.loads(r.stdout.strip().splitlines()[-1]),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    with open(p["ready"], "w", encoding="utf-8") as f:
        json.dump(ready, f, indent=2, ensure_ascii=False)
    print(json.dumps(ready, ensure_ascii=False))


def read_text(path):
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read()
    except OSError:
        return ""


def tail_lines(path, n):
    lines = read_text(path).splitlines()
    return "\n".join(lines[-n:]) + ("\n" if lines else "")


def stop_pid(pid, p, timeout):
    """SIGTERM → timeout 동안 대기 → SIGKILL. 종료 여부를 돌려준다."""
    if not pid_alive(pid):
        return "already-exited"
    if not pid_is_our_broker(pid, p["config"]):
        return "pid-mismatch"
    os.kill(pid, signal.SIGTERM)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not pid_alive(pid):
            return "terminated"
        time.sleep(0.2)
    os.kill(pid, signal.SIGKILL)
    deadline = time.time() + 5
    while time.time() < deadline:
        if not pid_alive(pid):
            return "killed"
        time.sleep(0.2)
    return "still-alive"


def cmd_stop(args):
    p = paths(args.dir)
    if not os.path.isfile(p["pid"]):
        print(json.dumps({"stopped": False, "reason": "no pid file"}))
        return
    pid = int(open(p["pid"]).read().strip())
    result = stop_pid(pid, p, args.timeout)
    if result in ("terminated", "killed", "already-exited"):
        os.remove(p["pid"])
        if os.path.isfile(p["ready"]):
            os.remove(p["ready"])
    print(json.dumps({"stopped": result in ("terminated", "killed", "already-exited"), "pid": pid, "result": result}))
    if result in ("still-alive", "pid-mismatch"):
        sys.exit(9)


def cmd_status(args):
    p = paths(args.dir)
    info = {"dir": p["dir"], "running": False}
    if os.path.isfile(p["pid"]):
        pid = int(open(p["pid"]).read().strip())
        info["pid"] = pid
        info["running"] = pid_alive(pid) and pid_is_our_broker(pid, p["config"])
    if os.path.isfile(p["ready"]):
        info["ready"] = read_ready(p)
    print(json.dumps(info, ensure_ascii=False))


# ---------------------------------------------------------------- helper commands

def cmd_topic_create(args):
    p = paths(args.dir)
    ready = read_ready(p)
    r = run_helper(args, p, ["topic-create", ready["bootstrap"], args.name, args.partitions, args.replication_factor])
    sys.exit(r.returncode)


def cmd_produce(args):
    p = paths(args.dir)
    ready = read_ready(p)
    r = run_helper(args, p, ["produce", ready["bootstrap"], args.topic, os.path.abspath(args.spec)])
    sys.exit(r.returncode)


def cmd_consume(args):
    p = paths(args.dir)
    ready = read_ready(p)
    r = run_helper(args, p, ["consume", ready["bootstrap"], args.topic, "true" if args.from_beginning else "false",
                             args.max, int(args.timeout * 1000)])
    sys.exit(r.returncode)


def produce_test(args, p, topic, partitions):
    """헤더 3개·8-byte big-endian key·partition 지정 record 를 보내고 consume 으로 동일 bytes 를 확인한다."""
    ready = read_ready(p)
    bootstrap = ready["bootstrap"]
    records = []
    # Pulse 계약과 유사한 형태: key 8 byte big-endian, header 3개(application 은 UUID 가 아닌 문자열), 명시 partition
    for i, part in enumerate(sorted({0, partitions // 2, partitions - 1})):
        key = struct.pack(">q", 0x0102030405060708 + i)
        value = bytes([0x00, 0xff, 0x7f, 0x80]) + f'{{"seq":{i},"txid":{0x0102030405060708 + i}}}'.encode() + bytes([0x0a, 0x00])
        headers = [("application", b"scouter-go-selftest"), ("type", f"xlog-{i}".encode()), ("bin", bytes(range(0, 256, 17)))]
        records.append({"partition": part, "key": key, "value": value, "headers": headers})
    spec = os.path.join(p["helper"], "produce-test.tsv")
    os.makedirs(p["helper"], exist_ok=True)
    with open(spec, "w", encoding="utf-8") as f:
        for r in records:
            hdr = ";".join(f"{k.encode().hex()}={v.hex()}" for k, v in r["headers"])
            f.write(f"{r['partition']}\t{r['key'].hex()}\t{r['value'].hex()}\t{hdr}\n")
    pr = run_helper(args, p, ["produce", bootstrap, topic, spec], capture=True)
    if pr.returncode != 0:
        print(pr.stderr, file=sys.stderr)
        return False, "produce failed"
    metas = [json.loads(l) for l in pr.stdout.splitlines() if l.strip()]
    if [m["partition"] for m in metas] != [r["partition"] for r in records]:
        return False, f"produce partition mismatch: {metas}"

    cr = run_helper(args, p, ["consume", bootstrap, topic, "true", len(records), 20000], capture=True)
    if cr.returncode != 0:
        print(cr.stderr, file=sys.stderr)
        return False, "consume failed or fewer records than expected"
    got = [json.loads(l) for l in cr.stdout.splitlines() if l.strip()]
    expect = sorted((r["partition"], r["key"].hex(), r["value"].hex(), [(k, v.hex()) for k, v in r["headers"]]) for r in records)
    actual = sorted((g["partition"], g["key_hex"], g["value_hex"], [(h["key"], h["value_hex"]) for h in g["headers"]]) for g in got)
    if expect != actual:
        return False, f"byte mismatch\n expect={expect}\n actual={actual}"
    return True, {"topic": topic, "records": len(records), "partitions": [r["partition"] for r in records],
                  "offsets": [m["offset"] for m in metas], "consumed": got}


def cmd_produce_test(args):
    p = paths(args.dir)
    topic = args.topic or f"kafka-local-selftest-{int(time.time())}"
    if not args.topic:
        r = run_helper(args, p, ["topic-create", read_ready(p)["bootstrap"], topic, args.partitions, 1], capture=True)
        if r.returncode != 0:
            die("topic-create 실패:\n" + r.stderr, 10)
    ok, detail = produce_test(args, p, topic, args.partitions)
    print(json.dumps({"produce_test": "pass" if ok else "fail", "detail": detail}, ensure_ascii=False))
    sys.exit(0 if ok else 11)


def cmd_selftest(args):
    p = paths(args.dir)
    cmd_start(args)
    ok, detail = False, "not run"
    try:
        topic = f"kafka-local-selftest-{int(time.time())}"
        r = run_helper(args, p, ["topic-create", read_ready(p)["bootstrap"], topic, args.partitions, 1], capture=True)
        if r.returncode != 0:
            detail = "topic-create 실패:\n" + r.stderr
        else:
            print(r.stdout.strip())
            ok, detail = produce_test(args, p, topic, args.partitions)
    finally:
        pid = int(open(p["pid"]).read().strip())
        result = stop_pid(pid, p, args.stop_timeout)
        if result in ("terminated", "killed", "already-exited"):
            os.remove(p["pid"])
            if os.path.isfile(p["ready"]):
                os.remove(p["ready"])
        print(json.dumps({"stop": result, "pid": pid}))
        if result not in ("terminated", "killed"):
            ok, detail = False, f"stop failed ({result}); produce-test={detail}"
    print(json.dumps({"selftest": "pass" if ok else "fail", "detail": detail}, ensure_ascii=False))
    sys.exit(0 if ok else 11)


def read_jsonl(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def cmd_compare(args):
    """기대(Go test 가 쓴 JSON lines)와 실제(consume 출력)를 partition 별 순서까지 비교한다."""
    def by_partition(rows):
        out = {}
        for r in rows:
            out.setdefault(r["partition"], []).append(
                (r["key_hex"], r["value_hex"], [(h["key"], h["value_hex"]) for h in r.get("headers") or []]))
        return out
    expect, actual = read_jsonl(args.expect), read_jsonl(args.actual)
    e, a = by_partition(expect), by_partition(actual)
    ok = e == a
    detail = {"expected": len(expect), "actual": len(actual), "partitions": len(e)}
    if not ok:
        for part in sorted(set(e) | set(a)):
            if e.get(part) != a.get(part):
                detail["first_mismatch_partition"] = part
                detail["expected_count"] = len(e.get(part, []))
                detail["actual_count"] = len(a.get(part, []))
                break
    print(json.dumps({"compare": "pass" if ok else "fail", **detail}, ensure_ascii=False))
    sys.exit(0 if ok else 12)


def cmd_dump_log(args):
    """topic 의 모든 segment 를 kafka.tools.DumpLogSegments 로 읽어 batch 별 compresscodec 을 센다(읽기 전용)."""
    p = paths(args.dir)
    read_ready(p)
    rt = resolve_runtime(args, p)
    jh = java_home(args)
    segments = []
    for name in sorted(os.listdir(p["data"])):
        if name.startswith(args.topic + "-") and name[len(args.topic) + 1:].isdigit():
            part_dir = os.path.join(p["data"], name)
            segments += sorted(os.path.join(part_dir, f) for f in os.listdir(part_dir) if f.endswith(".log"))
    if not segments:
        die(f"topic {args.topic} 의 segment 가 없습니다.", 13)
    cmd = [os.path.join(jh, "bin", "java"), "-Xmx256m", *rt["log_opts"](p["logs"], "dump-log.log", "warn"),
           "-cp", os.pathsep.join(rt["broker_cp"]), "kafka.tools.DumpLogSegments", "--files", ",".join(segments)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        die("DumpLogSegments 실패:\n" + r.stderr, 13)
    codecs, records = {}, 0
    for line in r.stdout.splitlines():
        if "baseOffset:" in line and "compresscodec:" in line:
            codec = line.split("compresscodec:", 1)[1].split()[0]
            codecs[codec] = codecs.get(codec, 0) + 1
            if "count:" in line:
                records += int(line.split("count:", 1)[1].split()[0])
    print(json.dumps({"topic": args.topic, "segments": len(segments), "batches": sum(codecs.values()),
                      "records": records, "codecs": codecs}, ensure_ascii=False))


def cmd_classpath(args):
    home = kafka_home(args)
    if home:
        rt = resolve_runtime(args)
        print(f"[dist] kafka_home={home} kafka_version={rt['kafka_version']} java_home={java_home(args)} jars={len(rt['broker_cp'])}")
        for path in rt["broker_cp"]:
            print("  " + path)
        print(f"[helper] compile_cp={rt['helper_compile_cp']}")
        sys.exit(0)
    cache = gradle_cache(args)
    for label, arts in (("broker", BROKER_ARTIFACTS), ("helper", HELPER_ARTIFACTS)):
        jars, missing = resolve_classpath(cache, arts)
        print(f"[{label}] java_home={java_home(args)} cache={cache}")
        for coord, path in jars:
            print(f"  {coord}\t{path}")
        for m in missing:
            print(f"  MISSING {m}")
    sys.exit(0)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--java-home", help=f"JDK 경로 (기본: KAFKA_LOCAL_JAVA_HOME, {DEFAULT_JAVA_HOME}, JAVA_HOME 순)")
    ap.add_argument("--kafka-home", help="풀어 둔 Kafka binary 배포본 경로 (기본: KAFKA_LOCAL_HOME). libs/*.jar 를 classpath 로 쓴다. 없으면 Gradle 캐시 fallback")
    ap.add_argument("--gradle-cache", help="(fallback) Gradle 캐시 files-2.1 경로 (기본: ~/.gradle/caches/modules-2/files-2.1)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add_start_opts(s):
        s.add_argument("--port", type=int, default=0, help="broker PLAINTEXT 포트 (기본: 임의)")
        s.add_argument("--controller-port", type=int, default=0, help="KRaft controller 포트 (기본: 임의)")
        s.add_argument("--timeout", type=float, default=90, help="기동 대기 초")
        s.add_argument("--heap", default="512m", help="broker -Xmx")
        s.add_argument("--log-level", default="info", choices=["trace", "debug", "info", "warn", "error"])

    s = sub.add_parser("start"); s.add_argument("--dir", required=True); add_start_opts(s); s.set_defaults(fn=cmd_start)
    s = sub.add_parser("stop"); s.add_argument("--dir", required=True); s.add_argument("--timeout", type=float, default=30); s.set_defaults(fn=cmd_stop)
    s = sub.add_parser("status"); s.add_argument("--dir", required=True); s.set_defaults(fn=cmd_status)
    s = sub.add_parser("topic-create"); s.add_argument("--dir", required=True); s.add_argument("--name", required=True)
    s.add_argument("--partitions", type=int, required=True); s.add_argument("--replication-factor", type=int, default=1); s.set_defaults(fn=cmd_topic_create)
    s = sub.add_parser("produce"); s.add_argument("--dir", required=True); s.add_argument("--topic", required=True); s.add_argument("--spec", required=True); s.set_defaults(fn=cmd_produce)
    s = sub.add_parser("consume"); s.add_argument("--dir", required=True); s.add_argument("--topic", required=True)
    s.add_argument("--from-beginning", action="store_true"); s.add_argument("--max", type=int, default=1); s.add_argument("--timeout", type=float, default=20); s.set_defaults(fn=cmd_consume)
    s = sub.add_parser("produce-test"); s.add_argument("--dir", required=True); s.add_argument("--topic", help="기존 topic (없으면 새로 만든다)")
    s.add_argument("--partitions", type=int, default=30); s.set_defaults(fn=cmd_produce_test)
    s = sub.add_parser("selftest"); s.add_argument("--dir", required=True); add_start_opts(s)
    s.add_argument("--partitions", type=int, default=30); s.add_argument("--stop-timeout", type=float, default=30); s.set_defaults(fn=cmd_selftest)
    s = sub.add_parser("classpath"); s.set_defaults(fn=cmd_classpath)
    s = sub.add_parser("compare"); s.add_argument("--expect", required=True); s.add_argument("--actual", required=True); s.set_defaults(fn=cmd_compare)
    s = sub.add_parser("dump-log"); s.add_argument("--dir", required=True); s.add_argument("--topic", required=True); s.set_defaults(fn=cmd_dump_log)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
