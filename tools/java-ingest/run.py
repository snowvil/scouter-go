#!/usr/bin/env python3
"""고정 원본 PodSeqManager·ObjectCpuChecker·GeoIpUtil을 수정 없이 컴파일·실행해 ingest golden을 만들거나 비교한다.

원본 source tree는 읽기만 한다. 공통 class는 index oracle(internal/storage/legacyindex/testdata/run_oracle.py)과
같은 방식으로 임시 디렉터리에 컴파일하고, server 원본 3개와 ThreadScala만 seam(tools/java-ingest/seams)과 함께 붙인다.
사용법은 tools/java-ingest/README.md.
"""
import argparse
import difflib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / "tools/java-ingest"
DEST = ROOT / "internal/ingest/testdata/java-oracle"
SCENARIOS = ["kube", "cpu", "geoip", "alerts"]
TIMEZONE = "Asia/Seoul"
SERVER = "scouter.server/src/main"
SERVER_JAVA = ["java/scouter/server/kube/PodSeqManager.java"]
SERVER_SCALA = [
    "scala/scouter/server/core/app/ObjectCpuChecker.scala",
    "scala/scouter/server/geoip/GeoIpUtil.scala",
    "scala/scouter/server/util/ThreadScala.scala",
    "scala/scouter/server/core/AgentManager.scala",
    "scala/scouter/server/core/ObjectMap.scala",
]
RESOURCES = ["scouter.common/src/main/resources"]
MAXMIND = "scouter.deploy/lib/maxmind.geoip.jar"

spec = importlib.util.spec_from_file_location("index_oracle", ROOT / "internal/storage/legacyindex/testdata/run_oracle.py")
index_oracle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(index_oracle)


def run(args, cwd=None, check=True):
    result = subprocess.run([str(x) for x in args], cwd=cwd, capture_output=True, text=True, env={"PATH": os.environ.get("PATH", "")})
    if check and result.returncode:
        raise RuntimeError("command failed: " + " ".join(map(str, args))[:400] + " ...\n" + result.stdout[-8000:] + result.stderr[-8000:])
    return result


def java8_home():
    explicit = os.environ.get("SCOUTER_JAVA8_HOME")
    if explicit:
        return Path(explicit)
    if Path("/usr/libexec/java_home").exists():
        result = subprocess.run(["/usr/libexec/java_home", "-v", "1.8"], capture_output=True, text=True)
        if result.returncode == 0:
            return Path(result.stdout.strip())
    raise RuntimeError("JDK 8 is required (set SCOUTER_JAVA8_HOME); the Scala 2.10 compiler runs on JDK 8 only")


def compile_oracle(source, work):
    source, work = Path(source).resolve(), Path(work).resolve()
    jdk8 = java8_home()
    cp = index_oracle.compile_classes(source, work)
    rels = [SERVER + "/" + r for r in SERVER_JAVA + SERVER_SCALA]
    if run(["git", "-C", source, "status", "--porcelain", "--", *rels, *RESOURCES, MAXMIND]).stdout:
        raise RuntimeError("ingest oracle source modified; oracle rejected")
    classes = work / "classes"
    full_cp = os.pathsep.join([cp, str(source / MAXMIND)])
    seams_java = sorted((TOOLS / "seams").rglob("*.java"))
    seams_scala = sorted((TOOLS / "seams").rglob("*.scala"))
    java_src = [source / SERVER / r for r in SERVER_JAVA]
    scala_src = [source / SERVER / r for r in SERVER_SCALA]
    oracle = [TOOLS / "IngestOracle.java"]
    jars = source / "scouter.deploy/lib/scala"
    compiler_cp = os.pathsep.join(str(p) for p in [jars / "scala-compiler.jar", jars / "scala-library.jar", jars / "scala-reflect.jar", classes])
    # mixed compile: scalac parses the Java sources and emits the Scala classes, then javac compiles the Java.
    run([jdk8 / "bin/java", "-XX:ActiveProcessorCount=2", "-Dscala.usejavacp=true", "-cp", compiler_cp,
         "scala.tools.nsc.Main", "-usejavacp", "-classpath", full_cp, "-d", classes, *scala_src, *seams_scala, *seams_java, *java_src, *oracle])
    run([jdk8 / "bin/javac", "-source", "8", "-target", "8", "-encoding", "UTF-8", "-nowarn", "-cp", full_cp + os.pathsep + str(classes),
         "-d", classes, *seams_java, *java_src, *oracle])
    return os.pathsep.join([str(classes), full_cp, *[str(source / r) for r in RESOURCES]])


def generate(source, out, scenarios):
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="scouter-ingest-oracle-") as work:
        runtime_cp = compile_oracle(source, Path(work))
        jdk8 = java8_home()
        for scenario in scenarios:
            # One JVM per scenario: PodSeqManager/ObjectCpuChecker/GeoIpUtil state is static.
            result = run([jdk8 / "bin/java", "-XX:ActiveProcessorCount=2", f"-Duser.timezone={TIMEZONE}", "-Dfile.encoding=UTF-8",
                          "-cp", runtime_cp, "IngestOracle", scenario, out.resolve()], cwd=work)
            print(result.stdout.strip())
        java_version = run([jdk8 / "bin/java", "-version"]).stderr.strip().splitlines()
    manifest = {
        "source_commit": index_oracle.SHA,
        "source_version": "2.21.3",
        "synthetic_only": True,
        "generator": "python3 tools/java-ingest/run.py --generate internal/ingest/testdata/java-oracle",
        "check": "python3 tools/java-ingest/run.py --check",
        "readme": "tools/java-ingest/README.md",
        "runtime_jdk": java_version,
        "original_classes": [SERVER + "/" + r for r in SERVER_JAVA + SERVER_SCALA] + ["scouter.common (all)", MAXMIND],
        "scenarios": scenarios,
    }
    (out / "oracle-manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")


def check(source):
    manifest = json.loads((DEST / "oracle-manifest.json").read_text())
    with tempfile.TemporaryDirectory(prefix="scouter-ingest-oracle-check-") as tmp:
        tmp = Path(tmp)
        generate(source, tmp, manifest["scenarios"])
        failed = False
        for name in sorted({p.name for p in DEST.iterdir()} | {p.name for p in tmp.iterdir()}):
            if name == "oracle-manifest.json":
                continue  # runtime_jdk may differ by machine
            a, b = DEST / name, tmp / name
            if not a.exists() or not b.exists() or a.read_bytes() != b.read_bytes():
                failed = True
                print(f"DIFF {name}")
                if a.exists() and b.exists():
                    sys.stdout.writelines(list(difflib.unified_diff(a.read_text().splitlines(True), b.read_text().splitlines(True), str(a), str(b)))[:80])
        if failed:
            sys.exit(1)
        print("java-ingest oracle: golden matches")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path(os.environ.get("SCOUTER_SOURCE", ROOT.parent / "scouter")))
    parser.add_argument("--generate", type=Path, help="빈 디렉터리 또는 internal/ingest/testdata/java-oracle")
    parser.add_argument("--scenario", action="append", choices=SCENARIOS)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        check(args.source)
    elif args.generate:
        generate(args.source, args.generate, args.scenario or SCENARIOS)
    else:
        parser.print_help()
        sys.exit(2)


if __name__ == "__main__":
    main()
