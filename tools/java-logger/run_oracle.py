#!/usr/bin/env python3
"""고정 원본 Logger.scala·RequestLogger.java·scouter.common Pack/Value toString을 임시 디렉터리에
수정 없이 컴파일·실행해 internal/logger/testdata/java golden을 생성하거나(--generate) 비교한다(--check).

원본 source tree는 읽기만 한다. Configure·LoginManager만 seam(tools/java-logger/seams)으로 바꾼다.
사용법은 tools/java-logger/README.md.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / "tools/java-logger"
DEST = ROOT / "internal/logger/testdata/java"
SERVER = "scouter.server/src/main"
SCALA_SOURCES = [
    "scala/scouter/server/Logger.scala",
    "scala/scouter/server/ConfObserver.scala",
    "scala/scouter/server/LoginUser.scala",
    "scala/scouter/server/util/ThreadScala.scala",
    "scala/scouter/server/util/EnumerScala.scala",
]
JAVA_SOURCES = ["java/scouter/server/logs/RequestLogger.java"]
TIMEZONE = "Asia/Seoul"
EXTRA_TIMEZONES = ["America/New_York"]
JVM_FLAGS = ["-XX:ActiveProcessorCount=2", "-Dfile.encoding=UTF-8", "-Duser.language=en", "-Duser.country=US"]
ORACLE_ENV = {"SCOUTER_ORACLE_ENV_MARKER": "synthetic"}


def run(args, cwd=None, env=None):
    result = subprocess.run([str(a) for a in args], cwd=cwd, env=env, capture_output=True, text=True)
    if result.returncode:
        raise SystemExit("command failed: " + " ".join(map(str, args))[:300] + "\n" + result.stdout[-6000:] + result.stderr[-6000:])
    return result


def java8_home():
    explicit = os.environ.get("SCOUTER_JAVA8_HOME")
    if explicit:
        return Path(explicit)
    if Path("/usr/libexec/java_home").exists():
        r = subprocess.run(["/usr/libexec/java_home", "-v", "1.8"], capture_output=True, text=True)
        if r.returncode == 0:
            return Path(r.stdout.strip())
    raise SystemExit("JDK 8 is required (set SCOUTER_JAVA8_HOME)")


def sha256(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def argfile(path, files):
    path.write_text("\n".join('"' + str(p).replace("\\", "\\\\").replace('"', '\\"') + '"' for p in files))
    return "@" + str(path)


def generate(source, out):
    baseline = json.loads((ROOT / "docs/compatibility/baseline.json").read_text())["scouter"]
    sha = run(["git", "-C", source, "rev-parse", "HEAD"]).stdout.strip()
    if sha != baseline["commit"]:
        raise SystemExit(f"source SHA mismatch: expected {baseline['commit']}, got {sha}")
    rels = ["scouter.common", "scouter.deploy/lib/scala"] + [SERVER + "/" + r for r in SCALA_SOURCES + JAVA_SOURCES]
    if run(["git", "-C", source, "status", "--porcelain", "--", *rels]).stdout:
        raise SystemExit("pinned source is modified; oracle rejected")
    jdk = java8_home()
    version = subprocess.run([jdk / "bin/java", "-version"], capture_output=True, text=True).stderr.splitlines()
    with tempfile.TemporaryDirectory(prefix="scouter-logger-oracle-") as tmp:
        tmp = Path(tmp)
        classes = tmp / "classes"
        classes.mkdir()
        common = sorted((source / "scouter.common/src/main/java").rglob("*.java"))
        run([jdk / "bin/javac", "-source", "8", "-target", "8", "-nowarn", "-encoding", "UTF-8", "-d", classes,
             argfile(tmp / "common.txt", common)])
        scala = source / "scouter.deploy/lib/scala"
        compiler_cp = os.pathsep.join(str(scala / j) for j in ["scala-compiler.jar", "scala-library.jar", "scala-reflect.jar"])
        cp = os.pathsep.join([str(classes), str(scala / "scala-library.jar")])
        seams = sorted((TOOLS / "seams").rglob("*.java"))
        java_src = [source / SERVER / r for r in JAVA_SOURCES]
        scala_src = [source / SERVER / r for r in SCALA_SOURCES]
        # mixed compile: scalac parses the Java seams/RequestLogger, javac then compiles them.
        run([jdk / "bin/java", "-Dscala.usejavacp=true", "-cp", compiler_cp, "scala.tools.nsc.Main", "-usejavacp",
             "-classpath", cp, "-d", classes, *scala_src, *seams, *java_src])
        oracles = sorted(TOOLS.glob("*.java"))
        run([jdk / "bin/javac", "-source", "8", "-target", "8", "-nowarn", "-encoding", "UTF-8", "-cp", cp, "-d", classes,
             *seams, *java_src, *oracles])
        java = [jdk / "bin/java", *JVM_FLAGS]
        run([*java, f"-Duser.timezone={TIMEZONE}", "-cp", cp, "PackStringOracle", out, "packs"], env=ORACLE_ENV)
        for tz in [TIMEZONE, *EXTRA_TIMEZONES]:
            run([*java, f"-Duser.timezone={tz}", "-cp", cp, "PackStringOracle", out, "time"], env=ORACLE_ENV)
        work = tmp / "work"
        work.mkdir()
        run([*java, f"-Duser.timezone={TIMEZONE}", "-cp", cp, "LoggerOracle", out, work], cwd=work, env=ORACLE_ENV)
    goldens = sorted(p for p in out.iterdir() if p.name != "manifest.json")
    for p in goldens:
        text = p.read_text(encoding="utf-8")
        for leak in {str(Path.home()), str(tmp), os.environ.get("USER", "") or "\0"}:
            if leak and leak in text:
                raise SystemExit(f"environment leak in {p.name}")
    manifest = {
        "source_commit": sha,
        "source_version": baseline["version"],
        "synthetic_only": True,
        "jdk": version[0] if version else "",
        "command": "python3 tools/java-logger/run_oracle.py --generate",
        "compiled_original": {SERVER + "/" + r: sha256(source / SERVER / r) for r in SCALA_SOURCES + JAVA_SOURCES},
        "oracle_sources": {str(p.relative_to(ROOT)): sha256(p) for p in sorted(TOOLS.rglob("*.java"))},
        "files": [{"name": p.name, "bytes": p.stat().st_size, "sha256": sha256(p)} for p in goldens],
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--generate", action="store_true", help="internal/logger/testdata/java를 다시 만든다")
    mode.add_argument("--check", action="store_true", help="임시 디렉터리에 다시 만들어 committed golden과 byte 비교")
    args = parser.parse_args()
    source = Path(os.environ.get("SCOUTER_SOURCE", ROOT.parent / "scouter")).resolve()
    with tempfile.TemporaryDirectory(prefix="scouter-logger-golden-") as tmp:
        out = Path(tmp)
        generate(source, out)
        if args.check:
            expected = {p.name: p.read_bytes() for p in DEST.glob("*") if p.is_file()}
            got = {p.name: p.read_bytes() for p in out.iterdir()}
            changed = sorted(k for k in expected.keys() | got.keys() if expected.get(k) != got.get(k))
            if changed:
                raise SystemExit("golden drift: " + ", ".join(changed))
            print(f"PASS: {len(got)} logger golden files, source {json.loads(got['manifest.json'])['source_commit']}")
            return
        DEST.mkdir(parents=True, exist_ok=True)
        for p in DEST.iterdir():
            if p.is_file():
                p.unlink()
        for p in out.iterdir():
            shutil.copyfile(p, DEST / p.name)
        print(f"Generated {len(list(out.iterdir()))} logger golden files in {DEST.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
