#!/usr/bin/env python3
"""고정 Java/Scala 원본 Index oracle. 원본은 읽기만 하고 classes는 임시 경로에 작성한다."""
import argparse
import os
from pathlib import Path
import subprocess
import tempfile

SHA = "dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5"
ROOT = Path(__file__).resolve().parents[4]


def run(args):
    result = subprocess.run([str(x) for x in args], capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout


def compile_classes(source, work):
    source, work = Path(source).resolve(), Path(work).resolve()
    if run(["git", "-C", source, "rev-parse", "HEAD"]).strip() != SHA:
        raise RuntimeError("source SHA mismatch")
    if run(["git", "-C", source, "status", "--porcelain", "--", "scouter.common", "scouter.server/src/main/scala/scouter/server/db/io", "scouter.server/src/main/java/scouter/server/db/io", "scouter.server/src/main/scala/scouter/server/util/EnumerScala.scala", "scouter.deploy/lib/scala"]):
        raise RuntimeError("source modified; oracle rejected")
    classes = work / "classes"
    classes.mkdir(parents=True, exist_ok=True)
    stubs = work / "stubs/scouter/server"
    stubs.mkdir(parents=True, exist_ok=True)
    # Only logging/configuration are isolated. All storage/codec/hash/date classes are original.
    (stubs / "Configure.java").write_text("package scouter.server; public class Configure { public int log_index_traversal_warning_count=100; public static Configure getInstance(){return new Configure();} }")
    (stubs / "Logger.java").write_text("package scouter.server; public class Logger { public static void println(String a,String b){} public static void println(String a,int b,String c){} }")
    java = sorted((source / "scouter.common/src/main/java").rglob("*.java"))
    java += list(stubs.glob("*.java"))
    java += [source / "scouter.server/src/main/java/scouter/server/db/io" / f for f in ["MemHashBlock.java", "MemTimeBlock.java"]]
    argfile = work / "java-sources.txt"
    argfile.write_text("\n".join('"' + str(p).replace("\\", "\\\\").replace('"', '\\"') + '"' for p in java))
    run(["javac", "--release", "8", "-encoding", "UTF-8", "-d", classes, "@" + str(argfile)])
    scala = source / "scouter.deploy/lib/scala"
    library = scala / "scala-library.jar"
    compiler_cp = os.pathsep.join(str(p) for p in [scala / "scala-compiler.jar", library, scala / "scala-reflect.jar", classes])
    cp = str(classes) + os.pathsep + str(library)
    files = [source / "scouter.server/src/main/scala/scouter/server/db/io" / f for f in ["RealKeyFile.scala", "IndexKeyFile.scala", "IndexTimeFile.scala"]]
    files += [source / "scouter.server/src/main/scala/scouter/server/util/EnumerScala.scala"]
    scala_java = os.environ.get("SCOUTER_SCALA_JAVA")
    if not scala_java and Path("/usr/libexec/java_home").exists():
        java8 = subprocess.run(["/usr/libexec/java_home", "-v", "1.8"], capture_output=True, text=True)
        scala_java = str(Path(java8.stdout.strip()) / "bin/java") if java8.returncode == 0 else "java"
    run([scala_java or "java", "-Dscala.usejavacp=true", "-cp", compiler_cp, "scala.tools.nsc.Main", "-usejavacp", "-classpath", cp, "-d", classes, *files])
    return cp


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path(os.environ.get("SCOUTER_SOURCE", ROOT.parent / "scouter")))
    parser.add_argument("--classes-dir", type=Path, help="이 디렉터리에 classes를 보존하고 classpath를 출력")
    parser.add_argument("--mode", default="generate", choices=["generate", "verify"])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.classes_dir:
        print(compile_classes(args.source, args.classes_dir))
        return
    if args.output is None:
        parser.error("--output required")
    with tempfile.TemporaryDirectory(prefix="scouter-index-oracle-") as work:
        cp = compile_classes(args.source, work)
        run(["javac", "--release", "8", "-cp", cp, "-d", Path(work) / "classes", Path(__file__).with_name("IndexOracle.java")])
        print(run(["java", "-Duser.timezone=Asia/Seoul", "-cp", cp, "IndexOracle", args.mode, args.output]), end="")


if __name__ == "__main__":
    main()
