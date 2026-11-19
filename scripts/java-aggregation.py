#!/usr/bin/env python3
"""고정 실제 HLL, StringSet, TagConfig 및 First/Next/Top100 클래스 oracle."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("aggregation_index", ROOT / "internal/storage/legacyindex/testdata/run_oracle.py")
index = importlib.util.module_from_spec(spec)
spec.loader.exec_module(index)

def guard(source):
    if index.run(["git", "-C", source, "rev-parse", "HEAD"]).strip() != index.SHA:
        raise RuntimeError("aggregation source SHA mismatch")
    records = index.run(["git", "-C", source, "ls-tree", "-r", "HEAD", "--", "scouter.common", "scouter.server", "scouter.deploy/lib/scala"])
    manifest = []
    for record in records.splitlines():
        meta, name = record.split("\t", 1)
        mode, kind, blob = meta.split()
        if kind != "blob":
            continue
        path = source / name
        data = os.readlink(path).encode() if mode == "120000" else path.read_bytes()
        actual = hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()
        if actual != blob:
            raise RuntimeError(f"original source differs from pinned blob: {name}")
        manifest.append({"path": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(), "git_blob": blob})
    return {"source_commit": index.SHA, "original_sources": manifest}

def compile_original(source, work):
    report = guard(source)
    cp = index.compile_classes(source, work)
    classes = work / "classes"
    java = list((source / "scouter.server/src/main/java/scouter/server/util/cardinality").glob("*.java"))
    java.append(source / "scouter.server/src/main/java/scouter/server/tagcnt/core/ValueCount.java")
    seams = {
        "scouter/server/db/DBCtr.java": 'package scouter.server.db; public class DBCtr {public static boolean running=false;public static String getRootPath(){return System.getProperty("oracle.root");}}',
        "scouter/server/core/CoreRun.java": 'package scouter.server.core; public class CoreRun {public static boolean running=false;}',
        "scouter/server/Logger.java": 'package scouter.server; public class Logger {public static void println(String a,String b){}public static void println(String a,int b,String c){}public static void println(String a,int b,String c,Throwable d){}}',
    }
    for name,code in seams.items():
        p=work / "aggregation-seams" / name
        p.parent.mkdir(parents=True,exist_ok=True)
        p.write_text(code)
        java.append(p)
    index.run(["javac", "--release", "8", "-encoding", "UTF-8", "-cp", cp, "-d", classes, *java])
    rels = ["tagcnt/TagCountConfig.scala", "tagcnt/core/TagCountUtil.scala", "tagcnt/core/ValueCountTotal.scala", "tagcnt/first/IndexFile.scala", "tagcnt/first/KeyDataFile.scala", "tagcnt/next/IndexFile.scala", "tagcnt/next/KeyFile.scala", "tagcnt/next/DataFile.scala"]
    rels += ["util/ThreadScala.scala", "db/VisitorDB.scala", "db/VisitorHourlyDB.scala"]
    files = [source / "scouter.server/src/main/scala/scouter/server" / name for name in rels]
    jars = source / "scouter.deploy/lib/scala"
    compiler = os.pathsep.join(str(p) for p in [jars / "scala-compiler.jar", jars / "scala-library.jar", jars / "scala-reflect.jar", classes])
    scala_java = os.environ.get("SCOUTER_SCALA_JAVA")
    if not scala_java and Path("/usr/libexec/java_home").exists():
        result = subprocess.run(["/usr/libexec/java_home", "-v", "1.8"], capture_output=True, text=True)
        if result.returncode == 0:
            scala_java = str(Path(result.stdout.strip()) / "bin/java")
    index.run([scala_java or "java", "-Dscala.usejavacp=true", "-cp", compiler, "scala.tools.nsc.Main", "-usejavacp", "-classpath", cp, "-d", classes, *files])
    index.run(["javac", "--release", "8", "-encoding", "UTF-8", "-cp", cp, "-d", classes, ROOT / "tools/java-aggregation/AggregationOracle.java"])
    report["harness_sha256"] = hashlib.sha256((ROOT / "tools/java-aggregation/AggregationOracle.java").read_bytes()).hexdigest()
    (work / "source-guard.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
    return cp

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT.parent / "scouter")
    parser.add_argument("--classes-dir", type=Path, required=True)
    args = parser.parse_args()
    print(compile_original(args.source.resolve(), args.classes_dir.resolve()))

if __name__ == "__main__":
    main()
