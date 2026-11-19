#!/usr/bin/env python3
"""고정 원본 관리 명령 handler(계정·설정·alert·관리·AgentInfo·ThreadList)와 Telegraf/HTTP 입력 handler를
임시 디렉터리에 컴파일·직접 실행하여 golden(request/response bytes, JSON 관측값)을 생성/비교한다.

원본 source tree는 읽기만 한다. scenario마다 별도 JVM과 별도 작업 디렉터리(`./conf`, plugin_dir, db_dir)를
사용하므로 원본 singleton(Configure, CounterManager, AccountManager, RemoteControlManager)의 상태가
scenario 사이에 섞이지 않는다. 사용법·seam 목록은 tools/java-management/README.md.
"""
import argparse
import difflib
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("index_oracle", ROOT / "internal/storage/legacyindex/testdata/run_oracle.py")
index_oracle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(index_oracle)

TOOLS = ROOT / "tools/java-management"
DEST = ROOT / "internal/service/testdata/management"
TIMEZONE = "Asia/Seoul"
# The original ${...} substitution is defective (management-commands.md §3.1). The oracle JVM therefore
# runs with this synthetic environment only; the marker lets the oracle detect and redact the affected values.
ORACLE_ENV = {"SCOUTER_ORACLE_ENV_MARKER": "synthetic"}
SCENARIOS = ["account", "configure", "counters", "alert", "manage", "agentinfo", "threadlist",
             "telegraf-fresh", "telegraf", "telegraf-legacy", "counterhttp", "register", "descriptors", "telegraf-golden",
             "login", "xlogbounds"]
TELEGRAF_GOLDEN = ROOT / "internal/telegraf/testdata/golden_packs.json"
TELEGRAF_GOLDEN_TEST = ROOT / "internal/telegraf/golden_test.go"
SERVER = "scouter.server/src/main"
SERVER_JAVA = [
    "java/scouter/server/Configure.java",
    "java/scouter/server/CounterManager.java",
    "java/scouter/server/ScouterTgMtConfig.java",
    "java/scouter/server/support/telegraf/TgConfig.java",
    "java/scouter/server/support/telegraf/TgCounterMapping.java",
    "java/scouter/server/support/telegraf/TgmConfig.java",
    "java/scouter/server/http/handler/CounterHandler.java",
    "java/scouter/server/http/handler/RegisterHandler.java",
    "java/scouter/server/http/handler/TelegrafInputHandler.java",
    "java/scouter/server/http/model/CounterProtocol.java",
    "java/scouter/server/http/model/InfluxSingleLine.java",
    "java/scouter/server/http/model/NumberValueWithTime.java",
    "java/scouter/server/core/app/MeterCounter.java",
    "java/scouter/server/core/app/MeterCounterManager.java",
    "java/scouter/server/util/AsyncRun.java",
    "java/scouter/server/plugin/PluginHelper.java",
    "java/scouter/server/plugin/impl/Neighbor.java",
    "java/scouter/server/plugin/alert/AlertConf.java",
    "java/scouter/server/plugin/alert/AlertEngine.java",
    "java/scouter/server/plugin/alert/AlertRule.java",
    "java/scouter/server/plugin/alert/AlertRuleLoader.java",
    "java/scouter/server/plugin/alert/AlertUtil.java",
    "java/scouter/server/plugin/alert/RealCounter.java",
    "java/scouter/server/netio/service/ServiceHandlingProxy.java",
    "java/scouter/server/netio/service/anotation/ServiceHandler.java",
    "java/scouter/server/netio/service/handle/ManageService.java",
]
SERVER_SCALA = [
    "scala/scouter/server/LoginUser.scala",
    "scala/scouter/server/account/AccountFileHandler.scala",
    "scala/scouter/server/account/AccountManager.scala",
    "scala/scouter/server/account/GroupFileHandler.scala",
    "scala/scouter/server/management/RemoteControl.scala",
    "scala/scouter/server/management/RemoteControlManager.scala",
    "scala/scouter/server/util/XmlUtil.scala",
    "scala/scouter/server/util/ThreadScala.scala",
    "scala/scouter/server/core/cache/CacheOut.scala",
    "scala/scouter/server/core/cache/AlertScriptLoadMessageCache.scala",
    "scala/scouter/server/core/cache/AlertScriptLoadMessageLoopCache.scala",
    "scala/scouter/server/core/cache/CounterTimeCache.scala",
    "scala/scouter/server/core/cache/XLogCache.scala",
    "scala/scouter/server/core/cache/XLogLoopCache.scala",
    "scala/scouter/server/netio/service/handle/AccountService.scala",
    "scala/scouter/server/netio/service/handle/ConfigureService.scala",
    "scala/scouter/server/netio/service/handle/AlertScriptingService.scala",
    "scala/scouter/server/netio/service/handle/AgentInfo.scala",
    "scala/scouter/server/netio/service/handle/ThreadList.scala",
    "scala/scouter/server/netio/service/handle/LoginService.scala",
    "scala/scouter/server/netio/service/handle/XLogService.scala",
]
# EnumerScala.scala는 index oracle 기본 컴파일에 이미 포함된다.
RESOURCES = ["scouter.common/src/main/resources", "scouter.server/src/main/resources"]
DEPLOY_JARS = ["json-simple-1.1.1.jar", "javax.servlet-api-3.1.0.jar", "httpclient-4.3.6.jar", "httpcore-4.3.3.jar"]
# 원본 pom.xml의 javassist 3.29.2-GA. deploy/lib/javassist.jar(3.18)는 AlertRuleLoader의 toClass(Class)가 없어 컴파일되지 않는다.
JAVASSIST = Path(os.environ.get("SCOUTER_JAVASSIST_JAR", Path.home() / ".m2/repository/org/javassist/javassist/3.29.2-GA/javassist-3.29.2-GA.jar"))


def run(args, cwd=None, check=True, env=None):
    result = subprocess.run([str(x) for x in args], cwd=cwd, capture_output=True, text=True, env=env)
    if check and result.returncode:
        raise RuntimeError("command failed: " + " ".join(map(str, args))[:400] + " ...\n" + result.stdout[-12000:] + result.stderr[-12000:])
    return result


def java8_home():
    explicit = os.environ.get("SCOUTER_JAVA8_HOME")
    if explicit:
        return Path(explicit)
    if Path("/usr/libexec/java_home").exists():
        result = subprocess.run(["/usr/libexec/java_home", "-v", "1.8"], capture_output=True, text=True)
        if result.returncode == 0:
            return Path(result.stdout.strip())
    raise RuntimeError("JDK 8 is required (set SCOUTER_JAVA8_HOME); JAXB/Scala 2.10 compiler run on JDK 8 only")


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def git_blob(source, rel):
    return run(["git", "-C", source, "rev-parse", "HEAD:" + rel]).stdout.strip()


def oracle_sources():
    return sorted(p for p in TOOLS.rglob("*") if p.suffix in (".java", ".scala"))


def display_path(p, source):
    p = Path(p).resolve()
    for base, label in ((Path(source).resolve(), "$SCOUTER_SOURCE/"), (Path.home().resolve(), "~/")):
        try:
            return label + str(p.relative_to(base))
        except ValueError:
            pass
    return p.name


def compile_management(source, work):
    source, work = Path(source).resolve(), Path(work).resolve()
    jdk8 = java8_home()
    cp = index_oracle.compile_classes(source, work)
    rels = [SERVER + "/" + r for r in SERVER_JAVA + SERVER_SCALA]
    if run(["git", "-C", source, "status", "--porcelain", "--", *rels, *RESOURCES, "scouter.deploy/lib"]).stdout:
        raise RuntimeError("management oracle source modified; oracle rejected")
    if not JAVASSIST.is_file():
        raise RuntimeError(f"javassist 3.29.2-GA jar not found: {JAVASSIST} (set SCOUTER_JAVASSIST_JAR)")
    classes = work / "classes"
    lib = source / "scouter.deploy/lib"
    extra = [lib / j for j in DEPLOY_JARS] + [JAVASSIST]
    full_cp = os.pathsep.join([cp, *map(str, extra)])
    seams = sorted((TOOLS / "seams").rglob("*.java"))
    scala_seams = sorted((TOOLS / "seams").rglob("*.scala"))
    access = sorted((TOOLS / "access").rglob("*.java"))
    java_src = [source / SERVER / r for r in SERVER_JAVA]
    scala_src = [source / SERVER / r for r in SERVER_SCALA]
    jars = source / "scouter.deploy/lib/scala"
    compiler_cp = os.pathsep.join(str(p) for p in [jars / "scala-compiler.jar", jars / "scala-library.jar", jars / "scala-reflect.jar", classes])
    # mixed compile: scalac가 Java source를 parse만 하고 Scala class를 만든 뒤 javac가 Java를 컴파일한다.
    run([jdk8 / "bin/java", "-XX:ActiveProcessorCount=2", "-Dscala.usejavacp=true", "-cp", compiler_cp,
         "scala.tools.nsc.Main", "-usejavacp", "-classpath", full_cp, "-d", classes, *scala_src, *scala_seams, *seams, *java_src, *access])
    run([jdk8 / "bin/javac", "-source", "8", "-target", "8", "-encoding", "UTF-8", "-nowarn", "-cp", full_cp + os.pathsep + str(classes),
         "-d", classes, *seams, *java_src, *access, *sorted(TOOLS.glob("*.java"))])
    runtime_cp = os.pathsep.join([str(classes), full_cp, *[str(source / r) for r in RESOURCES]])
    return runtime_cp


def java18_home():
    explicit = os.environ.get("SCOUTER_JAVA18_HOME")
    if explicit:
        return Path(explicit)
    if Path("/usr/libexec/java_home").exists():
        result = subprocess.run(["/usr/libexec/java_home", "-v", "18"], capture_output=True, text=True)
        if result.returncode == 0:
            return Path(result.stdout.strip())
    raise RuntimeError("JDK 18 is required for the descriptor scenario (set SCOUTER_JAVA18_HOME)")


def jvm_command(runtime_cp, scenario, outdir, java_home=None, props=None):
    home = java_home or java8_home()
    return [home / "bin/java", "-XX:ActiveProcessorCount=2", f"-Duser.timezone={TIMEZONE}", "-Dfile.encoding=UTF-8",
            "-Duser.language=en", "-Duser.country=US", *[f"-D{k}={v}" for k, v in (props or {}).items()],
            "-cp", runtime_cp, "ManagementOracle", scenario, outdir]


def run_jvm(runtime_cp, scenario, target, cwd, java_home=None, props=None):
    cwd.mkdir(parents=True, exist_ok=True)
    target.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    result = run(jvm_command(runtime_cp, scenario, target.resolve(), java_home, props), cwd=cwd, check=False, env=ORACLE_ENV)
    if result.returncode:
        raise RuntimeError(f"scenario {scenario} failed\n{result.stdout[-6000:]}{result.stderr[-12000:]}")
    print(result.stdout.rstrip() + f" ({time.monotonic() - started:.1f}s)")


def telegraf_golden_inputs():
    """Inputs only (name/offsetMs/remote/body) of the Go golden, so Go-side expected packs do not affect the oracle."""
    import re
    start = re.search(r"const goldenStart = (\d+)", TELEGRAF_GOLDEN_TEST.read_text())
    if not start:
        raise RuntimeError("goldenStart not found in internal/telegraf/golden_test.go")
    steps = [{k: s[k] for k in ("name", "offsetMs", "remote", "body")} for s in json.loads(TELEGRAF_GOLDEN.read_text())]
    return {"source": "internal/telegraf/testdata/golden_packs.json (name/offsetMs/remote/body only)",
            "goldenStart": int(start.group(1)), "config": "TgConfig.getSampleContents() (Go sampleConfig)", "steps": steps}


def run_scenario(runtime_cp, scenario, target, work):
    cwd = work / "run" / scenario
    if scenario == "descriptors":
        run_jvm(runtime_cp, scenario, target, cwd / "jdk8")
        run_jvm(runtime_cp, scenario, target, cwd / "jdk18", java_home=java18_home())
        a, b = (json.loads((target / f"descriptors.java{v}.json").read_text()) for v in ("8", "18"))
        summary = {"note": "element sets of RealCounter/PluginHelper descriptors per JDK; raw item order is Class.getDeclaredMethods() order and differs by JDK and between runs",
                   "RealCounter_same_set": a["RealCounter"]["items"] == b["RealCounter"]["items"],
                   "PluginHelper_same_set": a["PluginHelper"]["items"] == b["PluginHelper"]["items"],
                   "counts": {"java8": [a["RealCounter"]["count"], a["PluginHelper"]["count"]], "java18": [b["RealCounter"]["count"], b["PluginHelper"]["count"]]}}
        (target / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        return
    if scenario == "telegraf-golden":
        target.mkdir(parents=True, exist_ok=True)
        inputs = telegraf_golden_inputs()
        (target / "input-steps.json").write_text(json.dumps(inputs, indent=2, ensure_ascii=False) + "\n")
        props = {"oracle.input": (target / "input-steps.json").resolve(), "oracle.goldenStart": inputs["goldenStart"]}
        run_jvm(runtime_cp, "telegraf-golden-warmup", target / "warmup", cwd / "warmup", props=props)
        site = (cwd / "warmup/conf/counters.site.xml").read_bytes()
        (cwd / "replay/conf").mkdir(parents=True)
        (cwd / "replay/conf/counters.site.xml").write_bytes(site)
        run_jvm(runtime_cp, scenario, target, cwd / "replay", props=props)
        return
    run_jvm(runtime_cp, scenario, target, cwd)


def generate(source, out, scenarios):
    with tempfile.TemporaryDirectory(prefix="scouter-management-oracle-") as work:
        work = Path(work)
        runtime_cp = compile_management(source, work)
        for scenario in scenarios:
            run_scenario(runtime_cp, scenario, out / scenario, work)
        class_count = sum(1 for _ in (work / "classes").rglob("*.class"))
        work_paths = [str(work), str(work.resolve())]
    jdk8 = java8_home()
    java_version = run([jdk8 / "bin/java", "-version"]).stderr.strip().splitlines()
    extra = {}
    if "descriptors" in scenarios:
        extra["descriptor_jdk18"] = run([java18_home() / "bin/java", "-version"]).stderr.strip().splitlines()
    if "telegraf-golden" in scenarios:
        extra["telegraf_golden_input"] = {"path": str(TELEGRAF_GOLDEN.relative_to(ROOT)),
                                          "inputs_sha256": hashlib.sha256((out / "telegraf-golden/input-steps.json").read_bytes()).hexdigest()}
    source = Path(source).resolve()
    manifest = {
        "source_commit": index_oracle.SHA,
        "source_version": "2.21.3",
        "synthetic_only": True,
        "timezone": TIMEZONE,
        "file_encoding": "UTF-8",
        "generator": "python3 scripts/java-management.py --generate internal/service/testdata/management",
        "check": "python3 scripts/java-management.py --check",
        "readme": "tools/java-management/README.md",
        "runtime_jdk": java_version,
        **extra,
        "xml_output_basis": {
            "counters/counters.site.after-*.xml, telegraf*/counters.site.*.xml": "written by the original CounterManager (FileUtil.saveText) after normalizeCountersXml: JDK 8 built-in JAXB (com.sun.xml.internal.bind) formatted marshal of a document produced by the JDK 8 built-in Xalan transform (XmlUtil.writeXmlFileWithIndent); UTF-8, 4-space indent, ends with exactly one newline",
            "account/account*.default.xml": "byte copies of the classpath defaults scouter.server/src/main/resources/scouter/server/account/*.xml (tab indent, no trailing newline)",
            "counters.site.seed.xml, counters.site.set-input.xml": "oracle inputs, not original output",
            "not captured": "the Xalan string returned by XmlUtil.writeXmlFileWithIndent for account/group edits; the original discards it and never writes the file",
            "jdk_dependence": "JDK 8 only; the JDK 18 Xalan output differs in whitespace (management-commands.md 2.4)",
        },
        "scala_compiler": "scouter.deploy/lib/scala/scala-compiler.jar 2.10.4 on the runtime JDK",
        "compiled_class_count": class_count,
        "scenarios": scenarios,
        "response_framing": "*.response.bin = bytes written by the handler (HasNEXT+item ...) followed by the NoNEXT byte that TcpServiceWorker appends; handler exceptions are swallowed by the original ServiceHandlingProxy",
        "request_framing": "*.request.bin = body only (the bytes the handler reads after writeText(cmd)+writeLong(session))",
        "external_jars": [{"path": display_path(p, source), "sha256": sha256(p)}
                          for p in [source / "scouter.deploy/lib" / j for j in DEPLOY_JARS] + [JAVASSIST]],
        "server_source_checksums": [{"path": SERVER + "/" + r, "sha256": sha256(source / SERVER / r), "git_blob": git_blob(source, SERVER + "/" + r)}
                                    for r in SERVER_JAVA + SERVER_SCALA],
        "resource_checksums": [{"path": str(p.relative_to(source)), "sha256": sha256(p)}
                               for r in RESOURCES for p in sorted((source / r).rglob("*")) if p.is_file()],
        "oracle_source_checksums": [{"path": str(p.relative_to(ROOT)), "sha256": sha256(p)} for p in oracle_sources()],
        "files": [{"name": str(p.relative_to(out)), "bytes": p.stat().st_size, "sha256": sha256(p)}
                  for p in sorted(out.rglob("*")) if p.is_file() and p.name != "oracle-manifest.json"],
    }
    (out / "oracle-manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    leak_check(out, work_paths)
    return manifest


def leak_check(out, extra):
    """Fails when host/user/path/environment details reached the golden files."""
    import getpass
    import socket
    needles = {str(Path.home()), "/private/var/folders", "/var/folders", tempfile.gettempdir(), "PATH=", "JAVA_HOME=",
               "SCOUTER_ORACLE_ENV_MARKER", "java.class.path", *extra}
    for name in (getpass.getuser(), socket.gethostname(), socket.gethostname().split(".")[0]):
        if len(name) >= 4:
            needles.add(name)
    found = []
    for p in sorted(out.rglob("*")):
        if not p.is_file():
            continue
        data = p.read_bytes()
        for n in needles:
            if n and n.encode() in data:
                found.append(f"{p.relative_to(out)}: contains {n!r}")
    if found:
        raise RuntimeError("golden leak check failed:\n" + "\n".join(found[:40]))


def compare(golden, fresh):
    problems = []
    gfiles = {str(p.relative_to(golden)) for p in golden.rglob("*") if p.is_file()}
    ffiles = {str(p.relative_to(fresh)) for p in fresh.rglob("*") if p.is_file()}
    for name in sorted(gfiles ^ ffiles):
        problems.append(f"file set differs: {name} ({'golden only' if name in gfiles else 'fresh only'})")
    for name in sorted(gfiles & ffiles):
        a, b = (golden / name).read_bytes(), (fresh / name).read_bytes()
        if a == b:
            continue
        if name.endswith(".json") or name.endswith(".xml") or name.endswith(".txt") or name.endswith(".conf"):
            diff = difflib.unified_diff(a.decode("utf-8", "replace").splitlines(), b.decode("utf-8", "replace").splitlines(),
                                        "golden/" + name, "fresh/" + name, lineterm="", n=1)
            problems.append("\n".join(list(diff)[:60]))
        else:
            problems.append(f"bytes differ: {name} ({len(a)} vs {len(b)} bytes)")
    return problems


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--generate", type=Path, help="빈(또는 없는) 디렉터리에 golden 생성")
    group.add_argument("--check", nargs="?", const=DEST, type=Path, help="임시 디렉터리에 재생성 후 golden과 비교")
    group.add_argument("--classes-dir", type=Path, help="이 디렉터리에 classes를 보존하고 runtime classpath 출력")
    group.add_argument("--verify-xml", type=Path, help="Go가 쓴 conf 디렉터리 복사본(account.xml, account_group.xml, counters.site.xml)을 원본 parser로 읽어 JSON 출력")
    parser.add_argument("--scenario", action="append", choices=SCENARIOS, help="일부 scenario만 실행(--generate 전용)")
    parser.add_argument("--source", type=Path, default=Path(os.environ.get("SCOUTER_SOURCE", ROOT.parent / "scouter")))
    args = parser.parse_args()
    if args.classes_dir:
        print(compile_management(args.source, args.classes_dir))
        return
    if args.verify_xml:
        with tempfile.TemporaryDirectory(prefix="scouter-management-verify-") as work:
            runtime_cp = compile_management(args.source, work)
            conf = args.verify_xml.resolve()
            result = run(jvm_command(runtime_cp, "verify-xml", conf), cwd=work, check=False)
            sys.stdout.write(result.stdout)
            sys.stderr.write(result.stderr[-6000:])
            sys.exit(result.returncode)
    if args.generate:
        out = args.generate
        if out.exists() and any(out.iterdir()):
            parser.error("--generate output must be empty")
        out.mkdir(parents=True, exist_ok=True)
        generate(args.source, out.resolve(), args.scenario or SCENARIOS)
        return
    golden = args.check.resolve()
    if not (golden / "oracle-manifest.json").is_file():
        parser.error(f"golden manifest not found: {golden}")
    expected = json.loads((golden / "oracle-manifest.json").read_text())
    with tempfile.TemporaryDirectory(prefix="scouter-management-check-") as tmp:
        fresh = Path(tmp) / "out"
        fresh.mkdir()
        generate(args.source, fresh, expected["scenarios"])
        problems = compare(golden, fresh)
    if problems:
        print("\n".join(problems))
        print(f"FAIL: {len(problems)} difference(s) between golden and regenerated oracle output")
        sys.exit(1)
    print(f"OK: regenerated {len(expected['files'])} golden files identical ({golden})")


if __name__ == "__main__":
    main()
