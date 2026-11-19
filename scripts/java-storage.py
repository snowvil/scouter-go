#!/usr/bin/env python3
"""실제 고정 Java/Scala DB writer/reader로 synthetic 저장 fixture를 검증한다."""
import argparse
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("index_oracle", ROOT / "internal/storage/legacyindex/testdata/run_oracle.py")
index_oracle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(index_oracle)
run = index_oracle.run


def compile_storage(source, work):
    source, work = Path(source).resolve(), Path(work).resolve()
    cp = index_oracle.compile_classes(source, work)
    if run(["git", "-C", source, "status", "--porcelain", "--", "scouter.server/src/main/scala/scouter/server/db", "scouter.server/src/main/java/scouter/server/db", "scouter.deploy/lib/scala"]):
        raise RuntimeError("storage source modified; oracle rejected")
    classes = work / "classes"
    stubs = work / "storage-stubs"
    config = """package scouter.server;
public class Configure {
 private static final Configure INSTANCE=new Configure();
 public static Configure getInstance(){return INSTANCE;}
 public int log_index_traversal_warning_count=100;
 public int _mgr_text_db_index_default_mb=1,_mgr_text_db_index_service_mb=1,_mgr_text_db_index_api_mb=1,_mgr_text_db_index_ua_mb=1,_mgr_text_db_index_login_mb=1,_mgr_text_db_index_desc_mb=1,_mgr_text_db_index_hmsg_mb=1,_mgr_text_db_daily_index_mb=1,_mgr_xlog_id_index_mb=1;
 public boolean compress_xlog_enabled=false,compress_profile_enabled=false;
 public int _compress_write_buffer_block_count=2,_compress_read_cache_block_count=2,_compress_dailycount_header_cache_size=4,_compress_read_cache_expired_ms=10000,_compress_write_thread=1;
}"""
    sources = {
        "scouter/server/Configure.java": config,
        "scouter/server/Logger.java": "package scouter.server; public class Logger {public static void println(String s){} public static void println(String a,String b){} public static void println(String a,int b,String c){} public static void println(String a,int b,String c,Throwable d){} }",
        "scouter/server/ShutdownManager.java": "package scouter.server; public class ShutdownManager {public static void add(scouter.util.IShutdown s){} }",
        "scouter/server/ConfObserver.java": "package scouter.server; public class ConfObserver {public static void put(String n,Runnable r){} }",
        "scouter/server/db/DBCtr.java": "package scouter.server.db; public class DBCtr {public static String getRootPath(){return System.getProperty(\"oracle.root\");} }",
        # Path routing only; actual XLog data writers/readers and GZipStore are original.
        "scouter/server/db/XLogWR.java": "package scouter.server.db; public class XLogWR {public static String dir(){return \"/xlog\";} }",
    }
    java = []
    for name, text in sources.items():
        p = stubs / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
        java.append(p)
    java += sorted((source / "scouter.server/src/main/java/scouter/server/db/io/zip").glob("*.java"))
    run(["javac", "--release", "8", "-encoding", "UTF-8", "-cp", cp, "-d", classes, *java])
    rels = ["io/RealDataFile.scala", "obj/ObjectData.scala", "obj/ObjectIndex.scala", "text/TextPermData.scala", "text/TextPermIndex.scala", "text/TextTable.scala"]
    rels += ["xlog/" + name + ".scala" for name in ["XLogDataWriter", "XLogDataReader", "XLogProfileDataWriter", "XLogProfileDataReader", "XLogIndex", "XLogProfileIndex"]]
    rels += ["counter/" + name + ".scala" for name in ["DailyCounterUtils", "DailyCounterData", "DailyCounterIndex", "RealtimeCounterData", "RealtimeCounterIndex", "RealtimeCounterKeyFile", "RealtimeCounterDBHelper", "RealtimeCounterDBHeader"]]
    files = [source / "scouter.server/src/main/scala/scouter/server/db" / name for name in rels]
    jars = source / "scouter.deploy/lib/scala"
    compiler_cp = os.pathsep.join(str(p) for p in [jars / "scala-compiler.jar", jars / "scala-library.jar", jars / "scala-reflect.jar", classes])
    java8 = os.environ.get("SCOUTER_SCALA_JAVA")
    if not java8 and Path("/usr/libexec/java_home").exists():
        result = subprocess.run(["/usr/libexec/java_home", "-v", "1.8"], capture_output=True, text=True)
        if result.returncode == 0:
            java8 = str(Path(result.stdout.strip()) / "bin/java")
    run([java8 or "java", "-Dscala.usejavacp=true", "-cp", compiler_cp, "scala.tools.nsc.Main", "-usejavacp", "-classpath", cp, "-d", classes, *files])
    run(["javac", "--release", "8", "-encoding", "UTF-8", "-cp", cp, "-d", classes, ROOT / "tools/java-storage/StorageOracle.java", ROOT / "tools/java-storage/ObjectRemoveOracle.java"])
    return cp


def compile_extra(source, work):
    source, work = Path(source).resolve(), Path(work).resolve()
    cp = compile_storage(source, work)
    classes = work / "classes"
    stubs = work / "storage-stubs"
    config = stubs / "scouter/server/Configure.java"
    config.write_text(config.read_text().replace("public boolean compress_xlog_enabled", "public boolean _trace=false; public boolean compress_xlog_enabled"))
    seams = {
        "scouter/server/db/DBCtr.java": 'package scouter.server.db; public class DBCtr {public static int MAX_QUE_SIZE=4; public static boolean running=false; public static String getRootPath(){return System.getProperty("oracle.root");}}',
        "scouter/server/core/CoreRun.java": 'package scouter.server.core; public class CoreRun {public static boolean running=false;}',
        "scouter/server/core/AgentManager.java": 'package scouter.server.core; public class AgentManager {private static java.util.Map<Integer,scouter.lang.pack.ObjectPack> objects=new java.util.HashMap<>(); public static scouter.lang.pack.ObjectPack getAgent(int h){return objects.get(h);} public static void active(scouter.lang.pack.ObjectPack p){objects.put(p.objHash,p);}}',
        # SpanStepBuilder only uses this to populate nameDebug, which is not serialized.
        "scouter/server/db/TextRD.java": 'package scouter.server.db; public class TextRD {public static String getString(String d,String t,int h){return "synthetic-debug-name";}}',
    }
    java = [config]
    for name, code in seams.items():
        path = stubs / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(code)
        java.append(path)
    java += sorted((source / "scouter.server/src/main/java/scouter/server/util/cardinality").glob("*.java"))
    run(["javac", "--release", "8", "-encoding", "UTF-8", "-cp", cp, "-d", classes, *java])
    rels = [kind + "/" + name + ".scala" for kind, names in {
        "alert": ["AlertWriter", "AlertReader", "AlertIndex"],
        "summary": ["SummaryWriter", "SummaryReader", "SummaryIndex"],
        "status": ["StatusWriter", "StatusReader", "StatusIndex"],
        "span": ["ZipkinSpanDataWriter", "ZipkinSpanDataReader", "ZipkinSpanIndex"],
    }.items() for name in names]
    files = [source / "scouter.server/src/main/scala/scouter/server/db" / name for name in rels]
    files += [source / "scouter.server/src/main/scala/scouter/server" / name for name in
              ["db/BatchDB.scala", "db/StackAnalyzerDB.scala", "util/BinSearch.scala", "util/ThreadScala.scala", "core/app/SpanStepBuilder.scala"]]
    if run(["git", "-C", source, "status", "--porcelain", "--", *[str(path.relative_to(source)) for path in files], "scouter.server/src/main/java/scouter/server/util/cardinality"]):
        raise RuntimeError("extra oracle source modified")
    jars = source / "scouter.deploy/lib/scala"
    compiler_cp = os.pathsep.join(str(p) for p in [jars / "scala-compiler.jar", jars / "scala-library.jar", jars / "scala-reflect.jar", classes])
    java8 = os.environ.get("SCOUTER_SCALA_JAVA")
    if not java8 and Path("/usr/libexec/java_home").exists():
        result = subprocess.run(["/usr/libexec/java_home", "-v", "1.8"], capture_output=True, text=True)
        if result.returncode == 0:
            java8 = str(Path(result.stdout.strip()) / "bin/java")
    run([java8 or "java", "-XX:ActiveProcessorCount=2", "-Dscala.usejavacp=true", "-cp", compiler_cp,
         "scala.tools.nsc.Main", "-usejavacp", "-classpath", cp, "-d", classes, *files])
    run(["javac", "--release", "8", "-encoding", "UTF-8", "-cp", cp, "-d", classes,
         ROOT / "tools/java-storage/ExtraDataset.java", ROOT / "tools/java-storage/ExtraStorageOracle.java"])
    return cp


def compile_counter_handlers(source, work):
    cp = compile_extra(source, work)
    source, work = Path(source).resolve(), Path(work).resolve()
    classes, stubs = work / "classes", work / "storage-stubs"
    seams = {
        "scouter/server/db/DailyCounterRD.java": """package scouter.server.db;
public class DailyCounterRD {
 public static scouter.lang.value.Value[] values;
 public static scouter.lang.value.Value[] getValues(String d,scouter.lang.CounterKey k){return values;}
}""",
    }
    seams["scouter/server/db/HandlerOracleInput.java"] = """package scouter.server.db;
public class HandlerOracleInput {public static int[] hashes=new int[0]; public static byte[][] records=new byte[0][]; public static long[] times=new long[0]; public static scouter.lang.value.MapValue[] counters=new scouter.lang.value.MapValue[0];}"""
    seams["scouter/server/db/ObjectRD.java"] = 'package scouter.server.db; public class ObjectRD {public static String getObjName(String d,int h){return "synthetic";}}'
    seams["scouter/server/core/cache/CounterCache.java"] = """package scouter.server.core.cache;
public class CounterCache {public static scouter.lang.value.Value get(scouter.lang.CounterKey k){return null;} public static java.util.Map<String,scouter.lang.value.Value> getObjectCounters(int h,byte t){return new java.util.HashMap<>();}}"""
    java = []
    for name, code in seams.items():
        path = stubs / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(code)
        java.append(path)
    java.append(source / "scouter.server/src/main/java/scouter/server/netio/service/anotation/ServiceHandler.java")
    files = [source / "scouter.server/src/main/scala/scouter/server" / name for name in
             ["netio/service/handle/DailyCounterService.scala", "netio/service/handle/CounterService.scala", "util/TimedSeries.scala"]
             + ["netio/service/handle/" + name + ".scala" for name in
                ["SummaryService", "SummaryEndUserNav", "SummaryEndUserAjax", "SummaryEndUserError"]]]
    if run(["git", "-C", source, "status", "--porcelain", "--", *[str(p.relative_to(source)) for p in java[-1:] + files]]):
        raise RuntimeError("counter handler oracle source modified")
    run(["javac", "--release", "8", "-cp", cp, "-d", classes, *java])
    jars = source / "scouter.deploy/lib/scala"
    compiler_cp = os.pathsep.join(str(p) for p in [jars / "scala-compiler.jar", jars / "scala-library.jar", jars / "scala-reflect.jar", classes])
    java8 = os.environ.get("SCOUTER_SCALA_JAVA")
    if not java8 and Path("/usr/libexec/java_home").exists():
        result = subprocess.run(["/usr/libexec/java_home", "-v", "1.8"], capture_output=True, text=True)
        if result.returncode == 0:
            java8 = str(Path(result.stdout.strip()) / "bin/java")
    (classes / "scouter/server/core/AgentManager.class").unlink()
    agent_scala = stubs / "AgentSelectionSeam.scala"
    agent_scala.write_text("""package scouter.server.core
object AgentManager {
 val objMap=new scouter.util.IntKeyMap[scouter.lang.pack.ObjectPack]()
 def getAgent(h:Int)={val p=new scouter.lang.pack.ObjectPack();p.objHash=h;p.objName="synthetic";p}
 def getAgentName(h:Int)="synthetic"
 def getLiveObjHashList(t:String)={val l=new java.util.ArrayList[Int]();scouter.server.db.HandlerOracleInput.hashes.foreach(h=>l.add(h));l}
 def getCurrentObjects(t:String)={val p=new scouter.lang.pack.MapPack();val l=p.newList("objHash");scouter.server.db.HandlerOracleInput.hashes.foreach(h=>l.add(h));p}
 def getDailyObjects(d:String,t:String)=getCurrentObjects(t)
 def getPeriodicObjects(s:String,e:String,t:String)=getCurrentObjects(t)
}
""")
    seam_scala = stubs / "HandlerDataSeams.scala"
    seam_scala.write_text("""package scouter.server.db
object SummaryRD {def readByTime(t:Byte,d:String,s:Long,e:Long,f:(Long,Array[Byte])=>Unit){HandlerOracleInput.records.foreach(r=>f(s,r))}}
object RealtimeCounterRD {
 def read[R](n:String,d:String,s:Long,e:Long,f:(Long,scouter.lang.value.MapValue)=>R){HandlerOracleInput.times.indices.foreach(i=>f(HandlerOracleInput.times(i),HandlerOracleInput.counters(i)))}
 def readBulk[R](d:String,s:Long,e:Long,f:scouter.lang.value.MapValue=>R){HandlerOracleInput.counters.foreach(r=>f(r))}
}
""")
    files = [agent_scala, seam_scala] + files
    run([java8 or "java", "-XX:ActiveProcessorCount=2", "-Dscala.usejavacp=true", "-cp", compiler_cp,
         "scala.tools.nsc.Main", "-usejavacp", "-classpath", cp, "-d", classes, *files])
    run(["javac", "--release", "8", "-cp", cp, "-d", classes, ROOT / "tools/java-storage/CounterHandlerOracle.java", ROOT / "tools/java-storage/SummaryHandlerOracle.java"])
    return cp


def compile_db_handlers(source, work):
    cp = compile_counter_handlers(source, work)
    source, work = Path(source).resolve(), Path(work).resolve()
    classes, stubs = work / "classes", work / "storage-stubs"
    seams = {
        "scouter/server/core/cache/CounterCache.java": """package scouter.server.core.cache;
public class CounterCache {public static java.util.Map<scouter.lang.CounterKey,scouter.lang.value.Value> values=new java.util.HashMap<>(); public static scouter.lang.value.Value get(scouter.lang.CounterKey k){return values.get(k);} public static java.util.Map<String,scouter.lang.value.Value> getObjectCounters(int h,byte t){return new java.util.HashMap<>();}}""",
        "scouter/server/core/cache/StatusCache.java": """package scouter.server.core.cache;
public class StatusCache {public static scouter.lang.pack.StatusPack value; public static scouter.lang.pack.StatusPack get(int h,String k){return value!=null&&value.objHash==h&&value.key.equals(k)?value:null;}}""",
        "scouter/server/db/StatusWR.java": 'package scouter.server.db; public class StatusWR {public static String status="status";public static String getDBPath(String d){return System.getProperty("oracle.root")+"/"+d+"/status";}}',
        # Symbol-only seam: tests never execute the Agent forwarding branches.
        "scouter/server/netio/AgentCall.java": 'package scouter.server.netio; public class AgentCall {public static scouter.lang.pack.Pack call(scouter.lang.pack.ObjectPack o,String c,scouter.lang.pack.MapPack p){throw new UnsupportedOperationException("Agent branch outside DB oracle");}}',
    }
    java = []
    for name, code in seams.items():
        path = stubs / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(code)
        java.append(path)
    run(["javac", "--release", "8", "-cp", cp, "-d", classes, *java])
    files = [source / "scouter.server/src/main/scala/scouter/server" / name for name in
             ["db/StatusRD.scala", "netio/service/handle/MariaService.scala", "netio/service/handle/CubridService.scala"]]
    if run(["git", "-C", source, "status", "--porcelain", "--", *[str(p.relative_to(source)) for p in files]]):
        raise RuntimeError("DB handler oracle source modified")
    jars = source / "scouter.deploy/lib/scala"
    compiler_cp = os.pathsep.join(str(p) for p in [jars / "scala-compiler.jar", jars / "scala-library.jar", jars / "scala-reflect.jar", classes])
    java8 = os.environ.get("SCOUTER_SCALA_JAVA")
    if not java8 and Path("/usr/libexec/java_home").exists():
        result = subprocess.run(["/usr/libexec/java_home", "-v", "1.8"], capture_output=True, text=True)
        if result.returncode == 0:
            java8 = str(Path(result.stdout.strip()) / "bin/java")
    run([java8 or "java", "-XX:ActiveProcessorCount=2", "-Dscala.usejavacp=true", "-cp", compiler_cp,
         "scala.tools.nsc.Main", "-usejavacp", "-classpath", cp, "-d", classes, *files])
    run(["javac", "--release", "8", "-cp", cp, "-d", classes, ROOT / "tools/java-storage/DBHandlerOracle.java"])
    return cp


def compile_ttl(source, work):
    source, work = Path(source).resolve(), Path(work).resolve()
    cp = index_oracle.compile_classes(source, work)
    classes = work / "classes"
    files = [source / "scouter.server/src/main/scala/scouter/server/db/io" / name for name in ["RealKeyFile2.scala", "IndexKeyFile2.scala"]]
    jars = source / "scouter.deploy/lib/scala"
    compiler_cp = os.pathsep.join(str(p) for p in [jars / "scala-compiler.jar", jars / "scala-library.jar", jars / "scala-reflect.jar", classes])
    java8 = os.environ.get("SCOUTER_SCALA_JAVA")
    if not java8 and Path("/usr/libexec/java_home").exists():
        result = subprocess.run(["/usr/libexec/java_home", "-v", "1.8"], capture_output=True, text=True)
        if result.returncode == 0:
            java8 = str(Path(result.stdout.strip()) / "bin/java")
    run([java8 or "java", "-XX:ActiveProcessorCount=2", "-Dscala.usejavacp=true", "-cp", compiler_cp,
         "scala.tools.nsc.Main", "-usejavacp", "-classpath", cp, "-d", classes, *files])
    run(["javac", "--release", "8", "-cp", cp, "-d", classes, ROOT / "tools/java-storage/TTLOracle.java"])
    return cp


def compile_local_store(source, work):
    cp = compile_db_handlers(source, work)
    source, work = Path(source).resolve(), Path(work).resolve()
    classes = work / "classes"
    config = work / "storage-stubs/scouter/server/Configure.java"
    config.write_text(config.read_text().replace("public int log_index_traversal_warning_count", "public int _mgr_kv_store_index_default_mb=8; public int log_index_traversal_warning_count"))
    run(["javac", "--release", "8", "-cp", cp, "-d", classes, config])
    files = [source / "scouter.server/src/main/scala/scouter/server" / name for name in
             ["db/io/RealKeyFile2.scala", "db/io/IndexKeyFile2.scala", "db/kv/KeyValueStoreWriter.scala", "db/kv/KeyValueStoreIndex.scala", "db/kv/KeyValueStoreReader.scala", "db/KeyValueStoreRW.scala", "netio/service/handle/KeyValueStoreService.scala", "netio/service/handle/DatabaseService.scala"]]
    if run(["git", "-C", source, "status", "--porcelain", "--", *[str(p.relative_to(source)) for p in files]]):
        raise RuntimeError("local store oracle source modified")
    jars = source / "scouter.deploy/lib/scala"
    compiler_cp = os.pathsep.join(str(p) for p in [jars / "scala-compiler.jar", jars / "scala-library.jar", jars / "scala-reflect.jar", classes])
    java8 = os.environ.get("SCOUTER_SCALA_JAVA")
    if not java8 and Path("/usr/libexec/java_home").exists():
        result = subprocess.run(["/usr/libexec/java_home", "-v", "1.8"], capture_output=True, text=True)
        if result.returncode == 0:
            java8 = str(Path(result.stdout.strip()) / "bin/java")
    run([java8 or "java", "-XX:ActiveProcessorCount=2", "-Dscala.usejavacp=true", "-cp", compiler_cp,
         "scala.tools.nsc.Main", "-usejavacp", "-classpath", cp, "-d", classes, *files])
    run(["javac", "--release", "8", "-cp", cp, "-d", classes, ROOT / "tools/java-storage/LocalStoreOracle.java"])
    return cp


def compile_config(source, work):
    """원본 TextWR/TextRD/TextPermWR/TextPermRD, KeyValueStoreIndex, AutoDeleteScheduler를 설정 키별 동작 oracle용으로 컴파일한다."""
    cp = compile_storage(source, work)
    source, work = Path(source).resolve(), Path(work).resolve()
    classes, stubs = work / "classes", work / "storage-stubs"
    config = stubs / "scouter/server/Configure.java"
    config.write_text(config.read_text().replace("public int log_index_traversal_warning_count", "public boolean mgr_text_db_daily_service_enabled=false,mgr_text_db_daily_api_enabled=false,mgr_text_db_daily_ua_enabled=false,mgr_purge_enabled=true; public int mgr_purge_disk_usage_pct=0,mgr_purge_profile_keep_days=10,mgr_purge_xlog_keep_days=30,mgr_purge_counter_keep_days=70,mgr_purge_realtime_counter_keep_days=70,mgr_purge_visitor_counter_keep_days=70,mgr_purge_tag_counter_keep_days=70,mgr_purge_daily_text_days=140,mgr_purge_sum_data_days=60,_mgr_kv_store_index_default_mb=8; public int log_index_traversal_warning_count"))
    seams = {
        # ConfObserver keeps the registered Runnables so the oracle can run them like applyConfig does.
        "scouter/server/ConfObserver.java": "package scouter.server; public class ConfObserver {public static java.util.Map<String,Runnable> observers=new java.util.LinkedHashMap<String,Runnable>(); public static void put(String n,Runnable r){observers.put(n,r);} public static void exec(){for(Runnable r:observers.values())r.run();}}",
        # Background writer threads stop at once; the oracle drives process() and drains the queue itself.
        "scouter/server/db/DBCtr.java": 'package scouter.server.db; public class DBCtr {public static int MAX_QUE_SIZE=10000; public static int LARGE_MAX_QUE_SIZE=100000; public static boolean running=false; public static String getRootPath(){return System.getProperty("oracle.root");}}',
        "scouter/server/core/CoreRun.java": "package scouter.server.core; public class CoreRun {public static boolean running(){return false;}}",
        "scouter/server/db/ZipkinSpanWR.java": 'package scouter.server.db; public class ZipkinSpanWR {public static String SPAN_DIR(){return "/span";}}',
    }
    java = [config]
    for name, code in seams.items():
        path = stubs / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(code)
        java.append(path)
    server = source / "scouter.server/src/main"
    originals = [server / "java/scouter/server" / name for name in ["db/TextDupCheck.java", "core/ServerStat.java", "core/AutoDeleteScheduler.java"]]
    scala = [server / "scala/scouter/server" / name for name in
             ["util/ThreadScala.scala", "core/cache/TextCache.scala", "db/TextWR.scala", "db/TextRD.scala", "db/TextPermWR.scala", "db/TextPermRD.scala",
              "db/io/RealKeyFile2.scala", "db/io/IndexKeyFile2.scala", "db/kv/KeyValueStoreIndex.scala"]]
    if run(["git", "-C", source, "status", "--porcelain", "--", *[str(p.relative_to(source)) for p in originals + scala]]):
        raise RuntimeError("config oracle source modified")
    run(["javac", "--release", "8", "-encoding", "UTF-8", "-cp", cp, "-d", classes, *java, *originals])
    jars = source / "scouter.deploy/lib/scala"
    compiler_cp = os.pathsep.join(str(p) for p in [jars / "scala-compiler.jar", jars / "scala-library.jar", jars / "scala-reflect.jar", classes])
    java8 = os.environ.get("SCOUTER_SCALA_JAVA")
    if not java8 and Path("/usr/libexec/java_home").exists():
        result = subprocess.run(["/usr/libexec/java_home", "-v", "1.8"], capture_output=True, text=True)
        if result.returncode == 0:
            java8 = str(Path(result.stdout.strip()) / "bin/java")
    run([java8 or "java", "-XX:ActiveProcessorCount=2", "-Dscala.usejavacp=true", "-cp", compiler_cp,
         "scala.tools.nsc.Main", "-usejavacp", "-classpath", cp, "-d", classes, *scala])
    run(["javac", "--release", "8", "-encoding", "UTF-8", "-cp", cp, "-d", classes, ROOT / "tools/java-storage/ConfigStorageOracle.java"])
    return cp


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--generate", type=Path)
    group.add_argument("--verify", type=Path)
    group.add_argument("--generate-extra", type=Path)
    group.add_argument("--verify-extra", type=Path)
    group.add_argument("--generate-spans", type=Path, help="원본 SpanStepBuilder input/output golden만 생성")
    group.add_argument("--generate-counter-handlers", type=Path)
    group.add_argument("--generate-db-handlers", type=Path)
    group.add_argument("--generate-ttl", type=Path)
    group.add_argument("--verify-ttl", type=Path)
    group.add_argument("--generate-local-store", type=Path)
    group.add_argument("--generate-local-store-writes", type=Path, help="원본 KeyValueStoreService SET/TTL handler 6종 fixture와 kv2 DB를 생성")
    parser.add_argument("--classes-dir", type=Path)
    parser.add_argument("--config-classes-dir", type=Path, help="설정 키 동작 oracle(ConfigStorageOracle) classes를 보존하고 classpath를 출력")
    parser.add_argument("--source", type=Path, default=Path(os.environ.get("SCOUTER_SOURCE", ROOT.parent / "scouter")))
    args = parser.parse_args()
    local_store = args.generate_local_store or args.generate_local_store_writes
    if args.classes_dir:
        print(compile_storage(args.source, args.classes_dir))
        return
    if args.config_classes_dir:
        print(compile_config(args.source, args.config_classes_dir))
        return
    target = args.generate or args.verify or args.generate_extra or args.verify_extra or args.generate_spans or args.generate_counter_handlers or args.generate_db_handlers or args.generate_ttl or args.verify_ttl or local_store
    if target is None:
        parser.error("select a generate/verify mode or --classes-dir")
    if (args.generate or args.generate_extra or args.generate_spans or args.generate_counter_handlers or args.generate_db_handlers or args.generate_ttl or local_store) and target.exists() and any(target.iterdir()):
        parser.error("--generate output must be empty; never overwrite existing DB")
    with tempfile.TemporaryDirectory(prefix="scouter-storage-oracle-") as work:
        extra = args.generate_extra or args.verify_extra or args.generate_spans
        cp = compile_local_store(args.source, work) if local_store else compile_ttl(args.source, work) if args.generate_ttl or args.verify_ttl else compile_db_handlers(args.source, work) if args.generate_db_handlers else compile_counter_handlers(args.source, work) if args.generate_counter_handlers else compile_extra(args.source, work) if extra else compile_storage(args.source, work)
        mode = "writes" if args.generate_local_store_writes else "spans" if args.generate_spans else "generate" if args.generate or args.generate_extra or args.generate_counter_handlers or args.generate_db_handlers or args.generate_ttl or args.generate_local_store else "verify"
        print(run(["java", "-XX:ActiveProcessorCount=2", "-Duser.timezone=Asia/Seoul", "-cp", cp,
                   "LocalStoreOracle" if local_store else "TTLOracle" if args.generate_ttl or args.verify_ttl else "DBHandlerOracle" if args.generate_db_handlers else "CounterHandlerOracle" if args.generate_counter_handlers else "ExtraStorageOracle" if extra else "StorageOracle", mode, target.resolve()]), end="")
        if args.generate or args.generate_extra or args.generate_spans or args.generate_counter_handlers or args.generate_db_handlers or args.generate_ttl or local_store:
            manifest = {"source_commit": index_oracle.SHA, "source_version": "2.21.3", "synthetic_only": True, "timezone": "Asia/Seoul", "dataset": "tools/java-storage/README.md", "files": [{"name": str(p.relative_to(target)), "bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(target.rglob("*")) if p.is_file()]}
            manifest["compiled_class_count"] = sum(1 for _ in (Path(work) / "classes").rglob("*.class"))
            manifest["javac"] = run(["javac", "-version"]).strip()
            manifest["oracle_source_checksums"] = [{"path": str(p.relative_to(ROOT)), "bytes": p.stat().st_size,
                                                     "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                                                    for p in sorted((ROOT / "tools/java-storage").glob("*.java"))]
            if extra or args.generate_counter_handlers or args.generate_db_handlers or args.generate_ttl or local_store:
                server = args.source.resolve() / "scouter.server/src/main"
                evidence = sorted((server / "java/scouter/server/db").rglob("*.java"))
                evidence += sorted((server / "scala/scouter/server/db").rglob("*.scala"))
                evidence += [server / "scala/scouter/server" / name for name in
                             ["core/app/SpanStepBuilder.scala", "util/BinSearch.scala", "util/ThreadScala.scala"]]
                if args.generate_counter_handlers or args.generate_db_handlers or local_store:
                    evidence += [server / "scala/scouter/server" / name for name in
                                 ["util/TimedSeries.scala", "netio/service/handle/DailyCounterService.scala", "netio/service/handle/CounterService.scala"]
                                 + ["netio/service/handle/" + name + ".scala" for name in
                                    ["SummaryService", "SummaryEndUserNav", "SummaryEndUserAjax", "SummaryEndUserError"]]]
                    evidence += [server / "java/scouter/server/netio/service/anotation/ServiceHandler.java"]
                if args.generate_db_handlers or local_store:
                    evidence += [server / "scala/scouter/server/netio/service/handle" / (name + ".scala") for name in ["MariaService", "CubridService"]]
                if local_store:
                    evidence += [server / "scala/scouter/server/netio/service/handle" / (name + ".scala") for name in ["KeyValueStoreService", "DatabaseService"]]
                if args.generate_ttl:
                    evidence = sorted((server / "java/scouter/server/db/io").glob("*.java")) + sorted((server / "scala/scouter/server/db/io").glob("*.scala"))
                    evidence += [server / "scala/scouter/server/util/EnumerScala.scala"]
                evidence += sorted((server / "java/scouter/server/util/cardinality").glob("*.java"))
                evidence += sorted((args.source.resolve() / "scouter.common/src/main/java").rglob("*.java"))
                manifest["guarded_source_checksums"] = [{"path": str(p.relative_to(args.source.resolve())), "bytes": p.stat().st_size,
                                                         "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(set(evidence))]
                manifest["mode"] = "local-store-writes" if args.generate_local_store_writes else "local-store" if args.generate_local_store else "ttl" if args.generate_ttl else "db-handlers" if args.generate_db_handlers else "handlers" if args.generate_counter_handlers else "spans" if args.generate_spans else "extra-storage"
                manifest["infrastructure_seams"] = ["Configure/Logger/lifecycle/root routing", "Agent selection and Counter/Summary record supply for handler mode", "TextRD debug-name only for SpanStepBuilder"]
                if args.generate_db_handlers or local_store:
                    manifest["infrastructure_seams"] += ["CounterCache/StatusCache synthetic record supply", "StatusWR path-only", "unused AgentCall branches throw UnsupportedOperationException"]
            (target / "oracle-manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
