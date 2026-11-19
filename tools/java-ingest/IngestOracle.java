// Java oracle for the ingest side paths ported to internal/ingest: PodSeqManager (with the kube branch of
// AgentManager.active/inactive), ObjectCpuChecker (with its dump daemon) and GeoIpUtil. The original classes
// run unmodified; only their infrastructure is replaced by the seams under seams/. Every scenario writes one
// JSON file of steps {op, in..., out} that internal/ingest/java_oracle_test.go replays against the Go engine.
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import scouter.lang.constants.ScouterConstants;
import scouter.lang.pack.ObjectPack;
import scouter.lang.pack.PerfCounterPack;
import scouter.lang.pack.XLogPack;
import scouter.lang.value.BooleanValue;
import scouter.lang.value.DecimalValue;
import scouter.lang.value.DoubleValue;
import scouter.lang.value.FloatValue;
import scouter.lang.value.MapValue;
import scouter.lang.value.TextValue;
import scouter.lang.value.Value;
import scouter.lang.pack.AlertPack;
import scouter.server.Configure;
import scouter.server.core.AgentManager;
import scouter.server.core.AlertCore;
import scouter.server.core.cache.AlertCache;
import scouter.server.core.CoreRun;
import scouter.server.core.app.ObjectCpuChecker;
import scouter.server.db.TextPermWR;
import scouter.server.geoip.GISDataUtil;
import scouter.server.geoip.GeoIpUtil;
import scouter.server.kube.PodSeqManager;
import scouter.server.netio.AgentCallLog;
import scouter.util.HashUtil;
import scouter.util.StringUtil;

public final class IngestOracle {
    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args[1]);
        Files.createDirectories(out);
        List<Object> steps;
        switch (args[0]) {
            case "kube": steps = kube(); break;
            case "cpu": steps = cpu(); break;
            case "geoip": steps = geoip(); break;
            case "alerts": steps = alerts(); break;
            default: throw new IllegalArgumentException(args[0]);
        }
        Map<String, Object> doc = new LinkedHashMap<String, Object>();
        doc.put("scenario", args[0]);
        doc.put("steps", steps);
        Files.write(out.resolve(args[0] + ".json"), (json(doc, "") + "\n").getBytes(StandardCharsets.UTF_8));
        System.out.println(args[0] + ": " + steps.size() + " steps");
        System.exit(0); // ObjectCpuChecker's daemon is not a reason to keep the JVM alive
    }

    // ---- typed values: {"t": text|bool|decimal|float|double, "v": ...} ----
    static Map<String, Object> tv(String t, Object v) { Map<String, Object> m = new LinkedHashMap<String, Object>(); m.put("t", t); m.put("v", v); return m; }
    static Map<String, Object> text(String v) { return tv("text", v); }
    static Map<String, Object> bool(boolean v) { return tv("bool", v); }
    static Map<String, Object> dec(long v) { return tv("decimal", v); }
    static Map<String, Object> dbl(double v) { return tv("double", v); }
    static Map<String, Object> flt(float v) { return tv("float", v); }

    @SuppressWarnings("unchecked")
    static Value value(Object spec) {
        Map<String, Object> m = (Map<String, Object>) spec;
        String t = (String) m.get("t");
        Object v = m.get("v");
        if (t.equals("text")) return new TextValue((String) v);
        if (t.equals("bool")) return new BooleanValue((Boolean) v);
        if (t.equals("decimal")) return new DecimalValue(((Number) v).longValue());
        if (t.equals("float")) return new FloatValue(((Number) v).floatValue());
        if (t.equals("double")) return new DoubleValue(((Number) v).doubleValue());
        throw new IllegalArgumentException(t);
    }

    static List<Object> pairs(Object... kv) {
        List<Object> out = new ArrayList<Object>();
        for (int i = 0; i < kv.length; i += 2) { List<Object> p = new ArrayList<Object>(); p.add(kv[i]); p.add(kv[i + 1]); out.add(p); }
        return out;
    }

    @SuppressWarnings("unchecked")
    static MapValue mapValue(List<Object> pairs) {
        MapValue m = new MapValue();
        for (Object o : pairs) { List<Object> p = (List<Object>) o; m.put((String) p.get(0), value(p.get(1))); }
        return m;
    }

    static List<Object> calls() {
        List<Object> out = new ArrayList<Object>();
        for (AgentCallLog.Call c : AgentCallLog.take()) {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("objHash", c.objHash);
            m.put("cmd", c.cmd);
            m.put("param", hex(c.param));
            out.add(m);
        }
        return out;
    }

    // ---------------------------------------------------------------- kube
    static final IdentityHashMap<ObjectPack, Long> logicalInitial = new IdentityHashMap<ObjectPack, Long>();

    @SuppressWarnings("unchecked")
    static <T> T staticField(Class<?> c, String name) throws Exception { Field f = c.getDeclaredField(name); f.setAccessible(true); return (T) f.get(null); }
    @SuppressWarnings("unchecked")
    static <T> T field(Object o, String name) throws Exception { Field f = o.getClass().getDeclaredField(name); f.setAccessible(true); return (T) f.get(o); }

    // initialTime is System.currentTimeMillis() in the original. Each step runs at a logical time; before it,
    // every initial pack's initialTime is rebased so that (now - initialTime) equals the logical elapsed time.
    // Steps use elapsed values whose outcome does not depend on the few ms between the rebase and the check
    // (14000, exactly 15000, or more: drift can only make "initialTime > now - 15000" more false).
    static void rebase(long logicalNow) throws Exception {
        ConcurrentHashMap<String, ObjectPack> initial = staticField(PodSeqManager.class, "initialHostObjMap");
        long real = System.currentTimeMillis();
        for (ObjectPack p : initial.values()) {
            Long t = logicalInitial.get(p);
            if (t != null) p.initialTime = t + (real - logicalNow);
        }
    }
    static void stamp(long logicalNow) throws Exception {
        ConcurrentHashMap<String, ObjectPack> initial = staticField(PodSeqManager.class, "initialHostObjMap");
        for (ObjectPack p : initial.values()) if (!logicalInitial.containsKey(p)) logicalInitial.put(p, logicalNow);
    }

    static Map<String, Object> kubeState() throws Exception {
        Map<String, Object> state = new LinkedHashMap<String, Object>();
        ConcurrentHashMap<String, PodSeqManager> managers = staticField(PodSeqManager.class, "managerMap");
        List<Object> ms = new ArrayList<Object>();
        for (String pod : new TreeMap<String, PodSeqManager>(managers).keySet()) {
            PodSeqManager m = managers.get(pod);
            ConcurrentHashMap<String, Integer> hostSeq = field(m, "hostSeqMap");
            ConcurrentHashMap<Integer, String> seqHost = field(m, "seqHostMap");
            if (hostSeq.isEmpty() && seqHost.isEmpty()) continue; // Go drops empty managers (same behavior)
            Map<String, Object> e = new LinkedHashMap<String, Object>();
            e.put("pod", pod);
            List<Object> hs = new ArrayList<Object>();
            for (Map.Entry<String, Integer> x : new TreeMap<String, Integer>(hostSeq).entrySet()) hs.addAll(pairs(x.getKey(), x.getValue()));
            List<Object> sh = new ArrayList<Object>();
            for (Map.Entry<Integer, String> x : new TreeMap<Integer, String>(seqHost).entrySet()) sh.addAll(pairs(x.getKey(), x.getValue()));
            e.put("hostSeq", hs);
            e.put("seqHost", sh);
            ms.add(e);
        }
        state.put("managers", ms);
        ConcurrentHashMap<String, ObjectPack> initial = staticField(PodSeqManager.class, "initialHostObjMap");
        List<Object> is = new ArrayList<Object>();
        for (Map.Entry<String, ObjectPack> x : new TreeMap<String, ObjectPack>(initial).entrySet()) {
            Map<String, Object> e = new LinkedHashMap<String, Object>();
            e.put("host", x.getKey());
            e.put("podName", x.getValue().podName);
            e.put("podSeq", x.getValue().podSeq);
            e.put("t", logicalInitial.get(x.getValue()));
            is.add(e);
        }
        state.put("initial", is);
        return state;
    }

    // AgentManager.active lines 82-99: the kube branch only (the rest of active is not part of PodSeqManager).
    static Map<String, Object> active(long t, String objName, List<Object> tags) throws Exception {
        rebase(t);
        ObjectPack p = new ObjectPack();
        p.objName = objName;
        p.objType = "tomcat";
        p.objHash = HashUtil.hash(objName);
        p.tags = mapValue(tags);
        p.allowProceed = true;
        boolean useKubeSeq = p.tags.getBoolean(ScouterConstants.USE_KUBE_SEQ);
        String podName = p.tags.getText(ScouterConstants.POD_NAME);
        if (useKubeSeq && StringUtil.isNotEmpty(podName)) {
            PodSeqManager podSeqManager = PodSeqManager.getInstance(podName);
            podSeqManager.applyPodSeq(p);
            PodSeqManager.pushSeqToAgent(p);
        }
        stamp(t);
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("op", "active");
        step.put("t", t);
        step.put("objName", objName);
        step.put("tags", tags);
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        out.put("allowProceed", p.allowProceed);
        out.put("pushSeq", p.pushSeq);
        out.put("podSeq", p.podSeq);
        out.put("podName", p.podName);
        out.put("hostName", p.hostName);
        out.put("calls", calls());
        out.putAll(kubeState());
        step.put("out", out);
        return step;
    }

    // AgentManager.inactive line 214-216: objectInactivated(hostName) on the stored object's podName.
    static Map<String, Object> inactive(long t, String podName, String hostName) throws Exception {
        rebase(t);
        PodSeqManager.getInstance(podName).objectInactivated(hostName);
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("op", "inactive");
        step.put("t", t);
        step.put("podName", podName);
        step.put("hostName", hostName);
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        out.put("calls", calls());
        out.putAll(kubeState());
        step.put("out", out);
        return step;
    }

    static List<Object> kube(String pod, String host, Object seq) {
        List<Object> tags = pairs(ScouterConstants.POD_NAME, text(pod), ScouterConstants.HOST_NAME, text(host), ScouterConstants.USE_KUBE_SEQ, bool(true));
        if (seq != null) tags.addAll(pairs(ScouterConstants.KUBE_SEQ, seq));
        return tags;
    }

    static List<Object> kube() throws Exception {
        List<Object> s = new ArrayList<Object>();
        s.add(active(0, "/h1", kube("app", "h1", dec(-1))));        // init: held, seq 0 pushed
        s.add(active(5000, "/h1", kube("app", "h1", dec(-1))));     // within 15s: held, same seq pushed
        s.add(active(5000, "/h2", kube("app", "h2", dec(-1))));     // second host: seq 1
        s.add(active(6000, "/h1", kube("app", "h1", dec(0))));      // confirmed: proceeds, initial removed
        s.add(active(6000, "/h3", kube("app", "h3", dec(1))));      // seq taken by h2: re-init, seq 2
        s.add(active(20000, "/h2", kube("app", "h2", dec(-1))));    // exactly 15s: proceeds, no push, seq released
        s.add(active(20000, "/h3", kube("app", "h3", dec(-1))));    // 14s: still held, initial seq pushed
        s.add(active(21000, "/h4", kube("app", "h4", dec(7))));     // recover seq 7
        s.add(active(21000, "/h4", kube("app", "h4", dec(9))));     // recover seq 9: seq 7 stays stale
        s.add(inactive(22000, "app", "h4"));                        // releases 9 only
        s.add(active(22000, "/h5", kube("app", "h5", dec(-1))));    // lowest free seq: 1
        s.add(active(22000, "/h6", kube("app", "h6", null)));       // absent kubeSeq is 0, owned by h1: re-init
        s.add(active(22000, "/h3", kube("app", "h3", flt(2.5f))));  // Float 2.5 -> 2: confirmed
        s.add(active(22000, "/nohost", pairs(ScouterConstants.POD_NAME, text("app"), ScouterConstants.USE_KUBE_SEQ, bool(true), ScouterConstants.KUBE_SEQ, dec(-1))));
        s.add(active(22000, "/emptyhost", kube("app", "", dec(-1))));
        s.add(active(22000, "/off", pairs(ScouterConstants.POD_NAME, text("app"), ScouterConstants.HOST_NAME, text("x"), ScouterConstants.USE_KUBE_SEQ, bool(false))));
        s.add(active(22000, "/textflag", pairs(ScouterConstants.POD_NAME, text("app"), ScouterConstants.HOST_NAME, text("x"), ScouterConstants.USE_KUBE_SEQ, text("true"))));
        s.add(active(22000, "/emptypod", kube("", "x", dec(-1))));
        s.add(active(22000, "/d", kube("app", "hd", dbl(-1.5))));   // Double -1.5 -> -1: init
        s.add(active(22000, "/t", kube("app", "ht", text("-1"))));  // Text is not a Number: 0
        s.add(active(30000, "/h7", kube("p1", "h7", dec(-1))));     // init in pod p1
        s.add(active(45000, "/h7", kube("p2", "h7", dec(-1))));     // 15s later in p2: copies p1's podName
        s.add(inactive(46000, "p1", "h7"));                         // the copied podName releases p1
        s.add(inactive(46000, "app", null));                        // null hostName: nothing
        s.add(active(50000, "/h8", kube("app", "h8", dec(50))));    // recover 50
        s.add(active(50000, "/h8", kube("app", "h8", dec(-1))));    // init with a previous seq: getNewPodSeq reuses 50
        s.add(active(50000, "/h9", kube("app", "h9", dec(50))));    // 50 owned by h8: re-init h9
        s.add(active(70000, "/h9", kube("app", "h9", dec(-1))));    // 20s later: proceeds, releases h9's seq
        s.add(active(70000, "/h9", kube("app", "h9", dec(-1))));    // initial record kept: proceeds again
        s.add(inactive(71000, "app", "h9"));                        // removes the kept initial record
        s.add(active(71000, "/h9", kube("app", "h9", dec(-1))));    // fresh init again
        return s;
    }

    // ----------------------------------------------------------------- cpu
    static Map<String, Object> agent(String objName, String objType, List<Object> tags) {
        ObjectPack p = new ObjectPack();
        p.objName = objName;
        p.objType = objType;
        p.objHash = HashUtil.hash(objName);
        p.tags = mapValue(tags);
        AgentManager.active(p); // the original registration (objMap.put, name table)
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("op", "agent");
        step.put("objName", objName);
        step.put("objType", objType);
        step.put("tags", tags);
        return step;
    }

    static Object cpuStatus(String objName) throws Exception {
        Object map = ObjectCpuChecker.objectCpuMap();
        Method get = map.getClass().getMethod("get", Object.class);
        Object opt = get.invoke(map, objName);
        if (!(Boolean) opt.getClass().getMethod("isDefined").invoke(opt)) return null;
        Object status = opt.getClass().getMethod("get").invoke(opt);
        return status.getClass().getMethod("firstOccurenceTime").invoke(status);
    }

    static Map<String, Object> counter(String objName, long time, byte timetype, List<Object> data) throws Exception {
        int startedBefore = AgentCallLog.started.get();
        PerfCounterPack pc = new PerfCounterPack();
        pc.objName = objName;
        pc.time = time;
        pc.timetype = timetype;
        pc.data = mapValue(data);
        ObjectCpuChecker.checkCpu(pc);
        // checkCpu enqueues at most one dump and the daemon takes it concurrently: wait until the queue is
        // empty and every started call is recorded, then a short grace for a call between get() and call().
        long deadline = System.currentTimeMillis() + 5000;
        do {
            if (System.currentTimeMillis() > deadline) throw new IllegalStateException("dump daemon did not drain");
            Thread.sleep(20);
        } while (ObjectCpuChecker.queue().size() > 0 || AgentCallLog.size() < AgentCallLog.started.get() - startedBefore);
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("op", "counter");
        step.put("objName", objName);
        step.put("time", time);
        step.put("timetype", (int) timetype);
        step.put("data", data);
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        out.put("first", cpuStatus(objName));
        out.put("calls", calls());
        step.put("out", out);
        return step;
    }

    static List<Object> adc(boolean enabled, Object threshold, Object duration) {
        return pairs(ScouterConstants.TAG_AUTODUMP_CPU_ENABLED, bool(enabled), ScouterConstants.TAG_AUTODUMP_CPU_THRESHOLD, threshold, ScouterConstants.TAG_AUTODUMP_CPU_DURATION, duration);
    }
    static List<Object> cpu(Object v) { return pairs("ProcCpu", v); }

    static List<Object> cpu() throws Exception {
        CoreRun.running = true;
        List<Object> s = new ArrayList<Object>();
        long t0 = 1760000000000L;
        s.add(agent("/was", "tomcat", adc(true, dec(50), dec(10000))));
        s.add(counter("/was", t0, (byte) 1, cpu(flt(60))));           // streak starts
        s.add(counter("/was", t0 + 5000, (byte) 1, cpu(flt(60))));
        s.add(counter("/was", t0 + 10000, (byte) 1, cpu(flt(60))));   // exactly duration: no dump
        s.add(counter("/was", t0 + 10001, (byte) 1, cpu(flt(60))));   // dump, cleared
        s.add(counter("/was", t0 + 20000, (byte) 1, cpu(flt(60))));
        s.add(counter("/was", t0 + 21000, (byte) 1, cpu(flt(50))));   // == threshold: cleared
        s.add(counter("/was", t0 + 22000, (byte) 1, cpu(dec(51))));   // Decimal over the threshold
        s.add(counter("/was", t0 + 23000, (byte) 1, pairs("Other", dec(1)))); // no ProcCpu: kept
        s.add(counter("/was", t0 + 42001, (byte) 1, cpu(flt(60))));   // older than duration*2: cleared, no dump
        s.add(counter("/was", t0 + 43000, (byte) 3, cpu(flt(60))));   // not REALTIME
        s.add(counter("/was", t0 + 44000, (byte) 1, cpu(dbl(Double.NaN)))); // NaN: cleared
        s.add(counter("/was", t0 + 45000, (byte) 1, cpu(text("99")))); // Text: getFloat 0
        s.add(agent("/was", "tomcat", adc(false, dec(50), dec(10000))));
        s.add(counter("/was", t0 + 46000, (byte) 1, cpu(flt(60))));   // disabled: cleared
        s.add(agent("/host", "linux", adc(true, dec(0), dec(-1))));    // family "host"
        s.add(counter("/host", t0, (byte) 1, cpu(flt(60))));
        s.add(agent("/odd", "nosuchtype", adc(true, dec(0), dec(-1)))); // unknown type
        s.add(counter("/odd", t0, (byte) 1, cpu(flt(60))));
        s.add(counter("/absent", t0, (byte) 1, cpu(flt(60))));        // no agent
        s.add(agent("/neg", "java", adc(true, dec(0), dec(-1))));      // negative duration: dump on the first pack
        s.add(counter("/neg", t0, (byte) 1, cpu(dec(1))));
        s.add(agent("/wrap", "jboss", adc(true, dbl(1e12), dec(1 << 30)))); // threshold saturates, duration*2 wraps
        s.add(counter("/wrap", t0, (byte) 1, cpu(dbl(Math.pow(2, 31) * 1.01))));
        s.add(counter("/wrap", t0 + 1, (byte) 1, cpu(dbl(Math.pow(2, 31) * 1.01))));
        s.add(agent("/fth", "tomcat", adc(true, flt(10.9f), flt(-0.5f)))); // Float tags narrow to 10 and 0
        s.add(counter("/fth", t0, (byte) 1, cpu(flt(10.5f))));
        s.add(counter("/fth", t0 + 1, (byte) 1, cpu(flt(10.5f))));
        s.add(saturate(t0));
        return s;
    }

    // RequestQueue(100).put drops when full: the daemon holds one call (gate closed), 100 wait, 4 are dropped.
    static Map<String, Object> saturate(long t0) throws Exception {
        agent("/sat", "tomcat", adc(true, dec(0), dec(-1)));
        AgentCallLog.take();
        AgentCallLog.gate = new CountDownLatch(1);
        int triggers = 105;
        PerfCounterPack first = new PerfCounterPack();
        first.objName = "/sat"; first.time = t0; first.timetype = 1; first.data = mapValue(cpu(dec(1)));
        ObjectCpuChecker.checkCpu(first);
        long deadline = System.currentTimeMillis() + 5000;
        while (AgentCallLog.waiting.get() < 1) {
            if (System.currentTimeMillis() > deadline) throw new IllegalStateException("daemon did not take the first dump");
            Thread.sleep(2);
        }
        for (int i = 1; i < triggers; i++) {
            PerfCounterPack pc = new PerfCounterPack();
            pc.objName = "/sat"; pc.time = t0 + i; pc.timetype = 1; pc.data = mapValue(cpu(dec(1)));
            ObjectCpuChecker.checkCpu(pc);
        }
        int queued = ObjectCpuChecker.queue().size();
        CountDownLatch g = AgentCallLog.gate;
        AgentCallLog.gate = null;
        g.countDown();
        deadline = System.currentTimeMillis() + 5000;
        while (ObjectCpuChecker.queue().size() > 0 || AgentCallLog.waiting.get() > 0) {
            if (System.currentTimeMillis() > deadline) throw new IllegalStateException("daemon did not drain");
            Thread.sleep(2);
        }
        Thread.sleep(50);
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("op", "saturate");
        step.put("objName", "/sat");
        step.put("triggers", triggers);
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        out.put("queuedWhileBlocked", queued);
        out.put("called", AgentCallLog.take().size());
        step.put("out", out);
        return step;
    }

    // --------------------------------------------------------------- geoip
    static int lastLookups;

    static List<Object> ip(int... octets) { List<Object> l = new ArrayList<Object>(); for (int o : octets) l.add(o); return l; }
    static byte[] bytes(List<Object> ip) { if (ip == null) return null; byte[] b = new byte[ip.size()]; for (int i = 0; i < b.length; i++) b[i] = (byte) ((Integer) ip.get(i)).intValue(); return b; }
    static String dotted(List<Object> ip) throws Exception { return InetAddress.getByAddress(bytes(ip)).getHostAddress(); }

    static Map<String, Object> db(boolean ok) {
        GISDataUtil.ok_$eq(ok);
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("op", "db");
        step.put("ok", ok);
        return step;
    }
    // {"op":"table","entries":[{ip, found, country, city}], "fallback": {country, city} | null}
    static Map<String, Object> table(List<Object> entries, String[] fallback) throws Exception {
        GISDataUtil.clear();
        for (Object o : entries) {
            @SuppressWarnings("unchecked") Map<String, Object> e = (Map<String, Object>) o;
            String ip = dotted((List<Object>) e.get("ip"));
            if ((Boolean) e.get("found")) GISDataUtil.put(ip, (String) e.get("country"), (String) e.get("city"));
            else GISDataUtil.putMissing(ip);
        }
        GISDataUtil.fallback_$eq(fallback);
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("op", "table");
        step.put("entries", entries);
        if (fallback == null) step.put("fallback", null);
        else { Map<String, Object> f = new LinkedHashMap<String, Object>(); f.put("country", fallback[0]); f.put("city", fallback[1]); step.put("fallback", f); }
        return step;
    }
    static Map<String, Object> entry(List<Object> ip, boolean found, String country, String city) {
        Map<String, Object> e = new LinkedHashMap<String, Object>();
        e.put("ip", ip); e.put("found", found); e.put("country", country); e.put("city", city);
        return e;
    }

    static List<Object> texts() {
        List<Object> out = new ArrayList<Object>();
        for (Object[] a : TextPermWR.take()) out.add(java.util.Arrays.asList(a[0], a[1], a[2]));
        return out;
    }
    static int lookupsDelta() { int now = GISDataUtil.lookups(); int d = now - lastLookups; lastLookups = now; return d; }

    static Map<String, Object> xlog(List<Object> ip, String countryCode) {
        XLogPack p = new XLogPack();
        p.ipaddr = bytes(ip);
        p.countryCode = countryCode;
        GeoIpUtil.setNationAndCity(p);
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("op", "xlog");
        step.put("ip", ip);
        step.put("countryCode", countryCode);
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        out.put("countryCode", p.countryCode);
        out.put("city", p.city);
        out.put("lookups", lookupsDelta());
        out.put("texts", texts());
        step.put("out", out);
        return step;
    }

    // count XLogs with addresses base+i (big-endian increment of the last three octets); cities are either
    // the fallback's (same) or "c<i>" via the table (distinct).
    static Map<String, Object> bulk(int first, int start, int count, boolean distinctCities) throws Exception {
        if (distinctCities) {
            GISDataUtil.clear();
            for (int i = start; i < start + count; i++) GISDataUtil.put(dotted(bulkIP(first, i)), "ZZ", "c" + i);
        }
        int texts = 0;
        for (int i = start; i < start + count; i++) {
            XLogPack p = new XLogPack();
            p.ipaddr = bytes(bulkIP(first, i));
            GeoIpUtil.setNationAndCity(p);
        }
        texts = TextPermWR.take().size();
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("op", "bulk");
        step.put("first", first);
        step.put("start", start);
        step.put("count", count);
        step.put("distinctCities", distinctCities);
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        out.put("lookups", lookupsDelta());
        out.put("texts", texts);
        step.put("out", out);
        return step;
    }
    static List<Object> bulkIP(int first, int i) { return ip(first, (i >> 16) & 0xff, (i >> 8) & 0xff, i & 0xff); }

    // Every second octet for the private-range first octets: which addresses reach the lookup.
    static Map<String, Object> sweep(int[] firsts) throws Exception {
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        for (int a : firsts) {
            List<Object> looked = new ArrayList<Object>();
            for (int b = 0; b < 256; b++) {
                XLogPack p = new XLogPack();
                p.ipaddr = bytes(ip(a, b, 7, 7));
                GeoIpUtil.setNationAndCity(p);
                if (lookupsDelta() > 0) looked.add(b);
            }
            out.put(String.valueOf(a), looked);
        }
        TextPermWR.take();
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("op", "sweep");
        List<Object> f = new ArrayList<Object>();
        for (int a : firsts) f.add(a);
        step.put("firsts", f);
        step.put("out", out);
        return step;
    }

    static List<Object> geoip() throws Exception {
        List<Object> s = new ArrayList<Object>();
        List<Object> entries = new ArrayList<Object>();
        entries.add(entry(ip(8, 8, 8, 8), true, "US", "Mountain View"));
        entries.add(entry(ip(1, 1, 1, 1), true, "AU", ""));
        entries.add(entry(ip(9, 9, 9, 9), true, null, "Nowhere"));
        entries.add(entry(ip(4, 4, 4, 4), true, "DE", null));
        entries.add(entry(ip(2, 2, 2, 2), false, null, null));
        entries.add(entry(ip(192, 168, 1, 1), true, "XX", "Private"));
        s.add(table(entries, null));
        s.add(xlog(ip(8, 8, 8, 8), null));        // not loaded: nothing
        s.add(db(true));
        s.add(xlog(ip(8, 8, 8, 8), null));        // lookup, city text
        s.add(xlog(ip(8, 8, 8, 8), ""));          // cached, empty country is not set
        s.add(xlog(ip(8, 8, 8, 8), "KR"));        // country already set
        s.add(xlog(ip(1, 1, 1, 1), null));        // empty city -> unknown
        s.add(xlog(ip(4, 4, 4, 4), null));        // null city -> unknown (no second text)
        s.add(xlog(ip(9, 9, 9, 9), null));        // null country
        s.add(xlog(ip(2, 2, 2, 2), null));        // not found
        s.add(xlog(ip(2, 2, 2, 2), null));        // not cached
        s.add(xlog(ip(10, 1, 1, 1), null));       // private
        s.add(xlog(ip(172, 16, 1, 1), null));     // private
        s.add(xlog(ip(172, 200, 1, 1), null));    // signed byte -56: public
        s.add(xlog(ip(192, 168, 1, 1), null));    // never private (byte == 168)
        s.add(xlog(ip(0, 0, 0, 0), null));        // ipInt 0
        s.add(xlog(ip(8, 8, 8), null));           // length 3
        s.add(xlog(null, null));                  // no address
        s.add(db(false));
        s.add(xlog(ip(8, 8, 8, 8), null));        // cached but DB not loaded: nothing
        s.add(db(true));
        s.add(table(new ArrayList<Object>(), new String[] {"ZZ", "Somewhere"}));
        s.add(sweep(new int[] {10, 127, 172, 192, 8}));
        s.add(bulk(11, 0, 10001, false));         // 10001 new addresses: one text (Somewhere)
        s.add(bulk(11, 1, 1, false));             // second address still cached
        s.add(bulk(11, 0, 1, false));             // first address evicted (FIFO)
        s.add(bulk(12, 0, 10001, true));          // 10001 cities: the first city leaves citySet
        s.add(table(new ArrayList<Object>(), new String[] {"ZZ", "Somewhere"}));
        s.add(bulk(13, 0, 1, false));             // "Somewhere" was evicted from citySet: written again
        List<Object> again = new ArrayList<Object>();
        again.add(entry(ip(14, 0, 0, 1), true, "ZZ", "c0"));
        s.add(table(again, null));
        s.add(xlog(ip(14, 0, 0, 1), null));       // city c0 was evicted from citySet: written again
        return s;
    }

    // -------------------------------------------------------------- alerts
    // The original AgentManager (active, inactive, the 1s daemon, ObjectMap) runs with CoreRun.running.
    // Each step has a logical time T; the stored packs' wakeup is rebased before the step so that
    // real now - wakeup equals the logical age, and the daemon decides with real time. Ticks use ages
    // at least 2s away from every dead-time boundary, so the daemon's scheduling cannot change them.
    // Timestamps in messages are rewritten from the rebased (real) wakeup to ALERT_BASE + logical.
    static final long ALERT_BASE = 1760000000000L;
    static final Map<Integer, Long> logicalWakeup = new LinkedHashMap<Integer, Long>();
    static final Map<Integer, Long> realWakeup = new LinkedHashMap<Integer, Long>();

    static String ts(long ms) { return new java.sql.Timestamp(ms).toString(); }

    // Timestamp.toString drops trailing zeros ("...05.12" for 120ms), so one instant can be a
    // prefix of the next ("...05.121"): a timestamp is rewritten only where no digit follows it.
    static String rewriteTs(String msg, long real, long logical) {
        return msg.replaceAll(java.util.regex.Pattern.quote(ts(real)) + "(?!\\d)",
                java.util.regex.Matcher.quoteReplacement(ts(ALERT_BASE + logical)));
    }

    static void rebaseObjects(long t) {
        long real = System.currentTimeMillis();
        java.util.Enumeration<ObjectPack> en = AgentManager.getObjPacks();
        while (en.hasMoreElements()) {
            ObjectPack o = en.nextElement();
            Long l = logicalWakeup.get(o.objHash);
            if (l != null) {
                o.wakeup = real - (t - l);
                realWakeup.put(o.objHash, o.wakeup);
            }
        }
    }

    // A MAYBE_OBJ_DUP message prints the old pack's wakeup, and active() then stamps the stored
    // pack with a second System.currentTimeMillis(): the two differ by a millisecond when a
    // millisecond boundary falls between them, so every instant of the call [dupFrom, dupTo] is
    // rewritten to the step's logical time.
    static List<Object> alertRecords(long t, Integer dupHash, long dupFrom, long dupTo) {
        List<Object> out = new ArrayList<Object>();
        List<AlertPack> all = new ArrayList<AlertPack>();
        List<String> via = new ArrayList<String>();
        for (AlertPack a : AlertCore.take()) { all.add(a); via.add("core"); }
        for (AlertPack a : AlertCache.take()) { all.add(a); via.add("cache"); }
        for (int i = 0; i < all.size(); i++) {
            AlertPack a = all.get(i);
            String msg = a.message;
            for (Map.Entry<Integer, Long> e : realWakeup.entrySet()) {
                Long l = logicalWakeup.get(e.getKey());
                if (l != null) msg = rewriteTs(msg, e.getValue(), l);
            }
            if (dupHash != null) for (long r = dupFrom; r <= dupTo; r++) msg = rewriteTs(msg, r, t);
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("via", via.get(i));
            m.put("title", a.title);
            m.put("level", (int) a.level);
            m.put("objHash", a.objHash);
            m.put("objType", a.objType);
            m.put("message", msg);
            out.add(m);
        }
        return out;
    }

    static Map<String, Object> aliveMap() {
        Map<String, Object> m = new TreeMap<String, Object>();
        java.util.Enumeration<ObjectPack> en = AgentManager.getObjPacks();
        while (en.hasMoreElements()) { ObjectPack o = en.nextElement(); m.put(o.objName, o.alive); }
        return m;
    }

    static Map<String, Object> activeObject(long t, String objName, String objType, String addr, String version, List<Object> tags) {
        rebaseObjects(t);
        ObjectPack p = new ObjectPack();
        p.objName = objName;
        p.objType = objType;
        p.address = addr;
        p.version = version;
        p.tags = mapValue(tags);
        long from = System.currentTimeMillis();
        AgentManager.active(p);
        ObjectPack stored = AgentManager.getAgent(HashUtil.hash(objName));
        long real = stored.wakeup;
        List<Object> alerts = alertRecords(t, stored.objHash, from, real);
        logicalWakeup.put(stored.objHash, t);
        realWakeup.put(stored.objHash, real);
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("op", "active");
        step.put("t", t);
        step.put("objName", objName);
        step.put("objType", objType);
        step.put("address", addr);
        step.put("version", version);
        step.put("tags", tags);
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        out.put("alerts", alerts);
        out.put("alive", aliveMap());
        step.put("out", out);
        return step;
    }

    static Map<String, Object> tickObjects(long t) throws Exception {
        rebaseObjects(t);
        Thread.sleep(1600); // at least one AgentManager daemon pass (1000ms period)
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("op", "tick");
        step.put("t", t);
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        out.put("alerts", alertRecords(t, null, 0, -1));
        out.put("alive", aliveMap());
        step.put("out", out);
        return step;
    }

    static Map<String, Object> conf(String key, int value) throws Exception {
        java.lang.reflect.Field f = Configure.class.getField(key);
        f.setInt(Configure.getInstance(), value);
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("op", "conf");
        step.put("key", key);
        step.put("value", value);
        return step;
    }

    static Map<String, Object> timestamps() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        for (long ms : new long[] {ALERT_BASE, ALERT_BASE + 1, ALERT_BASE + 120, ALERT_BASE + 999, 0L, 1L, 86399999L}) m.put(String.valueOf(ms), ts(ms));
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("op", "timestamps");
        step.put("out", m);
        return step;
    }

    // ObjectMap.objectTable is an IntKeyMap: the daemon's (and so the alerts') order. 300 puts of
    // object-name hashes with removals in between, through two rehashes.
    static Map<String, Object> intKeyMap() {
        scouter.util.IntKeyMap<String> m = new scouter.util.IntKeyMap<String>();
        List<Object> ops = new ArrayList<Object>();
        for (int i = 0; i < 300; i++) {
            int k = HashUtil.hash("/obj" + i);
            m.put(k, "x");
            ops.add(k);
            if (i % 7 == 3) {
                int r = HashUtil.hash("/obj" + (i / 2));
                m.remove(r);
                ops.add("-" + r);
            }
        }
        List<Object> order = new ArrayList<Object>();
        scouter.util.IntEnumer en = m.keys();
        while (en.hasMoreElements()) order.add(en.nextInt());
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("op", "intkeymap");
        step.put("ops", ops);
        step.put("out", order);
        return step;
    }

    static List<Object> alerts() throws Exception {
        CoreRun.running = true;
        List<Object> s = new ArrayList<Object>();
        s.add(timestamps());
        s.add(intKeyMap());
        List<Object> rich = pairs("k", text("v"), "n", dec(3), "b", bool(true), "f", flt(1.5f), "d", dbl(2.25),
                "neg", dec(-7), "big", dbl(1e20), "small", flt(1.0E-4f), "e", text(""), "t2", dbl(0.1),
                "a1", dec(1), "a2", dec(2), "a3", dec(3), "a4", dec(4), "a5", dec(5), "a6", dec(6));
        s.add(activeObject(0, "/a", "tomcat", "10.0.0.1", "1.0", rich));
        s.add(activeObject(0, "/sub", "datasource", "", "", pairs()));
        s.add(activeObject(0, "/unk", "unknowntype", "", "", pairs()));
        s.add(activeObject(0, "/z", "zipkin", "", "", pairs()));
        s.add(activeObject(0, "/zs", "z$svc", "", "", pairs()));
        s.add(activeObject(0, "/neg", "tomcat", "", "", pairs("deadTime", dec(-1))));
        s.add(activeObject(0, "/fl", "tomcat", "", "", pairs("deadTime", flt(2500.9f))));
        s.add(activeObject(0, "/batch", "batch", "", "", pairs("deadTime", dec(-1))));
        s.add(tickObjects(5000));   // /neg, /fl dead
        s.add(tickObjects(12000));  // /a, /sub, /unk dead (8000)
        s.add(activeObject(13000, "/a", "tomcat", "10.0.0.1", "1.0", pairs("k", text("w"))));
        s.add(conf("object_zipkin_deadtime_ms", 20000));
        s.add(tickObjects(25000));  // /z, /zs (20000) and /a (13000 + 8000) dead
        s.add(conf("object_inactive_alert_level", 258));
        s.add(activeObject(26000, "/a", "tomcat", "10.0.0.1", "1.0", pairs("k", text("x"))));
        s.add(activeObject(26000, "/dup", "tomcat", "", "v0", pairs()));
        for (int i = 1; i <= 20; i++) {
            s.add(activeObject(26000 + i * 100, "/dup", "tomcat", "", "v" + (i % 2), pairs("i", dec(i))));
        }
        s.add(activeObject(28200, "/dup", "tomcat", "", "v1", pairs("i", dec(21))));
        return s;
    }

    // ------------------------------------------------------------- output
    static String hex(byte[] b) { StringBuilder sb = new StringBuilder(); for (byte x : b) sb.append(String.format("%02x", x & 0xff)); return sb.toString(); }

    @SuppressWarnings("unchecked")
    static String json(Object o, String indent) {
        if (o == null) return "null";
        if (o instanceof String) return quote((String) o);
        if (o instanceof Boolean || o instanceof Integer || o instanceof Long || o instanceof Short || o instanceof Byte) return String.valueOf(o);
        if (o instanceof Float || o instanceof Double) {
            double d = ((Number) o).doubleValue();
            if (Double.isNaN(d)) return "\"NaN\"";
            if (o instanceof Float) return Float.toString((Float) o);
            return Double.toString(d);
        }
        String in = indent + "  ";
        if (o instanceof Map) {
            Map<String, Object> m = (Map<String, Object>) o;
            if (m.isEmpty()) return "{}";
            StringBuilder sb = new StringBuilder("{\n");
            int i = 0;
            for (Map.Entry<String, Object> e : m.entrySet()) {
                sb.append(in).append(quote(e.getKey())).append(": ").append(json(e.getValue(), in));
                sb.append(++i < m.size() ? ",\n" : "\n");
            }
            return sb.append(indent).append("}").toString();
        }
        if (o instanceof List) {
            List<Object> l = (List<Object>) o;
            if (l.isEmpty()) return "[]";
            boolean flat = true;
            for (Object x : l) if (x instanceof Map || x instanceof List) flat = false;
            if (flat) {
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < l.size(); i++) sb.append(i > 0 ? ", " : "").append(json(l.get(i), in));
                return sb.append("]").toString();
            }
            StringBuilder sb = new StringBuilder("[\n");
            for (int i = 0; i < l.size(); i++) sb.append(in).append(json(l.get(i), in)).append(i + 1 < l.size() ? ",\n" : "\n");
            return sb.append(indent).append("]").toString();
        }
        throw new IllegalArgumentException(o.getClass().getName());
    }
    static String quote(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            if (c == '"' || c == '\\') sb.append('\\').append(c);
            else if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
            else sb.append(c);
        }
        return sb.append('"').toString();
    }
}
