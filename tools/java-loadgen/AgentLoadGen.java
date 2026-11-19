import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

import scouter.Version;
import scouter.agent.Configure;
import scouter.agent.netio.data.DataProxy;
import scouter.agent.netio.data.UDPDataSendThread;
import scouter.agent.netio.data.net.DataUdpAgent;
import scouter.agent.trace.TraceContext;
import scouter.io.DataInputX;
import scouter.lang.pack.ObjectPack;
import scouter.net.NetCafe;
import scouter.lang.pack.PackEnum;
import scouter.lang.pack.XLogDiscardTypes;
import scouter.lang.pack.XLogPack;
import scouter.lang.step.MethodStep;
import scouter.lang.step.SqlStep3;
import scouter.lang.step.Step;
import scouter.lang.step.StepSingle;
import scouter.util.HashUtil;

/**
 * XLog load generator on the original Java agent send path.
 *
 * Every transaction goes through the original DataProxy exactly as TraceMain.endHttpService does:
 * sendServiceName/sendHashedMessage (text dictionary, sent once per hash), sendProfile (XLogProfilePack2,
 * split by profile_step_max_count) and sendXLog. DataProxy, UDPDataSendThread and DataUdpAgent are the
 * original classes; only the DatagramSocket inside DataUdpAgent is replaced by a subclass whose send()
 * counts datagrams and calls the original DatagramSocket.send.
 *
 * txid = (runId << 32) | seq, seq = 1..assigned. A seq whose XLog datagram failed is listed in the report.
 */
public class AgentLoadGen {
    static final LongAdder datagrams = new LongAdder();
    static final LongAdder bytes = new LongAdder();
    static final LongAdder sendErrors = new LongAdder();
    static final LongAdder single = new LongAdder();
    static final LongAdder multi = new LongAdder();
    static final LongAdder multiPacks = new LongAdder();
    static final LongAdder mtuFragments = new LongAdder();
    static final LongAdder mtuPacks = new LongAdder();
    static final LongAdder unknownDatagrams = new LongAdder();
    static final Map<Byte, LongAdder> packTypes = new ConcurrentHashMap<>();
    static final ThreadLocal<boolean[]> sendFailed = ThreadLocal.withInitial(() -> new boolean[1]);

    /** Counts what the original DataUdpAgent writes; the bytes on the wire are unchanged. */
    static final class CountingSocket extends DatagramSocket {
        CountingSocket(InetSocketAddress local) throws SocketException {
            super(local);
        }

        @Override
        public void send(DatagramPacket p) throws IOException {
            try {
                super.send(p);
            } catch (IOException e) {
                sendErrors.increment();
                sendFailed.get()[0] = true;
                throw e;
            }
            datagrams.increment();
            bytes.add(p.getLength());
            classify(p.getData(), p.getOffset(), p.getLength());
        }
    }

    static void countType(byte type) {
        packTypes.computeIfAbsent(type, k -> new LongAdder()).increment();
    }

    static void classify(byte[] b, int off, int len) {
        if (len < 4) {
            unknownDatagrams.increment();
            return;
        }
        int magic = DataInputX.toInt(b, off);
        try {
            if (magic == NetCafe.UDP_CAFE && len > 4) { // one pack
                single.increment();
                countType(b[off + 4]);
            } else if (magic == NetCafe.UDP_CAFE_N) { // count + packs
                multi.increment();
                int n = DataInputX.toShort(b, off + 4);
                multiPacks.add(n);
                DataInputX in = new DataInputX(b, off + 6);
                for (int i = 0; i < n; i++) {
                    countType(in.readPack().getPackType());
                }
            } else if (magic == NetCafe.UDP_CAFE_MTU && len > 24) { // objHash pkid total num blob
                mtuFragments.increment();
                int num = DataInputX.toShort(b, off + 18);
                if (num == 0) {
                    mtuPacks.increment();
                    int lead = b[off + 20] & 0xff;
                    int data = off + 21 + (lead == 255 ? 2 : lead == 254 ? 4 : 0);
                    countType(b[data]);
                }
            } else {
                unknownDatagrams.increment();
            }
        } catch (IOException | RuntimeException e) {
            unknownDatagrams.increment();
        }
    }

    static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    static Map<String, String> parse(String[] args) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (!a.startsWith("--") || i + 1 >= args.length) {
                throw new IllegalArgumentException("expected --name value pairs, got " + a);
            }
            m.put(a.substring(2), args[++i]);
        }
        return m;
    }

    static void require(boolean ok, String message) {
        if (!ok) {
            throw new IllegalArgumentException(message);
        }
    }

    static final String[] SQL_SHAPES = {
            "SELECT ID, NAME, STATUS FROM LOADGEN_ITEM_%d WHERE ID = ?",
            "UPDATE LOADGEN_ORDER_%d SET STATUS = ? WHERE ORDER_ID = ?",
            "INSERT INTO LOADGEN_EVENT_%d (ID, KIND, CREATED) VALUES (?, ?, ?)",
            "SELECT COUNT(*) FROM LOADGEN_LOG_%d WHERE CREATED > ?",
    };

    /** Profile of at least `size` bytes (Step.toBytes), alternating MethodStep and SqlStep3. */
    static Step[] profileTemplate(int size, int variant) {
        List<Step> steps = new ArrayList<>();
        int bytes = 0;
        int index = 0;
        while (bytes < size) {
            StepSingle s;
            if (index % 2 == 0) {
                MethodStep m = new MethodStep();
                m.hash = DataProxy.sendMethodName("com.example.loadgen.Service" + variant + ".step" + (index % 64) + "()");
                m.elapsed = 1 + index % 7;
                s = m;
            } else {
                SqlStep3 q = new SqlStep3();
                q.hash = DataProxy.sendSqlText(String.format(SQL_SHAPES[index % SQL_SHAPES.length], (index / 2) % 32));
                q.elapsed = 2 + index % 11;
                q.param = "1," + index;
                q.updated = 1;
                s = q;
            }
            s.index = index;
            s.parent = -1;
            s.start_time = index;
            steps.add(s);
            bytes = Step.toBytes(steps).length;
            index++;
        }
        return steps.toArray(new Step[0]);
    }

    public static void main(String[] argv) throws Exception {
        Map<String, String> a = parse(argv);
        String target = a.getOrDefault("target", "");
        int colon = target.lastIndexOf(':');
        require(colon > 0, "--target host:port is required");
        InetAddress host = InetAddress.getByName(target.substring(0, colon));
        int port = Integer.parseInt(target.substring(colon + 1));
        require(host.isLoopbackAddress(), "--target must be a loopback address");
        require(port > 0 && port < 65536, "--target port out of range");
        int objects = Integer.parseInt(a.getOrDefault("objects", "1"));
        int threads = Integer.parseInt(a.getOrDefault("threads", "1"));
        double rate = Double.parseDouble(a.getOrDefault("rate", "100"));
        double duration = Double.parseDouble(a.getOrDefault("duration", "10"));
        double profileRatio = Double.parseDouble(a.getOrDefault("profile-ratio", "0"));
        long seed = Long.parseLong(a.getOrDefault("seed", "1"));
        int services = Integer.parseInt(a.getOrDefault("services", "100"));
        String[] sizeText = a.getOrDefault("profile-sizes", "4096").split(",");
        int[] sizes = new int[sizeText.length];
        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = Integer.parseInt(sizeText[i].trim());
            require(sizes[i] > 0 && sizes[i] <= 4 << 20, "--profile-sizes must be 1..4194304 bytes");
        }
        long runId = a.containsKey("run-id") ? Long.parseLong(a.get("run-id")) : (mix(seed) & 0x7fffffffL);
        if (runId == 0) {
            runId = 1;
        }
        require(runId > 0 && runId <= 0x7fffffffL, "--run-id must be 1..2147483647");
        require(objects >= 1 && objects <= 10000, "--objects must be 1..10000");
        require(threads >= 1 && threads <= 256, "--threads must be 1..256");
        require(rate >= 0, "--rate must be >= 0 (0 = unlimited)");
        require(duration > 0 && duration <= 3600, "--duration must be 0 < s <= 3600");
        require(profileRatio >= 0 && profileRatio <= 1, "--profile-ratio must be 0..1");
        // Service names are pushed through the 1024-entry UDPDataSendThread queue on first use,
        // like the agent; the heartbeat thread keeps the rest of the queue below 512.
        require(services >= 1 && services <= 500, "--services must be 1..500");

        Configure conf = Configure.getInstance();
        Map<String, Object> changed = new LinkedHashMap<>();
        conf.net_collector_ip = host.getHostAddress();
        conf.net_collector_udp_port = port;
        conf.net_local_udp_ip = "127.0.0.1";
        changed.put("net_collector_ip", conf.net_collector_ip);
        changed.put("net_collector_udp_port", port);
        changed.put("net_local_udp_ip", conf.net_local_udp_ip);
        for (Map.Entry<String, String> e : a.entrySet()) {
            if (!e.getKey().startsWith("set.")) {
                continue;
            }
            String name = e.getKey().substring(4);
            require(name.equals("net_udp_packet_max_bytes") || name.equals("net_udp_collection_interval_ms")
                    || name.equals("profile_step_max_count"), "--set." + name + " is not supported");
            Field f = Configure.class.getField(name);
            if (f.getType() == long.class) {
                f.setLong(conf, Long.parseLong(e.getValue()));
            } else {
                f.setInt(conf, Integer.parseInt(e.getValue()));
            }
            changed.put(name, f.get(conf));
        }
        require(conf.net_udp_packet_max_bytes >= 512 && conf.net_udp_packet_max_bytes <= 60000,
                "net_udp_packet_max_bytes must be 512..60000");
        // Original Configure clamps profile_step_max_count to >= 128.
        require(conf.profile_step_max_count >= 128, "profile_step_max_count must be >= 128");

        // Open the original agent socket first, then swap in the counting subclass before any pack is sent
        // (DataProxy's static init starts UDPDataSendThread).
        DataUdpAgent udp = DataUdpAgent.getInstance();
        Field socketField = DataUdpAgent.class.getDeclaredField("datagram");
        socketField.setAccessible(true);
        ((DatagramSocket) socketField.get(udp)).close();
        CountingSocket socket = new CountingSocket(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
        socketField.set(udp, socket);

        Map<String, Object> agentConf = new LinkedHashMap<>();
        for (Field f : Configure.class.getFields()) {
            if (!Modifier.isStatic(f.getModifiers())) {
                agentConf.put(f.getName(), f.get(conf));
            }
        }

        int[] objHashes = new int[objects];
        String[] objNames = new String[objects];
        for (int i = 0; i < objects; i++) {
            objNames[i] = String.format("/loadgen-%d/agent-%05d", runId, i + 1);
            objHashes[i] = HashUtil.hash(objNames[i]);
        }
        String[] serviceNames = new String[services];
        for (int i = 0; i < services; i++) {
            serviceNames[i] = String.format("/loadgen/api/v1/resource%03d", i + 1);
        }

        UDPDataSendThread collect = UDPDataSendThread.getInstance();
        AtomicInteger heartbeatRounds = new AtomicInteger();
        Thread heartbeat = new Thread(() -> {
            // AgentHeartBeat.alive: one ObjectPack per object every 2000 ms (@Counter default interval).
            while (!Thread.currentThread().isInterrupted()) {
                long round = System.nanoTime();
                for (int i = 0; i < objects; i++) {
                    while (collect.getQueueSize() >= 512) {
                        LockSupport.parkNanos(1_000_000);
                        if (Thread.currentThread().isInterrupted()) {
                            return;
                        }
                    }
                    ObjectPack p = new ObjectPack();
                    p.objType = "java";
                    p.objHash = objHashes[i];
                    p.objName = objNames[i];
                    p.version = Version.getAgentFullVersion();
                    p.address = "127.0.0.1";
                    DataProxy.sendHeartBeat(p);
                }
                heartbeatRounds.incrementAndGet();
                long sleep = 2_000_000_000L - (System.nanoTime() - round);
                if (sleep > 0) {
                    try {
                        Thread.sleep(sleep / 1_000_000, (int) (sleep % 1_000_000));
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        }, "loadgen-heartbeat");
        heartbeat.setDaemon(true);
        heartbeat.start();
        // Let the first heartbeat round leave before XLogs, like an agent that has registered.
        while (heartbeatRounds.get() == 0) {
            Thread.sleep(10);
        }

        Configure.bindObjHash(objHashes[0]);
        Step[][] templates = new Step[sizes.length][];
        int[] templateBytes = new int[sizes.length];
        for (int i = 0; i < sizes.length; i++) {
            templates[i] = profileTemplate(sizes[i], i);
            templateBytes[i] = Step.toBytes(templates[i]).length;
        }

        com.sun.management.OperatingSystemMXBean os =
                (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        AtomicLong seqs = new AtomicLong();
        LongAdder xlogSent = new LongAdder();
        LongAdder xlogFailed = new LongAdder();
        LongAdder profiled = new LongAdder();
        ConcurrentLinkedQueue<Long> failedSeqs = new ConcurrentLinkedQueue<>();
        final long fRunId = runId;
        long datagramsBefore = datagrams.sum();
        long bytesBefore = bytes.sum();
        long cpuStart = os.getProcessCpuTime();
        long start = System.nanoTime();
        long deadline = start + (long) (duration * 1e9);
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int ti = t;
            workers[t] = new Thread(() -> {
                String threadName = Thread.currentThread().getName();
                long interval = rate > 0 ? (long) (threads * 1e9 / rate) : 0;
                long next = start + (rate > 0 ? (long) (ti * 1e9 / rate) : 0);
                boolean[] failed = sendFailed.get();
                while (true) {
                    if (interval > 0) {
                        long now = System.nanoTime();
                        if (next >= deadline) {
                            break;
                        }
                        if (next > now) {
                            LockSupport.parkNanos(next - now);
                        }
                        next += interval;
                    }
                    if (System.nanoTime() >= deadline) {
                        break;
                    }
                    long seq = seqs.incrementAndGet();
                    long r = mix(seed ^ (seq * 0x9E3779B97F4A7C15L));
                    int obj = (int) ((seq - 1) % objects);
                    Configure.bindObjHash(objHashes[obj]);
                    int elapsed = 1 + (int) ((r >>> 8) % 1000);
                    long txid = (fRunId << 32) | seq;

                    TraceContext ctx = new TraceContext();
                    ctx.txid = txid;
                    ctx.xType = 0;
                    ctx.discardType = XLogDiscardTypes.XLogDiscard.NONE;
                    ctx.startTime = System.currentTimeMillis() - elapsed;
                    ctx.serviceHash = DataProxy.sendServiceName(serviceNames[(int) ((r >>> 20) % services)]);
                    if (((r >>> 11) & ((1L << 53) - 1)) * 0x1.0p-53 < profileRatio) {
                        Step[] steps = templates[(int) ((r >>> 40) % templates.length)];
                        DataProxy.sendProfile(steps, ctx);
                        profiled.increment();
                    }

                    XLogPack pack = new XLogPack();
                    pack.elapsed = elapsed;
                    pack.service = ctx.serviceHash;
                    pack.threadNameHash = DataProxy.sendHashedMessage(threadName);
                    pack.xType = ctx.xType;
                    pack.txid = ctx.txid;
                    pack.gxid = ctx.gxid;
                    pack.status = 200;
                    pack.sqlCount = (int) (r & 7);
                    pack.sqlTime = pack.sqlCount * 2;
                    pack.ipaddr = new byte[] {127, 0, 0, 1};
                    pack.userid = r >>> 48;
                    pack.discardType = XLogDiscardTypes.DISCARD_NONE;
                    pack.profileSize = ctx.profileSize;
                    pack.profileCount = ctx.profileCount;
                    pack.cpu = elapsed / 4;
                    pack.kbytes = 16;
                    failed[0] = false;
                    DataProxy.sendXLog(pack);
                    if (failed[0]) {
                        xlogFailed.increment();
                        if (failedSeqs.size() < 100000) {
                            failedSeqs.add(seq);
                        }
                    } else {
                        xlogSent.increment();
                    }
                }
            }, "loadgen-worker-" + t);
            workers[t].start();
        }
        for (Thread w : workers) {
            w.join();
        }
        long sendEnd = System.nanoTime();
        long cpuSend = os.getProcessCpuTime() - cpuStart;
        long sendDatagrams = datagrams.sum() - datagramsBefore;
        long sendBytes = bytes.sum() - bytesBefore;
        heartbeat.interrupt();
        heartbeat.join(5000);
        // Drain the original UDPDataSendThread queue (texts, heartbeats) before reading the counters.
        long drainDeadline = System.nanoTime() + 5_000_000_000L;
        while (collect.getQueueSize() > 0 && System.nanoTime() < drainDeadline) {
            Thread.sleep(10);
        }
        Thread.sleep(conf.net_udp_collection_interval_ms + 200);
        int queueLeft = collect.getQueueSize();

        long assigned = seqs.get();
        if (a.containsKey("txid-file")) {
            java.util.Set<Long> bad = new java.util.HashSet<>(failedSeqs);
            Path path = Paths.get(a.get("txid-file"));
            try (BufferedWriter w = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
                for (long s = 1; s <= assigned; s++) {
                    if (!bad.contains(s)) {
                        w.write(Long.toString((runId << 32) | s));
                        w.newLine();
                    }
                }
            }
        }

        double wall = (sendEnd - start) / 1e9;
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("tool", "java-loadgen");
        report.put("tps_note", "sender-side counts only; completed XLog TPS needs the server-side stored/queried count");
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("target", conf.net_collector_ip + ":" + port);
        params.put("objects", objects);
        params.put("threads", threads);
        params.put("rate_target_per_s", rate);
        params.put("duration_s", duration);
        params.put("profile_ratio", profileRatio);
        params.put("profile_sizes", sizes);
        params.put("profile_template_bytes", templateBytes);
        params.put("services", services);
        params.put("seed", seed);
        params.put("run_id", runId);
        report.put("params", params);
        Map<String, Object> txid = new LinkedHashMap<>();
        txid.put("rule", "txid = (run_id << 32) | seq, seq = 1..assigned");
        txid.put("assigned", assigned);
        txid.put("first", assigned > 0 ? (runId << 32) | 1 : 0);
        txid.put("last", assigned > 0 ? (runId << 32) | assigned : 0);
        txid.put("failed_seqs", new ArrayList<>(failedSeqs));
        txid.put("failed_seqs_truncated", xlogFailed.sum() > failedSeqs.size());
        txid.put("object_rule", "objHash = HashUtil.hash(\"/loadgen-<run_id>/agent-<NNNNN>\"), object = (seq-1) % objects");
        report.put("txid", txid);
        Map<String, Object> x = new LinkedHashMap<>();
        x.put("sent", xlogSent.sum());
        x.put("failed", xlogFailed.sum());
        x.put("with_profile", profiled.sum());
        x.put("send_wall_s", wall);
        x.put("sent_per_s", xlogSent.sum() / wall);
        report.put("xlog", x);
        Map<String, Object> net = new LinkedHashMap<>();
        net.put("datagrams", datagrams.sum());
        net.put("bytes", bytes.sum());
        net.put("send_errors", sendErrors.sum());
        net.put("single_datagrams", single.sum());
        net.put("multi_datagrams", multi.sum());
        net.put("multi_packs", multiPacks.sum());
        net.put("mtu_fragments", mtuFragments.sum());
        net.put("mtu_packs", mtuPacks.sum());
        net.put("unknown_datagrams", unknownDatagrams.sum());
        net.put("expected_server_udp_packets", single.sum() + multiPacks.sum() + mtuPacks.sum());
        Map<String, Long> types = new TreeMap<>();
        for (Map.Entry<Byte, LongAdder> e : packTypes.entrySet()) {
            types.put(packName(e.getKey()), e.getValue().sum());
        }
        net.put("packs_by_type", types);
        net.put("send_phase_datagrams_per_s", sendDatagrams / wall);
        net.put("send_phase_bytes_per_s", sendBytes / wall);
        net.put("collect_queue_left", queueLeft);
        report.put("udp", net);
        Map<String, Object> cpu = new LinkedHashMap<>();
        cpu.put("send_phase_process_cpu_s", cpuSend / 1e9);
        cpu.put("send_phase_cpu_pct_of_one_core", cpuSend / 1e7 / wall);
        cpu.put("available_processors", Runtime.getRuntime().availableProcessors());
        cpu.put("java_version", System.getProperty("java.version"));
        report.put("cpu", cpu);
        report.put("agent_conf", agentConf);
        report.put("agent_conf_changed", changed);
        report.put("agent_version", Version.getAgentFullVersion());
        String json = toJson(report);
        if (a.containsKey("report")) {
            Files.write(Paths.get(a.get("report")), (json + "\n").getBytes(StandardCharsets.UTF_8));
        }
        System.out.println(json);
        socket.close();
        System.exit(xlogFailed.sum() == 0 && queueLeft == 0 ? 0 : 3);
    }

    static String packName(byte type) {
        for (Field f : PackEnum.class.getFields()) {
            try {
                if (f.getType() == byte.class && f.getByte(null) == type) {
                    return f.getName();
                }
            } catch (IllegalAccessException e) {
                break;
            }
        }
        return "TYPE_" + type;
    }

    static String toJson(Object o) {
        if (o == null) {
            return "null";
        }
        if (o instanceof String) {
            StringBuilder sb = new StringBuilder("\"");
            for (char c : ((String) o).toCharArray()) {
                if (c == '"' || c == '\\') {
                    sb.append('\\').append(c);
                } else if (c < 0x20) {
                    sb.append(String.format("\\u%04x", (int) c));
                } else {
                    sb.append(c);
                }
            }
            return sb.append('"').toString();
        }
        if (o instanceof Double) {
            double d = (Double) o;
            return Double.isFinite(d) ? String.format(java.util.Locale.ROOT, "%.3f", d) : "null";
        }
        if (o instanceof Number || o instanceof Boolean) {
            return o.toString();
        }
        if (o instanceof int[]) {
            List<Object> l = new ArrayList<>();
            for (int v : (int[]) o) {
                l.add(v);
            }
            return toJson(l);
        }
        if (o instanceof Map) {
            StringBuilder sb = new StringBuilder("{");
            for (Map.Entry<?, ?> e : ((Map<?, ?>) o).entrySet()) {
                if (sb.length() > 1) {
                    sb.append(", ");
                }
                sb.append(toJson(String.valueOf(e.getKey()))).append(": ").append(toJson(e.getValue()));
            }
            return sb.append('}').toString();
        }
        if (o instanceof List) {
            StringBuilder sb = new StringBuilder("[");
            for (Object v : (List<?>) o) {
                if (sb.length() > 1) {
                    sb.append(", ");
                }
                sb.append(toJson(v));
            }
            return sb.append(']').toString();
        }
        return toJson(o.toString());
    }
}
