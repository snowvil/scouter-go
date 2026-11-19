// HTTP input oracle: unchanged TelegrafInputHandler/InfluxSingleLine/CounterProtocol/ScouterTgMtConfig/
// TgConfig (JAXB)/Configure telegraf parsing/CounterManager/CounterHandler/RegisterHandler/MeterCounter/
// CounterTimeCache. NetDataProcessor is a recording seam (the UDP core pipeline is outside this oracle);
// TelegrafOracleAccess only exposes the package-private count(line, remoteAddr) so receivedTime is fixed.
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import scouter.io.DataInputX;
import scouter.io.DataOutputX;
import scouter.lang.Counter;
import scouter.lang.ObjectType;
import scouter.lang.pack.Pack;
import scouter.lang.pack.PerfCounterPack;
import scouter.lang.value.NumberValue;
import scouter.net.RequestCmd;
import scouter.server.Configure;
import scouter.server.CounterManager;
import scouter.server.LoginManager;
import scouter.server.http.handler.CounterHandler;
import scouter.server.http.handler.RegisterHandler;
import scouter.server.http.handler.TelegrafOracleAccess;
import scouter.server.http.model.CounterProtocol;
import scouter.server.http.model.InfluxSingleLine;
import scouter.server.netio.data.NetDataProcessor;

final class TelegrafOracle extends OracleSupport {
    static final long RECEIVED = 1767193200123L; // 2026-01-01 00:00:00.123 KST
    static final long T0 = 1767193200000000000L; // line protocol nanoseconds
    static final String REMOTE = "10.0.0.7";
    static final long SESSION = 7001L;

    static final String CUSTOM_TG_XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
            + "<TelegrafConfig>\n"
            + "    <enabled>true</enabled>\n"
            + "    <debugEnabled>false</debugEnabled>\n"
            + "    <deltaCounterNormalizeDefault>true</deltaCounterNormalizeDefault>\n"
            + "    <deltaCounterNormalizeDefaultSeconds>30</deltaCounterNormalizeDefaultSeconds>\n"
            + "    <objectDeadtimeMs>45000</objectDeadtimeMs>\n"
            + "    <measurements>\n"
            + "        <measurement>\n"
            + "            <measurementName>syn</measurementName>\n"
            + "            <enabled>true</enabled>\n"
            + "            <debugEnabled>false</debugEnabled>\n"
            + "            <objFamilyBase>SYN_FAMILY</objFamilyBase>\n"
            + "            <objFamilyAppendTags><tag>tier</tag></objFamilyAppendTags>\n"
            + "            <objTypeBase>SYN_TYPE</objTypeBase>\n"
            + "            <objTypePrependTags><tag>scouter_obj_type_prefix</tag></objTypePrependTags>\n"
            + "            <objTypeAppendTags><tag>tier</tag></objTypeAppendTags>\n"
            + "            <objTypeIcon>redis</objTypeIcon>\n"
            + "            <objNameBase>SYN_NAME</objNameBase>\n"
            + "            <objNameAppendTags><tag>port</tag></objNameAppendTags>\n"
            + "            <hostTag>host</hostTag>\n"
            + "            <hostMappings><hostMapping><telegraf>raw-host</telegraf><scouter>mapped-host</scouter></hostMapping></hostMappings>\n"
            + "            <tagFilters><tagFilter><tag>tier</tag><match>web</match><match>!skip</match></tagFilter></tagFilters>\n"
            + "            <counterMappings>\n"
            + "                <counterMapping><tgFieldName>gauge</tgFieldName><counterName>syn_gauge_$port$</counterName><displayName>Gauge $port$</displayName><unit>cnt</unit><totalizable>true</totalizable><normalizeSeconds>0</normalizeSeconds><deltaType>NONE</deltaType></counterMapping>\n"
            + "                <counterMapping><tgFieldName>norm</tgFieldName><counterName>syn_norm</counterName><displayName>Norm</displayName><unit>ms</unit><totalizable>false</totalizable><normalizeSeconds>30</normalizeSeconds><deltaType>NONE</deltaType></counterMapping>\n"
            + "                <counterMapping><tgFieldName>bytes</tgFieldName><counterName>syn_bytes</counterName><displayName>Bytes</displayName><unit>b</unit><totalizable>false</totalizable><normalizeSeconds>0</normalizeSeconds><deltaType>DELTA</deltaType></counterMapping>\n"
            + "                <counterMapping><tgFieldName>reqs</tgFieldName><counterName>syn_reqs</counterName><displayName>Reqs</displayName><unit>cnt</unit><totalizable>true</totalizable><normalizeSeconds>0</normalizeSeconds><deltaType>BOTH</deltaType></counterMapping>\n"
            + "            </counterMappings>\n"
            + "        </measurement>\n"
            + "        <measurement>\n"
            + "            <measurementName>off</measurementName>\n"
            + "            <enabled>false</enabled>\n"
            + "            <objFamilyBase>OFF_FAMILY</objFamilyBase>\n"
            + "            <objTypeBase>OFF_TYPE</objTypeBase>\n"
            + "            <objNameBase>OFF_NAME</objNameBase>\n"
            + "            <counterMappings><counterMapping><tgFieldName>v</tgFieldName><counterName>off_v</counterName></counterMapping></counterMappings>\n"
            + "        </measurement>\n"
            + "        <measurement>\n"
            + "            <measurementName>nofamily</measurementName>\n"
            + "            <objTypeBase>NF_TYPE</objTypeBase>\n"
            + "            <counterMappings><counterMapping><tgFieldName>v</tgFieldName><counterName>nf_v</counterName></counterMapping></counterMappings>\n"
            + "        </measurement>\n"
            + "    </measurements>\n"
            + "</TelegrafConfig>\n";

    static final String LEGACY_CONF = "server_id=synthetic-server\n"
            + "input_telegraf_enabled=true\n"
            + "input_telegraf_delta_counter_normalize_default=false\n"
            + "input_telegraf_$leg$_enabled=true\n"
            + "input_telegraf_$leg$_counter_mappings=gauge:leg_gauge_$port$:Gauge $port$:cnt:true,&bytes:leg_bytes::b,&&reqs:leg_reqs:Reqs:cnt:true:30,short:leg_short\n"
            + "input_telegraf_$leg$_objFamily_base=LEG_FAMILY\n"
            + "input_telegraf_$leg$_objType_base=LEG_TYPE\n"
            + "input_telegraf_$leg$_objType_prepend_tags=\n"
            + "input_telegraf_$leg$_objType_append_tags=tier\n"
            + "input_telegraf_$leg$_objType_icon=linux\n"
            + "input_telegraf_$leg$_objName_base=LEG_NAME\n"
            + "input_telegraf_$leg$_objName_append_tags=port\n"
            + "input_telegraf_$leg$_host_tag=hostname\n"
            + "input_telegraf_$leg$_host_mappings=raw-host:mapped-host,broken\n"
            + "input_telegraf_$leg$_tag_filter=tier:web,tier:!skip,bad\n"
            + "input_telegraf_$legoff$_enabled=false\n"
            + "input_telegraf_$legoff$_objFamily_base=LEGOFF\n"
            + "input_telegraf_$legoff$_counter_mappings=v:legoff_v\n";

    /**
     * Pre-existing counters.site.xml with one ObjectType. Without any ObjectType the original
     * CounterManager.safelyAddFamily -> saveCustomContent -> normalizeCountersXml throws NPE on
     * counters.types.objectType (see scenario telegraf-fresh), so Telegraf registration never succeeds.
     */
    static final String SEED_SITE = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
            + "<Counters>\n"
            + "    <Types>\n"
            + "        <ObjectType disp=\"Seed\" family=\"seed_family\" icon=\"\" name=\"seed_type\" sub-object=\"false\"/>\n"
            + "    </Types>\n"
            + "    <Familys>\n"
            + "        <Family master=\"seed_counter\" name=\"seed_family\">\n"
            + "            <Counter all=\"true\" disp=\"Seed\" icon=\"\" name=\"seed_counter\" total=\"true\" unit=\"cnt\"/>\n"
            + "        </Family>\n"
            + "    </Familys>\n"
            + "</Counters>\n";

    public static void main(String[] args) throws Exception {
        String scenario = args[0];
        switch (scenario) {
            case "telegraf-fresh": telegrafFresh(); break;
            case "telegraf-golden-warmup": telegrafGoldenWarmup(); break;
            case "telegraf-golden": telegrafGolden(); break;
            case "telegraf": telegraf(); break;
            case "telegraf-legacy": telegrafLegacy(); break;
            case "counterhttp": counterHttp(); break;
            case "register": register(); break;
            default: throw new IllegalArgumentException("unknown scenario " + scenario);
        }
        finish(scenario);
        System.exit(0);
    }

    static final List<Object> steps = new ArrayList<Object>();

    static void telegrafFresh() throws Exception {
        ManagementOracle.writeConf("scouter.conf", "server_id=synthetic-server\n");
        ManagementOracle.writeConf("scouter-telegraf.xml", CUSTOM_TG_XML);
        ingest("fresh-first", "syn,host=raw-host,tier=web,port=6379 gauge=10i " + T0);
        ingest("fresh-retry", "syn,host=raw-host,tier=web,port=6379 gauge=11i " + (T0 + 10_000_000_000L));
        facts.put("steps", steps);
        facts.put("site_after_fresh", ManagementOracle.fileFact(CWD.resolve("conf/counters.site.xml")));
        snapshotSite("counters.site.after-fresh.xml");
    }

    static void telegraf() throws Exception {
        ManagementOracle.writeConf("scouter.conf", "server_id=synthetic-server\n");
        ManagementOracle.writeConf("scouter-telegraf.xml", CUSTOM_TG_XML);
        ManagementOracle.writeConf("counters.site.xml", SEED_SITE);
        writeText("counters.site.seed.xml", SEED_SITE);
        // Configuration inputs for Go-side replay (internal/telegraf java_oracle_test.go).
        writeText("scouter.conf.input", "server_id=synthetic-server\n");
        writeText("scouter-telegraf.input.xml", CUSTOM_TG_XML);
        LoginManager.addUser(SESSION, "synthetic-a", "10.0.0.1", "admin");
        Configure c = Configure.getInstance();
        Map<String, Object> cfg = new LinkedHashMap<String, Object>();
        cfg.put("input_telegraf_enabled", c.input_telegraf_enabled); cfg.put("net_http_server_enabled", c.net_http_server_enabled);
        cfg.put("telegraf_object_deadtime_ms", c.telegraf_object_deadtime_ms);
        cfg.put("measurements", new ArrayList<Object>(new java.util.TreeSet<String>(c.telegrafInputConfigMap.keySet())));
        facts.put("config", cfg);

        lineKeys(new String[] {
                "syn,host=raw-host,tier=web gauge=1i,norm=2 " + T0,
                "syn,host=raw-host,tier=web norm=2,gauge=1i " + T0,
                "syn\\,x,tag\\ a=b\\=c f\\ 1=1,f2=2",
                "cpu usage=1",
                "plain"});
        String tags = "syn,host=raw-host,tier=web,port=6379,scouter_obj_type_prefix=PRE ";
        ingest("first-registers-type", tags + "gauge=10i,norm=2.5,bytes=1000i,reqs=5i,ignored=1,flag=true,msg=\"a b,c=d\" " + T0);
        ingest("second-delta-baseline", tags + "gauge=12i,norm=3.5,bytes=1600i,reqs=8i " + (T0 + 10_000_000_000L));
        ingest("third-delta-float", tags + "gauge=13,norm=4,bytes=2200.5,reqs=9.5 " + (T0 + 20_000_000_000L));
        ingest("new-port-adds-counter", "syn,host=raw-host,tier=web,port=6380,scouter_obj_type_prefix=PRE gauge=7i " + (T0 + 30_000_000_000L));
        ingest("tier-api-negated-filter", "syn,host=other-host,tier=api,port=1 gauge=-5i " + T0);
        ingest("filtered-out", "syn,host=raw-host,tier=skip,port=1 gauge=1i " + T0);
        ingest("disabled-measurement", "off,host=h v=1 " + T0);
        ingest("invalid-config-measurement", "nofamily,host=h v=1 " + T0);
        ingest("unknown-measurement", "nosuch,host=h v=1 " + T0);
        ingest("missing-timestamp", "syn,host=raw-host,tier=web,port=1 gauge=1i");
        ingest("bad-long-value", "syn,host=raw-host,tier=web,port=1 gauge=1.5i " + T0);
        ingest("escaped-host-no-prefix", "syn,host=raw\\ host,tier=web,port=2 gauge=3i,norm=1e3 " + T0);
        ingest("no-host-tag", "syn,tier=web,port=3 gauge=4i " + T0);
        facts.put("steps", steps);
        snapshotSite("counters.site.after-telegraf.xml");
        ManagementOracle.waitForQueue(SESSION);
        dispatch("check-job-after-telegraf", RequestCmd.CHECK_JOB, pack(map("session", SESSION)), 'P');
    }

    static void telegrafLegacy() throws Exception {
        ManagementOracle.writeConf("scouter.conf", LEGACY_CONF);
        ManagementOracle.writeConf("counters.site.xml", SEED_SITE);
        writeText("scouter.conf.input", LEGACY_CONF);
        LoginManager.addUser(SESSION, "synthetic-a", "10.0.0.1", "admin");
        Configure c = Configure.getInstance();
        Map<String, Object> cfg = new LinkedHashMap<String, Object>();
        cfg.put("input_telegraf_enabled", c.input_telegraf_enabled);
        cfg.put("measurements", new ArrayList<Object>(new java.util.TreeSet<String>(c.telegrafInputConfigMap.keySet())));
        Map<String, Object> mapping = new TreeMap<String, Object>();
        for (Map.Entry<String, CounterProtocol> e : c.telegrafInputConfigMap.get("leg").getCounterMapping().entrySet()) {
            CounterProtocol p = e.getValue();
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("name", p.getName()); m.put("displayName", p.getDisplayName()); m.put("unit", p.getUnit());
            m.put("total", p.isTotal()); m.put("deltaType", p.getDeltaType().name()); m.put("normalizeSec", p.getNormalizeSec());
            mapping.put(e.getKey(), m);
        }
        cfg.put("leg_counter_mappings", mapping);
        cfg.put("leg_host_mapping", new TreeMap<String, String>(c.telegrafInputConfigMap.get("leg").getHostMapping()));
        cfg.put("leg_tag_filter", new TreeMap<String, Object>(c.telegrafInputConfigMap.get("leg").getTagFilter()));
        cfg.put("leg_objType_prepend_tags", c.telegrafInputConfigMap.get("leg").getObjTypePrependTags());
        facts.put("config", cfg);
        String tags = "leg,hostname=raw-host,tier=web,port=7000 ";
        ingest("legacy-first", tags + "gauge=1i,bytes=100i,reqs=10i,short=0.5 " + T0);
        ingest("legacy-second", tags + "gauge=2i,bytes=400i,reqs=16i,short=0.25 " + (T0 + 5_000_000_000L));
        ingest("legacy-disabled", "legoff,hostname=h v=1 " + T0);
        facts.put("steps", steps);
        snapshotSite("counters.site.after-legacy.xml");
    }

    // ---------------------------------------------------------------- replay of internal/telegraf/testdata/golden_packs.json inputs
    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> goldenSteps() throws Exception {
        String path = System.getProperty("oracle.input");
        require(path != null, "-Doracle.input");
        Map<String, Object> root = (Map<String, Object>) new org.json.simple.parser.JSONParser().parse(
                new String(Files.readAllBytes(java.nio.file.Paths.get(path)), StandardCharsets.UTF_8));
        return (List<Map<String, Object>>) root.get("steps");
    }

    static long goldenStart() { return Long.parseLong(System.getProperty("oracle.goldenStart")); }

    static void goldenConf() throws Exception {
        ManagementOracle.writeConf("scouter.conf", "server_id=synthetic-server\n");
        // Same configuration as the Go test (sampleConfig = TgConfig.getSampleContents()).
        ManagementOracle.writeConf("scouter-telegraf.xml", scouter.server.support.telegraf.TgConfig.getSampleContents());
    }

    /**
     * Phase 1 (own JVM): register every objType/counter the replay needs, starting from SEED_SITE (a file
     * without ObjectType would hit the telegraf-fresh NPE). Its MeterCounter/CounterTimeCache state is discarded.
     */
    static void telegrafGoldenWarmup() throws Exception {
        goldenConf();
        ManagementOracle.writeConf("counters.site.xml", SEED_SITE);
        long start = goldenStart();
        // Several passes: registerNewObjType0 builds a new Family holding only the current line's counters and
        // safelyAddFamily replaces the existing <Family> element, so a later objType of the same family drops
        // counters registered earlier. Passes repeat until every parsed line emits on its first count.
        List<Object> passes = new ArrayList<Object>();
        boolean stable = false;
        for (int pass = 1; pass <= 4 && !stable; pass++) {
            stable = true;
            List<Object> lines = new ArrayList<Object>();
            for (Map<String, Object> step : goldenSteps()) {
                long received = start + ((Number) step.get("offsetMs")).longValue();
                for (String l : ((String) step.get("body")).split("\n")) {
                    if (l.isEmpty()) continue;
                    ByteArrayErr err = ByteArrayErr.capture();
                    InfluxSingleLine line;
                    try { line = InfluxSingleLine.of(l, Configure.getInstance(), received); } finally { err.restore(); }
                    Map<String, Object> rec = new LinkedHashMap<String, Object>();
                    rec.put("step", step.get("name")); rec.put("line", l); rec.put("parsed", line != null);
                    if (line != null) {
                        boolean emitted = false; int attempt = 0;
                        while (!emitted && attempt++ < 4) {
                            TelegrafOracleAccess.count(line, REMOTE);
                            emitted = !NetDataProcessor.drain().isEmpty();
                            if (!emitted && !waitRegistered(line)) break;
                        }
                        rec.put("emitted_after_attempts", emitted ? attempt : -1);
                        if (attempt != 1) stable = false;
                    }
                    lines.add(rec);
                }
            }
            Map<String, Object> p = new LinkedHashMap<String, Object>();
            p.put("pass", pass); p.put("lines", lines);
            passes.add(p);
        }
        require(stable, "registration did not converge in 4 passes");
        facts.put("passes", passes);
        write("counters.site.registered.xml", read(CWD.resolve("conf/counters.site.xml")));
    }

    /**
     * Phase 2 (fresh JVM, counters.site.xml from phase 1): each body goes through the original
     * TelegrafInputHandler.handlerRequest (1000-line limit, HashMap de-dup, iteration order, HttpServer.getRemoteAddr)
     * with a minimal HttpServletRequest proxy. PerfCounterPack.time (= System.currentTimeMillis() at the call) is
     * checked to be inside the call and replaced by goldenStart + offsetMs, the Go test clock.
     */
    static void telegrafGolden() throws Exception {
        goldenConf();
        long start = goldenStart();
        List<Object> out = new ArrayList<Object>();
        for (Map<String, Object> step : goldenSteps()) {
            String name = (String) step.get("name"), body = (String) step.get("body"), remote = (String) step.get("remote");
            long offset = ((Number) step.get("offsetMs")).longValue();
            int logStart = scouter.server.Logger.seen.size();
            long before = System.currentTimeMillis();
            scouter.server.http.handler.TelegrafInputHandler.getInstance().handlerRequest(request(body, remote), null);
            long after = System.currentTimeMillis();
            List<Object> packs = new ArrayList<Object>();
            List<NetDataProcessor.Record> recs = NetDataProcessor.drain();
            int i = 0;
            for (NetDataProcessor.Record r : recs) {
                Map<String, Object> p = new LinkedHashMap<String, Object>();
                if (r.pack instanceof scouter.lang.pack.ObjectPack) {
                    scouter.lang.pack.ObjectPack o = (scouter.lang.pack.ObjectPack) r.pack;
                    p.put("type", "object"); p.put("peer", r.addr.getHostAddress()); p.put("objHash", o.objHash); p.put("objName", o.objName);
                    p.put("objType", o.objType); p.put("address", o.address); p.put("values", goldenValues(o.tags));
                } else {
                    PerfCounterPack c = (PerfCounterPack) r.pack;
                    require(c.time >= before && c.time <= after, "perf time inside handlerRequest call");
                    c.time = start + offset;
                    p.put("type", "perf"); p.put("peer", r.addr.getHostAddress()); p.put("objName", c.objName);
                    p.put("time", c.time); p.put("timetype", (int) c.timetype); p.put("values", goldenValues(c.data));
                }
                String file = "step" + out.size() + "-" + (i++) + ".pack.bin";
                write(file, pack(r.pack));
                p.put("file", file);
                packs.add(p);
            }
            List<String> logs = new ArrayList<String>();
            for (int k = logStart; k < scouter.server.Logger.seen.size(); k++) {
                String l = scouter.server.Logger.seen.get(k);
                if (!l.startsWith("[TG002]") && !l.startsWith("[TG003]")) logs.add(sanitize(l));
            }
            Map<String, Object> s = new LinkedHashMap<String, Object>();
            s.put("name", name); s.put("offsetMs", offset); s.put("remote", remote); s.put("body", body);
            s.put("perf_time_normalized_to", start + offset); s.put("logs", logs); s.put("packs", packs);
            out.add(s);
        }
        writeText("java_packs.json", Json.write(out) + "\n");
        facts.put("steps", out.size());
    }

    /** MapValue entries in wire (Hashtable enumeration) order; float/double carry exact IEEE bits. */
    static List<Object> goldenValues(scouter.lang.value.MapValue m) {
        List<Object> l = new ArrayList<Object>();
        java.util.Enumeration<String> keys = m.keys();
        while (keys.hasMoreElements()) {
            String k = keys.nextElement(); scouter.lang.value.Value v = m.get(k);
            Map<String, Object> e = new LinkedHashMap<String, Object>();
            e.put("key", k);
            switch (v.getValueType()) {
                case scouter.lang.value.ValueEnum.DECIMAL: e.put("type", "decimal"); e.put("value", String.valueOf(((scouter.lang.value.DecimalValue) v).value)); break;
                case scouter.lang.value.ValueEnum.FLOAT: { float f = ((scouter.lang.value.FloatValue) v).value; e.put("type", "float"); e.put("value", Float.toString(f)); e.put("bits", String.format("0x%08x", Float.floatToRawIntBits(f))); break; }
                case scouter.lang.value.ValueEnum.DOUBLE: { double d = ((scouter.lang.value.DoubleValue) v).value; e.put("type", "double"); e.put("value", Double.toString(d)); e.put("bits", String.format("0x%016x", Double.doubleToRawLongBits(d))); break; }
                default: e.put("type", String.valueOf(v.getValueType())); e.put("value", v.toString());
            }
            l.add(e);
        }
        return l;
    }

    static javax.servlet.http.HttpServletRequest request(final String body, final String remote) {
        final java.io.BufferedReader reader = new java.io.BufferedReader(new StringReader(body));
        return (javax.servlet.http.HttpServletRequest) java.lang.reflect.Proxy.newProxyInstance(
                javax.servlet.http.HttpServletRequest.class.getClassLoader(), new Class<?>[] {javax.servlet.http.HttpServletRequest.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getReader": return reader;
                        case "getHeader": return null;
                        case "getRemoteAddr": return remote;
                        default: throw new UnsupportedOperationException("HttpServletRequest." + method.getName() + " not used by the oracle");
                    }
                });
    }

    static void snapshotSite(String name) throws Exception {
        Path site = CWD.resolve("conf/counters.site.xml");
        if (Files.exists(site)) write(name, read(site)); else facts.put(name, "missing");
    }

    static void lineKeys(String[] lines) {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        for (String l : lines) m.put(l, InfluxSingleLine.toLineStringKey(l));
        facts.put("line_string_keys", m);
    }

    static Map<String, Object> describeLine(InfluxSingleLine line) {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        if (line == null) { m.put("parsed", false); return m; }
        m.put("parsed", true);
        m.put("measurement", line.getMeasurement()); m.put("host", line.getHost()); m.put("family", line.getFamily());
        m.put("objType", line.getObjType()); m.put("objTypeIcon", line.getObjTypeIcon()); m.put("objName", line.getObjName());
        m.put("objHash", line.getObjHash()); m.put("receivedTime", line.getReceivedTime()); m.put("timestampOrigin", line.getTimestampOrigin());
        m.put("tags", new TreeMap<String, String>(line.getTags()));
        Map<String, Object> fields = new TreeMap<String, Object>();
        for (Map.Entry<CounterProtocol, NumberValue> e : line.getNumberFields().entrySet()) {
            Map<String, Object> f = new LinkedHashMap<String, Object>();
            f.put("value", describeValue(e.getValue()));
            List<Object> counters = new ArrayList<Object>();
            for (Counter c : e.getKey().toCounters(line.getTags())) counters.add(describeCounter(c));
            f.put("counters", counters);
            fields.put(e.getKey().getName(), f);
        }
        m.put("numberFields", fields);
        return m;
    }

    static Map<String, Object> describeCounter(Counter c) {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        if (c == null) return null;
        m.put("name", c.getName()); m.put("disp", c.getDisplayName()); m.put("unit", c.getUnit()); m.put("icon", c.getIcon());
        m.put("total", c.isTotal()); m.put("all", c.isAll());
        return m;
    }

    /** Parses with the original InfluxSingleLine.of, then counts until packs are emitted (or registration settles). */
    static void ingest(String name, String lineStr) throws Exception {
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("name", name); step.put("line", lineStr);
        int logStart = scouter.server.Logger.seen.size();
        ByteArrayErr err = ByteArrayErr.capture();
        InfluxSingleLine line;
        try { line = InfluxSingleLine.of(lineStr, Configure.getInstance(), RECEIVED); } finally { err.restore(); }
        step.put("parse", describeLine(line));
        step.put("parse_stderr_first_line", firstException(err.text()));
        List<Object> attempts = new ArrayList<Object>();
        if (line != null) {
            for (int attempt = 1; attempt <= 4; attempt++) {
                TelegrafOracleAccess.count(line, REMOTE);
                List<NetDataProcessor.Record> recs = NetDataProcessor.drain();
                Map<String, Object> a = new LinkedHashMap<String, Object>();
                a.put("attempt", attempt);
                List<Object> packs = new ArrayList<Object>();
                for (int i = 0; i < recs.size(); i++) {
                    NetDataProcessor.Record r = recs.get(i);
                    String file = name + "-" + attempt + "-" + i + ".pack.bin";
                    write(file, pack(r.pack));
                    Map<String, Object> p = describePack(r.pack);
                    p.put("file", file); p.put("addr", r.addr.getHostAddress());
                    packs.add(p);
                }
                a.put("packs", packs);
                attempts.add(a);
                if (!recs.isEmpty()) break;
                boolean settled = waitRegistered(line);
                a.put("registration_settled", settled);
                a.put("objectType_after", describeObjectType(line.getObjType()));
                if (!settled) break; // registration failed or never converges: further counts repeat the same enqueue
            }
        }
        step.put("attempts", attempts);
        List<String> logs = new ArrayList<String>();
        synchronized (scouter.server.Logger.seen) {
            for (int i = logStart; i < scouter.server.Logger.seen.size(); i++) logs.add(sanitize(scouter.server.Logger.seen.get(i)));
        }
        step.put("logs", logs);
        steps.add(step);
    }

    static boolean waitRegistered(InfluxSingleLine line) {
        for (int i = 0; i < 200; i++) {
            ObjectType t = CounterManager.getInstance().getCounterEngine().getObjectType(line.getObjType());
            boolean ok = t != null;
            if (ok) for (CounterProtocol p : line.getNumberFields().keySet())
                for (Counter c : p.toCounters(line.getTags())) if (t.getCounter(c.getName()) == null) ok = false;
            if (ok) { sleep(400); return true; }
            sleep(50);
        }
        return false;
    }

    static Map<String, Object> describeObjectType(String name) {
        ObjectType t = CounterManager.getInstance().getCounterEngine().getObjectType(name);
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        if (t == null) { m.put("exists", false); return m; }
        m.put("exists", true); m.put("disp", t.getDisplayName()); m.put("icon", t.getIcon());
        m.put("family", t.getFamily() == null ? null : t.getFamily().getName());
        m.put("master", t.getFamily() == null ? null : t.getFamily().getMaster());
        List<Object> counters = new ArrayList<Object>();
        if (t.getFamily() != null) for (Counter c : t.getFamily().listCounters()) counters.add(describeCounter(c));
        m.put("familyCounters", counters);
        return m;
    }

    // ---------------------------------------------------------------- CounterHandler (/counter JSON)
    static void counterHttp() throws Exception {
        ManagementOracle.writeConf("scouter.conf", "server_id=synthetic-server\n");
        counterCase("two-objects", "[{\"object\":{\"host\":\"VM1\",\"name\":\"redis1\",\"type\":\"redis\",\"address\":\"10.10.10.10\"},"
                + "\"counters\":[{\"name\":\"aof\",\"value\":55},{\"name\":\"cpu\",\"value\":42.5}]},"
                + "{\"object\":{\"host\":\"VM2\",\"name\":\"redis2\",\"type\":\"redis\",\"address\":\"10.10.10.11\"},"
                + "\"counters\":[{\"name\":\"aof\",\"value\":35}]}]");
        counterCase("single-no-name-no-counters", "{\"object\":{\"host\":\"VM3\",\"type\":\"redis\",\"address\":\"10.10.10.12\"}}");
        counterCase("array-with-non-objects", "[1,\"x\",{\"object\":{\"host\":\"VM4\",\"name\":\"\",\"type\":\"redis\",\"address\":\"10.10.10.13\"},\"counters\":[]}]");
        counterCase("no-object-key", "{\"counters\":[{\"name\":\"aof\",\"value\":1}]}");
        counterCase("number-root", "42");
        counterCase("invalid-json", "{\"object\":");
        counterCase("missing-address", "[{\"object\":{\"host\":\"VM5\",\"name\":\"a\",\"type\":\"redis\"}},"
                + "{\"object\":{\"host\":\"VM6\",\"name\":\"b\",\"type\":\"redis\",\"address\":\"10.10.10.14\"}}]");
        counterCase("unicode-name", "{\"object\":{\"host\":\"호스트\",\"name\":\"이름\",\"type\":\"redis\",\"address\":\"10.10.10.15\"},\"counters\":[{\"name\":\"카운터\",\"value\":1}]}");
        facts.put("steps", steps);
    }

    static void counterCase(String name, String body) throws Exception {
        int logStart = scouter.server.Logger.seen.size();
        long before = System.currentTimeMillis();
        CounterHandler.process(new StringReader(body));
        long after = System.currentTimeMillis();
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("name", name); step.put("body", body);
        List<Object> packs = new ArrayList<Object>();
        List<NetDataProcessor.Record> recs = NetDataProcessor.drain();
        for (int i = 0; i < recs.size(); i++) {
            byte[] raw = recs.get(i).bytes;
            DataInputX in = new DataInputX(raw);
            int cafe = in.readInt();
            Pack p = in.readPack();
            Map<String, Object> rec = new LinkedHashMap<String, Object>();
            rec.put("cafe", String.format("0x%08x", cafe));
            if (p instanceof PerfCounterPack) {
                // perfPack.time = System.currentTimeMillis() in CounterHandler.extractPerfCounterPack: normalized to 0.
                long t = ((PerfCounterPack) p).time;
                rec.put("time_in_call_window", t >= before && t <= after);
                ((PerfCounterPack) p).time = 0L;
                DataOutputX o = new DataOutputX(); o.writeInt(cafe); o.writePack(p); raw = o.toByteArray();
                rec.put("time_normalized_to", 0);
            }
            String file = name + "-" + i + ".cafe.bin";
            write(file, raw);
            rec.put("file", file); rec.put("addr", recs.get(i).addr.getHostAddress()); rec.put("pack", describePack(p));
            packs.add(rec);
        }
        step.put("records", packs);
        List<String> logs = new ArrayList<String>();
        for (int i = logStart; i < scouter.server.Logger.seen.size(); i++) logs.add(sanitize(scouter.server.Logger.seen.get(i)));
        step.put("logs", logs);
        steps.add(step);
    }

    // ---------------------------------------------------------------- RegisterHandler (/register JSON)
    static void register() throws Exception {
        ManagementOracle.writeConf("scouter.conf", "server_id=synthetic-server\n");
        LoginManager.addUser(SESSION, "synthetic-a", "10.0.0.1", "admin");
        registerCase("new-type", "{\"object\":{\"type\":\"reg_redis\",\"display\":\"Reg Redis\"},\"counters\":["
                + "{\"name\":\"aof\",\"unit\":\"cnt\",\"display\":\"AOF\",\"total\":true,\"all\":true},"
                + "{\"name\":\"clients\",\"unit\":\"cnt\"},"
                + "{\"name\":\"cpu\",\"unit\":\"%\",\"display\":\"CPU\",\"total\":false}]}", "reg_redis");
        snapshotSite("counters.site.after-register-new.xml");
        ManagementOracle.waitForQueue(SESSION);
        dispatch("check-job-after-register", RequestCmd.CHECK_JOB, pack(map("session", SESSION)), 'P');
        registerCase("existing-type-add-counter", "{\"object\":{\"type\":\"reg_redis\"},\"counters\":["
                + "{\"name\":\"aof\",\"unit\":\"cnt\"},{\"name\":\"mem\",\"unit\":\"MB\",\"display\":\"Mem\",\"all\":false}]}", "reg_redis");
        snapshotSite("counters.site.after-register-edit.xml");
        registerCase("invalid-json", "{\"object\":", null);
        registerCase("missing-object", "{\"counters\":[]}", null);
        registerCase("total-not-boolean", "{\"object\":{\"type\":\"reg_bad\"},\"counters\":[{\"name\":\"x\",\"total\":\"yes\"}]}", "reg_bad");
        facts.put("steps", steps);
    }

    static void registerCase(String name, String body, String objType) throws Exception {
        int logStart = scouter.server.Logger.seen.size();
        boolean result = RegisterHandler.process(new StringReader(body));
        Map<String, Object> step = new LinkedHashMap<String, Object>();
        step.put("name", name); step.put("body", body); step.put("result", result);
        if (objType != null) step.put("objectType_after", describeObjectType(objType));
        List<String> logs = new ArrayList<String>();
        for (int i = logStart; i < scouter.server.Logger.seen.size(); i++) logs.add(sanitize(scouter.server.Logger.seen.get(i)));
        step.put("logs", logs);
        steps.add(step);
    }

    /** Captures System.err (printStackTrace of swallowed parse errors). */
    static final class ByteArrayErr {
        final java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
        java.io.PrintStream old;
        static ByteArrayErr capture() throws Exception { ByteArrayErr e = new ByteArrayErr(); e.old = System.err; System.setErr(new java.io.PrintStream(e.buf, true, "UTF-8")); return e; }
        void restore() { System.err.flush(); System.setErr(old); }
        String text() { return new String(buf.toByteArray(), StandardCharsets.UTF_8); }
    }
}
