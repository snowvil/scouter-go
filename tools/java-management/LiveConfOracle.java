// Executes the unchanged Scouter 2.21.3 LoginService and XLogService handlers through the original
// ServiceHandlingProxy with the original Configure (reload(true) after each scouter.conf change), the original
// AccountManager and XLogCache/XLogLoopCache, and the XLogRD/LoginManager seams (tools/java-management/README.md).
// It fixes the per-request reads of server_id, ext_link_name, ext_link_url_pattern, net_tcp_client_so_timeout_ms,
// tagcnt_enabled (LOGIN), xlog_realtime/pasttime_lower_bound_ms (TRANX_REAL_TIME_GROUP, TRANX_LOAD_TIME_GROUP)
// and req_search_xlog_max_count (XLOG_LOAD_BY_TXIDS, SEARCH_XLOG_LIST). All data is synthetic.
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import scouter.io.DataInputX;
import scouter.io.DataOutputX;
import scouter.lang.pack.MapPack;
import scouter.lang.pack.XLogPack;
import scouter.lang.value.DecimalValue;
import scouter.lang.value.ListValue;
import scouter.lang.value.TextValue;
import scouter.lang.value.Value;
import scouter.net.RequestCmd;
import scouter.net.TcpFlag;
import scouter.server.Configure;
import scouter.server.core.cache.XLogCache;
import scouter.server.db.XLogRD;
import scouter.util.CipherUtil;
import scouter.util.DateUtil;
import scouter.util.SysJMX;

final class LiveConfOracle extends OracleSupport {
    static final long CONF_MTIME = 1_700_000_000_000L; // distinct, increasing mtimes make every reload(true) apply
    static int confVersion;

    /** Writes ./conf/scouter.conf, gives it a new mtime and applies it like the 3s Configure thread would. */
    static Configure applyConf(String name, String text) throws Exception {
        Path p = ManagementOracle.conf("scouter.conf");
        ManagementOracle.writeConf("scouter.conf", text);
        Files.setLastModifiedTime(p, FileTime.fromMillis(CONF_MTIME + 10_000L * (++confVersion)));
        Configure c = Configure.getInstance();
        c.reload(true);
        writeText("scouter.conf." + name, text);
        return c;
    }

    // ---------------------------------------------------------------- LOGIN (V1)
    static void login() throws Exception {
        ManagementOracle.writeConf("scouter.conf", "");
        Files.setLastModifiedTime(ManagementOracle.conf("scouter.conf"), FileTime.fromMillis(CONF_MTIME));
        writeText("scouter.conf.default", "");
        String host = SysJMX.getHostName();
        loginCase("login-default", host);
        dispatch("login-fail", RequestCmd.LOGIN, pack(loginRequest(CipherUtil.sha256("wrong"))), 'P');

        applyConf("changed", "server_id=synthetic-server-1\n"
                + "ext_link_name=paper-synthetic\n"
                + "ext_link_url_pattern=http://paper.invalid/index.html#/paper?instances=$[objHashes]&from=$[from]&to=$[to]\n"
                + "tagcnt_enabled=false\n"
                + "net_tcp_client_so_timeout_ms=5000\n");
        loginCase("login-changed", host);

        // getBoolean: only "true" is true; getInt: parse failure -> default 8000; getValue trims; an empty value stays empty.
        applyConf("odd", "server_id=\n"
                + "ext_link_name=  spaced value  \n"
                + "ext_link_url_pattern=\n"
                + "tagcnt_enabled=yes\n"
                + "net_tcp_client_so_timeout_ms=not-a-number\n");
        loginCase("login-odd", host);

        applyConf("restored", "tagcnt_enabled=TRUE\nnet_tcp_client_so_timeout_ms=-1\n");
        loginCase("login-restored", host);
    }

    static MapPack loginRequest(String pass) {
        return map("id", "admin", "pass", pass, "ip", "192.0.2.10", "hostname", "synthetic-client", "version", "2.21.3");
    }

    static void loginCase(String name, String host) throws Exception {
        Configure c = Configure.getInstance();
        Map<String, Object> applied = new LinkedHashMap<String, Object>();
        applied.put("server_id", host.equals(c.server_id) ? "<hostname>" : c.server_id);
        applied.put("ext_link_name", c.ext_link_name);
        applied.put("ext_link_url_pattern", c.ext_link_url_pattern);
        applied.put("tagcnt_enabled", c.tagcnt_enabled);
        applied.put("net_tcp_client_so_timeout_ms", c.net_tcp_client_so_timeout_ms);
        long before = System.currentTimeMillis();
        Map<String, Object> rec = dispatch(name, RequestCmd.LOGIN, pack(loginRequest(CipherUtil.sha256("admin"))), 'P');
        long after = System.currentTimeMillis();
        rec.put("applied", applied);
        redactLogin(name, host, before, after);
    }

    /** LOGIN "time" is System.currentTimeMillis(): checked to lie in the call window, then written as 0. */
    static void redactLogin(String name, String host, long before, long after) throws Exception {
        Path p = out.resolve(name + ".response.bin");
        DataInputX in = new DataInputX(read(p));
        require(in.readByte() == TcpFlag.HasNEXT, "LOGIN item");
        MapPack mp = (MapPack) in.readPack();
        require(in.readByte() == TcpFlag.NoNEXT, "LOGIN single item");
        long t = mp.getLong("time");
        require(t >= before && t <= after, "LOGIN time in call window");
        mp.put("time", new DecimalValue(0));
        Value sid = mp.get("server_id");
        boolean hostRedacted = sid instanceof TextValue && host.equals(((TextValue) sid).value);
        if (hostRedacted) mp.put("server_id", new TextValue("<hostname>"));
        DataOutputX o = new DataOutputX();
        o.writeByte(TcpFlag.HasNEXT);
        o.writePack(mp);
        o.writeByte(TcpFlag.NoNEXT);
        Files.write(p, o.toByteArray());
        for (Object rec : cases) {
            @SuppressWarnings("unchecked") Map<String, Object> r = (Map<String, Object>) rec;
            if (name.equals(r.get("name"))) {
                r.put("response_bytes", read(p).length);
                r.put("items", decodeItems(read(p), 'P'));
                r.put("redactions", hostRedacted ? "time in call window -> 0; server_id host name -> <hostname>" : "time in call window -> 0");
            }
        }
    }

    // ---------------------------------------------------------------- XLog bounds and search count (V2)
    static final int OBJ = 0x1234;
    static final int[] ELAPSED = {-3, 0, 1, 499, 500, 501};
    static final int SEARCH_COUNT = 502;

    static byte[] xlog(long endTime, long txid, int elapsed) {
        XLogPack x = new XLogPack();
        x.endTime = endTime; x.objHash = OBJ; x.service = 7; x.txid = txid; x.elapsed = elapsed;
        return pack(x);
    }

    static void xlogBounds() throws Exception {
        ManagementOracle.writeConf("scouter.conf", "");
        Files.setLastModifiedTime(ManagementOracle.conf("scouter.conf"), FileTime.fromMillis(CONF_MTIME));
        String date = "20261013", date2 = "20261014";
        long base = DateUtil.yyyymmdd(date) + 3_600_000L, base2 = DateUtil.yyyymmdd(date2) + 3_600_000L;
        DataOutputX boundsBin = new DataOutputX(), searchBin = new DataOutputX();
        for (int i = 0; i < ELAPSED.length; i++) {
            byte[] b = xlog(base + i * 1000L, 1000 + i, ELAPSED[i]);
            XLogCache.put(OBJ, ELAPSED[i], false, b); // XLogCore: XLogCache.put(objHash, elapsed, error != 0, bytes)
            XLogRD.seamAdd(date, base + i * 1000L, 1000 + i, b);
            boundsBin.write(b);
        }
        ListValue txids = new ListValue();
        for (int i = 0; i < SEARCH_COUNT; i++) {
            byte[] b = xlog(base2 + i * 10L, 5001 + i, 10);
            XLogRD.seamAdd(date2, base2 + i * 10L, 5001 + i, b);
            searchBin.write(b);
            txids.add(5001 + i);
        }
        write("xlogs-bounds.bin", boundsBin.toByteArray());
        write("xlogs-search.bin", searchBin.toByteArray());
        Map<String, Object> input = new LinkedHashMap<String, Object>();
        input.put("objHash", OBJ); input.put("bounds_date", date); input.put("bounds_base_time", base);
        input.put("search_date", date2); input.put("search_base_time", base2); input.put("search_count", SEARCH_COUNT);
        input.put("timezone_note", "dates are Asia/Seoul (oracle -Duser.timezone)");
        facts.put("input", input);

        int[][] combos = {
            {0, 0}, {1, 0}, {499, 0}, {500, 0}, {501, 0},
            {0, 500}, {1, 500}, {499, 500}, {500, 500}, {501, 500},
            {0, 1}, {0, 499}, {0, 501},
            {0, -5}, {-10, -5}, {-10, -20},
        };
        for (int[] c : combos) {
            int bound = c[0], limit = c[1];
            Configure conf = applyConf("bound" + bound, "xlog_realtime_lower_bound_ms=" + bound + "\nxlog_pasttime_lower_bound_ms=" + bound + "\n");
            require(conf.xlog_realtime_lower_bound_ms == bound && conf.xlog_pasttime_lower_bound_ms == bound, "bound applied");
            String suffix = "-bound" + bound + "-limit" + limit;
            Map<String, Object> rt = dispatch("realtime" + suffix, RequestCmd.TRANX_REAL_TIME_GROUP,
                    pack(map("index", 0, "loop", 0L, "limit", limit, "objHash", list(OBJ))), 'P');
            rt.put("bound", bound); rt.put("limit", limit);
            Map<String, Object> h = dispatch("history" + suffix, RequestCmd.TRANX_LOAD_TIME_GROUP,
                    pack(map("date", date, "stime", base - 1, "etime", base + 10_000L, "limit", limit, "max", 0, "reverse", false, "objHash", list(OBJ))), 'P');
            h.put("bound", bound); h.put("limit", limit);
        }
        applyConf("bound0", "xlog_realtime_lower_bound_ms=0\nxlog_pasttime_lower_bound_ms=0\n");
        Map<String, Object> hm = dispatch("history-bound0-limit0-max2", RequestCmd.TRANX_LOAD_TIME_GROUP,
                pack(map("date", date, "stime", base - 1, "etime", base + 10_000L, "limit", 0, "max", 2, "reverse", false, "objHash", list(OBJ))), 'P');
        hm.put("bound", 0); hm.put("limit", 0);

        for (int max : new int[] {-1, 0, 1, 2, 499, 500, 501}) {
            Configure conf = applyConf("max" + max, "req_search_xlog_max_count=" + max + "\n");
            require(conf.req_search_xlog_max_count == max, "max applied");
            summarize(dispatch("txids-max" + max, RequestCmd.XLOG_LOAD_BY_TXIDS, pack(map("date", date2, "txid", txids)), 'P'), max);
            summarize(dispatch("search-max" + max, RequestCmd.SEARCH_XLOG_LIST, pack(map("stime", base2 - 1, "etime", base2 + 600_000L)), 'P'), max);
        }
    }

    /**
     * Large XLog streams: items become the txid list plus the SHA-256 of the response bytes. For max >= 499 the
     * request/response .bin files (tens of KiB each) are not kept; the hash still fixes every byte.
     */
    static void summarize(Map<String, Object> rec, int max) throws Exception {
        Path req = out.resolve((String) rec.get("request")), resp = out.resolve((String) rec.get("response"));
        byte[] response = read(resp);
        List<Object> txids = new java.util.ArrayList<Object>();
        DataInputX in = new DataInputX(response);
        while (in.readByte() == TcpFlag.HasNEXT) txids.add(((XLogPack) in.readPack()).txid);
        rec.put("max", max);
        rec.remove("items");
        rec.put("item_count", txids.size());
        rec.put("txids", txids);
        rec.put("response_sha256", sha256(response));
        if (max >= 499) {
            rec.put("request_sha256", sha256(read(req)));
            Files.delete(req); Files.delete(resp);
            rec.put("request", null); rec.put("response", null);
        }
    }
}
