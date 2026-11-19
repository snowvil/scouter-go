// Independent test harness. Original Scouter Client classes (Apache-2.0) are compiled in place,
// without copying or modifying their transport, registry, login or serialization code.
//
// A line-oriented driver for scripts/java-agent-interop.py: each stdin line is
// "<id> <op> key=value ..." (values URL-encoded) and each answer is one stdout
// line "R <id> <json>". Every query goes through the original TcpProxy on a
// fresh Client socket so the socket read timeout (Server.soTimeOut) can differ
// per request. Response contents are summarised (sizes, types, hashes,
// substring matches); environment values and other agent data are not printed.
import scouter.client.net.DummyTcpProxy;
import scouter.client.net.LoginMgr;
import scouter.client.net.LoginResult;
import scouter.client.net.TcpProxy;
import scouter.client.server.Server;
import scouter.client.server.ServerManager;
import scouter.lang.TimeTypeEnum;
import scouter.lang.pack.MapPack;
import scouter.lang.pack.ObjectPack;
import scouter.lang.pack.Pack;
import scouter.lang.pack.XLogPack;
import scouter.lang.pack.XLogProfilePack;
import scouter.lang.step.MethodStep;
import scouter.lang.step.Step;
import scouter.lang.value.BlobValue;
import scouter.lang.value.ListValue;
import scouter.lang.value.MapValue;
import scouter.lang.value.TextValue;
import scouter.lang.value.Value;
import scouter.net.RequestCmd;
import scouter.util.Hexa32;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class AgentInteropClient {
    private static Server server;
    private static int defaultSoTimeout;

    public static void main(String[] args) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;
            String[] parts = line.split(" ");
            String id = parts[0];
            Map<String, String> kv = new LinkedHashMap<>();
            for (int i = 2; i < parts.length; i++) {
                int eq = parts[i].indexOf('=');
                if (eq > 0) kv.put(parts[i].substring(0, eq), URLDecoder.decode(parts[i].substring(eq + 1), "UTF-8"));
            }
            String answer;
            try {
                answer = handle(parts.length > 1 ? parts[1] : "", kv);
            } catch (Throwable t) {
                answer = new Json().put("ok", false).put("exception", t.getClass().getName() + ": " + t.getMessage()).toString();
            }
            System.out.println("R " + id + " " + answer);
            System.out.flush();
            if ("quit".equals(parts.length > 1 ? parts[1] : "")) break;
        }
        ServerManager.getInstance().shutdown();
        System.exit(0);
    }

    static String handle(String op, Map<String, String> kv) throws Exception {
        switch (op) {
            case "login": return login(kv);
            case "objects": return objects();
            case "xlogs": return xlogs(kv);
            case "text": return text(kv);
            case "profile": return profile(kv);
            case "counter": return counter(kv);
            case "relay": return relay(kv);
            case "quit": return new Json().put("ok", true).toString();
            default: return new Json().put("ok", false).put("exception", "unknown op " + op).toString();
        }
    }

    static String login(Map<String, String> kv) {
        server = new Server("127.0.0.1", kv.get("port"));
        ServerManager.getInstance().addServer(server);
        LoginResult result = LoginMgr.login(server.getId(), kv.getOrDefault("user", "admin"), kv.getOrDefault("password", "admin"));
        defaultSoTimeout = server.getSoTimeOut();
        return new Json().put("ok", result.success && server.getSession() != 0)
                .put("soTimeout", server.getSoTimeOut()).put("timezone", server.getTimezone()).toString();
    }

    // A fresh original TcpProxy per request: the pool is drained and the proxy
    // is closed (CLOSE command) afterwards, so soTimeout applies to its socket.
    static final class Call {
        final List<Pack> packs = new ArrayList<>();
        boolean valid;
        long elapsedMs;
        long blobBytes;
        int blobs;
        String blobSha256;
    }

    static Call call(String command, Pack param, int soTimeout, boolean blob) throws Exception {
        if (server == null || server.getSession() == 0) throw new IllegalStateException("not logged in");
        server.getConnectionPool().closeAll();
        server.setSoTimeOut(soTimeout > 0 ? soTimeout : defaultSoTimeout);
        TcpProxy proxy = TcpProxy.getTcpProxy(server.getId());
        if (proxy instanceof DummyTcpProxy) throw new IllegalStateException("DummyTcpProxy: server not connected");
        Call c = new Call();
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        long start = System.nanoTime();
        try {
            if (blob) {
                proxy.process(command, param, input -> {
                    byte[] b = input.readBlob();
                    c.blobs++;
                    c.blobBytes += b.length;
                    sha.update(b);
                });
                c.blobSha256 = hex(sha.digest());
            } else {
                c.packs.addAll(proxy.process(command, param));
            }
            c.valid = proxy.isValid();
        } finally {
            c.elapsedMs = (System.nanoTime() - start) / 1000000L;
            proxy.close();
            server.setSoTimeOut(defaultSoTimeout);
        }
        return c;
    }

    static String objects() throws Exception {
        Call c = call(RequestCmd.OBJECT_LIST_REAL_TIME, null, 0, false);
        List<Json> list = new ArrayList<>();
        for (Pack p : c.packs) {
            if (!(p instanceof ObjectPack)) continue;
            ObjectPack o = (ObjectPack) p;
            // Only the CPU autodump tags (ScouterConstants.TAG_AUTODUMP_CPU_*); other tags are not printed.
            Map<String, Object> autodump = new TreeMap<>();
            for (String key : new String[]{"ADC", "ADC_TH", "ADC_D"}) {
                Value v = o.tags == null ? null : o.tags.get(key);
                if (v != null) autodump.put(key, v.toString());
            }
            list.add(new Json().put("hash", o.objHash).put("name", o.objName).put("type", o.objType).put("alive", o.alive)
                    .put("autodumpTags", autodump));
        }
        return new Json().put("ok", c.valid).put("objects", list).toString();
    }

    static String today(Map<String, String> kv) {
        String date = kv.get("date");
        if (date != null) return date;
        return LocalDate.now(ZoneId.of(kv.getOrDefault("tz", "Asia/Seoul"))).format(DateTimeFormatter.BASIC_ISO_DATE);
    }

    static String xlogs(Map<String, String> kv) throws Exception {
        MapPack request = new MapPack();
        request.put("loop", 0L);
        request.put("index", 0);
        request.put("limit", 0);
        request.put("count", Integer.parseInt(kv.getOrDefault("count", "200")));
        request.newList("objHash").add(Integer.parseInt(kv.get("objHash")));
        Call c = call(RequestCmd.TRANX_REAL_TIME_GROUP_LATEST, request, 0, false);
        List<Json> list = new ArrayList<>();
        for (Pack p : c.packs) {
            if (!(p instanceof XLogPack)) continue;
            XLogPack x = (XLogPack) p;
            list.add(new Json().put("txid", x.txid).put("objHash", x.objHash).put("service", x.service)
                    .put("elapsed", x.elapsed).put("error", x.error).put("endTime", x.endTime).put("xType", x.xType));
        }
        return new Json().put("ok", c.valid).put("count", list.size()).put("xlogs", list).toString();
    }

    static String text(Map<String, String> kv) throws Exception {
        int hash = Integer.parseInt(kv.get("hash"));
        MapPack request = new MapPack();
        request.put("date", today(kv));
        request.put("type", kv.get("type"));
        request.newList("hash").add(hash);
        Call c = call(RequestCmd.GET_TEXT, request, 0, false);
        String value = null;
        if (!c.packs.isEmpty() && c.packs.get(0) instanceof MapPack) {
            value = ((MapPack) c.packs.get(0)).getText(Hexa32.toString32(hash));
        }
        return new Json().put("ok", c.valid).put("found", value != null).put("text", value).toString();
    }

    static String profile(Map<String, String> kv) throws Exception {
        MapPack request = new MapPack();
        request.put("date", today(kv));
        request.put("txid", Long.parseLong(kv.get("txid")));
        request.put("max", -1);
        Call c = call(RequestCmd.TRANX_PROFILE, request, 0, false);
        Map<String, Integer> types = new TreeMap<>();
        List<Object> methods = new ArrayList<>();
        int steps = 0;
        for (Pack p : c.packs) {
            if (!(p instanceof XLogProfilePack)) continue;
            Step[] decoded = Step.toObjects(((XLogProfilePack) p).profile);
            steps += decoded.length;
            for (Step s : decoded) {
                types.merge(s.getClass().getSimpleName(), 1, Integer::sum);
                if (s instanceof MethodStep && methods.size() < 8) methods.add(((MethodStep) s).hash);
            }
        }
        return new Json().put("ok", c.valid).put("packs", c.packs.size()).put("steps", steps)
                .put("stepTypes", types).put("methodHashes", methods).toString();
    }

    static String counter(Map<String, String> kv) throws Exception {
        MapPack request = new MapPack();
        request.put("objHash", Integer.parseInt(kv.get("objHash")));
        request.put("counter", kv.get("name"));
        request.put("timetype", TimeTypeEnum.REALTIME);
        if (server == null || server.getSession() == 0) throw new IllegalStateException("not logged in");
        server.getConnectionPool().closeAll();
        TcpProxy proxy = TcpProxy.getTcpProxy(server.getId());
        Value v;
        boolean valid;
        try {
            v = proxy.getSingleValue(RequestCmd.COUNTER_REAL_TIME, request);
            valid = proxy.isValid();
        } finally {
            proxy.close();
        }
        return new Json().put("ok", valid).put("present", v != null)
                .put("valueType", v == null ? null : v.getClass().getSimpleName())
                .put("value", v == null ? null : v.toString()).toString();
    }

    // relay cmd=<CMD> objHash=<h> [soTimeout=ms] [blob=1] [expect=<substring>]
    //       [p.<key>=<text>] [i.<key>=<long>] — parameters of the MapPack sent.
    static String relay(Map<String, String> kv) throws Exception {
        String command = kv.get("cmd");
        MapPack request = new MapPack();
        if (kv.containsKey("objHash")) request.put("objHash", Integer.parseInt(kv.get("objHash")));
        if (kv.containsKey("objType")) request.put("objType", kv.get("objType"));
        for (Map.Entry<String, String> e : kv.entrySet()) {
            if (e.getKey().startsWith("p.")) request.put(e.getKey().substring(2), e.getValue());
            if (e.getKey().startsWith("i.")) request.put(e.getKey().substring(2), Long.parseLong(e.getValue()));
        }
        int soTimeout = Integer.parseInt(kv.getOrDefault("soTimeout", "0"));
        boolean blob = "1".equals(kv.get("blob"));
        Call c = call(command, request, soTimeout, blob);
        Json json = new Json().put("ok", c.valid).put("valid", c.valid).put("elapsedMs", c.elapsedMs)
                .put("count", blob ? c.blobs : c.packs.size());
        if (blob) {
            json.put("blobBytes", c.blobBytes).put("blobSha256", c.blobSha256);
            return json.toString();
        }
        List<Json> packs = new ArrayList<>();
        StringBuilder all = new StringBuilder();
        long bytes = 0;
        for (Pack p : c.packs) {
            Json pj = new Json().put("type", p.getClass().getSimpleName());
            if (p instanceof MapPack) {
                MapPack m = (MapPack) p;
                Map<String, Object> keys = new TreeMap<>();
                Iterator<String> it = m.keys();
                while (it.hasNext()) {
                    String key = it.next();
                    Value v = m.get(key);
                    if (v instanceof ListValue) keys.put(key, "list:" + ((ListValue) v).size());
                    else if (v instanceof BlobValue) keys.put(key, "blob:" + ((BlobValue) v).value.length);
                    else keys.put(key, v == null ? "null" : v.getClass().getSimpleName());
                    flatten(key, v, all);
                    bytes += v instanceof BlobValue ? ((BlobValue) v).value.length : 0;
                }
                if (m.containsKey("interopCall")) pj.put("interopCall", m.getLong("interopCall"));
                if (m.containsKey("interopTag")) pj.put("interopTag", m.getText("interopTag"));
                if (m.containsKey("error")) pj.put("errorText", m.getText("error"));
                pj.put("keys", keys);
            }
            packs.add(pj);
        }
        json.put("packs", packs).put("textChars", all.length()).put("blobBytes", bytes);
        for (Map.Entry<String, String> e : kv.entrySet()) {
            if (e.getKey().startsWith("expect")) json.put(e.getKey(), all.indexOf(e.getValue()) >= 0);
        }
        return json.toString();
    }

    static void flatten(String key, Value v, StringBuilder out) {
        if (v instanceof ListValue) {
            ListValue l = (ListValue) v;
            for (int i = 0; i < l.size(); i++) flatten(key, l.get(i), out);
        } else if (v instanceof MapValue) {
            MapValue m = (MapValue) v;
            for (java.util.Enumeration<String> it = m.keys(); it.hasMoreElements(); ) {
                String k = it.nextElement();
                flatten(k, m.get(k), out);
            }
        } else if (v instanceof TextValue) {
            out.append(key).append('=').append(((TextValue) v).value).append('\n');
        } else if (v != null && !(v instanceof BlobValue)) {
            out.append(key).append('=').append(v.toString()).append('\n');
        }
    }

    static String hex(byte[] b) {
        StringBuilder s = new StringBuilder();
        for (byte x : b) s.append(String.format("%02x", x));
        return s.toString();
    }

    // Minimal JSON writer for the summaries above.
    static final class Json {
        private final StringBuilder b = new StringBuilder("{");

        Json put(String key, Object value) {
            if (b.length() > 1) b.append(',');
            quote(b, key);
            b.append(':');
            write(b, value);
            return this;
        }

        @Override public String toString() { return b.toString() + "}"; }

        static void write(StringBuilder b, Object v) {
            if (v == null) b.append("null");
            else if (v instanceof Json) b.append(v.toString());
            else if (v instanceof Number || v instanceof Boolean) b.append(v);
            else if (v instanceof Map) {
                b.append('{');
                boolean first = true;
                for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                    if (!first) b.append(',');
                    first = false;
                    quote(b, String.valueOf(e.getKey()));
                    b.append(':');
                    write(b, e.getValue());
                }
                b.append('}');
            } else if (v instanceof List) {
                b.append('[');
                boolean first = true;
                for (Object o : (List<?>) v) {
                    if (!first) b.append(',');
                    first = false;
                    write(b, o);
                }
                b.append(']');
            } else quote(b, v.toString());
        }

        static void quote(StringBuilder b, String s) {
            b.append('"');
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == '"' || c == '\\') b.append('\\').append(c);
                else if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
                else b.append(c);
            }
            b.append('"');
        }
    }
}
