// Shared helpers for ManagementOracle: original ServiceHandlingProxy dispatch with the TcpServiceWorker
// NoNEXT trailer, request/response file output, deterministic JSON description of Pack/Value.
// No storage/codec logic is reimplemented here; all bytes come from the original common codec.
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import scouter.io.DataInputX;
import scouter.io.DataOutputX;
import scouter.lang.pack.MapPack;
import scouter.lang.pack.ObjectPack;
import scouter.lang.pack.Pack;
import scouter.lang.pack.PackEnum;
import scouter.lang.pack.PerfCounterPack;
import scouter.lang.pack.TextPack;
import scouter.lang.value.BlobValue;
import scouter.lang.value.BooleanValue;
import scouter.lang.value.DecimalValue;
import scouter.lang.value.DoubleValue;
import scouter.lang.value.FloatValue;
import scouter.lang.value.ListValue;
import scouter.lang.value.MapValue;
import scouter.lang.value.TextHashValue;
import scouter.lang.value.TextValue;
import scouter.lang.value.Value;
import scouter.lang.value.ValueEnum;
import scouter.net.TcpFlag;
import scouter.server.netio.service.ServiceHandlingProxy;

abstract class OracleSupport {
    static Path out;
    static final Path CWD = Paths.get("").toAbsolutePath();
    static final List<Object> cases = new ArrayList<Object>();
    static final Map<String, Object> facts = new LinkedHashMap<String, Object>();
    static final byte[] NONE = new byte[0];
    static boolean loaded;

    static void require(boolean ok, String what) { if (!ok) throw new AssertionError("oracle precondition failed: " + what); }

    static byte[] pack(Pack p) { try { return new DataOutputX().writePack(p).toByteArray(); } catch (IOException e) { throw new RuntimeException(e); } }
    static byte[] value(Value v) { try { return new DataOutputX().writeValue(v).toByteArray(); } catch (IOException e) { throw new RuntimeException(e); } }

    static MapPack map(Object... kv) {
        MapPack p = new MapPack();
        for (int i = 0; i < kv.length; i += 2) p.put((String) kv[i], toValue(kv[i + 1]));
        return p;
    }
    static MapValue mapValue(Object... kv) {
        MapValue m = new MapValue();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], toValue(kv[i + 1]));
        return m;
    }
    static ListValue list(Object... items) { ListValue l = new ListValue(); for (Object o : items) l.add(toValue(o)); return l; }
    static Value toValue(Object o) {
        if (o instanceof Value) return (Value) o;
        if (o instanceof String) return new TextValue((String) o);
        if (o instanceof Boolean) return new BooleanValue((Boolean) o);
        if (o instanceof Integer || o instanceof Long) return new DecimalValue(((Number) o).longValue());
        if (o instanceof Float) return new FloatValue((Float) o);
        if (o instanceof Double) return new DoubleValue((Double) o);
        throw new IllegalArgumentException("unsupported " + o);
    }

    static synchronized void load() {
        if (!loaded) { ServiceHandlingProxy.load(); loaded = true; }
    }

    /**
     * Runs one Client command through the original ServiceHandlingProxy (which swallows handler exceptions),
     * then appends NoNEXT like TcpServiceWorker. kind: 'P' = Pack items, 'V' = Value items.
     */
    static Map<String, Object> dispatch(String name, String cmd, byte[] body, char kind) throws Exception {
        load();
        int logStart = scouter.server.Logger.seen.size();
        ByteArrayOutputStream errBuf = new ByteArrayOutputStream();
        PrintStream oldErr = System.err;
        DataOutputX dout = new DataOutputX();
        System.setErr(new PrintStream(errBuf, true, "UTF-8"));
        try {
            ServiceHandlingProxy.process(cmd, new DataInputX(body), dout, true);
        } finally {
            System.err.flush();
            System.setErr(oldErr);
        }
        dout.writeByte(TcpFlag.NoNEXT);
        byte[] response = dout.toByteArray();
        Files.write(out.resolve(name + ".request.bin"), body);
        Files.write(out.resolve(name + ".response.bin"), response);
        Map<String, Object> rec = new LinkedHashMap<String, Object>();
        rec.put("name", name);
        rec.put("command", cmd);
        rec.put("request", name + ".request.bin");
        rec.put("request_bytes", body.length);
        rec.put("response", name + ".response.bin");
        rec.put("response_bytes", response.length);
        rec.put("item_kind", kind == 'P' ? "pack" : "value");
        rec.put("items", decodeItems(response, kind));
        rec.put("stderr_first_line", firstException(errBuf.toString("UTF-8")));
        List<String> logs = new ArrayList<String>();
        synchronized (scouter.server.Logger.seen) {
            for (int i = logStart; i < scouter.server.Logger.seen.size(); i++) logs.add(sanitize(scouter.server.Logger.seen.get(i)));
        }
        rec.put("logs", logs);
        cases.add(rec);
        return rec;
    }

    static List<Object> decodeItems(byte[] response, char kind) throws IOException {
        DataInputX in = new DataInputX(response);
        List<Object> items = new ArrayList<Object>();
        while (true) {
            byte flag = in.readByte();
            if (flag == TcpFlag.NoNEXT) break;
            require(flag == TcpFlag.HasNEXT, "response flag " + flag);
            try {
                items.add(kind == 'P' ? describePack(in.readPack()) : describeValue(in.readValue()));
            } catch (RuntimeException e) {
                // e.g. handler wrote HasNEXT, then threw before the item: the worker's NoNEXT becomes the item type byte.
                Map<String, Object> bad = new LinkedHashMap<String, Object>();
                bad.put("undecodable_after_HasNEXT", e.toString());
                items.add(bad);
                break;
            }
        }
        return items;
    }

    static String firstException(String err) {
        for (String line : err.split("\n")) {
            String l = line.trim();
            if (l.isEmpty() || l.startsWith("at ") || l.startsWith("[oracle-log]") || l.startsWith("...")) continue;
            return sanitize(l);
        }
        return null;
    }

    static String sanitize(String s) {
        if (s == null) return null;
        return scrubEnvDump(s.replace(CWD.toString(), "<cwd>").replace(CWD.toFile().getAbsoluteFile().toString(), "<cwd>"));
    }

    /** The oracle JVM environment holds only this marker (scripts/java-management.py ORACLE_ENV). */
    static final String ENV_MARKER = "SCOUTER_ORACLE_ENV_MARKER=synthetic";
    static final String ENV_DUMP = "<env+sysprop map toString>";

    /**
     * Original ConfigValueUtil.replaceSysProp calls ParamText.getText(Object) with the Map of all environment
     * variables and system properties, so every ${...} becomes that Map's toString. The dump (paths, user,
     * JVM details) is replaced by ENV_DUMP; the text before '{' and after '}' is kept.
     */
    static String scrubEnvDump(String s) {
        if (s == null || !s.contains(ENV_MARKER)) return s;
        int start = s.lastIndexOf('{', s.indexOf(ENV_MARKER)), end = s.lastIndexOf('}');
        if (start < 0 || end < start) return ENV_DUMP;
        return s.substring(0, start) + ENV_DUMP + s.substring(end + 1);
    }

    static Map<String, Object> describePack(Pack p) {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        switch (p.getPackType()) {
            case PackEnum.MAP: {
                m.put("pack", "map");
                Map<String, Object> v = new LinkedHashMap<String, Object>();
                Iterator<String> it = ((MapPack) p).keys();
                while (it.hasNext()) { String k = it.next(); v.put(k, describeValue(((MapPack) p).get(k))); }
                m.put("v", v);
                break;
            }
            case PackEnum.TEXT: {
                TextPack t = (TextPack) p;
                m.put("pack", "text"); m.put("xtype", t.xtype); m.put("hash", t.hash); m.put("text", t.text);
                break;
            }
            case PackEnum.OBJECT: {
                ObjectPack o = (ObjectPack) p;
                m.put("pack", "object"); m.put("objType", o.objType); m.put("objHash", o.objHash); m.put("objName", o.objName);
                m.put("address", o.address); m.put("version", o.version); m.put("alive", o.alive); m.put("wakeup", o.wakeup);
                m.put("tags", describeValue(o.tags));
                break;
            }
            case PackEnum.PERF_COUNTER: {
                PerfCounterPack c = (PerfCounterPack) p;
                m.put("pack", "perf_counter"); m.put("time", c.time); m.put("objName", c.objName); m.put("timetype", (int) c.timetype);
                m.put("data", describeValue(c.data));
                break;
            }
            default:
                m.put("pack", (int) p.getPackType()); m.put("toString", p.toString());
        }
        return m;
    }

    static Map<String, Object> describeValue(Value v) {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        if (v == null) { m.put("t", "java-null"); return m; }
        switch (v.getValueType()) {
            case ValueEnum.NULL: m.put("t", "null"); break;
            case ValueEnum.BOOLEAN: m.put("t", "boolean"); m.put("v", ((BooleanValue) v).value); break;
            case ValueEnum.DECIMAL: m.put("t", "decimal"); m.put("v", ((DecimalValue) v).value); break;
            case ValueEnum.FLOAT: m.put("t", "float"); m.put("v", Float.toString(((FloatValue) v).value)); break;
            case ValueEnum.DOUBLE: m.put("t", "double"); m.put("v", Double.toString(((DoubleValue) v).value)); break;
            case ValueEnum.TEXT: m.put("t", "text"); m.put("v", ((TextValue) v).value); break;
            case ValueEnum.TEXT_HASH: m.put("t", "text_hash"); m.put("v", ((TextHashValue) v).value); break;
            case ValueEnum.BLOB: { byte[] b = ((BlobValue) v).value; m.put("t", "blob"); m.put("len", b == null ? -1 : b.length); m.put("hex", hex(b)); break; }
            case ValueEnum.LIST: {
                List<Object> l = new ArrayList<Object>(); ListValue lv = (ListValue) v;
                for (int i = 0; i < lv.size(); i++) l.add(describeValue(lv.get(i)));
                m.put("t", "list"); m.put("v", l); break;
            }
            case ValueEnum.MAP: {
                Map<String, Object> mv = new LinkedHashMap<String, Object>();
                Enumeration<String> keys = ((MapValue) v).keys();
                while (keys.hasMoreElements()) { String k = keys.nextElement(); mv.put(k, describeValue(((MapValue) v).get(k))); }
                m.put("t", "map"); m.put("v", mv); break;
            }
            default: m.put("t", "type" + v.getValueType()); m.put("v", v.toString());
        }
        return m;
    }

    static String hex(byte[] b) {
        if (b == null) return null;
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) sb.append(String.format("%02x", x & 0xff));
        return sb.toString();
    }

    static String sha256(byte[] b) {
        try { return hex(MessageDigest.getInstance("SHA-256").digest(b)); } catch (Exception e) { throw new RuntimeException(e); }
    }

    /** Relative path -> {bytes, sha256} for every regular file under dir (sorted). */
    static Map<String, Object> tree(Path dir) throws IOException {
        Map<String, Object> m = new TreeMap<String, Object>();
        if (!Files.exists(dir)) return m;
        List<Path> files = new ArrayList<Path>();
        collect(dir.toFile(), files);
        for (Path p : files) {
            Map<String, Object> f = new LinkedHashMap<String, Object>();
            byte[] b = Files.readAllBytes(p);
            f.put("bytes", b.length); f.put("sha256", sha256(b));
            m.put(dir.relativize(p).toString().replace(File.separatorChar, '/'), f);
        }
        return m;
    }
    private static void collect(File f, List<Path> files) {
        File[] children = f.listFiles();
        if (children == null) return;
        for (File c : children) { if (c.isDirectory()) collect(c, files); else files.add(c.toPath()); }
    }

    static void write(String name, byte[] bytes) throws IOException { Files.write(out.resolve(name), bytes); }
    static void writeText(String name, String text) throws IOException { write(name, text.getBytes(StandardCharsets.UTF_8)); }
    static byte[] read(Path p) throws IOException { return Files.readAllBytes(p); }

    static void finish(String scenario) throws IOException {
        Map<String, Object> root = new LinkedHashMap<String, Object>();
        root.put("scenario", scenario);
        root.put("cases", cases);
        root.put("facts", facts);
        writeText("cases.json", Json.write(root) + "\n");
        System.out.println(scenario + ": " + cases.size() + " handler cases, " + facts.size() + " facts");
    }

    static void sleep(long ms) { try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }

    /** Minimal deterministic JSON writer (insertion-ordered maps). */
    static final class Json {
        static String write(Object o) { StringBuilder sb = new StringBuilder(); write(sb, o, 0); return sb.toString(); }
        @SuppressWarnings("unchecked")
        static void write(StringBuilder sb, Object o, int indent) {
            if (o == null) sb.append("null");
            else if (o instanceof String) str(sb, (String) o);
            else if (o instanceof Boolean || o instanceof Integer || o instanceof Long || o instanceof Short || o instanceof Byte) sb.append(o);
            else if (o instanceof Map) {
                Map<String, Object> m = (Map<String, Object>) o;
                if (m.isEmpty()) { sb.append("{}"); return; }
                sb.append("{\n"); int i = 0;
                for (Map.Entry<String, Object> e : m.entrySet()) {
                    pad(sb, indent + 1); str(sb, e.getKey()); sb.append(": "); write(sb, e.getValue(), indent + 1);
                    sb.append(++i < m.size() ? ",\n" : "\n");
                }
                pad(sb, indent); sb.append('}');
            } else if (o instanceof List) {
                List<Object> l = (List<Object>) o;
                if (l.isEmpty()) { sb.append("[]"); return; }
                sb.append("[\n");
                for (int i = 0; i < l.size(); i++) { pad(sb, indent + 1); write(sb, l.get(i), indent + 1); sb.append(i + 1 < l.size() ? ",\n" : "\n"); }
                pad(sb, indent); sb.append(']');
            } else str(sb, String.valueOf(o));
        }
        static void pad(StringBuilder sb, int n) { for (int i = 0; i < n; i++) sb.append("  "); }
        static void str(StringBuilder sb, String s) {
            s = sanitize(s);
            sb.append('"');
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                switch (c) {
                    case '"': sb.append("\\\""); break;
                    case '\\': sb.append("\\\\"); break;
                    case '\n': sb.append("\\n"); break;
                    case '\r': sb.append("\\r"); break;
                    case '\t': sb.append("\\t"); break;
                    default: if (c < 0x20) sb.append(String.format("\\u%04x", (int) c)); else sb.append(c);
                }
            }
            sb.append('"');
        }
    }
}
