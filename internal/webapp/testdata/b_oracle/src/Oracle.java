import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import scouter.io.DataOutputX;
import scouter.lang.enumeration.ParameterizedMessageLevel;
import scouter.lang.pack.XLogPack;
import scouter.lang.step.*;
import scouter.lang.value.*;
import scouter.util.HashUtil;
import scouter.util.Hexa32;
import scouterx.webapp.model.XLogData;
import scouterx.webapp.model.scouter.SXLog;
import scouterx.webapp.model.scouter.step.SCommonSpanStep;
import scouterx.webapp.model.scouter.step.SSpanCallStep;
import scouterx.webapp.model.scouter.step.SSpanStep;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

// Java oracle for internal/webapp B controllers (scouter dee47fb5 sources,
// jackson-databind 2.12.6.1 as pinned in scouter.webapp/pom.xml, Lombok 1.18.20).
public class Oracle {
    static final ObjectMapper api = new ObjectMapper().configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, true);
    static final ObjectMapper out = new ObjectMapper();

    static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) sb.append(String.format("%02x", x & 0xff));
        return sb.toString();
    }

    static void base(StepSingle s, int i) { s.parent = i - 1; s.index = i; s.start_time = 1000 + i; s.start_cpu = 50 + i; }

    static <T extends CommonSpanStep> T span(T s, int i) {
        base(s, i);
        s.hash = -1234567; s.elapsed = 77; s.error = 3; s.timestamp = 1700000000123L; s.spanType = 2;
        s.localEndpointServiceName = 111; s.localEndpointIp = new byte[]{10, 0, 0, 1}; s.localEndpointPort = (short) 8080;
        s.remoteEndpointServiceName = 0; s.remoteEndpointIp = new byte[]{(byte) 192, (byte) 168, 1, 2}; s.remoteEndpointPort = (short) -2;
        s.debug = true; s.shared = false;
        s.annotationTimestamps = new ListValue(); s.annotationTimestamps.add(1700000000001L); s.annotationTimestamps.add(1700000000002L);
        s.annotationValues = new ListValue(); s.annotationValues.add("cs"); s.annotationValues.add("cr");
        s.tags = new MapValue(); s.tags.put("http.method", "GET"); s.tags.put("http.status_code", 200L); s.tags.put("b", "x"); s.tags.put("a", "y");
        return s;
    }

    static List<Step> steps() {
        List<Step> l = new ArrayList<>();
        int i = 1;
        MethodStep m = new MethodStep(); base(m, i++); m.hash = 11; m.elapsed = 12; m.cputime = 13; l.add(m);
        MethodStep2 m2 = new MethodStep2(); base(m2, i++); m2.hash = -11; m2.elapsed = 12; m2.cputime = 13; m2.error = 14; l.add(m2);
        SqlStep q = new SqlStep(); base(q, i++); q.hash = 21; q.elapsed = 22; q.cputime = 23; q.param = "a,b"; q.error = 24; l.add(q);
        SqlStep2 q2 = new SqlStep2(); base(q2, i++); q2.hash = 21; q2.param = "p"; q2.xtype = 1; l.add(q2);
        SqlStep3 q3 = new SqlStep3(); base(q3, i++); q3.hash = 21; q3.param = ""; q3.xtype = 0x12; q3.updated = -1; l.add(q3);
        MessageStep ms = new MessageStep(); base(ms, i++); ms.message = "hello \"world\"\n"; l.add(ms);
        HashedMessageStep hm = new HashedMessageStep(); base(hm, i++); hm.hash = 31; hm.time = 32; hm.value = 33; l.add(hm);
        ParameterizedMessageStep pm = new ParameterizedMessageStep(); base(pm, i++); pm.setMessage(41, "x", "y"); pm.setElapsed(42); pm.setLevel(ParameterizedMessageLevel.WARN); l.add(pm);
        SocketStep so = new SocketStep(new byte[]{127, 0, 0, 1}, 6100); base(so, i++); so.elapsed = 51; so.error = 52; l.add(so);
        ApiCallStep ac = new ApiCallStep(); base(ac, i++); ac.txid = 9000000000L; ac.hash = 61; ac.elapsed = 62; ac.cputime = 63; ac.error = 64; ac.opt = 1; ac.address = "http://a"; l.add(ac);
        ApiCallStep2 ac2 = new ApiCallStep2(); base(ac2, i++); ac2.txid = -5; ac2.hash = 61; ac2.opt = 0; ac2.address = ""; ac2.async = 1; ac2.start = 99; l.add(ac2);
        ThreadSubmitStep ts = new ThreadSubmitStep(); base(ts, i++); ts.txid = 7; ts.hash = 71; ts.elapsed = 72; ts.cputime = 73; ts.error = 74; l.add(ts);
        DumpStep d = new DumpStep(); base(d, i++); d.stacks = new int[]{81, -82}; d.threadId = 83; d.threadName = "main"; d.threadState = "RUNNABLE"; d.lockOwnerId = -1; d.lockName = ""; d.lockOwnerName = "o"; l.add(d);
        DispatchStep ds = new DispatchStep(); base(ds, i++); ds.txid = 8; ds.hash = 91; ds.opt = 2; ds.address = "q"; l.add(ds);
        ThreadCallPossibleStep tc = new ThreadCallPossibleStep(); base(tc, i++); tc.txid = 9; tc.hash = 101; tc.elapsed = 102; tc.threaded = 1; tc.nameTemp = "n"; tc.isIgnoreIfNoThreaded = true; l.add(tc);
        MethodSum su = new MethodSum(); su.hash = 111; su.count = 112; su.elapsed = 113; su.cputime = 114; l.add(su);
        SqlSum ss = new SqlSum(); ss.hash = 121; ss.count = 122; ss.elapsed = 123; ss.cputime = 124; ss.error = 125; ss.param = "p"; ss.param_error = "pe"; l.add(ss);
        ApiCallSum as = new ApiCallSum(); as.hash = 131; as.count = 132; as.elapsed = 133; as.cputime = 134; as.error = 135; as.opt = 0; l.add(as);
        SocketSum sk = new SocketSum(new byte[]{1, 2, 3, 4}, 141); sk.count = 142; sk.elapsed = 143; sk.error = 144; l.add(sk);
        MessageSum mg = new MessageSum("m"); mg.count = 151; l.add(mg);
        StepControl sc = new StepControl(" ** Profile Truncated ** "); sc.code = 161; l.add(sc);
        l.add(span(new SpanStep(), i++));
        SpanCallStep spc = span(new SpanCallStep(), i++); spc.txid = 171; spc.opt = 1; spc.address = "addr"; spc.async = 1; l.add(spc);
        return l;
    }

    public static void main(String[] args) throws Exception {
        ObjectNode root = out.createObjectNode();
        root.put("timezone", TimeZone.getDefault().getID());

        ArrayNode stepArr = root.putArray("steps");
        for (Step s : steps()) {
            DataOutputX dout = new DataOutputX();
            dout.writeStep(s);
            byte[] b = dout.toByteArray();
            Step dec = Step.toObjectList(b).get(0);
            ObjectNode n = stepArr.addObject();
            n.put("hex", hex(b));
            n.put("json", api.writeValueAsString(dec));
            if (dec instanceof SpanStep || dec instanceof SpanCallStep) {
                SCommonSpanStep c = dec instanceof SpanStep ? SSpanStep.of((SpanStep) dec) : SSpanCallStep.of((SpanCallStep) dec);
                int lh = c.getLocalEndpoint().getHash(), rh = c.getRemoteEndpoint().getHash();
                c.getLocalEndpoint().setServiceName(lh == 0 ? "" : "object#" + lh);
                c.getRemoteEndpoint().setServiceName(rh == 0 ? "" : "object#" + rh);
                n.put("sspan", api.writeValueAsString(c));
            }
        }

        XLogPack x = new XLogPack();
        x.endTime = 1700000000999L; x.objHash = -101; x.service = 202; x.txid = -303; x.caller = 404; x.gxid = 505;
        x.elapsed = 606; x.error = 707; x.cpu = 808; x.sqlCount = 9; x.sqlTime = 10; x.ipaddr = new byte[]{(byte) 172, 16, 0, 9};
        x.kbytes = 11; x.status = 12; x.userid = -13; x.userAgent = 14; x.referer = 15; x.group = 16; x.apicallCount = 17;
        x.apicallTime = 18; x.countryCode = "KR"; x.city = 19; x.xType = 3; x.login = 20; x.desc = 0; x.webHash = 21; x.webTime = 22;
        x.hasDump = 1; x.threadNameHash = 23; x.text1 = "t1"; x.text2 = ""; x.queuingHostHash = 24; x.queuingTime = 25;
        x.queuing2ndHostHash = 26; x.queuing2ndTime = 27; x.text3 = "t3"; x.text4 = "t4"; x.text5 = "t5"; x.profileCount = 28;
        x.profileSize = 29;
        byte[] xb = new DataOutputX().writePack(x).toByteArray();
        XLogPack xd = (XLogPack) new scouter.io.DataInputX(xb).readPack();
        ObjectNode xn = root.putObject("xlog");
        xn.put("hex", hex(xb));
        xn.put("sxlog", api.writeValueAsString(SXLog.of(xd)));
        xn.put("xlogData", api.writeValueAsString(XLogData.of(xd, 0)));

        ArrayNode dbl = root.putArray("doubles");
        for (double d : new double[]{0.0, -0.0, 1.0, 0.1, 1.5e-3, 9.99e-4, 1234567.0, 1.0e7, 12345678.9, 1.0e-10, -2.5e21, Double.MAX_VALUE, Double.MIN_VALUE, 100.0 / 3}) {
            ObjectNode n = dbl.addObject();
            n.put("bits", Long.toString(Double.doubleToRawLongBits(d)));
            n.put("double", Double.toString(d));
            n.put("decimalValue", new DoubleValue(d).toString());
        }
        ArrayNode flt = root.putArray("floats");
        for (float f : new float[]{0.0f, 1.0f, 0.1f, 3.4028235e38f, 1.0e7f, 9999999.0f, 1.0e-3f, 9.9e-4f, 1.4e-45f, 33.333332f}) {
            ObjectNode n = flt.addObject();
            n.put("bits", Integer.toString(Float.floatToRawIntBits(f)));
            n.put("float", Float.toString(f));
        }

        ArrayNode ints = root.putArray("intSets");
        int[][] intCases = {{100, 1, 17, 33}, {65536, 1, 16, 0, -1, 2147483647}, {5, 4, 3, 2, 1, 0, 31, 47, 63, 79, 95, 111, 127, 143, 159}};
        for (int[] c : intCases) {
            Set<Integer> s = new HashSet<>();
            for (int v : c) s.add(v);
            ObjectNode n = ints.addObject();
            ArrayNode in = n.putArray("in");
            for (int v : c) in.add(v);
            ArrayNode o = n.putArray("order");
            for (int v : s) o.add(v);
        }
        ArrayNode strs = root.putArray("stringSets");
        String[][] strCases = {{"service", "sql", "error", "apicall", "object", "ua", "hmsg"}, {"http.method", "http.status_code", "b", "a"}, {"k1", "k2", "k10", "한글", "]u:k"}};
        for (String[] c : strCases) {
            Set<String> s = new HashSet<>(Arrays.asList(c));
            ObjectNode n = strs.addObject();
            ArrayNode in = n.putArray("in");
            for (String v : c) in.add(v);
            ArrayNode o = n.putArray("order");
            for (String v : s) o.add(v);
        }

        ArrayNode fmts = root.putArray("formats");
        String[][] fmtCases = {{"user %s login %s", "a", "b"}, {"%2$s-%1$s", "a", "b"}, {"%s %<s %S", "ab", "cd"}, {"%-5s|%5s|%.1s", "a", "b", "xyz"},
                {"%d", "1"}, {"%s %s %s", "a", "b"}, {"100%% %s%n", "z"}, {"%b %h", "x", "abc"}, {"no params", "a"}, {"%q", "a"}};
        for (String[] c : fmtCases) {
            ParameterizedMessageStep p = new ParameterizedMessageStep();
            p.setMessage(1, Arrays.copyOfRange(c, 1, c.length));
            ObjectNode n = fmts.addObject();
            n.put("format", c[0]);
            ArrayNode a = n.putArray("args");
            for (int k = 1; k < c.length; k++) a.add(c[k]);
            n.put("result", p.buildMessasge(c[0]));
        }

        ArrayNode sdf = root.putArray("simpleDate");
        for (String s : new String[]{"20240101123456", "20240101250000", "20241301000000", "2024010112345678", "20240101123456xyz", "2024010112345", "2024010112ab56", "20240310023000", ""}) {
            ObjectNode n = sdf.addObject();
            n.put("in", s);
            try {
                n.put("millis", Long.toString(new SimpleDateFormat("yyyyMMddHHmmss").parse(s).getTime()));
            } catch (Exception e) {
                n.put("error", e.getClass().getSimpleName());
            }
        }
        ArrayNode ldt = root.putArray("localDateTime");
        String[][] ldtCases = {{"yyyyMMddHHmm", "202401011234"}, {"yyyyMMddHHmm", "202402301200"}, {"yyyyMMddHHmm", "202402321200"}, {"yyyyMMddHHmm", "202401012400"},
                {"yyyyMMddHHmm", "202401012401"}, {"yyyyMMddHHmm", "20240101123"}, {"yyyyMMddHH", "2024010112"}, {"yyyyMMddHH", "2024013124"}, {"yyyyMMddHH", "2024133112"}};
        for (String[] c : ldtCases) {
            ObjectNode n = ldt.addObject();
            n.put("pattern", c[0]);
            n.put("in", c[1]);
            try {
                LocalDateTime t = LocalDateTime.parse(c[1], DateTimeFormatter.ofPattern(c[0]));
                n.put("millis", Long.toString(t.atZone(ZoneId.systemDefault()).toEpochSecond() * 1000L));
            } catch (Exception e) {
                n.put("error", e.getClass().getSimpleName());
            }
        }

        ArrayNode hx = root.putArray("shortener");
        for (String u : new String[]{"http://localhost:6180/index.html?a=1", "", "한글"}) {
            ObjectNode n = hx.addObject();
            n.put("url", u);
            n.put("hash", HashUtil.hash(u));
            n.put("hexa32", Hexa32.toString32(HashUtil.hash(u)));
        }
        ArrayNode h32 = root.putArray("hexa32");
        for (long v : new long[]{0, 9, 10, -1, Long.MIN_VALUE, Long.MAX_VALUE, 792539709424970410L}) {
            ObjectNode n = h32.addObject();
            n.put("n", Long.toString(v));
            n.put("s", Hexa32.toString32(v));
        }
        System.out.println(out.writerWithDefaultPrettyPrinter().writeValueAsString(root));
    }
}
