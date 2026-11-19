import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;

import scouter.io.DataInputX;
import scouter.io.DataOutputX;
import scouter.lang.pack.AlertPack;
import scouter.lang.pack.BatchPack;
import scouter.lang.pack.DroppedXLogPack;
import scouter.lang.pack.InteractionPerfCounterPack;
import scouter.lang.pack.MapPack;
import scouter.lang.pack.ObjectPack;
import scouter.lang.pack.Pack;
import scouter.lang.pack.PerfCounterPack;
import scouter.lang.pack.SpanContainerPack;
import scouter.lang.pack.SpanPack;
import scouter.lang.pack.StackPack;
import scouter.lang.pack.StatusPack;
import scouter.lang.pack.SummaryPack;
import scouter.lang.pack.TextPack;
import scouter.lang.pack.XLogPack;
import scouter.lang.pack.XLogProfilePack;
import scouter.lang.pack.XLogProfilePack2;
import scouter.lang.value.BlobValue;
import scouter.lang.value.BooleanValue;
import scouter.lang.value.DecimalValue;
import scouter.lang.value.DoubleSummary;
import scouter.lang.value.DoubleValue;
import scouter.lang.value.FloatArray;
import scouter.lang.value.FloatValue;
import scouter.lang.value.IP4Value;
import scouter.lang.value.IntArray;
import scouter.lang.value.ListValue;
import scouter.lang.value.LongArray;
import scouter.lang.value.LongSummary;
import scouter.lang.value.MapValue;
import scouter.lang.value.NullValue;
import scouter.lang.value.TextArray;
import scouter.lang.value.TextHashValue;
import scouter.lang.value.TextValue;
import scouter.lang.value.Value;
import scouter.util.DateUtil;

/**
 * Synthetic golden for the Pack/Value toString() text that NetDataProcessor prints for
 * log_udp_* (and XLogProfilePack*.toDebugString used by log_test_rate). Every value is
 * synthetic; the SQL text is a made-up statement used only to check the Go masking.
 *
 * Usage: java PackStringOracle <outdir> <mode>
 *   mode "packs": packstring.json (packs, values, profile debug strings) + numbers.json
 *   mode "time":  time-<timezone id with / replaced by _>.json
 */
public class PackStringOracle {
	public static void main(String[] args) throws Exception {
		File out = new File(args[0]);
		if ("time".equals(args[1])) {
			String tz = TimeZone.getDefault().getID();
			write(new File(out, "time-" + tz.replace('/', '_') + ".json"), timeCases(tz));
			return;
		}
		StringBuilder sb = new StringBuilder();
		sb.append("{\n\"timezone\": ").append(q(TimeZone.getDefault().getID())).append(",\n");
		sb.append("\"packs\": [\n");
		List<Object[]> packs = packs();
		for (int i = 0; i < packs.size(); i++) {
			Pack p = (Pack) packs.get(i)[1];
			byte[] b = new DataOutputX().writePack(p).toByteArray();
			// NetDataProcessor prints decoded packs: MapValue order is the Hashtable built from wire order.
			Pack decoded = new DataInputX(b).readPack();
			sb.append(i == 0 ? "" : ",\n").append("{\"name\": ").append(q((String) packs.get(i)[0]))
					.append(", \"pack\": ").append(q(hex(b))).append(", \"text\": ").append(qn(decoded.toString())).append("}");
		}
		sb.append("\n],\n\"values\": [\n");
		List<Object[]> values = values();
		for (int i = 0; i < values.size(); i++) {
			Value v = (Value) values.get(i)[1];
			byte[] b = new DataOutputX().writeValue(v).toByteArray();
			Value decoded = new DataInputX(b).readValue();
			sb.append(i == 0 ? "" : ",\n").append("{\"name\": ").append(q((String) values.get(i)[0]))
					.append(", \"value\": ").append(q(hex(b))).append(", \"text\": ").append(qn(decoded.toString())).append("}");
		}
		sb.append("\n],\n\"profile_debug\": [\n");
		List<Object[]> dbg = profileDebug();
		for (int i = 0; i < dbg.size(); i++) {
			XLogProfilePack p = (XLogProfilePack) dbg.get(i)[1];
			String svc = (String) dbg.get(i)[2];
			byte[] b = new DataOutputX().writePack(p).toByteArray();
			XLogProfilePack decoded = (XLogProfilePack) new DataInputX(b).readPack();
			sb.append(i == 0 ? "" : ",\n").append("{\"name\": ").append(q((String) dbg.get(i)[0]))
					.append(", \"pack\": ").append(q(hex(b))).append(", \"svc_name\": ").append(svc == null ? "null" : q(svc))
					.append(", \"text\": ").append(q(decoded.toDebugString(svc))).append("}");
		}
		sb.append("\n]\n}\n");
		write(new File(out, "packstring.json"), sb.toString());
		write(new File(out, "numbers.json"), numbers());
	}

	static String timeCases(String tz) {
		long[] fixed = { 0L, 1L, DateUtil.getTime("20000101", "yyyyMMdd") - 1, DateUtil.getTime("20000101", "yyyyMMdd"),
				946684799999L, 946684800000L, 1700000000000L, 1700000000005L, 1700000000120L, 1700000000100L,
				1760486399999L, 1760486400000L, 1767225599999L, 1767225600000L,
				// America/New_York 2026 DST transitions: 2026-03-08 07:00Z and 2026-11-01 06:00Z.
				1772953199999L, 1772953200000L, 1772956800000L, 1793512799999L, 1793512800000L, 1793516400000L,
				4102358400000L };
		Random r = new Random(20261015L);
		List<Long> times = new ArrayList<Long>();
		for (long t : fixed)
			times.add(t);
		for (int i = 0; i < 300; i++)
			times.add(946684800000L + (long) (r.nextDouble() * (4102358400000L - 946684800000L)));
		StringBuilder sb = new StringBuilder();
		sb.append("{\n\"timezone\": ").append(q(tz)).append(",\n\"cases\": [\n");
		for (int i = 0; i < times.size(); i++) {
			long t = times.get(i);
			sb.append(i == 0 ? "" : ",\n").append("{\"ms\": ").append(t);
			sb.append(", \"datetime\": ").append(q(DateUtil.datetime(t)));
			sb.append(", \"timestamp\": ").append(q(DateUtil.timestamp(t)));
			sb.append(", \"yyyymmdd\": ").append(q(DateUtil.yyyymmdd(t)));
			sb.append(", \"date_unit\": ").append(DateUtil.getDateUnit(t));
			sb.append(", \"sql_timestamp\": ").append(q(new Timestamp(t).toString()));
			sb.append("}");
		}
		sb.append("\n],\n\"parse\": [\n");
		String[] dates = { "20000101", "19991231", "19700101", "20991231", "21000101", "20261015", "20240229", "20250229",
				"20261340", "20261100", "20261232", "2026+101", "+0261015", "-0011015", "abcdefgh", "2026101x" };
		for (int i = 0; i < dates.length; i++) {
			String d = dates[i];
			String unit;
			try {
				unit = Long.toString(DateUtil.getDateUnit(DateUtil.yyyymmdd(d)));
			} catch (Exception e) {
				unit = q("error");
			}
			sb.append(i == 0 ? "" : ",\n").append("{\"date\": ").append(q(d)).append(", \"date_unit\": ").append(unit).append("}");
		}
		sb.append("\n]\n}\n");
		return sb.toString();
	}

	static String numbers() {
		DecimalFormat fmt = new DecimalFormat("#0.0#################");
		List<Float> floats = new ArrayList<Float>();
		float[] ff = { 0f, -0f, 1f, -1f, 0.1f, 0.5f, 1.5f, 3.14159f, 100f, 1234567f, 9999999f, 1e7f, 1.0E-3f, 9.999E-4f,
				1e-5f, 1.17549435E-38f, 1.4E-45f, Float.MAX_VALUE, Float.NaN, Float.POSITIVE_INFINITY,
				Float.NEGATIVE_INFINITY, 123456.78f, 0.001f, 16777216f, 3.4e10f, 2.5e-7f };
		for (float f : ff)
			floats.add(f);
		Random r = new Random(1015L);
		for (int i = 0; i < 400; i++)
			floats.add(Float.intBitsToFloat(r.nextInt()));
		for (int i = 0; i < 400; i++)
			floats.add((float) ((r.nextDouble() - 0.5) * Math.pow(10, r.nextInt(16) - 6)));
		List<Double> doubles = new ArrayList<Double>();
		double[] dd = { 0d, -0d, 1d, -1d, 0.1d, 0.5d, 1.5d, 3.141592653589793d, 100d, 1e7d, 1e15d, 1e16d, 1e17d, 1e20d,
				1e22d, 1e23d, 2e23d, 1.7976931348623157E308d, 4.9E-324d, 1e-17d, 1e-18d, 1e-19d, 5e-19d, 1.5e-18d,
				2.5e-18d, 0.0000000000000000015d, 1.25e-17d, -1e-20d, 1e-5d, 0.001d, 0.3d, 2.0 / 3.0, 1.0 / 3.0,
				123456789.123456789d, 0.000123456789012345678d, 9.999999999999999e-19d, 9.5e-19d, -9.5e-19d,
				Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 2.0E-3d, 1.0E-2d, 3.0E10d };
		for (double d : dd)
			doubles.add(d);
		for (int i = 0; i < 400; i++)
			doubles.add(Double.longBitsToDouble(r.nextLong()));
		for (int i = 0; i < 600; i++)
			doubles.add((r.nextDouble() - 0.5) * Math.pow(10, r.nextInt(44) - 22));
		StringBuilder sb = new StringBuilder();
		sb.append("{\n\"float_to_string\": [\n");
		for (int i = 0; i < floats.size(); i++) {
			float f = floats.get(i);
			sb.append(i == 0 ? "" : ",\n").append("[").append(q(String.format("%08x", Float.floatToRawIntBits(f))))
					.append(", ").append(q(Float.toString(f))).append("]");
		}
		sb.append("\n],\n\"double_decimal_format\": [\n");
		for (int i = 0; i < doubles.size(); i++) {
			double d = doubles.get(i);
			sb.append(i == 0 ? "" : ",\n").append("[").append(q(String.format("%016x", Double.doubleToRawLongBits(d))))
					.append(", ").append(q(fmt.format(d))).append("]");
		}
		sb.append("\n]\n}\n");
		return sb.toString();
	}

	static MapValue mapOf(Object... kv) {
		MapValue m = new MapValue();
		for (int i = 0; i < kv.length; i += 2)
			m.put((String) kv[i], (Value) kv[i + 1]);
		return m;
	}

	static ListValue listOf(Value... vs) {
		ListValue l = new ListValue();
		for (Value v : vs)
			l.add(v);
		return l;
	}

	static DoubleSummary dsum(double sum, int count, double min, double max) {
		DoubleSummary s = new DoubleSummary();
		s.sum = sum;
		s.count = count;
		s.min = min;
		s.max = max;
		return s;
	}

	static LongSummary lsum(long sum, int count, long min, long max) {
		LongSummary s = new LongSummary();
		s.sum = sum;
		s.count = count;
		s.min = min;
		s.max = max;
		return s;
	}

	static MapValue bigMap(int n, long seed) {
		Random r = new Random(seed);
		MapValue m = new MapValue();
		for (int i = 0; i < n; i++) {
			String key;
			switch (i % 4) {
			case 0:
				key = "k" + i;
				break;
			case 1:
				key = "counter_" + Integer.toHexString(r.nextInt());
				break;
			case 2:
				key = "한글" + i; // Hangul key
				break;
			default:
				key = "Key-" + r.nextInt(1000) + "-😀"; // supplementary char key
			}
			m.put(key, new DecimalValue(i));
		}
		return m;
	}

	static List<Object[]> values() {
		List<Object[]> v = new ArrayList<Object[]>();
		v.add(new Object[] { "null", new NullValue() });
		v.add(new Object[] { "bool_true", new BooleanValue(true) });
		v.add(new Object[] { "bool_false", new BooleanValue(false) });
		v.add(new Object[] { "decimal_0", new DecimalValue(0) });
		v.add(new Object[] { "decimal_neg", new DecimalValue(-1234567890123L) });
		v.add(new Object[] { "decimal_min", new DecimalValue(Long.MIN_VALUE) });
		v.add(new Object[] { "float", new FloatValue(1.5f) });
		v.add(new Object[] { "float_big", new FloatValue(1.0e10f) });
		v.add(new Object[] { "float_small", new FloatValue(1.0e-5f) });
		v.add(new Object[] { "double", new DoubleValue(3.25d) });
		v.add(new Object[] { "double_big", new DoubleValue(1.0e20d) });
		v.add(new Object[] { "double_tiny", new DoubleValue(1.0e-20d) });
		v.add(new Object[] { "double_negzero", new DoubleValue(-0.0d) });
		v.add(new Object[] { "double_nan", new DoubleValue(Double.NaN) });
		v.add(new Object[] { "double_inf", new DoubleValue(Double.NEGATIVE_INFINITY) });
		v.add(new Object[] { "double_summary", dsum(10.5, 3, 0.25, 7.0) });
		v.add(new Object[] { "long_summary", lsum(100, 4, -5, 70) });
		v.add(new Object[] { "text", new TextValue("plain text 한글") });
		v.add(new Object[] { "text_empty", new TextValue("") });
		v.add(new Object[] { "text_hash_pos", new TextHashValue(0x1abc) });
		v.add(new Object[] { "text_hash_neg", new TextHashValue(-0x1abc) });
		v.add(new Object[] { "text_hash_min", new TextHashValue(Integer.MIN_VALUE) });
		v.add(new Object[] { "blob", new BlobValue(new byte[] { 1, 2, 3 }) });
		v.add(new Object[] { "blob_empty", new BlobValue(new byte[0]) });
		v.add(new Object[] { "ip4", new IP4Value(new byte[] { (byte) 192, 0, 2, (byte) 255 }) });
		v.add(new Object[] { "int_array", new IntArray(new int[] { 1, -2, Integer.MAX_VALUE }) });
		v.add(new Object[] { "int_array_empty", new IntArray(new int[0]) });
		v.add(new Object[] { "float_array", new FloatArray(new float[] { 1f, 0.5f, 1e-4f, 3e9f }) });
		v.add(new Object[] { "long_array", new LongArray(new long[] { Long.MIN_VALUE, 0, 9 }) });
		v.add(new Object[] { "text_array", new TextArray(new String[] { "a", "", "b c" }) });
		v.add(new Object[] { "list", listOf(new DecimalValue(1), new TextValue("x"), new NullValue(), listOf(), new MapValue()) });
		v.add(new Object[] { "list_empty", new ListValue() });
		v.add(new Object[] { "map_empty", new MapValue() });
		v.add(new Object[] { "map_small", mapOf("b", new DecimalValue(2), "a", new TextValue("1"), "c", new NullValue()) });
		v.add(new Object[] { "map_nested", mapOf("inner", mapOf("x", new DecimalValue(1), "y", listOf(new BooleanValue(true))), "z", new DoubleValue(0.1)) });
		int[] sizes = { 1, 7, 8, 9, 12, 17, 18, 25, 36, 40, 72, 100 };
		for (int i = 0; i < sizes.length; i++)
			v.add(new Object[] { "map_order_" + sizes[i], bigMap(sizes[i], 77L + i) });
		return v;
	}

	static List<Object[]> packs() {
		List<Object[]> v = new ArrayList<Object[]>();
		MapPack mp = new MapPack();
		mp.put("b", new DecimalValue(2));
		mp.put("a", new TextValue("one"));
		mp.put("list", listOf(new DecimalValue(1), new DecimalValue(2)));
		mp.put("map", mapOf("x", new DecimalValue(1), "y", new DecimalValue(2)));
		mp.put("nul", new NullValue());
		v.add(new Object[] { "map_pack", mp });
		v.add(new Object[] { "map_pack_empty", new MapPack() });

		XLogPack x = new XLogPack();
		x.endTime = 1760486400123L;
		x.objHash = -12345;
		x.service = 987654321;
		x.txid = Long.MIN_VALUE;
		x.caller = 9;
		x.gxid = 10;
		x.elapsed = 1500;
		x.error = -77;
		v.add(new Object[] { "xlog", x });
		XLogPack x0 = new XLogPack();
		v.add(new Object[] { "xlog_zero", x0 });

		DroppedXLogPack d = new DroppedXLogPack();
		d.gxid = -5;
		d.txid = 1234567890123L;
		v.add(new Object[] { "dropped_xlog", d });

		XLogProfilePack p1 = new XLogProfilePack();
		p1.time = 1760486400000L;
		p1.objHash = 123;
		p1.service = 456;
		p1.txid = 789;
		p1.profile = new byte[] { 1, 2, 3, 4, 5 };
		v.add(new Object[] { "profile1", p1 });
		XLogProfilePack2 p2 = new XLogProfilePack2();
		p2.time = 1760486400999L;
		p2.objHash = -1;
		p2.service = 2;
		p2.txid = 3;
		p2.gxid = -4;
		p2.xType = 1;
		p2.discardType = 2;
		p2.profile = new byte[0];
		v.add(new Object[] { "profile2", p2 });

		SpanPack sp = new SpanPack();
		sp.gxid = 11;
		sp.txid = 12;
		sp.caller = -13;
		sp.timestamp = 1760486400000L;
		sp.elapsed = 14;
		sp.spanType = 2;
		sp.name = -15;
		sp.objHash = 16;
		sp.error = 0;
		sp.localEndpointServiceName = 17;
		sp.localEndpointIp = new byte[] { 10, 0, 0, 1 };
		sp.localEndpointPort = 8080;
		sp.remoteEndpointServiceName = 18;
		sp.remoteEndpointIp = new byte[0];
		sp.remoteEndpointPort = -1;
		sp.debug = true;
		sp.shared = false;
		sp.annotationTimestamps = listOf(new DecimalValue(1), new DecimalValue(2));
		sp.annotationValues = listOf(new TextValue("a1"), new TextValue("a2"));
		sp.tags = mapOf("http.method", new TextValue("GET"), "http.status", new TextValue("200"));
		v.add(new Object[] { "span", sp });
		SpanPack sp6 = new SpanPack();
		sp6.localEndpointIp = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
		sp6.remoteEndpointIp = new byte[] { 1, 2, 3 };
		sp6.annotationTimestamps = new ListValue();
		sp6.annotationValues = new ListValue();
		sp6.tags = new MapValue();
		v.add(new Object[] { "span_ipv6", sp6 });

		SpanContainerPack sc = new SpanContainerPack();
		sc.gxid = -999;
		sc.spanCount = 33;
		sc.timestamp = 1760486400000L;
		sc.spans = new byte[0];
		v.add(new Object[] { "span_container", sc });

		v.add(new Object[] { "text_service", text("service", 0x12345, "/synthetic/service.do") });
		v.add(new Object[] { "text_sql", text("sql", -0x2468, "select col_a from synthetic_table where col_b = ?") });
		v.add(new Object[] { "text_sql_unicode", text("sql", 7, "update synthetic_t set c = '한글' where id = ?") });
		v.add(new Object[] { "text_login", text("login", 8, "synthetic-login-id") });
		v.add(new Object[] { "text_error", text("error", 9, "java.lang.IllegalStateException: synthetic") });
		v.add(new Object[] { "text_empty", text("", 0, "") });

		byte[] tts = { 0, 1, 2, 3, 4, 5, 6, 7, -1 };
		for (byte tt : tts) {
			PerfCounterPack pc = new PerfCounterPack();
			pc.time = 1760486405000L;
			pc.objName = "/synthetic-host/tomcat1";
			pc.timetype = tt;
			pc.data = mapOf("TPS", new FloatValue(12.5f), "ActiveService", new DecimalValue(3), "ElapsedTime",
					new DoubleValue(123.456), "objHash", new DecimalValue(-1), "time", new DecimalValue(1760486405000L));
			v.add(new Object[] { "perf_counter_tt" + tt, pc });
		}

		InteractionPerfCounterPack ip = new InteractionPerfCounterPack();
		ip.time = 1760486400000L;
		ip.objName = "/synthetic-host/app";
		ip.interactionType = "INTR_API_OUTGOING";
		ip.fromHash = 1;
		ip.toHash = -2;
		ip.period = 30;
		ip.count = 4;
		ip.errorCount = 5;
		ip.totalElapsed = 600;
		ip.customData = mapOf("k", new TextValue("v"));
		v.add(new Object[] { "interaction_counter", ip });

		AlertPack al = new AlertPack();
		al.time = 1760486400000L;
		al.level = 2;
		al.objType = "tomcat";
		al.objHash = 0x7fffffff;
		al.title = "SYNTHETIC_TITLE";
		al.message = "synthetic message";
		al.tags = mapOf("t", new DecimalValue(1));
		v.add(new Object[] { "alert", al });
		AlertPack al2 = new AlertPack();
		al2.level = (byte) 255;
		al2.objType = "";
		al2.title = "";
		al2.message = "";
		v.add(new Object[] { "alert_neg_level", al2 });

		ObjectPack o1 = new ObjectPack();
		o1.objType = "tomcat";
		o1.objHash = 42;
		o1.objName = "/synthetic-host/tomcat1";
		o1.address = "192.0.2.1";
		o1.version = "2.21.3";
		o1.alive = true;
		o1.wakeup = 1760486400120L;
		o1.tags = mapOf("detectedType", new TextValue("tomcat"));
		v.add(new Object[] { "object_full", o1 });
		ObjectPack o2 = new ObjectPack();
		o2.objType = "host";
		o2.objHash = -42;
		o2.objName = "/synthetic-host";
		o2.address = "";
		o2.version = "";
		o2.alive = false;
		o2.wakeup = 0;
		v.add(new Object[] { "object_min", o2 });
		ObjectPack o3 = new ObjectPack();
		o3.objType = "host";
		o3.objName = "/h";
		o3.address = "";
		o3.version = "";
		o3.wakeup = 1760486400000L;
		v.add(new Object[] { "object_wakeup_zero_ms", o3 });
		ObjectPack o4 = new ObjectPack();
		o4.objType = "host";
		o4.objName = "/h";
		o4.address = "";
		o4.version = "";
		o4.wakeup = 1760486400005L;
		v.add(new Object[] { "object_wakeup_5ms", o4 });

		StatusPack st = new StatusPack();
		st.time = 1760486400000L;
		st.objType = "tomcat";
		st.objHash = 1;
		st.key = "thread";
		st.data = mapOf("a", new DecimalValue(1), "b", listOf(new TextValue("x")));
		v.add(new Object[] { "status", st });

		StackPack sk = new StackPack();
		sk.time = 1760486400000L;
		sk.objHash = 99;
		sk.data = new byte[] { 9, 9, 9 };
		v.add(new Object[] { "stack", sk });

		SummaryPack su = new SummaryPack();
		su.time = 1760486700000L;
		su.objHash = 5;
		su.objType = "tomcat";
		su.stype = 3;
		su.table = mapOf("id", listOf(new DecimalValue(1)), "count", listOf(new DecimalValue(2)));
		v.add(new Object[] { "summary", su });

		BatchPack bp = new BatchPack();
		bp.startTime = 1760486400000L;
		bp.objHash = 7;
		bp.batchJobId = "synthetic-job";
		bp.args = "";
		bp.pID = 4321;
		bp.elapsedTime = 1000;
		bp.objName = "/synthetic-host/batch";
		bp.objType = "batch";
		v.add(new Object[] { "batch", bp });

		XLogPack before = new XLogPack();
		before.endTime = 1000L;
		v.add(new Object[] { "xlog_before_base", before });
		return v;
	}

	static List<Object[]> profileDebug() {
		List<Object[]> v = new ArrayList<Object[]>();
		XLogProfilePack p1 = new XLogProfilePack();
		p1.time = 1760486400000L;
		p1.objHash = 123;
		p1.service = -456;
		p1.txid = 789;
		p1.profile = new byte[] { 1, 2 };
		v.add(new Object[] { "profile1_svc", p1, "/synthetic/service.do" });
		v.add(new Object[] { "profile1_null_svc", p1, null });
		byte[] xt = { 0, 1, 2, 3, 4, 5 };
		for (byte t : xt) {
			XLogProfilePack2 p2 = new XLogProfilePack2();
			p2.time = 1760486400000L;
			p2.objHash = 1;
			p2.service = 2;
			p2.txid = 3;
			p2.gxid = t % 2 == 0 ? 3 : (t == 5 ? 0 : 4);
			p2.xType = t;
			p2.discardType = (byte) (t == 4 ? -1 : t);
			p2.profile = new byte[t];
			v.add(new Object[] { "profile2_xtype" + t, p2, "/svc" + t });
		}
		return v;
	}

	static TextPack text(String type, int hash, String text) {
		TextPack t = new TextPack();
		t.xtype = type;
		t.hash = hash;
		t.text = text;
		return t;
	}

	static String hex(byte[] b) {
		StringBuilder sb = new StringBuilder();
		for (byte x : b)
			sb.append(String.format("%02x", x & 0xff));
		return sb.toString();
	}

	static String qn(String s) {
		return s == null ? "null" : q(s);
	}

	static String q(String s) {
		StringBuilder sb = new StringBuilder("\"");
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			switch (c) {
			case '"':
				sb.append("\\\"");
				break;
			case '\\':
				sb.append("\\\\");
				break;
			case '\n':
				sb.append("\\n");
				break;
			case '\r':
				sb.append("\\r");
				break;
			case '\t':
				sb.append("\\t");
				break;
			default:
				if (c < 0x20 || c > 0x7e)
					sb.append(String.format("\\u%04x", (int) c));
				else
					sb.append(c);
			}
		}
		return sb.append('"').toString();
	}

	static void write(File f, String s) throws Exception {
		Writer w = new OutputStreamWriter(new FileOutputStream(f), "UTF-8");
		try {
			w.write(s);
		} finally {
			w.close();
		}
	}
}
