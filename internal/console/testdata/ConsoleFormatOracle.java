// Console formatting oracle for the -console port (internal/console).
// It calls the pinned Scouter helpers the term handlers use (FormatUtil.print,
// StringUtil.leftPad/rpad, Hexa32.toString32) and the JDK conversions they rely
// on (Double.toString through string concatenation, Integer.toString(v, 16)).
//
// Generate with JDK 8 and JDK 17 against the pinned scouter.common source and
// with a fixed locale, then compare (see internal/console/CONTRACT.md):
//
//   javac -encoding UTF-8 -source 8 -target 8 -sourcepath <scouter>/scouter.common/src/main/java -d out ConsoleFormatOracle.java
//   java -Duser.language=en -Duser.country=US -cp out ConsoleFormatOracle > format-jdk8.golden
//
// Output: one line per case, "kind<TAB>input<TAB>output". Doubles and floats
// are given as raw IEEE bits in hex so the Go side reads the exact value.
import scouter.lang.value.DecimalValue;
import scouter.lang.value.DoubleValue;
import scouter.lang.value.FloatValue;
import scouter.util.FormatUtil;
import scouter.util.Hexa32;
import scouter.util.StringUtil;

public class ConsoleFormatOracle {
	static final double[] DOUBLES = {
		0.0, -0.0, 0.5, 1.5, 2.5, 3.5, -0.5, -1.5, -2.5, 0.49999999999999994, 0.5000000000000001,
		1234.5, 1235.5, 999999.5, 1000000.5, 12345678.9, 2.675, 0.05, 0.15, 0.25, 0.35, 0.45, 1.25, 1.35,
		-0.05, -0.04, -0.25, 9.95, 99.95, 999.95, 1e15 + 0.5, 4503599627370495.5, 9007199254740993.0,
		1e20, 1.7976931348623157e308, 4.9e-324, 123456789012.345, -987654321.5,
		Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
	};
	static final float[] FLOATS = {
		0f, 0.1f, 0.5f, 1.45f, 1.5f, 2.5f, 2.675f, 16777217f, 123456.78f, 3.4028235e38f, 1e-7f, 0.05f, 0.25f,
	};
	static final long[] LONGS = {
		0L, 1L, -1L, 999L, 1000L, -1000L, 1234567L, -1234567L, Integer.MAX_VALUE, Integer.MIN_VALUE,
		Long.MAX_VALUE, Long.MIN_VALUE, 9007199254740993L,
	};
	// valueCount sums (float widened to double) and a few plain doubles.
	static final double[] TOSTRING = {
		0.0, 1.0, 12.0, 100.0, 0.001, 0.0001, 9999999.0, 10000000.0, 12345678.0, 1.0e21, 1.0e-5,
		(double) 0.1f, (double) 1.45f, (double) 123456.78f, (double) 16777216f, (double) 3.4028235e38f,
		2.0e-3, 1.0e23, 2.0e23, 5.0e-324, 1.7976931348623157e308, 0.30000000000000004, -0.0, -12.5,
		Double.NaN, Double.POSITIVE_INFINITY,
	};
	static final int[] HEX = { 0, 1, 15, 255, -1, -255, Integer.MAX_VALUE, Integer.MIN_VALUE, 0x7f3a19c2 };
	static final long[] HEXA32 = { 0L, 9L, 10L, 31L, 32L, -1L, -10L, 1234567890123L, -1234567890123L, Long.MAX_VALUE, Long.MIN_VALUE };
	static final String[] PADS = { "", "a", "abcde", "abcdef", "한글", "😀x" };

	static String bits(double d) {
		return Long.toHexString(Double.doubleToRawLongBits(d));
	}

	static String bits(float f) {
		return Integer.toHexString(Float.floatToRawIntBits(f));
	}

	static String hex(String s) {
		StringBuilder sb = new StringBuilder();
		try {
			for (byte b : s.getBytes("UTF-8")) {
				sb.append(String.format("%02x", b & 0xff));
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return sb.length() == 0 ? "-" : sb.toString();
	}

	static void out(String kind, String in, String result) {
		System.out.println(kind + "\t" + in + "\t" + result);
	}

	public static void main(String[] args) {
		for (double d : DOUBLES) {
			out("double#,##0", bits(d), FormatUtil.print(d, "#,##0"));
			out("double#,##0.0", bits(d), FormatUtil.print(d, "#,##0.0"));
			out("DoubleValue#,##0", bits(d), FormatUtil.print(new DoubleValue(d), "#,##0"));
		}
		for (float f : FLOATS) {
			out("float#,##0", bits(f), FormatUtil.print(f, "#,##0"));
			out("float#,##0.0", bits(f), FormatUtil.print(f, "#,##0.0"));
			out("FloatValue#,##0", bits(f), FormatUtil.print(new FloatValue(f), "#,##0"));
		}
		for (long l : LONGS) {
			out("long#,##0", Long.toString(l), FormatUtil.print(l, "#,##0"));
			out("DecimalValue#,##0", Long.toString(l), FormatUtil.print(new DecimalValue(l), "#,##0"));
		}
		for (double d : TOSTRING) {
			out("Double.toString", bits(d), "" + d);
		}
		for (int i : HEX) {
			out("Integer.toString16", Integer.toString(i), Integer.toString(i, 16));
		}
		for (long l : HEXA32) {
			out("Hexa32", Long.toString(l), Hexa32.toString32(l));
		}
		for (String s : PADS) {
			out("leftPad5", hex(s), "[" + StringUtil.leftPad(s, 5) + "]");
			out("rpad5", hex(s), "[" + StringUtil.rpad(s, 5) + "]");
		}
	}
}
