// Java oracle for internal/config Properties parsing (testdata only).
// Usage: java PropertiesOracle <file>... > ../properties/expected.json
// Prints {file: {"entries": {key: value}, "error": msg|null}} with every
// non-ASCII or control char as a JSON u-escape so Go's encoding/json decodes it.
import java.io.*;
import java.util.*;

public class PropertiesOracle {
	static String q(String s) {
		StringBuilder b = new StringBuilder("\"");
		for (char c : s.toCharArray()) {
			if (c == '"' || c == '\\') b.append('\\').append(c);
			else if (c < 0x20 || c > 0x7e) b.append(String.format("\\u%04x", (int) c));
			else b.append(c);
		}
		return b.append('"').toString();
	}
	public static void main(String[] args) throws Exception {
		StringBuilder out = new StringBuilder("{\n");
		for (int f = 0; f < args.length; f++) {
			Properties p = new Properties();
			String err = null;
			try (InputStream in = new FileInputStream(args[f])) {
				p.load(in);
			} catch (IllegalArgumentException e) {
				err = e.getMessage();
			}
			out.append("  ").append(q(new File(args[f]).getName())).append(": {\"error\": ").append(err == null ? "null" : q(err)).append(", \"entries\": {");
			int i = 0;
			for (String k : new TreeSet<String>(p.stringPropertyNames())) {
				out.append(i++ == 0 ? "" : ", ").append(q(k)).append(": ").append(q(p.getProperty(k)));
			}
			out.append("}}").append(f + 1 < args.length ? ",\n" : "\n");
		}
		System.out.print(out.append("}\n"));
	}
}
