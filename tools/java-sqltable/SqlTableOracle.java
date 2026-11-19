import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import scouter.util.SQLSimpleParser;

/**
 * Runs the pinned scouter.util.SQLSimpleParser exactly as SqlTables.parseTable does
 * (a new parser per SQL text, getCrudInfo) over synthetic SQL texts.
 *
 * stdin : one case per line, "name\tbase64(UTF-8 sql)".
 * stdout: first line "#java.specification.version\t<v>\tuser.language\t<l>", then one line per
 *         case, "name\tbase64(UTF-8 result)\t<Throwable class or empty>".
 * A Throwable escaping getCrudInfo (only an Error can: getCrudInfo catches Exception) is what
 * SqlTables.process turns into failSet + S112; the result column is then empty.
 */
public final class SqlTableOracle {
	public static void main(String[] args) throws Exception {
		Base64.Decoder dec = Base64.getDecoder();
		Base64.Encoder enc = Base64.getEncoder();
		PrintStream out = new PrintStream(System.out, false, "UTF-8");
		out.print("#java.specification.version\t" + System.getProperty("java.specification.version")
				+ "\tuser.language\t" + System.getProperty("user.language") + "\n");
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
		String line;
		while ((line = in.readLine()) != null) {
			if (line.isEmpty()) {
				continue;
			}
			int tab = line.indexOf('\t');
			String name = line.substring(0, tab);
			String sql = new String(dec.decode(line.substring(tab + 1)), StandardCharsets.UTF_8);
			String result = "";
			String thrown = "";
			try {
				result = new SQLSimpleParser().getCrudInfo(sql);
			} catch (Throwable t) {
				thrown = t.getClass().getName();
			}
			out.print(name + "\t" + enc.encodeToString(result.getBytes(StandardCharsets.UTF_8)) + "\t" + thrown + "\n");
		}
		out.flush();
	}
}
