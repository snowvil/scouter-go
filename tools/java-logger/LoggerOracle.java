import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scouter.server.ConfObserver;
import scouter.server.Configure;
import scouter.server.Logger;
import scouter.server.LoginManager;
import scouter.server.logs.RequestLogger;
import scouter.util.DateUtil;
import scouter.util.StringSet;

/**
 * Runs the pinned, unmodified scouter.server.Logger (Scala object) and
 * scouter.server.logs.RequestLogger with a Configure/LoginManager seam and records the
 * observable file names, line formats and deletion decisions. Wall-clock parts are
 * normalized: a datetime prefix becomes "<datetime>", a timestamp prefix "<timestamp>",
 * today's yyyyMMdd in a file name "<today>", and day offsets are written as "<today-N>".
 *
 * Usage: java LoggerOracle <outdir> <workdir>
 */
public class LoggerOracle {
	static final Pattern DATETIME = Pattern.compile("^(\\d{8}) \\d{2}:\\d{2}:\\d{2} ");
	static final Pattern TIMESTAMP = Pattern.compile("^(\\d{8}) \\d{2}:\\d{2}:\\d{2}\\.\\d{3} ");
	static final Charset UTF8 = Charset.forName("UTF-8");

	static File work;
	static String today;
	static StringBuilder json = new StringBuilder();
	static boolean firstSection = true;

	public static void main(String[] args) throws Exception {
		File out = new File(args[0]);
		work = new File(args[1]).getCanonicalFile();
		ByteArrayOutputStream stdout = new ByteArrayOutputStream();
		PrintStream realOut = System.out;
		System.setOut(new PrintStream(stdout, true, "UTF-8"));

		Configure c = Configure.getInstance();
		c.mgr_log_ignore_ids = new StringSet();
		c.mgr_log_ignore_ids.put("S999");
		today = DateUtil.yyyymmdd();
		json.append("{\n");

		// 0. warm-up. The first println initializes object Logger, which starts its
		// daemon thread. The daemon's first iteration sees lastDataUnit (0 or the value
		// openFile just set) and may close pw and set it to null while the main thread
		// is writing; that line then goes to System.out or the write hits a null pw
		// (original start-up race, CONTRACT.md D-L4). Log once into a throw-away dir,
		// wait until the daemon has finished its first iteration (it then sleeps 5 s in
		// ThreadUtil.sleep), discard the captured stdout and only then switch to dirA.
		c.log_dir = new File(work, "logs-warmup").getPath();
		Logger.println("warm-up");
		awaitLoggerDaemonSleeping();
		stdout.reset();
		File dirA = new File(work, "logs-a");
		c.log_dir = dirA.getPath();
		ConfObserver.exec();

		// 1. line formats, ID filter, rate limit, trace, rotation=true file name
		Logger.println("plain message");
		Logger.println("S100", "id message");
		Logger.println("S999", "ignored by mgr_log_ignore_ids");
		Logger.println("S999", 10, "ignored with rate");
		Logger.println("S101", 10, "rate first");
		Logger.println("S101", 10, "rate second suppressed");
		Logger.println("S102", 0, "sec zero first");
		Logger.println("S102", 0, "sec zero second");
		Logger.println("S103", 10, "with throwable", new IllegalStateException("synthetic failure"));
		Logger.trace("trace while _trace=false");
		c._trace = true;
		Logger.trace("trace while _trace=true");
		Logger.println("S104", "copied to stdout while _trace=true");
		c._trace = false;
		Logger.println("multi\nline message");
		Logger.println("");
		section("formats", dirA, stdout);

		// 2. rate-limit map: StringLongLinkedMap.setMax(1000) evicts the oldest id
		File dirR = new File(work, "logs-rate");
		c.log_dir = dirR.getPath();
		ConfObserver.exec();
		for (int i = 0; i <= 1000; i++)
			Logger.println("R" + i, 100, "first R" + i);
		Logger.println("R0", 100, "second R0 after eviction");
		Logger.println("R1", 100, "second R1 still limited");
		Logger.println("R1000", 100, "second R1000 still limited");
		List<String> rate = new ArrayList<String>();
		for (String line : readLines(new File(dirR, "server-" + today + ".log")))
			if (line.contains("second"))
				rate.add(normalize(line));
		field("rate_limit_after_1001_ids", list(rate));

		// 3. rotation=false: server.log, truncated on every open (D-L1)
		File dirB = new File(work, "logs-b");
		c.log_dir = dirB.getPath();
		c.log_rotation_enabled = false;
		ConfObserver.exec();
		Logger.println("rotation off first open");
		List<String> first = normLines(new File(dirB, "server.log"));
		c.log_rotation_enabled = true;
		ConfObserver.exec();
		c.log_rotation_enabled = false;
		ConfObserver.exec();
		Logger.println("rotation off second open");
		List<String> second = normLines(new File(dirB, "server.log"));
		field("rotation_off_files", list(names(dirB)));
		field("rotation_off_first_open", list(first));
		field("rotation_off_after_reopen", list(second));

		// 4. log_dir change through ConfObserver
		File dirC = new File(work, "logs-c");
		c.log_dir = dirC.getPath();
		c.log_rotation_enabled = true;
		ConfObserver.exec();
		Logger.println("after log_dir change");
		field("dir_change_files", list(names(dirC)));

		// 5. clearOldLog decisions (keep_days=3, rotation on), then keep_days<=0 and rotation off
		File dirD = new File(work, "logs-d");
		c.log_dir = dirD.getPath();
		ConfObserver.exec();
		c.log_keep_days = 3;
		String[] templates = clearTemplates();
		createAll(dirD, templates);
		new File(dirD, "server-" + DateUtil.yyyymmdd(System.currentTimeMillis() - 30 * DateUtil.MILLIS_PER_DAY) + ".log.d").mkdirs();
		clearOldLog();
		field("clear_keep3_remaining", list(templateNames(dirD)));
		List<String> deleteLines = new ArrayList<String>();
		for (String line : normLines(new File(dirD, "server-" + today + ".log")))
			if (line.contains("delete log file")) {
				String l = line.replace(dirD.getCanonicalPath(), "<log_dir>").replace(dirD.getAbsolutePath(), "<log_dir>");
				int slash = l.lastIndexOf('/');
				deleteLines.add(l.substring(0, slash + 1) + toTemplate(l.substring(slash + 1)));
			}
		// listFiles() order is file-system dependent; the decision set is the contract.
		java.util.Collections.sort(deleteLines);
		field("clear_keep3_log_lines", list(deleteLines));

		File dirE = new File(work, "logs-e");
		c.log_dir = dirE.getPath();
		ConfObserver.exec();
		createAll(dirE, templates);
		c.log_keep_days = 0;
		clearOldLog();
		field("clear_keep0_remaining", list(templateNames(dirE)));
		c.log_keep_days = 3;
		c.log_rotation_enabled = false;
		clearOldLog();
		field("clear_rotation_off_remaining", list(templateNames(dirE)));
		c.log_rotation_enabled = true;

		// 6. RequestLogger: static DIRECTORY is read once at class init
		File dirQ = new File(work, "logs-q");
		c.log_dir = dirQ.getPath();
		LoginManager.register(77L, "synthetic-user", "192.0.2.10");
		RequestLogger rl = RequestLogger.getInstance();
		List<String> results = new ArrayList<String>();
		results.add("SERVER_DB_DELETE known=" + rl.add("SERVER_DB_DELETE", 77L));
		results.add("REMOTE_CONTROL unknown=" + rl.add("REMOTE_CONTROL", 999L));
		results.add("REMOTE_CONTROL_ALL known=" + rl.add("REMOTE_CONTROL_ALL", 77L));
		results.add("TRANX_LOAD_TIME_GROUP known=" + rl.add("TRANX_LOAD_TIME_GROUP", 77L));
		results.add("TRANX_LOAD_TIME_GROUP_V2 known=" + rl.add("TRANX_LOAD_TIME_GROUP_V2", 77L));
		results.add("OBJECT_LIST_REAL_TIME known=" + rl.add("OBJECT_LIST_REAL_TIME", 77L));
		rl.registerCmd("OBJECT_RESET_CACHE");
		rl.registerCmd("OBJECT_RESET_CACHE");
		rl.registerCmd("SERVER_DB_DELETE");
		rl.registerCmd("ACTIVE_QUERY_LIST");
		results.add("OBJECT_RESET_CACHE known=" + rl.add("OBJECT_RESET_CACHE", 77L));
		results.add("ACTIVE_QUERY_LIST known=" + rl.add("ACTIVE_QUERY_LIST", 77L));
		// log_dir change after init does not move request-*.log (D-L2)
		c.log_dir = new File(work, "logs-q2").getPath();
		ConfObserver.exec();
		results.add("SERVER_DB_DELETE after log_dir change known=" + rl.add("SERVER_DB_DELETE", 77L));
		field("request_add_results", list(results));
		File req = new File(dirQ, "request-" + today + ".log");
		long deadline = System.currentTimeMillis() + 10000;
		// 7 lines: 4 known/unknown adds, 1 register, OBJECT_RESET_CACHE, SERVER_DB_DELETE after the
		// log_dir change (still the init dir, D-L2). Commands outside the set write nothing.
		final int expectedRequestLines = 7;
		while (System.currentTimeMillis() < deadline && (!req.exists() || readLines(req).size() < expectedRequestLines))
			Thread.sleep(20);
		List<String> reqLines = new ArrayList<String>();
		for (String line : readLines(req))
			reqLines.add(normalizeTimestamp(line));
		field("request_files", list(names(dirQ)));
		field("request_lines", list(reqLines));
		field("request_files_new_dir", list(names(new File(work, "logs-q2"))));

		json.append("\n}\n");
		System.setOut(realOut);
		Writer w = new OutputStreamWriter(new FileOutputStream(new File(out, "logger.json")), UTF8);
		try {
			w.write(json.toString());
		} finally {
			w.close();
		}
		System.out.println("PASS LoggerOracle");
		System.exit(0);
	}

	/** Waits (at most 10 s) until the "scouter.server.Logger" daemon sleeps after its first iteration. */
	static void awaitLoggerDaemonSleeping() throws Exception {
		long deadline = System.currentTimeMillis() + 10000;
		while (System.currentTimeMillis() < deadline) {
			for (Thread t : Thread.getAllStackTraces().keySet())
				if ("scouter.server.Logger".equals(t.getName()) && t.getState() == Thread.State.TIMED_WAITING)
					return;
			Thread.sleep(5);
		}
		throw new IllegalStateException("Logger daemon did not reach its first sleep within 10 s");
	}

	static void clearOldLog() throws Exception {
		Class<?> k = Class.forName("scouter.server.Logger$");
		Object module = k.getField("MODULE$").get(null);
		Method m = k.getDeclaredMethod("clearOldLog");
		m.setAccessible(true);
		m.invoke(module);
	}

	/** Day offsets relative to today; "<today-N>" is replaced with yyyyMMdd of now - N days. */
	static String[] clearTemplates() {
		return new String[] { "server-<today>.log", "server-<today-3>.log", "server-<today-4>.log", "server-<today-30>.log",
				"request-<today-3>.log", "request-<today-4>.log", "request-<today-30>.txt", "server-<today-30>.log.1",
				"server-<today-30>", "other-<today-30>.log", "server-19991231.log", "server-21000101.log",
				"server-20261340.log", "server-abcdefgh.log", "server-+0201015.log", "server-1.log",
				"request-<today+1>.log", "Server-<today-30>.log", "server-<today-30>.gz.log" };
	}

	static String expand(String t) {
		Matcher m = Pattern.compile("<today([+-]\\d+)?>").matcher(t);
		StringBuffer sb = new StringBuffer();
		while (m.find()) {
			int off = m.group(1) == null ? 0 : Integer.parseInt(m.group(1).replace("+", ""));
			m.appendReplacement(sb, DateUtil.yyyymmdd(System.currentTimeMillis() + off * DateUtil.MILLIS_PER_DAY));
		}
		m.appendTail(sb);
		return sb.toString();
	}

	static void createAll(File dir, String[] templates) throws Exception {
		dir.mkdirs();
		for (String t : templates) {
			File f = new File(dir, expand(t));
			if (!f.exists())
				new FileOutputStream(f).close();
		}
	}

	/** Remaining files mapped back to their templates (unknown names are kept verbatim). */
	static List<String> templateNames(File dir) {
		List<String> out = new ArrayList<String>();
		for (String n : names(dir))
			out.add(toTemplate(n));
		java.util.Collections.sort(out);
		return out;
	}

	static String toTemplate(String n) {
		String name = n.replace("<today>", today);
		for (String x : clearTemplates())
			if (expand(x).equals(name))
				return x;
		String dirTmpl = "server-" + DateUtil.yyyymmdd(System.currentTimeMillis() - 30 * DateUtil.MILLIS_PER_DAY) + ".log.d/";
		if (name.equals(dirTmpl))
			return "server-<today-30>.log.d/";
		return n;
	}

	static List<String> names(File dir) {
		String[] n = dir.list();
		if (n == null)
			return new ArrayList<String>();
		List<String> out = new ArrayList<String>();
		for (String x : n)
			out.add(new File(dir, x).isDirectory() ? x.replace(today, "<today>") + "/" : x.replace(today, "<today>"));
		java.util.Collections.sort(out);
		return out;
	}

	static void section(String name, File dir, ByteArrayOutputStream stdout) throws Exception {
		field(name + "_files", list(names(dir)));
		field(name + "_lines", list(normLines(new File(dir, "server-" + today + ".log"))));
		List<String> outLines = new ArrayList<String>();
		for (String line : new String(stdout.toByteArray(), UTF8).split("\n", -1))
			if (line.length() > 0)
				outLines.add(normalize(line));
		field(name + "_stdout", list(outLines));
		stdout.reset();
	}

	static List<String> normLines(File f) throws Exception {
		List<String> out = new ArrayList<String>();
		for (String line : readLines(f)) {
			if (line.startsWith("\t"))
				continue; // ThreadUtil.getStackTrace frames ("\t" + element) depend on oracle line numbers
			out.add(normalize(line));
		}
		return out;
	}

	static List<String> readLines(File f) throws Exception {
		if (!f.exists())
			return new ArrayList<String>();
		String s = new String(Files.readAllBytes(f.toPath()), UTF8);
		List<String> out = new ArrayList<String>(Arrays.asList(s.split("\n", -1)));
		if (!out.isEmpty() && out.get(out.size() - 1).isEmpty())
			out.remove(out.size() - 1);
		return out;
	}

	static String normalize(String line) {
		Matcher m = DATETIME.matcher(line);
		if (m.find()) {
			if (!m.group(1).equals(today))
				throw new IllegalStateException("date prefix is not today: " + line);
			return "<datetime> " + line.substring(m.end());
		}
		return line;
	}

	static String normalizeTimestamp(String line) {
		Matcher m = TIMESTAMP.matcher(line);
		if (m.find()) {
			if (!m.group(1).equals(today))
				throw new IllegalStateException("date prefix is not today: " + line);
			return "<timestamp> " + line.substring(m.end());
		}
		return line;
	}

	static void field(String name, String value) {
		json.append(firstSection ? "" : ",\n").append(PackStringOracle.q(name)).append(": ").append(value);
		firstSection = false;
	}

	static String list(List<String> v) {
		StringBuilder sb = new StringBuilder("[");
		for (int i = 0; i < v.size(); i++)
			sb.append(i == 0 ? "" : ", ").append(PackStringOracle.q(v.get(i)));
		return sb.append("]").toString();
	}
}
