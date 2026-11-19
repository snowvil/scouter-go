// Seam: original scouter.server.Logger writes rotating files under log_dir and depends on Configure.
// The oracle prints to stderr and keeps the ids it saw so cases can assert on log side effects.
package scouter.server;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
public final class Logger {
    public static final List<String> seen = Collections.synchronizedList(new ArrayList<String>());
    private static void out(String s) { seen.add(s); System.err.println("[oracle-log] " + s); }
    public static void trace(Object message) {}
    public static void println(Object message) { out(String.valueOf(message)); }
    public static void println(String id, Object message) { out("[" + id + "] " + message); }
    public static void println(String id, int sec, Object message) { out("[" + id + "] " + message); }
    public static void println(String id, int sec, Object message, Throwable t) { out("[" + id + "] " + message + " " + t); if (t != null) t.printStackTrace(); }
    public static void printStackTrace(Throwable t) { out("stack " + t); t.printStackTrace(); }
    public static void printStackTrace(String id, Throwable t) { out("[" + id + "] stack " + t); t.printStackTrace(); }
}
