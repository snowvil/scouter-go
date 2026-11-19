package scouter.agent;

/** Seam for the original agent file logger: stderr only, with the original method signatures used by the send path. */
public class Logger {
    public static void println(Object message) {
        System.err.println(message);
    }

    public static void println(String id, Object message) {
        System.err.println(id + " " + message);
    }

    public static void println(String id, String message, Throwable t) {
        System.err.println(id + " " + message + " " + t);
    }

    public static void info(String message) {
        System.err.println(message);
    }
}
