// Seam: shutdown hook registry (DBCtr/RemoteControlManager register but never run it here).
package scouter.server;
public final class ShutdownManager { public static void add(scouter.util.IShutdown s) {} }
