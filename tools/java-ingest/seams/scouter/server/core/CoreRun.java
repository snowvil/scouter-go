// Seam: CoreRun.scala:6-18 (daemon loop flag). ObjectCpuChecker's dump daemon loops while it is true.
package scouter.server.core;
public final class CoreRun { public static boolean running = true; public static final int MAX_QUE_SIZE = 10000; }
