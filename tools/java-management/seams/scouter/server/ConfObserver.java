// Seam: config change observers are not exercised; Configure.applyConfig calls exec().
package scouter.server;
public final class ConfObserver { public static void put(String name, Runnable r) {} public static void exec() {} }
