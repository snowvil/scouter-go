// Seam: the original PlugInManager runs JVM plugin scripts (out of scope). AgentManager.active only calls active(p).
package scouter.server.plugin;
import scouter.lang.pack.ObjectPack;
public final class PlugInManager {
    public static void active(ObjectPack p) {}
}
