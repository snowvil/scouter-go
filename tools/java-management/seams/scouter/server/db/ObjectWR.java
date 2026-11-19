// Seam: the original ObjectWR queues add/remove for an async writer thread, so a remove never changes
// what ObjectRD.getObjectList returns inside the same handler call. The seam only records the calls.
package scouter.server.db;
import java.util.ArrayList;
import java.util.List;
public final class ObjectWR {
    public static final List<Integer> removed = new ArrayList<Integer>();
    public static final List<scouter.lang.pack.ObjectPack> added = new ArrayList<scouter.lang.pack.ObjectPack>();
    public static void add(scouter.lang.pack.ObjectPack p) { added.add(p); }
    public static void remove(int objHash) { removed.add(objHash); }
    public static String getDBPath(String date) { return "./database/" + date + "/object"; }
}
