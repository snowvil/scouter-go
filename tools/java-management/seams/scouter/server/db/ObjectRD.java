// Seam: daily object DB reader. The original decodes a fresh ObjectPack from bytes on every call,
// so tag mutations by a handler never leak into the next call; the seam round-trips through the codec too.
package scouter.server.db;
import java.util.ArrayList;
import java.util.List;
import scouter.io.DataInputX;
import scouter.io.DataOutputX;
import scouter.lang.pack.MapPack;
import scouter.lang.pack.ObjectPack;
import scouter.lang.value.ListValue;
public final class ObjectRD {
    private static final List<byte[]> daily = new ArrayList<byte[]>();
    public static void seed(ObjectPack p) { try { daily.add(new DataOutputX().writePack(p).toByteArray()); } catch (Exception e) { throw new RuntimeException(e); } }
    public static void reset() { daily.clear(); }
    public static List<ObjectPack> getObjectList(String date) {
        List<ObjectPack> list = new ArrayList<ObjectPack>();
        try { for (byte[] b : daily) list.add((ObjectPack) new DataInputX(b).readPack()); } catch (Exception e) { throw new RuntimeException(e); }
        return list;
    }
    public static ObjectPack getObjectPack(String date, int hash) { for (ObjectPack p : getObjectList(date)) if (p.objHash == hash) return p; return null; }
    public static String getObjName(String date, int objHash) { ObjectPack p = getObjectPack(date, objHash); return p == null ? null : p.objName; }
    public static MapPack getDailyAgent(String date) {
        MapPack m = new MapPack(); ListValue t = m.newList("objType"), h = m.newList("objHash"), n = m.newList("objName");
        for (ObjectPack p : getObjectList(date)) { t.add(p.objType); h.add(p.objHash); n.add(p.objName); }
        return m;
    }
}
