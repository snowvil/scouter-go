// Seam: the UDP/HTTP ingest entry point. CounterHandler passes CAFE-framed bytes, TelegrafInputHandler passes packs.
package scouter.server.netio.data;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import scouter.lang.pack.Pack;
public final class NetDataProcessor {
    public static final class Record { public final byte[] bytes; public final Pack pack; public final InetAddress addr; Record(byte[] b, Pack p, InetAddress a) { bytes = b; pack = p; addr = a; } }
    public static final List<Record> records = new ArrayList<Record>();
    public static synchronized void add(byte[] data, InetAddress addr) { records.add(new Record(data, null, addr)); }
    public static synchronized void add(Pack pack, InetAddress addr) { records.add(new Record(null, pack, addr)); }
    public static synchronized List<Record> drain() { List<Record> out = new ArrayList<Record>(records); records.clear(); return out; }
}
