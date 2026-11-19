// Seam: the original CounterManager also loads counters.site.xml from Configure.CONF_DIR and writes it.
// ObjectCpuChecker and AgentManager call getInstance().getCounterEngine() (getObjectType, isPrimaryObject); the
// engine here is the original CounterEngine parsing the original bundled counters.xml resource.
package scouter.server;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import scouter.lang.counters.CounterEngine;
import scouter.lang.pack.ObjectPack;
public final class CounterManager {
    private static final CounterManager instance = new CounterManager();
    private final CounterEngine engine = new CounterEngine();
    private CounterManager() {
        try (InputStream in = CounterEngine.class.getResourceAsStream("/scouter/lang/counters/counters.xml")) {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            for (int n; (n = in.read(buf)) > 0; ) b.write(buf, 0, n);
            if (!engine.parse(b.toByteArray())) throw new IllegalStateException("counters.xml parse failed");
        } catch (Exception e) { throw new RuntimeException(e); }
    }
    public static CounterManager getInstance() { return instance; }
    public CounterEngine getCounterEngine() { return engine; }
    // AgentManager.active calls it first; counters.site.xml writing is outside this oracle.
    public boolean addObjectTypeIfNotExist(ObjectPack objectPack) { return false; }
}
