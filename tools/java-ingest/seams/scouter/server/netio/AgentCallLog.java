// Oracle recorder for the AgentCall seam: every call with the encoded parameter, and an optional gate
// that blocks calls (to keep the ObjectCpuChecker daemon busy while its queue saturates).
package scouter.server.netio;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import scouter.io.DataOutputX;
import scouter.lang.pack.MapPack;
import scouter.lang.pack.ObjectPack;
public final class AgentCallLog {
    public static final class Call {
        public final int objHash; public final String cmd; public final byte[] param;
        Call(int objHash, String cmd, byte[] param) { this.objHash = objHash; this.cmd = cmd; this.param = param; }
    }
    private static final List<Call> calls = new ArrayList<Call>();
    public static volatile CountDownLatch gate;
    public static final AtomicInteger waiting = new AtomicInteger();
    public static final AtomicInteger started = new AtomicInteger();
    public static void record(ObjectPack o, String cmd, MapPack param) {
        started.incrementAndGet();
        byte[] b;
        try { b = new DataOutputX().writePack(param).toByteArray(); } catch (Exception e) { throw new RuntimeException(e); }
        CountDownLatch g = gate;
        if (g != null) {
            waiting.incrementAndGet();
            try { g.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            waiting.decrementAndGet();
        }
        synchronized (calls) { calls.add(new Call(o.objHash, cmd, b)); }
    }
    public static List<Call> take() { synchronized (calls) { List<Call> out = new ArrayList<Call>(calls); calls.clear(); return out; } }
    public static int size() { synchronized (calls) { return calls.size(); } }
}
