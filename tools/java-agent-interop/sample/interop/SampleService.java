package interop;

// The traced "service". Worker 0 makes every 5th call slow (an active service
// for OBJECT_ACTIVE_SERVICE_LIST); every 7th call of any worker fails.
public final class SampleService {
    public static String handle(int worker, long seq) {
        String a = stepLoad(seq);
        String b = stepCompute(a);
        if (worker == 0 && seq % 5 == 0) {
            stepWait(2500);
        }
        if (seq % 7 == 0) {
            throw new IllegalStateException("interop synthetic error");
        }
        return b;
    }

    public static String stepLoad(long seq) {
        SampleApp.sleep(5);
        return "item-" + seq;
    }

    public static String stepCompute(String in) {
        StringBuilder b = new StringBuilder(in);
        for (int i = 0; i < 100; i++) b.append(i % 10);
        return b.toString();
    }

    public static void stepWait(long ms) {
        SampleApp.sleep(ms);
    }
}
