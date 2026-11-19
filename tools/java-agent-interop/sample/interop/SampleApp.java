// Synthetic application for scripts/java-agent-interop.py. It runs under the
// real scouter.agent.java (-javaagent) with
//   hook_service_patterns=interop.SampleService.handle
//   hook_method_patterns=interop.SampleService.*
// so every handle() call becomes an XLog with a method profile. No network,
// no files: the agent does all the I/O.
package interop;

import java.util.concurrent.CountDownLatch;

public final class SampleApp {
    public static void main(String[] args) throws Exception {
        int workers = Integer.getInteger("interop.workers", 3);
        int idle = Integer.getInteger("interop.idleThreads", 100);
        long intervalMs = Long.getLong("interop.intervalMs", 200L);
        final CountDownLatch never = new CountDownLatch(1);
        for (int i = 0; i < idle; i++) {
            Thread t = new Thread(() -> park(12, never), "interop-idle-" + i);
            t.setDaemon(true);
            t.start();
        }
        for (int w = 0; w < workers; w++) {
            final int worker = w;
            Thread t = new Thread(() -> {
                long seq = 0;
                while (true) {
                    seq++;
                    try {
                        SampleService.handle(worker, seq);
                    } catch (RuntimeException expected) {
                        // synthetic error transaction
                    }
                    sleep(intervalMs);
                }
            }, "interop-worker-" + w);
            t.setDaemon(true);
            t.start();
        }
        System.out.println("SAMPLE READY workers=" + workers + " idleThreads=" + idle);
        System.out.flush();
        // Exit when the driver closes stdin (or kills the process).
        while (System.in.read() >= 0) {
        }
    }

    static void park(int depth, CountDownLatch latch) {
        if (depth > 0) {
            park(depth - 1, latch);
            return;
        }
        try {
            latch.await();
        } catch (InterruptedException ignored) {
        }
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }
}
