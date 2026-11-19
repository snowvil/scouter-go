// Test-only agent request handler for scripts/java-agent-interop.py.
//
// The original agent (ReqestHandlingProxy.load) also scans the package named
// by -Dscouter.handler and registers every public @RequestHandler method; the
// class names are visited in sorted order, so this "zzinterop" handler is
// registered after the built-in AgentClassHandle and replaces its
// OBJECT_CHECK_RESOURCE_FILE handler (the agent logs A129 "duplicated Handler
// key"). The jar is appended with -Xbootclasspath/a because the agent classes
// live on the boot class path (manifest Boot-Class-Path).
//
// OBJECT_CHECK_RESOURCE_FILE is a single-target Pack relay with the general
// Go relay limits (8MiB, so_timeout+RequestTimeout). The handler makes a real
// agent TcpWorker answer slowly, answer with a large Pack, or lose its socket
// in the middle of a request, all driven by the request MapPack:
//   interopMode=sleep  sleepMs=<n>   answer after n ms
//   interopMode=big    bytes=<n>     answer with a BlobValue of n bytes
//   interopMode=close                close the agent socket, then return
// Each call is appended to the file named by -Dinterop.hookLog.
package zzinterop;

import scouter.agent.netio.request.anotation.RequestHandler;
import scouter.io.DataInputX;
import scouter.io.DataOutputX;
import scouter.lang.pack.MapPack;
import scouter.lang.pack.Pack;
import scouter.lang.value.BlobValue;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class InteropHooks {
    private static final AtomicInteger CALLS = new AtomicInteger();

    @RequestHandler("OBJECT_CHECK_RESOURCE_FILE")
    public Pack hook(Pack param, DataInputX in, DataOutputX out) throws Exception {
        MapPack request = (MapPack) param;
        int call = CALLS.incrementAndGet();
        String mode = request.getText("interopMode");
        String tag = request.getText("interopTag");
        log(call, mode, tag, "start");
        MapPack answer = new MapPack();
        answer.put("interopCall", call);
        answer.put("interopTag", tag);
        if ("sleep".equals(mode)) {
            Thread.sleep(request.getLong("sleepMs"));
        } else if ("big".equals(mode)) {
            answer.put("blob", new BlobValue(new byte[request.getInt("bytes")]));
        } else if ("close".equals(mode)) {
            out.close();
        }
        log(call, mode, tag, "end");
        return answer;
    }

    private static synchronized void log(int call, String mode, String tag, String phase) {
        String path = System.getProperty("interop.hookLog");
        if (path == null) return;
        try (FileWriter w = new FileWriter(path, true)) {
            w.write(System.currentTimeMillis() + " " + call + " " + mode + " " + tag + " " + phase + "\n");
        } catch (IOException ignored) {
        }
    }
}
