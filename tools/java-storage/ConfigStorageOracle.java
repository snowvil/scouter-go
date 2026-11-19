// Executes the pinned original Scouter TextWR/TextRD/TextPermWR/TextPermRD,
// TextPermIndex/TextTable/XLogIndex/KeyValueStoreIndex and AutoDeleteScheduler
// (dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5, Apache-2.0) to show how the
// scouter.conf keys mgr_text_db_daily_*_enabled, *_index_*_mb and mgr_purge_*
// change the files on disk. Infrastructure seams only (scripts/java-storage.py
// compile_config): Configure fields, Logger, ConfObserver (keeps Runnables),
// DBCtr (root + running=false), CoreRun.running()=false, ZipkinSpanWR.SPAN_DIR.
// With running=false the writer threads end at once; this harness runs the
// body of TextPermWR's writer loop (hasKey -> data.write -> index.set) itself.
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import scouter.server.ConfObserver;
import scouter.server.Configure;
import scouter.server.db.TextPermWR;
import scouter.server.db.TextPermWR$;
import scouter.server.db.TextRD;
import scouter.server.db.TextWR;
import scouter.server.db.TextWR$;
import scouter.server.db.kv.KeyValueStoreIndex;
import scouter.server.db.text.TextPermData;
import scouter.server.db.text.TextPermIndex;
import scouter.server.db.text.TextTable;
import scouter.server.db.xlog.XLogIndex;
import scouter.util.DateUtil;

public class ConfigStorageOracle {
    public static void main(String[] args) throws Exception {
        String root = args[1];
        System.setProperty("oracle.root", root);
        switch (args[0]) {
            case "text": text(Paths.get(args[2])); break;
            case "sizes": sizes(root, args); break;
            case "read-sizes": readSizes(root); break;
            case "purge": purge(root, args); break;
            default: throw new IllegalArgumentException(args[0]);
        }
        System.exit(0); // TextWR keeps no live thread, but be explicit.
    }

    static void set(String key, String value) throws Exception {
        Field f = Configure.class.getField(key);
        if (f.getType() == boolean.class) f.setBoolean(Configure.getInstance(), value.equals("1") || value.equals("true"));
        else f.setInt(Configure.getInstance(), Integer.parseInt(value));
    }

    // Initializes the TextWR/TextPermWR objects and waits for their writer
    // threads, which exit at once with DBCtr.running=false (their final close()
    // must not race with the harness).
    static void startText() throws Exception {
        Object a = TextWR$.MODULE$, b = TextPermWR$.MODULE$;
        for (Thread t : Thread.getAllStackTraces().keySet())
            if (t.getName().startsWith("scouter.server.db.Text")) t.join(10000);
    }

    @SuppressWarnings("unchecked")
    static void drainPerm() throws Exception {
        scouter.util.RequestQueue<Object> q = (scouter.util.RequestQueue<Object>) (Object) TextPermWR$.MODULE$.queue();
        Object d;
        while ((d = q.getNoWait()) != null) {
            String div = (String) d.getClass().getMethod("div").invoke(d);
            int hash = (Integer) d.getClass().getMethod("hash").invoke(d);
            String text = (String) d.getClass().getMethod("text").invoke(d);
            scala.Tuple2<TextPermIndex, TextPermData> db = TextPermWR.open(div);
            if (!db._1().hasKey(hash)) {
                long pos = db._2().write(text.getBytes("UTF8"));
                db._1().set(hash, pos);
            }
        }
    }

    static void text(Path script) throws Exception {
        startText();
        Class<?> unitClass = Class.forName("scouter.server.db.TextDupCheck$TextUnit");
        Constructor<?> unit = unitClass.getConstructor(String.class, int.class);
        unit.setAccessible(true);
        Constructor<?> data = Class.forName("scouter.server.db.TextWR$Data").getConstructor(String.class, String.class, int.class, String.class, unitClass);
        for (String line : Files.readAllLines(script, StandardCharsets.UTF_8)) {
            String[] f = line.split(" ", 5);
            switch (f[0]) {
                case "daily":
                    set("mgr_text_db_daily_service_enabled", f[1]);
                    set("mgr_text_db_daily_api_enabled", f[2]);
                    set("mgr_text_db_daily_ua_enabled", f[3]);
                    break;
                case "put": {
                    int hash = Integer.parseInt(f[3]);
                    String text = f.length > 4 ? f[4] : "";
                    TextWR.process((TextWR.Data) data.newInstance(f[1], f[2], hash, text, unit.newInstance(f[1], hash)));
                    drainPerm();
                    break;
                }
                case "get":
                    System.out.println("get " + f[1] + " " + f[2] + " " + f[3] + "=" + TextRD.getString(f[1], f[2], Integer.parseInt(f[3])));
                    break;
                default:
                    throw new IllegalArgumentException(line);
            }
        }
        drainPerm();
        // TextPermIndex.closeAll does not close an index whose refrence count
        // is > 0 (every TextPermWR.open after the first raises it), so the
        // running server relies on FlushCtr's periodic flush for that header.
        // Run one such flush pass now instead of waiting for its 1s/4s timer.
        flushAll();
        TextWR.close();
        TextPermWR.close();
    }

    // One FlushCtr.process() pass: flush every registered dirty IFlushable.
    static void flushAll() throws Exception {
        Object ctr = scouter.io.FlushCtr.getInstance();
        Field tf = scouter.io.FlushCtr.class.getDeclaredField("table");
        tf.setAccessible(true);
        scouter.util.IntKeyMap<?> table = (scouter.util.IntKeyMap<?>) tf.get(ctr);
        synchronized (ctr) {
            java.util.Enumeration<?> en = table.values();
            while (en.hasMoreElements()) {
                Object item = en.nextElement();
                Field of = item.getClass().getDeclaredField("object");
                of.setAccessible(true);
                scouter.io.IFlushable f = (scouter.io.IFlushable) of.get(item);
                if (f.isDirty()) f.flush();
            }
        }
    }

    static final String[] DIVS = {"service", "apicall", "ua", "login", "desc", "hmsg", "sql"};
    static final String DATE = "20261013";

    // sizes <root> default service api ua login desc hmsg daily kv xlogid
    static void sizes(String root, String[] a) throws Exception {
        String[] keys = {"_mgr_text_db_index_default_mb", "_mgr_text_db_index_service_mb", "_mgr_text_db_index_api_mb", "_mgr_text_db_index_ua_mb", "_mgr_text_db_index_login_mb", "_mgr_text_db_index_desc_mb", "_mgr_text_db_index_hmsg_mb", "_mgr_text_db_daily_index_mb", "_mgr_kv_store_index_default_mb", "_mgr_xlog_id_index_mb"};
        for (int i = 0; i < keys.length; i++) set(keys[i], a[2 + i]);
        String perm = root + "/00000000/text/text_";
        new File(root + "/00000000/text").mkdirs();
        for (String div : DIVS) {
            TextPermData d = TextPermData.open(div, perm + div);
            TextPermIndex x = TextPermIndex.open(div, perm + div);
            x.set(1, d.write("x".getBytes("UTF8")));
        }
        TextPermIndex.closeAll();
        TextPermData.closeAll();
        new File(root + "/" + DATE + "/text").mkdirs();
        TextTable daily = TextTable.open(root + "/" + DATE + "/text/text");
        daily.set("error", 1, "x".getBytes("UTF8"));
        daily.close();
        new File(root + "/" + DATE + "/xlog").mkdirs();
        XLogIndex xi = new XLogIndex(root + "/" + DATE + "/xlog/xlog");
        xi.setByTxid(1, 0);
        xi.setByGxid(2, 0);
        xi.close();
        new File(root + "/00000000/kv2").mkdirs();
        KeyValueStoreIndex kv = KeyValueStoreIndex.open("space", root + "/00000000/kv2/kv_space");
        kv.set("k".getBytes("UTF8"), 0L);
        kv.close();
        System.out.println("sizes ok");
    }

    static void require(boolean ok, String what) {
        if (!ok) throw new IllegalStateException(what);
    }

    // Opens files created by another writer with Configure defaults: MemHashBlock
    // takes the hash size from the existing file length.
    static void readSizes(String root) throws Exception {
        String perm = root + "/00000000/text/text_";
        for (String div : DIVS) {
            TextPermData d = TextPermData.open(div, perm + div);
            TextPermIndex x = TextPermIndex.open(div, perm + div);
            long pos = x.get(1);
            require(pos >= 0 && new String(d.read(pos), "UTF-8").equals("x"), "perm " + div);
            require(x.get(2) < 0, "perm miss " + div);
        }
        TextPermIndex.closeAll();
        TextPermData.closeAll();
        TextTable daily = TextTable.open(root + "/" + DATE + "/text/text");
        require(new String(daily.get("error", 1), "UTF-8").equals("x"), "daily");
        daily.close();
        XLogIndex xi = new XLogIndex(root + "/" + DATE + "/xlog/xlog");
        require(xi.getByTxid(1) == 0 && xi.getByGxid(2).size() == 1 && xi.getByTxid(3) == -1, "xlog id");
        xi.close();
        KeyValueStoreIndex kv = KeyValueStoreIndex.open("space", root + "/00000000/kv2/kv_space");
        require(kv.get("k".getBytes("UTF8")) == 0 && kv.get("z".getBytes("UTF8")) == -1, "kv");
        kv.close();
        System.out.println("read-sizes ok");
    }

    // purge <root> <phase>...: each phase is key=value[,key=value]; the first
    // configures the scheduler before construction, later ones run the
    // ConfObserver like a reload. One AutoDeleteScheduler.run() iteration per
    // phase, then the remaining files are listed.
    static void purge(String root, String[] a) throws Exception {
        Class<?> c = Class.forName("scouter.server.core.AutoDeleteScheduler");
        Object scheduler = null;
        String[] order = {"purgeOnDiskFreeSize", "purgeProfile", "purgeXlog", "purgeTagCounter", "purgeText", "purgeSum", "purgeVisitorCounter", "purgeRealtimeCounter", "purgeAllCounter"};
        for (int phase = 0; phase + 2 < a.length; phase++) {
            for (String kv : a[phase + 2].split(",")) {
                if (kv.isEmpty()) continue;
                String[] p = kv.split("=", 2);
                set(p[0], p[1]);
            }
            if (scheduler == null) {
                Constructor<?> ctor = c.getDeclaredConstructor();
                ctor.setAccessible(true);
                scheduler = ctor.newInstance(); // not started: run() is driven below
            } else {
                ConfObserver.exec();
            }
            if (Configure.getInstance().getClass().getField("mgr_purge_enabled").getBoolean(Configure.getInstance())) {
                String today = DateUtil.yyyymmdd();
                for (String name : order) {
                    Method m = c.getDeclaredMethod(name, String.class);
                    m.setAccessible(true);
                    m.invoke(scheduler, today);
                }
            }
            List<String> files = new ArrayList<String>();
            list(new File(root), "", files);
            Collections.sort(files);
            for (String f : files) System.out.println("phase " + phase + " " + f);
        }
    }

    static void list(File dir, String prefix, List<String> out) {
        File[] children = dir.listFiles();
        if (children == null) return;
        for (File f : children) {
            String rel = prefix.isEmpty() ? f.getName() : prefix + "/" + f.getName();
            if (f.isDirectory()) list(f, rel, out);
            else out.add(rel);
        }
    }
}
