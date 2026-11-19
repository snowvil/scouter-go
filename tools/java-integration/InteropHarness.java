// Independent test harness. Original Scouter classes (Apache-2.0) are compiled in place,
// without copying or modifying their transport, registry, login or serialization code.
import scouter.agent.Configure;
import scouter.agent.netio.data.HostAgentDataProxy;
import scouter.agent.netio.data.net.DataUdpAgent;
import scouter.agent.netio.data.net.TcpWorker;
import scouter.agent.netio.request.ReqestHandlingProxy;
import scouter.client.net.DummyTcpProxy;
import scouter.client.net.LoginMgr;
import scouter.client.net.LoginResult;
import scouter.client.net.TcpProxy;
import scouter.client.server.Server;
import scouter.client.server.ServerManager;
import scouter.io.DataOutputX;
import scouter.lang.TextTypes;
import scouter.lang.TimeTypeEnum;
import scouter.lang.pack.*;
import scouter.lang.step.MessageStep;
import scouter.lang.step.Step;
import scouter.lang.value.*;
import scouter.net.RequestCmd;
import scouter.util.HashUtil;
import scouter.util.Hexa32;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class InteropHarness {
    private static final String NAME = "/java-interop-synthetic";
    private static final String SERVICE = "interop synthetic service";
    private static final String ERROR = "interop synthetic error";
    private static final String COUNTER = "Cpu";
    private static final int OBJECT = HashUtil.hash(NAME);
    private static final int SERVICE_HASH = HashUtil.hash(SERVICE);
    private static final int ERROR_HASH = HashUtil.hash(ERROR);
    private static final int EMPTY_HASH = HashUtil.hash("interop synthetic empty dictionary key");
    private static final int MISSING_HASH = HashUtil.hash("interop synthetic missing dictionary key");
    private static final int MISSING_OBJECT = HashUtil.hash("/interop-synthetic-missing-object");
    private static final long TXID = 720000000001L;
    private static final String MARKER = "original-host-tcp";
    private static final long POLL_MILLIS = 10000L;
    private static Server server;
    private static long time;
    private static String date;

    interface Check { boolean run() throws Exception; }

    static void require(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }

    static void eventually(String label, Check check) throws Exception {
        long deadline = System.nanoTime() + POLL_MILLIS * 1000000L;
        do {
            if (check.run()) return;
            Thread.sleep(100);
        } while (System.nanoTime() < deadline);
        throw new AssertionError("Timeout waiting for " + label);
    }

    static TcpProxy proxy() {
        require(server.getSession() != 0 && server.isOpen() && server.isConnected(),
                "Original Client must have an authenticated, connected session");
        TcpProxy proxy = TcpProxy.getTcpProxy(server.getId());
        require(!(proxy instanceof DummyTcpProxy), "DummyTcpProxy must never satisfy integration tests");
        return proxy;
    }

    static List<Pack> packs(String command, MapPack request) {
        TcpProxy proxy = proxy();
        try {
            List<Pack> result = proxy.process(command, request);
            require(proxy.isValid(), "Original Client transport failed: " + command);
            return result;
        } finally {
            TcpProxy.putTcpProxy(proxy);
        }
    }

    static Pack one(String command, MapPack request) {
        List<Pack> result = packs(command, request);
        require(result.size() <= 1, "Unexpected response count: " + command);
        return result.isEmpty() ? null : result.get(0);
    }

    private static Value counter(MapPack request) {
        TcpProxy proxy = proxy();
        try {
            Value result = proxy.getSingleValue(RequestCmd.COUNTER_REAL_TIME, request);
            require(proxy.isValid(), "Original Client realtime counter transport failed");
            return result;
        } finally {
            TcpProxy.putTcpProxy(proxy);
        }
    }

    private static byte[] chunk(int index) {
        MessageStep step = new MessageStep(index * 10, index == 1
                ? "interop-first-" + "synthetic-".repeat(180) : "interop-second");
        step.index = index;
        step.parent = -1;
        return Step.toBytes(Arrays.asList(step));
    }

    private static byte[] profileBytes() {
        byte[] first = chunk(1), second = chunk(2);
        byte[] all = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, all, first.length, second.length);
        return all;
    }

    static void send(Pack pack) throws Exception {
        require(DataUdpAgent.getInstance().write(new DataOutputX().writePack(pack).toByteArray()),
                "Original Host UDP sender rejected synthetic " + pack.getClass().getSimpleName());
    }

    private static void seed() throws Exception {
        Configure configure = Configure.getInstance();
        require(NAME.equals(configure.getObjName()) && configure.getObjHash() == OBJECT,
                "Actual Host Configure must identify the synthetic object");
        ObjectPack object = new ObjectPack();
        object.objHash = OBJECT;
        object.objName = NAME;
        object.objType = "host";
        object.address = "127.0.0.1";
        object.version = "java-interop-synthetic";
        object.wakeup = time;
        object.tags.put("interop", "synthetic");
        HostAgentDataProxy.sendHeartBeat(object);
        HostAgentDataProxy.sendError(ERROR_HASH, ERROR);
        send(new TextPack(TextTypes.SERVICE, SERVICE_HASH, SERVICE));
        // Establish dictionary readiness before the XLog group lookup consumes it.
        eventually("seed service dictionary readiness", () -> {
            Pack p=one(RequestCmd.GET_TEXT,textRequest(TextTypes.SERVICE,SERVICE_HASH));
            return p instanceof MapPack && SERVICE.equals(((MapPack)p).getText(Hexa32.toString32(SERVICE_HASH)));
        });

        PerfCounterPack[] counters = new PerfCounterPack[2];
        for (int i = 0; i < counters.length; i++) {
            PerfCounterPack counter = new PerfCounterPack();
            counter.time = time + i * 2000L;
            counter.objName = NAME;
            counter.timetype = TimeTypeEnum.REALTIME;
            counter.put(COUNTER, new FloatValue(i == 0 ? 12.5f : 25.25f));
            counters[i] = counter;
        }
        // HostAgentDataProxy uses the original CAFN list transport for this pair.
        HostAgentDataProxy.sendCounter(counters);

        XLogPack xlog = new XLogPack();
        xlog.endTime = time;
        xlog.objHash = OBJECT;
        xlog.service = SERVICE_HASH;
        xlog.txid = TXID;
        xlog.gxid = TXID;
        xlog.elapsed = 123;
        xlog.error = ERROR_HASH;
        xlog.xType = XLogTypes.WEB_SERVICE;
        xlog.text1 = "synthetic-xlog-tail";
        send(xlog);

        XLogProfilePack first = new XLogProfilePack();
        first.time = time;
        first.objHash = OBJECT;
        first.service = SERVICE_HASH;
        first.txid = TXID;
        first.profile = chunk(1);
        send(first); // Larger than configured MTU: original CAFM fragmentation.
        // Fragment reassembly and separate datagrams do not guarantee persistence order.
        MapPack firstProfile=new MapPack();firstProfile.put("date",date);firstProfile.put("txid",TXID);
        firstProfile.put("gxid",TXID);firstProfile.put("xlogType",XLogTypes.WEB_SERVICE);firstProfile.put("max",-1);
        eventually("first fragmented Profile persisted before V2", () -> {
            Pack p=one(RequestCmd.TRANX_PROFILE,firstProfile);
            return p instanceof XLogProfilePack && Arrays.equals(chunk(1),((XLogProfilePack)p).profile);
        });
        XLogProfilePack2 second = new XLogProfilePack2();
        second.time = time;
        second.objHash = OBJECT;
        second.service = SERVICE_HASH;
        second.txid = TXID;
        second.gxid = TXID;
        second.xType = XLogTypes.WEB_SERVICE;
        second.profile = chunk(2);
        send(second);

        // Keep the original eight Pack seed/order; append four small 2C records.
        // ERROR is daily text. Original permanent-text storage deliberately skips empty strings.
        send(new TextPack(TextTypes.ERROR, EMPTY_HASH, ""));
        PerfCounterPack daily = new PerfCounterPack();
        daily.time = time;
        daily.objName = NAME;
        daily.timetype = TimeTypeEnum.FIVE_MIN;
        daily.put(COUNTER, new FloatValue(37.5f));
        send(daily);
        for (int i = 1; i <= 2; i++) {
            XLogPack extra = new XLogPack();
            extra.endTime = time + (i == 1 ? 1250 : 2750);
            extra.objHash = OBJECT;
            extra.service = SERVICE_HASH;
            extra.txid = TXID + i;
            extra.gxid = TXID + i;
            extra.elapsed = i == 1 ? 40 : 250;
            extra.xType = XLogTypes.WEB_SERVICE;
            send(extra);
            MapPack stored=new MapPack();stored.put("date",date);stored.put("txid",extra.txid);
            eventually("ordered seed XLog persisted", () -> one(RequestCmd.XLOG_READ_BY_TXID,stored) instanceof XLogPack);
        }
    }

    private static MapPack textRequest(String type, int hash) {
        MapPack request = new MapPack();
        request.put("date", date);
        request.put("type", type);
        request.newList("hash").add(hash);
        return request;
    }

    static MapPack map(Pack response, String label) {
        require(response instanceof MapPack, label + " response must be MapPack");
        return (MapPack) response;
    }

    static ListValue list(MapPack response, String key) {
        require(response.get(key) instanceof ListValue, "Response must contain ListValue: " + key);
        return (ListValue) response.get(key);
    }

    static double number(Value value) {
        require(value instanceof NumberValue, "Response value must be numeric");
        return ((NumberValue) value).doubleValue();
    }

    static long integer(Value value) {
        require(value instanceof DecimalValue, "Response integer must be DecimalValue");
        return ((DecimalValue) value).value;
    }

    private static void series(MapPack response) {
        ListValue times = list(response, "time"), values = list(response, "value");
        require(times.size() == 2 && values.size() == 2, "Realtime history arrays must contain two samples");
        require(integer(times.get(0)) == time && integer(times.get(1)) == time + 2000,
                "Realtime history sample timestamps/order changed");
        require(number(values.get(0)) == 12.5 && number(values.get(1)) == 25.25,
                "Realtime history sample values/order changed");
    }

    static MapPack dateRequest() {
        MapPack request = new MapPack();
        request.put("date", date);
        return request;
    }

    static void integers(MapPack request, String key, int... values) {
        ListValue list = request.newList(key);
        for (int value : values) list.add(value);
    }

    static void strings(MapPack request, String key, String... values) {
        ListValue list = request.newList(key);
        for (String value : values) list.add(value);
    }

    private static void verifyObjects(boolean restarted) {
        MapPack request = new MapPack();
        request.put("objHash", OBJECT); // Original default-today OBJECT_INFO contract.
        ObjectPack object = (ObjectPack) one(RequestCmd.OBJECT_INFO, request);
        require(object != null && object.objHash == OBJECT && NAME.equals(object.objName),
                "Default-today OBJECT_INFO must resolve the stored object");
        MapPack daily = map(one(RequestCmd.OBJECT_LIST_LOAD_DATE, dateRequest()), "Daily objects");
        ListValue hashes = list(daily, "objHash"), names = list(daily, "objName"), types = list(daily, "objType");
        require(hashes.size() == names.size() && hashes.size() == types.size() && hashes.size() == (restarted ? 2 : 1),
                "Daily object parallel arrays must align with Host and persisted implicit Batch");
        require(integer(hashes.get(0)) == OBJECT && NAME.equals(names.getString(0))
                && "host".equals(types.getString(0)), "Daily object arrays changed identity/order");
    }

    private static void textPack(Pack response, String type, int hash, String text) {
        require(response instanceof TextPack, "Dictionary stream must contain TextPack");
        TextPack pack = (TextPack) response;
        require(type.equals(pack.xtype) && pack.hash == hash && text.equals(pack.text),
                "Dictionary stream type/hash/content/order changed");
    }

    private static void verifyDictionary() throws Exception {
        eventually("existing daily ERROR empty dictionary string", () -> {
            Pack response = one(RequestCmd.GET_TEXT, textRequest(TextTypes.ERROR, EMPTY_HASH));
            return response instanceof MapPack && ((MapPack) response).containsKey(Hexa32.toString32(EMPTY_HASH))
                    && "".equals(((MapPack) response).getText(Hexa32.toString32(EMPTY_HASH)));
        });
        MapPack request = dateRequest();
        request.put("type", TextTypes.ERROR);
        integers(request, "hash", MISSING_HASH, EMPTY_HASH, ERROR_HASH, ERROR_HASH);
        List<Pack> maps = packs(RequestCmd.GET_TEXT_100, request);
        require(maps.size() == 1, "Small GET_TEXT_100 result must use one MapPack");
        MapPack textMap = map(maps.get(0), "GET_TEXT_100");
        require(textMap.size() == 2 && textMap.containsKey(Hexa32.toString32(EMPTY_HASH))
                && "".equals(textMap.getText(Hexa32.toString32(EMPTY_HASH)))
                && ERROR.equals(textMap.getText(Hexa32.toString32(ERROR_HASH)))
                && !textMap.containsKey(Hexa32.toString32(MISSING_HASH)),
                "Map dictionary must retain empty, omit missing and replace duplicate hash");
        require(new ArrayList<>(textMap.keySet()).equals(Arrays.asList(
                Hexa32.toString32(EMPTY_HASH), Hexa32.toString32(ERROR_HASH))),
                "Map dictionary insertion order changed");
        List<Pack> texts = packs(RequestCmd.GET_TEXT_PACK, request);
        require(texts.size() == 3, "GET_TEXT_PACK must retain found duplicate entries and omit missing");
        textPack(texts.get(0), TextTypes.ERROR, EMPTY_HASH, "");
        textPack(texts.get(1), TextTypes.ERROR, ERROR_HASH, ERROR);
        textPack(texts.get(2), TextTypes.ERROR, ERROR_HASH, ERROR);

        MapPack mixed = dateRequest();
        integers(mixed, "hash", EMPTY_HASH, ERROR_HASH, MISSING_HASH, SERVICE_HASH, SERVICE_HASH);
        strings(mixed, "type", TextTypes.ERROR, TextTypes.ERROR, TextTypes.SERVICE, TextTypes.ERROR, TextTypes.SERVICE);
        texts = packs(RequestCmd.GET_TEXT_ANY_TYPE, mixed);
        require(texts.size() == 3, "ANY_TYPE must zip type/hash, retain empty and omit both missing lookups");
        textPack(texts.get(0), TextTypes.ERROR, EMPTY_HASH, "");
        textPack(texts.get(1), TextTypes.ERROR, ERROR_HASH, ERROR);
        textPack(texts.get(2), TextTypes.SERVICE, SERVICE_HASH, SERVICE);
        require(map(one(RequestCmd.GET_TEXT, textRequest(TextTypes.SERVICE, MISSING_HASH)), "Missing GET_TEXT").isEmpty(),
                "GET_TEXT missing hash must return one empty MapPack");
        require(packs(RequestCmd.GET_TEXT_100, textRequest(TextTypes.SERVICE, MISSING_HASH)).isEmpty(),
                "GET_TEXT_100 missing hash must return no Pack");
        require(packs(RequestCmd.GET_TEXT_PACK, textRequest(TextTypes.SERVICE, MISSING_HASH)).isEmpty(),
                "GET_TEXT_PACK missing hash must return no Pack");
        MapPack missingMixed = dateRequest();
        missingMixed.newList("hash").add(MISSING_HASH);
        missingMixed.newList("type").add(TextTypes.SERVICE);
        require(packs(RequestCmd.GET_TEXT_ANY_TYPE, missingMixed).isEmpty(),
                "GET_TEXT_ANY_TYPE missing hash must return no Pack");
    }

    private static MapPack counterRange() {
        MapPack request = new MapPack();
        request.put("counter", COUNTER);
        request.put("stime", time - 1);
        request.put("etime", time + 2001);
        return request;
    }

    private static void verifyCounters(boolean restarted) throws Exception {
        if (!restarted) {
            MapPack request = new MapPack();
            request.put("objHash", OBJECT);
            strings(request, "counter", COUNTER, "interop-missing-counter", COUNTER);
            MapPack multi = map(one(RequestCmd.COUNTER_REAL_TIME_MULTI, request), "Counter multi");
            ListValue names = list(multi, "counter"), values = list(multi, "value");
            require(names.size() == 2 && values.size() == 2 && COUNTER.equals(names.getString(0))
                    && COUNTER.equals(names.getString(1)) && number(values.get(0)) == 25.25
                    && number(values.get(1)) == 25.25, "Counter multi must retain request order/duplicates, omit missing");
            request = new MapPack();
            request.put("counter", COUNTER);
            request.put("objType", "host");
            MapPack all = map(one(RequestCmd.COUNTER_REAL_TIME_ALL, request), "Counter all");
            require(list(all, "objHash").size() == 1 && list(all, "value").size() == 1
                    && integer(list(all, "objHash").get(0)) == OBJECT
                    && number(list(all, "value").get(0)) == 25.25, "Counter all arrays must align");
            request = new MapPack();
            request.put("objHash", OBJECT);
            MapPack objectAll = map(one(RequestCmd.COUNTER_REAL_TIME_OBJECT_ALL, request), "Counter object all");
            ListValue allNames = list(objectAll, "counter"), allValues = list(objectAll, "value");
            require(allNames.size() == 3 && allValues.size() == 3,
                    "Actual NetDataProcessor adds objHash/time to original Cpu counter data");
            MapValue byName = new MapValue();
            for (int i = 0; i < allNames.size(); i++) byName.put(allNames.getString(i), allValues.get(i));
            counterMetadata(byName);
            request = new MapPack();
            request.put("objType", "host");
            MapPack objectType = map(one(RequestCmd.COUNTER_REAL_TIME_OBJECT_TYPE_ALL, request), "Counter object type all");
            require(objectType.size() == 1 && objectType.get(NAME) instanceof MapValue,
                    "Object type counters use object name and MapValue");
            counterMetadata((MapValue) objectType.get(NAME));
        }
        MapPack explicit = counterRange();
        integers(explicit, "objHash", OBJECT, MISSING_OBJECT);
        List<Pack> all = packs(RequestCmd.COUNTER_PAST_TIME_ALL, explicit);
        require(all.size() == 2, "Past counter ALL must return one MapPack per requested object");
        MapPack found = map(all.get(0), "Past counter first");
        require(found.getInt("objHash") == OBJECT, "Past counter ALL must preserve object request order");
        series(found);
        List<Pack> grouped = packs(RequestCmd.COUNTER_PAST_TIME_GROUP, explicit);
        require(grouped.size() == 2, "Past group retains one result per requested hash");
        MapPack presentGroup = map(grouped.get(0), "Past group found");
        require(presentGroup.getInt("objHash") == OBJECT
                && list(presentGroup, "time").size() == 0 && list(presentGroup, "value").size() == 0,
                "Original readBulk skips the first physical minute with data offset zero");
        MapPack absentGroup = map(grouped.get(1), "Past group absent");
        require(absentGroup.getInt("objHash") == MISSING_OBJECT
                && list(absentGroup, "time").size() == 0 && list(absentGroup, "value").size() == 0,
                "Past group missing object has empty aligned arrays");
        // Per-object indexed reads accept offset zero. Verify the actual
        // receiver-injected metadata was persisted independently of bulk's gate.
        for (String key : Arrays.asList("objHash", "time")) {
            MapPack metadataRequest = counterRange();
            metadataRequest.put("counter", key);
            metadataRequest.put("objHash", OBJECT);
            MapPack metadata = map(one(RequestCmd.COUNTER_PAST_TIME, metadataRequest), "Persisted counter metadata");
            ListValue stamps = list(metadata, "time"), values = list(metadata, "value");
            require(stamps.size() == 2 && values.size() == 2, "Metadata history has two indexed rows");
            for (int i = 0; i < 2; i++) {
                require(stamps.getLong(i) == time + i * 2000 && values.get(i) instanceof DecimalValue
                        && values.getLong(i) == (key.equals("objHash") ? OBJECT : time + i * 2000),
                        "Persisted receiver metadata type/value/time alignment");
            }
        }
        MapPack missing = map(all.get(1), "Past counter missing object");
        require(missing.getInt("objHash") == MISSING_OBJECT && list(missing, "time").size() == 0
                && list(missing, "value").size() == 0, "Missing object must retain aligned empty history arrays");
        MapPack typed = counterRange();
        typed.put("objType", "host");
        all = packs(RequestCmd.COUNTER_PAST_TIME_ALL, typed);
        require(all.size() == 1 && map(all.get(0), "Past counter type").getInt("objHash") == OBJECT,
                "Past counter ALL objType selector must resolve the daily object");
        series((MapPack) all.get(0));

        MapPack daily = dateRequest();
        daily.put("objHash", OBJECT);
        daily.put("counter", COUNTER);
        daily.put("timetype", TimeTypeEnum.FIVE_MIN);
        eventually("five-minute daily counter", () -> {
            Pack response = one(RequestCmd.COUNTER_PAST_DATE, daily);
            if (response == null) return false;
            dailySeries(map(response, "Past date FIVE_MIN"));
            return true;
        });
        dailySeries(map(one(RequestCmd.COUNTER_TODAY, daily), "Today FIVE_MIN"));
    }

    private static void counterMetadata(MapValue data) {
        require(data.size() == 3 && data.get(COUNTER) instanceof FloatValue
                && ((FloatValue) data.get(COUNTER)).value == 25.25f
                && data.get("objHash") instanceof DecimalValue && data.getLong("objHash") == OBJECT
                && data.get("time") instanceof DecimalValue && data.getLong("time") == time + 2000,
                "NetDataProcessor receive metadata/cache preserves original types and latest time");
    }

    private static void dailySeries(MapPack response) {
        ListValue times = list(response, "time"), values = list(response, "value");
        require(times.size() == 288 && values.size() == 288, "FIVE_MIN daily arrays must contain 288 buckets");
        long midnight = LocalDate.parse(date, DateTimeFormatter.BASIC_ISO_DATE)
                .atStartOfDay(ZoneId.of("Asia/Seoul")).toInstant().toEpochMilli();
        int filled = (int) ((time - midnight) / 300000L);
        for (int i = 0; i < 288; i++) {
            require(integer(times.get(i)) == midnight + i * 300000L,
                    "FIVE_MIN timestamps must start at Asia/Seoul midnight with five-minute spacing");
            if (i == filled) require(number(values.get(i)) == 37.5, "FIVE_MIN filled bucket value changed");
            else require(values.get(i) instanceof NullValue, "Unwritten FIVE_MIN buckets must remain NullValue");
        }
    }

    private static MapPack realtimeRequest(long loop, int index) {
        MapPack request = new MapPack();
        request.put("loop", loop);
        request.put("index", index);
        request.put("limit", 200); // Existing elapsed123 error XLog must bypass this lower bound.
        request.put("count", 2);
        request.newList("objHash").add(OBJECT);
        return request;
    }

    private static MapPack realtimeMeta(List<Pack> result) {
        require(!result.isEmpty(), "Realtime result must contain metadata first");
        MapPack metadata = map(result.get(0), "Realtime metadata first");
        require(metadata.size() == 2 && metadata.get("loop") instanceof DecimalValue
                && metadata.get("index") instanceof DecimalValue, "Realtime metadata keys/types changed");
        require(metadata.getLong("loop") == 0 && metadata.getInt("index") == 3,
                "Three synthetic XLogs must produce exact loop=0/index=3, including filtered records");
        return metadata;
    }

    private static void xlogIDs(List<Pack> result, int first, int end, long... expected) {
        require(end - first == expected.length, "Unexpected XLog response count");
        for (int i = 0; i < expected.length; i++) {
            require(result.get(first + i) instanceof XLogPack, "XLog record must precede/follow metadata as specified");
            XLogPack xlog = (XLogPack) result.get(first + i);
            require(xlog.txid == expected[i] && xlog.objHash == OBJECT, "XLog transaction order/filter changed");
        }
    }

    private static void verifyRealtime() throws Exception {
        eventually("three XLogs in realtime ring", () -> {
            List<Pack> all = packs(RequestCmd.TRANX_REAL_TIME_GROUP_LATEST, realtimeRequest(0, 0));
            if (all.isEmpty() || !(all.get(0) instanceof MapPack) || ((MapPack) all.get(0)).getInt("index") != 3)
                return false;
            return true;
        });
        List<Pack> filtered = packs(RequestCmd.TRANX_REAL_TIME_GROUP, realtimeRequest(0, 0));
        MapPack metadata = realtimeMeta(filtered);
        xlogIDs(filtered, 1, filtered.size(), TXID, TXID + 2);
        List<Pack> resumed = packs(RequestCmd.TRANX_REAL_TIME_GROUP,
                realtimeRequest(metadata.getLong("loop"), metadata.getInt("index")));
        realtimeMeta(resumed);
        require(resumed.size() == 1, "Exact realtime cursor resume must return metadata only");
        List<Pack> latest = packs(RequestCmd.TRANX_REAL_TIME_GROUP_LATEST, realtimeRequest(0, 0));
        realtimeMeta(latest);
        xlogIDs(latest, 1, latest.size(), TXID + 1, TXID + 2);
        // Java getListWithinCount retains the requested latest count when gap==0.
        latest = packs(RequestCmd.TRANX_REAL_TIME_GROUP_LATEST, realtimeRequest(0, 3));
        realtimeMeta(latest);
        xlogIDs(latest, 1, latest.size(), TXID + 1, TXID + 2);
    }

    private static MapPack historyRequest(int pageCount, long bucket, long txid) {
        MapPack request = dateRequest();
        request.put("stime", time);
        request.put("etime", time + 3999);
        request.put("pageCount", pageCount);
        request.put("lastBucketTime", bucket);
        request.put("txid", txid);
        request.put("limit", 0);
        request.put("max", 0);
        request.put("reverse", false);
        request.newList("objHash").add(OBJECT);
        return request;
    }

    private static MapPack historyMeta(List<Pack> result, boolean more, long bucket, long txid) {
        require(!result.isEmpty(), "History paging must contain metadata last");
        MapPack metadata = map(result.get(result.size() - 1), "History metadata last");
        require(metadata.size() == 3 && metadata.get("hasMore") instanceof BooleanValue
                && metadata.get("lastXLogTime") instanceof DecimalValue
                && metadata.get("lastTxid") instanceof DecimalValue, "History metadata keys/types changed");
        require(metadata.getBoolean("hasMore") == more && metadata.getLong("lastXLogTime") == bucket
                && metadata.getLong("lastTxid") == txid, "History hasMore/bucket/txid cursor changed");
        return metadata;
    }

    private static void verifyPaging() throws Exception {
        eventually("all history records", () -> packs(RequestCmd.TRANX_LOAD_TIME_GROUP, historyRequest(0, 0, 0)).size() == 3);
        List<Pack> baseline = packs(RequestCmd.TRANX_LOAD_TIME_GROUP, historyRequest(0, 0, 0));
        xlogIDs(baseline, 0, baseline.size(), TXID, TXID + 1, TXID + 2);
        List<Pack> first = packs(RequestCmd.TRANX_LOAD_TIME_GROUP_V2, historyRequest(2, 0, 0));
        xlogIDs(first, 0, first.size() - 1, TXID, TXID + 1);
        MapPack metadata = historyMeta(first, true, time + 1000, TXID + 1);
        require(metadata.getLong("lastXLogTime") != ((XLogPack) first.get(1)).endTime,
                "Paging cursor must use index bucket, not exact XLog endTime");
        List<Pack> second = packs(RequestCmd.TRANX_LOAD_TIME_GROUP_V2,
                historyRequest(2, metadata.getLong("lastXLogTime"), metadata.getLong("lastTxid")));
        xlogIDs(second, 0, second.size() - 1, TXID + 2);
        MapPack last = historyMeta(second, false, time + 2500, TXID + 2);
        List<Pack> exhausted = packs(RequestCmd.TRANX_LOAD_TIME_GROUP_V2,
                historyRequest(2, last.getLong("lastXLogTime"), last.getLong("lastTxid")));
        require(exhausted.size() == 1, "Exhausted history cursor must return metadata only");
        historyMeta(exhausted, false, 0, 0);
        List<Pack> merged = new ArrayList<>(first.subList(0, first.size() - 1));
        merged.addAll(second.subList(0, second.size() - 1));
        require(merged.size() == baseline.size(), "Paged union must equal ordinary history without duplicates/gaps");
        for (int i = 0; i < merged.size(); i++) {
            require(Arrays.equals(new DataOutputX().writePack(merged.get(i)).toByteArray(),
                    new DataOutputX().writePack(baseline.get(i)).toByteArray()),
                    "Paged union bytes/order must equal ordinary history");
        }
    }

    private static void verify(boolean restarted) throws Exception {
        MapPack objectRequest = new MapPack();
        objectRequest.put("date", date);
        objectRequest.put("objHash", OBJECT);
        eventually("ObjectPack history", () -> {
            Pack response = one(RequestCmd.OBJECT_INFO, objectRequest);
            if (response == null) return false;
            require(response instanceof ObjectPack, "OBJECT_INFO response must be ObjectPack");
            ObjectPack object = (ObjectPack) response;
            require(object.objHash == OBJECT && NAME.equals(object.objName)
                    && "host".equals(object.objType), "Stored object identity changed");
            require("synthetic".equals(object.tags.getText("interop")), "Object tags changed");
            return true;
        });
        if (!restarted) {
            eventually("realtime objects", () -> {
                for (Pack response : packs(RequestCmd.OBJECT_LIST_REAL_TIME, null)) {
                    require(response instanceof ObjectPack, "Realtime object response must be ObjectPack");
                    if (((ObjectPack) response).objHash == OBJECT) return true;
                }
                return false;
            });
        }
        eventually("service dictionary", () -> {
            Pack response = one(RequestCmd.GET_TEXT, textRequest(TextTypes.SERVICE, SERVICE_HASH));
            if (response == null) return false;
            require(response instanceof MapPack, "GET_TEXT must return MapPack");
            return SERVICE.equals(((MapPack) response).getText(Hexa32.toString32(SERVICE_HASH)));
        });
        eventually("error dictionary via actual HostAgentDataProxy", () -> {
            Pack response = one(RequestCmd.GET_TEXT, textRequest(TextTypes.ERROR, ERROR_HASH));
            return response instanceof MapPack
                    && ERROR.equals(((MapPack) response).getText(Hexa32.toString32(ERROR_HASH)));
        });

        MapPack counterRequest = new MapPack();
        counterRequest.put("objHash", OBJECT);
        counterRequest.put("counter", COUNTER);
        counterRequest.put("timetype", TimeTypeEnum.REALTIME);
        if (!restarted) {
            eventually("realtime counter", () -> {
                Value value = counter(counterRequest);
                return value instanceof NumberValue && ((NumberValue) value).doubleValue() == 25.25;
            });
        }
        MapPack historyRequest = new MapPack();
        historyRequest.put("objHash", OBJECT);
        historyRequest.put("counter", COUNTER);
        historyRequest.put("stime", time - 1);
        historyRequest.put("etime", time + 2001);
        eventually("counter history", () -> {
            Pack response = one(RequestCmd.COUNTER_PAST_TIME, historyRequest);
            if (response == null) return false;
            require(response instanceof MapPack, "Counter history must be MapPack");
            MapPack map = (MapPack) response;
            require(map.get("time") instanceof ListValue && map.get("value") instanceof ListValue,
                    "Counter history must contain time/value lists");
            ListValue times = (ListValue) map.get("time"), values = (ListValue) map.get("value");
            if (times.size() != 2 || values.size() != 2) return false;
            require(((NumberValue) times.get(0)).longValue() == time
                    && ((NumberValue) times.get(1)).longValue() == time + 2000,
                    "Counter history time/order changed");
            require(((NumberValue) values.get(0)).doubleValue() == 12.5
                    && ((NumberValue) values.get(1)).doubleValue() == 25.25,
                    "Counter history values changed");
            return true;
        });

        MapPack transactionRequest = new MapPack();
        transactionRequest.put("date", date);
        transactionRequest.put("txid", TXID);
        transactionRequest.put("gxid", TXID);
        transactionRequest.put("xlogType", XLogTypes.WEB_SERVICE);
        transactionRequest.put("max", -1);
        eventually("XLog history", () -> {
            Pack response = one(RequestCmd.XLOG_READ_BY_TXID, transactionRequest);
            if (response == null) return false;
            require(response instanceof XLogPack, "XLog history must return XLogPack");
            XLogPack xlog = (XLogPack) response;
            require(xlog.txid == TXID && xlog.gxid == TXID && xlog.objHash == OBJECT
                    && xlog.endTime == time && xlog.elapsed == 123 && xlog.error == ERROR_HASH
                    && xlog.service == SERVICE_HASH && "synthetic-xlog-tail".equals(xlog.text1),
                    "XLog fields changed");
            return true;
        });
        eventually("ordered Profile V1/V2 chunks", () -> {
            Pack response = one(RequestCmd.TRANX_PROFILE, transactionRequest);
            if (response == null) return false;
            require(response instanceof XLogProfilePack, "Profile query must return ProfilePack");
            byte[] bytes = ((XLogProfilePack) response).profile;
            if (!Arrays.equals(profileBytes(), bytes)) return false;
            Step[] steps = Step.toObjects(bytes);
            require(steps.length == 2 && steps[0] instanceof MessageStep && steps[1] instanceof MessageStep,
                    "Original Java Step decoder must decode both messages");
            require(((MessageStep) steps[0]).index == 1 && ((MessageStep) steps[1]).index == 2
                    && "interop-second".equals(((MessageStep) steps[1]).message),
                    "Profile Step order or content changed");
            return true;
        });
        List<byte[]> chunks = new ArrayList<>();
        TcpProxy proxy = proxy();
        try {
            proxy.process(RequestCmd.TRANX_PROFILE_FULL, transactionRequest, in -> chunks.add(in.readBlob()));
            require(proxy.isValid(), "Original Client full profile transport failed");
        } finally {
            TcpProxy.putTcpProxy(proxy);
        }
        require(chunks.size() == 2 && Arrays.equals(chunks.get(0), chunk(1))
                && Arrays.equals(chunks.get(1), chunk(2)), "Full profile Blob chunks/order changed");
        verifyObjects(restarted);
        verifyDictionary();
        verifyCounters(restarted);
        if (!restarted) verifyRealtime();
        verifyPaging();
        System.out.println("PASS 2C " + (restarted ? "restart" : "seed")
                + " dated/default objects; dictionary daily ERROR empty/missing/order; counter multi/all/daily288;"
                + (restarted ? "" : " realtime metaFIRST loop0/index3;")
                + " history V2 metaLAST page2+1/exhausted cursor/byte-equal union");
        System.out.println("PASS " + (restarted ? "restart-history" : "ingest-query")
                + " object/text/counter/xlog/profile; actual Host UDP and Desktop Client network");
    }

    private static void relay() throws Exception {
        MapPack request = new MapPack();
        request.put("objHash", OBJECT);
        eventually("actual Host TCP_AGENT V1 OBJECT_ENV relay", () -> {
            Pack response = one(RequestCmd.OBJECT_ENV, request);
            if (response == null) return false;
            require(response instanceof MapPack, "Actual Host OBJECT_ENV must return MapPack");
            // Never print environment or property values. Check only a synthetic marker.
            return MARKER.equals(((MapPack) response).getText("scouter.integration.marker"));
        });
        System.out.println("PASS tcp-agent-v1 relay via original TcpWorker/ReqestHandlingProxy/HostAgentEnv");
    }

    public static void main(String[] args) throws Exception {
        require(args.length == 3, "usage: InteropHarness <tcp-port> <epoch-ms> <seed|verify>");
        time = Long.parseLong(args[1]);
        date = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("Asia/Seoul"))
                .format(Instant.ofEpochMilli(time));
        boolean restarted = "verify".equals(args[2]);
        require(restarted || "seed".equals(args[2]), "Unknown harness mode");
        server = new Server("127.0.0.1", args[0]);
        ServerManager.getInstance().addServer(server);
        TcpWorker worker = null;
        Thread agentThread = null;
        try {
            LoginResult invalid = LoginMgr.login(server.getId(), "admin", "synthetic-invalid-password");
            require(!invalid.success && server.getSession() == 0, "Invalid login must be rejected");
            require("Authentication failed".equals(invalid.errorMessage),
                    "Invalid login must reach original authentication response path");
            LoginResult valid = LoginMgr.login(server.getId(), "admin", "admin");
            require(valid.success && server.getSession() != 0, "Actual original LoginMgr login failed");
            require("Asia/Seoul".equals(server.getTimezone()), "Login timezone changed");
            MapPack xml = LoginMgr.getCounterXmlServer(server.getId());
            require(xml != null && xml.get("default") instanceof BlobValue
                    && ((BlobValue) xml.get("default")).value.length > 0,
                    "Actual Client counter XML negotiation must return nonempty BlobValue");
            try (java.io.InputStream originalXml = InteropHarness.class.getResourceAsStream(
                    "/scouter/lang/counters/counters.xml")) {
                require(originalXml != null, "Pinned original counter XML resource is missing");
                require(Arrays.equals(originalXml.readAllBytes(), ((BlobValue) xml.get("default")).value),
                        "Go counter XML must equal the pinned original default resource");
            }
            require(server.getCounterEngine().getObjectType("host") != null,
                    "Original LoginMgr must parse host counter metadata");
            Pack status = one(RequestCmd.SERVER_STATUS, null);
            require(status instanceof MapPack && ((MapPack) status).getLong("time") > 0,
                    "Actual ServerManager SERVER_STATUS bodyless command must be supported");
            System.out.println("PASS invalid+valid login and counter XML/status via original LoginMgr/TcpProxy/ClientTCP");
            if (!restarted) {
                seed();
                ReqestHandlingProxy.load();
                worker = new TcpWorker();
                require(worker.prepare(), "Actual Host TcpWorker socket preparation failed");
                agentThread = new Thread(worker, "integration-original-host-tcp");
                agentThread.setDaemon(true);
                agentThread.start();
            }
            verify(restarted);
            if (!restarted) { relay(); ExtraInteropHarness.seed(time, date, OBJECT, NAME); }
            ExtraInteropHarness.verify(time, date, OBJECT, NAME, restarted);
        } finally {
            if (worker != null) worker.close();
            if (agentThread != null) agentThread.join(2000);
            ServerManager.getInstance().shutdown();
            if (!restarted) DataUdpAgent.getInstance().close();
        }
    }
}
