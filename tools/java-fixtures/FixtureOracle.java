// Synthetic interoperability oracle using the pinned Scouter common implementation.
// No production accounts, SQL, addresses, or profiles are loaded.
import java.nio.file.*;
import java.util.*;
import scouter.io.*;
import scouter.lang.pack.*;
import scouter.lang.value.*;

public final class FixtureOracle {
    private static Path output;
    private static void emit(String name, DataOutputX out) throws Exception {
        Files.write(output.resolve(name + ".bin"), out.toByteArray());
    }
    private static void pack(String name, Pack p) throws Exception {
        DataOutputX out = new DataOutputX().writePack(p);
        // Java MapValue is a Hashtable, not insertion ordered; compare its values by key.
        byte[] encoded = out.toByteArray();
        Pack decoded = new DataInputX(encoded).readPack();
        if (p instanceof MapPack) {
            compareMaps(((MapPack)p).toMap(), ((MapPack)decoded).toMap());
        } else if (p instanceof ObjectPack) {
            compareValues(((ObjectPack)p).tags, ((ObjectPack)decoded).tags);
        } else if (p instanceof PerfCounterPack) {
            compareValues(((PerfCounterPack)p).data, ((PerfCounterPack)decoded).data);
        } else {
        byte[] reencoded = new DataOutputX().writePack(decoded).toByteArray();
        if (!Arrays.equals(encoded, reencoded)) {
            int at=0; while(at<Math.min(encoded.length,reencoded.length) && encoded[at]==reencoded[at])at++;
            throw new AssertionError("Java round-trip failed: " + name+" offset="+at+" original="+encoded.length+" decoded="+reencoded.length+" byte="+(encoded[at]&255)+" vs "+(reencoded[at]&255));
        }
        }
        emit(name, out);
    }
    private static void compareMaps(Map<String,Value> a, Map<String,Value> b) throws Exception {
        if(!a.keySet().equals(b.keySet())) throw new AssertionError("map keys changed");
        for(String key:a.keySet()) compareValues(a.get(key),b.get(key));
    }
    private static void compareValues(Value a, Value b) throws Exception {
        if(a.getValueType()!=b.getValueType()) throw new AssertionError("value type changed");
        if(a instanceof MapValue) {
            compareMaps(((MapValue)a).toMap(),((MapValue)b).toMap());
        } else if(a instanceof ListValue) {
            ListValue x=(ListValue)a,y=(ListValue)b;
            if(x.size()!=y.size()) throw new AssertionError("list count changed");
            for(int i=0;i<x.size();i++)compareValues(x.get(i),y.get(i));
        } else if(!Arrays.equals(new DataOutputX().writeValue(a).toByteArray(),new DataOutputX().writeValue(b).toByteArray())) {
            throw new AssertionError("Value semantic bytes changed: "+a.getValueType());
        }
    }
    private static MapValue values() {
        MapValue m = new MapValue();
        m.put("null", new NullValue());
        m.put("bool", new BooleanValue(true));
        m.put("decimal", new DecimalValue(-549755813888L));
        m.put("float", new FloatValue(-0.0f));
        m.put("double", new DoubleValue(Double.POSITIVE_INFINITY));
        DoubleSummary ds = new DoubleSummary(); ds.sum=7.5; ds.count=3; ds.min=-1.25; ds.max=8.75;
        LongSummary ls = new LongSummary(); ls.sum=Long.MIN_VALUE; ls.count=2; ls.min=-7; ls.max=9;
        m.put("double_summary", ds); m.put("long_summary", ls);
        m.put("text", new TextValue("합성 fixture 😀"));
        m.put("hash", new TextHashValue(-1234567));
        m.put("blob", new BlobValue(new byte[] {0, 1, (byte)255}));
        m.put("ip", new IP4Value(new byte[] { (byte)192, 0, 2, 7 }));
        ListValue lv = new ListValue(); lv.add(new DecimalValue(128)); lv.add(new TextValue("nested"));
        m.put("list", lv);
        m.put("ints", new IntArray(new int[] {Integer.MIN_VALUE, 0, Integer.MAX_VALUE}));
        m.put("floats", new FloatArray(new float[] {-1.25f, Float.NaN, Float.POSITIVE_INFINITY}));
        m.put("texts", new TextArray(new String[] {"", "한글", null}));
        m.put("longs", new LongArray(new long[] {Long.MIN_VALUE, -1, Long.MAX_VALUE}));
        MapValue nested = new MapValue(); nested.put("z", new DecimalValue(1)); nested.put("a", new DecimalValue(2));
        m.put("map", nested);
        return m;
    }
    private static XLogPack xlog() {
        XLogPack p = new XLogPack();
        p.endTime=1700000000123L; p.objHash=-1234567; p.service=7654321;
        p.txid=Long.MIN_VALUE+7; p.caller=0x1122334455667788L; p.gxid=Long.MAX_VALUE-9;
        p.elapsed=128; p.error=-32769; p.cpu=127; p.sqlCount=3; p.sqlTime=42;
        p.ipaddr=new byte[] {(byte)192,0,2,7}; p.kbytes=32768; p.status=200;
        p.userid=-549755813889L; p.userAgent=17; p.referer=18; p.group=19; p.apicallCount=2; p.apicallTime=6;
        p.countryCode="ZZ"; p.city=20; p.xType=2; p.login=21; p.desc=22; p.webHash=23; p.webTime=24;
        p.hasDump=1; p.threadNameHash=-25; p.text1="합성"; p.text2="fixture";
        p.queuingHostHash=26; p.queuingTime=27; p.queuing2ndHostHash=28; p.queuing2ndTime=29;
        p.text3="third"; p.text4="fourth"; p.text5="fifth"; p.profileCount=30; p.b3Mode=true;
        p.profileSize=31; p.discardType=2; p.ignoreGlobalConsequentSampling=true;
        return p;
    }
    private static void legacyXLogs(XLogPack p) throws Exception {
        DataOutputX b = new DataOutputX();
        b.writeDecimal(p.endTime).writeDecimal(p.objHash).writeDecimal(p.service)
            .writeLong(p.txid).writeLong(p.caller).writeLong(p.gxid)
            .writeDecimal(p.elapsed).writeDecimal(p.error).writeDecimal(p.cpu).writeDecimal(p.sqlCount).writeDecimal(p.sqlTime)
            .writeBlob(p.ipaddr).writeDecimal(p.kbytes).writeDecimal(p.status).writeDecimal(p.userid)
            .writeDecimal(p.userAgent).writeDecimal(p.referer).writeDecimal(p.group).writeDecimal(p.apicallCount).writeDecimal(p.apicallTime);
        for (int version=0; version<=12; version++) {
            switch(version) {
                case 1: b.writeText(p.countryCode).writeDecimal(p.city); break;
                case 2: b.writeByte(p.xType); break;
                case 3: b.writeDecimal(p.login).writeDecimal(p.desc); break;
                case 4: b.writeDecimal(p.webHash).writeDecimal(p.webTime); break;
                case 5: b.writeByte(p.hasDump); break;
                case 6: b.writeDecimal(p.threadNameHash); break;
                case 7: b.writeText(p.text1).writeText(p.text2); break;
                case 8: b.writeDecimal(p.queuingHostHash).writeDecimal(p.queuingTime).writeDecimal(p.queuing2ndHostHash).writeDecimal(p.queuing2ndTime); break;
                case 9: b.writeText(p.text3).writeText(p.text4).writeText(p.text5); break;
                case 10: b.writeDecimal(p.profileCount); break;
                case 11: b.writeBoolean(p.b3Mode); break;
                case 12: b.writeDecimal(p.profileSize).writeByte(p.discardType).writeBoolean(p.ignoreGlobalConsequentSampling); break;
            }
            DataOutputX out = new DataOutputX().writeByte(PackEnum.XLOG).writeBlob(b.toByteArray());
            XLogPack decoded=(XLogPack)new DataInputX(out.toByteArray()).readPack();
            if (decoded.endTime!=p.endTime || decoded.txid!=p.txid || (version==12 && decoded.profileSize!=p.profileSize))
                throw new AssertionError("Java legacy XLog decode failed");
            emit("xlog_v"+version, out);
        }
    }
    public static void main(String[] args) throws Exception {
        output = Paths.get(args[0]);
        long[] decimals = {Long.MIN_VALUE, -549755813889L, -549755813888L, -2147483649L, -2147483648L,
            -8388609L, -8388608L, -32769, -32768, -129, -128, -1, 0, 1, 127, 128, 32767, 32768,
            8388607L, 8388608L, 2147483647L, 2147483648L, 549755813887L, 549755813888L, Long.MAX_VALUE};
        for(int i=0;i<decimals.length;i++) emit("decimal_"+i, new DataOutputX().writeDecimal(decimals[i]));
        DataOutputX primitives = new DataOutputX();
        primitives.writeBoolean(true).writeBoolean(false).writeByte(-7).writeShort(-32768).writeInt(-1234567)
            .writeLong(Long.MIN_VALUE+7).writeFloat(-0.0f).writeFloat(Float.NaN).writeDouble(Double.POSITIVE_INFINITY)
            .writeInt3(-8388608).writeLong5(-549755813888L);
        emit("primitives", primitives);
        for(int length : new int[] {0, 1, 253, 254, 65535, 65536}) {
            byte[] bytes=new byte[length]; for(int i=0;i<length;i++) bytes[i]=(byte)(i*31);
            emit("blob_"+length, new DataOutputX().writeBlob(bytes));
        }
        emit("text", new DataOutputX().writeText((String)null).writeText("").writeText("합성 fixture 😀"));
        MapValue all = values();
        emit("values", new DataOutputX().writeValue(all));
        MapPack map = new MapPack(); map.put("z", all); map.put("a", new DecimalValue(-7)); pack("map", map);
        ObjectPack object = new ObjectPack(); object.objType="javaee"; object.objHash=-1234567; object.objName="/synthetic/app";
        object.address="192.0.2.7"; object.version="synthetic-2.21.3"; object.alive=true; object.wakeup=1700000000123L;
        object.tags=all; pack("object", object);
        PerfCounterPack counter = new PerfCounterPack(); counter.time=1700000000123L; counter.objName=object.objName;
        counter.timetype=1; counter.data=all; pack("counter", counter);
        TextPack text = new TextPack(); text.xtype="service"; text.hash=-1234567; text.text="synthetic service 한글"; pack("textpack", text);
        XLogPack x=xlog(); pack("xlog",x); legacyXLogs(x);
        XLogProfilePack profile=new XLogProfilePack(); profile.time=x.endTime; profile.objHash=x.objHash;
        profile.service=x.service; profile.txid=x.txid; profile.profile=new byte[] {0,1,(byte)255,2,3}; pack("profile",profile);
        XLogProfilePack2 profile2=new XLogProfilePack2(); profile2.time=x.endTime; profile2.objHash=x.objHash;
        profile2.service=x.service; profile2.txid=x.txid; profile2.profile=profile.profile; profile2.gxid=x.gxid;
        profile2.xType=2; profile2.discardType=1; profile2.ignoreGlobalConsequentSampling=true; pack("profile2",profile2);
    }
}
