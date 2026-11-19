// Synthetic oracle from Scouter common@dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5.
// Copyright 2015 the original author or authors, Apache License 2.0.
// https://github.com/scouter-project/scouter; no production data is loaded.
import java.nio.file.*;
import java.lang.reflect.*;
import java.util.*;
import scouter.io.*;
import scouter.lang.pack.*;
import scouter.lang.value.*;

public final class PackOracle {
    private static Path output;
    private static final long TIME = 1700000000123L;
    private static MapValue map() {
        MapValue v = new MapValue();
        v.put("hash", new DecimalValue(-1234567));
        v.put("text", new TextValue("합성"));
        v.put("blob", new BlobValue(new byte[] {0,1,(byte)255}));
        return v;
    }
    // Compare public wire-bearing fields semantically. Hashtable/HashMap iteration
    // order is not an oracle for insertion order or byte-stable Java reencoding.
    private static void same(Object a, Object b) throws Exception {
        if (a == null || b == null) { if (a != b) throw new AssertionError("null differs"); return; }
        if (a instanceof MapValue) { same(((MapValue)a).toMap(), ((MapValue)b).toMap()); return; }
        if (a instanceof ListValue) {
            ListValue x=(ListValue)a,y=(ListValue)b;
            if (x.size()!=y.size()) throw new AssertionError("list size");
            for(int i=0;i<x.size();i++) same(x.get(i),y.get(i));
            return;
        }
        if (a instanceof Value) {
            if (!Arrays.equals(new DataOutputX().writeValue((Value)a).toByteArray(),new DataOutputX().writeValue((Value)b).toByteArray())) throw new AssertionError("value differs");
            return;
        }
        if (a instanceof Map) {
            Map<?,?> x=(Map<?,?>)a,y=(Map<?,?>)b;
            if (!x.keySet().equals(y.keySet())) throw new AssertionError("map keys");
            for(Object k:x.keySet()) same(x.get(k),y.get(k));
            return;
        }
        if (a instanceof List) {
            List<?> x=(List<?>)a,y=(List<?>)b;
            if (x.size()!=y.size()) throw new AssertionError("list size");
            for(int i=0;i<x.size();i++) same(x.get(i),y.get(i));
            return;
        }
        if (a instanceof byte[]) { if (!Arrays.equals((byte[])a,(byte[])b)) throw new AssertionError("bytes differ"); return; }
        if (a instanceof Pack) {
            if (a.getClass()!=b.getClass()) throw new AssertionError("pack class");
            for(Field f:a.getClass().getFields()) {
                if (Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers())) continue;
                same(f.get(a), f.get(b));
            }
            return;
        }
        if (!a.equals(b)) throw new AssertionError("field differs: "+a+" / "+b);
    }
    private static void emit(String name, Pack p) throws Exception {
        byte[] raw=new DataOutputX().writePack(p).toByteArray();
        Pack parsed=new DataInputX(raw).readPack();
        same(p,parsed);
        Files.write(output.resolve("pack-extra-"+name+".bin"),raw);
    }
    private static SpanPack span() {
        SpanPack p=new SpanPack();
        p.gxid=Long.MAX_VALUE-9;p.txid=Long.MIN_VALUE+7;p.caller=0x1122334455667788L;p.timestamp=TIME;
        p.elapsed=128;p.spanType=2;p.name=-32769;p.objHash=-1234567;p.error=32768;
        p.localEndpointServiceName=-8388608;p.localEndpointIp=new byte[]{(byte)192,0,2,7};p.localEndpointPort=(short)65535;
        p.remoteEndpointServiceName=8388607;p.remoteEndpointIp=new byte[]{0x20,1,0x0d,(byte)0xb8,0,0,0,0,0,0,0,0,0,0,0,1};p.remoteEndpointPort=443;
        p.debug=true;p.shared=false;
        p.annotationTimestamps=new ListValue();p.annotationTimestamps.add(new DecimalValue(TIME));p.annotationTimestamps.add(new DecimalValue(TIME+1));
        p.annotationValues=new ListValue();p.annotationValues.add(new TextValue("start"));p.annotationValues.add(new TextValue("끝"));p.tags=map();
        return p;
    }
    private static BatchPack batch(int count) {
        BatchPack p=new BatchPack();p.objHash=-1234567;p.objName="/synthetic/batch";p.objType="batch";p.batchJobId="합성-job";p.args="synthetic";p.pID=-123;
        p.startTime=TIME;p.elapsedTime=2147483648L;p.threadCnt=3;p.cpuTime=4;p.gcTime=5;p.gcCount=6;
        p.sqlTotalCnt=count;p.sqlTotalTime=7;p.sqlTotalRows=8;p.sqlTotalRuns=9;p.isStack=true;p.position=Long.MAX_VALUE-7;
        if(count>0) {
            p.sqlStats=new ArrayList<>();p.uniqueSqls=new HashMap<>();
            for(int i=0;i<count;i++) {
                MapValue m=new MapValue();m.put("hashValue",-1234567+i);m.put("runs",2+i);m.put("startTime",TIME+i);m.put("endTime",TIME+10+i);
                m.put("totalTime",10L+i);m.put("minTime",1L+i);m.put("maxTime",9L+i);m.put("processedRows",20L+i);m.put("rowed",new BooleanValue(i==0));p.sqlStats.add(m);
                p.uniqueSqls.put(-1234567+i,"synthetic SQL "+i);
            }
        }
        return p;
    }
    public static void main(String[] args) throws Exception {
        output=Paths.get(args[0]);
        emit("dropped",new DroppedXLogPack(Long.MAX_VALUE-9,Long.MIN_VALUE+7));
        SpanPack s=span();emit("span",s);
        SpanContainerPack c=new SpanContainerPack();c.gxid=s.gxid;c.spanCount=2;c.timestamp=TIME;c.spans=SpanPack.toBytes(new SpanPack[]{s,s});emit("span-container",c);
        SpanContainerPack empty=new SpanContainerPack();empty.gxid=1;empty.spans=new byte[0];emit("span-container-empty",empty);
        StatusPack status=new StatusPack();status.time=TIME;status.objType="javaee";status.objHash=-1234567;status.key="synthetic-status";status.data=map();emit("status",status);
        StackPack stack=new StackPack();stack.time=TIME;stack.objHash=-1234567;stack.data=new byte[]{0,1,(byte)255,2};emit("stack",stack);
        StackPack stackEmpty=new StackPack();stackEmpty.data=new byte[0];emit("stack-empty",stackEmpty);
        SummaryPack summary=new SummaryPack();summary.time=TIME;summary.objHash=-1234567;summary.objType="javaee";summary.stype=3;summary.table=map();emit("summary",summary);
        emit("batch",batch(2));emit("batch-zero",batch(0));
        InteractionPerfCounterPack i=new InteractionPerfCounterPack();i.time=TIME;i.objName="/synthetic/app";i.interactionType="synthetic-api";i.fromHash=-1234567;i.toHash=7654321;i.period=3000;i.count=4;i.errorCount=1;i.totalElapsed=2147483648L;i.customData=map();emit("interaction",i);
        AlertPack a=new AlertPack();a.time=TIME;a.level=2;a.objType="javaee";a.objHash=-1234567;a.title="synthetic alert";a.message="합성 message";a.tags=map();emit("alert",a);
    }
}
