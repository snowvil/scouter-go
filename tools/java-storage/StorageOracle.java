/* Synthetic fixture only. Actual DB/codec/hash/date/gzip classes are unmodified
 * Scouter Apache-2.0 source dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5.
 * Configuration, logging, lifecycle and root/path routing are isolated stubs. */
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.lang.reflect.*;
import scouter.io.*;
import scouter.lang.*;
import scouter.lang.pack.*;
import scouter.lang.value.*;
import scouter.server.Configure;
import scouter.server.db.obj.*;
import scouter.server.db.text.*;
import scouter.server.db.xlog.*;
import scouter.server.db.counter.*;
import scouter.server.db.io.zip.*;
import scouter.server.db.io.IndexTimeFile;
import scouter.util.*;

public class StorageOracle {
    static final String DATE="20260101", OBJ="/oracle/app";
    static final long TIME=1767193200125L;
    static void require(boolean value,String message){if(!value)throw new AssertionError(message);}
    static String file(Path root,String dir,String name)throws Exception{Path path=root.resolve(DATE).resolve(dir);Files.createDirectories(path);return path.resolve(name).toString();}
    static byte[] objectBytes(String name)throws Exception {
        ObjectPack p=new ObjectPack();p.objType="java";p.objHash=42;p.objName=name;p.address="127.0.0.1";p.version="oracle";p.wakeup=TIME;p.tags.put("region","synthetic");
        return Arrays.copyOf(new DataOutputX().writePack(p).toByteArray(),4096);
    }
    static XLogPack xlog(int n){XLogPack p=new XLogPack();p.txid=101+n;p.endTime=TIME+(n==0?200:n==1?100:550);p.gxid=n<2?9001:0;p.objHash=42;p.service=7;p.elapsed=10;return p;}
    static Value scalar(String prefix){switch(prefix){case "b":return new BooleanValue(true);case "f":return new FloatValue(1.25f);case "d":return new DoubleValue(2.5);default:return new DecimalValue(-9223372036854775807L);}}
    static void generate(Path root,boolean zipped)throws Exception{
        System.setProperty("oracle.root",root.toString());Configure.getInstance().compress_xlog_enabled=zipped;Configure.getInstance().compress_profile_enabled=zipped;
        String object=file(root,"object","obj");ObjectData od=new ObjectData(object);ObjectIndex oi=new ObjectIndex(object);
        long position=od.write(objectBytes("oracle-first"));oi.set(42,position);od.update(position,objectBytes("oracle-last"));od.close();oi.close();
        String perm=root.resolve("00000000/text/text_service").toString();Files.createDirectories(Paths.get(perm).getParent());TextPermData td=new TextPermData("service",perm);TextPermIndex ti=new TextPermIndex("service",perm);
        ti.set(7,td.write("첫 서비스".getBytes("UTF-8")));td.close();ti.close();
        TextTable daily=new TextTable(file(root,"text","text"));daily.set("error",8,"합성 오류".getBytes("UTF-8"));daily.set("error",9,new byte[0]);daily.close();
        String xfile=file(root,"xlog","xlog");XLogDataWriter xw=new XLogDataWriter(DATE,xfile);XLogIndex xi=new XLogIndex(xfile);
        for(int n=0;n<3;n++){XLogPack p=xlog(n);long pos=xw.write(new DataOutputX().writePack(p).toByteArray());xi.setByTxid(p.txid,pos);xi.setByGxid(p.gxid,pos);xi.setByTime(p.endTime,pos);}xw.close();xi.close();
        XLogProfileDataWriter pw=new XLogProfileDataWriter(DATE,xfile);XLogProfileIndex pi=new XLogProfileIndex(xfile);for(byte[] chunk:new byte[][]{{1,2},{3,4}})pi.addByTxid(101,pw.write(chunk));pw.close();pi.close();
        String counter=file(root,"counter","5m");DailyCounterData cd=new DailyCounterData(counter,"rw");DailyCounterIndex ci=new DailyCounterIndex(counter);
        for(byte kind=2;kind<=6;kind++)for(String prefix:new String[]{"b","f","d","n"}){CounterKey key=new CounterKey(HashUtil.hash(OBJ),prefix+kind,kind);ci.set(key.getBytesKey(),cd.writeNew(key,0,scalar(prefix)));}cd.close();ci.close();
        String real=file(root,"counter","real");RealtimeCounterDBHeader rh=new RealtimeCounterDBHeader(real);RealtimeCounterData rd=new RealtimeCounterData(real);RealtimeCounterIndex ri=new RealtimeCounterIndex(real);
        rh.intern("cpu",true);rh.intern("errors",true);
        for(int n=0;n<2;n++){MapValue values=new MapValue();values.put("cpu",new DoubleValue(12.5+n));values.put("errors",new DecimalValue(7+n));long pos=rd.write(RealtimeCounterDBHelper.getTagBytes(rh.getTagStrInt(),values));ri.write(HashUtil.hash(OBJ),TIME+n*500,pos);}rd.close();ri.close();rh.close();
        if(zipped)GZipStore.getInstance().shutdown();
    }
    static void verify(Path root,boolean zipped)throws Exception{
        System.setProperty("oracle.root",root.toString());Configure.getInstance().compress_xlog_enabled=!zipped;Configure.getInstance().compress_profile_enabled=!zipped;
        String object=file(root,"object","obj");ObjectData od=new ObjectData(object);ObjectIndex oi=new ObjectIndex(object);
        ObjectPack op=(ObjectPack)new DataInputX(od.read(oi.get(42))).readPack();require(op.objName.equals("oracle-last")&&op.objHash==42&&op.tags.getText("region").equals("synthetic"),"object");require(Files.size(Paths.get(object+".data"))==4100,"fixed object slot");od.close();oi.close();
        String perm=root.resolve("00000000/text/text_service").toString();TextPermData td=new TextPermData("service",perm);TextPermIndex ti=new TextPermIndex("service",perm);require(new String(td.read(ti.get(7)),"UTF-8").equals("첫 서비스"),"permanent text");td.close();ti.close();
        TextTable daily=new TextTable(file(root,"text","text"));require(new String(daily.get("error",8),"UTF-8").equals("합성 오류"),"daily text");require(daily.get("error",9).length==0,"empty daily text");daily.close();
        String xfile=file(root,"xlog","xlog");XLogDataReader xr=new XLogDataReader(DATE,xfile);XLogIndex xi=new XLogIndex(xfile);
        for(int n=0;n<3;n++){XLogPack want=xlog(n);XLogPack got=(XLogPack)new DataInputX(xr.read(xi.getByTxid(want.txid))).readPack();require(got.txid==want.txid&&got.endTime==want.endTime&&got.gxid==want.gxid&&got.objHash==42&&got.service==7&&got.elapsed==10,"xlog txid");}
        List<?> gpositions=xi.getByGxid(9001);require(gpositions.size()==2,"gxid count");require(((XLogPack)new DataInputX(xr.read(((Number)gpositions.get(0)).longValue())).readPack()).txid==102,"gxid latest first");
        final List<Long> txids=new ArrayList<Long>();IndexTimeFile timeIndex=new IndexTimeFile(xfile+"_tim");timeIndex.read(TIME,TIME+1000,new scala.runtime.AbstractFunction2<Object,byte[],Object>(){public Object apply(Object time,byte[] pos){try{txids.add(((XLogPack)new DataInputX(xr.read(DataInputX.toLong5(pos,0))).readPack()).txid);return null;}catch(Exception e){throw new RuntimeException(e);}}});require(txids.equals(Arrays.asList(102L,101L,103L)),"time V1 order "+txids);timeIndex.close();xr.close();xi.close();
        XLogProfileDataReader pr=new XLogProfileDataReader(DATE,xfile);XLogProfileIndex pi=new XLogProfileIndex(xfile);List<?> positions=pi.getByTxid(101);require(positions.size()==2,"profile chunk count");for(int n=0;n<2;n++)require(Arrays.equals(pr.read(((Number)positions.get(n)).longValue()),new byte[]{(byte)(n*2+1),(byte)(n*2+2)}),"profile chunk order");pr.close();pi.close();
        String counter=file(root,"counter","5m");DailyCounterData cd=new DailyCounterData(counter,"r");DailyCounterIndex ci=new DailyCounterIndex(counter);
        for(byte kind=2;kind<=6;kind++)for(String prefix:new String[]{"b","f","d","n"}){CounterKey key=new CounterKey(HashUtil.hash(OBJ),prefix+kind,kind);long pos=ci.get(key.getBytesKey());require(pos>=0,"daily counter index");require(cd.getValue(pos,0).toJavaObject().equals(scalar(prefix).toJavaObject()),"daily value "+prefix+kind);require(cd.getValues(pos).length==DailyCounterUtils.getBucketCount(kind),"daily bucket shape");}cd.close();ci.close();
        String real=file(root,"counter","real");final RealtimeCounterDBHeader rh=new RealtimeCounterDBHeader(real);final RealtimeCounterData rd=new RealtimeCounterData(real);RealtimeCounterIndex ri=new RealtimeCounterIndex(real);final List<Long> timestamps=new ArrayList<Long>();
        ri.read(HashUtil.hash(OBJ),TIME,TIME+1000,new scala.runtime.AbstractFunction2<Object,MapValue,Object>(){public Object apply(Object time,MapValue values){int n=timestamps.size();timestamps.add(((Number)time).longValue());require(((DoubleValue)values.get("cpu")).value==12.5+n,"realtime double");require(((DecimalValue)values.get("errors")).value==7+n,"realtime decimal");return null;}},rh.getTagIntStr(),new scala.runtime.AbstractFunction1<Object,byte[]>(){public byte[] apply(Object pos){return rd.read(((Number)pos).longValue());}});
        require(timestamps.equals(Arrays.asList(TIME,TIME+500)),"realtime range "+timestamps);rd.close();ri.close();rh.close();if(zipped)GZipStore.getInstance().shutdown();
    }
    static void finishGzip()throws Exception{
        // Original IOChannel flush schedules async workers. Drain the real worker pool
        // before a fixture is considered complete; no DB implementation is replaced.
        Field field=IOChannel.class.getDeclaredField("exec");field.setAccessible(true);ExecutorService pool=(ExecutorService)field.get(null);pool.shutdown();require(pool.awaitTermination(30,TimeUnit.SECONDS),"gzip async flush timeout");
    }
    public static void main(String[] args)throws Exception{
        String mode=args[0];Path root=Paths.get(args[1]).toAbsolutePath();
        try{for(boolean zipped:new boolean[]{false,true}){Path dir=root.resolve(zipped?"gzip":"plain");if(mode.equals("generate"))generate(dir,zipped);else verify(dir,zipped);}finishGzip();System.out.println("PASS actual Java/Scala Storage "+mode+" source=dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5");}
        catch(Throwable e){e.printStackTrace();System.exit(1);}
    }
}
