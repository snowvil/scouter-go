// Original MariaService/CubridService and actual StatusRD/index/reader execution.
// Pinned Apache-2.0 source is unchanged. Cache inputs/root routing are explicit seams.
import java.nio.file.*;
import java.util.*;
import scouter.io.*;
import scouter.lang.*;
import scouter.lang.constants.StatusConstants;
import scouter.lang.pack.*;
import scouter.lang.value.*;
import scouter.net.TcpFlag;
import scouter.server.core.cache.CounterCache;
import scouter.server.core.cache.StatusCache;
import scouter.server.db.status.*;
import scouter.server.netio.service.handle.*;

public final class DBHandlerOracle {
    interface Handler{void run(DataInputX in,DataOutputX out);}
    static void require(boolean v,String s){if(!v)throw new AssertionError(s);}
    static byte[] bytes(Pack p)throws Exception{return new DataOutputX().writePack(p).toByteArray();}
    static MapPack request(){MapPack p=new MapPack();p.newList("objHash").add(42);p.getList("objHash").add(43);p.put("date",ExtraDataset.DATE);p.put("stime",ExtraDataset.TIME-1);p.put("etime",ExtraDataset.TIME+501);p.put("key","synthetic-querylist");return p;}
    static byte[] run(Path root,String name,MapPack request,Handler handler)throws Exception {
        byte[] input=bytes(request);DataOutputX out=new DataOutputX();handler.run(new DataInputX(input),out);byte[] response=out.toByteArray();
        Files.write(root.resolve("db-"+name+"-request.bin"),input);Files.write(root.resolve("db-"+name+"-response.bin"),response);return response;
    }
    static Value value(byte[] raw)throws Exception{DataInputX in=new DataInputX(raw);require(in.readByte()==TcpFlag.HasNEXT,"Value HasNEXT");return in.readValue();}
    static List<Pack> packs(byte[] raw)throws Exception{DataInputX in=new DataInputX(raw);List<Pack> out=new ArrayList<>();while(in.available()>0){require(in.readByte()==TcpFlag.HasNEXT,"Pack HasNEXT");out.add(in.readPack());}return out;}
    static void counter(int hash,String key,long n){CounterCache.values.put(new CounterKey(hash,key,TimeTypeEnum.REALTIME),new DecimalValue(n));}
    static void cacheInputs(Path root)throws Exception {
        String[] activity={"COM_CALL_PROCEDURE","COM_SELECT","COM_UPDATE","COM_INSERT","COM_DELETE"};String[] dml={"num_query_selects","num_query_updates","num_query_inserts","num_query_deletes"};
        for(int h=42;h<=43;h++){int scale=h-41;counter(h,"THREADS_TOTAL",10*scale);counter(h,"THREADS_RUNNING",scale);for(int i=0;i<activity.length;i++)counter(h,activity[i],(i+1)*scale);for(int i=0;i<dml.length;i++)counter(h,dml[i],(i+1)*scale);counter(h,"db_num",2*scale);}
        counter(42,"INNO_BUF_HIT",80);counter(43,"INNO_BUF_HIT",100);counter(42,"KEY_RATIO",50);counter(42,"QCACHE_RATIO",0);counter(42,"TCACHE_RATIO",25);
        for(int h=42;h<=43;h++){PerfCounterPack p=new PerfCounterPack();p.time=ExtraDataset.TIME;p.objName="/oracle/db-"+h;p.timetype=TimeTypeEnum.REALTIME;for(Map.Entry<CounterKey,Value> e:CounterCache.values.entrySet())if(e.getKey().objHash==h)p.put(e.getKey().counter,e.getValue());Files.write(root.resolve("db-counter-"+h+"-input.bin"),bytes(p));}
        StatusPack p=new StatusPack();p.objHash=42;p.objType="mariadb";p.key=StatusConstants.EVENTS_STATEMENTS_SUMMARY_BY_DIGEST;p.time=System.currentTimeMillis();
        p.data.newList("AVG_TIMER_WAIT").add(2.5);p.data.getList("AVG_TIMER_WAIT").add(7.5);p.data.newList("COUNT_STAR").add(2);p.data.getList("COUNT_STAR").add(1);
        p.data.newList("LAST_SEEN").add(p.time);p.data.getList("LAST_SEEN").add(p.time);StatusCache.value=p;Files.write(root.resolve("db-response-time-input-status.bin"),bytes(p));
    }
    static StatusPack digest(long t,long min,long max,long avg){StatusPack p=new StatusPack();p.time=t;p.objHash=42;p.objType="mariadb";p.key=StatusConstants.EVENTS_STATEMENTS_SUMMARY_BY_DIGEST;p.data.newList("DIGEST_TEXT").add(7);p.data.newList("MIN_TIMER_WAIT").add(min);p.data.newList("MAX_TIMER_WAIT").add(max);p.data.newList("AVG_TIMER_WAIT").add(avg);p.data.newList("LAST_SEEN").add(t);return p;}
    static void storedInputs(Path root)throws Exception {
        System.setProperty("oracle.root",root.toAbsolutePath().toString());Path directory=root.resolve(ExtraDataset.DATE).resolve("status");Files.createDirectories(directory);String file=directory.resolve("status").toString();
        StatusWriter writer=new StatusWriter(file);StatusIndex index=new StatusIndex(file);
        StatusPack[] records={digest(ExtraDataset.TIME,10,40,20),digest(ExtraDataset.TIME+100,5,60,30),ExtraDataset.status(ExtraDataset.TIME+200,42),ExtraDataset.status(ExtraDataset.TIME+300,43)};
        records[2].key="synthetic-querylist";records[3].key="synthetic-querylist";
        for(int i=0;i<records.length;i++){StatusPack p=records[i];byte[] raw=bytes(p);index.add(p.time,writer.write(raw));Files.write(root.resolve("db-status-"+i+"-input.bin"),raw);}writer.close();index.close();
    }
    public static void main(String[] args)throws Exception {
        Path root=Paths.get(args[1]);Files.createDirectories(root);cacheInputs(root);storedInputs(root);MariaService maria=new MariaService();CubridService cubrid=new CubridService();MapPack req=request();
        MapValue connections=(MapValue)value(run(root,"connections",req,(in,out)->maria.realtimeConnections(in,out,true)));require(connections.getLong("total")==30&&connections.getLong("active")==3,"Connections MapValue aggregation");
        MapValue activity=(MapValue)value(run(root,"activity",req,(in,out)->maria.realtimeActivity(in,out,true)));require(activity.getLong("call")==3&&activity.getLong("select")==6&&activity.getLong("delete")==15,"Activity MapValue");
        MapValue dml=(MapValue)value(run(root,"cubrid-dml",req,(in,out)->cubrid.realtimeDML(in,out,true)));require(dml.getLong("select")==3&&dml.getLong("delete")==12&&"24".equals(dml.getText("dbname")),"Cubrid dbname decimal string concatenation");
        MapPack hit=(MapPack)packs(run(root,"hit-ratio",req,(in,out)->maria.realtimeHitRatio(in,out,true))).get(0);require(hit.get("innodb_buffer") instanceof FloatValue&&hit.getFloat("innodb_buffer")==90&&hit.getFloat("query_cache")==0,"Hit MapPack FloatValue members");
        List<Pack> response=packs(run(root,"response-time",req,(in,out)->maria.realtimeResponseTime(in,out,true)));require(response.size()==1,"Only cached object response");MapPack mean=(MapPack)response.get(0);require(mean.getInt("objHash")==42&&mean.get("value") instanceof DoubleValue&&((DoubleValue)mean.get("value")).value==12.5/3,"Response MapPack weighted DoubleValue");
        List<Pack> digest=packs(run(root,"digest-table",req,(in,out)->maria.digestTable(in,out,true)));require(digest.size()==4&&digest.get(0) instanceof MapPack&&((MapPack)digest.get(0)).containsKey("percent")&&digest.get(3) instanceof StatusPack,"Digest progress, summary, latest Status order");MapPack sum=(MapPack)digest.get(2);require(sum.get("AVG_TIMER_WAIT") instanceof DecimalValue&&sum.getLong("AVG_TIMER_WAIT")==50&&sum.getLong("MIN_TIMER_WAIT")==5&&sum.getLong("MAX_TIMER_WAIT")==60&&sum.getInt("count")==2,"Digest snapshot Decimal AVG sum");
        for(String command:Arrays.asList("cubrid-status","cubrid-server-info")){List<Pack> selected=packs(run(root,command,req,(in,out)->{if(command.equals("cubrid-status"))cubrid.realtimeDMLStatus(in,out,true);else cubrid.DbServerInfo(in,out,true);}));require(selected.size()==2,"Actual StatusRD catches nonlocal return, allowing outer repetition");for(Pack raw:selected){StatusPack p=(StatusPack)raw;require(p.objHash==43&&p.time==ExtraDataset.TIME+300,"Each loop repeats reverse first matching status");}}
        Files.write(root.resolve("db-observations.json"),("{\"cubrid_status_repeated_hashes\":[43,43],\"cubrid_status_repeated_times\":[1767193200425,1767193200425],\"digest_avg_decimal_sum\":50,\"connections_response\":\"MapValue\",\"hit_response\":\"MapPack(FloatValue)\",\"response_time\":\"MapPack(DoubleValue)\"}\n").getBytes("UTF-8"));
        System.out.println("PASS actual Maria/Cubrid/StatusRD: MapValue/Float/Double/Decimal digest, caught nonlocal return repeated newest status");
    }
}
