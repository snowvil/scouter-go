// Independent synthetic inputs through pinned original Scala handler/common Value code.
// Original Scouter source is Apache-2.0; no handler or serialization replica is used.
import java.nio.file.*;
import java.util.*;
import scouter.io.*;
import scouter.lang.pack.*;
import scouter.lang.value.*;
import scouter.net.TcpFlag;
import scouter.server.db.DailyCounterRD;
import scouter.server.db.HandlerOracleInput;
import scouter.server.netio.service.handle.CounterService;
import scouter.server.netio.service.handle.DailyCounterService;
import scouter.server.util.TimedSeries;

public final class CounterHandlerOracle {
    static void require(boolean v,String s){if(!v)throw new AssertionError(s);}
    static void today(Path root,String label,int[] hashes,Value[] input,double expected)throws Exception {
        HandlerOracleInput.hashes=hashes;DailyCounterRD.values=input;
        MapPack req=new MapPack();req.put("counter","cpu");req.put("objType","host");req.put("mode","avg");
        byte[] request=new DataOutputX().writePack(req).toByteArray();DataOutputX out=new DataOutputX();
        new DailyCounterService().getRealDateTotal(new DataInputX(request),out,true);
        byte[] response=out.toByteArray();DataInputX in=new DataInputX(response);
        require(in.readByte()==TcpFlag.HasNEXT,"HasNEXT");MapPack p=in.readMapPack();
        require(p.getList("time").size()==1&&p.getList("value").size()==1,"Original singleton array");
        require(((DoubleValue)p.getList("value").get(0)).value==expected,"Original singleton initial288/count288");
        Files.write(root.resolve("today-tot-"+label+"-request.bin"),request);
        Files.write(root.resolve("today-tot-"+label+"-response.bin"),response);
    }
    static void past(Path root,String name,long[] times)throws Exception {
        HandlerOracleInput.hashes=new int[]{42};HandlerOracleInput.times=times;HandlerOracleInput.counters=new MapValue[times.length];
        for(int i=0;i<times.length;i++){MapValue v=new MapValue();v.put("cpu",new DoubleValue(12+i*12));HandlerOracleInput.counters[i]=v;}
        MapPack req=new MapPack();req.put("stime",1000L);req.put("etime",3000L);req.put("counter","cpu");req.put("objType","host");req.put("mode","avg");
        byte[] request=new DataOutputX().writePack(req).toByteArray();DataOutputX out=new DataOutputX();String failure="";
        try{new CounterService().getPastTimeTotal(new DataInputX(request),out,true);}catch(Throwable e){failure=e.getClass().getName();}
        byte[] response=out.toByteArray();
        if(times.length==0){require(failure.isEmpty(),"Empty handler must succeed");DataInputX in=new DataInputX(response);require(in.readByte()==TcpFlag.HasNEXT,"Past HasNEXT");MapPack p=in.readMapPack();require(p.getList("time").size()==0&&p.getList("value").size()==0,"Past empty lists");}
        else require(failure.equals("java.lang.ClassCastException")&&response.length==0,"Nonempty handler fails before any response bytes");
        Files.write(root.resolve("past-tot-"+name+"-request.bin"),request);Files.write(root.resolve("past-tot-"+name+"-response.bin"),response);
        Files.write(root.resolve("past-tot-"+name+"-observation.json"),("{\"failure\":\""+failure+"\",\"response_bytes\":"+response.length+"}\n").getBytes("UTF-8"));
    }
    static void group(Path root,boolean metadata)throws Exception {
        HandlerOracleInput.counters=new MapValue[]{new MapValue()};MapValue row=HandlerOracleInput.counters[0];row.put("cpu",new DoubleValue(12));
        if(metadata){row.put("objHash",42);row.put("time",1000L);}
        MapPack req=new MapPack();req.put("stime",1000L);req.put("etime",3000L);req.put("counter","cpu");req.newList("objHash").add(42);
        byte[] request=new DataOutputX().writePack(req).toByteArray();DataOutputX out=new DataOutputX();new CounterService().getPastTimeGroupAll(new DataInputX(request),out,true);
        byte[] response=out.toByteArray();DataInputX in=new DataInputX(response);require(in.readByte()==TcpFlag.HasNEXT,"Group HasNEXT");MapPack p=in.readMapPack();
        require(p.getInt("objHash")==42&&p.getList("time").size()==(metadata?1:0)&&p.getList("value").size()==(metadata?1:0),"Bulk embedded metadata only");
        if(metadata)require(p.getList("time").getLong(0)==1000&&p.getList("value").getDouble(0)==12,"Bulk embedded metadata values");
        String name="past-group-"+(metadata?"metadata":"plain");Files.write(root.resolve(name+"-request.bin"),request);Files.write(root.resolve(name+"-response.bin"),response);
        Files.write(root.resolve(name+"-row.bin"),new DataOutputX().writeValue(row).toByteArray());
    }
    public static void main(String[] args)throws Exception {
        Path root=Paths.get(args[1]);Files.createDirectories(root);
        today(root,"empty",new int[0],null,1.0);
        today(root,"zero",new int[]{42},new Value[]{new DoubleValue(0),new DoubleValue(0)},1.0);
        today(root,"one-positive",new int[]{42},new Value[]{new DoubleValue(12)},300.0/289.0);
        // The original catches index1 access and still serializes the already accumulated index0.
        today(root,"two-positive",new int[]{42},new Value[]{new DoubleValue(12),new DoubleValue(24)},300.0/289.0);
        TimedSeries<Integer,Double> empty=new TimedSeries<>();empty.addEnd();
        require(empty.getMinTime()==Long.MAX_VALUE&&empty.getMaxTime()==Long.MIN_VALUE,"Empty TimedSeries bounds");
        String seriesFailure="";
        try{TimedSeries<Integer,Double> series=new TimedSeries<>();series.add(42,1000L,12.0);series.add(42,3000L,24.0);series.addEnd();}
        catch(Throwable e){seriesFailure=e.getClass().getName();}
        require(seriesFailure.equals("java.lang.ClassCastException"),"Actual Arrays.sort Unit cast must fail");
        String doubleFailure="";try{new DoubleValue(1.25).add(new DoubleValue(2.5));}catch(Throwable e){doubleFailure=e.getClass().getName();}
        require(doubleFailure.equals("java.lang.ClassCastException"),"Original DoubleValue addition cast");
        require(((DecimalValue)new DecimalValue(1).add(new DoubleValue(2.5))).value==3,"Original Decimal truncation");
        require(((FloatValue)new FloatValue(1.25f).add(new DoubleValue(2.5))).value==3.75f,"Original Float addition");
        Files.write(root.resolve("counter-handler-observations.json"),("{\"today_tot\":{\"empty\":1.0,\"zero\":1.0,\"one_positive\":1.0380622837370241,\"two_positive_partial\":1.0380622837370241,\"list_length\":1},\"nonempty_timed_series\":\""+seriesFailure+"\",\"double_add\":\""+doubleFailure+"\",\"empty_timed_series_min\":9223372036854775807,\"empty_timed_series_max\":-9223372036854775808}\n").getBytes("UTF-8"));
        past(root,"empty",new long[0]);past(root,"two-points",new long[]{1000,3000});
        group(root,false);group(root,true);
        SummaryHandlerOracle.generate(root);
        System.out.println("PASS actual DailyCounterService/TimedSeries/NumberValue contracts: singleton, caught partial, nonempty ClassCastException");
    }
}
