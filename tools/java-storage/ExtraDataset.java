// Synthetic common Pack inputs. Scouter original codec is Apache-2.0 @ pinned baseline.
import java.util.*;
import scouter.lang.*;
import scouter.lang.pack.*;
import scouter.lang.value.*;

public final class ExtraDataset {
    public static final String DATE="20260101", OBJECT="/oracle/app";
    public static final long TIME=1767193200125L;
    public static StatusPack status(long time,int hash) {
        StatusPack p=new StatusPack();p.time=time;p.objHash=hash;p.objType="host";p.key="synthetic-status";
        p.data.put("state","synthetic");return p;
    }
    public static SummaryPack summary(long time,int hash) {
        SummaryPack p=new SummaryPack();p.time=time;p.objHash=hash;p.objType="host";p.stype=SummaryEnum.APP;
        String[] keys={"id","count","error","elapsed","cpu","mem"};long[] values={7,3,1,90,12,1024};
        for(int i=0;i<keys.length;i++)p.table.newList(keys[i]).add(values[i]);return p;
    }
    public static AlertPack alert(long time,int hash) {
        AlertPack p=new AlertPack();p.time=time;p.objHash=hash;p.objType="host";p.level=2;
        p.title="synthetic alert";p.message="synthetic message";p.tags.put("region","synthetic");return p;
    }
    public static StackPack stack(long time,int hash,int number) {
        StackPack p=new StackPack();p.time=time;p.objHash=hash;p.setStack("synthetic stack\nframe-"+(number==1?"one":"two")+"\n");return p;
    }
    public static BatchPack batch(long time,int hash,String object,int number) {
        BatchPack p=new BatchPack();p.startTime=time;p.objHash=hash;p.objName=object;p.objType="batch";
        p.batchJobId="synthetic-job-"+number;p.args="synthetic";p.pID=number+6;p.elapsedTime=number==1?25:50;
        p.threadCnt=2;p.cpuTime=5;p.gcTime=6;p.gcCount=1;p.sqlTotalCnt=1;p.sqlTotalTime=20;p.sqlTotalRows=10;p.sqlTotalRuns=2;
        p.sqlStats=new ArrayList<>();MapValue sql=new MapValue();sql.put("hashValue",7);sql.put("runs",2);
        sql.put("startTime",time);sql.put("endTime",time+p.elapsedTime);sql.put("totalTime",20);sql.put("minTime",5);
        sql.put("maxTime",15);sql.put("processedRows",10);sql.put("rowed",new BooleanValue(true));p.sqlStats.add(sql);
        p.uniqueSqls=new HashMap<>();p.uniqueSqls.put(7,"synthetic SQL");return p;
    }
    private static SpanPack span(long gid,long id,long caller,long timestamp,byte kind,int hash,boolean simple) {
        SpanPack p=new SpanPack();p.gxid=gid;p.txid=id;p.caller=caller;p.timestamp=timestamp;p.spanType=kind;
        p.objHash=hash;p.name=simple?(int)id:7;p.elapsed=25;p.tags=new MapValue();
        p.annotationTimestamps=new ListValue();p.annotationValues=new ListValue();return p;
    }
    public static List<SpanPack> spans(long time,long gid,int hash,boolean simple) {
        SpanPack root=span(gid,gid,0,time,SpanTypes.SERVER,hash,simple);
        SpanPack call=span(gid,gid+1,gid,time+5,SpanTypes.CLIENT,hash,simple);
        call.tags.put("http.url","http://synthetic.invalid/path");
        SpanPack unknown=span(gid,gid+2,gid,time+10,SpanTypes.UNKNOWN,hash,simple);
        SpanPack nested=span(gid,gid+3,gid+2,time+11,SpanTypes.UNKNOWN,hash,simple);
        SpanPack excluded=span(gid,gid+4,gid+1,time+6,SpanTypes.UNKNOWN,hash,simple);
        return Arrays.asList(nested,excluded,call,root,unknown);
    }
    public static SpanContainerPack container(long time,long gid,int hash) {
        SpanContainerPack p=new SpanContainerPack();p.timestamp=time;p.gxid=gid;
        List<SpanPack> spans=spans(time,gid,hash,false);p.spanCount=spans.size();p.spans=SpanPack.toBytes(spans);return p;
    }
}
