// Synthetic fixtures through unmodified Scouter DB writers/readers and SpanStepBuilder.
// Original classes Apache-2.0 @ dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5.
import java.nio.file.*;
import java.util.*;
import java.lang.reflect.*;
import scouter.io.*;
import scouter.lang.pack.*;
import scouter.lang.step.*;
import scouter.lang.value.*;
import scouter.server.db.status.*;
import scouter.server.db.summary.*;
import scouter.server.db.alert.*;
import scouter.server.db.span.*;
import scouter.server.db.BatchDB$;
import scouter.server.db.StackAnalyzerDB$;
import scouter.server.core.app.SpanStepBuilder$;
import scala.collection.mutable.ListBuffer;
import scala.Tuple2;

public final class ExtraStorageOracle {
    static final String DATE=ExtraDataset.DATE, OBJECT=ExtraDataset.OBJECT;
    static final long TIME=ExtraDataset.TIME;
    static void require(boolean value,String message){if(!value)throw new AssertionError(message);}
    static byte[] bytes(Pack p)throws Exception{return new DataOutputX().writePack(p).toByteArray();}
    static void same(Pack expected,byte[] actual)throws Exception {
        Pack parsed=new DataInputX(actual).readPack();
        require(Arrays.equals(bytes(expected),bytes(parsed)),"Pack semantics differ: "+expected.getClass().getSimpleName());
    }
    static String file(Path root,String dir,String name)throws Exception {
        Path path=root.resolve(DATE).resolve(dir);Files.createDirectories(path);return path.resolve(name).toString();
    }
    static Object invoke(Object module,String name,Class<?>[] types,Object... args)throws Exception {
        Method method=module.getClass().getDeclaredMethod(name,types);method.setAccessible(true);return method.invoke(module,args);
    }
    static List<Step> steps(List<SpanPack> spans,long gxid,long txid) {
        ListBuffer<SpanPack> input=new ListBuffer<>();for(SpanPack p:spans)input.$plus$eq(p);
        Tuple2<ListBuffer<StepSingle>,SpanPack> result=SpanStepBuilder$.MODULE$.toSteps(gxid,txid,input);
        List<Step> out=new ArrayList<>();scala.collection.Iterator<StepSingle> iterator=result._1().iterator();
        while(iterator.hasNext())out.add(iterator.next());return out;
    }
    static void emitSpan(Path root,String name,List<SpanPack> spans,long gxid,long txid)throws Exception {
        SpanContainerPack container=new SpanContainerPack();container.gxid=gxid;container.timestamp=spans.get(0).timestamp;
        container.spanCount=spans.size();container.spans=SpanPack.toBytes(spans);
        Files.write(root.resolve(name+"-input.bin"),bytes(container));
        byte[] expected=Step.toBytes(steps(spans,gxid,txid));
        require(Step.toObjects(expected).length>0,"Span steps must be decoded by original Java");
        Files.write(root.resolve(name+"-steps.bin"),expected);
    }
    static SpanPack span(long id,long caller,long timestamp,byte kind) {
        SpanPack p=new SpanPack();p.gxid=100;p.txid=id;p.caller=caller;p.timestamp=timestamp;p.spanType=kind;
        p.name=7;p.objHash=42;p.elapsed=id==100?200:25;p.tags=new MapValue();
        p.annotationTimestamps=new ListValue();p.annotationTimestamps.add(timestamp);
        p.annotationValues=new ListValue();p.annotationValues.add("synthetic annotation");
        p.localEndpointServiceName=8;p.localEndpointIp=new byte[]{127,0,0,1};p.localEndpointPort=8080;
        p.remoteEndpointServiceName=9;p.remoteEndpointIp=new byte[]{127,0,0,2};p.remoteEndpointPort=443;return p;
    }
    static void writeSpans(Path root)throws Exception {
        Files.createDirectories(root);
        List<SpanPack> simple=ExtraDataset.spans(1000,100,42,true);
        emitSpan(root,"span-simple",simple,100,100);
        require(Files.size(root.resolve("span-simple-steps.bin"))==203,"Previously verified original 203-byte fixture changed");
        List<Step> simpleSteps=steps(simple,100,100);int[] parents={-1,0,0,2}, starts={0,5,10,11};
        require(simpleSteps.size()==4,"Simple CLIENT boundary must exclude its child");
        for(int i=0;i<4;i++){StepSingle p=(StepSingle)simpleSteps.get(i);require(p.index==i&&p.parent==parents[i]&&p.start_time==starts[i],"Simple hierarchy");}
        SpanPack server=span(100,0,1000,SpanTypes.SERVER), rootClient=span(100,0,1000,SpanTypes.CLIENT);
        SpanPack first=span(10,100,1010,SpanTypes.UNKNOWN), tied=span(11,100,1010,SpanTypes.UNKNOWN);
        SpanPack client=span(200,100,1020,SpanTypes.CLIENT), sharedServer=span(200,100,1020,SpanTypes.SERVER);
        client.tags.put("http.url","https://example.invalid/synthetic");client.debug=true;sharedServer.shared=true;
        List<SpanPack> shared=Arrays.asList(server,first,tied,client,span(12,200,1030,SpanTypes.UNKNOWN),sharedServer,rootClient);
        emitSpan(root,"span-shared",shared,100,100);
        List<Step> sharedSteps=steps(shared,100,100);
        require(sharedSteps.size()==3,"Timestamp tie must drop one sibling and CLIENT boundary must stop descent");
        for(int i=0;i<3;i++){StepSingle p=(StepSingle)sharedSteps.get(i);require(p.index==i&&p.parent==(i==0?-1:0)&&p.start_time==i*10,"Shared hierarchy");}
        require(sharedSteps.get(2) instanceof SpanCallStep && ((SpanCallStep)sharedSteps.get(2)).txid==200
                && ((SpanCallStep)sharedSteps.get(2)).opt==1,"Shared CLIENT call optional address");
        Files.write(root.resolve("span-metadata.json"),("{\"source_commit\":\"dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5\","
            +"\"simple\":{\"gxid\":100,\"txid\":100,\"steps\":4,\"bytes\":203,\"parent\":[-1,0,0,2],\"start_time\":[0,5,10,11]},"
            +"\"shared\":{\"gxid\":100,\"txid\":100,\"steps\":3,\"parent\":[-1,0,0],\"start_time\":[0,10,20]}}\n").getBytes("UTF-8"));
    }
    static void generate(Path root)throws Exception {
        System.setProperty("oracle.root",root.toString());Path golden=root.resolve("oracle-extra");Files.createDirectories(golden);
        StatusPack status=ExtraDataset.status(TIME,42);String statusFile=file(root,"status","status");
        StatusWriter sw=new StatusWriter(statusFile);StatusIndex si=new StatusIndex(statusFile);si.add(TIME,sw.write(bytes(status)));sw.close();si.close();
        SummaryPack summary=ExtraDataset.summary(TIME,42);String summaryFile=file(root,"sum","sum");
        SummaryWriter sumw=new SummaryWriter(summaryFile);SummaryIndex sumi=new SummaryIndex(summaryFile+"_app");sumi.add(TIME,sumw.write(bytes(summary)));sumw.close();sumi.close();
        AlertPack alert=ExtraDataset.alert(TIME,42);String alertFile=file(root,"alert","alert");
        AlertWriter aw=new AlertWriter(alertFile);AlertIndex ai=new AlertIndex(alertFile);ai.add(TIME,aw.write(bytes(alert)));aw.close();ai.close();
        SpanContainerPack container=ExtraDataset.container(TIME,9001,42);String spanFile=file(root,"span","span");
        ZipkinSpanDataWriter spw=new ZipkinSpanDataWriter(DATE,spanFile);ZipkinSpanIndex spi=new ZipkinSpanIndex(spanFile);
        long spanPos=spw.write(bytes(container));spi.setByGxid(9001,spanPos);spi.setByTime(TIME,spanPos);spw.close();spi.close();
        Files.write(golden.resolve("status.bin"),bytes(status));Files.write(golden.resolve("summary.bin"),bytes(summary));
        Files.write(golden.resolve("alert.bin"),bytes(alert));Files.write(golden.resolve("span-container.bin"),bytes(container));
        Files.write(golden.resolve("span-storage-steps.bin"),Step.toBytes(steps(SpanPack.toObjectList(container.spans),9001,9001)));
        String app=file(root,"oracle","app");Files.createDirectories(Paths.get(app));
        for(int n=1;n<=2;n++) {
            long timestamp=TIME+(n-1)*100;StackPack stack=ExtraDataset.stack(timestamp,42,n);
            Object stackModule=StackAnalyzerDB$.MODULE$;
            long stackPos=((Number)invoke(stackModule,"append",new Class<?>[]{String.class,StackPack.class},app,stack)).longValue();
            invoke(stackModule,"appendX",new Class<?>[]{String.class,long.class,long.class},app,timestamp,stackPos);
            Files.write(golden.resolve("stack-"+n+".bin"),bytes(stack));
            BatchPack batch=ExtraDataset.batch(timestamp,42,OBJECT,n);Files.write(golden.resolve("batch-"+n+".bin"),bytes(batch));
            Object batchModule=BatchDB$.MODULE$;
            long batchPos=((Number)invoke(batchModule,"append",new Class<?>[]{String.class,BatchPack.class},app,batch)).longValue();
            invoke(batchModule,"appendX",new Class<?>[]{String.class,long.class,long.class},app,timestamp+batch.elapsedTime,batchPos);
            batch.position=batchPos;DataOutputX simple=new DataOutputX();batch.writeSimple(simple);
            Files.write(golden.resolve("batch-simple-"+n+".bin"),simple.toByteArray());
        }
        writeSpans(golden);
    }
    static scala.runtime.AbstractFunction2<Object,byte[],Object> expect(final Pack pack,final List<Long> times) {
        return new scala.runtime.AbstractFunction2<Object,byte[],Object>(){public Object apply(Object time,byte[] data){
            try{same(pack,data);times.add(((Number)time).longValue());return null;}catch(Exception e){throw new RuntimeException(e);}}};
    }
    static void verify(Path root)throws Exception {
        System.setProperty("oracle.root",root.toString());
        String sf=file(root,"status","status");final StatusReader sr=new StatusReader(sf);StatusIndex si=new StatusIndex(sf);
        List<Long> times=new ArrayList<>();si.read(TIME-1,TIME+999,expect(ExtraDataset.status(TIME,42),times),new scala.runtime.AbstractFunction1<Object,byte[]>(){public byte[] apply(Object pos){return sr.read(((Number)pos).longValue());}});
        require(times.equals(Arrays.asList(TIME)),"Status time/index");sr.close();si.close();
        String sumf=file(root,"sum","sum");final SummaryReader sumr=new SummaryReader(sumf);SummaryIndex sumi=new SummaryIndex(sumf+"_app");times.clear();
        sumi.read(TIME-1,TIME+999,expect(ExtraDataset.summary(TIME,42),times),new scala.runtime.AbstractFunction1<Object,byte[]>(){public byte[] apply(Object pos){return sumr.read(((Number)pos).longValue());}});
        require(times.equals(Arrays.asList(TIME)),"Summary time/index");sumr.close();sumi.close();
        String af=file(root,"alert","alert");final AlertReader ar=new AlertReader(af);AlertIndex ai=new AlertIndex(af);times.clear();
        ai.read(TIME-1,TIME+999,expect(ExtraDataset.alert(TIME,42),times),new scala.runtime.AbstractFunction1<Object,byte[]>(){public byte[] apply(Object pos){return ar.read(((Number)pos).longValue());}});
        require(times.equals(Arrays.asList(TIME)),"Alert time/index");ar.close();ai.close();
        String spf=file(root,"span","span");ZipkinSpanDataReader spr=new ZipkinSpanDataReader(DATE,spf);ZipkinSpanIndex spi=new ZipkinSpanIndex(spf);
        List<?> positions=spi.getByGxid(9001);require(positions.size()==1,"Span gxid index");same(ExtraDataset.container(TIME,9001,42),spr.read(((Number)positions.get(0)).longValue()));spr.close();spi.close();
        final List<Long> stackTimes=new ArrayList<>();StackAnalyzerDB$.MODULE$.read(OBJECT,TIME-1,TIME+1000,new scala.runtime.AbstractFunction2<Object,byte[],Object>(){public Object apply(Object tm,byte[] raw){try{
            int n=stackTimes.size()+1;StackPack p=(StackPack)new DataInputX(raw).readPack();StackPack expected=ExtraDataset.stack(TIME+(n-1)*100,42,n);
            require(p.time==expected.time&&p.objHash==42&&p.getStack().equals(expected.getStack()),"Stack codec/compression/content");stackTimes.add(((Number)tm).longValue());return null;
        }catch(Exception e){throw new RuntimeException(e);}}});require(stackTimes.equals(Arrays.asList(TIME,TIME+100)),"Stack records/order");
        final List<Long> batchTimes=new ArrayList<>();final List<BatchPack> batches=new ArrayList<>();
        BatchDB$.MODULE$.read(OBJECT,TIME-1,TIME+1000,null,0,new scala.runtime.AbstractFunction2<Object,BatchPack,Object>(){public Object apply(Object tm,BatchPack p){batchTimes.add(((Number)tm).longValue());batches.add(p);return null;}});
        require(batchTimes.equals(Arrays.asList(TIME+25,TIME+150)),"Batch records/endTime order");
        long expectedPosition=0;
        for(int n=1;n<=2;n++) {
            BatchPack expected=ExtraDataset.batch(TIME+(n-1)*100,42,OBJECT,n), listed=batches.get(n-1);
            require(listed.position==expectedPosition&&listed.batchJobId.equals(expected.batchJobId)&&listed.sqlStats==null,"Batch simple metadata/physical position");
            byte[] detail=BatchDB$.MODULE$.read(OBJECT,listed.startTime+listed.elapsedTime,listed.position);same(expected,detail);
            expectedPosition+=4+bytes(expected).length;
        }
    }
    public static void main(String[] args)throws Exception {
        Path root=Paths.get(args[1]).toAbsolutePath();
        if(args[0].equals("generate")){generate(root);verify(root);}
        else if(args[0].equals("spans"))writeSpans(root);
        else if(args[0].equals("verify"))verify(root);
        else throw new IllegalArgumentException("Unknown extra oracle mode");
        System.out.println("PASS actual original extra Storage/SpanStepBuilder "+args[0]);
    }
}
