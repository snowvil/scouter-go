// Synthetic common Packs sent through actual Host UDP; queries use original Client TcpProxy.
// Original codecs/API are Apache-2.0 Scouter at the guarded baseline.
import java.util.*;
import scouter.io.DataOutputX;
import scouter.io.DataInputX;
import scouter.lang.*;
import scouter.lang.pack.*;
import scouter.lang.step.*;
import scouter.lang.value.*;
import scouter.net.RequestCmd;
import scouter.client.net.TcpProxy;
import scouter.agent.netio.data.net.DataUdpAgent;
import scouter.util.HashUtil;

public final class ExtraInteropHarness {
    static final String BATCH_NAME="/interop/batch";
    static final int BATCH_HASH=HashUtil.hash(BATCH_NAME);
    static final long GXID=820000000001L, SHARED_GXID=820000001001L, DROP_GXID=830000000001L, END_ID=4294967303L;
    static long receiveLower,receiveUpper;
    static int seededPacks,seededDatagrams;
    static void send(Pack p)throws Exception{int length=new DataOutputX().writePack(p).toByteArray().length;seededPacks++;seededDatagrams+=(length+511)/512;InteropHarness.send(p);}
    static void require(boolean v,String s){InteropHarness.require(v,s);}
    // CAFN carries an ordered sequence within one original Host datagram.
    // Separate UDP datagrams can be processed concurrently and are not an ordering oracle.
    static void sendOrdered(Pack... packs)throws Exception {
        List<byte[]> encoded=new ArrayList<>();for(Pack p:packs)encoded.add(new DataOutputX().writePack(p).toByteArray());
        require(DataUdpAgent.getInstance().write(encoded),"Original Host ordered CAFN rejected");seededPacks+=packs.length;seededDatagrams++;
    }
    static MapPack range(String date,long from,long to){MapPack p=new MapPack();p.put("date",date);p.put("stime",from);p.put("etime",to);return p;}
    static SummaryPack summary(long t,int hash,byte kind,int count) {
        SummaryPack p=ExtraDataset.summary(t,hash);p.stype=kind;
        if(kind==SummaryEnum.APP||kind==SummaryEnum.SQL||kind==SummaryEnum.APICALL)return p;
        p.table=new MapValue();String[] keys;
        if(kind==SummaryEnum.IP||kind==SummaryEnum.USER_AGENT)keys=new String[]{"id","count"};
        else if(kind==SummaryEnum.SERVICE_ERROR)keys=new String[]{"id","error","service","message","count","txid","sql","apicall","fullstack"};
        else if(kind==SummaryEnum.ALERT){p.table.newList("title").add("synthetic summary alert");keys=new String[]{"count","level"};}
        else if(kind==SummaryEnum.ENDUSER_AJAX_TIME)keys=new String[]{"id","uri","ip","count","duration","userAgent"};
        else if(kind==SummaryEnum.ENDUSER_SCRIPT_ERROR)keys=new String[]{"id","host","stacktrace","userAgent","count","uri","message","file","lineNumber","columnNumber"};
        else keys=new String[]{"id","uri","ip","count","navigationStart","unloadEventStart","unloadEventEnd","fetchStart","domainLookupStart","domainLookupEnd","connectStart","connectEnd","requestStart","responseStart","responseEnd","domLoading","domInteractive","domContentLoadedEventStart","domContentLoadedEventEnd","domComplete","loadEventStart","loadEventEnd"};
        for(String key:keys)p.table.newList(key).add(key.equals("id")?(kind>=10?END_ID:7):key.equals("count")?count:key.equals("host")?99:key.equals("stacktrace")?44:key.equals("level")?2:10);
        return p;
    }
    static InteractionPerfCounterPack interaction(long t,String name,int hash,int target,int count){
        InteractionPerfCounterPack p=new InteractionPerfCounterPack(name,"synthetic-api");p.time=t;p.fromHash=hash;p.toHash=target;p.period=2000;p.count=count;p.errorCount=1;p.totalElapsed=30;p.customData.put("load",new FloatValue(count));return p;
    }
    static SpanContainerPack shared(long t)throws Exception {
        List<SpanPack> base=ExtraDataset.spans(t,SHARED_GXID,BATCH_HASH,false);
        SpanPack nested=base.get(0),excluded=base.get(1),call=base.get(2),root=base.get(3),unknown=base.get(4);
        nested.caller=SHARED_GXID;nested.timestamp=t+10;call.timestamp=t+20;excluded.timestamp=t+30;
        call.debug=true;call.tags.put("http.url","https://example.invalid/synthetic");
        call.annotationTimestamps.add(t+20);call.annotationValues.add("synthetic annotation");
        call.localEndpointIp=new byte[]{127,0,0,1};call.localEndpointPort=8080;call.remoteEndpointIp=new byte[]{127,0,0,2};call.remoteEndpointPort=443;
        SpanPack server=(SpanPack)new DataInputX(new DataOutputX().writePack(call).toByteArray()).readPack();server.spanType=SpanTypes.SERVER;server.shared=true;
        SpanPack rootClient=(SpanPack)new DataInputX(new DataOutputX().writePack(root).toByteArray()).readPack();rootClient.spanType=SpanTypes.CLIENT;
        SpanContainerPack p=new SpanContainerPack();p.timestamp=t;p.gxid=SHARED_GXID;p.spanCount=7;p.spans=SpanPack.toBytes(Arrays.asList(root,unknown,nested,call,excluded,server,rootClient));return p;
    }
    public static void seed(long t,String date,int hash,String name)throws Exception {
        receiveLower=System.currentTimeMillis();
        StatusPack status=ExtraDataset.status(1,hash);status.key="querylist";send(status);
        send(ExtraDataset.alert(1,hash));
        sendOrdered(interaction(t,name,hash,777,1),interaction(t,name,hash,777,2));
        send(interaction(System.currentTimeMillis()-120000,name,hash,778,3));
        sendOrdered(ExtraDataset.stack(t,hash,1),ExtraDataset.stack(t+100,hash,2));
        byte[] kinds={SummaryEnum.APP,SummaryEnum.SQL,SummaryEnum.APICALL,SummaryEnum.IP,SummaryEnum.USER_AGENT,SummaryEnum.SERVICE_ERROR,SummaryEnum.ALERT,SummaryEnum.ENDUSER_NAVIGATION_TIME,SummaryEnum.ENDUSER_AJAX_TIME,SummaryEnum.ENDUSER_SCRIPT_ERROR};
        for(byte kind:kinds){if(kind>=10)sendOrdered(summary(t,hash,kind,2),summary(t+1,hash,kind,3));else send(summary(t,hash,kind,2));}
        sendOrdered(ExtraDataset.batch(t,BATCH_HASH,BATCH_NAME,1),ExtraDataset.batch(t+100,BATCH_HASH,BATCH_NAME,2));
        send(ExtraDataset.container(t,GXID,BATCH_HASH));
        XLogPack b3=new XLogPack();b3.txid=GXID;b3.gxid=GXID;b3.objHash=BATCH_HASH;b3.service=7;b3.endTime=t+25;b3.elapsed=25;b3.xType=XLogTypes.ZIPKIN_SPAN;send(b3);
        send(shared(t));XLogPack sharedRoot=new XLogPack();sharedRoot.txid=SHARED_GXID;sharedRoot.gxid=SHARED_GXID;sharedRoot.objHash=BATCH_HASH;sharedRoot.service=7;sharedRoot.endTime=t+26;sharedRoot.elapsed=25;sharedRoot.xType=XLogTypes.ZIPKIN_SPAN;send(sharedRoot);
        XLogProfilePack2 pending=new XLogProfilePack2();pending.txid=DROP_GXID+1;pending.gxid=DROP_GXID;pending.time=t;pending.objHash=hash;pending.service=7;pending.xType=XLogTypes.BACK_THREAD2;pending.discardType=1;pending.profile=Step.toBytes(Arrays.asList(new MessageStep(0,"synthetic dropped child")));
        XLogPack child=new XLogPack();child.txid=pending.txid;child.gxid=pending.gxid;child.endTime=t;child.objHash=hash;child.service=7;child.xType=pending.xType;child.discardType=1;
        sendOrdered(pending,child,new DroppedXLogPack(DROP_GXID,DROP_GXID));
        receiveUpper=System.currentTimeMillis()+10000;
        require(seededPacks==29,"Extra seed Pack count");
        System.out.println("PASS extra UDP packs="+seededPacks+" datagrams="+seededDatagrams+"; status/alert/interaction3/stack2/summary13/batch2/spanContainer2/B3XLog2/pendingProfile/pendingXLog/dropped");
    }
    static List<Pack> query(String cmd,MapPack p){return InteropHarness.packs(cmd,p);}
    static MapPack result(String cmd,MapPack p){return InteropHarness.map(InteropHarness.one(cmd,p),cmd);}
    public static void verify(long t,String date,int hash,String name,boolean restarted)throws Exception {
        MapPack batchObject=new MapPack();batchObject.put("date",date);batchObject.put("objHash",BATCH_HASH);InteropHarness.eventually("implicit Batch heartbeat",()->InteropHarness.one(RequestCmd.OBJECT_INFO,batchObject) instanceof ObjectPack);
        MapPack daily=new MapPack();daily.put("date",date);daily.put("counter","Cpu");InteropHarness.integers(daily,"objHash",hash);
        for(String cmd:Arrays.asList(RequestCmd.COUNTER_PAST_DATE_GROUP,RequestCmd.COUNTER_TODAY_GROUP)){
            List<Pack> rows=query(cmd,daily);require(rows.size()==1,"Daily group one object");MapPack p=(MapPack)rows.get(0);require(p.getInt("objHash")==hash&&p.getList("time").size()==288&&p.getList("value").size()==288,"Daily group arrays");int filled=0;for(int i=0;i<288;i++){Value v=p.getList("value").get(i);if(!(v instanceof NullValue)){require(((NumberValue)v).doubleValue()==37.5,"Daily group sample");filled++;}}require(filled==1,"Daily group sparse count");
        }
        List<Pack> objects=query(RequestCmd.OBJECT_TODAY_FULL_LIST,null);require(objects.size()==2,"Implicit Batch object persisted with Host");for(Pack raw:objects){require(raw instanceof ObjectPack,"Full object Pack");ObjectPack p=(ObjectPack)raw;String statusTag=p.tags.getText("status");require(Arrays.asList("active","inactive","dead").contains(statusTag),"Full object status tag");}
        if(!restarted){
            MapPack serviceGroups=new MapPack();InteropHarness.integers(serviceGroups,"objHash",hash);
            InteropHarness.eventually("realtime service group startup/cache",()->{MapPack p=result(RequestCmd.REALTIME_SERVICE_GROUP,serviceGroups);if(p.getList("name").size()==0)return false;require(p.getList("name").size()==1&&"/**".equals(p.getList("name").getString(0))&&p.getList("count").getInt(0)==3&&p.getList("error").getInt(0)==1,"Service group name/count/error");require(Math.abs(p.getList("elapsed").getFloat(0)-(413.0f/3))<0.0001,"Service group Float average");return true;});
            require(query(RequestCmd.REALTIME_SERVICE_GROUP,new MapPack()).isEmpty(),"Service group missing object list is no response");
        }
        if(!restarted){MapPack group=new MapPack();group.put("counter","Cpu");InteropHarness.integers(group,"objHash",hash);MapPack values=result(RequestCmd.COUNTER_REAL_TIME_GROUP,group);require(values.getList("objHash").size()==1&&values.getList("objHash").getInt(0)==hash&&values.getList("value").getDouble(0)==25.25,"Realtime group values");}
        MapPack status=new MapPack();status.put("objHash",hash);status.put("key","querylist");status.put("time",restarted?System.currentTimeMillis()-60000:receiveLower-1);
        InteropHarness.eventually("receiver Status timestamp",()->{List<Pack> rows=query(RequestCmd.STATUS_AROUND_VALUE,status);if(rows.isEmpty())return false;require(rows.get(0) instanceof StatusPack,"StatusPack");StatusPack p=(StatusPack)rows.get(0);require(p.time!=1&&p.objHash==hash&&"synthetic".equals(p.data.getText("state")),"Status override/data");if(!restarted)require(p.time>=receiveLower&&p.time<=receiveUpper,"Status server clock range");return true;});
        MapPack alerts=range(date,t-180000,t+180000);alerts.put("key","synthetic alert");
        InteropHarness.eventually("Alert history receiver clock",()->{List<Pack> rows=query(RequestCmd.ALERT_LOAD_TIME,alerts);if(rows.isEmpty())return false;require(rows.size()==1&&rows.get(0) instanceof AlertPack,"Alert history one Pack");AlertPack p=(AlertPack)rows.get(0);require(p.time!=1&&p.level==2&&p.objHash==hash&&"synthetic".equals(p.tags.getText("region")),"Alert server time/fields");if(!restarted)require(p.time>=receiveLower&&p.time<=receiveUpper,"Alert clock range");return true;});
        if(!restarted){MapPack rt=new MapPack();rt.put("loop",0L);rt.put("index",0);rt.put("objType","host");List<Pack> rows=query(RequestCmd.ALERT_REAL_TIME,rt);require(rows.size()==2&&rows.get(0) instanceof MapPack&&rows.get(1) instanceof AlertPack,"Alert meta FIRST then Pack");MapPack meta=(MapPack)rows.get(0);require(meta.getLong("loop")==0&&meta.getInt("index")==1,"Alert cursor");rt.put("first",true);require(query(RequestCmd.ALERT_REAL_TIME,rt).size()==1,"Alert first metadata-only");
            MapPack intr=new MapPack();InteropHarness.integers(intr,"objHash",hash);List<Pack> interactions=query(RequestCmd.INTR_COUNTER_REAL_TIME_BY_OBJ,intr);require(interactions.size()==2&&interactions.get(0) instanceof InteractionPerfCounterPack,"Cache TTL uses receive time, not payload timestamp");InteractionPerfCounterPack p=(InteractionPerfCounterPack)interactions.get(0);require(p.toHash==777&&p.count==2&&p.customData.getFloat("load")==2,"Interaction replacement/order");Thread.sleep(10100);InteropHarness.eventually("interaction expiry sweep",()->query(RequestCmd.INTR_COUNTER_REAL_TIME_BY_OBJ,intr).isEmpty());}
        MapPack stacks=new MapPack();stacks.put("objName",name);stacks.put("from",t-1);stacks.put("to",t+101);
        InteropHarness.eventually("Stack history",()->{List<Pack> rows=query(RequestCmd.GET_STACK_ANALYZER,stacks);if(rows.size()!=2)return false;for(int i=0;i<2;i++){require(rows.get(i) instanceof StackPack,"Stack Pack type");StackPack p=(StackPack)rows.get(i);require(p.time==t+i*100&&p.getStack().equals(ExtraDataset.stack(t+i*100,hash,i+1).getStack()),"Original stack compression/content/order");}return true;});
        List<Long> indexes=new ArrayList<>();TcpProxy proxy=InteropHarness.proxy();try{proxy.process(RequestCmd.GET_STACK_INDEX,stacks,in->indexes.add(in.readLong()));require(proxy.isValid(),"Stack index transport");}finally{TcpProxy.putTcpProxy(proxy);}require(indexes.equals(Arrays.asList(t,t+100)),"Stack index raw long response");
        MapPack batch=range(date,t-1,t+151);batch.put("objHash",BATCH_HASH);batch.put("from",t-1);batch.put("to",t+151);
        List<BatchPack> jobs=new ArrayList<>();proxy=InteropHarness.proxy();try{proxy.process(RequestCmd.BATCH_HISTORY_LIST,batch,in->{BatchPack p=new BatchPack();p.readSimple(in);jobs.add(p);});require(proxy.isValid(),"Batch simple transport");}finally{TcpProxy.putTcpProxy(proxy);}require(jobs.size()==2,"Batch history count");long position=0;
        for(int i=0;i<2;i++){BatchPack listed=jobs.get(i),expected=ExtraDataset.batch(t+i*100,BATCH_HASH,BATCH_NAME,i+1);require(listed.position==position&&listed.sqlStats==null&&listed.uniqueSqls==null&&listed.batchJobId.equals(expected.batchJobId),"Batch simple/details projection/position");MapPack detail=new MapPack();detail.put("date",date);detail.put("objHash",BATCH_HASH);detail.put("startTime",listed.startTime);detail.put("elapsedTime",listed.elapsedTime);detail.put("position",listed.position);Pack raw=InteropHarness.one(RequestCmd.BATCH_HISTORY_DETAIL,detail);require(raw instanceof BatchPack,"Batch detail Pack");BatchPack full=(BatchPack)raw;require(full.sqlStats.size()==1&&"synthetic SQL".equals(full.uniqueSqls.get(7)),"Batch SQL detail");position+=4+new DataOutputX().writePack(expected).toByteArray().length;}
        verifySummary(t,date,hash);verifyTrace(t,date);
        System.out.println("PASS 2D "+(restarted?"restart":"seed")+" Status/Alert clock override; interaction replacement/expiry; compressed Stack+raw indexes; implicit Batch object/simple/detail/physical positions; Summary10 kinds; B3 profile/GXID; dropped child absent");
    }
    static void verifySummary(long t,String date,int hash) {
        String[] commands={RequestCmd.LOAD_SERVICE_SUMMARY,RequestCmd.LOAD_SQL_SUMMARY,RequestCmd.LOAD_APICALL_SUMMARY,RequestCmd.LOAD_IP_SUMMARY,RequestCmd.LOAD_UA_SUMMARY,RequestCmd.LOAD_SERVICE_ERROR_SUMMARY,RequestCmd.LOAD_ALERT_SUMMARY,RequestCmd.LOAD_ENDUSER_NAV_SUMMARY,RequestCmd.LOAD_ENDUSER_AJAX_SUMMARY,RequestCmd.LOAD_ENDUSER_ERROR_SUMMARY};
        MapPack request=range(date,t-1,t+2);request.put("objHash",hash);request.put("objType","host");
        for(int i=0;i<commands.length;i++){MapPack p=result(commands[i],request);require(p.getList("id").size()==1,"Summary nonempty parallel lists: "+commands[i]);if(i<7)require(p.getList("count").getInt(0)==(i<3?3:2),"Summary count");else{require(p.getList("id").getLong(0)==END_ID,"EndUser long ID retained");if(i==9)require(p.get("count")==null&&p.getList("host").getInt(0)==44,"Original error host=stacktrace/count omitted");else require(p.getList("count").getInt(0)==3,"Original EndUser long-ID replacement");}}
    }
    static void verifyTrace(long t,String date)throws Exception {
        MapPack request=new MapPack();request.put("date",date);request.put("txid",GXID);request.put("gxid",GXID);request.put("xlogType",XLogTypes.ZIPKIN_SPAN);request.put("max",1);
        InteropHarness.eventually("B3 synthesized profile",()->{Pack raw=InteropHarness.one(RequestCmd.TRANX_PROFILE,request);if(!(raw instanceof XLogProfilePack))return false;XLogProfilePack p=(XLogProfilePack)raw;Step[] steps=Step.toObjects(p.profile);require(p.txid==GXID&&p.objHash==BATCH_HASH&&p.service==7&&p.time==0&&p.elapsed==0,"B3 Profile serialized fields");require(steps.length==4,"B3 max ignored, CLIENT child excluded");int[] parent={-1,0,0,2},start={0,5,10,11};for(int i=0;i<steps.length;i++){StepSingle s=(StepSingle)steps[i];require(s.index==i&&s.parent==parent[i]&&s.start_time==start[i],"B3 hierarchy/order");}require(steps[1] instanceof SpanCallStep&&((SpanCallStep)steps[1]).txid==GXID+1&&"http://synthetic.invalid/path".equals(((SpanCallStep)steps[1]).address),"B3 URL opt");return true;});
        byte[] expected=((XLogProfilePack)InteropHarness.one(RequestCmd.TRANX_PROFILE,request)).profile;List<byte[]> chunks=new ArrayList<>();TcpProxy proxy=InteropHarness.proxy();try{proxy.process(RequestCmd.TRANX_PROFILE_FULL,request,in->chunks.add(in.readBlob()));require(proxy.isValid(),"B3 full transport");}finally{TcpProxy.putTcpProxy(proxy);}require(chunks.size()==1&&Arrays.equals(chunks.get(0),expected),"B3 FULL single identical Blob");
        MapPack gx=new MapPack();gx.put("date",date);gx.put("gxid",GXID);List<Pack> rows=query(RequestCmd.XLOG_READ_BY_GXID,gx);require(rows.size()==1&&((XLogPack)rows.get(0)).txid==GXID,"GXID history");
        MapPack spanHistory=range(date,t,t+30);spanHistory.put("gxid",GXID);require(query(RequestCmd.XLOG_LOAD_BY_GXID,spanHistory).size()==1,"GXID date selector");
        MapPack quick=new MapPack();quick.put("date",date);quick.put("gxid",GXID);quick.put("txid",GXID);rows=query(RequestCmd.QUICKSEARCH_XLOG_LIST,quick);require(rows.size()==2&&((XLogPack)rows.get(0)).txid==GXID&&((XLogPack)rows.get(1)).txid==GXID,"Quicksearch duplicate preserved");
        MapPack search=range(date,t,t+30);search.put("objHash",BATCH_HASH);rows=query(RequestCmd.SEARCH_XLOG_LIST,search);require(rows.size()==2&&((XLogPack)rows.get(0)).txid==GXID&&((XLogPack)rows.get(1)).txid==SHARED_GXID,"Search chronological object filter");
        MapPack fallback=new MapPack();fallback.put("date",date);fallback.put("txid",GXID+3);fallback.put("gxid",GXID);Pack parent=InteropHarness.one(RequestCmd.XLOG_READ_BY_TXID,fallback);require(parent instanceof XLogPack&&((XLogPack)parent).txid==GXID,"B3 missing nested XLog ancestry fallback");
        MapPack ids=new MapPack();ids.put("date",date);ListValue values=ids.newList("txid");values.add(GXID);values.add(-1L);values.add(GXID);rows=query(RequestCmd.XLOG_LOAD_BY_TXIDS,ids);require(rows.size()==2&&((XLogPack)rows.get(0)).txid==GXID&&((XLogPack)rows.get(1)).txid==GXID,"TXID input order/miss omit/duplicate keep");
        MapPack sharedRequest=new MapPack();sharedRequest.put("date",date);sharedRequest.put("txid",SHARED_GXID);sharedRequest.put("gxid",SHARED_GXID);sharedRequest.put("xlogType",XLogTypes.ZIPKIN_SPAN);sharedRequest.put("max",1);
        XLogProfilePack shared=(XLogProfilePack)InteropHarness.one(RequestCmd.TRANX_PROFILE,sharedRequest);require(shared!=null,"Shared-ID profile");Step[] sharedSteps=Step.toObjects(shared.profile);require(sharedSteps.length==3,"Shared SERVER/CLIENT and timestamp tie yield3Steps");for(int i=0;i<3;i++){StepSingle s=(StepSingle)sharedSteps[i];require(s.index==i&&s.parent==(i==0?-1:0)&&s.start_time==i*10,"Shared-ID hierarchy");}SpanCallStep call=(SpanCallStep)sharedSteps[2];require(call.txid==SHARED_GXID+1&&call.debug&&"https://example.invalid/synthetic".equals(call.address)&&call.localEndpointPort==8080&&call.annotationValues.size()==1,"Shared call optionalURL/endpoint/annotation/debug");
        MapPack missing=new MapPack();missing.put("date",date);missing.put("txid",DROP_GXID+1);require(query(RequestCmd.XLOG_READ_BY_TXID,missing).isEmpty(),"Dropped pending child must not become completed XLog");missing.put("gxid",0L);require(query(RequestCmd.TRANX_PROFILE,missing).isEmpty(),"Dropped profile must be absent");
    }
}
