// Executes unchanged Apache-2.0 Scouter KeyValueStoreService/DatabaseService and
// actual KV/Status writers, readers and indices from the pinned source tree.
// Only root/config/lifecycle and unused AgentCall branches are infrastructure seams.
import java.nio.file.*;
import java.util.*;
import scouter.io.*;
import scouter.lang.pack.*;
import scouter.lang.value.*;
import scouter.server.db.KeyValueStoreRW$;
import scouter.server.db.kv.*;
import scouter.server.netio.service.handle.*;

public final class LocalStoreOracle {
    static final String GLOBAL="__SCOUTER_KV_GLOBAL__", CUSTOM="synthetic-space";
    interface Handler {void run(DataInputX in,DataOutputX out);}
    static void require(boolean v,String s){if(!v)throw new AssertionError(s);}
    static byte[] valueBytes(Value v)throws Exception{return new DataOutputX().writeValue(v).toByteArray();}
    static byte[] packBytes(Pack v)throws Exception{return new DataOutputX().writePack(v).toByteArray();}
    static byte[] run(byte[] request,Handler handler)throws Exception {DataOutputX out=new DataOutputX();handler.run(new DataInputX(request),out);return out.toByteArray();}
    static byte[] fixture(Path root,String name,byte[] request,Handler handler)throws Exception {
        byte[] response=run(request,handler);Files.write(root.resolve(name+"-request.bin"),request);Files.write(root.resolve(name+"-response.bin"),response);return response;
    }
    static void seed(ListValue seeds,String space,String key,String text){
        require(KeyValueStoreRW$.MODULE$.set(space,key,text,-1L),"Actual RW.set "+key);
        MapValue entry=new MapValue();entry.put("keySpace",space);entry.put("key",key);entry.put("value",text);seeds.add(entry);
    }
    static MapPack custom(String key){MapPack p=new MapPack();p.put("keySpace",CUSTOM);if(key!=null)p.put("key",key);return p;}
    static MapPack bulk(String space){MapPack p=new MapPack();if(space!=null)p.put("keySpace",space);ListValue l=p.newList("key");l.add("b");l.add("missing");l.add("a");l.add("b");return p;}
    static void requireText(byte[] raw,String expected)throws Exception {Value value=DBHandlerOracle.value(raw);require(value instanceof TextValue&&Objects.equals(value.toString(),expected),"Original TextValue response "+expected);}
    static void close(String space){
        KeyValueStoreIndex index=KeyValueStoreIndex$.MODULE$.get(space);index.index().hashBlock().flush();
        // Original Reader.close removes the Writer table entry. Retain both
        // actual handles before closing; no source lifecycle replacement.
        KeyValueStoreWriter writer=KeyValueStoreWriter$.MODULE$.get(space);
        KeyValueStoreReader reader=KeyValueStoreReader$.MODULE$.get(space);
        reader.close();writer.close();index.close();
    }
    // Write-handler mode: every SET/TTL case runs through the actual
    // KeyValueStoreService method and the actual RW/index/writer; handler
    // exceptions (the original proxy swallows them) are recorded, not hidden.
    static final String TTL_SPACE="synthetic-ttl-space";
    static void writeCase(Path root,ListValue cases,String name,String command,byte[] request,Handler handler)throws Exception {
        DataOutputX out=new DataOutputX();boolean throwsException=false;String error="";
        try{handler.run(new DataInputX(request),out);}catch(Throwable t){throwsException=true;error=t.getClass().getName();}
        byte[] response=out.toByteArray();Files.write(root.resolve(name+"-request.bin"),request);Files.write(root.resolve(name+"-response.bin"),response);
        MapValue item=new MapValue();item.put("name",name);item.put("command",command);item.put("request",new BlobValue(request));item.put("response",new BlobValue(response));item.put("throws",new BooleanValue(throwsException));item.put("error",error);cases.add(item);
    }
    static MapPack kvPack(String space,String key,String value,Value ttl){MapPack p=new MapPack();if(space!=null)p.put("keySpace",space);if(key!=null)p.put("key",key);if(value!=null)p.put("value",value);if(ttl!=null)p.put("ttl",ttl);return p;}
    static MapPack bulkPack(String space,Value ttl,MapValue kv){MapPack p=new MapPack();if(space!=null)p.put("keySpace",space);if(ttl!=null)p.put("ttl",ttl);if(kv!=null)p.put("kv",kv);return p;}
    static MapValue kvMap(String... pairs){MapValue m=new MapValue();for(int i=0;i<pairs.length;i+=2)m.put(pairs[i],pairs[i+1]);return m;}
    static MapPack keys(String space,String... names){MapPack p=new MapPack();if(space!=null)p.put("keySpace",space);ListValue l=p.newList("key");for(String n:names)l.add(n);return p;}
    static void requireBoolean(byte[] raw,boolean expected)throws Exception {Value value=DBHandlerOracle.value(raw);require(value instanceof BooleanValue&&((BooleanValue)value).value==expected,"Original BooleanValue response "+expected);}
    static void writes(Path root)throws Exception {
        KeyValueStoreService kv=new KeyValueStoreService();ListValue cases=new ListValue();long before=System.currentTimeMillis()/1000L;
        Handler setGlobal=(in,out)->kv.setText(in,out,true),ttlGlobal=(in,out)->kv.setTTL(in,out,true),bulkGlobal=(in,out)->kv.setTextBulk(in,out,true);
        Handler setCustom=(in,out)->kv.setTextToCustomStore(in,out,true),ttlCustom=(in,out)->kv.setTtlToCustomStore(in,out,true),bulkCustom=(in,out)->kv.setTextBulkToCustomStore(in,out,true);
        writeCase(root,cases,"set-global","SET_GLOBAL_KV",packBytes(kvPack(null,"a","alpha",null)),setGlobal);
        writeCase(root,cases,"set-global-ttl","SET_GLOBAL_KV",packBytes(kvPack(null,"b","beta",new DecimalValue(3600))),setGlobal);
        writeCase(root,cases,"set-global-overwrite","SET_GLOBAL_KV",packBytes(kvPack(null,"a","alpha-2",new DecimalValue(-7))),setGlobal);
        writeCase(root,cases,"set-global-empty-value","SET_GLOBAL_KV",packBytes(kvPack(null,"c","",null)),setGlobal);
        writeCase(root,cases,"set-global-no-value","SET_GLOBAL_KV",packBytes(kvPack(null,"c",null,null)),setGlobal);
        writeCase(root,cases,"set-global-nontext-value","SET_GLOBAL_KV",packBytes(withValue(kvPack(null,"c",null,null),new DecimalValue(9))),setGlobal);
        writeCase(root,cases,"set-global-no-key","SET_GLOBAL_KV",packBytes(kvPack(null,null,"orphan",null)),setGlobal);
        writeCase(root,cases,"set-global-nontext-key","SET_GLOBAL_KV",packBytes(withKey(kvPack(null,null,"orphan",null),new DecimalValue(1))),setGlobal);
        writeCase(root,cases,"set-global-empty-key","SET_GLOBAL_KV",packBytes(kvPack(null,"","orphan",null)),setGlobal);
        writeCase(root,cases,"set-global-ttl-float","SET_GLOBAL_KV",packBytes(kvPack(null,"f","float-ttl",new FloatValue(90.9f))),setGlobal);
        writeCase(root,cases,"set-global-ttl-double","SET_GLOBAL_KV",packBytes(kvPack(null,"d","double-ttl",new DoubleValue(-0.5d))),setGlobal);
        writeCase(root,cases,"set-global-ttl-text","SET_GLOBAL_KV",packBytes(kvPack(null,"t","text-ttl",new TextValue("5"))),setGlobal);
        writeCase(root,cases,"set-global-ttl-zero","SET_GLOBAL_KV",packBytes(kvPack(null,"z","zero-ttl",new DecimalValue(0))),setGlobal);
        writeCase(root,cases,"set-global-unicode","SET_GLOBAL_KV",packBytes(kvPack(null,"한글-key","한글 값",null)),setGlobal);
        writeCase(root,cases,"ttl-global","SET_GLOBAL_TTL",packBytes(kvPack(null,"a",null,new DecimalValue(7200))),ttlGlobal);
        writeCase(root,cases,"ttl-global-missing","SET_GLOBAL_TTL",packBytes(kvPack(null,"missing",null,new DecimalValue(10))),ttlGlobal);
        writeCase(root,cases,"ttl-global-permanent","SET_GLOBAL_TTL",packBytes(kvPack(null,"b",null,null)),ttlGlobal);
        writeCase(root,cases,"ttl-global-no-key","SET_GLOBAL_TTL",packBytes(kvPack(null,null,null,new DecimalValue(5))),ttlGlobal);
        writeCase(root,cases,"ttl-global-empty-key","SET_GLOBAL_TTL",packBytes(kvPack(null,"",null,new DecimalValue(5))),ttlGlobal);
        MapValue bulk=kvMap("k0","v0","k1","v1","k2","v2","k3","v3","k4","v4","k5","v5","k6","v6","","empty-key","e","");bulk.put("n",new DecimalValue(5));
        writeCase(root,cases,"bulk-global","SET_GLOBAL_KV_BULK",packBytes(bulkPack(null,new DecimalValue(60),bulk)),bulkGlobal);
        writeCase(root,cases,"bulk-global-no-ttl","SET_GLOBAL_KV_BULK",packBytes(bulkPack(null,null,kvMap("p","perm"))),bulkGlobal);
        writeCase(root,cases,"bulk-global-empty","SET_GLOBAL_KV_BULK",packBytes(bulkPack(null,new DecimalValue(1),new MapValue())),bulkGlobal);
        writeCase(root,cases,"bulk-global-absent","SET_GLOBAL_KV_BULK",packBytes(bulkPack(null,new DecimalValue(1),null)),bulkGlobal);
        writeCase(root,cases,"set-custom","SET_CUSTOM_KV",packBytes(kvPack(CUSTOM,"a","custom-alpha",new DecimalValue(-1))),setCustom);
        writeCase(root,cases,"set-custom-no-space","SET_CUSTOM_KV",packBytes(kvPack(null,"a","orphan",null)),setCustom);
        writeCase(root,cases,"set-custom-empty-space","SET_CUSTOM_KV",packBytes(kvPack("","a","orphan",null)),setCustom);
        writeCase(root,cases,"set-custom-no-key","SET_CUSTOM_KV",packBytes(kvPack(CUSTOM,null,"orphan",null)),setCustom);
        writeCase(root,cases,"set-custom-ttl","SET_CUSTOM_KV",packBytes(kvPack(CUSTOM,"b","custom-beta",new DecimalValue(120))),setCustom);
        writeCase(root,cases,"ttl-custom","SET_CUSTOM_TTL",packBytes(kvPack(CUSTOM,"a",null,new DecimalValue(30))),ttlCustom);
        writeCase(root,cases,"ttl-custom-no-space","SET_CUSTOM_TTL",packBytes(kvPack(null,"a",null,new DecimalValue(30))),ttlCustom);
        writeCase(root,cases,"ttl-custom-no-key","SET_CUSTOM_TTL",packBytes(kvPack(CUSTOM,null,null,null)),ttlCustom);
        writeCase(root,cases,"ttl-custom-new-space","SET_CUSTOM_TTL",packBytes(kvPack(TTL_SPACE,"nothing",null,new DecimalValue(5))),ttlCustom);
        writeCase(root,cases,"bulk-custom","SET_CUSTOM_KV_BULK",packBytes(bulkPack(CUSTOM,new DecimalValue(-3),kvMap("x","cx","y","cy"))),bulkCustom);
        writeCase(root,cases,"bulk-custom-empty-kv","SET_CUSTOM_KV_BULK",packBytes(bulkPack(CUSTOM,null,new MapValue())),bulkCustom);
        writeCase(root,cases,"bulk-custom-empty-param","SET_CUSTOM_KV_BULK",packBytes(new MapPack()),bulkCustom);
        writeCase(root,cases,"bulk-custom-no-space","SET_CUSTOM_KV_BULK",packBytes(bulkPack(null,null,kvMap("x","1"))),bulkCustom);
        long after=System.currentTimeMillis()/1000L;
        writeCase(root,cases,"get-global-a","GET_GLOBAL_KV",valueBytes(new TextValue("a")),(in,out)->kv.getText(in,out,true));
        writeCase(root,cases,"get-global-c","GET_GLOBAL_KV",valueBytes(new TextValue("c")),(in,out)->kv.getText(in,out,true));
        writeCase(root,cases,"get-global-bulk","GET_GLOBAL_KV_BULK",packBytes(keys(null,"a","b","c","f","t","한글-key","k0","k6","","e","n","p","missing")),(in,out)->kv.getTextBulk(in,out,true));
        writeCase(root,cases,"get-custom-a","GET_CUSTOM_KV",packBytes(custom("a")),(in,out)->kv.getTextFromCustomStore(in,out,true));
        writeCase(root,cases,"get-custom-bulk","GET_CUSTOM_KV_BULK",packBytes(keys(CUSTOM,"a","b","x","y","missing")),(in,out)->kv.getTextBulkFromCustomStore(in,out,true));
        writeCase(root,cases,"get-ttl-space","GET_CUSTOM_KV",packBytes(kvPack(TTL_SPACE,"nothing",null,null)),(in,out)->kv.getTextFromCustomStore(in,out,true));
        // Defect candidates: the original handler throws before any write and
        // ServiceHandlingProxy swallows it, so the Client observes no body.
        writeCase(root,cases,"bulk-global-nonmap","SET_GLOBAL_KV_BULK",packBytes(withKV(bulkPack(null,null,null),new TextValue("x"))),bulkGlobal);
        writeCase(root,cases,"bulk-custom-absent-kv","SET_CUSTOM_KV_BULK",packBytes(bulkPack(CUSTOM,null,null)),bulkCustom);
        writeCase(root,cases,"bulk-custom-nonmap","SET_CUSTOM_KV_BULK",packBytes(withKV(bulkPack(CUSTOM,null,null),new ListValue())),bulkCustom);
        int throwing=0;
        for(int i=0;i<cases.size();i++){
            MapValue item=(MapValue)cases.get(i);String name=item.getText("name");byte[] response=((BlobValue)item.get("response")).value;
            if(item.getBoolean("throws")){throwing++;require(response.length==0,"Throwing handler emits nothing: "+name);continue;}
            if(name.startsWith("get-"))continue;
            if(name.endsWith("-no-key")||name.endsWith("-nontext-key")||name.equals("bulk-global-empty")||name.equals("bulk-global-absent")||name.equals("bulk-custom-empty-param"))require(response.length==0,"Null key/absent kv has no body: "+name);
            else if(name.startsWith("bulk-"))require(DBHandlerOracle.packs(response).get(0) instanceof MapPack,"Bulk MapPack response: "+name);
            else requireBoolean(response,!(name.endsWith("-empty-value")||name.endsWith("-no-value")||name.endsWith("-nontext-value")||name.endsWith("-empty-key")||name.endsWith("-no-space")||name.endsWith("-empty-space")));
        }
        require(throwing==3,"NPE/ClassCastException cases");
        requireText(((BlobValue)((MapValue)cases.get(cases.size()-9)).get("response")).value,"alpha-2");
        MapPack globalBulk=(MapPack)DBHandlerOracle.packs(((BlobValue)((MapValue)cases.get(cases.size()-7)).get("response")).value).get(0);
        require(globalBulk.size()==8&&globalBulk.getText("a").equals("alpha-2")&&globalBulk.getText("k6").equals("v6")&&globalBulk.getText("p").equals("perm")&&globalBulk.get("e")==null&&globalBulk.get("n")==null&&globalBulk.get("c")==null,"Bulk read after writes");
        close(GLOBAL);close(CUSTOM);close(TTL_SPACE);
        Files.write(root.resolve("kv-write-cases.bin"),valueBytes(cases));
        Files.write(root.resolve("kv-write-observations.json"),("{\"global\":\""+GLOBAL+"\",\"custom\":\""+CUSTOM+"\",\"ttl_space\":\""+TTL_SPACE+"\",\"root\":\"00000000/kv2\",\"before_seconds\":"+before+",\"after_seconds\":"+after+",\"no_expiry\":"+DataInputX.LONG5_MAX_VALUE+",\"cases\":"+cases.size()+",\"throwing_cases\":"+throwing+",\"locale\":\""+Locale.getDefault()+"\"}\n").getBytes("UTF-8"));
        System.out.println("PASS actual 6 KV SET/TTL handlers + RW/index/writer, "+cases.size()+" cases ("+throwing+" original handler exceptions), 6 READ re-queries");
    }
    static MapPack withValue(MapPack p,Value v){p.put("value",v);return p;}
    static MapPack withKey(MapPack p,Value v){p.put("key",v);return p;}
    static MapPack withKV(MapPack p,Value v){p.put("kv",v);return p;}
    public static void main(String[] args)throws Exception {
        Path root=Paths.get(args[1]);Files.createDirectories(root);System.setProperty("oracle.root",root.toAbsolutePath().toString());
        if(args[0].equals("writes")){writes(root);return;}
        KeyValueStoreService kv=new KeyValueStoreService();ListValue seeds=new ListValue();
        seed(seeds,GLOBAL,"a","synthetic-alpha");seed(seeds,GLOBAL,"b","synthetic-beta");seed(seeds,CUSTOM,"a","synthetic-custom-alpha");seed(seeds,CUSTOM,"b","synthetic-custom-beta");
        requireText(fixture(root,"kv-global",valueBytes(new TextValue("a")),(in,out)->kv.getText(in,out,true)),"synthetic-alpha");
        requireText(fixture(root,"kv-global-missing",valueBytes(new TextValue("missing")),(in,out)->kv.getText(in,out,true)),"");
        requireText(fixture(root,"kv-custom",packBytes(custom("a")),(in,out)->kv.getTextFromCustomStore(in,out,true)),"synthetic-custom-alpha");
        requireText(fixture(root,"kv-custom-missing",packBytes(custom("missing")),(in,out)->kv.getTextFromCustomStore(in,out,true)),"");
        for(String space:Arrays.asList(GLOBAL,CUSTOM)){
            String name=space.equals(GLOBAL)?"kv-global-bulk":"kv-custom-bulk";
            byte[] raw=fixture(root,name,packBytes(bulk(space.equals(GLOBAL)?null:space)),(in,out)->{if(space.equals(GLOBAL))kv.getTextBulk(in,out,true);else kv.getTextBulkFromCustomStore(in,out,true);});
            MapPack result=(MapPack)DBHandlerOracle.packs(raw).get(0);require(result.size()==2,"Bulk miss omission/duplicate replacement");
            require(Arrays.equals(packBytes(result),packBytes(expectedBulk(space))),"Bulk insertion order b,a preserved");
        }
        MapPack empty=new MapPack();empty.newList("key");require(fixture(root,"kv-global-bulk-empty",packBytes(empty),(in,out)->kv.getTextBulk(in,out,true)).length==0,"Empty bulk has no body");
        require(fixture(root,"kv-custom-no-key",packBytes(custom(null)),(in,out)->kv.getTextFromCustomStore(in,out,true)).length==0,"Absent custom key has no body");
        List<Value> inputs=new ArrayList<>();
        for(float f:new float[]{0f,-0f,0.001f,10000000f,Float.NaN,Float.POSITIVE_INFINITY,Float.NEGATIVE_INFINITY,Float.MIN_VALUE,Float.MAX_VALUE})inputs.add(new FloatValue(f));
        for(double d:new double[]{0d,-0d,0.001d,10000000d,1.2345678901234567d,Double.NaN,Double.POSITIVE_INFINITY,Double.NEGATIVE_INFINITY,Double.MIN_VALUE,Double.MAX_VALUE})inputs.add(new DoubleValue(d));
        inputs.add(new DecimalValue(Long.MIN_VALUE));inputs.add(new BooleanValue(true));inputs.add(NullValue.value);inputs.add(new TextValue("한글-key"));inputs.add(new BlobValue(new byte[]{1,2}));
        ListValue list=new ListValue();list.add(new TextValue("한글"));list.add(NullValue.value);list.add(new DecimalValue(7));inputs.add(list);
        MapValue map=new MapValue();map.put("Aa",new DecimalValue(1));map.put("BB",new DecimalValue(2));map.put("nested",list);inputs.add(map);
        List<byte[]> requests=new ArrayList<>();List<String> keys=new ArrayList<>();
        // Handler reads Value before toString; compute the key from that same
        // round-trip, retaining original Hashtable reconstruction semantics.
        for(int i=0;i<inputs.size();i++){byte[] request=valueBytes(inputs.get(i));String key=new DataInputX(request).readValue().toString();requests.add(request);keys.add(key);if(key!=null&&!key.isEmpty())seed(seeds,GLOBAL,key,"synthetic-key-"+i);}
        ListValue cases=new ListValue();
        for(int i=0;i<inputs.size();i++){
            byte[] response=run(requests.get(i),(in,out)->kv.getText(in,out,true));String key=keys.get(i);String expected=KeyValueStoreRW$.MODULE$.get(GLOBAL,key);
            requireText(response,expected==null?"":expected);MapValue item=new MapValue();item.put("request",new BlobValue(requests.get(i)));item.put("response",new BlobValue(response));item.put("key",key==null?NullValue.value:new TextValue(key));cases.add(item);
        }
        Files.write(root.resolve("kv-key-cases.bin"),valueBytes(cases));Files.write(root.resolve("kv-seeds.bin"),valueBytes(seeds));
        DBHandlerOracle.storedInputs(root);DatabaseService database=new DatabaseService();MapPack query=DBHandlerOracle.request();query.put("objHash",42);
        MapPack times=(MapPack)DBHandlerOracle.packs(fixture(root,"query-list-exist",packBytes(query),(in,out)->database.existQueryList(in,out,true))).get(0);
        require(times.getList("time").size()==3&&times.getList("time").getLong(2)==ExtraDataset.TIME+200,"Query-list includes every matching Status key");
        List<Pack> data=DBHandlerOracle.packs(fixture(root,"query-list-load",packBytes(query),(in,out)->database.loadQueryList(in,out,true)));
        require(data.size()==3,"Query-list emits 3 Data MapPacks");
        for(int i=0;i<data.size();i++){
            StatusPack input=(StatusPack)new DataInputX(Files.readAllBytes(root.resolve("db-status-"+i+"-input.bin"))).readPack();
            require(Arrays.equals(packBytes(data.get(i)),packBytes(input.toMapPack())),"StatusPack.toMapPack actual Hashtable order/data only");
            Files.write(root.resolve("query-list-map-"+i+".bin"),packBytes(input.toMapPack()));
        }
        close(GLOBAL);close(CUSTOM);
        Files.write(root.resolve("kv-observations.json"),("{\"global\":\""+GLOBAL+"\",\"custom\":\""+CUSTOM+"\",\"root\":\"00000000/kv2\",\"key_cases\":"+cases.size()+",\"locale\":\""+Locale.getDefault()+"\",\"query_list_times\":[1767193200125,1767193200225,1767193200325],\"query_list_data_only\":true}\n").getBytes("UTF-8"));
        System.out.println("PASS actual KV writer/index/reader + 4 KV READ handlers, "+cases.size()+" Value keys, 2 Database query-list handlers");
    }
    static MapPack expectedBulk(String space){MapPack p=new MapPack();p.put("b",space.equals(GLOBAL)?"synthetic-beta":"synthetic-custom-beta");p.put("a",space.equals(GLOBAL)?"synthetic-alpha":"synthetic-custom-alpha");return p;}
}
