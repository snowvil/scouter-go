// Synthetic request/Pack inputs through actual original four Summary handlers.
// Apache-2.0 Scouter source is unchanged; only SummaryRD data supply is a seam.
import java.nio.file.*;
import java.util.*;
import scouter.io.*;
import scouter.lang.*;
import scouter.lang.pack.*;
import scouter.lang.value.*;
import scouter.net.TcpFlag;
import scouter.server.db.HandlerOracleInput;
import scouter.server.netio.service.handle.*;

public final class SummaryHandlerOracle {
    interface Handler{void run(DataInputX in,DataOutputX out);}
    static void require(boolean v,String s){if(!v)throw new AssertionError(s);}
    static final long ID=4294967303L;
    static SummaryPack record(byte kind,int count) {
        SummaryPack p=new SummaryPack();p.time=ExtraDataset.TIME;p.objHash=42;p.objType="host";p.stype=kind;
        String[] keys;
        if(kind==SummaryEnum.ENDUSER_AJAX_TIME)keys=new String[]{"uri","ip","count","duration","userAgent"};
        else if(kind==SummaryEnum.ENDUSER_SCRIPT_ERROR)keys=new String[]{"host","stacktrace","userAgent","count","uri","message","file","lineNumber","columnNumber"};
        else keys=new String[]{"uri","ip","count","navigationStart","unloadEventStart","unloadEventEnd","fetchStart","domainLookupStart","domainLookupEnd","connectStart","connectEnd","requestStart","responseStart","responseEnd","domLoading","domInteractive","domContentLoadedEventStart","domContentLoadedEventEnd","domComplete","loadEventStart","loadEventEnd"};
        p.table.newList("id").add(ID);
        for(String key:keys)p.table.newList(key).add(key.equals("count")?count:key.equals("host")?99:key.equals("stacktrace")?44:10);
        return p;
    }
    static MapPack run(Path root,String name,SummaryPack[] input,Handler handler)throws Exception {
        MapPack request=new MapPack();request.put("date",ExtraDataset.DATE);request.put("stime",ExtraDataset.TIME-1);
        request.put("etime",ExtraDataset.TIME+1);request.put("objType","host");request.put("objHash",42);
        byte[] req=new DataOutputX().writePack(request).toByteArray();HandlerOracleInput.records=new byte[input.length][];
        for(int i=0;i<input.length;i++){HandlerOracleInput.records[i]=new DataOutputX().writePack(input[i]).toByteArray();Files.write(root.resolve("summary-"+name+"-input-"+i+".bin"),HandlerOracleInput.records[i]);}
        DataOutputX out=new DataOutputX();handler.run(new DataInputX(req),out);byte[] raw=out.toByteArray();DataInputX in=new DataInputX(raw);
        require(in.readByte()==TcpFlag.HasNEXT,"Summary HasNEXT");MapPack result=in.readMapPack();
        Files.write(root.resolve("summary-"+name+"-request.bin"),req);Files.write(root.resolve("summary-"+name+"-response.bin"),raw);return result;
    }
    static void generate(Path root)throws Exception {
        MapPack app=run(root,"app",new SummaryPack[]{ExtraDataset.summary(ExtraDataset.TIME,42)},(in,out)->new SummaryService().LOAD_SERVICE_SUMMARY(in,out,true));
        require(app.getList("count").getInt(0)==3,"APP count");
        MapPack ajax=run(root,"ajax",new SummaryPack[]{record(SummaryEnum.ENDUSER_AJAX_TIME,2),record(SummaryEnum.ENDUSER_AJAX_TIME,3)},(in,out)->new SummaryEndUserAjax().LOAD_ENDUSER_AJAX_SUMMARY(in,out,true));
        MapPack nav=run(root,"nav",new SummaryPack[]{record(SummaryEnum.ENDUSER_NAVIGATION_TIME,2),record(SummaryEnum.ENDUSER_NAVIGATION_TIME,3)},(in,out)->new SummaryEndUserNav().LOAD_ENDUSER_NAV_SUMMARY(in,out,true));
        MapPack error=run(root,"error",new SummaryPack[]{record(SummaryEnum.ENDUSER_SCRIPT_ERROR,2),record(SummaryEnum.ENDUSER_SCRIPT_ERROR,3)},(in,out)->new SummaryEndUserError().LOAD_ENDUSER_ERROR_SUMMARY(in,out,true));
        for(MapPack p:Arrays.asList(ajax,nav,error))require(p.getList("id").size()==1&&p.getList("id").getLong(0)==ID,"Long IDs retained");
        require(ajax.getList("count").getInt(0)==3&&ajax.getList("duration").getLong(0)==10,"Actual int lookup misses long ID, replacement takes second row");
        require(nav.getList("count").getInt(0)==3&&nav.getList("navigationStart").getLong(0)==10,"NAV long ID replacement");
        require(error.getList("host").getInt(0)==44&&error.getList("stacktrace").getInt(0)==44&&error.get("count")==null,"Error host=stacktrace and count omitted");
        System.out.println("PASS actual SummaryService/EndUserNav/Ajax/Error: long-ID replacement, host=stacktrace, count omitted");
    }
}
