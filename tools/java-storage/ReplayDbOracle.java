// Synthetic replay verification; storage implementations are pinned original Scouter classes.
import java.nio.file.*;
import java.util.*;
import scouter.io.*;
import scouter.lang.pack.*;
import scouter.server.db.io.*;
import scouter.server.db.io.zip.*;
import scouter.server.db.xlog.*;
import scouter.server.db.obj.*;
import scouter.server.db.text.*;
import scouter.util.*;
public class ReplayDbOracle {
 static long logs,profiles,profileBytes,objects,texts,keyed;
 static Set<Long> ids=new TreeSet<Long>();
 static scala.runtime.AbstractFunction1<Object,byte[]> reader(final XLogDataReader r){return new scala.runtime.AbstractFunction1<Object,byte[]>(){public byte[] apply(Object pos){return r.read(((Number)pos).longValue());}};}
 static void check(boolean ok,String msg){if(!ok)throw new AssertionError(msg);}
 static void date(final Path root,final String date)throws Exception {
  final String x=root.resolve(date+"/xlog/xlog").toString();
  if(Files.exists(Paths.get(x+"_tim.kfile"))){
   final XLogDataReader r=new XLogDataReader(date,x);final XLogIndex idx=new XLogIndex(x);
   IndexTimeFile time=new IndexTimeFile(x+"_tim");long start=DateUtil.yyyymmdd(date);
   time.read(start,start+86400000L-1,new scala.runtime.AbstractFunction2<Object,byte[],Object>(){public Object apply(Object t,byte[] pos){try{
    byte[] b=r.read(DataInputX.toLong5(pos,0));check(b!=null,"null xlog");XLogPack p=(XLogPack)new DataInputX(b).readPack();logs++;ids.add(p.txid);
    byte[] byid=r.read(idx.getByTxid(p.txid));check(Arrays.equals(b,byid),"txid lookup mismatch "+p.txid);keyed++;return null;
   }catch(Exception e){throw new RuntimeException(e);}}});time.close();idx.close();r.close();
   if(Files.exists(Paths.get(x+"_profile.kfile"))){final XLogProfileDataReader pr=new XLogProfileDataReader(date,x);XLogProfileIndex pi=new XLogProfileIndex(x);
    for(long id:ids){for(Object pos:pi.getByTxid(id)){byte[] b=pr.read(((Number)pos).longValue());check(b!=null,"null profile "+id);profiles++;profileBytes+=b.length;}}pi.close();pr.close();}
  }
  String o=root.resolve(date+"/object/obj").toString();if(Files.exists(Paths.get(o+".kfile"))){final ObjectData od=new ObjectData(o);ObjectIndex oi=new ObjectIndex(o);
   oi.read(new scala.runtime.AbstractFunction2<byte[],byte[],Object>(){public Object apply(byte[] key,byte[] b){try{check(new DataInputX(b).readPack() instanceof ObjectPack,"object decode");objects++;return null;}catch(Exception e){throw new RuntimeException(e);}}},new scala.runtime.AbstractFunction1<Object,byte[]>(){public byte[] apply(Object pos){return od.read(((Number)pos).longValue());}});oi.close();od.close();}
  Path td=root.resolve(date+"/text");if(Files.isDirectory(td)){try(DirectoryStream<Path> ds=Files.newDirectoryStream(td,"*.kfile")){for(Path f:ds){String base=f.toString().replace(".kfile","");if(f.getFileName().toString().equals("text.kfile")){TextTable tt=new TextTable(base);tt.read(new scala.runtime.AbstractFunction2<byte[],byte[],Object>(){public Object apply(byte[] k,byte[] b){check(b!=null,"daily text");texts++;return null;}});tt.close();}else{String div=f.getFileName().toString().replace("text_","").replace(".kfile","");final TextPermData data=new TextPermData(div,base);TextPermIndex ti=new TextPermIndex(div,base);ti.read(new scala.runtime.AbstractFunction2<byte[],byte[],Object>(){public Object apply(byte[] k,byte[] b){check(b!=null,"perm text");texts++;return null;}},new scala.runtime.AbstractFunction1<Object,byte[]>(){public byte[] apply(Object pos){return data.read(((Number)pos).longValue());}});ti.close();data.close();}}}}
  Path zd=root.resolve(date+"/xlog/data");if(Files.isDirectory(zd)){long count=new DataInputX(Files.readAllBytes(zd.resolve("count.dat"))).readLong();try(DirectoryStream<Path> ds=Files.newDirectoryStream(zd,"xlog.*")){for(Path f:ds){long n=Long.parseLong(f.getFileName().toString().substring(5));byte[] b=CompressUtil.unZip(Files.readAllBytes(f));long required=count/33554432L==n?count%33554432L:33554432L;check(b!=null&&b.length>=required,"block/count mismatch");System.out.println("BLOCK date="+date+" name="+f.getFileName()+" decoded="+b.length+" count="+count+" required="+required);}}}
 }
 static void append(Path root,String date)throws Exception{System.setProperty("oracle.root",root.toString());String x=root.resolve(date+"/xlog/xlog").toString();XLogDataWriter w=new XLogDataWriter(date,x);XLogIndex i=new XLogIndex(x);for(int n=0;n<3;n++){XLogPack p=new XLogPack();p.txid=7000001+n;p.endTime=DateUtil.yyyymmdd(date)+1000+n;p.objHash=42;p.service=7;p.elapsed=10;long pos=w.write(new DataOutputX().writePack(p).toByteArray());i.setByTxid(p.txid,pos);i.setByTime(p.endTime,pos);}w.close();i.close();GZipStore.getInstance().shutdown();StorageOracle.finishGzip();System.out.println("APPENDED=3");}
 static void query(String addr,String date)throws Exception {
  String[] parts=addr.split(":");java.net.Socket sock=new java.net.Socket(parts[0],Integer.parseInt(parts[1]));sock.setSoTimeout(10000);
  DataOutputX out=new DataOutputX(new java.io.DataOutputStream(sock.getOutputStream()));DataInputX in=new DataInputX(new java.io.DataInputStream(sock.getInputStream()));out.writeInt(0xcafe2001);
  scouter.lang.pack.MapPack login=new scouter.lang.pack.MapPack();login.put("id","admin");byte[] h=java.security.MessageDigest.getInstance("SHA-256").digest("qwertyuiop!@#$%^&*()zxcvbnm,.admin".getBytes("UTF-8"));StringBuilder hex=new StringBuilder();for(byte b:h)hex.append(String.format("%02x",b&255));login.put("pass",hex.toString());out.writeText("LOGIN").writeLong(0).writePack(login);out.flush();long session=0;int flag;while((flag=in.readByte())==3){scouter.lang.pack.MapPack m=(scouter.lang.pack.MapPack)in.readPack();session=m.getLong("session");}check(flag==4&&session!=0,"login");
  scouter.lang.pack.MapPack req=new scouter.lang.pack.MapPack();req.put("date",date);req.put("stime",DateUtil.yyyymmdd(date));req.put("etime",DateUtil.yyyymmdd(date)+2000);req.put("max",0);req.newList("objHash").add(42);out.writeText("TRANX_LOAD_TIME_GROUP").writeLong(session).writePack(req);out.flush();Set<Long> got=new HashSet<Long>();while((flag=in.readByte())==3){scouter.lang.pack.Pack p=in.readPack();if(p instanceof XLogPack)got.add(((XLogPack)p).txid);}check(flag==4,"query response");check(got.containsAll(Arrays.asList(7000001L,7000002L,7000003L)),"appended query "+got);sock.close();System.out.println("PASS Go server queried Java appended txids="+got);
 }
 public static void main(String[] args)throws Exception{try{if(args[0].equals("query")){query(args[1],args[2]);System.exit(0);}Path root=Paths.get(args[0]).toAbsolutePath();System.setProperty("oracle.root",root.toString());if(args.length>1){append(root,args[1]);System.exit(0);}try(DirectoryStream<Path> ds=Files.newDirectoryStream(root)){for(Path d:ds)if(Files.isDirectory(d)&&d.getFileName().toString().matches("[0-9]{8}"))date(root,d.getFileName().toString());}
 Files.write(Paths.get(args[0]+".txids.txt"),ids.stream().map(String::valueOf).collect(java.util.stream.Collectors.toList()));System.out.println("RESULT xlogs="+logs+" unique="+ids.size()+" decoded="+logs+" txid_lookups="+keyed+" profile_chunks="+profiles+" profile_bytes="+profileBytes+" objects="+objects+" texts="+texts);System.exit(0);}catch(Throwable e){e.printStackTrace();System.exit(1);}}
}
