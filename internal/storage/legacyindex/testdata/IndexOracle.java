/* Synthetic oracle. Storage/codec/hash/date classes are unmodified Apache-2.0
 * Scouter source at dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5. */
import java.nio.file.*;
import java.util.*;
import scouter.server.db.io.*;
import scouter.io.*;
import scouter.util.*;
public class IndexOracle {
    public static final long TIME=1767193200125L; // 2026-01-01 00:00:00.125 Asia/Seoul
    static void require(boolean ok,String why){if(!ok)throw new AssertionError(why);}
    static byte[] filled(int n){byte[] v=new byte[n];Arrays.fill(v,(byte)0x81);return v;}
    static void putHash(IndexKeyFile idx){
        idx.put(new byte[]{0,(byte)255,2},new byte[]{(byte)128});
        idx.put(new byte[]{0,(byte)255,2},new byte[]{3});
        int[] sizes={0,253,254,65535,65536};
        for(int n:sizes)idx.put(new byte[]{(byte)(n>>>16),(byte)(n>>>8),(byte)n},filled(n));
    }
    static void putTime(IndexTimeFile idx) throws Exception{
        idx.put(TIME,new byte[]{127});idx.put(TIME,new byte[]{(byte)128});idx.put(TIME,new byte[]{(byte)128});idx.put(TIME+400,new byte[]{0});
    }
    public static void main(String[] args) throws Exception{
        String mode=args[0];Path root=Paths.get(args[1]);Files.createDirectories(root);
        if(mode.equals("generate")){
            IndexKeyFile hash=new IndexKeyFile(root.resolve("hash").toString(),1);putHash(hash);hash.close();
            IndexTimeFile time=new IndexTimeFile(root.resolve("time").toString());putTime(time);time.close();
            // Nonzero bool deletion read, original count is retained.
            RealKeyFile keys=new RealKeyFile(root.resolve("hash").toString());keys.setDelete(2,true);keys.close();
            for(byte[] k:new byte[][]{new byte[0],new byte[]{0,(byte)255,2},new byte[]{(byte)128}})
                System.out.println("HASH "+HashUtil.hash(k));
        }else{
            IndexKeyFile hash=new IndexKeyFile(root.resolve("hash").toString(),1);
            require(Arrays.equals(hash.get(new byte[]{0,(byte)255,2}),new byte[]{3}),"hash get");
            require(hash.getAll(new byte[]{0,(byte)255,2}).size()==1,"deleted skipped");
            for(int n:new int[]{0,253,254,65535,65536})require(Arrays.equals(hash.get(new byte[]{(byte)(n>>>16),(byte)(n>>>8),(byte)n}),filled(n)),"blob width"+n);
            // verify-double-crash: two appended records of a new key both link to the empty saved head.
            boolean fork=mode.equals("verify-double-crash");
            if(fork){byte[] k="double-crash".getBytes("UTF-8");require(hash.get(k)==null&&hash.getAll(k).size()==0,"fork unreachable from head");}
            // A Go writable open tombstones a refused fork (deleted flag), so Java counts 6 active + 2 deleted.
            require(hash.getStat().get("count").intValue()==6,"hash active count");
            // deleted = the fixture's own setDelete(2) plus the two tombstones (flag 2 reads as true).
            if(fork){require(hash.getStat().get("deleted").intValue()==3,"fork tombstoned");}hash.close();
            IndexTimeFile time=new IndexTimeFile(root.resolve("time").toString());
            java.util.List<String> observed=new ArrayList<String>();
            time.read(TIME,TIME+500,new scala.runtime.AbstractFunction2<Object,byte[],Object>(){
                public Object apply(Object ms,byte[] value){observed.add(ms.toString()+":"+value[0]);return null;}
            });
            require(observed.equals(Arrays.asList(TIME+":-128",TIME+":127",(TIME+400)+":0")),"TreeSet order/dedupe "+observed);time.close();
            MemTimeBlock block=new MemTimeBlock(root.resolve("time").toString());require(block.getCount()==6,"time count records+occupied");block.close();
        }
        System.out.println("PASS actual Java/Scala Index "+mode);
    }
}
