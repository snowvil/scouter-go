// Actual original RealKeyFile2/IndexKeyFile2/MemHashBlock, Apache-2.0 pinned source.
// All data/header/TTL/update/delete bytes are emitted/read by original classes.
import java.nio.file.*;
import java.util.*;
import scouter.io.*;
import scouter.server.db.io.*;
import scouter.util.HashUtil;

public final class TTLOracle {
    static final int SIZE=1048576;
    static void require(boolean value,String message){if(!value)throw new AssertionError(message);}
    static byte[] key(String s)throws Exception{return s.getBytes("UTF-8");}
    static byte[] offset(long n){return DataOutputX.toBytes5(n);}
    static String collision()throws Exception {int bucket=(HashUtil.hash(key("a"))&Integer.MAX_VALUE);bucket%=SIZE/5;for(int i=0;i<5000000;i++){String s="collision-"+i;if((HashUtil.hash(key(s))&Integer.MAX_VALUE)%(SIZE/5)==bucket)return s;}throw new AssertionError("No collision");}
    static void generate(Path root)throws Exception {
        Files.createDirectories(root);IndexKeyFile2 idx=new IndexKeyFile2(root.resolve("ttl").toString(),1);String collision=collision();
        idx.put(key("a"),offset(11),-1);long first=idx.hashBlock().get(HashUtil.hash(key("a")));idx.keyFile().setExpire(first,100);
        idx.put(key(collision),offset(22),-1);idx.put(key("a"),offset(33),-1);require(idx.delete(key("a"))==1,"Delete newest");
        idx.updateOrPut(key("a"),offset(44),-1);long latest=idx.hashBlock().get(HashUtil.hash(key("a")));
        idx.put(key("expired"),offset(55),-1);long expired=idx.hashBlock().get(HashUtil.hash(key("expired")));idx.keyFile().setExpire(expired,100);
        idx.put(key("deleted"),offset(66),-1);require(idx.delete(key("deleted"))==1,"Delete separate key");
        idx.put(key("empty"),new byte[0],-1);idx.put(key("future"),offset(77),-1);long future=idx.hashBlock().get(HashUtil.hash(key("future")));idx.keyFile().setExpire(future,4294967295L);
        require(idx.keyFile().getExpire(first)==DataInputX.LONG5_MAX_VALUE&&idx.keyFile().getExpire(latest)==DataInputX.LONG5_MAX_VALUE,"Update reset old expire/deleted matching records to sentinel");
        idx.hashBlock().flush();idx.close();
        String json="{\"hash_size_bytes\":1048576,\"no_expiry\":549755813632,\"collision_key\":\""+collision+"\",\"first_a_position\":"+first+",\"latest_a_position\":"+latest+",\"expired_position\":"+expired+",\"future_position\":"+future+",\"a_value_offset\":44,\"collision_value_offset\":22,\"expired_seconds\":100,\"future_seconds\":4294967295,\"a_physical_matches\":3,\"source_commit\":\"dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5\"}\n";
        Files.write(root.resolve("ttl-metadata.json"),json.getBytes("UTF-8"));
    }
    static void verify(Path root)throws Exception {
        String collision=collision();IndexKeyFile2 idx=new IndexKeyFile2(root.resolve("ttl").toString(),1);
        require(Arrays.equals(idx.get(key("a")),offset(44)),"Latest a lookup");require(Arrays.equals(idx.get(key(collision)),offset(22)),"Collision chain");
        require(idx.getAll(key("a")).size()==3,"Matching updates followed by append preserve all3 physical records");for(byte[] raw:idx.getAll(key("a")))require(Arrays.equals(raw,offset(44)),"All physical a values updated");
        require(idx.get(key("expired"))==null&&idx.getAll(key("expired")).size()==1,"Expired latest hides lookup but original getAll includes nondeleted expired");
        require(idx.get(key("deleted"))==null&&idx.getAll(key("deleted")).isEmpty(),"Deleted flag");require(idx.get(key("empty"))!=null&&idx.get(key("empty")).length==0,"Empty Blob vs missing");
        require(Arrays.equals(idx.get(key("future")),offset(77)),"Absolute future expire");
        long pos=idx.keyFile().getFirstPos(),length=idx.keyFile().getLength();int matching=0;
        while(pos<length){KeyFileItem2 r=idx.keyFile().getRecord(pos);if(Arrays.equals(r.key(),key("a"))){require(!r.deleted()&&r.expire()==549755813632L,"Rewritten flags and actual LONG5 sentinel");matching++;}pos=r.offset();}
        require(pos==length&&matching==3,"EOF record walk");idx.close();
    }
    public static void main(String[] args)throws Exception {Path root=Paths.get(args[1]);if(args[0].equals("generate"))generate(root);else require(args[0].equals("verify"),"Unknown mode");verify(root);System.out.println("PASS actual TTLIndex "+args[0]+": collision/update-all+append/expired/deleted/empty/sentinel/reopen");}
}
