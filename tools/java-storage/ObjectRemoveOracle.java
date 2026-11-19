// Object remove oracle: the original ObjectData/ObjectIndex (and the
// IndexKeyFile delete flag beneath ObjectIndex.delete) of pinned source
// dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5 write, delete and read object
// records exactly as ObjectWR.remove / ObjectRD.getObjectList do, minus the
// server queue thread. Compiled with StorageOracle by scripts/java-storage.py.
import java.nio.file.*;
import java.util.*;
import scouter.io.DataInputX;
import scouter.io.DataOutputX;
import scouter.lang.pack.ObjectPack;
import scouter.server.db.obj.*;

public class ObjectRemoveOracle {
    static final String DATE="20260101";
    static final int BLOCK_SIZE=4096; // ObjectWR.BLOCK_SIZE
    static void require(boolean ok,String what){if(!ok)throw new IllegalStateException(what);}
    static String file(Path root){Path dir=root.resolve(DATE).resolve("object");dir.toFile().mkdirs();return dir.resolve("obj").toString();}
    static byte[] objectBytes(int hash)throws Exception{
        ObjectPack p=new ObjectPack();p.objType="java";p.objHash=hash;p.objName="/remove-oracle-"+hash;p.address="127.0.0.1";p.version="oracle";p.alive=true;
        byte[] b=new DataOutputX().writePack(p).toByteArray();byte[] buff=new byte[BLOCK_SIZE];System.arraycopy(b,0,buff,0,b.length);return buff;
    }
    // ObjectWR add order: index.get < 0 then writer.write + index.set.
    static void generate(Path root,int[] hashes,int removed)throws Exception{
        String f=file(root);ObjectData od=new ObjectData(f);ObjectIndex oi=new ObjectIndex(f);
        for(int h:hashes){require(oi.get(h)<0,"fresh hash "+h);oi.set(h,od.write(objectBytes(h)));}
        remove(oi,removed);od.close();oi.close();
    }
    // ObjectWR delete order: index.get >= 0 then index.delete.
    static void remove(ObjectIndex oi,int hash){if(oi.get(hash)>=0)oi.delete(hash);}
    // ObjectRD.read/getObjectList (ObjectRD.scala:78-116): ObjectIndex.open,
    // ObjectData.open, idx.read(handler, reader.read). ObjectRD itself is not
    // compiled (it pulls the ObjectWR thread and TextRD/TextWR).
    static List<Integer> list(Path root)throws Exception{
        String f=file(root);final ObjectData od=ObjectData.open(f);ObjectIndex oi=ObjectIndex.open(f);
        final List<Integer> out=new ArrayList<Integer>();
        oi.read(new scala.runtime.AbstractFunction2<byte[],byte[],Object>(){public Object apply(byte[] key,byte[] data){try{ObjectPack p=(ObjectPack)new DataInputX(data).readPack();require(p.objHash==DataInputX.toInt(key,0),"key/pack hash");require(p.objName.equals("/remove-oracle-"+p.objHash),"pack name");out.add(p.objHash);return null;}catch(Exception e){throw new RuntimeException(e);}}},
                new scala.runtime.AbstractFunction1<Object,byte[]>(){public byte[] apply(Object pos){return od.read(((Number)pos).longValue());}});
        Collections.sort(out);od.close();oi.close();return out;
    }
    static List<Integer> csv(String s){List<Integer> out=new ArrayList<Integer>();if(!s.isEmpty())for(String p:s.split(","))out.add(Integer.parseInt(p.trim()));return out;}
    public static void main(String[] args)throws Exception{
        try{
            String mode=args[0];Path root=Paths.get(args[1]).toAbsolutePath();
            if(mode.equals("generate")){generate(root,new int[]{1,2,3,4,5},3);}
            else if(mode.equals("remove")){String f=file(root);ObjectIndex oi=new ObjectIndex(f);remove(oi,Integer.parseInt(args[2]));oi.close();}
            else if(mode.equals("verify")){
                List<Integer> want=csv(args[2]),got=list(root);require(got.equals(want),"object list "+got+" != "+want);
                // ObjectRD.getObjectPack (:34-56): idx.get(hash) < 0 means absent.
                String f=file(root);ObjectIndex oi=ObjectIndex.open(f);
                for(int h:csv(args[3]))require(oi.get(h)<0,"removed hash "+h+" still indexed");
                for(int h:want)require(oi.get(h)>=0,"live hash "+h+" not indexed");
                oi.close();
            } else throw new IllegalArgumentException(mode);
            System.out.println("PASS actual Java/Scala ObjectIndex "+mode+" source=dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5");
        }catch(Throwable e){e.printStackTrace();System.exit(1);}
    }
}
