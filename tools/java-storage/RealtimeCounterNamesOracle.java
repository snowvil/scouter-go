/* Synthetic fixture only. Actual RealtimeCounterDBHeader/Data/Index/DBHelper
 * classes are unmodified Scouter Apache-2.0 source
 * dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5 (compiled by scripts/java-storage.py).
 *
 * A realtime counter name dictionary (.head) larger than Go's former 4096-name
 * read limit, written the way RealtimeCounterWR does (intern(keySet) once per
 * pack, getTagBytes, data write, index write):
 *   generate <root> <names>  records k=0..names/100-1 at TIME+k*10, 100 names
 *                            each (c%05d = Decimal i), then TIME+1000 with
 *                            c00000=-1 and the last name=-2.
 *   verify <root> <names>    the generated records plus Go's continuation at
 *                            TIME+2000: c04500=7, go-new=8 (id names+1); then
 *                            appends TIME+2500: go-new=10, java-after=9.
 *   verify-go <root>         a day Go created (.head through createNames, a
 *                            stale tail after the length, a partially stored
 *                            pack): names a,b,c; TIME a=1,b=2; TIME+10 c=3;
 *                            TIME+20 a=4 (d dropped by Go). Read through
 *                            RealtimeCounterDBHelper.open(date, true) and the
 *                            calls of RealtimeCounterRD.read and readBulk. */
import java.nio.file.*;
import java.util.*;
import scouter.lang.value.*;
import scouter.server.db.counter.*;
import scouter.util.*;
import scouter.io.DataInputX;
import scala.collection.mutable.ArrayBuffer;

public class RealtimeCounterNamesOracle {
    static final String DATE="20260101", OBJ="/oracle/app";
    static final long TIME=1767193200125L;
    static void require(boolean value,String message){if(!value)throw new AssertionError(message);}
    static String name(int i){return String.format("c%05d",i);}
    static String real(Path root)throws Exception{Path dir=root.resolve(DATE).resolve("counter");Files.createDirectories(dir);return dir.resolve("real").toString();}
    static void put(RealtimeCounterDBHeader rh,RealtimeCounterData rd,RealtimeCounterIndex ri,long time,MapValue values){
        rh.intern(values.keySet());
        ri.write(HashUtil.hash(OBJ),time,rd.write(RealtimeCounterDBHelper.getTagBytes(rh.getTagStrInt(),values)));
    }
    static void verifyGo(Path root)throws Exception{
        System.setProperty("oracle.root",root.toString());
        final RealtimeCounterDBHelper db=new RealtimeCounterDBHelper().open(DATE,true);require(db!=null,"go day");
        IntKeyMap<String> tags=db.counterDbHeader().getTagIntStr();
        require(tags.size()==3&&"a".equals(tags.get(1))&&"b".equals(tags.get(2))&&"c".equals(tags.get(3)),"go dictionary "+tags);
        final List<Long> times=new ArrayList<Long>();final List<MapValue> rows=new ArrayList<MapValue>();
        // RealtimeCounterRD.read(objName, date, stime, etime, handler)
        db.counterIndex().read(HashUtil.hash(OBJ),TIME,TIME+1000,new scala.runtime.AbstractFunction2<Object,MapValue,Object>(){public Object apply(Object time,MapValue values){times.add(((Number)time).longValue());rows.add(values);return null;}},tags,new scala.runtime.AbstractFunction1<Object,byte[]>(){public byte[] apply(Object pos){return db.counterData().read(((Number)pos).longValue());}});
        require(times.equals(Arrays.asList(TIME,TIME+10,TIME+20)),"go times "+times);
        require(rows.get(0).size()==2&&((DecimalValue)rows.get(0).get("a")).value==1&&((DecimalValue)rows.get(0).get("b")).value==2,"row 0 "+rows.get(0));
        require(rows.get(1).size()==1&&((DecimalValue)rows.get(1).get("c")).value==3,"row 1 "+rows.get(1));
        require(rows.get(2).size()==1&&((DecimalValue)rows.get(2).get("a")).value==4,"partial row "+rows.get(2));
        // RealtimeCounterRD.readBulk: one 60s chunk, records through setTagBytesMulti.
        scala.Tuple2<byte[],byte[]> pos=db.counterIndex().getStartEndDataPos(TIME,TIME+59999);
        require(pos._1()!=null&&pos._2()!=null,"bulk positions");
        ArrayBuffer<MapValue> bulk=RealtimeCounterDBHelper.setTagBytesMulti(tags,db.counterData().readBulk(DataInputX.toLong5(pos._1(),0),DataInputX.toLong5(pos._2(),0)));
        require(bulk.size()==3&&bulk.apply(2).size()==1&&((DecimalValue)bulk.apply(2).get("a")).value==4,"bulk "+bulk);
        db.close();
        System.out.println("verified a Go-created day: 3 names, partial record");
    }
    public static void main(String[] args)throws Exception{
        if(args[0].equals("verify-go")){verifyGo(Paths.get(args[1]));return;}
        Path root=Paths.get(args[1]);int names=Integer.parseInt(args[2]);String real=real(root);
        final RealtimeCounterDBHeader rh=new RealtimeCounterDBHeader(real);final RealtimeCounterData rd=new RealtimeCounterData(real);RealtimeCounterIndex ri=new RealtimeCounterIndex(real);
        if(args[0].equals("generate")){
            for(int k=0;k<names/100;k++){MapValue values=new MapValue();for(int i=k*100;i<(k+1)*100;i++)values.put(name(i),new DecimalValue(i));put(rh,rd,ri,TIME+k*10,values);}
            MapValue last=new MapValue();last.put(name(0),new DecimalValue(-1));last.put(name(names-1),new DecimalValue(-2));put(rh,rd,ri,TIME+1000,last);
            require(rh.getTagIntStr().size()==names,"java dictionary size");
            System.out.println("generated "+names+" realtime counter names");
        }else if(args[0].equals("verify")){
            require(rh.getTagIntStr().size()==names+1,"dictionary size "+rh.getTagIntStr().size());
            require("go-new".equals(rh.getTagIntStr().get(names+1)),"go id "+rh.getTagIntStr().get(names+1));
            require(rh.getTagStrInt().size()==names+1&&rh.getTagStrInt().get("go-new")==names+1,"name to id");
            final List<Long> times=new ArrayList<Long>();final List<MapValue> rows=new ArrayList<MapValue>();
            ri.read(HashUtil.hash(OBJ),TIME,TIME+3000,new scala.runtime.AbstractFunction2<Object,MapValue,Object>(){public Object apply(Object time,MapValue values){times.add(((Number)time).longValue());rows.add(values);return null;}},rh.getTagIntStr(),new scala.runtime.AbstractFunction1<Object,byte[]>(){public byte[] apply(Object pos){return rd.read(((Number)pos).longValue());}});
            int records=names/100;require(times.size()==records+2,"records "+times.size());
            for(int k=0;k<records;k++){require(times.get(k)==TIME+k*10,"time "+k);MapValue v=rows.get(k);require(v.size()==100,"row size "+k);for(int i=k*100;i<(k+1)*100;i++)require(((DecimalValue)v.get(name(i))).value==i,"value "+name(i));}
            MapValue last=rows.get(records);require(((DecimalValue)last.get(name(0))).value==-1&&((DecimalValue)last.get(name(names-1))).value==-2,"java last row "+last);
            MapValue go=rows.get(records+1);require(times.get(records+1)==TIME+2000&&go.size()==2,"go row "+go);
            require(((DecimalValue)go.get(name(4500))).value==7&&((DecimalValue)go.get("go-new")).value==8,"go values "+go);
            MapValue after=new MapValue();after.put("go-new",new DecimalValue(10));after.put("java-after",new DecimalValue(9));put(rh,rd,ri,TIME+2500,after);
            require(rh.getTagIntStr().size()==names+2,"java append");
            System.out.println("verified "+(names+1)+" realtime counter names, appended java-after");
        }else throw new IllegalArgumentException(args[0]);
        rd.close();ri.close();rh.close();
    }
}
