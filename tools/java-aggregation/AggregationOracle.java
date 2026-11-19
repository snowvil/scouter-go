import java.nio.file.*;
import java.util.*;
import scouter.server.util.cardinality.HyperLogLog;
import scouter.server.tagcnt.TagCountConfig$;
import scouter.server.tagcnt.core.ValueCount;
import scouter.server.tagcnt.core.ValueCountTotal;
import scouter.lang.value.*;
import scouter.util.*;

/** Calls actual pinned algorithms and First/Next writers/readers, not copied formats. */
public class AggregationOracle {
 static long key(){return BitUtil.composite(HashUtil.hash("service"),HashUtil.hash("service"));}
 static HyperLogLog hll(){HyperLogLog h=new HyperLogLog(20);for(long v:new long[]{1,1,-1,2,Long.MIN_VALUE,Long.MAX_VALUE,0})h.offer(v);return h;}
 static void require(boolean ok,String what){if(!ok)throw new AssertionError(what);}
 static void generate(Path root)throws Exception{
  Files.createDirectories(root);HyperLogLog h=hll();Files.write(root.resolve("hll.bin"),h.getBytes());HyperLogLog other=new HyperLogLog(20);other.offer(2);other.offer(3);h.addAll(other);Files.write(root.resolve("merged.bin"),h.getBytes());
  Path tags=root.resolve("20261013/tagcnt/java");Files.createDirectories(tags);
  scouter.server.tagcnt.first.IndexFile first=new scouter.server.tagcnt.first.IndexFile(tags.resolve("first").toString(),1);
  scouter.server.tagcnt.next.IndexFile next=new scouter.server.tagcnt.next.IndexFile(tags.resolve("next").toString(),1);
  for(int i=1;i<=100;i++)first.add(key(),new TextHashValue(i),710,1f);
  long tieKey=BitUtil.composite(HashUtil.hash("service"),HashUtil.hash("service-time-sum"));first.add(tieKey,new TextHashValue(1),710,.25f);first.add(tieKey,new TextHashValue(2),710,.75f);first.add(tieKey,new TextHashValue(3),710,1.5f);
  ArrayList<ValueCount> ties=new ArrayList<>();for(int i=1;i<=3;i++)ties.add(new ValueCount(new TextHashValue(i),i==1?.25f:i==2?.75f:1.5f));Collections.sort(ties,(a,b)->(int)(b.valueCount-a.valueCount));StringBuilder order=new StringBuilder("TIE_ORDER=");for(ValueCount v:ties)order.append(((TextHashValue)v.tagValue).value).append(",");System.out.println(order);
  float[] hour=new float[60];hour[10]=1;next.add(key(),new TextHashValue(101),7,hour);first.close();next.close();
  TopN<ValueCount> top=new TopN<>(100,TopN.DIRECTION.DESC);top.add(new ValueCount(new TextHashValue(101),1));HashMap<Value,Integer> values=new HashMap<>();for(int i=1;i<=100;i++)values.put(new TextHashValue(i),i);for(Value v:values.keySet())top.add(new ValueCount(v,1));
  Files.write(root.resolve("cache.bin"),new ValueCountTotal(101,101f,top.getList()).toByteArray());
  StringEnumer groups=TagCountConfig$.MODULE$.getTagGroups();while(groups.hasMoreElements()){String g=groups.nextString();StringBuilder names=new StringBuilder(g);StringEnumer n=TagCountConfig$.MODULE$.getTagNames(g);while(n.hasMoreElements())names.append("|").append(n.nextString());System.out.println(names);}
  // Actual hourly load receives a pre-suffixed name, producing the second suffix.
  System.out.println("HLL="+hll().cardinality()+" MERGED="+h.cardinality());
  System.setProperty("oracle.root",root.resolve("visitor-original").toString());long now=System.currentTimeMillis();Path visit=root.resolve("visitor-original").resolve(DateUtil.yyyymmdd(now)).resolve("visit_hourly");Files.createDirectories(visit);HyperLogLog old=new HyperLogLog(20);old.offer(1);Files.write(visit.resolve(Hexa32.toString32(42)+"_"+DateUtil.getHour(now)+".usr"),old.getBytes());
  HyperLogLog restored=scouter.server.db.VisitorHourlyDB$.MODULE$.getNewObject(42);restored.offer(2);require(restored.cardinality()==1,"actual hourly double suffix");System.out.println("ORIGINAL_HOURLY_DOUBLE_SUFFIX_PASS");
 }
 static void verify(Path root)throws Exception{
  require(Arrays.equals(Files.readAllBytes(root.resolve("hll.bin")),hll().getBytes()),"Go HLL bytes");
  Path tags=root.resolve("20261013/tagcnt/java");scouter.server.tagcnt.first.IndexFile first=new scouter.server.tagcnt.first.IndexFile(tags.resolve("first").toString(),1);scouter.server.tagcnt.next.IndexFile next=new scouter.server.tagcnt.next.IndexFile(tags.resolve("next").toString(),1);
  for(int i=1;i<=100;i++)require(first.get(key(),new TextHashValue(i))[430]==1f,"Go First "+i);
  require(first.get(key(),new TextHashValue(101))==null,"First overflow");require(next.get(key(),new TextHashValue(101))[430]==1f,"Next overflow");require(next.get(key(),new DecimalValue(101))==null,"typed Value");first.close();next.close();
  ValueCountTotal c=new ValueCountTotal().toObject(Files.readAllBytes(tags.resolve("top100."+key()+".dat")));require(c.howManyValues()==101&&c.totalCount()==101f&&c.values().size()==100,"Go Top100 format");System.out.println("GO_TO_JAVA_PASS");
 }
 public static void main(String[]args)throws Exception{if(args[0].equals("generate"))generate(Paths.get(args[1]));else verify(Paths.get(args[1]));}
}
